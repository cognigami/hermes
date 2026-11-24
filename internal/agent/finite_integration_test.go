package agent_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"hermes/internal/controller"
	"hermes/internal/platform"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func finiteCertificate(t *testing.T, name string, parent *x509.Certificate, signer *ecdsa.PrivateKey) (tls.Certificate, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	serial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if e != nil {
		t.Fatal(e)
	}
	certificate := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	if parent == nil {
		certificate.IsCA = true
		certificate.BasicConstraintsValid = true
		certificate.KeyUsage |= x509.KeyUsageCertSign
		parent = certificate
		signer = key
	}
	der, e := x509.CreateCertificate(rand.Reader, certificate, parent, &key.PublicKey, signer)
	if e != nil {
		t.Fatal(e)
	}
	parsed, e := x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, parsed, key
}
func TestRegisteredNativeFiniteControllerSDKExchange(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("registered native Darwin qualification only")
	}
	ctx := context.Background()
	binary, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	runtimeBytes, e := os.ReadFile(binary)
	if e != nil {
		t.Fatal(e)
	}
	artifact := contracts.Artifact{Digest: contracts.Hash(runtimeBytes), Size: int64(len(runtimeBytes)), MediaType: "application/octet-stream"}
	build := platform.Current()
	ref := contracts.FiniteRuntimeRef{Runtime: "synthetic-v1", GOOS: build.GOOS, GOARCH: build.GOARCH, ISABaseline: build.ISABaseline, Executable: artifact}
	policy := contracts.ResourcePolicy{SchemaVersion: 1, Revision: 1, ControllerHostID: "host", TransferSlots: 1, ValidatorSlots: 1, TransferReservation: contracts.ResourceVector{CPUMilli: 100, MemoryBytes: 16 << 20, SpoolBytes: 64 << 20}, ValidatorReservation: contracts.ResourceVector{CPUMilli: 100, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}, Hosts: []contracts.HostApproval{{HostID: "host", WorkerID: "worker", MaxAssignments: 4, Limits: contracts.ResourceVector{CPUMilli: 1000, MemoryBytes: 512 << 20, SpoolBytes: 256 << 20}, Baseline: contracts.ResourceVector{CPUMilli: 100, MemoryBytes: 64 << 20, SpoolBytes: 16 << 20}}}, Profiles: []contracts.ResourceProfile{{ID: "small", Conservative: true, Allocation: contracts.Allocation{Resources: contracts.ResourceVector{CPUMilli: 250, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}}}}}
	c, e := controller.Open(controller.Config{Root: filepath.Join(t.TempDir(), "controller"), Workers: []string{"worker"}, Profile: "small", Runtime: "synthetic-v1", Validator: "synthetic-v1", ValidatorArgv: []string{binary, "-test.run=NoValidatorExecution"}, ResourcePolicy: &policy, MaxArtifactBytes: 64 << 20, MaxConcurrent: 4})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	_, ca, signer := finiteCertificate(t, "ca", nil, nil)
	serverPair, _, _ := finiteCertificate(t, "server", ca, signer)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	server := httptest.NewUnstartedServer(c.Handler())
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverPair}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}
	server.StartTLS()
	defer server.Close()
	client := func(principal string) *agent.Client {
		pair, _, _ := finiteCertificate(t, principal, ca, signer)
		transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{pair}}}
		t.Cleanup(transport.CloseIdleConnections)
		return &agent.Client{URL: server.URL, HTTP: &http.Client{Transport: transport}}
	}
	operator, worker := client("operator"), client("worker:worker")
	if e = operator.Publish(ctx, artifact.Digest, artifact.Size, "", bytes.NewReader(runtimeBytes)); e != nil {
		t.Fatal(e)
	}
	if e = c.ConfigureFiniteRuntimes(ctx, []contracts.FiniteRuntimeRef{ref}); e != nil {
		t.Fatal(e)
	}
	submission := contracts.Submission{SchemaVersion: 1, RequestID: "finite-study", RunID: "finite-run"}
	for _, id := range []string{"unit-a", "unit-b"} {
		unit := contracts.Unit{SchemaVersion: 1, UnitID: id, SpecDigest: contracts.Hash([]byte(id)), Runtime: "synthetic-v1", Operation: "synthetic-v1", Validator: "synthetic-v1"}
		submission.Units = append(submission.Units, unit)
		submission.Packages = append(submission.Packages, contracts.Package{SchemaVersion: 1, PackageID: "package-" + id, Units: []contracts.UnitRef{{UnitID: id, SpecDigest: unit.SpecDigest}}})
	}
	if e = operator.Do(ctx, "POST", "/v1/runs", submission, nil); e != nil {
		t.Fatal(e)
	}
	hostRoot := t.TempDir()
	cfg := agent.Config{Root: hostRoot, WorkerID: "worker", SessionID: "finite-session", HostID: "host", Client: worker, MaxArtifactBytes: 64 << 20, MaxConcurrent: 2}
	if _, e = agent.RegisterFiniteHost(ctx, cfg); e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	request := contracts.FiniteExportRequest{SchemaVersion: 1, RequestID: "seal-fixed-batch", BundleID: "registered-batch", WorkerID: "worker", SessionID: "finite-session", HostID: "host", MemberLimit: 2, StopNewUnitsAt: now.Add(time.Minute), TerminateAt: now.Add(2 * time.Minute), HandoffDeadline: now.Add(3 * time.Minute)}
	envelope, e := c.ExportFinite(ctx, request)
	if e != nil {
		t.Fatal(e)
	}
	if len(envelope.Bundle.Members) != 2 {
		t.Fatal("registered batch not multi-member", len(envelope.Bundle.Members))
	}
	source := t.TempDir()
	if e = os.Mkdir(filepath.Join(source, "blobs"), 0700); e != nil {
		t.Fatal(e)
	}
	body, _ := json.Marshal(envelope)
	if e = os.WriteFile(filepath.Join(source, "bundle.json"), body, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(source, "blobs", artifact.Digest), runtimeBytes, 0600); e != nil {
		t.Fatal(e)
	}
	cfg.Client = nil
	finite := agent.FiniteConfig{Config: cfg, BundleDirectory: source, Runtimes: []agent.FiniteRuntimeBinding{{Reference: ref, Argv: []string{binary, "-test.run=TestFiniteApplicationHelper", "--", "--finite-app"}}}}
	if e = agent.RunFinite(ctx, finite, envelope); e != nil {
		t.Fatal(e)
	}
	output := filepath.Join(hostRoot, "exchange", envelope.Bundle.BundleID)
	ack, e := c.ImportFiniteDirectory(ctx, output)
	if e != nil {
		t.Fatal(e)
	}
	if len(ack.Members) != 2 {
		t.Fatal("import omitted finite members", ack)
	}
	for _, member := range ack.Members {
		if len(member.RawPublished) != 1 || member.FinalDigest == "" {
			t.Fatal("raw/final import ACK incomplete", member)
		}
	}
	ackBody, _ := json.Marshal(ack)
	ackPath := filepath.Join(output, "ack.json")
	if e = os.WriteFile(ackPath, ackBody, 0600); e != nil {
		t.Fatal(e)
	}
	if e = agent.ApplyFiniteImportAck(hostRoot, ack, ackPath); e != nil {
		t.Fatal(e)
	}
	if e = agent.ApplyFiniteImportAck(hostRoot, ack); e != nil {
		t.Fatal("repeated genuine ACK failed", e)
	}
	again, e := c.ImportFiniteDirectory(ctx, output)
	if e != nil {
		t.Fatal("repeated retained import failed", e)
	}
	first, _ := contracts.Digest(ack)
	second, _ := contracts.Digest(again)
	if first != second {
		t.Fatal("idempotent import changed ACK")
	}
	if e = agent.RunFinite(ctx, finite, envelope); e != nil {
		t.Fatal("fully acknowledged batch attempted re-execution", e)
	}
}
