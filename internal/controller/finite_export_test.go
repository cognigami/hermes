package controller

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

type finiteFixture struct {
	ca, cert, key    string
	c                *Controller
	cfg              Config
	operator, worker *agent.Client
	native           agent.Config
	ref              contracts.FiniteRuntimeRef
	request          contracts.FiniteExportRequest
}

func newFiniteFixture(t *testing.T) *finiteFixture {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("actual native Darwin registration required")
	}
	p := fixtureResourcePolicy([]string{"local"})
	p.Hosts = p.Hosts[:1]
	p.Hosts[0].WorkerID = "local"
	p.Hosts[0].Limits.CPUMilli = 1250
	cfg := ingressConfig(t, filepath.Join(t.TempDir(), "controller"), p)
	c := ingressOpen(t, cfg)
	dir := t.TempDir()
	_, ca, signer, caPath, _ := certificate(t, dir, "finite-ca", nil, nil)
	serverPair, _, _, _, _ := certificate(t, dir, "server", ca, signer)
	_, _, _, oc, ok := certificate(t, dir, "operator", ca, signer)
	_, _, _, wc, wk := certificate(t, dir, "worker:local", ca, signer)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	server := httptest.NewUnstartedServer(c.Handler())
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverPair}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	t.Cleanup(server.Close)
	operator, e := agent.NewClient(server.URL, caPath, oc, ok)
	if e != nil {
		t.Fatal(e)
	}
	worker, e := agent.NewClient(server.URL, caPath, wc, wk)
	if e != nil {
		t.Fatal(e)
	}
	native := agent.Config{Root: filepath.Join(t.TempDir(), "native"), WorkerID: "local", SessionID: "finite-native-session", HostID: "controller", MaxArtifactBytes: 1 << 20, MaxConcurrent: 2, Client: worker}
	_, e = agent.RegisterFiniteHost(context.Background(), native)
	if e != nil {
		t.Fatal(e)
	}
	s := submission("finite-run")
	s.Units = s.Units[:2]
	s.Packages = s.Packages[:1]
	second := s.Packages[0]
	second.PackageID = "second"
	second.Units = append([]contracts.UnitRef(nil), second.Units[1:]...)
	s.Packages[0].Units = s.Packages[0].Units[:1]
	s.Packages = append(s.Packages, second)
	if e = operator.Do(context.Background(), "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	data := []byte("controller metadata-only approved executable fixture")
	artifact := contracts.Artifact{Digest: contracts.Hash(data), Size: int64(len(data)), MediaType: "application/octet-stream"}
	if e = c.PublishArtifact(context.Background(), artifact.Digest, artifact.Size, "", bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	baseline, _ := finiteBaseline(runtime.GOARCH)
	ref := contracts.FiniteRuntimeRef{Runtime: cfg.Runtime, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, ISABaseline: baseline, Executable: artifact}
	if e = c.ConfigureFiniteRuntimes(context.Background(), []contracts.FiniteRuntimeRef{ref}); e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC()
	request := contracts.FiniteExportRequest{SchemaVersion: 1, RequestID: "finite-export", BundleID: "finite-bundle", WorkerID: "local", SessionID: native.SessionID, HostID: "controller", MemberLimit: 2, StopNewUnitsAt: now.Add(time.Minute), TerminateAt: now.Add(2 * time.Minute), HandoffDeadline: now.Add(3 * time.Minute)}
	return &finiteFixture{c: c, cfg: cfg, operator: operator, worker: worker, native: native, ref: ref, request: request, ca: caPath, cert: oc, key: ok}
}
func (f *finiteFixture) seal(t *testing.T) contracts.FiniteBundleEnvelope {
	t.Helper()
	var env contracts.FiniteBundleEnvelope
	if e := f.operator.Do(context.Background(), "POST", "/v1/finite/export", f.request, &env); e != nil {
		t.Fatal(e)
	}
	if e := env.Validate(); e != nil {
		t.Fatal(e)
	}
	return env
}
func TestFiniteExportAtomicPinnedBatchAndRetry(t *testing.T) {
	f := newFiniteFixture(t)
	env := f.seal(t)
	if len(env.Bundle.Members) != 2 || env.Bundle.ExportRevision < 1 {
		t.Fatal("not a fixed multi-member intent", env)
	}
	for _, table := range []string{"assignments", "owners", "allocation_reservations", "finite_members"} {
		if ingressCount(t, f.c, table) != 2 {
			t.Fatal(table)
		}
	}
	for _, m := range env.Bundle.Members {
		var receipt int
		if e := f.c.db.QueryRow("SELECT receipt FROM assignments WHERE id=?", m.Work.Assignment.AssignmentID).Scan(&receipt); e != nil || receipt != 0 {
			t.Fatal("fabricated online receipt", receipt, e)
		}
		if m.Work.Assignment.StopNewUnitsAt.After(f.request.StopNewUnitsAt) {
			t.Fatal("start deadline extended")
		}
	}
	reservationControl(t, f.c, "worker", "local", "drain", 0)
	retry, e := f.c.ExportFinite(context.Background(), f.request)
	if e != nil || !bytes.Equal(encoded(retry), encoded(env)) {
		t.Fatal("retry changed sealed authority", e)
	}
	if e = f.c.ConfigureFiniteRuntimes(context.Background(), nil); e != nil {
		t.Fatal(e)
	}
	stored, e := f.c.FiniteBundle(context.Background(), f.request.BundleID)
	if e != nil || !bytes.Equal(encoded(stored), encoded(env)) {
		t.Fatal("disabled config rewrote old intent", e)
	}
	changed := f.request
	changed.MemberLimit = 1
	if _, e = f.c.ExportFinite(context.Background(), changed); e == nil {
		t.Fatal("request-ID body conflict accepted")
	}
	changed.RequestID = "new-export"
	changed.BundleID = "new-bundle"
	if _, e = f.c.ExportFinite(context.Background(), changed); e == nil {
		t.Fatal("active batch got new authority")
	}
}
func TestFiniteExportFreshNativeAndPausedGate(t *testing.T) {
	f := newFiniteFixture(t)
	reservationControl(t, f.c, "worker", "local", "drain", 0)
	if _, e := f.c.ExportFinite(context.Background(), f.request); !errors.Is(e, ErrFiniteUnavailable) {
		t.Fatal(e)
	}
	if ingressCount(t, f.c, "assignments") != 0 || ingressCount(t, f.c, "finite_exports") != 0 {
		t.Fatal("failed seal left ownership")
	}
	reservationControl(t, f.c, "worker", "local", "resume", 1)
	f.seal(t)
}
func TestFiniteRuntimeApprovalDoesNotRestoreOnRestart(t *testing.T) {
	f := newFiniteFixture(t)
	f.c.Close()
	c, e := Open(f.cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if _, e = c.ExportFinite(context.Background(), f.request); !errors.Is(e, ErrFiniteUnavailable) {
		t.Fatal("persisted approval resurrected", e)
	}
	if e = c.ConfigureFiniteRuntimes(context.Background(), []contracts.FiniteRuntimeRef{f.ref, f.ref}); !errors.Is(e, ErrFiniteInvalid) {
		t.Fatal("ambiguous bindings accepted", e)
	}
	missing := f.ref
	missing.Executable.Digest = contracts.Hash([]byte("missing"))
	if e = c.ConfigureFiniteRuntimes(context.Background(), []contracts.FiniteRuntimeRef{missing}); e == nil {
		t.Fatal("unverified catalog reference accepted")
	}
	if e = c.ConfigureFiniteRuntimes(context.Background(), []contracts.FiniteRuntimeRef{f.ref}); e != nil {
		t.Fatal(e)
	}
	if _, e = c.ExportFinite(context.Background(), f.request); e != nil {
		t.Fatal(e)
	}
}
func TestFiniteRegistrationTotalsGuardAndLegacyBoundary(t *testing.T) {
	f := newFiniteFixture(t)
	var body []byte
	if e := f.c.db.QueryRow("SELECT manifest FROM host_observations WHERE host_id='controller'").Scan(&body); e != nil {
		t.Fatal(e)
	}
	var original contracts.WorkerRegistration
	if e := json.Unmarshal(body, &original); e != nil {
		t.Fatal(e)
	}
	for _, n := range []int64{-1, 0} {
		r := original
		r.RequestID = "bad-total-" + agent.ID()
		r.RetainedSpoolBytes = 1
		r.TotalSpoolBytes = &n
		if e := f.worker.Do(context.Background(), "POST", "/v1/register", r, nil); e == nil {
			t.Fatal("invalid total accepted", n)
		}
		var after []byte
		f.c.db.QueryRow("SELECT manifest FROM host_observations WHERE host_id='controller'").Scan(&after)
		if !bytes.Equal(body, after) {
			t.Fatal("invalid report persisted")
		}
	}
	legacy := original
	legacy.RequestID = "legacy-native"
	legacy.TotalSpoolBytes = nil
	var legacyBudget contracts.HostBudget
	if e := f.worker.Do(context.Background(), "POST", "/v1/register", legacy, &legacyBudget); e != nil {
		t.Fatal("legacy online registration rejected", e)
	}
	if _, e := f.c.ExportFinite(context.Background(), f.request); !errors.Is(e, ErrFiniteUnavailable) {
		t.Fatal("nil full total admitted", e)
	}
	if _, e := agent.RegisterFiniteHost(context.Background(), f.native); e != nil {
		t.Fatal(e)
	}
	f.seal(t)
}
func TestFiniteHTTPRejectsWorkerOperatorRoute(t *testing.T) {
	f := newFiniteFixture(t)
	if e := f.worker.Do(context.Background(), "POST", "/v1/finite/export", f.request, nil); e == nil {
		t.Fatal("worker got operator authority")
	}
	if ingressCount(t, f.c, "assignments") != 0 {
		t.Fatal("unauthorized issuance")
	}
}
