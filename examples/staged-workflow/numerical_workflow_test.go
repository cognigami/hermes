package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"hermes/internal/agent"
	"hermes/internal/contracts"
	"hermes/internal/controller"
	numerical "hermes/testdata/numerical-workflow"
)

type numericalHarness struct {
	t                                                           *testing.T
	ctx                                                         context.Context
	cfg                                                         controller.Config
	approval                                                    controller.WorkflowAdapterConfig
	c                                                           *controller.Controller
	server                                                      *httptest.Server
	op, worker                                                  *agent.Client
	pair                                                        tls.Certificate
	caPool                                                      *x509.CertPool
	ca, opCert, opKey, workerCert, workerKey, agentRoot, binary string
	cancel                                                      context.CancelFunc
	joined                                                      []<-chan error
	registration                                                contracts.WorkflowRegistration
}

func numericalCertificate(t *testing.T, root, name string, ca *x509.Certificate, key *ecdsa.PrivateKey) (tls.Certificate, *x509.Certificate, *ecdsa.PrivateKey, string, string) {
	t.Helper()
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	if ca == nil {
		cert.IsCA = true
		cert.BasicConstraintsValid = true
		cert.KeyUsage |= x509.KeyUsageCertSign
		ca = cert
		key = private
	}
	if name == "server" {
		cert.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca, &private.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	cp, kp := filepath.Join(root, name+".pem"), filepath.Join(root, name+"-key.pem")
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	secret := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err = os.WriteFile(cp, certificate, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(kp, secret, 0600); err != nil {
		t.Fatal(err)
	}
	identity, err := tls.X509KeyPair(certificate, secret)
	if err != nil {
		t.Fatal(err)
	}
	return identity, parsed, private, cp, kp
}
func numericalPolicy() *contracts.ResourcePolicy {
	return &contracts.ResourcePolicy{SchemaVersion: 1, Revision: 1, ControllerHostID: "numerical-host", Hosts: []contracts.HostApproval{{HostID: "numerical-host", WorkerID: "worker", Limits: contracts.ResourceVector{CPUMilli: 4000, MemoryBytes: 512 << 20, SpoolBytes: 256 << 20}, Baseline: contracts.ResourceVector{CPUMilli: 125, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}, MaxAssignments: 1}}, Profiles: []contracts.ResourceProfile{{ID: "numerical-leaf", Conservative: true, Allocation: contracts.Allocation{Resources: contracts.ResourceVector{CPUMilli: 250, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}}}}, TransferSlots: 1, ValidatorSlots: 2, TransferReservation: contracts.ResourceVector{CPUMilli: 100, MemoryBytes: 4 << 20, SpoolBytes: 1 << 20}, ValidatorReservation: contracts.ResourceVector{CPUMilli: 100, MemoryBytes: 64 << 20, SpoolBytes: 4 << 20}}
}
func newNumericalHarness(t *testing.T, binary string, max *int64) *numericalHarness {
	t.Helper()
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	n := &numericalHarness{t: t, ctx: ctx, binary: binary, agentRoot: filepath.Join(root, "agent")}
	n.cfg = controller.Config{Root: filepath.Join(root, "controller"), Workers: []string{"worker"}, ResourcePolicy: numericalPolicy(), Runtime: numerical.Contract, Validator: numerical.Contract, Profile: "numerical-leaf", ValidatorArgv: []string{binary, "validate"}, MaxArtifactBytes: 1 << 20}
	executable, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	n.approval = controller.WorkflowAdapterConfig{Contract: numerical.Contract, RuntimeDigest: contracts.Hash(executable), Argv: []string{binary, "decide"}, Timeout: 2 * time.Second, MaxMetadataBytes: contracts.MaxMetadataBytes, MaxLogBytes: contracts.MaxMessageBytes}
	_, ca, key, caPath, _ := numericalCertificate(t, root, "ca", nil, nil)
	n.ca = caPath
	n.pair, _, _, _, _ = numericalCertificate(t, root, "server", ca, key)
	_, _, _, n.opCert, n.opKey = numericalCertificate(t, root, "operator", ca, key)
	_, _, _, n.workerCert, n.workerKey = numericalCertificate(t, root, "worker:worker", ca, key)
	n.caPool = x509.NewCertPool()
	n.caPool.AddCert(ca)
	n.start(false)
	t.Cleanup(n.stop)
	settings := numerical.Settings{WorkflowID: "numerical"}
	for i, values := range [][]int64{{1, 2, 3}, {4, 5}, {6}} {
		settings.Plans = append(settings.Plans, n.publish(numerical.Encode(numerical.Plan{SchemaVersion: 1, Stage: i, Stages: 3, Values: values})))
	}
	settingsRef := n.publish(numerical.Encode(settings))
	initial, err := numerical.Stage(settings, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.registration = contracts.WorkflowRegistration{SchemaVersion: 1, RequestID: "numerical-register", WorkflowID: settings.WorkflowID, Contract: numerical.Contract, RuntimeDigest: n.approval.RuntimeDigest, Settings: settingsRef, Initial: initial.Submission, DecisionUnit: initial.DecisionUnit, DecisionPath: initial.DecisionPath, MaxSubmittedUnits: max}
	var status contracts.WorkflowStatus
	if err = n.op.Do(n.ctx, "POST", "/v1/workflows", n.registration, &status); err != nil {
		t.Fatal(err)
	}
	return n
}
func (n *numericalHarness) start(continuation bool) {
	n.t.Helper()
	var err error
	n.c, err = controller.Open(n.cfg)
	if err != nil {
		n.t.Fatal(err)
	}
	if err = n.c.InstallWorkflows(n.ctx); err != nil {
		n.t.Fatal(err)
	}
	n.server = httptest.NewUnstartedServer(n.c.HandlerWithWorkflows(&n.approval))
	n.server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{n.pair}, ClientCAs: n.caPool, ClientAuth: tls.RequireAndVerifyClientCert}
	n.server.StartTLS()
	n.op, err = agent.NewClient(n.server.URL, n.ca, n.opCert, n.opKey)
	if err != nil {
		n.t.Fatal(err)
	}
	n.worker, err = agent.NewClient(n.server.URL, n.ca, n.workerCert, n.workerKey)
	if err != nil {
		n.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(n.ctx)
	n.cancel = cancel
	n.joined = nil
	launch := func(run func() error) {
		done := make(chan error, 1)
		n.joined = append(n.joined, done)
		go func() { done <- run() }()
	}
	launch(func() error { return n.c.RunValidators(ctx) })
	if continuation {
		launch(func() error { return n.c.RunWorkflows(ctx, n.approval) })
	}
	launch(func() error {
		return agent.Run(ctx, agent.Config{Root: n.agentRoot, WorkerID: "worker", SessionID: "numerical-session", HostID: "numerical-host", Runtime: numerical.Contract, Profile: "numerical-leaf", RuntimeArgv: []string{n.binary, "runtime"}, Client: n.worker, MaxArtifactBytes: 1 << 20, MaxConcurrent: 1})
	})
}
func (n *numericalHarness) stop() {
	n.t.Helper()
	if n.cancel == nil {
		return
	}
	n.cancel()
	for _, done := range n.joined {
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				n.t.Errorf("joined numerical service: %v", err)
			}
		case <-time.After(5 * time.Second):
			n.t.Error("numerical service failed bounded join")
		}
	}
	n.server.Close()
	if err := n.c.Close(); err != nil {
		n.t.Error(err)
	}
	n.op.HTTP.CloseIdleConnections()
	n.worker.HTTP.CloseIdleConnections()
	n.cancel = nil
}
func (n *numericalHarness) publish(b []byte) contracts.Artifact {
	n.t.Helper()
	ref := numerical.Artifact(b)
	if err := n.op.Publish(n.ctx, ref.Digest, ref.Size, "", bytes.NewReader(b)); err != nil {
		n.t.Fatal(err)
	}
	return ref
}
func (n *numericalHarness) status() contracts.WorkflowStatus {
	n.t.Helper()
	var s contracts.WorkflowStatus
	if err := n.op.Do(n.ctx, "GET", "/v1/workflows/status?workflow=numerical", nil, &s); err != nil {
		n.t.Fatal(err)
	}
	return s
}
func (n *numericalHarness) await(condition func() bool) {
	n.t.Helper()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-n.ctx.Done():
			n.t.Fatal(n.ctx.Err())
		case <-timer.C:
			n.t.Fatalf("numerical progress timeout: %+v", n.status())
		case <-ticker.C:
		}
	}
}
func (n *numericalHarness) initialAccepted() {
	n.await(func() bool {
		var s contracts.Status
		return n.op.Do(n.ctx, "GET", "/v1/status?run=numerical-stage-0", nil, &s) == nil && s.Accepted == 1 && s.Quiescent
	})
}
func (n *numericalHarness) control(action string) contracts.WorkflowStatus {
	n.t.Helper()
	s := n.status()
	r := contracts.WorkflowControl{SchemaVersion: 1, RequestID: fmt.Sprintf("numerical-%s-%d", action, s.Revision), WorkflowID: "numerical", ExpectedRevision: s.Revision, Action: action}
	if action == "stop" {
		r.TerminateAt = time.Now().Add(time.Second)
	}
	if err := n.op.Do(n.ctx, "POST", "/v1/workflows/control", r, &s); err != nil {
		n.t.Fatal(err)
	}
	return s
}
func (n *numericalHarness) metadata(fn func(*sql.DB)) {
	n.t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(n.cfg.Root, "metadata.sqlite")+"?mode=ro&_pragma=query_only(1)")
	if err != nil {
		n.t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	fn(db)
}
func (n *numericalHarness) verifyAcceptedStages(stages int) {
	n.t.Helper()
	expected := []int64{14, 55, 91}
	prior := ""
	n.metadata(func(db *sql.DB) {
		var units, attempts, accepted int
		for q, dst := range map[string]*int{`SELECT count(*) FROM run_units`: &units, `SELECT sum(attempts) FROM unit_attempts`: &attempts, `SELECT count(*) FROM acceptances`: &accepted} {
			if err := db.QueryRow(q).Scan(dst); err != nil {
				n.t.Fatal(err)
			}
		}
		if units != stages || attempts != stages || accepted != stages {
			n.t.Fatalf("prefix recomputation/false acceptance: units=%d attempts=%d accepted=%d", units, attempts, accepted)
		}
		for i := 0; i < stages; i++ {
			var manifest, definition []byte
			var validator, state string
			err := db.QueryRow(`SELECT c.manifest,d.definition,a.validator,c.state FROM acceptances a JOIN candidates c ON c.digest=a.candidate JOIN definitions d ON d.unit_id=a.unit_id WHERE a.run_id=?`, fmt.Sprintf("numerical-stage-%d", i)).Scan(&manifest, &definition, &validator, &state)
			if err != nil {
				n.t.Fatal(err)
			}
			if validator != numerical.Contract || state != "valid" {
				n.t.Fatal("not actual domain acceptance", validator, state)
			}
			var candidate contracts.Candidate
			var unit contracts.Unit
			if err = json.Unmarshal(manifest, &candidate); err != nil {
				n.t.Fatal(err)
			}
			if err = json.Unmarshal(definition, &unit); err != nil {
				n.t.Fatal(err)
			}
			if i > 0 && (len(unit.Inputs) != 2 || unit.Inputs[1].Digest != prior) {
				n.t.Fatal("successor did not reference original accepted artifact")
			}
			found := false
			for _, f := range candidate.Files {
				if f.Path != "decision.json" {
					continue
				}
				found = true
				prior = f.Artifact.Digest
				request, err := http.NewRequestWithContext(n.ctx, "GET", n.op.URL+"/v1/artifacts/"+prior, nil)
				if err != nil {
					n.t.Fatal(err)
				}
				response, err := n.op.HTTP.Do(request)
				if err != nil {
					n.t.Fatal(err)
				}
				var result numerical.Result
				err = contracts.Decode(response.Body, &result)
				response.Body.Close()
				if response.StatusCode != 200 || err != nil || result.Value != expected[i] || result.Stage != i {
					n.t.Fatalf("stage%d mathematical result %+v %v", i, result, err)
				}
			}
			if !found {
				n.t.Fatal("canonical numerical decision missing")
			}
		}
	})
}
func TestNumericalWorkflowRealThreeStageContinuation(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native resource qualification exists only for Darwin")
	}
	binary := filepath.Join(t.TempDir(), "numerical-fixture")
	build := exec.Command("go", "build", "-o", binary, "./testdata/numerical-workflow/cmd")
	build.Dir = "../.."
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("fixture build: %v %s", err, output)
	}
	t.Run("restart-explicit-stop-replay-and-prescribed-completion", func(t *testing.T) {
		n := newNumericalHarness(t, binary, nil)
		n.initialAccepted()
		original := n.status()
		n.control("stop")
		n.stop()
		n.start(true)
		n.await(func() bool {
			s, err := n.c.ResourceStatus(n.ctx, "numerical-host")
			return err == nil && s.RetainedCopiesBytes > 0
		})
		stopped := n.status()
		if stopped.State != "stopped" || stopped.Stage != 0 || stopped.SubmittedUnits != 1 || !stopped.CreatedAt.Equal(original.CreatedAt) {
			t.Fatalf("restart lost stop/history: %+v", stopped)
		}
		n.control("resume")
		n.await(func() bool { return n.status().State == "completed" })
		done := n.status()
		if done.Stage != 2 || done.SubmittedUnits != 3 || done.MaxSubmittedUnits != nil || !done.CreatedAt.Equal(original.CreatedAt) || done.Elapsed < original.Elapsed || done.Reason != "sum of squares complete: 91" {
			t.Fatalf("continuation/history: %+v", done)
		}
		n.await(func() bool {
			var s contracts.Status
			return n.op.Do(n.ctx, "GET", "/v1/status?run=numerical-stage-2", nil, &s) == nil && s.Quiescent
		})
		n.verifyAcceptedStages(3)
		var replay contracts.WorkflowStatus
		if err := n.op.Do(n.ctx, "POST", "/v1/workflows", n.registration, &replay); err != nil {
			t.Fatal("registration replay", err)
		}
		n.stop()
		n.start(true)
		if s := n.status(); s.State != "completed" || s.SubmittedUnits != 3 || !s.CreatedAt.Equal(original.CreatedAt) {
			t.Fatalf("completion restart: %+v", s)
		}
		n.verifyAcceptedStages(3)
	})
	t.Run("configured-cumulative-unit-allowance", func(t *testing.T) {
		limit := int64(2)
		n := newNumericalHarness(t, binary, &limit)
		n.initialAccepted()
		original := n.status()
		n.stop()
		n.start(true)
		n.await(func() bool {
			s := n.status()
			return s.SubmittedUnits == 2 && strings.Contains(s.Reason, "allowance exhausted")
		})
		n.verifyAcceptedStages(2)
		type intentSnapshot struct {
			requestID     string
			input, output []byte
		}
		readIntent := func() intentSnapshot {
			var snapshot intentSnapshot
			n.metadata(func(db *sql.DB) {
				var count int
				if err := db.QueryRow(`SELECT count(*) FROM workflow_intents WHERE workflow_id='numerical' AND from_stage=1`).Scan(&count); err != nil || count != 1 {
					t.Fatalf("expected exactly one pending third-stage intent: count=%d err=%v", count, err)
				}
				var created sql.NullString
				if err := db.QueryRow(`SELECT request_id,input,output,created_run FROM workflow_intents WHERE workflow_id='numerical' AND from_stage=1`).Scan(&snapshot.requestID, &snapshot.input, &snapshot.output, &created); err != nil {
					t.Fatal(err)
				}
				if created.Valid || snapshot.requestID == "" {
					t.Fatalf("third-stage intent unexpectedly created or unbound: %+v", created)
				}
				var decision contracts.WorkflowAdapterOutput
				if err := contracts.Decode(bytes.NewReader(snapshot.output), &decision); err != nil || decision.Next == nil || decision.Next.Submission.RunID != "numerical-stage-2" {
					t.Fatalf("wrong pending numerical successor: %+v %v", decision, err)
				}
			})
			return snapshot
		}
		beforeIntent := readIntent()
		n.control("stop")
		n.stop()
		n.start(true)
		s := n.status()
		if s.State != "stopped" || s.SubmittedUnits != 2 || s.MaxSubmittedUnits == nil || *s.MaxSubmittedUnits != 2 || !s.CreatedAt.Equal(original.CreatedAt) {
			t.Fatalf("allowance/stop reset by restart: %+v", s)
		}
		n.verifyAcceptedStages(2)
		afterIntent := readIntent()
		if beforeIntent.requestID != afterIntent.requestID || !bytes.Equal(beforeIntent.input, afterIntent.input) || !bytes.Equal(beforeIntent.output, afterIntent.output) {
			t.Fatal("stop/restart changed the durable uncreated third-stage intent")
		}
	})
}
