package controller

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
	"encoding/pem"
	"errors"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"hermes/testdata/synthetic"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fixture struct {
	c                       *Controller
	cfg                     Config
	server                  *httptest.Server
	operator, worker        *agent.Client
	other                   *agent.Client
	ca, cert, key, app, cli string
	cancel                  context.CancelFunc
	done                    chan error
	stop                    func()
	stopValidation          func()
}

func certificate(t *testing.T, dir, name string, ca *x509.Certificate, signer *ecdsa.PrivateKey) (tls.Certificate, *x509.Certificate, *ecdsa.PrivateKey, string, string) {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	serial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if e != nil {
		t.Fatal(e)
	}
	cert := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	if ca == nil {
		cert.IsCA = true
		cert.BasicConstraintsValid = true
		cert.KeyUsage |= x509.KeyUsageCertSign
		ca = cert
		signer = key
	}
	if name == "server" {
		cert.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		cert.DNSNames = []string{"localhost"}
	}
	der, e := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, signer)
	if e != nil {
		t.Fatal(e)
	}
	parsed, e := x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	keyDER, e := x509.MarshalECPrivateKey(key)
	if e != nil {
		t.Fatal(e)
	}
	cp, kp := filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	cb, kb := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if e = os.WriteFile(cp, cb, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(kp, kb, 0600); e != nil {
		t.Fatal(e)
	}
	pair, e := tls.X509KeyPair(cb, kb)
	if e != nil {
		t.Fatal(e)
	}
	return pair, parsed, key, cp, kp
}
func setup(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	root, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	for _, cmd := range []string{"hermes-synthetic", "hermes-synthetic-validator", "hermes", "hermes-controller", "hermes-agent"} {
		p := exec.Command("go", "build", "-o", filepath.Join(dir, cmd), "./cmd/"+cmd)
		p.Dir = root
		p.Env = append(os.Environ(), "CGO_ENABLED=0")
		if b, e := p.CombinedOutput(); e != nil {
			t.Fatal(e, string(b))
		}
	}
	_, ca, signer, caPath, _ := certificate(t, dir, "ca", nil, nil)
	pair, _, _, _, _ := certificate(t, dir, "server", ca, signer)
	_, _, _, oc, ok := certificate(t, dir, "operator", ca, signer)
	_, _, _, wc, wk := certificate(t, dir, "worker:local", ca, signer)
	_, _, _, otherCert, otherKey := certificate(t, dir, "worker:other", ca, signer)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	cfg := Config{Root: filepath.Join(dir, "controller"), Workers: []string{"local", "other"}, Profile: "synthetic-local", Runtime: "synthetic-v1", Validator: "synthetic-v1", ValidatorArgv: []string{filepath.Join(dir, "hermes-synthetic-validator")}, MaxArtifactBytes: 1 << 20}
	c, e := Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	server := httptest.NewUnstartedServer(c.Handler())
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}
	server.StartTLS()
	op, e := agent.NewClient(server.URL, caPath, oc, ok)
	if e != nil {
		t.Fatal(e)
	}
	worker, e := agent.NewClient(server.URL, caPath, wc, wk)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.RunValidators(ctx) }()
	other, e := agent.NewClient(server.URL, caPath, otherCert, otherKey)
	if e != nil {
		t.Fatal(e)
	}
	f := &fixture{c: c, cfg: cfg, server: server, operator: op, worker: worker, other: other, ca: caPath, cert: oc, key: ok, app: filepath.Join(dir, "hermes-synthetic"), cli: filepath.Join(dir, "hermes"), cancel: cancel, done: done}
	var once, validationOnce sync.Once
	f.stopValidation = func() { validationOnce.Do(func() { cancel(); <-done }) }
	f.stop = func() { once.Do(func() { f.stopValidation(); server.Close(); c.Close() }) }
	t.Cleanup(f.stop)
	return f
}
func submission(id string) contracts.Submission {
	u := contracts.Unit{SchemaVersion: 1, UnitID: "a", SpecDigest: contracts.Hash([]byte("a")), Operation: "synthetic-v1", Runtime: "synthetic-v1", Validator: "synthetic-v1"}
	b := u
	b.UnitID = "b"
	b.SpecDigest = contracts.Hash([]byte("b"))
	d := u
	d.UnitID = "c"
	d.SpecDigest = contracts.Hash([]byte("c"))
	d.Dependencies = []string{"a", "b"}
	return contracts.Submission{SchemaVersion: 1, RequestID: "submit-" + id, RunID: id, Units: []contracts.Unit{u, b, d}, Packages: []contracts.Package{{SchemaVersion: 1, PackageID: "first", Units: []contracts.UnitRef{{UnitID: u.UnitID, SpecDigest: u.SpecDigest}, {UnitID: b.UnitID, SpecDigest: b.SpecDigest}}}, {SchemaVersion: 1, PackageID: "second", Units: []contracts.UnitRef{{UnitID: d.UnitID, SpecDigest: d.SpecDigest}}}}}
}
func await(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if fn() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out")
		case <-tick.C:
		}
	}
}

type loseReplies struct {
	base http.RoundTripper
	mu   sync.Mutex
	lost map[string]bool
}

func (l *loseReplies) RoundTrip(r *http.Request) (*http.Response, error) {
	response, e := l.base.RoundTrip(r)
	if e != nil {
		return nil, e
	}
	l.mu.Lock()
	drop := !l.lost[r.URL.Path] && (r.URL.Path == "/v1/poll" || r.URL.Path == "/v1/receipt" || r.URL.Path == "/v1/candidate" || r.URL.Path == "/v1/control/ack")
	if drop {
		l.lost[r.URL.Path] = true
	}
	l.mu.Unlock()
	if drop {
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		return nil, errors.New("synthetic lost reply after commit")
	}
	return response, nil
}

type blockPublication struct {
	base    http.RoundTripper
	blocked atomic.Bool
}

func (b *blockPublication) RoundTrip(r *http.Request) (*http.Response, error) {
	if b.blocked.Load() && (r.Method == "PUT" || r.URL.Path == "/v1/candidate" || r.URL.Path == "/v1/receipt") {
		return nil, errors.New("synthetic delivery outage")
	}
	return b.base.RoundTrip(r)
}

func TestConnectedAcceptance(t *testing.T) {
	f := setup(t)
	if other, e := Open(f.cfg); e == nil {
		other.Close()
		t.Fatal("second authority acquired lock")
	}
	s := submission("run")
	input := []byte("supplied synthetic input")
	prep := []byte("reusable synthetic preparation")
	inputRef := contracts.Artifact{Digest: contracts.Hash(input), Size: int64(len(input)), MediaType: "application/octet-stream"}
	prepRef := contracts.Artifact{Digest: contracts.Hash(prep), Size: int64(len(prep)), MediaType: "application/octet-stream"}
	for _, item := range []struct {
		a contracts.Artifact
		b []byte
	}{{inputRef, input}, {prepRef, prep}} {
		if e := f.operator.Publish(context.Background(), item.a.Digest, item.a.Size, "", bytes.NewReader(item.b)); e != nil {
			t.Fatal(e)
		}
	}
	s.Units[0].Inputs = []contracts.Artifact{inputRef}
	s.Packages[0].Preparation = []contracts.Artifact{prepRef}
	path := filepath.Join(t.TempDir(), "submission.json")
	if e := os.WriteFile(path, encoded(s), 0600); e != nil {
		t.Fatal(e)
	}
	cli := exec.Command(f.cli, "--url", f.server.URL, "--ca", f.ca, "--cert", f.cert, "--key", f.key, "submit", path)
	if b, e := cli.CombinedOutput(); e != nil {
		t.Fatal(e, string(b))
	} // CLI is gone before any execution.
	if e := f.operator.Do(context.Background(), "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal("lost submit reply replay", e)
	}
	changed := s
	changed.RunID = "other"
	if e := f.operator.Do(context.Background(), "POST", "/v1/runs", changed, nil); e == nil {
		t.Fatal("request ID reuse accepted")
	}
	changed = submission("another")
	changed.Units[0].Operation = "different-scientific-operation"
	if e := f.operator.Do(context.Background(), "POST", "/v1/runs", changed, nil); e == nil {
		t.Fatal("global unit meaning changed across runs")
	}
	loss := &loseReplies{base: f.worker.HTTP.Transport, lost: map[string]bool{}}
	block := &blockPublication{base: loss}
	block.blocked.Store(true)
	f.worker.HTTP.Transport = block
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	spool := filepath.Join(t.TempDir(), "agent")
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relativeSpool, err := filepath.Rel(cwd, spool)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.IsAbs(relativeSpool) {
		t.Fatal("regression must use relative spool path")
	}
	go func() {
		done <- agent.Run(ctx, agent.Config{Root: relativeSpool, WorkerID: "local", SessionID: "session", Runtime: "synthetic-v1", Profile: "synthetic-local", RuntimeArgv: []string{f.app}, Client: f.worker, MaxArtifactBytes: 1 << 20})
	}()
	defer func() {
		cancel()
		if e := <-done; !errors.Is(e, context.Canceled) {
			t.Errorf("agent exit: %v", e)
		}
	}()
	// Both independent units must finish and spool despite receipt/publication outage.
	await(t, func() bool {
		b, e := os.ReadFile(filepath.Join(spool, "journal.json"))
		if e != nil {
			return false
		}
		var j struct {
			Quiescent  bool                  `json:"quiescent"`
			Candidates []contracts.Candidate `json:"candidates"`
		}
		return json.Unmarshal(b, &j) == nil && j.Quiescent && len(j.Candidates) == 2
	})
	block.blocked.Store(false)
	await(t, func() bool {
		v, e := f.c.Status(context.Background(), "run")
		return e == nil && v.Accepted == 3 && v.Quiescent
	})
	for _, ref := range []contracts.Artifact{inputRef, prepRef} {
		matches, e := filepath.Glob(filepath.Join(spool, "assignment-*", "inputs", ref.Digest))
		if e != nil || len(matches) != 1 {
			t.Fatal("input/preparation not staged", matches, e)
		}
		data, e := os.ReadFile(matches[0])
		if e != nil || contracts.Hash(data) != ref.Digest {
			t.Fatal("staging changed bytes", e)
		}
	}
	var assignments, candidates int
	if e := f.c.db.QueryRow("SELECT COUNT(*) FROM assignments").Scan(&assignments); e != nil {
		t.Fatal(e)
	}
	if e := f.c.db.QueryRow("SELECT COUNT(*) FROM candidates").Scan(&candidates); e != nil {
		t.Fatal(e)
	}
	if assignments != 2 || candidates != 3 {
		t.Fatal("duplicate execution/publication", assignments, candidates)
	}
	loss.mu.Lock()
	if len(loss.lost) != 3 {
		t.Error("ack loss paths not exercised")
	}
	loss.mu.Unlock()
	// Status and accepted recovery must not reopen payloads.
	if e := os.Rename(filepath.Join(f.cfg.Root, "artifacts"), filepath.Join(f.cfg.Root, "unavailable-blobs")); e != nil {
		t.Fatal(e)
	}
	v, e := f.c.Status(context.Background(), "run")
	if e != nil || v.Accepted != 3 {
		t.Fatal(v, e)
	}
}
func TestValidationBoundary(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	s := submission("manual")
	s.Units = s.Units[:1]
	s.Packages = s.Packages[:1]
	s.Packages[0].Units = s.Packages[0].Units[:1]
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	var work contracts.Work
	p := contracts.PollRequest{SchemaVersion: 1, RequestID: "poll", WorkerID: "local", SessionID: "session"}
	if e := f.worker.Do(ctx, "POST", "/v1/poll", p, &work); e != nil {
		t.Fatal(e)
	}
	a := work.Assignment
	lostSpoolPoll := p
	lostSpoolPoll.RequestID = "fresh-root-poll"
	if e := f.worker.Do(ctx, "POST", "/v1/poll", lostSpoolPoll, nil); e == nil {
		t.Fatal("fresh poll authorized replay without retained journal")
	}
	r := contracts.Receipt{SchemaVersion: 1, RequestID: "receipt", AssignmentID: a.AssignmentID, WorkerID: a.WorkerID, SessionID: a.SessionID}
	if e := f.worker.Do(ctx, "POST", "/v1/receipt", r, nil); e != nil {
		t.Fatal(e)
	}
	send := func(kind string, publish bool) error {
		u := s.Units[0]
		value := synthetic.Expected(u.UnitID, u.SpecDigest)
		if kind == "reject" {
			value = "incorrect"
		}
		data := encoded(synthetic.Result{UnitID: u.UnitID, SpecDigest: u.SpecDigest, Value: value})
		if kind == "second-valid" {
			data = append(data, '\n')
		}
		artifact := contracts.Artifact{Digest: contracts.Hash(data), Size: int64(len(data)), MediaType: "application/json"}
		if publish {
			if e := f.worker.Publish(ctx, artifact.Digest, artifact.Size, a.AssignmentID, bytes.NewReader(data)); e != nil {
				return e
			}
		}
		outcome := "complete"
		if kind == "operational" {
			outcome = "validator_error"
		}
		candidate := contracts.Candidate{SchemaVersion: 1, UnitID: u.UnitID, SpecDigest: u.SpecDigest, AssignmentID: a.AssignmentID, AttemptID: a.AssignmentID, Outcome: outcome, Files: []contracts.ResultFile{{Path: "result.json", Artifact: artifact}}}
		receipt := r
		receipt.RequestID = kind
		return f.worker.Do(ctx, "POST", "/v1/candidate", contracts.CandidateReport{Receipt: receipt, Candidate: candidate}, nil)
	}
	if e := send("missing", false); e == nil {
		t.Fatal("accepted missing durable bytes")
	}
	if e := send("reject", true); e != nil {
		t.Fatal(e)
	}
	if e := send("operational", true); e != nil {
		t.Fatal(e)
	}
	await(t, func() bool {
		var n int
		f.c.db.QueryRow("SELECT COUNT(*) FROM candidates WHERE state IN('rejected','operational_failed')").Scan(&n)
		return n == 2
	})
	status, e := f.c.Status(ctx, "manual")
	if e != nil || status.Accepted != 0 {
		t.Fatal(status, e)
	}
	var rejected string
	if e = f.c.db.QueryRow("SELECT digest FROM candidates WHERE state='rejected' LIMIT 1").Scan(&rejected); e != nil {
		t.Fatal(e)
	}
	if _, e = f.c.db.Exec("INSERT INTO acceptances VALUES(?,?,?,?,?)", "manual", s.Units[0].UnitID, rejected, "synthetic-v1", encoded(contracts.Verdict{SchemaVersion: 1, Valid: true})); e == nil {
		t.Fatal("SQL accepted candidate without application-valid state")
	}
	if e = send("valid", true); e != nil {
		t.Fatal(e)
	}
	if e = send("valid", true); e != nil {
		t.Fatal("redelivery", e)
	}
	if e = send("second-valid", true); e != nil {
		t.Fatal(e)
	}
	await(t, func() bool {
		var n int
		f.c.db.QueryRow("SELECT COUNT(*) FROM candidates WHERE state='valid'").Scan(&n)
		return n == 2
	})
	status, e = f.c.Status(ctx, "manual")
	if e != nil || status.Accepted != 1 || status.Conflicts == 0 || status.Quiescent {
		t.Fatal(status, e)
	}
	// Acceptance doesn't erase still-live ownership.
	var owners int
	f.c.db.QueryRow("SELECT COUNT(*) FROM owners").Scan(&owners)
	if owners != 1 {
		t.Fatal("accepted unit lost owner")
	}
	if e = f.operator.Do(ctx, "POST", "/v1/poll", p, nil); e == nil {
		t.Fatal("operator used worker API")
	}
	bad := r
	bad.SessionID = "wrong"
	bad.RequestID = "wrong-session"
	if e = f.worker.Do(ctx, "POST", "/v1/receipt", bad, nil); e == nil {
		t.Fatal("session mismatch")
	}
	for _, body := range []string{`{"schema_version":2}`, strings.Repeat(" ", contracts.MaxMetadataBytes+1)} {
		req, _ := http.NewRequest("POST", f.server.URL+"/v1/poll", strings.NewReader(body))
		resp, e := f.worker.HTTP.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		resp.Body.Close()
		if resp.StatusCode == 200 {
			t.Fatal("malformed request accepted")
		}
	}
}
