package controller

import (
	"bytes"
	"context"
	"errors"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func ingressConfig(t *testing.T, root string, p *contracts.ResourcePolicy) Config {
	t.Helper()
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	return Config{Root: root, Workers: []string{"local"}, ResourcePolicy: p, Runtime: "synthetic-v1", Validator: "synthetic-v1", Profile: "synthetic-local", ValidatorArgv: []string{exe}, MaxArtifactBytes: 1 << 20}
}
func ingressOpen(t *testing.T, cfg Config) *Controller {
	t.Helper()
	c, e := Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func ingressFixture(t *testing.T) (*Controller, Config) {
	t.Helper()
	cfg := ingressConfig(t, filepath.Join(t.TempDir(), "controller"), fixtureResourcePolicy([]string{"local"}))
	return ingressOpen(t, cfg), cfg
}
func ingressAssignment(t *testing.T, c *Controller, id string) contracts.Assignment {
	t.Helper()
	p := c.cfg.ResourcePolicy
	a := contracts.Assignment{SchemaVersion: 1, AssignmentID: id, RunID: id, WorkerID: "local", SessionID: "session", PackageDigest: contracts.Hash([]byte("package")), Runtime: c.cfg.Runtime, Profile: p.Profiles[0].ID, PolicyRevision: p.Revision, Allocation: p.Profiles[0].Allocation, Units: []contracts.UnitRef{{UnitID: "u", SpecDigest: contracts.Hash([]byte("u"))}}, StopNewUnitsAt: time.Now().Add(time.Minute), TerminateAt: time.Now().Add(2 * time.Minute), HandoffDeadline: time.Now().Add(3 * time.Minute)}
	tx, e := c.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if _, e = tx.Exec("INSERT INTO runs VALUES(?)", id); e != nil {
		t.Fatal(e)
	}
	if _, e = tx.Exec("INSERT INTO assignments(id,run_id,worker,session,manifest) VALUES(?,?,?,?,?)", id, id, a.WorkerID, a.SessionID, encoded(a)); e != nil {
		t.Fatal(e)
	}
	host, _ := approvalForWorker(*p, "local")
	if e = c.reserveAllocation(tx, host, a); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	return a
}
func ingressCount(t *testing.T, c *Controller, table string) int {
	t.Helper()
	var n int
	if e := c.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func ingressState(t *testing.T, c *Controller) (int64, contracts.ResourceVector) {
	t.Helper()
	tx, e := c.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	pending, held, e := ingressUsage(tx)
	if e != nil {
		t.Fatal(e)
	}
	return pending, held
}
func ingressPolicyChange(t *testing.T, c *Controller, p contracts.ResourcePolicy) {
	t.Helper()
	tx, e := c.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if e = c.installPolicy(tx, p); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
}

func TestIngressInvalidHeaderDoesNotChargeOrRead(t *testing.T) {
	c, _ := ingressFixture(t)
	for _, tc := range []struct {
		digest string
		size   int64
	}{{"bad", 1}, {strings.Repeat("A", 64), 1}, {contracts.Hash(nil), -1}, {contracts.Hash(nil), c.cfg.MaxArtifactBytes + 1}} {
		if e := c.PublishArtifact(context.Background(), tc.digest, tc.size, "", ingressNeverRead{}); !errors.Is(e, ErrInvalidArtifact) {
			t.Fatal(e)
		}
	}
	if ingressCount(t, c, "controller_ingress") != 0 || ingressCount(t, c, "ingress_transfers") != 0 {
		t.Fatal("invalid header charged")
	}
	if e := c.PublishArtifact(context.Background(), contracts.Hash([]byte("x")), 1, "unknown", ingressNeverRead{}); !errors.Is(e, ErrInvalidArtifact) {
		t.Fatal(e)
	}
}

type ingressNeverRead struct{}

func (ingressNeverRead) Read([]byte) (int, error) { panic("invalid/busy ingress read source") }

func TestIngressSlotsAtomicAndMetadataResponsive(t *testing.T) {
	c, _ := ingressFixture(t)
	ctx := context.Background()
	_, first, e := c.BeginArtifactTransfer(ctx, "")
	if e != nil {
		t.Fatal(e)
	}
	defer first()
	_, second, e := c.BeginArtifactTransfer(ctx, "")
	if e != nil {
		t.Fatal(e)
	}
	defer second()
	if _, unexpected, err := c.BeginArtifactTransfer(ctx, ""); !errors.Is(err, ErrControllerCapacity) {
		if unexpected != nil {
			unexpected()
		}
		t.Fatal(err)
	}
	if e = c.PublishArtifact(ctx, contracts.Hash([]byte("x")), 1, "", ingressNeverRead{}); !errors.Is(e, ErrControllerCapacity) {
		t.Fatal(e)
	}
	var n int
	if e = c.db.QueryRow("SELECT count(*) FROM runs").Scan(&n); e != nil {
		t.Fatal(e)
	}
	first()
	first()
	_, third, e := c.BeginArtifactTransfer(ctx, "")
	if e != nil {
		t.Fatal(e)
	}
	third()
	second()
	// A racing set of clients may admit only the policy limit, not all clients
	// observing len(channel) before anyone inserts its hold.
	var wg sync.WaitGroup
	releases := make(chan func(), 12)
	errs := make(chan error, 12)
	gate := make(chan struct{})
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			_, release, e := c.BeginArtifactTransfer(ctx, "")
			if e != nil {
				errs <- e
			} else {
				releases <- release
			}
		}()
	}
	close(gate)
	wg.Wait()
	close(releases)
	close(errs)
	if len(releases) != c.cfg.ResourcePolicy.TransferSlots {
		t.Fatal("non-atomic slot admission", len(releases))
	}
	for e := range errs {
		if !errors.Is(e, ErrControllerCapacity) {
			t.Error(e)
		}
	}
	for release := range releases {
		release()
	}
}

type ingressBlocking struct {
	once             sync.Once
	started, unblock chan struct{}
	body             io.Reader
}

func (r *ingressBlocking) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started); <-r.unblock })
	return r.body.Read(p)
}

type ingressClosable struct {
	*ingressBlocking
	closeOnce sync.Once
}

func (r *ingressClosable) Close() error { r.closeOnce.Do(func() { close(r.unblock) }); return nil }
func ingressUnblock(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
func ingressWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("ingress operation stalled")
	}
}

func TestIngressPendingCreditDedupAndTerminalCharge(t *testing.T) {
	c, _ := ingressFixture(t)
	a := ingressAssignment(t, c, "credit")
	data := []byte("result")
	digest := contracts.Hash(data)
	r := &ingressBlocking{started: make(chan struct{}), unblock: make(chan struct{}), body: bytes.NewReader(data)}
	done := make(chan error, 1)
	go func() { done <- c.PublishArtifact(context.Background(), digest, int64(len(data)), a.AssignmentID, r) }()
	defer ingressUnblock(r.unblock)
	ingressWait(t, r.started)
	pending, held := ingressState(t, c)
	if pending != 0 || held != c.cfg.ResourcePolicy.TransferReservation {
		t.Fatal(pending, held)
	}
	if _, e := c.db.Exec("UPDATE assignments SET final='{}' WHERE id=?", a.AssignmentID); e != nil {
		t.Fatal(e)
	}
	if pending, _ = ingressState(t, c); pending != int64(len(data)) {
		t.Fatal("terminal pending bytes remained credited", pending)
	}
	close(r.unblock)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if e := c.PublishArtifact(context.Background(), digest, int64(len(data)), a.AssignmentID, bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	if ingressCount(t, c, "artifacts") != 1 || ingressCount(t, c, "assignment_output_blobs") != 1 {
		t.Fatal("redelivery duplicated metadata")
	}
	for _, table := range []string{"controller_ingress", "ingress_sources", "ingress_assignment_blobs"} {
		if ingressCount(t, c, table) != 0 {
			t.Fatal("resolved publication remained in pending index", table)
		}
	}
	var got int64
	if e := c.db.QueryRow("SELECT bytes FROM assignment_output WHERE assignment_id=?", a.AssignmentID).Scan(&got); e != nil || got != int64(len(data)) {
		t.Fatal(got, e)
	}
	if e := c.PublishArtifact(context.Background(), digest, int64(len(data))+1, a.AssignmentID, ingressNeverRead{}); !errors.Is(e, ErrInvalidArtifact) {
		t.Fatal("size conflict", e)
	}
}
func TestIngressRetainedOutputCanExceedForecastAndUseLargerCurrentTransport(t *testing.T) {
	p := fixtureResourcePolicy([]string{"local"})
	p.Profiles[0].Allocation.Resources.SpoolBytes = 2
	p.TransferReservation.SpoolBytes = 1
	c := ingressOpen(t, ingressConfig(t, filepath.Join(t.TempDir(), "controller"), p))
	a := ingressAssignment(t, c, "old-small")
	data := []byte("larger")
	digest := contracts.Hash(data)
	if e := c.PublishArtifact(context.Background(), digest, int64(len(data)), a.AssignmentID, ingressNeverRead{}); !errors.Is(e, ErrControllerCapacity) {
		t.Fatal("small transport not retryable", e)
	}
	if ingressCount(t, c, "controller_ingress") != 0 {
		t.Fatal("unadmitted intent charged")
	}
	next := *p
	next.Revision = 2
	next.TransferReservation.SpoolBytes = 16
	ingressPolicyChange(t, c, next)
	r := &ingressBlocking{started: make(chan struct{}), unblock: make(chan struct{}), body: bytes.NewReader(data)}
	done := make(chan error, 1)
	go func() { done <- c.PublishArtifact(context.Background(), digest, int64(len(data)), a.AssignmentID, r) }()
	defer ingressUnblock(r.unblock)
	ingressWait(t, r.started)
	pending, _ := ingressState(t, c)
	if pending != int64(len(data))-2 {
		t.Fatal("excess pending forecast not fully charged", pending)
	}
	close(r.unblock)
	if e := <-done; e != nil {
		t.Fatal("forecast became hard output rejection", e)
	}
	var received int64
	if e := c.db.QueryRow("SELECT bytes FROM assignment_output WHERE assignment_id=?", a.AssignmentID).Scan(&received); e != nil || received != int64(len(data)) {
		t.Fatal(received, e)
	}
	var b []byte
	if e := c.db.QueryRow("SELECT manifest FROM assignments WHERE id=?", a.AssignmentID).Scan(&b); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(b, encoded(a)) {
		t.Fatal("old grant mutated")
	}
	if pending, _ := ingressState(t, c); pending != 0 {
		t.Fatal(pending)
	}
}
func TestIngressCorrectedUnverifiedSizeAfterRestart(t *testing.T) {
	c, cfg := ingressFixture(t)
	a := ingressAssignment(t, c, "corrected-size")
	data := []byte("bytes")
	digest := contracts.Hash(data)
	if e := c.PublishArtifact(context.Background(), digest, 7, a.AssignmentID, bytes.NewReader(data)); e == nil {
		t.Fatal("short body accepted")
	}
	if ingressCount(t, c, "artifacts") != 0 || ingressCount(t, c, "assignment_output_blobs") != 0 {
		t.Fatal("malformed body published metadata")
	}
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	c = ingressOpen(t, cfg)
	if e := c.PublishArtifact(context.Background(), digest, 5, a.AssignmentID, bytes.NewReader(data)); e != nil {
		t.Fatal("unverified header permanently bound size", e)
	}
	var size int64
	if e := c.db.QueryRow("SELECT size FROM artifacts WHERE digest=?", digest).Scan(&size); e != nil || size != 5 {
		t.Fatal(size, e)
	}
	if ingressCount(t, c, "controller_ingress") != 0 || ingressCount(t, c, "ingress_assignment_blobs") != 0 {
		t.Fatal("verified publication did not resolve max pending charge")
	}
}
func TestIngressPendingMaxChargeAndBoundSourceCredit(t *testing.T) {
	c, _ := ingressFixture(t)
	a := ingressAssignment(t, c, "source-a")
	b := ingressAssignment(t, c, "source-b")
	data := []byte("body")
	digest := contracts.Hash(data)
	for _, claim := range []struct {
		id   string
		size int64
	}{{a.AssignmentID, 2}, {b.AssignmentID, 7}} {
		if e := c.PublishArtifact(context.Background(), digest, claim.size, claim.id, bytes.NewReader(nil)); e == nil {
			t.Fatal("bad body accepted")
		}
	}
	var charged int64
	if e := c.db.QueryRow("SELECT size FROM controller_ingress WHERE digest=?", digest).Scan(&charged); e != nil || charged != 7 {
		t.Fatal("pending max charge lost", charged, e)
	}
	pending, _ := ingressState(t, c)
	if pending != 5 {
		t.Fatal("first source credited beyond its own two-byte claim", pending)
	}
	if e := c.PublishArtifact(context.Background(), digest, 4, a.AssignmentID, bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	if ingressCount(t, c, "ingress_assignment_blobs") != 0 {
		t.Fatal("verified digest did not clear every pending assignment claim")
	}
	if e := c.PublishArtifact(context.Background(), digest, 4, b.AssignmentID, bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	if ingressCount(t, c, "artifacts") != 1 || ingressCount(t, c, "assignment_output_blobs") != 2 {
		t.Fatal("verified shared digest accounting incorrect")
	}
}

type ingressCleanupDenied struct {
	dir  string
	body io.Reader
	once sync.Once
	err  error
}

func (r *ingressCleanupDenied) Read(p []byte) (int, error) {
	r.once.Do(func() { r.err = os.Chmod(r.dir, 0500) })
	if r.err != nil {
		return 0, r.err
	}
	return r.body.Read(p)
}
func TestIngressCleanupFailureRetainsTemporaryHoldUntilReopen(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses synthetic directory write denial; artifact package fault test covers cleanup failure")
	}
	c, cfg := ingressFixture(t)
	directory := filepath.Join(cfg.Root, "artifacts")
	defer os.Chmod(directory, 0700)
	reader := &ingressCleanupDenied{dir: directory, body: bytes.NewReader([]byte("raw"))}
	e := c.PublishArtifact(context.Background(), contracts.Hash([]byte("raw")), 3, "", reader)
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(e, artifacts.ErrIngressCleanup) {
		t.Fatal("cleanup failure lost typed boundary", e)
	}
	if ingressCount(t, c, "ingress_transfers") != 1 || len(c.transferSlots) != 1 {
		t.Fatal("unremoved scratch lost temporary reservation")
	}
	_, held := ingressState(t, c)
	if held != cfg.ResourcePolicy.TransferReservation {
		t.Fatal(held)
	}
	if ingressCount(t, c, "artifacts") != 0 || ingressCount(t, c, "controller_ingress") != 1 {
		t.Fatal("failed cleanup published catalog or lost pending intent")
	}
	left, e := filepath.Glob(filepath.Join(directory, ".ingress-*"))
	if e != nil || len(left) != 1 {
		t.Fatal(left, e)
	}
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	ingressWait(t, closed) // lifetime ends even though conservative row/slot remains.
	c = ingressOpen(t, cfg)
	if ingressCount(t, c, "ingress_transfers") != 0 {
		t.Fatal("reopened authority retained dead Go hold after successful scratch cleanup")
	}
	left, e = filepath.Glob(filepath.Join(directory, ".ingress-*"))
	if e != nil || len(left) != 0 {
		t.Fatal("exclusive startup did not clean reserved scratch", left, e)
	}
	if ingressCount(t, c, "controller_ingress") != 1 {
		t.Fatal("scratch cleanup incorrectly removed ambiguous permanent charge")
	}
}

func TestIngressOldHoldsSurvivePolicyShrink(t *testing.T) {
	c, _ := ingressFixture(t)
	p := *c.cfg.ResourcePolicy
	old := p.TransferReservation
	_, release, e := c.BeginArtifactTransfer(context.Background(), "")
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	p.Revision++
	p.TransferReservation.MemoryBytes = 1
	p.Hosts = append([]contracts.HostApproval(nil), p.Hosts...)
	p.Hosts[0].Limits.MemoryBytes = p.Hosts[0].Baseline.MemoryBytes + int64(p.ValidatorSlots)*p.ValidatorReservation.MemoryBytes + int64(p.TransferSlots)
	ingressPolicyChange(t, c, p)
	_, held := ingressState(t, c)
	if held != old {
		t.Fatal("active hold shrank", held, old)
	}
	if _, unexpected, err := c.BeginArtifactTransfer(context.Background(), ""); !errors.Is(err, ErrControllerCapacity) {
		if unexpected != nil {
			unexpected()
		}
		t.Fatal("old hold omitted from current admission", err)
	}
	release()
	_, last, e := c.BeginArtifactTransfer(context.Background(), "")
	if e != nil {
		t.Fatal(e)
	}
	last()
}
func TestIngressCloseCancelsClosableReadAndKeepsIntent(t *testing.T) {
	c, cfg := ingressFixture(t)
	r := &ingressClosable{ingressBlocking: &ingressBlocking{started: make(chan struct{}), unblock: make(chan struct{}), body: bytes.NewReader(nil)}}
	defer r.Close()
	done := make(chan error, 1)
	go func() { done <- c.PublishArtifact(context.Background(), contracts.Hash([]byte("blocked")), 7, "", r) }()
	ingressWait(t, r.started)
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	ingressWait(t, closed)
	if e := <-done; e == nil {
		t.Fatal("cancelled read accepted")
	}
	reopened := ingressOpen(t, cfg)
	if ingressCount(t, reopened, "artifacts") != 0 || ingressCount(t, reopened, "controller_ingress") != 1 || ingressCount(t, reopened, "ingress_transfers") != 0 {
		t.Fatal("cancellation lost conservative intent or retained dead hold")
	}
}
func TestIngressCloseJoinsIOBeforeAuthorityRelease(t *testing.T) {
	c, cfg := ingressFixture(t)
	r := &ingressBlocking{started: make(chan struct{}), unblock: make(chan struct{}), body: bytes.NewReader([]byte("x"))}
	defer ingressUnblock(r.unblock)
	done := make(chan error, 1)
	go func() { done <- c.PublishArtifact(context.Background(), contracts.Hash([]byte("x")), 1, "", r) }()
	ingressWait(t, r.started)
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("authority released while source IO alive")
	case <-time.After(50 * time.Millisecond):
	}
	if other, e := Open(cfg); e == nil {
		other.Close()
		t.Fatal("another authority admitted while write alive")
	}
	close(r.unblock)
	ingressWait(t, closed)
	if e := <-done; e == nil {
		t.Fatal("cancelled upload accepted")
	}
	reopened := ingressOpen(t, cfg)
	_ = reopened
}
func TestIngressGETReleaseIsIdempotentAndHonorsClose(t *testing.T) {
	c, _ := ingressFixture(t)
	derived, release, e := c.BeginArtifactTransfer(context.Background(), "")
	if e != nil {
		t.Fatal(e)
	}
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	ingressWait(t, derived.Done())
	select {
	case <-closed:
		t.Fatal("GET authority released before handler completion")
	default:
	}
	release()
	release()
	ingressWait(t, closed)
}
func TestIngressFinalTransactionFailureDoesNotPublishMetadata(t *testing.T) {
	c, _ := ingressFixture(t)
	a := ingressAssignment(t, c, "tx-fail")
	data := []byte("bytes")
	digest := contracts.Hash(data)
	if _, e := c.db.Exec(`CREATE TRIGGER ingress_test_fail BEFORE INSERT ON assignment_output_blobs BEGIN SELECT RAISE(ABORT,'injected output link fault'); END;`); e != nil {
		t.Fatal(e)
	}
	if e := c.PublishArtifact(context.Background(), digest, 5, a.AssignmentID, bytes.NewReader(data)); e == nil {
		t.Fatal("link transaction failure hidden")
	}
	if ingressCount(t, c, "artifacts") != 0 || ingressCount(t, c, "assignment_output_blobs") != 0 {
		t.Fatal("partial publication metadata committed")
	}
	if e := c.store.Contains(digest, 5); e != nil {
		t.Fatal("raw bytes missing", e)
	}
	if _, e := c.db.Exec("DROP TRIGGER ingress_test_fail"); e != nil {
		t.Fatal(e)
	}
	if e := c.PublishArtifact(context.Background(), digest, 5, a.AssignmentID, bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
}
func TestIngressCrashHelper(t *testing.T) {
	root := os.Getenv("HERMES_INGRESS_CRASH_ROOT")
	if root == "" {
		return
	}
	cfg := ingressConfig(t, root, fixtureResourcePolicy([]string{"local"}))
	c := ingressOpen(t, cfg)
	phase := os.Getenv("HERMES_INGRESS_CRASH_PHASE")
	c.testCommitBoundary = func(operation string, committed bool) error {
		if phase == operation+"/"+map[bool]string{false: "before", true: "after"}[committed] {
			os.Exit(77)
		}
		return nil
	}
	if e := c.PublishArtifact(context.Background(), contracts.Hash([]byte("bytes")), 5, "", bytes.NewReader([]byte("bytes"))); e != nil {
		t.Fatal(e)
	}
	t.Fatal("crash seam not reached")
}
func TestIngressProcessCrashPublicationBoundaries(t *testing.T) {
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	for _, phase := range []string{"artifact-intent/after", "artifact-publication/before", "artifact-publication/after"} {
		t.Run(phase, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "controller")
			child := exec.Command(exe, "-test.run=^TestIngressCrashHelper$")
			child.Env = append(os.Environ(), "HERMES_INGRESS_CRASH_ROOT="+root, "HERMES_INGRESS_CRASH_PHASE="+phase)
			out, e := child.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(e, &exited) || exited.ExitCode() != 77 {
				t.Fatal(string(out), e)
			}
			c := ingressOpen(t, ingressConfig(t, root, fixtureResourcePolicy([]string{"local"})))
			expected, pending := 0, 1
			if phase == "artifact-publication/after" {
				expected, pending = 1, 0
			}
			if ingressCount(t, c, "artifacts") != expected || ingressCount(t, c, "controller_ingress") != pending || ingressCount(t, c, "ingress_transfers") != 0 {
				t.Fatal("publication boundary recovery")
			}
			if expected == 1 {
				if e = c.store.Contains(contracts.Hash([]byte("bytes")), 5); e != nil {
					t.Fatal("metadata referenced missing bytes", e)
				}
			}
		})
	}
}
