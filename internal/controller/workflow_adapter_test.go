package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hermes/internal/contracts"

	"golang.org/x/sys/unix"
)

func TestWorkflowAdapterRejectsFIFOWithoutWriter(t *testing.T) {
	root := t.TempDir()
	fifo := filepath.Join(root, "mistaken-executable")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	c := &Controller{cfg: Config{Root: root}}
	cfg := WorkflowAdapterConfig{Contract: "staged-v1", RuntimeDigest: contracts.Hash(nil), Argv: []string{fifo}, Timeout: time.Second, MaxMetadataBytes: contracts.MaxMetadataBytes, MaxLogBytes: contracts.MaxMessageBytes}
	done := make(chan error, 1)
	go func() { _, err := c.approveWorkflowAdapter(context.Background(), cfg); done <- err }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "regular file") {
			t.Fatalf("FIFO rejection: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("adapter approval blocked on FIFO without writer")
	}
}

func workflowFillRetainedCapacity(t *testing.T, c *Controller) {
	t.Helper()
	ctx := context.Background()
	tx, err := c.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.policy(tx)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	host, ok := approvalForHost(p, p.ControllerHostID)
	if !ok {
		tx.Rollback()
		t.Fatal("no controller host")
	}
	used, limit, err := c.hostUsage(tx, p, host, -1)
	if err == nil {
		err = c.addOutputReservations(tx, &used)
	}
	tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ReserveControllerStorage(ctx, "synthetic-capacity-blocker", limit.SpoolBytes-used.SpoolBytes); err != nil {
		t.Fatal(err)
	}
}
func TestWorkflowAdapterCopyCapacityAndStartupWaiting(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	workflowFillRetainedCapacity(t, f.c)
	before := workflowCount(t, f.c, `SELECT count(*) FROM controller_storage`)
	entries, err := os.ReadDir(f.cfg.Root)
	if err != nil {
		t.Fatal(err)
	}
	images := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "workflow-runtime-") {
			images++
		}
	}
	if _, err = f.c.approveWorkflowAdapter(context.Background(), f.adapter); !errors.Is(err, ErrControllerCapacity) {
		t.Fatalf("copy capacity: %v", err)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM controller_storage`); n != before {
		t.Fatalf("refused copy charged/wrote: %d -> %d", before, n)
	}
	entries, err = os.ReadDir(f.cfg.Root)
	if err != nil {
		t.Fatal(err)
	}
	after := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "workflow-runtime-") {
			after++
		}
	}
	if after != images {
		t.Fatalf("unadmitted runtime directory created: %d -> %d", images, after)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err = f.c.RunWorkflows(ctx, f.adapter); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup capacity wait exited permanently: %v", err)
	}
}
func TestWorkflowAdapterPerPathScratchAndCapacityWaitAttempts(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	workflowAccept(t, f, "first", f.decision)
	input := workflowLoad(t, f.c).input
	tx, err := f.c.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.c.policy(tx)
	tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	// Identical digests still require two actual staged files.
	large := input
	large.Decision.Size = p.ValidatorReservation.SpoolBytes/2 + 1
	large.Settings = large.Decision
	if _, err = f.c.invokeWorkflowAdapter(context.Background(), f.approved, large); !errors.Is(err, ErrControllerCapacity) {
		t.Fatalf("per-path bytes undercharged: %v", err)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM validator_claims WHERE exited=0`); n != 0 {
		t.Fatalf("unadmitted helper claim: %d", n)
	}
	for i := 0; i < p.ValidatorSlots; i++ {
		claim, err := f.c.ReserveControllerChild(context.Background(), "continuation", "occupied", contracts.ResourceVector{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { claim.CancelBeforeStart(context.Background()) })
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err = f.c.RunWorkflows(ctx, f.adapter); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("helper capacity wait: %v", err)
	}
	if n := workflowCount(t, f.c, `SELECT adapter_attempts FROM workflows WHERE id='workflow'`); n != 0 {
		t.Fatalf("capacity consumed adapter attempts: %d", n)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM controller_storage WHERE claim_id IS NOT NULL`); n != 0 {
		t.Fatalf("unadmitted scratch charged: %d", n)
	}
}
func TestWorkflowAdapterRuntimeCopyChargeSurvivesReopen(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	relative := filepath.Base(f.approved.directory)
	if err := f.c.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.c, err = Open(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	var charged int64
	if err = f.c.db.QueryRow(`SELECT size FROM controller_storage WHERE path=? AND claim_id IS NULL`, relative).Scan(&charged); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(f.approved.executable)
	if err != nil || charged != info.Size() {
		t.Fatalf("runtime retention charge %d: %v", charged, err)
	}
	// A failed hash also retains its accounted private copy; there is no GC.
	cfg := f.adapter
	cfg.RuntimeDigest = contracts.Hash([]byte("wrong"))
	before := workflowCount(t, f.c, `SELECT count(*) FROM controller_storage WHERE claim_id IS NULL`)
	bad, err := f.c.approveWorkflowAdapter(context.Background(), cfg)
	if err == nil {
		t.Fatal("bad hash accepted")
	}
	if _, err = os.Stat(bad.executable); err != nil {
		t.Fatal("failed private copy not retained", err)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM controller_storage WHERE claim_id IS NULL`); n != before+1 {
		t.Fatal("failed copy lost charge")
	}
}
func TestWorkflowCopyNeverExceedsReservation(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.WriteFile(source, []byte("source-grew"), 0600); err != nil {
		t.Fatal(err)
	}
	src, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	dst, err := os.Create(filepath.Join(root, "copy"))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if err = copyWorkflowFile(context.Background(), dst, src, 3, nil); err == nil {
		t.Fatal("growing source accepted")
	}
	info, err := dst.Stat()
	if err != nil || info.Size() != 3 {
		t.Fatalf("copy exceeded admitted bytes: %v %v", info, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = copyWorkflowFile(ctx, dst, src, 3, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled copy", err)
	}
}

func TestWorkflowAdapterProtectedCopyAndApproval(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	workflowAccept(t, f, "first", f.decision)
	input := workflowLoad(t, f.c).input
	// Changing the deployment source after boundary verification cannot change
	// what this approved loop executes. Status never reads either executable.
	if err := os.WriteFile(f.adapter.Argv[0], []byte("replaced deployment source"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.invokeWorkflowAdapter(context.Background(), f.approved, input); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.approveWorkflowAdapter(context.Background(), f.adapter); err == nil {
		t.Fatal("changed executable approved")
	}
	input.Contract = "wrong"
	if _, err := f.c.invokeWorkflowAdapter(context.Background(), f.approved, input); err == nil {
		t.Fatal("wrong contract invoked")
	}
	input = workflowLoad(t, f.c).input
	input.RuntimeDigest = contracts.Hash([]byte("wrong"))
	if _, err := f.c.invokeWorkflowAdapter(context.Background(), f.approved, input); err == nil {
		t.Fatal("wrong runtime invoked")
	}
}
func TestWorkflowAdapterMissingArtifactTimeoutAndMalformedVerdict(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	workflowAccept(t, f, "first", f.decision)
	input := workflowLoad(t, f.c).input
	a := f.approved
	a.cfg.Timeout = time.Nanosecond
	if _, err := f.c.invokeWorkflowAdapter(context.Background(), a, input); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
	a = f.approved
	a.cfg.MaxMetadataBytes = 1
	if _, err := f.c.invokeWorkflowAdapter(context.Background(), a, input); err == nil {
		t.Fatal("input limit ignored")
	}
	// Missing boolean must be an operational error, never a prescribed stop.
	input.Decision = workflowArtifact(t, f.c, []byte(`{}`))
	if _, err := f.c.invokeWorkflowAdapter(context.Background(), f.approved, input); err == nil {
		t.Fatal("missing decision became stop")
	}
	input = workflowLoad(t, f.c).input
	file, err := f.c.store.Open(input.Decision.Digest)
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	file.Close()
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err = f.c.invokeWorkflowAdapter(context.Background(), f.approved, input); err == nil {
		t.Fatal("missing artifact became stop")
	}
	if s, err := f.c.WorkflowStatus(context.Background(), "workflow"); err != nil || s.State != "active" {
		t.Fatalf("operational error changed state: %+v %v", s, err)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM validator_claims WHERE exited=0`); n != 0 {
		t.Fatalf("known no-start/exit leaked claim: %d", n)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM controller_storage WHERE claim_id IS NOT NULL`); n != 0 {
		t.Fatalf("known no-start/exit leaked scratch: %d", n)
	}
}

// Re-executed test binary exercises the actual strict process protocol without
// shell commands or a second adapter framework.
func TestWorkflowAdapterProtocolHelper(t *testing.T) {
	marker := -1
	for i, a := range os.Args {
		if a == "--workflow-helper" {
			marker = i
		}
	}
	if marker < 0 {
		return
	}
	if os.Getenv("HERMES_TEST_SECRET") != "" {
		os.Exit(8)
	}
	switch os.Args[marker+1] {
	case "malformed":
		os.Stdout.WriteString(`{"schema_version":1`)
	case "missing":
		os.Stdout.WriteString(`{"schema_version":1}`)
	case "both":
		os.Stdout.WriteString(`{"schema_version":1,"stop_reason":"stop","next":{}}`)
	case "output":
		os.Stdout.WriteString(strings.Repeat("x", contracts.MaxMetadataBytes+1))
	case "log":
		os.Stderr.WriteString(strings.Repeat("x", contracts.MaxMessageBytes+1))
	case "clean":
		os.Stdout.WriteString(`{"schema_version":1,"stop_reason":"clean"}`)
	}
	os.Exit(0)
}
func TestWorkflowAdapterStrictOutputLogsAndEnvironment(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	workflowAccept(t, f, "first", f.decision)
	input := workflowLoad(t, f.c).input
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	cfg := f.adapter
	cfg.Argv = []string{binary, "-test.run=^TestWorkflowAdapterProtocolHelper$", "--", "--workflow-helper", "clean"}
	cfg.RuntimeDigest = contracts.Hash(data)
	approved, err := f.c.approveWorkflowAdapter(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	input.RuntimeDigest = cfg.RuntimeDigest
	t.Setenv("HERMES_TEST_SECRET", "must-not-inherit")
	if out, err := f.c.invokeWorkflowAdapter(context.Background(), approved, input); err != nil || out.StopReason != "clean" {
		t.Fatalf("clean invocation: %+v %v", out, err)
	}
	for _, mode := range []string{"malformed", "missing", "both", "output", "log"} {
		t.Run(mode, func(t *testing.T) {
			a := approved
			a.cfg.Argv = append([]string(nil), approved.cfg.Argv...)
			a.cfg.Argv[len(a.cfg.Argv)-1] = mode
			if _, err := f.c.invokeWorkflowAdapter(context.Background(), a, input); err == nil {
				t.Fatal("invalid adapter protocol accepted")
			}
		})
	}
}
