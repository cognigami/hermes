package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"hermes/internal/contracts"
)

type workflowFixture struct {
	c            *Controller
	cfg          Config
	adapter      WorkflowAdapterConfig
	approved     approvedWorkflowAdapter
	registration contracts.WorkflowRegistration
	next         contracts.WorkflowNext
	decision     contracts.Artifact
}

func workflowGraph(id string) contracts.Submission {
	u := contracts.Unit{SchemaVersion: 1, UnitID: id + "-unit", SpecDigest: contracts.Hash([]byte(id)), Operation: "synthetic", Runtime: "synthetic-v1", Validator: "synthetic-v1"}
	return contracts.Submission{SchemaVersion: 1, RequestID: id + "-request", RunID: id, Units: []contracts.Unit{u}, Packages: []contracts.Package{{SchemaVersion: 1, PackageID: id + "-package", Units: []contracts.UnitRef{{UnitID: u.UnitID, SpecDigest: u.SpecDigest}}}}}
}
func workflowArtifact(t *testing.T, c *Controller, data []byte) contracts.Artifact {
	t.Helper()
	a := contracts.Artifact{Digest: contracts.Hash(data), Size: int64(len(data)), MediaType: "application/json"}
	if err := c.store.Publish(context.Background(), a.Digest, a.Size, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.Exec(`INSERT OR IGNORE INTO artifacts VALUES(?,?)`, a.Digest, a.Size); err != nil {
		t.Fatal(err)
	}
	return a
}
func newWorkflowFixture(t *testing.T, max *int64) *workflowFixture {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "staged-adapter")
	cmd := exec.Command("go", "build", "-o", binary, "./examples/staged-workflow")
	cmd.Dir = "../.."
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build example: %v %s", err, b)
	}
	executable, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	f := &workflowFixture{cfg: Config{ResourcePolicy: fixtureResourcePolicy([]string{"worker"}), Root: filepath.Join(dir, "controller"), Workers: []string{"worker"}, Profile: "synthetic-local", Runtime: "synthetic-v1", Validator: "synthetic-v1", ValidatorArgv: []string{"/usr/bin/true"}, MaxArtifactBytes: 1 << 20}}
	f.c, err = Open(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.c.Close() })
	if err = f.c.InstallWorkflows(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.adapter = WorkflowAdapterConfig{Contract: "staged-v1", RuntimeDigest: contracts.Hash(executable), Argv: []string{binary}, Timeout: time.Second, MaxMetadataBytes: contracts.MaxMetadataBytes, MaxLogBytes: contracts.MaxMessageBytes}
	f.approved, err = f.c.approveWorkflowAdapter(context.Background(), f.adapter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(f.approved.directory) })
	f.decision = workflowArtifact(t, f.c, []byte(`{"continue":true}`))
	prep := workflowArtifact(t, f.c, []byte(`{"prepared":"reusable"}`))
	next := workflowGraph("second")
	next.Units[0].Inputs = []contracts.Artifact{f.decision}
	next.Packages[0].Preparation = []contracts.Artifact{prep}
	f.next = contracts.WorkflowNext{Submission: next, DecisionUnit: next.Units[0].UnitID, DecisionPath: "decision.json"}
	settings := workflowArtifact(t, f.c, encoded(struct {
		Next *contracts.WorkflowNext `json:"next"`
	}{&f.next}))
	initial := workflowGraph("first")
	f.registration = contracts.WorkflowRegistration{SchemaVersion: 1, RequestID: "register", WorkflowID: "workflow", Contract: f.adapter.Contract, RuntimeDigest: f.adapter.RuntimeDigest, Settings: settings, Initial: initial, DecisionUnit: initial.Units[0].UnitID, DecisionPath: "decision.json", MaxSubmittedUnits: max}
	if _, err = f.c.RegisterWorkflow(context.Background(), f.registration, f.adapter); err != nil {
		t.Fatal(err)
	}
	return f
}
func workflowAccept(t *testing.T, f *workflowFixture, run string, decision contracts.Artifact) {
	t.Helper()
	var definition []byte
	if err := f.c.db.QueryRow(`SELECT d.definition FROM run_units u JOIN definitions d ON d.unit_id=u.unit_id WHERE u.run_id=?`, run).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	var u contracts.Unit
	if err := json.Unmarshal(definition, &u); err != nil {
		t.Fatal(err)
	}
	v := contracts.Candidate{SchemaVersion: 1, UnitID: u.UnitID, SpecDigest: u.SpecDigest, AssignmentID: run + "-assignment", AttemptID: run + "-assignment", Outcome: "success", Files: []contracts.ResultFile{{Path: "decision.json", Artifact: decision}}}
	digest, _ := contracts.Digest(v)
	tx, err := f.c.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	readyExec(t, tx, `INSERT INTO assignments(id,run_id,worker,session,manifest) VALUES(?,?,'worker','session',?)`, v.AssignmentID, run, encoded(contracts.Assignment{AssignmentID: v.AssignmentID, RunID: run, TerminateAt: time.Now().Add(time.Minute)}))
	readyExec(t, tx, `INSERT INTO owners VALUES(?,?,?)`, run, u.UnitID, v.AssignmentID)
	readyExec(t, tx, `INSERT INTO candidates(digest,run_id,unit_id,assignment_id,manifest,output_digest,state) VALUES(?,?,?,?,?,'output','valid')`, digest, run, u.UnitID, v.AssignmentID, encoded(v))
	readyExec(t, tx, `INSERT INTO acceptances VALUES(?,?,?,'synthetic-v1','{}')`, run, u.UnitID, digest)
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func workflowLoad(t *testing.T, c *Controller) workflowSnapshot {
	t.Helper()
	tx, err := c.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	x, err := workflowSnapshotFor(tx, "workflow")
	if err != nil {
		t.Fatal(err)
	}
	return x
}
func workflowCount(t *testing.T, c *Controller, q string) int {
	t.Helper()
	var n int
	if err := c.db.QueryRow(q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func TestWorkflowSuccessorPreservesHistoryAndAcceptedPrefix(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	ctx := context.Background()
	before, err := f.c.WorkflowStatus(ctx, "workflow")
	if err != nil {
		t.Fatal(err)
	}
	if err = f.c.advanceWorkflow(ctx, f.approved, "workflow"); err != nil {
		t.Fatal(err)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM runs`); n != 1 {
		t.Fatal(n)
	}
	workflowAccept(t, f, "first", f.decision)
	if err = f.c.advanceWorkflow(ctx, f.approved, "workflow"); err != nil {
		t.Fatal(err)
	}
	after, err := f.c.WorkflowStatus(ctx, "workflow")
	if err != nil {
		t.Fatal(err)
	}
	if after.Stage != 1 || after.SubmittedUnits != 2 || after.MaxSubmittedUnits != nil || !after.CreatedAt.Equal(before.CreatedAt) || after.Elapsed < before.Elapsed {
		t.Fatalf("history: %+v -> %+v", before, after)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM owners`); n != 1 {
		t.Fatalf("old ownership changed: %d", n)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM run_units WHERE run_id='second' AND unit_id='first-unit'`); n != 0 {
		t.Fatal("accepted prefix replayed")
	}
	var b []byte
	if err = f.c.db.QueryRow(`SELECT manifest FROM packages WHERE run_id='second'`).Scan(&b); err != nil {
		t.Fatal(err)
	}
	var p contracts.Package
	if err = json.Unmarshal(b, &p); err != nil || len(p.Preparation) != 1 {
		t.Fatalf("preparation not reused: %s %v", b, err)
	}
	// Historical payloads are not replayed by status or the next stage.
	if _, err = f.c.db.Exec(`UPDATE candidates SET manifest='INVALID HISTORICAL PAYLOAD' WHERE run_id='first'`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.c.WorkflowStatus(ctx, "workflow"); err != nil {
		t.Fatal(err)
	}
	// The second accepted boolean outcome prescribes a stop; old owners can remain.
	stop := workflowArtifact(t, f.c, []byte(`{"continue":false}`))
	workflowAccept(t, f, "second", stop)
	if err = f.c.advanceWorkflow(ctx, f.approved, "workflow"); err != nil {
		t.Fatal(err)
	}
	done, err := f.c.WorkflowStatus(ctx, "workflow")
	if err != nil || done.State != "completed" || done.SubmittedUnits != 2 {
		t.Fatalf("stop %+v %v", done, err)
	}
	// Acceptance-complete continuation can leave old owners; an explicit stop
	// still drains them without making a prescribed stop resumable.
	stopped, err := f.c.ControlWorkflow(ctx, contracts.WorkflowControl{SchemaVersion: 1, RequestID: "stop-completed", WorkflowID: "workflow", ExpectedRevision: done.Revision, Action: "stop", TerminateAt: time.Now().Add(time.Minute)})
	if err != nil || stopped.State != "completed" {
		t.Fatalf("stop completed: %+v %v", stopped, err)
	}
	if _, err = f.c.ControlWorkflow(ctx, contracts.WorkflowControl{SchemaVersion: 1, RequestID: "resume-completed", WorkflowID: "workflow", ExpectedRevision: stopped.Revision, Action: "resume"}); err == nil {
		t.Fatal("prescribed stop resumed")
	}
}
func TestWorkflowCrashIntentAndCreationReopen(t *testing.T) {
	for _, boundary := range []struct {
		operation string
		committed bool
	}{{"workflow-intent", false}, {"workflow-intent", true}, {"request", false}, {"request", true}} {
		t.Run(boundary.operation+map[bool]string{true: "-after", false: "-before"}[boundary.committed], func(t *testing.T) {
			f := newWorkflowFixture(t, nil)
			workflowAccept(t, f, "first", f.decision)
			injected := false
			f.c.testCommitBoundary = func(op string, committed bool) error {
				if !injected && op == boundary.operation && committed == boundary.committed {
					injected = true
					return errors.New("injected crash")
				}
				return nil
			}
			if err := f.c.advanceWorkflow(context.Background(), f.approved, "workflow"); err == nil || !injected {
				t.Fatalf("missing crash: %v", err)
			}
			f.c.testCommitBoundary = nil
			if err := f.c.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			f.c, err = Open(f.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.c.InstallWorkflows(context.Background()); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err = f.c.advanceWorkflow(context.Background(), f.approved, "workflow"); err != nil {
					t.Fatal(err)
				}
			}
			if n := workflowCount(t, f.c, `SELECT count(*) FROM runs`); n != 2 {
				t.Fatalf("runs=%d", n)
			}
			if n := workflowCount(t, f.c, `SELECT count(*) FROM workflow_intents`); n != 1 {
				t.Fatalf("intents=%d", n)
			}
			s, err := f.c.WorkflowStatus(context.Background(), "workflow")
			if err != nil || s.SubmittedUnits != 2 {
				t.Fatalf("usage %+v %v", s, err)
			}
		})
	}
}
func TestWorkflowStopRacesAndResumeDoNotReopenRuns(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	ctx := context.Background()
	workflowAccept(t, f, "first", f.decision)
	snapshot := workflowLoad(t, f.c)
	output := contracts.WorkflowAdapterOutput{SchemaVersion: 1, Next: &f.next}
	tx, err := f.c.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err = persistWorkflowIntent(tx, snapshot, output); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	snapshot = workflowLoad(t, f.c)
	stop := contracts.WorkflowControl{SchemaVersion: 1, RequestID: "stop", WorkflowID: "workflow", ExpectedRevision: 1, Action: "stop", TerminateAt: time.Now().Add(time.Minute)}
	s, err := f.c.ControlWorkflow(ctx, stop)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.c.createWorkflowSuccessor(ctx, snapshot); err == nil {
		t.Fatal("stopped workflow created successor")
	}
	if _, err = f.c.ControlWorkflow(ctx, stop); err != nil {
		t.Fatal("stop replay", err)
	}
	if err = f.c.Close(); err != nil {
		t.Fatal(err)
	}
	f.c, err = Open(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.c.advanceWorkflow(ctx, f.approved, "workflow"); err != nil {
		t.Fatal(err)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM runs`); n != 1 {
		t.Fatal(n)
	}
	resume := contracts.WorkflowControl{SchemaVersion: 1, RequestID: "resume", WorkflowID: "workflow", ExpectedRevision: s.Revision, Action: "resume"}
	if _, err = f.c.ControlWorkflow(ctx, resume); err != nil {
		t.Fatal(err)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM target_controls WHERE kind='run' AND target='first' AND action='drain'`); n != 1 {
		t.Fatal("resume reopened old run")
	}
	if err = f.c.advanceWorkflow(ctx, f.approved, "workflow"); err != nil {
		t.Fatal(err)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM runs`); n != 2 {
		t.Fatal(n)
	}
}
func TestWorkflowStopDuringDecisionRejectsIntent(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	workflowAccept(t, f, "first", f.decision)
	x := workflowLoad(t, f.c)
	_, err := f.c.ControlWorkflow(context.Background(), contracts.WorkflowControl{SchemaVersion: 1, RequestID: "stop", WorkflowID: "workflow", ExpectedRevision: 1, Action: "stop", TerminateAt: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.c.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = persistWorkflowIntent(tx, x, contracts.WorkflowAdapterOutput{SchemaVersion: 1, Next: &f.next}); err == nil {
		t.Fatal("stale adapter result persisted after stop")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM workflow_intents`); n != 0 {
		t.Fatalf("stopped intent count=%d", n)
	}
}

func TestWorkflowAllowanceReplayAndIntentConflicts(t *testing.T) {
	limit := int64(1)
	f := newWorkflowFixture(t, &limit)
	ctx := context.Background()
	if _, err := f.c.RegisterWorkflow(ctx, f.registration, f.adapter); err != nil {
		t.Fatal(err)
	}
	changed := f.registration
	changed.Contract = "other"
	if _, err := f.c.RegisterWorkflow(ctx, changed, f.adapter); err == nil {
		t.Fatal("wrong contract allowed")
	}
	changed = f.registration
	changed.DecisionPath = "other.json"
	if _, err := f.c.RegisterWorkflow(ctx, changed, f.adapter); err == nil {
		t.Fatal("request body conflict allowed")
	}
	workflowAccept(t, f, "first", f.decision)
	if err := f.c.advanceWorkflow(ctx, f.approved, "workflow"); err == nil {
		t.Fatal("allowance overrun")
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM runs`); n != 1 {
		t.Fatal(n)
	}
	x := workflowLoad(t, f.c)
	if x.output == nil {
		t.Fatal("intention not durable before failed creation")
	}
	tx, err := f.c.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = persistWorkflowIntent(tx, x, *x.output); err == nil {
		t.Fatal("duplicate intent allowed")
	}
}
func TestWorkflowCanonicalMetadataAndMissingFiles(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	workflowAccept(t, f, "first", f.decision)
	tx, err := f.c.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	readyExec(t, tx, `UPDATE workflow_stages SET decision_path='not-declared.json'`)
	if _, err = workflowSnapshotFor(tx, "workflow"); err == nil {
		t.Fatal("arbitrary decision path accepted")
	}
	readyExec(t, tx, `UPDATE workflow_stages SET decision_path='decision.json'`)
	readyExec(t, tx, `UPDATE acceptances SET validator='wrong'`)
	if _, err = workflowSnapshotFor(tx, "workflow"); err == nil {
		t.Fatal("wrong validator accepted")
	}
	readyExec(t, tx, `UPDATE acceptances SET validator='synthetic-v1'`)
	readyExec(t, tx, `UPDATE candidates SET manifest=replace(CAST(manifest AS TEXT),'"spec_digest":"','"spec_digest":"bad')`)
	if _, err = workflowSnapshotFor(tx, "workflow"); err == nil {
		t.Fatal("wrong spec accepted")
	}
}
