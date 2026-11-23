package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type agentSnapshot struct {
	ReceiptAcknowledged bool                        `json:"receipt_acknowledged"`
	ProcessStarted      bool                        `json:"process_started"`
	Work                contracts.Work              `json:"work"`
	Started             []string                    `json:"started"`
	Candidates          []contracts.Candidate       `json:"candidates"`
	Quiescent           bool                        `json:"quiescent"`
	FinalSent           bool                        `json:"final_sent"`
	Control             contracts.AssignmentControl `json:"control"`
}

func snapshot(root string) agentSnapshot {
	var s agentSnapshot
	paths, _ := filepath.Glob(filepath.Join(root, "active", "*", "journal.json"))
	archived, _ := filepath.Glob(filepath.Join(root, "completed", "*", "journal.json"))
	paths = append(paths, archived...)
	paths = append(paths, filepath.Join(root, "journal.json"))
	var latest time.Time
	var selected string
	for _, path := range paths {
		if info, e := os.Stat(path); e == nil && (selected == "" || info.ModTime().After(latest)) {
			latest = info.ModTime()
			selected = path
		}
	}
	b, e := os.ReadFile(selected)
	if e == nil {
		json.Unmarshal(b, &s)
	}
	return s
}
func workspaceFiles(root string, suffix ...string) ([]string, error) {
	var result []string
	for _, base := range []string{"active", "completed"} {
		parts := []string{root, base, "*", "work"}
		parts = append(parts, suffix...)
		paths, e := filepath.Glob(filepath.Join(parts...))
		if e != nil {
			return nil, e
		}
		result = append(result, paths...)
	}
	return result, nil
}

func launchAgent(t *testing.T, f *fixture, client *agent.Client, worker, root string, delay int) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- agent.Run(ctx, agent.Config{Root: root, WorkerID: worker, SessionID: "session-" + worker, Runtime: "synthetic-v1", Profile: "synthetic-local", RuntimeArgv: []string{f.app, "--delay-millis", fmt.Sprint(delay)}, Client: client, MaxArtifactBytes: 1 << 20, ShutdownGrace: 50 * time.Millisecond})
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			e := <-done
			if e != nil && !errors.Is(e, context.Canceled) {
				t.Errorf("agent %s: %v", worker, e)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

type countPolls struct {
	base http.RoundTripper
	n    atomic.Int64
}

func (c *countPolls) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == "/v1/poll" {
		c.n.Add(1)
	}
	return c.base.RoundTrip(r)
}
func independentSubmission() contracts.Submission {
	s := oneSubmission("workers")
	base := s.Units[0]
	s.Units = nil
	s.Packages = nil
	for i := 0; i < 4; i++ {
		u := base
		u.UnitID = fmt.Sprintf("unit-%d", i)
		u.SpecDigest = contracts.Hash([]byte(u.UnitID))
		s.Units = append(s.Units, u)
	}
	for i := 0; i < 2; i++ {
		p := contracts.Package{SchemaVersion: 1, PackageID: fmt.Sprint(i)}
		for _, u := range s.Units[i*2 : i*2+2] {
			p.Units = append(p.Units, contracts.UnitRef{UnitID: u.UnitID, SpecDigest: u.SpecDigest})
		}
		s.Packages = append(s.Packages, p)
	}
	return s
}
func TestTwoLogicalWorkersDrainAndResume(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	s := independentSubmission()
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	loss := &loseReplies{base: f.worker.HTTP.Transport, lost: map[string]bool{}}
	f.worker.HTTP.Transport = loss
	rootA, rootB := filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "b")
	stopA := launchAgent(t, f, f.worker, "local", rootA, 250)
	await(t, func() bool { return len(snapshot(rootA).Started) == 1 })
	firstAssignment := snapshot(rootA).Work.Assignment.AssignmentID
	stopB := launchAgent(t, f, f.other, "other", rootB, 150)
	request := contracts.ControlRequest{SchemaVersion: 1, RequestID: "drain-a", TargetKind: "worker", TargetID: "local", Action: "drain", TerminateAt: time.Now().Add(2 * time.Second)}
	var intent contracts.ControlStatus
	if e := f.operator.Do(ctx, "POST", "/v1/control", request, &intent); e != nil {
		t.Fatal(e)
	}
	if intent.State != "intent_persisted" {
		t.Fatal(intent)
	}
	if e := f.operator.Do(ctx, "POST", "/v1/control", request, &intent); e != nil {
		t.Fatal("lost control intent reply replay", e)
	}
	await(t, func() bool {
		q, e := f.c.ControlStatus(ctx, "worker", "local")
		return e == nil && q.State == "complete"
	})
	await(t, func() bool { q, e := f.c.Status(ctx, "workers"); return e == nil && q.Accepted == 4 && q.Quiescent })
	a := snapshot(rootA)
	if len(a.Started) != 1 || a.Control.Revision != 1 || !a.FinalSent {
		t.Fatal("drain crossed grant barrier", a)
	}
	var last int
	if e := f.c.db.QueryRow("SELECT last_grant FROM assignment_controls WHERE assignment_id=?", firstAssignment).Scan(&last); e != nil || last != 1 {
		t.Fatal("controller barrier missing", last, e)
	}
	loss.mu.Lock()
	ackLost := loss.lost["/v1/control/ack"]
	loss.mu.Unlock()
	if !ackLost {
		t.Fatal("control acknowledgment loss not exercised")
	}
	owned := a.Work.Assignment
	ack := contracts.ControlAck{Receipt: contracts.Receipt{SchemaVersion: 1, RequestID: owned.AssignmentID + ":control:1", AssignmentID: owned.AssignmentID, WorkerID: owned.WorkerID, SessionID: owned.SessionID}, Revision: 1, LastGrantIndex: 1, LastGrantedUnitID: owned.Units[0].UnitID}
	if e := f.worker.Do(ctx, "POST", "/v1/control/ack", ack, nil); e != nil {
		t.Fatal("lost barrier acknowledgment retry", e)
	}
	stopA()
	stopB()
	// Restart the paused agent, prove real idle polls cannot issue new work, then resume.
	fresh := oneSubmission("fresh")
	fresh.Units[0].UnitID = "fresh-unit"
	fresh.Units[0].SpecDigest = contracts.Hash([]byte("fresh"))
	fresh.Packages[0].Units = []contracts.UnitRef{{UnitID: fresh.Units[0].UnitID, SpecDigest: fresh.Units[0].SpecDigest}}
	if e := f.operator.Do(ctx, "POST", "/v1/runs", fresh, nil); e != nil {
		t.Fatal(e)
	}
	counter := &countPolls{base: f.worker.HTTP.Transport}
	f.worker.HTTP.Transport = counter
	stopRestarted := launchAgent(t, f, f.worker, "local", rootA, 0)
	await(t, func() bool { return counter.n.Load() >= 3 })
	q, e := f.c.Status(ctx, "fresh")
	if e != nil || q.Accepted != 0 || !q.Quiescent {
		t.Fatal("paused restart issued work", q, e)
	}
	resume := contracts.ControlRequest{SchemaVersion: 1, RequestID: "resume-a", TargetKind: "worker", TargetID: "local", Action: "resume", ExpectedRevision: 1}
	if e = f.operator.Do(ctx, "POST", "/v1/control", resume, &intent); e != nil {
		t.Fatal(e)
	}
	await(t, func() bool { q, e := f.c.Status(ctx, "fresh"); return e == nil && q.Accepted == 1 && q.Quiescent })
	var oldRevision int
	if e = f.c.db.QueryRow("SELECT revision FROM assignment_controls WHERE assignment_id=?", firstAssignment).Scan(&oldRevision); e != nil || oldRevision != 1 {
		t.Fatal("resume reversed issued drain", oldRevision, e)
	}
	stopRestarted()
	// Target pause survives controller metadata reopening.
	request.RequestID = "pause-again"
	request.ExpectedRevision = 2
	request.TerminateAt = time.Now().Add(time.Second)
	if e = f.operator.Do(ctx, "POST", "/v1/control", request, &intent); e != nil {
		t.Fatal(e)
	}
	f.stop()
	reopened, e := Open(f.cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	control, e := reopened.ControlStatus(ctx, "worker", "local")
	if e != nil || !control.IssuancePaused || control.Revision != 3 {
		t.Fatal("pause lost across controller reopen", control, e)
	}
}
func TestReconcileRevisionsAndLostSpool(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	ctx := context.Background()
	s := oneSubmission("loss")
	base := s.Units[0]
	for _, id := range []string{"b", "c"} {
		u := base
		u.UnitID = id
		u.SpecDigest = contracts.Hash([]byte(id))
		s.Units = append(s.Units, u)
		s.Packages[0].Units = append(s.Packages[0].Units, contracts.UnitRef{UnitID: id, SpecDigest: u.SpecDigest})
	}
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	var work contracts.Work
	poll := contracts.PollRequest{SchemaVersion: 1, RequestID: "reserve", WorkerID: "local", SessionID: "session"}
	if e := f.worker.Do(ctx, "POST", "/v1/poll", poll, &work); e != nil {
		t.Fatal(e)
	}
	a := *work.Assignment
	if e := f.other.Do(ctx, "POST", "/v1/receipt", contracts.Receipt{SchemaVersion: 1, RequestID: "forged-worker", AssignmentID: a.AssignmentID, WorkerID: a.WorkerID, SessionID: a.SessionID}, nil); e == nil {
		t.Fatal("other worker impersonated assignment owner")
	}
	digest, _ := contracts.Digest(a)
	report := contracts.ReconcileRequest{Receipt: contracts.Receipt{SchemaVersion: 1, RequestID: "report-1", AssignmentID: a.AssignmentID, WorkerID: a.WorkerID, SessionID: a.SessionID}, Revision: 1, AssignmentDigest: digest}
	var result contracts.ReconcileResult
	if e := f.worker.Do(ctx, "POST", "/v1/reconcile", report, &result); e != nil {
		t.Fatal(e)
	}
	report.Revision = 3
	report.RequestID = "report-3"
	report.ProcessStarted = true
	report.Started = []string{a.Units[0].UnitID}
	if e := f.worker.Do(ctx, "POST", "/v1/reconcile", report, &result); e != nil || result.State != "uncertain" {
		t.Fatal(result, e)
	}
	stale := report
	stale.Revision = 2
	stale.RequestID = "stale"
	stale.ProcessStarted = false
	stale.Started = nil
	if e := f.worker.Do(ctx, "POST", "/v1/reconcile", stale, &result); e != nil || result.State != "uncertain" {
		t.Fatal("stale report regressed uncertainty", result, e)
	}
	stale.Revision = 4
	stale.RequestID = "regression"
	if e := f.worker.Do(ctx, "POST", "/v1/reconcile", stale, &result); e == nil {
		t.Fatal("newer regressive report accepted")
	}
	candidate, data := singleCandidate(a, s.Units[0])
	ref := candidate.Files[0].Artifact
	if e := f.worker.Publish(ctx, ref.Digest, ref.Size, a.AssignmentID, bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	receipt := report.Receipt
	receipt.RequestID = "candidate"
	if e := f.worker.Do(ctx, "POST", "/v1/candidate", contracts.CandidateReport{Receipt: receipt, Candidate: candidate}, nil); e != nil {
		t.Fatal(e)
	}
	resolution := contracts.LostSpoolResolution{Receipt: report.Receipt, EvidenceKind: "operator_exact_execution_terminated", Reason: "test owner observed exact leaf termination; disk confirmed lost"}
	resolution.RequestID = "proof"
	if e := f.operator.Do(ctx, "POST", "/v1/lost-spool", resolution, nil); e == nil {
		t.Fatal("missing positive proof accepted")
	}
	resolution.ExecutionTerminated = true
	if e := f.operator.Do(ctx, "POST", "/v1/lost-spool", resolution, nil); e == nil {
		t.Fatal("missing positive spool-loss proof accepted")
	}
	resolution.SpoolIrrecoverablyLost = true
	if e := f.operator.Do(ctx, "POST", "/v1/lost-spool", resolution, nil); e != nil {
		t.Fatal(e)
	}
	var retryWork contracts.Work
	p := contracts.PollRequest{SchemaVersion: 1, RequestID: "retry-missing", WorkerID: "other", SessionID: "other-session"}
	if e := f.other.Do(ctx, "POST", "/v1/poll", p, &retryWork); e != nil {
		t.Fatal(e)
	}
	if retryWork.Assignment == nil || len(retryWork.Assignment.Units) != 2 {
		t.Fatal("retained candidate recomputed or missing units not retried", retryWork)
	}
	accounting := func() string {
		var encoded string
		if e := f.c.db.QueryRow(`SELECT group_concat(record,'|') FROM (SELECT n.unit_id||':'||n.attempts||':'||n.reason||':'||coalesce(o.assignment_id,'') record FROM unit_attempts n LEFT JOIN owners o ON o.run_id=n.run_id AND o.unit_id=n.unit_id WHERE n.run_id='loss' ORDER BY n.unit_id)`).Scan(&encoded); e != nil {
			t.Fatal(e)
		}
		return encoded
	}
	beforeDelayedFinal := accounting()
	for _, mode := range []string{"never-started", "interrupted"} {
		oldReceipt := report.Receipt
		oldReceipt.RequestID = "delayed-old-" + mode
		final := contracts.FinalReport{Receipt: oldReceipt, Completed: []string{base.UnitID}}
		if mode == "never-started" {
			final.NeverStarted = []string{"b", "c"}
		} else {
			final.Interrupted = []string{"b", "c"}
		}
		e := f.worker.Do(ctx, "POST", "/v1/final", final, nil)
		if e == nil || !strings.Contains(e.Error(), "terminal-resolved") {
			t.Fatal("superseded final applied", mode, e)
		}
		if after := accounting(); after != beforeDelayedFinal {
			t.Fatal("old final changed new attempt accounting/owner/reason", mode, beforeDelayedFinal, after)
		}
	}
	for _, u := range retryWork.Units {
		if u.UnitID == base.UnitID {
			t.Fatal("pending retained unit replayed")
		}
	}
	status, e := f.c.Status(ctx, "loss")
	if e != nil || status.WaitingRetainedValidation != 1 {
		t.Fatal(status, e)
	}
	if e = f.c.validateNext(ctx); e != nil {
		t.Fatal(e)
	}
	// A late valid old candidate wins normally and drains, but does not erase, a live retry.
	late, lateData := singleCandidate(a, s.Units[1])
	lateRef := late.Files[0].Artifact
	if e = f.worker.Publish(ctx, lateRef.Digest, lateRef.Size, a.AssignmentID, bytes.NewReader(lateData)); e != nil {
		t.Fatal(e)
	}
	receipt.RequestID = "late"
	if e = f.worker.Do(ctx, "POST", "/v1/candidate", contracts.CandidateReport{Receipt: receipt, Candidate: late}, nil); e != nil {
		t.Fatal(e)
	}
	if e = f.c.validateNext(ctx); e != nil {
		t.Fatal(e)
	}
	retryA := retryWork.Assignment
	retryReceipt := contracts.Receipt{SchemaVersion: 1, RequestID: "control-poll", AssignmentID: retryA.AssignmentID, WorkerID: retryA.WorkerID, SessionID: retryA.SessionID}
	var control contracts.AssignmentControl
	if e = f.other.Do(ctx, "POST", "/v1/control/poll", retryReceipt, &control); e != nil || control.Action != "drain" {
		t.Fatal("late acceptance did not drain retry", control, e)
	}
	var owner string
	if e = f.c.db.QueryRow("SELECT assignment_id FROM owners WHERE run_id='loss' AND unit_id='b'").Scan(&owner); e != nil || owner != retryA.AssignmentID {
		t.Fatal("late acceptance erased live ownership", owner, e)
	}
	resolution.Receipt = retryReceipt
	resolution.RequestID = "proof-retry"
	if e = f.operator.Do(ctx, "POST", "/v1/lost-spool", resolution, nil); e != nil {
		t.Fatal(e)
	}
	p.RequestID = "exhausted"
	var empty contracts.Work
	if e = f.other.Do(ctx, "POST", "/v1/poll", p, &empty); e != nil || empty.Assignment != nil {
		t.Fatal("attempt budget not enforced", empty, e)
	}
	status, e = f.c.Status(ctx, "loss")
	if e != nil || status.Accepted != 2 || status.Exhausted != 1 || status.WaitingRetainedValidation != 0 {
		t.Fatal(status, e)
	}
}
