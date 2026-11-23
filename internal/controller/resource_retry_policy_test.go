package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hermes/internal/contracts"
	"hermes/testdata/synthetic"
	"testing"
)

func virtualResourceFailure(t *testing.T, f *fixture, a contracts.Assignment, request string) {
	t.Helper()
	digest, _ := contracts.Digest(a.Allocation)
	event := contracts.ResourceStop{Receipt: contracts.Receipt{SchemaVersion: 1, RequestID: request, AssignmentID: a.AssignmentID, WorkerID: a.WorkerID, SessionID: a.SessionID}, AllocationDigest: digest, Phase: "unit", UnitID: a.Units[0].UnitID, GrantIndex: 1, Kind: "memory_limit", Source: "synthetic_test_event", EvidenceReference: request, KnownInadequateLimit: a.Allocation.Resources.MemoryBytes}
	if e := f.worker.Do(context.Background(), "POST", "/v1/resource-stop", event, nil); e != nil {
		t.Fatal(e)
	}
	event.RequestID = request + "-final"
	if e := f.worker.Do(context.Background(), "POST", "/v1/final", contracts.FinalReport{Receipt: event.Receipt, Interrupted: []string{a.Units[0].UnitID}}, nil); e != nil {
		t.Fatal(e)
	}
}
func nextResourceWork(t *testing.T, f *fixture, prefix string) contracts.Work {
	t.Helper()
	var work contracts.Work
	attempt := 0
	await(t, func() bool {
		attempt++
		e := f.worker.Do(context.Background(), "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: fmt.Sprintf("%s-%d", prefix, attempt), WorkerID: "local", SessionID: "session"}, &work)
		if e != nil {
			t.Fatal(e)
		}
		return work.Assignment != nil
	})
	return work
}
func TestTypedResourceAttemptsExhaustPersistently(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	ctx := context.Background()
	s := oneSubmission("resource-exhaust")
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	first := nextResourceWork(t, f, "first")
	virtualResourceFailure(t, f, *first.Assignment, "stop-small")
	next := nextResourceWork(t, f, "next")
	if next.Assignment.Allocation.Resources.MemoryBytes <= first.Assignment.Allocation.Resources.MemoryBytes {
		t.Fatal("known inadequate allocation repeated")
	}
	virtualResourceFailure(t, f, *next.Assignment, "stop-large")
	status, e := f.c.Status(ctx, s.RunID)
	if e != nil || status.Exhausted != 1 || status.Accepted != 0 || !status.Quiescent {
		t.Fatal(status, e)
	}
	f.stop()
	reopened, e := Open(f.cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	status, e = reopened.Status(ctx, s.RunID)
	if e != nil || status.Exhausted != 1 {
		t.Fatal("resource exhaustion not persistent", status, e)
	}
}
func TestOrdinaryRejectionAfterResourceRetryDoesNotAuthorizeAnotherCalculation(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	f.c.cfg.MaxAttempts = 3
	ctx := context.Background()
	s := oneSubmission("ordinary-reject")
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	first := nextResourceWork(t, f, "first")
	virtualResourceFailure(t, f, *first.Assignment, "stop")
	next := nextResourceWork(t, f, "next")
	candidate, data := singleCandidate(*next.Assignment, s.Units[0])
	var result synthetic.Result
	if e := json.Unmarshal(data, &result); e != nil {
		t.Fatal(e)
	}
	result.Value = "wrong"
	data = encoded(result)
	ref := &candidate.Files[0].Artifact
	ref.Size = int64(len(data))
	ref.Digest = contracts.Hash(data)
	if e := f.worker.Publish(ctx, ref.Digest, ref.Size, candidate.AssignmentID, bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	receipt := contracts.Receipt{SchemaVersion: 1, RequestID: "candidate", AssignmentID: candidate.AssignmentID, WorkerID: "local", SessionID: "session"}
	if e := f.worker.Do(ctx, "POST", "/v1/candidate", contracts.CandidateReport{Receipt: receipt, Candidate: candidate}, nil); e != nil {
		t.Fatal(e)
	}
	if e := f.c.validateNext(ctx); e != nil {
		t.Fatal(e)
	}
	var state string
	if e := f.c.db.QueryRow("SELECT state FROM candidates WHERE assignment_id=?", candidate.AssignmentID).Scan(&state); e != nil || state != "rejected" {
		t.Fatal("not an ordinary domain rejection", state, e)
	}
	receipt.RequestID = "final"
	if e := f.worker.Do(ctx, "POST", "/v1/final", contracts.FinalReport{Receipt: receipt, Completed: []string{candidate.UnitID}}, nil); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		var work contracts.Work
		if e := f.worker.Do(ctx, "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: fmt.Sprintf("no-third-%d", i), WorkerID: "local", SessionID: "session"}, &work); e != nil || work.Assignment != nil {
			t.Fatal("old resource cause authorized replay of ordinary rejection", work, e)
		}
	}
	var attempts int
	if e := f.c.db.QueryRow("SELECT attempts FROM unit_attempts WHERE run_id=?", s.RunID).Scan(&attempts); e != nil || attempts != 2 {
		t.Fatal(attempts, e)
	}
}
