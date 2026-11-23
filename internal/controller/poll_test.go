package controller

import (
	"context"
	"fmt"
	"hermes/internal/contracts"
	"testing"
)

func TestEmptyPollsDoNotPersistOutcomes(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	counts := func() (int, int) {
		t.Helper()
		var requests, assignments int
		if e := f.c.db.QueryRow("SELECT COUNT(*) FROM requests").Scan(&requests); e != nil {
			t.Fatal(e)
		}
		if e := f.c.db.QueryRow("SELECT COUNT(*) FROM assignments").Scan(&assignments); e != nil {
			t.Fatal(e)
		}
		return requests, assignments
	}
	beforeRequests, beforeAssignments := counts()
	for i := 0; i < 25; i++ {
		p := contracts.PollRequest{SchemaVersion: 1, RequestID: fmt.Sprintf("empty-%d", i), WorkerID: "local", SessionID: "session"}
		var work contracts.Work
		if e := f.worker.Do(ctx, "POST", "/v1/poll", p, &work); e != nil {
			t.Fatal(e)
		}
		if work.Assignment != nil {
			t.Fatal("unexpected reservation")
		}
	}
	requests, assignments := counts()
	if requests != beforeRequests || assignments != beforeAssignments {
		t.Fatal("empty polls persisted permanent metadata", requests, assignments)
	}
	s := oneSubmission("ready")
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	loss := &loseReplies{base: f.worker.HTTP.Transport, lost: map[string]bool{}}
	f.worker.HTTP.Transport = loss
	// An earlier empty request is read-only, so it can now obtain work. Once it
	// reserves, its exact outcome must survive a lost response and repeated retry.
	p := contracts.PollRequest{SchemaVersion: 1, RequestID: "empty-0", WorkerID: "local", SessionID: "session"}
	var first, again contracts.Work
	if e := f.worker.Do(ctx, "POST", "/v1/poll", p, &first); e == nil {
		t.Fatal("lost reservation reply not exercised")
	}
	if e := f.worker.Do(ctx, "POST", "/v1/poll", p, &first); e != nil {
		t.Fatal(e)
	}
	if e := f.worker.Do(ctx, "POST", "/v1/poll", p, &again); e != nil {
		t.Fatal(e)
	}
	if first.Assignment == nil || again.Assignment == nil || first.Assignment.AssignmentID != again.Assignment.AssignmentID {
		t.Fatal("reservation retry changed identity")
	}
	requests, assignments = counts()
	if requests != beforeRequests+2 || assignments != beforeAssignments+1 {
		t.Fatal("reservation outcome not exactly once", requests, assignments)
	}
}
