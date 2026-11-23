package controller

import (
	"context"
	"fmt"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"testing"
	"time"
)

func TestRunDrainFansOutToTwoReservedWorkers(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	s := independentSubmission()
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	clients := []*agent.Client{f.worker, f.other}
	ids := []string{"local", "other"}
	var work [2]contracts.Work
	var receipts [2]contracts.Receipt
	for i, client := range clients {
		p := contracts.PollRequest{SchemaVersion: 1, RequestID: fmt.Sprint("reserve-", i), WorkerID: ids[i], SessionID: ids[i]}
		if e := client.Do(ctx, "POST", "/v1/poll", p, &work[i]); e != nil {
			t.Fatal(e)
		}
		a := work[i].Assignment
		receipts[i] = contracts.Receipt{SchemaVersion: 1, RequestID: "control-poll", AssignmentID: a.AssignmentID, WorkerID: a.WorkerID, SessionID: a.SessionID}
	}
	request := contracts.ControlRequest{SchemaVersion: 1, RequestID: "run-drain", TargetKind: "run", TargetID: s.RunID, Action: "drain", TerminateAt: time.Now().Add(time.Second)}
	var intent contracts.ControlStatus
	if e := f.operator.Do(ctx, "POST", "/v1/control", request, &intent); e != nil {
		t.Fatal(e)
	}
	for i, client := range clients {
		var command contracts.AssignmentControl
		if e := client.Do(ctx, "POST", "/v1/control/poll", receipts[i], &command); e != nil || command.Revision != 1 || command.Action != "drain" {
			t.Fatal(command, e)
		}
		r := receipts[i]
		r.RequestID = "ack"
		if e := client.Do(ctx, "POST", "/v1/control/ack", contracts.ControlAck{Receipt: r, Revision: 1}, nil); e != nil {
			t.Fatal(e)
		}
	}
	status, e := f.c.ControlStatus(ctx, "run", s.RunID)
	if e != nil || status.State == "complete" || status.PendingBarriers != 0 || status.LiveAssignments != 2 {
		t.Fatal("barrier acknowledgment invented completion", status, e)
	}
	for i, client := range clients {
		r := receipts[i]
		r.RequestID = "final"
		final := contracts.FinalReport{Receipt: r}
		for _, u := range work[i].Assignment.Units {
			final.NeverStarted = append(final.NeverStarted, u.UnitID)
		}
		if e = client.Do(ctx, "POST", "/v1/final", final, nil); e != nil {
			t.Fatal(e)
		}
	}
	status, e = f.c.ControlStatus(ctx, "run", s.RunID)
	if e != nil || status.State != "complete" || !status.IssuancePaused {
		t.Fatal(status, e)
	}
	var empty contracts.Work
	if e = f.worker.Do(ctx, "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: "paused", WorkerID: "local", SessionID: "local"}, &empty); e != nil || empty.Assignment != nil {
		t.Fatal("run drain did not persist issuance barrier", empty, e)
	}
	request.RequestID = "resume"
	request.ExpectedRevision = 1
	request.Action = "resume"
	if e = f.operator.Do(ctx, "POST", "/v1/control", request, &intent); e != nil {
		t.Fatal(e)
	}
	var issued contracts.Work
	if e = f.worker.Do(ctx, "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: "resumed", WorkerID: "local", SessionID: "local"}, &issued); e != nil || issued.Assignment == nil {
		t.Fatal("resume did not enable unstarted handback", issued, e)
	}
}
