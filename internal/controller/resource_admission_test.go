package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"hermes/internal/contracts"
	"testing"
)

func copyFixturePolicy(t *testing.T, p *contracts.ResourcePolicy) contracts.ResourcePolicy {
	t.Helper()
	var clone contracts.ResourcePolicy
	if e := json.Unmarshal(encoded(p), &clone); e != nil {
		t.Fatal(e)
	}
	return clone
}
func TestPolicyRevisionChangesUnassignedNotIssuedAllocation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	for _, run := range []string{"old-allocation", "new-allocation"} {
		if e := f.operator.Do(ctx, "POST", "/v1/runs", oneSubmission(run), nil); e != nil {
			t.Fatal(e)
		}
	}
	var old, next contracts.Work
	if e := f.worker.Do(ctx, "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: "old", WorkerID: "local", SessionID: "session"}, &old); e != nil {
		t.Fatal(e)
	}
	before, _ := contracts.Digest(*old.Assignment)
	p := copyFixturePolicy(t, f.cfg.ResourcePolicy)
	p.Revision = 2
	p.Profiles[0].Allocation.Resources.MemoryBytes = 128 << 20
	request := contracts.PolicyRequest{SchemaVersion: 1, RequestID: "policy", ExpectedRevision: 1, Policy: p}
	if e := f.operator.Do(ctx, "POST", "/v1/resource-policy", request, nil); e != nil {
		t.Fatal(e)
	}
	if e := f.other.Do(ctx, "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: "new", WorkerID: "other", SessionID: "session"}, &next); e != nil {
		t.Fatal(e)
	}
	if next.Assignment == nil || next.Assignment.PolicyRevision != 2 || next.Assignment.Allocation.Resources.MemoryBytes != 128<<20 {
		t.Fatal(next)
	}
	var b []byte
	if e := f.c.db.QueryRow("SELECT manifest FROM assignments WHERE id=?", old.Assignment.AssignmentID).Scan(&b); e != nil {
		t.Fatal(e)
	}
	var persisted contracts.Assignment
	if e := json.Unmarshal(b, &persisted); e != nil {
		t.Fatal(e)
	}
	after, _ := contracts.Digest(persisted)
	if before != after || persisted.Allocation.Resources.MemoryBytes != 64<<20 {
		t.Fatal("issued allocation changed", persisted)
	}
	f.stop()
	reopened, e := Open(f.cfg)
	if e != nil {
		t.Fatal("persisted newer policy lost to old bootstrap config", e)
	}
	defer reopened.Close()
	tx, e := reopened.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	current, e := reopened.policy(tx)
	if e != nil || current.Revision != 2 {
		t.Fatal(current, e)
	}
}
func TestAggregateHostCPUStopsFourthReservation(t *testing.T) {
	f := setup(t)
	f.c.cfg.MaxConcurrent = 4
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if e := f.operator.Do(ctx, "POST", "/v1/runs", oneSubmission(fmt.Sprintf("aggregate-%d", i)), nil); e != nil {
			t.Fatal(e)
		}
	}
	request := contracts.PollRequest{SchemaVersion: 1, WorkerID: "local", SessionID: "session"}
	for i := 0; i < 4; i++ {
		request.RequestID = fmt.Sprintf("poll-%d", i)
		var work contracts.Work
		if e := f.worker.Do(ctx, "POST", "/v1/poll", request, &work); e != nil {
			t.Fatal(e)
		}
		if i == 3 {
			if work.Assignment != nil {
				t.Fatal("fourth250m job exceeded1000m host with125m baseline", work)
			}
			break
		}
		if work.Assignment == nil {
			t.Fatal("first three did not fit", i)
		}
		a := work.Assignment
		if e := f.worker.Do(ctx, "POST", "/v1/receipt", contracts.Receipt{SchemaVersion: 1, RequestID: fmt.Sprintf("receipt-%d", i), WorkerID: a.WorkerID, SessionID: a.SessionID, AssignmentID: a.AssignmentID}, nil); e != nil {
			t.Fatal(e)
		}
		digest, _ := contracts.Digest(*a)
		request.Retained = append(request.Retained, contracts.RetainedAssignment{AssignmentID: a.AssignmentID, Digest: digest})
	}
}
func TestUnknownHardEnforcementDoesNotStarveIndependentSoftWork(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	p := copyFixturePolicy(t, f.cfg.ResourcePolicy)
	p.Revision = 2
	p.Profiles[0].Allocation.RequiresHardTreeMemory = true
	p.Profiles[1].Allocation.RequiresHardTreeMemory = true
	soft := p.Profiles[0]
	soft.ID = "approved-soft"
	soft.Allocation.RequiresHardTreeMemory = false
	p.Profiles = append(p.Profiles, soft)
	if e := f.operator.Do(ctx, "POST", "/v1/resource-policy", contracts.PolicyRequest{SchemaVersion: 1, RequestID: "policy", ExpectedRevision: 1, Policy: p}, nil); e != nil {
		t.Fatal(e)
	}
	hard := oneSubmission("hard")
	hard.Units[0].UnitID = "hard-unit"
	hard.Units[0].RequiredCapabilities = []string{"hard_tree_memory"}
	hard.Packages[0].Units[0].UnitID = "hard-unit"
	for _, s := range []contracts.Submission{hard, oneSubmission("soft")} {
		if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
			t.Fatal(e)
		}
	}
	for i := 0; i < 2; i++ {
		var work contracts.Work
		if e := f.worker.Do(ctx, "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: fmt.Sprintf("poll-%d", i), WorkerID: "local", SessionID: "session"}, &work); e != nil {
			t.Fatal(e)
		}
		if i == 0 && work.Assignment != nil {
			t.Fatal("unknown hard enforcement admitted", work)
		}
		if i == 1 && (work.Assignment == nil || work.Assignment.RunID != "soft" || work.Assignment.Profile != "approved-soft") {
			t.Fatal("unfit hard head starved soft work", work)
		}
	}
}
