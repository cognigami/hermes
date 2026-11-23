package controller

import (
	"context"
	"encoding/json"
	"errors"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func twoUnitRun(id string) contracts.Submission {
	s := oneSubmission(id)
	first := s.Units[0]
	first.UnitID = id + "-0"
	first.SpecDigest = contracts.Hash([]byte(first.UnitID))
	second := first
	second.UnitID = id + "-1"
	second.SpecDigest = contracts.Hash([]byte(second.UnitID))
	s.Units = []contracts.Unit{first, second}
	s.Packages[0].Units = []contracts.UnitRef{{UnitID: first.UnitID, SpecDigest: first.SpecDigest}, {UnitID: second.UnitID, SpecDigest: second.SpecDigest}}
	return s
}
func activeSnapshots(root string) []agentSnapshot {
	paths, _ := filepath.Glob(filepath.Join(root, "active", "*", "journal.json"))
	var result []agentSnapshot
	for _, path := range paths {
		b, e := os.ReadFile(path)
		if e != nil {
			continue
		}
		var s agentSnapshot
		if json.Unmarshal(b, &s) == nil {
			result = append(result, s)
		}
	}
	return result
}
func TestOneHostRunsTwoAssignmentsWithIndependentGates(t *testing.T) {
	f := setup(t)
	f.c.cfg.MaxConcurrent = 2
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if e := f.operator.Do(ctx, "POST", "/v1/runs", twoUnitRun(id), nil); e != nil {
			t.Fatal(e)
		}
	}
	root := filepath.Join(t.TempDir(), "host")
	running, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	counter := &countPolls{base: f.worker.HTTP.Transport}
	f.worker.HTTP.Transport = counter
	go func() {
		done <- agent.Run(running, agent.Config{Root: root, WorkerID: "local", SessionID: "concurrent", Runtime: "synthetic-v1", Profile: "synthetic-local", RuntimeArgv: []string{f.app, "--delay-millis", "500"}, Client: f.worker, MaxArtifactBytes: 1 << 20, MaxConcurrent: 2})
	}()
	defer func() {
		cancel()
		if e := <-done; !errors.Is(e, context.Canceled) {
			t.Error(e)
		}
	}()
	var simultaneous []agentSnapshot
	await(t, func() bool {
		simultaneous = activeSnapshots(root)
		if len(simultaneous) != 2 {
			return false
		}
		for _, s := range simultaneous {
			if !s.ProcessStarted || len(s.Started) != 1 || s.Quiescent || !s.ReceiptAcknowledged {
				return false
			}
		}
		return true
	})
	// The same host/session can issue another poll only with every exact retained journal.
	p := contracts.PollRequest{SchemaVersion: 1, RequestID: "bounded-third", WorkerID: "local", SessionID: "concurrent"}
	for _, s := range simultaneous {
		digest, _ := contracts.Digest(*s.Work.Assignment)
		p.Retained = append(p.Retained, contracts.RetainedAssignment{AssignmentID: s.Work.Assignment.AssignmentID, Digest: digest})
	}
	var empty contracts.Work
	if e := f.worker.Do(ctx, "POST", "/v1/poll", p, &empty); e != nil || empty.Assignment != nil {
		t.Fatal("host concurrency bound failed", empty, e)
	}
	var intent contracts.ControlStatus
	if e := f.operator.Do(ctx, "POST", "/v1/control", contracts.ControlRequest{SchemaVersion: 1, RequestID: "only-a", TargetKind: "run", TargetID: "a", Action: "drain", TerminateAt: time.Now().Add(2 * time.Second)}, &intent); e != nil {
		t.Fatal(e)
	}
	await(t, func() bool {
		a, e := f.c.Status(ctx, "a")
		if e != nil || a.Accepted != 1 || !a.Quiescent {
			return false
		}
		b, e := f.c.Status(ctx, "b")
		return e == nil && b.Accepted == 2 && b.Quiescent
	})
	before := counter.n.Load()
	await(t, func() bool { return counter.n.Load() >= before+3 })
	await(t, func() bool { return len(activeSnapshots(root)) == 0 })
	var assignments int
	if e := f.c.db.QueryRow("SELECT count(*) FROM assignments WHERE worker='local'").Scan(&assignments); e != nil || assignments != 2 {
		t.Fatal("host replayed or duplicated assignments", assignments, e)
	}
}
func TestRetainedSnapshotAllowsOnlyExactKnownTerminalExtras(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	s := oneSubmission("inventory")
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	var first contracts.Work
	p := contracts.PollRequest{SchemaVersion: 1, RequestID: "first", WorkerID: "local", SessionID: "session"}
	if e := f.worker.Do(ctx, "POST", "/v1/poll", p, &first); e != nil {
		t.Fatal(e)
	}
	a := first.Assignment
	r := contracts.Receipt{SchemaVersion: 1, RequestID: "receipt", AssignmentID: a.AssignmentID, WorkerID: a.WorkerID, SessionID: a.SessionID}
	if e := f.worker.Do(ctx, "POST", "/v1/receipt", r, nil); e != nil {
		t.Fatal(e)
	}
	r.RequestID = "final"
	if e := f.worker.Do(ctx, "POST", "/v1/final", contracts.FinalReport{Receipt: r, NeverStarted: []string{s.Units[0].UnitID}}, nil); e != nil {
		t.Fatal(e)
	}
	digest, _ := contracts.Digest(*a)
	terminal := contracts.RetainedAssignment{AssignmentID: a.AssignmentID, Digest: digest}
	p.RequestID = "snapshot-before-final-ack"
	p.Retained = []contracts.RetainedAssignment{terminal}
	var second contracts.Work
	if e := f.worker.Do(ctx, "POST", "/v1/poll", p, &second); e != nil || second.Assignment == nil {
		t.Fatal("known terminal extra became fatal", second, e)
	}
	b := second.Assignment
	r.AssignmentID = b.AssignmentID
	r.RequestID = "receipt-second"
	if e := f.worker.Do(ctx, "POST", "/v1/receipt", r, nil); e != nil {
		t.Fatal(e)
	}
	currentDigest, _ := contracts.Digest(*b)
	current := contracts.RetainedAssignment{AssignmentID: b.AssignmentID, Digest: currentDigest}
	for _, test := range []struct {
		name string
		refs []contracts.RetainedAssignment
	}{{"missing", []contracts.RetainedAssignment{terminal}}, {"unknown", []contracts.RetainedAssignment{current, {AssignmentID: "unknown", Digest: digest}}}, {"mismatch", []contracts.RetainedAssignment{current, {AssignmentID: a.AssignmentID, Digest: contracts.Hash([]byte("wrong"))}}}} {
		p.RequestID = test.name
		p.Retained = test.refs
		var work contracts.Work
		if e := f.worker.Do(ctx, "POST", "/v1/poll", p, &work); e == nil {
			t.Fatal("unsafe inventory accepted", test.name)
		}
	}
	p.RequestID = "exact-terminal-and-active"
	p.Retained = []contracts.RetainedAssignment{current, terminal}
	var work contracts.Work
	if e := f.worker.Do(ctx, "POST", "/v1/poll", p, &work); e != nil || work.Assignment != nil {
		t.Fatal("valid full inventory rejected", work, e)
	}
}
