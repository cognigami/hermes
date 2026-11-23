package controller

import (
	"context"
	"encoding/json"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"path/filepath"
	"testing"
)

func TestTypedSyntheticResourceStopAutomaticallyGetsSufficientAllocation(t *testing.T) {
	for _, phase := range []string{"prepare", "unit"} {
		t.Run(phase, func(t *testing.T) {
			f := setup(t)
			ctx := context.Background()
			s := oneSubmission("resource-" + phase)
			if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
				t.Fatal(e)
			}
			running, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() {
				done <- agent.Run(running, agent.Config{Root: filepath.Join(t.TempDir(), "agent"), WorkerID: "local", SessionID: "resource-session", Runtime: "synthetic-v1", Profile: "synthetic-local", RuntimeArgv: []string{f.app}, Client: f.worker, MaxArtifactBytes: 1 << 20, SyntheticBackend: true, SyntheticMemoryBytes: 256 << 20, SyntheticPhase: phase})
			}()
			defer func() {
				cancel()
				if e := <-done; e != nil && e != context.Canceled {
					t.Error(e)
				}
			}()
			await(t, func() bool {
				status, e := f.c.Status(ctx, s.RunID)
				return e == nil && status.Accepted == 1 && status.Quiescent
			})
			rows, e := f.c.db.Query("SELECT manifest FROM assignments ORDER BY rowid")
			if e != nil {
				t.Fatal(e)
			}
			var assignments []contracts.Assignment
			for rows.Next() {
				var b []byte
				if e = rows.Scan(&b); e != nil {
					t.Fatal(e)
				}
				var a contracts.Assignment
				if e = json.Unmarshal(b, &a); e != nil {
					t.Fatal(e)
				}
				assignments = append(assignments, a)
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				t.Fatal(e)
			}
			if len(assignments) != 2 {
				t.Fatal("not exactly one automatic escalation", len(assignments))
			}
			first, next := assignments[0], assignments[1]
			if first.Allocation.Resources.MemoryBytes != 64<<20 || next.Allocation.Resources.MemoryBytes != 256<<20 || !next.Allocation.Exclusive || first.Units[0] != next.Units[0] {
				t.Fatal("retry mutated science or did not grant larger sufficient resources", first, next)
			}
			var attempts, floor int64
			if e = f.c.db.QueryRow("SELECT attempts FROM unit_attempts WHERE run_id=? AND unit_id=?", s.RunID, s.Units[0].UnitID).Scan(&attempts); e != nil {
				t.Fatal(e)
			}
			if e = f.c.db.QueryRow("SELECT memory_floor FROM resource_failures WHERE run_id=? AND unit_id=?", s.RunID, s.Units[0].UnitID).Scan(&floor); e != nil {
				t.Fatal(e)
			}
			if attempts != 2 || floor != 64<<20 {
				t.Fatal("preparation failure refunded or inadequate floor lost", attempts, floor)
			}
		})
	}
}
func TestApplicationFailureTextCannotAuthorizeResourceRetry(t *testing.T) {
	f := setup(t)
	s := oneSubmission("no-resource-inference")
	ctx := context.Background()
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	var work contracts.Work
	if e := f.worker.Do(ctx, "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: "poll", WorkerID: "local", SessionID: "session"}, &work); e != nil {
		t.Fatal(e)
	}
	a := work.Assignment
	digest, _ := contracts.Digest(a.Allocation)
	event := contracts.ResourceStop{Receipt: contracts.Receipt{SchemaVersion: 1, RequestID: "stop", WorkerID: a.WorkerID, SessionID: a.SessionID, AssignmentID: a.AssignmentID}, AllocationDigest: digest, Phase: "unit", UnitID: a.Units[0].UnitID, GrantIndex: 1, Kind: "memory_limit", Source: "application_exit", EvidenceReference: "OOM in stdout / exit137", KnownInadequateLimit: a.Allocation.Resources.MemoryBytes}
	if e := f.worker.Do(ctx, "POST", "/v1/resource-stop", event, nil); e == nil {
		t.Fatal("application text became infrastructure evidence")
	}
	event.RequestID = "unqualified-native"
	event.Source = "qualified_backend_event"
	if e := f.worker.Do(ctx, "POST", "/v1/resource-stop", event, nil); e == nil {
		t.Fatal("unknown native memory enforcement qualified")
	}
	var count int
	if e := f.c.db.QueryRow("SELECT count(*) FROM resource_failures").Scan(&count); e != nil || count != 0 {
		t.Fatal(count, e)
	}
}
