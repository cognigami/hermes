package controller

import (
	"bytes"
	"context"
	"hermes/internal/contracts"
	"hermes/testdata/synthetic"
	"testing"
)

func TestTypedResourceRetryIndependentOfRejectedValidationEvidenceOrder(t *testing.T) {
	for _, before := range []bool{true, false} {
		name := "validation-after"
		if before {
			name = "validation-before"
		}
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			f.stopValidation()
			ctx := context.Background()
			s := oneSubmission("evidence-order")
			if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
				t.Fatal(e)
			}
			work := nextResourceWork(t, f, "first")
			a := *work.Assignment
			candidate, _ := singleCandidate(a, s.Units[0])
			data := encoded(synthetic.Result{UnitID: candidate.UnitID, SpecDigest: candidate.SpecDigest, Value: "wrong"})
			candidate.Files[0].Artifact.Digest = contracts.Hash(data)
			candidate.Files[0].Artifact.Size = int64(len(data))
			digest, _ := contracts.Digest(a.Allocation)
			receipt := contracts.Receipt{SchemaVersion: 1, RequestID: "cause", AssignmentID: a.AssignmentID, WorkerID: a.WorkerID, SessionID: a.SessionID}
			event := contracts.ResourceStop{Receipt: receipt, AllocationDigest: digest, Phase: "unit", UnitID: candidate.UnitID, GrantIndex: 1, Kind: "memory_limit", Source: "synthetic_test_event", EvidenceReference: "controlled", KnownInadequateLimit: a.Allocation.Resources.MemoryBytes, ConfirmedMinimum: contracts.ResourceVector{MemoryBytes: 256 << 20}}
			if e := f.worker.Do(ctx, "POST", "/v1/resource-stop", event, nil); e != nil {
				t.Fatal(e)
			}
			ref := candidate.Files[0].Artifact
			if e := f.worker.Publish(ctx, ref.Digest, ref.Size, a.AssignmentID, bytes.NewReader(data)); e != nil {
				t.Fatal(e)
			}
			receipt.RequestID = "candidate"
			if e := f.worker.Do(ctx, "POST", "/v1/candidate", contracts.CandidateReport{Receipt: receipt, Candidate: candidate}, nil); e != nil {
				t.Fatal(e)
			}
			if before {
				if e := f.c.validateNext(ctx); e != nil {
					t.Fatal(e)
				}
			}
			receipt.RequestID = "terminal"
			proof := contracts.LostSpoolResolution{Receipt: receipt, SpoolIrrecoverablyLost: true, ExecutionTerminated: true, EvidenceKind: "operator_exact_execution_terminated", Reason: "synthetic exact leaf stopped and spool lost"}
			if e := f.operator.Do(ctx, "POST", "/v1/lost-spool", proof, nil); e != nil {
				t.Fatal(e)
			}
			if !before {
				if e := f.c.validateNext(ctx); e != nil {
					t.Fatal(e)
				}
			}
			next := nextResourceWork(t, f, "retry")
			if next.Assignment.Allocation.Resources.MemoryBytes != 256<<20 || next.Assignment.Units[0] != a.Units[0] {
				t.Fatal("ordering changed resource retry/floor/science", next)
			}
		})
	}
}
