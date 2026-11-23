package contracts_test

import (
	"hermes/internal/contracts"
	"math"
	"testing"
	"time"
)

func TestResourceArithmeticAndScientificIdentity(t *testing.T) {
	a := contracts.ResourceVector{CPUMilli: 1000, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}
	baseline := contracts.ResourceVector{CPUMilli: 500, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}
	limit := contracts.ResourceVector{CPUMilli: 4000, MemoryBytes: 512 << 20, SpoolBytes: 64 << 20}
	used := baseline
	for i := 0; i < 3; i++ {
		next, e := used.Add(a)
		if e != nil || !next.Fits(limit) {
			t.Fatal(next, e)
		}
		used = next
	}
	fourth, e := used.Add(a)
	if e != nil || fourth.Fits(limit) {
		t.Fatal("aggregate budget did not reject fourth", fourth, e)
	}
	if _, e = (contracts.ResourceVector{CPUMilli: math.MaxInt64}).Add(contracts.ResourceVector{CPUMilli: 1}); e == nil {
		t.Fatal("overflow accepted")
	}
	if (contracts.ResourceVector{MemoryBytes: -1}).Validate() == nil {
		t.Fatal("negative resource quantity")
	}
	u := unit("allocation-independent")
	scientific, _ := contracts.Digest(u)
	now := time.Now()
	assignment := contracts.Assignment{SchemaVersion: 1, AssignmentID: "a", RunID: "r", WorkerID: "w", SessionID: "s", PackageDigest: contracts.Hash(nil), Units: []contracts.UnitRef{{UnitID: u.UnitID, SpecDigest: u.SpecDigest}}, Runtime: u.Runtime, Profile: "small", PolicyRevision: 1, Allocation: contracts.Allocation{Resources: a}, StopNewUnitsAt: now, TerminateAt: now.Add(time.Minute), HandoffDeadline: now.Add(2 * time.Minute)}
	before, _ := contracts.Digest(assignment)
	assignment.Profile = "large-exclusive"
	assignment.Allocation = contracts.Allocation{Resources: contracts.ResourceVector{CPUMilli: 2000, MemoryBytes: 256 << 20, SpoolBytes: 16 << 20}, Exclusive: true}
	after, _ := contracts.Digest(assignment)
	same, _ := contracts.Digest(u)
	if before == after || same != scientific {
		t.Fatal("allocation not immutable assignment metadata or changed scientific identity")
	}
}
