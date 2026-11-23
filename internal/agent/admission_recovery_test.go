package agent

import (
	"hermes/internal/contracts"
	"testing"
)

func admissionBudget() contracts.HostBudget {
	return contracts.HostBudget{SchemaVersion: 1, PolicyRevision: 1, Approval: contracts.HostApproval{HostID: "host", WorkerID: "worker", MaxAssignments: 4, Limits: contracts.ResourceVector{CPUMilli: 4000, MemoryBytes: 512 << 20, SpoolBytes: 64 << 20}, Baseline: contracts.ResourceVector{CPUMilli: 500, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}}, Profiles: []contracts.ResourceProfile{{ID: "small", Conservative: true, Allocation: contracts.Allocation{Resources: contracts.ResourceVector{CPUMilli: 1000, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}}}}}
}
func receiptForAdmission(id string, b contracts.HostBudget) journal {
	work := testWork()
	work.Assignment.AssignmentID = id
	work.Assignment.WorkerID = b.Approval.WorkerID
	work.Assignment.PolicyRevision = b.PolicyRevision
	work.Assignment.Profile = b.Profiles[0].ID
	work.Assignment.Allocation = b.Profiles[0].Allocation
	work.Budget = &b
	return journal{Work: work, AllocationAdmitted: true}
}
func TestRestoredAdmittedReceiptStillRequiresFreshNativeCapacity(t *testing.T) {
	for _, dimension := range []string{"cpu", "memory", "free-spool", "hard-enforcement"} {
		t.Run(dimension, func(t *testing.T) {
			root, _, index := spoolFixture(t)
			budget := admissionBudget()
			if dimension == "hard-enforcement" {
				budget.Profiles[0].Allocation.RequiresHardTreeMemory = true
			}
			manager, e := newAdmission(root, index, budget)
			if e != nil {
				t.Fatal(e)
			}
			j := receiptForAdmission("old", budget)
			if e = manager.register(j); e != nil {
				t.Fatal(e)
			}
			observation := contracts.HostObservation{Resources: budget.Approval.Limits, AvailableSpoolBytes: 64 << 20, HardTreeMemory: "supported"}
			switch dimension {
			case "cpu":
				observation.Resources.CPUMilli = 1000
			case "memory":
				observation.Resources.MemoryBytes = 64 << 20
			case "free-spool":
				observation.AvailableSpoolBytes = 1
			case "hard-enforcement":
				observation.HardTreeMemory = "unknown"
			}
			manager.observe = func(string) (contracts.HostObservation, error) { return observation, nil }
			ok, e := manager.try("old")
			if e != nil || ok {
				t.Fatal("persisted admitted bit bypassed fresh check", dimension, ok, e)
			}
		})
	}
}
func TestIssuedApprovalRightsSurviveDefaultShrinkWithoutLendingToNewWork(t *testing.T) {
	root, _, index := spoolFixture(t)
	old := admissionBudget()
	current := old
	current.PolicyRevision = 2
	current.Approval.Limits.CPUMilli = 1000
	manager, e := newAdmission(root, index, current)
	if e != nil {
		t.Fatal(e)
	}
	manager.observe = func(string) (contracts.HostObservation, error) {
		return contracts.HostObservation{Resources: old.Approval.Limits, AvailableSpoolBytes: 64 << 20, HardTreeMemory: "unknown"}, nil
	}
	if e = manager.register(receiptForAdmission("old", old)); e != nil {
		t.Fatal(e)
	}
	ok, e := manager.try("old")
	if e != nil || !ok {
		t.Fatal("current default revoked old receipt", ok, e)
	}
	if e = manager.register(receiptForAdmission("new", current)); e != nil {
		t.Fatal(e)
	}
	ok, e = manager.try("old")
	if e != nil || !ok {
		t.Fatal("old approval lost with coexisting new revision", ok, e)
	}
	ok, e = manager.try("new")
	if e != nil || ok {
		t.Fatal("new revision borrowed old larger rights", ok, e)
	}
	manager.observe = func(string) (contracts.HostObservation, error) {
		return contracts.HostObservation{Resources: contracts.ResourceVector{CPUMilli: 2000, MemoryBytes: 512 << 20, SpoolBytes: 64 << 20}, AvailableSpoolBytes: 64 << 20, HardTreeMemory: "unknown"}, nil
	}
	ok, e = manager.try("old")
	if e != nil || ok {
		t.Fatal("old rights ignored shared native aggregate", ok, e)
	}
}
