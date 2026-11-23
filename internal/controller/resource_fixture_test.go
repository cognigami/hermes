package controller

import (
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// These ceilings describe logical supplied-input test hosts, not entitlements
// or qualification of several physical machines on the test runner.
func fixtureResourcePolicy(workers []string) *contracts.ResourcePolicy {
	p := &contracts.ResourcePolicy{SchemaVersion: 1, Revision: 1, ControllerHostID: "controller", TransferSlots: 2, ValidatorSlots: 2, TransferReservation: contracts.ResourceVector{CPUMilli: 100, MemoryBytes: 4 << 20, SpoolBytes: 1 << 20}, ValidatorReservation: contracts.ResourceVector{CPUMilli: 100, MemoryBytes: 64 << 20, SpoolBytes: 4 << 20}, AllowSyntheticBackend: true}
	host := contracts.HostApproval{HostID: "controller", Limits: contracts.ResourceVector{CPUMilli: 1000, MemoryBytes: 512 << 20, SpoolBytes: 64 << 20}, Baseline: contracts.ResourceVector{CPUMilli: 125, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}, MaxAssignments: 4}
	p.Hosts = append(p.Hosts, host)
	for _, worker := range workers {
		h := host
		h.HostID = worker
		h.WorkerID = worker
		p.Hosts = append(p.Hosts, h)
	}
	p.Profiles = []contracts.ResourceProfile{{ID: "synthetic-local", Conservative: true, Allocation: contracts.Allocation{Resources: contracts.ResourceVector{CPUMilli: 250, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}}}, {ID: "synthetic-large", Conservative: true, Allocation: contracts.Allocation{Resources: contracts.ResourceVector{CPUMilli: 500, MemoryBytes: 256 << 20, SpoolBytes: 16 << 20}, Exclusive: true}}}
	return p
}
func seedFixtureInventory(t *testing.T, c *Controller) {
	t.Helper()
	tx, e := c.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	for _, worker := range c.cfg.Workers {
		r := contracts.WorkerRegistration{SchemaVersion: 1, RequestID: "fixture-register-" + worker, WorkerID: worker, SessionID: "fixture-inventory", HostID: worker, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Observed: contracts.HostObservation{Resources: contracts.ResourceVector{CPUMilli: 1000, MemoryBytes: 512 << 20, SpoolBytes: 64 << 20}, AvailableSpoolBytes: 64 << 20, HardTreeMemory: "unknown", Source: "synthetic_test"}}
		if _, e = c.registerHost(tx, r); e != nil {
			t.Fatal(e)
		}
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
}
func saveFixturePolicy(t *testing.T, root string, p *contracts.ResourcePolicy) {
	t.Helper()
	if e := os.WriteFile(filepath.Join(filepath.Dir(root), "resource-policy.json"), encoded(p), 0600); e != nil {
		t.Fatal(e)
	}
}
