package agent

import (
	"context"
	"errors"
	"hermes/internal/contracts"
	"hermes/internal/platform"
	"runtime"
)

func (h *host) initializeResources(ctx context.Context) error {
	owners := map[string]string{}
	for id := range h.active {
		owners[contracts.Hash([]byte(id))] = id
	}
	index, e := openSpool(h.cfg.Root, h.store, owners)
	if e != nil {
		return e
	}
	h.spool = index
	for _, r := range h.active {
		r.spool = index
		if e = r.trackWorktree(); e != nil {
			return e
		}
	}
	var active []string
	for id := range h.active {
		active = append(active, id)
	}
	snapshot, e := index.Snapshot(active)
	if e != nil {
		return e
	}
	observed, e := platform.ObserveResources(h.cfg.Root)
	if e != nil {
		return e
	}
	registration := contracts.WorkerRegistration{SchemaVersion: 1, RequestID: ID(), WorkerID: h.cfg.WorkerID, SessionID: h.cfg.SessionID, HostID: h.cfg.HostID, Observed: observed, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, RetainedSpoolBytes: snapshot.ClosedBytes, SpoolRevision: snapshot.Revision, InventoryRevision: h.inventory.Revision}
	var budget contracts.HostBudget
	for {
		e = h.cfg.Client.Do(ctx, "POST", "/v1/register", registration, &budget)
		if e == nil {
			break
		}
		if e = retry(ctx, e); e != nil {
			return e
		}
	}
	if budget.Approval.WorkerID != h.cfg.WorkerID || budget.Approval.HostID != h.cfg.HostID {
		return errors.New("registered budget identity mismatch")
	}
	if h.cfg.SyntheticBackend && !budget.AllowSyntheticBackend {
		return errors.New("synthetic backend is not approved")
	}
	h.admission, e = newAdmission(h.cfg.Root, index, budget)
	if e != nil {
		return e
	}
	if h.cfg.MaxConcurrent > budget.Approval.MaxAssignments {
		h.cfg.MaxConcurrent = budget.Approval.MaxAssignments
	}
	for _, r := range h.active {
		r.admission = h.admission
		if e = h.admission.register(r.j); e != nil {
			return e
		}
	}
	return nil
}
func (h *host) resourceSnapshot() (spoolSnapshot, error) {
	if h.admission == nil {
		return spoolSnapshot{}, nil
	}
	return h.admission.snapshot("")
}

// A confirmed empty reply has no persisted reservation/outcome. Replace its
// nonce only when retained inventory actually changed, never on an idle tick.
func (h *host) refreshEmptyPoll() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.inventory.Pending == nil || h.admission == nil {
		return nil
	}
	snapshot, e := h.resourceSnapshot()
	if e != nil {
		return e
	}
	old := h.inventory.Pending
	if old.SpoolRevision == snapshot.Revision && old.InventoryRevision == h.inventory.Revision {
		return nil
	}
	h.inventory.Pending = nil
	return h.save()
}
