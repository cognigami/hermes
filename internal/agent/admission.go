package agent

import (
	"context"
	"errors"
	"hermes/internal/contracts"
	"hermes/internal/platform"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type reservation struct {
	allocation          contracts.Allocation
	issued              contracts.HostBudget
	admitted, quiescent bool
}
type admission struct {
	mu      sync.Mutex
	budget  contracts.HostBudget
	entries map[string]reservation
	spool   *spool
	root    string
	failure error
	// Only tests replace this native metadata observation; not an approval source.
	observe func(string) (contracts.HostObservation, error)
}

func validateWorkAllocation(w contracts.Work) error {
	if w.Assignment == nil || w.Budget == nil {
		return errors.New("assignment lacks approved resource budget")
	}
	a := w.Assignment
	b := *w.Budget
	if e := validateBudget(b); e != nil {
		return e
	}
	if a.PolicyRevision != b.PolicyRevision || a.WorkerID != b.Approval.WorkerID {
		return errors.New("allocation/budget identity mismatch")
	}
	for _, p := range b.Profiles {
		if p.ID == a.Profile && p.Allocation == a.Allocation {
			return a.Allocation.Validate()
		}
	}
	return errors.New("assignment allocation differs from approved profile")
}
func newAdmission(root string, index *spool, budget contracts.HostBudget) (*admission, error) {
	if e := validateBudget(budget); e != nil {
		return nil, e
	}
	return &admission{budget: budget, entries: map[string]reservation{}, spool: index, root: root}, nil
}
func validateBudget(b contracts.HostBudget) error {
	if b.SchemaVersion != 1 || b.PolicyRevision < 1 || b.Approval.HostID == "" || b.Approval.WorkerID == "" || b.Approval.MaxAssignments < 1 || b.Approval.MaxAssignments > 64 || b.Approval.Limits.Validate() != nil || b.Approval.Baseline.Validate() != nil || b.ControllerOverhead.Validate() != nil {
		return errors.New("invalid approved host budget")
	}
	return nil
}
func (m *admission) block(e error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failure == nil {
		m.failure = e
	}
}
func (m *admission) err() error { m.mu.Lock(); defer m.mu.Unlock(); return m.failure }
func (m *admission) register(j journal) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	a := j.Work.Assignment
	if a == nil {
		return errors.New("assignment absent from resource reservation")
	}
	var issued contracts.HostBudget
	if a.Allocation.Resources != (contracts.ResourceVector{}) {
		if e := validateWorkAllocation(j.Work); e != nil {
			return e
		}
		issued = *j.Work.Budget
	}
	m.entries[a.AssignmentID] = reservation{allocation: a.Allocation, issued: issued, admitted: j.AllocationAdmitted, quiescent: j.Quiescent && !j.Uncertain}
	return nil
}
func (m *admission) remove(id string) { m.mu.Lock(); defer m.mu.Unlock(); delete(m.entries, id) }
func (m *admission) quiescent(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.entries[id]
	if ok {
		entry.quiescent = true
		m.entries[id] = entry
	}
}
func (m *admission) snapshot(omit string) (spoolSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failure != nil {
		return spoolSnapshot{}, m.failure
	}
	var active []string
	for id := range m.entries {
		if id != omit {
			active = append(active, id)
		}
	}
	return m.spool.Snapshot(active)
}
func (m *admission) update(b contracts.HostBudget) error {
	if e := validateBudget(b); e != nil {
		return e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if b.Approval.HostID != m.budget.Approval.HostID || b.Approval.WorkerID != m.budget.Approval.WorkerID {
		return errors.New("host budget identity changed")
	}
	if b.PolicyRevision >= m.budget.PolicyRevision {
		m.budget = b
	}
	return nil
}
func (m *admission) try(id string) (bool, error) {
	observer := m.observe
	if observer == nil {
		observer = platform.ObserveResources
	}
	observed, e := observer(m.root)
	if e != nil {
		return false, e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failure != nil {
		return false, m.failure
	}
	target, ok := m.entries[id]
	if !ok {
		return false, errors.New("assignment not registered for admission")
	}
	if e = target.allocation.Validate(); e != nil {
		return false, e
	}
	if target.allocation.RequiresHardTreeMemory && observed.HardTreeMemory != "supported" {
		return false, nil
	}
	var active []string
	for id := range m.entries {
		active = append(active, id)
	}
	snapshot, e := m.spool.Snapshot(active)
	if e != nil {
		m.failure = e
		return false, e
	}
	// Approval rights are immutable per issued receipt. Current defaults cannot
	// revoke old work or lend its entitlement to a new revision. Native usage
	// and retained bytes are nevertheless shared and freshly checked every start.
	rights := target.issued
	overhead := rights.ControllerOverhead
	if m.budget.ControllerOverhead.CPUMilli > overhead.CPUMilli {
		overhead.CPUMilli = m.budget.ControllerOverhead.CPUMilli
	}
	if m.budget.ControllerOverhead.MemoryBytes > overhead.MemoryBytes {
		overhead.MemoryBytes = m.budget.ControllerOverhead.MemoryBytes
	}
	if m.budget.ControllerOverhead.SpoolBytes > overhead.SpoolBytes {
		overhead.SpoolBytes = m.budget.ControllerOverhead.SpoolBytes
	}
	used, e := rights.Approval.Baseline.Add(overhead)
	if e != nil {
		return false, e
	}
	used, e = used.Add(contracts.ResourceVector{SpoolBytes: snapshot.ClosedBytes})
	if e == nil {
		used, e = used.Add(contracts.ResourceVector{SpoolBytes: max(rights.OtherRetainedSpoolBytes, m.budget.OtherRetainedSpoolBytes)})
	}
	if e != nil {
		return false, e
	}
	var futureSpool int64
	for other, entry := range m.entries {
		if e = entry.allocation.Validate(); e != nil {
			return false, nil
		}
		if other != id && !entry.quiescent && (target.allocation.Exclusive || entry.allocation.Exclusive) {
			return false, nil
		}
		v := entry.allocation.Resources
		owned := snapshot.OwnedBytes[other]
		if owned > v.SpoolBytes {
			v.SpoolBytes = owned
		}
		used, e = used.Add(v)
		if e != nil {
			return false, e
		}
		if owned < entry.allocation.Resources.SpoolBytes {
			headroom := entry.allocation.Resources.SpoolBytes - owned
			if futureSpool > int64(^uint64(0)>>1)-headroom {
				return false, errors.New("spool headroom overflow")
			}
			futureSpool += headroom
		}
	}
	if !used.Fits(rights.Approval.Limits) || !used.Fits(observed.Resources) || futureSpool > observed.AvailableSpoolBytes {
		return false, nil
	}
	target.admitted = true
	m.entries[id] = target
	return true, nil
}
func (r *runner) waitAdmission(ctx context.Context) (bool, error) {
	if r.admission == nil {
		return true, nil
	}
	if r.j.Work.Assignment.Allocation.Resources == (contracts.ResourceVector{}) {
		return false, errors.New("legacy assignment has no approved immutable allocation")
	}
	for {
		r.mu.Lock()
		open, e := r.gateOpenLocked()
		r.mu.Unlock()
		if e != nil || !open {
			return false, e
		}
		ok, e := r.admission.try(r.j.Work.Assignment.AssignmentID)
		if e != nil {
			return false, e
		}
		if ok {
			r.mu.Lock()
			r.j.AllocationAdmitted = true
			e = r.save()
			r.mu.Unlock()
			return e == nil, e
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Only active owned directories are walked, before process launch/recovery or
// after proven quiescence. No terminal-history traversal on admission/status.
func (r *runner) trackWorktree() error {
	if r.spool == nil {
		return nil
	}
	count := 0
	e := filepath.WalkDir(r.cfg.Root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		count++
		if count > 65536 {
			return errors.New("owned worktree metadata entry bound exceeded")
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("unsupported retained worktree symlink")
		}
		return r.spool.TrackFile(r.j.Work.Assignment.AssignmentID, path)
	})
	if e != nil && r.admission != nil {
		r.admission.block(e)
	}
	return e
}
func (r *runner) checkOwnedSpool() error {
	if r.admission == nil {
		return nil
	}
	snapshot, e := r.admission.snapshot("")
	if e != nil {
		return e
	}
	a := r.j.Work.Assignment
	owned := snapshot.OwnedBytes[a.AssignmentID]
	if owned > a.Allocation.Resources.SpoolBytes {
		r.mu.Lock()
		defer r.mu.Unlock()
		phase := "prepare"
		if len(r.j.Started) > 0 {
			phase = "unit"
		}
		return r.recordResourceStopLocked("spool_exhausted", "local_spool_accounting", phase, a.Allocation.Resources.SpoolBytes, contracts.ResourceVector{SpoolBytes: owned})
	}
	return nil
}
