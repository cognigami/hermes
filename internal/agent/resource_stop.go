package agent

import (
	"context"
	"errors"
	"fmt"
	"hermes/internal/contracts"
)

var errResourceStopped = errors.New("execution stopped by typed infrastructure resource evidence")

func (r *runner) recordResourceStopLocked(kind, source, phase string, limit int64, minimum contracts.ResourceVector) error {
	if r.j.ResourceStop != nil {
		return errResourceStopped
	}
	a := r.j.Work.Assignment
	digest, _ := contracts.Digest(a.Allocation)
	event := contracts.ResourceStop{Receipt: r.receipt("resource-stop"), AllocationDigest: digest, Phase: phase, Kind: kind, Source: source, EvidenceReference: source + ":" + contracts.Hash([]byte(a.AssignmentID)), KnownInadequateLimit: limit, ConfirmedMinimum: minimum}
	if phase == "unit" {
		event.GrantIndex = len(r.j.Started)
		if event.GrantIndex < 1 {
			return errors.New("resource event has no current grant")
		}
		event.UnitID = r.j.Started[event.GrantIndex-1]
	}
	r.j.ResourceStop = &event
	r.j.GateClosed = true
	if e := r.save(); e != nil {
		return e
	}
	return errResourceStopped
}
func (r *runner) syntheticStopLocked(phase string) error {
	if !r.cfg.SyntheticBackend || r.cfg.SyntheticMemoryBytes == 0 || r.cfg.SyntheticPhase != phase || r.j.Work.Assignment.Allocation.Resources.MemoryBytes >= r.cfg.SyntheticMemoryBytes {
		return nil
	}
	if r.j.Work.Budget == nil || !r.j.Work.Budget.AllowSyntheticBackend {
		return errors.New("synthetic infrastructure event is not approved")
	}
	return r.recordResourceStopLocked("memory_limit", "synthetic_test_event", phase, r.j.Work.Assignment.Allocation.Resources.MemoryBytes, contracts.ResourceVector{MemoryBytes: r.cfg.SyntheticMemoryBytes})
}
func (r *runner) reportResourceStop(ctx context.Context) error {
	r.mu.Lock()
	event := r.j.ResourceStop
	acknowledged := r.j.ResourceStopAcknowledged
	r.mu.Unlock()
	if event == nil || acknowledged {
		return nil
	}
	for {
		var ack contracts.Acknowledgment
		e := r.cfg.Client.Do(ctx, "POST", "/v1/resource-stop", *event, &ack)
		if e == nil && (ack.SchemaVersion != 1 || !ack.Received) {
			return fmt.Errorf("%w: invalid resource evidence acknowledgment", ErrProtocol)
		}
		if e == nil {
			break
		}
		if e = retry(ctx, e); e != nil {
			return e
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.j.ResourceStopAcknowledged = true
	return r.save()
}
