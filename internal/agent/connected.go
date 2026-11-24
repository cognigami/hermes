package agent

import (
	"context"
	"errors"
	"hermes/internal/contracts"
	"path/filepath"
	"sync"
	"time"
)

type assignmentCompletion struct {
	id  string
	err error
}

func Run(ctx context.Context, cfg Config) error {
	if cfg.Root == "" || cfg.Client == nil || cfg.Client.HTTP == nil || cfg.WorkerID == "" || cfg.SessionID == "" || cfg.Runtime == "" || cfg.Profile == "" || len(cfg.RuntimeArgv) == 0 || cfg.MaxArtifactBytes <= 0 {
		return errors.New("incomplete agent config")
	}
	if !filepath.IsAbs(cfg.RuntimeArgv[0]) {
		return errors.New("runtime executable must be absolute")
	}
	if cfg.ShutdownGrace == 0 {
		cfg.ShutdownGrace = 100 * time.Millisecond
	}
	if cfg.ShutdownGrace < 0 {
		return errors.New("invalid shutdown grace")
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = maxActiveJournals
	}
	if cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > maxActiveJournals {
		return errors.New("invalid active assignment bound")
	}
	if cfg.HostID == "" {
		cfg.HostID = cfg.WorkerID
	}
	if cfg.SyntheticMemoryBytes < 0 || cfg.SyntheticMemoryBytes > 0 && !cfg.SyntheticBackend {
		return errors.New("synthetic demand requires explicit synthetic backend")
	}
	if cfg.SyntheticPhase == "" {
		cfg.SyntheticPhase = "unit"
	}
	if cfg.SyntheticPhase != "unit" && cfg.SyntheticPhase != "prepare" {
		return errors.New("invalid synthetic stop phase")
	}
	h, e := openHost(cfg)
	if e != nil {
		return e
	}
	defer h.Close()
	for _, r := range h.active {
		if r.j.Finite != nil {
			return errors.New("retained finite work requires finite execution/import acknowledgment, not connected polling")
		}
	}
	runningCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	// Resolve the one durable in-flight poll before recovery can start anything.
	if h.inventory.Pending != nil {
		var w contracts.Work
		for {
			e = cfg.Client.Do(ctx, "POST", "/v1/poll", *h.inventory.Pending, &w)
			if e == nil {
				break
			}
			if e = retry(ctx, e); e != nil {
				return e
			}
		}
		if e = validateWork(w); e != nil {
			return e
		}
		if e = h.confirmInventory(); e != nil {
			return e
		}
		if w.Assignment != nil {
			if _, e = h.receive(w); e != nil {
				return e
			}
		}
	}
	uncertain := false
	for _, r := range h.active {
		r.received = time.Now()
		if r.j.FinalSent {
			continue
		}
		if r.j.Uncertain || ((r.j.ProcessStarted || len(r.j.Started) > 0) && !r.j.Quiescent) {
			r.j.Uncertain = true
			if e = r.save(); e != nil {
				return e
			}
			uncertain = true
		}
		if e = r.reportReconciliation(ctx); e != nil {
			return e
		}
	}
	if uncertain {
		for _, r := range h.active {
			if r.j.Uncertain {
				if e = r.deliver(ctx); e != nil {
					return e
				}
			} else if r.j.Quiescent && !r.j.FinalSent {
				if e = r.assignment(ctx); e != nil {
					return e
				}
			}
		}
		return errors.New("retained execution ownership uncertain; host admission closed")
	}
	if e = h.initializeResources(ctx); e != nil {
		return e
	}
	cfg.MaxConcurrent = h.cfg.MaxConcurrent
	done := make(chan assignmentCompletion, maxActiveJournals)
	started := map[string]bool{}
	start := func(r *runner) {
		id := r.j.Work.Assignment.AssignmentID
		if started[id] {
			return
		}
		started[id] = true
		wg.Add(1)
		go func() { defer wg.Done(); done <- assignmentCompletion{id, r.assignment(runningCtx)} }()
	}
	for {
		if e = h.admission.err(); e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		for id, r := range h.active {
			r.mu.Lock()
			finalSent := r.j.FinalSent
			r.mu.Unlock()
			if finalSent && !started[id] {
				if e = h.retire(r); e != nil {
					return e
				}
				continue
			}
			if !started[id] && len(started) < cfg.MaxConcurrent {
				r.received = time.Now()
				start(r)
			}
		}
		select {
		case completion := <-done:
			r := h.active[completion.id]
			delete(started, completion.id)
			if completion.err != nil {
				return completion.err
			}
			if e = h.retire(r); e != nil {
				return e
			}
			continue
		default:
		}
		request, e := h.pollRequest()
		if e != nil {
			return e
		}
		if request == nil {
			select {
			case completion := <-done:
				r := h.active[completion.id]
				delete(started, completion.id)
				if completion.err != nil {
					return completion.err
				}
				if e = h.retire(r); e != nil {
					return e
				}
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(25 * time.Millisecond):
			}
			continue
		}
		var work contracts.Work
		e = cfg.Client.Do(ctx, "POST", "/v1/poll", *request, &work)
		if e != nil {
			if e = retry(ctx, e); e != nil {
				return e
			}
			continue
		}
		if e = validateWork(work); e != nil {
			return e
		}
		if e = h.confirmInventory(); e != nil {
			return e
		}
		if work.Assignment == nil {
			if e = h.refreshEmptyPoll(); e != nil {
				return e
			}
			if e = wait(ctx); e != nil {
				return e
			}
			continue
		}
		r, e := h.receive(work)
		if e != nil {
			return e
		}
		if len(started) < cfg.MaxConcurrent {
			r.received = time.Now()
			start(r)
		}
	}
}
func (h *host) confirmInventory() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.inventory.Reconciled {
		return nil
	}
	h.inventory.Reconciled = true
	h.inventory.Revision++
	return h.save()
}

func (h *host) pollRequest() (*contracts.PollRequest, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.inventory.Pending != nil {
		return h.inventory.Pending, nil
	}
	if len(h.active) >= h.cfg.MaxConcurrent {
		return nil, nil
	}
	p := &contracts.PollRequest{SchemaVersion: 1, RequestID: ID(), WorkerID: h.cfg.WorkerID, SessionID: h.cfg.SessionID}
	for _, entry := range h.inventory.Entries {
		r := h.active[entry.AssignmentID]
		if r == nil {
			return nil, errors.New("missing active journal")
		}
		r.mu.Lock()
		ready := r.j.ReceiptAcknowledged && !r.j.Uncertain
		r.mu.Unlock()
		if !ready {
			return nil, nil
		}
		p.Retained = append(p.Retained, contracts.RetainedAssignment{AssignmentID: entry.AssignmentID, Digest: entry.Digest})
	}
	snapshot, e := h.resourceSnapshot()
	if e != nil {
		return nil, e
	}
	p.RetainedSpoolBytes = snapshot.ClosedBytes
	p.SpoolRevision = snapshot.Revision
	p.InventoryRevision = h.inventory.Revision
	h.inventory.Pending = p
	if e := h.save(); e != nil {
		return nil, e
	}
	return p, nil
}
