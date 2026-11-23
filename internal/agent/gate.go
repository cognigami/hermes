package agent

import (
	"context"
	"errors"
	"fmt"
	"hermes/internal/contracts"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

func (r *runner) reportReconciliation(ctx context.Context) error {
	r.mu.Lock()
	r.j.ReconcileRevision++
	if e := r.save(); e != nil {
		r.mu.Unlock()
		return e
	}
	a := r.j.Work.Assignment
	digest, _ := contracts.Digest(*a)
	report := contracts.ReconcileRequest{Receipt: r.receipt(""), Revision: r.j.ReconcileRevision, OperationalReason: r.j.OperationalReason, AssignmentDigest: digest, ProcessStarted: r.j.ProcessStarted, Quiescent: r.j.Quiescent, Started: append([]string{}, r.j.Started...)}
	for _, c := range r.j.Candidates {
		d, _ := contracts.Digest(c)
		report.CandidateDigests = append(report.CandidateDigests, d)
	}
	r.mu.Unlock()
	report.RequestID = ""
	d, _ := contracts.Digest(report)
	report.RequestID = a.AssignmentID + ":reconcile:" + d
	for {
		var response contracts.ReconcileResult
		e := r.cfg.Client.Do(ctx, "POST", "/v1/reconcile", report, &response)
		if e == nil {
			if response.SchemaVersion != 1 || (response.State != "owned" && response.State != "terminal" && response.State != "uncertain" && response.State != "quiescent_pending_delivery") {
				return fmt.Errorf("%w: invalid reconcile response", ErrProtocol)
			}
			if response.State == "uncertain" && !r.j.Uncertain {
				r.mu.Lock()
				r.j.Uncertain = true
				r.j.GateClosed = true
				err := r.save()
				r.mu.Unlock()
				if err != nil {
					return err
				}
				return errors.New("controller reports uncertain ownership; no new starts")
			}
			if response.State == "terminal" && !r.j.Quiescent {
				return errors.New("retained assignment already terminal: explicit local reconciliation required")
			}
			return nil
		}
		if e = retry(ctx, e); e != nil {
			return e
		}
	}
}
func (r *runner) grant(id string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	open, err := r.gateOpenLocked()
	if !open {
		if err != nil {
			if e := r.save(); e != nil {
				return false, e
			}
		}
		return false, err
	}
	r.j.Started = append(r.j.Started, id)
	if e := r.save(); e != nil {
		r.j.GateClosed = true
		return false, e
	}
	return true, nil
}
func leafExitProven(err error) bool {
	var exited *exec.ExitError
	return err == nil || errors.As(err, &exited)
}
func (r *runner) gateOpenLocked() (bool, error) {
	a := r.j.Work.Assignment
	now := time.Now()
	wall := now.UTC()
	if wall.Before(r.j.ReceiptWall) || (!r.lastWall.IsZero() && wall.Before(r.lastWall)) {
		r.j.GateClosed = true
		r.j.OperationalReason = "clock validity uncertain"
		return false, errors.New("clock validity uncertain")
	}
	r.lastWall = wall
	if r.j.GateClosed || r.j.Control.Revision > 0 || r.j.Uncertain || !wall.Before(a.StopNewUnitsAt) || (!r.received.IsZero() && !now.Before(r.received.Add(a.StopNewUnitsAt.Sub(r.received.UTC())))) {
		return false, nil
	}
	return true, nil
}

func (r *runner) applyControl(c contracts.AssignmentControl) (contracts.ControlAck, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.j.Work.Assignment
	ack := contracts.ControlAck{Receipt: r.receipt("control:" + strconv.FormatInt(c.Revision, 10)), Revision: c.Revision, LastGrantIndex: len(r.j.Started)}
	if len(r.j.Started) > 0 {
		ack.LastGrantedUnitID = r.j.Started[len(r.j.Started)-1]
	}
	if c.SchemaVersion != 1 || c.AssignmentID != a.AssignmentID || c.Revision < 0 {
		return ack, errors.New("invalid control identity")
	}
	if c.Revision == 0 {
		if c.Action != "" || !c.TerminateAt.IsZero() {
			return ack, errors.New("invalid empty control")
		}
		return ack, nil
	}
	if c.Action != "drain" || c.TerminateAt.IsZero() || c.TerminateAt.After(a.TerminateAt) {
		return ack, errors.New("invalid drain cutoff")
	}
	persist := func() error {
		if e := r.save(); e != nil {
			return e
		}
		r.controlDurable = r.j.Control.Revision
		return nil
	}
	if c.Revision < r.j.Control.Revision {
		if r.controlDurable < r.j.Control.Revision {
			return ack, persist()
		}
		return ack, nil
	}
	if c.Revision == r.j.Control.Revision {
		if c != r.j.Control {
			return ack, errors.New("conflicting control revision")
		}
		if r.controlDurable < c.Revision {
			return ack, persist()
		}
		return ack, nil
	}
	r.j.Control = c
	return ack, persist()
}
func (r *runner) controlLoop(ctx context.Context) error {
	for {
		var control contracts.AssignmentControl
		e := r.cfg.Client.Do(ctx, "POST", "/v1/control/poll", r.receipt("control-poll"), &control)
		if e != nil {
			if e = retry(ctx, e); e != nil {
				return e
			}
			continue
		}
		ack, e := r.applyControl(control)
		if e != nil {
			return e
		}
		if control.Revision > 0 {
			if e = r.cfg.Client.Do(ctx, "POST", "/v1/control/ack", ack, nil); e != nil {
				if e = retry(ctx, e); e != nil {
					return e
				}
				continue
			}
		}
		if e = wait(ctx); e != nil {
			return e
		}
	}
}

// Only a still-owned child handle is signaled. This qualifies a leaf execution
// contract, never descendant containment or restart ownership from numeric IDs.
func (r *runner) signal(cmd *exec.Cmd, sig os.Signal) error {
	if r.testSignal != nil {
		return r.testSignal(cmd.Process, sig)
	}
	return cmd.Process.Signal(sig)
}
func (r *runner) supervise(ctx context.Context, cmd *exec.Cmd, stop <-chan struct{}) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	sent := false
	for {
		r.mu.Lock()
		cutoff := r.j.Work.Assignment.TerminateAt
		if r.j.Control.Revision > 0 && r.j.Control.TerminateAt.Before(cutoff) {
			cutoff = r.j.Control.TerminateAt
		}
		grace, base := r.cfg.ShutdownGrace, r.received
		r.mu.Unlock()
		remaining := time.Until(cutoff)
		if !base.IsZero() {
			if mono := time.Until(base.Add(cutoff.Sub(base.UTC()))); mono < remaining {
				remaining = mono
			}
		}
		if remaining <= 0 {
			if e := r.signal(cmd, syscall.SIGKILL); e != nil {
				return fmt.Errorf("force termination: %w", e)
			}
			return nil
		}
		if remaining <= grace && !sent {
			if e := r.signal(cmd, syscall.SIGTERM); e != nil {
				return fmt.Errorf("cooperative termination: %w", e)
			}
			sent = true
		}
		select {
		case <-stop:
			return nil
		case <-ctx.Done():
			if e := r.signal(cmd, syscall.SIGKILL); e != nil {
				return fmt.Errorf("cancel termination: %w", e)
			}
			return nil
		case <-ticker.C:
		}
	}
}
