// Package agent runs one explicitly configured local package profile. It does
// not qualify general process-tree containment or restart adoption (S3).
package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Config struct {
	Root, WorkerID, SessionID, Runtime, Profile string
	HostID                                      string
	SyntheticBackend                            bool
	SyntheticMemoryBytes                        int64
	SyntheticPhase                              string
	RuntimeArgv                                 []string
	Client                                      *Client
	MaxArtifactBytes                            int64
	ShutdownGrace                               time.Duration
	MaxConcurrent                               int
}
type journal struct {
	Work                     contracts.Work              `json:"work"`
	ProcessStarted           bool                        `json:"process_started"`
	Started                  []string                    `json:"started"`
	Candidates               []contracts.Candidate       `json:"candidates"`
	Delivered                int                         `json:"delivered"`
	ReceiptAcknowledged      bool                        `json:"receipt_acknowledged"`
	Quiescent                bool                        `json:"quiescent"`
	FinalSent                bool                        `json:"final_sent"`
	Control                  contracts.AssignmentControl `json:"control"`
	ReceiptWall              time.Time                   `json:"receipt_wall"`
	Uncertain                bool                        `json:"uncertain"`
	GateClosed               bool                        `json:"gate_closed"`
	ReconcileRevision        int64                       `json:"reconcile_revision"`
	OperationalReason        string                      `json:"operational_reason,omitempty"`
	AllocationAdmitted       bool                        `json:"allocation_admitted"`
	ResourceStop             *contracts.ResourceStop     `json:"resource_stop,omitempty"`
	LaunchFailed             bool                        `json:"launch_failed,omitempty"`
	ResourceStopAcknowledged bool                        `json:"resource_stop_acknowledged"`
}
type runner struct {
	cfg               Config
	mu                sync.Mutex
	j                 journal
	store             *artifacts.Store
	spool             *spool
	admission         *admission
	received          time.Time
	lastWall          time.Time
	terminalPersisted bool
	controlDurable    int64
	// Tests may replace signals on this owned live child; never a numeric PID lookup.
	testSignal func(*os.Process, os.Signal) error
	// Synthetic helper selectors supplied explicitly by tests, never inherited.
	testEnvironment []string
}

func ID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func durable(path string, value any) error {
	b, e := json.Marshal(value)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".journal-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func (r *runner) save() error {
	path := filepath.Join(r.cfg.Root, "journal.json")
	if e := durable(path, r.j); e != nil {
		return e
	}
	if r.spool != nil {
		if e := r.spool.TrackFile(r.j.Work.Assignment.AssignmentID, path); e != nil {
			if r.admission != nil {
				r.admission.block(e)
			}
			return e
		}
	}
	return nil
}
func retry(ctx context.Context, err error) error {
	if errors.Is(err, ErrProtocol) {
		return err
	}
	var response *HTTPError
	if errors.As(err, &response) && response.Status >= 400 && response.Status < 500 {
		return err
	}
	return wait(ctx)
}
func wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}
func (r *runner) receipt(suffix string) contracts.Receipt {
	a := r.j.Work.Assignment
	return contracts.Receipt{SchemaVersion: 1, RequestID: a.AssignmentID + ":" + suffix, AssignmentID: a.AssignmentID, WorkerID: a.WorkerID, SessionID: a.SessionID}
}
func (r *runner) assignment(ctx context.Context) (resultErr error) {
	// Delivery runs independently of grant progress. Local publication is the only
	// acknowledgment needed before the next issued unit; outage retains bytes.
	deliveryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	deliveryResult := make(chan error, 1)
	go func() { deliveryResult <- r.deliver(deliveryCtx) }()
	controlCtx, stopControl := context.WithCancel(ctx)
	controlDone := make(chan error, 1)
	go func() {
		err := r.controlLoop(controlCtx)
		if err != nil && controlCtx.Err() == nil {
			r.mu.Lock()
			r.j.GateClosed = true
			r.save()
			r.mu.Unlock()
		}
		controlDone <- err
	}()
	defer func() {
		stopControl()
		e := <-controlDone
		if resultErr == nil && e != nil && !errors.Is(e, context.Canceled) {
			resultErr = e
		}
	}()
	var executionErr error
	if !r.j.Quiescent {
		var admitted bool
		admitted, executionErr = r.waitAdmission(ctx)
		if admitted && executionErr == nil {
			fetchCtx, fetchCancel := context.WithDeadline(ctx, r.j.Work.Assignment.TerminateAt)
			executionErr = r.fetchInputs(fetchCtx)
			fetchCancel()
			if executionErr == nil {
				executionErr = r.execute(ctx)
			}
		} else {
			r.mu.Lock()
			r.j.Quiescent = true
			if executionErr != nil {
				r.j.OperationalReason = executionErr.Error()
			}
			saveErr := r.save()
			r.terminalPersisted = saveErr == nil
			r.mu.Unlock()
			if saveErr != nil {
				executionErr = saveErr
			}
		}
	}
	if executionErr != nil && !r.j.ProcessStarted && r.j.ResourceStop != nil {
		r.mu.Lock()
		r.j.Quiescent = true
		saveErr := r.save()
		r.terminalPersisted = saveErr == nil
		r.mu.Unlock()
		if saveErr != nil {
			executionErr = saveErr
		}
	}
	if r.admission != nil && r.terminalPersisted {
		r.admission.quiescent(r.j.Work.Assignment.AssignmentID)
	}
	if executionErr != nil && !r.terminalPersisted {
		r.mu.Lock()
		uncertain := r.j.Uncertain
		r.mu.Unlock()
		if uncertain {
			if err := r.reportReconciliation(ctx); err != nil {
				cancel()
				<-deliveryResult
				return fmt.Errorf("%v; report: %w", executionErr, err)
			}
			if err := <-deliveryResult; err != nil {
				return fmt.Errorf("%v; delivery: %w", executionErr, err)
			}
			return executionErr
		}
		cancel()
		<-deliveryResult
		return executionErr
	}
	if e := r.reportReconciliation(ctx); e != nil {
		cancel()
		<-deliveryResult
		return e
	}
	if e := <-deliveryResult; e != nil {
		return e
	}
	if e := r.reportResourceStop(ctx); e != nil {
		return e
	}
	r.mu.Lock()
	final := contracts.FinalReport{Receipt: r.receipt("final"), OperationalReason: r.j.OperationalReason, LaunchFailed: r.j.LaunchFailed}
	for _, c := range r.j.Candidates {
		final.Completed = append(final.Completed, c.UnitID)
	}
	for _, u := range r.j.Work.Assignment.Units {
		found := false
		for _, s := range r.j.Started {
			if s == u.UnitID {
				found = true
			}
		}
		if !found {
			final.NeverStarted = append(final.NeverStarted, u.UnitID)
		} else {
			completed := false
			for _, c := range r.j.Candidates {
				if c.UnitID == u.UnitID {
					completed = true
				}
			}
			if !completed {
				final.Interrupted = append(final.Interrupted, u.UnitID)
			}
		}
	}
	r.mu.Unlock()
	for {
		e := r.cfg.Client.Do(ctx, "POST", "/v1/final", final, nil)
		if e == nil {
			break
		}
		if e = retry(ctx, e); e != nil {
			return e
		}
	}
	r.mu.Lock()
	r.j.FinalSent = true
	e := r.save()
	controlled := r.j.Control.Revision > 0
	r.mu.Unlock()
	if e != nil {
		return e
	}
	if executionErr != nil && !controlled && r.j.ResourceStop == nil {
		return executionErr
	}
	return nil
}
func (r *runner) deliver(ctx context.Context) error {
	// Receipt reporting is retryable delivery, not permission to execute the
	// finite assignment already reserved and journaled locally.
	for {
		e := r.cfg.Client.Do(ctx, "POST", "/v1/receipt", r.receipt("receipt"), nil)
		if e == nil {
			break
		}
		if e = retry(ctx, e); e != nil {
			return e
		}
	}
	r.mu.Lock()
	if !r.j.ReceiptAcknowledged {
		r.j.ReceiptAcknowledged = true
		if err := r.save(); err != nil {
			r.mu.Unlock()
			return err
		}
	}
	r.mu.Unlock()
	for {
		r.mu.Lock()
		index := r.j.Delivered
		done := r.j.Quiescent || r.j.Uncertain
		var c contracts.Candidate
		if index < len(r.j.Candidates) {
			c = r.j.Candidates[index]
		}
		r.mu.Unlock()
		if c.UnitID == "" {
			if done {
				return nil
			}
			if e := wait(ctx); e != nil {
				return e
			}
			continue
		}
		var failure error
		for _, f := range c.Files {
			src, e := r.store.Open(f.Artifact.Digest)
			if e != nil {
				return e
			}
			failure = r.cfg.Client.Publish(ctx, f.Artifact.Digest, f.Artifact.Size, c.AssignmentID, src)
			src.Close()
			if failure != nil {
				break
			}
		}
		if failure == nil {
			report := contracts.CandidateReport{Receipt: r.receipt("candidate:" + contracts.Hash([]byte(c.UnitID))), Candidate: c}
			var ack contracts.Acknowledgment
			failure = r.cfg.Client.Do(ctx, "POST", "/v1/candidate", report, &ack)
			expected, _ := contracts.Digest(c)
			if failure == nil && (ack.SchemaVersion != 1 || ack.RawPublished != expected) {
				failure = fmt.Errorf("%w: candidate acknowledgment identity mismatch", ErrProtocol)
			}
		}
		if failure != nil {
			if e := retry(ctx, failure); e != nil {
				return e
			}
			continue
		}
		r.mu.Lock()
		r.j.Delivered++
		e := r.save()
		r.mu.Unlock()
		if e != nil {
			return e
		}
	}
}

type logBuffer struct{ buffer bytes.Buffer }

func (b *logBuffer) Write(p []byte) (int, error) {
	if b.buffer.Len()+len(p) > contracts.MaxMessageBytes {
		return 0, errors.New("application log limit")
	}
	return b.buffer.Write(p)
}
