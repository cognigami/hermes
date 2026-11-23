package agent

import (
	"context"
	"errors"
	"fmt"
	"hermes/internal/contracts"
	"hermes/internal/platform"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

func (r *runner) execute(ctx context.Context) (resultErr error) {
	r.terminalPersisted = false
	a := r.j.Work.Assignment
	bounded, cancel := context.WithDeadline(ctx, a.TerminateAt)
	defer cancel()
	dir := filepath.Join(r.cfg.Root, "work")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	output := filepath.Join(dir, "output")
	if e := os.MkdirAll(output, 0700); e != nil {
		return e
	}
	manifest := filepath.Join(dir, "manifest.json")
	local := struct {
		SchemaVersion int                   `json:"schema_version"`
		Assignment    *contracts.Assignment `json:"assignment"`
		Units         []contracts.Unit      `json:"units"`
	}{1, a, r.j.Work.Units}
	if e := durable(manifest, local); e != nil {
		return e
	}
	if e := durable(filepath.Join(dir, "package.json"), r.j.Work.Package); e != nil {
		return e
	}
	if e := r.stage(bounded, dir); e != nil {
		return e
	}
	if e := r.trackWorktree(); e != nil {
		return e
	}
	if e := r.checkOwnedSpool(); e != nil {
		return e
	}
	r.mu.Lock()
	if e := r.syntheticStopLocked("prepare"); e != nil {
		r.mu.Unlock()
		return e
	}
	open, gateErr := r.gateOpenLocked()
	if !open {
		r.j.Quiescent = true
		e := r.save()
		r.terminalPersisted = e == nil
		r.mu.Unlock()
		if e != nil {
			return e
		}
		return gateErr
	}
	launched := false
	defer func() {
		if launched {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		r.j.LaunchFailed = true
		r.j.GateClosed = true
		r.j.Quiescent = true
		r.j.Uncertain = false
		if resultErr != nil {
			r.j.OperationalReason = resultErr.Error()
			if len(r.j.OperationalReason) > 4096 {
				r.j.OperationalReason = r.j.OperationalReason[:4096]
			}
		}
		if saveErr := r.save(); saveErr != nil {
			resultErr = errors.Join(resultErr, saveErr)
		} else {
			r.terminalPersisted = true
		}
	}()
	r.j.ProcessStarted = true
	e := r.save()
	r.mu.Unlock()
	if e != nil {
		return e
	}
	args := append([]string{}, r.cfg.RuntimeArgv[1:]...)
	args = append(args, "--manifest", manifest, "--output", output)
	// All signals, including cancellation, go through the same owned-handle path.
	// CommandContext would silently install a second, unchecked Kill path.
	cmd := exec.Command(r.cfg.RuntimeArgv[0], args...)
	cmd.Dir = dir
	cpu := a.Allocation.Resources.CPUMilli
	if cpu == 0 {
		cpu = 1000
	}
	hints, envErr := platform.NativeThreadEnvironment(cpu)
	if envErr != nil {
		return envErr
	}
	cmd.Env = append([]string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C"}, hints...)
	cmd.Env = append(cmd.Env, r.testEnvironment...)
	stdin, e := cmd.StdinPipe()
	if e != nil {
		return e
	}
	defer stdin.Close()
	stdout, e := cmd.StdoutPipe()
	if e != nil {
		return e
	}
	defer stdout.Close()
	cmd.Stderr = &logBuffer{}
	if e = cmd.Start(); e != nil {
		return e
	}
	launched = true
	stop := make(chan struct{})
	monitor := make(chan error, 1)
	go func() {
		err := r.supervise(bounded, cmd, stop)
		if err != nil {
			r.mu.Lock()
			r.j.GateClosed = true
			r.j.OperationalReason = "enforcement uncertainty: " + err.Error()
			saveErr := r.save()
			r.mu.Unlock()
			if saveErr != nil {
				err = fmt.Errorf("%v; journal: %w", err, saveErr)
			}
		}
		// Interrupt protocol waits at termination too: successful signal delivery
		// still does not prove exit or guarantee that a pipe will reach EOF.
		stdin.Close()
		stdout.Close()
		monitor <- err
	}()
	exit := make(chan error, 1)
	waitStarted, waited, monitorRead := false, false, false
	var waitErr, enforcementErr error
	beginWait := func() {
		if !waitStarted {
			waitStarted = true
			go func() { exit <- cmd.Wait() }()
		}
	}
	defer func() {
		close(stop)
		if !monitorRead {
			enforcementErr = <-monitor
		}
		if !waited {
			if enforcementErr == nil {
				enforcementErr = r.signal(cmd, syscall.SIGKILL)
			}
			beginWait()
			// Signal delivery is not exit proof. Even successful Kill may leave an
			// uninterruptible process; never wait without a bound on the failure path.
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case waitErr = <-exit:
				waited = true
			case <-timer.C:
			}
			timer.Stop()
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		// ProcessState is read only after receiving this owned Cmd.Wait result.
		// Copy/log errors can coexist with a positive OS exit observation.
		if !waited || cmd.ProcessState == nil {
			r.j.Uncertain = true
			r.j.GateClosed = true
			r.j.Quiescent = false
			reason := "enforcement uncertainty: leaf exit proof unavailable"
			if waited && waitErr != nil {
				reason = "enforcement uncertainty: wait failed: " + waitErr.Error()
			}
			if enforcementErr != nil {
				reason = "enforcement uncertainty: " + enforcementErr.Error()
			}
			if len(reason) > 4096 {
				reason = reason[:4096]
			}
			r.j.OperationalReason = reason
			resultErr = errors.New(reason)
			if err := r.save(); err != nil {
				resultErr = err
			}
			return
		}
		r.j.Quiescent = true
		if resultErr != nil {
			reason := resultErr.Error()
			if r.j.Control.Revision > 0 {
				reason = "drain interruption"
			} else if bounded.Err() != nil {
				reason = "assignment deadline/cancellation"
			}
			if enforcementErr != nil {
				reason = "signal error with observed leaf exit: " + enforcementErr.Error()
			}
			if len(reason) > 4096 {
				reason = reason[:4096]
			}
			r.j.OperationalReason = reason
		}
		if err := r.save(); err != nil {
			resultErr = err
		} else {
			r.terminalPersisted = true
		}
	}()
	reader := contracts.MessageReader(stdout)
	m, e := contracts.ReadMessage(reader)
	if e != nil || m.Type != "ready" {
		return fmt.Errorf("application ready: %v", e)
	}
	for _, u := range a.Units {
		if err := bounded.Err(); err != nil {
			return err
		}
		granted, e := r.grant(u.UnitID)
		if e != nil {
			return e
		}
		if !granted {
			break
		}
		if e = contracts.WriteMessage(stdin, contracts.Message{SchemaVersion: 1, Type: "execute_unit", UnitID: u.UnitID}); e != nil {
			return e
		}
		m, e = contracts.ReadMessage(reader)
		if e != nil {
			return e
		}
		if m.Type != "unit_result" || m.UnitID != u.UnitID {
			return errors.New("unexpected unit result")
		}
		candidate, e := r.collect(bounded, output, m.ManifestPath, u)
		if e != nil {
			return e
		}
		if e = r.persistCandidate(candidate); e != nil {
			return e
		}
		if e = r.trackWorktree(); e != nil {
			return e
		}
		if e = r.checkOwnedSpool(); e != nil {
			return e
		}
		if e = contracts.WriteMessage(stdin, contracts.Message{SchemaVersion: 1, Type: "published", UnitID: u.UnitID}); e != nil {
			return e
		}
	}
	if e = contracts.WriteMessage(stdin, contracts.Message{SchemaVersion: 1, Type: "finish"}); e != nil {
		return e
	}
	m, e = contracts.ReadMessage(reader)
	if e != nil || m.Type != "finished" {
		return errors.New("missing finished")
	}
	beginWait()
	select {
	case waitErr = <-exit:
		waited = true
		return waitErr
	case enforcementErr = <-monitor:
		monitorRead = true
		if enforcementErr != nil {
			return enforcementErr
		}
	case <-bounded.Done():
		return bounded.Err()
	}
	// A successful termination signal still requires observed exit, with a bound.
	select {
	case waitErr = <-exit:
		waited = true
		return waitErr
	case <-time.After(100 * time.Millisecond):
		return errors.New("leaf exit proof unavailable after termination")
	}
}

// Only durable manifest records become visible to the independent publisher.
func (r *runner) persistCandidate(candidate contracts.Candidate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.j.Candidates = append(r.j.Candidates, candidate)
	if e := r.save(); e != nil {
		r.j.Candidates = r.j.Candidates[:len(r.j.Candidates)-1]
		r.j.GateClosed = true
		return e
	}
	return nil
}

func (r *runner) collect(ctx context.Context, output, path string, u contracts.UnitRef) (contracts.Candidate, error) {
	var c contracts.Candidate
	root, e := os.OpenRoot(output)
	if e != nil {
		return c, e
	}
	defer root.Close()
	f, e := openRegular(root, path)
	if e != nil {
		return c, e
	}
	e = contracts.Decode(f, &c)
	f.Close()
	if e != nil {
		return c, e
	}
	if e = c.Validate(); e != nil {
		return c, e
	}
	a := r.j.Work.Assignment
	if c.UnitID != u.UnitID || c.SpecDigest != u.SpecDigest || c.AssignmentID != a.AssignmentID || c.AttemptID != a.AssignmentID {
		return c, errors.New("unbound candidate")
	}
	for _, ref := range c.Files {
		src, e := openRegular(root, ref.Path)
		if e != nil {
			return c, e
		}
		e = r.publishBlob(ctx, ref.Artifact.Digest, ref.Artifact.Size, src)
		src.Close()
		if e != nil {
			return c, e
		}
	}
	return c, nil
}
