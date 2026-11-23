package controller

import (
	"context"
	"errors"
	"hermes/internal/platform"
	"os/exec"
	"time"
)

func runClaimedLeaf(ctx context.Context, claim *ChildClaim, cmd *exec.Cmd) (resultErr error) {
	attempted := false
	defer func() {
		if !attempted {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			e := claim.CancelBeforeStart(cleanup)
			if e == nil {
				e = claim.CleanupScratch(cleanup)
			}
			if e != nil {
				resultErr = errors.Join(resultErr, e)
			}
		}
	}()
	// Only explicit resource/thread hints may be added; never inherit controller credentials.
	resources, e := claim.Resources(ctx)
	if e != nil {
		return e
	}
	hints, e := platform.NativeThreadEnvironment(resources.CPUMilli)
	if e != nil {
		return e
	}
	cmd.Env = append([]string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C"}, hints...)
	if e := claim.Started(ctx); e != nil {
		return e
	}
	attempted = true
	if e := cmd.Start(); e != nil {
		proof, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		bookkeeping := claim.ObserveStartFailure(proof, cmd, e)
		if bookkeeping == nil {
			bookkeeping = claim.CleanupScratch(proof)
		}
		return errors.Join(e, bookkeeping)
	}
	type exit struct{ wait, bookkeeping error }
	done := make(chan exit, 1)
	go func() {
		waitErr := cmd.Wait()
		var bookkeeping error
		if cmd.ProcessState != nil {
			metadata, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			bookkeeping = claim.ObserveWait(metadata, cmd, waitErr)
			if bookkeeping == nil {
				bookkeeping = claim.CleanupScratch(metadata)
			}
		}
		done <- exit{waitErr, bookkeeping}
	}()
	select {
	case observed := <-done:
		return errors.Join(observed.wait, observed.bookkeeping)
	case <-ctx.Done():
		signalErr := cmd.Process.Kill()
		select {
		case observed := <-done:
			return errors.Join(ctx.Err(), observed.bookkeeping)
		case <-time.After(100 * time.Millisecond):
			return errors.Join(errors.New("controller child exit unproven; capacity and scratch retained"), ctx.Err(), signalErr)
		}
	}
}
