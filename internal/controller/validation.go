package controller

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// RunValidators uses one bounded isolated process at a time, never inside a transaction.
func (c *Controller) RunValidators(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if e := c.validateNext(ctx); e != nil {
			return e
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (c *Controller) validateNext(ctx context.Context) error {
	if e := c.cleanupExitedScratch(ctx); e != nil {
		return e
	}
	var digest, run, unit string
	var b []byte
	tx, e := c.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var position int64
	query := `SELECT rowid,digest,run_id,unit_id,manifest FROM candidates WHERE rowid>? AND state IN('pending','operational_failed') AND attempts<3 AND NOT EXISTS(SELECT 1 FROM acceptances a WHERE a.candidate=candidates.digest) ORDER BY rowid LIMIT 1`
	e = tx.QueryRow(query, c.validatorCursor).Scan(&position, &digest, &run, &unit, &b)
	if errors.Is(e, sql.ErrNoRows) && c.validatorCursor != 0 {
		c.validatorCursor = 0
		e = tx.QueryRow(query, 0).Scan(&position, &digest, &run, &unit, &b)
	}
	if errors.Is(e, sql.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	c.validatorCursor = position
	var v contracts.Candidate
	if e = json.Unmarshal(b, &v); e != nil {
		return e
	}
	var definition []byte
	if e = tx.QueryRow("SELECT definition FROM definitions WHERE unit_id=?", unit).Scan(&definition); e != nil {
		return e
	}
	var pinned contracts.Unit
	if e = json.Unmarshal(definition, &pinned); e != nil {
		return e
	}
	var claim *ChildClaim
	if pinned.Validator == c.cfg.Validator && pinned.Runtime == c.cfg.Runtime {
		minimum, e := validationScratch(v, b, definition)
		if e != nil {
			return e
		}
		claim, e = c.reserveControllerChild(tx, "validator", digest, minimum)
		if errors.Is(e, ErrControllerCapacity) {
			if _, err := tx.Exec("UPDATE candidates SET reason=? WHERE digest=? AND reason!=?", e.Error(), digest, e.Error()); err != nil {
				return err
			}
			return tx.Commit()
		}
		if e != nil {
			return e
		}
	}
	if _, e = tx.Exec("UPDATE candidates SET state='validating',attempts=attempts+1 WHERE digest=?", digest); e != nil {
		return e
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	var verdict contracts.Verdict
	var operational error
	if pinned.Validator != c.cfg.Validator || pinned.Runtime != c.cfg.Runtime {
		operational = errors.New("configured validator/runtime does not match pinned unit")
	} else {
		verdict, operational = c.invoke(ctx, v, b, definition, claim)
	}
	tx, e = c.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	state := "valid"
	reason := verdict.Reason
	if operational != nil {
		state = "operational_failed"
		reason = operational.Error()
	} else if !verdict.Valid {
		state = "rejected"
	}
	if _, e = tx.Exec("UPDATE candidates SET state=?,reason=? WHERE digest=?", state, reason, digest); e != nil {
		return e
	}
	if state == "valid" {
		inserted, err := tx.Exec("INSERT OR IGNORE INTO acceptances VALUES(?,?,?,?,?)", run, unit, digest, c.cfg.Validator, encoded(verdict))
		if err != nil {
			return err
		}
		n, err := inserted.RowsAffected()
		if err != nil {
			return err
		}
		if n == 1 {
			if e = acceptReadiness(tx, run, unit); e != nil {
				return e
			}
			if _, e = tx.Exec("UPDATE unit_attempts SET reason='' WHERE run_id=? AND unit_id=?", run, unit); e != nil {
				return e
			}
			if e = c.drainAcceptedRetry(tx, run, unit, v.AssignmentID); e != nil {
				return e
			}
		}

	}
	if e = c.reconcileTerminal(tx); e != nil {
		return e
	}
	if e = c.reconcileResourceAssignment(tx, v.AssignmentID); e != nil {
		return e
	}
	return c.commit(tx, "acceptance")
}

type limitedBuffer struct {
	buffer bytes.Buffer
	max    int
}

func (b *limitedBuffer) Bytes() []byte  { return b.buffer.Bytes() }
func (b *limitedBuffer) String() string { return b.buffer.String() }

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.max-b.buffer.Len() {
		return 0, errors.New("validator output limit")
	}
	return b.buffer.Write(p)
}
func validationScratch(v contracts.Candidate, manifest, unit []byte) (contracts.ResourceVector, error) {
	amount := contracts.ResourceVector{SpoolBytes: int64(len(manifest)) + int64(len(unit))}
	for _, f := range v.Files {
		var e error
		amount, e = amount.Add(contracts.ResourceVector{SpoolBytes: f.Artifact.Size})
		if e != nil {
			return amount, e
		}
	}
	return amount, nil
}
func (c *Controller) invoke(ctx context.Context, v contracts.Candidate, manifest, unit []byte, claim *ChildClaim) (result contracts.Verdict, resultErr error) {
	minimum, e := validationScratch(v, manifest, unit)
	if e != nil {
		return result, e
	}
	relative := "validation-" + claim.id
	started := false
	defer func() {
		if !started {
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
	if e = claim.ReserveScratch(ctx, relative, minimum.SpoolBytes); e != nil {
		return result, e
	}
	dir := filepath.Join(c.cfg.Root, relative)
	if e = os.Mkdir(dir, 0700); e != nil {
		return result, e
	}
	for _, f := range v.Files {
		src, e := c.store.Open(f.Artifact.Digest)
		if e != nil {
			return result, e
		}
		target := filepath.Join(dir, "result", filepath.FromSlash(f.Path))
		if e = os.MkdirAll(filepath.Dir(target), 0700); e != nil {
			src.Close()
			return result, e
		}
		dst, e := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			src.Close()
			return result, e
		}
		_, e = io.Copy(dst, src)
		closeErr := dst.Close()
		src.Close()
		if e != nil {
			return result, e
		}
		if closeErr != nil {
			return result, closeErr
		}
	}
	if e = os.WriteFile(filepath.Join(dir, "candidate.json"), manifest, 0600); e != nil {
		return result, e
	}
	if e = os.WriteFile(filepath.Join(dir, "unit.json"), unit, 0600); e != nil {
		return result, e
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	args := append([]string{}, c.cfg.ValidatorArgv[1:]...)
	args = append(args, "--manifest", filepath.Join(dir, "candidate.json"), "--root", filepath.Join(dir, "result"), "--unit", filepath.Join(dir, "unit.json"))
	cmd := exec.Command(c.cfg.ValidatorArgv[0], args...)
	out := &limitedBuffer{max: contracts.MaxMessageBytes}
	logs := &limitedBuffer{max: contracts.MaxMessageBytes}
	cmd.Stdout = out
	cmd.Stderr = logs
	started = true
	if e = runClaimedLeaf(bounded, claim, cmd); e != nil {
		return result, e
	}
	if e = contracts.Decode(bytes.NewReader(out.Bytes()), &result); e != nil {
		return result, e
	}
	return result, result.Validate()
}
