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
	var digest, run, unit string
	var b []byte
	tx, e := c.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	e = tx.QueryRow(`SELECT digest,run_id,unit_id,manifest FROM candidates WHERE state IN('pending','operational_failed') AND attempts<3 AND NOT EXISTS(SELECT 1 FROM acceptances a WHERE a.candidate=candidates.digest) ORDER BY rowid LIMIT 1`).Scan(&digest, &run, &unit, &b)
	if errors.Is(e, sql.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	if _, e = tx.Exec("UPDATE candidates SET state='validating',attempts=attempts+1 WHERE digest=?", digest); e != nil {
		return e
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	var v contracts.Candidate
	if e = json.Unmarshal(b, &v); e != nil {
		return e
	}
	var definition []byte
	if e = c.db.QueryRowContext(ctx, "SELECT definition FROM definitions WHERE unit_id=?", unit).Scan(&definition); e != nil {
		return e
	}
	var pinned contracts.Unit
	if e = json.Unmarshal(definition, &pinned); e != nil {
		return e
	}
	var verdict contracts.Verdict
	var operational error
	if pinned.Validator != c.cfg.Validator || pinned.Runtime != c.cfg.Runtime {
		operational = errors.New("configured validator/runtime does not match pinned unit")
	} else {
		verdict, operational = c.invoke(ctx, v, b, definition)
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
			if _, e = tx.Exec("UPDATE unit_attempts SET reason='' WHERE run_id=? AND unit_id=?", run, unit); e != nil {
				return e
			}
			if e = c.drainAcceptedRetry(tx, run, unit, v.AssignmentID); e != nil {
				return e
			}
		}

		if _, e = tx.Exec(`UPDATE run_units SET ready=1 WHERE run_id=? AND ready=0 AND NOT EXISTS(SELECT 1 FROM dependencies d WHERE d.run_id=run_units.run_id AND d.unit_id=run_units.unit_id AND NOT EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id=d.run_id AND a.unit_id=d.prerequisite))`, run); e != nil {
			return e
		}
	}
	if e = c.reconcileTerminal(tx); e != nil {
		return e
	}
	return c.commit(tx, "acceptance")
}

type limitedBuffer struct {
	bytes.Buffer
	max int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.max-b.Len() {
		return 0, errors.New("validator output limit")
	}
	return b.Buffer.Write(p)
}
func (c *Controller) invoke(ctx context.Context, v contracts.Candidate, manifest, unit []byte) (contracts.Verdict, error) {
	var result contracts.Verdict
	dir, e := os.MkdirTemp(c.cfg.Root, "validation-")
	if e != nil {
		return result, e
	}
	defer os.RemoveAll(dir)
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
	cmd := exec.CommandContext(bounded, c.cfg.ValidatorArgv[0], args...)
	out := &limitedBuffer{max: contracts.MaxMessageBytes}
	logs := &limitedBuffer{max: contracts.MaxMessageBytes}
	cmd.Stdout = out
	cmd.Stderr = logs
	if e = cmd.Run(); e != nil {
		return result, e
	}
	if e = contracts.Decode(bytes.NewReader(out.Bytes()), &result); e != nil {
		return result, e
	}
	return result, result.Validate()
}
