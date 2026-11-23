package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hermes/internal/contracts"
	"os/exec"
)

var ErrControllerCapacity = errors.New("waiting for approved controller child capacity")

// ChildClaim is a controller-local validator/continuation reservation, not a
// process owner or a job framework. Callers retain their own live Cmd handle.
type childResources struct {
	Role, Reference string
	Resources       contracts.ResourceVector
}

type ChildClaim struct {
	controller *Controller
	id         string
	instance   string
}

func (c *Controller) ReserveControllerChild(ctx context.Context, role, reference string, minimum contracts.ResourceVector) (*ChildClaim, error) {
	tx, e := c.db.BeginTx(ctx, nil)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	claim, e := c.reserveControllerChild(tx, role, reference, minimum)
	if e != nil {
		return nil, e
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	return claim, nil
}
func (c *Controller) reserveControllerChild(tx *sql.Tx, role, reference string, minimum contracts.ResourceVector) (*ChildClaim, error) {
	if c.lifetime.Err() != nil {
		return nil, errors.New("controller closing")
	}
	if role != "validator" && role != "continuation" {
		return nil, errors.New("unsupported controller child role")
	}
	if len(reference) > 256 {
		return nil, errors.New("controller child reference too large")
	}
	p, e := c.policy(tx)
	if e != nil {
		return nil, e
	}
	if !minimum.Fits(p.ValidatorReservation) {
		return nil, fmt.Errorf("%w: per-path staging/minimum exceeds approved helper reservation", ErrControllerCapacity)
	}
	var count int
	if e = tx.QueryRow("SELECT count(*) FROM validator_claims WHERE exited=0").Scan(&count); e != nil {
		return nil, e
	}
	if count >= p.ValidatorSlots {
		return nil, ErrControllerCapacity
	}
	id := randomID()
	record := childResources{role, reference, p.ValidatorReservation}
	if _, e = tx.Exec("INSERT INTO validator_claims(id,resources,instance) VALUES(?,?,?)", id, encoded(record), c.instance); e != nil {
		return nil, e
	}
	host, ok := approvalForHost(p, p.ControllerHostID)
	if !ok {
		return nil, errors.New("controller host missing")
	}
	used, limit, e := c.hostUsage(tx, p, host, -1)
	if e != nil {
		return nil, e
	}
	if e = c.addOutputReservations(tx, &used); e != nil {
		return nil, e
	}
	if !used.Fits(limit) {
		if _, e = tx.Exec("DELETE FROM validator_claims WHERE id=?", id); e != nil {
			return nil, e
		}
		return nil, ErrControllerCapacity
	}
	return &ChildClaim{c, id, c.instance}, nil
}
func (claim *ChildClaim) Resources(ctx context.Context) (contracts.ResourceVector, error) {
	var b []byte
	if e := claim.controller.db.QueryRowContext(ctx, "SELECT resources FROM validator_claims WHERE id=? AND instance=?", claim.id, claim.instance).Scan(&b); e != nil {
		return contracts.ResourceVector{}, e
	}
	var record childResources
	if e := json.Unmarshal(b, &record); e != nil {
		return contracts.ResourceVector{}, e
	}
	return record.Resources, nil
}

// ObserveWait uses the actual owned Cmd after Wait. A copy/output error does
// not erase its positive OS ProcessState, but remains an operational error.
func (claim *ChildClaim) ObserveWait(ctx context.Context, cmd *exec.Cmd, waitErr error) error {
	if cmd == nil || cmd.Process == nil || cmd.ProcessState == nil {
		return errors.New("owned Wait has no process exit state")
	}
	_, e := claim.controller.db.ExecContext(ctx, "UPDATE validator_claims SET exited=1 WHERE id=? AND instance=?", claim.id, claim.instance)
	return e
}

func (claim *ChildClaim) Started(ctx context.Context) error {
	result, e := claim.controller.db.ExecContext(ctx, "UPDATE validator_claims SET started=1 WHERE id=? AND instance=? AND exited=0", claim.id, claim.instance)
	if e != nil {
		return e
	}
	n, e := result.RowsAffected()
	if e != nil {
		return e
	}
	if n != 1 {
		return errors.New("controller child claim is not live")
	}
	return nil
}

// ObserveExit accepts only actual Wait results that establish observed exit.
// Unknown errors retain the reservation. No PID, timeout or lock heuristic.
func (claim *ChildClaim) ObserveExit(ctx context.Context, waitErr error) error {
	var exited *exec.ExitError
	if waitErr != nil && !errors.As(waitErr, &exited) {
		return fmt.Errorf("controller child exit unproven: %w", waitErr)
	}
	_, e := claim.controller.db.ExecContext(ctx, "UPDATE validator_claims SET exited=1 WHERE id=? AND instance=?", claim.id, claim.instance)
	return e
}

// ObserveStartFailure requires the actual failed Cmd.Start result and no process
// handle. This is live no-child evidence, never a fabricated successful Wait.
func (claim *ChildClaim) ObserveStartFailure(ctx context.Context, cmd *exec.Cmd, startErr error) error {
	if cmd == nil || startErr == nil || cmd.Process != nil {
		return errors.New("failed Start did not establish no child")
	}
	_, e := claim.controller.db.ExecContext(ctx, "UPDATE validator_claims SET exited=1 WHERE id=? AND instance=?", claim.id, claim.instance)
	return e
}

func (claim *ChildClaim) CancelBeforeStart(ctx context.Context) error {
	_, e := claim.controller.db.ExecContext(ctx, "UPDATE validator_claims SET exited=1 WHERE id=? AND instance=? AND started=0", claim.id, claim.instance)
	return e
}
func (c *Controller) restoreChildClaims(tx *sql.Tx) error {
	_, e := tx.Exec("UPDATE validator_claims SET exited=1 WHERE started=0 AND instance!=?", c.instance)
	return e
}
