package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"strings"
)

// ReserveControllerStorage records an exact relative retained-copy path BEFORE
// its creator writes bytes. Runtime copies are permanent charges (no implicit GC).
func (c *Controller) ReserveControllerStorage(ctx context.Context, relative string, size int64) error {
	return c.reserveStorage(ctx, relative, size, "")
}
func (c *Controller) reserveStorage(ctx context.Context, relative string, size int64, claim string) error {
	if !contracts.ValidPath(relative) || size < 0 {
		return errors.New("invalid controller storage reservation")
	}
	tx, e := c.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var previous int64
	var previousClaim sql.NullString
	e = tx.QueryRow("SELECT size,claim_id FROM controller_storage WHERE path=?", relative).Scan(&previous, &previousClaim)
	if e == nil {
		if previous != size || previousClaim.String != claim {
			return errors.New("controller retained-copy reservation is immutable")
		}
		return nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	var owner any
	if claim != "" {
		owner = claim
	}
	if _, e = tx.Exec("INSERT INTO controller_storage VALUES(?,?,?)", relative, size, owner); e != nil {
		return e
	}
	p, e := c.policy(tx)
	if e != nil {
		return e
	}
	host, ok := approvalForHost(p, p.ControllerHostID)
	if !ok {
		return errors.New("controller approval missing")
	}
	used, limit, e := c.hostUsage(tx, p, host, -1)
	if e != nil {
		return e
	}
	if e = c.addOutputReservations(tx, &used); e != nil {
		return e
	}
	if !used.Fits(limit) {
		return ErrControllerCapacity
	}
	return tx.Commit()
}
func (claim *ChildClaim) ReserveScratch(ctx context.Context, relative string, size int64) error {
	if relative != "validation-"+claim.id && relative != "continuation-"+claim.id {
		return errors.New("scratch directory must be bound to child claim")
	}
	var b []byte
	if e := claim.controller.db.QueryRowContext(ctx, "SELECT resources FROM validator_claims WHERE id=? AND instance=? AND exited=0", claim.id, claim.instance).Scan(&b); e != nil {
		return e
	}
	var record childResources
	if e := json.Unmarshal(b, &record); e != nil {
		return e
	}
	if size < 0 || size > record.Resources.SpoolBytes {
		return errors.New("per-path staging exceeds approved helper spool reservation")
	}
	return claim.controller.reserveStorage(ctx, relative, size, claim.id)
}

// CleanupScratch never derives exit from deadline, PID or lock state. Failed or
// unknown cleanup leaves its durable byte charge intact across controller death.
func (claim *ChildClaim) CleanupScratch(ctx context.Context) error {
	var exited int
	if e := claim.controller.db.QueryRowContext(ctx, "SELECT exited FROM validator_claims WHERE id=? AND instance=?", claim.id, claim.instance).Scan(&exited); e != nil {
		return e
	}
	if exited != 1 {
		return errors.New("child exit is not proven; scratch remains charged")
	}
	rows, e := claim.controller.db.QueryContext(ctx, "SELECT path FROM controller_storage WHERE claim_id=?", claim.id)
	if e != nil {
		return e
	}
	var paths []string
	for rows.Next() {
		var path string
		if e = rows.Scan(&path); e != nil {
			rows.Close()
			return e
		}
		paths = append(paths, path)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, relative := range paths {
		if !contracts.ValidPath(relative) || !strings.HasSuffix(relative, claim.id) {
			return errors.New("invalid child scratch path")
		}
		path := filepath.Join(claim.controller.cfg.Root, filepath.FromSlash(relative))
		if e = os.RemoveAll(path); e != nil {
			return e
		}
		if _, e = os.Lstat(path); !errors.Is(e, os.ErrNotExist) {
			if e == nil {
				return errors.New("scratch still retained")
			}
			return e
		}
		parent, e := os.Open(claim.controller.cfg.Root)
		if e != nil {
			return e
		}
		e = parent.Sync()
		parent.Close()
		if e != nil {
			return e
		}
		if _, e = claim.controller.db.ExecContext(ctx, "DELETE FROM controller_storage WHERE path=? AND claim_id=?", relative, claim.id); e != nil {
			return e
		}
	}
	return nil
}
