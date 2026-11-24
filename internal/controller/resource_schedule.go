package controller

import (
	"database/sql"
	"errors"
	"hermes/internal/contracts"
	"time"
)

func (c *Controller) resourceReady(tx *sql.Tx, run, unit string) (bool, error) {
	var attempts int
	var reason string
	e := tx.QueryRow("SELECT attempts,reason FROM unit_attempts WHERE run_id=? AND unit_id=?", run, unit).Scan(&attempts, &reason)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return false, e
	}
	if attempts >= c.cfg.MaxAttempts || (reason != "" && !isResourceWait(reason)) {
		return false, nil
	}
	var rejected int
	if e = tx.QueryRow("SELECT count(*) FROM candidates WHERE run_id=? AND unit_id=? AND state='rejected'", run, unit).Scan(&rejected); e != nil {
		return false, e
	}
	var after string
	e = tx.QueryRow("SELECT next_after FROM resource_failures f JOIN assignments a ON a.id=f.assignment_id WHERE f.run_id=? AND f.unit_id=? AND attempt_no=? AND (a.final IS NOT NULL OR EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)) ORDER BY f.rowid DESC LIMIT 1", run, unit, attempts).Scan(&after)
	if errors.Is(e, sql.ErrNoRows) {
		return rejected == 0, nil
	}
	if e != nil {
		return false, e
	}
	at, e := time.Parse(time.RFC3339Nano, after)
	if e != nil {
		return false, e
	}
	return !time.Now().Before(at), nil
}
func (c *Controller) poll(tx *sql.Tx, p contracts.PollRequest) (any, error) {
	if p.SchemaVersion != 1 || !c.allowedWorker(p.WorkerID) || p.SessionID == "" || len(p.SessionID) > 256 || p.RetainedSpoolBytes < 0 || p.SpoolRevision < 0 || p.InventoryRevision < 0 {
		return nil, errors.New("invalid worker session/resource inventory")
	}
	// Connected inventory/receipt checks are not finite start authority.
	if _, e := c.validateRetained(tx, p); e != nil {
		return nil, e
	}
	work, e := c.reserveReadyAssignment(tx, reservationInventory{Worker: p.WorkerID, Session: p.SessionID, Key: p.RequestID, RetainedSpoolBytes: p.RetainedSpoolBytes, Covered: p.Retained})
	if e != nil {
		return nil, e
	}
	if work.Assignment != nil {
		policy, e := c.policy(tx)
		if e != nil {
			return nil, e
		}
		host, ok := approvalForWorker(policy, p.WorkerID)
		if !ok {
			return nil, errors.New("selected worker host approval missing")
		}
		if e = c.saveSpoolAccount(tx, host, p); e != nil {
			return nil, e
		}
	}
	return work, nil
}
