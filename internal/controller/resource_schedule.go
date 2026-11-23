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
	active, e := c.validateRetained(tx, p)
	if e != nil {
		return nil, e
	}
	policy, e := c.policy(tx)
	if e != nil {
		return nil, e
	}
	host, ok := approvalForWorker(policy, p.WorkerID)
	if !ok || active >= c.cfg.MaxConcurrent || active >= host.MaxAssignments {
		return contracts.Work{SchemaVersion: 1}, nil
	}
	var paused int
	if e = tx.QueryRow("SELECT count(*) FROM target_controls WHERE kind='worker' AND target=? AND action='drain'", p.WorkerID).Scan(&paused); e != nil {
		return nil, e
	}
	if paused > 0 {
		return contracts.Work{SchemaVersion: 1}, nil
	}
	// One indexed package per transaction bounds eligibility work. Advancing the
	// per-worker keyset cursor also gives an unfit head no permanent veto.
	packages, e := readyPackages(tx, c.readyCursor[p.WorkerID], 1)
	if e != nil {
		return nil, e
	}
	if len(packages) == 0 {
		previous := c.readyCursor[p.WorkerID]
		c.readyCursor[p.WorkerID] = 0
		if previous != 0 {
			packages, e = readyPackages(tx, 0, 1)
			if e != nil {
				return nil, e
			}
		}
		if len(packages) == 0 {
			return contracts.Work{SchemaVersion: 1}, nil
		}
	}
	x := packages[0]
	c.readyCursor[p.WorkerID] = x.Cursor
	var selected []contracts.UnitRef
	var profile contracts.ResourceProfile
	var revision int64
	for _, ref := range x.Ready {
		eligible, e := c.resourceReady(tx, x.RunID, ref.UnitID)
		if e != nil {
			return nil, e
		}
		if !eligible {
			continue
		}
		chosen, rev, e := c.profileFor(tx, p, x.RunID, []contracts.UnitRef{ref})
		if e != nil {
			return nil, e
		}
		if chosen.ID == "" {
			continue
		}
		if len(selected) == 0 {
			profile = chosen
			revision = rev
		}
		if chosen.ID != profile.ID {
			continue
		}
		selected = append(selected, ref)
		if len(selected) == 64 {
			break
		}
	}
	if len(selected) == 0 {
		return contracts.Work{SchemaVersion: 1}, nil
	}
	now := time.Now().UTC()
	a := contracts.Assignment{SchemaVersion: 1, AssignmentID: "assignment-" + contracts.Hash([]byte(p.WorkerID+":"+p.RequestID+":"+p.SessionID)), RunID: x.RunID, WorkerID: p.WorkerID, SessionID: p.SessionID, PackageDigest: x.Digest, Units: selected, Runtime: c.cfg.Runtime, Profile: profile.ID, PolicyRevision: revision, Allocation: profile.Allocation, StopNewUnitsAt: now.Add(c.cfg.StopAfter), TerminateAt: now.Add(c.cfg.TerminateAfter), HandoffDeadline: now.Add(c.cfg.HandoffAfter)}
	if e = a.Validate(); e != nil {
		return nil, e
	}
	if _, e = tx.Exec("INSERT INTO assignments(id,run_id,worker,session,manifest) VALUES(?,?,?,?,?)", a.AssignmentID, a.RunID, a.WorkerID, a.SessionID, encoded(a)); e != nil {
		return nil, e
	}
	if e = c.reserveAllocation(tx, host, a); e != nil {
		return nil, e
	}
	if e = c.saveSpoolAccount(tx, host, p); e != nil {
		return nil, e
	}
	for _, ref := range selected {
		if _, e = tx.Exec("INSERT INTO unit_attempts VALUES(?,?,1,'',0) ON CONFLICT(run_id,unit_id) DO UPDATE SET attempts=attempts+1,reason=''", a.RunID, ref.UnitID); e != nil {
			return nil, e
		}
		if _, e = tx.Exec("INSERT INTO members VALUES(?,?,?)", a.AssignmentID, a.RunID, ref.UnitID); e != nil {
			return nil, e
		}
		if _, e = tx.Exec("INSERT INTO owners VALUES(?,?,?)", a.RunID, ref.UnitID, a.AssignmentID); e != nil {
			return nil, e
		}
	}
	return c.work(tx, a)
}
