package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"time"
)

const controlSchema = `
CREATE TABLE IF NOT EXISTS target_controls(kind TEXT NOT NULL,target TEXT NOT NULL,revision INTEGER NOT NULL,action TEXT NOT NULL,terminate_at TEXT NOT NULL,PRIMARY KEY(kind,target));
CREATE TABLE IF NOT EXISTS assignment_controls(assignment_id TEXT PRIMARY KEY REFERENCES assignments(id),revision INTEGER NOT NULL,terminate_at TEXT NOT NULL,acked INTEGER NOT NULL DEFAULT 0,last_grant INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS execution_reports(assignment_id TEXT PRIMARY KEY REFERENCES assignments(id),state TEXT NOT NULL,manifest BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS terminal_evidence(assignment_id TEXT PRIMARY KEY REFERENCES assignments(id),manifest BLOB NOT NULL,reconciled INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS unit_attempts(run_id TEXT NOT NULL,unit_id TEXT NOT NULL,attempts INTEGER NOT NULL,reason TEXT NOT NULL,data_loss INTEGER NOT NULL,PRIMARY KEY(run_id,unit_id),FOREIGN KEY(run_id,unit_id) REFERENCES run_units(run_id,unit_id));
INSERT OR IGNORE INTO unit_attempts SELECT run_id,unit_id,count(*),'',0 FROM members GROUP BY run_id,unit_id;
`

func (c *Controller) allowedWorker(id string) bool {
	for _, w := range c.cfg.Workers {
		if w == id {
			return true
		}
	}
	return false
}
func (c *Controller) control(tx *sql.Tx, r contracts.ControlRequest) (any, error) {
	if r.SchemaVersion != 1 || (r.TargetKind != "run" && r.TargetKind != "worker") || r.TargetID == "" || (r.Action != "drain" && r.Action != "resume") || r.ExpectedRevision < 0 {
		return nil, errors.New("invalid control")
	}
	if r.TargetKind == "worker" {
		if !c.allowedWorker(r.TargetID) {
			return nil, errors.New("unknown worker")
		}
	} else {
		var n int
		if e := tx.QueryRow("SELECT count(*) FROM runs WHERE id=?", r.TargetID).Scan(&n); e != nil {
			return nil, e
		}
		if n != 1 {
			return nil, errors.New("unknown run")
		}
	}
	if r.Action == "drain" && r.TerminateAt.IsZero() {
		return nil, errors.New("drain requires finite cutoff")
	}
	var revision int64
	e := tx.QueryRow("SELECT revision FROM target_controls WHERE kind=? AND target=?", r.TargetKind, r.TargetID).Scan(&revision)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	if revision != r.ExpectedRevision {
		return nil, errors.New("stale control revision")
	}
	revision++
	if _, e = tx.Exec("INSERT INTO target_controls VALUES(?,?,?,?,?) ON CONFLICT(kind,target) DO UPDATE SET revision=excluded.revision,action=excluded.action,terminate_at=excluded.terminate_at", r.TargetKind, r.TargetID, revision, r.Action, r.TerminateAt.Format(time.RFC3339Nano)); e != nil {
		return nil, e
	}
	if r.Action == "drain" {
		column := "run_id"
		if r.TargetKind == "worker" {
			column = "worker"
		}
		rows, e := tx.Query("SELECT id,manifest FROM assignments WHERE "+column+"=? AND final IS NULL", r.TargetID)
		if e != nil {
			return nil, e
		}
		var pending []contracts.Assignment
		for rows.Next() {
			var id string
			var b []byte
			if e = rows.Scan(&id, &b); e != nil {
				rows.Close()
				return nil, e
			}
			var a contracts.Assignment
			if e = json.Unmarshal(b, &a); e != nil {
				rows.Close()
				return nil, e
			}
			pending = append(pending, a)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
		for _, a := range pending {
			if e = c.enqueueDrain(tx, a, r.TerminateAt); e != nil {
				return nil, e
			}
		}

	}
	return contracts.ControlStatus{SchemaVersion: 1, TargetKind: r.TargetKind, TargetID: r.TargetID, Revision: revision, IssuancePaused: r.Action == "drain", State: "intent_persisted"}, nil
}
func (c *Controller) enqueueDrain(tx *sql.Tx, a contracts.Assignment, requested time.Time) error {
	deadline := a.TerminateAt
	if requested.Before(deadline) {
		deadline = requested
	}
	var old string
	e := tx.QueryRow("SELECT terminate_at FROM assignment_controls WHERE assignment_id=?", a.AssignmentID).Scan(&old)
	if e == nil {
		prior, err := time.Parse(time.RFC3339Nano, old)
		if err != nil {
			return err
		}
		if prior.Before(deadline) {
			deadline = prior
		}
	} else if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	_, e = tx.Exec("INSERT INTO assignment_controls VALUES(?,1,?,0,0) ON CONFLICT(assignment_id) DO UPDATE SET revision=revision+1,terminate_at=excluded.terminate_at", a.AssignmentID, deadline.Format(time.RFC3339Nano))
	return e
}
func (c *Controller) drainAcceptedRetry(tx *sql.Tx, run, unit, source string) error {
	var b []byte
	e := tx.QueryRow("SELECT a.manifest FROM owners o JOIN assignments a ON a.id=o.assignment_id WHERE o.run_id=? AND o.unit_id=? AND a.id!=? AND a.final IS NULL", run, unit, source).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	var a contracts.Assignment
	if e = json.Unmarshal(b, &a); e != nil {
		return e
	}
	return c.enqueueDrain(tx, a, time.Now().UTC().Add(time.Second))
}

func (c *Controller) assignmentControl(tx *sql.Tx, r contracts.Receipt) (contracts.AssignmentControl, error) {
	a, e := c.bound(tx, r)
	out := contracts.AssignmentControl{SchemaVersion: 1, AssignmentID: a.AssignmentID}
	if e != nil {
		return out, e
	}
	var deadline string
	e = tx.QueryRow("SELECT revision,terminate_at FROM assignment_controls WHERE assignment_id=?", a.AssignmentID).Scan(&out.Revision, &deadline)
	if errors.Is(e, sql.ErrNoRows) {
		return out, nil
	}
	if e != nil {
		return out, e
	}
	out.Action = "drain"
	out.TerminateAt, e = time.Parse(time.RFC3339Nano, deadline)
	return out, e
}
func (c *Controller) ackControl(tx *sql.Tx, r contracts.ControlAck) (any, error) {
	a, e := c.bound(tx, r.Receipt)
	if e != nil {
		return nil, e
	}
	if r.LastGrantIndex < 0 || r.LastGrantIndex > len(a.Units) || (r.LastGrantIndex == 0 && r.LastGrantedUnitID != "") || (r.LastGrantIndex > 0 && a.Units[r.LastGrantIndex-1].UnitID != r.LastGrantedUnitID) {
		return nil, errors.New("invalid grant barrier")
	}
	var rev int64
	var last int
	if e = tx.QueryRow("SELECT revision,last_grant FROM assignment_controls WHERE assignment_id=?", a.AssignmentID).Scan(&rev, &last); e != nil {
		return nil, e
	}
	if r.Revision > rev || r.Revision < 1 || (r.Revision == rev && r.LastGrantIndex < last) {
		return nil, errors.New("invalid control acknowledgment revision")
	}
	if r.Revision == rev {
		if _, e = tx.Exec("UPDATE assignment_controls SET acked=?,last_grant=? WHERE assignment_id=?", rev, r.LastGrantIndex, a.AssignmentID); e != nil {
			return nil, e
		}
	}
	return contracts.Acknowledgment{SchemaVersion: 1, Received: true}, nil
}
func (c *Controller) ControlStatus(ctx context.Context, kind, target string) (contracts.ControlStatus, error) {
	s := contracts.ControlStatus{SchemaVersion: 1, TargetKind: kind, TargetID: target}
	switch kind {
	case "worker":
		if !c.allowedWorker(target) {
			return s, errors.New("unknown worker")
		}
	case "run":
		var count int
		if err := c.db.QueryRowContext(ctx, "SELECT count(*) FROM runs WHERE id=?", target).Scan(&count); err != nil {
			return s, err
		}
		if count != 1 {
			return s, errors.New("unknown run")
		}
	default:
		return s, errors.New("invalid target")
	}
	var action string
	e := c.db.QueryRowContext(ctx, "SELECT revision,action FROM target_controls WHERE kind=? AND target=?", kind, target).Scan(&s.Revision, &action)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return s, e
	}
	s.IssuancePaused = action == "drain"
	column := "run_id"
	if kind == "worker" {
		column = "worker"
	}
	for _, q := range []struct {
		sql string
		dst *int
	}{{"SELECT count(*) FROM assignments a WHERE a." + column + "=? AND a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)", &s.LiveAssignments}, {"SELECT count(*) FROM assignment_controls c JOIN assignments a ON a.id=c.assignment_id WHERE a." + column + "=? AND c.acked<c.revision AND a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)", &s.PendingBarriers}, {"SELECT count(*) FROM execution_reports e JOIN assignments a ON a.id=e.assignment_id WHERE a." + column + "=? AND e.state='uncertain' AND a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)", &s.UncertainAssignments}} {
		if e = c.db.QueryRowContext(ctx, q.sql, target).Scan(q.dst); e != nil {
			return s, e
		}
	}
	s.State = "eligible"
	if s.IssuancePaused {
		s.State = "draining"
		if s.LiveAssignments == 0 && s.PendingBarriers == 0 {
			s.State = "complete"
		} else if s.UncertainAssignments > 0 {
			s.State = "uncertain"
		} else if s.PendingBarriers > 0 {
			s.State = "pending_offline_expiry"
		} else {
			var running int
			if e = c.db.QueryRowContext(ctx, "SELECT count(*) FROM assignments a WHERE a."+column+"=? AND a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id) AND NOT EXISTS(SELECT 1 FROM execution_reports e WHERE e.assignment_id=a.id AND e.state='quiescent_pending_delivery')", target).Scan(&running); e != nil {
				return s, e
			}
			if running > 0 {
				s.State = "draining"
			} else {
				s.State = "pending_delivery"
			}
		}
	}
	return s, nil
}
