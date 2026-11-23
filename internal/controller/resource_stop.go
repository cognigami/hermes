package controller

import (
	"database/sql"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"strings"
	"time"
)

func (c *Controller) resourceStop(tx *sql.Tx, r contracts.ResourceStop) (any, error) {
	a, e := c.bound(tx, r.Receipt)
	if e != nil {
		return nil, e
	}
	if e = a.Allocation.Validate(); e != nil {
		return nil, e
	}
	digest, _ := contracts.Digest(a.Allocation)
	if r.AllocationDigest != digest || r.EvidenceReference == "" || len(r.EvidenceReference) > 256 || r.ConfirmedMinimum.Validate() != nil {
		return nil, errors.New("invalid typed resource evidence binding")
	}
	p, e := c.policyAt(tx, a.PolicyRevision)
	if e != nil {
		return nil, e
	}
	switch r.Source {
	case "synthetic_test_event":
		if !p.AllowSyntheticBackend {
			return nil, errors.New("synthetic resource source is not approved")
		}
	case "local_spool_accounting":
		if r.Kind != "spool_exhausted" {
			return nil, errors.New("spool accountant cannot attest memory pressure")
		}
	case "qualified_backend_event":
		h, ok := approvalForWorker(p, a.WorkerID)
		if !ok {
			return nil, errors.New("resource host missing")
		}
		registration, e := c.workerObservation(tx, h)
		if e != nil {
			return nil, e
		}
		if r.Kind != "memory_limit" || registration.Observed.HardTreeMemory != "supported" {
			return nil, errors.New("required enforcement source unqualified")
		}
	default:
		return nil, errors.New("unqualified resource source")
	}
	var memoryFloor, spoolFloor int64
	switch r.Kind {
	case "memory_limit":
		memoryFloor = a.Allocation.Resources.MemoryBytes
		if r.KnownInadequateLimit != memoryFloor {
			return nil, errors.New("memory limit differs from immutable allocation")
		}
	case "spool_exhausted":
		spoolFloor = a.Allocation.Resources.SpoolBytes
		if r.KnownInadequateLimit != spoolFloor {
			return nil, errors.New("spool limit differs from immutable allocation")
		}
	default:
		return nil, errors.New("unknown resource stop kind")
	}
	refs := a.Units
	switch r.Phase {
	case "prepare":
		if r.UnitID != "" || r.GrantIndex != 0 {
			return nil, errors.New("preparation evidence has a unit grant")
		}
	case "unit":
		if r.GrantIndex < 1 || r.GrantIndex > len(a.Units) || a.Units[r.GrantIndex-1].UnitID != r.UnitID {
			return nil, errors.New("resource stop grant mismatch")
		}
		refs = []contracts.UnitRef{a.Units[r.GrantIndex-1]}
	default:
		return nil, errors.New("invalid resource stop phase")
	}
	var recorded int
	e = tx.QueryRow("SELECT last_grant FROM assignment_controls WHERE assignment_id=?", a.AssignmentID).Scan(&recorded)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	var reportBytes []byte
	e = tx.QueryRow("SELECT manifest FROM execution_reports WHERE assignment_id=?", a.AssignmentID).Scan(&reportBytes)
	if e == nil {
		var report contracts.ReconcileRequest
		if e = json.Unmarshal(reportBytes, &report); e != nil {
			return nil, e
		}
		if len(report.Started) > recorded {
			recorded = len(report.Started)
		}
	} else if !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	if recorded > r.GrantIndex {
		return nil, errors.New("resource evidence predates recorded grants")
	}
	canonical := r
	canonical.RequestID = ""
	body := encoded(canonical)
	for _, u := range refs {
		var old []byte
		e = tx.QueryRow("SELECT manifest FROM resource_failures WHERE run_id=? AND unit_id=? AND assignment_id=?", a.RunID, u.UnitID, a.AssignmentID).Scan(&old)
		if e == nil {
			if string(old) != string(body) {
				return nil, errors.New("resource cause for attempt is immutable")
			}
			continue
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return nil, e
		}
		var terminal int
		if e = tx.QueryRow("SELECT count(*) FROM assignments WHERE id=? AND (final IS NOT NULL OR EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=assignments.id))", a.AssignmentID).Scan(&terminal); e != nil {
			return nil, e
		}
		if terminal > 0 {
			return nil, errors.New("new resource cause cannot follow terminal handoff")
		}
		var owner string
		if e = tx.QueryRow("SELECT assignment_id FROM owners WHERE run_id=? AND unit_id=?", a.RunID, u.UnitID).Scan(&owner); e != nil || owner != a.AssignmentID {
			return nil, errors.New("resource evidence has no current owner")
		}
		var attempt int
		if e = tx.QueryRow("SELECT attempts FROM unit_attempts WHERE run_id=? AND unit_id=?", a.RunID, u.UnitID).Scan(&attempt); e != nil {
			return nil, e
		}
		if attempt < 1 {
			return nil, errors.New("resource attempt was not reserved")
		}
		minimum := r.ConfirmedMinimum
		after := time.Now().UTC().Add(100 * time.Millisecond).Format(time.RFC3339Nano)
		if _, e = tx.Exec("INSERT INTO resource_failures VALUES(?,?,?,?,?,?,?,?,?,?,?)", a.RunID, u.UnitID, a.AssignmentID, attempt, memoryFloor, spoolFloor, minimum.CPUMilli, minimum.MemoryBytes, minimum.SpoolBytes, after, body); e != nil {
			return nil, e
		}
		if _, e = tx.Exec("UPDATE unit_attempts SET reason='resource: waiting quiescence' WHERE run_id=? AND unit_id=?", a.RunID, u.UnitID); e != nil {
			return nil, e
		}
	}
	return contracts.Acknowledgment{SchemaVersion: 1, Received: true}, nil
}
func (c *Controller) reconcileResourceAssignment(tx *sql.Tx, id string) error {
	rows, e := tx.Query("SELECT f.run_id,f.unit_id,f.attempt_no FROM resource_failures f JOIN unit_attempts n ON n.run_id=f.run_id AND n.unit_id=f.unit_id WHERE f.assignment_id=? AND f.attempt_no=n.attempts", id)
	if e != nil {
		return e
	}
	type unit struct {
		run, id string
		attempt int
	}
	var units []unit
	for rows.Next() {
		var u unit
		if e = rows.Scan(&u.run, &u.id, &u.attempt); e != nil {
			rows.Close()
			return e
		}
		units = append(units, u)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, u := range units {
		var accepted, terminal, pending int
		if e = tx.QueryRow("SELECT count(*) FROM acceptances WHERE run_id=? AND unit_id=?", u.run, u.id).Scan(&accepted); e != nil {
			return e
		}
		reason := ""
		if accepted == 0 {
			if e = tx.QueryRow("SELECT count(*) FROM assignments a WHERE id=? AND (final IS NOT NULL OR EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id))", id).Scan(&terminal); e != nil {
				return e
			}
			if e = tx.QueryRow("SELECT count(*) FROM candidates WHERE run_id=? AND unit_id=? AND state!='rejected'", u.run, u.id).Scan(&pending); e != nil {
				return e
			}
			switch {
			case terminal == 0:
				reason = "resource: waiting quiescence"
			case pending > 0:
				reason = "resource: waiting retained validation"
			case u.attempt >= c.cfg.MaxAttempts:
				reason = "attempt budget exhausted"
			default:
				reason = "resource: backoff / waiting sufficient approved capacity"
			}
		}
		if _, e = tx.Exec("UPDATE unit_attempts SET reason=? WHERE run_id=? AND unit_id=?", reason, u.run, u.id); e != nil {
			return e
		}
	}
	return nil
}
func isResourceWait(reason string) bool { return strings.HasPrefix(reason, "resource:") }
