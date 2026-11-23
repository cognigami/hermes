package controller

import (
	"database/sql"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
)

func (c *Controller) reconcile(tx *sql.Tx, r contracts.ReconcileRequest) (any, error) {
	a, e := c.bound(tx, r.Receipt)
	if e != nil {
		return nil, e
	}
	digest, _ := contracts.Digest(a)
	if len(r.OperationalReason) > 4096 || r.Revision < 1 || digest != r.AssignmentDigest || len(r.Started) > len(a.Units) || len(r.CandidateDigests) > len(a.Units) || (!r.ProcessStarted && len(r.Started) > 0) {
		return nil, errors.New("invalid reconciliation identity/inventory")
	}
	for i, id := range r.Started {
		if a.Units[i].UnitID != id {
			return nil, errors.New("invalid reconciled grant order")
		}
	}
	seen := map[string]bool{}
	for _, d := range r.CandidateDigests {
		if len(d) != 64 || seen[d] {
			return nil, errors.New("invalid candidate inventory")
		}
		seen[d] = true
	}
	var final []byte
	if e = tx.QueryRow("SELECT final FROM assignments WHERE id=?", a.AssignmentID).Scan(&final); e != nil {
		return nil, e
	}
	var terminal int
	if e = tx.QueryRow("SELECT count(*) FROM terminal_evidence WHERE assignment_id=?", a.AssignmentID).Scan(&terminal); e != nil {
		return nil, e
	}
	if final != nil || terminal > 0 {
		return contracts.ReconcileResult{SchemaVersion: 1, State: "terminal"}, nil
	}
	var oldBody []byte
	var oldState string
	e = tx.QueryRow("SELECT manifest,state FROM execution_reports WHERE assignment_id=?", a.AssignmentID).Scan(&oldBody, &oldState)
	if e == nil {
		var previous contracts.ReconcileRequest
		if e = json.Unmarshal(oldBody, &previous); e != nil {
			return nil, e
		}
		if r.Revision < previous.Revision {
			return contracts.ReconcileResult{SchemaVersion: 1, State: oldState}, nil
		}
		if r.Revision == previous.Revision {
			return nil, errors.New("conflicting reconciliation revision")
		}
		if len(r.Started) < len(previous.Started) || (previous.ProcessStarted && !r.ProcessStarted) || (previous.Quiescent && !r.Quiescent) {
			return nil, errors.New("reconciliation progress regressed")
		}
		if oldState == "uncertain" && len(r.Started) != len(previous.Started) {
			return nil, errors.New("new grants after uncertain ownership")
		}
		for _, d := range previous.CandidateDigests {
			if !seen[d] {
				return nil, errors.New("candidate inventory regressed")
			}
		}
	} else if !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	var barrier int
	e = tx.QueryRow("SELECT last_grant FROM assignment_controls WHERE assignment_id=?", a.AssignmentID).Scan(&barrier)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	if len(r.Started) < barrier {
		return nil, errors.New("reconciliation regresses acknowledged barrier")
	}
	state := "owned"
	if r.Quiescent {
		state = "quiescent_pending_delivery"
	}
	if (r.ProcessStarted && !r.Quiescent) || oldState == "uncertain" {
		state = "uncertain"
	}
	if _, e = tx.Exec("INSERT INTO execution_reports VALUES(?,?,?) ON CONFLICT(assignment_id) DO UPDATE SET state=excluded.state,manifest=excluded.manifest", a.AssignmentID, state, encoded(r)); e != nil {
		return nil, e
	}
	return contracts.ReconcileResult{SchemaVersion: 1, State: state}, nil
}
func (c *Controller) resolveLostSpool(tx *sql.Tx, r contracts.LostSpoolResolution) (any, error) {
	a, e := c.bound(tx, r.Receipt)
	if e != nil {
		return nil, e
	}
	if !r.SpoolIrrecoverablyLost || !r.ExecutionTerminated || (r.EvidenceKind != "operator_exact_execution_terminated" && r.EvidenceKind != "operator_confirmed_machine_shutdown") || r.Reason == "" || len(r.Reason) > 4096 {
		return nil, errors.New("positive exact termination AND irrecoverable spool loss evidence required")
	}
	if _, e = tx.Exec("INSERT INTO terminal_evidence(assignment_id,manifest) VALUES(?,?)", a.AssignmentID, encoded(r)); e != nil {
		return nil, e
	}
	if e = c.reconcileTerminal(tx); e != nil {
		return nil, e
	}
	return contracts.Acknowledgment{SchemaVersion: 1, Received: true}, nil
}

// Terminal proof does not bypass retained-result validation. Missing units can
// retry only after their own old execution is proven terminal; attempts persist.
func (c *Controller) reconcileTerminal(tx *sql.Tx) error {
	rows, e := tx.Query("SELECT t.assignment_id,a.run_id FROM terminal_evidence t JOIN assignments a ON a.id=t.assignment_id WHERE reconciled=0")
	if e != nil {
		return e
	}
	type record struct{ id, run string }
	var records []record
	for rows.Next() {
		var r record
		if e = rows.Scan(&r.id, &r.run); e != nil {
			rows.Close()
			return e
		}
		records = append(records, r)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, r := range records {
		rows, e = tx.Query("SELECT unit_id FROM members WHERE assignment_id=?", r.id)
		if e != nil {
			return e
		}
		var ids []string
		for rows.Next() {
			var id string
			if e = rows.Scan(&id); e != nil {
				rows.Close()
				return e
			}
			ids = append(ids, id)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		waiting := false
		for _, id := range ids {
			var accepted, pending, retained, attempts int
			for _, q := range []struct {
				sql  string
				args []any
				dst  *int
			}{{"SELECT count(*) FROM acceptances WHERE run_id=? AND unit_id=?", []any{r.run, id}, &accepted}, {"SELECT count(*) FROM candidates WHERE run_id=? AND unit_id=? AND state IN('pending','validating','operational_failed')", []any{r.run, id}, &pending}, {"SELECT count(*) FROM candidates WHERE run_id=? AND unit_id=?", []any{r.run, id}, &retained}, {"SELECT attempts FROM unit_attempts WHERE run_id=? AND unit_id=?", []any{r.run, id}, &attempts}} {
				if e = tx.QueryRow(q.sql, q.args...).Scan(q.dst); e != nil {
					return e
				}
			}
			reason := ""
			loss := 0
			if accepted == 0 {
				if pending > 0 {
					reason = "waiting retained validation"
					waiting = true
				} else if retained > 0 {
					reason = "retained domain rejection: retry policy required"
				} else {
					loss = 1
					if attempts >= c.cfg.MaxAttempts {
						reason = "attempt budget exhausted"
					}
				}
			}
			if _, e = tx.Exec("UPDATE unit_attempts SET reason=?,data_loss=max(data_loss,?) WHERE run_id=? AND unit_id=?", reason, loss, r.run, id); e != nil {
				return e
			}
			if pending == 0 || accepted > 0 {
				if _, e = tx.Exec("DELETE FROM owners WHERE run_id=? AND unit_id=? AND assignment_id=?", r.run, id, r.id); e != nil {
					return e
				}
			}
		}
		if !waiting {
			if _, e = tx.Exec("UPDATE terminal_evidence SET reconciled=1 WHERE assignment_id=?", r.id); e != nil {
				return e
			}
		}
	}
	return nil
}
