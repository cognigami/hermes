package controller

import (
	"database/sql"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
)

// A final can commit concurrently with an inventory snapshot. Known exact
// terminal extras are safe; missing unresolved or unknown extras are not.
func (c *Controller) validateRetained(tx *sql.Tx, p contracts.PollRequest) (int, error) {
	if len(p.Retained) > 64 {
		return 0, errors.New("retained inventory too large")
	}
	claimed := map[string]string{}
	for _, r := range p.Retained {
		if r.AssignmentID == "" || len(r.Digest) != 64 || claimed[r.AssignmentID] != "" {
			return 0, errors.New("invalid retained inventory")
		}
		claimed[r.AssignmentID] = r.Digest
	}
	rows, e := tx.Query(`SELECT a.id,a.manifest,a.receipt,coalesce(e.state,'') FROM assignments a LEFT JOIN execution_reports e ON e.assignment_id=a.id WHERE a.worker=? AND a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)`, p.WorkerID)
	if e != nil {
		return 0, e
	}
	type record struct {
		id, state string
		b         []byte
		receipt   int
	}
	var active []record
	for rows.Next() {
		var r record
		if e = rows.Scan(&r.id, &r.b, &r.receipt, &r.state); e != nil {
			rows.Close()
			return 0, e
		}
		active = append(active, r)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return 0, e
	}
	for _, r := range active {
		var a contracts.Assignment
		if e = json.Unmarshal(r.b, &a); e != nil {
			return 0, e
		}
		digest, _ := contracts.Digest(a)
		if a.SessionID != p.SessionID || claimed[r.id] != digest {
			return 0, errors.New("missing/mismatched unresolved retained journal; reconciliation required")
		}
		if r.receipt != 1 || r.state == "uncertain" {
			return 0, errors.New("unresolved assignment not reconciled")
		}
		delete(claimed, r.id)
	}
	for id, digest := range claimed {
		var b []byte
		var terminal int
		e = tx.QueryRow(`SELECT a.manifest,(a.final IS NOT NULL OR EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)) FROM assignments a WHERE a.id=?`, id).Scan(&b, &terminal)
		if e != nil {
			return 0, errors.New("unknown extra retained assignment")
		}
		var a contracts.Assignment
		if e = json.Unmarshal(b, &a); e != nil {
			return 0, e
		}
		expected, _ := contracts.Digest(a)
		if terminal != 1 || a.WorkerID != p.WorkerID || a.SessionID != p.SessionID || digest != expected {
			return 0, errors.New("extra retained identity is not exact known terminal work")
		}
	}
	return len(active), nil
}
