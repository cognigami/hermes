package controller

import (
	"database/sql"
	"errors"
	"hermes/internal/contracts"
)

func (c *Controller) bootstrapResources(tx *sql.Tx, p contracts.ResourcePolicy) error {
	var initialized int
	if e := tx.QueryRow("SELECT count(*) FROM controller_retained WHERE id=1").Scan(&initialized); e != nil {
		return e
	}
	if initialized == 0 {
		if _, e := tx.Exec("INSERT INTO controller_retained SELECT 1,coalesce(sum(size),0) FROM artifacts"); e != nil {
			return e
		}
	}
	var storageInitialized int
	if e := tx.QueryRow("SELECT count(*) FROM controller_storage_totals WHERE id=1").Scan(&storageInitialized); e != nil {
		return e
	}
	if storageInitialized == 0 {
		if _, e := tx.Exec("INSERT INTO controller_storage_totals SELECT 1,coalesce(sum(size),0) FROM controller_storage"); e != nil {
			return e
		}
	}
	if e := c.bootstrapControllerStorage(tx); e != nil {
		return e
	}
	var installed int
	if e := tx.QueryRow("SELECT count(*) FROM resource_policies").Scan(&installed); e != nil {
		return e
	}
	var head int64
	headErr := tx.QueryRow("SELECT revision FROM resource_head WHERE id=1").Scan(&head)
	if headErr != nil && !errors.Is(headErr, sql.ErrNoRows) {
		return headErr
	}
	if head > p.Revision {
		var digest string
		if e := tx.QueryRow("SELECT digest FROM resource_policies WHERE revision=?", p.Revision).Scan(&digest); e != nil {
			return e
		}
		configured, _ := contracts.Digest(p)
		if digest != configured {
			return errors.New("bootstrap policy revision is immutable")
		}
		var e error
		p, e = c.policyAt(tx, head)
		if e != nil {
			return e
		}
	}
	if e := c.installPolicy(tx, p); e != nil {
		return e
	}
	controller, ok := approvalForHost(p, p.ControllerHostID)
	if !ok {
		return errors.New("controller host approval missing")
	}
	overhead, e := controllerOverhead(p)
	if e != nil {
		return e
	}
	total, e := controller.Baseline.Add(overhead)
	if e != nil || !total.Fits(minimumVector(controller.Limits, c.controllerObserved.Resources)) {
		return errors.New("controller service/transfer/helper overhead exceeds approved or observed host capacity")
	}
	if installed == 0 {
		rows, e := tx.Query("SELECT digest FROM candidates WHERE state='validating'")
		if e != nil {
			return e
		}
		var old []string
		for rows.Next() {
			var digest string
			if e = rows.Scan(&digest); e != nil {
				rows.Close()
				return e
			}
			old = append(old, digest)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		for _, digest := range old {
			record := struct {
				Role, Reference string
				Resources       contracts.ResourceVector
			}{"validator", digest, p.ValidatorReservation}
			if _, e = tx.Exec("INSERT INTO validator_claims(id,started,resources,instance) VALUES(?,1,?,'legacy-unknown')", randomID(), encoded(record)); e != nil {
				return e
			}
		}
	}
	return c.restoreChildClaims(tx)
}
func (c *Controller) registerRequirements(tx *sql.Tx, s contracts.Submission) error {
	units := map[string]bool{}
	for _, u := range s.Units {
		units[u.UnitID] = true
	}
	seen := map[string]bool{}
	for _, r := range s.Requirements {
		if !units[r.UnitID] || seen[r.UnitID] || r.Minimum.Validate() != nil || len(r.AllowedHosts) > 64 || len(r.AllowedProfiles) > 64 {
			return errors.New("invalid operational resource requirements")
		}
		seen[r.UnitID] = true
		if _, e := tx.Exec("INSERT INTO resource_requirements VALUES(?,?,?)", s.RunID, r.UnitID, encoded(r)); e != nil {
			return e
		}
	}
	return nil
}
