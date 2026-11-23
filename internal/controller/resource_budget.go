package controller

import (
	"database/sql"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
)

func (c *Controller) policyAt(tx *sql.Tx, revision int64) (contracts.ResourcePolicy, error) {
	if p, ok := c.policyCache[revision]; ok {
		return p, nil
	}
	var p contracts.ResourcePolicy
	var b []byte
	if e := tx.QueryRow("SELECT manifest FROM resource_policies WHERE revision=?", revision).Scan(&b); e != nil {
		return p, e
	}
	if e := json.Unmarshal(b, &p); e != nil {
		return p, e
	}
	c.policyCache[revision] = p
	return p, nil
}
func (c *Controller) hostBudget(tx *sql.Tx, p contracts.ResourcePolicy, worker, session string) (contracts.HostBudget, error) {
	h, ok := approvalForWorker(p, worker)
	if !ok {
		return contracts.HostBudget{}, errors.New("worker host has no approval")
	}
	b := contracts.HostBudget{SchemaVersion: 1, PolicyRevision: p.Revision, Approval: h, Profiles: p.Profiles, AllowSyntheticBackend: p.AllowSyntheticBackend}
	if h.HostID == p.ControllerHostID {
		var e error
		b.ControllerOverhead, e = c.controllerFootprint(tx, p)
		if e != nil {
			return b, e
		}
		if e = c.addOutputReservations(tx, &b.ControllerOverhead); e != nil {
			return b, e
		}
	}
	e := tx.QueryRow("SELECT coalesce(sum(bytes),0) FROM worker_spool_accounts WHERE host_id=? AND session_id!=?", h.HostID, session).Scan(&b.OtherRetainedSpoolBytes)
	return b, e
}
