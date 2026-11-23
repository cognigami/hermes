package controller

import (
	"database/sql"
	"encoding/json"
	"hermes/internal/contracts"
	"net/http"
)

func (c *Controller) controlRoute(w http.ResponseWriter, r *http.Request, principal string, operator, worker bool) bool {
	operatorRoute := r.URL.Path == "/v1/control" || r.URL.Path == "/v1/lost-spool"
	workerRoute := r.URL.Path == "/v1/control/poll" || r.URL.Path == "/v1/control/ack" || r.URL.Path == "/v1/reconcile"
	if !operatorRoute && !workerRoute {
		return false
	}
	if (operatorRoute && !operator) || (workerRoute && !worker) {
		http.Error(w, "wrong role", 403)
		return true
	}
	var b []byte
	var e error
	switch r.URL.Path {
	case "/v1/control":
		var p contracts.ControlRequest
		if e = contracts.Decode(r.Body, &p); e == nil {
			b, e = c.mutate(r.Context(), principal, p.RequestID, p, func(tx *sql.Tx) (any, error) { return c.control(tx, p) })
		}
	case "/v1/lost-spool":
		var p contracts.LostSpoolResolution
		if e = contracts.Decode(r.Body, &p); e == nil {
			b, e = c.mutate(r.Context(), principal, p.RequestID, p, func(tx *sql.Tx) (any, error) { return c.resolveLostSpool(tx, p) })
		}
	case "/v1/control/ack":
		var p contracts.ControlAck
		if e = contracts.Decode(r.Body, &p); e == nil {
			b, e = c.mutate(r.Context(), principal, p.RequestID, p, func(tx *sql.Tx) (any, error) { return c.ackControl(tx, p) })
		}
	case "/v1/reconcile":
		var p contracts.ReconcileRequest
		if e = contracts.Decode(r.Body, &p); e == nil {
			b, e = c.mutate(r.Context(), principal, p.RequestID, p, func(tx *sql.Tx) (any, error) { return c.reconcile(tx, p) })
		}
	case "/v1/control/poll":
		var p contracts.Receipt
		if e = contracts.Decode(r.Body, &p); e == nil {
			var tx *sql.Tx
			tx, e = c.db.BeginTx(r.Context(), nil)
			if e == nil {
				var result contracts.AssignmentControl
				result, e = c.assignmentControl(tx, p)
				tx.Rollback()
				if e == nil {
					b, e = json.Marshal(result)
				}
			}
		}
	}
	if e != nil {
		http.Error(w, e.Error(), 400)
	} else {
		w.Write(b)
	}
	return true
}
