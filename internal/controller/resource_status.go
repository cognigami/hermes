package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
)

type ResourceChildStatus struct {
	ClaimID   string                   `json:"claim_id"`
	Role      string                   `json:"role"`
	Reference string                   `json:"reference"`
	Resources contracts.ResourceVector `json:"resources"`
	Started   bool                     `json:"started"`
	State     string                   `json:"state"`
}
type ResourceHostStatus struct {
	SchemaVersion       int                        `json:"schema_version"`
	PolicyRevision      int64                      `json:"policy_revision"`
	Approval            contracts.HostApproval     `json:"approval"`
	Observed            *contracts.HostObservation `json:"observed,omitempty"`
	KnownReserved       contracts.ResourceVector   `json:"known_reserved"`
	EffectiveLimit      contracts.ResourceVector   `json:"effective_limit"`
	CapacityKnown       bool                       `json:"capacity_known"`
	WaitingReason       string                     `json:"waiting_reason,omitempty"`
	ControllerChildren  []ResourceChildStatus      `json:"controller_children,omitempty"`
	RetainedCopiesBytes int64                      `json:"retained_copies_bytes"`
}

func (c *Controller) ResourceStatus(ctx context.Context, hostID string) (ResourceHostStatus, error) {
	result := ResourceHostStatus{SchemaVersion: 1}
	tx, e := c.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if e != nil {
		return result, e
	}
	defer tx.Rollback()
	p, e := c.policy(tx)
	if e != nil {
		return result, e
	}
	result.PolicyRevision = p.Revision
	if hostID == "" {
		hostID = p.ControllerHostID
	}
	h, ok := approvalForHost(p, hostID)
	if !ok {
		return result, errors.New("unknown approved host")
	}
	result.Approval = h
	if hostID == p.ControllerHostID {
		observation := c.controllerObserved
		result.Observed = &observation
	} else {
		registration, e := c.workerObservation(tx, h)
		if e == nil {
			result.Observed = &registration.Observed
		} else if !errors.Is(e, sql.ErrNoRows) {
			return result, e
		}
	}
	result.KnownReserved, result.EffectiveLimit, e = c.hostUsage(tx, p, h, -1)
	if errors.Is(e, sql.ErrNoRows) || errors.Is(e, errUnknownHostResources) {
		result.WaitingReason = e.Error()
	} else if e != nil {
		return result, e
	} else {
		result.CapacityKnown = true
	}
	if hostID != p.ControllerHostID {
		return result, nil
	}
	if e = c.addOutputReservations(tx, &result.KnownReserved); e != nil {
		return result, e
	}
	if e = tx.QueryRow("SELECT bytes FROM controller_storage_totals WHERE id=1").Scan(&result.RetainedCopiesBytes); e != nil {
		return result, e
	}
	rows, e := tx.Query("SELECT id,started,instance,resources FROM validator_claims WHERE exited=0 ORDER BY rowid")
	if e != nil {
		return result, e
	}
	for rows.Next() {
		var entry ResourceChildStatus
		var instance string
		var b []byte
		if e = rows.Scan(&entry.ClaimID, &entry.Started, &instance, &b); e != nil {
			rows.Close()
			return result, e
		}
		var record childResources
		if e = json.Unmarshal(b, &record); e != nil {
			rows.Close()
			return result, e
		}
		entry.Role = record.Role
		entry.Reference = record.Reference
		entry.Resources = record.Resources
		entry.State = "reserved_never_launched"
		if entry.Started {
			entry.State = "started_exit_not_observed"
			if instance != c.instance {
				entry.State = "prior_controller_exit_unproven"
				result.WaitingReason = "controller child exit unproven; reservation and scratch retained"
			}
		}
		result.ControllerChildren = append(result.ControllerChildren, entry)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return result, e
	}
	if !result.KnownReserved.Fits(result.EffectiveLimit) && result.WaitingReason == "" {
		result.WaitingReason = "committed/retained resources exceed current approved capacity"
	}
	return result, nil
}
