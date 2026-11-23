package controller

import (
	"database/sql"
	"errors"
	"hermes/internal/contracts"
)

func (c *Controller) updateResourcePolicy(tx *sql.Tx, r contracts.PolicyRequest) (any, error) {
	if r.SchemaVersion != 1 {
		return nil, errors.New("unsupported policy request")
	}
	current, e := c.policy(tx)
	if e != nil {
		return nil, e
	}
	if current.Revision != r.ExpectedRevision || r.Policy.Revision <= current.Revision {
		return nil, errors.New("resource policy revision conflict")
	}
	if r.Policy.ControllerHostID != current.ControllerHostID {
		return nil, errors.New("controller host binding cannot change through resource policy")
	}
	if e = c.installPolicy(tx, r.Policy); e != nil {
		return nil, e
	}
	host, ok := approvalForHost(r.Policy, r.Policy.ControllerHostID)
	if !ok {
		return nil, errors.New("controller host missing")
	}
	overhead, e := controllerOverhead(r.Policy)
	if e != nil {
		return nil, e
	}
	required, e := host.Baseline.Add(overhead)
	if e != nil || !required.Fits(minimumVector(host.Limits, c.controllerObserved.Resources)) {
		return nil, errors.New("controller overhead exceeds approved/observed capacity")
	}
	return contracts.Acknowledgment{SchemaVersion: 1, Received: true}, nil
}
