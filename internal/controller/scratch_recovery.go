package controller

import (
	"context"
	"errors"
)

// Only exact, indexed scratch for a durably observed-exited/never-launched
// child is eligible. Unknown started children and permanent runtime copies stay.
func (c *Controller) cleanupExitedScratch(ctx context.Context) error {
	rows, e := c.db.QueryContext(ctx, "SELECT DISTINCT c.id,c.instance FROM validator_claims c JOIN controller_storage s ON s.claim_id=c.id WHERE c.exited=1 LIMIT 16")
	if e != nil {
		return e
	}
	var claims []*ChildClaim
	for rows.Next() {
		claim := &ChildClaim{controller: c}
		if e = rows.Scan(&claim.id, &claim.instance); e != nil {
			rows.Close()
			return e
		}
		claims = append(claims, claim)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, claim := range claims {
		if e = claim.CleanupScratch(ctx); e != nil {
			return errors.New("known-exited controller scratch cleanup failed: " + e.Error())
		}
	}
	return nil
}
