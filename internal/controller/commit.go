package controller

import "database/sql"

func (c *Controller) commit(tx *sql.Tx, operation string) error {
	if c.testCommitBoundary != nil {
		if e := c.testCommitBoundary(operation, false); e != nil {
			return e
		}
	}
	if e := tx.Commit(); e != nil {
		return e
	}
	if c.testCommitBoundary != nil {
		return c.testCommitBoundary(operation, true)
	}
	return nil
}
