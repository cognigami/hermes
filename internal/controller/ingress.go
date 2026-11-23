package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"hermes/internal/platform"
	"io"
	"log"
	"sync"
	"time"
)

var ErrInvalidArtifact = errors.New("invalid artifact publication")

const ingressSchema = `
CREATE TABLE IF NOT EXISTS ingress_transfers(id TEXT PRIMARY KEY,instance TEXT NOT NULL,policy_revision INTEGER NOT NULL,assignment_id TEXT NOT NULL,resources BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS ingress_sources(digest TEXT PRIMARY KEY REFERENCES controller_ingress(digest),assignment_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS ingress_assignment_blobs(assignment_id TEXT NOT NULL REFERENCES assignments(id),digest TEXT NOT NULL,size INTEGER NOT NULL CHECK(size>=0),PRIMARY KEY(assignment_id,digest));
CREATE INDEX IF NOT EXISTS ingress_source_assignment ON ingress_sources(assignment_id,digest);
CREATE INDEX IF NOT EXISTS ingress_pending_digest ON ingress_assignment_blobs(digest);
`

// Go transfers cannot survive process death. Unresolved permanent intents stay
// charged; superseded pending rows may retire only into a verified catalog row.
func restoreIngress(tx *sql.Tx) error {
	for _, statement := range []string{
		"DELETE FROM ingress_transfers",
		"DELETE FROM ingress_sources WHERE EXISTS(SELECT 1 FROM artifacts a WHERE a.digest=ingress_sources.digest)",
		"DELETE FROM ingress_assignment_blobs WHERE EXISTS(SELECT 1 FROM artifacts a WHERE a.digest=ingress_assignment_blobs.digest)",
		"DELETE FROM controller_ingress WHERE EXISTS(SELECT 1 FROM artifacts a WHERE a.digest=controller_ingress.digest)",
	} {
		if _, e := tx.Exec(statement); e != nil {
			return e
		}
	}
	return nil
}

// ingressUsage counts pending permanent bytes once, crediting only the original
// live source's remaining positive output reservation. Terminal proof ends that
// protection automatically. Actual output links are still publication-only.
func ingressUsage(tx *sql.Tx) (int64, contracts.ResourceVector, error) {
	var pending int64
	var held contracts.ResourceVector
	rows, e := tx.Query(`SELECT i.size,coalesce(s.assignment_id,''),coalesce(b.size,0) FROM controller_ingress i LEFT JOIN ingress_sources s ON s.digest=i.digest LEFT JOIN ingress_assignment_blobs b ON b.assignment_id=s.assignment_id AND b.digest=i.digest WHERE NOT EXISTS(SELECT 1 FROM artifacts a WHERE a.digest=i.digest)`)
	if e != nil {
		return 0, held, e
	}
	perSource := map[string]int64{}
	for rows.Next() {
		var size, sourceClaim int64
		var source string
		if e = rows.Scan(&size, &source, &sourceClaim); e != nil {
			rows.Close()
			return 0, held, e
		}
		sum, err := (contracts.ResourceVector{SpoolBytes: pending}).Add(contracts.ResourceVector{SpoolBytes: size})
		if err != nil {
			rows.Close()
			return 0, held, err
		}
		pending = sum.SpoolBytes
		if source != "" {
			credit := size
			if sourceClaim < credit {
				credit = sourceClaim
			}
			v, err := (contracts.ResourceVector{SpoolBytes: perSource[source]}).Add(contracts.ResourceVector{SpoolBytes: credit})
			if err != nil {
				rows.Close()
				return 0, held, err
			}
			perSource[source] = v.SpoolBytes
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return 0, held, e
	}
	for source, bytes := range perSource {
		var capacity, received int64
		e = tx.QueryRow(`SELECT r.spool,coalesce(o.bytes,0) FROM allocation_reservations r JOIN assignments a ON a.id=r.assignment_id LEFT JOIN assignment_output o ON o.assignment_id=a.id WHERE a.id=? AND a.final IS NULL AND r.unknown=0 AND r.spool>0 AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)`, source).Scan(&capacity, &received)
		if errors.Is(e, sql.ErrNoRows) {
			continue
		}
		if e != nil {
			return 0, held, e
		}
		if received < 0 {
			return 0, held, errors.New("invalid assignment output accounting")
		}
		credit := int64(0)
		if capacity > received {
			credit = capacity - received
		}
		if bytes < credit {
			credit = bytes
		}
		if credit > pending {
			return 0, held, errors.New("invalid pending ingress credit")
		}
		pending -= credit
	}
	rows, e = tx.Query("SELECT resources FROM ingress_transfers")
	if e != nil {
		return 0, held, e
	}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			rows.Close()
			return 0, held, e
		}
		var v contracts.ResourceVector
		if e = json.Unmarshal(b, &v); e != nil {
			rows.Close()
			return 0, held, e
		}
		held, e = held.Add(v)
		if e != nil {
			rows.Close()
			return 0, held, e
		}
	}
	e = rows.Err()
	rows.Close()
	return pending, held, e
}
func ingressDigest(d string) bool {
	if len(d) != 64 {
		return false
	}
	for _, c := range d {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func invalidIngress(reason string) error { return fmt.Errorf("%w: %s", ErrInvalidArtifact, reason) }

func (c *Controller) ingressPolicy(tx *sql.Tx, assignmentID string) (contracts.ResourcePolicy, contracts.Assignment, error) {
	var a contracts.Assignment
	if assignmentID == "" {
		p, e := c.policy(tx)
		return p, a, e
	}
	var b []byte
	if e := tx.QueryRow("SELECT manifest FROM assignments WHERE id=?", assignmentID).Scan(&b); e != nil {
		if errors.Is(e, sql.ErrNoRows) {
			return contracts.ResourcePolicy{}, a, invalidIngress("unknown assignment")
		}
		return contracts.ResourcePolicy{}, a, e
	}
	if e := json.Unmarshal(b, &a); e != nil {
		return contracts.ResourcePolicy{}, a, e
	}
	p, e := c.policyAt(tx, a.PolicyRevision)
	return p, a, e
}

// Bind unique assignment output intents before publication. Allocation spool is
// a forecast credit, NEVER a hard limit on retained scientifically valid bytes.
func ingressOutputIntent(tx *sql.Tx, a contracts.Assignment, digest string, size int64) error {
	if a.AssignmentID == "" {
		return nil
	}
	var existing int64
	// Only a verified output link can bind exact length. Pending headers may
	// overstate it; keep their worst-case charge until actual verification.
	e := tx.QueryRow("SELECT size FROM assignment_output_blobs WHERE assignment_id=? AND digest=?", a.AssignmentID, digest).Scan(&existing)
	if e == nil {
		if existing != size {
			return invalidIngress("published assignment digest size changed")
		}
		size = existing
	} else if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	_, e = tx.Exec("INSERT INTO ingress_assignment_blobs VALUES(?,?,?) ON CONFLICT(assignment_id,digest) DO UPDATE SET size=max(size,excluded.size)", a.AssignmentID, digest, size)
	return e
}

// claimIngress couples slot count, policy snapshot, output envelope and permanent
// intent in one short transaction. There is never SQL/authority locking over IO.
func (c *Controller) claimIngress(ctx context.Context, assignmentID, digest string, size int64, publish bool) (func() error, error) {
	observed, e := platform.ObserveResources(c.cfg.Root)
	if e != nil {
		return nil, e
	}
	tx, e := c.db.BeginTx(ctx, nil)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	old, a, e := c.ingressPolicy(tx, assignmentID)
	if e != nil {
		return nil, e
	}
	p, e := c.policy(tx)
	if e != nil {
		return nil, e
	}
	reservation := maximumVector(old.TransferReservation, p.TransferReservation)
	var count int
	if e = tx.QueryRow("SELECT count(*) FROM ingress_transfers").Scan(&count); e != nil {
		return nil, e
	}
	if count >= p.TransferSlots {
		return nil, ErrControllerCapacity
	}
	if publish && size > reservation.SpoolBytes {
		return nil, fmt.Errorf("%w: upload scratch exceeds transfer reservation", ErrControllerCapacity)
	}
	var additional int64
	if publish {
		var existing int64
		e = tx.QueryRow("SELECT size FROM artifacts WHERE digest=?", digest).Scan(&existing)
		if e == nil && existing != size {
			return nil, invalidIngress("verified digest size changed")
		}
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return nil, e
		}
		var registered int
		if e = tx.QueryRow("SELECT count(*) FROM artifacts WHERE digest=?", digest).Scan(&registered); e != nil {
			return nil, e
		}
		if registered == 0 {
			additional = size
		}
		if e = ingressOutputIntent(tx, a, digest, size); e != nil {
			return nil, e
		}
		if registered == 0 {
			if _, e = tx.Exec("INSERT INTO controller_ingress VALUES(?,?) ON CONFLICT(digest) DO UPDATE SET size=max(size,excluded.size)", digest, size); e != nil {
				return nil, e
			}
			if _, e = tx.Exec("INSERT OR IGNORE INTO ingress_sources VALUES(?,?)", digest, assignmentID); e != nil {
				return nil, e
			}
		}
	}
	id := randomID()
	if _, e = tx.Exec("INSERT INTO ingress_transfers VALUES(?,?,?,?,?)", id, c.instance, p.Revision, assignmentID, encoded(reservation)); e != nil {
		return nil, e
	}
	host, ok := approvalForHost(p, p.ControllerHostID)
	if !ok {
		return nil, errors.New("controller approval missing")
	}
	used, limit, e := c.hostUsage(tx, p, host, -1)
	if e != nil {
		return nil, e
	}
	if e = c.addOutputReservations(tx, &used); e != nil {
		return nil, e
	}
	limit = minimumVector(limit, observed.Resources)
	if !used.Fits(limit) {
		return nil, ErrControllerCapacity
	}
	// Native free space is an additional conservative check, not total capacity.
	// Count full live temporary reservations even if some have partly materialized.
	_, active, e := ingressUsage(tx)
	if e != nil {
		return nil, e
	}
	needed, e := active.Add(contracts.ResourceVector{SpoolBytes: additional})
	if e != nil {
		return nil, e
	}
	if needed.SpoolBytes > observed.AvailableSpoolBytes {
		return nil, ErrControllerCapacity
	}
	// The channel is only a physical upper bound; SQL serializes policy count.
	select {
	case c.transferSlots <- struct{}{}:
	default:
		return nil, ErrControllerCapacity
	}
	committed := false
	defer func() {
		if !committed {
			<-c.transferSlots
		}
	}()
	operation := "transfer"
	if publish {
		operation = "artifact-intent"
	}
	if e = c.commit(tx, operation); e != nil {
		return nil, e
	}
	committed = true
	var once sync.Once
	var releaseErr error
	release := func() error {
		once.Do(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, releaseErr = c.db.ExecContext(cleanup, "DELETE FROM ingress_transfers WHERE id=? AND instance=?", id, c.instance)
			<-c.transferSlots
		})
		return releaseErr
	}
	return release, nil
}

// BeginArtifactTransfer is for already-authorized GET handlers. The caller must
// stop its response IO on cancellation and release only after IO has returned.
func (c *Controller) BeginArtifactTransfer(ctx context.Context, assignmentID string) (context.Context, func(), error) {
	derived, finish, e := c.beginArtifactTransfer(ctx)
	if e != nil {
		return nil, nil, e
	}
	release, e := c.claimIngress(derived, assignmentID, "", 0, false)
	if e != nil {
		finish()
		return nil, nil, e
	}
	var once sync.Once
	return derived, func() {
		once.Do(func() {
			if e := release(); e != nil {
				log.Printf("artifact transfer hold retained after release failure: %v", e)
			}
			finish()
		})
	}, nil
}

func (c *Controller) PublishArtifact(ctx context.Context, digest string, size int64, assignmentID string, src io.Reader) (result error) {
	if !ingressDigest(digest) || size < 0 || size > c.cfg.MaxArtifactBytes || src == nil {
		return invalidIngress("digest, declared size or reader")
	}
	derived, finish, e := c.beginArtifactTransfer(ctx)
	if e != nil {
		return e
	}
	defer finish()
	if closer, ok := src.(io.Closer); ok {
		done := make(chan struct{})
		stop := context.AfterFunc(derived, func() { defer close(done); closer.Close() })
		defer func() {
			if !stop() {
				<-done
			}
		}()
	}
	release, e := c.claimIngress(derived, assignmentID, digest, size, true)
	if e != nil {
		return e
	}
	retainHold := false
	defer func() {
		if !retainHold {
			result = errors.Join(result, release())
		}
	}()
	if e = c.store.Publish(derived, digest, size, src); e != nil {
		// Unremoved private scratch may be a distinct retained copy. Keep its
		// exact approved temporary hold until exclusive-owner startup cleanup.
		retainHold = errors.Is(e, artifacts.ErrIngressCleanup)
		return e
	}
	tx, e := c.db.BeginTx(derived, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var registered int64
	e = tx.QueryRow("SELECT size FROM artifacts WHERE digest=?", digest).Scan(&registered)
	if e == nil {
		if registered != size {
			return invalidIngress("published digest size changed")
		}
	} else if errors.Is(e, sql.ErrNoRows) {
		var retained int64
		if e = tx.QueryRow("SELECT bytes FROM controller_retained WHERE id=1").Scan(&retained); e != nil {
			return e
		}
		if _, e = (contracts.ResourceVector{SpoolBytes: retained}).Add(contracts.ResourceVector{SpoolBytes: size}); e != nil {
			return e
		}
		if _, e = tx.Exec("INSERT INTO artifacts VALUES(?,?)", digest, size); e != nil {
			return e
		}
	} else {
		return e
	}
	if assignmentID != "" {
		var linked int
		e = tx.QueryRow("SELECT count(*) FROM assignment_output_blobs WHERE assignment_id=? AND digest=?", assignmentID, digest).Scan(&linked)
		if e != nil {
			return e
		}
		if linked == 0 {
			var received int64
			if e = tx.QueryRow("SELECT coalesce((SELECT bytes FROM assignment_output WHERE assignment_id=?),0)", assignmentID).Scan(&received); e != nil {
				return e
			}
			next, e := (contracts.ResourceVector{SpoolBytes: received}).Add(contracts.ResourceVector{SpoolBytes: size})
			if e != nil {
				return e
			}
			if _, e = tx.Exec("INSERT INTO assignment_output_blobs VALUES(?,?,?)", assignmentID, digest, size); e != nil {
				return e
			}
			if _, e = tx.Exec("INSERT INTO assignment_output VALUES(?,?) ON CONFLICT(assignment_id) DO UPDATE SET bytes=excluded.bytes", assignmentID, next.SpoolBytes); e != nil {
				return e
			}
		}
	}
	// Resolve the pending charge into verified retained/catalog bytes in this
	// SAME transaction; no byte deletion and no historical pending-table scan.
	if _, e = tx.Exec("DELETE FROM ingress_sources WHERE digest=?", digest); e != nil {
		return e
	}
	if _, e = tx.Exec("DELETE FROM controller_ingress WHERE digest=?", digest); e != nil {
		return e
	}
	if _, e = tx.Exec("DELETE FROM ingress_assignment_blobs WHERE digest=?", digest); e != nil {
		return e
	}
	return c.commit(tx, "artifact-publication")
}
