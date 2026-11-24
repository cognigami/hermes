package controller

import (
	"database/sql"
	"errors"
	"hermes/internal/contracts"
	"time"
)

// reservationInventory contains admission facts, not an online Poll/receipt.
// Its caller establishes their provenance: connected retained inventory or the
// exact registered finite host/session. No transport acknowledgment is invented.
type reservationInventory struct {
	Worker, Session, Key string
	RetainedSpoolBytes   int64
	Covered              []contracts.RetainedAssignment
	Bounds               *reservationBounds
}
type reservationBounds struct{ Stop, Terminate, Handoff time.Time }

// reserveReadyAssignment is the shared transactional selector/reserver. All
// issuance gates apply to both connected and finite authority. Online receipt/
// retained-inventory protocol validation remains in poll, outside this helper.
func (c *Controller) reserveReadyAssignment(tx *sql.Tx, request reservationInventory) (contracts.Work, error) {
	empty := contracts.Work{SchemaVersion: contracts.Version}
	policy, e := c.policy(tx)
	if e != nil {
		return empty, e
	}
	host, ok := approvalForWorker(policy, request.Worker)
	if !ok {
		return empty, nil
	}
	var active, uncertain int
	if e = tx.QueryRow(`SELECT count(*),coalesce(sum(a.session!=? OR coalesce(r.state,'')='uncertain'),0) FROM assignments a LEFT JOIN execution_reports r ON r.assignment_id=a.id WHERE a.worker=? AND a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)`, request.Session, request.Worker).Scan(&active, &uncertain); e != nil {
		return empty, e
	}
	if uncertain > 0 {
		return empty, errors.New("worker has unresolved execution/session ownership")
	}
	if active >= c.cfg.MaxConcurrent || active >= host.MaxAssignments {
		return empty, nil
	}
	var paused int
	if e = tx.QueryRow("SELECT count(*) FROM target_controls WHERE kind='worker' AND target=? AND action='drain'", request.Worker).Scan(&paused); e != nil {
		return empty, e
	}
	if paused > 0 {
		return empty, nil
	}
	packages, e := readyPackages(tx, c.readyCursor[request.Worker], 1)
	if e != nil {
		return empty, e
	}
	if len(packages) == 0 {
		previous := c.readyCursor[request.Worker]
		c.readyCursor[request.Worker] = 0
		if previous != 0 {
			packages, e = readyPackages(tx, 0, 1)
			if e != nil {
				return empty, e
			}
		}
		if len(packages) == 0 {
			return empty, nil
		}
	}
	x := packages[0]
	c.readyCursor[request.Worker] = x.Cursor
	var selected []contracts.UnitRef
	var profile contracts.ResourceProfile
	var revision int64
	for _, ref := range x.Ready {
		eligible, e := c.resourceReady(tx, x.RunID, ref.UnitID)
		if e != nil {
			return empty, e
		}
		if !eligible {
			continue
		}
		chosen, rev, e := c.profileForInventory(tx, request.Worker, request.Session, request.RetainedSpoolBytes, request.Covered, x.RunID, []contracts.UnitRef{ref})
		if e != nil {
			return empty, e
		}
		if chosen.ID == "" {
			continue
		}
		if len(selected) == 0 {
			profile = chosen
			revision = rev
		}
		if chosen.ID != profile.ID {
			continue
		}
		selected = append(selected, ref)
		if len(selected) == 64 {
			break
		}
	}
	if len(selected) == 0 {
		return empty, nil
	}
	now := time.Now().UTC()
	bounds := reservationBounds{now.Add(c.cfg.StopAfter), now.Add(c.cfg.TerminateAfter), now.Add(c.cfg.HandoffAfter)}
	if request.Bounds != nil {
		if request.Bounds.Stop.Before(bounds.Stop) {
			bounds.Stop = request.Bounds.Stop
		}
		if request.Bounds.Terminate.Before(bounds.Terminate) {
			bounds.Terminate = request.Bounds.Terminate
		}
		if request.Bounds.Handoff.Before(bounds.Handoff) {
			bounds.Handoff = request.Bounds.Handoff
		}
	}
	a := contracts.Assignment{SchemaVersion: contracts.Version, AssignmentID: "assignment-" + contracts.Hash([]byte(request.Worker+":"+request.Key+":"+request.Session)), RunID: x.RunID, WorkerID: request.Worker, SessionID: request.Session, PackageDigest: x.Digest, Units: selected, Runtime: c.cfg.Runtime, Profile: profile.ID, PolicyRevision: revision, Allocation: profile.Allocation, StopNewUnitsAt: bounds.Stop, TerminateAt: bounds.Terminate, HandoffDeadline: bounds.Handoff}
	if e = a.Validate(); e != nil {
		return empty, e
	}
	if _, e = tx.Exec("INSERT INTO assignments(id,run_id,worker,session,manifest) VALUES(?,?,?,?,?)", a.AssignmentID, a.RunID, a.WorkerID, a.SessionID, encoded(a)); e != nil {
		return empty, e
	}
	if e = c.reserveAllocation(tx, host, a); e != nil {
		return empty, e
	}
	for _, ref := range selected {
		if _, e = tx.Exec("INSERT INTO unit_attempts VALUES(?,?,1,'',0) ON CONFLICT(run_id,unit_id) DO UPDATE SET attempts=attempts+1,reason=''", a.RunID, ref.UnitID); e != nil {
			return empty, e
		}
		if _, e = tx.Exec("INSERT INTO members VALUES(?,?,?)", a.AssignmentID, a.RunID, ref.UnitID); e != nil {
			return empty, e
		}
		if _, e = tx.Exec("INSERT INTO owners VALUES(?,?,?)", a.RunID, ref.UnitID, a.AssignmentID); e != nil {
			return empty, e
		}
	}
	return c.work(tx, a)
}
