package controller

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"math"
)

var errUnknownHostResources = errors.New("unresolved legacy allocation has unknown resource reservation")

func randomID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func minimumVector(a, b contracts.ResourceVector) contracts.ResourceVector {
	if b.CPUMilli < a.CPUMilli {
		a.CPUMilli = b.CPUMilli
	}
	if b.MemoryBytes < a.MemoryBytes {
		a.MemoryBytes = b.MemoryBytes
	}
	if b.SpoolBytes < a.SpoolBytes {
		a.SpoolBytes = b.SpoolBytes
	}
	return a
}
func maximumVector(a, b contracts.ResourceVector) contracts.ResourceVector {
	if b.CPUMilli > a.CPUMilli {
		a.CPUMilli = b.CPUMilli
	}
	if b.MemoryBytes > a.MemoryBytes {
		a.MemoryBytes = b.MemoryBytes
	}
	if b.SpoolBytes > a.SpoolBytes {
		a.SpoolBytes = b.SpoolBytes
	}
	return a
}
func (c *Controller) workerObservation(tx *sql.Tx, h contracts.HostApproval) (contracts.WorkerRegistration, error) {
	var r contracts.WorkerRegistration
	var b []byte
	if e := tx.QueryRow("SELECT manifest FROM host_observations WHERE host_id=?", h.HostID).Scan(&b); e != nil {
		return r, e
	}
	return r, json.Unmarshal(b, &r)
}
func (c *Controller) reserveAllocation(tx *sql.Tx, h contracts.HostApproval, a contracts.Assignment) error {
	v := a.Allocation.Resources
	_, e := tx.Exec("INSERT INTO allocation_reservations VALUES(?,?,?,?,?,?,?,?)", a.AssignmentID, h.HostID, a.WorkerID, v.CPUMilli, v.MemoryBytes, v.SpoolBytes, a.Allocation.Exclusive, v == (contracts.ResourceVector{}))
	return e
}
func (c *Controller) saveSpoolAccount(tx *sql.Tx, h contracts.HostApproval, p contracts.PollRequest) error {
	var old, rev, inventory int64
	e := tx.QueryRow("SELECT bytes,spool_revision,inventory_revision FROM worker_spool_accounts WHERE host_id=? AND session_id=?", h.HostID, p.SessionID).Scan(&old, &rev, &inventory)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	if p.InventoryRevision < inventory || (p.InventoryRevision == inventory && p.SpoolRevision <= rev) {
		if old >= p.RetainedSpoolBytes {
			return nil
		}
	}
	_, e = tx.Exec("INSERT INTO worker_spool_accounts VALUES(?,?,?,?,?) ON CONFLICT(host_id,session_id) DO UPDATE SET bytes=excluded.bytes,spool_revision=max(spool_revision,excluded.spool_revision),inventory_revision=max(inventory_revision,excluded.inventory_revision)", h.HostID, p.SessionID, p.RetainedSpoolBytes, p.SpoolRevision, p.InventoryRevision)
	return e
}
func (c *Controller) reportedSpool(tx *sql.Tx, h contracts.HostApproval, p contracts.PollRequest) (int64, error) {
	var other, current int64
	if e := tx.QueryRow("SELECT coalesce(sum(bytes),0) FROM worker_spool_accounts WHERE host_id=? AND session_id!=?", h.HostID, p.SessionID).Scan(&other); e != nil {
		return 0, e
	}
	e := tx.QueryRow("SELECT bytes FROM worker_spool_accounts WHERE host_id=? AND session_id=?", h.HostID, p.SessionID).Scan(&current)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return 0, e
	}
	if p.RetainedSpoolBytes > current {
		current = p.RetainedSpoolBytes
	}
	// A pre-final poll snapshot may still cover a now-terminal owner rather
	// than report its bytes as closed. Keep its output/work envelope charged
	// until the worker supplies a post-retirement inventory; never treat exit
	// or a raw ACK as deletion. This is bounded by the retained active list.
	for _, entry := range p.Retained {
		var held int64
		e := tx.QueryRow(`SELECT max(r.spool,coalesce((SELECT max(f.min_spool) FROM resource_failures f WHERE f.assignment_id=a.id),0)) FROM allocation_reservations r JOIN assignments a ON a.id=r.assignment_id WHERE a.id=? AND (a.final IS NOT NULL OR EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id))`, entry.AssignmentID).Scan(&held)
		if errors.Is(e, sql.ErrNoRows) {
			continue
		}
		if e != nil {
			return 0, e
		}
		sum, e := (contracts.ResourceVector{SpoolBytes: current}).Add(contracts.ResourceVector{SpoolBytes: held})
		if e != nil {
			return 0, e
		}
		current = sum.SpoolBytes
	}
	v, e := (contracts.ResourceVector{SpoolBytes: other}).Add(contracts.ResourceVector{SpoolBytes: current})
	if e != nil {
		return 0, e
	}
	return v.SpoolBytes, nil
}
func (c *Controller) controllerFootprint(tx *sql.Tx, p contracts.ResourcePolicy) (contracts.ResourceVector, error) {
	fixed, e := controllerOverhead(p)
	if e != nil {
		return fixed, e
	}
	var held contracts.ResourceVector
	rows, e := tx.Query("SELECT resources FROM validator_claims WHERE exited=0")
	if e != nil {
		return fixed, e
	}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			rows.Close()
			return fixed, e
		}
		var record struct{ Resources contracts.ResourceVector }
		if e = json.Unmarshal(b, &record); e != nil {
			rows.Close()
			return fixed, e
		}
		held, e = held.Add(record.Resources)
		if e != nil {
			rows.Close()
			return fixed, e
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return fixed, e
	}
	var helper contracts.ResourceVector
	for i := 0; i < p.ValidatorSlots; i++ {
		helper, e = helper.Add(p.ValidatorReservation)
		if e != nil {
			return fixed, e
		}
	}
	excess := maximumVector(held, helper)
	fixed, e = fixed.Add(contracts.ResourceVector{CPUMilli: excess.CPUMilli - helper.CPUMilli, MemoryBytes: excess.MemoryBytes - helper.MemoryBytes, SpoolBytes: excess.SpoolBytes - helper.SpoolBytes})
	if e != nil {
		return fixed, e
	}
	var retained, pending, pages, pageSize int64
	if e = tx.QueryRow("SELECT bytes FROM controller_retained WHERE id=1").Scan(&retained); e != nil {
		return fixed, e
	}
	var activeTransfers contracts.ResourceVector
	pending, activeTransfers, e = ingressUsage(tx)
	if e != nil {
		return fixed, e
	}
	var configuredTransfers contracts.ResourceVector
	for i := 0; i < p.TransferSlots; i++ {
		configuredTransfers, e = configuredTransfers.Add(p.TransferReservation)
		if e != nil {
			return fixed, e
		}
	}
	effectiveTransfers := maximumVector(activeTransfers, configuredTransfers)
	fixed, e = fixed.Add(contracts.ResourceVector{CPUMilli: effectiveTransfers.CPUMilli - configuredTransfers.CPUMilli, MemoryBytes: effectiveTransfers.MemoryBytes - configuredTransfers.MemoryBytes, SpoolBytes: effectiveTransfers.SpoolBytes - configuredTransfers.SpoolBytes})
	if e != nil {
		return fixed, e
	}
	if e = tx.QueryRow("PRAGMA page_count").Scan(&pages); e != nil {
		return fixed, e
	}
	if e = tx.QueryRow("PRAGMA page_size").Scan(&pageSize); e != nil {
		return fixed, e
	}
	if pages < 0 || pageSize <= 0 || pages > math.MaxInt64/pageSize {
		return fixed, errors.New("metadata size overflow")
	}
	var retainedCopies int64
	if e = tx.QueryRow("SELECT bytes-(SELECT coalesce(sum(s.size),0) FROM validator_claims c JOIN controller_storage s ON s.claim_id=c.id WHERE c.exited=0) FROM controller_storage_totals WHERE id=1").Scan(&retainedCopies); e != nil {
		return fixed, e
	}
	for _, n := range []int64{retained, pending, pages * pageSize, retainedCopies} {
		fixed, e = fixed.Add(contracts.ResourceVector{SpoolBytes: n})
		if e != nil {
			return fixed, e
		}
	}
	return fixed, nil
}
func (c *Controller) hostUsage(tx *sql.Tx, p contracts.ResourcePolicy, h contracts.HostApproval, reportedSpool int64) (contracts.ResourceVector, contracts.ResourceVector, error) {
	used := h.Baseline
	limit := h.Limits
	if h.HostID == p.ControllerHostID {
		limit = minimumVector(limit, c.controllerObserved.Resources)
		overhead, e := c.controllerFootprint(tx, p)
		if e != nil {
			return used, limit, e
		}
		used, e = used.Add(overhead)
		if e != nil {
			return used, limit, e
		}
	}
	if h.WorkerID != "" {
		r, e := c.workerObservation(tx, h)
		if errors.Is(e, sql.ErrNoRows) && h.HostID == p.ControllerHostID {
			r.Observed = c.controllerObserved
			e = nil
		}
		if e != nil {
			return used, limit, e
		}
		limit = minimumVector(limit, r.Observed.Resources)
		if reportedSpool < 0 {
			if e = tx.QueryRow("SELECT coalesce(sum(bytes),0) FROM worker_spool_accounts WHERE host_id=?", h.HostID).Scan(&reportedSpool); e != nil {
				return used, limit, e
			}
		}
		used, e = used.Add(contracts.ResourceVector{SpoolBytes: reportedSpool})
		if e != nil {
			return used, limit, e
		}
	}
	var v contracts.ResourceVector
	var unknown int
	if e := tx.QueryRow(`SELECT coalesce(sum(r.cpu),0),coalesce(sum(r.memory),0),coalesce(sum(r.spool),0),coalesce(sum(r.unknown),0) FROM allocation_reservations r JOIN assignments a ON a.id=r.assignment_id WHERE r.host_id=? AND a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)`, h.HostID).Scan(&v.CPUMilli, &v.MemoryBytes, &v.SpoolBytes, &unknown); e != nil {
		return used, limit, e
	}
	if unknown > 0 {
		return used, limit, errUnknownHostResources
	}
	var e error
	used, e = used.Add(v)
	return used, limit, e
}
func (c *Controller) addOutputReservations(tx *sql.Tx, used *contracts.ResourceVector) error {
	var amount int64
	e := tx.QueryRow(`SELECT coalesce(sum(max(0,r.spool-coalesce(o.bytes,0))),0) FROM allocation_reservations r JOIN assignments a ON a.id=r.assignment_id LEFT JOIN assignment_output o ON o.assignment_id=a.id WHERE a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)`).Scan(&amount)
	if e != nil {
		return e
	}
	next, e := used.Add(contracts.ResourceVector{SpoolBytes: amount})
	if e == nil {
		*used = next
	}
	return e
}
func (c *Controller) unitFitsProfile(tx *sql.Tx, run string, ref contracts.UnitRef, profile contracts.ResourceProfile, h contracts.HostApproval, r contracts.WorkerRegistration) (bool, error) {
	var b []byte
	if e := tx.QueryRow("SELECT definition FROM definitions WHERE unit_id=?", ref.UnitID).Scan(&b); e != nil {
		return false, e
	}
	var unit contracts.Unit
	if e := json.Unmarshal(b, &unit); e != nil {
		return false, e
	}
	for _, capability := range unit.RequiredCapabilities {
		supported := capability == "os:"+r.GOOS || capability == "arch:"+r.GOARCH || capability == "leaf_exit" || (capability == "hard_tree_memory" && r.Observed.HardTreeMemory == "supported" && profile.Allocation.RequiresHardTreeMemory)
		if !supported {
			return false, nil
		}
	}
	var requirement contracts.UnitResourceRequirement
	e := tx.QueryRow("SELECT manifest FROM resource_requirements WHERE run_id=? AND unit_id=?", run, ref.UnitID).Scan(&b)
	if e == nil {
		if e = json.Unmarshal(b, &requirement); e != nil {
			return false, e
		}
	} else if !errors.Is(e, sql.ErrNoRows) {
		return false, e
	}
	if !requirement.Minimum.Fits(profile.Allocation.Resources) || (requirement.Minimum == (contracts.ResourceVector{}) && !profile.Conservative) || len(requirement.AllowedHosts) > 0 && !contains(requirement.AllowedHosts, h.HostID) || len(requirement.AllowedProfiles) > 0 && !contains(requirement.AllowedProfiles, profile.ID) {
		return false, nil
	}
	var floorMemory, floorSpool, minCPU, minMemory, minSpool int64
	if e = tx.QueryRow("SELECT coalesce(max(memory_floor),0),coalesce(max(spool_floor),0),coalesce(max(min_cpu),0),coalesce(max(min_memory),0),coalesce(max(min_spool),0) FROM resource_failures WHERE run_id=? AND unit_id=?", run, ref.UnitID).Scan(&floorMemory, &floorSpool, &minCPU, &minMemory, &minSpool); e != nil {
		return false, e
	}
	v := profile.Allocation.Resources
	return !(floorMemory > 0 && v.MemoryBytes <= floorMemory) && !(floorSpool > 0 && v.SpoolBytes <= floorSpool) && (contracts.ResourceVector{CPUMilli: minCPU, MemoryBytes: minMemory, SpoolBytes: minSpool}).Fits(v), nil
}
func (c *Controller) profileFor(tx *sql.Tx, poll contracts.PollRequest, run string, refs []contracts.UnitRef) (contracts.ResourceProfile, int64, error) {
	var none contracts.ResourceProfile
	p, e := c.policy(tx)
	if e != nil {
		return none, 0, e
	}
	h, ok := approvalForWorker(p, poll.WorkerID)
	if !ok {
		return none, 0, nil
	}
	r, e := c.workerObservation(tx, h)
	if errors.Is(e, sql.ErrNoRows) {
		return none, 0, nil
	}
	if e != nil {
		return none, 0, e
	}
	spool, e := c.reportedSpool(tx, h, poll)
	if e != nil {
		return none, 0, e
	}
	used, limit, e := c.hostUsage(tx, p, h, spool)
	if errors.Is(e, errUnknownHostResources) {
		return none, 0, nil
	}
	if e != nil {
		return none, 0, e
	}
	var computing, exclusive int
	if e = tx.QueryRow(`SELECT count(*),coalesce(sum(r.exclusive),0) FROM allocation_reservations r JOIN assignments a ON a.id=r.assignment_id WHERE r.host_id=? AND a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id) AND NOT EXISTS(SELECT 1 FROM execution_reports e WHERE e.assignment_id=a.id AND e.state='quiescent_pending_delivery')`, h.HostID).Scan(&computing, &exclusive); e != nil {
		return none, 0, e
	}
	if exclusive > 0 {
		return none, 0, nil
	}
	controllerHost, ok := approvalForHost(p, p.ControllerHostID)
	if !ok {
		return none, 0, errors.New("controller host missing")
	}
	controllerUsed, controllerLimit, e := c.hostUsage(tx, p, controllerHost, -1)
	if h.HostID == p.ControllerHostID {
		controllerUsed = used
		controllerLimit = limit
		e = nil
	}
	if errors.Is(e, errUnknownHostResources) {
		return none, 0, nil
	}
	if e != nil {
		return none, 0, e
	}
	if e = c.addOutputReservations(tx, &controllerUsed); e != nil {
		return none, 0, e
	}
	for _, profile := range p.Profiles {
		if profile.Allocation.Exclusive && computing > 0 || profile.Allocation.RequiresHardTreeMemory && r.Observed.HardTreeMemory != "supported" {
			continue
		}
		eligible := true
		for _, ref := range refs {
			fit, e := c.unitFitsProfile(tx, run, ref, profile, h, r)
			if e != nil {
				return none, 0, e
			}
			if !fit {
				eligible = false
				break
			}
		}
		if !eligible {
			continue
		}
		next, e := used.Add(profile.Allocation.Resources)
		if e != nil || !next.Fits(limit) {
			continue
		}
		controllerNext := controllerUsed
		if h.HostID == p.ControllerHostID {
			controllerNext, e = controllerNext.Add(profile.Allocation.Resources)
			if e != nil {
				return none, 0, e
			}
		}
		controllerNext, e = controllerNext.Add(contracts.ResourceVector{SpoolBytes: profile.Allocation.Resources.SpoolBytes})
		if e == nil && controllerNext.Fits(controllerLimit) {
			return profile, p.Revision, nil
		}
	}
	return none, 0, nil
}
func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
