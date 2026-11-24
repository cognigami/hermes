package controller

import (
	"database/sql"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
)

const resourceSchema = `
CREATE TABLE IF NOT EXISTS resource_policies(revision INTEGER PRIMARY KEY,digest TEXT NOT NULL,manifest BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS resource_head(id INTEGER PRIMARY KEY CHECK(id=1),revision INTEGER NOT NULL REFERENCES resource_policies(revision));
CREATE TABLE IF NOT EXISTS host_bindings(host_id TEXT PRIMARY KEY,worker_id TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS host_observations(host_id TEXT PRIMARY KEY REFERENCES host_bindings(host_id),session_id TEXT NOT NULL,manifest BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS worker_spool_accounts(host_id TEXT NOT NULL,session_id TEXT NOT NULL,bytes INTEGER NOT NULL,spool_revision INTEGER NOT NULL,inventory_revision INTEGER NOT NULL,PRIMARY KEY(host_id,session_id));
CREATE TABLE IF NOT EXISTS assignment_output(assignment_id TEXT PRIMARY KEY REFERENCES assignments(id),bytes INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS assignment_output_blobs(assignment_id TEXT NOT NULL REFERENCES assignments(id),digest TEXT NOT NULL,size INTEGER NOT NULL,PRIMARY KEY(assignment_id,digest));
CREATE TABLE IF NOT EXISTS resource_requirements(run_id TEXT NOT NULL,unit_id TEXT NOT NULL,manifest BLOB NOT NULL,PRIMARY KEY(run_id,unit_id),FOREIGN KEY(run_id,unit_id) REFERENCES run_units(run_id,unit_id));
CREATE TABLE IF NOT EXISTS allocation_reservations(assignment_id TEXT PRIMARY KEY REFERENCES assignments(id),host_id TEXT NOT NULL REFERENCES host_bindings(host_id),worker_id TEXT NOT NULL,cpu INTEGER NOT NULL,memory INTEGER NOT NULL,spool INTEGER NOT NULL,exclusive INTEGER NOT NULL,unknown INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS allocation_host ON allocation_reservations(host_id,assignment_id);
CREATE TABLE IF NOT EXISTS resource_failures(run_id TEXT NOT NULL,unit_id TEXT NOT NULL,assignment_id TEXT NOT NULL REFERENCES assignments(id),attempt_no INTEGER NOT NULL,memory_floor INTEGER NOT NULL,spool_floor INTEGER NOT NULL,min_cpu INTEGER NOT NULL,min_memory INTEGER NOT NULL,min_spool INTEGER NOT NULL,next_after TEXT NOT NULL,manifest BLOB NOT NULL,PRIMARY KEY(run_id,unit_id,assignment_id));
CREATE INDEX IF NOT EXISTS resource_failure_unit ON resource_failures(run_id,unit_id);
CREATE TABLE IF NOT EXISTS controller_storage_init(id INTEGER PRIMARY KEY CHECK(id=1));
CREATE TABLE IF NOT EXISTS controller_storage(path TEXT PRIMARY KEY,size INTEGER NOT NULL CHECK(size>=0),claim_id TEXT REFERENCES validator_claims(id));
CREATE INDEX IF NOT EXISTS controller_storage_claim ON controller_storage(claim_id);
CREATE TABLE IF NOT EXISTS controller_storage_totals(id INTEGER PRIMARY KEY CHECK(id=1),bytes INTEGER NOT NULL);
CREATE TRIGGER IF NOT EXISTS controller_storage_insert AFTER INSERT ON controller_storage BEGIN UPDATE controller_storage_totals SET bytes=bytes+NEW.size WHERE id=1; END;
CREATE TRIGGER IF NOT EXISTS controller_storage_delete AFTER DELETE ON controller_storage BEGIN UPDATE controller_storage_totals SET bytes=bytes-OLD.size WHERE id=1; END;
CREATE TABLE IF NOT EXISTS controller_ingress(digest TEXT PRIMARY KEY,size INTEGER NOT NULL CHECK(size>=0));
CREATE TABLE IF NOT EXISTS controller_retained(id INTEGER PRIMARY KEY CHECK(id=1),bytes INTEGER NOT NULL);
CREATE TRIGGER IF NOT EXISTS retained_artifact_insert AFTER INSERT ON artifacts BEGIN UPDATE controller_retained SET bytes=bytes+NEW.size WHERE id=1; END;
CREATE TABLE IF NOT EXISTS validator_claims(id TEXT PRIMARY KEY,started INTEGER NOT NULL DEFAULT 0,exited INTEGER NOT NULL DEFAULT 0,resources BLOB NOT NULL,instance TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS validator_live ON validator_claims(exited);
`

func (c *Controller) installPolicy(tx *sql.Tx, p contracts.ResourcePolicy) error {
	if e := p.Validate(); e != nil {
		return e
	}
	for _, h := range p.Hosts {
		if h.MaxAssignments < 1 || h.MaxAssignments > 64 {
			return errors.New("host requires bounded max_assignments")
		}
	}
	digest, _ := contracts.Digest(p)
	var existing string
	e := tx.QueryRow("SELECT digest FROM resource_policies WHERE revision=?", p.Revision).Scan(&existing)
	if e == nil {
		if existing != digest {
			return errors.New("resource policy revision is immutable")
		}
	} else if errors.Is(e, sql.ErrNoRows) {
		if _, e = tx.Exec("INSERT INTO resource_policies VALUES(?,?,?)", p.Revision, digest, encoded(p)); e != nil {
			return e
		}
	} else {
		return e
	}
	for _, h := range p.Hosts {
		var worker sql.NullString
		e = tx.QueryRow("SELECT worker_id FROM host_bindings WHERE host_id=?", h.HostID).Scan(&worker)
		if e == nil {
			if worker.String != h.WorkerID {
				return errors.New("host/worker binding is immutable")
			}
		} else if errors.Is(e, sql.ErrNoRows) {
			var value any
			if h.WorkerID != "" {
				value = h.WorkerID
			}
			if _, e = tx.Exec("INSERT INTO host_bindings VALUES(?,?)", h.HostID, value); e != nil {
				return e
			}
		} else {
			return e
		}
	}
	var current int64
	e = tx.QueryRow("SELECT revision FROM resource_head WHERE id=1").Scan(&current)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	if p.Revision < current {
		return errors.New("stale resource policy revision")
	}
	_, e = tx.Exec("INSERT INTO resource_head VALUES(1,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision", p.Revision)
	return e
}
func (c *Controller) policy(tx *sql.Tx) (contracts.ResourcePolicy, error) {
	var p contracts.ResourcePolicy
	var revision int64
	if e := tx.QueryRow("SELECT revision FROM resource_head WHERE id=1").Scan(&revision); e != nil {
		return p, e
	}
	if cached, ok := c.policyCache[revision]; ok {
		return cached, nil
	}
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
func controllerOverhead(p contracts.ResourcePolicy) (contracts.ResourceVector, error) {
	var total contracts.ResourceVector
	for i := 0; i < p.TransferSlots; i++ {
		var e error
		total, e = total.Add(p.TransferReservation)
		if e != nil {
			return total, e
		}
	}
	for i := 0; i < p.ValidatorSlots; i++ {
		var e error
		total, e = total.Add(p.ValidatorReservation)
		if e != nil {
			return total, e
		}
	}
	return total, nil
}
func approvalForWorker(p contracts.ResourcePolicy, worker string) (contracts.HostApproval, bool) {
	for _, h := range p.Hosts {
		if h.WorkerID == worker && worker != "" {
			return h, true
		}
	}
	return contracts.HostApproval{}, false
}
func approvalForHost(p contracts.ResourcePolicy, id string) (contracts.HostApproval, bool) {
	for _, h := range p.Hosts {
		if h.HostID == id {
			return h, true
		}
	}
	return contracts.HostApproval{}, false
}
func validateObservation(o contracts.HostObservation) error {
	if o.Resources.Validate() != nil || o.Resources.CPUMilli == 0 || o.Resources.MemoryBytes == 0 || o.Resources.SpoolBytes == 0 || o.AvailableSpoolBytes < 0 || o.AvailableSpoolBytes > o.Resources.SpoolBytes || (o.HardTreeMemory != "unknown" && o.HardTreeMemory != "supported") {
		return errors.New("incomplete resource observation")
	}
	return nil
}
func (c *Controller) registerHost(tx *sql.Tx, r contracts.WorkerRegistration) (any, error) {
	if r.SchemaVersion != 1 || !c.allowedWorker(r.WorkerID) || r.SessionID == "" || r.GOOS == "" || r.GOARCH == "" {
		return nil, errors.New("invalid worker registration")
	}
	p, e := c.policy(tx)
	if e != nil {
		return nil, e
	}
	h, ok := approvalForWorker(p, r.WorkerID)
	if !ok || h.HostID != r.HostID {
		return nil, errors.New("unapproved host/worker binding")
	}
	if e = validateObservation(r.Observed); e != nil {
		return nil, e
	}
	if r.Observed.Source != "host_os" && (r.Observed.Source != "synthetic_test" || !p.AllowSyntheticBackend) {
		return nil, errors.New("unapproved observation source")
	}
	if r.Observed.Source == "synthetic_test" && r.Observed.HardTreeMemory != "unknown" {
		return nil, errors.New("synthetic inventory cannot qualify hard enforcement")
	}
	if r.TotalSpoolBytes != nil && (*r.TotalSpoolBytes < 0 || *r.TotalSpoolBytes < r.RetainedSpoolBytes) {
		return nil, errors.New("invalid total retained spool accounting")
	}
	if _, e = tx.Exec("INSERT INTO host_observations VALUES(?,?,?) ON CONFLICT(host_id) DO UPDATE SET session_id=excluded.session_id,manifest=excluded.manifest", h.HostID, r.SessionID, encoded(r)); e != nil {
		return nil, e
	}
	if r.RetainedSpoolBytes < 0 || r.SpoolRevision < 0 || r.InventoryRevision < 0 {
		return nil, errors.New("invalid retained spool accounting")
	}
	if e = c.saveSpoolAccount(tx, h, contracts.PollRequest{SessionID: r.SessionID, RetainedSpoolBytes: r.RetainedSpoolBytes, SpoolRevision: r.SpoolRevision, InventoryRevision: r.InventoryRevision}); e != nil {
		return nil, e
	}
	return c.hostBudget(tx, p, r.WorkerID, r.SessionID)
}
