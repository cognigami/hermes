package controller

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hermes/internal/contracts"
	"sort"
	"time"
)

var ErrFiniteInvalid = errors.New("invalid finite exchange")
var ErrFiniteUnavailable = errors.New("finite exchange unavailable")

const finiteSchema = `
CREATE TABLE IF NOT EXISTS finite_runtime_config(id INTEGER PRIMARY KEY CHECK(id=1),instance TEXT NOT NULL,manifest BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS finite_exports(revision INTEGER PRIMARY KEY AUTOINCREMENT,bundle_id TEXT NOT NULL UNIQUE,worker_id TEXT NOT NULL,session_id TEXT NOT NULL,registration_id TEXT NOT NULL,digest TEXT UNIQUE,manifest BLOB NOT NULL,UNIQUE(worker_id,session_id,registration_id));
CREATE TABLE IF NOT EXISTS finite_members(assignment_id TEXT PRIMARY KEY REFERENCES assignments(id),export_revision INTEGER NOT NULL REFERENCES finite_exports(revision),assignment_digest TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS finite_member_ack(assignment_id TEXT PRIMARY KEY REFERENCES finite_members(assignment_id),reconciliation_revision INTEGER NOT NULL DEFAULT 0,final_digest TEXT NOT NULL DEFAULT '',resource_stop_ack INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS finite_raw_ack(assignment_id TEXT NOT NULL REFERENCES finite_members(assignment_id),candidate_digest TEXT NOT NULL REFERENCES candidates(digest),PRIMARY KEY(assignment_id,candidate_digest));
CREATE INDEX IF NOT EXISTS finite_export_members ON finite_members(export_revision,assignment_id);
CREATE INDEX IF NOT EXISTS finite_worker_requests ON requests(principal);
`

// ConfigureFiniteRuntimes is LOCAL deployment configuration, never an HTTP
// approval endpoint or permission to execute request-selected argv. A new
// controller instance must explicitly configure again; old sealed bundles stay
// readable and importable even when future export permission is absent/disabled.
func (c *Controller) ConfigureFiniteRuntimes(ctx context.Context, approved []contracts.FiniteRuntimeRef) error {
	if len(approved) > contracts.MaxFiniteMembers {
		return fmt.Errorf("%w: runtime approval bound", ErrFiniteInvalid)
	}
	tx, e := c.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	seen := map[string]bool{}
	for _, r := range approved {
		if e = r.Validate(); e != nil {
			return fmt.Errorf("%w: %v", ErrFiniteInvalid, e)
		}
		baseline, ok := finiteBaseline(r.GOARCH)
		if r.Runtime != c.cfg.Runtime || r.GOOS != "darwin" || !ok || r.ISABaseline != baseline {
			return fmt.Errorf("%w: unsupported deployment runtime target", ErrFiniteInvalid)
		}
		key := r.Runtime + ":" + r.GOOS + ":" + r.GOARCH + ":" + r.ISABaseline
		if seen[key] {
			return fmt.Errorf("%w: ambiguous approved runtime", ErrFiniteInvalid)
		}
		seen[key] = true
		for _, a := range append([]contracts.Artifact{r.Executable}, r.Support...) {
			if e = artifactExists(tx, a); e != nil {
				return fmt.Errorf("approved runtime bytes are not durable: %w", e)
			}
		}
	}
	b := encoded(approved)
	if len(b) > contracts.MaxMetadataBytes {
		return fmt.Errorf("%w: approval metadata bound", ErrFiniteInvalid)
	}
	if _, e = tx.Exec("INSERT INTO finite_runtime_config VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET instance=excluded.instance,manifest=excluded.manifest", c.instance, b); e != nil {
		return e
	}
	return tx.Commit()
}
func finiteBaseline(arch string) (string, bool) {
	switch arch {
	case "amd64":
		return "v1", true
	case "arm64":
		return "v8.0", true
	}
	return "", false
}
func (c *Controller) finiteRuntime(tx *sql.Tx, r contracts.WorkerRegistration) (contracts.FiniteRuntimeRef, error) {
	var b []byte
	var ref contracts.FiniteRuntimeRef
	if e := tx.QueryRow("SELECT manifest FROM finite_runtime_config WHERE id=1 AND instance=?", c.instance).Scan(&b); e != nil {
		if errors.Is(e, sql.ErrNoRows) {
			return ref, fmt.Errorf("%w: local runtime approval not configured", ErrFiniteUnavailable)
		}
		return ref, e
	}
	var approved []contracts.FiniteRuntimeRef
	if e := contracts.Decode(bytes.NewReader(b), &approved); e != nil {
		return ref, e
	}
	baseline, ok := finiteBaseline(r.GOARCH)
	if !ok {
		return ref, fmt.Errorf("%w: unsupported registered architecture", ErrFiniteUnavailable)
	}
	matches := 0
	for _, candidate := range approved {
		if candidate.Runtime == c.cfg.Runtime && candidate.GOOS == r.GOOS && candidate.GOARCH == r.GOARCH && candidate.ISABaseline == baseline {
			matches++
			ref = candidate
		}
	}
	if matches != 1 {
		return ref, fmt.Errorf("%w: no unique local runtime approval for registered host", ErrFiniteUnavailable)
	}
	return ref, nil
}

func (c *Controller) ExportFinite(ctx context.Context, request contracts.FiniteExportRequest) (contracts.FiniteBundleEnvelope, error) {
	var envelope contracts.FiniteBundleEnvelope
	if e := request.Validate(); e != nil {
		return envelope, fmt.Errorf("%w: %v", ErrFiniteInvalid, e)
	}
	body, e := c.mutate(ctx, "operator", request.RequestID, request, func(tx *sql.Tx) (any, error) { return c.sealFinite(tx, request) })
	if e != nil {
		return envelope, e
	}
	if e = contracts.Decode(bytes.NewReader(body), &envelope); e != nil {
		return envelope, e
	}
	return envelope, envelope.Validate()
}
func (c *Controller) sealFinite(tx *sql.Tx, r contracts.FiniteExportRequest) (contracts.FiniteBundleEnvelope, error) {
	var empty contracts.FiniteBundleEnvelope
	if !r.StopNewUnitsAt.After(time.Now()) {
		return empty, fmt.Errorf("%w: finite start bound already expired", ErrFiniteInvalid)
	}
	if !c.allowedWorker(r.WorkerID) {
		return empty, fmt.Errorf("%w: unconfigured worker", ErrFiniteInvalid)
	}
	policy, e := c.policy(tx)
	if e != nil {
		return empty, e
	}
	host, ok := approvalForWorker(policy, r.WorkerID)
	if !ok || host.HostID != r.HostID {
		return empty, fmt.Errorf("%w: host/worker binding", ErrFiniteInvalid)
	}
	observation, e := c.workerObservation(tx, host)
	if e != nil {
		if errors.Is(e, sql.ErrNoRows) {
			return empty, fmt.Errorf("%w: real native registration required", ErrFiniteUnavailable)
		}
		return empty, e
	}
	if observation.SessionID != r.SessionID || observation.GOOS != "darwin" || observation.Observed.Source != "host_os" || observation.TotalSpoolBytes == nil {
		return empty, fmt.Errorf("%w: exact registered native session/full spool accounting required", ErrFiniteUnavailable)
	}
	if *observation.TotalSpoolBytes < 0 || *observation.TotalSpoolBytes < observation.RetainedSpoolBytes {
		return empty, fmt.Errorf("%w: invalid registered retained total", ErrFiniteInvalid)
	}
	var latest string
	if e = tx.QueryRow("SELECT id FROM requests INDEXED BY finite_worker_requests WHERE principal=? ORDER BY rowid DESC LIMIT 1", "worker:"+r.WorkerID).Scan(&latest); e != nil || latest != observation.RequestID {
		return empty, fmt.Errorf("%w: refresh actual native registration before finite sealing", ErrFiniteUnavailable)
	}
	var active int
	if e = tx.QueryRow("SELECT count(*) FROM assignments a WHERE a.worker=? AND a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)", r.WorkerID).Scan(&active); e != nil {
		return empty, e
	}
	if active != 0 {
		return empty, fmt.Errorf("%w: reconcile existing execution before finite batch", ErrFiniteUnavailable)
	}
	ref, e := c.finiteRuntime(tx, observation)
	if e != nil {
		return empty, e
	}
	var existing int
	if e = tx.QueryRow("SELECT count(*) FROM finite_exports WHERE bundle_id=? OR (worker_id=? AND session_id=? AND registration_id=?)", r.BundleID, r.WorkerID, r.SessionID, observation.RequestID).Scan(&existing); e != nil {
		return empty, e
	}
	if existing != 0 {
		return empty, fmt.Errorf("%w: immutable bundle/registration already consumed; retry original request", ErrFiniteInvalid)
	}
	inserted, e := tx.Exec("INSERT INTO finite_exports(bundle_id,worker_id,session_id,registration_id,manifest) VALUES(?,?,?,?,?)", r.BundleID, r.WorkerID, r.SessionID, observation.RequestID, []byte("{}"))
	if e != nil {
		return empty, e
	}
	revision, e := inserted.LastInsertId()
	if e != nil {
		return empty, e
	}
	now := time.Now().UTC()
	bounds := reservationBounds{now.Add(c.cfg.StopAfter), now.Add(c.cfg.TerminateAfter), now.Add(c.cfg.HandoffAfter)}
	if r.StopNewUnitsAt.Before(bounds.Stop) {
		bounds.Stop = r.StopNewUnitsAt
	}
	if r.TerminateAt.Before(bounds.Terminate) {
		bounds.Terminate = r.TerminateAt
	}
	if r.HandoffDeadline.Before(bounds.Handoff) {
		bounds.Handoff = r.HandoffDeadline
	}
	bundle := contracts.FiniteBundle{SchemaVersion: contracts.Version, BundleID: r.BundleID, WorkerID: r.WorkerID, SessionID: r.SessionID, HostID: r.HostID, ExportRevision: revision, RuntimeRefs: []contracts.FiniteRuntimeRef{ref}}
	seenCursor := map[int64]bool{}
	for scanned := 0; scanned < contracts.MaxUnits && len(bundle.Members) < r.MemberLimit; scanned++ {
		work, e := c.reserveReadyAssignment(tx, reservationInventory{Worker: r.WorkerID, Session: r.SessionID, Key: fmt.Sprintf("finite:%s:%d:%d", r.BundleID, revision, len(bundle.Members)), RetainedSpoolBytes: *observation.TotalSpoolBytes, Bounds: &bounds})
		if e != nil {
			return empty, e
		}
		if work.Assignment == nil {
			cursor := c.readyCursor[r.WorkerID]
			if cursor == 0 || seenCursor[cursor] {
				break
			}
			seenCursor[cursor] = true
			continue
		}
		seenCursor = map[int64]bool{}
		digest, e := contracts.Digest(*work.Assignment)
		if e != nil {
			return empty, e
		}
		if _, e = tx.Exec("INSERT INTO finite_members VALUES(?,?,?)", work.Assignment.AssignmentID, revision, digest); e != nil {
			return empty, e
		}
		if _, e = tx.Exec("INSERT INTO finite_member_ack(assignment_id) VALUES(?)", work.Assignment.AssignmentID); e != nil {
			return empty, e
		}
		bundle.Members = append(bundle.Members, contracts.FiniteMember{Work: work, AssignmentDigest: digest})
	}
	if len(bundle.Members) == 0 {
		return empty, fmt.Errorf("%w: no ready/admissible finite members", ErrFiniteUnavailable)
	}
	// Refresh all HostBudget snapshots after ALL reservations, not successively
	// smaller prefixes of the batch. Assignment bounds/profiles never change.
	inventory := map[string]contracts.Artifact{}
	add := func(a contracts.Artifact) error {
		if prior, ok := inventory[a.Digest]; ok && prior != a {
			return fmt.Errorf("%w: conflicting artifact descriptors", ErrFiniteInvalid)
		}
		if e := artifactExists(tx, a); e != nil {
			return e
		}
		inventory[a.Digest] = a
		return nil
	}
	for _, a := range append([]contracts.Artifact{ref.Executable}, ref.Support...) {
		if e = add(a); e != nil {
			return empty, e
		}
	}
	for i, m := range bundle.Members {
		work, e := c.work(tx, *m.Work.Assignment)
		if e != nil {
			return empty, e
		}
		bundle.Members[i].Work = work
		for _, a := range work.Package.Preparation {
			if e = add(a); e != nil {
				return empty, e
			}
		}
		for _, u := range work.Units {
			for _, a := range u.Inputs {
				if e = add(a); e != nil {
					return empty, e
				}
			}
		}
	}
	for _, a := range inventory {
		bundle.Blobs = append(bundle.Blobs, a)
	}
	sort.Slice(bundle.Blobs, func(i, j int) bool { return bundle.Blobs[i].Digest < bundle.Blobs[j].Digest })
	digest, e := contracts.Digest(bundle)
	if e != nil {
		return empty, e
	}
	envelope := contracts.FiniteBundleEnvelope{BundleDigest: digest, Bundle: bundle}
	if e = envelope.Validate(); e != nil {
		return empty, fmt.Errorf("%w: %v", ErrFiniteInvalid, e)
	}
	if !bounds.Stop.After(time.Now()) {
		return empty, fmt.Errorf("%w: start bound expired during sealing", ErrFiniteUnavailable)
	}
	if _, e = tx.Exec("UPDATE finite_exports SET digest=?,manifest=? WHERE revision=?", digest, encoded(envelope), revision); e != nil {
		return empty, e
	}
	// The existing host accounting remains the only budget algorithm.
	controllerHost, ok := approvalForHost(policy, policy.ControllerHostID)
	if !ok {
		return empty, errors.New("controller approval unavailable")
	}
	used, limit, e := c.hostUsage(tx, policy, controllerHost, -1)
	if e != nil {
		return empty, e
	}
	if e = c.addOutputReservations(tx, &used); e != nil {
		return empty, e
	}
	if !used.Fits(limit) {
		return empty, ErrControllerCapacity
	}
	return envelope, nil
}

func (c *Controller) FiniteBundle(ctx context.Context, bundleID string) (contracts.FiniteBundleEnvelope, error) {
	var envelope contracts.FiniteBundleEnvelope
	if !contracts.SafeFiniteBundleID(bundleID) {
		return envelope, fmt.Errorf("%w: bundle ID", ErrFiniteInvalid)
	}
	var b []byte
	if e := c.db.QueryRowContext(ctx, "SELECT manifest FROM finite_exports WHERE bundle_id=?", bundleID).Scan(&b); e != nil {
		return envelope, e
	}
	if e := contracts.Decode(bytes.NewReader(b), &envelope); e != nil {
		return envelope, e
	}
	return envelope, envelope.Validate()
}
