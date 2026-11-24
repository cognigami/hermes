// Package controller owns local authority, metadata transactions and acceptance.
package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"hermes/internal/platform"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Config struct {
	Root                                    string
	Workers                                 []string
	MaxAttempts                             int
	MaxConcurrent                           int
	ResourcePolicy                          *contracts.ResourcePolicy
	StopAfter, TerminateAfter, HandoffAfter time.Duration
	Profile                                 string
	Runtime                                 string
	Validator                               string
	ValidatorArgv                           []string
	MaxArtifactBytes                        int64
}
type Controller struct {
	cfg                Config
	db                 *sql.DB
	store              *artifacts.Store
	lock               *os.File
	policyCache        map[int64]contracts.ResourcePolicy
	instance           string
	controllerObserved contracts.HostObservation
	transferSlots      chan struct{}
	readyCursor        map[string]int64
	validatorCursor    int64
	transferMu         sync.Mutex
	transferWG         sync.WaitGroup
	closing            bool
	lifetime           context.Context
	cancelLifetime     context.CancelFunc
	// Only tests set this seam to terminate at actual metadata commit boundaries.
	testCommitBoundary func(operation string, committed bool) error
}

func Open(cfg Config) (*Controller, error) {
	if cfg.ResourcePolicy == nil {
		return nil, errors.New("approved resource policy required")
	}
	if err := cfg.ResourcePolicy.Validate(); err != nil {
		return nil, err
	}
	if cfg.Root == "" || len(cfg.Workers) == 0 || cfg.Profile == "" || cfg.Runtime == "" || cfg.Validator == "" || len(cfg.ValidatorArgv) == 0 || cfg.MaxArtifactBytes <= 0 {
		return nil, errors.New("incomplete controller config")
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 64
	}
	if cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > 64 {
		return nil, errors.New("invalid concurrent assignment bound")
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 2
	}
	if cfg.MaxAttempts < 1 {
		return nil, errors.New("invalid max attempts")
	}
	if cfg.StopAfter == 0 && cfg.TerminateAfter == 0 && cfg.HandoffAfter == 0 {
		cfg.StopAfter = 5 * time.Minute
		cfg.TerminateAfter = 6 * time.Minute
		cfg.HandoffAfter = 7 * time.Minute
	}
	if cfg.StopAfter <= 0 || cfg.StopAfter > cfg.TerminateAfter || cfg.TerminateAfter >= cfg.HandoffAfter {
		return nil, errors.New("invalid finite profile bounds")
	}
	seenWorkers := map[string]bool{}
	for _, id := range cfg.Workers {
		if id == "" || len(id) > 256 || seenWorkers[id] {
			return nil, errors.New("invalid worker identities")
		}
		seenWorkers[id] = true
	}
	if !filepath.IsAbs(cfg.ValidatorArgv[0]) {
		return nil, errors.New("validator executable must be absolute")
	}
	if e := os.MkdirAll(cfg.Root, 0700); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(filepath.Join(cfg.Root, "authority.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		f.Close()
		return nil, fmt.Errorf("controller authority already held: %w", e)
	}
	c := &Controller{cfg: cfg, lock: f, policyCache: map[int64]contracts.ResourcePolicy{}, instance: randomID(), transferSlots: make(chan struct{}, 16), readyCursor: map[string]int64{}}
	c.lifetime, c.cancelLifetime = context.WithCancel(context.Background())
	success := false
	defer func() {
		if !success {
			c.Close()
		}
	}()
	c.controllerObserved, e = platform.ObserveResources(cfg.Root)
	if e != nil {
		return nil, e
	}
	c.store, e = artifacts.New(filepath.Join(cfg.Root, "artifacts"), cfg.MaxArtifactBytes)
	if e != nil {
		return nil, e
	}
	c.db, e = sql.Open("sqlite", filepath.Join(cfg.Root, "metadata.sqlite"))
	if e != nil {
		return nil, e
	}
	c.db.SetMaxOpenConns(1)
	if _, e = c.db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;`); e != nil {
		return nil, e
	}
	var fk, sync int
	var mode string
	if e = c.db.QueryRow("PRAGMA foreign_keys").Scan(&fk); e != nil {
		return nil, e
	}
	if e = c.db.QueryRow("PRAGMA synchronous").Scan(&sync); e != nil {
		return nil, e
	}
	if e = c.db.QueryRow("PRAGMA journal_mode").Scan(&mode); e != nil {
		return nil, e
	}
	if fk != 1 || sync != 2 || mode != "wal" {
		return nil, errors.New("unsupported SQLite durability settings")
	}
	if _, e = c.db.Exec(schema); e != nil {
		return nil, e
	}
	if _, e = c.db.Exec(controlSchema); e != nil {
		return nil, e
	}
	if _, e = c.db.Exec(resourceSchema); e != nil {
		return nil, e
	}
	if _, e = c.db.Exec(finiteSchema); e != nil {
		return nil, e
	}
	if _, e = c.db.Exec(ingressSchema); e != nil {
		return nil, e
	}
	tx, err := c.db.Begin()
	if err != nil {
		return nil, err
	}
	if err = installReadiness(tx); err != nil {
		tx.Rollback()
		return nil, err
	}
	if err = restoreIngress(tx); err != nil {
		tx.Rollback()
		return nil, err
	}
	if err = c.bootstrapResources(tx, *cfg.ResourcePolicy); err != nil {
		tx.Rollback()
		return nil, err
	}
	// Interrupted invocation is not a completed validation failure. Preserve
	// child-ownership claims before requeueing its candidate/refunding attempt.
	if _, err = tx.Exec("UPDATE candidates SET state='pending',attempts=max(attempts-1,0) WHERE state='validating'"); err != nil {
		tx.Rollback()
		return nil, err
	}
	if err = c.reconcileTerminal(tx); err != nil {
		tx.Rollback()
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if err = c.cleanupExitedScratch(context.Background()); err != nil {
		return nil, err
	}
	success = true
	return c, nil
}
func (c *Controller) Close() error {
	c.transferMu.Lock()
	c.closing = true
	c.transferMu.Unlock()
	if c.cancelLifetime != nil {
		c.cancelLifetime()
	}
	c.transferWG.Wait()
	var e error
	if c.db != nil {
		e = c.db.Close()
	}
	if c.lock != nil {
		if x := c.lock.Close(); e == nil {
			e = x
		}
	}
	return e
}

const schema = `
CREATE TABLE IF NOT EXISTS artifacts(digest TEXT PRIMARY KEY,size INTEGER NOT NULL CHECK(size>=0));
CREATE TABLE IF NOT EXISTS definitions(unit_id TEXT PRIMARY KEY,spec TEXT NOT NULL,definition BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS run_units(run_id TEXT NOT NULL REFERENCES runs(id),unit_id TEXT NOT NULL REFERENCES definitions(unit_id),ready INTEGER NOT NULL CHECK(ready IN(0,1)),PRIMARY KEY(run_id,unit_id));
CREATE INDEX IF NOT EXISTS ready_units ON run_units(ready,run_id,unit_id);
CREATE TABLE IF NOT EXISTS dependencies(run_id TEXT NOT NULL,unit_id TEXT NOT NULL,prerequisite TEXT NOT NULL,PRIMARY KEY(run_id,unit_id,prerequisite),FOREIGN KEY(run_id,unit_id) REFERENCES run_units(run_id,unit_id),FOREIGN KEY(run_id,prerequisite) REFERENCES run_units(run_id,unit_id));
CREATE TABLE IF NOT EXISTS packages(run_id TEXT NOT NULL REFERENCES runs(id),id TEXT NOT NULL,digest TEXT NOT NULL,manifest BLOB NOT NULL,PRIMARY KEY(run_id,id));
CREATE TABLE IF NOT EXISTS assignments(id TEXT PRIMARY KEY,run_id TEXT NOT NULL REFERENCES runs(id),worker TEXT NOT NULL,session TEXT NOT NULL,manifest BLOB NOT NULL,receipt INTEGER NOT NULL DEFAULT 0,final BLOB);
CREATE INDEX IF NOT EXISTS active_assignments ON assignments(worker,final);
CREATE TABLE IF NOT EXISTS members(assignment_id TEXT NOT NULL REFERENCES assignments(id),run_id TEXT NOT NULL,unit_id TEXT NOT NULL,PRIMARY KEY(assignment_id,unit_id),FOREIGN KEY(run_id,unit_id) REFERENCES run_units(run_id,unit_id));
CREATE TABLE IF NOT EXISTS owners(run_id TEXT NOT NULL,unit_id TEXT NOT NULL,assignment_id TEXT NOT NULL REFERENCES assignments(id),PRIMARY KEY(run_id,unit_id),FOREIGN KEY(run_id,unit_id) REFERENCES run_units(run_id,unit_id));
CREATE TABLE IF NOT EXISTS candidates(digest TEXT PRIMARY KEY,run_id TEXT NOT NULL,unit_id TEXT NOT NULL,assignment_id TEXT NOT NULL REFERENCES assignments(id),manifest BLOB NOT NULL,output_digest TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('pending','validating','operational_failed','valid','rejected')),attempts INTEGER NOT NULL DEFAULT 0,reason TEXT NOT NULL DEFAULT '',FOREIGN KEY(run_id,unit_id) REFERENCES run_units(run_id,unit_id));
CREATE INDEX IF NOT EXISTS validation_queue ON candidates(state,attempts);
CREATE TABLE IF NOT EXISTS candidate_files(candidate TEXT NOT NULL REFERENCES candidates(digest),digest TEXT NOT NULL REFERENCES artifacts(digest),PRIMARY KEY(candidate,digest));
CREATE TABLE IF NOT EXISTS acceptances(run_id TEXT NOT NULL,unit_id TEXT NOT NULL,candidate TEXT NOT NULL REFERENCES candidates(digest),validator TEXT NOT NULL,verdict BLOB NOT NULL,PRIMARY KEY(run_id,unit_id),FOREIGN KEY(run_id,unit_id) REFERENCES run_units(run_id,unit_id));
CREATE TRIGGER IF NOT EXISTS valid_acceptance BEFORE INSERT ON acceptances BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM candidates c WHERE c.digest=NEW.candidate AND c.run_id=NEW.run_id AND c.unit_id=NEW.unit_id AND c.state='valid') THEN RAISE(ABORT,'acceptance requires valid candidate') END; END;
CREATE TABLE IF NOT EXISTS requests(principal TEXT NOT NULL,id TEXT NOT NULL,body_digest TEXT NOT NULL,outcome BLOB NOT NULL,PRIMARY KEY(principal,id));
`

func encoded(v any) []byte {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return b
}
func artifactExists(tx *sql.Tx, a contracts.Artifact) error {
	var size int64
	if e := tx.QueryRow("SELECT size FROM artifacts WHERE digest=?", a.Digest).Scan(&size); e != nil {
		return fmt.Errorf("artifact not durably registered: %w", e)
	}
	if size != a.Size {
		return errors.New("artifact size mismatch")
	}
	return nil
}
func (c *Controller) mutate(ctx context.Context, principal, id string, body any, fn func(*sql.Tx) (any, error)) ([]byte, error) {
	if id == "" || len(id) > 256 {
		return nil, errors.New("request ID required")
	}
	digest := contracts.Hash(encoded(body))
	tx, e := c.db.BeginTx(ctx, nil)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	var prior string
	var outcome []byte
	e = tx.QueryRow("SELECT body_digest,outcome FROM requests WHERE principal=? AND id=?", principal, id).Scan(&prior, &outcome)
	if e == nil {
		if prior != digest {
			return nil, errors.New("request ID reused with different body")
		}
		return outcome, nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	result, e := fn(tx)
	if e != nil {
		return nil, e
	}
	// An empty poll reserves nothing. Roll back its read-only transaction;
	// only actual reservations need a permanent retry outcome and FULL commit.
	if work, ok := result.(contracts.Work); ok && work.Assignment == nil {
		return encoded(work), nil
	}
	outcome = encoded(result)
	if len(outcome) > contracts.MaxMetadataBytes {
		return nil, errors.New("response metadata too large; reduce package size")
	}
	if _, e = tx.Exec("INSERT INTO requests VALUES(?,?,?,?)", principal, id, digest, outcome); e != nil {
		return nil, e
	}
	operation := "request"
	switch body.(type) {
	case contracts.PollRequest:
		operation = "assignment"
	case contracts.CandidateReport:
		operation = "candidate"
	}
	if e = c.commit(tx, operation); e != nil {
		return nil, e
	}
	return outcome, nil
}
func (c *Controller) submit(tx *sql.Tx, s contracts.Submission) (any, error) {
	if s.SchemaVersion != 1 || s.RunID == "" || len(s.RunID) > 256 || len(s.Packages) == 0 || len(s.Packages) > contracts.MaxUnits {
		return nil, errors.New("invalid submission")
	}
	if e := contracts.ValidateGraph(s.Units); e != nil {
		return nil, e
	}
	units := map[string]contracts.Unit{}
	for _, u := range s.Units {
		if u.Runtime != c.cfg.Runtime || u.Validator != c.cfg.Validator {
			return nil, errors.New("unsupported application contract")
		}
		units[u.UnitID] = u
		for _, a := range u.Inputs {
			if e := artifactExists(tx, a); e != nil {
				return nil, e
			}
		}
	}
	covered := map[string]bool{}
	for _, p := range s.Packages {
		if e := p.Validate(); e != nil {
			return nil, e
		}
		for _, a := range p.Preparation {
			if e := artifactExists(tx, a); e != nil {
				return nil, e
			}
		}
		for _, r := range p.Units {
			u, ok := units[r.UnitID]
			if !ok || u.SpecDigest != r.SpecDigest || covered[r.UnitID] {
				return nil, errors.New("invalid package membership")
			}
			covered[r.UnitID] = true
		}
	}
	if len(covered) != len(units) {
		return nil, errors.New("units missing packages")
	}
	if _, e := tx.Exec("INSERT INTO runs VALUES(?)", s.RunID); e != nil {
		return nil, e
	}
	for _, u := range s.Units {
		var b []byte
		e := tx.QueryRow("SELECT definition FROM definitions WHERE unit_id=?", u.UnitID).Scan(&b)
		if e == nil {
			var prior contracts.Unit
			if e = json.Unmarshal(b, &prior); e != nil {
				return nil, e
			}
			if e = contracts.CompatibleUnit(prior, u); e != nil {
				return nil, e
			}
		} else if errors.Is(e, sql.ErrNoRows) {
			if _, e = tx.Exec("INSERT INTO definitions VALUES(?,?,?)", u.UnitID, u.SpecDigest, encoded(u)); e != nil {
				return nil, e
			}
		} else {
			return nil, e
		}
		ready := 0
		if len(u.Dependencies) == 0 {
			ready = 1
		}
		if _, e = tx.Exec("INSERT INTO run_units VALUES(?,?,?)", s.RunID, u.UnitID, ready); e != nil {
			return nil, e
		}
	}
	for _, u := range s.Units {
		for _, d := range u.Dependencies {
			if _, e := tx.Exec("INSERT INTO dependencies VALUES(?,?,?)", s.RunID, u.UnitID, d); e != nil {
				return nil, e
			}
		}
	}
	for _, p := range s.Packages {
		digest, _ := contracts.Digest(p)
		if _, e := tx.Exec("INSERT INTO packages VALUES(?,?,?,?)", s.RunID, p.PackageID, digest, encoded(p)); e != nil {
			return nil, e
		}
	}
	if e := registerReadiness(tx, s); e != nil {
		return nil, e
	}
	if e := c.registerRequirements(tx, s); e != nil {
		return nil, e
	}
	return contracts.Acknowledgment{SchemaVersion: 1, RunID: s.RunID}, nil
}
func (c *Controller) work(tx *sql.Tx, a contracts.Assignment) (contracts.Work, error) {
	w := contracts.Work{SchemaVersion: 1, Assignment: &a}
	var packageBytes []byte
	if e := tx.QueryRow("SELECT manifest FROM packages WHERE run_id=? AND digest=?", a.RunID, a.PackageDigest).Scan(&packageBytes); e != nil {
		return w, e
	}
	var p contracts.Package
	if e := json.Unmarshal(packageBytes, &p); e != nil {
		return w, e
	}
	w.Package = &p
	if a.Allocation.Resources != (contracts.ResourceVector{}) {
		policy, e := c.policyAt(tx, a.PolicyRevision)
		if e != nil {
			return w, e
		}
		budget, e := c.hostBudget(tx, policy, a.WorkerID, a.SessionID)
		if e != nil {
			return w, e
		}
		w.Budget = &budget
	}
	for _, r := range a.Units {
		var b []byte
		if e := tx.QueryRow("SELECT definition FROM definitions WHERE unit_id=?", r.UnitID).Scan(&b); e != nil {
			return w, e
		}
		var u contracts.Unit
		if e := json.Unmarshal(b, &u); e != nil {
			return w, e
		}
		w.Units = append(w.Units, u)
	}
	return w, nil
}
func (c *Controller) bound(tx *sql.Tx, r contracts.Receipt) (contracts.Assignment, error) {
	var a contracts.Assignment
	if r.SchemaVersion != 1 {
		return a, errors.New("unsupported version")
	}
	var b []byte
	if e := tx.QueryRow("SELECT manifest FROM assignments WHERE id=?", r.AssignmentID).Scan(&b); e != nil {
		return a, e
	}
	if e := json.Unmarshal(b, &a); e != nil {
		return a, e
	}
	if a.WorkerID != r.WorkerID || a.SessionID != r.SessionID {
		return a, errors.New("assignment identity mismatch")
	}
	return a, nil
}
func (c *Controller) candidate(tx *sql.Tx, r contracts.CandidateReport) (any, error) {
	a, e := c.bound(tx, r.Receipt)
	if e != nil {
		return nil, e
	}
	v := r.Candidate
	if e = v.Validate(); e != nil {
		return nil, e
	}
	match := false
	for _, u := range a.Units {
		if u.UnitID == v.UnitID && u.SpecDigest == v.SpecDigest {
			match = true
		}
	}
	if !match || v.AssignmentID != a.AssignmentID || v.AttemptID != a.AssignmentID {
		return nil, errors.New("candidate outside reserved assignment")
	}
	for _, f := range v.Files {
		if e = artifactExists(tx, f.Artifact); e != nil {
			return nil, e
		}
	}
	digest, _ := contracts.Digest(v)
	output, _ := contracts.Digest(struct {
		Outcome string
		Files   []contracts.ResultFile
	}{v.Outcome, v.Files})
	if _, e = tx.Exec("INSERT OR IGNORE INTO candidates(digest,run_id,unit_id,assignment_id,manifest,output_digest,state) VALUES(?,?,?,?,?,?,'pending')", digest, a.RunID, v.UnitID, a.AssignmentID, encoded(v), output); e != nil {
		return nil, e
	}
	for _, f := range v.Files {
		if _, e = tx.Exec("INSERT OR IGNORE INTO candidate_files VALUES(?,?)", digest, f.Artifact.Digest); e != nil {
			return nil, e
		}
	}
	return contracts.Acknowledgment{SchemaVersion: 1, RawPublished: digest}, nil
}
func (c *Controller) final(tx *sql.Tx, r contracts.FinalReport) (any, error) {
	a, e := c.bound(tx, r.Receipt)
	if e != nil {
		return nil, e
	}
	var resolved int
	if e = tx.QueryRow("SELECT count(*) FROM terminal_evidence WHERE assignment_id=?", a.AssignmentID).Scan(&resolved); e != nil {
		return nil, e
	}
	if resolved > 0 {
		return nil, errors.New("terminal-resolved assignment cannot apply a new final inventory")
	}
	if len(r.OperationalReason) > 4096 {
		return nil, errors.New("operational reason too large")
	}
	var uncertain int
	if e = tx.QueryRow("SELECT count(*) FROM execution_reports e WHERE e.assignment_id=? AND e.state='uncertain' AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=e.assignment_id)", a.AssignmentID).Scan(&uncertain); e != nil {
		return nil, e
	}
	if uncertain > 0 {
		return nil, errors.New("uncertain execution requires exact terminal evidence")
	}
	var minStarted int
	e = tx.QueryRow("SELECT last_grant FROM assignment_controls WHERE assignment_id=?", a.AssignmentID).Scan(&minStarted)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	var reportBytes []byte
	e = tx.QueryRow("SELECT manifest FROM execution_reports WHERE assignment_id=?", a.AssignmentID).Scan(&reportBytes)
	if e == nil {
		var report contracts.ReconcileRequest
		if e = json.Unmarshal(reportBytes, &report); e != nil {
			return nil, e
		}
		if len(report.Started) > minStarted {
			minStarted = len(report.Started)
		}
	} else if !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	var resourceEvidence []byte
	err := tx.QueryRow("SELECT manifest FROM resource_failures WHERE assignment_id=? LIMIT 1", a.AssignmentID).Scan(&resourceEvidence)
	if err == nil {
		var stop contracts.ResourceStop
		if err = json.Unmarshal(resourceEvidence, &stop); err != nil {
			return nil, err
		}
		if stop.GrantIndex > minStarted {
			minStarted = stop.GrantIndex
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if r.LaunchFailed && (minStarted != 0 || len(r.Completed) != 0 || len(r.Interrupted) != 0) {
		return nil, errors.New("failed launch contradicts observed unit grants/results")
	}
	for _, id := range r.NeverStarted {
		for i, u := range a.Units {
			if id == u.UnitID && i < minStarted {
				return nil, errors.New("never-started inventory contradicts recorded grants")
			}
		}
	}
	seen := map[string]bool{}
	members := map[string]bool{}
	for _, u := range a.Units {
		members[u.UnitID] = true
	}
	for _, id := range append(append(append([]string{}, r.Completed...), r.NeverStarted...), r.Interrupted...) {
		if !members[id] || seen[id] {
			return nil, errors.New("invalid final inventory")
		}
		seen[id] = true
	}
	if len(seen) != len(members) {
		return nil, errors.New("incomplete final inventory")
	}
	for _, id := range r.Completed {
		var n int
		if e = tx.QueryRow("SELECT COUNT(*) FROM candidates WHERE assignment_id=? AND unit_id=?", a.AssignmentID, id).Scan(&n); e != nil {
			return nil, e
		}
		if n == 0 {
			return nil, errors.New("completed result not yet published")
		}
	}
	var previous []byte
	if e = tx.QueryRow("SELECT final FROM assignments WHERE id=?", a.AssignmentID).Scan(&previous); e != nil {
		return nil, e
	}
	b := encoded(r)
	if previous != nil && string(previous) != string(b) {
		return nil, errors.New("conflicting final inventory")
	}
	if _, e = tx.Exec("UPDATE assignments SET final=? WHERE id=?", b, a.AssignmentID); e != nil {
		return nil, e
	}
	if previous == nil {
		for _, id := range r.NeverStarted {
			if r.LaunchFailed {
				continue
			}
			if _, e = tx.Exec("UPDATE unit_attempts SET attempts=max(attempts-1,0) WHERE run_id=? AND unit_id=? AND NOT EXISTS(SELECT 1 FROM resource_failures f WHERE f.run_id=unit_attempts.run_id AND f.unit_id=unit_attempts.unit_id AND f.assignment_id=?)", a.RunID, id, a.AssignmentID); e != nil {
				return nil, e
			}
		}
		for _, id := range r.Interrupted {
			if _, e = tx.Exec("UPDATE unit_attempts SET reason='interrupted: explicit retry policy required' WHERE run_id=? AND unit_id=?", a.RunID, id); e != nil {
				return nil, e
			}
		}
	}
	if _, e = tx.Exec("DELETE FROM owners WHERE assignment_id=?", a.AssignmentID); e != nil {
		return nil, e
	}
	if e = c.reconcileResourceAssignment(tx, a.AssignmentID); e != nil {
		return nil, e
	}
	return contracts.Acknowledgment{SchemaVersion: 1, Quiescent: true}, nil
}
func (c *Controller) Status(ctx context.Context, run string) (contracts.Status, error) {
	s := contracts.Status{SchemaVersion: 1, RunID: run}
	var n int
	if e := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runs WHERE id=?", run).Scan(&n); e != nil {
		return s, e
	}
	if n == 0 {
		return s, errors.New("unknown run")
	}
	for _, q := range []struct {
		sql string
		dst *int
	}{{"SELECT COUNT(*) FROM run_units WHERE run_id=?", &s.Units}, {"SELECT COUNT(*) FROM acceptances WHERE run_id=?", &s.Accepted}, {"SELECT COUNT(*) FROM candidates WHERE run_id=? AND state='operational_failed'", &s.ValidationFailures}, {"SELECT COUNT(*) FROM candidates c JOIN acceptances a ON a.run_id=c.run_id AND a.unit_id=c.unit_id JOIN candidates canonical ON canonical.digest=a.candidate WHERE c.run_id=? AND c.output_digest!=canonical.output_digest", &s.Conflicts}} {
		if e := c.db.QueryRowContext(ctx, q.sql, run).Scan(q.dst); e != nil {
			return s, e
		}
	}
	for _, q := range []struct {
		sql string
		dst *int
	}{{"SELECT count(*) FROM execution_reports e JOIN assignments a ON a.id=e.assignment_id WHERE a.run_id=? AND e.state='uncertain' AND a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=a.id)", &s.UncertainAssignments}, {"SELECT count(*) FROM unit_attempts WHERE run_id=? AND reason='attempt budget exhausted'", &s.Exhausted}, {"SELECT count(*) FROM unit_attempts WHERE run_id=? AND data_loss=1", &s.DataLossUnits}, {"SELECT count(*) FROM unit_attempts WHERE run_id=? AND reason='waiting retained validation'", &s.WaitingRetainedValidation}} {
		if e := c.db.QueryRowContext(ctx, q.sql, run).Scan(q.dst); e != nil {
			return s, e
		}
	}
	if e := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM assignments WHERE run_id=? AND final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence t WHERE t.assignment_id=assignments.id)", run).Scan(&n); e != nil {
		return s, e
	}
	s.Quiescent = n == 0
	return s, nil
}
