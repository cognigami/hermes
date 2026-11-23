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
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"time"
)

type Config struct {
	Root             string
	WorkerID         string
	Profile          string
	Runtime          string
	Validator        string
	ValidatorArgv    []string
	MaxArtifactBytes int64
}
type Controller struct {
	cfg   Config
	db    *sql.DB
	store *artifacts.Store
	lock  *os.File
	// Only tests set this seam to terminate at actual metadata commit boundaries.
	testCommitBoundary func(operation string, committed bool) error
}

func Open(cfg Config) (*Controller, error) {
	if cfg.Root == "" || cfg.WorkerID == "" || cfg.Profile == "" || cfg.Runtime == "" || cfg.Validator == "" || len(cfg.ValidatorArgv) == 0 || cfg.MaxArtifactBytes <= 0 {
		return nil, errors.New("incomplete controller config")
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
	c := &Controller{cfg: cfg, lock: f}
	success := false
	defer func() {
		if !success {
			c.Close()
		}
	}()
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
	// Interrupted validation did not commit an operational failure. Refund its
	// claim so even an interrupted third invocation can finish after recovery.
	if _, e = c.db.Exec("UPDATE candidates SET state='pending',attempts=max(attempts-1,0) WHERE state='validating'"); e != nil {
		return nil, e
	}
	success = true
	return c, nil
}
func (c *Controller) Close() error {
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
		if u.Runtime != c.cfg.Runtime || u.Validator != c.cfg.Validator || len(u.RequiredCapabilities) > 0 {
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
	return contracts.Acknowledgment{SchemaVersion: 1, RunID: s.RunID}, nil
}
func (c *Controller) poll(tx *sql.Tx, p contracts.PollRequest) (any, error) {
	if p.SchemaVersion != 1 || p.WorkerID != c.cfg.WorkerID || p.SessionID == "" || len(p.SessionID) > 256 {
		return nil, errors.New("invalid worker session")
	}
	var b []byte
	e := tx.QueryRow("SELECT manifest FROM assignments WHERE worker=? AND final IS NULL ORDER BY id LIMIT 1", p.WorkerID).Scan(&b)
	if e == nil {
		var a contracts.Assignment
		if e = json.Unmarshal(b, &a); e != nil {
			return nil, e
		}
		if a.SessionID != p.SessionID {
			return nil, errors.New("unfinished prior session: reconciliation required")
		}
		// The original request outcome is replayed by mutate before this
		// function. A different request cannot prove a lost spool never ran.
		return nil, errors.New("unresolved assignment: original request retry or retained journal required; reconciliation deferred to S3")
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return nil, e
	}
	rows, e := tx.Query("SELECT run_id,manifest,digest FROM packages ORDER BY run_id,id")
	if e != nil {
		return nil, e
	}
	type pkg struct {
		run, digest string
		p           contracts.Package
	}
	var packages []pkg
	for rows.Next() {
		var x pkg
		if e = rows.Scan(&x.run, &b, &x.digest); e != nil {
			rows.Close()
			return nil, e
		}
		if e = json.Unmarshal(b, &x.p); e != nil {
			rows.Close()
			return nil, e
		}
		packages = append(packages, x)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	for _, x := range packages {
		var selected []contracts.UnitRef
		for _, r := range x.p.Units {
			var eligible int
			e = tx.QueryRow(`SELECT COUNT(*) FROM run_units u WHERE run_id=? AND unit_id=? AND ready=1 AND NOT EXISTS(SELECT 1 FROM owners o WHERE o.run_id=u.run_id AND o.unit_id=u.unit_id) AND NOT EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id=u.run_id AND a.unit_id=u.unit_id) AND NOT EXISTS(SELECT 1 FROM candidates c WHERE c.run_id=u.run_id AND c.unit_id=u.unit_id)`, x.run, r.UnitID).Scan(&eligible)
			if e != nil {
				return nil, e
			}
			if eligible == 1 {
				selected = append(selected, r)
			}
		}
		if len(selected) == 0 {
			continue
		}
		now := time.Now().UTC()
		a := contracts.Assignment{SchemaVersion: 1, AssignmentID: "assignment-" + contracts.Hash([]byte(p.RequestID+":"+p.SessionID)), RunID: x.run, WorkerID: p.WorkerID, SessionID: p.SessionID, PackageDigest: x.digest, Units: selected, Runtime: c.cfg.Runtime, Profile: c.cfg.Profile, PolicyRevision: 1, StopNewUnitsAt: now.Add(5 * time.Minute), TerminateAt: now.Add(6 * time.Minute), HandoffDeadline: now.Add(7 * time.Minute)}
		if e = a.Validate(); e != nil {
			return nil, e
		}
		if _, e = tx.Exec("INSERT INTO assignments(id,run_id,worker,session,manifest) VALUES(?,?,?,?,?)", a.AssignmentID, a.RunID, a.WorkerID, a.SessionID, encoded(a)); e != nil {
			return nil, e
		}
		for _, r := range selected {
			if _, e = tx.Exec("INSERT INTO members VALUES(?,?,?)", a.AssignmentID, a.RunID, r.UnitID); e != nil {
				return nil, e
			}
			if _, e = tx.Exec("INSERT INTO owners VALUES(?,?,?)", a.RunID, r.UnitID, a.AssignmentID); e != nil {
				return nil, e
			}
		}
		return c.work(tx, a)
	}
	return contracts.Work{SchemaVersion: 1}, nil
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
	if !match || v.AssignmentID != a.AssignmentID || v.AttemptID != a.AssignmentID || a.Runtime != c.cfg.Runtime || a.Profile != c.cfg.Profile {
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
	seen := map[string]bool{}
	members := map[string]bool{}
	for _, u := range a.Units {
		members[u.UnitID] = true
	}
	for _, id := range append(append([]string{}, r.Completed...), r.NeverStarted...) {
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
	if _, e = tx.Exec("DELETE FROM owners WHERE assignment_id=?", a.AssignmentID); e != nil {
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
	if e := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM assignments WHERE run_id=? AND final IS NULL", run).Scan(&n); e != nil {
		return s, e
	}
	s.Quiescent = n == 0
	return s, nil
}
