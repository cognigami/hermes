package controller

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"hermes/internal/contracts"
)

func readinessDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "readiness.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	for _, stmt := range []string{`PRAGMA foreign_keys=ON`, schema, controlSchema} {
		if _, err = db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	return db
}
func readyTx(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback() })
	return tx
}
func readyExec(t *testing.T, tx *sql.Tx, q string, args ...any) {
	t.Helper()
	if _, err := tx.Exec(q, args...); err != nil {
		t.Fatal(err)
	}
}
func readySubmission(t *testing.T, tx *sql.Tx, run string, deps map[string][]string, order ...string) contracts.Submission {
	t.Helper()
	s := contracts.Submission{SchemaVersion: 1, RunID: run}
	readyExec(t, tx, `INSERT INTO runs VALUES(?)`, run)
	readyExec(t, tx, `INSERT INTO assignments(id,run_id,worker,session,manifest) VALUES(?,?,'worker','session','{}')`, run, run)
	for _, id := range order {
		spec := contracts.Hash([]byte(id))
		readyExec(t, tx, `INSERT OR IGNORE INTO definitions VALUES(?,?,?)`, id, spec, `{}`)
		ready := 0
		if len(deps[id]) == 0 {
			ready = 1
		}
		readyExec(t, tx, `INSERT INTO run_units VALUES(?,?,?)`, run, id, ready)
		s.Units = append(s.Units, contracts.Unit{UnitID: id, SpecDigest: spec, Dependencies: deps[id]})
		p := contracts.Package{SchemaVersion: 1, PackageID: "pkg-" + id, Units: []contracts.UnitRef{{UnitID: id, SpecDigest: spec}}}
		digest, _ := contracts.Digest(p)
		readyExec(t, tx, `INSERT INTO packages VALUES(?,?,?,?)`, run, p.PackageID, digest, encoded(p))
		s.Packages = append(s.Packages, p)
	}
	for _, u := range s.Units {
		for _, d := range u.Dependencies {
			readyExec(t, tx, `INSERT INTO dependencies VALUES(?,?,?)`, run, u.UnitID, d)
		}
	}
	return s
}
func readyAccept(t *testing.T, tx *sql.Tx, run, id string) {
	t.Helper()
	digest := run + ":" + id
	readyExec(t, tx, `INSERT INTO candidates(digest,run_id,unit_id,assignment_id,manifest,output_digest,state) VALUES(?,?,?,?,'INVALID PAYLOAD','unused','valid')`, digest, run, id, run)
	readyExec(t, tx, `INSERT INTO acceptances VALUES(?,?,?,'validator','INVALID VERDICT')`, run, id, digest)
	if err := acceptReadiness(tx, run, id); err != nil {
		t.Fatal(err)
	}
}
func readyPage(t *testing.T, tx *sql.Tx, after int64, limit int) []readyPackage {
	t.Helper()
	p, err := readyPackages(tx, after, limit)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReadinessStableOrderContinuationAndFilters(t *testing.T) {
	db := readinessDB(t)
	tx := readyTx(t, db)
	if err := installReadiness(tx); err != nil {
		t.Fatal(err)
	}
	s := readySubmission(t, tx, "z-run", nil, "z-unit", "a-unit", "owned", "candidate")
	if err := registerReadiness(tx, s); err != nil {
		t.Fatal(err)
	}
	readyExec(t, tx, `INSERT INTO owners VALUES('z-run','owned','z-run')`)
	readyExec(t, tx, `INSERT INTO candidates(digest,run_id,unit_id,assignment_id,manifest,output_digest,state) VALUES('pending','z-run','candidate','z-run','{}','unused','pending')`)
	p := readyPage(t, tx, 0, 1)
	if len(p) != 1 || p[0].PackageID != "pkg-z-unit" || len(p[0].Ready) != 1 {
		t.Fatalf("first page: %+v", p)
	}
	// A resource-ineligible head is skipped by the caller using this cursor.
	next := readyPage(t, tx, p[0].Cursor, 1)
	if len(next) != 1 || next[0].PackageID != "pkg-a-unit" || next[0].Cursor <= p[0].Cursor {
		t.Fatalf("next page: %+v", next)
	}
	if tail := readyPage(t, tx, next[0].Cursor, 1); len(tail) != 0 {
		t.Fatalf("owned/candidate leaked: %+v", tail)
	}
	readyExec(t, tx, `DELETE FROM owners`)
	if p = readyPage(t, tx, next[0].Cursor, 1); len(p) != 1 || p[0].PackageID != "pkg-owned" {
		t.Fatalf("released owner not available: %+v", p)
	}
	readyExec(t, tx, `INSERT INTO target_controls(kind,target,revision,action,terminate_at) VALUES('run','z-run',1,'drain','')`)
	if p = readyPage(t, tx, 0, 10); len(p) != 0 {
		t.Fatalf("drained run eligible: %+v", p)
	}
}

func TestReadinessAcceptanceReleasesOnlySatisfiedDirectDependents(t *testing.T) {
	db := readinessDB(t)
	tx := readyTx(t, db)
	if err := installReadiness(tx); err != nil {
		t.Fatal(err)
	}
	s := readySubmission(t, tx, "run", map[string][]string{"join": {"left", "right"}, "later": {"join"}}, "left", "right", "join", "later", "independent")
	if err := registerReadiness(tx, s); err != nil {
		t.Fatal(err)
	}
	readyAccept(t, tx, "run", "left")
	p := readyPage(t, tx, 0, 10)
	if len(p) != 2 || p[0].PackageID != "pkg-right" || p[1].PackageID != "pkg-independent" {
		t.Fatalf("premature join: %+v", p)
	}
	readyAccept(t, tx, "run", "right")
	p = readyPage(t, tx, 0, 10)
	if len(p) != 2 || p[0].PackageID != "pkg-independent" || p[1].PackageID != "pkg-join" {
		t.Fatalf("release order: %+v", p)
	}
	cursor := p[1].Cursor
	// Defensive repeated notification cannot create another queue entry.
	if err := acceptReadiness(tx, "run", "right"); err != nil {
		t.Fatal(err)
	}
	if p = readyPage(t, tx, 0, 10); len(p) != 2 || p[1].Cursor != cursor {
		t.Fatalf("duplicate release: %+v", p)
	}
	readyAccept(t, tx, "run", "join")
	if p = readyPage(t, tx, 0, 10); len(p) != 2 || p[1].PackageID != "pkg-later" {
		t.Fatalf("transitive release: %+v", p)
	}
	// Corrupt historical JSON proves subsequent scheduling/install uses metadata,
	// not accepted manifests, candidate payloads or validator verdicts.
	readyExec(t, tx, `UPDATE packages SET manifest='INVALID HISTORY' WHERE id IN ('pkg-left','pkg-right','pkg-join')`)
	if err := installReadiness(tx); err != nil {
		t.Fatal(err)
	}
	if p = readyPage(t, tx, 0, 10); len(p) != 2 {
		t.Fatalf("historical rows affected scheduling: %+v", p)
	}
}

func TestReadinessCandidateStates(t *testing.T) {
	for _, state := range []string{"rejected", "pending", "validating", "operational_failed", "valid"} {
		for _, mixed := range []bool{false, true} {
			name := state + "/separate"
			if mixed {
				name = state + "/mixed"
			}
			t.Run(name, func(t *testing.T) {
				db := readinessDB(t)
				tx := readyTx(t, db)
				if err := installReadiness(tx); err != nil {
					t.Fatal(err)
				}
				s := readySubmission(t, tx, "run", nil, "candidate", "independent")
				if mixed {
					p := s.Packages[0]
					p.Units = append(p.Units, s.Packages[1].Units...)
					readyExec(t, tx, `DELETE FROM packages WHERE run_id='run'`)
					digest, _ := contracts.Digest(p)
					readyExec(t, tx, `INSERT INTO packages VALUES('run',?,?,?)`, p.PackageID, digest, encoded(p))
					s.Packages = []contracts.Package{p}
				}
				if err := registerReadiness(tx, s); err != nil {
					t.Fatal(err)
				}
				// Historical rejection must not hide another unresolved or valid result.
				for _, c := range []struct{ digest, state string }{{"old", "rejected"}, {"current", state}} {
					readyExec(t, tx, `INSERT INTO candidates(digest,run_id,unit_id,assignment_id,manifest,output_digest,state) VALUES(?,'run','candidate','run','{}','unused',?)`, c.digest, c.state)
				}
				page := readyPage(t, tx, 0, 10)
				seen := map[string]bool{}
				for _, p := range page {
					for _, r := range p.Ready {
						seen[r.UnitID] = true
					}
				}
				if !seen["independent"] || seen["candidate"] != (state == "rejected") {
					t.Fatalf("state=%s mixed=%v ready=%v", state, mixed, seen)
				}
				expected := 1
				if !mixed && state == "rejected" {
					expected = 2
				}
				if len(page) != expected {
					t.Fatalf("package selection state=%s mixed=%v: %+v", state, mixed, page)
				}
			})
		}
	}
}

func TestReadinessOrderedPackageSubset(t *testing.T) {
	db := readinessDB(t)
	tx := readyTx(t, db)
	if err := installReadiness(tx); err != nil {
		t.Fatal(err)
	}
	s := readySubmission(t, tx, "run", map[string][]string{"blocked": {"first"}}, "first", "blocked", "third")
	// A single immutable package can contain both ready and blocked members.
	p := s.Packages[0]
	p.Units = []contracts.UnitRef{s.Packages[2].Units[0], s.Packages[1].Units[0], s.Packages[0].Units[0]}
	readyExec(t, tx, `DELETE FROM packages WHERE run_id='run'`)
	digest, _ := contracts.Digest(p)
	readyExec(t, tx, `INSERT INTO packages VALUES('run',?,?,?)`, p.PackageID, digest, encoded(p))
	s.Packages = []contracts.Package{p}
	if err := registerReadiness(tx, s); err != nil {
		t.Fatal(err)
	}
	page := readyPage(t, tx, 0, 10)
	if len(page) != 1 || len(page[0].Ready) != 2 || page[0].Ready[0].UnitID != "third" || page[0].Ready[1].UnitID != "first" {
		t.Fatalf("ordered subset: %+v", page)
	}
	cursor := page[0].Cursor
	readyAccept(t, tx, "run", "first")
	page = readyPage(t, tx, 0, 10)
	if len(page) != 1 || page[0].Cursor != cursor || len(page[0].Ready) != 2 || page[0].Ready[0].UnitID != "third" || page[0].Ready[1].UnitID != "blocked" {
		t.Fatalf("released subset: %+v", page)
	}
}

func TestReadinessLegacyBootstrapOnceAcrossReopen(t *testing.T) {
	db := readinessDB(t)
	tx := readyTx(t, db)
	readySubmission(t, tx, "legacy", nil, "old", "new")
	readyExec(t, tx, `INSERT INTO candidates(digest,run_id,unit_id,assignment_id,manifest,output_digest,state) VALUES('accepted','legacy','old','legacy','INVALID PAYLOAD','unused','valid')`)
	readyExec(t, tx, `INSERT INTO acceptances VALUES('legacy','old','accepted','validator','INVALID VERDICT')`)
	if err := installReadiness(tx); err != nil {
		t.Fatal(err)
	}
	readyExec(t, tx, `UPDATE packages SET manifest='INVALID HISTORY' WHERE id='pkg-old'`)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var seq int
	var name, path string
	if err := db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	reopened.SetMaxOpenConns(1)
	t.Cleanup(func() { reopened.Close() })
	if _, err = reopened.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		t.Fatal(err)
	}
	tx = readyTx(t, reopened)
	if err := installReadiness(tx); err != nil {
		t.Fatal(err)
	}
	p := readyPage(t, tx, 0, 10)
	if len(p) != 1 || p[0].PackageID != "pkg-new" {
		t.Fatalf("reopened queue: %+v", p)
	}
	var members int
	if err := tx.QueryRow(`SELECT count(*) FROM readiness_members`).Scan(&members); err != nil || members != 2 {
		t.Fatalf("members=%d err=%v", members, err)
	}
}

func TestReadinessRejectsInvalidMembership(t *testing.T) {
	for _, kind := range []string{"cross-run", "spec", "duplicate", "missing"} {
		t.Run(kind, func(t *testing.T) {
			db := readinessDB(t)
			tx := readyTx(t, db)
			if err := installReadiness(tx); err != nil {
				t.Fatal(err)
			}
			s := readySubmission(t, tx, "run", nil, "one", "two")
			switch kind {
			case "cross-run":
				s.RunID = "other"
			case "spec":
				s.Packages[0].Units[0].SpecDigest = contracts.Hash([]byte("wrong"))
			case "duplicate":
				s.Packages[1].Units = s.Packages[0].Units
			case "missing":
				s.Packages = s.Packages[:1]
			}
			if err := registerReadiness(tx, s); err == nil {
				t.Fatal("invalid membership accepted")
			}
		})
	}
}

func TestReadinessMigrationRollbackAndIndexes(t *testing.T) {
	db := readinessDB(t)
	tx := readyTx(t, db)
	readySubmission(t, tx, "legacy", nil, "one")
	readyExec(t, tx, `UPDATE packages SET manifest='broken'`)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx = readyTx(t, db)
	if err := installReadiness(tx); err == nil {
		t.Fatal("malformed legacy metadata accepted")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx = readyTx(t, db)
	var n int
	if err := tx.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='readiness_migrations'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("migration not atomic: %d %v", n, err)
	}
	readyExec(t, tx, `DELETE FROM packages`)
	if err := installReadiness(tx); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct{ query, index string }{
		{`SELECT unit_id FROM dependencies WHERE run_id='run' AND prerequisite='one'`, "dependency_dependents"},
		{`SELECT 1 FROM candidates WHERE run_id='run' AND unit_id='one'`, "candidate_units"},
		{`SELECT seq FROM readiness_queue WHERE seq>10 ORDER BY seq LIMIT 2`, "INTEGER PRIMARY KEY"},
	} {
		rows, err := tx.Query("EXPLAIN QUERY PLAN " + check.query)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(detail)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(plan.String(), check.index) {
			t.Fatalf("index %s absent: %s", check.index, plan.String())
		}
	}
}
