package controller

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"hermes/internal/contracts"
)

const readinessSchema = `
CREATE TABLE IF NOT EXISTS readiness_migrations(name TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS readiness_members(
 run_id TEXT NOT NULL, unit_id TEXT NOT NULL, package_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
 PRIMARY KEY(run_id,unit_id), UNIQUE(run_id,package_id,ordinal),
 FOREIGN KEY(run_id,unit_id) REFERENCES run_units(run_id,unit_id),
 FOREIGN KEY(run_id,package_id) REFERENCES packages(run_id,id));
CREATE TABLE IF NOT EXISTS readiness_queue(
 seq INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, package_id TEXT NOT NULL,
 UNIQUE(run_id,package_id), FOREIGN KEY(run_id,package_id) REFERENCES packages(run_id,id));
CREATE INDEX IF NOT EXISTS dependency_dependents ON dependencies(run_id,prerequisite,unit_id);
CREATE INDEX IF NOT EXISTS candidate_units ON candidates(run_id,unit_id);
`

// installReadiness bootstraps legacy metadata once, in the caller's transaction.
// Neither migration nor steady-state scheduling reads candidate/result payloads.
func installReadiness(tx *sql.Tx) error {
	if _, err := tx.Exec(readinessSchema); err != nil {
		return err
	}
	var done int
	if err := tx.QueryRow(`SELECT count(*) FROM readiness_migrations WHERE name='package-members-v1'`).Scan(&done); err != nil {
		return err
	}
	if done != 0 {
		return nil
	}
	rows, err := tx.Query(`SELECT run_id,id,manifest FROM packages ORDER BY rowid`)
	if err != nil {
		return err
	}
	type legacy struct {
		run, id  string
		manifest []byte
	}
	var old []legacy
	for rows.Next() {
		var p legacy
		if err = rows.Scan(&p.run, &p.id, &p.manifest); err != nil {
			rows.Close()
			return err
		}
		old = append(old, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, p := range old {
		var manifest contracts.Package
		if err := json.Unmarshal(p.manifest, &manifest); err != nil {
			return err
		}
		if manifest.PackageID != p.id {
			return errors.New("legacy package identity mismatch")
		}
		if err := registerReadyPackage(tx, p.run, manifest); err != nil {
			return err
		}
	}
	// Canonical acceptance is terminal even if an old ready flag remained set.
	if _, err = tx.Exec(`UPDATE run_units SET ready=0 WHERE EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id=run_units.run_id AND a.unit_id=run_units.unit_id)`); err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO readiness_migrations VALUES('package-members-v1')`)
	return err
}

// registerReadiness runs after the immutable graph and package rows are inserted.
func registerReadiness(tx *sql.Tx, s contracts.Submission) error {
	for _, p := range s.Packages {
		if err := registerReadyPackage(tx, s.RunID, p); err != nil {
			return err
		}
	}
	var missing int
	if err := tx.QueryRow(`SELECT count(*) FROM run_units u WHERE run_id=? AND NOT EXISTS(SELECT 1 FROM readiness_members m WHERE m.run_id=u.run_id AND m.unit_id=u.unit_id)`, s.RunID).Scan(&missing); err != nil {
		return err
	}
	if missing != 0 {
		return errors.New("units missing readiness membership")
	}
	return nil
}

func registerReadyPackage(tx *sql.Tx, run string, p contracts.Package) error {
	if err := p.Validate(); err != nil {
		return err
	}
	for i, ref := range p.Units {
		var spec string
		if err := tx.QueryRow(`SELECT d.spec FROM run_units u JOIN definitions d ON d.unit_id=u.unit_id WHERE u.run_id=? AND u.unit_id=?`, run, ref.UnitID).Scan(&spec); err != nil {
			return err
		}
		if spec != ref.SpecDigest {
			return errors.New("readiness membership spec mismatch")
		}
		if _, err := tx.Exec(`INSERT INTO readiness_members VALUES(?,?,?,?)`, run, ref.UnitID, p.PackageID, i); err != nil {
			return err
		}
	}
	return enqueueReadyPackage(tx, run, p.PackageID)
}

func enqueueReadyPackage(tx *sql.Tx, run, pkg string) error {
	_, err := tx.Exec(`INSERT INTO readiness_queue(run_id,package_id)
 SELECT ?,? WHERE EXISTS(SELECT 1 FROM readiness_members m JOIN run_units u ON u.run_id=m.run_id AND u.unit_id=m.unit_id
 WHERE m.run_id=? AND m.package_id=? AND u.ready=1 AND NOT EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id=u.run_id AND a.unit_id=u.unit_id))
 ON CONFLICT(run_id,package_id) DO NOTHING`, run, pkg, run, pkg)
	return err
}

// acceptReadiness is called only for the first canonical acceptance. The reverse
// edge index bounds release work to direct dependents, never the run's history.
func acceptReadiness(tx *sql.Tx, run, unit string) error {
	if _, err := tx.Exec(`UPDATE run_units SET ready=0 WHERE run_id=? AND unit_id=?`, run, unit); err != nil {
		return err
	}
	rows, err := tx.Query(`SELECT d.unit_id,m.package_id FROM dependencies d JOIN readiness_members m ON m.run_id=d.run_id AND m.unit_id=d.unit_id
 WHERE d.run_id=? AND d.prerequisite=? AND NOT EXISTS(
 SELECT 1 FROM dependencies pending WHERE pending.run_id=d.run_id AND pending.unit_id=d.unit_id
 AND NOT EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id=pending.run_id AND a.unit_id=pending.prerequisite))
 AND NOT EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id=d.run_id AND a.unit_id=d.unit_id)
 ORDER BY m.ordinal,d.unit_id`, run, unit)
	if err != nil {
		return err
	}
	type dependent struct{ unit, pkg string }
	var released []dependent
	for rows.Next() {
		var d dependent
		if err = rows.Scan(&d.unit, &d.pkg); err != nil {
			rows.Close()
			return err
		}
		released = append(released, d)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, d := range released {
		if _, err = tx.Exec(`UPDATE run_units SET ready=1 WHERE run_id=? AND unit_id=?`, run, d.unit); err != nil {
			return err
		}
		if err = enqueueReadyPackage(tx, run, d.pkg); err != nil {
			return err
		}
	}
	_, err = tx.Exec(`DELETE FROM readiness_queue WHERE run_id=? AND package_id=(SELECT package_id FROM readiness_members WHERE run_id=? AND unit_id=?)
 AND NOT EXISTS(SELECT 1 FROM readiness_members m JOIN run_units u ON u.run_id=m.run_id AND u.unit_id=m.unit_id WHERE m.run_id=readiness_queue.run_id AND m.package_id=readiness_queue.package_id AND u.ready=1)`, run, run, unit)
	return err
}

type readyPackage struct {
	RunID, PackageID, Digest string
	Package                  contracts.Package
	Ready                    []contracts.UnitRef
	Cursor                   int64
}

const eligibleReadyUnits = ` FROM readiness_members m JOIN run_units u ON u.run_id=m.run_id AND u.unit_id=m.unit_id
 WHERE m.run_id=q.run_id AND m.package_id=q.package_id AND u.ready=1
 AND NOT EXISTS(SELECT 1 FROM owners o WHERE o.run_id=u.run_id AND o.unit_id=u.unit_id)
 AND NOT EXISTS(SELECT 1 FROM candidates c WHERE c.run_id=u.run_id AND c.unit_id=u.unit_id AND c.state!='rejected')
 AND NOT EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id=u.run_id AND a.unit_id=u.unit_id)`

// readyPackages returns bounded keyset pages. Admission belongs to the caller:
// continue after the last Cursor when an earlier package does not fit capacity.
// Rejected candidates do not block this queue; the caller must still authorize
// any retry. Unresolved or valid retained candidates always block recomputation.
func readyPackages(tx *sql.Tx, afterSeq int64, limit int) ([]readyPackage, error) {
	if limit < 1 || limit > contracts.MaxUnits {
		return nil, fmt.Errorf("invalid readiness page size: %d", limit)
	}
	rows, err := tx.Query(`SELECT q.run_id,q.package_id,q.seq FROM readiness_queue q WHERE q.seq>?
 AND NOT EXISTS(SELECT 1 FROM target_controls t WHERE t.kind='run' AND t.target=q.run_id AND t.action='drain')
 AND EXISTS(SELECT 1`+eligibleReadyUnits+`) ORDER BY q.seq LIMIT ?`, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	var result []readyPackage
	for rows.Next() {
		var p readyPackage
		if err = rows.Scan(&p.RunID, &p.PackageID, &p.Cursor); err != nil {
			rows.Close()
			return nil, err
		}
		result = append(result, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for i := range result {
		p := &result[i]
		var manifest []byte
		if err = tx.QueryRow(`SELECT digest,manifest FROM packages WHERE run_id=? AND id=?`, p.RunID, p.PackageID).Scan(&p.Digest, &manifest); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(manifest, &p.Package); err != nil {
			return nil, err
		}
		refs, err := tx.Query(`SELECT u.unit_id FROM readiness_queue q, readiness_members m JOIN run_units u ON u.run_id=m.run_id AND u.unit_id=m.unit_id
 WHERE q.run_id=? AND q.package_id=? AND m.run_id=q.run_id AND m.package_id=q.package_id AND u.ready=1
 AND NOT EXISTS(SELECT 1 FROM owners o WHERE o.run_id=u.run_id AND o.unit_id=u.unit_id)
 AND NOT EXISTS(SELECT 1 FROM candidates c WHERE c.run_id=u.run_id AND c.unit_id=u.unit_id AND c.state!='rejected')
 AND NOT EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id=u.run_id AND a.unit_id=u.unit_id) ORDER BY m.ordinal`, p.RunID, p.PackageID)
		if err != nil {
			return nil, err
		}
		eligible := map[string]bool{}
		for refs.Next() {
			var id string
			if err = refs.Scan(&id); err != nil {
				refs.Close()
				return nil, err
			}
			eligible[id] = true
		}
		err = refs.Err()
		refs.Close()
		if err != nil {
			return nil, err
		}
		for _, ref := range p.Package.Units {
			if eligible[ref.UnitID] {
				p.Ready = append(p.Ready, ref)
			}
		}
	}
	return result, nil
}
