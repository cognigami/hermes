package controller

import (
	"database/sql"
	"errors"
	"hermes/internal/contracts"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// One metadata-only migration covers pre-ledger canonical orphans and private
// staging/runtime copies. Ordinary recovery/admission thereafter uses the index.
func (c *Controller) bootstrapControllerStorage(tx *sql.Tx) error {
	var initialized int
	if e := tx.QueryRow("SELECT count(*) FROM controller_storage_init").Scan(&initialized); e != nil {
		return e
	}
	if initialized > 0 {
		return nil
	}
	dir, e := os.Open(filepath.Join(c.cfg.Root, "artifacts"))
	if e != nil {
		return e
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(128)
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".ingress-") {
				return errors.New("abandoned ingress escaped artifact-store initialization")
			}
			info, e := entry.Info()
			if e != nil {
				return e
			}
			ref := contracts.Artifact{Digest: entry.Name(), Size: info.Size(), MediaType: "application/octet-stream"}
			if !info.Mode().IsRegular() || ref.Validate() != nil {
				return errors.New("unknown artifact namespace during resource bootstrap")
			}
			if _, e = tx.Exec("INSERT OR IGNORE INTO controller_ingress SELECT ?,? WHERE NOT EXISTS(SELECT 1 FROM artifacts WHERE digest=?)", ref.Digest, ref.Size, ref.Digest); e != nil {
				return e
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	roots, e := os.ReadDir(c.cfg.Root)
	if e != nil {
		return e
	}
	for _, entry := range roots {
		name := entry.Name()
		if !strings.HasPrefix(name, "validation-") && !strings.HasPrefix(name, "workflow-runtime-") && !strings.HasPrefix(name, "workflow-decision-") {
			continue
		}
		if !entry.IsDir() {
			return errors.New("unexpected controller scratch namespace")
		}
		var total int64
		count := 0
		e = filepath.WalkDir(filepath.Join(c.cfg.Root, name), func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			count++
			if count > 65536 {
				return errors.New("legacy controller scratch metadata bound exceeded")
			}
			if d.IsDir() {
				return nil
			}
			info, e := d.Info()
			if e != nil {
				return e
			}
			if !info.Mode().IsRegular() {
				return errors.New("unknown retained scratch entry")
			}
			v, e := (contracts.ResourceVector{SpoolBytes: total}).Add(contracts.ResourceVector{SpoolBytes: info.Size()})
			if e == nil {
				total = v.SpoolBytes
			}
			return e
		})
		if e != nil {
			return e
		}
		if _, e = tx.Exec("INSERT INTO controller_storage(path,size) VALUES(?,?)", name, total); e != nil {
			return e
		}
	}
	_, e = tx.Exec("INSERT INTO controller_storage_init VALUES(1)")
	return e
}
