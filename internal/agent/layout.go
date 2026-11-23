package agent

import (
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
)

// The only supported mixed layout is the explicit interrupted migration:
// catalogue intent for exactly this legacy identity, then an empty target dir,
// then an atomic move of the legacy journal. Anything else is ambiguous.
// Reject exact indexed aliases without scanning terminal history. Normal
// retirement atomically renames the directory, so only one name may exist.
func validateExecutionNamespace(root, key string) error {
	_, active := os.Lstat(filepath.Join(root, "active", key))
	_, completed := os.Lstat(filepath.Join(root, "completed", key))
	if active != nil && !errors.Is(active, os.ErrNotExist) {
		return active
	}
	if completed != nil && !errors.Is(completed, os.ErrNotExist) {
		return completed
	}
	if active == nil && completed == nil {
		return errors.New("indexed execution has both active and completed namespaces")
	}
	return nil
}

func (h *host) validateLegacyLayout(data []byte, hasInventory bool) error {
	var j journal
	if e := json.Unmarshal(data, &j); e != nil {
		return e
	}
	if e := validateWork(j.Work); e != nil {
		return e
	}
	a := j.Work.Assignment
	if a == nil {
		return errors.New("legacy journal has no assignment")
	}
	digest, _ := contracts.Digest(*a)
	entry := activeEntry{a.AssignmentID, digest}
	if e := h.validateEntry(entry, j); e != nil {
		return e
	}
	entries, e := os.ReadDir(filepath.Join(h.cfg.Root, "active"))
	if e != nil {
		return e
	}
	if !hasInventory {
		if len(entries) != 0 {
			return errors.New("legacy state has unindexed active execution metadata")
		}
		return nil
	}
	if len(h.inventory.Entries) != 1 || h.inventory.Entries[0] != entry {
		return errors.New("legacy journal is not exact indexed migration intent")
	}
	hash := contracts.Hash([]byte(a.AssignmentID))
	for _, d := range entries {
		if !d.IsDir() || d.Name() != hash {
			return errors.New("unknown active execution beside legacy migration")
		}
		contents, e := os.ReadDir(filepath.Join(h.cfg.Root, "active", d.Name()))
		if e != nil {
			return e
		}
		if len(contents) != 0 {
			return errors.New("duplicate/nonempty active execution beside legacy journal")
		}
	}
	return nil
}
