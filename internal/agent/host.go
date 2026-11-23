package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const maxActiveJournals = 64

type activeEntry struct {
	AssignmentID string `json:"assignment_id"`
	Digest       string `json:"digest"`
}
type hostInventory struct {
	Revision      int64                  `json:"revision,omitempty"`
	Reconciled    bool                   `json:"reconciled"`
	SchemaVersion int                    `json:"schema_version"`
	Entries       []activeEntry          `json:"entries"`
	Pending       *contracts.PollRequest `json:"pending_poll,omitempty"`
}
type host struct {
	cfg       Config
	lock      *os.File
	store     *artifacts.Store
	spool     *spool
	admission *admission
	mu        sync.Mutex
	inventory hostInventory
	active    map[string]*runner
}

func openHost(cfg Config) (*host, error) {
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 1
	}
	if cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > maxActiveJournals || cfg.WorkerID == "" || cfg.SessionID == "" || cfg.MaxArtifactBytes <= 0 {
		return nil, errors.New("invalid local host configuration")
	}
	root, e := filepath.Abs(cfg.Root)
	if e != nil {
		return nil, e
	}
	cfg.Root = root
	if e = os.MkdirAll(root, 0700); e != nil {
		return nil, e
	}
	lock, e := os.OpenFile(filepath.Join(root, "agent.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		lock.Close()
		return nil, errors.New("agent host authority busy")
	}
	h := &host{cfg: cfg, lock: lock, active: map[string]*runner{}}
	success := false
	defer func() {
		if !success {
			h.Close()
		}
	}()
	identity := struct{ Worker, Session string }{cfg.WorkerID, cfg.SessionID}
	identityPath := filepath.Join(root, "identity.json")
	b, e := os.ReadFile(identityPath)
	fresh := errors.Is(e, os.ErrNotExist)
	if e == nil {
		var prior struct{ Worker, Session string }
		if e = json.Unmarshal(b, &prior); e != nil {
			return nil, e
		}
		if prior != identity {
			return nil, errors.New("persisted worker/session identity mismatch")
		}
	} else if !fresh {
		return nil, e
	}
	inventoryPath := filepath.Join(root, "active.json")
	legacyPath := filepath.Join(root, "journal.json")
	legacy, legacyErr := os.ReadFile(legacyPath)
	if legacyErr != nil && !errors.Is(legacyErr, os.ErrNotExist) {
		return nil, errors.New("legacy execution metadata unreadable")
	}
	if fresh {
		for _, name := range []string{"active.json", "journal.json", "active", "completed"} {
			if _, err := os.Lstat(filepath.Join(root, name)); err == nil {
				return nil, errors.New("execution metadata exists without host identity")
			} else if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
		if e = durable(identityPath, identity); e != nil {
			return nil, e
		}
	}
	for _, name := range []string{"active", "completed"} {
		if e = os.MkdirAll(filepath.Join(root, name), 0700); e != nil {
			return nil, e
		}
	}
	b, e = os.ReadFile(inventoryPath)
	if e == nil {
		if e = contracts.Decode(bytes.NewReader(b), &h.inventory); e != nil {
			return nil, e
		}
		if legacyErr == nil {
			if e = h.validateLegacyLayout(legacy, true); e != nil {
				return nil, e
			}
		}
	} else if errors.Is(e, os.ErrNotExist) {
		h.inventory = hostInventory{SchemaVersion: 1, Reconciled: !fresh && legacyErr == nil}
		if !fresh {
			if legacyErr != nil {
				return nil, errors.New("host active inventory missing; cannot infer quiescence")
			}
			if e = h.validateLegacyLayout(legacy, false); e != nil {
				return nil, e
			}
			var j journal
			if e = json.Unmarshal(legacy, &j); e != nil {
				return nil, e
			}
			if e = validateWork(j.Work); e != nil || j.Work.Assignment == nil {
				return nil, errors.New("invalid legacy journal")
			}
			d, _ := contracts.Digest(*j.Work.Assignment)
			h.inventory.Entries = []activeEntry{{j.Work.Assignment.AssignmentID, d}}
		}
		if e = h.save(); e != nil {
			return nil, e
		}
	} else {
		return nil, e
	}
	if h.inventory.SchemaVersion != 1 || len(h.inventory.Entries) > maxActiveJournals {
		return nil, errors.New("invalid active inventory")
	}
	h.store, e = artifacts.New(filepath.Join(root, "blobs"), cfg.MaxArtifactBytes)
	if e != nil {
		return nil, e
	}
	seen := map[string]bool{}
	for _, entry := range h.inventory.Entries {
		if entry.AssignmentID == "" || seen[entry.AssignmentID] {
			return nil, errors.New("invalid duplicate active identity")
		}
		seen[entry.AssignmentID] = true
		if e = validateExecutionNamespace(cfg.Root, contracts.Hash([]byte(entry.AssignmentID))); e != nil {
			return nil, e
		}
		dir := h.assignmentRoot(entry.AssignmentID)
		path := filepath.Join(dir, "journal.json")
		b, e = os.ReadFile(path)
		if errors.Is(e, os.ErrNotExist) && legacyErr == nil {
			var j journal
			if err := json.Unmarshal(legacy, &j); err != nil {
				return nil, err
			}
			if j.Work.Assignment != nil && j.Work.Assignment.AssignmentID == entry.AssignmentID {
				if e = os.MkdirAll(dir, 0700); e != nil {
					return nil, e
				}
				if e = os.Rename(legacyPath, path); e != nil {
					return nil, e
				}
				if e = syncDirectory(filepath.Join(root, "active")); e != nil {
					return nil, e
				}
				if e = syncDirectory(root); e != nil {
					return nil, e
				}
				if e = syncDirectory(dir); e != nil {
					return nil, e
				}
				b = legacy
				e = nil
				legacyErr = os.ErrNotExist
			}
		}
		if errors.Is(e, os.ErrNotExist) {
			archived := filepath.Join(h.completedRoot(entry.AssignmentID), "journal.json")
			b, e = os.ReadFile(archived)
			if e == nil {
				var j journal
				if e = json.Unmarshal(b, &j); e != nil {
					return nil, e
				}
				if e = h.validateEntry(entry, j); e != nil {
					return nil, e
				}
				if !j.FinalSent || !j.Quiescent || j.Uncertain {
					return nil, errors.New("archive lacks terminal acknowledgment")
				}
				continue
			}
		}
		if e != nil {
			return nil, fmt.Errorf("missing/incomplete retained journal: %s: %w", entry.AssignmentID, e)
		}
		var j journal
		if e = json.Unmarshal(b, &j); e != nil {
			return nil, e
		}
		if e = h.validateEntry(entry, j); e != nil {
			return nil, e
		}
		local := cfg
		local.Root = dir
		h.active[entry.AssignmentID] = &runner{cfg: local, j: j, store: h.store}
	}
	directories, e := os.ReadDir(filepath.Join(root, "active"))
	if e != nil {
		return nil, e
	}
	if len(directories) > maxActiveJournals {
		return nil, errors.New("too many active journal directories")
	}
	for _, d := range directories {
		known := false
		for _, entry := range h.inventory.Entries {
			if d.Name() == contracts.Hash([]byte(entry.AssignmentID)) {
				known = true
			}
		}
		if !d.IsDir() || !known {
			return nil, errors.New("unindexed active journal; execution uncertainty")
		}
	}
	// An indexed completed archive is a crash during retirement, not missing work.
	var retained []activeEntry
	for _, entry := range h.inventory.Entries {
		if h.active[entry.AssignmentID] != nil {
			retained = append(retained, entry)
		}
	}
	if len(retained) != len(h.inventory.Entries) {
		h.inventory.Entries = retained
		if e = h.save(); e != nil {
			return nil, e
		}
	}
	success = true
	return h, nil
}
func (h *host) validateEntry(entry activeEntry, j journal) error {
	if e := validateWork(j.Work); e != nil {
		return e
	}
	a := j.Work.Assignment
	if a == nil {
		return errors.New("active journal has no assignment")
	}
	d, _ := contracts.Digest(*a)
	if a.AssignmentID != entry.AssignmentID || d != entry.Digest || a.WorkerID != h.cfg.WorkerID || a.SessionID != h.cfg.SessionID {
		return errors.New("retained journal identity/digest mismatch")
	}
	if j.FinalSent && (!j.Quiescent || j.Uncertain) {
		return errors.New("contradictory terminal journal")
	}
	return nil
}
func (h *host) save() error { return durable(filepath.Join(h.cfg.Root, "active.json"), h.inventory) }
func (h *host) assignmentRoot(id string) string {
	return filepath.Join(h.cfg.Root, "active", contracts.Hash([]byte(id)))
}
func (h *host) completedRoot(id string) string {
	return filepath.Join(h.cfg.Root, "completed", contracts.Hash([]byte(id)))
}
func syncDirectory(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func (h *host) receive(w contracts.Work) (*runner, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := validateWork(w); e != nil {
		return nil, e
	}
	a := w.Assignment
	if a == nil {
		return nil, errors.New("receipt requires assignment")
	}
	if a.WorkerID != h.cfg.WorkerID || a.SessionID != h.cfg.SessionID || a.Runtime != h.cfg.Runtime || (a.Profile != h.cfg.Profile && a.Allocation.Resources == (contracts.ResourceVector{})) {
		return nil, errors.New("incompatible assignment")
	}
	if h.admission != nil && a.Allocation.Resources != (contracts.ResourceVector{}) {
		if e := validateWorkAllocation(w); e != nil {
			return nil, e
		}
		if e := h.admission.update(*w.Budget); e != nil {
			return nil, e
		}
	}
	d, _ := contracts.Digest(*a)
	if r := h.active[a.AssignmentID]; r != nil {
		current, _ := contracts.Digest(*r.j.Work.Assignment)
		if current != d {
			return nil, errors.New("assignment ID reused with different meaning")
		}
		h.inventory.Pending = nil
		if e := h.save(); e != nil {
			return nil, e
		}
		return r, nil
	}
	if _, e := os.Stat(h.completedRoot(a.AssignmentID)); e == nil {
		return nil, errors.New("terminal assignment must not execute again")
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	if len(h.active) >= maxActiveJournals {
		return nil, errors.New("active journal limit")
	}
	h.inventory.Entries = append(h.inventory.Entries, activeEntry{a.AssignmentID, d})
	h.inventory.Revision++
	if e := h.save(); e != nil {
		return nil, e
	}
	dir := h.assignmentRoot(a.AssignmentID)
	if e := os.Mkdir(dir, 0700); e != nil {
		return nil, e
	}
	if e := syncDirectory(filepath.Dir(dir)); e != nil {
		return nil, e
	}
	local := h.cfg
	local.Root = dir
	r := &runner{cfg: local, j: journal{Work: w, ReceiptWall: time.Now().UTC()}, store: h.store, spool: h.spool, admission: h.admission}
	if e := r.save(); e != nil {
		return nil, e
	}
	h.active[a.AssignmentID] = r
	if h.admission != nil {
		if e := h.admission.register(r.j); e != nil {
			return nil, e
		}
	}
	h.inventory.Pending = nil
	h.inventory.Reconciled = true
	if e := h.save(); e != nil {
		return nil, e
	}
	return r, nil
}
func (h *host) retire(r *runner) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	r.mu.Lock()
	valid := r.j.FinalSent && r.j.Quiescent && !r.j.Uncertain
	id := r.j.Work.Assignment.AssignmentID
	r.mu.Unlock()
	if !valid {
		return errors.New("cannot retire unresolved execution")
	}
	if e := r.trackWorktree(); e != nil {
		return e
	}
	if e := os.Rename(h.assignmentRoot(id), h.completedRoot(id)); e != nil {
		return e
	}
	if e := syncDirectory(filepath.Join(h.cfg.Root, "active")); e != nil {
		return e
	}
	if e := syncDirectory(filepath.Join(h.cfg.Root, "completed")); e != nil {
		return e
	}
	var entries []activeEntry
	for _, entry := range h.inventory.Entries {
		if entry.AssignmentID != id {
			entries = append(entries, entry)
		}
	}
	h.inventory.Entries = entries
	h.inventory.Revision++
	if e := h.save(); e != nil {
		return e
	}
	delete(h.active, id)
	if h.admission != nil {
		h.admission.remove(id)
	}
	return nil
}
func (h *host) Close() error {
	var ledgerError error
	if h.spool != nil {
		ledgerError = h.spool.Close()
	}
	if h.lock != nil {
		e := h.lock.Close()
		if ledgerError != nil {
			return ledgerError
		}
		return e
	}
	return ledgerError
}
