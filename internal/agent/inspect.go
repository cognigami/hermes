package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"golang.org/x/sys/unix"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
)

type LocalAssignmentInspection struct {
	AssignmentID string `json:"assignment_id"`
	State        string `json:"state"`
	Reason       string `json:"reason,omitempty"`
}
type LocalQuiescence struct {
	SchemaVersion   int                         `json:"schema_version"`
	State           string                      `json:"state"`
	Reason          string                      `json:"reason,omitempty"`
	PendingDelivery int                         `json:"pending_delivery"`
	Assignments     []LocalAssignmentInspection `json:"assignments,omitempty"`
}

// CheckLocalQuiescence is a read-only snapshot under the host lock. Callers must
// disable managed starts first and serialize lifecycle operations. Its released
// lock is not exclusion against a later concurrent manual start or uninstall.
func CheckLocalQuiescence(root string) LocalQuiescence {
	result := LocalQuiescence{SchemaVersion: 1, State: "uncertain"}
	fail := func(reason string) LocalQuiescence { result.State = "uncertain"; result.Reason = reason; return result }
	if !filepath.IsAbs(root) {
		return fail("absolute state root required")
	}
	lock, e := os.Open(filepath.Join(root, "agent.lock"))
	if e != nil {
		return fail("host lock metadata unavailable")
	}
	defer lock.Close()
	if e = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		if errors.Is(e, unix.EWOULDBLOCK) || errors.Is(e, unix.EAGAIN) {
			result.State = "busy"
			result.Reason = "host authority is active"
			return result
		}
		return fail("host lock cannot establish inspection authority")
	}
	var identity struct{ Worker, Session string }
	b, e := os.ReadFile(filepath.Join(root, "identity.json"))
	if e != nil || json.Unmarshal(b, &identity) != nil || identity.Worker == "" || identity.Session == "" {
		return fail("host identity missing or invalid")
	}
	inspect := func(j journal, expected *activeEntry) error {
		if e := validateWork(j.Work); e != nil {
			return e
		}
		a := j.Work.Assignment
		if a == nil || a.WorkerID != identity.Worker || a.SessionID != identity.Session {
			return errors.New("journal identity mismatch")
		}
		if expected != nil {
			digest, _ := contracts.Digest(*a)
			if a.AssignmentID != expected.AssignmentID || digest != expected.Digest {
				return errors.New("journal digest mismatch")
			}
		}
		summary := LocalAssignmentInspection{AssignmentID: a.AssignmentID, State: "quiescent"}
		if j.Delivered < 0 || j.Delivered > len(j.Candidates) || len(j.Started) > len(a.Units) || (!j.ProcessStarted && len(j.Started) > 0) || (j.FinalSent && (!j.Quiescent || j.Uncertain)) {
			return errors.New("inconsistent journal progress")
		}
		if j.Uncertain || (j.ProcessStarted && !j.Quiescent) {
			summary.State = "uncertain"
			summary.Reason = "process exit is not proven"
			result.Assignments = append(result.Assignments, summary)
			return errors.New(summary.Reason)
		}
		if !j.ProcessStarted {
			summary.State = "never_launched"
		}
		result.Assignments = append(result.Assignments, summary)
		result.PendingDelivery += len(j.Candidates) - j.Delivered
		if !j.FinalSent {
			result.PendingDelivery++
		}
		return nil
	}
	b, e = os.ReadFile(filepath.Join(root, "active.json"))
	if errors.Is(e, os.ErrNotExist) {
		active, err := os.ReadDir(filepath.Join(root, "active"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fail("active execution metadata unreadable")
		}
		if len(active) > 0 {
			return fail("legacy state has unindexed active execution metadata")
		}
		b, e = os.ReadFile(filepath.Join(root, "journal.json"))
		if e != nil {
			return fail("retained journal/inventory missing")
		}
		var j journal
		if e = json.Unmarshal(b, &j); e != nil {
			return fail("invalid legacy journal")
		}
		if e = inspect(j, nil); e != nil {
			return fail(e.Error())
		}
		result.State = "quiescent"
		return result
	}
	if e != nil {
		return fail("active inventory unavailable")
	}
	if _, err := os.Lstat(filepath.Join(root, "journal.json")); err == nil {
		return fail("mixed modern inventory and unindexed legacy execution metadata")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fail("legacy execution metadata unreadable")
	}
	var inventory hostInventory
	if e = contracts.Decode(bytes.NewReader(b), &inventory); e != nil || inventory.SchemaVersion != 1 || len(inventory.Entries) > maxActiveJournals {
		return fail("invalid active inventory")
	}
	if !inventory.Reconciled {
		return fail("host inventory has not established reconciliation")
	}
	known := map[string]bool{}
	for _, entry := range inventory.Entries {
		hash := contracts.Hash([]byte(entry.AssignmentID))
		if entry.AssignmentID == "" || known[hash] {
			return fail("duplicate/invalid active identity")
		}
		known[hash] = true
		if err := validateExecutionNamespace(root, hash); err != nil {
			return fail(err.Error())
		}
		b, e = os.ReadFile(filepath.Join(root, "active", hash, "journal.json"))
		archived := false
		if errors.Is(e, os.ErrNotExist) {
			b, e = os.ReadFile(filepath.Join(root, "completed", hash, "journal.json"))
			archived = e == nil
		}
		if e != nil {
			return fail("retained assignment journal missing")
		}
		var j journal
		if e = json.Unmarshal(b, &j); e != nil {
			return fail("retained assignment journal malformed")
		}
		if archived && !j.FinalSent {
			return fail("archive lacks terminal acknowledgment")
		}
		if e = inspect(j, &entry); e != nil {
			return fail(e.Error())
		}
	}
	entries, e := os.ReadDir(filepath.Join(root, "active"))
	if e != nil || len(entries) > maxActiveJournals {
		return fail("active journal directory missing or excessive")
	}
	for _, entry := range entries {
		if !entry.IsDir() || !known[entry.Name()] {
			return fail("unindexed active journal")
		}
	}
	result.State = "quiescent"
	return result
}
