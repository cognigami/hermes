package agent

import (
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"testing"
)

func TestIndexedActiveCannotHideSameAssignmentArchive(t *testing.T) {
	root := t.TempDir()
	cfg := hostTestConfig(root)
	h, e := openHost(cfg)
	if e != nil {
		t.Fatal(e)
	}
	work := testWork()
	if _, e = h.receive(work); e != nil {
		t.Fatal(e)
	}
	if e = h.confirmInventory(); e != nil {
		t.Fatal(e)
	}
	h.Close()
	key := contracts.Hash([]byte(work.Assignment.AssignmentID))
	archive := filepath.Join(root, "completed", key)
	if e = os.MkdirAll(archive, 0700); e != nil {
		t.Fatal(e)
	}
	if e = durable(filepath.Join(archive, "journal.json"), journal{Work: work, ProcessStarted: true}); e != nil {
		t.Fatal(e)
	}
	if q := CheckLocalQuiescence(root); q.State != "uncertain" {
		t.Fatal("active never-launched journal hid unknown started archive", q)
	}
	if reopened, e := openHost(cfg); e == nil {
		reopened.Close()
		t.Fatal("recovery trusted stale ledger under duplicate namespaces")
	}
}
func TestAtomicArchiveBeforeInventoryRemovalStillRecovers(t *testing.T) {
	root := t.TempDir()
	cfg := hostTestConfig(root)
	h, e := openHost(cfg)
	if e != nil {
		t.Fatal(e)
	}
	work := testWork()
	r, e := h.receive(work)
	if e != nil {
		t.Fatal(e)
	}
	r.j.Quiescent = true
	r.j.FinalSent = true
	if e = r.save(); e != nil {
		t.Fatal(e)
	}
	if e = h.confirmInventory(); e != nil {
		t.Fatal(e)
	}
	h.Close()
	key := contracts.Hash([]byte(work.Assignment.AssignmentID))
	if e = os.MkdirAll(filepath.Join(root, "completed"), 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(filepath.Join(root, "active", key), filepath.Join(root, "completed", key)); e != nil {
		t.Fatal(e)
	}
	if q := CheckLocalQuiescence(root); q.State != "quiescent" {
		t.Fatal("atomic archive crash window rejected", q)
	}
	reopened, e := openHost(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	if len(reopened.inventory.Entries) != 0 {
		t.Fatal("archived index entry not retired")
	}
}
