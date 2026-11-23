package agent

import (
	"os"
	"path/filepath"
	"testing"
)

func hostTestConfig(root string) Config {
	return Config{Root: root, WorkerID: "worker", SessionID: "session", Runtime: "synthetic-v1", Profile: "synthetic-local", MaxArtifactBytes: 1 << 20}
}
func TestLocalInspectionIsReadOnlyAndConservative(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent")
	if q := CheckLocalQuiescence(missing); q.State != "uncertain" {
		t.Fatal(q)
	}
	if _, e := os.Stat(missing); !os.IsNotExist(e) {
		t.Fatal("inspection created missing root", e)
	}
	root := t.TempDir()
	h, e := openHost(hostTestConfig(root))
	if e != nil {
		t.Fatal(e)
	}
	if q := CheckLocalQuiescence(root); q.State != "busy" {
		t.Fatal(q)
	}
	r, e := h.receive(testWork())
	if e != nil {
		t.Fatal(e)
	}
	h.Close()
	if q := CheckLocalQuiescence(root); q.State != "quiescent" || q.PendingDelivery != 1 {
		t.Fatal("never-launched receipt lost positive proof or pending inventory", q)
	}
	r.j.ProcessStarted = true
	if e = r.save(); e != nil {
		t.Fatal(e)
	}
	if q := CheckLocalQuiescence(root); q.State != "uncertain" {
		t.Fatal("start marker inferred exit", q)
	}
	r.j.Quiescent = true
	if e = r.save(); e != nil {
		t.Fatal(e)
	}
	if q := CheckLocalQuiescence(root); q.State != "quiescent" || q.PendingDelivery != 1 {
		t.Fatal("pending final prevented known local quiescence", q)
	}
	if e = os.Remove(filepath.Join(r.cfg.Root, "journal.json")); e != nil {
		t.Fatal(e)
	}
	if q := CheckLocalQuiescence(root); q.State != "uncertain" {
		t.Fatal("missing journal inferred exit", q)
	}
}
func TestFreshUnreconciledInventoryIsNotExitProof(t *testing.T) {
	root := t.TempDir()
	h, e := openHost(hostTestConfig(root))
	if e != nil {
		t.Fatal(e)
	}
	h.Close()
	if q := CheckLocalQuiescence(root); q.State != "uncertain" {
		t.Fatal("fresh state could hide lost prior journals", q)
	}
	h, e = openHost(hostTestConfig(root))
	if e != nil {
		t.Fatal(e)
	}
	if e = h.confirmInventory(); e != nil {
		t.Fatal(e)
	}
	h.Close()
	if q := CheckLocalQuiescence(root); q.State != "quiescent" {
		t.Fatal("known empty reconciliation did not establish safe empty host", q)
	}
}

func TestLocalInspectionDoesNotReadTerminalHistory(t *testing.T) {
	root := t.TempDir()
	h, e := openHost(hostTestConfig(root))
	if e != nil {
		t.Fatal(e)
	}
	if e = h.confirmInventory(); e != nil {
		t.Fatal(e)
	}
	h.Close()
	if e = os.WriteFile(filepath.Join(root, "completed", "historical-invalid-bytes"), []byte("not JSON"), 0600); e != nil {
		t.Fatal(e)
	}
	if q := CheckLocalQuiescence(root); q.State != "quiescent" {
		t.Fatal("historical archives were interpreted as active execution", q)
	}
}
