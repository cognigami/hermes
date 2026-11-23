package agent

import (
	"bytes"
	"encoding/json"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"testing"
)

func legacyRoot(t *testing.T, journalBytes []byte) string {
	t.Helper()
	root := t.TempDir()
	if e := os.WriteFile(filepath.Join(root, "agent.lock"), nil, 0600); e != nil {
		t.Fatal(e)
	}
	if e := durable(filepath.Join(root, "identity.json"), struct{ Worker, Session string }{"worker", "session"}); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(root, "journal.json"), journalBytes, 0600); e != nil {
		t.Fatal(e)
	}
	return root
}
func TestPersistedLifecycleProofPresenceSharedByRecoveryAndInspection(t *testing.T) {
	valid := journal{Work: testWork()}
	base, e := json.Marshal(valid)
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"process_started", "quiescent", "uncertain", "final_sent"} {
		for _, mode := range []string{"missing", "null"} {
			t.Run(key+"-"+mode, func(t *testing.T) {
				var fields map[string]json.RawMessage
				if e := json.Unmarshal(base, &fields); e != nil {
					t.Fatal(e)
				}
				if mode == "missing" {
					delete(fields, key)
				} else {
					fields[key] = json.RawMessage("null")
				}
				data, e := json.Marshal(fields)
				if e != nil {
					t.Fatal(e)
				}
				root := legacyRoot(t, data)
				if q := CheckLocalQuiescence(root); q.State != "uncertain" {
					t.Fatal("inspection backfilled lifecycle proof", q)
				}
				if h, e := openHost(hostTestConfig(root)); e == nil {
					h.Close()
					t.Fatal("recovery backfilled lifecycle proof")
				}
				after, e := os.ReadFile(filepath.Join(root, "journal.json"))
				if e != nil || !bytes.Equal(after, data) {
					t.Fatal("incomplete proof was rewritten", e)
				}
			})
		}
	}
	onlyWork, _ := json.Marshal(map[string]any{"work": testWork()})
	root := legacyRoot(t, onlyWork)
	if q := CheckLocalQuiescence(root); q.State != "uncertain" {
		t.Fatal("Work-only object proved never launched", q)
	}
	if h, e := openHost(hostTestConfig(root)); e == nil {
		h.Close()
		t.Fatal("Work-only journal recovered execution")
	}
	positive := legacyRoot(t, base)
	if q := CheckLocalQuiescence(positive); q.State != "quiescent" {
		t.Fatal("explicit never-launched proof rejected", q)
	}
	h, e := openHost(hostTestConfig(positive))
	if e != nil {
		t.Fatal(e)
	}
	h.Close()
	if q := CheckLocalQuiescence(positive); q.State != "quiescent" {
		t.Fatal("valid legacy migration lost proof", q)
	}
}
func TestMixedLayoutsCannotHideUnknownExecution(t *testing.T) {
	t.Run("legacy-with-unindexed-active", func(t *testing.T) {
		data, _ := json.Marshal(journal{Work: testWork(), ProcessStarted: true, Quiescent: true})
		root := legacyRoot(t, data)
		unknown := workFor("unknown")
		dir := filepath.Join(root, "active", contracts.Hash([]byte("unknown")))
		if e := os.MkdirAll(dir, 0700); e != nil {
			t.Fatal(e)
		}
		if e := durable(filepath.Join(dir, "journal.json"), journal{Work: unknown, ProcessStarted: true}); e != nil {
			t.Fatal(e)
		}
		if q := CheckLocalQuiescence(root); q.State != "uncertain" {
			t.Fatal("legacy proof hid active journal", q)
		}
		if h, e := openHost(hostTestConfig(root)); e == nil {
			h.Close()
			t.Fatal("recovery ignored active orphan")
		}
	})
	t.Run("modern-with-leftover-legacy", func(t *testing.T) {
		root := t.TempDir()
		h, e := openHost(hostTestConfig(root))
		if e != nil {
			t.Fatal(e)
		}
		if e = h.confirmInventory(); e != nil {
			t.Fatal(e)
		}
		h.Close()
		if e = durable(filepath.Join(root, "journal.json"), journal{Work: testWork(), ProcessStarted: true}); e != nil {
			t.Fatal(e)
		}
		if q := CheckLocalQuiescence(root); q.State != "uncertain" {
			t.Fatal("modern inventory hid legacy execution", q)
		}
		if h, e := openHost(hostTestConfig(root)); e == nil {
			h.Close()
			t.Fatal("recovery ignored leftover legacy")
		}
	})
}
func TestExactInterruptedLegacyMigrationCanResume(t *testing.T) {
	for _, withDirectory := range []bool{false, true} {
		t.Run(map[bool]string{false: "intent", true: "empty-directory"}[withDirectory], func(t *testing.T) {
			work := testWork()
			data, _ := json.Marshal(journal{Work: work})
			root := legacyRoot(t, data)
			digest, _ := contracts.Digest(*work.Assignment)
			inventory := hostInventory{SchemaVersion: 1, Reconciled: true, Entries: []activeEntry{{work.Assignment.AssignmentID, digest}}}
			if e := durable(filepath.Join(root, "active.json"), inventory); e != nil {
				t.Fatal(e)
			}
			if withDirectory {
				if e := os.MkdirAll(filepath.Join(root, "active", contracts.Hash([]byte(work.Assignment.AssignmentID))), 0700); e != nil {
					t.Fatal(e)
				}
			}
			if q := CheckLocalQuiescence(root); q.State != "uncertain" {
				t.Fatal("incomplete migration treated as final proof", q)
			}
			h, e := openHost(hostTestConfig(root))
			if e != nil {
				t.Fatal("exact indexed migration did not resume", e)
			}
			h.Close()
			if q := CheckLocalQuiescence(root); q.State != "quiescent" {
				t.Fatal("resumed migration lost never-launched proof", q)
			}
		})
	}
}
func TestContradictoryLifecycleProofRejected(t *testing.T) {
	for _, j := range []journal{{Work: testWork(), Started: []string{"u"}}, {Work: testWork(), Quiescent: true, Uncertain: true}, {Work: testWork(), FinalSent: true}} {
		b, _ := json.Marshal(j)
		var decoded journal
		if e := json.Unmarshal(b, &decoded); e == nil {
			t.Fatal("contradictory proof decoded", j)
		}
	}
}
