package agent

import (
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"testing"
)

func workFor(id string) contracts.Work {
	w := testWork()
	w.Assignment.AssignmentID = id
	w.Units[0].UnitID = "unit-" + id
	w.Units[0].SpecDigest = contracts.Hash([]byte(id))
	refs := []contracts.UnitRef{{UnitID: w.Units[0].UnitID, SpecDigest: w.Units[0].SpecDigest}}
	w.Package.PackageID = "package-" + id
	w.Package.Units = refs
	w.Assignment.Units = refs
	w.Assignment.PackageDigest, _ = contracts.Digest(*w.Package)
	return w
}
func TestHostKeepsAssignmentGatesAndOneAuthority(t *testing.T) {
	root := t.TempDir()
	cfg := hostTestConfig(root)
	cfg.MaxConcurrent = 2
	h, e := openHost(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	if other, e := openHost(cfg); e == nil {
		other.Close()
		t.Fatal("second host authority acquired lock")
	}
	a, e := h.receive(workFor("first"))
	if e != nil {
		t.Fatal(e)
	}
	b, e := h.receive(workFor("second"))
	if e != nil {
		t.Fatal(e)
	}
	if a.cfg.Root == b.cfg.Root || a.store != b.store {
		t.Fatal("assignment journal isolation/shared blobs lost")
	}
	_, e = a.applyControl(contracts.AssignmentControl{SchemaVersion: 1, AssignmentID: "first", Revision: 1, Action: "drain", TerminateAt: a.j.Work.Assignment.TerminateAt})
	if e != nil {
		t.Fatal(e)
	}
	if allowed, e := a.grant(a.j.Work.Assignment.Units[0].UnitID); e != nil || allowed {
		t.Fatal("drained gate opened", allowed, e)
	}
	b.j.ProcessStarted = true
	if allowed, e := b.grant(b.j.Work.Assignment.Units[0].UnitID); e != nil || !allowed {
		t.Fatal("neighbour drain changed unrelated gate", allowed, e)
	}
}
func TestHostMissingJournalAndOrphanNeverReconstructExecution(t *testing.T) {
	for _, fault := range []string{"missing", "digest", "orphan"} {
		t.Run(fault, func(t *testing.T) {
			root := t.TempDir()
			cfg := hostTestConfig(root)
			h, e := openHost(cfg)
			if e != nil {
				t.Fatal(e)
			}
			r, e := h.receive(workFor("retained"))
			if e != nil {
				t.Fatal(e)
			}
			h.Close()
			switch fault {
			case "missing":
				if e = os.Remove(filepath.Join(r.cfg.Root, "journal.json")); e != nil {
					t.Fatal(e)
				}
			case "digest":
				r.j.Work.Assignment.Profile = "changed"
				if e = r.save(); e != nil {
					t.Fatal(e)
				}
			case "orphan":
				if e = os.Mkdir(filepath.Join(root, "active", "unindexed"), 0700); e != nil {
					t.Fatal(e)
				}
			}
			if recovered, e := openHost(cfg); e == nil {
				recovered.Close()
				t.Fatal("fault reconstructed an execution right")
			}
			if q := CheckLocalQuiescence(root); q.State != "uncertain" {
				t.Fatal("damaged host inferred quiescence", q)
			}
		})
	}
}
func TestHostPendingPollAndArchiveRecoveryAreDurable(t *testing.T) {
	root := t.TempDir()
	cfg := hostTestConfig(root)
	h, e := openHost(cfg)
	if e != nil {
		t.Fatal(e)
	}
	first, e := h.pollRequest()
	if e != nil || first == nil {
		t.Fatal(first, e)
	}
	id := first.RequestID
	h.Close()
	h, e = openHost(cfg)
	if e != nil {
		t.Fatal(e)
	}
	again, e := h.pollRequest()
	if e != nil || again.RequestID != id {
		t.Fatal("pending poll identity lost", again, e)
	}
	r, e := h.receive(workFor("finished"))
	if e != nil {
		t.Fatal(e)
	}
	if h.inventory.Pending != nil {
		t.Fatal("receipt did not clear pending request")
	}
	r.j.Quiescent = true
	r.j.FinalSent = true
	if e = r.save(); e != nil {
		t.Fatal(e)
	}
	// Crash between directory archive and active-inventory removal.
	if e = os.Rename(h.assignmentRoot("finished"), h.completedRoot("finished")); e != nil {
		t.Fatal(e)
	}
	h.Close()
	h, e = openHost(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	if len(h.active) != 0 || len(h.inventory.Entries) != 0 {
		t.Fatal("terminal archive was replayed")
	}
	if _, e = h.receive(workFor("finished")); e == nil {
		t.Fatal("finalized assignment ID authorized another execution")
	}
}
