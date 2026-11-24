package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"testing"
)

func finiteReceipt(a contracts.Assignment, id string) contracts.Receipt {
	return contracts.Receipt{SchemaVersion: 1, RequestID: id, WorkerID: a.WorkerID, SessionID: a.SessionID, AssignmentID: a.AssignmentID}
}
func finiteNoStart(env contracts.FiniteBundleEnvelope, m contracts.FiniteMember) contracts.FiniteFinalInventory {
	a := *m.Work.Assignment
	r := contracts.ReconcileRequest{Receipt: finiteReceipt(a, "reconcile-"+a.AssignmentID), Revision: 1, AssignmentDigest: m.AssignmentDigest, ProcessStarted: false, Quiescent: true}
	final := contracts.FinalReport{Receipt: finiteReceipt(a, "final-"+a.AssignmentID)}
	for _, u := range a.Units {
		final.NeverStarted = append(final.NeverStarted, u.UnitID)
	}
	return contracts.FiniteFinalInventory{SchemaVersion: 1, BundleDigest: env.BundleDigest, AssignmentDigest: m.AssignmentDigest, Reconciliation: r, Final: &final}
}
func finiteTestOutput(t *testing.T, env contracts.FiniteBundleEnvelope) string {
	t.Helper()
	root := t.TempDir()
	for _, dir := range []string{"blobs", "candidates", "finals"} {
		if e := os.Mkdir(filepath.Join(root, dir), 0700); e != nil {
			t.Fatal(e)
		}
	}
	finiteTestJSON(t, filepath.Join(root, "bundle.json"), env)
	return root
}
func finiteTestJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
}
func finiteTestCandidate(t *testing.T, root string, m contracts.FiniteMember) (contracts.CandidateReport, string) {
	t.Helper()
	a := *m.Work.Assignment
	candidate, data := singleCandidate(a, m.Work.Units[0])
	report := contracts.CandidateReport{Receipt: finiteReceipt(a, "candidate-"+a.AssignmentID), Candidate: candidate}
	digest, _ := contracts.Digest(candidate)
	finiteTestJSON(t, filepath.Join(root, "candidates", digest+".json"), report)
	if e := os.WriteFile(filepath.Join(root, "blobs", candidate.Files[0].Artifact.Digest), data, 0600); e != nil {
		t.Fatal(e)
	}
	return report, digest
}
func finiteRawCount(ack contracts.FiniteImportAck) int {
	n := 0
	for _, m := range ack.Members {
		n += len(m.RawPublished)
	}
	return n
}
func TestFiniteCandidateOnlyImportAndPartialAckReplay(t *testing.T) {
	f := newFiniteFixture(t)
	env := f.seal(t)
	root := finiteTestOutput(t, env)
	// Only raw candidate files: no lifecycle file or complete-output marker.
	first, firstDigest := finiteTestCandidate(t, root, env.Bundle.Members[0])
	second, secondDigest := finiteTestCandidate(t, root, env.Bundle.Members[1])
	if firstDigest > secondDigest {
		first, second = second, first
		firstDigest, secondDigest = secondDigest, firstDigest
	}
	laterPath := filepath.Join(root, "candidates", secondDigest+".json")
	if e := os.Remove(laterPath); e != nil {
		t.Fatal(e)
	}
	ack, e := f.c.ImportFiniteDirectory(context.Background(), root)
	if e != nil || finiteRawCount(ack) != 1 {
		t.Fatal("candidate-only", ack, e)
	}
	if ingressCount(t, f.c, "execution_reports") != 0 || ingressCount(t, f.c, "acceptances") != 0 {
		t.Fatal("raw import inferred lifecycle or science acceptance")
	}
	for _, m := range ack.Members {
		if m.FinalDigest != "" || m.ReconciliationRevision != 0 {
			t.Fatal("invented final/progress ACK")
		}
	}
	finiteTestJSON(t, laterPath, second)
	ack, e = f.c.ImportFiniteDirectory(context.Background(), root)
	if e != nil || finiteRawCount(ack) != 2 {
		t.Fatal(ack, e)
	}
	// Arrange source lexical order so the first record replays a historical ACK,
	// then a later file fails. Returned ACK must retain BOTH already committed.
	files, e := filepath.Glob(filepath.Join(root, "candidates", "*.json"))
	if e != nil || len(files) != 2 {
		t.Fatal(files, e)
	}
	if e = os.WriteFile(files[1], []byte("{"), 0600); e != nil {
		t.Fatal(e)
	}
	partial, e := f.c.ImportFiniteDirectory(context.Background(), root)
	if e == nil || finiteRawCount(partial) != 2 {
		t.Fatal("cached replay regressed partial ACK", partial, e)
	}
	// No import created online receipt evidence.
	for _, report := range []contracts.CandidateReport{first, second} {
		var receipt int
		f.c.db.QueryRow("SELECT receipt FROM assignments WHERE id=?", report.AssignmentID).Scan(&receipt)
		if receipt != 0 {
			t.Fatal("fabricated receipt")
		}
	}
}
func TestFiniteCandidateScopeAndInvalidIncomingBytes(t *testing.T) {
	f := newFiniteFixture(t)
	env := f.seal(t)
	root := finiteTestOutput(t, env)
	report, _ := finiteTestCandidate(t, root, env.Bundle.Members[0])
	file := report.Candidate.Files[0].Artifact
	if _, e := f.c.ImportFiniteCandidate(context.Background(), env.BundleDigest, report); !errors.Is(e, ErrFiniteUnavailable) {
		t.Fatal("metadata ACK preceded raw durability", e)
	}
	if e := os.WriteFile(filepath.Join(root, "blobs", file.Digest), bytes.Repeat([]byte("x"), int(file.Size)), 0600); e != nil {
		t.Fatal(e)
	}
	ack, e := f.c.ImportFiniteDirectory(context.Background(), root)
	if e == nil || finiteRawCount(ack) != 0 {
		t.Fatal("bad bytes acknowledged", ack, e)
	}
	report.Candidate.SpecDigest = contracts.Hash([]byte("foreign-spec"))
	if _, e = f.c.ImportFiniteCandidate(context.Background(), env.BundleDigest, report); !errors.Is(e, ErrFiniteInvalid) {
		t.Fatal("foreign original scope accepted", e)
	}
	if ingressCount(t, f.c, "candidates") != 0 {
		t.Fatal("invalid candidate committed")
	}
}
func TestFiniteGenuineNeverStartedAndStopAtomicity(t *testing.T) {
	f := newFiniteFixture(t)
	env := f.seal(t)
	m := env.Bundle.Members[0]
	inventory := finiteNoStart(env, m)
	// No executor was launched in this test; the all-never-started proof is true.
	// Typed pressure below is an explicitly policy-approved SYNTHETIC test event,
	// not fabricated native OS enforcement/capacity evidence.
	allocationDigest, _ := contracts.Digest(m.Work.Assignment.Allocation)
	stop := contracts.ResourceStop{Receipt: finiteReceipt(*m.Work.Assignment, "stop"), Kind: "spool_exhausted", Source: "synthetic_test_event", Phase: "prepare", AllocationDigest: allocationDigest, KnownInadequateLimit: m.Work.Assignment.Allocation.Resources.SpoolBytes, EvidenceReference: "controlled-synthetic-pressure"}
	inventory.ResourceStop = &stop
	bad := inventory
	bad.Final = &contracts.FinalReport{Receipt: inventory.Final.Receipt}
	if _, e := f.c.ImportFiniteFinal(context.Background(), bad); e == nil {
		t.Fatal("foreign/incomplete final accepted")
	}
	if ingressCount(t, f.c, "resource_failures") != 0 || ingressCount(t, f.c, "execution_reports") != 0 {
		t.Fatal("failed final leaked stop/reconcile")
	}
	ack, e := f.c.ImportFiniteFinal(context.Background(), inventory)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, a := range ack.Members {
		if a.AssignmentDigest == m.AssignmentDigest {
			expected, _ := contracts.Digest(*inventory.Final)
			if a.FinalDigest != expected || !a.ResourceStopAcknowledged || a.ReconciliationRevision != 1 {
				t.Fatal("wrong genuine ACK", a)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("missing member")
	}
	if ingressCount(t, f.c, "resource_failures") != 1 {
		t.Fatal("typed floor missing")
	}
	var final []byte
	if e = f.c.db.QueryRow("SELECT final FROM assignments WHERE id=?", m.Work.Assignment.AssignmentID).Scan(&final); e != nil || len(final) == 0 {
		t.Fatal("stop committed without final", e)
	}
	retry, e := f.c.ImportFiniteFinal(context.Background(), inventory)
	if e != nil || !bytes.Equal(encoded(retry), encoded(ack)) {
		t.Fatal("final retry changed receipt", e)
	}
	if e = f.c.ConfigureFiniteRuntimes(context.Background(), nil); e != nil {
		t.Fatal(e)
	}
	other := finiteNoStart(env, env.Bundle.Members[1])
	if _, e = f.c.ImportFiniteFinal(context.Background(), other); e != nil {
		t.Fatal("approval disable blocked old import", e)
	}
}
func TestFiniteUncertainInventoryWithoutOuterFinal(t *testing.T) {
	f := newFiniteFixture(t)
	env := f.seal(t)
	m := env.Bundle.Members[0]
	inventory := finiteNoStart(env, m)
	inventory.Final = nil
	inventory.Uncertain = true
	inventory.Reconciliation.Quiescent = false
	ack, e := f.c.ImportFiniteFinal(context.Background(), inventory)
	if e != nil {
		t.Fatal(e)
	}
	var state string
	if e = f.c.db.QueryRow("SELECT state FROM execution_reports WHERE assignment_id=?", m.Work.Assignment.AssignmentID).Scan(&state); e != nil || state != "uncertain" {
		t.Fatal("explicit uncertainty became permission", state, e)
	}
	for _, a := range ack.Members {
		if a.FinalDigest != "" {
			t.Fatal("uncertainty acknowledged nonexistent final")
		}
	}
	if ingressCount(t, f.c, "owners") != 2 {
		t.Fatal("uncertainty released ownership")
	}
}
func TestFiniteStopAndFinalRollbackOnStickyUncertainty(t *testing.T) {
	f := newFiniteFixture(t)
	env := f.seal(t)
	m := env.Bundle.Members[0]
	unknown := finiteNoStart(env, m)
	unknown.Final = nil
	unknown.Uncertain = true
	unknown.Reconciliation.Quiescent = false
	if _, e := f.c.ImportFiniteFinal(context.Background(), unknown); e != nil {
		t.Fatal(e)
	}
	attempted := finiteNoStart(env, m)
	attempted.Reconciliation.Revision = 2
	attempted.Reconciliation.RequestID = "unsupported-positive-proof"
	digest, _ := contracts.Digest(m.Work.Assignment.Allocation)
	attempted.ResourceStop = &contracts.ResourceStop{Receipt: finiteReceipt(*m.Work.Assignment, "synthetic-pressure"), Kind: "spool_exhausted", Source: "synthetic_test_event", Phase: "prepare", AllocationDigest: digest, KnownInadequateLimit: m.Work.Assignment.Allocation.Resources.SpoolBytes, EvidenceReference: "controlled-synthetic-event"}
	if _, e := f.c.ImportFiniteFinal(context.Background(), attempted); e == nil {
		t.Fatal("uncertainty bypassed by a new positive claim")
	}
	if ingressCount(t, f.c, "resource_failures") != 0 {
		t.Fatal("stop leaked from failed lifecycle transaction")
	}
	var report []byte
	if e := f.c.db.QueryRow("SELECT manifest FROM execution_reports WHERE assignment_id=?", m.Work.Assignment.AssignmentID).Scan(&report); e != nil || !bytes.Equal(report, encoded(unknown.Reconciliation)) {
		t.Fatal("reconcile leaked from failed final", e)
	}
	ack, e := f.c.FiniteAcknowledgment(context.Background(), env.BundleDigest)
	if e != nil {
		t.Fatal(e)
	}
	for _, a := range ack.Members {
		if a.ResourceStopAcknowledged || a.FinalDigest != "" || a.ReconciliationRevision > 1 {
			t.Fatal("failed transaction acknowledged", a)
		}
	}
}
func TestFiniteSourceSymlinkRejectedBeforeRead(t *testing.T) {
	f := newFiniteFixture(t)
	env := f.seal(t)
	root := finiteTestOutput(t, env)
	report, digest := finiteTestCandidate(t, root, env.Bundle.Members[0])
	path := filepath.Join(root, "blobs", report.Candidate.Files[0].Artifact.Digest)
	if e := os.Remove(path); e != nil {
		t.Fatal(e)
	}
	outside := filepath.Join(t.TempDir(), "private")
	if e := os.WriteFile(outside, []byte("not an authorized source blob"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink(outside, path); e != nil {
		t.Fatal(e)
	}
	ack, e := f.c.ImportFiniteDirectory(context.Background(), root)
	if e == nil || finiteRawCount(ack) != 0 {
		t.Fatal("external link read/published", digest, e)
	}
}
