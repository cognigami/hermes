package contracts_test

import (
	"bytes"
	"encoding/json"
	"hermes/internal/contracts"
	"strings"
	"testing"
	"time"
)

func finiteFixture() contracts.FiniteBundleEnvelope {
	u := unit("finite-unit")
	ref := contracts.UnitRef{UnitID: u.UnitID, SpecDigest: u.SpecDigest}
	p := contracts.Package{SchemaVersion: 1, PackageID: "package", Units: []contracts.UnitRef{ref}}
	pd, _ := contracts.Digest(p)
	allocation := contracts.Allocation{Resources: contracts.ResourceVector{CPUMilli: 1000, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := contracts.Assignment{SchemaVersion: 1, AssignmentID: "assignment", RunID: "run", WorkerID: "worker", SessionID: "session", PackageDigest: pd, Units: []contracts.UnitRef{ref}, Runtime: u.Runtime, Profile: "small", PolicyRevision: 1, Allocation: allocation, StopNewUnitsAt: now, TerminateAt: now.Add(time.Minute), HandoffDeadline: now.Add(2 * time.Minute)}
	ad, _ := contracts.Digest(a)
	budget := contracts.HostBudget{SchemaVersion: 1, PolicyRevision: 1, Approval: contracts.HostApproval{HostID: "host", WorkerID: "worker", MaxAssignments: 2, Limits: contracts.ResourceVector{CPUMilli: 4000, MemoryBytes: 512 << 20, SpoolBytes: 64 << 20}}, Profiles: []contracts.ResourceProfile{{ID: "small", Conservative: true, Allocation: allocation}}}
	runtime := contracts.Artifact{Digest: contracts.Hash(nil), MediaType: "application/octet-stream"}
	b := contracts.FiniteBundle{SchemaVersion: 1, BundleID: "bundle-safe_1", WorkerID: "worker", SessionID: "session", HostID: "host", ExportRevision: 1, Members: []contracts.FiniteMember{{AssignmentDigest: ad, Work: contracts.Work{SchemaVersion: 1, Assignment: &a, Package: &p, Units: []contracts.Unit{u}, Budget: &budget}}}, RuntimeRefs: []contracts.FiniteRuntimeRef{{Runtime: u.Runtime, GOOS: "darwin", GOARCH: "amd64", ISABaseline: "v1", Executable: runtime}}, Blobs: []contracts.Artifact{runtime}}
	digest, _ := contracts.Digest(b)
	return contracts.FiniteBundleEnvelope{BundleDigest: digest, Bundle: b}
}
func TestFiniteBundleDigestBindingAndOriginalBounds(t *testing.T) {
	e := finiteFixture()
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(e)
	var decoded contracts.FiniteBundleEnvelope
	if err := contracts.Decode(bytes.NewReader(data), &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatal(err)
	}
	if !decoded.Bundle.Members[0].Work.Assignment.TerminateAt.Equal(e.Bundle.Members[0].Work.Assignment.TerminateAt) {
		t.Fatal("bounds renewed")
	}
	decoded.Bundle.ExportRevision++
	if decoded.Validate() == nil {
		t.Fatal("modified export seal accepted")
	}
}
func TestFiniteBundleRejectsScopeInventoryAndPathAmbiguity(t *testing.T) {
	for _, kind := range []string{"path", "identity", "duplicate-member", "size", "missing-runtime", "over-bound"} {
		t.Run(kind, func(t *testing.T) {
			e := finiteFixture()
			switch kind {
			case "path":
				e.Bundle.BundleID = "../elsewhere"
			case "identity":
				e.Bundle.Members[0].Work.Assignment.SessionID = "other"
			case "duplicate-member":
				e.Bundle.Members = append(e.Bundle.Members, e.Bundle.Members[0])
			case "size":
				e.Bundle.Blobs[0].Size = 1
			case "missing-runtime":
				e.Bundle.RuntimeRefs = nil
			case "over-bound":
				e.Bundle.Blobs[0].MediaType = strings.Repeat("x", contracts.MaxMetadataBytes)
			}
			e.BundleDigest, _ = contracts.Digest(e.Bundle)
			if e.Validate() == nil {
				t.Fatal("invalid finite bundle accepted", kind)
			}
		})
	}
}
func TestFiniteLifecycleRequiresExplicitFactsAndQuiescentFinal(t *testing.T) {
	bundle := finiteFixture()
	m := bundle.Bundle.Members[0]
	receipt := contracts.Receipt{SchemaVersion: 1, RequestID: "finite-final", AssignmentID: m.Work.Assignment.AssignmentID, WorkerID: bundle.Bundle.WorkerID, SessionID: bundle.Bundle.SessionID}
	f := contracts.FiniteFinalInventory{SchemaVersion: 1, BundleDigest: bundle.BundleDigest, AssignmentDigest: m.AssignmentDigest, Reconciliation: contracts.ReconcileRequest{Receipt: receipt, Revision: 1, AssignmentDigest: m.AssignmentDigest, Quiescent: true}, Final: &contracts.FinalReport{Receipt: receipt, NeverStarted: []string{m.Work.Units[0].UnitID}}}
	data, _ := json.Marshal(f)
	var decoded contracts.FiniteFinalInventory
	if e := contracts.Decode(bytes.NewReader(data), &decoded); e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"uncertain", "process_started", "quiescent"} {
		for _, null := range []bool{false, true} {
			var raw map[string]any
			json.Unmarshal(data, &raw)
			target := raw
			if key != "uncertain" {
				target = raw["reconciliation"].(map[string]any)
			}
			if null {
				target[key] = nil
			} else {
				delete(target, key)
			}
			body, _ := json.Marshal(raw)
			if e := contracts.Decode(bytes.NewReader(body), &decoded); e == nil {
				t.Fatal("missing/null fact became lifecycle proof", key, null)
			}
		}
	}
	f.Uncertain = true
	if f.Validate() == nil {
		t.Fatal("uncertain final accepted")
	}
}
func TestFiniteInventoryRejectsContradictionsAndBindsExactPartition(t *testing.T) {
	b := finiteFixture()
	a := *b.Bundle.Members[0].Work.Assignment
	receipt := contracts.Receipt{SchemaVersion: 1, RequestID: "proof", AssignmentID: a.AssignmentID, WorkerID: a.WorkerID, SessionID: a.SessionID}
	fixture := func() contracts.FiniteFinalInventory {
		return contracts.FiniteFinalInventory{SchemaVersion: 1, BundleDigest: b.BundleDigest, AssignmentDigest: b.Bundle.Members[0].AssignmentDigest, Reconciliation: contracts.ReconcileRequest{Receipt: receipt, Revision: 1, AssignmentDigest: b.Bundle.Members[0].AssignmentDigest, Quiescent: true}, Final: &contracts.FinalReport{Receipt: receipt, NeverStarted: []string{a.Units[0].UnitID}}}
	}
	for _, kind := range []string{"hidden-grant", "bad-digest", "duplicate-digest", "nested-version", "nested-request", "overlap", "foreign", "incomplete"} {
		t.Run(kind, func(t *testing.T) {
			f := fixture()
			switch kind {
			case "hidden-grant":
				f.Reconciliation.Started = []string{a.Units[0].UnitID}
			case "bad-digest":
				f.Reconciliation.CandidateDigests = []string{"not-hex"}
			case "duplicate-digest":
				f.Reconciliation.CandidateDigests = []string{contracts.Hash(nil), contracts.Hash(nil)}
			case "nested-version":
				f.Final.SchemaVersion = 2
			case "nested-request":
				f.Final.RequestID = ""
			case "overlap":
				f.Final.Completed = []string{a.Units[0].UnitID}
			case "foreign":
				f.Final.NeverStarted = []string{"foreign"}
			case "incomplete":
				f.Final.NeverStarted = nil
			}
			if f.ValidateForAssignment(a) == nil {
				t.Fatal("invalid finite facts accepted", kind)
			}
		})
	}
	if e := fixture().ValidateForAssignment(a); e != nil {
		t.Fatal(e)
	}
	f := fixture()
	f.Reconciliation.ProcessStarted = true
	f.Final.LaunchFailed = true
	if e := f.ValidateForAssignment(a); e != nil {
		t.Fatal("positive no-child failed launch rejected", e)
	}
}
func TestFiniteBundleRejectsDistinctAssignmentsOwningSameRunUnit(t *testing.T) {
	e := finiteFixture()
	member := e.Bundle.Members[0]
	a := *member.Work.Assignment
	a.AssignmentID = "other-assignment"
	member.Work.Assignment = &a
	member.AssignmentDigest, _ = contracts.Digest(a)
	e.Bundle.Members = append(e.Bundle.Members, member)
	e.BundleDigest, _ = contracts.Digest(e.Bundle)
	if e.Validate() == nil {
		t.Fatal("duplicate run/unit ownership accepted")
	}
	e.Bundle.Members[1].Work.Assignment.RunID = "other-run"
	e.Bundle.Members[1].AssignmentDigest, _ = contracts.Digest(*e.Bundle.Members[1].Work.Assignment)
	e.BundleDigest, _ = contracts.Digest(e.Bundle)
	if err := e.Validate(); err != nil {
		t.Fatal("same unit in different run rejected", err)
	}
}

func TestFiniteAckIsDigestScopedNotAcceptance(t *testing.T) {
	e := finiteFixture()
	ack := contracts.FiniteImportAck{SchemaVersion: 1, BundleID: e.Bundle.BundleID, BundleDigest: e.BundleDigest, Members: []contracts.FiniteMemberAck{{AssignmentDigest: e.Bundle.Members[0].AssignmentDigest, RawPublished: []string{contracts.Hash([]byte("candidate"))}}}}
	if err := ack.Validate(); err != nil {
		t.Fatal(err)
	}
	ack.Members[0].RawPublished = append(ack.Members[0].RawPublished, ack.Members[0].RawPublished[0])
	if ack.Validate() == nil {
		t.Fatal("duplicate raw ACK accepted")
	}
}
