package contracts_test

import (
	"bytes"
	"encoding/json"
	"hermes/internal/contracts"
	"hermes/testdata/synthetic"
	"strings"
	"testing"
	"time"
)

func unit(id string) contracts.Unit {
	return contracts.Unit{SchemaVersion: 1, UnitID: id, SpecDigest: contracts.Hash([]byte("spec")), Operation: "synthetic-v1", Runtime: "synthetic-v1", Validator: "synthetic-v1"}
}
func TestImmutableIdentity(t *testing.T) {
	u := unit("a")
	if e := contracts.CompatibleUnit(u, u); e != nil {
		t.Fatal(e)
	}
	changed := u
	changed.Runtime = "other"
	if contracts.CompatibleUnit(u, changed) == nil {
		t.Fatal("metadata mutation accepted under same supplied spec")
	}
	changed = u
	changed.SpecDigest = contracts.Hash([]byte("other"))
	if contracts.CompatibleUnit(u, changed) == nil {
		t.Fatal("spec mutation accepted")
	}
	before, _ := contracts.Digest(u)
	now := time.Now()
	a := contracts.Assignment{SchemaVersion: 1, AssignmentID: "a", RunID: "r", WorkerID: "w", SessionID: "s", PackageDigest: before, Units: []contracts.UnitRef{{UnitID: u.UnitID, SpecDigest: u.SpecDigest}}, Runtime: u.Runtime, Profile: "small", PolicyRevision: 1, StopNewUnitsAt: now, TerminateAt: now.Add(time.Minute), HandoffDeadline: now.Add(2 * time.Minute)}
	if e := a.Validate(); e != nil {
		t.Fatal(e)
	}
	a.WorkerID = "other"
	a.Profile = "large"
	if e := a.Validate(); e != nil {
		t.Fatal(e)
	}
	after, _ := contracts.Digest(u)
	if before != after {
		t.Fatal("placement changed scientific identity")
	}
}
func TestStrictBounds(t *testing.T) {
	for _, s := range []string{`{"schema_version":1,"schema_version":2,"type":"ready"}`, `{"schema_version":1,"type":"ready","extra":1}`, `{"schema_version":1,"type":"ready"} {}`, strings.Repeat(" ", contracts.MaxMetadataBytes+1)} {
		var m contracts.Message
		if contracts.Decode(strings.NewReader(s), &m) == nil {
			t.Fatalf("accepted invalid metadata")
		}
	}
	for _, v := range []int{0, 2} {
		if (contracts.Message{SchemaVersion: v, Type: "ready"}).Validate() == nil {
			t.Fatal("version accepted")
		}
	}
	r := contracts.MessageReader(strings.NewReader(strings.Repeat("x", contracts.MaxMessageBytes+2) + "\n"))
	if _, e := contracts.ReadMessage(r); e == nil {
		t.Fatal("oversized message")
	}
	var b bytes.Buffer
	m := contracts.Message{SchemaVersion: 1, Type: "ready"}
	if e := contracts.WriteMessage(&b, m); e != nil {
		t.Fatal(e)
	}
	if got, e := contracts.ReadMessage(contracts.MessageReader(&b)); e != nil || got != m {
		t.Fatal(got, e)
	}
}
func TestGraph(t *testing.T) {
	a, b := unit("a"), unit("b")
	b.Dependencies = []string{"a"}
	if e := contracts.ValidateGraph([]contracts.Unit{a, b}); e != nil {
		t.Fatal(e)
	}
	a.Dependencies = []string{"b"}
	if contracts.ValidateGraph([]contracts.Unit{a, b}) == nil {
		t.Fatal("cycle accepted")
	}
	if contracts.ValidateGraph([]contracts.Unit{b}) == nil {
		t.Fatal("missing dependency accepted")
	}
}
func TestPathsAndCandidate(t *testing.T) {
	for _, p := range []string{"../x", "/x", "a/../b", "a\\b", ".", ""} {
		if contracts.ValidPath(p) {
			t.Fatal(p)
		}
	}
	c := contracts.Candidate{SchemaVersion: 1, UnitID: "u", SpecDigest: contracts.Hash(nil), AssignmentID: "a", AttemptID: "a", Outcome: "complete", Files: []contracts.ResultFile{{Path: "r.json", Artifact: contracts.Artifact{Digest: contracts.Hash(nil), MediaType: "application/json"}}}}
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	c.Files = append(c.Files, c.Files[0])
	if c.Validate() == nil {
		t.Fatal("duplicate result file")
	}
	b, _ := json.Marshal(c)
	var d contracts.Candidate
	if e := contracts.Decode(bytes.NewReader(b), &d); e != nil {
		t.Fatal(e)
	}
}

func TestVerdictRequiresExplicitBoolean(t *testing.T) {
	for _, s := range []string{`{"schema_version":1}`, `{"schema_version":1,"valid":null}`, `{"schema_version":1,"valid":"false"}`, `{"schema_version":1,"valid":true,"VALID":false}`} {
		var v contracts.Verdict
		if err := contracts.Decode(strings.NewReader(s), &v); err == nil {
			t.Fatalf("accepted malformed verdict: %s", s)
		}
	}
	for _, valid := range []bool{false, true} {
		b, _ := json.Marshal(contracts.Verdict{SchemaVersion: 1, Valid: valid, Reason: "explicit decision"})
		var v contracts.Verdict
		if err := contracts.Decode(bytes.NewReader(b), &v); err != nil {
			t.Fatal(err)
		}
		if err := v.Validate(); err != nil {
			t.Fatal(err)
		}
		if v.Valid != valid {
			t.Fatal("changed decision")
		}
	}
}

func TestCaseAliasKeysRejected(t *testing.T) {
	for _, s := range []string{`{"schema_version":2,"SCHEMA_VERSION":1,"type":"ready"}`, `{"outer":{"valid":true,"VALID":false}}`, `{"spec_digest":1,"ſpec_digest":2}`} {
		var v any
		if err := contracts.Decode(strings.NewReader(s), &v); err == nil {
			t.Fatalf("accepted case alias: %s", s)
		}
	}
}

func TestFixtureAssignmentValidation(t *testing.T) {
	for _, tc := range []string{"valid_external", "duplicate", "mismatch", "malformed", "empty"} {
		t.Run(tc, func(t *testing.T) {
			var m synthetic.Manifest
			if err := synthetic.Load("../../testdata/synthetic/manifest.json", &m); err != nil {
				t.Fatal(err)
			}
			switch tc {
			case "valid_external":
				m.Units[0].Dependencies = []string{"earlier-accepted"}
			case "duplicate":
				m.Units = append(m.Units, m.Units[0])
				m.Assignment.Units = append(m.Assignment.Units, m.Assignment.Units[0])
			case "mismatch":
				m.Units[0].UnitID = "not-assigned"
			case "malformed":
				m.Units[0].Runtime = ""
			case "empty":
				m.Units = nil
				m.Assignment.Units = nil
			}
			err := m.Validate()
			if (err == nil) != (tc == "valid_external") {
				t.Fatalf("unexpected validation result: %v", err)
			}
		})
	}
}
