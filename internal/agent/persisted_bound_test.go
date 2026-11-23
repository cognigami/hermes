package agent

import (
	"encoding/json"
	"fmt"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func boundedJournalFixture(t *testing.T, target int) (journal, int, int) {
	t.Helper()
	w := testWork()
	w.Units = nil
	w.Package.Units = nil
	w.Assignment.Units = nil
	j := journal{ProcessStarted: true}
	artifact := contracts.Artifact{Digest: contracts.Hash(nil), MediaType: strings.Repeat("m", 192)}
	file := contracts.ResultFile{Path: "f/0000", Artifact: artifact}
	base := testWork().Units[0]
	for i := 0; i < 129; i++ {
		u := base
		u.UnitID = fmt.Sprintf("unit-%04d", i)
		u.SpecDigest = contracts.Hash([]byte(u.UnitID))
		w.Units = append(w.Units, u)
		ref := contracts.UnitRef{UnitID: u.UnitID, SpecDigest: u.SpecDigest}
		w.Package.Units = append(w.Package.Units, ref)
		w.Assignment.Units = append(w.Assignment.Units, ref)
		j.Started = append(j.Started, u.UnitID)
		if i < 128 {
			j.Candidates = append(j.Candidates, contracts.Candidate{SchemaVersion: 1, UnitID: u.UnitID, SpecDigest: u.SpecDigest, AssignmentID: w.Assignment.AssignmentID, AttemptID: w.Assignment.AssignmentID, Outcome: "complete", Files: []contracts.ResultFile{file}})
		}
	}
	w.Assignment.PackageDigest, _ = contracts.Digest(*w.Package)
	j.Work = w
	type unchecked journal
	b, e := json.Marshal(unchecked(j))
	if e != nil {
		t.Fatal(e)
	}
	encodedFile, _ := json.Marshal(file)
	remaining := target - len(b)
	if remaining <= 0 {
		t.Fatal("fixture base too large")
	}
	additional, remainder := remaining/(len(encodedFile)+1), remaining%(len(encodedFile)+1)
	lastUnit, lastFile := -1, -1
	for i := range j.Candidates {
		count := additional
		if count > contracts.MaxUnits-1 {
			count = contracts.MaxUnits - 1
		}
		for k := 1; k <= count; k++ {
			next := file
			next.Path = fmt.Sprintf("f/%04d", k)
			j.Candidates[i].Files = append(j.Candidates[i].Files, next)
			lastUnit, lastFile = i, k
		}
		additional -= count
		if additional == 0 {
			break
		}
	}
	if additional != 0 || lastUnit < 0 {
		t.Fatal("fixture cannot fit legal per-unit candidate limits")
	}
	for remainder > 0 {
		if remainder == 1 {
			j.Candidates[lastUnit].Files[lastFile].Path += "x"
			break
		}
		part := remainder
		if part > 64 {
			part = 64
		}
		j.Candidates[lastUnit].Files[lastFile].Path += "/" + strings.Repeat("x", part-1)
		remainder -= part
	}
	for _, c := range j.Candidates {
		if e = c.Validate(); e != nil {
			t.Fatal(e)
		}
		wire, _ := json.Marshal(c)
		if len(wire) > contracts.MaxMetadataBytes {
			t.Fatal("fixture candidate exceeds wire bound")
		}
	}
	b, e = json.Marshal(unchecked(j))
	if e != nil || len(b) != target {
		t.Fatal("fixture not exact requested bound", len(b), target, e)
	}
	if e = validateWork(j.Work); e != nil {
		t.Fatal(e)
	}
	return j, lastUnit, lastFile
}
func TestPersistedJournalSerializationAndRecoveryShareBoundary(t *testing.T) {
	j, unit, file := boundedJournalFixture(t, maxPersistedJournalBytes-1)
	root := t.TempDir()
	r := runner{cfg: Config{Root: root}, j: j}
	if e := r.save(); e != nil {
		t.Fatal("under limit failed", e)
	}
	path := filepath.Join(root, "journal.json")
	under, e := os.ReadFile(path)
	if e != nil || len(under) != maxPersistedJournalBytes-1 {
		t.Fatal(len(under), e)
	}
	var decoded journal
	if e = json.Unmarshal(under, &decoded); e != nil {
		t.Fatal("under-limit journal unrecoverable", e)
	}
	r.j.Candidates[unit].Files[file].Path += "x"
	if e = r.save(); e != nil {
		t.Fatal("exact limit failed", e)
	}
	exact, e := os.ReadFile(path)
	if e != nil || len(exact) != maxPersistedJournalBytes {
		t.Fatal(len(exact), e)
	}
	if e = json.Unmarshal(exact, &decoded); e != nil {
		t.Fatal("exact-limit journal unrecoverable", e)
	}
	before := contracts.Hash(exact)
	// The next assigned unit has a valid, individually bounded candidate, but its
	// cumulative manifest must not replace the last recoverable journal or become
	// visible to the independent publisher without a successful local commit.
	u := j.Work.Units[len(j.Work.Units)-1]
	candidate := contracts.Candidate{SchemaVersion: 1, UnitID: u.UnitID, SpecDigest: u.SpecDigest, AssignmentID: j.Work.Assignment.AssignmentID, AttemptID: j.Work.Assignment.AssignmentID, Outcome: "complete", Files: []contracts.ResultFile{{Path: "next.json", Artifact: contracts.Artifact{Digest: contracts.Hash(nil), MediaType: "application/json"}}}}
	if e = candidate.Validate(); e != nil {
		t.Fatal(e)
	}
	count := len(r.j.Candidates)
	if e = r.persistCandidate(candidate); e == nil {
		t.Fatal("oversize candidate journal acknowledged")
	}
	if len(r.j.Candidates) != count || !r.j.GateClosed {
		t.Fatal("uncommitted candidate exposed or start gate left open")
	}
	after, e := os.ReadFile(path)
	if e != nil || contracts.Hash(after) != before {
		t.Fatal("oversize replaced previous durable journal", e)
	}
	if e = json.Unmarshal(after, &decoded); e != nil || len(decoded.Candidates) != count {
		t.Fatal("last valid proof unreadable after rejected replacement", e)
	}
	type unchecked journal
	sizeBytes, _ := json.Marshal(unchecked(r.j))
	growth := maxPersistedJournalBytes + 1 - len(sizeBytes)
	if growth < 1 {
		t.Fatal("unexpected fixture size")
	}
	r.j.Candidates[unit].Files[file].Path += strings.Repeat("x", growth)
	over, _ := json.Marshal(unchecked(r.j))
	if len(over) != maxPersistedJournalBytes+1 {
		t.Fatal("not exactly one byte over", len(over))
	}
	if e = r.save(); e == nil {
		t.Fatal("one byte over limit serialized")
	}
	again, e := os.ReadFile(path)
	if e != nil || contracts.Hash(again) != before {
		t.Fatal("one byte over limit changed durable journal", e)
	}
}
