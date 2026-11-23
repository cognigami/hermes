package contracts_test

import (
	"bytes"
	"encoding/json"
	"hermes/internal/contracts"
	"testing"
)

func TestConnectedEnvelopeRoundtrip(t *testing.T) {
	r := contracts.CandidateReport{Receipt: contracts.Receipt{SchemaVersion: 1, RequestID: "request", AssignmentID: "assignment", WorkerID: "worker", SessionID: "session"}, Candidate: contracts.Candidate{SchemaVersion: 1, UnitID: "unit", SpecDigest: contracts.Hash(nil), AssignmentID: "assignment", AttemptID: "assignment", Outcome: "complete", Files: []contracts.ResultFile{{Path: "result.json", Artifact: contracts.Artifact{Digest: contracts.Hash(nil), MediaType: "application/json"}}}}}
	b, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	var decoded contracts.CandidateReport
	if e = contracts.Decode(bytes.NewReader(b), &decoded); e != nil {
		t.Fatal(e)
	}
	if decoded.Receipt != r.Receipt {
		t.Fatal("receipt identity lost", decoded)
	}
	if e = decoded.Candidate.Validate(); e != nil {
		t.Fatal(e)
	}
	if !bytes.Contains(b, []byte(`"schema_version":1`)) || bytes.Contains(b, []byte(`"Receipt"`)) {
		t.Fatal("receipt must be flat protocol envelope", string(b))
	}
}
