package main

import (
	"bytes"
	"encoding/json"
	"hermes/internal/contracts"
	"os"
	"testing"
)

func TestSyntheticStopAndMissingDecision(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.WriteFile("settings.json", []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	input := contracts.WorkflowAdapterInput{SchemaVersion: 1, WorkflowID: "workflow", Contract: "synthetic", RuntimeDigest: contracts.Hash(nil), CandidateDigest: contracts.Hash(nil), Decision: contracts.Artifact{Digest: contracts.Hash(nil), MediaType: "application/json"}, Settings: contracts.Artifact{Digest: contracts.Hash(nil), MediaType: "application/json"}, DecisionFile: "decision.json", SettingsFile: "settings.json"}
	// Use the shared protocol definition, not a competing example schema.
	for _, decision := range []string{`{"continue":false}`, `{}`, `{"continue":true}`} {
		if err := os.WriteFile("decision.json", []byte(decision), 0600); err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		var stdout bytes.Buffer
		err = evaluate(bytes.NewReader(b), &stdout)
		if decision == `{"continue":false}` {
			if err != nil {
				t.Fatal(err)
			}
			var output contracts.WorkflowAdapterOutput
			if err = contracts.Decode(&stdout, &output); err != nil || output.StopReason == "" {
				t.Fatalf("stop %v %+v", err, output)
			}
		} else if err == nil {
			t.Fatal("missing decision/graph became stop")
		}
	}
}
