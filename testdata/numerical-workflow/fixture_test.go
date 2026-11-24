package numericalworkflow

import (
	"os"
	"path/filepath"
	"testing"

	"hermes/internal/contracts"
)

func TestSuppliedNumericalValuesAndDomainRejection(t *testing.T) {
	inputs := [][]int64{{1, 2, 3}, {4, 5}, {6}}
	expected := []int64{14, 55, 91}
	var prior *Result
	for i, values := range inputs {
		r, err := Calculate(Plan{SchemaVersion: 1, Stage: i, Stages: 3, Values: values}, prior)
		if err != nil || r.Value != expected[i] || r.Continue != (i < 2) {
			t.Fatalf("stage%d: %+v %v", i, r, err)
		}
		prior = &r
	}
	plan := Encode(Plan{SchemaVersion: 1, Stage: 0, Stages: 3, Values: inputs[0]})
	ref := Artifact(plan)
	next, err := Stage(Settings{WorkflowID: "numeric", Plans: []contracts.Artifact{ref, ref, ref}}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	wrong := Encode(Result{SchemaVersion: 1, Stage: 0, Value: 15, Continue: true})
	c := contracts.Candidate{SchemaVersion: 1, UnitID: next.DecisionUnit, SpecDigest: next.Submission.Units[0].SpecDigest, AssignmentID: "attempt", AttemptID: "attempt", Outcome: "calculated"}
	for name, b := range map[string][]byte{"plan.json": plan, "decision.json": wrong} {
		if err = os.WriteFile(filepath.Join(root, name), b, 0600); err != nil {
			t.Fatal(err)
		}
		c.Files = append(c.Files, contracts.ResultFile{Path: name, Artifact: Artifact(b)})
	}
	v, err := Validate(root, next.Submission.Units[0], c)
	if err != nil || v.Valid {
		t.Fatalf("incorrect math became acceptance: %+v %v", v, err)
	}
}
