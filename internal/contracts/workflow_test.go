package contracts

import (
	"strings"
	"testing"
)

func TestWorkflowOutputRequiresExclusiveExplicitOutcome(t *testing.T) {
	for _, data := range []string{`{}`, `{"schema_version":1}`, `{"schema_version":1,"stop_reason":"stop","next":{}}`, `{"schema_version":2,"stop_reason":"stop"}`, `{"schema_version":1,"stop_reason":"stop","unexpected":true}`} {
		var out WorkflowAdapterOutput
		err := Decode(strings.NewReader(data), &out)
		if err == nil {
			err = out.Validate()
		}
		if err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
	out := WorkflowAdapterOutput{SchemaVersion: 1, StopReason: "application stop"}
	if err := out.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestWorkflowRegistrationAllowanceAndPaths(t *testing.T) {
	r := WorkflowRegistration{SchemaVersion: 1, RequestID: "request", WorkflowID: "workflow", Contract: "contract", RuntimeDigest: Hash([]byte("runtime")), Settings: Artifact{Digest: Hash(nil), MediaType: "application/json"}, DecisionUnit: "decision", DecisionPath: "decision.json"}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	max := int64(0)
	r.MaxSubmittedUnits = &max
	if err := r.Validate(); err == nil {
		t.Fatal("zero allowance accepted")
	}
	r.MaxSubmittedUnits = nil
	r.DecisionPath = "../escape"
	if err := r.Validate(); err == nil {
		t.Fatal("invalid decision path accepted")
	}
}
