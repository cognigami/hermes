package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hermes/internal/agent"
	"hermes/internal/contracts"
)

func workflowCLIClient(t *testing.T, handler http.HandlerFunc) *agent.Client {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	return &agent.Client{URL: server.URL, HTTP: server.Client()}
}
func workflowCLIFile(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return workflowCLIBytes(t, data)
}
func workflowCLIBytes(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func workflowCLIRegistration() contracts.WorkflowRegistration {
	unit := contracts.Unit{SchemaVersion: 1, UnitID: "unit", SpecDigest: contracts.Hash([]byte("spec")), Operation: "synthetic", Runtime: "synthetic-v1", Validator: "synthetic-v1"}
	return contracts.WorkflowRegistration{SchemaVersion: 1, RequestID: "register-once", WorkflowID: "workflow", Contract: "staged-v1", RuntimeDigest: contracts.Hash([]byte("adapter")), Settings: contracts.Artifact{Digest: contracts.Hash([]byte("{}")), Size: 2, MediaType: "application/json"}, Initial: contracts.Submission{SchemaVersion: 1, RequestID: "initial", RunID: "run", Units: []contracts.Unit{unit}, Packages: []contracts.Package{{SchemaVersion: 1, PackageID: "package", Units: []contracts.UnitRef{{UnitID: unit.UnitID, SpecDigest: unit.SpecDigest}}}}}, DecisionUnit: unit.UnitID, DecisionPath: "decision.json"}
}

func TestWorkflowCLIRegistrationPreservesRequest(t *testing.T) {
	request := workflowCLIRegistration()
	calls := 0
	client := workflowCLIClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/workflows" {
			t.Errorf("wrong route: %s %s", r.Method, r.URL)
		}
		var received contracts.WorkflowRegistration
		if err := contracts.Decode(r.Body, &received); err != nil {
			t.Error(err)
		}
		expected, _ := contracts.Digest(request)
		actual, _ := contracts.Digest(received)
		if actual != expected {
			t.Errorf("request changed: %+v", received)
		}
		_ = json.NewEncoder(w).Encode(contracts.WorkflowStatus{SchemaVersion: 1, WorkflowID: request.WorkflowID, Revision: 1, State: "active", RunID: "run"})
	})
	path := workflowCLIFile(t, request)
	for i := 0; i < 2; i++ {
		var out bytes.Buffer
		handled, err := runWorkflowCommand(context.Background(), client, []string{"workflow-register", path}, &out)
		if !handled || err != nil {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
		var status contracts.WorkflowStatus
		if err := contracts.Decode(&out, &status); err != nil || status.WorkflowID != request.WorkflowID {
			t.Fatalf("status=%+v err=%v", status, err)
		}
	}
	if calls != 2 {
		t.Fatal("missing retries", calls)
	}
}
func TestWorkflowCLIControlAndEscapedStatus(t *testing.T) {
	id := "workflow&other=value"
	request := contracts.WorkflowControl{SchemaVersion: 1, RequestID: "stop-once", WorkflowID: id, ExpectedRevision: 1, Action: "stop", TerminateAt: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)}
	client := workflowCLIClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/workflows/control":
			var received contracts.WorkflowControl
			if r.Method != http.MethodPost {
				t.Error("control method", r.Method)
			}
			if err := contracts.Decode(r.Body, &received); err != nil {
				t.Error(err)
			}
			if received != request {
				t.Error("control changed", received)
			}
		case "/v1/workflows/status":
			if r.Method != http.MethodGet || r.URL.Query().Get("workflow") != id || len(r.URL.Query()) != 1 {
				t.Error("status query changed", r.URL)
			}
		default:
			t.Error("unexpected route", r.URL)
		}
		_ = json.NewEncoder(w).Encode(contracts.WorkflowStatus{SchemaVersion: 1, WorkflowID: id, Revision: 2, State: "stopped", RunID: "run", Reason: "operator stop"})
	})
	for _, args := range [][]string{{"workflow-control", workflowCLIFile(t, request)}, {"workflow-status", id}} {
		var out bytes.Buffer
		handled, err := runWorkflowCommand(context.Background(), client, args, &out)
		if !handled || err != nil {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
		if !strings.Contains(out.String(), "operator stop") {
			t.Fatal("lost server reason", out.String())
		}
	}
}
func TestWorkflowCLIRejectsMalformedBeforeNetwork(t *testing.T) {
	valid, _ := json.Marshal(workflowCLIRegistration())
	for _, data := range [][]byte{[]byte(`{`), append(append([]byte{}, valid...), []byte(` {}`)...), []byte(`{"schema_version":1,"schema_version":1}`), []byte(`{"schema_version":1,"argv":["/bin/sh"]}`), bytes.Repeat([]byte(" "), contracts.MaxMetadataBytes+1)} {
		var out bytes.Buffer
		handled, err := runWorkflowCommand(context.Background(), nil, []string{"workflow-register", workflowCLIBytes(t, data)}, &out)
		if !handled || err == nil || out.Len() != 0 {
			t.Fatalf("malformed handled=%v err=%v output=%s", handled, err, out.String())
		}
	}
	for _, version := range []int{0, 2} {
		request := workflowCLIRegistration()
		request.SchemaVersion = version
		var out bytes.Buffer
		if _, err := runWorkflowCommand(context.Background(), nil, []string{"workflow-register", workflowCLIFile(t, request)}, &out); err == nil {
			t.Fatal("unsupported request version", version)
		}
	}
}
func TestWorkflowCLIRejectsWrongResponse(t *testing.T) {
	for _, status := range []contracts.WorkflowStatus{{SchemaVersion: 2, WorkflowID: "workflow", Revision: 1}, {SchemaVersion: 1, WorkflowID: "other", Revision: 1}, {SchemaVersion: 1, WorkflowID: "workflow"}} {
		client := workflowCLIClient(t, func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(status) })
		var out bytes.Buffer
		handled, err := runWorkflowCommand(context.Background(), client, []string{"workflow-status", "workflow"}, &out)
		if !handled || !errors.Is(err, agent.ErrProtocol) || out.Len() != 0 {
			t.Fatalf("status=%+v handled=%v err=%v", status, handled, err)
		}
	}
	client := workflowCLIClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "adapter not configured", http.StatusServiceUnavailable)
	})
	var out bytes.Buffer
	_, err := runWorkflowCommand(context.Background(), client, []string{"workflow-register", workflowCLIFile(t, workflowCLIRegistration())}, &out)
	var httpError *agent.HTTPError
	if !errors.As(err, &httpError) || httpError.Status != http.StatusServiceUnavailable || out.Len() != 0 {
		t.Fatalf("missing server error: %v", err)
	}
}
func TestWorkflowCLIDoesNotInterceptOtherCommands(t *testing.T) {
	var out bytes.Buffer
	handled, err := runWorkflowCommand(context.Background(), nil, []string{"submit", "not-read"}, &out)
	if handled || err != nil || out.Len() != 0 {
		t.Fatalf("other command intercepted: %v %v", handled, err)
	}
	for _, args := range [][]string{{"workflow-status"}, {"workflow-status", ""}, {"workflow-register", "file", "extra"}} {
		handled, err = runWorkflowCommand(context.Background(), nil, args, &out)
		if !handled || err == nil {
			t.Fatalf("bad args accepted: %v %v %v", args, handled, err)
		}
	}
}
