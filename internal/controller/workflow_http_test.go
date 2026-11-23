package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hermes/internal/contracts"
)

func workflowHTTP(t *testing.T, c *Controller, method, path, principal string, approval *WorkflowAdapterConfig, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	w := httptest.NewRecorder()
	if !c.serveWorkflowHTTP(w, r, principal, approval) {
		t.Fatalf("workflow route unhandled: %s", path)
	}
	return w
}

type workflowUnreadBody struct{}

func (workflowUnreadBody) Read([]byte) (int, error) { panic("unauthorized/unrelated body read") }
func (workflowUnreadBody) Close() error             { return nil }

var _ io.ReadCloser = workflowUnreadBody{}

func TestWorkflowHTTPDispatchAndOperatorBoundary(t *testing.T) {
	c := &Controller{}
	for _, path := range []string{"/v1/runs", "/v1/workflows-other", "/v1/workflow", "/"} {
		r := httptest.NewRequest("POST", path, nil)
		r.Body = workflowUnreadBody{}
		w := httptest.NewRecorder()
		if c.serveWorkflowHTTP(w, r, "operator", nil) || w.Body.Len() != 0 || len(w.Header()) != 0 {
			t.Fatalf("unrelated route affected: %s", path)
		}
	}
	for _, principal := range []string{"worker:local", "unknown", ""} {
		for _, path := range []string{"/v1/workflows", "/v1/workflows/control", "/v1/workflows/status?workflow=known", "/v1/workflows/unknown"} {
			r := httptest.NewRequest("POST", path, nil)
			r.Body = workflowUnreadBody{}
			w := httptest.NewRecorder()
			if !c.serveWorkflowHTTP(w, r, principal, nil) || w.Code != http.StatusForbidden {
				t.Fatalf("principal %q route %s: %d", principal, path, w.Code)
			}
		}
	}
	if w := workflowHTTP(t, c, "POST", "/v1/workflows/unknown", "operator", nil, nil); w.Code != 404 {
		t.Fatal(w.Code)
	}
	for _, tc := range []struct{ method, path, allow string }{{"GET", "/v1/workflows", "POST"}, {"GET", "/v1/workflows/control", "POST"}, {"POST", "/v1/workflows/status", "GET"}} {
		w := workflowHTTP(t, c, tc.method, tc.path, "operator", nil, nil)
		if w.Code != 405 || w.Header().Get("Allow") != tc.allow {
			t.Fatalf("wrong method: %d %v", w.Code, w.Header())
		}
	}
}
func TestWorkflowHTTPRegistrationReplayAndNoServerApproval(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	before := workflowCount(t, f.c, `SELECT count(*) FROM runs`)
	r := f.registration
	r.WorkflowID = "http-workflow"
	r.RequestID = "http-register"
	r.Initial = workflowGraph("http-initial")
	r.DecisionUnit = r.Initial.Units[0].UnitID
	w := workflowHTTP(t, f.c, "POST", "/v1/workflows", "operator", nil, encoded(r))
	if w.Code != 503 || workflowCount(t, f.c, `SELECT count(*) FROM runs`) != before {
		t.Fatalf("unapproved graph created: %d %s", w.Code, w.Body)
	}
	// HTTP only compares server-owned approval metadata, never launches a process.
	approval := f.adapter
	approval.Argv = []string{"/not-an-executable-in-http"}
	w = workflowHTTP(t, f.c, "POST", "/v1/workflows", "operator", &approval, encoded(r))
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("register: %d %s", w.Code, w.Body)
	}
	replay := workflowHTTP(t, f.c, "POST", "/v1/workflows", "operator", &approval, encoded(r))
	if replay.Code != 200 || replay.Body.String() != w.Body.String() {
		t.Fatalf("request replay: %d %s", replay.Code, replay.Body)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM runs`); n != before+1 {
		t.Fatalf("runs=%d", n)
	}
	r.DecisionPath = "different.json"
	if w = workflowHTTP(t, f.c, "POST", "/v1/workflows", "operator", &approval, encoded(r)); w.Code != 400 {
		t.Fatalf("conflicting request: %d", w.Code)
	}
	r = f.registration
	r.RequestID = "bad-approval"
	r.WorkflowID = "bad-approval"
	r.Initial = workflowGraph("bad-approval")
	r.DecisionUnit = r.Initial.Units[0].UnitID
	r.RuntimeDigest = contracts.Hash([]byte("not-server-approved"))
	if w = workflowHTTP(t, f.c, "POST", "/v1/workflows", "operator", &approval, encoded(r)); w.Code != 400 {
		t.Fatalf("body chose runtime approval: %d", w.Code)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM runs`); n != before+1 {
		t.Fatalf("bad approval registered graph: %d", n)
	}
}
func TestWorkflowHTTPRejectsMalformedAndOversizeBodies(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	for _, path := range []string{"/v1/workflows", "/v1/workflows/control"} {
		for _, body := range []string{`{`, `{}`, `{"schema_version":2}`, `{"schema_version":1,"argv":["/request-selected-runtime"]}`, `{"schema_version":1,"schema_version":1}`, `{} {}`, strings.Repeat(" ", contracts.MaxMetadataBytes+1)} {
			w := workflowHTTP(t, f.c, "POST", path, "operator", &f.adapter, []byte(body))
			if w.Code != 400 {
				t.Fatalf("%s malformed body: %d %s", path, w.Code, w.Body)
			}
		}
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM runs`); n != 1 {
		t.Fatalf("malformed request changed graphs: %d", n)
	}
}
func TestWorkflowHTTPStatusAndControlWithoutRuntime(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	for _, tc := range []struct {
		path   string
		status int
	}{{"/v1/workflows/status", 400}, {"/v1/workflows/status?workflow=missing", 404}, {"/v1/workflows/status?workflow=workflow", 200}} {
		w := workflowHTTP(t, f.c, "GET", tc.path, "operator", nil, nil)
		if w.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.path, w.Code, w.Body)
		}
		if tc.status == 200 {
			var s contracts.WorkflowStatus
			if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil || !strings.Contains(s.Reason, "unavailable") {
				t.Fatalf("unconfigured explanation: %+v %v", s, err)
			}
		}
	}
	recorded, err := f.c.WorkflowStatus(context.Background(), "workflow")
	if err != nil || recorded.Reason != "" {
		t.Fatalf("GET mutated recorded status: %+v %v", recorded, err)
	}
	stop := contracts.WorkflowControl{SchemaVersion: 1, RequestID: "http-stop", WorkflowID: "workflow", ExpectedRevision: 1, Action: "stop", TerminateAt: time.Now().Add(time.Minute)}
	w := workflowHTTP(t, f.c, "POST", "/v1/workflows/control", "operator", nil, encoded(stop))
	if w.Code != 200 {
		t.Fatalf("stop: %d %s", w.Code, w.Body)
	}
	replay := workflowHTTP(t, f.c, "POST", "/v1/workflows/control", "operator", nil, encoded(stop))
	if replay.Code != 200 || replay.Body.String() != w.Body.String() {
		t.Fatalf("stop replay: %d %s", replay.Code, replay.Body)
	}
	resume := contracts.WorkflowControl{SchemaVersion: 1, RequestID: "http-resume", WorkflowID: "workflow", ExpectedRevision: 2, Action: "resume"}
	if w = workflowHTTP(t, f.c, "POST", "/v1/workflows/control", "operator", nil, encoded(resume)); w.Code != 200 {
		t.Fatalf("resume: %d %s", w.Code, w.Body)
	}
	if n := workflowCount(t, f.c, `SELECT count(*) FROM target_controls WHERE kind='run' AND target='first' AND action='drain'`); n != 1 {
		t.Fatal("HTTP resume reopened independent run drain")
	}
	missing := stop
	missing.RequestID = "missing-control"
	missing.WorkflowID = "missing"
	if w = workflowHTTP(t, f.c, "POST", "/v1/workflows/control", "operator", nil, encoded(missing)); w.Code != 404 {
		t.Fatalf("missing control: %d %s", w.Code, w.Body)
	}
	if _, err = f.c.db.Exec(`UPDATE workflows SET state='completed',reason='application prescribed stop' WHERE id='workflow'`); err != nil {
		t.Fatal(err)
	}
	w = workflowHTTP(t, f.c, "GET", "/v1/workflows/status?workflow=workflow", "operator", nil, nil)
	var completed contracts.WorkflowStatus
	if err = json.Unmarshal(w.Body.Bytes(), &completed); w.Code != 200 || err != nil || completed.Reason != "application prescribed stop" {
		t.Fatalf("completed reason overwritten: %d %+v %v", w.Code, completed, err)
	}
}
