package controller

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"hermes/internal/agent"
	"hermes/internal/contracts"
)

type workflowWireFixture struct {
	server                               *httptest.Server
	operator, worker                     *agent.Client
	ca, cert, key, serverCert, serverKey string
	roots                                *x509.CertPool
}

func workflowWire(t *testing.T, c *Controller, approval *WorkflowAdapterConfig) workflowWireFixture {
	t.Helper()
	dir := t.TempDir()
	_, ca, signer, caPath, _ := certificate(t, dir, "ca", nil, nil)
	pair, _, _, serverCert, serverKey := certificate(t, dir, "server", ca, signer)
	_, _, _, cert, key := certificate(t, dir, "operator", ca, signer)
	_, _, _, workerCert, workerKey := certificate(t, dir, "worker:worker", ca, signer)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	server := httptest.NewUnstartedServer(c.HandlerWithWorkflows(approval))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}
	server.StartTLS()
	t.Cleanup(server.Close)
	op, err := agent.NewClient(server.URL, caPath, cert, key)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := agent.NewClient(server.URL, caPath, workerCert, workerKey)
	if err != nil {
		t.Fatal(err)
	}
	return workflowWireFixture{server, op, worker, caPath, cert, key, serverCert, serverKey, roots}
}
func workflowTool(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", path, "./cmd/"+name)
	cmd.Dir = "../.."
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v %s", name, err, b)
	}
	return path
}
func workflowCLI(t *testing.T, cli string, w workflowWireFixture, args ...string) contracts.WorkflowStatus {
	t.Helper()
	argv := []string{"--url", w.operator.URL, "--ca", w.ca, "--cert", w.cert, "--key", w.key}
	cmd := exec.Command(cli, append(argv, args...)...)
	var out, diagnostic bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &diagnostic
	if err := cmd.Run(); err != nil {
		t.Fatalf("workflow CLI: %v %s", err, diagnostic.String())
	}
	var status contracts.WorkflowStatus
	if err := contracts.Decode(&out, &status); err != nil {
		t.Fatal(err)
	}
	return status
}
func workflowJSONFile(t *testing.T, value any) string {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "request.json")
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestWorkflowMTLSDispatchAndRealCLI(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	w := workflowWire(t, f.c, &f.adapter)
	cli := workflowTool(t, "hermes")
	request := f.registration
	request.RequestID = "wire-register"
	request.WorkflowID = "wire-workflow"
	request.Initial.RequestID = "wire-initial"
	request.Initial.RunID = "wire-run"
	var status contracts.WorkflowStatus
	err := w.worker.Do(context.Background(), http.MethodPost, "/v1/workflows", request, &status)
	var failure *agent.HTTPError
	if !errors.As(err, &failure) || failure.Status != http.StatusForbidden {
		t.Fatalf("worker registration allowed: %v", err)
	}
	if n := workflowCount(t, f.c, "SELECT count(*) FROM workflows"); n != 1 {
		t.Fatal("worker created graph", n)
	}
	path := workflowJSONFile(t, request)
	for i := 0; i < 2; i++ {
		status = workflowCLI(t, cli, w, "workflow-register", path)
		if status.WorkflowID != request.WorkflowID || status.Revision != 1 {
			t.Fatalf("bad registration %+v", status)
		}
	}
	if n := workflowCount(t, f.c, "SELECT count(*) FROM workflows"); n != 2 {
		t.Fatal("retry duplicated workflow", n)
	}
	// Legacy routes retain the same authentication and behavior in the new handler.
	var run contracts.Status
	if err = w.operator.Do(context.Background(), http.MethodGet, "/v1/status?run=wire-run", nil, &run); err != nil {
		t.Fatal(err)
	}
	control := contracts.WorkflowControl{SchemaVersion: 1, RequestID: "wire-stop", WorkflowID: request.WorkflowID, ExpectedRevision: 1, Action: "stop", TerminateAt: time.Now().Add(time.Minute)}
	status = workflowCLI(t, cli, w, "workflow-control", workflowJSONFile(t, control))
	if status.State != "stopped" || status.Revision != 2 {
		t.Fatalf("stop not applied %+v", status)
	}
	status = workflowCLI(t, cli, w, "workflow-status", request.WorkflowID)
	if status.State != "stopped" {
		t.Fatalf("status lost control %+v", status)
	}
	err = w.worker.Do(context.Background(), http.MethodGet, "/v1/workflows/status?workflow=wire-workflow", nil, &status)
	if !errors.As(err, &failure) || failure.Status != http.StatusForbidden {
		t.Fatalf("worker metadata allowed: %v", err)
	}
	// A normal TLS client has no client certificate; the server must refuse it.
	anonymous := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: w.roots}}, Timeout: time.Second}
	defer anonymous.CloseIdleConnections()
	if response, err := anonymous.Get(w.server.URL + "/v1/workflows/status?workflow=wire-workflow"); err == nil {
		response.Body.Close()
		t.Fatal("missing client certificate accepted")
	}
	disabled := workflowWire(t, f.c, nil)
	status = workflowCLI(t, cli, disabled, "workflow-status", request.WorkflowID)
	if status.State != "stopped" {
		t.Fatal("unconfigured adapter hid metadata")
	}
	request.RequestID = "disabled-register"
	request.WorkflowID = "disabled"
	request.Initial.RunID = "disabled-run"
	request.Initial.RequestID = "disabled-initial"
	err = disabled.operator.Do(context.Background(), http.MethodPost, "/v1/workflows", request, &status)
	if !errors.As(err, &failure) || failure.Status != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured registration accepted: %v", err)
	}
	if n := workflowCount(t, f.c, "SELECT count(*) FROM workflows"); n != 2 {
		t.Fatal("unconfigured handler created graph", n)
	}
}

func TestProductionControllerWorkflowStartupAndUnconfiguredRestart(t *testing.T) {
	f := newWorkflowFixture(t, nil)
	w := workflowWire(t, f.c, &f.adapter)
	binary := workflowTool(t, "hermes-controller")
	w.server.Close()
	endpoint := w.operator.URL
	address := w.server.Listener.Addr().String()
	before, err := f.c.ResourceStatus(context.Background(), f.cfg.ResourcePolicy.ControllerHostID)
	if err != nil {
		t.Fatal(err)
	}
	image, err := os.Stat(f.adapter.Argv[0])
	if err != nil {
		t.Fatal(err)
	}
	f.c.Close()
	policy := workflowJSONFile(t, f.cfg.ResourcePolicy)
	approval := workflowJSONFile(t, map[string]any{"schema_version": 1, "contract": f.adapter.Contract, "runtime_digest": f.adapter.RuntimeDigest, "argv": f.adapter.Argv, "timeout_millis": int64(f.adapter.Timeout / time.Millisecond), "max_metadata_bytes": f.adapter.MaxMetadataBytes, "max_log_bytes": f.adapter.MaxLogBytes})
	start := func(config string) func() {
		args := []string{"--root", f.cfg.Root, "--listen", address, "--ca", w.ca, "--cert", w.serverCert, "--key", w.serverKey, "--workers", "worker", "--validator", f.cfg.ValidatorArgv[0], "--resource-policy", policy}
		if config != "" {
			args = append(args, "--workflow-adapter-config", config)
		}
		cmd := exec.Command(binary, args...)
		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		var once sync.Once
		stop := func() {
			once.Do(func() {
				_ = cmd.Process.Signal(os.Interrupt)
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("controller stop: %v %s", err, output.String())
					}
				case <-time.After(10 * time.Second):
					_ = cmd.Process.Kill()
					<-done
					t.Error("controller did not join its service loops")
				}
			})
		}
		t.Cleanup(stop)
		// Probe only the authenticated local endpoint; no process/PID liveness inference.
		w.operator.URL = endpoint
		await(t, func() bool {
			var s contracts.WorkflowStatus
			return w.operator.Do(context.Background(), http.MethodGet, "/v1/workflows/status?workflow=workflow", nil, &s) == nil
		})
		return stop
	}
	stop := start(approval)
	await(t, func() bool {
		var resources ResourceHostStatus
		if err := w.operator.Do(context.Background(), http.MethodGet, "/v1/resources?host="+f.cfg.ResourcePolicy.ControllerHostID, nil, &resources); err != nil {
			return false
		}
		return resources.RetainedCopiesBytes >= before.RetainedCopiesBytes+image.Size()
	})
	// This verifies daemon startup/metadata/resource wiring, not a new numerical
	// acceptance: canonical continuation behavior is exercised by the bridge tests.
	stop()
	stop = start("")
	var status contracts.WorkflowStatus
	if err := w.operator.Do(context.Background(), http.MethodGet, "/v1/workflows/status?workflow=workflow", nil, &status); err != nil {
		t.Fatal(err)
	}
	var rejection *agent.HTTPError
	err = w.operator.Do(context.Background(), http.MethodPost, "/v1/workflows", f.registration, &status)
	if !errors.As(err, &rejection) || rejection.Status != http.StatusServiceUnavailable {
		t.Fatalf("restart silently enabled adapter: %v", err)
	}
	control := contracts.WorkflowControl{SchemaVersion: 1, RequestID: "disabled-stop", WorkflowID: "workflow", ExpectedRevision: 1, Action: "stop", TerminateAt: time.Now().Add(time.Minute)}
	if err := w.operator.Do(context.Background(), http.MethodPost, "/v1/workflows/control", control, &status); err != nil || status.State != "stopped" {
		t.Fatalf("unconfigured control unavailable: %+v %v", status, err)
	}
	stop()
}
