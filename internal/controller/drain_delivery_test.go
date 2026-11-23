package controller

import (
	"context"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestDrainDuringRealFixturePreparation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	s := submission("preparation")
	s.Units = s.Units[:2]
	s.Packages = s.Packages[:1]
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	root := filepath.Join(t.TempDir(), "spool")
	running, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- agent.Run(running, agent.Config{Root: root, WorkerID: "local", SessionID: "session-local", Runtime: "synthetic-v1", Profile: "synthetic-local", RuntimeArgv: []string{f.app, "--prepare-delay-millis", "2000"}, Client: f.worker, MaxArtifactBytes: 1 << 20, ShutdownGrace: 30 * time.Millisecond})
	}()
	defer func() { cancel(); <-done }()
	await(t, func() bool {
		j := snapshot(root)
		return j.Work.Assignment != nil && j.ProcessStarted && !j.Quiescent && len(j.Started) == 0
	})
	request := contracts.ControlRequest{SchemaVersion: 1, RequestID: "drain-preparation", TargetKind: "worker", TargetID: "local", Action: "drain", TerminateAt: time.Now().Add(200 * time.Millisecond)}
	var intent contracts.ControlStatus
	if e := f.operator.Do(ctx, "POST", "/v1/control", request, &intent); e != nil {
		t.Fatal(e)
	}
	await(t, func() bool {
		q, e := f.c.ControlStatus(ctx, "worker", "local")
		return e == nil && q.State == "complete"
	})
	await(t, func() bool { return snapshot(root).FinalSent })
	j := snapshot(root)
	if len(j.Started) != 0 || !j.Quiescent || !j.FinalSent {
		t.Fatal("preparation drain did not hand back never-started inventory", j)
	}
}
func TestQuiescentDrainRemainsPendingUntilResultInventoryDelivered(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	s := submission("pending-delivery")
	s.Units = s.Units[:2]
	s.Packages = s.Packages[:1]
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	var blocked atomic.Bool
	blocked.Store(true)
	proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blocked.Load() && (r.Method == "PUT" || r.URL.Path == "/v1/candidate") {
			http.Error(w, "synthetic transfer outage", 503)
			return
		}
		f.c.Handler().ServeHTTP(w, r)
	}))
	proxy.TLS = f.server.TLS.Clone()
	proxy.StartTLS()
	defer proxy.Close()
	root := filepath.Join(t.TempDir(), "spool")
	_, stop := agentProcess(t, f, proxy.URL, root, "20")
	defer stop()
	await(t, func() bool { j := snapshot(root); return j.Quiescent && len(j.Candidates) == 2 })
	request := contracts.ControlRequest{SchemaVersion: 1, RequestID: "drain-spooled", TargetKind: "worker", TargetID: "local", Action: "drain", TerminateAt: time.Now().Add(time.Second)}
	var intent contracts.ControlStatus
	if e := f.operator.Do(ctx, "POST", "/v1/control", request, &intent); e != nil {
		t.Fatal(e)
	}
	await(t, func() bool {
		q, e := f.c.ControlStatus(ctx, "worker", "local")
		return e == nil && q.State == "pending_delivery" && q.PendingBarriers == 0
	})
	status, e := f.c.Status(ctx, s.RunID)
	if e != nil || status.Quiescent || status.Accepted != 0 {
		t.Fatal("local exit invented durable controller handoff", status, e)
	}
	blocked.Store(false)
	await(t, func() bool {
		q, e := f.c.ControlStatus(ctx, "worker", "local")
		return e == nil && q.State == "complete"
	})
	await(t, func() bool { q, e := f.c.Status(ctx, s.RunID); return e == nil && q.Accepted == 2 && q.Quiescent })
}
