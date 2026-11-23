package controller

import (
	"context"
	"hermes/internal/contracts"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestUncertainRestartStillPublishesRetainedCandidate(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	s := submission("uncertain-delivery")
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
	_, kill := agentProcess(t, f, proxy.URL, root, "400")
	await(t, func() bool { j := snapshot(root); return len(j.Started) == 2 && len(j.Candidates) == 1 })
	original := snapshot(root).Work.Assignment.AssignmentID
	kill()
	blocked.Store(false)
	_, stop := agentProcess(t, f, proxy.URL, root, "400")
	defer stop()
	await(t, func() bool {
		q, e := f.c.Status(ctx, s.RunID)
		return e == nil && q.Accepted == 1 && q.UncertainAssignments == 1 && !q.Quiescent
	})
	j := snapshot(root)
	if j.Work.Assignment.AssignmentID != original || len(j.Started) != 2 || len(j.Candidates) != 1 || j.Quiescent || j.FinalSent {
		t.Fatal("uncertain restart changed execution truth", j)
	}
	var owners int
	if e := f.c.db.QueryRow("SELECT count(*) FROM owners WHERE run_id=?", s.RunID).Scan(&owners); e != nil || owners != 2 {
		t.Fatal("publication cleared unknown ownership", owners, e)
	}
	var empty contracts.Work
	if e := f.other.Do(ctx, "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: "no-duplicate", WorkerID: "other", SessionID: "other"}, &empty); e != nil || empty.Assignment != nil {
		t.Fatal("uncertain remainder reassigned", empty, e)
	}
}
func TestOfflineOriginalStartDeadlineRetainsRemainder(t *testing.T) {
	f := setup(t)
	f.c.cfg.StopAfter = time.Second
	f.c.cfg.TerminateAfter = 3 * time.Second
	f.c.cfg.HandoffAfter = 4 * time.Second
	ctx := context.Background()
	s := submission("offline-bound")
	s.Units = s.Units[:2]
	s.Packages = s.Packages[:1]
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	root := filepath.Join(t.TempDir(), "spool")
	_, stop := agentProcess(t, f, f.server.URL, root, "1200")
	defer stop()
	await(t, func() bool { return len(snapshot(root).Started) == 1 })
	original := snapshot(root).Work.Assignment
	f.stop()
	await(t, func() bool { j := snapshot(root); return j.Quiescent && len(j.Candidates) == 1 })
	j := snapshot(root)
	if len(j.Started) != 1 || j.FinalSent || !j.Work.Assignment.StopNewUnitsAt.Equal(original.StopNewUnitsAt) {
		t.Fatal("offline deadline extended or next unit started", j)
	}
	c, e := Open(f.cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	q, e := c.Status(ctx, s.RunID)
	if e != nil || q.Quiescent || q.Accepted != 0 {
		t.Fatal("offline expiry invented handoff", q, e)
	}
}
