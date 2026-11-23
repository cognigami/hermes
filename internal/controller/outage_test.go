package controller

import (
	"context"
	"hermes/internal/contracts"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func controllerProcess(t *testing.T, f *fixture, url string) func() {
	t.Helper()
	dir := filepath.Dir(f.app)
	log, e := os.CreateTemp(dir, "live-controller-log-")
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(filepath.Join(dir, "hermes-controller"), "--root", f.cfg.Root, "--listen", strings.TrimPrefix(url, "https://"), "--ca", f.ca, "--cert", filepath.Join(dir, "server.crt"), "--key", filepath.Join(dir, "server.key"), "--workers", "local,other", "--validator", f.cfg.ValidatorArgv[0])
	cmd.Stdout = log
	cmd.Stderr = log
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	var once sync.Once
	stop := func() { once.Do(func() { cmd.Process.Kill(); cmd.Wait(); log.Close() }) }
	t.Cleanup(stop)
	return stop
}
func agentProcess(t *testing.T, f *fixture, url, root string, delay string) (*exec.Cmd, func()) {
	t.Helper()
	dir := filepath.Dir(f.app)
	log, e := os.CreateTemp(dir, "live-agent-log-")
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(filepath.Join(dir, "hermes-agent"), "--root", root, "--worker", "local", "--session", "live-session", "--url", url, "--ca", f.ca, "--cert", filepath.Join(dir, "worker:local.crt"), "--key", filepath.Join(dir, "worker:local.key"), "--runtime", f.app, "--runtime-arg=--delay-millis", "--runtime-arg="+delay)
	cmd.Stdout = log
	cmd.Stderr = log
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	var once sync.Once
	stop := func() { once.Do(func() { cmd.Process.Kill(); cmd.Wait(); log.Close() }) }
	t.Cleanup(stop)
	return cmd, stop
}
func TestControllerOutageLeavesIssuedLeafExecutionIndependent(t *testing.T) {
	f := setup(t)
	url := f.server.URL
	f.stop()
	stopController := controllerProcess(t, f, url)
	s := submission("outage")
	ctx := context.Background()
	await(t, func() bool { return f.operator.Do(ctx, "POST", "/v1/runs", s, nil) == nil })
	root := filepath.Join(t.TempDir(), "spool")
	_, stopAgent := agentProcess(t, f, url, root, "300")
	await(t, func() bool { return len(snapshot(root).Started) == 1 })
	first := snapshot(root).Work.Assignment.AssignmentID
	stopController()
	await(t, func() bool { s := snapshot(root); return s.Quiescent && len(s.Candidates) == 2 })
	if snapshot(root).Work.Assignment.AssignmentID != first {
		t.Fatal("agent refilled while controller absent")
	}
	restart := controllerProcess(t, f, url)
	defer restart()
	await(t, func() bool {
		var s contracts.Status
		return f.operator.Do(ctx, "GET", "/v1/status?run=outage", nil, &s) == nil && s.Accepted == 3 && s.Quiescent
	})
	stopAgent()
	paths, e := filepath.Glob(filepath.Join(root, "assignment-*", "output", "*.manifest.json"))
	if e != nil || len(paths) != 3 {
		t.Fatal("issued/accepted prefix replayed", paths, e)
	}
}
func TestAgentProcessRestartReportsUncertaintyWithoutReplaying(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	s := oneSubmission("agent-crash")
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	root := filepath.Join(t.TempDir(), "spool")
	_, kill := agentProcess(t, f, f.server.URL, root, "800")
	await(t, func() bool { return len(snapshot(root).Started) == 1 })
	original := snapshot(root).Work.Assignment.AssignmentID
	kill()
	restarted, stop := agentProcess(t, f, f.server.URL, root, "800")
	defer stop()
	await(t, func() bool { q, e := f.c.Status(ctx, "agent-crash"); return e == nil && q.UncertainAssignments == 1 })
	snp := snapshot(root)
	if snp.Work.Assignment.AssignmentID != original || len(snp.Started) != 1 || snp.Quiescent {
		t.Fatal("restart invented quiescence or replayed", snp)
	}
	var count int
	if e := f.c.db.QueryRow("SELECT count(*) FROM assignments WHERE run_id='agent-crash'").Scan(&count); e != nil || count != 1 {
		t.Fatal("uncertain ownership was reassigned", count, e)
	}
	_ = restarted // no PID discovery, adoption or signaling of the surviving application.
}
