package controller

import (
	"bytes"
	"context"
	"hermes/internal/contracts"
	"hermes/testdata/synthetic"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func oneSubmission(run string) contracts.Submission {
	s := submission(run)
	s.Units = s.Units[:1]
	s.Packages = s.Packages[:1]
	s.Packages[0].Units = s.Packages[0].Units[:1]
	return s
}
func singleCandidate(a contracts.Assignment, u contracts.Unit) (contracts.Candidate, []byte) {
	data := encoded(synthetic.Result{UnitID: u.UnitID, SpecDigest: u.SpecDigest, Value: synthetic.Expected(u.UnitID, u.SpecDigest)})
	return contracts.Candidate{SchemaVersion: 1, UnitID: u.UnitID, SpecDigest: u.SpecDigest, AssignmentID: a.AssignmentID, AttemptID: a.AssignmentID, Outcome: "complete", Files: []contracts.ResultFile{{Path: "result.json", Artifact: contracts.Artifact{Digest: contracts.Hash(data), Size: int64(len(data)), MediaType: "application/json"}}}}, data
}
func TestValidatorPinAndDurableRegistration(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	ctx := context.Background()
	s := oneSubmission("pin")
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	var work contracts.Work
	if e := f.worker.Do(ctx, "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: "poll", WorkerID: "local", SessionID: "session"}, &work); e != nil {
		t.Fatal(e)
	}
	candidate, data := singleCandidate(*work.Assignment, s.Units[0])
	ref := candidate.Files[0].Artifact
	// Blob existence alone, even published by the store, is not controller acknowledgment metadata.
	if e := f.c.store.Publish(ctx, ref.Digest, ref.Size, bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	if e := f.c.store.Contains(ref.Digest, ref.Size); e != nil {
		t.Fatal(e)
	}
	report := contracts.CandidateReport{Receipt: contracts.Receipt{SchemaVersion: 1, RequestID: "candidate", AssignmentID: candidate.AssignmentID, WorkerID: "local", SessionID: "session"}, Candidate: candidate}
	if e := f.worker.Do(ctx, "POST", "/v1/candidate", report, nil); e == nil {
		t.Fatal("Contains substituted for durable registered metadata")
	}
	if e := f.worker.Publish(ctx, ref.Digest, ref.Size, candidate.AssignmentID, bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	if e := f.worker.Do(ctx, "POST", "/v1/candidate", report, nil); e != nil {
		t.Fatal(e)
	}
	f.stop()
	cfg := f.cfg
	cfg.Validator = "new-meaning"
	c, e := Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if e = c.validateNext(ctx); e != nil {
		t.Fatal(e)
	}
	status, e := c.Status(ctx, "pin")
	if e != nil || status.Accepted != 0 || status.ValidationFailures != 1 {
		t.Fatal(status, e)
	}
}
func TestControllerProcessLoss(t *testing.T) {
	f := setup(t)
	url := f.server.URL
	f.stop()
	dir := filepath.Dir(f.app)
	ctx := context.Background()
	var process *exec.Cmd
	stop := func() {
		if process != nil {
			process.Process.Kill()
			process.Wait()
			process = nil
		}
	}
	defer stop()
	start := func() {
		t.Helper()
		log, e := os.CreateTemp(dir, "controller-log-")
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { log.Close() })
		process = exec.Command(filepath.Join(dir, "hermes-controller"), "--root", f.cfg.Root, "--listen", strings.TrimPrefix(url, "https://"), "--ca", f.ca, "--cert", filepath.Join(dir, "server.crt"), "--key", filepath.Join(dir, "server.key"), "--worker", "local", "--validator", f.cfg.ValidatorArgv[0])
		process.Stdout = log
		process.Stderr = log
		if e = process.Start(); e != nil {
			t.Fatal(e)
		}
	}
	start()
	s := oneSubmission("process-loss")
	await(t, func() bool { return f.operator.Do(ctx, "POST", "/v1/runs", s, nil) == nil })
	poll := contracts.PollRequest{SchemaVersion: 1, RequestID: "reserve", WorkerID: "local", SessionID: "session"}
	var first contracts.Work
	if e := f.worker.Do(ctx, "POST", "/v1/poll", poll, &first); e != nil {
		t.Fatal(e)
	}
	stop()
	start()
	var again contracts.Work
	await(t, func() bool { return f.worker.Do(ctx, "POST", "/v1/poll", poll, &again) == nil })
	if first.Assignment.AssignmentID != again.Assignment.AssignmentID {
		t.Fatal("lost reservation across process loss")
	}
	candidate, data := singleCandidate(*again.Assignment, s.Units[0])
	ref := candidate.Files[0].Artifact
	if e := f.worker.Publish(ctx, ref.Digest, ref.Size, candidate.AssignmentID, bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	receipt := contracts.Receipt{SchemaVersion: 1, RequestID: "candidate", AssignmentID: candidate.AssignmentID, WorkerID: "local", SessionID: "session"}
	if e := f.worker.Do(ctx, "POST", "/v1/candidate", contracts.CandidateReport{Receipt: receipt, Candidate: candidate}, nil); e != nil {
		t.Fatal(e)
	}
	stop()
	start()
	await(t, func() bool {
		var status contracts.Status
		return f.operator.Do(ctx, "GET", "/v1/status?run=process-loss", nil, &status) == nil && status.Accepted == 1 && !status.Quiescent
	})
	receipt.RequestID = "final"
	if e := f.worker.Do(ctx, "POST", "/v1/final", contracts.FinalReport{Receipt: receipt, Completed: []string{candidate.UnitID}}, nil); e != nil {
		t.Fatal(e)
	}
	stop()
	start()
	await(t, func() bool {
		var status contracts.Status
		return f.operator.Do(ctx, "GET", "/v1/status?run=process-loss", nil, &status) == nil && status.Accepted == 1 && status.Quiescent
	})
}
