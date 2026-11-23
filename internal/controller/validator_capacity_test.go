package controller

import (
	"bytes"
	"context"
	"fmt"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"testing"
)

func TestOversizedPerPathValidatorWorkDoesNotStarveIndependentCandidate(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	ctx := context.Background()
	for _, run := range []string{"big-staging", "small-staging"} {
		if e := f.operator.Do(ctx, "POST", "/v1/runs", oneSubmission(run), nil); e != nil {
			t.Fatal(e)
		}
	}
	publish := func(client *agent.Client, worker, run string, big bool) {
		var work contracts.Work
		for tries := 0; tries < 4 && work.Assignment == nil; tries++ {
			if e := client.Do(ctx, "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: fmt.Sprintf("poll-%s-%d", worker, tries), WorkerID: worker, SessionID: "session"}, &work); e != nil {
				t.Fatal(e)
			}
		}
		if work.Assignment == nil || work.Assignment.RunID != run {
			t.Fatal("wrong assignment", work)
		}
		candidate, data := singleCandidate(*work.Assignment, oneSubmission(run).Units[0])
		if big {
			data = bytes.Repeat([]byte("x"), 8192)
			artifact := contracts.Artifact{Digest: contracts.Hash(data), Size: int64(len(data)), MediaType: "application/json"}
			candidate.Files = nil
			for i := 0; i < 1024; i++ {
				candidate.Files = append(candidate.Files, contracts.ResultFile{Path: fmt.Sprintf("result-%04d.json", i), Artifact: artifact})
			}
		}
		ref := candidate.Files[0].Artifact
		if e := client.Publish(ctx, ref.Digest, ref.Size, candidate.AssignmentID, bytes.NewReader(data)); e != nil {
			t.Fatal(e)
		}
		if e := client.Do(ctx, "POST", "/v1/candidate", contracts.CandidateReport{Receipt: contracts.Receipt{SchemaVersion: 1, RequestID: "candidate-" + worker, AssignmentID: candidate.AssignmentID, WorkerID: worker, SessionID: "session"}, Candidate: candidate}, nil); e != nil {
			t.Fatal(e)
		}
	}
	publish(f.worker, "local", "big-staging", true)
	publish(f.other, "other", "small-staging", false)
	if e := f.c.validateNext(ctx); e != nil {
		t.Fatal(e)
	}
	if e := f.c.validateNext(ctx); e != nil {
		t.Fatal(e)
	}
	small, e := f.c.Status(ctx, "small-staging")
	if e != nil || small.Accepted != 1 {
		t.Fatal("ineligible validator head starved independent result", small, e)
	}
	var attempts int
	var state, reason string
	if e = f.c.db.QueryRow("SELECT attempts,state,reason FROM candidates WHERE run_id='big-staging'").Scan(&attempts, &state, &reason); e != nil || attempts != 0 || state != "pending" || reason == "" {
		t.Fatal("capacity became a scientific/operational verdict", attempts, state, reason, e)
	}
}
