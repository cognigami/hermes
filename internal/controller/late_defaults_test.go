package controller

import (
	"bytes"
	"context"
	"hermes/internal/contracts"
	"testing"
)

func TestIssuedCandidateRawACKSurvivesChangedControllerDefaults(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	ctx := context.Background()
	s := oneSubmission("old-defaults")
	if e := f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	var work contracts.Work
	if e := f.worker.Do(ctx, "POST", "/v1/poll", contracts.PollRequest{SchemaVersion: 1, RequestID: "poll", WorkerID: "local", SessionID: "session"}, &work); e != nil {
		t.Fatal(e)
	}
	a := *work.Assignment
	candidate, data := singleCandidate(a, s.Units[0])
	ref := candidate.Files[0].Artifact
	f.c.cfg.Runtime = "new-runtime"
	f.c.cfg.Profile = "new-profile"
	if e := f.worker.Publish(ctx, ref.Digest, ref.Size, a.AssignmentID, bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	report := contracts.CandidateReport{Receipt: contracts.Receipt{SchemaVersion: 1, RequestID: "raw", AssignmentID: a.AssignmentID, WorkerID: a.WorkerID, SessionID: a.SessionID}, Candidate: candidate}
	for _, kind := range []string{"unit", "spec", "session", "attempt"} {
		bad := report
		bad.RequestID = "bad-" + kind
		switch kind {
		case "unit":
			bad.Candidate.UnitID = "wrong"
		case "spec":
			bad.Candidate.SpecDigest = contracts.Hash([]byte("wrong"))
		case "session":
			bad.SessionID = "wrong"
		case "attempt":
			bad.Candidate.AttemptID = "wrong"
		}
		if e := f.worker.Do(ctx, "POST", "/v1/candidate", bad, nil); e == nil {
			t.Fatal("unbound late candidate accepted", kind)
		}
	}
	var ack contracts.Acknowledgment
	if e := f.worker.Do(ctx, "POST", "/v1/candidate", report, &ack); e != nil || ack.RawPublished == "" {
		t.Fatal("current defaults discarded issued raw result", ack, e)
	}
	var owners int
	if e := f.c.db.QueryRow("SELECT count(*) FROM owners WHERE assignment_id=?", a.AssignmentID).Scan(&owners); e != nil || owners != 1 {
		t.Fatal("raw ACK released ownership", owners, e)
	}
	if e := f.c.validateNext(ctx); e != nil {
		t.Fatal(e)
	}
	status, e := f.c.Status(ctx, s.RunID)
	if e != nil || status.Accepted != 0 || status.ValidationFailures != 1 {
		t.Fatal("configuration mismatch became domain decision", status, e)
	}
}
