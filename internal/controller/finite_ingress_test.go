package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hermes/internal/contracts"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// This exercises the actual operator binary and mTLS HTTP route, not the local
// ImportFiniteDirectory convenience or a fake publication response.
func TestFiniteOperatorCLIIngressCreditAndIssuedTransportMinimum(t *testing.T) {
	f := newFiniteFixture(t)
	env := f.seal(t)
	ctx := context.Background()
	root := finiteTestOutput(t, env)
	cli := filepath.Join(t.TempDir(), "hermes")
	project, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	build := exec.Command("go", "build", "-o", cli, "./cmd/hermes")
	build.Dir = project
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, e := build.CombinedOutput(); e != nil {
		t.Fatal(e, string(out))
	}
	run := func() {
		t.Helper()
		cmd := exec.Command(cli, "--url", f.operator.URL, "--ca", f.ca, "--cert", f.cert, "--key", f.key, "finite-import", root)
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Fatal("operator CLI import", e, string(out))
		}
	}
	var policy contracts.ResourcePolicy
	tx, e := f.c.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	p, e := f.c.policy(tx)
	if e != nil {
		t.Fatal(e)
	}
	host, _ := approvalForHost(p, p.ControllerHostID)
	used, _, e := f.c.hostUsage(tx, p, host, -1)
	if e == nil {
		e = f.c.addOutputReservations(tx, &used)
	}
	tx.Rollback()
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(encoded(p), &policy); e != nil {
		t.Fatal(e)
	}
	policy.Revision++
	policy.Hosts[0].Limits.SpoolBytes = used.SpoolBytes + (64 << 10)
	ingressPolicyChange(t, f.c, policy)
	create := func(m contracts.FiniteMember, fill byte) (contracts.CandidateReport, string) {
		t.Helper()
		report, _ := finiteTestCandidate(t, root, m)
		oldDigest, _ := contracts.Digest(report.Candidate)
		if e := os.Remove(filepath.Join(root, "candidates", oldDigest+".json")); e != nil {
			t.Fatal(e)
		}
		data := bytes.Repeat([]byte{fill}, 256<<10)
		ref := contracts.Artifact{Digest: contracts.Hash(data), Size: int64(len(data)), MediaType: "application/octet-stream"}
		report.Candidate.Files[0].Artifact = ref
		digest, _ := contracts.Digest(report.Candidate)
		finiteTestJSON(t, filepath.Join(root, "candidates", digest+".json"), report)
		if e := os.WriteFile(filepath.Join(root, "blobs", ref.Digest), data, 0600); e != nil {
			t.Fatal(e)
		}
		return report, ref.Digest
	}
	first, digest := create(env.Bundle.Members[0], 'a')
	data := bytes.Repeat([]byte{'a'}, 256<<10)
	// Generic operator ?assignment remains deliberately UNBOUND: the current
	// tight host has less spare than this blob beyond already-reserved output.
	if e = f.operator.Publish(ctx, digest, int64(len(data)), first.AssignmentID, bytes.NewReader(data)); e == nil {
		t.Fatal("generic operator upload borrowed assignment credit")
	}
	run()
	var received int64
	var links int
	if e = f.c.db.QueryRow("SELECT bytes FROM assignment_output WHERE assignment_id=?", first.AssignmentID).Scan(&received); e != nil || received != int64(len(data)) {
		t.Fatal("HTTP source output credit missing", received, e)
	}
	if e = f.c.db.QueryRow("SELECT count(*) FROM assignment_output_blobs WHERE assignment_id=? AND digest=?", first.AssignmentID, digest).Scan(&links); e != nil || links != 1 {
		t.Fatal("HTTP output source link missing", links, e)
	}
	// Restore only the original approved ceiling, then shrink CURRENT transfer
	// capacity below the payload. The immutable issued member keeps the original
	// 1MiB minimum through ordinary bound transfer admission.
	policy.Revision++
	policy.Hosts[0].Limits.SpoolBytes = p.Hosts[0].Limits.SpoolBytes
	policy.TransferReservation.SpoolBytes = 128 << 10
	ingressPolicyChange(t, f.c, policy)
	second, secondDigest := create(env.Bundle.Members[1], 'b')
	data = bytes.Repeat([]byte{'b'}, 256<<10)
	if e = f.operator.Publish(ctx, secondDigest, int64(len(data)), second.AssignmentID, bytes.NewReader(data)); e == nil {
		t.Fatal("generic operator query borrowed issued transport minimum")
	}
	run()
	if e = f.c.db.QueryRow("SELECT bytes FROM assignment_output WHERE assignment_id=?", second.AssignmentID).Scan(&received); e != nil || received != int64(len(data)) {
		t.Fatal("old-issued transport delivery lost source accounting", received, e)
	}
	if e = f.c.db.QueryRow("SELECT count(*) FROM assignment_output_blobs").Scan(&links); e != nil || links != 2 {
		t.Fatal("redelivery duplicated or lost source links", links, e)
	}
	for _, m := range env.Bundle.Members {
		var manifest []byte
		if e = f.c.db.QueryRow("SELECT manifest FROM assignments WHERE id=?", m.Work.Assignment.AssignmentID).Scan(&manifest); e != nil || !bytes.Equal(manifest, encoded(*m.Work.Assignment)) {
			t.Fatal("issued assignment mutated", e)
		}
	}
	if ingressCount(t, f.c, "acceptances") != 0 || ingressCount(t, f.c, "execution_reports") != 0 {
		t.Fatal("raw HTTP transport inferred science or lifecycle")
	}
}

func TestFiniteOperatorArtifactBindingRejectedBeforeIngress(t *testing.T) {
	f := newFiniteFixture(t)
	env := f.seal(t)
	m := env.Bundle.Members[0]
	body := []byte("unpublished synthetic bytes")
	digest := contracts.Hash(body)
	for _, scope := range []struct{ bundle, member, assignment string }{{contracts.Hash([]byte("unknown")), m.AssignmentDigest, m.Work.Assignment.AssignmentID}, {env.BundleDigest, contracts.Hash([]byte("foreign-member")), m.Work.Assignment.AssignmentID}, {env.BundleDigest, m.AssignmentDigest, "unknown-online-assignment"}} {
		if e := f.c.PublishFiniteArtifact(context.Background(), scope.bundle, scope.member, scope.assignment, digest, int64(len(body)), ingressNeverRead{}); e == nil {
			t.Fatal("unsealed scope admitted")
		}
		path := "/v1/finite/artifacts/" + digest + "?bundle=" + scope.bundle + "&member=" + scope.member + "&assignment=" + scope.assignment
		request, e := http.NewRequestWithContext(context.Background(), "PUT", f.operator.URL+path, bytes.NewReader(body))
		if e != nil {
			t.Fatal(e)
		}
		request.Header.Set("X-Artifact-Size", fmt.Sprint(len(body)))
		response, e := f.operator.HTTP.Do(request)
		if e != nil {
			t.Fatal(e)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatal("invalid HTTP sealed scope", response.StatusCode)
		}
	}
	if ingressCount(t, f.c, "controller_ingress") != 0 || ingressCount(t, f.c, "assignment_output_blobs") != 0 {
		t.Fatal("unauthorized scope created ingress/credit")
	}
}
