package packaging

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"golang.org/x/sys/unix"
	"hermes/internal/contracts"
	"hermes/testdata/synthetic"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func trialHelper(t *testing.T) string {
	t.Helper()
	// Track testdata inputs in Go's test cache; subprocess builds alone do not.
	for _, name := range []string{"main.go", "run.sh"} {
		b, e := os.ReadFile(filepath.Join("testdata", "service-trial", name))
		if e != nil || len(b) == 0 {
			t.Fatal("missing fixture source", name, e)
		}
	}
	path := filepath.Join(t.TempDir(), "trial-helper")
	c := exec.Command("go", "build", "-o", path, "./testdata/service-trial")
	c.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, e := c.CombinedOutput(); e != nil {
		t.Fatal(string(out), e)
	}
	return path
}
func TestServiceTrialPreparesOnlySyntheticInputs(t *testing.T) {
	helper := trialHelper(t)
	root := t.TempDir()
	if out, e := exec.Command(helper, "prepare-inputs", root).CombinedOutput(); e != nil {
		t.Fatal(string(out), e)
	}
	caBytes, e := os.ReadFile(filepath.Join(root, "pki", "ca.pem"))
	if e != nil {
		t.Fatal(e)
	}
	block, _ := pem.Decode(caBytes)
	if block == nil {
		t.Fatal("missing PEM")
	}
	ca, e := x509.ParseCertificate(block.Bytes)
	if e != nil || !ca.IsCA {
		t.Fatal(ca, e)
	}
	for _, name := range []string{"server", "operator", "worker"} {
		pair, e := tls.LoadX509KeyPair(filepath.Join(root, "pki", name+".pem"), filepath.Join(root, "pki", name+"-key.pem"))
		if e != nil {
			t.Fatal(e)
		}
		c, e := x509.ParseCertificate(pair.Certificate[0])
		if e != nil {
			t.Fatal(e)
		}
		if e = c.CheckSignatureFrom(ca); e != nil {
			t.Fatal(e)
		}
		if name == "server" {
			if e = c.VerifyHostname("127.0.0.1"); e != nil {
				t.Fatal(e)
			}
		}
		if name == "worker" && c.Subject.CommonName != "worker:service-trial" {
			t.Fatal(c.Subject)
		}
	}
	b, e := os.ReadFile(filepath.Join(root, "submission.json"))
	if e != nil {
		t.Fatal(e)
	}
	var s contracts.Submission
	if e = json.Unmarshal(b, &s); e != nil {
		t.Fatal(e)
	}
	if e = contracts.ValidateGraph(s.Units); e != nil {
		t.Fatal(e)
	}
	if len(s.Units) != 3 || len(s.Packages) != 2 || len(s.Units[2].Dependencies) != 2 {
		t.Fatal(s)
	}
	if out, e := exec.Command("sh", "-n", "testdata/service-trial/run.sh").CombinedOutput(); e != nil {
		t.Fatal(string(out), e)
	}
}
func TestServiceTrialLeafValidatorAndLifetimeLock(t *testing.T) {
	helper := trialHelper(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "trial-validator")
	b, e := os.ReadFile(helper)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, b, 0700); e != nil {
		t.Fatal(e)
	}
	lockPath := filepath.Join(dir, "validator.lock")
	if e = os.WriteFile(lockPath, nil, 0600); e != nil {
		t.Fatal(e)
	}
	delayPath := filepath.Join(dir, "validator-delay-ms")
	if e = os.WriteFile(delayPath, []byte("5000"), 0600); e != nil {
		t.Fatal(e)
	}
	u := contracts.Unit{SchemaVersion: 1, UnitID: "leaf", SpecDigest: contracts.Hash([]byte("leaf")), Operation: "synthetic-v1", Runtime: "synthetic-v1", Validator: "synthetic-v1"}
	data, e := json.Marshal(synthetic.Result{UnitID: u.UnitID, SpecDigest: u.SpecDigest, Value: synthetic.Expected(u.UnitID, u.SpecDigest)})
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "result.json"), data, 0600); e != nil {
		t.Fatal(e)
	}
	candidate := contracts.Candidate{SchemaVersion: 1, UnitID: u.UnitID, SpecDigest: u.SpecDigest, AssignmentID: "a", AttemptID: "a", Outcome: "complete", Files: []contracts.ResultFile{{Path: "result.json", Artifact: contracts.Artifact{Digest: contracts.Hash(data), Size: int64(len(data)), MediaType: "application/json"}}}}
	for name, value := range map[string]any{"unit.json": u, "candidate.json": candidate} {
		b, e = json.Marshal(value)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, name), b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	args := []string{"--manifest", filepath.Join(dir, "candidate.json"), "--root", dir, "--unit", filepath.Join(dir, "unit.json")}
	cmd := exec.Command(path, args...)
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	lock, e := os.OpenFile(lockPath, os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer lock.Close()
	deadline := time.Now().Add(3 * time.Second)
	held := false
	for time.Now().Before(deadline) {
		e = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if e != nil {
			held = true
			break
		}
		unix.Flock(int(lock.Fd()), unix.LOCK_UN)
		time.Sleep(10 * time.Millisecond)
	}
	if !held {
		t.Fatal("validator never held lifetime lock")
	}
	if e = cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	if e = cmd.Wait(); e == nil {
		t.Fatal("killed leaf unexpectedly succeeded")
	}
	if e = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		t.Fatal("exit did not release lock", e)
	}
	unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	if e = os.WriteFile(delayPath, []byte("0"), 0600); e != nil {
		t.Fatal(e)
	}
	out, e := exec.Command(path, args...).CombinedOutput()
	if e != nil {
		t.Fatal(string(out), e)
	}
	var verdict contracts.Verdict
	if e = json.Unmarshal(out, &verdict); e != nil || !verdict.Valid {
		t.Fatal(string(out), e)
	}
	if e = os.WriteFile(filepath.Join(dir, "result.json"), []byte("corrupt"), 0600); e != nil {
		t.Fatal(e)
	}
	out, e = exec.Command(path, args...).CombinedOutput()
	if e == nil || !strings.Contains(string(out), "bytes mismatch") {
		t.Fatal(string(out), e)
	}
}
