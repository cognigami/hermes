package agent

import (
	"context"
	"hermes/internal/contracts"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testWork() contracts.Work {
	u := contracts.Unit{SchemaVersion: 1, UnitID: "u", SpecDigest: contracts.Hash([]byte("spec")), Operation: "synthetic-v1", Runtime: "synthetic-v1", Validator: "synthetic-v1"}
	p := contracts.Package{SchemaVersion: 1, PackageID: "p", Units: []contracts.UnitRef{{UnitID: u.UnitID, SpecDigest: u.SpecDigest}}}
	digest, _ := contracts.Digest(p)
	now := time.Now()
	a := contracts.Assignment{SchemaVersion: 1, AssignmentID: "assignment-a", RunID: "run", WorkerID: "worker", SessionID: "session", PackageDigest: digest, Units: p.Units, Runtime: "synthetic-v1", Profile: "synthetic-local", PolicyRevision: 1, StopNewUnitsAt: now.Add(time.Minute), TerminateAt: now.Add(2 * time.Minute), HandoffDeadline: now.Add(3 * time.Minute)}
	return contracts.Work{SchemaVersion: 1, Assignment: &a, Package: &p, Units: []contracts.Unit{u}}
}
func TestWorkBinding(t *testing.T) {
	for _, name := range []string{"valid", "package", "unit", "runtime", "count"} {
		t.Run(name, func(t *testing.T) {
			w := testWork()
			switch name {
			case "package":
				w.Package.PackageID = "changed"
			case "unit":
				w.Units[0].SpecDigest = contracts.Hash([]byte("wrong"))
			case "runtime":
				w.Units[0].Runtime = "wrong"
			case "count":
				w.Units = nil
			}
			e := validateWork(w)
			if (e == nil) != (name == "valid") {
				t.Fatal(e)
			}
		})
	}
}

type testTransport func(*http.Request) (*http.Response, error)

func (f testTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func uncertainControllerClient() *Client {
	return &Client{URL: "https://synthetic.invalid", HTTP: &http.Client{Transport: testTransport(func(r *http.Request) (*http.Response, error) {
		body := `{"schema_version":1,"received":true}`
		if r.URL.Path == "/v1/reconcile" {
			body = `{"schema_version":1,"state":"uncertain"}`
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}
}
func TestRestartPreparationIsUncertain(t *testing.T) {
	root := t.TempDir()
	j := journal{Work: testWork(), ProcessStarted: true}
	if e := durable(filepath.Join(root, "identity.json"), struct{ Worker, Session string }{"worker", "session"}); e != nil {
		t.Fatal(e)
	}
	if e := durable(filepath.Join(root, "journal.json"), j); e != nil {
		t.Fatal(e)
	}
	e := Run(context.Background(), Config{Root: root, WorkerID: "worker", SessionID: "session", Runtime: "synthetic-v1", Profile: "synthetic-local", RuntimeArgv: []string{filepath.Join(root, "must-not-execute")}, Client: uncertainControllerClient(), MaxArtifactBytes: 1 << 20})
	if e == nil || !strings.Contains(e.Error(), "ownership uncertain") {
		t.Fatal(e)
	}
	var loaded journal
	b, e := os.ReadFile(filepath.Join(root, "active", contracts.Hash([]byte(j.Work.Assignment.AssignmentID)), "journal.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = contracts.Decode(strings.NewReader(string(b)), &loaded); e != nil {
		t.Fatal(e)
	}
	if len(loaded.Started) != 0 || !loaded.ProcessStarted {
		t.Fatal("journal changed or granted a unit")
	}
}
