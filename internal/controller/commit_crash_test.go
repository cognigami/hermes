package controller

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"hermes/internal/contracts"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This test-only entrypoint hosts the actual authority and HTTP/validator paths.
// There are no production environment-variable fault controls.
func TestMetadataCrashHelper(t *testing.T) {
	if os.Getenv("HERMES_TEST_AUTHORITY") != "1" {
		return
	}
	cfg := Config{ResourcePolicy: fixtureResourcePolicy([]string{"local", "other"}), MaxConcurrent: 1, Root: os.Getenv("HERMES_TEST_ROOT"), Workers: []string{"local"}, Profile: "synthetic-local", Runtime: "synthetic-v1", Validator: "synthetic-v1", ValidatorArgv: []string{os.Getenv("HERMES_TEST_VALIDATOR")}, MaxArtifactBytes: 1 << 20}
	c, e := Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	seedFixtureInventory(t, c)
	operation, phase := os.Getenv("HERMES_TEST_OPERATION"), os.Getenv("HERMES_TEST_PHASE")
	thirdAttempt := operation == "acceptance" && phase == "before"
	validationReady := make(chan struct{})
	if !thirdAttempt {
		close(validationReady)
	}
	c.testCommitBoundary = func(op string, committed bool) error {
		if thirdAttempt && op == "candidate" && committed {
			if _, err := c.db.Exec("UPDATE candidates SET attempts=2"); err != nil {
				panic(err)
			}
			close(validationReady)
		}
		if op == operation && ((phase == "after") == committed) {
			os.Exit(91)
		}
		return nil
	}
	ca, e := os.ReadFile(os.Getenv("HERMES_TEST_CA"))
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("CA")
	}
	go func() {
		<-validationReady
		if e := c.RunValidators(context.Background()); e != nil {
			panic(e)
		}
	}()
	s := &http.Server{Addr: os.Getenv("HERMES_TEST_LISTEN"), Handler: c.Handler(), ReadHeaderTimeout: time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}}
	if e = s.ListenAndServeTLS(os.Getenv("HERMES_TEST_CERT"), os.Getenv("HERMES_TEST_KEY")); e != nil {
		t.Fatal(e)
	}
}
func TestMetadataCommitProcessCrashes(t *testing.T) {
	f := setup(t)
	url := f.server.URL
	f.stop()
	binary, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	dir := filepath.Dir(f.app)
	for _, operation := range []string{"assignment", "candidate", "acceptance"} {
		for _, phase := range []string{"before", "after"} {
			t.Run(operation+"-"+phase, func(t *testing.T) {
				root := filepath.Join(t.TempDir(), "authority")
				cfg := f.cfg
				cfg.Root = root
				var process *exec.Cmd
				var done chan error
				stop := func() {
					if process != nil {
						process.Process.Kill()
						<-done
						process = nil
					}
				}
				defer stop()
				start := func(fault bool) {
					log, e := os.CreateTemp(dir, "commit-crash-log-")
					if e != nil {
						t.Fatal(e)
					}
					t.Cleanup(func() { log.Close() })
					process = exec.Command(binary, "-test.run=^TestMetadataCrashHelper$")
					op := ""
					if fault {
						op = operation
					}
					process.Env = append(os.Environ(), "HERMES_TEST_AUTHORITY=1", "HERMES_TEST_ROOT="+root, "HERMES_TEST_VALIDATOR="+f.cfg.ValidatorArgv[0], "HERMES_TEST_CA="+f.ca, "HERMES_TEST_CERT="+filepath.Join(dir, "server.crt"), "HERMES_TEST_KEY="+filepath.Join(dir, "server.key"), "HERMES_TEST_LISTEN="+strings.TrimPrefix(url, "https://"), "HERMES_TEST_OPERATION="+op, "HERMES_TEST_PHASE="+phase)
					process.Stdout = log
					process.Stderr = log
					if e = process.Start(); e != nil {
						t.Fatal(e)
					}
					done = make(chan error, 1)
					current, result := process, done
					go func() { result <- current.Wait() }()
				}
				start(true)
				ctx := context.Background()
				s := oneSubmission("crash")
				dep := s.Units[0]
				dep.UnitID = "dependent"
				dep.SpecDigest = contracts.Hash([]byte("dependent"))
				dep.Dependencies = []string{s.Units[0].UnitID}
				s.Units = append(s.Units, dep)
				s.Packages = append(s.Packages, contracts.Package{SchemaVersion: 1, PackageID: "second", Units: []contracts.UnitRef{{UnitID: dep.UnitID, SpecDigest: dep.SpecDigest}}})
				await(t, func() bool { return f.operator.Do(ctx, "POST", "/v1/runs", s, nil) == nil })
				p := contracts.PollRequest{SchemaVersion: 1, RequestID: "reserve", WorkerID: "local", SessionID: "session"}
				var work contracts.Work
				pollErr := f.worker.Do(ctx, "POST", "/v1/poll", p, &work)
				var report contracts.CandidateReport
				if operation != "assignment" {
					if pollErr != nil {
						t.Fatal(pollErr)
					}
					c, data := singleCandidate(*work.Assignment, s.Units[0])
					ref := c.Files[0].Artifact
					if e = f.worker.Publish(ctx, ref.Digest, ref.Size, c.AssignmentID, bytes.NewReader(data)); e != nil {
						t.Fatal(e)
					}
					report = contracts.CandidateReport{Receipt: contracts.Receipt{SchemaVersion: 1, RequestID: "candidate", AssignmentID: c.AssignmentID, WorkerID: "local", SessionID: "session"}, Candidate: c}
					_ = f.worker.Do(ctx, "POST", "/v1/candidate", report, nil)
				}
				select {
				case err := <-done:
					process = nil
					exit, ok := err.(*exec.ExitError)
					if !ok || exit.ExitCode() != 91 {
						t.Fatal("fault did not terminate at commit boundary", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("commit boundary did not fire")
				}
				c, e := Open(cfg)
				if e != nil {
					t.Fatal(e)
				}
				table := "assignments"
				if operation == "candidate" {
					table = "candidates"
				}
				if operation == "acceptance" {
					table = "acceptances"
				}
				var count int
				e = c.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count)
				if e != nil {
					c.Close()
					t.Fatal(e)
				}
				want := 0
				if phase == "after" {
					want = 1
				}
				if count != want {
					c.Close()
					t.Fatalf("%s %s commit persisted %d, want %d", operation, phase, count, want)
				}
				if operation == "acceptance" {
					if phase == "before" {
						var attempts int
						var state string
						if err := c.db.QueryRow("SELECT attempts,state FROM candidates").Scan(&attempts, &state); err != nil {
							c.Close()
							t.Fatal(err)
						}
						if attempts != 2 || state != "pending" {
							c.Close()
							t.Fatal("interrupted third attempt consumed failure budget", attempts, state)
						}
					}
					var ready int
					if e = c.db.QueryRow("SELECT ready FROM run_units WHERE unit_id='dependent'").Scan(&ready); e != nil {
						c.Close()
						t.Fatal(e)
					}
					if ready != want {
						c.Close()
						t.Fatal("readiness not atomic with acceptance", ready, want)
					}
				}
				c.Close()
				// Recover through the same authority, replaying the original request after
				// both rollback and committed-before-response crashes.
				start(false)
				if operation == "assignment" {
					await(t, func() bool { return f.worker.Do(ctx, "POST", "/v1/poll", p, &work) == nil })
					if work.Assignment == nil {
						t.Fatal("reservation lost")
					}
				} else {
					await(t, func() bool { return f.worker.Do(ctx, "POST", "/v1/candidate", report, nil) == nil })
					await(t, func() bool {
						var status contracts.Status
						return f.operator.Do(ctx, "GET", "/v1/status?run=crash", nil, &status) == nil && status.Accepted == 1
					})
				}
			})
		}
	}
}
