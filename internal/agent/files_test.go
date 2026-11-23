package agent

import (
	"context"
	"encoding/json"
	"flag"
	"golang.org/x/sys/unix"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A test-only leaf application deliberately advertises a FIFO in either output
// position. It waits afterward so only prompt file rejection can finish the test.
func TestFIFOApplicationHelper(t *testing.T) {
	mode := os.Getenv("HERMES_TEST_FIFO")
	if mode == "" {
		return
	}
	args := flag.Args()
	paths := map[string]string{}
	for i := 0; i+1 < len(args); i += 2 {
		paths[args[i]] = args[i+1]
	}
	var work contracts.Work
	f, e := os.Open(paths["--manifest"])
	if e != nil {
		t.Fatal(e)
	}
	e = contracts.Decode(f, &work)
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	if e = contracts.WriteMessage(os.Stdout, contracts.Message{SchemaVersion: 1, Type: "ready"}); e != nil {
		t.Fatal(e)
	}
	grant, e := contracts.ReadMessage(contracts.MessageReader(os.Stdin))
	if e != nil || grant.Type != "execute_unit" {
		t.Fatal(grant, e)
	}
	out := paths["--output"]
	name := "candidate.json"
	if mode == "manifest" {
		if e = unix.Mkfifo(filepath.Join(out, name), 0600); e != nil {
			t.Fatal(e)
		}
	} else {
		if e = unix.Mkfifo(filepath.Join(out, "result.json"), 0600); e != nil {
			t.Fatal(e)
		}
		u := work.Units[0]
		c := contracts.Candidate{SchemaVersion: 1, UnitID: u.UnitID, SpecDigest: u.SpecDigest, AssignmentID: work.Assignment.AssignmentID, AttemptID: work.Assignment.AssignmentID, Outcome: "complete", Files: []contracts.ResultFile{{Path: "result.json", Artifact: contracts.Artifact{Digest: contracts.Hash(nil), Size: 0, MediaType: "application/json"}}}}
		b, e := json.Marshal(c)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(out, name), b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	if e = contracts.WriteMessage(os.Stdout, contracts.Message{SchemaVersion: 1, Type: "unit_result", UnitID: grant.UnitID, ManifestPath: name}); e != nil {
		t.Fatal(e)
	}
	time.Sleep(30 * time.Second)
}
func TestFIFOOutputsFailWithoutBlocking(t *testing.T) {
	binary, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"manifest", "result"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("HERMES_TEST_FIFO", mode)
			root := t.TempDir()
			store, e := artifacts.New(filepath.Join(root, "blobs"), 1<<20)
			if e != nil {
				t.Fatal(e)
			}
			r := runner{testEnvironment: []string{"HERMES_TEST_FIFO=" + mode}, cfg: Config{Root: root, RuntimeArgv: []string{binary, "-test.run=^TestFIFOApplicationHelper$", "--"}}, j: journal{Work: testWork()}, store: store}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- r.execute(ctx) }()
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), "not a regular file") {
					t.Fatal("FIFO was not explicitly rejected", err)
				}
			case <-ctx.Done():
				t.Fatal("FIFO open/read blocked beyond cancellation")
			}
		})
	}
}
