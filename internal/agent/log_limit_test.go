package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLogBufferCopyEnforcesBound(t *testing.T) {
	buffer := &logBuffer{}
	n, e := io.Copy(buffer, bytes.NewReader(bytes.Repeat([]byte("x"), contracts.MaxMessageBytes+1)))
	if e == nil || n > contracts.MaxMessageBytes || buffer.buffer.Len() > contracts.MaxMessageBytes {
		t.Fatal("io.Copy bypassed log limit", n, e)
	}
}
func TestBoundedLogApplicationHelper(t *testing.T) {
	if os.Getenv("HERMES_TEST_LOG_LEAF") == "" {
		return
	}
	paths := map[string]string{}
	args := flag.Args()
	for i := 0; i+1 < len(args); i += 2 {
		paths[args[i]] = args[i+1]
	}
	file, e := os.Open(paths["--manifest"])
	if e != nil {
		t.Fatal(e)
	}
	var w contracts.Work
	e = contracts.Decode(file, &w)
	file.Close()
	if e != nil {
		t.Fatal(e)
	}
	reader := contracts.MessageReader(os.Stdin)
	if e = contracts.WriteMessage(os.Stdout, contracts.Message{SchemaVersion: 1, Type: "ready"}); e != nil {
		t.Fatal(e)
	}
	grant, e := contracts.ReadMessage(reader)
	if e != nil {
		t.Fatal(e)
	}
	u := w.Units[0]
	data := []byte("retained synthetic bytes")
	candidate := contracts.Candidate{SchemaVersion: 1, UnitID: u.UnitID, SpecDigest: u.SpecDigest, AssignmentID: w.Assignment.AssignmentID, AttemptID: w.Assignment.AssignmentID, Outcome: "complete", Files: []contracts.ResultFile{{Path: "result.json", Artifact: contracts.Artifact{Digest: contracts.Hash(data), Size: int64(len(data)), MediaType: "application/json"}}}}
	encoded, e := json.Marshal(candidate)
	if e != nil {
		t.Fatal(e)
	}
	for path, content := range map[string][]byte{"candidate.json": encoded, "result.json": data} {
		if e = os.WriteFile(filepath.Join(paths["--output"], path), content, 0600); e != nil {
			t.Fatal(e)
		}
	}
	if e = contracts.WriteMessage(os.Stdout, contracts.Message{SchemaVersion: 1, Type: "unit_result", UnitID: grant.UnitID, ManifestPath: "candidate.json"}); e != nil {
		t.Fatal(e)
	}
	if _, e = contracts.ReadMessage(reader); e != nil {
		t.Fatal(e)
	}
	if _, e = contracts.ReadMessage(reader); e != nil {
		t.Fatal(e)
	}
	if e = contracts.WriteMessage(os.Stdout, contracts.Message{SchemaVersion: 1, Type: "finished"}); e != nil {
		t.Fatal(e)
	}
	os.Stderr.Write(bytes.Repeat([]byte("x"), contracts.MaxMessageBytes+1))
}
func TestObservedWaitLogCopyErrorIsQuiescentAndRetainsPrefix(t *testing.T) {
	binary, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	root := t.TempDir()
	store, e := artifacts.New(filepath.Join(root, "blobs"), 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	w := testWork()
	now := time.Now()
	w.Assignment.StopNewUnitsAt = now.Add(2 * time.Second)
	w.Assignment.TerminateAt = now.Add(3 * time.Second)
	w.Assignment.HandoffDeadline = now.Add(4 * time.Second)
	r := runner{cfg: Config{Root: root, RuntimeArgv: []string{binary, "-test.run=^TestBoundedLogApplicationHelper$", "--"}}, testEnvironment: []string{"HERMES_TEST_LOG_LEAF=1"}, j: journal{Work: w, ReceiptWall: now.UTC()}, store: store, received: now}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	e = r.execute(ctx)
	if e == nil || !strings.Contains(e.Error(), "application log limit") {
		t.Fatal("copy failure hidden", e)
	}
	if !r.j.Quiescent || r.j.Uncertain || !r.terminalPersisted || len(r.j.Candidates) != 1 {
		t.Fatal("known Wait exit lost proof or retained prefix", r.j, e)
	}
}
