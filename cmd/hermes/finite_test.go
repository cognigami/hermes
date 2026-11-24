package main

import (
	"bytes"
	"context"
	"encoding/json"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func finiteCLIEnvelope(t *testing.T) contracts.FiniteBundleEnvelope {
	t.Helper()
	now := time.Now().UTC()
	allocation := contracts.Allocation{Resources: contracts.ResourceVector{CPUMilli: 250, MemoryBytes: 64 << 20, SpoolBytes: 8 << 20}}
	budget := contracts.HostBudget{SchemaVersion: 1, PolicyRevision: 1, Approval: contracts.HostApproval{HostID: "host", WorkerID: "worker", MaxAssignments: 2, Limits: contracts.ResourceVector{CPUMilli: 4000, MemoryBytes: 512 << 20, SpoolBytes: 64 << 20}}, Profiles: []contracts.ResourceProfile{{ID: "small", Conservative: true, Allocation: allocation}}}
	executable := contracts.Artifact{Digest: contracts.Hash(nil), MediaType: "application/octet-stream"}
	b := contracts.FiniteBundle{SchemaVersion: 1, BundleID: "bundle", WorkerID: "worker", SessionID: "session", HostID: "host", ExportRevision: 1, RuntimeRefs: []contracts.FiniteRuntimeRef{{Runtime: "synthetic-v1", GOOS: "darwin", GOARCH: "amd64", ISABaseline: "v1", Executable: executable}}, Blobs: []contracts.Artifact{executable}}
	for _, id := range []string{"one", "two"} {
		u := contracts.Unit{SchemaVersion: 1, UnitID: id, SpecDigest: contracts.Hash([]byte(id)), Operation: "synthetic-v1", Runtime: "synthetic-v1", Validator: "synthetic-v1"}
		ref := contracts.UnitRef{UnitID: id, SpecDigest: u.SpecDigest}
		p := contracts.Package{SchemaVersion: 1, PackageID: "package-" + id, Units: []contracts.UnitRef{ref}}
		pd, _ := contracts.Digest(p)
		a := contracts.Assignment{SchemaVersion: 1, AssignmentID: "assignment-" + id, RunID: "run", WorkerID: b.WorkerID, SessionID: b.SessionID, PackageDigest: pd, Units: []contracts.UnitRef{ref}, Runtime: u.Runtime, Profile: "small", PolicyRevision: 1, Allocation: allocation, StopNewUnitsAt: now.Add(time.Minute), TerminateAt: now.Add(2 * time.Minute), HandoffDeadline: now.Add(3 * time.Minute)}
		digest, _ := contracts.Digest(a)
		b.Members = append(b.Members, contracts.FiniteMember{AssignmentDigest: digest, Work: contracts.Work{SchemaVersion: 1, Assignment: &a, Package: &p, Units: []contracts.Unit{u}, Budget: &budget}})
	}
	digest, _ := contracts.Digest(b)
	env := contracts.FiniteBundleEnvelope{BundleDigest: digest, Bundle: b}
	if e := env.Validate(); e != nil {
		t.Fatal(e)
	}
	return env
}
func finiteCLIJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
}
func finiteCLIRequest(env contracts.FiniteBundleEnvelope) contracts.FiniteExportRequest {
	a := env.Bundle.Members[0].Work.Assignment
	return contracts.FiniteExportRequest{SchemaVersion: 1, RequestID: "request", BundleID: env.Bundle.BundleID, WorkerID: env.Bundle.WorkerID, SessionID: env.Bundle.SessionID, HostID: env.Bundle.HostID, MemberLimit: len(env.Bundle.Members), StopNewUnitsAt: a.StopNewUnitsAt, TerminateAt: a.TerminateAt, HandoffDeadline: a.HandoffDeadline}
}
func TestFiniteExportRejectsWrongValidReplyBeforeDestinationWrite(t *testing.T) {
	for _, kind := range []string{"bundle", "worker", "session", "host", "member-limit", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			env := finiteCLIEnvelope(t)
			request := finiteCLIRequest(env)
			switch kind {
			case "bundle":
				request.BundleID = "different"
			case "worker":
				request.WorkerID = "different"
			case "session":
				request.SessionID = "different"
			case "host":
				request.HostID = "different"
			case "member-limit":
				request.MemberLimit = 1
			case "deadline":
				request.StopNewUnitsAt = request.StopNewUnitsAt.Add(-time.Second)
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.URL.Path != "/v1/finite/export" {
					t.Error("payload requested before reply scope validation", r.URL.Path)
					http.Error(w, "unexpected", 500)
					return
				}
				json.NewEncoder(w).Encode(env)
			}))
			defer server.Close()
			client := &agent.Client{URL: server.URL, HTTP: server.Client()}
			dir := t.TempDir()
			file := filepath.Join(dir, "request.json")
			finiteCLIJSON(t, file, request)
			destination := filepath.Join(dir, "not-created")
			var out bytes.Buffer
			handled, e := runFiniteCommand(context.Background(), client, []string{"finite-export", file, destination}, &out)
			if !handled || e == nil {
				t.Fatal("wrong valid reply accepted", e)
			}
			if _, e = os.Lstat(destination); !os.IsNotExist(e) {
				t.Fatal("protocol error wrote destination", e)
			}
		})
	}
}
func TestFiniteCLIImportPersistsPartialAckAndMonotonicReplay(t *testing.T) {
	env := finiteCLIEnvelope(t)
	root := t.TempDir()
	for _, dir := range []string{"blobs", "candidates"} {
		os.Mkdir(filepath.Join(root, dir), 0700)
	}
	finiteCLIJSON(t, filepath.Join(root, "bundle.json"), env)
	type record struct {
		report                         contracts.CandidateReport
		digest, blob, assignmentDigest string
	}
	var records []record
	for _, m := range env.Bundle.Members {
		a := *m.Work.Assignment
		u := m.Work.Units[0]
		data := []byte("synthetic-output-" + u.UnitID)
		file := contracts.Artifact{Digest: contracts.Hash(data), Size: int64(len(data)), MediaType: "application/octet-stream"}
		candidate := contracts.Candidate{SchemaVersion: 1, UnitID: u.UnitID, SpecDigest: u.SpecDigest, AssignmentID: a.AssignmentID, AttemptID: a.AssignmentID, Outcome: "complete", Files: []contracts.ResultFile{{Path: "result", Artifact: file}}}
		report := contracts.CandidateReport{Receipt: contracts.Receipt{SchemaVersion: 1, RequestID: "raw-" + u.UnitID, WorkerID: a.WorkerID, SessionID: a.SessionID, AssignmentID: a.AssignmentID}, Candidate: candidate}
		digest, _ := contracts.Digest(candidate)
		finiteCLIJSON(t, filepath.Join(root, "candidates", digest+".json"), report)
		os.WriteFile(filepath.Join(root, "blobs", file.Digest), data, 0600)
		records = append(records, record{report, digest, file.Digest, m.AssignmentDigest})
	}
	sort.Slice(records, func(i, j int) bool { return records[i].digest < records[j].digest })
	if e := os.Remove(filepath.Join(root, "blobs", records[1].blob)); e != nil {
		t.Fatal(e)
	}
	current := contracts.FiniteImportAck{SchemaVersion: 1, BundleID: env.Bundle.BundleID, BundleDigest: env.BundleDigest}
	for _, m := range env.Bundle.Members {
		current.Members = append(current.Members, contracts.FiniteMemberAck{AssignmentDigest: m.AssignmentDigest})
	}
	var cached contracts.FiniteImportAck
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/v1/finite/ack":
			json.NewEncoder(w).Encode(current)
		case r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/v1/finite/artifacts/"):
			if r.URL.Query().Get("bundle") != env.BundleDigest || r.URL.Query().Get("member") != records[0].assignmentDigest || r.URL.Query().Get("assignment") != records[0].report.AssignmentID {
				t.Error("missing sealed upload scope")
				http.Error(w, "bad scope", 400)
				return
			}
			io.Copy(io.Discard, r.Body)
			json.NewEncoder(w).Encode(contracts.Acknowledgment{SchemaVersion: 1, Published: true})
		case r.Method == "POST" && r.URL.Path == "/v1/finite/candidate":
			if cached.SchemaVersion == 0 {
				for i := range current.Members {
					if current.Members[i].AssignmentDigest == records[0].assignmentDigest {
						current.Members[i].RawPublished = []string{records[0].digest}
					}
				}
				b, _ := json.Marshal(current)
				json.Unmarshal(b, &cached)
			}
			json.NewEncoder(w).Encode(cached)
		default:
			http.Error(w, "unexpected", 500)
		}
	}))
	defer server.Close()
	client := &agent.Client{URL: server.URL, HTTP: server.Client()}
	var out bytes.Buffer
	if e := importFiniteOutput(context.Background(), client, root, &out); e == nil {
		t.Fatal("missing later file silently accepted")
	}
	readAck := func() contracts.FiniteImportAck {
		var ack contracts.FiniteImportAck
		b, e := os.ReadFile(filepath.Join(root, "ack.json"))
		if e != nil {
			t.Fatal(e)
		}
		if e = json.Unmarshal(b, &ack); e != nil {
			t.Fatal(e)
		}
		if e = ack.Validate(); e != nil {
			t.Fatal(e)
		}
		return ack
	}
	count := func(ack contracts.FiniteImportAck) int {
		n := 0
		for _, m := range ack.Members {
			n += len(m.RawPublished)
		}
		return n
	}
	if ack := readAck(); count(ack) != 1 {
		t.Fatal("partial committed ACK not durable", ack)
	}
	for i := range current.Members {
		if current.Members[i].AssignmentDigest == records[1].assignmentDigest {
			current.Members[i].RawPublished = []string{records[1].digest}
		}
	}
	out.Reset()
	if e := importFiniteOutput(context.Background(), client, root, &out); e == nil {
		t.Fatal("missing later file silently accepted")
	}
	if ack := readAck(); count(ack) != 2 {
		t.Fatal("historical cached reply regressed durable ACK", ack)
	}
}
func TestFiniteExportWritesDeclaredBlobAndManifest(t *testing.T) {
	env := finiteCLIEnvelope(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error(r.Method)
		}
		w.Write(nil)
	}))
	defer server.Close()
	client := &agent.Client{URL: server.URL, HTTP: server.Client()}
	destination := filepath.Join(t.TempDir(), "bundle")
	if e := writeFiniteExport(context.Background(), client, env, destination); e != nil {
		t.Fatal(e)
	}
	root, e := os.OpenRoot(destination)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	var got contracts.FiniteBundleEnvelope
	if e = finiteCLIMetadata(root, "bundle.json", &got); e != nil || got.BundleDigest != env.BundleDigest {
		t.Fatal(e)
	}
	if e = writeFiniteExport(context.Background(), client, env, destination); e != nil {
		t.Fatal("idempotent materialization", e)
	}
}
