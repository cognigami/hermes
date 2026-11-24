package agent

import (
	"context"
	"encoding/json"
	"hermes/internal/contracts"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestFiniteApplicationHelper(t *testing.T) {
	enabled := false
	manifestPath := ""
	output := ""
	for i, arg := range os.Args {
		if arg == "--finite-app" {
			enabled = true
		}
		if i+1 < len(os.Args) {
			if arg == "--manifest" {
				manifestPath = os.Args[i+1]
			}
			if arg == "--output" {
				output = os.Args[i+1]
			}
		}
	}
	if !enabled {
		return
	}
	var manifest struct {
		Assignment *contracts.Assignment `json:"assignment"`
		Units      []contracts.Unit      `json:"units"`
	}
	body, e := os.ReadFile(manifestPath)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(body, &manifest); e != nil {
		t.Fatal(e)
	}
	send := func(kind, unit, path string) {
		if e := contracts.WriteMessage(os.Stdout, contracts.Message{SchemaVersion: 1, Type: kind, UnitID: unit, ManifestPath: path}); e != nil {
			t.Fatal(e)
		}
	}
	send("ready", "", "")
	reader := contracts.MessageReader(os.Stdin)
	for {
		message, e := contracts.ReadMessage(reader)
		if e != nil {
			t.Fatal(e)
		}
		if message.Type == "finish" {
			send("finished", "", "")
			os.Exit(0)
		}
		if message.Type != "execute_unit" {
			t.Fatal("unexpected grant", message.Type)
		}
		unit := manifest.Units[0]
		payload := []byte("durable finite result")
		if e = os.WriteFile(filepath.Join(output, "result.bin"), payload, 0600); e != nil {
			t.Fatal(e)
		}
		candidate := contracts.Candidate{SchemaVersion: 1, UnitID: unit.UnitID, SpecDigest: unit.SpecDigest, AssignmentID: manifest.Assignment.AssignmentID, AttemptID: manifest.Assignment.AssignmentID, Outcome: "ok", Files: []contracts.ResultFile{{Path: "result.bin", Artifact: contracts.Artifact{Digest: contracts.Hash(payload), Size: int64(len(payload)), MediaType: "application/octet-stream"}}}}
		data, _ := json.Marshal(candidate)
		if e = os.WriteFile(filepath.Join(output, "candidate.json"), data, 0600); e != nil {
			t.Fatal(e)
		}
		send("unit_result", unit.UnitID, "candidate.json")
		message, e = contracts.ReadMessage(reader)
		if e != nil || message.Type != "published" {
			t.Fatal("missing local published ACK", message, e)
		}
		cwd, _ := os.Getwd()
		hostRoot := filepath.Clean(filepath.Join(cwd, "../../.."))
		digest, _ := contracts.Digest(candidate)
		if _, e = os.Stat(filepath.Join(hostRoot, "exchange", "registered-batch", "candidates", digest+".json")); e != nil {
			t.Fatal("finite candidate was not durable before published ACK", e)
		}
	}
}
func TestFiniteUnknownCapabilityBlocksBeforePreparation(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native Darwin finite execution only")
	}
	cfg, envelope := finiteAgentFixture(t, 1)
	envelope.Bundle.Members[0].Work.Units[0].RequiredCapabilities = []string{"unqualified_future_backend"}
	envelope.BundleDigest, _ = contracts.Digest(envelope.Bundle)
	if RunFinite(context.Background(), cfg, envelope) == nil {
		t.Fatal("unknown capability admitted")
	}
	member := envelope.Bundle.Members[0]
	work := filepath.Join(cfg.Config.Root, "active", contracts.Hash([]byte(member.Work.Assignment.AssignmentID)), "work")
	if _, e := os.Stat(work); !os.IsNotExist(e) {
		t.Fatal("unsupported capability reached preparation", e)
	}
	inventory := loadFiniteFinal(t, cfg, envelope, 0)
	if inventory.Reconciliation.ProcessStarted || inventory.Final == nil || len(inventory.Final.NeverStarted) != 1 {
		t.Fatal("capability denial fabricated app lifecycle")
	}
}
func TestFiniteApprovedRuntimeFileIsFreshlyVerified(t *testing.T) {
	path := filepath.Join(t.TempDir(), "support.bin")
	data := []byte("approved immutable support")
	if e := os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	artifact := contracts.Artifact{Digest: contracts.Hash(data), Size: int64(len(data)), MediaType: "application/octet-stream"}
	if e := verifyFiniteRuntimeFile(context.Background(), path, artifact); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, []byte("tampered immutable support"), 0600); e != nil {
		t.Fatal(e)
	}
	if verifyFiniteRuntimeFile(context.Background(), path, artifact) == nil {
		t.Fatal("mutable runtime/support bytes reused without fresh build binding")
	}
}

func TestFiniteRealMultiMemberExecutionRawAckAndNoReplay(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native Darwin finite execution only")
	}
	cfg, envelope := finiteAgentFixture(t, 2)
	binary := cfg.Runtimes[0].Argv[0]
	file, e := os.Open(binary)
	if e != nil {
		t.Fatal(e)
	}
	data, e := io.ReadAll(file)
	file.Close()
	if e != nil {
		t.Fatal(e)
	}
	ref := cfg.Runtimes[0].Reference
	ref.Executable.Digest = contracts.Hash(data)
	ref.Executable.Size = int64(len(data))
	cfg.Runtimes[0].Reference = ref
	cfg.Runtimes[0].Argv = []string{binary, "-test.run=TestFiniteApplicationHelper", "--", "--finite-app"}
	envelope.Bundle.RuntimeRefs[0] = ref
	envelope.Bundle.Blobs[0] = ref.Executable
	envelope.BundleDigest, _ = contracts.Digest(envelope.Bundle)
	for i := 0; i < 2; i++ {
		if e = RunFinite(context.Background(), cfg, envelope); e != nil {
			t.Fatal(e)
		}
	}
	h, e := openHost(cfg.Config)
	if e != nil {
		t.Fatal(e)
	}
	ack := contracts.FiniteImportAck{SchemaVersion: 1, BundleID: envelope.Bundle.BundleID, BundleDigest: envelope.BundleDigest}
	for _, r := range h.active {
		if !r.j.ProcessStarted || !r.j.Quiescent || len(r.j.Started) != 1 || len(r.j.Candidates) != 1 || r.j.Delivered != 0 || r.j.ReceiptAcknowledged || r.j.FinalSent {
			t.Fatal("finite execution/ACK facts incorrect", r.j)
		}
		candidate := r.j.Candidates[0]
		digest, _ := contracts.Digest(candidate)
		ad, _ := contracts.Digest(*r.j.Work.Assignment)
		ack.Members = append(ack.Members, contracts.FiniteMemberAck{AssignmentDigest: ad, RawPublished: []string{digest}})
	}
	h.Close()
	if e = ApplyFiniteImportAck(cfg.Config.Root, ack); e != nil {
		t.Fatal(e)
	}
	h, e = openHost(cfg.Config)
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range h.active {
		if r.j.Delivered != 1 || r.j.FinalSent || r.j.ReceiptAcknowledged {
			t.Fatal("raw ACK fabricated final/receipt")
		}
	}
	h.Close()
	for i := range ack.Members {
		for index, m := range envelope.Bundle.Members {
			if m.AssignmentDigest == ack.Members[i].AssignmentDigest {
				inventory := loadFiniteFinal(t, cfg, envelope, index)
				ack.Members[i].FinalDigest, _ = contracts.Digest(*inventory.Final)
				ack.Members[i].ReconciliationRevision = inventory.Reconciliation.Revision
			}
		}
	}
	if e = ApplyFiniteImportAck(cfg.Config.Root, ack); e != nil {
		t.Fatal(e)
	}
	if e = ApplyFiniteImportAck(cfg.Config.Root, ack); e != nil {
		t.Fatal("repeated terminal ACK not idempotent", e)
	}
}
