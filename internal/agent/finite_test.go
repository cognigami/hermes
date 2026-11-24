package agent

import (
	"context"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"hermes/internal/platform"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func finiteAgentFixture(t *testing.T, count int) (FiniteConfig, contracts.FiniteBundleEnvelope) {
	t.Helper()
	budget := admissionBudget()
	w := testWork()
	w.Assignment.Profile = "small"
	w.Assignment.Allocation = budget.Profiles[0].Allocation
	w.Budget = &budget
	binary, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	build := platform.Current()
	artifact := contracts.Artifact{Digest: contracts.Hash(nil), MediaType: "application/octet-stream"}
	ref := contracts.FiniteRuntimeRef{Runtime: w.Assignment.Runtime, GOOS: build.GOOS, GOARCH: build.GOARCH, ISABaseline: build.ISABaseline, Executable: artifact}
	b := contracts.FiniteBundle{SchemaVersion: 1, BundleID: "registered-batch", WorkerID: w.Assignment.WorkerID, SessionID: w.Assignment.SessionID, HostID: "host", ExportRevision: 1, RuntimeRefs: []contracts.FiniteRuntimeRef{ref}, Blobs: []contracts.Artifact{artifact}}
	for i := 0; i < count; i++ {
		member := w
		a := *w.Assignment
		a.AssignmentID += string(rune('a' + i))
		a.RunID += string(rune('a' + i))
		member.Assignment = &a
		digest, _ := contracts.Digest(a)
		b.Members = append(b.Members, contracts.FiniteMember{Work: member, AssignmentDigest: digest})
	}
	digest, _ := contracts.Digest(b)
	cfg := FiniteConfig{Config: Config{Root: t.TempDir(), WorkerID: b.WorkerID, SessionID: b.SessionID, HostID: b.HostID, Runtime: w.Assignment.Runtime, Profile: "small", MaxArtifactBytes: 1 << 20, MaxConcurrent: 2}, BundleDirectory: t.TempDir(), Runtimes: []FiniteRuntimeBinding{{Reference: ref, Argv: []string{binary}}}}
	return cfg, contracts.FiniteBundleEnvelope{BundleDigest: digest, Bundle: b}
}
func retainFiniteFixture(t *testing.T, cfg FiniteConfig, envelope contracts.FiniteBundleEnvelope, uncertain bool) {
	t.Helper()
	h, e := openHost(cfg.Config)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	if e = h.initializeLocalSpool(); e != nil {
		t.Fatal(e)
	}
	if e = h.installBudget(*envelope.Bundle.Members[0].Work.Budget); e != nil {
		t.Fatal(e)
	}
	for i, m := range envelope.Bundle.Members {
		r, e := h.receive(m.Work)
		if e != nil {
			t.Fatal(e)
		}
		r.j.Finite = &finiteJournal{BundleID: envelope.Bundle.BundleID, BundleDigest: envelope.BundleDigest, ExportRevision: 1}
		r.j.ReconcileRevision = 1
		if uncertain && i == 0 {
			r.j.ProcessStarted = true
		} else {
			r.j.Quiescent = true
		}
		if e = r.save(); e != nil {
			t.Fatal(e)
		}
	}
}
func loadFiniteFinal(t *testing.T, cfg FiniteConfig, e contracts.FiniteBundleEnvelope, index int) contracts.FiniteFinalInventory {
	t.Helper()
	path := filepath.Join(cfg.Config.Root, "exchange", e.Bundle.BundleID, "finals", e.Bundle.Members[index].AssignmentDigest+".json")
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var result contracts.FiniteFinalInventory
	if err = contracts.Decode(f, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func TestFiniteQuiescentReopenAndGenuineIdempotentFinalAck(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native Darwin finite execution only")
	}
	cfg, envelope := finiteAgentFixture(t, 2)
	retainFiniteFixture(t, cfg, envelope, false)
	for i := 0; i < 2; i++ {
		if e := RunFinite(context.Background(), cfg, envelope); e != nil {
			t.Fatal(e)
		}
	}
	ack := contracts.FiniteImportAck{SchemaVersion: 1, BundleID: envelope.Bundle.BundleID, BundleDigest: envelope.BundleDigest}
	for index, member := range envelope.Bundle.Members {
		inventory := loadFiniteFinal(t, cfg, envelope, index)
		if inventory.Final == nil || inventory.Reconciliation.ProcessStarted || len(inventory.Final.NeverStarted) != 1 {
			t.Fatal("replay changed positive no-child facts")
		}
		digest, _ := contracts.Digest(*inventory.Final)
		ack.Members = append(ack.Members, contracts.FiniteMemberAck{AssignmentDigest: member.AssignmentDigest, FinalDigest: digest, ReconciliationRevision: inventory.Reconciliation.Revision})
	}
	h, e := openHost(cfg.Config)
	if e != nil {
		t.Fatal(e)
	}
	if len(h.active) != 2 {
		t.Fatal("offline finals retired without genuine ACK")
	}
	for _, r := range h.active {
		if r.j.ReceiptAcknowledged || r.j.FinalSent {
			t.Fatal("offline synthesized ACK")
		}
	}
	h.Close()
	for i := 0; i < 2; i++ {
		if e = ApplyFiniteImportAck(cfg.Config.Root, ack); e != nil {
			t.Fatal(e)
		}
	}
	h, e = openHost(cfg.Config)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	if len(h.active) != 0 {
		t.Fatal("genuine final ACK did not retire")
	}
}
func TestFiniteMalformedLaterAckDoesNotMutateEarlierMember(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native Darwin finite execution only")
	}
	cfg, envelope := finiteAgentFixture(t, 2)
	retainFiniteFixture(t, cfg, envelope, false)
	if e := RunFinite(context.Background(), cfg, envelope); e != nil {
		t.Fatal(e)
	}
	before := map[string][]byte{}
	for _, m := range envelope.Bundle.Members {
		path := filepath.Join(cfg.Config.Root, "active", contracts.Hash([]byte(m.Work.Assignment.AssignmentID)), "journal.json")
		data, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		before[path] = data
	}
	indexPath := filepath.Join(cfg.Config.Root, "active.json")
	index, e := os.ReadFile(indexPath)
	if e != nil {
		t.Fatal(e)
	}
	first := loadFiniteFinal(t, cfg, envelope, 0)
	digest, _ := contracts.Digest(*first.Final)
	ack := contracts.FiniteImportAck{SchemaVersion: 1, BundleID: envelope.Bundle.BundleID, BundleDigest: envelope.BundleDigest, Members: []contracts.FiniteMemberAck{{AssignmentDigest: envelope.Bundle.Members[0].AssignmentDigest, FinalDigest: digest}, {AssignmentDigest: envelope.Bundle.Members[1].AssignmentDigest, RawPublished: []string{contracts.Hash([]byte("unknown"))}}}}
	snapshot := func() spoolSnapshot {
		h, e := openHost(cfg.Config)
		if e != nil {
			t.Fatal(e)
		}
		defer h.Close()
		if e = h.initializeLocalSpool(); e != nil {
			t.Fatal(e)
		}
		var active []string
		for id := range h.active {
			active = append(active, id)
		}
		s, e := h.spool.Snapshot(active)
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	ledgerBefore := snapshot()
	body, _ := json.Marshal(ack)
	source := filepath.Join(cfg.Config.Root, "exchange", ack.BundleID, "ack.json")
	if e = os.WriteFile(source, body, 0600); e != nil {
		t.Fatal(e)
	}
	if ApplyFiniteImportAck(cfg.Config.Root, ack, source) == nil {
		t.Fatal("unknown later candidate ACK accepted")
	}
	ledgerAfter := snapshot()
	if ledgerBefore.TotalBytes != ledgerAfter.TotalBytes || ledgerBefore.Revision != ledgerAfter.Revision {
		t.Fatal("malformed ACK mutated accounting before full prevalidation")
	}

	for path, data := range before {
		after, e := os.ReadFile(path)
		if e != nil || string(data) != string(after) {
			t.Fatal("malformed later member mutated earlier journal", path, e)
		}
	}
	after, e := os.ReadFile(indexPath)
	if e != nil || string(index) != string(after) {
		t.Fatal("malformed ACK changed active index", e)
	}
}
func TestFiniteReopenAfterOneMemberFinalAckSkipsExactArchive(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native Darwin finite execution only")
	}
	cfg, envelope := finiteAgentFixture(t, 2)
	retainFiniteFixture(t, cfg, envelope, false)
	if e := RunFinite(context.Background(), cfg, envelope); e != nil {
		t.Fatal(e)
	}
	inventory := loadFiniteFinal(t, cfg, envelope, 0)
	digest, _ := contracts.Digest(*inventory.Final)
	ack := contracts.FiniteImportAck{SchemaVersion: 1, BundleID: envelope.Bundle.BundleID, BundleDigest: envelope.BundleDigest, Members: []contracts.FiniteMemberAck{{AssignmentDigest: envelope.Bundle.Members[0].AssignmentDigest, FinalDigest: digest}}}
	if e := ApplyFiniteImportAck(cfg.Config.Root, ack); e != nil {
		t.Fatal(e)
	}
	if e := RunFinite(context.Background(), cfg, envelope); e != nil {
		t.Fatal("known archived member blocked remaining retained output", e)
	}
	h, e := openHost(cfg.Config)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	if len(h.active) != 1 {
		t.Fatal("partial ACK replay changed execution membership")
	}
	for _, r := range h.active {
		if r.j.ProcessStarted || !r.j.Quiescent || r.j.FinalSent {
			t.Fatal("partial recovery re-executed or acknowledged remaining work")
		}
	}
}

func TestFiniteUncertainRestartClosesAllMemberStarts(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native Darwin finite execution only")
	}
	cfg, envelope := finiteAgentFixture(t, 2)
	retainFiniteFixture(t, cfg, envelope, true)
	if RunFinite(context.Background(), cfg, envelope) == nil {
		t.Fatal("uncertain ownership not reported")
	}
	inventory := loadFiniteFinal(t, cfg, envelope, 0)
	if !inventory.Uncertain || inventory.Final != nil || inventory.Reconciliation.Quiescent {
		t.Fatal("restart invented exit proof")
	}
	h, e := openHost(cfg.Config)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	for _, r := range h.active {
		if !r.j.GateClosed {
			t.Fatal("host uncertainty left a grant gate open")
		}
	}
}
func TestFiniteRejectsUnknownPlatformWithoutHostMutation(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("non-Darwin fail-closed test")
	}
	cfg, e := finiteAgentFixture(t, 1)
	if RunFinite(context.Background(), cfg, e) == nil {
		t.Fatal("nonqualified observation admitted")
	}
	if _, err := os.Stat(filepath.Join(cfg.Config.Root, "identity.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unsupported platform mutated host identity")
	}
}
func TestRegistrationIncludesTerminalButUnretiredTotal(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native resource observation only")
	}
	cfg, envelope := finiteAgentFixture(t, 1)
	retainFiniteFixture(t, cfg, envelope, false)
	var received contracts.WorkerRegistration
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/register" {
			t.Error("register-only polled/executed", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		if e := contracts.Decode(r.Body, &received); e != nil {
			t.Error(e)
			w.WriteHeader(400)
			return
		}
		json.NewEncoder(w).Encode(envelope.Bundle.Members[0].Work.Budget)
	}))
	defer server.Close()
	cfg.Config.Client = &Client{URL: server.URL, HTTP: server.Client()}
	if _, e := RegisterFiniteHost(context.Background(), cfg.Config); e != nil {
		t.Fatal(e)
	}
	if received.TotalSpoolBytes == nil || *received.TotalSpoolBytes <= received.RetainedSpoolBytes || received.SpoolRevision <= 0 {
		t.Fatal("actual full-root snapshot omitted active quiescent retained bytes", received)
	}
}
