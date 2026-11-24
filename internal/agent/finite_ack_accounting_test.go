package agent

import (
	"context"
	"encoding/json"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestFiniteRetainedAckSourcesChargedBeforeArchiveAndIdempotentReplay(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native Darwin finite exchange only")
	}
	cfg, envelope := finiteAgentFixture(t, 1)
	retainFiniteFixture(t, cfg, envelope, false)
	if e := RunFinite(context.Background(), cfg, envelope); e != nil {
		t.Fatal(e)
	}
	inventory := loadFiniteFinal(t, cfg, envelope, 0)
	digest, _ := contracts.Digest(*inventory.Final)
	ack := contracts.FiniteImportAck{SchemaVersion: 1, BundleID: envelope.Bundle.BundleID, BundleDigest: envelope.BundleDigest, Members: []contracts.FiniteMemberAck{{AssignmentDigest: inventory.AssignmentDigest, FinalDigest: digest}}}
	body, _ := json.Marshal(ack)
	canonical := filepath.Join(cfg.Config.Root, "exchange", ack.BundleID, "ack.json")
	snapshot := func() (spoolSnapshot, int64) {
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
		var charge int64
		h.spool.db.QueryRow("SELECT size FROM spool_files WHERE key=?", spoolKey(filepath.Join("exchange", ack.BundleID, "ack.json"))).Scan(&charge)
		return s, charge
	}
	before, _ := snapshot()
	id := envelope.Bundle.Members[0].Work.Assignment.AssignmentID
	activeJournal := filepath.Join(cfg.Config.Root, "active", contracts.Hash([]byte(id)), "journal.json")
	oldInfo, e := os.Stat(activeJournal)
	if e != nil {
		t.Fatal(e)
	}
	// This is the retained ACK produced by the operator import after bootstrap.
	if e = os.WriteFile(canonical, body, 0600); e != nil {
		t.Fatal(e)
	}
	if e = ApplyFiniteImportAck(cfg.Config.Root, ack); e != nil {
		t.Fatal(e)
	}
	after, charge := snapshot()
	archive := filepath.Join(cfg.Config.Root, "completed", contracts.Hash([]byte(id)), "journal.json")
	newInfo, e := os.Stat(archive)
	if e != nil {
		t.Fatal(e)
	}
	if charge != int64(len(body)) || after.TotalBytes-before.TotalBytes != int64(len(body))+newInfo.Size()-oldInfo.Size() {
		t.Fatal("retained ACK missing from exact totals across archive", charge, before, after)
	}
	if e = ApplyFiniteImportAck(cfg.Config.Root, ack, canonical); e != nil {
		t.Fatal(e)
	}
	repeated, _ := snapshot()
	if repeated.TotalBytes != after.TotalBytes || repeated.Revision != after.Revision {
		t.Fatal("repeat archived ACK charged twice")
	}
	external := filepath.Join(t.TempDir(), "ack.json")
	if e = os.WriteFile(external, body, 0600); e != nil {
		t.Fatal(e)
	}
	if e = ApplyFiniteImportAck(cfg.Config.Root, ack, external); e != nil {
		t.Fatal(e)
	}
	outside, _ := snapshot()
	if outside.TotalBytes != after.TotalBytes || outside.Revision != after.Revision {
		t.Fatal("external readonly ACK became owned spool")
	}
	alternate := filepath.Join(cfg.Config.Root, "supplied-ack.json")
	if e = os.WriteFile(alternate, body, 0600); e != nil {
		t.Fatal(e)
	}
	if e = ApplyFiniteImportAck(cfg.Config.Root, ack, alternate); e != nil {
		t.Fatal(e)
	}
	inside, _ := snapshot()
	if inside.TotalBytes-after.TotalBytes != int64(len(body)) {
		t.Fatal("noncanonical managed ACK source exempted", inside.TotalBytes, after.TotalBytes)
	}
}
