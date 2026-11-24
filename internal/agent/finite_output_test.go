package agent

import (
	"bytes"
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
	"testing"
)

func finiteOutputFixture(t *testing.T) (*host, *runner, *finiteOutput) {
	t.Helper()
	cfg, envelope := finiteAgentFixture(t, 1)
	h, e := openHost(cfg.Config)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { h.Close() })
	if e = h.initializeLocalSpool(); e != nil {
		t.Fatal(e)
	}
	budget := *envelope.Bundle.Members[0].Work.Budget
	if e = h.installBudget(budget); e != nil {
		t.Fatal(e)
	}
	h.admission.observe = func(string) (contracts.HostObservation, error) {
		return contracts.HostObservation{Resources: budget.Approval.Limits, AvailableSpoolBytes: 1 << 30, HardTreeMemory: "unknown", Source: "explicit_synthetic_test"}, nil
	}
	r, e := h.receive(envelope.Bundle.Members[0].Work)
	if e != nil {
		t.Fatal(e)
	}
	r.j.Finite = &finiteJournal{BundleID: envelope.Bundle.BundleID, BundleDigest: envelope.BundleDigest, ExportRevision: 1}
	r.j.ReconcileRevision = 1
	r.j.Quiescent = true
	if e = r.save(); e != nil {
		t.Fatal(e)
	}
	output, e := newFiniteOutput(context.Background(), h, envelope)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { output.Close() })
	return h, r, output
}
func TestFiniteOutputChargesOriginalAndCopyWithNoPrefixReplay(t *testing.T) {
	h, r, out := finiteOutputFixture(t)
	payload := []byte("shared physical copy")
	digest := contracts.Hash(payload)
	if e := r.publishBlob(context.Background(), digest, int64(len(payload)), bytes.NewReader(payload)); e != nil {
		t.Fatal(e)
	}
	a := r.j.Work.Assignment
	c := contracts.Candidate{SchemaVersion: 1, UnitID: a.Units[0].UnitID, SpecDigest: a.Units[0].SpecDigest, AssignmentID: a.AssignmentID, AttemptID: a.AssignmentID, Outcome: "ok", Files: []contracts.ResultFile{{Path: "result", Artifact: contracts.Artifact{Digest: digest, Size: int64(len(payload)), MediaType: "application/octet-stream"}}}}
	r.j.ProcessStarted = true
	r.j.Started = []string{c.UnitID}
	r.j.Candidates = []contracts.Candidate{c}
	if e := r.save(); e != nil {
		t.Fatal(e)
	}
	if e := out.candidate(r, c); e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"blob/" + digest, spoolKey(filepath.Join(out.relative, "blobs", digest))} {
		var charged int64
		if e := h.spool.db.QueryRow("SELECT size FROM spool_files WHERE key=?", key).Scan(&charged); e != nil || charged != int64(len(payload)) {
			t.Fatal("same digest erased distinct physical bytes", key, charged, e)
		}
	}
	before, e := h.spool.Snapshot([]string{a.AssignmentID})
	if e != nil {
		t.Fatal(e)
	}
	if e = out.candidate(r, c); e != nil {
		t.Fatal(e)
	}
	after, e := h.spool.Snapshot([]string{a.AssignmentID})
	if e != nil {
		t.Fatal(e)
	}
	if before.Revision != after.Revision || before.TotalBytes != after.TotalBytes {
		t.Fatal("already exposed candidate prefix replayed")
	}
}
func TestFiniteFailedCopyIntentsRemainChargedWithoutCandidateExposure(t *testing.T) {
	h, r, out := finiteOutputFixture(t)
	payload := []byte("expected bytes")
	digest := contracts.Hash(payload)
	path := "blobs/" + digest
	if e := out.write(r, path, int64(len(payload)), bytes.NewReader([]byte("bad")), digest); e == nil {
		t.Fatal("corrupt output copied")
	}
	for _, path := range []string{path, "pending/" + contracts.Hash([]byte(path))} {
		var charged int64
		if e := h.spool.db.QueryRow("SELECT size FROM spool_files WHERE key=?", spoolKey(filepath.Join(out.relative, path))).Scan(&charged); e != nil || charged != int64(len(payload)) {
			t.Fatal("failed ambiguous intent lost charge", charged, e)
		}
	}
	if _, e := out.fs.Stat(path); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("incomplete blob exposed at final name", e)
	}
	files, e := out.fs.ReadFile("candidates/nonexistent.json")
	if e == nil || len(files) != 0 {
		t.Fatal("failed output exposed candidate")
	}
}
func TestFiniteOutputRejectsEscapeAndFIFOWithoutOpeningStream(t *testing.T) {
	_, r, out := finiteOutputFixture(t)
	if out.write(r, "../outside", 0, bytes.NewReader(nil), contracts.Hash(nil)) == nil {
		t.Fatal("output escaped root")
	}
	digest := contracts.Hash(nil)
	path := "blobs/" + digest
	if e := unix.Mkfifo(filepath.Join(out.h.cfg.Root, out.relative, filepath.FromSlash(path)), 0600); e != nil {
		t.Fatal(e)
	}
	if out.write(r, path, 0, bytes.NewReader(nil), digest) == nil {
		t.Fatal("FIFO treated as existing finite blob")
	}
}
func TestFiniteShorterReplacementRetainsOldChargeUntilDurableShrink(t *testing.T) {
	for _, kind := range []string{"destination", "pending"} {
		t.Run(kind, func(t *testing.T) {
			h, r, out := finiteOutputFixture(t)
			path := "finals/shrinking.json"
			pending := "pending/" + contracts.Hash([]byte(path))
			oldPath := path
			if kind == "pending" {
				oldPath = pending
			}
			old := bytes.Repeat([]byte("x"), 32<<10)
			if e := out.fs.WriteFile(oldPath, old, 0600); e != nil {
				t.Fatal(e)
			}
			if e := out.record(r, oldPath, int64(len(old))); e != nil {
				t.Fatal(e)
			}
			extra := "pending/retained"
			f, e := out.fs.OpenFile(extra, os.O_CREATE|os.O_WRONLY, 0600)
			if e != nil {
				t.Fatal(e)
			}
			spoolBytes := r.j.Work.Assignment.Allocation.Resources.SpoolBytes
			if e = f.Truncate(spoolBytes); e != nil {
				t.Fatal(e)
			}
			f.Close()
			if e = out.record(r, extra, spoolBytes); e != nil {
				t.Fatal(e)
			}
			id := r.j.Work.Assignment.AssignmentID
			snapshot, e := h.spool.Snapshot([]string{id})
			if e != nil {
				t.Fatal(e)
			}
			entry := h.admission.entries[id]
			entry.issued.Approval.Limits.SpoolBytes = entry.issued.Approval.Baseline.SpoolBytes + snapshot.OwnedBytes[id] - 1
			h.admission.entries[id] = entry
			body := []byte("short metadata")
			if e = out.write(r, path, int64(len(body)), bytes.NewReader(body), contracts.Hash(body)); e == nil {
				t.Fatal("shorter intent borrowed capacity from still-retained larger bytes")
			}
			info, e := out.fs.Stat(oldPath)
			if e != nil || info.Size() != int64(len(old)) {
				t.Fatal("denied copy truncated old retained bytes", e)
			}
			var charged int64
			if e = h.spool.db.QueryRow("SELECT size FROM spool_files WHERE key=?", spoolKey(filepath.Join(out.relative, oldPath))).Scan(&charged); e != nil || charged < int64(len(old)) {
				t.Fatal("larger old retained charge shrank before replacement", charged, e)
			}
			entry.issued.Approval.Limits.SpoolBytes = 64 << 20
			h.admission.entries[id] = entry
			if e = out.write(r, path, int64(len(body)), bytes.NewReader(body), contracts.Hash(body)); e != nil {
				t.Fatal(e)
			}
			for _, expect := range []struct {
				path string
				size int64
			}{{path, int64(len(body))}, {pending, 0}} {
				if e = h.spool.db.QueryRow("SELECT size FROM spool_files WHERE key=?", spoolKey(filepath.Join(out.relative, expect.path))).Scan(&charged); e != nil || charged != expect.size {
					t.Fatal("durable replacement did not establish exact smaller charge", expect.path, charged, e)
				}
			}
		})
	}
}

func TestFiniteLateProvisionedManagedSourcesAreIndexedButExternalSourcesAreNot(t *testing.T) {
	h, r, out := finiteOutputFixture(t)
	cfg := FiniteConfig{BundleDirectory: filepath.Join(h.cfg.Root, "incoming")}
	if e := os.MkdirAll(filepath.Join(cfg.BundleDirectory, "blobs"), 0700); e != nil {
		t.Fatal(e)
	}
	runtimeBytes := []byte("local runtime")
	supportBytes := []byte("local support")
	runtimeArtifact := contracts.Artifact{Digest: contracts.Hash(runtimeBytes), Size: int64(len(runtimeBytes)), MediaType: "application/octet-stream"}
	supportArtifact := contracts.Artifact{Digest: contracts.Hash(supportBytes), Size: int64(len(supportBytes)), MediaType: "application/octet-stream"}
	envelope := out.envelope
	ref := envelope.Bundle.RuntimeRefs[0]
	ref.Executable = runtimeArtifact
	ref.Support = []contracts.Artifact{supportArtifact}
	envelope.Bundle.RuntimeRefs = []contracts.FiniteRuntimeRef{ref}
	envelope.Bundle.Blobs = []contracts.Artifact{runtimeArtifact, supportArtifact}
	local := filepath.Join(h.cfg.Root, "deployed")
	if e := os.Mkdir(local, 0700); e != nil {
		t.Fatal(e)
	}
	exe := filepath.Join(local, "executable")
	support := filepath.Join(local, "support")
	cfg.Runtimes = []FiniteRuntimeBinding{{Reference: ref, Argv: []string{exe}, SupportPaths: map[string]string{supportArtifact.Digest: support}}}
	files := map[string][]byte{filepath.Join(cfg.BundleDirectory, "bundle.json"): []byte("metadata"), filepath.Join(cfg.BundleDirectory, "blobs", runtimeArtifact.Digest): runtimeBytes, filepath.Join(cfg.BundleDirectory, "blobs", supportArtifact.Digest): supportBytes, exe: runtimeBytes, support: supportBytes}
	var added int64
	for path, data := range files {
		if e := os.WriteFile(path, data, 0600); e != nil {
			t.Fatal(e)
		}
		added += int64(len(data))
	}
	id := r.j.Work.Assignment.AssignmentID
	before, e := h.spool.Snapshot([]string{id})
	if e != nil {
		t.Fatal(e)
	}
	if e = h.trackFiniteSources(cfg, envelope, id); e != nil {
		t.Fatal(e)
	}
	after, e := h.spool.Snapshot([]string{id})
	if e != nil {
		t.Fatal(e)
	}
	if after.TotalBytes-before.TotalBytes != added {
		t.Fatal("late owned source paths omitted or deduplicated distinct copies", before.TotalBytes, after.TotalBytes, added)
	}
	external := t.TempDir()
	if e = os.WriteFile(filepath.Join(external, "bundle.json"), []byte("external preprovisioned"), 0600); e != nil {
		t.Fatal(e)
	}
	cfg.BundleDirectory = external
	cfg.Runtimes[0].Argv = []string{filepath.Join(external, "executable")}
	cfg.Runtimes[0].SupportPaths = map[string]string{supportArtifact.Digest: filepath.Join(external, "support")}
	if e = os.WriteFile(cfg.Runtimes[0].Argv[0], runtimeBytes, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(cfg.Runtimes[0].SupportPaths[supportArtifact.Digest], supportBytes, 0600); e != nil {
		t.Fatal(e)
	}
	if e = h.trackFiniteSources(cfg, envelope, id); e != nil {
		t.Fatal(e)
	}
	unchanged, e := h.spool.Snapshot([]string{id})
	if e != nil {
		t.Fatal(e)
	}
	if unchanged.TotalBytes != after.TotalBytes {
		t.Fatal("external preprovisioned files became owned spool")
	}
}

func TestFiniteCopyCapacityDeniedBeforeAnyBytes(t *testing.T) {
	h, r, out := finiteOutputFixture(t)
	h.admission.observe = func(string) (contracts.HostObservation, error) {
		return contracts.HostObservation{Resources: r.j.Work.Budget.Approval.Limits, AvailableSpoolBytes: 0}, nil
	}
	digest := contracts.Hash([]byte("bytes"))
	if out.write(r, "blobs/"+digest, 5, bytes.NewReader([]byte("bytes")), digest) == nil {
		t.Fatal("copy ignored actual free space")
	}
	if _, e := out.fs.Stat("blobs/" + digest); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("denied copy wrote final bytes")
	}
}
