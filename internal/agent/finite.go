package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"hermes/internal/platform"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// FiniteRuntimeBinding is local deployment approval; the bundle cannot choose argv.
type FiniteRuntimeBinding struct {
	Reference    contracts.FiniteRuntimeRef `json:"reference"`
	Argv         []string                   `json:"argv"`
	SupportPaths map[string]string          `json:"support_paths,omitempty"`
}
type FiniteConfig struct {
	Config          Config
	BundleDirectory string
	Runtimes        []FiniteRuntimeBinding
}
type finiteJournal struct {
	BundleID        string   `json:"bundle_id"`
	BundleDigest    string   `json:"bundle_digest"`
	ExportRevision  int64    `json:"export_revision"`
	RawAcknowledged []string `json:"raw_acknowledged,omitempty"`
}

func validateFiniteJournal(j journal) error {
	if j.Finite == nil {
		return nil
	}
	f := j.Finite
	if !contracts.SafeFiniteBundleID(f.BundleID) || !spoolDigest(f.BundleDigest) || f.ExportRevision < 1 || j.ReconcileRevision < 1 {
		return errors.New("invalid persisted finite export binding")
	}
	known := map[string]bool{}
	for _, candidate := range j.Candidates {
		digest, _ := contracts.Digest(candidate)
		known[digest] = true
	}
	acked := map[string]bool{}
	for _, digest := range f.RawAcknowledged {
		if !known[digest] || acked[digest] {
			return errors.New("invalid persisted finite raw acknowledgment")
		}
		acked[digest] = true
	}
	prefix := 0
	for _, candidate := range j.Candidates {
		digest, _ := contracts.Digest(candidate)
		if !acked[digest] {
			break
		}
		prefix++
	}
	if j.Delivered != prefix {
		return errors.New("finite delivered prefix lacks genuine raw acknowledgments")
	}
	return nil
}

// RegisterFiniteHost refreshes actual observed inventory under the normal host
// lock, without polling, receiving an assignment, or launching any child.
func RegisterFiniteHost(ctx context.Context, cfg Config) (contracts.HostBudget, error) {
	if cfg.Root == "" || cfg.Client == nil || cfg.Client.HTTP == nil {
		return contracts.HostBudget{}, errors.New("registration requires a real controller client")
	}
	if cfg.HostID == "" {
		cfg.HostID = cfg.WorkerID
	}
	h, e := openHost(cfg)
	if e != nil {
		return contracts.HostBudget{}, e
	}
	defer h.Close()
	if e = h.initializeResources(ctx); e != nil {
		return contracts.HostBudget{}, e
	}
	return h.admission.budget, nil
}
func RunFinite(ctx context.Context, cfg FiniteConfig, envelope contracts.FiniteBundleEnvelope) error {
	if cfg.Config.Root == "" || cfg.BundleDirectory == "" {
		return errors.New("finite host/input root required")
	}
	if cfg.Config.ShutdownGrace == 0 {
		cfg.Config.ShutdownGrace = 100 * time.Millisecond
	}
	if cfg.Config.ShutdownGrace < 0 {
		return errors.New("negative shutdown grace")
	}
	if cfg.Config.MaxConcurrent == 0 {
		cfg.Config.MaxConcurrent = maxActiveJournals
	}
	if cfg.Config.SyntheticMemoryBytes < 0 || cfg.Config.SyntheticMemoryBytes > 0 && !cfg.Config.SyntheticBackend {
		return errors.New("synthetic demand requires explicitly approved backend")
	}
	if cfg.Config.SyntheticPhase == "" {
		cfg.Config.SyntheticPhase = "unit"
	}
	if cfg.Config.SyntheticPhase != "unit" && cfg.Config.SyntheticPhase != "prepare" {
		return errors.New("invalid synthetic stop phase")
	}
	if e := envelope.Validate(); e != nil {
		return e
	}
	b := envelope.Bundle
	var handoff time.Time
	for _, member := range b.Members {
		if member.Work.Assignment.HandoffDeadline.After(handoff) {
			handoff = member.Work.Assignment.HandoffDeadline
		}
	}
	ctx, cancel := context.WithDeadline(ctx, handoff)
	defer cancel()
	if cfg.Config.Client != nil {
		return errors.New("finite execution must not have a connected client")
	}
	if runtime.GOOS != "darwin" {
		return errors.New("finite execution requires qualified native Darwin observations")
	}
	if cfg.Config.WorkerID != b.WorkerID || cfg.Config.SessionID != b.SessionID || cfg.Config.HostID != b.HostID {
		return errors.New("finite provisioned identity mismatch")
	}
	bindings := map[string]FiniteRuntimeBinding{}
	for _, binding := range cfg.Runtimes {
		if _, ok := bindings[binding.Reference.Runtime]; ok {
			return errors.New("duplicate local runtime approval")
		}
		bindings[binding.Reference.Runtime] = binding
	}
	for _, ref := range b.RuntimeRefs {
		local, ok := bindings[ref.Runtime]
		a, _ := contracts.Digest(ref)
		d, _ := contracts.Digest(local.Reference)
		if !ok || a != d || len(local.Argv) == 0 || !filepath.IsAbs(local.Argv[0]) {
			return errors.New("finite runtime lacks exact local deployment approval")
		}
	}
	if len(b.RuntimeRefs) > 0 {
		cfg.Config.Runtime = b.RuntimeRefs[0].Runtime
	}
	h, e := openHost(cfg.Config)
	if e != nil {
		return e
	}
	defer h.Close()
	if h.inventory.Pending != nil {
		return errors.New("unresolved connected poll must be reconciled before finite execution")
	}
	if e = h.initializeLocalSpool(); e != nil {
		return e
	}
	if e = h.installBudget(*b.Members[0].Work.Budget); e != nil {
		return e
	}
	members := map[string]contracts.FiniteMember{}
	for _, m := range b.Members {
		members[m.Work.Assignment.AssignmentID] = m
	}
	hostUncertain := false
	for id, r := range h.active {
		member, ok := members[id]
		prior, _ := contracts.Digest(r.j.Work)
		expected, _ := contracts.Digest(member.Work)
		if !ok || prior != expected {
			return errors.New("finite root contains unrelated/different retained execution")
		}
		if r.j.Finite == nil {
			if r.j.ProcessStarted || r.j.Quiescent || r.j.ReceiptAcknowledged || len(r.j.Started) > 0 || len(r.j.Candidates) > 0 || r.j.Control.Revision != 0 {
				return errors.New("retained work lacks finite export binding")
			}
		} else if r.j.Finite.BundleID != b.BundleID || r.j.Finite.BundleDigest != envelope.BundleDigest || r.j.Finite.ExportRevision != b.ExportRevision {
			return errors.New("finite export binding changed")
		}
		if r.j.ProcessStarted && !r.j.Quiescent {
			r.j.Uncertain = true
			r.j.GateClosed = true
			r.j.OperationalReason = "restart has no owned child exit proof"
			if e = r.save(); e != nil {
				return e
			}
		}
		if r.j.Uncertain {
			hostUncertain = true
		}
	}
	var runners []*runner
	for _, m := range b.Members {
		a := m.Work.Assignment
		if h.active[a.AssignmentID] == nil {
			if _, e = h.readFiniteArchive(m, envelope); e == nil {
				continue
			} else if !errors.Is(e, os.ErrNotExist) {
				return e
			}
		}
		binding := bindings[a.Runtime]
		h.cfg.Runtime = a.Runtime
		h.cfg.RuntimeArgv = binding.Argv
		r, err := h.receive(m.Work)
		if err != nil {
			return err
		}
		r.cfg.RuntimeArgv = append([]string{}, binding.Argv...)
		r.cfg.Runtime = a.Runtime
		r.finiteRuntime = &binding
		if r.j.Finite == nil {
			r.j.Finite = &finiteJournal{BundleID: b.BundleID, BundleDigest: envelope.BundleDigest, ExportRevision: b.ExportRevision}
			r.j.ReconcileRevision = 1
			if err = r.save(); err != nil {
				return err
			}
		}
		if hostUncertain {
			r.j.GateClosed = true
			if e = r.save(); e != nil {
				return e
			}
		}
		runners = append(runners, r)
	}
	if len(runners) == 0 {
		return nil
	}
	if e = h.trackFiniteSources(cfg, envelope, runners[0].j.Work.Assignment.AssignmentID); e != nil {
		return e
	}
	output, e := newFiniteOutput(ctx, h, envelope)
	if e != nil {
		return e
	}
	defer output.Close()
	for _, r := range runners {
		r.finiteOutput = output
	}
	if e = output.bundle(runners[0]); e != nil {
		return e
	}
	source, e := os.OpenRoot(cfg.BundleDirectory)
	if e != nil {
		return e
	}
	defer source.Close()
	slots := make(chan struct{}, h.cfg.MaxConcurrent)
	results := make(chan error, len(runners))
	var group sync.WaitGroup
	for _, r := range runners {
		group.Add(1)
		go func(r *runner) {
			defer group.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				results <- ctx.Err()
				return
			}
			results <- r.runFinite(ctx, source, output)
		}(r)
	}
	group.Wait()
	close(results)
	var result error
	for err := range results {
		result = errors.Join(result, err)
	}
	return result
}

// Supplied external files remain deployment-owned. Exact referenced paths that
// already reside inside the managed root must still be indexed, including files
// provisioned after its one-time metadata bootstrap. No source/history scan.
func (h *host) trackManagedSource(owner, path string) error {
	root, e := filepath.EvalSymlinks(h.cfg.Root)
	if e != nil {
		return e
	}
	resolved, e := filepath.EvalSymlinks(path)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	resolved, e = filepath.Abs(resolved)
	if e != nil {
		return e
	}
	relative, e := filepath.Rel(root, resolved)
	if e != nil {
		return e
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil
	}
	return h.spool.TrackFile(owner, filepath.Join(h.cfg.Root, relative))
}
func (h *host) trackFiniteSources(cfg FiniteConfig, envelope contracts.FiniteBundleEnvelope, owner string) error {
	track := func(path string) error { return h.trackManagedSource(owner, path) }
	var e error
	if e = track(filepath.Join(cfg.BundleDirectory, "bundle.json")); e != nil {
		return e
	}
	for _, artifact := range envelope.Bundle.Blobs {
		if e = track(filepath.Join(cfg.BundleDirectory, "blobs", artifact.Digest)); e != nil {
			return e
		}
	}
	bindings := map[string]FiniteRuntimeBinding{}
	for _, binding := range cfg.Runtimes {
		bindings[binding.Reference.Runtime] = binding
	}
	for _, ref := range envelope.Bundle.RuntimeRefs {
		binding := bindings[ref.Runtime]
		if len(binding.Argv) > 0 {
			if e = track(binding.Argv[0]); e != nil {
				return e
			}
		}
		for _, support := range ref.Support {
			if path, ok := binding.SupportPaths[support.Digest]; ok {
				if e = track(path); e != nil {
					return e
				}
			}
		}
	}
	return nil
}
func (r *runner) checkFiniteEnvironment(ctx context.Context) error {
	if r.finiteRuntime == nil {
		return errors.New("missing local finite runtime approval")
	}
	ref := r.finiteRuntime.Reference
	build := platform.Current()
	if build.GOOS != "darwin" || ref.GOOS != build.GOOS || ref.GOARCH != build.GOARCH || build.ISABaseline == "unknown" || ref.ISABaseline != build.ISABaseline {
		return errors.New("finite runtime build/target/ISA binding is not qualified")
	}
	observed, e := platform.ObserveResources(r.admission.root)
	if e != nil {
		return e
	}
	for _, u := range r.j.Work.Units {
		if !contracts.SupportsCapabilities(u.RequiredCapabilities, build.GOOS, build.GOARCH, observed.HardTreeMemory, r.j.Work.Assignment.Allocation) {
			return errors.New("finite unit requires unavailable capability")
		}
	}
	if e := verifyFiniteRuntimeFile(ctx, r.cfg.RuntimeArgv[0], ref.Executable); e != nil {
		return e
	}
	for _, artifact := range ref.Support {
		path, ok := r.finiteRuntime.SupportPaths[artifact.Digest]
		if !ok {
			return errors.New("approved runtime support artifact lacks local path binding")
		}
		if e := verifyFiniteRuntimeFile(ctx, path, artifact); e != nil {
			return e
		}
	}
	return nil
}
func verifyFiniteRuntimeFile(ctx context.Context, path string, artifact contracts.Artifact) error {
	if !filepath.IsAbs(path) {
		return errors.New("approved runtime file path must be absolute")
	}
	if artifact.Size < 0 || artifact.Size == math.MaxInt64 {
		return errors.New("invalid runtime file size bound")
	}
	root, e := os.OpenRoot(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer root.Close()
	f, e := openRegular(root, filepath.Base(path))
	if e != nil {
		return e
	}
	defer f.Close()
	size, e := f.Stat()
	if e != nil {
		return e
	}
	if size.Size() != artifact.Size {
		return errors.New("local approved executable size mismatch")
	}
	hasher := sha256.New()
	n, e := io.Copy(hasher, io.LimitReader(finiteContextReader{ctx, f}, artifact.Size+1))
	if e != nil {
		return e
	}
	if n != artifact.Size || hex.EncodeToString(hasher.Sum(nil)) != artifact.Digest {
		return errors.New("local approved executable digest mismatch")
	}
	return nil
}
func (r *runner) runFinite(ctx context.Context, source *os.Root, out *finiteOutput) error {
	r.mu.Lock()
	uncertain := r.j.Uncertain
	quiescent := r.j.Quiescent
	r.mu.Unlock()
	var executionErr error
	if !quiescent && !uncertain {
		executionErr = r.checkFiniteEnvironment(ctx)
		var admitted bool
		if executionErr == nil {
			admitted, executionErr = r.waitAdmission(ctx)
		}
		if admitted && executionErr == nil {
			inputCtx, cancelInputs := context.WithDeadline(ctx, r.j.Work.Assignment.TerminateAt)
			seen := map[string]bool{}
			for _, artifact := range workInputs(r.j.Work) {
				if seen[artifact.Digest] {
					continue
				}
				seen[artifact.Digest] = true
				verified, e := r.spool.Verified(artifact.Digest, artifact.Size)
				if e != nil {
					executionErr = e
					break
				}
				if verified {
					continue
				}
				f, e := openRegular(source, "blobs/"+artifact.Digest)
				if e != nil {
					executionErr = e
					break
				}
				e = r.publishBlob(inputCtx, artifact.Digest, artifact.Size, finiteContextReader{inputCtx, f})
				f.Close()
				if e != nil {
					executionErr = e
					break
				}
			}
			cancelInputs()
			if executionErr == nil {
				executionErr = r.execute(ctx)
			}
		}
		r.mu.Lock()
		if !r.j.ProcessStarted && !r.j.Quiescent {
			r.j.Quiescent = true
			r.j.GateClosed = true
			if executionErr != nil {
				r.j.OperationalReason = executionErr.Error()
			}
			executionErr = errors.Join(executionErr, r.save())
		}
		quiescent = r.j.Quiescent
		r.mu.Unlock()
		if quiescent {
			r.admission.quiescent(r.j.Work.Assignment.AssignmentID)
		}
	}
	r.mu.Lock()
	candidates := append([]contracts.Candidate{}, r.j.Candidates...)
	r.mu.Unlock()
	for _, candidate := range candidates {
		if e := out.candidate(r, candidate); e != nil {
			return errors.Join(executionErr, e)
		}
	}
	if e := out.final(r); e != nil {
		return errors.Join(executionErr, e)
	}
	if uncertain {
		return errors.Join(executionErr, errors.New("retained finite execution is uncertain; no new starts"))
	}
	return executionErr
}
func (r *runner) finiteInventory() contracts.FiniteFinalInventory {
	// Caller holds r.mu. This is a fact snapshot, not a delivered reconciliation.
	a := r.j.Work.Assignment
	digest, _ := contracts.Digest(*a)
	receipt := r.receipt("")
	receipt.RequestID = contracts.Hash([]byte(r.j.Finite.BundleDigest + digest + "reconcile"))
	reconciliation := contracts.ReconcileRequest{Receipt: receipt, Revision: r.j.ReconcileRevision, AssignmentDigest: digest, OperationalReason: r.j.OperationalReason, ProcessStarted: r.j.ProcessStarted, Quiescent: r.j.Quiescent, Started: append([]string{}, r.j.Started...)}
	for _, candidate := range r.j.Candidates {
		d, _ := contracts.Digest(candidate)
		reconciliation.CandidateDigests = append(reconciliation.CandidateDigests, d)
	}
	inventory := contracts.FiniteFinalInventory{SchemaVersion: 1, BundleDigest: r.j.Finite.BundleDigest, AssignmentDigest: digest, Uncertain: r.j.Uncertain, Reconciliation: reconciliation, ResourceStop: r.j.ResourceStop}
	if r.j.Quiescent && !r.j.Uncertain {
		final := r.finalSnapshotLocked()
		final.RequestID = contracts.Hash([]byte(r.j.Finite.BundleDigest + digest + "final"))
		inventory.Final = &final
	}
	return inventory
}

// ApplyFiniteImportAck changes only facts explicitly acknowledged by a real
// import. It never uses a deadline, an output file, or scientific acceptance.
// File-reading callers pass their source path so noncanonical managed ACK files
// are charged too. The canonical exchange/<BundleID>/ack.json is always checked;
// preprovisioned external readonly ACK sources remain deployment-owned.
func ApplyFiniteImportAck(root string, ack contracts.FiniteImportAck, sourcePaths ...string) error {
	if e := ack.Validate(); e != nil {
		return e
	}
	identityData, e := os.ReadFile(filepath.Join(root, "identity.json"))
	if e != nil {
		return e
	}
	var identity struct{ Worker, Session string }
	if e = contracts.Decode(bytes.NewReader(identityData), &identity); e != nil {
		return e
	}
	h, e := openHost(Config{Root: root, WorkerID: identity.Worker, SessionID: identity.Session, MaxArtifactBytes: 1 << 40})
	if e != nil {
		return e
	}
	defer h.Close()
	if e = h.initializeLocalSpool(); e != nil {
		return e
	}
	file, e := openRegular(h.spool.fs, filepath.ToSlash(filepath.Join("exchange", ack.BundleID, "bundle.json")))
	if e != nil {
		return e
	}
	var envelope contracts.FiniteBundleEnvelope
	e = contracts.Decode(file, &envelope)
	file.Close()
	if e != nil {
		return e
	}
	if e = envelope.Validate(); e != nil {
		return e
	}
	if envelope.BundleDigest != ack.BundleDigest || envelope.Bundle.BundleID != ack.BundleID {
		return errors.New("ACK does not address retained original bundle")
	}
	originals := map[string]contracts.FiniteMember{}
	for _, member := range envelope.Bundle.Members {
		originals[member.AssignmentDigest] = member
	}
	type update struct {
		target   *runner
		next     journal
		archived bool
	}
	var updates []update
	for _, member := range ack.Members {
		original, ok := originals[member.AssignmentDigest]
		if !ok {
			return errors.New("ACK assignment not in original bundle")
		}
		var target *runner
		for _, r := range h.active {
			d, _ := contracts.Digest(*r.j.Work.Assignment)
			if d == member.AssignmentDigest {
				target = r
				break
			}
		}
		archived := false
		if target == nil {
			target, e = h.readFiniteArchive(original, envelope)
			if e != nil {
				return e
			}
			archived = true
		}
		prior, _ := contracts.Digest(target.j.Work)
		expected, _ := contracts.Digest(original.Work)
		if prior != expected {
			return errors.New("ACK member differs from original work")
		}
		data, e := json.Marshal(target.j)
		if e != nil {
			return e
		}
		var next journal
		if e = json.Unmarshal(data, &next); e != nil {
			return e
		}
		proposal := &runner{j: next}
		if e = applyFiniteMemberAckLocked(proposal, ack, member); e != nil {
			return e
		}
		updates = append(updates, update{target: target, next: proposal.j, archived: archived})
	}
	// Operator import may have retained the ACK after spool initialization.
	// Track only exact sources after all malformed-body checks, under this lock.
	owner := envelope.Bundle.Members[0].Work.Assignment.AssignmentID
	canonical := filepath.Join(h.cfg.Root, "exchange", ack.BundleID, "ack.json")
	if e = h.trackManagedSource(owner, canonical); e != nil {
		return e
	}
	for _, path := range sourcePaths {
		if e = h.trackManagedSource(owner, path); e != nil {
			return e
		}
	}
	// Every body/binding is checked before any journal changes. A subsequent
	// operational write failure may retain a prefix of genuine acknowledgments.
	for _, update := range updates {
		if update.archived {
			continue
		}
		r := update.target
		prior := r.j
		r.j = update.next
		if e = r.save(); e != nil {
			r.j = prior
			return e
		}
		if r.j.FinalSent {
			if e = h.retire(r); e != nil {
				return e
			}
		}
	}
	return nil
}
func (h *host) readFiniteArchive(original contracts.FiniteMember, envelope contracts.FiniteBundleEnvelope) (*runner, error) {
	id := original.Work.Assignment.AssignmentID
	relative := filepath.Join("completed", contracts.Hash([]byte(id)), "journal.json")
	file, e := openRegular(h.spool.fs, filepath.ToSlash(relative))
	if e != nil {
		return nil, e
	}
	data, e := io.ReadAll(io.LimitReader(file, maxPersistedJournalBytes+1))
	file.Close()
	if e != nil {
		return nil, e
	}
	var j journal
	if e = json.Unmarshal(data, &j); e != nil {
		return nil, e
	}
	actual, _ := contracts.Digest(j.Work)
	expected, _ := contracts.Digest(original.Work)
	if actual != expected || !j.FinalSent || !j.Quiescent || j.Uncertain || j.Finite == nil || j.Finite.BundleID != envelope.Bundle.BundleID || j.Finite.BundleDigest != envelope.BundleDigest || j.Finite.ExportRevision != envelope.Bundle.ExportRevision {
		return nil, errors.New("finite archive lacks exact acknowledged original binding")
	}
	local := h.cfg
	local.Root = h.completedRoot(id)
	return &runner{cfg: local, j: j, store: h.store, spool: h.spool}, nil
}
func applyFiniteMemberAckLocked(r *runner, ack contracts.FiniteImportAck, member contracts.FiniteMemberAck) error {
	if r.j.Finite == nil || r.j.Finite.BundleDigest != ack.BundleDigest {
		return errors.New("ACK finite bundle mismatch")
	}
	known := map[string]bool{}
	for _, c := range r.j.Candidates {
		d, _ := contracts.Digest(c)
		known[d] = true
	}
	accepted := map[string]bool{}
	for _, d := range r.j.Finite.RawAcknowledged {
		accepted[d] = true
	}
	for _, d := range member.RawPublished {
		if !known[d] {
			return errors.New("ACK names unknown candidate")
		}
		accepted[d] = true
	}
	if member.ReconciliationRevision != 0 && member.ReconciliationRevision != r.j.ReconcileRevision {
		return errors.New("ACK reconciliation revision mismatch")
	}
	if member.ResourceStopAcknowledged && r.j.ResourceStop == nil {
		return errors.New("ACK names absent resource evidence")
	}
	inventory := r.finiteInventory()
	if e := inventory.ValidateForAssignment(*r.j.Work.Assignment); e != nil {
		return e
	}
	if member.FinalDigest != "" {
		if inventory.Final == nil {
			return errors.New("ACK cannot prove local quiescence")
		}
		d, _ := contracts.Digest(*inventory.Final)
		if d != member.FinalDigest || len(accepted) != len(known) || r.j.ResourceStop != nil && !member.ResourceStopAcknowledged && !r.j.ResourceStopAcknowledged {
			return errors.New("ACK final body/publication/evidence mismatch")
		}
	}
	r.j.Finite.RawAcknowledged = nil
	for _, c := range r.j.Candidates {
		d, _ := contracts.Digest(c)
		if accepted[d] {
			r.j.Finite.RawAcknowledged = append(r.j.Finite.RawAcknowledged, d)
		}
	}
	r.j.Delivered = 0
	for _, c := range r.j.Candidates {
		d, _ := contracts.Digest(c)
		if !accepted[d] {
			break
		}
		r.j.Delivered++
	}
	if member.ResourceStopAcknowledged {
		r.j.ResourceStopAcknowledged = true
	}
	if member.FinalDigest != "" {
		r.j.FinalSent = true
	}
	return nil
}
func (m *admission) observeResources() (contracts.HostObservation, error) {
	if m.observe != nil {
		return m.observe(m.root)
	}
	return platform.ObserveResources(m.root)
}
