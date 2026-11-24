package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"golang.org/x/sys/unix"
	"hermes/internal/contracts"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

// finiteOutput has exactly one private root inside the host spool. It is not a
// second artifact store: original verified blobs remain in the shared store.
type finiteOutput struct {
	ctx      context.Context
	h        *host
	envelope contracts.FiniteBundleEnvelope
	fs       *os.Root
	relative string
	mu       sync.Mutex
}

func newFiniteOutput(ctx context.Context, h *host, e contracts.FiniteBundleEnvelope) (*finiteOutput, error) {
	relative := filepath.Join("exchange", e.Bundle.BundleID)
	if err := h.spool.fs.MkdirAll(relative, 0700); err != nil {
		return nil, err
	}
	fs, err := h.spool.fs.OpenRoot(relative)
	if err != nil {
		return nil, err
	}
	for _, dir := range []string{"blobs", "candidates", "finals", "pending"} {
		if err = fs.MkdirAll(dir, 0700); err != nil {
			fs.Close()
			return nil, err
		}
	}
	return &finiteOutput{ctx: ctx, h: h, envelope: e, fs: fs, relative: relative}, nil
}
func (o *finiteOutput) Close() error { return o.fs.Close() }
func (o *finiteOutput) record(r *runner, path string, size int64) error {
	return o.h.spool.record(o.ctx, spoolKey(filepath.Join(o.relative, filepath.FromSlash(path))), size, r.j.Work.Assignment.AssignmentID, false)
}

// Only the finite output writer changes these exact file keys. Retain a
// larger prior intent/physical file until durable replacement proves shrinkage.
func (o *finiteOutput) reserve(r *runner, path string, size int64) error {
	if info, e := o.fs.Lstat(path); e == nil {
		if !info.Mode().IsRegular() {
			return errors.New("finite output must be regular")
		}
		size = max(size, info.Size())
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	var prior int64
	e := o.h.spool.db.QueryRow("SELECT size FROM spool_files WHERE key=?", spoolKey(filepath.Join(o.relative, filepath.FromSlash(path)))).Scan(&prior)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	return o.record(r, path, max(prior, size))
}
func (o *finiteOutput) metadata(r *runner, path string, value any) error {
	body, e := json.Marshal(value)
	if e != nil {
		return e
	}
	if len(body) > contracts.MaxMetadataBytes {
		return errors.New("finite output metadata exceeds bound")
	}
	return o.write(r, path, int64(len(body)), bytes.NewReader(body), contracts.Hash(body))
}
func (o *finiteOutput) bundle(r *runner) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.metadata(r, "bundle.json", o.envelope)
}
func (o *finiteOutput) candidate(r *runner, candidate contracts.Candidate) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	digest, e := contracts.Digest(candidate)
	if e != nil {
		return e
	}
	path := "candidates/" + digest + ".json"
	receipt := r.receipt("")
	receipt.RequestID = contracts.Hash([]byte(o.envelope.BundleDigest + candidate.AssignmentID + digest))
	report := contracts.CandidateReport{Receipt: receipt, Candidate: candidate}
	body, e := json.Marshal(report)
	if e != nil {
		return e
	}
	// An identical durable candidate already implies its referenced copies were
	// synced first. Never rehash its accepted payload prefix during replay.
	if f, e := openRegular(o.fs, path); e == nil {
		prior, readErr := io.ReadAll(io.LimitReader(f, contracts.MaxMetadataBytes+1))
		f.Close()
		if readErr != nil {
			return readErr
		}
		if bytes.Equal(prior, body) {
			return nil
		}
		return errors.New("finite candidate body conflict")
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	seen := map[string]bool{}
	for _, file := range candidate.Files {
		a := file.Artifact
		if seen[a.Digest] {
			continue
		}
		seen[a.Digest] = true
		verified, e := r.spool.Verified(a.Digest, a.Size)
		if e != nil {
			return e
		}
		if !verified {
			return errors.New("finite candidate blob lacks durable verification")
		}
		source, e := r.store.Open(a.Digest)
		if e != nil {
			return e
		}
		e = o.write(r, "blobs/"+a.Digest, a.Size, source, a.Digest)
		source.Close()
		if e != nil {
			return e
		}
	}
	return o.write(r, path, int64(len(body)), bytes.NewReader(body), contracts.Hash(body))
}
func (o *finiteOutput) final(r *runner) error {
	r.mu.Lock()
	inventory := r.finiteInventory()
	r.mu.Unlock()
	if e := inventory.ValidateForAssignment(*r.j.Work.Assignment); e != nil {
		return e
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.metadata(r, "finals/"+inventory.AssignmentDigest+".json", inventory)
}

// Reserve BOTH pending-copy intent and eventual retained path before writing.
// Failed intents remain charged. No root/history scan or cleanup-as-proof.
func (o *finiteOutput) write(r *runner, path string, size int64, source io.Reader, digest string) error {
	if !contracts.ValidPath(path) || size < 0 || size == math.MaxInt64 {
		return errors.New("invalid finite output path/size")
	}
	pending := "pending/" + contracts.Hash([]byte(path))
	if e := o.reserve(r, pending, size); e != nil {
		return e
	}
	if e := o.reserve(r, path, size); e != nil {
		return e
	}
	if e := o.ctx.Err(); e != nil {
		return e
	}
	observed, e := r.admission.observeResources()
	if e != nil {
		return e
	}
	if size > observed.AvailableSpoolBytes {
		return errors.New("finite output lacks native free space")
	}
	ok, e := r.admission.try(r.j.Work.Assignment.AssignmentID)
	if e != nil {
		return e
	}
	if !ok {
		return errors.New("finite output exceeds approved aggregate capacity")
	}
	// Existing immutable blobs/bundle metadata must match; finals may evolve only
	// from nonterminal evidence, never an already positively quiescent body.
	if existing, e := openRegular(o.fs, path); e == nil {
		hasher := sha256.New()
		n, readErr := io.Copy(hasher, existing)
		existing.Close()
		if readErr != nil {
			return readErr
		}
		if n == size && hex.EncodeToString(hasher.Sum(nil)) == digest {
			if e := o.record(r, path, size); e != nil {
				return e
			}
			return o.clearPending(r, pending)
		}
		if path == "bundle.json" || len(path) > 6 && path[:6] == "blobs/" {
			return errors.New("immutable finite output conflict")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if info, err := o.fs.Lstat(pending); err == nil && !info.Mode().IsRegular() {
		return errors.New("finite pending copy must be regular")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, e := o.fs.OpenFile(pending, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return e
	}
	hasher := sha256.New()
	n, e := io.Copy(io.MultiWriter(f, hasher), io.LimitReader(finiteContextReader{o.ctx, source}, size+1))
	if e == nil && (n != size || hex.EncodeToString(hasher.Sum(nil)) != digest) {
		e = errors.New("finite output size/digest mismatch")
	}
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = o.fs.Rename(pending, path); e != nil {
		return e
	}
	directory, e := o.fs.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	e = directory.Sync()
	directory.Close()
	if e != nil {
		return e
	}
	if e = o.record(r, path, size); e != nil {
		return e
	}
	return o.clearPending(r, pending)
}
func (o *finiteOutput) clearPending(r *runner, path string) error {
	if info, e := o.fs.Lstat(path); e == nil && !info.Mode().IsRegular() {
		return errors.New("finite pending path must be regular")
	} else if e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	f, e := o.fs.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return e
	}
	e = f.Sync()
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	dir, e := o.fs.Open("pending")
	if e != nil {
		return e
	}
	e = dir.Sync()
	dir.Close()
	if e != nil {
		return e
	}
	return o.record(r, path, 0)
}

type finiteContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r finiteContextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.reader.Read(p)
}
