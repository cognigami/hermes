package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"hermes/internal/contracts"

	"golang.org/x/sys/unix"
)

// WorkflowAdapterConfig approves one pure, side-effect-free leaf executable.
// The controller root and its private executable copies must remain protected
// from external mutation, just like the existing immutable artifact store.
type WorkflowAdapterConfig struct {
	Contract, RuntimeDigest       string
	Argv                          []string
	Timeout                       time.Duration
	MaxMetadataBytes, MaxLogBytes int
}
type approvedWorkflowAdapter struct {
	cfg                   WorkflowAdapterConfig
	directory, executable string
}

// Unknown exit can leave the shared runner's I/O goroutines alive. Diagnostics
// may snapshot these bounded buffers without racing those retained writers.
type workflowBuffer struct {
	mu sync.Mutex
	limitedBuffer
}

func (b *workflowBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limitedBuffer.Write(p)
}
func (b *workflowBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limitedBuffer.String()
}
func (b *workflowBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.limitedBuffer.Bytes()...)
}

// copyWorkflowFile never writes beyond its admitted per-path size. Cancellation
// closes the owned handles and joins the copy before the caller can clean up.
func copyWorkflowFile(ctx context.Context, dst, src *os.File, size int64, digest io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		var target io.Writer = dst
		if digest != nil {
			target = io.MultiWriter(dst, digest)
		}
		if _, err := io.CopyN(target, src, size); err != nil {
			done <- err
			return
		}
		var extra [1]byte
		n, err := src.Read(extra[:])
		if n != 0 {
			done <- errors.New("workflow source exceeds reserved copy size")
			return
		}
		if err != io.EOF {
			if err == nil {
				err = io.ErrNoProgress
			}
			done <- err
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		src.Close()
		dst.Close()
		return errors.Join(ctx.Err(), <-done)
	}
}

func (c *Controller) approveWorkflowAdapter(ctx context.Context, cfg WorkflowAdapterConfig) (approvedWorkflowAdapter, error) {
	cfg.Argv = append([]string(nil), cfg.Argv...)
	a := approvedWorkflowAdapter{cfg: cfg}
	if cfg.Contract == "" || len(cfg.Argv) == 0 || !filepath.IsAbs(cfg.Argv[0]) || cfg.Timeout <= 0 || cfg.MaxMetadataBytes < 1 || cfg.MaxMetadataBytes > contracts.MaxMetadataBytes || cfg.MaxLogBytes < 1 || cfg.MaxLogBytes > contracts.MaxMessageBytes {
		return a, errors.New("invalid workflow adapter approval")
	}
	// Nonblocking open prevents a mistaken FIFO path from hanging startup.
	// Inspect this same handle before reading; a path-only check would race.
	fd, err := unix.Open(cfg.Argv[0], unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return a, err
	}
	src := os.NewFile(uintptr(fd), cfg.Argv[0])
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return a, err
	}
	if !info.Mode().IsRegular() {
		return a, errors.New("workflow adapter executable must be a regular file")
	}
	relative := "workflow-runtime-" + randomID()
	if err = c.ReserveControllerStorage(ctx, relative, info.Size()); err != nil {
		return a, err
	}
	// These bytes (including failed partial copies) remain permanently charged.
	dir := filepath.Join(c.cfg.Root, relative)
	a.directory = dir
	if err = os.Mkdir(dir, 0700); err != nil {
		return a, err
	}
	a.executable = filepath.Join(dir, "adapter")
	dst, err := os.OpenFile(a.executable, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0500)
	if err != nil {
		return a, err
	}
	hash := sha256.New()
	err = copyWorkflowFile(ctx, dst, src, info.Size(), hash)
	if err == nil {
		err = dst.Sync()
	}
	closeErr := dst.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return a, err
	}
	if hex.EncodeToString(hash.Sum(nil)) != cfg.RuntimeDigest {
		return a, errors.New("workflow adapter executable digest mismatch")
	}
	for _, path := range []string{dir, c.cfg.Root} {
		parent, err := os.Open(path)
		if err != nil {
			return a, err
		}
		err = parent.Sync()
		closeErr := parent.Close()
		if err != nil {
			return a, err
		}
		if closeErr != nil {
			return a, closeErr
		}
	}
	// Execute this protected retained copy, never the changed source path.
	return a, nil
}
func (c *Controller) invokeWorkflowAdapter(ctx context.Context, a approvedWorkflowAdapter, input contracts.WorkflowAdapterInput) (contracts.WorkflowAdapterOutput, error) {
	var out contracts.WorkflowAdapterOutput
	if err := input.Validate(); err != nil {
		return out, err
	}
	if input.Contract != a.cfg.Contract || input.RuntimeDigest != a.cfg.RuntimeDigest {
		return out, errors.New("workflow does not match approved adapter contract/runtime")
	}
	data := encoded(input)
	if len(data) > a.cfg.MaxMetadataBytes {
		return out, errors.New("workflow adapter input too large")
	}
	minimum, err := (contracts.ResourceVector{SpoolBytes: input.Decision.Size}).Add(contracts.ResourceVector{SpoolBytes: input.Settings.Size})
	if err != nil {
		return out, err
	}
	claim, err := c.ReserveControllerChild(ctx, "continuation", input.WorkflowID, minimum)
	if err != nil {
		return out, err
	}
	relative := "continuation-" + claim.id
	// Before Start, cancellation proves no child; use the shared claim cleanup.
	cancelUnstarted := func(cause error) error {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := claim.CancelBeforeStart(cleanup)
		if err == nil {
			err = claim.CleanupScratch(cleanup)
		}
		return errors.Join(cause, err)
	}
	if err = claim.ReserveScratch(ctx, relative, minimum.SpoolBytes); err != nil {
		return out, cancelUnstarted(err)
	}
	dir := filepath.Join(c.cfg.Root, relative)
	if err = os.Mkdir(dir, 0700); err != nil {
		return out, cancelUnstarted(err)
	}
	for _, f := range []struct {
		name string
		ref  contracts.Artifact
	}{{input.DecisionFile, input.Decision}, {input.SettingsFile, input.Settings}} {
		src, err := c.store.Open(f.ref.Digest)
		if err != nil {
			return out, cancelUnstarted(err)
		}
		dst, err := os.OpenFile(filepath.Join(dir, f.name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0400)
		if err != nil {
			src.Close()
			return out, cancelUnstarted(err)
		}
		copyErr := copyWorkflowFile(ctx, dst, src, f.ref.Size, nil)
		srcErr := src.Close()
		dstErr := dst.Close()
		if err := errors.Join(copyErr, srcErr, dstErr); err != nil {
			return out, cancelUnstarted(err)
		}
	}
	run, cancel := context.WithTimeout(ctx, a.cfg.Timeout)
	defer cancel()
	cmd := exec.Command(a.executable, a.cfg.Argv[1:]...)
	cmd.Dir = dir
	cmd.Stdin = bytes.NewReader(data)
	stdout := &workflowBuffer{limitedBuffer: limitedBuffer{max: a.cfg.MaxMetadataBytes}}
	stderr := &workflowBuffer{limitedBuffer: limitedBuffer{max: a.cfg.MaxLogBytes}}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err = runClaimedLeaf(run, claim, cmd); err != nil {
		return out, fmt.Errorf("workflow adapter failed: %w: %s", err, stderr.String())
	}
	if err = contracts.Decode(bytes.NewReader(stdout.Bytes()), &out); err != nil {
		return out, err
	}
	return out, out.Validate()
}

func (c *Controller) advanceWorkflow(ctx context.Context, a approvedWorkflowAdapter, id string) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	x, err := workflowSnapshotFor(tx, id)
	tx.Rollback()
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if x.status.State != "active" {
		return nil
	}
	if x.registration.Contract != a.cfg.Contract || x.registration.RuntimeDigest != a.cfg.RuntimeDigest {
		return errors.New("workflow pinned adapter unavailable")
	}
	if x.output == nil {
		out, err := c.invokeWorkflowAdapter(ctx, a, x.input)
		if err != nil {
			return err
		}
		tx, err = c.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err = persistWorkflowIntent(tx, x, out); err != nil {
			tx.Rollback()
			return err
		}
		if err = c.commit(tx, "workflow-intent"); err != nil {
			tx.Rollback()
			return err
		}
		tx, err = c.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		x, err = workflowSnapshotFor(tx, id)
		tx.Rollback()
		if err != nil {
			return err
		}
	}
	return c.createWorkflowSuccessor(ctx, x)
}

// RunWorkflows is an optional controller service loop. Startup/HTTP wiring is
// separate. It polls bounded metadata pages; it never runs application work or
// waits for old assignments to finish once every stage unit is accepted.
func (c *Controller) RunWorkflows(ctx context.Context, cfg WorkflowAdapterConfig) error {
	var a approvedWorkflowAdapter
	var err error
	for {
		a, err = c.approveWorkflowAdapter(ctx, cfg)
		if !errors.Is(err, ErrControllerCapacity) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err != nil {
		return err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	after := ""
	for {
		rows, err := c.db.QueryContext(ctx, `SELECT id FROM workflows WHERE state='active' AND adapter_attempts<3 AND id>? ORDER BY id LIMIT 16`, after)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			after = ""
		}
		for _, id := range ids {
			after = id
			if err = c.advanceWorkflow(ctx, a, id); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if errors.Is(err, ErrControllerCapacity) {
					continue
				}
				// Operational adapter errors are visible and bounded, never a prescribed
				// stop or domain verdict. Other workflows continue independently.
				reason := err.Error()
				if len(reason) > 4096 {
					reason = reason[:4096]
				}
				if _, err = c.db.ExecContext(ctx, `UPDATE workflows SET reason=?,adapter_attempts=adapter_attempts+1 WHERE id=? AND state='active'`, reason, id); err != nil {
					return err
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
