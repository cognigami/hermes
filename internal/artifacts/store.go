// Package artifacts publishes immutable SHA256-addressed bytes before metadata
// may refer to them. The root must be on a trusted local filesystem.
package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ErrIngressCleanup means unpublished scratch could not be removed. Callers must
// retain the affected transfer/storage reservation until exclusive-owner reopen
// successfully cleans the abandoned scratch.
var ErrIngressCleanup = errors.New("artifact ingress scratch cleanup failed")

// Store requires exclusive application ownership of its directory. Published
// files must never be modified or removed, including by callers of Open.
// MaxSize bounds each publication; transfer concurrency is the caller's concern.
type Store struct {
	root    string
	maxSize int64
	// Narrow I/O seams let package tests inject durability failures. Production
	// stores always use the real filesystem operations below.
	syncFile      func(*os.File) error
	syncDirectory func(string) error
	removeIngress func(string) error
}

// New must run under the caller's exclusive store authority before any live
// publishers start. It removes only unacknowledged ingress scratch left by a
// previous publisher process; canonical blobs are never garbage-collected.
func New(root string, maxSize int64) (*Store, error) {
	root = filepath.Clean(root)
	if maxSize <= 0 {
		return nil, errors.New("artifact limit must be positive")
	}
	if err := os.Mkdir(root, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("artifact root must be a real directory")
	}
	// Persist the root directory entry as well as its contents. Callers must
	// provision its parent directory on the same supported local filesystem.
	if err := syncDir(filepath.Dir(root)); err != nil {
		return nil, err
	}
	if err := cleanAbandonedIngress(root); err != nil {
		return nil, err
	}
	return &Store{root: root, maxSize: maxSize, syncFile: (*os.File).Sync, syncDirectory: syncDir, removeIngress: os.Remove}, nil
}

func cleanAbandonedIngress(path string) error {
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	// Scan names in bounded batches, retaining only abandoned scratch names.
	// Finish enumeration before unlinking so directory changes cannot skip names.
	var scratch []string
	for {
		names, err := dir.Readdirnames(128)
		for _, name := range names {
			if strings.HasPrefix(name, ".ingress-") {
				scratch = append(scratch, name)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	for _, name := range scratch {
		info, err := root.Lstat(name)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("nonregular entry in reserved ingress namespace")
		}
		if err := root.Remove(name); err != nil {
			return err
		}
	}
	if len(scratch) != 0 {
		return dir.Sync()
	}
	return nil
}

func validDigest(digest string) bool {
	if len(digest) != 64 {
		return false
	}
	for _, c := range digest {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// Publish verifies length and digest while streaming to a private file in the
// artifact filesystem. It syncs bytes, installs without replacement, then syncs
// directory metadata before success. Failure may leave an unreferenced blob,
// never a success for incomplete bytes. Re-delivery still verifies incoming data.
func (s *Store) Publish(ctx context.Context, digest string, size int64, src io.Reader) (result error) {
	if !validDigest(digest) {
		return errors.New("invalid SHA256 digest")
	}
	if size < 0 || size > s.maxSize {
		return errors.New("artifact size outside configured limit")
	}
	f, err := os.CreateTemp(s.root, ".ingress-")
	if err != nil {
		return err
	}
	defer func() {
		if err := s.removeIngress(f.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, fmt.Errorf("%w: %w", ErrIngressCleanup, err))
		}
	}()
	defer f.Close()
	hash := sha256.New()
	// Read the declared bytes and one extra byte, without overflow at MaxInt64.
	n, err := io.Copy(io.MultiWriter(f, hash), io.LimitReader(contextReader{ctx, src}, size))
	if err != nil {
		return err
	}
	if n != size {
		return errors.New("artifact length mismatch")
	}
	var extra [1]byte
	nExtra, err := io.ReadFull(contextReader{ctx, src}, extra[:])
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if nExtra != 0 {
		return errors.New("artifact exceeds declared length")
	}
	if hex.EncodeToString(hash.Sum(nil)) != digest {
		return errors.New("artifact digest mismatch")
	}
	if err := s.syncFile(f); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	dest := filepath.Join(s.root, digest)
	if err := os.Link(f.Name(), dest); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		// Existing immutable content was already verified on ingress. Never rehash
		// it during normal publication/status/recovery.
		if err := s.Contains(digest, size); err != nil {
			return err
		}
	}
	return s.syncDirectory(s.root)
}

// Contains checks trusted immutable metadata, not payload bytes. This is not an
// integrity audit or proof of durable publication: a concurrent Publish may not
// yet have synced its directory. Only successful Publish authorizes a new
// metadata reference; callers must persist that success in their authority.
// Boundary verification happens exclusively in Publish.
func (s *Store) Contains(digest string, size int64) error {
	if !validDigest(digest) || size < 0 || size > s.maxSize {
		return errors.New("invalid artifact reference")
	}
	info, err := os.Lstat(filepath.Join(s.root, digest))
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != size {
		return fmt.Errorf("invalid stored artifact %s", digest)
	}
	return nil
}

// Open returns a read-only handle. Callers must authorize access separately.
func (s *Store) Open(digest string) (*os.File, error) {
	if !validDigest(digest) {
		return nil, errors.New("invalid SHA256 digest")
	}
	info, err := os.Lstat(filepath.Join(s.root, digest))
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("artifact is not a regular file")
	}
	return os.Open(filepath.Join(s.root, digest))
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

type contextReader struct {
	ctx context.Context
	src io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.src.Read(p)
}
