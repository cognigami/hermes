package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hermes/internal/artifacts"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

// spool is a retained-byte/cache index, not execution or acceptance authority.
// The caller holds host authority and tracks subsequent worktree writes. It
// charges logical retained regular-file bytes, not a hard filesystem quota.
type spool struct {
	mu    sync.Mutex
	root  string
	fs    *os.Root
	db    *sql.DB
	store *artifacts.Store
}
type spoolSnapshot struct {
	TotalBytes  int64
	ClosedBytes int64
	Revision    int64
	OwnedBytes  map[string]int64
}

const spoolSchema = `
CREATE TABLE IF NOT EXISTS spool_meta(id INTEGER PRIMARY KEY CHECK(id=1),total INTEGER NOT NULL CHECK(total>=0),revision INTEGER NOT NULL CHECK(revision>=0),initialized INTEGER NOT NULL CHECK(initialized IN(0,1)));
INSERT OR IGNORE INTO spool_meta VALUES(1,0,0,0);
CREATE TABLE IF NOT EXISTS spool_owners(owner TEXT PRIMARY KEY,bytes INTEGER NOT NULL CHECK(bytes>=0));
CREATE TABLE IF NOT EXISTS spool_files(key TEXT PRIMARY KEY,size INTEGER NOT NULL CHECK(size>=0),owner TEXT NOT NULL,verified INTEGER NOT NULL CHECK(verified IN(0,1)));
`

func openSpool(root string, store *artifacts.Store, activeOwners map[string]string) (*spool, error) {
	if store == nil {
		return nil, errors.New("spool requires artifact store")
	}
	absolute, e := filepath.Abs(root)
	if e != nil {
		return nil, e
	}
	info, e := os.Lstat(absolute)
	if e != nil {
		return nil, e
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("spool root must be a real directory")
	}
	fs, e := os.OpenRoot(absolute)
	if e != nil {
		return nil, e
	}
	s := &spool{root: absolute, fs: fs, store: store}
	success := false
	defer func() {
		if !success {
			s.Close()
		}
	}()
	for _, name := range []string{"spool.sqlite", "spool.sqlite-wal", "spool.sqlite-shm", "spool.sqlite-journal"} {
		info, err := fs.Lstat(name)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if err == nil && !info.Mode().IsRegular() {
			return nil, errors.New("spool index path must be a regular file")
		}
	}
	s.db, e = sql.Open("sqlite", filepath.Join(absolute, "spool.sqlite"))
	if e != nil {
		return nil, e
	}
	s.db.SetMaxOpenConns(1)
	if _, e = s.db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;"); e != nil {
		return nil, e
	}
	var version int
	if e = s.db.QueryRow("PRAGMA user_version").Scan(&version); e != nil {
		return nil, e
	}
	if version != 0 && version != 1 {
		return nil, errors.New("unsupported spool index version")
	}
	if _, e = s.db.Exec(spoolSchema); e != nil {
		return nil, e
	}
	if _, e = s.db.Exec("PRAGMA user_version=1"); e != nil {
		return nil, e
	}
	tx, e := s.db.Begin()
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	var initialized int
	if e = tx.QueryRow("SELECT initialized FROM spool_meta WHERE id=1").Scan(&initialized); e != nil {
		return nil, e
	}
	if initialized == 0 {
		if e = s.bootstrap(tx, ".", activeOwners, 0); e != nil {
			return nil, e
		}
		if _, e = tx.Exec("UPDATE spool_meta SET initialized=1 WHERE id=1"); e != nil {
			return nil, e
		}
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	success = true
	return s, nil
}
func (s *spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var e error
	if s.db != nil {
		e = s.db.Close()
		s.db = nil
	}
	if s.fs != nil {
		if err := s.fs.Close(); e == nil {
			e = err
		}
		s.fs = nil
	}
	return e
}
func spoolBaseline(path string) bool {
	switch filepath.ToSlash(path) {
	case "spool.sqlite", "spool.sqlite-wal", "spool.sqlite-shm", "spool.sqlite-journal", "identity.json", "agent.lock", "active.json":
		return true
	}
	return false
}
func spoolDigest(d string) bool {
	if len(d) != 64 {
		return false
	}
	for _, c := range d {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func spoolKey(relative string) string {
	p := strings.Split(filepath.ToSlash(relative), "/")
	if len(p) >= 3 && (p[0] == "active" || p[0] == "completed") {
		return "work/" + strings.Join(p[1:], "/")
	}
	if len(p) == 2 && p[0] == "blobs" && spoolDigest(p[1]) {
		return "blob/" + p[1]
	}
	return "file/" + filepath.ToSlash(relative)
}

// Worktrees move between active/completed ONLY by caller-serialized atomic
// rename. Two physical namespace entries cannot share one accounting key.
// An ambiguity error means these totals must not be used for admission.
func (s *spool) rejectWorkAlias(relative string) error {
	parts := strings.Split(filepath.ToSlash(relative), "/")
	if len(parts) < 3 || (parts[0] != "active" && parts[0] != "completed") {
		return nil
	}
	if parts[0] == "active" {
		parts[0] = "completed"
	} else {
		parts[0] = "active"
	}
	opposite := filepath.FromSlash(strings.Join(parts, "/"))
	_, err := s.fs.Lstat(opposite)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot exclude opposite worktree entry: %w", err)
	}
	return fmt.Errorf("ambiguous spool worktree alias: %s and %s", relative, opposite)
}

// Only directory handles are opened during bootstrap. Root confines traversal;
// O_DIRECTORY|O_NONBLOCK prevents a swapped FIFO/non-directory from blocking.
func (s *spool) bootstrap(tx *sql.Tx, dir string, active map[string]string, depth int) error {
	if depth > 64 {
		return errors.New("spool directory nesting exceeds bound")
	}
	f, e := s.fs.OpenFile(dir, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NONBLOCK, 0)
	if e != nil {
		return e
	}
	entries, e := f.ReadDir(-1)
	f.Close()
	if e != nil {
		return e
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if spoolBaseline(path) {
			continue
		}
		info, e := s.fs.Lstat(path)
		if e != nil {
			return e
		}
		if info.IsDir() {
			if e = s.bootstrap(tx, path, active, depth+1); e != nil {
				return e
			}
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		} // Never open symlinks, devices or FIFOs.
		if e = s.rejectWorkAlias(path); e != nil {
			return e
		}
		owner := ""
		parts := strings.Split(filepath.ToSlash(path), "/")
		if len(parts) >= 3 && parts[0] == "active" {
			owner = active[parts[1]]
		}
		if e = spoolRecord(tx, spoolKey(path), info.Size(), owner, false); e != nil {
			return e
		}
	}
	return nil
}
func (s *spool) relative(path string) (string, error) {
	if !filepath.IsAbs(path) {
		path = filepath.Join(s.root, path)
	}
	relative, e := filepath.Rel(s.root, path)
	if e != nil {
		return "", e
	}
	if relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("file outside spool root")
	}
	return relative, nil
}

// TrackFile uses metadata only. First charge owns the bytes; archive renaming
// keeps the same key/owner. Closed retained data is never silently discarded.
// The caller must fail admission on any tracking error, not use stale totals.
func (s *spool) TrackFile(owner, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return errors.New("spool index closed")
	}
	relative, e := s.relative(path)
	if e != nil {
		return e
	}
	if spoolBaseline(relative) {
		return nil
	}
	info, e := s.fs.Lstat(relative)
	if e != nil {
		return e
	}
	if !info.Mode().IsRegular() {
		return errors.New("tracked spool file must be regular")
	}
	if e = s.rejectWorkAlias(relative); e != nil {
		return e
	}
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = spoolRecord(tx, spoolKey(relative), info.Size(), owner, false); e != nil {
		return e
	}
	return tx.Commit()
}

// spoolRecord maintains cached totals/owner rows and revision transactionally.
// Blob intents retain their declared charge even after failed publication; a
// metadata lookup never turns an existing blob into a verified cache record.
func spoolRecord(tx *sql.Tx, key string, size int64, owner string, verified bool) error {
	if size < 0 {
		return errors.New("negative spool bytes")
	}
	var oldSize int64
	var oldOwner string
	var oldVerified int
	e := tx.QueryRow("SELECT size,owner,verified FROM spool_files WHERE key=?", key).Scan(&oldSize, &oldOwner, &oldVerified)
	exists := e == nil
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	if exists {
		if strings.HasPrefix(key, "blob/") {
			if oldVerified == 1 && oldSize != size {
				return errors.New("verified blob size mismatch")
			}
			if oldVerified == 0 && !verified && oldSize > size {
				size = oldSize
			}
		}
		owner = oldOwner
		if oldVerified == 1 {
			verified = true
		}
		if oldSize == size && oldVerified == boolInt(verified) {
			return nil
		}
	}
	delta := size - oldSize // both operands in [0,MaxInt64].
	var total, revision, owned int64
	if e = tx.QueryRow("SELECT total,revision FROM spool_meta WHERE id=1").Scan(&total, &revision); e != nil {
		return e
	}
	if delta > 0 && total > math.MaxInt64-delta || delta < 0 && total < -delta || revision == math.MaxInt64 {
		return errors.New("spool total/revision overflow")
	}
	e = tx.QueryRow("SELECT bytes FROM spool_owners WHERE owner=?", owner).Scan(&owned)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	if delta > 0 && owned > math.MaxInt64-delta || delta < 0 && owned < -delta {
		return errors.New("spool owner bytes overflow")
	}
	if _, e = tx.Exec("INSERT INTO spool_files VALUES(?,?,?,?) ON CONFLICT(key) DO UPDATE SET size=excluded.size,verified=excluded.verified", key, size, owner, boolInt(verified)); e != nil {
		return e
	}
	if _, e = tx.Exec("INSERT INTO spool_owners VALUES(?,?) ON CONFLICT(owner) DO UPDATE SET bytes=excluded.bytes", owner, owned+delta); e != nil {
		return e
	}
	_, e = tx.Exec("UPDATE spool_meta SET total=?,revision=? WHERE id=1", total+delta, revision+1)
	return e
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// Publish always verifies incoming bytes, including redelivery to a verified
// digest. Only callers using Verified may reuse the protected local cache.
func (s *spool) Publish(ctx context.Context, digest string, size int64, owner string, src io.Reader) error {
	if !spoolDigest(digest) || size < 0 {
		return errors.New("invalid spool artifact reference")
	}
	// Reserve the entire declared blob charge before streaming, without holding
	// an admission/SQL/index lock across hashing or durable artifact publication.
	if e := s.record(ctx, "blob/"+digest, size, owner, false); e != nil {
		return e
	}
	if e := s.store.Publish(ctx, digest, size, src); e != nil {
		return e
	}
	return s.record(ctx, "blob/"+digest, size, owner, true)
}
func (s *spool) record(ctx context.Context, key string, size int64, owner string, verified bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return errors.New("spool index closed")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = spoolRecord(tx, key, size, owner, verified); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *spool) Verified(digest string, size int64) (bool, error) {
	if !spoolDigest(digest) || size < 0 {
		return false, errors.New("invalid spool artifact reference")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return false, errors.New("spool index closed")
	}
	var stored int64
	var verified int
	e := s.db.QueryRow("SELECT size,verified FROM spool_files WHERE key=?", "blob/"+digest).Scan(&stored, &verified)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if verified == 0 {
		return false, nil
	}
	if stored != size {
		return false, errors.New("verified blob size mismatch")
	}
	return true, nil
}
func (s *spool) Snapshot(activeOwnerIDs []string) (spoolSnapshot, error) {
	out := spoolSnapshot{OwnedBytes: map[string]int64{}}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return out, errors.New("spool index closed")
	}
	if e := s.db.QueryRow("SELECT total,revision FROM spool_meta WHERE id=1").Scan(&out.TotalBytes, &out.Revision); e != nil {
		return out, e
	}
	out.ClosedBytes = out.TotalBytes
	for _, owner := range activeOwnerIDs {
		if owner == "" {
			continue
		}
		if _, ok := out.OwnedBytes[owner]; ok {
			continue
		}
		var bytes int64
		e := s.db.QueryRow("SELECT bytes FROM spool_owners WHERE owner=?", owner).Scan(&bytes)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return out, e
		}
		if bytes < 0 || bytes > out.ClosedBytes {
			return out, fmt.Errorf("inconsistent spool owner total for %q", owner)
		}
		out.OwnedBytes[owner] = bytes
		out.ClosedBytes -= bytes
	}
	return out, nil
}
