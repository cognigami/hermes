package agent

import (
	"bytes"
	"context"
	"errors"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func spoolFixture(t *testing.T) (string, *artifacts.Store, *spool) {
	t.Helper()
	root := t.TempDir()
	store, e := artifacts.New(filepath.Join(root, "blobs"), 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	s, e := openSpool(root, store, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return root, store, s
}
func spoolFile(t *testing.T, root, path, content string) string {
	t.Helper()
	p := filepath.Join(root, path)
	if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(p, []byte(content), 0600); e != nil {
		t.Fatal(e)
	}
	return p
}
func spoolStatus(t *testing.T, s *spool, active ...string) spoolSnapshot {
	t.Helper()
	out, e := s.Snapshot(active)
	if e != nil {
		t.Fatal(e)
	}
	return out
}

func TestSpoolBootstrapMetadataAndArchiveKeys(t *testing.T) {
	root := t.TempDir()
	hash := contracts.Hash([]byte("assignment"))
	store, e := artifacts.New(filepath.Join(root, "blobs"), 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"identity.json", "agent.lock", "active.json"} {
		spoolFile(t, root, name, "baseline")
	}
	for _, name := range []string{"journal.json", "identity.json", "active.json"} {
		spoolFile(t, root, filepath.Join("active", hash, name), "not-json")
	}
	spoolFile(t, root, "unowned.txt", "closed")
	outside := spoolFile(t, t.TempDir(), "external", "not owned bytes")
	if e = os.Symlink(outside, filepath.Join(root, "external-link")); e != nil {
		t.Fatal(e)
	}
	if e = unix.Mkfifo(filepath.Join(root, "pipe"), 0600); e != nil {
		t.Fatal(e)
	}
	s, e := openSpool(root, store, map[string]string{hash: "assignment"})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	before := spoolStatus(t, s, "assignment")
	if before.TotalBytes != 30 || before.ClosedBytes != 6 || before.OwnedBytes["assignment"] != 24 {
		t.Fatal(before)
	}
	if e = os.Mkdir(filepath.Join(root, "completed"), 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(filepath.Join(root, "active", hash), filepath.Join(root, "completed", hash)); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"journal.json", "identity.json", "active.json"} {
		if e = s.TrackFile("different-owner", filepath.Join(root, "completed", hash, name)); e != nil {
			t.Fatal(e)
		}
	}
	after := spoolStatus(t, s, "assignment", "assignment")
	if before.TotalBytes != after.TotalBytes || before.Revision != after.Revision || after.OwnedBytes["assignment"] != 24 {
		t.Fatal(before, after)
	}
	closed := spoolStatus(t, s, "different-owner")
	if closed.ClosedBytes != closed.TotalBytes || closed.OwnedBytes["different-owner"] != 0 {
		t.Fatal(closed)
	}
	if e = s.TrackFile("assignment", filepath.Join(root, "pipe")); e == nil {
		t.Fatal("FIFO accepted")
	}
	externalDir := t.TempDir()
	spoolFile(t, externalDir, "file", "outside")
	if e = os.Symlink(externalDir, filepath.Join(root, "ancestor")); e != nil {
		t.Fatal(e)
	}
	if e = s.TrackFile("assignment", filepath.Join(root, "ancestor", "file")); e == nil {
		t.Fatal("external ancestor followed")
	}
	if e = s.TrackFile("assignment", outside); e == nil {
		t.Fatal("outside root accepted")
	}
}
func TestSpoolRetainsPublishedBytesAndOnlyExplicitVerification(t *testing.T) {
	root, store, s := spoolFixture(t)
	ctx := context.Background()
	data := []byte("retained bytes")
	digest := contracts.Hash(data)
	if ok, e := s.Verified(digest, int64(len(data))); e != nil || ok {
		t.Fatal(ok, e)
	}
	if e := store.Publish(ctx, digest, int64(len(data)), bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	if ok, e := s.Verified(digest, int64(len(data))); e != nil || ok {
		t.Fatal("path visibility trusted", ok, e)
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e := openSpool(root, store, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	// Existing initialized index is not rescanned just because unknown bytes appeared.
	if ok, e := s.Verified(digest, int64(len(data))); e != nil || ok {
		t.Fatal(ok, e)
	}
	if e = s.Publish(ctx, digest, int64(len(data)), "a", bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	if e = s.Publish(ctx, digest, int64(len(data)), "b", bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	if ok, e := s.Verified(digest, int64(len(data))); e != nil || !ok {
		t.Fatal(ok, e)
	}
	out := spoolStatus(t, s, "a", "b")
	if out.TotalBytes != int64(len(data)) || out.ClosedBytes != 0 || out.OwnedBytes["a"] != int64(len(data)) || out.OwnedBytes["b"] != 0 {
		t.Fatal(out)
	}
	if out = spoolStatus(t, s, "b"); out.ClosedBytes != int64(len(data)) {
		t.Fatal("first-owner charge silently reassigned", out)
	}
	if e = s.Publish(ctx, digest, int64(len(data)), "b", bytes.NewReader(bytes.Repeat([]byte("x"), len(data)))); e == nil {
		t.Fatal("untrusted redelivery bypassed verification")
	}
	if ok, e := s.Verified(digest, int64(len(data))); e != nil || !ok {
		t.Fatal("failure downgraded prior verification", ok, e)
	}
	// A steady restart must not even traverse retained directory history. A
	// traversal of this metadata-only sentinel would hit the bootstrap bound.
	deep := root
	for i := 0; i < 70; i++ {
		deep = filepath.Join(deep, "d")
		if e = os.Mkdir(deep, 0700); e != nil {
			t.Fatal(e)
		}
	}
	before := spoolStatus(t, s)
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	// Restart and status cannot require traversal or payload/journal reads.
	hidden := filepath.Join(t.TempDir(), "retained")
	if e = os.Rename(filepath.Join(root, "blobs"), hidden); e != nil {
		t.Fatal(e)
	}
	s, e = openSpool(root, store, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	out = spoolStatus(t, s)
	if out.TotalBytes != before.TotalBytes || out.Revision != before.Revision {
		t.Fatal(before, out)
	}
	if ok, e := s.Verified(digest, int64(len(data))); e != nil || !ok {
		t.Fatal("metadata lookup touched payload", ok, e)
	}
	if e = os.Rename(hidden, filepath.Join(root, "blobs")); e != nil {
		t.Fatal(e)
	}
}
func TestSpoolBootstrapBlobIsNotVerified(t *testing.T) {
	root := t.TempDir()
	store, e := artifacts.New(filepath.Join(root, "blobs"), 100)
	if e != nil {
		t.Fatal(e)
	}
	data := []byte("old")
	digest := contracts.Hash(data)
	if e = store.Publish(context.Background(), digest, 3, bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	s, e := openSpool(root, store, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if ok, e := s.Verified(digest, 3); e != nil || ok {
		t.Fatal("bootstrap inferred verified", ok, e)
	}
	if got := spoolStatus(t, s); got.TotalBytes != 3 || got.ClosedBytes != 3 {
		t.Fatal(got)
	}
	if e = s.Publish(context.Background(), digest, 3, "a", bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	if got := spoolStatus(t, s, "a"); got.TotalBytes != 3 || got.ClosedBytes != 3 {
		t.Fatal(got)
	}
}

type spoolBlockedReader struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
	reader  io.Reader
}

func (r *spoolBlockedReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started); <-r.release })
	return r.reader.Read(p)
}
func TestSpoolIntentPrecedesStreamingWithoutHoldingIndexMutex(t *testing.T) {
	root, _, s := spoolFixture(t)
	data := []byte("stream")
	reader := &spoolBlockedReader{started: make(chan struct{}), release: make(chan struct{}), reader: bytes.NewReader(data)}
	done := make(chan error, 1)
	go func() { done <- s.Publish(context.Background(), contracts.Hash(data), int64(len(data)), "a", reader) }()
	defer func() {
		select {
		case <-reader.release:
		default:
			close(reader.release)
		}
	}()
	select {
	case <-reader.started:
	case <-time.After(time.Second):
		t.Fatal("stream never began")
	}
	progress := make(chan error, 1)
	path := spoolFile(t, root, "note", "x")
	go func() {
		out, e := s.Snapshot([]string{"a"})
		if e == nil && out.TotalBytes != int64(len(data)) {
			e = errors.New("missing durable intent charge")
		}
		if e == nil {
			e = s.TrackFile("a", path)
		}
		progress <- e
	}()
	select {
	case e := <-progress:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("index lock held across streaming")
	}
	close(reader.release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
}
func TestSpoolConcurrentDigestDedupAndFailureCannotDowngrade(t *testing.T) {
	_, _, s := spoolFixture(t)
	data := []byte("same blob")
	digest := contracts.Hash(data)
	var wg sync.WaitGroup
	failures := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := data
			owner := "a"
			if i%2 != 0 {
				body = bytes.Repeat([]byte("!"), len(data))
				owner = "b"
			}
			e := s.Publish(context.Background(), digest, int64(len(data)), owner, bytes.NewReader(body))
			if i%2 == 0 && e != nil {
				failures <- e
			}
			if i%2 != 0 && e == nil {
				failures <- errors.New("bad bytes accepted")
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		t.Error(e)
	}
	out := spoolStatus(t, s, "a", "b")
	if out.TotalBytes != int64(len(data)) || out.OwnedBytes["a"]+out.OwnedBytes["b"] != out.TotalBytes || out.ClosedBytes != 0 {
		t.Fatal(out)
	}
	if ok, e := s.Verified(digest, int64(len(data))); e != nil || !ok {
		t.Fatal(ok, e)
	}
	var rows int
	if e := s.db.QueryRow("SELECT count(*) FROM spool_files WHERE key=?", "blob/"+digest).Scan(&rows); e != nil || rows != 1 {
		t.Fatal(rows, e)
	}
}
func TestSpoolPublicationFailureAndFinalTransactionFailure(t *testing.T) {
	root, _, s := spoolFixture(t)
	data := []byte("candidate")
	digest := contracts.Hash(data)
	ctx := context.Background()
	if _, e := s.db.Exec(`CREATE TRIGGER fail_verification BEFORE UPDATE ON spool_files WHEN NEW.verified=1 BEGIN SELECT RAISE(ABORT,'injected final transaction failure'); END;`); e != nil {
		t.Fatal(e)
	}
	if e := s.Publish(ctx, digest, int64(len(data)), "a", bytes.NewReader(data)); e == nil {
		t.Fatal("final metadata failure hidden")
	}
	if ok, e := s.Verified(digest, int64(len(data))); e != nil || ok {
		t.Fatal("bytes alone became verified", ok, e)
	}
	if _, e := os.Stat(filepath.Join(root, "blobs", digest)); e != nil {
		t.Fatal(e)
	}
	if got := spoolStatus(t, s, "a"); got.TotalBytes != int64(len(data)) {
		t.Fatal(got)
	}
	if _, e := s.db.Exec("DROP TRIGGER fail_verification"); e != nil {
		t.Fatal(e)
	}
	if e := s.Publish(ctx, digest, int64(len(data)), "a", bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	absent := contracts.Hash([]byte("absent"))
	if e := s.Publish(ctx, absent, 6, "a", bytes.NewReader([]byte("bad"))); e == nil {
		t.Fatal("short bytes accepted")
	}
	if ok, e := s.Verified(absent, 6); e != nil || ok {
		t.Fatal(ok, e)
	}
	if got := spoolStatus(t, s); got.TotalBytes != int64(len(data))+6 {
		t.Fatal("failed intent not charged", got)
	}
}
func TestSpoolOverflowRollbackAndMutableFileSizes(t *testing.T) {
	root, _, s := spoolFixture(t)
	path := spoolFile(t, root, "journal", "abc")
	if e := s.TrackFile("a", path); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := s.TrackFile("a", path); e != nil {
		t.Fatal(e)
	}
	if got := spoolStatus(t, s, "a"); got.TotalBytes != 1 || got.OwnedBytes["a"] != 1 {
		t.Fatal(got)
	}
	// Force a real index bound, then verify the entire accounting transaction rolls back.
	if _, e := s.db.Exec("UPDATE spool_meta SET revision=?", int64(math.MaxInt64)); e != nil {
		t.Fatal(e)
	}
	before := spoolStatus(t, s, "a")
	newPath := spoolFile(t, root, "extra", "new")
	if e := s.TrackFile("a", newPath); e == nil {
		t.Fatal("revision overflow accepted")
	}
	after := spoolStatus(t, s, "a")
	if after.TotalBytes != before.TotalBytes || after.Revision != before.Revision {
		t.Fatal(before, after)
	}
	if _, e := s.db.Exec("UPDATE spool_meta SET revision=0"); e != nil {
		t.Fatal(e)
	}
	huge := contracts.Hash([]byte("huge"))
	if e := s.Publish(context.Background(), huge, math.MaxInt64, "a", bytes.NewReader(nil)); e == nil {
		t.Fatal("total overflow accepted")
	}
	if got := spoolStatus(t, s, "a"); got.TotalBytes != 1 {
		t.Fatal(got)
	}
}
func TestSpoolRejectsDualPhysicalWorktreeKeys(t *testing.T) {
	t.Run("bootstrap", func(t *testing.T) {
		root := t.TempDir()
		store, e := artifacts.New(filepath.Join(root, "blobs"), 100)
		if e != nil {
			t.Fatal(e)
		}
		hash := contracts.Hash([]byte("assignment"))
		spoolFile(t, root, filepath.Join("active", hash, "result"), "abc")
		duplicate := spoolFile(t, root, filepath.Join("completed", hash, "result"), "12345")
		if s, e := openSpool(root, store, map[string]string{hash: "assignment"}); e == nil {
			s.Close()
			t.Fatal("distinct 3+5 byte files accepted as one stable key")
		} else if !strings.Contains(e.Error(), "ambiguous spool worktree alias") {
			t.Fatal(e)
		}
		// An ambiguous bootstrap must not commit initialized or any partial rows.
		if e = os.Remove(duplicate); e != nil {
			t.Fatal(e)
		}
		s, e := openSpool(root, store, map[string]string{hash: "assignment"})
		if e != nil {
			t.Fatal(e)
		}
		defer s.Close()
		if got := spoolStatus(t, s, "assignment"); got.TotalBytes != 3 || got.OwnedBytes["assignment"] != 3 {
			t.Fatal(got)
		}
	})
	t.Run("track-boundary", func(t *testing.T) {
		root, _, s := spoolFixture(t)
		hash := contracts.Hash([]byte("assignment"))
		active := spoolFile(t, root, filepath.Join("active", hash, "result"), "abc")
		if e := s.TrackFile("assignment", active); e != nil {
			t.Fatal(e)
		}
		before := spoolStatus(t, s, "assignment")
		completed := spoolFile(t, root, filepath.Join("completed", hash, "result"), "12345")
		for _, p := range []string{active, completed} {
			if e := s.TrackFile("assignment", p); e == nil || !strings.Contains(e.Error(), "ambiguous spool worktree alias") {
				t.Fatal("tracking ambiguity licensed an accounting update", p, e)
			}
		}
		// Last committed bytes remain unchanged, NOT a usable current admission
		// observation after TrackFile failed. The caller must stop admission.
		after := spoolStatus(t, s, "assignment")
		if after.TotalBytes != before.TotalBytes || after.Revision != before.Revision {
			t.Fatal(before, after)
		}
	})
}

func TestSpoolRejectsSymlinkIndex(t *testing.T) {
	root := t.TempDir()
	store, e := artifacts.New(filepath.Join(root, "blobs"), 100)
	if e != nil {
		t.Fatal(e)
	}
	outside := spoolFile(t, t.TempDir(), "foreign", "foreign")
	if e = os.Symlink(outside, filepath.Join(root, "spool.sqlite")); e != nil {
		t.Fatal(e)
	}
	if s, e := openSpool(root, store, nil); e == nil {
		s.Close()
		t.Fatal("external SQLite index followed")
	}
	b, e := os.ReadFile(outside)
	if e != nil || string(b) != "foreign" {
		t.Fatal(string(b), e)
	}
}

func TestSpoolUnverifiedSizeHeaderCanRecoverAfterRestart(t *testing.T) {
	root, store, index := spoolFixture(t)
	ctx := context.Background()
	data := []byte("correct bytes")
	digest := contracts.Hash(data)
	if e := index.Publish(ctx, digest, 100, "first", bytes.NewReader([]byte("short"))); e == nil {
		t.Fatal("bad size verified")
	}
	if ok, e := index.Verified(digest, int64(len(data))); e != nil || ok {
		t.Fatal("unverified intent poisoned cache acquisition", ok, e)
	}
	if e := index.Close(); e != nil {
		t.Fatal(e)
	}
	reopened, e := openSpool(root, store, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	if e = reopened.Publish(ctx, digest, int64(len(data)), "other", bytes.NewReader(data)); e != nil {
		t.Fatal("corrected declaration failed", e)
	}
	snapshot := spoolStatus(t, reopened, "first")
	if snapshot.TotalBytes != int64(len(data)) || snapshot.OwnedBytes["first"] != int64(len(data)) {
		t.Fatal("actual verified length/first owner not bound", snapshot)
	}
	if ok, e := reopened.Verified(digest, 100); e == nil || ok {
		t.Fatal("verified length became mutable", ok, e)
	}
}
func TestSpoolPendingSizesMaxAndConcurrentVerificationNeverDowngrades(t *testing.T) {
	_, _, index := spoolFixture(t)
	ctx := context.Background()
	data := []byte("data")
	digest := contracts.Hash(data)
	key := "blob/" + digest
	if e := index.record(ctx, key, 0, "first", false); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for _, size := range []int64{1, 100, 3, 20, 2} {
		wg.Add(1)
		go func(size int64) {
			defer wg.Done()
			if e := index.record(ctx, key, size, "later", false); e != nil {
				t.Error(e)
			}
		}(size)
	}
	wg.Wait()
	if snapshot := spoolStatus(t, index, "first"); snapshot.TotalBytes != 100 || snapshot.OwnedBytes["first"] != 100 {
		t.Fatal("pending bytes shrank or changed owner", snapshot)
	}
	if e := index.Publish(ctx, digest, int64(len(data)), "later", bytes.NewReader(data)); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := index.record(ctx, key, int64(len(data)), "later", false); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if ok, e := index.Verified(digest, int64(len(data))); e != nil || !ok {
		t.Fatal("pending redelivery downgraded verification", ok, e)
	}
	if snapshot := spoolStatus(t, index, "first"); snapshot.TotalBytes != 4 || snapshot.OwnedBytes["first"] != 4 {
		t.Fatal(snapshot)
	}
}
