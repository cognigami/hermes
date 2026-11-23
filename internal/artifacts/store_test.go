package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func digestOf(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(filepath.Join(t.TempDir(), "blobs"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPublicationAndConcurrentRedelivery(t *testing.T) {
	s := newStore(t)
	data := "durable synthetic result"
	digest := digestOf(data)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Publish(context.Background(), digest, int64(len(data)), strings.NewReader(data)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := s.Contains(digest, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	f, err := s.Open(digest)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil || string(got) != data {
		t.Fatalf("read=%q, err=%v", got, err)
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("unexpected ingress debris: %v", entries)
	}
}

func TestFailedIngressCannotBecomeReference(t *testing.T) {
	for _, tc := range []struct {
		name, data, digest string
		size               int64
	}{
		{"short", "abc", digestOf("abc"), 4},
		{"long", "abcd", digestOf("abc"), 3},
		{"wrong hash", "abc", digestOf("xyz"), 3},
		{"traversal", "abc", "../outside", 3},
		{"negative", "abc", digestOf("abc"), -1},
		{"oversized", "abc", digestOf("abc"), 1025},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newStore(t)
			if err := s.Publish(context.Background(), tc.digest, tc.size, strings.NewReader(tc.data)); err == nil {
				t.Fatal("accepted invalid ingress")
			}
			entries, err := os.ReadDir(s.root)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("published failed input: %v", entries)
			}
		})
	}
}

func TestCancellationAndReaderFailure(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Publish(ctx, digestOf("abc"), 3, strings.NewReader("abc")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if err := s.Publish(context.Background(), digestOf("abc"), 3, failingReader{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("reader failure: %v", err)
	}
	entries, err := os.ReadDir(s.root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed ingress debris=%v, err=%v", entries, err)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestReferenceAndRootValidation(t *testing.T) {
	s := newStore(t)
	digest := digestOf("abc")
	if err := s.Contains(digest, 3); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing blob: %v", err)
	}
	if err := s.Publish(context.Background(), digest, 3, strings.NewReader("abc")); err != nil {
		t.Fatal(err)
	}
	if err := s.Contains(digest, 4); err == nil {
		t.Fatal("accepted length mismatch")
	}
	if _, err := s.Open("../outside"); err == nil {
		t.Fatal("accepted traversal")
	}
	if _, err := New(s.root, 0); err == nil {
		t.Fatal("accepted zero limit")
	}
	if _, err := New(filepath.Join(s.root, digest), 1024); err == nil {
		t.Fatal("accepted file as store root")
	}
}

func TestRootTrailingSeparator(t *testing.T) {
	root := filepath.Join(t.TempDir(), "blobs")
	s, err := New(root+string(os.PathSeparator), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if s.root != root || filepath.Dir(s.root) != filepath.Dir(root) {
		t.Fatalf("root not normalized: %q", s.root)
	}
	if err := s.Publish(context.Background(), digestOf("abc"), 3, strings.NewReader("abc")); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(filepath.Dir(root), "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if _, err := New(link+string(os.PathSeparator), 1024); err == nil {
		t.Fatal("accepted symlink root with trailing separator")
	}
}

func TestSyncFailuresAreNotAcknowledged(t *testing.T) {
	for _, stage := range []string{"file", "directory"} {
		t.Run(stage, func(t *testing.T) {
			s := newStore(t)
			injected := errors.New("injected sync failure")
			if stage == "file" {
				s.syncFile = func(*os.File) error { return injected }
			} else {
				s.syncDirectory = func(string) error { return injected }
			}
			if err := s.Publish(context.Background(), digestOf("abc"), 3, strings.NewReader("abc")); !errors.Is(err, injected) {
				t.Fatalf("sync failure acknowledged: %v", err)
			}
			if stage == "file" {
				if err := s.Contains(digestOf("abc"), 3); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("linked before file sync: %v", err)
				}
			} else {
				// Visibility is deliberately weaker than a successful Publish.
				if err := s.Contains(digestOf("abc"), 3); err != nil {
					t.Fatal(err)
				}
			}
			s.syncFile = (*os.File).Sync
			s.syncDirectory = syncDir
			if err := s.Publish(context.Background(), digestOf("abc"), 3, strings.NewReader("abc")); err != nil {
				t.Fatalf("retry failed: %v", err)
			}
		})
	}
}

func TestProcessExitAtPublicationBoundaries(t *testing.T) {
	for _, stage := range []string{"file_synced", "linked", "directory_synced"} {
		t.Run(stage, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "blobs")
			cmd := exec.Command(os.Args[0], "-test.run=^TestPublicationCrashHelper$")
			cmd.Env = append(os.Environ(), "HERMES_ARTIFACT_CRASH_ROOT="+root, "HERMES_ARTIFACT_CRASH_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 73 {
				t.Fatalf("helper: %v %s", err, out)
			}
			s, err := New(root, 1024)
			if err != nil {
				t.Fatal(err)
			}
			if stage == "file_synced" {
				if err := s.Contains(digestOf("abc"), 3); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("premature destination: %v", err)
				}
			} else {
				f, err := s.Open(digestOf("abc"))
				if err != nil {
					t.Fatal(err)
				}
				b, err := io.ReadAll(f)
				f.Close()
				if err != nil || string(b) != "abc" {
					t.Fatalf("incomplete linked bytes: %q %v", b, err)
				}
			}
			// A caller that lost its response retries the same bytes; orphan ingress
			// files may remain but no partial destination is accepted or replaced.
			if err := s.Publish(context.Background(), digestOf("abc"), 3, strings.NewReader("abc")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// This child exits without deferred cleanup. It models process loss, not
// filesystem/power loss; the latter requires target-host evidence.
func TestPublicationCrashHelper(t *testing.T) {
	root := os.Getenv("HERMES_ARTIFACT_CRASH_ROOT")
	if root == "" {
		return
	}
	stage := os.Getenv("HERMES_ARTIFACT_CRASH_STAGE")
	s, err := New(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	s.syncFile = func(f *os.File) error {
		if err := f.Sync(); err != nil {
			return err
		}
		if stage == "file_synced" {
			os.Exit(73)
		}
		return nil
	}
	s.syncDirectory = func(path string) error {
		if stage == "linked" {
			os.Exit(73)
		}
		if err := syncDir(path); err != nil {
			return err
		}
		if stage == "directory_synced" {
			os.Exit(73)
		}
		return nil
	}
	if err := s.Publish(context.Background(), digestOf("abc"), 3, strings.NewReader("abc")); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash boundary not reached")
}

func TestExistingDestinationNeverReplaced(t *testing.T) {
	s := newStore(t)
	digest := digestOf("abc")
	dest := filepath.Join(s.root, digest)
	// Model a damaged destination: fail closed instead of overwriting it.
	if err := os.WriteFile(dest, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Publish(context.Background(), digest, 3, strings.NewReader("abc")); err == nil {
		t.Fatal("accepted damaged existing destination")
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != "x" {
		t.Fatalf("destination replaced: %q %v", got, err)
	}
}
