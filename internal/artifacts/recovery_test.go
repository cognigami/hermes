package artifacts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
)

func TestReopenCleansOnlyAbandonedIngress(t *testing.T) {
	s := newStore(t)
	data := "acknowledged result bytes"
	digest := digestOf(data)
	if err := s.Publish(context.Background(), digest, int64(len(data)), strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(s.root, digest))
	if err != nil {
		t.Fatal(err)
	}
	// Cross directory enumeration batches; canonical names must never be opened,
	// rehashed, rewritten or removed by scratch cleanup.
	for i := 0; i < 300; i++ {
		name := digestOf(fmt.Sprintf("historical-%d", i))
		if err := os.WriteFile(filepath.Join(s.root, name), []byte("retained"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 257; i++ {
		if err := os.WriteFile(filepath.Join(s.root, fmt.Sprintf(".ingress-%d", i)), []byte("unfinished upload"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := New(s.root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 301 {
		t.Fatalf("retained entries=%d, want 301 canonical blobs", len(entries))
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".ingress-") {
			t.Fatal("abandoned scratch survived", entry.Name())
		}
	}
	after, err := os.Stat(filepath.Join(s.root, digest))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("canonical inode replaced")
	}
	f, err := reopened.Open(digest)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, len(data))
	if _, err := f.Read(b); err != nil {
		t.Fatal(err)
	}
	if string(b) != data {
		t.Fatal("canonical bytes changed")
	}
}

func TestPublicationCleanupFailureIsNotAcknowledged(t *testing.T) {
	for _, kind := range []string{"fresh", "duplicate", "source failure"} {
		t.Run(kind, func(t *testing.T) {
			s := newStore(t)
			digest := digestOf("abc")
			if kind == "duplicate" {
				if err := s.Publish(context.Background(), digest, 3, strings.NewReader("abc")); err != nil {
					t.Fatal(err)
				}
			}
			unlinkErr := errors.New("injected unlink failure")
			sourceErr := errors.New("injected source failure")
			var scratch string
			s.removeIngress = func(path string) error { scratch = path; return unlinkErr }
			var err error
			if kind == "source failure" {
				err = s.Publish(context.Background(), digest, 3, iotest.ErrReader(sourceErr))
			} else {
				err = s.Publish(context.Background(), digest, 3, strings.NewReader("abc"))
			}
			if !errors.Is(err, ErrIngressCleanup) || !errors.Is(err, unlinkErr) {
				t.Fatalf("cleanup failure hidden: %v", err)
			}
			if kind == "source failure" && !errors.Is(err, sourceErr) {
				t.Fatalf("source failure hidden: %v", err)
			}
			tempInfo, err := os.Stat(scratch)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "duplicate" {
				canonical, err := os.Stat(filepath.Join(s.root, digest))
				if err != nil {
					t.Fatal(err)
				}
				if os.SameFile(tempInfo, canonical) {
					t.Fatal("duplicate fixture must retain a separate, fully copied scratch inode")
				}
			}
			// No publishers remain; exclusive reopen is the recovery boundary, not a
			// claim that the failed call made a catalog reference safe to acknowledge.
			reopened, err := New(s.root, 1024)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(scratch); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("scratch survived reopen: %v", err)
			}
			if err = reopened.Publish(context.Background(), digest, 3, strings.NewReader("abc")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReopenRejectsNonregularIngress(t *testing.T) {
	for _, kind := range []string{"directory", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			s := newStore(t)
			target := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(target, []byte("must survive"), 0600); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(s.root, ".ingress-unexpected")
			if kind == "directory" {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := New(s.root, 1024); err == nil {
				t.Fatal("nonregular reserved entry accepted")
			}
			b, err := os.ReadFile(target)
			if err != nil || string(b) != "must survive" {
				t.Fatalf("outside bytes changed: %q %v", b, err)
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatal("unexpected entry was removed", err)
			}
		})
	}
}
