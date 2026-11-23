package agent

import (
	"context"
	"errors"
	"hermes/internal/artifacts"
	"testing"
)

type mustNotRead struct{ t *testing.T }

func (r mustNotRead) Read([]byte) (int, error) {
	r.t.Error("publication read after authority cleanup failure")
	return 0, errors.New("read forbidden")
}
func TestFailedArtifactCleanupClosesAllHostPublicationAndGrantGates(t *testing.T) {
	manager := &admission{entries: map[string]reservation{}}
	first := runner{j: journal{Work: testWork()}, admission: manager}
	other := runner{j: journal{Work: testWork()}, admission: manager}
	failure := errors.Join(artifacts.ErrIngressCleanup, errors.New("retained staging inode"))
	first.blockFailedArtifactCleanup(failure)
	if !first.j.GateClosed || !errors.Is(manager.err(), artifacts.ErrIngressCleanup) {
		t.Fatal("cleanup failure did not latch host")
	}
	if e := other.publishBlob(context.Background(), "", 0, mustNotRead{t}); !errors.Is(e, artifacts.ErrIngressCleanup) {
		t.Fatal("publication continued after failure", e)
	}
	other.mu.Lock()
	open, e := other.gateOpenLocked()
	other.mu.Unlock()
	if open || !errors.Is(e, artifacts.ErrIngressCleanup) || !other.j.GateClosed {
		t.Fatal("neighbour could grant another unit", open, e)
	}
}
