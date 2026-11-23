package agent

import (
	"context"
	"errors"
	"hermes/internal/artifacts"
	"io"
)

func (r *runner) publishBlob(ctx context.Context, digest string, size int64, src io.Reader) error {
	if r.admission != nil {
		if e := r.admission.err(); e != nil {
			return e
		}
	}
	var e error
	if r.spool != nil {
		e = r.spool.Publish(ctx, digest, size, r.j.Work.Assignment.AssignmentID, src)
	} else {
		e = r.store.Publish(ctx, digest, size, src)
	}
	r.blockFailedArtifactCleanup(e)
	return e
}
func (r *runner) blockFailedArtifactCleanup(e error) {
	if !errors.Is(e, artifacts.ErrIngressCleanup) {
		return
	}
	if r.admission != nil {
		r.admission.block(e)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.j.GateClosed = true
}
