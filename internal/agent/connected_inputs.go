package agent

import (
	"context"
	"errors"
	"hermes/internal/contracts"
	"net/http"
	"net/url"
)

func workInputs(w contracts.Work) []contracts.Artifact {
	refs := append([]contracts.Artifact{}, w.Package.Preparation...)
	for _, u := range w.Units {
		refs = append(refs, u.Inputs...)
	}
	return refs
}

// Network acquisition belongs to connected orchestration. The concrete executor
// only materializes previously verified immutable local artifacts.
func (r *runner) fetchInputs(ctx context.Context) error {
	seen := map[string]bool{}
	for _, a := range workInputs(r.j.Work) {
		if seen[a.Digest] {
			continue
		}
		seen[a.Digest] = true
		if r.spool != nil {
			verified, e := r.spool.Verified(a.Digest, a.Size)
			if e != nil {
				return e
			}
			if verified {
				continue
			}
		}
		req, e := http.NewRequestWithContext(ctx, "GET", r.cfg.Client.URL+"/v1/artifacts/"+a.Digest+"?assignment="+url.QueryEscape(r.j.Work.Assignment.AssignmentID), nil)
		if e != nil {
			return e
		}
		response, e := r.cfg.Client.HTTP.Do(req)
		if e != nil {
			return e
		}
		if response.StatusCode != 200 {
			response.Body.Close()
			return errors.New("input acquisition denied")
		}
		e = r.publishBlob(ctx, a.Digest, a.Size, response.Body)
		response.Body.Close()
		if e != nil {
			return e
		}
	}
	return nil
}
