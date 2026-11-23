package agent

import (
	"context"
	"errors"
	"hermes/internal/contracts"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

func validateWork(w contracts.Work) error {
	if w.SchemaVersion != 1 {
		return errors.New("unsupported work version")
	}
	if w.Assignment == nil {
		if w.Package != nil || w.Units != nil {
			return errors.New("idle work contains dangling package or units")
		}
		return nil
	}
	if w.Package == nil {
		return errors.New("incomplete work")
	}

	a, p := w.Assignment, w.Package
	if e := a.Validate(); e != nil {
		return e
	}
	if e := p.Validate(); e != nil {
		return e
	}
	digest, e := contracts.Digest(*p)
	if e != nil {
		return e
	}
	if digest != a.PackageDigest {
		return errors.New("package digest mismatch")
	}
	if len(w.Units) != len(a.Units) {
		return errors.New("work unit count mismatch")
	}
	members := map[string]string{}
	for _, u := range p.Units {
		members[u.UnitID] = u.SpecDigest
	}
	for i, u := range w.Units {
		if e = u.Validate(); e != nil {
			return e
		}
		r := a.Units[i]
		if u.UnitID != r.UnitID || u.SpecDigest != r.SpecDigest || members[u.UnitID] != u.SpecDigest || u.Runtime != a.Runtime {
			return errors.New("work unit binding mismatch")
		}
	}
	return nil
}
func (r *runner) stage(ctx context.Context, dir string) error {
	refs := append([]contracts.Artifact{}, r.j.Work.Package.Preparation...)
	for _, u := range r.j.Work.Units {
		refs = append(refs, u.Inputs...)
	}
	if len(refs) == 0 {
		return nil
	}
	input := filepath.Join(dir, "inputs")
	if e := os.MkdirAll(input, 0700); e != nil {
		return e
	}
	seen := map[string]bool{}
	for _, a := range refs {
		if seen[a.Digest] {
			continue
		}
		seen[a.Digest] = true
		req, e := http.NewRequestWithContext(ctx, "GET", r.cfg.Client.URL+"/v1/artifacts/"+a.Digest+"?assignment="+r.j.Work.Assignment.AssignmentID, nil)
		if e != nil {
			return e
		}
		response, e := r.cfg.Client.HTTP.Do(req)
		if e != nil {
			return e
		}
		if response.StatusCode != 200 {
			response.Body.Close()
			return errors.New("input staging denied")
		}
		e = r.store.Publish(ctx, a.Digest, a.Size, response.Body)
		response.Body.Close()
		if e != nil {
			return e
		}
		src, e := r.store.Open(a.Digest)
		if e != nil {
			return e
		}
		dst, e := os.OpenFile(filepath.Join(input, a.Digest), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if e != nil {
			src.Close()
			return e
		}
		_, e = io.Copy(dst, src)
		src.Close()
		closeError := dst.Close()
		if e != nil {
			return e
		}
		if closeError != nil {
			return closeError
		}
	}
	return nil
}
