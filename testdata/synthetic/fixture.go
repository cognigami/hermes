// Package synthetic is a supplied-input fixture, not a scientific application.
package synthetic

import (
	"errors"
	"fmt"
	"hermes/internal/contracts"
	"io"
	"os"
	"path/filepath"
)

type Manifest struct {
	SchemaVersion int                  `json:"schema_version"`
	Assignment    contracts.Assignment `json:"assignment"`
	Units         []contracts.Unit     `json:"units"`
	Failure       string               `json:"failure"`
	DelayMillis   int                  `json:"delay_millis"`
}

func (m Manifest) Validate() error {
	if m.SchemaVersion != contracts.Version {
		return errors.New("unsupported fixture version")
	}
	if e := m.Assignment.Validate(); e != nil {
		return e
	}
	// This is an assignment-local manifest, not the complete sealed graph.
	// The controller checks graph edges and accepted external prerequisites.
	if len(m.Units) == 0 || len(m.Units) > contracts.MaxUnits {
		return errors.New("invalid fixture unit count")
	}
	if len(m.Units) != len(m.Assignment.Units) {
		return errors.New("assignment units mismatch")
	}
	seen := map[string]bool{}
	for i, u := range m.Units {
		if e := u.Validate(); e != nil {
			return e
		}
		if seen[u.UnitID] {
			return errors.New("duplicate fixture unit")
		}
		seen[u.UnitID] = true
		r := m.Assignment.Units[i]
		if r.UnitID != u.UnitID || r.SpecDigest != u.SpecDigest {
			return errors.New("assignment units mismatch")
		}
	}
	switch m.Failure {
	case "", "exit_prepare", "exit_unit", "reject", "validator_error":
	default:
		return errors.New("unknown failure")
	}
	if m.DelayMillis < 0 || m.DelayMillis > 60000 {
		return errors.New("invalid delay")
	}
	return nil
}
func Load(path string, dst any) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return contracts.Decode(f, dst)
}

type Result struct {
	UnitID     string `json:"unit_id"`
	SpecDigest string `json:"spec_digest"`
	Value      string `json:"value"`
}

func Expected(unit, spec string) string { return contracts.Hash([]byte(unit + ":" + spec)) }
func ReadFile(root, relative string) ([]byte, error) {
	if !contracts.ValidPath(relative) {
		return nil, errors.New("invalid path")
	}
	r, e := os.OpenRoot(root)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	f, e := r.Open(filepath.FromSlash(relative))
	if e != nil {
		return nil, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return nil, e
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	b, e := io.ReadAll(io.LimitReader(f, contracts.MaxMetadataBytes+1))
	if e != nil {
		return nil, e
	}
	if len(b) > contracts.MaxMetadataBytes {
		return nil, fmt.Errorf("fixture file too large")
	}
	return b, nil
}
