// Package contracts defines bounded metadata, never artifact payloads.
package contracts

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"reflect"
	"strings"
	"time"
)

const Version = 1
const MaxMetadataBytes = 1 << 20
const MaxMessageBytes = 64 << 10
const MaxUnits = 1024

// Decode rejects unknown fields, trailing values and oversized input.
func Decode(r io.Reader, dst any) error {
	b, err := io.ReadAll(io.LimitReader(r, MaxMetadataBytes+1))
	if err != nil {
		return err
	}
	if len(b) > MaxMetadataBytes {
		return errors.New("metadata too large")
	}
	return decode(b, dst)
}
func decode(b []byte, dst any) error {
	// encoding/json otherwise silently accepts duplicate object keys.
	keys := json.NewDecoder(bytes.NewReader(b))
	keys.UseNumber()
	if err := uniqueKeys(keys, 0); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func uniqueKeys(d *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("JSON nesting too deep")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			s, ok := key.(string)
			// encoding/json matches struct fields case-insensitively. Fold
			// aliases too so a later spelling cannot overwrite a prior field.
			s = strings.ToLower(strings.ToUpper(s))
			if !ok || seen[s] {
				return errors.New("duplicate or invalid JSON key")
			}
			seen[s] = true
			if err := uniqueKeys(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := uniqueKeys(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = d.Token()
	return err
}

func Digest(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return Hash(b), nil
}
func Hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func validDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == sha256.Size && s == strings.ToLower(s)
}
func id(s string) bool { return s != "" && len(s) <= 256 && !strings.ContainsAny(s, "\x00\r\n") }
func version(v int) error {
	if v != Version {
		return fmt.Errorf("unsupported schema_version %d", v)
	}
	return nil
}

type Artifact struct {
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	MediaType string `json:"media_type"`
}

func (a Artifact) Validate() error {
	if !validDigest(a.Digest) || a.Size < 0 || !id(a.MediaType) {
		return errors.New("invalid artifact")
	}
	return nil
}

type Unit struct {
	SchemaVersion        int        `json:"schema_version"`
	UnitID               string     `json:"unit_id"`
	SpecDigest           string     `json:"spec_digest"`
	Inputs               []Artifact `json:"inputs"`
	Operation            string     `json:"operation"`
	Runtime              string     `json:"runtime"`
	Validator            string     `json:"validator"`
	Dependencies         []string   `json:"dependencies"`
	RequiredCapabilities []string   `json:"required_capabilities"`
}

func (u Unit) Validate() error {
	if err := version(u.SchemaVersion); err != nil {
		return err
	}
	if !id(u.UnitID) || !validDigest(u.SpecDigest) || !id(u.Operation) || !id(u.Runtime) || !id(u.Validator) {
		return errors.New("invalid unit identity")
	}
	for _, a := range u.Inputs {
		if err := a.Validate(); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, s := range u.Dependencies {
		if !id(s) || s == u.UnitID || seen[s] {
			return errors.New("invalid dependency")
		}
		seen[s] = true
	}
	for _, s := range u.RequiredCapabilities {
		if !id(s) {
			return errors.New("invalid capability")
		}
	}
	return nil
}

// CompatibleUnit compares the complete immutable definition, not only the supplied spec digest.
func CompatibleUnit(existing, incoming Unit) error {
	if err := existing.Validate(); err != nil {
		return err
	}
	if err := incoming.Validate(); err != nil {
		return err
	}
	if !reflect.DeepEqual(existing, incoming) {
		return errors.New("incompatible unit re-registration")
	}
	return nil
}

type UnitRef struct {
	UnitID     string `json:"unit_id"`
	SpecDigest string `json:"spec_digest"`
}

func refs(rs []UnitRef) error {
	if len(rs) == 0 || len(rs) > MaxUnits {
		return errors.New("invalid unit count")
	}
	seen := map[string]bool{}
	for _, r := range rs {
		if !id(r.UnitID) || !validDigest(r.SpecDigest) || seen[r.UnitID] {
			return errors.New("invalid unit reference")
		}
		seen[r.UnitID] = true
	}
	return nil
}

type Package struct {
	SchemaVersion int        `json:"schema_version"`
	PackageID     string     `json:"package_id"`
	Units         []UnitRef  `json:"units"`
	Preparation   []Artifact `json:"preparation"`
}

func (p Package) Validate() error {
	if e := version(p.SchemaVersion); e != nil {
		return e
	}
	if !id(p.PackageID) {
		return errors.New("invalid package id")
	}
	if e := refs(p.Units); e != nil {
		return e
	}
	for _, a := range p.Preparation {
		if e := a.Validate(); e != nil {
			return e
		}
	}
	return nil
}

type Assignment struct {
	SchemaVersion   int        `json:"schema_version"`
	AssignmentID    string     `json:"assignment_id"`
	RunID           string     `json:"run_id"`
	WorkerID        string     `json:"worker_id"`
	SessionID       string     `json:"session_id"`
	PackageDigest   string     `json:"package_digest"`
	Units           []UnitRef  `json:"units"`
	Runtime         string     `json:"runtime"`
	Profile         string     `json:"profile"`
	Allocation      Allocation `json:"allocation"`
	PolicyRevision  int64      `json:"policy_revision"`
	StopNewUnitsAt  time.Time  `json:"stop_new_units_at"`
	TerminateAt     time.Time  `json:"terminate_at"`
	HandoffDeadline time.Time  `json:"handoff_deadline"`
}

func (a Assignment) Validate() error {
	if e := version(a.SchemaVersion); e != nil {
		return e
	}
	for _, s := range []string{a.AssignmentID, a.RunID, a.WorkerID, a.SessionID, a.Runtime, a.Profile} {
		if !id(s) {
			return errors.New("invalid assignment identity")
		}
	}
	if !validDigest(a.PackageDigest) || a.PolicyRevision < 1 || a.StopNewUnitsAt.IsZero() || a.StopNewUnitsAt.After(a.TerminateAt) || !a.TerminateAt.Before(a.HandoffDeadline) {
		return errors.New("invalid assignment bounds")
	}
	return refs(a.Units)
}

type ResultFile struct {
	Path     string   `json:"path"`
	Artifact Artifact `json:"artifact"`
}

func ValidPath(s string) bool {
	return s != "" && len(s) <= 4096 && s != "." && !strings.ContainsAny(s, "\\\x00") && !path.IsAbs(s) && path.Clean(s) == s && s != ".." && !strings.HasPrefix(s, "../")
}

type Candidate struct {
	SchemaVersion int          `json:"schema_version"`
	UnitID        string       `json:"unit_id"`
	SpecDigest    string       `json:"spec_digest"`
	AssignmentID  string       `json:"assignment_id"`
	AttemptID     string       `json:"attempt_id"`
	Outcome       string       `json:"outcome"`
	Files         []ResultFile `json:"files"`
}

func (c Candidate) Validate() error {
	if e := version(c.SchemaVersion); e != nil {
		return e
	}
	if !id(c.UnitID) || !validDigest(c.SpecDigest) || !id(c.AssignmentID) || !id(c.AttemptID) || !id(c.Outcome) || len(c.Files) == 0 || len(c.Files) > MaxUnits {
		return errors.New("invalid candidate")
	}
	seen := map[string]bool{}
	for _, f := range c.Files {
		if !ValidPath(f.Path) || seen[f.Path] {
			return errors.New("invalid result path")
		}
		seen[f.Path] = true
		if e := f.Artifact.Validate(); e != nil {
			return e
		}
	}
	return nil
}

// Message is NDJSON. Published acknowledges agent-local durable publication.
// ExecuteUnit -> UnitResult -> Published is serialized; Finish -> Finished closes grants.
type Message struct {
	SchemaVersion int    `json:"schema_version"`
	Type          string `json:"type"`
	UnitID        string `json:"unit_id,omitempty"`
	ManifestPath  string `json:"manifest_path,omitempty"`
}

func (m Message) Validate() error {
	if e := version(m.SchemaVersion); e != nil {
		return e
	}
	switch m.Type {
	case "ready", "finish", "finished":
		if m.UnitID != "" || m.ManifestPath != "" {
			return errors.New("unexpected message fields")
		}
	case "execute_unit", "published":
		if !id(m.UnitID) || m.ManifestPath != "" {
			return errors.New("invalid unit message")
		}
	case "unit_result":
		if !id(m.UnitID) || !ValidPath(m.ManifestPath) {
			return errors.New("invalid result message")
		}
	default:
		return errors.New("unknown message type")
	}
	return nil
}
func ReadMessage(r *bufio.Reader) (Message, error) {
	var m Message
	b, e := r.ReadSlice('\n')
	if e != nil {
		return m, e
	}
	if len(b) > MaxMessageBytes {
		return m, errors.New("message too large")
	}
	if e = decode(b, &m); e != nil {
		return m, e
	}
	return m, m.Validate()
}
func MessageReader(r io.Reader) *bufio.Reader { return bufio.NewReaderSize(r, MaxMessageBytes+1) }
func WriteMessage(w io.Writer, m Message) error {
	if e := m.Validate(); e != nil {
		return e
	}
	return json.NewEncoder(w).Encode(m)
}

type Verdict struct {
	SchemaVersion int    `json:"schema_version"`
	Valid         bool   `json:"valid"`
	Reason        string `json:"reason"`
}

// UnmarshalJSON requires an explicit boolean: missing/null is an operational
// protocol error, never an implicit domain rejection.
func (v *Verdict) UnmarshalJSON(b []byte) error {
	var wire struct {
		SchemaVersion int    `json:"schema_version"`
		Valid         *bool  `json:"valid"`
		Reason        string `json:"reason"`
	}
	if err := decode(b, &wire); err != nil {
		return err
	}
	if wire.Valid == nil {
		return errors.New("verdict requires explicit non-null valid boolean")
	}
	*v = Verdict{SchemaVersion: wire.SchemaVersion, Valid: *wire.Valid, Reason: wire.Reason}
	return nil
}

func (v Verdict) Validate() error {
	if e := version(v.SchemaVersion); e != nil {
		return e
	}
	if len(v.Reason) > 4096 {
		return errors.New("verdict reason too long")
	}
	return nil
}

// ValidateGraph checks a sealed submission before issuance.
func ValidateGraph(units []Unit) error {
	if len(units) == 0 || len(units) > MaxUnits {
		return errors.New("invalid graph size")
	}
	byID := map[string]Unit{}
	for _, u := range units {
		if e := u.Validate(); e != nil {
			return e
		}
		if _, ok := byID[u.UnitID]; ok {
			return errors.New("duplicate unit")
		}
		byID[u.UnitID] = u
	}
	state := map[string]int{}
	var visit func(string) error
	visit = func(s string) error {
		u, ok := byID[s]
		if !ok {
			return errors.New("missing dependency")
		}
		if state[s] == 1 {
			return errors.New("dependency cycle")
		}
		if state[s] == 2 {
			return nil
		}
		state[s] = 1
		for _, d := range u.Dependencies {
			if e := visit(d); e != nil {
				return e
			}
		}
		state[s] = 2
		return nil
	}
	for s := range byID {
		if e := visit(s); e != nil {
			return e
		}
	}
	return nil
}
