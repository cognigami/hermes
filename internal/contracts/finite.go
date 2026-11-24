package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"
)

const MaxFiniteMembers = 64

// Runtime artifacts are descriptions, never executable/argv permission. The
// deployment must match them against its independently approved runtime binding.
type FiniteRuntimeRef struct {
	Runtime     string     `json:"runtime"`
	GOOS        string     `json:"goos"`
	GOARCH      string     `json:"goarch"`
	ISABaseline string     `json:"isa_baseline"`
	Executable  Artifact   `json:"executable"`
	Support     []Artifact `json:"support,omitempty"`
}
type FiniteMember struct {
	Work             Work   `json:"work"`
	AssignmentDigest string `json:"assignment_digest"`
}
type FiniteBundle struct {
	SchemaVersion  int                `json:"schema_version"`
	BundleID       string             `json:"bundle_id"`
	WorkerID       string             `json:"worker_id"`
	SessionID      string             `json:"session_id"`
	HostID         string             `json:"host_id"`
	ExportRevision int64              `json:"export_revision"`
	Members        []FiniteMember     `json:"members"`
	RuntimeRefs    []FiniteRuntimeRef `json:"runtime_refs"`
	Blobs          []Artifact         `json:"blobs"`
}

// The payload has no digest field: its canonical Digest is not self-referential.
type FiniteBundleEnvelope struct {
	BundleDigest string       `json:"bundle_digest"`
	Bundle       FiniteBundle `json:"bundle"`
}
type FiniteExportRequest struct {
	SchemaVersion   int       `json:"schema_version"`
	RequestID       string    `json:"request_id"`
	BundleID        string    `json:"bundle_id"`
	WorkerID        string    `json:"worker_id"`
	SessionID       string    `json:"session_id"`
	HostID          string    `json:"host_id"`
	MemberLimit     int       `json:"member_limit"`
	StopNewUnitsAt  time.Time `json:"stop_new_units_at"`
	TerminateAt     time.Time `json:"terminate_at"`
	HandoffDeadline time.Time `json:"handoff_deadline"`
}
type FiniteFinalInventory struct {
	SchemaVersion    int              `json:"schema_version"`
	BundleDigest     string           `json:"bundle_digest"`
	AssignmentDigest string           `json:"assignment_digest"`
	Uncertain        bool             `json:"uncertain"`
	Reconciliation   ReconcileRequest `json:"reconciliation"`
	Final            *FinalReport     `json:"final,omitempty"`
	ResourceStop     *ResourceStop    `json:"resource_stop,omitempty"`
}
type FiniteMemberAck struct {
	AssignmentDigest         string   `json:"assignment_digest"`
	RawPublished             []string `json:"raw_published,omitempty"`
	ReconciliationRevision   int64    `json:"reconciliation_revision,omitempty"`
	FinalDigest              string   `json:"final_digest,omitempty"`
	ResourceStopAcknowledged bool     `json:"resource_stop_acknowledged,omitempty"`
}
type FiniteImportAck struct {
	BundleID      string            `json:"bundle_id"`
	SchemaVersion int               `json:"schema_version"`
	BundleDigest  string            `json:"bundle_digest"`
	Members       []FiniteMemberAck `json:"members"`
}

func SafeFiniteBundleID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}
func finiteMetadata(v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if len(b) > MaxMetadataBytes {
		return errors.New("finite metadata exceeds bound")
	}
	return nil
}
func (r FiniteRuntimeRef) Validate() error {
	if !id(r.Runtime) || !id(r.GOOS) || !id(r.GOARCH) || !id(r.ISABaseline) || len(r.Support) > MaxUnits {
		return errors.New("invalid approved runtime reference")
	}
	if e := r.Executable.Validate(); e != nil {
		return e
	}
	for _, a := range r.Support {
		if e := a.Validate(); e != nil {
			return e
		}
	}
	return nil
}
func (r FiniteExportRequest) Validate() error {
	if version(r.SchemaVersion) != nil || !id(r.RequestID) || !SafeFiniteBundleID(r.BundleID) || !id(r.WorkerID) || !id(r.SessionID) || !id(r.HostID) || r.MemberLimit < 1 || r.MemberLimit > MaxFiniteMembers || r.StopNewUnitsAt.IsZero() || r.StopNewUnitsAt.After(r.TerminateAt) || !r.TerminateAt.Before(r.HandoffDeadline) {
		return errors.New("invalid finite export request")
	}
	return finiteMetadata(r)
}
func (e FiniteBundleEnvelope) Validate() error {
	if !validDigest(e.BundleDigest) {
		return errors.New("invalid finite bundle digest")
	}
	if err := e.Bundle.Validate(); err != nil {
		return err
	}
	digest, err := Digest(e.Bundle)
	if err != nil {
		return err
	}
	if digest != e.BundleDigest {
		return errors.New("finite bundle digest mismatch")
	}
	return finiteMetadata(e)
}
func (b FiniteBundle) Validate() error {
	if version(b.SchemaVersion) != nil || !SafeFiniteBundleID(b.BundleID) || !id(b.WorkerID) || !id(b.SessionID) || !id(b.HostID) || b.ExportRevision < 1 || len(b.Members) < 1 || len(b.Members) > MaxFiniteMembers || len(b.RuntimeRefs) < 1 || len(b.RuntimeRefs) > MaxFiniteMembers || len(b.Blobs) > MaxUnits {
		return errors.New("invalid finite bundle scope/bounds")
	}
	inventory := map[string]Artifact{}
	for _, a := range b.Blobs {
		if e := a.Validate(); e != nil {
			return e
		}
		if _, ok := inventory[a.Digest]; ok {
			return errors.New("duplicate finite inventory digest")
		}
		inventory[a.Digest] = a
	}
	check := func(a Artifact) error {
		stored, ok := inventory[a.Digest]
		if !ok || stored != a {
			return errors.New("artifact missing/conflicting in finite inventory")
		}
		return nil
	}
	runtimes := map[string]bool{}
	for _, r := range b.RuntimeRefs {
		if e := r.Validate(); e != nil {
			return e
		}
		if runtimes[r.Runtime] {
			return errors.New("duplicate runtime binding")
		}
		runtimes[r.Runtime] = true
		if e := check(r.Executable); e != nil {
			return e
		}
		for _, a := range r.Support {
			if e := check(a); e != nil {
				return e
			}
		}
	}
	seen := map[string]bool{}
	ownedUnits := map[[2]string]bool{}
	for _, m := range b.Members {
		w := m.Work
		if w.SchemaVersion != Version || w.Assignment == nil || w.Package == nil || w.Budget == nil {
			return errors.New("incomplete finite member")
		}
		a := w.Assignment
		if e := a.Validate(); e != nil {
			return e
		}
		if e := a.Allocation.Validate(); e != nil {
			return e
		}
		if e := w.Package.Validate(); e != nil {
			return e
		}
		ad, _ := Digest(*a)
		pd, _ := Digest(*w.Package)
		if seen[a.AssignmentID] || ad != m.AssignmentDigest || a.PackageDigest != pd || a.WorkerID != b.WorkerID || a.SessionID != b.SessionID || !runtimes[a.Runtime] {
			return errors.New("finite member identity/digest mismatch")
		}
		seen[a.AssignmentID] = true
		budget := w.Budget
		if budget.SchemaVersion != Version || budget.PolicyRevision != a.PolicyRevision || budget.Approval.WorkerID != b.WorkerID || budget.Approval.HostID != b.HostID || len(b.Members) > budget.Approval.MaxAssignments {
			return errors.New("finite member host budget mismatch")
		}
		approved := false
		for _, p := range budget.Profiles {
			if p.ID == a.Profile && p.Allocation == a.Allocation {
				approved = true
			}
		}
		if !approved {
			return errors.New("finite allocation not in approved profile")
		}
		if len(w.Units) != len(a.Units) {
			return errors.New("finite member unit inventory mismatch")
		}
		packageRefs := map[string]string{}
		for _, ref := range w.Package.Units {
			packageRefs[ref.UnitID] = ref.SpecDigest
		}
		for i, u := range w.Units {
			key := [2]string{a.RunID, u.UnitID}
			if ownedUnits[key] {
				return errors.New("duplicate finite run/unit owner")
			}
			ownedUnits[key] = true
			if e := u.Validate(); e != nil {
				return e
			}
			if u.UnitID != a.Units[i].UnitID || u.SpecDigest != a.Units[i].SpecDigest || packageRefs[u.UnitID] != u.SpecDigest || u.Runtime != a.Runtime {
				return errors.New("finite member unit binding mismatch")
			}
			for _, input := range u.Inputs {
				if e := check(input); e != nil {
					return e
				}
			}
		}
		for _, input := range w.Package.Preparation {
			if e := check(input); e != nil {
				return e
			}
		}
	}
	return finiteMetadata(b)
}
func (f *FiniteFinalInventory) UnmarshalJSON(data []byte) error {
	type plain FiniteFinalInventory
	var raw map[string]json.RawMessage
	if e := json.Unmarshal(data, &raw); e != nil {
		return e
	}
	if v, ok := raw["uncertain"]; !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
		return errors.New("missing explicit finite uncertainty proof")
	}
	var lifecycle map[string]json.RawMessage
	if e := json.Unmarshal(raw["reconciliation"], &lifecycle); e != nil {
		return e
	}
	for _, key := range []string{"process_started", "quiescent"} {
		v, ok := lifecycle[key]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return errors.New("missing explicit finite lifecycle proof")
		}
	}
	var value plain
	if e := decode(data, &value); e != nil {
		return e
	}
	*f = FiniteFinalInventory(value)
	return f.Validate()
}
func (f FiniteFinalInventory) Validate() error {
	r := f.Reconciliation
	if version(f.SchemaVersion) != nil || !validDigest(f.BundleDigest) || !validDigest(f.AssignmentDigest) || r.SchemaVersion != Version || r.Revision < 1 || r.AssignmentDigest != f.AssignmentDigest || !id(r.AssignmentID) || !id(r.WorkerID) || !id(r.SessionID) || !id(r.RequestID) || len(r.Started) > MaxUnits || len(r.CandidateDigests) > MaxUnits || f.Uncertain && r.Quiescent {
		return errors.New("invalid finite lifecycle inventory")
	}
	if !r.ProcessStarted && (len(r.Started) > 0 || len(r.CandidateDigests) > 0) {
		return errors.New("finite grants/candidates without process start")
	}
	started := map[string]bool{}
	for _, unit := range r.Started {
		if !id(unit) || started[unit] {
			return errors.New("invalid finite granted unit inventory")
		}
		started[unit] = true
	}
	digests := map[string]bool{}
	for _, digest := range r.CandidateDigests {
		if !validDigest(digest) || digests[digest] {
			return errors.New("invalid finite candidate digest inventory")
		}
		digests[digest] = true
	}
	if len(r.CandidateDigests) > len(r.Started) {
		return errors.New("finite candidates exceed grants")
	}
	validReceipt := func(receipt Receipt) bool {
		return receipt.SchemaVersion == Version && id(receipt.RequestID) && receipt.AssignmentID == r.AssignmentID && receipt.WorkerID == r.WorkerID && receipt.SessionID == r.SessionID
	}
	if f.Final != nil {
		if f.Uncertain || !r.Quiescent || !validReceipt(f.Final.Receipt) {
			return errors.New("finite final lacks matching positive quiescence")
		}
		partition := map[string]bool{}
		for _, list := range [][]string{f.Final.Completed, f.Final.Interrupted, f.Final.NeverStarted} {
			if len(list) > MaxUnits {
				return errors.New("finite final partition exceeds bound")
			}
			for _, unit := range list {
				if !id(unit) || partition[unit] {
					return errors.New("invalid/overlapping finite final partition")
				}
				partition[unit] = true
			}
		}
		if len(partition) == 0 || len(partition) > MaxUnits || len(f.Final.Completed) != len(r.CandidateDigests) {
			return errors.New("invalid finite final candidate partition")
		}
		for _, unit := range f.Final.Completed {
			if !started[unit] {
				return errors.New("finite completed unit not granted")
			}
		}
		for _, unit := range f.Final.Interrupted {
			if !started[unit] {
				return errors.New("finite interrupted unit not granted")
			}
		}
		for _, unit := range f.Final.NeverStarted {
			if started[unit] {
				return errors.New("finite never-started unit was granted")
			}
		}
	}
	if stop := f.ResourceStop; stop != nil {
		if !validReceipt(stop.Receipt) || !validDigest(stop.AllocationDigest) || !id(stop.Kind) || !id(stop.Source) || !id(stop.EvidenceReference) || stop.KnownInadequateLimit < 0 || stop.GrantIndex < 0 || stop.ConfirmedMinimum.Validate() != nil || stop.Phase != "prepare" && stop.Phase != "unit" {
			return errors.New("invalid finite resource evidence envelope")
		}
	}
	return finiteMetadata(f)
}

// ValidateForAssignment adds exact issued membership/partition checks. Importers
// must additionally use the ordinary candidate/resource evidence validators.
func (f FiniteFinalInventory) ValidateForAssignment(a Assignment) error {
	if e := f.Validate(); e != nil {
		return e
	}
	if e := a.Validate(); e != nil {
		return e
	}
	digest, e := Digest(a)
	if e != nil {
		return e
	}
	r := f.Reconciliation
	if digest != f.AssignmentDigest || r.AssignmentID != a.AssignmentID || r.WorkerID != a.WorkerID || r.SessionID != a.SessionID || len(r.Started) > len(a.Units) {
		return errors.New("finite final does not bind issued assignment")
	}
	for i, unit := range r.Started {
		if a.Units[i].UnitID != unit {
			return errors.New("finite grants not issued unit prefix")
		}
	}
	if stop := f.ResourceStop; stop != nil {
		allocation, _ := Digest(a.Allocation)
		if allocation != stop.AllocationDigest || stop.GrantIndex != len(r.Started) || stop.Phase == "prepare" && (stop.GrantIndex != 0 || stop.UnitID != "") || stop.Phase == "unit" && (stop.GrantIndex < 1 || r.Started[stop.GrantIndex-1] != stop.UnitID) {
			return errors.New("finite resource evidence not bound to issued allocation/grant")
		}
	}
	if f.Final != nil {
		members := map[string]bool{}
		for _, unit := range a.Units {
			members[unit.UnitID] = true
		}
		count := 0
		for _, list := range [][]string{f.Final.Completed, f.Final.Interrupted, f.Final.NeverStarted} {
			for _, unit := range list {
				if !members[unit] {
					return errors.New("foreign finite final unit")
				}
				count++
			}
		}
		if count != len(a.Units) {
			return errors.New("finite final partition incomplete")
		}
	}
	return nil
}
func (a FiniteImportAck) Validate() error {
	if version(a.SchemaVersion) != nil || !SafeFiniteBundleID(a.BundleID) || !validDigest(a.BundleDigest) || len(a.Members) > MaxFiniteMembers {
		return errors.New("invalid finite import acknowledgment")
	}
	seen := map[string]bool{}
	for _, m := range a.Members {
		if !validDigest(m.AssignmentDigest) || seen[m.AssignmentDigest] || len(m.RawPublished) > MaxUnits || m.ReconciliationRevision < 0 || m.FinalDigest != "" && !validDigest(m.FinalDigest) {
			return errors.New("invalid finite member acknowledgment")
		}
		seen[m.AssignmentDigest] = true
		digests := map[string]bool{}
		for _, d := range m.RawPublished {
			if !validDigest(d) || digests[d] {
				return errors.New("invalid duplicate raw acknowledgment")
			}
			digests[d] = true
		}
	}
	return finiteMetadata(a)
}
