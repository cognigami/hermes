package contracts

import (
	"errors"
	"math"
)

// ResourceVector is an operational budget, never scientific unit identity.
type ResourceVector struct {
	CPUMilli    int64 `json:"cpu_milli"`
	MemoryBytes int64 `json:"memory_bytes"`
	SpoolBytes  int64 `json:"spool_bytes"`
}

func (v ResourceVector) Validate() error {
	if v.CPUMilli < 0 || v.MemoryBytes < 0 || v.SpoolBytes < 0 {
		return errors.New("negative resource quantity")
	}
	return nil
}
func (v ResourceVector) Add(b ResourceVector) (ResourceVector, error) {
	if e := v.Validate(); e != nil {
		return v, e
	}
	if e := b.Validate(); e != nil {
		return v, e
	}
	if v.CPUMilli > math.MaxInt64-b.CPUMilli || v.MemoryBytes > math.MaxInt64-b.MemoryBytes || v.SpoolBytes > math.MaxInt64-b.SpoolBytes {
		return v, errors.New("resource quantity overflow")
	}
	return ResourceVector{v.CPUMilli + b.CPUMilli, v.MemoryBytes + b.MemoryBytes, v.SpoolBytes + b.SpoolBytes}, nil
}
func (v ResourceVector) Fits(limit ResourceVector) bool {
	return v.Validate() == nil && limit.Validate() == nil && v.CPUMilli <= limit.CPUMilli && v.MemoryBytes <= limit.MemoryBytes && v.SpoolBytes <= limit.SpoolBytes
}

type Allocation struct {
	Resources              ResourceVector `json:"resources"`
	Exclusive              bool           `json:"exclusive"`
	RequiresHardTreeMemory bool           `json:"requires_hard_tree_memory"`
}

func (a Allocation) Validate() error {
	if e := a.Resources.Validate(); e != nil {
		return e
	}
	if a.Resources.CPUMilli == 0 || a.Resources.MemoryBytes == 0 || a.Resources.SpoolBytes == 0 {
		return errors.New("allocation requires explicit positive CPU, memory and spool budgets")
	}
	return nil
}

type ResourceProfile struct {
	ID           string     `json:"id"`
	Allocation   Allocation `json:"allocation"`
	Conservative bool       `json:"conservative"`
}
type HostApproval struct {
	HostID         string         `json:"host_id"`
	WorkerID       string         `json:"worker_id,omitempty"`
	Limits         ResourceVector `json:"limits"`
	Baseline       ResourceVector `json:"baseline"`
	MaxAssignments int            `json:"max_assignments"`
}
type ResourcePolicy struct {
	SchemaVersion         int               `json:"schema_version"`
	Revision              int64             `json:"revision"`
	ControllerHostID      string            `json:"controller_host_id"`
	Hosts                 []HostApproval    `json:"hosts"`
	Profiles              []ResourceProfile `json:"profiles"`
	TransferSlots         int               `json:"transfer_slots"`
	ValidatorSlots        int               `json:"validator_slots"`
	TransferReservation   ResourceVector    `json:"transfer_reservation"`
	ValidatorReservation  ResourceVector    `json:"validator_reservation"`
	AllowSyntheticBackend bool              `json:"allow_synthetic_backend"`
}

func (p ResourcePolicy) Validate() error {
	if p.SchemaVersion != 1 || p.Revision < 1 || !id(p.ControllerHostID) || len(p.Hosts) == 0 || len(p.Hosts) > 64 || len(p.Profiles) == 0 || len(p.Profiles) > 64 || p.TransferSlots < 1 || p.TransferSlots > 16 || p.ValidatorSlots < 1 || p.ValidatorSlots > 16 {
		return errors.New("invalid resource policy bounds")
	}
	hosts, workers, profiles := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, h := range p.Hosts {
		if !id(h.HostID) || hosts[h.HostID] || h.Limits.Validate() != nil || h.Baseline.Validate() != nil || !h.Baseline.Fits(h.Limits) {
			return errors.New("invalid host approval")
		}
		hosts[h.HostID] = true
		if h.WorkerID != "" {
			if !id(h.WorkerID) || workers[h.WorkerID] {
				return errors.New("host and worker binding must be one-to-one")
			}
			workers[h.WorkerID] = true
		}
	}
	if !hosts[p.ControllerHostID] {
		return errors.New("controller host lacks approved budget")
	}
	for _, profile := range p.Profiles {
		if !id(profile.ID) || profiles[profile.ID] || profile.Allocation.Validate() != nil {
			return errors.New("invalid approved profile")
		}
		profiles[profile.ID] = true
	}
	if p.TransferReservation.Validate() != nil || p.ValidatorReservation.Validate() != nil {
		return errors.New("invalid controller overhead")
	}
	return nil
}

type HostObservation struct {
	Resources           ResourceVector `json:"resources"`
	AvailableSpoolBytes int64          `json:"available_spool_bytes"`
	HardTreeMemory      string         `json:"hard_tree_memory"`
	Source              string         `json:"source"`
}
type WorkerRegistration struct {
	SchemaVersion      int             `json:"schema_version"`
	RequestID          string          `json:"request_id"`
	WorkerID           string          `json:"worker_id"`
	SessionID          string          `json:"session_id"`
	HostID             string          `json:"host_id"`
	Observed           HostObservation `json:"observed"`
	GOOS               string          `json:"goos"`
	GOARCH             string          `json:"goarch"`
	RetainedSpoolBytes int64           `json:"retained_spool_bytes,omitempty"`
	TotalSpoolBytes    *int64          `json:"total_spool_bytes,omitempty"`
	SpoolRevision      int64           `json:"spool_revision,omitempty"`
	InventoryRevision  int64           `json:"inventory_revision,omitempty"`
}
type HostBudget struct {
	SchemaVersion           int               `json:"schema_version"`
	PolicyRevision          int64             `json:"policy_revision"`
	Approval                HostApproval      `json:"approval"`
	ControllerOverhead      ResourceVector    `json:"controller_overhead"`
	OtherRetainedSpoolBytes int64             `json:"other_retained_spool_bytes"`
	Profiles                []ResourceProfile `json:"profiles"`
	AllowSyntheticBackend   bool              `json:"allow_synthetic_backend"`
}
type UnitResourceRequirement struct {
	UnitID          string         `json:"unit_id"`
	Minimum         ResourceVector `json:"minimum"`
	AllowedHosts    []string       `json:"allowed_hosts,omitempty"`
	AllowedProfiles []string       `json:"allowed_profiles,omitempty"`
}
type PolicyRequest struct {
	SchemaVersion    int            `json:"schema_version"`
	RequestID        string         `json:"request_id"`
	ExpectedRevision int64          `json:"expected_revision"`
	Policy           ResourcePolicy `json:"policy"`
}

type ResourceStop struct {
	Receipt
	AllocationDigest     string         `json:"allocation_digest"`
	Phase                string         `json:"phase"`
	UnitID               string         `json:"unit_id,omitempty"`
	Kind                 string         `json:"kind"`
	Source               string         `json:"source"`
	EvidenceReference    string         `json:"evidence_reference"`
	KnownInadequateLimit int64          `json:"known_inadequate_limit"`
	GrantIndex           int            `json:"grant_index"`
	ConfirmedMinimum     ResourceVector `json:"confirmed_minimum"`
}
