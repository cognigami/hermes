package contracts

import "time"

type Acknowledgment struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id,omitempty"`
	Received      bool   `json:"received,omitempty"`
	RawPublished  string `json:"raw_published,omitempty"`
	Quiescent     bool   `json:"quiescent,omitempty"`
	Published     bool   `json:"published,omitempty"`
}

// Connected DTOs contain bounded metadata only. Every mutation has a stable
// request ID; a retry must repeat exactly the same body.
type Submission struct {
	SchemaVersion int                       `json:"schema_version"`
	RequestID     string                    `json:"request_id"`
	RunID         string                    `json:"run_id"`
	Units         []Unit                    `json:"units"`
	Packages      []Package                 `json:"packages"`
	Requirements  []UnitResourceRequirement `json:"requirements,omitempty"`
}
type RetainedAssignment struct {
	AssignmentID string `json:"assignment_id"`
	Digest       string `json:"digest"`
}

type PollRequest struct {
	SchemaVersion      int                  `json:"schema_version"`
	RequestID          string               `json:"request_id"`
	WorkerID           string               `json:"worker_id"`
	SessionID          string               `json:"session_id"`
	Retained           []RetainedAssignment `json:"retained,omitempty"`
	RetainedSpoolBytes int64                `json:"retained_spool_bytes,omitempty"`
	SpoolRevision      int64                `json:"spool_revision,omitempty"`
	InventoryRevision  int64                `json:"inventory_revision,omitempty"`
}
type Work struct {
	SchemaVersion int         `json:"schema_version"`
	Assignment    *Assignment `json:"assignment,omitempty"`
	Package       *Package    `json:"package,omitempty"`
	Units         []Unit      `json:"units,omitempty"`
	Budget        *HostBudget `json:"budget,omitempty"`
}
type Receipt struct {
	SchemaVersion int    `json:"schema_version"`
	RequestID     string `json:"request_id"`
	AssignmentID  string `json:"assignment_id"`
	WorkerID      string `json:"worker_id"`
	SessionID     string `json:"session_id"`
}
type CandidateReport struct {
	Receipt
	Candidate Candidate `json:"candidate"`
}
type FinalReport struct {
	Receipt
	Completed         []string `json:"completed"`
	NeverStarted      []string `json:"never_started"`
	Interrupted       []string `json:"interrupted"`
	OperationalReason string   `json:"operational_reason,omitempty"`
	LaunchFailed      bool     `json:"launch_failed,omitempty"`
}
type Status struct {
	SchemaVersion             int    `json:"schema_version"`
	RunID                     string `json:"run_id"`
	Units                     int    `json:"units"`
	Accepted                  int    `json:"accepted"`
	Quiescent                 bool   `json:"quiescent"`
	ValidationFailures        int    `json:"validation_failures"`
	Conflicts                 int    `json:"conflicts"`
	UncertainAssignments      int    `json:"uncertain_assignments"`
	Exhausted                 int    `json:"exhausted"`
	DataLossUnits             int    `json:"data_loss_units"`
	WaitingRetainedValidation int    `json:"waiting_retained_validation"`
}

type ControlRequest struct {
	SchemaVersion    int       `json:"schema_version"`
	RequestID        string    `json:"request_id"`
	TargetKind       string    `json:"target_kind"`
	TargetID         string    `json:"target_id"`
	ExpectedRevision int64     `json:"expected_revision"`
	Action           string    `json:"action"`
	TerminateAt      time.Time `json:"terminate_at"`
}
type AssignmentControl struct {
	SchemaVersion int       `json:"schema_version"`
	AssignmentID  string    `json:"assignment_id"`
	Revision      int64     `json:"revision"`
	Action        string    `json:"action"`
	TerminateAt   time.Time `json:"terminate_at"`
}
type ControlAck struct {
	Receipt
	Revision          int64  `json:"revision"`
	LastGrantIndex    int    `json:"last_grant_index"`
	LastGrantedUnitID string `json:"last_granted_unit_id"`
}
type ControlStatus struct {
	SchemaVersion        int    `json:"schema_version"`
	TargetKind           string `json:"target_kind"`
	TargetID             string `json:"target_id"`
	Revision             int64  `json:"revision"`
	IssuancePaused       bool   `json:"issuance_paused"`
	PendingBarriers      int    `json:"pending_barriers"`
	LiveAssignments      int    `json:"live_assignments"`
	UncertainAssignments int    `json:"uncertain_assignments"`
	State                string `json:"state"`
}
type ReconcileRequest struct {
	Receipt
	Revision          int64    `json:"revision"`
	OperationalReason string   `json:"operational_reason,omitempty"`
	AssignmentDigest  string   `json:"assignment_digest"`
	ProcessStarted    bool     `json:"process_started"`
	Quiescent         bool     `json:"quiescent"`
	Started           []string `json:"started"`
	CandidateDigests  []string `json:"candidate_digests"`
}
type ReconcileResult struct {
	SchemaVersion int    `json:"schema_version"`
	State         string `json:"state"`
}
type LostSpoolResolution struct {
	Receipt
	SpoolIrrecoverablyLost bool   `json:"spool_irrecoverably_lost"`
	ExecutionTerminated    bool   `json:"execution_terminated"`
	EvidenceKind           string `json:"evidence_kind"`
	Reason                 string `json:"reason"`
}
