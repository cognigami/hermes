package contracts

import (
	"errors"
	"time"
)

// WorkflowRegistration binds a sealed initial graph and a deterministic leaf
// adapter. Settings and decision files are application data, not Hermes rules.
type WorkflowRegistration struct {
	SchemaVersion     int        `json:"schema_version"`
	RequestID         string     `json:"request_id"`
	WorkflowID        string     `json:"workflow_id"`
	Contract          string     `json:"contract"`
	RuntimeDigest     string     `json:"runtime_digest"`
	Settings          Artifact   `json:"settings"`
	Initial           Submission `json:"initial"`
	DecisionUnit      string     `json:"decision_unit"`
	DecisionPath      string     `json:"decision_path"`
	MaxSubmittedUnits *int64     `json:"max_submitted_units,omitempty"`
}

func (r WorkflowRegistration) Validate() error {
	if version(r.SchemaVersion) != nil || !id(r.RequestID) || !id(r.WorkflowID) || !id(r.Contract) || !validDigest(r.RuntimeDigest) || !id(r.DecisionUnit) || !ValidPath(r.DecisionPath) {
		return errors.New("invalid workflow registration")
	}
	if err := r.Settings.Validate(); err != nil {
		return err
	}
	if r.MaxSubmittedUnits != nil && (*r.MaxSubmittedUnits < 1 || int64(len(r.Initial.Units)) > *r.MaxSubmittedUnits) {
		return errors.New("initial graph exceeds submitted-unit allowance")
	}
	return nil
}

type WorkflowControl struct {
	SchemaVersion    int       `json:"schema_version"`
	RequestID        string    `json:"request_id"`
	WorkflowID       string    `json:"workflow_id"`
	ExpectedRevision int64     `json:"expected_revision"`
	Action           string    `json:"action"`
	TerminateAt      time.Time `json:"terminate_at,omitempty"`
}

func (r WorkflowControl) Validate() error {
	if version(r.SchemaVersion) != nil || !id(r.RequestID) || !id(r.WorkflowID) || r.ExpectedRevision < 1 || (r.Action != "stop" && r.Action != "resume") || (r.Action == "stop" && r.TerminateAt.IsZero()) {
		return errors.New("invalid workflow control")
	}
	return nil
}

type WorkflowStatus struct {
	SchemaVersion     int           `json:"schema_version"`
	WorkflowID        string        `json:"workflow_id"`
	Revision          int64         `json:"revision"`
	State             string        `json:"state"`
	Stage             int64         `json:"stage"`
	RunID             string        `json:"run_id"`
	CreatedAt         time.Time     `json:"created_at"`
	Elapsed           time.Duration `json:"elapsed_ns"`
	SubmittedUnits    int64         `json:"submitted_units"`
	MaxSubmittedUnits *int64        `json:"max_submitted_units,omitempty"`
	Reason            string        `json:"reason,omitempty"`
}

// Input paths are relative to the private invocation directory. Only the named
// canonical decision and settings bytes are staged, never historical outputs.
type WorkflowAdapterInput struct {
	SchemaVersion   int      `json:"schema_version"`
	WorkflowID      string   `json:"workflow_id"`
	Stage           int64    `json:"stage"`
	Contract        string   `json:"contract"`
	RuntimeDigest   string   `json:"runtime_digest"`
	CandidateDigest string   `json:"candidate_digest"`
	Decision        Artifact `json:"decision"`
	Settings        Artifact `json:"settings"`
	DecisionFile    string   `json:"decision_file"`
	SettingsFile    string   `json:"settings_file"`
}

func (r WorkflowAdapterInput) Validate() error {
	if version(r.SchemaVersion) != nil || !id(r.WorkflowID) || r.Stage < 0 || !id(r.Contract) || !validDigest(r.RuntimeDigest) || !validDigest(r.CandidateDigest) || !ValidPath(r.DecisionFile) || !ValidPath(r.SettingsFile) || r.DecisionFile == r.SettingsFile {
		return errors.New("invalid workflow adapter input")
	}
	if err := r.Decision.Validate(); err != nil {
		return err
	}
	return r.Settings.Validate()
}

type WorkflowNext struct {
	Submission   Submission `json:"submission"`
	DecisionUnit string     `json:"decision_unit"`
	DecisionPath string     `json:"decision_path"`
}

func (r WorkflowNext) Validate() error {
	if !id(r.Submission.RunID) || !id(r.Submission.RequestID) || !id(r.DecisionUnit) || !ValidPath(r.DecisionPath) {
		return errors.New("invalid workflow successor")
	}
	if err := ValidateGraph(r.Submission.Units); err != nil {
		return err
	}
	for _, u := range r.Submission.Units {
		if u.UnitID == r.DecisionUnit {
			return nil
		}
	}
	return errors.New("decision unit missing from successor")
}

type WorkflowAdapterOutput struct {
	SchemaVersion int           `json:"schema_version"`
	Next          *WorkflowNext `json:"next,omitempty"`
	StopReason    string        `json:"stop_reason,omitempty"`
}

func (r WorkflowAdapterOutput) Validate() error {
	if version(r.SchemaVersion) != nil || (r.Next == nil) == (r.StopReason == "") || len(r.StopReason) > 4096 {
		return errors.New("adapter must return exactly a successor or prescribed stop")
	}
	if r.Next != nil {
		return r.Next.Validate()
	}
	return nil
}
