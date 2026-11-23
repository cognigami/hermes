package contracts

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
	SchemaVersion int       `json:"schema_version"`
	RequestID     string    `json:"request_id"`
	RunID         string    `json:"run_id"`
	Units         []Unit    `json:"units"`
	Packages      []Package `json:"packages"`
}
type PollRequest struct {
	SchemaVersion int    `json:"schema_version"`
	RequestID     string `json:"request_id"`
	WorkerID      string `json:"worker_id"`
	SessionID     string `json:"session_id"`
}
type Work struct {
	SchemaVersion int         `json:"schema_version"`
	Assignment    *Assignment `json:"assignment,omitempty"`
	Package       *Package    `json:"package,omitempty"`
	Units         []Unit      `json:"units,omitempty"`
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
	Completed    []string `json:"completed"`
	NeverStarted []string `json:"never_started"`
}
type Status struct {
	SchemaVersion      int    `json:"schema_version"`
	RunID              string `json:"run_id"`
	Units              int    `json:"units"`
	Accepted           int    `json:"accepted"`
	Quiescent          bool   `json:"quiescent"`
	ValidationFailures int    `json:"validation_failures"`
	Conflicts          int    `json:"conflicts"`
}
