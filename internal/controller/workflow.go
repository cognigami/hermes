package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"hermes/internal/contracts"
)

const workflowSchema = `
CREATE TABLE IF NOT EXISTS workflows(
 id TEXT PRIMARY KEY,revision INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('active','stopped','completed')),
 registration BLOB NOT NULL,created_at TEXT NOT NULL,current_stage INTEGER NOT NULL,submitted_units INTEGER NOT NULL,
 reason TEXT NOT NULL DEFAULT '',adapter_attempts INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS active_workflows ON workflows(state,id);
CREATE TABLE IF NOT EXISTS workflow_stages(
 workflow_id TEXT NOT NULL REFERENCES workflows(id),ordinal INTEGER NOT NULL,run_id TEXT NOT NULL UNIQUE REFERENCES runs(id),
 decision_unit TEXT NOT NULL,decision_path TEXT NOT NULL,created_at TEXT NOT NULL,
 PRIMARY KEY(workflow_id,ordinal),FOREIGN KEY(run_id,decision_unit) REFERENCES run_units(run_id,unit_id));
CREATE TABLE IF NOT EXISTS workflow_intents(
 workflow_id TEXT NOT NULL,from_stage INTEGER NOT NULL,input BLOB NOT NULL,output BLOB NOT NULL,request_id TEXT NOT NULL UNIQUE,
 created_at TEXT NOT NULL,created_run TEXT REFERENCES runs(id),PRIMARY KEY(workflow_id,from_stage),
 FOREIGN KEY(workflow_id,from_stage) REFERENCES workflow_stages(workflow_id,ordinal));
`

func installWorkflows(tx *sql.Tx) error { _, err := tx.Exec(workflowSchema); return err }
func (c *Controller) InstallWorkflows(ctx context.Context) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = installWorkflows(tx); err != nil {
		return err
	}
	return tx.Commit()
}
func (c *Controller) RegisterWorkflow(ctx context.Context, r contracts.WorkflowRegistration, approval WorkflowAdapterConfig) (contracts.WorkflowStatus, error) {
	var out contracts.WorkflowStatus
	if err := r.Validate(); err != nil {
		return out, err
	}
	if r.Contract != approval.Contract || r.RuntimeDigest != approval.RuntimeDigest {
		return out, errors.New("workflow does not match approved adapter contract/runtime")
	}
	b, err := c.mutate(ctx, "operator", r.RequestID, r, func(tx *sql.Tx) (any, error) {
		found := false
		for _, u := range r.Initial.Units {
			if u.UnitID == r.DecisionUnit {
				found = true
			}
		}
		if !found {
			return nil, errors.New("initial decision unit missing")
		}
		if err := artifactExists(tx, r.Settings); err != nil {
			return nil, err
		}
		if _, err := c.submit(tx, r.Initial); err != nil {
			return nil, err
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := tx.Exec(`INSERT INTO workflows(id,revision,state,registration,created_at,current_stage,submitted_units) VALUES(?,1,'active',?,?,0,?)`, r.WorkflowID, encoded(r), now, len(r.Initial.Units)); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(`INSERT INTO workflow_stages VALUES(?,0,?,?,?,?)`, r.WorkflowID, r.Initial.RunID, r.DecisionUnit, r.DecisionPath, now); err != nil {
			return nil, err
		}
		return workflowStatus(tx, r.WorkflowID)
	})
	if err == nil {
		err = json.Unmarshal(b, &out)
	}
	return out, err
}
func workflowStatus(tx *sql.Tx, id string) (contracts.WorkflowStatus, error) {
	s := contracts.WorkflowStatus{SchemaVersion: 1, WorkflowID: id}
	var registration []byte
	var created string
	err := tx.QueryRow(`SELECT w.revision,w.state,w.current_stage,w.created_at,w.submitted_units,w.reason,w.registration,s.run_id FROM workflows w JOIN workflow_stages s ON s.workflow_id=w.id AND s.ordinal=w.current_stage WHERE w.id=?`, id).Scan(&s.Revision, &s.State, &s.Stage, &created, &s.SubmittedUnits, &s.Reason, &registration, &s.RunID)
	if err != nil {
		return s, err
	}
	var r contracts.WorkflowRegistration
	if err = json.Unmarshal(registration, &r); err != nil {
		return s, err
	}
	s.MaxSubmittedUnits = r.MaxSubmittedUnits
	s.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	s.Elapsed = time.Since(s.CreatedAt)
	return s, err
}
func (c *Controller) WorkflowStatus(ctx context.Context, id string) (contracts.WorkflowStatus, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return contracts.WorkflowStatus{}, err
	}
	defer tx.Rollback()
	return workflowStatus(tx, id)
}
func (c *Controller) ControlWorkflow(ctx context.Context, r contracts.WorkflowControl) (contracts.WorkflowStatus, error) {
	var out contracts.WorkflowStatus
	if err := r.Validate(); err != nil {
		return out, err
	}
	b, err := c.mutate(ctx, "operator", r.RequestID, r, func(tx *sql.Tx) (any, error) {
		s, err := workflowStatus(tx, r.WorkflowID)
		if err != nil {
			return nil, err
		}
		if s.Revision != r.ExpectedRevision || (s.State == "completed" && r.Action == "resume") {
			return nil, errors.New("stale or completed workflow control")
		}
		state := "active"
		if r.Action == "stop" {
			state = "stopped"
		}
		if s.State == "completed" {
			state = "completed"
		}
		if _, err = tx.Exec(`UPDATE workflows SET state=?,revision=revision+1,adapter_attempts=0,reason=CASE WHEN state='completed' THEN reason ELSE '' END WHERE id=?`, state, r.WorkflowID); err != nil {
			return nil, err
		}
		if r.Action == "stop" {
			rows, err := tx.Query(`SELECT s.run_id,COALESCE(t.revision,0) FROM workflow_stages s LEFT JOIN target_controls t ON t.kind='run' AND t.target=s.run_id WHERE s.workflow_id=? AND (s.ordinal=(SELECT current_stage FROM workflows WHERE id=s.workflow_id) OR EXISTS(SELECT 1 FROM assignments a WHERE a.run_id=s.run_id AND a.final IS NULL AND NOT EXISTS(SELECT 1 FROM terminal_evidence e WHERE e.assignment_id=a.id))) ORDER BY s.ordinal`, r.WorkflowID)
			if err != nil {
				return nil, err
			}
			type target struct {
				id  string
				rev int64
			}
			var targets []target
			for rows.Next() {
				var t target
				if err = rows.Scan(&t.id, &t.rev); err != nil {
					rows.Close()
					return nil, err
				}
				targets = append(targets, t)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return nil, err
			}
			for _, t := range targets {
				if _, err = c.control(tx, contracts.ControlRequest{SchemaVersion: 1, TargetKind: "run", TargetID: t.id, ExpectedRevision: t.rev, Action: "drain", TerminateAt: r.TerminateAt}); err != nil {
					return nil, err
				}
			}
		}
		// Resume permits future continuation only, never reopening run-level drains.
		return workflowStatus(tx, r.WorkflowID)
	})
	if err == nil {
		err = json.Unmarshal(b, &out)
	}
	return out, err
}

type workflowSnapshot struct {
	status       contracts.WorkflowStatus
	registration contracts.WorkflowRegistration
	input        contracts.WorkflowAdapterInput
	output       *contracts.WorkflowAdapterOutput
	requestID    string
}

func workflowSnapshotFor(tx *sql.Tx, id string) (workflowSnapshot, error) {
	var x workflowSnapshot
	var err error
	x.status, err = workflowStatus(tx, id)
	if err != nil {
		return x, err
	}
	var b []byte
	if err = tx.QueryRow(`SELECT registration FROM workflows WHERE id=?`, id).Scan(&b); err != nil {
		return x, err
	}
	if err = json.Unmarshal(b, &x.registration); err != nil {
		return x, err
	}
	var input, output []byte
	err = tx.QueryRow(`SELECT input,output,request_id FROM workflow_intents WHERE workflow_id=? AND from_stage=?`, id, x.status.Stage).Scan(&input, &output, &x.requestID)
	if err == nil {
		var o contracts.WorkflowAdapterOutput
		if err = json.Unmarshal(input, &x.input); err != nil {
			return x, err
		}
		err = json.Unmarshal(output, &o)
		x.output = &o
		return x, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return x, err
	}
	var pending int
	if err = tx.QueryRow(`SELECT count(*) FROM run_units u WHERE run_id=? AND NOT EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id=u.run_id AND a.unit_id=u.unit_id)`, x.status.RunID).Scan(&pending); err != nil {
		return x, err
	}
	if pending != 0 {
		return x, sql.ErrNoRows
	}
	var manifest, definition []byte
	var path, unit, candidate, validator string
	if err = tx.QueryRow(`SELECT s.decision_unit,s.decision_path,a.candidate,c.manifest,d.definition,a.validator FROM workflow_stages s JOIN acceptances a ON a.run_id=s.run_id AND a.unit_id=s.decision_unit JOIN candidates c ON c.digest=a.candidate JOIN definitions d ON d.unit_id=s.decision_unit WHERE s.workflow_id=? AND s.ordinal=?`, id, x.status.Stage).Scan(&unit, &path, &candidate, &manifest, &definition, &validator); err != nil {
		return x, err
	}
	var v contracts.Candidate
	if err = json.Unmarshal(manifest, &v); err != nil {
		return x, err
	}
	var pinned contracts.Unit
	if err = json.Unmarshal(definition, &pinned); err != nil {
		return x, err
	}
	if err = v.Validate(); err != nil {
		return x, err
	}
	if v.UnitID != unit || v.SpecDigest != pinned.SpecDigest || validator != pinned.Validator {
		return x, errors.New("canonical decision identity/validator mismatch")
	}
	x.input = contracts.WorkflowAdapterInput{SchemaVersion: 1, WorkflowID: id, Stage: x.status.Stage, Contract: x.registration.Contract, RuntimeDigest: x.registration.RuntimeDigest, CandidateDigest: candidate, Settings: x.registration.Settings, DecisionFile: "decision.json", SettingsFile: "settings.json"}
	found := false
	for _, f := range v.Files {
		if f.Path == path {
			x.input.Decision = f.Artifact
			found = true
		}
	}
	if !found {
		return x, errors.New("canonical decision file missing")
	}
	if err = artifactExists(tx, x.input.Decision); err != nil {
		return x, err
	}
	if err = artifactExists(tx, x.input.Settings); err != nil {
		return x, err
	}
	err = x.input.Validate()
	return x, err
}
func persistWorkflowIntent(tx *sql.Tx, x workflowSnapshot, o contracts.WorkflowAdapterOutput) error {
	if err := o.Validate(); err != nil {
		return err
	}
	s, err := workflowStatus(tx, x.status.WorkflowID)
	if err != nil {
		return err
	}
	if s.State != "active" || s.Revision != x.status.Revision || s.Stage != x.status.Stage {
		return errors.New("workflow changed while adapter ran")
	}
	// Canonical identity must remain pinned, not a caller-selected candidate.
	current, err := workflowSnapshotFor(tx, s.WorkflowID)
	if err != nil {
		return err
	}
	if string(encoded(current.input)) != string(encoded(x.input)) {
		return errors.New("workflow decision changed")
	}
	request := "workflow-next-" + contracts.Hash([]byte(fmt.Sprintf("%s:%d", s.WorkflowID, s.Stage)))
	_, err = tx.Exec(`INSERT INTO workflow_intents(workflow_id,from_stage,input,output,request_id,created_at) VALUES(?,?,?,?,?,?)`, s.WorkflowID, s.Stage, encoded(x.input), encoded(o), request, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
func (c *Controller) createWorkflowSuccessor(ctx context.Context, x workflowSnapshot) error {
	_, err := c.mutate(ctx, "workflow:"+x.status.WorkflowID, x.requestID, *x.output, func(tx *sql.Tx) (any, error) {
		s, err := workflowStatus(tx, x.status.WorkflowID)
		if err != nil {
			return nil, err
		}
		if s.State != "active" || s.Revision != x.status.Revision || s.Stage != x.status.Stage {
			return nil, errors.New("workflow changed before successor creation")
		}
		if x.output.Next == nil {
			if _, err = tx.Exec(`UPDATE workflows SET state='completed',revision=revision+1,reason=? WHERE id=?`, x.output.StopReason, s.WorkflowID); err != nil {
				return nil, err
			}
			return workflowStatus(tx, s.WorkflowID)
		}
		n := x.output.Next
		if s.MaxSubmittedUnits != nil && int64(len(n.Submission.Units)) > *s.MaxSubmittedUnits-s.SubmittedUnits {
			return nil, errors.New("workflow submitted-unit allowance exhausted")
		}
		for _, u := range n.Submission.Units {
			var old int
			if err = tx.QueryRow(`SELECT count(*) FROM workflow_stages s JOIN run_units u ON u.run_id=s.run_id WHERE s.workflow_id=? AND u.unit_id=?`, s.WorkflowID, u.UnitID).Scan(&old); err != nil {
				return nil, err
			}
			if old != 0 {
				return nil, errors.New("successor must not resubmit earlier workflow units")
			}
		}
		if _, err = c.submit(tx, n.Submission); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(`INSERT INTO workflow_stages VALUES(?,?,?,?,?,?)`, s.WorkflowID, s.Stage+1, n.Submission.RunID, n.DecisionUnit, n.DecisionPath, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(`UPDATE workflow_intents SET created_run=? WHERE workflow_id=? AND from_stage=?`, n.Submission.RunID, s.WorkflowID, s.Stage); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(`UPDATE workflows SET current_stage=current_stage+1,revision=revision+1,submitted_units=submitted_units+?,adapter_attempts=0,reason='' WHERE id=?`, len(n.Submission.Units), s.WorkflowID); err != nil {
			return nil, err
		}
		return workflowStatus(tx, s.WorkflowID)
	})
	return err
}
