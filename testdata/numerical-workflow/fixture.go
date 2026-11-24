// Package numericalworkflow is an application-owned supplied-value fixture.
// It accumulates small integer sums of squares; it owns no scheduling authority.
package numericalworkflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
	"hermes/internal/contracts"
)

const Contract = "numerical-squares-v1"

type Plan struct {
	SchemaVersion int     `json:"schema_version"`
	Stage         int     `json:"stage"`
	Stages        int     `json:"stages"`
	Values        []int64 `json:"values"`
}
type Result struct {
	SchemaVersion int   `json:"schema_version"`
	Stage         int   `json:"stage"`
	Value         int64 `json:"value"`
	Continue      bool  `json:"continue"`
}
type Settings struct {
	WorkflowID string               `json:"workflow_id"`
	Plans      []contracts.Artifact `json:"plans"`
}

func Encode(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
func Artifact(b []byte) contracts.Artifact {
	return contracts.Artifact{Digest: contracts.Hash(b), Size: int64(len(b)), MediaType: "application/json"}
}
func Load(root, name string) ([]byte, error) {
	if !contracts.ValidPath(name) {
		return nil, errors.New("invalid fixture path")
	}
	fs, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	defer fs.Close()
	f, err := fs.OpenFile(name, os.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > contracts.MaxMetadataBytes {
		return nil, errors.New("fixture input not bounded regular metadata")
	}
	return io.ReadAll(io.LimitReader(f, contracts.MaxMetadataBytes+1))
}
func decode(b []byte, v any) error { return contracts.Decode(bytes.NewReader(b), v) }
func unitSpec(inputs []contracts.Artifact) string {
	d, _ := contracts.Digest(struct {
		Operation string
		Inputs    []contracts.Artifact
	}{Contract, inputs})
	return d
}
func Stage(s Settings, stage int, prior *contracts.Artifact) (contracts.WorkflowNext, error) {
	var n contracts.WorkflowNext
	if stage < 0 || stage >= len(s.Plans) || len(s.Plans) != 3 || s.WorkflowID == "" || (stage == 0) != (prior == nil) {
		return n, errors.New("invalid numerical stage")
	}
	inputs := []contracts.Artifact{s.Plans[stage]}
	if prior != nil {
		inputs = append(inputs, *prior)
	}
	id := fmt.Sprintf("%s-stage-%d", s.WorkflowID, stage)
	u := contracts.Unit{SchemaVersion: 1, UnitID: id + "-unit", SpecDigest: unitSpec(inputs), Inputs: inputs, Operation: Contract, Runtime: Contract, Validator: Contract}
	n = contracts.WorkflowNext{Submission: contracts.Submission{SchemaVersion: 1, RequestID: id + "-submit", RunID: id, Units: []contracts.Unit{u}, Packages: []contracts.Package{{SchemaVersion: 1, PackageID: id + "-package", Units: []contracts.UnitRef{{UnitID: u.UnitID, SpecDigest: u.SpecDigest}}}}}, DecisionUnit: u.UnitID, DecisionPath: "decision.json"}
	return n, n.Validate()
}
func Calculate(p Plan, prior *Result) (Result, error) {
	var r Result
	if p.SchemaVersion != 1 || p.Stages != 3 || p.Stage < 0 || p.Stage >= p.Stages || len(p.Values) == 0 || len(p.Values) > 16 || (p.Stage == 0) != (prior == nil) {
		return r, errors.New("invalid supplied numerical plan")
	}
	value := int64(0)
	if prior != nil {
		if prior.SchemaVersion != 1 || prior.Stage != p.Stage-1 || !prior.Continue || prior.Value < 0 {
			return r, errors.New("invalid prior accepted result")
		}
		value = prior.Value
	}
	for _, x := range p.Values {
		if x < -1000000 || x > 1000000 {
			return r, errors.New("fixture value outside small integer range")
		}
		square := x * x
		if value > math.MaxInt64-square {
			return r, errors.New("numerical sum overflow")
		}
		value += square
	}
	return Result{SchemaVersion: 1, Stage: p.Stage, Value: value, Continue: p.Stage+1 < p.Stages}, nil
}
func evaluateFiles(u contracts.Unit, files map[string][]byte) (Result, error) {
	var zero Result
	if u.Operation != Contract || u.Runtime != Contract || u.Validator != Contract || len(u.Inputs) < 1 || len(u.Inputs) > 2 || u.SpecDigest != unitSpec(u.Inputs) {
		return zero, errors.New("pinned numerical specification mismatch")
	}
	names := []string{"plan.json", "previous.json"}
	for i, a := range u.Inputs {
		b, ok := files[names[i]]
		if !ok || a.Digest != contracts.Hash(b) || a.Size != int64(len(b)) {
			return zero, errors.New("numerical evidence does not match pinned input")
		}
	}
	var p Plan
	if err := decode(files["plan.json"], &p); err != nil {
		return zero, err
	}
	var prior *Result
	if len(u.Inputs) == 2 {
		prior = &Result{}
		if err := decode(files["previous.json"], prior); err != nil {
			return zero, err
		}
	}
	return Calculate(p, prior)
}
func Validate(root string, u contracts.Unit, c contracts.Candidate) (contracts.Verdict, error) {
	if err := c.Validate(); err != nil {
		return contracts.Verdict{}, err
	}
	if c.UnitID != u.UnitID || c.SpecDigest != u.SpecDigest {
		return contracts.Verdict{}, errors.New("candidate outside pinned numerical unit")
	}
	files := map[string][]byte{}
	for _, f := range c.Files {
		b, err := Load(root, f.Path)
		if err != nil {
			return contracts.Verdict{}, err
		}
		if f.Artifact.Digest != contracts.Hash(b) || f.Artifact.Size != int64(len(b)) {
			return contracts.Verdict{}, errors.New("candidate bytes mismatch")
		}
		files[f.Path] = b
	}
	expected, err := evaluateFiles(u, files)
	if err != nil {
		return contracts.Verdict{}, err
	}
	var actual Result
	if err = decode(files["decision.json"], &actual); err != nil {
		return contracts.Verdict{}, err
	}
	if actual != expected {
		return contracts.Verdict{SchemaVersion: 1, Valid: false, Reason: "incorrect numerical value or continuation decision"}, nil
	}
	return contracts.Verdict{SchemaVersion: 1, Valid: true, Reason: "checked supplied sum of squares and prior accepted reference"}, nil
}
func Runtime(stdin io.Reader, stdout io.Writer, manifest, output string) error {
	body, err := Load(filepath.Dir(manifest), filepath.Base(manifest))
	if err != nil {
		return err
	}
	var local struct {
		SchemaVersion int                  `json:"schema_version"`
		Assignment    contracts.Assignment `json:"assignment"`
		Units         []contracts.Unit     `json:"units"`
	}
	if err = decode(body, &local); err != nil {
		return err
	}
	if len(local.Units) != 1 || local.SchemaVersion != 1 {
		return errors.New("fixture requires one unit")
	}
	u := local.Units[0]
	a := local.Assignment
	if err = a.Validate(); err != nil {
		return err
	}
	send := func(kind, path string) error {
		id := ""
		if kind == "unit_result" {
			id = u.UnitID
		}
		return contracts.WriteMessage(stdout, contracts.Message{SchemaVersion: 1, Type: kind, UnitID: id, ManifestPath: path})
	}
	if err = send("ready", ""); err != nil {
		return err
	}
	reader := contracts.MessageReader(stdin)
	executed, pending := false, false
	for {
		m, err := contracts.ReadMessage(reader)
		if err != nil {
			return err
		}
		switch m.Type {
		case "finish":
			if pending {
				return errors.New("finish before durable publication")
			}
			return send("finished", "")
		case "published":
			if !pending || m.UnitID != u.UnitID {
				return errors.New("unexpected publication acknowledgment")
			}
			pending = false
		case "execute_unit":
			if executed || m.UnitID != u.UnitID {
				return errors.New("unexpected numerical grant")
			}
			executed = true
			files := map[string][]byte{}
			names := []string{"plan.json", "previous.json"}
			for i, ref := range u.Inputs {
				if i >= len(names) {
					return errors.New("too many numerical inputs")
				}
				b, err := Load(".", "inputs/"+ref.Digest)
				if err != nil {
					return err
				}
				files[names[i]] = b
			}
			result, err := evaluateFiles(u, files)
			if err != nil {
				return err
			}
			files["decision.json"] = Encode(result)
			c := contracts.Candidate{SchemaVersion: 1, UnitID: u.UnitID, SpecDigest: u.SpecDigest, AssignmentID: a.AssignmentID, AttemptID: a.AssignmentID, Outcome: "calculated"}
			for _, name := range []string{"plan.json", "previous.json", "decision.json"} {
				b, ok := files[name]
				if !ok {
					continue
				}
				if err = os.WriteFile(filepath.Join(output, name), b, 0600); err != nil {
					return err
				}
				c.Files = append(c.Files, contracts.ResultFile{Path: name, Artifact: Artifact(b)})
			}
			if err = os.WriteFile(filepath.Join(output, "candidate.json"), Encode(c), 0600); err != nil {
				return err
			}
			pending = true
			if err = send("unit_result", "candidate.json"); err != nil {
				return err
			}
		default:
			return errors.New("unexpected numerical protocol message")
		}
	}
}
func Decide(stdin io.Reader, stdout io.Writer) error {
	var input contracts.WorkflowAdapterInput
	if err := contracts.Decode(stdin, &input); err != nil {
		return err
	}
	if err := input.Validate(); err != nil {
		return err
	}
	b, err := Load(".", input.DecisionFile)
	if err != nil {
		return err
	}
	var result Result
	if err = decode(b, &result); err != nil {
		return err
	}
	b, err = Load(".", input.SettingsFile)
	if err != nil {
		return err
	}
	var s Settings
	if err = decode(b, &s); err != nil {
		return err
	}
	if input.Contract != Contract || s.WorkflowID != input.WorkflowID || result.Stage != int(input.Stage) || result.SchemaVersion != 1 {
		return errors.New("numerical decision contract mismatch")
	}
	out := contracts.WorkflowAdapterOutput{SchemaVersion: 1}
	if result.Continue {
		next, err := Stage(s, result.Stage+1, &input.Decision)
		if err != nil {
			return err
		}
		out.Next = &next
	} else {
		if result.Stage != 2 {
			return errors.New("premature numerical stop")
		}
		out.StopReason = fmt.Sprintf("sum of squares complete: %d", result.Value)
	}
	if err = out.Validate(); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(out)
}
