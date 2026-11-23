package contracts_test

import (
	"bytes"
	"context"
	"encoding/json"
	"hermes/internal/contracts"
	"hermes/testdata/synthetic"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExecutableFixture(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	for _, name := range []string{"hermes-synthetic", "hermes-synthetic-validator"} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(bin, name), "./cmd/"+name)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("fixture build: %v %s", e, out)
		}
	}
	for _, failure := range []string{"", "external_dependency", "reject", "validator_error", "exit_prepare", "exit_unit"} {
		t.Run(failure, func(t *testing.T) {
			var cfg synthetic.Manifest
			if e := synthetic.Load(filepath.Join(root, "testdata/synthetic/manifest.json"), &cfg); e != nil {
				t.Fatal(e)
			}
			if failure == "external_dependency" {
				// The controller has accepted this prerequisite in an earlier assignment.
				cfg.Units[0].Dependencies = []string{"previously-accepted-unit"}
			} else {
				second := cfg.Units[0]
				second.UnitID = "unit-b"
				cfg.Units = append(cfg.Units, second)
				cfg.Assignment.Units = append(cfg.Assignment.Units, contracts.UnitRef{UnitID: second.UnitID, SpecDigest: second.SpecDigest})
				cfg.Failure = failure
			}
			dir := t.TempDir()
			data, _ := json.Marshal(cfg)
			manifest := filepath.Join(dir, "input.json")
			if e := os.WriteFile(manifest, data, 0600); e != nil {
				t.Fatal(e)
			}
			output := filepath.Join(dir, "output")
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, filepath.Join(bin, "hermes-synthetic"), "--manifest", manifest, "--output", output)
			in, e := cmd.StdinPipe()
			if e != nil {
				t.Fatal(e)
			}
			out, e := cmd.StdoutPipe()
			if e != nil {
				t.Fatal(e)
			}
			var logs bytes.Buffer
			cmd.Stderr = &logs
			if e = cmd.Start(); e != nil {
				t.Fatal(e)
			}
			defer func() {
				in.Close()
				if cmd.ProcessState == nil {
					cmd.Process.Kill()
					cmd.Wait()
				}
			}()
			reader := contracts.MessageReader(out)
			ready, e := contracts.ReadMessage(reader)
			if failure == "exit_prepare" {
				if e == nil {
					t.Fatal("preparation failure emitted ready")
				}
				if cmd.Wait() == nil {
					t.Fatal("expected operational exit")
				}
				return
			}
			if e != nil || ready.Type != "ready" {
				t.Fatal(ready, e)
			}
			entries, e := os.ReadDir(output)
			if e != nil || len(entries) != 0 {
				t.Fatal("autonomous work before grant", e)
			}
			for _, u := range cfg.Units {
				if e = contracts.WriteMessage(in, contracts.Message{SchemaVersion: 1, Type: "execute_unit", UnitID: u.UnitID}); e != nil {
					t.Fatal(e)
				}
				result, e := contracts.ReadMessage(reader)
				if failure == "exit_unit" {
					if e == nil {
						t.Fatal("unit failure emitted result")
					}
					if cmd.Wait() == nil {
						t.Fatal("expected operational exit")
					}
					return
				}
				if e != nil || result.Type != "unit_result" || result.UnitID != u.UnitID {
					t.Fatal(result, e)
				}
				unitFile := filepath.Join(dir, "unit.json")
				data, _ := json.Marshal(u)
				if e = os.WriteFile(unitFile, data, 0600); e != nil {
					t.Fatal(e)
				}
				validator := exec.CommandContext(ctx, filepath.Join(bin, "hermes-synthetic-validator"), "--manifest", filepath.Join(output, result.ManifestPath), "--root", output, "--unit", unitFile)
				var verdictLog bytes.Buffer
				validator.Stderr = &verdictLog
				b, e := validator.Output()
				if failure == "validator_error" {
					if e == nil || len(b) != 0 {
						t.Fatal("operational failure became domain verdict")
					}
				} else {
					if e != nil {
						t.Fatal(e, verdictLog.String())
					}
					var v contracts.Verdict
					if e = contracts.Decode(bytes.NewReader(b), &v); e != nil {
						t.Fatal(e)
					}
					if e = v.Validate(); e != nil {
						t.Fatal(e)
					}
					if v.Valid != (failure == "" || failure == "external_dependency") {
						t.Fatal(v)
					}
				}
				if e = contracts.WriteMessage(in, contracts.Message{SchemaVersion: 1, Type: "published", UnitID: u.UnitID}); e != nil {
					t.Fatal(e)
				}
			}
			if e = contracts.WriteMessage(in, contracts.Message{SchemaVersion: 1, Type: "finish"}); e != nil {
				t.Fatal(e)
			}
			finished, e := contracts.ReadMessage(reader)
			if e != nil || finished.Type != "finished" {
				t.Fatal(finished, e)
			}
			if e = cmd.Wait(); e != nil {
				t.Fatal(e, logs.String())
			}
			if strings.Count(logs.String(), "prepared once") != 1 {
				t.Fatal("preparation repeated")
			}
		})
	}
}
