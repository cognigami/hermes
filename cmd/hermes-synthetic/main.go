package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hermes/internal/contracts"
	"hermes/testdata/synthetic"
	"os"
	"path/filepath"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	manifest := flag.String("manifest", "", "local fixture manifest")
	output := flag.String("output", "", "private output directory")
	delay := flag.Int("delay-millis", -1, "override per-unit synthetic delay")
	preparationDelay := flag.Int("prepare-delay-millis", 0, "synthetic preparation delay")
	flag.Parse()
	var cfg synthetic.Manifest
	if e := synthetic.Load(*manifest, &cfg); e != nil {
		return e
	}
	if *delay >= 0 {
		cfg.DelayMillis = *delay
	}
	if *preparationDelay < 0 || *preparationDelay > 60000 {
		return errors.New("invalid preparation delay")
	}
	if e := cfg.Validate(); e != nil {
		return e
	}
	time.Sleep(time.Duration(*preparationDelay) * time.Millisecond)
	if cfg.Failure == "exit_prepare" {
		return errors.New("controlled preparation failure")
	}
	if e := os.MkdirAll(*output, 0700); e != nil {
		return e
	}
	fmt.Fprintln(os.Stderr, "prepared once")
	send := func(t, u, p string) error {
		return contracts.WriteMessage(os.Stdout, contracts.Message{SchemaVersion: 1, Type: t, UnitID: u, ManifestPath: p})
	}
	if e := send("ready", "", ""); e != nil {
		return e
	}
	reader := contracts.MessageReader(os.Stdin)
	done := map[string]bool{}
	pending := ""
	for {
		m, e := contracts.ReadMessage(reader)
		if e != nil {
			return e
		}
		switch m.Type {
		case "finish":
			if pending != "" {
				return errors.New("finish before local publication")
			}
			return send("finished", "", "")
		case "published":
			if pending == "" || pending != m.UnitID {
				return errors.New("unexpected publication acknowledgment")
			}
			done[pending] = true
			pending = ""
		case "execute_unit":
			if pending != "" || done[m.UnitID] {
				return errors.New("unit already running or completed")
			}
			var u *contracts.Unit
			for i := range cfg.Units {
				if cfg.Units[i].UnitID == m.UnitID {
					u = &cfg.Units[i]
					break
				}
			}
			if u == nil {
				return errors.New("undeclared unit")
			}
			if cfg.Failure == "exit_unit" {
				return errors.New("controlled unit failure")
			}
			time.Sleep(time.Duration(cfg.DelayMillis) * time.Millisecond)
			result := synthetic.Result{UnitID: u.UnitID, SpecDigest: u.SpecDigest, Value: synthetic.Expected(u.UnitID, u.SpecDigest)}
			if cfg.Failure == "reject" {
				result.Value = "incorrect"
			}
			bytes, e := json.Marshal(result)
			if e != nil {
				return e
			}
			name := contracts.Hash([]byte(u.UnitID)) + ".json"
			if e = os.WriteFile(filepath.Join(*output, name), bytes, 0600); e != nil {
				return e
			}
			outcome := "complete"
			if cfg.Failure == "validator_error" {
				outcome = "validator_error"
			}
			c := contracts.Candidate{SchemaVersion: 1, UnitID: u.UnitID, SpecDigest: u.SpecDigest, AssignmentID: cfg.Assignment.AssignmentID, AttemptID: cfg.Assignment.AssignmentID, Outcome: outcome, Files: []contracts.ResultFile{{Path: name, Artifact: contracts.Artifact{Digest: contracts.Hash(bytes), Size: int64(len(bytes)), MediaType: "application/json"}}}}
			bytes, e = json.Marshal(c)
			if e != nil {
				return e
			}
			name = contracts.Hash([]byte(u.UnitID)) + ".manifest.json"
			if e = os.WriteFile(filepath.Join(*output, name), bytes, 0600); e != nil {
				return e
			}
			pending = u.UnitID
			if e = send("unit_result", u.UnitID, name); e != nil {
				return e
			}
		default:
			return errors.New("unexpected agent message")
		}
	}
}
