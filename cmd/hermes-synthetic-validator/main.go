package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hermes/internal/contracts"
	"hermes/testdata/synthetic"
	"os"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	manifest := flag.String("manifest", "", "candidate manifest")
	root := flag.String("root", "", "private result directory")
	// The pinned unit is required: self-consistency of an arbitrary result is not validation.
	unit := flag.String("unit", "", "pinned unit manifest")
	flag.Parse()
	var c contracts.Candidate
	if e := synthetic.Load(*manifest, &c); e != nil {
		return e
	}
	if e := c.Validate(); e != nil {
		return e
	}
	var u contracts.Unit
	if e := synthetic.Load(*unit, &u); e != nil {
		return e
	}
	if e := u.Validate(); e != nil {
		return e
	}
	if c.Outcome == "validator_error" {
		return errors.New("controlled validator operational failure")
	}
	valid := c.UnitID == u.UnitID && c.SpecDigest == u.SpecDigest && c.Outcome == "complete" && len(c.Files) == 1
	for _, f := range c.Files {
		b, e := synthetic.ReadFile(*root, f.Path)
		if e != nil {
			return e
		}
		if contracts.Hash(b) != f.Artifact.Digest || int64(len(b)) != f.Artifact.Size {
			return errors.New("unverified artifact")
		}
		var r synthetic.Result
		if e = contracts.Decode(bytes.NewReader(b), &r); e != nil {
			return e
		}
		valid = valid && r.UnitID == u.UnitID && r.SpecDigest == u.SpecDigest && r.Value == synthetic.Expected(u.UnitID, u.SpecDigest)
	}
	reason := "synthetic result matches pinned unit"
	if !valid {
		reason = "synthetic domain mismatch"
	}
	return json.NewEncoder(os.Stdout).Encode(contracts.Verdict{SchemaVersion: 1, Valid: valid, Reason: reason})
}
