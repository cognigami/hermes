package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"hermes/internal/contracts"
	numerical "hermes/testdata/numerical-workflow"
)

func run() error {
	if len(os.Args) < 2 {
		return errors.New("numerical fixture mode required")
	}
	if os.Args[1] == "decide" {
		return numerical.Decide(os.Stdin, os.Stdout)
	}
	f := flag.NewFlagSet(os.Args[1], flag.ContinueOnError)
	manifest := f.String("manifest", "", "pinned manifest")
	root := f.String("root", "", "result root")
	unit := f.String("unit", "", "pinned unit")
	output := f.String("output", "", "private output directory")
	if err := f.Parse(os.Args[2:]); err != nil {
		return err
	}
	switch os.Args[1] {
	case "runtime":
		return numerical.Runtime(os.Stdin, os.Stdout, *manifest, *output)
	case "validate":
		read := func(path string, dst any) error {
			b, err := numerical.Load(filepath.Dir(path), filepath.Base(path))
			if err != nil {
				return err
			}
			return contracts.Decode(bytes.NewReader(b), dst)
		}
		var c contracts.Candidate
		var u contracts.Unit
		if err := read(*manifest, &c); err != nil {
			return err
		}
		if err := read(*unit, &u); err != nil {
			return err
		}
		verdict, err := numerical.Validate(*root, u, c)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(verdict)
	default:
		return errors.New("unknown numerical fixture mode")
	}
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
