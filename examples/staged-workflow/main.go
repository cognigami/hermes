// staged-workflow is a pure synthetic leaf adapter, not a scheduler. The
// application-owned settings prescribe a graph; an accepted boolean decision
// selects that graph or a prescribed stop. It never submits anything itself.
package main

import (
	"encoding/json"
	"fmt"
	"hermes/internal/contracts"
	"io"
	"os"
)

type decision struct {
	Continue *bool `json:"continue"`
}
type settings struct {
	Next *contracts.WorkflowNext `json:"next,omitempty"`
}

func evaluate(stdin io.Reader, stdout io.Writer) error {
	var input contracts.WorkflowAdapterInput
	if err := contracts.Decode(stdin, &input); err != nil {
		return err
	}
	if err := input.Validate(); err != nil {
		return err
	}
	read := func(path string, v any) error {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		return contracts.Decode(f, v)
	}
	var d decision
	var s settings
	if err := read(input.DecisionFile, &d); err != nil {
		return err
	}
	if err := read(input.SettingsFile, &s); err != nil {
		return err
	}
	output := contracts.WorkflowAdapterOutput{SchemaVersion: 1, StopReason: "synthetic decision prescribed stop"}
	if d.Continue == nil {
		return fmt.Errorf("missing synthetic decision")
	}
	if *d.Continue {
		output.StopReason = ""
		output.Next = s.Next
	}
	if err := output.Validate(); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(output)
}
func main() {
	if err := evaluate(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
