package main

import (
	"encoding/json"
	"flag"
	"hermes/internal/controller"
	"io"
)

// runQuiescenceInspection is dispatched before normal service flag/config IO.
func runQuiescenceInspection(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("check-quiescence", flag.ContinueOnError)
	root := flags.String("root", "", "absolute controller state root")
	if e := flags.Parse(args); e != nil {
		return e
	}
	result, e := controller.CheckLocalQuiescence(*root)
	if outputErr := json.NewEncoder(stdout).Encode(result); outputErr != nil {
		return outputErr
	}
	return e
}
