package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"hermes/internal/agent"
	"hermes/internal/contracts"
)

// Workflow commands only submit application metadata. Executable approval stays
// in controller-local configuration, never in these requests.
func runWorkflowCommand(ctx context.Context, client *agent.Client, args []string, out io.Writer) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}
	switch args[0] {
	case "workflow-register", "workflow-control", "workflow-status":
	default:
		return false, nil
	}
	if len(args) != 2 {
		return true, fmt.Errorf("%s requires one file or workflow ID", args[0])
	}
	var status contracts.WorkflowStatus
	var id string
	switch args[0] {
	case "workflow-register":
		var request contracts.WorkflowRegistration
		if err := readWorkflowRequest(args[1], &request); err != nil {
			return true, err
		}
		if err := request.Validate(); err != nil {
			return true, err
		}
		id = request.WorkflowID
		if err := client.Do(ctx, http.MethodPost, "/v1/workflows", request, &status); err != nil {
			return true, err
		}
	case "workflow-control":
		var request contracts.WorkflowControl
		if err := readWorkflowRequest(args[1], &request); err != nil {
			return true, err
		}
		if err := request.Validate(); err != nil {
			return true, err
		}
		id = request.WorkflowID
		if err := client.Do(ctx, http.MethodPost, "/v1/workflows/control", request, &status); err != nil {
			return true, err
		}
	case "workflow-status":
		id = args[1]
		if id == "" {
			return true, fmt.Errorf("workflow ID must not be empty")
		}
		if err := client.Do(ctx, http.MethodGet, "/v1/workflows/status?workflow="+url.QueryEscape(id), nil, &status); err != nil {
			return true, err
		}
	}
	if status.SchemaVersion != 1 || status.WorkflowID != id || status.Revision < 1 {
		return true, fmt.Errorf("%w: incompatible workflow status or identity", agent.ErrProtocol)
	}
	return true, json.NewEncoder(out).Encode(status)
}

func readWorkflowRequest(path string, dst any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return contracts.Decode(f, dst)
}
