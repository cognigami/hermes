package controller

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"hermes/internal/contracts"
)

// serveWorkflowHTTP is called only after the controller's existing mTLS
// authentication. principal and approval must come from that server, never the
// request body. This helper does not install schema or start adapter execution.
func (c *Controller) serveWorkflowHTTP(w http.ResponseWriter, r *http.Request, principal string, approval *WorkflowAdapterConfig) bool {
	if r.URL.Path != "/v1/workflows" && !strings.HasPrefix(r.URL.Path, "/v1/workflows/") {
		return false
	}
	if principal != "operator" {
		http.Error(w, "operator required", http.StatusForbidden)
		return true
	}
	method := ""
	switch r.URL.Path {
	case "/v1/workflows", "/v1/workflows/control":
		method = http.MethodPost
	case "/v1/workflows/status":
		method = http.MethodGet
	default:
		http.NotFound(w, r)
		return true
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	if r.URL.Path == "/v1/workflows" && approval == nil {
		http.Error(w, "workflow adapter unavailable: no server approval", http.StatusServiceUnavailable)
		return true
	}
	var result contracts.WorkflowStatus
	var err error
	if method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, contracts.MaxMetadataBytes)
	}
	switch r.URL.Path {
	case "/v1/workflows":
		var registration contracts.WorkflowRegistration
		if err = contracts.Decode(r.Body, &registration); err == nil {
			result, err = c.RegisterWorkflow(r.Context(), registration, *approval)
		}
	case "/v1/workflows/control":
		var control contracts.WorkflowControl
		if err = contracts.Decode(r.Body, &control); err == nil {
			result, err = c.ControlWorkflow(r.Context(), control)
		}
	case "/v1/workflows/status":
		id := r.URL.Query().Get("workflow")
		if id == "" || len(id) > 256 || strings.ContainsAny(id, "\x00\r\n") {
			err = errors.New("workflow ID required")
		} else {
			result, err = c.WorkflowStatus(r.Context(), id)
		}
		// Explain unavailable continuation only in the response. Preserve recorded
		// operational reasons and completed application stop reasons unchanged.
		if err == nil && approval == nil && result.State == "active" && result.Reason == "" {
			result.Reason = "workflow adapter unavailable: no server approval"
		}
	}
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, sql.ErrNoRows) {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
	return true
}
