package controller

import (
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"net/http"
	"strconv"
	"strings"
)

// Runtime approval is deliberately NOT an HTTP operation. These operator routes
// consume already configured deployment bindings and existing sealed authority.
func (c *Controller) finiteRoute(w http.ResponseWriter, r *http.Request, operator bool) bool {
	if !strings.HasPrefix(r.URL.Path, "/v1/finite/") {
		return false
	}
	if !operator {
		http.Error(w, "operator required", http.StatusForbidden)
		return true
	}
	var result any
	var e error
	switch {
	case r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/v1/finite/artifacts/"):
		digest := strings.TrimPrefix(r.URL.Path, "/v1/finite/artifacts/")
		var size int64
		size, e = strconv.ParseInt(r.Header.Get("X-Artifact-Size"), 10, 64)
		if e != nil {
			e = errors.Join(ErrFiniteInvalid, e)
		} else {
			e = c.PublishFiniteArtifact(r.Context(), r.URL.Query().Get("bundle"), r.URL.Query().Get("member"), r.URL.Query().Get("assignment"), digest, size, r.Body)
		}
		result = contracts.Acknowledgment{SchemaVersion: contracts.Version, Published: e == nil}
	case r.Method == "POST" && r.URL.Path == "/v1/finite/export":
		r.Body = http.MaxBytesReader(w, r.Body, contracts.MaxMetadataBytes)
		var request contracts.FiniteExportRequest
		if e = contracts.Decode(r.Body, &request); e == nil {
			result, e = c.ExportFinite(r.Context(), request)
		} else {
			e = errors.Join(ErrFiniteInvalid, e)
		}
	case r.Method == "GET" && r.URL.Path == "/v1/finite/bundle":
		result, e = c.FiniteBundle(r.Context(), r.URL.Query().Get("id"))
	case r.Method == "GET" && r.URL.Path == "/v1/finite/ack":
		result, e = c.FiniteAcknowledgment(r.Context(), r.URL.Query().Get("bundle"))
	case r.Method == "POST" && r.URL.Path == "/v1/finite/candidate":
		r.Body = http.MaxBytesReader(w, r.Body, contracts.MaxMetadataBytes)
		var report contracts.CandidateReport
		if e = contracts.Decode(r.Body, &report); e == nil {
			result, e = c.ImportFiniteCandidate(r.Context(), r.URL.Query().Get("bundle"), report)
		} else {
			e = errors.Join(ErrFiniteInvalid, e)
		}
	case r.Method == "POST" && r.URL.Path == "/v1/finite/final":
		r.Body = http.MaxBytesReader(w, r.Body, contracts.MaxMetadataBytes)
		var inventory contracts.FiniteFinalInventory
		if e = contracts.Decode(r.Body, &inventory); e == nil {
			result, e = c.ImportFiniteFinal(r.Context(), inventory)
		} else {
			e = errors.Join(ErrFiniteInvalid, e)
		}
	default:
		http.Error(w, "not found", http.StatusNotFound)
		return true
	}
	if e != nil {
		status := http.StatusInternalServerError
		if errors.Is(e, ErrFiniteInvalid) || errors.Is(e, ErrInvalidArtifact) {
			status = http.StatusBadRequest
		} else if errors.Is(e, ErrControllerCapacity) || errors.Is(e, ErrFiniteUnavailable) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, e.Error(), status)
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
	return true
}
