package controller

import (
	"encoding/hex"
	"errors"
	"io"
	"math"
	"path/filepath"
	"strings"
	"time"

	"hermes/internal/contracts"
)

// LoadWorkflowAdapterConfig reads local server configuration, never an HTTP
// registration body. Loading validates metadata only; executable verification
// remains at the RunWorkflows boundary.
func LoadWorkflowAdapterConfig(r io.Reader) (WorkflowAdapterConfig, error) {
	var input struct {
		SchemaVersion    int       `json:"schema_version"`
		Contract         string    `json:"contract"`
		RuntimeDigest    string    `json:"runtime_digest"`
		Argv             []*string `json:"argv"`
		TimeoutMillis    int64     `json:"timeout_millis"`
		MaxMetadataBytes int       `json:"max_metadata_bytes"`
		MaxLogBytes      int       `json:"max_log_bytes"`
	}
	if err := contracts.Decode(r, &input); err != nil {
		return WorkflowAdapterConfig{}, err
	}
	invalid := func() (WorkflowAdapterConfig, error) {
		return WorkflowAdapterConfig{}, errors.New("invalid workflow adapter configuration")
	}
	if input.SchemaVersion != 1 || input.Contract == "" || len(input.Contract) > 256 || strings.ContainsAny(input.Contract, "\x00\r\n") {
		return invalid()
	}
	digest, err := hex.DecodeString(input.RuntimeDigest)
	if err != nil || len(digest) != 32 || input.RuntimeDigest != strings.ToLower(input.RuntimeDigest) {
		return invalid()
	}
	argv := make([]string, len(input.Argv))
	for i, arg := range input.Argv {
		if arg == nil || strings.ContainsRune(*arg, 0) {
			return invalid()
		}
		argv[i] = *arg
	}
	if len(argv) == 0 || !filepath.IsAbs(argv[0]) {
		return invalid()
	}
	if input.TimeoutMillis <= 0 || input.TimeoutMillis > math.MaxInt64/int64(time.Millisecond) || input.MaxMetadataBytes <= 0 || input.MaxMetadataBytes > contracts.MaxMetadataBytes || input.MaxLogBytes <= 0 || input.MaxLogBytes > contracts.MaxMessageBytes {
		return invalid()
	}
	return WorkflowAdapterConfig{Contract: input.Contract, RuntimeDigest: input.RuntimeDigest, Argv: argv, Timeout: time.Duration(input.TimeoutMillis) * time.Millisecond, MaxMetadataBytes: input.MaxMetadataBytes, MaxLogBytes: input.MaxLogBytes}, nil
}
