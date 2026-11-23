package controller

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"hermes/internal/contracts"
)

func workflowConfigJSON(t *testing.T, change func(map[string]any)) string {
	t.Helper()
	value := map[string]any{"schema_version": 1, "contract": "staged-v1", "runtime_digest": contracts.Hash([]byte("runtime")), "argv": []string{"/nonexistent/metadata-only-adapter", "literal ; $(not-a-shell)", ""}, "timeout_millis": int64(1500), "max_metadata_bytes": contracts.MaxMetadataBytes, "max_log_bytes": contracts.MaxMessageBytes}
	if change != nil {
		change(value)
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
func TestLoadWorkflowAdapterConfig(t *testing.T) {
	cfg, err := LoadWorkflowAdapterConfig(strings.NewReader(workflowConfigJSON(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Contract != "staged-v1" || cfg.Timeout != 1500*time.Millisecond || len(cfg.Argv) != 3 || cfg.Argv[1] != "literal ; $(not-a-shell)" || cfg.Argv[2] != "" || cfg.MaxMetadataBytes != contracts.MaxMetadataBytes || cfg.MaxLogBytes != contracts.MaxMessageBytes {
		t.Fatalf("mapping: %+v", cfg)
	}
	max := int64(math.MaxInt64 / int64(time.Millisecond))
	cfg, err = LoadWorkflowAdapterConfig(strings.NewReader(workflowConfigJSON(t, func(v map[string]any) { v["timeout_millis"] = max })))
	if err != nil || cfg.Timeout <= 0 {
		t.Fatalf("largest representable duration: %+v %v", cfg, err)
	}
}
func TestLoadWorkflowAdapterConfigRejectsInvalidMetadata(t *testing.T) {
	cases := []struct {
		name, key string
		value     any
	}{
		{"version", "schema_version", 2}, {"missing-version", "schema_version", nil},
		{"contract", "contract", ""}, {"contract-nul", "contract", "bad\x00contract"},
		{"digest", "runtime_digest", "bad"}, {"uppercase-digest", "runtime_digest", strings.Repeat("A", 64)},
		{"relative-executable", "argv", []string{"relative"}}, {"empty-argv", "argv", []string{}}, {"nul-executable", "argv", []string{"/bad\x00path"}}, {"nul-argument", "argv", []string{"/adapter", "arg\x00bad"}}, {"null-argument", "argv", []any{"/adapter", nil}}, {"null-executable", "argv", []any{nil}},
		{"zero-timeout", "timeout_millis", 0}, {"negative-timeout", "timeout_millis", -1}, {"overflow-timeout", "timeout_millis", math.MaxInt64/int64(time.Millisecond) + 1}, {"fractional-timeout", "timeout_millis", 1.5},
		{"zero-metadata", "max_metadata_bytes", 0}, {"excessive-metadata", "max_metadata_bytes", contracts.MaxMetadataBytes + 1},
		{"zero-logs", "max_log_bytes", 0}, {"negative-logs", "max_log_bytes", -1}, {"excessive-logs", "max_log_bytes", contracts.MaxMessageBytes + 1},
		{"unrecognized-field", "environment", map[string]string{"SECRET": "must not inherit"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := workflowConfigJSON(t, func(v map[string]any) { v[tc.key] = tc.value })
			if _, err := LoadWorkflowAdapterConfig(strings.NewReader(body)); err == nil {
				t.Fatal("invalid local configuration accepted")
			}
		})
	}
	for _, body := range []string{`{`, workflowConfigJSON(t, nil) + ` {}`, `{"schema_version":1,"schema_version":1}`, strings.Repeat(" ", contracts.MaxMetadataBytes+1)} {
		if _, err := LoadWorkflowAdapterConfig(strings.NewReader(body)); err == nil {
			t.Fatal("malformed/oversize configuration accepted")
		}
	}
}
