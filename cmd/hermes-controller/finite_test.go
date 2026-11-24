package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"hermes/internal/contracts"
	"hermes/internal/controller"

	"golang.org/x/sys/unix"
)

func finiteConfigBytes(t *testing.T, refs []contracts.FiniteRuntimeRef) []byte {
	t.Helper()
	b, err := json.Marshal(struct {
		SchemaVersion int                          `json:"schema_version"`
		Approved      []contracts.FiniteRuntimeRef `json:"approved_runtimes"`
	}{1, refs})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func finiteRuntimeFixture(t *testing.T) (*controller.Controller, controller.Config, contracts.FiniteRuntimeRef) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	v := contracts.ResourceVector{CPUMilli: 1000, MemoryBytes: 256 << 20, SpoolBytes: 64 << 20}
	p := &contracts.ResourcePolicy{SchemaVersion: 1, Revision: 1, ControllerHostID: "controller", Hosts: []contracts.HostApproval{{HostID: "controller", Limits: v, MaxAssignments: 1}}, Profiles: []contracts.ResourceProfile{{ID: "synthetic-local", Conservative: true, Allocation: contracts.Allocation{Resources: contracts.ResourceVector{CPUMilli: 1, MemoryBytes: 1, SpoolBytes: 1}}}}, TransferSlots: 1, ValidatorSlots: 1, TransferReservation: contracts.ResourceVector{SpoolBytes: 1 << 20}, ValidatorReservation: contracts.ResourceVector{SpoolBytes: 1 << 20}}
	cfg := controller.Config{Root: filepath.Join(t.TempDir(), "controller"), Workers: []string{"worker"}, ResourcePolicy: p, Runtime: "synthetic-v1", Validator: "synthetic-v1", Profile: "synthetic-local", ValidatorArgv: []string{exe}, MaxArtifactBytes: 1 << 20}
	c, err := controller.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	data := []byte("verified synthetic runtime bytes")
	a := contracts.Artifact{Digest: contracts.Hash(data), Size: int64(len(data)), MediaType: "application/octet-stream"}
	if err = c.PublishArtifact(context.Background(), a.Digest, a.Size, "", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	return c, cfg, contracts.FiniteRuntimeRef{Runtime: "synthetic-v1", GOOS: "darwin", GOARCH: "amd64", ISABaseline: "v1", Executable: a}
}
func TestFiniteConfigStrictLocalEnvelope(t *testing.T) {
	for _, body := range []string{`{}`, `{"schema_version":2,"approved_runtimes":[]}`, `{"schema_version":1}`, `{"schema_version":1,"approved_runtimes":null}`, `{"schema_version":1,"approved_runtimes":[],"argv":["/request-runtime"]}`, `{"schema_version":1,"approved_runtimes":[],"approved_runtimes":[]}`, `{"schema_version":1,"approved_runtimes":[]} {}`, `{`, strings.Repeat(" ", contracts.MaxMetadataBytes+1)} {
		if _, err := loadFiniteRuntimeConfig(strings.NewReader(body)); err == nil {
			t.Fatalf("invalid envelope accepted: %s", body[:min(len(body), 100)])
		}
	}
	refs := make([]contracts.FiniteRuntimeRef, contracts.MaxFiniteMembers+1)
	if _, err := loadFiniteRuntimeConfig(bytes.NewReader(finiteConfigBytes(t, refs))); err == nil {
		t.Fatal("approval count unbounded")
	}
	empty, err := loadFiniteRuntimeConfig(strings.NewReader(`{"schema_version":1,"approved_runtimes":[]}`))
	if err != nil || len(empty) != 0 {
		t.Fatal("explicit disabled config", err)
	}
}
func TestFiniteConfigurationUsesCanonicalDescriptorValidator(t *testing.T) {
	c, _, ref := finiteRuntimeFixture(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		change func(*contracts.FiniteRuntimeRef)
	}{
		{"runtime", func(r *contracts.FiniteRuntimeRef) { r.Runtime = "unapproved-contract" }},
		{"target", func(r *contracts.FiniteRuntimeRef) { r.GOOS = "linux" }},
		{"architecture", func(r *contracts.FiniteRuntimeRef) { r.GOARCH = "unknown" }},
		{"isa", func(r *contracts.FiniteRuntimeRef) { r.ISABaseline = "unknown" }},
		{"digest", func(r *contracts.FiniteRuntimeRef) { r.Executable.Digest = "bad" }},
		{"size", func(r *contracts.FiniteRuntimeRef) { r.Executable.Size++ }},
		{"unpublished", func(r *contracts.FiniteRuntimeRef) { r.Executable.Digest = contracts.Hash([]byte("not in catalog")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := ref
			tc.change(&bad)
			path := filepath.Join(t.TempDir(), "finite.json")
			if err := os.WriteFile(path, finiteConfigBytes(t, []contracts.FiniteRuntimeRef{bad}), 0600); err != nil {
				t.Fatal(err)
			}
			if err := configureFiniteRuntimes(ctx, c, path); err == nil {
				t.Fatal("invalid runtime approval accepted")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "finite.json")
	if err := os.WriteFile(path, finiteConfigBytes(t, []contracts.FiniteRuntimeRef{ref, ref}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := configureFiniteRuntimes(ctx, c, path); err == nil {
		t.Fatal("duplicate compatible binding accepted")
	}
	if err := os.WriteFile(path, finiteConfigBytes(t, []contracts.FiniteRuntimeRef{ref}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := configureFiniteRuntimes(ctx, c, path); err != nil {
		t.Fatal("valid catalog approval", err)
	}
	fifo := filepath.Join(t.TempDir(), "fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	if err := configureFiniteRuntimes(ctx, c, fifo); err == nil {
		t.Fatal("FIFO configuration accepted")
	}
}
func TestNoFiniteConfigDoesNotRestorePreviousApproval(t *testing.T) {
	c, cfg, ref := finiteRuntimeFixture(t)
	ctx := context.Background()
	if err := c.ConfigureFiniteRuntimes(ctx, []contracts.FiniteRuntimeRef{ref}); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := controller.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = configureFiniteRuntimes(ctx, reopened, ""); err != nil {
		reopened.Close()
		t.Fatal(err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(cfg.Root, "metadata.sqlite")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var manifest []byte
	if err = db.QueryRow(`SELECT manifest FROM finite_runtime_config WHERE id=1`).Scan(&manifest); err != nil {
		t.Fatal(err)
	}
	var refs []contracts.FiniteRuntimeRef
	if err = contracts.Decode(bytes.NewReader(manifest), &refs); err != nil || len(refs) != 1 || refs[0].Executable != ref.Executable {
		t.Fatalf("no-file startup rewrote persisted descriptors: %s %v", manifest, err)
	}
	// The API's instance tag, rather than copying these persisted descriptors,
	// keeps this new instance unapproved (covered by controller export tests).
}
func TestArtifactIngressLimitBounds(t *testing.T) {
	for _, n := range []int64{-1, 0, math.MaxInt64} {
		if err := validateMaxArtifactBytes(n); err == nil {
			t.Fatalf("invalid limit %d", n)
		}
	}
	for _, n := range []int64{1, 1 << 20, 32 << 20, math.MaxInt64 - 1} {
		if err := validateMaxArtifactBytes(n); err != nil {
			t.Fatalf("valid limit %d: %v", n, err)
		}
	}
}
func TestFiniteMainEntryHelper(t *testing.T) {
	marker := -1
	for i, arg := range os.Args {
		if arg == "--finite-main-helper" {
			marker = i
			break
		}
	}
	if marker < 0 {
		return
	}
	os.Args = append([]string{"hermes-controller"}, os.Args[marker+1:]...)
	flag.CommandLine = flag.NewFlagSet("hermes-controller", flag.ContinueOnError)
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	os.Exit(0)
}
func TestFiniteFlagsPreserveNoTLSFastPaths(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runMain := func(args ...string) ([]byte, error) {
		t.Helper()
		all := append([]string{"-test.run=^TestFiniteMainEntryHelper$", "--", "--finite-main-helper"}, args...)
		return exec.Command(executable, all...).CombinedOutput()
	}
	out, err := runMain("--version", "--finite-runtime-config", "/missing/config", "--max-artifact-bytes=-1")
	if err != nil {
		t.Fatalf("version fast path: %s %v", out, err)
	}
	var version struct {
		SchemaVersion int `json:"schema_version"`
	}
	if err = json.Unmarshal(out, &version); err != nil || version.SchemaVersion != 1 {
		t.Fatalf("version: %s %v", out, err)
	}
	out, err = runMain("--max-artifact-bytes=-1")
	if err == nil || !bytes.Contains(out, []byte("max-artifact-bytes")) {
		t.Fatalf("invalid limit didn't fail before TLS IO: %s %v", out, err)
	}
	missing := filepath.Join(t.TempDir(), "missing-root")
	out, err = runMain("check-quiescence", "--root", missing)
	if err == nil || !bytes.Contains(out, []byte(`"state":"uncertain"`)) {
		t.Fatalf("inspection fast path: %s %v", out, err)
	}
	if _, err = os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("inspection initialized root", err)
	}
}
