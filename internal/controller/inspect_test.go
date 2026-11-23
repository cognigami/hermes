package controller

import (
	"context"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func inspectionSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	snapshot := map[string]string{}
	for _, name := range []string{"authority.lock", "metadata.sqlite", "metadata.sqlite-wal", "metadata.sqlite-shm", "metadata.sqlite-journal", "validation-retained/result"} {
		b, e := os.ReadFile(filepath.Join(root, name))
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		snapshot[name] = contracts.Hash(b)
	}
	return snapshot
}
func TestControllerInspectionClosedRootPreservesState(t *testing.T) {
	c, cfg := ingressFixture(t)
	claim, e := c.ReserveControllerChild(context.Background(), "validator", "retained-result", contracts.ResourceVector{})
	if e != nil {
		t.Fatal(e)
	}
	// A real owned leaf Wait is positive proof, not a fabricated successful Wait.
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(exe, "-test.run=^TestControllerInspectionLeaf$")
	cmd.Env = append(os.Environ(), "HERMES_INSPECTION_LEAF=1")
	if e = claim.Started(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	waitErr := cmd.Wait()
	if e = claim.ObserveWait(context.Background(), cmd, waitErr); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(filepath.Join(cfg.Root, "validation-retained"), 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(cfg.Root, "validation-retained", "result"), []byte("retained synthetic output"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	before := inspectionSnapshot(t, cfg.Root)
	report, e := CheckLocalQuiescence(cfg.Root)
	if e != nil || report.State != "quiescent" || report.SchemaVersion != 1 || report.UnresolvedChildren != 0 {
		t.Fatal(report, e)
	}
	if after := inspectionSnapshot(t, cfg.Root); !reflect.DeepEqual(before, after) {
		t.Fatal("read-only inspection modified state", before, after)
	}
}
func TestControllerInspectionLeaf(t *testing.T) {
	if os.Getenv("HERMES_INSPECTION_LEAF") == "" {
		return
	}
}
func TestControllerInspectionAuthorityAndUnresolvedChildren(t *testing.T) {
	c, cfg := ingressFixture(t)
	report, e := CheckLocalQuiescence(cfg.Root)
	if e == nil || report.State != "busy" {
		t.Fatal(report, e)
	}
	for _, role := range []string{"validator", "continuation"} {
		claim, e := c.ReserveControllerChild(context.Background(), role, "PID absent; deadline 1970; NOT exit proof", contracts.ResourceVector{})
		if e != nil {
			t.Fatal(e)
		}
		if e = claim.Started(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	before := inspectionSnapshot(t, cfg.Root)
	report, e = CheckLocalQuiescence(cfg.Root)
	if e == nil || report.State != "uncertain" || report.UnresolvedChildren != 2 {
		t.Fatal("lock-free parent/PID/deadline erased child uncertainty", report, e)
	}
	if after := inspectionSnapshot(t, cfg.Root); !reflect.DeepEqual(before, after) {
		t.Fatal("inspection reconciled/cleared child claims")
	}
}
func TestControllerInspectionKnownPrestartClaim(t *testing.T) {
	c, cfg := ingressFixture(t)
	if _, e := c.ReserveControllerChild(context.Background(), "continuation", "not launched", contracts.ResourceVector{}); e != nil {
		t.Fatal(e)
	}
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	report, e := CheckLocalQuiescence(cfg.Root)
	if e != nil || report.State != "quiescent" {
		t.Fatal(report, e)
	}
}
func TestControllerInspectionIncompleteOrUnknownMetadata(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"missing bootstrap", "DELETE FROM controller_storage_init"},
		{"missing resource head", "DELETE FROM resource_head"},
		{"future DB version", "PRAGMA user_version=999"},
		{"future tracking field", "ALTER TABLE validator_claims ADD COLUMN pid INTEGER"},
		{"invalid exit flag", "UPDATE validator_claims SET exited=2"},
		{"invalid start flag", "UPDATE validator_claims SET started=2"},
		{"missing claim fields", `UPDATE validator_claims SET resources='{"Role":"validator"}'`},
		{"null claim", `UPDATE validator_claims SET resources='null'`},
		{"unknown claim fields", `UPDATE validator_claims SET resources='{"Role":"validator","Reference":"x","Resources":{"cpu_milli":0,"memory_bytes":0,"spool_bytes":0},"PID":123}'`},
		{"missing vector proof", `UPDATE validator_claims SET resources='{"Role":"validator","Reference":"x","Resources":{"cpu_milli":0,"spool_bytes":0}}'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, cfg := ingressFixture(t)
			if _, e := c.ReserveControllerChild(context.Background(), "validator", "x", contracts.ResourceVector{}); e != nil {
				t.Fatal(e)
			}
			if _, e := c.db.Exec(tc.sql); e != nil {
				t.Fatal(e)
			}
			if e := c.Close(); e != nil {
				t.Fatal(e)
			}
			report, e := CheckLocalQuiescence(cfg.Root)
			if e == nil || report.State != "uncertain" {
				t.Fatal("unknown proof accepted", report, e)
			}
		})
	}
}
func TestControllerInspectionMissingCorruptAndSymlinkMetadata(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if report, e := CheckLocalQuiescence(missing); e == nil || report.State != "uncertain" {
		t.Fatal(report, e)
	}
	if _, e := os.Stat(missing); !os.IsNotExist(e) {
		t.Fatal("created missing root", e)
	}
	for _, name := range []string{"authority.lock", "metadata.sqlite", "metadata.sqlite-wal", "metadata.sqlite-shm"} {
		t.Run(name, func(t *testing.T) {
			c, cfg := ingressFixture(t)
			if e := c.Close(); e != nil {
				t.Fatal(e)
			}
			external := filepath.Join(t.TempDir(), "external")
			if e := os.WriteFile(external, []byte("external untouched"), 0600); e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(cfg.Root, name)
			if e := os.Remove(path); e != nil && !os.IsNotExist(e) {
				t.Fatal(e)
			}
			if e := os.Symlink(external, path); e != nil {
				t.Fatal(e)
			}
			report, e := CheckLocalQuiescence(cfg.Root)
			if e == nil || report.State != "uncertain" {
				t.Fatal(report, e)
			}
			b, e := os.ReadFile(external)
			if e != nil || string(b) != "external untouched" {
				t.Fatal(string(b), e)
			}
		})
	}
	c, cfg := ingressFixture(t)
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(cfg.Root, "metadata.sqlite"), []byte("not SQLite"), 0600); e != nil {
		t.Fatal(e)
	}
	report, e := CheckLocalQuiescence(cfg.Root)
	if e == nil || report.State != "uncertain" {
		t.Fatal(report, e)
	}
}

// This child either inspects read-only or dies with committed WAL still present.
func TestControllerInspectionProcessHelper(t *testing.T) {
	root := os.Getenv("HERMES_CONTROLLER_INSPECTION_ROOT")
	if root == "" {
		return
	}
	if os.Getenv("HERMES_CONTROLLER_INSPECTION_MODE") == "crash-writer" {
		c := ingressOpen(t, ingressConfig(t, root, fixtureResourcePolicy([]string{"local"})))
		claim, e := c.ReserveControllerChild(context.Background(), "validator", "no parent exit inference", contracts.ResourceVector{})
		if e != nil {
			t.Fatal(e)
		}
		if e = claim.Started(context.Background()); e != nil {
			t.Fatal(e)
		}
		os.Exit(77)
	}
	report, e := CheckLocalQuiescence(root)
	if encodeErr := json.NewEncoder(os.Stdout).Encode(report); encodeErr != nil {
		os.Exit(4)
	}
	if e != nil {
		os.Exit(3)
	}
	os.Exit(0)
}
func inspectionChild(t *testing.T, root, mode string) (LocalQuiescence, error) {
	t.Helper()
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestControllerInspectionProcessHelper$")
	cmd.Env = append(os.Environ(), "HERMES_CONTROLLER_INSPECTION_ROOT="+root, "HERMES_CONTROLLER_INSPECTION_MODE="+mode)
	b, e := cmd.CombinedOutput()
	var report LocalQuiescence
	if mode != "crash-writer" {
		if parse := json.Unmarshal(b, &report); parse != nil {
			t.Fatal(string(b), parse)
		}
	}
	return report, e
}
func TestControllerInspectionNeverIgnoresCommittedWAL(t *testing.T) {
	c, cfg := ingressFixture(t)
	if _, e := c.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); e != nil {
		t.Fatal(e)
	}
	claim, e := c.ReserveControllerChild(context.Background(), "validator", "committed only in WAL", contracts.ResourceVector{})
	if e != nil {
		t.Fatal(e)
	}
	if e = claim.Started(context.Background()); e != nil {
		t.Fatal(e)
	}
	// Keep SQLite's WAL-index owner alive but release the service authority. No
	// writes race the independent read-only process during this synthetic test.
	if e = c.lock.Close(); e != nil {
		t.Fatal(e)
	}
	before := inspectionSnapshot(t, cfg.Root)
	report, e := inspectionChild(t, cfg.Root, "inspect")
	if e == nil || report.State != "uncertain" {
		t.Fatal("committed WAL child marker ignored", report, e)
	}
	if after := inspectionSnapshot(t, cfg.Root); !reflect.DeepEqual(before, after) {
		t.Fatal("WAL inspection wrote metadata", before, after)
	}
}
func TestControllerInspectionCrashWALDoesNotRepair(t *testing.T) {
	root := filepath.Join(t.TempDir(), "controller")
	_, e := inspectionChild(t, root, "crash-writer")
	var exited *exec.ExitError
	if !errors.As(e, &exited) || exited.ExitCode() != 77 {
		t.Fatal(e)
	}
	before := inspectionSnapshot(t, root)
	report, e := CheckLocalQuiescence(root)
	if e == nil || report.State != "uncertain" {
		t.Fatal(report, e)
	}
	if strings.TrimSpace(report.Reason) == "" {
		t.Fatal("missing uncertainty reason")
	}
	if after := inspectionSnapshot(t, root); !reflect.DeepEqual(before, after) {
		t.Fatal("read-only inspector repaired/cracked WAL metadata")
	}
}
