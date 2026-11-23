package controller

import (
	"context"
	"errors"
	"hermes/internal/contracts"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestControllerHelperStagingCountsEveryPathBeforeLaunch(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	v := contracts.Candidate{}
	for i := 0; i < 1024; i++ {
		v.Files = append(v.Files, contracts.ResultFile{Artifact: contracts.Artifact{Size: 1 << 20}})
	}
	minimum, e := validationScratch(v, []byte("manifest"), []byte("unit"))
	if e != nil || minimum.SpoolBytes != (1<<30)+12 {
		t.Fatal("duplicate digest copies undercharged", minimum, e)
	}
	if _, e = f.c.ReserveControllerChild(context.Background(), "validator", "per-path", minimum); !errors.Is(e, ErrControllerCapacity) {
		t.Fatal("insufficient helper spool admitted", e)
	}
	var claims int
	if e = f.c.db.QueryRow("SELECT count(*) FROM validator_claims").Scan(&claims); e != nil || claims != 0 {
		t.Fatal("capacity refusal left a claim", claims, e)
	}
}
func TestControllerUnknownChildHoldsSlotAndScratchAcrossRestart(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	ctx := context.Background()
	unknown, e := f.c.ReserveControllerChild(ctx, "validator", "unknown-exit", contracts.ResourceVector{})
	if e != nil {
		t.Fatal(e)
	}
	relative := "validation-" + unknown.id
	if e = unknown.ReserveScratch(ctx, relative, 1024); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(filepath.Join(f.cfg.Root, relative), 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(f.cfg.Root, relative, "retained"), []byte("data"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = unknown.Started(ctx); e != nil {
		t.Fatal(e)
	}
	if e = unknown.ObserveExit(ctx, errors.New("wait observation unavailable")); e == nil {
		t.Fatal("unknown wait released capacity")
	}
	if e = unknown.CleanupScratch(ctx); e == nil {
		t.Fatal("unproven child scratch deleted")
	}
	unstarted, e := f.c.ReserveControllerChild(ctx, "continuation", "not-launched", contracts.ResourceVector{})
	if e != nil {
		t.Fatal(e)
	}
	f.stop()
	reopened, e := Open(f.cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	var held, never int
	if e = reopened.db.QueryRow("SELECT exited FROM validator_claims WHERE id=?", unknown.id).Scan(&held); e != nil {
		t.Fatal(e)
	}
	if e = reopened.db.QueryRow("SELECT exited FROM validator_claims WHERE id=?", unstarted.id).Scan(&never); e != nil {
		t.Fatal(e)
	}
	if held != 0 || never != 1 {
		t.Fatal("restart discarded unknown claim or failed never-launched proof", held, never)
	}
	if _, e = reopened.ReserveControllerChild(ctx, "continuation", "spare-approved-slot", contracts.ResourceVector{}); e != nil {
		t.Fatal(e)
	}
	if _, e = reopened.ReserveControllerChild(ctx, "validator", "over-capacity", contracts.ResourceVector{}); !errors.Is(e, ErrControllerCapacity) {
		t.Fatal("unknown slot did not count", e)
	}
	if _, e = os.Stat(filepath.Join(f.cfg.Root, relative, "retained")); e != nil {
		t.Fatal("unknown child lost retained scratch", e)
	}
}
func TestControllerObservedWaitDoesNotFreeScratchUntilCleanup(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	ctx := context.Background()
	claim, e := f.c.ReserveControllerChild(ctx, "validator", "observed", contracts.ResourceVector{SpoolBytes: 1024})
	if e != nil {
		t.Fatal(e)
	}
	relative := "validation-" + claim.id
	if e = claim.ReserveScratch(ctx, relative, 1024); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(filepath.Join(f.cfg.Root, relative), 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(f.cfg.Root, relative, "input"), []byte("data"), 0600); e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(f.app, "-h")
	if e = claim.Started(ctx); e != nil {
		t.Fatal(e)
	}
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	waitErr := cmd.Wait()
	if e = claim.ObserveExit(ctx, waitErr); e != nil {
		t.Fatal(e)
	}
	var bytes int64
	if e = f.c.db.QueryRow("SELECT size FROM controller_storage WHERE claim_id=?", claim.id).Scan(&bytes); e != nil || bytes != 1024 {
		t.Fatal("exit discarded retained bytes", bytes, e)
	}
	if e = claim.CleanupScratch(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(f.cfg.Root, relative)); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("scratch not removed", e)
	}
	var rows int
	if e = f.c.db.QueryRow("SELECT count(*) FROM controller_storage WHERE claim_id=?", claim.id).Scan(&rows); e != nil || rows != 0 {
		t.Fatal(rows, e)
	}
}
func TestControllerRuntimeCopiesAreChargedPerPath(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	ctx := context.Background()
	for _, path := range []string{"workflow-runtime-one/binary", "workflow-runtime-two/binary"} {
		if e := f.c.ReserveControllerStorage(ctx, path, 3<<20); e != nil {
			t.Fatal(e)
		}
		if e := f.c.ReserveControllerStorage(ctx, path, 3<<20); e != nil {
			t.Fatal(e)
		}
	}
	var bytes int64
	if e := f.c.db.QueryRow("SELECT sum(size) FROM controller_storage").Scan(&bytes); e != nil || bytes != 6<<20 {
		t.Fatal("same executable bytes deduped across actual copies", bytes, e)
	}
}

func TestFailedStartHasPositiveNoChildProofAndRetainsScratchUntilCleanup(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	ctx := context.Background()
	for _, name := range []string{"missing", "nonexec"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "validator")
			if name == "nonexec" {
				if e := os.WriteFile(path, []byte("not executable"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			claim, e := f.c.ReserveControllerChild(ctx, "validator", name, contracts.ResourceVector{SpoolBytes: 128})
			if e != nil {
				t.Fatal(e)
			}
			relative := "validation-" + claim.id
			if e = claim.ReserveScratch(ctx, relative, 128); e != nil {
				t.Fatal(e)
			}
			if e = os.Mkdir(filepath.Join(f.cfg.Root, relative), 0700); e != nil {
				t.Fatal(e)
			}
			if e = claim.Started(ctx); e != nil {
				t.Fatal(e)
			}
			cmd := exec.Command(path)
			startErr := cmd.Start()
			if startErr == nil {
				t.Fatal("invalid executable started")
			}
			if e = claim.ObserveStartFailure(ctx, cmd, startErr); e != nil {
				t.Fatal(e)
			}
			var exited, charged int
			if e = f.c.db.QueryRow("SELECT exited FROM validator_claims WHERE id=?", claim.id).Scan(&exited); e != nil {
				t.Fatal(e)
			}
			if e = f.c.db.QueryRow("SELECT count(*) FROM controller_storage WHERE claim_id=?", claim.id).Scan(&charged); e != nil {
				t.Fatal(e)
			}
			if exited != 1 || charged != 1 {
				t.Fatal("no-child proof lost or scratch prematurely freed", exited, charged)
			}
			if e = claim.CleanupScratch(ctx); e != nil {
				t.Fatal(e)
			}
		})
	}
}
