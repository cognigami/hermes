package controller

import (
	"bytes"
	"context"
	"errors"
	"hermes/internal/contracts"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestClaimedLeafCopyErrorPreservesOperationalFailureButReleasesKnownExit(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	claim, e := f.c.ReserveControllerChild(ctx, "validator", "bounded-copy", contracts.ResourceVector{})
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command("/bin/echo", "one small kernel pipe write")
	cmd.Stdout = &limitedBuffer{max: 1}
	e = runClaimedLeaf(ctx, claim, cmd)
	if e == nil {
		t.Fatal("output limit became success")
	}
	if cmd.ProcessState == nil || !cmd.ProcessState.Success() {
		t.Fatal("fixture lacks positive OS exit", cmd.ProcessState, e)
	}
	var exited int
	if err := f.c.db.QueryRow("SELECT exited FROM validator_claims WHERE id=?", claim.id).Scan(&exited); err != nil || exited != 1 {
		t.Fatal("copy error leaked known-exited reservation", exited, err)
	}
	var accepted int
	if err := f.c.db.QueryRow("SELECT count(*) FROM acceptances").Scan(&accepted); err != nil || accepted != 0 {
		t.Fatal("copy error accepted science", accepted, err)
	}
}
func TestClaimedLeafEnvironmentIsExplicitAndResourceBounded(t *testing.T) {
	f := setup(t)
	f.stopValidation()
	t.Setenv("HERMES_CREDENTIAL_SENTINEL", "must-not-inherit")
	t.Setenv("OMP_NUM_THREADS", "999")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	claim, e := f.c.ReserveControllerChild(ctx, "continuation", "env", contracts.ResourceVector{})
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command("/usr/bin/env")
	var out bytes.Buffer
	cmd.Stdout = &out
	if e = runClaimedLeaf(ctx, claim, cmd); e != nil {
		t.Fatal(e)
	}
	value := out.String()
	if strings.Contains(value, "HERMES_CREDENTIAL_SENTINEL") || strings.Contains(value, "999") || !strings.Contains(value, "GOMAXPROCS=1\n") || !strings.Contains(value, "OMP_NUM_THREADS=1\n") {
		t.Fatal("unsafe inherited/unbounded environment", value)
	}
	if e = claim.ObserveWait(ctx, &exec.Cmd{}, errors.New("no observed wait")); e == nil {
		t.Fatal("missing owned wait state accepted")
	}
}
