package controller

import (
	"context"
	"hermes/internal/contracts"
	"os/exec"
	"strings"
	"testing"
)

func TestControlStatusRejectsUnknownTargets(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	for _, kind := range []string{"worker", "run"} {
		var status contracts.ControlStatus
		e := f.operator.Do(ctx, "GET", "/v1/control/status?kind="+kind+"&target=typo", nil, &status)
		if e == nil || !strings.Contains(e.Error(), "unknown "+kind) {
			t.Fatal("unknown target appeared eligible", kind, status, e)
		}
	}
	cli := exec.Command(f.cli, "--url", f.server.URL, "--ca", f.ca, "--cert", f.cert, "--key", f.key, "control-status", "run:typo")
	out, e := cli.CombinedOutput()
	if e == nil || !strings.Contains(string(out), "unknown run") {
		t.Fatal("CLI unknown target appeared eligible", string(out), e)
	}
	s := oneSubmission("known")
	if e = f.operator.Do(ctx, "POST", "/v1/runs", s, nil); e != nil {
		t.Fatal(e)
	}
	for _, target := range []struct{ kind, id string }{{"worker", "local"}, {"run", s.RunID}} {
		var status contracts.ControlStatus
		if e = f.operator.Do(ctx, "GET", "/v1/control/status?kind="+target.kind+"&target="+target.id, nil, &status); e != nil || status.State != "eligible" || status.Revision != 0 || status.IssuancePaused {
			t.Fatal("known no-control target not eligible", status, e)
		}
	}
}
