package agent

import (
	"context"
	"hermes/internal/artifacts"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLiveFailedStartIsKnownQuiescentWithoutFakeWait(t *testing.T) {
	for _, kind := range []string{"missing", "nonexec"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			binary := filepath.Join(root, "runtime")
			if kind == "nonexec" {
				if e := os.WriteFile(binary, []byte("not executable"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			store, e := artifacts.New(filepath.Join(root, "blobs"), 1<<20)
			if e != nil {
				t.Fatal(e)
			}
			r := runner{cfg: Config{Root: root, RuntimeArgv: []string{binary}}, j: journal{Work: testWork(), ReceiptWall: time.Now().UTC()}, store: store}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if e = r.execute(ctx); e == nil {
				t.Fatal("failed Start became success")
			}
			if !r.j.Quiescent || r.j.Uncertain || !r.j.LaunchFailed || !r.terminalPersisted || len(r.j.Started) != 0 || len(r.j.Candidates) != 0 {
				t.Fatal("live failed Start leaked uncertainty or invented unit/result", r.j)
			}
			data, e := os.ReadFile(filepath.Join(root, "journal.json"))
			if e != nil {
				t.Fatal(e)
			}
			var recovered journal
			if e = recovered.UnmarshalJSON(data); e != nil || !recovered.Quiescent || !recovered.LaunchFailed {
				t.Fatal("no-child proof not durable", e)
			}
		})
	}
}
