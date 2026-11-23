package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestSignalLeafHelper(t *testing.T) {
	marker := os.Getenv("HERMES_TEST_SIGNAL_LEAF")
	if marker == "" {
		return
	}
	if e := contracts.WriteMessage(os.Stdout, contracts.Message{SchemaVersion: 1, Type: "ready"}); e != nil {
		t.Fatal(e)
	}
	if _, e := contracts.ReadMessage(contracts.MessageReader(os.Stdin)); e != nil {
		t.Fatal(e)
	}
	time.Sleep(700 * time.Millisecond)
	if e := os.WriteFile(marker, []byte("natural application return, not exit authority"), 0600); e != nil {
		t.Fatal(e)
	}
}
func TestSignalErrorsReportUncertaintyWithoutWaitingOrFallback(t *testing.T) {
	binary, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	for _, failure := range []string{"cooperative", "force"} {
		t.Run(failure, func(t *testing.T) {
			root := t.TempDir()
			marker := filepath.Join(root, "natural-return")
			t.Setenv("HERMES_TEST_SIGNAL_LEAF", marker)
			store, e := artifacts.New(filepath.Join(root, "blobs"), 1<<20)
			if e != nil {
				t.Fatal(e)
			}
			work := testWork()
			now := time.Now()
			work.Assignment.StopNewUnitsAt = now.Add(100 * time.Millisecond)
			work.Assignment.TerminateAt = now.Add(250 * time.Millisecond)
			work.Assignment.HandoffDeadline = now.Add(time.Second)
			reports := make(chan contracts.ReconcileRequest, 4)
			var finals atomic.Int64
			client := &Client{URL: "https://synthetic.invalid", HTTP: &http.Client{Transport: testTransport(func(req *http.Request) (*http.Response, error) {
				var value any = contracts.Acknowledgment{SchemaVersion: 1, Received: true}
				switch req.URL.Path {
				case "/v1/control/poll":
					value = contracts.AssignmentControl{SchemaVersion: 1, AssignmentID: work.Assignment.AssignmentID}
				case "/v1/reconcile":
					var report contracts.ReconcileRequest
					if e := contracts.Decode(req.Body, &report); e != nil {
						return nil, e
					}
					reports <- report
					value = contracts.ReconcileResult{SchemaVersion: 1, State: "uncertain"}
				case "/v1/final":
					finals.Add(1)
					value = contracts.Acknowledgment{SchemaVersion: 1, Quiescent: true}
				}
				b, _ := json.Marshal(value)
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(b))}, nil
			})}}
			r := runner{testEnvironment: []string{"HERMES_TEST_SIGNAL_LEAF=" + marker}, cfg: Config{Root: root, RuntimeArgv: []string{binary, "-test.run=^TestSignalLeafHelper$", "--"}, Client: client, ShutdownGrace: 50 * time.Millisecond}, j: journal{Work: work, ReceiptWall: now.UTC()}, store: store, received: now}
			var calls atomic.Int64
			r.testSignal = func(_ *os.Process, sig os.Signal) error {
				calls.Add(1)
				if failure == "force" && sig == syscall.SIGTERM {
					return nil
				}
				return syscall.EPERM
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			start := time.Now()
			err := r.assignment(ctx)
			if err == nil || !strings.Contains(err.Error(), "enforcement uncertainty") || time.Since(start) > 600*time.Millisecond {
				t.Fatal("signal failure blocked or was hidden", time.Since(start), err)
			}
			if !r.j.Uncertain || !r.j.GateClosed || r.j.Quiescent || r.j.FinalSent || r.terminalPersisted || finals.Load() != 0 {
				t.Fatal("failed enforcement invented terminal authority", r.j)
			}
			select {
			case report := <-reports:
				if report.Quiescent || !report.ProcessStarted || !strings.Contains(report.OperationalReason, "operation not permitted") {
					t.Fatal("enforcement uncertainty not reported", report)
				}
			default:
				t.Fatal("missing reconciliation report")
			}
			want := int64(1)
			if failure == "force" {
				want = 2
			}
			if calls.Load() != want {
				t.Fatal("unchecked fallback signal path", calls.Load(), want)
			}
			if _, e = os.Stat(marker); !os.IsNotExist(e) {
				t.Fatal("helper already returned; unknown live case not exercised", e)
			}
			// No real denied signal or alternate cleanup signal is used. The bounded leaf
			// returns naturally; this file is NOT treated as authoritative quiescence.
			timer := time.NewTimer(time.Second)
			defer timer.Stop()
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				if _, e = os.Stat(marker); e == nil {
					break
				}
				select {
				case <-timer.C:
					t.Fatal("an unchecked Kill bypassed the injected signal denial")
				case <-tick.C:
				}
			}
			if r.j.Quiescent {
				t.Fatal("later file presence cleared uncertainty")
			}
		})
	}
}
