package agent

import (
	"bytes"
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestDrainGrantSerialization(t *testing.T) {
	for i := 0; i < 20; i++ {
		root := t.TempDir()
		r := runner{cfg: Config{Root: root}, j: journal{Work: testWork(), ProcessStarted: true}}
		control := contracts.AssignmentControl{SchemaVersion: 1, AssignmentID: r.j.Work.Assignment.AssignmentID, Revision: 1, Action: "drain", TerminateAt: r.j.Work.Assignment.TerminateAt}
		var ack contracts.ControlAck
		var grant bool
		var first, second error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); grant, first = r.grant("u") }()
		go func() { defer wg.Done(); ack, second = r.applyControl(control) }()
		wg.Wait()
		if first != nil || second != nil {
			t.Fatal(first, second)
		}
		want := 0
		if grant {
			want = 1
		}
		if ack.LastGrantIndex != want {
			t.Fatal("drain ack crossed persisted grant", ack, grant)
		}
		if allowed, e := r.grant("u"); e != nil || allowed {
			t.Fatal("grant after barrier", allowed, e)
		}
		var j journal
		b, e := os.ReadFile(filepath.Join(root, "journal.json"))
		if e != nil {
			t.Fatal(e)
		}
		if e = contracts.Decode(bytes.NewReader(b), &j); e != nil {
			t.Fatal(e)
		}
		if j.Control.Revision != 1 || len(j.Started) != want {
			t.Fatal("barrier not durable")
		}
	}
}
func TestGrantStorageFailureClosesGate(t *testing.T) {
	r := runner{cfg: Config{Root: filepath.Join(t.TempDir(), "missing")}, j: journal{Work: testWork(), ProcessStarted: true}}
	if allowed, e := r.grant("u"); e == nil || allowed {
		t.Fatal("failed write permitted grant")
	}
	if allowed, e := r.grant("u"); e != nil || allowed {
		t.Fatal("failed storage gate reopened", allowed, e)
	}
}
func TestFailedControlSaveCannotAcknowledgeRetry(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	r := runner{cfg: Config{Root: root}, j: journal{Work: testWork()}}
	c := contracts.AssignmentControl{SchemaVersion: 1, AssignmentID: r.j.Work.Assignment.AssignmentID, Revision: 1, Action: "drain", TerminateAt: r.j.Work.Assignment.TerminateAt}
	if _, e := r.applyControl(c); e == nil {
		t.Fatal("missing journal parent accepted control")
	}
	if _, e := r.applyControl(c); e == nil {
		t.Fatal("in-memory revision substituted for durable control")
	}
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	if _, e := r.applyControl(c); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(root, "journal.json")); e != nil {
		t.Fatal("retry acknowledgment was not durable", e)
	}
}

func TestLeafExitProof(t *testing.T) {
	if !leafExitProven(nil) || !leafExitProven(&exec.ExitError{}) {
		t.Fatal("observed exit not recognized")
	}
	for _, e := range []error{unix.ECHILD, unix.EPERM, errors.New("unknown wait failure")} {
		if leafExitProven(e) {
			t.Fatal("unknown wait interpreted as quiescent", e)
		}
	}
}
func TestNoPreparationPastGate(t *testing.T) {
	for _, mode := range []string{"expired", "clock"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			w := testWork()
			if mode == "expired" {
				w.Assignment.StopNewUnitsAt = time.Now().Add(-time.Second)
			}
			r := runner{cfg: Config{Root: root, RuntimeArgv: []string{filepath.Join(root, "must-not-run")}}, j: journal{Work: w}}
			if mode == "clock" {
				r.j.ReceiptWall = time.Now().Add(time.Hour)
			}
			e := r.execute(context.Background())
			if mode == "clock" && e == nil {
				t.Fatal("invalid clock accepted")
			}
			if mode == "expired" && e != nil {
				t.Fatal(e)
			}
			if r.j.ProcessStarted || !r.j.Quiescent || !r.terminalPersisted {
				t.Fatal("preparation started or no terminal evidence")
			}
		})
	}
}
func TestSlowLeafHelper(t *testing.T) {
	mode := os.Getenv("HERMES_TEST_SLOW_LEAF")
	if mode == "" {
		return
	}
	if mode == "ignore-term" {
		signal.Ignore(syscall.SIGTERM)
	}
	if mode == "prepare" {
		time.Sleep(30 * time.Second)
		return
	}
	if e := contracts.WriteMessage(os.Stdout, contracts.Message{SchemaVersion: 1, Type: "ready"}); e != nil {
		t.Fatal(e)
	}
	if _, e := contracts.ReadMessage(contracts.MessageReader(os.Stdin)); e != nil {
		t.Fatal(e)
	}
	time.Sleep(30 * time.Second)
}
func TestLeafFiniteDeadlineDuringPreparationAndUnit(t *testing.T) {
	binary, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"prepare", "unit", "ignore-term"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("HERMES_TEST_SLOW_LEAF", mode)
			root := t.TempDir()
			w := testWork()
			now := time.Now()
			w.Assignment.StopNewUnitsAt = now.Add(100 * time.Millisecond)
			w.Assignment.TerminateAt = now.Add(250 * time.Millisecond)
			w.Assignment.HandoffDeadline = now.Add(time.Second)
			store, e := artifacts.New(filepath.Join(root, "blobs"), 1<<20)
			if e != nil {
				t.Fatal(e)
			}
			r := runner{testEnvironment: []string{"HERMES_TEST_SLOW_LEAF=" + mode}, cfg: Config{Root: root, RuntimeArgv: []string{binary, "-test.run=^TestSlowLeafHelper$", "--"}, ShutdownGrace: 30 * time.Millisecond}, j: journal{Work: w}, store: store, received: now}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			start := time.Now()
			if e = r.execute(ctx); e == nil {
				t.Fatal("slow leaf completed unexpectedly")
			}
			if time.Since(start) > time.Second || !r.j.Quiescent || !r.terminalPersisted {
				t.Fatal("deadline did not prove leaf exit")
			}
			want := 0
			if mode != "prepare" {
				want = 1
			}
			if len(r.j.Started) != want {
				t.Fatal("incorrect grant inventory", r.j.Started)
			}
		})
	}
}
