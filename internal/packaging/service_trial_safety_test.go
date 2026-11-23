package packaging

import (
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestTrialCommandBoundaryCancelsForegroundChild(t *testing.T) {
	helper := trialHelper(t)
	for _, mode := range []string{"timeout", "interrupt"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			millis := "600"
			if mode == "interrupt" {
				millis = "10000"
			}
			output, e := os.Create(filepath.Join(root, "output"))
			if e != nil {
				t.Fatal(e)
			}
			defer output.Close()
			c := exec.Command(helper, "test-boundary", root, millis)
			c.Stdout = output
			c.Stderr = output
			start := time.Now()
			if e = c.Start(); e != nil {
				t.Fatal(e)
			}
			done := make(chan error, 1)
			go func() { done <- c.Wait() }()
			defer c.Process.Kill() // Owned handle is safe even after Wait reports done.
			readyUntil := time.Now().Add(500 * time.Millisecond)
			ready := false
			for time.Now().Before(readyUntil) {
				if _, e = os.Stat(filepath.Join(root, "child-ready")); e == nil {
					ready = true
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !ready {
				t.Fatal("foreground synthetic child did not start")
			}
			if mode == "interrupt" {
				if e = c.Process.Signal(syscall.SIGTERM); e != nil {
					t.Fatal(e)
				}
			}
			select {
			case e = <-done:
				if e == nil {
					t.Fatal("cancelled command succeeded")
				}
			case <-time.After(2 * time.Second):
				c.Process.Kill()
				t.Fatal("command/pipe wait exceeded bound")
			}
			if time.Since(start) > 2500*time.Millisecond {
				t.Fatal("unbounded cancellation")
			}
			lock, e := os.OpenFile(filepath.Join(root, "child.lock"), os.O_RDWR, 0)
			if e != nil {
				t.Fatal(e)
			}
			defer lock.Close()
			unlocked := false
			until := time.Now().Add(500 * time.Millisecond)
			for time.Now().Before(until) {
				if e = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); e == nil {
					unlocked = true
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !unlocked {
				t.Fatal("foreground child continued holding lifetime lock after cleanup")
			}
			defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
			if _, e = os.Stat(filepath.Join(root, "child-continued")); !os.IsNotExist(e) {
				t.Fatal("cancelled child continued calculation", e)
			}
		})
	}
}
func TestTrialCannotAttributeNaturalTimeoutToServiceStop(t *testing.T) {
	helper := trialHelper(t)
	for _, mode := range []string{"test-cleanup-proof", "test-cleanup-noop"} {
		out, e := exec.Command(helper, mode).CombinedOutput()
		if e == nil || !strings.Contains(string(out), "deadline exceeded") {
			t.Fatal("slow/no-op stop falsely established cleanup", mode, string(out), e)
		}
	}
}
func TestTrialBusyRefusalNeedsActualVersionedInspectorGate(t *testing.T) {
	helper := trialHelper(t)
	valid := "{\"schema_version\":1,\"state\":\"busy\",\"reason\":\"host authority is active\"}\nlocal quiescence busy: host authority is active\nquiescence unknown/busy; uninstall incomplete, binary, credentials and state preserved\n"
	for _, tc := range []struct {
		name, text string
		ok         bool
	}{{"actual_gate", valid, true}, {"ipc", "service registration unknown: IPC failure", false}, {"permission", "Permission denied", false}, {"wrong_version", strings.Replace(valid, "schema_version\":1", "schema_version\":2", 1), false}, {"json_only", strings.Split(valid, "\n")[0], false}, {"message_only", strings.Join(strings.Split(valid, "\n")[1:], "\n"), false}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "capture")
			if e := os.WriteFile(path, []byte(tc.text), 0600); e != nil {
				t.Fatal(e)
			}
			out, e := exec.Command(helper, "test-busy-proof", path).CombinedOutput()
			if (e == nil) != tc.ok {
				t.Fatal(tc.name, string(out), e)
			}
		})
	}
}
