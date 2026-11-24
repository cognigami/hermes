package main

import (
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFiniteCLIRejectsUnscopedArgumentsBeforeHostMutation(t *testing.T) {
	for _, args := range [][]string{{"--root", "relative"}, {"--root", t.TempDir(), "--max-artifact-bytes", "0"}, {"--root", t.TempDir(), "--max-artifact-bytes", "9223372036854775807"}, {"--root", t.TempDir(), "unexpected"}, {"--root", t.TempDir(), "--bundle", "relative", "--worker", "w", "--session", "s"}} {
		if e := runFiniteCommand("finite-run", args); e == nil {
			t.Fatal("invalid/unscoped CLI accepted", args)
		}
	}
}
func TestFiniteCLIMetadataRejectsFIFOAndUnknownAckFields(t *testing.T) {
	root := t.TempDir()
	fifo := filepath.Join(root, "fifo")
	if e := unix.Mkfifo(fifo, 0600); e != nil {
		t.Fatal(e)
	}
	var value any
	if e := decodeFiniteFile(fifo, &value); e == nil {
		t.Fatal("FIFO accepted as finite metadata")
	}
	ack := filepath.Join(root, "ack.json")
	if e := os.WriteFile(ack, []byte(`{"schema_version":1,"bundle_id":"b","bundle_digest":"`+strings.Repeat("0", 64)+`","members":[],"accepted":true}`), 0600); e != nil {
		t.Fatal(e)
	}
	state := filepath.Join(root, "state")
	if e := runFiniteCommand("finite-ack", []string{"--root", state, "--ack", ack}); e == nil {
		t.Fatal("science acceptance field accepted as raw ACK")
	}
	if _, e := os.Stat(state); !os.IsNotExist(e) {
		t.Fatal("invalid ACK created host state", e)
	}
}
