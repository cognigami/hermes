package main

import (
	"bytes"
	"encoding/json"
	"hermes/internal/contracts"
	"hermes/internal/controller"
	"os"
	"path/filepath"
	"testing"
)

func TestInspectionCommandNeedsNoTLSAndCreatesNothing(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	var out bytes.Buffer
	if e := runQuiescenceInspection([]string{"--root", root}, &out); e == nil {
		t.Fatal("missing root declared quiescent")
	}
	var report controller.LocalQuiescence
	if e := json.Unmarshal(out.Bytes(), &report); e != nil || report.SchemaVersion != 1 || report.State != "uncertain" {
		t.Fatal(out.String(), e)
	}
	if _, e := os.Lstat(root); !os.IsNotExist(e) {
		t.Fatal("inspection initialized missing root", e)
	}
}
func TestInspectionCommandClosedCurrentRoot(t *testing.T) {
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	v := contracts.ResourceVector{CPUMilli: 1000, MemoryBytes: 256 << 20, SpoolBytes: 64 << 20}
	p := &contracts.ResourcePolicy{SchemaVersion: 1, Revision: 1, ControllerHostID: "controller", Hosts: []contracts.HostApproval{{HostID: "controller", Limits: v, MaxAssignments: 1}}, Profiles: []contracts.ResourceProfile{{ID: "synthetic-local", Conservative: true, Allocation: contracts.Allocation{Resources: contracts.ResourceVector{CPUMilli: 1, MemoryBytes: 1, SpoolBytes: 1}}}}, TransferSlots: 1, ValidatorSlots: 1, TransferReservation: contracts.ResourceVector{SpoolBytes: 1 << 20}, ValidatorReservation: contracts.ResourceVector{SpoolBytes: 1 << 20}}
	root := filepath.Join(t.TempDir(), "controller")
	c, e := controller.Open(controller.Config{Root: root, Workers: []string{"local"}, ResourcePolicy: p, Runtime: "synthetic-v1", Validator: "synthetic-v1", Profile: "synthetic-local", ValidatorArgv: []string{exe}, MaxArtifactBytes: 1 << 20})
	if e != nil {
		t.Fatal(e)
	}
	var out bytes.Buffer
	if e = runQuiescenceInspection([]string{"--root", root}, &out); e == nil {
		t.Fatal("live authority inspected quiescent")
	}
	var busy controller.LocalQuiescence
	if e = json.Unmarshal(out.Bytes(), &busy); e != nil || busy.State != "busy" {
		t.Fatal(out.String(), e)
	}
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	out.Reset()
	if e = runQuiescenceInspection([]string{"--root", root}, &out); e != nil {
		t.Fatal(out.String(), e)
	}
	var report controller.LocalQuiescence
	if e = json.Unmarshal(out.Bytes(), &report); e != nil || report.State != "quiescent" || report.SchemaVersion != 1 {
		t.Fatal(out.String(), e)
	}
}
