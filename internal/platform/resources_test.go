//go:build darwin

package platform

import (
	"hermes/internal/contracts"
	"math"
	"path/filepath"
	"runtime"
	"testing"
)

func TestNativeResourceArithmetic(t *testing.T) {
	got, err := nativeResources(8, 16<<30, 100, 25, 4096)
	if err != nil || got.Resources != (contracts.ResourceVector{CPUMilli: 8000, MemoryBytes: 16 << 30, SpoolBytes: 409600}) || got.AvailableSpoolBytes != 102400 || got.Source != "host_os" || got.HardTreeMemory != "unknown" {
		t.Fatal(got, err)
	}
	full, err := nativeResources(1, 1, 1, 0, 1)
	if err != nil || full.Resources.SpoolBytes != 1 || full.AvailableSpoolBytes != 0 {
		t.Fatal("full filesystem is not unknown total capacity", full, err)
	}
	for _, tc := range []struct {
		name                           string
		cpus                           int
		memory, total, available, size uint64
	}{
		{"zero CPU", 0, 1, 1, 0, 1},
		{"negative CPU", -1, 1, 1, 0, 1},
		{"CPU overflow", int(math.MaxInt64/1000 + 1), 1, 1, 0, 1},
		{"zero memory", 1, 0, 1, 0, 1},
		{"memory overflow", 1, uint64(math.MaxInt64) + 1, 1, 0, 1},
		{"zero blocks", 1, 1, 0, 0, 1},
		{"zero block size", 1, 1, 1, 0, 0},
		{"available exceeds total", 1, 1, 1, 2, 1},
		{"byte overflow", 1, 1, uint64(math.MaxInt64/4096) + 1, 1, 4096},
		{"block size overflow", 1, 1, 1, 0, math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := nativeResources(tc.cpus, tc.memory, tc.total, tc.available, tc.size)
			if err == nil || got.Resources != (contracts.ResourceVector{}) || got.AvailableSpoolBytes != 0 || got.Source != "unknown" || got.HardTreeMemory != "unknown" {
				t.Fatal("capacity fallback on invalid observation", got, err)
			}
		})
	}
}

func TestObserveNativeResourcesReadOnly(t *testing.T) {
	got, err := ObserveResources(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if got.Resources.CPUMilli != int64(runtime.NumCPU())*1000 || got.Resources.MemoryBytes <= 0 || got.Resources.SpoolBytes <= 0 || got.AvailableSpoolBytes < 0 || got.AvailableSpoolBytes > got.Resources.SpoolBytes || got.Source != "host_os" || got.HardTreeMemory != "unknown" {
		t.Fatal(got)
	}
	t.Logf("native OS facts (not approved budgets or hard enforcement): %+v", got)
}
func TestObserveInvalidPathFailsClosed(t *testing.T) {
	for _, path := range []string{"", filepath.Join(t.TempDir(), "missing"), "\x00"} {
		got, err := ObserveResources(path)
		if err == nil || got.Resources != (contracts.ResourceVector{}) || got.AvailableSpoolBytes != 0 || got.Source != "unknown" || got.HardTreeMemory != "unknown" {
			t.Fatal(path, got, err)
		}
	}
}
