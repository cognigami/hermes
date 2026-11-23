//go:build darwin

package platform

import (
	"errors"
	"fmt"
	"hermes/internal/contracts"
	"math"
	"runtime"

	"golang.org/x/sys/unix"
)

// ObserveResources reads native OS capacity and the filesystem containing path.
// Total spool capacity and currently available bytes are distinct observations.
// Neither increases operator-approved budgets or qualifies hard enforcement.
func ObserveResources(path string) (contracts.HostObservation, error) {
	memory, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return unknownResources(), fmt.Errorf("observe native memory: %w", err)
	}
	var fs unix.Statfs_t
	if err = unix.Statfs(path, &fs); err != nil {
		return unknownResources(), fmt.Errorf("observe spool filesystem: %w", err)
	}
	return nativeResources(runtime.NumCPU(), memory, fs.Blocks, fs.Bavail, uint64(fs.Bsize))
}

func unknownResources() contracts.HostObservation {
	return contracts.HostObservation{Source: "unknown", HardTreeMemory: "unknown"}
}

func nativeResources(cpus int, memory, totalBlocks, availableBlocks, blockSize uint64) (contracts.HostObservation, error) {
	fail := func(reason string) (contracts.HostObservation, error) { return unknownResources(), errors.New(reason) }
	if cpus <= 0 || uint64(cpus) > math.MaxInt64/1000 {
		return fail("invalid or overflowing logical CPU capacity")
	}
	if memory == 0 || memory > math.MaxInt64 {
		return fail("invalid or overflowing native memory capacity")
	}
	if blockSize == 0 || totalBlocks == 0 || availableBlocks > totalBlocks {
		return fail("invalid filesystem capacity")
	}
	if totalBlocks > math.MaxInt64/blockSize {
		return fail("filesystem byte capacity overflow")
	}
	// availableBlocks <= totalBlocks, so its product is bounded by the same check.
	return contracts.HostObservation{Resources: contracts.ResourceVector{CPUMilli: int64(cpus) * 1000, MemoryBytes: int64(memory), SpoolBytes: int64(totalBlocks * blockSize)}, AvailableSpoolBytes: int64(availableBlocks * blockSize), Source: "host_os", HardTreeMemory: "unknown"}, nil
}
