//go:build !darwin

package platform

import (
	"fmt"
	"hermes/internal/contracts"
	"runtime"
)

// ObserveResources fails closed until effective resource observation is qualified
// for this environment. In particular, Linux host RAM is not container capacity.
func ObserveResources(path string) (contracts.HostObservation, error) {
	return contracts.HostObservation{Source: "unknown", HardTreeMemory: "unknown"}, fmt.Errorf("effective resource observation unsupported on %s", runtime.GOOS)
}
