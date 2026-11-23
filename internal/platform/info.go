// Package platform reports build identity, not host qualification.
package platform

import (
	"hermes/internal/contracts"
	"runtime"
	"runtime/debug"
)

// Version is set by the package build with -X hermes/internal/platform.Version.
var Version = "dev"

type Info struct {
	SchemaVersion   int          `json:"schema_version"`
	Version         string       `json:"version"`
	GOOS            string       `json:"goos"`
	GOARCH          string       `json:"goarch"`
	ISABaseline     string       `json:"isa_baseline"`
	GoVersion       string       `json:"go_version"`
	CGOEnabled      string       `json:"cgo_enabled"`
	ProtocolVersion int          `json:"protocol_version"`
	Capabilities    Capabilities `json:"capabilities"`
}

type Capabilities struct {
	LeafExit              string `json:"leaf_exit"`
	RestartAdoption       string `json:"restart_adoption"`
	DescendantContainment string `json:"descendant_containment"`
	HardTreeMemory        string `json:"hard_tree_memory"`
	GPU                   string `json:"gpu"`
}

// Current performs no host discovery. Unknown enforcement is not admission.
func Current() Info {
	i := Info{SchemaVersion: 1, Version: Version, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, ISABaseline: "unknown", GoVersion: runtime.Version(), CGOEnabled: "unknown", ProtocolVersion: contracts.Version,
		Capabilities: Capabilities{LeafExit: "owned_handle_only", RestartAdoption: "unsupported", DescendantContainment: "unknown", HardTreeMemory: "unknown", GPU: "unsupported"}}
	if b, ok := debug.ReadBuildInfo(); ok {
		for _, s := range b.Settings {
			if (runtime.GOARCH == "amd64" && s.Key == "GOAMD64") || (runtime.GOARCH == "arm64" && s.Key == "GOARM64") {
				i.ISABaseline = s.Value
			}
			if s.Key == "CGO_ENABLED" {
				i.CGOEnabled = s.Value
			}
		}
	}
	return i
}
