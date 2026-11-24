package contracts_test

import (
	"hermes/internal/contracts"
	"testing"
)

func TestCapabilitiesNeedKnownMatchingFacts(t *testing.T) {
	a := contracts.Allocation{}
	if !contracts.SupportsCapabilities([]string{"leaf_exit", "os:darwin", "arch:arm64"}, "darwin", "arm64", "unknown", a) {
		t.Fatal("qualified facts rejected")
	}
	for _, cap := range []string{"unknown", "os:linux", "arch:amd64", "hard_tree_memory"} {
		if contracts.SupportsCapabilities([]string{cap}, "darwin", "arm64", "supported", a) {
			t.Fatal("unqualified capability accepted", cap)
		}
	}
	a.RequiresHardTreeMemory = true
	if contracts.SupportsCapabilities([]string{"hard_tree_memory"}, "darwin", "arm64", "unknown", a) {
		t.Fatal("unknown memory enforcement accepted")
	}
	if !contracts.SupportsCapabilities([]string{"hard_tree_memory"}, "darwin", "arm64", "supported", a) {
		t.Fatal("approved qualified memory denied")
	}
}
