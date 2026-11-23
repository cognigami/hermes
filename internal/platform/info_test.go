package platform

import (
	"encoding/json"
	"hermes/internal/contracts"
	"runtime"
	"testing"
)

func TestCurrentReportsBuildNotHostQualification(t *testing.T) {
	i := Current()
	if i.SchemaVersion != 1 || i.ProtocolVersion != contracts.Version || i.Version != Version || i.GOOS != runtime.GOOS || i.GOARCH != runtime.GOARCH || i.GoVersion != runtime.Version() {
		t.Fatal(i)
	}
	if i.ISABaseline == "" || i.CGOEnabled == "" {
		t.Fatal("missing explicit unknown build settings", i)
	}
	if i.Capabilities.DescendantContainment != "unknown" || i.Capabilities.HardTreeMemory != "unknown" || i.Capabilities.RestartAdoption != "unsupported" || i.Capabilities.GPU != "unsupported" || i.Capabilities.LeafExit != "owned_handle_only" {
		t.Fatal("unsupported qualification claim", i)
	}
	b, e := json.Marshal(i)
	if e != nil {
		t.Fatal(e)
	}
	var roundtrip Info
	if e = json.Unmarshal(b, &roundtrip); e != nil || roundtrip != i {
		t.Fatal(string(b), e)
	}
}
