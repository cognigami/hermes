package packaging

import (
	"strings"
	"testing"
)

func TestOnlyExactQualifiedLaunchdAbsenceIsAbsent(t *testing.T) {
	exact := "Bad request.\nCould not find service \"org.hermes.agent\" in domain for system\n"
	for _, tc := range []struct{ name, status, diagnostic, want string }{
		{"registered", "0", "", "registered"},
		{"qualified_absence", "113", exact, "absent"},
		{"wrong_code", "5", exact, "unknown"},
		{"ipc_error", "5", "IPC connection failed\n", "unknown"},
		{"permission", "1", "Permission denied\n", "unknown"},
		{"other_label", "113", strings.ReplaceAll(exact, "org.hermes.agent", "org.hermes.other"), "unknown"},
		{"generic_notfound", "113", "not found\n", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, e := shell(t, `. "$1"; hermes_role agent; code=$2; diagnostic_text=$3; launchctl() { test "$LC_ALL" = C || exit 90; printf '%s' "$diagnostic_text" >&2; return "$code"; }; if hermes_registered; then printf registered; else printf absent; fi`, shared(t), tc.status, tc.diagnostic)
			if tc.want == "unknown" {
				if e == nil || !strings.Contains(string(out), "registration unknown") {
					t.Fatal(string(out), e)
				}
			} else if e != nil || string(out) != tc.want {
				t.Fatal(string(out), e)
			}
		})
	}
}
