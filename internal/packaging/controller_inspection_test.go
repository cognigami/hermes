package packaging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestControllerUninstallRequiresItsOwnPositiveInspection(t *testing.T) {
	root := t.TempDir()
	install := filepath.Join(root, "install")
	state := filepath.Join(root, "state")
	for _, p := range []string{filepath.Join(install, "credentials"), state} {
		if e := os.MkdirAll(p, 0700); e != nil {
			t.Fatal(e)
		}
	}
	for name, content := range map[string]string{"state-root": state + "\n", "credentials/ca.pem": "ca", "credentials/cert.pem": "cert", "credentials/key.pem": "secret"} {
		if e := os.WriteFile(filepath.Join(install, name), []byte(content), 0600); e != nil {
			t.Fatal(e)
		}
	}
	if e := os.WriteFile(filepath.Join(root, "job.plist"), []byte("synthetic managed plist"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(state, "metadata.sqlite"), []byte("synthetic retained child metadata"), 0600); e != nil {
		t.Fatal(e)
	}
	binary := `#!/bin/sh
set -eu
test "$1" = check-quiescence && test "$2" = --root && test "$3" = "$FIXTURE/state"
test ! -e "$FIXTURE/registered"
if test ! -e "$FIXTURE/observed-exit"; then
 printf '%s\n' '{"schema_version":1,"state":"uncertain","reason":"controller child exit remains unproven","unresolved_children":1}'
 exit 1
fi
printf '%s\n' '{"schema_version":1,"state":"quiescent","unresolved_children":0}'
`
	if e := os.WriteFile(filepath.Join(install, "hermes-controller"), []byte(binary), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(root, "registered"), nil, 0600); e != nil {
		t.Fatal(e)
	}
	script := `
. "$1"
export FIXTURE=$2
hermes_root() { :; }
hermes_protected_path() { :; }
hermes_role() { HERMES_KIND=controller; HERMES_LABEL=org.hermes.controller; HERMES_PLIST=$FIXTURE/job.plist; }
plutil() { printf '%s\n' "$FIXTURE/install/hermes-controller"; }
launchctl() {
 printf '%s\n' "$1" >> "$FIXTURE/operations"
 case "$1" in
 disable) : ;;
 print) if test -e "$FIXTURE/registered"; then return 0; fi; printf 'Bad request.\nCould not find service "org.hermes.controller" in domain for system\n' >&2; return 113 ;;
 bootout) rm "$FIXTURE/registered" ;;
 *) exit 91 ;;
 esac
}
hermes_main controller uninstall "$FIXTURE/install" "$FIXTURE/state"
`
	before := map[string]string{}
	for _, name := range []string{"install/hermes-controller", "install/state-root", "install/credentials/ca.pem", "install/credentials/cert.pem", "install/credentials/key.pem", "job.plist", "state/metadata.sqlite"} {
		b, e := os.ReadFile(filepath.Join(root, name))
		if e != nil {
			t.Fatal(e)
		}
		before[name] = string(b)
	}
	out, e := shell(t, script, shared(t), root)
	if e == nil || !strings.Contains(string(out), "controller child exit remains unproven") || !strings.Contains(string(out), "uninstall incomplete") {
		t.Fatal("controller skipped inspection", string(out), e)
	}
	for name, content := range before {
		b, e := os.ReadFile(filepath.Join(root, name))
		if e != nil || string(b) != content {
			t.Fatal("failed controller inspection lost recovery bytes", name, e)
		}
	}
	operations, e := os.ReadFile(filepath.Join(root, "operations"))
	if e != nil || !strings.HasPrefix(string(operations), "disable\nprint\nbootout\nprint\n") {
		t.Fatal("inspected before persistent service stop", string(operations), e)
	}
	// This is a synthetic checker result, NOT a real controller journal edit.
	if e = os.WriteFile(filepath.Join(root, "observed-exit"), nil, 0600); e != nil {
		t.Fatal(e)
	}
	if out, e = shell(t, script, shared(t), root); e != nil {
		t.Fatal(string(out), e)
	}
	for _, name := range []string{"install/hermes-controller", "install/credentials/key.pem", "job.plist"} {
		if _, e = os.Lstat(filepath.Join(root, name)); !os.IsNotExist(e) {
			t.Fatal("known file retained after successful inspection", name, e)
		}
	}
	b, e := os.ReadFile(filepath.Join(state, "metadata.sqlite"))
	if e != nil || string(b) != before["state/metadata.sqlite"] {
		t.Fatal("controller state removed", e)
	}
}
