// Package packaging tests package assets in private directories, never host services.
package packaging

import (
	"bytes"
	"encoding/xml"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func assets(t *testing.T, kind string) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "deploy", kind, "darwin"))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
func shared(t *testing.T) string { return filepath.Join(assets(t, "agent"), "service.sh") }
func shell(t *testing.T, script string, args ...string) ([]byte, error) {
	t.Helper()
	c := exec.Command("sh", append([]string{"-c", script, "packaging-test"}, args...)...)
	return c.CombinedOutput()
}
func TestPlistEscapesExactArguments(t *testing.T) {
	for _, kind := range []string{"agent", "controller"} {
		t.Run(kind, func(t *testing.T) {
			special := "a & <b> \"quote\" 'apostrophe' $(touch nope); value"
			out, e := shell(t, `. "$1"; HERMES_ASSETS=$2; hermes_role "$3"; hermes_render _hermes '/opt/Hermes App' '/var/Hermes State' --worker "$4"`, shared(t), assets(t, kind), kind, special)
			if e != nil {
				t.Fatal(string(out), e)
			}
			d := xml.NewDecoder(bytes.NewReader(out))
			var values []string
			for {
				tok, err := d.Token()
				if err != nil {
					if err != io.EOF {
						t.Fatal(err, string(out))
					}
					break
				}
				if start, ok := tok.(xml.StartElement); ok && start.Name.Local == "string" {
					var s string
					if err = d.DecodeElement(&s, &start); err != nil {
						t.Fatal(err)
					}
					values = append(values, s)
				}
			}
			if len(values) != 14 || values[len(values)-1] != special || values[3] != "/opt/Hermes App/hermes-"+kind {
				t.Fatal(values)
			}
			if !strings.Contains(string(out), "<key>AbandonProcessGroup</key><false/>") {
				t.Fatal("default cleanup lost")
			}
		})
	}
}
func TestScriptSyntaxAndInvalidArguments(t *testing.T) {
	for _, kind := range []string{"agent", "controller"} {
		for _, name := range []string{"install.sh", "uninstall.sh", "lifecycle.sh"} {
			p := filepath.Join(assets(t, kind), name)
			if out, e := exec.Command("sh", "-n", p).CombinedOutput(); e != nil {
				t.Fatal(string(out), e)
			}
		}
	}
	if out, e := exec.Command("sh", "-n", shared(t)).CombinedOutput(); e != nil {
		t.Fatal(string(out), e)
	}
	for _, script := range []string{`hermes_absolute relative`, `hermes_absolute /`, `hermes_absolute /a/../b`, `hermes_text 'line
break'`} {
		if out, e := shell(t, `. "$1"; `+script, shared(t)); e == nil {
			t.Fatal("invalid argument accepted", string(out))
		}
	}
	for _, flag := range []string{"--root", "-root=/other", "--ca=/other", "-cert", "--key"} {
		out, e := shell(t, `. "$1"; hermes_main agent install _hermes /dedicated/install /dedicated/state ca cert key "$2"`, shared(t), flag)
		if e == nil || !strings.Contains(string(out), "flags are managed") {
			t.Fatal("reserved flag accepted", flag, string(out), e)
		}
	}
	// Usage validation precedes root/account/service operations.
	for _, kind := range []string{"agent", "controller"} {
		dir := t.TempDir()
		for _, name := range []string{"install.sh", "service.sh"} {
			source := filepath.Join(assets(t, kind), name)
			if name == "service.sh" {
				source = shared(t)
			}
			b, e := os.ReadFile(source)
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(filepath.Join(dir, name), b, 0600); e != nil {
				t.Fatal(e)
			}
		}
		if out, e := exec.Command("sh", filepath.Join(dir, "install.sh")).CombinedOutput(); e == nil || !strings.Contains(string(out), "usage:") {
			t.Fatal(string(out), e)
		}
	}
}
func TestIsolatedInstallStopResumeAndSafeUninstall(t *testing.T) {
	for _, kind := range []string{"agent", "controller"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			packageDir := filepath.Join(dir, "package")
			if e := os.Mkdir(packageDir, 0700); e != nil {
				t.Fatal(e)
			}
			template, e := os.ReadFile(filepath.Join(assets(t, kind), "service.plist"))
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(filepath.Join(packageDir, "service.plist"), template, 0600); e != nil {
				t.Fatal(e)
			}
			// Stub core checker: initially unknown; later positively quiescent.
			binary := []byte("#!/bin/sh\ngrep -Fx \"$0\" \"$HERMES_TEST_DIR/protected-paths\" >/dev/null || exit 93\ntest ! -f \"$HERMES_TEST_DIR/registered\" || exit 94\ntest -f \"$HERMES_TEST_DIR/quiescent\"\n")
			if e = os.WriteFile(filepath.Join(packageDir, "hermes-"+kind), binary, 0700); e != nil {
				t.Fatal(e)
			}
			for _, name := range []string{"ca", "cert", "key"} {
				if e = os.WriteFile(filepath.Join(dir, name), []byte(name), 0600); e != nil {
					t.Fatal(e)
				}
			}
			script := `
. "$1"
export HERMES_TEST_DIR=$2
HERMES_ASSETS=$3
kind=$4
# These functions replace ALL privilege/service operations. No launchctl runs.
hermes_root() { :; }
hermes_role() { HERMES_KIND=$1; HERMES_LABEL=org.hermes.$1; HERMES_PLIST=$HERMES_TEST_DIR/job.plist; }
id() { printf '501\n'; }
chown() { printf '%s\n' "$*" >> "$HERMES_TEST_DIR/ownership"; }
hermes_protected_path() { printf '%s\n' "$1" >> "$HERMES_TEST_DIR/protected-paths"; }
plutil() { if test "$1" = -extract; then printf '%s\n' "$HERMES_TEST_DIR/install/hermes-$kind"; fi; }
launchctl() {
 printf '%s\n' "$1" >> "$HERMES_TEST_DIR/operations"
 case "$1" in
 print)
  if test -f "$HERMES_TEST_DIR/unknown"; then printf 'IPC connection failed\n' >&2; return 5; fi
  if test -f "$HERMES_TEST_DIR/registered"; then return 0; fi
  printf 'Bad request.\nCould not find service "%s" in domain for system\n' "org.hermes.$kind" >&2
  return 113 ;;
 bootstrap) touch "$HERMES_TEST_DIR/registered" ;;
 bootout) rm "$HERMES_TEST_DIR/registered" ;;
 enable|disable) : ;;
 *) exit 91 ;;
 esac
}
install=$HERMES_TEST_DIR/install
state=$HERMES_TEST_DIR/state
# A permissive caller umask cannot cause creation of unsafe missing parents.
if (umask 000; hermes_main "$kind" install _hermes "$HERMES_TEST_DIR/missing/install" "$state" "$HERMES_TEST_DIR/ca" "$HERMES_TEST_DIR/cert" "$HERMES_TEST_DIR/key"); then exit 95; fi
test ! -e "$HERMES_TEST_DIR/missing"
# Unknown registration must reject a fresh install before copying files.
touch "$HERMES_TEST_DIR/unknown"
if (hermes_main "$kind" install _hermes "$install" "$state" "$HERMES_TEST_DIR/ca" "$HERMES_TEST_DIR/cert" "$HERMES_TEST_DIR/key"); then exit 96; fi
test ! -e "$install"
rm "$HERMES_TEST_DIR/unknown"
hermes_main "$kind" install _hermes "$install" "$state" "$HERMES_TEST_DIR/ca" "$HERMES_TEST_DIR/cert" "$HERMES_TEST_DIR/key" --worker local
printf retained > "$state/result"
# Unknown state is not absence in stop/start/uninstall/repeated-install guards.
touch "$HERMES_TEST_DIR/unknown"
for operation in stop start uninstall; do
 if (hermes_main "$kind" "$operation" "$install" "$state"); then exit 97; fi
 test -f "$install/hermes-$kind" && test -f "$install/credentials/key.pem" && test -f "$install/state-root" && test -f "$HERMES_TEST_DIR/job.plist" && test -f "$state/result"
done
if (hermes_main "$kind" install _hermes "$install" "$state" "$HERMES_TEST_DIR/ca" "$HERMES_TEST_DIR/cert" "$HERMES_TEST_DIR/key" --worker local); then exit 98; fi
rm "$HERMES_TEST_DIR/unknown"
hermes_main "$kind" install _hermes "$install" "$state" "$HERMES_TEST_DIR/ca" "$HERMES_TEST_DIR/cert" "$HERMES_TEST_DIR/key" --worker local
hermes_main "$kind" stop "$install" "$state"
test ! -e "$HERMES_TEST_DIR/registered"
hermes_main "$kind" install _hermes "$install" "$state" "$HERMES_TEST_DIR/ca" "$HERMES_TEST_DIR/cert" "$HERMES_TEST_DIR/key" --worker local
test ! -e "$HERMES_TEST_DIR/registered"
hermes_main "$kind" start "$install" "$state"
test -e "$HERMES_TEST_DIR/registered"
if test "$kind" = agent; then
 if (hermes_main "$kind" uninstall "$install" "$state"); then exit 92; fi
 test -f "$install/hermes-agent" && test -f "$install/credentials/key.pem" && test -f "$state/result"
fi
touch "$HERMES_TEST_DIR/quiescent"
hermes_main "$kind" uninstall "$install" "$state"
test ! -e "$install/hermes-$kind" && test ! -e "$install/credentials/key.pem" && test -f "$state/result"
hermes_main "$kind" uninstall "$install" "$state"
touch "$HERMES_TEST_DIR/unknown"
if (hermes_main "$kind" uninstall "$install" "$state"); then exit 99; fi
test -f "$state/result"
`
			if out, e := shell(t, script, shared(t), dir, packageDir, kind); e != nil {
				t.Fatal(string(out), e)
			}
			ops, e := os.ReadFile(filepath.Join(dir, "operations"))
			if e != nil {
				t.Fatal(e)
			}
			if !strings.Contains(string(ops), "disable\nprint\nbootout\nprint\n") {
				t.Fatal("stop ordering", string(ops))
			}
			ownership, e := os.ReadFile(filepath.Join(dir, "ownership"))
			if e != nil {
				t.Fatal(e)
			}
			for _, path := range []string{"install", "install/hermes-" + kind, "install/state-root"} {
				if !strings.Contains(string(ownership), "root:wheel "+filepath.Join(dir, path)+"\n") {
					t.Fatal("missing privileged ownership", path, string(ownership))
				}
			}
			protected, e := os.ReadFile(filepath.Join(dir, "protected-paths"))
			if e != nil {
				t.Fatal(e)
			}
			if !strings.Contains(string(protected), filepath.Join(dir, "install/hermes-"+kind)+"\n") {
				t.Fatal("checker path unprotected", string(protected))
			}
		})
	}
}
