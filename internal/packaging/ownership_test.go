package packaging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrivilegedPathOwnershipAndMode(t *testing.T) {
	for _, tc := range []struct {
		ownerMode string
		ok        bool
	}{{"0 755", true}, {"501 755", false}, {"0 775", false}, {"0 777", false}, {"0 1777", false}} {
		out, e := shell(t, `. "$1"; mode=$2; stat() { printf '%s\n' "$mode"; }; hermes_protected_path /`, shared(t), tc.ownerMode)
		if (e == nil) != tc.ok {
			t.Fatal(tc, string(out), e)
		}
	}
}
func TestPrivilegedPathRejectsWritableAncestor(t *testing.T) {
	// The test only inspects its own synthetic paths; stat ownership is supplied.
	dir := t.TempDir()
	install := filepath.Join(dir, "install")
	if e := os.Mkdir(install, 0700); e != nil {
		t.Fatal(e)
	}
	out, e := shell(t, `. "$1"; install=$2; stat() { if test "$3" = "$install"; then printf '0 755\n'; else printf '501 755\n'; fi; }; hermes_protected_path "$install"`, shared(t), install)
	if e == nil || !strings.Contains(string(out), "root-owned") {
		t.Fatal("writable ancestor accepted", string(out), e)
	}
	link := filepath.Join(dir, "link")
	if e = os.Symlink(install, link); e != nil {
		t.Fatal(e)
	}
	out, e = shell(t, `. "$1"; hermes_protected_path "$2"`, shared(t), link)
	if e == nil || !strings.Contains(string(out), "symlinks") {
		t.Fatal("replaceable symlink accepted", string(out), e)
	}
}
