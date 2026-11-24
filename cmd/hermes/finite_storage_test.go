package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFiniteManifestExposureNeverOverwritesForeignFileOrLink(t *testing.T) {
	path := t.TempDir()
	root, e := os.OpenRoot(path)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	foreign := []byte("foreign file must remain unchanged")
	name := filepath.Join(path, "bundle.json")
	if e = os.WriteFile(name, foreign, 0600); e != nil {
		t.Fatal(e)
	}
	if e = finiteCLIWrite(root, "bundle.json", []byte("replacement"), false); e == nil {
		t.Fatal("foreign manifest overwritten")
	}
	got, e := os.ReadFile(name)
	if e != nil || !bytes.Equal(got, foreign) {
		t.Fatal(got, e)
	}
	if e = os.Remove(name); e != nil {
		t.Fatal(e)
	}
	outside := filepath.Join(t.TempDir(), "private")
	if e = os.WriteFile(outside, foreign, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Symlink(outside, name); e != nil {
		t.Fatal(e)
	}
	if e = finiteCLIWrite(root, "bundle.json", []byte("replacement"), false); e == nil {
		t.Fatal("foreign symlink overwritten")
	}
	info, e := os.Lstat(name)
	if e != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("foreign link replaced", e)
	}
	got, e = os.ReadFile(outside)
	if e != nil || !bytes.Equal(got, foreign) {
		t.Fatal("foreign target changed", e)
	}
}
