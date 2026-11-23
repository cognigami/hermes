package agent

import (
	"errors"
	"golang.org/x/sys/unix"
	"hermes/internal/contracts"
	"os"
	"path/filepath"
)

// Open nonblocking before fstat: opening a FIFO for reading can otherwise wait
// forever even after the application process/context has been terminated.
func openRegular(root *os.Root, path string) (*os.File, error) {
	if !contracts.ValidPath(path) {
		return nil, errors.New("invalid application file path")
	}
	f, e := root.OpenFile(filepath.FromSlash(path), os.O_RDONLY|unix.O_NONBLOCK, 0)
	if e != nil {
		return nil, e
	}
	info, e := f.Stat()
	if e != nil {
		f.Close()
		return nil, e
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("application output is not a regular file")
	}
	return f, nil
}
