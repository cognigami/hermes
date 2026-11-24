package main

import (
	"context"
	"errors"
	"io"
	"math"
	"os"

	"hermes/internal/contracts"
	"hermes/internal/controller"

	"golang.org/x/sys/unix"
)

func validateMaxArtifactBytes(size int64) error {
	if size < 1 || size > math.MaxInt64-1 {
		return errors.New("max-artifact-bytes must be positive and at most 9223372036854775806")
	}
	return nil
}

// This envelope is LOCAL deployment configuration, not an export request. The
// controller API alone validates descriptor targets, identity and catalog refs.
func loadFiniteRuntimeConfig(source io.Reader) ([]contracts.FiniteRuntimeRef, error) {
	var envelope struct {
		SchemaVersion int                           `json:"schema_version"`
		Approved      *[]contracts.FiniteRuntimeRef `json:"approved_runtimes"`
	}
	if err := contracts.Decode(source, &envelope); err != nil {
		return nil, err
	}
	if envelope.SchemaVersion != 1 || envelope.Approved == nil || len(*envelope.Approved) > contracts.MaxFiniteMembers {
		return nil, errors.New("invalid finite runtime configuration envelope")
	}
	return *envelope.Approved, nil
}
func configureFiniteRuntimes(ctx context.Context, c *controller.Controller, path string) error {
	// Approval is instance-bound in the controller. Absence must not reapply a
	// previous instance's persisted descriptors; old bundles remain usable.
	if path == "" {
		return nil
	}
	var refs []contracts.FiniteRuntimeRef
	{
		fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		source := os.NewFile(uintptr(fd), path)
		defer source.Close()
		info, err := source.Stat()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("finite runtime config must be a regular file")
		}
		refs, err = loadFiniteRuntimeConfig(source)
		if err != nil {
			return err
		}
	}
	return c.ConfigureFiniteRuntimes(ctx, refs)
}
