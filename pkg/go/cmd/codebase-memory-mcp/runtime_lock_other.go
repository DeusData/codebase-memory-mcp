//go:build !darwin && !linux && !windows

package main

import (
	"fmt"
	"os"
)

func platformOpenRuntimeSetLockOwner(path string) (*os.File, error) {
	return os.Open(path)
}

func platformCreatePrivateMutationRuntimeDirectory() (string, error) {
	directory, err := os.MkdirTemp("", "codebase-memory-mcp-mutation-*")
	if err != nil {
		return "", err
	}
	status, err := os.Lstat(directory)
	if err != nil || !status.IsDir() || status.Mode()&os.ModeSymlink != 0 ||
		status.Mode().Perm()&0077 != 0 {
		_ = os.RemoveAll(directory)
		return "", fmt.Errorf(
			"could not create an owner-private mutation runtime snapshot",
		)
	}
	return directory, nil
}

// Unsupported wrapper platforms never reclaim a syntactically valid owner by
// PID. Ownerless stale locks remain recoverable through the generic age rule.
func platformRuntimeSetLockProcessAlive(pid int) bool {
	return pid > 0
}

func platformRuntimeSetFileLinkCountOne(_ string, _ os.FileInfo) bool {
	return true
}
