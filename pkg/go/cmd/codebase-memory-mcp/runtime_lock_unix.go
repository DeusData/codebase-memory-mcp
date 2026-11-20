//go:build darwin || linux

package main

import (
	"fmt"
	"os"
	"syscall"
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

func platformRuntimeSetLockProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	// Only ESRCH proves the owner is gone. Permission denial and every other
	// result are conservatively treated as evidence that it may still be live.
	return err != syscall.ESRCH
}

func platformRuntimeSetFileLinkCountOne(_ string, info os.FileInfo) bool {
	status, ok := info.Sys().(*syscall.Stat_t)
	return ok && status.Nlink == 1
}
