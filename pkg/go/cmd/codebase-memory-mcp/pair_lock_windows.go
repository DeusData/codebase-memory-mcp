//go:build windows

package main

import "syscall"

func cbmWindowsLockProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	// PROCESS_QUERY_LIMITED_INFORMATION is sufficient for an existence probe and
	// is available on every Windows version supported by the package wrapper.
	handle, err := syscall.OpenProcess(0x1000, false, uint32(pid))
	if err == nil {
		_ = syscall.CloseHandle(handle)
		return true
	}
	// ERROR_INVALID_PARAMETER is the documented result for a process identifier
	// that no longer exists. Access denial is conservatively treated as live.
	return err != syscall.Errno(87)
}
