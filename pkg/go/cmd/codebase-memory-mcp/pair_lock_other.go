//go:build !windows

package main

// Windows pair publication is never invoked by a non-Windows wrapper. Keeping
// the fallback conservative also lets platform-neutral transaction tests run
// without introducing a second process-liveness policy.
func cbmWindowsLockProcessAlive(pid int) bool {
	return pid > 0
}
