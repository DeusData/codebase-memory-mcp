//go:build windows

package main

import (
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

const windowsExitedProcessProbeHelper = "CBM_GO_EXITED_PROCESS_PROBE_HELPER"

func TestWindowsExitedProcessProbeHelper(t *testing.T) {
	if os.Getenv(windowsExitedProcessProbeHelper) != "1" {
		t.Skip("helper process only")
	}
}

func TestRuntimeSetProcessProbeRejectsExitedOpenProcess(t *testing.T) {
	command := exec.Command(
		os.Args[0], "-test.run=^TestWindowsExitedProcessProbeHelper$",
	)
	command.Env = append(os.Environ(), windowsExitedProcessProbeHelper+"=1")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()

	handle, err := syscall.OpenProcess(
		syscall.SYNCHRONIZE, false, uint32(command.Process.Pid),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.CloseHandle(handle)
	event, err := syscall.WaitForSingleObject(handle, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if event != syscall.WAIT_OBJECT_0 {
		t.Fatalf("helper process wait result = %d, want WAIT_OBJECT_0", event)
	}

	// Keep both the exec.Cmd handle and our explicit handle open. OpenProcess can
	// still succeed in this state even though the process has already exited.
	if platformRuntimeSetLockProcessAlive(command.Process.Pid) {
		t.Fatal("exited process with a retained process object was reported live")
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	waited = true
}

func TestRuntimeSetProcessProbeAcceptsCurrentProcess(t *testing.T) {
	if !platformRuntimeSetLockProcessAlive(os.Getpid()) {
		t.Fatal("current process was reported dead")
	}
}

func TestRuntimeSetLiveOwnerReadAllowsReleaseRename(t *testing.T) {
	lock, err := acquireRuntimeSetLock(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if lock.file != nil {
			_ = releaseRuntimeSetLock(lock)
		}
	}()

	priorObserver := runtimeSetLockOwnerReadObserver
	defer func() { runtimeSetLockOwnerReadObserver = priorObserver }()
	ownerReadHeld := make(chan struct{})
	releaseOwnerRead := make(chan struct{})
	var firstOwnerRead atomic.Bool
	runtimeSetLockOwnerReadObserver = func() {
		if firstOwnerRead.CompareAndSwap(false, true) {
			close(ownerReadHeld)
			<-releaseOwnerRead
		}
	}

	waiterResult := make(chan bool, 1)
	contenderToken := strings.Repeat("b", runtimeSetLockTokenSize*2)
	go func() {
		waiterResult <- runtimeSetTryReclaimLock(lock.path, contenderToken)
	}()
	<-ownerReadHeld
	releaseErr := releaseRuntimeSetLock(lock)
	close(releaseOwnerRead)
	reclaimed := <-waiterResult
	if releaseErr != nil {
		t.Fatalf("release with live owner reader: %v", releaseErr)
	}
	if reclaimed {
		t.Fatal("waiter reclaimed a proven-live owner")
	}
}
