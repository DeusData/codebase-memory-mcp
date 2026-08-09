//go:build windows

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

const windowsFilePersistentACLs = 0x00000008

var (
	windowsAdvapi32                        = syscall.NewLazyDLL("advapi32.dll")
	windowsKernel32                        = syscall.NewLazyDLL("kernel32.dll")
	windowsConvertStringSecurityDescriptor = windowsAdvapi32.NewProc(
		"ConvertStringSecurityDescriptorToSecurityDescriptorW",
	)
	windowsGetVolumeInformationByHandle = windowsKernel32.NewProc(
		"GetVolumeInformationByHandleW",
	)
)

func windowsExtendedPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(absolute, `\\?\`) {
		return absolute, nil
	}
	if strings.HasPrefix(absolute, `\\.\`) {
		return "", fmt.Errorf("refusing Windows device path: %s", path)
	}
	if strings.HasPrefix(absolute, `\\`) {
		return `\\?\UNC\` + strings.TrimPrefix(absolute, `\\`), nil
	}
	return `\\?\` + absolute, nil
}

func platformOpenRuntimeSetLockOwner(path string) (*os.File, error) {
	extended, err := windowsExtendedPath(path)
	if err != nil {
		return nil, err
	}
	pointer, err := syscall.UTF16PtrFromString(extended)
	if err != nil {
		return nil, err
	}
	handle, err := syscall.CreateFile(
		pointer,
		syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = syscall.CloseHandle(handle)
		return nil, syscall.EINVAL
	}
	return file, nil
}

func windowsPrivateDirectorySecurityDescriptor() (uintptr, error) {
	token, err := syscall.OpenCurrentProcessToken()
	if err != nil {
		return 0, err
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil {
		return 0, err
	}
	userSID, err := user.User.Sid.String()
	if err != nil {
		return 0, err
	}
	// Protected DACL: the explicit current user owns the directory and that
	// user, LocalSystem, and administrators have full control.
	// Object/container inheritance applies the same policy to every runtime
	// leaf created inside the snapshot.
	sddl, err := syscall.UTF16PtrFromString(
		"O:" + userSID +
			"D:P(A;OICI;FA;;;" + userSID + ")" +
			"(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)",
	)
	if err != nil {
		return 0, err
	}
	var descriptor uintptr
	result, _, callErr := windowsConvertStringSecurityDescriptor.Call(
		uintptr(unsafe.Pointer(sddl)),
		1,
		uintptr(unsafe.Pointer(&descriptor)),
		0,
	)
	if result == 0 {
		if callErr != nil && callErr != syscall.Errno(0) {
			return 0, callErr
		}
		return 0, syscall.EINVAL
	}
	return descriptor, nil
}

func windowsDirectoryVolumePersistsACLs(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	var flags uint32
	result, _, callErr := windowsGetVolumeInformationByHandle.Call(
		directory.Fd(),
		0,
		0,
		0,
		0,
		uintptr(unsafe.Pointer(&flags)),
		0,
		0,
	)
	if result == 0 {
		if callErr != nil && callErr != syscall.Errno(0) {
			return callErr
		}
		return syscall.EINVAL
	}
	if flags&windowsFilePersistentACLs == 0 {
		return fmt.Errorf("temporary volume does not persist Windows ACLs")
	}
	return nil
}

func platformCreatePrivateMutationRuntimeDirectory() (string, error) {
	descriptor, err := windowsPrivateDirectorySecurityDescriptor()
	if err != nil {
		return "", err
	}
	defer syscall.LocalFree(syscall.Handle(descriptor))
	attributes := syscall.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(syscall.SecurityAttributes{})),
		SecurityDescriptor: descriptor,
	}
	// The protected child DACL makes the snapshot owner-private on a volume
	// with persistent ACLs. It cannot prevent a principal that already holds
	// FILE_DELETE_CHILD on a hostile shared TEMP parent from replacing the
	// pathname, so the supported boundary remains a normal per-user TEMP root.
	for attempt := 0; attempt < 128; attempt++ {
		token, err := runtimeSetLockToken()
		if err != nil {
			return "", err
		}
		directory := filepath.Join(
			os.TempDir(), "codebase-memory-mcp-mutation-"+token,
		)
		extended, err := windowsExtendedPath(directory)
		if err != nil {
			return "", err
		}
		pointer, err := syscall.UTF16PtrFromString(extended)
		if err != nil {
			return "", err
		}
		if err := syscall.CreateDirectory(pointer, &attributes); err != nil {
			if err == syscall.ERROR_ALREADY_EXISTS {
				continue
			}
			return "", err
		}
		if err := windowsDirectoryVolumePersistsACLs(directory); err != nil {
			_ = os.RemoveAll(directory)
			return "", err
		}
		return directory, nil
	}
	return "", fmt.Errorf("could not reserve a private mutation runtime snapshot")
}

func platformRuntimeSetLockProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if uint64(pid) > uint64(^uint32(0)) {
		// An out-of-range value cannot be probed without truncation, so it is not
		// evidence that an otherwise valid owner is dead.
		return true
	}
	// A process object can remain open after its process exits. SYNCHRONIZE lets
	// us distinguish that signaled object from a running process instead of
	// treating every successful OpenProcess call as proof that the PID is live.
	handle, err := syscall.OpenProcess(syscall.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		// ERROR_INVALID_PARAMETER is the documented result for a process
		// identifier whose object no longer exists. Access denial and every other
		// probe error are conservatively treated as live.
		return err != syscall.Errno(87)
	}
	event, waitErr := syscall.WaitForSingleObject(handle, 0)
	closeErr := syscall.CloseHandle(handle)
	if waitErr != nil || closeErr != nil {
		return true
	}
	switch event {
	case syscall.WAIT_OBJECT_0:
		return false
	case syscall.WAIT_TIMEOUT:
		return true
	default:
		return true
	}
}

func platformRuntimeSetFileLinkCountOne(path string, _ os.FileInfo) bool {
	// os.Open applies Go's absolute long-path handling before CreateFileW.
	// Passing an ordinary UTF-16 path directly to syscall.CreateFile would
	// reject otherwise valid custom cache paths beyond MAX_PATH.
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	var information syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(
		syscall.Handle(file.Fd()), &information,
	); err != nil {
		return false
	}
	return information.NumberOfLinks == 1
}
