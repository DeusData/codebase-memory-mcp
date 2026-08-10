package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type archiveTestRoundTripper func(*http.Request) (*http.Response, error)

func (transport archiveTestRoundTripper) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	return transport(request)
}

type archiveCountingBody struct {
	reader io.Reader
	read   int64
}

func (body *archiveCountingBody) Read(buffer []byte) (int, error) {
	count, err := body.reader.Read(buffer)
	body.read += int64(count)
	return count, err
}

func (*archiveCountingBody) Close() error { return nil }

func assertRuntimeTag(t *testing.T, directory, binaryName, tag string) {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(directory, binaryName))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "binary:"+tag {
		t.Fatalf("%s = %q, want %q", binaryName, contents, "binary:"+tag)
	}
}

func writeTestRuntimeSet(t *testing.T, directory, binaryName, tag string) {
	t.Helper()
	if err := os.MkdirAll(directory, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(directory, binaryName), []byte("binary:"+tag), 0755,
	); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsUsesOneDirectBinary(t *testing.T) {
	binary := filepath.Join("cache", version, "ui", windowsBinaryName)
	if got := executionPathForOS(binary, "windows"); got != binary {
		t.Fatalf("Windows execution path = %q, want direct binary %q", got, binary)
	}
	if got := binaryNameForOS("windows"); got != "codebase-memory-mcp.exe" {
		t.Fatalf("Windows binary name = %q", got)
	}
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, obsolete := range []string{"payload", "launcher"} {
		if strings.Contains(strings.ToLower(string(source)), obsolete) {
			t.Fatalf("Go wrapper still contains obsolete %q model", obsolete)
		}
	}
}

func TestCacheSensitiveMutationActionIsExplicit(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{args: []string{"update", "--yes"}, want: "update"},
		{args: []string{"uninstall", "--yes"}, want: "uninstall"},
		{args: []string{"install", "--yes"}, want: "install"},
		{args: []string{"cli", "update"}, want: ""},
		{args: []string{"daemon", "update"}, want: ""},
	}
	for _, testCase := range cases {
		if got := runtimeMutationAction(testCase.args); got != testCase.want {
			t.Errorf(
				"runtimeMutationAction(%q) = %q, want %q",
				testCase.args,
				got,
				testCase.want,
			)
		}
	}
}

func TestWrapperUninstallNeverDefaultsToItsCacheBinary(t *testing.T) {
	custom := []string{"uninstall", "--dir", filepath.Join("custom", "bin")}
	if got := nativeArgs(custom); !reflect.DeepEqual(got, custom) {
		t.Fatalf("custom uninstall args = %q, want %q", got, custom)
	}
	got := nativeArgs([]string{"uninstall", "--yes"})
	wantBase := "bin"
	if runtime.GOOS == "windows" {
		wantBase = "codebase-memory-mcp"
	}
	if len(got) != 4 || got[2] != "--dir" ||
		filepath.Base(got[3]) != wantBase {
		t.Fatalf("default uninstall args do not target managed install: %q", got)
	}
}

func writeTarGz(t *testing.T, archivePath string, names []string) {
	t.Helper()
	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	tw := tar.NewWriter(gz)
	for _, name := range names {
		contents := []byte("contents:" + name)
		header := &tar.Header{
			Name:     name,
			Mode:     0600,
			Size:     int64(len(contents)),
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(contents); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCompressedArchiveDownloadRejectsDeclaredAndActualOverflow(t *testing.T) {
	const maxBytes = int64(8)
	priorClient := httpsOnlyClient
	defer func() { httpsOnlyClient = priorClient }()

	t.Run("declared", func(t *testing.T) {
		httpsOnlyClient = &http.Client{Transport: archiveTestRoundTripper(
			func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode:    http.StatusOK,
					Body:          io.NopCloser(strings.NewReader("")),
					ContentLength: maxBytes + 1,
					Header:        make(http.Header),
					Request:       request,
				}, nil
			},
		)}
		destination := filepath.Join(t.TempDir(), "release.tar.gz")
		err := httpGetWithLimit(
			"https://example.invalid/release.tar.gz", destination, maxBytes,
		)
		if err == nil || !strings.Contains(err.Error(), "compressed safety limit") {
			t.Fatalf("compressed declared overflow error = %v", err)
		}
		if _, statErr := os.Stat(destination); !os.IsNotExist(statErr) {
			t.Fatal("declared oversized archive created a download")
		}
	})

	t.Run("actual", func(t *testing.T) {
		body := &archiveCountingBody{reader: bytes.NewReader(
			bytes.Repeat([]byte("x"), 1024),
		)}
		httpsOnlyClient = &http.Client{Transport: archiveTestRoundTripper(
			func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode:    http.StatusOK,
					Body:          body,
					ContentLength: -1,
					Header:        make(http.Header),
					Request:       request,
				}, nil
			},
		)}
		destination := filepath.Join(t.TempDir(), "release.tar.gz")
		err := httpGetWithLimit(
			"https://example.invalid/release.tar.gz", destination, maxBytes,
		)
		if err == nil || !strings.Contains(err.Error(), "compressed safety limit") {
			t.Fatalf("compressed actual overflow error = %v", err)
		}
		if body.read != maxBytes+1 {
			t.Fatalf("oversized response bytes consumed = %d, want %d", body.read, maxBytes+1)
		}
		if _, statErr := os.Stat(destination); !os.IsNotExist(statErr) {
			t.Fatal("rejected compressed archive left a partial download")
		}
	})
}

func testArchiveLimits(
	members int, memberBytes, expandedBytes int64,
) archiveResourceLimits {
	return archiveResourceLimits{
		compressedBytes: 1024 * 1024,
		members:         members,
		memberBytes:     memberBytes,
		expandedBytes:   expandedBytes,
	}
}

func TestTarArchiveRejectsMemberAndExpandedResourceOverflow(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name      string
		names     []string
		limits    archiveResourceLimits
		wantError string
	}{
		{
			name:      "member count",
			names:     []string{"one", "two", "three"},
			limits:    testArchiveLimits(2, 1024, 4096),
			wantError: "member safety limit",
		},
		{
			name:      "declared member bytes",
			names:     []string{"oversized"},
			limits:    testArchiveLimits(4, 4, 4096),
			wantError: "expanded safety limit",
		},
		{
			name:      "declared aggregate bytes",
			names:     []string{"one", "two"},
			limits:    testArchiveLimits(4, 1024, 15),
			wantError: "aggregate expanded safety limit",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			archivePath := filepath.Join(root, strings.ReplaceAll(testCase.name, " ", "-")+".tar.gz")
			writeTarGz(t, archivePath, testCase.names)
			_, err := tarGzMemberNamesWithLimits(archivePath, testCase.limits)
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("tar resource overflow error = %v", err)
			}
		})
	}
}

func TestArchiveCopyRejectsActualMemberAggregateAndMetadataMismatch(t *testing.T) {
	tests := []struct {
		name      string
		contents  string
		declared  int64
		actual    int64
		limits    archiveResourceLimits
		wantError string
	}{
		{
			name:      "actual member bytes",
			contents:  "12345",
			declared:  5,
			limits:    testArchiveLimits(1, 4, 10),
			wantError: "actual expanded safety limit",
		},
		{
			name:      "actual aggregate bytes",
			contents:  "12",
			declared:  2,
			actual:    4,
			limits:    testArchiveLimits(1, 10, 5),
			wantError: "aggregate actual expanded safety limit",
		},
		{
			name:      "declared actual mismatch",
			contents:  "123",
			declared:  2,
			limits:    testArchiveLimits(1, 10, 10),
			wantError: "does not match declared size",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			actual := testCase.actual
			var output bytes.Buffer
			err := copyArchiveMemberWithLimits(
				&output,
				strings.NewReader(testCase.contents),
				"member",
				testCase.declared,
				&actual,
				testCase.limits,
			)
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("actual archive overflow error = %v", err)
			}
		})
	}
}

func writeZip(t *testing.T, archivePath string, names []string) {
	t.Helper()
	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(file)
	for _, name := range names {
		member, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := member.Write([]byte("contents:" + name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestZipArchiveRejectsMemberAndExpandedResourceOverflow(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name      string
		names     []string
		limits    archiveResourceLimits
		wantError string
	}{
		{
			name:      "member count",
			names:     []string{"one", "two", "three"},
			limits:    testArchiveLimits(2, 1024, 4096),
			wantError: "member safety limit",
		},
		{
			name:      "declared member bytes",
			names:     []string{"oversized"},
			limits:    testArchiveLimits(4, 4, 4096),
			wantError: "expanded safety limit",
		},
		{
			name:      "declared aggregate bytes",
			names:     []string{"one", "two"},
			limits:    testArchiveLimits(4, 1024, 15),
			wantError: "aggregate expanded safety limit",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			archivePath := filepath.Join(root, strings.ReplaceAll(testCase.name, " ", "-")+".zip")
			writeZip(t, archivePath, testCase.names)
			destination := filepath.Join(root, strings.ReplaceAll(testCase.name, " ", "-")+"-extract")
			if err := os.Mkdir(destination, 0755); err != nil {
				t.Fatal(err)
			}
			_, err := extractZipWithLimits(
				archivePath,
				destination,
				testCase.names,
				testCase.names[:1],
				testCase.limits,
			)
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("zip resource overflow error = %v", err)
			}
		})
	}
}

func verifyTestBinary(path string) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(string(contents), "binary:") {
		return fmt.Errorf("invalid test binary")
	}
	return nil
}

func TestMutationSnapshotReleasesCacheLockAndCleansAfterLaunchFailure(t *testing.T) {
	directory := t.TempDir()
	binary := "codebase-memory-mcp"
	executable := filepath.Join(directory, binary)
	writeTestRuntimeSet(t, directory, binary, "cached")
	var snapshotExecutable string
	runnerCalled := false
	err := execBinaryWithRuntimeLockAndRunner(
		executable,
		[]string{"install", "--yes"},
		verifyTestBinary,
		func(candidate string, args []string) error {
			runnerCalled = true
			snapshotExecutable = candidate
			if reflect.DeepEqual(args, []string{"install", "--yes"}) == false {
				t.Fatalf("snapshot mutation args = %q", args)
			}
			if filepath.Dir(candidate) == directory {
				t.Fatal("mutation launched from the shared package cache")
			}
			status, err := os.Lstat(filepath.Dir(candidate))
			if err != nil {
				t.Fatal(err)
			}
			if !status.IsDir() ||
				(runtime.GOOS != "windows" && status.Mode().Perm()&0077 != 0) {
				t.Fatalf("mutation snapshot mode = %v, want owner-private", status.Mode())
			}
			assertRuntimeTag(
				t, filepath.Dir(candidate), binary, "cached",
			)
			if _, err := os.Stat(filepath.Join(
				directory, runtimeSetLockName,
			)); !os.IsNotExist(err) {
				t.Fatal("cache lock remained held when snapshot runner started")
			}
			if err := os.WriteFile(
				executable, []byte("binary:successor"), 0755,
			); err != nil {
				t.Fatal(err)
			}
			contents, err := os.ReadFile(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if string(contents) != "binary:cached" {
				t.Fatalf("cache mutation changed private snapshot: %q", contents)
			}
			return fmt.Errorf("injected snapshot launch failure")
		},
	)
	if err == nil || !strings.Contains(err.Error(), "injected snapshot launch failure") {
		t.Fatalf("mutation launch failure = %v", err)
	}
	if !runnerCalled {
		t.Fatal("verified mutation snapshot was not launched")
	}
	if _, err := os.Stat(filepath.Dir(snapshotExecutable)); !os.IsNotExist(err) {
		t.Fatal("failed mutation left its private runtime snapshot")
	}
	if _, err := os.Stat(filepath.Join(
		directory, runtimeSetLockName,
	)); !os.IsNotExist(err) {
		t.Fatal("failed mutation left the package-cache lock")
	}
}

func TestMutationSnapshotEnvironmentIgnoresExternalAssetOverrides(t *testing.T) {
	t.Setenv("CBM_ASSETS_DIR", filepath.Join(t.TempDir(), "integrations"))
	t.Setenv("CBM_UI_ASSETS_DIR", filepath.Join(t.TempDir(), "ui"))
	for _, entry := range mutationSnapshotEnvironment() {
		name := entry
		if separator := strings.IndexByte(entry, '='); separator >= 0 {
			name = entry[:separator]
		}
		if strings.EqualFold(name, "CBM_ASSETS_DIR") ||
			strings.EqualFold(name, "CBM_UI_ASSETS_DIR") {
			t.Fatalf("mutation snapshot environment retained %q", entry)
		}
	}
}

func TestMutationSnapshotRejectsExplicitTargetOverlappingCache(t *testing.T) {
	directory := t.TempDir()
	executable := filepath.Join(directory, "codebase-memory-mcp")
	for _, args := range [][]string{
		{"install", "--dir", directory},
		{"uninstall", "--dir=" + directory},
	} {
		runnerCalled := false
		err := execBinaryWithRuntimeLockAndRunner(
			executable,
			args,
			nil,
			func(string, []string) error {
				runnerCalled = true
				return nil
			},
		)
		if err == nil || !strings.Contains(err.Error(), "overlaps the shared package cache") {
			t.Fatalf("overlapping mutation target error = %v", err)
		}
		if runnerCalled {
			t.Fatal("overlapping cache mutation target was launched")
		}
	}
}

func TestMutationSnapshotVerifierFailureNeverLaunches(t *testing.T) {
	directory := t.TempDir()
	binary := "codebase-memory-mcp"
	executable := filepath.Join(directory, binary)
	writeTestRuntimeSet(t, directory, binary, "cached")
	verifierCalls := 0
	verifier := func(path string) error {
		verifierCalls++
		if filepath.Dir(path) != directory {
			return fmt.Errorf("injected private snapshot verification failure")
		}
		return verifyTestBinary(path)
	}
	runnerCalled := false
	err := execBinaryWithRuntimeLockAndRunner(
		executable,
		[]string{"install", "--yes"},
		verifier,
		func(string, []string) error {
			runnerCalled = true
			return nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "failed verification") {
		t.Fatalf("snapshot verifier failure = %v", err)
	}
	if verifierCalls != 2 {
		t.Fatalf("snapshot verifier calls = %d, want source and snapshot", verifierCalls)
	}
	if runnerCalled {
		t.Fatal("mutation runner was invoked after snapshot verification failure")
	}
	if _, err := os.Stat(filepath.Join(
		directory, runtimeSetLockName,
	)); !os.IsNotExist(err) {
		t.Fatal("snapshot verification failure left the package-cache lock")
	}
}

func TestMutationSnapshotCleanupFailurePreservesNativeResult(t *testing.T) {
	const exitHelper = "CBM_GO_MUTATION_EXIT_HELPER"
	if os.Getenv(exitHelper) == "1" {
		os.Exit(7)
	}
	exitCommand := exec.Command(
		os.Args[0],
		"-test.run=^TestMutationSnapshotCleanupFailurePreservesNativeResult$",
	)
	exitCommand.Env = append(os.Environ(), exitHelper+"=1")
	nativeErr := exitCommand.Run()
	exitErr, ok := nativeErr.(*exec.ExitError)
	if !ok {
		t.Fatalf("exit helper error = %T %v", nativeErr, nativeErr)
	}

	priorCleanup := runtimeMutationSnapshotCleanup
	defer func() { runtimeMutationSnapshotCleanup = priorCleanup }()
	runtimeMutationSnapshotCleanup = func(path string) error {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
		return errors.New("injected private snapshot cleanup failure")
	}
	for _, testCase := range []struct {
		name       string
		runnerErr  error
		wantResult error
	}{
		{name: "native success remains success"},
		{name: "native exit error identity survives", runnerErr: exitErr, wantResult: exitErr},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := t.TempDir()
			binary := "codebase-memory-mcp"
			writeTestRuntimeSet(t, directory, binary, "cached")
			result := execBinaryWithRuntimeLockAndRunner(
				filepath.Join(directory, binary),
				[]string{"install", "--yes"},
				verifyTestBinary,
				func(string, []string) error { return testCase.runnerErr },
			)
			if result != testCase.wantResult {
				t.Fatalf("native result identity changed: got %v, want %v", result, testCase.wantResult)
			}
			if exitErr != nil && testCase.wantResult != nil {
				var recovered *exec.ExitError
				if !errors.As(result, &recovered) || recovered != exitErr {
					t.Fatal("native ExitError was not preserved through cleanup failure")
				}
			}
		})
	}
}

func TestConcurrentRuntimePublishersAreSerialized(t *testing.T) {
	root := t.TempDir()
	firstSource := filepath.Join(root, "source-first")
	secondSource := filepath.Join(root, "source-second")
	destination := filepath.Join(root, "destination")
	binary := "codebase-memory-mcp"
	writeTestRuntimeSet(t, firstSource, binary, "first")
	writeTestRuntimeSet(t, secondSource, binary, "second")

	firstPaused := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondWaiting := make(chan struct{})
	secondEnteredPublication := make(chan struct{})
	firstResult := make(chan error, 1)
	secondResult := make(chan error, 1)
	var pauseOnce sync.Once
	var waitOnce sync.Once
	var enteredOnce sync.Once
	priorObserver := runtimeSetLockWaitObserver
	runtimeSetLockWaitObserver = func() {
		waitOnce.Do(func() { close(secondWaiting) })
	}
	defer func() { runtimeSetLockWaitObserver = priorObserver }()

	firstRenamer := func(sourcePath, destinationPath string) error {
		if err := os.Rename(sourcePath, destinationPath); err != nil {
			return err
		}
		// One file publishes now, so the mid-publication pause hooks the binary
		// rename itself: the runtime lock is still held until publish returns.
		if destinationPath == filepath.Join(destination, binary) {
			pauseOnce.Do(func() {
				close(firstPaused)
				<-releaseFirst
			})
		}
		return nil
	}
	secondRenamer := func(sourcePath, destinationPath string) error {
		name := filepath.Base(destinationPath)
		if filepath.Dir(destinationPath) == destination && name == binary {
			enteredOnce.Do(func() { close(secondEnteredPublication) })
		}
		return os.Rename(sourcePath, destinationPath)
	}

	go func() {
		firstResult <- publishRuntimeSetWithRecoveryAndRenamer(
			firstSource,
			destination,
			binary,
			verifyTestBinary,
			firstRenamer,
		)
	}()
	select {
	case <-firstPaused:
	case <-time.After(5 * time.Second):
		t.Fatal("first publisher did not reach the held publication gate")
	}

	go func() {
		secondResult <- publishRuntimeSetWithRecoveryAndRenamer(
			secondSource,
			destination,
			binary,
			verifyTestBinary,
			secondRenamer,
		)
	}()

	concurrentPublication := false
	var secondErr error
	select {
	case <-secondWaiting:
		// The serialized implementation reaches this branch while the first
		// publisher still owns the held publication gate.
	case <-secondEnteredPublication:
		concurrentPublication = true
		select {
		case secondErr = <-secondResult:
		case <-time.After(5 * time.Second):
			close(releaseFirst)
			t.Fatal("concurrent second publisher did not finish")
		}
	case <-time.After(5 * time.Second):
		close(releaseFirst)
		t.Fatal("second publisher neither waited nor entered publication")
	}
	close(releaseFirst)

	var firstErr error
	select {
	case firstErr = <-firstResult:
	case <-time.After(5 * time.Second):
		t.Fatal("first publisher did not finish after its gate was released")
	}
	if !concurrentPublication {
		select {
		case secondErr = <-secondResult:
		case <-time.After(5 * time.Second):
			t.Fatal("serialized second publisher did not finish")
		}
	}
	if concurrentPublication {
		t.Fatal("second publisher entered publication while the first sequence was held")
	}
	if firstErr != nil {
		t.Fatalf("first publisher failed: %v", firstErr)
	}
	if secondErr != nil {
		t.Fatalf("serialized second publisher failed: %v", secondErr)
	}
	assertRuntimeTag(t, destination, binary, "first")
}

func TestKilledRuntimePublisherIsReconciledByLockedReadiness(t *testing.T) {
	for _, testCase := range []struct {
		name                  string
		crashPhase            string
		expectedReady         bool
		expectedBackupMembers int
		expectRetiredMarker   bool
		expectCleanupMarker   bool
	}{
		{
			name:                  "executable retired before other leaves",
			crashPhase:            "retired-executable",
			expectedReady:         false,
			expectedBackupMembers: 1,
		},
		{
			name:                  "all leaves retired before retirement marker",
			crashPhase:            runtimeBackupBeforeMarkerEvent,
			expectedReady:         false,
			expectedBackupMembers: 1,
		},
		{
			name:                  "complete publish before cleanup",
			crashPhase:            "published-binary",
			expectedReady:         true,
			expectedBackupMembers: 1,
			expectRetiredMarker:   true,
		},
		{
			name:                  "cleanup interrupted after one retired member",
			crashPhase:            runtimeBackupCleanupRemovedEvent,
			expectedReady:         true,
			expectedBackupMembers: 0,
			expectRetiredMarker:   true,
			expectCleanupMarker:   true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			destination := filepath.Join(root, "destination")
			marker := filepath.Join(root, "crash-reached")
			binary := "codebase-memory-mcp"
			writeTestRuntimeSet(t, source, binary, "candidate")
			writeTestRuntimeSet(t, destination, binary, "old")
			if err := os.WriteFile(
				filepath.Join(destination, binary), []byte("corrupt:old"), 0755,
			); err != nil {
				t.Fatal(err)
			}

			command := exec.Command(
				os.Args[0], "-test.run=^TestRuntimePublicationCrashHelper$",
			)
			command.Env = append(
				os.Environ(),
				"CBM_TEST_RUNTIME_CRASH_HELPER=1",
				"CBM_TEST_RUNTIME_CRASH_SOURCE="+source,
				"CBM_TEST_RUNTIME_CRASH_DESTINATION="+destination,
				"CBM_TEST_RUNTIME_CRASH_MARKER="+marker,
				"CBM_TEST_RUNTIME_CRASH_PHASE="+testCase.crashPhase,
			)
			var stderr bytes.Buffer
			command.Stderr = &stderr
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waitResult := make(chan error, 1)
			go func() { waitResult <- command.Wait() }()
			finished := false
			defer func() {
				if !finished {
					_ = command.Process.Kill()
					<-waitResult
				}
			}()

			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(marker); err == nil {
					break
				}
				select {
				case err := <-waitResult:
					finished = true
					t.Fatalf(
						"publication helper exited before crash gate: %v: %s",
						err, stderr.String(),
					)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatalf(
						"publication helper did not reach crash gate: %s",
						stderr.String(),
					)
				}
				time.Sleep(10 * time.Millisecond)
			}

			entries, err := os.ReadDir(destination)
			if err != nil {
				t.Fatal(err)
			}
			backupCount := 0
			for _, entry := range entries {
				if runtimeBackupDirectoryName(entry.Name()) {
					backupCount++
					if !entry.IsDir() {
						t.Fatal("killed publisher backup transaction is not a directory")
					}
					backupPath := filepath.Join(destination, entry.Name())
					backupEntries, err := os.ReadDir(backupPath)
					if err != nil {
						t.Fatal(err)
					}
					memberCount := 0
					for _, backupEntry := range backupEntries {
						if runtimeBackupTargetName(backupEntry.Name(), binary) {
							memberCount++
						}
					}
					if memberCount != testCase.expectedBackupMembers {
						t.Fatalf(
							"backup member count at crash gate = %d, want %d",
							memberCount, testCase.expectedBackupMembers,
						)
					}
					for markerName, expected := range map[string]bool{
						runtimeBackupRetired:     testCase.expectRetiredMarker,
						runtimeBackupCleanupOnly: testCase.expectCleanupMarker,
					} {
						_, markerErr := os.Stat(filepath.Join(backupPath, markerName))
						if expected && markerErr != nil {
							t.Fatalf("expected backup marker %s is missing: %v", markerName, markerErr)
						}
						if !expected && !os.IsNotExist(markerErr) {
							t.Fatalf("unexpected backup marker %s exists", markerName)
						}
					}
				}
			}
			if backupCount != 1 {
				t.Fatalf("killed publisher backup count = %d, want 1", backupCount)
			}

			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := <-waitResult; err == nil {
				t.Fatal("publication helper was not killed")
			}
			finished = true

			ready, err := runtimeSetReadyLocked(
				destination, binary, verifyTestBinary,
			)
			if err != nil {
				t.Fatal(err)
			}
			if ready != testCase.expectedReady {
				t.Fatalf(
					"reconciled readiness = %v, want %v",
					ready, testCase.expectedReady,
				)
			}
			if !ready {
				contents, err := os.ReadFile(filepath.Join(destination, binary))
				if err != nil {
					t.Fatal(err)
				}
				if string(contents) != "corrupt:old" {
					t.Fatalf("recovered prior binary = %q", contents)
				}
				if err := publishRuntimeSetWithRecovery(
					source, destination, binary, verifyTestBinary,
				); err != nil {
					t.Fatal(err)
				}
			}
			assertRuntimeTag(
				t, destination, binary, "candidate",
			)
			entries, err = os.ReadDir(destination)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), runtimeBackupPrefix) {
					t.Fatalf("orphan backup survived recovery: %s", entry.Name())
				}
			}
			if _, err := os.Stat(filepath.Join(
				destination, runtimeSetLockName,
			)); !os.IsNotExist(err) {
				t.Fatal("runtime-set lock survived killed-process recovery")
			}
		})
	}
}

func TestRuntimePublicationCrashHelper(t *testing.T) {
	if os.Getenv("CBM_TEST_RUNTIME_CRASH_HELPER") != "1" {
		return
	}
	source := os.Getenv("CBM_TEST_RUNTIME_CRASH_SOURCE")
	destination := os.Getenv("CBM_TEST_RUNTIME_CRASH_DESTINATION")
	marker := os.Getenv("CBM_TEST_RUNTIME_CRASH_MARKER")
	crashPhase := os.Getenv("CBM_TEST_RUNTIME_CRASH_PHASE")
	binary := "codebase-memory-mcp"
	reachCrashGate := func() error {
		if err := os.WriteFile(marker, []byte("reached\n"), 0600); err != nil {
			return err
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	priorCrashObserver := runtimeBackupCrashObserver
	defer func() { runtimeBackupCrashObserver = priorCrashObserver }()
	runtimeBackupCrashObserver = func(event, _ string) error {
		if event == crashPhase {
			return reachCrashGate()
		}
		return nil
	}
	renameFile := func(sourcePath, destinationPath string) error {
		if err := os.Rename(sourcePath, destinationPath); err != nil {
			return err
		}
		backupParent := filepath.Base(filepath.Dir(destinationPath))
		if crashPhase == "retired-executable" &&
			runtimeBackupDirectoryName(backupParent) &&
			filepath.Base(destinationPath) == binary {
			return reachCrashGate()
		}
		if crashPhase == "published-binary" &&
			destinationPath == filepath.Join(destination, binary) {
			return reachCrashGate()
		}
		return nil
	}
	if err := publishRuntimeSetWithRecoveryAndRenamer(
		source,
		destination,
		binary,
		verifyTestBinary,
		renameFile,
	); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeReadinessRejectsMultiplyLinkedLeaves(t *testing.T) {
	directory := t.TempDir()
	binary := "codebase-memory-mcp"
	writeTestRuntimeSet(t, directory, binary, "linked")
	if err := os.Link(
		filepath.Join(directory, binary),
		filepath.Join(directory, "binary-hardlink"),
	); err != nil {
		t.Skipf("filesystem does not support hard links: %v", err)
	}
	if runtimeSetReady(directory, binary, verifyTestBinary) {
		t.Fatal("runtime set accepted a multiply-linked binary leaf")
	}
}

func TestRuntimeSetLockReclaimsOnlyDefinitelyDeadOwner(t *testing.T) {
	destination := t.TempDir()
	lockPath := filepath.Join(destination, runtimeSetLockName)
	if err := os.Mkdir(lockPath, 0700); err != nil {
		t.Fatal(err)
	}
	ownerToken := strings.Repeat("a", runtimeSetLockTokenSize*2)
	if err := writeRuntimeSetLockOwner(lockPath, ownerToken); err != nil {
		t.Fatal(err)
	}
	contenderToken := strings.Repeat("b", runtimeSetLockTokenSize*2)
	priorProcessAlive := runtimeSetProcessAlive
	defer func() { runtimeSetProcessAlive = priorProcessAlive }()

	runtimeSetProcessAlive = func(int) bool { return true }
	if runtimeSetTryReclaimLock(lockPath, contenderToken) {
		t.Fatal("runtime-set lock reclaimed an owner that may still be live")
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("live owner's runtime-set lock was damaged: %v", err)
	}

	runtimeSetProcessAlive = func(int) bool { return false }
	if !runtimeSetTryReclaimLock(lockPath, contenderToken) {
		t.Fatal("runtime-set lock did not reclaim a definitely dead owner")
	}
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatal("reclaimed dead-owner runtime-set lock still exists")
	}
}

func TestRuntimeSetLiveOwnerSkipsIdentityCapture(t *testing.T) {
	destination := t.TempDir()
	lock, err := acquireRuntimeSetLock(destination)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			if err := releaseRuntimeSetLock(lock); err != nil {
				t.Errorf("release live runtime-set lock: %v", err)
			}
		}
	}()

	priorObserver := runtimeSetLockCaptureObserver
	priorProcessAlive := runtimeSetProcessAlive
	defer func() {
		runtimeSetLockCaptureObserver = priorObserver
		runtimeSetProcessAlive = priorProcessAlive
	}()
	captures := 0
	runtimeSetLockCaptureObserver = func() { captures++ }
	runtimeSetProcessAlive = func(int) bool { return true }

	contenderToken := strings.Repeat("b", runtimeSetLockTokenSize*2)
	if runtimeSetTryReclaimLock(lock.path, contenderToken) {
		t.Fatal("runtime-set lock reclaimed a proven-live owner")
	}
	if captures != 0 {
		t.Fatalf("live runtime-set lock identity was captured %d times", captures)
	}
	if err := releaseRuntimeSetLock(lock); err != nil {
		t.Fatalf("release live runtime-set lock: %v", err)
	}
	released = true
}

func TestRuntimeSetLockReclaimsOnlyStaleOwnerlessDirectory(t *testing.T) {
	destination := t.TempDir()
	lockPath := filepath.Join(destination, runtimeSetLockName)
	if err := os.Mkdir(lockPath, 0700); err != nil {
		t.Fatal(err)
	}
	contenderToken := strings.Repeat("c", runtimeSetLockTokenSize*2)
	if runtimeSetTryReclaimLock(lockPath, contenderToken) {
		t.Fatal("fresh ownerless runtime-set lock was reclaimed")
	}
	staleTime := time.Now().Add(-runtimeSetOwnerlessStale - time.Second)
	if err := os.Chtimes(lockPath, staleTime, staleTime); err != nil {
		t.Fatal(err)
	}
	if !runtimeSetTryReclaimLock(lockPath, contenderToken) {
		t.Fatal("stale ownerless runtime-set lock was not reclaimed")
	}
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatal("reclaimed ownerless runtime-set lock still exists")
	}
}

func TestExpiredLeaseNeverStealsProvenLiveOwner(t *testing.T) {
	destination := t.TempDir()
	lockPath := filepath.Join(destination, runtimeSetLockName)
	record := runtimeSetLockOwnerRecord{
		PID: os.Getpid(), Token: strings.Repeat("a", runtimeSetLockTokenSize*2),
		LeaseExpires: time.Now().Add(-time.Second).UnixMilli(),
	}
	contents, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	contents = append(contents, '\n')
	if err := os.WriteFile(lockPath, contents, 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	priorProcessAlive := runtimeSetProcessAlive
	defer func() { runtimeSetProcessAlive = priorProcessAlive }()
	runtimeSetProcessAlive = func(int) bool { return true }
	contender := strings.Repeat("b", runtimeSetLockTokenSize*2)
	if runtimeSetTryReclaimLock(lockPath, contender) {
		t.Fatal("runtime-set lock stole an expired lease from a proven-live owner")
	}
	after, err := os.Lstat(lockPath)
	if err != nil {
		t.Fatalf("live owner's expired lock was removed: %v", err)
	}
	if !runtimeSetSameLockObject(before, after) {
		t.Fatal("live owner's expired lock identity changed")
	}
	afterContents, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterContents, contents) {
		t.Fatalf("live owner's expired lock contents changed: %q", afterContents)
	}
}

func TestLiveLegacyLockIsNotReclaimedSolelyByAge(t *testing.T) {
	destination := t.TempDir()
	lockPath := filepath.Join(destination, runtimeSetLockName)
	if err := os.Mkdir(lockPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeRuntimeSetLockOwner(
		lockPath, strings.Repeat("a", runtimeSetLockTokenSize*2),
	); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-runtimeSetLegacyStale - time.Second)
	if err := os.Chtimes(lockPath, stale, stale); err != nil {
		t.Fatal(err)
	}
	priorProcessAlive := runtimeSetProcessAlive
	defer func() { runtimeSetProcessAlive = priorProcessAlive }()
	runtimeSetProcessAlive = func(int) bool { return true }
	if runtimeSetTryReclaimLock(
		lockPath, strings.Repeat("b", runtimeSetLockTokenSize*2),
	) {
		t.Fatal("live legacy runtime-set lock was reclaimed solely by age")
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("live legacy runtime-set lock was damaged: %v", err)
	}
}

func TestStalledRuntimeLockCreatorNeverDeletesSuccessor(t *testing.T) {
	destination := t.TempDir()
	var successor *runtimeSetLock
	var successorErr error
	priorObserver := runtimeSetLockClaimObserver
	defer func() { runtimeSetLockClaimObserver = priorObserver }()
	runtimeSetLockClaimObserver = func() error {
		runtimeSetLockClaimObserver = nil
		successor, successorErr = acquireRuntimeSetLock(destination)
		return fmt.Errorf("injected stalled creator abort")
	}

	if first, err := acquireRuntimeSetLock(destination); err == nil {
		_ = releaseRuntimeSetLock(first)
		t.Fatal("stalled creator unexpectedly acquired over its successor")
	} else if !strings.Contains(err.Error(), "stalled creator abort") {
		t.Fatalf("stalled creator failed for wrong reason: %v", err)
	}
	if successorErr != nil || successor == nil {
		t.Fatalf("successor did not acquire reclaimed lock: %v", successorErr)
	}
	if _, err := os.Stat(successor.path); err != nil {
		t.Fatalf("failed creator deleted successor lock: %v", err)
	}
	if err := releaseRuntimeSetLock(successor); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeSetLockReleaseRetiresDescriptor(t *testing.T) {
	destination := t.TempDir()
	lock, err := acquireRuntimeSetLock(destination)
	if err != nil {
		t.Fatal(err)
	}
	if lock.file == nil {
		t.Fatal("acquired runtime-set lock has no writable descriptor")
	}
	if err := releaseRuntimeSetLock(lock); err != nil {
		t.Fatal(err)
	}
	if lock.file != nil {
		t.Fatal("released runtime-set lock retained its closed descriptor")
	}
	if _, err := os.Lstat(lock.path); !os.IsNotExist(err) {
		t.Fatalf("released runtime-set lock remained at its canonical path: %v", err)
	}
}

func TestWindowsLongRuntimePathPublishesAndBecomesReady(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows long-path regression")
	}
	root := t.TempDir()
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "runtime")
	for len(destination) <= 300 {
		destination = filepath.Join(
			destination, "long-runtime-directory-segment",
		)
	}
	if len(destination) <= 260 {
		t.Fatalf("test runtime path is not long: %d", len(destination))
	}
	binary := windowsBinaryName
	writeTestRuntimeSet(t, source, binary, "long-path")
	if err := publishRuntimeSetWithRecovery(
		source, destination, binary, verifyTestBinary,
	); err != nil {
		t.Fatalf("publication under a long Windows runtime path failed: %v", err)
	}
	ready, err := runtimeSetReadyLocked(
		destination, binary, verifyTestBinary,
	)
	if err != nil {
		t.Fatalf("locked readiness under a long Windows runtime path failed: %v", err)
	}
	if !ready {
		t.Fatal("published long-path Windows runtime set was not ready")
	}
}

func TestOrphanReconciliationRejectsMultiplyLinkedBackupMembers(t *testing.T) {
	directory := t.TempDir()
	binary := "codebase-memory-mcp"
	backup := filepath.Join(
		directory, runtimeBackupPrefix+strings.Repeat("a", 32),
	)
	if err := os.Mkdir(backup, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(backup, runtimeBackupRetired), nil, 0600,
	); err != nil {
		t.Fatal(err)
	}
	member := filepath.Join(backup, binary)
	if err := os.WriteFile(member, []byte("binary:old"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(
		member, filepath.Join(directory, "backup-hardlink-copy"),
	); err != nil {
		t.Skipf("filesystem does not support hard links: %v", err)
	}

	ready, err := runtimeSetReadyLocked(
		directory, binary, verifyTestBinary,
	)
	if err == nil || !strings.Contains(
		err.Error(), "unsafe package-cache backup member",
	) {
		t.Fatalf("unsafe orphan reconciliation = (%v, %v)", ready, err)
	}
	if _, err := os.Stat(backup); err != nil {
		t.Fatalf("unsafe backup was mutated: %v", err)
	}
	if _, err := os.Stat(filepath.Join(
		directory, runtimeSetLockName,
	)); !os.IsNotExist(err) {
		t.Fatal("runtime-set lock survived rejected orphan reconciliation")
	}
}

func TestRuntimeSetLockSerializesProcesses(t *testing.T) {
	const helperEnv = "CBM_GO_RUNTIME_LOCK_HELPER"
	if os.Getenv(helperEnv) == "1" {
		destination := os.Getenv("CBM_GO_RUNTIME_LOCK_DIRECTORY")
		waitingPath := os.Getenv("CBM_GO_RUNTIME_LOCK_WAITING")
		acquiredPath := os.Getenv("CBM_GO_RUNTIME_LOCK_ACQUIRED")
		var waitOnce sync.Once
		runtimeSetLockWaitObserver = func() {
			waitOnce.Do(func() {
				if err := os.WriteFile(waitingPath, []byte("waiting"), 0600); err != nil {
					t.Fatal(err)
				}
			})
		}
		lock, err := acquireRuntimeSetLock(destination)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(acquiredPath, []byte("acquired"), 0600); err != nil {
			_ = releaseRuntimeSetLock(lock)
			t.Fatal(err)
		}
		if err := releaseRuntimeSetLock(lock); err != nil {
			t.Fatal(err)
		}
		return
	}

	destination := t.TempDir()
	waitingPath := filepath.Join(t.TempDir(), "waiting")
	acquiredPath := filepath.Join(t.TempDir(), "acquired")
	lock, err := acquireRuntimeSetLock(destination)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			_ = releaseRuntimeSetLock(lock)
		}
	}()

	cmd := exec.Command(os.Args[0], "-test.run=^TestRuntimeSetLockSerializesProcesses$")
	cmd.Env = append(
		os.Environ(),
		helperEnv+"=1",
		"CBM_GO_RUNTIME_LOCK_DIRECTORY="+destination,
		"CBM_GO_RUNTIME_LOCK_WAITING="+waitingPath,
		"CBM_GO_RUNTIME_LOCK_ACQUIRED="+acquiredPath,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitResult := make(chan error, 1)
	go func() { waitResult <- cmd.Wait() }()
	childFinished := false
	defer func() {
		if !childFinished {
			_ = cmd.Process.Kill()
			<-waitResult
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case err := <-waitResult:
			childFinished = true
			t.Fatalf(
				"runtime-set lock helper exited before waiting: %v\nstdout: %s\nstderr: %s",
				err,
				stdout.String(),
				stderr.String(),
			)
		default:
		}
		if _, err := os.Stat(waitingPath); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if !time.Now().Before(deadline) {
			_ = cmd.Process.Kill()
			<-waitResult
			childFinished = true
			t.Fatalf(
				"child did not report waiting on parent lock\nstdout: %s\nstderr: %s",
				stdout.String(),
				stderr.String(),
			)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := os.Stat(acquiredPath); err == nil {
		t.Fatal("child acquired the runtime-set lock while the parent owned it")
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := releaseRuntimeSetLock(lock); err != nil {
		t.Fatal(err)
	}
	released = true
	select {
	case err := <-waitResult:
		childFinished = true
		if err != nil {
			t.Fatalf(
				"runtime-set lock helper failed: %v\nstdout: %s\nstderr: %s",
				err,
				stdout.String(),
				stderr.String(),
			)
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-waitResult
		childFinished = true
		t.Fatal("child did not acquire the released runtime-set lock")
	}
	if _, err := os.Stat(acquiredPath); err != nil {
		t.Fatalf("child never acquired the released runtime-set lock: %v", err)
	}
}
