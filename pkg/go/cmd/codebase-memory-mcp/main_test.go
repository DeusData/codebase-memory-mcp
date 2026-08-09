package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
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
	"strconv"
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

func TestStandardAndUICachePathsDoNotCollide(t *testing.T) {
	t.Setenv("CBM_CACHE_DIR", t.TempDir())
	t.Setenv("CBM_VARIANT", "standard")
	standard := binPath()
	t.Setenv("CBM_VARIANT", "ui")
	ui := binPath()
	if standard == ui {
		t.Fatalf("standard and UI cache paths collide at %q", standard)
	}
	if filepath.Base(filepath.Dir(standard)) != "standard" ||
		filepath.Base(filepath.Dir(ui)) != "ui" {
		t.Fatalf("variant cache paths = %q and %q", standard, ui)
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

func TestArchiveNamespaceIsExactAndUIHasOnePack(t *testing.T) {
	binary := "codebase-memory-mcp"
	base := archiveNamesForOS("linux", binary)
	pack := "cbm-ui-" + strings.Repeat("a", 64) + ".pack"
	uiNames := append(append([]string(nil), base...), pack)
	got, err := validateArchiveMemberNames(uiNames, base, "ui", false)
	if err != nil {
		t.Fatal(err)
	}
	if got != pack {
		t.Fatalf("UI pack = %q, want %q", got, pack)
	}
	badCases := [][]string{
		append(append([]string(nil), uiNames...), "unexpected-root-file"),
		append(append([]string(nil), uiNames...),
			"cbm-ui-"+strings.Repeat("b", 64)+".pack"),
		base,
	}
	for _, names := range badCases {
		if _, err := validateArchiveMemberNames(names, base, "ui", false); err == nil {
			t.Fatalf("UI archive namespace accepted invalid members: %q", names)
		}
	}
	if _, err := validateArchiveMemberNames(uiNames, base, "standard", false); err == nil {
		t.Fatal("standard archive accepted a UI pack")
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

func TestTarExtractionValidatesNamespaceAndExtractsRuntimeSet(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "release.tar.gz")
	destination := filepath.Join(root, "extract")
	if err := os.Mkdir(destination, 0755); err != nil {
		t.Fatal(err)
	}
	binary := "codebase-memory-mcp"
	archiveNames := archiveNamesForOS("linux", binary)
	writeTarGz(t, archivePath, archiveNames)
	runtimeNames, err := extractTarGz(
		archivePath,
		destination,
		archiveNames,
		[]string{binary, integrationsFileName},
		"standard",
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{binary, integrationsFileName}
	if !reflect.DeepEqual(runtimeNames, want) {
		t.Fatalf("extracted runtime names = %q, want %q", runtimeNames, want)
	}
	if _, err := os.Stat(filepath.Join(destination, "LICENSE")); !os.IsNotExist(err) {
		t.Fatal("non-runtime LICENSE member was extracted")
	}

	badArchive := filepath.Join(root, "bad.tar.gz")
	writeTarGz(t, badArchive, append(archiveNames, "unexpected-root-file"))
	badDestination := filepath.Join(root, "bad-extract")
	if err := os.Mkdir(badDestination, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := extractTarGz(
		badArchive,
		badDestination,
		archiveNames,
		[]string{binary, integrationsFileName},
		"standard",
	); err == nil {
		t.Fatal("tar extraction accepted an unexpected root member")
	}
}

func TestTarExtractionRejectsHardlinkMember(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "hardlink.tar.gz")
	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{
		Name: "codebase-memory-mcp", Typeflag: tar.TypeLink,
		Linkname: "cbm-integrations.json", Mode: 0755,
	}); err != nil {
		t.Fatal(err)
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
	if _, err := tarGzMemberNames(archivePath); err == nil {
		t.Fatal("tar namespace accepted a hardlink member")
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

func TestUIZipExtractionIncludesOneContentAddressedPack(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "release.zip")
	destination := filepath.Join(root, "extract")
	if err := os.Mkdir(destination, 0755); err != nil {
		t.Fatal(err)
	}
	pack := "cbm-ui-" + strings.Repeat("a", 64) + ".pack"
	archiveNames := archiveNamesForOS("windows", windowsBinaryName)
	writeZip(t, archivePath, append(archiveNames, pack))
	runtimeNames, err := extractZip(
		archivePath,
		destination,
		archiveNames,
		[]string{windowsBinaryName, integrationsFileName},
		"ui",
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{windowsBinaryName, integrationsFileName, pack}
	if !reflect.DeepEqual(runtimeNames, want) {
		t.Fatalf("extracted runtime names = %q, want %q", runtimeNames, want)
	}
	for _, name := range want {
		if !regularRuntimeFile(filepath.Join(destination, name)) {
			t.Fatalf("runtime member %q was not extracted", name)
		}
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
				"standard",
				testCase.limits,
			)
			if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("zip resource overflow error = %v", err)
			}
		})
	}
}

func writeTestRuntimeSet(
	t *testing.T,
	directory, binaryName, tag, variant string,
) string {
	t.Helper()
	if err := os.MkdirAll(directory, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(directory, binaryName), []byte("binary:"+tag), 0755,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(directory, integrationsFileName),
		[]byte("integrations:"+tag),
		0644,
	); err != nil {
		t.Fatal(err)
	}
	pack := ""
	if variant == "ui" {
		packContents := []byte("pack:" + tag)
		digest := sha256.Sum256(packContents)
		pack = "cbm-ui-" + hex.EncodeToString(digest[:]) + ".pack"
		if err := os.WriteFile(
			filepath.Join(directory, pack), packContents, 0644,
		); err != nil {
			t.Fatal(err)
		}
	}
	return pack
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

func TestRuntimeReadinessRequiresTheCompleteSelectedVariant(t *testing.T) {
	binary := "codebase-memory-mcp"
	standard := filepath.Join(t.TempDir(), "standard")
	writeTestRuntimeSet(t, standard, binary, "standard", "standard")
	if !runtimeSetReady(standard, binary, "standard", verifyTestBinary) {
		t.Fatal("complete standard runtime set is not ready")
	}
	if err := os.Remove(filepath.Join(standard, integrationsFileName)); err != nil {
		t.Fatal(err)
	}
	if runtimeSetReady(standard, binary, "standard", verifyTestBinary) {
		t.Fatal("runtime set without integrations sidecar is ready")
	}

	ui := filepath.Join(t.TempDir(), "ui")
	writeTestRuntimeSet(t, ui, binary, "ui", "ui")
	if !runtimeSetReady(ui, binary, "ui", verifyTestBinary) {
		t.Fatal("complete UI runtime set is not ready")
	}
	uiNames, ok := runtimeSetNames(ui, binary, "ui")
	if !ok {
		t.Fatal("complete UI runtime set has no member list")
	}
	pack := uiNames[1]
	if err := os.WriteFile(filepath.Join(ui, pack), []byte("corrupt"), 0644); err != nil {
		t.Fatal(err)
	}
	if runtimeSetReady(ui, binary, "ui", verifyTestBinary) {
		t.Fatal("UI runtime set accepts pack bytes that do not match the filename digest")
	}
	writeTestRuntimeSet(t, ui, binary, "ui", "ui")
	secondPack := "cbm-ui-" + strings.Repeat("b", 64) + ".pack"
	if err := os.WriteFile(filepath.Join(ui, secondPack), []byte("extra"), 0644); err != nil {
		t.Fatal(err)
	}
	if runtimeSetReady(ui, binary, "ui", verifyTestBinary) {
		t.Fatal("UI runtime set with two packs is ready")
	}
}

func assertRuntimeTag(
	t *testing.T,
	directory, binaryName, tag, variant string,
) {
	t.Helper()
	checks := map[string]string{
		binaryName:           "binary:" + tag,
		integrationsFileName: "integrations:" + tag,
	}
	if variant == "ui" {
		packContents := []byte("pack:" + tag)
		digest := sha256.Sum256(packContents)
		checks["cbm-ui-"+hex.EncodeToString(digest[:])+".pack"] = "pack:" + tag
	}
	for name, want := range checks {
		contents, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(contents) != want {
			t.Fatalf("%s = %q, want %q", name, contents, want)
		}
	}
}

func TestRuntimePublicationRepairsPartialCache(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	binary := "codebase-memory-mcp"
	writeTestRuntimeSet(t, source, binary, "candidate", "ui")
	if err := os.Mkdir(destination, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(destination, binary), []byte("corrupt"), 0755,
	); err != nil {
		t.Fatal(err)
	}
	stalePack := "cbm-ui-" + strings.Repeat("b", 64) + ".pack"
	if err := os.WriteFile(
		filepath.Join(destination, stalePack), []byte("stale"), 0644,
	); err != nil {
		t.Fatal(err)
	}

	if err := publishRuntimeSetWithRecovery(
		source, destination, binary, "ui", verifyTestBinary,
	); err != nil {
		t.Fatal(err)
	}
	assertRuntimeTag(t, destination, binary, "candidate", "ui")
	if _, err := os.Stat(filepath.Join(destination, stalePack)); !os.IsNotExist(err) {
		t.Fatal("stale UI pack survived runtime publication")
	}
}

func TestRuntimePublicationRepairsDigestMismatchedPack(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	binary := "codebase-memory-mcp"
	writeTestRuntimeSet(t, source, binary, "candidate", "ui")
	oldPack := writeTestRuntimeSet(t, destination, binary, "old", "ui")
	if err := os.WriteFile(
		filepath.Join(destination, oldPack), []byte("corrupt pack"), 0644,
	); err != nil {
		t.Fatal(err)
	}
	if runtimeSetReady(destination, binary, "ui", verifyTestBinary) {
		t.Fatal("digest-mismatched destination pack was accepted before repair")
	}

	if err := publishRuntimeSetWithRecovery(
		source, destination, binary, "ui", verifyTestBinary,
	); err != nil {
		t.Fatal(err)
	}
	assertRuntimeTag(t, destination, binary, "candidate", "ui")
	if _, err := os.Stat(filepath.Join(destination, oldPack)); !os.IsNotExist(err) {
		t.Fatal("digest-mismatched UI pack survived runtime publication")
	}
}

func TestRuntimePublicationCommitsSidecarsBeforeBinary(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	binary := "codebase-memory-mcp"
	pack := writeTestRuntimeSet(t, source, binary, "candidate", "ui")
	committed := []string{}
	renameFile := func(sourcePath, destinationPath string) error {
		name := filepath.Base(destinationPath)
		if filepath.Dir(destinationPath) == destination &&
			(name == binary || name == integrationsFileName || name == pack) {
			committed = append(committed, name)
		}
		return os.Rename(sourcePath, destinationPath)
	}
	if err := publishRuntimeSetWithRecoveryAndRenamer(
		source,
		destination,
		binary,
		"ui",
		verifyTestBinary,
		renameFile,
	); err != nil {
		t.Fatal(err)
	}
	want := []string{integrationsFileName, pack, binary}
	if !reflect.DeepEqual(committed, want) {
		t.Fatalf("runtime commit order = %q, want %q", committed, want)
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
		if crashPhase == "published-integrations" &&
			destinationPath == filepath.Join(destination, integrationsFileName) {
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
		"standard",
		verifyTestBinary,
		renameFile,
	); err != nil {
		t.Fatal(err)
	}
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
			expectedBackupMembers: 2,
		},
		{
			name:                  "partial sidecar publish",
			crashPhase:            "published-integrations",
			expectedReady:         false,
			expectedBackupMembers: 2,
			expectRetiredMarker:   true,
		},
		{
			name:                  "complete publish before cleanup",
			crashPhase:            "published-binary",
			expectedReady:         true,
			expectedBackupMembers: 2,
			expectRetiredMarker:   true,
		},
		{
			name:                  "cleanup interrupted after one retired member",
			crashPhase:            runtimeBackupCleanupRemovedEvent,
			expectedReady:         true,
			expectedBackupMembers: 1,
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
			writeTestRuntimeSet(t, source, binary, "candidate", "standard")
			writeTestRuntimeSet(t, destination, binary, "old", "standard")
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
				destination, binary, "standard", verifyTestBinary,
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
				contents, err = os.ReadFile(
					filepath.Join(destination, integrationsFileName),
				)
				if err != nil {
					t.Fatal(err)
				}
				if string(contents) != "integrations:old" {
					t.Fatalf("recovered prior integrations = %q", contents)
				}
				if err := publishRuntimeSetWithRecovery(
					source, destination, binary, "standard", verifyTestBinary,
				); err != nil {
					t.Fatal(err)
				}
			}
			assertRuntimeTag(
				t, destination, binary, "candidate", "standard",
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

func TestConcurrentRuntimePublishersAreSerialized(t *testing.T) {
	root := t.TempDir()
	firstSource := filepath.Join(root, "source-first")
	secondSource := filepath.Join(root, "source-second")
	destination := filepath.Join(root, "destination")
	binary := "codebase-memory-mcp"
	firstPack := writeTestRuntimeSet(
		t, firstSource, binary, "first", "ui",
	)
	secondPack := writeTestRuntimeSet(
		t, secondSource, binary, "second", "ui",
	)

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
		if destinationPath == filepath.Join(destination, integrationsFileName) {
			pauseOnce.Do(func() {
				close(firstPaused)
				<-releaseFirst
			})
		}
		return nil
	}
	secondRenamer := func(sourcePath, destinationPath string) error {
		name := filepath.Base(destinationPath)
		if filepath.Dir(destinationPath) == destination &&
			(name == binary || name == integrationsFileName || name == secondPack) {
			enteredOnce.Do(func() { close(secondEnteredPublication) })
		}
		return os.Rename(sourcePath, destinationPath)
	}

	go func() {
		firstResult <- publishRuntimeSetWithRecoveryAndRenamer(
			firstSource,
			destination,
			binary,
			"ui",
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
			"ui",
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
	assertRuntimeTag(t, destination, binary, "first", "ui")
	if _, err := os.Stat(filepath.Join(destination, secondPack)); !os.IsNotExist(err) {
		t.Fatal("serialized losing publisher damaged the winning runtime set")
	}
	if _, err := os.Stat(filepath.Join(destination, firstPack)); err != nil {
		t.Fatalf("winning UI pack is missing: %v", err)
	}
}

func TestConcurrentPublisherWaitsForRollbackBeforePublishing(t *testing.T) {
	root := t.TempDir()
	firstSource := filepath.Join(root, "source-first")
	secondSource := filepath.Join(root, "source-second")
	destination := filepath.Join(root, "destination")
	binary := "codebase-memory-mcp"
	firstPack := writeTestRuntimeSet(
		t, firstSource, binary, "first", "ui",
	)
	secondPack := writeTestRuntimeSet(
		t, secondSource, binary, "second", "ui",
	)
	oldPack := writeTestRuntimeSet(t, destination, binary, "old", "ui")
	verifier := func(path string) error {
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(contents) == "binary:old" {
			return fmt.Errorf("old runtime set requires repair")
		}
		return verifyTestBinary(path)
	}

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

	failedBinary := false
	firstRenamer := func(sourcePath, destinationPath string) error {
		if destinationPath == filepath.Join(destination, binary) && !failedBinary {
			failedBinary = true
			return fmt.Errorf("injected binary publication failure")
		}
		if err := os.Rename(sourcePath, destinationPath); err != nil {
			return err
		}
		if destinationPath == filepath.Join(destination, integrationsFileName) {
			pauseOnce.Do(func() {
				close(firstPaused)
				<-releaseFirst
			})
		}
		return nil
	}
	secondRenamer := func(sourcePath, destinationPath string) error {
		name := filepath.Base(destinationPath)
		if filepath.Dir(destinationPath) == destination &&
			(name == binary || name == integrationsFileName || name == secondPack) {
			enteredOnce.Do(func() { close(secondEnteredPublication) })
		}
		return os.Rename(sourcePath, destinationPath)
	}

	go func() {
		firstResult <- publishRuntimeSetWithRecoveryAndRenamer(
			firstSource,
			destination,
			binary,
			"ui",
			verifier,
			firstRenamer,
		)
	}()
	select {
	case <-firstPaused:
	case <-time.After(5 * time.Second):
		t.Fatal("failing publisher did not reach the held publication gate")
	}

	go func() {
		secondResult <- publishRuntimeSetWithRecoveryAndRenamer(
			secondSource,
			destination,
			binary,
			"ui",
			verifier,
			secondRenamer,
		)
	}()

	concurrentPublication := false
	var secondErr error
	select {
	case <-secondWaiting:
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
		t.Fatal("second publisher neither waited nor entered failing publication")
	}
	close(releaseFirst)

	var firstErr error
	select {
	case firstErr = <-firstResult:
	case <-time.After(5 * time.Second):
		t.Fatal("failing publisher did not finish rollback")
	}
	if !concurrentPublication {
		select {
		case secondErr = <-secondResult:
		case <-time.After(5 * time.Second):
			t.Fatal("publisher waiting for rollback did not finish")
		}
	}
	if concurrentPublication {
		t.Fatal("second publisher entered publication before rollback completed")
	}
	if firstErr == nil || !failedBinary {
		t.Fatal("first publisher did not report the injected publication failure")
	}
	if secondErr != nil {
		t.Fatalf("publisher after rollback failed: %v", secondErr)
	}
	assertRuntimeTag(t, destination, binary, "second", "ui")
	for _, obsoletePack := range []string{firstPack, oldPack} {
		if _, err := os.Stat(filepath.Join(destination, obsoletePack)); !os.IsNotExist(err) {
			t.Fatalf("obsolete UI pack survived serialized rollback: %s", obsoletePack)
		}
	}
	if _, err := os.Stat(filepath.Join(destination, secondPack)); err != nil {
		t.Fatalf("post-rollback winner's UI pack is missing: %v", err)
	}
	if !runtimeSetReady(destination, binary, "ui", verifier) {
		t.Fatal("post-rollback winner is incomplete")
	}
}

func TestRuntimePublicationFailureRestoresPriorCompleteSet(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	binary := "codebase-memory-mcp"
	candidatePack := writeTestRuntimeSet(
		t, source, binary, "candidate", "ui",
	)
	oldPack := writeTestRuntimeSet(t, destination, binary, "old", "ui")
	verifier := func(path string) error {
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(contents) == "binary:old" {
			return fmt.Errorf("old runtime set requires repair")
		}
		return verifyTestBinary(path)
	}
	failed := false
	renameFile := func(sourcePath, destinationPath string) error {
		if destinationPath == filepath.Join(destination, binary) && !failed {
			failed = true
			return fmt.Errorf("injected binary publication failure")
		}
		return os.Rename(sourcePath, destinationPath)
	}

	err := publishRuntimeSetWithRecoveryAndRenamer(
		source,
		destination,
		binary,
		"ui",
		verifier,
		renameFile,
	)
	if err == nil {
		t.Fatal("injected publication failure unexpectedly succeeded")
	}
	if !failed {
		t.Fatal("test did not reach the injected binary publication failure")
	}
	assertRuntimeTag(t, destination, binary, "old", "ui")
	if !runtimeSetReady(destination, binary, "ui", nil) {
		t.Fatal("rollback did not restore a structurally complete prior set")
	}
	if _, err := os.Stat(filepath.Join(destination, candidatePack)); !os.IsNotExist(err) {
		t.Fatal("failed candidate UI pack survived rollback")
	}
	if _, err := os.Stat(filepath.Join(destination, oldPack)); err != nil {
		t.Fatalf("rolled-back UI pack is missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destination, runtimeSetLockName)); !os.IsNotExist(err) {
		t.Fatal("runtime-set lock survived rollback")
	}
}

func TestMutationSnapshotReleasesCacheLockAndCleansAfterLaunchFailure(t *testing.T) {
	directory := t.TempDir()
	binary := "codebase-memory-mcp"
	executable := filepath.Join(directory, binary)
	writeTestRuntimeSet(t, directory, binary, "cached", "standard")
	var snapshotExecutable string
	runnerCalled := false
	err := execBinaryWithRuntimeLockAndRunner(
		executable,
		[]string{"install", "--yes"},
		"standard",
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
				t, filepath.Dir(candidate), binary, "cached", "standard",
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
			"standard",
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
	writeTestRuntimeSet(t, directory, binary, "cached", "standard")
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
		"standard",
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

func TestMutationSnapshotPreservesCompleteUIRuntimeSet(t *testing.T) {
	directory := t.TempDir()
	binary := "codebase-memory-mcp"
	executable := filepath.Join(directory, binary)
	pack := writeTestRuntimeSet(t, directory, binary, "ui-cached", "ui")
	var snapshotDirectory string
	err := execBinaryWithRuntimeLockAndRunner(
		executable,
		[]string{"install", "--yes"},
		"ui",
		verifyTestBinary,
		func(candidate string, _ []string) error {
			snapshotDirectory = filepath.Dir(candidate)
			names, ok := runtimeSetNames(snapshotDirectory, binary, "ui")
			want := []string{integrationsFileName, pack, binary}
			if !ok || !reflect.DeepEqual(names, want) {
				t.Fatalf("UI mutation snapshot names = %q, want %q", names, want)
			}
			assertRuntimeTag(
				t, snapshotDirectory, binary, "ui-cached", "ui",
			)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(snapshotDirectory); !os.IsNotExist(err) {
		t.Fatal("successful UI mutation left its private runtime snapshot")
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
			writeTestRuntimeSet(t, directory, binary, "cached", "standard")
			result := execBinaryWithRuntimeLockAndRunner(
				filepath.Join(directory, binary),
				[]string{"install", "--yes"},
				"standard",
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

func TestMutationSnapshotParentDeathLeavesCacheRecoverable(t *testing.T) {
	const roleEnvironment = "CBM_GO_MUTATION_SNAPSHOT_ROLE"
	role := os.Getenv(roleEnvironment)
	if role == "child" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		info := fmt.Sprintf("%d\n%s\n", os.Getpid(), executable)
		childInfo := os.Getenv("CBM_GO_MUTATION_CHILD_INFO")
		stagedChildInfo := childInfo + ".tmp"
		if err := os.WriteFile(stagedChildInfo, []byte(info), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(stagedChildInfo, childInfo); err != nil {
			t.Fatal(err)
		}
		continuePath := os.Getenv("CBM_GO_MUTATION_CHILD_CONTINUE")
		deadline := time.Now().Add(20 * time.Second)
		for {
			if _, err := os.Stat(continuePath); err == nil {
				break
			} else if !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if !time.Now().Before(deadline) {
				t.Fatal("mutation child timed out waiting for successor publication")
			}
			time.Sleep(10 * time.Millisecond)
		}
		integrationBytes, err := os.ReadFile(filepath.Join(
			filepath.Dir(executable), integrationsFileName,
		))
		if err != nil {
			t.Fatal(err)
		}
		if string(integrationBytes) != "integrations:parent-death" {
			t.Fatalf("mutation child sidecar changed: %q", integrationBytes)
		}
		if err := os.WriteFile(
			os.Getenv("CBM_GO_MUTATION_CHILD_SURVIVED"),
			[]byte("survived\n"),
			0600,
		); err != nil {
			t.Fatal(err)
		}
		return
	}
	if role == "parent" {
		directory := os.Getenv("CBM_GO_MUTATION_CACHE")
		executable := filepath.Join(
			directory, binaryNameForOS(runtime.GOOS),
		)
		runner := func(candidate string, args []string) error {
			command := exec.Command(candidate, args...)
			childEnvironment := make([]string, 0, len(os.Environ())+1)
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, roleEnvironment+"=") {
					childEnvironment = append(childEnvironment, entry)
				}
			}
			command.Env = append(childEnvironment, roleEnvironment+"=child")
			return command.Run()
		}
		if err := execBinaryWithRuntimeLockAndRunner(
			executable,
			[]string{
				"-test.run=^TestMutationSnapshotParentDeathLeavesCacheRecoverable$",
			},
			"standard",
			nil,
			runner,
		); err != nil {
			t.Fatal(err)
		}
		t.Fatal("mutation parent returned before the child was released")
	}

	root := t.TempDir()
	directory := filepath.Join(root, "cache")
	if err := os.Mkdir(directory, 0755); err != nil {
		t.Fatal(err)
	}
	binary := binaryNameForOS(runtime.GOOS)
	stagedExecutable, err := copyRuntimeStage(
		os.Args[0], directory, true, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	cacheExecutable := filepath.Join(directory, binary)
	if err := os.Rename(stagedExecutable, cacheExecutable); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(directory, integrationsFileName),
		[]byte("integrations:parent-death"),
		0644,
	); err != nil {
		t.Fatal(err)
	}
	childInfo := filepath.Join(root, "child-info")
	childContinue := filepath.Join(root, "child-continue")
	childSurvived := filepath.Join(root, "child-survived")
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestMutationSnapshotParentDeathLeavesCacheRecoverable$",
	)
	command.Env = append(
		os.Environ(),
		roleEnvironment+"=parent",
		"CBM_GO_MUTATION_CACHE="+directory,
		"CBM_GO_MUTATION_CHILD_INFO="+childInfo,
		"CBM_GO_MUTATION_CHILD_CONTINUE="+childContinue,
		"CBM_GO_MUTATION_CHILD_SURVIVED="+childSurvived,
	)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitResult := make(chan error, 1)
	go func() { waitResult <- command.Wait() }()
	parentFinished := false
	childPID := 0
	snapshotExecutable := ""
	defer func() {
		if !parentFinished {
			_ = command.Process.Kill()
			<-waitResult
		}
		if childPID > 0 && runtimeSetProcessAlive(childPID) {
			if child, err := os.FindProcess(childPID); err == nil {
				_ = child.Kill()
			}
		}
		if snapshotExecutable != "" {
			_ = os.RemoveAll(filepath.Dir(snapshotExecutable))
		}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		contents, err := os.ReadFile(childInfo)
		if err == nil {
			lines := strings.Split(strings.TrimSpace(string(contents)), "\n")
			if len(lines) != 2 {
				t.Fatalf("mutation child info = %q", contents)
			}
			childPID, err = strconv.Atoi(lines[0])
			if err != nil {
				t.Fatal(err)
			}
			snapshotExecutable = lines[1]
			break
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case err := <-waitResult:
			parentFinished = true
			t.Fatalf("mutation parent exited before child start: %v: %s", err, stderr.String())
		default:
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("mutation child did not start: %s", stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if filepath.Dir(snapshotExecutable) == directory {
		t.Fatal("mutation child executed from the shared package cache")
	}
	if _, err := os.Stat(filepath.Join(
		directory, runtimeSetLockName,
	)); !os.IsNotExist(err) {
		t.Fatal("mutation parent retained the cache lock while child was running")
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := <-waitResult; err == nil {
		t.Fatal("mutation parent was not hard-killed")
	}
	parentFinished = true
	if !runtimeSetProcessAlive(childPID) {
		t.Fatal("hard-killing the wrapper also stopped the mutation child")
	}

	source := filepath.Join(root, "successor")
	writeTestRuntimeSet(t, source, binary, "successor", "standard")
	if err := publishRuntimeSetWithRecovery(
		source, directory, binary, "standard", verifyTestBinary,
	); err != nil {
		t.Fatalf("successor publication after parent death failed: %v", err)
	}
	assertRuntimeTag(t, directory, binary, "successor", "standard")
	if err := os.WriteFile(childContinue, []byte("continue\n"), 0600); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(childSurvived); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if !time.Now().Before(deadline) {
			t.Fatal("snapshot child did not survive successor cache publication")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(
		directory, runtimeSetLockName,
	)); !os.IsNotExist(err) {
		t.Fatal("successor recovery left the package-cache lock")
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

func TestRuntimeReadinessRejectsMultiplyLinkedLeaves(t *testing.T) {
	directory := t.TempDir()
	binary := "codebase-memory-mcp"
	writeTestRuntimeSet(t, directory, binary, "linked", "standard")
	if err := os.Link(
		filepath.Join(directory, integrationsFileName),
		filepath.Join(directory, "integrations-hardlink"),
	); err != nil {
		t.Skipf("filesystem does not support hard links: %v", err)
	}
	if runtimeSetReady(directory, binary, "standard", verifyTestBinary) {
		t.Fatal("runtime set accepted a multiply-linked integrations leaf")
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
	writeTestRuntimeSet(t, source, binary, "long-path", "standard")
	if err := publishRuntimeSetWithRecovery(
		source, destination, binary, "standard", verifyTestBinary,
	); err != nil {
		t.Fatalf("publication under a long Windows runtime path failed: %v", err)
	}
	ready, err := runtimeSetReadyLocked(
		destination, binary, "standard", verifyTestBinary,
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
		directory, binary, "standard", verifyTestBinary,
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
