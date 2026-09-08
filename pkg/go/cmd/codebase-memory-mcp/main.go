// codebase-memory-mcp — Go installer wrapper.
//
// On first run, downloads the pre-built binary for the current platform from
// GitHub Releases, caches it, and replaces the current process with it.
// Subsequent runs skip directly to exec.
//
// Install:
//
//	go install github.com/DeusData/codebase-memory-mcp/pkg/go/cmd/codebase-memory-mcp@latest
package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	repo              = "DeusData/codebase-memory-mcp"
	version           = "0.10.8"
	windowsBinaryName = "codebase-memory-mcp.exe"

	maxRedirects            = 5
	requestTimeout          = 2 * time.Minute
	connectTimeout          = 15 * time.Second
	responseHeaderTimeout   = 30 * time.Second
	candidateTimeout        = 15 * time.Second
	maxChecksumManifestSize = 1024 * 1024
	maxReleaseArchiveSize   = int64(256 * 1024 * 1024)
	maxArchiveMembers       = 64
	maxArchiveMemberSize    = int64(256 * 1024 * 1024)
	maxArchiveExpandedSize  = int64(512 * 1024 * 1024)

	runtimeSetLockName               = ".codebase-memory-mcp-runtime.lock"
	runtimeSetLockWait               = 45 * time.Second
	runtimeSetOwnerlessStale         = 30 * time.Second
	runtimeSetLockLease              = 5 * time.Minute
	runtimeSetLockHeartbeat          = 20 * time.Second
	runtimeSetLegacyStale            = time.Hour
	runtimeSetLockPoll               = 25 * time.Millisecond
	runtimeSetLockOwnerFile          = "owner.json"
	runtimeSetLockTokenSize          = 16
	runtimeBackupPrefix              = ".cbm-runtime-backup-"
	runtimeBackupRetired             = ".retirement-complete"
	runtimeBackupCleanupOnly         = ".cleanup-only"
	runtimeBackupBeforeMarkerEvent   = "before-retirement-marker"
	runtimeBackupCleanupRemovedEvent = "cleanup-member-removed"
)

type archiveResourceLimits struct {
	compressedBytes int64
	members         int
	memberBytes     int64
	expandedBytes   int64
}

var defaultArchiveResourceLimits = archiveResourceLimits{
	compressedBytes: maxReleaseArchiveSize,
	members:         maxArchiveMembers,
	memberBytes:     maxArchiveMemberSize,
	expandedBytes:   maxArchiveExpandedSize,
}

type runtimeSetLock struct {
	path  string
	token string
	file  *os.File
}

type runtimeSetLockOwnerRecord struct {
	PID          int    `json:"pid"`
	Token        string `json:"token"`
	LeaseExpires int64  `json:"lease_expires_ms,omitempty"`
}

type runtimeBackupFile struct {
	path   string
	digest [sha256.Size]byte
}

type runtimeBackupTransaction struct {
	path        string
	retired     bool
	cleanupOnly bool
	files       map[string]runtimeBackupFile
}

// Tests use this observer to distinguish a publisher waiting on the runtime
// lock from one entering the shared publication sequence. Production leaves
// it nil.
var (
	runtimeSetLockWaitObserver      func()
	runtimeSetLockClaimObserver     func() error
	runtimeSetLockCaptureObserver   func()
	runtimeSetLockOwnerReadObserver func()
	runtimeBackupCrashObserver      func(string, string) error
	runtimeSetProcessAlive          = platformRuntimeSetLockProcessAlive
	runtimeMutationSnapshotCleanup  = os.RemoveAll
)

func main() {
	mutation := runtimeMutationAction(os.Args[1:])
	if mutation == "update" {
		fmt.Fprintln(
			os.Stderr,
			"This Go wrapper is maintained by Go. Update it with \"go install github.com/DeusData/codebase-memory-mcp/pkg/go/cmd/codebase-memory-mcp@latest\".",
		)
		os.Exit(2)
	}
	executable, err := ensureBinary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "codebase-memory-mcp: %v\n", err)
		os.Exit(1)
	}
	args := nativeArgs(os.Args[1:])
	if mutation == "install" || mutation == "uninstall" {
		err = execBinaryWithRuntimeLock(executable, args)
	} else {
		err = execBinary(executable, args)
	}
	if err != nil {
		if runtime.GOOS == "windows" {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				printPortableMutationGuidance(args)
				os.Exit(exitErr.ExitCode())
			}
		}
		fmt.Fprintf(os.Stderr, "codebase-memory-mcp: %v\n", err)
		os.Exit(1)
	}
}

func ensureBinary() (string, error) {
	binary := binPath()
	ready, err := runtimeSetReadyLocked(
		filepath.Dir(binary), filepath.Base(binary), verifyCandidate,
	)
	if err != nil {
		return "", err
	}
	if ready {
		return executionPathForOS(binary, runtime.GOOS), nil
	}
	if err := download(binary); err != nil {
		return "", err
	}
	return executionPathForOS(binary, runtime.GOOS), nil
}

func binPath() string {
	return filepath.Join(
		cacheDir(), version, binaryNameForOS(runtime.GOOS),
	)
}

func binaryNameForOS(targetOS string) string {
	if targetOS == "windows" {
		return windowsBinaryName
	}
	return "codebase-memory-mcp"
}

func executionPathForOS(binary, targetOS string) string {
	return binary
}

func printPortableMutationGuidance(args []string) {
	action := runtimeMutationAction(args)
	if action != "uninstall" {
		return
	}
	packageCommand := "Remove-Item (Get-Command codebase-memory-mcp).Source"
	fmt.Fprintf(
		os.Stderr,
		"This Go Windows copy is portable. Use %q for package maintenance, or run %q once to create a managed installation with coordinated self-update/uninstall.\n",
		packageCommand,
		"codebase-memory-mcp install --yes",
	)
}

func runtimeMutationAction(args []string) string {
	for _, argument := range args {
		switch argument {
		case "--help", "-h", "--version":
			return ""
		case "cli", "hook-augment", "config":
			return ""
		case "install", "update", "uninstall":
			return argument
		}
		if strings.HasPrefix(argument, "-") {
			continue
		}
		return ""
	}
	return ""
}

func defaultManagedInstallDir() string {
	if runtime.GOOS == "windows" {
		base := os.Getenv("LOCALAPPDATA")
		if base == "" {
			if home, err := os.UserHomeDir(); err == nil {
				base = filepath.Join(home, "AppData", "Local")
			}
		}
		return filepath.Join(base, "Programs", "codebase-memory-mcp")
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".local", "bin")
	}
	return filepath.Join(os.TempDir(), "codebase-memory-mcp-managed")
}

func nativeArgs(args []string) []string {
	result := append([]string(nil), args...)
	if runtimeMutationAction(result) != "uninstall" {
		return result
	}
	for _, argument := range result {
		if argument == "--dir" || strings.HasPrefix(argument, "--dir=") {
			return result
		}
	}
	return append(result, "--dir", defaultManagedInstallDir())
}

func mutationTargetDirectory(args []string) (string, bool) {
	for index, argument := range args {
		if strings.HasPrefix(argument, "--dir=") {
			return strings.TrimPrefix(argument, "--dir="), true
		}
		if argument == "--dir" && index+1 < len(args) {
			return args[index+1], true
		}
	}
	return "", false
}

func pathsReferToSameDirectory(left, right string) bool {
	leftAbsolute, leftErr := filepath.Abs(left)
	rightAbsolute, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	leftAbsolute = filepath.Clean(leftAbsolute)
	rightAbsolute = filepath.Clean(rightAbsolute)
	if leftAbsolute == rightAbsolute ||
		(runtime.GOOS == "windows" && strings.EqualFold(leftAbsolute, rightAbsolute)) {
		return true
	}
	leftStatus, leftErr := os.Stat(leftAbsolute)
	rightStatus, rightErr := os.Stat(rightAbsolute)
	return leftErr == nil && rightErr == nil &&
		leftStatus.IsDir() && rightStatus.IsDir() &&
		os.SameFile(leftStatus, rightStatus)
}

func mutationSnapshotEnvironment() []string {
	blocked := []string{"CBM_ASSETS_DIR", "CBM_UI_ASSETS_DIR"}
	result := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		separator := strings.IndexByte(entry, '=')
		name := entry
		if separator >= 0 {
			name = entry[:separator]
		}
		drop := false
		for _, blockedName := range blocked {
			if strings.EqualFold(name, blockedName) {
				drop = true
				break
			}
		}
		if !drop {
			result = append(result, entry)
		}
	}
	return result
}

func cacheDir() string {
	if d := os.Getenv("CBM_CACHE_DIR"); d != "" {
		return d
	}
	switch runtime.GOOS {
	case "windows":
		if d := os.Getenv("LOCALAPPDATA"); d != "" {
			return filepath.Join(d, "codebase-memory-mcp")
		}
	case "darwin":
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, "Library", "Caches", "codebase-memory-mcp")
		}
	}
	if d := os.Getenv("XDG_CACHE_HOME"); d != "" {
		return filepath.Join(d, "codebase-memory-mcp")
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".cache", "codebase-memory-mcp")
	}
	return filepath.Join(os.TempDir(), "codebase-memory-mcp")
}

func goos() string {
	switch runtime.GOOS {
	case "darwin":
		return "darwin"
	case "linux":
		return "linux"
	case "windows":
		return "windows"
	default:
		return runtime.GOOS
	}
}

func goarch() string {
	switch runtime.GOARCH {
	case "amd64":
		return "amd64"
	case "arm64":
		return "arm64"
	default:
		return runtime.GOARCH
	}
}

func archiveNamesForOS(platform, binaryName string) []string {
	installer := "install.sh"
	if platform == "windows" {
		installer = "install.ps1"
	}
	return []string{
		binaryName,
		"LICENSE",
		installer,
		"THIRD_PARTY_NOTICES.md",
	}
}

func download(dest string) error {
	platform := goos()
	arch := goarch()
	ext := "tar.gz"
	if platform == "windows" {
		ext = "zip"
	}

	portable := ""
	if platform == "linux" {
		portable = "-portable"
	}
	archive := fmt.Sprintf(
		"codebase-memory-mcp-%s-%s%s.%s",
		platform, arch, portable, ext,
	)
	url := fmt.Sprintf("https://github.com/%s/releases/download/v%s/%s", repo, version, archive)
	checksumURL := fmt.Sprintf("https://github.com/%s/releases/download/v%s/checksums.txt", repo, version)

	fmt.Fprintf(os.Stderr, "codebase-memory-mcp: downloading v%s for %s/%s...\n", version, platform, arch)

	tmp, err := os.MkdirTemp("", "cbm-install-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	archivePath := filepath.Join(tmp, "cbm."+ext)
	if err := httpGet(url, archivePath); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	// A release binary is executable input, so checksum verification is a
	// mandatory precondition rather than a best-effort warning.
	checksums, err := fetchChecksums(checksumURL)
	if err != nil {
		return fmt.Errorf("checksum manifest unavailable: %w", err)
	}
	expected, ok := checksums[archive]
	if !ok {
		return fmt.Errorf("checksum manifest has no entry for %s", archive)
	}
	if err := verifyChecksum(archivePath, expected); err != nil {
		return err
	}

	binName := binaryNameForOS(platform)
	archiveNames := archiveNamesForOS(platform, binName)
	extractNames := []string{binName}
	var runtimeNames []string

	if ext == "tar.gz" {
		runtimeNames, err = extractTarGz(
			archivePath, tmp, archiveNames, extractNames,
		)
		if err != nil {
			return fmt.Errorf("extraction failed: %w", err)
		}
	} else {
		runtimeNames, err = extractZip(
			archivePath, tmp, archiveNames, extractNames,
		)
		if err != nil {
			return fmt.Errorf("extraction failed: %w", err)
		}
	}
	for _, name := range runtimeNames {
		extracted := filepath.Join(tmp, name)
		mode := os.FileMode(0644)
		if name == binName {
			mode = 0755
		}
		if err := os.Chmod(extracted, mode); err != nil {
			return fmt.Errorf("could not set candidate permissions for %s: %w", name, err)
		}
	}
	if err := verifyCandidate(filepath.Join(tmp, binName)); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return fmt.Errorf("could not create cache dir: %w", err)
	}

	if err := publishRuntimeSetWithRecovery(
		tmp, filepath.Dir(dest), binName, verifyCandidate,
	); err != nil {
		return fmt.Errorf("could not install runtime set: %w", err)
	}

	return nil
}

// validateURLScheme rejects non-https URLs before any fetch (defense-in-depth).
func validateURLScheme(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return fmt.Errorf("refusing non-https URL: %s", rawURL)
	}
	return nil
}

// httpsOnlyClient returns an HTTP client that rejects non-HTTPS redirects.
var httpsOnlyClient = &http.Client{
	Timeout: requestTimeout,
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: connectTimeout}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   connectTimeout,
		ResponseHeaderTimeout: responseHeaderTimeout,
		ExpectContinueTimeout: time.Second,
	},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if err := validateURLScheme(req.URL.String()); err != nil {
			return fmt.Errorf("refusing unsafe redirect: %w", err)
		}
		if len(via) > maxRedirects {
			return fmt.Errorf("too many redirects")
		}
		return nil
	},
}

func httpGet(rawURL, dest string) error {
	return httpGetWithLimit(rawURL, dest, maxReleaseArchiveSize)
}

func httpGetWithLimit(rawURL, dest string, maxBytes int64) error {
	if err := validateURLScheme(rawURL); err != nil {
		return err
	}
	if maxBytes <= 0 {
		return fmt.Errorf("invalid compressed archive safety limit")
	}
	resp, err := httpsOnlyClient.Get(rawURL) //nolint:gosec
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d for %s", resp.StatusCode, rawURL)
	}
	if resp.ContentLength > maxBytes {
		return fmt.Errorf(
			"release archive exceeds the %d-byte compressed safety limit",
			maxBytes,
		)
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	written, copyErr := io.Copy(f, io.LimitReader(resp.Body, maxBytes+1))
	closeErr := f.Close()
	if copyErr == nil && written > maxBytes {
		copyErr = fmt.Errorf(
			"release archive exceeds the %d-byte compressed safety limit",
			maxBytes,
		)
	}
	if copyErr != nil {
		_ = os.Remove(dest)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(dest)
	}
	return closeErr
}

func fetchChecksums(url string) (map[string]string, error) {
	if err := validateURLScheme(url); err != nil {
		return nil, err
	}
	resp, err := httpsOnlyClient.Get(url) //nolint:gosec
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxChecksumManifestSize+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxChecksumManifestSize {
		return nil, fmt.Errorf("checksums.txt exceeds the 1 MiB safety limit")
	}
	result := make(map[string]string)
	for _, line := range strings.Split(string(body), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 {
			name := parts[1]
			if strings.HasPrefix(name, "*") {
				name = name[1:]
			}
			digest := strings.ToLower(parts[0])
			if len(digest) != sha256.Size*2 {
				return nil, fmt.Errorf("invalid SHA-256 checksum for %s", name)
			}
			if _, err := hex.DecodeString(digest); err != nil {
				return nil, fmt.Errorf("invalid SHA-256 checksum for %s: %w", name, err)
			}
			if prior, exists := result[name]; exists && prior != digest {
				return nil, fmt.Errorf("conflicting SHA-256 checksums for %s", name)
			}
			result[name] = digest
		}
	}
	return result, nil
}

func verifyChecksum(path, expected string) error {
	expected = strings.ToLower(expected)
	if len(expected) != sha256.Size*2 {
		return fmt.Errorf("invalid SHA-256 checksum length")
	}
	if _, err := hex.DecodeString(expected); err != nil {
		return fmt.Errorf("invalid SHA-256 checksum: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	actual := hex.EncodeToString(h.Sum(nil))
	if actual != expected {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expected, actual)
	}
	return nil
}

func validateArchiveMemberNames(
	names, archiveNames []string, caseInsensitive bool,
) error {
	required := make(map[string]struct{}, len(archiveNames))
	for _, name := range archiveNames {
		required[name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(names))
	found := make(map[string]struct{}, len(archiveNames))
	for _, name := range names {
		key := name
		if caseInsensitive {
			key = strings.ToLower(name)
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf(
				"duplicate or case-conflicting archive entry: %q", name,
			)
		}
		seen[key] = struct{}{}
		if _, isRequired := required[name]; !isRequired {
			return fmt.Errorf(
				"archive must contain only the exact root files: %s",
				strings.Join(archiveNames, ", "),
			)
		}
		found[name] = struct{}{}
	}
	if len(seen) != len(archiveNames) || len(found) != len(required) {
		return fmt.Errorf(
			"archive must contain exactly one of each required root file: %s",
			strings.Join(archiveNames, ", "),
		)
	}
	return nil
}

func validateArchiveResourceLimits(limits archiveResourceLimits) error {
	if limits.compressedBytes <= 0 || limits.members <= 0 ||
		limits.memberBytes <= 0 || limits.expandedBytes <= 0 {
		return fmt.Errorf("invalid archive resource safety limits")
	}
	return nil
}

func requireCompressedArchiveWithinLimit(
	archivePath string, limits archiveResourceLimits,
) error {
	status, err := os.Stat(archivePath)
	if err != nil {
		return err
	}
	if !status.Mode().IsRegular() {
		return fmt.Errorf("release archive is not a regular file")
	}
	if status.Size() > limits.compressedBytes {
		return fmt.Errorf(
			"release archive exceeds the %d-byte compressed safety limit",
			limits.compressedBytes,
		)
	}
	return nil
}

func accountDeclaredArchiveMember(
	name string, size int64, expanded *int64, limits archiveResourceLimits,
) error {
	if size < 0 {
		return fmt.Errorf("archive member has a negative declared size: %q", name)
	}
	if size > limits.memberBytes {
		return fmt.Errorf(
			"archive member %q exceeds the %d-byte expanded safety limit",
			name, limits.memberBytes,
		)
	}
	if *expanded > limits.expandedBytes-size {
		return fmt.Errorf(
			"archive exceeds the %d-byte aggregate expanded safety limit",
			limits.expandedBytes,
		)
	}
	*expanded += size
	return nil
}

func copyArchiveMemberWithLimits(
	destination io.Writer,
	source io.Reader,
	name string,
	declaredSize int64,
	actualExpanded *int64,
	limits archiveResourceLimits,
) error {
	remaining := limits.expandedBytes - *actualExpanded
	copyLimit := limits.memberBytes
	if remaining < copyLimit {
		copyLimit = remaining
	}
	copied, err := io.Copy(
		destination, io.LimitReader(source, copyLimit+1),
	)
	if err != nil {
		return err
	}
	if copied > limits.memberBytes {
		return fmt.Errorf(
			"archive member %q exceeds the %d-byte actual expanded safety limit",
			name, limits.memberBytes,
		)
	}
	if copied > remaining {
		return fmt.Errorf(
			"archive exceeds the %d-byte aggregate actual expanded safety limit",
			limits.expandedBytes,
		)
	}
	if copied != declaredSize {
		return fmt.Errorf(
			"archive member %q actual size %d does not match declared size %d",
			name, copied, declaredSize,
		)
	}
	*actualExpanded += copied
	return nil
}

func tarGzMemberNames(archivePath string) ([]string, error) {
	return tarGzMemberNamesWithLimits(
		archivePath, defaultArchiveResourceLimits,
	)
}

func tarGzMemberNamesWithLimits(
	archivePath string, limits archiveResourceLimits,
) ([]string, error) {
	if err := validateArchiveResourceLimits(limits); err != nil {
		return nil, err
	}
	if err := requireCompressedArchiveWithinLimit(archivePath, limits); err != nil {
		return nil, err
	}
	f, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var names []string
	var declaredExpanded int64
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA {
			return nil, fmt.Errorf(
				"archive member is not a regular root file: %q", hdr.Name,
			)
		}
		if len(names) >= limits.members {
			return nil, fmt.Errorf(
				"archive exceeds the %d-member safety limit", limits.members,
			)
		}
		if err := accountDeclaredArchiveMember(
			hdr.Name, hdr.Size, &declaredExpanded, limits,
		); err != nil {
			return nil, err
		}
		names = append(names, hdr.Name)
	}
	return names, nil
}

func extractTarGz(
	archivePath, destDir string,
	archiveNames, extractNames []string,
) ([]string, error) {
	return extractTarGzWithLimits(
		archivePath,
		destDir,
		archiveNames,
		extractNames,
		defaultArchiveResourceLimits,
	)
}

func extractTarGzWithLimits(
	archivePath, destDir string,
	archiveNames, extractNames []string,
	limits archiveResourceLimits,
) ([]string, error) {
	names, err := tarGzMemberNamesWithLimits(archivePath, limits)
	if err != nil {
		return nil, err
	}
	if err := validateArchiveMemberNames(names, archiveNames, false); err != nil {
		return nil, err
	}
	runtimeNames := append([]string(nil), extractNames...)
	targets := make(map[string]struct{}, len(runtimeNames))
	for _, name := range runtimeNames {
		targets[name] = struct{}{}
	}

	f, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	extracted := make(map[string]struct{}, len(runtimeNames))
	memberCount := 0
	var declaredExpanded int64
	var actualExpanded int64
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		memberCount++
		if memberCount > limits.members {
			return nil, fmt.Errorf(
				"archive exceeds the %d-member safety limit", limits.members,
			)
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA {
			return nil, fmt.Errorf(
				"archive member is not a regular root file: %q", hdr.Name,
			)
		}
		if err := accountDeclaredArchiveMember(
			hdr.Name, hdr.Size, &declaredExpanded, limits,
		); err != nil {
			return nil, err
		}
		if _, wanted := targets[hdr.Name]; wanted {
			outputPath := filepath.Join(destDir, hdr.Name)
			out, err := os.OpenFile(
				outputPath,
				os.O_WRONLY|os.O_CREATE|os.O_EXCL,
				0600,
			)
			if err != nil {
				return nil, err
			}
			copyErr := copyArchiveMemberWithLimits(
				out, tr, hdr.Name, hdr.Size, &actualExpanded, limits,
			)
			closeErr := out.Close()
			if copyErr != nil {
				_ = os.Remove(outputPath)
				return nil, copyErr
			}
			if closeErr != nil {
				_ = os.Remove(outputPath)
				return nil, closeErr
			}
			extracted[hdr.Name] = struct{}{}
		} else if err := copyArchiveMemberWithLimits(
			io.Discard, tr, hdr.Name, hdr.Size, &actualExpanded, limits,
		); err != nil {
			return nil, err
		}
	}
	if len(extracted) != len(runtimeNames) {
		return nil, fmt.Errorf("archive runtime set extraction was incomplete")
	}
	return runtimeNames, nil
}

func extractZip(
	archivePath, destDir string,
	archiveNames, extractNames []string,
) ([]string, error) {
	return extractZipWithLimits(
		archivePath,
		destDir,
		archiveNames,
		extractNames,
		defaultArchiveResourceLimits,
	)
}

func extractZipWithLimits(
	archivePath, destDir string,
	archiveNames, extractNames []string,
	limits archiveResourceLimits,
) ([]string, error) {
	if err := validateArchiveResourceLimits(limits); err != nil {
		return nil, err
	}
	if err := requireCompressedArchiveWithinLimit(archivePath, limits); err != nil {
		return nil, err
	}
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if len(r.File) > limits.members {
		return nil, fmt.Errorf(
			"archive exceeds the %d-member safety limit", limits.members,
		)
	}
	names := make([]string, 0, len(r.File))
	declaredSizes := make(map[string]int64, len(r.File))
	var declaredExpanded int64
	for _, f := range r.File {
		name, err := validateWindowsZipMember(f.Name)
		if err != nil {
			return nil, err
		}
		if !f.FileInfo().Mode().IsRegular() {
			return nil, fmt.Errorf(
				"archive member is not a regular root file: %q", f.Name,
			)
		}
		if f.UncompressedSize64 > uint64(limits.memberBytes) {
			return nil, fmt.Errorf(
				"archive member %q exceeds the %d-byte expanded safety limit",
				name, limits.memberBytes,
			)
		}
		declaredSize := int64(f.UncompressedSize64)
		if err := accountDeclaredArchiveMember(
			name, declaredSize, &declaredExpanded, limits,
		); err != nil {
			return nil, err
		}
		names = append(names, name)
		declaredSizes[name] = declaredSize
	}
	if err := validateArchiveMemberNames(names, archiveNames, true); err != nil {
		return nil, err
	}
	runtimeNames := append([]string(nil), extractNames...)
	targetNames := make(map[string]struct{}, len(runtimeNames))
	for _, name := range runtimeNames {
		targetNames[name] = struct{}{}
	}
	extracted := make(map[string]struct{}, len(runtimeNames))
	var actualExpanded int64

	for index, target := range r.File {
		name := names[index]
		rc, err := target.Open()
		if err != nil {
			return nil, err
		}
		var destination io.Writer = io.Discard
		var out *os.File
		outputPath := ""
		if _, wanted := targetNames[name]; wanted {
			outputPath = filepath.Join(destDir, name)
			out, err = os.OpenFile(
				outputPath,
				os.O_WRONLY|os.O_CREATE|os.O_EXCL,
				0600,
			)
			if err != nil {
				_ = rc.Close()
				return nil, err
			}
			destination = out
		}
		copyErr := copyArchiveMemberWithLimits(
			destination,
			rc,
			name,
			declaredSizes[name],
			&actualExpanded,
			limits,
		)
		var closeOutErr error
		if out != nil {
			closeOutErr = out.Close()
		}
		closeReadErr := rc.Close()
		if copyErr != nil {
			if outputPath != "" {
				_ = os.Remove(outputPath)
			}
			return nil, copyErr
		}
		if closeOutErr != nil {
			_ = os.Remove(outputPath)
			return nil, closeOutErr
		}
		if closeReadErr != nil {
			if outputPath != "" {
				_ = os.Remove(outputPath)
			}
			return nil, closeReadErr
		}
		if outputPath != "" {
			extracted[name] = struct{}{}
		}
	}
	if len(extracted) != len(runtimeNames) {
		return nil, fmt.Errorf("archive runtime set extraction was incomplete")
	}
	return runtimeNames, nil
}

func validateWindowsZipMember(raw string) (string, error) {
	name := strings.ReplaceAll(raw, "\\", "/")
	segmentsName := strings.TrimSuffix(name, "/")
	if segmentsName == "" || strings.HasPrefix(name, "/") || strings.Contains(name, ":") {
		return "", fmt.Errorf("unsafe zip entry path: %q", raw)
	}
	for _, segment := range strings.Split(segmentsName, "/") {
		if segment == "" || segment == "." || segment == ".." ||
			strings.HasSuffix(segment, ".") || strings.HasSuffix(segment, " ") {
			return "", fmt.Errorf("unsafe zip entry path: %q", raw)
		}
	}
	return name, nil
}

func verifyCandidate(path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), candidateTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "--version")
	cmd.Stdin = nil
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("downloaded binary verification timed out: %w", ctx.Err())
		}
		return fmt.Errorf("downloaded binary failed to run: %w", err)
	}
	return nil
}

func fileSHA256(path string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	source, err := os.Open(path)
	if err != nil {
		return digest, err
	}
	defer source.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, source); err != nil {
		return digest, err
	}
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func filesEqualSHA256(left, right string) (bool, error) {
	leftInfo, err := os.Lstat(left)
	if err != nil {
		return false, err
	}
	rightInfo, err := os.Lstat(right)
	if err != nil {
		return false, err
	}
	if !leftInfo.Mode().IsRegular() || !rightInfo.Mode().IsRegular() ||
		!platformRuntimeSetFileLinkCountOne(left, leftInfo) ||
		!platformRuntimeSetFileLinkCountOne(right, rightInfo) ||
		leftInfo.Size() != rightInfo.Size() {
		return false, nil
	}
	leftDigest, err := fileSHA256(left)
	if err != nil {
		return false, err
	}
	rightDigest, err := fileSHA256(right)
	if err != nil {
		return false, err
	}
	return leftDigest == rightDigest, nil
}

func regularRuntimeFile(path string) bool {
	status, err := os.Lstat(path)
	return err == nil && status.Mode().IsRegular() &&
		platformRuntimeSetFileLinkCountOne(path, status)
}

func runtimeSetNames(directory, binaryName string) ([]string, bool) {
	if !regularRuntimeFile(filepath.Join(directory, binaryName)) {
		return nil, false
	}
	return []string{binaryName}, true
}

func runtimeSetReady(
	directory, binaryName string, verifier func(string) error,
) bool {
	if _, ok := runtimeSetNames(directory, binaryName); !ok {
		return false
	}
	return verifier == nil || verifier(filepath.Join(directory, binaryName)) == nil
}

func requireSafeRuntimeDirectory(directory string) error {
	status, err := os.Lstat(directory)
	if err != nil {
		return err
	}
	if !status.IsDir() || status.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing unsafe package-cache directory: %s", directory)
	}
	return nil
}

func runtimeSetReadyLocked(
	directory, binaryName string, verifier func(string) error,
) (ready bool, result error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return false, err
	}
	if err := requireSafeRuntimeDirectory(directory); err != nil {
		return false, err
	}
	lock, err := acquireRuntimeSetLock(directory)
	if err != nil {
		return false, err
	}
	defer func() {
		attachRuntimeLockReleaseError(&result, releaseRuntimeSetLock(lock))
	}()
	if err := refreshRuntimeSetLock(lock); err != nil {
		return false, err
	}
	if err := reconcileRuntimeBackups(
		directory, binaryName, verifier, lock,
	); err != nil {
		return false, err
	}
	return runtimeSetReady(directory, binaryName, verifier), nil
}

func copyRuntimeStage(
	sourcePath, destinationDirectory string, executable bool,
	maintainLease func() error,
) (string, error) {
	input, err := os.Open(sourcePath)
	if err != nil {
		return "", err
	}
	defer input.Close()
	pattern := ".cbm-runtime-stage-*"
	if executable && runtime.GOOS == "windows" {
		pattern += ".exe"
	}
	output, err := os.CreateTemp(destinationDirectory, pattern)
	if err != nil {
		return "", err
	}
	stagedPath := output.Name()
	mode := os.FileMode(0644)
	if executable {
		mode = 0755
	}
	buffer := make([]byte, 1024*1024)
	lastRefresh := time.Now()
	var copyErr error
	for {
		count, readErr := input.Read(buffer)
		if count > 0 {
			written := 0
			for written < count {
				amount, writeErr := output.Write(buffer[written:count])
				if writeErr != nil {
					copyErr = writeErr
					break
				}
				written += amount
			}
		}
		if copyErr != nil || readErr == io.EOF {
			break
		}
		if readErr != nil {
			copyErr = readErr
			break
		}
		if maintainLease != nil && time.Since(lastRefresh) >= runtimeSetLockHeartbeat {
			if err := maintainLease(); err != nil {
				copyErr = err
				break
			}
			lastRefresh = time.Now()
		}
	}
	chmodErr := output.Chmod(mode)
	syncErr := output.Sync()
	closeErr := output.Close()
	for _, candidate := range []error{copyErr, chmodErr, syncErr, closeErr} {
		if candidate != nil {
			_ = os.Remove(stagedPath)
			return "", candidate
		}
	}
	return stagedPath, nil
}

// A grouped backup is a small process-crash journal. runtimeBackupRetired
// separates a partial retirement from partial publication;
// runtimeBackupCleanupOnly makes deletion retryable after the destination has
// been accepted or restored.
func runtimeBackupDirectoryName(name string) bool {
	if !strings.HasPrefix(name, runtimeBackupPrefix) {
		return false
	}
	token := strings.TrimPrefix(name, runtimeBackupPrefix)
	decoded, err := hex.DecodeString(token)
	return err == nil && token == strings.ToLower(token) &&
		len(decoded) == runtimeSetLockTokenSize
}

func runtimeBackupTargetName(name, binaryName string) bool {
	return name == binaryName
}

func createRuntimeBackupDirectory(directory string) (string, error) {
	for attempt := 0; attempt < 128; attempt++ {
		token, err := runtimeSetLockToken()
		if err != nil {
			return "", err
		}
		backupDirectory := filepath.Join(directory, runtimeBackupPrefix+token)
		if err := os.Mkdir(backupDirectory, 0700); err == nil {
			return backupDirectory, nil
		} else if !os.IsExist(err) {
			return "", err
		}
	}
	return "", fmt.Errorf("could not reserve a package-cache backup transaction")
}

func createRuntimeBackupMarker(backupDirectory, name string) error {
	marker, err := os.OpenFile(
		filepath.Join(backupDirectory, name),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0600,
	)
	if err != nil {
		return err
	}
	syncErr := marker.Sync()
	closeErr := marker.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func inspectRuntimeBackupDirectory(
	directory, entryName, binaryName string,
) (*runtimeBackupTransaction, error) {
	backupDirectory := filepath.Join(directory, entryName)
	directoryStatus, err := os.Lstat(backupDirectory)
	if err != nil || !directoryStatus.IsDir() ||
		directoryStatus.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf(
			"refusing unsafe package-cache backup: %s", backupDirectory,
		)
	}
	backup := &runtimeBackupTransaction{
		path: backupDirectory, files: make(map[string]runtimeBackupFile),
	}
	entries, err := os.ReadDir(backupDirectory)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		candidate := filepath.Join(backupDirectory, name)
		status, err := os.Lstat(candidate)
		if err != nil {
			return nil, err
		}
		if name == runtimeBackupRetired || name == runtimeBackupCleanupOnly {
			if !status.Mode().IsRegular() ||
				!platformRuntimeSetFileLinkCountOne(candidate, status) ||
				status.Size() != 0 {
				return nil, fmt.Errorf(
					"refusing unsafe package-cache backup marker: %s", candidate,
				)
			}
			if name == runtimeBackupRetired {
				backup.retired = true
			} else {
				backup.cleanupOnly = true
			}
			continue
		}
		if !runtimeBackupTargetName(name, binaryName) ||
			!status.Mode().IsRegular() ||
			!platformRuntimeSetFileLinkCountOne(candidate, status) {
			return nil, fmt.Errorf(
				"refusing unsafe package-cache backup member: %s", candidate,
			)
		}
		digest, err := fileSHA256(candidate)
		if err != nil {
			return nil, err
		}
		backup.files[name] = runtimeBackupFile{path: candidate, digest: digest}
	}
	return backup, nil
}

func listRuntimeBackupDirectories(
	directory, binaryName string,
) ([]*runtimeBackupTransaction, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		if runtimeBackupDirectoryName(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	backups := make([]*runtimeBackupTransaction, 0, len(names))
	for _, name := range names {
		backup, err := inspectRuntimeBackupDirectory(directory, name, binaryName)
		if err != nil {
			return nil, err
		}
		backups = append(backups, backup)
	}
	return backups, nil
}

func currentRuntimeTargets(
	directory, binaryName string,
) (map[string]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	targets := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if !runtimeBackupTargetName(name, binaryName) {
			continue
		}
		target := filepath.Join(directory, name)
		status, err := os.Lstat(target)
		if err != nil || !status.Mode().IsRegular() ||
			!platformRuntimeSetFileLinkCountOne(target, status) {
			return nil, fmt.Errorf("refusing unsafe package-cache target: %s", target)
		}
		targets[name] = target
	}
	return targets, nil
}

func cleanupRuntimeBackup(
	backup *runtimeBackupTransaction, lock *runtimeSetLock,
) error {
	if err := assertRuntimeSetLockOwner(lock); err != nil {
		return err
	}
	if !backup.cleanupOnly {
		if err := createRuntimeBackupMarker(
			backup.path, runtimeBackupCleanupOnly,
		); err != nil {
			return err
		}
		backup.cleanupOnly = true
	}
	names := make([]string, 0, len(backup.files))
	for name := range backup.files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := assertRuntimeSetLockOwner(lock); err != nil {
			return err
		}
		member := backup.files[name]
		if !pathMatchesSHA256(member.path, member.digest) {
			return fmt.Errorf(
				"package-cache backup member changed during cleanup: %s", name,
			)
		}
		if err := assertRuntimeSetLockOwner(lock); err != nil {
			return err
		}
		if err := os.Remove(member.path); err != nil {
			return err
		}
		if runtimeBackupCrashObserver != nil {
			if err := runtimeBackupCrashObserver(
				runtimeBackupCleanupRemovedEvent, member.path,
			); err != nil {
				return err
			}
		}
	}
	if backup.retired {
		if err := assertRuntimeSetLockOwner(lock); err != nil {
			return err
		}
		if err := os.Remove(filepath.Join(backup.path, runtimeBackupRetired)); err != nil {
			return err
		}
	}
	if err := assertRuntimeSetLockOwner(lock); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(backup.path, runtimeBackupCleanupOnly)); err != nil {
		return err
	}
	return os.Remove(backup.path)
}

func copyRuntimeBackupFile(
	member runtimeBackupFile, target string, executable bool, lock *runtimeSetLock,
) error {
	staged, err := copyRuntimeStage(
		member.path,
		filepath.Dir(target),
		executable,
		func() error { return refreshRuntimeSetLock(lock) },
	)
	if err != nil {
		return err
	}
	defer os.Remove(staged)
	stagedDigest, err := fileSHA256(staged)
	if err != nil {
		return err
	}
	if stagedDigest != member.digest {
		return fmt.Errorf("package-cache backup changed while it was being restored")
	}
	if err := assertRuntimeSetLockOwner(lock); err != nil {
		return err
	}
	return os.Rename(staged, target)
}

func reconcileRuntimeBackups(
	directory, binaryName string,
	verifier func(string) error,
	lock *runtimeSetLock,
) error {
	if err := refreshRuntimeSetLock(lock); err != nil {
		return err
	}
	backups, err := listRuntimeBackupDirectories(directory, binaryName)
	if err != nil {
		return err
	}
	remaining := make([]*runtimeBackupTransaction, 0, len(backups))
	for _, backup := range backups {
		if backup.cleanupOnly {
			if err := cleanupRuntimeBackup(backup, lock); err != nil {
				return err
			}
		} else {
			remaining = append(remaining, backup)
		}
	}
	backups = remaining
	if len(backups) == 0 {
		return nil
	}
	if runtimeSetReady(directory, binaryName, verifier) {
		for _, backup := range backups {
			if err := cleanupRuntimeBackup(backup, lock); err != nil {
				return err
			}
		}
		return nil
	}
	if len(backups) == 0 {
		return nil
	}
	remaining = remaining[:0]
	for _, backup := range backups {
		if !backup.retired && len(backup.files) == 0 {
			if err := cleanupRuntimeBackup(backup, lock); err != nil {
				return err
			}
		} else {
			remaining = append(remaining, backup)
		}
	}
	backups = remaining
	if len(backups) == 0 {
		return nil
	}
	if len(backups) != 1 {
		return fmt.Errorf(
			"multiple package-cache backup transactions require manual recovery",
		)
	}
	backup := backups[0]
	targets, err := currentRuntimeTargets(directory, binaryName)
	if err != nil {
		return err
	}
	if !backup.retired {
		for name, member := range backup.files {
			if target, exists := targets[name]; exists &&
				!pathMatchesSHA256(target, member.digest) {
				return fmt.Errorf(
					"package-cache backup conflicts with current target: %s", name,
				)
			}
		}
	} else {
		if target, exists := targets[binaryName]; exists {
			if err := assertRuntimeSetLockOwner(lock); err != nil {
				return err
			}
			if err := os.Remove(target); err != nil {
				return err
			}
			delete(targets, binaryName)
		}
		names := make([]string, 0, len(targets))
		for name := range targets {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if err := assertRuntimeSetLockOwner(lock); err != nil {
				return err
			}
			if err := os.Remove(targets[name]); err != nil {
				return err
			}
		}
	}
	restoreNames := make([]string, 0, len(backup.files))
	for name := range backup.files {
		if name != binaryName {
			restoreNames = append(restoreNames, name)
		}
	}
	sort.Strings(restoreNames)
	if _, exists := backup.files[binaryName]; exists {
		restoreNames = append(restoreNames, binaryName)
	}
	for _, name := range restoreNames {
		member := backup.files[name]
		target := filepath.Join(directory, name)
		if !pathMatchesSHA256(target, member.digest) {
			if err := copyRuntimeBackupFile(
				member, target, name == binaryName, lock,
			); err != nil {
				return err
			}
		}
	}
	for name, member := range backup.files {
		if !pathMatchesSHA256(filepath.Join(directory, name), member.digest) {
			return fmt.Errorf("package-cache backup restoration failed: %s", name)
		}
	}
	if err := cleanupRuntimeBackup(backup, lock); err != nil {
		return err
	}
	return nil
}

func pathMatchesSHA256(path string, expected [sha256.Size]byte) bool {
	if !regularRuntimeFile(path) {
		return false
	}
	actual, err := fileSHA256(path)
	return err == nil && actual == expected
}

func runtimeSetLockToken() (string, error) {
	raw := make([]byte, runtimeSetLockTokenSize)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func runtimeSetLockOwner(lockPath string) (runtimeSetLockOwnerRecord, bool) {
	var empty runtimeSetLockOwnerRecord
	status, err := os.Lstat(lockPath)
	if err != nil || status.Mode()&os.ModeSymlink != 0 ||
		(!status.Mode().IsRegular() && !status.IsDir()) {
		return empty, false
	}
	ownerPath := lockPath
	if status.IsDir() {
		ownerPath = filepath.Join(lockPath, runtimeSetLockOwnerFile)
	}
	ownerStatus, err := os.Lstat(ownerPath)
	if err != nil || !ownerStatus.Mode().IsRegular() {
		return empty, false
	}
	ownerFile, err := platformOpenRuntimeSetLockOwner(ownerPath)
	if err != nil {
		return empty, false
	}
	if runtimeSetLockOwnerReadObserver != nil {
		runtimeSetLockOwnerReadObserver()
	}
	data, readErr := io.ReadAll(ownerFile)
	closeErr := ownerFile.Close()
	if readErr != nil || closeErr != nil {
		return empty, false
	}
	var record runtimeSetLockOwnerRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return empty, false
	}
	decoded, decodeErr := hex.DecodeString(record.Token)
	if record.PID <= 0 || record.Token != strings.ToLower(record.Token) ||
		decodeErr != nil || len(decoded) != runtimeSetLockTokenSize {
		return empty, false
	}
	return record, true
}

func runtimeSetSameLockObject(left, right os.FileInfo) bool {
	return left != nil && right != nil && os.SameFile(left, right) &&
		left.Mode().Type() == right.Mode().Type()
}

// captureRuntimeSetLockObject resolves the lock's file identity while its
// canonical pathname still exists. On Windows, os.FileInfo identity can be
// resolved lazily from its pathname; carrying an untouched Lstat result across
// a rename therefore loses the object we meant to revalidate.
func captureRuntimeSetLockObject(
	lockPath string, observedStatus os.FileInfo,
) (os.FileInfo, error) {
	object, err := os.Open(lockPath)
	if err != nil {
		return nil, err
	}
	if runtimeSetLockCaptureObserver != nil {
		runtimeSetLockCaptureObserver()
	}
	descriptorStatus, descriptorErr := object.Stat()
	canonicalStatus, canonicalErr := os.Lstat(lockPath)
	identityOK := descriptorErr == nil && canonicalErr == nil &&
		runtimeSetSameLockObject(observedStatus, descriptorStatus) &&
		runtimeSetSameLockObject(descriptorStatus, canonicalStatus)
	closeErr := object.Close()
	if !identityOK {
		return nil, fmt.Errorf(
			"package-cache runtime-set publication lock ownership changed",
		)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return descriptorStatus, nil
}

func writeRuntimeSetLockRecord(owner *os.File, token string) error {
	if err := owner.Truncate(0); err != nil {
		return err
	}
	if _, err := owner.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := json.NewEncoder(owner).Encode(runtimeSetLockOwnerRecord{
		PID:          os.Getpid(),
		Token:        token,
		LeaseExpires: time.Now().Add(runtimeSetLockLease).UnixMilli(),
	}); err != nil {
		return err
	}
	return owner.Sync()
}

// writeRuntimeSetLockOwner creates the legacy directory-owner shape used by
// migration tests and previous wrapper releases.
func writeRuntimeSetLockOwner(lockPath, token string) error {
	owner, err := os.OpenFile(
		filepath.Join(lockPath, runtimeSetLockOwnerFile),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0600,
	)
	if err != nil {
		return err
	}
	writeErr := json.NewEncoder(owner).Encode(runtimeSetLockOwnerRecord{
		PID: os.Getpid(), Token: token,
	})
	syncErr := owner.Sync()
	closeErr := owner.Close()
	for _, candidate := range []error{writeErr, syncErr, closeErr} {
		if candidate != nil {
			return candidate
		}
	}
	return nil
}

func runtimeSetOwnerReclaimable(
	status os.FileInfo, owner runtimeSetLockOwnerRecord, ownerOK bool,
) bool {
	if ownerOK {
		// Lease expiry and age are not evidence that a syntactically valid
		// owner is dead. A paused live publisher can resume between lock-owner
		// assertions, so reclaim only when the platform proves its PID is gone.
		return !runtimeSetProcessAlive(owner.PID)
	}
	return time.Since(status.ModTime()) >= runtimeSetOwnerlessStale
}

func restoreDisplacedRuntimeSetLock(reclaimed, lockPath string, status os.FileInfo) {
	if status == nil || !status.Mode().IsRegular() {
		return
	}
	if err := os.Link(reclaimed, lockPath); err == nil {
		_ = os.Remove(reclaimed)
	}
}

func runtimeSetTryReclaimLock(lockPath, contenderToken string) bool {
	observedStatus, err := os.Lstat(lockPath)
	if os.IsNotExist(err) {
		return true
	}
	if err != nil || observedStatus.Mode()&os.ModeSymlink != 0 ||
		(!observedStatus.Mode().IsRegular() && !observedStatus.IsDir()) {
		return false
	}
	owner, ownerOK := runtimeSetLockOwner(lockPath)
	if !runtimeSetOwnerReclaimable(observedStatus, owner, ownerOK) {
		return false
	}
	// Pin identity only after the observed owner is reclaimable. On Windows,
	// opening a proven-live lock here can deny the owner's release rename.
	observedStatus, err = captureRuntimeSetLockObject(lockPath, observedStatus)
	if err != nil {
		return os.IsNotExist(err)
	}
	reclaimed := lockPath + ".reclaimed-" + contenderToken
	if err := os.Rename(lockPath, reclaimed); err != nil {
		return os.IsNotExist(err)
	}
	movedStatus, movedErr := os.Lstat(reclaimed)
	if movedErr != nil || !runtimeSetSameLockObject(observedStatus, movedStatus) {
		restoreDisplacedRuntimeSetLock(reclaimed, lockPath, movedStatus)
		return false
	}
	movedOwner, movedOwnerOK := runtimeSetLockOwner(reclaimed)
	if !runtimeSetOwnerReclaimable(movedStatus, movedOwner, movedOwnerOK) {
		restoreDisplacedRuntimeSetLock(reclaimed, lockPath, movedStatus)
		return false
	}
	if movedStatus.IsDir() {
		_ = os.RemoveAll(reclaimed)
	} else {
		_ = os.Remove(reclaimed)
	}
	return true
}

func assertRuntimeSetLockOwner(lock *runtimeSetLock) error {
	if lock == nil || lock.file == nil {
		return fmt.Errorf("missing package-cache runtime-set publication lock")
	}
	descriptorStatus, descriptorErr := lock.file.Stat()
	canonicalStatus, canonicalErr := os.Lstat(lock.path)
	owner, ownerOK := runtimeSetLockOwner(lock.path)
	if descriptorErr != nil || canonicalErr != nil ||
		!runtimeSetSameLockObject(descriptorStatus, canonicalStatus) ||
		!ownerOK || owner.PID != os.Getpid() || owner.Token != lock.token {
		return fmt.Errorf(
			"package-cache runtime-set publication lock ownership changed",
		)
	}
	return nil
}

func refreshRuntimeSetLock(lock *runtimeSetLock) error {
	if err := assertRuntimeSetLockOwner(lock); err != nil {
		return err
	}
	if err := writeRuntimeSetLockRecord(lock.file, lock.token); err != nil {
		return err
	}
	return assertRuntimeSetLockOwner(lock)
}

func acquireRuntimeSetLock(destinationDirectory string) (*runtimeSetLock, error) {
	token, err := runtimeSetLockToken()
	if err != nil {
		return nil, err
	}
	lockPath := filepath.Join(destinationDirectory, runtimeSetLockName)
	claimPath := lockPath + ".claim-" + token
	deadline := time.Now().Add(runtimeSetLockWait)
	for {
		owner, claimErr := os.OpenFile(
			claimPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600,
		)
		if claimErr != nil {
			return nil, claimErr
		}
		if err := writeRuntimeSetLockRecord(owner, token); err != nil {
			_ = owner.Close()
			_ = os.Remove(claimPath)
			return nil, err
		}
		if runtimeSetLockClaimObserver != nil {
			if err := runtimeSetLockClaimObserver(); err != nil {
				_ = owner.Close()
				_ = os.Remove(claimPath)
				return nil, err
			}
		}
		descriptorStatus, descriptorErr := owner.Stat()
		claimStatus, claimStatusErr := os.Lstat(claimPath)
		if descriptorErr != nil || claimStatusErr != nil ||
			!runtimeSetSameLockObject(descriptorStatus, claimStatus) {
			_ = owner.Close()
			_ = os.Remove(claimPath)
			return nil, fmt.Errorf(
				"package-cache runtime-set publication lock ownership changed",
			)
		}
		if err := owner.Close(); err != nil {
			_ = os.Remove(claimPath)
			return nil, err
		}
		linkErr := os.Link(claimPath, lockPath)
		if linkErr == nil {
			if err := os.Remove(claimPath); err != nil {
				return nil, err
			}
			owner, openErr := os.OpenFile(lockPath, os.O_RDWR, 0)
			if openErr != nil {
				return nil, openErr
			}
			reopenedStatus, reopenedErr := owner.Stat()
			canonicalStatus, canonicalErr := os.Lstat(lockPath)
			lockOwner, ownerOK := runtimeSetLockOwner(lockPath)
			if reopenedErr != nil || canonicalErr != nil ||
				!runtimeSetSameLockObject(descriptorStatus, reopenedStatus) ||
				!runtimeSetSameLockObject(reopenedStatus, canonicalStatus) ||
				!ownerOK || lockOwner.PID != os.Getpid() ||
				lockOwner.Token != token {
				_ = owner.Close()
				return nil, fmt.Errorf(
					"package-cache runtime-set publication lock ownership changed",
				)
			}
			return &runtimeSetLock{path: lockPath, token: token, file: owner}, nil
		}
		_ = os.Remove(claimPath)
		if !os.IsExist(linkErr) {
			return nil, linkErr
		}
		if runtimeSetTryReclaimLock(lockPath, token) {
			continue
		}
		if runtimeSetLockWaitObserver != nil {
			runtimeSetLockWaitObserver()
		}
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf(
				"timed out waiting for package-cache runtime-set publication lock",
			)
		}
		time.Sleep(runtimeSetLockPoll)
	}
}

func releaseRuntimeSetLock(lock *runtimeSetLock) error {
	if err := assertRuntimeSetLockOwner(lock); err != nil {
		if lock != nil && lock.file != nil {
			_ = lock.file.Close()
			lock.file = nil
		}
		return err
	}
	descriptorStatus, descriptorErr := lock.file.Stat()
	canonicalStatus, canonicalErr := os.Lstat(lock.path)
	owner, ownerOK := runtimeSetLockOwner(lock.path)
	if descriptorErr != nil || canonicalErr != nil ||
		!runtimeSetSameLockObject(descriptorStatus, canonicalStatus) ||
		!ownerOK || owner.PID != os.Getpid() || owner.Token != lock.token {
		_ = lock.file.Close()
		lock.file = nil
		return fmt.Errorf(
			"package-cache runtime-set publication lock ownership changed",
		)
	}
	if err := lock.file.Close(); err != nil {
		lock.file = nil
		return err
	}
	lock.file = nil
	released := lock.path + ".released-" + lock.token
	if err := os.Rename(lock.path, released); err != nil {
		return err
	}
	releasedStatus, releasedErr := os.Lstat(released)
	owner, ownerOK = runtimeSetLockOwner(released)
	if releasedErr != nil ||
		!runtimeSetSameLockObject(descriptorStatus, releasedStatus) ||
		!ownerOK || owner.PID != os.Getpid() || owner.Token != lock.token {
		restoreDisplacedRuntimeSetLock(released, lock.path, releasedStatus)
		return fmt.Errorf(
			"package-cache runtime-set publication lock ownership changed",
		)
	}
	return os.Remove(released)
}

func attachRuntimeLockReleaseError(result *error, releaseErr error) {
	if releaseErr == nil {
		return
	}
	if *result == nil {
		*result = releaseErr
		return
	}
	*result = fmt.Errorf("%v; runtime lock release failed: %w", *result, releaseErr)
}

func publishRuntimeSetWithRecoveryAndRenamer(
	sourceDirectory, destinationDirectory, binaryName string,
	verifier func(string) error,
	renameFile func(string, string) error,
) (result error) {
	sourceNames, ok := runtimeSetNames(sourceDirectory, binaryName)
	if !ok {
		return fmt.Errorf("source release does not contain a complete runtime set")
	}
	if verifier != nil {
		if err := verifier(filepath.Join(sourceDirectory, binaryName)); err != nil {
			return fmt.Errorf("source runtime binary failed verification: %w", err)
		}
	}
	if err := os.MkdirAll(destinationDirectory, 0755); err != nil {
		return err
	}
	if err := requireSafeRuntimeDirectory(destinationDirectory); err != nil {
		return err
	}
	lock, err := acquireRuntimeSetLock(destinationDirectory)
	if err != nil {
		return err
	}
	defer func() {
		attachRuntimeLockReleaseError(&result, releaseRuntimeSetLock(lock))
	}()
	if err := refreshRuntimeSetLock(lock); err != nil {
		return err
	}
	if err := reconcileRuntimeBackups(
		destinationDirectory, binaryName, verifier, lock,
	); err != nil {
		return err
	}
	// A contender may have committed while this process waited. Its complete
	// runtime set wins and is never retired or mixed with this contender.
	if runtimeSetReady(destinationDirectory, binaryName, verifier) {
		return nil
	}

	staged := make(map[string]string, len(sourceNames))
	backupDirectory := ""
	defer func() {
		for _, stagedPath := range staged {
			_ = os.Remove(stagedPath)
		}
	}()

	operationErr := func() error {
		for _, name := range sourceNames {
			stagedPath, err := copyRuntimeStage(
				filepath.Join(sourceDirectory, name),
				destinationDirectory,
				name == binaryName,
				func() error { return refreshRuntimeSetLock(lock) },
			)
			if err != nil {
				return err
			}
			staged[name] = stagedPath
		}
		if err := refreshRuntimeSetLock(lock); err != nil {
			return err
		}
		var err error
		backupDirectory, err = createRuntimeBackupDirectory(destinationDirectory)
		if err != nil {
			return err
		}

		retireNames := []string{binaryName}
		retired := make(map[string]struct{}, len(retireNames))
		// Retire the executable first so a partial publication is never ready.
		for _, name := range retireNames {
			if err := assertRuntimeSetLockOwner(lock); err != nil {
				return err
			}
			if _, duplicate := retired[name]; duplicate {
				continue
			}
			retired[name] = struct{}{}
			target := filepath.Join(destinationDirectory, name)
			status, err := os.Lstat(target)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return err
			}
			if !status.Mode().IsRegular() ||
				!platformRuntimeSetFileLinkCountOne(target, status) {
				return fmt.Errorf("refusing unsafe package-cache target: %s", target)
			}
			backup := filepath.Join(backupDirectory, name)
			if err := renameFile(target, backup); err != nil {
				return err
			}
		}
		if err := assertRuntimeSetLockOwner(lock); err != nil {
			return err
		}
		if runtimeBackupCrashObserver != nil {
			if err := runtimeBackupCrashObserver(
				runtimeBackupBeforeMarkerEvent, backupDirectory,
			); err != nil {
				return err
			}
		}
		if err := createRuntimeBackupMarker(
			backupDirectory, runtimeBackupRetired,
		); err != nil {
			return err
		}

		// runtimeSetNames orders every sidecar before the binary readiness signal.
		for _, name := range sourceNames {
			if err := assertRuntimeSetLockOwner(lock); err != nil {
				return err
			}
			target := filepath.Join(destinationDirectory, name)
			if err := renameFile(staged[name], target); err != nil {
				identical, compareErr := filesEqualSHA256(staged[name], target)
				if compareErr != nil || !identical {
					return err
				}
				_ = os.Remove(staged[name])
			}
			delete(staged, name)
		}
		if !runtimeSetReady(destinationDirectory, binaryName, verifier) {
			return fmt.Errorf("published package-cache runtime set failed verification")
		}
		if err := assertRuntimeSetLockOwner(lock); err != nil {
			return err
		}
		return nil
	}()

	if operationErr != nil {
		if lockErr := assertRuntimeSetLockOwner(lock); lockErr != nil {
			return fmt.Errorf("publication failed and lock ownership changed: %v: %w", operationErr, lockErr)
		}
	}
	if operationErr == nil {
		backup, err := inspectRuntimeBackupDirectory(
			destinationDirectory, filepath.Base(backupDirectory), binaryName,
		)
		if err != nil {
			return err
		}
		if err := cleanupRuntimeBackup(backup, lock); err != nil {
			return err
		}
		return nil
	}
	preserveWinner := runtimeSetReady(
		destinationDirectory, binaryName, verifier,
	)
	if backupDirectory != "" {
		if recoveryErr := reconcileRuntimeBackups(
			destinationDirectory, binaryName, verifier, lock,
		); recoveryErr != nil {
			return fmt.Errorf(
				"%v; package-cache recovery failed: %w", operationErr, recoveryErr,
			)
		}
	}
	if preserveWinner {
		return nil
	}
	return operationErr
}

func publishRuntimeSetWithRecovery(
	sourceDirectory, destinationDirectory, binaryName string,
	verifier func(string) error,
) error {
	return publishRuntimeSetWithRecoveryAndRenamer(
		sourceDirectory,
		destinationDirectory,
		binaryName,
		verifier,
		os.Rename,
	)
}

func execBinary(executable string, args []string) error {
	if runtime.GOOS == "windows" {
		cmd := exec.Command(executable, args...)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	}
	return syscall.Exec(executable, append([]string{executable}, args...), os.Environ())
}

func createMutationRuntimeSnapshot(
	executable string,
	verifier func(string) error,
	maintainLease func() error,
) (snapshotDirectory, snapshotExecutable string, result error) {
	sourceDirectory := filepath.Dir(executable)
	binaryName := filepath.Base(executable)
	names, ok := runtimeSetNames(sourceDirectory, binaryName)
	if !ok {
		return "", "", fmt.Errorf(
			"cached runtime assets changed before mutation snapshot",
		)
	}

	snapshotDirectory, err := platformCreatePrivateMutationRuntimeDirectory()
	if err != nil {
		return "", "", err
	}
	createdDirectory := snapshotDirectory
	defer func() {
		if result != nil {
			_ = os.RemoveAll(createdDirectory)
		}
	}()
	if err := requireSafeRuntimeDirectory(snapshotDirectory); err != nil {
		return "", "", fmt.Errorf(
			"owner-private mutation runtime snapshot is unsafe: %w", err,
		)
	}

	for _, name := range names {
		sourcePath := filepath.Join(sourceDirectory, name)
		expectedDigest, err := fileSHA256(sourcePath)
		if err != nil {
			return "", "", err
		}
		stagedPath, err := copyRuntimeStage(
			sourcePath,
			snapshotDirectory,
			name == binaryName,
			maintainLease,
		)
		if err != nil {
			return "", "", err
		}
		stagedDigest, digestErr := fileSHA256(stagedPath)
		if digestErr != nil || stagedDigest != expectedDigest ||
			!pathMatchesSHA256(sourcePath, expectedDigest) {
			_ = os.Remove(stagedPath)
			return "", "", fmt.Errorf(
				"cached runtime asset changed while mutation snapshot was copied: %s",
				name,
			)
		}
		if err := os.Rename(
			stagedPath, filepath.Join(snapshotDirectory, name),
		); err != nil {
			_ = os.Remove(stagedPath)
			return "", "", err
		}
	}

	snapshotExecutable = filepath.Join(snapshotDirectory, binaryName)
	if !runtimeSetReady(
		snapshotDirectory, binaryName, verifier,
	) {
		return "", "", fmt.Errorf(
			"private mutation runtime snapshot failed verification",
		)
	}
	return snapshotDirectory, snapshotExecutable, nil
}

func runMutationBinary(executable string, args []string) error {
	cmd := exec.Command(executable, args...)
	cmd.Env = mutationSnapshotEnvironment()
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func execBinaryWithRuntimeLockAndRunner(
	executable string,
	args []string,
	verifier func(string) error,
	runner func(string, []string) error,
) (result error) {
	if runner == nil {
		return fmt.Errorf("missing mutation runtime runner")
	}
	directory := filepath.Dir(executable)
	if target, ok := mutationTargetDirectory(args); ok &&
		pathsReferToSameDirectory(target, directory) {
		return fmt.Errorf(
			"refusing mutation target that overlaps the shared package cache: %s",
			target,
		)
	}
	if err := requireSafeRuntimeDirectory(directory); err != nil {
		return err
	}
	lock, err := acquireRuntimeSetLock(directory)
	if err != nil {
		return err
	}
	lockHeld := true
	defer func() {
		if lockHeld {
			attachRuntimeLockReleaseError(&result, releaseRuntimeSetLock(lock))
		}
	}()
	if err := refreshRuntimeSetLock(lock); err != nil {
		return err
	}
	if !runtimeSetReady(
		directory, filepath.Base(executable), verifier,
	) {
		return fmt.Errorf("cached runtime assets changed before mutation launch")
	}
	snapshotDirectory, snapshotExecutable, err := createMutationRuntimeSnapshot(
		executable,
		verifier,
		func() error { return refreshRuntimeSetLock(lock) },
	)
	if err != nil {
		return err
	}
	defer func() {
		// A hard-killed wrapper can bypass this cleanup while its native child
		// still consumes adjacent snapshot assets. Do not scavenge such a
		// directory by PID or age, and never replace the native command result
		// merely because best-effort cleanup leaked an isolated private copy.
		if cleanupErr := runtimeMutationSnapshotCleanup(snapshotDirectory); cleanupErr != nil {
			fmt.Fprintf(
				os.Stderr,
				"codebase-memory-mcp: warning: private mutation snapshot cleanup failed: %v\n",
				cleanupErr,
			)
		}
	}()
	releaseErr := releaseRuntimeSetLock(lock)
	lockHeld = false
	if releaseErr != nil {
		return releaseErr
	}
	return runner(snapshotExecutable, args)
}

func execBinaryWithRuntimeLock(executable string, args []string) error {
	return execBinaryWithRuntimeLockAndRunner(
		executable,
		args,
		verifyCandidate,
		runMutationBinary,
	)
}
