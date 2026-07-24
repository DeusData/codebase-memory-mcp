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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

const (
	repo                = "DeusData/codebase-memory-mcp"
	version             = "0.8.1"
	windowsLauncherName = "codebase-memory-mcp.exe"
	windowsPayloadName  = "codebase-memory-mcp.payload.exe"

	maxRedirects            = 5
	requestTimeout          = 2 * time.Minute
	connectTimeout          = 15 * time.Second
	responseHeaderTimeout   = 30 * time.Second
	candidateTimeout        = 15 * time.Second
	maxChecksumManifestSize = 1024 * 1024

	windowsPairLockName           = ".codebase-memory-mcp-pair.lock"
	windowsPairLockWait           = 45 * time.Second
	windowsPairOwnerlessStale     = 30 * time.Second
	windowsPairLockPoll           = 25 * time.Millisecond
	windowsPairLockOwnerFile      = "owner"
	windowsPairTransactionTagSize = 16
)

type windowsPairLock struct {
	path  string
	token string
}

func main() {
	executable, err := ensureBinary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "codebase-memory-mcp: %v\n", err)
		os.Exit(1)
	}
	if err := execBinary(executable, os.Args[1:]); err != nil {
		if runtime.GOOS == "windows" {
			if exitErr, ok := err.(*exec.ExitError); ok {
				printPortableMutationGuidance(os.Args[1:])
				os.Exit(exitErr.ExitCode())
			}
		}
		fmt.Fprintf(os.Stderr, "codebase-memory-mcp: %v\n", err)
		os.Exit(1)
	}
}

func ensureBinary() (string, error) {
	payload := binPath()
	ready := regularFileExists(payload)
	if runtime.GOOS == "windows" {
		ready = ready && regularFileExists(windowsLauncherPath())
		if ready {
			ready = verifyCandidate(windowsLauncherPath()) == nil
		}
	}
	if ready {
		return executionPathForOS(payload, runtime.GOOS), nil
	}
	if err := download(payload); err != nil {
		return "", err
	}
	return executionPathForOS(payload, runtime.GOOS), nil
}

func binPath() string {
	name := "codebase-memory-mcp"
	if runtime.GOOS == "windows" {
		// The payload remains the immutable exact-version pair's readiness
		// signal. executionPathForOS selects its adjacent permanent launcher.
		name = windowsPayloadName
	}
	return filepath.Join(cacheDir(), version, name)
}

func executionPathForOS(payload, targetOS string) string {
	if targetOS == "windows" {
		return filepath.Join(filepath.Dir(payload), windowsLauncherName)
	}
	return payload
}

func windowsLauncherPath() string {
	return filepath.Join(filepath.Dir(binPath()), windowsLauncherName)
}

func regularFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

func printPortableMutationGuidance(args []string) {
	action := portableMutationAction(args)
	if action == "" {
		return
	}
	packageCommand := "go install github.com/DeusData/codebase-memory-mcp/pkg/go/cmd/codebase-memory-mcp@latest"
	if action == "uninstall" {
		packageCommand = "Remove-Item (Get-Command codebase-memory-mcp).Source"
	}
	fmt.Fprintf(
		os.Stderr,
		"This Go Windows copy is portable. Use %q for package maintenance, or run %q once to create a managed launcher with coordinated self-update/uninstall.\n",
		packageCommand,
		"codebase-memory-mcp install --yes",
	)
}

func portableMutationAction(args []string) string {
	for _, argument := range args {
		switch argument {
		case "cli", "hook-augment", "config", "install", "--help", "-h", "--version":
			return ""
		case "update", "uninstall":
			return argument
		}
	}
	return ""
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
	archive := fmt.Sprintf("codebase-memory-mcp-%s-%s%s.%s", platform, arch, portable, ext)
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

	binName := "codebase-memory-mcp"
	requiredNames := []string{binName}
	archiveNames := requiredNames
	if platform == "windows" {
		binName = windowsPayloadName
		requiredNames = []string{windowsLauncherName, windowsPayloadName}
		archiveNames = []string{
			windowsLauncherName,
			windowsPayloadName,
			"LICENSE",
			"install.ps1",
			"THIRD_PARTY_NOTICES.md",
		}
	}

	if ext == "tar.gz" {
		if err := extractTarGz(archivePath, tmp, binName); err != nil {
			return fmt.Errorf("extraction failed: %w", err)
		}
	} else {
		if err := extractZip(archivePath, tmp, archiveNames, requiredNames); err != nil {
			return fmt.Errorf("extraction failed: %w", err)
		}
	}
	for _, name := range requiredNames {
		extracted := filepath.Join(tmp, name)
		if err := os.Chmod(extracted, 0755); err != nil {
			return fmt.Errorf("could not set candidate permissions for %s: %w", name, err)
		}
	}
	if platform == "windows" {
		// In portable mode the canonical launcher must successfully resolve the
		// adjacent payload before the pair is cached.
		if err := verifyCandidate(filepath.Join(tmp, windowsLauncherName)); err != nil {
			return fmt.Errorf("downloaded launcher failed verification: %w", err)
		}
	} else if err := verifyCandidate(filepath.Join(tmp, binName)); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return fmt.Errorf("could not create cache dir: %w", err)
	}

	if platform == "windows" {
		if err := installWindowsPairAtomically(tmp, filepath.Dir(dest)); err != nil {
			return fmt.Errorf("could not install Windows release pair: %w", err)
		}
	} else if err := installCandidateAtomically(filepath.Join(tmp, binName), dest); err != nil {
		return fmt.Errorf("could not install binary: %w", err)
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
	if err := validateURLScheme(rawURL); err != nil {
		return err
	}
	resp, err := httpsOnlyClient.Get(rawURL) //nolint:gosec
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d for %s", resp.StatusCode, rawURL)
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, resp.Body)
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
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

func extractTarGz(archivePath, destDir, targetFile string) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	found := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := strings.ReplaceAll(hdr.Name, "\\", "/")
		if name == targetFile {
			if found {
				os.Remove(filepath.Join(destDir, targetFile))
				return fmt.Errorf("duplicate %s in archive", targetFile)
			}
			found = true
			out, err := os.OpenFile(
				filepath.Join(destDir, targetFile),
				os.O_WRONLY|os.O_CREATE|os.O_EXCL,
				0600,
			)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(out, tr) //nolint:gosec
			closeErr := out.Close()
			if copyErr != nil {
				os.Remove(filepath.Join(destDir, targetFile))
				return copyErr
			}
			if closeErr != nil {
				os.Remove(filepath.Join(destDir, targetFile))
				return closeErr
			}
		}
	}
	if !found {
		return fmt.Errorf("%s not found in archive", targetFile)
	}
	return nil
}

func extractZip(
	archivePath, destDir string,
	archiveNames, extractNames []string,
) error {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer r.Close()
	seen := make(map[string]struct{}, len(r.File))
	required := make(map[string]struct{}, len(archiveNames))
	targets := make(map[string]*zip.File, len(extractNames))
	for _, name := range archiveNames {
		required[name] = struct{}{}
	}
	for _, f := range r.File {
		name, err := validateWindowsZipMember(f.Name)
		if err != nil {
			return err
		}
		key := strings.ToLower(strings.TrimSuffix(name, "/"))
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate or case-conflicting zip entry: %q", f.Name)
		}
		seen[key] = struct{}{}
		if _, ok := required[name]; !ok || f.FileInfo().IsDir() {
			return fmt.Errorf(
				"archive must contain only the exact root files: %s",
				strings.Join(archiveNames, ", "),
			)
		}
		for _, extractName := range extractNames {
			if name == extractName {
				targets[name] = f
			}
		}
	}
	if len(seen) != len(archiveNames) || len(targets) != len(extractNames) {
		return fmt.Errorf(
			"archive must contain exactly one of each required root file: %s",
			strings.Join(archiveNames, ", "),
		)
	}

	for _, name := range extractNames {
		target := targets[name]
		rc, err := target.Open()
		if err != nil {
			return err
		}
		outputPath := filepath.Join(destDir, name)
		out, err := os.OpenFile(
			outputPath,
			os.O_WRONLY|os.O_CREATE|os.O_EXCL,
			0600,
		)
		if err != nil {
			rc.Close()
			return err
		}
		_, copyErr := io.Copy(out, rc) //nolint:gosec
		closeOutErr := out.Close()
		closeReadErr := rc.Close()
		if copyErr != nil {
			os.Remove(outputPath)
			return copyErr
		}
		if closeOutErr != nil {
			os.Remove(outputPath)
			return closeOutErr
		}
		if closeReadErr != nil {
			os.Remove(outputPath)
			return closeReadErr
		}
	}
	return nil
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
	leftInfo, err := os.Stat(left)
	if err != nil {
		return false, err
	}
	rightInfo, err := os.Stat(right)
	if err != nil {
		return false, err
	}
	if !leftInfo.Mode().IsRegular() || !rightInfo.Mode().IsRegular() ||
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

func installCandidateAtomically(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	pattern := "." + filepath.Base(dst) + ".*.tmp"
	if runtime.GOOS == "windows" {
		pattern += ".exe"
	}
	out, err := os.CreateTemp(filepath.Dir(dst), pattern)
	if err != nil {
		return err
	}
	staged := out.Name()
	defer os.Remove(staged)
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Chmod(0755); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := verifyCandidate(staged); err != nil {
		return err
	}
	if err := os.Rename(staged, dst); err != nil {
		identical, compareErr := filesEqualSHA256(staged, dst)
		if compareErr == nil && identical {
			return verifyCandidate(dst)
		}
		return err
	}
	return nil
}

func windowsPairToken() (string, error) {
	raw := make([]byte, windowsPairTransactionTagSize)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func windowsPairLockOwner(lockPath string) (int, string, bool) {
	data, err := os.ReadFile(filepath.Join(lockPath, windowsPairLockOwnerFile))
	if err != nil {
		return 0, "", false
	}
	var pid int
	var token string
	var extra string
	count, _ := fmt.Sscanf(string(data), "%d %s %s", &pid, &token, &extra)
	decoded, decodeErr := hex.DecodeString(token)
	if count != 2 || pid <= 0 || decodeErr != nil ||
		len(decoded) != windowsPairTransactionTagSize {
		return 0, "", false
	}
	return pid, token, true
}

func windowsPairTryReclaimLock(lockPath, contenderToken string) bool {
	pid, _, ownerOK := windowsPairLockOwner(lockPath)
	reclaim := ownerOK && !cbmWindowsLockProcessAlive(pid)
	if !ownerOK {
		status, err := os.Stat(lockPath)
		if os.IsNotExist(err) {
			return true
		}
		if err != nil {
			return false
		}
		reclaim = time.Since(status.ModTime()) >= windowsPairOwnerlessStale
	}
	if !reclaim {
		return false
	}
	reclaimed := lockPath + ".reclaimed-" + contenderToken
	if err := os.Rename(lockPath, reclaimed); err != nil {
		return os.IsNotExist(err)
	}
	_ = os.RemoveAll(reclaimed)
	return true
}

func acquireWindowsPairLock(dstDir string) (*windowsPairLock, error) {
	token, err := windowsPairToken()
	if err != nil {
		return nil, err
	}
	lockPath := filepath.Join(dstDir, windowsPairLockName)
	deadline := time.Now().Add(windowsPairLockWait)
	for {
		if err := os.Mkdir(lockPath, 0700); err == nil {
			owner := []byte(fmt.Sprintf("%d %s\n", os.Getpid(), token))
			ownerPath := filepath.Join(lockPath, windowsPairLockOwnerFile)
			if err := os.WriteFile(ownerPath, owner, 0600); err != nil {
				_ = os.RemoveAll(lockPath)
				return nil, err
			}
			return &windowsPairLock{path: lockPath, token: token}, nil
		} else if !os.IsExist(err) {
			return nil, err
		}
		if windowsPairTryReclaimLock(lockPath, token) {
			continue
		}
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf(
				"timed out waiting for Windows package-cache publication lock")
		}
		time.Sleep(windowsPairLockPoll)
	}
}

func releaseWindowsPairLock(lock *windowsPairLock) error {
	if lock == nil {
		return fmt.Errorf("missing Windows package-cache publication lock")
	}
	pid, token, ok := windowsPairLockOwner(lock.path)
	if !ok || pid != os.Getpid() || token != lock.token {
		return fmt.Errorf("Windows package-cache publication lock ownership changed")
	}
	if err := os.Remove(filepath.Join(lock.path, windowsPairLockOwnerFile)); err != nil {
		return err
	}
	return os.Remove(lock.path)
}

func windowsPairReady(dstDir string, verifier func(string) error) bool {
	launcher := filepath.Join(dstDir, windowsLauncherName)
	payload := filepath.Join(dstDir, windowsPayloadName)
	launcherStatus, launcherErr := os.Lstat(launcher)
	payloadStatus, payloadErr := os.Lstat(payload)
	return launcherErr == nil && payloadErr == nil &&
		launcherStatus.Mode().IsRegular() && payloadStatus.Mode().IsRegular() &&
		verifier(launcher) == nil
}

func copyWindowsPairStage(src, dst string) error {
	input, err := os.Open(src)
	if err != nil {
		return err
	}
	output, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		_ = input.Close()
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeInputErr := input.Close()
	chmodErr := output.Chmod(0755)
	syncErr := output.Sync()
	closeOutputErr := output.Close()
	for _, candidate := range []error{
		copyErr, closeInputErr, chmodErr, syncErr, closeOutputErr,
	} {
		if candidate != nil {
			return candidate
		}
	}
	return nil
}

func pathMatchesSHA256(path string, expected [sha256.Size]byte) bool {
	status, err := os.Lstat(path)
	if err != nil || !status.Mode().IsRegular() {
		return false
	}
	actual, err := fileSHA256(path)
	return err == nil && actual == expected
}

func installWindowsPairAtomicallyWithVerifier(
	srcDir, dstDir string, verifier func(string) error,
) (result error) {
	names := []string{windowsLauncherName, windowsPayloadName}
	if err := verifier(filepath.Join(srcDir, windowsLauncherName)); err != nil {
		return fmt.Errorf("source Windows release pair failed verification: %w", err)
	}
	lock, err := acquireWindowsPairLock(dstDir)
	if err != nil {
		return err
	}
	defer func() {
		if releaseErr := releaseWindowsPairLock(lock); result == nil && releaseErr != nil {
			result = releaseErr
		}
	}()

	// A contender may have committed while this process waited. Preserve that
	// complete pair regardless of whether its authenticated release variant is
	// byte-identical to this contender.
	if windowsPairReady(dstDir, verifier) {
		return nil
	}

	transaction, err := windowsPairToken()
	if err != nil {
		return err
	}
	staged := make(map[string]string, len(names))
	stagedDigests := make(map[string][sha256.Size]byte, len(names))
	backups := make(map[string]string, len(names))
	publishedDigests := make(map[string][sha256.Size]byte, len(names))
	defer func() {
		for _, stagedPath := range staged {
			_ = os.Remove(stagedPath)
		}
	}()

	operationErr := func() error {
		for _, name := range names {
			stagePath := filepath.Join(
				dstDir, ".cbm-pair-stage-"+transaction+"-"+name)
			if err := copyWindowsPairStage(
				filepath.Join(srcDir, name), stagePath); err != nil {
				return err
			}
			staged[name] = stagePath
			digest, err := fileSHA256(stagePath)
			if err != nil {
				return err
			}
			stagedDigests[name] = digest
		}

		for _, name := range names {
			target := filepath.Join(dstDir, name)
			status, err := os.Lstat(target)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return err
			}
			if !status.Mode().IsRegular() {
				return fmt.Errorf(
					"refusing unsafe Windows package-cache target: %s", target)
			}
			backup := filepath.Join(
				dstDir, ".cbm-pair-backup-"+transaction+"-"+name)
			if err := os.Rename(target, backup); err != nil {
				return err
			}
			backups[name] = backup
		}

		// Payload is the readiness signal and is deliberately published last.
		for _, name := range names {
			target := filepath.Join(dstDir, name)
			if err := os.Rename(staged[name], target); err != nil {
				return err
			}
			delete(staged, name)
			publishedDigests[name] = stagedDigests[name]
		}
		if !windowsPairReady(dstDir, verifier) {
			return fmt.Errorf("published Windows package-cache pair failed verification")
		}
		return nil
	}()

	if operationErr == nil || windowsPairReady(dstDir, verifier) {
		for _, backup := range backups {
			_ = os.Remove(backup)
		}
		return nil
	}

	// Roll back only files whose digest proves this transaction published them.
	for index := len(names) - 1; index >= 0; index-- {
		name := names[index]
		digest, ok := publishedDigests[name]
		target := filepath.Join(dstDir, name)
		if ok && pathMatchesSHA256(target, digest) {
			_ = os.Remove(target)
		}
	}
	for _, name := range names {
		backup, ok := backups[name]
		if !ok {
			continue
		}
		target := filepath.Join(dstDir, name)
		if _, err := os.Lstat(target); os.IsNotExist(err) {
			_ = os.Rename(backup, target)
		}
	}
	return operationErr
}

func installWindowsPairAtomically(srcDir, dstDir string) error {
	return installWindowsPairAtomicallyWithVerifier(
		srcDir, dstDir, verifyCandidate)
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
