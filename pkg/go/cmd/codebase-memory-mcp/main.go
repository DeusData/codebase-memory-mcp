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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

const (
	repo    = "DeusData/codebase-memory-mcp"
	version = "0.6.0"
)

func main() {
	bin, err := ensureBinary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "codebase-memory-mcp: %v\n", err)
		os.Exit(1)
	}
	if err := execBinary(bin, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "codebase-memory-mcp: %v\n", err)
		os.Exit(1)
	}
}

func ensureBinary() (string, error) {
	bin := binPath()
	if _, err := os.Stat(bin); err == nil {
		return bin, nil
	}
	if err := download(bin); err != nil {
		return "", err
	}
	return bin, nil
}

func binPath() string {
	name := "codebase-memory-mcp"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(cacheDir(), version, name)
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

	archive := fmt.Sprintf("codebase-memory-mcp-%s-%s.%s", platform, arch, ext)
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

	// Verify checksum if available (non-fatal if checksums.txt unreachable)
	if checksums, err := fetchChecksums(checksumURL); err == nil {
		if expected, ok := checksums[archive]; ok {
			if err := verifyChecksum(archivePath, expected); err != nil {
				return err
			}
		}
	}

	binName := "codebase-memory-mcp"
	if platform == "windows" {
		binName += ".exe"
	}

	if ext == "tar.gz" {
		if err := extractTarGz(archivePath, tmp, binName); err != nil {
			return fmt.Errorf("extraction failed: %w", err)
		}
	} else {
		if err := extractZip(archivePath, tmp, binName); err != nil {
			return fmt.Errorf("extraction failed: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return fmt.Errorf("could not create cache dir: %w", err)
	}

	if err := copyFile(filepath.Join(tmp, binName), dest); err != nil {
		return fmt.Errorf("could not install binary: %w", err)
	}

	if err := os.Chmod(dest, 0755); err != nil {
		return fmt.Errorf("could not set permissions: %w", err)
	}

	return nil
}

// validateURLScheme rejects non-https URLs before any fetch (defense-in-depth).
func validateURLScheme(rawURL string) error {
	if !strings.HasPrefix(rawURL, "https://") {
		return fmt.Errorf("refusing non-https URL: %s", rawURL)
	}
	return nil
}

// httpsOnlyClient returns an HTTP client that rejects non-HTTPS redirects.
var httpsOnlyClient = &http.Client{
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if req.URL.Scheme != "https" {
			return fmt.Errorf("refusing non-https redirect to %s", req.URL)
		}
		if len(via) >= 10 {
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
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
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
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string)
	for _, line := range strings.Split(string(body), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 {
			result[parts[1]] = parts[0]
		}
	}
	return result, nil
}

func verifyChecksum(path, expected string) error {
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
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if filepath.Base(hdr.Name) == targetFile {
			out, err := os.Create(filepath.Join(destDir, targetFile))
			if err != nil {
				return err
			}
			defer out.Close()
			_, err = io.Copy(out, tr) //nolint:gosec
			return err
		}
	}
	return fmt.Errorf("%s not found in archive", targetFile)
}

func extractZip(archivePath, destDir, targetFile string) error {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer r.Close()
	for _, f := range r.File {
		if filepath.Base(f.Name) == targetFile {
			rc, err := f.Open()
			if err != nil {
				return err
			}
			defer rc.Close()
			out, err := os.Create(filepath.Join(destDir, targetFile))
			if err != nil {
				return err
			}
			defer out.Close()
			_, err = io.Copy(out, rc) //nolint:gosec
			return err
		}
	}
	return fmt.Errorf("%s not found in archive", targetFile)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

func execBinary(bin string, args []string) error {
	if runtime.GOOS == "windows" {
		cmd := exec.Command(bin, args...)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	}
	return syscall.Exec(bin, append([]string{bin}, args...), os.Environ())
}
