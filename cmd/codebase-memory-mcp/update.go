package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/selfupdate"
)

// newCommand wraps exec.Command for testability.
var newCommand = exec.Command

func runUpdate(args []string) int {
	dryRun := false
	for _, a := range args {
		if a == "--dry-run" {
			dryRun = true
		}
	}

	currentVersion := strings.TrimSuffix(version, "-dev")
	fmt.Printf("\ncodebase-memory-mcp %s — checking for updates...\n", version)

	if runtime.GOOS == "windows" {
		fmt.Println("Self-update is not supported on Windows.")
		fmt.Println("Download the latest release manually from:")
		fmt.Println("  https://github.com/DeusData/codebase-memory-mcp/releases/latest")
		return 1
	}

	ctx := context.Background()

	release, err := selfupdate.FetchLatestRelease(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: fetch release: %v\n", err)
		return 1
	}

	latest := release.LatestVersion()
	if latest == "" {
		fmt.Println("Could not determine latest version.")
		return 1
	}

	if selfupdate.CompareVersions(latest, currentVersion) <= 0 {
		fmt.Printf("Already up to date (v%s).\n", currentVersion)
		return 0
	}

	fmt.Printf("Update available: v%s → v%s\n", currentVersion, latest)

	assetName := selfupdate.AssetName()
	asset := release.FindAsset(assetName)
	if asset == nil {
		fmt.Fprintf(os.Stderr, "error: no release asset for %s/%s (%s)\n", runtime.GOOS, runtime.GOARCH, assetName)
		return 1
	}

	if dryRun {
		fmt.Printf("[dry-run] Would download: %s (%d bytes)\n", assetName, asset.Size)
		fmt.Println("[dry-run] Would replace binary and re-run install.")
		return 0
	}

	binaryData, err := downloadAndVerify(ctx, release, assetName, asset)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	if err := replaceBinary(binaryData); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	fmt.Println("Re-applying installation...")
	runInstall([]string{})

	fmt.Printf("\nUpdated to v%s. Restart Claude Code / Codex to activate.\n", latest)
	return 0
}

// downloadAndVerify downloads the release asset and verifies its checksum.
func downloadAndVerify(ctx context.Context, release *selfupdate.Release, assetName string, asset *selfupdate.Asset) ([]byte, error) {
	fmt.Println("Downloading checksums...")
	checksums, err := selfupdate.DownloadChecksums(ctx, release)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: %v (skipping checksum verification)\n", err)
		checksums = nil
	}

	fmt.Printf("Downloading %s...\n", assetName)
	body, _, err := selfupdate.DownloadAsset(ctx, asset.BrowserDownloadURL)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}

	tarballData, err := io.ReadAll(body)
	body.Close()
	if err != nil {
		return nil, fmt.Errorf("read download: %w", err)
	}

	if checksums != nil {
		if expected, ok := checksums[assetName]; ok {
			hash := sha256.Sum256(tarballData)
			actual := hex.EncodeToString(hash[:])
			if actual != expected {
				return nil, fmt.Errorf("checksum mismatch\n  expected: %s\n  actual:   %s", expected, actual)
			}
			fmt.Println("Checksum verified.")
		}
	}

	binaryData, err := extractBinaryFromTarGz(tarballData)
	if err != nil {
		return nil, fmt.Errorf("extract: %w", err)
	}

	return binaryData, nil
}

// replaceBinary atomically swaps the current binary with the new one.
func replaceBinary(binaryData []byte) error {
	binaryPath, err := detectBinaryPath()
	if err != nil {
		return err
	}

	fmt.Printf("Replacing binary at %s...\n", binaryPath)

	tmpPath := binaryPath + ".tmp"
	if err := os.WriteFile(tmpPath, binaryData, 0o600); err != nil {
		return fmt.Errorf("write temp: %w", err)
	}
	if err := os.Chmod(tmpPath, 0o500); err != nil {
		return fmt.Errorf("chmod temp: %w", err)
	}

	bakPath := binaryPath + ".bak"
	if cpErr := copyFile(binaryPath, bakPath); cpErr != nil {
		fmt.Fprintf(os.Stderr, "warning: backup failed: %v\n", cpErr)
	}

	if err := os.Rename(tmpPath, binaryPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename: %w", err)
	}

	if err := verifyBinary(binaryPath); err != nil {
		fmt.Println("Restoring previous version...")
		if restoreErr := os.Rename(bakPath, binaryPath); restoreErr != nil {
			return fmt.Errorf("restore failed (%v), backup at: %s", restoreErr, bakPath)
		}
		fmt.Println("Previous version restored.")
		return fmt.Errorf("new binary verification failed: %w", err)
	}

	os.Remove(bakPath)
	return nil
}

// extractBinaryFromTarGz extracts the first regular file from a .tar.gz archive.
func extractBinaryFromTarGz(data []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar: %w", err)
		}
		if hdr.Typeflag == tar.TypeReg && strings.HasPrefix(filepath.Base(hdr.Name), "codebase-memory-mcp") {
			content, err := io.ReadAll(tr)
			if err != nil {
				return nil, fmt.Errorf("read entry: %w", err)
			}
			return content, nil
		}
	}
	return nil, fmt.Errorf("binary not found in archive")
}

// verifyBinary runs --version on the new binary to ensure it works.
func verifyBinary(path string) error {
	cmd := newCommand(path, "--version")
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("--version failed: %w", err)
	}
	output := strings.TrimSpace(string(out))
	if !strings.Contains(output, "codebase-memory-mcp") {
		return fmt.Errorf("unexpected output: %s", output)
	}
	return nil
}

// copyFile copies a file from src to dst.
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

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}
