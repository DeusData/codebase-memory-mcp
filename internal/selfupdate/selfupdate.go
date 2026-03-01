package selfupdate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// ReleaseURL is the GitHub API endpoint for latest release. Exported for test injection.
var ReleaseURL = "https://api.github.com/repos/DeusData/codebase-memory-mcp/releases/latest"

// Release holds parsed GitHub release metadata.
type Release struct {
	TagName string  `json:"tag_name"`
	Assets  []Asset `json:"assets"`
}

// Asset holds a single release artifact.
type Asset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Size               int64  `json:"size"`
}

// FetchLatestRelease fetches release metadata from GitHub.
func FetchLatestRelease(ctx context.Context) (*Release, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", ReleaseURL, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("github api status=%d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var release Release
	if err := json.Unmarshal(body, &release); err != nil {
		return nil, fmt.Errorf("parse json: %w", err)
	}
	return &release, nil
}

// LatestVersion returns the version string from the latest release (without "v" prefix).
func (r *Release) LatestVersion() string {
	return strings.TrimPrefix(r.TagName, "v")
}

// FindAsset finds a release asset by name.
func (r *Release) FindAsset(name string) *Asset {
	for i := range r.Assets {
		if r.Assets[i].Name == name {
			return &r.Assets[i]
		}
	}
	return nil
}

// AssetName returns the expected release asset name for the current platform.
func AssetName() string {
	if runtime.GOOS == "windows" {
		return fmt.Sprintf("codebase-memory-mcp-%s-%s.zip", runtime.GOOS, runtime.GOARCH)
	}
	return fmt.Sprintf("codebase-memory-mcp-%s-%s.tar.gz", runtime.GOOS, runtime.GOARCH)
}

// CompareVersions compares two semver strings (e.g. "0.2.1" vs "0.2.0").
// Returns >0 if a > b, <0 if a < b, 0 if equal.
func CompareVersions(a, b string) int {
	a = strings.TrimPrefix(a, "v")
	b = strings.TrimPrefix(b, "v")

	// Strip pre-release suffixes for comparison (e.g. "0.2.1-dev" → "0.2.1")
	aBase := strings.SplitN(a, "-", 2)[0]
	bBase := strings.SplitN(b, "-", 2)[0]

	aParts := strings.Split(aBase, ".")
	bParts := strings.Split(bBase, ".")
	for i := 0; i < len(aParts) && i < len(bParts); i++ {
		ai, _ := strconv.Atoi(aParts[i])
		bi, _ := strconv.Atoi(bParts[i])
		if ai != bi {
			return ai - bi
		}
	}
	if len(aParts) != len(bParts) {
		return len(aParts) - len(bParts)
	}

	// Same base version — non-dev beats dev (e.g. "0.2.1" > "0.2.1-dev")
	aHasPre := strings.Contains(a, "-")
	bHasPre := strings.Contains(b, "-")
	if aHasPre && !bHasPre {
		return -1
	}
	if !aHasPre && bHasPre {
		return 1
	}
	return 0
}

// AllowedDownloadPrefixes controls which URL prefixes are accepted by DownloadAsset.
// Exported for test injection only.
var AllowedDownloadPrefixes = []string{
	"https://github.com/",
	"https://api.github.com/",
}

// DownloadAsset downloads a release asset and returns the body reader and content length.
// Caller must close the returned ReadCloser.
func DownloadAsset(ctx context.Context, rawURL string) (io.ReadCloser, int64, error) {
	allowed := false
	for _, prefix := range AllowedDownloadPrefixes {
		if strings.HasPrefix(rawURL, prefix) {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, 0, fmt.Errorf("refusing to download from non-GitHub URL: %s", rawURL)
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, http.NoBody)
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("download: %w", err)
	}

	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("download status=%d", resp.StatusCode)
	}

	return resp.Body, resp.ContentLength, nil
}

// DownloadChecksums downloads and parses the checksums.txt file from a release.
// Returns a map of filename → hex-encoded SHA-256 hash.
func DownloadChecksums(ctx context.Context, release *Release) (map[string]string, error) {
	asset := release.FindAsset("checksums.txt")
	if asset == nil {
		return nil, fmt.Errorf("checksums.txt not found in release")
	}

	body, _, err := DownloadAsset(ctx, asset.BrowserDownloadURL)
	if err != nil {
		return nil, fmt.Errorf("download checksums: %w", err)
	}
	defer body.Close()

	data, err := io.ReadAll(io.LimitReader(body, 1<<16))
	if err != nil {
		return nil, fmt.Errorf("read checksums: %w", err)
	}

	checksums := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			checksums[parts[1]] = parts[0]
		}
	}
	return checksums, nil
}
