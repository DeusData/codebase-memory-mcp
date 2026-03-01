package selfupdate

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
)

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		a, b string
		want int // >0, <0, or 0
	}{
		{"0.2.1", "0.2.0", 1},
		{"0.2.0", "0.2.0", 0},
		{"0.1.9", "0.2.0", -1},
		{"0.10.0", "0.2.0", 1},
		{"1.0.0", "0.99.99", 1},
		{"0.0.1", "0.0.2", -1},
		{"v0.2.1", "0.2.1", 0},
		{"0.2.1", "v0.2.1", 0},
		{"0.2.1-dev", "0.2.1", -1},
		{"0.2.1", "0.2.1-dev", 1},
		{"0.2.1-dev", "0.2.1-dev", 0},
		{"0.3.0", "0.2.1-dev", 1},
		{"0.2.0", "0.2.1-dev", -1},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_vs_%s", tt.a, tt.b), func(t *testing.T) {
			got := CompareVersions(tt.a, tt.b)
			switch {
			case tt.want > 0 && got <= 0:
				t.Fatalf("CompareVersions(%q, %q) = %d, want > 0", tt.a, tt.b, got)
			case tt.want < 0 && got >= 0:
				t.Fatalf("CompareVersions(%q, %q) = %d, want < 0", tt.a, tt.b, got)
			case tt.want == 0 && got != 0:
				t.Fatalf("CompareVersions(%q, %q) = %d, want 0", tt.a, tt.b, got)
			}
		})
	}
}

func TestAssetName(t *testing.T) {
	name := AssetName()
	expected := fmt.Sprintf("codebase-memory-mcp-%s-%s", runtime.GOOS, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		expected += ".zip"
	} else {
		expected += ".tar.gz"
	}
	if name != expected {
		t.Fatalf("AssetName() = %q, want %q", name, expected)
	}
}

func TestFetchLatestRelease(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{
			"tag_name": "v1.0.0",
			"assets": [
				{"name": "codebase-memory-mcp-linux-amd64.tar.gz", "browser_download_url": "https://example.com/linux.tar.gz", "size": 1024},
				{"name": "checksums.txt", "browser_download_url": "https://example.com/checksums.txt", "size": 256}
			]
		}`)
	}))
	defer ts.Close()

	old := ReleaseURL
	ReleaseURL = ts.URL
	defer func() { ReleaseURL = old }()

	release, err := FetchLatestRelease(context.Background())
	if err != nil {
		t.Fatalf("FetchLatestRelease() error: %v", err)
	}

	if release.LatestVersion() != "1.0.0" {
		t.Fatalf("LatestVersion() = %q, want %q", release.LatestVersion(), "1.0.0")
	}

	if len(release.Assets) != 2 {
		t.Fatalf("expected 2 assets, got %d", len(release.Assets))
	}
}

func TestFetchLatestRelease_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer ts.Close()

	old := ReleaseURL
	ReleaseURL = ts.URL
	defer func() { ReleaseURL = old }()

	_, err := FetchLatestRelease(context.Background())
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestRelease_FindAsset(t *testing.T) {
	release := &Release{
		Assets: []Asset{
			{Name: "file-a.tar.gz"},
			{Name: "file-b.tar.gz"},
			{Name: "checksums.txt"},
		},
	}

	if a := release.FindAsset("file-b.tar.gz"); a == nil {
		t.Fatal("expected to find file-b.tar.gz")
	}
	if a := release.FindAsset("nonexistent"); a != nil {
		t.Fatal("expected nil for nonexistent asset")
	}
}

func TestDownloadChecksums(t *testing.T) {
	checksumServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "abc123  file-a.tar.gz\ndef456  file-b.tar.gz\n")
	}))
	defer checksumServer.Close()

	// Allow local test server URLs
	orig := AllowedDownloadPrefixes
	AllowedDownloadPrefixes = append(AllowedDownloadPrefixes, checksumServer.URL)
	t.Cleanup(func() { AllowedDownloadPrefixes = orig })

	release := &Release{
		Assets: []Asset{
			{Name: "checksums.txt", BrowserDownloadURL: checksumServer.URL},
		},
	}

	checksums, err := DownloadChecksums(context.Background(), release)
	if err != nil {
		t.Fatalf("DownloadChecksums() error: %v", err)
	}

	if checksums["file-a.tar.gz"] != "abc123" {
		t.Fatalf("expected abc123 for file-a.tar.gz, got %q", checksums["file-a.tar.gz"])
	}
	if checksums["file-b.tar.gz"] != "def456" {
		t.Fatalf("expected def456 for file-b.tar.gz, got %q", checksums["file-b.tar.gz"])
	}
}

func TestDownloadChecksums_NoChecksumsFile(t *testing.T) {
	release := &Release{
		Assets: []Asset{
			{Name: "file-a.tar.gz"},
		},
	}

	_, err := DownloadChecksums(context.Background(), release)
	if err == nil {
		t.Fatal("expected error when checksums.txt is missing")
	}
}
