package tools

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		a, b string
		want int // >0, 0, or <0
	}{
		{"0.2.1", "0.2.0", 1},
		{"0.2.0", "0.2.0", 0},
		{"0.1.9", "0.2.0", -1},
		{"0.10.0", "0.2.0", 1}, // numeric, not string comparison
		{"1.0.0", "0.99.99", 1},
		{"0.0.1", "0.0.2", -1},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_vs_%s", tt.a, tt.b), func(t *testing.T) {
			got := compareVersions(tt.a, tt.b)
			switch {
			case tt.want > 0 && got <= 0:
				t.Fatalf("compareVersions(%q, %q) = %d, want > 0", tt.a, tt.b, got)
			case tt.want < 0 && got >= 0:
				t.Fatalf("compareVersions(%q, %q) = %d, want < 0", tt.a, tt.b, got)
			case tt.want == 0 && got != 0:
				t.Fatalf("compareVersions(%q, %q) = %d, want 0", tt.a, tt.b, got)
			}
		})
	}
}

func TestCheckForUpdate_NewerAvailable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"tag_name": "v99.0.0"}`)
	}))
	defer ts.Close()

	old := releaseURL
	releaseURL = ts.URL
	defer func() { releaseURL = old }()

	srv := &Server{}
	srv.checkForUpdate()

	notice, _ := srv.updateNotice.Load().(string)
	if notice == "" {
		t.Fatal("expected update notice to be set")
	}
	if !strings.Contains(notice, "v99.0.0") {
		t.Fatalf("notice should mention v99.0.0, got: %s", notice)
	}
	if !strings.Contains(notice, "codebase-memory-mcp update") {
		t.Fatalf("notice should contain update command, got: %s", notice)
	}
}

func TestCheckForUpdate_AlreadyCurrent(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"tag_name": "v%s"}`, Version)
	}))
	defer ts.Close()

	old := releaseURL
	releaseURL = ts.URL
	defer func() { releaseURL = old }()

	srv := &Server{}
	srv.checkForUpdate()

	notice, _ := srv.updateNotice.Load().(string)
	if notice != "" {
		t.Fatalf("expected empty notice for current version, got: %s", notice)
	}
}

func TestCheckForUpdate_OlderAvailable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"tag_name": "v0.0.1"}`)
	}))
	defer ts.Close()

	old := releaseURL
	releaseURL = ts.URL
	defer func() { releaseURL = old }()

	srv := &Server{}
	srv.checkForUpdate()

	notice, _ := srv.updateNotice.Load().(string)
	if notice != "" {
		t.Fatalf("expected empty notice for older version, got: %s", notice)
	}
}

func TestCheckForUpdate_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer ts.Close()

	old := releaseURL
	releaseURL = ts.URL
	defer func() { releaseURL = old }()

	srv := &Server{}
	srv.checkForUpdate() // should not panic

	notice, _ := srv.updateNotice.Load().(string)
	if notice != "" {
		t.Fatalf("expected empty notice on server error, got: %s", notice)
	}
}

func TestCheckForUpdate_MalformedJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `not json at all!!!`)
	}))
	defer ts.Close()

	old := releaseURL
	releaseURL = ts.URL
	defer func() { releaseURL = old }()

	srv := &Server{}
	srv.checkForUpdate() // should not panic

	notice, _ := srv.updateNotice.Load().(string)
	if notice != "" {
		t.Fatalf("expected empty notice on malformed JSON, got: %s", notice)
	}
}

func TestAddUpdateNotice_ShowsOnce(t *testing.T) {
	srv := &Server{}
	srv.updateNotice.Store("v0.2.0 → v0.3.0 available")

	first := &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "{}"}}}
	srv.addUpdateNotice(first)
	if len(first.Content) != 2 {
		t.Fatalf("expected 2 content blocks, got %d", len(first.Content))
	}
	tc, ok := first.Content[0].(*mcp.TextContent)
	if !ok || tc.Text != "v0.2.0 → v0.3.0 available" {
		t.Fatalf("expected notice as first content block, got %v", first.Content[0])
	}

	second := &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "{}"}}}
	srv.addUpdateNotice(second)
	if len(second.Content) != 1 {
		t.Fatal("expected update notice to be absent in second call (show-once)")
	}
}
