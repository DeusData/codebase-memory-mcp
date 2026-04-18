package orgdiscovery

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestScanOrg_BasicDiscovery(t *testing.T) {
	// Mock GitHub API
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/orgs/TestOrg/repos" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		// Check auth header
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing or wrong auth header")
		}

		repos := []ghRepo{
			{Name: "payments-api", CloneURL: "https://github.com/TestOrg/payments-api.git", Language: "TypeScript", Topics: []string{"team-payments", "nestjs"}, DefaultBranch: "main"},
			{Name: "dashboard-ui", CloneURL: "https://github.com/TestOrg/dashboard-ui.git", Language: "Vue", Topics: []string{"team-frontend", "vue"}, DefaultBranch: "main"},
			{Name: "old-service", CloneURL: "https://github.com/TestOrg/old-service.git", Language: "JavaScript", Archived: true},
			{Name: "fork-repo", CloneURL: "https://github.com/TestOrg/fork-repo.git", Language: "Go", Fork: true},
			{Name: "infra-terraform", CloneURL: "https://github.com/TestOrg/infra-terraform.git", Language: "HCL", Topics: []string{"team-platform", "infrastructure"}, DefaultBranch: "main"},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(repos)
	}))
	defer server.Close()

	scanner := NewScanner("TestOrg", "test-token")
	scanner.SetAPIBaseURL(server.URL)

	repos, err := scanner.ScanOrg(context.Background())
	if err != nil {
		t.Fatalf("ScanOrg: %v", err)
	}

	// Should skip archived and forked repos
	if len(repos) != 3 {
		t.Fatalf("repos count: got %d, want 3", len(repos))
	}

	// Check payments-api
	if repos[0].Name != "payments-api" {
		t.Errorf("repos[0].Name: got %q, want %q", repos[0].Name, "payments-api")
	}
	if repos[0].Team != "payments" {
		t.Errorf("repos[0].Team: got %q, want %q", repos[0].Team, "payments")
	}
	if repos[0].Type != "backend" {
		t.Errorf("repos[0].Type: got %q, want %q", repos[0].Type, "backend")
	}

	// Check dashboard-ui (Vue = frontend)
	if repos[1].Type != "frontend" {
		t.Errorf("repos[1].Type: got %q, want %q", repos[1].Type, "frontend")
	}
	if repos[1].Team != "frontend" {
		t.Errorf("repos[1].Team: got %q, want %q", repos[1].Team, "frontend")
	}

	// Check infra-terraform
	if repos[2].Type != "infra" {
		t.Errorf("repos[2].Type: got %q, want %q", repos[2].Type, "infra")
	}
}

func TestScanOrg_Pagination(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		page := r.URL.Query().Get("page")

		var repos []ghRepo
		if page == "" || page == "1" {
			// Return full page (100 items) to trigger pagination
			repos = make([]ghRepo, 100)
			for i := range repos {
				repos[i] = ghRepo{
					Name:     fmt.Sprintf("repo-%03d", i),
					CloneURL: fmt.Sprintf("https://github.com/TestOrg/repo-%03d.git", i),
					Language: "TypeScript",
				}
			}
		} else {
			// Page 2: partial page (stops pagination)
			repos = []ghRepo{
				{Name: "repo-100", CloneURL: "https://github.com/TestOrg/repo-100.git", Language: "Go"},
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(repos)
	}))
	defer server.Close()

	scanner := NewScanner("TestOrg", "test-token")
	scanner.SetAPIBaseURL(server.URL)

	repos, err := scanner.ScanOrg(context.Background())
	if err != nil {
		t.Fatalf("ScanOrg: %v", err)
	}

	if len(repos) != 101 {
		t.Errorf("repos count: got %d, want 101", len(repos))
	}
	if callCount != 2 {
		t.Errorf("API calls: got %d, want 2", callCount)
	}
}

func TestScanOrg_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		w.Write([]byte(`{"message":"Bad credentials"}`))
	}))
	defer server.Close()

	scanner := NewScanner("TestOrg", "bad-token")
	scanner.SetAPIBaseURL(server.URL)

	_, err := scanner.ScanOrg(context.Background())
	if err == nil {
		t.Fatal("expected error for 403 response")
	}
}

func TestScanUpdatedSince_ReturnsOnlyRecent(t *testing.T) {
	now := time.Now()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("sort") != "pushed" {
			t.Error("expected sort=pushed")
		}
		if r.URL.Query().Get("direction") != "desc" {
			t.Error("expected direction=desc")
		}

		repos := []ghRepo{
			{Name: "just-pushed", CloneURL: "https://github.com/T/just-pushed.git", Language: "TypeScript", PushedAt: now.Add(-1 * time.Hour).Format(time.RFC3339)},
			{Name: "pushed-today", CloneURL: "https://github.com/T/pushed-today.git", Language: "Go", PushedAt: now.Add(-5 * time.Hour).Format(time.RFC3339)},
			{Name: "old-repo", CloneURL: "https://github.com/T/old-repo.git", Language: "Python", PushedAt: now.Add(-48 * time.Hour).Format(time.RFC3339)},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(repos)
	}))
	defer server.Close()

	scanner := NewScanner("T", "tok")
	scanner.SetAPIBaseURL(server.URL)

	since := now.Add(-24 * time.Hour)
	repos, err := scanner.ScanUpdatedSince(context.Background(), since)
	if err != nil {
		t.Fatalf("ScanUpdatedSince: %v", err)
	}
	if len(repos) != 2 {
		t.Fatalf("repos: got %d, want 2", len(repos))
	}
	if repos[0].Name != "just-pushed" {
		t.Errorf("repos[0]: got %q, want %q", repos[0].Name, "just-pushed")
	}
	if repos[1].Name != "pushed-today" {
		t.Errorf("repos[1]: got %q, want %q", repos[1].Name, "pushed-today")
	}
}

func TestScanUpdatedSince_StopsEarly(t *testing.T) {
	now := time.Now()
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		// First page: 100 repos, last one is old — should not fetch page 2
		repos := make([]ghRepo, 100)
		for i := range repos {
			pushedAt := now.Add(-1 * time.Hour) // recent
			if i == 99 {
				pushedAt = now.Add(-48 * time.Hour) // old — triggers early stop
			}
			repos[i] = ghRepo{
				Name:     fmt.Sprintf("repo-%03d", i),
				CloneURL: fmt.Sprintf("https://github.com/T/repo-%03d.git", i),
				Language: "Go",
				PushedAt: pushedAt.Format(time.RFC3339),
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(repos)
	}))
	defer server.Close()

	scanner := NewScanner("T", "tok")
	scanner.SetAPIBaseURL(server.URL)

	since := now.Add(-24 * time.Hour)
	repos, err := scanner.ScanUpdatedSince(context.Background(), since)
	if err != nil {
		t.Fatalf("ScanUpdatedSince: %v", err)
	}
	if callCount != 1 {
		t.Errorf("API calls: got %d, want 1 (should stop early)", callCount)
	}
	if len(repos) != 99 {
		t.Errorf("repos: got %d, want 99", len(repos))
	}
}

func TestScanUpdatedSince_EmptyWhenNoChanges(t *testing.T) {
	now := time.Now()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		repos := []ghRepo{
			{Name: "stale-1", CloneURL: "https://github.com/T/stale-1.git", Language: "Go", PushedAt: now.Add(-72 * time.Hour).Format(time.RFC3339)},
			{Name: "stale-2", CloneURL: "https://github.com/T/stale-2.git", Language: "Go", PushedAt: now.Add(-96 * time.Hour).Format(time.RFC3339)},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(repos)
	}))
	defer server.Close()

	scanner := NewScanner("T", "tok")
	scanner.SetAPIBaseURL(server.URL)

	since := now.Add(-24 * time.Hour)
	repos, err := scanner.ScanUpdatedSince(context.Background(), since)
	if err != nil {
		t.Fatalf("ScanUpdatedSince: %v", err)
	}
	if len(repos) != 0 {
		t.Errorf("repos: got %d, want 0", len(repos))
	}
}

func TestScanUpdatedSince_SkipsArchivedAndForks(t *testing.T) {
	now := time.Now()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		repos := []ghRepo{
			{Name: "active-repo", CloneURL: "https://github.com/T/active-repo.git", Language: "Go", PushedAt: now.Add(-1 * time.Hour).Format(time.RFC3339)},
			{Name: "archived-repo", CloneURL: "https://github.com/T/archived-repo.git", Language: "Go", PushedAt: now.Add(-1 * time.Hour).Format(time.RFC3339), Archived: true},
			{Name: "forked-repo", CloneURL: "https://github.com/T/forked-repo.git", Language: "Go", PushedAt: now.Add(-1 * time.Hour).Format(time.RFC3339), Fork: true},
			{Name: "another-active", CloneURL: "https://github.com/T/another-active.git", Language: "TypeScript", PushedAt: now.Add(-2 * time.Hour).Format(time.RFC3339)},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(repos)
	}))
	defer server.Close()

	scanner := NewScanner("T", "tok")
	scanner.SetAPIBaseURL(server.URL)

	since := now.Add(-24 * time.Hour)
	repos, err := scanner.ScanUpdatedSince(context.Background(), since)
	if err != nil {
		t.Fatalf("ScanUpdatedSince: %v", err)
	}
	if len(repos) != 2 {
		t.Fatalf("repos: got %d, want 2 (archived and forked should be skipped)", len(repos))
	}
	if repos[0].Name != "active-repo" {
		t.Errorf("repos[0]: got %q, want %q", repos[0].Name, "active-repo")
	}
	if repos[1].Name != "another-active" {
		t.Errorf("repos[1]: got %q, want %q", repos[1].Name, "another-active")
	}
}

func TestInferTeamFromTopics(t *testing.T) {
	tests := []struct {
		topics []string
		want   string
	}{
		{[]string{"team-payments", "nestjs"}, "payments"},
		{[]string{"nestjs", "microservice"}, ""},
		{[]string{"team-platform"}, "platform"},
		{nil, ""},
	}
	for _, tt := range tests {
		got := inferTeamFromTopics(tt.topics)
		if got != tt.want {
			t.Errorf("inferTeamFromTopics(%v): got %q, want %q", tt.topics, got, tt.want)
		}
	}
}

func TestInferTypeFromLanguage(t *testing.T) {
	tests := []struct {
		lang   string
		topics []string
		want   string
	}{
		{"TypeScript", nil, "backend"},
		{"Vue", nil, "frontend"},
		{"HCL", nil, "infra"},
		{"TypeScript", []string{"frontend"}, "frontend"},
		{"TypeScript", []string{"library"}, "library"},
		{"", nil, "other"},
	}
	for _, tt := range tests {
		got := inferTypeFromLanguage(tt.lang, tt.topics)
		if got != tt.want {
			t.Errorf("inferType(%q, %v): got %q, want %q", tt.lang, tt.topics, got, tt.want)
		}
	}
}
