package orgdiscovery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
)

// newTestScanner creates a Scanner pointing at the given httptest server.
func newTestScanner(serverURL string) *Scanner {
	s := NewScanner("TestOrg", "test-token")
	s.SetAPIBaseURL(serverURL)
	return s
}

func TestEnrichOwnership_CodeownersFirst(t *testing.T) {
	codeownersContent := "* @TestOrg/platform-team\n/src/ @TestOrg/frontend-team\n"
	encoded := base64.StdEncoding.EncodeToString([]byte(codeownersContent))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/repos/TestOrg/my-service/contents/.github/CODEOWNERS":
			json.NewEncoder(w).Encode(ghContentsResponse{Content: encoded, Encoding: "base64"})
		case r.URL.Path == "/orgs/TestOrg/teams":
			// Return a team that also claims this repo
			json.NewEncoder(w).Encode([]ghTeam{{Slug: "other-team"}})
		case r.URL.Path == "/orgs/TestOrg/teams/other-team/repos":
			json.NewEncoder(w).Encode([]ghTeamRepo{
				{Name: "my-service", Permissions: map[string]bool{"admin": true}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	scanner := newTestScanner(server.URL)
	repos := []manifest.Repo{
		{Name: "my-service", GitHubURL: "https://github.com/TestOrg/my-service.git"},
	}

	err := scanner.EnrichOwnership(context.Background(), repos)
	if err != nil {
		t.Fatalf("EnrichOwnership: %v", err)
	}

	// CODEOWNERS should win over Teams API
	if repos[0].Team != "platform-team" {
		t.Errorf("Team: got %q, want %q", repos[0].Team, "platform-team")
	}
}

func TestEnrichOwnership_TeamsAPIFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/repos/TestOrg/backend-svc/contents/.github/CODEOWNERS":
			http.NotFound(w, r) // No CODEOWNERS
		case r.URL.Path == "/orgs/TestOrg/teams":
			json.NewEncoder(w).Encode([]ghTeam{{Slug: "team-payments-devs"}})
		case r.URL.Path == "/orgs/TestOrg/teams/team-payments-devs/repos":
			json.NewEncoder(w).Encode([]ghTeamRepo{
				{Name: "backend-svc", Permissions: map[string]bool{"admin": true, "push": true}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	scanner := newTestScanner(server.URL)
	repos := []manifest.Repo{
		{Name: "backend-svc", GitHubURL: "https://github.com/TestOrg/backend-svc.git"},
	}

	err := scanner.EnrichOwnership(context.Background(), repos)
	if err != nil {
		t.Fatalf("EnrichOwnership: %v", err)
	}

	if repos[0].Team != "payments" {
		t.Errorf("Team: got %q, want %q", repos[0].Team, "payments")
	}
}

func TestEnrichOwnership_TopicFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/repos/TestOrg/topic-repo/contents/.github/CODEOWNERS":
			http.NotFound(w, r)
		case r.URL.Path == "/orgs/TestOrg/teams":
			json.NewEncoder(w).Encode([]ghTeam{}) // No teams
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	scanner := newTestScanner(server.URL)
	repos := []manifest.Repo{
		{Name: "topic-repo", GitHubURL: "https://github.com/TestOrg/topic-repo.git", Team: "crm"},
	}

	err := scanner.EnrichOwnership(context.Background(), repos)
	if err != nil {
		t.Fatalf("EnrichOwnership: %v", err)
	}

	// Should keep existing topic-based team
	if repos[0].Team != "crm" {
		t.Errorf("Team: got %q, want %q", repos[0].Team, "crm")
	}
}

func TestEnrichOwnership_NameFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/repos/TestOrg/automation-workflows/contents/.github/CODEOWNERS":
			http.NotFound(w, r)
		case r.URL.Path == "/orgs/TestOrg/teams":
			json.NewEncoder(w).Encode([]ghTeam{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	scanner := newTestScanner(server.URL)
	repos := []manifest.Repo{
		{Name: "automation-workflows", GitHubURL: "https://github.com/TestOrg/automation-workflows.git"},
	}

	err := scanner.EnrichOwnership(context.Background(), repos)
	if err != nil {
		t.Fatalf("EnrichOwnership: %v", err)
	}

	if repos[0].Team != "automation" {
		t.Errorf("Team: got %q, want %q", repos[0].Team, "automation")
	}
}

func TestFetchCodeowners_ParsesCatchAll(t *testing.T) {
	content := "# Top-level ownership\n* @TestOrg/platform-core\n/frontend/ @TestOrg/ui-team\n*.vue @TestOrg/ui-team\n"
	encoded := base64.StdEncoding.EncodeToString([]byte(content))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(ghContentsResponse{Content: encoded, Encoding: "base64"})
	}))
	defer server.Close()

	scanner := newTestScanner(server.URL)
	owner := scanner.fetchCodeowners(context.Background(), "some-repo")

	if owner != "platform-core" {
		t.Errorf("fetchCodeowners: got %q, want %q", owner, "platform-core")
	}
}

func TestFetchCodeowners_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer server.Close()

	scanner := newTestScanner(server.URL)
	owner := scanner.fetchCodeowners(context.Background(), "no-codeowners-repo")

	if owner != "" {
		t.Errorf("fetchCodeowners: got %q, want empty", owner)
	}
}

func TestFetchTeamRepos_MostSpecificTeamPreferred(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/orgs/TestOrg/teams":
			json.NewEncoder(w).Encode([]ghTeam{
				{Slug: "team-revex-memberships-devs"},  // specific team (1 repo)
				{Slug: "team-revex-saas-devs"},          // broad team (3 repos)
			})
		case "/orgs/TestOrg/teams/team-revex-memberships-devs/repos":
			json.NewEncoder(w).Encode([]ghTeamRepo{
				{Name: "membership-backend", Permissions: map[string]bool{"push": true}},
			})
		case "/orgs/TestOrg/teams/team-revex-saas-devs/repos":
			json.NewEncoder(w).Encode([]ghTeamRepo{
				{Name: "membership-backend", Permissions: map[string]bool{"push": true}},
				{Name: "other-service", Permissions: map[string]bool{"push": true}},
				{Name: "yet-another", Permissions: map[string]bool{"push": true}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	scanner := newTestScanner(server.URL)
	teamsMap, err := scanner.fetchTeamRepos(context.Background())
	if err != nil {
		t.Fatalf("fetchTeamRepos: %v", err)
	}

	// Most specific team (fewer repos) should win
	if teamsMap["membership-backend"] != "revex" {
		t.Errorf("membership-backend team: got %q, want %q", teamsMap["membership-backend"], "revex")
	}
}

func TestInferTeamFromName(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"automation-engine", "automation"},
		{"leadgen-forms", "leadgen"},
		{"revex-billing", "revex"},
		{"dev-checkout", "commerce"},
		{"ai-assistant", "ai"},
		{"mobile-app", "mobile"},
		{"marketplace-api", "marketplace"},
		{"sdet-framework", "sdet"},
		{"i18n-translations", "i18n"},
		{"ghl-revex-payments", "revex"},
		{"ghl-crm-contacts", "crm"},
		{"platform-core", "platform"},
		{"unknown-service", ""}, // unknown = empty
	}
	for _, tt := range tests {
		got := inferTeamFromName(tt.name)
		if got != tt.want {
			t.Errorf("inferTeamFromName(%q): got %q, want %q", tt.name, got, tt.want)
		}
	}
}
