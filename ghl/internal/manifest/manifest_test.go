package manifest_test

import (
	"strings"
	"testing"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
)

const sampleYAML = `
repos:
  - name: membership-backend
    github_url: https://github.com/GoHighLevel/membership-backend
    team: revex
    type: backend
    tags: [membership, billing, subscription]

  - name: ghl-revex-frontend
    github_url: https://github.com/GoHighLevel/ghl-revex-frontend
    team: revex
    type: frontend
    tags: [crm, contacts, pipeline]

  - name: platform-backend
    github_url: https://github.com/GoHighLevel/platform-backend
    team: platform
    type: backend
    tags: [infrastructure, routing]
`

func TestLoad_ParsesAllRepos(t *testing.T) {
	m, err := manifest.LoadReader(strings.NewReader(sampleYAML))
	if err != nil {
		t.Fatalf("LoadReader failed: %v", err)
	}
	if len(m.Repos) != 3 {
		t.Fatalf("want 3 repos, got %d", len(m.Repos))
	}
}

func TestLoad_RepoFields(t *testing.T) {
	m, err := manifest.LoadReader(strings.NewReader(sampleYAML))
	if err != nil {
		t.Fatalf("LoadReader failed: %v", err)
	}
	r := m.Repos[0]
	if r.Name != "membership-backend" {
		t.Errorf("Name: want membership-backend, got %q", r.Name)
	}
	if r.GitHubURL != "https://github.com/GoHighLevel/membership-backend" {
		t.Errorf("GitHubURL: want ..., got %q", r.GitHubURL)
	}
	if r.Team != "revex" {
		t.Errorf("Team: want revex, got %q", r.Team)
	}
	if r.Type != "backend" {
		t.Errorf("Type: want backend, got %q", r.Type)
	}
	if len(r.Tags) != 3 {
		t.Errorf("Tags: want 3, got %d", len(r.Tags))
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	_, err := manifest.LoadReader(strings.NewReader("not: valid: yaml: :::"))
	if err == nil {
		t.Error("want error for invalid YAML, got nil")
	}
}

func TestLoad_EmptyRepos(t *testing.T) {
	m, err := manifest.LoadReader(strings.NewReader("repos: []"))
	if err != nil {
		t.Fatalf("LoadReader failed: %v", err)
	}
	if len(m.Repos) != 0 {
		t.Errorf("want 0 repos, got %d", len(m.Repos))
	}
}

func TestManifest_FindByName(t *testing.T) {
	m, _ := manifest.LoadReader(strings.NewReader(sampleYAML))

	r, ok := m.FindByName("ghl-revex-frontend")
	if !ok {
		t.Fatal("FindByName: want found, got not found")
	}
	if r.Type != "frontend" {
		t.Errorf("Type: want frontend, got %q", r.Type)
	}

	_, ok = m.FindByName("nonexistent-repo")
	if ok {
		t.Error("FindByName: want not found for unknown name")
	}
}

func TestManifest_FilterByTeam(t *testing.T) {
	m, _ := manifest.LoadReader(strings.NewReader(sampleYAML))
	revex := m.FilterByTeam("revex")
	if len(revex) != 2 {
		t.Errorf("FilterByTeam(revex): want 2, got %d", len(revex))
	}
	platform := m.FilterByTeam("platform")
	if len(platform) != 1 {
		t.Errorf("FilterByTeam(platform): want 1, got %d", len(platform))
	}
}

func TestRepo_Validate(t *testing.T) {
	valid := manifest.Repo{Name: "foo", GitHubURL: "https://github.com/GoHighLevel/foo"}
	if err := valid.Validate(); err != nil {
		t.Errorf("Validate: want nil for valid repo, got %v", err)
	}

	missingName := manifest.Repo{GitHubURL: "https://github.com/GoHighLevel/foo"}
	if err := missingName.Validate(); err == nil {
		t.Error("Validate: want error for missing name")
	}

	missingURL := manifest.Repo{Name: "foo"}
	if err := missingURL.Validate(); err == nil {
		t.Error("Validate: want error for missing github_url")
	}

	badURL := manifest.Repo{Name: "foo", GitHubURL: "not-a-url"}
	if err := badURL.Validate(); err == nil {
		t.Error("Validate: want error for invalid github_url")
	}
}
