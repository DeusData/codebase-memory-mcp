// Package orgdiscovery discovers repositories in a GitHub organization via the API.
package orgdiscovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
)

// Scanner discovers repositories in a GitHub organization via API.
type Scanner struct {
	org        string
	token      string
	client     *http.Client
	apiBaseURL string // default: "https://api.github.com", override for tests
}

// NewScanner creates a scanner for the given GitHub org.
func NewScanner(org, token string) *Scanner {
	return &Scanner{
		org:        org,
		token:      token,
		client:     &http.Client{Timeout: 30 * time.Second},
		apiBaseURL: "https://api.github.com",
	}
}

// SetAPIBaseURL overrides the GitHub API base URL (for testing with httptest).
func (s *Scanner) SetAPIBaseURL(url string) {
	s.apiBaseURL = url
}

// ScanOrg lists all repos in the org and returns them as manifest.Repo entries.
// It paginates through all pages (100 per page).
// Filters out: archived repos, forks.
func (s *Scanner) ScanOrg(ctx context.Context) ([]manifest.Repo, error) {
	var allRepos []manifest.Repo
	page := 1

	for {
		repos, hasMore, err := s.fetchRepoPage(ctx, page)
		if err != nil {
			return nil, fmt.Errorf("orgdiscovery: fetch page %d: %w", page, err)
		}
		allRepos = append(allRepos, repos...)
		if !hasMore {
			break
		}
		page++
	}

	return allRepos, nil
}

// ghRepo is the GitHub API response for a single repo.
type ghRepo struct {
	Name          string   `json:"name"`
	FullName      string   `json:"full_name"`
	CloneURL      string   `json:"clone_url"`
	HTMLURL       string   `json:"html_url"`
	Description   string   `json:"description"`
	Language      string   `json:"language"`
	Topics        []string `json:"topics"`
	DefaultBranch string   `json:"default_branch"`
	Archived      bool     `json:"archived"`
	Fork          bool     `json:"fork"`
	Size          int      `json:"size"`
	PushedAt      string   `json:"pushed_at"`
}

func (s *Scanner) fetchRepoPage(ctx context.Context, page int) ([]manifest.Repo, bool, error) {
	url := fmt.Sprintf("%s/orgs/%s/repos?type=all&per_page=100&page=%d&sort=full_name", s.apiBaseURL, s.org, page)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, false, fmt.Errorf("github API %d: %s", resp.StatusCode, string(body))
	}

	var ghRepos []ghRepo
	if err := json.NewDecoder(resp.Body).Decode(&ghRepos); err != nil {
		return nil, false, fmt.Errorf("decode response: %w", err)
	}

	var repos []manifest.Repo
	for _, gh := range ghRepos {
		if gh.Archived || gh.Fork {
			continue
		}
		repo := manifest.Repo{
			Name:      gh.Name,
			GitHubURL: gh.CloneURL,
			Team:      inferTeamFromTopics(gh.Topics),
			Type:      inferTypeFromLanguage(gh.Language, gh.Topics),
			Tags:      buildTags(gh.Language, gh.Topics),
		}
		repos = append(repos, repo)
	}

	hasMore := len(ghRepos) == 100 // Full page means there might be more
	return repos, hasMore, nil
}

// inferTeamFromTopics extracts team from topics with "team-" prefix.
func inferTeamFromTopics(topics []string) string {
	for _, t := range topics {
		if strings.HasPrefix(t, "team-") {
			return strings.TrimPrefix(t, "team-")
		}
	}
	return "" // will be enriched later by CODEOWNERS/Teams API
}

// inferTypeFromLanguage makes a best guess at repo type from primary language.
func inferTypeFromLanguage(lang string, topics []string) string {
	// Check topics first
	for _, t := range topics {
		switch t {
		case "library", "lib", "package":
			return "library"
		case "infrastructure", "infra", "terraform", "helm":
			return "infra"
		case "documentation", "docs":
			return "docs"
		case "frontend", "ui", "web":
			return "frontend"
		case "backend", "api", "service", "microservice":
			return "backend"
		}
	}
	// Fall back to language
	switch strings.ToLower(lang) {
	case "vue", "svelte":
		return "frontend"
	case "hcl":
		return "infra"
	case "":
		return "other"
	default:
		return "backend" // most GHL repos are backend services
	}
}

// ScanUpdatedSince returns repos that were pushed to since the given time.
// Uses the GitHub API sort=pushed parameter to efficiently find recently-changed repos.
// Stops paginating when it reaches repos older than since.
func (s *Scanner) ScanUpdatedSince(ctx context.Context, since time.Time) ([]manifest.Repo, error) {
	var updated []manifest.Repo
	page := 1

	for {
		url := fmt.Sprintf("%s/orgs/%s/repos?type=all&per_page=100&page=%d&sort=pushed&direction=desc",
			s.apiBaseURL, s.org, page)

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+s.token)
		req.Header.Set("Accept", "application/vnd.github+json")

		resp, err := s.client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("github API %d: %s", resp.StatusCode, string(body))
		}

		var ghRepos []ghRepo
		if err := json.NewDecoder(resp.Body).Decode(&ghRepos); err != nil {
			return nil, err
		}

		if len(ghRepos) == 0 {
			break
		}

		reachedOld := false
		for _, gh := range ghRepos {
			if gh.Archived || gh.Fork {
				continue
			}
			pushedAt, err := time.Parse(time.RFC3339, gh.PushedAt)
			if err != nil {
				continue
			}
			if pushedAt.Before(since) {
				reachedOld = true
				break
			}
			repo := manifest.Repo{
				Name:      gh.Name,
				GitHubURL: gh.CloneURL,
				Team:      inferTeamFromTopics(gh.Topics),
				Type:      inferTypeFromLanguage(gh.Language, gh.Topics),
				Tags:      buildTags(gh.Language, gh.Topics),
			}
			updated = append(updated, repo)
		}

		if reachedOld || len(ghRepos) < 100 {
			break
		}
		page++
	}

	return updated, nil
}

// buildTags combines language and topics into tags.
func buildTags(lang string, topics []string) []string {
	tags := make([]string, 0, len(topics)+1)
	if lang != "" {
		tags = append(tags, strings.ToLower(lang))
	}
	for _, t := range topics {
		if !strings.HasPrefix(t, "team-") { // skip team topics, already in Team field
			tags = append(tags, t)
		}
	}
	return tags
}
