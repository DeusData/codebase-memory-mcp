// Package orgdiscovery provides ownership enrichment for GitHub repos.
package orgdiscovery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
)

// EnrichOwnership enriches repos with team ownership from CODEOWNERS files
// and GitHub Teams API. Updates the Team field on each repo.
// Priority: CODEOWNERS catch-all > Teams(admin) > Topics(team-*) > existing Team > name inference
func (s *Scanner) EnrichOwnership(ctx context.Context, repos []manifest.Repo) error {
	// Fetch team→repo mappings from GitHub Teams API
	teamsMap, err := s.fetchTeamRepos(ctx)
	if err != nil {
		log.Printf("orgdiscovery: teams API failed, skipping: %v", err)
		teamsMap = make(map[string]string)
	}

	// Fetch CODEOWNERS catch-all for each repo concurrently
	codeownersMap := s.fetchAllCodeowners(ctx, repos)

	for i, repo := range repos {
		// Priority 1: CODEOWNERS catch-all
		if owner := codeownersMap[repo.Name]; owner != "" {
			repos[i].Team = owner
			continue
		}
		// Priority 2: GitHub Teams (admin permission)
		if team := teamsMap[repo.Name]; team != "" {
			repos[i].Team = team
			continue
		}
		// Priority 3: Topic-based team (already set by ScanOrg)
		if repos[i].Team != "" {
			continue
		}
		// Priority 4: Infer from repo name prefix
		repos[i].Team = inferTeamFromName(repo.Name)
	}

	return nil
}

// fetchAllCodeowners fetches CODEOWNERS catch-all owners for all repos concurrently.
// Uses a semaphore to limit concurrent requests.
func (s *Scanner) fetchAllCodeowners(ctx context.Context, repos []manifest.Repo) map[string]string {
	const concurrency = 10

	result := make(map[string]string, len(repos))
	var mu sync.Mutex
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, repo := range repos {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			owner := s.fetchCodeowners(ctx, name)
			if owner != "" {
				mu.Lock()
				result[name] = owner
				mu.Unlock()
			}
		}(repo.Name)
	}

	wg.Wait()
	return result
}

// ghContentsResponse is the GitHub contents API response.
type ghContentsResponse struct {
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
}

// fetchCodeowners fetches and parses the CODEOWNERS file for a repo.
// Returns the default (catch-all *) owner team, or "" if not found.
func (s *Scanner) fetchCodeowners(ctx context.Context, repoName string) string {
	url := fmt.Sprintf("%s/repos/%s/%s/contents/.github/CODEOWNERS", s.apiBaseURL, s.org, repoName)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := s.client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ""
	}
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return ""
	}

	var contents ghContentsResponse
	if err := json.NewDecoder(resp.Body).Decode(&contents); err != nil {
		return ""
	}

	if contents.Encoding != "base64" {
		return ""
	}

	decoded, err := base64.StdEncoding.DecodeString(contents.Content)
	if err != nil {
		return ""
	}

	return parseCatchAllOwner(string(decoded), s.org)
}

// parseCatchAllOwner extracts the team from the catch-all (*) line in CODEOWNERS content.
// Looks for @org/team-slug format and returns team-slug.
func parseCatchAllOwner(content, org string) string {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "*" {
			// Look for @org/team pattern
			for _, owner := range fields[1:] {
				prefix := "@" + org + "/"
				if strings.HasPrefix(owner, prefix) {
					return strings.TrimPrefix(owner, prefix)
				}
			}
		}
	}
	return ""
}

// ghTeam is the GitHub Teams API response for a single team.
type ghTeam struct {
	Slug string `json:"slug"`
}

// ghTeamRepo is the GitHub Teams repo response.
type ghTeamRepo struct {
	Name        string            `json:"name"`
	Permissions map[string]bool   `json:"permissions"`
}

// fetchTeamRepos fetches team->repo mappings from the GitHub Teams API.
// Returns map[repoName]teamSlug for teams with admin or maintain permission.
func (s *Scanner) fetchTeamRepos(ctx context.Context) (map[string]string, error) {
	teams, err := s.listTeams(ctx)
	if err != nil {
		return nil, fmt.Errorf("list teams: %w", err)
	}

	// map[repoName] -> {teamSlug, priority}
	type ownership struct {
		team     string
		priority int // admin=3, maintain=2, push=1
	}
	best := make(map[string]ownership)

	for _, team := range teams {
		repos, err := s.listTeamRepos(ctx, team.Slug)
		if err != nil {
			log.Printf("orgdiscovery: list repos for team %s: %v", team.Slug, err)
			continue
		}
		for _, repo := range repos {
			p := permissionPriority(repo.Permissions)
			if p == 0 {
				continue
			}
			if cur, ok := best[repo.Name]; !ok || p > cur.priority {
				best[repo.Name] = ownership{team: team.Slug, priority: p}
			}
		}
	}

	result := make(map[string]string, len(best))
	for name, o := range best {
		result[name] = o.team
	}
	return result, nil
}

// permissionPriority returns a numeric priority for the highest permission level.
func permissionPriority(perms map[string]bool) int {
	if perms["admin"] {
		return 3
	}
	if perms["maintain"] {
		return 2
	}
	if perms["push"] {
		return 1
	}
	return 0
}

// listTeams lists all teams in the organization.
func (s *Scanner) listTeams(ctx context.Context) ([]ghTeam, error) {
	var allTeams []ghTeam
	page := 1

	for {
		url := fmt.Sprintf("%s/orgs/%s/teams?per_page=100&page=%d", s.apiBaseURL, s.org, page)
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

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("teams API %d: %s", resp.StatusCode, string(body))
		}

		var teams []ghTeam
		if err := json.NewDecoder(resp.Body).Decode(&teams); err != nil {
			return nil, fmt.Errorf("decode teams: %w", err)
		}
		allTeams = append(allTeams, teams...)

		if len(teams) < 100 {
			break
		}
		page++
	}

	return allTeams, nil
}

// listTeamRepos lists all repos for a specific team.
func (s *Scanner) listTeamRepos(ctx context.Context, teamSlug string) ([]ghTeamRepo, error) {
	var allRepos []ghTeamRepo
	page := 1

	for {
		url := fmt.Sprintf("%s/orgs/%s/teams/%s/repos?per_page=100&page=%d", s.apiBaseURL, s.org, teamSlug, page)
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

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("team repos API %d: %s", resp.StatusCode, string(body))
		}

		var repos []ghTeamRepo
		if err := json.NewDecoder(resp.Body).Decode(&repos); err != nil {
			return nil, fmt.Errorf("decode team repos: %w", err)
		}
		allRepos = append(allRepos, repos...)

		if len(repos) < 100 {
			break
		}
		page++
	}

	return allRepos, nil
}

// inferTeamFromName guesses team from common GHL repo name prefixes.
func inferTeamFromName(name string) string {
	// Order matters: longer prefixes first to avoid false matches
	prefixes := []struct {
		prefix string
		team   string
	}{
		{"ghl-revex-", "revex"},
		{"ghl-crm-", "crm"},
		{"automation-", "automation"},
		{"leadgen-", "leadgen"},
		{"revex-", "revex"},
		{"dev-", "commerce"},
		{"ai-", "ai"},
		{"mobile-", "mobile"},
		{"marketplace-", "marketplace"},
		{"sdet-", "sdet"},
		{"i18n-", "i18n"},
		{"platform-", "platform"},
	}
	for _, p := range prefixes {
		if strings.HasPrefix(name, p.prefix) {
			return p.team
		}
	}
	return "platform" // default for GHL
}
