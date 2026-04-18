// Package pipeline — PopulateFromProjectDB builds org.db using MCP tools only.
// Phase 1: list_projects → repo metadata + team ownership
// Phase 2: search_graph(label=Route) per project → routes → api_contracts
//          get_architecture per project → node/edge stats (packages via Module nodes)
// Phase 3: CrossReferenceContracts → match consumers to providers
//
// IMPORTANT: Do NOT open project .db files from Go — this conflicts with the C binary
// subprocesses and crashes the bridge pool. Use MCP tools only.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdb"
)

// MCPCaller is the interface for calling MCP tools on the C binary.
type MCPCaller interface {
	CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error)
}

// PopulateOrgFromProjectDBs builds org.db in 3 phases using MCP tools.
// Phase 1: list_projects → repo metadata (single call)
// Phase 2: search_graph(label=Route) per project → routes → api_contracts
// Phase 3: CrossReferenceContracts → match consumers to providers
func PopulateOrgFromProjectDBs(ctx context.Context, db *orgdb.DB, caller MCPCaller, repos []manifest.Repo, cbmCacheDir string) error {
	// ── Phase 1: Repo metadata from list_projects ──
	result, err := caller.CallTool(ctx, "list_projects", nil)
	if err != nil {
		return fmt.Errorf("pipeline: list_projects: %w", err)
	}
	text := extractText(result)
	if text == "" || text == "null" {
		return fmt.Errorf("pipeline: list_projects returned empty")
	}

	var projects []projectInfo
	if err := json.Unmarshal([]byte(text), &projects); err != nil {
		var wrapped struct{ Projects []projectInfo }
		if err2 := json.Unmarshal([]byte(text), &wrapped); err2 != nil {
			return fmt.Errorf("pipeline: parse list_projects: %w", err)
		}
		projects = wrapped.Projects
	}

	slog.Info("phase 1: populating repo metadata", "projects", len(projects))

	repoByName := make(map[string]manifest.Repo, len(repos))
	for _, r := range repos {
		repoByName[r.Name] = r
	}

	var entries []projEntry

	for _, proj := range projects {
		repoName := stripProjectPrefix(proj.Name)
		repo, ok := repoByName[repoName]
		if !ok {
			repo = manifest.Repo{Name: repoName}
		}

		db.UpsertRepo(orgdb.RepoRecord{
			Name:      repoName,
			GitHubURL: repo.GitHubURL,
			Team:      repo.Team,
			Type:      repo.Type,
			NodeCount: proj.Nodes,
			EdgeCount: proj.Edges,
		})
		db.UpsertTeamOwnership(repoName, repo.Team, "")

		entries = append(entries, projEntry{projectName: proj.Name, repoName: repoName})
	}

	slog.Info("phase 1 complete", "repos", len(entries))

	// If Phase 1 found too few projects, GCS data likely hasn't loaded yet.
	// Wait up to 3 minutes, polling list_projects every 30s.
	if len(entries) < 50 {
		slog.Info("phase 1 found few projects — waiting for GCS data to load", "found", len(entries))
		entries = waitForProjects(ctx, caller, db, repoByName, repos, 50, 3*time.Minute)
		slog.Info("after waiting", "projects", len(entries))
	}

	// ── Phase 2: Extract routes via search_graph(label=Route) ──
	// Each project's graph has Route nodes with qualified_name = "__route__METHOD__path"
	// Circuit breaker: stop after 5 consecutive errors (C binary unstable)
	slog.Info("phase 2: extracting routes from project graphs", "projects", len(entries))

	routeCount := 0
	errorCount := 0
	consecutiveErrors := 0
	const maxConsecutiveErrors = 5

	for i, entry := range entries {
		// Rate limit: 1 call/sec to avoid pool exhaustion
		if i > 0 {
			time.Sleep(1 * time.Second)
		}

		// search_graph returns Route-label nodes for this project
		searchResult, err := caller.CallTool(ctx, "search_graph", map[string]interface{}{
			"project": entry.projectName,
			"label":   "Route",
			"limit":   200, // max routes per project
		})
		if err != nil {
			errorCount++
			consecutiveErrors++
			if consecutiveErrors <= 5 {
				slog.Warn("search_graph(Route) failed", "project", entry.projectName, "err", err)
			}
			if consecutiveErrors >= maxConsecutiveErrors && routeCount == 0 {
				slog.Warn("phase 2: circuit breaker — search_graph not working, skipping",
					"errors", errorCount)
				break
			}
			continue
		}
		consecutiveErrors = 0

		searchText := extractText(searchResult)
		if searchText == "" || searchText == "null" {
			continue
		}

		var searchResp searchGraphResponse
		if err := json.Unmarshal([]byte(searchText), &searchResp); err != nil {
			continue
		}

		// Parse routes from qualified_name: "__route__METHOD__path"
		for _, node := range searchResp.Results {
			method, path := parseRouteQualifiedName(node.QualifiedName)
			if path == "" {
				continue
			}
			db.InsertAPIContract(orgdb.APIContract{
				ProviderRepo:   entry.repoName,
				Method:         method,
				Path:           path,
				ProviderSymbol: node.Name,
				Confidence:     0.3,
			})
			routeCount++
		}

		if (i+1)%50 == 0 {
			slog.Info("phase 2 progress", "processed", i+1, "total", len(entries),
				"routes", routeCount, "errors", errorCount)
		}
	}

	slog.Info("phase 2 complete", "routes", routeCount, "errors", errorCount)

	// ── Phase 3: Cross-reference contracts (only if phase 2 found data) ──
	if routeCount > 0 {
		slog.Info("phase 3: cross-referencing API contracts")
		matched, err := db.CrossReferenceContracts()
		if err != nil {
			slog.Warn("cross-reference failed", "err", err)
		} else {
			slog.Info("phase 3 complete", "matched", matched)
		}
	}

	slog.Info("org.db populated", "repos", len(entries), "routes", routeCount, "errors", errorCount)
	return nil
}

type projEntry struct {
	projectName string // original project name (for MCP calls)
	repoName    string // stripped name (for org.db)
}

type searchGraphResponse struct {
	Total   int              `json:"total"`
	Results []searchGraphNode `json:"results"`
	HasMore bool             `json:"has_more"`
}

type searchGraphNode struct {
	Name          string `json:"name"`
	QualifiedName string `json:"qualified_name"`
	Label         string `json:"label"`
	FilePath      string `json:"file_path"`
}

type projectInfo struct {
	Name  string `json:"name"`
	Nodes int    `json:"nodes"`
	Edges int    `json:"edges"`
}

// parseRouteQualifiedName extracts method and path from "__route__METHOD__path".
// Example: "__route__POST__/api/orders" → ("POST", "/api/orders")
// Example: "__route__ANY__/health" → ("ANY", "/health")
func parseRouteQualifiedName(qn string) (string, string) {
	const prefix = "__route__"
	if !strings.HasPrefix(qn, prefix) {
		return "", ""
	}
	rest := qn[len(prefix):] // "POST__/api/orders"
	idx := strings.Index(rest, "__")
	if idx < 0 {
		return "", ""
	}
	method := rest[:idx]
	path := rest[idx+2:] // skip "__"
	if path == "" {
		return "", ""
	}
	return strings.ToUpper(method), path
}

func stripProjectPrefix(name string) string {
	for _, prefix := range []string{
		"data-fleet-cache-repos-",
		"tmp-fleet-cache-repos-",
		"tmp-fleet-cache-",
		"app-fleet-cache-",
	} {
		if strings.HasPrefix(name, prefix) {
			return strings.TrimPrefix(name, prefix)
		}
	}
	return name
}

// waitForProjects polls list_projects until minCount projects are available or timeout.
func waitForProjects(ctx context.Context, caller MCPCaller, db *orgdb.DB,
	repoByName map[string]manifest.Repo, repos []manifest.Repo,
	minCount int, timeout time.Duration) []projEntry {

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		time.Sleep(30 * time.Second)

		result, err := caller.CallTool(ctx, "list_projects", nil)
		if err != nil {
			continue
		}
		text := extractText(result)
		if text == "" || text == "null" {
			continue
		}

		var projects []projectInfo
		if err := json.Unmarshal([]byte(text), &projects); err != nil {
			var wrapped struct{ Projects []projectInfo }
			if err2 := json.Unmarshal([]byte(text), &wrapped); err2 != nil {
				continue
			}
			projects = wrapped.Projects
		}

		slog.Info("waitForProjects: poll", "found", len(projects), "need", minCount)

		if len(projects) >= minCount {
			var entries []projEntry
			for _, proj := range projects {
				repoName := stripProjectPrefix(proj.Name)
				repo, ok := repoByName[repoName]
				if !ok {
					repo = manifest.Repo{Name: repoName}
				}
				db.UpsertRepo(orgdb.RepoRecord{
					Name:      repoName,
					GitHubURL: repo.GitHubURL,
					Team:      repo.Team,
					Type:      repo.Type,
					NodeCount: proj.Nodes,
					EdgeCount: proj.Edges,
				})
				db.UpsertTeamOwnership(repoName, repo.Team, "")
				entries = append(entries, projEntry{projectName: proj.Name, repoName: repoName})
			}
			return entries
		}
	}

	slog.Warn("waitForProjects: timeout — proceeding with available projects")
	result, err := caller.CallTool(ctx, "list_projects", nil)
	if err != nil {
		return nil
	}
	text := extractText(result)
	var projects []projectInfo
	if err := json.Unmarshal([]byte(text), &projects); err != nil {
		return nil
	}
	var entries []projEntry
	for _, proj := range projects {
		repoName := stripProjectPrefix(proj.Name)
		repo := repoByName[repoName]
		db.UpsertRepo(orgdb.RepoRecord{
			Name:      repoName,
			GitHubURL: repo.GitHubURL,
			Team:      repo.Team,
			Type:      repo.Type,
			NodeCount: proj.Nodes,
			EdgeCount: proj.Edges,
		})
		db.UpsertTeamOwnership(repoName, repo.Team, "")
		entries = append(entries, projEntry{projectName: proj.Name, repoName: repoName})
	}
	return entries
}

func extractText(result *mcp.ToolResult) string {
	if result == nil || len(result.Content) == 0 {
		return ""
	}
	return result.Content[0].Text
}
