// Package pipeline — PopulateFromProjectDB builds org.db using MCP tools only.
// Phase 1: list_projects → repo metadata + team ownership
// Phase 2: get_architecture per project → routes + packages → api_contracts + repo_dependencies
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
// Phase 2: get_architecture per project → routes + packages (rate-limited, ~3 min)
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

	// ── Phase 2: Extract routes + packages via get_architecture ──
	// Circuit breaker: if first 3 calls all fail, skip phase 2 entirely
	// (C binary may not support get_architecture or project .db files not ready)
	slog.Info("phase 2: extracting routes and packages from project DBs", "projects", len(entries))

	routeCount := 0
	packageCount := 0
	errorCount := 0
	consecutiveErrors := 0
	const maxConsecutiveErrors = 3 // circuit breaker threshold

	for i, entry := range entries {
		// Rate limit: 1 call/sec to avoid pool exhaustion
		if i > 0 {
			time.Sleep(1 * time.Second)
		}

		archResult, err := caller.CallTool(ctx, "get_architecture", map[string]interface{}{
			"project": entry.projectName,
		})
		if err != nil {
			errorCount++
			consecutiveErrors++
			if consecutiveErrors <= 3 {
				slog.Warn("get_architecture failed", "project", entry.projectName, "err", err,
					"consecutive_errors", consecutiveErrors)
			}
			// Circuit breaker: stop if first N calls all fail
			if consecutiveErrors >= maxConsecutiveErrors && routeCount == 0 && packageCount == 0 {
				slog.Warn("phase 2: circuit breaker tripped — C binary get_architecture not available, skipping",
					"errors", errorCount, "threshold", maxConsecutiveErrors)
				break
			}
			continue
		}
		consecutiveErrors = 0 // reset on success

		archText := extractText(archResult)
		if archText == "" || archText == "null" {
			continue
		}

		// Parse architecture response
		var arch architectureResponse
		if err := json.Unmarshal([]byte(archText), &arch); err != nil {
			continue
		}

		// Extract routes → api_contracts
		for _, route := range arch.Routes {
			if route.Path == "" {
				continue
			}
			db.InsertAPIContract(orgdb.APIContract{
				ProviderRepo:   entry.repoName,
				Method:         strings.ToUpper(route.Method),
				Path:           route.Path,
				ProviderSymbol: route.Handler,
				Confidence:     0.3,
			})
			routeCount++
		}

		// Extract GHL-internal packages → repo_dependencies
		for _, pkg := range arch.Packages {
			if isGHLPackage(pkg.Name) {
				scope, name := splitPackage(pkg.Name)
				if scope != "" {
					db.UpsertPackageDep(entry.repoName, orgdb.Dep{
						Scope:   scope,
						Name:    name,
						DepType: "dependencies",
					})
					packageCount++
				}
			}
		}

		if (i+1)%50 == 0 {
			slog.Info("phase 2 progress", "processed", i+1, "total", len(entries),
				"routes", routeCount, "packages", packageCount, "errors", errorCount)
		}
	}

	slog.Info("phase 2 complete", "routes", routeCount, "packages", packageCount, "errors", errorCount)

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

	slog.Info("org.db populated",
		"repos", len(entries), "routes", routeCount, "packages", packageCount, "errors", errorCount)
	return nil
}

type projEntry struct {
	projectName string // original project name (for MCP calls)
	repoName    string // stripped name (for org.db)
}

// architectureResponse is the parsed get_architecture response.
type architectureResponse struct {
	Routes   []archRoute   `json:"routes"`
	Packages []archPackage `json:"packages"`
}

type archRoute struct {
	Method  string `json:"method"`
	Path    string `json:"path"`
	Handler string `json:"handler"`
}

type archPackage struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type projectInfo struct {
	Name  string `json:"name"`
	Nodes int    `json:"nodes"`
	Edges int    `json:"edges"`
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

func isGHLPackage(name string) bool {
	return strings.HasPrefix(name, "@platform-core/") ||
		strings.HasPrefix(name, "@platform-ui/") ||
		strings.HasPrefix(name, "@gohighlevel/") ||
		strings.HasPrefix(name, "@ghl/") ||
		strings.HasPrefix(name, "@frontend-core/")
}

func splitPackage(name string) (string, string) {
	if !strings.HasPrefix(name, "@") {
		return "", name
	}
	idx := strings.Index(name, "/")
	if idx < 0 {
		return "", name
	}
	return name[:idx], name[idx+1:]
}

// waitForProjects polls list_projects until minCount projects are available or timeout.
// Returns updated entries with repo metadata populated in org.db.
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
			// Re-populate org.db with full project list
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
	// Return whatever we got last time (re-enumerate)
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
