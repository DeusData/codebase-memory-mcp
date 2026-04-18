// Package pipeline — PopulateFromProjectDB builds org.db using MCP tools only.
//
// 4 phases:
//   Phase 1: list_projects → repo metadata + team ownership
//   Phase 2a: search_graph(label=Route) → provider-side api_contracts
//   Phase 2b: search_code(InternalRequest) → consumer-side api_contracts
//   Phase 2c: search_code(@platform-core/) → package deps (repo_dependencies)
//   Phase 3: CrossReferenceContracts → match consumers to providers
//
// IMPORTANT: Do NOT open project .db files from Go — this conflicts with the C binary
// subprocesses and crashes the bridge pool. Use MCP tools only.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
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

// PopulateOrgFromProjectDBs builds org.db using MCP tools in 4 phases.
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

	// Wait for GCS data if too few projects
	if len(entries) < 50 {
		slog.Info("waiting for GCS data to load", "found", len(entries))
		entries = waitForProjects(ctx, caller, db, repoByName, repos, 50, 3*time.Minute)
		slog.Info("after waiting", "projects", len(entries))
	}

	// ── Phase 2a: Extract routes → provider contracts ──
	routeCount := extractRoutes(ctx, db, caller, entries)

	// ── Phase 2b: Extract InternalRequest calls → consumer contracts ──
	consumerCount := extractConsumers(ctx, db, caller, entries)

	// ── Phase 2c: Extract @platform-core package deps ──
	packageCount := extractPackageDeps(ctx, db, caller, entries)

	// ── Phase 3: Cross-reference contracts ──
	matched := 0
	if routeCount > 0 && consumerCount > 0 {
		slog.Info("phase 3: cross-referencing API contracts")
		var err error
		matched, err = db.CrossReferenceContracts()
		if err != nil {
			slog.Warn("cross-reference failed", "err", err)
		} else {
			slog.Info("phase 3 complete", "matched", matched)
		}
	}

	slog.Info("org.db fully populated",
		"repos", len(entries), "routes", routeCount, "consumers", consumerCount,
		"packages", packageCount, "cross_referenced", matched)
	return nil
}

// extractRoutes calls search_graph(label=Route) per project and inserts provider contracts.
func extractRoutes(ctx context.Context, db *orgdb.DB, caller MCPCaller, entries []projEntry) int {
	slog.Info("phase 2a: extracting routes", "projects", len(entries))
	routeCount, errorCount, consecutiveErrors := 0, 0, 0

	for i, entry := range entries {
		if i > 0 {
			time.Sleep(500 * time.Millisecond)
		}

		result, err := caller.CallTool(ctx, "search_graph", map[string]interface{}{
			"project": entry.projectName,
			"label":   "Route",
			"limit":   500,
		})
		if err != nil {
			errorCount++
			consecutiveErrors++
			if consecutiveErrors >= 5 && routeCount == 0 {
				slog.Warn("phase 2a: circuit breaker", "errors", errorCount)
				break
			}
			continue
		}
		consecutiveErrors = 0

		text := extractText(result)
		if text == "" || text == "null" {
			continue
		}
		var resp searchGraphResponse
		if err := json.Unmarshal([]byte(text), &resp); err != nil {
			continue
		}

		for _, node := range resp.Results {
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

		if (i+1)%100 == 0 {
			slog.Info("phase 2a progress", "processed", i+1, "routes", routeCount)
		}
	}
	slog.Info("phase 2a complete", "routes", routeCount, "errors", errorCount)
	return routeCount
}

// extractConsumers calls search_code(InternalRequest) + get_code_snippet per project
// to find outbound service calls and insert consumer-side contracts.
func extractConsumers(ctx context.Context, db *orgdb.DB, caller MCPCaller, entries []projEntry) int {
	slog.Info("phase 2b: extracting InternalRequest consumers", "projects", len(entries))
	consumerCount, errorCount, consecutiveErrors := 0, 0, 0

	for i, entry := range entries {
		if i > 0 {
			time.Sleep(500 * time.Millisecond)
		}

		// search_code finds functions containing "InternalRequest"
		result, err := caller.CallTool(ctx, "search_code", map[string]interface{}{
			"project": entry.projectName,
			"pattern": "InternalRequest",
			"limit":   50,
		})
		if err != nil {
			errorCount++
			consecutiveErrors++
			if consecutiveErrors >= 5 && consumerCount == 0 {
				slog.Warn("phase 2b: circuit breaker", "errors", errorCount)
				break
			}
			continue
		}
		consecutiveErrors = 0

		text := extractText(result)
		if text == "" || text == "null" {
			continue
		}

		var codeResp searchCodeResponse
		if err := json.Unmarshal([]byte(text), &codeResp); err != nil {
			continue
		}

		// For each matching function, get the source code to extract service/route
		for j, match := range codeResp.Results {
			if j >= 10 {
				break // limit get_code_snippet calls per project
			}
			if match.QualifiedName == "" {
				continue
			}

			time.Sleep(200 * time.Millisecond) // rate limit snippet calls

			snippetResult, err := caller.CallTool(ctx, "get_code_snippet", map[string]interface{}{
				"project":        entry.projectName,
				"qualified_name": match.QualifiedName,
			})
			if err != nil {
				continue
			}
			snippetText := extractText(snippetResult)
			if snippetText == "" {
				continue
			}

			// Parse the source code for InternalRequest.METHOD({serviceName, route})
			var snippet codeSnippetResponse
			if err := json.Unmarshal([]byte(snippetText), &snippet); err != nil {
				continue
			}

			calls := parseInternalRequestCalls(snippet.Source)
			for _, call := range calls {
				db.InsertAPIContract(orgdb.APIContract{
					ConsumerRepo:   entry.repoName,
					Method:         strings.ToUpper(call.method),
					Path:           "/" + call.serviceName + "/" + call.route,
					ConsumerSymbol: match.Node,
					Confidence:     0.5,
				})
				consumerCount++
			}
		}

		if (i+1)%100 == 0 {
			slog.Info("phase 2b progress", "processed", i+1, "consumers", consumerCount)
		}
	}
	slog.Info("phase 2b complete", "consumers", consumerCount, "errors", errorCount)
	return consumerCount
}

// extractPackageDeps calls search_code(@platform-core/) per project to find package imports.
func extractPackageDeps(ctx context.Context, db *orgdb.DB, caller MCPCaller, entries []projEntry) int {
	slog.Info("phase 2c: extracting package dependencies", "projects", len(entries))
	packageCount, errorCount, consecutiveErrors := 0, 0, 0

	for i, entry := range entries {
		if i > 0 {
			time.Sleep(500 * time.Millisecond)
		}

		// Search for GHL-internal package imports
		for _, scope := range []string{"@platform-core/", "@platform-ui/", "@gohighlevel/", "@frontend-core/"} {
			result, err := caller.CallTool(ctx, "search_code", map[string]interface{}{
				"project": entry.projectName,
				"pattern": scope,
				"limit":   20,
			})
			if err != nil {
				errorCount++
				consecutiveErrors++
				if consecutiveErrors >= 10 {
					break
				}
				continue
			}
			consecutiveErrors = 0

			text := extractText(result)
			if text == "" || text == "null" {
				continue
			}

			var codeResp searchCodeResponse
			if err := json.Unmarshal([]byte(text), &codeResp); err != nil {
				continue
			}

			// For each matching file, try to get the source to extract exact package names
			seen := make(map[string]bool)
			for j, match := range codeResp.Results {
				if j >= 3 {
					break // limit per scope
				}
				if match.QualifiedName == "" {
					continue
				}

				time.Sleep(200 * time.Millisecond)

				snippetResult, err := caller.CallTool(ctx, "get_code_snippet", map[string]interface{}{
					"project":        entry.projectName,
					"qualified_name": match.QualifiedName,
				})
				if err != nil {
					continue
				}
				snippetText := extractText(snippetResult)
				if snippetText == "" {
					continue
				}

				var snippet codeSnippetResponse
				if err := json.Unmarshal([]byte(snippetText), &snippet); err != nil {
					continue
				}

				pkgs := parsePackageImports(snippet.Source, scope)
				for _, pkg := range pkgs {
					if seen[pkg] {
						continue
					}
					seen[pkg] = true
					scopePart := strings.TrimSuffix(scope, "/")
					db.UpsertPackageDep(entry.repoName, orgdb.Dep{
						Scope:   scopePart,
						Name:    pkg,
						DepType: "dependencies",
					})
					packageCount++
				}
			}
		}

		if (i+1)%100 == 0 {
			slog.Info("phase 2c progress", "processed", i+1, "packages", packageCount)
		}
	}
	slog.Info("phase 2c complete", "packages", packageCount, "errors", errorCount)
	return packageCount
}

// ── Types ──

type projEntry struct {
	projectName string
	repoName    string
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

type searchCodeResponse struct {
	Results []searchCodeResult `json:"results"`
}

type searchCodeResult struct {
	Node          string `json:"node"`
	QualifiedName string `json:"qualified_name"`
	Label         string `json:"label"`
	File          string `json:"file"`
	StartLine     int    `json:"start_line"`
	EndLine       int    `json:"end_line"`
	MatchLines    []int  `json:"match_lines"`
}

type codeSnippetResponse struct {
	Name          string `json:"name"`
	QualifiedName string `json:"qualified_name"`
	Source        string `json:"source"`
	FilePath      string `json:"file_path"`
}

type projectInfo struct {
	Name  string `json:"name"`
	Nodes int    `json:"nodes"`
	Edges int    `json:"edges"`
}

type internalCall struct {
	method      string
	serviceName string
	route       string
}

// ── Parsers ──

// parseRouteQualifiedName extracts method and path from "__route__METHOD__path".
func parseRouteQualifiedName(qn string) (string, string) {
	const prefix = "__route__"
	if !strings.HasPrefix(qn, prefix) {
		return "", ""
	}
	rest := qn[len(prefix):]
	idx := strings.Index(rest, "__")
	if idx < 0 {
		return "", ""
	}
	method := rest[:idx]
	path := rest[idx+2:]
	if path == "" {
		return "", ""
	}
	return strings.ToUpper(method), path
}

// InternalRequest.get/post/put/delete({ serviceName: ..., route: ... })
var internalRequestRe = regexp.MustCompile(
	`InternalRequest\.(get|post|put|delete|patch)\(\{[^}]*serviceName:\s*(?:SERVICE_NAME\.)?["']?([A-Z_]+)["']?[^}]*route:\s*` + "`?" + `[^` + "`" + `'"]*?([a-zA-Z][a-zA-Z0-9/\-_:]+)`,
)

// parseInternalRequestCalls extracts service calls from source code.
func parseInternalRequestCalls(source string) []internalCall {
	matches := internalRequestRe.FindAllStringSubmatch(source, -1)
	var calls []internalCall
	for _, m := range matches {
		if len(m) >= 4 {
			calls = append(calls, internalCall{
				method:      m[1],
				serviceName: m[2],
				route:       strings.TrimPrefix(m[3], "/"),
			})
		}
	}
	return calls
}

// parsePackageImports finds @scope/name patterns in source code.
func parsePackageImports(source, scope string) []string {
	var pkgs []string
	seen := make(map[string]bool)
	// Match: from "@platform-core/base-service" or require("@platform-core/base-service")
	re := regexp.MustCompile(regexp.QuoteMeta(scope) + `([a-zA-Z0-9_-]+)`)
	matches := re.FindAllStringSubmatch(source, -1)
	for _, m := range matches {
		if len(m) >= 2 && !seen[m[1]] {
			seen[m[1]] = true
			pkgs = append(pkgs, m[1])
		}
	}
	return pkgs
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

	slog.Warn("waitForProjects: timeout")
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
