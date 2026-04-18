// Package pipeline — PopulateFromProjectDB builds org.db from hydrated project .db files.
// This is the CORRECT approach for Cloud Run: project .db files are persisted to GCS
// and hydrated on startup. No source clones needed.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdb"
)

// MCPCaller is the interface for calling MCP tools on the C binary.
type MCPCaller interface {
	CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error)
}

// PopulateOrgFromProjectDBs builds the org.db from all hydrated project .db files.
// It calls MCP tools (list_projects, search_graph, get_architecture) on the C binary
// to extract route, dependency, and architecture data from each project's SQLite DB.
// This works on fresh containers because project .db files are hydrated from GCS.
func PopulateOrgFromProjectDBs(ctx context.Context, db *orgdb.DB, caller MCPCaller, repos []manifest.Repo) error {
	// Step 1: List all indexed projects
	projects, err := listProjects(ctx, caller)
	if err != nil {
		return fmt.Errorf("pipeline: list projects: %w", err)
	}
	slog.Info("populating org.db from project DBs", "projects", len(projects))

	// Build repo lookup for team/type metadata
	repoByName := make(map[string]manifest.Repo, len(repos))
	for _, r := range repos {
		repoByName[r.Name] = r
	}

	populated := 0
	for _, proj := range projects {
		repoName := proj.Project
		// Try to match project name to manifest repo
		repo, ok := repoByName[repoName]
		if !ok {
			// Try common prefixes that the C binary adds (path-based project names)
			for _, prefix := range []string{
				"data-fleet-cache-repos-",
				"tmp-fleet-cache-repos-",
				"tmp-fleet-cache-",
				"app-fleet-cache-",
			} {
				stripped := strings.TrimPrefix(repoName, prefix)
				if stripped != repoName {
					if r, found := repoByName[stripped]; found {
						repo = r
						repoName = stripped
						ok = true
						break
					}
				}
			}
		}
		if !ok {
			// Use project name as-is with default metadata
			repo = manifest.Repo{Name: repoName}
		}

		// Clear old data
		db.ClearRepoData(repoName)

		// Upsert repo record
		db.UpsertRepo(orgdb.RepoRecord{
			Name:      repoName,
			GitHubURL: repo.GitHubURL,
			Team:      repo.Team,
			Type:      repo.Type,
			NodeCount: proj.Nodes,
			EdgeCount: proj.Edges,
		})
		db.UpsertTeamOwnership(repoName, repo.Team, "")

		// Extract routes from project DB via MCP
		routes, err := getRoutes(ctx, caller, proj.Project)
		if err != nil {
			slog.Warn("failed to get routes from project DB", "project", proj.Project, "err", err)
		} else {
			for _, route := range routes {
				db.InsertAPIContract(orgdb.APIContract{
					ProviderRepo:   repoName,
					Method:         route.Method,
					Path:           route.Path,
					ProviderSymbol: route.Handler,
					Confidence:     0.3,
				})
			}
		}

		// Extract HTTP_CALLS (cross-service calls) from project DB
		httpCalls, err := getHTTPCalls(ctx, caller, proj.Project)
		if err != nil {
			slog.Warn("failed to get HTTP calls from project DB", "project", proj.Project, "err", err)
		} else {
			for _, call := range httpCalls {
				db.InsertAPIContract(orgdb.APIContract{
					ConsumerRepo:   repoName,
					Method:         call.Method,
					Path:           call.Path,
					ConsumerSymbol: call.Caller,
					Confidence:     0.5,
				})
			}
		}

		populated++
		if populated%50 == 0 {
			slog.Info("org.db population progress", "populated", populated, "total", len(projects))
		}
	}

	// Cross-reference contracts
	matched, err := db.CrossReferenceContracts()
	if err != nil {
		slog.Warn("cross-reference contracts failed", "err", err)
	} else {
		slog.Info("cross-referenced API contracts", "matched", matched)
	}

	slog.Info("org.db populated from project DBs", "repos", populated, "projects", len(projects))
	return nil
}

// projectInfo holds basic info from list_projects.
type projectInfo struct {
	Project string `json:"name"`
	Nodes   int    `json:"nodes"`
	Edges   int    `json:"edges"`
}

func listProjects(ctx context.Context, caller MCPCaller) ([]projectInfo, error) {
	result, err := caller.CallTool(ctx, "list_projects", nil)
	if err != nil {
		return nil, err
	}
	text := extractText(result)
	if text == "" {
		return nil, nil
	}

	// list_projects returns {"projects": [...]}
	var resp struct {
		Projects []projectInfo `json:"projects"`
	}
	if err := json.Unmarshal([]byte(text), &resp); err != nil {
		// Try as raw array
		var projects []projectInfo
		if err2 := json.Unmarshal([]byte(text), &projects); err2 != nil {
			return nil, fmt.Errorf("parse list_projects: %w", err)
		}
		return projects, nil
	}
	return resp.Projects, nil
}

type routeInfo struct {
	Method  string
	Path    string
	Handler string
}

func getRoutes(ctx context.Context, caller MCPCaller, project string) ([]routeInfo, error) {
	// Use search_graph to find all Route nodes in this project
	result, err := caller.CallTool(ctx, "search_graph", map[string]interface{}{
		"project": project,
		"label":   "Route",
		"limit":   500,
	})
	if err != nil {
		return nil, err
	}
	text := extractText(result)
	if text == "" || text == "[]" || text == "null" {
		return nil, nil
	}

	var nodes []struct {
		Name       string `json:"name"`
		QN         string `json:"qualified_name"`
		Properties string `json:"properties"`
	}
	if err := json.Unmarshal([]byte(text), &nodes); err != nil {
		return nil, fmt.Errorf("parse route nodes: %w", err)
	}

	var routes []routeInfo
	for _, n := range nodes {
		route := routeInfo{Path: n.Name}
		// Parse properties JSON for method
		var props map[string]interface{}
		if json.Unmarshal([]byte(n.Properties), &props) == nil {
			if m, ok := props["method"].(string); ok {
				route.Method = m
			}
			if h, ok := props["handler"].(string); ok {
				route.Handler = h
			}
		}
		// Extract method from qualified name: __route__POST__/path
		if strings.HasPrefix(n.QN, "__route__") {
			parts := strings.SplitN(strings.TrimPrefix(n.QN, "__route__"), "__", 2)
			if len(parts) == 2 {
				route.Method = parts[0]
				if route.Path == "" {
					route.Path = parts[1]
				}
			}
		}
		if route.Method == "" {
			route.Method = "GET" // default
		}
		routes = append(routes, route)
	}
	return routes, nil
}

type httpCallInfo struct {
	Method string
	Path   string
	Caller string
}

func getHTTPCalls(ctx context.Context, caller MCPCaller, project string) ([]httpCallInfo, error) {
	// Use search_graph to find edges of type HTTP_CALLS
	result, err := caller.CallTool(ctx, "search_graph", map[string]interface{}{
		"project":      project,
		"label":        "Function",
		"relationship": "HTTP_CALLS",
		"direction":    "outbound",
		"limit":        500,
	})
	if err != nil {
		return nil, err
	}
	text := extractText(result)
	if text == "" || text == "[]" || text == "null" {
		return nil, nil
	}

	var nodes []struct {
		Name       string `json:"name"`
		QN         string `json:"qualified_name"`
		Neighbors  string `json:"neighbors"`
		Properties string `json:"properties"`
	}
	if err := json.Unmarshal([]byte(text), &nodes); err != nil {
		return nil, nil // silently skip parse errors
	}

	var calls []httpCallInfo
	for _, n := range nodes {
		// The neighbor is the Route node being called
		var neighbors []struct {
			Name string `json:"name"`
			QN   string `json:"qualified_name"`
		}
		if json.Unmarshal([]byte(n.Neighbors), &neighbors) == nil {
			for _, neighbor := range neighbors {
				call := httpCallInfo{
					Caller: n.QN,
					Path:   neighbor.Name,
				}
				// Extract method from route QN
				if strings.HasPrefix(neighbor.QN, "__route__") {
					parts := strings.SplitN(strings.TrimPrefix(neighbor.QN, "__route__"), "__", 2)
					if len(parts) == 2 {
						call.Method = parts[0]
						call.Path = parts[1]
					}
				}
				if call.Method == "" {
					call.Method = "GET"
				}
				calls = append(calls, call)
			}
		}
	}
	return calls, nil
}

func extractText(result *mcp.ToolResult) string {
	if result == nil || len(result.Content) == 0 {
		return ""
	}
	return result.Content[0].Text
}
