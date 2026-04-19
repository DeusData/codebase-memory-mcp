// Package orgtools provides MCP tool handlers for org-level intelligence queries.
package orgtools

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/discovery"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdb"
)

// BridgeCaller can invoke search_code on a per-project basis via the C binary.
type BridgeCaller interface {
	CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error)
}

// OrgService dispatches org tool calls to the appropriate orgdb query.
// The DB can be swapped at runtime via SetDB (e.g., after re-hydration).
type OrgService struct {
	db     *orgdb.DB
	bridge BridgeCaller
	mu     sync.RWMutex
}

// New creates an OrgService backed by the given org database.
func New(db *orgdb.DB) *OrgService {
	return &OrgService{db: db}
}

// SetBridge sets the bridge caller used for cross-repo code search fan-out.
func (s *OrgService) SetBridge(b BridgeCaller) {
	s.mu.Lock()
	s.bridge = b
	s.mu.Unlock()
}

func (s *OrgService) getBridge() BridgeCaller {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bridge
}

// SetDB atomically swaps the underlying database (used after re-hydration).
func (s *OrgService) SetDB(db *orgdb.DB) {
	s.mu.Lock()
	s.db = db
	s.mu.Unlock()
}

func (s *OrgService) getDB() *orgdb.DB {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db
}

// Definitions returns the MCP tool definitions for all org tools.
func (s *OrgService) Definitions() []discovery.ToolDefinition {
	return []discovery.ToolDefinition{
		{
			Name:        "org_dependency_graph",
			Description: "Show which repos depend on a package or repo, and what depends on them.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"package_scope": map[string]interface{}{"type": "string", "description": "Package scope, e.g. @platform-core"},
					"package_name":  map[string]interface{}{"type": "string", "description": "Package name, e.g. base-service"},
				},
				"required": []string{"package_scope", "package_name"},
			},
		},
		{
			Name:        "org_blast_radius",
			Description: "Compute cross-repo blast radius for a change in a repo.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"repo": map[string]interface{}{"type": "string", "description": "Repository name"},
				},
				"required": []string{"repo"},
			},
		},
		{
			Name:        "org_trace_flow",
			Description: "Trace end-to-end flow across services via API contracts and event contracts.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"trigger":   map[string]interface{}{"type": "string", "description": "Starting repo name"},
					"direction": map[string]interface{}{"type": "string", "enum": []string{"downstream", "upstream"}, "default": "downstream"},
					"max_hops":  map[string]interface{}{"type": "integer", "default": 3, "maximum": 4},
				},
				"required": []string{"trigger"},
			},
		},
		{
			Name:        "org_team_topology",
			Description: "Show team ownership, repos, and inter-team dependencies.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"team": map[string]interface{}{"type": "string", "description": "Team name"},
				},
				"required": []string{"team"},
			},
		},
		{
			Name:        "org_search",
			Description: "Search repos across the org by name, team, or type.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"query": map[string]interface{}{"type": "string", "description": "Search query"},
					"scope": map[string]interface{}{"type": "string", "enum": []string{"all", "service", "frontend", "worker", "library", "tests", "other"}, "default": "all"},
					"team":  map[string]interface{}{"type": "string", "description": "Filter by team"},
					"limit": map[string]interface{}{"type": "integer", "default": 10},
				},
				"required": []string{"query"},
			},
		},
		{
			Name:        "org_code_search",
			Description: "Search code across ALL indexed repos in the org. Fans out search_code to the top repos by size. Use this instead of search_code when you need cross-repo results.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"pattern":          map[string]interface{}{"type": "string", "description": "Code pattern to search for (e.g. 'Controller', 'handlePayment'). Leading @ is stripped automatically."},
					"max_repos":        map[string]interface{}{"type": "integer", "default": 20, "description": "Max repos to search (top N by size). Default 20."},
					"case_insensitive": map[string]interface{}{"type": "boolean", "default": true, "description": "Case-insensitive matching. Default true for cross-repo search."},
				},
				"required": []string{"pattern"},
			},
		},
	}
}

// CallTool routes a tool call to the appropriate handler.
func (s *OrgService) CallTool(ctx context.Context, name string, args map[string]interface{}) (interface{}, error) {
	switch name {
	case "org_dependency_graph":
		return s.dependencyGraph(args)
	case "org_blast_radius":
		return s.blastRadius(args)
	case "org_trace_flow":
		return s.traceFlow(args)
	case "org_team_topology":
		return s.teamTopology(args)
	case "org_search":
		return s.search(args)
	case "org_code_search":
		return s.codeSearch(ctx, args)
	default:
		return nil, fmt.Errorf("unknown org tool: %s", name)
	}
}

// IsOrgTool returns true if the tool name is handled by this service.
func (s *OrgService) IsOrgTool(name string) bool {
	switch name {
	case "org_dependency_graph", "org_blast_radius", "org_trace_flow", "org_team_topology", "org_search", "org_code_search":
		return true
	}
	return false
}

// NormalizePattern strips a leading '@' from decorator patterns and optionally
// lowercases the pattern for case-insensitive matching.
// Exported so it can be reused by the bridge handler for regular search_code.
func NormalizePattern(pattern string, caseInsensitive bool) string {
	pattern = strings.TrimPrefix(pattern, "@")
	if caseInsensitive {
		pattern = strings.ToLower(pattern)
	}
	return pattern
}

// ---------- handlers ----------

func (s *OrgService) dependencyGraph(args map[string]interface{}) (interface{}, error) {
	scope, _ := args["package_scope"].(string)
	name, _ := args["package_name"].(string)
	if scope == "" || name == "" {
		return nil, fmt.Errorf("package_scope and package_name are required")
	}
	return s.getDB().QueryDependents(scope, name)
}

func (s *OrgService) blastRadius(args map[string]interface{}) (interface{}, error) {
	repo, _ := args["repo"].(string)
	if repo == "" {
		return nil, fmt.Errorf("repo is required")
	}
	return s.getDB().QueryBlastRadius(repo)
}

func (s *OrgService) traceFlow(args map[string]interface{}) (interface{}, error) {
	trigger, _ := args["trigger"].(string)
	direction, _ := args["direction"].(string)
	maxHops := 3
	if mh, ok := args["max_hops"].(float64); ok {
		maxHops = int(mh)
	}
	if direction == "" {
		direction = "downstream"
	}
	if trigger == "" {
		return nil, fmt.Errorf("trigger is required")
	}
	return s.getDB().TraceFlow(trigger, direction, maxHops)
}

func (s *OrgService) teamTopology(args map[string]interface{}) (interface{}, error) {
	team, _ := args["team"].(string)
	if team == "" {
		return nil, fmt.Errorf("team is required")
	}
	return s.getDB().TeamTopology(team)
}

func (s *OrgService) search(args map[string]interface{}) (interface{}, error) {
	query, _ := args["query"].(string)
	scope, _ := args["scope"].(string)
	team, _ := args["team"].(string)
	limit := 10
	if l, ok := args["limit"].(float64); ok {
		limit = int(l)
	}
	if scope == "" {
		scope = "all"
	}
	if query == "" {
		return nil, fmt.Errorf("query is required")
	}
	return s.getDB().SearchRepos(query, scope, team, limit)
}

// CodeSearchResult holds aggregated search results from one repo.
type CodeSearchResult struct {
	Project string `json:"project"`
	Content string `json:"content"`
	IsError bool   `json:"is_error,omitempty"`
}

// codeSearch fans out search_code calls to the top repos by node count.
func (s *OrgService) codeSearch(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	pattern, _ := args["pattern"].(string)
	if pattern == "" {
		return nil, fmt.Errorf("pattern is required")
	}

	maxRepos := 20
	if mr, ok := args["max_repos"].(float64); ok && int(mr) > 0 {
		maxRepos = int(mr)
	}
	if maxRepos > 50 {
		maxRepos = 50
	}

	// Default case_insensitive to true for cross-repo search
	caseInsensitive := true
	if ci, ok := args["case_insensitive"].(bool); ok {
		caseInsensitive = ci
	}

	// Normalize: strip @ prefix, optionally lowercase
	pattern = NormalizePattern(pattern, caseInsensitive)

	bridge := s.getBridge()
	if bridge == nil {
		return nil, fmt.Errorf("org_code_search: bridge not configured")
	}

	// Get top repos by node count from org.db
	repos, err := s.getDB().TopReposByNodeCount(maxRepos)
	if err != nil {
		return nil, fmt.Errorf("org_code_search: list repos: %w", err)
	}
	if len(repos) == 0 {
		return []CodeSearchResult{}, nil
	}

	// Fan out with concurrency limit of 4
	const maxConcurrency = 4
	sem := make(chan struct{}, maxConcurrency)
	var mu sync.Mutex
	var results []CodeSearchResult

	var wg sync.WaitGroup
	for _, repo := range repos {
		wg.Add(1)
		go func(project string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			toolResult, callErr := bridge.CallTool(ctx, "search_code", map[string]interface{}{
				"project": project,
				"pattern": pattern,
			})

			mu.Lock()
			defer mu.Unlock()

			if callErr != nil {
				// Don't fail the whole search; record the error for this repo
				results = append(results, CodeSearchResult{
					Project: project,
					Content: fmt.Sprintf("error: %v", callErr),
					IsError: true,
				})
				return
			}

			// Extract text content from tool result
			if toolResult != nil {
				for _, c := range toolResult.Content {
					if c.Text != "" && c.Text != "No results found." {
						results = append(results, CodeSearchResult{
							Project: project,
							Content: c.Text,
						})
					}
				}
			}
		}(repo)
	}
	wg.Wait()

	// Sort: successful results first (by project name), errors last
	sort.Slice(results, func(i, j int) bool {
		if results[i].IsError != results[j].IsError {
			return !results[i].IsError
		}
		return results[i].Project < results[j].Project
	})

	return results, nil
}
