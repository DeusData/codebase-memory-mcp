// Package orgtools provides MCP tool handlers for org-level intelligence queries.
package orgtools

import (
	"context"
	"fmt"
	"sync"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/discovery"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdb"
)

// OrgService dispatches org tool calls to the appropriate orgdb query.
// The DB can be swapped at runtime via SetDB (e.g., after re-hydration).
type OrgService struct {
	db *orgdb.DB
	mu sync.RWMutex
}

// New creates an OrgService backed by the given org database.
func New(db *orgdb.DB) *OrgService {
	return &OrgService{db: db}
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

// Definitions returns the MCP tool definitions for all 5 org tools.
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
	default:
		return nil, fmt.Errorf("unknown org tool: %s", name)
	}
}

// IsOrgTool returns true if the tool name is handled by this service.
func (s *OrgService) IsOrgTool(name string) bool {
	switch name {
	case "org_dependency_graph", "org_blast_radius", "org_trace_flow", "org_team_topology", "org_search":
		return true
	}
	return false
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
