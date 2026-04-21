// Package orgtools provides MCP tool handlers for org-level intelligence queries.
package orgtools

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
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
	db       *orgdb.DB
	bridge   BridgeCaller
	cacheDir string // CBM cache dir where .db files live
	mu       sync.RWMutex
}

// New creates an OrgService backed by the given org database.
func New(db *orgdb.DB) *OrgService {
	return &OrgService{db: db}
}

// SetCacheDir sets the directory where per-project .db files are stored.
func (s *OrgService) SetCacheDir(dir string) {
	s.mu.Lock()
	s.cacheDir = dir
	s.mu.Unlock()
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

// FTSMatch holds a single FTS5 match from a per-project .db file.
type FTSMatch struct {
	Name          string `json:"name"`
	QualifiedName string `json:"qualified_name"`
	Label         string `json:"label"`
	FilePath      string `json:"file_path"`
}

// codeSearch queries per-project FTS5 indexes directly via SQL.
// This is orders of magnitude faster than grep fan-out: <1s vs 2-5min.
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

	limitPerRepo := 10
	if lpr, ok := args["limit"].(float64); ok && int(lpr) > 0 {
		limitPerRepo = int(lpr)
		if limitPerRepo > 50 {
			limitPerRepo = 50
		}
	}

	s.mu.RLock()
	cacheDir := s.cacheDir
	s.mu.RUnlock()

	if cacheDir == "" {
		return nil, fmt.Errorf("org_code_search: cache dir not configured")
	}

	// Get top repos by node count from org.db
	repos, err := s.getDB().TopReposByNodeCount(maxRepos)
	if err != nil {
		return nil, fmt.Errorf("org_code_search: list repos: %w", err)
	}
	if len(repos) == 0 {
		return []CodeSearchResult{}, nil
	}

	slog.Info("org_code_search: query", "repos", len(repos), "pattern", pattern)

	// Query each project concurrently. FTS5 first (fast), LIKE fallback for
	// camelCase patterns that FTS5's unicode61 tokenizer splits apart.
	const maxConcurrency = 20
	sem := make(chan struct{}, maxConcurrency)
	var mu sync.Mutex
	// Initialize as empty slice (not nil) so JSON marshals as [] instead of null
	// when no repos match.
	results := []CodeSearchResult{}

	var wg sync.WaitGroup
	for _, repo := range repos {
		wg.Add(1)
		go func(repoName string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			projectName := "data-fleet-cache-repos-" + repoName
			dbPath := filepath.Join(cacheDir, projectName+".db")

			// Try FTS5 first (fast — inverted index lookup).
			matches, queryErr := queryFTS5(ctx, dbPath, projectName, pattern, limitPerRepo)
			if queryErr != nil {
				slog.Debug("org_code_search: FTS5 error, trying LIKE", "repo", repoName, "err", queryErr)
			}

			// Fallback: if FTS5 returns nothing, try substring LIKE on nodes
			// table. This catches camelCase identifiers like "InternalRequest"
			// that FTS5's unicode61 tokenizer splits into separate tokens.
			if len(matches) == 0 {
				matches, queryErr = queryLike(ctx, dbPath, projectName, pattern, limitPerRepo)
				if queryErr != nil {
					slog.Debug("org_code_search: LIKE error", "repo", repoName, "err", queryErr)
					return
				}
			}
			if len(matches) == 0 {
				return
			}

			mu.Lock()
			defer mu.Unlock()

			matchJSON, _ := json.Marshal(map[string]interface{}{
				"repo":    repoName,
				"matches": matches,
				"count":   len(matches),
			})
			results = append(results, CodeSearchResult{
				Project: repoName,
				Content: string(matchJSON),
			})
		}(repo)
	}
	wg.Wait()

	sort.Slice(results, func(i, j int) bool {
		return results[i].Project < results[j].Project
	})

	slog.Info("org_code_search: complete", "repos_searched", len(repos), "repos_with_matches", len(results))
	return results, nil
}

// queryFTS5 opens a per-project .db and queries its nodes_fts index.
// Works well for whole-word queries that match FTS5 token boundaries.
func queryFTS5(ctx context.Context, dbPath, project, pattern string, limit int) ([]FTSMatch, error) {
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(2000)&mode=ro")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx,
		`SELECT name, qualified_name, label, file_path
		 FROM nodes_fts WHERE nodes_fts MATCH ? LIMIT ?`,
		pattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var matches []FTSMatch
	for rows.Next() {
		var m FTSMatch
		if err := rows.Scan(&m.Name, &m.QualifiedName, &m.Label, &m.FilePath); err != nil {
			continue
		}
		matches = append(matches, m)
	}
	return matches, rows.Err()
}

// queryLike falls back to substring matching on the nodes table.
// Catches camelCase identifiers that FTS5 tokenizes into separate tokens
// (e.g., "InternalRequest" indexed as "Internal"+"Request").
// Slower than FTS5 but always correct for substring semantics.
func queryLike(ctx context.Context, dbPath, project, pattern string, limit int) ([]FTSMatch, error) {
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(2000)&mode=ro")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	like := "%" + pattern + "%"
	rows, err := db.QueryContext(ctx,
		`SELECT name, qualified_name, label, file_path
		 FROM nodes
		 WHERE (name LIKE ? OR qualified_name LIKE ? OR file_path LIKE ?)
		 LIMIT ?`,
		like, like, like, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var matches []FTSMatch
	for rows.Next() {
		var m FTSMatch
		if err := rows.Scan(&m.Name, &m.QualifiedName, &m.Label, &m.FilePath); err != nil {
			continue
		}
		matches = append(matches, m)
	}
	return matches, rows.Err()
}
