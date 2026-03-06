package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/pipeline"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/DeusData/codebase-memory-mcp/internal/watcher"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Version is the current release version, referenced by MCP handshake and update checker.
const Version = "0.4.3"

// releaseURL is the GitHub API endpoint for latest release. Package-level var for test injection.
var releaseURL = "https://api.github.com/repos/DeusData/codebase-memory-mcp/releases/latest"

// Server wraps the MCP server with tool handlers.
type Server struct {
	mcp      *mcp.Server
	router   *store.StoreRouter
	watcher  *watcher.Watcher
	indexMu  sync.Mutex
	handlers map[string]mcp.ToolHandler

	// Session-aware fields (set once via sync.Once, then immutable)
	sessionOnce    sync.Once
	sessionRoot    string // absolute path from client
	sessionProject string // derived from sessionRoot via ProjectNameFromPath
	indexStatus    atomic.Value
	indexStartedAt atomic.Value // time.Time — when current/last index started
	updateNotice   atomic.Value // string — set once by checkForUpdate, cleared after first injection
}

// NewServer creates a new MCP server with all tools registered.
func NewServer(r *store.StoreRouter) *Server {
	srv := &Server{
		router:   r,
		handlers: make(map[string]mcp.ToolHandler),
	}

	srv.mcp = mcp.NewServer(
		&mcp.Implementation{
			Name:    "codebase-memory-mcp",
			Version: Version,
		},
		&mcp.ServerOptions{
			InitializedHandler:      srv.onInitialized,
			RootsListChangedHandler: srv.onRootsChanged,
		},
	)

	srv.registerTools()
	srv.watcher = watcher.New(r, srv.syncProject)
	return srv
}

// StartWatcher launches the background file-change polling goroutine.
// It stops when ctx is cancelled.
func (s *Server) StartWatcher(ctx context.Context) {
	go s.watcher.Run(ctx)
}

// syncProject is called by the watcher when file changes are detected.
// Uses TryLock to skip if an index operation is already in progress.
func (s *Server) syncProject(ctx context.Context, projectName, rootPath string) error {
	if !s.indexMu.TryLock() {
		slog.Debug("watcher.skip", "path", rootPath, "reason", "index_in_progress")
		return nil
	}
	defer s.indexMu.Unlock()
	st, err := s.router.ForProject(projectName)
	if err != nil {
		return fmt.Errorf("store for %s: %w", projectName, err)
	}
	p := pipeline.New(ctx, st, rootPath, discover.ModeFull)
	return p.Run()
}

// MCPServer returns the underlying MCP server.
func (s *Server) MCPServer() *mcp.Server {
	return s.mcp
}

// Router returns the underlying StoreRouter for direct access (e.g. CLI mode).
func (s *Server) Router() *store.StoreRouter {
	return s.router
}

// SessionProject returns the auto-detected session project name (may be empty).
func (s *Server) SessionProject() string {
	return s.sessionProject
}

// SetSessionRoot sets the session root path directly (for CLI mode).
func (s *Server) SetSessionRoot(rootPath string) {
	go s.checkForUpdate()
	s.sessionOnce.Do(func() {
		s.sessionRoot = rootPath
		if rootPath != "" {
			s.sessionProject = pipeline.ProjectNameFromPath(rootPath)
		}
	})
}

// --- Session detection ---

// onInitialized is called when the client sends notifications/initialized.
func (s *Server) onInitialized(ctx context.Context, req *mcp.InitializedRequest) {
	go s.checkForUpdate()
	s.sessionOnce.Do(func() {
		s.sessionRoot = s.detectSessionRoot(ctx, req.Session)
		if s.sessionRoot != "" {
			s.sessionProject = pipeline.ProjectNameFromPath(s.sessionRoot)
			s.startAutoIndex()
		}
	})
}

// onRootsChanged re-detects session root if not yet set.
func (s *Server) onRootsChanged(ctx context.Context, req *mcp.RootsListChangedRequest) {
	go s.checkForUpdate()
	s.sessionOnce.Do(func() {
		s.sessionRoot = s.detectSessionRoot(ctx, req.Session)
		if s.sessionRoot != "" {
			s.sessionProject = pipeline.ProjectNameFromPath(s.sessionRoot)
			s.startAutoIndex()
		}
	})
}

// detectSessionRoot tries multiple fallback strategies to find the project root.
func (s *Server) detectSessionRoot(ctx context.Context, session *mcp.ServerSession) string {
	// 1. Try MCP roots protocol
	if session != nil {
		result, err := session.ListRoots(ctx, nil)
		if err == nil && len(result.Roots) > 0 {
			uri := result.Roots[0].URI
			if path, ok := parseFileURI(uri); ok {
				slog.Info("session.root.from_roots", "path", path)
				return path
			}
		}
	}

	// 2. Fall back to process working directory
	if cwd, err := os.Getwd(); err == nil && cwd != "/" && cwd != os.Getenv("HOME") {
		slog.Info("session.root.from_cwd", "path", cwd)
		return cwd
	}

	// 3. Fall back to single indexed project from DB
	projects, err := s.router.ListProjects()
	if err == nil && len(projects) == 1 && projects[0].RootPath != "" {
		slog.Info("session.root.from_db", "path", projects[0].RootPath)
		return projects[0].RootPath
	}

	slog.Info("session.root.none", "reason", "no_roots_no_cwd_no_single_project")
	return ""
}

// parseFileURI extracts a filesystem path from a file:// URI.
func parseFileURI(uri string) (string, bool) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "file" {
		return "", false
	}
	path := u.Path
	// On Windows, file:///C:/path parses to /C:/path — strip leading / before drive letter
	if len(path) >= 3 && path[0] == '/' && path[2] == ':' {
		path = path[1:]
	}
	return filepath.FromSlash(path), true
}

// startAutoIndex triggers background indexing for the session project.
func (s *Server) startAutoIndex() {
	hasDB := s.router.HasProject(s.sessionProject)

	if !hasDB {
		s.indexStatus.Store("indexing")
	} else {
		s.indexStatus.Store("ready")
	}

	go func() {
		if !s.indexMu.TryLock() {
			slog.Debug("autoindex.skip", "reason", "index_in_progress")
			return
		}
		defer s.indexMu.Unlock()

		s.indexStartedAt.Store(time.Now())
		if !hasDB {
			s.indexStatus.Store("indexing")
		}

		st, err := s.router.ForProject(s.sessionProject)
		if err != nil {
			slog.Warn("autoindex.store.err", "err", err)
			return
		}
		p := pipeline.New(context.Background(), st, s.sessionRoot, discover.ModeFull)
		if err := p.Run(); err != nil {
			slog.Warn("autoindex.err", "err", err)
			return
		}
		s.indexStatus.Store("ready")
		slog.Info("autoindex.done", "project", s.sessionProject)
	}()
}

// --- Store routing ---

// resolveStore returns the Store for the given project, or the session project if empty.
func (s *Server) resolveStore(project string) (*store.Store, error) {
	if project == "*" || project == "all" {
		return nil, fmt.Errorf("cross-project queries are not supported; use list_projects to find a specific project name, or omit the project parameter to use the current session project")
	}
	if project == "" {
		project = s.sessionProject
	}
	if project == "" {
		return nil, fmt.Errorf("no project specified and no session project detected; pass project parameter")
	}
	if !s.router.HasProject(project) {
		return nil, fmt.Errorf("project %q not found; use list_projects to see available projects", project)
	}
	return s.router.ForProject(project)
}

// resolveProjectName returns the effective project name for routing.
func (s *Server) resolveProjectName(project string) string {
	if project == "" {
		return s.sessionProject
	}
	return project
}

// addIndexStatus adds the index_status field to response data if indexing is in progress.
func (s *Server) addIndexStatus(data map[string]any) {
	status, _ := s.indexStatus.Load().(string)
	if status == "indexing" {
		data["index_status"] = "indexing"
	}
}

// addUpdateNotice prepends an update notice to the first tool response, then clears itself.
func (s *Server) addUpdateNotice(result *mcp.CallToolResult) {
	if notice, ok := s.updateNotice.Load().(string); ok && notice != "" {
		result.Content = append([]mcp.Content{&mcp.TextContent{Text: notice}}, result.Content...)
		s.updateNotice.Store("")
	}
}

// checkForUpdate fetches the latest GitHub release and stores a notice if newer.
func (s *Server) checkForUpdate() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", releaseURL, http.NoBody)
	if err != nil {
		slog.Warn("update check: request create failed", "err", err)
		return
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Warn("update check: http failed", "err", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		slog.Warn("update check: bad status", "status", resp.StatusCode)
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		slog.Warn("update check: body read failed", "err", err)
		return
	}

	var release struct {
		TagName string `json:"tag_name"`
	}
	if err := json.Unmarshal(body, &release); err != nil {
		slog.Warn("update check: json parse failed", "err", err)
		return
	}

	latest := strings.TrimPrefix(release.TagName, "v")
	if latest == "" || latest == Version {
		slog.Debug("update check: current", "version", Version, "latest", latest)
		return
	}
	if compareVersions(latest, Version) > 0 {
		notice := fmt.Sprintf(
			"⚡ Update available: v%s → v%s — run: codebase-memory-mcp update",
			Version, latest)
		s.updateNotice.Store(notice)
		slog.Info("update available", "current", Version, "latest", latest)
	}
}

// compareVersions compares two semver strings (e.g. "0.2.1" vs "0.2.0").
// Returns >0 if a > b, <0 if a < b, 0 if equal.
func compareVersions(a, b string) int {
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")
	for i := 0; i < len(aParts) && i < len(bParts); i++ {
		ai, _ := strconv.Atoi(aParts[i])
		bi, _ := strconv.Atoi(bParts[i])
		if ai != bi {
			return ai - bi
		}
	}
	return len(aParts) - len(bParts)
}

// --- Tool registration ---

func (s *Server) addTool(tool *mcp.Tool, handler mcp.ToolHandler) {
	s.mcp.AddTool(tool, handler)
	s.handlers[tool.Name] = handler
}

// CallTool invokes a tool handler directly by name, bypassing MCP transport.
func (s *Server) CallTool(ctx context.Context, name string, argsJSON json.RawMessage) (*mcp.CallToolResult, error) {
	handler, ok := s.handlers[name]
	if !ok {
		return nil, fmt.Errorf("unknown tool: %s", name)
	}
	if len(argsJSON) == 0 {
		argsJSON = json.RawMessage(`{}`)
	}
	req := &mcp.CallToolRequest{
		Params: &mcp.CallToolParamsRaw{
			Name:      name,
			Arguments: argsJSON,
		},
	}
	return handler(ctx, req)
}

// ToolNames returns all registered tool names in sorted order.
func (s *Server) ToolNames() []string {
	names := make([]string, 0, len(s.handlers))
	for name := range s.handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *Server) registerTools() {
	s.registerGraphTools()
	s.registerProjectTools()
	s.registerTraceTools()
	s.registerDetectChanges()
	s.registerArchitectureTools()
}

func (s *Server) registerArchitectureTools() {
	s.addTool(&mcp.Tool{
		Name:        "get_architecture",
		Description: "Get codebase architecture overview. Returns structural analysis computed from the code graph. Call with aspects=['all'] for full orientation or select specific aspects for targeted queries. Available aspects: languages (language breakdown), packages (top packages with fan-in/out), entry_points (main/init functions), routes (HTTP endpoints with handlers), hotspots (most-called functions), boundaries (cross-package call volumes), services (cross-service HTTP/async links), layers (package-level layer classification — heuristic), clusters (community detection via Louvain algorithm on CALLS + HTTP_CALLS + ASYNC_CALLS — reveals hidden functional modules across packages and services), file_tree (condensed directory structure), adr (stored Architecture Decision Record — use manage_adr to create/update). Recommended: call this first when exploring an unfamiliar codebase.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"aspects": {
					"type": "array",
					"items": {"type": "string", "enum": ["all", "languages", "packages", "entry_points", "routes", "hotspots", "boundaries", "services", "layers", "clusters", "file_tree", "adr"]},
					"description": "Which architecture aspects to return. Default: ['all']. Use specific aspects to reduce output: ['languages', 'packages'] for quick orientation, ['hotspots', 'boundaries'] for dependency analysis, ['clusters'] for community detection across CALLS/HTTP/ASYNC edges."
				},
				"project": {
					"type": "string",
					"description": "Project to analyze. Defaults to session project."
				}
			}
		}`),
	}, s.handleGetArchitecture)

	s.addTool(&mcp.Tool{
		Name:        "manage_adr",
		Description: "Manage the Architecture Decision Record (ADR) for a project. CRUD operations for a persistent, section-based architectural summary. Modes: get (retrieve full ADR or specific sections via include filter), store (create or fully replace — all 6 sections required), update (patch specific sections — only canonical keys accepted, unmentioned sections preserved), delete (remove ADR). Fixed sections in canonical order: PURPOSE, STACK, ARCHITECTURE, PATTERNS, TRADEOFFS, PHILOSOPHY. Max 8000 chars total. Validation: store rejects content missing any section; update rejects non-canonical keys. Use include=['STACK','PATTERNS'] with get to fetch only needed sections (saves tokens). PLAN ALIGNMENT: Before finalizing any implementation plan, fetch the ADR and validate the plan against it — check ARCHITECTURE for structural fit, PATTERNS for convention compliance, STACK for technology alignment, PHILOSOPHY for principle adherence. Flag conflicts before proceeding. For creating/replacing: explore the codebase first using get_architecture and graph tools, then enter plan mode to draft the ADR collaboratively with the user. Store only after user approval.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"mode": {
					"type": "string",
					"enum": ["get", "store", "update", "delete"],
					"description": "Operation: 'get' retrieves ADR, 'store' creates/replaces (all 6 sections required), 'update' patches sections (canonical keys only), 'delete' removes."
				},
				"project": {
					"type": "string",
					"description": "Project name. Defaults to session project."
				},
				"content": {
					"type": "string",
					"description": "Full ADR markdown (required for mode='store'). Must contain all 6 ## SECTION headers: PURPOSE, STACK, ARCHITECTURE, PATTERNS, TRADEOFFS, PHILOSOPHY. Missing sections will be rejected."
				},
				"sections": {
					"type": "object",
					"additionalProperties": {"type": "string"},
					"description": "Section updates (required for mode='update'). Keys must be canonical section names (PURPOSE, STACK, ARCHITECTURE, PATTERNS, TRADEOFFS, PHILOSOPHY). Non-canonical keys are rejected. Values are new content. Unmentioned sections preserved."
				},
				"include": {
					"type": "array",
					"items": {"type": "string", "enum": ["PURPOSE", "STACK", "ARCHITECTURE", "PATTERNS", "TRADEOFFS", "PHILOSOPHY"]},
					"description": "Section filter for mode='get'. Returns only the listed sections instead of the full ADR. Example: ['STACK', 'PATTERNS'] returns ~800 chars instead of ~8000. Omit to get all sections."
				}
			},
			"required": ["mode"]
		}`),
	}, s.handleManageADR)
}

// registerGraphTools registers tools for graph querying, searching, and tracing.
func (s *Server) registerGraphTools() {
	s.registerIndexAndTraceTool()
	s.registerSchemaAndSnippetTools()
	s.registerSearchTools()
	s.registerQueryTool()
}

func (s *Server) registerIndexAndTraceTool() {
	s.addTool(&mcp.Tool{
		Name:        "index_repository",
		Description: "Index a repository into the code graph. Parses source files, extracts functions/classes/modules, resolves call relationships (CALLS), read references (USAGE), interface implementations (IMPLEMENTS + OVERRIDE), HTTP/async cross-service links, and git history change coupling (FILE_CHANGES_WITH). Supports incremental reindex via content hashing. Auto-sync keeps the graph fresh after initial indexing. If repo_path is omitted, uses the auto-detected session project root. Use mode='fast' for large repos (>50K files) — skips generated code, test fixtures, large files (>512KB), and non-source files for 30-50% faster indexing at the cost of some coverage.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"repo_path": {
					"type": "string",
					"description": "Absolute path to the repository to index. If omitted, uses the auto-detected session project root."
				},
				"mode": {
					"type": "string",
					"enum": ["full", "fast"],
					"description": "Indexing mode. 'full' (default): parse all supported files. 'fast': aggressive filtering — skips generated code, test fixtures, docs, large files (>512KB), and non-source assets for faster indexing of large repos."
				}
			}
		}`),
	}, s.handleIndexRepository)

	s.addTool(&mcp.Tool{
		Name:        "trace_call_path",
		Description: "Trace the call path of a function (who calls it, what it calls). Requires exact function name — use search_graph first to find the exact name. Follow up with get_code_snippet to read the actual source code. Returns hop-by-hop callees/callers with edge types (CALLS, HTTP_CALLS, ASYNC_CALLS, USAGE, OVERRIDE). If the function is not found, returns suggestions of similar names — use the qualified_name from suggestions in a retry. Use depth=1 first, increase only if needed. Use direction='both' for full cross-service context — HTTP_CALLS edges from other services appear as inbound edges, so direction='outbound' alone misses cross-service callers. Best practice: search_graph(name_pattern='.*Order.*') → trace_call_path(function_name='processOrder') → get_code_snippet(qualified_name='...')",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"function_name": {
					"type": "string",
					"description": "Name of the function to trace (e.g. 'ProcessOrder')"
				},
				"depth": {
					"type": "integer",
					"description": "Maximum BFS depth (1-5, default 3)"
				},
				"direction": {
					"type": "string",
					"description": "Traversal direction: 'outbound' (what it calls), 'inbound' (what calls it), or 'both'",
					"enum": ["outbound", "inbound", "both"]
				},
				"risk_labels": {
					"type": "boolean",
					"description": "Add risk classification (CRITICAL/HIGH/MEDIUM/LOW) based on hop depth. Hop 1=CRITICAL, 2=HIGH, 3=MEDIUM, 4+=LOW. Includes impact_summary with counts. Default false."
				},
				"min_confidence": {
					"type": "number",
					"description": "Minimum confidence threshold (0.0-1.0) for CALLS edges. Filters out low-confidence fuzzy matches. Bands: high (>=0.7), medium (>=0.45), speculative (<0.45). Default 0 (no filter)."
				},
				"project": {
					"type": "string",
					"description": "Project to trace in. Defaults to session project."
				}
			},
			"required": ["function_name"]
		}`),
	}, s.handleTraceCallPath)
}

func (s *Server) registerSchemaAndSnippetTools() {
	s.addTool(&mcp.Tool{
		Name:        "get_graph_schema",
		Description: "Return the schema of the indexed code graph: node label counts, edge type counts, relationship patterns (e.g. Function-CALLS->Function), and sample function/class names. Use to understand what's in the graph before querying.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"project": {
					"type": "string",
					"description": "Project to get schema for. Defaults to session project."
				}
			}
		}`),
	}, s.handleGetGraphSchema)

	s.addTool(&mcp.Tool{
		Name:        "get_code_snippet",
		Description: "Retrieve source code for a function/class by name with rich metadata. Accepts exact qualified name ('myproject.cmd.server.main.HandleRequest'), partial QN suffix ('main.HandleRequest'), or short name ('HandleRequest'). Returns source code, signature, return type, complexity, decorators, docstring, and caller/callee counts. Returns suggestions with status='ambiguous' when multiple matches found (no 'error' key in disambiguation responses). Use auto_resolve=true to let the tool pick the best match from <=2 candidates (returns alternatives for correction). Use include_neighbors=true to get caller/callee names alongside counts.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"qualified_name": {
					"type": "string",
					"description": "Name or qualified name of the function/class. Exact QN for precision, short name for discovery. Returns suggestions if ambiguous."
				},
				"project": {
					"type": "string",
					"description": "Project to search in. Defaults to session project."
				},
				"auto_resolve": {
					"type": "boolean",
					"description": "When true and <=2 ambiguous candidates exist, auto-pick the best match (highest degree, prefer non-test). Returns source with match_method='auto_best' and alternatives list. Default: false."
				},
				"include_neighbors": {
					"type": "boolean",
					"description": "When true, include caller_names and callee_names arrays (up to 10 each) alongside the counts. Default: false."
				}
			},
			"required": ["qualified_name"]
		}`),
	}, s.handleGetCodeSnippet)
}

func (s *Server) registerSearchTools() {
	s.addTool(&mcp.Tool{
		Name:        "search_graph",
		Description: "Search the code knowledge graph for functions, classes, modules, routes, and other code elements. Search is case-insensitive by default (set case_sensitive=true for exact case). Best practice: use regex alternatives for broad matching — include abbreviations ('handler|hdlr|ctrl'), word forms ('sponsor|sponsoring|sponsored'), and synonyms ('delete|remove|drop'). One broad regex replaces multiple narrow searches. Returns nodes matching the criteria with their connectivity (in/out degree). Results are sorted by relevance by default (exact match first, prefix match second, then by connectivity). Community nodes are excluded by default. Pass exclude_labels: [] to include them. Best practice: Chain with trace_call_path and get_code_snippet for complete answers. Example workflow: search_graph(name_pattern='.*Order.*') → trace_call_path(function_name='processOrder') → get_code_snippet(qualified_name='...'). Returns 10 results per page (use offset to paginate, has_more indicates more pages). name_pattern and qn_pattern support full Go regex — one precise regex replaces multiple literal searches. See parameter descriptions for patterns. For dead code: use relationship='CALLS', direction='inbound', max_degree=0, exclude_entry_points=true. For fan-out: use relationship='CALLS', direction='outbound', min_degree=N. Route nodes: properties.handler contains the actual handler function name. Prefer this over query_graph for counting — no row cap. IMPORTANT: The 'relationship' filter counts how many edges of that type each node has (degree filtering) — it does NOT return the actual edges. To list cross-service HTTP_CALLS or ASYNC_CALLS edges with their properties, use query_graph with Cypher instead. Relationship types: CALLS, HTTP_CALLS, ASYNC_CALLS, IMPORTS, DEFINES, DEFINES_METHOD, HANDLES, CONTAINS_FILE, CONTAINS_FOLDER, CONTAINS_PACKAGE, IMPLEMENTS, OVERRIDE, USAGE, FILE_CHANGES_WITH.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"project": {
					"type": "string",
					"description": "Project to search in. Defaults to session project."
				},
				"label": {
					"type": "string",
					"description": "Node label filter: Function, Class, Module, Method, Interface, Enum, Type, File, Package, Folder, Route"
				},
				"name_pattern": {
					"type": "string",
					"description": "Regex pattern matched against the short node name. Case-insensitive by default. Supports full Go regex: '.*Handler$' (suffix), 'get|set|delete' (alternatives — no backslash before pipe), '^on[A-Z]' (prefix+char class). Best practice: include word variations in alternatives — 'auth|authenticate|authorization' (word forms), 'handler|hdlr|ctrl' (abbreviations), 'create|new|init' (synonyms). One regex with | replaces multiple separate searches."
				},
				"qn_pattern": {
					"type": "string",
					"description": "Regex pattern matched against the qualified name (full module path). Case-insensitive by default. Use to scope searches to directories/modules: '.*services\\.order\\..*' (order service), '.*tests\\..*' (test files only), '.*controller.*\\.handle.*' (handler methods in controllers). Combine with name_pattern for precise cross-cutting queries."
				},
				"file_pattern": {
					"type": "string",
					"description": "Glob pattern for file path within the project. Use to filter by directory ('**/services/**'), file extension ('*.py', '*.yaml'), or filename ('**/Makefile'). Essential for shared-repo projects where multiple languages coexist — e.g., use '*.html' to find only HTML files in a JavaScript project."
				},
				"relationship": {
					"type": "string",
					"description": "Filter by relationship type: CALLS, HTTP_CALLS, ASYNC_CALLS, IMPORTS, DEFINES, DEFINES_METHOD, HANDLES, CONTAINS_FILE, CONTAINS_FOLDER, CONTAINS_PACKAGE, IMPLEMENTS"
				},
				"direction": {
					"type": "string",
					"description": "Edge direction for degree filters: 'inbound', 'outbound', or 'any'",
					"enum": ["inbound", "outbound", "any"]
				},
				"min_degree": {
					"type": "integer",
					"description": "Minimum edge count (e.g. 10 for high fan-out functions)"
				},
				"max_degree": {
					"type": "integer",
					"description": "Maximum edge count (e.g. 0 for dead code detection)"
				},
				"exclude_entry_points": {
					"type": "boolean",
					"description": "Exclude entry points (route handlers, main(), framework-registered functions) from results. Use with max_degree=0 for accurate dead code detection."
				},
				"limit": {
					"type": "integer",
					"description": "Max results per page (default: 10). Use small limits and paginate with offset — response includes has_more flag."
				},
				"offset": {
					"type": "integer",
					"description": "Skip N results for pagination (default: 0). Check has_more in response to know if more pages exist."
				},
				"include_connected": {
					"type": "boolean",
					"description": "Include connected node names in results (default: false). Expensive — only enable when you need to see neighbor names."
				},
				"exclude_labels": {
					"type": "array",
					"items": {"type": "string"},
					"description": "Labels to exclude from results. Community nodes are excluded by default — pass [] to include them."
				},
				"sort_by": {
					"type": "string",
					"enum": ["relevance", "name", "degree"],
					"description": "Sort order. Default: relevance (exact match first, prefix match second, then by connectivity)"
				},
				"case_sensitive": {
					"type": "boolean",
					"description": "Match patterns case-sensitively. Default: false (case-insensitive). Set true for exact case matching."
				}
			}
		}`),
	}, s.handleSearchGraph)

	s.addTool(&mcp.Tool{
		Name:        "search_code",
		Description: "Search for text in source code files (like grep, scoped to indexed project). Search is case-insensitive by default (set case_sensitive=true for exact case). With regex=true, use alternatives for broad matching: 'TODO|FIXME|HACK|WORKAROUND' (issue markers), 'sponsor|sponsoring|sponsored' (word forms), 'import|require|include' (cross-language patterns). Returns matching lines with file path, line number, and context. Returns 10 matches per page — use offset to paginate, has_more indicates more pages. Use for: string literals, error messages, TODO comments, config values, import statements. Prefer search_graph for finding functions/classes by name — search_code is for text content that isn't in the graph.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"pattern": {
					"type": "string",
					"description": "Text to search for. Case-insensitive by default. Literal string match unless regex=true. With regex=true: Go regex syntax (no backslash before pipe). Best practice: use alternatives for word form variance — 'deprecat|obsolete|legacy' catches 'deprecated', 'deprecating', 'obsolete', etc. A partial stem with alternatives is more effective than an exact word."
				},
				"file_pattern": {
					"type": "string",
					"description": "Glob pattern to filter files (e.g. '*.go', '*.py', '*.toml'). Use to focus search on specific file types or directories."
				},
				"regex": {
					"type": "boolean",
					"description": "Treat pattern as a regular expression (default: false)"
				},
				"max_results": {
					"type": "integer",
					"description": "Max matches per page (default: 10). Response includes has_more flag for pagination."
				},
				"offset": {
					"type": "integer",
					"description": "Skip N matches for pagination (default: 0). Check has_more in response."
				},
				"case_sensitive": {
					"type": "boolean",
					"description": "Match case-sensitively. Default: false (case-insensitive). Set true for exact case matching."
				},
				"project": {
					"type": "string",
					"description": "Project to search in. Defaults to session project."
				}
			},
			"required": ["pattern"]
		}`),
	}, s.handleSearchCode)
}

func (s *Server) registerQueryTool() {
	s.addTool(&mcp.Tool{
		Name:        "query_graph",
		Description: "Execute a Cypher-like graph query. String matching in WHERE is case-sensitive by default. For case-insensitive regex: use (?i) flag — WHERE f.name =~ '(?i)handler'. CONTAINS and STARTS WITH are always case-sensitive — use =~ with (?i) for case-insensitive substring matching: WHERE f.name =~ '(?i).*order.*'. Best practice: use regex alternatives for word form variance — WHERE f.name =~ '(?i)sponsor|sponsoring|sponsored'. Tip: prefer search_graph for simple name searches (case-insensitive by default) — use query_graph only when you need Cypher's relationship patterns or edge properties. WARNING: 200-row cap applies BEFORE aggregation — COUNT queries on large codebases silently undercount. For fan-out/fan-in counting, use search_graph with min_degree/max_degree instead. Best for: relationship patterns, filtered joins, path queries, and edge property filtering. Supports WHERE on edge properties: r.url_path CONTAINS 'orders', r.confidence >= 0.6, r.method = 'POST', r.confidence_band = 'high', r.validated_by_trace = true, r.coupling_score >= 0.5. This is the correct tool for listing cross-service edges — use MATCH (a)-[r:HTTP_CALLS]->(b) RETURN a.name, b.name, r.url_path, r.confidence, r.confidence_band to see HTTP links with URLs and confidence scores (bands: high>=0.7, medium>=0.45, speculative>=0.25), or MATCH (a)-[r:ASYNC_CALLS]->(b) for async dispatch edges. For change coupling: MATCH (a)-[r:FILE_CHANGES_WITH]->(b) RETURN a.name, b.name, r.coupling_score, r.co_change_count. For interface method overrides: MATCH (s)-[r:OVERRIDE]->(i) to find struct methods implementing interface methods. For read references (callbacks, variable assignments): MATCH (a)-[r:USAGE]->(b). Always use LIMIT. Edge types: CALLS, HTTP_CALLS, ASYNC_CALLS, IMPORTS, DEFINES, DEFINES_METHOD, HANDLES, IMPLEMENTS, OVERRIDE, USAGE, FILE_CHANGES_WITH.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"query": {
					"type": "string",
					"description": "Cypher query, e.g. MATCH (f:Function)-[:CALLS]->(g:Function) WHERE f.name = 'main' RETURN g.name, g.qualified_name LIMIT 20"
				},
				"project": {
					"type": "string",
					"description": "Project to query. Defaults to session project."
				}
			},
			"required": ["query"]
		}`),
	}, s.handleQueryGraph)
}

// registerProjectTools registers tools for project management.
func (s *Server) registerProjectTools() {
	s.addTool(&mcp.Tool{
		Name:        "list_projects",
		Description: "List all indexed projects with their indexed_at timestamp, root path, and node/edge counts.",
		InputSchema: json.RawMessage(`{"type": "object", "properties": {}}`),
	}, s.handleListProjects)

	s.addTool(&mcp.Tool{
		Name:        "delete_project",
		Description: "Delete an indexed project and all its graph data (nodes, edges, file hashes). Removes the project's .db file. This action is irreversible.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"project_name": {
					"type": "string",
					"description": "Name of the project to delete"
				}
			},
			"required": ["project_name"]
		}`),
	}, s.handleDeleteProject)

	s.addTool(&mcp.Tool{
		Name:        "index_status",
		Description: "Check the indexing status of a project. Returns whether the project is indexed, currently indexing, or not found. Shows last indexed timestamp, node/edge counts, and whether the index is initial or incremental. Use this to check if the graph is ready for queries.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"project": {
					"type": "string",
					"description": "Project name to check. Defaults to the auto-detected session project."
				}
			}
		}`),
	}, s.handleIndexStatus)
}

// --- Helpers ---

// jsonResult marshals data to JSON and returns as tool result.
func jsonResult(data any) *mcp.CallToolResult {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return errResult("json marshal err=" + err.Error())
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(b)},
		},
	}
}

// errResult returns a tool result indicating an error.
func errResult(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: msg},
		},
		IsError: true,
	}
}

// parseArgs unmarshals the raw JSON arguments into a map.
func parseArgs(req *mcp.CallToolRequest) (map[string]any, error) {
	if len(req.Params.Arguments) == 0 {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal(req.Params.Arguments, &m); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	return m, nil
}

// getStringArg extracts a string argument from parsed args.
func getStringArg(args map[string]any, key string) string {
	v, ok := args[key]
	if !ok {
		return ""
	}
	str, ok := v.(string)
	if !ok {
		return ""
	}
	return str
}

// getIntArg extracts an integer argument with a default value.
func getIntArg(args map[string]any, key string, defaultVal int) int {
	v, ok := args[key]
	if !ok {
		return defaultVal
	}
	f, ok := v.(float64) // JSON numbers decode as float64
	if !ok {
		return defaultVal
	}
	return int(f)
}

// getMapStringArg extracts a map[string]string argument from parsed args.
func getMapStringArg(args map[string]any, key string) map[string]string {
	v, ok := args[key]
	if !ok {
		return nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	result := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			result[k] = s
		}
	}
	return result
}

// getBoolArg extracts a boolean argument from parsed args.
// getFloatArg extracts a float64 argument with a default value.
func getFloatArg(args map[string]any, key string, defaultVal float64) float64 {
	v, ok := args[key]
	if !ok {
		return defaultVal
	}
	f, ok := v.(float64)
	if !ok {
		return defaultVal
	}
	return f
}

func getBoolArg(args map[string]any, key string) bool {
	v, ok := args[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	if !ok {
		return false
	}
	return b
}

// findNodeAcrossProjects searches for a node by simple name in the specified project.
// Falls back to the session project if no filter is given.
func (s *Server) findNodeAcrossProjects(name string, projectFilter ...string) (*store.Node, string, error) {
	filter := s.sessionProject
	if len(projectFilter) > 0 && projectFilter[0] != "" {
		if projectFilter[0] == "*" || projectFilter[0] == "all" {
			return nil, "", fmt.Errorf("cross-project queries are not supported; use list_projects to find a specific project name, or omit the project parameter to use the current session project")
		}
		filter = projectFilter[0]
	}
	if filter == "" {
		return nil, "", fmt.Errorf("no project specified and no session project detected")
	}
	if !s.router.HasProject(filter) {
		return nil, "", fmt.Errorf("project %q not found; use list_projects to see available projects", filter)
	}

	st, err := s.router.ForProject(filter)
	if err != nil {
		return nil, "", err
	}
	projects, _ := st.ListProjects()
	for _, p := range projects {
		nodes, findErr := st.FindNodesByName(p.Name, name)
		if findErr != nil {
			continue
		}
		if len(nodes) > 0 {
			return nodes[0], p.Name, nil
		}
	}
	return nil, "", fmt.Errorf("node not found: %s", name)
}
