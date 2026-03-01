package httplink

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// RouteHandler represents a discovered HTTP route handler.
type RouteHandler struct {
	Path              string
	Method            string
	FunctionName      string
	QualifiedName     string
	HandlerRef        string // resolved handler function reference (e.g. "h.CreateOrder")
	ResolvedHandlerQN string // set by createRegistrationCallEdges — actual handler QN
	Protocol          string // "ws", "sse", or "" (standard HTTP)
}

// HTTPCallSite represents a discovered HTTP call site.
type HTTPCallSite struct {
	Path                string
	Method              string // best-effort: "GET", "POST", etc. or "" if unknown
	SourceName          string
	SourceQualifiedName string
	SourceLabel         string // "Function", "Method", or "Module"
	IsAsync             bool   // true when source uses async dispatch keywords
}

// HTTPLink represents a matched HTTP call from caller to handler.
type HTTPLink struct {
	CallerQN    string
	CallerLabel string
	HandlerQN   string
	URLPath     string
	EdgeType    string // "HTTP_CALLS" or "ASYNC_CALLS"
}

// Linker discovers cross-service HTTP calls and creates HTTP_CALLS edges.
type Linker struct {
	store          *store.Store
	project        string
	config         *LinkerConfig
	routesByFunc   map[string][]int // funcQN → indices into routes slice
	extraCallSites []HTTPCallSite   // injected from pipeline (e.g., InfraFile URLs)
}

// New creates a new HTTP Linker.
func New(s *store.Store, project string) *Linker {
	return &Linker{store: s, project: project, config: DefaultConfig()}
}

// SetConfig sets the linker configuration. If cfg is nil, defaults are used.
func (l *Linker) SetConfig(cfg *LinkerConfig) {
	if cfg != nil {
		l.config = cfg
	}
}

// AddCallSites allows the pipeline to inject additional call sites from infra files.
func (l *Linker) AddCallSites(sites []HTTPCallSite) {
	l.extraCallSites = append(l.extraCallSites, sites...)
}

// regex patterns for route and URL discovery
var (
	// Python decorators: @app.post("/path"), @router.get(""), @router.get("/path")
	pyRouteRe = regexp.MustCompile(`@\w+\.(get|post|put|delete|patch)\(\s*["']([^"']*)["']`)

	// Go gin/chi routes: .POST("/path"), .Get("/path"), .POST("", handler)
	goRouteRe = regexp.MustCompile(`\.(GET|POST|PUT|DELETE|PATCH|Get|Post|Put|Delete|Patch)\(\s*["']([^"']*)["']`)

	// Go gin group: .Group("/prefix")
	goGroupRe = regexp.MustCompile(`(\w+)\s*(?::=|=)\s*\w+\.Group\(\s*["']([^"']+)["']`)

	// Go gin/chi route handler reference: captures the last argument (handler, not middleware)
	// .POST("/path", h.CreateOrder) or .Get("/path", handler)
	goRouteHandlerRe = regexp.MustCompile(`\.(GET|POST|PUT|DELETE|PATCH|Get|Post|Put|Delete|Patch)\s*\(\s*"[^"]*"\s*(?:,\s*[\w.]+)*,\s*([\w.]+)\s*\)`)

	// Go chi: r.Route("/prefix", func(r chi.Router) { ... })
	goChiRouteRe = regexp.MustCompile(`\.Route\(\s*"([^"]+)"\s*,\s*func`)

	// Express.js routes: captures (receiver).(method)("path") — filtered by allowlist
	expressRouteRe = regexp.MustCompile(`(\w+)\.(get|post|put|delete|patch)\(\s*["'` + "`" + `]([^"'` + "`" + `]+)["'` + "`" + `]`)

	// Express.js handler reference: captures (receiver).(method)("path", ..., handler)
	expressHandlerRe = regexp.MustCompile(`(\w+)\.(get|post|put|delete|patch)\(\s*["'` + "`" + `][^"'` + "`" + `]+["'` + "`" + `]\s*(?:,\s*[\w.]+)*,\s*([\w.]+)\s*\)`)

	// Java Spring annotations: @GetMapping("/path"), @PostMapping, @RequestMapping
	springMappingRe = regexp.MustCompile(`@(Get|Post|Put|Delete|Patch|Request)Mapping\(\s*(?:value\s*=\s*)?["']([^"']+)["']`)

	// Rust Actix annotations: #[get("/path")], #[post("/path")]
	actixRouteRe = regexp.MustCompile(`#\[(get|post|put|delete|patch)\(\s*"([^"]+)"`)

	// PHP Laravel routes: Route::get("/path", Route::post("/path"
	laravelRouteRe = regexp.MustCompile(`Route::(get|post|put|delete|patch)\(\s*["']([^"']+)["']`)

	// C# ASP.NET route attributes: [HttpGet("/path")], [Route("/path")]
	aspnetRouteRe     = regexp.MustCompile(`\[(Http(?:Get|Post|Put|Delete|Patch))\(\s*"([^"]+)"`)
	aspnetRouteAttrRe = regexp.MustCompile(`\[Route\(\s*"([^"]+)"`)

	// Kotlin Ktor routes: get("/path") {, post("/path") {
	ktorRouteRe = regexp.MustCompile(`\b(get|post|put|delete|patch)\(\s*"([^"]+)"\s*\)`)

	// Laravel handler: Route::get("/path", [Controller::class, "method"]) or "Controller@method"
	laravelHandlerArrayRe = regexp.MustCompile(`Route::(get|post|put|delete|patch)\(\s*["'][^"']+["']\s*,\s*\[(\w+)::class\s*,\s*["'](\w+)["']\]`)
	laravelHandlerAtRe    = regexp.MustCompile(`Route::(get|post|put|delete|patch)\(\s*["'][^"']+["']\s*,\s*["'](\w+)@(\w+)["']`)

	// URL patterns in source: https://host/path or http://host/path — captures domain and path
	urlRe = regexp.MustCompile(`https?://([a-zA-Z0-9.\-]+)(/[a-zA-Z0-9_/:.\-]+)`)

	// Path-only patterns: "/api/something" (quoted paths starting with /)
	pathRe = regexp.MustCompile(`["'](/[a-zA-Z0-9_/:.\-]{2,})["']`)

	// Python WebSocket routes: @app.websocket("/path"), @app.websocket("")
	pyWSRouteRe = regexp.MustCompile(`@\w+\.websocket\(\s*["']([^"']*)["']`)

	// Spring WebSocket: @MessageMapping("/path")
	springWSRe = regexp.MustCompile(`@MessageMapping\(\s*["']([^"']+)["']`)

	// Kotlin Ktor WebSocket: webSocket("/path") {
	ktorWSRe = regexp.MustCompile(`\bwebSocket\(\s*"([^"]+)"\s*\)`)

	// FastAPI prefix: app.include_router(var, prefix="/prefix")
	fastAPIIncludeRe = regexp.MustCompile(`\.include_router\(\s*(\w+)\s*,\s*prefix\s*=\s*["']([^"']+)["']`)

	// Python import: from module.path import var_name
	pyImportRe = regexp.MustCompile(`from\s+([\w.]+)\s+import\s+(\w+)`)

	// Express prefix: app.use("/prefix", routerVar)
	expressUseRe = regexp.MustCompile(`\.use\(\s*["'` + "`" + `]([^"'` + "`" + `]+)["'` + "`" + `]\s*,\s*(\w+)`)

	// JS/TS import patterns for router variable resolution
	jsRequireRe = regexp.MustCompile(`(?:const|let|var)\s+(\w+)\s*=\s*require\(\s*["']([^"']+)["']`)
	jsImportRe  = regexp.MustCompile(`import\s+(\w+)\s+from\s+["']([^"']+)["']`)

	// Path param normalizers
	colonParamRe = regexp.MustCompile(`:[a-zA-Z_]+`)
	braceParamRe = regexp.MustCompile(`\{[a-zA-Z_]+\}`)
)

// expressReceiverAllowlist restricts Express route matching to known router variable names.
// Prevents false positives from req.get("Header"), res.get("key"), map.get("key"), etc.
var expressReceiverAllowlist = map[string]bool{
	"app": true, "router": true, "server": true, "api": true,
	"routes": true, "express": true, "route": true,
}

// Run executes the HTTP linking pass.
func (l *Linker) Run() ([]HTTPLink, error) {
	proj, err := l.store.GetProject(l.project)
	if err != nil {
		return nil, fmt.Errorf("get project: %w", err)
	}
	rootPath := proj.RootPath

	// Load config from project root if using defaults
	if l.config.HTTPLinker.MinConfidence == nil &&
		l.config.HTTPLinker.FuzzyMatching == nil &&
		len(l.config.HTTPLinker.ExcludePaths) == 0 {
		l.config = LoadConfig(rootPath)
	}

	l.routesByFunc = make(map[string][]int)
	routes := l.discoverRoutes(rootPath)
	slog.Info("httplink.routes", "count", len(routes))

	// Build routesByFunc index
	for i, rh := range routes {
		l.routesByFunc[rh.QualifiedName] = append(l.routesByFunc[rh.QualifiedName], i)
	}

	// Resolve cross-file group prefixes before inserting Route nodes
	l.resolveCrossFileGroupPrefixes(routes, rootPath) // Go gin
	l.resolveFastAPIPrefixes(routes, rootPath)        // Python FastAPI
	l.resolveExpressPrefixes(routes, rootPath)        // JS/TS Express

	// Resolve handler references first so insertRouteNodes can use the actual handler QN
	l.createRegistrationCallEdges(routes)

	// Insert Route nodes and HANDLES edges (uses ResolvedHandlerQN from above)
	l.insertRouteNodes(routes, rootPath)

	callSites := l.discoverCallSites(rootPath)
	callSites = append(callSites, l.extraCallSites...)
	slog.Info("httplink.callsites", "count", len(callSites))

	links := l.matchAndLink(routes, callSites)
	slog.Info("httplink.links", "count", len(links))

	return links, nil
}

// insertRouteNodes creates Route nodes for each discovered route handler and
// HANDLES edges from the handler function to the Route node.
func (l *Linker) insertRouteNodes(routes []RouteHandler, rootPath string) {
	for i := range routes {
		l.insertSingleRouteNode(&routes[i], rootPath)
	}
	slog.Info("httplink.route_nodes", "count", len(routes))
}

// insertSingleRouteNode creates a Route node and HANDLES edge for one route handler.
func (l *Linker) insertSingleRouteNode(rh *RouteHandler, rootPath string) {
	normalMethod := rh.Method
	if normalMethod == "" {
		normalMethod = "ANY"
	}
	normalPath := strings.ReplaceAll(rh.Path, "/", "_")
	normalPath = strings.Trim(normalPath, "_")
	routeQN := rh.QualifiedName + ".route." + normalMethod + "." + normalPath
	routeName := normalMethod + " " + rh.Path

	// Use resolved handler QN if available, otherwise fall back to registering function
	handlerQN := rh.QualifiedName
	if rh.ResolvedHandlerQN != "" {
		handlerQN = rh.ResolvedHandlerQN
	}

	// Look up handler node BEFORE creating Route — we need its file_path
	handlerNode, _ := l.store.FindNodeByQN(l.project, handlerQN)

	routeProps := l.buildRouteProps(rh, handlerNode, rootPath)

	// Inherit file_path and line range from handler function
	var filePath string
	var startLine, endLine int
	if handlerNode != nil {
		if handlerNode.FilePath != "" {
			filePath = handlerNode.FilePath
		}
		startLine = handlerNode.StartLine
		endLine = handlerNode.EndLine
	}

	routeID, err := l.store.UpsertNode(&store.Node{
		Project:       l.project,
		Label:         "Route",
		Name:          routeName,
		QualifiedName: routeQN,
		FilePath:      filePath,
		StartLine:     startLine,
		EndLine:       endLine,
		Properties:    routeProps,
	})
	if err != nil || routeID == 0 {
		return
	}

	l.linkHandlerToRoute(handlerNode, routeID, routeQN)
}

// buildRouteProps constructs the properties map for a Route node, including protocol detection.
func (l *Linker) buildRouteProps(rh *RouteHandler, handlerNode *store.Node, rootPath string) map[string]any {
	handlerQN := rh.QualifiedName
	if rh.ResolvedHandlerQN != "" {
		handlerQN = rh.ResolvedHandlerQN
	}

	routeProps := map[string]any{
		"method":  rh.Method,
		"path":    rh.Path,
		"handler": handlerQN,
	}

	// Protocol from route extraction (Python websocket, Spring MessageMapping, Ktor webSocket)
	if rh.Protocol != "" {
		routeProps["protocol"] = rh.Protocol
		return routeProps
	}

	// Detect protocol from handler source if not already set
	if handlerNode != nil && handlerNode.FilePath != "" && handlerNode.StartLine > 0 {
		handlerSource := readSourceLines(rootPath, handlerNode.FilePath, handlerNode.StartLine, handlerNode.EndLine)
		if protocol := detectProtocol(handlerSource); protocol != "" {
			routeProps["protocol"] = protocol
		}
	}

	return routeProps
}

// linkHandlerToRoute creates HANDLES edge and marks handler as entry point.
func (l *Linker) linkHandlerToRoute(handlerNode *store.Node, routeID int64, routeQN string) {
	if handlerNode == nil {
		return
	}

	if _, edgeErr := l.store.InsertEdge(&store.Edge{
		Project:  l.project,
		SourceID: handlerNode.ID,
		TargetID: routeID,
		Type:     "HANDLES",
	}); edgeErr != nil {
		// FK failures expected: LastInsertId() can return stale IDs for upserted Route nodes
		slog.Info("httplink.handles_edge.skip", "route", routeQN)
	}

	// Mark handler as entry point (for dead code detection)
	if handlerNode.Properties == nil {
		handlerNode.Properties = map[string]any{}
	}
	handlerNode.Properties["is_entry_point"] = true
	if _, upsertErr := l.store.UpsertNode(handlerNode); upsertErr != nil {
		slog.Warn("httplink.entry_point.err", "err", upsertErr)
	}
}

// discoverRoutes finds route handler registrations from Function nodes.
func (l *Linker) discoverRoutes(rootPath string) []RouteHandler {
	var routes []RouteHandler

	funcs, err := l.store.FindNodesByLabel(l.project, "Function")
	if err != nil {
		slog.Warn("httplink.routes.funcs.err", "err", err)
		return routes
	}

	methods, err := l.store.FindNodesByLabel(l.project, "Method")
	if err != nil {
		slog.Warn("httplink.routes.methods.err", "err", err)
	} else {
		funcs = append(funcs, methods...)
	}

	// Track which PHP files have Function/Method nodes (for dedup in module scan)
	phpFilesWithFuncs := map[string]bool{}

	for _, f := range funcs {
		// Python: check decorators property
		routes = append(routes, extractPythonRoutes(f)...)

		// Java: check annotation-based decorators (Spring)
		routes = append(routes, extractJavaRoutes(f)...)

		// Rust: check attribute decorators (Actix)
		routes = append(routes, extractRustRoutes(f)...)

		// C# ASP.NET: check attribute decorators
		routes = append(routes, extractASPNetRoutes(f)...)

		// Source-based route discovery (Go gin, Express.js, PHP Laravel, Kotlin Ktor)
		if f.FilePath != "" && f.StartLine > 0 && f.EndLine > 0 {
			source := readSourceLines(rootPath, f.FilePath, f.StartLine, f.EndLine)
			if source != "" {
				routes = append(routes, extractGoRoutes(f, source)...)
				routes = append(routes, extractExpressRoutes(f, source)...)
				routes = append(routes, extractLaravelRoutes(f, source)...)
				routes = append(routes, extractKtorRoutes(f, source)...)
			}
		}

		if strings.HasSuffix(f.FilePath, ".php") {
			phpFilesWithFuncs[f.FilePath] = true
		}
	}

	// Module-level route scanning: some frameworks register routes at file top level
	// (not inside any function body). Scan modules for route patterns.
	modules, err := l.store.FindNodesByLabel(l.project, "Module")
	if err != nil {
		slog.Warn("httplink.routes.modules.err", "err", err)
		return routes
	}

	for _, m := range modules {
		isPHP := strings.HasSuffix(m.FilePath, ".php")
		isJSTS := strings.HasSuffix(m.FilePath, ".js") ||
			strings.HasSuffix(m.FilePath, ".ts") ||
			strings.HasSuffix(m.FilePath, ".mjs") ||
			strings.HasSuffix(m.FilePath, ".mts")

		if !isPHP && !isJSTS {
			continue
		}
		// For PHP: skip files where routes were already extracted from function bodies
		if isPHP && phpFilesWithFuncs[m.FilePath] {
			continue
		}
		source := readSourceFull(rootPath, m.FilePath)
		if source == "" {
			continue
		}
		if isPHP {
			routes = append(routes, extractLaravelRoutes(m, source)...)
		}
		if isJSTS {
			routes = append(routes, extractExpressRoutes(m, source)...)
		}
	}

	return routes
}

// extractPythonRoutes extracts route handlers from Python decorator metadata.
func extractPythonRoutes(f *store.Node) []RouteHandler {
	var routes []RouteHandler

	decs, ok := f.Properties["decorators"]
	if !ok {
		return routes
	}

	// decorators is stored as []any (JSON deserialized)
	decList, ok := decs.([]any)
	if !ok {
		return routes
	}

	for _, d := range decList {
		decStr, ok := d.(string)
		if !ok {
			continue
		}
		// Standard HTTP routes
		matches := pyRouteRe.FindAllStringSubmatch(decStr, -1)
		for _, m := range matches {
			routes = append(routes, RouteHandler{
				Path:          m[2],
				Method:        strings.ToUpper(m[1]),
				FunctionName:  f.Name,
				QualifiedName: f.QualifiedName,
			})
		}
		// WebSocket routes: @app.websocket("/path")
		if wm := pyWSRouteRe.FindStringSubmatch(decStr); wm != nil {
			routes = append(routes, RouteHandler{
				Path:          wm[1],
				Method:        "WS",
				Protocol:      "ws",
				FunctionName:  f.Name,
				QualifiedName: f.QualifiedName,
			})
		}
	}

	return routes
}

// extractGoRoutes extracts route registrations from Go source code (gin/chi patterns).
// Resolves gin group prefixes and chi r.Route("/prefix", func...) blocks.
func extractGoRoutes(f *store.Node, source string) []RouteHandler {
	routes := make([]RouteHandler, 0, 4)

	// Build a map of variable name → group prefix from gin Group() calls
	groupPrefixes := map[string]string{}
	for _, gm := range goGroupRe.FindAllStringSubmatch(source, -1) {
		groupPrefixes[gm[1]] = gm[2]
	}

	// Chi Route() prefix stack for nested r.Route("/prefix", func...) blocks
	type chiBlock struct {
		prefix string
		depth  int
	}
	var chiStack []chiBlock
	braceDepth := 0

	lines := strings.Split(source, "\n")
	for _, line := range lines {
		// Detect chi .Route("/prefix", func...) blocks
		if cm := goChiRouteRe.FindStringSubmatch(line); cm != nil {
			chiStack = append(chiStack, chiBlock{prefix: cm[1], depth: braceDepth})
		}

		// Track brace depth
		braceDepth += strings.Count(line, "{") - strings.Count(line, "}")

		// Pop closed chi blocks
		for len(chiStack) > 0 && braceDepth <= chiStack[len(chiStack)-1].depth {
			chiStack = chiStack[:len(chiStack)-1]
		}

		rm := goRouteRe.FindStringSubmatch(line)
		if rm == nil {
			continue
		}
		method := strings.ToUpper(rm[1])
		path := rm[2]

		// Apply chi prefix stack if active, otherwise try gin group resolution
		if len(chiStack) > 0 {
			var fullPrefix string
			for _, block := range chiStack {
				fullPrefix = strings.TrimRight(fullPrefix, "/") + "/" + strings.TrimLeft(block.prefix, "/")
			}
			if path == "/" || path == "" {
				path = fullPrefix
			} else {
				path = strings.TrimRight(fullPrefix, "/") + "/" + strings.TrimLeft(path, "/")
			}
		} else {
			path = resolveGroupPrefix(line, rm[1], path, groupPrefixes)
		}

		// Capture handler reference (last argument) for CALLS edge creation
		var handlerRef string
		if hm := goRouteHandlerRe.FindStringSubmatch(line); hm != nil {
			handlerRef = hm[2]
		}

		routes = append(routes, RouteHandler{
			Path:          path,
			Method:        method,
			FunctionName:  f.Name,
			QualifiedName: f.QualifiedName,
			HandlerRef:    handlerRef,
		})
	}

	return routes
}

// resolveGroupPrefix resolves a router group prefix for a route line.
// It finds the receiver variable (e.g., "contracts" in "contracts.POST")
// and looks up its group prefix.
func resolveGroupPrefix(line, method, path string, groupPrefixes map[string]string) string {
	idx := strings.Index(line, "."+method+"(")
	if idx <= 0 {
		return path
	}
	prefix := strings.TrimSpace(line[:idx])
	parts := strings.Fields(prefix)
	if len(parts) == 0 {
		return path
	}
	receiver := parts[len(parts)-1]
	gp, ok := groupPrefixes[receiver]
	if !ok {
		return path
	}
	resolved := strings.TrimRight(gp, "/") + "/" + strings.TrimLeft(path, "/")
	if resolved == "/" {
		return gp
	}
	return resolved
}

// extractExpressRoutes extracts route registrations from JS/TS source (Express/Koa patterns).
// Uses receiver allowlist to avoid false positives from req.get(), res.get(), etc.
func extractExpressRoutes(f *store.Node, source string) []RouteHandler {
	routes := make([]RouteHandler, 0, 4)
	for _, line := range strings.Split(source, "\n") {
		rm := expressRouteRe.FindStringSubmatch(line)
		if rm == nil {
			continue
		}
		// rm[1]=receiver, rm[2]=method, rm[3]=path
		receiver := strings.ToLower(rm[1])
		if !expressReceiverAllowlist[receiver] {
			continue
		}

		// Express overloads .get(): with 1 arg it's a config getter (app.get('trust proxy')),
		// with 2+ args it's a route (app.get('/path', handler)). Only .get() has this
		// overload — .post/.put/.delete/.patch are always routes.
		if strings.EqualFold(rm[2], "get") {
			// Check if there's a comma after the closing quote — indicates a callback/handler arg
			matchEnd := strings.Index(line, rm[0]) + len(rm[0])
			rest := strings.TrimSpace(line[matchEnd:])
			if !strings.HasPrefix(rest, ",") {
				continue // Single-arg app.get('setting') — config getter, skip
			}
		}

		var handlerRef string
		hm := expressHandlerRe.FindStringSubmatch(line)
		if hm != nil {
			handlerRef = hm[3] // group 3 after adding receiver capture
		}

		routes = append(routes, RouteHandler{
			Path:          rm[3],
			Method:        strings.ToUpper(rm[2]),
			FunctionName:  f.Name,
			QualifiedName: f.QualifiedName,
			HandlerRef:    handlerRef,
		})
	}
	return routes
}

// extractJavaRoutes extracts routes from Java Spring annotations in decorators.
func extractJavaRoutes(f *store.Node) []RouteHandler {
	var routes []RouteHandler
	decs, ok := f.Properties["decorators"]
	if !ok {
		return routes
	}
	decList, ok := decs.([]any)
	if !ok {
		return routes
	}
	for _, d := range decList {
		decStr, ok := d.(string)
		if !ok {
			continue
		}
		// Standard Spring HTTP mappings
		matches := springMappingRe.FindAllStringSubmatch(decStr, -1)
		for _, m := range matches {
			method := strings.ToUpper(m[1])
			if method == "REQUEST" {
				method = "" // RequestMapping doesn't specify method
			}
			routes = append(routes, RouteHandler{
				Path:          m[2],
				Method:        method,
				FunctionName:  f.Name,
				QualifiedName: f.QualifiedName,
			})
		}
		// Spring WebSocket: @MessageMapping("/path")
		if wm := springWSRe.FindStringSubmatch(decStr); wm != nil {
			routes = append(routes, RouteHandler{
				Path:          wm[1],
				Method:        "WS",
				Protocol:      "ws",
				FunctionName:  f.Name,
				QualifiedName: f.QualifiedName,
			})
		}
	}
	return routes
}

// extractRustRoutes extracts routes from Rust Actix attribute decorators.
func extractRustRoutes(f *store.Node) []RouteHandler {
	var routes []RouteHandler
	decs, ok := f.Properties["decorators"]
	if !ok {
		return routes
	}
	decList, ok := decs.([]any)
	if !ok {
		return routes
	}
	for _, d := range decList {
		decStr, ok := d.(string)
		if !ok {
			continue
		}
		matches := actixRouteRe.FindAllStringSubmatch(decStr, -1)
		for _, m := range matches {
			routes = append(routes, RouteHandler{
				Path:          m[2],
				Method:        strings.ToUpper(m[1]),
				FunctionName:  f.Name,
				QualifiedName: f.QualifiedName,
			})
		}
	}
	return routes
}

// extractLaravelRoutes extracts route registrations from PHP Laravel source.
func extractLaravelRoutes(f *store.Node, source string) []RouteHandler {
	routes := make([]RouteHandler, 0, 4)
	for _, line := range strings.Split(source, "\n") {
		rm := laravelRouteRe.FindStringSubmatch(line)
		if rm == nil {
			continue
		}

		// Try to extract handler reference from [Controller::class, "method"] or "Controller@method"
		var handlerRef string
		if am := laravelHandlerArrayRe.FindStringSubmatch(line); am != nil {
			handlerRef = am[3] // method name from [Controller::class, "method"]
		} else if atm := laravelHandlerAtRe.FindStringSubmatch(line); atm != nil {
			handlerRef = atm[3] // method name from "Controller@method"
		}

		routes = append(routes, RouteHandler{
			Path:          rm[2],
			Method:        strings.ToUpper(rm[1]),
			FunctionName:  f.Name,
			QualifiedName: f.QualifiedName,
			HandlerRef:    handlerRef,
		})
	}
	return routes
}

// extractASPNetRoutes extracts route handlers from C# ASP.NET attribute metadata.
func extractASPNetRoutes(f *store.Node) []RouteHandler {
	var routes []RouteHandler

	decs, ok := f.Properties["decorators"]
	if !ok {
		return routes
	}

	decList, ok := decs.([]any)
	if !ok {
		return routes
	}

	for _, d := range decList {
		decStr, ok := d.(string)
		if !ok {
			continue
		}
		// [HttpGet("/path")] pattern
		matches := aspnetRouteRe.FindAllStringSubmatch(decStr, -1)
		for _, m := range matches {
			method := strings.TrimPrefix(m[1], "Http")
			routes = append(routes, RouteHandler{
				Path:          m[2],
				Method:        strings.ToUpper(method),
				FunctionName:  f.Name,
				QualifiedName: f.QualifiedName,
			})
		}
		// [Route("/path")] pattern
		routeMatches := aspnetRouteAttrRe.FindAllStringSubmatch(decStr, -1)
		for _, m := range routeMatches {
			routes = append(routes, RouteHandler{
				Path:          m[1],
				Method:        "",
				FunctionName:  f.Name,
				QualifiedName: f.QualifiedName,
			})
		}
	}
	return routes
}

// extractKtorRoutes extracts route handlers from Kotlin Ktor source code.
func extractKtorRoutes(f *store.Node, source string) []RouteHandler {
	routes := make([]RouteHandler, 0, 4)
	for _, line := range strings.Split(source, "\n") {
		// Standard HTTP routes
		if rm := ktorRouteRe.FindStringSubmatch(line); rm != nil {
			routes = append(routes, RouteHandler{
				Path:          rm[2],
				Method:        strings.ToUpper(rm[1]),
				FunctionName:  f.Name,
				QualifiedName: f.QualifiedName,
			})
			continue
		}
		// WebSocket routes: webSocket("/path") {
		if wm := ktorWSRe.FindStringSubmatch(line); wm != nil {
			routes = append(routes, RouteHandler{
				Path:          wm[1],
				Method:        "WS",
				Protocol:      "ws",
				FunctionName:  f.Name,
				QualifiedName: f.QualifiedName,
			})
		}
	}
	return routes
}

// discoverCallSites finds HTTP URL references in Module constants and Function source.
func (l *Linker) discoverCallSites(rootPath string) []HTTPCallSite {
	var sites []HTTPCallSite

	// Module constants
	modules, err := l.store.FindNodesByLabel(l.project, "Module")
	if err != nil {
		slog.Warn("httplink.callsites.modules.err", "err", err)
	} else {
		for _, m := range modules {
			sites = append(sites, extractModuleCallSites(m)...)
		}
	}

	// Function/Method source
	funcs, err := l.store.FindNodesByLabel(l.project, "Function")
	if err != nil {
		slog.Warn("httplink.callsites.funcs.err", "err", err)
	} else {
		for _, f := range funcs {
			sites = append(sites, extractFunctionCallSites(f, rootPath)...)
		}
	}

	methods, err := l.store.FindNodesByLabel(l.project, "Method")
	if err != nil {
		slog.Warn("httplink.callsites.methods.err", "err", err)
	} else {
		for _, f := range methods {
			sites = append(sites, extractFunctionCallSites(f, rootPath)...)
		}
	}

	return sites
}

// extractModuleCallSites extracts HTTP paths from module constants.
func extractModuleCallSites(m *store.Node) []HTTPCallSite {
	var sites []HTTPCallSite

	constants, ok := m.Properties["constants"]
	if !ok {
		return sites
	}

	constList, ok := constants.([]any)
	if !ok {
		return sites
	}

	for _, c := range constList {
		cStr, ok := c.(string)
		if !ok {
			continue
		}
		paths := extractURLPaths(cStr)
		for _, p := range paths {
			sites = append(sites, HTTPCallSite{
				Path:                p,
				SourceName:          m.Name,
				SourceQualifiedName: m.QualifiedName,
				SourceLabel:         "Module",
			})
		}
	}

	return sites
}

// detectHTTPMethod tries to find the HTTP method used near a URL path in source code.
func detectHTTPMethod(source string) string {
	upper := strings.ToUpper(source)
	for _, verb := range []string{"POST", "PUT", "DELETE", "PATCH", "GET"} {
		// Python: requests.post(, httpx.post(
		if strings.Contains(upper, "REQUESTS."+verb+"(") || strings.Contains(upper, "HTTPX."+verb+"(") {
			return verb
		}
		// Go: "POST" near http.NewRequest
		if strings.Contains(upper, `"`+verb+`"`) && strings.Contains(upper, "HTTP.") {
			return verb
		}
		// JS: method: "POST", method: 'POST'
		if strings.Contains(upper, "METHOD") && strings.Contains(upper, verb) {
			return verb
		}
		// Java: HttpMethod.POST, .method(POST
		if strings.Contains(upper, "HTTPMETHOD."+verb) {
			return verb
		}
		// Rust: reqwest::Client::new().post(, .get(
		if strings.Contains(source, "."+strings.ToLower(verb)+"(") {
			return verb
		}
		// PHP: curl CURLOPT_CUSTOMREQUEST
		if strings.Contains(upper, "CURLOPT") && strings.Contains(upper, verb) {
			return verb
		}
	}
	return ""
}

// httpClientKeywords are patterns indicating actual HTTP client usage.
// A function must contain at least one of these to be considered an HTTP call site.
var httpClientKeywords = []string{
	// Python
	"requests.get", "requests.post", "requests.put", "requests.delete", "requests.patch",
	"httpx.", "aiohttp.", "urllib.request",
	// Go
	"http.Get", "http.Post", "http.NewRequest", "client.Do(",
	// JavaScript/TypeScript
	"fetch(", "axios.", ".ajax(",
	// Java
	"HttpClient", "RestTemplate", "WebClient", "OkHttpClient",
	"HttpURLConnection", "openConnection(",
	// Rust
	"reqwest::", "hyper::", "surf::", "ureq::",
	// PHP
	"curl_exec", "curl_init", "Guzzle", "Http::get", "Http::post",
	// Scala
	"sttp.", "http4s", "HttpClient", "wsClient",
	// C++
	"curl_easy", "cpr::Get", "cpr::Post", "httplib::",
	// Lua
	"socket.http", "http.request", "curl.",
	// C#
	"HttpClient", "WebClient", "RestClient", "HttpWebRequest",
	// Kotlin
	"OkHttpClient", "HttpClient", "ktor.client",
	// Generic
	"send_request", "http_client",
}

// asyncDispatchKeywords indicate cross-service async dispatch via HTTP.
// Functions containing these keywords create ASYNC_CALLS edges instead of HTTP_CALLS.
var asyncDispatchKeywords = []string{
	// Cloud Tasks (GCP) — task body contains HTTP URL
	"CreateTask", "create_task",
	// Pub/Sub publish (GCP) — push subscriptions deliver via HTTP
	"topic.Publish", "publisher.publish", "topic.publish",
	// AWS SQS/SNS — SQS + Lambda/HTTP, SNS + HTTP subscription
	"sqs.send_message", "sns.publish",
	// RabbitMQ — exchange → HTTP consumer
	"basic_publish",
	// Kafka — consumer often fronted by HTTP
	"producer.send", "producer.Send",
}

// wsPatterns indicate WebSocket usage in handler source.
var wsPatterns = []string{
	// Go: gorilla/nhooyr websocket
	"websocket.Upgrade", "websocket.Accept", "upgrader.Upgrade",
	// JS/TS: ws library, socket.io
	`ws.on("connection`, `io.on("connection`, "new WebSocket(",
	// Generic
	"WebSocketSession", "wsHandler",
}

// ssePatterns indicate Server-Sent Events usage in handler source.
var ssePatterns = []string{
	"text/event-stream",
	"EventSourceResponse",
	"SseEmitter",
	"ServerSentEvent",
	"event-stream",
}

// detectProtocol checks handler source for WebSocket or SSE patterns.
// Returns "ws", "sse", or "" (standard HTTP).
func detectProtocol(source string) string {
	for _, p := range wsPatterns {
		if strings.Contains(source, p) {
			return "ws"
		}
	}
	for _, p := range ssePatterns {
		if strings.Contains(source, p) {
			return "sse"
		}
	}
	return ""
}

// extractFunctionCallSites extracts HTTP paths from function source code.
func extractFunctionCallSites(f *store.Node, rootPath string) []HTTPCallSite {
	sites := make([]HTTPCallSite, 0, 4)

	if f.FilePath == "" || f.StartLine <= 0 || f.EndLine <= 0 {
		return sites
	}

	// Skip Python dunder methods — they configure, not call
	if strings.HasPrefix(f.Name, "__") && strings.HasSuffix(f.Name, "__") {
		return sites
	}

	source := readSourceLines(rootPath, f.FilePath, f.StartLine, f.EndLine)
	if source == "" {
		return sites
	}

	// Require at least one HTTP client or async dispatch keyword to avoid
	// false positives from functions that merely store URL strings in variables
	hasHTTPClient := false
	for _, kw := range httpClientKeywords {
		if strings.Contains(source, kw) {
			hasHTTPClient = true
			break
		}
	}

	hasAsyncDispatch := false
	for _, kw := range asyncDispatchKeywords {
		if strings.Contains(source, kw) {
			hasAsyncDispatch = true
			break
		}
	}

	if !hasHTTPClient && !hasAsyncDispatch {
		return sites
	}

	// Sync (HTTP client) takes precedence over async dispatch
	isAsync := hasAsyncDispatch && !hasHTTPClient

	method := detectHTTPMethod(source)

	paths := extractURLPaths(source)
	for _, p := range paths {
		sites = append(sites, HTTPCallSite{
			Path:                p,
			Method:              method,
			SourceName:          f.Name,
			SourceQualifiedName: f.QualifiedName,
			SourceLabel:         f.Label,
			IsAsync:             isAsync,
		})
	}

	return sites
}

// externalDomains are well-known external API domains whose paths
// should not be matched against internal route handlers.
var externalDomains = []string{
	"googleapis.com",
	"google.com",
	"github.com",
	"gitlab.com",
	"docker.com",
	"docker.io",
	"npmjs.org",
	"pypi.org",
	"cloudflare.com",
	"sentry.io",
	"aws.amazon.com",
}

// defaultExcludePaths are common utility endpoints that produce noise in HTTP_CALLS.
// These are excluded from route matching by default.
var defaultExcludePaths = []string{
	"/health",
	"/healthz",
	"/ready",
	"/readyz",
	"/metrics",
	"/favicon.ico",
}

// isExternalDomain checks if a domain is a well-known external API.
func isExternalDomain(domain string) bool {
	domain = strings.ToLower(domain)
	for _, ext := range externalDomains {
		if domain == ext || strings.HasSuffix(domain, "."+ext) {
			return true
		}
	}
	return false
}

// ExtractURLPaths finds URL path segments from text (exported for use by pipeline).
func ExtractURLPaths(text string) []string {
	return extractURLPaths(text)
}

// extractURLPaths finds URL path segments from text.
func extractURLPaths(text string) []string {
	seen := map[string]bool{}
	var paths []string

	// Full URLs: extract domain and path, skip external domains
	for _, m := range urlRe.FindAllStringSubmatch(text, -1) {
		domain := m[1]
		p := m[2]
		if isExternalDomain(domain) {
			continue
		}
		if !seen[p] {
			seen[p] = true
			paths = append(paths, p)
		}
	}

	// Quoted path literals
	for _, m := range pathRe.FindAllStringSubmatch(text, -1) {
		p := m[1]
		if !seen[p] {
			seen[p] = true
			paths = append(paths, p)
		}
	}

	// Try to extract URLs from embedded JSON strings (e.g., Cloud Tasks payloads)
	for _, p := range extractJSONStringPaths(text) {
		if !seen[p] {
			seen[p] = true
			paths = append(paths, p)
		}
	}

	return paths
}

// extractJSONStringPaths tries to JSON-parse the text (or substrings that look
// like JSON) and extract URL paths from string values within.
func extractJSONStringPaths(text string) []string {
	seen := make(map[string]bool)
	var paths []string

	// Find JSON-like substrings: {...} or [...]
	for _, bounds := range findJSONBounds(text) {
		var parsed any
		if err := json.Unmarshal([]byte(bounds), &parsed); err != nil {
			continue
		}
		var raw []string
		walkJSONForURLs(parsed, &raw)
		for _, p := range raw {
			if !seen[p] {
				seen[p] = true
				paths = append(paths, p)
			}
		}
	}

	return paths
}

// findJSONBounds extracts substrings that look like JSON objects or arrays.
func findJSONBounds(text string) []string {
	results := make([]string, 0, 4)
	for _, opener := range []byte{'{', '['} {
		closer := byte('}')
		if opener == '[' {
			closer = ']'
		}
		results = append(results, scanJSONBlocks(text, opener, closer)...)
	}
	return results
}

// scanJSONBlocks scans text for balanced JSON blocks delimited by opener/closer.
func scanJSONBlocks(text string, opener, closer byte) []string {
	var results []string
	start := strings.IndexByte(text, opener)
	for start >= 0 && start < len(text) {
		end, ok := findBalancedEnd(text, start, opener, closer)
		if !ok {
			break
		}
		candidate := text[start : end+1]
		if len(candidate) > 5 {
			results = append(results, candidate)
		}
		start = end + 1
		next := strings.IndexByte(text[start:], opener)
		if next < 0 {
			break
		}
		start += next
	}
	return results
}

// findBalancedEnd finds the index of the closing bracket that balances the opener at start.
// Returns the index and true if found, or 0 and false if unbalanced.
func findBalancedEnd(text string, start int, opener, closer byte) (int, bool) {
	depth := 0
	inStr := false
	for i := start; i < len(text); i++ {
		ch := text[i]
		if inStr {
			if ch == '\\' {
				i++ // skip escaped char
				continue
			}
			if ch == '"' {
				inStr = false
			}
			continue
		}
		switch ch {
		case '"':
			inStr = true
		case opener:
			depth++
		case closer:
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return 0, false
}

// walkJSONForURLs recursively walks parsed JSON and extracts URL paths.
func walkJSONForURLs(v any, out *[]string) {
	switch val := v.(type) {
	case map[string]any:
		for _, child := range val {
			walkJSONForURLs(child, out)
		}
	case []any:
		for _, child := range val {
			walkJSONForURLs(child, out)
		}
	case string:
		// Check if value is a URL or path
		for _, m := range urlRe.FindAllStringSubmatch(val, -1) {
			if !isExternalDomain(m[1]) {
				*out = append(*out, m[2])
			}
		}
		for _, m := range pathRe.FindAllStringSubmatch(`"`+val+`"`, -1) {
			*out = append(*out, m[1])
		}
	}
}

// matchAndLink matches call site paths to route handler paths and creates edges.
// Uses multi-signal probabilistic scoring (path Jaccard, depth, method, source type).
// Only creates edges above the confidence threshold.
func (l *Linker) matchAndLink(routes []RouteHandler, callSites []HTTPCallSite) []HTTPLink {
	var links []HTTPLink

	for _, cs := range callSites {
		for _, rh := range routes {
			if sameService(cs.SourceQualifiedName, rh.QualifiedName) {
				continue
			}

			// Skip noisy utility endpoints
			if isPathExcluded(rh.Path, l.config.AllExcludePaths()) {
				continue
			}

			// Multi-signal confidence scoring
			pathScore := pathMatchScore(cs.Path, rh.Path)
			if pathScore == 0 {
				continue
			}

			score := pathScore*sourceWeight(cs.SourceLabel) + methodBonus(cs.Method, rh.Method)
			if score < l.config.EffectiveMinConfidence() {
				continue
			}
			if score > 1.0 {
				score = 1.0
			}

			// Create edge with confidence score
			edgeType := "HTTP_CALLS"
			if cs.IsAsync {
				edgeType = "ASYNC_CALLS"
			}

			callerNode, _ := l.store.FindNodeByQN(l.project, cs.SourceQualifiedName)
			handlerNode, _ := l.store.FindNodeByQN(l.project, rh.QualifiedName)
			if callerNode != nil && handlerNode != nil {
				band := confidenceBand(score)
				props := map[string]any{
					"url_path":        cs.Path,
					"confidence":      score,
					"confidence_band": band,
				}
				if rh.Method != "" {
					props["method"] = rh.Method
				}
				_, _ = l.store.InsertEdge(&store.Edge{
					Project:    l.project,
					SourceID:   callerNode.ID,
					TargetID:   handlerNode.ID,
					Type:       edgeType,
					Properties: props,
				})
			}

			links = append(links, HTTPLink{
				CallerQN:    cs.SourceQualifiedName,
				CallerLabel: cs.SourceLabel,
				HandlerQN:   rh.QualifiedName,
				URLPath:     cs.Path,
				EdgeType:    edgeType,
			})
		}
	}

	return links
}

// normalizePath normalizes a URL path for comparison.
// numericSegmentRe matches path segments that are pure numeric IDs.
var numericSegmentRe = regexp.MustCompile(`/\d+(/|$)`)

// uuidSegmentRe matches path segments that are UUIDs.
var uuidSegmentRe = regexp.MustCompile(`/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(/|$)`)

func normalizePath(path string) string {
	path = strings.TrimRight(path, "/")
	path = colonParamRe.ReplaceAllString(path, "*")
	path = braceParamRe.ReplaceAllString(path, "*")
	// Normalize UUIDs and numeric IDs to wildcards for better matching
	path = uuidSegmentRe.ReplaceAllString(path, "/*$1")
	path = numericSegmentRe.ReplaceAllString(path, "/*$1")
	return strings.ToLower(path)
}

// matchConfidenceThreshold is the minimum score for an HTTP_CALLS edge.
// Lowered from 0.45 to 0.25 to include speculative matches with confidence bands.
const matchConfidenceThreshold = 0.25

// pathMatchScore returns a confidence score (0.0–1.0) for how well callPath
// matches routePath. Returns 0 if no match.
//
// Multi-signal scoring (inspired by RAD/Code2DFD research):
//
//	confidence = matchBase × (0.5 × jaccard + 0.5 × depthFactor)
//
// Where:
//
//	matchBase:   exact=0.95, suffix=0.75, wildcard=0.55
//	jaccard:     segment Jaccard similarity (non-wildcard segments)
//	depthFactor: min(matched_segments / 3.0, 1.0) — longer paths = more specific
func pathMatchScore(callPath, routePath string) float64 {
	normCall := normalizePath(callPath)
	normRoute := normalizePath(routePath)

	if normCall == "" || normRoute == "" {
		return 0
	}

	// Determine structural match type
	var matchBase float64
	var matchedCallSegs, matchedRouteSegs []string

	switch {
	case normCall == normRoute:
		matchBase = 0.95
		matchedCallSegs = splitSegments(normCall)
		matchedRouteSegs = splitSegments(normRoute)
	case strings.HasSuffix(normCall, normRoute):
		matchBase = 0.75
		matchedCallSegs = splitSegments(normRoute) // use the route portion that matched
		matchedRouteSegs = splitSegments(normRoute)
	default:
		// Segment-by-segment wildcard matching
		callParts := strings.Split(normCall, "/")
		routeParts := strings.Split(normRoute, "/")
		if len(callParts) != len(routeParts) {
			return 0
		}
		for i := range callParts {
			if callParts[i] != routeParts[i] && callParts[i] != "*" && routeParts[i] != "*" {
				return 0
			}
		}
		matchBase = 0.55
		matchedCallSegs = splitSegments(normCall)
		matchedRouteSegs = splitSegments(normRoute)
	}

	// Jaccard similarity on non-empty, non-wildcard segments
	jaccard := segmentJaccard(matchedCallSegs, matchedRouteSegs)

	// Depth factor: more segments = more specific match
	totalSegs := len(matchedRouteSegs)
	depthFactor := float64(totalSegs) / 3.0
	if depthFactor > 1.0 {
		depthFactor = 1.0
	}

	score := matchBase * (0.5*jaccard + 0.5*depthFactor)
	if score > 1.0 {
		score = 1.0
	}
	return score
}

// splitSegments splits a normalized path into non-empty segments.
func splitSegments(path string) []string {
	var segs []string
	for _, s := range strings.Split(path, "/") {
		if s != "" {
			segs = append(segs, s)
		}
	}
	return segs
}

// segmentJaccard computes Jaccard similarity on non-wildcard path segments.
// Wildcards (*) are excluded from both sets since they match anything.
func segmentJaccard(segsA, segsB []string) float64 {
	setA := make(map[string]bool)
	setB := make(map[string]bool)
	for _, s := range segsA {
		if s != "*" {
			setA[s] = true
		}
	}
	for _, s := range segsB {
		if s != "*" {
			setB[s] = true
		}
	}

	if len(setA) == 0 && len(setB) == 0 {
		return 0
	}

	intersection := 0
	for k := range setA {
		if setB[k] {
			intersection++
		}
	}

	union := len(setA)
	for k := range setB {
		if !setA[k] {
			union++
		}
	}

	if union == 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}

// methodBonus returns a confidence adjustment based on HTTP method matching.
//
//	+0.10 if both methods are known and match
//	 0.00 if one or both methods are unknown
//	-0.15 if both methods are known and mismatch
func methodBonus(callMethod, routeMethod string) float64 {
	if callMethod == "" || routeMethod == "" {
		return 0
	}
	if strings.EqualFold(callMethod, routeMethod) {
		return 0.10
	}
	return -0.15
}

// sourceWeight returns a confidence multiplier based on call site type.
// Function/Method sources are higher confidence (HTTP client in source code)
// than Module sources (URL in constants — may be config, not a call).
func sourceWeight(label string) float64 {
	switch label {
	case "Function", "Method":
		return 1.0
	default:
		return 0.85
	}
}

// resolveFastAPIPrefixes resolves include_router prefixes for FastAPI routes.
// Scans Python Module files for app.include_router(var, prefix="/prefix") calls
// and prepends the prefix to routes from the imported module.
func (l *Linker) resolveFastAPIPrefixes(routes []RouteHandler, rootPath string) {
	modules, err := l.store.FindNodesByLabel(l.project, "Module")
	if err != nil {
		return
	}

	for _, mod := range modules {
		if !strings.HasSuffix(mod.FilePath, ".py") {
			continue
		}

		source, readErr := os.ReadFile(filepath.Join(rootPath, mod.FilePath))
		if readErr != nil {
			continue
		}
		srcStr := string(source)

		includes := fastAPIIncludeRe.FindAllStringSubmatch(srcStr, -1)
		if len(includes) == 0 {
			continue
		}

		// Build import map: var_name → dotted module path
		imports := map[string]string{}
		for _, m := range pyImportRe.FindAllStringSubmatch(srcStr, -1) {
			imports[m[2]] = m[1] // var_name → module.path
		}

		for _, inc := range includes {
			varName := inc[1]
			prefix := inc[2]

			modulePath, ok := imports[varName]
			if !ok {
				continue
			}

			// Convert dotted module path to file path fragment
			fileFrag := strings.ReplaceAll(modulePath, ".", "/")
			normalizedPrefix := strings.TrimRight(prefix, "/")

			prefixed := 0
			for i := range routes {
				if strings.HasPrefix(routes[i].Path, normalizedPrefix) {
					continue
				}
				// Match routes whose QN contains the imported module path
				if strings.Contains(routes[i].QualifiedName, fileFrag+".py") ||
					strings.Contains(routes[i].QualifiedName, fileFrag+"/") {
					routes[i].Path = normalizedPrefix + "/" + strings.TrimLeft(routes[i].Path, "/")
					prefixed++
				}
			}
			if prefixed > 0 {
				slog.Info("httplink.fastapi_prefix", "prefix", prefix, "module", modulePath, "routes", prefixed)
			}
		}
	}
}

// resolveExpressPrefixes resolves app.use("/prefix", router) for Express routes.
// Scans JS/TS Module files for .use("/prefix", routerVar) calls and prepends
// the prefix to routes from the imported module.
func (l *Linker) resolveExpressPrefixes(routes []RouteHandler, rootPath string) {
	modules, err := l.store.FindNodesByLabel(l.project, "Module")
	if err != nil {
		return
	}

	for _, mod := range modules {
		if !isJSTSModule(mod.FilePath) {
			continue
		}

		source, readErr := os.ReadFile(filepath.Join(rootPath, mod.FilePath))
		if readErr != nil {
			continue
		}
		srcStr := string(source)

		uses := expressUseRe.FindAllStringSubmatch(srcStr, -1)
		if len(uses) == 0 {
			continue
		}

		imports := buildJSImportMap(srcStr)
		l.applyExpressUsePrefixes(routes, uses, imports)
	}
}

// isJSTSModule returns true if the file path is a JS/TS module file.
func isJSTSModule(filePath string) bool {
	return strings.HasSuffix(filePath, ".js") || strings.HasSuffix(filePath, ".ts") ||
		strings.HasSuffix(filePath, ".mjs") || strings.HasSuffix(filePath, ".tsx")
}

// buildJSImportMap builds a map of var_name to module path from require/import statements.
func buildJSImportMap(src string) map[string]string {
	imports := map[string]string{}
	for _, m := range jsRequireRe.FindAllStringSubmatch(src, -1) {
		imports[m[1]] = m[2]
	}
	for _, m := range jsImportRe.FindAllStringSubmatch(src, -1) {
		imports[m[1]] = m[2]
	}
	return imports
}

// applyExpressUsePrefixes applies .use("/prefix", routerVar) prefix resolution to routes.
func (l *Linker) applyExpressUsePrefixes(routes []RouteHandler, uses [][]string, imports map[string]string) {
	for _, use := range uses {
		prefix := use[1]
		varName := use[2]

		modulePath, ok := imports[varName]
		if !ok {
			continue
		}

		// Strip leading ./ from relative import
		fileFrag := strings.TrimPrefix(modulePath, "./")
		fileFrag = strings.TrimPrefix(fileFrag, "../")
		normalizedPrefix := strings.TrimRight(prefix, "/")

		prefixed := 0
		for i := range routes {
			if strings.HasPrefix(routes[i].Path, normalizedPrefix) {
				continue
			}
			if strings.Contains(routes[i].QualifiedName, fileFrag+".js") ||
				strings.Contains(routes[i].QualifiedName, fileFrag+".ts") ||
				strings.Contains(routes[i].QualifiedName, fileFrag+"/") {
				routes[i].Path = normalizedPrefix + "/" + strings.TrimLeft(routes[i].Path, "/")
				prefixed++
			}
		}
		if prefixed > 0 {
			slog.Info("httplink.express_prefix", "prefix", prefix, "module", modulePath, "routes", prefixed)
		}
	}
}

// pathsMatch is a convenience wrapper for tests — returns true if score >= threshold.
func pathsMatch(callPath, routePath string) bool {
	return pathMatchScore(callPath, routePath) >= matchConfidenceThreshold
}

// sameService checks if two qualified names share the same directory path.
// It strips the last 2 segments (module file + function/method name) from each
// QN and compares the remaining directory prefix. If the prefixes are identical,
// the nodes are in the same deployable unit.
//
// Example: "myapp.docker-images.cloud-runs.svcA.module.func" → dir prefix "myapp.docker-images.cloud-runs.svcA"
//
//	"myapp.docker-images.cloud-runs.svcB.routes.handler" → dir prefix "myapp.docker-images.cloud-runs.svcB"
//	→ different prefix → different service → returns false
func sameService(qn1, qn2 string) bool {
	parts1 := strings.Split(qn1, ".")
	parts2 := strings.Split(qn2, ".")

	// Strip last 2 segments (module + name) to get directory path
	const strip = 2
	if len(parts1) <= strip || len(parts2) <= strip {
		return false
	}
	dir1 := strings.Join(parts1[:len(parts1)-strip], ".")
	dir2 := strings.Join(parts2[:len(parts2)-strip], ".")
	return dir1 == dir2
}

// crossFileGroupRe matches patterns like: funcName(something.Group("/prefix"))
// Captures the function name being called and the group prefix.
var crossFileGroupRe = regexp.MustCompile(`(\w+)\s*\(\s*\w+\.Group\s*\(\s*"([^"]+)"\s*\)`)

// crossFileGroupVarRe matches the variable-based pattern:
// varName := something.Group("/prefix")
// ... (next line)
// funcName(varName)
var crossFileGroupVarRe = regexp.MustCompile(`(\w+)\s*:?=\s*\w+\.Group\s*\(\s*"([^"]+)"\s*\)`)

// resolveCrossFileGroupPrefixes resolves Group() prefixes from caller functions
// for routes that were registered without a group prefix within their own function.
func (l *Linker) resolveCrossFileGroupPrefixes(routes []RouteHandler, rootPath string) {
	for funcQN, indices := range l.routesByFunc {
		funcNode, _ := l.store.FindNodeByQN(l.project, funcQN)
		if funcNode == nil {
			continue
		}

		callerEdges, _ := l.store.FindEdgesByTargetAndType(funcNode.ID, "CALLS")
		if len(callerEdges) == 0 {
			continue
		}

		l.resolveCallerGroupPrefixes(routes, indices, callerEdges, funcNode.Name, rootPath)
	}
}

// resolveCallerGroupPrefixes checks each caller's source for Group() prefix passing
// and prepends the prefix to the routes at the given indices.
func (l *Linker) resolveCallerGroupPrefixes(routes []RouteHandler, indices []int, callerEdges []*store.Edge, funcName, rootPath string) {
	for _, edge := range callerEdges {
		callerNode, _ := l.store.FindNodeByID(edge.SourceID)
		if callerNode == nil || callerNode.FilePath == "" || callerNode.StartLine <= 0 {
			continue
		}

		callerSource := readSourceLines(rootPath, callerNode.FilePath, callerNode.StartLine, callerNode.EndLine)
		if callerSource == "" {
			continue
		}

		// Pattern 1: direct â RegisterRoutes(router.Group("/api"))
		for _, m := range crossFileGroupRe.FindAllStringSubmatch(callerSource, -1) {
			if m[1] == funcName {
				l.prependPrefixToRoutes(routes, indices, m[2])
				break
			}
		}

		// Pattern 2: variable-based â v1 := router.Group("/api"); RegisterRoutes(v1)
		l.resolveVarGroupPrefix(routes, indices, callerSource, funcName)
	}
}

// resolveVarGroupPrefix resolves Group() prefixes passed via intermediate variables.
func (l *Linker) resolveVarGroupPrefix(routes []RouteHandler, indices []int, callerSource, funcName string) {
	varPrefixes := map[string]string{}
	for _, m := range crossFileGroupVarRe.FindAllStringSubmatch(callerSource, -1) {
		varPrefixes[m[1]] = m[2]
	}
	if len(varPrefixes) == 0 {
		return
	}
	callRe := regexp.MustCompile(regexp.QuoteMeta(funcName) + `\s*\(\s*(\w+)`)
	for _, cm := range callRe.FindAllStringSubmatch(callerSource, -1) {
		if prefix, ok := varPrefixes[cm[1]]; ok {
			l.prependPrefixToRoutes(routes, indices, prefix)
			break
		}
	}
}

// prependPrefixToRoutes prepends a group prefix to routes at the given indices,
// but only if the route path doesn't already start with the prefix.
func (l *Linker) prependPrefixToRoutes(routes []RouteHandler, indices []int, prefix string) {
	for _, idx := range indices {
		rh := &routes[idx]
		normalizedPrefix := strings.TrimRight(prefix, "/")
		if !strings.HasPrefix(rh.Path, normalizedPrefix) {
			rh.Path = normalizedPrefix + "/" + strings.TrimLeft(rh.Path, "/")
		}
	}
	slog.Info("httplink.cross_file_prefix", "prefix", prefix, "routes", len(indices))
}

// createRegistrationCallEdges creates CALLS edges from route-registering functions
// to the handler functions they reference (e.g. .POST("/path", h.CreateOrder)).
func (l *Linker) createRegistrationCallEdges(routes []RouteHandler) {
	count := 0
	for i := range routes {
		rh := &routes[i]
		if rh.HandlerRef == "" {
			continue
		}

		// Find the registering function node
		registrar, _ := l.store.FindNodeByQN(l.project, rh.QualifiedName)
		if registrar == nil {
			continue
		}

		// Resolve handler reference — try method name (strip receiver prefix like "h.")
		handlerName := rh.HandlerRef
		if idx := strings.LastIndex(handlerName, "."); idx >= 0 {
			handlerName = handlerName[idx+1:]
		}

		// Search for the handler function/method by name
		handlerNodes, _ := l.store.FindNodesByName(l.project, handlerName)
		if len(handlerNodes) == 0 {
			continue
		}

		// Use the first match (typically unique within a project)
		handler := handlerNodes[0]

		// Propagate resolved handler QN back to route for insertRouteNodes
		rh.ResolvedHandlerQN = handler.QualifiedName

		_, _ = l.store.InsertEdge(&store.Edge{
			Project:  l.project,
			SourceID: registrar.ID,
			TargetID: handler.ID,
			Type:     "CALLS",
			Properties: map[string]any{
				"via": "route_registration",
			},
		})
		count++
	}
	if count > 0 {
		slog.Info("httplink.registration_edges", "count", count)
	}
}

// readSourceLines reads specific lines from a file on disk.
// readSourceFull reads the entire file content as a string.
func readSourceFull(rootPath, relPath string) string {
	data, err := os.ReadFile(filepath.Join(rootPath, relPath))
	if err != nil {
		return ""
	}
	return string(data)
}

func readSourceLines(rootPath, relPath string, startLine, endLine int) string {
	absPath := filepath.Join(rootPath, relPath)
	f, err := os.Open(absPath)
	if err != nil {
		return ""
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		if lineNum >= startLine && lineNum <= endLine {
			lines = append(lines, scanner.Text())
		}
		if lineNum > endLine {
			break
		}
	}
	return strings.Join(lines, "\n")
}
