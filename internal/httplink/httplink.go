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
	Path          string
	Method        string
	FunctionName  string
	QualifiedName string
}

// HTTPCallSite represents a discovered HTTP call site.
type HTTPCallSite struct {
	Path                string
	Method              string // best-effort: "GET", "POST", etc. or "" if unknown
	SourceName          string
	SourceQualifiedName string
	SourceLabel         string // "Function", "Method", or "Module"
}

// HTTPLink represents a matched HTTP call from caller to handler.
type HTTPLink struct {
	CallerQN    string
	CallerLabel string
	HandlerQN   string
	URLPath     string
}

// Linker discovers cross-service HTTP calls and creates HTTP_CALLS edges.
type Linker struct {
	store   *store.Store
	project string
}

// New creates a new HTTP Linker.
func New(s *store.Store, project string) *Linker {
	return &Linker{store: s, project: project}
}

// regex patterns for route and URL discovery
var (
	// Python decorators: @app.post("/path"), @router.get("/path")
	pyRouteRe = regexp.MustCompile(`@\w+\.(get|post|put|delete|patch)\(\s*["']([^"']+)["']`)

	// Go gin routes: .POST("/path", .GET("/path"
	goRouteRe = regexp.MustCompile(`\.(GET|POST|PUT|DELETE|PATCH)\(\s*["']([^"']+)["']`)

	// Express.js routes: app.get("/path", router.post("/path"
	expressRouteRe = regexp.MustCompile(`\w+\.(get|post|put|delete|patch)\(\s*["'` + "`" + `]([^"'` + "`" + `]+)["'` + "`" + `]`)

	// Java Spring annotations: @GetMapping("/path"), @PostMapping, @RequestMapping
	springMappingRe = regexp.MustCompile(`@(Get|Post|Put|Delete|Patch|Request)Mapping\(\s*(?:value\s*=\s*)?["']([^"']+)["']`)

	// Rust Actix annotations: #[get("/path")], #[post("/path")]
	actixRouteRe = regexp.MustCompile(`#\[(get|post|put|delete|patch)\(\s*"([^"]+)"`)

	// PHP Laravel routes: Route::get("/path", Route::post("/path"
	laravelRouteRe = regexp.MustCompile(`Route::(get|post|put|delete|patch)\(\s*["']([^"']+)["']`)

	// URL patterns in source: https://host/path or http://host/path — captures domain and path
	urlRe = regexp.MustCompile(`https?://([a-zA-Z0-9.\-]+)(/[a-zA-Z0-9_/:.\-]+)`)

	// Path-only patterns: "/api/something" (quoted paths starting with /)
	pathRe = regexp.MustCompile(`["'](/[a-zA-Z0-9_/:.\-]{2,})["']`)

	// Path param normalizers
	colonParamRe = regexp.MustCompile(`:[a-zA-Z_]+`)
	braceParamRe = regexp.MustCompile(`\{[a-zA-Z_]+\}`)
)

// Run executes the HTTP linking pass.
func (l *Linker) Run() ([]HTTPLink, error) {
	proj, err := l.store.GetProject(l.project)
	if err != nil {
		return nil, fmt.Errorf("get project: %w", err)
	}
	rootPath := proj.RootPath

	routes := l.discoverRoutes(rootPath)
	slog.Info("httplink.routes", "count", len(routes))

	// Insert Route nodes and HANDLES edges
	l.insertRouteNodes(routes)

	callSites := l.discoverCallSites(rootPath)
	slog.Info("httplink.callsites", "count", len(callSites))

	links := l.matchAndLink(routes, callSites)
	slog.Info("httplink.links", "count", len(links))

	return links, nil
}

// insertRouteNodes creates Route nodes for each discovered route handler and
// HANDLES edges from the handler function to the Route node.
func (l *Linker) insertRouteNodes(routes []RouteHandler) {
	for _, rh := range routes {
		// Build a stable qualified name for the Route node
		normalMethod := rh.Method
		if normalMethod == "" {
			normalMethod = "ANY"
		}
		normalPath := strings.ReplaceAll(rh.Path, "/", "_")
		normalPath = strings.Trim(normalPath, "_")
		routeQN := rh.QualifiedName + ".route." + normalMethod + "." + normalPath

		routeName := normalMethod + " " + rh.Path

		routeID, err := l.store.UpsertNode(&store.Node{
			Project:       l.project,
			Label:         "Route",
			Name:          routeName,
			QualifiedName: routeQN,
			Properties: map[string]any{
				"method":  rh.Method,
				"path":    rh.Path,
				"handler": rh.QualifiedName,
			},
		})
		if err != nil || routeID == 0 {
			continue
		}

		// Create HANDLES edge from handler → Route
		handlerNode, _ := l.store.FindNodeByQN(l.project, rh.QualifiedName)
		if handlerNode != nil {
			l.store.InsertEdge(&store.Edge{
				Project:  l.project,
				SourceID: handlerNode.ID,
				TargetID: routeID,
				Type:     "HANDLES",
			})

			// Mark handler as entry point (for Feature 4)
			if handlerNode.Properties == nil {
				handlerNode.Properties = map[string]any{}
			}
			handlerNode.Properties["is_entry_point"] = true
			l.store.UpsertNode(handlerNode)
		}
	}
	slog.Info("httplink.route_nodes", "count", len(routes))
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

	for _, f := range funcs {
		// Python: check decorators property
		routes = append(routes, extractPythonRoutes(f)...)

		// Java: check annotation-based decorators (Spring)
		routes = append(routes, extractJavaRoutes(f)...)

		// Rust: check attribute decorators (Actix)
		routes = append(routes, extractRustRoutes(f)...)

		// Source-based route discovery (Go gin, Express.js, PHP Laravel)
		if f.FilePath != "" && f.StartLine > 0 && f.EndLine > 0 {
			source := readSourceLines(rootPath, f.FilePath, f.StartLine, f.EndLine)
			if source != "" {
				routes = append(routes, extractGoRoutes(f, source)...)
				routes = append(routes, extractExpressRoutes(f, source)...)
				routes = append(routes, extractLaravelRoutes(f, source)...)
			}
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
		matches := pyRouteRe.FindAllStringSubmatch(decStr, -1)
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

// extractGoRoutes extracts route registrations from Go source code (gin patterns).
func extractGoRoutes(f *store.Node, source string) []RouteHandler {
	var routes []RouteHandler

	matches := goRouteRe.FindAllStringSubmatch(source, -1)
	for _, m := range matches {
		routes = append(routes, RouteHandler{
			Path:          m[2],
			Method:        strings.ToUpper(m[1]),
			FunctionName:  f.Name,
			QualifiedName: f.QualifiedName,
		})
	}

	return routes
}

// extractExpressRoutes extracts route registrations from JS/TS source (Express/Koa patterns).
func extractExpressRoutes(f *store.Node, source string) []RouteHandler {
	var routes []RouteHandler
	matches := expressRouteRe.FindAllStringSubmatch(source, -1)
	for _, m := range matches {
		routes = append(routes, RouteHandler{
			Path:          m[2],
			Method:        strings.ToUpper(m[1]),
			FunctionName:  f.Name,
			QualifiedName: f.QualifiedName,
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
	var routes []RouteHandler
	matches := laravelRouteRe.FindAllStringSubmatch(source, -1)
	for _, m := range matches {
		routes = append(routes, RouteHandler{
			Path:          m[2],
			Method:        strings.ToUpper(m[1]),
			FunctionName:  f.Name,
			QualifiedName: f.QualifiedName,
		})
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
	// Generic
	"send_request", "http_client",
}

// extractFunctionCallSites extracts HTTP paths from function source code.
func extractFunctionCallSites(f *store.Node, rootPath string) []HTTPCallSite {
	var sites []HTTPCallSite

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

	// Require at least one HTTP client keyword to avoid false positives
	// from functions that merely store URL strings in variables
	hasHTTPClient := false
	for _, kw := range httpClientKeywords {
		if strings.Contains(source, kw) {
			hasHTTPClient = true
			break
		}
	}
	if !hasHTTPClient {
		return sites
	}

	method := detectHTTPMethod(source)

	paths := extractURLPaths(source)
	for _, p := range paths {
		sites = append(sites, HTTPCallSite{
			Path:                p,
			Method:              method,
			SourceName:          f.Name,
			SourceQualifiedName: f.QualifiedName,
			SourceLabel:         f.Label,
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
	var results []string
	for _, opener := range []byte{'{', '['} {
		closer := byte('}')
		if opener == '[' {
			closer = ']'
		}
		start := strings.IndexByte(text, opener)
		for start >= 0 && start < len(text) {
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
				if ch == '"' {
					inStr = true
				} else if ch == opener {
					depth++
				} else if ch == closer {
					depth--
					if depth == 0 {
						candidate := text[start : i+1]
						if len(candidate) > 5 { // skip trivially small
							results = append(results, candidate)
						}
						start = i + 1
						break
					}
				}
			}
			if depth != 0 {
				break
			}
			next := strings.IndexByte(text[start:], opener)
			if next < 0 {
				break
			}
			start += next
		}
	}
	return results
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

			// Multi-signal confidence scoring
			pathScore := pathMatchScore(cs.Path, rh.Path)
			if pathScore == 0 {
				continue
			}

			score := pathScore*sourceWeight(cs.SourceLabel) + methodBonus(cs.Method, rh.Method)
			if score < matchConfidenceThreshold {
				continue
			}
			if score > 1.0 {
				score = 1.0
			}

			// Create HTTP_CALLS edge with confidence score
			callerNode, _ := l.store.FindNodeByQN(l.project, cs.SourceQualifiedName)
			handlerNode, _ := l.store.FindNodeByQN(l.project, rh.QualifiedName)
			if callerNode != nil && handlerNode != nil {
				props := map[string]any{
					"url_path":   cs.Path,
					"confidence": score,
				}
				if rh.Method != "" {
					props["method"] = rh.Method
				}
				_, _ = l.store.InsertEdge(&store.Edge{
					Project:    l.project,
					SourceID:   callerNode.ID,
					TargetID:   handlerNode.ID,
					Type:       "HTTP_CALLS",
					Properties: props,
				})
			}

			links = append(links, HTTPLink{
				CallerQN:    cs.SourceQualifiedName,
				CallerLabel: cs.SourceLabel,
				HandlerQN:   rh.QualifiedName,
				URLPath:     cs.Path,
			})
		}
	}

	return links
}

// normalizePath normalizes a URL path for comparison.
func normalizePath(path string) string {
	path = strings.TrimRight(path, "/")
	path = colonParamRe.ReplaceAllString(path, "*")
	path = braceParamRe.ReplaceAllString(path, "*")
	return strings.ToLower(path)
}

// matchConfidenceThreshold is the minimum score for an HTTP_CALLS edge.
const matchConfidenceThreshold = 0.3

// pathMatchScore returns a confidence score (0.0–1.0) for how well callPath
// matches routePath. Returns 0 if no match.
//
// Multi-signal scoring (inspired by RAD/Code2DFD research):
//   confidence = matchBase × (0.5 × jaccard + 0.5 × depthFactor)
//
// Where:
//   matchBase:   exact=0.95, suffix=0.75, wildcard=0.55
//   jaccard:     segment Jaccard similarity (non-wildcard segments)
//   depthFactor: min(matched_segments / 3.0, 1.0) — longer paths = more specific
func pathMatchScore(callPath, routePath string) float64 {
	normCall := normalizePath(callPath)
	normRoute := normalizePath(routePath)

	if normCall == "" || normRoute == "" {
		return 0
	}

	// Determine structural match type
	var matchBase float64
	var matchedCallSegs, matchedRouteSegs []string

	if normCall == normRoute {
		matchBase = 0.95
		matchedCallSegs = splitSegments(normCall)
		matchedRouteSegs = splitSegments(normRoute)
	} else if strings.HasSuffix(normCall, normRoute) {
		matchBase = 0.75
		matchedCallSegs = splitSegments(normRoute) // use the route portion that matched
		matchedRouteSegs = splitSegments(normRoute)
	} else {
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
//          "myapp.docker-images.cloud-runs.svcB.routes.handler" → dir prefix "myapp.docker-images.cloud-runs.svcB"
//          → different prefix → different service → returns false
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

// readSourceLines reads specific lines from a file on disk.
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
