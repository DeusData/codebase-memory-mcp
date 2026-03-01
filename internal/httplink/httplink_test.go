package httplink

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"/api/orders/", "/api/orders"},
		{"/api/orders", "/api/orders"},
		{"/api/orders/:id", "/api/orders/*"},
		{"/api/orders/{order_id}", "/api/orders/*"},
		{"/API/Orders", "/api/orders"},
		{"/api/:version/items/:id", "/api/*/items/*"},
		{"/api/{version}/items/{id}", "/api/*/items/*"},
		{"/", ""},
		{"", ""},
	}
	for _, tt := range tests {
		got := normalizePath(tt.input)
		if got != tt.want {
			t.Errorf("normalizePath(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestPathsMatch(t *testing.T) {
	tests := []struct {
		callPath  string
		routePath string
		want      bool
	}{
		// Exact match
		{"/api/orders", "/api/orders", true},
		{"/api/orders/", "/api/orders", true},

		// Case insensitive
		{"/API/Orders", "/api/orders", true},

		// Suffix match (call has host prefix, route is just path)
		{"https://example.com/api/orders", "/api/orders", true},
		{"/api/orders", "/api/orders", true},

		// Wildcard params
		{"/api/orders/:id", "/api/orders/{order_id}", true},
		{"/api/orders/123", "/api/orders/:id", true}, // 123 matches * (normalized :id)

		// Segment wildcard: :version normalizes to *, matches any segment
		{"/api/:version/items", "/api/v1/items", true},

		// Different lengths
		{"/api/orders", "/api/orders/detail", false},
		{"/api", "/api/orders", false},

		// Both have wildcards
		{"/api/*/items", "/api/*/items", true},

		// No match
		{"/api/users", "/api/orders", false},
	}
	for _, tt := range tests {
		got := pathsMatch(tt.callPath, tt.routePath)
		if got != tt.want {
			t.Errorf("pathsMatch(%q, %q) = %v, want %v", tt.callPath, tt.routePath, got, tt.want)
		}
	}
}

func TestPathsMatchSuffix(t *testing.T) {
	// Suffix match: normalized call path ends with normalized route path
	got := pathsMatch("/host/prefix/api/orders", "/api/orders")
	if !got {
		t.Error("expected suffix match for /host/prefix/api/orders -> /api/orders")
	}
}

func TestPathMatchScore(t *testing.T) {
	tests := []struct {
		call  string
		route string
		min   float64
		max   float64
	}{
		// Exact matches: matchBase=0.95, confidence = 0.95 × (0.5×jaccard + 0.5×depthFactor)
		{"/api/orders", "/api/orders", 0.78, 0.82},                   // jaccard=1.0, depth=2/3=0.667 → 0.95×0.833 ≈ 0.79
		{"/integrate", "/integrate", 0.60, 0.67},                     // jaccard=1.0, depth=1/3=0.333 → 0.95×0.667 ≈ 0.63
		{"/api/v1/orders/items", "/api/v1/orders/items", 0.93, 0.96}, // jaccard=1.0, depth=4/3→1.0 → 0.95×1.0 = 0.95

		// Suffix matches: matchBase=0.75
		{"https://host/api/orders", "/api/orders", 0.60, 0.66}, // jaccard=1.0, depth=0.667 → 0.75×0.833 ≈ 0.625

		// Numeric IDs normalized to wildcard → exact match with :id (also normalized to *)
		{"/api/orders/123", "/api/orders/:id", 0.90, 0.96}, // both normalize to /api/orders/* → exact match

		// No match
		{"/api/users", "/api/orders", 0.0, 0.0},
		{"/", "/api/orders", 0.0, 0.0}, // empty normalized
		{"", "/api/orders", 0.0, 0.0},
	}
	for _, tt := range tests {
		got := pathMatchScore(tt.call, tt.route)
		if got < tt.min || got > tt.max {
			t.Errorf("pathMatchScore(%q, %q) = %.2f, want [%.2f, %.2f]", tt.call, tt.route, got, tt.min, tt.max)
		}
	}
}

func TestSameService(t *testing.T) {
	tests := []struct {
		qn1  string
		qn2  string
		want bool
	}{
		// Full directory comparison: strip last 2 segments (module+name), compare rest
		// "a.b.c.mod.func" → dir="a.b.c", so same dir = same service
		{"a.b.c.mod.Func1", "a.b.c.mod.Func2", true},     // same dir (a.b.c)
		{"a.b.c.mod.Func1", "a.b.x.mod.Func2", false},    // different dir (a.b.c vs a.b.x)
		{"a.b.c.d.mod.Func", "a.b.c.d.mod.Other", true},  // same deep dir (a.b.c.d)
		{"a.b.c.d.mod.Func", "a.b.c.e.mod.Other", false}, // different deep dir
		{"short.x", "short.y", false},                    // only 2 segments → strip leaves empty → false
		{"a.b", "a.b", false},                            // 2 segments → not enough to determine
		{"a.b.c", "a.b.c", true},                         // 3 segments: dir="a", same
		{"a.b.c", "x.b.c", false},                        // 3 segments: dir="a" vs "x"
		// Realistic multi-service QN patterns
		{"myapp.docker-images.cloud-runs.order-service.main.Func", "myapp.docker-images.cloud-runs.order-service.handlers.Other", true},
		{"myapp.docker-images.cloud-runs.order-service.main.Func", "myapp.docker-images.cloud-runs.notification-service.main.health_check", false},
		{"myapp.docker-images.cloud-runs.svcA.sub.mod.Func", "myapp.docker-images.cloud-runs.svcA.sub.mod.Other", true},
		{"myapp.docker-images.cloud-runs.svcA.sub.mod.Func", "myapp.docker-images.cloud-runs.svcB.sub.mod.Other", false},
	}
	for _, tt := range tests {
		got := sameService(tt.qn1, tt.qn2)
		if got != tt.want {
			t.Errorf("sameService(%q, %q) = %v, want %v", tt.qn1, tt.qn2, got, tt.want)
		}
	}
}

func TestExtractURLPaths(t *testing.T) {
	tests := []struct {
		text string
		want int // expected number of paths
	}{
		{`URL = "https://example.com/api/orders"`, 1},
		{`fetch("http://host/api/v1/items")`, 1},
		{`path = "/api/orders"`, 1},
		{`no urls here`, 0},
		{`both = "https://a.com/api/x" and "/api/y"`, 2},
	}
	for _, tt := range tests {
		got := extractURLPaths(tt.text)
		if len(got) != tt.want {
			t.Errorf("extractURLPaths(%q) returned %d paths, want %d: %v", tt.text, len(got), tt.want, got)
		}
	}
}

func TestExtractPythonRoutes(t *testing.T) {
	node := &store.Node{
		Name:          "create_order",
		QualifiedName: "proj.api.routes.create_order",
		Properties: map[string]any{
			"decorators": []any{
				`@app.post("/api/orders")`,
			},
		},
	}

	routes := extractPythonRoutes(node)
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	if routes[0].Path != "/api/orders" {
		t.Errorf("path = %q, want /api/orders", routes[0].Path)
	}
	if routes[0].Method != "POST" {
		t.Errorf("method = %q, want POST", routes[0].Method)
	}
	if routes[0].QualifiedName != "proj.api.routes.create_order" {
		t.Errorf("qn = %q, want proj.api.routes.create_order", routes[0].QualifiedName)
	}
}

func TestExtractPythonRoutesMultiple(t *testing.T) {
	node := &store.Node{
		Name:          "handler",
		QualifiedName: "proj.api.handler",
		Properties: map[string]any{
			"decorators": []any{
				`@router.get("/api/items/{item_id}")`,
				`@router.post("/api/items")`,
			},
		},
	}

	routes := extractPythonRoutes(node)
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}
}

func TestExtractPythonRoutesNoDecorators(t *testing.T) {
	node := &store.Node{
		Name:          "helper",
		QualifiedName: "proj.utils.helper",
		Properties:    map[string]any{},
	}

	routes := extractPythonRoutes(node)
	if len(routes) != 0 {
		t.Errorf("expected 0 routes, got %d", len(routes))
	}
}

func TestExtractGoRoutes(t *testing.T) {
	source := `
		r.POST("/api/orders", h.CreateOrder)
		r.GET("/api/orders/:id", h.GetOrder)
	`
	node := &store.Node{
		Name:          "RegisterRoutes",
		QualifiedName: "proj.api.RegisterRoutes",
	}

	routes := extractGoRoutes(node, source)
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}
	if routes[0].Path != "/api/orders" {
		t.Errorf("route[0].Path = %q, want /api/orders", routes[0].Path)
	}
	if routes[0].Method != "POST" {
		t.Errorf("route[0].Method = %q, want POST", routes[0].Method)
	}
	if routes[1].Path != "/api/orders/:id" {
		t.Errorf("route[1].Path = %q, want /api/orders/:id", routes[1].Path)
	}
}

func TestReadSourceLines(t *testing.T) {
	dir, err := os.MkdirTemp("", "httplink-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	content := "line1\nline2\nline3\nline4\nline5\n"
	path := filepath.Join(dir, "test.go")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	got := readSourceLines(dir, "test.go", 2, 4)
	want := "line2\nline3\nline4"
	if got != want {
		t.Errorf("readSourceLines = %q, want %q", got, want)
	}
}

func TestReadSourceLinesMissingFile(t *testing.T) {
	got := readSourceLines("/nonexistent", "missing.go", 1, 10)
	if got != "" {
		t.Errorf("expected empty string for missing file, got %q", got)
	}
}

func TestLinkerRun(t *testing.T) {
	// Set up a temp directory with a Python route handler and a Go caller
	dir, err := os.MkdirTemp("", "httplink-e2e-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Write a Go file that contains a URL constant
	goDir := filepath.Join(dir, "caller")
	if err := os.MkdirAll(goDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(goDir, "client.go"), []byte(`package caller
const OrderURL = "https://api.example.com/api/orders"
`), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "testproj"
	if err := s.UpsertProject(project, dir); err != nil {
		t.Fatal(err)
	}

	// Create a Module node with constants containing a URL
	callerID, _ := s.UpsertNode(&store.Node{
		Project:       project,
		Label:         "Module",
		Name:          "client.go",
		QualifiedName: "testproj.caller.client",
		FilePath:      "caller/client.go",
		Properties: map[string]any{
			"constants": []any{`OrderURL = "https://api.example.com/api/orders"`},
		},
	})

	// Create a Function node with a Python route decorator
	handlerID, _ := s.UpsertNode(&store.Node{
		Project:       project,
		Label:         "Function",
		Name:          "create_order",
		QualifiedName: "testproj.handler.routes.create_order",
		FilePath:      "handler/routes.py",
		Properties: map[string]any{
			"decorators": []any{`@app.post("/api/orders")`},
		},
	})

	if callerID == 0 || handlerID == 0 {
		t.Fatal("failed to create test nodes")
	}

	linker := New(s, project)
	links, err := linker.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(links) == 0 {
		t.Fatal("expected at least 1 HTTP link, got 0")
	}

	// Verify the link
	found := false
	for _, link := range links {
		if link.CallerQN == "testproj.caller.client" && link.HandlerQN == "testproj.handler.routes.create_order" {
			found = true
			t.Logf("link: %s -> %s (path=%s)", link.CallerQN, link.HandlerQN, link.URLPath)
		}
	}
	if !found {
		t.Error("expected link from testproj.caller.client to testproj.handler.routes.create_order")
		for _, link := range links {
			t.Logf("  got: %s -> %s", link.CallerQN, link.HandlerQN)
		}
	}

	// Verify edge was created in store
	callerNode, _ := s.FindNodeByQN(project, "testproj.caller.client")
	if callerNode == nil {
		t.Fatal("caller node not found")
	}
	edges, _ := s.FindEdgesBySourceAndType(callerNode.ID, "HTTP_CALLS")
	if len(edges) == 0 {
		t.Error("expected HTTP_CALLS edge in store, got 0")
	}
}

func TestExtractJSONStringPaths(t *testing.T) {
	tests := []struct {
		name string
		text string
		want int
	}{
		{
			name: "JSON object with URL",
			text: `BODY = '{"target": "https://api.internal.com/api/orders", "method": "POST"}'`,
			want: 1, // /api/orders
		},
		{
			name: "JSON object with path",
			text: `CONFIG = {"endpoint": "/api/v1/process", "timeout": 30}`,
			want: 1, // /api/v1/process
		},
		{
			name: "no JSON",
			text: `plain string without json`,
			want: 0,
		},
		{
			name: "nested JSON with URL",
			text: `{"services": [{"url": "https://svc.example.com/api/health"}]}`,
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractJSONStringPaths(tt.text)
			if len(got) != tt.want {
				t.Errorf("extractJSONStringPaths(%q) returned %d paths, want %d: %v", tt.text, len(got), tt.want, got)
			}
		})
	}
}

func TestRouteNodesCreated(t *testing.T) {
	dir, err := os.MkdirTemp("", "httplink-route-nodes-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "testproj"
	if err := s.UpsertProject(project, dir); err != nil {
		t.Fatal(err)
	}

	// Create a Function node with a Python route decorator
	_, _ = s.UpsertNode(&store.Node{
		Project:       project,
		Label:         "Function",
		Name:          "create_order",
		QualifiedName: "testproj.handler.routes.create_order",
		FilePath:      "handler/routes.py",
		Properties: map[string]any{
			"decorators": []any{`@app.post("/api/orders")`},
		},
	})

	linker := New(s, project)
	_, err = linker.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify Route node was created
	routeNodes, _ := s.FindNodesByLabel(project, "Route")
	if len(routeNodes) != 1 {
		t.Fatalf("expected 1 Route node, got %d", len(routeNodes))
	}
	rn := routeNodes[0]
	if rn.FilePath != "handler/routes.py" {
		t.Errorf("Route file_path = %q, want 'handler/routes.py'", rn.FilePath)
	}
	if rn.Name != "POST /api/orders" {
		t.Errorf("Route name = %q, want 'POST /api/orders'", rn.Name)
	}
	if rn.Properties["method"] != "POST" {
		t.Errorf("Route method = %v, want POST", rn.Properties["method"])
	}
	if rn.Properties["path"] != "/api/orders" {
		t.Errorf("Route path = %v, want /api/orders", rn.Properties["path"])
	}

	// Verify HANDLES edge from handler → Route
	handlerNode, _ := s.FindNodeByQN(project, "testproj.handler.routes.create_order")
	if handlerNode == nil {
		t.Fatal("handler node not found")
	}
	edges, _ := s.FindEdgesBySourceAndType(handlerNode.ID, "HANDLES")
	if len(edges) != 1 {
		t.Errorf("expected 1 HANDLES edge, got %d", len(edges))
	}

	// Verify handler marked as entry point
	if handlerNode.Properties["is_entry_point"] != true {
		t.Error("expected handler to be marked as is_entry_point")
	}
}

func TestCrossFileGroupPrefix(t *testing.T) {
	dir, err := os.MkdirTemp("", "httplink-crossfile-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Write a two-file Go project: main.go calls RegisterRoutes(v1.Group("/api"))
	if err := os.MkdirAll(filepath.Join(dir, "cmd"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "routes"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "cmd", "main.go"), []byte(`package main

func setup(r *gin.Engine) {
	RegisterRoutes(r.Group("/api"))
}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "routes", "routes.go"), []byte(`package routes

func RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/orders", ListOrders)
	rg.POST("/orders", CreateOrder)
}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "testproj"
	if err := s.UpsertProject(project, dir); err != nil {
		t.Fatal(err)
	}

	// Create function nodes that simulate what the pipeline would create
	setupID, _ := s.UpsertNode(&store.Node{
		Project:       project,
		Label:         "Function",
		Name:          "setup",
		QualifiedName: "testproj.cmd.main.setup",
		FilePath:      "cmd/main.go",
		StartLine:     3,
		EndLine:       5,
	})

	regID, _ := s.UpsertNode(&store.Node{
		Project:       project,
		Label:         "Function",
		Name:          "RegisterRoutes",
		QualifiedName: "testproj.routes.routes.RegisterRoutes",
		FilePath:      "routes/routes.go",
		StartLine:     3,
		EndLine:       6,
	})

	// Create CALLS edge: setup -> RegisterRoutes (as pipeline pass3 would)
	if _, err := s.InsertEdge(&store.Edge{
		Project:  project,
		SourceID: setupID,
		TargetID: regID,
		Type:     "CALLS",
	}); err != nil {
		t.Fatal(err)
	}

	linker := New(s, project)
	_, err = linker.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify Route nodes have the cross-file prefix /api prepended
	routeNodes, _ := s.FindNodesByLabel(project, "Route")
	if len(routeNodes) != 2 {
		t.Fatalf("expected 2 Route nodes, got %d", len(routeNodes))
	}

	foundPaths := map[string]bool{}
	for _, rn := range routeNodes {
		path, _ := rn.Properties["path"].(string)
		foundPaths[path] = true
		t.Logf("Route: %s (path=%s)", rn.Name, path)
	}

	if !foundPaths["/api/orders"] {
		t.Error("expected route path /api/orders with cross-file prefix")
	}
}

func TestCrossFileGroupPrefixVariable(t *testing.T) {
	dir, err := os.MkdirTemp("", "httplink-crossfile-var-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	if err := os.MkdirAll(filepath.Join(dir, "cmd"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "routes"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Variable-based pattern: v1 := r.Group("/api"); RegisterRoutes(v1)
	if err := os.WriteFile(filepath.Join(dir, "cmd", "main.go"), []byte(`package main

func setup(r *gin.Engine) {
	v1 := r.Group("/api")
	RegisterRoutes(v1)
}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "routes", "routes.go"), []byte(`package routes

func RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/items", ListItems)
}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "testproj"
	if err := s.UpsertProject(project, dir); err != nil {
		t.Fatal(err)
	}

	setupID, _ := s.UpsertNode(&store.Node{
		Project: project, Label: "Function", Name: "setup",
		QualifiedName: "testproj.cmd.main.setup",
		FilePath:      "cmd/main.go", StartLine: 3, EndLine: 6,
	})

	regID, _ := s.UpsertNode(&store.Node{
		Project: project, Label: "Function", Name: "RegisterRoutes",
		QualifiedName: "testproj.routes.routes.RegisterRoutes",
		FilePath:      "routes/routes.go", StartLine: 3, EndLine: 5,
	})

	if _, err := s.InsertEdge(&store.Edge{
		Project: project, SourceID: setupID, TargetID: regID, Type: "CALLS",
	}); err != nil {
		t.Fatal(err)
	}

	linker := New(s, project)
	if _, runErr := linker.Run(); runErr != nil {
		t.Fatal(runErr)
	}

	routeNodes, _ := s.FindNodesByLabel(project, "Route")
	if len(routeNodes) != 1 {
		t.Fatalf("expected 1 Route node, got %d", len(routeNodes))
	}

	path, _ := routeNodes[0].Properties["path"].(string)
	if path != "/api/items" {
		t.Errorf("expected /api/items, got %s", path)
	}
}

func TestRouteRegistrationCallEdges(t *testing.T) {
	dir, err := os.MkdirTemp("", "httplink-reg-edges-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	if err := os.MkdirAll(filepath.Join(dir, "routes"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "routes", "routes.go"), []byte(`package routes

func RegisterRoutes(rg *gin.RouterGroup) {
	rg.POST("/orders", h.CreateOrder)
	rg.GET("/orders/:id", h.GetOrder)
}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "testproj"
	if err := s.UpsertProject(project, dir); err != nil {
		t.Fatal(err)
	}

	// Create the registering function
	if _, err := s.UpsertNode(&store.Node{
		Project: project, Label: "Function", Name: "RegisterRoutes",
		QualifiedName: "testproj.routes.routes.RegisterRoutes",
		FilePath:      "routes/routes.go", StartLine: 3, EndLine: 6,
	}); err != nil {
		t.Fatal(err)
	}

	// Create handler functions (as pipeline would)
	if _, err := s.UpsertNode(&store.Node{
		Project: project, Label: "Method", Name: "CreateOrder",
		QualifiedName: "testproj.handlers.handler.CreateOrder",
		FilePath:      "handlers/handler.go", StartLine: 10, EndLine: 30,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertNode(&store.Node{
		Project: project, Label: "Method", Name: "GetOrder",
		QualifiedName: "testproj.handlers.handler.GetOrder",
		FilePath:      "handlers/handler.go", StartLine: 32, EndLine: 50,
	}); err != nil {
		t.Fatal(err)
	}

	linker := New(s, project)
	if _, runErr := linker.Run(); runErr != nil {
		t.Fatal(runErr)
	}

	// Verify CALLS edges from RegisterRoutes to handlers
	regNode, _ := s.FindNodeByQN(project, "testproj.routes.routes.RegisterRoutes")
	if regNode == nil {
		t.Fatal("RegisterRoutes node not found")
	}

	edges, _ := s.FindEdgesBySourceAndType(regNode.ID, "CALLS")
	if len(edges) < 2 {
		t.Errorf("expected at least 2 CALLS edges from RegisterRoutes, got %d", len(edges))
	}

	// Verify that CreateOrder is a target
	createNode, _ := s.FindNodeByQN(project, "testproj.handlers.handler.CreateOrder")
	if createNode == nil {
		t.Fatal("CreateOrder not found")
	}
	foundCreate := false
	for _, e := range edges {
		if e.TargetID == createNode.ID {
			foundCreate = true
			// Check the via property
			if via, ok := e.Properties["via"]; ok {
				if via != "route_registration" {
					t.Errorf("expected via=route_registration, got %v", via)
				}
			}
		}
	}
	if !foundCreate {
		t.Error("expected CALLS edge from RegisterRoutes to CreateOrder")
	}
}

func TestAsyncDispatchKeywords(t *testing.T) {
	dir, err := os.MkdirTemp("", "httplink-async-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	writeTestFile(t, dir, "taskworker", "dispatch.go", `package taskworker

func DispatchOrder(orderID string) {
	url := "https://api.internal.com/api/orders"
	client.CreateTask(ctx, url, payload)
}
`)
	writeTestFile(t, dir, "synccaller", "caller.go", `package synccaller

func CallOrder() {
	url := "https://api.internal.com/api/orders"
	requests.post(url, data)
}
`)
	writeTestFile(t, dir, "bothcaller", "both.go", `package bothcaller

func CallAndDispatch() {
	url := "https://api.internal.com/api/orders"
	requests.post(url, data)
	client.CreateTask(ctx, url, payload)
}
`)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "testproj"
	if err := s.UpsertProject(project, dir); err != nil {
		t.Fatal(err)
	}

	createTestNode(t, s, project, "DispatchOrder", "testproj.taskworker.dispatch.DispatchOrder", "taskworker/dispatch.go", 3, 6)
	createTestNode(t, s, project, "CallOrder", "testproj.synccaller.caller.CallOrder", "synccaller/caller.go", 3, 6)
	createTestNode(t, s, project, "CallAndDispatch", "testproj.bothcaller.both.CallAndDispatch", "bothcaller/both.go", 3, 7)

	_, _ = s.UpsertNode(&store.Node{
		Project: project, Label: "Function", Name: "create_order",
		QualifiedName: "testproj.handler.routes.create_order",
		FilePath:      "handler/routes.py",
		Properties:    map[string]any{"decorators": []any{`@app.post("/api/orders")`}},
	})

	linker := New(s, project)
	links, err := linker.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	edgeTypes := map[string]string{}
	for _, link := range links {
		edgeTypes[link.CallerQN] = link.EdgeType
	}

	assertEdgeType(t, edgeTypes, "testproj.taskworker.dispatch.DispatchOrder", "ASYNC_CALLS")
	assertEdgeType(t, edgeTypes, "testproj.synccaller.caller.CallOrder", "HTTP_CALLS")
	assertEdgeType(t, edgeTypes, "testproj.bothcaller.both.CallAndDispatch", "HTTP_CALLS")

	assertStoredEdgeCounts(t, s, project, "testproj.taskworker.dispatch.DispatchOrder", 1, 0)
	assertStoredEdgeCounts(t, s, project, "testproj.synccaller.caller.CallOrder", 0, 1)
}

func writeTestFile(t *testing.T, dir, subdir, filename, content string) {
	t.Helper()
	d := filepath.Join(dir, subdir)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, filename), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func createTestNode(t *testing.T, s *store.Store, project, name, qn, filePath string, startLine, endLine int) {
	t.Helper()
	_, err := s.UpsertNode(&store.Node{
		Project: project, Label: "Function", Name: name,
		QualifiedName: qn, FilePath: filePath,
		StartLine: startLine, EndLine: endLine,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertEdgeType(t *testing.T, edgeTypes map[string]string, qn, wantType string) {
	t.Helper()
	et, ok := edgeTypes[qn]
	if !ok {
		t.Errorf("expected link from %s", qn)
		return
	}
	if et != wantType {
		t.Errorf("%s edge type = %q, want %q", qn, et, wantType)
	}
}

func assertStoredEdgeCounts(t *testing.T, s *store.Store, project, qn string, wantAsync, wantHTTP int) {
	t.Helper()
	node, _ := s.FindNodeByQN(project, qn)
	if node == nil {
		t.Errorf("node not found: %s", qn)
		return
	}
	asyncEdges, _ := s.FindEdgesBySourceAndType(node.ID, "ASYNC_CALLS")
	if len(asyncEdges) != wantAsync {
		t.Errorf("%s: ASYNC_CALLS edges = %d, want %d", qn, len(asyncEdges), wantAsync)
	}
	httpEdges, _ := s.FindEdgesBySourceAndType(node.ID, "HTTP_CALLS")
	if len(httpEdges) != wantHTTP {
		t.Errorf("%s: HTTP_CALLS edges = %d, want %d", qn, len(httpEdges), wantHTTP)
	}
}

func TestExtractFunctionCallSitesAsync(t *testing.T) {
	// Test extractFunctionCallSites directly with a temp file containing async keywords.
	dir, err := os.MkdirTemp("", "httplink-extract-async-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Write a Go file with CreateTask and a URL
	if err := os.MkdirAll(filepath.Join(dir, "worker"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "worker", "task.go"), []byte(`package worker

func EnqueueJob(ctx context.Context) {
	url := "https://backend.internal.com/api/process"
	client.CreateTask(ctx, &taskspb.CreateTaskRequest{
		HttpRequest: &taskspb.HttpRequest{Url: url},
	})
}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	node := &store.Node{
		Project: "testproj", Label: "Function", Name: "EnqueueJob",
		QualifiedName: "testproj.worker.task.EnqueueJob",
		FilePath:      "worker/task.go", StartLine: 3, EndLine: 7,
	}

	sites := extractFunctionCallSites(node, dir)
	if len(sites) == 0 {
		t.Fatal("expected at least 1 call site, got 0")
	}

	foundAsync := false
	for _, s := range sites {
		if s.IsAsync {
			foundAsync = true
			if s.Path != "/api/process" {
				t.Errorf("async site path = %q, want /api/process", s.Path)
			}
		}
	}
	if !foundAsync {
		t.Error("expected at least one call site with IsAsync=true")
	}

	// Also test that a function with only sync keywords gets IsAsync=false
	if err := os.WriteFile(filepath.Join(dir, "worker", "sync.go"), []byte(`package worker

func SyncCall(ctx context.Context) {
	url := "https://backend.internal.com/api/process"
	requests.post(url, data)
}
`), 0o600); err != nil {
		t.Fatal(err)
	}

	syncNode := &store.Node{
		Project: "testproj", Label: "Function", Name: "SyncCall",
		QualifiedName: "testproj.worker.sync.SyncCall",
		FilePath:      "worker/sync.go", StartLine: 3, EndLine: 6,
	}

	syncSites := extractFunctionCallSites(syncNode, dir)
	if len(syncSites) == 0 {
		t.Fatal("expected at least 1 sync call site, got 0")
	}
	for _, s := range syncSites {
		if s.IsAsync {
			t.Errorf("sync call site should have IsAsync=false, got true (path=%s)", s.Path)
		}
	}
}

func TestLinkerSkipsSameService(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	dir, err := os.MkdirTemp("", "httplink-same-svc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	project := "testproj"
	if err := s.UpsertProject(project, dir); err != nil {
		t.Fatal(err)
	}

	// Both in the same service (same first 4 QN segments: testproj.cat.sub.svcA)
	_, _ = s.UpsertNode(&store.Node{
		Project:       project,
		Label:         "Module",
		Name:          "client.py",
		QualifiedName: "testproj.cat.sub.svcA.internal.client",
		FilePath:      "cat/sub/svcA/internal/client.py",
		Properties: map[string]any{
			"constants": []any{`URL = "https://localhost/api/orders"`},
		},
	})

	_, _ = s.UpsertNode(&store.Node{
		Project:       project,
		Label:         "Function",
		Name:          "handle_orders",
		QualifiedName: "testproj.cat.sub.svcA.internal.handle_orders",
		FilePath:      "cat/sub/svcA/internal/routes.py",
		Properties: map[string]any{
			"decorators": []any{`@app.get("/api/orders")`},
		},
	})

	linker := New(s, project)
	links, err := linker.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(links) != 0 {
		t.Errorf("expected 0 links (same service), got %d", len(links))
		for _, l := range links {
			t.Logf("  %s -> %s", l.CallerQN, l.HandlerQN)
		}
	}
}

func TestDetectProtocol(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{"go websocket upgrade", `err := websocket.Upgrade(w, r, nil, 1024, 1024)`, "ws"},
		{"go websocket accept", `conn, err := websocket.Accept(w, r, nil)`, "ws"},
		{"go upgrader", `conn, err := upgrader.Upgrade(w, r, nil)`, "ws"},
		{"js ws", `ws.on("connection", func)`, "ws"},
		{"js socketio", `io.on("connection", handler)`, "ws"},
		{"sse content type", `w.Header().Set("Content-Type", "text/event-stream")`, "sse"},
		{"python sse", `return EventSourceResponse(generate())`, "sse"},
		{"java sse emitter", `SseEmitter emitter = new SseEmitter()`, "sse"},
		{"java sse event", `ServerSentEvent event = ServerSentEvent.builder()`, "sse"},
		{"no protocol", `return json.Marshal(result)`, ""},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectProtocol(tt.source)
			if got != tt.want {
				t.Errorf("detectProtocol() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractPythonWSRoutes(t *testing.T) {
	node := &store.Node{
		Name:          "ws_handler",
		QualifiedName: "proj.api.ws_handler",
		Properties: map[string]any{
			"decorators": []any{
				`@app.websocket("/ws/chat")`,
			},
		},
	}

	routes := extractPythonRoutes(node)
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	if routes[0].Path != "/ws/chat" {
		t.Errorf("path = %q, want /ws/chat", routes[0].Path)
	}
	if routes[0].Method != "WS" {
		t.Errorf("method = %q, want WS", routes[0].Method)
	}
	if routes[0].Protocol != "ws" {
		t.Errorf("protocol = %q, want ws", routes[0].Protocol)
	}
}

func TestExtractSpringWSRoutes(t *testing.T) {
	node := &store.Node{
		Name:          "handleChat",
		QualifiedName: "proj.ChatController.handleChat",
		Properties: map[string]any{
			"decorators": []any{
				`@MessageMapping("/chat")`,
			},
		},
	}

	routes := extractJavaRoutes(node)
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	if routes[0].Path != "/chat" {
		t.Errorf("path = %q, want /chat", routes[0].Path)
	}
	if routes[0].Method != "WS" {
		t.Errorf("method = %q, want WS", routes[0].Method)
	}
	if routes[0].Protocol != "ws" {
		t.Errorf("protocol = %q, want ws", routes[0].Protocol)
	}
}

func TestExtractKtorWSRoutes(t *testing.T) {
	source := `
	webSocket("/chat") {
		for (frame in incoming) {
			send(frame)
		}
	}
	get("/api/health") {
		call.respond("ok")
	}
`
	node := &store.Node{
		Name:          "configureRouting",
		QualifiedName: "proj.Routing.configureRouting",
	}

	routes := extractKtorRoutes(node, source)
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	// Check WS route
	wsFound := false
	httpFound := false
	for _, r := range routes {
		if r.Protocol == "ws" && r.Path == "/chat" && r.Method == "WS" {
			wsFound = true
		}
		if r.Path == "/api/health" && r.Method == "GET" {
			httpFound = true
		}
	}
	if !wsFound {
		t.Error("expected WS route for /chat")
	}
	if !httpFound {
		t.Error("expected HTTP route for /api/health")
	}
}

func TestChiPrefix(t *testing.T) {
	source := `
func SetupRoutes(r chi.Router) {
	r.Route("/api", func(r chi.Router) {
		r.Get("/health", healthHandler)
		r.Route("/users", func(r chi.Router) {
			r.Get("/", listUsers)
			r.Post("/{id}", updateUser)
		})
	})
}
`
	node := &store.Node{
		Name:          "SetupRoutes",
		QualifiedName: "proj.SetupRoutes",
	}

	routes := extractGoRoutes(node, source)

	expectedPaths := map[string]string{
		"/api/health":     "GET",
		"/api/users":      "GET",
		"/api/users/{id}": "POST",
	}

	if len(routes) != len(expectedPaths) {
		t.Fatalf("expected %d routes, got %d", len(expectedPaths), len(routes))
		for _, r := range routes {
			t.Logf("  %s %s", r.Method, r.Path)
		}
	}

	for _, r := range routes {
		wantMethod, ok := expectedPaths[r.Path]
		if !ok {
			t.Errorf("unexpected route: %s %s", r.Method, r.Path)
			continue
		}
		if r.Method != wantMethod {
			t.Errorf("route %s: method = %q, want %q", r.Path, r.Method, wantMethod)
		}
	}
}

func TestChiPrefixMixedWithGin(t *testing.T) {
	// When no chi Route() blocks, gin group resolution should still work
	source := `
func RegisterRoutes(r *gin.RouterGroup) {
	orders := r.Group("/orders")
	orders.GET("/:id", getOrder)
	orders.POST("", createOrder)
}
`
	node := &store.Node{
		Name:          "RegisterRoutes",
		QualifiedName: "proj.RegisterRoutes",
	}

	routes := extractGoRoutes(node, source)
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	for _, r := range routes {
		if !strings.HasPrefix(r.Path, "/orders") {
			t.Errorf("expected /orders prefix, got %s", r.Path)
		}
	}
}

func TestFastAPIPrefix(t *testing.T) {
	dir, err := os.MkdirTemp("", "httplink-fastapi-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Write Python files: main.py with include_router, orders/routes.py with routes
	if err := os.MkdirAll(filepath.Join(dir, "orders"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "main.py"), []byte(`
from orders.routes import order_router

app = FastAPI()
app.include_router(order_router, prefix="/api/v1/orders")
`), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "testproj"
	if err := s.UpsertProject(project, dir); err != nil {
		t.Fatal(err)
	}

	// Create Module for main.py (has the include_router call)
	_, _ = s.UpsertNode(&store.Node{
		Project: project, Label: "Module", Name: "main.py",
		QualifiedName: "testproj//main.py",
		FilePath:      "main.py",
	})

	// Create Function with route in the orders module
	_, _ = s.UpsertNode(&store.Node{
		Project: project, Label: "Function", Name: "list_orders",
		QualifiedName: "testproj//orders/routes.py/list_orders",
		FilePath:      "orders/routes.py",
		Properties: map[string]any{
			"decorators": []any{`@order_router.get("/")`},
		},
	})

	linker := New(s, project)
	_, err = linker.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	routeNodes, _ := s.FindNodesByLabel(project, "Route")
	if len(routeNodes) != 1 {
		t.Fatalf("expected 1 Route node, got %d", len(routeNodes))
	}

	path, _ := routeNodes[0].Properties["path"].(string)
	if path != "/api/v1/orders/" {
		t.Errorf("expected /api/v1/orders/, got %s", path)
	}
}

func TestExpressPrefix(t *testing.T) {
	dir, err := os.MkdirTemp("", "httplink-express-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	if err := os.MkdirAll(filepath.Join(dir, "routes"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "app.js"), []byte(`
const orderRouter = require('./routes/orders');
app.use("/api/orders", orderRouter);
`), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "testproj"
	if err := s.UpsertProject(project, dir); err != nil {
		t.Fatal(err)
	}

	// Module for app.js
	_, _ = s.UpsertNode(&store.Node{
		Project: project, Label: "Module", Name: "app.js",
		QualifiedName: "testproj//app.js",
		FilePath:      "app.js",
	})

	// Function with route in orders module
	_, _ = s.UpsertNode(&store.Node{
		Project: project, Label: "Function", Name: "getOrder",
		QualifiedName: "testproj//routes/orders.js/getOrder",
		FilePath:      "routes/orders.js",
		StartLine:     1, EndLine: 5,
	})

	// Write the routes file with Express route
	if err := os.WriteFile(filepath.Join(dir, "routes", "orders.js"), []byte(`
router.get("/:id", function(req, res) {
	res.json({id: req.params.id});
});
`), 0o600); err != nil {
		t.Fatal(err)
	}

	linker := New(s, project)
	_, err = linker.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	routeNodes, _ := s.FindNodesByLabel(project, "Route")
	if len(routeNodes) != 1 {
		t.Fatalf("expected 1 Route node, got %d", len(routeNodes))
	}

	path, _ := routeNodes[0].Properties["path"].(string)
	if path != "/api/orders/:id" {
		t.Errorf("expected /api/orders/:id, got %s", path)
	}
}

func TestExpressRouteFiltering(t *testing.T) {
	tests := []struct {
		name       string
		line       string
		wantMatch  bool
		wantMethod string
		wantPath   string
	}{
		// Should match (allowlisted receivers)
		{"app.get", `app.get('/api/users', handler)`, true, "GET", "/api/users"},
		{"router.post", `router.post('/orders', handler)`, true, "POST", "/orders"},
		{"server.put", `server.put('/items', handler)`, true, "PUT", "/items"},
		{"api.delete", `api.delete('/users/:id', handler)`, true, "DELETE", "/users/:id"},
		{"routes.patch", `routes.patch('/items/:id', handler)`, true, "PATCH", "/items/:id"},
		// Should NOT match (not in allowlist)
		{"req.get", `req.get('Content-Type')`, false, "", ""},
		{"res.get", `res.get('key')`, false, "", ""},
		{"this.get", `this.get('property')`, false, "", ""},
		{"map.get", `map.get('key')`, false, "", ""},
		{"model.delete", `model.delete('record')`, false, "", ""},
		{"params.get", `params.get('id')`, false, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &store.Node{
				Name:          "testFunc",
				QualifiedName: "proj.test.testFunc",
			}
			routes := extractExpressRoutes(node, tt.line)
			if tt.wantMatch {
				if len(routes) == 0 {
					t.Errorf("expected route match, got 0 routes")
					return
				}
				if routes[0].Method != tt.wantMethod {
					t.Errorf("method = %q, want %q", routes[0].Method, tt.wantMethod)
				}
				if routes[0].Path != tt.wantPath {
					t.Errorf("path = %q, want %q", routes[0].Path, tt.wantPath)
				}
			} else if len(routes) > 0 {
				t.Errorf("expected no match, got %d routes: %v", len(routes), routes[0].Path)
			}
		})
	}
}

func TestLaravelModuleLevelRoutes(t *testing.T) {
	dir, err := os.MkdirTemp("", "httplink-laravel-module-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create a Laravel-style route file with module-level Route:: calls
	if err := os.MkdirAll(filepath.Join(dir, "routes"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "routes", "api.php"), []byte(`<?php

use App\Http\Controllers\OrderController;

Route::get('/api/orders', [OrderController::class, 'index']);
Route::post('/api/orders', [OrderController::class, 'store']);
Route::get('/api/orders/{id}', [OrderController::class, 'show']);
`), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	project := "testproj"
	if err := s.UpsertProject(project, dir); err != nil {
		t.Fatal(err)
	}

	// Create Module node for the PHP route file (as pipeline would)
	// No Function/Method nodes — routes are at module level
	_, _ = s.UpsertNode(&store.Node{
		Project:       project,
		Label:         "Module",
		Name:          "api.php",
		QualifiedName: "testproj.routes.api",
		FilePath:      "routes/api.php",
	})

	linker := New(s, project)
	_, err = linker.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	routeNodes, _ := s.FindNodesByLabel(project, "Route")
	if len(routeNodes) < 3 {
		t.Fatalf("expected at least 3 Route nodes from module-level Laravel routes, got %d", len(routeNodes))
	}

	foundPaths := map[string]bool{}
	for _, rn := range routeNodes {
		path, _ := rn.Properties["path"].(string)
		foundPaths[path] = true
		t.Logf("Route: %s (path=%s)", rn.Name, path)
	}

	for _, wantPath := range []string{"/api/orders", "/api/orders/{id}"} {
		if !foundPaths[wantPath] {
			t.Errorf("expected route path %s", wantPath)
		}
	}
}
