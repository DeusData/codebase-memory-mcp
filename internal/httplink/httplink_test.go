package httplink

import (
	"os"
	"path/filepath"
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
		{"/api/orders", "/api/orders", 0.78, 0.82},                  // jaccard=1.0, depth=2/3=0.667 → 0.95×0.833 ≈ 0.79
		{"/integrate", "/integrate", 0.60, 0.67},                     // jaccard=1.0, depth=1/3=0.333 → 0.95×0.667 ≈ 0.63
		{"/api/v1/orders/items", "/api/v1/orders/items", 0.93, 0.96}, // jaccard=1.0, depth=4/3→1.0 → 0.95×1.0 = 0.95

		// Suffix matches: matchBase=0.75
		{"https://host/api/orders", "/api/orders", 0.60, 0.66}, // jaccard=1.0, depth=0.667 → 0.75×0.833 ≈ 0.625

		// Wildcard matches: matchBase=0.55
		{"/api/orders/123", "/api/orders/:id", 0.43, 0.48}, // jaccard({api,orders,123}∩{api,orders})=2/3=0.667, depth=1.0 → 0.55×0.833 ≈ 0.458

		// No match
		{"/api/users", "/api/orders", 0.0, 0.0},
		{"/", "/api/orders", 0.0, 0.0},  // empty normalized
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
		{"a.b.c.mod.Func1", "a.b.c.mod.Func2", true},            // same dir (a.b.c)
		{"a.b.c.mod.Func1", "a.b.x.mod.Func2", false},           // different dir (a.b.c vs a.b.x)
		{"a.b.c.d.mod.Func", "a.b.c.d.mod.Other", true},         // same deep dir (a.b.c.d)
		{"a.b.c.d.mod.Func", "a.b.c.e.mod.Other", false},        // different deep dir
		{"short.x", "short.y", false},                             // only 2 segments → strip leaves empty → false
		{"a.b", "a.b", false},                                     // 2 segments → not enough to determine
		{"a.b.c", "a.b.c", true},                                  // 3 segments: dir="a", same
		{"a.b.c", "x.b.c", false},                                 // 3 segments: dir="a" vs "x"
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
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
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
`), 0o644); err != nil {
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
