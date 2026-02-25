package store

import (
	"fmt"
	"testing"
)

func TestOpenMemory(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	s.Close()
}

func TestNodeCRUD(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	// Create project first
	if err := s.UpsertProject("test", "/tmp/test"); err != nil {
		t.Fatalf("UpsertProject: %v", err)
	}

	// Insert node
	n := &Node{
		Project:       "test",
		Label:         "Function",
		Name:          "Foo",
		QualifiedName: "test.main.Foo",
		FilePath:      "main.go",
		StartLine:     10,
		EndLine:       20,
		Properties:    map[string]any{"signature": "func Foo(x int) error"},
	}
	id, err := s.UpsertNode(n)
	if err != nil {
		t.Fatalf("UpsertNode: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero id")
	}

	// Find by QN
	found, err := s.FindNodeByQN("test", "test.main.Foo")
	if err != nil {
		t.Fatalf("FindNodeByQN: %v", err)
	}
	if found == nil {
		t.Fatal("expected node, got nil")
	}
	if found.Name != "Foo" {
		t.Errorf("expected Foo, got %s", found.Name)
	}
	if found.Properties["signature"] != "func Foo(x int) error" {
		t.Errorf("unexpected signature: %v", found.Properties["signature"])
	}

	// Find by name
	nodes, err := s.FindNodesByName("test", "Foo")
	if err != nil {
		t.Fatalf("FindNodesByName: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}

	// Count
	count, err := s.CountNodes("test")
	if err != nil {
		t.Fatalf("CountNodes: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1, got %d", count)
	}
}

func TestNodeDedup(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	if err := s.UpsertProject("test", "/tmp/test"); err != nil {
		t.Fatalf("UpsertProject: %v", err)
	}

	// Insert same qualified_name twice — should update, not duplicate
	n1 := &Node{Project: "test", Label: "Function", Name: "Foo", QualifiedName: "test.main.Foo"}
	n2 := &Node{Project: "test", Label: "Function", Name: "Foo", QualifiedName: "test.main.Foo", Properties: map[string]any{"updated": true}}

	if _, err := s.UpsertNode(n1); err != nil {
		t.Fatalf("UpsertNode n1: %v", err)
	}
	if _, err := s.UpsertNode(n2); err != nil {
		t.Fatalf("UpsertNode n2: %v", err)
	}

	count, _ := s.CountNodes("test")
	if count != 1 {
		t.Errorf("expected 1 node after dedup, got %d", count)
	}

	// Verify it was updated
	found, _ := s.FindNodeByQN("test", "test.main.Foo")
	if found.Properties["updated"] != true {
		t.Error("expected updated property")
	}
}

func TestEdgeCRUD(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	if err := s.UpsertProject("test", "/tmp/test"); err != nil {
		t.Fatalf("UpsertProject: %v", err)
	}

	// Create two nodes
	id1, _ := s.UpsertNode(&Node{Project: "test", Label: "Function", Name: "A", QualifiedName: "test.A"})
	id2, _ := s.UpsertNode(&Node{Project: "test", Label: "Function", Name: "B", QualifiedName: "test.B"})

	// Insert edge
	_, err = s.InsertEdge(&Edge{Project: "test", SourceID: id1, TargetID: id2, Type: "CALLS"})
	if err != nil {
		t.Fatalf("InsertEdge: %v", err)
	}

	// Find by source
	edges, err := s.FindEdgesBySource(id1)
	if err != nil {
		t.Fatalf("FindEdgesBySource: %v", err)
	}
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	if edges[0].Type != "CALLS" {
		t.Errorf("expected CALLS, got %s", edges[0].Type)
	}

	// Count
	count, _ := s.CountEdges("test")
	if count != 1 {
		t.Errorf("expected 1, got %d", count)
	}
}

func TestCascadeDelete(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	// Create project with nodes and edges
	if err := s.UpsertProject("test", "/tmp/test"); err != nil {
		t.Fatalf("UpsertProject: %v", err)
	}
	id1, _ := s.UpsertNode(&Node{Project: "test", Label: "Function", Name: "A", QualifiedName: "test.A"})
	id2, _ := s.UpsertNode(&Node{Project: "test", Label: "Function", Name: "B", QualifiedName: "test.B"})
	if _, err := s.InsertEdge(&Edge{Project: "test", SourceID: id1, TargetID: id2, Type: "CALLS"}); err != nil {
		t.Fatalf("InsertEdge: %v", err)
	}

	// Delete project — should cascade
	if err := s.DeleteProject("test"); err != nil {
		t.Fatalf("DeleteProject: %v", err)
	}

	nodes, _ := s.CountNodes("test")
	edges, _ := s.CountEdges("test")
	if nodes != 0 {
		t.Errorf("expected 0 nodes after cascade, got %d", nodes)
	}
	if edges != 0 {
		t.Errorf("expected 0 edges after cascade, got %d", edges)
	}
}

func TestProjectCRUD(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	// Create
	if err := s.UpsertProject("myproject", "/home/user/myproject"); err != nil {
		t.Fatalf("UpsertProject: %v", err)
	}

	// Get
	p, err := s.GetProject("myproject")
	if err != nil {
		t.Fatalf("GetProject: %v", err)
	}
	if p.Name != "myproject" {
		t.Errorf("expected myproject, got %s", p.Name)
	}
	if p.RootPath != "/home/user/myproject" {
		t.Errorf("unexpected root: %s", p.RootPath)
	}

	// List
	projects, err := s.ListProjects()
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	if len(projects) != 1 {
		t.Fatalf("expected 1 project, got %d", len(projects))
	}
}

func TestFileHashes(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	if err := s.UpsertProject("test", "/tmp/test"); err != nil {
		t.Fatalf("UpsertProject: %v", err)
	}

	// Upsert
	if err := s.UpsertFileHash("test", "main.go", "abc123"); err != nil {
		t.Fatalf("UpsertFileHash: %v", err)
	}

	// Get
	hashes, err := s.GetFileHashes("test")
	if err != nil {
		t.Fatalf("GetFileHashes: %v", err)
	}
	if hashes["main.go"] != "abc123" {
		t.Errorf("expected abc123, got %s", hashes["main.go"])
	}

	// Update
	if err := s.UpsertFileHash("test", "main.go", "def456"); err != nil {
		t.Fatalf("UpsertFileHash update: %v", err)
	}
	hashes, _ = s.GetFileHashes("test")
	if hashes["main.go"] != "def456" {
		t.Errorf("expected def456, got %s", hashes["main.go"])
	}
}

func TestSearch(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	if err := s.UpsertProject("test", "/tmp/test"); err != nil {
		t.Fatalf("UpsertProject: %v", err)
	}
	if _, err := s.UpsertNode(&Node{Project: "test", Label: "Function", Name: "SubmitOrder", QualifiedName: "test.main.SubmitOrder", FilePath: "main.go"}); err != nil {
		t.Fatalf("UpsertNode SubmitOrder: %v", err)
	}
	if _, err := s.UpsertNode(&Node{Project: "test", Label: "Function", Name: "ProcessOrder", QualifiedName: "test.service.ProcessOrder", FilePath: "service.go"}); err != nil {
		t.Fatalf("UpsertNode ProcessOrder: %v", err)
	}
	if _, err := s.UpsertNode(&Node{Project: "test", Label: "Class", Name: "OrderService", QualifiedName: "test.service.OrderService", FilePath: "service.go"}); err != nil {
		t.Fatalf("UpsertNode OrderService: %v", err)
	}

	// Search by label
	output, err := s.Search(&SearchParams{Project: "test", Label: "Function"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(output.Results) != 2 {
		t.Errorf("expected 2 functions, got %d", len(output.Results))
	}
	if output.Total != 2 {
		t.Errorf("expected total=2, got %d", output.Total)
	}

	// Search by name pattern
	output, err = s.Search(&SearchParams{Project: "test", NamePattern: ".*Submit.*"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(output.Results) != 1 {
		t.Errorf("expected 1 match, got %d", len(output.Results))
	}

	// Search by file pattern
	output, err = s.Search(&SearchParams{Project: "test", FilePattern: "service*"})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(output.Results) != 2 {
		t.Errorf("expected 2 nodes in service.go, got %d", len(output.Results))
	}

	// Search with offset/limit pagination
	output, err = s.Search(&SearchParams{Project: "test", Limit: 1})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(output.Results) != 1 {
		t.Errorf("expected 1 result with limit=1, got %d", len(output.Results))
	}
	if output.Total != 3 {
		t.Errorf("expected total=3, got %d", output.Total)
	}

	output, err = s.Search(&SearchParams{Project: "test", Limit: 1, Offset: 1})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(output.Results) != 1 {
		t.Errorf("expected 1 result with limit=1 offset=1, got %d", len(output.Results))
	}
	if output.Total != 3 {
		t.Errorf("expected total=3, got %d", output.Total)
	}
}

func TestGlobToLike(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		{"**/*.py", "%%.py"},
		{"**/dir/**", "%dir%"},
		{"*.go", "%.go"},
		{"src/**", "src%"},
		{"**/test_*.py", "%test_%.py"},
		{"file?.txt", "file_.txt"},
		{"exact.go", "exact.go"},
		{"**/custom-pip-package/**", "%custom-pip-package%"},
	}
	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			got := globToLike(tt.pattern)
			if got != tt.want {
				t.Errorf("globToLike(%q) = %q, want %q", tt.pattern, got, tt.want)
			}
		})
	}
}

func TestGeneratedColumnURLPath(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Check that the generated column exists
	var colCount int
	err = s.DB().QueryRow(`SELECT COUNT(*) FROM pragma_table_xinfo('edges') WHERE name='url_path_gen'`).Scan(&colCount)
	if err != nil {
		t.Fatal(err)
	}
	if colCount != 1 {
		t.Skip("url_path_gen column not available (SQLite version may not support generated columns)")
	}
}

func TestFindEdgesByURLPath(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Create project
	if err := s.UpsertProject("test-proj", "/tmp/test"); err != nil {
		t.Fatal(err)
	}

	// Create two nodes
	srcID, _ := s.UpsertNode(&Node{
		Project: "test-proj", Label: "Function", Name: "caller",
		QualifiedName: "test.caller",
	})
	tgtID, _ := s.UpsertNode(&Node{
		Project: "test-proj", Label: "Function", Name: "handler",
		QualifiedName: "test.handler",
	})

	// Create HTTP_CALLS edge with url_path
	_, err = s.InsertEdge(&Edge{
		Project:    "test-proj",
		SourceID:   srcID,
		TargetID:   tgtID,
		Type:       "HTTP_CALLS",
		Properties: map[string]any{"url_path": "/api/orders/create", "confidence": 0.8},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Search for edges containing "orders"
	edges, err := s.FindEdgesByURLPath("test-proj", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	if edges[0].Properties["url_path"] != "/api/orders/create" {
		t.Errorf("unexpected url_path: %v", edges[0].Properties["url_path"])
	}

	// Search for non-matching
	edges, err = s.FindEdgesByURLPath("test-proj", "users")
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 0 {
		t.Fatalf("expected 0 edges, got %d", len(edges))
	}
}

func TestSearchExcludeLabels(t *testing.T) {
	s, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.UpsertProject("test", "/tmp/test"); err != nil {
		t.Fatal(err)
	}

	// Create nodes with different labels
	for i, label := range []string{"Function", "Route", "Method", "Route"} {
		_, _ = s.UpsertNode(&Node{
			Project:       "test",
			Label:         label,
			Name:          "node_" + label,
			QualifiedName: fmt.Sprintf("test.%s.node_%d", label, i),
			FilePath:      "test.go",
		})
	}

	// Search without exclusion
	output, err := s.Search(&SearchParams{
		Project: "test",
		Limit:   100,
	})
	if err != nil {
		t.Fatal(err)
	}
	total := output.Total

	// Search with Route excluded
	output2, err := s.Search(&SearchParams{
		Project:       "test",
		ExcludeLabels: []string{"Route"},
		Limit:         100,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Should have fewer results
	if output2.Total >= total {
		t.Errorf("exclude_labels didn't reduce results: before=%d, after=%d", total, output2.Total)
	}

	// Verify no Route nodes in results
	for _, r := range output2.Results {
		if r.Node.Label == "Route" {
			t.Errorf("found Route node despite exclude_labels")
		}
	}
}
