package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/metrics"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// testSnippetServer creates a Server with an in-memory-like store backed by a temp dir,
// pre-populated with a project, nodes, and edges.
func testSnippetServer(t *testing.T) *Server {
	t.Helper()

	tmpDir := t.TempDir()
	routerDir := filepath.Join(tmpDir, "db")
	projRoot := filepath.Join(tmpDir, "project")

	if err := os.MkdirAll(projRoot, 0o750); err != nil {
		t.Fatal(err)
	}

	// Write a sample source file
	srcContent := `package main

func HandleRequest() error {
	return nil
}

func ProcessOrder(id int) {
	// process
}

func Run() {
	// server
}
`
	if err := os.WriteFile(filepath.Join(projRoot, "main.go"), []byte(srcContent), 0o600); err != nil {
		t.Fatal(err)
	}

	router, err := store.NewRouterWithDir(routerDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(router.CloseAll)

	projName := "test-project"
	st, err := router.ForProject(projName)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertProject(projName, projRoot); err != nil {
		t.Fatal(err)
	}

	// Create nodes
	idHR, _ := st.UpsertNode(&store.Node{
		Project:       projName,
		Label:         "Function",
		Name:          "HandleRequest",
		QualifiedName: "test-project.cmd.server.main.HandleRequest",
		FilePath:      "main.go",
		StartLine:     3,
		EndLine:       5,
		Properties: map[string]any{
			"signature":   "func HandleRequest() error",
			"return_type": "error",
			"is_exported": true,
		},
	})
	idPO, _ := st.UpsertNode(&store.Node{
		Project:       projName,
		Label:         "Function",
		Name:          "ProcessOrder",
		QualifiedName: "test-project.cmd.server.main.ProcessOrder",
		FilePath:      "main.go",
		StartLine:     7,
		EndLine:       9,
		Properties:    map[string]any{"signature": "func ProcessOrder(id int)"},
	})
	idRun1, _ := st.UpsertNode(&store.Node{
		Project:       projName,
		Label:         "Function",
		Name:          "Run",
		QualifiedName: "test-project.cmd.server.Run",
		FilePath:      "main.go",
		StartLine:     11,
		EndLine:       13,
	})
	idRun2, _ := st.UpsertNode(&store.Node{
		Project:       projName,
		Label:         "Function",
		Name:          "Run",
		QualifiedName: "test-project.cmd.worker.Run",
		FilePath:      "main.go",
		StartLine:     11,
		EndLine:       13,
	})

	// Create edges: HandleRequest -> ProcessOrder, HandleRequest -> Run1
	_, _ = st.InsertEdge(&store.Edge{Project: projName, SourceID: idHR, TargetID: idPO, Type: "CALLS"})
	_, _ = st.InsertEdge(&store.Edge{Project: projName, SourceID: idHR, TargetID: idRun1, Type: "CALLS"})
	_ = idRun2 // used to create ambiguity for "Run"

	srv := &Server{
		router:         router,
		handlers:       make(map[string]mcp.ToolHandler),
		sessionProject: projName,
	}

	return srv
}

func callSnippetRaw(t *testing.T, srv *Server, qn string) *mcp.CallToolResult {
	t.Helper()
	args := map[string]any{"qualified_name": qn}
	rawArgs, _ := json.Marshal(args)

	req := &mcp.CallToolRequest{
		Params: &mcp.CallToolParamsRaw{
			Name:      "get_code_snippet",
			Arguments: rawArgs,
		},
	}

	result, err := srv.handleGetCodeSnippet(context.TODO(), req)
	if err != nil {
		t.Fatalf("handleGetCodeSnippet error: %v", err)
	}
	return result
}

func callSnippet(t *testing.T, srv *Server, qn string) map[string]any {
	t.Helper()
	return callSnippetWithOpts(t, srv, map[string]any{"qualified_name": qn})
}

func callSnippetWithOpts(t *testing.T, srv *Server, args map[string]any) map[string]any {
	t.Helper()
	// Remove empty project to match old behavior
	if p, ok := args["project"]; ok && p == "" {
		delete(args, "project")
	}
	rawArgs, _ := json.Marshal(args)

	req := &mcp.CallToolRequest{
		Params: &mcp.CallToolParamsRaw{
			Name:      "get_code_snippet",
			Arguments: rawArgs,
		},
	}

	result, err := srv.handleGetCodeSnippet(context.TODO(), req)
	if err != nil {
		t.Fatalf("handleGetCodeSnippet error: %v", err)
	}
	if len(result.Content) == 0 {
		t.Fatal("empty result content")
	}
	tc, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", result.Content[0])
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(tc.Text), &data); err != nil {
		t.Fatalf("unmarshal result: %v (text: %s)", err, tc.Text)
	}
	return data
}

func TestSnippet_ExactQN(t *testing.T) {
	srv := testSnippetServer(t)
	data := callSnippet(t, srv, "test-project.cmd.server.main.HandleRequest")

	if data["name"] != "HandleRequest" {
		t.Errorf("expected name=HandleRequest, got %v", data["name"])
	}
	if data["source"] == nil || data["source"] == "" {
		t.Error("expected non-empty source")
	}
	// Exact match should not have match_method
	if _, ok := data["match_method"]; ok {
		t.Errorf("exact match should not have match_method, got %v", data["match_method"])
	}
	// Should have enriched properties
	if data["signature"] != "func HandleRequest() error" {
		t.Errorf("expected signature, got %v", data["signature"])
	}
	if data["return_type"] != "error" {
		t.Errorf("expected return_type=error, got %v", data["return_type"])
	}
	// Should have caller/callee counts
	if data["callers"] == nil {
		t.Error("expected callers field")
	}
	if data["callees"] == nil {
		t.Error("expected callees field")
	}
	// HandleRequest has 0 callers and 2 callees
	if data["callers"] != float64(0) {
		t.Errorf("expected 0 callers, got %v", data["callers"])
	}
	if data["callees"] != float64(2) {
		t.Errorf("expected 2 callees, got %v", data["callees"])
	}
}

func TestSnippet_QNSuffix(t *testing.T) {
	srv := testSnippetServer(t)
	data := callSnippet(t, srv, "main.HandleRequest")

	if data["name"] != "HandleRequest" {
		t.Errorf("expected name=HandleRequest, got %v", data["name"])
	}
	if data["match_method"] != "suffix" {
		t.Errorf("expected match_method=suffix, got %v", data["match_method"])
	}
	if data["source"] == nil || data["source"] == "" {
		t.Error("expected non-empty source")
	}
}

func TestSnippet_UniqueShortName(t *testing.T) {
	srv := testSnippetServer(t)
	// "ProcessOrder" is unique across both suffix and name tiers
	data := callSnippet(t, srv, "ProcessOrder")

	if data["name"] != "ProcessOrder" {
		t.Errorf("expected name=ProcessOrder, got %v", data["name"])
	}
	// ProcessOrder QN ends with ".ProcessOrder" — suffix tier matches first (1 result)
	if data["match_method"] != "suffix" {
		t.Errorf("expected match_method=suffix, got %v", data["match_method"])
	}
	if data["source"] == nil || data["source"] == "" {
		t.Error("expected non-empty source")
	}
}

func TestSnippet_NameTier(t *testing.T) {
	// To test name-tier specifically, we need a node whose short name is unique
	// but whose QN doesn't end with ".ShortName" (so suffix tier misses).
	// This is hard to construct since QNs always end with the name.
	// Instead, verify that short names resolve correctly when suffix tier returns 1.
	srv := testSnippetServer(t)
	data := callSnippet(t, srv, "HandleRequest")

	if data["name"] != "HandleRequest" {
		t.Errorf("expected name=HandleRequest, got %v", data["name"])
	}
	// Suffix tier finds it (QN ends with ".HandleRequest"), which is correct behavior
	if data["match_method"] != "suffix" {
		t.Errorf("expected match_method=suffix, got %v", data["match_method"])
	}
}

func TestSnippet_AmbiguousShortName(t *testing.T) {
	srv := testSnippetServer(t)
	data := callSnippet(t, srv, "Run")

	// Should return suggestions, not source code
	if data["source"] != nil {
		t.Error("ambiguous lookup should not return source code")
	}
	// Should use "status"/"message" instead of "error"
	if data["status"] != "ambiguous" {
		t.Fatalf("expected status=ambiguous, got %v", data["status"])
	}
	if data["message"] == nil {
		t.Fatal("expected message field for ambiguous match")
	}
	// Must NOT have "error" key
	if _, hasError := data["error"]; hasError {
		t.Error("ambiguous response must not contain 'error' key")
	}
	suggestions, ok := data["suggestions"].([]any)
	if !ok {
		t.Fatalf("expected suggestions array, got %T", data["suggestions"])
	}
	if len(suggestions) < 2 {
		t.Errorf("expected at least 2 suggestions, got %d", len(suggestions))
	}
	// Verify suggestions have qualified_name
	for _, s := range suggestions {
		sMap, ok := s.(map[string]any)
		if !ok {
			t.Fatalf("expected suggestion map, got %T", s)
		}
		if sMap["qualified_name"] == nil || sMap["qualified_name"] == "" {
			t.Error("suggestion missing qualified_name")
		}
	}
}

func TestSnippet_NotFound(t *testing.T) {
	srv := testSnippetServer(t)
	result := callSnippetRaw(t, srv, "CompletelyNonexistentFunctionXYZ123")

	// Should be an error result (plain text or JSON with suggestions)
	if !result.IsError {
		// If not IsError, check it has suggestions
		tc, ok := result.Content[0].(*mcp.TextContent)
		if !ok {
			t.Fatal("expected TextContent")
		}
		var data map[string]any
		if err := json.Unmarshal([]byte(tc.Text), &data); err == nil {
			if data["suggestions"] == nil && data["status"] == nil {
				t.Error("expected status or suggestions for non-existent function")
			}
		}
		return
	}
	// Plain error result is fine for truly not-found names
}

func TestSnippet_FuzzySuggestions(t *testing.T) {
	srv := testSnippetServer(t)
	// "Handle" should fuzzy-match "HandleRequest" via findSimilarNodes
	data := callSnippet(t, srv, "Handle")

	// Should return suggestions (fuzzy match to HandleRequest)
	if suggestions, ok := data["suggestions"].([]any); ok {
		found := false
		for _, s := range suggestions {
			sMap, ok := s.(map[string]any)
			if !ok {
				t.Fatalf("expected suggestion map, got %T", s)
			}
			if sMap["name"] == "HandleRequest" {
				found = true
			}
		}
		if !found {
			t.Error("expected HandleRequest in fuzzy suggestions")
		}
	}
	// If no suggestions, it should at least have a status
	if data["suggestions"] == nil && data["status"] == nil && data["source"] == nil {
		t.Error("expected either suggestions, status, or source")
	}
}

func TestSnippet_EnrichedProperties(t *testing.T) {
	srv := testSnippetServer(t)
	data := callSnippet(t, srv, "test-project.cmd.server.main.HandleRequest")

	// Verify all enriched properties are present
	if data["signature"] == nil {
		t.Error("expected signature property")
	}
	if data["return_type"] == nil {
		t.Error("expected return_type property")
	}
	if data["is_exported"] != true {
		t.Errorf("expected is_exported=true, got %v", data["is_exported"])
	}
}

func TestSnippet_FuzzyLastSegment(t *testing.T) {
	srv := testSnippetServer(t)
	// "auth.handlers.HandleRequest" — full dotted string won't match anything,
	// but last segment "HandleRequest" should fuzzy-match
	data := callSnippet(t, srv, "auth.handlers.HandleRequest")

	// Should return suggestions via last-segment extraction
	if data["status"] != "ambiguous" && data["source"] == nil {
		// It could be a direct match via suffix or fuzzy
		if data["suggestions"] == nil {
			t.Error("expected suggestions for dotted input with last-segment fallback")
		}
	}
	// If it returned suggestions, verify HandleRequest is among them
	if suggestions, ok := data["suggestions"].([]any); ok {
		found := false
		for _, s := range suggestions {
			sMap, ok := s.(map[string]any)
			if !ok {
				t.Fatalf("expected suggestion map, got %T", s)
			}
			if sMap["name"] == "HandleRequest" {
				found = true
			}
		}
		if !found {
			t.Error("expected HandleRequest in last-segment suggestions")
		}
	}
}

func TestSnippet_AutoResolve_Default(t *testing.T) {
	srv := testSnippetServer(t)
	// "Run" is ambiguous (2 candidates). Without auto_resolve, should return suggestions.
	data := callSnippet(t, srv, "Run")

	if data["status"] != "ambiguous" {
		t.Errorf("expected status=ambiguous without auto_resolve, got %v", data["status"])
	}
	if data["source"] != nil {
		t.Error("should not return source without auto_resolve")
	}
}

func TestSnippet_AutoResolve_Enabled(t *testing.T) {
	srv := testSnippetServer(t)
	// "Run" is ambiguous (2 candidates). With auto_resolve=true, should pick best.
	data := callSnippetWithOpts(t, srv, map[string]any{
		"qualified_name": "Run",
		"auto_resolve":   true,
	})

	if data["source"] == nil || data["source"] == "" {
		t.Fatal("expected source with auto_resolve=true")
	}
	if data["match_method"] != "auto_best" {
		t.Errorf("expected match_method=auto_best, got %v", data["match_method"])
	}
	// Should have alternatives
	alternatives, ok := data["alternatives"].([]any)
	if !ok {
		t.Fatalf("expected alternatives array, got %T", data["alternatives"])
	}
	if len(alternatives) != 1 {
		t.Errorf("expected 1 alternative, got %d", len(alternatives))
	}
	// The picked one should be cmd.server.Run (has 1 inbound CALLS edge = higher degree)
	if data["qualified_name"] != "test-project.cmd.server.Run" {
		t.Errorf("expected auto_resolve to pick server.Run (higher degree), got %v", data["qualified_name"])
	}
}

func TestSnippet_IncludeNeighbors_Default(t *testing.T) {
	srv := testSnippetServer(t)
	data := callSnippet(t, srv, "test-project.cmd.server.main.HandleRequest")

	// Without include_neighbors, should NOT have caller_names/callee_names
	if data["caller_names"] != nil {
		t.Error("should not have caller_names without include_neighbors")
	}
	if data["callee_names"] != nil {
		t.Error("should not have callee_names without include_neighbors")
	}
	// But should still have counts
	if data["callers"] == nil || data["callees"] == nil {
		t.Error("expected callers/callees counts")
	}
}

func TestSnippet_IncludeNeighbors_Enabled(t *testing.T) {
	srv := testSnippetServer(t)
	data := callSnippetWithOpts(t, srv, map[string]any{
		"qualified_name":    "test-project.cmd.server.main.HandleRequest",
		"include_neighbors": true,
	})

	if data["source"] == nil || data["source"] == "" {
		t.Fatal("expected source")
	}
	// HandleRequest has 0 callers and 2 callees (ProcessOrder, Run)
	if data["caller_names"] != nil {
		t.Errorf("expected nil caller_names (0 callers), got %v", data["caller_names"])
	}
	calleeNames, ok := data["callee_names"].([]any)
	if !ok {
		t.Fatalf("expected callee_names array, got %T (%v)", data["callee_names"], data["callee_names"])
	}
	if len(calleeNames) != 2 {
		t.Errorf("expected 2 callee names, got %d", len(calleeNames))
	}
	// Verify the names are present (sorted alphabetically by SQL ORDER BY)
	names := make(map[string]bool)
	for _, n := range calleeNames {
		s, ok := n.(string)
		if !ok {
			t.Fatalf("expected string callee name, got %T", n)
		}
		names[s] = true
	}
	if !names["ProcessOrder"] {
		t.Error("expected ProcessOrder in callee_names")
	}
	if !names["Run"] {
		t.Error("expected Run in callee_names")
	}
}

func TestGetCodeSnippet_MetaField(t *testing.T) {
	srv := testSnippetServer(t)
	// Attach a metricsTracker backed by a temp file.
	savingsPath := filepath.Join(t.TempDir(), "savings.json")
	srv.metricsTracker = metrics.NewTracker(savingsPath)

	data := callSnippet(t, srv, "test-project.cmd.server.main.HandleRequest")

	// Source should still be present.
	if data["source"] == nil || data["source"] == "" {
		t.Fatal("expected non-empty source")
	}

	// _meta must exist.
	metaRaw, ok := data["_meta"]
	if !ok {
		t.Fatal("expected _meta field in response")
	}
	meta, ok := metaRaw.(map[string]any)
	if !ok {
		t.Fatalf("expected _meta to be a map, got %T", metaRaw)
	}

	tokensSaved, _ := meta["tokens_saved"].(float64)
	baselineTokens, _ := meta["baseline_tokens"].(float64)
	responseTokens, _ := meta["response_tokens"].(float64)
	reductionRatio, _ := meta["reduction_ratio"].(float64)

	if tokensSaved < 0 {
		t.Errorf("tokens_saved should be >= 0, got %v", tokensSaved)
	}
	if baselineTokens <= 0 {
		t.Errorf("baseline_tokens should be > 0, got %v", baselineTokens)
	}
	if responseTokens <= 0 {
		t.Errorf("response_tokens should be > 0, got %v", responseTokens)
	}
	if reductionRatio <= 0 {
		t.Errorf("reduction_ratio should be > 0, got %v", reductionRatio)
	}
}
