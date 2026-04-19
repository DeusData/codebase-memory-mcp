package orgtools

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdb"
)

// ---------- helpers ----------

func openTestDB(t *testing.T) *orgdb.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "org.db")
	db, err := orgdb.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seedRepo(t *testing.T, db *orgdb.DB, name, team, typ string) {
	t.Helper()
	err := db.UpsertRepo(orgdb.RepoRecord{
		Name:      name,
		GitHubURL: "https://github.com/GoHighLevel/" + name + ".git",
		Team:      team,
		Type:      typ,
		Languages: `["typescript"]`,
		NodeCount: 10,
		EdgeCount: 5,
	})
	if err != nil {
		t.Fatalf("UpsertRepo(%s): %v", name, err)
	}
}

// newService creates an OrgService backed by a temp DB.
func newService(t *testing.T) (*OrgService, *orgdb.DB) {
	t.Helper()
	db := openTestDB(t)
	return New(db), db
}

// ---------- Definitions ----------

func TestDefinitions_Returns5Tools(t *testing.T) {
	svc, _ := newService(t)
	defs := svc.Definitions()
	if len(defs) != 6 {
		t.Fatalf("want 6 definitions, got %d", len(defs))
	}

	expected := map[string]bool{
		"org_dependency_graph": false,
		"org_blast_radius":    false,
		"org_trace_flow":      false,
		"org_team_topology":   false,
		"org_search":          false,
		"org_code_search":     false,
	}
	for _, d := range defs {
		if _, ok := expected[d.Name]; !ok {
			t.Errorf("unexpected tool name: %q", d.Name)
		}
		expected[d.Name] = true
	}
	for name, found := range expected {
		if !found {
			t.Errorf("missing tool definition: %q", name)
		}
	}
}

// ---------- IsOrgTool ----------

func TestIsOrgTool_KnownTools(t *testing.T) {
	svc, _ := newService(t)
	for _, name := range []string{
		"org_dependency_graph", "org_blast_radius", "org_trace_flow",
		"org_team_topology", "org_search",
	} {
		if !svc.IsOrgTool(name) {
			t.Errorf("IsOrgTool(%q) = false, want true", name)
		}
	}
}

func TestIsOrgTool_UnknownTool(t *testing.T) {
	svc, _ := newService(t)
	if svc.IsOrgTool("unknown_tool") {
		t.Error("IsOrgTool(unknown_tool) = true, want false")
	}
}

// ---------- CallTool: org_dependency_graph ----------

func TestCallTool_DependencyGraph(t *testing.T) {
	svc, db := newService(t)

	seedRepo(t, db, "repo-a", "team-a", "backend")
	seedRepo(t, db, "repo-b", "team-b", "backend")

	for _, name := range []string{"repo-a", "repo-b"} {
		if err := db.UpsertPackageDep(name, orgdb.Dep{
			Scope: "@platform-core", Name: "base-service",
			DepType: "dependencies", VersionSpec: "^3.0.0",
		}); err != nil {
			t.Fatalf("UpsertPackageDep(%s): %v", name, err)
		}
	}

	result, err := svc.CallTool(context.Background(), "org_dependency_graph", map[string]interface{}{
		"package_scope": "@platform-core",
		"package_name":  "base-service",
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	deps, ok := result.([]orgdb.DependencyResult)
	if !ok {
		t.Fatalf("result type: got %T, want []orgdb.DependencyResult", result)
	}
	if len(deps) != 2 {
		t.Fatalf("want 2 results, got %d", len(deps))
	}
}

func TestCallTool_DependencyGraph_MissingArgs(t *testing.T) {
	svc, _ := newService(t)

	_, err := svc.CallTool(context.Background(), "org_dependency_graph", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing args")
	}
}

// ---------- CallTool: org_blast_radius ----------

func TestCallTool_BlastRadius(t *testing.T) {
	svc, db := newService(t)

	seedRepo(t, db, "provider-repo", "platform", "backend")
	seedRepo(t, db, "api-consumer", "payments", "backend")

	if err := db.InsertAPIContract(orgdb.APIContract{
		ProviderRepo: "provider-repo", ConsumerRepo: "api-consumer",
		Method: "GET", Path: "/api/v1/users", Confidence: 0.9,
	}); err != nil {
		t.Fatalf("InsertAPIContract: %v", err)
	}

	result, err := svc.CallTool(context.Background(), "org_blast_radius", map[string]interface{}{
		"repo": "provider-repo",
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	br, ok := result.(orgdb.BlastRadiusResult)
	if !ok {
		t.Fatalf("result type: got %T, want orgdb.BlastRadiusResult", result)
	}
	if br.TotalRepos != 1 {
		t.Errorf("TotalRepos: want 1, got %d", br.TotalRepos)
	}
}

func TestCallTool_BlastRadius_MissingArgs(t *testing.T) {
	svc, _ := newService(t)

	_, err := svc.CallTool(context.Background(), "org_blast_radius", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing args")
	}
}

// ---------- CallTool: org_trace_flow ----------

func TestCallTool_TraceFlow(t *testing.T) {
	svc, db := newService(t)

	seedRepo(t, db, "svc-a", "team", "backend")
	seedRepo(t, db, "svc-b", "team", "backend")

	if err := db.InsertAPIContract(orgdb.APIContract{
		ProviderRepo: "svc-a", ConsumerRepo: "svc-b",
		Method: "GET", Path: "/api/v1/data", Confidence: 0.9,
	}); err != nil {
		t.Fatalf("InsertAPIContract: %v", err)
	}

	result, err := svc.CallTool(context.Background(), "org_trace_flow", map[string]interface{}{
		"trigger":   "svc-a",
		"direction": "downstream",
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	steps, ok := result.([]orgdb.FlowStep)
	if !ok {
		t.Fatalf("result type: got %T, want []orgdb.FlowStep", result)
	}
	if len(steps) == 0 {
		t.Fatal("want at least 1 step, got 0")
	}
	if steps[0].FromRepo != "svc-a" || steps[0].ToRepo != "svc-b" {
		t.Errorf("step: got %s -> %s, want svc-a -> svc-b", steps[0].FromRepo, steps[0].ToRepo)
	}
}

func TestCallTool_TraceFlow_DefaultDirection(t *testing.T) {
	svc, db := newService(t)

	seedRepo(t, db, "svc-a", "team", "backend")
	seedRepo(t, db, "svc-b", "team", "backend")

	if err := db.InsertAPIContract(orgdb.APIContract{
		ProviderRepo: "svc-a", ConsumerRepo: "svc-b",
		Method: "GET", Path: "/api/v1/data", Confidence: 0.9,
	}); err != nil {
		t.Fatalf("InsertAPIContract: %v", err)
	}

	// No direction specified — should default to "downstream"
	result, err := svc.CallTool(context.Background(), "org_trace_flow", map[string]interface{}{
		"trigger": "svc-a",
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	steps, ok := result.([]orgdb.FlowStep)
	if !ok {
		t.Fatalf("result type: got %T", result)
	}
	if len(steps) == 0 {
		t.Fatal("want at least 1 step with default direction")
	}
}

func TestCallTool_TraceFlow_MissingArgs(t *testing.T) {
	svc, _ := newService(t)

	_, err := svc.CallTool(context.Background(), "org_trace_flow", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing trigger")
	}
}

// ---------- CallTool: org_team_topology ----------

func TestCallTool_TeamTopology(t *testing.T) {
	svc, db := newService(t)

	seedRepo(t, db, "revex-backend", "revex", "backend")
	seedRepo(t, db, "revex-frontend", "revex", "frontend")

	result, err := svc.CallTool(context.Background(), "org_team_topology", map[string]interface{}{
		"team": "revex",
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	info, ok := result.(orgdb.TeamInfo)
	if !ok {
		t.Fatalf("result type: got %T, want orgdb.TeamInfo", result)
	}
	if info.Team != "revex" {
		t.Errorf("Team: got %q, want %q", info.Team, "revex")
	}
	if len(info.Repos) != 2 {
		t.Errorf("Repos: want 2, got %d", len(info.Repos))
	}
}

func TestCallTool_TeamTopology_MissingArgs(t *testing.T) {
	svc, _ := newService(t)

	_, err := svc.CallTool(context.Background(), "org_team_topology", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing team")
	}
}

// ---------- CallTool: org_search ----------

func TestCallTool_Search(t *testing.T) {
	svc, db := newService(t)

	seedRepo(t, db, "ghl-revex-backend", "revex", "backend")
	seedRepo(t, db, "ghl-revex-frontend", "revex", "frontend")
	seedRepo(t, db, "ghl-payments-backend", "payments", "backend")

	result, err := svc.CallTool(context.Background(), "org_search", map[string]interface{}{
		"query": "revex",
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	repos, ok := result.([]orgdb.RepoSearchResult)
	if !ok {
		t.Fatalf("result type: got %T, want []orgdb.RepoSearchResult", result)
	}
	if len(repos) != 2 {
		t.Fatalf("want 2 results, got %d", len(repos))
	}
}

func TestCallTool_Search_WithFilters(t *testing.T) {
	svc, db := newService(t)

	seedRepo(t, db, "ghl-revex-backend", "revex", "backend")
	seedRepo(t, db, "ghl-revex-frontend", "revex", "frontend")

	result, err := svc.CallTool(context.Background(), "org_search", map[string]interface{}{
		"query": "revex",
		"scope": "backend",
		"team":  "revex",
		"limit": float64(5),
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	repos, ok := result.([]orgdb.RepoSearchResult)
	if !ok {
		t.Fatalf("result type: got %T", result)
	}
	if len(repos) != 1 {
		t.Fatalf("want 1 result with scope=backend, got %d", len(repos))
	}
	if repos[0].Name != "ghl-revex-backend" {
		t.Errorf("Name: got %q, want %q", repos[0].Name, "ghl-revex-backend")
	}
}

func TestCallTool_Search_MissingArgs(t *testing.T) {
	svc, _ := newService(t)

	_, err := svc.CallTool(context.Background(), "org_search", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing query")
	}
}

// ---------- CallTool: unknown tool ----------

func TestCallTool_UnknownTool(t *testing.T) {
	svc, _ := newService(t)

	_, err := svc.CallTool(context.Background(), "unknown_tool", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for unknown tool")
	}
}
