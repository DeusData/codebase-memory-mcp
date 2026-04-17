package discovery

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
)

type fakeToolCaller struct {
	tools map[string]func(params map[string]interface{}) *mcp.ToolResult
}

func (f *fakeToolCaller) CallTool(_ context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error) {
	if fn, ok := f.tools[name]; ok {
		return fn(params), nil
	}
	return &mcp.ToolResult{}, nil
}

func jsonToolResult(t *testing.T, payload interface{}) *mcp.ToolResult {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return &mcp.ToolResult{
		Content: []mcp.Content{{Type: "text", Text: string(raw)}},
	}
}

func TestDiscoverProjectsNormalizesCatalogFromRootPath(t *testing.T) {
	svc := NewService(&fakeToolCaller{
		tools: map[string]func(map[string]interface{}) *mcp.ToolResult{
			"list_projects": func(params map[string]interface{}) *mcp.ToolResult {
				return jsonToolResult(t, map[string]interface{}{
					"projects": []map[string]interface{}{
						{
							"name":      "app-fleet-cache-membership-backend",
							"root_path": "/app/fleet-cache/membership-backend",
							"nodes":     5942,
							"edges":     11602,
						},
					},
				})
			},
		},
	}, manifest.Manifest{
		Repos: []manifest.Repo{
			{Name: "membership-backend", Team: "revex", Type: "service", Tags: []string{"membership", "checkout"}},
		},
	}, Options{})

	catalog, err := svc.refreshCatalog(context.Background())
	if err != nil {
		t.Fatalf("refreshCatalog: %v", err)
	}
	if len(catalog) != 1 {
		t.Fatalf("catalog size: want 1, got %d", len(catalog))
	}
	if catalog[0].RepoSlug != "membership-backend" {
		t.Fatalf("repo slug: want membership-backend, got %q", catalog[0].RepoSlug)
	}
	if catalog[0].Team != "revex" {
		t.Fatalf("team: want revex, got %q", catalog[0].Team)
	}
}

func TestDiscoverProjectsRanksByMetadataAndBM25(t *testing.T) {
	svc := NewService(&fakeToolCaller{
		tools: map[string]func(map[string]interface{}) *mcp.ToolResult{
			"list_projects": func(params map[string]interface{}) *mcp.ToolResult {
				return jsonToolResult(t, map[string]interface{}{
					"projects": []map[string]interface{}{
						{
							"name":      "app-fleet-cache-membership-backend",
							"root_path": "/app/fleet-cache/membership-backend",
							"nodes":     5942,
							"edges":     11602,
						},
						{
							"name":      "app-fleet-cache-ghl-membership-frontend",
							"root_path": "/app/fleet-cache/ghl-membership-frontend",
							"nodes":     10287,
							"edges":     15213,
						},
					},
				})
			},
			"search_graph": func(params map[string]interface{}) *mcp.ToolResult {
				project, _ := params["project"].(string)
				switch project {
				case "app-fleet-cache-membership-backend":
					return jsonToolResult(t, map[string]interface{}{
						"total": 4,
						"results": []map[string]interface{}{
							{"label": "Function", "name": "acquireCheckoutLock", "rank": -14.0},
						},
					})
				case "app-fleet-cache-ghl-membership-frontend":
					return jsonToolResult(t, map[string]interface{}{
						"total": 1,
						"results": []map[string]interface{}{
							{"label": "Component", "name": "CheckoutPage", "rank": -2.0},
						},
					})
				default:
					return jsonToolResult(t, map[string]interface{}{"total": 0, "results": []map[string]interface{}{}})
				}
			},
			"get_architecture": func(params map[string]interface{}) *mcp.ToolResult {
				project, _ := params["project"].(string)
				if project == "app-fleet-cache-membership-backend" {
					return jsonToolResult(t, map[string]interface{}{
						"project":     project,
						"total_nodes": 5942,
						"total_edges": 11602,
						"node_labels": []map[string]interface{}{{"label": "Function", "count": 600}},
						"edge_types":  []map[string]interface{}{{"type": "CALLS", "count": 1800}},
					})
				}
				return jsonToolResult(t, map[string]interface{}{
					"project":     project,
					"total_nodes": 10287,
					"total_edges": 15213,
					"node_labels": []map[string]interface{}{{"label": "Component", "count": 420}},
					"edge_types":  []map[string]interface{}{{"type": "IMPORTS", "count": 2000}},
				})
			},
		},
	}, manifest.Manifest{
		Repos: []manifest.Repo{
			{Name: "membership-backend", Team: "revex", Type: "service", Tags: []string{"membership", "checkout", "contact"}},
			{Name: "ghl-membership-frontend", Team: "revex", Type: "frontend", Tags: []string{"membership", "checkout"}},
		},
	}, Options{MaxBM25Candidates: 5, MaxGraphCandidates: 3})

	resp, err := svc.DiscoverProjects(context.Background(), Request{
		Query:                  "add lock in membership checkout flow for contact purchases",
		Limit:                  5,
		IncludeGraphConfidence: true,
	})
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	if len(resp.PrimaryRepos) == 0 {
		t.Fatal("expected at least one primary repo")
	}
	if got := resp.PrimaryRepos[0].RepoSlug; got != "membership-backend" {
		t.Fatalf("top repo: want membership-backend, got %q", got)
	}
}

func TestDiscoverProjectsPenalizesPlaceholderIndexes(t *testing.T) {
	svc := NewService(&fakeToolCaller{
		tools: map[string]func(map[string]interface{}) *mcp.ToolResult{
			"list_projects": func(params map[string]interface{}) *mcp.ToolResult {
				return jsonToolResult(t, map[string]interface{}{
					"projects": []map[string]interface{}{
						{
							"name":      "app-fleet-cache-membership-backend",
							"root_path": "/app/fleet-cache/membership-backend",
							"nodes":     1,
							"edges":     0,
						},
						{
							"name":      "app-fleet-cache-ghl-membership-frontend",
							"root_path": "/app/fleet-cache/ghl-membership-frontend",
							"nodes":     1200,
							"edges":     2400,
						},
					},
				})
			},
			"search_graph": func(params map[string]interface{}) *mcp.ToolResult {
				project, _ := params["project"].(string)
				if project == "app-fleet-cache-membership-backend" {
					return jsonToolResult(t, map[string]interface{}{
						"total": 3,
						"results": []map[string]interface{}{
							{"label": "Function", "name": "fakeMatch", "rank": -12.0},
						},
					})
				}
				return jsonToolResult(t, map[string]interface{}{
					"total": 2,
					"results": []map[string]interface{}{
						{"label": "Component", "name": "CheckoutPage", "rank": -5.0},
					},
				})
			},
			"get_architecture": func(params map[string]interface{}) *mcp.ToolResult {
				project, _ := params["project"].(string)
				if project == "app-fleet-cache-membership-backend" {
					return jsonToolResult(t, map[string]interface{}{
						"project":     project,
						"total_nodes": 1,
						"total_edges": 0,
					})
				}
				return jsonToolResult(t, map[string]interface{}{
					"project":     project,
					"total_nodes": 1200,
					"total_edges": 2400,
				})
			},
		},
	}, manifest.Manifest{
		Repos: []manifest.Repo{
			{Name: "membership-backend", Team: "revex", Type: "service", Tags: []string{"membership", "checkout"}},
			{Name: "ghl-membership-frontend", Team: "revex", Type: "frontend", Tags: []string{"membership", "checkout"}},
		},
	}, Options{MaxBM25Candidates: 5, MaxGraphCandidates: 3})

	resp, err := svc.DiscoverProjects(context.Background(), Request{
		Query:                  "membership checkout",
		Limit:                  5,
		IncludeGraphConfidence: true,
	})
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	if len(resp.PrimaryRepos) == 0 {
		t.Fatal("expected at least one primary repo")
	}
	if got := resp.PrimaryRepos[0].RepoSlug; got != "ghl-membership-frontend" {
		t.Fatalf("top repo after placeholder penalty: want ghl-membership-frontend, got %q", got)
	}
}

func TestDiscoverProjectsReturnsCrossRepoCandidates(t *testing.T) {
	svc := NewService(&fakeToolCaller{
		tools: map[string]func(map[string]interface{}) *mcp.ToolResult{
			"list_projects": func(params map[string]interface{}) *mcp.ToolResult {
				return jsonToolResult(t, map[string]interface{}{
					"projects": []map[string]interface{}{
						{
							"name":      "app-fleet-cache-membership-backend",
							"root_path": "/app/fleet-cache/membership-backend",
							"nodes":     5942,
							"edges":     11602,
						},
						{
							"name":      "app-fleet-cache-ghl-membership-frontend",
							"root_path": "/app/fleet-cache/ghl-membership-frontend",
							"nodes":     10287,
							"edges":     15213,
						},
					},
				})
			},
			"search_graph": func(params map[string]interface{}) *mcp.ToolResult {
				project, _ := params["project"].(string)
				switch project {
				case "app-fleet-cache-membership-backend":
					return jsonToolResult(t, map[string]interface{}{
						"total": 3,
						"results": []map[string]interface{}{
							{"label": "Function", "name": "checkoutContactLock", "rank": -10.0},
						},
					})
				case "app-fleet-cache-ghl-membership-frontend":
					return jsonToolResult(t, map[string]interface{}{
						"total": 3,
						"results": []map[string]interface{}{
							{"label": "Component", "name": "CheckoutLockBanner", "rank": -9.0},
						},
					})
				default:
					return jsonToolResult(t, map[string]interface{}{"total": 0, "results": []map[string]interface{}{}})
				}
			},
			"get_architecture": func(params map[string]interface{}) *mcp.ToolResult {
				project, _ := params["project"].(string)
				if project == "app-fleet-cache-membership-backend" {
					return jsonToolResult(t, map[string]interface{}{
						"project":     project,
						"total_nodes": 5942,
						"total_edges": 11602,
						"node_labels": []map[string]interface{}{{"label": "Function", "count": 600}},
					})
				}
				return jsonToolResult(t, map[string]interface{}{
					"project":     project,
					"total_nodes": 10287,
					"total_edges": 15213,
					"node_labels": []map[string]interface{}{{"label": "Component", "count": 420}},
				})
			},
		},
	}, manifest.Manifest{
		Repos: []manifest.Repo{
			{Name: "membership-backend", Team: "revex", Type: "service", Tags: []string{"membership", "checkout", "contact"}},
			{Name: "ghl-membership-frontend", Team: "revex", Type: "frontend", Tags: []string{"membership", "checkout", "ui"}},
		},
	}, Options{MaxBM25Candidates: 5, MaxGraphCandidates: 3})

	resp, err := svc.DiscoverProjects(context.Background(), Request{
		Query:                  "add checkout lock ui and backend validation for membership contact purchases",
		Limit:                  5,
		IncludeGraphConfidence: true,
	})
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	if !resp.CrossRepo {
		t.Fatal("expected cross_repo=true")
	}
	if len(resp.PrimaryRepos)+len(resp.RelatedRepos) < 2 {
		t.Fatalf("expected at least two repos, got primary=%d related=%d", len(resp.PrimaryRepos), len(resp.RelatedRepos))
	}
}
