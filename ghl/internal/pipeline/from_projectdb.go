// Package pipeline — PopulateFromProjectDB builds org.db using MCP tools only.
//
// All extraction phases run with parallel worker pools for maximum speed.
// Phase 1 is sequential (single list_projects call), phases 2a-2d run
// concurrently with 8 workers each scanning projects in parallel.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdb"
)

const pipelineWorkers = 8

// MCPCaller is the interface for calling MCP tools on the C binary.
type MCPCaller interface {
	CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error)
}

// PopulateOrgFromProjectDBs builds org.db using MCP tools in parallel phases.
func PopulateOrgFromProjectDBs(ctx context.Context, db *orgdb.DB, caller MCPCaller, repos []manifest.Repo, cbmCacheDir string) error {
	// ── Phase 1: Repo metadata from list_projects (single call) ──
	result, err := caller.CallTool(ctx, "list_projects", nil)
	if err != nil {
		return fmt.Errorf("pipeline: list_projects: %w", err)
	}
	text := extractText(result)
	if text == "" || text == "null" {
		return fmt.Errorf("pipeline: list_projects returned empty")
	}

	var projects []projectInfo
	if err := json.Unmarshal([]byte(text), &projects); err != nil {
		var wrapped struct{ Projects []projectInfo }
		if err2 := json.Unmarshal([]byte(text), &wrapped); err2 != nil {
			return fmt.Errorf("pipeline: parse list_projects: %w", err)
		}
		projects = wrapped.Projects
	}

	slog.Info("phase 1: populating repo metadata", "projects", len(projects))

	repoByName := make(map[string]manifest.Repo, len(repos))
	for _, r := range repos {
		repoByName[r.Name] = r
	}

	var entries []projEntry
	for _, proj := range projects {
		repoName := stripProjectPrefix(proj.Name)
		repo, ok := repoByName[repoName]
		if !ok {
			repo = manifest.Repo{Name: repoName}
		}
		db.UpsertRepo(orgdb.RepoRecord{
			Name:      repoName,
			GitHubURL: repo.GitHubURL,
			Team:      repo.Team,
			Type:      repo.Type,
			NodeCount: proj.Nodes,
			EdgeCount: proj.Edges,
		})
		db.UpsertTeamOwnership(repoName, repo.Team, "")
		entries = append(entries, projEntry{projectName: proj.Name, repoName: repoName})
	}
	slog.Info("phase 1 complete", "repos", len(entries))

	// Wait for GCS data if too few projects
	if len(entries) < 50 {
		slog.Info("waiting for GCS data to load", "found", len(entries))
		entries = waitForProjects(ctx, caller, db, repoByName, repos, 50, 3*time.Minute)
		slog.Info("after waiting", "projects", len(entries))
	}

	// ── Phase 2: All extraction phases run in parallel ──
	var routeCount, consumerCount, packageCount, eventCount int64
	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		n := extractRoutes(ctx, db, caller, entries)
		atomic.StoreInt64(&routeCount, int64(n))
	}()
	go func() {
		defer wg.Done()
		n := extractConsumers(ctx, db, caller, entries)
		atomic.StoreInt64(&consumerCount, int64(n))
	}()
	go func() {
		defer wg.Done()
		n := extractPackageDeps(ctx, db, caller, entries)
		atomic.StoreInt64(&packageCount, int64(n))
	}()
	go func() {
		defer wg.Done()
		n := extractEventContracts(ctx, db, caller, entries)
		atomic.StoreInt64(&eventCount, int64(n))
	}()

	wg.Wait()

	rc := atomic.LoadInt64(&routeCount)
	cc := atomic.LoadInt64(&consumerCount)
	pc := atomic.LoadInt64(&packageCount)
	ec := atomic.LoadInt64(&eventCount)

	// ── Phase 2e: Infer package providers from repo names ──
	providerCount, provErr := db.InferPackageProviders()
	if provErr != nil {
		slog.Warn("infer package providers failed", "err", provErr)
	} else {
		slog.Info("phase 2e: inferred package providers", "count", providerCount)
	}

	// ── Phase 3: Cross-reference contracts ──
	// Fix __ path separators from C binary route qualified names before matching.
	// Provider paths arrive as "contacts__list" but consumers use "/CONTACTS_API/list",
	// so we must convert __ → / first for lastSegment/extractServiceIdentifier to work.
	if rc > 0 {
		fixCount, fixErr := db.FixRoutePaths()
		if fixErr != nil {
			slog.Warn("fix route paths failed", "err", fixErr)
		} else if fixCount > 0 {
			slog.Info("phase 3: fixed route paths", "count", fixCount)
		}
	}

	matched := 0
	if rc > 0 && cc > 0 {
		slog.Info("phase 3: cross-referencing API contracts")
		var err error
		matched, err = db.CrossReferenceContracts()
		if err != nil {
			slog.Warn("cross-reference failed", "err", err)
		} else {
			slog.Info("phase 3 complete", "api_matched", matched)
		}
	}

	if ec > 0 {
		eventMatched, err := db.CrossReferenceEventContracts()
		if err != nil {
			slog.Warn("cross-reference event contracts failed", "err", err)
		} else {
			slog.Info("event cross-reference complete", "matched", eventMatched)
		}
	}

	slog.Info("org.db fully populated",
		"repos", len(entries), "routes", rc, "consumers", cc,
		"events", ec, "packages", pc, "cross_referenced", matched)
	return nil
}

// ── Parallel worker pool helper ──

func parallelScan(entries []projEntry, workers int, fn func(entry projEntry)) {
	ch := make(chan projEntry, len(entries))
	for _, e := range entries {
		ch <- e
	}
	close(ch)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for entry := range ch {
				fn(entry)
			}
		}()
	}
	wg.Wait()
}

// ── Phase 2a: Routes (parallel) ──

func extractRoutes(ctx context.Context, db *orgdb.DB, caller MCPCaller, entries []projEntry) int {
	slog.Info("phase 2a: extracting routes", "projects", len(entries), "workers", pipelineWorkers)
	var count atomic.Int64

	parallelScan(entries, pipelineWorkers, func(entry projEntry) {
		result, err := caller.CallTool(ctx, "search_graph", map[string]interface{}{
			"project": entry.projectName,
			"label":   "Route",
			"limit":   500,
		})
		if err != nil {
			return
		}
		text := extractText(result)
		if text == "" || text == "null" {
			return
		}
		var resp searchGraphResponse
		if err := json.Unmarshal([]byte(text), &resp); err != nil {
			return
		}
		for _, node := range resp.Results {
			method, path := parseRouteQualifiedName(node.QualifiedName)
			if path == "" {
				continue
			}
			db.InsertAPIContract(orgdb.APIContract{
				ProviderRepo:   entry.repoName,
				Method:         method,
				Path:           path,
				ProviderSymbol: node.Name,
				Confidence:     0.3,
			})
			count.Add(1)
		}
	})

	n := int(count.Load())
	slog.Info("phase 2a complete", "routes", n)
	return n
}

// ── Phase 2b: Consumers (parallel) ──

func extractConsumers(ctx context.Context, db *orgdb.DB, caller MCPCaller, entries []projEntry) int {
	slog.Info("phase 2b: extracting InternalRequest consumers", "projects", len(entries), "workers", pipelineWorkers)
	var count atomic.Int64

	parallelScan(entries, pipelineWorkers, func(entry projEntry) {
		result, err := caller.CallTool(ctx, "search_code", map[string]interface{}{
			"project": entry.projectName,
			"pattern": "InternalRequest",
			"limit":   50,
		})
		if err != nil {
			return
		}
		text := extractText(result)
		if text == "" || text == "null" {
			return
		}
		var codeResp searchCodeResponse
		if err := json.Unmarshal([]byte(text), &codeResp); err != nil {
			return
		}
		for j, match := range codeResp.Results {
			if j >= 10 || match.QualifiedName == "" {
				continue
			}
			snippetResult, err := caller.CallTool(ctx, "get_code_snippet", map[string]interface{}{
				"project":        entry.projectName,
				"qualified_name": match.QualifiedName,
			})
			if err != nil {
				continue
			}
			snippetText := extractText(snippetResult)
			if snippetText == "" {
				continue
			}
			var snippet codeSnippetResponse
			if err := json.Unmarshal([]byte(snippetText), &snippet); err != nil {
				continue
			}
			calls := parseInternalRequestCalls(snippet.Source)
			for _, call := range calls {
				db.InsertAPIContract(orgdb.APIContract{
					ConsumerRepo:   entry.repoName,
					Method:         strings.ToUpper(call.method),
					Path:           "/" + call.serviceName + "/" + call.route,
					ConsumerSymbol: match.Node,
					Confidence:     0.5,
				})
				count.Add(1)
			}
		}
	})

	n := int(count.Load())
	slog.Info("phase 2b complete", "consumers", n)
	return n
}

// ── Phase 2c: Package deps (parallel) ──

func extractPackageDeps(ctx context.Context, db *orgdb.DB, caller MCPCaller, entries []projEntry) int {
	slog.Info("phase 2c: extracting package dependencies", "projects", len(entries), "workers", pipelineWorkers)
	var count atomic.Int64

	parallelScan(entries, pipelineWorkers, func(entry projEntry) {
		for _, scope := range []string{"@platform-core/", "@platform-ui/", "@gohighlevel/", "@frontend-core/"} {
			result, err := caller.CallTool(ctx, "search_code", map[string]interface{}{
				"project": entry.projectName,
				"pattern": scope,
				"limit":   20,
			})
			if err != nil {
				continue
			}
			text := extractText(result)
			if text == "" || text == "null" {
				continue
			}
			var codeResp searchCodeResponse
			if err := json.Unmarshal([]byte(text), &codeResp); err != nil {
				continue
			}
			seen := make(map[string]bool)
			for j, match := range codeResp.Results {
				if j >= 3 || match.QualifiedName == "" {
					continue
				}
				snippetResult, err := caller.CallTool(ctx, "get_code_snippet", map[string]interface{}{
					"project":        entry.projectName,
					"qualified_name": match.QualifiedName,
				})
				if err != nil {
					continue
				}
				snippetText := extractText(snippetResult)
				if snippetText == "" {
					continue
				}
				var snippet codeSnippetResponse
				if err := json.Unmarshal([]byte(snippetText), &snippet); err != nil {
					continue
				}
				pkgs := parsePackageImports(snippet.Source, scope)
				for _, pkg := range pkgs {
					if seen[pkg] {
						continue
					}
					seen[pkg] = true
					scopePart := strings.TrimSuffix(scope, "/")
					db.UpsertPackageDep(entry.repoName, orgdb.Dep{
						Scope:   scopePart,
						Name:    pkg,
						DepType: "dependencies",
					})
					count.Add(1)
				}
			}
		}
	})

	n := int(count.Load())
	slog.Info("phase 2c complete", "packages", n)
	return n
}

// ── Phase 2d: Event contracts (parallel) ──

var (
	consumerTopicRe = regexp.MustCompile(`@(?:Event|Message)Pattern\(\s*['"]([^'"]+)['"]`)
	producerTopicRe = regexp.MustCompile(`(?:pubSub|this\.(?:pubSub|client|eventBus))\.(?:publish|emit|send)\(\s*['"]([^'"]+)['"]`)
)

func extractEventContracts(ctx context.Context, db *orgdb.DB, caller MCPCaller, entries []projEntry) int {
	slog.Info("phase 2d: extracting event contracts", "projects", len(entries), "workers", pipelineWorkers)
	var count atomic.Int64

	searches := []struct {
		query string
		role  string
		re    *regexp.Regexp
	}{
		{"EventPattern", "consumer", consumerTopicRe},
		{"MessagePattern", "consumer", consumerTopicRe},
		{"publish", "producer", producerTopicRe},
		{"emit", "producer", producerTopicRe},
	}

	parallelScan(entries, pipelineWorkers, func(entry projEntry) {
		for _, search := range searches {
			result, err := caller.CallTool(ctx, "search_graph", map[string]interface{}{
				"project": entry.projectName,
				"query":   search.query,
				"limit":   20,
			})
			if err != nil {
				continue
			}
			text := extractText(result)
			if text == "" || text == "null" {
				continue
			}
			var resp searchGraphResponse
			if err := json.Unmarshal([]byte(text), &resp); err != nil {
				continue
			}
			for j, node := range resp.Results {
				if j >= 5 || node.QualifiedName == "" {
					continue
				}
				snippetResult, err := caller.CallTool(ctx, "get_code_snippet", map[string]interface{}{
					"project":        entry.projectName,
					"qualified_name": node.QualifiedName,
				})
				if err != nil {
					continue
				}
				snippetText := extractText(snippetResult)
				if snippetText == "" {
					continue
				}
				var snippet codeSnippetResponse
				if err := json.Unmarshal([]byte(snippetText), &snippet); err != nil {
					continue
				}
				topics := search.re.FindAllStringSubmatch(snippet.Source, -1)
				for _, tm := range topics {
					contract := orgdb.EventContract{
						Topic:     tm[1],
						EventType: "pubsub",
					}
					if search.role == "producer" {
						contract.ProducerRepo = entry.repoName
						contract.ProducerSymbol = node.Name
					} else {
						contract.ConsumerRepo = entry.repoName
						contract.ConsumerSymbol = node.Name
					}
					db.InsertEventContract(contract)
					count.Add(1)
				}
			}
		}
	})

	n := int(count.Load())
	slog.Info("phase 2d complete", "events", n)
	return n
}

// ── Types ──

type projEntry struct {
	projectName string
	repoName    string
}

type searchGraphResponse struct {
	Total   int              `json:"total"`
	Results []searchGraphNode `json:"results"`
	HasMore bool             `json:"has_more"`
}

type searchGraphNode struct {
	Name          string `json:"name"`
	QualifiedName string `json:"qualified_name"`
	Label         string `json:"label"`
	FilePath      string `json:"file_path"`
}

type searchCodeResponse struct {
	Results []searchCodeResult `json:"results"`
}

type searchCodeResult struct {
	Node          string `json:"node"`
	QualifiedName string `json:"qualified_name"`
	Label         string `json:"label"`
	File          string `json:"file"`
	StartLine     int    `json:"start_line"`
	EndLine       int    `json:"end_line"`
	MatchLines    []int  `json:"match_lines"`
}

type codeSnippetResponse struct {
	Name          string `json:"name"`
	QualifiedName string `json:"qualified_name"`
	Source        string `json:"source"`
	FilePath      string `json:"file_path"`
}

type projectInfo struct {
	Name  string `json:"name"`
	Nodes int    `json:"nodes"`
	Edges int    `json:"edges"`
}

type internalCall struct {
	method      string
	serviceName string
	route       string
}

// ── Parsers ──

func parseRouteQualifiedName(qn string) (string, string) {
	const prefix = "__route__"
	if !strings.HasPrefix(qn, prefix) {
		return "", ""
	}
	rest := qn[len(prefix):]
	idx := strings.Index(rest, "__")
	if idx < 0 {
		return "", ""
	}
	method := rest[:idx]
	path := rest[idx+2:]
	if path == "" {
		return "", ""
	}
	return strings.ToUpper(method), path
}

var (
	irMethodRe      = regexp.MustCompile(`InternalRequest\.(get|post|put|delete|patch)\(`)
	irServiceNameRe = regexp.MustCompile(`serviceName:\s*(?:SERVICE_NAME\.)?['"]?([A-Z][A-Z0-9_]+)`)
	irRouteRe       = regexp.MustCompile("route:\\s*[`'\"]([^`'\"]+)")
	templateExprRe  = regexp.MustCompile(`\$\{[^}]+\}`)
)

func parseInternalRequestCalls(source string) []internalCall {
	methodMatches := irMethodRe.FindAllStringSubmatchIndex(source, -1)
	var calls []internalCall

	for _, loc := range methodMatches {
		method := source[loc[2]:loc[3]]
		end := loc[1] + 500
		if end > len(source) {
			end = len(source)
		}
		block := source[loc[1]:end]

		snMatch := irServiceNameRe.FindStringSubmatch(block)
		routeMatch := irRouteRe.FindStringSubmatch(block)

		if snMatch != nil && routeMatch != nil {
			route := routeMatch[1]
			route = templateExprRe.ReplaceAllString(route, "*")
			route = strings.TrimPrefix(route, "/")
			if route != "" {
				calls = append(calls, internalCall{
					method:      method,
					serviceName: snMatch[1],
					route:       route,
				})
			}
		}
	}
	return calls
}

func parsePackageImports(source, scope string) []string {
	var pkgs []string
	seen := make(map[string]bool)
	re := regexp.MustCompile(regexp.QuoteMeta(scope) + `([a-zA-Z0-9_-]+)`)
	matches := re.FindAllStringSubmatch(source, -1)
	for _, m := range matches {
		if len(m) >= 2 && !seen[m[1]] {
			seen[m[1]] = true
			pkgs = append(pkgs, m[1])
		}
	}
	return pkgs
}

func stripProjectPrefix(name string) string {
	for _, prefix := range []string{
		"data-fleet-cache-repos-",
		"tmp-fleet-cache-repos-",
		"tmp-fleet-cache-",
		"app-fleet-cache-",
	} {
		if strings.HasPrefix(name, prefix) {
			return strings.TrimPrefix(name, prefix)
		}
	}
	return name
}

func waitForProjects(ctx context.Context, caller MCPCaller, db *orgdb.DB,
	repoByName map[string]manifest.Repo, repos []manifest.Repo,
	minCount int, timeout time.Duration) []projEntry {

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		time.Sleep(30 * time.Second)
		result, err := caller.CallTool(ctx, "list_projects", nil)
		if err != nil {
			continue
		}
		text := extractText(result)
		if text == "" || text == "null" {
			continue
		}
		var projects []projectInfo
		if err := json.Unmarshal([]byte(text), &projects); err != nil {
			var wrapped struct{ Projects []projectInfo }
			if err2 := json.Unmarshal([]byte(text), &wrapped); err2 != nil {
				continue
			}
			projects = wrapped.Projects
		}
		slog.Info("waitForProjects: poll", "found", len(projects), "need", minCount)
		if len(projects) >= minCount {
			return buildEntries(projects, db, repoByName)
		}
	}

	slog.Warn("waitForProjects: timeout")
	result, err := caller.CallTool(ctx, "list_projects", nil)
	if err != nil {
		return nil
	}
	text := extractText(result)
	var projects []projectInfo
	if err := json.Unmarshal([]byte(text), &projects); err != nil {
		return nil
	}
	return buildEntries(projects, db, repoByName)
}

func buildEntries(projects []projectInfo, db *orgdb.DB, repoByName map[string]manifest.Repo) []projEntry {
	var entries []projEntry
	for _, proj := range projects {
		repoName := stripProjectPrefix(proj.Name)
		repo := repoByName[repoName]
		db.UpsertRepo(orgdb.RepoRecord{
			Name:      repoName,
			GitHubURL: repo.GitHubURL,
			Team:      repo.Team,
			Type:      repo.Type,
			NodeCount: proj.Nodes,
			EdgeCount: proj.Edges,
		})
		db.UpsertTeamOwnership(repoName, repo.Team, "")
		entries = append(entries, projEntry{projectName: proj.Name, repoName: repoName})
	}
	return entries
}

func extractText(result *mcp.ToolResult) string {
	if result == nil || len(result.Content) == 0 {
		return ""
	}
	return result.Content[0].Text
}
