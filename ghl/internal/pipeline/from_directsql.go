// Package pipeline — PopulateOrgFromProjectDBsDirect reads project .db files
// directly with SQL queries instead of making ~19,000 MCP bridge calls.
// Reduces org.db population from ~20 minutes to ~30 seconds.
package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	_ "modernc.org/sqlite"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdb"
)

const directWorkers = 16

// PopulateOrgFromProjectDBsDirect builds org.db by reading project SQLite files
// directly — no MCP bridge calls. ~30s instead of ~20min.
func PopulateOrgFromProjectDBsDirect(ctx context.Context, orgDB *orgdb.DB, repos []manifest.Repo, cbmCacheDir string) error {
	// Find all project .db files
	entries, err := discoverProjectDBs(cbmCacheDir, repos)
	if err != nil {
		return fmt.Errorf("discover project dbs: %w", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("no project .db files found in %s", cbmCacheDir)
	}

	slog.Info("direct-sql: starting org.db population", "projects", len(entries), "workers", directWorkers)

	// Phase 1: Repo metadata (fast — just count nodes/edges per project)
	for _, e := range entries {
		orgDB.UpsertRepo(orgdb.RepoRecord{
			Name:      e.repoName,
			GitHubURL: e.repo.GitHubURL,
			Team:      e.repo.Team,
			Type:      e.repo.Type,
			NodeCount: e.nodeCount,
			EdgeCount: e.edgeCount,
		})
		orgDB.UpsertTeamOwnership(e.repoName, e.repo.Team, "")
	}
	slog.Info("direct-sql: phase 1 complete", "repos", len(entries))

	// Phase 2: All extraction phases in parallel
	var routeCount, consumerCount, packageCount, eventCount int64
	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		n := directExtractRoutes(ctx, orgDB, entries, cbmCacheDir)
		atomic.StoreInt64(&routeCount, int64(n))
	}()
	go func() {
		defer wg.Done()
		n := directExtractConsumers(ctx, orgDB, entries, cbmCacheDir)
		atomic.StoreInt64(&consumerCount, int64(n))
	}()
	go func() {
		defer wg.Done()
		n := directExtractPackageDeps(ctx, orgDB, entries, cbmCacheDir)
		atomic.StoreInt64(&packageCount, int64(n))
	}()
	go func() {
		defer wg.Done()
		n := directExtractEventContracts(ctx, orgDB, entries, cbmCacheDir)
		atomic.StoreInt64(&eventCount, int64(n))
	}()

	wg.Wait()

	rc := atomic.LoadInt64(&routeCount)
	cc := atomic.LoadInt64(&consumerCount)
	pc := atomic.LoadInt64(&packageCount)
	ec := atomic.LoadInt64(&eventCount)

	// Phase 2e: Infer package providers
	providerCount, provErr := orgDB.InferPackageProviders()
	if provErr != nil {
		slog.Warn("direct-sql: infer package providers failed", "err", provErr)
	} else {
		slog.Info("direct-sql: phase 2e complete", "providers", providerCount)
	}

	// Phase 3: Cross-reference contracts
	if rc > 0 {
		fixCount, fixErr := orgDB.FixRoutePaths()
		if fixErr != nil {
			slog.Warn("direct-sql: fix route paths failed", "err", fixErr)
		} else if fixCount > 0 {
			slog.Info("direct-sql: fixed route paths", "count", fixCount)
		}
	}

	matched := 0
	if rc > 0 && cc > 0 {
		var err error
		matched, err = orgDB.CrossReferenceContracts()
		if err != nil {
			slog.Warn("direct-sql: cross-reference failed", "err", err)
		} else {
			slog.Info("direct-sql: phase 3 complete", "api_matched", matched)
		}
	}

	if ec > 0 {
		eventMatched, err := orgDB.CrossReferenceEventContracts()
		if err != nil {
			slog.Warn("direct-sql: cross-reference events failed", "err", err)
		} else {
			slog.Info("direct-sql: event cross-reference complete", "matched", eventMatched)
		}
	}

	slog.Info("direct-sql: org.db fully populated",
		"repos", len(entries), "routes", rc, "consumers", cc,
		"events", ec, "packages", pc, "cross_referenced", matched)
	return nil
}

// ── Project discovery ──

type directEntry struct {
	dbPath    string
	repoName  string
	repo      manifest.Repo
	nodeCount int
	edgeCount int
}

func discoverProjectDBs(cbmCacheDir string, repos []manifest.Repo) ([]directEntry, error) {
	repoByName := make(map[string]manifest.Repo, len(repos))
	for _, r := range repos {
		repoByName[r.Name] = r
	}

	pattern := filepath.Join(cbmCacheDir, "*.db")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	var entries []directEntry
	for _, dbPath := range matches {
		base := filepath.Base(dbPath)
		if base == "org.db" || strings.HasPrefix(base, ".") {
			continue
		}
		projectName := strings.TrimSuffix(base, ".db")
		repoName := stripProjectPrefix(projectName)
		repo := repoByName[repoName]

		// Quick stat: count nodes and edges
		nodeCount, edgeCount := quickDBStats(dbPath)
		if nodeCount == 0 {
			continue
		}

		entries = append(entries, directEntry{
			dbPath:    dbPath,
			repoName:  repoName,
			repo:      repo,
			nodeCount: nodeCount,
			edgeCount: edgeCount,
		})
	}
	return entries, nil
}

func quickDBStats(dbPath string) (nodes, edges int) {
	db, err := openReadOnly(dbPath)
	if err != nil {
		return 0, 0
	}
	defer db.Close()
	db.QueryRow("SELECT COUNT(*) FROM nodes").Scan(&nodes)
	db.QueryRow("SELECT COUNT(*) FROM edges").Scan(&edges)
	return
}

func openReadOnly(dbPath string) (*sql.DB, error) {
	if _, err := os.Stat(dbPath); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// ── Phase 2a: Routes (direct SQL) ──

func directExtractRoutes(ctx context.Context, orgDB *orgdb.DB, entries []directEntry, cacheDir string) int {
	slog.Info("direct-sql: phase 2a: extracting routes", "projects", len(entries))
	var count atomic.Int64

	parallelScanDirect(entries, directWorkers, func(e directEntry) {
		db, err := openReadOnly(e.dbPath)
		if err != nil {
			return
		}
		defer db.Close()

		rows, err := db.QueryContext(ctx,
			`SELECT qualified_name, name FROM nodes WHERE label = 'Route' LIMIT 500`)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var qn, name string
			if err := rows.Scan(&qn, &name); err != nil {
				continue
			}
			method, path := parseRouteQualifiedName(qn)
			if path == "" {
				continue
			}
			orgDB.InsertAPIContract(orgdb.APIContract{
				ProviderRepo:   e.repoName,
				Method:         method,
				Path:           path,
				ProviderSymbol: name,
				Confidence:     0.3,
			})
			count.Add(1)
		}
	})

	n := int(count.Load())
	slog.Info("direct-sql: phase 2a complete", "routes", n)
	return n
}

// ── Phase 2b: InternalRequest consumers (direct SQL via edges) ──

func directExtractConsumers(ctx context.Context, orgDB *orgdb.DB, entries []directEntry, cacheDir string) int {
	slog.Info("direct-sql: phase 2b: extracting consumers", "projects", len(entries))
	var count atomic.Int64

	parallelScanDirect(entries, directWorkers, func(e directEntry) {
		db, err := openReadOnly(e.dbPath)
		if err != nil {
			return
		}
		defer db.Close()

		// Extract HTTP_CALLS edges — these represent InternalRequest calls
		// The C binary indexes these during the initial repo indexing pass.
		// Edge properties contain url_path and method info.
		rows, err := db.QueryContext(ctx,
			`SELECT src.name, e.properties
			 FROM edges e
			 JOIN nodes src ON e.source_id = src.id
			 WHERE e.type IN ('HTTP_CALLS', 'ASYNC_CALLS')
			 LIMIT 200`)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var srcName, propsJSON string
			if err := rows.Scan(&srcName, &propsJSON); err != nil {
				continue
			}
			// Parse edge properties for url_path and method
			method, path := parseEdgeHTTPProps(propsJSON)
			if path == "" {
				continue
			}
			orgDB.InsertAPIContract(orgdb.APIContract{
				ConsumerRepo:   e.repoName,
				Method:         method,
				Path:           path,
				ConsumerSymbol: srcName,
				Confidence:     0.5,
			})
			count.Add(1)
		}
	})

	n := int(count.Load())
	slog.Info("direct-sql: phase 2b complete", "consumers", n)
	return n
}

// PopulatePackageDepsOnly runs ONLY Phase 2c (package dependencies) against
// an existing org.db. Used to repair hydrated org.db files that were
// persisted before the package.json-based population was added.
//
// Safe to call when the other phases are already populated — it only touches
// the packages and repo_dependencies tables via UpsertPackageDep which
// handles deduplication.
func PopulatePackageDepsOnly(ctx context.Context, orgDB *orgdb.DB, repos []manifest.Repo, cbmCacheDir string) error {
	entries, err := discoverProjectDBs(cbmCacheDir, repos)
	if err != nil {
		return fmt.Errorf("discover project dbs: %w", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("no project .db files found in %s", cbmCacheDir)
	}
	slog.Info("direct-sql: backfilling package deps on hydrated org.db", "projects", len(entries))
	n := directExtractPackageDeps(ctx, orgDB, entries, cbmCacheDir)
	// Phase 2e: infer providers from repo names.
	providerCount, provErr := orgDB.InferPackageProviders()
	if provErr != nil {
		slog.Warn("direct-sql: infer package providers failed", "err", provErr)
	} else {
		slog.Info("direct-sql: providers backfilled", "providers", providerCount)
	}
	slog.Info("direct-sql: package deps backfill complete", "packages", n)
	return nil
}

// ── Phase 2c: Package dependencies (direct SQL via IMPORTS edges) ──

func directExtractPackageDeps(ctx context.Context, orgDB *orgdb.DB, entries []directEntry, cacheDir string) int {
	slog.Info("direct-sql: phase 2c: extracting package deps", "projects", len(entries))
	var count atomic.Int64

	// Primary source: read package.json from GCS Fuse mount.
	// GCS Fuse is at /data/fleet-cache/repos/<repoName>/
	cloneDirs := []string{"/data/fleet-cache/repos", "/tmp/fleet-repos"}

	parallelScanDirect(entries, directWorkers, func(e directEntry) {
		// Try to read package.json from clone dirs
		for _, baseDir := range cloneDirs {
			pkgPath := filepath.Join(baseDir, e.repoName, "package.json")
			deps, err := orgdb.ParsePackageJSON(pkgPath)
			if err != nil {
				continue
			}
			for _, dep := range deps {
				orgDB.UpsertPackageDep(e.repoName, dep)
				count.Add(1)
			}
			// Also set this repo as package provider if it IS a GHL internal package
			if scope, name, err := orgdb.ParsePackageName(pkgPath); err == nil && scope != "" {
				orgDB.SetPackageProvider(scope, name, e.repoName)
			}
			return // found package.json, done for this repo
		}

		// Fallback: query IMPORTS edges from project .db
		db, err := openReadOnly(e.dbPath)
		if err != nil {
			return
		}
		defer db.Close()

		rows, err := db.QueryContext(ctx,
			`SELECT DISTINCT tgt.name, tgt.qualified_name
			 FROM edges e
			 JOIN nodes tgt ON e.target_id = tgt.id
			 WHERE e.type = 'IMPORTS'
			 LIMIT 500`)
		if err != nil {
			return
		}

		scopes := []string{"@platform-core/", "@platform-ui/", "@gohighlevel/", "@frontend-core/"}
		seen := make(map[string]bool)
		for rows.Next() {
			var name, qn string
			if err := rows.Scan(&name, &qn); err != nil {
				continue
			}
			for _, scope := range scopes {
				scopePart := strings.TrimSuffix(scope, "/")
				if strings.Contains(name, scope) || strings.Contains(qn, scope) {
					pkg := extractPackageFromImport(name, qn, scope)
					if pkg != "" && !seen[scopePart+"/"+pkg] {
						seen[scopePart+"/"+pkg] = true
						orgDB.UpsertPackageDep(e.repoName, orgdb.Dep{
							Scope:   scopePart,
							Name:    pkg,
							DepType: "dependencies",
						})
						count.Add(1)
					}
				}
			}
		}
		rows.Close()
	})

	n := int(count.Load())
	slog.Info("direct-sql: phase 2c complete", "packages", n)
	return n
}

// ── Phase 2d: Event contracts (direct SQL via edges + node properties) ──

func directExtractEventContracts(ctx context.Context, orgDB *orgdb.DB, entries []directEntry, cacheDir string) int {
	slog.Info("direct-sql: phase 2d: extracting events", "projects", len(entries))
	var count atomic.Int64

	parallelScanDirect(entries, directWorkers, func(e directEntry) {
		db, err := openReadOnly(e.dbPath)
		if err != nil {
			return
		}
		defer db.Close()

		// Extract PUBLISHES/SUBSCRIBES edges — the C binary creates these for event patterns
		rows, err := db.QueryContext(ctx,
			`SELECT src.name, tgt.name, e.type, e.properties
			 FROM edges e
			 JOIN nodes src ON e.source_id = src.id
			 JOIN nodes tgt ON e.target_id = tgt.id
			 WHERE e.type IN ('PUBLISHES', 'SUBSCRIBES', 'EMITS', 'LISTENS')
			 LIMIT 200`)
		if err == nil {
			for rows.Next() {
				var srcName, tgtName, edgeType, propsJSON string
				if err := rows.Scan(&srcName, &tgtName, &edgeType, &propsJSON); err != nil {
					continue
				}
				topic := extractTopicFromEdge(tgtName, propsJSON)
				if topic == "" {
					topic = tgtName // fallback: use target node name as topic
				}
				contract := orgdb.EventContract{
					Topic:     topic,
					EventType: "pubsub",
				}
				if edgeType == "PUBLISHES" || edgeType == "EMITS" {
					contract.ProducerRepo = e.repoName
					contract.ProducerSymbol = srcName
				} else {
					contract.ConsumerRepo = e.repoName
					contract.ConsumerSymbol = srcName
				}
				orgDB.InsertEventContract(contract)
				count.Add(1)
			}
			rows.Close()
		}

		// Fallback: scan nodes with EventPattern/MessagePattern in their name
		// These are decorator-annotated methods that the C binary may index as plain nodes
		patternRows, err := db.QueryContext(ctx,
			`SELECT name, qualified_name, properties FROM nodes
			 WHERE name LIKE '%EventPattern%' OR name LIKE '%MessagePattern%'
			    OR qualified_name LIKE '%EventPattern%' OR qualified_name LIKE '%MessagePattern%'
			 LIMIT 50`)
		if err == nil {
			for patternRows.Next() {
				var name, qn, props string
				if err := patternRows.Scan(&name, &qn, &props); err != nil {
					continue
				}
				topic := extractTopicFromProps(props, name)
				if topic == "" {
					continue
				}
				orgDB.InsertEventContract(orgdb.EventContract{
					Topic:         topic,
					EventType:     "pubsub",
					ConsumerRepo:   e.repoName,
					ConsumerSymbol: name,
				})
				count.Add(1)
			}
			patternRows.Close()
		}
	})

	n := int(count.Load())
	slog.Info("direct-sql: phase 2d complete", "events", n)
	return n
}

// ── Helpers ──

func parallelScanDirect(entries []directEntry, workers int, fn func(e directEntry)) {
	ch := make(chan directEntry, len(entries))
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

// parseEdgeHTTPProps extracts method and path from edge properties JSON.
// Properties look like: {"url_path": "/api/v1/users", "method": "GET"}
func parseEdgeHTTPProps(propsJSON string) (method, path string) {
	if propsJSON == "" || propsJSON == "{}" {
		return "", ""
	}
	var props map[string]interface{}
	if err := json.Unmarshal([]byte(propsJSON), &props); err != nil {
		return "", ""
	}
	if p, ok := props["url_path"].(string); ok && p != "" {
		path = p
	} else if p, ok := props["route"].(string); ok && p != "" {
		path = p
	} else if p, ok := props["path"].(string); ok && p != "" {
		path = p
	}
	if m, ok := props["method"].(string); ok && m != "" {
		method = strings.ToUpper(m)
	} else {
		method = "GET" // default
	}
	return
}

// extractPackageFromImport extracts the package name from an import path.
// e.g., "@platform-core/base-service" → "base-service"
func extractPackageFromImport(name, qn, scope string) string {
	for _, s := range []string{name, qn} {
		idx := strings.Index(s, scope)
		if idx < 0 {
			continue
		}
		rest := s[idx+len(scope):]
		// Take until next / or end
		if slashIdx := strings.Index(rest, "/"); slashIdx >= 0 {
			rest = rest[:slashIdx]
		}
		// Clean up non-alphanumeric suffixes
		rest = strings.TrimRight(rest, "\"'`;,) ")
		if rest != "" {
			return rest
		}
	}
	return ""
}

// extractTopicFromEdge extracts a topic name from edge properties or target name.
func extractTopicFromEdge(targetName, propsJSON string) string {
	if propsJSON != "" && propsJSON != "{}" {
		var props map[string]interface{}
		if err := json.Unmarshal([]byte(propsJSON), &props); err == nil {
			if t, ok := props["topic"].(string); ok && t != "" {
				return t
			}
			if t, ok := props["event"].(string); ok && t != "" {
				return t
			}
			if t, ok := props["channel"].(string); ok && t != "" {
				return t
			}
		}
	}
	return ""
}

// extractTopicFromProps extracts a topic from node properties JSON.
func extractTopicFromProps(propsJSON, nodeName string) string {
	if propsJSON != "" && propsJSON != "{}" {
		var props map[string]interface{}
		if err := json.Unmarshal([]byte(propsJSON), &props); err == nil {
			if t, ok := props["topic"].(string); ok && t != "" {
				return t
			}
			if t, ok := props["pattern"].(string); ok && t != "" {
				return t
			}
		}
	}
	return ""
}
