// Package pipeline — PopulateOrgFromProjectDBsDirect reads project .db files
// directly with SQL queries instead of making ~19,000 MCP bridge calls.
// Reduces org.db population from ~20 minutes to ~30 seconds.
package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
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

// ── Phase 2b: InternalRequest consumers (direct SQL) ──

func directExtractConsumers(ctx context.Context, orgDB *orgdb.DB, entries []directEntry, cacheDir string) int {
	slog.Info("direct-sql: phase 2b: extracting consumers", "projects", len(entries))
	var count atomic.Int64

	parallelScanDirect(entries, directWorkers, func(e directEntry) {
		db, err := openReadOnly(e.dbPath)
		if err != nil {
			return
		}
		defer db.Close()

		// Find nodes containing "InternalRequest" in name or qualified_name
		rows, err := db.QueryContext(ctx,
			`SELECT qualified_name, name, file_path, start_line, end_line
			 FROM nodes
			 WHERE (name LIKE '%InternalRequest%' OR qualified_name LIKE '%InternalRequest%')
			 LIMIT 50`)
		if err != nil {
			return
		}
		defer rows.Close()

		type match struct {
			qn, name, filePath string
			startLine, endLine int
		}
		var matches []match
		for rows.Next() {
			var m match
			if err := rows.Scan(&m.qn, &m.name, &m.filePath, &m.startLine, &m.endLine); err != nil {
				continue
			}
			matches = append(matches, m)
		}

		// For each match, read the source file and parse InternalRequest calls
		for i, m := range matches {
			if i >= 10 {
				break
			}
			source := readSourceFromFile(cacheDir, e.dbPath, m.filePath, m.startLine, m.endLine)
			if source == "" {
				continue
			}
			calls := parseInternalRequestCalls(source)
			for _, call := range calls {
				orgDB.InsertAPIContract(orgdb.APIContract{
					ConsumerRepo:   e.repoName,
					Method:         strings.ToUpper(call.method),
					Path:           "/" + call.serviceName + "/" + call.route,
					ConsumerSymbol: m.name,
					Confidence:     0.5,
				})
				count.Add(1)
			}
		}
	})

	n := int(count.Load())
	slog.Info("direct-sql: phase 2b complete", "consumers", n)
	return n
}

// ── Phase 2c: Package dependencies (direct SQL) ──

func directExtractPackageDeps(ctx context.Context, orgDB *orgdb.DB, entries []directEntry, cacheDir string) int {
	slog.Info("direct-sql: phase 2c: extracting package deps", "projects", len(entries))
	var count atomic.Int64

	scopes := []string{"@platform-core/", "@platform-ui/", "@gohighlevel/", "@frontend-core/"}

	parallelScanDirect(entries, directWorkers, func(e directEntry) {
		db, err := openReadOnly(e.dbPath)
		if err != nil {
			return
		}
		defer db.Close()

		for _, scope := range scopes {
			// Search nodes whose name or qualified_name contains the scope
			rows, err := db.QueryContext(ctx,
				`SELECT qualified_name, name, file_path, start_line, end_line
				 FROM nodes
				 WHERE (name LIKE ? OR qualified_name LIKE ?)
				 LIMIT 20`,
				"%"+scope+"%", "%"+scope+"%")
			if err != nil {
				continue
			}

			type match struct {
				qn, name, filePath string
				startLine, endLine int
			}
			var matches []match
			for rows.Next() {
				var m match
				if err := rows.Scan(&m.qn, &m.name, &m.filePath, &m.startLine, &m.endLine); err != nil {
					continue
				}
				matches = append(matches, m)
			}
			rows.Close()

			seen := make(map[string]bool)
			for i, m := range matches {
				if i >= 3 {
					break
				}
				source := readSourceFromFile(cacheDir, e.dbPath, m.filePath, m.startLine, m.endLine)
				if source == "" {
					continue
				}
				pkgs := parsePackageImports(source, scope)
				for _, pkg := range pkgs {
					if seen[pkg] {
						continue
					}
					seen[pkg] = true
					scopePart := strings.TrimSuffix(scope, "/")
					orgDB.UpsertPackageDep(e.repoName, orgdb.Dep{
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
	slog.Info("direct-sql: phase 2c complete", "packages", n)
	return n
}

// ── Phase 2d: Event contracts (direct SQL) ──

var (
	directConsumerTopicRe = regexp.MustCompile(`@(?:Event|Message)Pattern\(\s*['"]([^'"]+)['"]`)
	directProducerTopicRe = regexp.MustCompile(`(?:pubSub|this\.(?:pubSub|client|eventBus))\.(?:publish|emit|send)\(\s*['"]([^'"]+)['"]`)
)

func directExtractEventContracts(ctx context.Context, orgDB *orgdb.DB, entries []directEntry, cacheDir string) int {
	slog.Info("direct-sql: phase 2d: extracting events", "projects", len(entries))
	var count atomic.Int64

	searches := []struct {
		query string
		role  string
		re    *regexp.Regexp
	}{
		{"EventPattern", "consumer", directConsumerTopicRe},
		{"MessagePattern", "consumer", directConsumerTopicRe},
		{"publish", "producer", directProducerTopicRe},
		{"emit", "producer", directProducerTopicRe},
	}

	parallelScanDirect(entries, directWorkers, func(e directEntry) {
		db, err := openReadOnly(e.dbPath)
		if err != nil {
			return
		}
		defer db.Close()

		for _, search := range searches {
			rows, err := db.QueryContext(ctx,
				`SELECT qualified_name, name, file_path, start_line, end_line
				 FROM nodes
				 WHERE (name LIKE ? OR qualified_name LIKE ?)
				 LIMIT 20`,
				"%"+search.query+"%", "%"+search.query+"%")
			if err != nil {
				continue
			}

			type match struct {
				qn, name, filePath string
				startLine, endLine int
			}
			var matches []match
			for rows.Next() {
				var m match
				if err := rows.Scan(&m.qn, &m.name, &m.filePath, &m.startLine, &m.endLine); err != nil {
					continue
				}
				matches = append(matches, m)
			}
			rows.Close()

			for i, m := range matches {
				if i >= 5 {
					break
				}
				source := readSourceFromFile(cacheDir, e.dbPath, m.filePath, m.startLine, m.endLine)
				if source == "" {
					continue
				}
				topics := search.re.FindAllStringSubmatch(source, -1)
				for _, tm := range topics {
					contract := orgdb.EventContract{
						Topic:     tm[1],
						EventType: "pubsub",
					}
					if search.role == "producer" {
						contract.ProducerRepo = e.repoName
						contract.ProducerSymbol = m.name
					} else {
						contract.ConsumerRepo = e.repoName
						contract.ConsumerSymbol = m.name
					}
					orgDB.InsertEventContract(contract)
					count.Add(1)
				}
			}
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

// readSourceFromFile reads source code lines from the repo clone.
// Falls back to empty string if file doesn't exist (no git clone available).
func readSourceFromFile(cacheDir, dbPath, relFilePath string, startLine, endLine int) string {
	if relFilePath == "" || startLine <= 0 {
		return ""
	}

	// Derive clone dir from project name: cbmCacheDir/../fleet-repos/<repoName>
	// Or try common patterns
	projectName := strings.TrimSuffix(filepath.Base(dbPath), ".db")
	repoName := stripProjectPrefix(projectName)

	// Try common clone locations
	candidates := []string{
		filepath.Join(filepath.Dir(cacheDir), "fleet-repos", repoName, relFilePath),
		filepath.Join("/tmp/fleet-repos", repoName, relFilePath),
		filepath.Join("/data/fleet-cache/repos", repoName, relFilePath),
	}

	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		lines := strings.Split(string(data), "\n")
		if startLine > len(lines) {
			return string(data) // return all if range is invalid
		}
		end := endLine
		if end > len(lines) || end <= 0 {
			end = len(lines)
		}
		return strings.Join(lines[startLine-1:end], "\n")
	}
	return ""
}
