package pipeline

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/zeebo/xxh3"
	"golang.org/x/sync/errgroup"

	"github.com/DeusData/codebase-memory-mcp/internal/cbm"
	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/httplink"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// Pipeline orchestrates the 3-pass indexing of a repository.
type Pipeline struct {
	ctx         context.Context
	Store       *store.Store
	RepoPath    string
	ProjectName string
	Mode        discover.IndexMode
	// buf holds all nodes/edges in memory during full-index passes 1-14.
	// nil during incremental mode and post-flush passes 15-18.
	buf *GraphBuffer
	// extractionCache maps file rel_path -> CBM extraction result for all post-definition passes
	extractionCache map[string]*cachedExtraction
	// registry indexes all Function/Method/Class nodes for call resolution
	registry *FunctionRegistry
	// importMaps stores per-module import maps: moduleQN -> localName -> resolvedQN
	importMaps map[string]map[string]string
	// returnTypes maps function QN -> return type QN for return-type-based type inference
	returnTypes ReturnTypeMap
}

// New creates a new Pipeline.
func New(ctx context.Context, s *store.Store, repoPath string, mode discover.IndexMode) *Pipeline {
	if mode == "" {
		mode = discover.ModeFull
	}
	projectName := ProjectNameFromPath(repoPath)
	return &Pipeline{
		ctx:             ctx,
		Store:           s,
		RepoPath:        repoPath,
		ProjectName:     projectName,
		Mode:            mode,
		extractionCache: make(map[string]*cachedExtraction),
		registry:        NewFunctionRegistry(),
		importMaps:      make(map[string]map[string]string),
	}
}

// ProjectNameFromPath derives a unique project name from an absolute path
// by replacing path separators with dashes and trimming the leading dash.
func ProjectNameFromPath(absPath string) string {
	// Clean and normalize separators (backslash is not a separator on non-Windows)
	cleaned := filepath.ToSlash(filepath.Clean(absPath))
	cleaned = strings.ReplaceAll(cleaned, "\\", "/")
	// Replace slashes and colons with dashes
	name := strings.ReplaceAll(cleaned, "/", "-")
	name = strings.ReplaceAll(name, ":", "-")
	// Collapse consecutive dashes (e.g. C:/ → C--)
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}
	// Trim leading dash (from leading /)
	name = strings.TrimLeft(name, "-")
	if name == "" {
		return "root"
	}
	return name
}

// checkCancel returns ctx.Err() if the pipeline's context has been cancelled.
func (p *Pipeline) checkCancel() error {
	return p.ctx.Err()
}

// --- Bridge methods: dispatch to in-memory buffer or SQLite store ---

func (p *Pipeline) upsertNode(n *store.Node) error {
	if p.buf != nil {
		p.buf.UpsertNode(n)
		return nil
	}
	_, err := p.Store.UpsertNode(n)
	return err
}

func (p *Pipeline) upsertNodeBatch(nodes []*store.Node) (map[string]int64, error) {
	if p.buf != nil {
		return p.buf.UpsertNodeBatch(nodes), nil
	}
	return p.Store.UpsertNodeBatch(nodes)
}

func (p *Pipeline) insertEdge(e *store.Edge) error {
	if p.buf != nil {
		p.buf.InsertEdge(e)
		return nil
	}
	_, err := p.Store.InsertEdge(e)
	return err
}

func (p *Pipeline) insertEdgeBatch(edges []*store.Edge) error {
	if p.buf != nil {
		p.buf.InsertEdgeBatch(edges)
		return nil
	}
	return p.Store.InsertEdgeBatch(edges)
}

func (p *Pipeline) findNodesByLabel(project, label string) ([]*store.Node, error) {
	if p.buf != nil {
		return p.buf.FindNodesByLabel(label), nil
	}
	return p.Store.FindNodesByLabel(project, label)
}

func (p *Pipeline) findNodeByQN(project, qn string) (*store.Node, error) {
	if p.buf != nil {
		return p.buf.FindNodeByQN(qn), nil
	}
	return p.Store.FindNodeByQN(project, qn)
}

func (p *Pipeline) findNodeByID(id int64) (*store.Node, error) {
	if p.buf != nil {
		return p.buf.FindNodeByID(id), nil
	}
	return p.Store.FindNodeByID(id)
}

func (p *Pipeline) findNodeIDsByQNs(project string, qns []string) (map[string]int64, error) {
	if p.buf != nil {
		return p.buf.FindNodeIDsByQNs(qns), nil
	}
	return p.Store.FindNodeIDsByQNs(project, qns)
}

func (p *Pipeline) findEdgesBySourceAndType(sourceID int64, edgeType string) ([]*store.Edge, error) {
	if p.buf != nil {
		return p.buf.FindEdgesBySourceAndType(sourceID, edgeType), nil
	}
	return p.Store.FindEdgesBySourceAndType(sourceID, edgeType)
}

// Run executes the full 3-pass pipeline within a single transaction.
// If file hashes from a previous run exist, only changed files are re-processed.
func (p *Pipeline) Run() error {
	runStart := time.Now()
	slog.Info("pipeline.start", "project", p.ProjectName, "path", p.RepoPath, "mode", string(p.Mode))

	if err := p.checkCancel(); err != nil {
		return err
	}

	// Discover source files (filesystem, no DB — runs outside transaction)
	discoverOpts := &discover.Options{Mode: p.Mode}
	if p.Mode == discover.ModeFast {
		discoverOpts.MaxFileSize = 512 * 1024 // 512KB cutoff in fast mode
	}
	files, err := discover.Discover(p.ctx, p.RepoPath, discoverOpts)
	if err != nil {
		return fmt.Errorf("discover: %w", err)
	}
	slog.Info("pipeline.discovered", "files", len(files))

	// Use MEMORY journal mode during fresh indexing for faster bulk writes.
	p.Store.BeginBulkWrite(p.ctx)

	wroteData := false
	if err := p.Store.WithTransaction(p.ctx, func(txStore *store.Store) error {
		origStore := p.Store
		p.Store = txStore
		defer func() { p.Store = origStore }()
		var passErr error
		wroteData, passErr = p.runPasses(files)
		return passErr
	}); err != nil {
		p.Store.EndBulkWrite(p.ctx)
		return err
	}

	p.Store.EndBulkWrite(p.ctx)

	// Only checkpoint + optimize when actual data was written.
	// No-op incremental reindexes skip this to avoid ANALYZE overhead.
	if wroteData {
		p.Store.Checkpoint(p.ctx)
	}

	nc, _ := p.Store.CountNodes(p.ProjectName)
	ec, _ := p.Store.CountEdges(p.ProjectName)
	slog.Info("pipeline.done", "nodes", nc, "edges", ec, "total_elapsed", time.Since(runStart))
	return nil
}

// runPasses executes all indexing passes (called within a transaction).
// Returns (wroteData, error) — wroteData is true if nodes/edges were written.
func (p *Pipeline) runPasses(files []discover.FileInfo) (bool, error) {
	if err := p.Store.UpsertProject(p.ProjectName, p.RepoPath); err != nil {
		return false, fmt.Errorf("upsert project: %w", err)
	}

	// Classify files as changed/unchanged using stored hashes
	changed, unchanged := p.classifyFiles(files)

	// If all files are changed (first index or no hashes), do full pass
	isFullIndex := len(unchanged) == 0
	if isFullIndex {
		return true, p.runFullPasses(files)
	}

	slog.Info("incremental.classify", "changed", len(changed), "unchanged", len(unchanged), "total", len(files))

	// Fast path: nothing changed → skip all heavy passes
	if len(changed) == 0 {
		slog.Info("incremental.noop", "reason", "no_changes")
		return false, nil
	}

	return true, p.runIncrementalPasses(files, changed, unchanged)
}

// runFullPasses runs the complete pipeline (no incremental optimization).
func (p *Pipeline) runFullPasses(files []discover.FileInfo) error {
	// Initialize in-memory graph buffer for passes 1-14.
	// All node/edge writes go to RAM; flushed to SQLite after pass 14.
	p.buf = newGraphBuffer(p.ProjectName)

	t := time.Now()
	if err := p.passStructure(files); err != nil {
		return fmt.Errorf("pass1 structure: %w", err)
	}
	slog.Info("pass.timing", "pass", "structure", "elapsed", time.Since(t))
	if err := p.checkCancel(); err != nil {
		return err
	}

	t = time.Now()
	p.passDefinitions(files) // includes Variable extraction + enrichment
	slog.Info("pass.timing", "pass", "definitions", "elapsed", time.Since(t))
	if err := p.checkCancel(); err != nil {
		return err
	}

	t = time.Now()
	p.passDecoratorTags() // auto-discover decorator semantic tags
	slog.Info("pass.timing", "pass", "decorator_tags", "elapsed", time.Since(t))

	t = time.Now()
	p.buildRegistry() // includes Variable label
	slog.Info("pass.timing", "pass", "registry", "elapsed", time.Since(t))
	if err := p.checkCancel(); err != nil {
		return err
	}

	t = time.Now()
	p.passInherits() // INHERITS edges from base_classes
	slog.Info("pass.timing", "pass", "inherits", "elapsed", time.Since(t))

	t = time.Now()
	p.passDecorates() // DECORATES edges from decorators
	slog.Info("pass.timing", "pass", "decorates", "elapsed", time.Since(t))
	if err := p.checkCancel(); err != nil {
		return err
	}

	t = time.Now()
	p.passImports()
	slog.Info("pass.timing", "pass", "imports", "elapsed", time.Since(t))
	if err := p.checkCancel(); err != nil {
		return err
	}

	t = time.Now()
	p.buildReturnTypeMap()
	p.passCalls()
	slog.Info("pass.timing", "pass", "calls", "elapsed", time.Since(t))
	if err := p.checkCancel(); err != nil {
		return err
	}

	t = time.Now()
	p.passUsages()
	slog.Info("pass.timing", "pass", "usages", "elapsed", time.Since(t))
	if err := p.checkCancel(); err != nil {
		return err
	}

	p.runSemanticEdgePasses()
	if err := p.checkCancel(); err != nil {
		return err
	}

	// passImplements needs extractionCache for Rust impl traits,
	// so it must run before cleanupASTCache.
	t = time.Now()
	p.passImplements()
	slog.Info("pass.timing", "pass", "implements", "elapsed", time.Since(t))

	p.cleanupASTCache()

	// Flush in-memory buffer to SQLite with deferred index creation.
	if err := p.buf.FlushTo(p.ctx, p.Store); err != nil {
		return fmt.Errorf("graph_buffer flush: %w", err)
	}
	p.buf = nil

	// Post-flush passes use Store directly (need indexes).
	t = time.Now()
	p.passTests() // TESTS/TESTS_FILE edges (DB-only)
	slog.Info("pass.timing", "pass", "tests", "elapsed", time.Since(t))

	t = time.Now()
	p.passCommunities() // Community nodes + MEMBER_OF edges (DB-only)
	slog.Info("pass.timing", "pass", "communities", "elapsed", time.Since(t))
	if err := p.checkCancel(); err != nil {
		return err
	}

	t = time.Now()
	if err := p.passHTTPLinks(); err != nil {
		slog.Warn("pass.httplink.err", "err", err)
	}
	slog.Info("pass.timing", "pass", "httplinks", "elapsed", time.Since(t))

	t = time.Now()
	p.passConfigLinker()
	slog.Info("pass.timing", "pass", "configlinker", "elapsed", time.Since(t))

	t = time.Now()
	p.passGitHistory()
	slog.Info("pass.timing", "pass", "githistory", "elapsed", time.Since(t))

	t = time.Now()
	p.updateFileHashes(files)
	slog.Info("pass.timing", "pass", "filehashes", "elapsed", time.Since(t))

	// Observability: per-edge-type counts
	p.logEdgeCounts()

	return nil
}

// runSemanticEdgePasses runs the semantic edge passes (USES_TYPE, THROWS, READS/WRITES, CONFIGURES).
func (p *Pipeline) runSemanticEdgePasses() {
	t := time.Now()
	p.passUsesType()
	slog.Info("pass.timing", "pass", "usestype", "elapsed", time.Since(t))

	t = time.Now()
	p.passThrows()
	slog.Info("pass.timing", "pass", "throws", "elapsed", time.Since(t))

	t = time.Now()
	p.passReadsWrites()
	slog.Info("pass.timing", "pass", "readwrite", "elapsed", time.Since(t))

	t = time.Now()
	p.passConfigures()
	slog.Info("pass.timing", "pass", "configures", "elapsed", time.Since(t))
}

// logEdgeCounts logs the count of each edge type for observability.
func (p *Pipeline) logEdgeCounts() {
	edgeTypes := []string{
		"CALLS", "USAGE", "IMPORTS", "DEFINES", "DEFINES_METHOD",
		"TESTS", "TESTS_FILE", "INHERITS", "DECORATES", "USES_TYPE",
		"THROWS", "RAISES", "READS", "WRITES", "CONFIGURES", "MEMBER_OF",
		"HTTP_CALLS", "HANDLES", "ASYNC_CALLS", "IMPLEMENTS", "OVERRIDE",
		"FILE_CHANGES_WITH", "CONTAINS_FILE", "CONTAINS_FOLDER", "CONTAINS_PACKAGE",
	}
	for _, edgeType := range edgeTypes {
		count, err := p.Store.CountEdgesByType(p.ProjectName, edgeType)
		if err == nil && count > 0 {
			slog.Info("pipeline.edges", "type", edgeType, "count", count)
		}
	}
}

// runIncrementalPasses re-indexes only changed files + their dependents.
func (p *Pipeline) runIncrementalPasses(
	allFiles []discover.FileInfo,
	changed, unchanged []discover.FileInfo,
) error {
	// Pass 1: Structure always runs on all files (fast, idempotent upserts)
	if err := p.passStructure(allFiles); err != nil {
		return fmt.Errorf("pass1 structure: %w", err)
	}
	if err := p.checkCancel(); err != nil {
		return err
	}

	// Remove stale nodes/edges for deleted files
	p.removeDeletedFiles(allFiles)

	// Delete nodes for changed files (will be re-created in pass 2)
	for _, f := range changed {
		_ = p.Store.DeleteNodesByFile(p.ProjectName, f.RelPath)
	}

	// Pass 2: Parse changed files only
	p.passDefinitions(changed)
	if err := p.checkCancel(); err != nil {
		return err
	}

	// Re-compute decorator tags globally (threshold is across all nodes)
	p.passDecoratorTags()

	// Build full registry: includes nodes from unchanged files (already in DB)
	// plus newly parsed nodes from changed files
	p.buildRegistry()
	if err := p.checkCancel(); err != nil {
		return err
	}

	// Re-build import maps for changed files (already done in passDefinitions)
	// Also load import maps for unchanged files from their AST (not cached)
	// For correctness, we need the full import map, but unchanged files don't
	// have ASTs cached. Rebuild imports only for changed files is sufficient
	// since unchanged file import edges still exist in DB.
	p.passImports()
	if err := p.checkCancel(); err != nil {
		return err
	}

	// Determine which files need call re-resolution:
	// changed files + files that import any changed module
	dependents := p.findDependentFiles(changed, unchanged)
	filesToResolve := mergeFiles(changed, dependents)
	slog.Info("incremental.resolve", "changed", len(changed), "dependents", len(dependents))

	// Delete edges for files being re-resolved (all AST-derived edge types)
	for _, f := range filesToResolve {
		_ = p.Store.DeleteEdgesBySourceFile(p.ProjectName, f.RelPath, "CALLS")
		_ = p.Store.DeleteEdgesBySourceFile(p.ProjectName, f.RelPath, "USAGE")
		_ = p.Store.DeleteEdgesBySourceFile(p.ProjectName, f.RelPath, "USES_TYPE")
		_ = p.Store.DeleteEdgesBySourceFile(p.ProjectName, f.RelPath, "THROWS")
		_ = p.Store.DeleteEdgesBySourceFile(p.ProjectName, f.RelPath, "RAISES")
		_ = p.Store.DeleteEdgesBySourceFile(p.ProjectName, f.RelPath, "READS")
		_ = p.Store.DeleteEdgesBySourceFile(p.ProjectName, f.RelPath, "WRITES")
		_ = p.Store.DeleteEdgesBySourceFile(p.ProjectName, f.RelPath, "CONFIGURES")
	}

	// Re-resolve calls + usages for changed + dependent files
	p.buildReturnTypeMap()
	p.passCallsForFiles(filesToResolve)
	p.passUsagesForFiles(filesToResolve)
	if err := p.checkCancel(); err != nil {
		return err
	}

	// AST-dependent passes (run on cached files before cleanup)
	p.passUsesType()
	p.passThrows()
	p.passReadsWrites()
	p.passConfigures()
	if err := p.checkCancel(); err != nil {
		return err
	}

	p.cleanupASTCache()

	// DB-derived edge types: delete all and re-run (cheap)
	_ = p.Store.DeleteEdgesByType(p.ProjectName, "TESTS")
	_ = p.Store.DeleteEdgesByType(p.ProjectName, "TESTS_FILE")
	p.passTests()

	_ = p.Store.DeleteEdgesByType(p.ProjectName, "INHERITS")
	p.passInherits()

	_ = p.Store.DeleteEdgesByType(p.ProjectName, "DECORATES")
	p.passDecorates()

	// Community detection: delete old communities and MEMBER_OF, re-run
	_ = p.Store.DeleteEdgesByType(p.ProjectName, "MEMBER_OF")
	_ = p.Store.DeleteNodesByLabel(p.ProjectName, "Community")
	p.passCommunities()
	if err := p.checkCancel(); err != nil {
		return err
	}

	// HTTP linking, config linking, and implements always run fully (they clean up first)
	if err := p.passHTTPLinks(); err != nil {
		slog.Warn("pass.httplink.err", "err", err)
	}
	p.passConfigLinker()
	p.passImplements()
	p.passGitHistory()

	p.updateFileHashes(allFiles)

	// Observability
	p.logEdgeCounts()

	return nil
}

// classifyFiles splits files into changed and unchanged based on stored hashes.
// File hashing is parallelized across CPU cores.
func (p *Pipeline) classifyFiles(files []discover.FileInfo) (changed, unchanged []discover.FileInfo) {
	storedHashes, err := p.Store.GetFileHashes(p.ProjectName)
	if err != nil || len(storedHashes) == 0 {
		return files, nil // no hashes → full index
	}

	type hashResult struct {
		Hash string
		Err  error
	}

	results := make([]hashResult, len(files))
	numWorkers := runtime.NumCPU()
	if numWorkers > len(files) {
		numWorkers = len(files)
	}

	g := new(errgroup.Group)
	g.SetLimit(numWorkers)
	for i, f := range files {
		g.Go(func() error {
			hash, hashErr := fileHash(f.Path)
			results[i] = hashResult{Hash: hash, Err: hashErr}
			return nil
		})
	}
	_ = g.Wait()

	for i, f := range files {
		r := results[i]
		if r.Err != nil {
			changed = append(changed, f)
			continue
		}
		if stored, ok := storedHashes[f.RelPath]; ok && stored == r.Hash {
			unchanged = append(unchanged, f)
		} else {
			changed = append(changed, f)
		}
	}
	return changed, unchanged
}

// findDependentFiles finds unchanged files that import any changed file's module.
func (p *Pipeline) findDependentFiles(changed, unchanged []discover.FileInfo) []discover.FileInfo {
	// Build set of module QNs for changed files
	changedModules := make(map[string]bool, len(changed))
	for _, f := range changed {
		mqn := fqn.ModuleQN(p.ProjectName, f.RelPath)
		changedModules[mqn] = true
		// Also add folder QN (for Go package-level imports)
		dir := filepath.Dir(f.RelPath)
		if dir != "." {
			changedModules[fqn.FolderQN(p.ProjectName, dir)] = true
		}
	}

	var dependents []discover.FileInfo
	for _, f := range unchanged {
		mqn := fqn.ModuleQN(p.ProjectName, f.RelPath)
		importMap := p.importMaps[mqn]
		// If no cached import map, check the store for IMPORTS edges
		if len(importMap) == 0 {
			importMap = p.loadImportMapFromDB(mqn)
		}
		for _, targetQN := range importMap {
			if changedModules[targetQN] {
				dependents = append(dependents, f)
				break
			}
		}
	}
	return dependents
}

// loadImportMapFromDB reconstructs an import map from stored IMPORTS edges.
func (p *Pipeline) loadImportMapFromDB(moduleQN string) map[string]string {
	moduleNode, err := p.Store.FindNodeByQN(p.ProjectName, moduleQN)
	if err != nil || moduleNode == nil {
		return nil
	}
	edges, err := p.Store.FindEdgesBySourceAndType(moduleNode.ID, "IMPORTS")
	if err != nil {
		return nil
	}
	result := make(map[string]string, len(edges))
	for _, e := range edges {
		target, tErr := p.Store.FindNodeByID(e.TargetID)
		if tErr != nil || target == nil {
			continue
		}
		alias := ""
		if a, ok := e.Properties["alias"].(string); ok {
			alias = a
		}
		if alias != "" {
			result[alias] = target.QualifiedName
		}
	}
	return result
}

// passCallsForFiles resolves calls only for the specified files (incremental).
func (p *Pipeline) passCallsForFiles(files []discover.FileInfo) {
	slog.Info("pass3.calls.incremental", "files", len(files))
	for _, f := range files {
		if p.ctx.Err() != nil {
			return
		}
		ext, ok := p.extractionCache[f.RelPath]
		if !ok {
			// File not in extraction cache — need to extract it
			source, err := os.ReadFile(f.Path)
			if err != nil {
				continue
			}
			source = stripBOM(source)
			cbmResult, err := cbm.ExtractFile(source, f.Language, p.ProjectName, f.RelPath)
			if err != nil {
				continue
			}
			ext = &cachedExtraction{Result: cbmResult, Language: f.Language}
			p.extractionCache[f.RelPath] = ext
		}
		edges := p.resolveFileCallsCBM(f.RelPath, ext)
		for _, re := range edges {
			callerNode, _ := p.Store.FindNodeByQN(p.ProjectName, re.CallerQN)
			targetNode, _ := p.Store.FindNodeByQN(p.ProjectName, re.TargetQN)
			if callerNode != nil && targetNode != nil {
				_, _ = p.Store.InsertEdge(&store.Edge{
					Project:    p.ProjectName,
					SourceID:   callerNode.ID,
					TargetID:   targetNode.ID,
					Type:       re.Type,
					Properties: re.Properties,
				})
			}
		}
	}
}

// removeDeletedFiles removes nodes/edges for files that no longer exist on disk.
func (p *Pipeline) removeDeletedFiles(currentFiles []discover.FileInfo) {
	currentSet := make(map[string]bool, len(currentFiles))
	for _, f := range currentFiles {
		currentSet[f.RelPath] = true
	}
	indexed, err := p.Store.ListFilesForProject(p.ProjectName)
	if err != nil {
		return
	}
	for _, filePath := range indexed {
		if !currentSet[filePath] {
			_ = p.Store.DeleteNodesByFile(p.ProjectName, filePath)
			_ = p.Store.DeleteFileHash(p.ProjectName, filePath)
			slog.Info("incremental.removed", "file", filePath)
		}
	}
}

func (p *Pipeline) cleanupASTCache() {
	// Release extraction cache (Go GC handles the cbm.FileResult structs)
	p.extractionCache = nil
}

func (p *Pipeline) updateFileHashes(files []discover.FileInfo) {
	type hashResult struct {
		Hash string
		Err  error
	}

	results := make([]hashResult, len(files))
	numWorkers := runtime.NumCPU()
	if numWorkers > len(files) {
		numWorkers = len(files)
	}

	g := new(errgroup.Group)
	g.SetLimit(numWorkers)
	for i, f := range files {
		g.Go(func() error {
			hash, hashErr := fileHash(f.Path)
			results[i] = hashResult{Hash: hash, Err: hashErr}
			return nil
		})
	}
	_ = g.Wait()

	// Collect successful hashes for batch upsert
	batch := make([]store.FileHash, 0, len(files))
	for i, f := range files {
		if results[i].Err == nil {
			batch = append(batch, store.FileHash{
				Project: p.ProjectName,
				RelPath: f.RelPath,
				SHA256:  results[i].Hash,
			})
		}
	}
	_ = p.Store.UpsertFileHashBatch(batch)
}

// mergeFiles returns the union of two file slices (deduped by RelPath).
func mergeFiles(a, b []discover.FileInfo) []discover.FileInfo {
	seen := make(map[string]bool, len(a))
	result := make([]discover.FileInfo, 0, len(a)+len(b))
	for _, f := range a {
		seen[f.RelPath] = true
		result = append(result, f)
	}
	for _, f := range b {
		if !seen[f.RelPath] {
			result = append(result, f)
		}
	}
	return result
}

// passStructure creates Project, Folder, Package, File nodes and containment edges.
// Collects all nodes/edges in memory first, then batch-writes to DB.
func (p *Pipeline) passStructure(files []discover.FileInfo) error {
	slog.Info("pass1.structure")

	dirSet, dirIsPackage := p.classifyDirectories(files)

	nodes := make([]*store.Node, 0, len(files)*2)
	edges := make([]pendingEdge, 0, len(files)*2)

	projectQN := p.ProjectName
	nodes = append(nodes, &store.Node{
		Project:       p.ProjectName,
		Label:         "Project",
		Name:          p.ProjectName,
		QualifiedName: projectQN,
	})

	dirNodes, dirEdges := p.buildDirNodesEdges(dirSet, dirIsPackage, projectQN)
	nodes = append(nodes, dirNodes...)
	edges = append(edges, dirEdges...)

	fileNodes, fileEdges := p.buildFileNodesEdges(files)
	nodes = append(nodes, fileNodes...)
	edges = append(edges, fileEdges...)

	return p.batchWriteStructure(nodes, edges)
}

// classifyDirectories collects all directories and determines which are packages.
func (p *Pipeline) classifyDirectories(files []discover.FileInfo) (allDirs, packageDirs map[string]bool) {
	packageIndicators := make(map[string]bool)
	for _, l := range lang.AllLanguages() {
		spec := lang.ForLanguage(l)
		if spec != nil {
			for _, pi := range spec.PackageIndicators {
				packageIndicators[pi] = true
			}
		}
	}

	allDirs = make(map[string]bool)
	for _, f := range files {
		dir := filepath.Dir(f.RelPath)
		for dir != "." && dir != "" && !allDirs[dir] {
			allDirs[dir] = true
			dir = filepath.Dir(dir)
		}
	}

	packageDirs = make(map[string]bool, len(allDirs))
	for dir := range allDirs {
		absDir := filepath.Join(p.RepoPath, dir)
		for indicator := range packageIndicators {
			if _, err := os.Stat(filepath.Join(absDir, indicator)); err == nil {
				packageDirs[dir] = true
				break
			}
		}
	}
	return
}

func (p *Pipeline) buildDirNodesEdges(dirSet, dirIsPackage map[string]bool, projectQN string) ([]*store.Node, []pendingEdge) {
	nodes := make([]*store.Node, 0, len(dirSet))
	edges := make([]pendingEdge, 0, len(dirSet))

	for dir := range dirSet {
		label := "Folder"
		edgeType := "CONTAINS_FOLDER"
		if dirIsPackage[dir] {
			label = "Package"
			edgeType = "CONTAINS_PACKAGE"
		}
		qn := fqn.FolderQN(p.ProjectName, dir)
		nodes = append(nodes, &store.Node{
			Project:       p.ProjectName,
			Label:         label,
			Name:          filepath.Base(dir),
			QualifiedName: qn,
			FilePath:      dir,
		})

		parent := filepath.Dir(dir)
		parentQN := projectQN
		if parent != "." && parent != "" {
			parentQN = fqn.FolderQN(p.ProjectName, parent)
		}
		edges = append(edges, pendingEdge{SourceQN: parentQN, TargetQN: qn, Type: edgeType})
	}
	return nodes, edges
}

func (p *Pipeline) buildFileNodesEdges(files []discover.FileInfo) ([]*store.Node, []pendingEdge) {
	nodes := make([]*store.Node, 0, len(files))
	edges := make([]pendingEdge, 0, len(files))

	for _, f := range files {
		fileQN := fqn.Compute(p.ProjectName, f.RelPath, "") + ".__file__"
		fileProps := map[string]any{
			"extension": filepath.Ext(f.RelPath),
			"is_test":   isTestFile(f.RelPath, f.Language),
		}
		if f.Language != "" {
			fileProps["language"] = string(f.Language)
		}
		nodes = append(nodes, &store.Node{
			Project:       p.ProjectName,
			Label:         "File",
			Name:          filepath.Base(f.RelPath),
			QualifiedName: fileQN,
			FilePath:      f.RelPath,
			Properties:    fileProps,
		})

		parentQN := p.dirQN(filepath.Dir(f.RelPath))
		edges = append(edges, pendingEdge{SourceQN: parentQN, TargetQN: fileQN, Type: "CONTAINS_FILE"})
	}
	return nodes, edges
}

func (p *Pipeline) batchWriteStructure(nodes []*store.Node, edges []pendingEdge) error {
	idMap, err := p.upsertNodeBatch(nodes)
	if err != nil {
		return fmt.Errorf("pass1 batch upsert: %w", err)
	}

	realEdges := make([]*store.Edge, 0, len(edges))
	for _, pe := range edges {
		srcID, srcOK := idMap[pe.SourceQN]
		tgtID, tgtOK := idMap[pe.TargetQN]
		if srcOK && tgtOK {
			realEdges = append(realEdges, &store.Edge{
				Project:    p.ProjectName,
				SourceID:   srcID,
				TargetID:   tgtID,
				Type:       pe.Type,
				Properties: pe.Properties,
			})
		}
	}

	if err := p.insertEdgeBatch(realEdges); err != nil {
		return fmt.Errorf("pass1 batch edges: %w", err)
	}
	return nil
}

func (p *Pipeline) dirQN(relDir string) string {
	if relDir == "." || relDir == "" {
		return p.ProjectName
	}
	return fqn.FolderQN(p.ProjectName, relDir)
}

// pendingEdge represents an edge to be created after batch node insertion,
// using qualified names that will be resolved to IDs.
type pendingEdge struct {
	SourceQN   string
	TargetQN   string
	Type       string
	Properties map[string]any
}

// parseResult holds the output of a pure file parse (no DB access).
type parseResult struct {
	File         discover.FileInfo
	Nodes        []*store.Node
	PendingEdges []pendingEdge
	ImportMap    map[string]string
	CBMResult    *cbm.FileResult // CBM extraction result (nil when using legacy AST path)
	Err          error
}

// passDefinitions extracts definitions from each file via CBM (C extraction library).
// Uses parallel extraction (Stage 1) followed by sequential batch DB writes (Stage 2).
func (p *Pipeline) passDefinitions(files []discover.FileInfo) {
	slog.Info("pass2.definitions")

	// Enrich JSON files with URL constants (for HTTP linking), then include
	// them in normal CBM extraction so they also get Variable/Class nodes.
	parseableFiles := make([]discover.FileInfo, 0, len(files))
	for _, f := range files {
		if f.Language == lang.JSON {
			if p.ctx.Err() != nil {
				return
			}
			if err := p.processJSONFile(f); err != nil {
				slog.Warn("pass2.json.err", "path", f.RelPath, "err", err)
			}
		}
		parseableFiles = append(parseableFiles, f)
	}

	if len(parseableFiles) == 0 {
		return
	}

	// Stage 1: Parallel CBM extraction (I/O + CPU, no DB, no shared state)
	// Adaptive pool auto-tunes concurrency via AIMD throughput feedback.
	t1 := time.Now()
	results := make([]*parseResult, len(parseableFiles))

	// Start readahead prefetcher to warm page cache ahead of workers
	pf := newPrefetcher(parseableFiles, 100)
	go pf.run(p.ctx)
	defer pf.stop()

	pool := newAdaptivePool(runtime.NumCPU())
	go pool.monitor(p.ctx)

	var wg sync.WaitGroup
	for i, f := range parseableFiles {
		pool.acquire()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer pool.releaseBytes(f.Size)
			if p.ctx.Err() != nil {
				return
			}
			results[i] = cbmParseFile(p.ProjectName, f)
			pf.advance(i + 1)
		}()
	}
	wg.Wait()
	pool.stop()
	slog.Info("pass2.stage1.extract", "files", len(parseableFiles), "elapsed", time.Since(t1))

	// Log C-side parse vs extraction breakdown
	profile := cbm.GetProfile()
	if profile.Files > 0 {
		slog.Info("pass2.stage1.profile",
			"files", profile.Files,
			"parse_total", time.Duration(profile.ParseNs),
			"extract_total", time.Duration(profile.ExtractNs),
			"parse_avg_us", profile.ParseNs/profile.Files/1000,
			"extract_avg_us", profile.ExtractNs/profile.Files/1000,
		)
	}

	// Stage 2: Sequential cache population + batch DB writes
	t2 := time.Now()
	var allNodes []*store.Node
	var allPendingEdges []pendingEdge

	for _, r := range results {
		if r == nil {
			continue
		}
		if r.Err != nil {
			slog.Warn("pass2.file.err", "path", r.File.RelPath, "err", r.Err)
			continue
		}
		// Populate extraction cache for use by later passes
		if r.CBMResult != nil {
			p.extractionCache[r.File.RelPath] = &cachedExtraction{
				Result:   r.CBMResult,
				Language: r.File.Language,
			}
		}
		// Store import map
		moduleQN := fqn.ModuleQN(p.ProjectName, r.File.RelPath)
		if len(r.ImportMap) > 0 {
			p.importMaps[moduleQN] = r.ImportMap
		}
		allNodes = append(allNodes, r.Nodes...)
		allPendingEdges = append(allPendingEdges, r.PendingEdges...)
	}

	slog.Info("pass2.stage2.collect", "nodes", len(allNodes), "edges", len(allPendingEdges), "elapsed", time.Since(t2))

	// Batch insert all nodes
	t3 := time.Now()
	idMap, err := p.upsertNodeBatch(allNodes)
	if err != nil {
		slog.Warn("pass2.batch_upsert.err", "err", err)
		return
	}
	slog.Info("pass2.stage3.upsert_nodes", "nodes", len(allNodes), "elapsed", time.Since(t3))

	// Resolve pending edges to real edges using the ID map
	t4 := time.Now()
	edges := make([]*store.Edge, 0, len(allPendingEdges))
	for _, pe := range allPendingEdges {
		srcID, srcOK := idMap[pe.SourceQN]
		tgtID, tgtOK := idMap[pe.TargetQN]
		if srcOK && tgtOK {
			edges = append(edges, &store.Edge{
				Project:    p.ProjectName,
				SourceID:   srcID,
				TargetID:   tgtID,
				Type:       pe.Type,
				Properties: pe.Properties,
			})
		}
	}

	if err := p.insertEdgeBatch(edges); err != nil {
		slog.Warn("pass2.batch_edges.err", "err", err)
	}
	slog.Info("pass2.stage4.insert_edges", "edges", len(edges), "elapsed", time.Since(t4))
}

// buildRegistry populates the FunctionRegistry from all Function, Method,
// and Class nodes in the store.
func (p *Pipeline) buildRegistry() {
	labels := []string{"Function", "Method", "Class", "Type", "Interface", "Enum", "Macro", "Variable"}
	for _, label := range labels {
		nodes, err := p.findNodesByLabel(p.ProjectName, label)
		if err != nil {
			continue
		}
		for _, n := range nodes {
			p.registry.Register(n.Name, n.QualifiedName, n.Label)
		}
	}
	slog.Info("registry.built", "entries", p.registry.Size())
}

// buildReturnTypeMap builds a map from function QN to its return type QN.
// Uses the "return_types" property stored on Function/Method nodes during pass2.
func (p *Pipeline) buildReturnTypeMap() {
	p.returnTypes = make(ReturnTypeMap)
	for _, label := range []string{"Function", "Method"} {
		nodes, err := p.findNodesByLabel(p.ProjectName, label)
		if err != nil {
			continue
		}
		for _, n := range nodes {
			retTypes, ok := n.Properties["return_types"]
			if !ok {
				continue
			}
			// return_types is stored as []any (JSON round-trip) containing type name strings
			typeList, ok := retTypes.([]any)
			if !ok || len(typeList) == 0 {
				continue
			}
			// Use the first return type — most functions return a single type
			firstType, ok := typeList[0].(string)
			if !ok || firstType == "" {
				continue
			}
			// Resolve the type name to a class QN
			classQN := resolveAsClass(firstType, p.registry, "", nil)
			if classQN != "" {
				p.returnTypes[n.QualifiedName] = classQN
			}
		}
	}
	if len(p.returnTypes) > 0 {
		slog.Info("return_types.built", "entries", len(p.returnTypes))
	}
}

// resolvedEdge represents an edge resolved during parallel call/usage resolution,
// stored as QN pairs to be converted to ID-based edges in the batch write stage.
type resolvedEdge struct {
	CallerQN   string
	TargetQN   string
	Type       string // "CALLS" or "USAGE"
	Properties map[string]any
}

// passCalls resolves call targets and creates CALLS edges.
// Uses parallel per-file resolution (Stage 1) followed by batch DB writes (Stage 2).
func (p *Pipeline) passCalls() {
	slog.Info("pass3.calls")

	// Collect files to process from extraction cache
	type fileEntry struct {
		relPath string
		ext     *cachedExtraction
	}
	var files []fileEntry
	for relPath, ext := range p.extractionCache {
		if lang.ForLanguage(ext.Language) != nil {
			files = append(files, fileEntry{relPath, ext})
		}
	}

	if len(files) == 0 {
		return
	}

	// Stage 1: Parallel per-file call resolution using CBM data
	results := make([][]resolvedEdge, len(files))
	numWorkers := runtime.NumCPU()
	if numWorkers > len(files) {
		numWorkers = len(files)
	}

	g, gctx := errgroup.WithContext(p.ctx)
	g.SetLimit(numWorkers)
	for i, fe := range files {
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			results[i] = p.resolveFileCallsCBM(fe.relPath, fe.ext)
			return nil
		})
	}
	_ = g.Wait()

	// Stage 2: Batch QN→ID resolution + batch edge insert
	p.flushResolvedEdges(results)
}

// flushResolvedEdges converts QN-based resolved edges to ID-based edges and batch-inserts them.
func (p *Pipeline) flushResolvedEdges(results [][]resolvedEdge) {
	// Collect all unique QNs
	qnSet := make(map[string]struct{})
	totalEdges := 0
	for _, fileEdges := range results {
		for _, re := range fileEdges {
			qnSet[re.CallerQN] = struct{}{}
			qnSet[re.TargetQN] = struct{}{}
			totalEdges++
		}
	}

	if totalEdges == 0 {
		return
	}

	// Batch resolve all QNs to IDs
	qns := make([]string, 0, len(qnSet))
	for qn := range qnSet {
		qns = append(qns, qn)
	}
	qnToID, err := p.findNodeIDsByQNs(p.ProjectName, qns)
	if err != nil {
		slog.Warn("pass3.resolve_ids.err", "err", err)
		return
	}

	// Build edges
	edges := make([]*store.Edge, 0, totalEdges)
	for _, fileEdges := range results {
		for _, re := range fileEdges {
			srcID, srcOK := qnToID[re.CallerQN]
			tgtID, tgtOK := qnToID[re.TargetQN]
			if srcOK && tgtOK {
				edges = append(edges, &store.Edge{
					Project:    p.ProjectName,
					SourceID:   srcID,
					TargetID:   tgtID,
					Type:       re.Type,
					Properties: re.Properties,
				})
			}
		}
	}

	if err := p.insertEdgeBatch(edges); err != nil {
		slog.Warn("pass3.batch_edges.err", "err", err)
	}
}

// resolveCallWithTypes resolves a callee name using the registry, import maps,
// and type inference for method dispatch.
func (p *Pipeline) resolveCallWithTypes(
	calleeName, moduleQN string,
	importMap map[string]string,
	typeMap TypeMap,
) ResolutionResult {
	// First, try type-based method dispatch for qualified calls like obj.method()
	if strings.Contains(calleeName, ".") {
		parts := strings.SplitN(calleeName, ".", 2)
		objName := parts[0]
		methodName := parts[1]

		// Check if the object has a known type from type inference
		if classQN, ok := typeMap[objName]; ok {
			candidate := classQN + "." + methodName
			if p.registry.Exists(candidate) {
				return ResolutionResult{QualifiedName: candidate, Strategy: "type_dispatch", Confidence: 0.90, CandidateCount: 1}
			}
		}
	}

	// Delegate to the registry's resolution strategy
	return p.registry.Resolve(calleeName, moduleQN, importMap)
}

// frameworkDecoratorPrefixes are decorator prefixes that indicate a function
// is registered as an entry point by a framework (not dead code).
var frameworkDecoratorPrefixes = []string{
	// Python web frameworks (route handlers)
	"@app.get", "@app.post", "@app.put", "@app.delete", "@app.patch",
	"@app.route", "@app.websocket",
	"@router.get", "@router.post", "@router.put", "@router.delete", "@router.patch",
	"@router.route", "@router.websocket",
	"@blueprint.", "@api.", "@ns.",
	// Python middleware and exception handlers (framework-registered)
	"@app.middleware", "@app.exception_handler", "@app.on_event",
	// Testing frameworks
	"@pytest.fixture", "@pytest.mark",
	// CLI frameworks
	"@click.command", "@click.group",
	// Task/worker frameworks
	"@celery.task", "@shared_task", "@task",
	// Signal handlers
	"@receiver",
	// Rust Actix/Axum/Rocket route macros (#[get("/path")] → extracted as get("/path"))
	"get(", "post(", "put(", "delete(", "patch(", "head(", "options(",
	"route(", "connect(", "trace(",
}

// hasFrameworkDecorator returns true if any decorator matches a framework pattern.
func hasFrameworkDecorator(decorators []string) bool {
	for _, dec := range decorators {
		for _, prefix := range frameworkDecoratorPrefixes {
			if strings.HasPrefix(dec, prefix) {
				return true
			}
		}
	}
	return false
}

// passImports creates IMPORTS edges from the import maps built during pass 2.
func (p *Pipeline) passImports() {
	slog.Info("pass2b.imports")
	count := 0
	for moduleQN, importMap := range p.importMaps {
		moduleNode, _ := p.findNodeByQN(p.ProjectName, moduleQN)
		if moduleNode == nil {
			continue
		}
		for localName, targetQN := range importMap {
			// Try to find the target as a Module node first
			targetNode, _ := p.findNodeByQN(p.ProjectName, targetQN)
			if targetNode == nil {
				// Try common suffixes: module QN might need .__init__ or similar
				logImportDrop(moduleQN, localName, targetQN)
				continue
			}
			_ = p.insertEdge(&store.Edge{
				Project:  p.ProjectName,
				SourceID: moduleNode.ID,
				TargetID: targetNode.ID,
				Type:     "IMPORTS",
				Properties: map[string]any{
					"alias": localName,
				},
			})
			count++
		}
	}
	slog.Info("pass2b.imports.done", "edges", count)
}

// passHTTPLinks runs the HTTP linker to discover cross-service HTTP calls.
func (p *Pipeline) passHTTPLinks() error {
	// Clean up stale Route/InfraFile nodes and HTTP_CALLS/HANDLES/ASYNC_CALLS edges before re-running
	_ = p.Store.DeleteNodesByLabel(p.ProjectName, "Route")
	_ = p.Store.DeleteNodesByLabel(p.ProjectName, "InfraFile")
	_ = p.Store.DeleteEdgesByType(p.ProjectName, "HTTP_CALLS")
	_ = p.Store.DeleteEdgesByType(p.ProjectName, "HANDLES")
	_ = p.Store.DeleteEdgesByType(p.ProjectName, "ASYNC_CALLS")

	// Index infrastructure files (Dockerfiles, compose, cloudbuild, .env)
	p.passInfraFiles()

	// Scan config files for env var URLs and create synthetic Module nodes
	envBindings := ScanProjectEnvURLs(p.RepoPath)
	if len(envBindings) > 0 {
		p.injectEnvBindings(envBindings)
	}

	linker := httplink.New(p.Store, p.ProjectName)

	// Feed InfraFile environment URLs into the HTTP linker
	infraSites := p.extractInfraCallSites()
	if len(infraSites) > 0 {
		linker.AddCallSites(infraSites)
		slog.Info("pass4.infra_callsites", "count", len(infraSites))
	}

	links, err := linker.Run()
	if err != nil {
		return err
	}
	slog.Info("pass4.httplinks", "links", len(links))
	return nil
}

// extractInfraCallSites extracts URL values from InfraFile environment properties
// and converts them to HTTPCallSite entries for the HTTP linker.
func (p *Pipeline) extractInfraCallSites() []httplink.HTTPCallSite {
	infraNodes, err := p.Store.FindNodesByLabel(p.ProjectName, "InfraFile")
	if err != nil {
		return nil
	}

	var sites []httplink.HTTPCallSite
	for _, node := range infraNodes {
		// InfraFile nodes use different property keys depending on source:
		// compose files: "environment", Dockerfiles/shell/.env: "env_vars",
		// cloudbuild: "deploy_env_vars"
		for _, envKey := range []string{"environment", "env_vars", "deploy_env_vars"} {
			sites = append(sites, extractEnvURLSites(node, envKey)...)
		}
	}
	return sites
}

// extractEnvURLSites extracts HTTP call sites from a single env property of an InfraFile node.
func extractEnvURLSites(node *store.Node, propKey string) []httplink.HTTPCallSite {
	env, ok := node.Properties[propKey]
	if !ok {
		return nil
	}

	// env_vars are stored as map[string]string (from Go), but after JSON round-trip
	// through SQLite they come back as map[string]any.
	var sites []httplink.HTTPCallSite
	switch envMap := env.(type) {
	case map[string]any:
		for _, val := range envMap {
			valStr, ok := val.(string)
			if !ok {
				continue
			}
			sites = append(sites, urlSitesFromValue(node, valStr)...)
		}
	case map[string]string:
		for _, valStr := range envMap {
			sites = append(sites, urlSitesFromValue(node, valStr)...)
		}
	}
	return sites
}

// urlSitesFromValue extracts URL paths from a string value and creates HTTPCallSite entries.
func urlSitesFromValue(node *store.Node, val string) []httplink.HTTPCallSite {
	if !strings.Contains(val, "http://") && !strings.Contains(val, "https://") && !strings.HasPrefix(val, "/") {
		return nil
	}

	paths := httplink.ExtractURLPaths(val)
	sites := make([]httplink.HTTPCallSite, 0, len(paths))
	for _, path := range paths {
		sites = append(sites, httplink.HTTPCallSite{
			Path:                path,
			SourceName:          node.Name,
			SourceQualifiedName: node.QualifiedName,
			SourceLabel:         "InfraFile",
		})
	}
	return sites
}

// injectEnvBindings creates or updates Module nodes for config files that contain
// environment variable URL bindings. These synthetic constants feed into the
// HTTP linker's call site discovery.
func (p *Pipeline) injectEnvBindings(bindings []EnvBinding) {
	byFile := make(map[string][]EnvBinding)
	for _, b := range bindings {
		byFile[b.FilePath] = append(byFile[b.FilePath], b)
	}

	count := 0
	for filePath, fileBindings := range byFile {
		moduleQN := fqn.ModuleQN(p.ProjectName, filePath)
		constants := buildConstantsList(fileBindings)

		if p.mergeWithExistingModule(moduleQN, constants) {
			count += len(fileBindings)
			continue
		}

		_, _ = p.Store.UpsertNode(&store.Node{
			Project:       p.ProjectName,
			Label:         "Module",
			Name:          filepath.Base(filePath),
			QualifiedName: moduleQN,
			FilePath:      filePath,
			Properties:    map[string]any{"constants": constants},
		})
		count += len(fileBindings)
	}

	if count > 0 {
		slog.Info("envscan.injected", "bindings", count, "files", len(byFile))
	}
}

// buildConstantsList converts env bindings to "KEY = VALUE" constant strings, capped at 50.
func buildConstantsList(bindings []EnvBinding) []string {
	constants := make([]string, 0, len(bindings))
	for _, b := range bindings {
		constants = append(constants, b.Key+" = "+b.Value)
	}
	if len(constants) > 50 {
		constants = constants[:50]
	}
	return constants
}

// mergeWithExistingModule merges new constants into an existing Module node's constant list.
// Returns true if the module existed and was updated.
func (p *Pipeline) mergeWithExistingModule(moduleQN string, constants []string) bool {
	existing, _ := p.Store.FindNodeByQN(p.ProjectName, moduleQN)
	if existing == nil {
		return false
	}
	existConsts, ok := existing.Properties["constants"].([]any)
	if !ok {
		return false
	}
	seen := make(map[string]bool, len(existConsts))
	for _, c := range existConsts {
		if s, ok := c.(string); ok {
			seen[s] = true
		}
	}
	for _, c := range constants {
		if !seen[c] {
			existConsts = append(existConsts, c)
		}
	}
	if existing.Properties == nil {
		existing.Properties = map[string]any{}
	}
	existing.Properties["constants"] = existConsts
	_, _ = p.Store.UpsertNode(existing)
	return true
}

// jsonURLKeyPattern matches JSON keys that likely contain URL/endpoint values.
var jsonURLKeyPattern = regexp.MustCompile(`(?i)(url|endpoint|base_url|host|api_url|service_url|target_url|callback_url|webhook|href|uri|address|server|origin|proxy|redirect|forward|destination)`)

// processJSONFile extracts URL-related string values from JSON config files.
// Uses a key-pattern allowlist to avoid flooding constants with noise.
func (p *Pipeline) processJSONFile(f discover.FileInfo) error {
	data, err := os.ReadFile(f.Path)
	if err != nil {
		return err
	}

	var parsed any
	if err := json.Unmarshal(data, &parsed); err != nil {
		return fmt.Errorf("json parse: %w", err)
	}

	var constants []string
	extractJSONURLValues(parsed, "", &constants, 0)

	if len(constants) == 0 {
		return nil
	}

	// Cap at 20 constants per JSON file
	if len(constants) > 20 {
		constants = constants[:20]
	}

	moduleQN := fqn.ModuleQN(p.ProjectName, f.RelPath)
	err = p.upsertNode(&store.Node{
		Project:       p.ProjectName,
		Label:         "Module",
		Name:          filepath.Base(f.RelPath),
		QualifiedName: moduleQN,
		FilePath:      f.RelPath,
		Properties:    map[string]any{"constants": constants},
	})
	return err
}

// extractJSONURLValues recursively extracts key=value pairs from JSON where
// the key matches the URL key pattern or the value looks like a URL/path.
func extractJSONURLValues(v any, key string, out *[]string, depth int) {
	if depth > 20 {
		return
	}

	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			extractJSONURLValues(child, k, out, depth+1)
		}
	case []any:
		for _, child := range val {
			extractJSONURLValues(child, key, out, depth+1)
		}
	case string:
		if key == "" || val == "" {
			return
		}
		// Include if key matches URL pattern
		if jsonURLKeyPattern.MatchString(key) {
			*out = append(*out, key+" = "+val)
			return
		}
		// Include if value looks like a URL or API path
		if looksLikeURL(val) {
			*out = append(*out, key+" = "+val)
		}
	}
}

// looksLikeURL returns true if s appears to be a URL or API path.
func looksLikeURL(s string) bool {
	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		return true
	}
	// Path starting with /api/ or containing at least 2 segments
	if strings.HasPrefix(s, "/") && strings.Count(s, "/") >= 2 {
		// Skip version-like paths: /1.0.0, /v2, /en
		seg := strings.TrimPrefix(s, "/")
		return len(seg) > 3
	}
	return false
}

// safeRowToLine converts a tree-sitter row (uint) to a 1-based line number (int).
// Returns math.MaxInt if the value would overflow.
// stripBOM removes a UTF-8 BOM (0xEF 0xBB 0xBF) from the start of source.
// Common in C# and Windows-generated files; tree-sitter may choke on BOM bytes.
func stripBOM(source []byte) []byte {
	if len(source) >= 3 && source[0] == 0xEF && source[1] == 0xBB && source[2] == 0xBF {
		return source[3:]
	}
	return source
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := xxh3.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
