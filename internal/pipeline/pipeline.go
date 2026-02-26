package pipeline

import (
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
	"time"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
	"github.com/zeebo/xxh3"
	"golang.org/x/sync/errgroup"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/httplink"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// Pipeline orchestrates the 3-pass indexing of a repository.
type Pipeline struct {
	Store       *store.Store
	RepoPath    string
	ProjectName string
	// astCache maps file rel_path -> (tree, source, language) for pass 3
	astCache map[string]*cachedAST
	// registry indexes all Function/Method/Class nodes for call resolution
	registry *FunctionRegistry
	// importMaps stores per-module import maps: moduleQN -> localName -> resolvedQN
	importMaps map[string]map[string]string
}

type cachedAST struct {
	Tree     *tree_sitter.Tree
	Source   []byte
	Language lang.Language
}

// New creates a new Pipeline.
func New(s *store.Store, repoPath string) *Pipeline {
	projectName := filepath.Base(repoPath)
	return &Pipeline{
		Store:       s,
		RepoPath:    repoPath,
		ProjectName: projectName,
		astCache:    make(map[string]*cachedAST),
		registry:    NewFunctionRegistry(),
		importMaps:  make(map[string]map[string]string),
	}
}

// Run executes the full 3-pass pipeline within a single transaction.
// If file hashes from a previous run exist, only changed files are re-processed.
func (p *Pipeline) Run() error {
	slog.Info("pipeline.start", "project", p.ProjectName, "path", p.RepoPath)

	// Discover source files (filesystem, no DB — runs outside transaction)
	files, err := discover.Discover(p.RepoPath, nil)
	if err != nil {
		return fmt.Errorf("discover: %w", err)
	}
	slog.Info("pipeline.discovered", "files", len(files))

	// Use MEMORY journal mode during fresh indexing for faster bulk writes.
	p.Store.BeginBulkWrite()

	wroteData := false
	if err := p.Store.WithTransaction(func(txStore *store.Store) error {
		origStore := p.Store
		p.Store = txStore
		defer func() { p.Store = origStore }()
		var passErr error
		wroteData, passErr = p.runPasses(files)
		return passErr
	}); err != nil {
		p.Store.EndBulkWrite()
		return err
	}

	p.Store.EndBulkWrite()

	// Only checkpoint + optimize when actual data was written.
	// No-op incremental reindexes skip this to avoid ANALYZE overhead.
	if wroteData {
		p.Store.Checkpoint()
	}

	nc, _ := p.Store.CountNodes(p.ProjectName)
	ec, _ := p.Store.CountEdges(p.ProjectName)
	slog.Info("pipeline.done", "nodes", nc, "edges", ec)
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
	t := time.Now()
	if err := p.passStructure(files); err != nil {
		return fmt.Errorf("pass1 structure: %w", err)
	}
	slog.Info("pass.timing", "pass", "structure", "elapsed", time.Since(t))

	t = time.Now()
	p.passDefinitions(files)
	slog.Info("pass.timing", "pass", "definitions", "elapsed", time.Since(t))

	t = time.Now()
	p.buildRegistry()
	slog.Info("pass.timing", "pass", "registry", "elapsed", time.Since(t))

	t = time.Now()
	p.passImports()
	slog.Info("pass.timing", "pass", "imports", "elapsed", time.Since(t))

	t = time.Now()
	p.passCalls()
	slog.Info("pass.timing", "pass", "calls", "elapsed", time.Since(t))

	t = time.Now()
	p.passUsages()
	slog.Info("pass.timing", "pass", "usages", "elapsed", time.Since(t))

	p.cleanupASTCache()

	t = time.Now()
	if err := p.passHTTPLinks(); err != nil {
		slog.Warn("pass4.httplink.err", "err", err)
	}
	slog.Info("pass.timing", "pass", "httplinks", "elapsed", time.Since(t))

	t = time.Now()
	p.passImplements()
	slog.Info("pass.timing", "pass", "implements", "elapsed", time.Since(t))

	t = time.Now()
	p.passGitHistory()
	slog.Info("pass.timing", "pass", "githistory", "elapsed", time.Since(t))

	t = time.Now()
	p.updateFileHashes(files)
	slog.Info("pass.timing", "pass", "filehashes", "elapsed", time.Since(t))

	return nil
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

	// Remove stale nodes/edges for deleted files
	p.removeDeletedFiles(allFiles)

	// Delete nodes for changed files (will be re-created in pass 2)
	for _, f := range changed {
		_ = p.Store.DeleteNodesByFile(p.ProjectName, f.RelPath)
	}

	// Pass 2: Parse changed files only
	p.passDefinitions(changed)

	// Build full registry: includes nodes from unchanged files (already in DB)
	// plus newly parsed nodes from changed files
	p.buildRegistry()

	// Re-build import maps for changed files (already done in passDefinitions)
	// Also load import maps for unchanged files from their AST (not cached)
	// For correctness, we need the full import map, but unchanged files don't
	// have ASTs cached. Rebuild imports only for changed files is sufficient
	// since unchanged file import edges still exist in DB.
	p.passImports()

	// Determine which files need call re-resolution:
	// changed files + files that import any changed module
	dependents := p.findDependentFiles(changed, unchanged)
	filesToResolve := mergeFiles(changed, dependents)
	slog.Info("incremental.resolve", "changed", len(changed), "dependents", len(dependents))

	// Delete CALLS and USAGE edges for files being re-resolved
	for _, f := range filesToResolve {
		_ = p.Store.DeleteEdgesBySourceFile(p.ProjectName, f.RelPath, "CALLS")
		_ = p.Store.DeleteEdgesBySourceFile(p.ProjectName, f.RelPath, "USAGE")
	}

	// Pass 3: Re-resolve calls + usages for changed + dependent files
	p.passCallsForFiles(filesToResolve)
	p.passUsagesForFiles(filesToResolve)

	p.cleanupASTCache()

	// HTTP linking and implements always run fully (they clean up first)
	if err := p.passHTTPLinks(); err != nil {
		slog.Warn("pass4.httplink.err", "err", err)
	}
	p.passImplements()
	p.passGitHistory()

	p.updateFileHashes(allFiles)
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

// passCallsForFiles resolves calls only for the specified files.
func (p *Pipeline) passCallsForFiles(files []discover.FileInfo) {
	slog.Info("pass3.calls.incremental", "files", len(files))
	for _, f := range files {
		cached, ok := p.astCache[f.RelPath]
		if !ok {
			// File not in AST cache — need to parse it
			source, err := os.ReadFile(f.Path)
			if err != nil {
				continue
			}
			tree, err := parser.Parse(f.Language, source)
			if err != nil {
				continue
			}
			cached = &cachedAST{Tree: tree, Source: source, Language: f.Language}
			p.astCache[f.RelPath] = cached
		}
		spec := lang.ForLanguage(cached.Language)
		if spec == nil {
			continue
		}
		p.processFileCalls(f.RelPath, cached, spec)
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
	for _, cached := range p.astCache {
		cached.Tree.Close()
	}
	p.astCache = nil
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

	// Collect all package indicators
	packageIndicators := make(map[string]bool)
	for _, l := range lang.AllLanguages() {
		spec := lang.ForLanguage(l)
		if spec != nil {
			for _, pi := range spec.PackageIndicators {
				packageIndicators[pi] = true
			}
		}
	}

	// Phase A: Collect all unique directories from file paths
	dirSet := make(map[string]bool)
	for _, f := range files {
		dir := filepath.Dir(f.RelPath)
		for dir != "." && dir != "" && !dirSet[dir] {
			dirSet[dir] = true
			dir = filepath.Dir(dir)
		}
	}

	// Phase B: Check package indicators via os.Stat (batched, not interleaved with SQL)
	dirIsPackage := make(map[string]bool, len(dirSet))
	for dir := range dirSet {
		absDir := filepath.Join(p.RepoPath, dir)
		for indicator := range packageIndicators {
			if _, err := os.Stat(filepath.Join(absDir, indicator)); err == nil {
				dirIsPackage[dir] = true
				break
			}
		}
	}

	// Phase C: Build all nodes and pending edges in memory
	var nodes []*store.Node
	var edges []pendingEdge

	// Project node
	projectQN := p.ProjectName
	nodes = append(nodes, &store.Node{
		Project:       p.ProjectName,
		Label:         "Project",
		Name:          p.ProjectName,
		QualifiedName: projectQN,
	})

	// Directory/Package nodes + containment edges
	for dir := range dirSet {
		dirName := filepath.Base(dir)
		label := "Folder"
		if dirIsPackage[dir] {
			label = "Package"
		}
		qn := fqn.FolderQN(p.ProjectName, dir)
		nodes = append(nodes, &store.Node{
			Project:       p.ProjectName,
			Label:         label,
			Name:          dirName,
			QualifiedName: qn,
			FilePath:      dir,
		})

		// Containment edge from parent
		parent := filepath.Dir(dir)
		var parentQN string
		if parent == "." || parent == "" {
			parentQN = projectQN
		} else {
			parentQN = fqn.FolderQN(p.ProjectName, parent)
		}

		edgeType := "CONTAINS_FOLDER"
		if dirIsPackage[dir] {
			edgeType = "CONTAINS_PACKAGE"
		}
		edges = append(edges, pendingEdge{
			SourceQN: parentQN,
			TargetQN: qn,
			Type:     edgeType,
		})
	}

	// File nodes + containment edges
	for _, f := range files {
		fileQN := fqn.Compute(p.ProjectName, f.RelPath, "") + ".__file__"
		nodes = append(nodes, &store.Node{
			Project:       p.ProjectName,
			Label:         "File",
			Name:          filepath.Base(f.RelPath),
			QualifiedName: fileQN,
			FilePath:      f.RelPath,
			Properties:    map[string]any{"extension": filepath.Ext(f.RelPath)},
		})

		dir := filepath.Dir(f.RelPath)
		parentQN := p.dirQN(dir)
		edges = append(edges, pendingEdge{
			SourceQN: parentQN,
			TargetQN: fileQN,
			Type:     "CONTAINS_FILE",
		})
	}

	// Phase D: Batch write all nodes
	idMap, err := p.Store.UpsertNodeBatch(nodes)
	if err != nil {
		return fmt.Errorf("pass1 batch upsert: %w", err)
	}

	// Resolve pending edges to real edges using ID map
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

	if err := p.Store.InsertEdgeBatch(realEdges); err != nil {
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
	Tree         *tree_sitter.Tree
	Source       []byte
	Nodes        []*store.Node
	PendingEdges []pendingEdge
	ImportMap    map[string]string
	Err          error
}

// passDefinitions parses each file and extracts function/class/method/module nodes.
// Uses parallel parsing (Stage 1) followed by sequential batch DB writes (Stage 2).
func (p *Pipeline) passDefinitions(files []discover.FileInfo) {
	slog.Info("pass2.definitions")

	// Separate JSON files (processed sequentially, they're fast and few)
	parseableFiles := make([]discover.FileInfo, 0, len(files))
	for _, f := range files {
		if f.Language == lang.JSON {
			if err := p.processJSONFile(f); err != nil {
				slog.Warn("pass2.json.err", "path", f.RelPath, "err", err)
			}
			continue
		}
		parseableFiles = append(parseableFiles, f)
	}

	if len(parseableFiles) == 0 {
		return
	}

	// Stage 1: Parallel parse (CPU-bound, no DB, no shared state)
	results := make([]*parseResult, len(parseableFiles))
	numWorkers := runtime.NumCPU()
	if numWorkers > len(parseableFiles) {
		numWorkers = len(parseableFiles)
	}

	g := new(errgroup.Group)
	g.SetLimit(numWorkers)
	for i, f := range parseableFiles {
		g.Go(func() error {
			results[i] = parseFileAST(p.ProjectName, f)
			return nil
		})
	}
	_ = g.Wait()

	// Stage 2: Sequential cache population + batch DB writes
	var allNodes []*store.Node
	var allPendingEdges []pendingEdge

	for _, r := range results {
		if r == nil || r.Err != nil {
			if r != nil && r.Err != nil {
				slog.Warn("pass2.file.err", "path", r.File.RelPath, "err", r.Err)
			}
			continue
		}
		// Populate AST cache (sequential, map writes)
		p.astCache[r.File.RelPath] = &cachedAST{
			Tree:     r.Tree,
			Source:   r.Source,
			Language: r.File.Language,
		}
		// Store import map
		moduleQN := fqn.ModuleQN(p.ProjectName, r.File.RelPath)
		if len(r.ImportMap) > 0 {
			p.importMaps[moduleQN] = r.ImportMap
		}
		allNodes = append(allNodes, r.Nodes...)
		allPendingEdges = append(allPendingEdges, r.PendingEdges...)
	}

	// Batch insert all nodes
	idMap, err := p.Store.UpsertNodeBatch(allNodes)
	if err != nil {
		slog.Warn("pass2.batch_upsert.err", "err", err)
		return
	}

	// Resolve pending edges to real edges using the ID map
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

	if err := p.Store.InsertEdgeBatch(edges); err != nil {
		slog.Warn("pass2.batch_edges.err", "err", err)
	}
}

// parseFileAST is a pure function that reads a file, parses its AST,
// and extracts all nodes and edges as data. No DB access, no shared state mutation.
func parseFileAST(projectName string, f discover.FileInfo) *parseResult {
	result := &parseResult{File: f}

	source, err := os.ReadFile(f.Path)
	if err != nil {
		result.Err = err
		return result
	}

	tree, err := parser.Parse(f.Language, source)
	if err != nil {
		result.Err = err
		return result
	}

	result.Tree = tree
	result.Source = source

	moduleQN := fqn.ModuleQN(projectName, f.RelPath)
	spec := lang.ForLanguage(f.Language)
	if spec == nil {
		return result
	}

	// Module node
	moduleNode := &store.Node{
		Project:       projectName,
		Label:         "Module",
		Name:          filepath.Base(f.RelPath),
		QualifiedName: moduleQN,
		FilePath:      f.RelPath,
	}
	result.Nodes = append(result.Nodes, moduleNode)

	// Extract definitions by walking the AST
	root := tree.RootNode()
	funcTypes := toSet(spec.FunctionNodeTypes)
	classTypes := toSet(spec.ClassNodeTypes)

	var constants []string

	// C/C++ macro tracking: extract macro definitions
	isCPP := f.Language == lang.CPP
	macroNames := make(map[string]bool) // track macro names for call site resolution

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		kind := node.Kind()

		if funcTypes[kind] {
			extractFunctionDef(node, source, f, projectName, moduleQN, result)
			return false
		}

		if classTypes[kind] {
			extractClassDef(node, source, f, projectName, moduleQN, spec, result)
			return false
		}

		// Macro definitions (C/C++ only)
		if isCPP && (kind == "preproc_def" || kind == "preproc_function_def") {
			extractMacroDef(node, source, f, projectName, moduleQN, macroNames, result)
			return false
		}

		if isConstantNode(node, f.Language) {
			c := extractConstant(node, source)
			if c != "" && len(c) > 1 {
				constants = append(constants, c)
			}
		}

		return true
	})

	// Store macro names in result for later call resolution (pass 3)
	if len(macroNames) > 0 {
		if moduleNode.Properties == nil {
			moduleNode.Properties = make(map[string]any)
		}
		macroList := make([]string, 0, len(macroNames))
		for name := range macroNames {
			macroList = append(macroList, name)
		}
		moduleNode.Properties["macros"] = macroList
	}

	// Resolve interpolated/concatenated strings
	resolved := resolveModuleStrings(root, source, f.Language)

	seen := make(map[string]bool, len(constants))
	for _, c := range constants {
		seen[c] = true
	}
	for name, value := range resolved {
		if value == "" {
			continue
		}
		entry := name + " = " + value
		if !seen[entry] {
			seen[entry] = true
			constants = append(constants, entry)
		}
	}

	// Update module node with constants (replace the initial module node)
	if len(constants) > 0 {
		moduleNode.Properties = map[string]any{"constants": constants}
	}

	// Parse imports
	result.ImportMap = parseImports(root, source, f.Language, projectName, f.RelPath)

	return result
}

// extractFunctionDef extracts a function/method node and DEFINES edge as data (no DB).
func extractFunctionDef(
	node *tree_sitter.Node, source []byte, f discover.FileInfo,
	projectName, moduleQN string, result *parseResult,
) {
	nameNode := node.ChildByFieldName("name")
	if nameNode == nil {
		return
	}
	name := parser.NodeText(nameNode, source)
	if name == "" {
		return
	}

	funcQN := fqn.Compute(projectName, f.RelPath, name)

	label := "Function"
	props := map[string]any{}

	paramsNode := node.ChildByFieldName("parameters")
	if paramsNode != nil {
		props["signature"] = parser.NodeText(paramsNode, source)
	}

	for _, field := range []string{"result", "return_type", "type"} {
		rtNode := node.ChildByFieldName(field)
		if rtNode != nil {
			props["return_type"] = parser.NodeText(rtNode, source)
			break
		}
	}

	recvNode := node.ChildByFieldName("receiver")
	if recvNode != nil {
		props["receiver"] = parser.NodeText(recvNode, source)
		label = "Method"
	}

	props["is_exported"] = isExported(name, f.Language)

	if f.Language == lang.Python {
		decorators := extractDecorators(node, source)
		if len(decorators) > 0 {
			props["decorators"] = decorators
			if hasFrameworkDecorator(decorators) {
				props["is_entry_point"] = true
			}
		}
	}

	if name == "main" {
		props["is_entry_point"] = true
	}

	startLine := safeRowToLine(node.StartPosition().Row)
	endLine := safeRowToLine(node.EndPosition().Row)

	result.Nodes = append(result.Nodes, &store.Node{
		Project:       projectName,
		Label:         label,
		Name:          name,
		QualifiedName: funcQN,
		FilePath:      f.RelPath,
		StartLine:     startLine,
		EndLine:       endLine,
		Properties:    props,
	})

	edgeType := "DEFINES"
	if label == "Method" {
		edgeType = "DEFINES_METHOD"
	}
	result.PendingEdges = append(result.PendingEdges, pendingEdge{
		SourceQN: moduleQN,
		TargetQN: funcQN,
		Type:     edgeType,
	})
}

// extractClassDef extracts a class/type node and its methods as data (no DB).
func extractClassDef(
	node *tree_sitter.Node, source []byte, f discover.FileInfo,
	projectName, moduleQN string, spec *lang.LanguageSpec, result *parseResult,
) {
	nameNode := node.ChildByFieldName("name")
	if nameNode == nil {
		return
	}
	name := parser.NodeText(nameNode, source)
	if name == "" {
		return
	}

	classQN := fqn.Compute(projectName, f.RelPath, name)
	label := classLabelForKind(node.Kind())

	if node.Kind() == "type_spec" {
		if typeNode := node.ChildByFieldName("type"); typeNode != nil {
			switch typeNode.Kind() {
			case "interface_type":
				label = "Interface"
			case "struct_type":
				label = "Class"
			}
		}
	}

	startLine := safeRowToLine(node.StartPosition().Row)
	endLine := safeRowToLine(node.EndPosition().Row)

	result.Nodes = append(result.Nodes, &store.Node{
		Project:       projectName,
		Label:         label,
		Name:          name,
		QualifiedName: classQN,
		FilePath:      f.RelPath,
		StartLine:     startLine,
		EndLine:       endLine,
		Properties:    map[string]any{"is_exported": isExported(name, f.Language)},
	})

	result.PendingEdges = append(result.PendingEdges, pendingEdge{
		SourceQN: moduleQN,
		TargetQN: classQN,
		Type:     "DEFINES",
	})

	// Extract methods inside the class
	extractClassMethodDefs(node, source, f, projectName, classQN, spec, result)

	// Extract fields inside the class/struct
	extractClassFieldDefs(node, source, f, projectName, classQN, spec, result)
}

// extractClassMethodDefs walks a class AST node and extracts Method nodes (no DB).
func extractClassMethodDefs(
	classNode *tree_sitter.Node, source []byte, f discover.FileInfo,
	projectName, classQN string, spec *lang.LanguageSpec, result *parseResult,
) {
	funcTypes := toSet(spec.FunctionNodeTypes)
	parser.Walk(classNode, func(child *tree_sitter.Node) bool {
		if child.Id() == classNode.Id() {
			return true
		}
		if !funcTypes[child.Kind()] {
			return true
		}

		mn := child.ChildByFieldName("name")
		if mn == nil {
			return false
		}
		methodName := parser.NodeText(mn, source)
		if methodName == "" {
			return false
		}

		methodQN := classQN + "." + methodName
		props := map[string]any{}

		paramsNode := child.ChildByFieldName("parameters")
		if paramsNode != nil {
			props["signature"] = parser.NodeText(paramsNode, source)
		}
		for _, field := range []string{"result", "return_type", "type"} {
			rtNode := child.ChildByFieldName(field)
			if rtNode != nil {
				props["return_type"] = parser.NodeText(rtNode, source)
				break
			}
		}
		props["is_exported"] = isExported(methodName, f.Language)

		mStartLine := safeRowToLine(child.StartPosition().Row)
		mEndLine := safeRowToLine(child.EndPosition().Row)

		result.Nodes = append(result.Nodes, &store.Node{
			Project:       projectName,
			Label:         "Method",
			Name:          methodName,
			QualifiedName: methodQN,
			FilePath:      f.RelPath,
			StartLine:     mStartLine,
			EndLine:       mEndLine,
			Properties:    props,
		})
		result.PendingEdges = append(result.PendingEdges, pendingEdge{
			SourceQN: classQN,
			TargetQN: methodQN,
			Type:     "DEFINES_METHOD",
		})
		return false
	})
}

// extractClassFieldDefs walks a class/struct AST node and extracts Field nodes (no DB).
func extractClassFieldDefs(
	classNode *tree_sitter.Node, source []byte, f discover.FileInfo,
	projectName, classQN string, spec *lang.LanguageSpec, result *parseResult,
) {
	if len(spec.FieldNodeTypes) == 0 {
		return
	}
	fieldTypes := toSet(spec.FieldNodeTypes)
	funcTypes := toSet(spec.FunctionNodeTypes)

	parser.Walk(classNode, func(child *tree_sitter.Node) bool {
		if child.Id() == classNode.Id() {
			return true
		}
		// Skip nested class/method definitions — they have their own extraction
		if funcTypes[child.Kind()] {
			return false
		}
		if !fieldTypes[child.Kind()] {
			return true
		}

		fieldName := extractFieldName(child, source, f.Language)
		if fieldName == "" {
			return false
		}

		fieldQN := classQN + "." + fieldName
		props := map[string]any{}

		// Extract type annotation if present
		fieldType := extractFieldType(child, source, f.Language)
		if fieldType != "" {
			props["type"] = fieldType
		}

		startLine := safeRowToLine(child.StartPosition().Row)
		endLine := safeRowToLine(child.EndPosition().Row)

		result.Nodes = append(result.Nodes, &store.Node{
			Project:       projectName,
			Label:         "Field",
			Name:          fieldName,
			QualifiedName: fieldQN,
			FilePath:      f.RelPath,
			StartLine:     startLine,
			EndLine:       endLine,
			Properties:    props,
		})
		result.PendingEdges = append(result.PendingEdges, pendingEdge{
			SourceQN: classQN,
			TargetQN: fieldQN,
			Type:     "DEFINES_FIELD",
		})
		return false
	})
}

// extractFieldName extracts the name from a field declaration node.
func extractFieldName(node *tree_sitter.Node, source []byte, l lang.Language) string {
	// Go: field_declaration has named children, first identifier is the name
	// C++/Java: field_declaration has a "declarator" field
	// Rust: field_declaration has a "name" field

	// Try "name" field first (Rust, some others)
	if nameNode := node.ChildByFieldName("name"); nameNode != nil {
		return parser.NodeText(nameNode, source)
	}

	// Try "declarator" field (C++, Java)
	if declNode := node.ChildByFieldName("declarator"); declNode != nil {
		// The declarator might be a pointer_declarator, array_declarator, etc.
		// Walk to find the identifier
		name := extractIdentifierFromDeclarator(declNode, source)
		if name != "" {
			return name
		}
	}

	// Go struct fields: first child that is an identifier (field_identifier)
	if l == lang.Go {
		for i := uint(0); i < node.ChildCount(); i++ {
			child := node.Child(i)
			if child != nil && (child.Kind() == "field_identifier" || child.Kind() == "identifier") {
				return parser.NodeText(child, source)
			}
		}
	}

	return ""
}

// extractIdentifierFromDeclarator walks a declarator subtree to find the identifier name.
func extractIdentifierFromDeclarator(node *tree_sitter.Node, source []byte) string {
	if node == nil {
		return ""
	}
	switch node.Kind() {
	case "identifier", "field_identifier":
		return parser.NodeText(node, source)
	case "pointer_declarator", "reference_declarator", "array_declarator":
		if declNode := node.ChildByFieldName("declarator"); declNode != nil {
			return extractIdentifierFromDeclarator(declNode, source)
		}
		// Fall through to child walk
	}
	// Walk children
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && (child.Kind() == "identifier" || child.Kind() == "field_identifier") {
			return parser.NodeText(child, source)
		}
	}
	return ""
}

// extractFieldType extracts the type annotation from a field declaration.
func extractFieldType(node *tree_sitter.Node, source []byte, l lang.Language) string {
	// Try "type" field (Go, Rust, Java)
	if typeNode := node.ChildByFieldName("type"); typeNode != nil {
		return parser.NodeText(typeNode, source)
	}
	return ""
}

// extractMacroDef extracts a Macro node from a C/C++ preprocessor definition.
func extractMacroDef(
	node *tree_sitter.Node, source []byte, f discover.FileInfo,
	projectName, moduleQN string, macroNames map[string]bool, result *parseResult,
) {
	nameNode := node.ChildByFieldName("name")
	if nameNode == nil {
		return
	}
	name := parser.NodeText(nameNode, source)
	if name == "" {
		return
	}

	macroNames[name] = true

	isFunctionLike := node.Kind() == "preproc_function_def"
	macroQN := moduleQN + "::macro::" + name

	props := map[string]any{
		"is_function_like": isFunctionLike,
	}

	if isFunctionLike {
		if paramsNode := node.ChildByFieldName("parameters"); paramsNode != nil {
			props["parameter_count"] = paramsNode.ChildCount()
		}
	}

	startLine := safeRowToLine(node.StartPosition().Row)
	endLine := safeRowToLine(node.EndPosition().Row)

	result.Nodes = append(result.Nodes, &store.Node{
		Project:       projectName,
		Label:         "Macro",
		Name:          name,
		QualifiedName: macroQN,
		FilePath:      f.RelPath,
		StartLine:     startLine,
		EndLine:       endLine,
		Properties:    props,
	})

	result.PendingEdges = append(result.PendingEdges, pendingEdge{
		SourceQN: moduleQN,
		TargetQN: macroQN,
		Type:     "DEFINES",
	})
}

// buildRegistry populates the FunctionRegistry from all Function, Method,
// and Class nodes in the store.
func (p *Pipeline) buildRegistry() {
	labels := []string{"Function", "Method", "Class", "Type", "Interface", "Enum", "Macro"}
	for _, label := range labels {
		nodes, err := p.Store.FindNodesByLabel(p.ProjectName, label)
		if err != nil {
			continue
		}
		for _, n := range nodes {
			p.registry.Register(n.Name, n.QualifiedName, n.Label)
		}
	}
	slog.Info("registry.built", "entries", p.registry.Size())
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

	// Collect files to process
	type fileEntry struct {
		relPath string
		cached  *cachedAST
	}
	var files []fileEntry
	for relPath, cached := range p.astCache {
		if lang.ForLanguage(cached.Language) != nil {
			files = append(files, fileEntry{relPath, cached})
		}
	}

	if len(files) == 0 {
		return
	}

	// Stage 1: Parallel per-file call resolution
	results := make([][]resolvedEdge, len(files))
	numWorkers := runtime.NumCPU()
	if numWorkers > len(files) {
		numWorkers = len(files)
	}

	g := new(errgroup.Group)
	g.SetLimit(numWorkers)
	for i, fe := range files {
		g.Go(func() error {
			results[i] = p.resolveFileCalls(fe.relPath, fe.cached)
			return nil
		})
	}
	_ = g.Wait()

	// Stage 2: Batch QN→ID resolution + batch edge insert
	p.flushResolvedEdges(results)
}

// resolveFileCalls resolves all call targets in a single file. Returns resolved edges as QN pairs.
// Thread-safe: reads from registry (RLock), importMaps (read-only), and AST cache (read-only).
func (p *Pipeline) resolveFileCalls(relPath string, cached *cachedAST) []resolvedEdge {
	spec := lang.ForLanguage(cached.Language)
	if spec == nil {
		return nil
	}

	callTypes := toSet(spec.CallNodeTypes)
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	root := cached.Tree.RootNode()
	importMap := p.importMaps[moduleQN]

	// Infer variable types for method dispatch
	typeMap := inferTypes(root, cached.Source, cached.Language, p.registry, moduleQN, importMap)

	var edges []resolvedEdge

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		if !callTypes[node.Kind()] {
			return true
		}

		calleeName := extractCalleeName(node, cached.Source, cached.Language)
		if calleeName == "" {
			return false
		}

		callerQN := findEnclosingFunction(node, cached.Source, p.ProjectName, relPath, spec)
		if callerQN == "" {
			callerQN = moduleQN
		}

		// Python self.method() resolution
		if cached.Language == lang.Python && strings.HasPrefix(calleeName, "self.") {
			classQN := findEnclosingClassQN(node, cached.Source, p.ProjectName, relPath)
			if classQN != "" {
				candidate := classQN + "." + calleeName[5:]
				if p.registry.Exists(candidate) {
					edges = append(edges, resolvedEdge{CallerQN: callerQN, TargetQN: candidate, Type: "CALLS"})
					return false
				}
			}
		}

		// Go receiver scoping
		localTypeMap := p.extendTypeMapWithReceiver(node, cached, typeMap, spec, moduleQN, importMap)

		targetQN := p.resolveCallWithTypes(calleeName, moduleQN, importMap, localTypeMap)
		if targetQN == "" {
			if fuzzyQN, ok := p.registry.FuzzyResolve(calleeName, moduleQN); ok {
				edges = append(edges, resolvedEdge{
					CallerQN:   callerQN,
					TargetQN:   fuzzyQN,
					Type:       "CALLS",
					Properties: map[string]any{"resolution_mode": "fuzzy"},
				})
			}
			return false
		}

		edges = append(edges, resolvedEdge{CallerQN: callerQN, TargetQN: targetQN, Type: "CALLS"})
		return false
	})

	return edges
}

// processFileCalls is the legacy sequential entry point for incremental re-indexing.
// It resolves calls for a single file and writes edges to DB immediately.
func (p *Pipeline) processFileCalls(relPath string, cached *cachedAST, _ *lang.LanguageSpec) {
	edges := p.resolveFileCalls(relPath, cached)
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
	qnToID, err := p.Store.FindNodeIDsByQNs(p.ProjectName, qns)
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

	if err := p.Store.InsertEdgeBatch(edges); err != nil {
		slog.Warn("pass3.batch_edges.err", "err", err)
	}
}

// extendTypeMapWithReceiver augments the type map with the Go receiver variable
// from the enclosing method declaration, if applicable.
func (p *Pipeline) extendTypeMapWithReceiver(
	node *tree_sitter.Node, cached *cachedAST, typeMap TypeMap,
	spec *lang.LanguageSpec, moduleQN string, importMap map[string]string,
) TypeMap {
	if cached.Language != lang.Go {
		return typeMap
	}
	funcTypes := toSet(spec.FunctionNodeTypes)
	enclosing := findEnclosingFuncNode(node, funcTypes)
	if enclosing == nil {
		return typeMap
	}
	varName, typeName := parseGoReceiverType(enclosing, cached.Source)
	if varName == "" || typeName == "" {
		return typeMap
	}
	classQN := resolveAsClass(typeName, p.registry, moduleQN, importMap)
	if classQN == "" {
		return typeMap
	}
	localTypeMap := make(TypeMap, len(typeMap)+1)
	for k, v := range typeMap {
		localTypeMap[k] = v
	}
	localTypeMap[varName] = classQN
	return localTypeMap
}

// resolveCallWithTypes resolves a callee name using the registry, import maps,
// and type inference for method dispatch.
func (p *Pipeline) resolveCallWithTypes(
	calleeName, moduleQN string,
	importMap map[string]string,
	typeMap TypeMap,
) string {
	// First, try type-based method dispatch for qualified calls like obj.method()
	if strings.Contains(calleeName, ".") {
		parts := strings.SplitN(calleeName, ".", 2)
		objName := parts[0]
		methodName := parts[1]

		// Check if the object has a known type from type inference
		if classQN, ok := typeMap[objName]; ok {
			candidate := classQN + "." + methodName
			if p.registry.Exists(candidate) {
				return candidate
			}
		}
	}

	// Delegate to the registry's resolution strategy
	return p.registry.Resolve(calleeName, moduleQN, importMap)
}

// === Helper functions ===

func extractCalleeName(node *tree_sitter.Node, source []byte, _ lang.Language) string {
	// For call_expression / call nodes, get the function being called
	funcNode := node.ChildByFieldName("function")
	if funcNode != nil {
		kind := funcNode.Kind()
		// Direct call: foo()
		if kind == "identifier" {
			return parser.NodeText(funcNode, source)
		}
		// Qualified call: pkg.Func() or obj.method()
		if kind == "selector_expression" || kind == "attribute" || kind == "member_expression" {
			return parser.NodeText(funcNode, source)
		}
	}

	// For method_invocation (Java), get name field
	nameNode := node.ChildByFieldName("name")
	if nameNode != nil {
		return parser.NodeText(nameNode, source)
	}

	return ""
}

func findEnclosingFunction(node *tree_sitter.Node, source []byte, project, relPath string, spec *lang.LanguageSpec) string {
	funcTypes := toSet(spec.FunctionNodeTypes)
	current := node.Parent()
	for current != nil {
		if funcTypes[current.Kind()] {
			nameNode := current.ChildByFieldName("name")
			if nameNode != nil {
				name := parser.NodeText(nameNode, source)
				return fqn.Compute(project, relPath, name)
			}
		}
		current = current.Parent()
	}
	return ""
}

func isConstantNode(node *tree_sitter.Node, language lang.Language) bool {
	// Only look at top-level nodes
	parent := node.Parent()
	if parent == nil {
		return false
	}
	parentKind := parent.Kind()
	kind := node.Kind()

	switch language {
	case lang.Go:
		return parentKind == "source_file" && (kind == "const_declaration" || kind == "var_declaration")
	case lang.Python:
		return parentKind == "module" && kind == "expression_statement"
	case lang.JavaScript, lang.TypeScript, lang.TSX:
		return parentKind == "program" && kind == "lexical_declaration"
	case lang.Rust:
		return parentKind == "source_file" && (kind == "const_item" || kind == "let_declaration")
	case lang.Java:
		// Java constants are in class bodies — handled differently (resolve.go walks class body)
		return false
	case lang.PHP:
		return parentKind == "program" && kind == "expression_statement"
	case lang.Scala:
		return (parentKind == "compilation_unit" || parentKind == "template_body") && kind == "val_definition"
	case lang.CPP:
		return parentKind == "translation_unit" && (kind == "preproc_def" || kind == "declaration")
	case lang.Lua:
		return parentKind == "chunk" && kind == "variable_declaration"
	default:
		return false
	}
}

func extractConstant(node *tree_sitter.Node, source []byte) string {
	text := parser.NodeText(node, source)
	// Take just the first line (name = value)
	if idx := strings.Index(text, "\n"); idx > 0 {
		text = text[:idx]
	}
	return strings.TrimSpace(text)
}

func extractDecorators(node *tree_sitter.Node, source []byte) []string {
	// In Python, decorators are siblings before the function_definition.
	// They show up as decorator children of a decorated_definition parent.
	parent := node.Parent()
	if parent == nil || parent.Kind() != "decorated_definition" {
		return nil
	}
	var decorators []string
	for i := uint(0); i < parent.ChildCount(); i++ {
		child := parent.Child(i)
		if child != nil && child.Kind() == "decorator" {
			decorators = append(decorators, parser.NodeText(child, source))
		}
	}
	return decorators
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

func isExported(name string, language lang.Language) bool {
	if name == "" {
		return false
	}
	switch language {
	case lang.Go:
		return name[0] >= 'A' && name[0] <= 'Z'
	case lang.Python:
		return !strings.HasPrefix(name, "_")
	case lang.Java, lang.CSharp:
		return name[0] >= 'A' && name[0] <= 'Z' // heuristic
	default:
		return true // assume exported
	}
}

func classLabelForKind(kind string) string {
	switch kind {
	case "interface_declaration", "trait_item", "trait_definition", "trait_declaration":
		return "Interface"
	case "enum_declaration", "enum_item", "enum_specifier":
		return "Enum"
	case "type_declaration", "type_alias_declaration", "type_item", "type_spec", "type_alias":
		return "Type"
	case "union_specifier", "union_item":
		return "Union"
	default:
		return "Class"
	}
}

func toSet(items []string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, item := range items {
		m[item] = true
	}
	return m
}

// passImports creates IMPORTS edges from the import maps built during pass 2.
func (p *Pipeline) passImports() {
	slog.Info("pass2b.imports")
	count := 0
	for moduleQN, importMap := range p.importMaps {
		moduleNode, _ := p.Store.FindNodeByQN(p.ProjectName, moduleQN)
		if moduleNode == nil {
			continue
		}
		for localName, targetQN := range importMap {
			// Try to find the target as a Module node first
			targetNode, _ := p.Store.FindNodeByQN(p.ProjectName, targetQN)
			if targetNode == nil {
				// Try common suffixes: module QN might need .__init__ or similar
				continue
			}
			_, _ = p.Store.InsertEdge(&store.Edge{
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
	_, err = p.Store.UpsertNode(&store.Node{
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
func safeRowToLine(row uint) int {
	const maxInt = int(^uint(0) >> 1) // math.MaxInt equivalent without importing math
	if row > uint(maxInt-1) {
		return maxInt
	}
	return int(row) + 1
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
