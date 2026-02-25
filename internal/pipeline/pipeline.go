package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"

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

	if err := p.Store.WithTransaction(func() error {
		return p.runPasses(files)
	}); err != nil {
		return err
	}

	nc, _ := p.Store.CountNodes(p.ProjectName)
	ec, _ := p.Store.CountEdges(p.ProjectName)
	slog.Info("pipeline.done", "nodes", nc, "edges", ec)
	return nil
}

// runPasses executes all indexing passes (called within a transaction).
func (p *Pipeline) runPasses(files []discover.FileInfo) error {
	if err := p.Store.UpsertProject(p.ProjectName, p.RepoPath); err != nil {
		return fmt.Errorf("upsert project: %w", err)
	}

	// Classify files as changed/unchanged using stored hashes
	changed, unchanged := p.classifyFiles(files)

	// If all files are changed (first index or no hashes), do full pass
	isFullIndex := len(unchanged) == 0
	if isFullIndex {
		return p.runFullPasses(files)
	}

	slog.Info("incremental.classify", "changed", len(changed), "unchanged", len(unchanged), "total", len(files))
	return p.runIncrementalPasses(files, changed, unchanged)
}

// runFullPasses runs the complete pipeline (no incremental optimization).
func (p *Pipeline) runFullPasses(files []discover.FileInfo) error {
	if err := p.passStructure(files); err != nil {
		return fmt.Errorf("pass1 structure: %w", err)
	}
	p.passDefinitions(files)
	p.buildRegistry()
	p.passImports()
	p.passCalls()
	p.cleanupASTCache()
	if err := p.passHTTPLinks(); err != nil {
		slog.Warn("pass4.httplink.err", "err", err)
	}
	p.passImplements()
	p.updateFileHashes(files)
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

	// Delete CALLS edges for files being re-resolved
	for _, f := range filesToResolve {
		_ = p.Store.DeleteEdgesBySourceFile(p.ProjectName, f.RelPath, "CALLS")
	}

	// Pass 3: Re-resolve calls for changed + dependent files
	p.passCallsForFiles(filesToResolve)

	p.cleanupASTCache()

	// HTTP linking and implements always run fully (they clean up first)
	if err := p.passHTTPLinks(); err != nil {
		slog.Warn("pass4.httplink.err", "err", err)
	}
	p.passImplements()

	p.updateFileHashes(allFiles)
	return nil
}

// classifyFiles splits files into changed and unchanged based on stored hashes.
func (p *Pipeline) classifyFiles(files []discover.FileInfo) (changed, unchanged []discover.FileInfo) {
	storedHashes, err := p.Store.GetFileHashes(p.ProjectName)
	if err != nil || len(storedHashes) == 0 {
		return files, nil // no hashes → full index
	}

	for _, f := range files {
		currentHash, hashErr := fileHash(f.Path)
		if hashErr != nil {
			changed = append(changed, f)
			continue
		}
		if stored, ok := storedHashes[f.RelPath]; ok && stored == currentHash {
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
	for _, f := range files {
		hash, hashErr := fileHash(f.Path)
		if hashErr == nil {
			_ = p.Store.UpsertFileHash(p.ProjectName, f.RelPath, hash)
		}
	}
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
func (p *Pipeline) passStructure(files []discover.FileInfo) error {
	slog.Info("pass1.structure")

	// Create project node
	_, err := p.Store.UpsertNode(&store.Node{
		Project:       p.ProjectName,
		Label:         "Project",
		Name:          p.ProjectName,
		QualifiedName: p.ProjectName,
	})
	if err != nil {
		return err
	}

	// Track which directories we've created
	createdDirs := map[string]bool{}

	// Collect all package indicators
	packageIndicators := map[string]bool{}
	for _, l := range lang.AllLanguages() {
		spec := lang.ForLanguage(l)
		if spec != nil {
			for _, pi := range spec.PackageIndicators {
				packageIndicators[pi] = true
			}
		}
	}

	for _, f := range files {
		dir := filepath.Dir(f.RelPath)

		// Create directory hierarchy
		p.ensureDirHierarchy(dir, createdDirs, packageIndicators)

		// Create File node
		fileQN := fqn.Compute(p.ProjectName, f.RelPath, "")
		fileID, fileErr := p.Store.UpsertNode(&store.Node{
			Project:       p.ProjectName,
			Label:         "File",
			Name:          filepath.Base(f.RelPath),
			QualifiedName: fileQN + ".__file__",
			FilePath:      f.RelPath,
			Properties:    map[string]any{"extension": filepath.Ext(f.RelPath)},
		})
		if fileErr != nil {
			slog.Warn("pass1.file.err", "path", f.RelPath, "err", fileErr)
			continue
		}

		// Create CONTAINS_FILE edge from parent dir
		parentQN := p.dirQN(dir)
		parentNode, _ := p.Store.FindNodeByQN(p.ProjectName, parentQN)
		if parentNode != nil {
			_, _ = p.Store.InsertEdge(&store.Edge{
				Project:  p.ProjectName,
				SourceID: parentNode.ID,
				TargetID: fileID,
				Type:     "CONTAINS_FILE",
			})
		}
	}

	return nil
}

func (p *Pipeline) ensureDirHierarchy(relDir string, created, pkgIndicators map[string]bool) {
	if relDir == "." || relDir == "" || created[relDir] {
		return
	}

	// Ensure parent first
	parent := filepath.Dir(relDir)
	if parent != "." && parent != relDir {
		p.ensureDirHierarchy(parent, created, pkgIndicators)
	}

	created[relDir] = true

	// Check if this is a package
	absDir := filepath.Join(p.RepoPath, relDir)
	isPackage := false
	for indicator := range pkgIndicators {
		if _, err := os.Stat(filepath.Join(absDir, indicator)); err == nil {
			isPackage = true
			break
		}
	}

	dirName := filepath.Base(relDir)
	label := "Folder"
	if isPackage {
		label = "Package"
	}
	qn := fqn.FolderQN(p.ProjectName, relDir)

	nodeID, _ := p.Store.UpsertNode(&store.Node{
		Project:       p.ProjectName,
		Label:         label,
		Name:          dirName,
		QualifiedName: qn,
		FilePath:      relDir,
	})

	// Create containment edge from parent
	var parentQN string
	if parent == "." || parent == "" {
		parentQN = p.ProjectName
	} else {
		parentQN = fqn.FolderQN(p.ProjectName, parent)
	}

	parentNode, _ := p.Store.FindNodeByQN(p.ProjectName, parentQN)
	if parentNode != nil && nodeID > 0 {
		edgeType := "CONTAINS_FOLDER"
		if isPackage {
			edgeType = "CONTAINS_PACKAGE"
		}
		_, _ = p.Store.InsertEdge(&store.Edge{
			Project:  p.ProjectName,
			SourceID: parentNode.ID,
			TargetID: nodeID,
			Type:     edgeType,
		})
	}
}

func (p *Pipeline) dirQN(relDir string) string {
	if relDir == "." || relDir == "" {
		return p.ProjectName
	}
	return fqn.FolderQN(p.ProjectName, relDir)
}

// passDefinitions parses each file and extracts function/class/method/module nodes.
func (p *Pipeline) passDefinitions(files []discover.FileInfo) {
	slog.Info("pass2.definitions")

	for _, f := range files {
		if f.Language == lang.JSON {
			if err := p.processJSONFile(f); err != nil {
				slog.Warn("pass2.json.err", "path", f.RelPath, "err", err)
			}
			continue
		}
		if err := p.processFileDefinitions(f); err != nil {
			slog.Warn("pass2.file.err", "path", f.RelPath, "err", err)
			// Continue with other files
		}
	}
}

func (p *Pipeline) processFileDefinitions(f discover.FileInfo) error {
	source, err := os.ReadFile(f.Path)
	if err != nil {
		return err
	}

	tree, err := parser.Parse(f.Language, source)
	if err != nil {
		return err
	}

	// Cache for pass 3
	p.astCache[f.RelPath] = &cachedAST{
		Tree:     tree,
		Source:   source,
		Language: f.Language,
	}

	moduleQN := fqn.ModuleQN(p.ProjectName, f.RelPath)
	spec := lang.ForLanguage(f.Language)
	if spec == nil {
		return nil
	}

	// Create Module node
	moduleID, err := p.Store.UpsertNode(&store.Node{
		Project:       p.ProjectName,
		Label:         "Module",
		Name:          filepath.Base(f.RelPath),
		QualifiedName: moduleQN,
		FilePath:      f.RelPath,
	})
	if err != nil {
		return err
	}

	// Extract definitions by walking the AST
	root := tree.RootNode()
	funcTypes := toSet(spec.FunctionNodeTypes)
	classTypes := toSet(spec.ClassNodeTypes)

	// Track raw constants for display + resolved constants for HTTP linking
	var constants []string

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		kind := node.Kind()

		// Functions / methods
		if funcTypes[kind] {
			p.extractFunction(node, source, f, moduleID)
			return false // don't recurse into function body
		}

		// Classes / types / interfaces
		if classTypes[kind] {
			p.extractClass(node, source, f, moduleID, spec)
			return false // don't recurse; extractClass handles methods internally
		}

		// Module-level constants (top-level assignments, const declarations)
		if isConstantNode(node, f.Language) {
			c := extractConstant(node, source)
			if c != "" && len(c) > 1 { // skip single-char constants
				constants = append(constants, c)
			}
		}

		return true
	})

	// Resolve interpolated/concatenated strings via AST-based constant propagation.
	// This resolves e.g. f"{BASE_URL}/path" → "https://example.com/path" in RAM only.
	resolved := resolveModuleStrings(root, source, f.Language)

	// Merge resolved values into constants (deduplicated)
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

	// Update module with constants
	if len(constants) > 0 {
		_, _ = p.Store.UpsertNode(&store.Node{
			Project:       p.ProjectName,
			Label:         "Module",
			Name:          filepath.Base(f.RelPath),
			QualifiedName: moduleQN,
			FilePath:      f.RelPath,
			Properties:    map[string]any{"constants": constants},
		})
	}

	// Parse imports for this file and store the import map
	importMap := parseImports(root, source, f.Language, p.ProjectName, f.RelPath)
	if len(importMap) > 0 {
		p.importMaps[moduleQN] = importMap
	}

	return nil
}

func (p *Pipeline) extractFunction(
	node *tree_sitter.Node, source []byte, f discover.FileInfo,
	moduleID int64,
) {
	nameNode := node.ChildByFieldName("name")
	if nameNode == nil {
		return
	}
	name := parser.NodeText(nameNode, source)
	if name == "" {
		return
	}

	funcQN := fqn.Compute(p.ProjectName, f.RelPath, name)

	// Determine if it's a method (has receiver — Go) or top-level function
	label := "Function"
	props := map[string]any{}

	// Extract signature
	paramsNode := node.ChildByFieldName("parameters")
	if paramsNode != nil {
		props["signature"] = parser.NodeText(paramsNode, source)
	}

	// Extract return type (field name varies by language)
	for _, field := range []string{"result", "return_type", "type"} {
		rtNode := node.ChildByFieldName(field)
		if rtNode != nil {
			props["return_type"] = parser.NodeText(rtNode, source)
			break
		}
	}

	// Go receiver -> this is a Method
	recvNode := node.ChildByFieldName("receiver")
	if recvNode != nil {
		props["receiver"] = parser.NodeText(recvNode, source)
		label = "Method"
	}

	// Is exported? (Go: starts with uppercase, Python: no underscore prefix)
	props["is_exported"] = isExported(name, f.Language)

	// Extract decorators (Python)
	if f.Language == lang.Python {
		decorators := extractDecorators(node, source)
		if len(decorators) > 0 {
			props["decorators"] = decorators
			if hasFrameworkDecorator(decorators) {
				props["is_entry_point"] = true
			}
		}
	}

	// Mark main() functions as entry points
	if name == "main" {
		props["is_entry_point"] = true
	}

	startLine := safeRowToLine(node.StartPosition().Row)
	endLine := safeRowToLine(node.EndPosition().Row)

	funcID, err := p.Store.UpsertNode(&store.Node{
		Project:       p.ProjectName,
		Label:         label,
		Name:          name,
		QualifiedName: funcQN,
		FilePath:      f.RelPath,
		StartLine:     startLine,
		EndLine:       endLine,
		Properties:    props,
	})
	if err != nil {
		return
	}

	// Create DEFINES edge from Module
	edgeType := "DEFINES"
	if label == "Method" {
		edgeType = "DEFINES_METHOD"
	}
	_, _ = p.Store.InsertEdge(&store.Edge{
		Project:  p.ProjectName,
		SourceID: moduleID,
		TargetID: funcID,
		Type:     edgeType,
	})
}

func (p *Pipeline) extractClass(
	node *tree_sitter.Node, source []byte, f discover.FileInfo,
	moduleID int64, spec *lang.LanguageSpec,
) {
	nameNode := node.ChildByFieldName("name")
	if nameNode == nil {
		return
	}
	name := parser.NodeText(nameNode, source)
	if name == "" {
		return
	}

	classQN := fqn.Compute(p.ProjectName, f.RelPath, name)

	// Determine specific label
	label := classLabelForKind(node.Kind())

	// Go type_spec: determine actual type from child (interface vs struct)
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

	classID, err := p.Store.UpsertNode(&store.Node{
		Project:       p.ProjectName,
		Label:         label,
		Name:          name,
		QualifiedName: classQN,
		FilePath:      f.RelPath,
		StartLine:     startLine,
		EndLine:       endLine,
		Properties:    map[string]any{"is_exported": isExported(name, f.Language)},
	})
	if err != nil {
		return
	}

	// DEFINES edge from Module
	_, _ = p.Store.InsertEdge(&store.Edge{
		Project:  p.ProjectName,
		SourceID: moduleID,
		TargetID: classID,
		Type:     "DEFINES",
	})

	// Extract methods inside the class
	p.extractClassMethods(node, source, f, classQN, classID, spec)
}

// extractClassMethods walks a class AST node and creates Method nodes for each method found.
func (p *Pipeline) extractClassMethods(
	classNode *tree_sitter.Node, source []byte, f discover.FileInfo,
	classQN string, classID int64, spec *lang.LanguageSpec,
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

		methodID, upsertErr := p.Store.UpsertNode(&store.Node{
			Project:       p.ProjectName,
			Label:         "Method",
			Name:          methodName,
			QualifiedName: methodQN,
			FilePath:      f.RelPath,
			StartLine:     mStartLine,
			EndLine:       mEndLine,
			Properties:    props,
		})
		if upsertErr == nil {
			_, _ = p.Store.InsertEdge(&store.Edge{
				Project:  p.ProjectName,
				SourceID: classID,
				TargetID: methodID,
				Type:     "DEFINES_METHOD",
			})
		}
		return false
	})
}

// buildRegistry populates the FunctionRegistry from all Function, Method,
// and Class nodes in the store.
func (p *Pipeline) buildRegistry() {
	labels := []string{"Function", "Method", "Class", "Type", "Interface", "Enum"}
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

// passCalls resolves call targets and creates CALLS edges.
func (p *Pipeline) passCalls() {
	slog.Info("pass3.calls")

	for relPath, cached := range p.astCache {
		spec := lang.ForLanguage(cached.Language)
		if spec == nil {
			continue
		}
		p.processFileCalls(relPath, cached, spec)
	}
}

func (p *Pipeline) processFileCalls(relPath string, cached *cachedAST, spec *lang.LanguageSpec) {
	callTypes := toSet(spec.CallNodeTypes)
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)

	root := cached.Tree.RootNode()

	// Get import map for this module
	importMap := p.importMaps[moduleQN]

	// Infer variable types for method dispatch
	typeMap := inferTypes(root, cached.Source, cached.Language, p.registry, moduleQN, importMap)

	// Walk to find calls and resolve them
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
		if p.resolvePythonSelfCall(node, cached, calleeName, callerQN, relPath) {
			return false
		}

		// Go receiver scoping — extend type map with receiver variable
		localTypeMap := p.extendTypeMapWithReceiver(node, cached, typeMap, spec, moduleQN, importMap)

		targetQN := p.resolveCallWithTypes(calleeName, moduleQN, importMap, localTypeMap)
		if targetQN == "" {
			return false
		}

		p.createCallEdge(callerQN, targetQN)
		return false
	})
}

// resolvePythonSelfCall handles Python self.method() resolution via enclosing class scope.
// Returns true if the call was resolved.
func (p *Pipeline) resolvePythonSelfCall(node *tree_sitter.Node, cached *cachedAST, calleeName, callerQN, relPath string) bool {
	if cached.Language != lang.Python || !strings.HasPrefix(calleeName, "self.") {
		return false
	}
	classQN := findEnclosingClassQN(node, cached.Source, p.ProjectName, relPath)
	if classQN == "" {
		return false
	}
	candidate := classQN + "." + calleeName[5:]
	if n, _ := p.Store.FindNodeByQN(p.ProjectName, candidate); n != nil {
		p.createCallEdge(callerQN, candidate)
		return true
	}
	return false
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

// createCallEdge creates a CALLS edge between caller and target by QN.
func (p *Pipeline) createCallEdge(callerQN, targetQN string) {
	callerNode, _ := p.Store.FindNodeByQN(p.ProjectName, callerQN)
	targetNode, _ := p.Store.FindNodeByQN(p.ProjectName, targetQN)
	if callerNode != nil && targetNode != nil {
		_, _ = p.Store.InsertEdge(&store.Edge{
			Project:  p.ProjectName,
			SourceID: callerNode.ID,
			TargetID: targetNode.ID,
			Type:     "CALLS",
		})
	}
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
			if node, _ := p.Store.FindNodeByQN(p.ProjectName, candidate); node != nil {
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
	// Clean up stale Route nodes and HTTP_CALLS/HANDLES/ASYNC_CALLS edges before re-running
	_ = p.Store.DeleteNodesByLabel(p.ProjectName, "Route")
	_ = p.Store.DeleteEdgesByType(p.ProjectName, "HTTP_CALLS")
	_ = p.Store.DeleteEdgesByType(p.ProjectName, "HANDLES")
	_ = p.Store.DeleteEdgesByType(p.ProjectName, "ASYNC_CALLS")

	// Scan config files for env var URLs and create synthetic Module nodes
	envBindings := ScanProjectEnvURLs(p.RepoPath)
	if len(envBindings) > 0 {
		p.injectEnvBindings(envBindings)
	}

	linker := httplink.New(p.Store, p.ProjectName)
	links, err := linker.Run()
	if err != nil {
		return err
	}
	slog.Info("pass4.httplinks", "links", len(links))
	return nil
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
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
