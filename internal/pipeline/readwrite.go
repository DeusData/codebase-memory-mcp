package pipeline

import (
	"log/slog"
	"runtime"
	"strings"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
	"golang.org/x/sync/errgroup"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// passVariables extracts module-level variable declarations and creates Variable
// nodes + DEFINES edges during passDefinitions.
func extractVariables(
	root *tree_sitter.Node, source []byte, f discover.FileInfo,
	projectName, moduleQN string, spec *lang.LanguageSpec, result *parseResult,
) {
	if spec == nil || len(spec.VariableNodeTypes) == 0 {
		return
	}
	varTypes := toSet(spec.VariableNodeTypes)

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		if !isModuleLevelNode(node, f.Language) || !varTypes[node.Kind()] {
			return true
		}

		names := extractVarNames(node, source, f.Language)
		for _, name := range names {
			if name == "" || name == "_" {
				continue
			}
			varQN := fqn.Compute(projectName, f.RelPath, name)
			startLine := safeRowToLine(node.StartPosition().Row)
			endLine := safeRowToLine(node.EndPosition().Row)

			result.Nodes = append(result.Nodes, &store.Node{
				Project:       projectName,
				Label:         "Variable",
				Name:          name,
				QualifiedName: varQN,
				FilePath:      f.RelPath,
				StartLine:     startLine,
				EndLine:       endLine,
				Properties:    map[string]any{"is_exported": isExported(name, f.Language)},
			})
			result.PendingEdges = append(result.PendingEdges, pendingEdge{
				SourceQN: moduleQN,
				TargetQN: varQN,
				Type:     "DEFINES",
			})
		}
		return false
	})
}

// isModuleLevelNode checks if a node is at the module/file top level.
func isModuleLevelNode(node *tree_sitter.Node, language lang.Language) bool {
	parent := node.Parent()
	if parent == nil {
		return false
	}
	parentKind := parent.Kind()

	switch language {
	case lang.Go, lang.Rust:
		return parentKind == "source_file"
	case lang.Python:
		if parentKind == "module" {
			return true
		}
		// Python wraps assignments: module → expression_statement → assignment
		if parentKind == "expression_statement" {
			if gp := parent.Parent(); gp != nil {
				return gp.Kind() == "module"
			}
		}
	case lang.JavaScript, lang.TypeScript, lang.TSX:
		if parentKind == "program" {
			return true
		}
		// JS/TS wraps exports: program → export_statement → declaration
		if parentKind == "export_statement" {
			if gp := parent.Parent(); gp != nil {
				return gp.Kind() == "program"
			}
		}
	}
	return false
}

// extractVarNames extracts variable names from a declaration node.
func extractVarNames(node *tree_sitter.Node, source []byte, language lang.Language) []string {
	switch language {
	case lang.Go:
		return extractGoVarNames(node, source)
	case lang.Python:
		return extractPythonVarNames(node, source)
	case lang.JavaScript, lang.TypeScript, lang.TSX:
		return extractJSVarNames(node, source)
	case lang.Rust:
		return extractRustVarNames(node, source)
	}
	return nil
}

func extractGoVarNames(node *tree_sitter.Node, source []byte) []string {
	var names []string
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		if child.Kind() == "var_spec" || child.Kind() == "const_spec" {
			if nameNode := child.ChildByFieldName("name"); nameNode != nil {
				names = append(names, parser.NodeText(nameNode, source))
			}
			return false
		}
		return true
	})
	return names
}

func extractPythonVarNames(node *tree_sitter.Node, source []byte) []string {
	leftNode := node.ChildByFieldName("left")
	if leftNode == nil || leftNode.Kind() != "identifier" {
		return nil
	}
	name := parser.NodeText(leftNode, source)
	if strings.HasPrefix(name, "__") {
		return nil
	}
	return []string{name}
}

func extractJSVarNames(node *tree_sitter.Node, source []byte) []string {
	var names []string
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		if child.Kind() == "variable_declarator" {
			if nameNode := child.ChildByFieldName("name"); nameNode != nil {
				if nameNode.Kind() == "identifier" {
					names = append(names, parser.NodeText(nameNode, source))
				}
			}
			return false
		}
		return true
	})
	return names
}

func extractRustVarNames(node *tree_sitter.Node, source []byte) []string {
	if nameNode := node.ChildByFieldName("name"); nameNode != nil {
		return []string{parser.NodeText(nameNode, source)}
	}
	return nil
}

// passReadsWrites creates READS and WRITES edges from functions to Variable nodes.
func (p *Pipeline) passReadsWrites() {
	slog.Info("pass.readwrite")

	type fileEntry struct {
		relPath string
		cached  *cachedAST
	}
	var files []fileEntry
	for relPath, cached := range p.astCache {
		spec := lang.ForLanguage(cached.Language)
		if spec != nil && len(spec.AssignmentNodeTypes) > 0 {
			files = append(files, fileEntry{relPath, cached})
		}
	}

	if len(files) == 0 {
		return
	}

	results := make([][]resolvedEdge, len(files))
	numWorkers := runtime.NumCPU()
	if numWorkers > len(files) {
		numWorkers = len(files)
	}

	g := new(errgroup.Group)
	g.SetLimit(numWorkers)
	for i, fe := range files {
		g.Go(func() error {
			results[i] = p.resolveFileReadsWrites(fe.relPath, fe.cached)
			return nil
		})
	}
	_ = g.Wait()

	p.flushResolvedEdges(results)

	total := 0
	for _, r := range results {
		total += len(r)
	}
	slog.Info("pass.readwrite.done", "edges", total)
}

func (p *Pipeline) resolveFileReadsWrites(relPath string, cached *cachedAST) []resolvedEdge {
	spec := lang.ForLanguage(cached.Language)
	if spec == nil {
		return nil
	}

	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]
	assignTypes := toSet(spec.AssignmentNodeTypes)
	root := cached.Tree.RootNode()

	var edges []resolvedEdge
	seen := make(map[[3]string]bool)

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		if node.Kind() != "identifier" {
			return true
		}

		refName := parser.NodeText(node, cached.Source)
		if refName == "" || isKeywordOrBuiltin(refName, cached.Language) {
			return false
		}

		targetQN := p.resolveVariableStrict(refName, moduleQN, importMap)
		if targetQN == "" {
			return false
		}

		callerQN := findEnclosingFunction(node, cached.Source, p.ProjectName, relPath, spec)
		if callerQN == "" {
			callerQN = moduleQN
		}
		if callerQN == targetQN {
			return false
		}

		edgeType := "READS"
		if isWriteContext(node, assignTypes, cached.Language) {
			edgeType = "WRITES"

			if isAugmentedAssignmentContext(node) {
				readKey := [3]string{callerQN, targetQN, "READS"}
				if !seen[readKey] {
					seen[readKey] = true
					edges = append(edges, resolvedEdge{
						CallerQN: callerQN,
						TargetQN: targetQN,
						Type:     "READS",
					})
				}
			}
		}

		key := [3]string{callerQN, targetQN, edgeType}
		if seen[key] {
			return false
		}
		seen[key] = true

		edges = append(edges, resolvedEdge{
			CallerQN: callerQN,
			TargetQN: targetQN,
			Type:     edgeType,
		})
		return false
	})

	return edges
}

// resolveVariableStrict resolves an identifier to a Variable node using only
// Strategy 1 (import map) and Strategy 2 (same module). Never project-wide fallback.
func (p *Pipeline) resolveVariableStrict(name, moduleQN string, importMap map[string]string) string {
	p.registry.mu.RLock()
	defer p.registry.mu.RUnlock()

	if importMap != nil {
		if resolved, ok := importMap[name]; ok {
			if label, exists := p.registry.exact[resolved]; exists && label == "Variable" {
				return resolved
			}
		}
	}

	candidate := moduleQN + "." + name
	if label, exists := p.registry.exact[candidate]; exists && label == "Variable" {
		return candidate
	}

	return ""
}

// isWriteContext returns true if the identifier is on the left side of an assignment.
func isWriteContext(node *tree_sitter.Node, assignTypes map[string]bool, language lang.Language) bool {
	parent := node.Parent()
	if parent == nil {
		return false
	}

	// For Go short_var_declaration, left is always expression_list
	if language == lang.Go && parent.Kind() == "expression_list" {
		if gp := parent.Parent(); gp != nil && gp.Kind() == "short_var_declaration" {
			leftNode := gp.ChildByFieldName("left")
			if leftNode != nil && leftNode.Id() == parent.Id() {
				return true
			}
		}
	}

	if !assignTypes[parent.Kind()] {
		return false
	}

	leftNode := parent.ChildByFieldName("left")
	if leftNode == nil {
		return false
	}

	if leftNode.Id() == node.Id() {
		return true
	}

	// For Go: left is expression_list, check if node is inside it
	if leftNode.Kind() == "expression_list" {
		for i := uint(0); i < leftNode.NamedChildCount(); i++ {
			child := leftNode.NamedChild(i)
			if child != nil && child.Id() == node.Id() {
				return true
			}
		}
	}

	return false
}

// isAugmentedAssignmentContext returns true if the node is in an augmented assignment (+=, -=).
func isAugmentedAssignmentContext(node *tree_sitter.Node) bool {
	parent := node.Parent()
	if parent == nil {
		return false
	}
	switch parent.Kind() {
	case "augmented_assignment", "augmented_assignment_expression", "compound_assignment_expr":
		return true
	}
	return false
}

// extractGlobalVarNames extracts module-level variable names for the global_vars
// Module property.
func extractGlobalVarNames(root *tree_sitter.Node, source []byte, f discover.FileInfo, spec *lang.LanguageSpec) []string {
	if spec == nil || len(spec.VariableNodeTypes) == 0 {
		return nil
	}
	varTypes := toSet(spec.VariableNodeTypes)
	var names []string

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		if !isModuleLevelNode(node, f.Language) || !varTypes[node.Kind()] {
			return true
		}
		names = append(names, extractVarNames(node, source, f.Language)...)
		return false
	})

	// Deduplicate
	seen := make(map[string]bool, len(names))
	deduped := names[:0]
	for _, n := range names {
		if n != "" && n != "_" && !seen[n] {
			seen[n] = true
			deduped = append(deduped, n)
		}
	}
	return deduped
}
