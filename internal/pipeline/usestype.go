package pipeline

import (
	"log/slog"
	"runtime"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
	"golang.org/x/sync/errgroup"

	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
)

// passUsesType creates USES_TYPE edges from functions to type/class nodes
// referenced in their signatures and bodies.
// Parallel per-file (same two-stage pattern as passCalls/passUsages).
func (p *Pipeline) passUsesType() {
	slog.Info("pass.usestype")

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

	// Stage 1: Parallel per-file type reference extraction
	results := make([][]resolvedEdge, len(files))
	numWorkers := runtime.NumCPU()
	if numWorkers > len(files) {
		numWorkers = len(files)
	}

	g := new(errgroup.Group)
	g.SetLimit(numWorkers)
	for i, fe := range files {
		g.Go(func() error {
			results[i] = p.resolveFileTypeRefs(fe.relPath, fe.cached)
			return nil
		})
	}
	_ = g.Wait()

	// Stage 2: Batch write
	p.flushResolvedEdges(results)

	total := 0
	for _, r := range results {
		total += len(r)
	}
	slog.Info("pass.usestype.done", "edges", total)
}

// resolveFileTypeRefs extracts type references from function signatures and bodies.
func (p *Pipeline) resolveFileTypeRefs(relPath string, cached *cachedAST) []resolvedEdge {
	spec := lang.ForLanguage(cached.Language)
	if spec == nil {
		return nil
	}

	funcTypes := toSet(spec.FunctionNodeTypes)
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]
	root := cached.Tree.RootNode()

	var edges []resolvedEdge
	seen := make(map[[2]string]bool)

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		if !funcTypes[node.Kind()] {
			return true
		}

		nameNode := node.ChildByFieldName("name")
		if nameNode == nil {
			return false
		}
		funcName := parser.NodeText(nameNode, cached.Source)
		if funcName == "" {
			return false
		}
		funcQN := fqn.Compute(p.ProjectName, relPath, funcName)

		// Extract type references from signature + body
		allTypes := make(map[string]bool)
		for _, t := range p.extractSignatureTypeRefs(node, cached.Source, cached.Language) {
			allTypes[t] = true
		}
		for _, t := range p.extractBodyTypeRefs(node, cached.Source, cached.Language) {
			allTypes[t] = true
		}

		for typeName := range allTypes {
			if isBuiltinType(typeName) {
				continue
			}
			targetQN := resolveAsClass(typeName, p.registry, moduleQN, importMap)
			if targetQN == "" {
				continue
			}
			key := [2]string{funcQN, targetQN}
			if seen[key] {
				continue
			}
			seen[key] = true
			edges = append(edges, resolvedEdge{
				CallerQN: funcQN,
				TargetQN: targetQN,
				Type:     "USES_TYPE",
			})
		}

		return false
	})

	return edges
}

// extractSignatureTypeRefs extracts type names from function parameter + return type.
func (p *Pipeline) extractSignatureTypeRefs(funcNode *tree_sitter.Node, source []byte, language lang.Language) []string {
	var types []string

	if paramsNode := funcNode.ChildByFieldName("parameters"); paramsNode != nil {
		types = append(types, extractParamTypes(paramsNode, source, language)...)
	}

	for _, field := range []string{"result", "return_type", "type"} {
		if retNode := funcNode.ChildByFieldName(field); retNode != nil {
			types = append(types, extractReturnTypes(retNode, source, language)...)
			break
		}
	}

	return types
}

// extractBodyTypeRefs walks a function body for type references in
// local var declarations, type assertions, casts, and generic instantiations.
func (p *Pipeline) extractBodyTypeRefs(funcNode *tree_sitter.Node, source []byte, language lang.Language) []string {
	bodyNode := funcNode.ChildByFieldName("body")
	if bodyNode == nil {
		bodyNode = funcNode.ChildByFieldName("block")
	}
	if bodyNode == nil {
		return nil
	}

	var types []string
	seen := make(map[string]bool)

	addType := func(name string) {
		name = cleanTypeName(name)
		if name != "" && !isBuiltinType(name) && !seen[name] {
			seen[name] = true
			types = append(types, name)
		}
	}

	parser.Walk(bodyNode, func(node *tree_sitter.Node) bool {
		extractBodyTypeRef(node, source, language, addType)
		return true
	})

	return types
}

// extractBodyTypeRef extracts a type reference from a single body AST node.
func extractBodyTypeRef(node *tree_sitter.Node, source []byte, language lang.Language, addType func(string)) {
	kind := node.Kind()
	switch language {
	case lang.Go:
		extractGoBodyType(node, source, kind, addType)
	case lang.TypeScript, lang.TSX:
		extractTSBodyType(node, source, kind, addType)
	case lang.Java:
		extractJavaBodyType(node, source, kind, addType)
	case lang.Python:
		if kind == "assignment" {
			if typeNode := node.ChildByFieldName("type"); typeNode != nil {
				addType(parser.NodeText(typeNode, source))
			}
		}
	case lang.Rust:
		extractRustBodyType(node, source, kind, addType)
	}
}

func extractGoBodyType(node *tree_sitter.Node, source []byte, kind string, addType func(string)) {
	switch kind {
	case "var_spec", "type_assertion", "type_conversion_expression", "composite_literal":
		if typeNode := node.ChildByFieldName("type"); typeNode != nil {
			addType(parser.NodeText(typeNode, source))
		}
	}
}

func extractTSBodyType(node *tree_sitter.Node, source []byte, kind string, addType func(string)) {
	switch kind {
	case "variable_declarator":
		if typeAnn := findChildByKind(node, "type_annotation"); typeAnn != nil {
			addType(extractTypeFromAnnotation(typeAnn, source))
		}
	case "as_expression", "satisfies_expression":
		if typeNode := node.ChildByFieldName("type"); typeNode != nil {
			addType(parser.NodeText(typeNode, source))
		}
	case "type_arguments":
		for i := uint(0); i < node.NamedChildCount(); i++ {
			if child := node.NamedChild(i); child != nil {
				addType(parser.NodeText(child, source))
			}
		}
	}
}

func extractJavaBodyType(node *tree_sitter.Node, source []byte, kind string, addType func(string)) {
	switch kind {
	case "local_variable_declaration", "cast_expression":
		if typeNode := node.ChildByFieldName("type"); typeNode != nil {
			addType(parser.NodeText(typeNode, source))
		}
	case "generic_type":
		if typeArgs := findChildByKind(node, "type_arguments"); typeArgs != nil {
			for i := uint(0); i < typeArgs.NamedChildCount(); i++ {
				if child := typeArgs.NamedChild(i); child != nil {
					addType(parser.NodeText(child, source))
				}
			}
		}
	}
}

func extractRustBodyType(node *tree_sitter.Node, source []byte, kind string, addType func(string)) {
	switch kind {
	case "let_declaration", "type_cast_expression":
		if typeNode := node.ChildByFieldName("type"); typeNode != nil {
			addType(parser.NodeText(typeNode, source))
		}
	}
}
