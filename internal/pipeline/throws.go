package pipeline

import (
	"log/slog"
	"runtime"
	"strings"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
	"golang.org/x/sync/errgroup"

	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
)

// passThrows creates THROWS/RAISES edges from functions to exception classes.
// Covers Java throws clauses + throw/raise statements in Java, Python, TS/JS.
func (p *Pipeline) passThrows() {
	slog.Info("pass.throws")

	type fileEntry struct {
		relPath string
		cached  *cachedAST
	}
	files := make([]fileEntry, 0, len(p.astCache))
	for relPath, cached := range p.astCache {
		spec := lang.ForLanguage(cached.Language)
		if spec == nil {
			continue
		}
		if len(spec.ThrowNodeTypes) == 0 && spec.ThrowsClauseField == "" {
			continue
		}
		files = append(files, fileEntry{relPath, cached})
	}

	if len(files) == 0 {
		return
	}

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
			results[i] = p.resolveFileThrows(fe.relPath, fe.cached)
			return nil
		})
	}
	_ = g.Wait()

	p.flushResolvedEdges(results)

	total := 0
	for _, r := range results {
		total += len(r)
	}
	slog.Info("pass.throws.done", "edges", total)
}

func (p *Pipeline) resolveFileThrows(relPath string, cached *cachedAST) []resolvedEdge {
	spec := lang.ForLanguage(cached.Language)
	if spec == nil {
		return nil
	}

	funcTypes := toSet(spec.FunctionNodeTypes)
	classTypes := toSet(spec.ClassNodeTypes)
	throwTypes := toSet(spec.ThrowNodeTypes)
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]
	root := cached.Tree.RootNode()

	var edges []resolvedEdge

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		if !funcTypes[node.Kind()] {
			return true
		}

		funcQN, funcName := computeFuncQN(node, cached.Source, p.ProjectName, relPath, classTypes)
		if funcName == "" {
			return false
		}

		// Java: declared throws clause
		edges = append(edges, p.extractDeclaredThrows(node, cached.Source, spec, funcQN, moduleQN, importMap)...)

		// Body: throw/raise statements
		edges = append(edges, p.extractBodyThrows(node, cached, throwTypes, funcQN, moduleQN, importMap)...)

		return false
	})

	return edges
}

// computeFuncQN computes the correct qualified name for a function node,
// handling both top-level functions and methods nested inside classes.
// Also handles C++ where the function name is in function_declarator.
func computeFuncQN(node *tree_sitter.Node, source []byte, projectName, relPath string, classTypes map[string]bool) (qn, name string) {
	nameNode := funcNameNode(node)
	if nameNode == nil {
		return "", ""
	}
	name = parser.NodeText(nameNode, source)
	if name == "" {
		return "", ""
	}

	// Check for enclosing class to compute correct QN
	current := node.Parent()
	for current != nil {
		if classTypes[current.Kind()] {
			classNameNode := current.ChildByFieldName("name")
			if classNameNode != nil {
				className := parser.NodeText(classNameNode, source)
				classQN := fqn.Compute(projectName, relPath, className)
				return classQN + "." + name, name
			}
		}
		current = current.Parent()
	}

	return fqn.Compute(projectName, relPath, name), name
}

// extractDeclaredThrows handles Java's "throws ExceptionA, ExceptionB" clause.
func (p *Pipeline) extractDeclaredThrows(node *tree_sitter.Node, source []byte, spec *lang.LanguageSpec, funcQN, moduleQN string, importMap map[string]string) []resolvedEdge {
	if spec.ThrowsClauseField == "" {
		return nil
	}
	throwsNode := node.ChildByFieldName(spec.ThrowsClauseField)
	if throwsNode == nil {
		return nil
	}

	var edges []resolvedEdge
	for i := uint(0); i < throwsNode.NamedChildCount(); i++ {
		child := throwsNode.NamedChild(i)
		if child == nil {
			continue
		}
		exName := cleanTypeName(parser.NodeText(child, source))
		if exName == "" || isBuiltinType(exName) {
			continue
		}
		targetQN := resolveAsClass(exName, p.registry, moduleQN, importMap)
		if targetQN != "" {
			edges = append(edges, resolvedEdge{
				CallerQN:   funcQN,
				TargetQN:   targetQN,
				Type:       "THROWS",
				Properties: map[string]any{"declared": true},
			})
		}
	}
	return edges
}

// extractBodyThrows walks function body for throw/raise statements.
func (p *Pipeline) extractBodyThrows(node *tree_sitter.Node, cached *cachedAST, throwTypes map[string]bool, funcQN, moduleQN string, importMap map[string]string) []resolvedEdge {
	bodyNode := node.ChildByFieldName("body")
	if bodyNode == nil {
		bodyNode = node.ChildByFieldName("block")
	}
	if bodyNode == nil {
		// Kotlin: function_body has no field name
		bodyNode = findChildByKind(node, "function_body")
	}
	if bodyNode == nil {
		return nil
	}

	var edges []resolvedEdge
	parser.Walk(bodyNode, func(child *tree_sitter.Node) bool {
		if !throwTypes[child.Kind()] {
			return true
		}

		exName := extractThrownExceptionName(child, cached.Source)
		if exName == "" || isBuiltinType(exName) {
			return false
		}
		targetQN := resolveAsClass(exName, p.registry, moduleQN, importMap)
		if targetQN != "" {
			edges = append(edges, resolvedEdge{
				CallerQN:   funcQN,
				TargetQN:   targetQN,
				Type:       "RAISES",
				Properties: map[string]any{"declared": false},
			})
		}
		return false
	})
	return edges
}

// extractThrownExceptionName extracts the class name from a throw/raise statement.
func extractThrownExceptionName(throwNode *tree_sitter.Node, source []byte) string {
	for i := uint(0); i < throwNode.NamedChildCount(); i++ {
		child := throwNode.NamedChild(i)
		if child == nil {
			continue
		}
		if name := extractExceptionClassName(child, source); name != "" {
			return name
		}
	}
	return ""
}

// extractExceptionClassName extracts the class name from an exception expression.
func extractExceptionClassName(child *tree_sitter.Node, source []byte) string {
	switch child.Kind() {
	case "new_expression", "object_creation_expression":
		return extractNewExprClassName(child, source)
	case "instance_expression":
		return extractInstanceExprClassName(child, source)
	case "call", "call_expression":
		return extractCallExprClassName(child, source)
	case "identifier", "type_identifier", "name":
		return parser.NodeText(child, source)
	}
	return ""
}

// extractNewExprClassName extracts the class name from a new expression.
func extractNewExprClassName(node *tree_sitter.Node, source []byte) string {
	if typeNode := node.ChildByFieldName("type"); typeNode != nil {
		return cleanTypeName(parser.NodeText(typeNode, source))
	}
	if node.NamedChildCount() == 0 {
		return ""
	}
	first := node.NamedChild(0)
	if first == nil {
		return ""
	}
	switch first.Kind() {
	case "identifier", "type_identifier", "name":
		return parser.NodeText(first, source)
	}
	return ""
}

// extractInstanceExprClassName extracts the class name from a Scala instance_expression.
func extractInstanceExprClassName(node *tree_sitter.Node, source []byte) string {
	for i := uint(0); i < node.NamedChildCount(); i++ {
		n := node.NamedChild(i)
		if n != nil && (n.Kind() == "type_identifier" || n.Kind() == "identifier") {
			return parser.NodeText(n, source)
		}
	}
	return ""
}

// extractCallExprClassName extracts the class name from a call/call_expression in throw context.
func extractCallExprClassName(node *tree_sitter.Node, source []byte) string {
	funcNode := node.ChildByFieldName("function")
	if funcNode == nil {
		return extractFirstIdentifierChild(node, source)
	}
	name := parser.NodeText(funcNode, source)
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	return name
}

// extractFirstIdentifierChild returns the text of the first identifier/simple_identifier child.
func extractFirstIdentifierChild(node *tree_sitter.Node, source []byte) string {
	for i := uint(0); i < node.NamedChildCount(); i++ {
		n := node.NamedChild(i)
		if n != nil && (n.Kind() == "identifier" || n.Kind() == "simple_identifier") {
			return parser.NodeText(n, source)
		}
	}
	return ""
}
