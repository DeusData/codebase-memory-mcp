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

	g := new(errgroup.Group)
	g.SetLimit(numWorkers)
	for i, fe := range files {
		g.Go(func() error {
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
	throwTypes := toSet(spec.ThrowNodeTypes)
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]
	root := cached.Tree.RootNode()

	var edges []resolvedEdge

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

		// Java: declared throws clause
		edges = append(edges, p.extractDeclaredThrows(node, cached.Source, spec, funcQN, moduleQN, importMap)...)

		// Body: throw/raise statements
		edges = append(edges, p.extractBodyThrows(node, cached, throwTypes, funcQN, moduleQN, importMap)...)

		return false
	})

	return edges
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
		if typeNode := child.ChildByFieldName("type"); typeNode != nil {
			return cleanTypeName(parser.NodeText(typeNode, source))
		}
		if child.NamedChildCount() > 0 {
			first := child.NamedChild(0)
			if first != nil && (first.Kind() == "identifier" || first.Kind() == "type_identifier") {
				return parser.NodeText(first, source)
			}
		}

	case "call", "call_expression":
		funcNode := child.ChildByFieldName("function")
		if funcNode == nil {
			return ""
		}
		name := parser.NodeText(funcNode, source)
		if idx := strings.LastIndex(name, "."); idx >= 0 {
			name = name[idx+1:]
		}
		return name

	case "identifier", "type_identifier":
		return parser.NodeText(child, source)
	}
	return ""
}
