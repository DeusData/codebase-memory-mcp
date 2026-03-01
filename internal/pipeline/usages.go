package pipeline

import (
	"log/slog"
	"runtime"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
	"golang.org/x/sync/errgroup"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// passUsages walks ASTs and creates USAGE edges for identifier references
// that are NOT inside call expressions (those are already CALLS edges).
// Uses parallel per-file resolution (Stage 1) followed by batch DB writes (Stage 2).
func (p *Pipeline) passUsages() {
	slog.Info("pass3b.usages")

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

	// Stage 1: Parallel per-file usage resolution
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
			results[i] = p.resolveFileUsages(fe.relPath, fe.cached)
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
	slog.Info("pass3b.usages.done", "edges", total)
}

// passUsagesForFiles runs usage detection only for the specified files (incremental).
func (p *Pipeline) passUsagesForFiles(files []discover.FileInfo) {
	slog.Info("pass3b.usages.incremental", "files", len(files))
	count := 0
	for _, f := range files {
		if p.ctx.Err() != nil {
			return
		}
		cached, ok := p.astCache[f.RelPath]
		if !ok {
			continue
		}
		edges := p.resolveFileUsages(f.RelPath, cached)
		// Write edges directly for incremental (small count)
		for _, re := range edges {
			callerNode, _ := p.Store.FindNodeByQN(p.ProjectName, re.CallerQN)
			targetNode, _ := p.Store.FindNodeByQN(p.ProjectName, re.TargetQN)
			if callerNode != nil && targetNode != nil {
				_, _ = p.Store.InsertEdge(&store.Edge{
					Project:  p.ProjectName,
					SourceID: callerNode.ID,
					TargetID: targetNode.ID,
					Type:     re.Type,
				})
				count++
			}
		}
	}
	slog.Info("pass3b.usages.incremental.done", "edges", count)
}

// referenceNodeTypes returns the AST node types that represent identifier
// references for a given language.
func referenceNodeTypes(language lang.Language) []string {
	switch language {
	case lang.Go:
		return []string{"identifier", "selector_expression"}
	case lang.Python:
		return []string{"identifier", "attribute"}
	case lang.JavaScript, lang.TypeScript, lang.TSX:
		return []string{"identifier", "member_expression"}
	case lang.Rust:
		return []string{"identifier", "scoped_identifier"}
	case lang.Java:
		return []string{"identifier", "field_access"}
	case lang.CPP:
		return []string{"identifier", "qualified_identifier"}
	case lang.PHP:
		return []string{"name", "member_access_expression"}
	case lang.Scala:
		return []string{"identifier", "field_expression"}
	case lang.Kotlin:
		return []string{"identifier", "navigation_expression"}
	default:
		return []string{"identifier"}
	}
}

// importNodeTypes returns the set of AST node types that represent import statements.
func importNodeTypes(spec *lang.LanguageSpec) map[string]bool {
	combined := make(map[string]bool)
	for _, t := range spec.ImportNodeTypes {
		combined[t] = true
	}
	for _, t := range spec.ImportFromTypes {
		combined[t] = true
	}
	return combined
}

// resolveFileUsages walks a file's AST to find identifier references that
// are not call expressions and resolves them to USAGE edges.
// Thread-safe: reads from registry (RLock) and importMaps (read-only).
func (p *Pipeline) resolveFileUsages(relPath string, cached *cachedAST) []resolvedEdge {
	spec := lang.ForLanguage(cached.Language)
	if spec == nil {
		return nil
	}

	refTypes := toSet(referenceNodeTypes(cached.Language))
	callTypes := toSet(spec.CallNodeTypes)
	importTypes := importNodeTypes(spec)
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]

	root := cached.Tree.RootNode()
	var edges []resolvedEdge

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		kind := node.Kind()

		if callTypes[kind] {
			return false
		}

		if importTypes[kind] {
			return false
		}

		if !refTypes[kind] {
			return true
		}

		if isDefinitionName(node) {
			return false
		}

		refName := parser.NodeText(node, cached.Source)
		if refName == "" || isKeywordOrBuiltin(refName, cached.Language) {
			return false
		}

		callerQN := findEnclosingFunction(node, cached.Source, p.ProjectName, relPath, spec)
		if callerQN == "" {
			callerQN = moduleQN
		}

		targetQN := p.registry.Resolve(refName, moduleQN, importMap)
		if targetQN == "" {
			return false
		}

		// Skip Variable targets — handled by passReadsWrites (READS/WRITES edges)
		if p.registry.LabelOf(targetQN) == "Variable" {
			return false
		}

		if targetQN == callerQN {
			return false
		}

		edges = append(edges, resolvedEdge{CallerQN: callerQN, TargetQN: targetQN, Type: "USAGE"})
		return false
	})
	return edges
}

// isDefinitionName returns true if the node is the name child of a function,
// class, method, or variable declaration — not a reference.
func isDefinitionName(node *tree_sitter.Node) bool {
	parent := node.Parent()
	if parent == nil {
		return false
	}
	nameChild := parent.ChildByFieldName("name")
	if nameChild != nil && nameChild.StartByte() == node.StartByte() && nameChild.EndByte() == node.EndByte() {
		parentKind := parent.Kind()
		switch parentKind {
		case "function_declaration", "function_definition", "method_declaration",
			"method_definition", "class_declaration", "class_definition",
			"type_spec", "type_alias", "interface_declaration",
			"enum_declaration", "trait_item", "struct_item",
			"generator_function_declaration", "function_expression",
			"arrow_function", "abstract_class_declaration",
			"function_signature", "type_alias_declaration",
			"short_var_declaration", "var_spec", "const_spec":
			return true
		}
	}
	return false
}

// isKeywordOrBuiltin returns true for language keywords and common builtins
// that should not be treated as references.
func isKeywordOrBuiltin(name string, language lang.Language) bool {
	// Single-character identifiers and very common names are noise
	if len(name) <= 1 {
		return true
	}

	// Common cross-language keywords
	switch name {
	case "if", "else", "for", "while", "return", "break", "continue",
		"switch", "case", "default", "try", "catch", "finally",
		"throw", "throws", "new", "delete", "this", "self", "super",
		"true", "false", "nil", "null", "None", "True", "False",
		"var", "let", "const", "int", "string", "bool", "float",
		"void", "byte", "rune", "error", "any", "interface",
		"class", "struct", "enum", "type", "func", "def", "fn",
		"import", "from", "as", "package", "module",
		"public", "private", "protected", "static", "final",
		"async", "await", "yield", "defer", "go", "chan",
		"range", "map", "make", "append", "len", "cap",
		"print", "println", "fmt", "os", "log",
		"isinstance", "str", "dict", "list", "tuple", "set",
		"Math", "Object", "Array", "String", "Number", "Boolean",
		"console", "document", "window", "undefined",
		"err", "ok", "ctx":
		return true
	}

	// Language-specific builtins
	switch language {
	case lang.Go:
		switch name {
		case "iota", "copy", "close", "panic", "recover",
			"int8", "int16", "int32", "int64",
			"uint", "uint8", "uint16", "uint32", "uint64",
			"float32", "float64", "complex64", "complex128",
			"uintptr":
			return true
		}
	case lang.Python:
		switch name {
		case "print", "range", "enumerate", "zip", "map", "filter",
			"sorted", "reversed", "open", "input", "super",
			"Exception", "ValueError", "TypeError", "KeyError",
			"IndexError", "AttributeError", "RuntimeError",
			"classmethod", "staticmethod", "property",
			"abstractmethod":
			return true
		}
	}

	return false
}
