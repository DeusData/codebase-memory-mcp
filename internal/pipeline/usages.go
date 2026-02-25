package pipeline

import (
	"log/slog"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// passUsages walks ASTs and creates USAGE edges for identifier references
// that are NOT inside call expressions (those are already CALLS edges).
// This distinguishes read references (passed as callbacks, stored in variables,
// used in type annotations) from function invocations.
func (p *Pipeline) passUsages() {
	slog.Info("pass3b.usages")
	count := 0
	for relPath, cached := range p.astCache {
		spec := lang.ForLanguage(cached.Language)
		if spec == nil {
			continue
		}
		count += p.processFileUsages(relPath, cached, spec)
	}
	slog.Info("pass3b.usages.done", "edges", count)
}

// passUsagesForFiles runs usage detection only for the specified files (incremental).
func (p *Pipeline) passUsagesForFiles(files []discover.FileInfo) {
	slog.Info("pass3b.usages.incremental", "files", len(files))
	count := 0
	for _, f := range files {
		cached, ok := p.astCache[f.RelPath]
		if !ok {
			continue
		}
		spec := lang.ForLanguage(cached.Language)
		if spec == nil {
			continue
		}
		count += p.processFileUsages(f.RelPath, cached, spec)
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

// processFileUsages walks a file's AST to find identifier references that
// are not call expressions and creates USAGE edges for resolved references.
func (p *Pipeline) processFileUsages(relPath string, cached *cachedAST, spec *lang.LanguageSpec) int {
	refTypes := toSet(referenceNodeTypes(cached.Language))
	callTypes := toSet(spec.CallNodeTypes)
	importTypes := importNodeTypes(spec)
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]

	root := cached.Tree.RootNode()
	count := 0

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		kind := node.Kind()

		// Skip call expression subtrees — those are handled by passCalls
		if callTypes[kind] {
			return false
		}

		// Skip import statement subtrees
		if importTypes[kind] {
			return false
		}

		if !refTypes[kind] {
			return true
		}

		// Skip if this identifier is inside a function/class definition name
		if isDefinitionName(node) {
			return false
		}

		refName := parser.NodeText(node, cached.Source)
		if refName == "" || isKeywordOrBuiltin(refName, cached.Language) {
			return false
		}

		// Find enclosing function for the "caller" side of the USAGE edge
		callerQN := findEnclosingFunction(node, cached.Source, p.ProjectName, relPath, spec)
		if callerQN == "" {
			callerQN = moduleQN
		}

		// Try to resolve the reference against the registry
		targetQN := p.registry.Resolve(refName, moduleQN, importMap)
		if targetQN == "" {
			return false
		}

		// Don't create USAGE edge to self
		if targetQN == callerQN {
			return false
		}

		// Create the USAGE edge
		callerNode, _ := p.Store.FindNodeByQN(p.ProjectName, callerQN)
		targetNode, _ := p.Store.FindNodeByQN(p.ProjectName, targetQN)
		if callerNode != nil && targetNode != nil {
			_, _ = p.Store.InsertEdge(&store.Edge{
				Project:  p.ProjectName,
				SourceID: callerNode.ID,
				TargetID: targetNode.ID,
				Type:     "USAGE",
			})
			count++
		}
		return false
	})
	return count
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
