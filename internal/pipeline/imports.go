package pipeline

import (
	"path/filepath"
	"strings"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"

	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
)

// parseImports extracts the import map for a source file.
// Returns localName -> resolvedQN mapping.
func parseImports(
	root *tree_sitter.Node,
	source []byte,
	language lang.Language,
	projectName, relPath string,
) map[string]string {
	switch language {
	case lang.Go:
		return parseGoImports(root, source, projectName)
	case lang.Python:
		return parsePythonImports(root, source, projectName, relPath)
	default:
		return nil
	}
}

// parseGoImports extracts Go import declarations.
// For each import spec: localName -> module QN (project-relative) or raw path.
//
// Go import AST structure:
//
//	import_declaration
//	  import_spec_list
//	    import_spec
//	      name: package_identifier (optional alias)
//	      path: interpreted_string_literal
func parseGoImports(
	root *tree_sitter.Node,
	source []byte,
	projectName string,
) map[string]string {
	imports := make(map[string]string)

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		if node.Kind() != "import_declaration" {
			return true
		}

		// Process each import_spec inside this declaration
		processGoImportDecl(node, source, projectName, imports)
		return false // don't recurse further
	})

	return imports
}

func processGoImportDecl(node *tree_sitter.Node, source []byte, projectName string, imports map[string]string) {
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		if child.Kind() != "import_spec" {
			return true
		}

		pathNode := child.ChildByFieldName("path")
		if pathNode == nil {
			return false
		}

		importPath := stripQuotes(parser.NodeText(pathNode, source))
		if importPath == "" {
			return false
		}

		// Determine the local name: alias if present, else last segment
		localName := lastPathSegment(importPath)
		nameNode := child.ChildByFieldName("name")
		if nameNode != nil {
			alias := parser.NodeText(nameNode, source)
			if alias != "" && alias != "." && alias != "_" {
				localName = alias
			}
		}

		// Resolve the import path to a project-internal QN if possible.
		// We check if any part of the import path matches the project name,
		// which indicates an internal package.
		resolvedQN := resolveGoImportPath(importPath, projectName)
		imports[localName] = resolvedQN

		return false
	})
}

// resolveGoImportPath converts a Go import path to a project-internal QN.
// For internal packages: "github.com/org/project/pkg/foo" -> "project.pkg.foo"
// For external packages: "fmt" -> "fmt", "net/http" -> "http"
func resolveGoImportPath(importPath, projectName string) string {
	parts := strings.Split(importPath, "/")

	// Check if this is a project-internal import by looking for the project
	// name in the path segments (common pattern: github.com/org/project/...)
	for i, part := range parts {
		if part == projectName {
			// Everything after the project name becomes the QN
			remaining := parts[i:]
			return strings.Join(remaining, ".")
		}
	}

	// External package: use the full path with dots
	return strings.Join(parts, ".")
}

// parsePythonImports extracts Python import statements.
//
// Python import AST structures:
//
//	import_statement:
//	  dotted_name children (e.g., "import foo.bar")
//	  aliased_import with alias (e.g., "import foo as f")
//
//	import_from_statement:
//	  module_name: dotted_name or relative_import
//	  name: dotted_name (what's being imported)
//	  Multiple names possible (e.g., "from foo import bar, baz")
func parsePythonImports(
	root *tree_sitter.Node,
	source []byte,
	projectName, relPath string,
) map[string]string {
	imports := make(map[string]string)

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		switch node.Kind() {
		case "import_statement":
			processPythonImport(node, source, projectName, imports)
			return false
		case "import_from_statement":
			processPythonFromImport(node, source, projectName, relPath, imports)
			return false
		}
		return true
	})

	return imports
}

// processPythonImport handles "import X" and "import X as Y" statements.
func processPythonImport(node *tree_sitter.Node, source []byte, projectName string, imports map[string]string) {
	for i := uint(0); i < node.NamedChildCount(); i++ {
		child := node.NamedChild(i)
		if child == nil {
			continue
		}

		switch child.Kind() {
		case "dotted_name":
			name := parser.NodeText(child, source)
			localName := lastDotSegment(name)
			imports[localName] = resolvePythonModule(name, projectName)

		case "aliased_import":
			nameNode := child.ChildByFieldName("name")
			aliasNode := child.ChildByFieldName("alias")
			if nameNode == nil {
				continue
			}
			name := parser.NodeText(nameNode, source)
			localName := lastDotSegment(name)
			if aliasNode != nil {
				localName = parser.NodeText(aliasNode, source)
			}
			imports[localName] = resolvePythonModule(name, projectName)
		}
	}
}

// processPythonFromImport handles "from X import Y" statements.
func processPythonFromImport(
	node *tree_sitter.Node,
	source []byte,
	projectName, relPath string,
	imports map[string]string,
) {
	// Get the module being imported from
	moduleNode := node.ChildByFieldName("module_name")
	var modulePath string
	isRelative := false

	if moduleNode != nil {
		modulePath = parser.NodeText(moduleNode, source)
		isRelative = strings.HasPrefix(modulePath, ".")
	} else {
		// Check for bare relative import: "from . import X"
		text := parser.NodeText(node, source)
		if strings.HasPrefix(text, "from .") {
			isRelative = true
			modulePath = "."
		}
	}

	// Resolve the base module
	var baseModule string
	if isRelative {
		baseModule = resolveRelativePythonImport(modulePath, relPath, projectName)
	} else {
		baseModule = resolvePythonModule(modulePath, projectName)
	}

	// Extract each imported name
	for i := uint(0); i < node.NamedChildCount(); i++ {
		child := node.NamedChild(i)
		if child == nil {
			continue
		}

		switch child.Kind() {
		case "dotted_name":
			name := parser.NodeText(child, source)
			// Skip the module_name itself (first dotted_name is often the source)
			if name == modulePath {
				continue
			}
			localName := lastDotSegment(name)
			if baseModule != "" {
				imports[localName] = baseModule + "." + name
			} else {
				imports[localName] = name
			}

		case "aliased_import":
			nameNode := child.ChildByFieldName("name")
			aliasNode := child.ChildByFieldName("alias")
			if nameNode == nil {
				continue
			}
			name := parser.NodeText(nameNode, source)
			localName := lastDotSegment(name)
			if aliasNode != nil {
				localName = parser.NodeText(aliasNode, source)
			}
			if baseModule != "" {
				imports[localName] = baseModule + "." + name
			} else {
				imports[localName] = name
			}
		}
	}
}

// resolvePythonModule converts a Python module path to a project QN.
// "utils" -> "project.utils", "foo.bar" -> "project.foo.bar"
func resolvePythonModule(modulePath, projectName string) string {
	if modulePath == "" {
		return projectName
	}
	return projectName + "." + modulePath
}

// resolveRelativePythonImport resolves relative imports like "from . import X"
// or "from ..utils import X" based on the current file's location.
func resolveRelativePythonImport(modulePath, relPath, projectName string) string {
	// Count leading dots for relative depth
	dots := 0
	for _, ch := range modulePath {
		if ch == '.' {
			dots++
		} else {
			break
		}
	}
	remainder := strings.TrimLeft(modulePath, ".")

	// Navigate up from the current file's directory
	dir := filepath.Dir(relPath)
	for i := 1; i < dots; i++ {
		dir = filepath.Dir(dir)
	}

	baseQN := fqn.FolderQN(projectName, dir)
	if dir == "." || dir == "" {
		baseQN = projectName
	}

	if remainder != "" {
		return baseQN + "." + remainder
	}
	return baseQN
}

// stripQuotes removes surrounding quotes from a string literal.
func stripQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
		// Handle backtick quotes (Go raw strings)
		if s[0] == '`' && s[len(s)-1] == '`' {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// lastPathSegment returns the last segment of a /-separated path.
func lastPathSegment(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
}

// lastDotSegment returns the last segment of a .-separated name.
func lastDotSegment(name string) string {
	parts := strings.Split(name, ".")
	return parts[len(parts)-1]
}
