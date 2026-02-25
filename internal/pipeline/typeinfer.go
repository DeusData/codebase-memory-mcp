package pipeline

import (
	"strings"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"

	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
)

// TypeMap tracks variable names to their inferred class/type qualified names.
// Key: variable name, Value: class/type QN in the registry.
type TypeMap map[string]string

// inferTypes walks the AST looking for variable assignments where the value
// is a constructor call (class instantiation) and builds a mapping from
// variable name to the class QN. This enables resolving method calls like
// `obj.method()` to `ClassName.method`.
func inferTypes(
	root *tree_sitter.Node,
	source []byte,
	language lang.Language,
	registry *FunctionRegistry,
	moduleQN string,
	importMap map[string]string,
) TypeMap {
	types := make(TypeMap)

	switch language {
	case lang.Python:
		inferPythonTypes(root, source, registry, moduleQN, importMap, types)
	case lang.Go:
		inferGoTypes(root, source, registry, moduleQN, importMap, types)
	}

	return types
}

// inferPythonTypes handles Python patterns like:
//
//	x = ClassName(args)
//	x = module.ClassName(args)
func inferPythonTypes(
	root *tree_sitter.Node,
	source []byte,
	registry *FunctionRegistry,
	moduleQN string,
	importMap map[string]string,
	types TypeMap,
) {
	parser.Walk(root, func(node *tree_sitter.Node) bool {
		// Look for assignment: expression_statement -> assignment
		if node.Kind() != "assignment" {
			return true
		}

		leftNode := node.ChildByFieldName("left")
		rightNode := node.ChildByFieldName("right")
		if leftNode == nil || rightNode == nil {
			return false
		}

		// Left side must be a simple identifier
		if leftNode.Kind() != "identifier" {
			return false
		}
		varName := parser.NodeText(leftNode, source)

		// Right side must be a call expression
		if rightNode.Kind() != "call" {
			return false
		}

		calleeName := extractCalleeForTypeInfer(rightNode, source)
		if calleeName == "" {
			return false
		}

		// Resolve the callee to see if it's a class
		classQN := resolveAsClass(calleeName, registry, moduleQN, importMap)
		if classQN != "" {
			types[varName] = classQN
		}

		return false
	})
}

// inferGoTypes handles Go patterns like:
//
//	var x = StructName{...}  (composite_literal)
//	x := StructName{...}     (short_var_declaration)
//	var x StructName          (var_declaration with type)
func inferGoTypes(
	root *tree_sitter.Node,
	source []byte,
	registry *FunctionRegistry,
	moduleQN string,
	importMap map[string]string,
	types TypeMap,
) {
	parser.Walk(root, func(node *tree_sitter.Node) bool {
		switch node.Kind() {
		case "short_var_declaration":
			inferGoShortVar(node, source, registry, moduleQN, importMap, types)
			return false
		case "var_declaration":
			inferGoVarDecl(node, source, registry, moduleQN, importMap, types)
			return false
		}
		return true
	})
}

// inferGoShortVar handles: x := StructName{} or x := pkg.StructName{}
func inferGoShortVar(
	node *tree_sitter.Node,
	source []byte,
	registry *FunctionRegistry,
	moduleQN string,
	importMap map[string]string,
	types TypeMap,
) {
	leftNode := node.ChildByFieldName("left")
	rightNode := node.ChildByFieldName("right")
	if leftNode == nil || rightNode == nil {
		return
	}

	varName := extractFirstIdentifier(leftNode, source)
	if varName == "" {
		return
	}

	// Check if right side is a composite literal (struct initialization)
	typeName := extractCompositeLiteralType(rightNode, source)
	if typeName == "" {
		// Try call expression (constructor pattern: NewFoo())
		if rightNode.Kind() == "expression_list" && rightNode.NamedChildCount() > 0 {
			firstExpr := rightNode.NamedChild(0)
			if firstExpr != nil {
				typeName = extractCompositeLiteralType(firstExpr, source)
			}
		}
		if typeName == "" {
			return
		}
	}

	classQN := resolveAsClass(typeName, registry, moduleQN, importMap)
	if classQN != "" {
		types[varName] = classQN
	}
}

// inferGoVarDecl handles: var x StructName or var x = StructName{}
func inferGoVarDecl(
	node *tree_sitter.Node,
	source []byte,
	registry *FunctionRegistry,
	moduleQN string,
	importMap map[string]string,
	types TypeMap,
) {
	// Walk var_spec children
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		if child.Kind() != "var_spec" {
			return true
		}

		nameNode := child.ChildByFieldName("name")
		typeNode := child.ChildByFieldName("type")
		if nameNode == nil {
			return false
		}

		varName := parser.NodeText(nameNode, source)

		// If there's an explicit type, use it
		if typeNode != nil {
			typeName := parser.NodeText(typeNode, source)
			// Strip pointer prefix
			typeName = strings.TrimPrefix(typeName, "*")
			classQN := resolveAsClass(typeName, registry, moduleQN, importMap)
			if classQN != "" {
				types[varName] = classQN
			}
		}

		return false
	})
}

// resolveAsClass checks if a name refers to a Class/Type node in the registry.
func resolveAsClass(name string, registry *FunctionRegistry, moduleQN string, importMap map[string]string) string {
	qn := registry.Resolve(name, moduleQN, importMap)
	if qn == "" {
		return ""
	}

	registry.mu.RLock()
	defer registry.mu.RUnlock()

	label, exists := registry.exact[qn]
	if !exists {
		return ""
	}

	// Only return if it's a class-like node
	switch label {
	case "Class", "Type", "Interface", "Enum":
		return qn
	}
	return ""
}

// extractCalleeForTypeInfer extracts the function/class name from a call node.
func extractCalleeForTypeInfer(callNode *tree_sitter.Node, source []byte) string {
	funcNode := callNode.ChildByFieldName("function")
	if funcNode == nil {
		return ""
	}

	switch funcNode.Kind() {
	case "identifier":
		return parser.NodeText(funcNode, source)
	case "attribute", "selector_expression":
		return parser.NodeText(funcNode, source)
	}
	return ""
}

// extractFirstIdentifier gets the first identifier from an expression list node.
func extractFirstIdentifier(node *tree_sitter.Node, source []byte) string {
	if node.Kind() == "identifier" {
		return parser.NodeText(node, source)
	}
	if node.Kind() == "expression_list" && node.NamedChildCount() > 0 {
		first := node.NamedChild(0)
		if first != nil && first.Kind() == "identifier" {
			return parser.NodeText(first, source)
		}
	}
	return ""
}

// extractCompositeLiteralType extracts the type name from a composite literal.
// E.g., "StructName{field: val}" -> "StructName"
func extractCompositeLiteralType(node *tree_sitter.Node, source []byte) string {
	if node.Kind() == "expression_list" && node.NamedChildCount() > 0 {
		node = node.NamedChild(0)
		if node == nil {
			return ""
		}
	}
	if node.Kind() != "composite_literal" {
		return ""
	}
	typeNode := node.ChildByFieldName("type")
	if typeNode == nil {
		return ""
	}
	typeName := parser.NodeText(typeNode, source)
	// Handle pointer types
	typeName = strings.TrimPrefix(typeName, "&")
	typeName = strings.TrimPrefix(typeName, "*")
	return typeName
}

// findEnclosingClassQN walks up the AST from a call node to find the enclosing
// class_definition (Python) and returns the class's qualified name.
// Returns "" if the call is not inside a class.
func findEnclosingClassQN(node *tree_sitter.Node, source []byte, project, relPath string) string {
	current := node.Parent()
	for current != nil {
		if current.Kind() == "class_definition" {
			nameNode := current.ChildByFieldName("name")
			if nameNode != nil {
				className := parser.NodeText(nameNode, source)
				return fqn.Compute(project, relPath, className)
			}
		}
		current = current.Parent()
	}
	return ""
}

// parseGoReceiverType extracts the receiver type name from a Go method's
// function_declaration node. Returns the variable name and type name.
// E.g., "(s *Server)" -> ("s", "Server")
func parseGoReceiverType(funcNode *tree_sitter.Node, source []byte) (varName, typeName string) {
	recvNode := funcNode.ChildByFieldName("receiver")
	if recvNode == nil {
		return "", ""
	}
	recvText := parser.NodeText(recvNode, source)
	// Strip parens: "(s *Server)" -> "s *Server"
	recvText = strings.TrimPrefix(recvText, "(")
	recvText = strings.TrimSuffix(recvText, ")")
	recvText = strings.TrimSpace(recvText)

	parts := strings.Fields(recvText)
	if len(parts) < 2 {
		return "", ""
	}
	varName = parts[0]
	typeName = parts[1]
	typeName = strings.TrimPrefix(typeName, "*")
	return varName, typeName
}

// findEnclosingFuncNode walks up the AST to find the nearest function_declaration
// or method_declaration ancestor.
func findEnclosingFuncNode(node *tree_sitter.Node, funcTypes map[string]bool) *tree_sitter.Node {
	current := node.Parent()
	for current != nil {
		if funcTypes[current.Kind()] {
			return current
		}
		current = current.Parent()
	}
	return nil
}
