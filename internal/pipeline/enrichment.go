package pipeline

import (
	"strings"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// countBranchingNodes counts branching AST nodes inside a function body
// as a proxy for cyclomatic complexity.
func countBranchingNodes(funcNode *tree_sitter.Node, branchingTypes []string) int {
	branchSet := toSet(branchingTypes)
	count := 0
	parser.Walk(funcNode, func(node *tree_sitter.Node) bool {
		if node.Id() == funcNode.Id() {
			return true // skip self, walk children
		}
		if branchSet[node.Kind()] {
			count++
		}
		return true
	})
	return count
}

// extractParamTypes extracts type names from a function's parameter list.
// Returns a slice of type name strings (e.g., ["Config", "string", "int"]).
func extractParamTypes(paramsNode *tree_sitter.Node, source []byte, language lang.Language) []string {
	var types []string
	seen := make(map[string]bool)

	addType := func(name string) {
		if name != "" && !isBuiltinType(name) && !seen[name] {
			seen[name] = true
			types = append(types, name)
		}
	}

	parser.Walk(paramsNode, func(node *tree_sitter.Node) bool {
		if node.Id() == paramsNode.Id() {
			return true
		}
		return extractParamType(node, source, language, addType)
	})
	return types
}

// extractParamType handles a single parameter node per language.
// Returns false to stop recursion when a param node is handled.
func extractParamType(node *tree_sitter.Node, source []byte, language lang.Language, addType func(string)) bool {
	switch language {
	case lang.Go:
		if node.Kind() == "parameter_declaration" {
			if typeNode := node.ChildByFieldName("type"); typeNode != nil {
				addType(cleanTypeName(parser.NodeText(typeNode, source)))
			}
			return false
		}
	case lang.Python:
		if node.Kind() == "typed_parameter" {
			if typeNode := node.ChildByFieldName("type"); typeNode != nil {
				addType(cleanTypeName(parser.NodeText(typeNode, source)))
			}
			return false
		}
	case lang.TypeScript, lang.TSX:
		if node.Kind() == "required_parameter" || node.Kind() == "optional_parameter" {
			if typeAnn := findChildByKind(node, "type_annotation"); typeAnn != nil {
				addType(extractTypeFromAnnotation(typeAnn, source))
			}
			return false
		}
	case lang.Java:
		if node.Kind() == "formal_parameter" || node.Kind() == "spread_parameter" {
			if typeNode := node.ChildByFieldName("type"); typeNode != nil {
				addType(cleanTypeName(parser.NodeText(typeNode, source)))
			}
			return false
		}
	case lang.Rust:
		if node.Kind() == "parameter" {
			if typeNode := node.ChildByFieldName("type"); typeNode != nil {
				addType(cleanTypeName(parser.NodeText(typeNode, source)))
			}
			return false
		}
	}
	return true
}

// extractReturnTypes extracts type names from a return type node.
func extractReturnTypes(retNode *tree_sitter.Node, source []byte, language lang.Language) []string {
	text := parser.NodeText(retNode, source)
	if text == "" {
		return nil
	}

	// For Go, the return type can be a parameter_list (multiple returns)
	if language == lang.Go && retNode.Kind() == "parameter_list" {
		return extractGoMultiReturnTypes(retNode, source)
	}

	// Single return type — extract the type name
	tn := cleanTypeName(text)
	if tn != "" && !isBuiltinType(tn) {
		return []string{tn}
	}
	return nil
}

// extractGoMultiReturnTypes extracts types from Go's (T1, T2, error) return.
func extractGoMultiReturnTypes(retNode *tree_sitter.Node, source []byte) []string {
	var types []string
	seen := make(map[string]bool)
	parser.Walk(retNode, func(node *tree_sitter.Node) bool {
		if node.Id() == retNode.Id() {
			return true
		}
		if node.Kind() == "parameter_declaration" {
			if typeNode := node.ChildByFieldName("type"); typeNode != nil {
				tn := cleanTypeName(parser.NodeText(typeNode, source))
				if tn != "" && !isBuiltinType(tn) && !seen[tn] {
					seen[tn] = true
					types = append(types, tn)
				}
			}
			return false
		}
		return true
	})
	return types
}

// extractBaseClasses extracts superclass names from a class definition.
func extractBaseClasses(node *tree_sitter.Node, source []byte, language lang.Language) []string {
	switch language {
	case lang.Python:
		return extractPythonBases(node, source)
	case lang.Java:
		return extractJavaBases(node, source)
	case lang.TypeScript, lang.TSX, lang.JavaScript:
		return extractTSBases(node, source)
	case lang.CPP:
		return extractCPPBases(node, source)
	case lang.Scala:
		return extractScalaBases(node, source)
	case lang.CSharp:
		return extractCSharpBases(node, source)
	case lang.PHP:
		return extractPHPBases(node, source)
	}
	return nil
}

func extractPythonBases(node *tree_sitter.Node, source []byte) []string {
	superNode := node.ChildByFieldName("superclasses")
	if superNode == nil {
		return nil
	}
	var bases []string
	for i := uint(0); i < superNode.NamedChildCount(); i++ {
		child := superNode.NamedChild(i)
		if child == nil || child.Kind() == "keyword_argument" {
			continue
		}
		if name := parser.NodeText(child, source); name != "" {
			bases = append(bases, name)
		}
	}
	return bases
}

func extractJavaBases(node *tree_sitter.Node, source []byte) []string {
	var bases []string
	if superNode := node.ChildByFieldName("superclass"); superNode != nil {
		if name := cleanTypeName(parser.NodeText(superNode, source)); name != "" {
			bases = append(bases, name)
		}
	}
	if implNode := node.ChildByFieldName("interfaces"); implNode != nil {
		for i := uint(0); i < implNode.NamedChildCount(); i++ {
			child := implNode.NamedChild(i)
			if child == nil {
				continue
			}
			if name := cleanTypeName(parser.NodeText(child, source)); name != "" {
				bases = append(bases, name)
			}
		}
	}
	return bases
}

func extractTSBases(node *tree_sitter.Node, source []byte) []string {
	var bases []string
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil || child.Kind() != "class_heritage" {
			continue
		}
		bases = append(bases, extractHeritageClauseNames(child, source)...)
	}
	return bases
}

// extractHeritageClauseNames extracts names from extends/implements clauses.
func extractHeritageClauseNames(heritage *tree_sitter.Node, source []byte) []string {
	var names []string
	for j := uint(0); j < heritage.ChildCount(); j++ {
		hChild := heritage.Child(j)
		if hChild == nil {
			continue
		}
		switch hChild.Kind() {
		case "extends_clause":
			names = append(names, extractExtendsNames(hChild, source)...)
		case "implements_clause":
			names = append(names, extractNamedChildTexts(hChild, source)...)
		}
	}
	return names
}

func extractExtendsNames(clause *tree_sitter.Node, source []byte) []string {
	if valNode := clause.ChildByFieldName("value"); valNode != nil {
		if name := parser.NodeText(valNode, source); name != "" {
			return []string{name}
		}
		return nil
	}
	// Fallback: iterate named children for identifiers
	var names []string
	for k := uint(0); k < clause.NamedChildCount(); k++ {
		ident := clause.NamedChild(k)
		if ident != nil && (ident.Kind() == "identifier" || ident.Kind() == "member_expression") {
			if name := parser.NodeText(ident, source); name != "" {
				names = append(names, name)
			}
		}
	}
	return names
}

func extractNamedChildTexts(node *tree_sitter.Node, source []byte) []string {
	var names []string
	for k := uint(0); k < node.NamedChildCount(); k++ {
		child := node.NamedChild(k)
		if child == nil {
			continue
		}
		if name := parser.NodeText(child, source); name != "" {
			names = append(names, name)
		}
	}
	return names
}

func extractCPPBases(node *tree_sitter.Node, source []byte) []string {
	var bases []string
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil || child.Kind() != "base_class_clause" {
			continue
		}
		for j := uint(0); j < child.NamedChildCount(); j++ {
			base := child.NamedChild(j)
			if base != nil && base.Kind() == "type_identifier" {
				if name := parser.NodeText(base, source); name != "" {
					bases = append(bases, name)
				}
			}
		}
	}
	return bases
}

func extractScalaBases(node *tree_sitter.Node, source []byte) []string {
	var bases []string
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil || child.Kind() != "extends_clause" {
			continue
		}
		for j := uint(0); j < child.NamedChildCount(); j++ {
			typeNode := child.NamedChild(j)
			if typeNode != nil && typeNode.Kind() == "type_identifier" {
				if name := parser.NodeText(typeNode, source); name != "" {
					bases = append(bases, name)
				}
			}
		}
	}
	return bases
}

func extractCSharpBases(node *tree_sitter.Node, source []byte) []string {
	baseList := node.ChildByFieldName("bases")
	if baseList == nil {
		return nil
	}
	var bases []string
	for i := uint(0); i < baseList.NamedChildCount(); i++ {
		child := baseList.NamedChild(i)
		if child == nil {
			continue
		}
		if name := cleanTypeName(parser.NodeText(child, source)); name != "" {
			bases = append(bases, name)
		}
	}
	return bases
}

func extractPHPBases(node *tree_sitter.Node, source []byte) []string {
	baseClause := node.ChildByFieldName("base_clause")
	if baseClause == nil {
		return nil
	}
	var bases []string
	for i := uint(0); i < baseClause.NamedChildCount(); i++ {
		child := baseClause.NamedChild(i)
		if child != nil && child.Kind() == "name" {
			if name := parser.NodeText(child, source); name != "" {
				bases = append(bases, name)
			}
		}
	}
	return bases
}

// isAbstractClass returns true if the class node has abstract modifiers.
func isAbstractClass(node *tree_sitter.Node, language lang.Language) bool {
	switch language {
	case lang.TypeScript, lang.TSX:
		return node.Kind() == "abstract_class_declaration"
	case lang.Java, lang.CSharp:
		// Check modifiers for "abstract" keyword
		mods := node.ChildByFieldName("modifiers")
		if mods == nil {
			return false
		}
		for i := uint(0); i < mods.ChildCount(); i++ {
			child := mods.Child(i)
			if child != nil && child.Kind() == "abstract" {
				return true
			}
		}
		return false
	}
	return false
}

// extractAllDecorators extracts decorators/annotations from a node across languages.
func extractAllDecorators(node *tree_sitter.Node, source []byte, language lang.Language, _ *lang.LanguageSpec) []string {
	switch language {
	case lang.Python:
		return extractDecorators(node, source)
	case lang.Java:
		return extractJavaAnnotations(node, source)
	case lang.TypeScript, lang.TSX:
		return extractTSDecorators(node, source)
	case lang.CSharp:
		return extractCSharpAttributes(node, source)
	}
	return nil
}

func extractJavaAnnotations(node *tree_sitter.Node, source []byte) []string {
	mods := node.ChildByFieldName("modifiers")
	if mods == nil {
		return nil
	}
	var decorators []string
	for i := uint(0); i < mods.ChildCount(); i++ {
		child := mods.Child(i)
		if child == nil {
			continue
		}
		if child.Kind() == "marker_annotation" || child.Kind() == "annotation" {
			decorators = append(decorators, parser.NodeText(child, source))
		}
	}
	return decorators
}

func extractTSDecorators(node *tree_sitter.Node, source []byte) []string {
	var decorators []string
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && child.Kind() == "decorator" {
			decorators = append(decorators, parser.NodeText(child, source))
		}
	}
	return decorators
}

func extractCSharpAttributes(node *tree_sitter.Node, source []byte) []string {
	var decorators []string
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil || child.Kind() != "attribute_list" {
			continue
		}
		for j := uint(0); j < child.NamedChildCount(); j++ {
			attr := child.NamedChild(j)
			if attr != nil && attr.Kind() == "attribute" {
				decorators = append(decorators, parser.NodeText(attr, source))
			}
		}
	}
	return decorators
}

// Helper functions

func extractTypeFromAnnotation(typeAnn *tree_sitter.Node, source []byte) string {
	// type_annotation → first named child is the type
	for i := uint(0); i < typeAnn.NamedChildCount(); i++ {
		child := typeAnn.NamedChild(i)
		if child != nil {
			return cleanTypeName(parser.NodeText(child, source))
		}
	}
	return ""
}

// cleanTypeName strips pointers, references, generics to get the base type name.
func cleanTypeName(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "*")
	s = strings.TrimPrefix(s, "&")
	s = strings.TrimPrefix(s, "[]")
	s = strings.TrimPrefix(s, "...")
	// Strip generic params: Map<String, Int> → Map
	if idx := strings.Index(s, "<"); idx > 0 {
		s = s[:idx]
	}
	// Strip array brackets: int[] → int
	if idx := strings.Index(s, "["); idx > 0 {
		s = s[:idx]
	}
	return strings.TrimSpace(s)
}

// isBuiltinType returns true for primitive/builtin type names that aren't
// useful to track as USES_TYPE targets.
func isBuiltinType(name string) bool {
	switch name {
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64",
		"float", "float32", "float64", "double",
		"string", "str", "bool", "boolean", "byte", "rune",
		"void", "None", "any", "interface", "object", "Object",
		"error", "uintptr", "complex64", "complex128",
		"number", "bigint", "symbol", "undefined", "null",
		"char", "short", "long", "i8", "i16", "i32", "i64",
		"u8", "u16", "u32", "u64", "f32", "f64", "usize", "isize",
		"self", "Self", "cls", "type":
		return true
	}
	return false
}

// buildSymbolSummary creates a compact symbol list for File node enrichment.
// Format: "kind:name" where kind is func/method/class/interface/type/var/const/macro/field.
func buildSymbolSummary(nodes []*store.Node, moduleQN string) []string {
	symbols := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n.QualifiedName == moduleQN {
			continue
		}
		prefix := labelToSymbolPrefix(n.Label)
		if prefix == "" {
			continue
		}
		symbols = append(symbols, prefix+":"+n.Name)
	}
	return symbols
}

func labelToSymbolPrefix(label string) string {
	switch label {
	case "Function":
		return "func"
	case "Method":
		return "method"
	case "Class":
		return "class"
	case "Interface":
		return "interface"
	case "Type":
		return "type"
	case "Enum":
		return "enum"
	case "Variable":
		return "var"
	case "Macro":
		return "macro"
	case "Field":
		return "field"
	default:
		return ""
	}
}
