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

// paramTypeExtractorFn handles parameter type extraction for a specific language.
// Returns false to stop recursion when a param node is handled.
type paramTypeExtractorFn func(node *tree_sitter.Node, source []byte, addType func(string)) bool

// extractParamTypeField extracts a type from a parameter node's "type" field.
func extractParamTypeField(kinds []string) paramTypeExtractorFn {
	kindSet := toSet(kinds)
	return func(node *tree_sitter.Node, source []byte, addType func(string)) bool {
		if !kindSet[node.Kind()] {
			return true
		}
		if typeNode := node.ChildByFieldName("type"); typeNode != nil {
			addType(cleanTypeName(parser.NodeText(typeNode, source)))
		}
		return false
	}
}

func extractParamTypeTS(node *tree_sitter.Node, source []byte, addType func(string)) bool {
	if node.Kind() == "required_parameter" || node.Kind() == "optional_parameter" {
		if typeAnn := findChildByKind(node, "type_annotation"); typeAnn != nil {
			addType(extractTypeFromAnnotation(typeAnn, source))
		}
		return false
	}
	return true
}

func extractParamTypeScala(node *tree_sitter.Node, source []byte, addType func(string)) bool {
	if node.Kind() != "parameter" {
		return true
	}
	for i := uint(0); i < node.NamedChildCount(); i++ {
		child := node.NamedChild(i)
		if child != nil && child.Kind() == "type_identifier" {
			addType(cleanTypeName(parser.NodeText(child, source)))
		}
	}
	return false
}

func extractParamTypeKotlin(node *tree_sitter.Node, source []byte, addType func(string)) bool {
	if node.Kind() != "parameter" {
		return true
	}
	if typeNode := node.ChildByFieldName("type"); typeNode != nil {
		addType(cleanTypeName(parser.NodeText(typeNode, source)))
	} else {
		for i := uint(0); i < node.NamedChildCount(); i++ {
			child := node.NamedChild(i)
			if child != nil && child.Kind() == "user_type" {
				addType(cleanTypeName(parser.NodeText(child, source)))
			}
		}
	}
	return false
}

func extractParamTypeDart(node *tree_sitter.Node, source []byte, addType func(string)) bool {
	if node.Kind() != "formal_parameter" {
		return true
	}
	if typeNode := findChildByKind(node, "type_identifier"); typeNode != nil {
		addType(cleanTypeName(parser.NodeText(typeNode, source)))
	}
	return false
}

func extractParamTypeGroovy(node *tree_sitter.Node, source []byte, addType func(string)) bool {
	if node.Kind() != "parameter" {
		return true
	}
	if node.ChildByFieldName("name") != nil {
		if typeNode := node.ChildByFieldName("type"); typeNode != nil {
			addType(cleanTypeName(parser.NodeText(typeNode, source)))
		}
	}
	return false
}

// noParamTypes is used for languages without parameter type annotations.
func noParamTypes(_ *tree_sitter.Node, _ []byte, _ func(string)) bool {
	return false
}

// paramTypeExtractors maps each language to its parameter type extractor.
var paramTypeExtractors = map[lang.Language]paramTypeExtractorFn{
	lang.Go:         extractParamTypeField([]string{"parameter_declaration"}),
	lang.Python:     extractParamTypeField([]string{"typed_parameter"}),
	lang.TypeScript: extractParamTypeTS,
	lang.TSX:        extractParamTypeTS,
	lang.Java:       extractParamTypeField([]string{"formal_parameter", "spread_parameter"}),
	lang.Rust:       extractParamTypeField([]string{"parameter"}),
	lang.CPP:        extractParamTypeField([]string{"parameter_declaration"}),
	lang.C:          extractParamTypeField([]string{"parameter_declaration"}),
	lang.ObjectiveC: extractParamTypeField([]string{"parameter_declaration"}),
	lang.CSharp:     extractParamTypeField([]string{"parameter"}),
	lang.PHP:        extractParamTypeField([]string{"simple_parameter", "variadic_parameter"}),
	lang.Scala:      extractParamTypeScala,
	lang.Zig:        extractParamTypeField([]string{"parameter"}),
	lang.Kotlin:     extractParamTypeKotlin,
	lang.Dart:       extractParamTypeDart,
	lang.Groovy:     extractParamTypeGroovy,
	lang.Swift:      extractParamTypeField([]string{"parameter"}),
	lang.OCaml:      extractParamTypeField([]string{"parameter"}),
	lang.Haskell:    noParamTypes,
	lang.Ruby:       noParamTypes,
	lang.Perl:       noParamTypes,
	lang.Erlang:     noParamTypes,
	lang.R:          noParamTypes,
	lang.Elixir:     noParamTypes,
}

// extractParamType handles a single parameter node per language.
// Returns false to stop recursion when a param node is handled.
func extractParamType(node *tree_sitter.Node, source []byte, language lang.Language, addType func(string)) bool {
	if fn, ok := paramTypeExtractors[language]; ok {
		return fn(node, source, addType)
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
	case lang.Kotlin:
		return extractKotlinBases(node, source)
	case lang.Ruby:
		return extractRubyBases(node, source)
	case lang.ObjectiveC:
		return extractObjCBases(node, source)
	case lang.Groovy:
		return extractGroovyBases(node, source)
	case lang.Dart:
		return extractDartBases(node, source)
	case lang.Swift:
		return extractSwiftBases(node, source)
	case lang.Rust:
		// Rust handles inheritance via impl blocks, not class definitions
		return nil
	case lang.Erlang:
		// Erlang has no class inheritance
		return nil
	}
	return nil
}

func extractRubyBases(node *tree_sitter.Node, source []byte) []string {
	// Ruby: class Child < Parent — superclass field contains "< Parent"
	superNode := node.ChildByFieldName("superclass")
	if superNode == nil {
		return nil
	}
	// The superclass node contains the "<" token and a constant child
	for i := uint(0); i < superNode.NamedChildCount(); i++ {
		child := superNode.NamedChild(i)
		if child != nil && child.Kind() == "constant" {
			name := parser.NodeText(child, source)
			if name != "" {
				return []string{name}
			}
		}
	}
	return nil
}

func extractGroovyBases(node *tree_sitter.Node, source []byte) []string {
	// Groovy: class Foo extends Bar → superclass field is an identifier
	if superNode := node.ChildByFieldName("superclass"); superNode != nil {
		if name := parser.NodeText(superNode, source); name != "" {
			return []string{name}
		}
	}
	return nil
}

func extractDartBases(node *tree_sitter.Node, source []byte) []string {
	// Dart: class Dog extends Animal → superclass field contains "extends Animal"
	var bases []string
	if superNode := node.ChildByFieldName("superclass"); superNode != nil {
		// Inside superclass, find type_identifier children
		for i := uint(0); i < superNode.NamedChildCount(); i++ {
			child := superNode.NamedChild(i)
			if child != nil && child.Kind() == "type_identifier" {
				if name := parser.NodeText(child, source); name != "" {
					bases = append(bases, name)
				}
			}
		}
	}
	// Dart: interfaces field for implements
	if ifaceNode := node.ChildByFieldName("interfaces"); ifaceNode != nil {
		for i := uint(0); i < ifaceNode.NamedChildCount(); i++ {
			child := ifaceNode.NamedChild(i)
			if child != nil {
				if name := cleanTypeName(parser.NodeText(child, source)); name != "" {
					bases = append(bases, name)
				}
			}
		}
	}
	return bases
}

func extractSwiftBases(node *tree_sitter.Node, source []byte) []string {
	// Swift: class Dog: Animal → inheritance_specifier children contain user_type → type_identifier
	var bases []string
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil {
			continue
		}
		if child.Kind() == "inheritance_specifier" {
			// Find type_identifier inside user_type
			parser.Walk(child, func(n *tree_sitter.Node) bool {
				if n.Kind() == "type_identifier" {
					if name := parser.NodeText(n, source); name != "" {
						bases = append(bases, name)
					}
					return false
				}
				return true
			})
		}
	}
	return bases
}

func extractObjCBases(node *tree_sitter.Node, source []byte) []string {
	// ObjC: @interface Child : Parent — superclass field is an identifier
	if superNode := node.ChildByFieldName("superclass"); superNode != nil {
		if name := parser.NodeText(superNode, source); name != "" {
			return []string{name}
		}
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
		// Navigate to the type_identifier child (raw text includes "extends" keyword)
		if typeID := findChildByKind(superNode, "type_identifier"); typeID != nil {
			if name := parser.NodeText(typeID, source); name != "" {
				bases = append(bases, name)
			}
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
		case "identifier", "member_expression":
			// JS class_heritage has bare identifiers (no extends_clause wrapper)
			if name := parser.NodeText(hChild, source); name != "" {
				names = append(names, name)
			}
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
	// C# base_list has no field name — find it by kind
	baseList := node.ChildByFieldName("bases")
	if baseList == nil {
		baseList = findChildByKind(node, "base_list")
	}
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
	// PHP base_clause has no field name — find it by kind
	baseClause := node.ChildByFieldName("base_clause")
	if baseClause == nil {
		baseClause = findChildByKind(node, "base_clause")
	}
	if baseClause == nil {
		return nil
	}
	var bases []string
	for i := uint(0); i < baseClause.NamedChildCount(); i++ {
		child := baseClause.NamedChild(i)
		if child != nil && (child.Kind() == "name" || child.Kind() == "qualified_name") {
			if name := parser.NodeText(child, source); name != "" {
				bases = append(bases, name)
			}
		}
	}
	return bases
}

func extractKotlinBases(node *tree_sitter.Node, source []byte) []string {
	var bases []string
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil {
			continue
		}
		if child.Kind() == "delegation_specifier_list" || child.Kind() == "delegation_specifiers" {
			for j := uint(0); j < child.NamedChildCount(); j++ {
				spec := child.NamedChild(j)
				if spec == nil {
					continue
				}
				// delegation_specifier → constructor_invocation or user_type
				text := parser.NodeText(spec, source)
				// Strip constructor args: "Parent()" → "Parent"
				if idx := strings.Index(text, "("); idx > 0 {
					text = text[:idx]
				}
				text = strings.TrimSpace(text)
				if text != "" {
					bases = append(bases, text)
				}
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
	case lang.Kotlin:
		return extractKotlinAnnotations(node, source)
	case lang.PHP:
		return extractPHPAttributes(node, source)
	case lang.Rust:
		return extractRustAttributes(node, source)
	case lang.Swift:
		return extractSwiftAttributes(node, source)
	case lang.Groovy:
		return extractGroovyAnnotations(node, source)
	case lang.Dart:
		return extractDartAnnotations(node, source)
	}
	return nil
}

func extractJavaAnnotations(node *tree_sitter.Node, source []byte) []string {
	mods := node.ChildByFieldName("modifiers")
	if mods == nil {
		mods = findChildByKind(node, "modifiers")
	}
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

// extractGroovyAnnotations extracts annotations from Groovy nodes.
// Groovy annotations are direct children of the function/class definition node.
func extractGroovyAnnotations(node *tree_sitter.Node, source []byte) []string {
	var decorators []string
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && child.Kind() == "annotation" {
			decorators = append(decorators, parser.NodeText(child, source))
		}
	}
	return decorators
}

func extractTSDecorators(node *tree_sitter.Node, source []byte) []string {
	var decorators []string
	// Check direct children
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && child.Kind() == "decorator" {
			decorators = append(decorators, parser.NodeText(child, source))
		}
	}
	// TS/TSX: decorators on class methods are siblings in class_body, not children
	// of method_definition. Check preceding siblings.
	if len(decorators) == 0 {
		decorators = extractTSPrecedingSiblingDecorators(node, source)
	}
	return decorators
}

// extractTSPrecedingSiblingDecorators collects decorator nodes that precede the
// given node in a class_body parent.
func extractTSPrecedingSiblingDecorators(node *tree_sitter.Node, source []byte) []string {
	parent := node.Parent()
	if parent == nil || parent.Kind() != "class_body" {
		return nil
	}
	nodeIdx := findNodeIndex(parent, node)
	if nodeIdx < 0 {
		return nil
	}
	return collectPrecedingDecorators(parent, nodeIdx, source)
}

// findNodeIndex returns the child index of node within parent, or -1 if not found.
func findNodeIndex(parent, node *tree_sitter.Node) int {
	for i := uint(0); i < parent.ChildCount(); i++ {
		child := parent.Child(i)
		if child != nil && child.Id() == node.Id() {
			return int(i)
		}
	}
	return -1
}

// collectPrecedingDecorators collects decorator nodes from siblings preceding nodeIdx.
func collectPrecedingDecorators(parent *tree_sitter.Node, nodeIdx int, source []byte) []string {
	var decorators []string
	for j := nodeIdx - 1; j >= 0; j-- {
		prev := parent.Child(uint(j))
		if prev == nil || prev.Kind() != "decorator" {
			break
		}
		decorators = append(decorators, parser.NodeText(prev, source))
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

func extractKotlinAnnotations(node *tree_sitter.Node, source []byte) []string {
	var decorators []string
	// Kotlin: modifiers → annotation nodes
	mods := node.ChildByFieldName("modifiers")
	if mods == nil {
		mods = findChildByKind(node, "modifiers")
	}
	if mods == nil {
		return nil
	}
	for i := uint(0); i < mods.ChildCount(); i++ {
		child := mods.Child(i)
		if child == nil {
			continue
		}
		if child.Kind() == "annotation" {
			decorators = append(decorators, parser.NodeText(child, source))
		}
	}
	return decorators
}

func extractPHPAttributes(node *tree_sitter.Node, source []byte) []string {
	var decorators []string
	// PHP 8 attributes: attribute_list → attribute_group → attribute
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil {
			continue
		}
		switch child.Kind() {
		case "attribute_list":
			decorators = append(decorators, extractPHPAttributeList(child, source)...)
		case "attribute_group":
			decorators = append(decorators, extractPHPAttributeGroup(child, source)...)
		}
	}
	return decorators
}

// extractPHPAttributeList extracts attributes from an attribute_list node.
func extractPHPAttributeList(list *tree_sitter.Node, source []byte) []string {
	var decorators []string
	for j := uint(0); j < list.NamedChildCount(); j++ {
		group := list.NamedChild(j)
		if group == nil {
			continue
		}
		switch group.Kind() {
		case "attribute_group":
			decorators = append(decorators, extractPHPAttributeGroup(group, source)...)
		case "attribute":
			decorators = append(decorators, parser.NodeText(group, source))
		}
	}
	return decorators
}

// extractPHPAttributeGroup extracts attribute nodes from an attribute_group.
func extractPHPAttributeGroup(group *tree_sitter.Node, source []byte) []string {
	var decorators []string
	for k := uint(0); k < group.NamedChildCount(); k++ {
		attr := group.NamedChild(k)
		if attr != nil && attr.Kind() == "attribute" {
			decorators = append(decorators, parser.NodeText(attr, source))
		}
	}
	return decorators
}

// extractRustAttributes extracts #[attr] attributes from preceding siblings.
// In Rust, attribute_item nodes are siblings of the annotated item, not children.
func extractRustAttributes(node *tree_sitter.Node, source []byte) []string {
	parent := node.Parent()
	if parent == nil {
		return nil
	}

	// Find our node's index among siblings, then collect preceding attribute_item nodes.
	nodeIdx := findNodeIndex(parent, node)
	if nodeIdx < 0 {
		return nil
	}

	var decorators []string
	for j := nodeIdx - 1; j >= 0; j-- {
		prev := parent.Child(uint(j))
		if prev == nil {
			continue
		}
		if prev.Kind() != "attribute_item" {
			break // stop at first non-attribute sibling
		}
		// Extract the attribute content (e.g., get("/users") from #[get("/users")])
		if attr := findChildByKind(prev, "attribute"); attr != nil {
			decorators = append(decorators, parser.NodeText(attr, source))
		} else {
			decorators = append(decorators, parser.NodeText(prev, source))
		}
	}

	// Also check inner attributes (#![...]) which are children of the node itself
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && child.Kind() == "attribute_item" {
			if attr := findChildByKind(child, "attribute"); attr != nil {
				decorators = append(decorators, parser.NodeText(attr, source))
			}
		}
	}

	return decorators
}

func extractSwiftAttributes(node *tree_sitter.Node, source []byte) []string {
	var decorators []string
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && child.Kind() == "attribute" {
			decorators = append(decorators, parser.NodeText(child, source))
		}
	}
	return decorators
}

func extractDartAnnotations(node *tree_sitter.Node, source []byte) []string {
	var decorators []string
	// Dart annotations are siblings of the annotated node, not children
	parent := node.Parent()
	if parent == nil {
		return nil
	}
	for i := uint(0); i < parent.ChildCount(); i++ {
		child := parent.Child(i)
		if child != nil && child.Id() == node.Id() {
			// Collect annotations from preceding siblings
			for j := int(i) - 1; j >= 0; j-- {
				prev := parent.Child(uint(j))
				if prev == nil {
					break
				}
				if prev.Kind() == "annotation" {
					decorators = append(decorators, parser.NodeText(prev, source))
				} else {
					break
				}
			}
			break
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
	s = strings.TrimPrefix(s, ": ") // TS/TSX type_annotation includes ": "
	s = strings.TrimPrefix(s, ":")
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

// tokenizeDecorator strips decorator syntax and splits into lowercase words.
// Example: "@login_required" → ["login", "required"]
// Example: "@GetMapping(\"/api\")" → ["mapping"] (stopword "get" filtered)
func tokenizeDecorator(dec string) []string {
	// Strip leading syntax: @, #[
	dec = strings.TrimPrefix(dec, "@")
	dec = strings.TrimPrefix(dec, "#[")
	dec = strings.TrimSuffix(dec, "]")
	// Strip arguments: everything from first ( onwards
	if idx := strings.Index(dec, "("); idx >= 0 {
		dec = dec[:idx]
	}
	// Split on delimiters: dots, underscores, hyphens, colons, slashes
	parts := strings.FieldsFunc(dec, func(r rune) bool {
		return r == '.' || r == '_' || r == '-' || r == ':' || r == '/'
	})
	// Split camelCase and collect lowercase words
	var words []string
	for _, part := range parts {
		for _, w := range splitCamelCase(part) {
			w = strings.ToLower(w)
			if len(w) >= 2 && !decoratorStopwords[w] {
				words = append(words, w)
			}
		}
	}
	return words
}

// splitCamelCase splits a string on lowercase→uppercase transitions.
// Example: "GetMapping" → ["Get", "Mapping"]
func splitCamelCase(s string) []string {
	if s == "" {
		return nil
	}
	var words []string
	start := 0
	for i := 1; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' && s[i-1] >= 'a' && s[i-1] <= 'z' {
			words = append(words, s[start:i])
			start = i
		}
	}
	words = append(words, s[start:])
	return words
}

// decoratorStopwords are common words filtered from decorator tag candidates.
var decoratorStopwords = map[string]bool{
	"get": true, "set": true, "new": true, "class": true,
	"method": true, "function": true, "value": true, "type": true,
	"param": true, "return": true, "public": true, "private": true,
	"for": true, "if": true, "the": true, "and": true,
	"or": true, "not": true, "with": true, "from": true,
	"app": true, "router": true,
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
