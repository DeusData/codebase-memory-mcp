package pipeline

import (
	"strings"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
)

// resolveModuleStrings performs in-memory constant propagation on module-level
// string assignments. It walks the AST top-to-bottom, collects simple string
// literals, then resolves interpolated and concatenated strings using the
// collected symbol table. Returns a map of variable name → resolved string.
//
// Supports: Python f-strings, JS/TS template literals, PHP encapsed strings,
// Scala string interpolation, Rust format!, Go fmt.Sprintf, and string
// concatenation (+ or .) in all languages.
//
// Source files are never modified — resolution is purely in RAM.
func resolveModuleStrings(root *tree_sitter.Node, source []byte, language lang.Language) map[string]string {
	symbols := make(map[string]string)

	// Walk only top-level children (module-level declarations)
	for i := uint(0); i < root.ChildCount(); i++ {
		child := root.Child(i)
		if child == nil {
			continue
		}
		name, value := resolveAssignment(child, source, language, symbols)
		if name != "" && value != "" {
			symbols[name] = value
		}
	}

	return symbols
}

// resolveAssignment tries to extract a (name, resolved_value) pair from
// a top-level AST node. Returns ("","") if the node isn't a string assignment.
func resolveAssignment(node *tree_sitter.Node, source []byte, language lang.Language, symbols map[string]string) (name, value string) {
	switch language {
	case lang.Python:
		return resolvePython(node, source, symbols)
	case lang.Go:
		return resolveGo(node, source, symbols)
	case lang.JavaScript, lang.TypeScript, lang.TSX:
		return resolveJS(node, source, symbols)
	case lang.Rust:
		return resolveRust(node, source, symbols)
	case lang.Java:
		return resolveJava(node, source, symbols)
	case lang.PHP:
		return resolvePHP(node, source, symbols)
	case lang.Scala:
		return resolveScala(node, source, symbols)
	case lang.Kotlin:
		return resolveKotlin(node, source, symbols)
	case lang.CPP:
		return resolveCPP(node, source, symbols)
	case lang.Lua:
		return resolveLua(node, source, symbols)
	default:
		return "", ""
	}
}

// --- Python ---
// expression_statement → assignment → (identifier, string|binary_operator)

func resolvePython(node *tree_sitter.Node, source []byte, symbols map[string]string) (name, value string) {
	if node.Kind() != "expression_statement" {
		return "", ""
	}
	assign := findChildByKind(node, "assignment")
	if assign == nil {
		return "", ""
	}
	nameNode := assign.ChildByFieldName("left")
	valueNode := assign.ChildByFieldName("right")
	if nameNode == nil || valueNode == nil || nameNode.Kind() != "identifier" {
		return "", ""
	}
	name = parser.NodeText(nameNode, source)
	value = resolveStringExpr(valueNode, source, symbols)
	return name, value
}

// --- Go ---
// const_declaration → const_spec → (identifier, expression_list)
// var_declaration → var_spec → (identifier, expression_list)

func resolveGo(node *tree_sitter.Node, source []byte, symbols map[string]string) (name, value string) {
	var spec *tree_sitter.Node
	switch node.Kind() {
	case "const_declaration":
		spec = findChildByKind(node, "const_spec")
	case "var_declaration":
		spec = findChildByKind(node, "var_spec")
	default:
		return "", ""
	}
	if spec == nil {
		return "", ""
	}
	nameNode := spec.ChildByFieldName("name")
	valueNode := spec.ChildByFieldName("value")
	if nameNode == nil || valueNode == nil {
		return "", ""
	}
	// value is an expression_list; take the first child
	if valueNode.Kind() == "expression_list" && valueNode.ChildCount() > 0 {
		valueNode = firstNonTrivialChild(valueNode)
		if valueNode == nil {
			return "", ""
		}
	}
	name = parser.NodeText(nameNode, source)
	value = resolveStringExpr(valueNode, source, symbols)
	return name, value
}

// --- JavaScript / TypeScript / TSX ---
// lexical_declaration → variable_declarator → (identifier, value)

func resolveJS(node *tree_sitter.Node, source []byte, symbols map[string]string) (name, value string) {
	if node.Kind() != "lexical_declaration" {
		return "", ""
	}
	decl := findChildByKind(node, "variable_declarator")
	if decl == nil {
		return "", ""
	}
	nameNode := decl.ChildByFieldName("name")
	valueNode := decl.ChildByFieldName("value")
	if nameNode == nil || valueNode == nil {
		return "", ""
	}
	name = parser.NodeText(nameNode, source)
	value = resolveStringExpr(valueNode, source, symbols)
	return name, value
}

// --- Rust ---
// const_item → (identifier, string_literal|binary_expression|macro_invocation)
// let_declaration → (identifier, value)

func resolveRust(node *tree_sitter.Node, source []byte, symbols map[string]string) (name, value string) {
	switch node.Kind() {
	case "const_item", "let_declaration":
		// both use field "name" for identifier and "value" for the expression
	default:
		return "", ""
	}
	nameNode := node.ChildByFieldName("name")
	if nameNode == nil {
		// let_declaration uses "pattern" field
		nameNode = node.ChildByFieldName("pattern")
	}
	valueNode := node.ChildByFieldName("value")
	if nameNode == nil || valueNode == nil {
		return "", ""
	}
	name = parser.NodeText(nameNode, source)
	value = resolveStringExpr(valueNode, source, symbols)
	return name, value
}

// --- Java ---
// class_declaration → class_body → field_declaration → variable_declarator
// We need to look inside class bodies for static final fields.

func resolveJava(node *tree_sitter.Node, source []byte, symbols map[string]string) (name, value string) {
	if node.Kind() != "class_declaration" {
		return "", ""
	}
	body := node.ChildByFieldName("body")
	if body == nil {
		return "", ""
	}
	// Walk field_declarations inside the class body
	for i := uint(0); i < body.ChildCount(); i++ {
		child := body.Child(i)
		if child == nil || child.Kind() != "field_declaration" {
			continue
		}
		decl := child.ChildByFieldName("declarator")
		if decl == nil || decl.Kind() != "variable_declarator" {
			continue
		}
		nameNode := decl.ChildByFieldName("name")
		valueNode := decl.ChildByFieldName("value")
		if nameNode == nil || valueNode == nil {
			continue
		}
		name = parser.NodeText(nameNode, source)
		value = resolveStringExpr(valueNode, source, symbols)
		if name != "" && value != "" {
			symbols[name] = value
		}
	}
	return "", "" // all collected via symbols map directly
}

// --- PHP ---
// expression_statement → assignment_expression → (variable_name, encapsed_string|binary_expression)

func resolvePHP(node *tree_sitter.Node, source []byte, symbols map[string]string) (name, value string) {
	if node.Kind() != "expression_statement" {
		return "", ""
	}
	assign := findChildByKind(node, "assignment_expression")
	if assign == nil {
		return "", ""
	}
	nameNode := assign.ChildByFieldName("left")
	valueNode := assign.ChildByFieldName("right")
	if nameNode == nil || valueNode == nil {
		return "", ""
	}
	// PHP variable names include $, extract just the name part
	name = extractPHPVarName(nameNode, source)
	if name == "" {
		return "", ""
	}
	value = resolveStringExpr(valueNode, source, symbols)
	return name, value
}

func extractPHPVarName(node *tree_sitter.Node, source []byte) string {
	if node.Kind() != "variable_name" {
		return ""
	}
	// variable_name has children: $ and name
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && child.Kind() == "name" {
			return parser.NodeText(child, source)
		}
	}
	return ""
}

// --- Scala ---
// val_definition → (identifier, string|interpolated_string_expression|infix_expression)

func resolveScala(node *tree_sitter.Node, source []byte, symbols map[string]string) (name, value string) {
	if node.Kind() != "val_definition" {
		return "", ""
	}
	nameNode := node.ChildByFieldName("pattern")
	valueNode := node.ChildByFieldName("value")
	if nameNode == nil || valueNode == nil {
		return "", ""
	}
	name = parser.NodeText(nameNode, source)
	value = resolveStringExpr(valueNode, source, symbols)
	return name, value
}

// --- Kotlin ---
// property_declaration → variable_declaration (contains identifier) + expression (the value)

func resolveKotlin(node *tree_sitter.Node, source []byte, symbols map[string]string) (name, value string) {
	if node.Kind() != "property_declaration" {
		return "", ""
	}
	// Find the variable_declaration child (contains the identifier name)
	varDecl := findChildByKind(node, "variable_declaration")
	if varDecl == nil {
		return "", ""
	}
	// The identifier is the first named child of variable_declaration
	nameNode := findChildByKind(varDecl, "identifier")
	if nameNode == nil {
		return "", ""
	}
	// The value expression is a direct child of property_declaration
	// It comes after the "=" token
	var valueNode *tree_sitter.Node
	foundEquals := false
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil {
			continue
		}
		if parser.NodeText(child, source) == "=" {
			foundEquals = true
			continue
		}
		if foundEquals && child.IsNamed() {
			valueNode = child
			break
		}
	}
	if valueNode == nil {
		return "", ""
	}
	name = parser.NodeText(nameNode, source)
	value = resolveStringExpr(valueNode, source, symbols)
	return name, value
}

// --- C++ ---
// preproc_def → identifier + preproc_arg (for #define)
// declaration → init_declarator → identifier + value (for const std::string x = "...")

func resolveCPP(node *tree_sitter.Node, source []byte, symbols map[string]string) (name, value string) {
	switch node.Kind() {
	case "preproc_def":
		// #define NAME "value"
		nameNode := findChildByKind(node, "identifier")
		valueNode := findChildByKind(node, "preproc_arg")
		if nameNode == nil || valueNode == nil {
			return "", ""
		}
		name = parser.NodeText(nameNode, source)
		// preproc_arg contains the raw text — try to extract string content
		argText := strings.TrimSpace(parser.NodeText(valueNode, source))
		if len(argText) >= 2 && argText[0] == '"' && argText[len(argText)-1] == '"' {
			return name, argText[1 : len(argText)-1]
		}
		// Could be a reference to another #define
		if val, ok := symbols[argText]; ok {
			return name, val
		}
		return "", ""

	case "declaration":
		// const std::string x = "value" or std::string x = base + "/path"
		// AST: declaration → init_declarator → declarator(identifier) + value
		initDecl := findChildByKind(node, "init_declarator")
		if initDecl == nil {
			return "", ""
		}
		nameNode := initDecl.ChildByFieldName("declarator")
		valueNode := initDecl.ChildByFieldName("value")
		if nameNode == nil || valueNode == nil {
			return "", ""
		}
		name = parser.NodeText(nameNode, source)
		value = resolveStringExpr(valueNode, source, symbols)
		return name, value
	}
	return "", ""
}

// --- Lua ---
// variable_declaration → assignment_statement → variable_list + expression_list
// Local: local x = "value" → variable_declaration containing assignment_statement

func resolveLua(node *tree_sitter.Node, source []byte, symbols map[string]string) (name, value string) {
	if node.Kind() != "variable_declaration" {
		return "", ""
	}
	assign := findChildByKind(node, "assignment_statement")
	if assign == nil {
		return "", ""
	}
	varList := findChildByKind(assign, "variable_list")
	exprList := findChildByKind(assign, "expression_list")
	if varList == nil || exprList == nil {
		return "", ""
	}

	// Take the first variable name and first expression value
	nameNode := firstNonTrivialChild(varList)
	valueNode := firstNonTrivialChild(exprList)
	if nameNode == nil || valueNode == nil {
		return "", ""
	}

	name = parser.NodeText(nameNode, source)
	value = resolveStringExpr(valueNode, source, symbols)
	return name, value
}

// --- Universal expression resolver ---

// resolveStringExpr resolves a string expression node to its string value.
// Handles: literal strings, interpolated strings, concatenation, fmt.Sprintf, format!.
func resolveStringExpr(node *tree_sitter.Node, source []byte, symbols map[string]string) string {
	if node == nil {
		return ""
	}
	kind := node.Kind()

	// Simple string literals
	if isStringLiteral(kind) {
		return extractStringContent(node, source)
	}

	// Identifiers: look up in symbol table
	if kind == "identifier" {
		return symbols[parser.NodeText(node, source)]
	}

	// PHP variable names ($varName): look up by name part (without $)
	if kind == "variable_name" {
		name := extractPHPVarName(node, source)
		return symbols[name]
	}

	// Python f-strings: string with string_start = f" or f'
	if kind == "string" {
		start := findChildByKind(node, "string_start")
		if start != nil {
			startText := parser.NodeText(start, source)
			if strings.HasPrefix(startText, "f") || strings.HasPrefix(startText, "F") {
				return resolveInterpolatedChildren(node, source, symbols, "interpolation", "string_content")
			}
		}
		// Plain string
		return extractStringContent(node, source)
	}

	// JS/TS template strings
	if kind == "template_string" {
		return resolveInterpolatedChildren(node, source, symbols, "template_substitution", "string_fragment")
	}

	// PHP encapsed strings (interpolated)
	if kind == "encapsed_string" {
		return resolvePHPEncapsed(node, source, symbols)
	}

	// Scala interpolated strings
	if kind == "interpolated_string_expression" {
		interpStr := findChildByKind(node, "interpolated_string")
		if interpStr != nil {
			return resolveScalaInterpolated(interpStr, source, symbols)
		}
		return ""
	}

	// String concatenation: binary_expression/binary_operator/infix_expression with +/.
	if kind == "binary_expression" || kind == "binary_operator" || kind == "infix_expression" {
		return resolveBinaryConcat(node, source, symbols)
	}

	// Go fmt.Sprintf / Java String.format / Lua string.format / Python calls / Rust format! macro
	if kind == "call_expression" || kind == "function_call" || kind == "call" {
		return resolveCallExpr(node, source, symbols)
	}
	if kind == "macro_invocation" {
		return resolveRustFormatMacro(node, source, symbols)
	}

	return ""
}

// resolveInterpolatedChildren resolves a node whose children alternate between
// interpolation nodes (containing variable refs) and literal content nodes.
// Used for Python f-strings and JS/TS template strings.
func resolveInterpolatedChildren(node *tree_sitter.Node, source []byte, symbols map[string]string, interpKind, contentKind string) string {
	var b strings.Builder
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil {
			continue
		}
		switch child.Kind() {
		case interpKind:
			// Find the identifier inside the interpolation
			ident := findDescendantByKind(child, "identifier")
			if ident != nil {
				name := parser.NodeText(ident, source)
				if val, ok := symbols[name]; ok {
					b.WriteString(val)
				} else {
					// Unresolvable — emit placeholder
					b.WriteString("{}")
				}
			}
		case contentKind:
			b.WriteString(parser.NodeText(child, source))
		}
	}
	return b.String()
}

// resolvePHPEncapsed resolves PHP interpolated strings.
// Children: " { variable_name } string_content "
func resolvePHPEncapsed(node *tree_sitter.Node, source []byte, symbols map[string]string) string {
	var b strings.Builder
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil {
			continue
		}
		switch child.Kind() {
		case "variable_name":
			name := extractPHPVarName(child, source)
			if val, ok := symbols[name]; ok {
				b.WriteString(val)
			} else {
				b.WriteString("{}")
			}
		case "string_content":
			b.WriteString(parser.NodeText(child, source))
		}
	}
	return b.String()
}

// resolveScalaInterpolated resolves Scala s"..." interpolated strings.
// The Scala tree-sitter grammar does NOT create child nodes for literal text
// between interpolations — we must extract them from byte gaps between children.
func resolveScalaInterpolated(node *tree_sitter.Node, source []byte, symbols map[string]string) string {
	var b strings.Builder
	nodeStart := node.StartByte()
	nodeEnd := node.EndByte()

	// Skip opening quote
	cursor := nodeStart + 1 // skip "

	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil {
			continue
		}
		kind := child.Kind()

		// Skip the quote delimiters themselves
		if kind == "\"" || parser.NodeText(child, source) == "\"" {
			cursor = child.EndByte()
			continue
		}

		// Emit any literal text between cursor and this child
		if child.StartByte() > cursor {
			b.Write(source[cursor:child.StartByte()])
		}

		if kind == "interpolation" {
			ident := findDescendantByKind(child, "identifier")
			if ident != nil {
				name := parser.NodeText(ident, source)
				if val, ok := symbols[name]; ok {
					b.WriteString(val)
				} else {
					b.WriteString("{}")
				}
			}
		}
		cursor = child.EndByte()
	}

	// Emit trailing literal text before closing quote
	if cursor < nodeEnd-1 { // -1 to skip closing "
		b.Write(source[cursor : nodeEnd-1])
	}

	return b.String()
}

// resolveBinaryConcat resolves string concatenation: left + right or left . right (PHP).
// Also handles || and ?? (nullish coalescing / logical OR default patterns).
func resolveBinaryConcat(node *tree_sitter.Node, source []byte, symbols map[string]string) string {
	opNode := node.ChildByFieldName("operator")
	if opNode == nil {
		return ""
	}
	op := parser.NodeText(opNode, source)

	switch op {
	case "+", ".", "..":
		left := resolveStringExpr(node.ChildByFieldName("left"), source, symbols)
		right := resolveStringExpr(node.ChildByFieldName("right"), source, symbols)
		if left == "" && right == "" {
			return ""
		}
		return left + right
	case "||", "??":
		// For || and ??, return the right operand (default value) when left can't be resolved.
		// This handles: process.env.KEY || "https://fallback"
		//               envVar ?? "https://default"
		left := resolveStringExpr(node.ChildByFieldName("left"), source, symbols)
		if left != "" {
			return left
		}
		return resolveStringExpr(node.ChildByFieldName("right"), source, symbols)
	default:
		return ""
	}
}

// resolveCallExpr resolves format-style calls: Go fmt.Sprintf, Lua string.format, Java String.format.
func resolveCallExpr(node *tree_sitter.Node, source []byte, symbols map[string]string) string {
	// Try "function" field (Go, Java) or "name" field (Lua function_call)
	funcNode := node.ChildByFieldName("function")
	if funcNode == nil {
		funcNode = node.ChildByFieldName("name")
	}
	if funcNode == nil {
		return ""
	}
	funcName := parser.NodeText(funcNode, source)

	// Try "arguments" field (Go, Java, Lua)
	args := node.ChildByFieldName("arguments")
	if args == nil {
		return ""
	}

	// Collect non-punctuation children as arguments
	var argNodes []*tree_sitter.Node
	for i := uint(0); i < args.ChildCount(); i++ {
		child := args.Child(i)
		if child == nil {
			continue
		}
		kind := child.Kind()
		if kind == "(" || kind == ")" || kind == "," {
			continue
		}
		argNodes = append(argNodes, child)
	}
	if len(argNodes) == 0 {
		return ""
	}

	switch funcName {
	case "fmt.Sprintf", "String.format", "string.format":
		// continue — supported format functions
	default:
		// For unknown functions with URL-like string arguments,
		// extract the URL as a fallback. Handles getEnv(), os.environ.get(), etc.
		return extractURLArgFallback(argNodes, source, symbols)
	}

	// First arg is the format string
	fmtStr := extractStringContent(argNodes[0], source)
	if fmtStr == "" {
		return ""
	}

	// Substitute %s, %v, %d with resolved argument values
	argIdx := 1
	var b strings.Builder
	for j := 0; j < len(fmtStr); j++ {
		if j+1 < len(fmtStr) && fmtStr[j] == '%' && (fmtStr[j+1] == 's' || fmtStr[j+1] == 'v' || fmtStr[j+1] == 'd') {
			if argIdx < len(argNodes) {
				val := resolveStringExpr(argNodes[argIdx], source, symbols)
				b.WriteString(val)
				argIdx++
			} else {
				b.WriteString("{}")
			}
			j++ // skip the format specifier char
		} else {
			b.WriteByte(fmtStr[j])
		}
	}
	return b.String()
}

// extractURLArgFallback scans function arguments for URL-like string literals.
// Used for unknown function calls like getEnv("KEY", "https://..."),
// os.environ.get("KEY", "https://..."), etc.
func extractURLArgFallback(argNodes []*tree_sitter.Node, source []byte, symbols map[string]string) string {
	for _, arg := range argNodes {
		val := resolveStringExpr(arg, source, symbols)
		if val != "" && looksLikeURL(val) {
			return val
		}
	}
	return ""
}

// resolveRustFormatMacro resolves Rust format!("...", args) macros.
func resolveRustFormatMacro(node *tree_sitter.Node, source []byte, symbols map[string]string) string {
	macroName := node.ChildByFieldName("macro")
	if macroName == nil || parser.NodeText(macroName, source) != "format" {
		return ""
	}
	tokenTree := findChildByKind(node, "token_tree")
	if tokenTree == nil {
		return ""
	}

	// Collect children: skip ( ) ,
	var parts []*tree_sitter.Node
	for i := uint(0); i < tokenTree.ChildCount(); i++ {
		child := tokenTree.Child(i)
		if child == nil {
			continue
		}
		kind := child.Kind()
		if kind == "(" || kind == ")" || kind == "," {
			continue
		}
		parts = append(parts, child)
	}
	if len(parts) == 0 {
		return ""
	}

	// First part is the format string
	fmtStr := extractStringContent(parts[0], source)
	if fmtStr == "" {
		return ""
	}

	// Substitute {} with resolved argument values
	argIdx := 1
	var b strings.Builder
	for j := 0; j < len(fmtStr); j++ {
		if j+1 < len(fmtStr) && fmtStr[j] == '{' && fmtStr[j+1] == '}' {
			if argIdx < len(parts) {
				val := resolveStringExpr(parts[argIdx], source, symbols)
				b.WriteString(val)
				argIdx++
			} else {
				b.WriteString("{}")
			}
			j++ // skip }
		} else {
			b.WriteByte(fmtStr[j])
		}
	}
	return b.String()
}

// --- Helpers ---

func isStringLiteral(kind string) bool {
	switch kind {
	case "interpreted_string_literal", "raw_string_literal", // Go
		"string_literal": // Rust, Java
		return true
	}
	// Note: PHP "encapsed_string" is NOT here because it can contain interpolation.
	// It's handled by resolvePHPEncapsed which covers both simple and interpolated cases.
	return false
}

// extractStringContent extracts the text content from a string literal node,
// stripping quotes. Works for Go, Rust, Java, JS/TS, PHP, Scala string nodes.
func extractStringContent(node *tree_sitter.Node, source []byte) string {
	if node == nil {
		return ""
	}
	// Look for content children (language-specific names for inner text)
	contentKinds := map[string]bool{
		"string_content":                     true, // Python, Rust, PHP, Scala
		"string_fragment":                    true, // JS/TS, Java
		"interpreted_string_literal_content": true, // Go
	}
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && contentKinds[child.Kind()] {
			return parser.NodeText(child, source)
		}
	}
	// Fallback: strip quotes from full text
	text := parser.NodeText(node, source)
	if len(text) >= 2 {
		first, last := text[0], text[len(text)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') || (first == '`' && last == '`') {
			return text[1 : len(text)-1]
		}
	}
	return ""
}

func findChildByKind(node *tree_sitter.Node, kind string) *tree_sitter.Node {
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && child.Kind() == kind {
			return child
		}
	}
	return nil
}

func findDescendantByKind(node *tree_sitter.Node, kind string) *tree_sitter.Node {
	if node.Kind() == kind {
		return node
	}
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child == nil {
			continue
		}
		if found := findDescendantByKind(child, kind); found != nil {
			return found
		}
	}
	return nil
}

// firstNonTrivialChild returns the first child that isn't punctuation.
func firstNonTrivialChild(node *tree_sitter.Node) *tree_sitter.Node {
	trivial := map[string]bool{"(": true, ")": true, ",": true, ";": true}
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && !trivial[child.Kind()] {
			return child
		}
	}
	return nil
}