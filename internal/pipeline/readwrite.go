package pipeline

import (
	"log/slog"
	"runtime"
	"strings"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
	"golang.org/x/sync/errgroup"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// passVariables extracts module-level variable declarations and creates Variable
// nodes + DEFINES edges during passDefinitions.
func extractVariables(
	root *tree_sitter.Node, source []byte, f discover.FileInfo,
	projectName, moduleQN string, spec *lang.LanguageSpec, result *parseResult,
) {
	if spec == nil || len(spec.VariableNodeTypes) == 0 {
		return
	}
	varTypes := toSet(spec.VariableNodeTypes)

	// Build a set of QNs already claimed by Function/Method nodes to avoid
	// overwriting them with Variable nodes (e.g., Lua anonymous function assignments,
	// JS/TS const arrow function assignments).
	var funcQNs map[string]bool
	switch f.Language {
	case lang.Lua, lang.JavaScript, lang.TypeScript, lang.TSX, lang.R,
		lang.Haskell, lang.OCaml:
		funcQNs = make(map[string]bool, len(result.Nodes))
		for _, n := range result.Nodes {
			if n.Label == "Function" || n.Label == "Method" {
				funcQNs[n.QualifiedName] = true
			}
		}
	}

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		if !isModuleLevelNode(node, f.Language) || !varTypes[node.Kind()] {
			return true
		}

		names := extractVarNames(node, source, f.Language)
		for _, name := range names {
			if name == "" || name == "_" {
				continue
			}
			varQN := fqn.Compute(projectName, f.RelPath, name)
			if funcQNs != nil && funcQNs[varQN] {
				continue // already extracted as Function/Method (Lua anonymous function)
			}
			startLine := safeRowToLine(node.StartPosition().Row)
			endLine := safeRowToLine(node.EndPosition().Row)

			result.Nodes = append(result.Nodes, &store.Node{
				Project:       projectName,
				Label:         "Variable",
				Name:          name,
				QualifiedName: varQN,
				FilePath:      f.RelPath,
				StartLine:     startLine,
				EndLine:       endLine,
				Properties:    map[string]any{"is_exported": isExported(name, f.Language)},
			})
			result.PendingEdges = append(result.PendingEdges, pendingEdge{
				SourceQN: moduleQN,
				TargetQN: varQN,
				Type:     "DEFINES",
			})
		}
		return false
	})
}

// moduleLevelParentKinds maps languages to their simple top-level parent kinds.
// Languages with more complex logic are handled in isModuleLevelNode directly.
var moduleLevelParentKinds = map[lang.Language][]string{
	lang.Go:         {"source_file"},
	lang.Rust:       {"source_file"},
	lang.CPP:        {"translation_unit"},
	lang.PHP:        {"program"},
	lang.Kotlin:     {"source_file"},
	lang.Java:       {"class_body"},
	lang.CSharp:     {"declaration_list"},
	lang.Scala:      {"template_body"},
	lang.Ruby:       {"program"},
	lang.C:          {"translation_unit"},
	lang.ObjectiveC: {"translation_unit"},
	lang.Bash:       {"program"},
	lang.Zig:        {"source_file"},
	lang.R:          {"program"},
	lang.Elixir:     {"source"},
	lang.Haskell:    {"declarations"},
	lang.OCaml:      {"compilation_unit"},
	lang.Groovy:     {"source_file"},
	lang.Dart:       {"program"},
	lang.SQL:        {"statement"},
	lang.Erlang:     {"source_file"},
	lang.Swift:      {"source_file"},
	lang.HCL:        {"body"},
	lang.SCSS:       {"stylesheet"},
	lang.Perl:       {"source_file", "assignment_expression", "expression_statement"},
}

// isModuleLevelNode checks if a node is at the module/file top level
// or (for class-based languages) at the class body level.
func isModuleLevelNode(node *tree_sitter.Node, language lang.Language) bool {
	parent := node.Parent()
	if parent == nil {
		return false
	}
	parentKind := parent.Kind()

	// Languages with custom grandparent logic
	switch language {
	case lang.Python:
		return isModuleLevelPython(parentKind, parent)
	case lang.JavaScript, lang.TypeScript, lang.TSX:
		return isModuleLevelJS(parentKind, parent)
	case lang.Lua:
		return isModuleLevelLua(parentKind, parent)
	case lang.YAML:
		return isModuleLevelYAML(parentKind, parent)
	}

	// Simple parent-kind lookup
	if kinds, ok := moduleLevelParentKinds[language]; ok {
		for _, k := range kinds {
			if parentKind == k {
				return true
			}
		}
	}
	return false
}

func isModuleLevelPython(parentKind string, parent *tree_sitter.Node) bool {
	if parentKind == "module" {
		return true
	}
	// Python wraps assignments: module → expression_statement → assignment
	if parentKind == "expression_statement" {
		if gp := parent.Parent(); gp != nil {
			return gp.Kind() == "module"
		}
	}
	return false
}

func isModuleLevelJS(parentKind string, parent *tree_sitter.Node) bool {
	if parentKind == "program" {
		return true
	}
	// JS/TS wraps exports: program → export_statement → declaration
	if parentKind == "export_statement" {
		if gp := parent.Parent(); gp != nil {
			return gp.Kind() == "program"
		}
	}
	return false
}

func isModuleLevelLua(parentKind string, parent *tree_sitter.Node) bool {
	if parentKind == "chunk" {
		return true
	}
	if parentKind == "block" {
		if gp := parent.Parent(); gp != nil {
			return gp.Kind() == "chunk"
		}
	}
	return false
}

func isModuleLevelYAML(parentKind string, parent *tree_sitter.Node) bool {
	if parentKind != "block_mapping" {
		return false
	}
	gp := parent.Parent()
	if gp == nil || gp.Kind() != "block_node" {
		return false
	}
	ggp := gp.Parent()
	return ggp != nil && ggp.GrammarName() == "_bgn_imp_doc"
}

// varNameExtractor is a function type for per-language variable name extraction.
type varNameExtractor func(node *tree_sitter.Node, source []byte) []string

// varNameExtractors maps each language to its variable name extractor.
var varNameExtractors = map[lang.Language]varNameExtractor{
	lang.Go:         extractGoVarNames,
	lang.Python:     extractPythonVarNames,
	lang.JavaScript: extractJSVarNames,
	lang.TypeScript: extractJSVarNames,
	lang.TSX:        extractJSVarNames,
	lang.Rust:       extractRustVarNames,
	lang.Java:       extractJavaCSharpVarNames,
	lang.CSharp:     extractJavaCSharpVarNames,
	lang.CPP:        extractCPPVarNames,
	lang.PHP:        extractPHPVarNames,
	lang.Lua:        extractLuaVarNames,
	lang.Scala:      extractScalaVarNames,
	lang.Kotlin:     extractKotlinVarNames,
	lang.Ruby:       extractRubyVarNames,
	lang.C:          extractCVarNames,
	lang.ObjectiveC: extractCVarNames,
	lang.Bash:       extractBashVarNames,
	lang.Zig:        extractZigVarNames,
	lang.R:          extractRVarNames,
	lang.Elixir:     extractElixirVarNames,
	lang.Haskell:    extractHaskellVarNames,
	lang.OCaml:      extractOCamlVarNames,
	lang.Groovy:     extractGroovyVarNames,
	lang.Dart:       extractDartVarNames,
	lang.Perl:       extractPerlVarNames,
	lang.Swift:      extractSwiftVarNames,
	lang.SCSS:       extractSCSSVarNames,
	lang.HCL:        extractHCLVarNames,
	lang.Erlang:     extractErlangVarNames,
	lang.SQL:        extractSQLVarNames,
	lang.YAML:       extractYAMLVarNames,
}

// extractVarNames extracts variable names from a declaration node.
func extractVarNames(node *tree_sitter.Node, source []byte, language lang.Language) []string {
	if fn, ok := varNameExtractors[language]; ok {
		return fn(node, source)
	}
	return nil
}

// extractErlangVarNames extracts names from Erlang -define() and -record() declarations.
// AST: pp_define → macro_lhs[field="lhs"] → var[field="name"]
// AST: record_decl → atom[field="name"]
func extractErlangVarNames(node *tree_sitter.Node, source []byte) []string {
	switch node.Kind() {
	case "pp_define":
		if lhs := node.ChildByFieldName("lhs"); lhs != nil {
			if nameNode := lhs.ChildByFieldName("name"); nameNode != nil {
				return []string{parser.NodeText(nameNode, source)}
			}
		}
	case "record_decl":
		if nameNode := node.ChildByFieldName("name"); nameNode != nil {
			return []string{parser.NodeText(nameNode, source)}
		}
	}
	return nil
}

// extractSQLVarNames extracts table/view names from SQL CREATE TABLE/VIEW statements.
// AST: create_table → object_reference → identifier[field="name"]
// AST: create_view → object_reference → identifier[field="name"]
func extractSQLVarNames(node *tree_sitter.Node, source []byte) []string {
	// Find the first direct object_reference child (contains the table/view name)
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && child.Kind() == "object_reference" {
			if nameNode := child.ChildByFieldName("name"); nameNode != nil {
				return []string{parser.NodeText(nameNode, source)}
			}
			// Fallback: text of the object_reference itself
			return []string{parser.NodeText(child, source)}
		}
	}
	return nil
}

// extractYAMLVarNames extracts top-level key names from YAML block_mapping_pair nodes.
// AST: block_mapping_pair → key field
func extractYAMLVarNames(node *tree_sitter.Node, source []byte) []string {
	if key := node.ChildByFieldName("key"); key != nil {
		return []string{parser.NodeText(key, source)}
	}
	return nil
}

// extractRVarNames extracts names from R left_assignment / right_assignment nodes.
func extractRVarNames(node *tree_sitter.Node, source []byte) []string {
	// Only extract assignments (<-, ->, =), not arithmetic/comparison operators
	if op := node.ChildByFieldName("operator"); op != nil {
		opText := parser.NodeText(op, source)
		if opText != "<-" && opText != "<<-" && opText != "->" && opText != "->>" && opText != "=" {
			return nil
		}
	}
	if lhs := node.ChildByFieldName("lhs"); lhs != nil {
		name := parser.NodeText(lhs, source)
		if name != "" {
			return []string{name}
		}
	}
	return nil
}

func extractElixirVarNames(node *tree_sitter.Node, source []byte) []string {
	// Elixir: binary_operator with = — lhs is the variable name
	if lhs := node.ChildByFieldName("left"); lhs != nil && lhs.Kind() == "identifier" {
		return []string{parser.NodeText(lhs, source)}
	}
	return nil
}

func extractHaskellVarNames(node *tree_sitter.Node, source []byte) []string {
	if nameNode := node.ChildByFieldName("name"); nameNode != nil {
		return []string{parser.NodeText(nameNode, source)}
	}
	return nil
}

func extractOCamlVarNames(node *tree_sitter.Node, source []byte) []string {
	// value_definition → let_binding (child) → pattern field = name
	lb := findChildByKind(node, "let_binding")
	if lb == nil {
		lb = node // let_binding directly
	}
	if pat := lb.ChildByFieldName("pattern"); pat != nil {
		return []string{parser.NodeText(pat, source)}
	}
	return nil
}

func extractGroovyVarNames(node *tree_sitter.Node, source []byte) []string {
	if nameNode := node.ChildByFieldName("name"); nameNode != nil {
		return []string{parser.NodeText(nameNode, source)}
	}
	// Groovy variable_declaration: first identifier child
	if id := findChildByKind(node, "identifier"); id != nil {
		return []string{parser.NodeText(id, source)}
	}
	return nil
}

func extractDartVarNames(node *tree_sitter.Node, source []byte) []string {
	if nameNode := node.ChildByFieldName("name"); nameNode != nil {
		return []string{parser.NodeText(nameNode, source)}
	}
	// Dart: initialized_identifier_list → initialized_identifier → identifier
	var names []string
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		if child.Kind() == "identifier" {
			names = append(names, parser.NodeText(child, source))
			return false
		}
		return true
	})
	return names
}

func extractPerlVarNames(node *tree_sitter.Node, source []byte) []string {
	// Perl variable_declaration: my $name = ... or my ($a, $b) = ...
	var names []string
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		if child.Kind() == "scalar" {
			for i := uint(0); i < child.ChildCount(); i++ {
				c := child.Child(i)
				if c != nil && c.Kind() == "varname" {
					names = append(names, parser.NodeText(c, source))
				}
			}
			return false
		}
		return true
	})
	return names
}

func extractSwiftVarNames(node *tree_sitter.Node, source []byte) []string {
	if nameNode := node.ChildByFieldName("name"); nameNode != nil {
		return []string{parser.NodeText(nameNode, source)}
	}
	// Swift: pattern → simple_identifier
	if pat := findChildByKind(node, "pattern"); pat != nil {
		if id := findChildByKind(pat, "simple_identifier"); id != nil {
			return []string{parser.NodeText(id, source)}
		}
	}
	return nil
}

func extractSCSSVarNames(node *tree_sitter.Node, source []byte) []string {
	// SCSS: declaration → property_name (grammar: variable) → "$name"
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil && child.GrammarName() == "variable" {
			return []string{parser.NodeText(child, source)}
		}
	}
	return nil
}

func extractHCLVarNames(node *tree_sitter.Node, source []byte) []string {
	// HCL: attribute → identifier = expression
	if id := findChildByKind(node, "identifier"); id != nil {
		return []string{parser.NodeText(id, source)}
	}
	return nil
}

func extractGoVarNames(node *tree_sitter.Node, source []byte) []string {
	var names []string
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		if child.Kind() == "var_spec" || child.Kind() == "const_spec" {
			if nameNode := child.ChildByFieldName("name"); nameNode != nil {
				names = append(names, parser.NodeText(nameNode, source))
			}
			return false
		}
		return true
	})
	return names
}

func extractPythonVarNames(node *tree_sitter.Node, source []byte) []string {
	leftNode := node.ChildByFieldName("left")
	if leftNode == nil || leftNode.Kind() != "identifier" {
		return nil
	}
	name := parser.NodeText(leftNode, source)
	if strings.HasPrefix(name, "__") {
		return nil
	}
	return []string{name}
}

func extractJSVarNames(node *tree_sitter.Node, source []byte) []string {
	var names []string
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		if child.Kind() == "variable_declarator" {
			if nameNode := child.ChildByFieldName("name"); nameNode != nil {
				if nameNode.Kind() == "identifier" {
					names = append(names, parser.NodeText(nameNode, source))
				}
			}
			return false
		}
		return true
	})
	return names
}

func extractRustVarNames(node *tree_sitter.Node, source []byte) []string {
	if nameNode := node.ChildByFieldName("name"); nameNode != nil {
		return []string{parser.NodeText(nameNode, source)}
	}
	return nil
}

// extractJavaCSharpVarNames extracts variable names from field_declaration nodes.
// AST: field_declaration → variable_declarator[declarator] → identifier[name]
func extractJavaCSharpVarNames(node *tree_sitter.Node, source []byte) []string {
	var names []string
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		if child.Kind() == "variable_declarator" {
			if nameNode := child.ChildByFieldName("name"); nameNode != nil {
				names = append(names, parser.NodeText(nameNode, source))
			}
			return false
		}
		return true
	})
	return names
}

// extractCPPVarNames extracts variable names from C++ declaration nodes.
// AST: declaration → init_declarator[declarator] → identifier[declarator]
func extractCPPVarNames(node *tree_sitter.Node, source []byte) []string {
	var names []string
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		if child.Kind() == "init_declarator" {
			if declNode := child.ChildByFieldName("declarator"); declNode != nil {
				if declNode.Kind() == "identifier" {
					names = append(names, parser.NodeText(declNode, source))
				}
			}
			return false
		}
		return true
	})
	return names
}

// extractPHPVarNames extracts variable names from PHP expression_statement assignments.
// AST: expression_statement → assignment_expression → variable_name[left]
func extractPHPVarNames(node *tree_sitter.Node, source []byte) []string {
	assign := findChildByKind(node, "assignment_expression")
	if assign == nil {
		return nil
	}
	leftNode := assign.ChildByFieldName("left")
	if leftNode == nil {
		return nil
	}
	name := parser.NodeText(leftNode, source)
	// Strip $ prefix from PHP variable names
	name = strings.TrimPrefix(name, "$")
	if name == "" {
		return nil
	}
	return []string{name}
}

// extractLuaVarNames extracts variable names from Lua variable_declaration.
// AST: variable_declaration → assignment_statement → variable_list → identifier
func extractLuaVarNames(node *tree_sitter.Node, source []byte) []string {
	var names []string
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		if child.Kind() == "identifier" {
			// Only grab identifiers in the variable_list (left side), not values
			if p := child.Parent(); p != nil && (p.Kind() == "variable_list" || p.Kind() == "variable_declaration") {
				names = append(names, parser.NodeText(child, source))
				return false
			}
		}
		return true
	})
	return names
}

// extractScalaVarNames extracts variable names from Scala val/var definitions.
// AST: val_definition → identifier[name]
func extractScalaVarNames(node *tree_sitter.Node, source []byte) []string {
	if nameNode := node.ChildByFieldName("name"); nameNode != nil {
		return []string{parser.NodeText(nameNode, source)}
	}
	// Fallback: find first identifier child
	if nameNode := findChildByKind(node, "identifier"); nameNode != nil {
		return []string{parser.NodeText(nameNode, source)}
	}
	return nil
}

// extractKotlinVarNames extracts variable names from Kotlin property_declaration.
// AST: property_declaration → variable_declaration → identifier
func extractKotlinVarNames(node *tree_sitter.Node, source []byte) []string {
	if nameNode := node.ChildByFieldName("name"); nameNode != nil {
		return []string{parser.NodeText(nameNode, source)}
	}
	// Kotlin: property_declaration → variable_declaration → identifier
	for i := uint(0); i < node.NamedChildCount(); i++ {
		child := node.NamedChild(i)
		if child == nil {
			continue
		}
		if child.Kind() == "variable_declaration" {
			if id := findChildByKind(child, "identifier"); id != nil {
				return []string{parser.NodeText(id, source)}
			}
		}
		if child.Kind() == "simple_identifier" || child.Kind() == "identifier" {
			return []string{parser.NodeText(child, source)}
		}
	}
	return nil
}

// extractRubyVarNames extracts variable names from Ruby assignment nodes.
// AST: assignment → constant[left="API_URL"]
func extractRubyVarNames(node *tree_sitter.Node, source []byte) []string {
	leftNode := node.ChildByFieldName("left")
	if leftNode == nil {
		return nil
	}
	name := parser.NodeText(leftNode, source)
	if name == "" {
		return nil
	}
	return []string{name}
}

// extractCVarNames extracts variable names from C declaration nodes.
// AST: declaration → init_declarator[declarator] → identifier or pointer_declarator → identifier
func extractCVarNames(node *tree_sitter.Node, source []byte) []string {
	var names []string
	parser.Walk(node, func(child *tree_sitter.Node) bool {
		kind := child.Kind()
		if kind != "init_declarator" && kind != "declaration" {
			return true
		}
		if name := extractCDeclaratorName(child, source); name != "" {
			names = append(names, name)
			return false
		}
		return true
	})
	return names
}

// extractCDeclaratorName extracts the identifier name from a C declarator node.
func extractCDeclaratorName(node *tree_sitter.Node, source []byte) string {
	declNode := node.ChildByFieldName("declarator")
	if declNode == nil {
		return ""
	}
	if declNode.Kind() == "pointer_declarator" {
		if id := declNode.ChildByFieldName("declarator"); id != nil && id.Kind() == "identifier" {
			return parser.NodeText(id, source)
		}
	}
	if declNode.Kind() == "identifier" {
		return parser.NodeText(declNode, source)
	}
	return ""
}

// extractBashVarNames extracts variable names from Bash variable_assignment.
// AST: variable_assignment → variable_name[name]
func extractBashVarNames(node *tree_sitter.Node, source []byte) []string {
	nameNode := node.ChildByFieldName("name")
	if nameNode == nil {
		return nil
	}
	return []string{parser.NodeText(nameNode, source)}
}

// extractZigVarNames extracts variable names from Zig variable_declaration.
// AST: variable_declaration → identifier (no field name, just the first identifier child)
func extractZigVarNames(node *tree_sitter.Node, source []byte) []string {
	if id := findChildByKind(node, "identifier"); id != nil {
		return []string{parser.NodeText(id, source)}
	}
	return nil
}

// passReadsWrites creates READS and WRITES edges from functions to Variable nodes.
func (p *Pipeline) passReadsWrites() {
	slog.Info("pass.readwrite")

	type fileEntry struct {
		relPath string
		cached  *cachedAST
	}
	var files []fileEntry
	for relPath, cached := range p.astCache {
		spec := lang.ForLanguage(cached.Language)
		if spec != nil && len(spec.AssignmentNodeTypes) > 0 {
			files = append(files, fileEntry{relPath, cached})
		}
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
			results[i] = p.resolveFileReadsWrites(fe.relPath, fe.cached)
			return nil
		})
	}
	_ = g.Wait()

	p.flushResolvedEdges(results)

	total := 0
	for _, r := range results {
		total += len(r)
	}
	slog.Info("pass.readwrite.done", "edges", total)
}

func (p *Pipeline) resolveFileReadsWrites(relPath string, cached *cachedAST) []resolvedEdge {
	spec := lang.ForLanguage(cached.Language)
	if spec == nil {
		return nil
	}

	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]
	assignTypes := toSet(spec.AssignmentNodeTypes)
	root := cached.Tree.RootNode()

	var edges []resolvedEdge
	seen := make(map[[3]string]bool)

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		if node.Kind() != "identifier" {
			return true
		}

		refName := parser.NodeText(node, cached.Source)
		if refName == "" || isKeywordOrBuiltin(refName, cached.Language) {
			return false
		}

		targetQN := p.resolveVariableStrict(refName, moduleQN, importMap)
		if targetQN == "" {
			return false
		}

		callerQN := findEnclosingFunction(node, cached.Source, p.ProjectName, relPath, spec)
		if callerQN == "" {
			callerQN = moduleQN
		}
		if callerQN == targetQN {
			return false
		}

		edgeType := "READS"
		if isWriteContext(node, assignTypes, cached.Language) {
			edgeType = "WRITES"

			if isAugmentedAssignmentContext(node) {
				readKey := [3]string{callerQN, targetQN, "READS"}
				if !seen[readKey] {
					seen[readKey] = true
					edges = append(edges, resolvedEdge{
						CallerQN: callerQN,
						TargetQN: targetQN,
						Type:     "READS",
					})
				}
			}
		}

		key := [3]string{callerQN, targetQN, edgeType}
		if seen[key] {
			return false
		}
		seen[key] = true

		edges = append(edges, resolvedEdge{
			CallerQN: callerQN,
			TargetQN: targetQN,
			Type:     edgeType,
		})
		return false
	})

	return edges
}

// resolveVariableStrict resolves an identifier to a Variable node using only
// Strategy 1 (import map) and Strategy 2 (same module). Never project-wide fallback.
func (p *Pipeline) resolveVariableStrict(name, moduleQN string, importMap map[string]string) string {
	p.registry.mu.RLock()
	defer p.registry.mu.RUnlock()

	if importMap != nil {
		if resolved, ok := importMap[name]; ok {
			if label, exists := p.registry.exact[resolved]; exists && label == "Variable" {
				return resolved
			}
		}
	}

	candidate := moduleQN + "." + name
	if label, exists := p.registry.exact[candidate]; exists && label == "Variable" {
		return candidate
	}

	return ""
}

// isWriteContext returns true if the identifier is on the left side of an assignment.
func isWriteContext(node *tree_sitter.Node, assignTypes map[string]bool, language lang.Language) bool {
	parent := node.Parent()
	if parent == nil {
		return false
	}

	// For Go short_var_declaration, left is always expression_list
	if language == lang.Go && parent.Kind() == "expression_list" {
		if gp := parent.Parent(); gp != nil && gp.Kind() == "short_var_declaration" {
			leftNode := gp.ChildByFieldName("left")
			if leftNode != nil && leftNode.Id() == parent.Id() {
				return true
			}
		}
	}

	if !assignTypes[parent.Kind()] {
		return false
	}

	leftNode := parent.ChildByFieldName("left")
	if leftNode == nil {
		return false
	}

	if leftNode.Id() == node.Id() {
		return true
	}

	// For Go: left is expression_list, check if node is inside it
	if leftNode.Kind() == "expression_list" {
		for i := uint(0); i < leftNode.NamedChildCount(); i++ {
			child := leftNode.NamedChild(i)
			if child != nil && child.Id() == node.Id() {
				return true
			}
		}
	}

	return false
}

// isAugmentedAssignmentContext returns true if the node is in an augmented assignment (+=, -=).
func isAugmentedAssignmentContext(node *tree_sitter.Node) bool {
	parent := node.Parent()
	if parent == nil {
		return false
	}
	switch parent.Kind() {
	case "augmented_assignment", "augmented_assignment_expression", "compound_assignment_expr":
		return true
	}
	return false
}

// extractGlobalVarNames extracts module-level variable names for the global_vars
// Module property.
func extractGlobalVarNames(root *tree_sitter.Node, source []byte, f discover.FileInfo, spec *lang.LanguageSpec) []string {
	if spec == nil || len(spec.VariableNodeTypes) == 0 {
		return nil
	}
	varTypes := toSet(spec.VariableNodeTypes)
	var names []string

	parser.Walk(root, func(node *tree_sitter.Node) bool {
		if !isModuleLevelNode(node, f.Language) || !varTypes[node.Kind()] {
			return true
		}
		names = append(names, extractVarNames(node, source, f.Language)...)
		return false
	})

	// Deduplicate
	seen := make(map[string]bool, len(names))
	deduped := names[:0]
	for _, n := range names {
		if n != "" && n != "_" && !seen[n] {
			seen[n] = true
			deduped = append(deduped, n)
		}
	}
	return deduped
}
