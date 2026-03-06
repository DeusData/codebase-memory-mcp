package pipeline

import (
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/cbm"
	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/fqn"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// cachedExtraction holds the CBM extraction result for a file.
// Replaces cachedAST for all post-definition passes.
type cachedExtraction struct {
	Result   *cbm.FileResult
	Language lang.Language
}

// cbmParseFile reads a file, calls cbm.ExtractFile(), and converts the
// result to the same parseResult format used by the batch write infrastructure.
// This replaces parseFileAST() — all AST walking happens in C.
func cbmParseFile(projectName string, f discover.FileInfo) *parseResult {
	source, cleanup, err := mmapFile(f.Path)
	if cleanup != nil {
		defer cleanup()
	}
	return cbmParseFileFromSource(projectName, f, source, err)
}

// cbmParseFileFromSource is like cbmParseFile but takes pre-read source data.
// Used by the producer-consumer pipeline where I/O and CPU are separated.
func cbmParseFileFromSource(projectName string, f discover.FileInfo, source []byte, readErr error) *parseResult {
	result := &parseResult{File: f}

	if readErr != nil {
		result.Err = readErr
		return result
	}

	// Strip UTF-8 BOM if present (common in C#/Windows-generated files)
	source = stripBOM(source)

	cbmResult, err := cbm.ExtractFile(source, f.Language, projectName, f.RelPath)
	if err != nil {
		slog.Warn("cbm.extract.err", "path", f.RelPath, "lang", f.Language, "err", err)
		result.Err = err
		return result
	}

	result.CBMResult = cbmResult

	moduleQN := fqn.ModuleQN(projectName, f.RelPath)

	// Module node
	moduleNode := &store.Node{
		Project:       projectName,
		Label:         "Module",
		Name:          filepath.Base(f.RelPath),
		QualifiedName: moduleQN,
		FilePath:      f.RelPath,
		Properties:    make(map[string]any),
	}
	result.Nodes = append(result.Nodes, moduleNode)

	// Convert CBM definitions to store.Node objects
	for i := range cbmResult.Definitions {
		// Fix: mark test functions as entry points.
		// The C extractor only sets is_test on the module, not on individual functions.
		// Test functions (Go Test*/Benchmark*/Example*, Python test_*, etc.) are invoked
		// by the test runner, not by the call graph — they must be entry points.
		def := &cbmResult.Definitions[i]
		if cbmResult.IsTestFile && !def.IsEntryPoint &&
			(def.Label == "Function" || def.Label == "Method") &&
			isTestFunction(def.Name, f.Language) {
			def.IsEntryPoint = true
			def.IsTest = true
		}

		// Mark exported Go handler methods as entry points.
		// Echo handlers are registered via method value references (g.POST("", h.Method))
		// which the C extractor doesn't track as explicit calls.
		if !def.IsEntryPoint && def.Label == "Method" && def.IsExported &&
			f.Language == lang.Go &&
			strings.Contains(f.RelPath, "handler") &&
			strings.Contains(def.Signature, "echo.Context") {
			def.IsEntryPoint = true
		}

		node, edge := cbmDefToNode(def, projectName, moduleQN)
		result.Nodes = append(result.Nodes, node)
		result.PendingEdges = append(result.PendingEdges, edge)
	}

	// Enrich module node with properties from CBM result
	enrichModuleNodeCBM(moduleNode, cbmResult, result)

	// Build import map from CBM imports
	if len(cbmResult.Imports) > 0 {
		importMap := make(map[string]string, len(cbmResult.Imports))
		for _, imp := range cbmResult.Imports {
			if imp.LocalName != "" && imp.ModulePath != "" {
				importMap[imp.LocalName] = imp.ModulePath
			}
		}
		result.ImportMap = importMap
	}

	moduleNode.Properties["imports_count"] = cbmResult.ImportCount
	moduleNode.Properties["is_test"] = cbmResult.IsTestFile

	// exports: collect exported symbol names
	var exports []string
	for _, n := range result.Nodes {
		if n.QualifiedName == moduleQN {
			continue
		}
		if exp, ok := n.Properties["is_exported"].(bool); ok && exp {
			exports = append(exports, n.Name)
		}
	}
	if len(exports) > 0 {
		moduleNode.Properties["exports"] = exports
	}

	if symbols := buildSymbolSummary(result.Nodes, moduleQN); len(symbols) > 0 {
		moduleNode.Properties["symbols"] = symbols
	}

	return result
}

// cbmDefToNode converts a CBM Definition to a store.Node and its DEFINES/DEFINES_METHOD edge.
func cbmDefToNode(def *cbm.Definition, projectName, moduleQN string) (*store.Node, pendingEdge) {
	props := map[string]any{}

	if def.Signature != "" {
		props["signature"] = def.Signature
	}
	if def.ReturnType != "" {
		props["return_type"] = def.ReturnType
	}
	if def.Receiver != "" {
		props["receiver"] = def.Receiver
	}
	if def.Docstring != "" {
		props["docstring"] = def.Docstring
	}
	if len(def.Decorators) > 0 {
		props["decorators"] = def.Decorators
		if hasFrameworkDecorator(def.Decorators) {
			props["is_entry_point"] = true
		}
	}
	if len(def.BaseClasses) > 0 {
		props["base_classes"] = def.BaseClasses
	}
	if len(def.ParamTypes) > 0 {
		props["param_types"] = def.ParamTypes
	}
	if def.Complexity > 0 {
		props["complexity"] = def.Complexity
	}
	if def.Lines > 0 {
		props["lines"] = def.Lines
	}

	props["is_exported"] = def.IsExported

	if def.IsAbstract {
		props["is_abstract"] = true
	}
	if def.IsTest {
		props["is_test"] = true
	}
	if def.IsEntryPoint {
		props["is_entry_point"] = true
	}

	node := &store.Node{
		Project:       projectName,
		Label:         def.Label,
		Name:          def.Name,
		QualifiedName: def.QualifiedName,
		FilePath:      def.FilePath,
		StartLine:     def.StartLine,
		EndLine:       def.EndLine,
		Properties:    props,
	}

	// Determine edge type and source QN
	edgeType := "DEFINES"
	sourceQN := moduleQN
	if def.Label == "Method" || def.Label == "Field" {
		edgeType = "DEFINES_METHOD"
		if def.Label == "Field" {
			edgeType = "DEFINES_FIELD"
		}
		if def.ParentClass != "" {
			sourceQN = def.ParentClass
		}
	}

	edge := pendingEdge{
		SourceQN: sourceQN,
		TargetQN: def.QualifiedName,
		Type:     edgeType,
	}

	return node, edge
}

// enrichModuleNodeCBM populates module node properties from CBM extraction results.
func enrichModuleNodeCBM(moduleNode *store.Node, cbmResult *cbm.FileResult, _ *parseResult) {
	// Additional module-level properties can be added here if CBM exposes them
	// (e.g., macros, constants, global_vars from CBMFileResult)
}

// inferTypesCBM builds a TypeMap from CBM TypeAssign data + registry resolution.
// Replaces the 14 language-specific infer*Types() functions.
func inferTypesCBM(
	typeAssigns []cbm.TypeAssign,
	registry *FunctionRegistry,
	moduleQN string,
	importMap map[string]string,
) TypeMap {
	types := make(TypeMap, len(typeAssigns))

	for _, ta := range typeAssigns {
		if ta.VarName == "" || ta.TypeName == "" {
			continue
		}
		classQN := resolveAsClass(ta.TypeName, registry, moduleQN, importMap)
		if classQN != "" {
			types[ta.VarName] = classQN
		}
	}

	// Return type propagation is handled by CBM TypeAssigns which already
	// detect constructor patterns. Additional return-type-based inference
	// from the returnTypes map is still useful for non-constructor calls.
	// This would require the call data which we have in CBM Calls.
	// For now, constructor-based inference covers the primary use case.

	return types
}

// resolveFileCallsCBM resolves all call targets using pre-extracted CBM data.
// Replaces resolveFileCalls() — no AST walking needed.
func (p *Pipeline) resolveFileCallsCBM(relPath string, ext *cachedExtraction) []resolvedEdge {
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]

	// Build type map from CBM type assignments
	typeMap := inferTypesCBM(ext.Result.TypeAssigns, p.registry, moduleQN, importMap)

	var edges []resolvedEdge

	for _, call := range ext.Result.Calls {
		calleeName := call.CalleeName
		callerQN := call.EnclosingFuncQN
		if calleeName == "" {
			continue
		}
		if callerQN == "" {
			callerQN = moduleQN
		}

		// Python self.method() resolution
		if strings.HasPrefix(calleeName, "self.") {
			classQN := extractClassFromMethodQN(callerQN)
			if classQN != "" {
				candidate := classQN + "." + calleeName[5:]
				if p.registry.Exists(candidate) {
					edges = append(edges, resolvedEdge{CallerQN: callerQN, TargetQN: candidate, Type: "CALLS"})
					continue
				}
			}
		}

		// Type-based method dispatch for qualified calls like obj.method()
		result := p.resolveCallWithTypes(calleeName, moduleQN, importMap, typeMap)
		if result.QualifiedName == "" {
			if fuzzyResult, ok := p.registry.FuzzyResolve(calleeName, moduleQN, importMap); ok {
				if fuzzyResult.QualifiedName == callerQN {
					continue // skip self-reference
				}
				edges = append(edges, resolvedEdge{
					CallerQN: callerQN,
					TargetQN: fuzzyResult.QualifiedName,
					Type:     "CALLS",
					Properties: map[string]any{
						"confidence":          fuzzyResult.Confidence,
						"confidence_band":     confidenceBand(fuzzyResult.Confidence),
						"resolution_strategy": fuzzyResult.Strategy,
					},
				})
			}
			continue
		}

		if result.QualifiedName == callerQN {
			continue // skip self-reference
		}

		edges = append(edges, resolvedEdge{
			CallerQN: callerQN,
			TargetQN: result.QualifiedName,
			Type:     "CALLS",
			Properties: map[string]any{
				"confidence":          result.Confidence,
				"confidence_band":     confidenceBand(result.Confidence),
				"resolution_strategy": result.Strategy,
			},
		})
	}

	return edges
}

// resolveFileUsagesCBM resolves usage references using pre-extracted CBM data.
// Replaces resolveFileUsages() — no AST walking needed.
func (p *Pipeline) resolveFileUsagesCBM(relPath string, ext *cachedExtraction) []resolvedEdge {
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]

	var edges []resolvedEdge
	seen := make(map[[2]string]bool)

	for _, usage := range ext.Result.Usages {
		refName := usage.RefName
		callerQN := usage.EnclosingFuncQN
		if refName == "" {
			continue
		}
		if callerQN == "" {
			callerQN = moduleQN
		}

		result := p.registry.Resolve(refName, moduleQN, importMap)
		if result.QualifiedName == "" {
			continue
		}

		key := [2]string{callerQN, result.QualifiedName}
		if seen[key] {
			continue
		}
		seen[key] = true

		edges = append(edges, resolvedEdge{
			CallerQN: callerQN,
			TargetQN: result.QualifiedName,
			Type:     "USAGE",
		})
	}

	return edges
}

// resolveFileThrowsCBM resolves throw/raise targets using pre-extracted CBM data.
// Replaces resolveFileThrows() — no AST walking needed.
func (p *Pipeline) resolveFileThrowsCBM(relPath string, ext *cachedExtraction) []resolvedEdge {
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]

	var edges []resolvedEdge
	seen := make(map[[2]string]bool)

	for _, thr := range ext.Result.Throws {
		excName := thr.ExceptionName
		funcQN := thr.EnclosingFuncQN
		if excName == "" || funcQN == "" {
			continue
		}

		key := [2]string{funcQN, excName}
		if seen[key] {
			continue
		}
		seen[key] = true

		// Determine edge type: THROWS for checked exceptions, RAISES for runtime/unchecked
		edgeType := "RAISES"
		if isCheckedException(excName) {
			edgeType = "THROWS"
		}

		// Try to resolve exception class
		result := p.registry.Resolve(excName, moduleQN, importMap)
		targetQN := excName
		if result.QualifiedName != "" {
			targetQN = result.QualifiedName
		}

		edges = append(edges, resolvedEdge{
			CallerQN: funcQN,
			TargetQN: targetQN,
			Type:     edgeType,
		})
	}

	return edges
}

// resolveFileReadsWritesCBM resolves reads/writes using pre-extracted CBM data.
// Replaces resolveFileReadsWrites() — no AST walking needed.
func (p *Pipeline) resolveFileReadsWritesCBM(relPath string, ext *cachedExtraction) []resolvedEdge {
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]

	var edges []resolvedEdge
	seen := make(map[[3]string]bool)

	for _, rw := range ext.Result.ReadWrites {
		varName := rw.VarName
		funcQN := rw.EnclosingFuncQN
		if varName == "" || funcQN == "" {
			continue
		}

		edgeType := "READS"
		if rw.IsWrite {
			edgeType = "WRITES"
		}

		key := [3]string{funcQN, varName, edgeType}
		if seen[key] {
			continue
		}
		seen[key] = true

		// Try to resolve variable to a known node
		result := p.registry.Resolve(varName, moduleQN, importMap)
		if result.QualifiedName == "" {
			continue
		}

		edges = append(edges, resolvedEdge{
			CallerQN: funcQN,
			TargetQN: result.QualifiedName,
			Type:     edgeType,
		})
	}

	return edges
}

// resolveFileTypeRefsCBM resolves type references using pre-extracted CBM data.
// Replaces resolveFileTypeRefs() — no AST walking needed.
func (p *Pipeline) resolveFileTypeRefsCBM(relPath string, ext *cachedExtraction) []resolvedEdge {
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]

	var edges []resolvedEdge
	seen := make(map[[2]string]bool)

	for _, tr := range ext.Result.TypeRefs {
		typeName := tr.TypeName
		funcQN := tr.EnclosingFuncQN
		if typeName == "" || funcQN == "" {
			continue
		}

		key := [2]string{funcQN, typeName}
		if seen[key] {
			continue
		}
		seen[key] = true

		// Resolve type name to a node QN
		result := p.registry.Resolve(typeName, moduleQN, importMap)
		if result.QualifiedName == "" {
			continue
		}

		edges = append(edges, resolvedEdge{
			CallerQN: funcQN,
			TargetQN: result.QualifiedName,
			Type:     "USES_TYPE",
		})
	}

	return edges
}

// resolveFileConfiguresCBM resolves env access calls using pre-extracted CBM data.
// Replaces resolveFileConfigures() — no AST walking needed.
func (p *Pipeline) resolveFileConfiguresCBM(relPath string, ext *cachedExtraction, envIndex map[string]string) []resolvedEdge {
	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)

	var edges []resolvedEdge
	seen := make(map[[2]string]bool)

	for _, ea := range ext.Result.EnvAccesses {
		envKey := ea.EnvKey
		funcQN := ea.EnclosingFuncQN
		if envKey == "" || funcQN == "" {
			continue
		}

		targetModuleQN, ok := envIndex[envKey]
		if !ok {
			continue
		}

		key := [2]string{funcQN, targetModuleQN}
		if seen[key] {
			continue
		}
		seen[key] = true

		_ = moduleQN
		edges = append(edges, resolvedEdge{
			CallerQN: funcQN,
			TargetQN: targetModuleQN,
			Type:     "CONFIGURES",
			Properties: map[string]any{
				"env_key": envKey,
			},
		})
	}

	return edges
}

// extractClassFromMethodQN extracts the class QN from a method QN.
// E.g., "project.path.ClassName.methodName" -> "project.path.ClassName"
func extractClassFromMethodQN(methodQN string) string {
	idx := strings.LastIndex(methodQN, ".")
	if idx <= 0 {
		return ""
	}
	return methodQN[:idx]
}

// isCheckedException returns true if the exception name looks like a checked exception
// (Java convention: checked exceptions don't extend RuntimeException).
func isCheckedException(excName string) bool {
	// Heuristic: exceptions ending in "Exception" without "Runtime" prefix are checked
	if strings.HasSuffix(excName, "Exception") && !strings.HasPrefix(excName, "Runtime") {
		return true
	}
	return false
}
