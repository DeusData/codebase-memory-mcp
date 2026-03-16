package pipeline

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
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

// Per-language file size limits to prevent tree-sitter stack overflows.
// SQL deeply recurses on bulk INSERTs; Windows has a 1MB default C stack.
const (
	maxSQLFileSize     = 1 << 20 // 1 MB
	maxGeneralFileSize = 4 << 20 // 4 MB
)

// cbmParseFile reads a file, calls cbm.ExtractFile(), and converts the
// result to the same parseResult format used by the batch write infrastructure.
// This replaces parseFileAST() — all AST walking happens in C.
func cbmParseFile(projectName string, f discover.FileInfo) *parseResult {
	// Guard: SQL tree-sitter grammar deeply recurses on large files (bulk INSERT
	// dumps), exhausting the C stack — especially on Windows (1 MB default).
	if f.Language == lang.SQL && f.Size > maxSQLFileSize {
		slog.Info("cbm.skip.large_sql", "path", f.RelPath, "size", f.Size)
		return &parseResult{File: f, Err: fmt.Errorf("skipped: SQL file too large (%d bytes, max %d)", f.Size, maxSQLFileSize)}
	}
	// General safety: files > 4 MB are likely generated/vendored and risk OOM or stack issues.
	if f.Size > maxGeneralFileSize {
		slog.Info("cbm.skip.large_file", "path", f.RelPath, "size", f.Size)
		return &parseResult{File: f, Err: fmt.Errorf("skipped: file too large (%d bytes, max %d)", f.Size, maxGeneralFileSize)}
	}

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
		node, edge := cbmDefToNode(&cbmResult.Definitions[i], projectName, moduleQN)
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
	if len(def.ParamNames) > 0 {
		props["param_names"] = def.ParamNames
	}
	if len(def.ParamTypes) > 0 {
		props["param_types"] = def.ParamTypes
	}
	if len(def.ReturnTypes) > 0 {
		props["return_types"] = def.ReturnTypes
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

	// Cross-file LSP resolution for Go files
	p.runGoLSPCrossFileResolution(ext, moduleQN, relPath, importMap)

	// Build type map from CBM type assignments
	typeMap := inferTypesCBM(ext.Result.TypeAssigns, p.registry, moduleQN, importMap)

	// LSP-resolved calls take priority (high confidence, type-aware).
	edges, lspCallerMethods := collectLSPResolvedEdges(ext.Result.ResolvedCalls)

	// Resolve remaining calls via registry + fuzzy matching
	for _, call := range ext.Result.Calls {
		if edge, ok := p.resolveCallEdge(call, moduleQN, importMap, typeMap, lspCallerMethods); ok {
			edges = append(edges, edge)
		}
	}

	// Python: track FastAPI Depends(func_ref) as CALLS edges
	edges = append(edges, p.extractPythonDependsEdges(relPath, ext)...)

	// C/C++: track dynamic DLL resolution (GetProcAddress/dlsym/Resolve) as CALLS edges
	edges = append(edges, p.extractDLLResolveEdges(relPath, ext)...)

	return edges
}

// runGoLSPCrossFileResolution re-runs LSP with cross-file definitions from imported packages.
func (p *Pipeline) runGoLSPCrossFileResolution(ext *cachedExtraction, moduleQN, relPath string, importMap map[string]string) {
	if ext.Language != lang.Go || p.goLSPIdx == nil {
		return
	}
	crossDefs := p.goLSPIdx.collectCrossFileDefs(importMap)
	if len(crossDefs) == 0 {
		return
	}
	source := readFileSource(p.RepoPath, relPath)
	if len(source) == 0 {
		return
	}
	fileDefs := cbm.DefsToLSPDefs(ext.Result.Definitions, moduleQN)
	resolved := cbm.RunGoLSPCrossFile(source, moduleQN, fileDefs, crossDefs, ext.Result.Imports)
	if len(resolved) > 0 {
		ext.Result.ResolvedCalls = resolved
	}
}

// collectLSPResolvedEdges converts LSP-resolved calls to edges and builds a caller+method dedup set.
func collectLSPResolvedEdges(resolvedCalls []cbm.ResolvedCall) (edges []resolvedEdge, lspCallerMethods map[string]bool) {
	lspResolved := make(map[string]bool)
	lspCallerMethods = make(map[string]bool)

	for _, rc := range resolvedCalls {
		if rc.CallerQN == "" || rc.CalleeQN == "" || rc.Confidence == 0 {
			continue
		}
		key := rc.CallerQN + "\x00" + rc.CalleeQN
		if lspResolved[key] {
			continue
		}
		lspResolved[key] = true
		edges = append(edges, resolvedEdge{
			CallerQN: rc.CallerQN,
			TargetQN: rc.CalleeQN,
			Type:     "CALLS",
			Properties: map[string]any{
				"confidence":          float64(rc.Confidence),
				"confidence_band":     confidenceBand(float64(rc.Confidence)),
				"resolution_strategy": rc.Strategy,
			},
		})

		shortName := rc.CalleeQN
		if idx := strings.LastIndex(shortName, "."); idx >= 0 {
			shortName = shortName[idx+1:]
		}
		lspCallerMethods[rc.CallerQN+"\x00"+shortName] = true
	}

	return edges, lspCallerMethods
}

// resolveCallEdge resolves a single call to an edge using the registry and type system.
func (p *Pipeline) resolveCallEdge(
	call cbm.Call, moduleQN string, importMap map[string]string,
	typeMap TypeMap, lspCallerMethods map[string]bool,
) (resolvedEdge, bool) {
	calleeName := call.CalleeName
	callerQN := call.EnclosingFuncQN
	if calleeName == "" {
		return resolvedEdge{}, false
	}
	if callerQN == "" {
		callerQN = moduleQN
	}

	// Skip if LSP already resolved this caller+method
	fuzzyShort := calleeName
	if idx := strings.LastIndex(fuzzyShort, "."); idx >= 0 {
		fuzzyShort = fuzzyShort[idx+1:]
	}
	if lspCallerMethods[callerQN+"\x00"+fuzzyShort] {
		return resolvedEdge{}, false
	}

	// Python self.method() resolution
	if strings.HasPrefix(calleeName, "self.") {
		classQN := extractClassFromMethodQN(callerQN)
		if classQN != "" {
			candidate := classQN + "." + calleeName[5:]
			if p.registry.Exists(candidate) {
				return resolvedEdge{CallerQN: callerQN, TargetQN: candidate, Type: "CALLS"}, true
			}
		}
	}

	// Type-based method dispatch for qualified calls like obj.method()
	result := p.resolveCallWithTypes(calleeName, moduleQN, importMap, typeMap)
	if result.QualifiedName == "" {
		if fuzzyResult, ok := p.registry.FuzzyResolve(calleeName, moduleQN, importMap); ok && fuzzyResult.Confidence >= 0.10 {
			return resolvedEdge{
				CallerQN: callerQN,
				TargetQN: fuzzyResult.QualifiedName,
				Type:     "CALLS",
				Properties: map[string]any{
					"confidence":          fuzzyResult.Confidence,
					"confidence_band":     confidenceBand(fuzzyResult.Confidence),
					"resolution_strategy": fuzzyResult.Strategy,
				},
			}, true
		}
		return resolvedEdge{}, false
	}

	if result.Confidence < 0.10 {
		return resolvedEdge{}, false
	}
	return resolvedEdge{
		CallerQN: callerQN,
		TargetQN: result.QualifiedName,
		Type:     "CALLS",
		Properties: map[string]any{
			"confidence":          result.Confidence,
			"confidence_band":     confidenceBand(result.Confidence),
			"resolution_strategy": result.Strategy,
		},
	}, true
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

// --- Python FastAPI Depends() tracking (#27) ---

// pythonDependsRe matches Depends(func_ref) patterns in Python function signatures.
// Captures the function reference passed to Depends().
var pythonDependsRe = regexp.MustCompile(`Depends\(\s*([\w.]+)`)

// extractPythonDependsEdges scans Python function signatures for Depends(func_ref)
// patterns and creates CALLS edges from the endpoint to the dependency function.
// Without this, functions referenced via Depends() appear as dead code (in_degree=0).
func (p *Pipeline) extractPythonDependsEdges(relPath string, ext *cachedExtraction) []resolvedEdge {
	if ext.Language != lang.Python || ext.Result == nil {
		return nil
	}

	// Early bail: check if any call in this file targets "Depends"
	hasDependsCall := false
	for _, call := range ext.Result.Calls {
		if call.CalleeName == "Depends" || strings.HasSuffix(call.CalleeName, ".Depends") {
			hasDependsCall = true
			break
		}
	}
	if !hasDependsCall {
		return nil
	}

	// Read full file source
	source := readFileSource(p.RepoPath, relPath)
	if len(source) == 0 {
		return nil
	}
	lines := strings.Split(string(source), "\n")

	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)
	importMap := p.importMaps[moduleQN]

	var edges []resolvedEdge
	seen := make(map[[2]string]bool)

	for _, def := range ext.Result.Definitions {
		if def.Label != "Function" && def.Label != "Method" {
			continue
		}
		if def.StartLine <= 0 {
			continue
		}

		// Extract function signature lines (def line through closing paren + colon).
		// Signatures can span multiple lines in Python.
		sigEnd := def.StartLine + 15 // scan up to 15 lines for multi-line signatures
		if def.EndLine > 0 && sigEnd > def.EndLine {
			sigEnd = def.EndLine
		}
		if sigEnd > len(lines) {
			sigEnd = len(lines)
		}

		var sig strings.Builder
		for i := def.StartLine - 1; i < sigEnd; i++ {
			sig.WriteString(lines[i])
			sig.WriteByte('\n')
			trimmed := strings.TrimSpace(lines[i])
			// Stop once we hit the colon that ends the function definition
			if strings.HasSuffix(trimmed, "):") || strings.HasSuffix(trimmed, ") :") ||
				strings.HasSuffix(trimmed, ") -> None:") || strings.Contains(trimmed, ") ->") {
				break
			}
		}

		sigStr := sig.String()
		matches := pythonDependsRe.FindAllStringSubmatch(sigStr, -1)
		for _, m := range matches {
			funcRef := m[1]
			if funcRef == "" {
				continue
			}

			key := [2]string{def.QualifiedName, funcRef}
			if seen[key] {
				continue
			}
			seen[key] = true

			result := p.registry.Resolve(funcRef, moduleQN, importMap)
			if result.QualifiedName == "" {
				continue
			}

			edges = append(edges, resolvedEdge{
				CallerQN: def.QualifiedName,
				TargetQN: result.QualifiedName,
				Type:     "CALLS",
				Properties: map[string]any{
					"confidence":          0.95,
					"confidence_band":     "high",
					"resolution_strategy": "fastapi_depends",
				},
			})
		}
	}

	if len(edges) > 0 {
		slog.Info("pass3.fastapi_depends", "file", relPath, "edges", len(edges))
	}
	return edges
}

// --- C/C++ dynamic DLL resolution tracking (#29) ---

// DLL resolution patterns for C/C++ dynamic linking.
var (
	// GetProcAddress(handle, "FunctionName") — Win32 API
	dllGetProcAddrRe = regexp.MustCompile(`GetProcAddress\s*\(\s*\w+\s*,\s*["'](\w+)["']`)
	// dlsym(handle, "function_name") — POSIX
	dllDlsymRe = regexp.MustCompile(`dlsym\s*\(\s*\w+\s*,\s*["'](\w+)["']`)
	// obj.Resolve("FunctionName") or obj->Resolve("FunctionName") — custom DLL loaders
	dllResolveRe = regexp.MustCompile(`(?:->|\.)\s*Resolve\s*\(\s*["'](\w+)["']`)
	// LoadLibrary("dll_name.dll") or dlopen("lib.so") — DLL name extraction
	dllLoadRe = regexp.MustCompile(`(?:LoadLibrary[AW]?|dlopen)\s*\(\s*["']([^"']+)["']`)
)

// extractDLLResolveEdges scans C/C++ function source for dynamic DLL resolution
// patterns (GetProcAddress, dlsym, Resolve) and creates CALLS edges to synthetic
// external function nodes, enabling call graph tracking across DLL boundaries.
func (p *Pipeline) extractDLLResolveEdges(relPath string, ext *cachedExtraction) []resolvedEdge {
	if ext.Language != lang.CPP && ext.Language != lang.C {
		return nil
	}
	if ext.Result == nil {
		return nil
	}

	// Early bail: check if any call targets a DLL resolution function
	hasDLLCall := false
	for _, call := range ext.Result.Calls {
		name := call.CalleeName
		if name == "GetProcAddress" || name == "GetProcAddressA" || name == "GetProcAddressW" ||
			name == "dlsym" || strings.HasSuffix(name, ".Resolve") || strings.HasSuffix(name, "->Resolve") {
			hasDLLCall = true
			break
		}
	}
	if !hasDLLCall {
		return nil
	}

	// Read full file source
	source := readFileSource(p.RepoPath, relPath)
	if len(source) == 0 {
		return nil
	}
	sourceStr := string(source)
	sourceLines := strings.Split(sourceStr, "\n")

	// Extract DLL name from LoadLibrary/dlopen calls (best-effort)
	dllName := "external"
	if m := dllLoadRe.FindStringSubmatch(sourceStr); m != nil {
		dllName = filepath.Base(m[1])
		// Strip extension
		if ext := filepath.Ext(dllName); ext != "" {
			dllName = strings.TrimSuffix(dllName, ext)
		}
	}

	moduleQN := fqn.ModuleQN(p.ProjectName, relPath)

	var edges []resolvedEdge
	seen := make(map[[2]string]bool)

	for _, def := range ext.Result.Definitions {
		if def.Label != "Function" && def.Label != "Method" {
			continue
		}
		if def.StartLine <= 0 || def.EndLine <= 0 {
			continue
		}

		// Extract function body source
		endLine := def.EndLine
		if endLine > len(sourceLines) {
			endLine = len(sourceLines)
		}
		var body strings.Builder
		for i := def.StartLine - 1; i < endLine; i++ {
			body.WriteString(sourceLines[i])
			body.WriteByte('\n')
		}
		bodyStr := body.String()

		// Match DLL resolution patterns
		for _, re := range []*regexp.Regexp{dllGetProcAddrRe, dllDlsymRe, dllResolveRe} {
			for _, m := range re.FindAllStringSubmatch(bodyStr, -1) {
				funcName := m[1]
				if funcName == "" {
					continue
				}

				callerQN := def.QualifiedName
				targetQN := moduleQN + ".dll." + dllName + "." + funcName

				key := [2]string{callerQN, targetQN}
				if seen[key] {
					continue
				}
				seen[key] = true

				edges = append(edges, resolvedEdge{
					CallerQN: callerQN,
					TargetQN: targetQN,
					Type:     "CALLS",
					Properties: map[string]any{
						"confidence":          0.85,
						"confidence_band":     "high",
						"resolution_strategy": "dll_resolve",
						"dll_name":            dllName,
						"dll_function":        funcName,
					},
				})
			}
		}
	}

	if len(edges) > 0 {
		slog.Info("pass3.dll_resolve", "file", relPath, "edges", len(edges))
	}

	return edges
}
