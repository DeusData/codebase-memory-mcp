package pipeline

import (
	"strings"
	"sync"
)

// ResolutionResult carries the resolved QN plus quality metadata.
// Initial confidence values are estimates — recalibrate after measuring
// precision per strategy on real repos.
type ResolutionResult struct {
	QualifiedName  string
	Strategy       string  // "import_map", "import_map_suffix", "same_module", "unique_name", "suffix_match", "fuzzy", "type_dispatch"
	Confidence     float64 // 0.0–1.0
	CandidateCount int     // how many candidates were considered
}

// FunctionRegistry indexes all Function, Method, and Class nodes by qualified
// name and simple name for fast call resolution.
type FunctionRegistry struct {
	mu sync.RWMutex
	// exact maps qualifiedName -> label (Function/Method/Class)
	exact map[string]string
	// byName maps simpleName -> []qualifiedName for reverse lookup
	byName map[string][]string
}

// NewFunctionRegistry creates an empty registry.
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		exact:  make(map[string]string),
		byName: make(map[string][]string),
	}
}

// Register adds a node to the registry.
func (r *FunctionRegistry) Register(name, qualifiedName, nodeLabel string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.exact[qualifiedName] = nodeLabel

	// Index by simple name (last segment after the final dot)
	simple := simpleName(qualifiedName)
	// Avoid duplicates in the slice
	for _, existing := range r.byName[simple] {
		if existing == qualifiedName {
			return
		}
	}
	r.byName[simple] = append(r.byName[simple], qualifiedName)
}

// Resolve attempts to find the qualified name of a callee using a prioritized
// resolution strategy:
//  1. Import map lookup
//  2. Same-module match
//  3. Project-wide single match by simple name
//  4. Suffix match with import distance scoring
func (r *FunctionRegistry) Resolve(calleeName, moduleQN string, importMap map[string]string) ResolutionResult {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Split calleeName for qualified calls like "pkg.Func" or "obj.method"
	parts := strings.SplitN(calleeName, ".", 2)
	prefix := parts[0]
	var suffix string
	if len(parts) > 1 {
		suffix = parts[1]
	}

	if result := r.resolveViaImportMap(prefix, suffix, importMap); result.QualifiedName != "" {
		return result
	}

	if result := r.resolveViaSameModule(calleeName, suffix, moduleQN); result.QualifiedName != "" {
		return result
	}

	return r.resolveViaNameLookup(calleeName, suffix, moduleQN, importMap)
}

// resolveViaImportMap tries to resolve a callee using the import map (Strategy 1).
func (r *FunctionRegistry) resolveViaImportMap(prefix, suffix string, importMap map[string]string) ResolutionResult {
	if importMap == nil {
		return ResolutionResult{}
	}
	resolved, ok := importMap[prefix]
	if !ok {
		return ResolutionResult{}
	}
	var candidate string
	if suffix != "" {
		candidate = resolved + "." + suffix
	} else {
		candidate = resolved
	}
	if _, exists := r.exact[candidate]; exists {
		return ResolutionResult{QualifiedName: candidate, Strategy: "import_map", Confidence: 0.95, CandidateCount: 1}
	}
	if suffix != "" {
		for qn := range r.exact {
			if strings.HasPrefix(qn, resolved+".") && strings.HasSuffix(qn, "."+suffix) {
				return ResolutionResult{QualifiedName: qn, Strategy: "import_map_suffix", Confidence: 0.85, CandidateCount: 1}
			}
		}
	}
	return ResolutionResult{}
}

// resolveViaSameModule tries to resolve a callee within the same module (Strategy 2).
func (r *FunctionRegistry) resolveViaSameModule(calleeName, suffix, moduleQN string) ResolutionResult {
	sameModule := moduleQN + "." + calleeName
	if _, exists := r.exact[sameModule]; exists {
		return ResolutionResult{QualifiedName: sameModule, Strategy: "same_module", Confidence: 0.90, CandidateCount: 1}
	}
	if suffix != "" {
		sameModuleQualified := moduleQN + "." + suffix
		if _, exists := r.exact[sameModuleQualified]; exists {
			return ResolutionResult{QualifiedName: sameModuleQualified, Strategy: "same_module", Confidence: 0.90, CandidateCount: 1}
		}
	}
	return ResolutionResult{}
}

// resolveViaNameLookup tries project-wide name lookup and suffix matching (Strategies 3+4).
func (r *FunctionRegistry) resolveViaNameLookup(calleeName, suffix, moduleQN string, importMap map[string]string) ResolutionResult {
	lookupName := calleeName
	if suffix != "" {
		lookupName = suffix
	}
	simple := simpleName(lookupName)
	candidates := r.byName[simple]

	// Strategy 3: unique name — single candidate project-wide
	if len(candidates) == 1 {
		conf := 0.75
		reachable := importMap == nil || isImportReachable(candidates[0], importMap)
		if !reachable {
			// Cross-app guard: in monorepos, reject unique_name matches across
			// app boundaries (e.g., apps/mobile → apps/api-go). These can only
			// communicate via HTTP_CALLS, not direct CALLS edges.
			if isCrossApp(moduleQN, candidates[0]) {
				return ResolutionResult{}
			}
			conf *= 0.5
		}
		return ResolutionResult{QualifiedName: candidates[0], Strategy: "unique_name", Confidence: conf, CandidateCount: 1}
	}

	// Strategy 4: suffix match with import distance scoring
	if suffix != "" {
		if res := r.resolveSuffixMatch(calleeName, suffix, moduleQN, importMap, candidates); res.QualifiedName != "" {
			return res
		}
	}

	return pickBestCandidate(candidates, moduleQN, importMap)
}

// resolveSuffixMatch handles Strategy 4 — suffix-based matching among multiple candidates.
func (r *FunctionRegistry) resolveSuffixMatch(calleeName, suffix, moduleQN string, importMap map[string]string, candidates []string) ResolutionResult {
	var matches []string
	for _, qn := range candidates {
		if strings.HasSuffix(qn, "."+calleeName) {
			conf := importAdjustedConfidence(0.55, qn, importMap)
			return ResolutionResult{QualifiedName: qn, Strategy: "suffix_match", Confidence: conf, CandidateCount: len(candidates)}
		}
		if strings.HasSuffix(qn, "."+suffix) {
			matches = append(matches, qn)
		}
	}
	if importMap != nil {
		matches = filterImportReachable(matches, importMap)
	}
	if len(matches) == 1 {
		return ResolutionResult{QualifiedName: matches[0], Strategy: "suffix_match", Confidence: 0.55, CandidateCount: len(candidates)}
	}
	if len(matches) > 1 {
		best := bestByImportDistance(matches, moduleQN)
		return ResolutionResult{QualifiedName: best, Strategy: "suffix_match", Confidence: 0.55, CandidateCount: len(matches)}
	}
	return ResolutionResult{}
}

// pickBestCandidate selects the best match from multiple candidates with import filtering.
func pickBestCandidate(candidates []string, moduleQN string, importMap map[string]string) ResolutionResult {
	if len(candidates) <= 1 {
		return ResolutionResult{}
	}
	filtered := candidates
	if importMap != nil {
		filtered = filterImportReachable(candidates, importMap)
	}
	if len(filtered) == 0 {
		best := bestByImportDistance(candidates, moduleQN)
		return ResolutionResult{QualifiedName: best, Strategy: "suffix_match", Confidence: 0.55 * 0.5, CandidateCount: len(candidates)}
	}
	if len(filtered) == 1 {
		return ResolutionResult{QualifiedName: filtered[0], Strategy: "suffix_match", Confidence: 0.55, CandidateCount: len(candidates)}
	}
	best := bestByImportDistance(filtered, moduleQN)
	return ResolutionResult{QualifiedName: best, Strategy: "suffix_match", Confidence: 0.55, CandidateCount: len(filtered)}
}

// importAdjustedConfidence halves confidence when a candidate is not import-reachable.
func importAdjustedConfidence(base float64, candidateQN string, importMap map[string]string) float64 {
	if importMap != nil && !isImportReachable(candidateQN, importMap) {
		return base * 0.5
	}
	return base
}

// FuzzyResolve attempts a loose match when Resolve() returns "".
// It searches for any registered function whose simple name matches the callee's
// last name segment. Returns the best match (by import distance) with confidence,
// or an empty result and false if no match is found.
//
// Unlike Resolve(), this does not require prefix/import agreement — it purely
// matches on the function name.
func (r *FunctionRegistry) FuzzyResolve(calleeName, moduleQN string, importMap map[string]string) (ResolutionResult, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Extract the simple name (last segment after dots)
	lookupName := simpleName(calleeName)
	candidates := r.byName[lookupName]

	if len(candidates) == 0 {
		return ResolutionResult{}, false
	}

	// If there's exactly one candidate, use it
	if len(candidates) == 1 {
		reachable := importMap == nil || isImportReachable(candidates[0], importMap)
		if !reachable && isCrossApp(moduleQN, candidates[0]) {
			return ResolutionResult{}, false
		}
		conf := 0.40
		if !reachable {
			conf *= 0.5
		}
		return ResolutionResult{
			QualifiedName: candidates[0], Strategy: "fuzzy",
			Confidence: conf, CandidateCount: 1,
		}, true
	}

	// Multiple candidates: filter by import reachability, then pick best by distance
	filtered := candidates
	if importMap != nil {
		filtered = filterImportReachable(candidates, importMap)
	}
	if len(filtered) == 0 {
		// No import-reachable candidates — filter cross-app, then pick best
		var sameApp []string
		for _, c := range candidates {
			if !isCrossApp(moduleQN, c) {
				sameApp = append(sameApp, c)
			}
		}
		if len(sameApp) == 0 {
			return ResolutionResult{}, false
		}
		best := bestByImportDistance(sameApp, moduleQN)
		if best == "" {
			return ResolutionResult{}, false
		}
		return ResolutionResult{
			QualifiedName: best, Strategy: "fuzzy",
			Confidence: 0.30 * 0.5, CandidateCount: len(sameApp),
		}, true
	}
	if len(filtered) == 1 {
		return ResolutionResult{
			QualifiedName: filtered[0], Strategy: "fuzzy",
			Confidence: 0.40, CandidateCount: len(candidates),
		}, true
	}
	best := bestByImportDistance(filtered, moduleQN)
	if best == "" {
		return ResolutionResult{}, false
	}
	return ResolutionResult{
		QualifiedName: best, Strategy: "fuzzy",
		Confidence: 0.30, CandidateCount: len(filtered),
	}, true
}

// LabelOf returns the node label for a qualified name, or "" if not registered.
func (r *FunctionRegistry) LabelOf(qualifiedName string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.exact[qualifiedName]
}

// Exists returns true if a qualified name is registered.
// Uses RLock for concurrent read safety.
func (r *FunctionRegistry) Exists(qualifiedName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.exact[qualifiedName]
	return ok
}

// FindByName returns all qualified names with the given simple name.
func (r *FunctionRegistry) FindByName(name string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]string, len(r.byName[name]))
	copy(result, r.byName[name])
	return result
}

// FindEndingWith returns all qualified names ending with ".suffix".
func (r *FunctionRegistry) FindEndingWith(suffix string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	target := "." + suffix
	var result []string
	for qn := range r.exact {
		if strings.HasSuffix(qn, target) {
			result = append(result, qn)
		}
	}
	return result
}

// Size returns the number of entries in the registry.
func (r *FunctionRegistry) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.exact)
}

// simpleName extracts the last dot-separated segment.
func simpleName(qn string) string {
	if idx := strings.LastIndex(qn, "."); idx >= 0 {
		return qn[idx+1:]
	}
	return qn
}

// bestByImportDistance picks the candidate whose QN shares the longest common
// prefix with the caller's module QN. This approximates "closest in the
// project structure".
func bestByImportDistance(candidates []string, callerModuleQN string) string {
	best := ""
	bestLen := -1

	for _, c := range candidates {
		prefixLen := commonPrefixLen(c, callerModuleQN)
		if prefixLen > bestLen {
			bestLen = prefixLen
			best = c
		}
	}
	return best
}

// commonPrefixLen returns the length of the common dot-segment prefix.
func commonPrefixLen(a, b string) int {
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")

	count := 0
	for i := 0; i < len(aParts) && i < len(bParts); i++ {
		if aParts[i] != bParts[i] {
			break
		}
		count++
	}
	return count
}

// modulePrefix extracts the module portion of a QN (everything before the last dot segment).
func modulePrefix(qn string) string {
	if idx := strings.LastIndex(qn, "."); idx >= 0 {
		return qn[:idx]
	}
	return qn
}

// isImportReachable checks if the candidate's module prefix appears anywhere
// in the caller's import map values.
func isImportReachable(candidateQN string, importMap map[string]string) bool {
	candidateModule := modulePrefix(candidateQN)
	for _, importedQN := range importMap {
		if strings.HasPrefix(candidateModule, importedQN) || strings.HasPrefix(importedQN, candidateModule) {
			return true
		}
	}
	return false
}

// filterImportReachable returns only candidates reachable via the import map.
// Returns the original slice if importMap is nil or filtering eliminates everything.
func filterImportReachable(candidates []string, importMap map[string]string) []string {
	if importMap == nil {
		return candidates
	}
	var filtered []string
	for _, c := range candidates {
		if isImportReachable(c, importMap) {
			filtered = append(filtered, c)
		}
	}
	return filtered
}

// isCrossApp detects when caller and callee are in different app boundaries
// within a monorepo. Qualified names encode the file path, so we extract the
// first 3 dotted segments after the project name (e.g., "apps.mobile.src" vs
// "apps.api-go.internal") and reject if they diverge. This prevents false
// CALLS edges between frontend and backend that can only communicate via HTTP.
func isCrossApp(callerQN, candidateQN string) bool {
	callerApp := appSegment(callerQN)
	candidateApp := appSegment(candidateQN)
	if callerApp == "" || candidateApp == "" {
		return false // can't determine, don't block
	}
	return callerApp != candidateApp
}

// appSegment extracts the app boundary segment from a qualified name.
// For "Project.apps.mobile.src.components.X" → "apps.mobile"
// For "Project.apps.api-go.internal.event.X" → "apps.api-go"
// For "Project.scripts.foo.X" → "scripts.foo"
// Returns "" if the QN is too short to determine.
func appSegment(qn string) string {
	parts := strings.SplitN(qn, ".", 4) // [project, dir1, dir2, rest]
	if len(parts) < 3 {
		return ""
	}
	return parts[1] + "." + parts[2]
}

// confidenceBand returns a human-readable band label for a confidence score.
func confidenceBand(score float64) string {
	switch {
	case score >= 0.7:
		return "high"
	case score >= 0.45:
		return "medium"
	default:
		return "speculative"
	}
}
