package pipeline

import (
	"strings"
	"sync"
)

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
func (r *FunctionRegistry) Resolve(calleeName, moduleQN string, importMap map[string]string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Split calleeName for qualified calls like "pkg.Func" or "obj.method"
	parts := strings.SplitN(calleeName, ".", 2)
	prefix := parts[0]
	var suffix string
	if len(parts) > 1 {
		suffix = parts[1]
	}

	if result := r.resolveViaImportMap(prefix, suffix, importMap); result != "" {
		return result
	}

	if result := r.resolveViaSameModule(calleeName, suffix, moduleQN); result != "" {
		return result
	}

	return r.resolveViaNameLookup(calleeName, suffix, moduleQN)
}

// resolveViaImportMap tries to resolve a callee using the import map (Strategy 1).
func (r *FunctionRegistry) resolveViaImportMap(prefix, suffix string, importMap map[string]string) string {
	if importMap == nil {
		return ""
	}
	resolved, ok := importMap[prefix]
	if !ok {
		return ""
	}
	var candidate string
	if suffix != "" {
		candidate = resolved + "." + suffix
	} else {
		candidate = resolved
	}
	if _, exists := r.exact[candidate]; exists {
		return candidate
	}
	if suffix != "" {
		for qn := range r.exact {
			if strings.HasPrefix(qn, resolved+".") && strings.HasSuffix(qn, "."+suffix) {
				return qn
			}
		}
	}
	return ""
}

// resolveViaSameModule tries to resolve a callee within the same module (Strategy 2).
func (r *FunctionRegistry) resolveViaSameModule(calleeName, suffix, moduleQN string) string {
	sameModule := moduleQN + "." + calleeName
	if _, exists := r.exact[sameModule]; exists {
		return sameModule
	}
	if suffix != "" {
		sameModuleQualified := moduleQN + "." + suffix
		if _, exists := r.exact[sameModuleQualified]; exists {
			return sameModuleQualified
		}
	}
	return ""
}

// resolveViaNameLookup tries project-wide name lookup and suffix matching (Strategies 3+4).
func (r *FunctionRegistry) resolveViaNameLookup(calleeName, suffix, moduleQN string) string {
	lookupName := calleeName
	if suffix != "" {
		lookupName = suffix
	}
	simple := simpleName(lookupName)
	candidates := r.byName[simple]
	if len(candidates) == 1 {
		return candidates[0]
	}

	if suffix != "" {
		var matches []string
		for _, qn := range candidates {
			if strings.HasSuffix(qn, "."+calleeName) {
				return qn
			}
			if strings.HasSuffix(qn, "."+suffix) {
				matches = append(matches, qn)
			}
		}
		if len(matches) == 1 {
			return matches[0]
		}
		if len(matches) > 1 {
			return bestByImportDistance(matches, moduleQN)
		}
	}

	if len(candidates) > 1 {
		return bestByImportDistance(candidates, moduleQN)
	}

	return ""
}

// FuzzyResolve attempts a loose match when Resolve() returns "".
// It searches for any registered function whose simple name matches the callee's
// last name segment. Returns the best match (by import distance) and true,
// or "" and false if no match is found.
//
// Unlike Resolve(), this does not require prefix/import agreement — it purely
// matches on the function name. Results should be marked as "fuzzy" resolution.
func (r *FunctionRegistry) FuzzyResolve(calleeName, moduleQN string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Extract the simple name (last segment after dots)
	lookupName := simpleName(calleeName)
	candidates := r.byName[lookupName]

	if len(candidates) == 0 {
		return "", false
	}

	// If there's exactly one candidate, use it
	if len(candidates) == 1 {
		return candidates[0], true
	}

	// Multiple candidates: pick best by import distance
	best := bestByImportDistance(candidates, moduleQN)
	if best != "" {
		return best, true
	}

	return "", false
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
