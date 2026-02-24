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

	// Strategy 1: Import map lookup
	if importMap != nil {
		if resolved, ok := importMap[prefix]; ok {
			var candidate string
			if suffix != "" {
				// Qualified call: pkg.Func -> resolved + "." + Func
				candidate = resolved + "." + suffix
			} else {
				// Direct import: from X import func -> resolved is the full QN
				candidate = resolved
			}
			if _, exists := r.exact[candidate]; exists {
				return candidate
			}
			// If the resolved path is a module, try appending the calleeName
			if suffix != "" {
				// Also try looking up just the suffix under the resolved module
				for qn := range r.exact {
					if strings.HasPrefix(qn, resolved+".") && strings.HasSuffix(qn, "."+suffix) {
						return qn
					}
				}
			}
		}
	}

	// Strategy 2: Same-module match
	sameModule := moduleQN + "." + calleeName
	if _, exists := r.exact[sameModule]; exists {
		return sameModule
	}
	// For qualified calls in the same module, try the full calleeName
	if suffix != "" {
		sameModuleQualified := moduleQN + "." + suffix
		if _, exists := r.exact[sameModuleQualified]; exists {
			return sameModuleQualified
		}
	}

	// Strategy 3: Project-wide single match by simple name
	lookupName := calleeName
	if suffix != "" {
		lookupName = suffix
	}
	simple := simpleName(lookupName)
	candidates := r.byName[simple]
	if len(candidates) == 1 {
		return candidates[0]
	}

	// Strategy 4: Suffix match with import distance scoring
	if suffix != "" {
		var matches []string
		for _, qn := range candidates {
			if strings.HasSuffix(qn, "."+calleeName) {
				return qn // exact suffix match
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

	// For non-qualified calls with multiple candidates, use import distance
	if len(candidates) > 1 {
		return bestByImportDistance(candidates, moduleQN)
	}

	return ""
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
