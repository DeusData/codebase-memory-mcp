package orgdb

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Dep represents a single GHL-internal package dependency.
type Dep struct {
	Scope       string // "@platform-core"
	Name        string // "base-service"
	DepType     string // "dependencies", "devDependencies", "peerDependencies"
	VersionSpec string // "^3.2.0"
}

var internalScopes = []string{
	"@platform-core/", "@platform-ui/", "@gohighlevel/", "@ghl/", "@frontend-core/",
}

// ParsePackageJSON reads a package.json file and extracts only GHL-internal
// dependencies (scoped under @platform-core, @platform-ui, @gohighlevel,
// @ghl, or @frontend-core). External deps are skipped.
func ParsePackageJSON(path string) ([]Dep, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("orgdb: read %s: %w", path, err)
	}

	var pkg struct {
		Dependencies     map[string]string `json:"dependencies"`
		DevDependencies  map[string]string `json:"devDependencies"`
		PeerDependencies map[string]string `json:"peerDependencies"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return nil, fmt.Errorf("orgdb: parse %s: %w", path, err)
	}

	var deps []Dep
	extract := func(depMap map[string]string, depType string) {
		for name, version := range depMap {
			scope, pkgName := splitScoped(name)
			if scope == "" || !isInternalScope(scope) {
				continue
			}
			deps = append(deps, Dep{
				Scope:       scope,
				Name:        pkgName,
				DepType:     depType,
				VersionSpec: version,
			})
		}
	}

	extract(pkg.Dependencies, "dependencies")
	extract(pkg.DevDependencies, "devDependencies")
	extract(pkg.PeerDependencies, "peerDependencies")

	return deps, nil
}

func splitScoped(name string) (string, string) {
	if !strings.HasPrefix(name, "@") {
		return "", name
	}
	idx := strings.Index(name, "/")
	if idx < 0 {
		return "", name
	}
	return name[:idx], name[idx+1:]
}

func isInternalScope(scope string) bool {
	prefix := scope + "/"
	for _, s := range internalScopes {
		if prefix == s {
			return true
		}
	}
	return false
}

// ParsePackageName reads the "name" field from a package.json file and splits it
// into scope and name. For example, "@platform-core/base-service" → ("@platform-core", "base-service").
// Returns empty strings if the name is not a scoped GHL-internal package.
func ParsePackageName(path string) (scope, name string, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("orgdb: read %s: %w", path, err)
	}

	var pkg struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &pkg); err != nil {
		return "", "", fmt.Errorf("orgdb: parse %s: %w", path, err)
	}

	if pkg.Name == "" {
		return "", "", nil
	}

	s, n := splitScoped(pkg.Name)
	if s == "" || !isInternalScope(s) {
		return "", "", nil
	}
	return s, n, nil
}
