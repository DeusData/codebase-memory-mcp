package orgdiscovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
)

// frameworkSignal maps a file path to a framework name and service type.
type frameworkSignal struct {
	Path      string
	Framework string
	Type      string
	IsDir     bool // true for directory-based signals (prefix match)
}

// frameworkSignals defines file-path-to-framework mappings checked against the Git Tree API.
var frameworkSignals = []frameworkSignal{
	// Backend frameworks
	{Path: "nest-cli.json", Framework: "nestjs", Type: "backend"},

	// Frontend frameworks
	{Path: "nuxt.config.ts", Framework: "nuxt", Type: "frontend"},
	{Path: "nuxt.config.js", Framework: "nuxt", Type: "frontend"},
	{Path: "next.config.js", Framework: "nextjs", Type: "frontend"},
	{Path: "next.config.ts", Framework: "nextjs", Type: "frontend"},
	{Path: "next.config.mjs", Framework: "nextjs", Type: "frontend"},
	{Path: "angular.json", Framework: "angular", Type: "frontend"},
	{Path: "vue.config.js", Framework: "vue-cli", Type: "frontend"},

	// Build tools / meta (no type override)
	{Path: "turbo.json", Framework: "turborepo", Type: ""},
	{Path: "pnpm-workspace.yaml", Framework: "pnpm-workspace", Type: ""},
	{Path: "lerna.json", Framework: "lerna", Type: ""},

	// Go
	{Path: "go.mod", Framework: "go", Type: "backend"},
	{Path: "cmd/", Framework: "go-service", Type: "backend", IsDir: true},

	// Python
	{Path: "pyproject.toml", Framework: "python", Type: "backend"},
	{Path: "requirements.txt", Framework: "python", Type: "backend"},

	// Infrastructure
	{Path: "Dockerfile", Framework: "docker", Type: ""},
	{Path: "helm/Chart.yaml", Framework: "helm", Type: "infra"},
	{Path: "terraform/", Framework: "terraform", Type: "infra", IsDir: true},
	{Path: "Jenkinsfile", Framework: "jenkins", Type: ""},

	// Mobile
	{Path: "pubspec.yaml", Framework: "flutter", Type: "mobile"},

	// Docs
	{Path: "mkdocs.yml", Framework: "mkdocs", Type: "docs"},
	{Path: "docusaurus.config.js", Framework: "docusaurus", Type: "docs"},
}

// nestjs monorepo signal: apps/ directory + nest-cli.json
var nestMonorepoDir = "apps/"

// packageJSONDeps maps npm dependency names to framework identifiers.
var packageJSONDeps = map[string]string{
	"@nestjs/core": "nestjs",
	"vue":          "vue",
	"react":        "react",
	"fastify":      "fastify",
	"express":      "express",
	"nuxt":         "nuxt",
	"next":         "nextjs",
}

// ghTree is the GitHub Git Tree API response.
type ghTree struct {
	SHA       string       `json:"sha"`
	Tree      []ghTreeNode `json:"tree"`
	Truncated bool         `json:"truncated"`
}

// ghTreeNode is a single entry in a Git Tree response.
type ghTreeNode struct {
	Path string `json:"path"`
	Type string `json:"type"` // "blob" or "tree"
}

// packageJSON is a minimal representation for dependency detection.
type packageJSON struct {
	Dependencies    map[string]string `json:"dependencies"`
	DevDependencies map[string]string `json:"devDependencies"`
}

// EnrichFrameworks detects frameworks for each repo using the GitHub Git Tree API.
// Updates Type and Tags on each repo. Adds framework to Tags.
func (s *Scanner) EnrichFrameworks(ctx context.Context, repos []manifest.Repo) error {
	const maxConcurrent = 10
	sem := make(chan struct{}, maxConcurrent)
	var mu sync.Mutex
	var firstErr error

	var wg sync.WaitGroup
	for i := range repos {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			framework, serviceType := s.detectFramework(ctx, repos[idx].Name, "main")

			mu.Lock()
			defer mu.Unlock()

			if framework != "" {
				if !contains(repos[idx].Tags, framework) {
					repos[idx].Tags = append(repos[idx].Tags, framework)
				}
			}
			if serviceType != "" {
				repos[idx].Type = serviceType
			}
		}(i)
	}
	wg.Wait()

	return firstErr
}

// detectFramework fetches the repo's file tree and infers framework from config files.
// It tries the given branch first, then falls back to "master" on 404.
func (s *Scanner) detectFramework(ctx context.Context, repoName, defaultBranch string) (framework, serviceType string) {
	tree, err := s.fetchTree(ctx, repoName, defaultBranch)
	if err != nil {
		// Fallback to master if main returned 404.
		if defaultBranch == "main" {
			tree, err = s.fetchTree(ctx, repoName, "master")
			if err != nil {
				return "", ""
			}
		} else {
			return "", ""
		}
	}

	// Build a set of paths for quick lookup.
	pathSet := make(map[string]bool, len(tree.Tree))
	hasPackageJSON := false
	for _, node := range tree.Tree {
		pathSet[node.Path] = true
		if node.Path == "package.json" {
			hasPackageJSON = true
		}
	}

	// Check each signal against the tree.
	var bestFramework, bestType string
	hasNestCLI := pathSet["nest-cli.json"]
	hasAppsDir := false

	for _, node := range tree.Tree {
		if strings.HasPrefix(node.Path, nestMonorepoDir) {
			hasAppsDir = true
			break
		}
	}

	for _, sig := range frameworkSignals {
		matched := false
		if sig.IsDir {
			// Directory signal: check if any path starts with the prefix.
			for _, node := range tree.Tree {
				if strings.HasPrefix(node.Path, sig.Path) {
					matched = true
					break
				}
			}
		} else {
			matched = pathSet[sig.Path]
		}

		if !matched {
			continue
		}

		// First matching signal with a non-empty type wins for type.
		if sig.Type != "" && bestType == "" {
			bestType = sig.Type
		}
		// First matching signal with a non-empty framework wins.
		if sig.Framework != "" && bestFramework == "" {
			bestFramework = sig.Framework
		}
	}

	// NestJS monorepo refinement: nest-cli.json + apps/ directory.
	if hasNestCLI && hasAppsDir && bestFramework == "nestjs" {
		bestFramework = "nestjs-monorepo"
	}

	// package.json refinement: fetch and check deps for more accurate framework.
	if hasPackageJSON && bestFramework == "" {
		if pkgFramework := s.fetchPackageJSONFramework(ctx, repoName, defaultBranch); pkgFramework != "" {
			bestFramework = pkgFramework
			// Infer type from package.json framework if not already set.
			if bestType == "" {
				bestType = typeFromPackageFramework(pkgFramework)
			}
		}
	}

	return bestFramework, bestType
}

// fetchTree fetches the Git Tree for a repo/branch via the GitHub API.
func (s *Scanner) fetchTree(ctx context.Context, repoName, branch string) (*ghTree, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/git/trees/%s?recursive=1", s.apiBaseURL, s.org, repoName, branch)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("github tree API %d: %s", resp.StatusCode, string(body))
	}

	var tree ghTree
	if err := json.NewDecoder(resp.Body).Decode(&tree); err != nil {
		return nil, fmt.Errorf("decode tree: %w", err)
	}
	return &tree, nil
}

// fetchPackageJSONFramework fetches package.json and checks deps for known frameworks.
func (s *Scanner) fetchPackageJSONFramework(ctx context.Context, repoName, branch string) string {
	url := fmt.Sprintf("%s/repos/%s/%s/contents/package.json?ref=%s", s.apiBaseURL, s.org, repoName, branch)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Accept", "application/vnd.github.raw+json")

	resp, err := s.client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return ""
	}

	var pkg packageJSON
	if err := json.NewDecoder(resp.Body).Decode(&pkg); err != nil {
		return ""
	}

	// Check dependencies first (higher priority), then devDependencies.
	for dep, fw := range packageJSONDeps {
		if _, ok := pkg.Dependencies[dep]; ok {
			return fw
		}
	}
	for dep, fw := range packageJSONDeps {
		if _, ok := pkg.DevDependencies[dep]; ok {
			return fw
		}
	}

	return ""
}

// typeFromPackageFramework maps a package.json-detected framework to a service type.
func typeFromPackageFramework(framework string) string {
	switch framework {
	case "nestjs", "fastify", "express":
		return "backend"
	case "vue", "react", "nuxt", "nextjs":
		return "frontend"
	default:
		return ""
	}
}

// contains checks if a string slice contains a value.
func contains(ss []string, val string) bool {
	for _, s := range ss {
		if s == val {
			return true
		}
	}
	return false
}
