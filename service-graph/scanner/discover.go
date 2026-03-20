package scanner

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var ignoreDirs = map[string]bool{
	"node_modules": true, ".git": true, "dist": true, "build": true,
	".next": true, "coverage": true, "__pycache__": true, ".turbo": true,
}

var scanExtensions = map[string]bool{
	".ts": true, ".js": true, ".graphql": true, ".gql": true,
}

// DiscoverFiles recursively walks a repo and returns files matching scan extensions.
func DiscoverFiles(repoPath string) ([]string, error) {
	var files []string
	err := filepath.Walk(repoPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip errors
		}
		if info.IsDir() {
			name := info.Name()
			if ignoreDirs[name] || (strings.HasPrefix(name, ".") && name != ".") {
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(path)
		if scanExtensions[ext] {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

// InferServiceID returns the basename of a repo path as its service ID.
func InferServiceID(repoPath string) string {
	return filepath.Base(repoPath)
}

// DiscoverRepos finds git repos one level deep inside a directory.
func DiscoverRepos(reposDir string) ([]string, error) {
	entries, err := os.ReadDir(reposDir)
	if err != nil {
		return nil, err
	}

	var repos []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		fullPath := filepath.Join(reposDir, entry.Name())
		// Check if it has .git/HEAD
		headPath := filepath.Join(fullPath, ".git", "HEAD")
		if _, err := os.Stat(headPath); err == nil {
			repos = append(repos, fullPath)
		}
	}
	sort.Strings(repos)
	return repos, nil
}
