package discover

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
)

// IGNORE_PATTERNS are directory names to skip during discovery.
var IGNORE_PATTERNS = map[string]bool{
	".cache": true, ".claude": true, ".eclipse": true, ".eggs": true,
	".env": true, ".git": true, ".gradle": true, ".hg": true,
	".idea": true, ".maven": true, ".mypy_cache": true, ".nox": true,
	".npm": true, ".nyc_output": true, ".pnpm-store": true,
	".pytest_cache": true, ".qdrant_code_embeddings": true,
	".ruff_cache": true, ".svn": true, ".tmp": true, ".tox": true,
	".venv": true, ".vs": true, ".vscode": true, ".yarn": true,
	"__pycache__": true, "bin": true, "bower_components": true,
	"build": true, "coverage": true, "dist": true, "env": true,
	"htmlcov": true, "node_modules": true, "obj": true, "out": true,
	"Pods": true, "site-packages": true, "target": true, "temp": true,
	"tmp": true, "vendor": true, "venv": true,
}

// IGNORE_SUFFIXES are file suffixes to skip.
var IGNORE_SUFFIXES = map[string]bool{
	".tmp": true, "~": true, ".pyc": true, ".pyo": true,
	".o": true, ".a": true, ".so": true, ".dll": true, ".class": true,
}

// FileInfo represents a discovered source file.
type FileInfo struct {
	Path     string        // absolute path
	RelPath  string        // relative to repo root
	Language lang.Language // detected language
}

// Options configures file discovery.
type Options struct {
	IgnoreFile string // path to .cgrignore file (optional)
}

// shouldSkipDir returns true if the directory should be skipped during discovery.
func shouldSkipDir(name, rel string, extraIgnore []string) bool {
	if IGNORE_PATTERNS[name] {
		return true
	}
	for _, pattern := range extraIgnore {
		if matched, _ := filepath.Match(pattern, name); matched {
			return true
		}
		if matched, _ := filepath.Match(pattern, rel); matched {
			return true
		}
	}
	return false
}

// Discover walks a repository and returns all source files.
func Discover(ctx context.Context, repoPath string, opts *Options) ([]FileInfo, error) {
	repoPath, err := filepath.Abs(repoPath)
	if err != nil {
		return nil, err
	}

	// Check cancellation before starting walk
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Load .cgrignore patterns if present
	var extraIgnore []string
	if opts != nil && opts.IgnoreFile != "" {
		extraIgnore, _ = loadIgnoreFile(opts.IgnoreFile)
	} else {
		ignPath := filepath.Join(repoPath, ".cgrignore")
		extraIgnore, _ = loadIgnoreFile(ignPath)
	}

	var files []FileInfo

	err = filepath.Walk(repoPath, func(path string, info os.FileInfo, walkErr error) error {
		// Check context cancellation periodically during walk
		if err := ctx.Err(); err != nil {
			return err
		}

		if walkErr != nil {
			return filepath.SkipDir
		}

		rel, _ := filepath.Rel(repoPath, path)

		if info.IsDir() {
			if shouldSkipDir(info.Name(), rel, extraIgnore) {
				return filepath.SkipDir
			}
			return nil
		}

		// Skip ignored suffixes
		for suffix := range IGNORE_SUFFIXES {
			if strings.HasSuffix(path, suffix) {
				return nil
			}
		}

		// Check if we support this language
		ext := filepath.Ext(path)
		l, ok := lang.LanguageForExtension(ext)
		if ok {
			files = append(files, FileInfo{
				Path:     path,
				RelPath:  filepath.ToSlash(rel),
				Language: l,
			})
			return nil
		}

		// JSON files: pick up selectively (skip tool configs, lock files)
		if ext == ".json" && !isIgnoredJSON(info.Name()) {
			files = append(files, FileInfo{
				Path:     path,
				RelPath:  filepath.ToSlash(rel),
				Language: lang.JSON,
			})
		}
		return nil
	})

	return files, err
}

// ignoredJSONFiles are JSON filenames to skip (tool configs, lock files, specs).
var ignoredJSONFiles = map[string]bool{
	"package.json":       true,
	"package-lock.json":  true,
	"tsconfig.json":      true,
	"jsconfig.json":      true,
	"composer.json":      true,
	"composer.lock":      true,
	"yarn.lock":          true,
	"openapi.json":       true,
	"swagger.json":       true,
	"jest.config.json":   true,
	".eslintrc.json":     true,
	".prettierrc.json":   true,
	".babelrc.json":      true,
	"tslint.json":        true,
	"angular.json":       true,
	"firebase.json":      true,
	"renovate.json":      true,
	"lerna.json":         true,
	"turbo.json":         true,
	".stylelintrc.json":  true,
	"pnpm-lock.json":     true,
	"deno.json":          true,
	"biome.json":         true,
	"devcontainer.json":  true,
	".devcontainer.json": true,
	"launch.json":        true,
	"settings.json":      true,
	"extensions.json":    true,
	"tasks.json":         true,
}

func isIgnoredJSON(name string) bool {
	return ignoredJSONFiles[name]
}

func loadIgnoreFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var patterns []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			patterns = append(patterns, line)
		}
	}
	return patterns, scanner.Err()
}
