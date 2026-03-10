package discover

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
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
	".venv": true, ".vs": true, ".vscode": true, ".worktrees": true, ".yarn": true,
	"__pycache__": true, "bin": true, "bower_components": true,
	"build": true, "coverage": true, "dist": true, "env": true,
	"htmlcov": true, "node_modules": true, "obj": true, "out": true,
	"Pods": true, "site-packages": true, "target": true, "temp": true,
	"tmp": true, "vendor": true, "venv": true,
}

// IGNORE_SUFFIXES are file suffixes that are never source files.
var IGNORE_SUFFIXES = map[string]bool{
	// Temp/compiled
	".tmp": true, "~": true, ".pyc": true, ".pyo": true,
	".o": true, ".a": true, ".so": true, ".dll": true, ".class": true,
	// Images
	".png": true, ".jpg": true, ".jpeg": true, ".gif": true,
	".ico": true, ".bmp": true, ".tiff": true, ".webp": true, ".svg": true,
	// Binaries
	".wasm": true, ".node": true, ".exe": true, ".bin": true, ".dat": true,
	// Databases
	".db": true, ".sqlite": true, ".sqlite3": true,
	// Fonts
	".woff": true, ".woff2": true, ".ttf": true, ".eot": true, ".otf": true,
}

// IndexMode controls how aggressively files are filtered during discovery.
type IndexMode string

const (
	ModeFull IndexMode = "full" // default: parse everything supported
	ModeFast IndexMode = "fast" // aggressive filtering for speed
)

// FileInfo represents a discovered source file.
type FileInfo struct {
	Path     string        // absolute path
	RelPath  string        // relative to repo root
	Language lang.Language // detected language
	Size     int64         // file size in bytes
}

// Options configures file discovery.
type Options struct {
	IgnoreFile  string    // path to .cgrignore file (optional)
	Mode        IndexMode // indexing mode (full or fast)
	MaxFileSize int64     // max file size in bytes (0 = no limit)
}

// fastIgnoreDirs are additional directories skipped in fast mode.
var fastIgnoreDirs = map[string]bool{
	"generated": true, "gen": true, "auto-generated": true,
	"fixtures": true, "testdata": true, "test_data": true,
	"__tests__": true, "__mocks__": true, "__snapshots__": true,
	"__fixtures__": true, "__test__": true,
	"docs": true, "doc": true, "documentation": true,
	"examples": true, "example": true, "samples": true, "sample": true,
	"assets": true, "static": true, "public": true, "media": true,
	"third_party": true, "thirdparty": true, "3rdparty": true, "external": true,
	"migrations": true, "seeds": true,
	"e2e": true, "integration": true,
	"locale": true, "locales": true, "i18n": true, "l10n": true,
	"scripts": true, "tools": true, "hack": true,
}

// fastIgnoreSuffixes are additional file extensions skipped in fast mode.
var fastIgnoreSuffixes = map[string]bool{
	// Archives
	".zip": true, ".tar": true, ".gz": true, ".bz2": true, ".xz": true,
	".rar": true, ".7z": true, ".jar": true, ".war": true, ".ear": true,
	// Media/audio/video
	".mp3": true, ".mp4": true, ".avi": true, ".mov": true, ".wav": true,
	".flac": true, ".ogg": true, ".mkv": true, ".webm": true,
	// Documents
	".pdf": true, ".doc": true, ".docx": true, ".xls": true, ".xlsx": true,
	".ppt": true, ".pptx": true, ".odt": true, ".ods": true,
	// Source maps
	".map": true,
	// Minified bundles
	".min.js": true, ".min.css": true,
	// Certificates/keys
	".pem": true, ".crt": true, ".key": true, ".cer": true, ".p12": true,
	// Serialized data
	".pb": true, ".proto": true, ".avro": true, ".parquet": true,
	// Compiled/intermediate
	".beam": true, ".elc": true, ".rlib": true,
	// Coverage/profiling
	".coverage": true, ".prof": true, ".out": true,
	// Patches
	".patch": true, ".diff": true,
}

// fastIgnoreFilenames are specific filenames skipped in fast mode.
var fastIgnoreFilenames = map[string]bool{
	"LICENSE": true, "LICENSE.txt": true, "LICENSE.md": true, "LICENSE-MIT": true, "LICENSE-APACHE": true,
	"LICENCE": true, "LICENCE.txt": true, "LICENCE.md": true,
	"CHANGELOG": true, "CHANGELOG.md": true, "CHANGES.md": true,
	"HISTORY": true, "HISTORY.md": true,
	"AUTHORS": true, "AUTHORS.md": true, "CONTRIBUTORS": true, "CONTRIBUTORS.md": true,
	"CODEOWNERS": true,
	"go.sum":     true, "yarn.lock": true, "pnpm-lock.yaml": true, "Pipfile.lock": true,
	"poetry.lock": true, "Gemfile.lock": true, "Cargo.lock": true, "mix.lock": true,
	"flake.lock": true, "pubspec.lock": true, "composer.lock": true,
	"configure": true, "Makefile.in": true, "config.guess": true, "config.sub": true,
	"package-lock.json": true,
}

// fastIgnorePatterns are suffix/contains patterns skipped in fast mode.
var fastIgnorePatterns = []string{
	".d.ts",          // TypeScript declaration files
	".bundle.",       // bundled files
	".chunk.",        // code-split chunks
	".generated.",    // generated code
	".pb.go",         // protobuf generated Go
	"_pb2.py",        // protobuf generated Python
	".pb2.py",        // protobuf generated Python (alt)
	"_grpc.pb.go",    // gRPC generated Go
	"_string.go",     // stringer generated Go
	"mock_",          // mock files prefix
	"_mock.",         // mock files suffix
	"_test_helpers.", // test helpers
	".stories.",      // Storybook stories
	".spec.",         // spec/test files
	".test.",         // test files
}

// shouldSkipDir returns true if the directory should be skipped during discovery.
func shouldSkipDir(name, rel string, extraIgnore []string, mode IndexMode) bool {
	if IGNORE_PATTERNS[name] {
		return true
	}
	if mode == ModeFast {
		// Skip all dot-directories not already in IGNORE_PATTERNS
		if strings.HasPrefix(name, ".") {
			return true
		}
		if fastIgnoreDirs[name] {
			return true
		}
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

// shouldSkipFile returns true if the file should be skipped in fast mode.
func shouldSkipFile(name, path string, size int64, opts *Options) bool {
	// File size limit (both modes)
	if opts != nil && opts.MaxFileSize > 0 && size > opts.MaxFileSize {
		return true
	}
	if opts == nil || opts.Mode != ModeFast {
		return false
	}
	// Fast-mode filename filter
	if fastIgnoreFilenames[name] {
		return true
	}
	// Fast-mode suffix filter
	for suffix := range fastIgnoreSuffixes {
		if strings.HasSuffix(path, suffix) {
			return true
		}
	}
	// Fast-mode pattern filter (contains/suffix patterns)
	for _, pattern := range fastIgnorePatterns {
		if strings.Contains(path, pattern) {
			return true
		}
	}
	return false
}

// hasIgnoredSuffix returns true if path ends with any suffix in IGNORE_SUFFIXES.
func hasIgnoredSuffix(path string) bool {
	for suffix := range IGNORE_SUFFIXES {
		if strings.HasSuffix(path, suffix) {
			return true
		}
	}
	return false
}

// Objective-C markers adapted from GitHub Linguist heuristics.yml
var objcMarkers = regexp.MustCompile(`@interface|@implementation|@protocol|@property|#import\s+[<"]|@selector|@encode|@synthesize|@dynamic`)

// MATLAB markers adapted from GitHub Linguist heuristics.yml
var matlabMarkers = regexp.MustCompile(`(?m)^\s*function\b|^\s*classdef\b|^\s*%%|^\s*%[^{]`)

// Magma markers — semicolons after end-blocks and procedure/intrinsic keywords are unique to Magma
var magmaMarkers = regexp.MustCompile(`(?m)end\s+(function|procedure|intrinsic)\s*;|\bintrinsic\s+\w+\s*\(|\bprocedure\s+\w+\s*\(|end\s+if\s*;|end\s+for\s*;|end\s+while\s*;`)

// disambiguateM determines whether a .m file is MATLAB, Objective-C, or Magma.
// Returns the detected language and true, or ("", false) if undetermined.
func disambiguateM(path string) (lang.Language, bool) {
	f, err := os.Open(path)
	if err != nil {
		return lang.MATLAB, true // default to MATLAB on error
	}
	defer f.Close()

	// Read first 4KB for heuristics
	buf := make([]byte, 4096)
	n, err := io.ReadAtLeast(f, buf, 1)
	if err != nil && n == 0 {
		return lang.MATLAB, true
	}
	head := string(buf[:n])

	if objcMarkers.MatchString(head) {
		return lang.ObjectiveC, true
	}
	// Check Magma before MATLAB — both use 'function' keyword but Magma has 'end function;'
	if magmaMarkers.MatchString(head) {
		return lang.Magma, true
	}
	if matlabMarkers.MatchString(head) {
		return lang.MATLAB, true
	}
	// Default to MATLAB (more common for .m in scientific codebases)
	return lang.MATLAB, true
}

// classifyFile checks if a file is a supported source file and returns its FileInfo.
// Returns nil if the file should be skipped.
func classifyFile(path, rel string, info os.FileInfo, opts *Options) *FileInfo {
	if hasIgnoredSuffix(path) {
		return nil
	}
	if shouldSkipFile(info.Name(), path, info.Size(), opts) {
		return nil
	}

	ext := filepath.Ext(path)

	// .m files are ambiguous: MATLAB vs Objective-C
	if ext == ".m" {
		ml, ok := disambiguateM(path)
		if ok {
			return &FileInfo{
				Path:     path,
				RelPath:  filepath.ToSlash(rel),
				Language: ml,
				Size:     info.Size(),
			}
		}
	}

	l, ok := lang.LanguageForExtension(ext)
	if !ok {
		l, ok = lang.LanguageForFilename(info.Name())
	}
	if !ok {
		return nil
	}

	if l == lang.JSON && isIgnoredJSON(info.Name()) {
		return nil
	}
	if l == lang.JSON && info.Size() > 100*1024 {
		slog.Warn("discover.skip_large_json", "path", rel, "size", info.Size())
		return nil
	}

	return &FileInfo{
		Path:     path,
		RelPath:  filepath.ToSlash(rel),
		Language: l,
		Size:     info.Size(),
	}
}

// Discover walks a repository and returns all source files.
func Discover(ctx context.Context, repoPath string, opts *Options) ([]FileInfo, error) {
	repoPath, err := filepath.Abs(repoPath)
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var extraIgnore []string
	if opts != nil && opts.IgnoreFile != "" {
		extraIgnore, _ = loadIgnoreFile(opts.IgnoreFile)
	} else {
		ignPath := filepath.Join(repoPath, ".cgrignore")
		extraIgnore, _ = loadIgnoreFile(ignPath)
	}

	var files []FileInfo

	mode := ModeFull
	if opts != nil && opts.Mode != "" {
		mode = opts.Mode
	}

	err = filepath.Walk(repoPath, func(path string, info os.FileInfo, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return filepath.SkipDir
		}

		rel, _ := filepath.Rel(repoPath, path)

		if info.IsDir() {
			if shouldSkipDir(info.Name(), rel, extraIgnore, mode) {
				return filepath.SkipDir
			}
			return nil
		}

		if fi := classifyFile(path, rel, info, opts); fi != nil {
			files = append(files, *fi)
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
