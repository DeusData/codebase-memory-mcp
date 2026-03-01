package pipeline

import (
	"path/filepath"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
)

// testFilePattern defines how to detect test files for a language.
type testFilePattern struct {
	// suffixes on the base filename (e.g., "_test.go")
	suffixes []string
	// prefixes on the base filename (e.g., "test_")
	prefixes []string
	// stripExtSuffixes: suffixes checked on the base name after stripping ext (e.g., ".test", ".spec")
	stripExtSuffixes []string
	// testDirs: directory patterns that indicate test files
	testDirs []string
}

// testFilePatterns maps languages to their test file detection patterns.
var testFilePatterns = map[lang.Language]testFilePattern{
	lang.Go: {suffixes: []string{"_test.go"}},
	lang.Python: {
		prefixes: []string{"test_"},
		suffixes: []string{"_test.py"},
		testDirs: []string{"__tests__", "tests"},
	},
	lang.JavaScript: {
		stripExtSuffixes: []string{".test", ".spec"},
		testDirs:         []string{"__tests__"},
	},
	lang.TypeScript: {
		stripExtSuffixes: []string{".test", ".spec"},
		testDirs:         []string{"__tests__"},
	},
	lang.TSX: {
		stripExtSuffixes: []string{".test", ".spec"},
		testDirs:         []string{"__tests__"},
	},
	lang.Java: {
		suffixes: []string{"Test.java", "Tests.java"},
		testDirs: []string{"src/test"},
	},
	lang.Rust: {
		suffixes: []string{"_test.rs"},
		testDirs: []string{"tests"},
	},
	lang.CPP: {
		stripExtSuffixes: []string{"_test"},
		testDirs:         []string{"test", "tests"},
	},
	lang.PHP: {
		suffixes: []string{"Test.php"},
		testDirs: []string{"tests"},
	},
	lang.Scala: {
		stripExtSuffixes: []string{"Spec", "Test"},
		testDirs:         []string{"src/test"},
	},
	lang.CSharp: {
		stripExtSuffixes: []string{"Test", "Tests"},
		testDirs:         []string{"Tests", "tests"},
	},
	lang.Kotlin: {
		stripExtSuffixes: []string{"Test", "Tests", "Spec"},
		testDirs:         []string{"src/test"},
	},
	lang.Lua: {
		suffixes: []string{"_test.lua", "_spec.lua"},
		prefixes: []string{"test_"},
		testDirs: []string{"spec"},
	},
}

// isTestFile returns true if the file path indicates a test file for the given language.
func isTestFile(relPath string, language lang.Language) bool {
	pattern, ok := testFilePatterns[language]
	if !ok {
		return false
	}

	base := filepath.Base(relPath)

	for _, s := range pattern.suffixes {
		if strings.HasSuffix(base, s) {
			return true
		}
	}
	for _, p := range pattern.prefixes {
		if strings.HasPrefix(base, p) {
			return true
		}
	}
	if len(pattern.stripExtSuffixes) > 0 {
		noExt := strings.TrimSuffix(base, filepath.Ext(base))
		for _, s := range pattern.stripExtSuffixes {
			if strings.HasSuffix(noExt, s) {
				return true
			}
		}
	}
	if len(pattern.testDirs) > 0 {
		return containsTestDir(filepath.Dir(relPath), pattern.testDirs...)
	}
	return false
}

// containsTestDir returns true if any segment of dir matches one of the patterns.
func containsTestDir(dir string, patterns ...string) bool {
	normalised := filepath.ToSlash(dir)
	for _, p := range patterns {
		if strings.Contains(normalised, p+"/") || strings.HasSuffix(normalised, p) {
			return true
		}
	}
	return false
}

// isTestFunction returns true if the function name indicates a test entry point
// (as opposed to a test helper). Used by passTests to gate TESTS edge creation.
func isTestFunction(funcName string, language lang.Language) bool {
	switch language {
	case lang.Go:
		return strings.HasPrefix(funcName, "Test") ||
			strings.HasPrefix(funcName, "Benchmark") ||
			strings.HasPrefix(funcName, "Example")

	case lang.Python:
		return strings.HasPrefix(funcName, "test_") ||
			strings.HasPrefix(funcName, "Test")

	case lang.JavaScript, lang.TypeScript, lang.TSX:
		// Jest/Vitest test entry functions
		switch funcName {
		case "describe", "it", "test", "beforeAll", "afterAll", "beforeEach", "afterEach":
			return true
		}
		return false

	case lang.Java:
		// JUnit test methods often annotated; name heuristic as fallback
		return strings.HasPrefix(funcName, "test") ||
			strings.HasSuffix(funcName, "Test")

	case lang.Rust:
		return strings.HasPrefix(funcName, "test_") ||
			strings.HasPrefix(funcName, "Test")

	case lang.CPP:
		return strings.HasPrefix(funcName, "Test") ||
			strings.HasPrefix(funcName, "test_")

	case lang.PHP:
		return strings.HasPrefix(funcName, "test") ||
			strings.HasPrefix(funcName, "Test")

	case lang.Scala:
		return strings.HasPrefix(funcName, "test") ||
			strings.HasSuffix(funcName, "Spec")

	case lang.CSharp:
		return strings.HasPrefix(funcName, "Test") ||
			strings.HasSuffix(funcName, "Test")

	case lang.Kotlin:
		return strings.HasPrefix(funcName, "test") ||
			strings.HasSuffix(funcName, "Test")

	case lang.Lua:
		return strings.HasPrefix(funcName, "test_") ||
			strings.HasPrefix(funcName, "test")
	}

	return false
}
