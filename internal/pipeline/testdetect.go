package pipeline

import (
	"path/filepath"
	"strings"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
)

// isTestFile returns true if the file path indicates a test file for the given language.
func isTestFile(relPath string, language lang.Language) bool {
	base := filepath.Base(relPath)
	dir := filepath.Dir(relPath)

	switch language {
	case lang.Go:
		return strings.HasSuffix(base, "_test.go")

	case lang.Python:
		if strings.HasPrefix(base, "test_") || strings.HasSuffix(base, "_test.py") {
			return true
		}
		return containsTestDir(dir, "__tests__", "tests")

	case lang.JavaScript, lang.TypeScript, lang.TSX:
		noExt := strings.TrimSuffix(base, filepath.Ext(base))
		// Handle double-extensions like .test.ts, .spec.tsx
		if strings.HasSuffix(noExt, ".test") || strings.HasSuffix(noExt, ".spec") {
			return true
		}
		return containsTestDir(dir, "__tests__")

	case lang.Java:
		if strings.HasSuffix(base, "Test.java") || strings.HasSuffix(base, "Tests.java") {
			return true
		}
		return containsTestDir(dir, "src/test")

	case lang.Rust:
		if strings.HasSuffix(base, "_test.rs") {
			return true
		}
		return containsTestDir(dir, "tests")

	case lang.CPP:
		noExt := strings.TrimSuffix(base, filepath.Ext(base))
		if strings.HasSuffix(noExt, "_test") {
			return true
		}
		return containsTestDir(dir, "test", "tests")

	case lang.PHP:
		if strings.HasSuffix(base, "Test.php") {
			return true
		}
		return containsTestDir(dir, "tests")

	case lang.Scala:
		noExt := strings.TrimSuffix(base, filepath.Ext(base))
		if strings.HasSuffix(noExt, "Spec") || strings.HasSuffix(noExt, "Test") {
			return true
		}
		return containsTestDir(dir, "src/test")

	case lang.CSharp:
		noExt := strings.TrimSuffix(base, filepath.Ext(base))
		if strings.HasSuffix(noExt, "Test") || strings.HasSuffix(noExt, "Tests") {
			return true
		}
		return containsTestDir(dir, "Tests", "tests")
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

	case lang.PHP:
		return strings.HasPrefix(funcName, "test") ||
			strings.HasPrefix(funcName, "Test")

	case lang.Scala:
		return strings.HasPrefix(funcName, "test") ||
			strings.HasSuffix(funcName, "Spec")

	case lang.CSharp:
		return strings.HasPrefix(funcName, "Test") ||
			strings.HasSuffix(funcName, "Test")
	}

	return false
}
