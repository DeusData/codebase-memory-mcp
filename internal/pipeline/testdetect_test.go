package pipeline

import (
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
)

func TestIsTestFileAllLanguages(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		testPath string // should return true
		srcPath  string // should return false
	}{
		{"Go", lang.Go, "foo_test.go", "foo.go"},
		{"Python", lang.Python, "test_handler.py", "handler.py"},
		{"JavaScript", lang.JavaScript, "handler.test.js", "handler.js"},
		{"TypeScript", lang.TypeScript, "handler.spec.ts", "handler.ts"},
		{"TSX", lang.TSX, "Component.test.tsx", "Component.tsx"},
		{"Java", lang.Java, "OrderTest.java", "Order.java"},
		{"Rust", lang.Rust, "handler_test.rs", "handler.rs"},
		{"CPP", lang.CPP, "handler_test.cpp", "handler.cpp"},
		{"CSharp", lang.CSharp, "OrderTest.cs", "Order.cs"},
		{"PHP", lang.PHP, "OrderTest.php", "Order.php"},
		{"Scala", lang.Scala, "OrderSpec.scala", "Order.scala"},
		{"Kotlin", lang.Kotlin, "OrderTest.kt", "Order.kt"},
		{"Lua", lang.Lua, "handler_test.lua", "handler.lua"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !isTestFile(tt.testPath, tt.language) {
				t.Errorf("isTestFile(%q, %s) = false, want true", tt.testPath, tt.language)
			}
			if isTestFile(tt.srcPath, tt.language) {
				t.Errorf("isTestFile(%q, %s) = true, want false", tt.srcPath, tt.language)
			}
		})
	}
}

func TestIsTestFunctionAllLanguages(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		testFunc string // should return true
		srcFunc  string // should return false
	}{
		{"Go", lang.Go, "TestCreate", "create"},
		{"Python", lang.Python, "test_create", "create"},
		{"JavaScript", lang.JavaScript, "test", "create"},
		{"TypeScript", lang.TypeScript, "describe", "create"},
		{"TSX", lang.TSX, "it", "create"},
		{"Java", lang.Java, "testCreate", "create"},
		{"Rust", lang.Rust, "test_create", "create"},
		{"CPP", lang.CPP, "TestCreate", "create"},
		{"CSharp", lang.CSharp, "TestCreate", "create"},
		{"PHP", lang.PHP, "testCreate", "create"},
		{"Scala", lang.Scala, "testCreate", "create"},
		{"Kotlin", lang.Kotlin, "testCreate", "create"},
		{"Lua", lang.Lua, "test_create", "create"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !isTestFunction(tt.testFunc, tt.language) {
				t.Errorf("isTestFunction(%q, %s) = false, want true", tt.testFunc, tt.language)
			}
			if isTestFunction(tt.srcFunc, tt.language) {
				t.Errorf("isTestFunction(%q, %s) = true, want false", tt.srcFunc, tt.language)
			}
		})
	}
}
