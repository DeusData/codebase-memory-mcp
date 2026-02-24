package pipeline

import (
	"testing"
	"unsafe"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
	tree_sitter_go "github.com/tree-sitter/tree-sitter-go/bindings/go"
	tree_sitter_java "github.com/tree-sitter/tree-sitter-java/bindings/go"
	tree_sitter_javascript "github.com/tree-sitter/tree-sitter-javascript/bindings/go"
	tree_sitter_php "github.com/tree-sitter/tree-sitter-php/bindings/go"
	tree_sitter_python "github.com/tree-sitter/tree-sitter-python/bindings/go"
	tree_sitter_rust "github.com/tree-sitter/tree-sitter-rust/bindings/go"
	tree_sitter_scala "github.com/tree-sitter/tree-sitter-scala/bindings/go"

	tree_sitter_cpp "github.com/tree-sitter/tree-sitter-cpp/bindings/go"
	tree_sitter_lua "github.com/tree-sitter-grammars/tree-sitter-lua/bindings/go"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
)

func parseSource(t *testing.T, language lang.Language, code string) (*tree_sitter.Tree, []byte) {
	t.Helper()

	var tsLang *tree_sitter.Language
	switch language {
	case lang.Python:
		tsLang = tree_sitter.NewLanguage(unsafe.Pointer(tree_sitter_python.Language()))
	case lang.Go:
		tsLang = tree_sitter.NewLanguage(unsafe.Pointer(tree_sitter_go.Language()))
	case lang.JavaScript:
		tsLang = tree_sitter.NewLanguage(unsafe.Pointer(tree_sitter_javascript.Language()))
	case lang.Rust:
		tsLang = tree_sitter.NewLanguage(unsafe.Pointer(tree_sitter_rust.Language()))
	case lang.Java:
		tsLang = tree_sitter.NewLanguage(unsafe.Pointer(tree_sitter_java.Language()))
	case lang.PHP:
		tsLang = tree_sitter.NewLanguage(unsafe.Pointer(tree_sitter_php.LanguagePHPOnly()))
	case lang.Scala:
		tsLang = tree_sitter.NewLanguage(unsafe.Pointer(tree_sitter_scala.Language()))
	case lang.CPP:
		tsLang = tree_sitter.NewLanguage(unsafe.Pointer(tree_sitter_cpp.Language()))
	case lang.Lua:
		tsLang = tree_sitter.NewLanguage(unsafe.Pointer(tree_sitter_lua.Language()))
	default:
		t.Fatalf("unsupported language: %s", language)
	}

	p := tree_sitter.NewParser()
	defer p.Close()
	if err := p.SetLanguage(tsLang); err != nil {
		t.Fatal(err)
	}
	source := []byte(code)
	tree := p.Parse(source, nil)
	return tree, source
}

func TestResolvePythonFString(t *testing.T) {
	code := `BASE_URL = "https://example.com"
URL = f"{BASE_URL}/notify-failure"
CONCAT = BASE_URL + "/api/orders"
`
	tree, source := parseSource(t, lang.Python, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.Python)

	assertSymbol(t, symbols, "BASE_URL", "https://example.com")
	assertSymbol(t, symbols, "URL", "https://example.com/notify-failure")
	assertSymbol(t, symbols, "CONCAT", "https://example.com/api/orders")
}

func TestResolvePythonChained(t *testing.T) {
	// 3-level chaining: A → B → C
	code := `HOST = "https://api.example.com"
BASE = f"{HOST}/v1"
ENDPOINT = f"{BASE}/orders"
`
	tree, source := parseSource(t, lang.Python, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.Python)

	assertSymbol(t, symbols, "HOST", "https://api.example.com")
	assertSymbol(t, symbols, "BASE", "https://api.example.com/v1")
	assertSymbol(t, symbols, "ENDPOINT", "https://api.example.com/v1/orders")
}

func TestResolveGoConcat(t *testing.T) {
	code := `package main
const baseURL = "https://example.com"
var fullURL = baseURL + "/api/orders"
`
	tree, source := parseSource(t, lang.Go, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.Go)

	assertSymbol(t, symbols, "baseURL", "https://example.com")
	assertSymbol(t, symbols, "fullURL", "https://example.com/api/orders")
}

func TestResolveGoSprintf(t *testing.T) {
	code := `package main
const baseURL = "https://example.com"
var url = fmt.Sprintf("%s/api/items", baseURL)
`
	tree, source := parseSource(t, lang.Go, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.Go)

	assertSymbol(t, symbols, "baseURL", "https://example.com")
	assertSymbol(t, symbols, "url", "https://example.com/api/items")
}

func TestResolveJSTemplate(t *testing.T) {
	code := "const baseUrl = \"https://example.com\";\nconst url = `${baseUrl}/api/orders`;\nconst concat = baseUrl + \"/api/orders\";\n"
	tree, source := parseSource(t, lang.JavaScript, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.JavaScript)

	assertSymbol(t, symbols, "baseUrl", "https://example.com")
	assertSymbol(t, symbols, "url", "https://example.com/api/orders")
	assertSymbol(t, symbols, "concat", "https://example.com/api/orders")
}

func TestResolveRustFormatMacro(t *testing.T) {
	code := `const BASE_URL: &str = "https://example.com";
let url = format!("{}/api/orders", BASE_URL);
let concat = String::from(BASE_URL) + "/api/orders";
`
	tree, source := parseSource(t, lang.Rust, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.Rust)

	assertSymbol(t, symbols, "BASE_URL", "https://example.com")
	assertSymbol(t, symbols, "url", "https://example.com/api/orders")
	// concat: String::from(BASE_URL) is a call_expression with "String::from", not "fmt.Sprintf"
	// The resolver sees binary_expression: call_expression + string_literal
	// call_expression resolves to "" (not fmt.Sprintf), so concat = "" + "/api/orders"
	assertSymbol(t, symbols, "concat", "/api/orders")
}

func TestResolveJavaConcat(t *testing.T) {
	code := `class Main {
    static final String BASE_URL = "https://example.com";
    static final String URL = BASE_URL + "/api/orders";
}`
	tree, source := parseSource(t, lang.Java, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.Java)

	assertSymbol(t, symbols, "BASE_URL", "https://example.com")
	assertSymbol(t, symbols, "URL", "https://example.com/api/orders")
}

func TestResolvePHPInterpolation(t *testing.T) {
	code := `<?php
$baseUrl = "https://example.com";
$url = "{$baseUrl}/api/orders";
$concat = $baseUrl . "/api/orders";
`
	tree, source := parseSource(t, lang.PHP, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.PHP)

	assertSymbol(t, symbols, "baseUrl", "https://example.com")
	assertSymbol(t, symbols, "url", "https://example.com/api/orders")
	assertSymbol(t, symbols, "concat", "https://example.com/api/orders")
}

func TestResolveScalaInterpolation(t *testing.T) {
	code := `val baseUrl = "https://example.com"
val url = s"${baseUrl}/api/orders"
val concat = baseUrl + "/api/orders"
`
	tree, source := parseSource(t, lang.Scala, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.Scala)

	assertSymbol(t, symbols, "baseUrl", "https://example.com")
	assertSymbol(t, symbols, "url", "https://example.com/api/orders")
	assertSymbol(t, symbols, "concat", "https://example.com/api/orders")
}

func TestResolveUnknownVariable(t *testing.T) {
	// When a variable can't be resolved, it should emit {}
	code := `URL = f"{UNKNOWN_VAR}/api/orders"
`
	tree, source := parseSource(t, lang.Python, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.Python)

	assertSymbol(t, symbols, "URL", "{}/api/orders")
}

func TestResolveNonStringAssignment(t *testing.T) {
	// Integer/boolean assignments should not produce entries
	code := `MAX_RETRIES = 3
DEBUG = True
NAME = "hello"
`
	tree, source := parseSource(t, lang.Python, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.Python)

	if _, ok := symbols["MAX_RETRIES"]; ok {
		t.Error("MAX_RETRIES should not be in symbols")
	}
	if _, ok := symbols["DEBUG"]; ok {
		t.Error("DEBUG should not be in symbols")
	}
	assertSymbol(t, symbols, "NAME", "hello")
}

func TestResolveCPPDefineAndConcat(t *testing.T) {
	code := `#define BASE_URL "https://example.com"
const std::string fullUrl = BASE_URL + "/api/orders";
`
	// Note: C++ #define string concat isn't valid C++ (can't + on string literals in preprocessor),
	// but tree-sitter parses it structurally and we resolve the intent.
	tree, source := parseSource(t, lang.CPP, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.CPP)

	assertSymbol(t, symbols, "BASE_URL", "https://example.com")
	// The declaration uses binary_expression with + — BASE_URL resolves from symbol table
	assertSymbol(t, symbols, "fullUrl", "https://example.com/api/orders")
}

func TestResolveLuaConcatAndFormat(t *testing.T) {
	code := `local base_url = "https://example.com"
local url = base_url .. "/api/orders"
local formatted = string.format("%s/api/items", base_url)
`
	tree, source := parseSource(t, lang.Lua, code)
	defer tree.Close()

	symbols := resolveModuleStrings(tree.RootNode(), source, lang.Lua)

	assertSymbol(t, symbols, "base_url", "https://example.com")
	assertSymbol(t, symbols, "url", "https://example.com/api/orders")
	assertSymbol(t, symbols, "formatted", "https://example.com/api/items")
}

func assertSymbol(t *testing.T, symbols map[string]string, name, want string) {
	t.Helper()
	got, ok := symbols[name]
	if !ok {
		t.Errorf("symbol %q not found in resolved symbols: %v", name, symbols)
		return
	}
	if got != want {
		t.Errorf("symbol %q = %q, want %q", name, got, want)
	}
}
