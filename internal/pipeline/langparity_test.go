package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// findFirstNodeByKind walks the AST and returns the first node matching any of the given kinds.
func findFirstNodeByKind(root *tree_sitter.Node, kinds ...string) *tree_sitter.Node {
	kindSet := make(map[string]bool, len(kinds))
	for _, k := range kinds {
		kindSet[k] = true
	}
	var found *tree_sitter.Node
	parser.Walk(root, func(n *tree_sitter.Node) bool {
		if found != nil {
			return false
		}
		if kindSet[n.Kind()] {
			found = n
			return false
		}
		return true
	})
	return found
}

// findParamsNode finds the parameter list node for a function, handling different
// tree-sitter grammar structures across languages.
func findParamsNode(funcNode *tree_sitter.Node, language lang.Language) *tree_sitter.Node {
	// Try standard field names first
	for _, f := range []string{"parameters", "formal_parameters", "value_parameters"} {
		if n := funcNode.ChildByFieldName(f); n != nil {
			return n
		}
	}
	// C/C++/ObjC: params are nested in function_declarator
	if language == lang.CPP || language == lang.C || language == lang.ObjectiveC {
		if decl := funcNode.ChildByFieldName("declarator"); decl != nil {
			if params := decl.ChildByFieldName("parameters"); params != nil {
				return params
			}
		}
	}
	// Zig: parameters child (no field name)
	if language == lang.Zig {
		return findFirstNodeByKind(funcNode, "parameters")
	}
	// Kotlin: function_value_parameters has no field name
	if language == lang.Kotlin {
		return findFirstNodeByKind(funcNode, "function_value_parameters")
	}
	return nil
}

// findReturnTypeNode finds the return type node for a function, handling different
// tree-sitter grammar structures across languages.
func findReturnTypeNode(funcNode *tree_sitter.Node, language lang.Language) *tree_sitter.Node {
	// Try standard field names
	for _, f := range []string{"return_type", "result", "type", "returns"} {
		if n := funcNode.ChildByFieldName(f); n != nil {
			return n
		}
	}
	// C++: return type is in the "type" field of function_definition
	if language == lang.CPP {
		if n := funcNode.ChildByFieldName("type"); n != nil {
			return n
		}
	}
	// Kotlin: return type (user_type) is an unnamed child after ":"
	if language == lang.Kotlin {
		return findFirstNodeByKind(funcNode, "user_type")
	}
	return nil
}

// writeLangTestFile creates a file with the given content inside dir.
func writeLangTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// --- Test 1: Complexity ---

func TestComplexityAllLanguages(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		source   string
	}{
		{"Python", lang.Python, "def f():\n    if x:\n        pass\n    for i in range(10):\n        pass\n"},
		{"Go", lang.Go, "package main\nfunc f() {\n\tif x {\n\t}\n\tfor i := 0; i < 10; i++ {\n\t}\n}\n"},
		{"JavaScript", lang.JavaScript, "function f() {\n\tif (x) {}\n\tfor (var i = 0; i < 10; i++) {}\n}\n"},
		{"TypeScript", lang.TypeScript, "function f() {\n\tif (x) {}\n\tfor (let i = 0; i < 10; i++) {}\n}\n"},
		{"TSX", lang.TSX, "function f() {\n\tif (x) {}\n\tfor (let i = 0; i < 10; i++) {}\n}\n"},
		{"Java", lang.Java, "class A {\n\tvoid f() {\n\t\tif (x) {}\n\t\tfor (int i = 0; i < 10; i++) {}\n\t}\n}\n"},
		{"Rust", lang.Rust, "fn f() {\n\tif x {\n\t}\n\tfor i in v {\n\t}\n}\n"},
		{"CPP", lang.CPP, "void f() {\n\tif (x) {}\n\tfor (int i = 0; i < 10; i++) {}\n}\n"},
		{"CSharp", lang.CSharp, "class A {\n\tvoid F() {\n\t\tif (x) {}\n\t\tfor (int i = 0; i < 10; i++) {}\n\t}\n}\n"},
		{"PHP", lang.PHP, "<?php\nfunction f() {\n\tif ($x) {}\n\tforeach ($a as $b) {}\n}\n"},
		{"Lua", lang.Lua, "function f()\n\tif x then\n\tend\n\tfor i = 1, 10 do\n\tend\nend\n"},
		{"Scala", lang.Scala, "object A {\n\tdef f(): Unit = {\n\t\tif (x) {}\n\t\tfor (i <- r) {}\n\t}\n}\n"},
		{"Kotlin", lang.Kotlin, "fun f() {\n\tif (true) {}\n\tfor (i in 1..10) {}\n}\n"},
		// New languages
		{"Ruby", lang.Ruby, "def f(x)\n  if x > 0\n    x\n  elsif x == 0\n    0\n  end\nend\n"},
		{"C", lang.C, "int f(int x) {\n\tif (x > 0) return x;\n\tfor (int i = 0; i < 10; i++) {}\n}\n"},
		{"Bash", lang.Bash, "f() {\n\tif [ -f file ]; then\n\t\techo ok\n\telif [ -d dir ]; then\n\t\techo dir\n\tfi\n}\n"},
		{"Zig", lang.Zig, "fn f(x: i32) i32 {\n\tif (x > 0) return x;\n\tfor (0..10) |i| { _ = i; }\n}\n"},
		{"ObjectiveC", lang.ObjectiveC, "void f(int x) {\n\tif (x > 0) {}\n\tfor (int i = 0; i < 10; i++) {}\n}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := lang.ForLanguage(tt.language)
			if spec == nil {
				t.Fatalf("no spec for %s", tt.language)
			}
			if len(spec.BranchingNodeTypes) == 0 {
				t.Fatalf("BranchingNodeTypes is empty for %s", tt.language)
			}

			tree, _ := parseSource(t, tt.language, tt.source)
			defer tree.Close()

			funcNode := findFirstNodeByKind(tree.RootNode(), spec.FunctionNodeTypes...)
			if funcNode == nil {
				t.Fatalf("no function node found in AST for %s", tt.language)
			}

			complexity := countBranchingNodes(funcNode, spec.BranchingNodeTypes)
			if complexity < 2 {
				t.Errorf("complexity = %d, want >= 2 for %s", complexity, tt.language)
			}
		})
	}
}

// --- Test 2: Param Type Extraction ---

func TestParamTypeExtractionAllLanguages(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		source   string
		wantType string
	}{
		{"Python", lang.Python, "def f(cfg: Config):\n    pass\n", "Config"},
		{"Go", lang.Go, "package main\nfunc f(cfg Config) {}\n", "Config"},
		{"TypeScript", lang.TypeScript, "function f(cfg: Config) {}\n", "Config"},
		{"TSX", lang.TSX, "function f(cfg: Config) {}\n", "Config"},
		{"Java", lang.Java, "class A {\n\tvoid f(Config cfg) {}\n}\n", "Config"},
		{"Rust", lang.Rust, "fn f(cfg: Config) {}\n", "Config"},
		{"CPP", lang.CPP, "void f(Config cfg) {}\n", "Config"},
		{"CSharp", lang.CSharp, "class A {\n\tvoid F(Config cfg) {}\n}\n", "Config"},
		{"PHP", lang.PHP, "<?php\nfunction f(Config $cfg) {}\n", "Config"},
		{"Scala", lang.Scala, "object A {\n\tdef f(cfg: Config): Unit = {}\n}\n", "Config"},
		{"Kotlin", lang.Kotlin, "fun f(cfg: Config) {}\n", "Config"},
		// New languages
		{"C", lang.C, "void f(Config cfg) {}\n", "Config"},
		{"Zig", lang.Zig, "fn f(cfg: Config) void {}\n", "Config"},
		{"ObjectiveC", lang.ObjectiveC, "void f(Config cfg) {}\n", "Config"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := lang.ForLanguage(tt.language)
			if spec == nil {
				t.Fatalf("no spec for %s", tt.language)
			}

			tree, src := parseSource(t, tt.language, tt.source)
			defer tree.Close()

			funcNode := findFirstNodeByKind(tree.RootNode(), spec.FunctionNodeTypes...)
			if funcNode == nil {
				t.Fatalf("no function node found in AST for %s", tt.language)
			}

			paramsNode := findParamsNode(funcNode, tt.language)
			if paramsNode == nil {
				t.Fatalf("no params node found for %s (func kind: %s)", tt.language, funcNode.Kind())
			}

			types := extractParamTypes(paramsNode, src, tt.language)
			found := false
			for _, tp := range types {
				if tp == tt.wantType {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("param types = %v, want to contain %q for %s", types, tt.wantType, tt.language)
			}
		})
	}
}

// --- Test 3: Return Type Extraction ---

func TestReturnTypeExtractionAllLanguages(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		source   string
		wantType string
	}{
		{"Python", lang.Python, "def f() -> Config:\n    pass\n", "Config"},
		{"Go", lang.Go, "package main\nfunc f() Config { return Config{} }\n", "Config"},
		{"TypeScript", lang.TypeScript, "function f(): Config { return {} as Config; }\n", "Config"},
		{"TSX", lang.TSX, "function f(): Config { return {} as Config; }\n", "Config"},
		{"Java", lang.Java, "class A {\n\tConfig f() { return null; }\n}\n", "Config"},
		{"Rust", lang.Rust, "fn f() -> Config { todo!() }\n", "Config"},
		{"CPP", lang.CPP, "Config f() { return Config(); }\n", "Config"},
		{"CSharp", lang.CSharp, "class A {\n\tConfig F() { return null; }\n}\n", "Config"},
		{"PHP", lang.PHP, "<?php\nfunction f(): Config { return new Config(); }\n", "Config"},
		{"Scala", lang.Scala, "object A {\n\tdef f(): Config = { null }\n}\n", "Config"},
		{"Kotlin", lang.Kotlin, "fun f(): Config { TODO() }\n", "Config"},
		{"ObjectiveC", lang.ObjectiveC, "Config f() { return (Config){}; }\n", "Config"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := lang.ForLanguage(tt.language)
			if spec == nil {
				t.Fatalf("no spec for %s", tt.language)
			}

			tree, src := parseSource(t, tt.language, tt.source)
			defer tree.Close()

			funcNode := findFirstNodeByKind(tree.RootNode(), spec.FunctionNodeTypes...)
			if funcNode == nil {
				t.Fatalf("no function node found for %s", tt.language)
			}

			retNode := findReturnTypeNode(funcNode, tt.language)
			if retNode == nil {
				t.Fatalf("no return type node found for %s (func kind: %s)", tt.language, funcNode.Kind())
			}

			types := extractReturnTypes(retNode, src, tt.language)
			found := false
			for _, tp := range types {
				if tp == tt.wantType {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("return types = %v, want to contain %q for %s", types, tt.wantType, tt.language)
			}
		})
	}
}

// --- Test 4: Base Class Extraction ---

func TestBaseClassExtractionAllLanguages(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		source   string
		wantBase string
	}{
		{"Python", lang.Python, "class Child(Parent):\n    pass\n", "Parent"},
		{"JavaScript", lang.JavaScript, "class Child extends Parent {}\n", "Parent"},
		{"TypeScript", lang.TypeScript, "class Child extends Parent {}\n", "Parent"},
		{"TSX", lang.TSX, "class Child extends Parent {}\n", "Parent"},
		{"Java", lang.Java, "class Child extends Parent {}\n", "Parent"},
		{"CPP", lang.CPP, "class Child : public Parent {};\n", "Parent"},
		{"CSharp", lang.CSharp, "class Child : Parent {}\n", "Parent"},
		{"PHP", lang.PHP, "<?php\nclass Child extends Parent {}\n", "Parent"},
		{"Scala", lang.Scala, "class Child extends Parent {}\n", "Parent"},
		{"Kotlin", lang.Kotlin, "class Child : Parent() {}\n", "Parent"},
		// New languages
		{"Ruby", lang.Ruby, "class Child < Parent\nend\n", "Parent"},
		{"ObjectiveC", lang.ObjectiveC, "@interface Child : Parent\n@end\n", "Parent"},
		{"Swift", lang.Swift, "class Child: Parent {\n}\n", "Parent"},
		{"Groovy", lang.Groovy, "class Child extends Parent {\n}\n", "Parent"},
		{"Dart", lang.Dart, "class Child extends Parent {\n}\n", "Parent"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := lang.ForLanguage(tt.language)
			if spec == nil {
				t.Fatalf("no spec for %s", tt.language)
			}

			tree, src := parseSource(t, tt.language, tt.source)
			defer tree.Close()

			classNode := findFirstNodeByKind(tree.RootNode(), spec.ClassNodeTypes...)
			if classNode == nil {
				t.Fatalf("no class node found for %s", tt.language)
			}

			bases := extractBaseClasses(classNode, src, tt.language)
			found := false
			for _, b := range bases {
				if b == tt.wantBase {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("base classes = %v, want to contain %q for %s", bases, tt.wantBase, tt.language)
			}
		})
	}
}

// --- Test 5: Decorator Extraction ---

func TestDecoratorExtractionAllLanguages(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		source   string
	}{
		{"Python", lang.Python, "@my_decorator\ndef f():\n    pass\n"},
		{"Java", lang.Java, "class A {\n\t@MyAnnotation\n\tvoid f() {}\n}\n"},
		{"TypeScript", lang.TypeScript, "class A {\n\t@MyDecorator\n\tf() {}\n}\n"},
		{"TSX", lang.TSX, "class A {\n\t@MyDecorator\n\tf() {}\n}\n"},
		{"CSharp", lang.CSharp, "class A {\n\t[MyAttribute]\n\tvoid F() {}\n}\n"},
		{"Kotlin", lang.Kotlin, "@MyAnnotation\nfun f() {}\n"},
		{"PHP", lang.PHP, "<?php\n#[MyAttribute]\nfunction f() {}\n"},
		{"Groovy", lang.Groovy, "@MyAnnotation\ndef f() {}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := lang.ForLanguage(tt.language)
			if spec == nil {
				t.Fatalf("no spec for %s", tt.language)
			}

			tree, src := parseSource(t, tt.language, tt.source)
			defer tree.Close()

			funcNode := findFirstNodeByKind(tree.RootNode(), spec.FunctionNodeTypes...)
			if funcNode == nil {
				t.Fatalf("no function node found for %s", tt.language)
			}

			decorators := extractAllDecorators(funcNode, src, tt.language, spec)
			if len(decorators) == 0 {
				t.Errorf("no decorators found for %s", tt.language)
			}
		})
	}
}

// --- Test 6: Variable Extraction (integration) ---

func TestVariableExtractionAllLanguages(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		ext      string
		source   string
	}{
		{"Python", lang.Python, ".py", "API_URL = \"https://example.com\"\n"},
		{"Go", lang.Go, ".go", "package main\n\nvar apiURL = \"https://example.com\"\n"},
		{"JavaScript", lang.JavaScript, ".js", "const API_URL = \"https://example.com\";\n"},
		{"TypeScript", lang.TypeScript, ".ts", "const API_URL: string = \"https://example.com\";\n"},
		{"TSX", lang.TSX, ".tsx", "const API_URL: string = \"https://example.com\";\n"},
		{"Rust", lang.Rust, ".rs", "static API_URL: &str = \"https://example.com\";\n"},
		{"Java", lang.Java, ".java", "class Config {\n\tstatic final String API_URL = \"https://example.com\";\n}\n"},
		{"CPP", lang.CPP, ".cpp", "const std::string API_URL = \"https://example.com\";\n"},
		{"CSharp", lang.CSharp, ".cs", "class Config {\n\tconst string API_URL = \"https://example.com\";\n}\n"},
		{"PHP", lang.PHP, ".php", "<?php\n$API_URL = \"https://example.com\";\n"},
		{"Lua", lang.Lua, ".lua", "local API_URL = \"https://example.com\"\n"},
		{"Scala", lang.Scala, ".scala", "object Config {\n\tval apiUrl = \"https://example.com\"\n}\n"},
		{"Kotlin", lang.Kotlin, ".kt", "val apiUrl = \"https://example.com\"\n"},
		// New languages
		{"Ruby", lang.Ruby, ".rb", "API_URL = 'https://example.com'\n"},
		{"C", lang.C, ".c", "const char *API_URL = \"https://example.com\";\n"},
		{"Bash", lang.Bash, ".sh", "API_URL=\"https://example.com\"\n"},
		{"Zig", lang.Zig, ".zig", "const API_URL = \"https://example.com\";\n"},
		{"ObjectiveC", lang.ObjectiveC, ".m", "NSString *const API_URL = @\"https://example.com\";\n"},
		{"Groovy", lang.Groovy, ".groovy", "def API_URL = 'https://example.com'\n"},
		{"Perl", lang.Perl, ".pl", "my $API_URL = 'https://example.com';\n"},
		{"R", lang.R, ".R", "API_URL <- \"https://example.com\"\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := lang.ForLanguage(tt.language)
			if spec == nil {
				t.Fatalf("no spec for %s", tt.language)
			}
			if len(spec.VariableNodeTypes) == 0 {
				t.Fatalf("VariableNodeTypes is empty for %s", tt.language)
			}

			dir := t.TempDir()
			writeLangTestFile(t, filepath.Join(dir, "main"+tt.ext), tt.source)

			s, err := store.OpenMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			p := New(context.Background(), s, dir)
			if err := p.Run(); err != nil {
				t.Fatal(err)
			}

			vars, err := s.FindNodesByLabel(p.ProjectName, "Variable")
			if err != nil {
				t.Fatal(err)
			}
			if len(vars) == 0 {
				t.Errorf("no Variable nodes found for %s", tt.language)
			}
		})
	}
}

// --- Test 7: Throw Detection (integration) ---
// Note: Rust excluded (uses panic! macro, not throw) and Lua excluded (no throw keyword).

func TestThrowDetectionAllLanguages(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		ext      string
		source   string
	}{
		{"Python", lang.Python, ".py", "class MyError(Exception):\n    pass\n\ndef f():\n    raise MyError()\n"},
		{"JavaScript", lang.JavaScript, ".js", "class MyError extends Error {}\nfunction f() {\n\tthrow new MyError();\n}\n"},
		{"TypeScript", lang.TypeScript, ".ts", "class MyError extends Error {}\nfunction f() {\n\tthrow new MyError();\n}\n"},
		{"TSX", lang.TSX, ".tsx", "class MyError extends Error {}\nfunction f() {\n\tthrow new MyError();\n}\n"},
		{"Java", lang.Java, ".java", "class MyError extends RuntimeException {}\nclass A {\n\tvoid f() {\n\t\tthrow new MyError();\n\t}\n}\n"},
		{"CPP", lang.CPP, ".cpp", "class MyError {};\nvoid f() {\n\tthrow MyError();\n}\n"},
		{"CSharp", lang.CSharp, ".cs", "class MyError : Exception {}\nclass A {\n\tvoid F() {\n\t\tthrow new MyError();\n\t}\n}\n"},
		{"PHP", lang.PHP, ".php", "<?php\nclass MyError extends Exception {}\nfunction f() {\n\tthrow new MyError();\n}\n"},
		{"Scala", lang.Scala, ".scala", "class MyError extends RuntimeException\nobject A {\n\tdef f(): Unit = {\n\t\tthrow new MyError()\n\t}\n}\n"},
		{"Kotlin", lang.Kotlin, ".kt", "class MyError : RuntimeException()\nfun f() {\n\tthrow MyError()\n}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := lang.ForLanguage(tt.language)
			if spec == nil {
				t.Fatalf("no spec for %s", tt.language)
			}
			if len(spec.ThrowNodeTypes) == 0 {
				t.Fatalf("ThrowNodeTypes is empty for %s", tt.language)
			}

			dir := t.TempDir()
			writeLangTestFile(t, filepath.Join(dir, "main"+tt.ext), tt.source)

			s, err := store.OpenMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			p := New(context.Background(), s, dir)
			if err := p.Run(); err != nil {
				t.Fatal(err)
			}

			throwEdges, err := s.FindEdgesByType(p.ProjectName, "RAISES")
			if err != nil {
				t.Fatal(err)
			}
			throwsEdges, err2 := s.FindEdgesByType(p.ProjectName, "THROWS")
			if err2 != nil {
				t.Fatal(err2)
			}
			total := len(throwEdges) + len(throwsEdges)
			if total == 0 {
				t.Errorf("no THROWS/RAISES edges found for %s", tt.language)
			}
		})
	}
}

// --- Debug: CPP Throw pipeline dump ---

// --- Test 8: Env Access Detection ---

func TestEnvAccessDetectionAllLanguages(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
	}{
		{"Python", lang.Python},
		{"Go", lang.Go},
		{"JavaScript", lang.JavaScript},
		{"TypeScript", lang.TypeScript},
		{"TSX", lang.TSX},
		{"Java", lang.Java},
		{"Rust", lang.Rust},
		{"CPP", lang.CPP},
		{"CSharp", lang.CSharp},
		{"PHP", lang.PHP},
		{"Lua", lang.Lua},
		{"Scala", lang.Scala},
		{"Kotlin", lang.Kotlin},
		// New languages
		{"Ruby", lang.Ruby},
		{"C", lang.C},
		{"Zig", lang.Zig},
		{"Elixir", lang.Elixir},
		{"Haskell", lang.Haskell},
		{"OCaml", lang.OCaml},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := lang.ForLanguage(tt.language)
			if spec == nil {
				t.Fatalf("no spec for %s", tt.language)
			}
			if len(spec.EnvAccessFunctions) == 0 && len(spec.EnvAccessMemberPatterns) == 0 {
				t.Fatalf("EnvAccessFunctions and EnvAccessMemberPatterns both empty for %s", tt.language)
			}
		})
	}
}

// --- Test 10: Arrow Function Class Properties as Methods ---

func TestArrowFunctionClassPropertiesAsMethods(t *testing.T) {
	tests := []struct {
		name          string
		language      lang.Language
		ext           string
		source        string
		wantMethods   []string
		wantSignature map[string]string // method name → expected signature
	}{
		{
			"TypeScript_with_type",
			lang.TypeScript,
			".ts",
			"class UserController {\n\tpublic getUsers: RequestHandler = async (req, res) => {\n\t\tres.json([]);\n\t};\n\tpublic getUser: RequestHandler = async (req, res) => {\n\t\tres.json({});\n\t};\n}\n",
			[]string{"getUsers", "getUser"},
			map[string]string{"getUsers": "(req, res)", "getUser": "(req, res)"},
		},
		{
			"TypeScript_no_type",
			lang.TypeScript,
			".ts",
			"class A {\n\tgreet = () => 'hello';\n}\n",
			[]string{"greet"},
			map[string]string{"greet": "()"},
		},
		{
			"JavaScript",
			lang.JavaScript,
			".js",
			"class A {\n\tgreet = () => 'hello';\n\thandle = async (req) => {\n\t\treturn req;\n\t};\n}\n",
			[]string{"greet", "handle"},
			map[string]string{"greet": "()", "handle": "(req)"},
		},
		{
			"TSX",
			lang.TSX,
			".tsx",
			"class App {\n\trender = () => <div/>;\n}\n",
			[]string{"render"},
			map[string]string{"render": "()"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := lang.ForLanguage(tt.language)
			if spec == nil {
				t.Fatalf("no spec for %s", tt.language)
			}

			tree, src := parseSource(t, tt.language, tt.source)
			defer tree.Close()

			classNode := findFirstNodeByKind(tree.RootNode(), spec.ClassNodeTypes...)
			if classNode == nil {
				t.Fatalf("no class node found")
			}

			f := discover.FileInfo{
				Path:     "/test/test" + tt.ext,
				RelPath:  "test" + tt.ext,
				Language: tt.language,
			}

			result := &parseResult{}
			extractClassDef(classNode, src, f, "test-project", "test-project::test"+tt.ext, spec, result)

			methodNodes := collectNodesByLabel(result.Nodes, "Method")
			assertNodeNamesExist(t, methodNodes, tt.wantMethods)
			assertNodeSignatures(t, methodNodes, tt.wantSignature)
			assertEdgeCount(t, result.PendingEdges, "DEFINES_METHOD", len(tt.wantMethods))
		})
	}
}

// --- Test 11: Const Arrow Functions as Function (not Variable) ---

func TestConstArrowFunctionsAsFunction(t *testing.T) {
	tests := []struct {
		name      string
		language  lang.Language
		ext       string
		source    string
		wantFuncs []string
	}{
		{
			"JavaScript",
			lang.JavaScript,
			".js",
			"const greet = () => 'hello';\nconst handler = async (req) => { return req; };\nconst name = 'Alice';\n",
			[]string{"greet", "handler"},
		},
		{
			"TypeScript",
			lang.TypeScript,
			".ts",
			"const greet = (): string => 'hello';\nexport const handler = async (req: Request): Promise<Response> => { return new Response(); };\nconst config = { port: 8080 };\n",
			[]string{"greet", "handler"},
		},
		{
			"TSX",
			lang.TSX,
			".tsx",
			"const App = () => <div/>;\nconst title = 'Hello';\n",
			[]string{"App"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, p := runPipelineWithFile(t, "test"+tt.ext, tt.source)

			funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
			assertNodeNamesExist(t, funcs, tt.wantFuncs)

			// Verify they are NOT Variables
			vars, _ := s.FindNodesByLabel(p.ProjectName, "Variable")
			assertNodesNotLabeled(t, vars, tt.wantFuncs, "Variable")

			for _, v := range vars {
				t.Logf("Variable: %s", v.Name)
			}
		})
	}
}

// --- Test 12: Go Interface Methods and IMPLEMENTS Edges ---

func TestGoInterfaceImplements(t *testing.T) {
	src := `package main

type Handler interface {
	ServeHTTP(w ResponseWriter, r *Request)
}

type Mux struct{}

func (m *Mux) ServeHTTP(w ResponseWriter, r *Request) {}

type Router interface {
	Get(pattern string)
	Post(pattern string)
}

type DefaultRouter struct{}

func (d *DefaultRouter) Get(pattern string) {}
func (d *DefaultRouter) Post(pattern string) {}
`
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	// Check Interface nodes exist
	ifaces, _ := s.FindNodesByLabel(p.ProjectName, "Interface")
	ifaceNames := map[string]bool{}
	for _, i := range ifaces {
		ifaceNames[i.Name] = true
		t.Logf("Interface: %s", i.Name)
	}
	if !ifaceNames["Handler"] {
		t.Error("Handler interface not found")
	}
	if !ifaceNames["Router"] {
		t.Error("Router interface not found")
	}

	// Check that interface methods are extracted as Method nodes
	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
	methodNames := map[string]bool{}
	for _, m := range methods {
		methodNames[m.Name] = true
		t.Logf("Method: %s (qn=%s)", m.Name, m.QualifiedName)
	}
	if !methodNames["ServeHTTP"] {
		t.Error("ServeHTTP method not found")
	}
	if !methodNames["Get"] {
		t.Error("Get method not found")
	}

	// Check IMPLEMENTS edges
	for _, iface := range ifaces {
		edges, _ := s.FindEdgesByTargetAndType(iface.ID, "IMPLEMENTS")
		t.Logf("Interface %s: %d IMPLEMENTS edges", iface.Name, len(edges))
		if len(edges) == 0 {
			t.Errorf("expected IMPLEMENTS edges for %s, got 0", iface.Name)
		}
	}
}

// --- Test 13: Rust impl Trait for Struct → IMPLEMENTS edges + method extraction ---

func TestRustImplTraitImplements(t *testing.T) {
	src := `trait Handler {
    fn handle(&self) -> String;
}

struct MyHandler;

impl Handler for MyHandler {
    fn handle(&self) -> String {
        String::from("handled")
    }
}

struct AnotherHandler;

impl Handler for AnotherHandler {
    fn handle(&self) -> String {
        String::from("another")
    }
}
`
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.rs"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	// Check that trait is found as Interface
	ifaces, _ := s.FindNodesByLabel(p.ProjectName, "Interface")
	if len(ifaces) == 0 {
		t.Fatal("no Interface nodes found")
	}
	t.Logf("Interfaces: %d", len(ifaces))
	for _, i := range ifaces {
		t.Logf("  Interface: %s", i.Name)
	}

	// Check that structs are found as Class
	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	classNames := map[string]bool{}
	for _, c := range classes {
		classNames[c.Name] = true
		t.Logf("  Class: %s", c.Name)
	}
	if !classNames["MyHandler"] {
		t.Error("MyHandler class not found")
	}

	// Check methods are extracted and linked to the struct
	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
	t.Logf("Methods: %d", len(methods))
	for _, m := range methods {
		t.Logf("  Method: %s (qn=%s)", m.Name, m.QualifiedName)
	}
	if len(methods) < 3 { // trait method + 2 impl methods
		t.Errorf("expected at least 3 methods, got %d", len(methods))
	}

	// Check IMPLEMENTS edges
	for _, iface := range ifaces {
		edges, _ := s.FindEdgesByTargetAndType(iface.ID, "IMPLEMENTS")
		t.Logf("Interface %s: %d IMPLEMENTS edges", iface.Name, len(edges))
		if len(edges) < 2 {
			t.Errorf("expected at least 2 IMPLEMENTS edges for %s, got %d", iface.Name, len(edges))
		}
	}
}

// --- Test 14: Elixir Custom Extraction ---

func TestElixirCustomExtraction(t *testing.T) {
	src := `defmodule Greeter do
  def greet(name) do
    "Hello #{name}"
  end

  defp internal_work(x) do
    x * 2
  end
end
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "greeter.ex"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	// Check Module/Class node for defmodule
	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	classNames := map[string]bool{}
	for _, c := range classes {
		classNames[c.Name] = true
		t.Logf("Class: %s (qn=%s)", c.Name, c.QualifiedName)
	}
	if !classNames["Greeter"] {
		t.Error("Greeter module not found as Class")
	}

	// Check Function nodes for def/defp
	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
	allFuncNames := map[string]bool{}
	for _, f := range funcs {
		allFuncNames[f.Name] = true
		t.Logf("Function: %s (qn=%s)", f.Name, f.QualifiedName)
	}
	for _, m := range methods {
		allFuncNames[m.Name] = true
		t.Logf("Method: %s (qn=%s)", m.Name, m.QualifiedName)
	}
	if !allFuncNames["greet"] {
		t.Error("greet function not found")
	}
	if !allFuncNames["internal_work"] {
		t.Error("internal_work function not found")
	}

	// Check DEFINES edges (Elixir functions are extracted with DEFINES edges, not DEFINES_METHOD)
	defEdges, _ := s.FindEdgesByType(p.ProjectName, "DEFINES")
	if len(defEdges) < 3 {
		// At least: module→greet, module→internal_work, file→module
		t.Errorf("expected at least 3 DEFINES edges, got %d", len(defEdges))
	}
}

// --- Test 15: R Function Name Resolution ---

func TestRFunctionNameResolution(t *testing.T) {
	src := `mutate <- function(x) {
  x + 1
}

square <- function(n) {
  n * n
}
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "funcs.R"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	funcNames := map[string]bool{}
	for _, f := range funcs {
		funcNames[f.Name] = true
		t.Logf("Function: %s (qn=%s)", f.Name, f.QualifiedName)
	}

	if !funcNames["mutate"] {
		t.Error("mutate function not found (name should be from parent assignment)")
	}
	if !funcNames["square"] {
		t.Error("square function not found")
	}

	// Ensure "function" keyword is NOT used as a name
	if funcNames["function"] {
		t.Error("function keyword incorrectly used as function name")
	}
}

// --- Test 16: Haskell Function Extraction ---

func TestHaskellFunctionExtraction(t *testing.T) {
	src := `greet :: String -> String
greet name = "Hello " ++ name

add :: Int -> Int -> Int
add x y = x + y
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "Main.hs"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	funcNames := map[string]bool{}
	for _, f := range funcs {
		funcNames[f.Name] = true
		t.Logf("Function: %s (qn=%s)", f.Name, f.QualifiedName)
	}

	if !funcNames["greet"] {
		t.Error("greet function not found")
	}
	if !funcNames["add"] {
		t.Error("add function not found")
	}
}

// --- Test 17: OCaml Function Extraction ---

func TestOCamlFunctionExtraction(t *testing.T) {
	src := `let greet name = Printf.printf "Hello %s" name

let add x y = x + y
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "main.ml"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	funcNames := map[string]bool{}
	for _, f := range funcs {
		funcNames[f.Name] = true
		t.Logf("Function: %s (qn=%s)", f.Name, f.QualifiedName)
	}

	if !funcNames["greet"] {
		t.Error("greet function not found")
	}
	if !funcNames["add"] {
		t.Error("add function not found")
	}
}

// --- Test 18: Groovy Function and Class Extraction ---

func TestGroovyClassMethodExtraction(t *testing.T) {
	src := `class Calculator {
    int add(int a, int b) {
        return a + b
    }

    int multiply(int a, int b) {
        return a * b
    }
}
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "Calculator.groovy"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	classNames := map[string]bool{}
	for _, c := range classes {
		classNames[c.Name] = true
		t.Logf("Class: %s", c.Name)
	}
	if !classNames["Calculator"] {
		t.Error("Calculator class not found")
	}

	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
	methodNames := map[string]bool{}
	for _, m := range methods {
		methodNames[m.Name] = true
		t.Logf("Method: %s", m.Name)
	}
	if !methodNames["add"] {
		t.Error("add method not found")
	}
	if !methodNames["multiply"] {
		t.Error("multiply method not found")
	}

	defMethodEdges, _ := s.FindEdgesByType(p.ProjectName, "DEFINES_METHOD")
	if len(defMethodEdges) < 2 {
		t.Errorf("expected at least 2 DEFINES_METHOD edges, got %d", len(defMethodEdges))
	}
}

// --- Test 19: Dart Function and Class Extraction ---

func TestDartClassMethodExtraction(t *testing.T) {
	src := `class Counter {
  int _count = 0;

  void increment() {
    _count++;
  }

  int getCount() {
    return _count;
  }
}
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "counter.dart"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	classNames := map[string]bool{}
	for _, c := range classes {
		classNames[c.Name] = true
		t.Logf("Class: %s", c.Name)
	}
	if !classNames["Counter"] {
		t.Error("Counter class not found")
	}

	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
	methodNames := map[string]bool{}
	for _, m := range methods {
		methodNames[m.Name] = true
		t.Logf("Method: %s (qn=%s)", m.Name, m.QualifiedName)
	}
	if !methodNames["increment"] {
		t.Error("increment method not found")
	}
	if !methodNames["getCount"] {
		t.Error("getCount method not found")
	}
}

// --- Test 20: HCL Block Extraction ---

func TestHCLBlockExtraction(t *testing.T) {
	src := `resource "aws_instance" "web" {
  ami           = "ami-12345"
  instance_type = "t2.micro"
}

variable "region" {
  default = "us-east-1"
}
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "main.tf"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	for _, c := range classes {
		t.Logf("Class: %s (qn=%s)", c.Name, c.QualifiedName)
	}
	if len(classes) == 0 {
		t.Error("no Class nodes found for HCL resource blocks")
	}

	// HCL variable blocks are extracted as Variable nodes
	vars, _ := s.FindNodesByLabel(p.ProjectName, "Variable")
	for _, v := range vars {
		t.Logf("Variable: %s (qn=%s)", v.Name, v.QualifiedName)
	}

	// Total should be at least 2 (resource block as Class + variable block as Variable)
	total := len(classes) + len(vars)
	if total < 2 {
		t.Errorf("expected at least 2 HCL block nodes (Class + Variable), got %d", total)
	}
}

// --- Test 21: Zig Test Declaration Extraction ---

func TestZigTestDeclaration(t *testing.T) {
	src := `const std = @import("std");
const expect = std.testing.expect;

fn add(a: i32, b: i32) i32 {
    return a + b;
}

test "addition works" {
    try expect(add(1, 2) == 3);
}

test "zero addition" {
    try expect(add(0, 0) == 0);
}
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "math.zig"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	funcNames := map[string]bool{}
	for _, f := range funcs {
		funcNames[f.Name] = true
		t.Logf("Function: %s (is_test=%v)", f.Name, f.Properties["is_test"])
	}

	if !funcNames["add"] {
		t.Error("add function not found")
	}

	// Test declarations should be extracted as Functions
	if !funcNames["addition works"] && !funcNames["test \"addition works\""] {
		// Test name might be stored with or without quotes
		found := false
		for name := range funcNames {
			if strings.Contains(name, "addition") {
				found = true
				break
			}
		}
		if !found {
			t.Error("'addition works' test not found")
		}
	}
}

// --- Test 22: Swift INHERITS Edge Extraction ---

func TestSwiftInheritsExtraction(t *testing.T) {
	src := `class Animal {
    var name: String = ""
}

class Dog: Animal {
    func bark() -> String {
        return "Woof"
    }
}
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "animals.swift"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	classNames := map[string]bool{}
	for _, c := range classes {
		classNames[c.Name] = true
		t.Logf("Class: %s", c.Name)
	}
	if !classNames["Animal"] {
		t.Error("Animal class not found")
	}
	if !classNames["Dog"] {
		t.Error("Dog class not found")
	}

	// Check INHERITS edges
	inheritEdges, _ := s.FindEdgesByType(p.ProjectName, "INHERITS")
	if len(inheritEdges) == 0 {
		t.Error("expected INHERITS edges, got 0")
	} else {
		t.Logf("INHERITS edges: %d", len(inheritEdges))
	}

	// Check methods
	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
	methodNames := map[string]bool{}
	for _, m := range methods {
		methodNames[m.Name] = true
		t.Logf("Method: %s", m.Name)
	}
	if !methodNames["bark"] {
		t.Error("bark method not found")
	}
}

// --- Test 23: PHP Method Declaration Extraction ---

func TestPHPMethodDeclaration(t *testing.T) {
	src := `<?php
class User {
    private string $name;

    public function getName(): string {
        return $this->name;
    }

    public function setName(string $name): void {
        $this->name = $name;
    }
}
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "User.php"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	classNames := map[string]bool{}
	for _, c := range classes {
		classNames[c.Name] = true
	}
	if !classNames["User"] {
		t.Error("User class not found")
	}

	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
	methodNames := map[string]bool{}
	for _, m := range methods {
		methodNames[m.Name] = true
		t.Logf("Method: %s (qn=%s)", m.Name, m.QualifiedName)
	}
	if !methodNames["getName"] {
		t.Error("getName method not found")
	}
	if !methodNames["setName"] {
		t.Error("setName method not found")
	}

	defMethodEdges, _ := s.FindEdgesByType(p.ProjectName, "DEFINES_METHOD")
	if len(defMethodEdges) < 2 {
		t.Errorf("expected at least 2 DEFINES_METHOD edges, got %d", len(defMethodEdges))
	}
}

// --- Test 24: Perl CALLS Edge Extraction ---

func TestPerlCallsExtraction(t *testing.T) {
	src := `sub greet {
    my ($name) = @_;
    print("Hello $name\n");
}

sub main {
    greet("World");
}
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "main.pl"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	funcNames := map[string]bool{}
	for _, f := range funcs {
		funcNames[f.Name] = true
		t.Logf("Function: %s", f.Name)
	}
	if !funcNames["greet"] {
		t.Error("greet function not found")
	}
	if !funcNames["main"] {
		t.Error("main function not found")
	}

	// Check CALLS edges
	callEdges, _ := s.FindEdgesByType(p.ProjectName, "CALLS")
	usageEdges, _ := s.FindEdgesByType(p.ProjectName, "USAGE")
	t.Logf("CALLS edges: %d, USAGE edges: %d", len(callEdges), len(usageEdges))
	total := len(callEdges) + len(usageEdges)
	if total == 0 {
		t.Error("expected CALLS or USAGE edges, got 0")
	}
}

// --- Test 25: Erlang Function and CALLS Extraction ---

func TestErlangCallsExtraction(t *testing.T) {
	src := `-module(mymod).
-export([greet/1]).

greet(Name) ->
    io:format("Hello ~s~n", [Name]).

main() ->
    greet("World").
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "mymod.erl"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	funcNames := map[string]bool{}
	for _, f := range funcs {
		funcNames[f.Name] = true
		t.Logf("Function: %s", f.Name)
	}
	if !funcNames["greet"] {
		t.Error("greet function not found")
	}

	// Check CALLS or USAGE edges
	callEdges, _ := s.FindEdgesByType(p.ProjectName, "CALLS")
	usageEdges, _ := s.FindEdgesByType(p.ProjectName, "USAGE")
	t.Logf("CALLS edges: %d, USAGE edges: %d", len(callEdges), len(usageEdges))
	// Require at least 1 CALLS edge (main→greet local call)
	if len(callEdges) == 0 {
		t.Error("expected at least 1 CALLS edge, got 0")
	}
}

// TestErlangVariableExtraction verifies -define() and -record() produce Variable nodes.
func TestErlangVariableExtraction(t *testing.T) {
	src := `-module(mymod).
-define(TIMEOUT, 5000).
-record(person, {name, age}).
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "mymod.erl"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	vars, _ := s.FindNodesByLabel(p.ProjectName, "Variable")
	varNames := map[string]bool{}
	for _, v := range vars {
		varNames[v.Name] = true
		t.Logf("Variable: %s (qn=%s)", v.Name, v.QualifiedName)
	}

	if !varNames["TIMEOUT"] {
		t.Error("expected Variable node for TIMEOUT (-define)")
	}
	if !varNames["person"] {
		t.Error("expected Variable node for person (-record)")
	}
}

// TestErlangMultiModuleCalls verifies cross-module function calls between Erlang files.
func TestErlangMultiModuleCalls(t *testing.T) {
	dir := t.TempDir()

	writeLangTestFile(t, filepath.Join(dir, "handler.erl"), `-module(handler).
-export([handle/1]).
handle(Req) ->
    Req.
`)

	writeLangTestFile(t, filepath.Join(dir, "server.erl"), `-module(server).
-export([start/0]).
start() ->
    handle(request).
`)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	// Both handle and start should exist as functions
	handleFuncs, _ := s.FindNodesByName(p.ProjectName, "handle")
	if len(handleFuncs) == 0 {
		t.Error("handle function not found")
	}

	startFuncs, _ := s.FindNodesByName(p.ProjectName, "start")
	if len(startFuncs) == 0 {
		t.Error("start function not found")
	}

	// Check for CALLS edge from start→handle
	callEdges, _ := s.FindEdgesByType(p.ProjectName, "CALLS")
	t.Logf("CALLS edges: %d", len(callEdges))
	if len(callEdges) == 0 {
		t.Error("expected CALLS edges between Erlang modules, got 0")
	}
}

// --- Test: SQL Table/View/Function Extraction ---

func TestSQLTableExtraction(t *testing.T) {
	src := `CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "schema.sql"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	vars, _ := s.FindNodesByLabel(p.ProjectName, "Variable")
	varNames := map[string]bool{}
	for _, v := range vars {
		varNames[v.Name] = true
		t.Logf("Variable: %s (qn=%s)", v.Name, v.QualifiedName)
	}

	if !varNames["users"] {
		t.Error("expected Variable node for 'users' table")
	}
	if !varNames["active_users"] {
		t.Error("expected Variable node for 'active_users' view")
	}
}

func TestSQLFunctionExtraction(t *testing.T) {
	src := `CREATE FUNCTION add(a INT, b INT) RETURNS INT AS $$ SELECT a + b; $$ LANGUAGE SQL;
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "funcs.sql"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	funcNames := map[string]bool{}
	for _, f := range funcs {
		funcNames[f.Name] = true
		t.Logf("Function: %s (qn=%s)", f.Name, f.QualifiedName)
	}

	if !funcNames["add"] {
		t.Error("expected Function node for 'add'")
	}
}

// --- Test: YAML Variable Extraction ---

func TestYAMLVariableExtraction(t *testing.T) {
	src := `name: myapp
version: "1.0"
services:
  web:
    image: nginx
  db:
    image: postgres
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "config.yaml"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	vars, _ := s.FindNodesByLabel(p.ProjectName, "Variable")
	varNames := map[string]bool{}
	for _, v := range vars {
		varNames[v.Name] = true
		t.Logf("Variable: %s (qn=%s)", v.Name, v.QualifiedName)
	}

	// Top-level keys should be extracted
	for _, expected := range []string{"name", "version", "services"} {
		if !varNames[expected] {
			t.Errorf("expected Variable node for top-level key %q", expected)
		}
	}

	// Nested keys should NOT be extracted (web, db, image)
	for _, nested := range []string{"web", "db", "image"} {
		if varNames[nested] {
			t.Errorf("nested key %q should not be a top-level Variable", nested)
		}
	}
}

// --- Test 26: Method Complexity on Class Methods ---

func TestMethodComplexityOnClassMethods(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		ext      string
		source   string
	}{
		{
			"Java", lang.Java, ".java",
			"class Service {\n\tvoid process(int x) {\n\t\tif (x > 0) {\n\t\t\tfor (int i = 0; i < x; i++) {}\n\t\t}\n\t}\n}\n",
		},
		{
			"TypeScript", lang.TypeScript, ".ts",
			"class Service {\n\tprocess(x: number) {\n\t\tif (x > 0) {\n\t\t\tfor (let i = 0; i < x; i++) {}\n\t\t}\n\t}\n}\n",
		},
		{
			"Python", lang.Python, ".py",
			"class Service:\n    def process(self, x):\n        if x > 0:\n            for i in range(x):\n                pass\n",
		},
		{
			"CSharp", lang.CSharp, ".cs",
			"class Service {\n\tvoid Process(int x) {\n\t\tif (x > 0) {\n\t\t\tfor (int i = 0; i < x; i++) {}\n\t\t}\n\t}\n}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeLangTestFile(t, filepath.Join(dir, "service"+tt.ext), tt.source)

			s, err := store.OpenMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			p := New(context.Background(), s, dir)
			if err := p.Run(); err != nil {
				t.Fatalf("Pipeline.Run: %v", err)
			}

			methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
			if len(methods) == 0 {
				t.Fatal("no methods found")
			}

			for _, m := range methods {
				complexity, ok := m.Properties["complexity"]
				t.Logf("Method: %s, complexity=%v (ok=%v)", m.Name, complexity, ok)
				if !ok || complexity == nil {
					t.Errorf("method %q has no complexity property", m.Name)
				} else {
					cVal, isNum := complexity.(float64)
					if isNum && cVal < 2 {
						t.Errorf("method %q complexity=%v, want >= 2", m.Name, complexity)
					}
				}
			}
		})
	}
}

// --- Test 27: Method Param Types on Class Methods ---

func TestMethodParamTypesOnClassMethods(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		ext      string
		source   string
		wantType string
	}{
		{
			"Java", lang.Java, ".java",
			"class Service {\n\tvoid process(Config cfg) {}\n}\n",
			"Config",
		},
		{
			"TypeScript", lang.TypeScript, ".ts",
			"class Service {\n\tprocess(cfg: Config) {}\n}\n",
			"Config",
		},
		{
			"CSharp", lang.CSharp, ".cs",
			"class Service {\n\tvoid Process(Config cfg) {}\n}\n",
			"Config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, p := runPipelineWithFile(t, "service"+tt.ext, tt.source)

			methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
			if len(methods) == 0 {
				t.Fatal("no methods found")
			}

			for _, m := range methods {
				assertParamTypeContains(t, m, tt.wantType)
			}
		})
	}
}

// --- Test 28: SCSS Variable Extraction ---

func TestSCSSVariableExtraction(t *testing.T) {
	src := `$primary-color: #007bff;
$font-size: 16px;

body {
  color: $primary-color;
  font-size: $font-size;
}
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "styles.scss"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	vars, _ := s.FindNodesByLabel(p.ProjectName, "Variable")
	varNames := map[string]bool{}
	for _, v := range vars {
		varNames[v.Name] = true
		t.Logf("Variable: %s", v.Name)
	}

	if len(vars) == 0 {
		t.Error("no SCSS variables found")
	}
}

// --- Test 29: Ruby CALLS Extraction ---

func TestRubyCallsExtraction(t *testing.T) {
	src := `def greet(name)
  puts "Hello #{name}"
end

def main
  greet("World")
end
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "app.rb"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	funcNames := map[string]bool{}
	for _, f := range funcs {
		funcNames[f.Name] = true
		t.Logf("Function: %s", f.Name)
	}
	if !funcNames["greet"] {
		t.Error("greet function not found")
	}

	// Check CALLS or USAGE edges
	callEdges, _ := s.FindEdgesByType(p.ProjectName, "CALLS")
	usageEdges, _ := s.FindEdgesByType(p.ProjectName, "USAGE")
	t.Logf("CALLS edges: %d, USAGE edges: %d", len(callEdges), len(usageEdges))
	total := len(callEdges) + len(usageEdges)
	if total == 0 {
		t.Error("expected CALLS or USAGE edges, got 0")
	}
}

// --- Test 30: ObjC Class and Method Extraction ---

func TestObjCClassMethodExtraction(t *testing.T) {
	src := `@interface Greeter : NSObject
- (void)greet:(NSString *)name;
@end

@implementation Greeter
- (void)greet:(NSString *)name {
    NSLog(@"Hello %@", name);
}

- (void)run {
    [self greet:@"World"];
}
@end
`
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, "Greeter.m"), src)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	classes, _ := s.FindNodesByLabel(p.ProjectName, "Class")
	classNames := map[string]bool{}
	for _, c := range classes {
		classNames[c.Name] = true
		t.Logf("Class: %s", c.Name)
	}
	if !classNames["Greeter"] {
		t.Error("Greeter class not found")
	}

	methods, _ := s.FindNodesByLabel(p.ProjectName, "Method")
	methodNames := map[string]bool{}
	for _, m := range methods {
		methodNames[m.Name] = true
		t.Logf("Method: %s", m.Name)
	}
	if len(methods) == 0 {
		t.Error("no methods found")
	}

	// Check for CALLS or USAGE edges (message_expression CALLS)
	callEdges, _ := s.FindEdgesByType(p.ProjectName, "CALLS")
	usageEdges, _ := s.FindEdgesByType(p.ProjectName, "USAGE")
	t.Logf("CALLS edges: %d, USAGE edges: %d", len(callEdges), len(usageEdges))
}

// --- Test 31: Rust attribute extraction (#[get("/path")]) --- (formerly Test 14)

func TestRustAttributeExtraction(t *testing.T) {
	src := `#[get("/users")]
async fn get_users() -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[post("/users")]
async fn create_user() -> HttpResponse {
    HttpResponse::Created().finish()
}
`
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.rs"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}

	funcs, _ := s.FindNodesByLabel(p.ProjectName, "Function")
	for _, f := range funcs {
		t.Logf("Function: %s (decorators=%v, is_entry_point=%v)",
			f.Name, f.Properties["decorators"], f.Properties["is_entry_point"])
	}

	// Check decorators are extracted
	foundDecorators := false
	for _, f := range funcs {
		if decs, ok := f.Properties["decorators"]; ok && decs != nil {
			foundDecorators = true
		}
	}
	if !foundDecorators {
		t.Error("no decorators found on any function")
	}

	// Check is_entry_point
	entryPoints := 0
	for _, f := range funcs {
		if ep, ok := f.Properties["is_entry_point"]; ok && ep == true {
			entryPoints++
		}
	}
	if entryPoints < 2 {
		t.Errorf("expected at least 2 entry points, got %d", entryPoints)
	}
}

// --- Shared Test Helpers ---

// runPipelineWithFile creates a temp dir with a single file, runs the pipeline, and returns the store and pipeline.
func runPipelineWithFile(t *testing.T, filename, source string) (*store.Store, *Pipeline) {
	t.Helper()
	dir := t.TempDir()
	writeLangTestFile(t, filepath.Join(dir, filename), source)

	s, err := store.OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	p := New(context.Background(), s, dir)
	if err := p.Run(); err != nil {
		t.Fatalf("Pipeline.Run: %v", err)
	}
	return s, p
}

// collectNodesByLabel returns nodes matching the given label.
func collectNodesByLabel(nodes []*store.Node, label string) []*store.Node {
	var out []*store.Node
	for _, n := range nodes {
		if n.Label == label {
			out = append(out, n)
		}
	}
	return out
}

// assertNodeNamesExist checks that all wantNames appear among the nodes.
func assertNodeNamesExist(t *testing.T, nodes []*store.Node, wantNames []string) {
	t.Helper()
	nameSet := map[string]bool{}
	for _, n := range nodes {
		nameSet[n.Name] = true
	}
	for _, want := range wantNames {
		if !nameSet[want] {
			t.Errorf("expected node %q not found; got: %v", want, nameSet)
		}
	}
}

// assertNodeSignatures checks that methods have expected signature properties.
func assertNodeSignatures(t *testing.T, nodes []*store.Node, wantSigs map[string]string) {
	t.Helper()
	for _, n := range nodes {
		wantSig, ok := wantSigs[n.Name]
		if !ok {
			continue
		}
		gotSig, _ := n.Properties["signature"].(string)
		if gotSig != wantSig {
			t.Errorf("node %q: signature=%q, want=%q", n.Name, gotSig, wantSig)
		}
	}
}

// assertEdgeCount checks that the number of edges with the given type matches expected.
func assertEdgeCount(t *testing.T, edges []pendingEdge, edgeType string, want int) {
	t.Helper()
	count := 0
	for _, pe := range edges {
		if pe.Type == edgeType {
			count++
		}
	}
	if count != want {
		t.Errorf("expected %d %s edges, got %d", want, edgeType, count)
	}
}

// assertNodesNotLabeled checks that none of the wantNames appear among nodes (wrong label).
func assertNodesNotLabeled(t *testing.T, nodes []*store.Node, wantNames []string, badLabel string) {
	t.Helper()
	for _, n := range nodes {
		for _, want := range wantNames {
			if n.Name == want {
				t.Errorf("%q should not be %s", want, badLabel)
			}
		}
	}
}

// assertParamTypeContains checks that a method's param_types property contains the expected type.
func assertParamTypeContains(t *testing.T, m *store.Node, wantType string) {
	t.Helper()
	paramTypes, ok := m.Properties["param_types"]
	t.Logf("Method: %s, param_types=%v", m.Name, paramTypes)
	if !ok || paramTypes == nil {
		t.Errorf("method %q has no param_types property", m.Name)
		return
	}
	pts, ok := paramTypes.([]interface{})
	if !ok {
		t.Errorf("method %q param_types is not a slice: %T", m.Name, paramTypes)
		return
	}
	for _, pt := range pts {
		if str, ok := pt.(string); ok && str == wantType {
			return
		}
	}
	t.Errorf("method %q param_types=%v, want to contain %q", m.Name, paramTypes, wantType)
}
