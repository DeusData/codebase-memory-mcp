package pipeline

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// --- Unit Tests: extractDocstring ---

var docstringTestCases = []struct {
	name     string
	language lang.Language
	source   string
	want     string // required substring in extracted docstring
}{
	{
		"Python",
		lang.Python,
		"def f():\n\t\"\"\"Computes the result.\"\"\"\n\tpass\n",
		"Computes the result.",
	},
	{
		"Python_multiline",
		lang.Python,
		"def f():\n\t\"\"\"Computes the result.\n\n\tMore details here.\n\t\"\"\"\n\tpass\n",
		"More details here.",
	},
	{
		"Go",
		lang.Go,
		"// ComputeResult computes the result.\nfunc f() {}\n",
		"ComputeResult computes the result.",
	},
	{
		"Go_multiline",
		lang.Go,
		"// ComputeResult computes the result.\n// It returns an int.\nfunc f() {}\n",
		"It returns an int.",
	},
	{
		"JavaScript",
		lang.JavaScript,
		"/** Computes the result. */\nfunction f() {}\n",
		"Computes the result.",
	},
	{
		"TypeScript",
		lang.TypeScript,
		"/** Computes the result. */\nfunction f(): void {}\n",
		"Computes the result.",
	},
	{
		"TSX",
		lang.TSX,
		"/** Computes the result. */\nfunction f(): void {}\n",
		"Computes the result.",
	},
	{
		"Java",
		lang.Java,
		"class A {\n\t/** Computes the result. */\n\tvoid f() {}\n}\n",
		"Computes the result.",
	},
	{
		"Java_multiline",
		lang.Java,
		"class A {\n\t/**\n\t * Computes the result.\n\t * @param x input\n\t */\n\tvoid f() {}\n}\n",
		"Computes the result.",
	},
	{
		"Rust",
		lang.Rust,
		"/// Computes the result.\nfn f() {}\n",
		"Computes the result.",
	},
	{
		"Rust_multiline",
		lang.Rust,
		"/// Computes the result.\n/// Returns nothing.\nfn f() {}\n",
		"Returns nothing.",
	},
	{
		"CPP",
		lang.CPP,
		"/// Computes the result.\nvoid f() {}\n",
		"Computes the result.",
	},
	{
		"CPP_block",
		lang.CPP,
		"/** Computes the result. */\nvoid f() {}\n",
		"Computes the result.",
	},
	{
		"CSharp",
		lang.CSharp,
		"class A {\n\t/// <summary>Computes the result.</summary>\n\tvoid F() {}\n}\n",
		"Computes the result.",
	},
	{
		"PHP",
		lang.PHP,
		"<?php\n/** Computes the result. */\nfunction f() {}\n",
		"Computes the result.",
	},
	{
		"Lua_line",
		lang.Lua,
		"--- Computes the result.\nfunction f() end\n",
		"Computes the result.",
	},
	{
		"Lua_block",
		lang.Lua,
		"--[[ Computes the result. ]]\nfunction f() end\n",
		"Computes the result.",
	},
	{
		"Scala",
		lang.Scala,
		"object A {\n\t/** Computes the result. */\n\tdef f(): Unit = {}\n}\n",
		"Computes the result.",
	},
	{
		"Kotlin",
		lang.Kotlin,
		"/** Computes the result. */\nfun f() {}\n",
		"Computes the result.",
	},
}

func TestDocstringExtractionAllLanguages(t *testing.T) {
	for _, tt := range docstringTestCases {
		t.Run(tt.name, func(t *testing.T) {
			tree, src := parseSource(t, tt.language, tt.source)
			defer tree.Close()

			spec := lang.ForLanguage(tt.language)
			if spec == nil {
				t.Fatalf("no spec for %s", tt.language)
			}

			funcNode := findFirstNodeByKind(tree.RootNode(), spec.FunctionNodeTypes...)
			if funcNode == nil {
				t.Fatalf("no function node found in AST")
			}

			got := extractDocstring(funcNode, src, tt.language)
			if !strings.Contains(got, tt.want) {
				t.Errorf("extractDocstring() = %q, want substring %q", got, tt.want)
			}
		})
	}
}

func TestDocstringExtractionNoDocstring(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		source   string
	}{
		{"Python", lang.Python, "def f():\n\tpass\n"},
		{"Go", lang.Go, "func f() {}\n"},
		{"JavaScript", lang.JavaScript, "function f() {}\n"},
		{"Java", lang.Java, "class A {\n\tvoid f() {}\n}\n"},
		{"Rust", lang.Rust, "fn f() {}\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, src := parseSource(t, tt.language, tt.source)
			defer tree.Close()

			spec := lang.ForLanguage(tt.language)
			funcNode := findFirstNodeByKind(tree.RootNode(), spec.FunctionNodeTypes...)
			if funcNode == nil {
				t.Fatalf("no function node found")
			}

			got := extractDocstring(funcNode, src, tt.language)
			if got != "" {
				t.Errorf("expected empty docstring, got %q", got)
			}
		})
	}
}

func TestDocstringBlankLineSeparation(t *testing.T) {
	// A blank line between comment and function means it's NOT a docstring.
	source := "// This is not a docstring.\n\nfunc f() {}\n"
	tree, src := parseSource(t, lang.Go, source)
	defer tree.Close()

	spec := lang.ForLanguage(lang.Go)
	funcNode := findFirstNodeByKind(tree.RootNode(), spec.FunctionNodeTypes...)
	if funcNode == nil {
		t.Fatal("no function node found")
	}

	got := extractDocstring(funcNode, src, lang.Go)
	if got != "" {
		t.Errorf("expected empty docstring (blank line separation), got %q", got)
	}
}

func TestClassDocstringExtraction(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		source   string
		want     string
	}{
		{
			"Python",
			lang.Python,
			"class MyClass:\n\t\"\"\"A documented class.\"\"\"\n\tpass\n",
			"A documented class.",
		},
		{
			"Java",
			lang.Java,
			"/** A documented class. */\nclass MyClass {}\n",
			"A documented class.",
		},
		{
			"Go",
			lang.Go,
			"// MyStruct is a documented struct.\ntype MyStruct struct{}\n",
			"MyStruct is a documented struct.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, src := parseSource(t, tt.language, tt.source)
			defer tree.Close()

			spec := lang.ForLanguage(tt.language)
			if spec == nil {
				t.Fatalf("no spec for %s", tt.language)
			}

			classNode := findFirstNodeByKind(tree.RootNode(), spec.ClassNodeTypes...)
			if classNode == nil {
				t.Fatalf("no class node found")
			}

			got := extractDocstring(classNode, src, tt.language)
			if !strings.Contains(got, tt.want) {
				t.Errorf("extractDocstring() = %q, want substring %q", got, tt.want)
			}
		})
	}
}

// --- Integration Test: pipeline stores docstring property ---

func TestDocstringIntegration(t *testing.T) {
	tests := []struct {
		name     string
		language lang.Language
		ext      string
		source   string
		label    string // "Function" or "Class"
		wantName string // node name to find
		want     string // docstring substring
	}{
		{
			"Go_function",
			lang.Go, ".go",
			"package main\n\n// Compute does something.\nfunc Compute() {}\n",
			"Function", "Compute", "Compute does something.",
		},
		{
			"Python_function",
			lang.Python, ".py",
			"def compute():\n\t\"\"\"Does something.\"\"\"\n\tpass\n",
			"Function", "compute", "Does something.",
		},
		{
			"Java_method",
			lang.Java, ".java",
			"class A {\n\t/** Computes result. */\n\tvoid compute() {}\n}\n",
			"Method", "compute", "Computes result.",
		},
		{
			"Kotlin_function",
			lang.Kotlin, ".kt",
			"/** Computes result. */\nfun compute() {}\n",
			"Function", "compute", "Computes result.",
		},
		{
			"Go_class",
			lang.Go, ".go",
			"package main\n\n// MyStruct is documented.\ntype MyStruct struct{}\n",
			"Class", "MyStruct", "MyStruct is documented.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			nodes, err := s.FindNodesByLabel(p.ProjectName, tt.label)
			if err != nil {
				t.Fatal(err)
			}

			var found bool
			for _, n := range nodes {
				if n.Name != tt.wantName {
					continue
				}
				found = true
				doc, ok := n.Properties["docstring"].(string)
				if !ok || doc == "" {
					t.Errorf("node %q has no docstring property", n.QualifiedName)
					continue
				}
				if !strings.Contains(doc, tt.want) {
					t.Errorf("node %q docstring = %q, want substring %q", n.QualifiedName, doc, tt.want)
				}
			}
			if !found {
				t.Errorf("no %s node named %q found", tt.label, tt.wantName)
			}
		})
	}
}
