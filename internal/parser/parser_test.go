package parser

import (
	"testing"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
)

func TestParseGo(t *testing.T) {
	source := []byte(`package main

func Hello() string {
	return "hello"
}

func Add(a, b int) int {
	return a + b
}
`)
	tree, err := Parse(lang.Go, source)
	if err != nil {
		t.Fatalf("Parse Go: %v", err)
	}
	defer tree.Close()

	root := tree.RootNode()
	if root == nil {
		t.Fatal("root node is nil")
	}

	var funcCount int
	Walk(root, func(n *tree_sitter.Node) bool {
		if n.Kind() == "function_declaration" {
			funcCount++
		}
		return true
	})
	if funcCount != 2 {
		t.Errorf("expected 2 function_declarations, got %d", funcCount)
	}
}

func TestParsePython(t *testing.T) {
	source := []byte(`def greet(name):
    return f"Hello, {name}"

class MyClass:
    def method(self):
        pass
`)
	tree, err := Parse(lang.Python, source)
	if err != nil {
		t.Fatalf("Parse Python: %v", err)
	}
	defer tree.Close()

	root := tree.RootNode()
	var funcCount, classCount int
	Walk(root, func(n *tree_sitter.Node) bool {
		switch n.Kind() {
		case "function_definition":
			funcCount++
		case "class_definition":
			classCount++
		}
		return true
	})
	if funcCount != 2 {
		t.Errorf("expected 2 function_definitions, got %d", funcCount)
	}
	if classCount != 1 {
		t.Errorf("expected 1 class_definition, got %d", classCount)
	}
}

func TestParseKotlin(t *testing.T) {
	source := []byte(`fun greet(name: String): String {
    return "Hello, $name"
}

class MyService {
    fun process(): Unit {}
}

object Singleton {
    fun instance(): Singleton = this
}
`)
	tree, err := Parse(lang.Kotlin, source)
	if err != nil {
		t.Fatalf("Parse Kotlin: %v", err)
	}
	defer tree.Close()

	root := tree.RootNode()
	var funcCount, classCount, objectCount int
	Walk(root, func(n *tree_sitter.Node) bool {
		switch n.Kind() {
		case "function_declaration":
			funcCount++
		case "class_declaration":
			classCount++
		case "object_declaration":
			objectCount++
		}
		return true
	})
	if funcCount != 3 {
		t.Errorf("expected 3 function_declarations, got %d", funcCount)
	}
	if classCount != 1 {
		t.Errorf("expected 1 class_declaration, got %d", classCount)
	}
	if objectCount != 1 {
		t.Errorf("expected 1 object_declaration, got %d", objectCount)
	}
}

func TestAllLanguagesLoad(t *testing.T) {
	for _, l := range lang.AllLanguages() {
		_, err := GetLanguage(l)
		if err != nil {
			t.Errorf("GetLanguage(%s): %v", l, err)
		}
	}
}

func TestParseCSharp(t *testing.T) {
	source := []byte(`using System;

namespace MyApp {
    public class Greeter {
        public string Greet(string name) {
            return $"Hello, {name}";
        }

        private void Helper() {}
    }

    public enum Color { Red, Green, Blue }
}
`)
	tree, err := Parse(lang.CSharp, source)
	if err != nil {
		t.Fatalf("Parse C#: %v", err)
	}
	defer tree.Close()

	root := tree.RootNode()
	if root == nil {
		t.Fatal("root node is nil")
	}

	var classCount, methodCount int
	Walk(root, func(n *tree_sitter.Node) bool {
		switch n.Kind() {
		case "class_declaration":
			classCount++
		case "method_declaration":
			methodCount++
		}
		return true
	})
	if classCount != 1 {
		t.Errorf("expected 1 class_declaration, got %d", classCount)
	}
	if methodCount != 2 {
		t.Errorf("expected 2 method_declarations, got %d", methodCount)
	}
}

func TestParseRuby(t *testing.T) {
	source := []byte(`class Greeter
  def greet(name)
	"Hello, #{name}"
  end`)
	tree, err := Parse(lang.Ruby, source)
	if err != nil {
		t.Fatalf("Parse Ruby: %v", err)
	}
	defer tree.Close()

	root := tree.RootNode()
	if root == nil {
		t.Fatal("root node is nil")
	}

	var classCount, methodCount int
	Walk(root, func(n *tree_sitter.Node) bool {
		switch n.Kind() {
		case "class":
			if n.NamedChildCount() > 0 {
				classCount++
			}
		case "method":
			methodCount++
		}
		return true
	})
	if classCount != 1 {
		t.Errorf("expected 1 class, got %d", classCount)
	}
	if methodCount != 1 {
		t.Errorf("expected 1 method, got %d", methodCount)
	}
}

func TestNodeText(t *testing.T) {
	source := []byte(`package main

func Hello() string {
	return "hello"
}
`)
	tree, err := Parse(lang.Go, source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	defer tree.Close()

	root := tree.RootNode()
	Walk(root, func(n *tree_sitter.Node) bool {
		if n.Kind() == "function_declaration" {
			nameNode := n.ChildByFieldName("name")
			if nameNode == nil {
				t.Error("function has no name node")
				return false
			}
			name := NodeText(nameNode, source)
			if name != "Hello" {
				t.Errorf("expected Hello, got %s", name)
			}
			return false
		}
		return true
	})
}
