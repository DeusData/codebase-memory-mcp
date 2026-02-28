package main

import (
	"fmt"
	"os"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
	"github.com/DeusData/codebase-memory-mcp/internal/parser"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
)

func printAST(node *tree_sitter.Node, source []byte, indent int) {
	if node == nil {
		return
	}
	prefix := ""
	for i := 0; i < indent; i++ {
		prefix += "  "
	}
	parentKind := "nil"
	if node.Parent() != nil {
		parentKind = node.Parent().Kind()
	}
	text := string(source[node.StartByte():node.EndByte()])
	if len(text) > 60 {
		text = text[:60] + "..."
	}
	fmt.Printf("%s%s (parent=%s) %q\n", prefix, node.Kind(), parentKind, text)
	for i := uint(0); i < node.ChildCount(); i++ {
		printAST(node.Child(i), source, indent+1)
	}
}

func main() {
	// Test Go - check if var groups have wrapping
	goCode := []byte("package main\n\nvar globalVar = 42\n\nvar (\n\ta = 1\n\tb = 2\n)\n")
	fmt.Println("=== GO AST ===")
	tree, err := parser.Parse(lang.Go, goCode)
	if err != nil {
		fmt.Println("Error:", err)
	}
	if tree != nil {
		printAST(tree.RootNode(), goCode, 0)
		tree.Close()
	}

	// Test Rust
	rustCode := []byte("pub static X: i32 = 5;\nconst Y: &str = \"hello\";\n")
	fmt.Println("\n=== RUST AST ===")
	tree2, err := parser.Parse(lang.Rust, rustCode)
	if err != nil {
		fmt.Println("Error:", err)
	}
	if tree2 != nil {
		printAST(tree2.RootNode(), rustCode, 0)
		tree2.Close()
	}

	// Test Python decorated function
	pyCode := []byte("@app.route('/api')\ndef handler():\n    pass\n")
	fmt.Println("\n=== PYTHON DECORATED FUNC ===")
	tree3, err := parser.Parse(lang.Python, pyCode)
	if err != nil {
		fmt.Println("Error:", err)
	}
	if tree3 != nil {
		printAST(tree3.RootNode(), pyCode, 0)
		tree3.Close()
	}

	// Test Python with type annotation assignment
	pyCode2 := []byte("x: int = 5\nlogger: Logger = get_logger()\n")
	fmt.Println("\n=== PYTHON TYPE ANNOTATED ASSIGNMENT ===")
	tree4, err := parser.Parse(lang.Python, pyCode2)
	if err != nil {
		fmt.Println("Error:", err)
	}
	if tree4 != nil {
		printAST(tree4.RootNode(), pyCode2, 0)
		tree4.Close()
	}

	os.Exit(0)
}
