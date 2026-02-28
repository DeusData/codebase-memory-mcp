package parser

import (
	"fmt"
	"sync"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"

	tree_sitter_cpp "github.com/tree-sitter/tree-sitter-cpp/bindings/go"
	tree_sitter_c_sharp "github.com/tree-sitter/tree-sitter-c-sharp/bindings/go"
	tree_sitter_go "github.com/tree-sitter/tree-sitter-go/bindings/go"
	tree_sitter_java "github.com/tree-sitter/tree-sitter-java/bindings/go"
	tree_sitter_javascript "github.com/tree-sitter/tree-sitter-javascript/bindings/go"
	tree_sitter_php "github.com/tree-sitter/tree-sitter-php/bindings/go"
	tree_sitter_python "github.com/tree-sitter/tree-sitter-python/bindings/go"
	tree_sitter_rust "github.com/tree-sitter/tree-sitter-rust/bindings/go"
	tree_sitter_scala "github.com/tree-sitter/tree-sitter-scala/bindings/go"
	tree_sitter_typescript "github.com/tree-sitter/tree-sitter-typescript/bindings/go"

	tree_sitter_kotlin "github.com/tree-sitter-grammars/tree-sitter-kotlin/bindings/go"
	tree_sitter_lua "github.com/tree-sitter-grammars/tree-sitter-lua/bindings/go"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
)

var (
	languagesOnce sync.Once
	languages     map[lang.Language]*tree_sitter.Language
	parserPools   map[lang.Language]*sync.Pool
)

func initLanguages() {
	languagesOnce.Do(func() {
		languages = map[lang.Language]*tree_sitter.Language{
			lang.Python:     tree_sitter.NewLanguage(tree_sitter_python.Language()),
			lang.JavaScript: tree_sitter.NewLanguage(tree_sitter_javascript.Language()),
			lang.TypeScript: tree_sitter.NewLanguage(tree_sitter_typescript.LanguageTypescript()),
			lang.TSX:        tree_sitter.NewLanguage(tree_sitter_typescript.LanguageTSX()),
			lang.Go:         tree_sitter.NewLanguage(tree_sitter_go.Language()),
			lang.Rust:       tree_sitter.NewLanguage(tree_sitter_rust.Language()),
			lang.Java:       tree_sitter.NewLanguage(tree_sitter_java.Language()),
			lang.CPP:    tree_sitter.NewLanguage(tree_sitter_cpp.Language()),
			lang.CSharp: tree_sitter.NewLanguage(tree_sitter_c_sharp.Language()),
			lang.PHP:    tree_sitter.NewLanguage(tree_sitter_php.LanguagePHPOnly()),
			lang.Lua:   tree_sitter.NewLanguage(tree_sitter_lua.Language()),
			lang.Scala: tree_sitter.NewLanguage(tree_sitter_scala.Language()),
			lang.Kotlin: tree_sitter.NewLanguage(tree_sitter_kotlin.Language()),
		}

		parserPools = make(map[lang.Language]*sync.Pool, len(languages))
		for l, tsLang := range languages {
			tsLang := tsLang
			parserPools[l] = &sync.Pool{
				New: func() any {
					p := tree_sitter.NewParser()
					if err := p.SetLanguage(tsLang); err != nil {
						panic(fmt.Sprintf("set language: %v", err))
					}
					return p
				},
			}
		}
	})
}

// GetLanguage returns the tree-sitter Language for a lang.Language.
func GetLanguage(l lang.Language) (*tree_sitter.Language, error) {
	initLanguages()
	tsLang, ok := languages[l]
	if !ok {
		return nil, fmt.Errorf("unsupported language: %s", l)
	}
	return tsLang, nil
}

// Parse parses source code into a tree-sitter AST Tree.
// The caller must call tree.Close() when done.
// Parsers are pooled per language via sync.Pool to avoid per-file allocation.
func Parse(l lang.Language, source []byte) (*tree_sitter.Tree, error) {
	initLanguages()

	pool, ok := parserPools[l]
	if !ok {
		return nil, fmt.Errorf("unsupported language: %s", l)
	}

	p, _ := pool.Get().(*tree_sitter.Parser)
	if p == nil {
		return nil, fmt.Errorf("failed to get parser for language %s", l)
	}
	tree := p.Parse(source, nil)
	pool.Put(p)

	if tree == nil {
		return nil, fmt.Errorf("parse failed for language %s", l)
	}

	return tree, nil
}

// WalkFunc is called for each node during AST traversal.
// Return false to skip children.
type WalkFunc func(node *tree_sitter.Node) bool

// Walk traverses the AST in depth-first order.
func Walk(node *tree_sitter.Node, fn WalkFunc) {
	if node == nil {
		return
	}
	if !fn(node) {
		return
	}
	for i := uint(0); i < node.ChildCount(); i++ {
		child := node.Child(i)
		if child != nil {
			Walk(child, fn)
		}
	}
}

// NodeText returns the text content of a node.
func NodeText(node *tree_sitter.Node, source []byte) string {
	return string(source[node.StartByte():node.EndByte()])
}
