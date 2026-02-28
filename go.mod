module github.com/DeusData/codebase-memory-mcp

go 1.26

require (
	github.com/mattn/go-sqlite3 v1.14.34
	github.com/modelcontextprotocol/go-sdk v1.3.1
	github.com/tree-sitter-grammars/tree-sitter-lua v0.4.1
	github.com/tree-sitter/go-tree-sitter v0.25.0
	github.com/tree-sitter/tree-sitter-cpp v0.23.4
	github.com/tree-sitter/tree-sitter-go v0.25.0
	github.com/tree-sitter/tree-sitter-java v0.23.5
	github.com/tree-sitter/tree-sitter-javascript v0.25.0
	github.com/tree-sitter/tree-sitter-php v0.24.2
	github.com/tree-sitter/tree-sitter-python v0.25.0
	github.com/tree-sitter/tree-sitter-rust v0.24.0
	github.com/tree-sitter/tree-sitter-scala v0.24.0
	github.com/tree-sitter/tree-sitter-typescript v0.23.2
	github.com/zeebo/xxh3 v1.1.0
	golang.org/x/sync v0.17.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/google/jsonschema-go v0.4.2 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/mattn/go-pointer v0.0.1 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.3 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/tools v0.38.0 // indirect
)

replace github.com/tree-sitter/go-tree-sitter => github.com/DeusData/go-tree-sitter v0.26.0-deusdata.1
