package lang

// Language represents a supported programming language.
type Language string

const (
	Python     Language = "python"
	JavaScript Language = "javascript"
	TypeScript Language = "typescript"
	Go         Language = "go"
	Rust       Language = "rust"
	Java       Language = "java"
	CPP        Language = "cpp"
	TSX        Language = "tsx"
	CSharp     Language = "c-sharp"
	PHP        Language = "php"
	Lua        Language = "lua"
	Scala      Language = "scala"
	Kotlin     Language = "kotlin"
	JSON       Language = "json" // Not in AllLanguages(); no LanguageSpec or tree-sitter grammar

	// Programming languages (Tier 1)
	Ruby       Language = "ruby"
	C          Language = "c"
	Bash       Language = "bash"
	Zig        Language = "zig"
	Elixir     Language = "elixir"
	Haskell    Language = "haskell"
	OCaml      Language = "ocaml"
	ObjectiveC Language = "objective-c"
	Swift      Language = "swift"
	Dart       Language = "dart"
	Perl       Language = "perl"
	Groovy     Language = "groovy"
	Erlang     Language = "erlang"
	R          Language = "r"

	// Helper languages (Tier 2)
	HTML       Language = "html"
	CSS        Language = "css"
	SCSS       Language = "scss"
	YAML       Language = "yaml"
	TOML       Language = "toml"
	HCL        Language = "hcl"
	SQL        Language = "sql"
	Dockerfile Language = "dockerfile"
)

// AllLanguages returns all supported languages.
func AllLanguages() []Language {
	return []Language{
		Python, JavaScript, TypeScript, TSX, Go, Rust, Java, CPP, CSharp, PHP, Lua, Scala, Kotlin,
		Ruby, C, Bash, Zig, Elixir, Haskell, OCaml, ObjectiveC,
		Swift, Dart, Perl, Groovy, Erlang, R,
		HTML, CSS, SCSS, YAML, TOML, HCL, SQL, Dockerfile,
	}
}

// LanguageSpec defines the tree-sitter node types for a language.
type LanguageSpec struct {
	Language          Language
	FileExtensions    []string
	FunctionNodeTypes []string
	ClassNodeTypes    []string
	FieldNodeTypes    []string // tree-sitter node kinds for struct/class fields
	ModuleNodeTypes   []string
	CallNodeTypes     []string
	ImportNodeTypes   []string
	ImportFromTypes   []string
	PackageIndicators []string

	// BranchingNodeTypes lists AST node kinds counted for complexity metric.
	BranchingNodeTypes []string
	// VariableNodeTypes lists module-level variable declaration node kinds.
	VariableNodeTypes []string
	// AssignmentNodeTypes lists assignment expression/statement node kinds.
	AssignmentNodeTypes []string
	// ThrowNodeTypes lists throw/raise statement node kinds.
	ThrowNodeTypes []string
	// ThrowsClauseField is the field name for declared throws (e.g. Java "throws").
	ThrowsClauseField string
	// DecoratorNodeTypes lists decorator/annotation node kinds.
	DecoratorNodeTypes []string
	// EnvAccessFunctions lists function names used to read env vars (e.g. "os.Getenv").
	EnvAccessFunctions []string
	// EnvAccessMemberPatterns lists member access patterns for env vars (e.g. "process.env").
	EnvAccessMemberPatterns []string
}

// registry maps file extensions to language specs.
var registry = map[string]*LanguageSpec{}

// Register adds a LanguageSpec to the global registry.
func Register(spec *LanguageSpec) {
	for _, ext := range spec.FileExtensions {
		registry[ext] = spec
	}
}

// ForExtension returns the LanguageSpec for a file extension (e.g. ".go").
func ForExtension(ext string) *LanguageSpec {
	return registry[ext]
}

// ForLanguage returns the LanguageSpec for a language.
func ForLanguage(lang Language) *LanguageSpec {
	for _, spec := range registry {
		if spec.Language == lang {
			return spec
		}
	}
	return nil
}

// LanguageForExtension returns the Language for a file extension.
func LanguageForExtension(ext string) (Language, bool) {
	spec := registry[ext]
	if spec == nil {
		return "", false
	}
	return spec.Language, true
}
