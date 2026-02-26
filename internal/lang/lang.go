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
	JSON       Language = "json" // Not in AllLanguages(); no LanguageSpec or tree-sitter grammar
)

// AllLanguages returns all supported languages.
func AllLanguages() []Language {
	return []Language{Python, JavaScript, TypeScript, TSX, Go, Rust, Java, CPP, CSharp, PHP, Lua, Scala}
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
