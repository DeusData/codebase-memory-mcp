package cbm

/*
#cgo CFLAGS: -std=c11 -D_DEFAULT_SOURCE -O2 -Wall -Ivendored/ts_runtime/include -Ivendored/ts_runtime/src
#include "cbm.h"
#include "helpers.h"
#include "lang_specs.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/DeusData/codebase-memory-mcp/internal/lang"
)

// FileResult holds the extraction results from one file.
type FileResult struct {
	Definitions []Definition
	Calls       []Call
	Imports     []Import
	Usages      []Usage
	Throws      []Throw
	ReadWrites  []ReadWrite
	TypeRefs    []TypeRef
	EnvAccesses []EnvAccess
	TypeAssigns []TypeAssign
	ImplTraits  []ImplTrait

	ModuleQN    string
	IsTestFile  bool
	ImportCount int
}

// ImplTrait represents a Rust `impl Trait for Struct` pair.
type ImplTrait struct {
	TraitName  string
	StructName string
}

// Definition represents a function/class/variable/module extracted from source.
type Definition struct {
	Name          string
	QualifiedName string
	Label         string // "Function", "Method", "Class", "Variable", "Module", "Interface", "Enum", "Type"
	FilePath      string
	StartLine     int
	EndLine       int
	Signature     string
	ReturnType    string
	Receiver      string
	Docstring     string
	ParentClass   string
	Decorators    []string
	BaseClasses   []string
	ParamTypes    []string
	Complexity    int
	Lines         int
	IsExported    bool
	IsAbstract    bool
	IsTest        bool
	IsEntryPoint  bool
}

// Call represents a raw call site (callee name + enclosing function).
type Call struct {
	CalleeName      string
	EnclosingFuncQN string
}

// Import represents a local name -> module path mapping.
type Import struct {
	LocalName  string
	ModulePath string
}

// Usage represents a reference to an identifier (not a call or import).
type Usage struct {
	RefName         string
	EnclosingFuncQN string
}

// Throw represents a throw/raise statement.
type Throw struct {
	ExceptionName   string
	EnclosingFuncQN string
}

// ReadWrite represents a variable read or write.
type ReadWrite struct {
	VarName         string
	EnclosingFuncQN string
	IsWrite         bool
}

// TypeRef represents a type reference in a function signature or body.
type TypeRef struct {
	TypeName        string
	EnclosingFuncQN string
}

// EnvAccess represents an environment variable access.
type EnvAccess struct {
	EnvKey          string
	EnclosingFuncQN string
}

// TypeAssign represents a variable assignment where RHS is a constructor call.
type TypeAssign struct {
	VarName         string
	TypeName        string
	EnclosingFuncQN string
}

var initOnce sync.Once

// Init initializes the C library. Safe to call multiple times.
func Init() {
	initOnce.Do(func() {
		C.cbm_init()
	})
}

// Shutdown cleans up the C library.
func Shutdown() {
	C.cbm_shutdown()
}


// ProfileStats holds accumulated profiling data from C extraction.
type ProfileStats struct {
	ParseNs   uint64
	ExtractNs uint64
	Files     uint64
}

// GetProfile returns accumulated parse/extraction timing and resets counters.
func GetProfile() ProfileStats {
	var p, e, f C.uint64_t
	C.cbm_get_profile(&p, &e, &f)
	C.cbm_reset_profile()
	return ProfileStats{ParseNs: uint64(p), ExtractNs: uint64(e), Files: uint64(f)}
}

// languageToC maps Go lang.Language to CBMLanguage enum.
var languageToC = map[lang.Language]C.CBMLanguage{
	lang.Go:         C.CBM_LANG_GO,
	lang.Python:     C.CBM_LANG_PYTHON,
	lang.JavaScript: C.CBM_LANG_JAVASCRIPT,
	lang.TypeScript: C.CBM_LANG_TYPESCRIPT,
	lang.TSX:        C.CBM_LANG_TSX,
	lang.Rust:       C.CBM_LANG_RUST,
	lang.Java:       C.CBM_LANG_JAVA,
	lang.CPP:        C.CBM_LANG_CPP,
	lang.CSharp:     C.CBM_LANG_CSHARP,
	lang.PHP:        C.CBM_LANG_PHP,
	lang.Lua:        C.CBM_LANG_LUA,
	lang.Scala:      C.CBM_LANG_SCALA,
	lang.Kotlin:     C.CBM_LANG_KOTLIN,
	lang.Ruby:       C.CBM_LANG_RUBY,
	lang.C:          C.CBM_LANG_C,
	lang.Bash:       C.CBM_LANG_BASH,
	lang.Zig:        C.CBM_LANG_ZIG,
	lang.Elixir:     C.CBM_LANG_ELIXIR,
	lang.Haskell:    C.CBM_LANG_HASKELL,
	lang.OCaml:      C.CBM_LANG_OCAML,
	lang.ObjectiveC: C.CBM_LANG_OBJC,
	lang.Swift:      C.CBM_LANG_SWIFT,
	lang.Dart:       C.CBM_LANG_DART,
	lang.Perl:       C.CBM_LANG_PERL,
	lang.Groovy:     C.CBM_LANG_GROOVY,
	lang.Erlang:     C.CBM_LANG_ERLANG,
	lang.R:          C.CBM_LANG_R,
	lang.HTML:       C.CBM_LANG_HTML,
	lang.CSS:        C.CBM_LANG_CSS,
	lang.SCSS:       C.CBM_LANG_SCSS,
	lang.YAML:       C.CBM_LANG_YAML,
	lang.TOML:       C.CBM_LANG_TOML,
	lang.HCL:        C.CBM_LANG_HCL,
	lang.SQL:        C.CBM_LANG_SQL,
	lang.Dockerfile: C.CBM_LANG_DOCKERFILE,
	// New languages (v0.5 expansion)
	lang.Clojure:    C.CBM_LANG_CLOJURE,
	lang.FSharp:     C.CBM_LANG_FSHARP,
	lang.Julia:      C.CBM_LANG_JULIA,
	lang.VimScript:  C.CBM_LANG_VIMSCRIPT,
	lang.Nix:        C.CBM_LANG_NIX,
	lang.CommonLisp: C.CBM_LANG_COMMONLISP,
	lang.Elm:        C.CBM_LANG_ELM,
	lang.Fortran:    C.CBM_LANG_FORTRAN,
	lang.CUDA:       C.CBM_LANG_CUDA,
	lang.COBOL:      C.CBM_LANG_COBOL,
	lang.Verilog:    C.CBM_LANG_VERILOG,
	lang.EmacsLisp:  C.CBM_LANG_EMACSLISP,
	lang.JSON:       C.CBM_LANG_JSON,
	lang.XML:        C.CBM_LANG_XML,
	lang.Markdown:   C.CBM_LANG_MARKDOWN,
	lang.Makefile:   C.CBM_LANG_MAKEFILE,
	lang.CMake:      C.CBM_LANG_CMAKE,
	lang.Protobuf:   C.CBM_LANG_PROTOBUF,
	lang.GraphQL:    C.CBM_LANG_GRAPHQL,
	lang.Vue:        C.CBM_LANG_VUE,
	lang.Svelte:     C.CBM_LANG_SVELTE,
	lang.Meson:      C.CBM_LANG_MESON,
	lang.GLSL:       C.CBM_LANG_GLSL,
	lang.INI:        C.CBM_LANG_INI,
	// Scientific/math languages
	lang.MATLAB: C.CBM_LANG_MATLAB,
	lang.Lean:   C.CBM_LANG_LEAN,
	lang.FORM:   C.CBM_LANG_FORM,
	lang.Magma:   C.CBM_LANG_MAGMA,
	lang.Wolfram: C.CBM_LANG_WOLFRAM,
}

// ParseTimeoutMicros is the default per-file parse timeout (10 seconds).
const ParseTimeoutMicros = 10_000_000

// ExtractFile runs the C extraction library on one file.
// source is the file content, language is the programming language,
// project and relPath are used for qualified name computation.
func ExtractFile(source []byte, language lang.Language, project, relPath string) (*FileResult, error) {
	Init()

	if len(source) == 0 {
		return &FileResult{}, nil
	}

	cLang, ok := languageToC[language]
	if !ok {
		return nil, fmt.Errorf("cbm: unsupported language %q", language)
	}

	cSource := (*C.char)(unsafe.Pointer(&source[0]))
	cSourceLen := C.int(len(source))
	cProject := C.CString(project)
	cRelPath := C.CString(relPath)
	defer C.free(unsafe.Pointer(cProject))
	defer C.free(unsafe.Pointer(cRelPath))

	result := C.cbm_extract_file(cSource, cSourceLen, cLang, cProject, cRelPath, C.int64_t(ParseTimeoutMicros))
	if result == nil {
		return nil, fmt.Errorf("cbm: extraction returned nil")
	}
	defer C.cbm_free_result(result)

	if result.has_error {
		return nil, fmt.Errorf("cbm: %s", C.GoString(result.error_msg))
	}

	return convertResult(result), nil
}

func convertResult(r *C.CBMFileResult) *FileResult {
	fr := &FileResult{
		ModuleQN:    C.GoString(r.module_qn),
		IsTestFile:  bool(r.is_test_file),
		ImportCount: int(r.imports_count),
	}

	// Definitions
	if r.defs.count > 0 {
		fr.Definitions = make([]Definition, r.defs.count)
		defs := unsafe.Slice(r.defs.items, r.defs.count)
		for i := range defs {
			d := &defs[i]
			fr.Definitions[i] = Definition{
				Name:          C.GoString(d.name),
				QualifiedName: C.GoString(d.qualified_name),
				Label:         C.GoString(d.label),
				FilePath:      C.GoString(d.file_path),
				StartLine:     int(d.start_line),
				EndLine:       int(d.end_line),
				Signature:     goStringOrEmpty(d.signature),
				ReturnType:    goStringOrEmpty(d.return_type),
				Receiver:      goStringOrEmpty(d.receiver),
				Docstring:     goStringOrEmpty(d.docstring),
				ParentClass:   goStringOrEmpty(d.parent_class),
				Decorators:    goStringSlice(d.decorators),
				BaseClasses:   goStringSlice(d.base_classes),
				ParamTypes:    goStringSlice(d.param_types),
				Complexity:    int(d.complexity),
				Lines:         int(d.lines),
				IsExported:    bool(d.is_exported),
				IsAbstract:    bool(d.is_abstract),
				IsTest:        bool(d.is_test),
				IsEntryPoint:  bool(d.is_entry_point),
			}
		}
	}

	// Calls
	if r.calls.count > 0 {
		fr.Calls = make([]Call, r.calls.count)
		calls := unsafe.Slice(r.calls.items, r.calls.count)
		for i, c := range calls {
			fr.Calls[i] = Call{
				CalleeName:      C.GoString(c.callee_name),
				EnclosingFuncQN: C.GoString(c.enclosing_func_qn),
			}
		}
	}

	// Imports
	if r.imports.count > 0 {
		fr.Imports = make([]Import, r.imports.count)
		imports := unsafe.Slice(r.imports.items, r.imports.count)
		for i, imp := range imports {
			fr.Imports[i] = Import{
				LocalName:  C.GoString(imp.local_name),
				ModulePath: C.GoString(imp.module_path),
			}
		}
	}

	// Usages
	if r.usages.count > 0 {
		fr.Usages = make([]Usage, r.usages.count)
		usages := unsafe.Slice(r.usages.items, r.usages.count)
		for i, u := range usages {
			fr.Usages[i] = Usage{
				RefName:         C.GoString(u.ref_name),
				EnclosingFuncQN: C.GoString(u.enclosing_func_qn),
			}
		}
	}

	// Throws
	if r.throws.count > 0 {
		fr.Throws = make([]Throw, r.throws.count)
		throws := unsafe.Slice(r.throws.items, r.throws.count)
		for i, t := range throws {
			fr.Throws[i] = Throw{
				ExceptionName:   C.GoString(t.exception_name),
				EnclosingFuncQN: C.GoString(t.enclosing_func_qn),
			}
		}
	}

	// ReadWrites
	if r.rw.count > 0 {
		fr.ReadWrites = make([]ReadWrite, r.rw.count)
		rws := unsafe.Slice(r.rw.items, r.rw.count)
		for i, rw := range rws {
			fr.ReadWrites[i] = ReadWrite{
				VarName:         C.GoString(rw.var_name),
				EnclosingFuncQN: C.GoString(rw.enclosing_func_qn),
				IsWrite:         bool(rw.is_write),
			}
		}
	}

	// TypeRefs
	if r.type_refs.count > 0 {
		fr.TypeRefs = make([]TypeRef, r.type_refs.count)
		refs := unsafe.Slice(r.type_refs.items, r.type_refs.count)
		for i, tr := range refs {
			fr.TypeRefs[i] = TypeRef{
				TypeName:        C.GoString(tr.type_name),
				EnclosingFuncQN: C.GoString(tr.enclosing_func_qn),
			}
		}
	}

	// EnvAccesses
	if r.env_accesses.count > 0 {
		fr.EnvAccesses = make([]EnvAccess, r.env_accesses.count)
		envs := unsafe.Slice(r.env_accesses.items, r.env_accesses.count)
		for i, ea := range envs {
			fr.EnvAccesses[i] = EnvAccess{
				EnvKey:          C.GoString(ea.env_key),
				EnclosingFuncQN: C.GoString(ea.enclosing_func_qn),
			}
		}
	}

	// TypeAssigns
	if r.type_assigns.count > 0 {
		fr.TypeAssigns = make([]TypeAssign, r.type_assigns.count)
		assigns := unsafe.Slice(r.type_assigns.items, r.type_assigns.count)
		for i, ta := range assigns {
			fr.TypeAssigns[i] = TypeAssign{
				VarName:         C.GoString(ta.var_name),
				TypeName:        C.GoString(ta.type_name),
				EnclosingFuncQN: C.GoString(ta.enclosing_func_qn),
			}
		}
	}

	// ImplTraits (Rust)
	if r.impl_traits.count > 0 {
		fr.ImplTraits = make([]ImplTrait, r.impl_traits.count)
		traits := unsafe.Slice(r.impl_traits.items, r.impl_traits.count)
		for i, it := range traits {
			fr.ImplTraits[i] = ImplTrait{
				TraitName:  C.GoString(it.trait_name),
				StructName: C.GoString(it.struct_name),
			}
		}
	}

	return fr
}

func goStringOrEmpty(s *C.char) string {
	if s == nil {
		return ""
	}
	return C.GoString(s)
}

func goStringSlice(arr **C.char) []string {
	if arr == nil {
		return nil
	}
	var result []string
	for i := 0; ; i++ {
		ptr := *(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(arr)) + uintptr(i)*unsafe.Sizeof(arr)))
		if ptr == nil {
			break
		}
		result = append(result, C.GoString(ptr))
	}
	return result
}
