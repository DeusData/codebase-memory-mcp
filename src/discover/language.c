/*
 * language.c — Language detection from filename and extension.
 *
 * Maps file extensions and special filenames to CBMLanguage enum values.
 * Handles .m disambiguation (Objective-C vs Magma vs MATLAB).
 * Consults the process-global user config (set via cbm_set_user_lang_config)
 * before the built-in lookup table.
 */
#include "discover/discover.h"
#include "discover/userconfig.h"
#include "cbm.h" // CBMLanguage, CBM_LANG_*

#include "foundation/constants.h"

enum { LANG_SCAN_PASSES = 2 };
#define SLEN(s) (sizeof(s) - 1)
#include <ctype.h>
#include <stdio.h>
#include <string.h>

/* ── Extension → Language lookup table ───────────────────────────── */

typedef struct {
    const char *ext; /* including dot, e.g. ".go" */
    CBMLanguage language;
} ext_entry_t;

/* Sorted by extension for binary search (but linear scan is fine for ~120 entries) */
static const ext_entry_t EXT_TABLE[] = {
    /* Bash */
    {".bash", CBM_LANG_BASH},
    {".sh", CBM_LANG_BASH},

    /* C */
    {".c", CBM_LANG_C},

    /* C++ */
    {".cc", CBM_LANG_CPP},
    {".ccm", CBM_LANG_CPP},
    {".cpp", CBM_LANG_CPP},
    {".cppm", CBM_LANG_CPP},
    {".cxx", CBM_LANG_CPP},
    {".h", CBM_LANG_CPP},
    {".hh", CBM_LANG_CPP},
    {".hpp", CBM_LANG_CPP},
    {".hxx", CBM_LANG_CPP},
    {".ixx", CBM_LANG_CPP},

    /* C# */
    {".cs", CBM_LANG_CSHARP},

    /* Clojure */
    {".clj", CBM_LANG_CLOJURE},
    {".cljc", CBM_LANG_CLOJURE},
    {".cljs", CBM_LANG_CLOJURE},

    /* CMake */
    {".cmake", CBM_LANG_CMAKE},

    /* COBOL */
    {".cbl", CBM_LANG_COBOL},
    {".cob", CBM_LANG_COBOL},

    /* Common Lisp */
    {".cl", CBM_LANG_COMMONLISP},
    {".lisp", CBM_LANG_COMMONLISP},
    {".lsp", CBM_LANG_COMMONLISP},

    /* CSS */
    {".css", CBM_LANG_CSS},

    /* CUDA */
    {".cu", CBM_LANG_CUDA},
    {".cuh", CBM_LANG_CUDA},

    /* Dart */
    {".dart", CBM_LANG_DART},

    /* Dockerfile */
    {".dockerfile", CBM_LANG_DOCKERFILE},

    /* Elixir */
    {".ex", CBM_LANG_ELIXIR},
    {".exs", CBM_LANG_ELIXIR},

    /* Elm */
    {".elm", CBM_LANG_ELM},

    /* Emacs Lisp */
    {".el", CBM_LANG_EMACSLISP},

    /* Erlang */
    {".erl", CBM_LANG_ERLANG},

    /* F# */
    {".fs", CBM_LANG_FSHARP},
    {".fsi", CBM_LANG_FSHARP},
    {".fsx", CBM_LANG_FSHARP},

    /* FORM */
    {".frm", CBM_LANG_FORM},
    {".prc", CBM_LANG_FORM},

    /* Fortran */
    {".f03", CBM_LANG_FORTRAN},
    {".f08", CBM_LANG_FORTRAN},
    {".f90", CBM_LANG_FORTRAN},
    {".f95", CBM_LANG_FORTRAN},

    /* GLSL */
    {".frag", CBM_LANG_GLSL},
    {".glsl", CBM_LANG_GLSL},
    {".vert", CBM_LANG_GLSL},

    /* Go */
    {".go", CBM_LANG_GO},

    /* GraphQL */
    {".gql", CBM_LANG_GRAPHQL},
    {".graphql", CBM_LANG_GRAPHQL},

    /* Groovy */
    {".gradle", CBM_LANG_GROOVY},
    {".groovy", CBM_LANG_GROOVY},

    /* Haskell */
    {".hs", CBM_LANG_HASKELL},

    /* HCL / Terraform */
    {".hcl", CBM_LANG_HCL},
    {".tf", CBM_LANG_HCL},

    /* HTML */
    {".htm", CBM_LANG_HTML},
    {".html", CBM_LANG_HTML},

    /* INI */
    {".cfg", CBM_LANG_INI},
    {".conf", CBM_LANG_INI},
    {".ini", CBM_LANG_INI},

    /* Java */
    {".java", CBM_LANG_JAVA},

    /* JavaScript */
    {".js", CBM_LANG_JAVASCRIPT},
    {".jsx", CBM_LANG_JAVASCRIPT},

    /* JSON */
    {".json", CBM_LANG_JSON},

    /* Julia */
    {".jl", CBM_LANG_JULIA},

    /* Kotlin */
    {".kt", CBM_LANG_KOTLIN},
    {".kts", CBM_LANG_KOTLIN},

    /* Lean */
    {".lean", CBM_LANG_LEAN},

    /* Lua */
    {".lua", CBM_LANG_LUA},

    /* Magma */
    {".mag", CBM_LANG_MAGMA},
    {".magma", CBM_LANG_MAGMA},

    /* Makefile */
    {".mk", CBM_LANG_MAKEFILE},

    /* Markdown */
    {".md", CBM_LANG_MARKDOWN},
    {".mdx", CBM_LANG_MARKDOWN},

    /* MATLAB */
    {".matlab", CBM_LANG_MATLAB},
    {".mlx", CBM_LANG_MATLAB},

    /* Meson */
    {".meson", CBM_LANG_MESON},

    /* Nix */
    {".nix", CBM_LANG_NIX},

    /* OCaml */
    {".ml", CBM_LANG_OCAML},
    {".mli", CBM_LANG_OCAML},

    /* Perl */
    {".pl", CBM_LANG_PERL},
    {".pm", CBM_LANG_PERL},

    /* PHP */
    {".php", CBM_LANG_PHP},

    /* Protobuf */
    {".proto", CBM_LANG_PROTOBUF},

    /* Python */
    {".py", CBM_LANG_PYTHON},

    /* R — case insensitive handled separately */
    {".R", CBM_LANG_R},
    {".r", CBM_LANG_R},

    /* Ruby */
    {".gemspec", CBM_LANG_RUBY},
    {".rake", CBM_LANG_RUBY},
    {".rb", CBM_LANG_RUBY},

    /* Rust */
    {".rs", CBM_LANG_RUST},

    /* Scala */
    {".sc", CBM_LANG_SCALA},
    {".scala", CBM_LANG_SCALA},

    /* SCSS */
    {".scss", CBM_LANG_SCSS},

    /* SQL */
    {".sql", CBM_LANG_SQL},

    /* Svelte */
    {".svelte", CBM_LANG_SVELTE},

    /* Swift */
    {".swift", CBM_LANG_SWIFT},

    /* SystemVerilog + Verilog */
    {".sv", CBM_LANG_VERILOG},
    {".v", CBM_LANG_VERILOG},

    /* TOML */
    {".toml", CBM_LANG_TOML},

    /* TSX */
    {".tsx", CBM_LANG_TSX},

    /* TypeScript */
    {".ts", CBM_LANG_TYPESCRIPT},

    /* VimScript */
    {".vim", CBM_LANG_VIMSCRIPT},
    {".vimrc", CBM_LANG_VIMSCRIPT},
    {"justfile", CBM_LANG_JUST},
    {"Justfile", CBM_LANG_JUST},
    {".justfile", CBM_LANG_JUST},
    {"hyprland.conf", CBM_LANG_HYPRLANG},
    {"ssh_config", CBM_LANG_SSHCONFIG},
    {"sshd_config", CBM_LANG_SSHCONFIG},
    {"BUILD", CBM_LANG_STARLARK},
    {"BUILD.bazel", CBM_LANG_STARLARK},
    {"WORKSPACE", CBM_LANG_STARLARK},
    {"WORKSPACE.bazel", CBM_LANG_STARLARK},

    /* Vue */
    {".vue", CBM_LANG_VUE},

    /* Wolfram */
    {".wl", CBM_LANG_WOLFRAM},
    {".wls", CBM_LANG_WOLFRAM},

    /* XML */
    {".xml", CBM_LANG_XML},
    {".xsd", CBM_LANG_XML},
    {".xsl", CBM_LANG_XML},
    {".svg", CBM_LANG_XML},

    /* YAML */
    {".yaml", CBM_LANG_YAML},
    {".yml", CBM_LANG_YAML},

    /* Ada */
    {".adb", CBM_LANG_ADA},

    /* Ada */
    {".ads", CBM_LANG_ADA},

    /* Agda */
    {".agda", CBM_LANG_AGDA},

    /* Astro */
    {".astro", CBM_LANG_ASTRO},

    /* AWK */
    {".awk", CBM_LANG_AWK},

    /* BitBake */
    {".bb", CBM_LANG_BITBAKE},

    /* BitBake */
    {".bbappend", CBM_LANG_BITBAKE},

    /* BitBake */
    {".bbclass", CBM_LANG_BITBAKE},

    /* Beancount */
    {".beancount", CBM_LANG_BEANCOUNT},

    /* BibTeX */
    {".bib", CBM_LANG_BIBTEX},

    /* Bicep */
    {".bicep", CBM_LANG_BICEP},

    /* Blade */
    /* .blade.php handled by userconfig compound extensions, not EXT_TABLE */

    /* Starlark */
    {".bzl", CBM_LANG_STARLARK},

    /* Cairo */
    {".cairo", CBM_LANG_CAIRO},

    /* Cap'n Proto */
    {".capnp", CBM_LANG_CAPNP},

    /* Apex */
    {".cls", CBM_LANG_APEX},

    /* Crystal */
    {".cr", CBM_LANG_CRYSTAL},

    /* CSV */
    {".csv", CBM_LANG_CSV},

    /* D */
    {".d", CBM_LANG_DLANG},

    /* Diff */
    {".diff", CBM_LANG_DIFF},

    /* Pascal */
    {".dpr", CBM_LANG_PASCAL},

    /* DeviceTree */
    {".dts", CBM_LANG_DEVICETREE},

    /* DeviceTree */
    {".dtsi", CBM_LANG_DEVICETREE},

    /* FunC */
    {".fc", CBM_LANG_FUNC},

    /* Fish */
    {".fish", CBM_LANG_FISH},

    /* Fennel */
    {".fnl", CBM_LANG_FENNEL},

    /* HLSL */
    {".fx", CBM_LANG_HLSL},

    /* GDScript */
    {".gd", CBM_LANG_GDSCRIPT},

    /* Gleam */
    {".gleam", CBM_LANG_GLEAM},

    /* GN */
    {".gn", CBM_LANG_GN},

    /* GN */
    {".gni", CBM_LANG_GN},

    /* Go Template */
    {".gotmpl", CBM_LANG_GOTEMPLATE},

    /* Hare */
    {".ha", CBM_LANG_HARE},

    /* Hyprlang */
    {".hl", CBM_LANG_HYPRLANG},

    /* HLSL */
    {".hlsl", CBM_LANG_HLSL},

    /* HLSL */
    {".hlsli", CBM_LANG_HLSL},

    /* ISPC */
    {".ispc", CBM_LANG_ISPC},

    /* Jinja2 */
    {".j2", CBM_LANG_JINJA2},

    /* Janet */
    {".janet", CBM_LANG_JANET},

    /* Jinja2 */
    {".jinja", CBM_LANG_JINJA2},

    /* Jinja2 */
    {".jinja2", CBM_LANG_JINJA2},

    /* JSON5 */
    {".json5", CBM_LANG_JSON5},

    /* Jsonnet */
    {".jsonnet", CBM_LANG_JSONNET},

    /* KDL */
    {".kdl", CBM_LANG_KDL},

    /* Linker Script */
    {".ld", CBM_LANG_LINKERSCRIPT},

    /* Linker Script */
    {".lds", CBM_LANG_LINKERSCRIPT},

    /* Jsonnet */
    {".libsonnet", CBM_LANG_JSONNET},

    /* Liquid */
    {".liquid", CBM_LANG_LIQUID},

    /* LLVM IR */
    {".ll", CBM_LANG_LLVM_IR},

    /* Pascal */
    {".lpr", CBM_LANG_PASCAL},

    /* Luau */
    {".luau", CBM_LANG_LUAU},

    /* Mermaid */
    {".mermaid", CBM_LANG_MERMAID},

    /* Mermaid */
    {".mmd", CBM_LANG_MERMAID},

    /* Move */
    {".move", CBM_LANG_MOVE},

    /* NASM */
    {".nasm", CBM_LANG_NASM},

    /* Nickel */
    {".ncl", CBM_LANG_NICKEL},

    /* Nim */
    {".nim", CBM_LANG_NIM},

    /* Nim */
    {".nims", CBM_LANG_NIM},

    /* Squirrel */
    {".nut", CBM_LANG_SQUIRREL},

    /* Odin */
    {".odin", CBM_LANG_ODIN},

    /* DeviceTree */
    {".overlay", CBM_LANG_DEVICETREE},

    /* Pascal */
    {".pas", CBM_LANG_PASCAL},

    /* Diff */
    {".patch", CBM_LANG_DIFF},

    /* Pkl */
    {".pkl", CBM_LANG_PKL},

    /* PO */
    {".po", CBM_LANG_PO},

    /* Pony */
    {".pony", CBM_LANG_PONY},

    /* PO */
    {".pot", CBM_LANG_PO},

    /* Puppet */
    {".pp", CBM_LANG_PUPPET},

    /* Prisma */
    {".prisma", CBM_LANG_PRISMA},

    /* Properties */
    {".properties", CBM_LANG_PROPERTIES},

    /* PowerShell */
    {".ps1", CBM_LANG_POWERSHELL},

    /* PowerShell */
    {".psd1", CBM_LANG_POWERSHELL},

    /* PowerShell */
    {".psm1", CBM_LANG_POWERSHELL},

    /* PureScript */
    {".purs", CBM_LANG_PURESCRIPT},

    /* ReScript */
    {".res", CBM_LANG_RESCRIPT},

    /* ReScript */
    {".resi", CBM_LANG_RESCRIPT},

    /* Racket */
    {".rkt", CBM_LANG_RACKET},

    /* RON */
    {".ron", CBM_LANG_RON},

    /* reStructuredText */
    {".rst", CBM_LANG_RST},

    /* Assembly */
    {".s", CBM_LANG_ASSEMBLY},

    /* Assembly */
    {".S", CBM_LANG_ASSEMBLY},

    /* Scheme */
    {".scm", CBM_LANG_SCHEME},

    /* Slang */
    {".slang", CBM_LANG_SLANG},

    /* Smali */
    {".smali", CBM_LANG_SMALI},

    /* Smithy */
    {".smithy", CBM_LANG_SMITHY},

    /* Solidity */
    {".sol", CBM_LANG_SOLIDITY},

    /* SOQL */
    {".soql", CBM_LANG_SOQL},

    /* SOSL */
    {".sosl", CBM_LANG_SOSL},

    /* Scheme */
    {".ss", CBM_LANG_SCHEME},

    /* Starlark */
    {".star", CBM_LANG_STARLARK},

    /* SystemVerilog */

    /* SystemVerilog */

    /* Sway */
    {".sw", CBM_LANG_SWAY},

    /* Tcl */
    {".tcl", CBM_LANG_TCL},

    /* TableGen */
    {".td", CBM_LANG_TABLEGEN},

    /* Templ */
    {".templ", CBM_LANG_TEMPL},

    /* Thrift */
    {".thrift", CBM_LANG_THRIFT},

    /* Teal */
    {".tl", CBM_LANG_TEAL},

    /* TLA+ */
    {".tla", CBM_LANG_TLAPLUS},

    /* Go Template */
    {".tmpl", CBM_LANG_GOTEMPLATE},

    /* Apex */
    {".trigger", CBM_LANG_APEX},

    /* Typst */
    {".typ", CBM_LANG_TYPST},

    /* VHDL */
    {".vhd", CBM_LANG_VHDL},

    /* VHDL */
    {".vhdl", CBM_LANG_VHDL},

    /* WGSL */
    {".wgsl", CBM_LANG_WGSL},

    /* WIT */
    {".wit", CBM_LANG_WIT},

    /* Zsh */
    {".zsh", CBM_LANG_ZSH},

    /* Zig */
    {".zig", CBM_LANG_ZIG},
};

#define EXT_TABLE_SIZE (sizeof(EXT_TABLE) / sizeof(EXT_TABLE[0]))

/* ── Special filename → Language lookup ──────────────────────────── */

typedef struct {
    const char *filename;
    CBMLanguage language;
} filename_entry_t;

static const filename_entry_t FILENAME_TABLE[] = {
    {"CMakeLists.txt", CBM_LANG_CMAKE},
    {"Dockerfile", CBM_LANG_DOCKERFILE},
    {"GNUmakefile", CBM_LANG_MAKEFILE},
    {"Makefile", CBM_LANG_MAKEFILE},
    {"makefile", CBM_LANG_MAKEFILE},
    {"meson.build", CBM_LANG_MESON},
    {"meson.options", CBM_LANG_MESON},
    {"meson_options.txt", CBM_LANG_MESON},
    {"kustomization.yaml", CBM_LANG_KUSTOMIZE},
    {"kustomization.yml", CBM_LANG_KUSTOMIZE},
    /* Note: FILENAME_TABLE uses case-sensitive strcmp, so mixed-case variants
     * (e.g. "Kustomization.yaml") are not matched here.  They fall through to
     * CBM_LANG_YAML and are re-classified by cbm_is_kustomize_file() in
     * pass_k8s.c, which performs a case-insensitive comparison.  This is the
     * intended behaviour — no additional entries are needed. */
    {".vimrc", CBM_LANG_VIMSCRIPT},
    {".zshrc", CBM_LANG_ZSH},
    {".zshenv", CBM_LANG_ZSH},
    {".zprofile", CBM_LANG_ZSH},
    {"justfile", CBM_LANG_JUST},
    {"Justfile", CBM_LANG_JUST},
    {".justfile", CBM_LANG_JUST},
    {"hyprland.conf", CBM_LANG_HYPRLANG},
    {"ssh_config", CBM_LANG_SSHCONFIG},
    {"sshd_config", CBM_LANG_SSHCONFIG},
    {".ssh/config", CBM_LANG_SSHCONFIG},
    {"BUILD", CBM_LANG_STARLARK},
    {"BUILD.bazel", CBM_LANG_STARLARK},
    {"WORKSPACE", CBM_LANG_STARLARK},
    {"WORKSPACE.bazel", CBM_LANG_STARLARK},
    {"requirements.txt", CBM_LANG_REQUIREMENTS},
    {"requirements-dev.txt", CBM_LANG_REQUIREMENTS},
    {"requirements-test.txt", CBM_LANG_REQUIREMENTS},
    {"Kconfig", CBM_LANG_KCONFIG},
    {"go.mod", CBM_LANG_GOMOD},

};

#define FILENAME_TABLE_SIZE (sizeof(FILENAME_TABLE) / sizeof(FILENAME_TABLE[0]))

/* ── Language names ──────────────────────────────────────────────── */

static const char *LANG_NAMES[CBM_LANG_COUNT] = {
    [CBM_LANG_GO] = "Go",
    [CBM_LANG_PYTHON] = "Python",
    [CBM_LANG_JAVASCRIPT] = "JavaScript",
    [CBM_LANG_TYPESCRIPT] = "TypeScript",
    [CBM_LANG_TSX] = "TSX",
    [CBM_LANG_RUST] = "Rust",
    [CBM_LANG_JAVA] = "Java",
    [CBM_LANG_CPP] = "C++",
    [CBM_LANG_CSHARP] = "C#",
    [CBM_LANG_PHP] = "PHP",
    [CBM_LANG_LUA] = "Lua",
    [CBM_LANG_SCALA] = "Scala",
    [CBM_LANG_KOTLIN] = "Kotlin",
    [CBM_LANG_RUBY] = "Ruby",
    [CBM_LANG_C] = "C",
    [CBM_LANG_BASH] = "Bash",
    [CBM_LANG_ZIG] = "Zig",
    [CBM_LANG_ELIXIR] = "Elixir",
    [CBM_LANG_HASKELL] = "Haskell",
    [CBM_LANG_OCAML] = "OCaml",
    [CBM_LANG_OBJC] = "Objective-C",
    [CBM_LANG_SWIFT] = "Swift",
    [CBM_LANG_DART] = "Dart",
    [CBM_LANG_PERL] = "Perl",
    [CBM_LANG_GROOVY] = "Groovy",
    [CBM_LANG_ERLANG] = "Erlang",
    [CBM_LANG_R] = "R",
    [CBM_LANG_HTML] = "HTML",
    [CBM_LANG_CSS] = "CSS",
    [CBM_LANG_SCSS] = "SCSS",
    [CBM_LANG_YAML] = "YAML",
    [CBM_LANG_TOML] = "TOML",
    [CBM_LANG_HCL] = "HCL",
    [CBM_LANG_SQL] = "SQL",
    [CBM_LANG_DOCKERFILE] = "Dockerfile",
    [CBM_LANG_CLOJURE] = "Clojure",
    [CBM_LANG_FSHARP] = "F#",
    [CBM_LANG_JULIA] = "Julia",
    [CBM_LANG_VIMSCRIPT] = "VimScript",
    [CBM_LANG_NIX] = "Nix",
    [CBM_LANG_COMMONLISP] = "Common Lisp",
    [CBM_LANG_ELM] = "Elm",
    [CBM_LANG_FORTRAN] = "Fortran",
    [CBM_LANG_CUDA] = "CUDA",
    [CBM_LANG_COBOL] = "COBOL",
    [CBM_LANG_VERILOG] = "Verilog",
    [CBM_LANG_EMACSLISP] = "Emacs Lisp",
    [CBM_LANG_JSON] = "JSON",
    [CBM_LANG_XML] = "XML",
    [CBM_LANG_MARKDOWN] = "Markdown",
    [CBM_LANG_MAKEFILE] = "Makefile",
    [CBM_LANG_CMAKE] = "CMake",
    [CBM_LANG_PROTOBUF] = "Protobuf",
    [CBM_LANG_GRAPHQL] = "GraphQL",
    [CBM_LANG_VUE] = "Vue",
    [CBM_LANG_SVELTE] = "Svelte",
    [CBM_LANG_MESON] = "Meson",
    [CBM_LANG_GLSL] = "GLSL",
    [CBM_LANG_INI] = "INI",
    [CBM_LANG_MATLAB] = "MATLAB",
    [CBM_LANG_LEAN] = "Lean",
    [CBM_LANG_FORM] = "FORM",
    [CBM_LANG_MAGMA] = "Magma",
    [CBM_LANG_WOLFRAM] = "Wolfram",
    [CBM_LANG_KUSTOMIZE] = "Kustomize",
    [CBM_LANG_K8S] = "Kubernetes",
    [CBM_LANG_SOLIDITY] = "Solidity",
    [CBM_LANG_TYPST] = "Typst",
    [CBM_LANG_GDSCRIPT] = "GDScript",
    [CBM_LANG_GLEAM] = "Gleam",
    [CBM_LANG_POWERSHELL] = "PowerShell",
    [CBM_LANG_PASCAL] = "Pascal",
    [CBM_LANG_DLANG] = "D",
    [CBM_LANG_NIM] = "Nim",
    [CBM_LANG_SCHEME] = "Scheme",
    [CBM_LANG_FENNEL] = "Fennel",
    [CBM_LANG_FISH] = "Fish",
    [CBM_LANG_AWK] = "AWK",
    [CBM_LANG_ZSH] = "Zsh",
    [CBM_LANG_TCL] = "Tcl",
    [CBM_LANG_ADA] = "Ada",
    [CBM_LANG_AGDA] = "Agda",
    [CBM_LANG_RACKET] = "Racket",
    [CBM_LANG_ODIN] = "Odin",
    [CBM_LANG_RESCRIPT] = "ReScript",
    [CBM_LANG_PURESCRIPT] = "PureScript",
    [CBM_LANG_NICKEL] = "Nickel",
    [CBM_LANG_CRYSTAL] = "Crystal",
    [CBM_LANG_TEAL] = "Teal",
    [CBM_LANG_HARE] = "Hare",
    [CBM_LANG_PONY] = "Pony",
    [CBM_LANG_LUAU] = "Luau",
    [CBM_LANG_JANET] = "Janet",
    [CBM_LANG_SWAY] = "Sway",
    [CBM_LANG_NASM] = "NASM",
    [CBM_LANG_ASSEMBLY] = "Assembly",
    [CBM_LANG_ASTRO] = "Astro",
    [CBM_LANG_BLADE] = "Blade",
    [CBM_LANG_JUST] = "Just",
    [CBM_LANG_GOTEMPLATE] = "Go Template",
    [CBM_LANG_TEMPL] = "Templ",
    [CBM_LANG_LIQUID] = "Liquid",
    [CBM_LANG_JINJA2] = "Jinja2",
    [CBM_LANG_PRISMA] = "Prisma",
    [CBM_LANG_HYPRLANG] = "Hyprlang",
    [CBM_LANG_DOTENV] = "DotEnv",
    [CBM_LANG_SYSTEMVERILOG] = "SystemVerilog",
    [CBM_LANG_DIFF] = "Diff",
    [CBM_LANG_WGSL] = "WGSL",
    [CBM_LANG_KDL] = "KDL",
    [CBM_LANG_JSON5] = "JSON5",
    [CBM_LANG_JSONNET] = "Jsonnet",
    [CBM_LANG_RON] = "RON",
    [CBM_LANG_THRIFT] = "Thrift",
    [CBM_LANG_CAPNP] = "Cap'n Proto",
    [CBM_LANG_PROPERTIES] = "Properties",
    [CBM_LANG_SSHCONFIG] = "SSH Config",
    [CBM_LANG_BIBTEX] = "BibTeX",
    [CBM_LANG_STARLARK] = "Starlark",
    [CBM_LANG_BICEP] = "Bicep",
    [CBM_LANG_CSV] = "CSV",
    [CBM_LANG_REQUIREMENTS] = "Requirements",
    [CBM_LANG_HLSL] = "HLSL",
    [CBM_LANG_VHDL] = "VHDL",
    [CBM_LANG_DEVICETREE] = "DeviceTree",
    [CBM_LANG_LINKERSCRIPT] = "Linker Script",
    [CBM_LANG_GN] = "GN",
    [CBM_LANG_KCONFIG] = "Kconfig",
    [CBM_LANG_BITBAKE] = "BitBake",
    [CBM_LANG_SMALI] = "Smali",
    [CBM_LANG_TABLEGEN] = "TableGen",
    [CBM_LANG_ISPC] = "ISPC",
    [CBM_LANG_CAIRO] = "Cairo",
    [CBM_LANG_MOVE] = "Move",
    [CBM_LANG_SQUIRREL] = "Squirrel",
    [CBM_LANG_FUNC] = "FunC",
    [CBM_LANG_REGEX] = "Regex",
    [CBM_LANG_JSDOC] = "JSDoc",
    [CBM_LANG_RST] = "reStructuredText",
    [CBM_LANG_BEANCOUNT] = "Beancount",
    [CBM_LANG_MERMAID] = "Mermaid",
    [CBM_LANG_PUPPET] = "Puppet",
    [CBM_LANG_PO] = "PO",
    [CBM_LANG_GITATTRIBUTES] = "gitattributes",
    [CBM_LANG_GITIGNORE] = "gitignore",
    [CBM_LANG_SLANG] = "Slang",
    [CBM_LANG_LLVM_IR] = "LLVM IR",
    [CBM_LANG_SMITHY] = "Smithy",
    [CBM_LANG_WIT] = "WIT",
    [CBM_LANG_TLAPLUS] = "TLA+",
    [CBM_LANG_PKL] = "Pkl",
    [CBM_LANG_GOMOD] = "Go Mod",
    [CBM_LANG_APEX] = "Apex",
    [CBM_LANG_SOQL] = "SOQL",
    [CBM_LANG_SOSL] = "SOSL",

};

/* ── Public API ──────────────────────────────────────────────────── */

CBMLanguage cbm_language_for_extension(const char *ext) {
    if (!ext || !ext[0]) {
        return CBM_LANG_COUNT;
    }

    /* Check user-defined overrides first */
    const cbm_userconfig_t *ucfg = cbm_get_user_lang_config();
    if (ucfg) {
        CBMLanguage ulang = cbm_userconfig_lookup(ucfg, ext);
        if (ulang != CBM_LANG_COUNT) {
            return ulang;
        }
    }

    for (size_t i = 0; i < EXT_TABLE_SIZE; i++) {
        if (strcmp(EXT_TABLE[i].ext, ext) == 0) {
            return EXT_TABLE[i].language;
        }
    }
    return CBM_LANG_COUNT;
}

CBMLanguage cbm_language_for_filename(const char *filename) {
    if (!filename || !filename[0]) {
        return CBM_LANG_COUNT;
    }

    /* Check special filenames first */
    for (size_t i = 0; i < FILENAME_TABLE_SIZE; i++) {
        if (strcmp(FILENAME_TABLE[i].filename, filename) == 0) {
            return FILENAME_TABLE[i].language;
        }
    }

    /* Fall back to extension-based lookup.
     * For compound extensions (e.g. ".blade.php") defined in the user config,
     * scan from the first dot in the basename toward the last, checking user
     * config at each position.  Built-in extensions use the last dot only. */
    const char *last_dot = strrchr(filename, '.');
    if (!last_dot) {
        return CBM_LANG_COUNT;
    }

    /* Probe user config for compound extensions (e.g. ".blade.php"). */
    const cbm_userconfig_t *ucfg = cbm_get_user_lang_config();
    if (ucfg) {
        const char *p = strchr(filename, '.');
        while (p && p < last_dot) {
            CBMLanguage lang = cbm_userconfig_lookup(ucfg, p);
            if (lang != CBM_LANG_COUNT) {
                return lang;
            }
            p = strchr(p + SKIP_ONE, '.');
        }
    }

    /* Standard single-extension lookup (built-ins + user overrides). */
    return cbm_language_for_extension(last_dot);
}

const char *cbm_language_name(CBMLanguage lang) {
    if (lang < 0 || lang >= CBM_LANG_COUNT) {
        return "Unknown";
    }
    return LANG_NAMES[lang] ? LANG_NAMES[lang] : "Unknown";
}

/* ── .m file disambiguation ──────────────────────────────────────── */

/* Simple substring search helper */
static bool str_contains(const char *haystack, const char *needle) {
    return strstr(haystack, needle) != NULL;
}

static bool has_objc_markers(const char *buf) {
    return str_contains(buf, "@interface") || str_contains(buf, "@implementation") ||
           str_contains(buf, "@protocol") || str_contains(buf, "@property") ||
           str_contains(buf, "#import") || str_contains(buf, "@selector") ||
           str_contains(buf, "@encode") || str_contains(buf, "@synthesize") ||
           str_contains(buf, "@dynamic");
}

static bool has_magma_end_markers(const char *buf) {
    return str_contains(buf, "end function;") || str_contains(buf, "end procedure;") ||
           str_contains(buf, "end intrinsic;") || str_contains(buf, "end if;") ||
           str_contains(buf, "end for;") || str_contains(buf, "end while;");
}

/* Check for "intrinsic Name(" or "procedure Name(" patterns. */
static bool has_magma_callable_pattern(const char *buf) {
    const char *markers[] = {"intrinsic ", "procedure "};
    for (int i = 0; i < LANG_SCAN_PASSES; i++) {
        const char *p = strstr(buf, markers[i]);
        if (!p) {
            continue;
        }
        p += strlen(markers[i]);
        while (*p && isalpha((unsigned char)*p)) {
            p++;
        }
        if (*p == '(') {
            return true;
        }
    }
    return false;
}

/* Scan lines for MATLAB-specific markers (function/classdef/%%). */
static bool has_matlab_line_markers(const char *buf) {
    const char *line = buf;
    while (*line) {
        const char *p = line;
        while (*p == ' ' || *p == '\t') {
            p++;
        }
        if (strncmp(p, "function ", SLEN("function ")) == 0 ||
            strncmp(p, "function\t", SLEN("function\t")) == 0 ||
            strncmp(p, "classdef ", SLEN("classdef ")) == 0 ||
            strncmp(p, "classdef\t", SLEN("classdef\t")) == 0 || strncmp(p, "%%", PAIR_LEN) == 0 ||
            (*p == '%' && *(p + SKIP_ONE) != '{')) {
            return true;
        }
        const char *nl = strchr(line, '\n');
        if (!nl) {
            break;
        }
        line = nl + SKIP_ONE;
    }
    return false;
}

CBMLanguage cbm_disambiguate_m(const char *path) {
    if (!path) {
        return CBM_LANG_MATLAB;
    }

    FILE *f = fopen(path, "r");
    if (!f) {
        return CBM_LANG_MATLAB;
    }

    /* Read first 4KB */
    char buf[CBM_SZ_4K + SKIP_ONE];
    size_t n = fread(buf, SKIP_ONE, CBM_SZ_4K, f);
    buf[n] = '\0';
    (void)fclose(f);

    if (has_objc_markers(buf)) {
        return CBM_LANG_OBJC;
    }
    if (has_magma_end_markers(buf)) {
        return CBM_LANG_MAGMA;
    }
    if ((str_contains(buf, "intrinsic ") || str_contains(buf, "procedure ")) &&
        has_magma_callable_pattern(buf)) {
        return CBM_LANG_MAGMA;
    }
    if (has_matlab_line_markers(buf)) {
        return CBM_LANG_MATLAB;
    }

    return CBM_LANG_MATLAB;
}
