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
#include "foundation/compat.h" // cbm_strcasestr
#include "foundation/compat_fs.h"

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
    /* Blazor components. The C# grammar recovers the @code block; the
     * surrounding markup parses as ERROR regions and is reported via
     * parse_partial, which is why this is a best-effort mapping rather
     * than a dedicated grammar. */
    {".razor", CBM_LANG_CSHARP},
    /* Razor Pages / MVC views. Same Razor syntax and the same C# host as
     * .razor, and equally unmapped before this: an ASP.NET Core app's views
     * produced no nodes at all. The `@page` directive that defines a Razor
     * Page lives in this file type, so the route extraction below matters
     * more here than it does for components. Best-effort on the same terms:
     * the C# grammar recovers the @{ } / @functions blocks, the surrounding
     * markup lands in ERROR regions and is reported via parse_partial. */
    {".cshtml", CBM_LANG_CSHARP},

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

    /* DotEnv */
    {".env", CBM_LANG_DOTENV},

    /* Elm */
    {".elm", CBM_LANG_ELM},

    /* ArkTS (HarmonyOS/OpenHarmony) */
    {".ets", CBM_LANG_ARKTS},

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

    /* Fortran. Upper-case suffixes are the C-preprocessed spellings of the
     * same free-form sources (#ifdef blocks); lookup is case-sensitive. */
    {".f03", CBM_LANG_FORTRAN},
    {".f08", CBM_LANG_FORTRAN},
    {".f90", CBM_LANG_FORTRAN},
    {".f95", CBM_LANG_FORTRAN},
    {".F03", CBM_LANG_FORTRAN},
    {".F08", CBM_LANG_FORTRAN},
    {".F90", CBM_LANG_FORTRAN},
    {".F95", CBM_LANG_FORTRAN},

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
    {".mjs", CBM_LANG_JAVASCRIPT}, /* ES modules (#197) */
    {".cjs", CBM_LANG_JAVASCRIPT}, /* CommonJS modules */

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
    {".m", CBM_LANG_MATLAB},
    {".matlab", CBM_LANG_MATLAB},
    {".mlx", CBM_LANG_MATLAB},

    /* Meson */
    {".meson", CBM_LANG_MESON},

    /* Mojo */
    {".mojo", CBM_LANG_MOJO},

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

    /* Oracle PL/SQL (do not map .sql — stays generic SQL; .prc stays FORM) */
    {".pks", CBM_LANG_PLSQL},
    {".pkb", CBM_LANG_PLSQL},
    {".pck", CBM_LANG_PLSQL},
    {".pls", CBM_LANG_PLSQL},
    {".plb", CBM_LANG_PLSQL},
    {".plsql", CBM_LANG_PLSQL},
    {".fnc", CBM_LANG_PLSQL},
    {".trg", CBM_LANG_PLSQL},
    {".bdy", CBM_LANG_PLSQL},
    {".tps", CBM_LANG_PLSQL},
    {".tpb", CBM_LANG_PLSQL},

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
    {".mts", CBM_LANG_TYPESCRIPT}, /* TS ES modules */
    {".cts", CBM_LANG_TYPESCRIPT}, /* TS CommonJS modules */

    /* VimScript */
    {".vim", CBM_LANG_VIMSCRIPT},
    {".vimrc", CBM_LANG_VIMSCRIPT},
    {"justfile", CBM_LANG_JUST},
    {"Justfile", CBM_LANG_JUST},
    {".justfile", CBM_LANG_JUST},
    {".just", CBM_LANG_JUST}, /* `import 'common.just'` target files */
    {"hyprland.conf", CBM_LANG_HYPRLANG},
    {"ssh_config", CBM_LANG_SSHCONFIG},
    {"sshd_config", CBM_LANG_SSHCONFIG},
    {"BUILD", CBM_LANG_STARLARK},
    {"BUILD.bazel", CBM_LANG_STARLARK},
    {"WORKSPACE", CBM_LANG_STARLARK},
    {"WORKSPACE.bazel", CBM_LANG_STARLARK},

    /* BitBake include fragments — `require/include foo.inc` target files.
     * NOTE: .inc is also used by ObjectScript include (macro) files; the
     * ambiguity is resolved by content in cbm_disambiguate_inc(). */
    {".inc", CBM_LANG_BITBAKE},

    /* InterSystems ObjectScript routines (.mac/.int/.rtn unambiguous; .cls is
     * shared with Apex and resolved by content in cbm_disambiguate_cls()). */
    {".mac", CBM_LANG_OBJECTSCRIPT_ROUTINE},
    {".int", CBM_LANG_OBJECTSCRIPT_ROUTINE},
    {".rtn", CBM_LANG_OBJECTSCRIPT_ROUTINE},

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
    /* MSBuild project system. Plain XML documents, and the files that carry a
     * .NET repository's package references, target frameworks, build hooks and
     * project layout — none of which were reachable while these were unmapped. */
    {".csproj", CBM_LANG_XML},
    {".vbproj", CBM_LANG_XML},
    {".fsproj", CBM_LANG_XML},
    {".props", CBM_LANG_XML},
    {".targets", CBM_LANG_XML},
    {".nuspec", CBM_LANG_XML},
    {".slnx", CBM_LANG_XML},
    {".runsettings", CBM_LANG_XML},
    /* .NET resource files: the localized strings a UI reads back by key. */
    {".resx", CBM_LANG_XML},
    /* XAML views: WPF, WinUI, MAUI (.xaml) and Avalonia (.axaml). */
    {".xaml", CBM_LANG_XML},
    {".axaml", CBM_LANG_XML},
    /* Application manifests: Apple property lists and privacy manifests, Win32
     * side-by-side manifests and MSIX packages. */
    {".plist", CBM_LANG_XML},
    {".xcprivacy", CBM_LANG_XML},
    {".manifest", CBM_LANG_XML},
    {".appxmanifest", CBM_LANG_XML},

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
    {".tpl", CBM_LANG_GOTEMPLATE}, /* Helm _helpers.tpl named-template definitions */

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

    /* Qt QML */
    {".qml", CBM_LANG_QML},

    /* CFML / ColdFusion — .cfm are tag templates; .cfc components may be EITHER
     * script-dialect (component { ... }) or tag-dialect (<cfcomponent> ...). The
     * table default is script; tag-based .cfc are resolved by content in
     * cbm_disambiguate_cfc(). */
    {".cfc", CBM_LANG_CFSCRIPT},
    {".cfm", CBM_LANG_CFML},

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

    /* Nim */

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

    /* Pine Script */
    {".pine", CBM_LANG_PINE},

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

    /* Regex */
    {".re", CBM_LANG_REGEX},

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

    /* Chialisp — .clsp puzzles, .clib/.clinc includable libraries */
    {".clsp", CBM_LANG_CHIALISP},
    {".clib", CBM_LANG_CHIALISP},
    {".clinc", CBM_LANG_CHIALISP},
    {".pdf", CBM_LANG_PDF},
    /* AsciiDoc; `.asc` is left out: it is as often an ASCII-armored signature */
    {".adoc", CBM_LANG_ASCIIDOC},
    {".asciidoc", CBM_LANG_ASCIIDOC},

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
    {".env", CBM_LANG_DOTENV},
    {".env.local", CBM_LANG_DOTENV},
    {".gitattributes", CBM_LANG_GITATTRIBUTES},

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
    [CBM_LANG_PINE] = "PineScript",
    [CBM_LANG_SOLIDITY] = "Solidity",
    [CBM_LANG_TYPST] = "Typst",
    [CBM_LANG_GDSCRIPT] = "GDScript",
    [CBM_LANG_GLEAM] = "Gleam",
    [CBM_LANG_POWERSHELL] = "PowerShell",
    [CBM_LANG_PASCAL] = "Pascal",
    [CBM_LANG_DLANG] = "D",
    [CBM_LANG_NIM] = "Nim",
    [CBM_LANG_SCHEME] = "Scheme",
    [CBM_LANG_CHIALISP] = "Chialisp",
    [CBM_LANG_PDF] = "PDF",
    [CBM_LANG_ASCIIDOC] = "AsciiDoc",
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
    [CBM_LANG_QML] = "QML",
    [CBM_LANG_CFSCRIPT] = "CFML",
    [CBM_LANG_CFML] = "CFML",
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
    [CBM_LANG_MOJO] = "Mojo",
    [CBM_LANG_OBJECTSCRIPT_UDL] = "ObjectScript UDL",
    [CBM_LANG_OBJECTSCRIPT_ROUTINE] = "ObjectScript Routine",
    [CBM_LANG_OBJECTSCRIPT_EXPORT] = "ObjectScript Export XML",
    [CBM_LANG_ARKTS] = "ArkTS",
    [CBM_LANG_PLSQL] = "PL/SQL",

};

/* ── Public API ──────────────────────────────────────────────────── */

static CBMLanguage lang_for_extension(const cbm_userconfig_t *ucfg, const char *ext) {
    if (!ext || !ext[0]) {
        return CBM_LANG_COUNT;
    }

    /* Check user-defined overrides first */
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

CBMLanguage cbm_language_for_extension(const char *ext) {
    return lang_for_extension(cbm_get_user_lang_config(), ext);
}

static CBMLanguage lang_for_filename(const cbm_userconfig_t *ucfg, const char *filename) {
    if (!filename || !filename[0]) {
        return CBM_LANG_COUNT;
    }

    /* Check special filenames first */
    for (size_t i = 0; i < FILENAME_TABLE_SIZE; i++) {
        if (strcmp(FILENAME_TABLE[i].filename, filename) == 0) {
            return FILENAME_TABLE[i].language;
        }
    }

    /* DotEnv variant filenames (".env.local", ".env.production", …): the
     * filename starts with ".env." but its last "extension" (e.g. ".local")
     * is not a real language extension.  Match the dotenv convention used by
     * pass_envscan/pass_infrascan (".env" exact, ".env." prefix, "*.env"
     * suffix) so file-index routing agrees with direct extraction. */
    if (strncmp(filename, ".env.", SLEN(".env.")) == 0) {
        return CBM_LANG_DOTENV;
    }

    /* Fall back to extension-based lookup.
     * For compound extensions (e.g. ".blade.php") defined in the user config,
     * scan from the first dot in the basename toward the last, checking user
     * config at each position.  Built-in extensions use the last dot only. */
    const char *last_dot = strrchr(filename, '.');
    if (!last_dot) {
        return CBM_LANG_COUNT;
    }

    /* Probe compound extensions (e.g. ".blade.php") from the first dot toward
     * the last. Built-in compounds are checked first so e.g. Laravel Blade
     * templates map to Blade rather than the single-extension fallback (PHP);
     * user config can still add more (#258). */
    static const struct {
        const char *ext;
        CBMLanguage lang;
    } COMPOUND_EXT_TABLE[] = {
        {".blade.php", CBM_LANG_BLADE},
    };
    const char *p = strchr(filename, '.');
    while (p && p < last_dot) {
        for (size_t i = 0; i < sizeof(COMPOUND_EXT_TABLE) / sizeof(COMPOUND_EXT_TABLE[0]); i++) {
            if (strcmp(p, COMPOUND_EXT_TABLE[i].ext) == 0) {
                return COMPOUND_EXT_TABLE[i].lang;
            }
        }
        if (ucfg) {
            CBMLanguage lang = cbm_userconfig_lookup(ucfg, p);
            if (lang != CBM_LANG_COUNT) {
                return lang;
            }
        }
        p = strchr(p + SKIP_ONE, '.');
    }

    /* Standard single-extension lookup (built-ins + user overrides). */
    return lang_for_extension(ucfg, last_dot);
}

CBMLanguage cbm_language_for_filename(const char *filename) {
    return lang_for_filename(cbm_get_user_lang_config(), filename);
}

const char *cbm_language_name(CBMLanguage lang) {
    if (lang < 0 || lang >= CBM_LANG_COUNT) {
        return "Unknown";
    }
    return LANG_NAMES[lang] ? LANG_NAMES[lang] : "Unknown";
}

/* ── Shebang interpreter detection (extensionless scripts) ────────── */

/* Basename of an interpreter path: the segment after the last '/'.  Shebangs
 * are a POSIX convention, so only '/' is treated as a separator. */
static const char *interp_basename(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + SKIP_ONE : path;
}

/* "python" optionally followed by an explicit numeric version (digits and dots
 * only, e.g. "python3", "python3.12").  Bounded and explicit so arbitrary
 * suffixes like "python-wrapper" are rejected. */
static bool is_python_interp(const char *base) {
    if (strncmp(base, "python", SLEN("python")) != 0) {
        return false;
    }
    const char *version = base + SLEN("python");
    if (*version == '\0') {
        return true;
    }

    /* Each numeric component must contain at least one digit. */
    bool need_digit = true;
    for (const char *v = version; *v; v++) {
        if (isdigit((unsigned char)*v)) {
            need_digit = false;
        } else if (*v == '.' && !need_digit) {
            need_digit = true;
        } else {
            return false;
        }
    }
    return !need_digit;
}

/* Map an interpreter basename to a language, or CBM_LANG_COUNT if unrecognized.
 * Non-python interpreters are matched exactly (no prefix/suffix logic). */
static CBMLanguage lang_for_interpreter(const char *base) {
    if (is_python_interp(base)) {
        return CBM_LANG_PYTHON;
    }
    static const struct {
        const char *name;
        CBMLanguage lang;
    } INTERP_TABLE[] = {
        {"sh", CBM_LANG_BASH},           {"bash", CBM_LANG_BASH}, {"dash", CBM_LANG_BASH},
        {"ksh", CBM_LANG_BASH},          {"zsh", CBM_LANG_BASH},  {"node", CBM_LANG_JAVASCRIPT},
        {"nodejs", CBM_LANG_JAVASCRIPT}, {"ruby", CBM_LANG_RUBY}, {"perl", CBM_LANG_PERL},
        {"php", CBM_LANG_PHP},           {"lua", CBM_LANG_LUA},
    };
    for (size_t i = 0; i < sizeof(INTERP_TABLE) / sizeof(INTERP_TABLE[0]); i++) {
        if (strcmp(base, INTERP_TABLE[i].name) == 0) {
            return INTERP_TABLE[i].lang;
        }
    }
    return CBM_LANG_COUNT;
}

/* Advance *cursor past leading blanks and return the next whitespace-delimited
 * token (NUL-terminated in place), or NULL when the line is exhausted. */
static char *shebang_next_token(char **cursor) {
    char *p = *cursor;
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    if (*p == '\0') {
        *cursor = p;
        return NULL;
    }
    char *start = p;
    while (*p && *p != ' ' && *p != '\t') {
        p++;
    }
    if (*p) {
        *p = '\0';
        p++;
    }
    *cursor = p;
    return start;
}

/* ── Content probes ────────────────────────────────────────────────
 * A few names do not decide a language alone; the file's first bytes do. Every
 * probe is a function of those bytes only, so discovery (which reads files)
 * and the pinned test-impact inventory (which reads git blobs) give a file the
 * same language. The bytes are raw on every platform: a Windows text-mode read
 * dropped carriage returns and stopped at a Ctrl-Z, which made the answer
 * depend on the host. */
enum {
    LANG_PROBE_LINE = 255,       /* a shebang first line; the ObjectScript export marker */
    LANG_PROBE_HEAD = CBM_SZ_4K, /* .m .cls .inc .frm .res */
    LANG_PROBE_CFC = CBM_SZ_16K, /* .cfc: a long license block may precede the first tag */
};

/* The first min(n, cap) bytes as a C string in dst[cap + 1]. The probes are
 * string scans, so an embedded NUL ends what they see, as it always did. */
static void lang_head_text(char *dst, size_t cap, const unsigned char *head, size_t n) {
    size_t len = n < cap ? n : cap;
    if (len > 0) {
        memcpy(dst, head, len);
    }
    dst[len] = '\0';
}

/* The first `cap` bytes of a file, raw. false when the file cannot be opened.
 * A read error keeps the bytes read before it, as the probes always did. */
static bool lang_read_head(const char *path, unsigned char *buf, size_t cap, size_t *n) {
    *n = 0;
    FILE *f = path ? cbm_fopen(path, "rb") : NULL;
    if (!f) {
        return false;
    }
    *n = fread(buf, SKIP_ONE, cap, f);
    (void)fclose(f);
    return true;
}

/* `more`: the file has bytes beyond head[0..n). A first line that fills the
 * probe and goes on is cut, and a cut line names no interpreter. */
static CBMLanguage lang_shebang(const unsigned char *head, size_t n, bool more) {
    if (n > LANG_PROBE_LINE) {
        n = LANG_PROBE_LINE;
        more = true;
    }
    if (n < PAIR_LEN) {
        return CBM_LANG_COUNT; /* too short to begin with "#!" */
    }
    char buf[LANG_PROBE_LINE + SKIP_ONE];
    memcpy(buf, head, n);
    bool have_newline = (memchr(buf, '\n', n) != NULL);
    if (!have_newline && n == LANG_PROBE_LINE && more) {
        return CBM_LANG_COUNT;
    }

    /* Must begin with "#!". */
    if (buf[0] != '#' || buf[1] != '!') {
        return CBM_LANG_COUNT;
    }

    /* Isolate the first line; reject an embedded NUL before the newline. */
    size_t line_len = 0;
    while (line_len < n && buf[line_len] != '\n') {
        if (buf[line_len] == '\0') {
            return CBM_LANG_COUNT; /* embedded NUL — treat as binary */
        }
        line_len++;
    }
    /* Trim a trailing CR so CRLF first lines parse. */
    if (line_len > 0 && buf[line_len - SKIP_ONE] == '\r') {
        line_len--;
    }
    buf[line_len] = '\0';

    /* First token after "#!" is the interpreter (or env). */
    char *cursor = buf + PAIR_LEN;
    char *interp = shebang_next_token(&cursor);
    if (!interp) {
        return CBM_LANG_COUNT;
    }
    const char *base = interp_basename(interp);

    /* "env [-S] <interp> [args...]": the real interpreter is the next token.
     * Only the plain "env <interp>" and "env -S/--split-string <interp> [args]"
     * shapes are supported. After the optional -S, the interpreter token must
     * be a real command, so reject option tokens (leading '-') and NAME=value
     * assignments (containing '=') -- e.g. "env PYTHON=/usr/bin/python
     * python-wrapper", where env would treat the first token as an env-var
     * setting rather than the program to run. */
    if (strcmp(base, "env") == 0) {
        char *tok = shebang_next_token(&cursor);
        if (tok && (strcmp(tok, "-S") == 0 || strcmp(tok, "--split-string") == 0)) {
            tok = shebang_next_token(&cursor);
        }
        if (!tok || tok[0] == '-' || strchr(tok, '=') != NULL) {
            return CBM_LANG_COUNT;
        }
        base = interp_basename(tok);
    }

    return lang_for_interpreter(base);
}

CBMLanguage cbm_language_from_shebang(const char *path) {
    if (!path) {
        return CBM_LANG_COUNT;
    }

    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return CBM_LANG_COUNT; /* fail closed on read error */
    }

    /* Read only a bounded first line, plus one byte to tell an exact end of
     * file from a longer first line. A read error fails closed rather than
     * parsing a partial buffer. */
    unsigned char buf[LANG_PROBE_LINE + SKIP_ONE];
    size_t n = fread(buf, SKIP_ONE, sizeof(buf), f);
    bool failed = ferror(f) != 0;
    (void)fclose(f);
    if (failed) {
        return CBM_LANG_COUNT;
    }
    return lang_shebang(buf, n > LANG_PROBE_LINE ? LANG_PROBE_LINE : n, n > LANG_PROBE_LINE);
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

static CBMLanguage lang_m_text(const char *buf) {
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

CBMLanguage cbm_disambiguate_m(const char *path) {
    unsigned char head[LANG_PROBE_HEAD];
    size_t n = 0;
    if (!lang_read_head(path, head, sizeof(head), &n)) {
        return CBM_LANG_MATLAB;
    }
    char buf[LANG_PROBE_HEAD + SKIP_ONE];
    lang_head_text(buf, LANG_PROBE_HEAD, head, n);
    return lang_m_text(buf);
}

/* Visual Basic 6 / VBA source exports are recognisable from their header (#721):
 * every module carries `Attribute VB_Name = "..."`, class modules open with
 * `VERSION 1.0 CLASS`, forms/controls with `VERSION 5.00` + `Begin VB.Form` /
 * `Begin VB.UserControl`. None of these occur in Apex or ObjectScript classes
 * or in FORM programs, whose .cls / .frm extensions VB6 happens to share. */
static bool has_vb6_markers(const char *buf) {
    return str_contains(buf, "Attribute VB_Name") || str_contains(buf, "VERSION 1.0 CLASS") ||
           str_contains(buf, "Begin VB.") || str_contains(buf, "\nOption Explicit");
}

/* Disambiguate .frm files: shared by the FORM symbolic-manipulation language
 * and Visual Basic 6 forms (#721). There is no Visual Basic language yet, so a
 * VB6 form is reported as unsupported (CBM_LANG_COUNT) rather than handed to
 * the FORM grammar, which yields no defs and stray junk nodes. Defaults to
 * FORM on any doubt (preserves existing behaviour). */
static CBMLanguage lang_frm_text(const char *buf) {
    /* VB6 form files open with "VERSION x.yy" on line 1. */
    if (strncmp(buf, "VERSION ", SLEN("VERSION ")) == 0 &&
        isdigit((unsigned char)buf[SLEN("VERSION ")])) {
        return CBM_LANG_COUNT;
    }
    return has_vb6_markers(buf) ? CBM_LANG_COUNT : CBM_LANG_FORM;
}

CBMLanguage cbm_disambiguate_frm(const char *path) {
    unsigned char head[LANG_PROBE_HEAD];
    size_t n = 0;
    if (!lang_read_head(path, head, sizeof(head), &n)) {
        return CBM_LANG_FORM;
    }
    char buf[LANG_PROBE_HEAD + SKIP_ONE];
    lang_head_text(buf, LANG_PROBE_HEAD, head, n);
    return lang_frm_text(buf);
}

/* Disambiguate .res files (#2176): ReScript source shares the extension with
 * two binary formats -- Godot resources (RSRC/RSCC magic, the default save
 * format for imported meshes) and Windows compiled resource files. Binary
 * content has no ReScript meaning, and a NUL byte never occurs in ReScript
 * text while both binary formats carry NULs in their first bytes. */
static CBMLanguage lang_res_bytes(const unsigned char *head, size_t n) {
    size_t len = n < LANG_PROBE_HEAD ? n : LANG_PROBE_HEAD;
    return (len > 0 && memchr(head, '\0', len)) ? CBM_LANG_COUNT : CBM_LANG_RESCRIPT;
}

CBMLanguage cbm_disambiguate_res(const char *path) {
    unsigned char head[LANG_PROBE_HEAD];
    size_t n = 0;
    if (!lang_read_head(path, head, sizeof(head), &n)) {
        return CBM_LANG_RESCRIPT;
    }
    return lang_res_bytes(head, n);
}

/* Disambiguate .cls files: shared by InterSystems ObjectScript UDL, Salesforce
 * Apex and Visual Basic 6 class modules (#721). ObjectScript class files begin
 * with a line of the form "Class <UppercasePackage>..."; VB6 class modules
 * carry the VB6 header markers and are reported as unsupported (CBM_LANG_COUNT)
 * until a Visual Basic grammar exists. Defaults to Apex on any doubt. */
static CBMLanguage lang_cls_text(const char *buf) {
    if (has_vb6_markers(buf)) {
        return CBM_LANG_COUNT;
    }

    const char *line = buf;
    while (*line) {
        if (strncmp(line, "Class ", SLEN("Class ")) == 0 &&
            isupper((unsigned char)line[SLEN("Class ")])) {
            return CBM_LANG_OBJECTSCRIPT_UDL;
        }
        const char *nl = strchr(line, '\n');
        if (!nl) {
            break;
        }
        line = nl + SKIP_ONE;
    }
    return CBM_LANG_APEX;
}

CBMLanguage cbm_disambiguate_cls(const char *path) {
    unsigned char head[LANG_PROBE_HEAD];
    size_t n = 0;
    if (!lang_read_head(path, head, sizeof(head), &n)) {
        return CBM_LANG_APEX;
    }
    char buf[LANG_PROBE_HEAD + SKIP_ONE];
    lang_head_text(buf, LANG_PROBE_HEAD, head, n);
    return lang_cls_text(buf);
}

/* Disambiguate .inc files: shared by BitBake include fragments and
 * InterSystems ObjectScript include (macro) files. ObjectScript .inc files are
 * predominantly macro definitions ("#define NAME ..." / "#def1arg NAME ...");
 * some also carry a "ROUTINE <Name>" header. The macro-preprocessor directives
 * are the strongest signal because that is the primary content of an .inc file,
 * whereas BitBake uses '#' only for "# comment" lines (always '#' + space).
 * We therefore match ObjectScript preprocessor directives ('#' immediately
 * followed by 'def'/';'), which BitBake never produces. Defaults to BitBake on
 * any doubt (preserves existing behaviour). */
static CBMLanguage lang_inc_text(const char *buf) {
    const char *line = buf;
    while (*line) {
        /* ObjectScript include header: a line beginning "ROUTINE <Uppercase>". */
        if (strncmp(line, "ROUTINE ", SLEN("ROUTINE ")) == 0 &&
            isupper((unsigned char)line[SLEN("ROUTINE ")])) {
            return CBM_LANG_OBJECTSCRIPT_ROUTINE;
        }
        /* ObjectScript macro directives — the primary content of .inc files.
         * "#define"/"#def1arg" (macro defs) and "#;" (line comment). BitBake's
         * only '#' use is "# comment" (hash + space), so these never collide. */
        if (strncmp(line, "#define", SLEN("#define")) == 0 ||
            strncmp(line, "#def1arg", SLEN("#def1arg")) == 0 ||
            strncmp(line, "#;", SLEN("#;")) == 0) {
            return CBM_LANG_OBJECTSCRIPT_ROUTINE;
        }
        const char *nl = strchr(line, '\n');
        if (!nl) {
            break;
        }
        line = nl + SKIP_ONE;
    }
    return CBM_LANG_BITBAKE;
}

CBMLanguage cbm_disambiguate_inc(const char *path) {
    unsigned char head[LANG_PROBE_HEAD];
    size_t n = 0;
    if (!lang_read_head(path, head, sizeof(head), &n)) {
        return CBM_LANG_BITBAKE;
    }
    char buf[LANG_PROBE_HEAD + SKIP_ONE];
    lang_head_text(buf, LANG_PROBE_HEAD, head, n);
    return lang_inc_text(buf);
}

/* Case-insensitive prefix match (portable — no strncasecmp dependency). */
static bool starts_with_ci(const char *s, const char *prefix) {
    for (; *prefix; s++, prefix++) {
        if (tolower((unsigned char)*s) != tolower((unsigned char)*prefix)) {
            return false;
        }
    }
    return true;
}

/* Disambiguate .cfc files: a ColdFusion component may be written in the script
 * dialect ("component { ... }", parsed by the JS-like cfscript grammar) or the
 * tag dialect ("<cfcomponent> ... <cffunction>", parsed by the HTML-derived cfml
 * grammar). The extension table defaults to cfscript because that is what modern
 * Lucee/ACF templates use, but large legacy codebases are predominantly tag-based
 * and feeding those to the wrong grammar fails wholesale. Routing rules:
 *   1. A "<cfcomponent" or top-level "<cffunction" tag ⇒ tag dialect. (The latter
 *      catches "bare" tag components that omit the <cfcomponent> wrapper.) This
 *      wins regardless of any leading <!---/<cfscript>, so it is checked first.
 *   2. Otherwise the file is script-dialect content. Find the first significant
 *      token, skipping whitespace and <!--- ---> comments:
 *        - a leading "<cfscript>" wrapper is still script content ⇒ cfscript;
 *        - a different leading tag (e.g. <cfquery> in a bare-tag file) ⇒ cfml;
 *        - anything else ("component { ... }") ⇒ cfscript.
 * Defaults to CBM_LANG_CFSCRIPT on any doubt (preserves table behaviour). */
static CBMLanguage lang_cfc_text(const char *buf) {
    /* Rule 1: explicit tag-component markers ⇒ tag dialect. */
    if (cbm_strcasestr(buf, "<cfcomponent") != NULL || cbm_strcasestr(buf, "<cffunction") != NULL) {
        return CBM_LANG_CFML;
    }

    /* Rule 2: locate the first significant token, past whitespace and comments. */
    const char *p = buf;
    for (;;) {
        while (*p && isspace((unsigned char)*p)) {
            p++;
        }
        if (starts_with_ci(p, "<!---")) {
            const char *end = strstr(p + SLEN("<!---"), "--->");
            if (!end) {
                break; /* comment runs past the buffer — treat as no token */
            }
            p = end + SLEN("--->");
            continue;
        }
        break;
    }
    if (*p == '<') {
        /* A leading <cfscript> wrapper is script content; any other leading tag
         * (bare-tag file) is tag content. */
        return starts_with_ci(p, "<cfscript") ? CBM_LANG_CFSCRIPT : CBM_LANG_CFML;
    }
    return CBM_LANG_CFSCRIPT;
}

CBMLanguage cbm_disambiguate_cfc(const char *path) {
    unsigned char head[LANG_PROBE_CFC];
    size_t n = 0;
    if (!lang_read_head(path, head, sizeof(head), &n)) {
        return CBM_LANG_CFSCRIPT;
    }
    char buf[LANG_PROBE_CFC + SKIP_ONE];
    lang_head_text(buf, LANG_PROBE_CFC, head, n);
    return lang_cfc_text(buf);
}

/* ── One classifier for a name plus its first bytes ──────────────── */

/* JSON files that are configuration, not source worth indexing. */
static const char *const IGNORED_JSON_FILES[] = {
    "package.json",       "package-lock.json", "tsconfig.json",
    "jsconfig.json",      "composer.json",     "composer.lock",
    "yarn.lock",          "openapi.json",      "swagger.json",
    "jest.config.json",   ".eslintrc.json",    ".prettierrc.json",
    ".babelrc.json",      "tslint.json",       "angular.json",
    "firebase.json",      "renovate.json",     "lerna.json",
    "turbo.json",         ".stylelintrc.json", "pnpm-lock.json",
    "deno.json",          "biome.json",        "devcontainer.json",
    ".devcontainer.json", "launch.json",       "settings.json",
    "extensions.json",    "tasks.json",        NULL};

static bool lang_name_in(const char *name, const char *const *list) {
    for (int i = 0; list[i]; i++) {
        if (strcmp(name, list[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* The content rule a name needs, or NULL. */
static const char *lang_probe_extension(const char *filename) {
    static const char *const PROBED[] = {".m", ".cls", ".inc", ".cfc", ".frm", ".res", NULL};
    const char *dot = strrchr(filename, '.');
    return (dot && lang_name_in(dot, PROBED)) ? dot : NULL;
}

/* "<Export generator=" in the first line-probe bytes: an ObjectScript Studio
 * export, which is XML by name. */
static bool lang_objectscript_export(const unsigned char *head, size_t n) {
    char buf[LANG_PROBE_LINE + SKIP_ONE];
    lang_head_text(buf, LANG_PROBE_LINE, head, n);
    return strstr(buf, "<Export generator=") != NULL;
}

_Static_assert(LANG_PROBE_CFC == CBM_LANGUAGE_PROBE_MAX, "the longest probe is the public bound");

size_t cbm_language_probe_bytes_with(const cbm_userconfig_t *config, const char *filename) {
    if (!filename) {
        return 0;
    }
    CBMLanguage lang = lang_for_filename(config, filename);
    if (lang == CBM_LANG_COUNT) {
        return LANG_PROBE_LINE; /* the shebang fallback */
    }
    const char *ext = lang_probe_extension(filename);
    if (ext) {
        return strcmp(ext, ".cfc") == 0 ? LANG_PROBE_CFC : LANG_PROBE_HEAD;
    }
    return lang == CBM_LANG_XML ? LANG_PROBE_LINE : 0;
}

CBMLanguage cbm_language_classify_with(const cbm_userconfig_t *config, const char *filename,
                                       const unsigned char *head, size_t head_len, bool more,
                                       bool readable) {
    if (!filename) {
        return CBM_LANG_COUNT;
    }
    if (!readable || !head) {
        head = NULL;
        head_len = 0;
    }
    CBMLanguage lang = lang_for_filename(config, filename);
    if (lang == CBM_LANG_COUNT) {
        /* The name says nothing: a conservative shebang probe, so extensionless
         * scripts get indexed (#1199). The name stays authoritative otherwise. */
        return readable ? lang_shebang(head, head_len, more) : CBM_LANG_COUNT;
    }
    const char *ext = lang_probe_extension(filename);
    if (ext) {
        char buf[LANG_PROBE_CFC + SKIP_ONE];
        bool cfc = strcmp(ext, ".cfc") == 0;
        lang_head_text(buf, cfc ? LANG_PROBE_CFC : LANG_PROBE_HEAD, head, head_len);
        if (strcmp(ext, ".m") == 0) {
            /* Objective-C, Magma or MATLAB */
            lang = readable ? lang_m_text(buf) : CBM_LANG_MATLAB;
        } else if (strcmp(ext, ".cls") == 0) {
            /* ObjectScript UDL, Apex or a Visual Basic 6 class module */
            lang = readable ? lang_cls_text(buf) : CBM_LANG_APEX;
        } else if (strcmp(ext, ".inc") == 0) {
            /* BitBake or an ObjectScript include */
            lang = readable ? lang_inc_text(buf) : CBM_LANG_BITBAKE;
        } else if (cfc) {
            /* script or tag dialect */
            lang = readable ? lang_cfc_text(buf) : CBM_LANG_CFSCRIPT;
        } else if (strcmp(ext, ".frm") == 0) {
            /* FORM or a Visual Basic 6 form (#721) */
            lang = readable ? lang_frm_text(buf) : CBM_LANG_FORM;
        } else {
            /* .res: ReScript, or a binary Godot / Windows resource (#2176) */
            lang = readable ? lang_res_bytes(head, head_len) : CBM_LANG_RESCRIPT;
        }
    }
    if (lang == CBM_LANG_XML && readable && lang_objectscript_export(head, head_len)) {
        return CBM_LANG_OBJECTSCRIPT_EXPORT;
    }
    if (lang == CBM_LANG_JSON && lang_name_in(filename, IGNORED_JSON_FILES)) {
        return CBM_LANG_COUNT;
    }
    return lang;
}

size_t cbm_language_probe_bytes(const char *filename) {
    return cbm_language_probe_bytes_with(cbm_get_user_lang_config(), filename);
}

CBMLanguage cbm_language_classify(const char *filename, const unsigned char *head, size_t head_len,
                                  bool more, bool readable) {
    return cbm_language_classify_with(cbm_get_user_lang_config(), filename, head, head_len, more,
                                      readable);
}

CBMLanguage cbm_language_for_file(const char *filename, const char *path) {
    size_t probe = cbm_language_probe_bytes(filename);
    if (probe == 0) {
        return cbm_language_classify(filename, NULL, 0, false, true);
    }
    /* One byte past the probe tells a file that ends there from a longer one. */
    unsigned char head[LANG_PROBE_CFC + SKIP_ONE];
    size_t n = 0;
    FILE *f = path ? cbm_fopen(path, "rb") : NULL;
    if (!f) {
        return cbm_language_classify(filename, NULL, 0, false, false);
    }
    n = fread(head, SKIP_ONE, probe + SKIP_ONE, f);
    bool failed = ferror(f) != 0;
    (void)fclose(f);
    bool more = n > probe;
    return cbm_language_classify(filename, head, more ? probe : n, more, !failed);
}
