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
            p = strchr(p + 1, '.');
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

CBMLanguage cbm_disambiguate_m(const char *path) {
    if (!path) {
        return CBM_LANG_MATLAB;
    }

    FILE *f = fopen(path, "r");
    if (!f) {
        return CBM_LANG_MATLAB;
    }

    /* Read first 4KB */
    char buf[4096 + 1];
    size_t n = fread(buf, 1, 4096, f);
    buf[n] = '\0';
    (void)fclose(f);

    /* Check Objective-C markers first */
    if (str_contains(buf, "@interface") || str_contains(buf, "@implementation") ||
        str_contains(buf, "@protocol") || str_contains(buf, "@property") ||
        str_contains(buf, "#import") || str_contains(buf, "@selector") ||
        str_contains(buf, "@encode") || str_contains(buf, "@synthesize") ||
        str_contains(buf, "@dynamic")) {
        return CBM_LANG_OBJC;
    }

    /* Check Magma markers (before MATLAB — both have 'function') */
    if (str_contains(buf, "end function;") || str_contains(buf, "end procedure;") ||
        str_contains(buf, "end intrinsic;") || str_contains(buf, "end if;") ||
        str_contains(buf, "end for;") || str_contains(buf, "end while;")) {
        return CBM_LANG_MAGMA;
    }

    /* Also check Magma-specific patterns with regex-like heuristics */
    if (str_contains(buf, "intrinsic ") || str_contains(buf, "procedure ")) {
        /* Look for "intrinsic Name(" or "procedure Name(" patterns */
        const char *markers[] = {"intrinsic ", "procedure "};
        for (int i = 0; i < 2; i++) {
            const char *p = strstr(buf, markers[i]);
            if (p) {
                p += strlen(markers[i]);
                /* Skip to see if there's an identifier followed by '(' */
                while (*p && isalpha((unsigned char)*p)) {
                    p++;
                }
                if (*p == '(') {
                    return CBM_LANG_MAGMA;
                }
            }
        }
    }

    /* Check MATLAB markers */
    /* Look for: function keyword at start of line, classdef, %% section markers */
    const char *line = buf;
    while (*line) {
        /* Skip leading whitespace */
        const char *p = line;
        while (*p == ' ' || *p == '\t') {
            p++;
        }

        if (strncmp(p, "function ", 9) == 0 || strncmp(p, "function\t", 9) == 0 ||
            strncmp(p, "classdef ", 9) == 0 || strncmp(p, "classdef\t", 9) == 0 ||
            strncmp(p, "%%", 2) == 0 || (*p == '%' && *(p + 1) != '{')) {
            return CBM_LANG_MATLAB;
        }

        /* Advance to next line */
        const char *nl = strchr(line, '\n');
        if (!nl) {
            break;
        }
        line = nl + 1;
    }

    /* Default to MATLAB */
    return CBM_LANG_MATLAB;
}
