#include "helpers.h"
#include "arena.h" // CBMArena, cbm_arena_alloc/strdup/strndup/sprintf
#include "cbm.h"   // CBMExtractCtx, CBMLanguage, CBM_LANG_*, EFCEntry, EFC_SIZE
#include "lang_specs.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include <stdint.h>          // uint32_t
#include <string.h>
#include <ctype.h>
#include <stdio.h>

// --- Node text extraction ---

char *cbm_node_text(CBMArena *a, TSNode node, const char *source) {
    uint32_t start = ts_node_start_byte(node);
    uint32_t end = ts_node_end_byte(node);
    if (end <= start) {
        return cbm_arena_strdup(a, "");
    }
    return cbm_arena_strndup(a, source + start, end - start);
}

// --- Keyword sets per language ---

static const char *go_keywords[] = {
    "break",       "case",    "chan",   "const",   "continue", "default", "defer",  "else",
    "fallthrough", "for",     "func",   "go",      "goto",     "if",      "import", "interface",
    "map",         "package", "range",  "return",  "select",   "struct",  "switch", "type",
    "var",         "true",    "false",  "nil",     "iota",     "append",  "cap",    "close",
    "complex",     "copy",    "delete", "imag",    "len",      "make",    "new",    "panic",
    "print",       "println", "real",   "recover", NULL};

static const char *python_keywords[] = {
    "False",   "None",     "True",     "and",    "as",        "assert",   "async",    "await",
    "break",   "class",    "continue", "def",    "del",       "elif",     "else",     "except",
    "finally", "for",      "from",     "global", "if",        "import",   "in",       "is",
    "lambda",  "nonlocal", "not",      "or",     "pass",      "raise",    "return",   "try",
    "while",   "with",     "yield",    "self",   "cls",       "__init__", "__name__", "__main__",
    "super",   "print",    "len",      "range",  "enumerate", "zip",      "map",      "filter",
    "type",    "int",      "str",      "float",  "bool",      "list",     "dict",     "set",
    "tuple",   "bytes",    NULL};

static const char *js_keywords[] = {
    "break",       "case",         "catch",         "class",    "const",       "continue",
    "debugger",    "default",      "delete",        "do",       "else",        "export",
    "extends",     "false",        "finally",       "for",      "function",    "if",
    "import",      "in",           "instanceof",    "let",      "new",         "null",
    "return",      "super",        "switch",        "this",     "throw",       "true",
    "try",         "typeof",       "undefined",     "var",      "void",        "while",
    "with",        "yield",        "async",         "await",    "of",          "static",
    "get",         "set",          "from",          "as",       "constructor", "prototype",
    "console",     "window",       "document",      "process",  "module",      "exports",
    "require",     "Array",        "Object",        "String",   "Number",      "Boolean",
    "Symbol",      "Map",          "Set",           "Promise",  "Error",       "RegExp",
    "Date",        "Math",         "JSON",          "parseInt", "parseFloat",  "setTimeout",
    "setInterval", "clearTimeout", "clearInterval", NULL};

static const char *rust_keywords[] = {
    "as",        "async",        "await",    "break",         "const",  "continue",
    "crate",     "dyn",          "else",     "enum",          "extern", "false",
    "fn",        "for",          "if",       "impl",          "in",     "let",
    "loop",      "match",        "mod",      "move",          "mut",    "pub",
    "ref",       "return",       "self",     "Self",          "static", "struct",
    "super",     "trait",        "true",     "type",          "unsafe", "use",
    "where",     "while",        "abstract", "become",        "box",    "do",
    "final",     "macro",        "override", "priv",          "try",    "typeof",
    "unsized",   "virtual",      "yield",    "Some",          "None",   "Ok",
    "Err",       "Vec",          "String",   "Box",           "Rc",     "Arc",
    "Option",    "Result",       "println",  "eprintln",      "format", "write",
    "writeln",   "print",        "eprint",   "panic",         "assert", "assert_eq",
    "assert_ne", "debug_assert", "todo",     "unimplemented", "cfg",    "derive",
    "test",      "allow",        "deny",     "warn",          "forbid", "deprecated",
    NULL};

static const char *java_keywords[] = {
    "abstract",  "assert",       "boolean",     "break",      "byte",    "case",       "catch",
    "char",      "class",        "const",       "continue",   "default", "do",         "double",
    "else",      "enum",         "extends",     "false",      "final",   "finally",    "float",
    "for",       "goto",         "if",          "implements", "import",  "instanceof", "int",
    "interface", "long",         "native",      "new",        "null",    "package",    "private",
    "protected", "public",       "return",      "short",      "static",  "strictfp",   "super",
    "switch",    "synchronized", "this",        "throw",      "throws",  "transient",  "true",
    "try",       "void",         "volatile",    "while",      "var",     "record",     "sealed",
    "permits",   "yield",        "System",      "String",     "Integer", "Long",       "Double",
    "Float",     "Boolean",      "Object",      "List",       "Map",     "Set",        "Optional",
    "Stream",    "Arrays",       "Collections", NULL};

static const char *generic_keywords[] = {
    "true",     "false",     "null",      "nil",    "None",   "undefined", "void",    "if",
    "else",     "for",       "while",     "do",     "switch", "case",      "default", "break",
    "continue", "return",    "throw",     "try",    "catch",  "finally",   "class",   "struct",
    "enum",     "interface", "trait",     "impl",   "import", "export",    "package", "module",
    "use",      "require",   "include",   "new",    "delete", "this",      "self",    "super",
    "public",   "private",   "protected", "static", "const",  "var",       "let",     "function",
    "def",      "fn",        "func",      "fun",    "proc",   "sub",       "method",  "async",
    "await",    "yield",     NULL};

bool cbm_is_keyword(const char *name, CBMLanguage lang) {
    if (!name || !name[0]) {
        return true;
    }

    const char **keywords;
    switch (lang) {
    case CBM_LANG_GO:
        keywords = go_keywords;
        break;
    case CBM_LANG_PYTHON:
        keywords = python_keywords;
        break;
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
        keywords = js_keywords;
        break;
    case CBM_LANG_RUST:
        keywords = rust_keywords;
        break;
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_SCALA:
        keywords = java_keywords;
        break;
    default:
        keywords = generic_keywords;
        break;
    }

    for (const char **kw = keywords; *kw; kw++) {
        if (strcmp(name, *kw) == 0) {
            return true;
        }
    }
    return false;
}

// --- Export detection ---

bool cbm_is_exported(const char *name, CBMLanguage lang) {
    if (!name || !name[0]) {
        return false;
    }
    switch (lang) {
    case CBM_LANG_GO:
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return (name[0] >= 'A' && name[0] <= 'Z');
    case CBM_LANG_PYTHON:
        return (name[0] != '_');
    case CBM_LANG_JAVA:
    case CBM_LANG_CSHARP:
    case CBM_LANG_KOTLIN:
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return (name[0] >= 'A' && name[0] <= 'Z');
    default:
        return true;
    }
}

// --- Test file detection ---

static bool has_suffix(const char *str, const char *suffix) {
    size_t slen = strlen(str);
    size_t xlen = strlen(suffix);
    if (xlen > slen) {
        return false;
    }
    return strcmp(str + slen - xlen, suffix) == 0;
}

static bool has_prefix(const char *str, const char *prefix) {
    return strncmp(str, prefix, strlen(prefix)) == 0;
}

// Extract basename from path
static const char *path_basename(const char *path) {
    const char *last = strrchr(path, '/');
    return last ? last + 1 : path;
}

// Strip extension from basename
static void strip_ext(const char *base, char *buf, size_t buflen) {
    const char *dot = strrchr(base, '.');
    if (dot && dot != base) {
        size_t len = (size_t)(dot - base);
        if (len >= buflen) {
            len = buflen - 1;
        }
        memcpy(buf, base, len);
        buf[len] = '\0';
    } else {
        snprintf(buf, buflen, "%s", base);
    }
}

bool cbm_is_test_file(const char *rel_path, CBMLanguage lang) {
    if (!rel_path) {
        return false;
    }
    const char *base = path_basename(rel_path);

    switch (lang) {
    case CBM_LANG_GO:
        return has_suffix(base, "_test.go");
    case CBM_LANG_PYTHON:
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return has_prefix(base, "test_") || has_suffix(base, "_test.py");
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX: {
        char noext[256];
        strip_ext(base, noext, sizeof(noext));
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return has_suffix(noext, ".test") || has_suffix(noext, ".spec") ||
               has_suffix(noext, "_test") || has_suffix(noext, "_spec") ||
               has_prefix(base, "test_");
    }
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_SCALA:
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return has_suffix(base, "Test.java") || has_suffix(base, "Tests.java") ||
               has_suffix(base, "Spec.java") || has_suffix(base, "Test.kt") ||
               has_suffix(base, "Spec.kt") || has_suffix(base, "Test.scala") ||
               has_suffix(base, "Spec.scala");
    case CBM_LANG_RUST:
        // Rust tests are typically mod tests inside the file, but test files too
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return has_suffix(base, "_test.rs") || has_prefix(base, "test_");
    case CBM_LANG_RUBY:
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return has_suffix(base, "_test.rb") || has_suffix(base, "_spec.rb") ||
               has_prefix(base, "test_");
    case CBM_LANG_PHP:
        return has_suffix(base, "Test.php");
    case CBM_LANG_CSHARP:
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return has_suffix(base, "Tests.cs") || has_suffix(base, "Test.cs");
    case CBM_LANG_CPP:
    case CBM_LANG_C:
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return has_suffix(base, "_test.c") || has_suffix(base, "_test.cc") ||
               has_suffix(base, "_test.cpp") || has_prefix(base, "test_");
    case CBM_LANG_MATLAB:
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return has_prefix(base, "test_") || has_prefix(base, "Test");
    default:
        return false;
    }
}

// --- AST traversal helpers ---

TSNode cbm_find_child_by_kind(TSNode parent, const char *kind) {
    uint32_t count = ts_node_child_count(parent);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(parent, i);
        if (strcmp(ts_node_type(child), kind) == 0) {
            return child;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

bool cbm_kind_in_set(TSNode node, const char **types) {
    if (!types) {
        return false;
    }
    const char *kind = ts_node_type(node);
    for (const char **t = types; *t; t++) {
        if (strcmp(kind, *t) == 0) {
            return true;
        }
    }
    return false;
}

bool cbm_has_ancestor_kind(TSNode node, const char *kind, int max_depth) {
    TSNode cur = node;
    for (int i = 0; i < max_depth; i++) {
        TSNode parent = ts_node_parent(cur);
        if (ts_node_is_null(parent)) {
            return false;
        }
        if (strcmp(ts_node_type(parent), kind) == 0) {
            return true;
        }
        cur = parent;
    }
    return false;
}

// Recursive branching count
// NOLINTNEXTLINE(misc-no-recursion) — intentional AST tree walk
static int count_branching_rec(TSNode node, const char **types) {
    int count = 0;
    const char *kind = ts_node_type(node);
    for (const char **t = types; *t; t++) {
        if (strcmp(kind, *t) == 0) {
            count++;
            break;
        }
    }
    uint32_t n = ts_node_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        count += count_branching_rec(ts_node_child(node, i), types);
    }
    return count;
}

int cbm_count_branching(TSNode node, const char **branching_types) {
    if (!branching_types) {
        return 0;
    }
    return count_branching_rec(node, branching_types);
}

// --- Enclosing function detection ---

// Language-specific function node types for parent-chain walk
static const char *func_kinds_go[] = {"function_declaration", "method_declaration", NULL};
static const char *func_kinds_python[] = {"function_definition", NULL};
static const char *func_kinds_js[] = {"function_declaration", "method_definition", "arrow_function",
                                      "function_expression", NULL};
static const char *func_kinds_rust[] = {"function_item", NULL};
static const char *func_kinds_java[] = {"method_declaration", "constructor_declaration", NULL};
static const char *func_kinds_cpp[] = {"function_definition", NULL};
static const char *func_kinds_ruby[] = {"method", "singleton_method", NULL};
static const char *func_kinds_php[] = {"function_definition", "method_declaration", NULL};
static const char *func_kinds_lua[] = {"function_declaration", "function_definition", NULL};
static const char *func_kinds_scala[] = {"function_definition", NULL};
static const char *func_kinds_kotlin[] = {"function_declaration", NULL};
static const char *func_kinds_elixir[] = {"call", NULL}; // def/defp are call nodes
static const char *func_kinds_haskell[] = {"function", "value_definition", NULL};
static const char *func_kinds_ocaml[] = {"value_definition", "let_binding", NULL};
static const char *func_kinds_zig[] = {"function_declaration", "test_declaration", NULL};
static const char *func_kinds_bash[] = {"function_definition", NULL};
static const char *func_kinds_erlang[] = {"function_clause", NULL};
static const char *func_kinds_csharp[] = {"method_declaration", "constructor_declaration", NULL};
static const char *func_kinds_matlab[] = {"function_definition", NULL};
static const char *func_kinds_lean[] = {"def", "theorem", "instance", "abbrev", NULL};
static const char *func_kinds_form[] = {"procedure_definition", NULL};
static const char *func_kinds_magma[] = {"function_definition", "procedure_definition",
                                         "intrinsic_definition", NULL};
static const char *func_kinds_wolfram[] = {"set_delayed_top", "set_top", "set_delayed", "set",
                                           NULL};
static const char *func_kinds_generic[] = {"function_declaration", "function_definition",
                                           "method_declaration", "method_definition", NULL};

static const char **func_kinds_for_lang(CBMLanguage lang) {
    switch (lang) {
    case CBM_LANG_GO:
        return func_kinds_go;
    case CBM_LANG_PYTHON:
        return func_kinds_python;
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
        return func_kinds_js;
    case CBM_LANG_RUST:
        return func_kinds_rust;
    case CBM_LANG_JAVA:
        return func_kinds_java;
    case CBM_LANG_CPP:
    case CBM_LANG_C:
        return func_kinds_cpp;
    case CBM_LANG_RUBY:
        return func_kinds_ruby;
    case CBM_LANG_PHP:
        return func_kinds_php;
    case CBM_LANG_LUA:
        return func_kinds_lua;
    case CBM_LANG_SCALA:
        return func_kinds_scala;
    case CBM_LANG_KOTLIN:
        return func_kinds_kotlin;
    case CBM_LANG_ELIXIR:
        return func_kinds_elixir;
    case CBM_LANG_HASKELL:
        return func_kinds_haskell;
    case CBM_LANG_OCAML:
        return func_kinds_ocaml;
    case CBM_LANG_ZIG:
        return func_kinds_zig;
    case CBM_LANG_BASH:
        return func_kinds_bash;
    case CBM_LANG_ERLANG:
        return func_kinds_erlang;
    case CBM_LANG_CSHARP:
        return func_kinds_csharp;
    case CBM_LANG_MATLAB:
        return func_kinds_matlab;
    case CBM_LANG_LEAN:
        return func_kinds_lean;
    case CBM_LANG_FORM:
        return func_kinds_form;
    case CBM_LANG_MAGMA:
        return func_kinds_magma;
    case CBM_LANG_WOLFRAM:
        return func_kinds_wolfram;
    default:
        return func_kinds_generic;
    }
}

TSNode cbm_find_enclosing_func(TSNode node, CBMLanguage lang) {
    const char **kinds = func_kinds_for_lang(lang);
    TSNode cur = node;
    for (;;) {
        TSNode parent = ts_node_parent(cur);
        if (ts_node_is_null(parent)) {
            break;
        }
        const char *pk = ts_node_type(parent);
        // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
        for (const char **k = kinds; *k; k++) {
            if (strcmp(pk, *k) == 0) {
                return parent;
            }
        }
        cur = parent;
    }
    TSNode null_node = {0};
    return null_node;
}

// Get the name of a function node (basic: try "name" field)
static const char *func_node_name(CBMArena *a, TSNode func_node, const char *source,
                                  CBMLanguage lang) {
    // Wolfram: set_delayed_top/set_top/set_delayed/set — LHS is apply(user_symbol("f"), ...)
    if (lang == CBM_LANG_WOLFRAM) {
        const char *nk = ts_node_type(func_node);
        if (strcmp(nk, "set_delayed_top") == 0 || strcmp(nk, "set_top") == 0 ||
            strcmp(nk, "set_delayed") == 0 || strcmp(nk, "set") == 0) {
            if (ts_node_named_child_count(func_node) > 0) {
                TSNode lhs = ts_node_named_child(func_node, 0);
                if (strcmp(ts_node_type(lhs), "apply") == 0 && ts_node_named_child_count(lhs) > 0) {
                    TSNode head = ts_node_named_child(lhs, 0);
                    if (strcmp(ts_node_type(head), "user_symbol") == 0) {
                        return cbm_node_text(a, head, source);
                    }
                }
            }
            return NULL;
        }
    }

    /* C/C++/CUDA/GLSL: function_definition has no "name" field.
     * Name is inside declarator chain: function_definition → declarator →
     * function_declarator → declarator → identifier. */
    if ((lang == CBM_LANG_C || lang == CBM_LANG_CPP ||
         lang == CBM_LANG_CUDA || lang == CBM_LANG_GLSL) &&
        strcmp(ts_node_type(func_node), "function_definition") == 0) {
        TSNode decl = ts_node_child_by_field_name(func_node, "declarator", 10);
        for (int depth = 0; depth < 8 && !ts_node_is_null(decl); depth++) {
            const char *dk = ts_node_type(decl);
            if (strcmp(dk, "identifier") == 0 || strcmp(dk, "field_identifier") == 0) {
                return cbm_node_text(a, decl, source);
            }
            if (strcmp(dk, "qualified_identifier") == 0 ||
                strcmp(dk, "scoped_identifier") == 0) {
                TSNode id = cbm_find_child_by_kind(decl, "identifier");
                if (ts_node_is_null(id))
                    id = cbm_find_child_by_kind(decl, "field_identifier");
                if (!ts_node_is_null(id)) return cbm_node_text(a, id, source);
                return NULL;
            }
            TSNode inner = ts_node_child_by_field_name(decl, "declarator", 10);
            if (ts_node_is_null(inner) && ts_node_named_child_count(decl) > 0)
                inner = ts_node_named_child(decl, 0);
            decl = inner;
        }
        return NULL;
    }

    TSNode name_node = ts_node_child_by_field_name(func_node, "name", 4);
    if (!ts_node_is_null(name_node)) {
        return cbm_node_text(a, name_node, source);
    }
    // Arrow functions: check parent variable_declarator
    if (strcmp(ts_node_type(func_node), "arrow_function") == 0) {
        TSNode parent = ts_node_parent(func_node);
        if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "variable_declarator") == 0) {
            TSNode vname = ts_node_child_by_field_name(parent, "name", 4);
            if (!ts_node_is_null(vname)) {
                return cbm_node_text(a, vname, source);
            }
        }
    }
    return NULL;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
const char *cbm_enclosing_func_qn(CBMArena *a, TSNode node, CBMLanguage lang, const char *source,
                                  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                                  const char *project, const char *rel_path,
                                  const char *module_qn) {
    TSNode func_node = cbm_find_enclosing_func(node, lang);
    if (ts_node_is_null(func_node)) {
        return module_qn;
    }
    const char *name = func_node_name(a, func_node, source, lang);
    if (!name || !name[0]) {
        return module_qn;
    }

    // Check if the function is inside a class — compute classQN.funcName
    const CBMLangSpec *spec = cbm_lang_spec(lang);
    if (spec && spec->class_node_types) {
        TSNode cur = ts_node_parent(func_node);
        while (!ts_node_is_null(cur)) {
            if (cbm_kind_in_set(cur, spec->class_node_types)) {
                TSNode class_name = ts_node_child_by_field_name(cur, "name", 4);
                if (!ts_node_is_null(class_name)) {
                    char *cname = cbm_node_text(a, class_name, source);
                    if (cname && cname[0]) {
                        const char *class_qn = cbm_fqn_compute(a, project, rel_path, cname);
                        return cbm_arena_sprintf(a, "%s.%s", class_qn, name);
                    }
                }
            }
            cur = ts_node_parent(cur);
        }
    }

    return cbm_fqn_compute(a, project, rel_path, name);
}

// --- Cached enclosing function QN ---

const char *cbm_enclosing_func_qn_cached(CBMExtractCtx *ctx, TSNode node) {
    uint32_t pos = ts_node_start_byte(node);

    // Check cache: find a function range that contains this position.
    // Linear scan is fine for EFC_SIZE=64 (all entries fit in ~1 cache line).
    for (int i = 0; i < ctx->ef_cache.count; i++) {
        EFCEntry *e = &ctx->ef_cache.entries[i];
        if (pos >= e->start_byte && pos < e->end_byte) {
            return e->qn;
        }
    }

    // Cache miss: compute via parent walk
    const char *qn = cbm_enclosing_func_qn(ctx->arena, node, ctx->language, ctx->source,
                                           ctx->project, ctx->rel_path, ctx->module_qn);

    // Cache the result: find the enclosing function's byte range
    TSNode func_node = cbm_find_enclosing_func(node, ctx->language);
    if (!ts_node_is_null(func_node) && ctx->ef_cache.count < EFC_SIZE) {
        EFCEntry *e = &ctx->ef_cache.entries[ctx->ef_cache.count++];
        e->start_byte = ts_node_start_byte(func_node);
        e->end_byte = ts_node_end_byte(func_node);
        e->qn = qn;
    }

    return qn;
}

// --- Module-level detection ---

// Module-level parent kind tables
static const char *module_parents_go[] = {"source_file", NULL};
static const char *module_parents_rust[] = {"source_file", "mod_item", NULL};
static const char *module_parents_java[] = {"program", "class_body", NULL};
static const char *module_parents_kotlin[] = {"source_file", "class_body", NULL};
static const char *module_parents_scala[] = {"compilation_unit", "template_body", NULL};
static const char *module_parents_csharp[] = {"compilation_unit", "class_declaration",
                                              "namespace_declaration", NULL};
static const char *module_parents_php[] = {"program", NULL};
static const char *module_parents_ruby[] = {"program", "class", "module", NULL};
static const char *module_parents_c[] = {"translation_unit", NULL};
static const char *module_parents_zig[] = {"source_file", NULL};
static const char *module_parents_bash[] = {"program", NULL};
static const char *module_parents_erlang[] = {"source", "source_file", NULL};
static const char *module_parents_haskell[] = {"declarations", NULL};
static const char *module_parents_ocaml[] = {"compilation_unit", NULL};
static const char *module_parents_elixir[] = {"source", NULL};
static const char *module_parents_html[] = {"document", NULL};
static const char *module_parents_css[] = {"stylesheet", NULL};
static const char *module_parents_sql[] = {"source_file", "program", "statement", NULL};
static const char *module_parents_toml[] = {"document", "table", "table_array_element", NULL};
static const char *module_parents_config[] = {
    "document", "table", "table_array_element", "section", "object", "element", "array", NULL};
static const char *module_parents_hcl[] = {"config_file", NULL};
static const char *module_parents_makefile[] = {"makefile", NULL};
static const char *module_parents_commonlisp[] = {"source", NULL};
static const char *module_parents_matlab[] = {"source_file", NULL};
static const char *module_parents_form[] = {"source_file", NULL};
static const char *module_parents_magma[] = {"source_file", NULL};

bool cbm_is_module_level(TSNode node, CBMLanguage lang) {
    TSNode parent = ts_node_parent(node);
    if (ts_node_is_null(parent)) {
        return false;
    }
    const char *pk = ts_node_type(parent);

    // Python: module or expression_statement -> module
    if (lang == CBM_LANG_PYTHON) {
        if (strcmp(pk, "module") == 0) {
            return true;
        }
        if (strcmp(pk, "expression_statement") == 0) {
            TSNode gp = ts_node_parent(parent);
            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            return !ts_node_is_null(gp) && strcmp(ts_node_type(gp), "module") == 0;
        }
        return false;
    }

    // JS/TS: program, or export_statement -> program
    if (lang == CBM_LANG_JAVASCRIPT || lang == CBM_LANG_TYPESCRIPT || lang == CBM_LANG_TSX) {
        if (strcmp(pk, "program") == 0) {
            return true;
        }
        if (strcmp(pk, "export_statement") == 0) {
            TSNode gp = ts_node_parent(parent);
            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            return !ts_node_is_null(gp) && strcmp(ts_node_type(gp), "program") == 0;
        }
        return false;
    }

    // Lua: chunk
    if (lang == CBM_LANG_LUA) {
        if (strcmp(pk, "chunk") == 0) {
            return true;
        }
        // assignment_statement -> chunk
        if (strcmp(pk, "assignment_statement") == 0) {
            TSNode gp = ts_node_parent(parent);
            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            return !ts_node_is_null(gp) && strcmp(ts_node_type(gp), "chunk") == 0;
        }
        return false;
    }

    // YAML: document or stream
    if (lang == CBM_LANG_YAML) {
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        return strcmp(pk, "document") == 0 || strcmp(pk, "stream") == 0 ||
               strcmp(pk, "block_mapping") == 0;
    }

    // Table lookup for the rest
    const char **parents = NULL;
    switch (lang) {
    case CBM_LANG_GO:
        parents = module_parents_go;
        break;
    case CBM_LANG_RUST:
        parents = module_parents_rust;
        break;
    case CBM_LANG_JAVA:
        parents = module_parents_java;
        break;
    case CBM_LANG_KOTLIN:
        parents = module_parents_kotlin;
        break;
    case CBM_LANG_SCALA:
        parents = module_parents_scala;
        break;
    case CBM_LANG_CSHARP:
        parents = module_parents_csharp;
        break;
    case CBM_LANG_PHP:
        parents = module_parents_php;
        break;
    case CBM_LANG_RUBY:
        parents = module_parents_ruby;
        break;
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_OBJC:
        parents = module_parents_c;
        break;
    case CBM_LANG_ZIG:
        parents = module_parents_zig;
        break;
    case CBM_LANG_BASH:
        parents = module_parents_bash;
        break;
    case CBM_LANG_ERLANG:
        parents = module_parents_erlang;
        break;
    case CBM_LANG_HASKELL:
        parents = module_parents_haskell;
        break;
    case CBM_LANG_OCAML:
        parents = module_parents_ocaml;
        break;
    case CBM_LANG_ELIXIR:
        parents = module_parents_elixir;
        break;
    case CBM_LANG_HTML:
        parents = module_parents_html;
        break;
    case CBM_LANG_CSS:
    case CBM_LANG_SCSS:
        parents = module_parents_css;
        break;
    case CBM_LANG_SQL:
        parents = module_parents_sql;
        break;
    case CBM_LANG_TOML:
        parents = module_parents_toml;
        break;
    case CBM_LANG_HCL:
        parents = module_parents_hcl;
        break;
    case CBM_LANG_JSON:
    case CBM_LANG_INI:
    case CBM_LANG_XML:
    case CBM_LANG_MARKDOWN:
        parents = module_parents_config;
        break;
    case CBM_LANG_SWIFT:
        parents = module_parents_zig;
        break; // source_file
    case CBM_LANG_DART:
        parents = module_parents_php;
        break; // program
    case CBM_LANG_PERL:
    case CBM_LANG_GROOVY:
        parents = module_parents_zig;
        break; // source_file
    case CBM_LANG_R:
        parents = module_parents_php;
        break; // program
    case CBM_LANG_MAKEFILE:
        parents = module_parents_makefile;
        break;
    case CBM_LANG_COMMONLISP:
        parents = module_parents_commonlisp;
        break;
    case CBM_LANG_MATLAB:
        parents = module_parents_matlab;
        break;
    case CBM_LANG_LEAN:
        parents = module_parents_zig;
        break; // source_file
    case CBM_LANG_FORM:
        parents = module_parents_form;
        break;
    case CBM_LANG_MAGMA:
        parents = module_parents_magma;
        break;
    default:
        return false;
    }
    if (parents) {
        // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
        for (const char **p = parents; *p; p++) {
            if (strcmp(pk, *p) == 0) {
                return true;
            }
        }
    }
    return false;
}

// --- FQN computation ---
// Mirrors Go's fqn.Compute(): project + path_parts_dotted + name

// Internal helper: find extension start in basename (returns length without ext)
static size_t strip_ext_len(const char *s, size_t len) {
    for (size_t i = len; i > 0; i--) {
        if (s[i - 1] == '.') {
            return i - 1;
        }
        if (s[i - 1] == '/') {
            break;
        }
    }
    return len;
}

char *cbm_fqn_compute(CBMArena *a, const char *project, const char *rel_path, const char *name) {
    // Build: project.path1.path2.filename.name
    // where rel_path = "path1/path2/filename.ext"
    // Strip extension, replace / with ., drop __init__ and index
    size_t proj_len = strlen(project);
    size_t path_len = strlen(rel_path);
    size_t name_len = name ? strlen(name) : 0;

    // Worst case: project + . + path (with / -> .) + . + name + NUL
    size_t max_len = proj_len + 1 + path_len + 1 + name_len + 1;
    char *buf = (char *)cbm_arena_alloc(a, max_len);
    if (!buf) {
        return NULL;
    }

    char *out = buf;
    memcpy(out, project, proj_len);
    out += proj_len;

    // Process path: strip extension, split by /, replace with dots
    // Skip trailing extension
    size_t plen = strip_ext_len(rel_path, path_len);

    // Split by '/' and append each part as .part
    const char *start = rel_path;
    const char *end_ptr = rel_path + plen;
    while (start < end_ptr) {
        const char *slash = (const char *)memchr(start, '/', end_ptr - start);
        const char *part_end = slash ? slash : end_ptr;
        size_t part_len = (size_t)(part_end - start);

        if (part_len > 0) {
            /* Handle __init__ (Python) and index (JS/TS):
             * Strip ONLY when a name suffix is provided, so that symbols
             * inside __init__.py get clean package QNs (proj.pkg.MyClass).
             * When no name is given (fqn_module for the file itself),
             * keep __init__/index to avoid QN collision with the Folder
             * node for the same directory. */
            bool is_last = (part_end == end_ptr);
            bool skip = false;
            if (is_last && name && name_len > 0) {
                if (part_len == 8 && memcmp(start, "__init__", 8) == 0) {
                    skip = true;
                }
                if (part_len == 5 && memcmp(start, "index", 5) == 0) {
                    skip = true;
                }
            }
            if (!skip) {
                *out++ = '.';
                memcpy(out, start, part_len);
                out += part_len;
            }
        }
        start = part_end + 1;
    }

    if (name && name_len > 0) {
        *out++ = '.';
        memcpy(out, name, name_len);
        out += name_len;
    }
    *out = '\0';
    return buf;
}

char *cbm_fqn_module(CBMArena *a, const char *project, const char *rel_path) {
    return cbm_fqn_compute(a, project, rel_path, NULL);
}

char *cbm_fqn_folder(CBMArena *a, const char *project, const char *rel_dir) {
    // project.dir1.dir2
    size_t proj_len = strlen(project);
    size_t dir_len = strlen(rel_dir);
    size_t max_len = proj_len + 1 + dir_len + 1;
    char *buf = (char *)cbm_arena_alloc(a, max_len);
    if (!buf) {
        return NULL;
    }

    char *out = buf;
    memcpy(out, project, proj_len);
    out += proj_len;

    if (dir_len > 0 && !(dir_len == 1 && rel_dir[0] == '.')) {
        const char *start = rel_dir;
        const char *end_ptr = rel_dir + dir_len;
        while (start < end_ptr) {
            const char *slash = (const char *)memchr(start, '/', end_ptr - start);
            const char *part_end = slash ? slash : end_ptr;
            size_t part_len = (size_t)(part_end - start);
            if (part_len > 0) {
                *out++ = '.';
                memcpy(out, start, part_len);
                out += part_len;
            }
            start = part_end + 1;
        }
    }
    *out = '\0';
    return buf;
}
