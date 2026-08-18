#include "helpers.h"
#include "arena.h" // CBMArena, cbm_arena_alloc/strdup/strndup/sprintf
#include "cbm.h"   // CBMExtractCtx, CBMLanguage, CBM_LANG_*, EFCEntry, EFC_SIZE
#include "lang_specs.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include "foundation/constants.h"
#include "foundation/compat.h" // CBM_TLS
#include <limits.h>
#include <stdlib.h> // calloc/free for the symbol-set cache

enum {
    MIN_ROUTE_LEN = 3,
    MIN_SYS_PATH_LEN = 4,
    MAX_ROUTE_SCAN = 20,
    NOEXT_BUF = 256,
    MIN_HEX_LEN = 3,
    MAX_HEX_NAME_LEN = 64,
    INIT_FILE_LEN = 8,  /* strlen("__init__") */
    INDEX_FILE_LEN = 5, /* strlen("index") */
    NOT_FOUND = -1,
};

/* Prefix length helper for strncmp with string literals. */
#define SLEN(s) (sizeof(s) - SKIP_ONE)
#include <stdint.h> // uint32_t
#include <string.h>
#include <ctype.h>
#include <stdio.h>

// --- Portable substring search ---

// Hand-rolled memmem: does not rely on the system memmem (GNU/BSD-only;
// msys2-clang on Windows lacks it), so it compiles identically everywhere.
void *cbm_memmem(const void *haystack, size_t haystack_len, const void *needle, size_t needle_len) {
    if (needle_len == 0) {
        return (void *)haystack;
    }
    if (needle_len > haystack_len) {
        return NULL;
    }
    const char *h = (const char *)haystack;
    size_t last = haystack_len - needle_len;
    for (size_t i = 0; i <= last; i++) {
        if (memcmp(h + i, needle, needle_len) == 0) {
            return (void *)(h + i);
        }
    }
    return NULL;
}

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

/* Kotlin hard keywords (those reserved everywhere). Kotlin does NOT reserve
 * primitive type names — `double`, `int`, `float`, `boolean` are ordinary
 * identifiers (the types are `Double`, `Int`, …), so a function named
 * `fun double()` is legal and must NOT be filtered as a keyword the way the
 * Java list (which lists Java primitives) would.  Soft/modifier keywords
 * (`data`, `open`, `sealed`, `suspend`, …) are context-sensitive and usable as
 * identifiers, so they are intentionally omitted. */
static const char *kotlin_keywords[] = {
    "as",     "break", "class", "continue",  "do",   "else", "false",     "for",
    "fun",    "if",    "in",    "interface", "is",   "null", "object",    "package",
    "return", "super", "this",  "throw",     "true", "try",  "typealias", "typeof",
    "val",    "var",   "when",  "while",     NULL};

static const char *generic_keywords[] = {
    "true",     "false",     "null",      "nil",    "None",   "undefined", "void",    "if",
    "else",     "for",       "while",     "do",     "switch", "case",      "default", "break",
    "continue", "return",    "throw",     "try",    "catch",  "finally",   "class",   "struct",
    "enum",     "interface", "trait",     "impl",   "import", "export",    "package", "module",
    "use",      "require",   "include",   "new",    "delete", "this",      "self",    "super",
    "public",   "private",   "protected", "static", "const",  "var",       "let",     "function",
    "def",      "fn",        "func",      "fun",    "proc",   "sub",       "method",  "async",
    "await",    "yield",     NULL};

/* Puppet reserves control-flow words but NOT `include`/`require`/`contain`,
 * which are ordinary built-in functions invoked as calls. Using the generic
 * list would wrongly drop `include`/`require` call edges, so Puppet gets its
 * own reserved-word set that omits them. */
static const char *puppet_keywords[] = {"true",   "false",  "undef",    "if",      "elsif",  "else",
                                        "unless", "case",   "and",      "or",      "in",     "node",
                                        "class",  "define", "inherits", "default", "return", NULL};

// True when `label` names a type-like container definition (see cbm.h). Single
// source of truth for the type-resolution / registry / IMPLEMENTS / LSP-type
// consumers — adding a label here updates them all.
bool cbm_label_is_type_like(const char *label) {
    if (!label) {
        return false;
    }
    return strcmp(label, "Class") == 0 || strcmp(label, "Struct") == 0 ||
           strcmp(label, "Interface") == 0 || strcmp(label, "Enum") == 0 ||
           strcmp(label, "Type") == 0 || strcmp(label, "Trait") == 0;
}

bool cbm_label_uses_source_span_selection(const char *label) {
    return label && (strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0 ||
                     cbm_label_is_type_like(label) || strcmp(label, "Module") == 0);
}

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
    case CBM_LANG_SCALA:
        keywords = java_keywords;
        break;
    case CBM_LANG_KOTLIN:
        keywords = kotlin_keywords;
        break;
    case CBM_LANG_PUPPET:
        keywords = puppet_keywords;
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

/* Keyword-like calls that should still be emitted as call records. Keep Python
 * entries in sync with lsp/py_builtins.c builtins.<name> nodes. */
static const char *const python_resolvable_builtins[] = {"len",  "print", "str",   "int",
                                                         "list", "dict",  "range", NULL};
static const char *const puppet_resolvable_builtins[] = {"include", NULL};

bool cbm_is_resolvable_builtin(const char *name, CBMLanguage lang) {
    if (!name || !name[0]) {
        return false;
    }
    const char *const *builtins = NULL;
    if (lang == CBM_LANG_PYTHON) {
        builtins = python_resolvable_builtins;
    } else if (lang == CBM_LANG_PUPPET) {
        builtins = puppet_resolvable_builtins;
    } else {
        return false;
    }
    for (const char *const *b = builtins; *b; b++) {
        if (strcmp(name, *b) == 0) {
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
        return (name[0] >= 'A' && name[0] <= 'Z');
    case CBM_LANG_PYTHON:
        return (name[0] != '_');
    case CBM_LANG_JAVA:
    case CBM_LANG_CSHARP:
    case CBM_LANG_KOTLIN:
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
    return last ? last + SKIP_ONE : path;
}

// Strip extension from basename
static void strip_ext(const char *base, char *buf, size_t buflen) {
    const char *dot = strrchr(base, '.');
    if (dot && dot != base) {
        size_t len = (size_t)(dot - base);
        if (len >= buflen) {
            len = buflen - SKIP_ONE;
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

    /* Directory-based, language-agnostic: a file under a conventional test
     * directory is a test file regardless of its own basename (e.g.
     * tests/helpers/fixtures.c, which no per-language suffix/prefix rule
     * below would otherwise catch). Mirrors the directory set cbm_is_test_path
     * (src/pipeline/pass_tests.c) already uses for TESTS-edge detection, so
     * the extraction-time is_test_file flag agrees with it (#1294). */
    if (strstr(rel_path, "__tests__/") || strstr(rel_path, "/tests/") ||
        strstr(rel_path, "/test/") || strstr(rel_path, "/spec/") ||
        has_prefix(rel_path, "tests/") || has_prefix(rel_path, "test/") ||
        has_prefix(rel_path, "spec/") || has_prefix(rel_path, "__tests__/")) {
        return true;
    }

    switch (lang) {
    case CBM_LANG_GO:
        return has_suffix(base, "_test.go");
    case CBM_LANG_PYTHON:
        return has_prefix(base, "test_") || has_suffix(base, "_test.py");
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX: {
        char noext[NOEXT_BUF];
        strip_ext(base, noext, sizeof(noext));
        return has_suffix(noext, ".test") || has_suffix(noext, ".spec") ||
               has_suffix(noext, "_test") || has_suffix(noext, "_spec") ||
               has_prefix(base, "test_");
    }
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_SCALA:
        return has_suffix(base, "Test.java") || has_suffix(base, "Tests.java") ||
               has_suffix(base, "Spec.java") || has_suffix(base, "Test.kt") ||
               has_suffix(base, "Spec.kt") || has_suffix(base, "Test.scala") ||
               has_suffix(base, "Spec.scala");
    case CBM_LANG_RUST:
        // Rust tests are typically mod tests inside the file, but test files too
        return has_suffix(base, "_test.rs") || has_prefix(base, "test_");
    case CBM_LANG_RUBY:
        return has_suffix(base, "_test.rb") || has_suffix(base, "_spec.rb") ||
               has_prefix(base, "test_");
    case CBM_LANG_PHP:
        return has_suffix(base, "Test.php");
    case CBM_LANG_CSHARP:
        return has_suffix(base, "Tests.cs") || has_suffix(base, "Test.cs");
    case CBM_LANG_CPP:
    case CBM_LANG_C:
        return has_suffix(base, "_test.c") || has_suffix(base, "_test.cc") ||
               has_suffix(base, "_test.cpp") || has_prefix(base, "test_");
    case CBM_LANG_MATLAB:
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

/* ── Node-type classification: TSSymbol bitset acceleration ───────────────
 * cbm_kind_in_set is called for nearly every AST node (function/class/call/
 * import/branching sets), so a linear strcmp over the type-name array is a hot
 * path. tree-sitter already assigns each node type a small integer TSSymbol, so
 * we precompute — per (language, type-array) — a bitset of the matching symbol
 * ids and test ts_node_symbol() in O(1) with no string work.
 *
 * The cache is THREAD-LOCAL: extraction workers are independent pthreads, so a
 * per-thread cache needs no locking and is trivially correct. Bitsets are built
 * once per (lang, array) per thread from static spec arrays (bounded, stable).
 * Any type name that fails to resolve to a symbol disables the bitset for that
 * set (exact=false) and we fall back to the exact strcmp behavior — so the
 * result is always identical to the original, only faster. */
static bool kind_in_set_strcmp(TSNode node, const char *const *types) {
    const char *kind = ts_node_type(node);
    for (const char *const *t = types; *t; t++) {
        if (strcmp(kind, *t) == 0) {
            return true;
        }
    }
    return false;
}

typedef struct {
    const TSLanguage *lang;   /* NULL = empty slot */
    const char *const *types; /* identity key (static spec array pointer) */
    uint64_t *bits;           /* symbol bitset; NULL when exact==false */
    uint32_t nsyms;           /* ts_language_symbol_count(lang) */
    bool exact;               /* false → every name resolved; use strcmp fallback */
} ks_slot_t;

enum { KS_SLOTS = 512, KS_SLOT_MASK = 511, KS_PROBE = 8 };
static CBM_TLS ks_slot_t ks_cache[KS_SLOTS];

static ks_slot_t *ks_build(const TSLanguage *lang, const char *const *types, ks_slot_t *s) {
    s->lang = lang;
    s->types = types;
    s->bits = NULL;
    s->nsyms = 0;
    s->exact = false;
    uint32_t nsyms = ts_language_symbol_count(lang);
    if (nsyms == 0) {
        return s; /* fall back to strcmp */
    }
    uint64_t *bits = calloc(((size_t)nsyms + 63) / 64, sizeof(uint64_t));
    if (!bits) {
        return s;
    }
    bool all_resolved = true;
    for (const char *const *t = types; *t; t++) {
        uint32_t len = (uint32_t)strlen(*t);
        /* A name may be a named node type or an anonymous token ("for", "&&"):
         * set whichever symbol(s) exist so ts_node_symbol matches either. */
        TSSymbol sn = ts_language_symbol_for_name(lang, *t, len, true);
        TSSymbol sa = ts_language_symbol_for_name(lang, *t, len, false);
        bool any = false;
        if (sn != 0 && sn < nsyms) {
            bits[sn >> 6] |= (uint64_t)1 << (sn & 63);
            any = true;
        }
        if (sa != 0 && sa < nsyms) {
            bits[sa >> 6] |= (uint64_t)1 << (sa & 63);
            any = true;
        }
        if (!any) {
            all_resolved = false; /* unknown name → can't represent exactly */
        }
    }
    if (!all_resolved) {
        free(bits);
        return s; /* exact stays false */
    }
    s->bits = bits;
    s->nsyms = nsyms;
    s->exact = true;
    return s;
}

/* Find or build the cache slot for (lang, types). Returns NULL only if the
 * thread-local table is saturated at this hash (extremely rare → strcmp). */
static ks_slot_t *ks_get(const TSLanguage *lang, const char *const *types) {
    uintptr_t h = ((uintptr_t)types >> 4) ^ ((uintptr_t)lang >> 3) ^ ((uintptr_t)types >> 13);
    for (int probe = 0; probe < KS_PROBE; probe++) {
        ks_slot_t *s = &ks_cache[(size_t)(h + (uintptr_t)probe) & KS_SLOT_MASK];
        if (s->lang == NULL) {
            return ks_build(lang, types, s);
        }
        if (s->lang == lang && s->types == types) {
            return s;
        }
    }
    return NULL;
}

bool cbm_kind_in_set(TSNode node, const char **types) {
    if (!types || !types[0]) {
        return false;
    }
    const TSLanguage *lang = ts_node_language(node);
    if (lang) {
        ks_slot_t *s = ks_get(lang, (const char *const *)types);
        if (s && s->exact && s->bits) {
            TSSymbol sym = ts_node_symbol(node);
            return sym < s->nsyms && (((s->bits[sym >> 6] >> (sym & 63)) & 1U) != 0);
        }
    }
    return kind_in_set_strcmp(node, (const char *const *)types);
}

bool cbm_is_namespace_scope_kind(CBMLanguage lang, const char *kind) {
    if (!kind) {
        return false;
    }
    if (lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA) {
        return strcmp(kind, "namespace_definition") == 0;
    }
    if (lang == CBM_LANG_TYPESCRIPT || lang == CBM_LANG_TSX) {
        return strcmp(kind, "internal_module") == 0;
    }
    return false;
}

/* Free the calling thread's node-type bitset cache (the calloc'd `bits` arrays
 * that cbm_kind_in_set builds lazily). The cache is thread-local, so each worker
 * thread and the main thread must call this at teardown (worker exit / process
 * exit) for LeakSanitizer to report no leak. Safe if no cache was ever built. */
void cbm_kind_in_set_free_cache(void) {
    for (int i = 0; i < KS_SLOTS; i++) {
        free(ks_cache[i].bits);
        ks_cache[i].bits = NULL;
        ks_cache[i].lang = NULL;
        ks_cache[i].types = NULL;
        ks_cache[i].nsyms = 0;
        ks_cache[i].exact = false;
    }
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

static int count_branching_iter(TSNode root, const char **types) {
    int count = 0;
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    bool complete = false;
    while (!complete) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (cbm_kind_in_set(node, types)) {
            count++;
        }
        if (ts_node_child_count(node) > 0 && ts_tree_cursor_goto_first_child(&cursor)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (!ts_tree_cursor_goto_parent(&cursor)) {
                complete = true;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cursor);
    return count;
}

int cbm_count_branching(TSNode node, const char **branching_types) {
    if (!branching_types) {
        return 0;
    }
    return count_branching_iter(node, branching_types);
}

// Loop node-type names across tree-sitter grammars, for loop-nesting depth.
bool cbm_is_loop_node_type(const char *kind) {
    if (!kind || !kind[0]) {
        return false;
    }
    switch (kind[0]) {
    case 'c':
        return strcmp(kind, "c_style_for_statement") == 0;
    case 'd':
        return strcmp(kind, "do_statement") == 0 || strcmp(kind, "do_while_statement") == 0;
    case 'e':
        return strcmp(kind, "enhanced_for_statement") == 0;
    case 'f':
        return strcmp(kind, "for") == 0 || strcmp(kind, "for_statement") == 0 ||
               strcmp(kind, "for_in_statement") == 0 || strcmp(kind, "for_of_statement") == 0 ||
               strcmp(kind, "for_each_statement") == 0 || strcmp(kind, "foreach_statement") == 0 ||
               strcmp(kind, "for_range_loop") == 0 || strcmp(kind, "for_expression") == 0;
    case 'l':
        return strcmp(kind, "loop_expression") == 0;
    case 'r':
        return strcmp(kind, "repeat_statement") == 0 || strcmp(kind, "repeat_while_statement") == 0;
    case 'u':
        return strcmp(kind, "until") == 0 || strcmp(kind, "until_modifier") == 0;
    case 'w':
        return strcmp(kind, "while") == 0 || strcmp(kind, "while_statement") == 0 ||
               strcmp(kind, "while_expression") == 0 || strcmp(kind, "while_let_expression") == 0 ||
               strcmp(kind, "while_modifier") == 0;
    default:
        return false;
    }
}

TSNode cbm_c_family_declarator_name(TSNode node) {
    /* Compatibility entry point retained for branch callers. Route both APIs
     * through the canonical #438 implementation so depth limits and terminal
     * name kinds cannot drift across extraction pathways. */
    return cbm_resolve_c_declarator_name_node(node);
}

// Is `kind` a chained member/subscript access node? Language-agnostic generic
// set covering the common grammars; used only for the structural "access depth"
// smell, so unmatched grammars simply report 0 (never wrong, just silent).
static bool is_member_access_node(const char *kind) {
    if (!kind || !kind[0]) {
        return false;
    }
    switch (kind[0]) {
    case 'a':
        return strcmp(kind, "attribute") == 0;
    case 'e':
        return strcmp(kind, "element_access_expression") == 0;
    case 'f':
        return strcmp(kind, "field_expression") == 0 || strcmp(kind, "field_access") == 0;
    case 'i':
        return strcmp(kind, "index_expression") == 0;
    case 'm':
        return strcmp(kind, "member_expression") == 0 ||
               strcmp(kind, "member_access_expression") == 0;
    case 'n':
        return strcmp(kind, "navigation_expression") == 0;
    case 's':
        return strcmp(kind, "selector_expression") == 0 || strcmp(kind, "subscript") == 0 ||
               strcmp(kind, "subscript_expression") == 0 || strcmp(kind, "scoped_identifier") == 0;
    default:
        return false;
    }
}

typedef struct {
    int bdepth;
    int ldepth;
    int adepth;
} complexity_depth_t;

typedef struct {
    complexity_depth_t inline_items[CBM_SZ_64];
    complexity_depth_t *items;
    int capacity;
} complexity_depth_stack_t;

static void complexity_depth_stack_init(complexity_depth_stack_t *stack) {
    stack->items = stack->inline_items;
    stack->capacity = CBM_SZ_64;
}

static void complexity_depth_stack_destroy(complexity_depth_stack_t *stack) {
    if (stack->items != stack->inline_items) {
        free(stack->items);
    }
}

/* Ensure an entry exists for a cursor depth. Ordinary syntax stays entirely in
 * the compact inline array; unusually deep trees grow geometrically. */
static bool complexity_depth_stack_ensure(complexity_depth_stack_t *stack, int required) {
    if (required > stack->capacity) {
        if (stack->capacity > INT_MAX / PAIR_LEN) {
            return false;
        }
        int next_capacity = stack->capacity;
        while (next_capacity < required) {
            if (next_capacity > INT_MAX / PAIR_LEN) {
                return false;
            }
            next_capacity *= PAIR_LEN;
        }
        if ((size_t)next_capacity > SIZE_MAX / sizeof(*stack->items)) {
            return false;
        }
        size_t bytes = (size_t)next_capacity * sizeof(*stack->items);
        complexity_depth_t *grown = NULL;
        if (stack->items == stack->inline_items) {
            grown = (complexity_depth_t *)malloc(bytes);
            if (grown) {
                memcpy(grown, stack->inline_items, (size_t)stack->capacity * sizeof(*stack->items));
            }
        } else {
            grown = (complexity_depth_t *)realloc(stack->items, bytes);
        }
        if (!grown) {
            return false;
        }
        stack->items = grown;
        stack->capacity = next_capacity;
    }
    return true;
}

// One cap-free cursor traversal computes all complexity metrics. Runtime is
// O(N) for N AST nodes; explicit scratch is O(D) for syntax depth D rather than
// O(N), and tree-sitter's cursor carries the matching traversal path.
bool cbm_compute_complexity(TSNode node, const char **branching_types, cbm_complexity_t *out) {
    out->cyclomatic = 0;
    out->cognitive = 0;
    out->loop_count = 0;
    out->loop_depth = 0;
    out->max_access_depth = 0;
    if (!branching_types) {
        return true;
    }
    complexity_depth_stack_t stack;
    complexity_depth_stack_init(&stack);
    stack.items[0] = (complexity_depth_t){0};
    int depth = 0;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    bool complete = false;
    bool success = true;
    while (!complete) {
        TSNode current = ts_tree_cursor_current_node(&cursor);
        complexity_depth_t child = stack.items[depth];
        const char *kind = ts_node_type(current);
        bool named = ts_node_is_named(current);

        child.adepth = 0;
        if (named && is_member_access_node(kind)) {
            child.adepth = stack.items[depth].adepth + SKIP_ONE;
            if (child.adepth > out->max_access_depth) {
                out->max_access_depth = child.adepth;
            }
        }
        if (cbm_kind_in_set(current, branching_types)) {
            out->cyclomatic++;
            out->cognitive += SKIP_ONE + stack.items[depth].bdepth;
            child.bdepth = stack.items[depth].bdepth + SKIP_ONE;
        }
        if (named && cbm_is_loop_node_type(kind)) {
            out->loop_count++;
            child.ldepth = stack.items[depth].ldepth + SKIP_ONE;
            if (child.ldepth > out->loop_depth) {
                out->loop_depth = child.ldepth;
            }
        }

        if (ts_node_child_count(current) > 0 && ts_tree_cursor_goto_first_child(&cursor)) {
            depth++;
            if (!complexity_depth_stack_ensure(&stack, depth + SKIP_ONE)) {
                success = false;
                break;
            }
            stack.items[depth] = child;
            continue;
        }
        if (ts_tree_cursor_goto_next_sibling(&cursor)) {
            continue;
        }
        bool found = false;
        while (ts_tree_cursor_goto_parent(&cursor)) {
            depth--;
            if (ts_tree_cursor_goto_next_sibling(&cursor)) {
                found = true;
                break;
            }
        }
        if (!found) {
            complete = true;
        }
    }
    ts_tree_cursor_delete(&cursor);
    complexity_depth_stack_destroy(&stack);
    if (!success) {
        return false;
    }
    return true;
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
    default: {
        /* Enclosing-function drift fix (QUALITY_ANALYSIS gap #3): languages
         * without a curated func_kinds entry previously fell back to
         * func_kinds_generic, which misses their real function node types
         * (e.g. dart function_signature, perl subroutine_declaration_statement,
         * scss mixin_statement, nix function_expression, fortran subroutine,
         * cobol program_definition, verilog/vhdl, ...). The enclosing-function
         * walk then never found the parent function and attributed every
         * in-body call to the Module node. Use the language spec's
         * function_node_types (the single source of truth that extraction
         * already uses) when the curated switch has no entry. Curated languages
         * above are unchanged. */
        const CBMLangSpec *spec = cbm_lang_spec(lang);
        if (spec && spec->function_node_types && spec->function_node_types[0])
            return spec->function_node_types;
        return func_kinds_generic;
    }
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

// Check if a node type is a terminal C declarator name.
static bool is_c_terminal_name(const char *dk) {
    return strcmp(dk, "identifier") == 0 || strcmp(dk, "field_identifier") == 0 ||
           strcmp(dk, "operator_name") == 0 || strcmp(dk, "operator_cast") == 0 ||
           strcmp(dk, "destructor_name") == 0;
}

// Resolve name from a C++ qualified_identifier/scoped_identifier.
static TSNode resolve_qualified_name(TSNode decl) {
    static const char *name_kinds[] = {"operator_name", "operator_cast",    "destructor_name",
                                       "identifier",    "field_identifier", NULL};
    for (const char **k = name_kinds; *k; k++) {
        TSNode found = cbm_find_child_by_kind(decl, *k);
        if (!ts_node_is_null(found)) {
            return found;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Resolve function name from C/C++/CUDA/GLSL declarator chain. Shared canonical
// implementation — see the header for the full rationale (#438).
TSNode cbm_resolve_c_declarator_name_node(TSNode func_node) {
    TSNode decl = ts_node_child_by_field_name(func_node, TS_FIELD("declarator"));
    for (int depth = 0; depth < CBM_DECLARATOR_DEPTH_LIMIT && !ts_node_is_null(decl); depth++) {
        const char *dk = ts_node_type(decl);
        if (is_c_terminal_name(dk)) {
            return decl;
        }
        if (strcmp(dk, "qualified_identifier") == 0 || strcmp(dk, "scoped_identifier") == 0) {
            return resolve_qualified_name(decl);
        }
        TSNode inner = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
        if (ts_node_is_null(inner) && ts_node_named_child_count(decl) > 0) {
            inner = ts_node_named_child(decl, 0);
        }
        if (ts_node_is_null(inner)) {
            break;
        }
        decl = inner;
    }
    TSNode null_node = {0};
    return null_node;
}

// Convert a resolved function/method name node to its name string. Most nodes
// map directly to their text, but a C++ conversion-operator's `operator_cast`
// node spans the full "operator bool() const" — this grammar folds the parameter
// list and cv-qualifiers into the node. The method's name is only the
// "operator <type>" prefix, so truncate at the first '(' and trim trailing
// space. Without this the conversion operator is indexed as "operator bool()
// const", and a member lookup for "operator bool" (the implicit call in
// `if (obj)`) misses.
char *cbm_func_name_node_text(CBMArena *a, TSNode name_node, const char *source, CBMLanguage lang) {
    char *text = cbm_node_text(a, name_node, source);
    if (text && strcmp(ts_node_type(name_node), "operator_cast") == 0) {
        char *paren = strchr(text, '(');
        if (paren) {
            while (paren > text && (paren[-1] == ' ' || paren[-1] == '\t')) {
                paren--;
            }
            *paren = '\0';
        }
    }
    /* Nix quoted attrpath segment: `"kebab-case" = a: a;` and `services."my.svc" = …`
     * are ordinary names that merely need quoting in source. The node text carries
     * the delimiters, so without this the def is named `"kebab-case"` — quotes and
     * all — and every consumer keying on the name (search, CALLS resolution, the
     * LSP join) would have to know to re-quote. Stripped here rather than in the
     * resolver so the def name and the call-scope QN, which both route through this
     * function, cannot disagree. */
    if (text && lang == CBM_LANG_NIX) {
        cbm_nix_strip_attr_quotes(text);
    }
    return text;
}

/* ── Nix attrpath helpers ───────────────────────────────────
 * A Nix binding's name is a PATH (`a.b.c = …`), whose segments may be quoted or
 * interpolated. These render it the way the rest of the extractor expects: leaf
 * segment as the name, leading segments as scope. Shared by the defs and unified
 * (call-scope) extractors so both compute the same QN — if they disagree, a CALLS
 * edge names a source node that does not exist and is dropped at write, which is
 * precisely how the function-header bug manifested. */

/* Strip one matching pair of surrounding double quotes, in place. */
void cbm_nix_strip_attr_quotes(char *text) {
    if (!text) {
        return;
    }
    size_t len = strlen(text);
    if (len >= CBM_QUOTE_PAIR && text[0] == '"' && text[len - SKIP_ONE] == '"') {
        memmove(text, text + SKIP_ONE, len - PAIR_LEN);
        text[len - PAIR_LEN] = '\0';
    }
}

/* True when a segment contains a `${...}` interpolation, and therefore has no
 * statically knowable name. The tree cursor visits the complete finite subtree
 * in O(N) runtime and O(1) auxiliary memory for N syntax nodes; unlike a fixed
 * stack, it cannot silently classify a later interpolation as static. */
bool cbm_nix_attr_is_interpolated(TSNode attr) {
    if (ts_node_is_null(attr)) {
        return false;
    }
    bool found = false;
    bool complete = false;
    TSTreeCursor cursor = ts_tree_cursor_new(attr);
    while (!complete) {
        if (strcmp(ts_node_type(ts_tree_cursor_current_node(&cursor)), "interpolation") == 0) {
            found = true;
            break;
        }
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (!ts_tree_cursor_goto_parent(&cursor)) {
                complete = true;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cursor);
    return found;
}

/* The leaf segment of an attrpath — the name. `attr` is a FIELD in this grammar,
 * not a node type (segments are identifier / string_expression / interpolation),
 * so iterate named children rather than matching on a type string. */
TSNode cbm_nix_attrpath_last_attr(TSNode attrpath) {
    TSNode last = {0};
    if (ts_node_is_null(attrpath)) {
        return last;
    }
    uint32_t n = ts_node_named_child_count(attrpath);
    if (n == 0) {
        return last;
    }
    return ts_node_named_child(attrpath, n - SKIP_ONE);
}

/* Source span of one statically rendered attr segment without surrounding
 * double quotes. The source belongs to the parsed tree, so node byte offsets
 * are the single authority used by both sizing and copying passes. */
static bool nix_attr_unquoted_span(TSNode segment, const char *source, uint32_t *start_out,
                                   uint32_t *end_out) {
    if (!source || !start_out || !end_out || ts_node_is_null(segment)) {
        return false;
    }
    uint32_t start = ts_node_start_byte(segment);
    uint32_t end = ts_node_end_byte(segment);
    if (end <= start) {
        return false;
    }
    if (end - start >= CBM_QUOTE_PAIR && source[start] == '"' && source[end - SKIP_ONE] == '"') {
        start += SKIP_ONE;
        end -= SKIP_ONE;
    }
    *start_out = start;
    *end_out = end;
    return true;
}

/* The scope prefix of an attrpath: every segment except the leaf, quote-stripped
 * and dot-joined. `a.b.fn = …` yields "a.b" so it qualifies identically to the
 * nested spelling `a = { b = { fn = …; }; }`. Returns NULL for a single-segment
 * path (no scope) or when a leading segment is interpolated (not nameable). */
const char *cbm_nix_attrpath_scope(CBMArena *a, TSNode attrpath, const char *source) {
    if (!a || !source || ts_node_is_null(attrpath)) {
        return NULL;
    }
    uint32_t n = ts_node_named_child_count(attrpath);
    if (n <= SKIP_ONE) {
        return NULL;
    }

    /* Validate and size once, then allocate/copy once. Repeatedly formatting
     * each growing prefix retained every abandoned prefix in the file arena,
     * taking O(L^2) runtime and memory for total scope length L. */
    size_t scope_len = 0;
    for (uint32_t i = 0; i + SKIP_ONE < n; i++) {
        TSNode seg = ts_node_named_child(attrpath, i);
        if (cbm_nix_attr_is_interpolated(seg)) {
            return NULL;
        }
        uint32_t start = 0;
        uint32_t end = 0;
        if (!nix_attr_unquoted_span(seg, source, &start, &end)) {
            return NULL;
        }
        size_t segment_len = (size_t)(end - start);
        size_t separator_len = i > 0 ? SKIP_ONE : 0;
        if (scope_len > SIZE_MAX - separator_len ||
            segment_len > SIZE_MAX - (scope_len + separator_len)) {
            return NULL;
        }
        scope_len += separator_len + segment_len;
    }
    if (scope_len == SIZE_MAX) {
        return NULL;
    }
    char *scope = cbm_arena_alloc(a, scope_len + SKIP_ONE);
    if (!scope) {
        return NULL;
    }
    size_t offset = 0;
    for (uint32_t i = 0; i + SKIP_ONE < n; i++) {
        TSNode seg = ts_node_named_child(attrpath, i);
        uint32_t start = 0;
        uint32_t end = 0;
        (void)nix_attr_unquoted_span(seg, source, &start, &end);
        if (i > 0) {
            scope[offset++] = '.';
        }
        size_t segment_len = (size_t)(end - start);
        memcpy(scope + offset, source + start, segment_len);
        offset += segment_len;
    }
    scope[offset] = '\0';
    return scope;
}

/* True when a Nix `binding`'s value is an attribute set, i.e. the binding names a
 * scope rather than defining a value. Deliberately excludes `let` bindings and
 * lambda-valued bindings: the former are lexical (C++ does not qualify by block
 * scope either), the latter are definitions in their own right. */
bool cbm_nix_binding_is_attrset_scope(TSNode node) {
    if (ts_node_is_null(node) || strcmp(ts_node_type(node), "binding") != 0) {
        return false;
    }
    TSNode value = ts_node_child_by_field_name(node, TS_FIELD("expression"));
    if (ts_node_is_null(value)) {
        return false;
    }
    const char *vk = ts_node_type(value);
    return strcmp(vk, "attrset_expression") == 0 || strcmp(vk, "rec_attrset_expression") == 0;
}

/* The scope QN contributed by a Nix `binding` whose value is an attribute set —
 * `setA = { … }` contributes "…file.setA", and a dotted `a.b = { … }` contributes
 * "…file.a.b". Returns saved_enclosing unchanged when the binding cannot name a
 * scope (empty or interpolated attrpath).
 *
 * Lives here, called by BOTH extract_defs.c and extract_unified.c, because those
 * two files carry separate compute_class_qn implementations. A def QN and a
 * call-scope QN that disagree by even one segment make the CALLS edge name a
 * source node that was never minted, and it is silently dropped at write. Sharing
 * the computation makes that class of drift impossible rather than merely
 * unlikely. */
const char *cbm_nix_binding_scope_qn(CBMExtractCtx *ctx, TSNode node, const char *saved_enclosing) {
    if (!ctx || ts_node_is_null(node) || strcmp(ts_node_type(node), "binding") != 0) {
        return saved_enclosing;
    }
    TSNode attrpath = ts_node_child_by_field_name(node, TS_FIELD("attrpath"));
    TSNode leaf = cbm_nix_attrpath_last_attr(attrpath);
    if (ts_node_is_null(leaf) || cbm_nix_attr_is_interpolated(leaf)) {
        return saved_enclosing;
    }
    char *leaf_text = cbm_node_text(ctx->arena, leaf, ctx->source);
    if (!leaf_text || !leaf_text[0]) {
        return saved_enclosing;
    }
    cbm_nix_strip_attr_quotes(leaf_text);
    const char *scope = cbm_nix_attrpath_scope(ctx->arena, attrpath, ctx->source);
    const char *rel = scope ? cbm_arena_sprintf(ctx->arena, "%s.%s", scope, leaf_text) : leaf_text;
    if (saved_enclosing) {
        return cbm_arena_sprintf(ctx->arena, "%s.%s", saved_enclosing, rel);
    }
    return cbm_fqn_compute_source_lang(ctx->arena, ctx->project, ctx->rel_path, rel, ctx->language);
}

/* The QN-relative name of a Nix binding: its attrpath scope joined to its leaf
 * name. `a.b.fn = …` yields "a.b.fn"; a bare `fn = …` yields "fn". Callers prepend
 * either the enclosing attrset scope or the module QN, so the two scope sources —
 * a dotted attrpath and an enclosing attrset — compose. Takes the already-resolved
 * leaf name rather than re-deriving it, so it cannot disagree with the name the
 * def was minted under. */
const char *cbm_nix_qn_name(CBMArena *a, TSNode func_node, const char *source, const char *name) {
    if (!name) {
        return NULL;
    }
    TSNode parent = ts_node_parent(func_node);
    if (ts_node_is_null(parent) || strcmp(ts_node_type(parent), "binding") != 0) {
        return name;
    }
    TSNode attrpath = ts_node_child_by_field_name(parent, TS_FIELD("attrpath"));
    const char *scope = cbm_nix_attrpath_scope(a, attrpath, source);
    return scope ? cbm_arena_sprintf(a, "%s.%s", scope, name) : name;
}

static const char *func_node_name(CBMArena *a, TSNode func_node, const char *source,
                                  CBMLanguage lang) {
    if ((lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA ||
         lang == CBM_LANG_GLSL || lang == CBM_LANG_HLSL || lang == CBM_LANG_ISPC ||
         lang == CBM_LANG_SLANG || lang == CBM_LANG_OBJC) &&
        strcmp(ts_node_type(func_node), "function_definition") == 0) {
        TSNode c_name = cbm_c_family_declarator_name(func_node);
        if (!ts_node_is_null(c_name)) {
            return cbm_node_text(a, c_name, source);
        }
    }

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

    TSNode name_node = ts_node_child_by_field_name(func_node, TS_FIELD("name"));
    if (!ts_node_is_null(name_node)) {
        return cbm_node_text(a, name_node, source);
    }
    // Arrow functions: check parent variable_declarator
    if (strcmp(ts_node_type(func_node), "arrow_function") == 0) {
        TSNode parent = ts_node_parent(func_node);
        if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "variable_declarator") == 0) {
            TSNode vname = ts_node_child_by_field_name(parent, TS_FIELD("name"));
            if (!ts_node_is_null(vname)) {
                return cbm_node_text(a, vname, source);
            }
        }
    }
    // C/C++/CUDA/GLSL: function_definition carries its name in the declarator chain.
    if (strcmp(ts_node_type(func_node), "function_definition") == 0) {
        TSNode dn = cbm_resolve_c_declarator_name_node(func_node);
        if (!ts_node_is_null(dn)) {
            return cbm_func_name_node_text(a, dn, source, lang);
        }
    }
    return NULL;
}

const char *cbm_enclosing_func_qn(CBMArena *a, TSNode node, CBMLanguage lang, const char *source,
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

    // Check if the function is inside a class — compute classQN.funcName.
    // For nested classes the class QN must carry the FULL nesting chain
    // (Outer.Inner, not just Inner) so it matches the class/method node QN the
    // def walk produces via compute_class_qn (extract_defs.c). Qualifying with
    // only the innermost class under-qualified the enclosing QN, so a call
    // inside a nested-class method sourced to the file node instead of its
    // method node and failed to join the LSP-resolved call by caller QN.
    const CBMLangSpec *spec = cbm_lang_spec(lang);
    if (spec && spec->class_node_types) {
        // Build the dotted class chain from the outermost enclosing class down
        // to the innermost. Walk parents collecting class names innermost-first,
        // then prepend each as we ascend so the result reads Outer.Inner.
        const char *class_chain = NULL;
        for (TSNode cur = ts_node_parent(func_node); !ts_node_is_null(cur);
             cur = ts_node_parent(cur)) {
            if (!cbm_kind_in_set(cur, spec->class_node_types) &&
                !cbm_is_namespace_scope_kind(lang, ts_node_type(cur))) {
                continue;
            }
            TSNode class_name = ts_node_child_by_field_name(cur, TS_FIELD("name"));
            if (ts_node_is_null(class_name)) {
                continue;
            }
            char *cname = cbm_node_text(a, class_name, source);
            if (!cname || !cname[0]) {
                continue;
            }
            class_chain = class_chain ? cbm_arena_sprintf(a, "%s.%s", cname, class_chain) : cname;
        }
        if (class_chain) {
            const char *class_qn = cbm_fqn_compute(a, project, rel_path, class_chain);
            return cbm_arena_sprintf(a, "%s.%s", class_qn, name);
        }
    }

    return cbm_fqn_compute(a, project, rel_path, name);
}

// --- Cached enclosing function QN ---

const char *cbm_enclosing_func_qn_cached(CBMExtractCtx *ctx, TSNode node) {
    uint32_t pos = ts_node_start_byte(node);

    // Check cache: find a function range that contains this position.
    // Linear scan is fine for EFC_SIZE=CBM_SZ_64 (all entries fit in ~1 cache line).
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
/* tree-sitter-properties roots at `file`. */
static const char *module_parents_properties[] = {"file", "source_file", NULL};

// Check if parent node kind matches direct-or-grandparent for scripting languages.
// Returns true if pk matches root_kind, or pk matches wrapper_kind and grandparent is root_kind.
static bool check_script_module_level(TSNode parent, const char *pk, const char *root_kind,
                                      const char *wrapper_kind) {
    if (strcmp(pk, root_kind) == 0) {
        return true;
    }
    if (wrapper_kind && strcmp(pk, wrapper_kind) == 0) {
        TSNode gp = ts_node_parent(parent);
        return !ts_node_is_null(gp) && strcmp(ts_node_type(gp), root_kind) == 0;
    }
    return false;
}

// Get the module-level parent type list for table-driven languages.
static const char **get_module_parents(CBMLanguage lang) {
    switch (lang) {
    case CBM_LANG_GO:
        return module_parents_go;
    case CBM_LANG_RUST:
        return module_parents_rust;
    case CBM_LANG_JAVA:
        return module_parents_java;
    case CBM_LANG_KOTLIN:
        return module_parents_kotlin;
    case CBM_LANG_SCALA:
        return module_parents_scala;
    case CBM_LANG_CSHARP:
        return module_parents_csharp;
    case CBM_LANG_PHP:
        return module_parents_php;
    case CBM_LANG_RUBY:
        return module_parents_ruby;
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_OBJC:
        return module_parents_c;
    case CBM_LANG_ZIG:
        return module_parents_zig;
    case CBM_LANG_BASH:
        return module_parents_bash;
    case CBM_LANG_ERLANG:
        return module_parents_erlang;
    case CBM_LANG_HASKELL:
        return module_parents_haskell;
    case CBM_LANG_OCAML:
        return module_parents_ocaml;
    case CBM_LANG_ELIXIR:
        return module_parents_elixir;
    case CBM_LANG_HTML:
        return module_parents_html;
    case CBM_LANG_CSS:
    case CBM_LANG_SCSS:
        return module_parents_css;
    case CBM_LANG_SQL:
        return module_parents_sql;
    case CBM_LANG_TOML:
        return module_parents_toml;
    case CBM_LANG_HCL:
        return module_parents_hcl;
    case CBM_LANG_JSON:
    case CBM_LANG_INI:
    case CBM_LANG_XML:
    case CBM_LANG_MARKDOWN:
        return module_parents_config;
    case CBM_LANG_SWIFT:
        return module_parents_zig;
    case CBM_LANG_DART:
        return module_parents_php;
    case CBM_LANG_PERL:
    case CBM_LANG_GROOVY:
    case CBM_LANG_DOCKERFILE: // top-level instructions are children of source_file
        return module_parents_zig;
    case CBM_LANG_R:
        return module_parents_php;
    case CBM_LANG_MAKEFILE:
        return module_parents_makefile;
    case CBM_LANG_COMMONLISP:
        return module_parents_commonlisp;
    case CBM_LANG_MATLAB:
        return module_parents_matlab;
    case CBM_LANG_LEAN:
        return module_parents_zig;
    case CBM_LANG_FORM:
        return module_parents_form;
    case CBM_LANG_MAGMA:
        return module_parents_magma;
    case CBM_LANG_PROPERTIES:
        return module_parents_properties;
    case CBM_LANG_GOMOD: // require_directive lives at source_file top level
        return module_parents_zig;
    default:
        return NULL;
    }
}

/* Variant that takes the node's parent DIRECTLY. The callers in
 * extract_defs.c iterate a known parent's children, so they already
 * have the parent — passing it here avoids ts_node_parent(node), which
 * is O(n) per call (tree-sitter nodes carry no parent pointer; the
 * parent is found by rescanning from the root). On a pathologically
 * large file (e.g. a 583k-line generated/fixture file with tens of
 * thousands of top-level statements) the old per-child ts_node_parent
 * made extraction O(n²) and effectively hung. */
bool cbm_is_module_level_p(TSNode parent, CBMLanguage lang) {
    if (ts_node_is_null(parent)) {
        return false;
    }
    const char *pk = ts_node_type(parent);

    // Languages with wrapper-pattern (expression_statement/export_statement/assignment_statement)
    if (lang == CBM_LANG_PYTHON) {
        return check_script_module_level(parent, pk, "module", "expression_statement");
    }
    if (lang == CBM_LANG_JAVASCRIPT || lang == CBM_LANG_TYPESCRIPT || lang == CBM_LANG_TSX) {
        return check_script_module_level(parent, pk, "program", "export_statement");
    }
    if (lang == CBM_LANG_LUA) {
        return check_script_module_level(parent, pk, "chunk", "assignment_statement");
    }
    if (lang == CBM_LANG_YAML) {
        return strcmp(pk, "document") == 0 || strcmp(pk, "stream") == 0 ||
               strcmp(pk, "block_mapping") == 0;
    }

    // Table lookup for the rest
    const char **parents = get_module_parents(lang);
    if (parents) {
        for (const char **p = parents; *p; p++) {
            if (strcmp(pk, *p) == 0) {
                return true;
            }
        }
    }
    return false;
}

/* Back-compat wrapper: computes the parent via ts_node_parent (O(n)).
 * Prefer cbm_is_module_level_p at call sites that already know the
 * parent (the common case — iterating a parent's children). */
bool cbm_is_module_level(TSNode node, CBMLanguage lang) {
    return cbm_is_module_level_p(ts_node_parent(node), lang);
}

// --- FQN computation ---
// Mirrors Go's fqn.Compute(): project + path_parts_dotted + name

// Internal helper: find extension start in basename (returns length without ext)
static size_t strip_ext_len(const char *s, size_t len) {
    for (size_t i = len; i > 0; i--) {
        if (s[i - SKIP_ONE] == '.') {
            /* A dot at the very start of a filename segment (index 0, or right
             * after a '/') is a DOTFILE marker (".env", ".gitignore"), NOT an
             * extension separator. Stripping there leaves an empty stem whose
             * module QN collides with the parent directory/project root. Keep
             * the whole name as the stem; the leading dot is dropped later in
             * append_path_segments. */
            if (i - SKIP_ONE == 0 || s[i - SKIP_ONE - SKIP_ONE] == '/') {
                return len;
            }
            return i - SKIP_ONE;
        }
        if (s[i - SKIP_ONE] == '/') {
            break;
        }
    }
    return len;
}

// Check if a path part should be skipped (Python __init__, JS/TS index).
static bool should_skip_fqn_part(const char *part, size_t part_len, bool is_last, bool has_name) {
    if (!is_last || !has_name) {
        return false;
    }
    if (part_len == INIT_FILE_LEN && memcmp(part, "__init__", INIT_FILE_LEN) == 0) {
        return true;
    }
    if (part_len == INDEX_FILE_LEN && memcmp(part, "index", INDEX_FILE_LEN) == 0) {
        return true;
    }
    return false;
}

// Append dotted path segments from rel_path (extension-stripped) to output buffer.
static char *append_path_segments(char *out, const char *rel_path, size_t plen, bool has_name) {
    const char *start = rel_path;
    const char *end_ptr = rel_path + plen;
    while (start < end_ptr) {
        const char *slash = (const char *)memchr(start, '/', end_ptr - start);
        const char *part_end = slash ? slash : end_ptr;
        size_t part_len = (size_t)(part_end - start);

        if (part_len > 0) {
            bool is_last = (part_end == end_ptr);
            if (!should_skip_fqn_part(start, part_len, is_last, has_name)) {
                /* Drop a leading '.' from a dotfile / hidden-dir segment
                 * (".env" -> "env", ".github" -> "github"). Otherwise the QN
                 * separator '.' plus the segment's own leading '.' produce a
                 * malformed "proj..env" double-dot, and a root dotfile's empty
                 * stem collides with the project QN. */
                const char *seg = start;
                size_t seg_len = part_len;
                if (seg[0] == '.') {
                    seg++;
                    seg_len--;
                }
                if (seg_len > 0) {
                    *out++ = '.';
                    memcpy(out, seg, seg_len);
                    out += seg_len;
                }
            }
        }
        start = part_end + SKIP_ONE;
    }
    return out;
}

char *cbm_fqn_compute(CBMArena *a, const char *project, const char *rel_path, const char *name) {
    if (!project)
        project = "";
    if (!rel_path)
        rel_path = "";
    size_t proj_len = strlen(project);
    size_t path_len = strlen(rel_path);
    size_t name_len = name ? strlen(name) : 0;

    size_t max_len = proj_len + SKIP_ONE + path_len + SKIP_ONE + name_len + SKIP_ONE;
    char *buf = (char *)cbm_arena_alloc(a, max_len);
    if (!buf) {
        return NULL;
    }

    char *out = buf;
    memcpy(out, project, proj_len);
    out += proj_len;

    size_t plen = strip_ext_len(rel_path, path_len);
    out = append_path_segments(out, rel_path, plen, name && name_len > 0);

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

// True when a language derives its module from the CONTAINING DIRECTORY (Java
// package, Go package) rather than baking the filename stem into the module QN.
// For these languages a sibling file in the same dir shares the module, and the
// type/method name is appended once — so a class `Outer` in `Outer.java` is
// `proj.Outer`, not `proj.Outer.Outer`, and a method in `myapp/db/conn.go`
// belongs to module `proj.myapp.db`, not `proj.myapp.db.conn`.
static bool cbm_lang_module_is_dir(CBMLanguage lang) {
    return lang == CBM_LANG_JAVA || lang == CBM_LANG_GO;
}

char *cbm_fqn_module_source_lang(CBMArena *a, const char *project, const char *rel_path,
                                 CBMLanguage lang) {
    if (!cbm_lang_module_is_dir(lang)) {
        // All other languages keep the legacy filename-stem module QN.
        return cbm_fqn_module(a, project, rel_path);
    }
    if (!rel_path) {
        rel_path = "";
    }
    // Module is the CONTAINING DIRECTORY: strip the basename (last '/' segment).
    const char *last_slash = strrchr(rel_path, '/');
    if (!last_slash) {
        // Root file: dir is empty → module is just the project.
        return cbm_fqn_folder(a, project, "");
    }
    size_t dir_len = (size_t)(last_slash - rel_path);
    char *dir = (char *)cbm_arena_alloc(a, dir_len + SKIP_ONE);
    if (!dir) {
        return NULL;
    }
    memcpy(dir, rel_path, dir_len);
    dir[dir_len] = '\0';
    return cbm_fqn_folder(a, project, dir);
}

char *cbm_fqn_compute_source_lang(CBMArena *a, const char *project, const char *rel_path,
                                  const char *name, CBMLanguage lang) {
    if (!cbm_lang_module_is_dir(lang)) {
        // All other languages keep the legacy filename-stem symbol QN.
        return cbm_fqn_compute(a, project, rel_path, name);
    }
    char *module = cbm_fqn_module_source_lang(a, project, rel_path, lang);
    if (!module) {
        return NULL;
    }
    if (!name || !name[0]) {
        return module;
    }
    return cbm_arena_sprintf(a, "%s.%s", module, name);
}

char *cbm_fqn_folder(CBMArena *a, const char *project, const char *rel_dir) {
    // project.dir1.dir2
    size_t proj_len = strlen(project);
    size_t dir_len = strlen(rel_dir);
    size_t max_len = proj_len + SKIP_ONE + dir_len + SKIP_ONE;
    char *buf = (char *)cbm_arena_alloc(a, max_len);
    if (!buf) {
        return NULL;
    }

    char *out = buf;
    memcpy(out, project, proj_len);
    out += proj_len;

    if (dir_len > 0 && !(dir_len == SKIP_ONE && rel_dir[0] == '.')) {
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
            start = part_end + SKIP_ONE;
        }
    }
    *out = '\0';
    return buf;
}

/* ── String literal classifier ──────────────────────────────────── */

// Check if a slash-prefixed string looks like a filesystem path.
static bool is_filesystem_path(const char *s, int len) {
    if (len <= MIN_SYS_PATH_LEN) {
        return false;
    }
    return strncmp(s, "/usr/", SLEN("/usr/")) == 0 || strncmp(s, "/bin/", SLEN("/bin/")) == 0 ||
           strncmp(s, "/etc/", SLEN("/etc/")) == 0 || strncmp(s, "/var/", SLEN("/var/")) == 0 ||
           strncmp(s, "/tmp/", SLEN("/tmp/")) == 0 || strncmp(s, "/opt/", SLEN("/opt/")) == 0 ||
           strncmp(s, "/home/", SLEN("/home/")) == 0 || strncmp(s, "/dev/", SLEN("/dev/")) == 0 ||
           strncmp(s, "/sys/", SLEN("/sys/")) == 0 || strncmp(s, "/proc/", SLEN("/proc/")) == 0;
}

// Check if a slash-prefixed string looks like a REST API path.
static bool is_rest_path(const char *s, int len) {
    if (is_filesystem_path(s, len)) {
        return false;
    }
    if (len > SKIP_ONE && s[len - SKIP_ONE] == '/') {
        return false; /* regex pattern */
    }
    if (s[SKIP_ONE] == '^') {
        return false; /* regex */
    }
    if (len == SKIP_ONE || (len == PAIR_LEN && s[SKIP_ONE] == '/')) {
        return false; /* bare / or // */
    }
    if (s[SKIP_ONE] == '.') {
        return false; /* relative path */
    }
    for (int i = SKIP_ONE; i < len && i < MAX_ROUTE_SCAN; i++) {
        char c = s[i];
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
            return true;
        }
    }
    return false;
}

static bool is_url_like(const char *s, int len) {
    if (len < MIN_ROUTE_LEN) {
        return false;
    }
    if (strstr(s, "://")) {
        return true;
    }
    if (s[0] == '/') {
        return is_rest_path(s, len);
    }
    return false;
}

static bool has_config_extension(const char *s, int len) {
    static const char *exts[] = {".toml", ".yaml", ".yml",  ".json",       ".ini",
                                 ".env",  ".cfg",  ".conf", ".properties", NULL};
    for (int i = 0; exts[i]; i++) {
        int elen = (int)strlen(exts[i]);
        if (len > elen && strcmp(s + len - elen, exts[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool is_env_var_pattern(const char *s, int len) {
    if (len < MIN_ROUTE_LEN || len > MAX_HEX_NAME_LEN) {
        return false;
    }
    bool has_upper = false;
    bool has_underscore = false;
    for (int i = 0; i < len; i++) {
        char c = s[i];
        if (c >= 'A' && c <= 'Z') {
            has_upper = true;
        } else if (c == '_') {
            has_underscore = true;
        } else if (c >= '0' && c <= '9') {
            /* digits ok */
        } else {
            return false;
        }
    }
    return has_upper && has_underscore;
}

int cbm_classify_string(const char *str, int len) {
    if (!str || len < PAIR_LEN) {
        return NOT_FOUND;
    }

    if (is_url_like(str, len)) {
        return CBM_STRREF_URL;
    }
    if (has_config_extension(str, len)) {
        return CBM_STRREF_CONFIG;
    }
    if (is_env_var_pattern(str, len)) {
        return CBM_STRREF_CONFIG;
    }

    return NOT_FOUND;
}

/* Flatten a JS/TS `template_string` node into plain text (issue #1006).
 * String fragments are kept verbatim; each ${...} substitution becomes the
 * "{}" placeholder so client URLs built from template literals share the
 * canonical parameter shape of server-side route paths
 * (`/things/${id}/x` -> "/things/{}/x"). Returns NULL when the node yields
 * no text or exceeds the route-sized buffer. */
const char *cbm_template_string_text(CBMArena *a, TSNode node, const char *source) {
    enum { TPL_BUF = 512 };
    char buf[TPL_BUF];
    size_t pos = 0;
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(node, i);
        const char *k = ts_node_type(c);
        if (strcmp(k, "string_fragment") == 0) {
            char *frag = cbm_node_text(a, c, source);
            if (!frag) {
                continue;
            }
            size_t fl = strlen(frag);
            if (pos + fl >= TPL_BUF) {
                return NULL;
            }
            memcpy(buf + pos, frag, fl);
            pos += fl;
        } else if (strcmp(k, "template_substitution") == 0) {
            if (pos + PAIR_LEN >= TPL_BUF) {
                return NULL;
            }
            buf[pos++] = '{';
            buf[pos++] = '}';
        }
    }
    if (pos == 0) {
        return NULL;
    }
    return cbm_arena_strndup(a, buf, pos);
}
