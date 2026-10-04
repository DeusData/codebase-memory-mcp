/*
 * callable_sig.c — see callable_sig.h for the identity contract (#2061).
 *
 * The builder renders the suffix into a growable arena buffer, then applies
 * the spelling rules in a single character pass, then the length cap. The result is re-checked
 * against cbm_qn_callable_base_len so a malformed parse can never mint a suffix the leaf splitters
 * would misread: such a suffix degrades to its hashed form.
 */
#include "callable_sig.h"
#include "helpers.h"
#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

enum {
    SIG_DEPTH_LIMIT = 64,                   /* type subtree recursion bound */
    SIG_DECL_DEPTH = 8,                     /* C-family declarator chain bound */
    SIG_MAX_ENTRIES = 256,                  /* entries tracked for the cap; more => capped */
    SIG_INITIAL_CAP = 256,                  /* first render buffer; grows by doubling */
    SIG_HASH_HEX = 16,                      /* FNV-1a 64 rendered as hex */
    SIG_OPERATOR_LEN = 8,                   /* strlen("operator") */
    SIG_CONST_LEN = 5,                      /* strlen("const") */
    SIG_VOLATILE_LEN = 8,                   /* strlen("volatile") */
    SIG_ARITY_DIGITS = 16,                  /* "(%d)" scratch */
    SIG_CAPPED_TAIL = 1 + SIG_HASH_HEX + 1, /* "#" hex ")" */
};

static const uint64_t SIG_FNV_OFFSET = 0xcbf29ce484222325ULL; /* FNV-1a 64 basis */
static const uint64_t SIG_FNV_PRIME = 0x100000001b3ULL;
static const char SIG_OPERATOR_CHARS[] = "+-*/%^&|~!=<>?[]";

CBMCallableIdentity cbm_callable_identity(CBMLanguage lang) {
    /* Every language keeps its historical QN until its enable change lands
     * (with the index-format bump that change carries). The planned modes:
     * Java/Kotlin/C#/C++/CUDA/Scala TYPED, Swift LABELED_TYPED, ObjC LABELED,
     * dynamic tier-2 languages ARITY. */
    (void)lang;
    return CBM_CALLABLE_ID_NONE;
}

/* ── Output buffer ─────────────────────────────────────────────── */

typedef struct {
    CBMArena *arena;
    const char *src;
    CBMLanguage lang;
    CBMCallableIdentity mode;
    char *buf;
    size_t len;
    size_t cap;
    bool overflow;
    int entries;
    bool too_many;
    size_t entry_end[SIG_MAX_ENTRIES];
    size_t open_off;  /* offset of the parameter list's "(" */
    size_t close_off; /* offset just past its ")" */
    bool skip_names;  /* nested C parameter lists: leave out parameter names */
} sig_ctx_t;

static bool sig_ident_char(unsigned char ch) {
    return isalnum(ch) || ch == '_' || ch == '$' || ch >= 0x80;
}

/* Grow by doubling inside the arena; the abandoned copies are bounded by the
 * final size (geometric), and the arena is the extraction's scratch anyway. */
static void sig_raw(sig_ctx_t *c, const char *s, size_t n) {
    if (c->overflow) {
        return;
    }
    if (c->len + n >= c->cap) {
        size_t cap = c->cap ? c->cap : SIG_INITIAL_CAP;
        while (c->len + n >= cap) {
            cap *= 2;
        }
        char *grown = cbm_arena_alloc(c->arena, cap);
        if (!grown) {
            c->overflow = true;
            return;
        }
        memcpy(grown, c->buf, c->len);
        c->buf = grown;
        c->cap = cap;
    }
    memcpy(c->buf + c->len, s, n);
    c->len += n;
    c->buf[c->len] = '\0';
}

static void sig_raw_str(sig_ctx_t *c, const char *s) {
    sig_raw(c, s, strlen(s));
}

/* Append one source token: internal whitespace is dropped except as a single
 * space between two identifier characters, which is also the rule at the
 * boundary with what is already in the buffer. */
static void sig_token(sig_ctx_t *c, const char *s, size_t n) {
    bool pending_space = false;
    for (size_t i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)s[i];
        if (isspace(ch)) {
            pending_space = true;
            continue;
        }
        bool first = c->len == 0 || c->buf[c->len - 1] == ' ';
        if (!first && sig_ident_char((unsigned char)c->buf[c->len - 1]) && sig_ident_char(ch) &&
            (pending_space || i == 0)) {
            sig_raw(c, " ", 1);
        }
        pending_space = false;
        sig_raw(c, (const char *)&s[i], 1);
    }
}

static const char *sig_variadic_marker(CBMLanguage lang) {
    return lang == CBM_LANG_JAVA ? "[]" : "~";
}

/* ── Type spelling ─────────────────────────────────────────────── */

static bool sig_skipped_kind(const char *kind) {
    return strstr(kind, "comment") != NULL || strcmp(kind, "annotation") == 0 ||
           strcmp(kind, "marker_annotation") == 0 || strcmp(kind, "attribute_list") == 0 ||
           strcmp(kind, "attribute") == 0 || strcmp(kind, "attribute_declaration") == 0 ||
           strcmp(kind, "attribute_specifier") == 0;
}

/* Whether a leaf is emitted: C-family nested parameter lists spell their
 * declarators without the parameter names. */
static void sig_leaf(sig_ctx_t *c, TSNode n) {
    const char *kind = ts_node_type(n);
    if (c->skip_names &&
        (strcmp(kind, "identifier") == 0 || strcmp(kind, "field_identifier") == 0)) {
        return;
    }
    uint32_t s = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    if (e - s == 3 && memcmp(c->src + s, "...", 3) == 0) {
        sig_raw_str(c, sig_variadic_marker(c->lang));
        return;
    }
    sig_token(c, c->src + s, e - s);
}

/* Render every leaf token of a type subtree, in order, with a cursor walk
 * bounded at SIG_DEPTH_LIMIT. C# tuple element names are not part of the
 * tuple's type identity and are skipped; Swift tuple labels are, and stay. */
static void sig_type_tokens(sig_ctx_t *c, TSNode root) {
    if (ts_node_is_null(root)) {
        return;
    }
    bool in_tuple_element[SIG_DEPTH_LIMIT + 1] = {false};
    TSTreeCursor cur = ts_tree_cursor_new(root);
    int depth = 0;
    for (;;) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        const char *kind = ts_node_type(n);
        const char *field = depth > 0 ? ts_tree_cursor_current_field_name(&cur) : NULL;
        bool skip = sig_skipped_kind(kind) ||
                    (in_tuple_element[depth] && field && strcmp(field, "name") == 0);
        if (!skip && ts_node_child_count(n) == 0) {
            sig_leaf(c, n);
        } else if (!skip && depth < SIG_DEPTH_LIMIT && ts_tree_cursor_goto_first_child(&cur)) {
            depth++;
            in_tuple_element[depth] = strcmp(kind, "tuple_element") == 0;
            continue;
        }
        while (depth > 0 && !ts_tree_cursor_goto_next_sibling(&cur)) {
            ts_tree_cursor_goto_parent(&cur);
            depth--;
        }
        if (depth == 0) {
            break;
        }
    }
    ts_tree_cursor_delete(&cur);
}

/* ── Entries ───────────────────────────────────────────────────── */

static bool sig_writes_types(const sig_ctx_t *c) {
    return c->mode == CBM_CALLABLE_ID_TYPED || c->mode == CBM_CALLABLE_ID_LABELED_TYPED;
}

static bool sig_writes_labels(const sig_ctx_t *c) {
    return c->mode == CBM_CALLABLE_ID_LABELED_TYPED || c->mode == CBM_CALLABLE_ID_LABELED;
}

static void sig_entry_open(sig_ctx_t *c) {
    if (c->entries > 0 && sig_writes_types(c)) {
        sig_raw(c, ",", 1);
    }
}

static void sig_entry_close(sig_ctx_t *c) {
    if (c->entries < SIG_MAX_ENTRIES) {
        c->entry_end[c->entries] = c->len;
    } else {
        c->too_many = true;
    }
    c->entries++;
}

/* label (NULL = none -> "_") followed by ':' in the labeled modes. */
static void sig_label(sig_ctx_t *c, const char *label, size_t n) {
    if (!sig_writes_labels(c)) {
        return;
    }
    if (label && n > 0) {
        sig_raw(c, label, n);
    } else {
        sig_raw(c, "_", 1);
    }
    sig_raw(c, ":", 1);
}

static void sig_label_node(sig_ctx_t *c, TSNode n) {
    if (ts_node_is_null(n)) {
        sig_label(c, NULL, 0);
        return;
    }
    uint32_t s = ts_node_start_byte(n);
    sig_label(c, c->src + s, ts_node_end_byte(n) - s);
}

/* An entry holding one plain type node (NULL node -> "?"). */
static void sig_entry_type(sig_ctx_t *c, TSNode type, const char *suffix) {
    sig_entry_open(c);
    sig_label(c, NULL, 0);
    if (sig_writes_types(c)) {
        size_t before = c->len;
        sig_type_tokens(c, type);
        if (c->len == before) {
            sig_raw(c, "?", 1);
        }
        if (suffix) {
            sig_raw_str(c, suffix);
        }
    }
    sig_entry_close(c);
}

static TSNode sig_field(TSNode n, const char *field) {
    return ts_node_child_by_field_name(n, field, (uint32_t)strlen(field));
}

static bool sig_node_text_is(const sig_ctx_t *c, TSNode n, const char *text) {
    uint32_t s = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    size_t len = strlen(text);
    return e - s == len && memcmp(c->src + s, text, len) == 0;
}

/* ── Java ──────────────────────────────────────────────────────── */

static void sig_java_params(sig_ctx_t *c, TSNode node) {
    TSNode params = sig_field(node, "parameters");
    uint32_t count = ts_node_is_null(params) ? 0 : ts_node_named_child_count(params);
    for (uint32_t i = 0; i < count; i++) {
        TSNode p = ts_node_named_child(params, i);
        const char *kind = ts_node_type(p);
        if (strcmp(kind, "formal_parameter") == 0) {
            sig_entry_open(c);
            sig_label(c, NULL, 0);
            if (sig_writes_types(c)) {
                size_t before = c->len;
                sig_type_tokens(c, sig_field(p, "type"));
                if (c->len == before) {
                    sig_raw(c, "?", 1);
                }
                sig_type_tokens(c, sig_field(p, "dimensions")); /* C-style int a[] */
            }
            sig_entry_close(c);
        } else if (strcmp(kind, "spread_parameter") == 0) {
            TSNode type = {0};
            uint32_t pc = ts_node_named_child_count(p);
            for (uint32_t j = 0; j < pc; j++) {
                TSNode ch = ts_node_named_child(p, j);
                const char *ck = ts_node_type(ch);
                if (strcmp(ck, "modifiers") != 0 && strcmp(ck, "variable_declarator") != 0 &&
                    !sig_skipped_kind(ck)) {
                    type = ch;
                    break;
                }
            }
            sig_entry_type(c, type, sig_variadic_marker(c->lang));
        }
        /* receiver_parameter (`Foo this`) is not part of the signature. */
    }
}

/* ── C# ────────────────────────────────────────────────────────── */

static void sig_cs_param(sig_ctx_t *c, TSNode p) {
    sig_entry_open(c);
    sig_label(c, NULL, 0);
    if (sig_writes_types(c)) {
        uint32_t pc = ts_node_named_child_count(p);
        for (uint32_t j = 0; j < pc; j++) {
            TSNode m = ts_node_named_child(p, j);
            if (strcmp(ts_node_type(m), "modifier") == 0 &&
                (sig_node_text_is(c, m, "ref") || sig_node_text_is(c, m, "out") ||
                 sig_node_text_is(c, m, "in") || sig_node_text_is(c, m, "readonly"))) {
                sig_type_tokens(c, m);
            }
        }
        size_t before = c->len;
        sig_type_tokens(c, sig_field(p, "type"));
        if (c->len == before) {
            sig_raw(c, "?", 1);
        }
    }
    sig_entry_close(c);
}

static void sig_cs_params(sig_ctx_t *c, TSNode node) {
    TSNode params = sig_field(node, "parameters");
    uint32_t count = ts_node_is_null(params) ? 0 : ts_node_child_count(params);
    for (uint32_t i = 0; i < count; i++) {
        TSNode p = ts_node_child(params, i);
        const char *field = ts_node_field_name_for_child(params, i);
        if (strcmp(ts_node_type(p), "parameter") == 0) {
            sig_cs_param(c, p);
        } else if (field && strcmp(field, "type") == 0) {
            /* `params T[] rest`: the grammar inlines the parameter's type and
             * name into the list; `params` itself does not change identity. */
            sig_entry_type(c, p, NULL);
        }
    }
}

static void sig_cs_tparams(sig_ctx_t *c, TSNode node) {
    TSNode tp = sig_field(node, "type_parameters");
    uint32_t count = ts_node_is_null(tp) ? 0 : ts_node_named_child_count(tp);
    int written = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode p = ts_node_named_child(tp, i);
        if (strcmp(ts_node_type(p), "type_parameter") != 0) {
            continue;
        }
        sig_raw(c, written ? "," : "<", 1);
        sig_type_tokens(c, sig_field(p, "name"));
        written++;
    }
    if (written) {
        sig_raw(c, ">", 1);
    }
}

/* ── C / C++ ───────────────────────────────────────────────────── */

static TSNode sig_c_inner_declarator(TSNode d) {
    TSNode next = sig_field(d, "declarator");
    if (!ts_node_is_null(next)) {
        return next;
    }
    uint32_t count = ts_node_named_child_count(d);
    for (uint32_t i = 0; i < count; i++) {
        TSNode ch = ts_node_named_child(d, i);
        if (strstr(ts_node_type(ch), "declarator") != NULL) {
            return ch;
        }
    }
    return (TSNode){0};
}

static bool sig_is_c_decl_kind(const char *k) {
    return strcmp(k, "function_definition") == 0 || strcmp(k, "declaration") == 0 ||
           strcmp(k, "field_declaration") == 0 || strcmp(k, "template_declaration") == 0;
}

/* The function_declarator of a C-family definition/declaration, looking
 * through (nested) template_declarations and pointer/reference return
 * declarators. */
static TSNode sig_c_function_declarator(TSNode node) {
    TSNode cur = node;
    for (int depth = 0; depth < SIG_DECL_DEPTH && !ts_node_is_null(cur) &&
                        strcmp(ts_node_type(cur), "template_declaration") == 0;
         depth++) {
        TSNode inner = {0};
        uint32_t count = ts_node_named_child_count(cur);
        for (uint32_t i = 0; i < count && ts_node_is_null(inner); i++) {
            TSNode ch = ts_node_named_child(cur, i);
            if (sig_is_c_decl_kind(ts_node_type(ch))) {
                inner = ch;
            }
        }
        cur = inner;
    }
    for (int depth = 0; depth < SIG_DECL_DEPTH && !ts_node_is_null(cur); depth++) {
        if (strcmp(ts_node_type(cur), "function_declarator") == 0) {
            return cur;
        }
        cur = sig_c_inner_declarator(cur);
    }
    return (TSNode){0};
}

static void sig_c_qualifiers(sig_ctx_t *c, TSNode n) {
    uint32_t count = ts_node_named_child_count(n);
    for (uint32_t i = 0; i < count; i++) {
        TSNode q = ts_node_named_child(n, i);
        if (strcmp(ts_node_type(q), "type_qualifier") == 0) {
            sig_type_tokens(c, q);
        }
    }
}

static bool sig_c_has_qualifier(TSNode n) {
    uint32_t count = ts_node_named_child_count(n);
    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(ts_node_type(ts_node_named_child(n, i)), "type_qualifier") == 0) {
            return true;
        }
    }
    return false;
}

static bool sig_c_is_name(TSNode d) {
    if (ts_node_is_null(d)) {
        return true;
    }
    const char *k = ts_node_type(d);
    return strstr(k, "declarator") == NULL;
}

static bool sig_c_is_param_kind(const char *k) {
    return strcmp(k, "parameter_declaration") == 0 ||
           strcmp(k, "optional_parameter_declaration") == 0 ||
           strcmp(k, "variadic_parameter_declaration") == 0;
}

static bool sig_c_is_void_list(const sig_ctx_t *c, TSNode params) {
    if (ts_node_named_child_count(params) != 1) {
        return false;
    }
    TSNode p = ts_node_named_child(params, 0);
    TSNode type = sig_field(p, "type");
    return strcmp(ts_node_type(p), "parameter_declaration") == 0 && !ts_node_is_null(type) &&
           sig_node_text_is(c, type, "void") && ts_node_is_null(sig_field(p, "declarator")) &&
           !sig_c_has_qualifier(p);
}

/* The parameter list of a function-typed parameter (`int (*cb)(int x)`),
 * spelled from its tokens without parameter names or default values. */
static void sig_c_nested_params(sig_ctx_t *c, TSNode fdecl) {
    TSNode params = sig_field(fdecl, "parameters");
    sig_raw(c, "(", 1);
    uint32_t count = ts_node_is_null(params) ? 0 : ts_node_child_count(params);
    bool void_list = !ts_node_is_null(params) && sig_c_is_void_list(c, params);
    int written = 0;
    bool saved = c->skip_names;
    c->skip_names = true;
    for (uint32_t i = 0; i < count && !void_list; i++) {
        TSNode p = ts_node_child(params, i);
        bool is_ellipsis = !ts_node_is_named(p) && sig_node_text_is(c, p, "...");
        if (!is_ellipsis && !sig_c_is_param_kind(ts_node_type(p))) {
            continue;
        }
        if (written++) {
            sig_raw(c, ",", 1);
        }
        if (is_ellipsis) {
            sig_raw_str(c, sig_variadic_marker(c->lang));
            continue;
        }
        sig_c_qualifiers(c, p);
        sig_type_tokens(c, sig_field(p, "type"));
        sig_type_tokens(c, sig_field(p, "declarator"));
    }
    c->skip_names = saved;
    sig_raw(c, ")", 1);
}

/* Declarator markers, outermost first: `*` (with its qualifiers unless it is
 * the top-level one, which the language drops), `&`/`&&`, an array that
 * decays to `*` at the top level, a pack/variadic as the variadic marker.
 * A function declarator opens "(" and is closed — with its parameter list —
 * after everything inside it, innermost first; one with nothing inside is a
 * function parameter decaying to a pointer. Returns whether any marker was
 * written, so the caller knows whether base cv-qualifiers are top-level. */
static bool sig_c_markers(sig_ctx_t *c, TSNode d) {
    TSNode pending[SIG_DECL_DEPTH];
    size_t pending_at[SIG_DECL_DEPTH];
    int npending = 0;
    bool wrote = false;
    for (int depth = 0; !sig_c_is_name(d) && depth < SIG_DECL_DEPTH; depth++) {
        const char *k = ts_node_type(d);
        TSNode inner = sig_c_inner_declarator(d);
        if (strstr(k, "pointer_declarator") != NULL) {
            sig_raw(c, "*", 1);
            if (!sig_c_is_name(inner)) {
                sig_c_qualifiers(c, d); /* not top-level: qualifiers are identity */
            }
        } else if (strstr(k, "reference_declarator") != NULL) {
            TSNode op = ts_node_child(d, 0);
            bool rvalue =
                !ts_node_is_null(op) && !ts_node_is_named(op) && sig_node_text_is(c, op, "&&");
            sig_raw_str(c, rvalue ? "&&" : "&");
        } else if (strstr(k, "array_declarator") != NULL) {
            sig_raw_str(c, sig_c_is_name(inner) ? "*" : "[]");
        } else if (strstr(k, "function_declarator") != NULL) {
            sig_raw(c, "(", 1);
            pending[npending] = d;
            pending_at[npending] = c->len;
            npending++;
        } else if (strcmp(k, "variadic_declarator") == 0) {
            sig_raw_str(c, sig_variadic_marker(c->lang));
        } else {
            d = inner; /* parenthesized_declarator and friends add nothing */
            continue;
        }
        wrote = true;
        d = inner;
    }
    while (npending > 0) {
        npending--;
        if (c->len == pending_at[npending]) {
            sig_raw(c, "*", 1); /* a function parameter decays to a pointer */
        }
        sig_raw(c, ")", 1);
        sig_c_nested_params(c, pending[npending]);
    }
    return wrote;
}

/* One C-family parameter's type: [non-top-level cv] base-type markers. */
static void sig_c_param_type(sig_ctx_t *c, TSNode p) {
    TSNode declarator = sig_field(p, "declarator");
    /* Probe whether the declarator adds markers, then render in order:
     * top-level base cv (no markers) is dropped like the language does. */
    size_t mark = c->len;
    bool has_markers = sig_c_markers(c, declarator);
    c->len = mark;
    c->buf[c->len] = '\0';
    if (has_markers) {
        sig_c_qualifiers(c, p);
    }
    TSNode type = sig_field(p, "type");
    const char *tk = ts_node_is_null(type) ? "" : ts_node_type(type);
    TSNode tag_name = sig_field(type, "name");
    bool elaborated = (strcmp(tk, "struct_specifier") == 0 || strcmp(tk, "class_specifier") == 0 ||
                       strcmp(tk, "union_specifier") == 0 || strcmp(tk, "enum_specifier") == 0) &&
                      !ts_node_is_null(tag_name) && ts_node_is_null(sig_field(type, "body"));
    size_t before = c->len;
    sig_type_tokens(c, elaborated ? tag_name : type);
    if (c->len == before) {
        sig_raw(c, "?", 1);
    }
    (void)sig_c_markers(c, declarator);
}

/* "(" entries ")" of the callable's own function_declarator. */
static void sig_c_param_list(sig_ctx_t *c, TSNode fdecl) {
    TSNode params = sig_field(fdecl, "parameters");
    c->open_off = c->len;
    sig_raw(c, "(", 1);
    uint32_t count = ts_node_is_null(params) ? 0 : ts_node_child_count(params);
    bool void_list = !ts_node_is_null(params) && sig_c_is_void_list(c, params);
    for (uint32_t i = 0; i < count && !void_list; i++) {
        TSNode p = ts_node_child(params, i);
        bool is_ellipsis = !ts_node_is_named(p) && sig_node_text_is(c, p, "...");
        if (!is_ellipsis && !sig_c_is_param_kind(ts_node_type(p))) {
            continue;
        }
        sig_entry_open(c);
        sig_label(c, NULL, 0);
        if (sig_writes_types(c)) {
            if (is_ellipsis) {
                sig_raw_str(c, sig_variadic_marker(c->lang));
            } else {
                sig_c_param_type(c, p);
            }
        }
        sig_entry_close(c);
    }
    sig_raw(c, ")", 1);
    c->close_off = c->len;
}

static void sig_cpp_tparams(sig_ctx_t *c, TSNode node) {
    TSNode tmpl = node;
    if (strcmp(ts_node_type(tmpl), "template_declaration") != 0) {
        tmpl = ts_node_parent(node);
        if (ts_node_is_null(tmpl) || strcmp(ts_node_type(tmpl), "template_declaration") != 0) {
            return;
        }
    }
    TSNode list = sig_field(tmpl, "parameters");
    uint32_t count = ts_node_is_null(list) ? 0 : ts_node_named_child_count(list);
    sig_raw(c, "<", 1);
    int written = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode p = ts_node_named_child(list, i);
        const char *k = ts_node_type(p);
        if (sig_skipped_kind(k)) {
            continue;
        }
        if (written++) {
            sig_raw(c, ",", 1);
        }
        /* Template identity depends on parameter kinds, not their names. */
        if (strcmp(k, "type_parameter_declaration") == 0 ||
            strcmp(k, "optional_type_parameter_declaration") == 0) {
            sig_raw_str(c, "typename");
        } else if (strcmp(k, "variadic_type_parameter_declaration") == 0) {
            sig_raw_str(c, "typename");
            sig_raw_str(c, sig_variadic_marker(c->lang));
        } else if (strcmp(k, "template_template_parameter_declaration") == 0) {
            sig_raw_str(c, "template");
        } else {
            sig_c_param_type(c, p);
        }
    }
    sig_raw(c, ">", 1);
}

/* `template <class T> void S<T>::f()`: the template parameters belong to the
 * class, and the in-class declaration `void f();` carries none — both must
 * spell the same identity. */
static bool sig_cpp_member_of_class_template(TSNode fdecl) {
    TSNode name = ts_node_is_null(fdecl) ? fdecl : sig_field(fdecl, "declarator");
    for (int depth = 0; depth < SIG_DECL_DEPTH && !ts_node_is_null(name) &&
                        strcmp(ts_node_type(name), "qualified_identifier") == 0;
         depth++) {
        TSNode scope = sig_field(name, "scope");
        if (!ts_node_is_null(scope) && strcmp(ts_node_type(scope), "template_type") == 0) {
            return true;
        }
        name = sig_field(name, "name");
    }
    return false;
}

static void sig_cpp_cvref(sig_ctx_t *c, TSNode fdecl) {
    uint32_t count = ts_node_named_child_count(fdecl);
    for (uint32_t i = 0; i < count; i++) {
        TSNode ch = ts_node_named_child(fdecl, i);
        const char *k = ts_node_type(ch);
        if (strcmp(k, "type_qualifier") == 0 || strcmp(k, "ref_qualifier") == 0) {
            sig_type_tokens(c, ch);
        }
    }
}

/* ── Kotlin ────────────────────────────────────────────────────── */

static bool sig_kotlin_is_vararg(const sig_ctx_t *c, TSNode mods) {
    uint32_t count = ts_node_named_child_count(mods);
    for (uint32_t i = 0; i < count; i++) {
        TSNode m = ts_node_named_child(mods, i);
        if (strcmp(ts_node_type(m), "parameter_modifier") == 0 &&
            sig_node_text_is(c, m, "vararg")) {
            return true;
        }
    }
    return false;
}

static void sig_kotlin_param(sig_ctx_t *c, TSNode p, bool vararg) {
    sig_entry_open(c);
    sig_label(c, NULL, 0);
    if (sig_writes_types(c)) {
        size_t before = c->len;
        uint32_t count = ts_node_named_child_count(p);
        for (uint32_t i = 0; i < count; i++) {
            TSNode ch = ts_node_named_child(p, i);
            if (strcmp(ts_node_type(ch), "simple_identifier") != 0) {
                sig_type_tokens(c, ch); /* type_modifiers + the type */
            }
        }
        if (c->len == before) {
            sig_raw(c, "?", 1);
        }
        if (vararg) {
            sig_raw_str(c, sig_variadic_marker(c->lang));
        }
    }
    sig_entry_close(c);
}

static void sig_kotlin_params(sig_ctx_t *c, TSNode node) {
    TSNode receiver = sig_field(node, "receiver");
    if (!ts_node_is_null(receiver)) {
        /* An extension's receiver is part of its identity: String.f(Int) and
         * Int.f(Int) share a base QN. */
        sig_entry_open(c);
        sig_label(c, NULL, 0);
        if (sig_writes_types(c)) {
            sig_raw_str(c, "this:");
            sig_type_tokens(c, receiver);
        }
        sig_entry_close(c);
    }
    TSNode params = cbm_find_child_by_kind(node, "function_value_parameters");
    uint32_t count = ts_node_is_null(params) ? 0 : ts_node_named_child_count(params);
    bool vararg = false;
    for (uint32_t i = 0; i < count; i++) {
        TSNode ch = ts_node_named_child(params, i);
        const char *k = ts_node_type(ch);
        if (strcmp(k, "parameter_modifiers") == 0) {
            vararg = vararg || sig_kotlin_is_vararg(c, ch);
        } else if (strcmp(k, "parameter") == 0) {
            sig_kotlin_param(c, ch, vararg);
            vararg = false;
        }
    }
}

/* ── Swift ─────────────────────────────────────────────────────── */

static void sig_swift_param(sig_ctx_t *c, TSNode p) {
    TSNode external = sig_field(p, "external_name");
    TSNode internal = {0};
    uint32_t count = ts_node_child_count(p);
    for (uint32_t i = 0; i < count; i++) {
        TSNode ch = ts_node_child(p, i);
        const char *field = ts_node_field_name_for_child(p, i);
        if (field && strcmp(field, "name") == 0 &&
            strcmp(ts_node_type(ch), "simple_identifier") == 0) {
            internal = ch;
            break;
        }
    }
    sig_entry_open(c);
    /* One name is both label and internal name; `_` means unlabeled. */
    sig_label_node(c, ts_node_is_null(external) ? internal : external);
    if (sig_writes_types(c)) {
        size_t before = c->len;
        for (uint32_t i = 0; i < count; i++) {
            TSNode ch = ts_node_child(p, i);
            const char *field = ts_node_field_name_for_child(p, i);
            if (field && strcmp(field, "external_name") == 0) {
                continue;
            }
            if (ts_node_eq(ch, internal)) {
                continue;
            }
            const char *k = ts_node_type(ch);
            if (strcmp(k, "parameter_modifiers") == 0) {
                uint32_t mc = ts_node_named_child_count(ch);
                for (uint32_t j = 0; j < mc; j++) {
                    TSNode m = ts_node_named_child(ch, j);
                    uint32_t ms = ts_node_start_byte(m);
                    if (ts_node_end_byte(m) > ms && c->src[ms] != '@') {
                        sig_type_tokens(c, m); /* inout; @escaping etc. dropped */
                    }
                }
            } else if (ts_node_is_named(ch)) {
                sig_type_tokens(c, ch);
            } else if (sig_node_text_is(c, ch, "...")) {
                sig_raw_str(c, sig_variadic_marker(c->lang));
            }
        }
        if (c->len == before) {
            sig_raw(c, "?", 1);
        }
    }
    sig_entry_close(c);
}

static void sig_swift_params(sig_ctx_t *c, TSNode node) {
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode ch = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(ch), "parameter") == 0) {
            sig_swift_param(c, ch);
        }
    }
}

/* ── Scala ─────────────────────────────────────────────────────── */

static void sig_scala_params(sig_ctx_t *c, TSNode node) {
    /* Every parameter clause, flattened: all of them are erased into the
     * method's JVM signature. */
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode clause = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(clause), "parameters") != 0) {
            continue;
        }
        uint32_t pc = ts_node_named_child_count(clause);
        for (uint32_t j = 0; j < pc; j++) {
            TSNode p = ts_node_named_child(clause, j);
            if (strcmp(ts_node_type(p), "parameter") == 0) {
                sig_entry_type(c, sig_field(p, "type"), NULL);
            }
        }
    }
}

/* ── Objective-C ───────────────────────────────────────────────── */

static void sig_objc_params(sig_ctx_t *c, TSNode node) {
    /* Selector keywords are the method's direct `identifier` children: the
     * first is the name, each later one labels the parameter after it. */
    uint32_t count = ts_node_child_count(node);
    bool saw_name = false;
    TSNode pending = {0};
    bool have_pending = false;
    for (uint32_t i = 0; i < count; i++) {
        TSNode ch = ts_node_child(node, i);
        const char *k = ts_node_type(ch);
        if (strcmp(k, "identifier") == 0) {
            if (saw_name) {
                pending = ch;
                have_pending = true;
            }
            saw_name = true;
        } else if (strcmp(k, "method_parameter") == 0) {
            sig_entry_open(c);
            if (have_pending) {
                sig_label_node(c, pending);
            } else {
                sig_label(c, NULL, 0);
            }
            have_pending = false;
            if (sig_writes_types(c)) {
                TSNode mt = cbm_find_child_by_kind(ch, "method_type");
                TSNode tn = ts_node_is_null(mt) ? mt : cbm_find_child_by_kind(mt, "type_name");
                size_t before = c->len;
                sig_type_tokens(c, ts_node_is_null(tn) ? mt : tn);
                if (c->len == before) {
                    sig_raw(c, "?", 1);
                }
            }
            sig_entry_close(c);
        } else if (!ts_node_is_named(ch) && sig_node_text_is(c, ch, "...")) {
            sig_entry_open(c);
            sig_raw_str(c, sig_variadic_marker(c->lang));
            sig_entry_close(c);
        }
    }
}

/* ── Generic ───────────────────────────────────────────────────── */

static void sig_generic_params(sig_ctx_t *c, TSNode node) {
    TSNode params = sig_field(node, "parameters");
    uint32_t count = ts_node_is_null(params) ? 0 : ts_node_named_child_count(params);
    for (uint32_t i = 0; i < count; i++) {
        TSNode p = ts_node_named_child(params, i);
        if (sig_skipped_kind(ts_node_type(p))) {
            continue;
        }
        TSNode type = sig_field(p, "type");
        sig_entry_type(c, type, NULL);
    }
}

/* ── Spelling pass ─────────────────────────────────────────────── */

/* Qualified type paths keep their last segment; "->" becomes "=>"; any other
 * '.' or "::" (a receiver function type, a leading global scope) is dropped,
 * so the suffix contract holds whatever the grammar produced. */
static size_t sig_spell(char *s, size_t n) {
    size_t out = 0;
    size_t i = 0;
    while (i < n) {
        size_t sep = 0;
        if (s[i] == '.') {
            sep = 1;
        } else if (s[i] == ':' && i + 1 < n && s[i + 1] == ':') {
            sep = 2;
        }
        if (sep) {
            bool ident_before = out > 0 && sig_ident_char((unsigned char)s[out - 1]);
            bool ident_after = i + sep < n && sig_ident_char((unsigned char)s[i + sep]);
            if (ident_before && ident_after) {
                while (out > 0 && sig_ident_char((unsigned char)s[out - 1])) {
                    out--;
                }
            }
            i += sep;
            continue;
        }
        if (s[i] == '-' && i + 1 < n && s[i + 1] == '>') {
            s[out++] = '=';
            s[out++] = '>';
            i += 2;
            continue;
        }
        s[out++] = s[i++];
    }
    s[out] = '\0';
    return out;
}

static uint64_t sig_fnv64(const char *s, size_t n) {
    uint64_t h = SIG_FNV_OFFSET;
    for (size_t i = 0; i < n; i++) {
        h ^= (unsigned char)s[i];
        h *= SIG_FNV_PRIME;
    }
    return h;
}

/* "(" leading whole entries ")" ... "#<hash>)" within CBM_CALLABLE_SIG_MAX. */
static const char *sig_capped(CBMArena *a, const sig_ctx_t *c, const char *full, size_t n) {
    char tail[SIG_CAPPED_TAIL + 1];
    snprintf(tail, sizeof(tail), "#%016llx)", (unsigned long long)sig_fnv64(full, n));
    size_t keep = 0; /* bytes of entry text after the "(" */
    bool comma = sig_writes_types(c);
    int limit = c->too_many ? SIG_MAX_ENTRIES : c->entries;
    for (int i = 0; i < limit; i++) {
        size_t end = c->entry_end[i];
        if (end < c->open_off + 1 || end > c->close_off) {
            break;
        }
        size_t candidate = end - (c->open_off + 1);
        if (1 + candidate + (comma ? 1 : 0) + SIG_CAPPED_TAIL > CBM_CALLABLE_SIG_MAX) {
            break;
        }
        keep = candidate;
    }
    char *out = cbm_arena_alloc(a, CBM_CALLABLE_SIG_MAX + 1);
    if (!out) {
        return NULL;
    }
    size_t len = 0;
    out[len++] = '(';
    memcpy(out + len, c->buf + c->open_off + 1, keep);
    len += keep;
    if (keep && comma) {
        out[len++] = ',';
    }
    memcpy(out + len, tail, SIG_CAPPED_TAIL);
    len += SIG_CAPPED_TAIL;
    out[len] = '\0';
    return out;
}

/* ── Builder ───────────────────────────────────────────────────── */

static void sig_render(sig_ctx_t *c, TSNode node) {
    switch (c->lang) {
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
    case CBM_LANG_C: {
        TSNode fdecl = sig_c_function_declarator(node);
        if (c->lang != CBM_LANG_C && !sig_cpp_member_of_class_template(fdecl)) {
            sig_cpp_tparams(c, node);
        }
        if (ts_node_is_null(fdecl)) {
            c->open_off = c->len;
            sig_raw(c, "()", 2);
            c->close_off = c->len;
            return;
        }
        sig_c_param_list(c, fdecl);
        sig_cpp_cvref(c, fdecl);
        return;
    }
    default:
        break;
    }
    if (c->lang == CBM_LANG_CSHARP) {
        sig_cs_tparams(c, node);
    }
    c->open_off = c->len;
    sig_raw(c, "(", 1);
    switch (c->lang) {
    case CBM_LANG_JAVA:
        sig_java_params(c, node);
        break;
    case CBM_LANG_CSHARP:
        sig_cs_params(c, node);
        break;
    case CBM_LANG_KOTLIN:
        sig_kotlin_params(c, node);
        break;
    case CBM_LANG_SWIFT:
        sig_swift_params(c, node);
        break;
    case CBM_LANG_SCALA:
        sig_scala_params(c, node);
        break;
    case CBM_LANG_OBJC:
        sig_objc_params(c, node);
        break;
    default:
        sig_generic_params(c, node);
        break;
    }
    sig_raw(c, ")", 1);
    c->close_off = c->len;
}

static bool sig_roundtrips(const char *suffix, size_t n) {
    /* "f" + suffix must invert to the base "f". */
    char probe[CBM_CALLABLE_SIG_MAX + 2];
    if (n > CBM_CALLABLE_SIG_MAX) {
        return false;
    }
    probe[0] = 'f';
    memcpy(probe + 1, suffix, n);
    probe[n + 1] = '\0';
    return cbm_qn_callable_base_len(probe) == 1;
}

/* "(#<hash>)": the identity of a signature that cannot be spelled safely. */
static const char *sig_hashed(CBMArena *a, const char *full, size_t n) {
    return cbm_arena_sprintf(a, "(#%016llx)", (unsigned long long)sig_fnv64(full, n));
}

const char *cbm_callable_sig_mode(CBMArena *a, TSNode func_node, const char *source,
                                  CBMLanguage lang, CBMCallableIdentity mode) {
    if (!a || !source || ts_node_is_null(func_node) || mode == CBM_CALLABLE_ID_NONE) {
        return NULL;
    }
    sig_ctx_t c;
    memset(&c, 0, sizeof(c));
    c.arena = a;
    c.src = source;
    c.lang = lang;
    c.mode = mode;
    sig_raw(&c, "", 0);
    if (c.overflow || !c.buf) {
        return NULL;
    }
    c.buf[0] = '\0';
    sig_render(&c, func_node);
    if (c.overflow) {
        return NULL;
    }
    if (mode == CBM_CALLABLE_ID_ARITY) {
        char digits[SIG_ARITY_DIGITS];
        snprintf(digits, sizeof(digits), "(%d)", c.entries);
        return cbm_arena_strdup(a, digits);
    }
    size_t full = c.len;
    char *spelled = cbm_arena_strndup(a, c.buf, full);
    if (!spelled) {
        return NULL;
    }
    size_t n = sig_spell(spelled, full);
    if (n <= CBM_CALLABLE_SIG_MAX) {
        return sig_roundtrips(spelled, n) ? spelled : sig_hashed(a, spelled, n);
    }
    /* Over the cap: re-derive the entry offsets in spelled coordinates. The
     * spelling pass only shortens and only looks one byte ahead, and every
     * recorded offset sits before a ',' or ')', so spelling a prefix gives
     * exactly that prefix's spelled length. */
    int limit = c.too_many ? SIG_MAX_ENTRIES : c.entries;
    for (int i = 0; i < limit; i++) {
        char *prefix = cbm_arena_strndup(a, c.buf, c.entry_end[i]);
        c.entry_end[i] = prefix ? sig_spell(prefix, c.entry_end[i]) : 0;
    }
    char *open_prefix = cbm_arena_strndup(a, c.buf, c.open_off);
    char *close_prefix = cbm_arena_strndup(a, c.buf, c.close_off);
    c.open_off = open_prefix ? sig_spell(open_prefix, c.open_off) : 0;
    c.close_off = close_prefix ? sig_spell(close_prefix, c.close_off) : 0;
    c.buf = spelled;
    c.len = n;
    const char *capped = sig_capped(a, &c, spelled, n);
    if (capped && sig_roundtrips(capped, strlen(capped))) {
        return capped;
    }
    return sig_hashed(a, spelled, n);
}

const char *cbm_callable_sig(CBMArena *a, TSNode func_node, const char *source, CBMLanguage lang) {
    return cbm_callable_sig_mode(a, func_node, source, lang, cbm_callable_identity(lang));
}

/* ── Inverse ───────────────────────────────────────────────────── */

static bool sig_ends_with_operator(const char *qn, size_t end) {
    if (end < SIG_OPERATOR_LEN ||
        memcmp(qn + end - SIG_OPERATOR_LEN, "operator", SIG_OPERATOR_LEN) != 0) {
        return false;
    }
    size_t at = end - SIG_OPERATOR_LEN;
    return at == 0 || !sig_ident_char((unsigned char)qn[at - 1]);
}

/* Whether qn[0..end) ends in a leaf a suffix may follow: an identifier (but
 * not a make `$(VAR)` reference), the '>' closing template parameters, a C++
 * `operator...` leaf, or operator symbols — alone (Swift/Scala `+`, `==`) or
 * after a Scala `_` (`unary_!`). A path such as `app/(auth)`, an anonymous
 * `(anon)` leaf or an empty leaf is never a callable followed by a suffix. */
static bool sig_callable_leaf_before(const char *qn, size_t end) {
    size_t start = end;
    while (start > 0 && qn[start - 1] != '.') {
        start--;
    }
    if (start == end) {
        return false;
    }
    unsigned char last = (unsigned char)qn[end - 1];
    if (last == '>' || (sig_ident_char(last) && last != '$')) {
        return true;
    }
    if (end - start >= SIG_OPERATOR_LEN && memcmp(qn + start, "operator", SIG_OPERATOR_LEN) == 0) {
        return true;
    }
    size_t op = end;
    while (op > start && strchr(SIG_OPERATOR_CHARS, qn[op - 1]) != NULL) {
        op--;
    }
    return op < end && (op == start || qn[op - 1] == '_');
}

size_t cbm_qn_callable_base_len_named(const char *qn, const char *name) {
    size_t base = cbm_qn_callable_base_len(qn);
    if (!qn || !name) {
        return base;
    }
    size_t len = strlen(qn);
    size_t n = strlen(name);
    if (base == len || n == 0 || base < n || memcmp(qn + base - n, name, n) != 0) {
        return len;
    }
    size_t at = base - n;
    bool anchored =
        at == 0 || qn[at - 1] == '.' || (at >= 2 && qn[at - 1] == ':' && qn[at - 2] == ':');
    return anchored ? base : len;
}

size_t cbm_qn_callable_base_len(const char *qn) {
    if (!qn) {
        return 0;
    }
    size_t len = strlen(qn);
    /* Trailing cvref (C++): const / volatile / & / && after the ')'. */
    size_t t = len;
    for (;;) {
        if (t > 0 && qn[t - 1] == '&') {
            t--;
        } else if (t >= SIG_CONST_LEN &&
                   memcmp(qn + t - SIG_CONST_LEN, "const", SIG_CONST_LEN) == 0) {
            t -= SIG_CONST_LEN;
        } else if (t >= SIG_VOLATILE_LEN &&
                   memcmp(qn + t - SIG_VOLATILE_LEN, "volatile", SIG_VOLATILE_LEN) == 0) {
            t -= SIG_VOLATILE_LEN;
        } else {
            break;
        }
    }
    if (t == 0 || qn[t - 1] != ')') {
        return len;
    }
    /* The balanced "(...)" that ends at t. */
    int depth = 0;
    size_t p = t;
    bool found = false;
    while (p > 0) {
        p--;
        if (qn[p] == ')') {
            depth++;
        } else if (qn[p] == '(') {
            depth--;
            if (depth == 0) {
                found = true;
                break;
            }
        }
    }
    if (!found || !sig_callable_leaf_before(qn, p)) {
        return len;
    }
    /* A bare C++ call-operator leaf `...operator()` is a name, not a suffix. */
    if (t - p == 2 && sig_ends_with_operator(qn, p)) {
        return len;
    }
    /* Optional "<tparams>" right before the "(". */
    if (qn[p - 1] == '>') {
        int adepth = 0;
        size_t q = p;
        while (q > 0) {
            q--;
            if (qn[q] == '>') {
                adepth++;
            } else if (qn[q] == '<') {
                adepth--;
                if (adepth == 0) {
                    break;
                }
            } else if (qn[q] == '.') {
                q = 0;
                adepth = 1;
                break;
            }
        }
        if (adepth == 0 && q > 0 && qn[q - 1] != '.' && !sig_ends_with_operator(qn, q)) {
            return q;
        }
    }
    return p;
}
