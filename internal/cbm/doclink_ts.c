/*
 * doclink_ts.c — TypeScript and JavaScript doc-comment references (JSDoc,
 * TSDoc) and the TS/JS doc-link scope.
 *
 * Tokens. Only a block comment that opens with a slash and two stars is a
 * doc. Its lines lose their leading white space, one `*` and one space. A
 * reference is
 *   - {@link Ref}, {@link Ref label}, {@link Ref | label}: family `link`;
 *   - {@linkcode Ref ...} the same way: family `linkcode`;
 *   - `@see Ref` (or `@see {Ref}`) at the start of a tag line that holds no
 *     inline link tag: family `see`.
 * The reference is the text up to the first `|` or white space; {@linkplain}
 * is not harvested. A URL is the core's href family. Text in a code span
 * (backticks on one line) or in a fenced code block holds no reference, and a
 * hyphenated word after @see (aria-colcount) is prose. A reference that names
 * a parameter of the documented declaration is the declaration's own: no
 * token.
 *
 * Scope blob, one record per line after the tag line "ts1" (tab-separated):
 *   M                    the file has module syntax (an import or an export)
 *   I local kind spec orig  an import binding: kind d (default), n (named,
 *                        `orig` is the name the module exports), s (a
 *                        namespace: `* as local`, `local = require(spec)`)
 *   A spec               export * from spec
 *   S name spec          export * as name from spec
 *   F name spec orig     export { orig as name } from spec
 *   L name local         export { local as name } (and `export default x;`:
 *                        name "default")
 *   D name flags         a top-level declaration: e exported, d the default
 *                        export (flags "-" for none)
 *   N path first last    a namespace body, path dotted from the file's top
 *                        level, with its lines; the persisted blob keeps
 *                        "N path" (cbm_doclink_ts_portable_scope)
 *   V name line first last  a function or class declared inside a function
 *                        body: its line and the body's lines (where it is
 *                        visible); only the file's own references read it, so
 *                        the persisted blob drops it
 */
#include "doclink.h"

#include "arena.h"
#include "helpers.h"
#include "foundation/constants.h"
#include "foundation/mem_core.h"
#include "tree_sitter/api.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { TS_PREFIX_LEN = 3, TS_FENCE_LEN = 3, TS_WALK_MAX_DEPTH = 64 };

/* ── Tokens ──────────────────────────────────────────────────────── */

typedef struct {
    CBMExtractCtx *ctx;
    const CBMDefinition *def;
} ts_emit_t;

static bool ts_ident_char(char c) {
    return isalnum((unsigned char)c) || c == '_' || c == '$' || (unsigned char)c >= 0x80;
}

/* A parameter named in the signature text "(a: T, ...rest, public b = 1)":
 * after a top-level `(` or `,`, past modifiers and `...`, the identifier. */
static bool ts_param_in_signature(const char *sig, const char *ref, size_t n) {
    static const char *const mods[] = {"public ",   "private ",  "protected ",
                                       "readonly ", "override ", NULL};
    int depth = 0;
    for (const char *p = sig; *p; p++) {
        if (*p == '(' || *p == '[' || *p == '{' || *p == '<') {
            depth++;
        } else if (*p == ')' || *p == ']' || *p == '}' || *p == '>') {
            depth--;
        }
        bool start = (*p == '(' && depth == SKIP_ONE) || (*p == ',' && depth == SKIP_ONE);
        if (!start) {
            continue;
        }
        const char *q = p + SKIP_ONE;
        for (bool again = true; again;) {
            while (*q == ' ' || *q == '\t' || *q == '\n') {
                q++;
            }
            again = false;
            for (int m = 0; mods[m]; m++) {
                size_t ml = strlen(mods[m]);
                if (strncmp(q, mods[m], ml) == 0) {
                    q += ml;
                    again = true;
                }
            }
        }
        while (*q == '.') {
            q++;
        }
        size_t k = 0;
        while (q[k] && ts_ident_char(q[k])) {
            k++;
        }
        if (k == n && memcmp(q, ref, n) == 0) {
            return true;
        }
    }
    return false;
}

/* A parameter of the documented declaration. */
static bool ts_is_param(const CBMDefinition *def, const char *ref, size_t n) {
    if (!def->param_names) {
        return def->signature && ts_param_in_signature(def->signature, ref, n);
    }
    for (int i = 0; def->param_names[i]; i++) {
        const char *p = def->param_names[i];
        while (*p == '.') {
            p++; /* a rest parameter */
        }
        if (strlen(p) == n && memcmp(p, ref, n) == 0) {
            return true;
        }
    }
    return false;
}

static bool ts_is_url(const char *t, size_t n) {
    size_t i = 0;
    while (i < n && (isalnum((unsigned char)t[i]) || t[i] == '+' || t[i] == '.' || t[i] == '-')) {
        i++;
    }
    if (i > 0 && i + CBM_SZ_3 <= n && memcmp(t + i, "://", CBM_SZ_3) == 0) {
        return true;
    }
    return (n > CBM_SZ_4 && strncmp(t, "www.", CBM_SZ_4) == 0) ||
           (n > CBM_SZ_7 && strncmp(t, "mailto:", CBM_SZ_7) == 0);
}

static void ts_push(const ts_emit_t *em, const char *ref, size_t n, int syntax, uint32_t line) {
    if (n == 0) {
        return;
    }
    if (n > CBM_DOCLINK_TS_REF_MAX) {
        n = CBM_DOCLINK_TS_REF_MAX + SKIP_ONE; /* clipped: the resolver calls it unparseable */
    }
    if (ts_is_url(ref, n)) {
        syntax = CBM_DOCLINK_HREF;
    } else {
        size_t k = 0;
        while (k < n && ts_ident_char(ref[k])) {
            k++;
        }
        if (k == n && ts_is_param(em->def, ref, n)) {
            return; /* the declaration's own parameter */
        }
    }
    char *copy = cbm_arena_strndup(em->ctx->arena, ref, n);
    if (!copy) {
        return;
    }
    CBMDocLink link = {
        .source_qn = em->def->qualified_name,
        .raw = copy,
        .line = line,
        .def_line = em->def->start_line,
        .syntax = (uint16_t)syntax,
    };
    cbm_doclinks_push(&em->ctx->result->doc_links, em->ctx->arena, link);
}

/* One doc line: its text after the leading `*`, and its source line. */
typedef struct {
    const char *p;
    size_t n;
    uint32_t line;
} ts_line_t;

/* The doc's lines, its opening and closing marks cut off. */
static int ts_doc_lines(CBMArena *a, const char *doc, uint32_t doc_line, ts_line_t **out) {
    size_t dl = strlen(doc);
    if (dl < TS_PREFIX_LEN || strncmp(doc, "/**", TS_PREFIX_LEN) != 0) {
        return 0;
    }
    const char *b = doc + TS_PREFIX_LEN;
    const char *e = doc + dl;
    if (e - b >= PAIR_LEN && e[-SKIP_ONE] == '/' && e[-PAIR_LEN] == '*') {
        e -= PAIR_LEN;
    }
    int cap = SKIP_ONE;
    for (const char *p = b; p < e; p++) {
        cap += *p == '\n';
    }
    ts_line_t *lines = (ts_line_t *)cbm_arena_alloc(a, (size_t)cap * sizeof(ts_line_t));
    if (!lines) {
        return 0;
    }
    int n = 0;
    uint32_t line = doc_line;
    for (const char *p = b; p <= e && n < cap;) {
        const char *le = p;
        while (le < e && *le != '\n') {
            le++;
        }
        const char *s = p;
        while (s < le && (*s == ' ' || *s == '\t')) {
            s++;
        }
        if (s < le && *s == '*') {
            s++;
            if (s < le && *s == ' ') {
                s++;
            }
        }
        const char *t = le;
        while (t > s && (t[-SKIP_ONE] == ' ' || t[-SKIP_ONE] == '\t' || t[-SKIP_ONE] == '\r')) {
            t--;
        }
        lines[n++] = (ts_line_t){s, (size_t)(t - s), line};
        line++;
        if (le >= e) {
            break;
        }
        p = le + SKIP_ONE;
    }
    *out = lines;
    return n;
}

/* Mark the bytes of a line that sit in a code span (backticks paired on the
 * line). */
static void ts_mask_spans(const ts_line_t *ln, bool *mask) {
    size_t i = 0;
    while (i < ln->n) {
        if (ln->p[i] != '`') {
            i++;
            continue;
        }
        size_t run = 0;
        while (i + run < ln->n && ln->p[i + run] == '`') {
            run++;
        }
        size_t j = i + run;
        size_t close = SIZE_MAX;
        while (j < ln->n) {
            size_t r = 0;
            while (j + r < ln->n && ln->p[j + r] == '`') {
                r++;
            }
            if (r == run) {
                close = j;
                break;
            }
            j += r ? r : SKIP_ONE;
        }
        if (close == SIZE_MAX) {
            i += run;
            continue;
        }
        for (size_t k = i; k < close + run; k++) {
            mask[k] = true;
        }
        i = close + run;
    }
}

/* The first position >= j of p[0, n) that is no white space. */
static size_t ts_skip_space(const char *p, size_t j, size_t n) {
    while (j < n && isspace((unsigned char)p[j])) {
        j++;
    }
    return j;
}

/* The reference of an inline tag body: up to `|`, white space or `}`. */
static size_t ts_ref_len(const char *p, size_t n) {
    size_t k = 0;
    while (k < n && p[k] != '|' && p[k] != '}' && !isspace((unsigned char)p[k])) {
        k++;
    }
    return k;
}

/* {@link ...} and {@linkcode ...} of one line; true when the line holds one. */
static bool ts_line_inline(const ts_emit_t *em, const ts_line_t *ln, const bool *mask) {
    bool any = false;
    static const char open[] = "{@link";
    size_t ol = sizeof(open) - SKIP_ONE;
    for (size_t i = 0; i + ol <= ln->n; i++) {
        if (mask[i] || memcmp(ln->p + i, open, ol) != 0) {
            continue;
        }
        size_t j = i + ol;
        int syntax = CBM_DOCLINK_TS_LINK;
        if (j + CBM_SZ_4 <= ln->n && memcmp(ln->p + j, "code", CBM_SZ_4) == 0) {
            syntax = CBM_DOCLINK_TS_LINKCODE;
            j += CBM_SZ_4;
        }
        if (j < ln->n && ln->p[j] != '}' && !isspace((unsigned char)ln->p[j])) {
            continue; /* {@linkplain} or another word */
        }
        any = true;
        j = ts_skip_space(ln->p, j, ln->n);
        ts_push(em, ln->p + j, ts_ref_len(ln->p + j, ln->n - j), syntax, ln->line);
        i = j;
    }
    return any;
}

/* `@see Ref` at the start of a tag line. */
static void ts_line_see(const ts_emit_t *em, const ts_line_t *ln) {
    static const char tag[] = "@see";
    size_t tl = sizeof(tag) - SKIP_ONE;
    if (ln->n < tl || memcmp(ln->p, tag, tl) != 0 ||
        (ln->n > tl && !isspace((unsigned char)ln->p[tl]))) {
        return;
    }
    size_t j = ts_skip_space(ln->p, tl, ln->n);
    const char *r = ln->p + j;
    size_t n = ln->n - j;
    if (n > 0 && r[0] == '{') {
        r++;
        n--;
        size_t k = 0;
        while (k < n && r[k] != '}') {
            k++;
        }
        n = k;
    }
    size_t k = 0;
    while (k < n && !isspace((unsigned char)r[k])) {
        k++;
    }
    while (k > 0 && (r[k - SKIP_ONE] == '.' || r[k - SKIP_ONE] == ',' || r[k - SKIP_ONE] == ';' ||
                     r[k - SKIP_ONE] == ':')) {
        k--; /* sentence punctuation */
    }
    size_t w = 0;
    while (w < k && ts_ident_char(r[w])) {
        w++;
    }
    if (w > 0 && w < k && r[w] == '-') {
        return; /* a hyphenated word: prose */
    }
    ts_push(em, r, k, CBM_DOCLINK_TS_SEE, ln->line);
}

void cbm_doclink_ts_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                              uint32_t doc_line) {
    if (!ctx || !ctx->result || !def || !doc || !def->qualified_name) {
        return;
    }
    CBMArena *a = ctx->scratch ? ctx->scratch : ctx->arena;
    ts_line_t *lines = NULL;
    int n = ts_doc_lines(a, doc, doc_line, &lines);
    ts_emit_t em = {ctx, def};
    bool fence = false;
    for (int i = 0; i < n; i++) {
        const ts_line_t *ln = &lines[i];
        if (ln->n >= TS_FENCE_LEN && memcmp(ln->p, "```", TS_FENCE_LEN) == 0) {
            fence = !fence;
            continue;
        }
        if (fence || ln->n == 0) {
            continue;
        }
        bool *mask = (bool *)cbm_arena_calloc(a, ln->n + SKIP_ONE);
        if (!mask) {
            return;
        }
        ts_mask_spans(ln, mask);
        if (!ts_line_inline(&em, ln, mask)) {
            ts_line_see(&em, ln);
        }
    }
}

/* ── Scope blob ──────────────────────────────────────────────────── */

typedef struct {
    CBMArena *a;
    char *buf;
    size_t len;
    size_t cap;
    bool oom;
} ts_sb_t;

static void ts_sb_put(ts_sb_t *sb, const char *s, size_t n) {
    if (sb->oom) {
        return;
    }
    if (sb->len + n + SKIP_ONE > sb->cap) {
        size_t ncap = sb->cap ? sb->cap * PAIR_LEN : CBM_SZ_256;
        while (ncap < sb->len + n + SKIP_ONE) {
            ncap *= PAIR_LEN;
        }
        char *grown = (char *)cbm_arena_alloc(sb->a, ncap);
        if (!grown) {
            sb->oom = true;
            return;
        }
        if (sb->len) {
            memcpy(grown, sb->buf, sb->len);
        }
        sb->buf = grown;
        sb->cap = ncap;
    }
    memcpy(sb->buf + sb->len, s, n);
    sb->len += n;
    sb->buf[sb->len] = '\0';
}

static void ts_sb_puts(ts_sb_t *sb, const char *s) {
    ts_sb_put(sb, s, strlen(s));
}

/* A field's text with tabs and newlines as spaces, never empty. */
static void ts_sb_field(ts_sb_t *sb, const char *s, size_t n) {
    ts_sb_put(sb, "\t", SKIP_ONE);
    if (n == 0) {
        ts_sb_put(sb, "-", SKIP_ONE);
        return;
    }
    for (size_t i = 0; i < n; i++) {
        char c = (s[i] == '\t' || s[i] == '\n' || s[i] == '\r') ? ' ' : s[i];
        ts_sb_put(sb, &c, SKIP_ONE);
    }
}

typedef struct {
    CBMExtractCtx *ctx;
    ts_sb_t *sb;
    bool module_syntax;
} ts_walk_t;

static bool ts_kind(TSNode n, const char *k) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), k) == 0;
}

static void ts_node_field(ts_walk_t *w, TSNode n) {
    uint32_t b = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    ts_sb_field(w->sb, w->ctx->source + b, e - b);
}

/* A string literal's content (quotes dropped). */
static void ts_string_field(ts_walk_t *w, TSNode s) {
    uint32_t b = ts_node_start_byte(s);
    uint32_t e = ts_node_end_byte(s);
    const char *src = w->ctx->source;
    if (e - b >= PAIR_LEN && (src[b] == '"' || src[b] == '\'' || src[b] == '`')) {
        b++;
        e--;
    }
    ts_sb_field(w->sb, src + b, e - b);
}

static TSNode ts_source_of(TSNode stmt) {
    TSNode s = ts_node_child_by_field_name(stmt, TS_FIELD("source"));
    if (!ts_node_is_null(s)) {
        return s;
    }
    uint32_t nc = ts_node_child_count(stmt);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(stmt, i);
        if (ts_kind(c, "string")) {
            return c;
        }
    }
    return s;
}

static void ts_import_rec(ts_walk_t *w, TSNode local, char kind, TSNode src, TSNode orig) {
    ts_sb_puts(w->sb, "\nI");
    ts_node_field(w, local);
    char k = kind;
    ts_sb_field(w->sb, &k, SKIP_ONE);
    ts_string_field(w, src);
    if (ts_node_is_null(orig)) {
        ts_sb_field(w->sb, "", 0);
    } else {
        ts_node_field(w, orig);
    }
}

static void ts_walk_import(ts_walk_t *w, TSNode stmt) {
    w->module_syntax = true;
    TSNode src = ts_source_of(stmt);
    uint32_t nc = ts_node_child_count(stmt);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(stmt, i);
        if (ts_kind(c, "import_require_clause")) {
            TSNode id = ts_node_named_child(c, 0);
            TSNode s = ts_node_child_by_field_name(c, TS_FIELD("source"));
            if (ts_kind(id, "identifier") && !ts_node_is_null(s)) {
                TSNode none = {0};
                ts_import_rec(w, id, 's', s, none);
            }
            continue;
        }
        if (!ts_kind(c, "import_clause") || ts_node_is_null(src)) {
            continue;
        }
        uint32_t cc = ts_node_child_count(c);
        for (uint32_t k = 0; k < cc; k++) {
            TSNode sub = ts_node_child(c, k);
            TSNode none = {0};
            if (ts_kind(sub, "identifier")) {
                ts_import_rec(w, sub, 'd', src, none);
            } else if (ts_kind(sub, "namespace_import")) {
                TSNode id = ts_node_named_child(sub, ts_node_named_child_count(sub) - SKIP_ONE);
                if (ts_kind(id, "identifier")) {
                    ts_import_rec(w, id, 's', src, none);
                }
            } else if (ts_kind(sub, "named_imports")) {
                TSTreeCursor cur = ts_tree_cursor_new(sub);
                for (bool more = ts_tree_cursor_goto_first_child(&cur); more;
                     more = ts_tree_cursor_goto_next_sibling(&cur)) {
                    TSNode spec = ts_tree_cursor_current_node(&cur);
                    if (!ts_kind(spec, "import_specifier")) {
                        continue;
                    }
                    TSNode name = ts_node_child_by_field_name(spec, TS_FIELD("name"));
                    TSNode alias = ts_node_child_by_field_name(spec, TS_FIELD("alias"));
                    if (!ts_node_is_null(name)) {
                        ts_import_rec(w, ts_node_is_null(alias) ? name : alias, 'n', src, name);
                    }
                }
                ts_tree_cursor_delete(&cur);
            }
        }
    }
}

/* The name a declaration node declares; null when it has none. */
static TSNode ts_decl_name(TSNode d) {
    return ts_node_child_by_field_name(d, TS_FIELD("name"));
}

static bool ts_is_decl(TSNode d) {
    static const char *const kinds[] = {"class_declaration",
                                        "abstract_class_declaration",
                                        "function_declaration",
                                        "generator_function_declaration",
                                        "function_signature",
                                        "interface_declaration",
                                        "type_alias_declaration",
                                        "enum_declaration",
                                        "internal_module",
                                        "module",
                                        "class",
                                        NULL};
    for (int i = 0; kinds[i]; i++) {
        if (ts_kind(d, kinds[i])) {
            return true;
        }
    }
    return false;
}

static void ts_decl_rec(ts_walk_t *w, TSNode name, bool exported, bool dflt) {
    ts_sb_puts(w->sb, "\nD");
    ts_node_field(w, name);
    char flags[CBM_SZ_4];
    size_t f = 0;
    if (exported) {
        flags[f++] = 'e';
    }
    if (dflt) {
        flags[f++] = 'd';
    }
    ts_sb_field(w->sb, flags, f);
}

static void ts_walk_namespaces(ts_walk_t *w, TSNode body, const char *prefix, int depth);

/* One top-level (or namespace-level, when prefix) declaration. */
static void ts_walk_decl(ts_walk_t *w, TSNode d, bool exported, bool dflt, const char *prefix,
                         int depth) {
    if (ts_kind(d, "ambient_declaration")) {
        uint32_t nc = ts_node_named_child_count(d);
        for (uint32_t i = 0; i < nc; i++) {
            ts_walk_decl(w, ts_node_named_child(d, i), exported, dflt, prefix, depth);
        }
        return;
    }
    if (ts_kind(d, "lexical_declaration") || ts_kind(d, "variable_declaration")) {
        if (prefix) {
            return;
        }
        TSTreeCursor cur = ts_tree_cursor_new(d);
        for (bool more = ts_tree_cursor_goto_first_child(&cur); more;
             more = ts_tree_cursor_goto_next_sibling(&cur)) {
            TSNode v = ts_tree_cursor_current_node(&cur);
            TSNode name = ts_kind(v, "variable_declarator") ? ts_decl_name(v) : v;
            if (ts_kind(name, "identifier")) {
                ts_decl_rec(w, name, exported, false);
            }
        }
        ts_tree_cursor_delete(&cur);
        return;
    }
    if (!ts_is_decl(d)) {
        return;
    }
    TSNode name = ts_decl_name(d);
    if (ts_node_is_null(name)) {
        return;
    }
    if (!prefix) {
        ts_decl_rec(w, name, exported, dflt);
    }
    if ((ts_kind(d, "internal_module") || ts_kind(d, "module")) && depth < TS_WALK_MAX_DEPTH) {
        TSNode body = ts_node_child_by_field_name(d, TS_FIELD("body"));
        if (ts_node_is_null(body)) {
            return;
        }
        uint32_t b = ts_node_start_byte(name);
        uint32_t e = ts_node_end_byte(name);
        size_t pl = prefix ? strlen(prefix) : 0;
        char *path = (char *)cbm_arena_alloc(w->sb->a, pl + (e - b) + PAIR_LEN);
        if (!path) {
            w->sb->oom = true;
            return;
        }
        size_t k = 0;
        if (pl) {
            memcpy(path, prefix, pl);
            path[pl] = '.';
            k = pl + SKIP_ONE;
        }
        memcpy(path + k, w->ctx->source + b, e - b);
        path[k + (e - b)] = '\0';
        ts_sb_puts(w->sb, "\nN");
        ts_sb_field(w->sb, path, strlen(path));
        char num[CBM_SZ_32];
        snprintf(num, sizeof(num), "%u", ts_node_start_point(body).row + SKIP_ONE);
        ts_sb_field(w->sb, num, strlen(num));
        snprintf(num, sizeof(num), "%u", ts_node_end_point(body).row + SKIP_ONE);
        ts_sb_field(w->sb, num, strlen(num));
        ts_walk_namespaces(w, body, path, depth + SKIP_ONE);
    }
}

/* The namespaces declared in a namespace body, exported or not. */
static void ts_walk_namespaces(ts_walk_t *w, TSNode body, const char *prefix, int depth) {
    TSTreeCursor cur = ts_tree_cursor_new(body);
    for (bool more = ts_tree_cursor_goto_first_child(&cur); more;
         more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        if (!ts_node_is_named(c)) {
            continue;
        }
        if (ts_kind(c, "export_statement")) {
            TSNode d = ts_node_child_by_field_name(c, TS_FIELD("declaration"));
            if (!ts_node_is_null(d)) {
                ts_walk_decl(w, d, true, false, prefix, depth);
            }
        } else if (ts_kind(c, "expression_statement")) {
            uint32_t ec = ts_node_named_child_count(c);
            for (uint32_t k = 0; k < ec; k++) {
                ts_walk_decl(w, ts_node_named_child(c, k), false, false, prefix, depth);
            }
        } else {
            ts_walk_decl(w, c, false, false, prefix, depth);
        }
    }
    ts_tree_cursor_delete(&cur);
}

static bool ts_has_default(TSNode stmt) {
    uint32_t nc = ts_node_child_count(stmt);
    for (uint32_t i = 0; i < nc; i++) {
        if (ts_kind(ts_node_child(stmt, i), "default")) {
            return true;
        }
    }
    return false;
}

static void ts_walk_export_clause(ts_walk_t *w, TSNode clause, TSNode src) {
    TSTreeCursor cur = ts_tree_cursor_new(clause);
    for (bool more = ts_tree_cursor_goto_first_child(&cur); more;
         more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode spec = ts_tree_cursor_current_node(&cur);
        if (!ts_kind(spec, "export_specifier")) {
            continue;
        }
        TSNode name = ts_node_child_by_field_name(spec, TS_FIELD("name"));
        TSNode alias = ts_node_child_by_field_name(spec, TS_FIELD("alias"));
        if (ts_node_is_null(name)) {
            continue;
        }
        TSNode as = ts_node_is_null(alias) ? name : alias;
        if (ts_node_is_null(src)) {
            ts_sb_puts(w->sb, "\nL");
            ts_node_field(w, as);
            ts_node_field(w, name);
        } else {
            ts_sb_puts(w->sb, "\nF");
            ts_node_field(w, as);
            ts_string_field(w, src);
            ts_node_field(w, name);
        }
    }
    ts_tree_cursor_delete(&cur);
}

static void ts_walk_export(ts_walk_t *w, TSNode stmt) {
    w->module_syntax = true;
    TSNode src = ts_source_of(stmt);
    TSNode decl = ts_node_child_by_field_name(stmt, TS_FIELD("declaration"));
    bool dflt = ts_has_default(stmt);
    if (!ts_node_is_null(decl)) {
        ts_walk_decl(w, decl, true, dflt, NULL, 0);
        return;
    }
    uint32_t nc = ts_node_child_count(stmt);
    bool star = false;
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(stmt, i);
        if (ts_kind(c, "export_clause")) {
            ts_walk_export_clause(w, c, src);
            return;
        }
        if (ts_kind(c, "namespace_export") && !ts_node_is_null(src)) {
            TSNode id = ts_node_named_child(c, 0);
            if (!ts_node_is_null(id)) {
                ts_sb_puts(w->sb, "\nS");
                ts_node_field(w, id);
                ts_string_field(w, src);
            }
            return;
        }
        star = star || ts_kind(c, "*");
    }
    if (star && !ts_node_is_null(src)) {
        ts_sb_puts(w->sb, "\nA");
        ts_string_field(w, src);
        return;
    }
    TSNode value = ts_node_child_by_field_name(stmt, TS_FIELD("value"));
    if (dflt && ts_kind(value, "identifier")) {
        ts_sb_puts(w->sb, "\nL\tdefault");
        ts_node_field(w, value);
    }
}

static bool ts_is_function_like(TSNode n) {
    static const char *const kinds[] = {"function_declaration", "generator_function_declaration",
                                        "function_expression",  "function",
                                        "generator_function",   "arrow_function",
                                        "method_definition",    NULL};
    for (int i = 0; kinds[i]; i++) {
        if (ts_kind(n, kinds[i])) {
            return true;
        }
    }
    return false;
}

typedef struct {
    uint32_t first; /* a function body's lines */
    uint32_t last;
    int depth; /* the function's depth in the tree */
} ts_scope_t;

static void ts_put_nested(ts_walk_t *w, TSNode decl, const ts_scope_t *in) {
    TSNode name = ts_decl_name(decl);
    if (!ts_kind(name, "identifier") && !ts_kind(name, "type_identifier")) {
        return;
    }
    char num[CBM_SZ_32];
    ts_sb_puts(w->sb, "\nV");
    ts_node_field(w, name);
    snprintf(num, sizeof(num), "%u", ts_node_start_point(decl).row + SKIP_ONE);
    ts_sb_field(w->sb, num, strlen(num));
    snprintf(num, sizeof(num), "%u", in->first);
    ts_sb_field(w->sb, num, strlen(num));
    snprintf(num, sizeof(num), "%u", in->last);
    ts_sb_field(w->sb, num, strlen(num));
}

/* V records: every function or class declared inside a function body, with
 * its line and the body's lines (the scope it is visible in). The graph gives
 * such a declaration a file-level qualified name. One cursor walk over the
 * tree; the stack holds the function bodies around the current node. */
static void ts_walk_nested(ts_walk_t *w, TSNode root) {
    int cap = CBM_SZ_64;
    int ns = 0;
    ts_scope_t *scopes = (ts_scope_t *)cbm_arena_alloc(w->sb->a, (size_t)cap * sizeof(ts_scope_t));
    if (!scopes) {
        w->sb->oom = true;
        return;
    }
    TSTreeCursor cur = ts_tree_cursor_new(root);
    int depth = 0;
    for (bool more = true; more && !w->sb->oom;) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        while (ns > 0 && scopes[ns - SKIP_ONE].depth >= depth) {
            ns--; /* left that function */
        }
        if (ts_node_is_named(n)) {
            if (ns > 0 &&
                (ts_kind(n, "function_declaration") ||
                 ts_kind(n, "generator_function_declaration") || ts_kind(n, "class_declaration"))) {
                ts_put_nested(w, n, &scopes[ns - SKIP_ONE]);
            }
            TSNode body = ts_is_function_like(n) ? ts_node_child_by_field_name(n, TS_FIELD("body"))
                                                 : (TSNode){0};
            if (!ts_node_is_null(body)) {
                if (ns == cap) {
                    int ncap = cap * PAIR_LEN;
                    ts_scope_t *g =
                        (ts_scope_t *)cbm_arena_alloc(w->sb->a, (size_t)ncap * sizeof(ts_scope_t));
                    if (!g) {
                        w->sb->oom = true;
                        break;
                    }
                    memcpy(g, scopes, (size_t)ns * sizeof(ts_scope_t));
                    scopes = g;
                    cap = ncap;
                }
                scopes[ns++] = (ts_scope_t){ts_node_start_point(body).row + SKIP_ONE,
                                            ts_node_end_point(body).row + SKIP_ONE, depth};
            }
        }
        if (ts_tree_cursor_goto_first_child(&cur)) {
            depth++;
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cur)) {
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
            depth--;
        }
    }
    ts_tree_cursor_delete(&cur);
}

const char *cbm_doclink_ts_scan_scope(CBMExtractCtx *ctx) {
    if (!ctx || ts_node_is_null(ctx->root)) {
        return NULL;
    }
    ts_sb_t sb = {.a = ctx->arena};
    ts_sb_puts(&sb, CBM_DOCLINK_TS_SCOPE_TAG);
    ts_walk_t w = {.ctx = ctx, .sb = &sb};
    TSTreeCursor cur = ts_tree_cursor_new(ctx->root);
    for (bool more = ts_tree_cursor_goto_first_child(&cur); more;
         more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        if (!ts_node_is_named(c)) {
            continue;
        }
        if (ts_kind(c, "import_statement")) {
            ts_walk_import(&w, c);
        } else if (ts_kind(c, "export_statement")) {
            ts_walk_export(&w, c);
        } else if (ts_kind(c, "expression_statement")) {
            uint32_t ec = ts_node_named_child_count(c);
            for (uint32_t k = 0; k < ec; k++) {
                ts_walk_decl(&w, ts_node_named_child(c, k), false, false, NULL, 0);
            }
        } else {
            ts_walk_decl(&w, c, false, false, NULL, 0);
        }
    }
    ts_tree_cursor_delete(&cur);
    ts_walk_nested(&w, ctx->root);
    if (w.module_syntax) {
        ts_sb_puts(&sb, "\nM");
    }
    return sb.oom ? NULL : sb.buf;
}

/* The blob without lines: "N path first last" -> "N path", V records dropped. */
char *cbm_doclink_ts_portable_scope(const char *scope) {
    size_t n = strlen(scope);
    char *out = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    for (const char *p = scope; *p;) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        size_t keep = len;
        if (len >= PAIR_LEN && p[0] == 'V' && p[SKIP_ONE] == '\t') {
            p = nl ? nl + SKIP_ONE : p + len;
            continue;
        }
        if (len >= PAIR_LEN && p[0] == 'N' && p[SKIP_ONE] == '\t') {
            const char *path_end = memchr(p + PAIR_LEN, '\t', len - PAIR_LEN);
            keep = path_end ? (size_t)(path_end - p) : len;
        }
        memcpy(out + w, p, keep);
        w += keep;
        if (nl) {
            out[w++] = '\n';
        }
        p = nl ? nl + SKIP_ONE : p + len;
    }
    out[w] = '\0';
    return out;
}
