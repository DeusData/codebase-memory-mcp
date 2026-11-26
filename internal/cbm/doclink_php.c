/*
 * doclink_php.c — PHP doc-comment references (PHPDoc) and the PHP doc-link
 * scope.
 *
 * Tokens. Only a block comment that opens with a slash and two stars is a
 * doc. Its lines lose their leading white space, one `*` and one space. A
 * reference is
 *   - `@see Ref` at the start of a tag line, a description may follow;
 *   - `{@see Ref}` or `{@see Ref description}` anywhere on a line;
 * both family `see`. The reference is the text up to white space (or the
 * closing brace), sentence punctuation at its end dropped. A URL is the core's
 * href family. Text in a code span (backticks on one line) or in a fenced
 * code block holds no reference. @link, @uses and the other tags are not
 * harvested.
 *
 * Scope blob, one record per line after the tag line "php1" (tab-separated):
 *   N blk name first last  a namespace block, numbered from 0 in file order,
 *                          its name ("-": the global namespace) and lines; a
 *                          file without a namespace declaration has none, and
 *                          its one block is number 0
 *   U blk kind alias name  a use import of block blk: kind c (a class or a
 *                          namespace), f (a function), k (a constant); the
 *                          alias as written, the name without its leading
 *                          backslash
 *   T kind fqn flags first last  a class (c), interface (i), trait (t) or enum
 *                          (e) declared at the top level of a block, its fully
 *                          qualified name and lines; flags `d` when the file
 *                          declares another type or function of the same name
 *                          (the graph keeps one node for both), else `-`
 *   B rel fqn              a supertype of the T record before it: rel e
 *                          (extends), i (implements), t (a used trait), the
 *                          name qualified as PHP resolves it in this file
 *   K kind name            a member of that type: m method, p property (no
 *                          `$`; promoted constructor parameters too), c
 *                          constant, e enum case
 *   F fqn flags            a function declared at the top level of a block;
 *                          flags as for T
 * "At the top level" includes the branches of a top-level conditional
 * (`if (!function_exists('f')) { function f() {} }`), never a function body.
 * Doc blocks on properties, class constants and enum cases, which have no
 * node, go through cbm_doclink_php_loose_docs: their references belong to
 * the type around them.
 *
 * The persisted blob (cbm_doclink_php_portable_scope) drops the lines of N
 * and T and the U records: other files read names only, and every name a
 * supertype record holds is already qualified.
 */
#include "doclink.h"

#include "arena.h"
#include "helpers.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/mem_core.h"
#include "tree_sitter/api.h"

#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    PHP_PREFIX_LEN = 3,
    PHP_FENCE_LEN = 3,
    PHP_SEE_LEN = 4,
    PHP_INLINE_LEN = 5,
    PHP_COND_DEPTH = 8, /* nested conditionals followed */
};

/* ── Names ───────────────────────────────────────────────────────── */

static bool php_eq_nocase(const char *a, size_t an, const char *b, size_t bn) {
    if (an != bn) {
        return false;
    }
    for (size_t i = 0; i < an; i++) {
        if (tolower((unsigned char)a[i]) != tolower((unsigned char)b[i])) {
            return false;
        }
    }
    return true;
}

/* out[*w..] += s[0, n); false when it does not fit. */
static bool php_put(char *out, size_t cap, size_t *w, const char *s, size_t n) {
    if (*w + n + SKIP_ONE > cap) {
        return false;
    }
    memcpy(out + *w, s, n);
    *w += n;
    out[*w] = '\0';
    return true;
}

size_t cbm_doclink_php_qualify(char *out, size_t cap, const char *ns, size_t ns_len,
                               const cbm_doclink_php_use_t *uses, int nuses, const char *ref,
                               size_t n) {
    size_t w = 0;
    if (cap == 0 || n == 0) {
        return 0;
    }
    out[0] = '\0';
    if (ref[0] == '\\') {
        return php_put(out, cap, &w, ref + SKIP_ONE, n - SKIP_ONE) ? w : 0;
    }
    static const char rel[] = "namespace\\";
    size_t rl = sizeof(rel) - SKIP_ONE;
    if (n > rl && php_eq_nocase(ref, rl, rel, rl)) {
        ref += rl; /* namespace\Foo: the current namespace's Foo */
        n -= rl;
    } else {
        const char *bs = memchr(ref, '\\', n);
        size_t first = bs ? (size_t)(bs - ref) : n;
        for (int i = 0; i < nuses; i++) {
            const cbm_doclink_php_use_t *u = &uses[i];
            if (u->kind == 'c' && php_eq_nocase(u->alias, u->alias_len, ref, first)) {
                bool ok = php_put(out, cap, &w, u->name, u->name_len) &&
                          php_put(out, cap, &w, ref + first, n - first);
                return ok ? w : 0;
            }
        }
    }
    bool ok =
        !ns_len || (php_put(out, cap, &w, ns, ns_len) && php_put(out, cap, &w, "\\", SKIP_ONE));
    return ok && php_put(out, cap, &w, ref, n) ? w : 0;
}

/* ── Tokens ──────────────────────────────────────────────────────── */

typedef struct {
    CBMExtractCtx *ctx;
    const CBMDefinition *def;
} php_emit_t;

static bool php_is_url(const char *t, size_t n) {
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

static void php_push(const php_emit_t *em, const char *ref, size_t n, uint32_t line) {
    while (n > 0 && (ref[n - SKIP_ONE] == '.' || ref[n - SKIP_ONE] == ',' ||
                     ref[n - SKIP_ONE] == ';' || ref[n - SKIP_ONE] == ':')) {
        n--; /* sentence punctuation */
    }
    if (n == 0) {
        return;
    }
    if (n > CBM_DOCLINK_PHP_REF_MAX) {
        n = CBM_DOCLINK_PHP_REF_MAX + SKIP_ONE; /* clipped: the resolver calls it unparseable */
    }
    int syntax = php_is_url(ref, n) ? CBM_DOCLINK_HREF : CBM_DOCLINK_PHP_SEE;
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
} php_line_t;

/* The doc's lines, its opening and closing marks cut off. */
static int php_doc_lines(CBMArena *a, const char *doc, uint32_t doc_line, php_line_t **out) {
    size_t dl = strlen(doc);
    if (dl < PHP_PREFIX_LEN || strncmp(doc, "/**", PHP_PREFIX_LEN) != 0) {
        return 0;
    }
    const char *b = doc + PHP_PREFIX_LEN;
    const char *e = doc + dl;
    if (e - b >= PAIR_LEN && e[-SKIP_ONE] == '/' && e[-PAIR_LEN] == '*') {
        e -= PAIR_LEN;
    }
    int cap = SKIP_ONE;
    for (const char *p = b; p < e; p++) {
        cap += *p == '\n';
    }
    php_line_t *lines = (php_line_t *)cbm_arena_alloc(a, (size_t)cap * sizeof(php_line_t));
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
        lines[n++] = (php_line_t){s, (size_t)(t - s), line};
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
static void php_mask_spans(const php_line_t *ln, bool *mask) {
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
static size_t php_skip_space(const char *p, size_t j, size_t n) {
    while (j < n && isspace((unsigned char)p[j])) {
        j++;
    }
    return j;
}

/* `{@see Ref ...}` anywhere on the line, outside code spans. */
static void php_line_inline(const php_emit_t *em, const php_line_t *ln, const bool *mask) {
    for (size_t i = 0; i + PHP_INLINE_LEN <= ln->n; i++) {
        if (mask[i] || memcmp(ln->p + i, "{@see", PHP_INLINE_LEN) != 0) {
            continue;
        }
        size_t j = i + PHP_INLINE_LEN;
        if (j < ln->n && ln->p[j] != '}' && !isspace((unsigned char)ln->p[j])) {
            continue; /* {@seealso} or another word */
        }
        j = php_skip_space(ln->p, j, ln->n);
        size_t k = j;
        while (k < ln->n && ln->p[k] != '}' && !isspace((unsigned char)ln->p[k])) {
            k++;
        }
        php_push(em, ln->p + j, k - j, ln->line);
        i = k;
    }
}

/* `@see Ref` at the start of a tag line. */
static void php_line_see(const php_emit_t *em, const php_line_t *ln) {
    if (ln->n < PHP_SEE_LEN || memcmp(ln->p, "@see", PHP_SEE_LEN) != 0 ||
        (ln->n > PHP_SEE_LEN && !isspace((unsigned char)ln->p[PHP_SEE_LEN]))) {
        return;
    }
    size_t j = php_skip_space(ln->p, PHP_SEE_LEN, ln->n);
    if (j < ln->n && ln->p[j] == '{') {
        return; /* @see {@see Ref}: the inline tag is the reference */
    }
    size_t k = j;
    while (k < ln->n && !isspace((unsigned char)ln->p[k])) {
        k++;
    }
    php_push(em, ln->p + j, k - j, ln->line);
}

void cbm_doclink_php_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                               uint32_t doc_line) {
    if (!ctx || !ctx->result || !def || !doc || !def->qualified_name) {
        return;
    }
    CBMArena *a = ctx->scratch ? ctx->scratch : ctx->arena;
    php_line_t *lines = NULL;
    int n = php_doc_lines(a, doc, doc_line, &lines);
    php_emit_t em = {ctx, def};
    bool fence = false;
    for (int i = 0; i < n; i++) {
        const php_line_t *ln = &lines[i];
        if (ln->n >= PHP_FENCE_LEN && memcmp(ln->p, "```", PHP_FENCE_LEN) == 0) {
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
        php_mask_spans(ln, mask);
        php_line_see(&em, ln);
        php_line_inline(&em, ln, mask);
    }
}

/* ── Scope blob ──────────────────────────────────────────────────── */

typedef struct {
    CBMArena *a;
    char *buf;
    size_t len;
    size_t cap;
    bool oom;
} php_sb_t;

static void php_sb_put(php_sb_t *sb, const char *s, size_t n) {
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

static void php_sb_puts(php_sb_t *sb, const char *s) {
    php_sb_put(sb, s, strlen(s));
}

/* A field: a tab, then the text (never empty: "-" stands for none). */
static void php_sb_field(php_sb_t *sb, const char *s, size_t n) {
    php_sb_put(sb, "\t", SKIP_ONE);
    php_sb_put(sb, n ? s : "-", n ? n : SKIP_ONE);
}

static void php_sb_num(php_sb_t *sb, uint32_t v) {
    char num[CBM_SZ_32];
    int n = snprintf(num, sizeof(num), "%u", v);
    php_sb_field(sb, num, n > 0 ? (size_t)n : 0);
}

typedef struct {
    CBMExtractCtx *ctx;
    php_sb_t *sb;
    CBMHashTable *names; /* short name of a top-level type or function -> count */
    int blk;
    const char *ns; /* the current block's namespace ("" for the global one) */
    size_t ns_len;
    cbm_doclink_php_use_t *uses; /* the current block's */
    int nuses;
    int cap_uses;
} php_walk_t;

static bool php_kind(TSNode n, const char *k) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), k) == 0;
}

/* A name node's text without white space (`A \ B` is `A\B`), in the arena. */
static const char *php_name_text(php_walk_t *w, TSNode n, size_t *len) {
    uint32_t b = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    char *out = (char *)cbm_arena_alloc(w->sb->a, (size_t)(e - b) + SKIP_ONE);
    if (!out) {
        w->sb->oom = true;
        *len = 0;
        return "";
    }
    size_t k = 0;
    for (uint32_t i = b; i < e; i++) {
        char c = w->ctx->source[i];
        if (!isspace((unsigned char)c)) {
            out[k++] = c;
        }
    }
    out[k] = '\0';
    *len = k;
    return out;
}

/* All named children of a node, read with one cursor walk (indexing them is
 * quadratic). */
static TSNode *php_children(php_walk_t *w, TSNode parent, int *count) {
    *count = 0;
    uint32_t nc = ts_node_named_child_count(parent);
    if (nc == 0) {
        return NULL;
    }
    TSNode *kids = (TSNode *)cbm_arena_alloc(w->sb->a, (size_t)nc * sizeof(TSNode));
    if (!kids) {
        w->sb->oom = true;
        return NULL;
    }
    TSTreeCursor cur = ts_tree_cursor_new(parent);
    for (bool more = ts_tree_cursor_goto_first_child(&cur); more && *count < (int)nc;
         more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        if (ts_node_is_named(c)) {
            kids[(*count)++] = c;
        }
    }
    ts_tree_cursor_delete(&cur);
    return kids;
}

/* `function` or `const` among a node's anonymous children: 'f', 'k', else
 * `dflt`. */
static char php_use_kind(TSNode n, char dflt) {
    uint32_t nc = ts_node_child_count(n);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(n, i);
        if (ts_node_is_named(c)) {
            continue;
        }
        if (php_kind(c, "function")) {
            return 'f';
        }
        if (php_kind(c, "const")) {
            return 'k';
        }
    }
    return dflt;
}

static void php_add_use(php_walk_t *w, char kind, const char *alias, size_t alias_len,
                        const char *name, size_t name_len) {
    if (name_len && name[0] == '\\') {
        name++;
        name_len--;
    }
    if (!alias_len || !name_len) {
        return;
    }
    if (w->nuses == w->cap_uses) {
        int ncap = w->cap_uses ? w->cap_uses * PAIR_LEN : CBM_SZ_8;
        cbm_doclink_php_use_t *g = (cbm_doclink_php_use_t *)cbm_arena_alloc(
            w->sb->a, (size_t)ncap * sizeof(cbm_doclink_php_use_t));
        if (!g) {
            w->sb->oom = true;
            return;
        }
        if (w->nuses) {
            memcpy(g, w->uses, (size_t)w->nuses * sizeof(cbm_doclink_php_use_t));
        }
        w->uses = g;
        w->cap_uses = ncap;
    }
    w->uses[w->nuses++] = (cbm_doclink_php_use_t){alias, alias_len, name, name_len, kind};
    php_sb_puts(w->sb, "\nU");
    php_sb_num(w->sb, (uint32_t)w->blk);
    php_sb_field(w->sb, &kind, SKIP_ONE);
    php_sb_field(w->sb, alias, alias_len);
    php_sb_field(w->sb, name, name_len);
}

/* One clause of a use declaration; `prefix` is a group's common prefix. */
static void php_use_clause(php_walk_t *w, TSNode clause, char kind, const char *prefix,
                           size_t prefix_len) {
    TSNode alias = ts_node_child_by_field_name(clause, TS_FIELD("alias"));
    TSNode name = ts_node_named_child(clause, 0);
    if (!php_kind(name, "name") && !php_kind(name, "qualified_name")) {
        return;
    }
    size_t nl = 0;
    const char *nt = php_name_text(w, name, &nl);
    const char *full = nt;
    size_t fl = nl;
    if (prefix_len) {
        char *joined =
            cbm_arena_sprintf(w->sb->a, "%.*s\\%.*s", (int)prefix_len, prefix, (int)nl, nt);
        if (!joined) {
            w->sb->oom = true;
            return;
        }
        full = joined;
        fl = strlen(joined);
    }
    const char *al = NULL;
    size_t all = 0;
    if (!ts_node_is_null(alias)) {
        al = php_name_text(w, alias, &all);
    } else {
        const char *bs = full + fl;
        while (bs > full && bs[-SKIP_ONE] != '\\') {
            bs--;
        }
        al = bs;
        all = (size_t)(full + fl - bs);
    }
    php_add_use(w, php_use_kind(clause, kind), al, all, full, fl);
}

static void php_use_decl(php_walk_t *w, TSNode decl) {
    char kind = php_use_kind(decl, 'c');
    TSNode body = ts_node_child_by_field_name(decl, TS_FIELD("body"));
    const char *prefix = NULL;
    size_t pl = 0;
    if (!ts_node_is_null(body)) {
        TSNode pre = ts_node_named_child(decl, 0);
        if (!php_kind(pre, "namespace_name")) {
            return;
        }
        prefix = php_name_text(w, pre, &pl);
        if (pl && prefix[0] == '\\') {
            prefix++;
            pl--;
        }
    }
    int n = 0;
    TSNode *kids = php_children(w, ts_node_is_null(body) ? decl : body, &n);
    for (int i = 0; i < n; i++) {
        if (php_kind(kids[i], "namespace_use_clause")) {
            php_use_clause(w, kids[i], kind, prefix, pl);
        }
    }
}

static bool php_is_type_decl(TSNode n) {
    return php_kind(n, "class_declaration") || php_kind(n, "interface_declaration") ||
           php_kind(n, "trait_declaration") || php_kind(n, "enum_declaration");
}

static char php_type_kind(TSNode n) {
    if (php_kind(n, "interface_declaration")) {
        return 'i';
    }
    if (php_kind(n, "trait_declaration")) {
        return 't';
    }
    return php_kind(n, "enum_declaration") ? 'e' : 'c';
}

/* The name of a type or function declaration, NUL-terminated in the arena. */
static const char *php_decl_name(php_walk_t *w, TSNode decl, size_t *len) {
    TSNode name = ts_node_child_by_field_name(decl, TS_FIELD("name"));
    *len = 0;
    return ts_node_is_null(name) ? NULL : php_name_text(w, name, len);
}

/* `fqn` of a declared short name in the current block. */
static const char *php_fqn(php_walk_t *w, const char *name, size_t len, size_t *out_len) {
    if (!w->ns_len) {
        *out_len = len;
        return name;
    }
    char *s = cbm_arena_sprintf(w->sb->a, "%.*s\\%.*s", (int)w->ns_len, w->ns, (int)len, name);
    if (!s) {
        w->sb->oom = true;
        *out_len = 0;
        return "";
    }
    *out_len = strlen(s);
    return s;
}

static char php_dup_flag(php_walk_t *w, const char *name) {
    uintptr_t count = (uintptr_t)cbm_ht_get(w->names, name);
    return count > SKIP_ONE ? 'd' : '-';
}

static void php_base(php_walk_t *w, char rel, TSNode name) {
    size_t nl = 0;
    const char *nt = php_name_text(w, name, &nl);
    char buf[CBM_SZ_1K];
    size_t fl =
        cbm_doclink_php_qualify(buf, sizeof(buf), w->ns, w->ns_len, w->uses, w->nuses, nt, nl);
    if (!fl) {
        return;
    }
    php_sb_puts(w->sb, "\nB");
    php_sb_field(w->sb, &rel, SKIP_ONE);
    php_sb_field(w->sb, buf, fl);
}

/* The names a base clause, interface clause or trait use lists. */
static void php_bases(php_walk_t *w, char rel, TSNode clause) {
    int n = 0;
    TSNode *kids = php_children(w, clause, &n);
    for (int i = 0; i < n; i++) {
        if (php_kind(kids[i], "name") || php_kind(kids[i], "qualified_name")) {
            php_base(w, rel, kids[i]);
        }
    }
}

static void php_member(php_walk_t *w, char kind, TSNode name) {
    size_t nl = 0;
    const char *nt = php_name_text(w, name, &nl);
    if (nl && nt[0] == '$') {
        nt++;
        nl--;
    }
    if (!nl) {
        return;
    }
    php_sb_puts(w->sb, "\nK");
    php_sb_field(w->sb, &kind, SKIP_ONE);
    php_sb_field(w->sb, nt, nl);
}

/* Promoted constructor parameters are properties. */
static void php_promoted(php_walk_t *w, TSNode method) {
    TSNode params = ts_node_child_by_field_name(method, TS_FIELD("parameters"));
    int n = 0;
    TSNode *kids = ts_node_is_null(params) ? NULL : php_children(w, params, &n);
    for (int i = 0; i < n; i++) {
        if (php_kind(kids[i], "property_promotion_parameter")) {
            TSNode name = ts_node_child_by_field_name(kids[i], TS_FIELD("name"));
            if (!ts_node_is_null(name)) {
                php_member(w, 'p', name);
            }
        }
    }
}

/* The elements of a property or constant declaration. */
static void php_elements(php_walk_t *w, TSNode decl, const char *elem, char kind) {
    int n = 0;
    TSNode *kids = php_children(w, decl, &n);
    for (int i = 0; i < n; i++) {
        if (!php_kind(kids[i], elem)) {
            continue;
        }
        TSNode name = ts_node_child_by_field_name(kids[i], TS_FIELD("name"));
        if (ts_node_is_null(name)) {
            name = ts_node_named_child(kids[i], 0);
        }
        if (php_kind(name, "name") || php_kind(name, "variable_name")) {
            php_member(w, kind, name);
        }
    }
}

static void php_type_body(php_walk_t *w, TSNode body) {
    int n = 0;
    TSNode *kids = php_children(w, body, &n);
    for (int i = 0; i < n; i++) {
        TSNode c = kids[i];
        if (php_kind(c, "use_declaration")) {
            php_bases(w, 't', c);
        }
    }
    for (int i = 0; i < n; i++) {
        TSNode c = kids[i];
        if (php_kind(c, "method_declaration")) {
            TSNode name = ts_node_child_by_field_name(c, TS_FIELD("name"));
            if (!ts_node_is_null(name)) {
                php_member(w, 'm', name);
            }
            php_promoted(w, c);
        } else if (php_kind(c, "property_declaration")) {
            php_elements(w, c, "property_element", 'p');
        } else if (php_kind(c, "const_declaration")) {
            php_elements(w, c, "const_element", 'c');
        } else if (php_kind(c, "enum_case")) {
            TSNode name = ts_node_child_by_field_name(c, TS_FIELD("name"));
            if (!ts_node_is_null(name)) {
                php_member(w, 'e', name);
            }
        }
    }
}

static void php_type(php_walk_t *w, TSNode decl) {
    size_t nl = 0;
    const char *name = php_decl_name(w, decl, &nl);
    if (!name || !nl) {
        return;
    }
    size_t fl = 0;
    const char *fqn = php_fqn(w, name, nl, &fl);
    char kind = php_type_kind(decl);
    char flag = php_dup_flag(w, name);
    php_sb_puts(w->sb, "\nT");
    php_sb_field(w->sb, &kind, SKIP_ONE);
    php_sb_field(w->sb, fqn, fl);
    php_sb_field(w->sb, &flag, SKIP_ONE);
    php_sb_num(w->sb, ts_node_start_point(decl).row + SKIP_ONE);
    php_sb_num(w->sb, ts_node_end_point(decl).row + SKIP_ONE);
    int n = 0;
    TSNode *kids = php_children(w, decl, &n);
    for (int i = 0; i < n; i++) {
        if (php_kind(kids[i], "base_clause")) {
            php_bases(w, kind == 'i' ? 'i' : 'e', kids[i]);
        } else if (php_kind(kids[i], "class_interface_clause")) {
            php_bases(w, 'i', kids[i]);
        }
    }
    TSNode body = ts_node_child_by_field_name(decl, TS_FIELD("body"));
    if (!ts_node_is_null(body)) {
        php_type_body(w, body);
    }
}

static void php_function(php_walk_t *w, TSNode decl) {
    size_t nl = 0;
    const char *name = php_decl_name(w, decl, &nl);
    if (!name || !nl) {
        return;
    }
    size_t fl = 0;
    const char *fqn = php_fqn(w, name, nl, &fl);
    char flag = php_dup_flag(w, name);
    php_sb_puts(w->sb, "\nF");
    php_sb_field(w->sb, fqn, fl);
    php_sb_field(w->sb, &flag, SKIP_ONE);
}

static void php_count_name(php_walk_t *w, TSNode decl);

static bool php_is_branch(TSNode n) {
    return php_kind(n, "if_statement") || php_kind(n, "compound_statement") ||
           php_kind(n, "else_clause") || php_kind(n, "else_if_clause") ||
           php_kind(n, "colon_block");
}

/* The types and functions a top-level conditional declares
 * (`if (!function_exists('f')) { function f() {} }`): PHP declares them when
 * the branch runs, and the graph has their nodes. Counted for the dup flags
 * (count) or written. */
static void php_cond_decls(php_walk_t *w, TSNode n, int depth, bool count) {
    if (depth > PHP_COND_DEPTH) {
        return;
    }
    int k = 0;
    TSNode *kids = php_children(w, n, &k);
    for (int i = 0; i < k && !w->sb->oom; i++) {
        TSNode c = kids[i];
        bool type = php_is_type_decl(c);
        if (type || php_kind(c, "function_definition")) {
            if (count) {
                php_count_name(w, c);
            } else if (type) {
                php_type(w, c);
            } else {
                php_function(w, c);
            }
        } else if (php_is_branch(c)) {
            php_cond_decls(w, c, depth + SKIP_ONE, count);
        }
    }
}

/* One namespace block: kids[from, to) are its statements. Its use imports
 * are read first: a supertype is qualified with all of them. */
static void php_block(php_walk_t *w, const TSNode *kids, int from, int to) {
    w->nuses = 0;
    for (int i = from; i < to; i++) {
        if (php_kind(kids[i], "namespace_use_declaration")) {
            php_use_decl(w, kids[i]);
        }
    }
    for (int i = from; i < to && !w->sb->oom; i++) {
        if (php_is_type_decl(kids[i])) {
            php_type(w, kids[i]);
        } else if (php_kind(kids[i], "function_definition")) {
            php_function(w, kids[i]);
        } else if (php_kind(kids[i], "if_statement")) {
            php_cond_decls(w, kids[i], 0, false);
        }
    }
}

static void php_count_name(php_walk_t *w, TSNode decl) {
    size_t nl = 0;
    const char *name = php_decl_name(w, decl, &nl);
    if (!name || !nl) {
        return;
    }
    uintptr_t count = (uintptr_t)cbm_ht_get(w->names, name);
    cbm_ht_set(w->names, name, (void *)(count + SKIP_ONE));
}

/* Count the short names of the top-level types and functions of kids[0, n)
 * and of braced namespace bodies among them. */
static void php_count_names(php_walk_t *w, const TSNode *kids, int n) {
    for (int i = 0; i < n; i++) {
        TSNode c = kids[i];
        if (php_is_type_decl(c) || php_kind(c, "function_definition")) {
            php_count_name(w, c);
        } else if (php_kind(c, "if_statement")) {
            php_cond_decls(w, c, 0, true);
        } else if (php_kind(c, "namespace_definition")) {
            TSNode body = ts_node_child_by_field_name(c, TS_FIELD("body"));
            int bn = 0;
            TSNode *bk = ts_node_is_null(body) ? NULL : php_children(w, body, &bn);
            php_count_names(w, bk, bn);
        }
    }
}

static void php_namespace_rec(php_walk_t *w, TSNode def, uint32_t first, uint32_t last) {
    TSNode name = ts_node_child_by_field_name(def, TS_FIELD("name"));
    w->ns = "";
    w->ns_len = 0;
    if (!ts_node_is_null(name)) {
        w->ns = php_name_text(w, name, &w->ns_len);
        if (w->ns_len && w->ns[0] == '\\') {
            w->ns++;
            w->ns_len--;
        }
    }
    php_sb_puts(w->sb, "\nN");
    php_sb_num(w->sb, (uint32_t)w->blk);
    php_sb_field(w->sb, w->ns, w->ns_len);
    php_sb_num(w->sb, first);
    php_sb_num(w->sb, last);
}

static void php_walk_blocks(php_walk_t *w, const TSNode *kids, int n, uint32_t file_last) {
    int first_ns = CBM_NOT_FOUND;
    for (int i = 0; i < n && first_ns < 0; i++) {
        first_ns = php_kind(kids[i], "namespace_definition") ? i : first_ns;
    }
    if (first_ns < 0) {
        w->blk = 0;
        php_block(w, kids, 0, n);
        return;
    }
    w->blk = CBM_NOT_FOUND;
    for (int i = first_ns; i < n && !w->sb->oom; i++) {
        if (!php_kind(kids[i], "namespace_definition")) {
            continue;
        }
        w->blk++;
        TSNode def = kids[i];
        TSNode body = ts_node_child_by_field_name(def, TS_FIELD("body"));
        uint32_t first = ts_node_start_point(def).row + SKIP_ONE;
        if (!ts_node_is_null(body)) {
            php_namespace_rec(w, def, first, ts_node_end_point(def).row + SKIP_ONE);
            int bn = 0;
            TSNode *bk = php_children(w, body, &bn);
            php_block(w, bk, 0, bn);
            continue;
        }
        int next = i + SKIP_ONE;
        while (next < n && !php_kind(kids[next], "namespace_definition")) {
            next++;
        }
        uint32_t last = next < n ? ts_node_start_point(kids[next]).row : file_last;
        php_namespace_rec(w, def, first, last);
        php_block(w, kids, i + SKIP_ONE, next);
        i = next - SKIP_ONE;
    }
}

const char *cbm_doclink_php_scan_scope(CBMExtractCtx *ctx) {
    if (!ctx || ts_node_is_null(ctx->root)) {
        return NULL;
    }
    php_sb_t sb = {.a = ctx->arena};
    php_sb_puts(&sb, CBM_DOCLINK_PHP_SCOPE_TAG);
    php_walk_t w = {.ctx = ctx, .sb = &sb, .ns = ""};
    w.names = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, CBM_SZ_32);
    if (!w.names) {
        return NULL;
    }
    int n = 0;
    TSNode *kids = php_children(&w, ctx->root, &n);
    php_count_names(&w, kids, n);
    php_walk_blocks(&w, kids, n, ts_node_end_point(ctx->root).row + SKIP_ONE);
    cbm_ht_free(w.names);
    return sb.oom ? NULL : sb.buf;
}

/* ── Loose docs ──────────────────────────────────────────────────── */

/* The definition of a type declaration: its name, at its line. */
static const CBMDefinition *php_type_def(const CBMExtractCtx *ctx, TSNode decl) {
    TSNode name = ts_node_child_by_field_name(decl, TS_FIELD("name"));
    if (ts_node_is_null(name)) {
        return NULL;
    }
    uint32_t b = ts_node_start_byte(name);
    size_t n = ts_node_end_byte(name) - b;
    uint32_t line = ts_node_start_point(decl).row + SKIP_ONE;
    const CBMDefArray *defs = &ctx->result->defs;
    for (int i = 0; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (d->name && d->qualified_name && d->start_line == line && strlen(d->name) == n &&
            memcmp(d->name, ctx->source + b, n) == 0) {
            return d;
        }
    }
    return NULL;
}

static bool php_member_decl(TSNode n) {
    return php_kind(n, "property_declaration") || php_kind(n, "const_declaration") ||
           php_kind(n, "enum_case");
}

static bool php_doc_comment(const CBMExtractCtx *ctx, TSNode n) {
    if (!php_kind(n, "comment")) {
        return false;
    }
    uint32_t b = ts_node_start_byte(n);
    size_t len = ts_node_end_byte(n) - b;
    return len > PHP_PREFIX_LEN + SKIP_ONE && memcmp(ctx->source + b, "/**", PHP_PREFIX_LEN) == 0 &&
           ctx->source[b + PHP_PREFIX_LEN] != '*';
}

/* The doc blocks right above a type's properties, constants and cases. */
static void php_loose_type(CBMExtractCtx *ctx, TSNode decl, cbm_doclink_taken_fn taken) {
    const CBMDefinition *def = php_type_def(ctx, decl);
    TSNode body = ts_node_child_by_field_name(decl, TS_FIELD("body"));
    if (!def || ts_node_is_null(body)) {
        return;
    }
    TSNode doc = {0};
    TSTreeCursor cur = ts_tree_cursor_new(body);
    for (bool more = ts_tree_cursor_goto_first_child(&cur); more;
         more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        if (!ts_node_is_named(c)) {
            continue;
        }
        if (php_doc_comment(ctx, c)) {
            doc = c;
            continue;
        }
        uint32_t first = ts_node_is_null(doc) ? 0 : ts_node_start_point(doc).row + SKIP_ONE;
        uint32_t last = ts_node_is_null(doc) ? 0 : ts_node_end_point(doc).row + SKIP_ONE;
        if (first && php_member_decl(c) &&
            ts_node_start_point(c).row + SKIP_ONE <= last + SKIP_ONE && !taken(ctx, first, last)) {
            CBMArena *a = ctx->scratch ? ctx->scratch : ctx->arena;
            uint32_t b = ts_node_start_byte(doc);
            char *text = cbm_arena_strndup(a, ctx->source + b, ts_node_end_byte(doc) - b);
            if (text) {
                cbm_doclink_php_parse_doc(ctx, def, text, first);
            }
        }
        doc = (TSNode){0};
    }
    ts_tree_cursor_delete(&cur);
}

/* The types of the file: top level, namespace bodies, top-level conditionals. */
static void php_loose_walk(CBMExtractCtx *ctx, TSNode parent, int depth,
                           cbm_doclink_taken_fn taken) {
    if (depth > PHP_COND_DEPTH) {
        return;
    }
    TSTreeCursor cur = ts_tree_cursor_new(parent);
    for (bool more = ts_tree_cursor_goto_first_child(&cur); more;
         more = ts_tree_cursor_goto_next_sibling(&cur)) {
        TSNode c = ts_tree_cursor_current_node(&cur);
        if (php_is_type_decl(c)) {
            php_loose_type(ctx, c, taken);
        } else if (php_kind(c, "namespace_definition")) {
            TSNode body = ts_node_child_by_field_name(c, TS_FIELD("body"));
            if (!ts_node_is_null(body)) {
                php_loose_walk(ctx, body, depth + SKIP_ONE, taken);
            }
        } else if (php_is_branch(c)) {
            php_loose_walk(ctx, c, depth + SKIP_ONE, taken);
        }
    }
    ts_tree_cursor_delete(&cur);
}

void cbm_doclink_php_loose_docs(CBMExtractCtx *ctx, const CBMDefinition *file_def,
                                cbm_doclink_taken_fn taken) {
    (void)file_def; /* a member's doc belongs to its type, never to the file */
    if (!ctx || !ctx->result || !taken || ts_node_is_null(ctx->root)) {
        return;
    }
    php_loose_walk(ctx, ctx->root, 0, taken);
}

/* The blob without lines and imports: "N blk name first last" -> "N blk
 * name", "T kind fqn flags first last" -> "T kind fqn flags", U dropped. */
char *cbm_doclink_php_portable_scope(const char *scope) {
    size_t n = strlen(scope);
    char *out = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    for (const char *p = scope; *p;) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        const char *next = nl ? nl + SKIP_ONE : p + len;
        bool rec = len >= PAIR_LEN && p[SKIP_ONE] == '\t';
        if (rec && p[0] == 'U') {
            p = next;
            continue;
        }
        size_t keep = len;
        if (rec && (p[0] == 'N' || p[0] == 'T')) {
            int tabs = 0;
            for (size_t k = 0; k < len; k++) {
                if (p[k] == '\t' && ++tabs == (p[0] == 'N' ? CBM_SZ_3 : CBM_SZ_4)) {
                    keep = k;
                    break;
                }
            }
        }
        memcpy(out + w, p, keep);
        w += keep;
        if (nl) {
            out[w++] = '\n';
        }
        p = next;
    }
    out[w] = '\0';
    return out;
}
