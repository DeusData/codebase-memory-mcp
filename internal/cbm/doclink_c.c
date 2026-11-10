/*
 * doclink_c.c — C doc-comment references (Doxygen) and the C doc-link scope.
 *
 * Files. Only .c and .h files: a .h is parsed as C++, which does not change
 * that; other C++ and Objective-C sources are not part of this leg.
 *
 * Docs. The four Doxygen comment styles: blocks opening with slash-star-star
 * or slash-star-bang, and runs of lines opening with three slashes or with
 * slash-slash-bang (a `<` after the opener marks a member's trailing doc). A
 * definition's stored doc goes through cbm_doclink_c_parse_doc. Every other
 * Doxygen block of the file outside function bodies (a prototype's doc in a
 * header, which the graph keeps no node for; a group's text) goes through
 * cbm_doclink_c_loose_docs, and its references belong to the file.
 *
 * Tokens:
 *   - `@ref name` or `\ref name`, an optional quoted label after it: family
 *     doxygen_ref;
 *   - the names listed after `@see`, `\see`, `@sa` or `\sa` (the paragraph up
 *     to an empty line or the next command at the start of a line) that have
 *     the shape of a symbol: `name`, `name()`, `::name`: family see. An
 *     explicit @ref inside such a paragraph is a doxygen_ref token.
 * Text inside \code, \verbatim, \dot, \msc or \startuml blocks, a Markdown
 * fence or a code span holds none, nor does the word after \c, \p, \a, \b, \e
 * or \em. A URL is the core's href family.
 *
 * Scope blob, one record per line after the tag line "c1" (tab-separated):
 *   D kind link name qn parent  a definition the graph has a node for:
 *                        kind f (function), v (variable), t (type: typedef,
 *                        struct, union or enum), e (enum constant), r (a
 *                        struct or union field), m (object-like macro), M
 *                        (function-like macro); link x (visible in every file:
 *                        external linkage, or a type, constant or macro of a
 *                        header) or s (this file only: static, a type,
 *                        constant or macro of a .c file, a field); qn the
 *                        node's qualified name; parent the enum of a constant
 *                        or the struct of a field ("-" for none)
 *   A name               a Doxygen page, group, anchor or section the file's
 *                        docs define
 * The blob holds no line, so it is its own persisted form.
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

enum { C_OPENER_LEN = 3, C_FENCE_LEN = 3, C_CMD_MAX = 32 };

/* True for a .c or .h file: the only files this leg reads. */
static bool c_leg_file(const char *rel_path) {
    size_t n = rel_path ? strlen(rel_path) : 0;
    return n > PAIR_LEN && rel_path[n - PAIR_LEN] == '.' &&
           (rel_path[n - SKIP_ONE] == 'c' || rel_path[n - SKIP_ONE] == 'h');
}

/* ── Doc lines ───────────────────────────────────────────────────── */

typedef struct {
    const char *p;
    size_t n;
    uint32_t line;
} c_line_t;

static bool c_has(const char *p, size_t n, const char *lit) {
    size_t l = strlen(lit);
    return n >= l && memcmp(p, lit, l) == 0;
}

/* One raw line of a doc comment without its comment marks. */
static c_line_t c_strip_line(const char *s, const char *le, bool first, bool *block,
                             uint32_t line) {
    while (s < le && (*s == ' ' || *s == '\t')) {
        s++;
    }
    size_t n = (size_t)(le - s);
    if (first && (c_has(s, n, "/**") || c_has(s, n, "/*!"))) {
        *block = true;
        s += C_OPENER_LEN;
    } else if (!*block && (c_has(s, n, "///") || c_has(s, n, "//!"))) {
        s += C_OPENER_LEN;
    } else if (*block && s < le && *s == '*' && !(s + SKIP_ONE < le && s[SKIP_ONE] == '/')) {
        s++;
    }
    if (s < le && *s == '<') {
        s++; /* a member's trailing doc */
    }
    const char *t = le;
    while (t > s && (t[-SKIP_ONE] == ' ' || t[-SKIP_ONE] == '\t' || t[-SKIP_ONE] == '\r')) {
        t--;
    }
    if (*block && t - s >= PAIR_LEN && t[-SKIP_ONE] == '/' && t[-PAIR_LEN] == '*') {
        t -= PAIR_LEN;
        while (t > s && (t[-SKIP_ONE] == ' ' || t[-SKIP_ONE] == '\t')) {
            t--;
        }
    }
    while (s < t && (*s == ' ' || *s == '\t')) {
        s++;
    }
    return (c_line_t){s, (size_t)(t - s), line};
}

/* The doc's lines without their comment marks. */
static int c_doc_lines(CBMArena *a, const char *doc, uint32_t doc_line, c_line_t **out) {
    int cap = SKIP_ONE;
    for (const char *p = doc; *p; p++) {
        cap += *p == '\n';
    }
    c_line_t *lines = (c_line_t *)cbm_arena_alloc(a, (size_t)cap * sizeof(c_line_t));
    if (!lines) {
        return 0;
    }
    int n = 0;
    bool block = false;
    for (const char *p = doc; n < cap;) {
        const char *le = strchr(p, '\n');
        if (!le) {
            le = p + strlen(p);
        }
        lines[n] = c_strip_line(p, le, n == 0, &block, doc_line + (uint32_t)n);
        n++;
        if (!*le) {
            break;
        }
        p = le + SKIP_ONE;
    }
    *out = lines;
    return n;
}

/* ── Tokens ──────────────────────────────────────────────────────── */

typedef struct {
    CBMExtractCtx *ctx;
    const CBMDefinition *def;
    bool see;     /* inside an @see paragraph */
    int verbatim; /* inside a code block: the index of its end command, else -1 */
    bool fence;
} c_emit_t;

static bool c_ident_start(char c) {
    return isalpha((unsigned char)c) || c == '_';
}

static bool c_ident_char(char c) {
    return isalnum((unsigned char)c) || c == '_';
}

static bool c_is_url(const char *t, size_t n) {
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

static void c_push(const c_emit_t *em, const char *ref, size_t n, int syntax, uint32_t line) {
    if (n == 0) {
        return;
    }
    if (n > CBM_DOCLINK_C_REF_MAX) {
        n = CBM_DOCLINK_C_REF_MAX + SKIP_ONE; /* clipped: the resolver calls it unparseable */
    }
    if (c_is_url(ref, n)) {
        syntax = CBM_DOCLINK_HREF;
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

/* Mark the bytes of a line that sit in a code span or a quoted label. */
static void c_mask(const c_line_t *ln, bool *mask) {
    for (size_t i = 0; i < ln->n;) {
        char q = ln->p[i];
        if (q != '`' && q != '"') {
            i++;
            continue;
        }
        size_t j = i + SKIP_ONE;
        while (j < ln->n && ln->p[j] != q) {
            j++;
        }
        if (j >= ln->n) {
            i++; /* unpaired */
            continue;
        }
        for (size_t k = i; k <= j; k++) {
            mask[k] = true;
        }
        i = j + SKIP_ONE;
    }
}

/* A command at p[i] (`\word` or `@word`, not inside a word or an address):
 * the word's length, else 0. */
static size_t c_command_at(const char *p, size_t i, size_t n) {
    if ((p[i] != '\\' && p[i] != '@') ||
        (i > 0 &&
         (c_ident_char(p[i - SKIP_ONE]) || p[i - SKIP_ONE] == '\\' || p[i - SKIP_ONE] == '@'))) {
        return 0;
    }
    size_t k = i + SKIP_ONE;
    while (k < n && isalpha((unsigned char)p[k])) {
        k++;
    }
    return k - i - SKIP_ONE;
}

static bool c_word_is(const char *w, size_t n, const char *lit) {
    return strlen(lit) == n && memcmp(w, lit, n) == 0;
}

static bool c_inline_cmd(const char *w, size_t n) {
    return c_word_is(w, n, "c") || c_word_is(w, n, "p") || c_word_is(w, n, "a") ||
           c_word_is(w, n, "b") || c_word_is(w, n, "e") || c_word_is(w, n, "em");
}

static const char *const C_BLOCK_OPEN[] = {"code", "verbatim", "dot", "msc", "startuml", NULL};
static const char *const C_BLOCK_CLOSE[] = {"endcode", "endverbatim", "enddot",
                                            "endmsc",  "enduml",      NULL};

static int c_block_index(const char *const *list, const char *w, size_t n) {
    for (int i = 0; list[i]; i++) {
        if (c_word_is(w, n, list[i])) {
            return i;
        }
    }
    return CBM_NOT_FOUND;
}

/* The end of the next non-space position of p[j, n). */
static size_t c_skip_space(const char *p, size_t j, size_t n) {
    while (j < n && isspace((unsigned char)p[j])) {
        j++;
    }
    return j;
}

/* `@ref name`: the name after the command (sentence punctuation dropped,
 * `()` kept); returns the position after it. */
static size_t c_ref(const c_emit_t *em, const c_line_t *ln, size_t j, const bool *mask) {
    j = c_skip_space(ln->p, j, ln->n);
    size_t k = j;
    while (k < ln->n && !isspace((unsigned char)ln->p[k]) && ln->p[k] != '"' && !mask[k]) {
        k++;
    }
    size_t e = k;
    while (e > j && strchr(".,;:", ln->p[e - SKIP_ONE])) {
        e--;
    }
    if (e > j && ln->p[e - SKIP_ONE] == ')' && !(e - j >= PAIR_LEN && ln->p[e - PAIR_LEN] == '(')) {
        e--; /* a closing parenthesis of the prose around it */
    }
    c_push(em, ln->p + j, e - j, CBM_DOCLINK_C_REF, ln->line);
    return k;
}

/* A word of an @see paragraph: a token when it has the shape of a symbol. */
static void c_see_word(const c_emit_t *em, const char *w, size_t n, uint32_t line) {
    while (n > 0 && strchr(".,;:", w[n - SKIP_ONE])) {
        n--;
    }
    if (c_is_url(w, n)) {
        c_push(em, w, n, CBM_DOCLINK_HREF, line);
        return;
    }
    size_t i = (n >= PAIR_LEN && w[0] == ':' && w[SKIP_ONE] == ':') ? PAIR_LEN : 0;
    if (i >= n || !c_ident_start(w[i])) {
        return;
    }
    while (i < n && c_ident_char(w[i])) {
        i++;
    }
    if (i + PAIR_LEN == n && w[i] == '(' && w[i + SKIP_ONE] == ')') {
        i = n;
    }
    if (i == n) {
        c_push(em, w, n, CBM_DOCLINK_C_SEE, line);
    }
}

/* One line outside code blocks: commands, and the words of an @see
 * paragraph. */
static void c_line(c_emit_t *em, const c_line_t *ln, const bool *mask) {
    size_t i = 0;
    bool line_start = true;
    while (i < ln->n) {
        if (mask[i] || isspace((unsigned char)ln->p[i]) || ln->p[i] == ',') {
            i++;
            continue;
        }
        size_t cl = c_command_at(ln->p, i, ln->n);
        if (cl > 0) {
            const char *w = ln->p + i + SKIP_ONE;
            size_t j = i + SKIP_ONE + cl;
            if (c_word_is(w, cl, "ref")) {
                i = c_ref(em, ln, j, mask);
            } else if (c_word_is(w, cl, "see") || c_word_is(w, cl, "sa")) {
                em->see = true;
                i = j;
            } else if (c_inline_cmd(w, cl)) {
                j = c_skip_space(ln->p, j, ln->n);
                while (j < ln->n && !isspace((unsigned char)ln->p[j])) {
                    j++; /* the word it formats is text */
                }
                i = j;
            } else {
                int open = c_block_index(C_BLOCK_OPEN, w, cl);
                if (open >= 0) {
                    em->verbatim = open;
                    return;
                }
                if (line_start) {
                    em->see = false; /* a new paragraph */
                }
                i = j;
            }
            line_start = false;
            continue;
        }
        size_t k = i;
        while (k < ln->n && !isspace((unsigned char)ln->p[k]) && ln->p[k] != ',' && !mask[k] &&
               c_command_at(ln->p, k, ln->n) == 0) {
            k++;
        }
        if (k == i) {
            k = i + SKIP_ONE;
        } else if (em->see) {
            c_see_word(em, ln->p + i, k - i, ln->line);
        }
        line_start = false;
        i = k;
    }
}

/* The end command of the open code block on this line, if any. */
static bool c_closes_block(const c_line_t *ln, int open) {
    for (size_t i = 0; i < ln->n; i++) {
        size_t cl = c_command_at(ln->p, i, ln->n);
        if (cl && c_word_is(ln->p + i + SKIP_ONE, cl, C_BLOCK_CLOSE[open])) {
            return true;
        }
    }
    return false;
}

static void c_harvest(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                      uint32_t doc_line) {
    CBMArena *a = ctx->scratch ? ctx->scratch : ctx->arena;
    c_line_t *lines = NULL;
    int n = c_doc_lines(a, doc, doc_line, &lines);
    c_emit_t em = {ctx, def, false, CBM_NOT_FOUND, false};
    for (int i = 0; i < n; i++) {
        const c_line_t *ln = &lines[i];
        if (em.verbatim >= 0) {
            if (c_closes_block(ln, em.verbatim)) {
                em.verbatim = CBM_NOT_FOUND;
            }
            continue;
        }
        if (ln->n >= C_FENCE_LEN &&
            (memcmp(ln->p, "```", C_FENCE_LEN) == 0 || memcmp(ln->p, "~~~", C_FENCE_LEN) == 0)) {
            em.fence = !em.fence;
            continue;
        }
        if (em.fence) {
            continue;
        }
        if (ln->n == 0) {
            em.see = false; /* an empty line ends a paragraph */
            continue;
        }
        bool *mask = (bool *)cbm_arena_calloc(a, ln->n + SKIP_ONE);
        if (!mask) {
            return;
        }
        c_mask(ln, mask);
        c_line(&em, ln, mask);
    }
}

void cbm_doclink_c_parse_doc(CBMExtractCtx *ctx, const CBMDefinition *def, const char *doc,
                             uint32_t doc_line) {
    if (!ctx || !ctx->result || !def || !doc || !def->qualified_name ||
        !c_leg_file(ctx->rel_path)) {
        return;
    }
    c_harvest(ctx, def, doc, doc_line);
}

/* ── Loose docs ──────────────────────────────────────────────────── */

static bool c_kind(TSNode n, const char *k) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), k) == 0;
}

/* A comment node's Doxygen style: 'b' a block, 'l' a line, 0 none. */
static char c_doc_style(const char *src, TSNode n) {
    uint32_t b = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    const char *p = src + b;
    size_t len = e - b;
    if (c_has(p, len, "/**/") || c_has(p, len, "/***")) {
        return 0;
    }
    if (c_has(p, len, "/**") || c_has(p, len, "/*!")) {
        return 'b';
    }
    if (c_has(p, len, "////")) {
        return 0;
    }
    return (c_has(p, len, "///") || c_has(p, len, "//!")) ? 'l' : 0;
}

typedef struct {
    CBMExtractCtx *ctx;
    cbm_doclink_taken_fn taken;
    const CBMDefinition *file_def;
    uint32_t start_byte; /* a pending run of line comments */
    uint32_t end_byte;
    uint32_t first;
    uint32_t last;
    bool pending;
} c_loose_t;

static void c_loose_emit(c_loose_t *lw, uint32_t sb, uint32_t eb, uint32_t first, uint32_t last) {
    if (lw->taken(lw->ctx, first, last)) {
        return; /* a definition's doc: harvested with the definition */
    }
    CBMArena *a = lw->ctx->scratch ? lw->ctx->scratch : lw->ctx->arena;
    char *doc = cbm_arena_strndup(a, lw->ctx->source + sb, eb - sb);
    if (doc) {
        c_harvest(lw->ctx, lw->file_def, doc, first);
    }
}

static void c_loose_flush(c_loose_t *lw) {
    if (lw->pending) {
        c_loose_emit(lw, lw->start_byte, lw->end_byte, lw->first, lw->last);
        lw->pending = false;
    }
}

static void c_loose_comment(c_loose_t *lw, TSNode n) {
    char style = c_doc_style(lw->ctx->source, n);
    uint32_t row = ts_node_start_point(n).row + SKIP_ONE;
    if (style == 'l' && lw->pending && row == lw->last + SKIP_ONE) {
        lw->end_byte = ts_node_end_byte(n); /* the run goes on */
        lw->last = row;
        return;
    }
    c_loose_flush(lw);
    if (style == 'l') {
        lw->pending = true;
        lw->start_byte = ts_node_start_byte(n);
        lw->end_byte = ts_node_end_byte(n);
        lw->first = row;
        lw->last = row;
    } else if (style == 'b') {
        c_loose_emit(lw, ts_node_start_byte(n), ts_node_end_byte(n), row,
                     ts_node_end_point(n).row + SKIP_ONE);
    }
}

void cbm_doclink_c_loose_docs(CBMExtractCtx *ctx, const CBMDefinition *file_def,
                              cbm_doclink_taken_fn taken) {
    if (!ctx || !ctx->result || !file_def || !taken || ts_node_is_null(ctx->root) ||
        !c_leg_file(ctx->rel_path)) {
        return;
    }
    c_loose_t lw = {.ctx = ctx, .taken = taken, .file_def = file_def};
    TSTreeCursor cur = ts_tree_cursor_new(ctx->root);
    for (bool more = ts_tree_cursor_goto_first_child(&cur); more;) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        if (c_kind(n, "comment")) {
            c_loose_comment(&lw, n);
        } else if (ts_node_is_named(n)) {
            c_loose_flush(&lw); /* code between: the run ends */
        }
        /* Doxygen reads no comment inside a function body */
        if (!c_kind(n, "compound_statement") && ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cur)) {
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cur);
    c_loose_flush(&lw);
}

/* ── Scope blob ──────────────────────────────────────────────────── */

typedef struct {
    CBMArena *a;
    char *buf;
    size_t len;
    size_t cap;
    bool oom;
} c_sb_t;

static void c_sb_put(c_sb_t *sb, const char *s, size_t n) {
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

static void c_sb_field(c_sb_t *sb, const char *s, size_t n) {
    c_sb_put(sb, "\t", SKIP_ONE);
    for (size_t i = 0; i < n; i++) {
        char c = (s[i] == '\t' || s[i] == '\n' || s[i] == '\r') ? ' ' : s[i];
        c_sb_put(sb, &c, SKIP_ONE);
    }
}

/* The name a declarator declares (through pointers, arrays, functions and
 * parentheses). */
static TSNode c_declarator_name(TSNode d) {
    for (int guard = 0; guard < CBM_SZ_32 && !ts_node_is_null(d); guard++) {
        if (c_kind(d, "identifier") || c_kind(d, "type_identifier")) {
            return d;
        }
        TSNode inner = ts_node_child_by_field_name(d, TS_FIELD("declarator"));
        if (ts_node_is_null(inner)) {
            inner = ts_node_named_child(d, 0); /* a parenthesized declarator */
        }
        d = inner;
    }
    return (TSNode){0};
}

static bool c_is_static(const char *src, TSNode decl) {
    uint32_t nc = ts_node_named_child_count(decl);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(decl, i);
        if (!c_kind(c, "storage_class_specifier")) {
            continue;
        }
        uint32_t b = ts_node_start_byte(c);
        uint32_t e = ts_node_end_byte(c);
        if (e - b == CBM_SZ_6 && memcmp(src + b, "static", CBM_SZ_6) == 0) {
            return true;
        }
    }
    return false;
}

/* The names a static declaration or definition declares. */
static void c_static_names(CBMExtractCtx *ctx, CBMHashTable *names, TSNode decl) {
    if (!c_is_static(ctx->source, decl)) {
        return;
    }
    TSTreeCursor cur = ts_tree_cursor_new(decl);
    for (bool more = ts_tree_cursor_goto_first_child(&cur); more;
         more = ts_tree_cursor_goto_next_sibling(&cur)) {
        const char *field = ts_tree_cursor_current_field_name(&cur);
        if (!field || strcmp(field, "declarator") != 0) {
            continue;
        }
        TSNode name = c_declarator_name(ts_tree_cursor_current_node(&cur));
        if (ts_node_is_null(name)) {
            continue;
        }
        uint32_t b = ts_node_start_byte(name);
        char *key = cbm_arena_strndup(ctx->arena, ctx->source + b, ts_node_end_byte(name) - b);
        if (key) {
            cbm_ht_set(names, key, (void *)(uintptr_t)SKIP_ONE);
        }
    }
    ts_tree_cursor_delete(&cur);
}

/* The static names of the file: top-level declarations, also inside
 * preprocessor conditionals and extern "C" blocks. */
static void c_collect_static(CBMExtractCtx *ctx, CBMHashTable *names) {
    TSTreeCursor cur = ts_tree_cursor_new(ctx->root);
    for (bool more = ts_tree_cursor_goto_first_child(&cur); more;) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        bool container = c_kind(n, "preproc_if") || c_kind(n, "preproc_ifdef") ||
                         c_kind(n, "preproc_else") || c_kind(n, "preproc_elif") ||
                         c_kind(n, "preproc_elifdef") || c_kind(n, "linkage_specification") ||
                         c_kind(n, "declaration_list");
        if (c_kind(n, "function_definition") || c_kind(n, "declaration")) {
            c_static_names(ctx, names, n);
        }
        if (container && ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cur)) {
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cur);
}

static char c_def_kind(const CBMDefinition *d) {
    const char *l = d->label;
    if (strcmp(l, "Function") == 0) {
        return 'f';
    }
    if (strcmp(l, "Variable") == 0) {
        return d->parent_class ? 'e' : 'v';
    }
    if (strcmp(l, "Field") == 0) {
        return 'r';
    }
    if (strcmp(l, "Macro") == 0) {
        return d->signature ? 'M' : 'm';
    }
    if (strcmp(l, "Type") == 0 || strcmp(l, "Class") == 0 || strcmp(l, "Enum") == 0) {
        return 't';
    }
    return 0;
}

/* `\page name`, `\defgroup name` ... in one doc text: A records. */
static void c_anchors(c_sb_t *sb, const char *doc) {
    static const char *const cmds[] = {"page",    "defgroup",   "addtogroup",    "anchor",
                                       "section", "subsection", "subsubsection", NULL};
    size_t n = strlen(doc);
    for (size_t i = 0; i < n; i++) {
        size_t cl = c_command_at(doc, i, n);
        if (!cl) {
            continue;
        }
        bool hit = false;
        for (int k = 0; cmds[k] && !hit; k++) {
            hit = c_word_is(doc + i + SKIP_ONE, cl, cmds[k]);
        }
        size_t j = i + SKIP_ONE + cl;
        if (!hit || j >= n || (doc[j] != ' ' && doc[j] != '\t')) {
            continue;
        }
        j = c_skip_space(doc, j, n);
        size_t k = j;
        while (k < n && (c_ident_char(doc[k]) || doc[k] == '.' || doc[k] == '-')) {
            k++;
        }
        if (k > j) {
            c_sb_put(sb, "\nA", PAIR_LEN);
            c_sb_field(sb, doc + j, k - j);
        }
        i = k;
    }
}

/* The anchors every Doxygen comment of the file defines. */
static void c_scope_anchors(CBMExtractCtx *ctx, c_sb_t *sb) {
    TSTreeCursor cur = ts_tree_cursor_new(ctx->root);
    for (bool more = ts_tree_cursor_goto_first_child(&cur); more;) {
        TSNode n = ts_tree_cursor_current_node(&cur);
        if (c_kind(n, "comment") && c_doc_style(ctx->source, n)) {
            uint32_t b = ts_node_start_byte(n);
            char *text = cbm_arena_strndup(sb->a, ctx->source + b, ts_node_end_byte(n) - b);
            if (text) {
                c_anchors(sb, text);
            }
        }
        if (!c_kind(n, "compound_statement") && ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cur)) {
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cur);
}

const char *cbm_doclink_c_scan_scope(CBMExtractCtx *ctx) {
    if (!ctx || !ctx->result || ts_node_is_null(ctx->root) || !c_leg_file(ctx->rel_path)) {
        return NULL;
    }
    c_sb_t sb = {.a = ctx->arena};
    c_sb_put(&sb, CBM_DOCLINK_C_SCOPE_TAG, strlen(CBM_DOCLINK_C_SCOPE_TAG));
    CBMHashTable *statics = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, CBM_SZ_32);
    CBMHashTable *seen = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, CBM_SZ_64);
    if (!statics || !seen) {
        cbm_ht_free(statics);
        cbm_ht_free(seen);
        return NULL;
    }
    c_collect_static(ctx, statics);
    size_t pl = strlen(ctx->rel_path);
    bool header = ctx->rel_path[pl - SKIP_ONE] == 'h';
    const CBMDefArray *defs = &ctx->result->defs;
    for (int i = 0; i < defs->count && !sb.oom; i++) {
        const CBMDefinition *d = &defs->items[i];
        char kind = (d->label && d->name && d->qualified_name) ? c_def_kind(d) : 0;
        if (!kind || cbm_ht_get(seen, d->qualified_name)) {
            continue; /* one node per qualified name (#if variants) */
        }
        cbm_ht_set(seen, d->qualified_name, (void *)(uintptr_t)SKIP_ONE);
        bool object = kind == 'f' || kind == 'v';
        char link = object ? (cbm_ht_get(statics, d->name) ? 's' : 'x')
                           : (header && kind != 'r' ? 'x' : 's');
        c_sb_put(&sb, "\nD", PAIR_LEN);
        c_sb_field(&sb, &kind, SKIP_ONE);
        c_sb_field(&sb, &link, SKIP_ONE);
        c_sb_field(&sb, d->name, strlen(d->name));
        c_sb_field(&sb, d->qualified_name, strlen(d->qualified_name));
        const char *parent =
            (kind == 'e' || kind == 'r') && d->parent_class ? d->parent_class : "-";
        c_sb_field(&sb, parent, strlen(parent));
    }
    cbm_ht_free(statics);
    cbm_ht_free(seen);
    c_scope_anchors(ctx, &sb);
    return sb.oom ? NULL : sb.buf;
}
