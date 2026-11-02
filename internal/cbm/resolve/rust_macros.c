#include "rust_macros.h"
#include "../helpers.h"
#include "foundation/hash_table.h"
#include "foundation/mem_core.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { RS_PATH_MAX = 4096, RS_NAME_MAX = 256, RS_EXPR_VARS_MAX = 8, RS_GLOBS_MAX = 16 };
#define RS_KEY_SEP "\x1f"

typedef enum {
    RS_MAC_NONE = 0,
    RS_MAC_ITEMS,      /* expands to its input items */
    RS_MAC_ITEMS_META, /* the same, after a leading #![meta] */
    RS_MAC_EXPRS,      /* its arguments are expressions the expansion uses */
    RS_MAC_OTHER
} rs_mac_class_t;

struct cbm_rust_item_macros {
    CBMArena arena;
    CBMHashTable *manifests; /* directory of each Cargo.toml */
    CBMHashTable *crates;    /* directory -> the lib crate name its manifest declares */
    CBMHashTable *macros;    /* package directory, separator, macro name -> class */
    CBMHashTable *exported;  /* crate name, separator, #[macro_export] macro name -> class */
    CBMHashTable *packages;  /* package directories with a macro of a readable class */
    bool readable_exports;   /* some crate exports a macro of a readable class */
};

static cbm_rust_item_macros_t *g_item_macros = NULL;

static bool rs_mac_readable(rs_mac_class_t kind) {
    return kind == RS_MAC_ITEMS || kind == RS_MAC_ITEMS_META || kind == RS_MAC_EXPRS;
}

/* ── Tokens ────────────────────────────────────────────────────────── */

/* The Rust tokens these rules need: enough to never read a delimiter inside a comment or a
 * literal, and to tell a lifetime from a character. */
typedef enum {
    RS_T_EOF = 0,
    RS_T_IDENT,
    RS_T_OPEN,
    RS_T_CLOSE,
    RS_T_PUNCT,
    RS_T_LITERAL
} rs_kind_t;

typedef struct {
    rs_kind_t kind;
    size_t start, end;
} rs_tok_t;

typedef struct {
    const char *text;
    size_t length, at;
} rs_lex_t;

static bool rs_ident_start(unsigned char ch) {
    return isalpha(ch) || ch == '_' || ch >= 0x80;
}

static bool rs_ident_part(unsigned char ch) {
    return isalnum(ch) || ch == '_' || ch >= 0x80;
}

static void rs_skip_block_comment(rs_lex_t *lx) {
    const char *text = lx->text;
    size_t n = lx->length, at = lx->at + 2, depth = 1;
    while (at < n && depth) {
        if (at + 1 < n && text[at] == '/' && text[at + 1] == '*') {
            depth++;
            at += 2;
        } else if (at + 1 < n && text[at] == '*' && text[at + 1] == '/') {
            depth--;
            at += 2;
        } else {
            at++;
        }
    }
    lx->at = at;
}

static void rs_skip_trivia(rs_lex_t *lx) {
    for (;;) {
        while (lx->at < lx->length && isspace((unsigned char)lx->text[lx->at]))
            lx->at++;
        if (lx->at + 1 >= lx->length || lx->text[lx->at] != '/')
            return;
        if (lx->text[lx->at + 1] == '/') {
            const char *end = memchr(lx->text + lx->at, '\n', lx->length - lx->at);
            lx->at = end ? (size_t)(end - lx->text) + 1 : lx->length;
        } else if (lx->text[lx->at + 1] == '*') {
            rs_skip_block_comment(lx);
        } else {
            return;
        }
    }
}

static void rs_skip_string(rs_lex_t *lx) {
    size_t at = lx->at + 1;
    while (at < lx->length && lx->text[at] != '"')
        at += lx->text[at] == '\\' ? 2 : 1;
    lx->at = at < lx->length ? at + 1 : lx->length;
}

/* At the first `#` or `"` after r, br or cr. False: this is no raw string. */
static bool rs_skip_raw_string(rs_lex_t *lx) {
    size_t at = lx->at, hashes = 0;
    while (at < lx->length && lx->text[at] == '#') {
        hashes++;
        at++;
    }
    if (at >= lx->length || lx->text[at] != '"')
        return false;
    at++;
    while (at < lx->length) {
        const char *quote = memchr(lx->text + at, '"', lx->length - at);
        if (!quote)
            break;
        at = (size_t)(quote - lx->text) + 1;
        size_t found = 0;
        while (found < hashes && at + found < lx->length && lx->text[at + found] == '#')
            found++;
        if (found == hashes) {
            lx->at = at + hashes;
            return true;
        }
    }
    lx->at = lx->length;
    return true;
}

/* A character literal, or a lifetime when no closing quote follows one character. */
static void rs_skip_quote(rs_lex_t *lx) {
    const char *text = lx->text;
    size_t n = lx->length, at = lx->at + 1;
    if (at < n && text[at] == '\\') {
        at += 2;
        while (at < n && text[at] != '\'' && text[at] != '\n')
            at++;
        lx->at = at < n && text[at] == '\'' ? at + 1 : at;
        return;
    }
    size_t one = at;
    if (one < n) {
        one++;
        while (one < n && ((unsigned char)text[one] & 0xC0) == 0x80)
            one++;
    }
    if (one < n && text[one] == '\'') {
        lx->at = one + 1;
        return;
    }
    while (at < n && rs_ident_part((unsigned char)text[at]))
        at++;
    lx->at = at;
}

/* After an identifier: true when it was a literal's prefix, consumed with its literal. */
static bool rs_literal_after_prefix(rs_lex_t *lx, size_t start, size_t size) {
    const char *text = lx->text;
    char first = text[start];
    if (lx->at >= lx->length)
        return false;
    char next = text[lx->at];
    bool raw_prefix =
        (size == 1 && first == 'r') ||
        (size == 2 && (!memcmp(text + start, "br", 2) || !memcmp(text + start, "cr", 2)));
    if (raw_prefix && (next == '"' || next == '#'))
        return rs_skip_raw_string(lx);
    if (size == 1 && (first == 'b' || first == 'c') && next == '"') {
        rs_skip_string(lx);
        return true;
    }
    if (size == 1 && first == 'b' && next == '\'') {
        rs_skip_quote(lx);
        return true;
    }
    return false;
}

static rs_tok_t rs_next_ident(rs_lex_t *lx, size_t start) {
    const char *text = lx->text;
    size_t at = start;
    while (at < lx->length && rs_ident_part((unsigned char)text[at]))
        at++;
    lx->at = at;
    rs_tok_t tok = {.kind = RS_T_IDENT, .start = start, .end = at};
    if (rs_literal_after_prefix(lx, start, at - start)) {
        tok.kind = RS_T_LITERAL;
        tok.end = lx->at;
        return tok;
    }
    /* r#name: the name is what the program spells. */
    if (at - start == 1 && text[start] == 'r' && at + 1 < lx->length && text[at] == '#' &&
        rs_ident_start((unsigned char)text[at + 1])) {
        at++;
        tok.start = at;
        while (at < lx->length && rs_ident_part((unsigned char)text[at]))
            at++;
        lx->at = at;
        tok.end = at;
    }
    return tok;
}

static rs_tok_t rs_next(rs_lex_t *lx) {
    rs_skip_trivia(lx);
    rs_tok_t tok = {.kind = RS_T_EOF, .start = lx->at, .end = lx->at};
    if (lx->at >= lx->length)
        return tok;
    unsigned char ch = (unsigned char)lx->text[lx->at];
    if (rs_ident_start(ch))
        return rs_next_ident(lx, lx->at);
    if (isdigit(ch)) {
        while (lx->at < lx->length && rs_ident_part((unsigned char)lx->text[lx->at]))
            lx->at++;
        tok.kind = RS_T_LITERAL;
    } else if (ch == '"') {
        rs_skip_string(lx);
        tok.kind = RS_T_LITERAL;
    } else if (ch == '\'') {
        rs_skip_quote(lx);
        tok.kind = RS_T_LITERAL;
    } else {
        lx->at++;
        tok.kind = ch == '(' || ch == '[' || ch == '{'   ? RS_T_OPEN
                   : ch == ')' || ch == ']' || ch == '}' ? RS_T_CLOSE
                                                         : RS_T_PUNCT;
    }
    tok.end = lx->at;
    return tok;
}

static bool rs_word_is(const rs_lex_t *lx, const rs_tok_t *tok, const char *word) {
    size_t size = strlen(word);
    return tok->kind == RS_T_IDENT && tok->end - tok->start == size &&
           !memcmp(lx->text + tok->start, word, size);
}

static bool rs_tok_char(const rs_lex_t *lx, const rs_tok_t *tok, char ch) {
    return (tok->kind == RS_T_PUNCT || tok->kind == RS_T_OPEN || tok->kind == RS_T_CLOSE) &&
           lx->text[tok->start] == ch;
}

static bool rs_expect(rs_lex_t *lx, char ch) {
    rs_tok_t tok = rs_next(lx);
    return rs_tok_char(lx, &tok, ch);
}

static bool rs_expect_word(rs_lex_t *lx, const char *word) {
    rs_tok_t tok = rs_next(lx);
    return rs_word_is(lx, &tok, word);
}

static bool rs_expect_var(rs_lex_t *lx, rs_tok_t *name) {
    if (!rs_expect(lx, '$'))
        return false;
    *name = rs_next(lx);
    return name->kind == RS_T_IDENT;
}

static char rs_closer(char open) {
    return open == '(' ? ')' : open == '[' ? ']' : '}';
}

static bool rs_same_text(const rs_lex_t *lx, const rs_tok_t *a, const rs_tok_t *b) {
    return a->end - a->start == b->end - b->start &&
           !memcmp(lx->text + a->start, lx->text + b->start, a->end - a->start);
}

/* From just after an opening delimiter to just after its closer, any nesting. */
static bool rs_skip_tree(rs_lex_t *lx) {
    size_t depth = 1;
    while (depth) {
        rs_tok_t tok = rs_next(lx);
        if (tok.kind == RS_T_EOF)
            return false;
        depth += tok.kind == RS_T_OPEN;
        depth -= tok.kind == RS_T_CLOSE;
    }
    return true;
}

/* ── Reading a definition ─────────────────────────────────────────── */

static bool rs_expect_repeat_op(rs_lex_t *lx) {
    rs_tok_t tok = rs_next(lx);
    return rs_tok_char(lx, &tok, '*') || rs_tok_char(lx, &tok, '+');
}

/* The transcriber of an items rule, after its opening delimiter:
 *     $( #[attr]* $item )*   or   #[attr]* $( $item )*   or   #[attr]* $item */
static bool rs_items_body(rs_lex_t *lx, const rs_tok_t *item, bool repeated, char close) {
    rs_tok_t tok = rs_next(lx);
    while (rs_tok_char(lx, &tok, '#')) {
        if (!rs_expect(lx, '[') || !rs_skip_tree(lx))
            return false;
        tok = rs_next(lx);
    }
    if (!rs_tok_char(lx, &tok, '$'))
        return false;
    rs_lex_t after_dollar = *lx;
    tok = rs_next(lx);
    bool opened = rs_tok_char(lx, &tok, '(');
    if (opened != repeated)
        return false;
    if (!opened)
        *lx = after_dollar;
    if (opened) {
        tok = rs_next(lx);
        while (rs_tok_char(lx, &tok, '#')) {
            if (!rs_expect(lx, '[') || !rs_skip_tree(lx))
                return false;
            tok = rs_next(lx);
        }
        if (!rs_tok_char(lx, &tok, '$'))
            return false;
    }
    tok = rs_next(lx);
    if (tok.kind != RS_T_IDENT || !rs_same_text(lx, &tok, item))
        return false;
    if (opened && (!rs_expect(lx, ')') || !rs_expect_repeat_op(lx)))
        return false;
    return rs_expect(lx, close);
}

/* One rule, just after the definition's opening delimiter:
 *     ( $( $item:item )* ) => { $( #[attr]* $item )* }
 * with an optional leading #![$meta:meta] in the pattern, and the same without the repetition. */
static rs_mac_class_t rs_items_rule(rs_lex_t lx, char outer_close) {
    rs_tok_t tok = rs_next(&lx);
    if (tok.kind != RS_T_OPEN)
        return RS_MAC_OTHER;
    char pattern_close = rs_closer(lx.text[tok.start]);
    rs_mac_class_t kind = RS_MAC_ITEMS;
    tok = rs_next(&lx);
    if (rs_tok_char(&lx, &tok, '#')) {
        rs_tok_t meta;
        if (!rs_expect(&lx, '!') || !rs_expect(&lx, '[') || !rs_expect_var(&lx, &meta) ||
            !rs_expect(&lx, ':') || !rs_expect_word(&lx, "meta") || !rs_expect(&lx, ']'))
            return RS_MAC_OTHER;
        kind = RS_MAC_ITEMS_META;
        tok = rs_next(&lx);
    }
    if (!rs_tok_char(&lx, &tok, '$'))
        return RS_MAC_OTHER;
    rs_lex_t after_dollar = lx;
    tok = rs_next(&lx);
    bool repeated = rs_tok_char(&lx, &tok, '(');
    rs_tok_t item;
    if (repeated) {
        if (!rs_expect_var(&lx, &item))
            return RS_MAC_OTHER;
    } else {
        lx = after_dollar;
        item = rs_next(&lx);
        if (item.kind != RS_T_IDENT)
            return RS_MAC_OTHER;
    }
    if (!rs_expect(&lx, ':') || !rs_expect_word(&lx, "item"))
        return RS_MAC_OTHER;
    if (repeated && (!rs_expect(&lx, ')') || !rs_expect_repeat_op(&lx)))
        return RS_MAC_OTHER;
    if (!rs_expect(&lx, pattern_close) || !rs_expect(&lx, '=') || !rs_expect(&lx, '>'))
        return RS_MAC_OTHER;
    tok = rs_next(&lx);
    if (tok.kind != RS_T_OPEN ||
        !rs_items_body(&lx, &item, repeated, rs_closer(lx.text[tok.start])))
        return RS_MAC_OTHER;
    tok = rs_next(&lx);
    if (rs_tok_char(&lx, &tok, ';'))
        tok = rs_next(&lx);
    return rs_tok_char(&lx, &tok, outer_close) ? kind : RS_MAC_OTHER;
}

/* A transcriber uses `$name` where it is more than text: not under stringify!. */
static bool rs_body_uses_var(rs_lex_t lx, const rs_tok_t *var) {
    size_t depth = 1;
    rs_tok_t previous = {.kind = RS_T_EOF};
    while (depth) {
        rs_tok_t tok = rs_next(&lx);
        if (tok.kind == RS_T_EOF)
            return false;
        if (rs_word_is(&lx, &tok, "stringify")) {
            rs_lex_t probe = lx;
            rs_tok_t bang = rs_next(&probe);
            rs_tok_t open = rs_next(&probe);
            if (rs_tok_char(&probe, &bang, '!') && open.kind == RS_T_OPEN) {
                lx = probe;
                if (!rs_skip_tree(&lx))
                    return false;
                continue;
            }
        }
        if (tok.kind == RS_T_IDENT && rs_tok_char(&lx, &previous, '$') &&
            rs_same_text(&lx, &tok, var))
            return true;
        depth += tok.kind == RS_T_OPEN;
        depth -= tok.kind == RS_T_CLOSE;
        previous = tok;
    }
    return false;
}

/* The pattern of an expression rule, after its opening delimiter: expr variables and commas,
 * then at most a trailing $($rest:tt)* and a trailing $(,)?. A macro that matches anything else is
 * doing something with its tokens other than evaluating them. */
static bool rs_exprs_pattern(rs_lex_t *lx, rs_tok_t *vars, int *nvars, char close) {
    bool tail = false;
    for (;;) {
        rs_tok_t tok = rs_next(lx);
        if (rs_tok_char(lx, &tok, close))
            return true;
        if (rs_tok_char(lx, &tok, ','))
            continue;
        if (!rs_tok_char(lx, &tok, '$'))
            return false;
        tok = rs_next(lx);
        if (tok.kind == RS_T_IDENT) {
            if (tail || !rs_expect(lx, ':') || !rs_expect_word(lx, "expr") ||
                *nvars >= RS_EXPR_VARS_MAX)
                return false;
            vars[(*nvars)++] = tok;
            continue;
        }
        /* $( $x:expr ),*, $( $rest:tt )* or $(,)?. Only $(,)? may follow a tt. */
        if (!rs_tok_char(lx, &tok, '('))
            return false;
        tok = rs_next(lx);
        if (rs_tok_char(lx, &tok, '$')) {
            rs_tok_t name = rs_next(lx);
            if (tail || name.kind != RS_T_IDENT || !rs_expect(lx, ':'))
                return false;
            rs_tok_t fragment = rs_next(lx);
            if (rs_word_is(lx, &fragment, "tt")) {
                tail = true;
            } else if (rs_word_is(lx, &fragment, "expr") && *nvars < RS_EXPR_VARS_MAX) {
                vars[(*nvars)++] = name;
            } else {
                return false;
            }
            tok = rs_next(lx);
            if (rs_tok_char(lx, &tok, ','))
                tok = rs_next(lx);
            if (!rs_tok_char(lx, &tok, ')'))
                return false;
        } else if (!rs_tok_char(lx, &tok, ',') || !rs_expect(lx, ')')) {
            return false;
        }
        tok = rs_next(lx);
        if (rs_tok_char(lx, &tok, ','))
            tok = rs_next(lx);
        if (!rs_tok_char(lx, &tok, '*') && !rs_tok_char(lx, &tok, '+') &&
            !rs_tok_char(lx, &tok, '?'))
            return false;
    }
}

/* Every rule, just after the definition's opening delimiter. Each pattern is expressions and each
 * transcriber uses all of them, so whichever rule matches, the arguments are evaluated. */
static rs_mac_class_t rs_exprs_rules(rs_lex_t lx, char outer_close) {
    bool any = false;
    for (;;) {
        rs_tok_t tok = rs_next(&lx);
        if (rs_tok_char(&lx, &tok, outer_close))
            return any ? RS_MAC_EXPRS : RS_MAC_OTHER;
        if (tok.kind != RS_T_OPEN)
            return RS_MAC_OTHER;
        rs_tok_t vars[RS_EXPR_VARS_MAX];
        int nvars = 0;
        if (!rs_exprs_pattern(&lx, vars, &nvars, rs_closer(lx.text[tok.start])) ||
            !rs_expect(&lx, '=') || !rs_expect(&lx, '>'))
            return RS_MAC_OTHER;
        tok = rs_next(&lx);
        if (tok.kind != RS_T_OPEN)
            return RS_MAC_OTHER;
        for (int i = 0; i < nvars; i++)
            if (!rs_body_uses_var(lx, &vars[i]))
                return RS_MAC_OTHER;
        any = any || nvars > 0;
        if (!rs_skip_tree(&lx))
            return RS_MAC_OTHER;
        tok = rs_next(&lx);
        if (rs_tok_char(&lx, &tok, outer_close))
            return any ? RS_MAC_EXPRS : RS_MAC_OTHER;
        if (!rs_tok_char(&lx, &tok, ';'))
            return RS_MAC_OTHER;
    }
}

static rs_mac_class_t rs_definition_class(const rs_lex_t *body, char outer_close) {
    rs_mac_class_t items = rs_items_rule(*body, outer_close);
    return items != RS_MAC_OTHER ? items : rs_exprs_rules(*body, outer_close);
}

/* ── The table ─────────────────────────────────────────────────────── */

cbm_rust_item_macros_t *cbm_rust_item_macros_new(void) {
    cbm_rust_item_macros_t *table = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, sizeof(*table));
    if (!table)
        return NULL;
    memset(table, 0, sizeof(*table));
    cbm_arena_init_lazy(&table->arena, 4096);
    table->manifests = cbm_ht_create(16);
    table->crates = cbm_ht_create(16);
    table->macros = cbm_ht_create(16);
    table->exported = cbm_ht_create(16);
    table->packages = cbm_ht_create(16);
    if (!table->manifests || !table->crates || !table->macros || !table->exported ||
        !table->packages) {
        cbm_rust_item_macros_free(table);
        return NULL;
    }
    return table;
}

void cbm_rust_item_macros_free(cbm_rust_item_macros_t *table) {
    if (!table)
        return;
    cbm_ht_free(table->manifests);
    cbm_ht_free(table->crates);
    cbm_ht_free(table->macros);
    cbm_ht_free(table->exported);
    cbm_ht_free(table->packages);
    cbm_arena_destroy(&table->arena);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, table);
}

void cbm_rust_item_macros_install(cbm_rust_item_macros_t *table) {
    if (g_item_macros != table)
        cbm_rust_item_macros_free(g_item_macros);
    g_item_macros = table;
}

static void rs_dir_of(const char *rel, char *dir, size_t capacity) {
    snprintf(dir, capacity, "%s", rel);
    char *slash = strrchr(dir, '/');
    if (slash)
        *slash = '\0';
    else
        dir[0] = '\0';
}

/* The lib crate's name as code spells it: [lib] name, else [package] name, with - folded to _. */
static const char *rs_manifest_crate(CBMArena *arena, const char *text, size_t length) {
    const char *names[2] = {NULL, NULL}; /* [package], [lib] */
    int section = -1;
    const char *stop = text + length;
    for (const char *line = text; line < stop;) {
        const char *end = memchr(line, '\n', (size_t)(stop - line));
        end = end ? end : stop;
        const char *at = line;
        while (at < end && (*at == ' ' || *at == '\t'))
            at++;
        size_t room = (size_t)(end - at);
        if (room && *at == '[') {
            section = room >= 9 && !strncmp(at, "[package]", 9) ? 0
                      : room >= 5 && !strncmp(at, "[lib]", 5)   ? 1
                                                                : -1;
        } else if (section >= 0 && room > 4 && !strncmp(at, "name", 4) && !names[section]) {
            const char *quote = memchr(at, '"', room);
            const char *close = quote ? memchr(quote + 1, '"', (size_t)(end - quote - 1)) : NULL;
            size_t size = close ? (size_t)(close - quote - 1) : 0;
            /* `name = "x"`, not `name-suffix = "x"`. */
            const char *after = at + 4;
            while (after < end && (*after == ' ' || *after == '\t'))
                after++;
            if (size && after < end && *after == '=')
                names[section] = cbm_arena_strndup(arena, quote + 1, size);
        }
        line = end + 1;
    }
    const char *chosen = names[1] ? names[1] : names[0];
    if (!chosen)
        return NULL;
    char *folded = cbm_arena_strdup(arena, chosen);
    for (char *at = folded; at && *at; at++)
        if (*at == '-')
            *at = '_';
    return folded;
}

bool cbm_rust_item_macros_add_manifest(cbm_rust_item_macros_t *table, const char *rel,
                                       const char *text, size_t length) {
    if (!table || !rel || strlen(rel) >= RS_PATH_MAX)
        return false;
    char dir[RS_PATH_MAX];
    rs_dir_of(rel, dir, sizeof(dir));
    const char *copy = cbm_arena_strdup(&table->arena, dir);
    if (!copy)
        return false;
    cbm_ht_set(table->manifests, copy, (void *)copy);
    const char *crate = text ? rs_manifest_crate(&table->arena, text, length) : NULL;
    if (crate)
        cbm_ht_set(table->crates, copy, (void *)crate);
    return true;
}

/* The nearest directory at or above this file that holds a Cargo.toml. No manifest at all:
 * the whole tree is one unit. Bounded by the depth of the path. */
static void rs_package_of(const cbm_rust_item_macros_t *table, const char *rel, char *dir,
                          size_t capacity) {
    rs_dir_of(rel, dir, capacity);
    while (*dir && !cbm_ht_get(table->manifests, dir)) {
        char *slash = strrchr(dir, '/');
        if (slash)
            *slash = '\0';
        else
            dir[0] = '\0';
    }
}

/* Whichever definition textual scope selects: all of them must say the same. */
static bool rs_note_class(CBMArena *arena, CBMHashTable *table, const char *owner, const char *text,
                          const rs_tok_t *name, rs_mac_class_t kind) {
    const char *key = cbm_arena_sprintf(arena, "%s" RS_KEY_SEP "%.*s", owner,
                                        (int)(name->end - name->start), text + name->start);
    if (!key)
        return false;
    uintptr_t known = (uintptr_t)cbm_ht_get(table, key);
    if (known && known != (uintptr_t)kind)
        kind = RS_MAC_OTHER;
    cbm_ht_set(table, key, (void *)(uintptr_t)kind);
    return true;
}

bool cbm_rust_item_macros_add_source(cbm_rust_item_macros_t *table, const char *rel,
                                     const char *source, size_t length) {
    if (!table || !rel || !source || strlen(rel) >= RS_PATH_MAX)
        return false;
    if (!cbm_memmem(source, length, "macro_rules", 11))
        return true;
    char package[RS_PATH_MAX];
    rs_package_of(table, rel, package, sizeof(package));
    const char *crate = cbm_ht_get(table->crates, package);
    rs_lex_t lx = {source, length, 0};
    bool exported = false;
    for (rs_tok_t tok = rs_next(&lx); tok.kind != RS_T_EOF; tok = rs_next(&lx)) {
        if (rs_word_is(&lx, &tok, "macro_export"))
            exported = true;
        if (!rs_word_is(&lx, &tok, "macro_rules")) {
            /* The attribute stands right before its item. */
            exported = exported && !rs_tok_char(&lx, &tok, ';') && !rs_tok_char(&lx, &tok, '{');
            continue;
        }
        rs_lex_t probe = lx;
        bool is_export = exported;
        exported = false;
        if (!rs_expect(&probe, '!'))
            continue;
        rs_tok_t name = rs_next(&probe);
        rs_tok_t open = rs_next(&probe);
        if (name.kind != RS_T_IDENT || open.kind != RS_T_OPEN)
            continue;
        rs_mac_class_t kind = rs_definition_class(&probe, rs_closer(source[open.start]));
        if (!rs_note_class(&table->arena, table->macros, package, source, &name, kind))
            return false;
        if (is_export && crate &&
            !rs_note_class(&table->arena, table->exported, crate, source, &name, kind))
            return false;
        table->readable_exports = table->readable_exports || (is_export && rs_mac_readable(kind));
        if (rs_mac_readable(kind) && !cbm_ht_get(table->packages, package)) {
            const char *copy = cbm_arena_strdup(&table->arena, package);
            if (!copy)
                return false;
            cbm_ht_set(table->packages, copy, (void *)copy);
        }
    }
    return true;
}

/* ── The view ──────────────────────────────────────────────────────── */

static rs_mac_class_t rs_lookup(const CBMHashTable *macros, const char *owner, const char *text,
                                const rs_tok_t *name) {
    char key[RS_PATH_MAX + RS_NAME_MAX];
    int size = snprintf(key, sizeof(key), "%s" RS_KEY_SEP "%.*s", owner,
                        (int)(name->end - name->start), text + name->start);
    if (size < 0 || (size_t)size >= sizeof(key))
        return RS_MAC_NONE;
    return (rs_mac_class_t)(uintptr_t)cbm_ht_get(macros, key);
}

static const char *rs_tok_copy(CBMArena *arena, const rs_lex_t *lx, const rs_tok_t *tok) {
    return cbm_arena_strndup(arena, lx->text + tok->start, tok->end - tok->start);
}

/* What this file's `use` paths say about macro names. `use tokio_test::assert_ok;` names the macro
 * another package of the project exports. A name a package defines but the file imports from a
 * crate the project does not hold is that crate's macro, whatever the package's own definition
 * says. A path from crate, self or super stays inside the package and changes nothing. */
typedef struct {
    const cbm_rust_item_macros_t *table;
    const char *package;
    CBMHashTable *names; /* macro name -> class, or RS_MAC_OTHER for a foreign one */
    CBMArena *arena;
    bool pin_project; /* `use pin_project_lite::pin_project` */
    /* `use tokio_test::*;` and `#[macro_use] extern crate x;`: their exported macros are in scope.
     */
    const char *globs[RS_GLOBS_MAX];
    int nglobs;
} rs_imports_t;

static void rs_import_leaf(rs_imports_t *imports, const rs_lex_t *lx, const char *root,
                           const rs_tok_t *leaf) {
    rs_mac_class_t exported = rs_lookup(imports->table->exported, root, lx->text, leaf);
    rs_mac_class_t own = rs_lookup(imports->table->macros, imports->package, lx->text, leaf);
    if (!exported && !own)
        return;
    if (!imports->names)
        imports->names = cbm_ht_create(4);
    const char *name = rs_tok_copy(imports->arena, lx, leaf);
    if (imports->names && name)
        cbm_ht_set(imports->names, name, (void *)(uintptr_t)(exported ? exported : RS_MAC_OTHER));
}

static void rs_note_glob(rs_imports_t *imports, const char *crate) {
    if (crate && imports->nglobs < RS_GLOBS_MAX)
        imports->globs[imports->nglobs++] = crate;
}

static void rs_read_imports(rs_imports_t *imports, const char *source, size_t length) {
    rs_lex_t lx = {source, length, 0};
    bool macro_use = false;
    for (rs_tok_t tok = rs_next(&lx); tok.kind != RS_T_EOF; tok = rs_next(&lx)) {
        if (rs_word_is(&lx, &tok, "macro_use")) {
            macro_use = true;
            continue;
        }
        if (rs_word_is(&lx, &tok, "extern")) {
            rs_lex_t probe = lx;
            rs_tok_t word = rs_next(&probe), crate = rs_next(&probe);
            if (macro_use && rs_word_is(&probe, &word, "crate") && crate.kind == RS_T_IDENT)
                rs_note_glob(imports, rs_tok_copy(imports->arena, &probe, &crate));
        }
        macro_use = macro_use && (rs_tok_char(&lx, &tok, ']') || rs_tok_char(&lx, &tok, '#') ||
                                  rs_tok_char(&lx, &tok, '['));
        if (!rs_word_is(&lx, &tok, "use"))
            continue;
        rs_tok_t root = rs_next(&lx);
        if (rs_tok_char(&lx, &root, ':') && rs_expect(&lx, ':'))
            root = rs_next(&lx);
        bool local = rs_word_is(&lx, &root, "crate") || rs_word_is(&lx, &root, "self") ||
                     rs_word_is(&lx, &root, "super");
        bool pin_crate = rs_word_is(&lx, &root, "pin_project_lite");
        const char *root_name =
            root.kind == RS_T_IDENT ? rs_tok_copy(imports->arena, &lx, &root) : "";
        /* Exported macros live at the crate root: only `use c::*` reaches them. */
        bool at_root = true;
        for (tok = rs_next(&lx); tok.kind != RS_T_EOF && !rs_tok_char(&lx, &tok, ';');
             tok = rs_next(&lx)) {
            if (!local && at_root && rs_tok_char(&lx, &tok, '*'))
                rs_note_glob(imports, root_name);
            if (tok.kind != RS_T_IDENT)
                continue;
            at_root = false;
            if (pin_crate && rs_word_is(&lx, &tok, "pin_project"))
                imports->pin_project = true;
            if (!local && root_name)
                rs_import_leaf(imports, &lx, root_name, &tok);
        }
        if (tok.kind == RS_T_EOF)
            break;
    }
}

typedef struct {
    char close;
    bool wrapper, outermost, semicolon;
} rs_frame_t;

typedef struct {
    const char *source;
    size_t length;
    char *view;
    uint32_t *spans, *exprs;
    int nspans, cap_spans, nexprs, cap_exprs;
    CBMArena *arena; /* NULL: the caller wants no spans */
    bool failed;
    rs_frame_t *stack;
    size_t depth, capacity, wrappers, open_at;
    const cbm_rust_item_macros_t *table;
    const char *package;
    const rs_imports_t *imports;
} rs_scan_t;

static void rs_blank(rs_scan_t *scan, size_t start, size_t end) {
    if (!scan->view) {
        scan->view = cbm_alloc(CBM_MEM_CLASS_EXTRACT, scan->length + 1);
        if (!scan->view) {
            scan->failed = true;
            return;
        }
        memcpy(scan->view, scan->source, scan->length);
        scan->view[scan->length] = '\0';
    }
    for (size_t at = start; at < end; at++)
        if (scan->view[at] != '\n' && scan->view[at] != '\r')
            scan->view[at] = ' ';
}

static void rs_note_pair(rs_scan_t *scan, uint32_t **items, int *count, int *capacity, int width,
                         uint32_t first, uint32_t second) {
    if (!scan->arena)
        return;
    if (*count == *capacity) {
        int cap = *capacity ? *capacity * 2 : 8;
        uint32_t *grown =
            cbm_arena_alloc(scan->arena, (size_t)cap * (size_t)width * sizeof(*grown));
        if (!grown) {
            scan->failed = true;
            return;
        }
        if (*count)
            memcpy(grown, *items, (size_t)*count * (size_t)width * sizeof(*grown));
        *items = grown;
        *capacity = cap;
    }
    (*items)[*count * width] = first;
    if (width > 1)
        (*items)[*count * width + 1] = second;
    (*count)++;
}

static bool rs_push_frame(rs_scan_t *scan, rs_frame_t frame) {
    if (scan->depth == scan->capacity) {
        size_t capacity = scan->capacity ? scan->capacity * 2 : 64;
        rs_frame_t *grown = cbm_alloc(CBM_MEM_CLASS_EXTRACT, capacity * sizeof(*grown));
        if (!grown)
            return false;
        if (scan->depth)
            memcpy(grown, scan->stack, scan->depth * sizeof(*grown));
        cbm_free(CBM_MEM_CLASS_EXTRACT, scan->stack);
        scan->stack = grown;
        scan->capacity = capacity;
    }
    scan->stack[scan->depth++] = frame;
    return true;
}

/* A closer: false when the delimiters do not balance by these tokens. */
static bool rs_scan_close(rs_scan_t *scan, rs_lex_t *lx, const rs_tok_t *tok) {
    if (!scan->depth || scan->stack[scan->depth - 1].close != scan->source[tok->start])
        return false;
    rs_frame_t frame = scan->stack[--scan->depth];
    if (!frame.wrapper)
        return true;
    rs_blank(scan, tok->start, tok->end);
    size_t end = tok->end;
    if (frame.semicolon) {
        rs_lex_t probe = *lx;
        rs_tok_t next = rs_next(&probe);
        if (rs_tok_char(&probe, &next, ';')) {
            rs_blank(scan, next.start, next.end);
            *lx = probe;
            end = next.end;
        }
    }
    scan->wrappers--;
    if (frame.outermost)
        rs_note_pair(scan, &scan->spans, &scan->nspans, &scan->cap_spans, 2,
                     (uint32_t)scan->open_at, (uint32_t)end);
    return true;
}

/* What a macro name means in this file, given how it was spelled. */
static rs_mac_class_t rs_macro_class(const rs_scan_t *scan, const rs_lex_t *lx,
                                     const rs_tok_t *name, const char *path_crate) {
    if (path_crate && !strcmp(path_crate, "crate"))
        return rs_lookup(scan->table->macros, scan->package, lx->text, name);
    if (path_crate)
        return rs_lookup(scan->table->exported, path_crate, lx->text, name);
    char spelled[RS_NAME_MAX];
    size_t size = name->end - name->start;
    if (scan->imports->names && size < sizeof(spelled)) {
        memcpy(spelled, lx->text + name->start, size);
        spelled[size] = '\0';
        rs_mac_class_t imported =
            (rs_mac_class_t)(uintptr_t)cbm_ht_get(scan->imports->names, spelled);
        if (imported)
            return imported;
    }
    rs_mac_class_t own = rs_lookup(scan->table->macros, scan->package, lx->text, name);
    if (own)
        return own;
    if (scan->imports->pin_project && rs_word_is(lx, name, "pin_project"))
        return RS_MAC_ITEMS;
    /* A name the file imports by glob from one crate only. */
    rs_mac_class_t globbed = RS_MAC_NONE;
    for (int i = 0; i < scan->imports->nglobs; i++) {
        rs_mac_class_t kind =
            rs_lookup(scan->table->exported, scan->imports->globs[i], lx->text, name);
        if (kind && globbed && kind != globbed)
            return RS_MAC_OTHER;
        globbed = kind ? kind : globbed;
    }
    return globbed;
}

/* `NAME!(...)` of a readable class: blank what is not the items or the expressions. */
static bool rs_scan_macro(rs_scan_t *scan, rs_lex_t *lx, const rs_tok_t *first,
                          const rs_tok_t *name, const char *path_crate) {
    rs_lex_t probe = *lx;
    rs_tok_t bang = rs_next(&probe);
    if (!rs_tok_char(&probe, &bang, '!'))
        return false;
    rs_tok_t open = rs_next(&probe);
    if (open.kind != RS_T_OPEN)
        return false;
    rs_mac_class_t kind = rs_macro_class(scan, lx, name, path_crate);
    if (!rs_mac_readable(kind))
        return false;
    char opener = scan->source[open.start];
    /* An expression list needs its parentheses: they are the expression the view leaves. */
    if (kind == RS_MAC_EXPRS) {
        if (opener != '(')
            return false;
        rs_blank(scan, first->start, bang.end);
        rs_note_pair(scan, &scan->exprs, &scan->nexprs, &scan->cap_exprs, 1, (uint32_t)open.start,
                     0);
        *lx = probe;
        rs_frame_t plain = {.close = ')'};
        return rs_push_frame(scan, plain);
    }
    rs_frame_t frame = {.close = rs_closer(opener),
                        .wrapper = true,
                        .semicolon = opener != '{',
                        .outermost = scan->wrappers == 0};
    rs_blank(scan, first->start, open.end);
    if (frame.outermost)
        scan->open_at = first->start;
    scan->wrappers++;
    *lx = probe;
    if (kind == RS_MAC_ITEMS_META) {
        rs_lex_t attr = *lx;
        rs_tok_t hash = rs_next(&attr);
        if (rs_tok_char(&attr, &hash, '#') && rs_expect(&attr, '!') && rs_expect(&attr, '[') &&
            rs_skip_tree(&attr)) {
            rs_blank(scan, hash.start, attr.at);
            *lx = attr;
        }
    }
    return rs_push_frame(scan, frame);
}

/* `crate_name::NAME!(...)`: at the first segment, with the rest still ahead. */
static bool rs_scan_path_macro(rs_scan_t *scan, rs_lex_t *lx, const rs_tok_t *first) {
    rs_lex_t probe = *lx;
    rs_tok_t c1 = rs_next(&probe), c2 = rs_next(&probe);
    if (!rs_tok_char(&probe, &c1, ':') || !rs_tok_char(&probe, &c2, ':') ||
        first->end - first->start >= RS_NAME_MAX)
        return false;
    rs_tok_t name = rs_next(&probe);
    if (name.kind != RS_T_IDENT)
        return false;
    char crate[RS_NAME_MAX];
    memcpy(crate, lx->text + first->start, first->end - first->start);
    crate[first->end - first->start] = '\0';
    rs_tok_t head = *first;
    if (!rs_scan_macro(scan, &probe, &head, &name, crate))
        return false;
    *lx = probe;
    return true;
}

/* One pass over the tokens with a stack of open delimiters: linear in the file. */
static bool rs_scan_tokens(rs_scan_t *scan) {
    rs_lex_t lx = {scan->source, scan->length, 0};
    char previous = 0;
    bool ok = true;
    for (rs_tok_t tok = rs_next(&lx); ok && tok.kind != RS_T_EOF; tok = rs_next(&lx)) {
        char here = tok.kind == RS_T_PUNCT ? scan->source[tok.start] : 0;
        if (tok.kind == RS_T_CLOSE) {
            ok = rs_scan_close(scan, &lx, &tok);
        } else if (tok.kind == RS_T_OPEN) {
            rs_frame_t plain = {.close = rs_closer(scan->source[tok.start])};
            ok = rs_push_frame(scan, plain);
        } else if (tok.kind == RS_T_IDENT && previous != ':' && previous != '$') {
            if (rs_scan_macro(scan, &lx, &tok, &tok, NULL))
                here = 0;
            else
                (void)rs_scan_path_macro(scan, &lx, &tok);
        }
        previous = here;
    }
    return ok;
}

static bool rs_scan_file(const char *rel, const char *source, size_t length, rs_scan_t *scan) {
    const cbm_rust_item_macros_t *table = g_item_macros;
    if (!table || !rel || !source || strlen(rel) >= RS_PATH_MAX || length > UINT32_MAX)
        return true;
    char package[RS_PATH_MAX];
    rs_package_of(table, rel, package, sizeof(package));
    /* Most files of most projects stop here, unread. */
    if (!table->readable_exports && !cbm_ht_get(table->packages, package) &&
        !cbm_memmem(source, length, "pin_project_lite", 16))
        return true;
    CBMArena names;
    cbm_arena_init_lazy(&names, 256);
    rs_imports_t imports = {.table = table, .package = package, .arena = &names};
    rs_read_imports(&imports, source, length);
    scan->table = table;
    scan->package = package;
    scan->imports = &imports;
    /* Delimiters that do not balance by these tokens: nothing here can be trusted as a wrapper. */
    bool ok = rs_scan_tokens(scan) && !scan->wrappers && !scan->failed;
    cbm_free(CBM_MEM_CLASS_EXTRACT, scan->stack);
    scan->stack = NULL;
    cbm_ht_free(imports.names);
    cbm_arena_destroy(&names);
    return ok;
}

char *cbm_rust_item_view(const char *rel, const char *source, size_t length) {
    rs_scan_t scan = {.source = source, .length = length};
    if (!rs_scan_file(rel, source, length, &scan)) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, scan.view);
        return NULL;
    }
    return scan.view;
}

bool cbm_rust_item_spans(CBMArena *arena, const char *rel, const char *source, size_t length,
                         cbm_rust_item_spans_t *out) {
    rs_scan_t scan = {.source = source, .length = length, .arena = arena};
    bool ok = rs_scan_file(rel, source, length, &scan);
    cbm_free(CBM_MEM_CLASS_EXTRACT, scan.view);
    *out = (cbm_rust_item_spans_t){.wrapped = ok ? scan.spans : NULL,
                                   .nwrapped = ok ? scan.nspans : 0,
                                   .expressions = ok ? scan.exprs : NULL,
                                   .nexpressions = ok ? scan.nexprs : 0};
    return ok || !scan.failed;
}
