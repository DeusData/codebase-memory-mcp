/*
 * test_impact.c — the test-impact engine behind detect_changes scope:"tests".
 *
 * Part 1: the test model (see test_impact.h).
 */
#include "mcp/test_impact.h"

#include "foundation/arena.h"

#include <ctype.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ── Model storage ───────────────────────────────────────────────── */

enum {
    TM_ARENA_BLOCK = 64 * 1024,
    TM_INVOCATION_ARENA_BLOCK = 4 * 1024,
    TM_FIRST_CAP = 64,
    TM_GROWTH = 2,
    /* #if nesting the scanner follows. Deeper frames are still counted, so the
     * matching #endif is found; only their brace depth is not restored. */
    TM_COND_MAX = 64,
};

struct cbm_test_model {
    CBMArena arena; /* owns every string and every array below */
    cbm_test_conventions_t conv;
    cbm_test_case_t *cases;
    int case_count;
    int case_cap;
    cbm_test_suite_t *suites;
    int suite_count;
    int suite_cap;
    cbm_test_registration_t *regs;
    int reg_count;
    int reg_cap;
    cbm_test_runner_suite_t *runner;
    int runner_count;
    int runner_cap;
    bool descriptor_mode;
    cbm_test_declaration_t *declarations;
    int declaration_count;
    bool presets[CBM_TEST_PRESET_COUNT];
    cbm_test_model_mapping_issue_t mapping;
    bool finished;
    bool incomplete;       /* sticky: a partial scan must never justify omission */
    bool scoped_uncertain; /* some suite is `uncertain`; see tm_add_source */
};

const cbm_test_conventions_t *cbm_test_conventions_cbm(void) {
    static const cbm_test_conventions_t conv = {
        .case_macro = "TEST",
        .suite_macro = "SUITE",
        .suite_fn_prefix = "suite_",
        .run_macro = "RUN_TEST",
        .suite_run_macro = "RUN_SELECTED_SUITE",
        .suite_run_perf_macro = "RUN_SELECTED_SUITE_PERF",
    };
    return &conv;
}

/* Room for one more item of `item_size` in an arena-backed array. The old
 * block stays in the arena; doubling keeps the total under twice the final
 * size. NULL = out of memory. */
static void *tm_grow(CBMArena *arena, void *items, int count, int *cap, size_t item_size) {
    if (count < *cap) {
        return items;
    }
    if (*cap < 0 || *cap > INT_MAX / TM_GROWTH) {
        return NULL;
    }
    int new_cap = *cap > 0 ? *cap * TM_GROWTH : TM_FIRST_CAP;
    if (item_size && (size_t)new_cap > SIZE_MAX / item_size) {
        return NULL;
    }
    void *grown = cbm_arena_alloc(arena, (size_t)new_cap * item_size);
    if (!grown) {
        return NULL;
    }
    if (count > 0) {
        memcpy(grown, items, (size_t)count * item_size);
    }
    *cap = new_cap;
    return grown;
}

static bool tm_copy_convention(CBMArena *arena, const char *in, const char **out) {
    *out = NULL;
    if (!in || !in[0]) {
        return true;
    }
    *out = cbm_arena_strdup(arena, in);
    return *out != NULL;
}

cbm_test_model_t *cbm_test_model_new(const cbm_test_conventions_t *conventions) {
    if (!conventions) {
        return NULL;
    }
    CBMArena arena;
    cbm_arena_init_sized(&arena, TM_ARENA_BLOCK);
    cbm_test_model_t *m = cbm_arena_calloc(&arena, sizeof(*m));
    if (!m) {
        cbm_arena_destroy(&arena);
        return NULL;
    }
    m->arena = arena; /* the model lives in its own arena */
    m->mapping.declaration_index = -1;
    bool ok =
        tm_copy_convention(&m->arena, conventions->case_macro, &m->conv.case_macro) &&
        tm_copy_convention(&m->arena, conventions->suite_macro, &m->conv.suite_macro) &&
        tm_copy_convention(&m->arena, conventions->suite_fn_prefix, &m->conv.suite_fn_prefix) &&
        tm_copy_convention(&m->arena, conventions->run_macro, &m->conv.run_macro) &&
        tm_copy_convention(&m->arena, conventions->suite_run_macro, &m->conv.suite_run_macro) &&
        tm_copy_convention(&m->arena, conventions->suite_run_perf_macro,
                           &m->conv.suite_run_perf_macro);
    if (!ok) {
        cbm_test_model_free(m);
        return NULL;
    }
    return m;
}

void cbm_test_model_free(cbm_test_model_t *m) {
    if (!m) {
        return;
    }
    CBMArena arena = m->arena; /* m itself is inside it */
    cbm_arena_destroy(&arena);
}

/* Config tokens plus the display names returned by cbm_language_name(). */
static int tm_language_kind(const char *language) {
    if (!language)
        return 0;
    if (strcmp(language, "c") == 0 || strcmp(language, "C") == 0)
        return 1;
    if (strcmp(language, "cpp") == 0 || strcmp(language, "c++") == 0 ||
        strcmp(language, "C++") == 0)
        return 2;
    if (strcmp(language, "cuda") == 0 || strcmp(language, "CUDA") == 0)
        return 3;
    return 0;
}

static bool tm_mapping_report(cbm_test_model_t *m, cbm_test_model_mapping_status_t reason,
                              int declaration, const char *file, int line) {
    m->incomplete = true;
    const cbm_test_model_mapping_issue_t *old = &m->mapping;
    int by_file = strcmp(file ? file : "", old->file ? old->file : "");
    bool earlier =
        old->reason == CBM_TEST_MODEL_MAPPING_OK || by_file < 0 ||
        (by_file == 0 &&
         (line < old->line || (line == old->line &&
                               (declaration < old->declaration_index ||
                                (declaration == old->declaration_index && reason < old->reason)))));
    if (!earlier)
        return true;
    const char *owned_file = file ? cbm_arena_strdup(&m->arena, file) : NULL;
    if (file && !owned_file)
        return false;
    m->mapping = (cbm_test_model_mapping_issue_t){
        .reason = reason, .declaration_index = declaration, .file = owned_file, .line = line};
    return true;
}

static bool tm_copy_declaration(cbm_test_model_t *m, const cbm_test_declaration_t *src,
                                cbm_test_declaration_t *dst) {
    *dst = *src;
    dst->name_args = NULL;
    if (!tm_copy_convention(&m->arena, src->language, &dst->language) ||
        !tm_copy_convention(&m->arena, src->define_macro, &dst->define_macro) ||
        !tm_copy_convention(&m->arena, src->runner_id, &dst->runner_id) ||
        !tm_copy_convention(&m->arena, src->macro, &dst->macro) ||
        !tm_copy_convention(&m->arena, src->perf_macro, &dst->perf_macro))
        return false;
    if (src->name_arg_count > 0) {
        size_t count = (size_t)src->name_arg_count;
        if (!src->name_args || count > SIZE_MAX / sizeof(int))
            return false;
        int *args = cbm_arena_alloc(&m->arena, count * sizeof(*args));
        if (!args)
            return false;
        memcpy(args, src->name_args, count * sizeof(*args));
        dst->name_args = args;
    } else if (src->name_arg_count < 0)
        return false;
    return true;
}

cbm_test_model_t *cbm_test_model_new_declarations(const cbm_test_declarations_t *declarations) {
    if (!declarations)
        return NULL;
    cbm_test_conventions_t empty = {0};
    cbm_test_model_t *m = cbm_test_model_new(&empty);
    if (!m)
        return NULL;
    m->descriptor_mode = true;
    m->mapping.declaration_index = -1;
    const cbm_test_declaration_t *items =
        cbm_test_declarations_items(declarations, &m->declaration_count);
    if (m->declaration_count < 0 || m->declaration_count > INT_MAX - 4 ||
        (size_t)m->declaration_count > SIZE_MAX / sizeof(*m->declarations))
        goto failed;
    if (m->declaration_count) {
        if (!items)
            goto failed;
        m->declarations =
            cbm_arena_calloc(&m->arena, (size_t)m->declaration_count * sizeof(*m->declarations));
        if (!m->declarations)
            goto failed;
        for (int i = 0; i < m->declaration_count; i++) {
            if (!tm_copy_declaration(m, &items[i], &m->declarations[i]))
                goto failed;
            if (!tm_language_kind(items[i].language) &&
                !tm_mapping_report(m, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_LANGUAGE, i, NULL, 0))
                goto failed;
        }
    }
    for (int i = 0; i < CBM_TEST_PRESET_COUNT; i++) {
        if (!cbm_test_declarations_preset(declarations, (cbm_test_preset_t)i, &m->presets[i]))
            goto failed;
    }
    if (m->presets[CBM_TEST_PRESET_C_CBM] &&
        !tm_copy_convention(&m->arena, "suite_", &m->conv.suite_fn_prefix))
        goto failed;
    return m;
failed:
    cbm_test_model_free(m);
    return NULL;
}

cbm_test_model_mapping_status_t cbm_test_model_mapping_status(
    const cbm_test_model_t *m, cbm_test_model_mapping_issue_t *issue) {
    cbm_test_model_mapping_issue_t result = {.reason = CBM_TEST_MODEL_MAPPING_INVALID_INPUT,
                                             .declaration_index = -1};
    if (m)
        result = m->mapping;
    if (issue)
        *issue = result;
    return result.reason;
}

const cbm_test_declaration_t *cbm_test_model_declarations(const cbm_test_model_t *m, int *count) {
    if (count)
        *count = m && m->descriptor_mode ? m->declaration_count : 0;
    return m && m->descriptor_mode ? m->declarations : NULL;
}

/* ── Lexer ───────────────────────────────────────────────────────────
 * Just enough C to tell code from comments and literals: identifiers,
 * single punctuation characters and whole preprocessor lines. Every read is
 * bounded by `end`; the text need not be terminated. */

typedef enum { TM_TOK_EOF = 0, TM_TOK_IDENT, TM_TOK_PUNCT, TM_TOK_DIRECTIVE } tm_tok_kind_t;

typedef struct {
    tm_tok_kind_t kind;
    const char *text; /* IDENT: the name. DIRECTIVE: what follows the '#'. */
    size_t len;
    char ch;  /* PUNCT */
    int line; /* 1-based line the token starts on */
} tm_tok_t;

typedef struct {
    const char *p;
    const char *end;
    int line;
    bool line_start;  /* only blanks so far on this line: a '#' opens a directive */
    bool directive;   /* lexing the text of one directive, continuations included */
    bool *incomplete; /* shared by lookahead copies; restoring a cursor cannot erase an error */
} tm_lex_t;

static bool tm_ident_start(char c) {
    return isalpha((unsigned char)c) || c == '_';
}

static bool tm_ident_char(char c) {
    return isalnum((unsigned char)c) || c == '_';
}

static void tm_lex_uncertain(tm_lex_t *lx) {
    if (lx->incomplete) {
        *lx->incomplete = true;
    }
}

/* The length of a line splice (backslash, then LF or CRLF) at p, else 0. */
static size_t tm_splice_len(const char *p, const char *end) {
    if (p >= end || *p != '\\') {
        return 0;
    }
    if (p + 1 < end && p[1] == '\n') {
        return 2;
    }
    return p + 2 < end && p[1] == '\r' && p[2] == '\n' ? 3 : 0;
}

static bool tm_blank(char c) {
    return c == ' ' || c == '\t';
}

/* Raw strings need a delimiter-aware C++ lexer, which this model does not
 * implement. Retain the old rows for diagnostics, but never certify them. */
static bool tm_raw_literal_starts(const char *p, const char *end) {
    size_t left = (size_t)(end - p);
    return (left >= 2 && p[0] == 'R' && p[1] == '"') ||
           (left >= 3 && (p[0] == 'u' || p[0] == 'U' || p[0] == 'L') && p[1] == 'R' &&
            p[2] == '"') ||
           (left >= 4 && p[0] == 'u' && p[1] == '8' && p[2] == 'R' && p[3] == '"');
}

static void tm_skip_line_comment(tm_lex_t *lx) {
    while (lx->p < lx->end && *lx->p != '\n') {
        /* Translation-phase line splicing can extend this comment. The
         * legacy lexer does not implement it, so its rows are uncertain. */
        if (tm_splice_len(lx->p, lx->end)) {
            tm_lex_uncertain(lx);
        }
        lx->p++;
    }
}

static void tm_skip_block_comment(tm_lex_t *lx) {
    lx->p += 2;
    while (lx->p < lx->end) {
        if (*lx->p == '*' && lx->p + 1 < lx->end && lx->p[1] == '/') {
            lx->p += 2;
            return;
        }
        if (*lx->p == '*' && tm_splice_len(lx->p + 1, lx->end)) {
            tm_lex_uncertain(lx); /* the splice may close the comment */
        }
        if (*lx->p == '\n') {
            lx->line++;
        }
        lx->p++;
    }
    tm_lex_uncertain(lx); /* EOF before the closing comment delimiter */
}

/* A string or character literal. It ends at its closing quote, or at the end
 * of the line when the quote is missing, so one stray quote costs one line. */
static void tm_skip_literal(tm_lex_t *lx) {
    char quote = *lx->p++;
    while (lx->p < lx->end && *lx->p != quote && *lx->p != '\n') {
        if (*lx->p == '\\' && lx->p + 1 < lx->end) {
            if (lx->p[1] == '\n') {
                lx->line++;
            }
            lx->p++;
        }
        lx->p++;
    }
    if (lx->p < lx->end && *lx->p == quote) {
        lx->p++;
    } else {
        tm_lex_uncertain(lx);
    }
}

/* The rest of a preprocessor line, backslash continuations included. */
static void tm_read_directive(tm_lex_t *lx, tm_tok_t *tok) {
    lx->p++; /* '#' */
    tok->kind = TM_TOK_DIRECTIVE;
    tok->text = lx->p;
    while (lx->p < lx->end && *lx->p != '\n') {
        if (tm_raw_literal_starts(lx->p, lx->end)) {
            tm_lex_uncertain(lx);
        }
        if (*lx->p == '/' && lx->p + 1 < lx->end && lx->p[1] == '/') {
            tm_skip_line_comment(lx);
            continue;
        }
        if (*lx->p == '"' || *lx->p == '\'') {
            tm_skip_literal(lx);
            continue;
        }
        size_t splice = tm_splice_len(lx->p, lx->end);
        if (splice) {
            /* Next to a blank a continuation only separates tokens; with
             * token characters on both sides it could join them, which the
             * re-lexing of this text (tm_directive_word) does not do. The
             * '#' precedes the text, so p[-1] is in bounds. */
            const char *after = lx->p + splice;
            if (!tm_blank(lx->p[-1]) && after < lx->end && !tm_blank(*after) && *after != '\n' &&
                *after != '\r') {
                tm_lex_uncertain(lx);
            }
            lx->line++;
            lx->p = after;
            continue;
        }
        if (*lx->p == '/' && lx->p + 1 < lx->end && lx->p[1] == '*') {
            tm_skip_block_comment(lx); /* may run on to later lines */
            continue;
        }
        lx->p++;
    }
    tok->len = (size_t)(lx->p - tok->text);
}

static void tm_next(tm_lex_t *lx, tm_tok_t *tok) {
    memset(tok, 0, sizeof(*tok));
    while (lx->p < lx->end) {
        char c = *lx->p;
        if (c == '\n') {
            lx->line++;
            lx->line_start = true;
            lx->p++;
        } else if (isspace((unsigned char)c)) {
            lx->p++;
        } else if (c == '/' && lx->p + 1 < lx->end && lx->p[1] == '/') {
            tm_skip_line_comment(lx);
        } else if (c == '/' && lx->p + 1 < lx->end && lx->p[1] == '*') {
            tm_skip_block_comment(lx);
        } else if (lx->directive && tm_splice_len(lx->p, lx->end)) {
            /* A continuation tm_read_directive already followed and judged. */
            lx->line++;
            lx->p += tm_splice_len(lx->p, lx->end);
        } else {
            break;
        }
    }
    tok->line = lx->line;
    if (lx->p >= lx->end) {
        return; /* TM_TOK_EOF */
    }
    char c = *lx->p;
    if (tm_raw_literal_starts(lx->p, lx->end)) {
        tm_lex_uncertain(lx);
    }
    if (tm_splice_len(lx->p, lx->end)) {
        tm_lex_uncertain(lx); /* a splice in plain code can join tokens */
    }
    bool line_start = lx->line_start;
    lx->line_start = false;
    if (c == '#' && line_start) {
        tm_read_directive(lx, tok);
    } else if (c == '"' || c == '\'') {
        tm_skip_literal(lx);
        tok->kind = TM_TOK_PUNCT;
        tok->ch = c;
    } else if (tm_ident_start(c)) {
        tok->kind = TM_TOK_IDENT;
        tok->text = lx->p;
        while (lx->p < lx->end && tm_ident_char(*lx->p)) {
            lx->p++;
        }
        tok->len = (size_t)(lx->p - tok->text);
    } else if (isdigit((unsigned char)c)) {
        /* A number, suffix included: `0x1F`, `10u`. Never an identifier. */
        while (lx->p < lx->end && tm_ident_char(*lx->p)) {
            lx->p++;
        }
        tok->kind = TM_TOK_PUNCT;
        tok->ch = '0';
    } else {
        tok->kind = TM_TOK_PUNCT;
        tok->ch = c;
        lx->p++;
    }
}

static bool tm_tok_is(const tm_tok_t *tok, const char *word) {
    return word && tok->kind == TM_TOK_IDENT && strlen(word) == tok->len &&
           memcmp(tok->text, word, tok->len) == 0;
}

static bool tm_tok_punct(const tm_tok_t *tok, char ch) {
    return tok->kind == TM_TOK_PUNCT && tok->ch == ch;
}

/* `( name )` right after a macro name. On a match the lexer stands behind the
 * `)`; otherwise it is left where it was. */
static bool tm_macro_arg(tm_lex_t *lx, tm_tok_t *name) {
    tm_lex_t at = *lx;
    tm_tok_t tok;
    tm_next(lx, &tok);
    if (tm_tok_punct(&tok, '(')) {
        tm_next(lx, name);
        if (name->kind == TM_TOK_IDENT) {
            tm_next(lx, &tok);
            if (tm_tok_punct(&tok, ')')) {
                return true;
            }
        }
    }
    *lx = at;
    return false;
}

/* Is the next token the `{` of a body? It is consumed on a match. */
static bool tm_body_opens(tm_lex_t *lx) {
    tm_lex_t at = *lx;
    tm_tok_t tok;
    tm_next(lx, &tok);
    if (tm_tok_punct(&tok, '{')) {
        return true;
    }
    *lx = at;
    return false;
}

/* `suite_name ( void )` after `void`: the written-out form of SUITE(name). On
 * a match `name` is the part after the prefix and the lexer stands behind the
 * `)`. */
static bool tm_suite_function(tm_lex_t *lx, const char *prefix, tm_tok_t *name) {
    if (!prefix) {
        return false;
    }
    tm_lex_t at = *lx;
    size_t prefix_len = strlen(prefix);
    tm_tok_t tok;
    tm_next(lx, name);
    if (name->kind == TM_TOK_IDENT && name->len > prefix_len &&
        memcmp(name->text, prefix, prefix_len) == 0) {
        tm_next(lx, &tok);
        if (tm_tok_punct(&tok, '(')) {
            tm_next(lx, &tok);
            if (tm_tok_is(&tok, "void")) {
                tm_next(lx, &tok);
                if (tm_tok_punct(&tok, ')')) {
                    name->text += prefix_len;
                    name->len -= prefix_len;
                    return true;
                }
            }
        }
    }
    *lx = at;
    return false;
}

/* ── Scanner ─────────────────────────────────────────────────────── */

typedef enum { TM_OPEN_NONE = 0, TM_OPEN_CASE, TM_OPEN_SUITE } tm_open_t;

typedef struct {
    int depth;  /* brace depth where the #if stood: each branch starts from it */
    bool guard; /* an include guard, which conditions nothing */
    bool seen_else;
} tm_cond_t;

typedef struct {
    cbm_test_model_t *m;
    const char *file;  /* arena copy shared by this file's entries */
    int language_kind; /* explicit descriptor input; zero for legacy/unsupported */
    tm_lex_t lx;
    int depth;
    tm_open_t open; /* the definition whose body the scanner is inside */
    int open_index;
    tm_cond_t cond[TM_COND_MAX];
    int cond_count;           /* may exceed TM_COND_MAX: the excess is counted only */
    int conditions;           /* frames that are real conditions */
    bool macro_registrations; /* a #define of this file registers tests */
    bool seen_token;          /* comments/whitespace do not prevent an initial include guard */
    bool guard_closed;        /* an ordinary include guard must enclose the remaining file */
    bool ok;
} tm_scan_t;

static const char *tm_name(tm_scan_t *sc, const tm_tok_t *tok) {
    const char *copy = cbm_arena_strndup(&sc->m->arena, tok->text, tok->len);
    sc->ok = sc->ok && copy != NULL;
    return copy;
}

static void tm_add_case(tm_scan_t *sc, const tm_tok_t *name, int line) {
    cbm_test_model_t *m = sc->m;
    cbm_test_case_t *items =
        tm_grow(&m->arena, m->cases, m->case_count, &m->case_cap, sizeof(*items));
    const char *copy = tm_name(sc, name);
    if (!items || !copy) {
        sc->ok = false;
        return;
    }
    m->cases = items;
    sc->open = TM_OPEN_CASE;
    sc->open_index = m->case_count;
    items[m->case_count++] = (cbm_test_case_t){.file = sc->file,
                                               .name = copy,
                                               .start_line = line,
                                               .end_line = line,
                                               .conditional = sc->conditions > 0};
}

static void tm_add_suite(tm_scan_t *sc, const tm_tok_t *name, int line) {
    cbm_test_model_t *m = sc->m;
    cbm_test_suite_t *items =
        tm_grow(&m->arena, m->suites, m->suite_count, &m->suite_cap, sizeof(*items));
    const char *copy = tm_name(sc, name);
    if (!items || !copy) {
        sc->ok = false;
        return;
    }
    m->suites = items;
    sc->open = TM_OPEN_SUITE;
    sc->open_index = m->suite_count;
    items[m->suite_count++] =
        (cbm_test_suite_t){.file = sc->file, .name = copy, .start_line = line, .end_line = line};
}

static void tm_add_registration(tm_scan_t *sc, const tm_tok_t *test, int line) {
    cbm_test_model_t *m = sc->m;
    cbm_test_registration_t *items =
        tm_grow(&m->arena, m->regs, m->reg_count, &m->reg_cap, sizeof(*items));
    const char *copy = tm_name(sc, test);
    if (!items || !copy) {
        sc->ok = false;
        return;
    }
    m->regs = items;
    items[m->reg_count++] = (cbm_test_registration_t){.file = sc->file,
                                                      .suite = m->suites[sc->open_index].name,
                                                      .test = copy,
                                                      .line = line,
                                                      .conditional = sc->conditions > 0};
}

static void tm_add_runner_suite(tm_scan_t *sc, const tm_tok_t *name, bool perf) {
    cbm_test_model_t *m = sc->m;
    cbm_test_runner_suite_t *items =
        tm_grow(&m->arena, m->runner, m->runner_count, &m->runner_cap, sizeof(*items));
    const char *copy = tm_name(sc, name);
    if (!items || !copy) {
        sc->ok = false;
        return;
    }
    m->runner = items;
    items[m->runner_count++] =
        (cbm_test_runner_suite_t){.name = copy, .perf = perf, .file = sc->file, .line = name->line};
}

/* Virtual preset records are immutable. Original configured records are kept
 * separately and returned unchanged by cbm_test_model_declarations(). */
static const int tm_cbm_name_arg = 0;
static const cbm_test_declaration_t tm_cbm_rules[] = {
    {.language = "c",
     .role = CBM_TEST_DECL_CASE,
     .define_macro = "TEST",
     .name_args = &tm_cbm_name_arg,
     .name_arg_count = 1,
     .runner_id = "{suite}:{name}",
     .test_arg = -1,
     .suite_arg = -1},
    {.language = "c",
     .role = CBM_TEST_DECL_SUITE,
     .define_macro = "SUITE",
     .name_args = &tm_cbm_name_arg,
     .name_arg_count = 1,
     .test_arg = -1,
     .suite_arg = -1},
    {.language = "c",
     .role = CBM_TEST_DECL_REGISTRATION,
     .macro = "RUN_TEST",
     .test_arg = 0,
     .suite_arg = -1},
    {.language = "c",
     .role = CBM_TEST_DECL_SUITE_REGISTRATION,
     .macro = "RUN_SELECTED_SUITE",
     .perf_macro = "RUN_SELECTED_SUITE_PERF",
     .test_arg = -1,
     .suite_arg = 0},
};

static bool tm_cbm_active(const tm_scan_t *sc) {
    return sc->language_kind == 1 && sc->m->presets[CBM_TEST_PRESET_C_CBM];
}

static int tm_rule_count(const tm_scan_t *sc) {
    return sc->m->declaration_count + (tm_cbm_active(sc) ? 4 : 0);
}

static const cbm_test_declaration_t *tm_rule(const tm_scan_t *sc, int index,
                                             int *declaration_index) {
    if (index < sc->m->declaration_count) {
        *declaration_index = index;
        return &sc->m->declarations[index];
    }
    *declaration_index = -1;
    return &tm_cbm_rules[index - sc->m->declaration_count];
}

static bool tm_rule_matches(const tm_scan_t *sc, const cbm_test_declaration_t *rule,
                            const tm_tok_t *tok, bool *perf) {
    *perf = false;
    if (tm_language_kind(rule->language) != sc->language_kind)
        return false;
    if (rule->role == CBM_TEST_DECL_CASE || rule->role == CBM_TEST_DECL_SUITE)
        return tm_tok_is(tok, rule->define_macro);
    if (rule->role == CBM_TEST_DECL_SUITE_REGISTRATION && tm_tok_is(tok, rule->perf_macro)) {
        *perf = true;
        return true;
    }
    return tm_tok_is(tok, rule->macro);
}

static void tm_scan_mapping(tm_scan_t *sc, cbm_test_model_mapping_status_t reason, int declaration,
                            int line) {
    if (!tm_mapping_report(sc->m, reason, declaration, sc->file, line))
        sc->ok = false;
}

static bool tm_macro_spelling(const char *name) {
    if (!name || !tm_ident_start(*name))
        return false;
    for (const char *p = name + 1; *p; p++) {
        if (!tm_ident_char(*p))
            return false;
    }
    return true;
}

static void tm_descriptor_preflight(tm_scan_t *sc) {
    if (!sc->language_kind) {
        tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_LANGUAGE, -1, 0);
        return;
    }
    /* The native gtest preset has its own suite/runner semantics. Reading
     * explicit C++ records alone cannot discharge that enabled contract. */
    if (sc->language_kind != 1 && sc->m->presets[CBM_TEST_PRESET_GTEST])
        tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_PRESET, -1, 0);
    for (int i = 0; i < sc->m->declaration_count && sc->ok; i++) {
        const cbm_test_declaration_t *rule = &sc->m->declarations[i];
        if (tm_language_kind(rule->language) != sc->language_kind)
            continue;
        if (rule->role == CBM_TEST_DECL_SUITE_REGISTRATION && rule->perf_macro &&
            strcmp(rule->macro, rule->perf_macro) == 0)
            tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_AMBIGUOUS, i, 0);
        bool definition = rule->role == CBM_TEST_DECL_CASE || rule->role == CBM_TEST_DECL_SUITE;
        if (!tm_macro_spelling(definition ? rule->define_macro : rule->macro) ||
            (rule->perf_macro && !tm_macro_spelling(rule->perf_macro)))
            tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_FORM, i, 0);
        if ((rule->role == CBM_TEST_DECL_CASE || rule->role == CBM_TEST_DECL_SUITE) &&
            (rule->name_arg_count != 1 || !rule->name_args || rule->name_args[0] < 0))
            tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_NAME, i, 0);
        if (rule->role == CBM_TEST_DECL_CASE &&
            (!rule->runner_id || strcmp(rule->runner_id, "{suite}:{name}") != 0))
            tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_TEMPLATE, i, 0);
    }
}

typedef struct {
    tm_tok_t first;
    bool has_token;
    bool single_identifier;
} tm_argument_t;

typedef struct {
    CBMArena arena;      /* temporary invocation owner, independent of the model arena */
    tm_argument_t *args; /* grows from actual arity, never from configured argument indexes */
    int count;
    int cap;
    tm_lex_t after;
} tm_invocation_t;

static bool tm_argument_append(tm_invocation_t *invocation, const tm_argument_t *arg) {
    if (invocation->count == invocation->cap) {
        if (invocation->cap > INT_MAX / 2)
            return false;
        int cap = invocation->cap ? invocation->cap * 2 : 8;
        if ((size_t)cap > SIZE_MAX / sizeof(*invocation->args))
            return false;
        tm_argument_t *args = cbm_arena_alloc(&invocation->arena, (size_t)cap * sizeof(*args));
        if (!args)
            return false;
        if (invocation->count)
            memcpy(args, invocation->args, (size_t)invocation->count * sizeof(*args));
        invocation->args = args;
        invocation->cap = cap;
    }
    invocation->args[invocation->count++] = *arg;
    return true;
}

/* C macro argument commas are shielded by parentheses, not by brackets,
 * braces or template angle brackets. The lexer handles comments and ordinary
 * literals as units. Unsupported raw/spliced text latches the shared flag.
 * Return 1=parsed, 0=malformed, -1=allocation/counter failure. */
static int tm_invocation_parse(tm_scan_t *sc, tm_lex_t at, tm_invocation_t *invocation) {
    tm_tok_t tok;
    tm_next(&at, &tok);
    if (!tm_tok_punct(&tok, '('))
        return 0;
    int depth = 0;
    tm_argument_t arg = {0};
    for (;;) {
        tm_next(&at, &tok);
        if (tok.kind == TM_TOK_EOF || tok.kind == TM_TOK_DIRECTIVE)
            return 0;
        if (tm_tok_punct(&tok, ')') && depth == 0) {
            if ((arg.has_token || invocation->count) && !tm_argument_append(invocation, &arg))
                return -1;
            invocation->after = at;
            return 1;
        }
        if (tm_tok_punct(&tok, ',') && depth == 0) {
            if (!tm_argument_append(invocation, &arg))
                return -1;
            memset(&arg, 0, sizeof(arg));
            continue;
        }
        /* Consuming an outer invocation must not hide another configured
         * invocation in an unselected argument. We cannot infer expansion or
         * execution of that inner call, so these rows cannot justify omission. */
        if (tok.kind == TM_TOK_IDENT) {
            for (int i = 0; i < tm_rule_count(sc); i++) {
                int declaration;
                const cbm_test_declaration_t *rule = tm_rule(sc, i, &declaration);
                bool perf;
                if (!tm_rule_matches(sc, rule, &tok, &perf))
                    continue;
                tm_lex_t ahead = at;
                tm_tok_t next;
                tm_next(&ahead, &next);
                if (tm_tok_punct(&next, '('))
                    tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_FORM, declaration,
                                    tok.line);
            }
        }
        if (!arg.has_token) {
            arg.first = tok;
            arg.has_token = true;
            arg.single_identifier = tok.kind == TM_TOK_IDENT;
        } else {
            arg.single_identifier = false;
        }
        if (tm_tok_punct(&tok, '(')) {
            if (depth == INT_MAX)
                return -1;
            depth++;
        } else if (tm_tok_punct(&tok, ')')) {
            depth--;
        }
    }
}

/* Inspect all rules against one untouched token/invocation snapshot. Exact
 * duplicates are idempotent; disagreement never acquires first-match priority. */
static bool tm_descriptor_ident(tm_scan_t *sc, const tm_tok_t *tok) {
    int first = -2;
    for (int i = 0; i < tm_rule_count(sc); i++) {
        int declaration;
        const cbm_test_declaration_t *rule = tm_rule(sc, i, &declaration);
        bool perf;
        if (tm_rule_matches(sc, rule, tok, &perf)) {
            first = declaration;
            break;
        }
    }
    if (first == -2)
        return false;
    /* A function-like macro name not followed by '(' is not an invocation:
     * the preprocessor leaves it alone (an array or variable that happens to
     * carry the name), so it defines and registers nothing. An opened '('
     * that does not close stays a malformed invocation below. */
    tm_lex_t peek = sc->lx;
    tm_tok_t next;
    tm_next(&peek, &next);
    if (!tm_tok_punct(&next, '('))
        return false;
    tm_invocation_t invocation = {0};
    cbm_arena_init_sized(&invocation.arena, TM_INVOCATION_ARENA_BLOCK);
    int parsed = tm_invocation_parse(sc, sc->lx, &invocation);
    if (parsed != 1) {
        if (parsed < 0)
            sc->ok = false;
        tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_MALFORMED_INVOCATION, first, tok->line);
        cbm_arena_destroy(&invocation.arena);
        return true;
    }
    const cbm_test_declaration_t *chosen = NULL;
    int chosen_index = -1, position = -1;
    bool chosen_perf = false, valid = true;
    for (int i = 0; i < tm_rule_count(sc); i++) {
        int declaration;
        const cbm_test_declaration_t *rule = tm_rule(sc, i, &declaration);
        bool perf;
        if (!tm_rule_matches(sc, rule, tok, &perf))
            continue;
        int argument = rule->role == CBM_TEST_DECL_REGISTRATION ? rule->test_arg : rule->suite_arg;
        if (rule->role == CBM_TEST_DECL_CASE || rule->role == CBM_TEST_DECL_SUITE) {
            if (rule->name_arg_count != 1 || !rule->name_args) {
                tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_NAME, declaration,
                                tok->line);
                valid = false;
                continue;
            }
            argument = rule->name_args[0];
        }
        if (rule->role == CBM_TEST_DECL_CASE &&
            (!rule->runner_id || strcmp(rule->runner_id, "{suite}:{name}") != 0)) {
            tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_TEMPLATE, declaration,
                            tok->line);
            valid = false;
        }
        if (argument < 0 || argument >= invocation.count) {
            tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_MISSING_ARGUMENT, declaration, tok->line);
            valid = false;
            continue;
        }
        if (!invocation.args[argument].single_identifier) {
            tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_ARGUMENT, declaration,
                            tok->line);
            valid = false;
        }
        if (chosen && (chosen->role != rule->role || position != argument || chosen_perf != perf)) {
            tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_AMBIGUOUS, declaration, tok->line);
            valid = false;
        }
        if (!chosen) {
            chosen = rule;
            chosen_index = declaration;
            position = argument;
        }
        chosen_perf = chosen_perf || perf;
    }
    if (!valid || !chosen || !sc->ok) {
        cbm_arena_destroy(&invocation.arena);
        return true;
    }
    tm_tok_t name = invocation.args[position].first;
    if (chosen->role == CBM_TEST_DECL_CASE || chosen->role == CBM_TEST_DECL_SUITE) {
        tm_lex_t after = invocation.after;
        if (sc->depth || sc->open != TM_OPEN_NONE || !tm_body_opens(&after)) {
            tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_FORM, chosen_index, tok->line);
        } else {
            sc->lx = after;
            sc->depth = 1;
            if (chosen->role == CBM_TEST_DECL_CASE)
                tm_add_case(sc, &name, tok->line);
            else
                tm_add_suite(sc, &name, tok->line);
        }
    } else if (chosen->role == CBM_TEST_DECL_REGISTRATION) {
        if (sc->open != TM_OPEN_SUITE || sc->depth == 0)
            tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_FORM, chosen_index, tok->line);
        else {
            sc->lx = invocation.after;
            tm_add_registration(sc, &name, tok->line);
        }
    } else {
        sc->lx = invocation.after;
        tm_add_runner_suite(sc, &name, chosen_perf);
    }
    cbm_arena_destroy(&invocation.arena);
    return true;
}

static void tm_descriptor_macro_body(tm_scan_t *sc, const tm_tok_t *word) {
    for (int i = 0; i < tm_rule_count(sc); i++) {
        int declaration;
        const cbm_test_declaration_t *rule = tm_rule(sc, i, &declaration);
        bool perf;
        if (!tm_rule_matches(sc, rule, word, &perf))
            continue;
        if (rule->role == CBM_TEST_DECL_REGISTRATION)
            sc->macro_registrations = true;
        else
            tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_FORM, declaration, word->line);
    }
}

/* The first word of a directive and what follows it. */
static void tm_directive_word(const tm_tok_t *tok, tm_tok_t *word, tm_lex_t *rest,
                              bool *incomplete) {
    *rest = (tm_lex_t){.p = tok->text,
                       .end = tok->text + tok->len,
                       .line = tok->line,
                       .directive = true,
                       .incomplete = incomplete};
    tm_next(rest, word);
}

/* `#ifndef X` directly followed by `#define X` is an include guard. */
static bool tm_is_include_guard(const tm_scan_t *sc, const tm_tok_t *guarded) {
    tm_lex_t ahead = sc->lx;
    tm_tok_t tok;
    tm_next(&ahead, &tok);
    if (tok.kind != TM_TOK_DIRECTIVE) {
        return false;
    }
    tm_tok_t word;
    tm_lex_t rest;
    tm_directive_word(&tok, &word, &rest, sc->lx.incomplete);
    if (!tm_tok_is(&word, "define")) {
        return false;
    }
    tm_next(&rest, &word);
    return word.kind == TM_TOK_IDENT && word.len == guarded->len &&
           memcmp(word.text, guarded->text, word.len) == 0;
}

static void tm_directive(tm_scan_t *sc, const tm_tok_t *tok) {
    tm_tok_t word;
    tm_lex_t rest;
    tm_directive_word(tok, &word, &rest, sc->lx.incomplete);
    if (tm_tok_is(&word, "if") || tm_tok_is(&word, "ifdef") || tm_tok_is(&word, "ifndef")) {
        bool guard = false;
        tm_tok_t condition;
        tm_next(&rest, &condition);
        if (condition.kind == TM_TOK_EOF ||
            (!tm_tok_is(&word, "if") && condition.kind != TM_TOK_IDENT)) {
            sc->m->incomplete = true;
        }
        if (tm_tok_is(&word, "ifndef")) {
            /* Only a guard that starts the file at outermost scope conditions
             * nothing. The same pair anywhere else is the define-if-undefined
             * idiom: an ordinary condition, so what it encloses is conditional. */
            guard = condition.kind == TM_TOK_IDENT && !sc->seen_token && !sc->depth &&
                    sc->open == TM_OPEN_NONE && !sc->cond_count &&
                    tm_is_include_guard(sc, &condition);
        }
        if (sc->cond_count < TM_COND_MAX) {
            sc->cond[sc->cond_count] = (tm_cond_t){.depth = sc->depth, .guard = guard};
        } else {
            sc->m->incomplete = true;
        }
        sc->cond_count++;
        sc->conditions += guard ? 0 : 1;
    } else if (tm_tok_is(&word, "elif") || tm_tok_is(&word, "else")) {
        /* Every branch starts from the brace depth the #if stood at. */
        if (tm_tok_is(&word, "elif")) {
            tm_tok_t expression;
            tm_next(&rest, &expression);
            if (expression.kind == TM_TOK_EOF) {
                sc->m->incomplete = true;
            }
        }
        if (sc->cond_count > 0 && sc->cond_count <= TM_COND_MAX) {
            tm_cond_t *condition = &sc->cond[sc->cond_count - 1];
            if (condition->seen_else || condition->guard) {
                /* A guard-like conditional with alternatives is not an
                 * unconditional include guard. Its old rows are diagnostic. */
                sc->m->incomplete = true;
            }
            condition->seen_else = tm_tok_is(&word, "else");
            sc->depth = condition->depth;
        } else {
            sc->m->incomplete = true;
        }
    } else if (tm_tok_is(&word, "endif")) {
        if (sc->cond_count > 0) {
            sc->cond_count--;
            bool guard = sc->cond_count < TM_COND_MAX && sc->cond[sc->cond_count].guard;
            sc->conditions -= guard ? 0 : 1;
            if (guard && sc->cond_count == 0) {
                sc->guard_closed = true;
            }
        } else {
            sc->m->incomplete = true;
        }
    } else if (tm_tok_is(&word, "define")) {
        /* A macro whose body registers a test: each use of it registers one
         * the scanner cannot name. The macro's own name is the first word. */
        tm_next(&rest, &word);
        for (tm_next(&rest, &word); word.kind != TM_TOK_EOF; tm_next(&rest, &word)) {
            if (sc->m->descriptor_mode) {
                tm_descriptor_macro_body(sc, &word);
                continue;
            }
            if (tm_tok_is(&word, sc->m->conv.run_macro)) {
                sc->macro_registrations = true;
            }
            if (tm_tok_is(&word, sc->m->conv.case_macro) ||
                tm_tok_is(&word, sc->m->conv.suite_macro) ||
                tm_tok_is(&word, sc->m->conv.suite_run_macro) ||
                tm_tok_is(&word, sc->m->conv.suite_run_perf_macro)) {
                sc->m->incomplete = true; /* hidden definitions or runner membership */
            }
        }
    }
}

static void tm_close_brace(tm_scan_t *sc, int line) {
    if (sc->depth == 0) {
        sc->m->incomplete = true;
        return; /* keep existing rows, but do not certify a stray closing brace */
    }
    sc->depth--;
    if (sc->depth > 0 || sc->open == TM_OPEN_NONE) {
        return;
    }
    if (sc->open == TM_OPEN_CASE) {
        sc->m->cases[sc->open_index].end_line = line;
    } else {
        sc->m->suites[sc->open_index].end_line = line;
    }
    sc->open = TM_OPEN_NONE;
}

/* A definition at file scope: TEST(name) {, SUITE(name) { or the written-out
 * suite function. The opening brace is consumed with it. */
static void tm_file_scope_ident(tm_scan_t *sc, const tm_tok_t *tok) {
    const cbm_test_conventions_t *conv = &sc->m->conv;
    tm_lex_t at = sc->lx;
    tm_tok_t name;
    bool is_case = tm_tok_is(tok, conv->case_macro);
    bool is_suite = !is_case && tm_tok_is(tok, conv->suite_macro);
    bool macro_definition = is_case || is_suite;
    bool written_suite = false;
    bool matched = false;
    if (macro_definition) {
        matched = tm_macro_arg(&sc->lx, &name);
    } else if (tm_tok_is(tok, "void")) {
        tm_lex_t ahead = sc->lx;
        tm_tok_t candidate;
        tm_next(&ahead, &candidate);
        size_t prefix_len = conv->suite_fn_prefix ? strlen(conv->suite_fn_prefix) : 0;
        written_suite = prefix_len && candidate.kind == TM_TOK_IDENT &&
                        candidate.len >= prefix_len &&
                        memcmp(candidate.text, conv->suite_fn_prefix, prefix_len) == 0;
        is_suite = tm_suite_function(&sc->lx, conv->suite_fn_prefix, &name);
        matched = is_suite;
    }
    if (!matched) {
        if (macro_definition || written_suite) {
            sc->m->incomplete = true;
        }
        sc->lx = at;
        return;
    }
    if (!tm_body_opens(&sc->lx)) {
        /* The written-out suite form also appears as a forward declaration.
         * Configured definition macros, however, must supply a body. */
        tm_lex_t ahead = sc->lx;
        tm_tok_t next;
        tm_next(&ahead, &next);
        if (macro_definition || !tm_tok_punct(&next, ';')) {
            sc->m->incomplete = true;
        }
        sc->lx = at;
        return;
    }
    if (sc->open != TM_OPEN_NONE) {
        sc->m->incomplete = true; /* a conditional branch replaced an unfinished definition */
    }
    sc->depth = 1;
    if (is_case) {
        tm_add_case(sc, &name, tok->line);
    } else {
        tm_add_suite(sc, &name, tok->line);
    }
}

static void tm_ident(tm_scan_t *sc, const tm_tok_t *tok) {
    const cbm_test_conventions_t *conv = &sc->m->conv;
    tm_tok_t name;
    if (tm_tok_is(tok, "namespace")) {
        sc->m->incomplete = true; /* file-scope-only model cannot map namespace identities */
    }
    if (sc->m->descriptor_mode) {
        if (tm_descriptor_ident(sc, tok))
            return;
        if (tm_cbm_active(sc) && tm_tok_is(tok, "void")) {
            if (sc->depth == 0) {
                tm_file_scope_ident(sc, tok);
            } else {
                tm_lex_t ahead = sc->lx;
                if (tm_suite_function(&ahead, conv->suite_fn_prefix, &name) &&
                    tm_body_opens(&ahead)) {
                    tm_scan_mapping(sc, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_FORM, -1, tok->line);
                }
            }
        }
        return;
    }
    bool perf = tm_tok_is(tok, conv->suite_run_perf_macro);
    if (perf || tm_tok_is(tok, conv->suite_run_macro)) {
        if (tm_macro_arg(&sc->lx, &name)) {
            tm_add_runner_suite(sc, &name, perf);
        } else {
            sc->m->incomplete = true;
        }
        return;
    }
    if (sc->depth == 0) {
        if (tm_tok_is(tok, conv->run_macro)) {
            sc->m->incomplete = true;
        }
        tm_file_scope_ident(sc, tok);
        return;
    }
    if (tm_tok_is(tok, conv->case_macro) || tm_tok_is(tok, conv->suite_macro)) {
        sc->m->incomplete = true; /* configured definitions below file scope are unsupported */
    }
    if (tm_tok_is(tok, "void")) {
        tm_lex_t ahead = sc->lx;
        if (tm_suite_function(&ahead, conv->suite_fn_prefix, &name) && tm_body_opens(&ahead)) {
            sc->m->incomplete = true;
        }
    }
    if (tm_tok_is(tok, conv->run_macro)) {
        if (sc->open == TM_OPEN_SUITE && tm_macro_arg(&sc->lx, &name)) {
            tm_add_registration(sc, &name, tok->line);
        } else {
            sc->m->incomplete = true;
        }
    }
}

static bool tm_scan_source(cbm_test_model_t *m, const char *file, const char *text, size_t len,
                           int language_kind) {
    if (!text) {
        text = ""; /* (NULL, 0) is empty input, never NULL pointer arithmetic */
    }
    if (memchr(text, '\0', len)) {
        m->incomplete = true;
    }
    /* Translation-phase line splicing is followed where the lexer reads it
     * correctly (a directive's continuation lines, an escaped newline in a
     * literal); a splice it cannot follow (in plain code, or extending a line
     * comment) marks the file uncertain where it is met. */
    tm_scan_t sc = {.m = m, .language_kind = language_kind, .ok = true};
    int first_suite = m->suite_count;
    sc.file = cbm_arena_strdup(&m->arena, file);
    if (!sc.file) {
        m->incomplete = true;
        return false;
    }
    if (m->descriptor_mode) {
        tm_descriptor_preflight(&sc);
        if (!sc.ok)
            return false;
        if (!sc.language_kind)
            return true; /* preserve uncertainty; do not pretend to parse it */
    }
    sc.lx = (tm_lex_t){
        .p = text, .end = text + len, .line = 1, .line_start = true, .incomplete = &m->incomplete};
    tm_tok_t tok;
    for (tm_next(&sc.lx, &tok); sc.ok && tok.kind != TM_TOK_EOF; tm_next(&sc.lx, &tok)) {
        if (sc.guard_closed) {
            m->incomplete = true; /* significant input after the supposed file guard */
        }
        if (tok.kind == TM_TOK_DIRECTIVE) {
            tm_directive(&sc, &tok);
        } else if (tok.kind == TM_TOK_IDENT) {
            tm_ident(&sc, &tok);
        } else if (tok.ch == '{') {
            sc.depth++;
        } else if (tok.ch == '}') {
            tm_close_brace(&sc, tok.line);
        }
        sc.seen_token = true;
    }
    if (!sc.ok || sc.depth || sc.open != TM_OPEN_NONE || sc.cond_count) {
        m->incomplete = true;
    }
    if (sc.open == TM_OPEN_CASE) {
        m->cases[sc.open_index].end_line = sc.lx.line; /* body open at end of file */
    } else if (sc.open == TM_OPEN_SUITE) {
        m->suites[sc.open_index].end_line = sc.lx.line;
    }
    for (int i = first_suite; sc.macro_registrations && i < m->suite_count; i++) {
        m->suites[i].macro_registrations = true;
    }
    return sc.ok;
}

/* Scan one file and scope what it left uncertain. The uncertainty of a file
 * that defines its own suite stays with those suites, which then run whole;
 * a file that registers runner suites or defines no suite has nothing to
 * scope it to, and a failure is never scoped. */
static bool tm_add_source(cbm_test_model_t *m, const char *file, const char *text, size_t len,
                          int language_kind) {
    if (!m || m->finished || !file || (!text && len > 0)) {
        if (m) {
            m->incomplete = true;
        }
        return false;
    }
    bool before = m->incomplete;
    int first_suite = m->suite_count;
    int first_runner = m->runner_count;
    m->incomplete = false;
    bool ok = tm_scan_source(m, file, text, len, language_kind);
    bool uncertain = m->incomplete;
    m->incomplete = before;
    if (!ok) {
        m->incomplete = true;
        return false;
    }
    if (uncertain) {
        bool own_suite = m->suite_count > first_suite;
        bool registers_runner = m->runner_count > first_runner;
        if (!own_suite || registers_runner) {
            m->incomplete = true;
        } else {
            for (int i = first_suite; i < m->suite_count; i++) {
                m->suites[i].uncertain = true;
            }
            m->scoped_uncertain = true;
        }
    }
    return true;
}

bool cbm_test_model_add_source(cbm_test_model_t *m, const char *file, const char *text,
                               size_t len) {
    if (m && m->descriptor_mode) {
        (void)tm_mapping_report(m, CBM_TEST_MODEL_MAPPING_INVALID_INPUT, -1, file, 0);
        return false;
    }
    return tm_add_source(m, file, text, len, 0);
}

bool cbm_test_model_add_source_language(cbm_test_model_t *m, const char *file, const char *language,
                                        const char *text, size_t len) {
    if (!m || !m->descriptor_mode || m->finished || !file || !language || !language[0] ||
        (!text && len > 0)) {
        if (m)
            (void)tm_mapping_report(m, CBM_TEST_MODEL_MAPPING_INVALID_INPUT, -1, file, 0);
        return false;
    }
    return tm_add_source(m, file, text, len, tm_language_kind(language));
}

/* ── Finish: canonical order and links ───────────────────────────── */

static int tm_cmp_text(const char *a, const char *b) {
    return strcmp(a ? a : "", b ? b : "");
}

static int tm_cmp_case(const void *left, const void *right) {
    const cbm_test_case_t *a = left;
    const cbm_test_case_t *b = right;
    int by_file = tm_cmp_text(a->file, b->file);
    if (by_file != 0) {
        return by_file;
    }
    return a->start_line != b->start_line ? (a->start_line < b->start_line ? -1 : 1)
                                          : tm_cmp_text(a->name, b->name);
}

static int tm_cmp_suite(const void *left, const void *right) {
    const cbm_test_suite_t *a = left;
    const cbm_test_suite_t *b = right;
    int by_file = tm_cmp_text(a->file, b->file);
    if (by_file != 0) {
        return by_file;
    }
    return a->start_line != b->start_line ? (a->start_line < b->start_line ? -1 : 1)
                                          : tm_cmp_text(a->name, b->name);
}

static int tm_cmp_registration(const void *left, const void *right) {
    const cbm_test_registration_t *a = left;
    const cbm_test_registration_t *b = right;
    int by_file = tm_cmp_text(a->file, b->file);
    if (by_file != 0) {
        return by_file;
    }
    return a->line != b->line ? (a->line < b->line ? -1 : 1) : tm_cmp_text(a->test, b->test);
}

static int tm_cmp_runner(const void *left, const void *right) {
    const cbm_test_runner_suite_t *a = left;
    const cbm_test_runner_suite_t *b = right;
    return tm_cmp_text(a->name, b->name);
}

/* (file, name): the key a registration resolves by. */
static int tm_cmp_case_key(const void *left, const void *right) {
    const cbm_test_case_t *const *a = left;
    const cbm_test_case_t *const *b = right;
    int by_file = tm_cmp_text((*a)->file, (*b)->file);
    return by_file != 0 ? by_file : tm_cmp_text((*a)->name, (*b)->name);
}

static bool tm_link_registrations(cbm_test_model_t *m) {
    if (m->reg_count == 0 || m->case_count == 0) {
        return true;
    }
    const cbm_test_case_t **index =
        cbm_arena_alloc(&m->arena, (size_t)m->case_count * sizeof(*index));
    if (!index) {
        return false;
    }
    for (int i = 0; i < m->case_count; i++) {
        index[i] = &m->cases[i];
    }
    qsort(index, (size_t)m->case_count, sizeof(*index), tm_cmp_case_key);
    for (int i = 0; i < m->reg_count; i++) {
        cbm_test_case_t key = {.file = m->regs[i].file, .name = m->regs[i].test};
        const cbm_test_case_t *key_ptr = &key;
        m->regs[i].resolved = bsearch(&key_ptr, index, (size_t)m->case_count, sizeof(*index),
                                      tm_cmp_case_key) != NULL;
    }
    return true;
}

bool cbm_test_model_finish(cbm_test_model_t *m) {
    if (!m || m->finished) {
        return false;
    }
    if (m->case_count > 1) {
        qsort(m->cases, (size_t)m->case_count, sizeof(*m->cases), tm_cmp_case);
    }
    if (m->suite_count > 1) {
        qsort(m->suites, (size_t)m->suite_count, sizeof(*m->suites), tm_cmp_suite);
    }
    if (m->reg_count > 1) {
        qsort(m->regs, (size_t)m->reg_count, sizeof(*m->regs), tm_cmp_registration);
    }
    if (m->runner_count > 1) {
        qsort(m->runner, (size_t)m->runner_count, sizeof(*m->runner), tm_cmp_runner);
        /* One entry per suite; it is a perf suite when any site says so. */
        int kept = 0;
        for (int i = 1; i < m->runner_count; i++) {
            if (strcmp(m->runner[kept].name, m->runner[i].name) == 0) {
                m->runner[kept].perf = m->runner[kept].perf || m->runner[i].perf;
            } else {
                m->runner[++kept] = m->runner[i];
            }
        }
        m->runner_count = kept + 1;
    }
    if (!tm_link_registrations(m)) {
        m->incomplete = true;
        return false;
    }
    m->finished = true;
    return true;
}

/* ── Accessors ───────────────────────────────────────────────────── */

bool cbm_test_model_complete(const cbm_test_model_t *m) {
    return m && m->finished && !m->incomplete && !m->scoped_uncertain;
}

bool cbm_test_model_narrowable(const cbm_test_model_t *m) {
    return m && m->finished && !m->incomplete;
}

const cbm_test_case_t *cbm_test_model_cases(const cbm_test_model_t *m, int *count) {
    bool ready = m && m->finished;
    if (count) {
        *count = ready ? m->case_count : 0;
    }
    return ready ? m->cases : NULL;
}

const cbm_test_suite_t *cbm_test_model_suites(const cbm_test_model_t *m, int *count) {
    bool ready = m && m->finished;
    if (count) {
        *count = ready ? m->suite_count : 0;
    }
    return ready ? m->suites : NULL;
}

const cbm_test_registration_t *cbm_test_model_registrations(const cbm_test_model_t *m, int *count) {
    bool ready = m && m->finished;
    if (count) {
        *count = ready ? m->reg_count : 0;
    }
    return ready ? m->regs : NULL;
}

const cbm_test_runner_suite_t *cbm_test_model_runner_suites(const cbm_test_model_t *m, int *count) {
    bool ready = m && m->finished;
    if (count) {
        *count = ready ? m->runner_count : 0;
    }
    return ready ? m->runner : NULL;
}
