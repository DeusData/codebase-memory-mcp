/*
 * test_impact_source.h — text facts about the C-family sources of one snapshot.
 *
 * Some seeding rules of the test-impact engine (smart-ci-design 5.1 and 5.8)
 * ask questions the graph cannot answer: which files include a header,
 * directly or not; where an identifier is written; which macros the headers
 * define and what their bodies name; what a changed line declares. This index
 * answers them from the exact bytes of the snapshot's C-family files.
 *
 * It ports the measured reference (scratchpad tia3/replay4.py: build_occ,
 * include_closure, header_macros and its line patterns) rule for rule,
 * including where the rules over-approximate: identifiers inside strings and
 * block comments count as occurrences. Every rule here can only add seeds, so
 * an approximation selects more tests, never fewer. Identifiers are ASCII
 * ([A-Za-z_][A-Za-z0-9_]*); a byte outside ASCII ends one.
 */
#ifndef CBM_TEST_IMPACT_SOURCE_H
#define CBM_TEST_IMPACT_SOURCE_H

#include <stdbool.h>
#include <stddef.h>

/* ── Line patterns ─────────────────────────────────────────────────
 * Each takes one line without its newline (a trailing '\r' is blank space)
 * and reports the declared name as an offset and length into the line. */

typedef struct {
    size_t start;
    size_t len;
} cbm_ti_span_t;

/* Blank, or a comment line: starts with `//`, `/ *` or `*`, or ends with the
 * close of a block comment. */
bool cbm_ti_line_is_comment(const char *line, size_t len);
/* `#if`, `#ifdef`, `#ifndef`, `#elif`, `#else`, `#endif`, `#undef` or
 * `#include`. */
bool cbm_ti_line_is_pp_condition(const char *line, size_t len);
/* `#define NAME`; *body is where the replacement text starts (after the
 * parameter list's '(' for a function-like macro, as the reference reads it). */
bool cbm_ti_match_define(const char *line, size_t len, cbm_ti_span_t *name, size_t *body);
/* The name a `typedef ... NAME [N];` introduces, anywhere on the line. */
bool cbm_ti_match_typedef(const char *line, size_t len, cbm_ti_span_t *name);
/* `[typedef] struct|union|enum NAME {` at the start of the line. */
bool cbm_ti_match_tag_declaration(const char *line, size_t len, cbm_ti_span_t *name);
/* `extern ... NAME [N];` at the start of the line. */
bool cbm_ti_match_extern(const char *line, size_t len, cbm_ti_span_t *name);
/* A function name followed by '(' on a line that starts with an identifier
 * character, not the first word: `int f(`, `static T *g (`. */
bool cbm_ti_match_prototype(const char *line, size_t len, cbm_ti_span_t *name);
/* The name a changed header line declares, trying define, typedef, tag
 * declaration, extern and prototype in that order (replay4 header_seeds). */
bool cbm_ti_match_declaration(const char *line, size_t len, cbm_ti_span_t *name);
/* `#include "PATH"`: the quoted path. */
bool cbm_ti_match_include(const char *line, size_t len, cbm_ti_span_t *path);
bool cbm_ti_is_keyword(const char *word, size_t len);

/* ── The index ─────────────────────────────────────────────────────── */

typedef struct cbm_ti_source cbm_ti_source_t;

typedef struct {
    int file; /* index in path order */
    int line; /* 1-based */
} cbm_ti_place_t;

typedef struct {
    const char *name;
    int file;
    /* Identifiers of the replacement text, keywords and the macro's own name
     * removed. Sorted, unique. */
    const char *const *tokens;
    int token_count;
    /* `X ##` and `## X` operands: a changed name that starts with a prefix or
     * ends with a suffix may be what the paste builds. */
    const char *const *paste_prefixes;
    int paste_prefix_count;
    const char *const *paste_suffixes;
    int paste_suffix_count;
} cbm_ti_macro_t;

/* NULL when out of memory. */
cbm_ti_source_t *cbm_ti_source_new(void);
/* Add one file: its repository-relative path and exact bytes (both copied).
 * false on a repeated path, after finish, or when out of memory. */
bool cbm_ti_source_add(cbm_ti_source_t *s, const char *path, const char *text, size_t len);
/* Split lines, resolve includes and read the headers' macros. Files are then
 * in path order. false when out of memory. */
bool cbm_ti_source_finish(cbm_ti_source_t *s);
void cbm_ti_source_free(cbm_ti_source_t *s);

int cbm_ti_source_file_count(const cbm_ti_source_t *s);
const char *cbm_ti_source_path(const cbm_ti_source_t *s, int file);
/* -1 when the snapshot has no such C-family file. */
int cbm_ti_source_find(const cbm_ti_source_t *s, const char *path);
/* .h, .hpp, .hh */
bool cbm_ti_source_is_header(const cbm_ti_source_t *s, int file);
int cbm_ti_source_line_count(const cbm_ti_source_t *s, int file);
/* Line text without its newline; NULL outside the file. */
const char *cbm_ti_source_line(const cbm_ti_source_t *s, int file, int line, size_t *len);

/* Mark every file that includes a marked file, directly or through other
 * files. `marked` holds one entry per file. A quoted include names the file
 * it resolves to from the including file's directory when that file exists,
 * otherwise every file whose path ends with the included path. */
void cbm_ti_source_include_closure(const cbm_ti_source_t *s, bool *marked);

/* The `#define`s of the header files, in (file, line) order. */
const cbm_ti_macro_t *cbm_ti_source_macros(const cbm_ti_source_t *s, int *count);

/* Where an identifier is written, in (file, line) order, one place per line.
 * Lines whose first non-blank text is `//` or `*` hold none, nor do keywords.
 * The index is built on the first call: false from build means out of memory. */
bool cbm_ti_source_build_occurrences(cbm_ti_source_t *s);
const cbm_ti_place_t *cbm_ti_source_occurrences(const cbm_ti_source_t *s, const char *token,
                                                size_t len, int *count);

#endif /* CBM_TEST_IMPACT_SOURCE_H */
