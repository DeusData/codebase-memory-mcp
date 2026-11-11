/*
 * test_impact_source.c — text facts about the C-family sources of one snapshot.
 *
 * Each line pattern reproduces one regular expression of the measured
 * reference (replay4.py) by hand, so its matches are the same without a regex
 * engine on any platform. The comment above each function quotes the
 * expression it reproduces.
 */
#include "mcp/test_impact_source.h"

#include "foundation/arena.h"
#include "foundation/hash_table.h"
#include "foundation/mem_core.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ── Characters ───────────────────────────────────────────────────── */

static bool ti_space(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\f' || c == '\v' || c == '\n';
}

static bool ti_word(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

static bool ti_start(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
}

static size_t ti_skip_space(const char *s, size_t i, size_t n) {
    while (i < n && ti_space(s[i])) {
        i++;
    }
    return i;
}

/* The end of s[from..n) without its trailing whitespace. */
static size_t ti_trim_end(const char *s, size_t from, size_t n) {
    while (n > from && ti_space(s[n - 1])) {
        n--;
    }
    return n;
}

static size_t ti_word_end(const char *s, size_t i, size_t n) {
    while (i < n && ti_word(s[i])) {
        i++;
    }
    return i;
}

static bool ti_at(const char *s, size_t i, size_t n, const char *word) {
    size_t len = strlen(word);
    return i + len <= n && memcmp(s + i, word, len) == 0;
}

/* `word` at i followed by a word boundary. */
static bool ti_keyword_at(const char *s, size_t i, size_t n, const char *word) {
    size_t len = strlen(word);
    return ti_at(s, i, n, word) && (i + len == n || !ti_word(s[i + len]));
}

/* `word` at i followed by at least one blank: `word\s+`. */
static bool ti_word_then_space(const char *s, size_t i, size_t n, const char *word) {
    size_t len = strlen(word);
    return ti_at(s, i, n, word) && i + len < n && ti_space(s[i + len]);
}

/* An identifier may start at i: `\b[A-Za-z_]`. */
static bool ti_identifier_at(const char *s, size_t i) {
    return ti_start(s[i]) && (i == 0 || !ti_word(s[i - 1]));
}

/* After an identifier ending at e: `\s*(?:\[[^\]]*\])?\s*;`. */
static bool ti_array_then_semicolon(const char *s, size_t e, size_t n) {
    size_t j = ti_skip_space(s, e, n);
    if (j < n && s[j] == ';') {
        return true;
    }
    if (j >= n || s[j] != '[') {
        return false;
    }
    const char *close = memchr(s + j + 1, ']', n - j - 1);
    if (!close) {
        return false;
    }
    j = ti_skip_space(s, (size_t)(close - s) + 1, n);
    return j < n && s[j] == ';';
}

/* ── Keywords (replay4 C_KEYWORDS), sorted for bsearch ───────────────── */

static const char *const ti_keywords[] = {
    "NULL",     "auto",    "bool",     "break",    "case",    "char",     "const",    "continue",
    "default",  "define",  "defined",  "do",       "double",  "elif",     "else",     "endif",
    "enum",     "extern",  "false",    "float",    "for",     "goto",     "if",       "ifdef",
    "ifndef",   "include", "inline",   "int",      "int16_t", "int32_t",  "int64_t",  "int8_t",
    "long",     "pragma",  "register", "restrict", "return",  "short",    "signed",   "size_t",
    "sizeof",   "static",  "struct",   "switch",   "true",    "typedef",  "uint16_t", "uint32_t",
    "uint64_t", "uint8_t", "union",    "unsigned", "void",    "volatile", "while"};

bool cbm_ti_is_keyword(const char *word, size_t len) {
    size_t lo = 0;
    size_t hi = sizeof(ti_keywords) / sizeof(ti_keywords[0]);
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        const char *k = ti_keywords[mid];
        size_t klen = strlen(k);
        int c = memcmp(word, k, len < klen ? len : klen);
        if (c == 0) {
            c = len < klen ? -1 : (len > klen ? 1 : 0);
        }
        if (c == 0) {
            return true;
        }
        if (c < 0) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return false;
}

/* ── Line patterns ─────────────────────────────────────────────────── */

/* The offset of the first "*" + "/" in s[from..to), or `to`. */
static size_t ti_comment_close(const char *s, size_t from, size_t to) {
    for (size_t i = from; i + 1 < to; i++) {
        if (s[i] == '*' && s[i + 1] == '/') {
            return i;
        }
    }
    return to;
}

/* A line that is nothing but comment: blank; `//`; a block opened here with
 * nothing after its first close; a block-comment line (`*` then whitespace or
 * the end); a close with nothing after it; or the closing line of a block
 * opened earlier (ends in the close, opens none). Anything else is code: a
 * line starting `*name`, `**` or `*(` dereferences, and code before a
 * trailing comment is a change. The reference pattern
 * `^\s*(//.*|/\*.*|\*.*|.*\*\/\s*)?$` took both for comments, and a PR's
 * `*out = f(..., true);` edits seeded nothing. A line-local view cannot tell
 * `* text` from a multiplication continuation; the project's format keeps
 * binary operators at line ends, so that spelling stays a comment line. */
bool cbm_ti_line_is_comment(const char *s, size_t n) {
    size_t i = ti_skip_space(s, 0, n);
    if (i == n) {
        return true;
    }
    size_t e = ti_trim_end(s, i, n);
    if (s[i] == '/' && i + 1 < e && s[i + 1] == '/') {
        return true;
    }
    if (s[i] == '/' && i + 1 < e && s[i + 1] == '*') {
        size_t close = ti_comment_close(s, i + 2, e);
        return close == e || close + 2 == e;
    }
    if (s[i] == '*') {
        if (i + 1 < e && s[i + 1] == '/') {
            return i + 2 == e;
        }
        return i + 1 == e || ti_space(s[i + 1]);
    }
    if (e - i >= 2 && s[e - 2] == '*' && s[e - 1] == '/') {
        for (size_t k = i; k + 1 < e; k++) {
            if (s[k] == '/' && s[k + 1] == '*') {
                return false;
            }
        }
        return true;
    }
    return false;
}

/* `^\s*#\s*(if|ifdef|ifndef|elif|else|endif|undef|include)\b` */
bool cbm_ti_line_is_pp_condition(const char *s, size_t n) {
    static const char *const words[] = {"if",   "ifdef", "ifndef", "elif",
                                        "else", "endif", "undef",  "include"};
    size_t i = ti_skip_space(s, 0, n);
    if (i >= n || s[i] != '#') {
        return false;
    }
    i = ti_skip_space(s, i + 1, n);
    size_t e = ti_word_end(s, i, n);
    for (size_t k = 0; k < sizeof(words) / sizeof(words[0]); k++) {
        if (e - i == strlen(words[k]) && memcmp(s + i, words[k], e - i) == 0) {
            return true;
        }
    }
    return false;
}

/* `^\s*#\s*define\s+([A-Za-z_]\w*)(\()?` */
bool cbm_ti_match_define(const char *s, size_t n, cbm_ti_span_t *name, size_t *body) {
    size_t i = ti_skip_space(s, 0, n);
    if (i >= n || s[i] != '#') {
        return false;
    }
    i = ti_skip_space(s, i + 1, n);
    if (!ti_word_then_space(s, i, n, "define")) {
        return false;
    }
    i = ti_skip_space(s, i + 6, n);
    if (i >= n || !ti_start(s[i])) {
        return false;
    }
    size_t e = ti_word_end(s, i, n);
    *name = (cbm_ti_span_t){i, e - i};
    if (body) {
        *body = e < n && s[e] == '(' ? e + 1 : e;
    }
    return true;
}

/* `\btypedef\b.*?\b([A-Za-z_]\w*)\s*(?:\[[^\]]*\])?\s*;`, searched */
bool cbm_ti_match_typedef(const char *s, size_t n, cbm_ti_span_t *name) {
    for (size_t p = 0; p + 7 <= n; p++) {
        if (!ti_keyword_at(s, p, n, "typedef") || (p > 0 && ti_word(s[p - 1]))) {
            continue;
        }
        for (size_t q = p + 7; q < n; q++) {
            if (!ti_identifier_at(s, q)) {
                continue;
            }
            size_t e = ti_word_end(s, q, n);
            if (ti_array_then_semicolon(s, e, n)) {
                *name = (cbm_ti_span_t){q, e - q};
                return true;
            }
        }
    }
    return false;
}

/* `^\s*(?:typedef\s+)?(?:struct|union|enum)\s+([A-Za-z_]\w*)\s*\{` */
bool cbm_ti_match_tag_declaration(const char *s, size_t n, cbm_ti_span_t *name) {
    size_t i = ti_skip_space(s, 0, n);
    if (ti_word_then_space(s, i, n, "typedef")) {
        i = ti_skip_space(s, i + 7, n);
    }
    size_t kw = ti_word_then_space(s, i, n, "struct")  ? 6
                : ti_word_then_space(s, i, n, "union") ? 5
                : ti_word_then_space(s, i, n, "enum")  ? 4
                                                       : 0;
    if (!kw) {
        return false;
    }
    i = ti_skip_space(s, i + kw, n);
    if (i >= n || !ti_start(s[i])) {
        return false;
    }
    size_t e = ti_word_end(s, i, n);
    size_t j = ti_skip_space(s, e, n);
    if (j >= n || s[j] != '{') {
        return false;
    }
    *name = (cbm_ti_span_t){i, e - i};
    return true;
}

/* `^\s*extern\b[^;(]*?\b([A-Za-z_]\w*)\s*(?:\[[^\]]*\])?\s*;` */
bool cbm_ti_match_extern(const char *s, size_t n, cbm_ti_span_t *name) {
    size_t i = ti_skip_space(s, 0, n);
    if (!ti_keyword_at(s, i, n, "extern")) {
        return false;
    }
    for (size_t q = i + 6; q < n && s[q] != ';' && s[q] != '('; q++) {
        if (!ti_identifier_at(s, q)) {
            continue;
        }
        size_t e = ti_word_end(s, q, n);
        if (ti_array_then_semicolon(s, e, n)) {
            *name = (cbm_ti_span_t){q, e - q};
            return true;
        }
    }
    return false;
}

/* `^[A-Za-z_][\w \t\*]*?\b([A-Za-z_]\w*)\s*\(` */
bool cbm_ti_match_prototype(const char *s, size_t n, cbm_ti_span_t *name) {
    if (n == 0 || !ti_start(s[0])) {
        return false;
    }
    for (size_t k = 1; k < n; k++) {
        if (ti_identifier_at(s, k)) {
            size_t e = ti_word_end(s, k, n);
            size_t j = ti_skip_space(s, e, n);
            if (j < n && s[j] == '(') {
                *name = (cbm_ti_span_t){k, e - k};
                return true;
            }
        }
        if (!ti_word(s[k]) && s[k] != ' ' && s[k] != '\t' && s[k] != '*') {
            return false;
        }
    }
    return false;
}

bool cbm_ti_match_declaration(const char *s, size_t n, cbm_ti_span_t *name) {
    return cbm_ti_match_define(s, n, name, NULL) || cbm_ti_match_typedef(s, n, name) ||
           cbm_ti_match_tag_declaration(s, n, name) || cbm_ti_match_extern(s, n, name) ||
           cbm_ti_match_prototype(s, n, name);
}

/* `^\s*#\s*include\s+"([^"]+)"` */
bool cbm_ti_match_include(const char *s, size_t n, cbm_ti_span_t *path) {
    size_t i = ti_skip_space(s, 0, n);
    if (i >= n || s[i] != '#') {
        return false;
    }
    i = ti_skip_space(s, i + 1, n);
    if (!ti_word_then_space(s, i, n, "include")) {
        return false;
    }
    i = ti_skip_space(s, i + 7, n);
    if (i >= n || s[i] != '"') {
        return false;
    }
    const char *close = memchr(s + i + 1, '"', n - i - 1);
    if (!close || close == s + i + 1) {
        return false;
    }
    *path = (cbm_ti_span_t){i + 1, (size_t)(close - s) - i - 1};
    return true;
}

/* ── The index ─────────────────────────────────────────────────────── */

typedef struct {
    int *items;
    int count;
    int cap;
} ti_ints_t;

typedef struct {
    cbm_ti_place_t *items;
    int count;
    int cap;
} ti_places_t;

typedef struct {
    const char *path;
    const char *text;
    size_t len;
    size_t *line_starts; /* line i (0-based) starts at line_starts[i] */
    int line_count;
    bool header;
    ti_ints_t includers; /* files that include this one */
} ti_file_t;

struct cbm_ti_source {
    CBMArena arena;
    ti_file_t *files;
    int file_count;
    int file_cap;
    bool finished;
    bool failed;
    CBMHashTable *by_path; /* path -> file index + 1 */
    cbm_ti_macro_t *macros;
    int macro_count;
    int macro_cap;
    CBMHashTable *occurrences; /* token -> ti_places_t* */
    bool occurrences_built;
};

static bool ti_grow(void **items, int *cap, int count, size_t size) {
    if (count < *cap) {
        return true;
    }
    int next = *cap ? *cap * 2 : 8;
    void *grown = cbm_realloc(CBM_MEM_CLASS_EXTRACT, *items, (size_t)next * size);
    if (!grown) {
        return false;
    }
    *items = grown;
    *cap = next;
    return true;
}

static bool ti_ints_push(ti_ints_t *a, int v) {
    if (!ti_grow((void **)&a->items, &a->cap, a->count, sizeof(*a->items))) {
        return false;
    }
    a->items[a->count++] = v;
    return true;
}

cbm_ti_source_t *cbm_ti_source_new(void) {
    cbm_ti_source_t *s = cbm_alloc(CBM_MEM_CLASS_EXTRACT, sizeof(*s));
    if (!s) {
        return NULL;
    }
    memset(s, 0, sizeof(*s));
    cbm_arena_init(&s->arena);
    s->by_path = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
    s->occurrences = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
    if (!s->by_path || !s->occurrences) {
        cbm_ti_source_free(s);
        return NULL;
    }
    return s;
}

static void ti_free_places(const char *key, void *value, void *userdata) {
    (void)key;
    (void)userdata;
    ti_places_t *places = value;
    cbm_free(CBM_MEM_CLASS_EXTRACT, places->items);
    cbm_free(CBM_MEM_CLASS_EXTRACT, places);
}

void cbm_ti_source_free(cbm_ti_source_t *s) {
    if (!s) {
        return;
    }
    for (int i = 0; i < s->file_count; i++) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, s->files[i].line_starts);
        cbm_free(CBM_MEM_CLASS_EXTRACT, s->files[i].includers.items);
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, s->files);
    cbm_free(CBM_MEM_CLASS_EXTRACT, s->macros);
    if (s->occurrences) {
        cbm_ht_foreach(s->occurrences, ti_free_places, NULL);
        cbm_ht_free(s->occurrences);
    }
    if (s->by_path) {
        cbm_ht_free(s->by_path);
    }
    cbm_arena_destroy(&s->arena);
    cbm_free(CBM_MEM_CLASS_EXTRACT, s);
}

static bool ti_is_header_path(const char *path) {
    size_t n = strlen(path);
    return (n >= 2 && strcmp(path + n - 2, ".h") == 0) ||
           (n >= 4 && strcmp(path + n - 4, ".hpp") == 0) ||
           (n >= 3 && strcmp(path + n - 3, ".hh") == 0);
}

bool cbm_ti_source_add(cbm_ti_source_t *s, const char *path, const char *text, size_t len) {
    if (!s || s->finished || !path || (!text && len) || cbm_ht_has(s->by_path, path)) {
        return false;
    }
    if (!ti_grow((void **)&s->files, &s->file_cap, s->file_count, sizeof(*s->files))) {
        return false;
    }
    char *own_text = cbm_arena_alloc(&s->arena, len + 1);
    char *own_path = cbm_arena_strdup(&s->arena, path);
    if (!own_text || !own_path) {
        return false;
    }
    if (len) {
        memcpy(own_text, text, len);
    }
    own_text[len] = '\0';
    s->files[s->file_count] = (ti_file_t){
        .path = own_path, .text = own_text, .len = len, .header = ti_is_header_path(own_path)};
    s->file_count++;
    return true;
}

/* Lines as the reference splits them (`text.split('\n')`): a final newline
 * leaves one more, empty line. */
static bool ti_split_lines(ti_file_t *f) {
    int count = 1;
    for (size_t i = 0; i < f->len; i++) {
        count += f->text[i] == '\n';
    }
    f->line_starts = cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)count * sizeof(size_t));
    if (!f->line_starts) {
        return false;
    }
    int at = 0;
    f->line_starts[at++] = 0;
    for (size_t i = 0; i < f->len; i++) {
        if (f->text[i] == '\n') {
            f->line_starts[at++] = i + 1;
        }
    }
    f->line_count = count;
    return true;
}

static int ti_path_compare(const void *left, const void *right) {
    return strcmp(((const ti_file_t *)left)->path, ((const ti_file_t *)right)->path);
}

int cbm_ti_source_file_count(const cbm_ti_source_t *s) {
    return s ? s->file_count : 0;
}

const char *cbm_ti_source_path(const cbm_ti_source_t *s, int file) {
    return s && file >= 0 && file < s->file_count ? s->files[file].path : NULL;
}

int cbm_ti_source_find(const cbm_ti_source_t *s, const char *path) {
    if (!s || !path) {
        return -1;
    }
    intptr_t at = (intptr_t)cbm_ht_get(s->by_path, path);
    return at > 0 ? (int)at - 1 : -1;
}

bool cbm_ti_source_is_header(const cbm_ti_source_t *s, int file) {
    return s && file >= 0 && file < s->file_count && s->files[file].header;
}

int cbm_ti_source_line_count(const cbm_ti_source_t *s, int file) {
    return s && file >= 0 && file < s->file_count ? s->files[file].line_count : 0;
}

const char *cbm_ti_source_line(const cbm_ti_source_t *s, int file, int line, size_t *len) {
    if (!s || file < 0 || file >= s->file_count || line < 1 || line > s->files[file].line_count) {
        if (len) {
            *len = 0;
        }
        return NULL;
    }
    const ti_file_t *f = &s->files[file];
    size_t start = f->line_starts[line - 1];
    size_t end = line < f->line_count ? f->line_starts[line] - 1 : f->len;
    if (len) {
        *len = end - start;
    }
    return f->text + start;
}

/* ── Includes ──────────────────────────────────────────────────────── */

/* os.path.normpath(os.path.join(dirname(from), inc)) for '/' paths. */
static char *ti_normalize_join(CBMArena *arena, const char *from, const char *inc, size_t inc_len) {
    size_t from_len = strlen(from);
    const char *slash = NULL;
    for (size_t i = 0; i < from_len; i++) {
        if (from[i] == '/') {
            slash = from + i;
        }
    }
    size_t dir_len = inc_len && inc[0] == '/' ? 0 : (slash ? (size_t)(slash - from) : 0);
    size_t total = dir_len + 1 + inc_len;
    char *joined = cbm_arena_alloc(arena, total + 1);
    char *out = cbm_arena_alloc(arena, total + 2);
    if (!joined || !out) {
        return NULL;
    }
    memcpy(joined, from, dir_len);
    joined[dir_len] = '/';
    memcpy(joined + dir_len + 1, inc, inc_len);
    joined[total] = '\0';
    bool absolute = inc_len && inc[0] == '/';
    size_t used = 0;
    size_t depth = 0; /* segments that can be removed by a later ".." */
    const char *p = dir_len ? joined : joined + 1;
    while (*p) {
        const char *e = strchr(p, '/');
        size_t seg = e ? (size_t)(e - p) : strlen(p);
        if (seg == 0 || (seg == 1 && p[0] == '.')) {
            /* skip */
        } else if (seg == 2 && p[0] == '.' && p[1] == '.' && depth > 0) {
            while (used > 0 && out[used - 1] != '/') {
                used--;
            }
            if (used > 0) {
                used--;
            }
            depth--;
        } else if (!(seg == 2 && p[0] == '.' && p[1] == '.' && absolute)) {
            if (used) {
                out[used++] = '/';
            }
            memcpy(out + used, p, seg);
            used += seg;
            depth += !(seg == 2 && p[0] == '.' && p[1] == '.');
        }
        p += seg + (e ? 1 : 0);
    }
    out[used] = '\0';
    return out;
}

/* Every segment suffix of every path, for includes that do not resolve from
 * the including file's directory. */
static bool ti_add_suffixes(cbm_ti_source_t *s, CBMHashTable *suffixes, ti_ints_t **lists,
                            int *list_count, int *list_cap) {
    for (int f = 0; f < s->file_count; f++) {
        const char *path = s->files[f].path;
        for (const char *p = path; p; p = strchr(p, '/') ? strchr(p, '/') + 1 : NULL) {
            intptr_t at = (intptr_t)cbm_ht_get(suffixes, p);
            if (!at) {
                if (!ti_grow((void **)lists, list_cap, *list_count, sizeof(**lists))) {
                    return false;
                }
                (*lists)[*list_count] = (ti_ints_t){0};
                at = ++*list_count;
                cbm_ht_set(suffixes, p, (void *)at);
            }
            if (!ti_ints_push(&(*lists)[at - 1], f)) {
                return false;
            }
        }
    }
    return true;
}

static bool ti_add_includer(cbm_ti_source_t *s, int target, int from) {
    ti_ints_t *in = &s->files[target].includers;
    if (in->count && in->items[in->count - 1] == from) {
        return true;
    }
    for (int i = 0; i < in->count; i++) {
        if (in->items[i] == from) {
            return true;
        }
    }
    return ti_ints_push(in, from);
}

static bool ti_resolve_include(cbm_ti_source_t *s, CBMHashTable *suffixes, const ti_ints_t *lists,
                               int from, const char *inc, size_t inc_len) {
    char *normal = ti_normalize_join(&s->arena, s->files[from].path, inc, inc_len);
    if (!normal) {
        return false;
    }
    int direct = cbm_ti_source_find(s, normal);
    if (direct >= 0) {
        return ti_add_includer(s, direct, from);
    }
    size_t skip = 0;
    while (skip < inc_len && (inc[skip] == '.' || inc[skip] == '/')) {
        skip++;
    }
    char *key = cbm_arena_strndup(&s->arena, inc + skip, inc_len - skip);
    if (!key) {
        return false;
    }
    intptr_t at = (intptr_t)cbm_ht_get(suffixes, key);
    for (int i = 0; at && i < lists[at - 1].count; i++) {
        if (!ti_add_includer(s, lists[at - 1].items[i], from)) {
            return false;
        }
    }
    return true;
}

static bool ti_build_includes(cbm_ti_source_t *s) {
    CBMHashTable *suffixes = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 0);
    ti_ints_t *lists = NULL;
    int list_count = 0;
    int list_cap = 0;
    bool ok = suffixes && ti_add_suffixes(s, suffixes, &lists, &list_count, &list_cap);
    for (int f = 0; ok && f < s->file_count; f++) {
        for (int line = 1; ok && line <= s->files[f].line_count; line++) {
            size_t len = 0;
            const char *text = cbm_ti_source_line(s, f, line, &len);
            cbm_ti_span_t inc;
            if (cbm_ti_match_include(text, len, &inc)) {
                ok = ti_resolve_include(s, suffixes, lists, f, text + inc.start, inc.len);
            }
        }
    }
    for (int i = 0; i < list_count; i++) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, lists[i].items);
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, lists);
    if (suffixes) {
        cbm_ht_free(suffixes);
    }
    return ok;
}

void cbm_ti_source_include_closure(const cbm_ti_source_t *s, bool *marked) {
    if (!s || !marked || !s->finished) {
        return;
    }
    int *stack = cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)(s->file_count + 1) * sizeof(int));
    if (!stack) {
        /* Out of memory: mark everything, which can only select more. */
        for (int i = 0; i < s->file_count; i++) {
            marked[i] = true;
        }
        return;
    }
    int top = 0;
    for (int i = 0; i < s->file_count; i++) {
        if (marked[i]) {
            stack[top++] = i;
        }
    }
    while (top > 0) {
        const ti_ints_t *in = &s->files[stack[--top]].includers;
        for (int i = 0; i < in->count; i++) {
            if (!marked[in->items[i]]) {
                marked[in->items[i]] = true;
                stack[top++] = in->items[i];
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_EXTRACT, stack);
}

/* ── Header macros ─────────────────────────────────────────────────── */

typedef struct {
    const char **items;
    int count;
    int cap;
} ti_strs_t;

static bool ti_strs_add_unique(CBMArena *arena, ti_strs_t *list, const char *word, size_t len) {
    for (int i = 0; i < list->count; i++) {
        if (strlen(list->items[i]) == len && memcmp(list->items[i], word, len) == 0) {
            return true;
        }
    }
    char *copy = cbm_arena_strndup(arena, word, len);
    if (!copy || !ti_grow((void **)&list->items, &list->cap, list->count, sizeof(*list->items))) {
        return false;
    }
    list->items[list->count++] = copy;
    return true;
}

static int ti_str_compare(const void *left, const void *right) {
    return strcmp(*(const char *const *)left, *(const char *const *)right);
}

/* `re.findall(r'([A-Za-z_]\w*)\s*##|##\s*([A-Za-z_]\w*)', text)` */
static bool ti_scan_pastes(CBMArena *arena, const char *t, size_t n, ti_strs_t *pre,
                           ti_strs_t *suf) {
    size_t i = 0;
    while (i < n) {
        if (ti_start(t[i])) {
            size_t e = ti_word_end(t, i, n);
            size_t j = ti_skip_space(t, e, n);
            if (j + 1 < n && t[j] == '#' && t[j + 1] == '#') {
                if (!ti_strs_add_unique(arena, pre, t + i, e - i)) {
                    return false;
                }
                i = j + 2;
                continue;
            }
        }
        if (i + 1 < n && t[i] == '#' && t[i + 1] == '#') {
            size_t j = ti_skip_space(t, i + 2, n);
            if (j < n && ti_start(t[j])) {
                size_t e = ti_word_end(t, j, n);
                if (!ti_strs_add_unique(arena, suf, t + j, e - j)) {
                    return false;
                }
                i = e;
                continue;
            }
        }
        i++;
    }
    return true;
}

/* `set(TOKEN_RX.findall(text)) - C_KEYWORDS - {name}` */
static bool ti_scan_tokens(CBMArena *arena, const char *t, size_t n, const char *name,
                           ti_strs_t *out) {
    size_t i = 0;
    while (i < n) {
        if (!ti_start(t[i])) {
            i++;
            continue;
        }
        size_t e = ti_word_end(t, i, n);
        bool own = strlen(name) == e - i && memcmp(name, t + i, e - i) == 0;
        if (!own && !cbm_ti_is_keyword(t + i, e - i) &&
            !ti_strs_add_unique(arena, out, t + i, e - i)) {
            return false;
        }
        i = e;
    }
    return true;
}

static bool ti_line_continues(const char *line, size_t len) {
    while (len > 0 && ti_space(line[len - 1])) {
        len--;
    }
    return len > 0 && line[len - 1] == '\\';
}

/* One `#define` starting at *line; *line ends on its last continuation line.
 * The body is the text after the name (and '('), then each continuation
 * line, joined with one blank. */
static bool ti_read_macro(cbm_ti_source_t *s, int file, int *line, cbm_ti_span_t name,
                          size_t body) {
    size_t len = 0;
    const char *text = cbm_ti_source_line(s, file, *line, &len);
    size_t cap = len - body + 1;
    int last = *line;
    const char *tail = text + body;
    size_t tail_len = len - body;
    while (ti_line_continues(tail, tail_len) && last < s->files[file].line_count) {
        last++;
        tail = cbm_ti_source_line(s, file, last, &tail_len);
        cap += tail_len + 1;
    }
    char *joined = cbm_arena_alloc(&s->arena, cap + 1);
    char *macro_name = cbm_arena_strndup(&s->arena, text + name.start, name.len);
    if (!joined || !macro_name) {
        return false;
    }
    size_t used = len - body;
    memcpy(joined, text + body, used);
    for (int l = *line + 1; l <= last; l++) {
        const char *more = cbm_ti_source_line(s, file, l, &tail_len);
        joined[used++] = ' ';
        memcpy(joined + used, more, tail_len);
        used += tail_len;
    }
    joined[used] = '\0';
    ti_strs_t tokens = {0};
    ti_strs_t pre = {0};
    ti_strs_t suf = {0};
    bool ok = ti_scan_tokens(&s->arena, joined, used, macro_name, &tokens) &&
              ti_scan_pastes(&s->arena, joined, used, &pre, &suf) &&
              ti_grow((void **)&s->macros, &s->macro_cap, s->macro_count, sizeof(*s->macros));
    if (ok && tokens.count > 1) {
        qsort(tokens.items, (size_t)tokens.count, sizeof(*tokens.items), ti_str_compare);
    }
    const char **kept[3] = {NULL, NULL, NULL};
    ti_strs_t *lists[3] = {&tokens, &pre, &suf};
    for (int k = 0; ok && k < 3; k++) {
        size_t bytes = (size_t)(lists[k]->count ? lists[k]->count : 1) * sizeof(char *);
        kept[k] = cbm_arena_alloc(&s->arena, bytes);
        ok = kept[k] != NULL;
        if (ok && lists[k]->count) {
            memcpy(kept[k], lists[k]->items, (size_t)lists[k]->count * sizeof(char *));
        }
    }
    if (ok) {
        s->macros[s->macro_count++] = (cbm_ti_macro_t){.name = macro_name,
                                                       .file = file,
                                                       .tokens = kept[0],
                                                       .token_count = tokens.count,
                                                       .paste_prefixes = kept[1],
                                                       .paste_prefix_count = pre.count,
                                                       .paste_suffixes = kept[2],
                                                       .paste_suffix_count = suf.count};
    }
    for (int k = 0; k < 3; k++) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, lists[k]->items);
    }
    *line = last;
    return ok;
}

static bool ti_build_macros(cbm_ti_source_t *s) {
    for (int f = 0; f < s->file_count; f++) {
        if (!s->files[f].header) {
            continue;
        }
        for (int line = 1; line <= s->files[f].line_count; line++) {
            size_t len = 0;
            const char *text = cbm_ti_source_line(s, f, line, &len);
            cbm_ti_span_t name;
            size_t body = 0;
            if (cbm_ti_match_define(text, len, &name, &body) &&
                !ti_read_macro(s, f, &line, name, body)) {
                return false;
            }
        }
    }
    return true;
}

const cbm_ti_macro_t *cbm_ti_source_macros(const cbm_ti_source_t *s, int *count) {
    if (count) {
        *count = s ? s->macro_count : 0;
    }
    return s ? s->macros : NULL;
}

bool cbm_ti_source_finish(cbm_ti_source_t *s) {
    if (!s || s->finished) {
        return false;
    }
    if (s->file_count > 1) {
        qsort(s->files, (size_t)s->file_count, sizeof(*s->files), ti_path_compare);
    }
    for (int f = 0; f < s->file_count; f++) {
        if (!ti_split_lines(&s->files[f])) {
            return false;
        }
        cbm_ht_set(s->by_path, s->files[f].path, (void *)(intptr_t)(f + 1));
    }
    s->finished = true;
    return ti_build_includes(s) && ti_build_macros(s);
}

/* ── Occurrences ───────────────────────────────────────────────────── */

static bool ti_note_occurrence(cbm_ti_source_t *s, const char *word, size_t len, int file,
                               int line) {
    char key[256];
    const char *lookup;
    char *heap_key = NULL;
    if (len < sizeof(key)) {
        memcpy(key, word, len);
        key[len] = '\0';
        lookup = key;
    } else {
        heap_key = cbm_arena_strndup(&s->arena, word, len);
        if (!heap_key) {
            return false;
        }
        lookup = heap_key;
    }
    ti_places_t *places = cbm_ht_get(s->occurrences, lookup);
    if (!places) {
        places = cbm_alloc(CBM_MEM_CLASS_EXTRACT, sizeof(*places));
        const char *owned = heap_key ? heap_key : cbm_arena_strndup(&s->arena, word, len);
        if (!places || !owned) {
            cbm_free(CBM_MEM_CLASS_EXTRACT, places);
            return false;
        }
        *places = (ti_places_t){0};
        cbm_ht_set(s->occurrences, owned, places);
    }
    if (places->count && places->items[places->count - 1].file == file &&
        places->items[places->count - 1].line == line) {
        return true; /* one place per line */
    }
    if (!ti_grow((void **)&places->items, &places->cap, places->count, sizeof(*places->items))) {
        return false;
    }
    places->items[places->count++] = (cbm_ti_place_t){file, line};
    return true;
}

/* `for t in set(TOKEN_RX.findall(ln))` on lines not starting `//` or `*`. */
static bool ti_scan_line(cbm_ti_source_t *s, int file, int line) {
    size_t len = 0;
    const char *text = cbm_ti_source_line(s, file, line, &len);
    size_t i = ti_skip_space(text, 0, len);
    if (i < len && (text[i] == '*' || (text[i] == '/' && i + 1 < len && text[i + 1] == '/'))) {
        return true;
    }
    while (i < len) {
        if (!ti_start(text[i])) {
            i++;
            continue;
        }
        size_t e = ti_word_end(text, i, len);
        if (!cbm_ti_is_keyword(text + i, e - i) &&
            !ti_note_occurrence(s, text + i, e - i, file, line)) {
            return false;
        }
        i = e;
    }
    return true;
}

bool cbm_ti_source_build_occurrences(cbm_ti_source_t *s) {
    if (!s || !s->finished || s->failed) {
        return false;
    }
    if (s->occurrences_built) {
        return true;
    }
    for (int f = 0; f < s->file_count; f++) {
        for (int line = 1; line <= s->files[f].line_count; line++) {
            if (!ti_scan_line(s, f, line)) {
                s->failed = true;
                return false;
            }
        }
    }
    s->occurrences_built = true;
    return true;
}

const cbm_ti_place_t *cbm_ti_source_occurrences(const cbm_ti_source_t *s, const char *token,
                                                size_t len, int *count) {
    *count = 0;
    if (!s || !s->occurrences_built || !token) {
        return NULL;
    }
    char key[256];
    if (len >= sizeof(key)) {
        return NULL; /* never indexed under a stack key; looked up below */
    }
    memcpy(key, token, len);
    key[len] = '\0';
    const ti_places_t *places = cbm_ht_get(s->occurrences, key);
    if (!places) {
        return NULL;
    }
    *count = places->count;
    return places->items;
}
