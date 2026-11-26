/*
 * doc_links_c.c — resolving C doc references (doclink_c.c): Doxygen @ref and
 * @see names, with the lookup Doxygen itself has, which knows no imports.
 *
 * A name (`name`, `name()`, `::name`) is looked up
 *   1. among the Doxygen pages, groups, anchors and sections of the
 *      repository's docs: such a name is no code entity (a graph gap), and
 *      never binds to a symbol of the same name;
 *   2. among the definitions of the documented file, static ones included;
 *   3. among the definitions every file sees: external linkage, and the types,
 *      enum constants and macros of headers. A static definition of another
 *      file is not visible.
 * A field of the documented file (`@ref field`) comes after its other
 * definitions and before what every file sees. `Type::member` names a field
 * of the struct or union Type, or a constant of the enum Type, Type found by
 * the same levels.
 * The first level with a candidate decides. Its candidates are counted by
 * qualified name: a prototype and its definition, and the #if variants of
 * one definition, are one node, the one the graph keeps for that name. A
 * definition (function, variable, type, enum constant) wins over a macro of
 * the same name; with `()` written, a function or function-like macro wins
 * over the others. One candidate binds (exact in the documented file,
 * unique elsewhere), several are ambiguous, none is missing; but a word of an
 * @see paragraph that names nothing and is a plain lowercase word (no `_`,
 * digit, capital, `()` or `::`) is prose, which Doxygen prints as text: no
 * row. Test code binds only from test code.
 */
#include "pipeline/doc_links.h"
#include "cbm.h"
#include "doclink.h"
#include "helpers.h" /* cbm_is_test_file */
#include "foundation/arena.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "graph_buffer/graph_buffer.h"

#include <ctype.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { CR_FIELDS = 6, CR_NAME_MAX = 256, CR_CANDS = 64, CR_QN_MAX = 2048 };

/* ── The index ───────────────────────────────────────────────────── */

typedef struct {
    int file;
    char kind; /* f v t e r m M */
    bool global;
    const char *name;
    const char *qn;
    const char *parent; /* the enum of a constant, the struct of a field; NULL */
    int next;           /* the next definition of the same name, or -1 */
} cr_def_t;

typedef struct {
    const char *path;
    bool test;
} cr_file_t;

typedef struct {
    CBMArena arena;
    cr_file_t *files;
    int nfiles;
    int *run_to_file;
    int run_count;
    cr_def_t *defs;
    int ndefs;
    int cap_defs;
    CBMHashTable *by_name; /* name -> the first definition's index + 1 */
    CBMHashTable *anchors; /* a Doxygen page, group, anchor or section name */
    CBMHashTable *by_path; /* rel_path -> file index + 1 */
    bool oom;
} cr_index_t;

static const char *cr_copy(cr_index_t *ix, const char *s, size_t n) {
    char *c = cbm_arena_strndup(&ix->arena, s, n);
    ix->oom = ix->oom || !c;
    return c;
}

static bool cr_grow(cr_index_t *ix) {
    if (ix->ndefs < ix->cap_defs) {
        return true;
    }
    int ncap = ix->cap_defs ? ix->cap_defs * PAIR_LEN : CBM_SZ_64;
    cr_def_t *g = (cr_def_t *)cbm_arena_alloc(&ix->arena, (size_t)ncap * sizeof(cr_def_t));
    if (!g) {
        ix->oom = true;
        return false;
    }
    if (ix->ndefs) {
        memcpy(g, ix->defs, (size_t)ix->ndefs * sizeof(cr_def_t));
    }
    ix->defs = g;
    ix->cap_defs = ncap;
    return true;
}

static void cr_read_record(cr_index_t *ix, int fi, const char **fl, const size_t *ln, int n) {
    if (fl[0][0] == 'D' && n >= CR_FIELDS - SKIP_ONE && cr_grow(ix)) { /* parent optional */
        cr_def_t *d = &ix->defs[ix->ndefs++];
        d->file = fi;
        d->kind = fl[1][0];
        d->global = fl[PAIR_LEN][0] == 'x';
        d->name = cr_copy(ix, fl[CBM_SZ_3], ln[CBM_SZ_3]);
        d->qn = cr_copy(ix, fl[CBM_SZ_4], ln[CBM_SZ_4]);
        bool none = n <= CBM_SZ_5 || (ln[CBM_SZ_5] == SKIP_ONE && fl[CBM_SZ_5][0] == '-');
        d->parent = none ? NULL : cr_copy(ix, fl[CBM_SZ_5], ln[CBM_SZ_5]);
        d->next = CBM_NOT_FOUND;
    } else if (fl[0][0] == 'A' && n >= PAIR_LEN) {
        const char *name = cr_copy(ix, fl[1], ln[1]);
        if (name && !cbm_ht_get(ix->anchors, name)) {
            cbm_ht_set(ix->anchors, name, (void *)(uintptr_t)SKIP_ONE);
        }
    }
}

static void cr_read_blob(cr_index_t *ix, int fi, const char *blob) {
    const char *line = strchr(blob, '\n'); /* the tag line */
    while (line && !ix->oom) {
        const char *b = line + SKIP_ONE;
        const char *e = strchr(b, '\n');
        if (!e) {
            e = b + strlen(b);
        }
        const char *fl[CR_FIELDS];
        size_t ln[CR_FIELDS];
        int n = 0;
        const char *s = b;
        for (const char *p = b; p <= e && n < CR_FIELDS; p++) {
            if (p == e || *p == '\t') {
                fl[n] = s;
                ln[n] = (size_t)(p - s);
                n++;
                s = p + SKIP_ONE;
            }
        }
        if (n > 0 && ln[0] == SKIP_ONE) {
            cr_read_record(ix, fi, fl, ln, n);
        }
        line = *e ? e : NULL;
    }
}

static void cr_destroy(void *index) {
    cr_index_t *ix = (cr_index_t *)index;
    if (!ix) {
        return;
    }
    cbm_ht_free(ix->by_name);
    cbm_ht_free(ix->anchors);
    cbm_ht_free(ix->by_path);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->run_to_file);
    cbm_arena_destroy(&ix->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, ix);
}

/* Chain every definition into its name's list, in index order. */
static bool cr_link(cr_index_t *ix) {
    ix->by_name = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, (uint32_t)(ix->ndefs + SKIP_ONE));
    if (!ix->by_name) {
        return false;
    }
    for (int i = ix->ndefs - SKIP_ONE; i >= 0; i--) {
        cr_def_t *d = &ix->defs[i];
        if (!d->name || !d->qn) {
            continue;
        }
        void *prev = cbm_ht_get(ix->by_name, d->name);
        d->next = prev ? (int)((uintptr_t)prev - SKIP_ONE) : CBM_NOT_FOUND;
        cbm_ht_set(ix->by_name, d->name, (void *)(uintptr_t)(i + SKIP_ONE));
    }
    return true;
}

static void *cr_build(const cbm_doclink_build_in_t *in) {
    cr_index_t *ix = (cr_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ix));
    if (!ix) {
        return NULL;
    }
    cbm_arena_init(&ix->arena);
    ix->nfiles = in->file_count;
    ix->run_count = in->run_file_count;
    ix->files =
        in->file_count
            ? (cr_file_t *)cbm_arena_calloc(&ix->arena, (size_t)in->file_count * sizeof(cr_file_t))
            : NULL;
    ix->run_to_file = (int *)cbm_calloc(CBM_MEM_CLASS_OTHER,
                                        (size_t)(in->run_file_count + SKIP_ONE) * sizeof(int));
    ix->by_path = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, (uint32_t)(in->file_count + SKIP_ONE));
    ix->anchors = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, CBM_SZ_64);
    bool ok = (!in->file_count || ix->files) && ix->run_to_file && ix->by_path && ix->anchors;
    for (int r = 0; ok && r < in->run_file_count; r++) {
        ix->run_to_file[r] = CBM_NOT_FOUND;
    }
    for (int i = 0; ok && i < in->file_count && !ix->oom; i++) {
        const cbm_doclink_file_t *src = &in->files[i];
        cr_file_t *f = &ix->files[i];
        f->path = src->rel_path;
        f->test = cbm_is_test_file(src->rel_path, CBM_LANG_C);
        cbm_ht_set(ix->by_path, src->rel_path, (void *)(uintptr_t)(i + SKIP_ONE));
        if (src->run_file >= 0 && src->run_file < in->run_file_count) {
            ix->run_to_file[src->run_file] = i;
        }
        if (src->scope) {
            cr_read_blob(ix, i, src->scope);
        }
    }
    ok = ok && !ix->oom && cr_link(ix);
    if (!ok) {
        cbm_log_error("doc_links.c.error", "step", "index", "reason", "alloc");
        cr_destroy(ix);
        return NULL;
    }
    char b[CBM_SZ_3][CBM_SZ_32];
    snprintf(b[0], sizeof(b[0]), "%d", ix->nfiles);
    snprintf(b[SKIP_ONE], sizeof(b[SKIP_ONE]), "%d", ix->ndefs);
    snprintf(b[PAIR_LEN], sizeof(b[PAIR_LEN]), "%u", cbm_ht_count(ix->anchors));
    cbm_log_info("doc_links.c.index", "files", b[0], "definitions", b[SKIP_ONE], "anchors",
                 b[PAIR_LEN]);
    return ix;
}

/* ── Resolution ──────────────────────────────────────────────────── */

/* The candidates of one level, one per qualified name. */
typedef struct {
    const cr_def_t *items[CR_CANDS];
    int n;
    bool full; /* more than CR_CANDS: ambiguous */
} cr_cands_t;

static void cr_add(cr_cands_t *c, const cr_def_t *d) {
    for (int i = 0; i < c->n; i++) {
        if (strcmp(c->items[i]->qn, d->qn) == 0) {
            return;
        }
    }
    if (c->n == CR_CANDS) {
        c->full = true;
        return;
    }
    c->items[c->n++] = d;
}

static bool cr_macro(const cr_def_t *d) {
    return d->kind == 'm' || d->kind == 'M';
}

static bool cr_callable(const cr_def_t *d) {
    return d->kind == 'f' || d->kind == 'M';
}

/* Keep the candidates `keep` accepts, when there is one. */
static void cr_prefer(cr_cands_t *c, bool (*keep)(const cr_def_t *), bool want) {
    int k = 0;
    for (int i = 0; i < c->n; i++) {
        k += keep(c->items[i]) == want;
    }
    if (k == 0 || k == c->n) {
        return;
    }
    int w = 0;
    for (int i = 0; i < c->n; i++) {
        if (keep(c->items[i]) == want) {
            c->items[w++] = c->items[i];
        }
    }
    c->n = w;
}

enum { CR_LEVEL_OWN = 0, CR_LEVEL_FIELD = 1, CR_LEVEL_GLOBAL = 2, CR_LEVELS = 3 };

/* The definitions of `name` one level sees: the documented file's own (any
 * linkage, fields apart), its fields, or those every file sees. */
static void cr_level(const cr_index_t *ix, const char *name, int src, int level, cr_cands_t *c) {
    void *v = cbm_ht_get(ix->by_name, name);
    for (int i = v ? (int)((uintptr_t)v - SKIP_ONE) : CBM_NOT_FOUND; i >= 0; i = ix->defs[i].next) {
        const cr_def_t *d = &ix->defs[i];
        bool field = d->kind == 'r';
        bool in = level == CR_LEVEL_GLOBAL ? d->global
                                           : d->file == src && field == (level == CR_LEVEL_FIELD);
        if (in) {
            cr_add(c, d);
        }
    }
}

static void cr_out_node(const cr_index_t *ix, int src, const cr_def_t *d, bool exact,
                        const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(graph, d->qn);
    if (!node || !node->label) {
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
        return;
    }
    if (ix->files[d->file].test && !ix->files[src].test) {
        out->reason = CBM_DOCLINK_REASON_TEST_ONLY;
        return;
    }
    out->kind = CBM_DOCLINK_EDGE;
    out->target = node;
    out->exact = exact;
}

static bool cr_ident(const char *s, size_t n) {
    if (n == 0 || !(isalpha((unsigned char)s[0]) || s[0] == '_')) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (!isalnum((unsigned char)s[i]) && s[i] != '_') {
            return false;
        }
    }
    return true;
}

/* The name of a reference, `::` in front and `()` behind dropped, and the
 * type of `Type::member` (scope, "" for none); false when it is no C name. */
static bool cr_name(const char *raw, char *out, char *scope, size_t cap, bool *call) {
    size_t n = strlen(raw);
    if (n >= PAIR_LEN && raw[0] == ':' && raw[SKIP_ONE] == ':') {
        raw += PAIR_LEN;
        n -= PAIR_LEN;
    }
    *call = n >= PAIR_LEN && raw[n - PAIR_LEN] == '(' && raw[n - SKIP_ONE] == ')';
    if (*call) {
        n -= PAIR_LEN;
    }
    scope[0] = '\0';
    const char *sep = NULL;
    for (size_t i = 0; i + SKIP_ONE < n && !sep; i++) {
        sep = raw[i] == ':' && raw[i + SKIP_ONE] == ':' ? raw + i : NULL;
    }
    if (sep) {
        size_t sl = (size_t)(sep - raw);
        if (sl >= cap || !cr_ident(raw, sl)) {
            return false;
        }
        memcpy(scope, raw, sl);
        scope[sl] = '\0';
        n -= sl + PAIR_LEN;
        raw = sep + PAIR_LEN;
    }
    if (n >= cap || !cr_ident(raw, n)) {
        return false;
    }
    memcpy(out, raw, n);
    out[n] = '\0';
    return true;
}

static bool cr_type(const cr_def_t *d) {
    return d->kind == 't';
}

/* `Type::member`: the type by the usual levels, then its field (a node at
 * the type's qualified name + `.` + member) or its enum constant. */
static void cr_member(const cr_index_t *ix, int src, const char *scope, const char *name,
                      const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    const cr_def_t *type = NULL;
    for (int level = 0; level < CR_LEVELS && !type; level++) {
        cr_cands_t c = {.n = 0};
        cr_level(ix, scope, src, level, &c);
        cr_prefer(&c, cr_type, true);
        if (c.n == 0 || !cr_type(c.items[0])) {
            continue;
        }
        if (c.full || c.n > SKIP_ONE) {
            out->reason = CBM_DOCLINK_REASON_AMBIGUOUS;
            return;
        }
        type = c.items[0];
    }
    if (!type) {
        return; /* missing */
    }
    char qn[CR_QN_MAX];
    int n = snprintf(qn, sizeof(qn), "%s.%s", type->qn, name);
    cr_def_t field = {.file = type->file, .kind = 'r', .name = name, .qn = qn};
    if (n > 0 && (size_t)n < sizeof(qn) && cbm_gbuf_find_by_qn(graph, qn)) {
        cr_out_node(ix, src, &field, true, graph, out);
        return;
    }
    void *v = cbm_ht_get(ix->by_name, name);
    for (int i = v ? (int)((uintptr_t)v - SKIP_ONE) : CBM_NOT_FOUND; i >= 0; i = ix->defs[i].next) {
        const cr_def_t *d = &ix->defs[i];
        if (d->kind == 'e' && d->parent && strcmp(d->parent, type->qn) == 0) {
            cr_out_node(ix, src, d, true, graph, out);
            return;
        }
    }
}

/* A plain lowercase word: what an @see paragraph's prose is made of. */
static bool cr_plain_word(const char *s) {
    for (; *s; s++) {
        if (*s < 'a' || *s > 'z') {
            return false;
        }
    }
    return true;
}

static void cr_resolve_name(const cr_index_t *ix, int src, const CBMDocLink *link,
                            const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out);

static void cr_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                       const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    (void)state; /* C keeps no per-file working state */
    const cr_index_t *ix = (const cr_index_t *)index;
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = CBM_DOCLINK_REASON_MISSING;
    out->target = NULL;
    out->exact = false;
    if (!ix || !link->raw || run_file < 0 || run_file >= ix->run_count ||
        ix->run_to_file[run_file] < 0) {
        return;
    }
    int src = ix->run_to_file[run_file];
    cr_resolve_name(ix, src, link, graph, out);
    if (link->syntax == CBM_DOCLINK_C_SEE && out->kind == CBM_DOCLINK_UNRESOLVED &&
        out->reason == CBM_DOCLINK_REASON_MISSING && cr_plain_word(link->raw)) {
        out->kind = CBM_DOCLINK_LOCAL; /* prose: Doxygen prints it as text */
    }
}

static void cr_resolve_name(const cr_index_t *ix, int src, const CBMDocLink *link,
                            const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    if (cbm_ht_get(ix->anchors, link->raw)) {
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP; /* a page, group or anchor */
        return;
    }
    char name[CR_NAME_MAX];
    char scope[CR_NAME_MAX];
    bool call = false;
    if (strlen(link->raw) > CBM_DOCLINK_C_REF_MAX ||
        !cr_name(link->raw, name, scope, sizeof(name), &call)) {
        out->reason = CBM_DOCLINK_REASON_UNPARSEABLE;
        return;
    }
    if (scope[0]) {
        cr_member(ix, src, scope, name, graph, out);
        return;
    }
    if (cbm_ht_get(ix->anchors, name)) {
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
        return;
    }
    for (int level = 0; level < CR_LEVELS; level++) {
        cr_cands_t c = {.n = 0};
        cr_level(ix, name, src, level, &c);
        if (c.n == 0) {
            continue;
        }
        if (call) {
            cr_prefer(&c, cr_callable, true); /* `()` written: what can be called */
        }
        cr_prefer(&c, cr_macro, false); /* then a definition before a macro */
        if (c.full || c.n > SKIP_ONE) {
            out->reason = CBM_DOCLINK_REASON_AMBIGUOUS;
            return;
        }
        cr_out_node(ix, src, c.items[0], level != CR_LEVEL_GLOBAL, graph, out);
        return;
    }
}

/* A function, variable, enum constant, field or macro (D kind link name qn
 * parent, kind f v e r m M) and a Doxygen anchor (A name) each declare one
 * name. A type (t) is what members and Type::member are found through:
 * GLOBAL, as is every new or changed record. */
static bool cr_removable(const char *line, size_t len, const char **name, size_t *nlen) {
    if (len <= PAIR_LEN || line[1] != '\t') {
        return false;
    }
    if (line[0] == 'A') {
        return cbm_doclinks_scope_field(line, len, 1, name, nlen);
    }
    return line[0] == 'D' && strchr("fverMm", line[PAIR_LEN]) && line[PAIR_LEN] &&
           line[PAIR_LEN + SKIP_ONE] == '\t' &&
           cbm_doclinks_scope_field(line, len, CBM_SZ_3, name, nlen);
}

static int cr_scope_delta(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                          void *ud) {
    return cbm_doclinks_scope_delta_lines(stored, fresh, cr_removable, removed, ud);
}

const cbm_doclink_resolver_t cbm_doclink_c_resolver = {
    .langs = {CBM_LANG_C, CBM_LANG_CPP},
    .lang_count = PAIR_LEN,
    .scope_tag = CBM_DOCLINK_C_SCOPE_TAG,
    .build = cr_build,
    .destroy = cr_destroy,
    .resolve = cr_resolve,
    .scope_delta = cr_scope_delta,
};
