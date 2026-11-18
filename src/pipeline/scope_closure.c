/*
 * scope_closure.c — the closure and lookup of the shared cross-file layer in
 * unordered mode; see scope_closure.h.
 *
 * After the parked include-scope branch (round 5, include_scope.c sections 3
 * and 4): components of the include graph, the per-name reach memo and the
 * unique-provider step. Its ordered mode (shell, Make flow order, call
 * segments, barriers) is not part of this file.
 */
#include "pipeline/scope_closure.h"

#include "foundation/arena.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/mem_core.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Rows ────────────────────────────────────────────────────────────── */

static const cbm_closure_lang_t SC_ROWS[] = {
    /* Protobuf: an import is needed even inside one package; `import public`
     * passes the imported file's names on; a qualified type name is read
     * from the referring file's package outward. */
    {.lang = CBM_LANG_PROTOBUF, .qualified_refs = true},
};

const cbm_closure_lang_t *cbm_closure_lang(CBMLanguage lang) {
    for (size_t i = 0; i < sizeof(SC_ROWS) / sizeof(SC_ROWS[0]); i++) {
        if (SC_ROWS[i].lang == lang) {
            return &SC_ROWS[i];
        }
    }
    return NULL;
}

/* ── Storage ─────────────────────────────────────────────────────────── */

typedef struct {
    int *v;
    int n;
    int cap;
} sc_ivec_t;

typedef struct {
    const char *name;
    const char *full; /* full name, or NULL */
    const char *qn;   /* graph node, or NULL */
    int file;
    bool conditional;
} sc_def_t;

typedef struct {
    const char *rel;
    const cbm_closure_lang_t *row;
    const char *package; /* "" when none */
    sc_ivec_t includes;  /* target file per include (-1: none) */
    sc_ivec_t reexports; /* parallel: 1 for a re-exporting include */
    bool opaque;
    int comp;
} sc_file_t;

/* What a component exports of one name: up to two distinct definitions and
 * whether one of them is conditional (enough to tell none, one, several). */
typedef struct {
    int def[2];
    int n;
} sc_sum_t;

typedef struct {
    sc_ivec_t files;
    sc_ivec_t succ;           /* components it re-exports from (deduplicated) */
    sc_ivec_t pred;           /* components re-exporting from it */
    bool export_complete;     /* every file whole, every re-export resolved and complete */
    bool reexport_unresolved; /* a re-export names no file: unresolved, not opaque */
} sc_comp_t;

struct cbm_closure {
    CBMArena arena;
    sc_file_t *files;
    int nfiles;
    int cap_files;
    sc_def_t *defs;
    int ndefs;
    int cap_defs;
    sc_comp_t *comps;
    int ncomps;
    CBMHashTable *by_name;  /* name -> sc_ivec_t* of def indexes */
    CBMHashTable *packages; /* package -> sc_ivec_t* of file indexes (rows with package_visible) */
    CBMHashTable *reach;    /* name -> CBMHashTable* (comp+1 as key text -> sc_sum_t*) */
    bool sealed;
    uint64_t work;
};

static bool sc_push(CBMArena *a, sc_ivec_t *x, int val) {
    if (x->n >= x->cap) {
        int ncap = x->cap ? x->cap * PAIR_LEN : CBM_SZ_4;
        int *nv = cbm_arena_alloc(a, (size_t)ncap * sizeof(*nv));
        if (!nv) {
            return false;
        }
        if (x->n) {
            memcpy(nv, x->v, (size_t)x->n * sizeof(*nv));
        }
        x->v = nv;
        x->cap = ncap;
    }
    x->v[x->n++] = val;
    return true;
}

static sc_ivec_t *sc_slot(CBMArena *a, CBMHashTable *t, const char *key) {
    sc_ivec_t *x = cbm_ht_get(t, key);
    if (!x) {
        x = cbm_arena_alloc(a, sizeof(*x));
        const char *k = cbm_arena_strdup(a, key);
        if (!x || !k) {
            return NULL;
        }
        memset(x, 0, sizeof(*x));
        cbm_ht_set(t, k, x);
    }
    return x;
}

cbm_closure_t *cbm_closure_new(void) {
    cbm_closure_t *s = cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*s));
    if (!s) {
        return NULL;
    }
    memset(s, 0, sizeof(*s));
    cbm_arena_init(&s->arena);
    s->by_name = cbm_ht_create(CBM_SZ_256);
    s->packages = cbm_ht_create(CBM_SZ_16);
    s->reach = cbm_ht_create(CBM_SZ_256);
    if (!s->by_name || !s->packages || !s->reach) {
        cbm_closure_free(s);
        return NULL;
    }
    return s;
}

static void sc_free_reach(const char *key, void *val, void *ud) {
    (void)key;
    (void)ud;
    cbm_ht_free((CBMHashTable *)val);
}

void cbm_closure_free(cbm_closure_t *s) {
    if (!s) {
        return;
    }
    if (s->reach) {
        cbm_ht_foreach(s->reach, sc_free_reach, NULL);
        cbm_ht_free(s->reach);
    }
    if (s->by_name) {
        cbm_ht_free(s->by_name);
    }
    if (s->packages) {
        cbm_ht_free(s->packages);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, s->files);
    cbm_free(CBM_MEM_CLASS_OTHER, s->defs);
    cbm_free(CBM_MEM_CLASS_OTHER, s->comps);
    cbm_arena_destroy(&s->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, s);
}

uint64_t cbm_closure_work(const cbm_closure_t *s) {
    return s ? s->work : 0;
}

/* Grow a heap array of `size`-byte elements to hold one more. */
static bool sc_grow(void **arr, int n, int *cap, size_t size) {
    if (n < *cap) {
        return true;
    }
    int ncap = *cap ? *cap * PAIR_LEN : CBM_SZ_16;
    void *na = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)ncap * size);
    if (!na) {
        return false;
    }
    if (n) {
        memcpy(na, *arr, (size_t)n * size);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, *arr);
    *arr = na;
    *cap = ncap;
    return true;
}

int cbm_closure_add_file(cbm_closure_t *s, const char *rel, const cbm_closure_lang_t *row,
                         const char *package) {
    if (!s || s->sealed || !rel || !row ||
        !sc_grow((void **)&s->files, s->nfiles, &s->cap_files, sizeof(*s->files))) {
        return CBM_NOT_FOUND;
    }
    sc_file_t *f = &s->files[s->nfiles];
    memset(f, 0, sizeof(*f));
    f->rel = cbm_arena_strdup(&s->arena, rel);
    f->row = row;
    f->package = cbm_arena_strdup(&s->arena, package ? package : "");
    f->comp = -1;
    if (!f->rel || !f->package) {
        return CBM_NOT_FOUND;
    }
    if (row->package_visible && f->package[0]) {
        sc_ivec_t *mates = sc_slot(&s->arena, s->packages, f->package);
        if (!mates || !sc_push(&s->arena, mates, s->nfiles)) {
            return CBM_NOT_FOUND;
        }
    }
    return s->nfiles++;
}

bool cbm_closure_add_def(cbm_closure_t *s, int file, const char *name, const char *full,
                         const char *qn, bool conditional) {
    if (!s || s->sealed || file < 0 || file >= s->nfiles || !name || !name[0] ||
        !sc_grow((void **)&s->defs, s->ndefs, &s->cap_defs, sizeof(*s->defs))) {
        return false;
    }
    sc_def_t *d = &s->defs[s->ndefs];
    d->name = cbm_arena_strdup(&s->arena, name);
    d->full = full ? cbm_arena_strdup(&s->arena, full) : NULL;
    d->qn = qn ? cbm_arena_strdup(&s->arena, qn) : NULL;
    d->file = file;
    d->conditional = conditional;
    sc_ivec_t *named = d->name ? sc_slot(&s->arena, s->by_name, d->name) : NULL;
    if (!named || !sc_push(&s->arena, named, s->ndefs)) {
        return false;
    }
    s->ndefs++;
    return true;
}

bool cbm_closure_add_include(cbm_closure_t *s, int file, int target, bool reexport) {
    if (!s || s->sealed || file < 0 || file >= s->nfiles || target >= s->nfiles) {
        return false;
    }
    sc_file_t *f = &s->files[file];
    return sc_push(&s->arena, &f->includes, target < 0 ? CBM_NOT_FOUND : target) &&
           sc_push(&s->arena, &f->reexports, reexport ? SKIP_ONE : 0);
}

bool cbm_closure_add_opaque(cbm_closure_t *s, int file) {
    if (!s || s->sealed || file < 0 || file >= s->nfiles) {
        return false;
    }
    s->files[file].opaque = true;
    return true;
}

/* ── Components (iterative Tarjan over re-export edges) ──────────────── */

typedef struct {
    int *index;
    int *low;
    bool *on_stack;
    int *stack;
    int top;
    int *call_node;
    int *call_edge;
    int next;
} sc_tarjan_t;

static bool sc_new_comp(cbm_closure_t *s, sc_tarjan_t *t, int root) {
    int c = s->ncomps++;
    sc_comp_t *comp = &s->comps[c];
    memset(comp, 0, sizeof(*comp));
    int w;
    do {
        w = t->stack[--t->top];
        t->on_stack[w] = false;
        s->files[w].comp = c;
        if (!sc_push(&s->arena, &comp->files, w)) {
            return false;
        }
        s->work++;
    } while (w != root);
    return true;
}

/* One depth-first search from `start`, without recursion. */
static bool sc_tarjan_from(cbm_closure_t *s, sc_tarjan_t *t, int start) {
    int depth = 0;
    t->call_node[0] = start;
    t->call_edge[0] = 0;
    t->index[start] = t->low[start] = t->next++;
    t->stack[t->top++] = start;
    t->on_stack[start] = true;
    while (depth >= 0) {
        int v = t->call_node[depth];
        sc_file_t *f = &s->files[v];
        bool descended = false;
        while (t->call_edge[depth] < f->includes.n) {
            int e = t->call_edge[depth]++;
            int w = f->includes.v[e];
            s->work++;
            if (w < 0 || !f->reexports.v[e]) {
                continue;
            }
            if (t->index[w] < 0) {
                t->index[w] = t->low[w] = t->next++;
                t->stack[t->top++] = w;
                t->on_stack[w] = true;
                depth++;
                t->call_node[depth] = w;
                t->call_edge[depth] = 0;
                descended = true;
                break;
            }
            if (t->on_stack[w] && t->index[w] < t->low[v]) {
                t->low[v] = t->index[w];
            }
        }
        if (descended) {
            continue;
        }
        if (t->low[v] == t->index[v] && !sc_new_comp(s, t, v)) {
            return false;
        }
        depth--;
        if (depth >= 0) {
            int u = t->call_node[depth];
            if (t->low[v] < t->low[u]) {
                t->low[u] = t->low[v];
            }
        }
    }
    return true;
}

/* Successor and predecessor components, without duplicates. */
static bool sc_comp_edges(cbm_closure_t *s) {
    int *seen = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)(s->ncomps ? s->ncomps : 1) * sizeof(int));
    if (!seen) {
        return false;
    }
    for (int c = 0; c < s->ncomps; c++) {
        seen[c] = -1;
    }
    bool ok = true;
    for (int c = 0; ok && c < s->ncomps; c++) {
        sc_comp_t *comp = &s->comps[c];
        for (int i = 0; ok && i < comp->files.n; i++) {
            const sc_file_t *f = &s->files[comp->files.v[i]];
            for (int e = 0; ok && e < f->includes.n; e++) {
                s->work++;
                if (!f->reexports.v[e]) {
                    continue;
                }
                int w = f->includes.v[e];
                if (w < 0) {
                    comp->reexport_unresolved = true;
                    continue;
                }
                int d = s->files[w].comp;
                if (d == c || seen[d] == c) {
                    continue;
                }
                seen[d] = c;
                ok = sc_push(&s->arena, &comp->succ, d) && sc_push(&s->arena, &s->comps[d].pred, c);
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, seen);
    return ok;
}

/* Tarjan numbers components in reverse topological order of the re-export
 * graph: a component's successors have smaller numbers, so one pass upward
 * settles export completeness. */
static void sc_export_complete(cbm_closure_t *s) {
    for (int c = 0; c < s->ncomps; c++) {
        sc_comp_t *comp = &s->comps[c];
        bool ok = !comp->reexport_unresolved;
        for (int i = 0; ok && i < comp->files.n; i++) {
            ok = !s->files[comp->files.v[i]].opaque;
        }
        for (int i = 0; ok && i < comp->succ.n; i++) {
            ok = s->comps[comp->succ.v[i]].export_complete;
        }
        comp->export_complete = ok;
        s->work++;
    }
}

int cbm_closure_seal(cbm_closure_t *s) {
    if (!s || s->sealed) {
        return CBM_NOT_FOUND;
    }
    int n = s->nfiles;
    size_t m = (size_t)(n ? n : 1);
    sc_tarjan_t t = {0};
    t.index = cbm_alloc(CBM_MEM_CLASS_OTHER, m * sizeof(int));
    t.low = cbm_alloc(CBM_MEM_CLASS_OTHER, m * sizeof(int));
    t.on_stack = cbm_alloc(CBM_MEM_CLASS_OTHER, m * sizeof(bool));
    t.stack = cbm_alloc(CBM_MEM_CLASS_OTHER, m * sizeof(int));
    t.call_node = cbm_alloc(CBM_MEM_CLASS_OTHER, m * sizeof(int));
    t.call_edge = cbm_alloc(CBM_MEM_CLASS_OTHER, m * sizeof(int));
    s->comps = cbm_alloc(CBM_MEM_CLASS_OTHER, m * sizeof(*s->comps));
    bool ok = t.index && t.low && t.on_stack && t.stack && t.call_node && t.call_edge && s->comps;
    for (int i = 0; ok && i < n; i++) {
        t.index[i] = -1;
        t.on_stack[i] = false;
    }
    for (int i = 0; ok && i < n; i++) {
        if (t.index[i] < 0) {
            ok = sc_tarjan_from(s, &t, i);
        }
    }
    ok = ok && sc_comp_edges(s);
    if (ok) {
        sc_export_complete(s);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, t.index);
    cbm_free(CBM_MEM_CLASS_OTHER, t.low);
    cbm_free(CBM_MEM_CLASS_OTHER, t.on_stack);
    cbm_free(CBM_MEM_CLASS_OTHER, t.stack);
    cbm_free(CBM_MEM_CLASS_OTHER, t.call_node);
    cbm_free(CBM_MEM_CLASS_OTHER, t.call_edge);
    s->sealed = ok;
    return ok ? 0 : CBM_NOT_FOUND;
}

/* ── Lookup ──────────────────────────────────────────────────────────── */

static void sc_sum_add(sc_sum_t *sum, int def) {
    for (int i = 0; i < sum->n && i < PAIR_LEN; i++) {
        if (sum->def[i] == def) {
            return;
        }
    }
    if (sum->n < PAIR_LEN) {
        sum->def[sum->n] = def;
    }
    if (sum->n <= PAIR_LEN) {
        sum->n++; /* n == 3 means "more than two"; the verdict needs no more */
    }
}

static void sc_sum_merge(sc_sum_t *into, const sc_sum_t *from) {
    for (int i = 0; i < from->n && i < PAIR_LEN; i++) {
        sc_sum_add(into, from->def[i]);
    }
    if (from->n > PAIR_LEN) {
        into->n = PAIR_LEN + SKIP_ONE;
    }
}

/* Whether a definition answers the reference, by name and (for a row with
 * qualified references) by its full name read from `package` outward. */
static bool sc_def_matches(const sc_def_t *d, const cbm_closure_ref_t *ref, const char *package,
                           bool qualified) {
    if (strcmp(d->name, ref->name) != 0) {
        return false;
    }
    if (!qualified || !ref->qual || !ref->qual[0]) {
        return true; /* a bare name: by its last segment */
    }
    if (!d->full) {
        return false;
    }
    char want[CBM_SZ_1K];
    if (ref->qual[0] == '.') {
        int n = snprintf(want, sizeof(want), "%s.%s", ref->qual + SKIP_ONE, ref->name);
        return n > 0 && (size_t)n < sizeof(want) && strcmp(d->full, want) == 0;
    }
    /* "a.b" in package "p.q": p.q.a.b, p.a.b, a.b */
    size_t plen = strlen(package);
    for (;;) {
        int n = plen ? snprintf(want, sizeof(want), "%.*s.%s.%s", (int)plen, package, ref->qual,
                                ref->name)
                     : snprintf(want, sizeof(want), "%s.%s", ref->qual, ref->name);
        if (n > 0 && (size_t)n < sizeof(want) && strcmp(d->full, want) == 0) {
            return true;
        }
        if (plen == 0) {
            return false;
        }
        while (plen > 0 && package[plen - SKIP_ONE] != '.') {
            plen--;
        }
        if (plen > 0) {
            plen--;
        }
    }
}

/* The reach memo of a name: for each component that exports a definition
 * named `name`, what it exports. Built once, by a backward search from the
 * components of the defining files along re-export edges. */
static CBMHashTable *sc_reach(cbm_closure_t *s, const char *name, const sc_ivec_t *named) {
    CBMHashTable *memo = cbm_ht_get(s->reach, name);
    if (memo) {
        return memo;
    }
    memo = cbm_ht_create(CBM_SZ_16);
    const char *key = cbm_arena_strdup(&s->arena, name);
    if (!memo || !key) {
        if (memo) {
            cbm_ht_free(memo);
        }
        return NULL;
    }
    cbm_ht_set(s->reach, key, memo);
    for (int k = 0; named && k < named->n; k++) {
        int d = named->v[k];
        /* every component this definition reaches, each once per definition */
        sc_ivec_t queue = {0};
        CBMHashTable *seen = cbm_ht_create(CBM_SZ_16);
        if (!seen || !sc_push(&s->arena, &queue, s->files[s->defs[d].file].comp)) {
            if (seen) {
                cbm_ht_free(seen);
            }
            return NULL;
        }
        for (int q = 0; q < queue.n; q++) {
            int c = queue.v[q];
            char ck[CBM_SZ_16];
            snprintf(ck, sizeof(ck), "%d", c);
            s->work++;
            if (cbm_ht_get(seen, ck)) {
                continue;
            }
            const char *ckey = cbm_arena_strdup(&s->arena, ck);
            sc_sum_t *sum = cbm_ht_get(memo, ck);
            if (!sum && ckey) {
                sum = cbm_arena_alloc(&s->arena, sizeof(*sum));
                if (sum) {
                    memset(sum, 0, sizeof(*sum));
                    cbm_ht_set(memo, ckey, sum);
                }
            }
            if (!sum || !ckey) {
                cbm_ht_free(seen);
                return NULL;
            }
            cbm_ht_set(seen, ckey, (void *)SKIP_ONE);
            sc_sum_add(sum, d);
            const sc_comp_t *comp = &s->comps[c];
            for (int i = 0; i < comp->pred.n; i++) {
                if (!sc_push(&s->arena, &queue, comp->pred.v[i])) {
                    cbm_ht_free(seen);
                    return NULL;
                }
            }
        }
        cbm_ht_free(seen);
    }
    return memo;
}

/* What a component exports of the name: the memo's summary, else nothing. */
static const sc_sum_t *sc_exports(CBMHashTable *memo, int comp) {
    char ck[CBM_SZ_16];
    snprintf(ck, sizeof(ck), "%d", comp);
    return memo ? cbm_ht_get(memo, ck) : NULL;
}

/* The definitions a file sees of the name, and how the closure stands. */
static void sc_visible(cbm_closure_t *s, int file, const cbm_closure_ref_t *ref, sc_sum_t *sum,
                       uint8_t *via, uint32_t *why) {
    const sc_file_t *f = &s->files[file];
    const sc_ivec_t *named = cbm_ht_get(s->by_name, ref->name);
    CBMHashTable *memo = named ? sc_reach(s, ref->name, named) : NULL;
    if (f->opaque) {
        *why |= CBM_CLOSURE_WHY_OPAQUE;
    }
    if (f->row->prelude_external) {
        *why |= CBM_CLOSURE_WHY_PRELUDE;
    }
    /* own definitions, then package mates */
    for (int k = 0; named && k < named->n; k++) {
        s->work++;
        const sc_def_t *d = &s->defs[named->v[k]];
        bool mate = f->row->package_visible && f->package[0] && d->file != file &&
                    strcmp(s->files[d->file].package, f->package) == 0 &&
                    s->files[d->file].row == f->row;
        if ((d->file == file || mate) &&
            sc_def_matches(d, ref, f->package, f->row->qualified_refs)) {
            sc_sum_add(sum, named->v[k]);
            *via = d->file == file ? CBM_CLOSURE_VIA_OWN : CBM_CLOSURE_VIA_PACKAGE;
        }
    }
    if (f->row->package_visible && f->package[0]) {
        const sc_ivec_t *mates = cbm_ht_get(s->packages, f->package);
        for (int i = 0; mates && i < mates->n; i++) {
            s->work++;
            if (s->files[mates->v[i]].opaque) {
                *why |= CBM_CLOSURE_WHY_OPAQUE;
            }
        }
    }
    /* what each include exports */
    for (int e = 0; e < f->includes.n; e++) {
        s->work++;
        int t = f->includes.v[e];
        if (t < 0) {
            *why |= CBM_CLOSURE_WHY_UNRESOLVED;
            continue;
        }
        const sc_comp_t *comp = &s->comps[s->files[t].comp];
        if (!comp->export_complete) {
            *why |= comp->reexport_unresolved ? CBM_CLOSURE_WHY_UNRESOLVED : CBM_CLOSURE_WHY_OPAQUE;
        }
        const sc_sum_t *ex = sc_exports(memo, s->files[t].comp);
        if (!ex) {
            continue;
        }
        sc_sum_t matched = {{0, 0}, 0};
        for (int i = 0; i < ex->n && i < PAIR_LEN; i++) {
            if (sc_def_matches(&s->defs[ex->def[i]], ref, f->package, f->row->qualified_refs)) {
                sc_sum_add(&matched, ex->def[i]);
            }
        }
        if (ex->n > PAIR_LEN) {
            matched.n = PAIR_LEN + SKIP_ONE; /* more than the summary keeps: several */
        }
        if (matched.n > 0) {
            sc_sum_merge(sum, &matched);
            *via = CBM_CLOSURE_VIA_INCLUDE;
        }
    }
}

cbm_closure_answer_t cbm_closure_lookup(cbm_closure_t *s, int file, const cbm_closure_ref_t *ref) {
    cbm_closure_answer_t a = {.verdict = CBM_CLOSURE_NO_ANSWER, .file = -1};
    if (!s || !s->sealed || file < 0 || file >= s->nfiles || !ref || !ref->name || !ref->name[0]) {
        return a;
    }
    if (ref->flags & CBM_CLOSURE_REF_LOCAL) {
        a.why = CBM_CLOSURE_WHY_LOCAL;
        return a;
    }
    sc_sum_t sum = {{0, 0}, 0};
    uint8_t via = 0;
    uint32_t why = 0;
    sc_visible(s, file, ref, &sum, &via, &why);
    a.visible = sum.n > PAIR_LEN ? PAIR_LEN : sum.n;
    a.complete =
        (why & (CBM_CLOSURE_WHY_UNRESOLVED | CBM_CLOSURE_WHY_OPAQUE | CBM_CLOSURE_WHY_PRELUDE)) ==
        0;
    if (sum.n == 0) {
        a.verdict = a.complete ? CBM_CLOSURE_PROVEN_ABSENT : CBM_CLOSURE_NO_ANSWER;
        a.why = why;
        return a;
    }
    if (sum.n > SKIP_ONE) {
        a.why = why | CBM_CLOSURE_WHY_AMBIGUOUS;
        return a;
    }
    const sc_def_t *d = &s->defs[sum.def[0]];
    if (!d->qn) {
        a.why = why | CBM_CLOSURE_WHY_NO_NODE;
        return a;
    }
    if (d->conditional) {
        why |= CBM_CLOSURE_WHY_CONDITIONAL;
    }
    a.qn = d->qn;
    a.file = d->file;
    a.via = via;
    a.why = why;
    /* a row where a second visible definition would not compile: one found
     * past an unresolved source or a prelude is still the one (never past an
     * opaque part or a conditional definition: those may hide or not hold) */
    uint32_t unproven = why;
    if (s->files[file].row->ambiguity_is_error) {
        unproven &= ~(uint32_t)(CBM_CLOSURE_WHY_UNRESOLVED | CBM_CLOSURE_WHY_PRELUDE);
    }
    a.verdict = unproven == 0 ? CBM_CLOSURE_PROVEN : CBM_CLOSURE_POSSIBLE;
    return a;
}
