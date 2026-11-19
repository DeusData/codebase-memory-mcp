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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Rows ────────────────────────────────────────────────────────────── */

static const cbm_closure_lang_t SC_ROWS[] = {
    /* Protobuf: an import is needed even inside one package; `import public`
     * passes the imported file's names on; a type reference is read against
     * full names from the scopes around it (protoc's scope walk). */
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
    const char *package; /* "" when none, NULL when unknown */
    sc_ivec_t includes;  /* target file per include (-1: none) */
    sc_ivec_t reexports; /* parallel: 1 for a re-exporting include */
    bool opaque;
    int comp;
} sc_file_t;

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
    CBMHashTable *by_rel;   /* rel -> file index + 1 */
    /* qualified rows (protoc's scope walk): every definition's full name ->
     * sc_ivec_t* of def indexes; every package and each package around it;
     * "<file>\x1f<name>" for a definition nested in a message of that file */
    CBMHashTable *fulls;
    CBMHashTable *pkgs;
    CBMHashTable *nested;
    bool sealed;
    uint64_t work;
};

/* One thread's memo. Per name, the components that export each definition of
 * it (a backward search, built once); per file of the last lookups, its start
 * components (its own and its includes'), its why bits and, once it asked a
 * second name no reach was built for, every component it sees (a forward
 * search). A lookup reads whichever is smaller; no answer depends on which. */
struct cbm_closure_memo {
    const cbm_closure_t *owner; /* the closure the memo was filled from */
    CBMArena arena;
    CBMHashTable *reach; /* name -> CBMHashTable* ("#def" -> sc_ivec_t* of components,
                          * "comp:def" -> present) */
    uint32_t *stamp;     /* per component: the backward search that reached it last */
    uint32_t epoch;      /* the current backward search */
    int *queue;          /* per component: a search's queue */
    uint32_t *start;     /* per component: file_mark when a start of cur_file */
    uint32_t *seen;      /* per component: file_mark when cur_file sees it (forward) */
    uint32_t fepoch;     /* file marks used */
    uint32_t file_mark;
    int cur_file; /* the file start/seen/file_why describe, -1 none */
    int nstarts;
    int misses; /* names cur_file built a reach for */
    bool forward;
    uint32_t file_why;
    int ncomps;
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
    s->by_rel = cbm_ht_create(CBM_SZ_256);
    s->fulls = cbm_ht_create(CBM_SZ_256);
    s->pkgs = cbm_ht_create(CBM_SZ_16);
    s->nested = cbm_ht_create(CBM_SZ_16);
    if (!s->by_name || !s->packages || !s->by_rel || !s->fulls || !s->pkgs || !s->nested) {
        cbm_closure_free(s);
        return NULL;
    }
    return s;
}

void cbm_closure_free(cbm_closure_t *s) {
    if (!s) {
        return;
    }
    if (s->by_name) {
        cbm_ht_free(s->by_name);
    }
    if (s->packages) {
        cbm_ht_free(s->packages);
    }
    if (s->by_rel) {
        cbm_ht_free(s->by_rel);
    }
    if (s->fulls) {
        cbm_ht_free(s->fulls);
    }
    if (s->pkgs) {
        cbm_ht_free(s->pkgs);
    }
    if (s->nested) {
        cbm_ht_free(s->nested);
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

int cbm_closure_file_count(const cbm_closure_t *s) {
    return s ? s->nfiles : 0;
}

int cbm_closure_def_count(const cbm_closure_t *s) {
    return s ? s->ndefs : 0;
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
    if (!s || s->sealed || !rel || !row || cbm_ht_get(s->by_rel, rel) ||
        !sc_grow((void **)&s->files, s->nfiles, &s->cap_files, sizeof(*s->files))) {
        return CBM_NOT_FOUND;
    }
    sc_file_t *f = &s->files[s->nfiles];
    memset(f, 0, sizeof(*f));
    f->rel = cbm_arena_strdup(&s->arena, rel);
    f->row = row;
    f->package = package ? cbm_arena_strdup(&s->arena, package) : NULL;
    f->comp = -1;
    if (!f->rel || (package && !f->package)) {
        return CBM_NOT_FOUND;
    }
    if (row->package_visible && f->package && f->package[0]) {
        sc_ivec_t *mates = sc_slot(&s->arena, s->packages, f->package);
        if (!mates || !sc_push(&s->arena, mates, s->nfiles)) {
            return CBM_NOT_FOUND;
        }
    }
    if (row->qualified_refs && f->package && f->package[0]) {
        for (size_t l = strlen(f->package); l > 0;) {
            char *k = cbm_arena_alloc(&s->arena, l + SKIP_ONE);
            if (!k) {
                return CBM_NOT_FOUND;
            }
            memcpy(k, f->package, l);
            k[l] = '\0';
            cbm_ht_set(s->pkgs, k, k);
            while (l > 0 && f->package[l - SKIP_ONE] != '.') {
                l--;
            }
            l = l > 0 ? l - SKIP_ONE : 0;
        }
    }
    cbm_ht_set(s->by_rel, f->rel, (void *)(intptr_t)(s->nfiles + SKIP_ONE));
    return s->nfiles++;
}

int cbm_closure_file(const cbm_closure_t *s, const char *rel) {
    if (!s || !rel) {
        return CBM_NOT_FOUND;
    }
    intptr_t v = (intptr_t)cbm_ht_get(s->by_rel, rel);
    return v > 0 ? (int)(v - SKIP_ONE) : CBM_NOT_FOUND;
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
    const sc_file_t *f = &s->files[file];
    if (f->row->qualified_refs && d->full) {
        sc_ivec_t *same = sc_slot(&s->arena, s->fulls, d->full);
        if (!same || !sc_push(&s->arena, same, s->ndefs)) {
            return false;
        }
        /* nested in a message of the file: its full name is not the package's
         * name followed by its own */
        size_t fl = strlen(d->full);
        size_t nl = strlen(d->name);
        size_t pl = fl > nl ? fl - nl - SKIP_ONE : 0;
        const char *pkg = f->package ? f->package : "";
        bool at_package =
            fl == nl ? !pkg[0] : (strlen(pkg) == pl && strncmp(d->full, pkg, pl) == 0);
        if (!at_package) {
            const char *k = cbm_arena_sprintf(&s->arena, "%d\x1f%s", file, d->name);
            if (!k) {
                return false;
            }
            cbm_ht_set(s->nested, k, (void *)k);
        }
    }
    s->ndefs++;
    s->work++;
    return true;
}

bool cbm_closure_add_include(cbm_closure_t *s, int file, int target, bool reexport) {
    if (!s || s->sealed || file < 0 || file >= s->nfiles || target >= s->nfiles) {
        return false;
    }
    sc_file_t *f = &s->files[file];
    s->work++;
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
    bool all; /* every include edge (marks cycles), else re-export edges (components) */
} sc_tarjan_t;

static bool sc_new_comp(cbm_closure_t *s, sc_tarjan_t *t, int root) {
    if (t->all) {
        /* an import cycle of more than one file: each one is opaque */
        int k = t->top;
        while (k > 0 && t->stack[k - SKIP_ONE] != root) {
            k--;
        }
        k = k > 0 ? k - SKIP_ONE : 0;
        bool cycle = t->top - k > SKIP_ONE;
        for (int i = k; i < t->top; i++) {
            t->on_stack[t->stack[i]] = false;
            if (cycle) {
                s->files[t->stack[i]].opaque = true;
            }
            s->work++;
        }
        t->top = k;
        return true;
    }
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
            if (w < 0 || (!t->all && !f->reexports.v[e])) {
                continue;
            }
            if (t->all && w == v) {
                f->opaque = true; /* imports itself */
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
    for (int pass = 0; ok && pass < PAIR_LEN; pass++) {
        t.all = pass == 0; /* first the import cycles, then the components */
        t.top = 0;
        t.next = 0;
        for (int i = 0; i < n; i++) {
            t.index[i] = -1;
            t.on_stack[i] = false;
        }
        for (int i = 0; ok && i < n; i++) {
            if (t.index[i] < 0) {
                ok = sc_tarjan_from(s, &t, i);
            }
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

/* ── The memo ────────────────────────────────────────────────────────── */

cbm_closure_memo_t *cbm_closure_memo_new(void) {
    cbm_closure_memo_t *m = cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*m));
    if (!m) {
        return NULL;
    }
    memset(m, 0, sizeof(*m));
    cbm_arena_init(&m->arena);
    return m;
}

static void sc_free_reach(const char *key, void *val, void *ud) {
    (void)key;
    (void)ud;
    cbm_ht_free((CBMHashTable *)val);
}

/* Forget everything (another closure, or the memo's first use). */
static void sc_memo_clear(cbm_closure_memo_t *m) {
    if (m->reach) {
        cbm_ht_foreach(m->reach, sc_free_reach, NULL);
        cbm_ht_free(m->reach);
        m->reach = NULL;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, m->stamp);
    cbm_free(CBM_MEM_CLASS_OTHER, m->queue);
    cbm_free(CBM_MEM_CLASS_OTHER, m->start);
    cbm_free(CBM_MEM_CLASS_OTHER, m->seen);
    m->stamp = NULL;
    m->queue = NULL;
    m->start = NULL;
    m->seen = NULL;
    m->ncomps = 0;
    m->owner = NULL;
    m->cur_file = -1;
    cbm_arena_destroy(&m->arena);
    cbm_arena_init(&m->arena);
}

void cbm_closure_memo_free(cbm_closure_memo_t *m) {
    if (!m) {
        return;
    }
    sc_memo_clear(m);
    cbm_arena_destroy(&m->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, m);
}

uint64_t cbm_closure_memo_work(const cbm_closure_memo_t *m) {
    return m ? m->work : 0;
}

/* Bind the memo to `s`: emptied when it was filled from another closure. */
static bool sc_memo_bind(cbm_closure_memo_t *m, const cbm_closure_t *s) {
    if (m->owner == s && m->reach) {
        return true;
    }
    uint64_t work = m->work;
    sc_memo_clear(m);
    m->work = work;
    size_t n = (size_t)(s->ncomps ? s->ncomps : 1);
    m->reach = cbm_ht_create(CBM_SZ_256);
    m->stamp = cbm_alloc(CBM_MEM_CLASS_OTHER, n * sizeof(*m->stamp));
    m->queue = cbm_alloc(CBM_MEM_CLASS_OTHER, n * sizeof(int));
    m->start = cbm_alloc(CBM_MEM_CLASS_OTHER, n * sizeof(*m->start));
    m->seen = cbm_alloc(CBM_MEM_CLASS_OTHER, n * sizeof(*m->seen));
    if (!m->reach || !m->stamp || !m->queue || !m->start || !m->seen) {
        sc_memo_clear(m);
        return false;
    }
    memset(m->stamp, 0, n * sizeof(*m->stamp));
    memset(m->start, 0, n * sizeof(*m->start));
    memset(m->seen, 0, n * sizeof(*m->seen));
    m->epoch = 0;
    m->fepoch = 0;
    m->cur_file = -1;
    m->ncomps = s->ncomps;
    m->owner = s;
    return true;
}

/* A mark no component carries yet: one per backward search. */
static uint32_t sc_memo_epoch(cbm_closure_memo_t *m) {
    if (m->epoch == UINT32_MAX) {
        memset(m->stamp, 0, (size_t)(m->ncomps ? m->ncomps : 1) * sizeof(*m->stamp));
        m->epoch = 0;
    }
    return ++m->epoch;
}

/* The reach of a name, built once per memo: for each definition named `name`,
 * the components that export it, its own and every one re-exporting it
 * (transitively): "#def" -> sc_ivec_t of components, and "comp:def". A
 * backward search from each definition's component along re-export edges,
 * each component once per definition. */
static CBMHashTable *sc_reach(const cbm_closure_t *s, cbm_closure_memo_t *m, const char *name,
                              const sc_ivec_t *named) {
    CBMHashTable *reach = cbm_ht_get(m->reach, name);
    if (reach) {
        return reach;
    }
    reach = cbm_ht_create(CBM_SZ_16);
    const char *key = cbm_arena_strdup(&m->arena, name);
    if (!reach || !key) {
        if (reach) {
            cbm_ht_free(reach);
        }
        return NULL;
    }
    /* kept only once whole: a failed build leaves the memo as it was */
    bool ok = true;
    for (int k = 0; ok && named && k < named->n; k++) {
        int d = named->v[k];
        sc_ivec_t *anc = cbm_arena_alloc(&m->arena, sizeof(*anc));
        const char *dk = cbm_arena_sprintf(&m->arena, "#%d", d);
        ok = anc && dk;
        if (!ok) {
            break;
        }
        memset(anc, 0, sizeof(*anc));
        cbm_ht_set(reach, dk, anc);
        int head = 0;
        int tail = 0;
        int start = s->files[s->defs[d].file].comp;
        uint32_t mark = sc_memo_epoch(m);
        m->queue[tail++] = start;
        m->stamp[start] = mark;
        while (ok && head < tail) {
            int c = m->queue[head++];
            m->work++;
            const char *pk = cbm_arena_sprintf(&m->arena, "%d:%d", c, d);
            ok = pk && sc_push(&m->arena, anc, c);
            if (ok) {
                cbm_ht_set(reach, pk, (void *)pk);
            }
            const sc_comp_t *comp = &s->comps[c];
            for (int i = 0; ok && i < comp->pred.n; i++) {
                int p = comp->pred.v[i];
                if (m->stamp[p] != mark) {
                    m->stamp[p] = mark;
                    m->queue[tail++] = p;
                }
            }
        }
    }
    if (!ok) {
        cbm_ht_free(reach);
        return NULL;
    }
    cbm_ht_set(m->reach, key, reach);
    return reach;
}

/* The file's start components (its own and each include's), marked, and its
 * why bits: once per file (a file's lookups come together). */
static void sc_memo_file(const cbm_closure_t *s, cbm_closure_memo_t *m, int file) {
    if (m->cur_file == file) {
        return;
    }
    if (m->fepoch == UINT32_MAX) {
        memset(m->start, 0, (size_t)(m->ncomps ? m->ncomps : 1) * sizeof(*m->start));
        memset(m->seen, 0, (size_t)(m->ncomps ? m->ncomps : 1) * sizeof(*m->seen));
        m->fepoch = 0;
    }
    m->file_mark = ++m->fepoch;
    m->cur_file = file;
    m->forward = false;
    m->misses = 0;
    const sc_file_t *f = &s->files[file];
    uint32_t why = 0;
    m->start[f->comp] = m->file_mark;
    m->nstarts = 1;
    for (int e = 0; e < f->includes.n; e++) {
        m->work++;
        int t = f->includes.v[e];
        if (t < 0) {
            why |= CBM_CLOSURE_WHY_UNRESOLVED;
            continue;
        }
        const sc_comp_t *comp = &s->comps[s->files[t].comp];
        if (!comp->export_complete) {
            why |= comp->reexport_unresolved ? CBM_CLOSURE_WHY_UNRESOLVED : CBM_CLOSURE_WHY_OPAQUE;
        }
        m->start[s->files[t].comp] = m->file_mark;
        m->nstarts++;
    }
    if (f->opaque) {
        why |= CBM_CLOSURE_WHY_OPAQUE;
    }
    if (f->row->prelude_external) {
        why |= CBM_CLOSURE_WHY_PRELUDE;
    }
    m->file_why = why;
}

/* Every component the current file sees: forward from its starts along
 * re-export edges, each component once. */
static void sc_memo_forward(const cbm_closure_t *s, cbm_closure_memo_t *m, int file) {
    const sc_file_t *f = &s->files[file];
    uint32_t mark = m->file_mark;
    int head = 0;
    int tail = 0;
    m->seen[f->comp] = mark;
    m->queue[tail++] = f->comp;
    for (int e = 0; e < f->includes.n; e++) {
        int t = f->includes.v[e];
        m->work++;
        if (t >= 0 && m->seen[s->files[t].comp] != mark) {
            m->seen[s->files[t].comp] = mark;
            m->queue[tail++] = s->files[t].comp;
        }
    }
    while (head < tail) {
        const sc_comp_t *comp = &s->comps[m->queue[head++]];
        for (int i = 0; i < comp->succ.n; i++) {
            int d = comp->succ.v[i];
            m->work++;
            if (m->seen[d] != mark) {
                m->seen[d] = mark;
                m->queue[tail++] = d;
            }
        }
    }
    m->forward = true;
}

/* Whether the current file sees definition `d` (named `name`): 1 yes, 0 no,
 * -1 out of memory. Its own component and its includes' are its starts; past
 * them, either the name's reach (iterating the fewer of the definition's
 * exporters and the file's starts) or, once the file asked a second name
 * without a reach, its forward search. */
static int sc_sees(const cbm_closure_t *s, cbm_closure_memo_t *m, int file, const char *name,
                   int d) {
    int c = s->files[s->defs[d].file].comp;
    m->work++;
    if (m->start[c] == m->file_mark) {
        return 1;
    }
    if (m->forward) {
        return m->seen[c] == m->file_mark;
    }
    CBMHashTable *reach = cbm_ht_get(m->reach, name);
    if (!reach) {
        if (m->misses > 0) {
            sc_memo_forward(s, m, file);
            return m->seen[c] == m->file_mark;
        }
        m->misses++;
        const sc_ivec_t *named = cbm_ht_get(s->by_name, name);
        reach = named ? sc_reach(s, m, name, named) : NULL;
        if (!reach) {
            return -1;
        }
    }
    char k[CBM_SZ_32];
    snprintf(k, sizeof(k), "#%d", d);
    const sc_ivec_t *anc = cbm_ht_get(reach, k);
    int na = anc ? anc->n : 0;
    if (na <= m->nstarts) {
        for (int i = 0; i < na; i++) {
            m->work++;
            if (m->start[anc->v[i]] == m->file_mark) {
                return 1;
            }
        }
        return 0;
    }
    const sc_file_t *f = &s->files[file];
    for (int e = -1; e < f->includes.n; e++) {
        int t = e < 0 ? file : f->includes.v[e];
        m->work++;
        if (t < 0) {
            continue;
        }
        snprintf(k, sizeof(k), "%d:%d", s->files[t].comp, d);
        if (cbm_ht_get(reach, k)) {
            return 1;
        }
    }
    return 0;
}

/* ── Lookup ──────────────────────────────────────────────────────────── */

enum { SC_FULL_MAX = 1024 };

/* protoc's scope walk for a reference written `r` (DescriptorBuilder::
 * LookupSymbol): the innermost scope around the site in which the FIRST
 * component names a symbol decides, and the whole reference must be there (no
 * backtracking). The package scopes are known (the file's package, each
 * package around it, the root); the enclosing message is not: a message of
 * the referring file that holds a definition of the first component's name may
 * decide first, which makes an answer at most possible. Symbols are read from
 * the whole run, a superset of what protoc sees: a scope taken too early only
 * finds nothing visible there (no answer), never another definition. */
typedef struct {
    const char *target; /* the full name the reference names, or NULL */
    bool qualified;     /* the row reads references against full names */
    bool any_scope;     /* the file's package is unknown: every scope may be around the site */
    bool maybe;         /* a message of the referring file may hold the first component */
    char buf[SC_FULL_MAX];
} sc_scope_t;

static void sc_scope_walk(const cbm_closure_t *s, int file, const cbm_closure_ref_t *ref,
                          sc_scope_t *w) {
    const sc_file_t *f = &s->files[file];
    const char *r = ref->written ? ref->written : ref->name;
    w->target = NULL;
    w->qualified = f->row->qualified_refs;
    w->any_scope = false;
    w->maybe = false;
    if (!w->qualified) {
        return;
    }
    if (r[0] == '.') {
        w->target = r + SKIP_ONE;
        return;
    }
    if (!f->package) {
        w->any_scope = true;
        return;
    }
    size_t xl = strcspn(r, ".");
    char key[SC_FULL_MAX];
    int n = snprintf(key, sizeof(key), "%d\x1f%.*s", file, (int)xl, r);
    w->maybe = n > 0 && (size_t)n < sizeof(key) && cbm_ht_get(s->nested, key);
    const char *k = f->package;
    for (size_t l = strlen(k);;) {
        n = l ? snprintf(key, sizeof(key), "%.*s.%.*s", (int)l, k, (int)xl, r)
              : snprintf(key, sizeof(key), "%.*s", (int)xl, r);
        if (n > 0 && (size_t)n < sizeof(key) &&
            (cbm_ht_get(s->fulls, key) || cbm_ht_get(s->pkgs, key))) {
            int m = l ? snprintf(w->buf, sizeof(w->buf), "%.*s.%s", (int)l, k, r)
                      : snprintf(w->buf, sizeof(w->buf), "%s", r);
            w->target = m > 0 && (size_t)m < sizeof(w->buf) ? w->buf : NULL;
            return;
        }
        if (l == 0) {
            return; /* the first component names nothing: protoc fails, no target */
        }
        while (l > 0 && k[l - SKIP_ONE] != '.') {
            l--;
        }
        l = l > 0 ? l - SKIP_ONE : 0;
    }
}

enum { SC_NO = 0, SC_YES = 1, SC_MAYBE = 2 };

/* Whether a definition is what the reference names: by name (a row read by
 * name, or a definition whose full name is unknown); for a row with qualified
 * references the walk's target, or maybe: a definition of the referring file
 * nested in a message (the site may be inside it), or any scope when the
 * file's package is unknown. */
static int sc_def_matches(const sc_def_t *d, int file, const cbm_closure_ref_t *ref,
                          const sc_scope_t *w) {
    if (strcmp(d->name, ref->name) != 0) {
        return SC_NO;
    }
    if (!w->qualified || !d->full) {
        return SC_YES;
    }
    if (w->target && strcmp(d->full, w->target) == 0) {
        return SC_YES;
    }
    if (!w->any_scope && !(w->maybe && d->file == file)) {
        return SC_NO;
    }
    const char *r = ref->written ? ref->written : ref->name;
    size_t fl = strlen(d->full);
    size_t rl = strlen(r);
    bool tail = fl == rl ? strcmp(d->full, r) == 0
                         : (fl > rl && d->full[fl - rl - SKIP_ONE] == '.' &&
                            strcmp(d->full + fl - rl, r) == 0);
    return tail ? SC_MAYBE : SC_NO;
}

/* The visible candidates so far: the first, and whether another differs. */
typedef struct {
    int first;
    bool several;
    bool maybe; /* the first one only maybe (the enclosing message may decide) */
    uint8_t via;
} sc_acc_t;

static void sc_take(const cbm_closure_t *s, cbm_closure_memo_t *m, const sc_ivec_t *list,
                    const cbm_closure_ref_t *ref, const sc_scope_t *w, int file, uint8_t via,
                    sc_acc_t *acc) {
    for (int i = 0; list && i < list->n && !acc->several; i++) {
        m->work++;
        int d = list->v[i];
        int match = d == acc->first ? SC_NO : sc_def_matches(&s->defs[d], file, ref, w);
        if (match == SC_NO) {
            continue;
        }
        if (acc->first < 0) {
            acc->first = d;
            acc->maybe = match == SC_MAYBE;
            acc->via = s->defs[d].file == file ? (uint8_t)CBM_CLOSURE_VIA_OWN : via;
        } else {
            acc->several = true;
        }
    }
}

/* The definitions a file sees of the name, and how the closure stands. */
static void sc_visible(const cbm_closure_t *s, cbm_closure_memo_t *m, int file,
                       const cbm_closure_ref_t *ref, const sc_scope_t *w, sc_acc_t *acc,
                       uint32_t *why) {
    const sc_file_t *f = &s->files[file];
    sc_memo_file(s, m, file);
    *why |= m->file_why;
    const sc_ivec_t *named = cbm_ht_get(s->by_name, ref->name);
    /* the candidates: the walk's target by its full name, else every
     * definition of the name */
    const sc_ivec_t *cands = named;
    if (w->qualified && w->target && !w->any_scope && !w->maybe) {
        cands = cbm_ht_get(s->fulls, w->target);
    }
    for (int i = 0; cands && i < cands->n && !acc->several; i++) {
        m->work++;
        int d = cands->v[i];
        int match = d == acc->first ? SC_NO : sc_def_matches(&s->defs[d], file, ref, w);
        if (match == SC_NO) {
            continue;
        }
        int seen = sc_sees(s, m, file, ref->name, d);
        if (seen < 0) {
            *why |= CBM_CLOSURE_WHY_OPAQUE; /* out of memory: nothing is proven */
            continue;
        }
        if (!seen) {
            continue;
        }
        if (acc->first < 0) {
            acc->first = d;
            acc->maybe = match == SC_MAYBE;
            acc->via = s->defs[d].file == file ? (uint8_t)CBM_CLOSURE_VIA_OWN
                                               : (uint8_t)CBM_CLOSURE_VIA_INCLUDE;
        } else {
            acc->several = true;
        }
    }
    /* package mates (rows with package_visible): their own definitions */
    if (f->row->package_visible && f->package && f->package[0]) {
        for (int k = 0; named && k < named->n && !acc->several; k++) {
            m->work++;
            const sc_def_t *d = &s->defs[named->v[k]];
            const sc_file_t *g = &s->files[d->file];
            if (d->file != file && g->row == f->row && g->package &&
                strcmp(g->package, f->package) == 0) {
                sc_ivec_t one = {.v = &named->v[k], .n = SKIP_ONE, .cap = SKIP_ONE};
                sc_take(s, m, &one, ref, w, file, CBM_CLOSURE_VIA_PACKAGE, acc);
            }
        }
        const sc_ivec_t *mates = cbm_ht_get(s->packages, f->package);
        for (int i = 0; mates && i < mates->n; i++) {
            m->work++;
            if (s->files[mates->v[i]].opaque) {
                *why |= CBM_CLOSURE_WHY_OPAQUE;
            }
        }
    }
}

static cbm_closure_answer_t sc_lookup(const cbm_closure_t *s, cbm_closure_memo_t *m, int file,
                                      const cbm_closure_ref_t *ref) {
    cbm_closure_answer_t a = {.verdict = CBM_CLOSURE_NO_ANSWER, .file = -1};
    if (ref->flags & CBM_CLOSURE_REF_LOCAL) {
        a.why = CBM_CLOSURE_WHY_LOCAL;
        return a;
    }
    sc_acc_t acc = {.first = -1, .several = false, .maybe = false, .via = 0};
    uint32_t why = 0;
    sc_scope_t walk;
    sc_scope_walk(s, file, ref, &walk);
    sc_visible(s, m, file, ref, &walk, &acc, &why);
    if (acc.maybe) {
        why |= CBM_CLOSURE_WHY_SCOPE;
    }
    a.visible = acc.several ? PAIR_LEN : (acc.first >= 0 ? SKIP_ONE : 0);
    a.complete =
        (why & (CBM_CLOSURE_WHY_UNRESOLVED | CBM_CLOSURE_WHY_OPAQUE | CBM_CLOSURE_WHY_PRELUDE)) ==
        0;
    if (acc.first < 0) {
        a.verdict = a.complete ? CBM_CLOSURE_PROVEN_ABSENT : CBM_CLOSURE_NO_ANSWER;
        a.why = why;
        return a;
    }
    if (acc.several) {
        a.why = why | CBM_CLOSURE_WHY_AMBIGUOUS;
        return a;
    }
    const sc_def_t *d = &s->defs[acc.first];
    if (!d->qn) {
        a.why = why | CBM_CLOSURE_WHY_NO_NODE;
        return a;
    }
    if (d->conditional) {
        why |= CBM_CLOSURE_WHY_CONDITIONAL;
    }
    a.qn = d->qn;
    a.full = d->full;
    a.file = d->file;
    a.via = acc.via;
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

cbm_closure_answer_t cbm_closure_lookup(const cbm_closure_t *s, cbm_closure_memo_t *memo, int file,
                                        const cbm_closure_ref_t *ref) {
    cbm_closure_answer_t none = {.verdict = CBM_CLOSURE_NO_ANSWER, .file = -1};
    if (!s || !s->sealed || file < 0 || file >= s->nfiles || !ref || !ref->name || !ref->name[0]) {
        return none;
    }
    cbm_closure_memo_t *own = memo ? NULL : cbm_closure_memo_new();
    cbm_closure_memo_t *m = memo ? memo : own;
    if (!m || !sc_memo_bind(m, s)) {
        cbm_closure_memo_free(own);
        return none;
    }
    cbm_closure_answer_t a = sc_lookup(s, m, file, ref);
    cbm_closure_memo_free(own);
    return a;
}
