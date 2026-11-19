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
    bool sealed;
    uint64_t work;
};

/* One thread's memo: per name, the definitions each component exports. */
struct cbm_closure_memo {
    const cbm_closure_t *owner; /* the closure the memo was filled from */
    CBMArena arena;
    CBMHashTable *reach; /* name -> CBMHashTable* (component as key text -> sc_ivec_t* of defs) */
    uint32_t *stamp;     /* per component: the search that reached it last */
    uint32_t epoch;      /* the current search */
    int *queue;          /* per component: the backward search's queue */
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
    if (!s->by_name || !s->packages || !s->by_rel) {
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
    m->stamp = NULL;
    m->queue = NULL;
    m->ncomps = 0;
    m->owner = NULL;
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
    if (!m->reach || !m->stamp || !m->queue) {
        sc_memo_clear(m);
        return false;
    }
    memset(m->stamp, 0, n * sizeof(*m->stamp));
    m->epoch = 0;
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

/* The reach of a name: for each component that exports a definition named
 * `name`, the definitions it exports. Built once per memo, by a backward
 * search from each defining file's component along re-export edges (each
 * component once per definition). */
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
        int head = 0;
        int tail = 0;
        int start = s->files[s->defs[d].file].comp;
        uint32_t mark = sc_memo_epoch(m);
        m->queue[tail++] = start;
        m->stamp[start] = mark;
        while (ok && head < tail) {
            int c = m->queue[head++];
            m->work++;
            char ck[CBM_SZ_16];
            snprintf(ck, sizeof(ck), "%d", c);
            sc_ivec_t *defs = cbm_ht_get(reach, ck);
            if (!defs) {
                defs = cbm_arena_alloc(&m->arena, sizeof(*defs));
                const char *ckey = cbm_arena_strdup(&m->arena, ck);
                ok = defs && ckey;
                if (ok) {
                    memset(defs, 0, sizeof(*defs));
                    cbm_ht_set(reach, ckey, defs);
                }
            }
            ok = ok && sc_push(&m->arena, defs, d);
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

/* What a component exports of the name: its list, else nothing. */
static const sc_ivec_t *sc_exports(CBMHashTable *reach, int comp) {
    char ck[CBM_SZ_16];
    snprintf(ck, sizeof(ck), "%d", comp);
    return reach ? cbm_ht_get(reach, ck) : NULL;
}

/* ── Lookup ──────────────────────────────────────────────────────────── */

/* Whether a definition may be what the reference names. By name always; for
 * a row with qualified references also by full name (scope_closure.h,
 * cbm_closure_ref_t): never false for a definition the reference could name
 * from some site of the file, so one candidate left is the one. */
static bool sc_def_matches(const sc_def_t *d, const cbm_closure_ref_t *ref, const char *package,
                           bool qualified) {
    if (strcmp(d->name, ref->name) != 0) {
        return false;
    }
    if (!qualified || !d->full) {
        return true;
    }
    const char *r = ref->written ? ref->written : ref->name;
    if (r[0] == '.') {
        return strcmp(d->full, r + SKIP_ONE) == 0;
    }
    size_t fl = strlen(d->full);
    size_t rl = strlen(r);
    if (fl == rl) {
        return strcmp(d->full, r) == 0; /* the root scope */
    }
    if (fl < rl + SKIP_ONE || d->full[fl - rl - SKIP_ONE] != '.' ||
        strcmp(d->full + fl - rl, r) != 0) {
        return false;
    }
    size_t pl = fl - rl - SKIP_ONE; /* full = P "." r: is P a scope around the site? */
    if (!package || !package[0]) {
        return true; /* no package (or unknown): any message scope may be */
    }
    size_t kl = strlen(package);
    if (pl <= kl && strncmp(package, d->full, pl) == 0 &&
        (package[pl] == '\0' || package[pl] == '.')) {
        return true; /* the package, or a package around it */
    }
    return pl > kl && strncmp(d->full, package, kl) == 0 && d->full[kl] == '.'; /* inside it */
}

/* The visible candidates so far: the first, and whether another differs. */
typedef struct {
    int first;
    bool several;
    uint8_t via;
} sc_acc_t;

static void sc_take(const cbm_closure_t *s, cbm_closure_memo_t *m, const sc_ivec_t *list,
                    const cbm_closure_ref_t *ref, const sc_file_t *f, int file, uint8_t via,
                    sc_acc_t *acc) {
    for (int i = 0; list && i < list->n && !acc->several; i++) {
        m->work++;
        int d = list->v[i];
        if (d == acc->first ||
            !sc_def_matches(&s->defs[d], ref, f->package, f->row->qualified_refs)) {
            continue;
        }
        if (acc->first < 0) {
            acc->first = d;
            acc->via = s->defs[d].file == file ? (uint8_t)CBM_CLOSURE_VIA_OWN : via;
        } else {
            acc->several = true;
        }
    }
}

/* The definitions a file sees of the name, and how the closure stands. */
static void sc_visible(const cbm_closure_t *s, cbm_closure_memo_t *m, int file,
                       const cbm_closure_ref_t *ref, sc_acc_t *acc, uint32_t *why) {
    const sc_file_t *f = &s->files[file];
    const sc_ivec_t *named = cbm_ht_get(s->by_name, ref->name);
    CBMHashTable *reach = named ? sc_reach(s, m, ref->name, named) : NULL;
    if (named && !reach) {
        *why |= CBM_CLOSURE_WHY_OPAQUE; /* out of memory: nothing is proven */
    }
    if (f->opaque) {
        *why |= CBM_CLOSURE_WHY_OPAQUE;
    }
    if (f->row->prelude_external) {
        *why |= CBM_CLOSURE_WHY_PRELUDE;
    }
    /* the file's own component: its definitions, and those of the files it
     * shares a re-export cycle with */
    sc_take(s, m, sc_exports(reach, f->comp), ref, f, file, CBM_CLOSURE_VIA_INCLUDE, acc);
    /* package mates (rows with package_visible): their own definitions */
    if (f->row->package_visible && f->package && f->package[0]) {
        for (int k = 0; named && k < named->n && !acc->several; k++) {
            m->work++;
            const sc_def_t *d = &s->defs[named->v[k]];
            const sc_file_t *g = &s->files[d->file];
            if (d->file != file && g->row == f->row && g->package &&
                strcmp(g->package, f->package) == 0) {
                sc_ivec_t one = {.v = &named->v[k], .n = SKIP_ONE, .cap = SKIP_ONE};
                sc_take(s, m, &one, ref, f, file, CBM_CLOSURE_VIA_PACKAGE, acc);
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
    /* what each include exports */
    for (int e = 0; e < f->includes.n; e++) {
        m->work++;
        int t = f->includes.v[e];
        if (t < 0) {
            *why |= CBM_CLOSURE_WHY_UNRESOLVED;
            continue;
        }
        const sc_comp_t *comp = &s->comps[s->files[t].comp];
        if (!comp->export_complete) {
            *why |= comp->reexport_unresolved ? CBM_CLOSURE_WHY_UNRESOLVED : CBM_CLOSURE_WHY_OPAQUE;
        }
        sc_take(s, m, sc_exports(reach, s->files[t].comp), ref, f, file, CBM_CLOSURE_VIA_INCLUDE,
                acc);
    }
}

static cbm_closure_answer_t sc_lookup(const cbm_closure_t *s, cbm_closure_memo_t *m, int file,
                                      const cbm_closure_ref_t *ref) {
    cbm_closure_answer_t a = {.verdict = CBM_CLOSURE_NO_ANSWER, .file = -1};
    if (ref->flags & CBM_CLOSURE_REF_LOCAL) {
        a.why = CBM_CLOSURE_WHY_LOCAL;
        return a;
    }
    sc_acc_t acc = {.first = -1, .several = false, .via = 0};
    uint32_t why = 0;
    sc_visible(s, m, file, ref, &acc, &why);
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
