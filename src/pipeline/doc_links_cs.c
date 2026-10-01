/*
 * doc_links_cs.c — C# cref resolution for doc_links.h.
 *
 * The project-wide index is built from every C# file's doc-link scope
 * (internal/cbm/doclink_cs.c): namespaces, usings, type and member
 * declarations. Graph nodes are only looked up by qualified name, never by
 * line, so the closure-repair route (whose unchanged nodes are line-less
 * proxies) resolves exactly as a full build does.
 *
 * Entities. A type entity is (FQN, generic arity): the namespace of the
 * declaring region plus the dotted local path, e.g. Acme.Core.Widget`1.
 * Partial types contribute several declarations. A declaration owns the node
 * at <module>.<path> when it is the last declaration of that path in its
 * file (the extractor keeps one node per qualified name and the last
 * declaration wins, so `Foo` / `Foo<T>` in one file leave one of them without
 * a node: a graph gap, never a fallback to the other arity). The same holds
 * for their members: `Foo.X` and `Foo<T>.X` share one node, and it belongs to
 * the later declaration.
 *
 * Lookup follows the field-tested prototype (private/field-tests/tools/h1,
 * v2 rules + the five recommended rules; H8 R1-R4):
 *   - scope order: documented/enclosing types (own, then inherited) ->
 *     namespace chain innermost first -> using alias -> usings (file and
 *     namespace-block usings in scope, `global using` and MSBuild usings of
 *     the project) and `using static` members; the first level with a visible
 *     candidate decides, more than one entity there is ambiguous
 *   - a single segment never takes a fully-qualified shortcut; qualified
 *     prefixes are tried namespace-relative (innermost first) before absolute
 *   - member kind: a parameter list or a type-argument list selects
 *     callables only; a property and a method of one name are ambiguous; an
 *     overload group without a signature is ambiguous
 *   - constructors are named by a parameter list on the type's name
 *     (`Foo(int)`) or by qualification (`Foo.Foo`); a bare `Foo` is the type,
 *     and no constructor is inherited
 *   - arity (R2/R3): a segment without type arguments names the arity-0 type
 *     (declared with or without a node) wherever one is in scope; only when
 *     no scope level has one does a generic type of the name bind. Type
 *     arguments select the types and generic methods of that arity only
 *   - explicit interface implementations are not addressable by simple name
 *   - visibility: product code never binds a test-only declaration
 *     (test_only_target, no fallback); a test program does not bind another
 *     program's global-namespace test type
 *   - external (R4): keyword aliases and System.* names missing from the
 *     corpus, open scopes (usings of namespaces outside the corpus) and open
 *     hierarchies (a base outside the corpus) are external, not missing
 *   - namespaces: a reference to one of the repository's namespaces is a
 *     graph gap (declared, no node); `Namespace.Type` for a type that
 *     namespace does not declare is missing (or external by R4), never a gap
 *   - MSBuild global usings (R1): Directory.Build.props -> project ->
 *     Directory.Build.targets, <Using Include/Remove>, ImplicitUsings SDK sets
 *   - what the parser could not place is never resolved around: a type whose
 *     members a parse error hides answers a member it does not show with
 *     graph_gap (not with an inherited or outer one); inside such a type a
 *     simple name found nowhere is graph_gap where it would otherwise be
 *     missing; and a name some file declares without an establishable
 *     namespace is graph_gap wherever it is referenced
 */
#include "pipeline/doc_links.h"

#include "doclink.h"
#include "helpers.h" /* cbm_fqn_module_source_lang */
#include "foundation/arena.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    CS_MAX_CANDS = 32,
    CS_MAX_SEGS = 16,
    CS_MAX_PARAMS = 24,
    CS_MAX_CHAIN = 16,
    CS_MAX_BFS = 64,
    CS_BFS_DEPTH = 25,
    CS_KEY_BUF = 2048,
    CS_PARAM_BUF = 160,
    CS_REF_BUF = 1024,
    CS_ARITY_NONE = -1,
};

/* ── Index data ──────────────────────────────────────────────────── */

typedef struct {
    int parent;
    uint32_t start;
    uint32_t end;
    const char *ns;
} cs_region_t;

typedef struct {
    int region;
    char kind; /* n namespace, s static, a alias, g global */
    const char *alias;
    const char *target;
} cs_using_t;

typedef struct {
    int region;
    uint32_t start;
    uint32_t end;
    char kind;
    const char *path;
    const char *name;
    const char *tparams;
    const char *bases;
    int arity;
    int entity;
    bool owns_node;  /* last declaration of its path in the file */
    bool incomplete; /* a parse error hides some of its members */
} cs_type_t;

/* Line numbers (start, end) are meaningful only in a file re-extracted by
 * this run: the persisted scope of every other file carries zeros (so an edit
 * that only moves lines keeps the file's surface). Lines are therefore read
 * only for the SOURCE file's own context, never for a target; target-side
 * ties are broken by declaration order. */
typedef struct {
    uint32_t start;
    int order;    /* declaration order in the file */
    int type_idx; /* the declaration it belongs to (index into the file's types) */
    int arity;    /* generic method arity */
    char kind;    /* c callable, v value, e event */
    bool explicit_impl;
    const char *owner;
    const char *name;
    const char *tparams;
    const char *sig; /* NULL for a non-callable */
} cs_member_t;

typedef struct {
    uint32_t from;
    uint32_t to;
} cs_span_lines_t;

typedef struct {
    const char *rel_path;
    const char *module_qn;
    bool is_test;
    cs_span_lines_t *unplaced; /* line ranges whose declarations could not be placed */
    int nunplaced;
    int unit;
    cs_region_t *regions;
    int nregions;
    cs_using_t *usings;
    int nusings;
    cs_type_t *types;
    int ntypes;
    int *types_by_path; /* indices sorted by (path, blob order) */
    cs_member_t *members;
    int nmembers;
    int *members_by_start;
} cs_file_t;

typedef struct {
    int file;
    int type;
} cs_decl_t;

typedef struct {
    const char *fqn;
    const char *name;
    const char *ns;
    int arity;
    char kind;
    cs_decl_t *decls;
    int ndecls;
    int dcap;
    int *bases;
    int nbases;
    bool open;
    bool incomplete; /* a declaration of it has members a parse error hides */
    bool any_prod;
    bool all_test;
} cs_entity_t;

typedef struct {
    int *items;
    int count;
    int cap;
} cs_ilist_t;

typedef struct {
    const char *dir;
    const char **usings;
    int nusings;
    int cap;
} cs_unit_t;

typedef struct {
    CBMArena arena;
    bool oom; /* an index allocation failed */
    const char *project;
    const char *repo_path;
    cs_file_t *files;
    int nfiles;
    int *run_to_file;
    int run_count;
    cs_entity_t *ents;
    int nents;
    int ecap;
    CBMHashTable *ent_by_key;      /* "fqn`arity" -> ent+1 */
    CBMHashTable *fqn_ents;        /* fqn -> cs_ilist_t* */
    CBMHashTable *ns_types;        /* "ns\x1f name" -> cs_ilist_t* (top-level types) */
    CBMHashTable *namespaces;      /* every namespace and prefix */
    CBMHashTable *type_names;      /* every type simple name */
    CBMHashTable *quarantine;      /* names of types declared where no scope is known */
    CBMHashTable *quarantine_test; /* the same, declared by test code only */
    CBMHashTable *unit_by_dir;     /* dir -> unit+1 */
    cs_unit_t *units;
    int nunits;
    int ucap;
} cs_index_t;

/* ── Small helpers ───────────────────────────────────────────────── */

/* Index memory. A failed allocation is remembered: whatever a caller does
 * with its NULL, the build as a whole reports failure instead of handing out
 * an index that silently lacks a declaration. */
static void *ix_alloc(cs_index_t *ix, size_t n) {
    void *p = cbm_arena_alloc(&ix->arena, n ? n : SKIP_ONE);
    ix->oom = ix->oom || !p;
    return p;
}

static char *ix_strndup(cs_index_t *ix, const char *s, size_t n) {
    char *p = cbm_arena_strndup(&ix->arena, s, n);
    ix->oom = ix->oom || !p;
    return p;
}

static char *ix_strdup(cs_index_t *ix, const char *s) {
    char *p = cbm_arena_strdup(&ix->arena, s ? s : "");
    ix->oom = ix->oom || !p;
    return p;
}

static bool ilist_push(cs_index_t *ix, cs_ilist_t *l, int v) {
    for (int i = 0; i < l->count; i++) {
        if (l->items[i] == v) {
            return true;
        }
    }
    if (l->count >= l->cap) {
        int ncap = l->cap ? l->cap * PAIR_LEN : CBM_SZ_4;
        int *grown = (int *)ix_alloc(ix, (size_t)ncap * sizeof(int));
        if (!grown) {
            return false;
        }
        if (l->count > 0) {
            memcpy(grown, l->items, (size_t)l->count * sizeof(int));
        }
        l->items = grown;
        l->cap = ncap;
    }
    l->items[l->count++] = v;
    return true;
}

static cs_ilist_t *ht_ilist(cs_index_t *ix, CBMHashTable *ht, const char *key, bool create) {
    cs_ilist_t *l = (cs_ilist_t *)cbm_ht_get(ht, key);
    if (l || !create) {
        return l;
    }
    l = (cs_ilist_t *)ix_alloc(ix, sizeof(*l));
    char *k = ix_strdup(ix, key);
    if (!l || !k) {
        return NULL;
    }
    memset(l, 0, sizeof(*l));
    cbm_ht_set(ht, k, l);
    return l;
}

/* Add `key` to a name set; false when memory ran out. */
static bool ht_mark(cs_index_t *ix, CBMHashTable *ht, const char *key) {
    if (cbm_ht_get(ht, key)) {
        return true;
    }
    char *k = ix_strdup(ix, key);
    if (!k) {
        return false;
    }
    cbm_ht_set(ht, k, (void *)k);
    return true;
}

/* A type name declared in `f` at a place no namespace or outer type could be
 * established for. Product code never binds test-only declarations, so a name
 * only test files quarantine blocks references from test files only. */
static bool quarantine_name(cs_index_t *ix, const cs_file_t *f, const char *name) {
    return ht_mark(ix, f->is_test ? ix->quarantine_test : ix->quarantine, name);
}

static bool cs_is_test_path(const char *rel) {
    /* C# test code: a directory named tests/test, or a *.Tests / *.UnitTests /
     * *.FunctionalTests project directory (the prototype's rule). */
    const char *p = rel;
    for (;;) {
        const char *slash = strchr(p, '/');
        if (!slash) {
            return false;
        }
        size_t n = (size_t)(slash - p);
        char seg[CBM_SZ_256];
        if (n < sizeof(seg)) {
            for (size_t i = 0; i < n; i++) {
                seg[i] = (char)tolower((unsigned char)p[i]);
            }
            seg[n] = '\0';
            if (strcmp(seg, "tests") == 0 || strcmp(seg, "test") == 0) {
                return true;
            }
            static const char *const sfx[] = {".tests", ".unittests", ".functionaltests"};
            for (size_t k = 0; k < sizeof(sfx) / sizeof(sfx[0]); k++) {
                size_t sl = strlen(sfx[k]);
                if (n >= sl && strcmp(seg + n - sl, sfx[k]) == 0) {
                    return true;
                }
            }
        }
        p = slash + SKIP_ONE;
    }
}

/* Split `s` in place at `sep` into at most `max` fields; returns the count. */
static int split_fields(char *s, char sep, char **out, int max) {
    int n = 0;
    out[n++] = s;
    for (char *p = s; *p && n < max; p++) {
        if (*p == sep) {
            *p = '\0';
            out[n++] = p + SKIP_ONE;
        }
    }
    return n;
}

static int count_list(const char *s, char sep) {
    if (!s || !s[0]) {
        return 0;
    }
    int n = 1;
    for (const char *p = s; *p; p++) {
        n += *p == sep;
    }
    return n;
}

/* ── Scope blob parsing ──────────────────────────────────────────── */

/* Sort keys carry their own comparison data: no global sort context. */
typedef struct {
    const char *path;
    uint32_t start;
    int idx;
} cs_sort_key_t;

static int sort_key_path_cmp(const void *a, const void *b) {
    const cs_sort_key_t *x = (const cs_sort_key_t *)a;
    const cs_sort_key_t *y = (const cs_sort_key_t *)b;
    int c = strcmp(x->path, y->path);
    return c ? c : (x->idx < y->idx ? -1 : (x->idx > y->idx));
}

static int sort_key_start_cmp(const void *a, const void *b) {
    const cs_sort_key_t *x = (const cs_sort_key_t *)a;
    const cs_sort_key_t *y = (const cs_sort_key_t *)b;
    if (x->start != y->start) {
        return x->start < y->start ? -1 : 1;
    }
    return x->idx < y->idx ? -1 : (x->idx > y->idx);
}

static int member_cmp(const void *a, const void *b) {
    const cs_member_t *x = (const cs_member_t *)a;
    const cs_member_t *y = (const cs_member_t *)b;
    int c = strcmp(x->owner, y->owner);
    if (c) {
        return c;
    }
    c = strcmp(x->name, y->name);
    if (c) {
        return c;
    }
    return x->order < y->order ? -1 : (x->order > y->order);
}

static bool parse_scope(cs_index_t *ix, cs_file_t *f, const char *blob) {
    size_t len = strlen(blob);
    char *buf = ix_strndup(ix, blob, len);
    if (!buf) {
        return false;
    }
    int nr = 1;
    int nu = 0;
    int nt = 0;
    int nm = 0;
    int nx = 0;
    int max_region = 0;
    for (const char *p = buf; *p;) {
        const char *nl = strchr(p, '\n');
        char tag = *p;
        if (tag == 'R') {
            nr++;
            int id = atoi(p + PAIR_LEN);
            max_region = id > max_region ? id : max_region;
        } else if (tag == 'U') {
            nu++;
        } else if (tag == 'T') {
            nt++;
        } else if (tag == 'M') {
            nm++;
        } else if (tag == 'X') {
            nx++;
        }
        if (!nl) {
            break;
        }
        p = nl + SKIP_ONE;
    }
    f->nregions = (max_region + SKIP_ONE) > nr ? max_region + SKIP_ONE : nr;
    f->regions = (cs_region_t *)ix_alloc(ix, (size_t)f->nregions * sizeof(cs_region_t));
    f->usings = (cs_using_t *)ix_alloc(ix, (size_t)nu * sizeof(cs_using_t));
    f->types = (cs_type_t *)ix_alloc(ix, (size_t)nt * sizeof(cs_type_t));
    f->members = (cs_member_t *)ix_alloc(ix, (size_t)nm * sizeof(cs_member_t));
    f->unplaced = (cs_span_lines_t *)ix_alloc(ix, (size_t)nx * sizeof(cs_span_lines_t));
    if (!f->regions || !f->usings || !f->types || !f->members || !f->unplaced) {
        return false;
    }
    for (int i = 0; i < f->nregions; i++) {
        f->regions[i] =
            (cs_region_t){.parent = CBM_NOT_FOUND, .start = 0, .end = UINT32_MAX, .ns = ""};
    }
    char *line = buf;
    bool first = true;
    while (line && *line) {
        char *nl = strchr(line, '\n');
        if (nl) {
            *nl = '\0';
        }
        char *fld[CBM_SZ_8];
        if (first) {
            first = false;
            if (strcmp(line, CBM_DOCLINK_CS_SCOPE_TAG) != 0) {
                return false;
            }
        } else if (line[0] == 'R') {
            int n = split_fields(line, '\t', fld, CBM_SZ_6);
            if (n == CBM_SZ_6) {
                int id = atoi(fld[1]);
                if (id > 0 && id < f->nregions) {
                    f->regions[id] = (cs_region_t){.parent = atoi(fld[2]),
                                                   .start = (uint32_t)strtoul(fld[3], NULL, 10),
                                                   .end = (uint32_t)strtoul(fld[4], NULL, 10),
                                                   .ns = fld[5]};
                }
            }
        } else if (line[0] == 'U') {
            int n = split_fields(line, '\t', fld, CBM_SZ_5);
            if (n == CBM_SZ_5) {
                f->usings[f->nusings++] =
                    (cs_using_t){.region = atoi(fld[1]),
                                 .kind = fld[2][0],
                                 .alias = strcmp(fld[3], "-") == 0 ? "" : fld[3],
                                 .target = fld[4]};
            }
        } else if (line[0] == 'T') {
            int n = split_fields(line, '\t', fld, CBM_SZ_8);
            if (n == CBM_SZ_8) {
                cs_type_t *t = &f->types[f->ntypes++];
                memset(t, 0, sizeof(*t));
                t->region = atoi(fld[1]);
                t->start = (uint32_t)strtoul(fld[2], NULL, 10);
                t->end = (uint32_t)strtoul(fld[3], NULL, 10);
                t->kind = fld[4][0];
                t->incomplete = fld[4][0] && fld[4][1] == '!';
                t->path = fld[5];
                const char *dot = strrchr(t->path, '.');
                t->name = dot ? dot + SKIP_ONE : t->path;
                t->tparams = fld[6];
                t->bases = fld[7];
                t->arity = count_list(t->tparams, ',');
                t->entity = CBM_NOT_FOUND;
            }
        } else if (line[0] == 'M') {
            int n = split_fields(line, '\t', fld, CBM_SZ_7);
            if (n == CBM_SZ_7) {
                cs_member_t *m = &f->members[f->nmembers];
                memset(m, 0, sizeof(*m));
                m->start = (uint32_t)strtoul(fld[1], NULL, 10);
                m->order = f->nmembers;
                m->kind = fld[2][0];
                m->explicit_impl = fld[3][0] == '1';
                char *path = fld[4];
                char *dot = strrchr(path, '.');
                /* records are in document order: a member belongs to the
                 * nearest preceding declaration of its type path (which of
                 * two same-path declarations -- Foo and Foo<T> -- matters) */
                int ti = f->ntypes - SKIP_ONE;
                if (dot) {
                    *dot = '\0';
                    while (ti >= 0 && strcmp(f->types[ti].path, path) != 0) {
                        ti--;
                    }
                }
                if (dot && ti >= 0) {
                    m->owner = path;
                    m->name = dot + SKIP_ONE;
                    m->type_idx = ti;
                    m->tparams = fld[5];
                    m->arity = count_list(m->tparams, ',');
                    m->sig = m->kind == 'c' ? fld[6] : NULL;
                    f->nmembers++;
                }
            }
        } else if (line[0] == 'X') {
            int n = split_fields(line, '\t', fld, CBM_SZ_4);
            if (n == 3 && f->nunplaced < nx) {
                f->unplaced[f->nunplaced++] =
                    (cs_span_lines_t){.from = (uint32_t)strtoul(fld[1], NULL, 10),
                                      .to = (uint32_t)strtoul(fld[2], NULL, 10)};
            }
        } else if (line[0] == 'Q') {
            int n = split_fields(line, '\t', fld, PAIR_LEN);
            if (n == PAIR_LEN && fld[1][0] && !quarantine_name(ix, f, fld[1])) {
                return false;
            }
        }
        line = nl ? nl + SKIP_ONE : NULL;
    }
    if (first) {
        return false;
    }
    /* Paths are unique per type except for same-file collisions; the last
     * declaration of a path owns its node. */
    f->types_by_path = (int *)ix_alloc(ix, (size_t)f->ntypes * sizeof(int));
    f->members_by_start = (int *)ix_alloc(ix, (size_t)f->nmembers * sizeof(int));
    if ((f->ntypes && !f->types_by_path) || (f->nmembers && !f->members_by_start)) {
        return false;
    }
    int nkeys = f->ntypes > f->nmembers ? f->ntypes : f->nmembers;
    cs_sort_key_t *keys =
        nkeys > 0
            ? (cs_sort_key_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)nkeys * sizeof(cs_sort_key_t))
            : NULL;
    if (nkeys > 0 && !keys) {
        return false;
    }
    for (int i = 0; i < f->ntypes; i++) {
        keys[i] = (cs_sort_key_t){.path = f->types[i].path, .start = 0, .idx = i};
    }
    if (f->ntypes > 0) {
        qsort(keys, (size_t)f->ntypes, sizeof(*keys), sort_key_path_cmp);
    }
    for (int i = 0; i < f->ntypes; i++) {
        f->types_by_path[i] = keys[i].idx;
        bool last = i + SKIP_ONE >= f->ntypes || strcmp(keys[i + SKIP_ONE].path, keys[i].path) != 0;
        f->types[keys[i].idx].owns_node = last;
    }
    if (f->nmembers > 0) {
        qsort(f->members, (size_t)f->nmembers, sizeof(cs_member_t), member_cmp);
    }
    for (int i = 0; i < f->nmembers; i++) {
        keys[i] = (cs_sort_key_t){.path = "", .start = f->members[i].start, .idx = i};
    }
    if (f->nmembers > 0) {
        qsort(keys, (size_t)f->nmembers, sizeof(*keys), sort_key_start_cmp);
    }
    for (int i = 0; i < f->nmembers; i++) {
        f->members_by_start[i] = keys[i].idx;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, keys);
    return true;
}

/* ── Units (C# projects) and their global usings ─────────────────── */

static bool dir_has_csproj(const char *repo, const char *dir) {
    char full[CS_KEY_BUF];
    if (snprintf(full, sizeof(full), "%s%s%s", repo, dir[0] ? "/" : "", dir) >= (int)sizeof(full)) {
        return false;
    }
    cbm_dir_t *d = cbm_opendir(full);
    if (!d) {
        return false;
    }
    bool found = false;
    cbm_dirent_t *e;
    while (!found && (e = cbm_readdir(d)) != NULL) {
        size_t n = strlen(e->name);
        found = !e->is_dir && n > strlen(".csproj") &&
                strcmp(e->name + n - strlen(".csproj"), ".csproj") == 0;
    }
    cbm_closedir(d);
    return found;
}

static int unit_get(cs_index_t *ix, const char *dir) {
    intptr_t v = (intptr_t)cbm_ht_get(ix->unit_by_dir, dir);
    if (v > 0) {
        return (int)(v - SKIP_ONE);
    }
    if (ix->nunits >= ix->ucap) {
        int ncap = ix->ucap ? ix->ucap * PAIR_LEN : CBM_SZ_64;
        cs_unit_t *grown = (cs_unit_t *)ix_alloc(ix, (size_t)ncap * sizeof(cs_unit_t));
        if (!grown) {
            return CBM_NOT_FOUND;
        }
        if (ix->nunits > 0) {
            memcpy(grown, ix->units, (size_t)ix->nunits * sizeof(cs_unit_t));
        }
        ix->units = grown;
        ix->ucap = ncap;
    }
    int id = ix->nunits++;
    memset(&ix->units[id], 0, sizeof(cs_unit_t));
    ix->units[id].dir = ix_strdup(ix, dir);
    cbm_ht_set(ix->unit_by_dir, ix->units[id].dir, (void *)(intptr_t)(id + SKIP_ONE));
    return id;
}

/* The nearest directory at or above the file's own that holds a *.csproj
 * (the repository root when none does). Directories walked on the way are
 * memoized to the same unit. */
static int unit_of(cs_index_t *ix, CBMHashTable *dir_unit, const char *rel) {
    char dir[CS_KEY_BUF];
    const char *slash = strrchr(rel, '/');
    snprintf(dir, sizeof(dir), "%.*s", slash ? (int)(slash - rel) : 0, rel);
    char walked[CBM_SZ_64][CS_KEY_BUF / CBM_SZ_8];
    int nwalked = 0;
    int unit = CBM_NOT_FOUND;
    for (;;) {
        intptr_t memo = (intptr_t)cbm_ht_get(dir_unit, dir);
        if (memo > 0) {
            unit = (int)(memo - SKIP_ONE);
            break;
        }
        if (!dir[0] || dir_has_csproj(ix->repo_path, dir)) {
            unit = unit_get(ix, dir);
            if (nwalked < CBM_SZ_64 && strlen(dir) < sizeof(walked[0])) {
                snprintf(walked[nwalked++], sizeof(walked[0]), "%s", dir);
            }
            break;
        }
        if (nwalked < CBM_SZ_64 && strlen(dir) < sizeof(walked[0])) {
            snprintf(walked[nwalked++], sizeof(walked[0]), "%s", dir);
        }
        char *s = strrchr(dir, '/');
        if (s) {
            *s = '\0';
        } else {
            dir[0] = '\0';
        }
    }
    for (int i = 0; i < nwalked && unit >= 0; i++) {
        if (!cbm_ht_get(dir_unit, walked[i])) {
            char *k = ix_strdup(ix, walked[i]);
            if (k) {
                cbm_ht_set(dir_unit, k, (void *)(intptr_t)(unit + SKIP_ONE));
            }
        }
    }
    return unit;
}

static void unit_add_using(cs_index_t *ix, cs_unit_t *u, const char *target) {
    for (int i = 0; i < u->nusings; i++) {
        if (strcmp(u->usings[i], target) == 0) {
            return;
        }
    }
    if (u->nusings >= u->cap) {
        int ncap = u->cap ? u->cap * PAIR_LEN : CBM_SZ_8;
        const char **grown = (const char **)ix_alloc(ix, (size_t)ncap * sizeof(char *));
        if (!grown) {
            return;
        }
        if (u->nusings > 0) {
            memcpy(grown, u->usings, (size_t)u->nusings * sizeof(char *));
        }
        u->usings = grown;
        u->cap = ncap;
    }
    u->usings[u->nusings++] = ix_strdup(ix, target);
}

static int strp_cmp(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

/* Global usings of every unit: `global using` directives of its files and the
 * MSBuild usings of the project files in its directory. */
static void units_collect_usings(cs_index_t *ix) {
    for (int fi = 0; fi < ix->nfiles; fi++) {
        const cs_file_t *f = &ix->files[fi];
        if (f->unit < 0) {
            continue;
        }
        for (int u = 0; u < f->nusings; u++) {
            if (f->usings[u].kind == 'g') {
                unit_add_using(ix, &ix->units[f->unit], f->usings[u].target);
            }
        }
    }
    for (int ui = 0; ui < ix->nunits; ui++) {
        cs_unit_t *u = &ix->units[ui];
        char full[CS_KEY_BUF];
        if (snprintf(full, sizeof(full), "%s%s%s", ix->repo_path, u->dir[0] ? "/" : "", u->dir) >=
            (int)sizeof(full)) {
            continue;
        }
        cbm_dir_t *d = cbm_opendir(full);
        if (!d) {
            continue;
        }
        /* Project files of the directory, sorted: a deterministic union. */
        char *projs[CBM_SZ_32] = {0};
        int np = 0;
        cbm_dirent_t *e;
        while ((e = cbm_readdir(d)) != NULL) {
            size_t n = strlen(e->name);
            if (!e->is_dir && n > strlen(".csproj") &&
                strcmp(e->name + n - strlen(".csproj"), ".csproj") == 0 && np < CBM_SZ_32) {
                char rel[CS_KEY_BUF];
                snprintf(rel, sizeof(rel), "%s%s%s", u->dir, u->dir[0] ? "/" : "", e->name);
                projs[np] = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, rel);
                np += projs[np] != NULL;
            }
        }
        cbm_closedir(d);
        qsort(projs, (size_t)np, sizeof(char *), strp_cmp);
        for (int p = 0; p < np; p++) {
            char **usings = NULL;
            int n = cbm_doclinks_msbuild_usings(ix->repo_path, projs[p], &usings);
            for (int k = 0; k < n; k++) {
                unit_add_using(ix, u, usings[k]);
            }
            cbm_doclinks_free_strv(usings);
            cbm_free(CBM_MEM_CLASS_OTHER, projs[p]);
        }
        if (u->nusings > 1) {
            qsort(u->usings, (size_t)u->nusings, sizeof(char *), strp_cmp);
        }
    }
}

/* ── Entities ────────────────────────────────────────────────────── */

enum { CS_ENT_FAIL = -1, CS_ENT_SKIP = -2 };

/* The entity (fqn, arity), created on first sight. CS_ENT_SKIP for a name too
 * long to be a declaration (the type is left out, never guessed at);
 * CS_ENT_FAIL when memory ran out. */
static int entity_get(cs_index_t *ix, const char *fqn, int arity, const char *name, const char *ns,
                      char kind) {
    char key[CS_KEY_BUF];
    if (snprintf(key, sizeof(key), "%s`%d", fqn, arity) >= (int)sizeof(key)) {
        return CS_ENT_SKIP;
    }
    intptr_t v = (intptr_t)cbm_ht_get(ix->ent_by_key, key);
    if (v > 0) {
        return (int)(v - SKIP_ONE);
    }
    if (ix->nents >= ix->ecap) {
        int ncap = ix->ecap ? ix->ecap * PAIR_LEN : CBM_SZ_1K;
        cs_entity_t *grown =
            (cs_entity_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)ncap * sizeof(cs_entity_t));
        if (!grown) {
            return CS_ENT_FAIL;
        }
        if (ix->nents > 0) {
            memcpy(grown, ix->ents, (size_t)ix->nents * sizeof(cs_entity_t));
        }
        cbm_free(CBM_MEM_CLASS_OTHER, ix->ents);
        ix->ents = grown;
        ix->ecap = ncap;
    }
    int id = ix->nents++;
    cs_entity_t *e = &ix->ents[id];
    memset(e, 0, sizeof(*e));
    e->fqn = ix_strdup(ix, fqn);
    e->name = name;
    e->ns = ns;
    e->arity = arity;
    e->kind = kind;
    e->all_test = true;
    char *k = ix_strdup(ix, key);
    if (!e->fqn || !k) {
        ix->nents--;
        return CS_ENT_FAIL;
    }
    cbm_ht_set(ix->ent_by_key, k, (void *)(intptr_t)(id + SKIP_ONE));
    cs_ilist_t *l = ht_ilist(ix, ix->fqn_ents, fqn, true);
    if (l) {
        ilist_push(ix, l, id);
    }
    return id;
}

static bool entity_add_decl(cs_index_t *ix, cs_entity_t *e, int file, int type) {
    if (e->ndecls >= e->dcap) {
        int ncap = e->dcap ? e->dcap * PAIR_LEN : CBM_SZ_2;
        cs_decl_t *grown = (cs_decl_t *)ix_alloc(ix, (size_t)ncap * sizeof(cs_decl_t));
        if (!grown) {
            return false;
        }
        if (e->ndecls > 0) {
            memcpy(grown, e->decls, (size_t)e->ndecls * sizeof(cs_decl_t));
        }
        e->decls = grown;
        e->dcap = ncap;
    }
    e->decls[e->ndecls++] = (cs_decl_t){.file = file, .type = type};
    bool test = ix->files[file].is_test;
    e->any_prod = e->any_prod || !test;
    e->all_test = e->all_test && test;
    return true;
}

static bool mark_namespace(cs_index_t *ix, const char *ns) {
    if (!ns || !ns[0]) {
        return true;
    }
    char buf[CS_KEY_BUF];
    snprintf(buf, sizeof(buf), "%s", ns);
    for (;;) {
        if (!ht_mark(ix, ix->namespaces, buf)) {
            return false;
        }
        char *dot = strrchr(buf, '.');
        if (!dot) {
            break;
        }
        *dot = '\0';
    }
    return true;
}

/* The entities of every declared type. *skipped counts declarations left out
 * because their qualified name is too long to index (machine-generated
 * nesting tests); their names are quarantined, so a reference to one never
 * binds anything else. false only when memory ran out. */
static bool build_entities(cs_index_t *ix, int *skipped) {
    for (int fi = 0; fi < ix->nfiles; fi++) {
        cs_file_t *f = &ix->files[fi];
        for (int r = 0; r < f->nregions; r++) {
            if (!mark_namespace(ix, f->regions[r].ns)) {
                return false;
            }
        }
        for (int ti = 0; ti < f->ntypes; ti++) {
            cs_type_t *t = &f->types[ti];
            int region = (t->region >= 0 && t->region < f->nregions) ? t->region : 0;
            const char *ns = f->regions[region].ns;
            char fqn[CS_KEY_BUF];
            char key[CS_KEY_BUF];
            int fl = (ns && ns[0]) ? snprintf(fqn, sizeof(fqn), "%s.%s", ns, t->path)
                                   : snprintf(fqn, sizeof(fqn), "%s", t->path);
            int kl = snprintf(key, sizeof(key), "%s\x1f%s", ns ? ns : "", t->name);
            int id = (fl < 0 || fl >= (int)sizeof(fqn) || kl < 0 || kl >= (int)sizeof(key))
                         ? CS_ENT_SKIP
                         : entity_get(ix, fqn, t->arity, t->name, ns, t->kind);
            if (id == CS_ENT_SKIP) {
                (*skipped)++;
                if (!quarantine_name(ix, f, t->name)) {
                    return false;
                }
                continue;
            }
            if (id < 0) {
                return false;
            }
            t->entity = id;
            if (!entity_add_decl(ix, &ix->ents[id], fi, ti)) {
                return false;
            }
            ix->ents[id].incomplete = ix->ents[id].incomplete || t->incomplete;
            if (!ht_mark(ix, ix->type_names, t->name)) {
                return false;
            }
            if (!strchr(t->path, '.')) {
                cs_ilist_t *l = ht_ilist(ix, ix->ns_types, key, true);
                if (!l || !ilist_push(ix, l, id)) {
                    return false;
                }
            }
        }
    }
    return true;
}

/* ── Node binding ────────────────────────────────────────────────── */

static const cbm_gbuf_node_t *decl_node(const cs_index_t *ix, const cbm_gbuf_t *g, cs_decl_t d) {
    const cs_file_t *f = &ix->files[d.file];
    const cs_type_t *t = &f->types[d.type];
    if (!t->owns_node || t->kind == 'd') {
        return NULL; /* a same-file twin took the node; delegates have none */
    }
    char qn[CS_KEY_BUF];
    if (snprintf(qn, sizeof(qn), "%s.%s", f->module_qn, t->path) >= (int)sizeof(qn)) {
        return NULL;
    }
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(g, qn);
    return (n && cbm_label_is_type_like(n->label)) ? n : NULL;
}

static bool label_is_callable(const char *l) {
    return l && (strcmp(l, "Method") == 0 || strcmp(l, "Function") == 0);
}

static bool label_is_value(const char *l) {
    return l && (strcmp(l, "Field") == 0 || strcmp(l, "Variable") == 0 ||
                 strcmp(l, "Property") == 0 || strcmp(l, "Constant") == 0);
}

/* The member's node; NULL when it has none. The node of <module>.<owner>.
 * <name> belongs to the last declaration of that qualified name in the file:
 * overloads of one type share it, but when `Foo` and `Foo<T>` both declare
 * the member, the earlier type's member has no node of its own (the same
 * rule as for the types themselves). NULL as well when the slot holds a
 * member of the other kind (an explicit implementation that won the name). */
static const cbm_gbuf_node_t *member_node(const cs_index_t *ix, const cbm_gbuf_t *g, int file,
                                          const cs_member_t *m) {
    const cs_file_t *f = &ix->files[file];
    const cs_member_t *last = m;
    for (const cs_member_t *p = m + SKIP_ONE; p < f->members + f->nmembers; p++) {
        if (strcmp(p->owner, m->owner) != 0 || strcmp(p->name, m->name) != 0) {
            break;
        }
        last = p;
    }
    if (f->types[last->type_idx].entity != f->types[m->type_idx].entity) {
        return NULL;
    }
    char qn[CS_KEY_BUF];
    if (snprintf(qn, sizeof(qn), "%s.%s.%s", f->module_qn, m->owner, m->name) >= (int)sizeof(qn)) {
        return NULL;
    }
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(g, qn);
    if (!n) {
        return NULL;
    }
    bool ok = m->kind == 'c' ? label_is_callable(n->label) : label_is_value(n->label);
    return ok ? n : NULL;
}

/* Representative ordering of declarations (the prototype's presentation
 * rule, made deterministic without node ids or lines): an implementation
 * before a /ref/ API stub, the source file itself, the longest common
 * directory prefix with it, then path and declaration order. */
static int common_dir_prefix(const char *a, const char *b) {
    int n = 0;
    for (;;) {
        const char *sa = strchr(a, '/');
        const char *sb = strchr(b, '/');
        if (!sa || !sb) {
            return n;
        }
        size_t la = (size_t)(sa - a);
        if (la != (size_t)(sb - b) || memcmp(a, b, la) != 0) {
            return n;
        }
        n++;
        a = sa + SKIP_ONE;
        b = sb + SKIP_ONE;
    }
}

static bool path_is_ref_stub(const char *rel) {
    return strncmp(rel, "ref/", 4) == 0 || strstr(rel, "/ref/") != NULL;
}

static bool decl_better(const cs_index_t *ix, int fa, int la, int fb, int lb, const char *src) {
    const char *ra = ix->files[fa].rel_path;
    const char *rb = ix->files[fb].rel_path;
    bool refa = path_is_ref_stub(ra);
    bool refb = path_is_ref_stub(rb);
    if (refa != refb) {
        return !refa;
    }
    bool sa = strcmp(ra, src) == 0;
    bool sb = strcmp(rb, src) == 0;
    if (sa != sb) {
        return sa;
    }
    int pa = common_dir_prefix(ra, src);
    int pb = common_dir_prefix(rb, src);
    if (pa != pb) {
        return pa > pb;
    }
    int c = strcmp(ra, rb);
    if (c != 0) {
        return c < 0;
    }
    return la < lb;
}

/* ── Resolution context ──────────────────────────────────────────── */

enum { CS_WANT_ANY = 0, CS_WANT_TYPE, CS_WANT_MEMBER };
enum { CS_OK = 0, CS_UNRES, CS_LOCAL };
enum { CS_MAX_USINGS = 128 };

typedef struct {
    int st;
    const cbm_gbuf_node_t *node;
    bool exact;
    int reason;
    bool sig_mismatch;
    bool is_namespace; /* a graph gap because the path is one of the repository's
                        * namespaces (declared, and without a node) */
} cs_res_t;

typedef struct {
    const cs_index_t *ix;
    const cbm_gbuf_t *g;
    int file;
    const cs_file_t *f;
    const char *ns;
    int chain[CS_MAX_CHAIN];
    int nchain;
    const char *tparams[CS_MAX_CHAIN + SKIP_ONE];
    int ntparams;
    bool prod;
    int unit;
    bool glob;
    bool inherit; /* false while resolving base types (no recursion) */
    const char *usings[CS_MAX_USINGS];
    int nusings;
    const cs_using_t *aliases[CS_MAX_USINGS];
    int naliases;
    const char *statics[CS_MAX_USINGS];
    int nstatics;
} cs_ctx_t;

typedef struct {
    char kind; /* T type, M member, G graph gap, X external */
    int ent;
    const char *name;
} cs_cand_t;

typedef struct {
    cs_cand_t items[CS_MAX_CANDS];
    int count;
} cs_cands_t;

static cs_res_t res_edge(const cbm_gbuf_node_t *n, bool exact) {
    return (cs_res_t){.st = CS_OK, .node = n, .exact = exact};
}

static cs_res_t res_unres(int reason) {
    return (cs_res_t){.st = CS_UNRES, .reason = reason};
}

static void cands_push(cs_cands_t *c, char kind, int ent, const char *name) {
    for (int i = 0; i < c->count; i++) {
        if (c->items[i].kind == kind && c->items[i].ent == ent &&
            ((!name && !c->items[i].name) ||
             (name && c->items[i].name && strcmp(name, c->items[i].name) == 0))) {
            return;
        }
    }
    if (c->count < CS_MAX_CANDS) {
        c->items[c->count++] = (cs_cand_t){.kind = kind, .ent = ent, .name = name};
    }
}

static int region_at(const cs_file_t *f, uint32_t line) {
    int best = 0;
    for (int r = SKIP_ONE; r < f->nregions; r++) {
        const cs_region_t *g = &f->regions[r];
        if (g->parent < 0 || line < g->start || line > g->end) {
            continue;
        }
        /* the innermost region; of two starting on one line, the later one */
        if (g->start >= f->regions[best].start) {
            best = r;
        }
    }
    return best;
}

/* Usings in scope at `region`: its own and every enclosing region's (a
 * namespace block's usings apply inside it only), plus the unit's global
 * usings. */
static void ctx_collect_usings(cs_ctx_t *c, int region) {
    const cs_file_t *f = c->f;
    int guard = 0;
    for (int r = region; r >= 0 && r < f->nregions && guard < f->nregions; guard++) {
        for (int u = 0; u < f->nusings; u++) {
            const cs_using_t *us = &f->usings[u];
            if (us->region != r) {
                continue;
            }
            if (us->kind == 'n' && c->nusings < CS_MAX_USINGS) {
                c->usings[c->nusings++] = us->target;
            } else if (us->kind == 'a' && c->naliases < CS_MAX_USINGS) {
                c->aliases[c->naliases++] = us;
            } else if (us->kind == 's' && c->nstatics < CS_MAX_USINGS) {
                c->statics[c->nstatics++] = us->target;
            }
        }
        if (r == 0) {
            break;
        }
        r = f->regions[r].parent;
    }
    if (c->unit >= 0) {
        const cs_unit_t *u = &c->ix->units[c->unit];
        for (int i = 0; i < u->nusings && c->nusings < CS_MAX_USINGS; i++) {
            c->usings[c->nusings++] = u->usings[i];
        }
    }
}

/* ── Arity, nesting, members ─────────────────────────────────────── */

/* Entities of `list` the written arity can denote: type arguments select that
 * arity; none select the arity-0 type when one is declared, else (only then)
 * any arity. */
static int arity_filter(const cs_index_t *ix, const int *list, int n, int arity, int *out,
                        int cap) {
    int k = 0;
    if (arity == CS_ARITY_NONE) {
        for (int i = 0; i < n && k < cap; i++) {
            if (ix->ents[list[i]].arity == 0) {
                out[k++] = list[i];
            }
        }
        if (k > 0) {
            return k;
        }
        for (int i = 0; i < n && k < cap; i++) {
            out[k++] = list[i];
        }
        return k;
    }
    for (int i = 0; i < n && k < cap; i++) {
        if (ix->ents[list[i]].arity == arity) {
            out[k++] = list[i];
        }
    }
    return k;
}

static int fqn_lookup(const cs_index_t *ix, const char *fqn, int arity, int *out, int cap) {
    const cs_ilist_t *l = (const cs_ilist_t *)cbm_ht_get(ix->fqn_ents, fqn);
    if (!l) {
        return 0;
    }
    return arity_filter(ix, l->items, l->count, arity, out, cap);
}

/* Types nested directly in entity `ent` named `name` (all arities). */
static int nested_of(const cs_index_t *ix, int ent, const char *name, int *out, int cap) {
    int k = 0;
    const cs_entity_t *e = &ix->ents[ent];
    for (int d = 0; d < e->ndecls; d++) {
        const cs_file_t *f = &ix->files[e->decls[d].file];
        const cs_type_t *t = &f->types[e->decls[d].type];
        char path[CS_KEY_BUF];
        if (snprintf(path, sizeof(path), "%s.%s", t->path, name) >= (int)sizeof(path)) {
            continue;
        }
        int lo = 0;
        int hi = f->ntypes;
        while (lo < hi) {
            int mid = lo + ((hi - lo) / PAIR_LEN);
            if (strcmp(f->types[f->types_by_path[mid]].path, path) < 0) {
                lo = mid + SKIP_ONE;
            } else {
                hi = mid;
            }
        }
        for (int i = lo; i < f->ntypes && strcmp(f->types[f->types_by_path[i]].path, path) == 0;
             i++) {
            int id = f->types[f->types_by_path[i]].entity;
            bool dup = false;
            for (int j = 0; j < k; j++) {
                dup = dup || out[j] == id;
            }
            if (!dup && id >= 0 && k < cap) {
                out[k++] = id;
            }
        }
    }
    return k;
}

typedef struct {
    int file;
    const cs_member_t *m;
} cs_mref_t;

/* Which members a lookup may select. */
enum {
    CS_SEL_CALLABLES = 1, /* a parameter or type-argument list was written */
    CS_SEL_CTORS = 2,     /* constructors count: the type itself was named with a
                           * parameter list, or qualified by its own name */
};

static int member_sel(bool callables_only, bool ctors) {
    return (callables_only ? CS_SEL_CALLABLES : 0) | (ctors ? CS_SEL_CTORS : 0);
}

/* Members named `name` declared by the entity (every declaration). Explicit
 * interface implementations are not addressable by simple name; a parameter
 * list selects callables only; a type-argument list (`arity` > 0) selects the
 * generic methods of that arity -- constructors and other non-generic members
 * are no candidates then (R3). A constructor is not a member name lookup
 * finds: it is selected only where `sel` says so, and never inherited. */
static int members_of(const cs_index_t *ix, int ent, const char *name, int sel, int arity,
                      cs_mref_t *out, int cap) {
    int k = 0;
    const cs_entity_t *e = &ix->ents[ent];
    bool callables_only = (sel & CS_SEL_CALLABLES) != 0;
    if (!(sel & CS_SEL_CTORS) && strcmp(name, e->name) == 0) {
        return 0; /* only its constructors carry the type's own name */
    }
    for (int d = 0; d < e->ndecls; d++) {
        const cs_file_t *f = &ix->files[e->decls[d].file];
        const char *owner = f->types[e->decls[d].type].path;
        int lo = 0;
        int hi = f->nmembers;
        while (lo < hi) {
            int mid = lo + ((hi - lo) / PAIR_LEN);
            const cs_member_t *m = &f->members[mid];
            int c = strcmp(m->owner, owner);
            if (c == 0) {
                c = strcmp(m->name, name);
            }
            if (c < 0) {
                lo = mid + SKIP_ONE;
            } else {
                hi = mid;
            }
        }
        for (int i = lo; i < f->nmembers; i++) {
            const cs_member_t *m = &f->members[i];
            if (strcmp(m->owner, owner) != 0 || strcmp(m->name, name) != 0) {
                break;
            }
            if (m->type_idx != e->decls[d].type) {
                continue; /* a same-path declaration of another arity owns it */
            }
            if (m->explicit_impl || (callables_only && m->kind != 'c')) {
                continue;
            }
            if (arity > 0 && (m->kind != 'c' || m->arity != arity)) {
                continue;
            }
            if (k < cap) {
                out[k++] = (cs_mref_t){.file = e->decls[d].file, .m = m};
            }
        }
    }
    return k;
}

static int super_bfs(const cs_index_t *ix, int ent, int *out, int cap) {
    int n = 0;
    int head = 0;
    int depth_end = 0;
    int depth = 0;
    out[n++] = ent;
    depth_end = n;
    while (head < n && depth < CS_BFS_DEPTH) {
        int cur = out[head++];
        const cs_entity_t *e = &ix->ents[cur];
        for (int b = 0; b < e->nbases; b++) {
            bool seen = false;
            for (int j = 0; j < n; j++) {
                seen = seen || out[j] == e->bases[b];
            }
            if (!seen && n < cap) {
                out[n++] = e->bases[b];
            }
        }
        if (head == depth_end) {
            depth++;
            depth_end = n;
        }
    }
    /* drop `ent` itself: callers want the supertypes */
    memmove(out, out + SKIP_ONE, (size_t)(n - SKIP_ONE) * sizeof(int));
    return n - SKIP_ONE;
}

static bool open_hierarchy(const cs_index_t *ix, int ent) {
    if (ix->ents[ent].open) {
        return true;
    }
    int sup[CS_MAX_BFS];
    int n = super_bfs(ix, ent, sup, CS_MAX_BFS);
    for (int i = 0; i < n; i++) {
        if (ix->ents[sup[i]].open) {
            return true;
        }
    }
    return false;
}

/* ── Visibility ──────────────────────────────────────────────────── */

static bool ent_visible(const cs_ctx_t *c, int ent) {
    const cs_entity_t *e = &c->ix->ents[ent];
    if (c->prod) {
        return e->any_prod;
    }
    /* test code: a global-namespace test type is local to its own program */
    if (e->all_test && (!e->ns || !e->ns[0])) {
        for (int d = 0; d < e->ndecls; d++) {
            if (c->ix->files[e->decls[d].file].unit == c->unit) {
                return true;
            }
        }
        return false;
    }
    return true;
}

static bool cand_visible(const cs_ctx_t *c, const cs_cand_t *cd) {
    return (cd->kind == 'T' || cd->kind == 'M') ? ent_visible(c, cd->ent) : true;
}

/* ── Reference syntax ────────────────────────────────────────────── */

typedef struct {
    char name[CBM_SZ_256];
    int arity; /* CS_ARITY_NONE: no type arguments written */
} cs_seg_t;

typedef struct {
    cs_seg_t segs[CS_MAX_SEGS];
    int nsegs;
    bool has_params;
    char params[CS_MAX_PARAMS][CS_PARAM_BUF];
    int nparams;
    char docid; /* 0, or T M P F E N */
    bool glob;
    bool op;              /* operator / indexer / conversion */
    bool keyword_rewrite; /* first segment was a keyword alias (int -> System.Int32) */
} cs_ref_t;

/* Index just past the bracket group opened at s[i] (< { [ ( nest together). */
static size_t group_end(const char *s, size_t n, size_t i) {
    int depth = 0;
    for (size_t k = i; k < n; k++) {
        char c = s[k];
        if (c == '<' || c == '{' || c == '[' || c == '(') {
            depth++;
        } else if (c == '>' || c == '}' || c == ']' || c == ')') {
            depth--;
            if (depth == 0) {
                return k + SKIP_ONE;
            }
        }
    }
    return n;
}

/* Count top-level comma-separated items of s[0..n). */
static int count_top(const char *s, size_t n) {
    bool any = false;
    int items = 1;
    int depth = 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (c == '<' || c == '{' || c == '[' || c == '(') {
            depth++;
        } else if (c == '>' || c == '}' || c == ']' || c == ')') {
            depth--;
        } else if (c == ',' && depth == 0) {
            items++;
        }
        if (!isspace((unsigned char)c)) {
            any = true;
        }
    }
    return any ? items : 0;
}

static bool ident_ok(const char *s) {
    if (strcmp(s, "#ctor") == 0 || strcmp(s, "#cctor") == 0) {
        return true;
    }
    if (!s[0] || !(isalpha((unsigned char)s[0]) || s[0] == '_')) {
        return false;
    }
    for (const char *p = s + SKIP_ONE; *p; p++) {
        if (!(isalnum((unsigned char)*p) || *p == '_')) {
            return false;
        }
    }
    return true;
}

/* One dotted segment: `Name`, `Name{T,U}`, `Name<T>`, `Name``2`. */
static bool parse_seg(const char *s, size_t n, cs_seg_t *out) {
    while (n > 0 && isspace((unsigned char)*s)) {
        s++;
        n--;
    }
    while (n > 0 && isspace((unsigned char)s[n - SKIP_ONE])) {
        n--;
    }
    out->arity = CS_ARITY_NONE;
    size_t name_end = n;
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '`') {
            name_end = i;
            size_t d = i;
            while (d < n && s[d] == '`') {
                d++;
            }
            out->arity = atoi(s + d);
            break;
        }
        if (s[i] == '{' || s[i] == '<') {
            name_end = i;
            size_t e = group_end(s, n, i);
            out->arity = count_top(s + i + SKIP_ONE, e > i + PAIR_LEN ? e - i - PAIR_LEN : 0);
            break;
        }
    }
    const char *name = s;
    if (name_end > 0 && name[0] == '@') {
        name++;
        name_end--;
    }
    if (name_end == 0 || name_end >= sizeof(out->name)) {
        return false;
    }
    memcpy(out->name, name, name_end);
    out->name[name_end] = '\0';
    return ident_ok(out->name);
}

/* Split a dotted path (dots inside type-argument groups do not split). */
static bool parse_path(const char *s, size_t n, cs_seg_t *segs, int *nsegs) {
    *nsegs = 0;
    size_t start = 0;
    int depth = 0;
    for (size_t i = 0; i <= n; i++) {
        char c = i < n ? s[i] : '.';
        if (c == '<' || c == '{' || c == '[' || c == '(') {
            depth++;
        } else if (c == '>' || c == '}' || c == ']' || c == ')') {
            depth--;
        } else if (c == '.' && depth == 0) {
            if (*nsegs >= CS_MAX_SEGS || !parse_seg(s + start, i - start, &segs[*nsegs])) {
                return false;
            }
            (*nsegs)++;
            start = i + SKIP_ONE;
        }
    }
    return *nsegs > 0;
}

static const char *const CS_KEYWORD_TYPES[][2] = {
    {"int", "Int32"},     {"string", "String"},   {"object", "Object"}, {"bool", "Boolean"},
    {"byte", "Byte"},     {"sbyte", "SByte"},     {"short", "Int16"},   {"ushort", "UInt16"},
    {"uint", "UInt32"},   {"long", "Int64"},      {"ulong", "UInt64"},  {"float", "Single"},
    {"double", "Double"}, {"decimal", "Decimal"}, {"char", "Char"},     {"nint", "IntPtr"},
    {"nuint", "UIntPtr"}, {"void", "Void"},
};

static const char *keyword_type(const char *s) {
    for (size_t i = 0; i < sizeof(CS_KEYWORD_TYPES) / sizeof(CS_KEYWORD_TYPES[0]); i++) {
        if (strcmp(s, CS_KEYWORD_TYPES[i][0]) == 0) {
            return CS_KEYWORD_TYPES[i][1];
        }
    }
    return NULL;
}

/* An operator / indexer / conversion reference: `operator +`, `this[int]`,
 * `op_Addition`, `Item(int)` (optionally after a qualifier and a dot).
 * Returns the qualifier length, or -1 when `v` is not one. */
static int operator_qualifier(const char *v) {
    for (const char *p = v; *p; p++) {
        if (p != v && p[-1] != '.') {
            continue;
        }
        const char *q = p;
        while (*q == ' ') {
            q++;
        }
        if (strncmp(q, "implicit ", 9) == 0 || strncmp(q, "explicit ", 9) == 0) {
            q += 9;
            while (*q == ' ') {
                q++;
            }
        }
        bool hit =
            (strncmp(q, "operator", 8) == 0 && !isalnum((unsigned char)q[8]) && q[8] != '_') ||
            (strncmp(q, "this", 4) == 0 && (q[4] == '[' || q[4] == ' ')) ||
            (strncmp(q, "op_", 3) == 0 && isupper((unsigned char)q[3])) ||
            (strncmp(q, "Item", 4) == 0 && (q[4] == '(' || q[4] == ' '));
        if (hit) {
            if (strncmp(q, "this", 4) == 0 && q[4] == ' ') {
                const char *r = q + 4;
                while (*r == ' ') {
                    r++;
                }
                hit = *r == '[';
            }
            if (strncmp(q, "Item", 4) == 0 && q[4] == ' ') {
                const char *r = q + 4;
                while (*r == ' ') {
                    r++;
                }
                hit = *r == '(';
            }
        }
        if (hit) {
            return p == v ? 0 : (int)(p - v - SKIP_ONE);
        }
    }
    return CBM_NOT_FOUND;
}

static bool parse_cref(const char *raw, cs_ref_t *r) {
    memset(r, 0, sizeof(*r));
    char v[CS_REF_BUF];
    snprintf(v, sizeof(v), "%s", raw ? raw : "");
    char *s = v;
    while (isspace((unsigned char)*s)) {
        s++;
    }
    size_t n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n - SKIP_ONE])) {
        s[--n] = '\0';
    }
    if (n == 0) {
        return false;
    }
    if (n > PAIR_LEN && s[1] == ':' && strchr("TMPFENO!", s[0])) {
        char k = s[0];
        if (k == '!') {
            return false; /* compiler error marker: the compiler could not bind it */
        }
        s += PAIR_LEN;
        while (isspace((unsigned char)*s)) {
            s++;
        }
        if (k == 'O') { /* DocFX overload-group id: a member without a signature */
            k = 'M';
            char *paren = strchr(s, '(');
            if (paren) {
                *paren = '\0';
            }
        }
        r->docid = k;
    }
    if (strncmp(s, "global::", 8) == 0) {
        r->glob = true;
        s += 8;
    }
    n = strlen(s);
    int opq = operator_qualifier(s);
    if (opq >= 0) {
        r->op = true;
        if (opq > 0) {
            return parse_path(s, (size_t)opq, r->segs, &r->nsegs);
        }
        return true;
    }
    /* `Path(params)`: the first top-level parenthesis starts the list. */
    size_t paren = n;
    int depth = 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (c == '<' || c == '{' || c == '[') {
            depth++;
        } else if (c == '>' || c == '}' || c == ']') {
            depth--;
        } else if (c == '(' && depth == 0) {
            paren = i;
            break;
        }
    }
    if (!parse_path(s, paren, r->segs, &r->nsegs)) {
        return false;
    }
    if (paren < n) {
        r->has_params = true;
        size_t close = group_end(s, n, paren);
        size_t inner_s = paren + SKIP_ONE;
        size_t inner_e = close > inner_s ? close - SKIP_ONE : inner_s;
        if (close > n || s[close - SKIP_ONE] != ')') {
            inner_e = n;
        }
        size_t start = inner_s;
        int d = 0;
        bool any = false;
        for (size_t i = inner_s; i < inner_e; i++) {
            if (!isspace((unsigned char)s[i])) {
                any = true;
                break;
            }
        }
        for (size_t i = inner_s; any && i <= inner_e; i++) {
            char c = i < inner_e ? s[i] : ',';
            if (c == '<' || c == '{' || c == '[' || c == '(') {
                d++;
            } else if (c == '>' || c == '}' || c == ']' || c == ')') {
                d--;
            } else if (c == ',' && d == 0) {
                if (r->nparams >= CS_MAX_PARAMS) {
                    return false;
                }
                cbm_doclink_cs_norm_type(s + start, i - start, r->params[r->nparams],
                                         sizeof(r->params[0]));
                r->nparams++;
                start = i + SKIP_ONE;
            }
        }
    }
    return true;
}

/* ── Scope levels ────────────────────────────────────────────────── */

static bool has_members(const cs_index_t *ix, int ent, const char *name, int sel, int arity) {
    cs_mref_t tmp[SKIP_ONE];
    return members_of(ix, ent, name, sel, arity, tmp, SKIP_ONE) > 0;
}

/* The types of `list` a scope level offers for the written arity. Type
 * arguments select that arity. Without them the name is the arity-0 type
 * (R2); only a lookup that found no such type at ANY level runs again
 * `relaxed`, where a generic type of the name is taken as meant. */
static void push_types(const cs_index_t *ix, cs_cands_t *c, const int *list, int n, int arity,
                       bool relaxed) {
    for (int i = 0; i < n; i++) {
        int a = ix->ents[list[i]].arity;
        bool take = arity == CS_ARITY_NONE ? (relaxed || a == 0) : a == arity;
        if (take) {
            cands_push(c, 'T', list[i], NULL);
        }
    }
}

/* What one scope level is asked for. */
typedef struct {
    const char *name;
    int arity; /* written type arguments; CS_ARITY_NONE when none */
    int want;
    int sel; /* CS_SEL_* for the members of the enclosing types */
    bool relaxed;
} cs_query_t;

static void chain_level(const cs_ctx_t *c, int ent, const cs_query_t *q, cs_cands_t *out) {
    const cs_index_t *ix = c->ix;
    if (q->want != CS_WANT_MEMBER) {
        if (strcmp(ix->ents[ent].name, q->name) == 0) {
            push_types(ix, out, &ent, SKIP_ONE, q->arity, q->relaxed);
        }
        int nested[CS_MAX_CANDS];
        int nn = nested_of(ix, ent, q->name, nested, CS_MAX_CANDS);
        push_types(ix, out, nested, nn, q->arity, q->relaxed);
    }
    if (q->want != CS_WANT_TYPE && has_members(ix, ent, q->name, q->sel, q->arity)) {
        cands_push(out, 'M', ent, q->name);
    }
}

static void inherited_level(const cs_ctx_t *c, int ent, const cs_query_t *q, cs_cands_t *out) {
    const cs_index_t *ix = c->ix;
    int sup[CS_MAX_BFS];
    int n = super_bfs(ix, ent, sup, CS_MAX_BFS);
    for (int i = 0; i < n && out->count == 0; i++) {
        if (q->want != CS_WANT_MEMBER) {
            int nested[CS_MAX_CANDS];
            int nn = nested_of(ix, sup[i], q->name, nested, CS_MAX_CANDS);
            push_types(ix, out, nested, nn, q->arity, q->relaxed);
        }
        if (q->want != CS_WANT_TYPE &&
            has_members(ix, sup[i], q->name, q->sel & ~CS_SEL_CTORS, q->arity)) {
            cands_push(out, 'M', sup[i], q->name);
        }
    }
}

static void ns_level(const cs_ctx_t *c, const char *ns, const cs_query_t *q, cs_cands_t *out) {
    char key[CS_KEY_BUF];
    if (snprintf(key, sizeof(key), "%s\x1f%s", ns, q->name) >= (int)sizeof(key)) {
        return;
    }
    const cs_ilist_t *l = (const cs_ilist_t *)cbm_ht_get(c->ix->ns_types, key);
    if (l) {
        push_types(c->ix, out, l->items, l->count, q->arity, q->relaxed);
    }
}

static bool external_prefix(const char *fqn) {
    static const char *const prefixes[] = {
        "Xunit.",
        "Microsoft.CodeAnalysis.",
        "Microsoft.Build.",
        "Microsoft.DotNet.XUnitExtensions.",
        "Microsoft.DotNet.RemoteExecutor.",
        "Microsoft.VisualStudio.",
        "NuGet.",
        "Moq.",
        "NUnit.",
        "Mono.Cecil.",
        "Newtonsoft.",
        "Windows.",
        "Microsoft.Diagnostics.Runtime.",
        "BenchmarkDotNet.",
        "FsCheck.",
        "Azure.",
        "Microsoft.Cci.",
        "Microsoft.Win32.TaskScheduler.",
        "Microsoft.Office.",
        "Microsoft.Web.",
        "Microsoft.Extensions.DependencyModel.Tests.",
        "Microsoft.SqlServer.",
    };
    char probe[CS_KEY_BUF];
    snprintf(probe, sizeof(probe), "%s.", fqn);
    for (size_t i = 0; i < sizeof(prefixes) / sizeof(prefixes[0]); i++) {
        if (strncmp(probe, prefixes[i], strlen(prefixes[i])) == 0) {
            return true;
        }
    }
    return false;
}

/* Strip a type-argument list off a written type name (`A.B<int>` -> `A.B`),
 * returning the arity written there (CS_ARITY_NONE when none). */
static int strip_type_args(const char *in, char *out, size_t cap) {
    snprintf(out, cap, "%s", in);
    char *lt = strchr(out, '<');
    if (!lt) {
        return CS_ARITY_NONE;
    }
    size_t n = strlen(out);
    size_t e = group_end(out, n, (size_t)(lt - out));
    int arity = count_top(
        lt + SKIP_ONE, e > (size_t)(lt - out) + PAIR_LEN ? e - (size_t)(lt - out) - PAIR_LEN : 0);
    *lt = '\0';
    return arity;
}

/* The target of a using alias (C# aliases name a namespace or a type): the
 * type, a graph gap for a corpus namespace (no node) or a type without one,
 * else an outside name. */
static void fqn_cand(const cs_ctx_t *c, const char *fqn, int arity, cs_cands_t *out) {
    const cs_index_t *ix = c->ix;
    int sel[CS_MAX_CANDS];
    int k = fqn_lookup(ix, fqn, arity, sel, CS_MAX_CANDS);
    if (k > 0) {
        for (int i = 0; i < k; i++) {
            cands_push(out, 'T', sel[i], NULL);
        }
        return;
    }
    if (!external_prefix(fqn) && cbm_ht_get(ix->namespaces, fqn)) {
        cands_push(out, 'G', CBM_NOT_FOUND, NULL);
        return;
    }
    cands_push(out, 'X', CBM_NOT_FOUND, NULL);
}

/* Candidates of level `lvl` for a simple name. Levels: 2k chain(k), 2k+1
 * inherited(k) for every chain type, then the namespace chain (innermost
 * first, global last), the alias, the usings. Returns false past the last
 * level; *exact is set for the levels that bind exactly (alias). */
static bool level_cands(const cs_ctx_t *c, int lvl, const cs_query_t *q, cs_cands_t *out,
                        bool *exact) {
    out->count = 0;
    *exact = false;
    int chain_levels = c->nchain * PAIR_LEN;
    if (lvl < chain_levels) {
        int ent = c->chain[lvl / PAIR_LEN];
        if (lvl % PAIR_LEN == 0) {
            chain_level(c, ent, q, out);
        } else if (c->inherit) {
            inherited_level(c, ent, q, out);
        }
        return true;
    }
    lvl -= chain_levels;
    /* namespace chain: c->ns, its prefixes, then "" */
    char ns[CS_KEY_BUF];
    snprintf(ns, sizeof(ns), "%s", c->ns ? c->ns : "");
    int ns_levels = ns[0] ? SKIP_ONE : 0;
    for (const char *p = ns; *p; p++) {
        ns_levels += *p == '.';
    }
    ns_levels += SKIP_ONE; /* the global namespace */
    if (lvl < ns_levels) {
        for (int i = 0; i < lvl; i++) {
            char *dot = strrchr(ns, '.');
            if (dot) {
                *dot = '\0';
            } else {
                ns[0] = '\0';
            }
        }
        if (q->want != CS_WANT_MEMBER) {
            ns_level(c, ns, q, out);
        }
        return true;
    }
    lvl -= ns_levels;
    if (lvl == 0) {
        /* an alias names a type or a namespace: no candidate for `Name{T}` */
        for (int i = 0; q->arity <= 0 && i < c->naliases; i++) {
            if (strcmp(c->aliases[i]->alias, q->name) == 0) {
                char base[CS_KEY_BUF];
                int a = strip_type_args(c->aliases[i]->target, base, sizeof(base));
                fqn_cand(c, base, a > 0 ? a : CS_ARITY_NONE, out);
                *exact = true;
                break;
            }
        }
        return true;
    }
    if (lvl == SKIP_ONE) {
        if (q->want != CS_WANT_MEMBER) {
            for (int i = 0; i < c->nusings; i++) {
                ns_level(c, c->usings[i], q, out);
            }
        }
        if (q->want == CS_WANT_ANY) {
            for (int i = 0; i < c->nstatics; i++) {
                char base[CS_KEY_BUF];
                (void)strip_type_args(c->statics[i], base, sizeof(base));
                int sel[CS_MAX_CANDS];
                int k = fqn_lookup(c->ix, base, CS_ARITY_NONE, sel, CS_MAX_CANDS);
                for (int j = 0; j < k; j++) {
                    if (has_members(c->ix, sel[j], q->name, q->sel & ~CS_SEL_CTORS, q->arity)) {
                        cands_push(out, 'M', sel[j], q->name);
                    }
                }
            }
        }
        return true;
    }
    return false;
}

typedef enum { CS_LOOKUP_NONE = 0, CS_LOOKUP_FOUND, CS_LOOKUP_INVISIBLE } cs_lookup_t;

/* The first scope level with a visible candidate. A level whose candidates
 * are all invisible (product code naming test-only code) does not bind: when
 * no later level has a visible candidate either, the result is INVISIBLE. */
static cs_lookup_t lookup_pass(const cs_ctx_t *c, const cs_query_t *q, cs_cands_t *out,
                               bool *exact) {
    cs_cands_t lv;
    bool invisible = false;
    for (int lvl = 0;; lvl++) {
        bool lvl_exact = false;
        if (!level_cands(c, lvl, q, &lv, &lvl_exact)) {
            break;
        }
        if (lv.count == 0) {
            continue;
        }
        out->count = 0;
        for (int i = 0; i < lv.count; i++) {
            if (cand_visible(c, &lv.items[i])) {
                cands_push(out, lv.items[i].kind, lv.items[i].ent, lv.items[i].name);
            }
        }
        if (out->count > 0) {
            *exact = lvl_exact;
            return CS_LOOKUP_FOUND;
        }
        invisible = true;
    }
    out->count = 0;
    return invisible ? CS_LOOKUP_INVISIBLE : CS_LOOKUP_NONE;
}

/* Scope lookup of a simple name. A name written without type arguments is
 * looked up as the arity-0 type first, through every level (R2: a generic
 * type of the same name never stands in for a declared arity-0 one); only
 * when no level has anything by that name does a second pass accept a
 * generic type. */
static cs_lookup_t lookup(const cs_ctx_t *c, const char *N, int arity, int want, int sel,
                          cs_cands_t *out, bool *exact) {
    cs_query_t q = {.name = N, .arity = arity, .want = want, .sel = sel, .relaxed = false};
    cs_lookup_t r = lookup_pass(c, &q, out, exact);
    if (r == CS_LOOKUP_NONE && arity == CS_ARITY_NONE) {
        q.relaxed = true;
        r = lookup_pass(c, &q, out, exact);
    }
    return r;
}

/* ── Results ─────────────────────────────────────────────────────── */

/* The entity's representative node: never a test declaration for product
 * code; then the representative ordering. A visible entity whose eligible
 * declarations all lack a node is a graph gap. */
static cs_res_t type_result(const cs_ctx_t *c, int ent, bool exact) {
    const cs_index_t *ix = c->ix;
    const cs_entity_t *e = &ix->ents[ent];
    const cbm_gbuf_node_t *best = NULL;
    int bf = 0;
    int bl = 0;
    for (int d = 0; d < e->ndecls; d++) {
        int file = e->decls[d].file;
        if (c->prod && ix->files[file].is_test) {
            continue;
        }
        const cbm_gbuf_node_t *n = decl_node(ix, c->g, e->decls[d]);
        if (!n) {
            continue;
        }
        int order = e->decls[d].type;
        if (!best || decl_better(ix, file, order, bf, bl, c->f->rel_path)) {
            best = n;
            bf = file;
            bl = order;
        }
    }
    return best ? res_edge(best, exact) : res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
}

static bool tparam_listed(const char *list, const char *name, size_t nl) {
    for (const char *p = list; p && *p;) {
        const char *e = strchr(p, ',');
        size_t n = e ? (size_t)(e - p) : strlen(p);
        if (n == nl && memcmp(p, name, n) == 0) {
            return true;
        }
        p = e ? e + SKIP_ONE : NULL;
    }
    return false;
}

static bool looks_like_tvar(const char *s, size_t n) {
    /* `T`, `T1`: the prototype's fallback for undeclared type variables */
    return (n == 1 && isupper((unsigned char)s[0])) ||
           (n == PAIR_LEN && isupper((unsigned char)s[0]) && isdigit((unsigned char)s[1]));
}

static size_t suffix_start(const char *s, size_t n) {
    size_t e = n;
    while (e > 0 && (s[e - SKIP_ONE] == ']' || s[e - SKIP_ONE] == '[' || s[e - SKIP_ONE] == ',' ||
                     s[e - SKIP_ONE] == '*')) {
        e--;
    }
    return e;
}

/* The written parameter types match a declaration's normalized signature;
 * the declaring type's and the method's type variables accept any type. */
static bool sig_match(const cs_ref_t *r, const char *decl_sig, const char *tv_type,
                      const char *tv_method) {
    int nd = (decl_sig && decl_sig[0]) ? count_list(decl_sig, '|') : 0;
    if (nd != r->nparams) {
        return false;
    }
    const char *p = decl_sig;
    for (int i = 0; i < nd; i++) {
        const char *e = strchr(p, '|');
        size_t bn = e ? (size_t)(e - p) : strlen(p);
        const char *a = r->params[i];
        size_t an = strlen(a);
        bool ok = (an == SKIP_ONE && a[0] == '?') || (bn == SKIP_ONE && p[0] == '?') ||
                  (an == bn && memcmp(a, p, an) == 0);
        if (!ok) {
            size_t ab = suffix_start(a, an);
            size_t bb = suffix_start(p, bn);
            bool tv = tparam_listed(tv_type, p, bb) || tparam_listed(tv_method, p, bb) ||
                      looks_like_tvar(p, bb);
            ok = tv && (an - ab) == (bn - bb) && memcmp(a + ab, p + bb, an - ab) == 0;
        }
        if (!ok) {
            return false;
        }
        p = e ? e + SKIP_ONE : p + bn;
    }
    return true;
}

/* Type variables of the entity's declaration in `file`. */
static const char *decl_tparams(const cs_index_t *ix, int ent, int file) {
    const cs_entity_t *e = &ix->ents[ent];
    for (int d = 0; d < e->ndecls; d++) {
        if (e->decls[d].file == file) {
            return ix->files[file].types[e->decls[d].type].tparams;
        }
    }
    return "";
}

enum { CS_MAX_MREFS = 64 };

/* Bind the selected members' node: the representative declaration among the
 * ones with a node (never a test declaration for product code); a graph gap
 * when none has one. Overloads of one declaration share their node. */
static cs_res_t bind_members(const cs_ctx_t *c, const cs_mref_t *m, int n, bool exact) {
    const cbm_gbuf_node_t *best = NULL;
    int bf = 0;
    int bl = 0;
    for (int i = 0; i < n; i++) {
        if (c->prod && c->ix->files[m[i].file].is_test) {
            continue;
        }
        const cbm_gbuf_node_t *node = member_node(c->ix, c->g, m[i].file, m[i].m);
        if (node &&
            (!best || decl_better(c->ix, m[i].file, m[i].m->order, bf, bl, c->f->rel_path))) {
            best = node;
            bf = m[i].file;
            bl = m[i].m->order;
        }
    }
    return best ? res_edge(best, exact) : res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
}

/* Member `name` of entity `ent` (its own declarations): the kind rule, the
 * overload rule and signature selection. `marity` > 0 (a written type-
 * argument list) keeps the generic methods of that arity only. sig_mismatch
 * marks "no overload has the written signature" so the caller can continue in
 * the supertypes. */
static cs_res_t member_result(const cs_ctx_t *c, int ent, const char *name, const cs_ref_t *r,
                              bool has_params, int marity, bool exact, bool ctors) {
    cs_mref_t ms[CS_MAX_MREFS];
    int msel = member_sel(has_params || marity > 0, ctors);
    int n = members_of(c->ix, ent, name, msel, marity, ms, CS_MAX_MREFS);
    int ncall = 0;
    int nval = 0;
    for (int i = 0; i < n; i++) {
        ncall += ms[i].m->kind == 'c';
        nval += ms[i].m->kind != 'c';
    }
    if (n == 0) {
        return res_unres(CBM_DOCLINK_REASON_MISSING);
    }
    if (!has_params && ncall > 0 && nval > 0) {
        return res_unres(CBM_DOCLINK_REASON_AMBIGUOUS); /* property and method of one name */
    }
    if (ncall == 0) {
        return bind_members(c, ms, n, exact);
    }
    /* distinct overload signatures across the declarations */
    const char *sigs[CS_MAX_MREFS];
    int nsig = 0;
    for (int i = 0; i < n; i++) {
        if (ms[i].m->kind != 'c') {
            continue;
        }
        bool seen = false;
        for (int k = 0; k < nsig; k++) {
            seen = seen || strcmp(sigs[k], ms[i].m->sig ? ms[i].m->sig : "") == 0;
        }
        if (!seen) {
            sigs[nsig++] = ms[i].m->sig ? ms[i].m->sig : "";
        }
    }
    if (!has_params) {
        if (nsig > SKIP_ONE) {
            return res_unres(CBM_DOCLINK_REASON_AMBIGUOUS); /* an overload group */
        }
        return bind_members(c, ms, n, exact);
    }
    cs_mref_t sel[CS_MAX_MREFS];
    int ns = 0;
    for (int i = 0; i < n; i++) {
        if (ms[i].m->kind != 'c') {
            continue;
        }
        const char *tv_type = decl_tparams(c->ix, ent, ms[i].file);
        if (sig_match(r, ms[i].m->sig, tv_type, ms[i].m->tparams)) {
            sel[ns++] = ms[i];
        }
    }
    if (ns == 0) {
        cs_res_t res = res_unres(CBM_DOCLINK_REASON_MISSING);
        res.sig_mismatch = true;
        return res;
    }
    return bind_members(c, sel, ns, exact);
}

/* A member the entity's parsed declarations do not have. When a parse error
 * hides some of its members the missing one may be among them: that is a
 * graph gap, and nothing further away may take its place. */
static bool hidden_member(const cs_index_t *ix, int ent) {
    return ix->ents[ent].incomplete;
}

/* A written signature none of the type's own overloads has: cref binding
 * continues in the supertypes. */
static cs_res_t sig_fallback(const cs_ctx_t *c, int ent, const char *name, const cs_ref_t *r,
                             int marity, bool exact, cs_res_t first) {
    if (hidden_member(c->ix, ent)) {
        return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    int sup[CS_MAX_BFS];
    int n = super_bfs(c->ix, ent, sup, CS_MAX_BFS);
    for (int i = 0; i < n; i++) {
        if (has_members(c->ix, sup[i], name, CS_SEL_CALLABLES, marity)) {
            cs_res_t r2 = member_result(c, sup[i], name, r, true, marity, exact, false);
            if (!(r2.st == CS_UNRES && r2.reason == CBM_DOCLINK_REASON_MISSING)) {
                return r2;
            }
        }
        if (hidden_member(c->ix, sup[i])) {
            return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
        }
    }
    if (open_hierarchy(c->ix, ent)) {
        return res_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
    first.sig_mismatch = false;
    return first;
}

/* Implicit roots every type inherits from without naming them. */
static int implicit_roots(const cs_index_t *ix, char kind, int *out) {
    const char *roots[3];
    int n = 0;
    if (kind == 'e') {
        roots[n++] = "System.Enum";
    }
    if (kind == 's' || kind == 'r') {
        roots[n++] = "System.ValueType";
    }
    if (kind == 'c' || kind == 's' || kind == 'r' || kind == 'i') {
        roots[n++] = "System.Object";
    }
    int k = 0;
    for (int i = 0; i < n; i++) {
        int sel[CS_MAX_CANDS];
        int m = fqn_lookup(ix, roots[i], 0, sel, CS_MAX_CANDS);
        if (m == SKIP_ONE) {
            out[k++] = sel[0];
        }
    }
    return k;
}

/* `name` as a member (or nested type) declared by `ent` itself. `ctors`:
 * the type was named, so its constructors count. *found is false when the
 * entity declares nothing by that name. */
static cs_res_t own_member(const cs_ctx_t *c, int ent, const char *name, int marity,
                           const cs_ref_t *r, bool has_params, bool exact, bool ctors,
                           bool *found) {
    const cs_index_t *ix = c->ix;
    *found = true;
    if (has_members(ix, ent, name, member_sel(has_params || marity > 0, ctors), marity)) {
        cs_res_t res = member_result(c, ent, name, r, has_params, marity, exact, ctors);
        if (res.sig_mismatch) {
            return sig_fallback(c, ent, name, r, marity, exact, res);
        }
        return res;
    }
    int nested[CS_MAX_CANDS];
    int sel[CS_MAX_CANDS];
    int nn = nested_of(ix, ent, name, nested, CS_MAX_CANDS);
    int k = arity_filter(ix, nested, nn, marity, sel, CS_MAX_CANDS);
    if (k > 0 && !has_params) {
        return k > SKIP_ONE ? res_unres(CBM_DOCLINK_REASON_AMBIGUOUS)
                            : type_result(c, sel[0], exact);
    }
    *found = false;
    return res_unres(CBM_DOCLINK_REASON_MISSING);
}

static cs_res_t resolve_member_in(const cs_ctx_t *c, int ent, const char *name0, int marity,
                                  const cs_ref_t *r, bool has_params, bool exact) {
    const cs_index_t *ix = c->ix;
    const char *name = name0;
    if (strcmp(name, "#ctor") == 0 || strcmp(name, "#cctor") == 0) {
        name = ix->ents[ent].name;
    }
    int inherited_sel = member_sel(has_params || marity > 0, false);
    bool found = false;
    cs_res_t own = own_member(c, ent, name, marity, r, has_params, exact, true, &found);
    if (found) {
        return own;
    }
    if (hidden_member(ix, ent)) {
        return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    int sup[CS_MAX_BFS];
    int ns = super_bfs(ix, ent, sup, CS_MAX_BFS);
    for (int i = 0; i < ns; i++) {
        cs_res_t inh = own_member(c, sup[i], name, marity, r, has_params, exact, false, &found);
        if (found) {
            return inh;
        }
        if (hidden_member(ix, sup[i])) {
            return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
        }
    }
    int roots[3];
    int nr = implicit_roots(ix, ix->ents[ent].kind, roots);
    for (int i = 0; i < nr; i++) {
        int chain[CS_MAX_BFS];
        chain[0] = roots[i];
        int nc = SKIP_ONE + super_bfs(ix, roots[i], chain + SKIP_ONE, CS_MAX_BFS - SKIP_ONE);
        for (int j = 0; j < nc; j++) {
            if (has_members(ix, chain[j], name, inherited_sel, marity)) {
                cs_res_t res =
                    member_result(c, chain[j], name, r, has_params, marity, exact, false);
                if (res.sig_mismatch) {
                    return sig_fallback(c, ent, name, r, marity, exact, res);
                }
                return res;
            }
        }
    }
    /* accessor names: get_X / set_X / add_X / remove_X */
    static const char *const acc[] = {"get_", "set_", "add_", "remove_"};
    for (size_t a = 0; a < sizeof(acc) / sizeof(acc[0]); a++) {
        size_t al = strlen(acc[a]);
        if (strncmp(name, acc[a], al) == 0 && name[al] &&
            has_members(ix, ent, name + al, 0, CS_ARITY_NONE)) {
            return member_result(c, ent, name + al, r, false, CS_ARITY_NONE, exact, false);
        }
    }
    if (open_hierarchy(ix, ent)) {
        return res_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
    return res_unres(CBM_DOCLINK_REASON_MISSING);
}

/* ── Type paths ──────────────────────────────────────────────────── */

typedef enum { TP_OK = 0, TP_PARTIAL, TP_RES } cs_tp_kind_t;

typedef struct {
    cs_tp_kind_t st;
    int ent;        /* OK: the type; PARTIAL: the last type reached */
    int failed_seg; /* PARTIAL: index of the segment not found */
    bool exact;
    cs_res_t res; /* RES */
} cs_tp_t;

static cs_tp_t tp_res(cs_res_t r) {
    return (cs_tp_t){.st = TP_RES, .res = r};
}

static bool join_segs(const cs_seg_t *segs, int from, int to, const char *prefix, char *out,
                      size_t cap) {
    size_t w = 0;
    out[0] = '\0';
    if (prefix && prefix[0]) {
        int k = snprintf(out, cap, "%s", prefix);
        if (k < 0 || (size_t)k >= cap) {
            return false;
        }
        w = (size_t)k;
    }
    for (int i = from; i < to; i++) {
        int k = snprintf(out + w, cap - w, "%s%s", w ? "." : "", segs[i].name);
        if (k < 0 || (size_t)k >= cap - w) {
            return false;
        }
        w += (size_t)k;
    }
    return true;
}

/* Walk segs[from..to) as nested types of `cur` (own, then inherited). */
static cs_tp_t walk_nested(const cs_ctx_t *c, int cur, const cs_seg_t *segs, int from, int to,
                           bool exact) {
    const cs_index_t *ix = c->ix;
    for (int i = from; i < to; i++) {
        int nested[CS_MAX_CANDS];
        int sel[CS_MAX_CANDS];
        int nn = nested_of(ix, cur, segs[i].name, nested, CS_MAX_CANDS);
        int k = arity_filter(ix, nested, nn, segs[i].arity, sel, CS_MAX_CANDS);
        if (k == 0) {
            int sup[CS_MAX_BFS];
            int ns = super_bfs(ix, cur, sup, CS_MAX_BFS);
            for (int s = 0; s < ns && k == 0; s++) {
                nn = nested_of(ix, sup[s], segs[i].name, nested, CS_MAX_CANDS);
                k = arity_filter(ix, nested, nn, segs[i].arity, sel, CS_MAX_CANDS);
            }
        }
        if (k == 0) {
            return (cs_tp_t){.st = TP_PARTIAL, .ent = cur, .failed_seg = i, .exact = exact};
        }
        if (k > SKIP_ONE) {
            return tp_res(res_unres(CBM_DOCLINK_REASON_AMBIGUOUS));
        }
        cur = sel[0];
    }
    return (cs_tp_t){.st = TP_OK, .ent = cur, .exact = exact};
}

static bool tparam_in_scope(const cs_ctx_t *c, const char *name) {
    for (int i = 0; i < c->ntparams; i++) {
        if (tparam_listed(c->tparams[i], name, strlen(name))) {
            return true;
        }
    }
    return false;
}

static bool scope_open(const cs_ctx_t *c) {
    for (int i = 0; i < c->nusings; i++) {
        if (!cbm_ht_get(c->ix->namespaces, c->usings[i]) || external_prefix(c->usings[i])) {
            return true;
        }
    }
    return c->nstatics > 0; /* the prototype's rule: a static import keeps the scope open */
}

/* Why a type path was not found. */
static cs_res_t classify_unfound_type(const cs_ctx_t *c, const cs_seg_t *segs, int n) {
    const cs_index_t *ix = c->ix;
    char full[CS_KEY_BUF];
    if (!join_segs(segs, 0, n, NULL, full, sizeof(full))) {
        return res_unres(CBM_DOCLINK_REASON_UNPARSEABLE);
    }
    int arity = segs[n - SKIP_ONE].arity;
    if (cbm_ht_get(ix->namespaces, full) && arity <= 0) {
        cs_res_t ns = res_unres(CBM_DOCLINK_REASON_GRAPH_GAP); /* a namespace: no node */
        ns.is_namespace = true;
        return ns;
    }
    if (n > SKIP_ONE) {
        if (external_prefix(full)) {
            return res_unres(CBM_DOCLINK_REASON_EXTERNAL);
        }
        char ns[CS_KEY_BUF];
        if (join_segs(segs, 0, n - SKIP_ONE, NULL, ns, sizeof(ns)) &&
            cbm_ht_get(ix->namespaces, ns)) {
            return res_unres(CBM_DOCLINK_REASON_MISSING);
        }
        if (!cbm_ht_get(ix->namespaces, segs[0].name) &&
            !cbm_ht_get(ix->type_names, segs[0].name)) {
            return res_unres(CBM_DOCLINK_REASON_EXTERNAL);
        }
        return res_unres(CBM_DOCLINK_REASON_MISSING);
    }
    if (tparam_in_scope(c, segs[0].name)) {
        return (cs_res_t){.st = CS_LOCAL};
    }
    if (scope_open(c)) {
        return res_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
    for (int i = 0; i < c->nchain; i++) {
        if (open_hierarchy(ix, c->chain[i])) {
            return res_unres(CBM_DOCLINK_REASON_EXTERNAL);
        }
    }
    return res_unres(CBM_DOCLINK_REASON_MISSING);
}

/* Namespace ancestors of the context, innermost first, without the global
 * namespace. Returns the count; names are written into `out`. */
static int ns_ancestors(const cs_ctx_t *c, char out[][CS_KEY_BUF / CBM_SZ_4], int cap) {
    int n = 0;
    char ns[CS_KEY_BUF];
    snprintf(ns, sizeof(ns), "%s", c->ns ? c->ns : "");
    while (ns[0] && n < cap) {
        snprintf(out[n++], CS_KEY_BUF / CBM_SZ_4, "%s", ns);
        char *dot = strrchr(ns, '.');
        if (!dot) {
            break;
        }
        *dot = '\0';
    }
    return n;
}

enum { CS_MAX_NS_DEPTH = 16 };

static cs_tp_t resolve_type_path(const cs_ctx_t *c, const cs_seg_t *segs, int n) {
    const cs_index_t *ix = c->ix;
    if (n <= 0) {
        return tp_res(res_unres(CBM_DOCLINK_REASON_UNPARSEABLE));
    }
    char full[CS_KEY_BUF];
    if (!join_segs(segs, 0, n, NULL, full, sizeof(full))) {
        return tp_res(res_unres(CBM_DOCLINK_REASON_UNPARSEABLE));
    }
    int arity_last = segs[n - SKIP_ONE].arity;
    int sel[CS_MAX_CANDS];
    char anc[CS_MAX_NS_DEPTH][CS_KEY_BUF / CBM_SZ_4];
    int nanc = ns_ancestors(c, anc, CS_MAX_NS_DEPTH);
    /* A single segment is a simple name: never a fully-qualified shortcut. */
    if (n > SKIP_ONE || c->glob) {
        int k = fqn_lookup(ix, full, arity_last, sel, CS_MAX_CANDS);
        if (k > SKIP_ONE) {
            return tp_res(res_unres(CBM_DOCLINK_REASON_AMBIGUOUS));
        }
        if (k == SKIP_ONE) {
            return (cs_tp_t){.st = TP_OK, .ent = sel[0], .exact = true};
        }
    }
    if (n > SKIP_ONE && !c->glob) {
        for (int a = 0; a < nanc; a++) {
            char rel[CS_KEY_BUF];
            if (!join_segs(segs, 0, n, anc[a], rel, sizeof(rel))) {
                continue;
            }
            int k = fqn_lookup(ix, rel, arity_last, sel, CS_MAX_CANDS);
            if (k > SKIP_ONE) {
                return tp_res(res_unres(CBM_DOCLINK_REASON_AMBIGUOUS));
            }
            if (k == SKIP_ONE) {
                return (cs_tp_t){.st = TP_OK, .ent = sel[0], .exact = true};
            }
        }
    }
    if (n > SKIP_ONE) {
        /* the longest qualified type prefix (>= 2 segments unless global::),
         * namespace-relative innermost first, then absolute; the rest walks
         * nested types */
        for (int i = n - SKIP_ONE; i >= SKIP_ONE; i--) {
            if (i < PAIR_LEN && !c->glob) {
                continue;
            }
            for (int a = 0; a <= (c->glob ? 0 : nanc); a++) {
                bool absolute = c->glob || a == nanc;
                char pfx[CS_KEY_BUF];
                if (!join_segs(segs, 0, i, absolute ? NULL : anc[a], pfx, sizeof(pfx))) {
                    continue;
                }
                int k = fqn_lookup(ix, pfx, segs[i - SKIP_ONE].arity, sel, CS_MAX_CANDS);
                if (k != SKIP_ONE) {
                    continue;
                }
                return walk_nested(c, sel[0], segs, i, n, true);
            }
        }
    }
    if (c->glob) {
        return tp_res(classify_unfound_type(c, segs, n));
    }
    int head_ar = n > SKIP_ONE ? segs[0].arity : arity_last;
    cs_cands_t cands;
    bool exact = false;
    cs_lookup_t lr = lookup(c, segs[0].name, head_ar, CS_WANT_TYPE, 0, &cands, &exact);
    if (lr == CS_LOOKUP_FOUND) {
        if (cands.count > SKIP_ONE) {
            return tp_res(res_unres(CBM_DOCLINK_REASON_AMBIGUOUS));
        }
        const cs_cand_t *cd = &cands.items[0];
        if (cd->kind == 'G') {
            return tp_res(res_unres(CBM_DOCLINK_REASON_GRAPH_GAP));
        }
        if (cd->kind == 'X') {
            return tp_res(res_unres(CBM_DOCLINK_REASON_EXTERNAL));
        }
        if (cd->kind != 'T') {
            return tp_res(res_unres(CBM_DOCLINK_REASON_UNPARSEABLE));
        }
        return walk_nested(c, cd->ent, segs, SKIP_ONE, n, exact);
    }
    if (lr == CS_LOOKUP_INVISIBLE) {
        return tp_res(res_unres(CBM_DOCLINK_REASON_TEST_ONLY));
    }
    return tp_res(classify_unfound_type(c, segs, n));
}

/* ── Simple names in scope ───────────────────────────────────────── */

static cs_res_t resolve_in_scope(const cs_ctx_t *c, const cs_ref_t *r) {
    const cs_index_t *ix = c->ix;
    const cs_seg_t *seg = &r->segs[0];
    int arity = seg->arity;
    /* R3: a type-argument list selects the generic types and generic methods
     * of that arity; a constructor or another non-generic member named like
     * the type is no candidate (members_of filters them). */
    /* a constructor is named by a parameter list: `Foo` alone is the type */
    int sel = member_sel(r->has_params || arity > 0, r->has_params);
    cs_cands_t cands;
    bool exact = false;
    cs_lookup_t lr = lookup(c, seg->name, arity, CS_WANT_ANY, sel, &cands, &exact);
    if (lr == CS_LOOKUP_INVISIBLE) {
        return res_unres(CBM_DOCLINK_REASON_TEST_ONLY);
    }
    if (lr == CS_LOOKUP_NONE) {
        if (tparam_in_scope(c, seg->name)) {
            return (cs_res_t){.st = CS_LOCAL};
        }
        cs_res_t unfound = classify_unfound_type(c, r->segs, SKIP_ONE);
        /* An enclosing type with members a parse error hides: a name that
         * would be reported missing may be one of them, so it is a gap. What
         * the scope explains otherwise (an open scope or hierarchy: external)
         * keeps its reason. */
        for (int i = 0; unfound.st == CS_UNRES && unfound.reason == CBM_DOCLINK_REASON_MISSING &&
                        i < c->nchain;
             i++) {
            if (hidden_member(ix, c->chain[i])) {
                return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
            }
        }
        return unfound;
    }
    if (cands.count > SKIP_ONE && r->has_params) {
        /* a parameter list selects a member: a type stands for its constructors */
        cs_cands_t conv = {.count = 0};
        for (int i = 0; i < cands.count; i++) {
            const cs_cand_t *cd = &cands.items[i];
            if (cd->kind == 'T' && has_members(ix, cd->ent, ix->ents[cd->ent].name,
                                               member_sel(true, true), CS_ARITY_NONE)) {
                cands_push(&conv, 'M', cd->ent, ix->ents[cd->ent].name);
            } else {
                cands_push(&conv, cd->kind, cd->ent, cd->name);
            }
        }
        cands = conv;
    }
    if (cands.count > SKIP_ONE) {
        return res_unres(CBM_DOCLINK_REASON_AMBIGUOUS);
    }
    const cs_cand_t *cd = &cands.items[0];
    if (cd->kind == 'G') {
        return res_unres(CBM_DOCLINK_REASON_GRAPH_GAP);
    }
    if (cd->kind == 'X') {
        return res_unres(CBM_DOCLINK_REASON_EXTERNAL);
    }
    if (cd->kind == 'T') {
        if (r->has_params) {
            return resolve_member_in(c, cd->ent, ix->ents[cd->ent].name, CS_ARITY_NONE, r, true,
                                     exact);
        }
        return type_result(c, cd->ent, exact);
    }
    cs_res_t res =
        member_result(c, cd->ent, cd->name, r, r->has_params, arity, exact, r->has_params);
    if (res.sig_mismatch) {
        return sig_fallback(c, cd->ent, cd->name, r, arity, exact, res);
    }
    return res;
}

/* ── Doc IDs and the top level ───────────────────────────────────── */

static cs_res_t resolve_docid(const cs_ctx_t *c, const cs_ref_t *r) {
    const cs_index_t *ix = c->ix;
    char full[CS_KEY_BUF];
    if (!join_segs(r->segs, 0, r->nsegs, NULL, full, sizeof(full))) {
        return res_unres(CBM_DOCLINK_REASON_UNPARSEABLE);
    }
    int sel[CS_MAX_CANDS];
    if (r->docid == 'N') {
        return res_unres(cbm_ht_get(ix->namespaces, full) ? CBM_DOCLINK_REASON_GRAPH_GAP
                                                          : CBM_DOCLINK_REASON_EXTERNAL);
    }
    if (r->docid == 'T') {
        int a = r->segs[r->nsegs - SKIP_ONE].arity;
        int k = fqn_lookup(ix, full, a > 0 ? a : 0, sel, CS_MAX_CANDS);
        if (k == SKIP_ONE) {
            return type_result(c, sel[0], true);
        }
        if (k > SKIP_ONE) {
            return res_unres(CBM_DOCLINK_REASON_AMBIGUOUS);
        }
        return classify_unfound_type(c, r->segs, r->nsegs);
    }
    if (r->nsegs < PAIR_LEN) {
        return res_unres(CBM_DOCLINK_REASON_UNPARSEABLE);
    }
    char type[CS_KEY_BUF];
    if (!join_segs(r->segs, 0, r->nsegs - SKIP_ONE, NULL, type, sizeof(type))) {
        return res_unres(CBM_DOCLINK_REASON_UNPARSEABLE);
    }
    int ta = r->segs[r->nsegs - PAIR_LEN].arity;
    int k = fqn_lookup(ix, type, ta > 0 ? ta : 0, sel, CS_MAX_CANDS);
    if (k == 0) {
        return classify_unfound_type(c, r->segs, r->nsegs - SKIP_ONE);
    }
    const cs_seg_t *last = &r->segs[r->nsegs - SKIP_ONE];
    return resolve_member_in(c, sel[0], last->name, last->arity, r, r->has_params, true);
}

static int res_order(const cs_res_t *r) {
    if (r->st != CS_UNRES) {
        return CBM_SZ_8;
    }
    static const int order[CBM_DOCLINK_REASON_COUNT] = {
        [CBM_DOCLINK_REASON_AMBIGUOUS] = 0,   [CBM_DOCLINK_REASON_EXTERNAL] = 1,
        [CBM_DOCLINK_REASON_GRAPH_GAP] = 2,   [CBM_DOCLINK_REASON_TEST_ONLY] = 3,
        [CBM_DOCLINK_REASON_NOT_INDEXED] = 4, [CBM_DOCLINK_REASON_MISSING] = 5,
        [CBM_DOCLINK_REASON_UNPARSEABLE] = 6,
    };
    return order[r->reason];
}

static cs_res_t resolve_ref(const cs_ctx_t *c, const cs_ref_t *r) {
    if (r->docid) {
        return resolve_docid(c, r);
    }
    if (r->op) {
        /* operators, indexers and conversions have no nodes */
        if (r->nsegs > 0) {
            cs_tp_t tp = resolve_type_path(c, r->segs, r->nsegs);
            if (tp.st == TP_RES) {
                return tp.res;
            }
            return res_unres(tp.st == TP_OK ? CBM_DOCLINK_REASON_GRAPH_GAP
                                            : CBM_DOCLINK_REASON_MISSING);
        }
        return res_unres(c->nchain > 0 ? CBM_DOCLINK_REASON_GRAPH_GAP : CBM_DOCLINK_REASON_MISSING);
    }
    if (r->nsegs == SKIP_ONE) {
        return resolve_in_scope(c, r);
    }
    const cs_seg_t *last = &r->segs[r->nsegs - SKIP_ONE];
    bool have_first = false;
    cs_res_t first = res_unres(CBM_DOCLINK_REASON_MISSING);
    if (!r->has_params) {
        cs_tp_t tp = resolve_type_path(c, r->segs, r->nsegs);
        if (tp.st == TP_OK) {
            return type_result(c, tp.ent, tp.exact);
        }
        if (tp.st == TP_PARTIAL) {
            if (tp.failed_seg == r->nsegs - SKIP_ONE) {
                return resolve_member_in(c, tp.ent, last->name, last->arity, r, false, tp.exact);
            }
            return res_unres(CBM_DOCLINK_REASON_MISSING);
        }
        first = tp.res;
        have_first = true;
    }
    cs_tp_t tp2 = resolve_type_path(c, r->segs, r->nsegs - SKIP_ONE);
    if (tp2.st == TP_OK) {
        return resolve_member_in(c, tp2.ent, last->name, last->arity, r, r->has_params, tp2.exact);
    }
    if (tp2.st == TP_PARTIAL) {
        return res_unres(CBM_DOCLINK_REASON_MISSING);
    }
    cs_res_t second = tp2.res;
    if (second.st == CS_UNRES && second.is_namespace) {
        /* The qualifier is one of the repository's namespaces, not a type: the
         * reference names a type OF that namespace, and that reading is the
         * first one (the namespace declares no such type: missing, or the
         * BCL's by R4). Only a reference to the namespace itself is the gap. */
        return have_first ? first : classify_unfound_type(c, r->segs, r->nsegs);
    }
    if (have_first && res_order(&first) < res_order(&second)) {
        return first;
    }
    return second;
}

/* ── Context ─────────────────────────────────────────────────────── */

typedef struct {
    uint32_t start;
    uint32_t end;
    int type;
} cs_span_t;

static int span_inner_first(const void *a, const void *b) {
    const cs_span_t *x = (const cs_span_t *)a;
    const cs_span_t *y = (const cs_span_t *)b;
    if (x->start != y->start) {
        return x->start > y->start ? -1 : 1;
    }
    if (x->end != y->end) {
        return x->end < y->end ? -1 : 1;
    }
    return x->type > y->type ? -1 : (x->type < y->type);
}

/* Scope of a definition starting at `line` in `file`: its namespace region,
 * the types enclosing it (the documented type itself first), their and the
 * documented method's type parameters, the usings in scope. `skip_type`
 * (>= 0) leaves that declaration out of the chain (base-type resolution). */
static void ctx_init(cs_ctx_t *c, const cs_index_t *ix, const cbm_gbuf_t *g, int file,
                     uint32_t line, int skip_type) {
    memset(c, 0, sizeof(*c));
    c->ix = ix;
    c->g = g;
    c->file = file;
    c->f = &ix->files[file];
    const cs_file_t *f = c->f;
    int region = region_at(f, line);
    c->ns = f->regions[region].ns;
    c->prod = !f->is_test;
    c->unit = f->unit;
    c->inherit = true;
    cs_span_t spans[CS_MAX_CHAIN * CBM_SZ_4];
    int ns = 0;
    for (int t = 0; t < f->ntypes && ns < (int)(sizeof(spans) / sizeof(spans[0])); t++) {
        if (t == skip_type || f->types[t].start > line || f->types[t].end < line ||
            f->types[t].entity < 0) {
            continue;
        }
        spans[ns++] = (cs_span_t){.start = f->types[t].start, .end = f->types[t].end, .type = t};
    }
    qsort(spans, (size_t)ns, sizeof(spans[0]), span_inner_first);
    for (int i = 0; i < ns && c->nchain < CS_MAX_CHAIN; i++) {
        const cs_type_t *t = &f->types[spans[i].type];
        bool dup = false;
        for (int j = 0; j < c->nchain; j++) {
            dup = dup || c->chain[j] == t->entity;
        }
        if (dup) {
            continue;
        }
        c->chain[c->nchain++] = t->entity;
        c->tparams[c->ntparams++] = t->tparams;
    }
    /* the documented method's own type parameters */
    int lo = 0;
    int hi = f->nmembers;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (f->members[f->members_by_start[mid]].start < line) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (int i = lo; i < f->nmembers; i++) {
        const cs_member_t *m = &f->members[f->members_by_start[i]];
        if (m->start != line) {
            break;
        }
        if (m->kind == 'c' && m->tparams && m->tparams[0] && c->ntparams <= CS_MAX_CHAIN) {
            c->tparams[c->ntparams++] = m->tparams;
            break;
        }
    }
    ctx_collect_usings(c, region);
}

/* ── Base types ──────────────────────────────────────────────────── */

/* The type declared last with exactly `path` in `f` (its node owner), or -1. */
static int type_with_path(const cs_file_t *f, const char *path) {
    int lo = 0;
    int hi = f->ntypes;
    while (lo < hi) {
        int mid = lo + ((hi - lo) / PAIR_LEN);
        if (strcmp(f->types[f->types_by_path[mid]].path, path) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    int found = CBM_NOT_FOUND;
    for (int i = lo; i < f->ntypes && strcmp(f->types[f->types_by_path[i]].path, path) == 0; i++) {
        found = f->types_by_path[i];
    }
    return found;
}

/* Scope of a type DECLARATION for its base list, from structure alone (a
 * persisted scope has no lines): the declaring region, the enclosing types by
 * path, the usings in scope. */
static void ctx_init_decl(cs_ctx_t *c, const cs_index_t *ix, const cbm_gbuf_t *g, int file,
                          int type) {
    memset(c, 0, sizeof(*c));
    c->ix = ix;
    c->g = g;
    c->file = file;
    c->f = &ix->files[file];
    const cs_file_t *f = c->f;
    const cs_type_t *t = &f->types[type];
    int region = (t->region >= 0 && t->region < f->nregions) ? t->region : 0;
    c->ns = f->regions[region].ns;
    c->prod = false; /* a declared base is whatever the compiler bound */
    c->unit = f->unit;
    c->inherit = false;
    char path[CS_KEY_BUF];
    snprintf(path, sizeof(path), "%s", t->path);
    for (;;) {
        char *dot = strrchr(path, '.');
        if (!dot) {
            break;
        }
        *dot = '\0';
        int outer = type_with_path(f, path);
        if (outer >= 0 && f->types[outer].entity >= 0 && c->nchain < CS_MAX_CHAIN) {
            c->chain[c->nchain++] = f->types[outer].entity;
            c->tparams[c->ntparams++] = f->types[outer].tparams;
        }
    }
    ctx_collect_usings(c, region);
}

static void resolve_bases(cs_index_t *ix, const cbm_gbuf_t *g) {
    for (int ei = 0; ei < ix->nents; ei++) {
        cs_entity_t *e = &ix->ents[ei];
        int bases[CS_MAX_CANDS];
        int nb = 0;
        for (int d = 0; d < e->ndecls; d++) {
            const cs_file_t *f = &ix->files[e->decls[d].file];
            const cs_type_t *t = &f->types[e->decls[d].type];
            if (!t->bases || !t->bases[0]) {
                continue;
            }
            cs_ctx_t c;
            ctx_init_decl(&c, ix, g, e->decls[d].file, e->decls[d].type);
            for (const char *p = t->bases; p && *p;) {
                const char *bar = strchr(p, '|');
                size_t n = bar ? (size_t)(bar - p) : strlen(p);
                cs_seg_t segs[CS_MAX_SEGS];
                int nsegs = 0;
                bool parsed = parse_path(p, n, segs, &nsegs);
                bool bound = false;
                if (parsed) {
                    cs_tp_t tp = resolve_type_path(&c, segs, nsegs);
                    if (tp.st == TP_OK && tp.ent != ei) {
                        bool dup = false;
                        for (int k = 0; k < nb; k++) {
                            dup = dup || bases[k] == tp.ent;
                        }
                        if (!dup && nb < CS_MAX_CANDS) {
                            bases[nb++] = tp.ent;
                        }
                        bound = true;
                    }
                }
                if (!bound) {
                    e->open = true; /* the hierarchy continues outside the corpus */
                }
                p = bar ? bar + SKIP_ONE : NULL;
            }
        }
        if (nb > 0) {
            e->bases = (int *)ix_alloc(ix, (size_t)nb * sizeof(int));
            if (e->bases) {
                memcpy(e->bases, bases, (size_t)nb * sizeof(int));
                e->nbases = nb;
            }
        }
    }
}

/* ── Index lifecycle and the resolver hooks ──────────────────────── */

static int decl_order_cmp(const void *a, const void *b, const cs_index_t *ix) {
    const cs_decl_t *x = (const cs_decl_t *)a;
    const cs_decl_t *y = (const cs_decl_t *)b;
    int c = strcmp(ix->files[x->file].rel_path, ix->files[y->file].rel_path);
    if (c) {
        return c;
    }
    return x->type < y->type ? -1 : (x->type > y->type);
}

/* Insertion sort: decl lists are tiny (partial types). Files were added in
 * rel_path order already, so this only orders same-file declarations. */
static void sort_decls(const cs_index_t *ix, cs_entity_t *e) {
    for (int i = SKIP_ONE; i < e->ndecls; i++) {
        cs_decl_t v = e->decls[i];
        int j = i - SKIP_ONE;
        while (j >= 0 && decl_order_cmp(&e->decls[j], &v, ix) > 0) {
            e->decls[j + SKIP_ONE] = e->decls[j];
            j--;
        }
        e->decls[j + SKIP_ONE] = v;
    }
}

static void cs_destroy(void *index) {
    cs_index_t *ix = (cs_index_t *)index;
    if (!ix) {
        return;
    }
    cbm_ht_free(ix->ent_by_key);
    cbm_ht_free(ix->fqn_ents);
    cbm_ht_free(ix->ns_types);
    cbm_ht_free(ix->namespaces);
    cbm_ht_free(ix->type_names);
    cbm_ht_free(ix->quarantine);
    cbm_ht_free(ix->quarantine_test);
    cbm_ht_free(ix->unit_by_dir);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->ents);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->run_to_file);
    cbm_arena_destroy(&ix->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, ix);
}

static void *cs_build(const cbm_doclink_build_in_t *in) {
    cs_index_t *ix = (cs_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ix));
    if (!ix) {
        return NULL;
    }
    cbm_arena_init(&ix->arena);
    ix->project = in->ctx->project_name;
    ix->repo_path = in->ctx->repo_path;
    ix->ent_by_key = cbm_ht_create(CBM_SZ_4K);
    ix->fqn_ents = cbm_ht_create(CBM_SZ_4K);
    ix->ns_types = cbm_ht_create(CBM_SZ_4K);
    ix->namespaces = cbm_ht_create(CBM_SZ_1K);
    ix->type_names = cbm_ht_create(CBM_SZ_4K);
    ix->quarantine = cbm_ht_create(CBM_SZ_64);
    ix->quarantine_test = cbm_ht_create(CBM_SZ_64);
    ix->unit_by_dir = cbm_ht_create(CBM_SZ_256);
    CBMHashTable *dir_unit = cbm_ht_create(CBM_SZ_1K);
    ix->nfiles = in->file_count;
    ix->files = (cs_file_t *)ix_alloc(ix, (size_t)(in->file_count ? in->file_count : 1) *
                                              sizeof(cs_file_t));
    ix->run_count = in->run_file_count;
    ix->run_to_file = (int *)cbm_alloc(
        CBM_MEM_CLASS_OTHER, (size_t)(in->run_file_count ? in->run_file_count : 1) * sizeof(int));
    if (!ix->ent_by_key || !ix->fqn_ents || !ix->ns_types || !ix->namespaces || !ix->type_names ||
        !ix->quarantine || !ix->quarantine_test || !ix->unit_by_dir || !dir_unit || !ix->files ||
        !ix->run_to_file) {
        cbm_log_error("doc_links.cs.error", "step", "tables", "reason", "alloc");
        cbm_ht_free(dir_unit);
        cs_destroy(ix);
        return NULL;
    }
    for (int i = 0; i < in->run_file_count; i++) {
        ix->run_to_file[i] = CBM_NOT_FOUND;
    }
    int bad_scopes = 0;
    for (int i = 0; i < in->file_count; i++) {
        const cbm_doclink_file_t *src = &in->files[i];
        cs_file_t *f = &ix->files[i];
        memset(f, 0, sizeof(*f));
        f->rel_path = ix_strdup(ix, src->rel_path);
        f->module_qn =
            cbm_fqn_module_source_lang(&ix->arena, ix->project, src->rel_path, CBM_LANG_CSHARP);
        f->is_test = cs_is_test_path(src->rel_path);
        if (!src->scope || !parse_scope(ix, f, src->scope)) {
            bad_scopes += src->scope != NULL;
            /* no scope: an empty file (only the root region) */
            f->regions = (cs_region_t *)ix_alloc(ix, sizeof(cs_region_t));
            if (!f->regions) {
                cbm_log_error("doc_links.cs.error", "step", "files", "reason", "alloc");
                cbm_ht_free(dir_unit);
                cs_destroy(ix);
                return NULL;
            }
            f->regions[0] = (cs_region_t){.parent = CBM_NOT_FOUND, .end = UINT32_MAX, .ns = ""};
            f->nregions = SKIP_ONE;
            f->nusings = f->ntypes = f->nmembers = f->nunplaced = 0;
        }
        f->unit = unit_of(ix, dir_unit, src->rel_path);
        if (src->run_file >= 0 && src->run_file < in->run_file_count) {
            ix->run_to_file[src->run_file] = i;
        }
    }
    cbm_ht_free(dir_unit);
    if (ix->oom) {
        cbm_log_error("doc_links.cs.error", "step", "scopes", "reason", "alloc");
        cs_destroy(ix);
        return NULL;
    }
    if (bad_scopes > 0) {
        char b[CBM_SZ_32];
        snprintf(b, sizeof(b), "%d", bad_scopes);
        cbm_log_warn("doc_links.cs.bad_scope", "files", b);
    }
    units_collect_usings(ix);
    int skipped = 0;
    if (!build_entities(ix, &skipped) || ix->oom) {
        cbm_log_error("doc_links.cs.error", "step", "entities", "reason", "alloc");
        cs_destroy(ix);
        return NULL;
    }
    for (int i = 0; i < ix->nents; i++) {
        sort_decls(ix, &ix->ents[i]);
    }
    resolve_bases(ix, in->graph);
    if (ix->oom) {
        cbm_log_error("doc_links.cs.error", "step", "bases", "reason", "alloc");
        cs_destroy(ix);
        return NULL;
    }
    int incomplete = 0;
    for (int i = 0; i < ix->nents; i++) {
        incomplete += ix->ents[i].incomplete;
    }
    char b[6][CBM_SZ_32];
    snprintf(b[0], sizeof(b[0]), "%d", ix->nfiles);
    snprintf(b[1], sizeof(b[1]), "%d", ix->nents);
    snprintf(b[2], sizeof(b[2]), "%d", ix->nunits);
    snprintf(b[3], sizeof(b[3]), "%d", skipped);
    snprintf(b[4], sizeof(b[4]), "%d", incomplete);
    snprintf(b[5], sizeof(b[5]), "%u",
             (unsigned)(cbm_ht_count(ix->quarantine) + cbm_ht_count(ix->quarantine_test)));
    cbm_log_info("doc_links.cs.index", "files", b[0], "types", b[1], "projects", b[2],
                 "skipped_types", b[3], "incomplete_types", b[4], "quarantined_names", b[5]);
    return ix;
}

/* R4: a keyword alias (int, string ...) or a qualified System.* name that the
 * corpus does not declare is the BCL's, not missing. (An unknown qualified
 * head that is neither a corpus namespace nor a corpus type is already
 * external through classify_unfound_type.) */
static int r4_reason(const cs_ref_t *r, bool parsed, const char *raw, int reason) {
    if (reason != CBM_DOCLINK_REASON_MISSING && reason != CBM_DOCLINK_REASON_UNPARSEABLE) {
        return reason;
    }
    if (parsed && r->keyword_rewrite) {
        return CBM_DOCLINK_REASON_EXTERNAL;
    }
    if (!parsed) {
        /* the head of the raw text before any parameter / type-argument list */
        char head[CBM_SZ_256];
        const char *s = raw;
        if (s[0] && s[1] == ':') {
            s += PAIR_LEN;
        }
        size_t n = strcspn(s, "({<.");
        snprintf(head, sizeof(head), "%.*s", (int)n, s);
        return keyword_type(head) ? CBM_DOCLINK_REASON_EXTERNAL : reason;
    }
    if (reason == CBM_DOCLINK_REASON_MISSING && r->nsegs > SKIP_ONE &&
        strcmp(r->segs[0].name, "System") == 0) {
        return CBM_DOCLINK_REASON_EXTERNAL;
    }
    return reason;
}

static void cs_resolve(const void *index, int run_file, const CBMDocLink *link,
                       const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    const cs_index_t *ix = (const cs_index_t *)index;
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = CBM_DOCLINK_REASON_MISSING;
    out->target = NULL;
    out->exact = false;
    if (!ix || run_file < 0 || run_file >= ix->run_count || ix->run_to_file[run_file] < 0) {
        return;
    }
    cs_ref_t r;
    if (!parse_cref(link->raw, &r)) {
        out->reason =
            r4_reason(&r, false, link->raw ? link->raw : "", CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    if (!r.docid && r.nsegs > 0 && r.nsegs < CS_MAX_SEGS) {
        const char *bcl = keyword_type(r.segs[0].name);
        if (bcl) {
            memmove(&r.segs[1], &r.segs[0], (size_t)r.nsegs * sizeof(cs_seg_t));
            snprintf(r.segs[0].name, sizeof(r.segs[0].name), "System");
            r.segs[0].arity = CS_ARITY_NONE;
            snprintf(r.segs[1].name, sizeof(r.segs[1].name), "%s", bcl);
            r.nsegs++;
            r.keyword_rewrite = true;
        }
    }
    /* What could not be placed is not resolved around: a definition in the
     * part of its file where the braces stop pairing has no known scope, and
     * a name some file declares without a known namespace could be that
     * declaration. Both are declared-but-unplaced, i.e. graph gaps. */
    const cs_file_t *f = &ix->files[ix->run_to_file[run_file]];
    bool unplaced = false;
    for (int i = 0; !unplaced && i < f->nunplaced; i++) {
        unplaced = link->def_line >= f->unplaced[i].from && link->def_line <= f->unplaced[i].to;
    }
    for (int i = 0; !unplaced && i < r.nsegs; i++) {
        unplaced = cbm_ht_get(ix->quarantine, r.segs[i].name) != NULL ||
                   (f->is_test && cbm_ht_get(ix->quarantine_test, r.segs[i].name) != NULL);
    }
    if (unplaced) {
        out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
        return;
    }
    cs_ctx_t c;
    ctx_init(&c, ix, graph, ix->run_to_file[run_file], link->def_line, CBM_NOT_FOUND);
    c.glob = r.glob;
    cs_res_t res = resolve_ref(&c, &r);
    if (res.st == CS_LOCAL) {
        out->kind = CBM_DOCLINK_LOCAL;
        return;
    }
    if (res.st == CS_OK && res.node) {
        out->kind = CBM_DOCLINK_EDGE;
        out->target = res.node;
        out->exact = res.exact;
        return;
    }
    out->reason = r4_reason(&r, true, link->raw, res.reason);
}

/* ── Incremental scope rules ─────────────────────────────────────── */

static bool cs_ci_suffix(const char *s, const char *sfx) {
    size_t n = strlen(s);
    size_t sl = strlen(sfx);
    if (n < sl) {
        return false;
    }
    for (size_t i = 0; i < sl; i++) {
        if (tolower((unsigned char)s[n - sl + i]) != sfx[i]) {
            return false;
        }
    }
    return true;
}

/* MSBuild project files set the global usings of every C# file of their
 * project (R1), so a change to one is never repairable file by file. */
static bool cs_scope_input(const char *rel_path) {
    return cs_ci_suffix(rel_path, ".csproj") || cs_ci_suffix(rel_path, ".props") ||
           cs_ci_suffix(rel_path, ".targets");
}

/* Scope line fields (0-based, the tag is field 0; internal/cbm/doclink_cs.c):
 * `U region kind alias target` and `M start kind explicit path tparams sig`. */
enum { CS_SCOPE_U_KIND = 2, CS_SCOPE_M_PATH = 4 };

static const char *delta_field(const char *line, size_t len, int idx, size_t *flen) {
    int f = 0;
    size_t s = 0;
    for (size_t i = 0; i <= len; i++) {
        if (i == len || line[i] == '\t') {
            if (f == idx) {
                *flen = i - s;
                return line + s;
            }
            f++;
            s = i + SKIP_ONE;
        }
    }
    *flen = 0;
    return NULL;
}

/* A using directive only its own file sees: kind n (namespace), s (static) or
 * a (alias). A `global using` namespace (g) is in scope in every file of the
 * project. */
static bool delta_local_using(const char *line, size_t len) {
    if (line[0] != 'U') {
        return false;
    }
    size_t klen = 0;
    const char *kind = delta_field(line, len, CS_SCOPE_U_KIND, &klen);
    return kind && klen == SKIP_ONE && kind[0] != 'g';
}

/* The next scope line that concerns other files (the file's own usings are
 * skipped); false at the end. */
static bool delta_next_line(const char **cursor, const char **line, size_t *len) {
    for (const char *p = *cursor; p && *p;) {
        const char *nl = strchr(p, '\n');
        size_t n = nl ? (size_t)(nl - p) : strlen(p);
        const char *next = nl ? nl + SKIP_ONE : p + n;
        if (n > 0 && !delta_local_using(p, n)) {
            *line = p;
            *len = n;
            *cursor = next;
            return true;
        }
        p = next;
    }
    *cursor = NULL;
    return false;
}

/* Compare a changed file's stored and fresh scopes line by line, in order.
 * Two differences leave every other file's resolution alone:
 *   - the file's own using directives (namespace, static, alias): they scope
 *     the file itself, and it is re-extracted anyway;
 *   - a member the fresh scope no longer has. Every member line is a method,
 *     constructor, property, field, event or enum member, so a reference that
 *     depended on it either bound it (an edge into this file) or names it in
 *     its unresolved row: the name is reported.
 * Everything else is GLOBAL: a new or changed line (a type, a member, a
 * signature, a namespace, a global using), a removed type (it may be another
 * type's base or an alias target, which changes how references THROUGH those
 * classify), a removed namespace, global using, quarantined name or unplaced
 * range, and a changed order (same-path declarations own their node by
 * order). */
static int cs_scope_delta(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                          void *ud) {
    const char *sp = stored;
    const char *fp = fresh;
    const char *sl = NULL;
    const char *fl = NULL;
    size_t slen = 0;
    size_t flen = 0;
    bool hs = delta_next_line(&sp, &sl, &slen);
    bool hf = delta_next_line(&fp, &fl, &flen);
    while (hs || hf) {
        if (hs && hf && slen == flen && memcmp(sl, fl, slen) == 0) {
            hs = delta_next_line(&sp, &sl, &slen);
            hf = delta_next_line(&fp, &fl, &flen);
            continue;
        }
        if (!hs || sl[0] != 'M') {
            return CBM_DOCLINK_DELTA_GLOBAL;
        }
        size_t plen = 0;
        const char *path = delta_field(sl, slen, CS_SCOPE_M_PATH, &plen);
        if (!path || plen == 0) {
            return CBM_NOT_FOUND; /* not a member line this code wrote */
        }
        size_t s = plen;
        while (s > 0 && path[s - SKIP_ONE] != '.') {
            s--;
        }
        if (!removed || !removed(ud, path + s, plen - s)) {
            return CBM_NOT_FOUND;
        }
        hs = delta_next_line(&sp, &sl, &slen);
    }
    return CBM_DOCLINK_DELTA_LOCAL;
}

const cbm_doclink_resolver_t cbm_doclink_cs_resolver = {
    .langs = {CBM_LANG_CSHARP},
    .lang_count = 1,
    .scope_tag = CBM_DOCLINK_CS_SCOPE_TAG,
    .build = cs_build,
    .destroy = cs_destroy,
    .resolve = cs_resolve,
    .scope_input = cs_scope_input,
    .scope_delta = cs_scope_delta,
};
