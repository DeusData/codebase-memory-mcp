/*
 * pass_shell.c — the Bash (+ Make, CI) infrastructure layer (PHASE 3 P3.2, P3.5).
 *
 * A shell script or a Make recipe that puts manifests, a kustomization or a
 * chart on a cluster DEPLOYS them: an edge from the function running the
 * command (a bash function, a Make rule; top-level code: the Module) to the
 * node of what it deploys -- existing nodes only:
 *   kubectl apply|create|replace -f F     a File holding Kubernetes objects:
 *                                         F itself, F's *.yaml|*.yml|*.json
 *                                         (-R: below it), or a glob's matches
 *   kubectl ... -k D                      the Kustomization of D
 *   kustomize build D | ... | kubectl apply -f -   (also kubectl kustomize D)
 *   helm install|upgrade [NAME] CHART     the Chart of a chart dir, else the
 *                                         external Chart of that name
 *                                         (repo/name, oci://.../name)
 *   helm template [NAME] CHART | ... | kubectl apply -f -
 *   find D -name G -exec kubectl apply -f {} \;    D's manifests matching G
 *
 * A program started by a path (./_build/kubermatic-installer, ${DIR}/x,
 * $(LOCALBIN)/controller-gen) SPAWNS the Process of its name, as
 * pass_spawns.c draws a call that starts a program. A program that is a file
 * of the repository -- the path names one, or a shell runs one
 * (bash|sh|zsh|ksh|dash [-opts] FILE) -- SPAWNS that script instead: its
 * Module (the source of its own top-level edges), else its File (P3.5).
 *
 * CI (P3.5): the command lines GitHub Actions steps and Prow job containers
 * run (pass_ci.c reads them) evaluate the same way, from the step's
 * directory with its env literals; their edges carry the CI and the job.
 *
 * Words are evaluated by constant propagation over the script, nothing is
 * executed: assignments (local / export / readonly / declare, arrays and
 * "${a[@]}"), ${X:-default}, $0 / BASH_SOURCE, $(dirname ..), $(basename ..),
 * $(cd .. && pwd), $(realpath ..), $(pwd), $(git rev-parse --show-toplevel).
 * A word with any other part is unknown, and an unknown target is no edge.
 * A value set in a branch, a loop or the right side of && / || is unknown
 * after it; a subshell's or pipeline element's changes do not leak; a
 * function body is read where it is defined, and the globals it sets are
 * unknown after it. cd / pushd / popd track the working directory. A
 * relative path resolves against it; with none tracked (a script runs from
 * wherever it is started), against the repository root and the script's
 * directory, linked only when exactly one of the two names a target.
 *
 * Make: the Makefile's variables expand into each recipe line (recursively,
 * with the last assignment; $(CURDIR) / $(shell pwd) are the Makefile's
 * directory, where its recipes run), which then evaluates as a shell line of
 * its own. A variable assigned under a conditional is unknown; an included
 * .mk file runs in its includer's directory, so it tracks none.
 */
#include "foundation/constants.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/hash_table.h"
#include "foundation/limits.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/str_util.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "cbm.h"
#include "lang_specs.h"

#include <ctype.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Candidate paths a target lookup scanned (complexity guard counter). */
_Atomic uint64_t g_sh_candidates = 0;

enum {
    SH_GROW_MIN = 16,
    SH_DEC = 10,
    SH_NO_DEPTH = -1,
};

/* A repository-anchored absolute path value: "\x01hack/ci" is <repo>/hack/ci. */
#define SH_ANCHOR '\x01'
static const char SH_ROOT[] = "\x01";

static bool sh_grow(void **items, int *cap, int count, size_t elem) {
    if (count < *cap) {
        return true;
    }
    int ncap = *cap ? *cap * PAIR_LEN : SH_GROW_MIN;
    void *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, *items, (size_t)ncap * elem);
    if (!grown) {
        return false;
    }
    *items = grown;
    *cap = ncap;
    return true;
}

static const char *sh_basename(const char *p) {
    const char *s = strrchr(p, '/');
    return s ? s + SKIP_ONE : p;
}

static bool sh_eq(const char *a, const char *b) {
    return a && b && strcmp(a, b) == 0;
}

static bool sh_starts(const char *s, const char *prefix) {
    return strncmp(s, prefix, strlen(prefix)) == 0;
}

/* ── The graph's deploy targets ────────────────────────────────────── */

typedef struct {
    const char **items; /* graph-owned paths */
    int count;
    int cap;
} sh_paths_t;

static void sh_paths_push(sh_paths_t *v, const char *p) {
    if (sh_grow((void **)&v->items, &v->cap, v->count, sizeof(*v->items))) {
        v->items[v->count++] = p;
    }
}

static int sh_cmp_str(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

static void sh_paths_sort_unique(sh_paths_t *v) {
    if (v->count == 0) {
        return;
    }
    qsort((void *)v->items, (size_t)v->count, sizeof(*v->items), sh_cmp_str);
    int w = SKIP_ONE;
    for (int i = SKIP_ONE; i < v->count; i++) {
        if (strcmp(v->items[i], v->items[w - SKIP_ONE]) != 0) {
            v->items[w++] = v->items[i];
        }
    }
    v->count = w;
}

/* First index whose path is >= key. */
static int sh_lower_bound(const sh_paths_t *v, const char *key) {
    int lo = 0;
    int hi = v->count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / PAIR_LEN;
        if (strcmp(v->items[mid], key) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    return lo;
}

static bool sh_paths_has(const sh_paths_t *v, const char *p) {
    int i = sh_lower_bound(v, p);
    return i < v->count && strcmp(v->items[i], p) == 0;
}

typedef struct {
    cbm_pipeline_ctx_t *ctx;
    sh_paths_t files;       /* every File node's path */
    sh_paths_t manifests;   /* files holding Kubernetes objects (.yaml/.yml/.json) */
    CBMHashTable *file_ids; /* path -> File node id */
    CBMHashTable *kusts;    /* kustomization path -> its Kustomization Resource id */
    CBMHashTable *charts;   /* Chart.yaml path -> Chart node id */
} sh_index_t;

static bool sh_manifest_ext(const char *path) {
    const char *dot = strrchr(sh_basename(path), '.');
    return dot &&
           (strcmp(dot, ".yaml") == 0 || strcmp(dot, ".yml") == 0 || strcmp(dot, ".json") == 0);
}

static bool sh_kustomize_base(const char *base) {
    return cbm_is_kustomize_file(base) || strcmp(base, "Kustomization") == 0;
}

static bool sh_index_build(sh_index_t *ix, cbm_pipeline_ctx_t *ctx) {
    ix->ctx = ctx;
    ix->file_ids = cbm_ht_create(0);
    ix->kusts = cbm_ht_create(0);
    ix->charts = cbm_ht_create(0);
    if (!ix->file_ids || !ix->kusts || !ix->charts) {
        return false;
    }
    const cbm_gbuf_node_t **nodes = NULL;
    int n = 0;
    if (cbm_gbuf_find_by_label(ctx->gbuf, "File", &nodes, &n) == 0) {
        for (int i = 0; i < n; i++) {
            if (nodes[i]->file_path) {
                sh_paths_push(&ix->files, nodes[i]->file_path);
                cbm_ht_set(ix->file_ids, nodes[i]->file_path, (void *)(intptr_t)nodes[i]->id);
            }
        }
    }
    if (cbm_gbuf_find_by_label(ctx->gbuf, "Resource", &nodes, &n) == 0) {
        for (int i = 0; i < n; i++) {
            const char *fp = nodes[i]->file_path;
            if (!fp) {
                continue;
            }
            bool kust_file = sh_kustomize_base(sh_basename(fp));
            if (kust_file && sh_eq(nodes[i]->name, "Kustomization")) {
                cbm_ht_set(ix->kusts, fp, (void *)(intptr_t)nodes[i]->id);
            } else if (!kust_file && sh_manifest_ext(fp)) {
                sh_paths_push(&ix->manifests, fp);
            }
        }
    }
    if (cbm_gbuf_find_by_label(ctx->gbuf, "Chart", &nodes, &n) == 0) {
        for (int i = 0; i < n; i++) {
            const char *fp = nodes[i]->file_path;
            const char *base = fp ? sh_basename(fp) : "";
            const char *qn = nodes[i]->qualified_name;
            if ((strcmp(base, "Chart.yaml") == 0 || strcmp(base, "Chart.yml") == 0) && qn &&
                !cbm_pipeline_chart_is_external(nodes[i])) {
                cbm_ht_set(ix->charts, fp, (void *)(intptr_t)nodes[i]->id);
            }
        }
    }
    sh_paths_sort_unique(&ix->files);
    sh_paths_sort_unique(&ix->manifests);
    return true;
}

static void sh_index_free(sh_index_t *ix) {
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)ix->files.items);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)ix->manifests.items);
    if (ix->file_ids) {
        cbm_ht_free(ix->file_ids);
    }
    if (ix->kusts) {
        cbm_ht_free(ix->kusts);
    }
    if (ix->charts) {
        cbm_ht_free(ix->charts);
    }
}

/* The paths of `v` below directory `dir` ("" = the root): [*lo, *hi). */
static void sh_range(const sh_paths_t *v, const char *dir, char *key, size_t key_sz, int *lo,
                     int *hi) {
    if (!dir[0]) {
        key[0] = '\0';
        *lo = 0;
        *hi = v->count;
        return;
    }
    snprintf(key, key_sz, "%s/", dir);
    size_t kl = strlen(key);
    *lo = sh_lower_bound(v, key);
    int h = *lo;
    while (h < v->count && strncmp(v->items[h], key, kl) == 0) {
        h++;
    }
    *hi = h;
}

static bool sh_dir_exists(const sh_index_t *ix, const char *dir) {
    if (!dir[0]) {
        return true;
    }
    char key[CBM_SZ_4K];
    int lo = 0;
    int hi = 0;
    sh_range(&ix->files, dir, key, sizeof(key), &lo, &hi);
    return hi > lo;
}

/* ── Path values ───────────────────────────────────────────────────── */

/* `rel` (a relative path: ./ and ../ allowed) inside repository directory
 * `base` ("" = the root), normalized; NULL when it leaves the repository. */
static char *sh_join(CBMArena *a, const char *base, const char *rel) {
    size_t bl = strlen(base);
    char *out = cbm_arena_alloc(a, bl + strlen(rel) + PAIR_LEN);
    if (!out) {
        return NULL;
    }
    memcpy(out, base, bl);
    size_t n = bl;
    out[n] = '\0';
    for (const char *p = rel; *p;) {
        while (*p == '/') {
            p++;
        }
        size_t sl = strcspn(p, "/");
        if (sl == 0) {
            break;
        }
        if (sl == SKIP_ONE && p[0] == '.') {
            p += sl;
            continue;
        }
        if (sl == PAIR_LEN && p[0] == '.' && p[SKIP_ONE] == '.') {
            if (n == 0) {
                return NULL;
            }
            while (n > 0 && out[n - SKIP_ONE] != '/') {
                n--;
            }
            if (n > 0) {
                n--;
            }
            out[n] = '\0';
            p += sl;
            continue;
        }
        if (n) {
            out[n++] = '/';
        }
        memcpy(out + n, p, sl);
        n += sl;
        out[n] = '\0';
        p += sl;
    }
    return out;
}

static const char *sh_anchor(CBMArena *a, const char *repo_path) {
    return repo_path ? cbm_arena_sprintf(a, "%c%s", SH_ANCHOR, repo_path) : NULL;
}

/* dirname(1), on a value: textual, keeps an anchored path anchored. */
static const char *sh_dirname(CBMArena *a, const char *v) {
    bool anch = v[0] == SH_ANCHOR;
    const char *p = anch ? v + SKIP_ONE : v;
    size_t len = strlen(p);
    while (len > SKIP_ONE && p[len - SKIP_ONE] == '/') {
        len--;
    }
    size_t k = len;
    while (k > 0 && p[k - SKIP_ONE] != '/') {
        k--;
    }
    if (k == 0) {
        return anch ? SH_ROOT : ".";
    }
    size_t dl = k - SKIP_ONE;
    while (dl > 0 && p[dl - SKIP_ONE] == '/') {
        dl--;
    }
    if (dl == 0) {
        return anch ? SH_ROOT : "/";
    }
    return anch ? cbm_arena_sprintf(a, "%c%.*s", SH_ANCHOR, (int)dl, p)
                : cbm_arena_strndup(a, p, dl);
}

/* basename(1), on a value. */
static const char *sh_value_basename(CBMArena *a, const char *v) {
    const char *p = v[0] == SH_ANCHOR ? v + SKIP_ONE : v;
    size_t len = strlen(p);
    while (len > SKIP_ONE && p[len - SKIP_ONE] == '/') {
        len--;
    }
    size_t k = len;
    while (k > 0 && p[k - SKIP_ONE] != '/') {
        k--;
    }
    return cbm_arena_strndup(a, p + k, len - k);
}

/* ── Hits: what a command deploys ──────────────────────────────────── */

typedef struct {
    const char *file; /* the script (caller-owned path) */
    int line;
    const char *via;  /* static */
    int64_t tgt;      /* the target node; 0: the external chart `ext` */
    const char *ext;  /* pass-arena */
    const char *path; /* pass-arena: the deployed path or chart; a spawned script */
    const char *ci;   /* static: the CI running the command; NULL: none */
    const char *job;  /* pass-arena: its job; NULL: none */
    bool chart;
    bool spawn;             /* SPAWNS the script `tgt` / the Process `ext`, not DEPLOYS */
    bool make;              /* SPAWNS the rule of target `ext` that makefile `path` reads */
    bool build;             /* the image `ext` is built from the Dockerfile `path` */
    bool http;              /* HTTP_CALLS the URL `ext` (scheme://host/path) with method `path` */
    const char *build_args; /* pass-arena, build: --build-arg as a JSON object; NULL: none */
    const char *default_of; /* pass-arena, build: JSON array of the ${X:-d} defaults naming `ext` */
} sh_hit_t;

typedef struct {
    sh_hit_t *items;
    int count;
    int cap;
    CBMArena arena;
    CBMHashTable *declarers; /* files running a container build: path -> path */
} sh_hits_t;

typedef struct {
    int64_t id;
    const char *path; /* graph-owned or file-arena */
} sh_target_t;

typedef struct {
    sh_target_t *items;
    int count;
    int cap;
} sh_targets_t;

static void sh_targets_push(sh_targets_t *v, int64_t id, const char *path) {
    if (id > 0 && sh_grow((void **)&v->items, &v->cap, v->count, sizeof(*v->items))) {
        v->items[v->count++] = (sh_target_t){id, path};
    }
}

/* ── Shell state ───────────────────────────────────────────────────── */

typedef struct {
    const char *s;      /* scalar value; NULL: unknown */
    const char **items; /* array items (an item NULL: unknown); NULL: no array */
    int n;
    const char *dflt_of; /* JSON array: the variables whose ${X:-d} default made it */
} sh_val_t;

static const sh_val_t SH_UNKNOWN_VAL = {NULL, NULL, 0, NULL};

typedef enum { SH_FREE, SH_AT, SH_LOST } sh_cwd_mode_t;

typedef struct {
    sh_cwd_mode_t mode;
    const char *dir; /* SH_AT: repository path ("" = the root) */
} sh_cwd_t;

typedef struct {
    const char *name;
    const sh_val_t *prev; /* NULL: was unset */
    bool local;
} sh_undo_t;

typedef struct {
    int undo;
    sh_cwd_t cwd;
    int ndirs;
} sh_mark_t;

typedef struct {
    const char **names;
    int n;
    int cap;
    bool cwd;
} sh_touch_t;

typedef struct {
    sh_index_t *ix;
    sh_hits_t *hits;
    CBMArena *a;        /* this file's values */
    const char *src;    /* the parsed text */
    const char *file;   /* the script / Makefile path */
    const char *script; /* $0: anchored script path; NULL: none (Make) */
    const char *sdir;   /* the script's directory */
    int line_base;      /* Make / CI: the text's first row */
    const char *ci;     /* the CI running the text (static); NULL: none */
    const char *job;    /* its job; NULL: none */
    bool exec;          /* an argv exec'd without a shell (Prow) */
    int in_func;
    CBMHashTable *env; /* name -> const sh_val_t * */
    sh_undo_t *undo;
    int nundo;
    int cap_undo;
    sh_cwd_t cwd;
    sh_cwd_t *dirs; /* pushd stack */
    int ndirs;
    int cap_dirs;
    /* While set, an evaluation adds to dflt_acc (a JSON array) the variables
     * whose ${X:-d} default supplied its value. */
    bool dflt_track;
    const char *dflt_acc;
} sh_t;

/* JSON array `list` (NULL: empty) with identifier `name` appended once. */
static const char *sh_names_add(CBMArena *a, const char *list, const char *name, size_t nl) {
    if (!list) {
        return cbm_arena_sprintf(a, "[\"%.*s\"]", (int)nl, name);
    }
    char quoted[CBM_SZ_256];
    snprintf(quoted, sizeof(quoted), "\"%.*s\"", (int)nl, name);
    if (strstr(list, quoted)) {
        return list;
    }
    return cbm_arena_sprintf(a, "%.*s,%s]", (int)(strlen(list) - SKIP_ONE), list, quoted);
}

/* JSON array `a` with the names of JSON array `b` appended once. */
static const char *sh_names_union(CBMArena *ar, const char *a, const char *b) {
    for (const char *p = b ? strchr(b, '"') : NULL; p;) {
        const char *e = strchr(p + SKIP_ONE, '"');
        if (!e) {
            break;
        }
        a = sh_names_add(ar, a, p + SKIP_ONE, (size_t)(e - p - SKIP_ONE));
        p = strchr(e + SKIP_ONE, '"');
    }
    return a;
}

static void sh_set(sh_t *s, const char *name, const sh_val_t *v, bool local) {
    if (!sh_grow((void **)&s->undo, &s->cap_undo, s->nundo, sizeof(*s->undo))) {
        return;
    }
    s->undo[s->nundo++] = (sh_undo_t){name, cbm_ht_get(s->env, name), local};
    cbm_ht_set(s->env, name, (void *)v);
}

static sh_mark_t sh_mark(const sh_t *s) {
    return (sh_mark_t){s->nundo, s->cwd, s->ndirs};
}

static bool sh_cwd_same(sh_cwd_t a, sh_cwd_t b) {
    return a.mode == b.mode && (a.mode != SH_AT || strcmp(a.dir, b.dir) == 0);
}

static void sh_rewind(sh_t *s, sh_mark_t m) {
    while (s->nundo > m.undo) {
        const sh_undo_t *u = &s->undo[--s->nundo];
        if (u->prev) {
            cbm_ht_set(s->env, u->name, (void *)u->prev);
        } else {
            (void)cbm_ht_delete(s->env, u->name);
        }
    }
    s->cwd = m.cwd;
    s->ndirs = m.ndirs;
}

static void sh_touch_add(sh_touch_t *t, const char *name) {
    if (sh_grow((void **)&t->names, &t->cap, t->n, sizeof(*t->names))) {
        t->names[t->n++] = name;
    }
}

/* End one alternative of a conditional: what it set is noted, then undone. */
static void sh_clause_end(sh_t *s, sh_mark_t m, sh_touch_t *t) {
    for (int i = m.undo; i < s->nundo; i++) {
        sh_touch_add(t, s->undo[i].name);
    }
    if (!sh_cwd_same(s->cwd, m.cwd) || s->ndirs != m.ndirs) {
        t->cwd = true;
    }
    sh_rewind(s, m);
}

/* After the conditional: whatever any alternative set is unknown. */
static void sh_group_end(sh_t *s, sh_touch_t *t) {
    for (int i = 0; i < t->n; i++) {
        sh_set(s, t->names[i], &SH_UNKNOWN_VAL, false);
    }
    if (t->cwd) {
        s->cwd = (sh_cwd_t){SH_LOST, NULL};
    }
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)t->names);
    *t = (sh_touch_t){0};
}

/* ── Word evaluation ───────────────────────────────────────────────── */

static const char *sh_text(sh_t *s, TSNode n) {
    uint32_t b = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    return cbm_arena_strndup(s->a, s->src + b, e - b);
}

static bool sh_type(TSNode n, const char *t) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), t) == 0;
}

/* A growable string in the file arena. */
typedef struct {
    char *buf;
    size_t len;
    size_t cap;
    bool bad; /* an unknown part */
} sh_str_t;

static void sh_str_add(CBMArena *a, sh_str_t *b, const char *p, size_t n) {
    if (b->bad || !p) {
        b->bad = true;
        return;
    }
    if (b->len + n + SKIP_ONE > b->cap) {
        size_t ncap = (b->cap ? b->cap : SH_GROW_MIN) * PAIR_LEN;
        while (ncap < b->len + n + SKIP_ONE) {
            ncap *= PAIR_LEN;
        }
        char *nb = cbm_arena_alloc(a, ncap);
        if (!nb) {
            b->bad = true;
            return;
        }
        if (b->len) {
            memcpy(nb, b->buf, b->len);
        }
        b->buf = nb;
        b->cap = ncap;
    }
    memcpy(b->buf + b->len, p, n);
    b->len += n;
    b->buf[b->len] = '\0';
}

static const char *sh_str_done(sh_str_t *b) {
    if (b->bad) {
        return NULL;
    }
    return b->buf ? b->buf : "";
}

/* Text with backslash escapes removed; in_dq: only the double-quote ones. */
static void sh_add_unescaped(CBMArena *a, sh_str_t *b, const char *p, size_t n, bool in_dq) {
    size_t i = 0;
    while (i < n) {
        size_t j = i;
        while (j < n && p[j] != '\\') {
            j++;
        }
        sh_str_add(a, b, p + i, j - i);
        if (j + SKIP_ONE >= n) {
            if (j < n) {
                sh_str_add(a, b, p + j, SKIP_ONE);
            }
            return;
        }
        char c = p[j + SKIP_ONE];
        if (c == '\n') {
            i = j + PAIR_LEN;
            continue;
        }
        if (!in_dq || c == '"' || c == '\\' || c == '$' || c == '`') {
            sh_str_add(a, b, p + j + SKIP_ONE, SKIP_ONE);
        } else {
            sh_str_add(a, b, p + j, PAIR_LEN);
        }
        i = j + PAIR_LEN;
    }
}

static const char *sh_eval(sh_t *s, TSNode n);

/* The cwd as a value: the anchored directory, or NULL. */
static const char *sh_cwd_value(sh_t *s) {
    return s->cwd.mode == SH_AT ? sh_anchor(s->a, s->cwd.dir) : NULL;
}

/* A variable's value; *found: it is set (or a special that exists). */
static const sh_val_t *sh_lookup(sh_t *s, const char *name, bool *found) {
    *found = true;
    if (strcmp(name, "0") == 0 || strcmp(name, "BASH_SOURCE") == 0) {
        if (!s->script) {
            return &SH_UNKNOWN_VAL;
        }
        sh_val_t *v = cbm_arena_alloc(s->a, sizeof(*v));
        if (v) {
            *v = (sh_val_t){s->script, NULL, 0, NULL};
        }
        return v ? v : &SH_UNKNOWN_VAL;
    }
    if (strcmp(name, "PWD") == 0) {
        sh_val_t *v = cbm_arena_alloc(s->a, sizeof(*v));
        if (v) {
            *v = (sh_val_t){sh_cwd_value(s), NULL, 0, NULL};
        }
        return v ? v : &SH_UNKNOWN_VAL;
    }
    if (!name[0] || (strchr("123456789@*#?$!-", name[0]) &&
                     (name[SKIP_ONE] == '\0' || (name[0] >= '1' && name[0] <= '9')))) {
        return &SH_UNKNOWN_VAL; /* positional and special parameters */
    }
    const sh_val_t *v = cbm_ht_get(s->env, name);
    if (!v) {
        *found = false;
        return &SH_UNKNOWN_VAL;
    }
    return v;
}

/* Concatenate the children of `n` in [from, to), source gaps included. */
static void sh_concat(sh_t *s, sh_str_t *b, TSNode n, uint32_t from, uint32_t to, bool in_dq) {
    uint32_t pos = from;
    for (uint32_t i = 0; i < ts_node_child_count(n) && !b->bad; i++) {
        TSNode c = ts_node_child(n, i);
        uint32_t cb = ts_node_start_byte(c);
        uint32_t ce = ts_node_end_byte(c);
        if (cb < from || ce > to) {
            continue;
        }
        if (cb > pos) {
            sh_add_unescaped(s->a, b, s->src + pos, cb - pos, in_dq);
        }
        if (!ts_node_is_named(c)) {
            if (!(in_dq && strcmp(ts_node_type(c), "\"") == 0)) {
                sh_str_add(s->a, b, s->src + cb, ce - cb);
            }
        } else if (sh_type(c, "string_content")) {
            sh_add_unescaped(s->a, b, s->src + cb, ce - cb, in_dq);
        } else {
            const char *v = sh_eval(s, c);
            sh_str_add(s->a, b, v, v ? strlen(v) : 0);
        }
        pos = ce;
    }
    if (pos < to && !b->bad) {
        sh_add_unescaped(s->a, b, s->src + pos, to - pos, in_dq);
    }
}

/* ${...}: a plain variable, an element 0, or a default (:- - := =). */
static const char *sh_eval_expansion(sh_t *s, TSNode n) {
    TSNode name = {0};
    const char *op = NULL;
    uint32_t op_end = 0;
    uint32_t close = ts_node_end_byte(n);
    uint32_t cc = ts_node_child_count(n);
    for (uint32_t i = 0; i < cc; i++) {
        TSNode c = ts_node_child(n, i);
        const char *t = ts_node_type(c);
        if (ts_node_is_named(c)) {
            if (ts_node_is_null(name)) {
                name = c;
            } else if (!op) {
                return NULL; /* ${X%pattern} and the like: no plain value */
            }
            continue;
        }
        if (strcmp(t, "${") == 0) {
            continue;
        }
        if (strcmp(t, "}") == 0) {
            close = ts_node_start_byte(c);
            continue;
        }
        if (op) {
            continue; /* inside the default */
        }
        if (ts_node_is_null(name)) {
            return NULL; /* ${#X}, ${!X}: not the value */
        }
        if (strcmp(t, ":-") != 0 && strcmp(t, "-") != 0 && strcmp(t, ":=") != 0 &&
            strcmp(t, "=") != 0) {
            return NULL; /* ${X%..}, ${X/..}, ${X:+..}, ... */
        }
        op = t;
        op_end = ts_node_end_byte(c);
    }
    if (ts_node_is_null(name)) {
        return NULL;
    }
    const char *nm = NULL;
    int index = SH_NO_DEPTH;
    if (sh_type(name, "subscript")) {
        TSNode base = ts_node_child_by_field_name(name, TS_FIELD("name"));
        TSNode idx = ts_node_child_by_field_name(name, TS_FIELD("index"));
        if (ts_node_is_null(base) || ts_node_is_null(idx)) {
            return NULL;
        }
        const char *it = sh_text(s, idx);
        if (!it || strcmp(it, "0") != 0) {
            return NULL;
        }
        nm = sh_text(s, base);
        index = 0;
    } else if (sh_type(name, "variable_name") || sh_type(name, "special_variable_name")) {
        nm = sh_text(s, name);
    } else {
        return NULL;
    }
    if (!nm) {
        return NULL;
    }
    bool found = false;
    const sh_val_t *v = sh_lookup(s, nm, &found);
    const char *val = v->s;
    if (index == 0 && v->items) {
        val = v->n > 0 ? v->items[0] : "";
    }
    if (!op) {
        if (found && s->dflt_track) {
            s->dflt_acc = sh_names_union(s->a, s->dflt_acc, v->dflt_of);
        }
        return found ? val : NULL;
    }
    bool colon = op[0] == ':';
    bool use_default = !found || (colon && val && !val[0]);
    if (!use_default) {
        if (s->dflt_track) {
            s->dflt_acc = sh_names_union(s->a, s->dflt_acc, v->dflt_of);
        }
        return val;
    }
    sh_str_t b = {0};
    sh_concat(s, &b, n, op_end, close, true);
    const char *d = sh_str_done(&b);
    if (s->dflt_track) {
        s->dflt_acc = sh_names_add(s->a, s->dflt_acc, nm, strlen(nm));
    }
    if (d && strchr(op, '=')) {
        sh_val_t *nv = cbm_arena_alloc(s->a, sizeof(*nv));
        if (nv) {
            *nv = (sh_val_t){d, NULL, 0, sh_names_add(s->a, NULL, nm, strlen(nm))};
            sh_set(s, nm, nv, false);
        }
    }
    return d;
}

/* A value as a repository path: anchored, or relative to a tracked
 * directory. NULL when it is neither. */
static const char *sh_cd_target(sh_t *s, const char *v, sh_cwd_t *out);

/* realpath / readlink -f: the anchored path, when it is decidable. */
static const char *sh_absolute(sh_t *s, const char *v) {
    if (!v) {
        return NULL;
    }
    if (v[0] == SH_ANCHOR) {
        return sh_anchor(s->a, sh_join(s->a, "", v + SKIP_ONE));
    }
    if (v[0] == '/' || s->cwd.mode != SH_AT) {
        return NULL;
    }
    return sh_anchor(s->a, sh_join(s->a, s->cwd.dir, v));
}

typedef struct {
    const char **v;
    bool *split;
    uint32_t *span; /* source bytes of the word: [2i] start, [2i+1] end */
    int n;
    int cap;
} sh_argv_t;

static void sh_argv_build(sh_t *s, TSNode cmd, sh_argv_t *out);

static void sh_argv_free(sh_argv_t *a) {
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)a->v);
    cbm_free(CBM_MEM_CLASS_OTHER, a->split);
    cbm_free(CBM_MEM_CLASS_OTHER, a->span);
    *a = (sh_argv_t){0};
}

/* The command a statement runs: the body of a redirection. */
static TSNode sh_unwrap(TSNode n) {
    while (sh_type(n, "redirected_statement")) {
        TSNode body = ts_node_child_by_field_name(n, TS_FIELD("body"));
        if (ts_node_is_null(body)) {
            break;
        }
        n = body;
    }
    return n;
}

/* The first non-option argument after argv[0]. */
static const char *sh_first_operand(const sh_argv_t *a, int from) {
    for (int i = from; i < a->n; i++) {
        if (a->v[i] && a->v[i][0] == '-' && a->v[i][SKIP_ONE]) {
            continue;
        }
        return a->v[i];
    }
    return NULL;
}

/* $(dirname ..), $(basename ..), $(pwd), $(realpath ..), $(readlink -f ..),
 * $(git rev-parse --show-toplevel). */
static const char *sh_eval_subst_command(sh_t *s, TSNode cmd) {
    sh_argv_t a = {0};
    sh_argv_build(s, cmd, &a);
    const char *out = NULL;
    const char *name = a.n > 0 ? a.v[0] : NULL;
    if (sh_eq(name, "dirname") && a.n == PAIR_LEN && a.v[SKIP_ONE]) {
        out = sh_dirname(s->a, a.v[SKIP_ONE]);
    } else if (sh_eq(name, "basename") && a.n == PAIR_LEN && a.v[SKIP_ONE]) {
        out = sh_value_basename(s->a, a.v[SKIP_ONE]);
    } else if (sh_eq(name, "pwd")) {
        out = sh_cwd_value(s);
    } else if (sh_eq(name, "realpath") ||
               (sh_eq(name, "readlink") && a.n > PAIR_LEN && a.v[SKIP_ONE] &&
                (sh_eq(a.v[SKIP_ONE], "-f") || sh_eq(a.v[SKIP_ONE], "-e") ||
                 sh_eq(a.v[SKIP_ONE], "-m")))) {
        out = sh_absolute(s, sh_first_operand(&a, SKIP_ONE));
    } else if (sh_eq(name, "git") && a.n == 3 && sh_eq(a.v[SKIP_ONE], "rev-parse") &&
               sh_eq(a.v[PAIR_LEN], "--show-toplevel")) {
        out = SH_ROOT;
    }
    sh_argv_free(&a);
    return out;
}

/* `cd DIR` as the left half of $(cd DIR && pwd): the directory, or NULL. */
static const char *sh_subst_cd(sh_t *s, TSNode cd) {
    cd = sh_unwrap(cd);
    if (!sh_type(cd, "command")) {
        return NULL;
    }
    sh_argv_t a = {0};
    sh_argv_build(s, cd, &a);
    const char *out = NULL;
    if (a.n > 0 && sh_eq(a.v[0], "cd")) {
        sh_cwd_t to = {SH_LOST, NULL};
        (void)sh_cd_target(s, sh_first_operand(&a, SKIP_ONE), &to);
        out = to.mode == SH_AT ? sh_anchor(s->a, to.dir) : NULL;
    }
    sh_argv_free(&a);
    return out;
}

static bool sh_is_pwd(sh_t *s, TSNode n) {
    n = sh_unwrap(n);
    if (!sh_type(n, "command")) {
        return false;
    }
    sh_argv_t a = {0};
    sh_argv_build(s, n, &a);
    bool pwd = a.n > 0 && sh_eq(a.v[0], "pwd");
    sh_argv_free(&a);
    return pwd;
}

static const char *sh_eval_subst(sh_t *s, TSNode n) {
    TSNode st[PAIR_LEN];
    int k = 0;
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        TSNode c = ts_node_named_child(n, i);
        if (sh_type(c, "comment")) {
            continue;
        }
        if (k == PAIR_LEN) {
            return NULL;
        }
        st[k++] = c;
    }
    if (k == SKIP_ONE && sh_type(st[0], "list") && ts_node_named_child_count(st[0]) == PAIR_LEN) {
        TSNode l = st[0];
        st[0] = ts_node_named_child(l, 0);
        st[SKIP_ONE] = ts_node_named_child(l, SKIP_ONE);
        k = PAIR_LEN;
    }
    if (k == PAIR_LEN) {
        return sh_is_pwd(s, st[SKIP_ONE]) ? sh_subst_cd(s, st[0]) : NULL;
    }
    if (k == SKIP_ONE) {
        TSNode c = sh_unwrap(st[0]);
        return sh_type(c, "command") ? sh_eval_subst_command(s, c) : NULL;
    }
    return NULL;
}

static const char *sh_eval(sh_t *s, TSNode n) {
    if (ts_node_is_null(n)) {
        return NULL;
    }
    const char *t = ts_node_type(n);
    uint32_t b = ts_node_start_byte(n);
    uint32_t e = ts_node_end_byte(n);
    if (strcmp(t, "word") == 0 || strcmp(t, "number") == 0 || strcmp(t, "extglob_pattern") == 0) {
        if (s->src[b] == '~') {
            return NULL;
        }
        sh_str_t out = {0};
        sh_add_unescaped(s->a, &out, s->src + b, e - b, false);
        return sh_str_done(&out);
    }
    if (strcmp(t, "raw_string") == 0) {
        return e - b >= PAIR_LEN ? cbm_arena_strndup(s->a, s->src + b + SKIP_ONE, e - b - PAIR_LEN)
                                 : NULL;
    }
    if (strcmp(t, "string") == 0 || strcmp(t, "concatenation") == 0) {
        sh_str_t out = {0};
        bool dq = strcmp(t, "string") == 0;
        uint32_t from = dq ? b + SKIP_ONE : b;
        uint32_t to = dq && e > b + SKIP_ONE ? e - SKIP_ONE : e;
        sh_concat(s, &out, n, from, to, dq);
        return sh_str_done(&out);
    }
    if (strcmp(t, "simple_expansion") == 0) {
        TSNode v = ts_node_named_child(n, 0);
        const char *nm = ts_node_is_null(v) ? NULL : sh_text(s, v);
        if (!nm) {
            return NULL;
        }
        bool found = false;
        const sh_val_t *val = sh_lookup(s, nm, &found);
        if (found && s->dflt_track) {
            s->dflt_acc = sh_names_union(s->a, s->dflt_acc, val->dflt_of);
        }
        return found ? val->s : NULL;
    }
    if (strcmp(t, "expansion") == 0) {
        return sh_eval_expansion(s, n);
    }
    if (strcmp(t, "command_substitution") == 0) {
        return sh_eval_subst(s, n);
    }
    return NULL;
}

/* A command word: its value; an option whose value is unknown is still that
 * option (--context="${ctx}" -> "--context="). */
static const char *sh_eval_word(sh_t *s, TSNode w) {
    const char *v = sh_eval(s, w);
    if (v || s->src[ts_node_start_byte(w)] != '-') {
        return v;
    }
    uint32_t b = ts_node_start_byte(w);
    uint32_t e = b;
    while (e < ts_node_end_byte(w) && !strchr("$\"'`\\", s->src[e])) {
        e++;
    }
    return cbm_arena_strndup(s->a, s->src + b, e - b);
}

/* "${name[@]}" / ${name[*]} as a whole word: the array, NULL when not. */
static const sh_val_t *sh_array_word(sh_t *s, TSNode w) {
    if (sh_type(w, "string") && ts_node_named_child_count(w) == SKIP_ONE) {
        w = ts_node_named_child(w, 0);
    }
    if (!sh_type(w, "expansion") || ts_node_named_child_count(w) != SKIP_ONE) {
        return NULL;
    }
    TSNode sub = ts_node_named_child(w, 0);
    if (!sh_type(sub, "subscript")) {
        return NULL;
    }
    TSNode base = ts_node_child_by_field_name(sub, TS_FIELD("name"));
    TSNode idx = ts_node_child_by_field_name(sub, TS_FIELD("index"));
    const char *it = ts_node_is_null(idx) ? NULL : sh_text(s, idx);
    if (ts_node_is_null(base) || !it || (strcmp(it, "@") != 0 && strcmp(it, "*") != 0)) {
        return NULL;
    }
    bool found = false;
    const sh_val_t *v = sh_lookup(s, sh_text(s, base), &found);
    return v;
}

/* An unquoted expansion may split into several words. */
static bool sh_may_split(TSNode w) {
    const char *t = ts_node_type(w);
    if (strcmp(t, "simple_expansion") == 0 || strcmp(t, "expansion") == 0 ||
        strcmp(t, "command_substitution") == 0) {
        return true;
    }
    if (strcmp(t, "concatenation") == 0) {
        for (uint32_t i = 0; i < ts_node_named_child_count(w); i++) {
            if (sh_may_split(ts_node_named_child(w, i))) {
                return true;
            }
        }
    }
    return false;
}

static void sh_argv_push(sh_argv_t *a, const char *v, bool split, uint32_t b, uint32_t e) {
    if (a->n >= a->cap) {
        int ncap = a->cap ? a->cap * PAIR_LEN : SH_GROW_MIN;
        const char **nv =
            cbm_realloc(CBM_MEM_CLASS_OTHER, (void *)a->v, (size_t)ncap * sizeof(*nv));
        if (!nv) {
            return;
        }
        a->v = nv;
        bool *ns = cbm_realloc(CBM_MEM_CLASS_OTHER, a->split, (size_t)ncap * sizeof(*ns));
        if (!ns) {
            return;
        }
        a->split = ns;
        uint32_t *nsp =
            cbm_realloc(CBM_MEM_CLASS_OTHER, a->span, (size_t)ncap * PAIR_LEN * sizeof(*nsp));
        if (!nsp) {
            return;
        }
        a->span = nsp;
        a->cap = ncap;
    }
    a->v[a->n] = v;
    a->split[a->n] = split;
    a->span[PAIR_LEN * a->n] = b;
    a->span[PAIR_LEN * a->n + SKIP_ONE] = e;
    a->n++;
}

static void sh_argv_word(sh_t *s, TSNode w, sh_argv_t *out) {
    uint32_t b = ts_node_start_byte(w);
    uint32_t e = ts_node_end_byte(w);
    const sh_val_t *arr = sh_array_word(s, w);
    if (arr) {
        if (!arr->items) {
            sh_argv_push(out, NULL, true, b, e);
            return;
        }
        for (int i = 0; i < arr->n; i++) {
            sh_argv_push(out, arr->items[i], false, b, e);
        }
        return;
    }
    const char *v = sh_eval_word(s, w);
    sh_argv_push(out, v, v ? false : sh_may_split(w), b, e);
}

/* argv of a command: its name and arguments, evaluated (NULL: unknown). */
static void sh_argv_build(sh_t *s, TSNode cmd, sh_argv_t *out) {
    uint32_t cc = ts_node_child_count(cmd);
    for (uint32_t i = 0; i < cc; i++) {
        const char *field = ts_node_field_name_for_child(cmd, i);
        TSNode c = ts_node_child(cmd, i);
        if (sh_eq(field, "name")) {
            TSNode w = ts_node_named_child(c, 0);
            if (!ts_node_is_null(w)) {
                sh_argv_word(s, w, out);
            }
        } else if (sh_eq(field, "argument")) {
            sh_argv_word(s, c, out);
        }
    }
}

/* ── Paths to targets ──────────────────────────────────────────────── */

typedef struct {
    bool recursive;
    const char *name; /* find -name glob; NULL: any */
    int maxdepth;     /* find -maxdepth; SH_NO_DEPTH: none */
} sh_find_t;

typedef int (*sh_target_fn)(sh_t *s, const char *path, const sh_find_t *f, sh_targets_t *out);

/* Candidate repository paths of a value: the anchored path; a relative one
 * in the tracked directory, or -- none tracked -- in the root and in the
 * script's directory. */
static int sh_candidates(sh_t *s, const char *v, const char *out[PAIR_LEN]) {
    if (!v || !v[0] || strstr(v, "://")) {
        return 0;
    }
    if (v[0] == SH_ANCHOR) {
        out[0] = sh_join(s->a, "", v + SKIP_ONE);
        return out[0] ? SKIP_ONE : 0;
    }
    if (v[0] == '/' || v[0] == '~') {
        return 0;
    }
    if (s->cwd.mode == SH_LOST) {
        return 0;
    }
    if (s->cwd.mode == SH_AT) {
        out[0] = sh_join(s->a, s->cwd.dir, v);
        return out[0] ? SKIP_ONE : 0;
    }
    int n = 0;
    const char *r = sh_join(s->a, "", v);
    const char *d = sh_join(s->a, s->sdir, v);
    if (r) {
        out[n++] = r;
    }
    if (d && (!r || strcmp(d, r) != 0)) {
        out[n++] = d;
    }
    return n;
}

/* The targets of value `v`: of its one candidate, or of the only one of two
 * candidates that has any. */
static void sh_targets_of(sh_t *s, const char *v, sh_target_fn fn, const sh_find_t *f,
                          sh_targets_t *out) {
    const char *c[PAIR_LEN] = {NULL, NULL};
    int n = sh_candidates(s, v, c);
    int mark = out->count;
    int found = 0;
    for (int i = 0; i < n; i++) {
        if (fn(s, c[i], f, out) > 0) {
            found++;
        }
    }
    if (found > SKIP_ONE) {
        out->count = mark;
    }
}

static bool sh_glob_seg(const char *p, size_t pn, const char *t, size_t tn) {
    size_t pi = 0;
    size_t ti = 0;
    size_t star = (size_t)-1;
    size_t mark = 0;
    while (ti < tn) {
        if (pi < pn && (p[pi] == '?' || p[pi] == t[ti])) {
            pi++;
            ti++;
        } else if (pi < pn && p[pi] == '*') {
            star = pi++;
            mark = ti;
        } else if (star != (size_t)-1) {
            pi = star + SKIP_ONE;
            ti = ++mark;
        } else {
            return false;
        }
    }
    while (pi < pn && p[pi] == '*') {
        pi++;
    }
    return pi == pn;
}

/* A path glob: * and ? within a segment, segment for segment. */
static bool sh_glob_path(const char *pat, const char *path) {
    for (;;) {
        size_t pl = strcspn(pat, "/");
        size_t tl = strcspn(path, "/");
        if (!sh_glob_seg(pat, pl, path, tl)) {
            return false;
        }
        if (!pat[pl] || !path[tl]) {
            return !pat[pl] && !path[tl];
        }
        pat += pl + SKIP_ONE;
        path += tl + SKIP_ONE;
    }
}

static int sh_add_manifest(sh_t *s, const char *p, sh_targets_t *out) {
    int64_t id = (int64_t)(intptr_t)cbm_ht_get(s->ix->file_ids, p);
    if (id <= 0) {
        return 0;
    }
    sh_targets_push(out, id, p);
    return SKIP_ONE;
}

/* kubectl -f PATH: the manifest, a directory's manifests, a glob's. */
static int sh_t_files(sh_t *s, const char *path, const sh_find_t *f, sh_targets_t *out) {
    const sh_paths_t *m = &s->ix->manifests;
    char key[CBM_SZ_4K];
    int lo = 0;
    int hi = 0;
    int added = 0;
    if (strpbrk(path, "*?[")) {
        if (strchr(path, '[')) {
            return 0;
        }
        const char *g = strpbrk(path, "*?");
        size_t dl = (size_t)(g - path);
        while (dl > 0 && path[dl - SKIP_ONE] != '/') {
            dl--;
        }
        char dir[CBM_SZ_4K];
        snprintf(dir, sizeof(dir), "%.*s", (int)(dl ? dl - SKIP_ONE : 0), path);
        sh_range(m, dir, key, sizeof(key), &lo, &hi);
        for (int i = lo; i < hi; i++) {
            atomic_fetch_add_explicit(&g_sh_candidates, SKIP_ONE, memory_order_relaxed);
            if (sh_glob_path(path, m->items[i])) {
                added += sh_add_manifest(s, m->items[i], out);
            }
        }
        return added;
    }
    if (sh_paths_has(m, path)) {
        return sh_add_manifest(s, path, out);
    }
    sh_range(m, path, key, sizeof(key), &lo, &hi);
    size_t kl = strlen(key);
    for (int i = lo; i < hi; i++) {
        atomic_fetch_add_explicit(&g_sh_candidates, SKIP_ONE, memory_order_relaxed);
        if (f->recursive || !strchr(m->items[i] + kl, '/')) {
            added += sh_add_manifest(s, m->items[i], out);
        }
    }
    return added;
}

/* find DIR -name G: DIR's manifests (to -maxdepth) whose name matches. */
static int sh_t_found(sh_t *s, const char *dir, const sh_find_t *f, sh_targets_t *out) {
    const sh_paths_t *m = &s->ix->manifests;
    char key[CBM_SZ_4K];
    int lo = 0;
    int hi = 0;
    int added = 0;
    if (!sh_dir_exists(s->ix, dir)) {
        return 0;
    }
    sh_range(m, dir, key, sizeof(key), &lo, &hi);
    size_t kl = strlen(key);
    for (int i = lo; i < hi; i++) {
        atomic_fetch_add_explicit(&g_sh_candidates, SKIP_ONE, memory_order_relaxed);
        const char *rest = m->items[i] + kl;
        int depth = SKIP_ONE;
        for (const char *p = rest; *p; p++) {
            depth += *p == '/';
        }
        const char *base = sh_basename(rest);
        if ((f->maxdepth == SH_NO_DEPTH || depth <= f->maxdepth) &&
            (!f->name || sh_glob_seg(f->name, strlen(f->name), base, strlen(base)))) {
            added += sh_add_manifest(s, m->items[i], out);
        }
    }
    return added;
}

/* -k DIR / kustomize build DIR: the Kustomization of DIR. */
static int sh_t_kust(sh_t *s, const char *dir, const sh_find_t *f, sh_targets_t *out) {
    (void)f;
    static const char *const names[] = {"kustomization.yaml", "kustomization.yml", "Kustomization"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        const char *p = cbm_arena_sprintf(s->a, "%s%s%s", dir, dir[0] ? "/" : "", names[i]);
        const char *key = p ? cbm_ht_get_key(s->ix->kusts, p) : NULL;
        if (key) {
            sh_targets_push(out, (int64_t)(intptr_t)cbm_ht_get(s->ix->kusts, key), dir);
            return SKIP_ONE;
        }
    }
    return 0;
}

/* A chart directory: its Chart. */
static int sh_t_chart(sh_t *s, const char *dir, const sh_find_t *f, sh_targets_t *out) {
    (void)f;
    static const char *const names[] = {"Chart.yaml", "Chart.yml"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        const char *p = cbm_arena_sprintf(s->a, "%s%s%s", dir, dir[0] ? "/" : "", names[i]);
        const char *key = p ? cbm_ht_get_key(s->ix->charts, p) : NULL;
        if (key) {
            sh_targets_push(out, (int64_t)(intptr_t)cbm_ht_get(s->ix->charts, key), dir);
            return SKIP_ONE;
        }
    }
    return 0;
}

/* Where `cd v` lands: *out = AT an existing directory, else LOST. */
static const char *sh_cd_target(sh_t *s, const char *v, sh_cwd_t *out) {
    *out = (sh_cwd_t){SH_LOST, NULL};
    if (!v) {
        return NULL;
    }
    const char *c[PAIR_LEN] = {NULL, NULL};
    int n = sh_candidates(s, v, c);
    const char *hit = NULL;
    for (int i = 0; i < n; i++) {
        if (sh_dir_exists(s->ix, c[i])) {
            if (hit) {
                return NULL;
            }
            hit = c[i];
        }
    }
    if (hit) {
        *out = (sh_cwd_t){SH_AT, hit};
    }
    return hit;
}

/* ── Deploy commands ───────────────────────────────────────────────── */

/* Record hit `x` of command `cmd`: its file, line and CI context are the
 * state's; ext / path are copied. */
static void sh_hit_push(sh_t *s, TSNode cmd, sh_hit_t x) {
    sh_hits_t *h = s->hits;
    if (!sh_grow((void **)&h->items, &h->cap, h->count, sizeof(*h->items))) {
        return;
    }
    x.file = s->file;
    x.line = (int)ts_node_start_point(cmd).row + SKIP_ONE + s->line_base;
    x.ext = x.ext ? cbm_arena_strdup(&h->arena, x.ext) : NULL;
    x.path = x.path ? cbm_arena_strdup(&h->arena, x.path) : NULL;
    x.build_args = x.build_args ? cbm_arena_strdup(&h->arena, x.build_args) : NULL;
    x.default_of = x.default_of ? cbm_arena_strdup(&h->arena, x.default_of) : NULL;
    x.ci = s->ci;
    x.job = s->job ? cbm_arena_strdup(&h->arena, s->job) : NULL;
    h->items[h->count++] = x;
}

static void sh_hit(sh_t *s, TSNode cmd, const char *via, const sh_target_t *t, const char *ext,
                   bool chart) {
    sh_hit_push(s, cmd,
                (sh_hit_t){.via = via,
                           .tgt = t ? t->id : 0,
                           .ext = ext,
                           .path = t ? t->path : ext,
                           .chart = chart});
}

static void sh_hit_all(sh_t *s, TSNode cmd, const char *via, const sh_targets_t *ts, bool chart) {
    for (int i = 0; i < ts->count; i++) {
        sh_hit(s, cmd, via, &ts->items[i], NULL, chart);
    }
}

static bool sh_in(const char *v, const char *const *set) {
    for (int i = 0; v && set[i]; i++) {
        if (strcmp(v, set[i]) == 0) {
            return true;
        }
    }
    return false;
}

static bool sh_numberish(const char *v) {
    if (!v[0] || v[0] < '0' || v[0] > '9') {
        return false;
    }
    for (const char *p = v; *p; p++) {
        if (!((*p >= '0' && *p <= '9') || *p == '.' || strchr("smhd", *p))) {
            return false;
        }
    }
    return true;
}

/* Word i of argv is an option, assignment or count a wrapper takes before
 * the program: by its value, or -- the value unknown -- by its text
 * (env OUT="$TMP/x" prog: the assignment is no program). */
static bool sh_wrapper_arg(const sh_t *s, const sh_argv_t *a, int i) {
    const char *v = a->v[i];
    if (v) {
        return v[0] == '-' || strchr(v, '=') || sh_numberish(v);
    }
    uint32_t b = a->span[PAIR_LEN * i];
    uint32_t e = a->span[PAIR_LEN * i + SKIP_ONE];
    return e > b && (s->src[b] == '-' || memchr(s->src + b, '=', e - b));
}

/* The index of the program argv runs, past wrappers (time, sudo, retry N,
 * timeout D, env A=B, ...); -1 when argv has none. Its value may be unknown
 * (NULL). */
static int sh_program(const sh_t *s, const sh_argv_t *a) {
    static const char *const wrappers[] = {"time",    "sudo", "exec",    "nice",  "nohup",
                                           "command", "env",  "timeout", "retry", NULL};
    int i = 0;
    while (i < a->n) {
        if (!a->v[i] || !sh_in(sh_basename(a->v[i]), wrappers)) {
            return i;
        }
        if (strcmp(a->v[i], "command") == 0 && i + SKIP_ONE < a->n &&
            (sh_eq(a->v[i + SKIP_ONE], "-v") || sh_eq(a->v[i + SKIP_ONE], "-V"))) {
            return -1; /* command -v X: is there an X, not a run of it */
        }
        i++;
        while (i < a->n && sh_wrapper_arg(s, a, i)) {
            i++;
        }
    }
    return -1;
}

/* kubectl's options that take a separate value (before the verb). */
static const char *const SH_KUBECTL_VALUED[] = {"-n",
                                                "--namespace",
                                                "--context",
                                                "--kubeconfig",
                                                "--cluster",
                                                "--user",
                                                "-l",
                                                "--selector",
                                                "-o",
                                                "--output",
                                                "--server",
                                                "-s",
                                                "--token",
                                                "--as",
                                                "--as-group",
                                                "--request-timeout",
                                                "--cache-dir",
                                                "--certificate-authority",
                                                "--client-certificate",
                                                "--client-key",
                                                NULL};

/* helm's options that take a separate value. */
static const char *const SH_HELM_VALUED[] = {"-n",
                                             "--namespace",
                                             "-f",
                                             "--values",
                                             "--set",
                                             "--set-string",
                                             "--set-file",
                                             "--set-json",
                                             "--set-literal",
                                             "--version",
                                             "--timeout",
                                             "--kubeconfig",
                                             "--kube-context",
                                             "--repo",
                                             "--post-renderer",
                                             "--post-renderer-args",
                                             "--description",
                                             "--username",
                                             "--password",
                                             "--ca-file",
                                             "--cert-file",
                                             "--key-file",
                                             "--keyring",
                                             "--history-max",
                                             "--name-template",
                                             "--registry-config",
                                             "--repository-config",
                                             "--repository-cache",
                                             "-o",
                                             "--output",
                                             "--api-versions",
                                             "-a",
                                             "--kube-version",
                                             "--labels",
                                             "-l",
                                             "--show-only",
                                             "-s",
                                             "--output-dir",
                                             "--release-name",
                                             "--kube-apiserver",
                                             "--kube-as-user",
                                             "--kube-as-group",
                                             "--kube-ca-file",
                                             "--kube-token",
                                             "--burst-limit",
                                             "--qps",
                                             NULL};

typedef enum { SH_SRC_NONE, SH_SRC_KUST, SH_SRC_CHART } sh_src_kind_t;

typedef struct {
    sh_src_kind_t kind;
    const char *v;
    const char *via;
} sh_src_t;

typedef struct {
    int verb; /* index of the verb in argv; -1 none */
    const char **files;
    int nfiles;
    const char **kusts;
    int nkusts;
    int cap_files;
    int cap_kusts;
    bool recursive;
} sh_kubectl_t;

static void sh_kubectl_free(sh_kubectl_t *k) {
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)k->files);
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)k->kusts);
}

static void sh_kubectl_add(const char ***v, int *n, int *cap, const char *p) {
    if (sh_grow((void **)v, cap, *n, sizeof(**v))) {
        (*v)[(*n)++] = p;
    }
}

/* kubectl [options] VERB ...: the verb and its -f / -k / -R. */
static void sh_kubectl_parse(const sh_argv_t *a, int from, sh_kubectl_t *k) {
    k->verb = -1;
    int i = from;
    while (i < a->n && k->verb < 0) {
        const char *w = a->v[i];
        if (!w) {
            return;
        }
        if (w[0] != '-') {
            k->verb = i++;
            break;
        }
        i += (sh_in(w, SH_KUBECTL_VALUED) && !strchr(w, '=')) ? PAIR_LEN : SKIP_ONE;
    }
    for (; i < a->n; i++) {
        const char *w = a->v[i];
        if (!w || w[0] != '-') {
            continue;
        }
        const char *next = i + SKIP_ONE < a->n ? a->v[i + SKIP_ONE] : NULL;
        if (strcmp(w, "--recursive") == 0) {
            k->recursive = true;
        } else if (strcmp(w, "--filename") == 0 || strcmp(w, "--kustomize") == 0) {
            bool fl = w[PAIR_LEN] == 'f';
            sh_kubectl_add(fl ? &k->files : &k->kusts, fl ? &k->nfiles : &k->nkusts,
                           fl ? &k->cap_files : &k->cap_kusts, next);
            i++;
        } else if (sh_starts(w, "--filename=")) {
            sh_kubectl_add(&k->files, &k->nfiles, &k->cap_files, strchr(w, '=') + SKIP_ONE);
        } else if (sh_starts(w, "--kustomize=")) {
            sh_kubectl_add(&k->kusts, &k->nkusts, &k->cap_kusts, strchr(w, '=') + SKIP_ONE);
        } else if (w[SKIP_ONE] != '-') {
            /* a cluster of short options: -f, -k, -R, -Rf */
            size_t len = strlen(w);
            bool ok = len > SKIP_ONE;
            for (size_t c = SKIP_ONE; c < len; c++) {
                ok = ok && ((w[c] >= 'a' && w[c] <= 'z') || (w[c] >= 'A' && w[c] <= 'Z'));
            }
            if (!ok) {
                continue;
            }
            if (strchr(w, 'R')) {
                k->recursive = true;
            }
            char last = w[len - SKIP_ONE];
            if (last == 'f' || last == 'k') {
                sh_kubectl_add(last == 'f' ? &k->files : &k->kusts,
                               last == 'f' ? &k->nfiles : &k->nkusts,
                               last == 'f' ? &k->cap_files : &k->cap_kusts, next);
                i++;
            }
        }
    }
}

static void sh_deploy_src(sh_t *s, TSNode cmd, const sh_src_t *src);

/* kubectl: apply / create / replace -f / -k, or `kubectl kustomize D` as a
 * pipe's source. */
static void sh_kubectl(sh_t *s, TSNode cmd, const sh_argv_t *a, int prog, const sh_src_t *in,
                       sh_src_t *out) {
    sh_kubectl_t k = {0};
    sh_kubectl_parse(a, prog + SKIP_ONE, &k);
    const char *verb = k.verb >= 0 ? a->v[k.verb] : NULL;
    if (sh_eq(verb, "kustomize")) {
        const char *d = sh_first_operand(a, k.verb + SKIP_ONE);
        *out = (sh_src_t){SH_SRC_KUST, d ? d : ".", "kustomize"};
    } else if (sh_eq(verb, "apply") || sh_eq(verb, "create") || sh_eq(verb, "replace")) {
        sh_find_t f = {k.recursive, NULL, SH_NO_DEPTH};
        for (int i = 0; i < k.nfiles; i++) {
            if (sh_eq(k.files[i], "-")) {
                sh_deploy_src(s, cmd, in);
                continue;
            }
            sh_targets_t ts = {0};
            sh_targets_of(s, k.files[i], sh_t_files, &f, &ts);
            sh_hit_all(s, cmd, "kubectl", &ts, false);
            cbm_free(CBM_MEM_CLASS_OTHER, ts.items);
        }
        for (int i = 0; i < k.nkusts; i++) {
            sh_targets_t ts = {0};
            sh_targets_of(s, k.kusts[i], sh_t_kust, &f, &ts);
            sh_hit_all(s, cmd, "kubectl", &ts, false);
            cbm_free(CBM_MEM_CLASS_OTHER, ts.items);
        }
    }
    sh_kubectl_free(&k);
}

/* The chart a helm word names: a chart dir, else an external chart. */
static void sh_chart(sh_t *s, TSNode cmd, const char *v) {
    if (!v) {
        return;
    }
    if (sh_starts(v, "oci://")) {
        const char *ext = sh_value_basename(s->a, v + strlen("oci://"));
        if (ext && ext[0] && !strchr(ext, ':')) {
            sh_hit(s, cmd, "helm", NULL, ext, true);
        }
        return;
    }
    sh_targets_t ts = {0};
    sh_targets_of(s, v, sh_t_chart, NULL, &ts);
    if (ts.count > 0) {
        sh_hit_all(s, cmd, "helm", &ts, true);
        cbm_free(CBM_MEM_CLASS_OTHER, ts.items);
        return;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, ts.items);
    /* repo/name: one slash, a plain name on each side */
    const char *slash = strchr(v, '/');
    if (!slash || strchr(slash + SKIP_ONE, '/') || v[0] == '.' || slash == v || !slash[SKIP_ONE]) {
        return;
    }
    for (const char *p = v; *p; p++) {
        if (!(*p == '/' || *p == '.' || *p == '_' || *p == '-' || (*p >= '0' && *p <= '9') ||
              (*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z'))) {
            return;
        }
    }
    sh_hit(s, cmd, "helm", NULL, slash + SKIP_ONE, true);
}

/* helm install|upgrade [NAME] CHART; helm template [NAME] CHART as a source. */
static void sh_helm(sh_t *s, TSNode cmd, const sh_argv_t *a, int prog, sh_src_t *out) {
    const char *verb = NULL;
    int pos[PAIR_LEN] = {0, 0};
    int npos = 0;
    int first_split = SH_NO_DEPTH; /* an unknown unquoted word: maybe several */
    bool gen = false;
    for (int i = prog + SKIP_ONE; i < a->n; i++) {
        const char *w = a->v[i];
        if (w && w[0] == '-') {
            gen = gen || strcmp(w, "--generate-name") == 0 || strcmp(w, "-g") == 0;
            if (sh_in(w, SH_HELM_VALUED) && !strchr(w, '=')) {
                i++;
            }
            continue;
        }
        if (!verb) {
            if (!w) {
                return;
            }
            verb = w;
            continue;
        }
        if (!w && a->split[i] && first_split == SH_NO_DEPTH) {
            first_split = npos;
        }
        if (npos < PAIR_LEN) {
            pos[npos] = i;
        }
        npos++;
    }
    if (!verb) {
        return;
    }
    if (strcmp(verb, "install") == 0 || strcmp(verb, "upgrade") == 0) {
        int at = gen ? 0 : SKIP_ONE;
        if (npos > at && (first_split == SH_NO_DEPTH || first_split > at)) {
            sh_chart(s, cmd, a->v[pos[at]]);
        }
    } else if (strcmp(verb, "template") == 0 && npos > 0 &&
               (first_split == SH_NO_DEPTH || first_split > SKIP_ONE)) {
        int at = npos >= PAIR_LEN ? SKIP_ONE : 0;
        *out = (sh_src_t){SH_SRC_CHART, a->v[pos[at]], "helm"};
    }
}

/* What a `kubectl apply -f -` reading the pipe deploys. */
static void sh_deploy_src(sh_t *s, TSNode cmd, const sh_src_t *src) {
    if (!src || src->kind == SH_SRC_NONE || !src->v) {
        return;
    }
    if (src->kind == SH_SRC_CHART) {
        sh_chart(s, cmd, src->v);
        return;
    }
    sh_targets_t ts = {0};
    sh_targets_of(s, src->v, sh_t_kust, NULL, &ts);
    sh_hit_all(s, cmd, src->via, &ts, false);
    cbm_free(CBM_MEM_CLASS_OTHER, ts.items);
}

/* find DIR [-type f] [-name G] [-maxdepth N] -exec kubectl apply -f {} \; */
static void sh_find(sh_t *s, TSNode cmd, const sh_argv_t *a, int prog) {
    int i = prog + SKIP_ONE;
    if (i >= a->n || !a->v[i] || a->v[i][0] == '-') {
        return;
    }
    const char *root = a->v[i++];
    sh_find_t f = {false, NULL, SH_NO_DEPTH};
    while (i < a->n) {
        const char *w = a->v[i];
        const char *next = i + SKIP_ONE < a->n ? a->v[i + SKIP_ONE] : NULL;
        if (!w) {
            return;
        }
        if (strcmp(w, "-exec") == 0) {
            break;
        }
        if (strcmp(w, "-name") == 0 && next) {
            f.name = next;
        } else if (strcmp(w, "-maxdepth") == 0 && next && sh_numberish(next)) {
            f.maxdepth = (int)strtol(next, NULL, SH_DEC);
        } else if (!(strcmp(w, "-type") == 0 && sh_eq(next, "f"))) {
            return;
        }
        i += PAIR_LEN; /* each test takes one value */
    }
    /* -exec PROGRAM ... {} ; */
    sh_argv_t e = {0};
    for (i++; i < a->n && !sh_eq(a->v[i], ";") && !sh_eq(a->v[i], "+"); i++) {
        sh_argv_push(&e, a->v[i], a->split[i], a->span[PAIR_LEN * i],
                     a->span[PAIR_LEN * i + SKIP_ONE]);
    }
    int p = sh_program(s, &e);
    if (p >= 0 && e.v[p] && strcmp(sh_basename(e.v[p]), "kubectl") == 0) {
        sh_kubectl_t k = {0};
        sh_kubectl_parse(&e, p + SKIP_ONE, &k);
        const char *verb = k.verb >= 0 ? e.v[k.verb] : NULL;
        if ((sh_eq(verb, "apply") || sh_eq(verb, "create") || sh_eq(verb, "replace")) &&
            k.nfiles == SKIP_ONE && k.nkusts == 0 && sh_eq(k.files[0], "{}")) {
            sh_targets_t ts = {0};
            sh_targets_of(s, root, sh_t_found, &f, &ts);
            sh_hit_all(s, cmd, "kubectl", &ts, false);
            cbm_free(CBM_MEM_CLASS_OTHER, ts.items);
        }
        sh_kubectl_free(&k);
    }
    sh_argv_free(&e);
}

/* One command of a pipeline (or a lone one): its deploys; *out: what it
 * writes to the pipe, when that is a kustomize build or a helm template. */
static void sh_deploy_command(sh_t *s, TSNode cmd, const sh_argv_t *a, const sh_src_t *in,
                              sh_src_t *out) {
    *out = (sh_src_t){SH_SRC_NONE, NULL, NULL};
    int prog = sh_program(s, a);
    if (prog < 0 || !a->v[prog]) {
        return;
    }
    const char *base = sh_basename(a->v[prog]);
    if (strcmp(base, "kubectl") == 0) {
        sh_kubectl(s, cmd, a, prog, in, out);
    } else if (strcmp(base, "helm") == 0) {
        sh_helm(s, cmd, a, prog, out);
    } else if (strcmp(base, "kustomize") == 0) {
        const char *verb = sh_first_operand(a, prog + SKIP_ONE);
        if (sh_eq(verb, "build")) {
            int at = SH_NO_DEPTH;
            for (int i = prog + SKIP_ONE; i < a->n; i++) {
                if (a->v[i] == verb) {
                    at = i;
                    break;
                }
            }
            const char *d = at >= 0 ? sh_first_operand(a, at + SKIP_ONE) : NULL;
            *out = (sh_src_t){SH_SRC_KUST, d ? d : ".", "kustomize"};
        }
    } else if (strcmp(base, "find") == 0) {
        sh_find(s, cmd, a, prog);
    }
}

/* The File node a repository path names; 0: none. */
static int64_t sh_repo_file_id(const sh_t *s, const char *path) {
    return (int64_t)(intptr_t)cbm_ht_get(s->ix->file_ids, path);
}

static const char *const SH_SHELLS[] = {"bash", "sh", "zsh", "ksh", "dash", NULL};

/* The one candidate of `path` that names a File of the repository (its
 * File node in *id); NULL: none, or two. */
static const char *sh_unique_file(sh_t *s, const char *path, int64_t *id) {
    const char *c[PAIR_LEN] = {NULL, NULL};
    int n = sh_candidates(s, path, c);
    const char *hit = NULL;
    int found = 0;
    for (int i = 0; i < n; i++) {
        int64_t f = sh_repo_file_id(s, c[i]);
        if (f > 0) {
            found++;
            *id = f;
            hit = c[i];
        }
    }
    return found == SKIP_ONE ? hit : NULL;
}

/* The program is a script of the repository -- its path names a file of it,
 * or a shell runs one (bash [-opts] FILE; -c TEXT is no file) -- resolved to
 * exactly one file: that script is spawned. */
static void sh_spawn_script(sh_t *s, TSNode cmd, const sh_argv_t *a, int p) {
    const char *v = a->v[p];
    const char *path = NULL;
    if (sh_in(sh_basename(v), SH_SHELLS)) {
        int j = p + SKIP_ONE;
        while (j < a->n && a->v[j] && a->v[j][0] == '-' && !sh_eq(a->v[j], "-c")) {
            j++;
        }
        path = j < a->n && !sh_eq(a->v[j], "-c") ? a->v[j] : NULL;
    } else if (strchr(v, '/') && !strpbrk(v, "*?[")) {
        path = v;
    }
    if (!path) {
        return;
    }
    int64_t id = 0;
    const char *hit = sh_unique_file(s, path, &id);
    if (hit) {
        sh_hit_push(s, cmd,
                    (sh_hit_t){.via = s->exec ? "exec" : "shell",
                               .tgt = id,
                               .ext = sh_basename(hit),
                               .path = hit,
                               .spawn = true});
    }
}

/* ── make TARGET (P3.5b) ───────────────────────────────────────────── */

static const char *const SH_MAKES[] = {"make", "gmake", NULL};
static const char *const SH_MAKEFILES[] = {"GNUmakefile", "makefile", "Makefile", NULL};

typedef enum { MK_OPT_NONE, MK_OPT_DIR, MK_OPT_FILE, MK_OPT_ARG } mk_opt_t;

/* A long option of make that takes a value (v: n bytes, no `=...`). */
static mk_opt_t sh_make_long(const char *v, size_t n) {
    static const struct {
        const char *name;
        mk_opt_t kind;
    } opts[] = {
        {"--directory", MK_OPT_DIR},   {"--file", MK_OPT_FILE},    {"--makefile", MK_OPT_FILE},
        {"--include-dir", MK_OPT_ARG}, {"--old-file", MK_OPT_ARG}, {"--assume-old", MK_OPT_ARG},
        {"--what-if", MK_OPT_ARG},     {"--new-file", MK_OPT_ARG}, {"--assume-new", MK_OPT_ARG}};
    for (size_t i = 0; i < sizeof(opts) / sizeof(opts[0]); i++) {
        if (strlen(opts[i].name) == n && strncmp(v, opts[i].name, n) == 0) {
            return opts[i].kind;
        }
    }
    return MK_OPT_NONE;
}

/* A command-line variable: NAME=, NAME:=, NAME::=, NAME+=, NAME?=, NAME!=. */
static bool sh_make_assignment(const char *v) {
    if (!(v[0] == '_' || ((v[0] | ' ') >= 'a' && (v[0] | ' ') <= 'z'))) {
        return false;
    }
    size_t i = SKIP_ONE;
    while (v[i] && (v[i] == '_' || v[i] == '.' || v[i] == '-' || (v[i] >= '0' && v[i] <= '9') ||
                    ((v[i] | ' ') >= 'a' && (v[i] | ' ') <= 'z'))) {
        i++;
    }
    while (v[i] == ' ' || v[i] == '\t') {
        i++;
    }
    if (v[i] == ':') {
        while (v[i] == ':') {
            i++;
        }
        return v[i] == '=';
    }
    return v[i] == '=' || (v[i] && strchr("+?!", v[i]) && v[i + SKIP_ONE] == '=');
}

/* -j N / -l N: N is a count. */
static bool sh_make_count(const char *v) {
    if (!v || !v[0]) {
        return false;
    }
    for (const char *p = v; *p; p++) {
        if (!(*p == '.' || (*p >= '0' && *p <= '9'))) {
            return false;
        }
    }
    return true;
}

/* -C DIR after `base`: DIR from base, an absolute (anchored) DIR alone. */
static const char *sh_make_join(CBMArena *a, const char *base, const char *dir) {
    if (dir[0] == SH_ANCHOR || dir[0] == '/' || sh_eq(base, ".")) {
        return dir;
    }
    return cbm_arena_sprintf(a, "%s/%s", base, dir);
}

/* The makefile make reads in directory `dir` (GNUmakefile, makefile,
 * Makefile, the first there is), for the one candidate of `dir` that has
 * one; NULL otherwise. */
static const char *sh_make_default(sh_t *s, const char *dir) {
    const char *c[PAIR_LEN] = {NULL, NULL};
    int n = sh_candidates(s, dir, c);
    const char *hit = NULL;
    int found = 0;
    for (int i = 0; i < n; i++) {
        for (int k = 0; SH_MAKEFILES[k]; k++) {
            const char *p = sh_join(s->a, c[i], SH_MAKEFILES[k]);
            if (p && sh_repo_file_id(s, p) > 0) {
                found++;
                hit = p;
                break;
            }
        }
    }
    return found == SKIP_ONE ? hit : NULL;
}

/* make [-C DIR] [-f FILE] [VAR=V] TARGET...: each TARGET SPAWNS the rule of
 * that name the makefile reads -- the first -f, else the directory's (-C is
 * cumulative) -- resolved when linking, through its includes. An unknown -C
 * or -f value is no edge, an unknown word no target; no target (the default
 * goal) is no edge. */
static void sh_make(sh_t *s, TSNode cmd, const sh_argv_t *a, int p) {
    const char *dir = ".";
    const char *file = NULL;
    const char **targets = cbm_arena_alloc(s->a, (size_t)(a->n + SKIP_ONE) * sizeof(*targets));
    if (!targets) {
        return;
    }
    int nt = 0;
    bool rest = false;
    for (int j = p + SKIP_ONE; j < a->n; j++) {
        const char *v = a->v[j];
        if (!v) {
            continue;
        }
        if (rest || v[0] != '-' || sh_eq(v, "-")) {
            if (rest || !sh_make_assignment(v)) {
                targets[nt++] = v;
            }
            continue;
        }
        if (sh_eq(v, "--")) {
            rest = true;
            continue;
        }
        const char *arg = NULL;
        mk_opt_t kind = MK_OPT_NONE;
        if (v[SKIP_ONE] == '-') {
            const char *eq = strchr(v, '=');
            kind = sh_make_long(v, eq ? (size_t)(eq - v) : strlen(v));
            arg = eq ? eq + SKIP_ONE : NULL;
            if (kind != MK_OPT_NONE && !eq) {
                j++;
                arg = j < a->n ? a->v[j] : NULL;
            }
        } else {
            for (size_t k = SKIP_ONE; v[k]; k++) {
                if (strchr("CfIoW", v[k])) {
                    kind = v[k] == 'C' ? MK_OPT_DIR : v[k] == 'f' ? MK_OPT_FILE : MK_OPT_ARG;
                    arg = v[k + SKIP_ONE] ? v + k + SKIP_ONE : NULL;
                    if (!arg) {
                        j++;
                        arg = j < a->n ? a->v[j] : NULL;
                    }
                    break;
                }
                if (v[k] == 'j' || v[k] == 'l') {
                    if (!v[k + SKIP_ONE] && j + SKIP_ONE < a->n &&
                        sh_make_count(a->v[j + SKIP_ONE])) {
                        j++;
                    }
                    break;
                }
            }
        }
        if (kind == MK_OPT_DIR || kind == MK_OPT_FILE) {
            if (!arg) {
                return;
            }
            if (kind == MK_OPT_DIR) {
                dir = sh_make_join(s->a, dir, arg);
            } else if (!file) {
                file = arg;
            }
        }
    }
    int64_t id = 0;
    const char *mkfile =
        file ? sh_unique_file(s, sh_make_join(s->a, dir, file), &id) : sh_make_default(s, dir);
    for (int i = 0; mkfile && i < nt; i++) {
        sh_hit_push(s, cmd,
                    (sh_hit_t){.via = s->exec ? "exec" : "shell",
                               .ext = targets[i],
                               .path = mkfile,
                               .spawn = true,
                               .make = true});
    }
}

/* A program started by a path (./_build/x, ${DIR}/x, $(LOCALBIN)/x) SPAWNS
 * the Process of its name -- the value's last segment, or, the value
 * unknown, the word's literal last segment. A path naming a file of the
 * repository is a script (sh_spawn_script), not a Process. */
static void sh_spawn(sh_t *s, TSNode cmd, const sh_argv_t *a) {
    int p = sh_program(s, a);
    if (p < 0) {
        return;
    }
    const char *v = a->v[p];
    const char *name = NULL;
    if (v) {
        if (sh_in(sh_basename(v), SH_MAKES)) {
            sh_make(s, cmd, a, p);
        }
        sh_spawn_script(s, cmd, a, p);
        if (!strchr(v, '/') || strpbrk(v, "*?[")) {
            return;
        }
        const char *c[PAIR_LEN] = {NULL, NULL};
        int n = sh_candidates(s, v, c);
        for (int i = 0; i < n; i++) {
            if (sh_repo_file_id(s, c[i]) > 0) {
                return;
            }
        }
        name = sh_value_basename(s->a, v);
    } else {
        uint32_t b = a->span[PAIR_LEN * p];
        uint32_t e = a->span[PAIR_LEN * p + SKIP_ONE];
        while (e > b && (s->src[e - SKIP_ONE] == '"' || s->src[e - SKIP_ONE] == '\'')) {
            e--;
        }
        uint32_t k = e;
        while (k > b && s->src[k - SKIP_ONE] != '/') {
            k--;
        }
        if (k == b) {
            return;
        }
        name = cbm_arena_strndup(s->a, s->src + k, e - k);
        if (name && strpbrk(name, "$`*?[]{}()\"' \t\\")) {
            return;
        }
    }
    if (!name || !name[0] || strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
        return;
    }
    sh_hit_push(s, cmd, (sh_hit_t){.via = s->exec ? "exec" : "shell", .ext = name, .spawn = true});
}

/* ── HTTP requests (F3) ─────────────────────────────────────────────── */

/* `url` (http / https) as scheme://host[:port]/path -- the user info, the
 * query and the fragment dropped, so no credential or token reaches the
 * graph; NULL when it is not one, or its host is unknown. */
static const char *sh_url_norm(CBMArena *a, const char *url) {
    const char *scheme_end = strstr(url, "://");
    if (!scheme_end || (strncmp(url, "http://", strlen("http://")) != 0 &&
                        strncmp(url, "https://", strlen("https://")) != 0)) {
        return NULL;
    }
    const char *auth = scheme_end + strlen("://");
    size_t al = strcspn(auth, "/?#");
    const char *at = NULL;
    for (size_t i = 0; i < al; i++) {
        if (auth[i] == '@') {
            at = auth + i;
        }
    }
    const char *host = at ? at + SKIP_ONE : auth;
    size_t hl = (size_t)(auth + al - host);
    const char *h = hl ? cbm_arena_strndup(a, host, hl) : NULL;
    if (!h || strpbrk(h, "$`{}\"' \t\\")) {
        return NULL;
    }
    const char *path = auth + al;
    size_t pl = strcspn(path, "?#");
    return cbm_arena_sprintf(a, "%.*s%.*s%.*s", (int)(auth - url), url, (int)hl, host,
                             pl ? (int)pl : SKIP_ONE, pl ? path : "/");
}

/* Skip one expansion at p (`${...}`, `$(...)`, `$NAME`, a backquoted
 * command); returns the byte after it. */
static const char *sh_url_skip_expansion(const char *p) {
    if (*p == '`') {
        const char *e = strchr(p + SKIP_ONE, '`');
        return e ? e + SKIP_ONE : p + strlen(p);
    }
    char open = p[SKIP_ONE];
    if (open == '{' || open == '(') {
        char close = open == '{' ? '}' : ')';
        int depth = 0;
        for (const char *q = p + SKIP_ONE; *q; q++) {
            depth += *q == open;
            depth -= *q == close;
            if (depth == 0) {
                return q + SKIP_ONE;
            }
        }
        return p + strlen(p);
    }
    const char *q = p + SKIP_ONE;
    while (isalnum((unsigned char)*q) || *q == '_') {
        q++;
    }
    return q;
}

/* An http(s) URL word the script states with a literal host but variable
 * parts after it ("https://host/releases/download/${VER}/x.tgz"),
 * from its source text as the shell reads the word -- up to an unquoted
 * blank or operator, as tree-sitter-bash may split an unquoted word with
 * expansions into several arguments: quotes dropped, each expansion of the
 * path a `{}` as in a Route path; then sh_url_norm. NULL when the host is
 * not literal. */
static const char *sh_url_template(sh_t *s, const sh_argv_t *a, int i) {
    sh_str_t out = {0};
    char quote = 0;
    for (const char *p = s->src + a->span[PAIR_LEN * i]; *p;) {
        if (!quote && (strchr(" \t\r\n|;&<>)", *p) || (*p == '\\' && p[SKIP_ONE] == '\n'))) {
            break;
        }
        if ((*p == '"' || *p == '\'') && (!quote || quote == *p)) {
            quote = quote ? 0 : *p;
            p++;
        } else if ((*p == '$' || *p == '`') && quote != '\'') {
            sh_str_add(s->a, &out, "{}", PAIR_LEN);
            p = sh_url_skip_expansion(p);
        } else {
            sh_str_add(s->a, &out, p, SKIP_ONE);
            p++;
        }
    }
    const char *raw = sh_str_done(&out);
    return raw ? sh_url_norm(s->a, raw) : NULL; /* a `{}` host is no literal one */
}

static bool sh_ieq(const char *a, const char *b) {
    for (; *a && *b; a++, b++) {
        if (toupper((unsigned char)*a) != toupper((unsigned char)*b)) {
            return false;
        }
    }
    return *a == *b;
}

/* Options whose value is a URL that is not the request: a proxy, a referer. */
static bool sh_http_url_option(const char *o) {
    static const char *const OPTS[] = {"-x",        "--proxy",   "--preproxy", "-e",
                                       "--referer", "--doh-url", "--proxy1.0", NULL};
    return sh_in(o, OPTS);
}

/* The method curl / wget sends: -X / --request / --method; a body (-d, -F,
 * --data*, --json, --form*, --post-data / --post-file) POST, -T PUT, -I HEAD,
 * -G back to GET; default GET. */
static const char *sh_http_method(const sh_argv_t *a, int from) {
    static const char *const METHODS[] = {"GET",    "POST", "PUT",     "PATCH",
                                          "DELETE", "HEAD", "OPTIONS", NULL};
    const char *method = "GET";
    bool get = false;
    for (int i = from; i < a->n; i++) {
        const char *w = a->v[i];
        if (!w) {
            continue;
        }
        const char *named = NULL;
        if (sh_eq(w, "-X") || sh_eq(w, "--request") || sh_eq(w, "--method")) {
            named = i + SKIP_ONE < a->n ? a->v[i + SKIP_ONE] : NULL;
        } else if (sh_starts(w, "-X") && w[PAIR_LEN]) {
            named = w + PAIR_LEN;
        } else if (sh_starts(w, "--request=") || sh_starts(w, "--method=")) {
            named = strchr(w, '=') + SKIP_ONE;
        }
        for (int m = 0; named && METHODS[m]; m++) {
            if (sh_ieq(named, METHODS[m])) {
                return METHODS[m];
            }
        }
        if (sh_eq(w, "-G") || sh_eq(w, "--get")) {
            get = true;
        } else if (sh_eq(w, "-I") || sh_eq(w, "--head")) {
            method = "HEAD";
        } else if (sh_eq(w, "-T") || sh_starts(w, "--upload-file")) {
            method = "PUT";
        } else if (sh_eq(w, "-d") || sh_eq(w, "-F") || sh_starts(w, "--data") ||
                   sh_starts(w, "--json") || sh_starts(w, "--form") ||
                   sh_starts(w, "--post-data") || sh_starts(w, "--post-file")) {
            method = strcmp(method, "GET") == 0 ? "POST" : method;
        }
    }
    return get && strcmp(method, "POST") == 0 ? "GET" : method;
}

/* curl / wget with a URL the script states (constant words): an HTTP
 * request to it, from the command's function. */
static void sh_http(sh_t *s, TSNode cmd, const sh_argv_t *a) {
    int p = sh_program(s, a);
    const char *prog = p >= 0 && a->v[p] ? sh_basename(a->v[p]) : NULL;
    if (!prog || (strcmp(prog, "curl") != 0 && strcmp(prog, "wget") != 0)) {
        return;
    }
    const char *via = strcmp(prog, "curl") == 0 ? "curl" : "wget";
    const char *method = sh_http_method(a, p + SKIP_ONE);
    for (int i = p + SKIP_ONE; i < a->n; i++) {
        /* arguments that touch in the source are one shell word, which
         * tree-sitter-bash may split around an expansion */
        int j = i;
        while (j + SKIP_ONE < a->n &&
               a->span[PAIR_LEN * (j + SKIP_ONE)] == a->span[PAIR_LEN * j + SKIP_ONE]) {
            j++;
        }
        bool url_option =
            i > p + SKIP_ONE && a->v[i - SKIP_ONE] && sh_http_url_option(a->v[i - SKIP_ONE]);
        sh_str_t word = {0};
        for (int k = i; k <= j; k++) {
            sh_str_add(s->a, &word, a->v[k], a->v[k] ? strlen(a->v[k]) : 0);
        }
        const char *w = sh_str_done(&word);
        /* a `$` left in an evaluated word is an expansion the grammar took
         * for text (`$OS-$ARCH.tgz`): no value, so the template */
        const char *url = url_option               ? NULL
                          : w && !strpbrk(w, "$`") ? sh_url_norm(s->a, w)
                                                   : sh_url_template(s, a, i);
        if (url) {
            sh_hit_push(s, cmd, (sh_hit_t){.via = via, .ext = url, .path = method, .http = true});
        }
        i = j;
    }
}

/* ── Statements ────────────────────────────────────────────────────── */

static void sh_walk(sh_t *s, TSNode n);

static void sh_walk_children(sh_t *s, TSNode n) {
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        sh_walk(s, ts_node_named_child(n, i));
    }
}

/* The commands inside $( ) and <( ) of a word, a value or a redirect run
 * too, each in a subshell of its own. */
static void sh_walk_substs(sh_t *s, TSNode n) {
    if (sh_type(n, "command_substitution") || sh_type(n, "process_substitution")) {
        sh_mark_t m = sh_mark(s);
        sh_walk_children(s, n);
        sh_rewind(s, m);
        return;
    }
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        sh_walk_substs(s, ts_node_named_child(n, i));
    }
}

static void sh_assign(sh_t *s, TSNode n, bool local) {
    TSNode name = ts_node_child_by_field_name(n, TS_FIELD("name"));
    TSNode value = ts_node_child_by_field_name(n, TS_FIELD("value"));
    if (!ts_node_is_null(value)) {
        sh_walk_substs(s, value);
    }
    if (ts_node_is_null(name)) {
        return;
    }
    if (sh_type(name, "subscript")) {
        TSNode base = ts_node_child_by_field_name(name, TS_FIELD("name"));
        if (!ts_node_is_null(base)) {
            sh_set(s, sh_text(s, base), &SH_UNKNOWN_VAL, local);
        }
        return;
    }
    const char *nm = sh_text(s, name);
    sh_val_t *v = cbm_arena_alloc(s->a, sizeof(*v));
    if (!nm || !v) {
        return;
    }
    *v = (sh_val_t){"", NULL, 0, NULL};
    bool track = s->dflt_track;
    const char *acc = s->dflt_acc;
    s->dflt_track = true;
    s->dflt_acc = NULL;
    if (sh_type(value, "array")) {
        uint32_t cnt = ts_node_named_child_count(value);
        const char **items = cbm_arena_alloc(s->a, (cnt ? cnt : SKIP_ONE) * sizeof(*items));
        if (!items) {
            return;
        }
        int k = 0;
        for (uint32_t i = 0; i < cnt; i++) {
            TSNode c = ts_node_named_child(value, i);
            if (!sh_type(c, "comment")) {
                items[k++] = sh_eval_word(s, c);
            }
        }
        *v = (sh_val_t){k > 0 ? items[0] : "", items, k, NULL};
    } else if (!ts_node_is_null(value)) {
        v->s = sh_eval(s, value);
    }
    v->dflt_of = s->dflt_acc;
    s->dflt_track = track;
    s->dflt_acc = acc;
    bool append = false;
    for (uint32_t i = 0; i < ts_node_child_count(n); i++) {
        TSNode c = ts_node_child(n, i);
        append = append || (!ts_node_is_named(c) && strcmp(ts_node_type(c), "+=") == 0);
    }
    if (append) {
        bool found = false;
        const sh_val_t *prev = sh_lookup(s, nm, &found);
        if (found) {
            v->s = (prev->s && v->s) ? cbm_arena_sprintf(s->a, "%s%s", prev->s, v->s) : NULL;
            v->items = NULL;
        }
    }
    sh_set(s, nm, v, local);
}

/* local / declare / typeset / export / readonly NAME[=VALUE] ... */
static void sh_declare(sh_t *s, TSNode n) {
    TSNode kw = ts_node_child(n, 0);
    const char *k = ts_node_is_null(kw) ? "" : ts_node_type(kw);
    bool local = strcmp(k, "local") == 0 ||
                 (s->in_func && (strcmp(k, "declare") == 0 || strcmp(k, "typeset") == 0));
    bool declares = local || strcmp(k, "declare") == 0 || strcmp(k, "typeset") == 0;
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        TSNode c = ts_node_named_child(n, i);
        if (sh_type(c, "variable_assignment")) {
            sh_assign(s, c, local);
        } else if (declares && sh_type(c, "variable_name")) {
            sh_set(s, sh_text(s, c), &SH_UNKNOWN_VAL, local);
        }
    }
}

/* cd / pushd / popd; true when the command is one. */
static bool sh_chdir(sh_t *s, const sh_argv_t *a) {
    const char *name = a->n > 0 ? a->v[0] : NULL;
    if (sh_eq(name, "popd")) {
        s->cwd = s->ndirs > 0 ? s->dirs[--s->ndirs] : (sh_cwd_t){SH_LOST, NULL};
        return true;
    }
    if (!sh_eq(name, "cd") && !sh_eq(name, "pushd")) {
        return false;
    }
    if (sh_eq(name, "pushd")) {
        if (sh_grow((void **)&s->dirs, &s->cap_dirs, s->ndirs, sizeof(*s->dirs))) {
            s->dirs[s->ndirs++] = s->cwd;
        }
    }
    const char *to = sh_first_operand(a, SKIP_ONE);
    sh_cwd_t next = {SH_LOST, NULL};
    if (to && strcmp(to, "-") != 0) {
        (void)sh_cd_target(s, to, &next);
    }
    s->cwd = next;
    return true;
}

/* A container build's flags that take a separate value. */
static const char *const SH_BUILD_VALUED[] = {"--build-arg",
                                              "--target",
                                              "--platform",
                                              "--label",
                                              "--cache-from",
                                              "--cache-to",
                                              "--secret",
                                              "--ssh",
                                              "-o",
                                              "--output",
                                              "--network",
                                              "--add-host",
                                              "--iidfile",
                                              "--progress",
                                              "--builder",
                                              "--build-context",
                                              "--shm-size",
                                              "--ulimit",
                                              "-m",
                                              "--memory",
                                              "--memory-swap",
                                              "--cpu-shares",
                                              "-c",
                                              "--cpuset-cpus",
                                              "--cpuset-mems",
                                              "--cgroup-parent",
                                              "--isolation",
                                              "--metadata-file",
                                              "--annotation",
                                              "--attest",
                                              "--security-opt",
                                              "--format",
                                              "--jobs",
                                              "--os",
                                              "--arch",
                                              "--variant",
                                              "--manifest",
                                              "--authfile",
                                              "--cert-dir",
                                              "--creds",
                                              "--allow",
                                              "--call",
                                              NULL};

enum { SH_BUILD_TAGS = 16, SH_IMAGE_NEST = 16 };

/* The repository of image word `i` whose value is unknown: the word up to
 * the ':' of its tag (or '@' of a digest) after its last '/', outside
 * $( ) ${ } and backticks, evaluated alone -- the repository is the image's
 * identity, a tag of unknown value keeps it. NULL: no tag to cut, or the
 * repository is unknown too. */
static const char *sh_image_repo(sh_t *s, TSNode cmd, const sh_argv_t *a, int i) {
    uint32_t b = a->span[(size_t)i * PAIR_LEN];
    uint32_t e = a->span[(size_t)i * PAIR_LEN + SKIP_ONE];
    char closers[SH_IMAGE_NEST];
    int depth = 0;
    uint32_t cut = 0;
    for (uint32_t k = b; k < e; k++) {
        char c = s->src[k];
        bool open2 = k + SKIP_ONE < e && c == '$' &&
                     (s->src[k + SKIP_ONE] == '(' || s->src[k + SKIP_ONE] == '{');
        if (depth > 0 && c == closers[depth - SKIP_ONE]) {
            depth--;
        } else if ((open2 || c == '`') && depth < SH_IMAGE_NEST) {
            closers[depth++] = c == '`' ? '`' : (s->src[k + SKIP_ONE] == '(' ? ')' : '}');
            k += open2 ? SKIP_ONE : 0;
        } else if (depth == 0 && c == '/') {
            cut = 0;
        } else if (depth == 0 && (c == ':' || c == '@') && cut == 0) {
            cut = k;
        }
    }
    if (cut == 0) {
        return NULL;
    }
    TSNode w = ts_node_descendant_for_byte_range(cmd, b, e);
    while (!ts_node_is_null(w) && (ts_node_start_byte(w) != b || ts_node_end_byte(w) != e)) {
        w = ts_node_parent(w);
    }
    sh_str_t out = {0};
    if (sh_type(w, "concatenation")) {
        sh_concat(s, &out, w, b, cut, false);
    } else if (sh_type(w, "string")) {
        sh_concat(s, &out, w, b + SKIP_ONE, cut, true);
    } else {
        return NULL;
    }
    return sh_str_done(&out);
}

/* JSON object `json` (NULL: none) with build argument `kv` ("K=V") added; a
 * `K` alone (the environment's value), an unknown word or a secret (by name
 * or value -- never kept in the graph, never an image) adds nothing. */
static const char *sh_build_arg(sh_t *s, const char *json, const char *kv) {
    const char *eq = kv ? strchr(kv, '=') : NULL;
    if (!eq || eq == kv) {
        return json;
    }
    for (const char *p = kv; p < eq; p++) {
        if (!isalnum((unsigned char)*p) && *p != '_') {
            return json;
        }
    }
    const char *key = cbm_arena_strndup(s->a, kv, (size_t)(eq - kv));
    if (!key || cbm_is_secret_binding(key, eq + SKIP_ONE)) {
        return json;
    }
    /* JSON escaping writes at most 6 bytes a byte: nothing is cut */
    int vn = (int)(strlen(eq + SKIP_ONE) * 6 + SKIP_ONE);
    char *ev = cbm_arena_alloc(s->a, (size_t)vn);
    if (!ev) {
        return json;
    }
    cbm_json_escape(ev, vn, eq + SKIP_ONE);
    const char *pair = cbm_arena_sprintf(s->a, "\"%.*s\":\"%s\"", (int)(eq - kv), kv, ev);
    if (!pair) {
        return json;
    }
    return json ? cbm_arena_sprintf(s->a, "%.*s,%s}", (int)(strlen(json) - SKIP_ONE), json, pair)
                : cbm_arena_sprintf(s->a, "{%s}", pair);
}

/* The variables whose ${X:-d} default supplied argv word `i` (a JSON array,
 * NULL: none): the word evaluated once more with the defaults recorded. */
static const char *sh_word_defaults(sh_t *s, TSNode cmd, const sh_argv_t *a, int i) {
    uint32_t b = a->span[(size_t)i * PAIR_LEN];
    uint32_t e = a->span[(size_t)i * PAIR_LEN + SKIP_ONE];
    if (!memchr(s->src + b, '$', e - b)) {
        return NULL;
    }
    TSNode w = ts_node_descendant_for_byte_range(cmd, b, e);
    while (!ts_node_is_null(w) && (ts_node_start_byte(w) != b || ts_node_end_byte(w) != e)) {
        w = ts_node_parent(w);
    }
    if (ts_node_is_null(w)) {
        return NULL;
    }
    bool track = s->dflt_track;
    const char *acc = s->dflt_acc;
    s->dflt_track = true;
    s->dflt_acc = NULL;
    (void)sh_eval_word(s, w);
    const char *out = s->dflt_acc;
    s->dflt_track = track;
    s->dflt_acc = acc;
    return out;
}

/* docker / podman / nerdctl [buildx | image | builder] build, buildah bud:
 * each tagged image is built from the Dockerfile -f names (against the cwd),
 * else <context>/Dockerfile -- one file of the repository. */
static void sh_build(sh_t *s, TSNode cmd, const sh_argv_t *a) {
    int p = sh_program(s, a);
    const char *tool = p >= 0 && a->v[p] ? sh_basename(a->v[p]) : NULL;
    int i = p + SKIP_ONE;
    if (tool && strcmp(tool, "buildah") == 0) {
        if (i >= a->n || !a->v[i] ||
            !(sh_eq(a->v[i], "bud") || sh_eq(a->v[i], "build") ||
              sh_eq(a->v[i], "build-using-dockerfile"))) {
            return;
        }
        i++;
    } else if (tool && (strcmp(tool, "docker") == 0 || strcmp(tool, "podman") == 0 ||
                        strcmp(tool, "nerdctl") == 0)) {
        if (i < a->n && sh_eq(a->v[i], "build")) {
            i++;
        } else if (i + SKIP_ONE < a->n &&
                   (sh_eq(a->v[i], "buildx") || sh_eq(a->v[i], "image") ||
                    sh_eq(a->v[i], "builder")) &&
                   sh_eq(a->v[i + SKIP_ONE], "build")) {
            i += PAIR_LEN;
        } else {
            return;
        }
    } else {
        return;
    }
    if (s->hits->declarers) {
        cbm_ht_set(s->hits->declarers, s->file, (void *)s->file);
    }
    int tags[SH_BUILD_TAGS];
    int tag_words[SH_BUILD_TAGS];
    const char *tag_vals[SH_BUILD_TAGS];
    int nt = 0;
    const char *dfile = NULL;
    bool dfile_known = true;
    const char *ctx = NULL;
    bool ctx_seen = false;
    const char *args = NULL;
    for (; i < a->n; i++) {
        const char *w = a->v[i];
        if (w && (sh_eq(w, "-t") || sh_eq(w, "--tag")) && i + SKIP_ONE < a->n) {
            if (nt < SH_BUILD_TAGS) {
                tags[nt] = i + SKIP_ONE;
                tag_words[nt] = i + SKIP_ONE;
                tag_vals[nt++] = a->v[i + SKIP_ONE];
            }
            i++;
        } else if (w && (sh_eq(w, "-f") || sh_eq(w, "--file")) && i + SKIP_ONE < a->n) {
            dfile = a->v[i + SKIP_ONE];
            dfile_known = dfile != NULL;
            i++;
        } else if (w && sh_eq(w, "--build-arg") && i + SKIP_ONE < a->n) {
            args = sh_build_arg(s, args, a->v[i + SKIP_ONE]);
            i++;
        } else if (w && strncmp(w, "--build-arg=", strlen("--build-arg=")) == 0) {
            args = sh_build_arg(s, args, w + strlen("--build-arg="));
        } else if (w && strncmp(w, "--tag=", strlen("--tag=")) == 0) {
            if (nt < SH_BUILD_TAGS && w[strlen("--tag=")]) {
                tags[nt] = -SKIP_ONE;
                tag_words[nt] = i;
                tag_vals[nt++] = w + strlen("--tag=");
            }
        } else if (w && strncmp(w, "--file=", strlen("--file=")) == 0) {
            dfile = w + strlen("--file=");
        } else if (w && w[0] == '-') {
            i += !strchr(w, '=') && sh_in(w, SH_BUILD_VALUED) ? SKIP_ONE : 0;
        } else if (!ctx_seen) {
            ctx_seen = true;
            ctx = w;
        }
    }
    if (nt == 0 || !ctx || !dfile_known || strcmp(ctx, "-") == 0 || strstr(ctx, "://") ||
        strncmp(ctx, "git@", strlen("git@")) == 0) {
        return;
    }
    const char *path = dfile ? dfile : sh_join(s->a, ctx, "Dockerfile");
    int64_t id = 0;
    const char *hit = path ? sh_unique_file(s, path, &id) : NULL;
    for (int k = 0; hit && k < nt; k++) {
        const char *img = tag_vals[k];
        if (!img && tags[k] >= 0) {
            img = sh_image_repo(s, cmd, a, tags[k]);
        }
        if (img && img[0]) {
            sh_hit_push(s, cmd,
                        (sh_hit_t){.via = "docker-build",
                                   .ext = img,
                                   .path = hit,
                                   .build = true,
                                   .build_args = args,
                                   .default_of = sh_word_defaults(s, cmd, a, tag_words[k])});
        }
    }
}

static void sh_command(sh_t *s, TSNode cmd) {
    sh_walk_substs(s, cmd);
    sh_argv_t a = {0};
    sh_argv_build(s, cmd, &a);
    if (!sh_chdir(s, &a)) {
        sh_src_t out;
        sh_deploy_command(s, cmd, &a, NULL, &out);
        sh_spawn(s, cmd, &a);
        sh_build(s, cmd, &a);
        sh_http(s, cmd, &a);
    }
    sh_argv_free(&a);
}

/* The redirects of a statement: `cat <<EOF | kubectl ...` -- the rest of
 * that pipe follows the heredoc start, inside the redirect, and reads the
 * heredoc, not this pipe; `< <(cmd)` runs cmd. */
static void sh_redirects(sh_t *s, TSNode el) {
    for (uint32_t r = 0; sh_type(el, "redirected_statement") && r < ts_node_child_count(el); r++) {
        TSNode red = ts_node_child(el, r);
        if (!ts_node_is_named(red) || sh_eq(ts_node_field_name_for_child(el, r), "body")) {
            continue;
        }
        if (sh_type(red, "heredoc_redirect")) {
            sh_walk_children(s, red);
        } else {
            sh_walk_substs(s, red);
        }
    }
}

/* The elements of a pipeline, in order, with what flows through it. An
 * element that redirects a pipeline continues this one: tree-sitter-bash
 * folds `a | b` followed by a line `cat <<EOF | c` into `a | (b cat | c)`. */
static void sh_pipe_run(sh_t *s, TSNode n, sh_src_t *src) {
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        TSNode el = ts_node_named_child(n, i);
        TSNode cmd = sh_unwrap(el);
        if (sh_type(cmd, "pipeline")) {
            sh_pipe_run(s, cmd, src);
            sh_redirects(s, el);
            continue;
        }
        if (!sh_type(cmd, "command")) {
            sh_walk(s, el);
            *src = (sh_src_t){SH_SRC_NONE, NULL, NULL};
            continue;
        }
        sh_walk_substs(s, cmd);
        sh_argv_t a = {0};
        sh_argv_build(s, cmd, &a);
        sh_src_t out;
        sh_deploy_command(s, cmd, &a, src, &out);
        if (out.kind != SH_SRC_NONE) {
            *src = out;
        }
        sh_spawn(s, cmd, &a);
        sh_build(s, cmd, &a);
        sh_http(s, cmd, &a);
        sh_argv_free(&a);
        sh_redirects(s, el);
    }
}

/* A pipeline: every element runs in a subshell; a kustomize build or helm
 * template feeds the kubectl that reads the pipe. */
static void sh_pipeline(sh_t *s, TSNode n) {
    sh_mark_t m = sh_mark(s);
    sh_src_t src = {SH_SRC_NONE, NULL, NULL};
    sh_pipe_run(s, n, &src);
    sh_rewind(s, m);
}

/* a && b || c: the first runs, the rest only maybe. */
static void sh_list(sh_t *s, TSNode n) {
    sh_touch_t t = {0};
    uint32_t cnt = ts_node_named_child_count(n);
    for (uint32_t i = 0; i < cnt; i++) {
        TSNode c = ts_node_named_child(n, i);
        if (i == 0) {
            sh_walk(s, c);
            continue;
        }
        sh_mark_t m = sh_mark(s);
        sh_walk(s, c);
        sh_clause_end(s, m, &t);
    }
    sh_group_end(s, &t);
}

static void sh_if(sh_t *s, TSNode n) {
    sh_touch_t t = {0};
    sh_mark_t m = {0};
    bool open = false;
    for (uint32_t i = 0; i < ts_node_child_count(n); i++) {
        TSNode c = ts_node_child(n, i);
        if (!ts_node_is_named(c) || sh_type(c, "comment")) {
            continue;
        }
        const char *field = ts_node_field_name_for_child(n, i);
        if (sh_eq(field, "condition")) {
            sh_walk(s, c);
            continue;
        }
        if (sh_type(c, "elif_clause") || sh_type(c, "else_clause")) {
            if (open) {
                sh_clause_end(s, m, &t);
                open = false;
            }
            sh_mark_t mc = sh_mark(s);
            sh_walk_children(s, c);
            sh_clause_end(s, mc, &t);
            continue;
        }
        if (!open) {
            m = sh_mark(s);
            open = true;
        }
        sh_walk(s, c);
    }
    if (open) {
        sh_clause_end(s, m, &t);
    }
    sh_group_end(s, &t);
}

static void sh_case(sh_t *s, TSNode n) {
    TSNode value = ts_node_child_by_field_name(n, TS_FIELD("value"));
    if (!ts_node_is_null(value)) {
        sh_walk_substs(s, value); /* case "$(cmd)" in */
    }
    sh_touch_t t = {0};
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        TSNode item = ts_node_named_child(n, i);
        if (!sh_type(item, "case_item")) {
            continue;
        }
        sh_mark_t m = sh_mark(s);
        for (uint32_t j = 0; j < ts_node_child_count(item); j++) {
            TSNode c = ts_node_child(item, j);
            if (ts_node_is_named(c) && !sh_eq(ts_node_field_name_for_child(item, j), "value")) {
                sh_walk(s, c);
            }
        }
        sh_clause_end(s, m, &t);
    }
    sh_group_end(s, &t);
}

/* A loop's body runs zero or more times, with its variable unknown. */
static void sh_loop(sh_t *s, TSNode n) {
    for (uint32_t i = 0; i < ts_node_child_count(n); i++) {
        if (sh_eq(ts_node_field_name_for_child(n, i), "value")) {
            sh_walk_substs(s, ts_node_child(n, i)); /* for x in $(cmd) */
        }
    }
    sh_touch_t t = {0};
    sh_mark_t m = sh_mark(s);
    TSNode var = ts_node_child_by_field_name(n, TS_FIELD("variable"));
    if (!ts_node_is_null(var)) {
        sh_set(s, sh_text(s, var), &SH_UNKNOWN_VAL, false);
    }
    TSNode cond = ts_node_child_by_field_name(n, TS_FIELD("condition"));
    if (!ts_node_is_null(cond)) {
        sh_walk(s, cond);
    }
    TSNode body = ts_node_child_by_field_name(n, TS_FIELD("body"));
    if (!ts_node_is_null(body)) {
        sh_walk_children(s, body);
    }
    sh_clause_end(s, m, &t);
    sh_group_end(s, &t);
}

/* A function body, read where it is defined: its locals stay inside; the
 * globals it sets and a directory it changes are unknown after it. */
static void sh_function(sh_t *s, TSNode n) {
    TSNode body = ts_node_child_by_field_name(n, TS_FIELD("body"));
    if (ts_node_is_null(body)) {
        return;
    }
    sh_mark_t m = sh_mark(s);
    s->in_func++;
    if (sh_type(body, "compound_statement")) {
        sh_walk_children(s, body);
    } else {
        sh_walk(s, body);
    }
    s->in_func--;
    sh_touch_t t = {0};
    CBMHashTable *locals = cbm_ht_create(0);
    for (int i = m.undo; locals && i < s->nundo; i++) {
        if (s->undo[i].local) {
            cbm_ht_set(locals, s->undo[i].name, (void *)s->undo[i].name);
        }
    }
    for (int i = m.undo; i < s->nundo; i++) {
        if (!locals || !cbm_ht_get(locals, s->undo[i].name)) {
            sh_touch_add(&t, s->undo[i].name);
        }
    }
    if (locals) {
        cbm_ht_free(locals);
    }
    t.cwd = !sh_cwd_same(s->cwd, m.cwd) || s->ndirs != m.ndirs;
    sh_rewind(s, m);
    sh_group_end(s, &t);
}

static void sh_walk(sh_t *s, TSNode n) {
    const char *t = ts_node_type(n);
    if (strcmp(t, "program") == 0 || strcmp(t, "compound_statement") == 0 ||
        strcmp(t, "negated_command") == 0) {
        sh_walk_children(s, n);
    } else if (strcmp(t, "variable_assignment") == 0) {
        sh_assign(s, n, false);
    } else if (strcmp(t, "variable_assignments") == 0) {
        for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
            sh_assign(s, ts_node_named_child(n, i), false);
        }
    } else if (strcmp(t, "declaration_command") == 0) {
        sh_declare(s, n);
    } else if (strcmp(t, "unset_command") == 0) {
        for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
            TSNode c = ts_node_named_child(n, i);
            if (sh_type(c, "variable_name") || sh_type(c, "word")) {
                sh_set(s, sh_text(s, c), &SH_UNKNOWN_VAL, false);
            }
        }
    } else if (strcmp(t, "command") == 0) {
        sh_command(s, n);
    } else if (strcmp(t, "redirected_statement") == 0) {
        TSNode body = ts_node_child_by_field_name(n, TS_FIELD("body"));
        if (!ts_node_is_null(body)) {
            sh_walk(s, body);
        }
        sh_mark_t m = sh_mark(s);
        sh_redirects(s, n);
        sh_rewind(s, m);
    } else if (strcmp(t, "pipeline") == 0) {
        sh_pipeline(s, n);
    } else if (strcmp(t, "list") == 0) {
        sh_list(s, n);
    } else if (strcmp(t, "subshell") == 0) {
        sh_mark_t m = sh_mark(s);
        sh_walk_children(s, n);
        sh_rewind(s, m);
    } else if (strcmp(t, "if_statement") == 0) {
        sh_if(s, n);
    } else if (strcmp(t, "case_statement") == 0) {
        sh_case(s, n);
    } else if (strcmp(t, "for_statement") == 0 || strcmp(t, "c_style_for_statement") == 0 ||
               strcmp(t, "while_statement") == 0) {
        sh_loop(s, n);
    } else if (strcmp(t, "function_definition") == 0) {
        sh_function(s, n);
    } else {
        sh_walk_substs(s, n); /* [[ -n "$(cmd)" ]], ((...)), a bare word */
    }
}

/* ── Files ─────────────────────────────────────────────────────────── */

static bool sh_mentions_deploy_tool(const char *src, size_t n) {
    static const char *const tools[] = {"kubectl", "helm",    "kustomize", "docker",
                                        "podman",  "buildah", "nerdctl"};
    for (size_t i = 0; i < n; i++) {
        char c = src[i] | ' ';
        if (!strchr("khdpbn", c)) {
            continue;
        }
        for (size_t t = 0; t < sizeof(tools) / sizeof(tools[0]); t++) {
            size_t tl = strlen(tools[t]);
            size_t j = 0;
            while (j < tl && i + j < n && (src[i + j] | ' ') == tools[t][j]) {
                j++;
            }
            if (j == tl) {
                return true;
            }
        }
    }
    return false;
}

static char *sh_read(const char *path, size_t *len) {
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > cbm_max_file_bytes()) {
        (void)fclose(f);
        return NULL;
    }
    enum { SH_TS_PAD = 16 }; /* tree-sitter's lexer reads past the end */
    char *buf = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)size + SH_TS_PAD);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t got = fread(buf, SKIP_ONE, (size_t)size, f);
    (void)fclose(f);
    memset(buf + got, 0, SH_TS_PAD);
    *len = got;
    return buf;
}

typedef struct {
    sh_index_t *ix;
    sh_hits_t *hits;
    TSParser *parser;
    bool ix_ready;
    bool ix_ok;
} sh_pass_t;

static void sh_state_init(sh_t *s, sh_pass_t *p, CBMArena *a, const char *rel) {
    *s = (sh_t){.ix = p->ix, .hits = p->hits, .a = a, .file = rel};
    s->env = cbm_ht_create(0);
    const char *dir = sh_dirname(a, rel);
    s->sdir = (dir && strcmp(dir, ".") != 0) ? dir : "";
}

static void sh_state_free(sh_t *s) {
    if (s->env) {
        cbm_ht_free(s->env);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, s->undo);
    cbm_free(CBM_MEM_CLASS_OTHER, s->dirs);
}

/* Parse `text` as bash and walk it. */
static void sh_run(sh_t *s, TSParser *parser, const char *text, size_t len) {
    if (!ts_parser_set_language(parser, cbm_ts_language(CBM_LANG_BASH))) {
        return;
    }
    TSTree *tree = ts_parser_parse_string(parser, NULL, text, (uint32_t)len);
    if (!tree) {
        return;
    }
    s->src = text;
    sh_walk(s, ts_tree_root_node(tree));
    ts_tree_delete(tree);
}

static void sh_script(sh_pass_t *p, const char *rel, const char *src, size_t len) {
    CBMArena a;
    cbm_arena_init(&a);
    sh_t s;
    sh_state_init(&s, p, &a, rel);
    if (s.env) {
        s.script = sh_anchor(&a, rel);
        s.cwd = (sh_cwd_t){SH_FREE, NULL};
        sh_run(&s, p->parser, src, len);
    }
    sh_state_free(&s);
    cbm_arena_destroy(&a);
}

/* ── Make ──────────────────────────────────────────────────────────── */

typedef struct {
    const char *raw;      /* the assigned text */
    const char *expanded; /* memo; NULL: not yet */
    bool known;
    bool busy;
} mk_var_t;

/* One recipe line as make runs it: a tab-led line and its \-continuations,
 * without the @ - + prefixes. */
typedef struct {
    uint32_t b; /* source bytes [b, e) */
    uint32_t e;
    uint32_t row; /* 0-based row of b */
} mk_line_t;

typedef struct mk mk_t;

/* An include directive's expanded file names (mk_closure). */
typedef void (*mk_include_fn)(mk_t *m, const char *names);

struct mk {
    CBMArena *a;
    const char *src;
    CBMHashTable *vars; /* name -> mk_var_t */
    bool makefile;      /* a Makefile (its recipes run in its dir), not an included .mk */
    mk_line_t *lines;
    int nlines;
    int cap_lines;
    mk_include_fn on_include; /* NULL: includes are not followed */
    void *ud;
};

static const char MK_UNKNOWN[] = "${__cbm_unknown}";

static const char *mk_expand(mk_t *m, const char *text);

static const char *mk_lookup(mk_t *m, const char *name) {
    if (strcmp(name, "CURDIR") == 0) {
        return m->makefile ? "${__cbm_curdir}" : MK_UNKNOWN;
    }
    mk_var_t *v = cbm_ht_get(m->vars, name);
    if (!v && strcmp(name, "MAKE") == 0) {
        return "make"; /* make's own command, unless the makefile sets it */
    }
    if (!v || !v->known || v->busy) {
        return MK_UNKNOWN;
    }
    if (!v->expanded) {
        v->busy = true;
        v->expanded = mk_expand(m, v->raw);
        v->busy = false;
    }
    return v->expanded ? v->expanded : MK_UNKNOWN;
}

static const char *mk_trim(CBMArena *a, const char *p, size_t n) {
    while (n && (*p == ' ' || *p == '\t')) {
        p++;
        n--;
    }
    while (n && (p[n - SKIP_ONE] == ' ' || p[n - SKIP_ONE] == '\t')) {
        n--;
    }
    return cbm_arena_strndup(a, p, n);
}

/* `cmd` is exactly the words `words` (NULL-terminated), however spaced. */
static bool mk_words(const char *cmd, const char *const *words) {
    const char *p = cmd;
    for (int i = 0; words[i]; i++) {
        while (*p == ' ' || *p == '\t') {
            p++;
        }
        size_t wl = strlen(words[i]);
        if (strncmp(p, words[i], wl) != 0 || (p[wl] && p[wl] != ' ' && p[wl] != '\t')) {
            return false;
        }
        p += wl;
    }
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    return *p == '\0';
}

/* $(NAME) / ${NAME} / $(shell pwd) / $(abspath X): its expansion. */
static const char *mk_reference(mk_t *m, const char *ref) {
    static const char *const pwd[] = {"pwd", NULL};
    static const char *const pwd_p[] = {"pwd", "-P", NULL};
    static const char *const toplevel[] = {"git", "rev-parse", "--show-toplevel", NULL};
    size_t rl = strcspn(ref, " \t");
    if (!ref[rl]) {
        return mk_lookup(m, ref);
    }
    const char *fn = cbm_arena_strndup(m->a, ref, rl);
    const char *arg = mk_trim(m->a, ref + rl, strlen(ref + rl));
    if (!fn || !arg) {
        return MK_UNKNOWN;
    }
    if (strcmp(fn, "shell") == 0) {
        const char *cmd = mk_expand(m, arg);
        if (cmd && (mk_words(cmd, pwd) || mk_words(cmd, pwd_p))) {
            return m->makefile ? "${__cbm_curdir}" : MK_UNKNOWN;
        }
        if (cmd && mk_words(cmd, toplevel)) {
            return "${__cbm_root}";
        }
        return MK_UNKNOWN;
    }
    if (strcmp(fn, "abspath") == 0 || strcmp(fn, "realpath") == 0) {
        const char *v = mk_expand(m, arg);
        return v && !strchr(v, ' ') ? v : MK_UNKNOWN;
    }
    return MK_UNKNOWN;
}

/* Make's expansion of `text`: references replaced, $$ -> $, what is not
 * known -> a shell variable no line sets. */
static const char *mk_expand(mk_t *m, const char *text) {
    sh_str_t b = {0};
    size_t n = strlen(text);
    size_t i = 0;
    while (i < n && !b.bad) {
        if (text[i] != '$') {
            size_t j = i;
            while (j < n && text[j] != '$') {
                j++;
            }
            sh_str_add(m->a, &b, text + i, j - i);
            i = j;
            continue;
        }
        if (i + SKIP_ONE >= n) {
            sh_str_add(m->a, &b, "$", SKIP_ONE);
            i++;
            continue;
        }
        char c = text[i + SKIP_ONE];
        if (c == '$') {
            sh_str_add(m->a, &b, "$", SKIP_ONE);
            i += PAIR_LEN;
            continue;
        }
        if (c != '(' && c != '{') {
            sh_str_add(m->a, &b, MK_UNKNOWN, strlen(MK_UNKNOWN));
            i += PAIR_LEN;
            continue;
        }
        char close = c == '(' ? ')' : '}';
        int depth = 0;
        size_t j = i + SKIP_ONE;
        for (; j < n; j++) {
            if (text[j] == c) {
                depth++;
            } else if (text[j] == close && --depth == 0) {
                break;
            }
        }
        if (j >= n) {
            sh_str_add(m->a, &b, MK_UNKNOWN, strlen(MK_UNKNOWN));
            break;
        }
        const char *ref = cbm_arena_strndup(m->a, text + i + PAIR_LEN, j - i - PAIR_LEN);
        const char *v = ref ? mk_reference(m, ref) : MK_UNKNOWN;
        sh_str_add(m->a, &b, v, strlen(v));
        i = j + SKIP_ONE;
    }
    return sh_str_done(&b);
}

/* A variable's value as make reads it: each backslash-newline, with the
 * blanks around it, is one space (a value expanded into a recipe line adds
 * no lines). */
static const char *mk_fold(CBMArena *a, const char *p, size_t n) {
    char *out = cbm_arena_alloc(a, n + SKIP_ONE);
    if (!out) {
        return NULL;
    }
    size_t o = 0;
    for (size_t i = 0; i < n; i++) {
        size_t k = i + SKIP_ONE;
        k += k < n && p[k] == '\r';
        if (p[i] != '\\' || k >= n || p[k] != '\n') {
            out[o++] = p[i];
            continue;
        }
        while (o > 0 && (out[o - SKIP_ONE] == ' ' || out[o - SKIP_ONE] == '\t')) {
            o--;
        }
        out[o++] = ' ';
        i = k;
        while (i + SKIP_ONE < n && (p[i + SKIP_ONE] == ' ' || p[i + SKIP_ONE] == '\t')) {
            i++;
        }
    }
    return mk_trim(a, out, o);
}

static void mk_assign(mk_t *m, TSNode n, bool cond) {
    TSNode name = ts_node_child_by_field_name(n, TS_FIELD("name"));
    TSNode value = ts_node_child_by_field_name(n, TS_FIELD("value"));
    if (ts_node_is_null(name)) {
        return;
    }
    uint32_t nb = ts_node_start_byte(name);
    const char *nm = mk_trim(m->a, m->src + nb, ts_node_end_byte(name) - nb);
    const char *raw = "";
    if (!ts_node_is_null(value)) {
        uint32_t vb = ts_node_start_byte(value);
        raw = mk_fold(m->a, m->src + vb, ts_node_end_byte(value) - vb);
    }
    const char *op = "=";
    for (uint32_t i = 0; i < ts_node_child_count(n); i++) {
        TSNode c = ts_node_child(n, i);
        if (!ts_node_is_named(c) && strchr(ts_node_type(c), '=')) {
            op = ts_node_type(c);
        }
    }
    if (!nm || !raw) {
        return;
    }
    mk_var_t *old = cbm_ht_get(m->vars, nm);
    if (strcmp(op, "?=") == 0 && old) {
        return;
    }
    mk_var_t *v = cbm_arena_alloc(m->a, sizeof(*v));
    if (!v) {
        return;
    }
    *v = (mk_var_t){raw, NULL, !cond && strcmp(op, "!=") != 0, false};
    if (strcmp(op, "+=") == 0 && old) {
        v->known = v->known && old->known;
        v->raw = cbm_arena_sprintf(m->a, "%s %s", old->raw, raw);
    }
    cbm_ht_set(m->vars, nm, v);
}

/* Variables and recipe lines of a makefile; `cond`: under a conditional. */
/* The recipe lines in the bytes of a recipe_line node, split as make splits
 * them: tree-sitter-make can let one node run over several lines (a quote in
 * `"Don't ..."` does it). */
static void mk_split_lines(mk_t *m, TSNode node) {
    const char *src = m->src;
    uint32_t p = ts_node_start_byte(node);
    uint32_t end = ts_node_end_byte(node);
    uint32_t row = ts_node_start_point(node).row;
    while (p < end) {
        uint32_t b = p;
        uint32_t brow = row;
        uint32_t q = p;
        for (;;) {
            while (q < end && src[q] != '\n') {
                q++;
            }
            uint32_t k = (q > p && src[q - SKIP_ONE] == '\r') ? q - SKIP_ONE : q;
            bool cont = k > p && src[k - SKIP_ONE] == '\\';
            if (!cont || q >= end) {
                break;
            }
            q++;
            row++;
            p = q;
        }
        uint32_t e = q;
        p = q < end ? q + SKIP_ONE : q;
        row++;
        while (e > b && src[e - SKIP_ONE] == '\r') {
            e--;
        }
        while (b < e && src[b] && strchr(" \t@-+", src[b])) {
            b++;
        }
        if (b < e && src[b] != '#' &&
            sh_grow((void **)&m->lines, &m->cap_lines, m->nlines, sizeof(*m->lines))) {
            m->lines[m->nlines++] = (mk_line_t){b, e, brow};
        }
    }
}

static int mk_cmp_line(const void *a, const void *b) {
    uint32_t x = ((const mk_line_t *)a)->b;
    uint32_t y = ((const mk_line_t *)b)->b;
    return (x > y) - (x < y);
}

static void mk_collect(mk_t *m, TSNode n, bool cond);
static void mk_collect_error(mk_t *m, TSNode err, bool cond);

static void mk_collect_one(mk_t *m, TSNode c, bool cond) {
    const char *t = ts_node_type(c);
    if (strcmp(t, "variable_assignment") == 0) {
        mk_assign(m, c, cond);
    } else if (strcmp(t, "recipe_line") == 0) {
        mk_split_lines(m, c);
    } else if (strcmp(t, "rule") == 0 || strcmp(t, "recipe") == 0) {
        mk_collect(m, c, cond);
    } else if (strcmp(t, "ERROR") == 0) {
        mk_collect_error(m, c, cond);
    } else if (strcmp(t, "define_directive") == 0) {
        TSNode name = ts_node_child_by_field_name(c, TS_FIELD("name"));
        if (!ts_node_is_null(name)) {
            uint32_t nb = ts_node_start_byte(name);
            const char *nm = mk_trim(m->a, m->src + nb, ts_node_end_byte(name) - nb);
            mk_var_t *v = cbm_arena_alloc(m->a, sizeof(*v));
            if (nm && v) {
                *v = (mk_var_t){"", NULL, false, false};
                cbm_ht_set(m->vars, nm, v);
            }
        }
    } else if (strstr(t, "conditional") || strstr(t, "if") == t ||
               strcmp(t, "else_directive") == 0 || strcmp(t, "elsif_directive") == 0) {
        mk_collect(m, c, true);
    } else if (strcmp(t, "include_directive") == 0) {
        TSNode names = ts_node_child_by_field_name(c, TS_FIELD("filenames"));
        if (m->on_include && !ts_node_is_null(names)) {
            uint32_t nb = ts_node_start_byte(names);
            const char *raw = mk_trim(m->a, m->src + nb, ts_node_end_byte(names) - nb);
            const char *v = raw ? mk_expand(m, raw) : NULL;
            if (v) {
                m->on_include(m, v);
            }
        }
    } else if (strstr(t, "directive")) {
        mk_collect(m, c, cond);
    }
}

static void mk_collect(mk_t *m, TSNode n, bool cond) {
    for (uint32_t i = 0; i < ts_node_named_child_count(n); i++) {
        mk_collect_one(m, ts_node_named_child(n, i), cond);
    }
}

/* +1 for a line opening a conditional or a define, -1 for its end, else 0. */
static int mk_directive_delta(const char *p, const char *end) {
    static const struct {
        const char *kw;
        int delta;
    } kws[] = {{"ifeq", SKIP_ONE},   {"ifneq", SKIP_ONE}, {"ifdef", SKIP_ONE}, {"ifndef", SKIP_ONE},
               {"define", SKIP_ONE}, {"endif", -1},       {"endef", -1}};
    while (p < end && (*p == ' ' || *p == '\t')) {
        p++;
    }
    for (size_t i = 0; i < sizeof(kws) / sizeof(kws[0]); i++) {
        size_t kl = strlen(kws[i].kw);
        if ((size_t)(end - p) >= kl && strncmp(p, kws[i].kw, kl) == 0 &&
            (p + kl == end || strchr(" \t(\r", p[kl]))) {
            return kws[i].delta;
        }
    }
    return 0;
}

/* A region tree-sitter-make could not parse (an indented, nested ifeq does
 * it) still holds rules and assignments; an assignment counts as certain
 * only outside the conditionals its lines open. */
static void mk_collect_error(mk_t *m, TSNode err, bool cond) {
    uint32_t scan = ts_node_start_byte(err);
    uint32_t end = ts_node_end_byte(err);
    int depth = 0;
    for (uint32_t i = 0; i < ts_node_named_child_count(err); i++) {
        TSNode c = ts_node_named_child(err, i);
        uint32_t cb = ts_node_start_byte(c);
        while (scan < cb && scan < end) {
            uint32_t le = scan;
            while (le < end && m->src[le] != '\n') {
                le++;
            }
            depth += mk_directive_delta(m->src + scan, m->src + le);
            depth = depth < 0 ? 0 : depth;
            scan = le + SKIP_ONE;
        }
        mk_collect_one(m, c, cond || depth > 0);
    }
}

static void sh_makefile(sh_pass_t *p, const char *rel, const char *src, size_t len) {
    if (!ts_parser_set_language(p->parser, cbm_ts_language(CBM_LANG_MAKEFILE))) {
        return;
    }
    TSTree *tree = ts_parser_parse_string(p->parser, NULL, src, (uint32_t)len);
    if (!tree) {
        return;
    }
    CBMArena a;
    cbm_arena_init(&a);
    const char *base = sh_basename(rel);
    mk_t m = {.a = &a,
              .src = src,
              .vars = cbm_ht_create(0),
              .makefile = strcmp(base, "Makefile") == 0 || strcmp(base, "GNUmakefile") == 0 ||
                          strcmp(base, "makefile") == 0};
    if (m.vars) {
        mk_collect(&m, ts_tree_root_node(tree), false);
    }
    if (m.nlines > SKIP_ONE) {
        qsort(m.lines, (size_t)m.nlines, sizeof(*m.lines), mk_cmp_line);
    }
    for (int r = 0; m.vars && r < m.nlines; r++) {
        if (r > 0 && m.lines[r].b == m.lines[r - SKIP_ONE].b) {
            continue; /* nested nodes covered it already */
        }
        uint32_t b = m.lines[r].b;
        uint32_t e = m.lines[r].e;
        const char *raw = cbm_arena_strndup(&a, src + b, e - b);
        const char *text = raw ? mk_expand(&m, raw) : NULL;
        if (!text || (!strchr(text, '/') && !strstr(text, "sh") && !strstr(text, "make") &&
                      !sh_mentions_deploy_tool(text, strlen(text)))) {
            continue; /* no deploy, program by path, shell or make */
        }
        size_t tl = strlen(text);
        char *buf = cbm_arena_alloc(&a, tl + CBM_SZ_16);
        if (!buf) {
            continue;
        }
        memcpy(buf, text, tl);
        memset(buf + tl, 0, CBM_SZ_16);
        sh_t s;
        sh_state_init(&s, p, &a, rel);
        if (s.env) {
            s.line_base = (int)m.lines[r].row;
            sh_val_t *root = cbm_arena_alloc(&a, sizeof(*root));
            sh_val_t *cur = cbm_arena_alloc(&a, sizeof(*cur));
            if (root && cur) {
                *root = (sh_val_t){SH_ROOT, NULL, 0, NULL};
                *cur = (sh_val_t){sh_anchor(&a, s.sdir), NULL, 0, NULL};
                cbm_ht_set(s.env, "__cbm_root", root);
                if (m.makefile) {
                    cbm_ht_set(s.env, "__cbm_curdir", cur);
                }
            }
            s.cwd = m.makefile ? (sh_cwd_t){SH_AT, s.sdir} : (sh_cwd_t){SH_FREE, NULL};
            sh_run(&s, p->parser, buf, tl);
        }
        sh_state_free(&s);
    }
    if (m.vars) {
        cbm_ht_free(m.vars);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, m.lines);
    cbm_arena_destroy(&a);
    ts_tree_delete(tree);
}

/* ── make targets: the rules a makefile reads (P3.5b) ──────────────── */

typedef struct {
    sh_pass_t *p;
    cbm_pipeline_ctx_t *ctx;
    CBMArena *a;
    CBMHashTable *seen; /* the files read */
    CBMHashTable *vars; /* shared by them, as make shares them */
    const char **order; /* the files, in reading order */
    int n;
    int cap;
    const char *mkdir; /* make's directory ("" = the root) */
} mk_closure_t;

static void mk_closure_file(mk_closure_t *cl, const char *rel);

/* An include directive: each name -- from make's directory ($(CURDIR) is
 * it), or the root ($(shell git rev-parse --show-toplevel)) -- is read in
 * turn, recursively, where the directive stands. A name not fully known is
 * skipped. */
static void mk_closure_include(mk_t *m, const char *names) {
    static const char curdir[] = "${__cbm_curdir}";
    static const char root[] = "${__cbm_root}";
    mk_closure_t *cl = m->ud;
    const char *p = names;
    for (;;) {
        while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') {
            p++;
        }
        size_t wl = strcspn(p, " \t\r\n");
        if (!wl) {
            return;
        }
        const char *w = cbm_arena_strndup(cl->a, p, wl);
        p += wl;
        const char *base = cl->mkdir;
        if (w && strncmp(w, curdir, sizeof(curdir) - SKIP_ONE) == 0) {
            w += sizeof(curdir) - SKIP_ONE;
        } else if (w && strncmp(w, root, sizeof(root) - SKIP_ONE) == 0) {
            w += sizeof(root) - SKIP_ONE;
            base = "";
        } else if (w && w[0] == '/') {
            continue;
        }
        const char *path = w && !strchr(w, '$') ? sh_join(cl->a, base, w) : NULL;
        if (path) {
            mk_closure_file(cl, path);
        }
    }
}

static void mk_closure_file(mk_closure_t *cl, const char *rel) {
    if (cbm_ht_get(cl->seen, rel) ||
        !sh_grow((void **)&cl->order, &cl->cap, cl->n, sizeof(*cl->order))) {
        return;
    }
    cbm_ht_set(cl->seen, rel, (void *)rel);
    cl->order[cl->n++] = rel;
    const char *abs = cbm_arena_sprintf(cl->a, "%s/%s", cl->ctx->repo_path, rel);
    size_t len = 0;
    char *src = abs ? sh_read(abs, &len) : NULL;
    if (!src) {
        return;
    }
    TSTree *tree = NULL;
    if (ts_parser_set_language(cl->p->parser, cbm_ts_language(CBM_LANG_MAKEFILE))) {
        tree = ts_parser_parse_string(cl->p->parser, NULL, src, (uint32_t)len);
    }
    if (tree) {
        mk_t m = {.a = cl->a,
                  .src = src,
                  .vars = cl->vars,
                  .makefile = true,
                  .on_include = mk_closure_include,
                  .ud = cl};
        mk_collect(&m, ts_tree_root_node(tree), false);
        cbm_free(CBM_MEM_CLASS_OTHER, m.lines);
        ts_tree_delete(tree);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, src);
}

static const char *mk_rule_key(CBMArena *a, const char *file, const char *name) {
    return cbm_arena_sprintf(a, "%s\x1f%s", file, name);
}

/* Each make hit's target: the rule node of that name in the makefile, else
 * in the files it includes, in reading order (the first rule of a file).
 * Read here, when linking, so an unchanged makefile is read too. */
static void sh_make_resolve(sh_pass_t *p, cbm_pipeline_ctx_t *ctx, sh_hits_t *h) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMHashTable *closures = cbm_ht_create(0); /* makefile -> mk_closure_t */
    CBMHashTable *want = cbm_ht_create(0);     /* every file a closure reads */
    CBMHashTable *rules = cbm_ht_create(0);    /* file \x1f name -> rule node */
    for (int i = 0; closures && want && i < h->count; i++) {
        const sh_hit_t *x = &h->items[i];
        if (!x->make || cbm_ht_get(closures, x->path)) {
            continue;
        }
        mk_closure_t *cl = cbm_arena_alloc(&a, sizeof(*cl));
        if (!cl) {
            continue;
        }
        const char *dir = sh_dirname(&a, x->path);
        *cl = (mk_closure_t){.p = p,
                             .ctx = ctx,
                             .a = &a,
                             .seen = cbm_ht_create(0),
                             .vars = cbm_ht_create(0),
                             .mkdir = dir && strcmp(dir, ".") != 0 ? dir : ""};
        if (cl->seen && cl->vars) {
            mk_closure_file(cl, x->path);
        }
        for (int k = 0; k < cl->n; k++) {
            cbm_ht_set(want, cl->order[k], (void *)cl->order[k]);
        }
        cbm_ht_set(closures, x->path, cl);
    }
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (rules && want && cbm_ht_count(want) > 0 &&
        cbm_gbuf_find_by_label(ctx->gbuf, "Function", &nodes, &count) == 0) {
        for (int i = 0; i < count; i++) {
            const char *fp = nodes[i]->file_path;
            if (!fp || !nodes[i]->name || !cbm_ht_get(want, fp)) {
                continue;
            }
            const char *key = mk_rule_key(&a, fp, nodes[i]->name);
            const cbm_gbuf_node_t *old = key ? cbm_ht_get(rules, key) : NULL;
            if (key && (!old || nodes[i]->start_line < old->start_line)) {
                cbm_ht_set(rules, key, (void *)nodes[i]);
            }
        }
    }
    for (int i = 0; rules && closures && i < h->count; i++) {
        sh_hit_t *x = &h->items[i];
        const mk_closure_t *cl = x->make ? cbm_ht_get(closures, x->path) : NULL;
        for (int k = 0; cl && k < cl->n && x->tgt <= 0; k++) {
            atomic_fetch_add_explicit(&g_sh_candidates, SKIP_ONE, memory_order_relaxed);
            const char *key = mk_rule_key(&a, cl->order[k], x->ext);
            const cbm_gbuf_node_t *rule = key ? cbm_ht_get(rules, key) : NULL;
            x->tgt = rule ? rule->id : 0;
        }
    }
    for (int i = 0; closures && i < h->count; i++) {
        mk_closure_t *cl = h->items[i].make ? cbm_ht_get(closures, h->items[i].path) : NULL;
        if (cl) {
            cbm_ht_delete(closures, h->items[i].path);
            cbm_ht_free(cl->seen);
            cbm_ht_free(cl->vars);
            cbm_free(CBM_MEM_CLASS_OTHER, (void *)cl->order);
        }
    }
    if (closures) {
        cbm_ht_free(closures);
    }
    if (want) {
        cbm_ht_free(want);
    }
    if (rules) {
        cbm_ht_free(rules);
    }
    cbm_arena_destroy(&a);
}

/* ── Edges ─────────────────────────────────────────────────────────── */

typedef struct {
    const char *file;
    const cbm_gbuf_node_t *node;
} sh_span_t;

static int sh_cmp_span(const void *a, const void *b) {
    const sh_span_t *x = a;
    const sh_span_t *y = b;
    return strcmp(x->file, y->file);
}

typedef struct {
    const cbm_gbuf_node_t **items;
    int count;
    int cap;
} sh_srcs_t;

static int sh_cmp_node_qn(const void *a, const void *b) {
    const cbm_gbuf_node_t *x = *(const cbm_gbuf_node_t *const *)a;
    const cbm_gbuf_node_t *y = *(const cbm_gbuf_node_t *const *)b;
    return strcmp(x->qualified_name ? x->qualified_name : "",
                  y->qualified_name ? y->qualified_name : "");
}

/* The innermost Function around `line` in `file`, else its Module, else its
 * File node. Into `twins` (when given): every Function of exactly that
 * span, in QN order -- the targets of one Make rule share it, and the
 * rule's recipe runs for each of them. */
static int64_t sh_source(cbm_pipeline_ctx_t *ctx, const sh_span_t *spans, int n,
                         CBMHashTable *modules, const char *file, int line, sh_srcs_t *twins) {
    int lo = 0;
    int hi = n;
    while (lo < hi) {
        int mid = lo + (hi - lo) / PAIR_LEN;
        if (strcmp(spans[mid].file, file) < 0) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    const cbm_gbuf_node_t *best = NULL;
    for (int i = lo; i < n && strcmp(spans[i].file, file) == 0; i++) {
        const cbm_gbuf_node_t *f = spans[i].node;
        if (f->start_line > line || f->end_line < line) {
            continue;
        }
        int w = f->end_line - f->start_line;
        int bw = best ? best->end_line - best->start_line : 0;
        if (!best || w < bw ||
            (w == bw && strcmp(f->qualified_name ? f->qualified_name : "",
                               best->qualified_name ? best->qualified_name : "") < 0)) {
            best = f;
        }
    }
    if (best) {
        for (int i = lo; twins && i < n && strcmp(spans[i].file, file) == 0; i++) {
            const cbm_gbuf_node_t *f = spans[i].node;
            if (f->start_line == best->start_line && f->end_line == best->end_line &&
                sh_grow((void **)&twins->items, &twins->cap, twins->count, sizeof(*twins->items))) {
                twins->items[twins->count++] = f;
            }
        }
        if (twins && twins->count > SKIP_ONE) {
            qsort((void *)twins->items, (size_t)twins->count, sizeof(*twins->items),
                  sh_cmp_node_qn);
        }
        return best->id;
    }
    int64_t mod = (int64_t)(intptr_t)cbm_ht_get(modules, file);
    if (mod > 0) {
        return mod;
    }
    const cbm_gbuf_node_t *fn = cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, file);
    return fn ? fn->id : 0;
}

/* The external Chart of `name` a helm command names: the hit keeps no
 * repository URL (a `repo/name` alias, an oci:// reference's last segment),
 * so the per-project one. */
static int64_t sh_external_chart(cbm_pipeline_ctx_t *ctx, const char *name, const char *file) {
    return cbm_pipeline_external_chart(ctx, name, NULL, file);
}

/* A hit's CI context as JSON members (`,"ci":..,"job":..`); "" for none. */
static void sh_ci_props(const sh_hit_t *x, char *out, size_t sz) {
    out[0] = '\0';
    if (!x->ci) {
        return;
    }
    char job[CBM_SZ_512];
    cbm_json_escape(job, (int)sizeof(job), x->job ? x->job : "");
    if (x->job) {
        snprintf(out, sz, ",\"ci\":\"%s\",\"job\":\"%s\"", x->ci, job);
    } else {
        snprintf(out, sz, ",\"ci\":\"%s\"", x->ci);
    }
}

/* SPAWNS to the rule `x->tgt` of a make target, the script `x->tgt` (its
 * Module, else its File) or the Process of `x->ext`, as pass_spawns.c draws
 * it. */
static int sh_link_spawn(cbm_pipeline_ctx_t *ctx, int64_t src, const sh_hit_t *x,
                         CBMHashTable *modules) {
    char program[CBM_SZ_128];
    cbm_json_escape(program, (int)sizeof(program), x->make ? "make" : x->ext);
    char path[CBM_SZ_2K];
    path[0] = '\0';
    int64_t tgt = 0;
    if (x->make) {
        char esc[CBM_SZ_1K];
        char target[CBM_SZ_512];
        cbm_json_escape(esc, (int)sizeof(esc), x->path);
        cbm_json_escape(target, (int)sizeof(target), x->ext);
        snprintf(path, sizeof(path), ",\"path\":\"%s\",\"target\":\"%s\"", esc, target);
        tgt = x->tgt;
    } else if (x->tgt > 0) {
        int64_t mod = (int64_t)(intptr_t)cbm_ht_get(modules, x->path);
        tgt = mod > 0 ? mod : x->tgt;
        char esc[CBM_SZ_1K];
        cbm_json_escape(esc, (int)sizeof(esc), x->path);
        snprintf(path, sizeof(path), ",\"path\":\"%s\"", esc);
    } else {
        char qn[CBM_SZ_256];
        snprintf(qn, sizeof(qn), "__process__%s", x->ext);
        char node_props[CBM_SZ_256];
        snprintf(node_props, sizeof(node_props), "{\"program\":\"%s\"}", program);
        tgt = cbm_gbuf_upsert_node(ctx->gbuf, "Process", x->ext, qn, "", 0, 0, node_props);
    }
    if (tgt <= 0 || tgt == src) {
        return 0;
    }
    char ci[CBM_SZ_1K];
    sh_ci_props(x, ci, sizeof(ci));
    char props[CBM_SZ_4K];
    snprintf(props, sizeof(props),
             "{\"api\":\"%s\",\"program\":\"%s\"%s%s,\"line\":%d,\"confidence\":1.00}", x->via,
             program, path, ci, x->line);
    cbm_gbuf_insert_edge(ctx->gbuf, src, tgt, "SPAWNS", props);
    return SKIP_ONE;
}

/* The edge hit `x` draws from source `src`; 1 when drawn. */
/* An image a build command makes DEPENDS_ON {via:build} the Dockerfile
 * it is built from (edges recomputed whole every run, as the image belongs
 * to no file). */
static int sh_link_build(cbm_pipeline_ctx_t *ctx, const sh_hit_t *x, CBMHashTable *modules) {
    int64_t mod = (int64_t)(intptr_t)cbm_ht_get(modules, x->path);
    int64_t img = mod > 0 ? cbm_pipeline_image_node(ctx, x->ext) : 0;
    if (img <= 0) {
        return 0;
    }
    char esc_file[CBM_SZ_1K];
    char esc_img[CBM_SZ_1K];
    char ci[CBM_SZ_1K];
    cbm_json_escape(esc_file, (int)sizeof(esc_file), x->file);
    cbm_json_escape(esc_img, (int)sizeof(esc_img), x->ext);
    sh_ci_props(x, ci, sizeof(ci));
    /* sized to what it holds: default_of and build_args are never cut */
    size_t pn = strlen(esc_file) + strlen(esc_img) + strlen(ci) +
                (x->default_of ? strlen(x->default_of) : 0) +
                (x->build_args ? strlen(x->build_args) : 0) + CBM_SZ_256;
    char *props = cbm_alloc(CBM_MEM_CLASS_OTHER, pn);
    if (!props) {
        return 0;
    }
    snprintf(props, pn,
             "{\"via\":\"build\",\"tool\":\"docker-build\",\"file\":\"%s\",\"image\":\"%s\"%s,"
             "\"line\":%d%s%s%s%s}",
             esc_file, esc_img, ci, x->line, x->default_of ? ",\"default_of\":" : "",
             x->default_of ? x->default_of : "", x->build_args ? ",\"build_args\":" : "",
             x->build_args ? x->build_args : "");
    cbm_gbuf_insert_edge(ctx->gbuf, img, mod, "DEPENDS_ON", props);
    cbm_free(CBM_MEM_CLASS_OTHER, props);
    return SKIP_ONE;
}

/* A host that is a service of the repository: one DNS label or a
 * `<svc>.<ns>.svc...` name whose first label names a Kubernetes Service of
 * it. Its Route is the path alone, where that service's handler of the path
 * meets it, as code clients do; any other host -- a download, a
 * port-forwarded localhost -- stays in the Route, so it never meets a
 * handler it does not reach. (Not a compose service: the Docker pass makes
 * those after this one in a full run, while a repair holds them already;
 * the QN tells them apart, as a repair's proxies carry no properties.) */
static bool sh_http_service(cbm_pipeline_ctx_t *ctx, const char *host) {
    const char *first_dot = strchr(host, '.');
    const char *svc = first_dot ? strchr(first_dot + SKIP_ONE, '.') : NULL;
    bool dns_form = !first_dot || (svc && sh_starts(svc, ".svc") &&
                                   (svc[strlen(".svc")] == '\0' || svc[strlen(".svc")] == '.'));
    if (!dns_form || strcmp(host, "localhost") == 0) {
        return false;
    }
    char name[CBM_SZ_256];
    snprintf(name, sizeof(name), "Service/%.*s",
             (int)(first_dot ? (size_t)(first_dot - host) : strlen(host)), host);
    const cbm_gbuf_node_t **nodes = NULL;
    int n = 0;
    if (cbm_gbuf_find_by_name(ctx->gbuf, name, &nodes, &n) != 0) {
        return false;
    }
    for (int i = 0; i < n; i++) {
        if (nodes[i]->label && strcmp(nodes[i]->label, "Resource") == 0 &&
            nodes[i]->qualified_name && !strstr(nodes[i]->qualified_name, "::compose/")) {
            return true;
        }
    }
    return false;
}

/* An HTTP request a script makes: HTTP_CALLS {via: curl | wget, method,
 * url_path, host} to its Route (sh_http_service). */
static int sh_link_http(cbm_pipeline_ctx_t *ctx, int64_t src, const sh_hit_t *x) {
    const char *auth = strstr(x->ext, "://");
    if (!auth || src <= 0) {
        return 0;
    }
    auth += strlen("://");
    size_t al = strcspn(auth, "/");
    char host[CBM_SZ_256];
    size_t hl = auth[0] == '[' ? strcspn(auth, "]") + SKIP_ONE : strcspn(auth, ":/");
    if (hl == 0 || hl > al || hl >= sizeof(host)) {
        return 0;
    }
    for (size_t i = 0; i < hl; i++) {
        host[i] = (char)tolower((unsigned char)auth[i]);
    }
    host[hl] = '\0';
    const char *path = auth + al;
    char cpath[CBM_SZ_2K];
    cbm_route_canon_path(path, cpath, sizeof(cpath));
    bool local = sh_http_service(ctx, host);
    char qn[CBM_ROUTE_QN_SIZE];
    snprintf(qn, sizeof(qn), "__route__%s__%.*s%s", x->path, local ? 0 : (int)(auth + al - x->ext),
             x->ext, cpath);
    char authority[CBM_SZ_256]; /* the host with its port */
    if (al >= sizeof(authority)) {
        return 0;
    }
    for (size_t i = 0; i < al; i++) {
        authority[i] = (char)tolower((unsigned char)auth[i]);
    }
    authority[al] = '\0';
    char ehost[CBM_SZ_512];
    char epath[CBM_SZ_4K];
    cbm_json_escape(ehost, (int)sizeof(ehost), authority);
    cbm_json_escape(epath, (int)sizeof(epath), path);
    char rprops[CBM_SZ_1K];
    if (local) {
        snprintf(rprops, sizeof(rprops), "{\"method\":\"%s\"}", x->path);
    } else {
        snprintf(rprops, sizeof(rprops), "{\"method\":\"%s\",\"host\":\"%s\",\"external\":true}",
                 x->path, ehost);
    }
    int64_t route =
        cbm_gbuf_upsert_node(ctx->gbuf, "Route", local ? path : x->ext, qn, "", 0, 0, rprops);
    if (route <= 0 || route == src) {
        return 0;
    }
    char ci[CBM_SZ_1K];
    sh_ci_props(x, ci, sizeof(ci));
    char props[CBM_SZ_4K + CBM_SZ_1K];
    snprintf(props, sizeof(props),
             "{\"via\":\"%s\",\"method\":\"%s\",\"url_path\":\"%s\",\"host\":\"%s\"%s,\"line\":%d}",
             x->via, x->path, epath, ehost, ci, x->line);
    cbm_gbuf_insert_edge(ctx->gbuf, src, route, "HTTP_CALLS", props);
    return SKIP_ONE;
}

static int sh_link_hit(cbm_pipeline_ctx_t *ctx, const sh_hit_t *x, int64_t src,
                       CBMHashTable *modules) {
    if (x->spawn) {
        return src > 0 ? sh_link_spawn(ctx, src, x, modules) : 0;
    }
    if (x->http) {
        return sh_link_http(ctx, src, x);
    }
    int64_t tgt = x->tgt > 0 ? x->tgt : sh_external_chart(ctx, x->ext, x->file);
    if (src <= 0 || tgt <= 0 || src == tgt) {
        return 0;
    }
    char esc[CBM_SZ_1K];
    char ci[CBM_SZ_1K];
    char props[CBM_SZ_4K];
    cbm_json_escape(esc, (int)sizeof(esc), x->path ? x->path : "");
    sh_ci_props(x, ci, sizeof(ci));
    snprintf(props, sizeof(props), "{\"via\":\"%s\",\"%s\":\"%s\"%s%s,\"line\":%d}", x->via,
             x->chart ? "chart" : "path", esc, x->tgt > 0 ? "" : ",\"external\":true", ci, x->line);
    cbm_gbuf_insert_edge(ctx->gbuf, src, tgt, "DEPLOYS", props);
    return SKIP_ONE;
}

static int sh_link(cbm_pipeline_ctx_t *ctx, const sh_hits_t *h) {
    CBMHashTable *files = cbm_ht_create(0);
    CBMHashTable *modules = cbm_ht_create(0);
    if (!files || !modules) {
        if (files) {
            cbm_ht_free(files);
        }
        if (modules) {
            cbm_ht_free(modules);
        }
        return 0;
    }
    for (int i = 0; i < h->count; i++) {
        const sh_hit_t *x = &h->items[i];
        cbm_ht_set(files, x->file, (void *)x->file);
        if ((x->spawn && x->tgt > 0) || x->build) {
            cbm_ht_set(files, x->path,
                       (void *)x->path); /* a spawned script's / Dockerfile's Module */
        }
    }
    sh_span_t *spans = NULL;
    int n = 0;
    int cap = 0;
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_label(ctx->gbuf, "Function", &nodes, &count) == 0) {
        for (int i = 0; i < count; i++) {
            const char *fp = nodes[i]->file_path;
            const char *key = fp ? cbm_ht_get_key(files, fp) : NULL;
            if (key && sh_grow((void **)&spans, &cap, n, sizeof(*spans))) {
                spans[n++] = (sh_span_t){key, nodes[i]};
            }
        }
    }
    if (n > SKIP_ONE) {
        qsort(spans, (size_t)n, sizeof(*spans), sh_cmp_span);
    }
    if (cbm_gbuf_find_by_label(ctx->gbuf, "Module", &nodes, &count) == 0) {
        for (int i = 0; i < count; i++) {
            const char *fp = nodes[i]->file_path;
            const char *key = fp ? cbm_ht_get_key(files, fp) : NULL;
            if (key && !cbm_ht_get(modules, key)) {
                cbm_ht_set(modules, key, (void *)(intptr_t)nodes[i]->id);
            }
        }
    }
    int edges = 0;
    sh_srcs_t twins = {0};
    for (int i = 0; i < h->count; i++) {
        const sh_hit_t *x = &h->items[i];
        if (x->build) {
            edges += sh_link_build(ctx, x, modules);
            continue;
        }
        twins.count = 0;
        int64_t one = sh_source(ctx, spans, n, modules, x->file, x->line, &twins);
        for (int k = 0; k < (twins.count ? twins.count : SKIP_ONE); k++) {
            edges += sh_link_hit(ctx, x, twins.count ? twins.items[k]->id : one, modules);
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)twins.items);
    cbm_free(CBM_MEM_CLASS_OTHER, spans);
    cbm_ht_free(files);
    cbm_ht_free(modules);
    return edges;
}

/* ── CI configurations (pass_ci.c) ─────────────────────────────────── */

typedef struct {
    sh_pass_t *p;
    const char *rel;
    CBMArena *a;
} sh_ci_t;

static void sh_ci_var(sh_t *s, const char *name, const char *value) {
    sh_val_t *v = value ? cbm_arena_alloc(s->a, sizeof(*v)) : NULL;
    if (v) {
        *v = (sh_val_t){value, NULL, 0, NULL};
    }
    sh_set(s, name, v ? v : &SH_UNKNOWN_VAL, false);
}

/* A command line CI runs: evaluated from its directory with its variables;
 * $0 is unknown. */
static void sh_ci_cmd(const cbm_ci_cmd_t *cmd, void *ud) {
    sh_ci_t *x = ud;
    sh_t s;
    sh_state_init(&s, x->p, x->a, x->rel);
    if (s.env) {
        s.line_base = cmd->row;
        s.ci = cmd->ci;
        s.job = cmd->job;
        s.exec = cmd->exec;
        const char *wd = cmd->wd ? sh_join(x->a, "", cmd->wd) : NULL;
        s.cwd = wd ? (sh_cwd_t){SH_AT, wd} : (sh_cwd_t){SH_LOST, NULL};
        if (strcmp(cmd->ci, "github-actions") == 0) {
            sh_ci_var(&s, "GITHUB_WORKSPACE", SH_ROOT);
            if (cmd->action_dir) {
                sh_ci_var(&s, "GITHUB_ACTION_PATH", sh_anchor(x->a, cmd->action_dir));
            }
        }
        for (int i = 0; i < cmd->nenv; i++) {
            sh_ci_var(&s, cmd->env[PAIR_LEN * i], cmd->env[PAIR_LEN * i + SKIP_ONE]);
        }
        sh_run(&s, x->p->parser, cmd->text, cmd->len);
    }
    sh_state_free(&s);
}

static void sh_ci_file(sh_pass_t *p, const char *rel, const char *src, size_t len) {
    CBMArena a;
    cbm_arena_init(&a);
    sh_ci_t x = {.p = p, .rel = rel, .a = &a};
    cbm_ci_commands(&a, rel, src, len, sh_ci_cmd, &x);
    cbm_arena_destroy(&a);
}

/* Prow job configs: the files whose top-level keys (YAML Variables) include
 * presubmits / postsubmits / periodics. */
static CBMHashTable *sh_prow_files(cbm_pipeline_ctx_t *ctx) {
    static const char *const keys[] = {"presubmits", "postsubmits", "periodics", NULL};
    CBMHashTable *out = NULL;
    for (int k = 0; keys[k]; k++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int n = 0;
        if (cbm_gbuf_find_by_name(ctx->gbuf, keys[k], &nodes, &n) != 0) {
            continue;
        }
        for (int i = 0; i < n; i++) {
            const char *fp = nodes[i]->file_path;
            if (!fp || !sh_eq(nodes[i]->label, "Variable")) {
                continue;
            }
            out = out ? out : cbm_ht_create(0);
            if (out) {
                cbm_ht_set(out, fp, (void *)fp);
            }
        }
    }
    return out;
}

/* One script, Makefile or CI configuration `rel` (on disk at `path`). */
static void sh_pass_file(sh_pass_t *p, cbm_pipeline_ctx_t *ctx, CBMHashTable *prow, const char *rel,
                         const char *path, CBMLanguage lang) {
    bool ci = cbm_ci_path(rel) || (prow && cbm_ht_get(prow, rel));
    if (lang != CBM_LANG_BASH && lang != CBM_LANG_MAKEFILE && !ci) {
        return;
    }
    size_t len = 0;
    char *src = sh_read(path, &len);
    if (!src) {
        return;
    }
    if (!p->ix_ready) {
        p->ix_ready = true;
        p->ix_ok = sh_index_build(p->ix, ctx);
        p->parser = ts_parser_new();
    }
    if (p->ix_ok && p->parser) {
        if (ci) {
            sh_ci_file(p, rel, src, len);
        } else if (lang == CBM_LANG_BASH) {
            sh_script(p, rel, src, len);
        } else {
            sh_makefile(p, rel, src, len);
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, src);
}

/* Mark the File node of each file running a container build
 * {"declares_build":true} -- resolved or not: an incremental run reads these
 * files again, as the image build edges are recomputed whole
 * (pipeline_incremental.c). The mark lives and dies with its file's node. */
static void sh_mark_declarers(cbm_pipeline_ctx_t *ctx, CBMHashTable *declarers) {
    static const char MARK[] = "\"declares_build\":true";
    const cbm_gbuf_node_t **nodes = NULL;
    int n = 0;
    if (cbm_ht_count(declarers) == 0 ||
        cbm_gbuf_find_by_label(ctx->gbuf, "File", &nodes, &n) != 0) {
        return;
    }
    for (int i = 0; i < n; i++) {
        cbm_gbuf_node_t *f = (cbm_gbuf_node_t *)nodes[i];
        const char *props = f->properties_json ? f->properties_json : "{}";
        if (!f->file_path || !cbm_ht_get(declarers, f->file_path) || strstr(props, MARK)) {
            continue;
        }
        size_t pl = strlen(props);
        char buf[CBM_SZ_4K];
        bool empty = pl < PAIR_LEN || strcmp(props, "{}") == 0;
        if (props[pl - SKIP_ONE] != '}' || pl + sizeof(MARK) + PAIR_LEN > sizeof(buf)) {
            continue;
        }
        snprintf(buf, sizeof(buf), "%.*s%s%s}", (int)(pl - SKIP_ONE), empty ? "{" : props,
                 empty ? "" : ",", MARK);
        (void)cbm_gbuf_node_set_properties_json(f, buf);
    }
}

void cbm_pipeline_pass_shell(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                             int file_count) {
    sh_index_t ix = {0};
    sh_hits_t hits = {0};
    cbm_arena_init(&hits.arena);
    hits.declarers = cbm_ht_create(0);
    sh_pass_t p = {.ix = &ix, .hits = &hits, .parser = NULL};
    CBMHashTable *prow = sh_prow_files(ctx);
    CBMHashTable *seen = ctx->build_decl_count > 0 ? cbm_ht_create(0) : NULL;
    for (int i = 0; i < file_count; i++) {
        if (seen) {
            cbm_ht_set(seen, files[i].rel_path, (void *)files[i].rel_path);
        }
        sh_pass_file(&p, ctx, prow, files[i].rel_path, files[i].path, files[i].language);
    }
    /* An incremental run recomputes every image's build edges: the unchanged
     * files that declared builds before are read again (pipeline_incremental.c). */
    for (int i = 0; seen && ctx->repo_path && i < ctx->build_decl_count; i++) {
        const char *rel = ctx->build_decl_files[i];
        if (!rel || cbm_ht_get(seen, rel)) {
            continue;
        }
        cbm_ht_set(seen, rel, (void *)rel);
        const char *slash = strrchr(rel, '/');
        CBMLanguage lang = cbm_language_for_filename(slash ? slash + SKIP_ONE : rel);
        char path[CBM_SZ_4K];
        snprintf(path, sizeof(path), "%s/%s", ctx->repo_path, rel);
        sh_pass_file(&p, ctx, prow, rel, path,
                     lang == CBM_LANG_MAKEFILE ? CBM_LANG_MAKEFILE : CBM_LANG_BASH);
    }
    if (seen) {
        cbm_ht_free(seen);
    }
    if (prow) {
        cbm_ht_free(prow);
    }
    if (hits.count > 0 && p.ix_ok && p.parser) {
        sh_make_resolve(&p, ctx, &hits);
    }
    int edges = hits.count > 0 ? sh_link(ctx, &hits) : 0;
    if (hits.declarers) {
        sh_mark_declarers(ctx, hits.declarers);
        cbm_ht_free(hits.declarers);
    }
    if (p.parser) {
        ts_parser_delete(p.parser);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, hits.items);
    cbm_arena_destroy(&hits.arena);
    if (p.ix_ready) {
        sh_index_free(&ix);
    }
    if (edges > 0) {
        char buf[CBM_SZ_32];
        snprintf(buf, sizeof(buf), "%d", edges);
        cbm_log_info("pass.shell.deploys", "edges", buf);
    }
}
