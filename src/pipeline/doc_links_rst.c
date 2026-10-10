/*
 * doc_links_rst.c — reStructuredText references -> MENTIONS edges, via "rst"
 * (the resolving half of internal/cbm/doclink_rst.c).
 *
 * Python domain (roles, object directives, autodoc) as the field test's
 * resolver (H3 PyResolver) reads it. A dotted name is read like Python's
 * import system: its longest prefix that is a module's import name (a .py
 * file, or a package's __init__.py, named from its topmost package
 * directory down) and the rest as a qualified name in that module; a
 * package's re-export (`from .base import Model` in its __init__.py, read
 * from the Python scope blobs of doclink_py.c) continues the lookup at its
 * source. Forms in Sphinx's search order: T, Class.T, module.T,
 * module.Class.T (a leading `.` searches the most specific first). EXACT
 * when one form names a node. Otherwise UNIQUE by name: the one Python
 * definition named like T's last piece whose qualified name ends with T --
 * for a T of two or more pieces only (a bare name is never searched: the
 * field test's errors were bare names), under a module context the one
 * inside the module when several are, and for an object directive (which
 * declares its object IN its module) only inside it.
 *
 * C domain: a C identifier is the whole name -- the one C or C++ definition
 * of that name and kind (H3 CResolver), a member through its struct.
 *
 * Paths: include / literalinclude (relative to the document; `/x` relative
 * to its documentation set: the directory of the nearest conf.py),
 * kernel-include and kernel-doc (relative to the repository root, as the
 * kernel's Sphinx extensions read them), extlink roles whose URL is a
 * repository blob path. A literalinclude's :lines: bind the innermost
 * definition holding them, its :pyobject: the named definition. Inline
 * literals: the Markdown resolver's code-span rules (doc_links_md.c).
 *
 * Unresolved: ambiguous (several), missing (the name's first piece or the
 * path's directory is the repository's), test_only_target (only test code has
 * it), unparseable. Anything else is LOCAL: another project's name (the
 * standard library, an intersphinx name), a bare name no context resolves.
 */
#include "pipeline/doc_links.h"

#include "doclink.h"
#include "foundation/arena.h"
#include "foundation/compat_fs.h" /* cbm_fopen */
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/mem_core.h"
#include "graph_buffer/graph_buffer.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    RR_PATH_CAP = 1024,                    /* the longest path or dotted name looked up */
    RR_INCLUDE_FILE_MAX = 8 * 1024 * 1024, /* an included file read for an open line range */
    RR_RAW_CAP = 4096,                     /* the longest token record read */
    RR_FIELDS = 8,                         /* fields of a token record */
    RR_PIECES = 64,                        /* pieces of a dotted name */
    RR_ALIAS_DEPTH = 12,                   /* re-exports followed (H3) */
    RR_TABLE_INIT = 1024,
};

/* ── Index ───────────────────────────────────────────────────────── */

/* A Python module by its import name. */
typedef struct rr_mod {
    const char *file;    /* its .py path */
    const char *qn_base; /* the QN prefix of its definitions: project + dotted path */
    bool is_pkg;         /* an __init__.py */
    struct rr_mod *next; /* another module of the same import name */
} rr_mod_t;

/* A package's re-export: `from src import orig as alias` (src absolute). */
typedef struct rr_alias {
    const char *alias;
    int level;
    const char *src; /* as written (relative when level > 0) */
    const char *orig;
    struct rr_alias *next;
} rr_alias_t;

/* A Sphinx documentation set: the directory of its conf.py. */
typedef struct {
    const char *dir;
    const char *primary;    /* primary_domain ("py" when unset) */
    const char *const *ext; /* extlink roles to repository paths */
    int next_count;
    const char *const *isx; /* intersphinx names */
    int nisx;
} rr_conf_t;

typedef struct {
    const char *project;
    size_t project_len;
    const char *repo; /* the repository root: an open line range reads its file */
    void *md;         /* the Markdown resolver's index: code spans, segments, folders */
    const char **run_paths;
    int run_count;
    CBMArena arena;
    CBMHashTable *mods;    /* import name -> rr_mod_t list */
    CBMHashTable *roots;   /* first piece of every import name */
    CBMHashTable *aliases; /* __init__.py path -> rr_alias_t list (first per alias) */
    CBMHashTable *modnode; /* .py path -> its Module node */
    rr_conf_t *confs;      /* sorted by dir */
    int nconfs;
} rr_index_t;

static void rr_destroy(void *index);

static bool rr_ends(const char *s, const char *suffix) {
    size_t n = strlen(s);
    size_t k = strlen(suffix);
    return n >= k && strcmp(s + n - k, suffix) == 0;
}

static const char *rr_base(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + SKIP_ONE : path;
}

/* A python source file and its import name (from its topmost package
 * directory: the directories above it holding an __init__.py). */
static bool rr_add_module(rr_index_t *x, const CBMHashTable *pkgdirs, const char *path) {
    size_t n = strlen(path);
    if (n < strlen(".py") + 1 || n >= RR_PATH_CAP) {
        return true;
    }
    char dotted[RR_PATH_CAP];
    size_t stem = n - strlen(".py");
    memcpy(dotted, path, stem);
    dotted[stem] = '\0';
    bool is_pkg = strcmp(rr_base(path), "__init__.py") == 0;
    if (is_pkg) {
        char *slash = strrchr(dotted, '/');
        if (!slash) {
            return true; /* a top-level __init__.py has no package name */
        }
        *slash = '\0';
    }
    /* the topmost package directory: walk up while the parent is a package */
    char dir[RR_PATH_CAP];
    snprintf(dir, sizeof(dir), "%s", dotted);
    char *cut = is_pkg ? dir + strlen(dir) : strrchr(dir, '/');
    size_t start = 0;
    if (!is_pkg && !cut) {
        start = 0; /* a top-level module */
    } else {
        if (cut) {
            *cut = '\0';
        }
        for (;;) {
            if (!cbm_ht_get(pkgdirs, dir)) {
                start = strlen(dir) + (dir[0] ? SKIP_ONE : 0);
                break;
            }
            char *up = strrchr(dir, '/');
            if (!up) {
                start = 0;
                break;
            }
            *up = '\0';
        }
    }
    if (start >= strlen(dotted)) {
        return true;
    }
    char *name = cbm_arena_strdup(&x->arena, dotted + start);
    char *qn = cbm_arena_sprintf(&x->arena, "%s.%s", x->project, dotted);
    rr_mod_t *m = (rr_mod_t *)cbm_arena_alloc(&x->arena, sizeof(*m));
    if (!name || !qn || !m) {
        return false;
    }
    for (char *c = name; *c; c++) {
        *c = *c == '/' ? '.' : *c;
    }
    for (char *c = qn + x->project_len + SKIP_ONE; *c; c++) {
        *c = *c == '/' ? '.' : *c;
    }
    m->file = cbm_arena_strdup(&x->arena, path);
    m->qn_base = qn;
    m->is_pkg = is_pkg;
    m->next = (rr_mod_t *)cbm_ht_get(x->mods, name);
    if (!m->file) {
        return false;
    }
    cbm_ht_set(x->mods, name, m);
    size_t root_len = strcspn(name, ".");
    char *root = cbm_arena_strndup(&x->arena, name, root_len);
    if (!root) {
        return false;
    }
    cbm_ht_set(x->roots, root, root);
    return true;
}

static bool rr_build_modules(rr_index_t *x, const cbm_gbuf_t *graph) {
    const cbm_gbuf_node_t **files = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_label(graph, "File", &files, &count) != 0) {
        return true;
    }
    CBMHashTable *pkgdirs = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, RR_TABLE_INIT);
    if (!pkgdirs) {
        return false;
    }
    bool ok = true;
    for (int i = 0; ok && i < count; i++) {
        const char *fp = files[i]->file_path;
        if (fp && strcmp(rr_base(fp), "__init__.py") == 0 && rr_base(fp) > fp) {
            char *d = cbm_arena_strndup(&x->arena, fp, (size_t)(rr_base(fp) - fp - SKIP_ONE));
            ok = d != NULL;
            if (ok) {
                cbm_ht_set(pkgdirs, d, d);
            }
        }
    }
    for (int i = 0; ok && i < count; i++) {
        const char *fp = files[i]->file_path;
        if (fp && rr_ends(fp, ".py")) {
            ok = rr_add_module(x, pkgdirs, fp);
        }
    }
    cbm_ht_free(pkgdirs);
    const cbm_gbuf_node_t **mods = NULL;
    int nmods = 0;
    if (ok && cbm_gbuf_find_by_label(graph, "Module", &mods, &nmods) == 0) {
        for (int i = 0; i < nmods; i++) {
            const char *fp = mods[i]->file_path;
            if (fp && rr_ends(fp, ".py") && !cbm_ht_get(x->modnode, fp)) {
                cbm_ht_set(x->modnode, fp, (void *)mods[i]); /* the graph outlives the index */
            }
        }
    }
    return ok;
}

/* Split a blob line into TAB-separated fields (in place). */
static int rr_split(char *line, char **f, int cap) {
    int n = 0;
    char *p = line;
    while (n < cap) {
        f[n++] = p;
        char *tab = strchr(p, '\t');
        if (!tab) {
            break;
        }
        *tab = '\0';
        p = tab + SKIP_ONE;
    }
    return n;
}

static int rr_conf_cmp(const void *a, const void *b) {
    return strcmp(((const rr_conf_t *)a)->dir, ((const rr_conf_t *)b)->dir);
}

/* One Python scope blob (doclink_py.c): re-exports and conf.py settings. */
static bool rr_read_scope(rr_index_t *x, const char *path, const char *scope, int *conf_cap) {
    char *copy = cbm_arena_strdup(&x->arena, scope);
    if (!copy) {
        return false;
    }
    rr_conf_t conf = {0};
    bool is_conf = false;
    const char **ext = NULL;
    const char **isx = NULL;
    int next = 0;
    int nisx = 0;
    char *save = NULL;
    for (char *line = strtok_r(copy, "\n", &save); line; line = strtok_r(NULL, "\n", &save)) {
        char *f[RR_FIELDS];
        int nf = rr_split(line, f, RR_FIELDS);
        if (strcmp(f[0], "I") == 0 && nf == 5) {
            rr_alias_t *a = (rr_alias_t *)cbm_arena_alloc(&x->arena, sizeof(*a));
            if (!a) {
                return false;
            }
            *a = (rr_alias_t){.alias = f[4], .level = atoi(f[1]), .src = f[2], .orig = f[3]};
            rr_alias_t *head = (rr_alias_t *)cbm_ht_get(x->aliases, path);
            if (!head) {
                cbm_ht_set(x->aliases, path, a);
            } else {
                rr_alias_t *t = head;
                bool seen = false;
                for (;; t = t->next) {
                    seen = seen || strcmp(t->alias, a->alias) == 0;
                    if (!t->next) {
                        break;
                    }
                }
                if (!seen) {
                    t->next = a; /* the first import of a name binds it (setdefault) */
                }
            }
        } else if (strcmp(f[0], "C") == 0) {
            is_conf = true;
        } else if (strcmp(f[0], "D") == 0 && nf == 2) {
            conf.primary = f[1];
        } else if ((strcmp(f[0], "X") == 0 || strcmp(f[0], "S") == 0) && nf == 2) {
            bool is_ext = f[0][0] == 'X';
            const char ***arr = is_ext ? &ext : &isx;
            int *cnt = is_ext ? &next : &nisx;
            const char **grown =
                (const char **)cbm_arena_alloc(&x->arena, (size_t)(*cnt + 1) * sizeof(char *));
            if (!grown) {
                return false;
            }
            if (*cnt > 0) {
                memcpy(grown, *arr, (size_t)*cnt * sizeof(char *));
            }
            grown[(*cnt)++] = f[1];
            *arr = grown;
        }
    }
    if (!is_conf) {
        return true;
    }
    if (x->nconfs >= *conf_cap) {
        int ncap = *conf_cap ? *conf_cap * PAIR_LEN : RR_FIELDS;
        rr_conf_t *grown = (rr_conf_t *)cbm_arena_alloc(&x->arena, (size_t)ncap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        if (x->nconfs > 0) {
            memcpy(grown, x->confs, (size_t)x->nconfs * sizeof(*grown));
        }
        x->confs = grown;
        *conf_cap = ncap;
    }
    const char *b = rr_base(path);
    conf.dir = cbm_arena_strndup(&x->arena, path, b > path ? (size_t)(b - path - SKIP_ONE) : 0);
    if (!conf.dir) {
        return false;
    }
    conf.primary = conf.primary ? conf.primary : "py";
    conf.ext = ext;
    conf.next_count = next;
    conf.isx = isx;
    conf.nisx = nisx;
    x->confs[x->nconfs++] = conf;
    return true;
}

static void *rr_build(const cbm_doclink_build_in_t *in) {
    rr_index_t *x = (rr_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*x));
    if (!x) {
        return NULL;
    }
    cbm_arena_init(&x->arena);
    x->project = in->ctx ? in->ctx->project_name : NULL;
    x->project_len = x->project ? strlen(x->project) : 0;
    x->repo = in->ctx ? in->ctx->repo_path : NULL;
    x->run_count = in->run_file_count;
    x->run_paths = in->run_file_count > 0
                       ? (const char **)cbm_calloc(CBM_MEM_CLASS_OTHER,
                                                   (size_t)in->run_file_count * sizeof(char *))
                       : NULL;
    x->mods = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, RR_TABLE_INIT);
    x->roots = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, RR_TABLE_INIT);
    x->aliases = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, RR_TABLE_INIT);
    x->modnode = cbm_ht_create_in(CBM_MEM_CLASS_HASH_TABLE, RR_TABLE_INIT);
    x->md = cbm_doclink_md_resolver.build(in);
    if ((in->run_file_count > 0 && !x->run_paths) || !x->mods || !x->roots || !x->aliases ||
        !x->modnode || !x->md || !x->project || !rr_build_modules(x, in->graph)) {
        rr_destroy(x);
        return NULL;
    }
    int conf_cap = 0;
    for (int i = 0; i < in->file_count; i++) {
        const cbm_doclink_file_t *f = &in->files[i];
        if (f->run_file >= 0 && f->run_file < x->run_count) {
            x->run_paths[f->run_file] = f->rel_path; /* valid until destroy */
        }
        if (f->scope && strncmp(f->scope, CBM_DOCLINK_PY_SCOPE_TAG "\n",
                                strlen(CBM_DOCLINK_PY_SCOPE_TAG "\n")) == 0) {
            const char *path = cbm_arena_strdup(&x->arena, f->rel_path);
            if (!path || !rr_read_scope(x, path, f->scope, &conf_cap)) {
                rr_destroy(x);
                return NULL;
            }
        }
    }
    if (x->nconfs > SKIP_ONE) {
        qsort(x->confs, (size_t)x->nconfs, sizeof(*x->confs), rr_conf_cmp);
    }
    return x;
}

static void rr_destroy(void *index) {
    rr_index_t *x = (rr_index_t *)index;
    if (!x) {
        return;
    }
    if (x->md) {
        cbm_doclink_md_resolver.destroy(x->md);
    }
    cbm_ht_free(x->mods);
    cbm_ht_free(x->roots);
    cbm_ht_free(x->aliases);
    cbm_ht_free(x->modnode);
    cbm_arena_destroy(&x->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, x->run_paths);
    cbm_free(CBM_MEM_CLASS_OTHER, x);
}

/* The documentation set a document belongs to: the nearest conf.py at or
 * above its directory; NULL for none. */
static const rr_conf_t *rr_conf_for(const rr_index_t *x, const char *doc) {
    char dir[RR_PATH_CAP];
    const char *b = rr_base(doc);
    size_t dl = b > doc ? (size_t)(b - doc - SKIP_ONE) : 0;
    if (dl >= sizeof(dir)) {
        return NULL;
    }
    memcpy(dir, doc, dl);
    dir[dl] = '\0';
    for (;;) {
        rr_conf_t key = {.dir = dir};
        const rr_conf_t *hit = (const rr_conf_t *)bsearch(&key, x->confs, (size_t)x->nconfs,
                                                          sizeof(*x->confs), rr_conf_cmp);
        if (hit) {
            return hit;
        }
        if (!dir[0]) {
            return NULL;
        }
        char *up = strrchr(dir, '/');
        if (up) {
            *up = '\0';
        } else {
            dir[0] = '\0';
        }
    }
}

static bool rr_listed(const char *const *list, int n, const char *s) {
    for (int i = 0; i < n; i++) {
        if (strcmp(list[i], s) == 0) {
            return true;
        }
    }
    return false;
}

/* ── Outcomes ────────────────────────────────────────────────────── */

typedef enum { RR_NONE = 0, RR_HIT, RR_AMBIGUOUS, RR_TEST_ONLY } rr_kind_t;

typedef struct {
    rr_kind_t kind;
    const cbm_gbuf_node_t *node;
    bool exact;
} rr_res_t;

static void rr_edge(cbm_doclink_outcome_t *out, const cbm_gbuf_node_t *n, bool exact,
                    uint32_t first, uint32_t last) {
    out->kind = CBM_DOCLINK_EDGE;
    out->target = n;
    out->exact = exact;
    out->target_first = first;
    out->target_last = last;
}

static void rr_unresolved(cbm_doclink_outcome_t *out, int reason) {
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = reason;
}

/* The outcome of a lookup. A name the document writes in full binds even
 * test code (`django.test.TestCase` is Django's product); the name searches
 * leave test code out themselves (R4). */
static void rr_finish(cbm_doclink_outcome_t *out, rr_res_t r) {
    if (r.kind == RR_HIT && r.node) {
        rr_edge(out, r.node, r.exact, 0, 0);
    } else if (r.kind == RR_AMBIGUOUS) {
        rr_unresolved(out, CBM_DOCLINK_REASON_AMBIGUOUS);
    } else if (r.kind == RR_TEST_ONLY) {
        rr_unresolved(out, CBM_DOCLINK_REASON_TEST_ONLY);
    }
}

/* ── Python names ────────────────────────────────────────────────── */

static bool rr_word(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' ||
           c >= 0x80;
}

/* PY_TARGET_RE ^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$ */
static bool rr_py_target(const char *t) {
    bool start = true;
    if (!t[0]) {
        return false;
    }
    for (const char *p = t; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '.') {
            if (start) {
                return false;
            }
            start = true;
            continue;
        }
        if (start && c >= '0' && c <= '9') {
            return false;
        }
        if (!rr_word(c)) {
            return false;
        }
        start = false;
    }
    return !start;
}

static const rr_mod_t *rr_module(const rr_index_t *x, const char *name, int *count) {
    const rr_mod_t *m = (const rr_mod_t *)cbm_ht_get(x->mods, name);
    *count = 0;
    for (const rr_mod_t *k = m; k; k = k->next) {
        (*count)++;
    }
    return m;
}

/* One lookup of H3 resolve_dotted: the longest module prefix, the rest by
 * qualified name. RR_NONE with `next` set: a package re-export names the
 * dotted name the lookup continues with. */
static rr_res_t rr_dotted_step(const rr_index_t *x, const cbm_gbuf_t *graph, const char *dotted,
                               char *next, size_t next_cap) {
    rr_res_t none = {RR_NONE, NULL, false};
    next[0] = '\0';
    size_t dlen = strlen(dotted);
    if (dlen == 0 || dlen >= RR_PATH_CAP) {
        return none;
    }
    char buf[RR_PATH_CAP];
    memcpy(buf, dotted, dlen + SKIP_ONE);
    size_t ends[RR_PIECES];
    int np = 0;
    for (size_t i = 0; i <= dlen && np < RR_PIECES; i++) {
        if (buf[i] == '.' || buf[i] == '\0') {
            ends[np++] = i;
        }
    }
    const rr_mod_t *mod = NULL;
    int k = np;
    for (; k > 0; k--) {
        char c = buf[ends[k - 1]];
        buf[ends[k - 1]] = '\0';
        int count = 0;
        mod = rr_module(x, buf, &count);
        buf[ends[k - 1]] = c;
        if (count > SKIP_ONE) {
            return (rr_res_t){RR_AMBIGUOUS, NULL, false};
        }
        if (mod) {
            break;
        }
    }
    if (!mod) {
        return none;
    }
    const char *rest = ends[k - 1] < dlen ? dotted + ends[k - 1] + SKIP_ONE : "";
    if (!rest[0]) {
        const cbm_gbuf_node_t *n = (const cbm_gbuf_node_t *)cbm_ht_get(x->modnode, mod->file);
        if (!n && mod->is_pkg) {
            char dir[RR_PATH_CAP];
            const char *b = rr_base(mod->file);
            size_t l = (size_t)(b - mod->file - SKIP_ONE);
            memcpy(dir, mod->file, l);
            dir[l] = '\0';
            n = cbm_doclink_md_folder(x->md, dir);
        }
        return n ? (rr_res_t){RR_HIT, n, true} : none;
    }
    char qn[RR_PATH_CAP * PAIR_LEN];
    snprintf(qn, sizeof(qn), "%s.%s", mod->qn_base, rest);
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(graph, qn);
    if (n) {
        return (rr_res_t){RR_HIT, n, true};
    }
    if (!mod->is_pkg) {
        return none;
    }
    size_t first_len = strcspn(rest, ".");
    for (const rr_alias_t *a = (const rr_alias_t *)cbm_ht_get(x->aliases, mod->file); a;
         a = a->next) {
        if (strlen(a->alias) != first_len || strncmp(a->alias, rest, first_len) != 0) {
            continue;
        }
        /* the source module, absolute: a relative import counts from the
         * package as written (H3 init_imports) */
        char pkg[RR_PATH_CAP];
        size_t pl = ends[k - 1];
        memcpy(pkg, dotted, pl);
        pkg[pl] = '\0';
        char src[RR_PATH_CAP];
        if (a->level > 0) {
            for (int up = a->level - SKIP_ONE; up > 0; up--) {
                char *dot = strrchr(pkg, '.');
                if (!dot) {
                    pkg[0] = '\0';
                    break;
                }
                *dot = '\0';
            }
            snprintf(src, sizeof(src), "%s%s%s", pkg, pkg[0] && a->src[0] ? "." : "", a->src);
        } else {
            snprintf(src, sizeof(src), "%s", a->src);
        }
        const char *tail = rest + first_len;
        int w = snprintf(next, next_cap, "%s%s%s%s", src, src[0] ? "." : "", a->orig, tail);
        if (w <= 0 || (size_t)w >= next_cap) {
            next[0] = '\0';
        }
        return none; /* the lookup continues at the re-export's source */
    }
    return none;
}

/* H3 resolve_dotted: a module prefix, the rest by qualified name, a package's
 * re-exports followed (at most RR_ALIAS_DEPTH of them, as H3). */
static rr_res_t rr_dotted(const rr_index_t *x, const cbm_gbuf_t *graph, const char *dotted) {
    rr_res_t none = {RR_NONE, NULL, false};
    char cur[RR_PATH_CAP];
    char next[RR_PATH_CAP];
    size_t n = strlen(dotted);
    if (n >= sizeof(cur)) {
        return none;
    }
    memcpy(cur, dotted, n + SKIP_ONE);
    for (int hop = 0; hop <= RR_ALIAS_DEPTH; hop++) {
        rr_res_t r = rr_dotted_step(x, graph, cur, next, sizeof(next));
        if (r.kind != RR_NONE || !next[0]) {
            return r;
        }
        memcpy(cur, next, strlen(next) + SKIP_ONE);
    }
    return none;
}

static const char *const RR_PY_LABELS[] = {"Class", "Function", "Method",    "Variable", "Module",
                                           "Field", "Enum",     "Interface", "Type",     NULL};

/* PY_KIND_LABELS: the labels a role or directive kind may bind by name. */
static bool rr_kind_label(const char *kind, const char *label) {
    static const struct {
        const char *kind;
        const char *labels[3];
    } map[] = {{"class", {"Class"}},
               {"exc", {"Class"}},
               {"exception", {"Class"}},
               {"meth", {"Method"}},
               {"method", {"Method"}},
               {"classmethod", {"Method"}},
               {"staticmethod", {"Method"}},
               {"func", {"Function"}},
               {"function", {"Function"}},
               {"decorator", {"Function"}},
               {"deco", {"Function"}},
               {"decoratormethod", {"Method"}},
               {"attr", {"Variable", "Method"}},
               {"attribute", {"Variable", "Method"}},
               {"property", {"Method", "Variable"}},
               {"data", {"Variable"}},
               {"const", {"Variable"}},
               {"mod", {"Module"}},
               {"module", {"Module"}}};
    for (size_t i = 0; i < sizeof(map) / sizeof(map[0]); i++) {
        if (strcmp(map[i].kind, kind) != 0) {
            continue;
        }
        for (int j = 0; j < 3 && map[i].labels[j]; j++) {
            if (strcmp(map[i].labels[j], label) == 0) {
                return true;
            }
        }
        return false;
    }
    return true; /* obj: any code label */
}

static bool rr_in_labels(const char *label, const char *const *labels) {
    for (int i = 0; label && labels[i]; i++) {
        if (strcmp(label, labels[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* The QN prefix of module M's definitions, written as M; NULL when M is no
 * module of the repository. */
static const char *rr_module_base(const rr_index_t *x, const char *m) {
    int count = 0;
    const rr_mod_t *mod = m && m[0] ? rr_module(x, m, &count) : NULL;
    return count == SKIP_ONE ? mod->qn_base : NULL;
}

static bool rr_under(const char *qn, const char *base) {
    size_t bl = strlen(base);
    return strncmp(qn, base, bl) == 0 && qn[bl] == '.';
}

/* Kinds whose one-piece name may be searched: a class, exception, function or
 * module-level datum is named by itself. A bare member (`:meth:`save``,
 * `:attr:`name``) names a member of SOME class: the field test found 37 of 40
 * such search results wrong without a class context, so it never is. */
static bool rr_bare_searchable(const char *kind) {
    static const char *const kinds[] = {"class", "exc", "func", "data", "const", "deco", NULL};
    for (int i = 0; kinds[i]; i++) {
        if (strcmp(kinds[i], kind) == 0) {
            return true;
        }
    }
    return false;
}

/* H3 unique_candidates, with the field test's fixes: a bare member name is
 * never searched (rr_bare_searchable); an object directive's target lies
 * inside the module it is declared in (its one-piece name only there);
 * test code is no target of a search (R4). */
static rr_res_t rr_unique(const rr_index_t *x, const cbm_gbuf_t *graph, const char *t,
                          const char *kind, const char *m, bool directive) {
    rr_res_t none = {RR_NONE, NULL, false};
    const char *last = strrchr(t, '.');
    const char *mbase = rr_module_base(x, m);
    if (!last && !(directive ? mbase != NULL : rr_bare_searchable(kind))) {
        return none;
    }
    const char *name = last ? last + SKIP_ONE : t;
    char suffix[RR_PATH_CAP];
    if (snprintf(suffix, sizeof(suffix), ".%s", t) >= (int)sizeof(suffix)) {
        return none;
    }
    size_t sl = strlen(suffix);
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_name(graph, name, &nodes, &count) != 0) {
        return none;
    }
    const cbm_gbuf_node_t *hit = NULL;
    int hits = 0;
    const cbm_gbuf_node_t *in_mod = NULL;
    int in_mod_hits = 0;
    int test_hits = 0;
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *n = nodes[i];
        const char *qn = n->qualified_name;
        if (!rr_in_labels(n->label, RR_PY_LABELS) || !rr_kind_label(kind, n->label) ||
            !n->file_path || !rr_ends(n->file_path, ".py") || !qn || strlen(qn) < sl ||
            strcmp(qn + strlen(qn) - sl, suffix) != 0) {
            continue;
        }
        if (cbm_doclink_md_test_path(n->file_path)) {
            test_hits++;
            continue;
        }
        hit = n;
        hits++;
        if (mbase && rr_under(qn, mbase)) {
            in_mod = n;
            in_mod_hits++;
        }
    }
    if (mbase && (directive || hits > SKIP_ONE) && in_mod_hits > 0) {
        hit = in_mod;
        hits = in_mod_hits;
    } else if (directive && mbase) {
        hits = 0; /* declared in its module: never one found outside it */
    }
    if (hits == SKIP_ONE) {
        return (rr_res_t){RR_HIT, hit, false};
    }
    if (hits > SKIP_ONE) {
        return (rr_res_t){RR_AMBIGUOUS, NULL, false};
    }
    return test_hits > 0 ? (rr_res_t){RR_TEST_ONLY, NULL, false} : none;
}

/* H3 PyResolver.resolve: the forms, then the name search. */
static rr_res_t rr_py_resolve(const rr_index_t *x, const cbm_gbuf_t *graph, const char *t,
                              const char *kind, const char *m, const char *c, bool refspecific,
                              bool directive) {
    char forms[4][RR_PATH_CAP];
    int nf = 0;
    bool has_m = m && m[0];
    bool has_c = c && c[0];
    if (refspecific) {
        if (has_m && has_c) {
            snprintf(forms[nf++], RR_PATH_CAP, "%s.%s.%s", m, c, t);
        }
        if (has_m) {
            snprintf(forms[nf++], RR_PATH_CAP, "%s.%s", m, t);
        }
        snprintf(forms[nf++], RR_PATH_CAP, "%s", t);
    } else {
        snprintf(forms[nf++], RR_PATH_CAP, "%s", t);
        if (has_c) {
            snprintf(forms[nf++], RR_PATH_CAP, "%s.%s", c, t);
        }
        if (has_m) {
            snprintf(forms[nf++], RR_PATH_CAP, "%s.%s", m, t);
        }
        if (has_m && has_c) {
            snprintf(forms[nf++], RR_PATH_CAP, "%s.%s.%s", m, c, t);
        }
    }
    /* a module answers only a module's kind: `:meth:`dispatch`` is never the
     * package tests/dispatch (whose import name is `dispatch`) */
    bool module_kind =
        strcmp(kind, "mod") == 0 || strcmp(kind, "module") == 0 || strcmp(kind, "obj") == 0;
    for (int i = 0; i < nf; i++) {
        rr_res_t r = rr_dotted(x, graph, forms[i]);
        if (r.kind == RR_HIT &&
            (module_kind || !r.node->label ||
             (strcmp(r.node->label, "Module") != 0 && strcmp(r.node->label, "Folder") != 0))) {
            return r;
        }
    }
    return rr_unique(x, graph, t, kind, m, directive);
}

/* Not found: the repository's name (a row) or another project's (LOCAL). */
static void rr_py_unfound(const rr_index_t *x, const char *t, cbm_doclink_outcome_t *out) {
    char root[RR_PATH_CAP];
    size_t rl = strcspn(t, ".");
    if (!strchr(t, '.') || rl >= sizeof(root)) {
        out->kind = CBM_DOCLINK_LOCAL; /* a bare name no context resolved */
        return;
    }
    memcpy(root, t, rl);
    root[rl] = '\0';
    if (cbm_ht_get(x->roots, root)) {
        rr_unresolved(out, CBM_DOCLINK_REASON_MISSING);
        return;
    }
    out->kind = CBM_DOCLINK_LOCAL;
}

/* ── Token records ───────────────────────────────────────────────── */

typedef struct {
    char buf[RR_RAW_CAP];
    char *f[RR_FIELDS];
    int n;
} rr_rec_t;

static bool rr_record(const char *raw, rr_rec_t *r) {
    size_t n = strlen(raw);
    if (n >= sizeof(r->buf)) {
        return false;
    }
    memcpy(r->buf, raw, n + SKIP_ONE);
    r->n = rr_split(r->buf, r->f, RR_FIELDS);
    for (int i = r->n; i < RR_FIELDS; i++) {
        r->f[i] = "";
    }
    return true;
}

/* H3 split_domain. */
static void rr_split_domain(const char *name, const char *primary, const char *const *py_members,
                            const char *const *c_members, char *dom, size_t cap,
                            const char **rest) {
    const char *colon = strchr(name, ':');
    if (colon) {
        size_t l = (size_t)(colon - name);
        snprintf(dom, cap, "%.*s", (int)(l < cap ? l : cap - SKIP_ONE), name);
        *rest = colon + SKIP_ONE;
        return;
    }
    const char *const *members = strcmp(primary, "c") == 0    ? c_members
                                 : strcmp(primary, "py") == 0 ? py_members
                                                              : NULL;
    bool in = false;
    for (int i = 0; members && members[i]; i++) {
        in = in || strcmp(members[i], name) == 0;
    }
    snprintf(dom, cap, "%s", in ? primary : "std");
    *rest = name;
}

static const char *const RR_PY_ROLES[] = {"func", "class", "meth",  "attr", "mod", "exc",
                                          "data", "obj",   "const", "deco", NULL};
static const char *const RR_C_ROLES[] = {"func", "macro",  "struct", "union", "enum", "enumerator",
                                         "type", "member", "data",   "var",   NULL};
static const char *const RR_PY_OBJ[] = {
    "class",     "exception", "method",   "classmethod", "staticmethod",
    "attribute", "property",  "function", "decorator",   "decoratormethod",
    "data",      "module",    NULL};
static const char *const RR_PY_DOMAIN_OBJ[] = {
    "class",     "exception", "method",        "classmethod", "staticmethod",
    "attribute", "property",  "function",      "decorator",   "decoratormethod",
    "data",      "module",    "currentmodule", NULL};
static const char *const RR_C_OBJ[] = {"function", "macro",      "type",   "struct", "union",
                                       "enum",     "enumerator", "member", "var",    NULL};
static const char *const RR_C_DOMAIN_OBJ[] = {
    "function", "macro", "type",      "struct",         "union",         "enum", "enumerator",
    "member",   "var",   "namespace", "namespace-push", "namespace-pop", NULL};

static bool rr_member_of(const char *s, const char *const *list) {
    for (int i = 0; list[i]; i++) {
        if (strcmp(list[i], s) == 0) {
            return true;
        }
    }
    return false;
}

/* H3 parse_target: title <target>, then !, ~, a leading . and a trailing (). */
typedef struct {
    char t[RR_PATH_CAP];
    bool bang;
    bool refspecific;
} rr_target_t;

static bool rr_parse_target(const char *content, rr_target_t *out) {
    char c[RR_PATH_CAP];
    size_t w = 0;
    for (const char *p = content; *p && w + SKIP_ONE < sizeof(c); p++) {
        if (*p == '\\' && (p[1] == ' ' || p[1] == '\t')) {
            p++; /* an escaped space joins its neighbours */
            continue;
        }
        c[w++] = *p;
    }
    c[w] = '\0';
    const char *t = c;
    size_t tl = w;
    if (w > 0 && c[w - SKIP_ONE] == '>') {
        char *lt = strrchr(c, '<');
        if (lt && lt > c) {
            /* a title before it: ^(.*?)\s*<([^<>]+)>$ */
            size_t inner = (size_t)(c + w - SKIP_ONE - (lt + SKIP_ONE));
            if (inner > 0 && !memchr(lt + SKIP_ONE, '>', inner)) {
                t = lt + SKIP_ONE;
                tl = inner;
                while (tl > 0 && t[0] == ' ') {
                    t++;
                    tl--;
                }
                while (tl > 0 && t[tl - SKIP_ONE] == ' ') {
                    tl--;
                }
            }
        }
    }
    out->bang = false;
    out->refspecific = false;
    for (int round = 0; round < PAIR_LEN; round++) {
        if (tl > 0 && t[0] == '!') {
            out->bang = true;
            t++;
            tl--;
        }
        if (tl > 0 && t[0] == '~') {
            t++;
            tl--;
        }
    }
    if (tl > 0 && t[0] == '.') {
        out->refspecific = true;
        t++;
        tl--;
    }
    if (tl >= PAIR_LEN && t[tl - PAIR_LEN] == '(' && t[tl - SKIP_ONE] == ')') {
        tl -= PAIR_LEN;
    }
    while (tl > 0 && t[0] == ' ') {
        t++;
        tl--;
    }
    while (tl > 0 && t[tl - SKIP_ONE] == ' ') {
        tl--;
    }
    if (tl >= sizeof(out->t)) {
        return false;
    }
    memcpy(out->t, t, tl);
    out->t[tl] = '\0';
    return true;
}

/* ── C domain ────────────────────────────────────────────────────── */

static bool rr_c_file(const char *path) {
    static const char *const exts[] = {".c",  ".h",   ".cc",  ".cpp", ".cxx",
                                       ".hh", ".hpp", ".hxx", NULL};
    for (int i = 0; path && exts[i]; i++) {
        if (rr_ends(path, exts[i])) {
            return true;
        }
    }
    return false;
}

/* H3 c_is_def: a definition, not a usage or a forward declaration. */
static bool rr_c_def(const cbm_gbuf_node_t *n) {
    static const char *const defining[] = {"Function", "Type", "Variable", "Field", "Method", NULL};
    return rr_in_labels(n->label, defining) || n->end_line > n->start_line;
}

/* Macro nodes are not proxied on the incremental route (pipeline_delta.c), so
 * a C role never binds one: a re-resolved document binds what a full build
 * does. */
static const char *const RR_C_LABELS[] = {"Function", "Class", "Enum", "Struct", "Type", NULL};

static rr_res_t rr_c_name(const cbm_gbuf_t *graph, const char *name, bool data,
                          const char *in_file) {
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    rr_res_t none = {RR_NONE, NULL, false};
    if (cbm_gbuf_find_by_name(graph, name, &nodes, &count) != 0) {
        return none;
    }
    const cbm_gbuf_node_t *hit = NULL;
    int hits = 0;
    const cbm_gbuf_node_t *def = NULL;
    int defs = 0;
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *n = nodes[i];
        const char *fp = n->file_path;
        bool label = rr_in_labels(n->label, RR_C_LABELS) ||
                     (data && n->label && strcmp(n->label, "Variable") == 0) ||
                     (n->label && strcmp(n->label, "Method") == 0 && fp &&
                      (rr_ends(fp, ".c") || rr_ends(fp, ".h")));
        if (!label || !rr_c_file(fp) || (in_file && strcmp(fp, in_file) != 0) ||
            (!in_file && cbm_doclink_md_test_path(fp))) {
            continue;
        }
        if (!in_file && !rr_c_def(n)) {
            continue;
        }
        hit = n;
        hits++;
        if (rr_c_def(n)) {
            def = n;
            defs++;
        }
    }
    if (in_file && hits > SKIP_ONE && defs == SKIP_ONE) {
        return (rr_res_t){RR_HIT, def, true}; /* H3 kd_node: the one definition */
    }
    if (hits == SKIP_ONE) {
        return (rr_res_t){RR_HIT, hit, in_file != NULL};
    }
    return hits > SKIP_ONE ? (rr_res_t){RR_AMBIGUOUS, NULL, false} : none;
}

/* A member through its struct: the one Field `struct.member`. */
static rr_res_t rr_c_member(const cbm_gbuf_t *graph, const char *owner, const char *member) {
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    rr_res_t none = {RR_NONE, NULL, false};
    if (cbm_gbuf_find_by_name(graph, member, &nodes, &count) != 0) {
        return none;
    }
    char suffix[RR_PATH_CAP];
    snprintf(suffix, sizeof(suffix), ".%s.%s", owner ? owner : "", member);
    const cbm_gbuf_node_t *hit = NULL;
    int hits = 0;
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *n = nodes[i];
        if (!n->label || strcmp(n->label, "Field") != 0 || !rr_c_file(n->file_path) ||
            cbm_doclink_md_test_path(n->file_path)) {
            continue;
        }
        if (owner && (!n->qualified_name || !rr_ends(n->qualified_name, suffix))) {
            continue;
        }
        hit = n;
        hits++;
    }
    if (hits == SKIP_ONE) {
        return (rr_res_t){RR_HIT, hit, false};
    }
    return hits > SKIP_ONE ? (rr_res_t){RR_AMBIGUOUS, NULL, false} : none;
}

static bool rr_c_ident(const char *s) {
    if (!s[0] || (s[0] >= '0' && s[0] <= '9')) {
        return false;
    }
    for (const char *p = s; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
              c == '_')) {
            return false;
        }
    }
    return true;
}

/* Is the name the repository's C code at all (a row rather than LOCAL)? */
static bool rr_c_known(const cbm_gbuf_t *graph, const char *name) {
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_name(graph, name, &nodes, &count) != 0) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        if (rr_c_file(nodes[i]->file_path)) {
            return true;
        }
    }
    return false;
}

/* H3 c_clean then c_role: struct/union/enum/typedef dropped, -> read as .,
 * a trailing * or () dropped; a member through its struct. */
static void rr_c_role(const cbm_gbuf_t *graph, const char *doc, const char *rname,
                      const char *target, cbm_doclink_outcome_t *out) {
    char t[RR_PATH_CAP];
    const char *s = target;
    static const char *const kws[] = {"struct ", "union ", "enum ", "typedef ", NULL};
    for (int i = 0; kws[i]; i++) {
        if (strncmp(s, kws[i], strlen(kws[i])) == 0) {
            s += strlen(kws[i]);
            break;
        }
    }
    size_t w = 0;
    for (const char *p = s; *p && w + SKIP_ONE < sizeof(t); p++) {
        if (p[0] == '-' && p[1] == '>') {
            t[w++] = '.';
            p++;
        } else {
            t[w++] = *p;
        }
    }
    while (w > 0 && (t[w - SKIP_ONE] == ' ' || t[w - SKIP_ONE] == '*')) {
        w--;
    }
    t[w] = '\0';
    if (w >= PAIR_LEN && t[w - PAIR_LEN] == '(' && t[w - SKIP_ONE] == ')') {
        t[w - PAIR_LEN] = '\0';
    }
    char *start = t;
    while (*start == ' ') {
        start++;
    }
    rr_res_t r;
    const char *name = start;
    if (strcmp(rname, "member") == 0 || strchr(start, '.')) {
        char *dot = strrchr(start, '.');
        const char *owner = NULL;
        if (dot) {
            /* The owner is the segment before the last dot: back to the dot
             * before it, or to the start of the name. */
            char *seg = dot;
            while (seg > start && seg[-SKIP_ONE] != '.') {
                seg--;
            }
            *dot = '\0';
            owner = seg;
            name = dot + SKIP_ONE;
        }
        if (!rr_c_ident(name) || (owner && !rr_c_ident(owner))) {
            rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
            return;
        }
        r = rr_c_member(graph, owner, name);
    } else {
        if (!rr_c_ident(name)) {
            rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
            return;
        }
        bool data = strcmp(rname, "data") == 0 || strcmp(rname, "var") == 0;
        r = rr_c_name(graph, name, data, NULL);
    }
    if (r.kind == RR_NONE) {
        if (rr_c_known(graph, name)) {
            rr_unresolved(out, CBM_DOCLINK_REASON_MISSING);
        } else {
            out->kind = CBM_DOCLINK_LOCAL;
        }
        return;
    }
    rr_finish(out, r);
}

/* H3 c_sig_name: the declared name of a C object directive's signature. */
static bool rr_c_sig_name(const char *dname, const char *sig, char *out, size_t cap) {
    if (strcmp(dname, "function") == 0) {
        size_t n = strlen(sig);
        /* (*name)( : a function pointer */
        for (size_t i = 0; i < n; i++) {
            if (sig[i] != '(') {
                continue;
            }
            size_t j = i + SKIP_ONE;
            while (j < n && sig[j] == ' ') {
                j++;
            }
            if (j >= n || sig[j] != '*') {
                continue;
            }
            j++;
            while (j < n && sig[j] == ' ') {
                j++;
            }
            size_t a = j;
            while (j < n && (rr_word((unsigned char)sig[j]) && (unsigned char)sig[j] < 0x80)) {
                j++;
            }
            size_t b = j;
            while (j < n && sig[j] == ' ') {
                j++;
            }
            if (b > a && j < n && sig[j] == ')') {
                j++;
                while (j < n && sig[j] == ' ') {
                    j++;
                }
                if (j < n && sig[j] == '(') {
                    snprintf(out, cap, "%.*s", (int)(b - a), sig + a);
                    return true;
                }
            }
        }
        /* ([A-Za-z_]\w*)\s*\( : the first identifier before a parenthesis */
        for (size_t i = 0; i < n; i++) {
            unsigned char c = (unsigned char)sig[i];
            bool start = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
            if (!start || (i > 0 && rr_word((unsigned char)sig[i - SKIP_ONE]) &&
                           (unsigned char)sig[i - SKIP_ONE] < 0x80)) {
                continue;
            }
            size_t j = i;
            while (j < n && rr_word((unsigned char)sig[j]) && (unsigned char)sig[j] < 0x80) {
                j++;
            }
            size_t k = j;
            while (k < n && sig[k] == ' ') {
                k++;
            }
            if (k < n && sig[k] == '(') {
                snprintf(out, cap, "%.*s", (int)(j - i), sig + i);
                return true;
            }
            i = j;
        }
        return false;
    }
    const char *s = sig;
    if (strcmp(dname, "macro") != 0) {
        static const char *const kws[] = {"struct ", "union ", "enum ", "typedef ", NULL};
        for (int i = 0; kws[i]; i++) {
            if (strncmp(s, kws[i], strlen(kws[i])) == 0) {
                s += strlen(kws[i]);
                break;
            }
        }
    }
    /* macro: the leading identifier; others: the last identifier */
    const char *best = NULL;
    size_t best_len = 0;
    for (const char *p = s; *p;) {
        unsigned char c = (unsigned char)*p;
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
            const char *e = p;
            while (*e && rr_word((unsigned char)*e) && (unsigned char)*e < 0x80) {
                e++;
            }
            best = p;
            best_len = (size_t)(e - p);
            if (strcmp(dname, "macro") == 0) {
                break;
            }
            p = e;
        } else {
            if (strcmp(dname, "macro") == 0) {
                break;
            }
            if (rr_word(c) && c < 0x80) { /* digits: part of no identifier start */
                while (*p && rr_word((unsigned char)*p) && (unsigned char)*p < 0x80) {
                    p++;
                }
                continue;
            }
            p++;
        }
    }
    if (!best || best_len >= cap) {
        return false;
    }
    memcpy(out, best, best_len);
    out[best_len] = '\0';
    return true;
}

/* ── Paths ───────────────────────────────────────────────────────── */

/* os.path.normpath(base/p) relative to the repository; false when it leaves
 * the root. */
static bool rr_norm(const char *base, const char *p, char *out, size_t cap) {
    char tmp[RR_PATH_CAP * PAIR_LEN];
    snprintf(tmp, sizeof(tmp), "%s%s%s", base, base[0] && p[0] ? "/" : "", p);
    const char *seg[RR_PIECES * PAIR_LEN];
    size_t len[RR_PIECES * PAIR_LEN];
    int n = 0;
    for (char *s = tmp; *s;) {
        char *e = strchr(s, '/');
        size_t l = e ? (size_t)(e - s) : strlen(s);
        if (l == 0 || (l == SKIP_ONE && s[0] == '.')) {
            /* nothing */
        } else if (l == PAIR_LEN && s[0] == '.' && s[1] == '.') {
            if (n == 0) {
                return false;
            }
            n--;
        } else if (n < (int)(sizeof(seg) / sizeof(seg[0]))) {
            seg[n] = s;
            len[n] = l;
            n++;
        } else {
            return false;
        }
        if (!e) {
            break;
        }
        s = e + SKIP_ONE;
    }
    size_t w = 0;
    for (int i = 0; i < n; i++) {
        if (w + len[i] + PAIR_LEN > cap) {
            return false;
        }
        if (i > 0) {
            out[w++] = '/';
        }
        memcpy(out + w, seg[i], len[i]);
        w += len[i];
    }
    out[w] = '\0';
    return true;
}

static void rr_doc_dir(const char *doc, char *out, size_t cap) {
    const char *b = rr_base(doc);
    size_t l = b > doc ? (size_t)(b - doc - SKIP_ONE) : 0;
    snprintf(out, cap, "%.*s", (int)(l < cap ? l : cap - SKIP_ONE), doc);
}

/* The File node at `rel`; else a row when its directory is the repository's
 * (doc rot), LOCAL otherwise. */
static const cbm_gbuf_node_t *rr_file(const rr_index_t *x, const cbm_gbuf_t *graph, const char *rel,
                                      bool folder_too, cbm_doclink_outcome_t *out) {
    const cbm_gbuf_node_t *f = rel[0] ? cbm_pipeline_file_node(graph, x->project, rel) : NULL;
    if (!f && folder_too && rel[0]) {
        f = cbm_doclink_md_folder(x->md, rel);
    }
    if (f) {
        return f;
    }
    char dir[RR_PATH_CAP];
    rr_doc_dir(rel, dir, sizeof(dir));
    if (dir[0] && cbm_doclink_md_folder(x->md, dir)) {
        rr_unresolved(out, CBM_DOCLINK_REASON_MISSING);
    } else {
        out->kind = CBM_DOCLINK_LOCAL;
    }
    return NULL;
}

/* include / literalinclude: Sphinx reads `/x` from the documentation set's
 * directory and everything else from the document's. */
static bool rr_doc_path(const rr_index_t *x, const char *doc, const char *arg, char *out,
                        size_t cap) {
    if (arg[0] == '/') {
        const rr_conf_t *conf = rr_conf_for(x, doc);
        return rr_norm(conf ? conf->dir : "", arg + SKIP_ONE, out, cap);
    }
    char dir[RR_PATH_CAP];
    rr_doc_dir(doc, dir, sizeof(dir));
    return rr_norm(dir, arg, out, cap);
}

/* An included file's text, read from the indexed file on disk (at most
 * RR_INCLUDE_FILE_MAX bytes). NULL when it cannot be read; the caller frees
 * it (cbm_free, OTHER). */
static char *rr_read(const rr_index_t *x, const char *rel, size_t *n) {
    *n = 0;
    if (!x->repo) {
        return NULL;
    }
    char abs[RR_PATH_CAP * PAIR_LEN];
    snprintf(abs, sizeof(abs), "%s/%s", x->repo, rel);
    FILE *f = cbm_fopen(abs, "rb");
    if (!f) {
        return NULL;
    }
    char *buf = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, RR_INCLUDE_FILE_MAX);
    *n = buf ? fread(buf, SKIP_ONE, RR_INCLUDE_FILE_MAX, f) : 0;
    (void)fclose(f);
    return buf;
}

static uint32_t rr_number(const char **pp) {
    uint32_t v = 0;
    const char *p = *pp;
    while (*p >= '0' && *p <= '9') {
        v = v < UINT32_MAX / 10 ? v * 10 + (uint32_t)(*p - '0') : UINT32_MAX;
        p++;
    }
    *pp = p;
    return v;
}

/* `:lines: 1,3,5-10,20-`: the span of all its ranges. `N-` runs to the file's
 * end (*b = UINT32_MAX) and `-N` from its start. 0 and 0 when it holds none. */
static void rr_lines(const char *v, uint32_t *a, uint32_t *b) {
    *a = 0;
    *b = 0;
    for (const char *p = v; *p;) {
        if (*p == ',' || *p == ' ') {
            p++;
            continue;
        }
        bool has_start = *p >= '0' && *p <= '9';
        uint32_t s = has_start ? rr_number(&p) : 0;
        uint32_t e = s;
        if (*p == '-') {
            p++;
            e = *p >= '0' && *p <= '9' ? rr_number(&p) : UINT32_MAX;
            s = has_start ? s : SKIP_ONE;
        } else if (!has_start) {
            p++; /* not a range: skip the character */
            continue;
        }
        if (s > 0 && e >= s) {
            *a = (*a == 0 || s < *a) ? s : *a;
            *b = e > *b ? e : *b;
        }
    }
}

static void rr_literalinclude(const rr_index_t *x, const cbm_gbuf_t *graph, const char *doc,
                              const rr_rec_t *r, cbm_doclink_outcome_t *out) {
    const char *arg = r->f[1];
    if (!arg[0] || strchr(arg, '$')) {
        rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    char rel[RR_PATH_CAP];
    if (!rr_doc_path(x, doc, arg, rel, sizeof(rel))) {
        out->kind = CBM_DOCLINK_LOCAL;
        return;
    }
    const cbm_gbuf_node_t *file = rr_file(x, graph, rel, false, out);
    if (!file) {
        return;
    }
    const char *lines = r->f[2];
    const char *pyobject = r->f[3];
    if (pyobject[0]) {
        /* H3: the definition `pyobject` of this .py file, by its QN */
        const cbm_gbuf_node_t *n = NULL;
        if (rr_ends(rel, ".py")) {
            char qn[RR_PATH_CAP * PAIR_LEN];
            char dotted[RR_PATH_CAP];
            size_t l = strlen(rel) - strlen(".py");
            snprintf(dotted, sizeof(dotted), "%.*s", (int)l, rel);
            if (rr_ends(dotted, "/__init__")) {
                dotted[strlen(dotted) - strlen("/__init__")] = '\0';
            }
            for (char *c = dotted; *c; c++) {
                *c = *c == '/' ? '.' : *c;
            }
            snprintf(qn, sizeof(qn), "%s.%s.%s", x->project, dotted, pyobject);
            n = cbm_gbuf_find_by_qn(graph, qn);
        }
        if (n) {
            rr_edge(out, n, true, 0, 0);
        } else {
            rr_unresolved(out, CBM_DOCLINK_REASON_MISSING);
        }
        return;
    }
    uint32_t a = 0;
    uint32_t b = 0;
    rr_lines(lines, &a, &b);
    if (a > 0) {
        if (b == UINT32_MAX) {
            /* `N-`: to the file's end, which only its text knows */
            size_t tn = 0;
            char *text = rr_read(x, rel, &tn);
            if (text) {
                uint32_t nl = 0;
                for (size_t i = 0; i < tn; i++) {
                    nl += text[i] == '\n';
                }
                nl += tn > 0 && text[tn - SKIP_ONE] != '\n';
                b = nl >= a ? nl : a;
                cbm_free(CBM_MEM_CLASS_OTHER, text);
            }
        }
        const cbm_gbuf_node_t *seg =
            b == UINT32_MAX ? NULL : cbm_doclink_md_segment(x->md, rel, a, b, NULL);
        rr_edge(out, seg ? seg : file, true, a, b == UINT32_MAX ? 0 : b);
        return;
    }
    rr_edge(out, file, true, 0, 0);
}

/* kernel-doc: the file, or one of its names (H3 kd_node). */
static void rr_kernel_doc(const rr_index_t *x, const cbm_gbuf_t *graph, const char *doc,
                          const rr_rec_t *r, cbm_doclink_outcome_t *out) {
    char rel[RR_PATH_CAP];
    if (!r->f[1][0]) {
        rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    if (!rr_norm("", r->f[1], rel, sizeof(rel))) {
        out->kind = CBM_DOCLINK_LOCAL;
        return;
    }
    const cbm_gbuf_node_t *file = rr_file(x, graph, rel, false, out);
    if (!file) {
        return;
    }
    const char *name = r->f[2];
    if (!name[0]) {
        rr_edge(out, file, true, 0, 0);
        return;
    }
    rr_res_t res = rr_c_name(graph, name, true, rel);
    if (res.kind == RR_NONE && rr_ends(rel, ".h")) {
        /* the comment documents a prototype: its one definition elsewhere */
        res = rr_c_name(graph, name, false, NULL);
    }
    if (res.kind == RR_NONE) {
        rr_unresolved(out, CBM_DOCLINK_REASON_MISSING);
        return;
    }
    rr_finish(out, res);
}

/* ── Resolve ─────────────────────────────────────────────────────── */

static void rr_py_role(const rr_index_t *x, const cbm_gbuf_t *graph, const char *doc,
                       const rr_conf_t *conf, const char *rname, const rr_rec_t *r,
                       cbm_doclink_outcome_t *out) {
    rr_target_t tg;
    if (!rr_parse_target(r->f[2], &tg)) {
        rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    if (tg.bang) {
        out->kind = CBM_DOCLINK_LOCAL; /* `!x`: no link wanted */
        return;
    }
    /* an intersphinx name: `python:dict` */
    const char *colon = strchr(tg.t, ':');
    if (colon && conf) {
        char key[RR_PATH_CAP];
        snprintf(key, sizeof(key), "%.*s", (int)(colon - tg.t), tg.t);
        if (rr_listed(conf->isx, conf->nisx, key)) {
            out->kind = CBM_DOCLINK_LOCAL;
            return;
        }
    }
    if (!rr_py_target(tg.t)) {
        rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    rr_res_t res = rr_py_resolve(x, graph, tg.t, rname, r->f[3], r->f[4], tg.refspecific, false);
    if (res.kind == RR_NONE) {
        rr_py_unfound(x, tg.t, out);
        return;
    }
    rr_finish(out, res);
}

static void rr_py_object(const rr_index_t *x, const cbm_gbuf_t *graph, const char *doc,
                         const char *dname, const rr_rec_t *r, cbm_doclink_outcome_t *out) {
    const char *m = r->f[3];
    if (strcmp(dname, "module") == 0) {
        const char *t = r->f[2];
        if (!rr_py_target(t)) {
            rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
            return;
        }
        rr_res_t res = rr_dotted(x, graph, t);
        if (res.kind == RR_NONE) {
            rr_unresolved(out, CBM_DOCLINK_REASON_MISSING);
            return;
        }
        rr_finish(out, res);
        return;
    }
    const char *fullname = r->f[6];
    if (!fullname[0]) {
        rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    char full[RR_PATH_CAP];
    snprintf(full, sizeof(full), "%s%s%s", m, m[0] ? "." : "", fullname);
    rr_res_t res = rr_dotted(x, graph, full);
    /* `.. py:function:: autofit` declares a definition inside a module: a
     * module, folder or file of that name is another entity of the name */
    if (res.kind == RR_HIT && res.node &&
        (strcmp(res.node->label, "Module") == 0 || strcmp(res.node->label, "Folder") == 0 ||
         strcmp(res.node->label, "File") == 0)) {
        res = (rr_res_t){RR_NONE, NULL, false};
    }
    if (res.kind != RR_HIT) {
        res = rr_py_resolve(x, graph, fullname, dname, m, NULL, false, true);
    }
    if (res.kind == RR_NONE) {
        rr_unresolved(out, CBM_DOCLINK_REASON_MISSING);
        return;
    }
    rr_finish(out, res);
}

static void rr_autodoc(const rr_index_t *x, const cbm_gbuf_t *graph, const char *doc,
                       const rr_rec_t *r, cbm_doclink_outcome_t *out) {
    char t[RR_PATH_CAP];
    snprintf(t, sizeof(t), "%s", r->f[2]);
    char *paren = strchr(t, '(');
    if (paren) {
        *paren = '\0';
    }
    size_t tl = strlen(t);
    while (tl > 0 && t[tl - SKIP_ONE] == ' ') {
        t[--tl] = '\0';
    }
    if (!rr_py_target(t)) {
        rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    const char *m = r->f[3];
    char full[RR_PATH_CAP];
    if (strcmp(r->f[1], "automodule") == 0 || !m[0]) {
        snprintf(full, sizeof(full), "%s", t);
    } else {
        snprintf(full, sizeof(full), "%s.%s", m, t);
    }
    rr_res_t res = rr_dotted(x, graph, full);
    if (res.kind != RR_HIT && strcmp(full, t) != 0) {
        res = rr_dotted(x, graph, t);
    }
    if (res.kind != RR_HIT) {
        rr_unresolved(out, CBM_DOCLINK_REASON_MISSING);
        return;
    }
    rr_finish(out, res);
}

static void rr_extlink(const rr_index_t *x, const cbm_gbuf_t *graph, const rr_rec_t *r,
                       cbm_doclink_outcome_t *out) {
    rr_target_t tg;
    if (!rr_parse_target(r->f[2], &tg) || !tg.t[0]) {
        rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    char rel[RR_PATH_CAP];
    const char *p = tg.t;
    while (*p == '/') {
        p++;
    }
    if (!rr_norm("", p, rel, sizeof(rel))) {
        out->kind = CBM_DOCLINK_LOCAL;
        return;
    }
    const cbm_gbuf_node_t *f = rr_file(x, graph, rel, true, out);
    if (f) {
        rr_edge(out, f, true, 0, 0);
    }
}

static void rr_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                       const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    (void)state;
    const rr_index_t *x = (const rr_index_t *)index;
    out->kind = CBM_DOCLINK_LOCAL;
    const char *doc = (run_file >= 0 && run_file < x->run_count) ? x->run_paths[run_file] : NULL;
    if (!doc || !link->raw) {
        rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    if (link->syntax == CBM_DOCLINK_MD_SUPERSEDES ||
        link->syntax == CBM_DOCLINK_MD_SUPERSEDES_PROSE) {
        /* a reST ADR's own supersedes statement (doc_adr.c) */
        cbm_doclink_md_resolver.resolve(x->md, NULL, run_file, link, graph, out);
        return;
    }
    if (link->syntax == CBM_DOCLINK_RST_CODE_PATH || link->syntax == CBM_DOCLINK_RST_CODE_NAME) {
        CBMDocLink md = *link;
        md.syntax = link->syntax == CBM_DOCLINK_RST_CODE_PATH ? CBM_DOCLINK_MD_CODE_PATH
                                                              : CBM_DOCLINK_MD_CODE_NAME;
        cbm_doclink_md_resolver.resolve(x->md, NULL, run_file, &md, graph, out);
        return;
    }
    rr_rec_t *r = (rr_rec_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*r));
    if (!r) {
        rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        return;
    }
    if (!rr_record(link->raw, r)) {
        rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        cbm_free(CBM_MEM_CLASS_OTHER, r);
        return;
    }
    const rr_conf_t *conf = rr_conf_for(x, doc);
    const char *primary = conf ? conf->primary : "py";
    char dom[RR_PATH_CAP];
    const char *rest = "";
    switch (link->syntax) {
    case CBM_DOCLINK_RST_ROLE:
        rr_split_domain(r->f[1], primary, RR_PY_ROLES, RR_C_ROLES, dom, sizeof(dom), &rest);
        if (strcmp(dom, "py") == 0 && rr_member_of(rest, RR_PY_ROLES)) {
            rr_py_role(x, graph, doc, conf, rest, r, out);
        } else if (strcmp(dom, "c") == 0 && rr_member_of(rest, RR_C_ROLES)) {
            rr_target_t tg;
            if (!rr_parse_target(r->f[2], &tg)) {
                rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
            } else if (!tg.bang) {
                rr_c_role(graph, doc, rest, tg.t, out);
            }
        } else if (strcmp(dom, "std") == 0 && conf &&
                   rr_listed(conf->ext, conf->next_count, rest)) {
            rr_extlink(x, graph, r, out);
        }
        break;
    case CBM_DOCLINK_RST_OBJECT:
        rr_split_domain(r->f[1], primary, RR_PY_DOMAIN_OBJ, RR_C_DOMAIN_OBJ, dom, sizeof(dom),
                        &rest);
        if (strcmp(dom, "py") == 0 && rr_member_of(rest, RR_PY_OBJ)) {
            rr_py_object(x, graph, doc, rest, r, out);
        } else if (strcmp(dom, "c") == 0 && rr_member_of(rest, RR_C_OBJ)) {
            char name[RR_PATH_CAP];
            if (!rr_c_sig_name(rest, r->f[2], name, sizeof(name))) {
                rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
            } else if (strcmp(rest, "member") == 0) {
                rr_res_t res = rr_c_member(graph, NULL, name);
                if (res.kind == RR_NONE) {
                    rr_unresolved(out, CBM_DOCLINK_REASON_MISSING);
                } else {
                    rr_finish(out, res);
                }
            } else {
                bool data = strcmp(rest, "var") == 0;
                rr_res_t res = rr_c_name(graph, name, data, NULL);
                if (res.kind == RR_NONE) {
                    rr_unresolved(out, CBM_DOCLINK_REASON_MISSING);
                } else {
                    rr_finish(out, res);
                }
            }
        }
        break;
    case CBM_DOCLINK_RST_AUTODOC:
        rr_autodoc(x, graph, doc, r, out);
        break;
    case CBM_DOCLINK_RST_INCLUDE: {
        char rel[RR_PATH_CAP];
        const char *arg = r->f[2];
        bool ok = strcmp(r->f[1], "kernel-include") == 0
                      ? rr_norm("", arg, rel, sizeof(rel))
                      : rr_doc_path(x, doc, arg, rel, sizeof(rel));
        if (strchr(arg, '$')) {
            rr_unresolved(out, CBM_DOCLINK_REASON_UNPARSEABLE);
        } else if (ok) {
            const cbm_gbuf_node_t *f = rr_file(x, graph, rel, false, out);
            if (f) {
                rr_edge(out, f, true, 0, 0);
            }
        }
        break;
    }
    case CBM_DOCLINK_RST_LITERALINCLUDE:
        rr_literalinclude(x, graph, doc, r, out);
        break;
    case CBM_DOCLINK_RST_KERNEL_DOC:
        rr_kernel_doc(x, graph, doc, r, out);
        break;
    default:
        break;
    }
    cbm_free(CBM_MEM_CLASS_OTHER, r);
}

const cbm_doclink_resolver_t cbm_doclink_rst_resolver = {
    .langs = {CBM_LANG_RST, CBM_LANG_PYTHON},
    .lang_count = 2,
    .scope_tag = CBM_DOCLINK_PY_SCOPE_TAG,
    .build = rr_build,
    .destroy = rr_destroy,
    .resolve = rr_resolve,
    .via = "rst",
};
