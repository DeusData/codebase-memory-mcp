/*
 * doc_links_ts.c — resolving TypeScript and JavaScript doc references
 * (doclink_ts.c), one index over TS, TSX and JS files, which import each other.
 *
 * A reference is a path: segments joined by `.`, `#` (an instance member) or
 * `~` (a module's member), with an optional `()` or meaning suffix
 * (`:class`, `:0`) that is dropped (one node holds every overload). Its first
 * segment is looked up as the TypeScript checker would see it from the
 * documented declaration, and the field test's prototype does:
 *   1. the declaration's own parameters (dropped at extraction);
 *   2. the namespaces around the reference's line, innermost first;
 *   3. the file's own top-level declarations;
 *   4. the file's imports: named, default and namespace imports, followed
 *      through `export { a as b }`, `export ... from` and `export *` barrels
 *      (a cycle guard and a depth bound; two barrels that answer differently
 *      are ambiguous); a relative specifier resolves with the usual extension
 *      and index rules; a bare one through the nearest tsconfig/jsconfig
 *      `paths` (the first target, as TypeScript tries it first) or
 *      `baseUrl`, else, when it is the `name` of a workspace package (the
 *      pipeline's package map), to that package's entry module (`exports`,
 *      `main`, `module`) when the repository indexes it; every other bare
 *      specifier, a package's subpath included, is external;
 *   5. the standard library's global names: external;
 *   6. the top-level declarations of the repository's script files (files
 *      without an import or an export are global): one binds, several are
 *      ambiguous;
 *   7. the members of the documented declaration's class or interface (of the
 *      declaration itself for a class or interface doc), `this.` included.
 * Nothing found is missing. The other segments walk members (a node's
 * qualified name + `.` + segment) or a module's exports. A member a class,
 * interface or enum declares without a node of its own is a graph gap. Test
 * code binds only from test code. Edges are exact for a name the file
 * declares or imports, unique otherwise.
 */
#include "pipeline/doc_links.h"
#include "pipeline/path_alias.h"
#include "pipeline/pipeline_internal.h" /* cbm_pipeline_file_node, cbm_pipeline_get_pkgmap */
#include "cbm.h"
#include "doclink.h"
#include "helpers.h" /* cbm_fqn_module_source_lang, cbm_is_test_file */
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

enum {
    TR_MAX_DEPTH = 16,
    TR_MAX_SEGS = 16,
    TR_QN_MAX = 2048,
    TR_PATH_MAX = 1024,
    TR_FIELDS = 5,
    TR_HTML_MIN = 11, /* HTML + Element */
    TR_SVG_MIN = 10,  /* SVG + Element */
};

/* ── The index ───────────────────────────────────────────────────── */

typedef struct {
    const char *a; /* local name / exported name */
    const char *spec;
    const char *b; /* orig / local */
    char kind;     /* imports: d n s */
} tr_rec_t;

typedef struct {
    const char *path;
    uint32_t first;
    uint32_t last;
} tr_ns_t;

typedef struct {
    const char *name;
    uint32_t line;  /* the declaration's */
    uint32_t first; /* the function body it is visible in */
    uint32_t last;
} tr_nest_t;

typedef struct {
    const char *path;
    const char *module_qn;
    const char *sym_qn; /* what its declarations' QNs start with: an index file's
                         * module QN without `.index` (fqn.c, strip_init_or_index) */
    bool test;
    bool module_syntax;
    tr_rec_t *imps; /* I */
    int nimps;
    tr_rec_t *stars; /* A: spec */
    int nstars;
    tr_rec_t *star_as; /* S: a = name, spec */
    int nstar_as;
    tr_rec_t *froms; /* F: a = name, spec, b = orig */
    int nfroms;
    tr_rec_t *locals; /* L: a = name, b = local */
    int nlocals;
    tr_rec_t *decls; /* D: a = name, kind e/d flags: 'e', 'd', 'b' (both), '-' */
    int ndecls;
    tr_ns_t *nss;
    int nnss;
    tr_nest_t *nested; /* V: only in a fresh blob, the run's own files */
    int nnested;
} tr_file_t;

typedef struct {
    CBMArena arena;
    const char *project;
    tr_file_t *files;
    int nfiles;
    int *run_to_file;
    int run_count;
    CBMHashTable *by_path;                      /* rel_path -> (file index + 1) as a pointer */
    CBMHashTable *globals;                      /* name -> tr_global_t */
    CBMHashTable *by_module;                    /* module QN -> (first file index + 1) */
    const cbm_path_alias_collection_t *aliases; /* the pipeline's, borrowed */
    CBMHashTable *pkgmap;                       /* the pipeline's, borrowed */
    bool oom;
} tr_index_t;

typedef struct {
    int file;
    int count; /* script files declaring the name */
} tr_global_t;

static const char *tr_copy(tr_index_t *ix, const char *s, size_t n) {
    char *c = cbm_arena_strndup(&ix->arena, s, n);
    ix->oom = ix->oom || !c;
    return c;
}

static bool tr_dash(const char *s, size_t n) {
    return n == SKIP_ONE && s[0] == '-';
}

/* Append one record to a growing arena array. */
static tr_rec_t *tr_push(tr_index_t *ix, tr_rec_t **items, int *count) {
    if (*count == 0 || (*count >= CBM_SZ_4 && (*count & (*count - SKIP_ONE)) == 0)) {
        int ncap = *count ? *count * PAIR_LEN : CBM_SZ_4; /* 4, 8, 16, ... */
        tr_rec_t *g = (tr_rec_t *)cbm_arena_alloc(&ix->arena, (size_t)ncap * sizeof(tr_rec_t));
        if (!g) {
            ix->oom = true;
            return NULL;
        }
        if (*count) {
            memcpy(g, *items, (size_t)*count * sizeof(tr_rec_t));
        }
        *items = g;
    }
    tr_rec_t *r = &(*items)[(*count)++];
    memset(r, 0, sizeof(*r));
    return r;
}

static void tr_push_ns(tr_index_t *ix, tr_file_t *f, const char *path, uint32_t first,
                       uint32_t last) {
    if (f->nnss == 0 || (f->nnss >= CBM_SZ_4 && (f->nnss & (f->nnss - SKIP_ONE)) == 0)) {
        int ncap = f->nnss ? f->nnss * PAIR_LEN : CBM_SZ_4;
        tr_ns_t *g = (tr_ns_t *)cbm_arena_alloc(&ix->arena, (size_t)ncap * sizeof(tr_ns_t));
        if (!g) {
            ix->oom = true;
            return;
        }
        if (f->nnss) {
            memcpy(g, f->nss, (size_t)f->nnss * sizeof(tr_ns_t));
        }
        f->nss = g;
    }
    f->nss[f->nnss++] = (tr_ns_t){path, first, last};
}

static void tr_push_nested(tr_index_t *ix, tr_file_t *f, const char *name, uint32_t line,
                           uint32_t first, uint32_t last) {
    if (f->nnested == 0 ||
        (f->nnested >= CBM_SZ_4 && (f->nnested & (f->nnested - SKIP_ONE)) == 0)) {
        int ncap = f->nnested ? f->nnested * PAIR_LEN : CBM_SZ_4;
        tr_nest_t *g = (tr_nest_t *)cbm_arena_alloc(&ix->arena, (size_t)ncap * sizeof(tr_nest_t));
        if (!g) {
            ix->oom = true;
            return;
        }
        if (f->nnested) {
            memcpy(g, f->nested, (size_t)f->nnested * sizeof(tr_nest_t));
        }
        f->nested = g;
    }
    f->nested[f->nnested++] = (tr_nest_t){name, line, first, last};
}

static void tr_read_record(tr_index_t *ix, tr_file_t *f, const char **fl, const size_t *ln, int n) {
    char tag = fl[0][0];
    tr_rec_t *r = NULL;
    if (tag == 'M') {
        f->module_syntax = true;
    } else if (tag == 'I' && n >= TR_FIELDS && (r = tr_push(ix, &f->imps, &f->nimps))) {
        r->a = tr_copy(ix, fl[1], ln[1]);
        r->kind = fl[PAIR_LEN][0];
        r->spec = tr_copy(ix, fl[CBM_SZ_3], ln[CBM_SZ_3]);
        r->b = tr_dash(fl[CBM_SZ_4], ln[CBM_SZ_4]) ? NULL : tr_copy(ix, fl[CBM_SZ_4], ln[CBM_SZ_4]);
    } else if (tag == 'A' && n >= PAIR_LEN && (r = tr_push(ix, &f->stars, &f->nstars))) {
        r->spec = tr_copy(ix, fl[1], ln[1]);
    } else if (tag == 'S' && n >= CBM_SZ_3 && (r = tr_push(ix, &f->star_as, &f->nstar_as))) {
        r->a = tr_copy(ix, fl[1], ln[1]);
        r->spec = tr_copy(ix, fl[PAIR_LEN], ln[PAIR_LEN]);
    } else if (tag == 'F' && n >= CBM_SZ_4 && (r = tr_push(ix, &f->froms, &f->nfroms))) {
        r->a = tr_copy(ix, fl[1], ln[1]);
        r->spec = tr_copy(ix, fl[PAIR_LEN], ln[PAIR_LEN]);
        r->b = tr_copy(ix, fl[CBM_SZ_3], ln[CBM_SZ_3]);
    } else if (tag == 'L' && n >= CBM_SZ_3 && (r = tr_push(ix, &f->locals, &f->nlocals))) {
        r->a = tr_copy(ix, fl[1], ln[1]);
        r->b = tr_copy(ix, fl[PAIR_LEN], ln[PAIR_LEN]);
    } else if (tag == 'D' && n >= CBM_SZ_3 && (r = tr_push(ix, &f->decls, &f->ndecls))) {
        r->a = tr_copy(ix, fl[1], ln[1]);
        bool e = memchr(fl[PAIR_LEN], 'e', ln[PAIR_LEN]) != NULL;
        bool d = memchr(fl[PAIR_LEN], 'd', ln[PAIR_LEN]) != NULL;
        r->kind = (e && d) ? 'b' : e ? 'e' : d ? 'd' : '-';
    } else if (tag == 'V' && n >= TR_FIELDS) {
        tr_push_nested(ix, f, tr_copy(ix, fl[1], ln[1]),
                       (uint32_t)strtoul(fl[PAIR_LEN], NULL, CBM_DECIMAL_BASE),
                       (uint32_t)strtoul(fl[CBM_SZ_3], NULL, CBM_DECIMAL_BASE),
                       (uint32_t)strtoul(fl[CBM_SZ_4], NULL, CBM_DECIMAL_BASE));
    } else if (tag == 'N' && n >= PAIR_LEN) {
        uint32_t first =
            n >= CBM_SZ_4 ? (uint32_t)strtoul(fl[PAIR_LEN], NULL, CBM_DECIMAL_BASE) : 0;
        uint32_t last = n >= CBM_SZ_4 ? (uint32_t)strtoul(fl[CBM_SZ_3], NULL, CBM_DECIMAL_BASE) : 0;
        tr_push_ns(ix, f, tr_copy(ix, fl[1], ln[1]), first, last);
    }
}

static void tr_read_blob(tr_index_t *ix, tr_file_t *f, const char *blob) {
    const char *line = strchr(blob, '\n'); /* the tag line */
    while (line && !ix->oom) {
        const char *b = line + SKIP_ONE;
        const char *e = strchr(b, '\n');
        if (!e) {
            e = b + strlen(b);
        }
        const char *fl[TR_FIELDS];
        size_t ln[TR_FIELDS];
        int n = 0;
        const char *s = b;
        for (const char *p = b; p <= e && n < TR_FIELDS; p++) {
            if (p == e || *p == '\t') {
                fl[n] = s;
                ln[n] = (size_t)(p - s);
                n++;
                s = p + SKIP_ONE;
            }
        }
        if (n > 0 && ln[0] == SKIP_ONE) {
            tr_read_record(ix, f, fl, ln, n);
        }
        line = *e ? e : NULL;
    }
}

static CBMLanguage tr_lang(const char *path) {
    const char *dot = strrchr(path, '.');
    if (dot && (strcmp(dot, ".ts") == 0 || strcmp(dot, ".mts") == 0 || strcmp(dot, ".cts") == 0)) {
        return CBM_LANG_TYPESCRIPT;
    }
    return dot && strcmp(dot, ".tsx") == 0 ? CBM_LANG_TSX : CBM_LANG_JAVASCRIPT;
}

static void tr_destroy(void *index) {
    tr_index_t *ix = (tr_index_t *)index;
    if (!ix) {
        return;
    }
    cbm_ht_free(ix->by_path);
    cbm_ht_free(ix->globals);
    cbm_ht_free(ix->by_module);
    cbm_free(CBM_MEM_CLASS_OTHER, ix->run_to_file);
    cbm_arena_destroy(&ix->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, ix);
}

static bool tr_add_globals(tr_index_t *ix) {
    ix->globals = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, CBM_SZ_256);
    if (!ix->globals) {
        return false;
    }
    for (int i = 0; i < ix->nfiles; i++) {
        const tr_file_t *f = &ix->files[i];
        if (f->module_syntax) {
            continue; /* a module's names are its own */
        }
        for (int k = 0; k < f->ndecls; k++) {
            tr_global_t *g = (tr_global_t *)cbm_ht_get(ix->globals, f->decls[k].a);
            if (g) {
                g->count += g->file == i ? 0 : SKIP_ONE;
                continue;
            }
            g = (tr_global_t *)cbm_arena_alloc(&ix->arena, sizeof(tr_global_t));
            if (!g) {
                return false;
            }
            *g = (tr_global_t){i, SKIP_ONE};
            cbm_ht_set(ix->globals, f->decls[k].a, g);
        }
    }
    return true;
}

/* The QN prefix of a file's declarations: an index module's QN loses its
 * last segment, as a symbol QN does in fqn.c. */
static const char *tr_symbol_prefix(tr_index_t *ix, const char *module_qn) {
    static const char suffix[] = ".index";
    size_t sl = sizeof(suffix) - SKIP_ONE;
    size_t n = module_qn ? strlen(module_qn) : 0;
    if (n <= sl || strcmp(module_qn + n - sl, suffix) != 0) {
        return module_qn;
    }
    return tr_copy(ix, module_qn, n - sl);
}

static void *tr_build(const cbm_doclink_build_in_t *in) {
    tr_index_t *ix = (tr_index_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*ix));
    if (!ix) {
        return NULL;
    }
    cbm_arena_init(&ix->arena);
    ix->project = in->ctx->project_name;
    ix->nfiles = in->file_count;
    ix->run_count = in->run_file_count;
    ix->files =
        in->file_count
            ? (tr_file_t *)cbm_arena_calloc(&ix->arena, (size_t)in->file_count * sizeof(tr_file_t))
            : NULL;
    ix->run_to_file = (int *)cbm_calloc(CBM_MEM_CLASS_OTHER,
                                        (size_t)(in->run_file_count + SKIP_ONE) * sizeof(int));
    ix->by_path = cbm_ht_create_in(CBM_MEM_CLASS_OTHER, (uint32_t)(in->file_count + SKIP_ONE));
    bool ok = (!in->file_count || ix->files) && ix->run_to_file && ix->by_path;
    for (int r = 0; ok && r < in->run_file_count; r++) {
        ix->run_to_file[r] = CBM_NOT_FOUND;
    }
    for (int i = 0; ok && i < in->file_count && !ix->oom; i++) {
        const cbm_doclink_file_t *src = &in->files[i];
        tr_file_t *f = &ix->files[i];
        f->path = src->rel_path;
        f->module_qn =
            cbm_fqn_module_source_lang(&ix->arena, ix->project, src->rel_path, CBM_LANG_TYPESCRIPT);
        f->test = cbm_is_test_file(src->rel_path, tr_lang(src->rel_path));
        ix->oom = ix->oom || !f->module_qn;
        f->sym_qn = tr_symbol_prefix(ix, f->module_qn);
        cbm_ht_set(ix->by_path, src->rel_path, (void *)(uintptr_t)(i + SKIP_ONE));
        if (src->run_file >= 0 && src->run_file < in->run_file_count) {
            ix->run_to_file[src->run_file] = i;
        }
        if (src->scope) {
            tr_read_blob(ix, f, src->scope);
        }
    }
    ok = ok && !ix->oom && tr_add_globals(ix);
    ix->aliases = in->ctx->path_aliases;
    ix->pkgmap = cbm_pipeline_get_pkgmap();
    ix->by_module =
        ok ? cbm_ht_create_in(CBM_MEM_CLASS_OTHER, (uint32_t)(ix->nfiles + SKIP_ONE)) : NULL;
    ok = ok && ix->by_module;
    for (int i = ix->nfiles - SKIP_ONE; ok && i >= 0; i--) {
        if (ix->files[i].module_qn) {
            cbm_ht_set(ix->by_module, ix->files[i].module_qn, (void *)(uintptr_t)(i + SKIP_ONE));
        }
    }
    if (!ok) {
        cbm_log_error("doc_links.ts.error", "step", "index", "reason", "alloc");
        tr_destroy(ix);
        return NULL;
    }
    char b[PAIR_LEN][CBM_SZ_32];
    snprintf(b[0], sizeof(b[0]), "%d", ix->nfiles);
    snprintf(b[SKIP_ONE], sizeof(b[SKIP_ONE]), "%u", cbm_ht_count(ix->globals));
    cbm_log_info("doc_links.ts.index", "files", b[0], "global_names", b[SKIP_ONE]);
    return ix;
}

/* ── Resolution ──────────────────────────────────────────────────── */

typedef enum {
    TR_NONE = 0, /* nothing found */
    TR_NODE,
    TR_MODULE, /* a file's exports (a namespace import) */
    TR_EXTERNAL,
    TR_AMBIGUOUS,
    TR_GAP, /* declared, without a node */
    TR_MISSING,
    TR_NOT_INDEXED, /* a relative import of a file the index does not hold */
} tr_kind_t;

typedef struct {
    tr_kind_t kind;
    const cbm_gbuf_node_t *node;
    int file;
    bool exact;
} tr_res_t;

typedef struct {
    const tr_index_t *ix;
    const cbm_gbuf_t *g;
    int src;
    cbm_doclink_outcome_t *out;
    int visited[TR_MAX_DEPTH * PAIR_LEN];
    const char *visited_name[TR_MAX_DEPTH * PAIR_LEN];
    int nvisited;
} tr_ctx_t;

static tr_res_t tr_mk(tr_kind_t k) {
    return (tr_res_t){k, NULL, CBM_NOT_FOUND, false};
}

/* The node at `qn`, when it belongs to file f (any file when f is NULL). */
static const cbm_gbuf_node_t *tr_node(const tr_ctx_t *c, const char *qn, const tr_file_t *f) {
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(c->g, qn);
    if (!n || !n->label) {
        return NULL;
    }
    if (f && (!n->file_path || strcmp(n->file_path, f->path) != 0)) {
        return NULL;
    }
    return n;
}

static bool tr_join(char *out, size_t cap, const char *a, const char *b, const char *c) {
    int n = c ? snprintf(out, cap, "%s.%s.%s", a, b, c) : snprintf(out, cap, "%s.%s", a, b);
    return n > 0 && (size_t)n < cap;
}

static const tr_rec_t *tr_decl(const tr_file_t *f, const char *name) {
    for (int i = 0; i < f->ndecls; i++) {
        if (strcmp(f->decls[i].a, name) == 0) {
            return &f->decls[i];
        }
    }
    return NULL;
}

/* A top-level declaration of file f: its node, or a graph gap. */
static tr_res_t tr_decl_res(const tr_ctx_t *c, const tr_file_t *f, const char *name) {
    char qn[TR_QN_MAX];
    if (!tr_join(qn, sizeof(qn), f->sym_qn, name, NULL)) {
        return tr_mk(TR_GAP);
    }
    const cbm_gbuf_node_t *n = tr_node(c, qn, f);
    tr_res_t r = tr_mk(n ? TR_NODE : TR_GAP);
    r.node = n;
    return r;
}

static int tr_file_at(const tr_index_t *ix, const char *path) {
    void *v = cbm_ht_get(ix->by_path, path);
    return v ? (int)((uintptr_t)v - SKIP_ONE) : CBM_NOT_FOUND;
}

/* Normalize dir + "/" + rel into out ("." and ".." resolved); false when it
 * leaves the repository. */
static bool tr_norm(const char *dir, size_t dl, const char *rel, char *out, size_t cap) {
    char buf[TR_PATH_MAX];
    int n = dl ? snprintf(buf, sizeof(buf), "%.*s/%s", (int)dl, dir, rel)
               : snprintf(buf, sizeof(buf), "%s", rel);
    if (n <= 0 || (size_t)n >= sizeof(buf)) {
        return false;
    }
    size_t w = 0;
    for (char *seg = buf; *seg;) {
        char *slash = strchr(seg, '/');
        size_t sl = slash ? (size_t)(slash - seg) : strlen(seg);
        if (sl == PAIR_LEN && seg[0] == '.' && seg[SKIP_ONE] == '.') {
            if (w == 0) {
                return false;
            }
            while (w > 0 && out[w - SKIP_ONE] != '/') {
                w--;
            }
            w = w > 0 ? w - SKIP_ONE : 0;
        } else if (!(sl == SKIP_ONE && seg[0] == '.') && sl > 0) {
            if (w > 0) {
                out[w++] = '/';
            }
            if (w + sl + SKIP_ONE >= cap) {
                return false;
            }
            memcpy(out + w, seg, sl);
            w += sl;
        }
        seg = slash ? slash + SKIP_ONE : seg + sl;
    }
    out[w] = '\0';
    return w > 0;
}

/* The file a repository-relative module path names: the path itself, the
 * path with a module extension, or its index file. */
static int tr_path_file(const tr_index_t *ix, const char *base) {
    static const char *const strip[] = {".js", ".jsx", ".mjs", ".cjs", ".ts", ".tsx", NULL};
    static const char *const exts[] = {".ts", ".tsx", ".d.ts", ".mts", ".cts",
                                       ".js", ".jsx", ".mjs",  ".cjs", NULL};
    static const char *const index[] = {"/index.ts", "/index.tsx", "/index.d.ts", "/index.js",
                                        NULL};
    int hit = tr_file_at(ix, base);
    if (hit >= 0) {
        return hit;
    }
    char stem[TR_PATH_MAX];
    snprintf(stem, sizeof(stem), "%s", base);
    size_t bl = strlen(stem);
    for (int i = 0; strip[i]; i++) {
        size_t el = strlen(strip[i]);
        if (bl > el && strcmp(stem + bl - el, strip[i]) == 0) {
            stem[bl - el] = '\0';
            break;
        }
    }
    char cand[TR_PATH_MAX + CBM_SZ_16];
    for (int i = 0; exts[i] && hit < 0; i++) {
        snprintf(cand, sizeof(cand), "%s%s", stem, exts[i]);
        hit = tr_file_at(ix, cand);
    }
    for (int i = 0; index[i] && hit < 0; i++) {
        snprintf(cand, sizeof(cand), "%s%s", base, index[i]);
        hit = tr_file_at(ix, cand);
    }
    return hit;
}

/* The workspace package a bare specifier names (`name` or `@scope/name`, no
 * subpath): the file of its entry module, as the pipeline's package map
 * holds it. */
static int tr_package_file(const tr_index_t *ix, const char *spec) {
    const char *slash = strchr(spec, '/');
    if (spec[0] == '@' && slash) {
        slash = strchr(slash + SKIP_ONE, '/');
    }
    if (slash || !ix->pkgmap || !ix->by_module) {
        return CBM_NOT_FOUND;
    }
    const char *qn = (const char *)cbm_ht_get(ix->pkgmap, spec);
    void *v = qn ? cbm_ht_get(ix->by_module, qn) : NULL;
    return v ? (int)((uintptr_t)v - SKIP_ONE) : CBM_NOT_FOUND;
}

/* A bare specifier the repository answers itself: the nearest
 * tsconfig/jsconfig `paths` or `baseUrl`, else a workspace package. */
static int tr_bare_file(const tr_index_t *ix, int from, const char *spec) {
    const cbm_path_alias_map_t *map =
        ix->aliases ? cbm_path_alias_find_for_file(ix->aliases, ix->files[from].path) : NULL;
    char aliased[TR_PATH_MAX];
    if (map && cbm_path_alias_resolve_into(map, spec, aliased, sizeof(aliased))) {
        int hit = tr_path_file(ix, aliased);
        if (hit >= 0) {
            return hit;
        }
    }
    return tr_package_file(ix, spec);
}

/* The file a module specifier written in file `from` names: -1 with
 * *external for a bare specifier the repository does not answer (a
 * package). */
static int tr_spec_file(const tr_index_t *ix, int from, const char *spec, bool *external) {
    *external = false;
    if (!(spec[0] == '.' &&
          (spec[SKIP_ONE] == '/' || spec[SKIP_ONE] == '\0' ||
           (spec[SKIP_ONE] == '.' && (spec[PAIR_LEN] == '/' || spec[PAIR_LEN] == '\0'))))) {
        int hit = tr_bare_file(ix, from, spec);
        *external = hit < 0;
        return hit;
    }
    const char *path = ix->files[from].path;
    const char *slash = strrchr(path, '/');
    char base[TR_PATH_MAX];
    if (!tr_norm(path, slash ? (size_t)(slash - path) : 0, spec, base, sizeof(base))) {
        return CBM_NOT_FOUND;
    }
    return tr_path_file(ix, base);
}

static tr_res_t tr_export_of(tr_ctx_t *c, int file, const char *name, int depth);

/* What a specifier's file exports as `name`. */
static tr_res_t tr_from(tr_ctx_t *c, int file, const char *spec, const char *name, int depth) {
    bool external = false;
    int f = tr_spec_file(c->ix, file, spec, &external);
    if (f < 0) {
        return tr_mk(external ? TR_EXTERNAL : TR_NOT_INDEXED);
    }
    if (!name) {
        tr_res_t r = tr_mk(TR_MODULE);
        r.file = f;
        return r;
    }
    return tr_export_of(c, f, name, depth + SKIP_ONE);
}

/* A name bound at the top level of file `file`: its declaration or import. */
static tr_res_t tr_top_binding(tr_ctx_t *c, int file, const char *name, int depth) {
    const tr_file_t *f = &c->ix->files[file];
    if (tr_decl(f, name)) {
        tr_res_t r = tr_decl_res(c, f, name);
        r.exact = true;
        return r;
    }
    for (int i = 0; i < f->nimps; i++) {
        const tr_rec_t *im = &f->imps[i];
        if (strcmp(im->a, name) != 0) {
            continue;
        }
        tr_res_t r = im->kind == 's'   ? tr_from(c, file, im->spec, NULL, depth)
                     : im->kind == 'd' ? tr_from(c, file, im->spec, "default", depth)
                                       : tr_from(c, file, im->spec, im->b ? im->b : name, depth);
        r.exact = r.kind == TR_NODE;
        return r;
    }
    return tr_mk(TR_NONE);
}

static bool tr_seen(tr_ctx_t *c, int file, const char *name) {
    for (int i = 0; i < c->nvisited; i++) {
        if (c->visited[i] == file && strcmp(c->visited_name[i], name) == 0) {
            return true;
        }
    }
    if (c->nvisited < (int)(sizeof(c->visited) / sizeof(c->visited[0]))) {
        c->visited[c->nvisited] = file;
        c->visited_name[c->nvisited] = name;
        c->nvisited++;
    }
    return false;
}

static bool tr_same(const tr_res_t *a, const tr_res_t *b) {
    return a->kind == b->kind && a->node == b->node && a->file == b->file;
}

/* What file `file` exports as `name`; TR_NONE when it exports no such name. */
static tr_res_t tr_export_of(tr_ctx_t *c, int file, const char *name, int depth) {
    if (depth > TR_MAX_DEPTH || tr_seen(c, file, name)) {
        return tr_mk(TR_NONE);
    }
    const tr_file_t *f = &c->ix->files[file];
    bool dflt = strcmp(name, "default") == 0;
    for (int i = 0; i < f->ndecls; i++) {
        const tr_rec_t *d = &f->decls[i];
        bool hit = dflt ? (d->kind == 'd' || d->kind == 'b')
                        : (strcmp(d->a, name) == 0 &&
                           (d->kind == 'e' || d->kind == 'b' || !f->module_syntax));
        if (hit) {
            return tr_decl_res(c, f, d->a);
        }
    }
    for (int i = 0; i < f->nlocals; i++) {
        if (strcmp(f->locals[i].a, name) == 0) {
            return tr_top_binding(c, file, f->locals[i].b, depth + SKIP_ONE);
        }
    }
    for (int i = 0; i < f->nfroms; i++) {
        if (strcmp(f->froms[i].a, name) == 0) {
            return tr_from(c, file, f->froms[i].spec, f->froms[i].b, depth);
        }
    }
    for (int i = 0; i < f->nstar_as; i++) {
        if (strcmp(f->star_as[i].a, name) == 0) {
            return tr_from(c, file, f->star_as[i].spec, NULL, depth);
        }
    }
    tr_res_t found = tr_mk(TR_NONE);
    for (int i = 0; i < f->nstars && !dflt; i++) {
        tr_res_t r = tr_from(c, file, f->stars[i].spec, name, depth);
        if (r.kind == TR_NONE || r.kind == TR_NOT_INDEXED) {
            continue;
        }
        if (found.kind == TR_NONE) {
            found = r;
        } else if (!tr_same(&found, &r)) {
            return tr_mk(TR_AMBIGUOUS);
        }
    }
    return found;
}

/* ECMAScript, DOM and Node.js globals: external. Sorted. */
static const char *const TR_LIB[] = {"AbortController",
                                     "AbortSignal",
                                     "AggregateError",
                                     "Array",
                                     "ArrayBuffer",
                                     "ArrayLike",
                                     "AsyncGenerator",
                                     "AsyncIterable",
                                     "AsyncIterableIterator",
                                     "AsyncIterator",
                                     "Atomics",
                                     "Awaited",
                                     "BigInt",
                                     "BigInt64Array",
                                     "BigUint64Array",
                                     "Blob",
                                     "Boolean",
                                     "Buffer",
                                     "CSSStyleDeclaration",
                                     "Capitalize",
                                     "ConstructorParameters",
                                     "CustomEvent",
                                     "DOMException",
                                     "DataView",
                                     "Date",
                                     "Document",
                                     "DocumentFragment",
                                     "Element",
                                     "Error",
                                     "ErrorEvent",
                                     "Event",
                                     "EventListener",
                                     "EventTarget",
                                     "Exclude",
                                     "Extract",
                                     "File",
                                     "FileList",
                                     "Float32Array",
                                     "Float64Array",
                                     "FormData",
                                     "Function",
                                     "Generator",
                                     "HTMLElement",
                                     "Headers",
                                     "Infinity",
                                     "InstanceType",
                                     "Int16Array",
                                     "Int32Array",
                                     "Int8Array",
                                     "Intl",
                                     "Iterable",
                                     "IterableIterator",
                                     "Iterator",
                                     "JSON",
                                     "KeyboardEvent",
                                     "Location",
                                     "Lowercase",
                                     "Map",
                                     "Math",
                                     "MessageEvent",
                                     "MouseEvent",
                                     "MutationObserver",
                                     "NaN",
                                     "Navigator",
                                     "Node",
                                     "NodeJS",
                                     "NodeList",
                                     "NonNullable",
                                     "Number",
                                     "Object",
                                     "Omit",
                                     "OmitThisParameter",
                                     "Parameters",
                                     "Partial",
                                     "Pick",
                                     "Promise",
                                     "PromiseLike",
                                     "PropertyKey",
                                     "Proxy",
                                     "RangeError",
                                     "Readonly",
                                     "ReadonlyArray",
                                     "ReadonlyMap",
                                     "ReadonlySet",
                                     "Record",
                                     "ReferenceError",
                                     "Reflect",
                                     "RegExp",
                                     "Request",
                                     "Required",
                                     "Response",
                                     "ReturnType",
                                     "Set",
                                     "SharedArrayBuffer",
                                     "String",
                                     "Symbol",
                                     "SyntaxError",
                                     "TemplateStringsArray",
                                     "TextDecoder",
                                     "TextEncoder",
                                     "ThisParameterType",
                                     "ThisType",
                                     "TypeError",
                                     "URIError",
                                     "URL",
                                     "URLSearchParams",
                                     "Uint16Array",
                                     "Uint32Array",
                                     "Uint8Array",
                                     "Uint8ClampedArray",
                                     "Uncapitalize",
                                     "Uppercase",
                                     "WeakMap",
                                     "WeakRef",
                                     "WeakSet",
                                     "WebSocket",
                                     "Window",
                                     "Worker",
                                     "XMLHttpRequest",
                                     "__dirname",
                                     "__filename",
                                     "clearInterval",
                                     "clearTimeout",
                                     "console",
                                     "decodeURIComponent",
                                     "document",
                                     "encodeURIComponent",
                                     "exports",
                                     "fetch",
                                     "globalThis",
                                     "isFinite",
                                     "isNaN",
                                     "module",
                                     "parseFloat",
                                     "parseInt",
                                     "process",
                                     "queueMicrotask",
                                     "require",
                                     "setImmediate",
                                     "setInterval",
                                     "setTimeout",
                                     "structuredClone",
                                     "undefined",
                                     "window"};

static int tr_lib_cmp(const void *k, const void *e) {
    return strcmp((const char *)k, *(const char *const *)e);
}

static bool tr_lib(const char *name) {
    size_t n = strlen(name);
    if ((n > TR_HTML_MIN && strncmp(name, "HTML", CBM_SZ_4) == 0 &&
         strcmp(name + n - CBM_SZ_7, "Element") == 0) ||
        (n > TR_SVG_MIN && strncmp(name, "SVG", CBM_SZ_3) == 0 &&
         strcmp(name + n - CBM_SZ_7, "Element") == 0)) {
        return true;
    }
    return bsearch(name, TR_LIB, sizeof(TR_LIB) / sizeof(TR_LIB[0]), sizeof(TR_LIB[0]),
                   tr_lib_cmp) != NULL;
}

static bool tr_label_container(const char *l) {
    return l && (strcmp(l, "Class") == 0 || strcmp(l, "Interface") == 0 || strcmp(l, "Enum") == 0);
}

/* The class, interface or enum whose members a bare name may be: the
 * documented declaration itself when it is one, else the one it is a member
 * of. NULL: none. */
static const cbm_gbuf_node_t *tr_container(const tr_ctx_t *c, const char *source_qn) {
    const cbm_gbuf_node_t *s = cbm_gbuf_find_by_qn(c->g, source_qn);
    if (s && tr_label_container(s->label)) {
        return s;
    }
    const char *dot = strrchr(source_qn, '.');
    if (!dot || (size_t)(dot - source_qn) >= TR_QN_MAX) {
        return NULL;
    }
    char parent[TR_QN_MAX];
    memcpy(parent, source_qn, (size_t)(dot - source_qn));
    parent[dot - source_qn] = '\0';
    const cbm_gbuf_node_t *p = cbm_gbuf_find_by_qn(c->g, parent);
    return p && tr_label_container(p->label) ? p : NULL;
}

static tr_res_t tr_member(const tr_ctx_t *c, const cbm_gbuf_node_t *of, const char *name) {
    char qn[TR_QN_MAX];
    if (!tr_join(qn, sizeof(qn), of->qualified_name, name, NULL)) {
        return tr_mk(TR_MISSING);
    }
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(c->g, qn);
    if (n) {
        tr_res_t r = tr_mk(TR_NODE);
        r.node = n;
        r.exact = true;
        return r;
    }
    /* a class, interface or enum member often has no node of its own */
    return tr_mk(tr_label_container(of->label) ? TR_GAP : TR_MISSING);
}

/* The first segment, seen from the documented declaration. */
static tr_res_t tr_head(tr_ctx_t *c, const CBMDocLink *link, const char *name) {
    const tr_file_t *src = &c->ix->files[c->src];
    if (strcmp(name, "this") == 0) {
        const cbm_gbuf_node_t *k = tr_container(c, link->source_qn);
        tr_res_t r = tr_mk(k ? TR_NODE : TR_MISSING);
        r.node = k;
        r.exact = true;
        return r;
    }
    /* the namespaces around the reference, innermost first */
    const tr_ns_t *best[TR_MAX_SEGS];
    int nb = 0;
    for (int i = 0; i < src->nnss && nb < TR_MAX_SEGS; i++) {
        const tr_ns_t *ns = &src->nss[i];
        if (ns->first <= link->line && link->line <= ns->last) {
            best[nb++] = ns;
        }
    }
    for (int k = nb - SKIP_ONE; k >= 0; k--) { /* nested ones come later in the blob */
        char qn[TR_QN_MAX];
        const cbm_gbuf_node_t *n =
            tr_join(qn, sizeof(qn), src->sym_qn, best[k]->path, name) ? tr_node(c, qn, src) : NULL;
        if (n) {
            tr_res_t r = tr_mk(TR_NODE);
            r.node = n;
            r.exact = true;
            return r;
        }
    }
    /* a function or class declared in a function body around the reference,
     * innermost first; the graph keeps it under a file-level name, which is
     * its own only when the node starts on its line */
    const tr_nest_t *vis = NULL;
    for (int i = 0; i < src->nnested; i++) {
        const tr_nest_t *v = &src->nested[i];
        if (strcmp(v->name, name) == 0 && v->first <= link->line && link->line <= v->last &&
            (!vis || v->last - v->first < vis->last - vis->first)) {
            vis = v;
        }
    }
    if (vis) {
        tr_res_t r = tr_decl_res(c, src, name);
        if (r.kind == TR_NODE && r.node->start_line != (int)vis->line) {
            r = tr_mk(TR_GAP); /* the node is another declaration's of that name */
        }
        r.exact = true;
        return r;
    }
    tr_res_t r = tr_top_binding(c, c->src, name, 0);
    if (r.kind != TR_NONE) {
        return r;
    }
    if (tr_lib(name)) {
        return tr_mk(TR_EXTERNAL);
    }
    const tr_global_t *g = (const tr_global_t *)cbm_ht_get(c->ix->globals, name);
    if (g) {
        if (g->count > SKIP_ONE) {
            return tr_mk(TR_AMBIGUOUS);
        }
        r = tr_decl_res(c, &c->ix->files[g->file], name);
        return r;
    }
    const cbm_gbuf_node_t *k = tr_container(c, link->source_qn);
    if (k) {
        r = tr_member(c, k, name);
        if (r.kind == TR_NODE) {
            return r;
        }
    }
    return tr_mk(TR_MISSING);
}

static bool tr_ident(const char *s, size_t n) {
    if (n == 0 || isdigit((unsigned char)s[0])) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)s[i];
        if (!(isalnum(ch) || ch == '_' || ch == '$' || ch >= 0x80)) {
            return false;
        }
    }
    return true;
}

/* Split a reference into its segments; false when it is no name path. */
static int tr_segments(char *buf, char **segs) {
    size_t n = strlen(buf);
    char *paren = strchr(buf, '(');
    if (paren) {
        if (buf[n - SKIP_ONE] != ')') {
            return 0;
        }
        *paren = '\0'; /* (), (args): one node holds every overload */
    }
    char *colon = strchr(buf, ':');
    if (colon) {
        *colon = '\0'; /* a meaning: :class, :0 */
    }
    int ns = 0;
    for (char *p = buf; ns < TR_MAX_SEGS;) {
        size_t k = strcspn(p, ".#~");
        if (!tr_ident(p, k)) {
            return 0;
        }
        segs[ns++] = p;
        if (p[k] == '\0') {
            break;
        }
        p[k] = '\0';
        p += k + SKIP_ONE;
    }
    return ns;
}

static void tr_out(tr_ctx_t *c, tr_res_t r) {
    switch (r.kind) {
    case TR_NODE: {
        const tr_file_t *src = &c->ix->files[c->src];
        const char *tp = r.node->file_path;
        int tf = tp ? tr_file_at(c->ix, tp) : CBM_NOT_FOUND;
        if (tf >= 0 && c->ix->files[tf].test && !src->test) {
            c->out->kind = CBM_DOCLINK_UNRESOLVED;
            c->out->reason = CBM_DOCLINK_REASON_TEST_ONLY;
            return;
        }
        c->out->kind = CBM_DOCLINK_EDGE;
        c->out->target = r.node;
        c->out->exact = r.exact;
        return;
    }
    case TR_EXTERNAL:
        c->out->reason = CBM_DOCLINK_REASON_EXTERNAL;
        break;
    case TR_AMBIGUOUS:
        c->out->reason = CBM_DOCLINK_REASON_AMBIGUOUS;
        break;
    case TR_GAP:
        c->out->reason = CBM_DOCLINK_REASON_GRAPH_GAP;
        break;
    case TR_NOT_INDEXED:
        c->out->reason = CBM_DOCLINK_REASON_NOT_INDEXED;
        break;
    default:
        c->out->reason = CBM_DOCLINK_REASON_MISSING;
        break;
    }
    c->out->kind = CBM_DOCLINK_UNRESOLVED;
}

static void tr_resolve(const void *index, void *state, int run_file, const CBMDocLink *link,
                       const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out) {
    (void)state; /* TS/JS keeps no per-file working state */
    const tr_index_t *ix = (const tr_index_t *)index;
    out->kind = CBM_DOCLINK_UNRESOLVED;
    out->reason = CBM_DOCLINK_REASON_MISSING;
    out->target = NULL;
    out->exact = false;
    if (!ix || !link->raw || run_file < 0 || run_file >= ix->run_count ||
        ix->run_to_file[run_file] < 0) {
        return;
    }
    tr_ctx_t c = {.ix = ix, .g = graph, .src = ix->run_to_file[run_file], .out = out};
    char buf[CBM_DOCLINK_TS_REF_MAX + PAIR_LEN];
    size_t rl = strlen(link->raw);
    char *segs[TR_MAX_SEGS];
    int ns = 0;
    if (rl <= CBM_DOCLINK_TS_REF_MAX && strncmp(link->raw, "module:", CBM_SZ_7) != 0) {
        memcpy(buf, link->raw, rl + SKIP_ONE);
        ns = tr_segments(buf, segs);
    }
    if (ns == 0) {
        out->reason = CBM_DOCLINK_REASON_UNPARSEABLE;
        return;
    }
    tr_res_t r = tr_head(&c, link, segs[0]);
    for (int i = SKIP_ONE; i < ns && (r.kind == TR_NODE || r.kind == TR_MODULE); i++) {
        if (r.kind == TR_MODULE) {
            c.nvisited = 0;
            r = tr_export_of(&c, r.file, segs[i], 0);
            r.kind = r.kind == TR_NONE ? TR_MISSING : r.kind;
        } else {
            r = tr_member(&c, r.node, segs[i]);
        }
    }
    if (r.kind == TR_MODULE) {
        /* a module named by itself: its file */
        const cbm_gbuf_node_t *fn =
            cbm_pipeline_file_node(graph, ix->project, ix->files[r.file].path);
        r = tr_mk(fn ? TR_NODE : TR_GAP);
        r.node = fn;
        r.exact = true;
    }
    tr_out(&c, r);
}

/* A top-level declaration (D name flags) declares one name: other files reach
 * it through an import of it, so a reference that did either bound it (an
 * edge into this file) or names it in its unresolved row. Imports, exports
 * and namespaces change how others resolve: GLOBAL. */
static bool tr_removable(const char *line, size_t len, const char **name, size_t *nlen) {
    return len > PAIR_LEN && line[0] == 'D' && line[1] == '\t' &&
           cbm_doclinks_scope_field(line, len, 1, name, nlen);
}

static int tr_scope_delta(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                          void *ud) {
    return cbm_doclinks_scope_delta_lines(stored, fresh, tr_removable, removed, ud);
}

const cbm_doclink_resolver_t cbm_doclink_ts_resolver = {
    .langs = {CBM_LANG_TYPESCRIPT, CBM_LANG_TSX, CBM_LANG_JAVASCRIPT},
    .lang_count = CBM_SZ_3,
    .scope_tag = CBM_DOCLINK_TS_SCOPE_TAG,
    .build = tr_build,
    .destroy = tr_destroy,
    .resolve = tr_resolve,
    .scope_delta = tr_scope_delta,
};
