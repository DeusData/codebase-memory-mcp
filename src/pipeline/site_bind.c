/*
 * site_bind.c — answers of the shared cross-file layer at call and usage
 * sites. See site_bind.h for where the resolvers ask and how they apply an
 * answer.
 *
 * The run's scope is the visibility closure (scope_closure.h) of every file
 * of a row language, from its scope facts (extract_scope.c): DEF events are
 * the definitions, INCLUDE events the includes, resolved to repository files
 * by the path rule (include_path.h), OPAQUE events parts the facts cannot
 * read. A usage is looked up by the type reference it ends (a QREF event).
 */
#include "pipeline/site_bind.h"

#include "cbm.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/include_path.h"
#include "pipeline/lsp_surface.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/scope_closure.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* Strategies and confidences of the layer's edges. */
#define SB_PROVEN_STRATEGY "include_scope"
#define SB_POSSIBLE_STRATEGY "include_scope_may"
#define SB_PROVEN_CONF 0.95
#define SB_POSSIBLE_CONF 0.85

enum { SB_PATH_BUF = 1024, SB_PROPS_BUF = 2048 };

struct cbm_site_scope {
    cbm_closure_t *closure;
    int files;
    int stored_files;
    int unreadable_files;
    int includes;
    int includes_resolved;
    /* run totals (count only), summed by every resolver thread */
    _Atomic uint64_t proven;
    _Atomic uint64_t possible;
    _Atomic uint64_t drawn;
    _Atomic uint64_t same;
    _Atomic uint64_t disagree;
    _Atomic uint64_t present;
    _Atomic uint64_t unsettled;
    _Atomic uint64_t absent;
    /* CBM_SITE_BIND_DUMP=<path> (test-seam builds only): one line per
     * answered site and per settled possible answer, appended (measurement) */
    FILE *dump;
    /* building only: resolved include path -> the first import spelling
     * that named it (in spell_arena); freed before the closure is sealed */
    CBMHashTable *spellings;
    CBMArena spell_arena;
};

static const char *sb_verdict_name(cbm_closure_verdict_t v) {
    switch (v) {
    case CBM_CLOSURE_PROVEN:
        return "proven";
    case CBM_CLOSURE_POSSIBLE:
        return "possible";
    case CBM_CLOSURE_PROVEN_ABSENT:
        return "absent";
    default:
        return "no_answer";
    }
}

struct cbm_site_memo {
    cbm_closure_memo_t *closure_memo;
};

void cbm_pipeline_site_memo_free(cbm_site_memo_t *m) {
    if (m) {
        cbm_closure_memo_free(m->closure_memo);
        cbm_free(CBM_MEM_CLASS_OTHER, m);
    }
}

/* A POSSIBLE answer, settled when the file is done. */
struct sb_pending {
    int64_t source_id;
    const cbm_gbuf_node_t *target;
    int index; /* the usage or call in sf->result */
    bool is_call;
    bool main_drew;                     /* usage: main's chain drew a USAGE edge at the site */
    const cbm_gbuf_node_t *main_target; /* ... to this node */
};

/* ── Building the run's scope ───────────────────────────────────────── */

/* One file's facts: fresh (a result) or stored (a surface row's "is"). */
typedef struct {
    const char *rel;
    CBMLanguage lang;
    const cbm_closure_lang_t *row;
    const CBMFileResult *result; /* fresh */
    cbm_lsp_scope_facts_t facts; /* stored */
    bool stored;
    bool readable;
    int index; /* in the closure */
} sb_file_t;

/* The package a file's facts name: every INCLUDE event carries it; NULL
 * (unknown) for a file without one. */
/* The file's package: the one its INCLUDE events carry; a file without one
 * (no import) gives it through its definitions: the full name of the one with
 * the fewest components is the package followed by its name (a top-level
 * definition; a nested one has more). NULL: unknown (no import, no
 * definition). `buf` holds a derived package. */
static const char *sb_package(const CBMScopeEvent *ev, int n, char *buf, size_t cap) {
    int best = -1;
    size_t best_dots = SIZE_MAX;
    for (int i = 0; i < n; i++) {
        if (ev[i].kind == CBM_SCOPE_INCLUDE && ev[i].qual) {
            return ev[i].qual; /* a stored row's INCLUDE carries none: read the definitions */
        }
        if (ev[i].kind == CBM_SCOPE_DEF && ev[i].qual && ev[i].name) {
            size_t dots = 0;
            for (const char *c = ev[i].qual; *c; c++) {
                dots += *c == '.';
            }
            if (dots < best_dots) {
                best_dots = dots;
                best = i;
            }
        }
    }
    if (best < 0) {
        return NULL;
    }
    const char *qual = ev[best].qual;
    size_t ql = strlen(qual);
    size_t nl = strlen(ev[best].name);
    if (ql == nl) {
        return strcmp(qual, ev[best].name) == 0 ? "" : NULL;
    }
    if (ql <= nl + SKIP_ONE || qual[ql - nl - SKIP_ONE] != '.' ||
        strcmp(qual + ql - nl, ev[best].name) != 0 || ql - nl - SKIP_ONE >= cap) {
        return NULL;
    }
    memcpy(buf, qual, ql - nl - SKIP_ONE);
    buf[ql - nl - SKIP_ONE] = '\0';
    return buf;
}

static void sb_add_events(const cbm_pipeline_ctx_t *ctx, cbm_site_scope_t *sc, const sb_file_t *f) {
    const CBMScopeEvent *ev = f->stored ? f->facts.events : f->result->scope.items;
    int n = f->stored ? f->facts.event_count : f->result->scope.count;
    for (int i = 0; i < n; i++) {
        const CBMScopeEvent *e = &ev[i];
        bool conditional = (e->flags & CBM_SCOPE_CONDITIONAL) != 0;
        switch (e->kind) {
        case CBM_SCOPE_DEF: {
            const CBMDefinition *d = NULL;
            if (f->stored && e->index >= 0 && e->index < f->facts.def_count) {
                d = &f->facts.defs[e->index];
            } else if (!f->stored && e->index >= 0 && e->index < f->result->defs.count) {
                d = &f->result->defs.items[e->index];
            }
            (void)cbm_closure_add_def(sc->closure, f->index, e->name, e->qual,
                                      d ? d->qualified_name : NULL, conditional);
            if (sc->dump) {
                fprintf(sc->dump, "D\t%s\t%s\t%s\t%s\n", f->rel, e->name ? e->name : "",
                        e->qual ? e->qual : "", d && d->qualified_name ? d->qualified_name : "");
            }
            break;
        }
        case CBM_SCOPE_ASSIGN:
            /* a name defined here with no node of the graph */
            (void)cbm_closure_add_def(sc->closure, f->index, e->name, NULL, NULL, conditional);
            break;
        case CBM_SCOPE_INCLUDE: {
            CBMImport imp;
            memset(&imp, 0, sizeof(imp));
            if (f->stored && e->index >= 0 && e->index < f->facts.module_count) {
                imp.module_path = f->facts.modules[e->index];
            } else if (!f->stored && e->index >= 0 && e->index < f->result->imports.count) {
                imp = f->result->imports.items[e->index];
            }
            char path[SB_PATH_BUF];
            int target = CBM_NOT_FOUND;
            if (imp.module_path &&
                cbm_include_path_resolve(ctx->include_universe, ctx->gbuf, ctx->project_name,
                                         f->rel, f->lang, &imp, path,
                                         sizeof(path)) == CBM_INCLUDE_PATH_FILE) {
                target = cbm_closure_file(sc->closure, path);
            }
            if (target >= 0 && sc->spellings) {
                const char *first = cbm_ht_get(sc->spellings, path);
                if (!first) {
                    const char *k = cbm_arena_strdup(&sc->spell_arena, path);
                    const char *v = cbm_arena_strdup(&sc->spell_arena, imp.module_path);
                    if (k && v) {
                        cbm_ht_set(sc->spellings, k, (void *)v);
                    }
                } else if (strcmp(first, imp.module_path) != 0) {
                    /* one file named by two import spellings: protoc reads two
                     * virtual files that define the same names ("already
                     * defined"), so nothing through it is proven */
                    (void)cbm_closure_add_opaque(sc->closure, target);
                }
            }
            sc->includes++;
            sc->includes_resolved += target >= 0;
            (void)cbm_closure_add_include(sc->closure, f->index, target,
                                          (e->flags & CBM_SCOPE_REEXPORT) != 0);
            break;
        }
        case CBM_SCOPE_OPAQUE:
            (void)cbm_closure_add_opaque(sc->closure, f->index);
            break;
        default:
            break;
        }
    }
}

cbm_site_scope_t *cbm_pipeline_site_scope_build(const cbm_pipeline_ctx_t *ctx,
                                                const cbm_file_info_t *fresh, int fresh_count,
                                                CBMFileResult **cache, const cbm_file_info_t *all,
                                                int all_count, const cbm_lsp_surface_row_t *stored,
                                                int stored_count) {
    if (!ctx || !ctx->include_universe) {
        return NULL;
    }
    /* Row files: the fresh ones, then the other discovered ones (stored). */
    int nrow = 0;
    for (int i = 0; i < fresh_count; i++) {
        nrow += cbm_closure_lang(fresh[i].language) != NULL;
    }
    for (int i = 0; all && i < all_count; i++) {
        nrow += cbm_closure_lang(all[i].language) != NULL;
    }
    if (nrow == 0) {
        return NULL; /* no file of a row language: no cost at all */
    }
    cbm_site_scope_t *sc = cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*sc));
    sb_file_t *files = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)nrow * sizeof(*files));
    bool *loaded = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)nrow * sizeof(*loaded));
    CBMHashTable *fresh_rel = cbm_ht_create(CBM_SZ_64);
    CBMHashTable *rows_by_rel = stored_count > 0 ? cbm_ht_create(CBM_SZ_256) : NULL;
    CBMArena facts_arena;
    cbm_arena_init(&facts_arena);
    bool ok = sc && files && loaded && fresh_rel && (stored_count == 0 || rows_by_rel);
    if (ok) {
        memset(sc, 0, sizeof(*sc));
        memset(files, 0, (size_t)nrow * sizeof(*files));
        memset(loaded, 0, (size_t)nrow * sizeof(*loaded));
        sc->closure = cbm_closure_new();
        ok = sc->closure != NULL;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        /* measurement only, in test-seam builds: the product never reads the
         * variable nor opens a file (sc->dump stays NULL) */
        char dump_path[SB_PATH_BUF];
        if (ok && cbm_safe_getenv("CBM_SITE_BIND_DUMP", dump_path, sizeof(dump_path), NULL)) {
            sc->dump = cbm_fopen(dump_path, "a");
        }
#endif
    } else if (sc) {
        memset(sc, 0, sizeof(*sc));
    }
    int n = 0;
    for (int i = 0; ok && i < fresh_count; i++) {
        const cbm_closure_lang_t *row = cbm_closure_lang(fresh[i].language);
        if (!row) {
            continue;
        }
        sb_file_t *f = &files[n];
        f->rel = fresh[i].rel_path;
        f->lang = fresh[i].language;
        f->row = row;
        f->result = cache ? cbm_pipeline_result_acquire(ctx, cache, i, NULL, &loaded[n]) : NULL;
        f->readable = f->result != NULL;
        cbm_ht_set(fresh_rel, f->rel, f);
        n++;
    }
    for (int i = 0; ok && i < stored_count; i++) {
        if (stored[i].rel_path) {
            cbm_ht_set(rows_by_rel, stored[i].rel_path, (void *)&stored[i]);
        }
    }
    for (int i = 0; ok && all && i < all_count; i++) {
        const cbm_closure_lang_t *row = cbm_closure_lang(all[i].language);
        if (!row || cbm_ht_get(fresh_rel, all[i].rel_path)) {
            continue;
        }
        sb_file_t *f = &files[n++];
        f->rel = all[i].rel_path;
        f->lang = all[i].language;
        f->row = row;
        f->stored = true;
        const cbm_lsp_surface_row_t *r = rows_by_rel ? cbm_ht_get(rows_by_rel, f->rel) : NULL;
        f->readable =
            r && cbm_lsp_surface_scope_from_json(&facts_arena, r->defs_json, &f->facts) == SKIP_ONE;
    }
    /* Files first, so an include can name any of them. */
    for (int i = 0; ok && i < n; i++) {
        sb_file_t *f = &files[i];
        const char *package = NULL;
        char pkgbuf[SB_PATH_BUF];
        if (f->readable) {
            package = f->stored ? sb_package(f->facts.events, f->facts.event_count, pkgbuf,
                                             sizeof(pkgbuf))
                                : sb_package(f->result->scope.items, f->result->scope.count, pkgbuf,
                                             sizeof(pkgbuf));
        }
        f->index = cbm_closure_add_file(sc->closure, f->rel, f->row, package);
        if (f->index < 0) {
            f->readable = false; /* a path seen twice: the first one stands */
            continue;
        }
        sc->files++;
        sc->stored_files += f->stored;
        if (!f->readable) {
            sc->unreadable_files++;
            (void)cbm_closure_add_opaque(sc->closure, f->index);
        }
    }
    if (ok) {
        cbm_arena_init(&sc->spell_arena);
        sc->spellings = cbm_ht_create(CBM_SZ_256);
    }
    for (int i = 0; ok && i < n; i++) {
        if (files[i].readable) {
            sb_add_events(ctx, sc, &files[i]);
        }
    }
    if (sc && sc->spellings) {
        cbm_ht_free(sc->spellings);
        sc->spellings = NULL;
        cbm_arena_destroy(&sc->spell_arena);
    }
    ok = ok && cbm_closure_seal(sc->closure) == 0;
    for (int i = 0; files && i < n; i++) {
        if (!files[i].stored && files[i].result) {
            cbm_pipeline_result_release((CBMFileResult *)files[i].result, loaded[i]);
        }
    }
    cbm_arena_destroy(&facts_arena); /* the closure copied what it keeps */
    cbm_free(CBM_MEM_CLASS_OTHER, files);
    cbm_free(CBM_MEM_CLASS_OTHER, loaded);
    if (fresh_rel) {
        cbm_ht_free(fresh_rel);
    }
    if (rows_by_rel) {
        cbm_ht_free(rows_by_rel);
    }
    if (!ok) {
        cbm_pipeline_site_scope_free(sc);
        return NULL;
    }
    char b[4][CBM_SZ_16];
    snprintf(b[0], sizeof(b[0]), "%d", sc->files);
    snprintf(b[1], sizeof(b[1]), "%d", cbm_closure_def_count(sc->closure));
    snprintf(b[2], sizeof(b[2]), "%d", sc->includes);
    snprintf(b[3], sizeof(b[3]), "%d", sc->includes_resolved);
    cbm_log_info("site_bind.scope", "files", b[0], "defs", b[1], "includes", b[2], "resolved",
                 b[3]);
    return sc;
}

void cbm_pipeline_site_scope_free(cbm_site_scope_t *sc) {
    if (!sc) {
        return;
    }
    if (sc->closure) {
        char b[8][CBM_SZ_32];
        const _Atomic uint64_t *v[8] = {&sc->proven,   &sc->possible, &sc->drawn,     &sc->same,
                                        &sc->disagree, &sc->present,  &sc->unsettled, &sc->absent};
        for (int i = 0; i < 8; i++) {
            snprintf(b[i], sizeof(b[i]), "%llu", (unsigned long long)atomic_load(v[i]));
        }
        /* drawn and present together: whether a possible answer's edge was drawn
         * by the layer or already there depends on which worker's buffer holds
         * main's edge (route-dependent); that it exists does not */
        snprintf(b[2], sizeof(b[2]), "%llu",
                 (unsigned long long)(atomic_load(v[2]) + atomic_load(v[5])));
        cbm_log_info("site_bind.run", "proven", b[0], "possible", b[1], "possible_drawn", b[2],
                     "possible_same", b[3], "possible_disagree", b[4], "possible_unsettled", b[6],
                     "proven_absent", b[7]);
    }
    if (sc->dump) {
        fclose(sc->dump);
    }
    cbm_closure_free(sc->closure);
    cbm_free(CBM_MEM_CLASS_OTHER, sc);
}

/* ── The test seam ───────────────────────────────────────────────────── */

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
enum { SB_MARKS = 16 };
static struct {
    char name[CBM_SZ_128];
    char target[CBM_SZ_512];
    cbm_site_verdict_t verdict;
} g_marks[SB_MARKS];
static int g_nmarks;

void cbm_pipeline_site_bind_test_mark(const char *name, const char *target_qn,
                                      cbm_site_verdict_t verdict) {
    if (!name) {
        g_nmarks = 0;
        return;
    }
    if (g_nmarks < SB_MARKS) {
        snprintf(g_marks[g_nmarks].name, sizeof(g_marks[g_nmarks].name), "%s", name);
        snprintf(g_marks[g_nmarks].target, sizeof(g_marks[g_nmarks].target), "%s",
                 target_qn ? target_qn : "");
        g_marks[g_nmarks].verdict = verdict;
        g_nmarks++;
    }
}

/* The mark of a site's name, or -1. */
static int sb_marked(const char *name) {
    for (int i = 0; name && i < g_nmarks; i++) {
        if (strcmp(name, g_marks[i].name) == 0) {
            return i;
        }
    }
    return CBM_NOT_FOUND;
}
#endif

/* ── Sites ───────────────────────────────────────────────────────────── */

void cbm_pipeline_site_bind_begin(cbm_site_file_t *sf, const cbm_pipeline_ctx_t *ctx,
                                  const cbm_gbuf_t *graph, const char *rel, CBMLanguage lang,
                                  const CBMFileResult *result, cbm_site_memo_t **memo,
                                  const CBMCallEvidence *evidence) {
    if (!sf) {
        return;
    }
    memset(sf, 0, sizeof(*sf));
    sf->ctx = ctx;
    sf->graph = graph;
    sf->result = result;
    sf->rel = rel;
    sf->lang = lang;
    sf->memo = memo;
    sf->evidence = evidence;
    sf->file = CBM_NOT_FOUND;
    const cbm_site_scope_t *sc = ctx ? ctx->site_scope : NULL;
    if (sc && result && graph && rel && cbm_closure_lang(lang)) {
        sf->file = cbm_closure_file(sc->closure, rel);
        sf->active = sf->file >= 0;
    }
}

/* The answer at every site of a file no row covers. */
static cbm_site_answer_t sb_no_answer(void) {
    cbm_site_answer_t none = {.verdict = CBM_SITE_NO_ANSWER, .res = {0}, .target = NULL};
    return none;
}

static cbm_site_answer_t sb_proven(const cbm_gbuf_node_t *target) {
    cbm_site_answer_t a = {.verdict = CBM_SITE_PROVEN, .target = target};
    a.res.qualified_name = target->qualified_name;
    a.res.strategy = SB_PROVEN_STRATEGY;
    a.res.confidence = SB_PROVEN_CONF;
    a.res.candidate_count = SKIP_ONE;
    return a;
}

/* Keep a POSSIBLE answer for the end step; false when out of memory (the
 * site then has no answer). */
static bool sb_pend(cbm_site_file_t *sf, const cbm_gbuf_node_t *source,
                    const cbm_gbuf_node_t *target, int index, bool is_call) {
    if (sf->npending >= sf->cap_pending) {
        int ncap = sf->cap_pending ? sf->cap_pending * PAIR_LEN : CBM_SZ_16;
        sb_pending_t *np = cbm_alloc(CBM_MEM_CLASS_RESOLVE, (size_t)ncap * sizeof(*np));
        if (!np) {
            return false;
        }
        if (sf->npending) {
            memcpy(np, sf->pending, (size_t)sf->npending * sizeof(*np));
        }
        cbm_free(CBM_MEM_CLASS_RESOLVE, sf->pending);
        sf->pending = np;
        sf->cap_pending = ncap;
    }
    sf->pending[sf->npending++] = (sb_pending_t){.source_id = source->id,
                                                 .target = target,
                                                 .index = index,
                                                 .is_call = is_call,
                                                 .main_drew = false,
                                                 .main_target = NULL};
    return true;
}

/* Turn a target node and a verdict into the site's answer. */
static cbm_site_answer_t sb_answer(cbm_site_file_t *sf, cbm_site_verdict_t v,
                                   const cbm_gbuf_node_t *source, const cbm_gbuf_node_t *target,
                                   int index, bool is_call) {
    if (!target) {
        return sb_no_answer();
    }
    if (v == CBM_SITE_PROVEN) {
        sf->proven++;
        return sb_proven(target);
    }
    if (v == CBM_SITE_POSSIBLE && sb_pend(sf, source, target, index, is_call)) {
        sf->possible++;
        cbm_site_answer_t a = {.verdict = CBM_SITE_POSSIBLE, .target = target};
        return a;
    }
    return sb_no_answer();
}

cbm_site_answer_t cbm_pipeline_site_bind_call(cbm_site_file_t *sf, const CBMCall *call,
                                              const cbm_gbuf_node_t *source) {
    if (!sf || !call || !source || !sf->result) {
        return sb_no_answer();
    }
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    int mark = sb_marked(call->callee_name);
    if (mark >= 0 && sf->graph) {
        return sb_answer(sf, g_marks[mark].verdict, source,
                         cbm_gbuf_find_by_qn(sf->graph, g_marks[mark].target),
                         (int)(call - sf->result->calls.items), true);
    }
#endif
    /* No row answers calls yet (Protobuf has none). */
    return sb_no_answer();
}

/* The type reference (a QREF event) that the site [start, end) is the last
 * name of: its text as written, or NULL. Events are sorted by position (a
 * QREF's is its end byte, its index the start byte). */
static const char *sb_qref_ending(const CBMFileResult *r, uint32_t start, uint32_t end) {
    const CBMScopeEvent *ev = r->scope.items;
    int lo = 0;
    int hi = r->scope.count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / PAIR_LEN;
        if (ev[mid].pos < end) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (int i = lo; i < r->scope.count && ev[i].pos == end; i++) {
        if (ev[i].kind == CBM_SCOPE_QREF && ev[i].index >= 0 && (uint32_t)ev[i].index <= start) {
            return ev[i].name;
        }
    }
    return NULL;
}

/* The thread's closure memo, made on its first lookup. */
static cbm_closure_memo_t *sb_memo(cbm_site_file_t *sf) {
    if (!sf->memo) {
        return NULL;
    }
    if (!*sf->memo) {
        cbm_site_memo_t *m = cbm_alloc(CBM_MEM_CLASS_OTHER, sizeof(*m));
        if (!m) {
            return NULL;
        }
        m->closure_memo = cbm_closure_memo_new();
        if (!m->closure_memo) {
            cbm_free(CBM_MEM_CLASS_OTHER, m);
            return NULL;
        }
        *sf->memo = m;
    }
    return (*sf->memo)->closure_memo;
}

cbm_site_answer_t cbm_pipeline_site_bind_usage(cbm_site_file_t *sf, const CBMUsage *usage,
                                               const cbm_gbuf_node_t *source) {
    if (!sf || !usage || !source || !sf->result || !usage->ref_name) {
        return sb_no_answer();
    }
    int index = (int)(usage - sf->result->usages.items);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    int mark = sb_marked(usage->ref_name);
    if (mark >= 0 && sf->graph) {
        return sb_answer(sf, g_marks[mark].verdict, source,
                         cbm_gbuf_find_by_qn(sf->graph, g_marks[mark].target), index, false);
    }
#endif
    if (!sf->active || usage->site_end_byte <= usage->site_start_byte) {
        return sb_no_answer();
    }
    const char *written = sb_qref_ending(sf->result, usage->site_start_byte, usage->site_end_byte);
    if (!written) {
        return sb_no_answer(); /* not the last name of a type reference */
    }
    const char *dot = strrchr(written, '.');
    if (strcmp(dot ? dot + SKIP_ONE : written, usage->ref_name) != 0) {
        return sb_no_answer();
    }
    cbm_closure_ref_t ref = {.name = usage->ref_name, .written = dot ? written : NULL};
    const cbm_site_scope_t *sc = sf->ctx->site_scope;
    cbm_closure_answer_t a = cbm_closure_lookup(sc->closure, sb_memo(sf), sf->file, &ref);
    if (sc->dump) {
        fprintf(sc->dump, "A\t%s\t%u\t%u\t%s\t%s\t%s\t%s\t%s\t%u\t%d\n", sf->rel,
                usage->site_start_byte, usage->site_end_byte, usage->ref_name, written,
                sb_verdict_name(a.verdict), a.qn ? a.qn : "", a.full ? a.full : "", a.why,
                a.visible);
    }
    switch (a.verdict) {
    case CBM_CLOSURE_PROVEN:
    case CBM_CLOSURE_POSSIBLE: {
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(sf->graph, a.qn);
        return sb_answer(sf, a.verdict == CBM_CLOSURE_PROVEN ? CBM_SITE_PROVEN : CBM_SITE_POSSIBLE,
                         source, target, index, false);
    }
    case CBM_CLOSURE_PROVEN_ABSENT:
        sf->absent++;
        return sb_no_answer();
    default:
        return sb_no_answer();
    }
}

void cbm_pipeline_site_bind_usage_drawn(cbm_site_file_t *sf, const CBMUsage *usage,
                                        const cbm_gbuf_node_t *target) {
    if (!sf || sf->npending == 0 || !usage || !sf->result) {
        return;
    }
    sb_pending_t *last = &sf->pending[sf->npending - SKIP_ONE];
    if (!last->is_call && last->index == (int)(usage - sf->result->usages.items)) {
        last->main_drew = true;
        last->main_target = target;
    }
}

void cbm_pipeline_site_usage_props(char *buf, size_t cap, const CBMUsage *usage,
                                   cbm_site_verdict_t verdict) {
    char esc[CBM_SZ_256];
    cbm_json_escape(esc, sizeof(esc), usage && usage->ref_name ? usage->ref_name : "");
    bool proven = verdict == CBM_SITE_PROVEN;
    snprintf(buf, cap, "{\"callee\":\"%s\",\"confidence\":%.2f,\"strategy\":\"%s\"}", esc,
             proven ? SB_PROVEN_CONF : SB_POSSIBLE_CONF,
             proven ? SB_PROVEN_STRATEGY : SB_POSSIBLE_STRATEGY);
}

/* ── The end step ────────────────────────────────────────────────────── */

static bool sb_has_edge(const cbm_gbuf_t *gb, int64_t source, int64_t target, const char *type) {
    const cbm_gbuf_edge_t **out = NULL;
    int count = 0;
    if (!gb || cbm_gbuf_find_edges_by_source_type(gb, source, type, &out, &count) != 0) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        if (out[i]->target_id == target) {
            return true;
        }
    }
    return false;
}

/* Draw a possible edge (the site's main drew nothing of its type). */
static bool sb_draw(cbm_site_file_t *sf, cbm_gbuf_t *edges, const sb_pending_t *p) {
    char props[SB_PROPS_BUF];
    if (!p->is_call) {
        cbm_pipeline_site_usage_props(props, sizeof(props), &sf->result->usages.items[p->index],
                                      CBM_SITE_POSSIBLE);
        return cbm_gbuf_insert_edge(edges, p->source_id, p->target->id, "USAGE", props) > 0;
    }
    const CBMCall *call = &sf->result->calls.items[p->index];
    char esc[CBM_SZ_256];
    cbm_json_escape(esc, sizeof(esc), call->callee_name ? call->callee_name : "");
    int n = snprintf(props, sizeof(props),
                     "{\"callee\":\"%s\",\"confidence\":%.2f,\"strategy\":\"%s\",\"candidates\":%d",
                     esc, SB_POSSIBLE_CONF, SB_POSSIBLE_STRATEGY, SKIP_ONE);
    cbm_pipeline_close_call_props(props, n, call, true);
    return cbm_gbuf_insert_edge(edges, p->source_id, p->target->id, "CALLS", props) > 0;
}

void cbm_pipeline_site_bind_end(cbm_site_file_t *sf, cbm_gbuf_t *edges) {
    if (!sf) {
        return;
    }
    cbm_site_scope_t *sc = sf->ctx ? sf->ctx->site_scope : NULL;
    int drawn = 0;
    int same = 0;
    int disagree = 0;
    int present = 0;
    int unsettled = 0;
    for (int i = 0; edges && i < sf->npending; i++) {
        const sb_pending_t *p = &sf->pending[i];
        const char *type = p->is_call ? "CALLS" : "USAGE";
        bool drew = false;
        const cbm_gbuf_node_t *main_target = NULL;
        if (p->is_call) {
            if (!sf->evidence) {
                unsettled++; /* what main emitted at the site is unknown: nothing drawn */
                continue;
            }
            const CBMCallEvidence *ev = &sf->evidence[p->index];
            drew = ev->calls_emitted;
            main_target =
                ev->candidate_qn ? cbm_gbuf_find_by_qn(sf->graph, ev->candidate_qn) : NULL;
        } else {
            drew = p->main_drew;
            main_target = p->main_target;
        }
        bool exists =
            sb_has_edge(edges, p->source_id, p->target->id, type) ||
            (sf->graph != edges && sb_has_edge(sf->graph, p->source_id, p->target->id, type));
        const char *outcome = "drawn";
        if (drew) {
            if (main_target == p->target || (!main_target && exists)) {
                same++;
                outcome = "same";
            } else {
                disagree++; /* main's edge stays alone */
                outcome = "disagree";
            }
        } else if (p->target->id == p->source_id || exists) {
            present++; /* the edge is there already (another site): nothing added */
            outcome = "present";
        } else if (sb_draw(sf, edges, p)) {
            drawn++;
        } else {
            outcome = "failed";
        }
        if (sc && sc->dump) {
            uint32_t at = p->is_call ? 0 : sf->result->usages.items[p->index].site_start_byte;
            fprintf(sc->dump, "S\t%s\t%u\t%s\t%s\t%s\n", sf->rel ? sf->rel : "", at, outcome,
                    p->target->qualified_name,
                    main_target && main_target->qualified_name ? main_target->qualified_name : "");
        }
    }
    if (sc && (sf->proven || sf->possible || sf->absent)) {
        atomic_fetch_add(&sc->proven, (uint64_t)sf->proven);
        atomic_fetch_add(&sc->possible, (uint64_t)sf->possible);
        atomic_fetch_add(&sc->drawn, (uint64_t)drawn);
        atomic_fetch_add(&sc->same, (uint64_t)same);
        atomic_fetch_add(&sc->disagree, (uint64_t)disagree);
        atomic_fetch_add(&sc->present, (uint64_t)present);
        atomic_fetch_add(&sc->unsettled, (uint64_t)unsettled);
        atomic_fetch_add(&sc->absent, (uint64_t)sf->absent);
        char b[8][CBM_SZ_16];
        const int v[8] = {sf->proven, sf->possible, drawn,     same,
                          disagree,   present,      unsettled, sf->absent};
        for (int i = 0; i < 8; i++) {
            snprintf(b[i], sizeof(b[i]), "%d", v[i]);
        }
        cbm_log_info("site_bind.file", "rel", sf->rel ? sf->rel : "", "proven", b[0], "possible",
                     b[1], "drawn", b[2], "same", b[3], "disagree", b[4], "present", b[5],
                     "unsettled", b[6], "absent", b[7]);
    }
    cbm_free(CBM_MEM_CLASS_RESOLVE, sf->pending);
    sf->pending = NULL;
    sf->npending = 0;
    sf->cap_pending = 0;
}
