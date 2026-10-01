/*
 * doc_links.c — doc-comment references -> MENTIONS edges: the language-
 * independent core (run state, per-file resolution, edge collapse, rows).
 * See doc_links.h; the C# resolver is doc_links_cs.c.
 */
#include "pipeline/doc_links.h"

#include "doclink.h"
#include "foundation/arena.h"
#include "foundation/constants.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "result_spill.h" /* a parked result's header: is there a scope to read back? */
#include "yyjson/yyjson.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Reasons ─────────────────────────────────────────────────────── */

static const char *const DOCLINK_REASON_NAMES[CBM_DOCLINK_REASON_COUNT] = {
    [CBM_DOCLINK_REASON_MISSING] = "missing",
    [CBM_DOCLINK_REASON_AMBIGUOUS] = "ambiguous",
    [CBM_DOCLINK_REASON_EXTERNAL] = "external",
    [CBM_DOCLINK_REASON_TEST_ONLY] = "test_only_target",
    [CBM_DOCLINK_REASON_NOT_INDEXED] = "not_indexed",
    [CBM_DOCLINK_REASON_GRAPH_GAP] = "graph_gap",
    [CBM_DOCLINK_REASON_UNPARSEABLE] = "unparseable",
    [CBM_DOCLINK_REASON_BELOW_BAR] = "below_bar_tier",
};

const char *cbm_doclink_reason_name(int reason) {
    if (reason < 0 || reason >= CBM_DOCLINK_REASON_COUNT) {
        return "missing";
    }
    return DOCLINK_REASON_NAMES[reason];
}

/* ── Resolver table ──────────────────────────────────────────────── */

/* One pointer per language with a resolver: the one line a language leg adds
 * to this file. */
static const cbm_doclink_resolver_t *const DOCLINK_RESOLVERS[] = {
    &cbm_doclink_cs_resolver,
};

enum { DOCLINK_RESOLVER_COUNT = sizeof(DOCLINK_RESOLVERS) / sizeof(DOCLINK_RESOLVERS[0]) };

static bool resolver_has_lang(const cbm_doclink_resolver_t *R, CBMLanguage lang) {
    for (int i = 0; i < R->lang_count && i < CBM_DOCLINK_RESOLVER_LANGS; i++) {
        if (R->langs[i] == lang) {
            return true;
        }
    }
    return false;
}

static int resolver_slot(CBMLanguage lang) {
    for (int i = 0; i < DOCLINK_RESOLVER_COUNT; i++) {
        if (resolver_has_lang(DOCLINK_RESOLVERS[i], lang)) {
            return i;
        }
    }
    return CBM_NOT_FOUND;
}

/* True when the scope blob's tag line is `tag`. */
static bool scope_has_tag(const char *scope, const char *tag) {
    if (!scope || !tag) {
        return false;
    }
    size_t tl = strlen(tag);
    return strncmp(scope, tag, tl) == 0 && (scope[tl] == '\n' || scope[tl] == '\0');
}

/* The resolver whose scope blobs carry this blob's tag line; NULL for none. */
static const cbm_doclink_resolver_t *resolver_of_scope(const char *scope) {
    for (int i = 0; i < DOCLINK_RESOLVER_COUNT; i++) {
        if (scope_has_tag(scope, DOCLINK_RESOLVERS[i]->scope_tag)) {
            return DOCLINK_RESOLVERS[i];
        }
    }
    return NULL;
}

/* ── Incremental scope rules ─────────────────────────────────────── */

bool cbm_doclinks_is_scope_input(const char *rel_path) {
    for (int i = 0; rel_path && i < DOCLINK_RESOLVER_COUNT; i++) {
        if (DOCLINK_RESOLVERS[i]->scope_input && DOCLINK_RESOLVERS[i]->scope_input(rel_path)) {
            return true;
        }
    }
    return false;
}

int cbm_doclinks_scope_delta(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                             void *ud) {
    if (!stored && !fresh) {
        return CBM_DOCLINK_DELTA_LOCAL;
    }
    if (!stored || !fresh) {
        return CBM_DOCLINK_DELTA_GLOBAL; /* a scope appeared, or left with its file */
    }
    const cbm_doclink_resolver_t *R = resolver_of_scope(stored);
    if (!R || R != resolver_of_scope(fresh) || !R->scope_delta) {
        return CBM_DOCLINK_DELTA_GLOBAL;
    }
    return R->scope_delta(stored, fresh, removed, ud);
}

/* ── Run state ───────────────────────────────────────────────────── */

typedef struct {
    cbm_doc_link_row_t *items;
    int count;
} doclink_file_rows_t;

struct cbm_doclinks {
    const cbm_file_info_t *files; /* borrowed: the run's file list */
    int file_count;
    const char *project; /* borrowed: names the File node of a file-level source */
    void *index[DOCLINK_RESOLVER_COUNT];
    doclink_file_rows_t *rows; /* per run file */
    CBMArena arena;            /* scope copies handed to the resolvers */
    _Atomic bool failed;
    _Atomic int64_t edges;
    _Atomic int64_t mentions;
    _Atomic int64_t self_mentions;
    _Atomic int64_t local_refs;
    _Atomic int64_t no_source;
    _Atomic int64_t reasons[CBM_DOCLINK_REASON_COUNT];
};

static const char *itoa64(int64_t v, char *buf, size_t n) {
    snprintf(buf, n, "%lld", (long long)v);
    return buf;
}

static bool want_doc_scope(const CBMFileResult *header) {
    return header->doc_scope != NULL; /* a parked header's pointer is only a presence bit */
}

static int file_cmp(const void *a, const void *b) {
    return strcmp(((const cbm_doclink_file_t *)a)->rel_path,
                  ((const cbm_doclink_file_t *)b)->rel_path);
}

/* True when file `i`'s result is parked on disk with a scope: an acquire that
 * handed nothing out then means a failed read, not "no scope". A header that
 * cannot be peeked counts as one. */
static bool parked_scope_unread(const cbm_pipeline_ctx_t *ctx, CBMFileResult **cache, int i) {
    if ((cache && cache[i]) || !ctx || !ctx->spill || !cbm_result_spill_has(ctx->spill, i)) {
        return false;
    }
    CBMFileResult header;
    return !cbm_result_spill_peek_header(ctx->spill, i, &header) || want_doc_scope(&header);
}

/* The scope of run file `i`, copied into the run's arena; NULL when the file
 * has none. *why is set when it has one that cannot be had (the copy fails,
 * or its parked result does not load): a scope dropped silently would take
 * the file's declarations out of the index, and references to them would
 * read as missing. */
static const char *run_file_scope(cbm_doclinks_t *dl, const cbm_pipeline_ctx_t *ctx,
                                  CBMFileResult **cache, int i, const char **why) {
    bool loaded = false;
    CBMFileResult *r = cbm_pipeline_result_acquire(ctx, cache, i, want_doc_scope, &loaded);
    const char *scope = NULL;
    if (r && r->doc_scope) {
        scope = cbm_arena_strdup(&dl->arena, r->doc_scope);
        if (!scope) {
            *why = "alloc";
        }
    } else if (!r && parked_scope_unread(ctx, cache, i)) {
        *why = "scope_unreadable";
    }
    cbm_pipeline_result_release(r, loaded);
    return scope;
}

/* Build one language's index over this run's files of that language plus the
 * base scopes tagged for it. Returns false, with *why, when a scope cannot be
 * read or copied or the index cannot be built: the caller fails the layer. */
static bool build_language(cbm_doclinks_t *dl, int slot, const cbm_pipeline_ctx_t *ctx,
                           const cbm_file_info_t *files, int file_count, CBMFileResult **cache,
                           const cbm_doclink_scope_t *base, int base_count, const cbm_gbuf_t *graph,
                           const char **why) {
    const cbm_doclink_resolver_t *R = DOCLINK_RESOLVERS[slot];
    int cap = 0;
    for (int i = 0; i < file_count; i++) {
        cap += resolver_has_lang(R, files[i].language);
    }
    for (int i = 0; i < base_count; i++) {
        cap += scope_has_tag(base[i].scope, R->scope_tag);
    }
    if (cap == 0) {
        return true;
    }
    cbm_doclink_file_t *lf =
        (cbm_doclink_file_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)cap * sizeof(*lf));
    if (!lf) {
        *why = "alloc";
        return false;
    }
    const char *failed = NULL;
    int n = 0;
    for (int i = 0; i < file_count; i++) {
        if (!resolver_has_lang(R, files[i].language)) {
            continue;
        }
        const char *scope = run_file_scope(dl, ctx, cache, i, &failed);
        lf[n++] =
            (cbm_doclink_file_t){.rel_path = files[i].rel_path, .scope = scope, .run_file = i};
    }
    for (int i = 0; i < base_count; i++) {
        if (!scope_has_tag(base[i].scope, R->scope_tag)) {
            continue;
        }
        const char *rel_path = cbm_arena_strdup(&dl->arena, base[i].rel_path);
        const char *scope = cbm_arena_strdup(&dl->arena, base[i].scope);
        if (!rel_path || !scope) {
            failed = "alloc";
        }
        lf[n++] =
            (cbm_doclink_file_t){.rel_path = rel_path, .scope = scope, .run_file = CBM_NOT_FOUND};
    }
    if (failed) {
        cbm_free(CBM_MEM_CLASS_OTHER, lf);
        *why = failed;
        return false;
    }
    qsort(lf, (size_t)n, sizeof(*lf), file_cmp);
    cbm_doclink_build_in_t in = {
        .ctx = ctx, .graph = graph, .files = lf, .file_count = n, .run_file_count = file_count};
    dl->index[slot] = R->build(&in);
    cbm_free(CBM_MEM_CLASS_OTHER, lf);
    if (!dl->index[slot]) {
        *why = "index";
    }
    return dl->index[slot] != NULL;
}

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
static atomic_bool doclinks_test_fail_build;

void cbm_doclinks_test_fail_build_once(void) {
    atomic_store(&doclinks_test_fail_build, true);
}
#endif

cbm_doclinks_t *cbm_doclinks_build(const cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                   int file_count, CBMFileResult **cache,
                                   const cbm_doclink_scope_t *base, int base_count,
                                   const cbm_gbuf_t *graph) {
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (atomic_exchange(&doclinks_test_fail_build, false)) {
        cbm_log_error("doc_links.error", "phase", "build", "reason", "alloc");
        return NULL;
    }
#endif
    cbm_doclinks_t *dl = (cbm_doclinks_t *)cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*dl));
    if (!dl) {
        cbm_log_error("doc_links.error", "phase", "build", "reason", "alloc");
        return NULL;
    }
    dl->files = files;
    dl->file_count = file_count;
    dl->project = ctx ? ctx->project_name : NULL;
    dl->rows = file_count > 0
                   ? (doclink_file_rows_t *)cbm_calloc(
                         CBM_MEM_CLASS_OTHER, (size_t)file_count * sizeof(doclink_file_rows_t))
                   : NULL;
    cbm_arena_init(&dl->arena);
    if (file_count > 0 && !dl->rows) {
        cbm_log_error("doc_links.error", "phase", "build", "reason", "alloc");
        cbm_doclinks_free(dl);
        return NULL;
    }
    for (int s = 0; s < DOCLINK_RESOLVER_COUNT; s++) {
        const char *why = "alloc";
        if (!build_language(dl, s, ctx, files, file_count, cache, base, base_count, graph, &why)) {
            cbm_log_error("doc_links.error", "phase", "index_build", "reason", why);
            cbm_doclinks_free(dl);
            return NULL;
        }
    }
    return dl;
}

/* ── Per-file resolution ─────────────────────────────────────────── */

typedef struct {
    int64_t src;
    int64_t tgt;
    uint32_t line;
    uint16_t syntax;
    bool exact;
} doclink_mention_t;

static int mention_cmp(const void *a, const void *b) {
    const doclink_mention_t *x = (const doclink_mention_t *)a;
    const doclink_mention_t *y = (const doclink_mention_t *)b;
    if (x->src != y->src) {
        return x->src < y->src ? -1 : 1;
    }
    if (x->tgt != y->tgt) {
        return x->tgt < y->tgt ? -1 : 1;
    }
    if (x->line != y->line) {
        return x->line < y->line ? -1 : 1;
    }
    return (int)x->syntax - (int)y->syntax;
}

/* Row memory is CBM_MEM_CLASS_STORE everywhere (the store reads and frees the
 * same rows): cbm_store_free_doc_links is its one release. */
static char *dup_or_empty(const char *s) {
    return cbm_mem_strdup(CBM_MEM_CLASS_STORE, s ? s : "");
}

static void mark_failed(cbm_doclinks_t *dl, const char *rel, const char *why) {
    if (!atomic_exchange(&dl->failed, true)) {
        cbm_log_error("doc_links.error", "phase", "resolve", "path", rel ? rel : "", "reason", why);
    }
}

/* Emit one MENTIONS edge per (source, target): first line, its syntax, the
 * mention count, and tier exact when any mention bound exactly. */
static void emit_mentions(cbm_doclinks_t *dl, doclink_mention_t *m, int n, cbm_gbuf_t *edge_out) {
    qsort(m, (size_t)n, sizeof(*m), mention_cmp);
    int i = 0;
    while (i < n) {
        int j = i;
        bool exact = false;
        while (j < n && m[j].src == m[i].src && m[j].tgt == m[i].tgt) {
            exact = exact || m[j].exact;
            j++;
        }
        char props[CBM_SZ_256];
        snprintf(props, sizeof(props),
                 "{\"via\":\"doc_comment\",\"syntax\":\"%s\",\"tier\":\"%s\",\"line\":%u,"
                 "\"count\":%d}",
                 cbm_doclink_syntax_name(m[i].syntax), exact ? "exact" : "unique", m[i].line,
                 j - i);
        cbm_gbuf_insert_edge(edge_out, m[i].src, m[i].tgt, "MENTIONS", props);
        atomic_fetch_add_explicit(&dl->edges, 1, memory_order_relaxed);
        i = j;
    }
}

void cbm_doclinks_resolve_file(cbm_doclinks_t *dl, int file_idx, const CBMFileResult *result,
                               const cbm_gbuf_t *graph, cbm_gbuf_t *edge_out) {
    if (!dl || !result || file_idx < 0 || file_idx >= dl->file_count ||
        result->doc_links.count == 0 || !result->doc_links.items) {
        return;
    }
    const cbm_file_info_t *fi = &dl->files[file_idx];
    int slot = resolver_slot(fi->language);
    const cbm_doclink_resolver_t *R = slot >= 0 ? DOCLINK_RESOLVERS[slot] : NULL;
    const void *index = slot >= 0 ? dl->index[slot] : NULL;
    int n = result->doc_links.count;
    doclink_mention_t *mentions =
        (doclink_mention_t *)cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)n * sizeof(*mentions));
    cbm_doc_link_row_t *rows =
        (cbm_doc_link_row_t *)cbm_calloc(CBM_MEM_CLASS_STORE, (size_t)n * sizeof(*rows));
    if (!mentions || !rows) {
        cbm_free(CBM_MEM_CLASS_OTHER, mentions);
        cbm_free(CBM_MEM_CLASS_STORE, rows);
        mark_failed(dl, fi->rel_path, "alloc");
        return;
    }
    int nm = 0;
    int nr = 0;
    /* the file's own node: looked up once, and only when a reference of the
     * file-level doc resolves */
    const cbm_gbuf_node_t *file_node = NULL;
    bool file_node_looked_up = false;
    for (int i = 0; i < n; i++) {
        const CBMDocLink *link = &result->doc_links.items[i];
        cbm_doclink_outcome_t out = {.kind = CBM_DOCLINK_UNRESOLVED,
                                     .reason = CBM_DOCLINK_REASON_MISSING};
        if (cbm_doclink_syntax_is_external(link->syntax)) {
            out.reason = CBM_DOCLINK_REASON_EXTERNAL;
        } else if (!R || !index) {
            continue; /* no resolver for this language: nothing to say */
        } else {
            R->resolve(index, file_idx, link, graph, &out);
        }
        if (out.kind == CBM_DOCLINK_LOCAL) {
            atomic_fetch_add_explicit(&dl->local_refs, 1, memory_order_relaxed);
            continue;
        }
        if (out.kind == CBM_DOCLINK_EDGE && out.target) {
            const cbm_gbuf_node_t *src = NULL;
            if (link->flags & CBM_DOCLINK_FLAG_FILE) {
                /* written in the file's own doc: the source is the File node */
                if (!file_node_looked_up) {
                    file_node = cbm_pipeline_file_node(graph, dl->project, fi->rel_path);
                    file_node_looked_up = true;
                }
                src = file_node;
            } else {
                src = cbm_gbuf_find_by_qn(graph, link->source_qn);
            }
            if (!src) {
                atomic_fetch_add_explicit(&dl->no_source, 1, memory_order_relaxed);
                continue;
            }
            if (src->id == out.target->id) {
                atomic_fetch_add_explicit(&dl->mentions, 1, memory_order_relaxed);
                atomic_fetch_add_explicit(&dl->self_mentions, 1, memory_order_relaxed);
                continue;
            }
            if (cbm_doclink_syntax_ships(link->syntax)) {
                atomic_fetch_add_explicit(&dl->mentions, 1, memory_order_relaxed);
                mentions[nm++] = (doclink_mention_t){.src = src->id,
                                                     .tgt = out.target->id,
                                                     .line = link->line,
                                                     .syntax = link->syntax,
                                                     .exact = out.exact};
                continue;
            }
            /* the ship gate: resolved, but this link family is below the
             * bar -- a row, not an edge */
            out.reason = CBM_DOCLINK_REASON_BELOW_BAR;
        }
        int reason = out.reason;
        if (reason < 0 || reason >= CBM_DOCLINK_REASON_COUNT) {
            reason = CBM_DOCLINK_REASON_MISSING;
        }
        cbm_doc_link_row_t *row = &rows[nr];
        row->rel_path = dup_or_empty(fi->rel_path);
        row->line = (int)link->line;
        row->syntax = dup_or_empty(cbm_doclink_syntax_name(link->syntax));
        row->raw = dup_or_empty(link->raw);
        row->reason = dup_or_empty(cbm_doclink_reason_name(reason));
        nr++;
        if (!row->rel_path || !row->syntax || !row->raw || !row->reason) {
            mark_failed(dl, fi->rel_path, "alloc");
            break;
        }
        atomic_fetch_add_explicit(&dl->reasons[reason], 1, memory_order_relaxed);
    }
    if (nm > 0) {
        emit_mentions(dl, mentions, nm, edge_out);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, mentions);
    if (nr == 0) {
        cbm_free(CBM_MEM_CLASS_STORE, rows);
        return;
    }
    dl->rows[file_idx].items = rows;
    dl->rows[file_idx].count = nr;
}

/* ── Rows ────────────────────────────────────────────────────────── */

void cbm_doclinks_take_rows(cbm_doclinks_t *dl, cbm_doc_link_row_t **rows, int *count,
                            bool *failed) {
    *rows = NULL;
    *count = 0;
    if (failed) {
        *failed = dl ? atomic_load(&dl->failed) : true;
    }
    if (!dl) {
        return;
    }
    int total = 0;
    for (int i = 0; i < dl->file_count; i++) {
        total += dl->rows[i].count;
    }
    cbm_doc_link_row_t *all =
        total > 0
            ? (cbm_doc_link_row_t *)cbm_alloc(CBM_MEM_CLASS_STORE, (size_t)total * sizeof(*all))
            : NULL;
    if (total > 0 && !all) {
        mark_failed(dl, NULL, "alloc");
        if (failed) {
            *failed = true;
        }
        return; /* the per-file rows are released by cbm_doclinks_free */
    }
    int w = 0;
    for (int i = 0; i < dl->file_count; i++) {
        if (dl->rows[i].count > 0) {
            memcpy(all + w, dl->rows[i].items, (size_t)dl->rows[i].count * sizeof(*all));
            w += dl->rows[i].count;
        }
        cbm_free(CBM_MEM_CLASS_STORE, dl->rows[i].items);
        dl->rows[i].items = NULL;
        dl->rows[i].count = 0;
    }
    *rows = all;
    *count = w;
    char b[8][CBM_SZ_32];
    cbm_log_info("doc_links.done", "edges", itoa64(atomic_load(&dl->edges), b[0], sizeof(b[0])),
                 "mentions", itoa64(atomic_load(&dl->mentions), b[1], sizeof(b[1])), "self",
                 itoa64(atomic_load(&dl->self_mentions), b[2], sizeof(b[2])), "local",
                 itoa64(atomic_load(&dl->local_refs), b[3], sizeof(b[3])), "unresolved",
                 itoa64(w, b[4], sizeof(b[4])), "no_source",
                 itoa64(atomic_load(&dl->no_source), b[5], sizeof(b[5])));
    cbm_log_info(
        "doc_links.unresolved", "missing",
        itoa64(atomic_load(&dl->reasons[CBM_DOCLINK_REASON_MISSING]), b[0], sizeof(b[0])),
        "ambiguous",
        itoa64(atomic_load(&dl->reasons[CBM_DOCLINK_REASON_AMBIGUOUS]), b[1], sizeof(b[1])),
        "external",
        itoa64(atomic_load(&dl->reasons[CBM_DOCLINK_REASON_EXTERNAL]), b[2], sizeof(b[2])),
        "test_only_target",
        itoa64(atomic_load(&dl->reasons[CBM_DOCLINK_REASON_TEST_ONLY]), b[3], sizeof(b[3])),
        "graph_gap",
        itoa64(atomic_load(&dl->reasons[CBM_DOCLINK_REASON_GRAPH_GAP]), b[4], sizeof(b[4])),
        "unparseable",
        itoa64(atomic_load(&dl->reasons[CBM_DOCLINK_REASON_UNPARSEABLE]), b[5], sizeof(b[5])),
        "not_indexed",
        itoa64(atomic_load(&dl->reasons[CBM_DOCLINK_REASON_NOT_INDEXED]), b[6], sizeof(b[6])),
        "below_bar_tier",
        itoa64(atomic_load(&dl->reasons[CBM_DOCLINK_REASON_BELOW_BAR]), b[7], sizeof(b[7])));
}

void cbm_doclinks_free_rows(cbm_doc_link_row_t *rows, int count) {
    cbm_store_free_doc_links(rows, count);
}

void cbm_doclinks_free(cbm_doclinks_t *dl) {
    if (!dl) {
        return;
    }
    for (int s = 0; s < DOCLINK_RESOLVER_COUNT; s++) {
        if (dl->index[s]) {
            DOCLINK_RESOLVERS[s]->destroy(dl->index[s]);
        }
    }
    if (dl->rows) {
        for (int i = 0; i < dl->file_count; i++) {
            cbm_doclinks_free_rows(dl->rows[i].items, dl->rows[i].count);
        }
        cbm_free(CBM_MEM_CLASS_OTHER, dl->rows);
    }
    cbm_arena_destroy(&dl->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, dl);
}

/* ── Pipeline bracket ────────────────────────────────────────────── */

void cbm_doclinks_begin(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count,
                        CBMFileResult **cache) {
    if (!ctx) {
        return;
    }
    ctx->doc_links = cbm_doclinks_build(ctx, files, file_count, cache, ctx->doc_link_base,
                                        ctx->doc_link_base_count, ctx->gbuf);
    if (!ctx->doc_links) {
        ctx->doc_links_failed = true; /* build logged doc_links.error */
    }
}

void cbm_doclinks_end(cbm_pipeline_ctx_t *ctx) {
    if (!ctx) {
        return;
    }
    cbm_doc_link_row_t *rows = NULL;
    int count = 0;
    bool failed = ctx->doc_links_failed;
    if (ctx->doc_links) {
        bool run_failed = false;
        cbm_doclinks_take_rows(ctx->doc_links, &rows, &count, &run_failed);
        failed = failed || run_failed;
        cbm_doclinks_free(ctx->doc_links);
        ctx->doc_links = NULL;
    }
    cbm_pipeline_set_doc_link_rows(ctx->pipeline, rows, count, failed);
    ctx->doc_links_failed = false;
}

int cbm_pipeline_pass_doc_links(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                int file_count) {
    if (!ctx) {
        return 0;
    }
    cbm_doclinks_begin(ctx, files, file_count, ctx->result_cache);
    if (file_count > 0 && !ctx->result_cache) {
        /* Without the result cache this route has nothing to read back. */
        cbm_log_error("doc_links.error", "phase", "sequential", "reason", "no_result_cache");
        ctx->doc_links_failed = true;
    }
    for (int i = 0; ctx->doc_links && ctx->result_cache && i < file_count; i++) {
        bool loaded = false;
        CBMFileResult *r = cbm_pipeline_result_acquire(ctx, ctx->result_cache, i, NULL, &loaded);
        if (r) {
            cbm_doclinks_resolve_file(ctx->doc_links, i, r, ctx->gbuf, ctx->gbuf);
        }
        cbm_pipeline_result_release(r, loaded);
    }
    cbm_doclinks_end(ctx);
    return 0;
}

/* ── Incremental carry-forward ───────────────────────────────────── */

static bool copy_row(cbm_doc_link_row_t *dst, const cbm_doc_link_row_t *src) {
    dst->rel_path = dup_or_empty(src->rel_path);
    dst->line = src->line;
    dst->syntax = dup_or_empty(src->syntax);
    dst->raw = dup_or_empty(src->raw);
    dst->reason = dup_or_empty(src->reason);
    return dst->rel_path && dst->syntax && dst->raw && dst->reason;
}

int cbm_doclinks_merge_rows(const cbm_doc_link_row_t *old_rows, int old_count,
                            const CBMHashTable *replaced, const cbm_doc_link_row_t *fresh,
                            int fresh_count, cbm_doc_link_row_t **out, int *out_count) {
    *out = NULL;
    *out_count = 0;
    int cap = old_count + fresh_count;
    if (cap == 0) {
        return 0;
    }
    cbm_doc_link_row_t *rows =
        (cbm_doc_link_row_t *)cbm_calloc(CBM_MEM_CLASS_STORE, (size_t)cap * sizeof(*rows));
    if (!rows) {
        return CBM_NOT_FOUND;
    }
    int n = 0;
    for (int i = 0; i < old_count; i++) {
        const cbm_doc_link_row_t *r = &old_rows[i];
        if (!r->rel_path || !r->rel_path[0]) {
            continue; /* a previous generation's error marker */
        }
        if (replaced && cbm_ht_get(replaced, r->rel_path)) {
            continue; /* re-extracted or deleted: the fresh rows replace them */
        }
        if (!copy_row(&rows[n++], r)) {
            cbm_doclinks_free_rows(rows, n);
            return CBM_NOT_FOUND;
        }
    }
    for (int i = 0; i < fresh_count; i++) {
        if (!copy_row(&rows[n++], &fresh[i])) {
            cbm_doclinks_free_rows(rows, n);
            return CBM_NOT_FOUND;
        }
    }
    *out = rows;
    *out_count = n;
    return 0;
}

/* ── Stored scopes ───────────────────────────────────────────────── */

/* The surface writer appends "dl" as the LAST top-level key, and a JSON
 * string cannot contain an unescaped quote, so the last `"dl":"` in the row
 * is that key. Only this tail is parsed: a row's "lsp" array is megabytes of
 * definitions nobody needs here (the C# bench corpus holds ~1.5 GB of them). */
int cbm_doclinks_scope_from_surface_json(const char *defs_json, char **out) {
    *out = NULL;
    if (!defs_json) {
        return 0;
    }
    static const char key[] = "\"dl\":\"";
    const size_t kl = sizeof(key) - SKIP_ONE;
    size_t n = strlen(defs_json);
    const char *hit = NULL;
    for (size_t i = n; i >= kl; i--) {
        if (defs_json[i - kl] == '"' && memcmp(defs_json + i - kl, key, kl) == 0) {
            hit = defs_json + i - kl;
            break;
        }
    }
    if (!hit) {
        return 0;
    }
    size_t tail = n - (size_t)(hit - defs_json);
    char *buf = (char *)cbm_alloc(CBM_MEM_CLASS_OTHER, tail + PAIR_LEN);
    if (!buf) {
        return CBM_NOT_FOUND;
    }
    buf[0] = '{';
    memcpy(buf + SKIP_ONE, hit, tail + SKIP_ONE);
    yyjson_doc *doc = yyjson_read(buf, tail + SKIP_ONE, 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    const char *scope = root ? yyjson_get_str(yyjson_obj_get(root, "dl")) : NULL;
    int rc = 0;
    if (!scope) {
        rc = CBM_NOT_FOUND; /* the key is there but not readable: a corrupt row */
    } else {
        *out = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, scope);
        rc = *out ? 0 : CBM_NOT_FOUND;
    }
    yyjson_doc_free(doc);
    cbm_free(CBM_MEM_CLASS_OTHER, buf);
    return rc;
}

int cbm_doclinks_scopes_from_surfaces(const cbm_lsp_surface_row_t *rows, int row_count,
                                      const CBMHashTable *skip, cbm_doclink_scope_t **out,
                                      int *count) {
    *out = NULL;
    *count = 0;
    if (!rows || row_count <= 0) {
        return 0;
    }
    cbm_doclink_scope_t *scopes = NULL;
    int n = 0;
    int cap = 0;
    for (int i = 0; i < row_count; i++) {
        const cbm_lsp_surface_row_t *row = &rows[i];
        if (!row->defs_json || !row->rel_path || (skip && cbm_ht_get(skip, row->rel_path))) {
            continue;
        }
        char *scope = NULL;
        if (cbm_doclinks_scope_from_surface_json(row->defs_json, &scope) != 0) {
            cbm_doclinks_free_scopes(scopes, n);
            return CBM_NOT_FOUND;
        }
        if (!scope) {
            continue; /* no scope: a language without a scope scanner */
        }
        if (n >= cap) {
            int ncap = cap ? cap * PAIR_LEN : CBM_SZ_64;
            cbm_doclink_scope_t *grown = (cbm_doclink_scope_t *)cbm_realloc(
                CBM_MEM_CLASS_OTHER, scopes, (size_t)ncap * sizeof(*grown));
            if (!grown) {
                cbm_free(CBM_MEM_CLASS_OTHER, scope);
                cbm_doclinks_free_scopes(scopes, n);
                return CBM_NOT_FOUND;
            }
            scopes = grown;
            cap = ncap;
        }
        scopes[n].rel_path = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, row->rel_path);
        scopes[n].scope = scope;
        n++;
        if (!scopes[n - SKIP_ONE].rel_path) {
            cbm_doclinks_free_scopes(scopes, n);
            return CBM_NOT_FOUND;
        }
    }
    *out = scopes;
    *count = n;
    return 0;
}

void cbm_doclinks_free_scopes(cbm_doclink_scope_t *scopes, int count) {
    if (!scopes) {
        return;
    }
    for (int i = 0; i < count; i++) {
        cbm_free(CBM_MEM_CLASS_OTHER, (char *)scopes[i].rel_path);
        cbm_free(CBM_MEM_CLASS_OTHER, (char *)scopes[i].scope);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, scopes);
}
