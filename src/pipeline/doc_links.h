/*
 * doc_links.h — doc-comment references -> MENTIONS edges.
 *
 * Extraction (internal/cbm/doclink.h) leaves every documented definition's
 * references in CBMFileResult.doc_links and every file's doc-link scope in
 * CBMFileResult.doc_scope. This layer resolves them in the per-file resolve
 * phase, beside CALLS:
 *
 *   cbm_doclinks_build         once per run, after the run's nodes exist: the
 *                              per-language indexes over every file's scope
 *                              (this run's results, plus the stored scopes of
 *                              files an incremental run did not re-extract)
 *   cbm_doclinks_resolve_file  per file, concurrently: MENTIONS edges into the
 *                              caller's edge buffer, unresolved rows into the
 *                              run's slot for that file
 *   cbm_doclinks_take_rows     the run's unresolved rows, for publication into
 *                              doc_link_unresolved
 *
 * Resolution is guess-free: a reference becomes an edge only when it names
 * one entity EXACTLY (qualified name, doc ID, alias) or UNIQUELY at the first
 * scope level of the language's own lookup rules. Everything else is a row
 * with a reason. One MENTIONS edge per (documented definition, target):
 * {"via":"doc_comment","syntax":..,"tier":..,"line":<first>,"count":n}.
 *
 * Ship gate: a link family (doclink.h) whose tier is below the audit's bar
 * does not ship. Its references still resolve, but a resolved one is written
 * as a row with reason below_bar_tier instead of an edge; an unresolved one
 * keeps its own reason.
 *
 * Per-language hooks: a cbm_doclink_resolver_t (C#: doc_links_cs.c).
 */
#ifndef CBM_PIPELINE_DOC_LINKS_H
#define CBM_PIPELINE_DOC_LINKS_H

#include "pipeline/pipeline_internal.h"
#include "store/store.h"

/* doc_link_unresolved.reason values. */
typedef enum {
    CBM_DOCLINK_REASON_MISSING = 0, /* named code that is not in the graph anywhere */
    CBM_DOCLINK_REASON_AMBIGUOUS,   /* several entities, or an overload group */
    CBM_DOCLINK_REASON_EXTERNAL,    /* outside the repository (URL, BCL, open scope) */
    CBM_DOCLINK_REASON_TEST_ONLY,   /* product code naming a test-only entity */
    CBM_DOCLINK_REASON_NOT_INDEXED, /* on disk, but not in the graph */
    CBM_DOCLINK_REASON_GRAPH_GAP,   /* declared in source, but without a graph node */
    CBM_DOCLINK_REASON_UNPARSEABLE, /* reference syntax not understood */
    CBM_DOCLINK_REASON_BELOW_BAR,   /* resolved, but its link family does not ship
                                     * (cbm_doclink_syntax_ships): below_bar_tier */
    CBM_DOCLINK_REASON_COUNT
} cbm_doclink_reason_t;

const char *cbm_doclink_reason_name(int reason);

typedef enum {
    CBM_DOCLINK_EDGE = 0,   /* target set: one MENTIONS edge */
    CBM_DOCLINK_UNRESOLVED, /* reason set: one doc_link_unresolved row */
    /* Neither an edge nor a row: what the parser took for a reference is no
     * reference to code elsewhere. It names the definition's own parameter or
     * type parameter, or it is text the language's doc tool renders as plain
     * text, which only the resolver can tell (it needs the index). Counted as
     * `local` in the doc_links.done log line, and nowhere in index_status. */
    CBM_DOCLINK_LOCAL,
} cbm_doclink_kind_t;

typedef struct {
    cbm_doclink_kind_t kind;
    const cbm_gbuf_node_t *target;
    bool exact; /* tier: "exact" (qualified/doc ID/alias) vs "unique" (scope lookup) */
    int reason;
} cbm_doclink_outcome_t;

/* A file's stored doc-link scope (incremental runs: files not re-extracted). */
typedef struct cbm_doclink_scope {
    const char *rel_path;
    const char *scope;
} cbm_doclink_scope_t;

typedef struct cbm_doclinks cbm_doclinks_t;

/* The resolve-phase bracket every pipeline route uses: begin builds the run
 * state into ctx->doc_links (over ctx->doc_link_base for an incremental run);
 * resolve_worker / the sequential pass call cbm_doclinks_resolve_file; end
 * hands the rows to ctx->pipeline (cbm_pipeline_set_doc_link_rows) and frees
 * the state. A failed build is logged and recorded, never silent. */
void cbm_doclinks_begin(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count,
                        CBMFileResult **cache);
void cbm_doclinks_end(cbm_pipeline_ctx_t *ctx);

/* Sequential pipelines: begin + resolve every file into ctx->gbuf + end. */
int cbm_pipeline_pass_doc_links(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                int file_count);

/* Rows of the next generation on an incremental run: the previous rows of the
 * files not re-extracted (paths in `replaced` are dropped, as is a previous
 * error marker) followed by this run's rows. Strings are copied; free with
 * cbm_doclinks_free_rows. Returns 0, -1 on allocation failure. */
int cbm_doclinks_merge_rows(const cbm_doc_link_row_t *old_rows, int old_count,
                            const CBMHashTable *replaced, const cbm_doc_link_row_t *fresh,
                            int fresh_count, cbm_doc_link_row_t **out, int *out_count);

/* Build the run's resolver state. `files[0..file_count)` / `cache` are the
 * files extracted in this run (results read through the spill contract);
 * `base` holds the scopes of the files it did not re-extract (NULL/0 on a full
 * run); `graph` is the run's complete node set, read-only from here on.
 * Returns NULL only when allocation failed (logged). */
cbm_doclinks_t *cbm_doclinks_build(const cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                   int file_count, CBMFileResult **cache,
                                   const cbm_doclink_scope_t *base, int base_count,
                                   const cbm_gbuf_t *graph);

/* Resolve files[file_idx]'s references. Concurrent calls for different files
 * are safe. Edges go to `edge_out` (the worker's buffer, or the graph itself
 * on a sequential run). */
void cbm_doclinks_resolve_file(cbm_doclinks_t *dl, int file_idx, const CBMFileResult *result,
                               const cbm_gbuf_t *graph, cbm_gbuf_t *edge_out);

/* Move the run's unresolved rows out (file order, then line order) together
 * with the failure flag; the caller frees them with cbm_doclinks_free_rows.
 * Logs one summary line. */
void cbm_doclinks_take_rows(cbm_doclinks_t *dl, cbm_doc_link_row_t **rows, int *count,
                            bool *failed);
void cbm_doclinks_free_rows(cbm_doc_link_row_t *rows, int count);
void cbm_doclinks_free(cbm_doclinks_t *dl);

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Test seam: the next cbm_doclinks_build fails the way an allocation failure
 * does (logged, NULL), so a test can follow a failed doc-link layer through
 * publication and index_status. Test builds only. */
void cbm_doclinks_test_fail_build_once(void);
#endif

/* The stored doc-link scopes of a previous generation (lsp_surface rows,
 * key "dl"), excluding `skip` paths. Strings are heap-owned by *out; free
 * with cbm_doclinks_free_scopes. Returns 0, or -1 when a row is malformed. */
int cbm_doclinks_scopes_from_surfaces(const cbm_lsp_surface_row_t *rows, int row_count,
                                      const CBMHashTable *skip, cbm_doclink_scope_t **out,
                                      int *count);
void cbm_doclinks_free_scopes(cbm_doclink_scope_t *scopes, int count);
/* The "dl" scope of one surface JSON: *out is a heap copy, or NULL when the
 * row has none. Returns 0, -1 when the key is unreadable or memory ran out. */
int cbm_doclinks_scope_from_surface_json(const char *defs_json, char **out);

/* ── Per-language resolver hooks ─────────────────────────────────── */

typedef struct {
    const char *rel_path;
    const char *scope; /* the file's doc-link scope blob (NULL when it has none) */
    int run_file;      /* index into the run's files[], or -1 for a base file */
} cbm_doclink_file_t;

/* What `build` is handed. LIFETIMES: the struct and its `files` array are
 * valid only during the `build` call (the array is freed right after it): a
 * resolver must not keep either pointer. The `rel_path` and `scope` strings
 * the array points to stay valid until `destroy`, so an index may keep those
 * without copying them. */
typedef struct {
    const cbm_pipeline_ctx_t *ctx;
    const cbm_gbuf_t *graph;
    const cbm_doclink_file_t *files; /* every file of the language, sorted by rel_path */
    int file_count;
    int run_file_count; /* size of the run's files[] (run_file indexes into it) */
} cbm_doclink_build_in_t;

/* Receives one name a changed file no longer declares. false when it cannot be
 * recorded: the hook then reports failure, and the caller fails closed. */
typedef bool (*cbm_doclink_name_fn)(void *ud, const char *name, size_t len);

/* What a changed file's scope means for the files an incremental run does NOT
 * re-extract. */
typedef enum {
    CBM_DOCLINK_DELTA_LOCAL = 0, /* nothing outside the file resolves differently, except
                                  * unresolved references that name a reported name */
    CBM_DOCLINK_DELTA_GLOBAL,    /* any file may resolve differently: not repairable
                                  * file by file */
} cbm_doclink_delta_t;

enum { CBM_DOCLINK_RESOLVER_LANGS = 4 };

/* Everything the resolving half needs from a language leg: one of these, and
 * its pointer in doc_links.c's resolver table. */
typedef struct {
    /* The languages whose files it resolves, in ONE index: a leg whose
     * references cross language values (TypeScript, TSX and JavaScript) lists
     * them all. No language may be listed by two resolvers. */
    CBMLanguage langs[CBM_DOCLINK_RESOLVER_LANGS];
    int lang_count;
    /* Tag line of its scope blobs (doclink.h); NULL: it has none. */
    const char *scope_tag;
    /* The language's project-wide index; NULL on allocation failure. */
    void *(*build)(const cbm_doclink_build_in_t *in);
    void (*destroy)(void *index);
    /* Resolve one reference of run file `run_file` (its own scope is in the
     * index). Thread-safe: the index is read-only after build. */
    void (*resolve)(const void *index, int run_file, const CBMDocLink *link,
                    const cbm_gbuf_t *graph, cbm_doclink_outcome_t *out);
    /* Incremental runs; both optional.
     * scope_input  true for a file that is no source of the language and has
     *              no scope blob, but sets the scope of its files. A change
     *              to one is GLOBAL. (C# needs none: its MSBuild project
     *              files have scope blobs of their own.)
     * scope_delta  compare a changed file's stored and fresh scope blobs (both
     *              of this language) and report the names it no longer
     *              declares through `removed`. Returns a cbm_doclink_delta_t,
     *              or -1 on failure. Without the hook every change is GLOBAL. */
    bool (*scope_input)(const char *rel_path);
    int (*scope_delta)(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                       void *ud);
} cbm_doclink_resolver_t;

extern const cbm_doclink_resolver_t cbm_doclink_cs_resolver;

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Test seam (doc_links_cs.c): the scope levels and overloads the C# resolver
 * looked at since the last reset. A test holds it against the size of its
 * input, so that a lookup whose cost grows faster than its input fails
 * without a clock. Test builds only. */
void cbm_doclink_cs_test_work_reset(void);
uint64_t cbm_doclink_cs_test_work(void);
#endif

/* True when `rel_path` is a scope input of some language (scope_input). */
bool cbm_doclinks_is_scope_input(const char *rel_path);

/* The scope delta of one changed file (`fresh` set) or deleted file (`fresh`
 * NULL): a cbm_doclink_delta_t, or -1 on failure. A file with no scope before
 * and after is LOCAL; a scope that appears, disappears or changes its
 * language is GLOBAL; otherwise the language's scope_delta hook decides. */
int cbm_doclinks_scope_delta(const char *stored, const char *fresh, cbm_doclink_name_fn removed,
                             void *ud);

#endif /* CBM_PIPELINE_DOC_LINKS_H */
