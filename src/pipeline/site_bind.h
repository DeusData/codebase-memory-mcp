#ifndef CBM_SITE_BIND_H
#define CBM_SITE_BIND_H

/*
 * site_bind.h — the one place where the shared cross-file layer answers a
 * call or usage site.
 *
 * Both call resolvers (pass_calls.c, pass_parallel.c) and both usage
 * resolvers (pass_usages.c, pass_parallel.c) ask here at the same sites and
 * apply the answer the same way, so the two routes cannot drift:
 *
 *   asked     a call site that no engine row matched (no CBMResolvedCall)
 *             and that does not require one, before main's service
 *             classification; a usage site that no engine row was found
 *             for, after the C member join and before the PHP vendor check
 *             and main's name chain.
 *   PROVEN    replaces main's name chain at the site (strategy
 *             `include_scope`, confidence 0.95). A call keeps every later
 *             step of main (service and route classification, spawn, field
 *             policy, guards) on the answer; a usage takes the target.
 *   POSSIBLE  main's chain decides the site as before. When the file is done
 *             (cbm_pipeline_site_bind_end) the layer draws its own edge
 *             (`include_scope_may`, confidence 0.85) only where main drew no
 *             edge of that type at the site; where main drew the same target
 *             nothing is added; where main drew another target main's edge
 *             stays alone and the disagreement is counted.
 *   NO_ANSWER main's chain, unchanged. A proven absence is counted only.
 *
 * Rows: the scope rows of scope_closure.h (Protobuf). A usage is answered
 * only at the last name of a type reference the scope facts recorded (a
 * QREF event ending where the usage ends), read as written. Calls of a row
 * language are not answered yet (Protobuf has none).
 *
 * The run's scope (cbm_site_scope_t) is built once after IMPORTS, on one
 * thread, before any resolver starts, and is read only afterwards. Each
 * resolver thread brings its own memo (cbm_site_memo_t, created on its first
 * lookup); no answer depends on what a memo holds.
 */

#include "cbm.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"

#include <stdbool.h>
#include <stddef.h>

typedef enum {
    CBM_SITE_NO_ANSWER = 0, /* main's chain decides, unchanged */
    CBM_SITE_PROVEN,        /* replaces main's chain at this site */
    CBM_SITE_POSSIBLE,      /* main's chain decides; an own edge may follow at the file's end */
} cbm_site_verdict_t;

typedef struct {
    cbm_site_verdict_t verdict;
    /* PROVEN only: the answer in the shape main's chain returns
     * (qualified_name is target's QN, candidate_count 1), and its node,
     * which the run's graph is known to hold. */
    cbm_resolution_t res;
    const cbm_gbuf_node_t *target;
} cbm_site_answer_t;

/* The run's scope: NULL when no discovered file has a row. */
typedef struct cbm_site_scope cbm_site_scope_t;

/* One resolver thread's memo (opaque); NULL until its first lookup. */
typedef struct cbm_site_memo cbm_site_memo_t;
void cbm_pipeline_site_memo_free(cbm_site_memo_t *m);

typedef struct sb_pending sb_pending_t;

/* One file's state, on the resolver's stack between begin and end. */
typedef struct {
    const cbm_pipeline_ctx_t *ctx;
    const cbm_gbuf_t *graph; /* the run's nodes, read-only while resolving */
    const CBMFileResult *result;
    const char *rel;
    CBMLanguage lang;
    bool active;                     /* false: no row covers this file, every site is NO_ANSWER */
    int file;                        /* the file in the run's scope (active only) */
    cbm_site_memo_t **memo;          /* the thread's memo slot */
    const CBMCallEvidence *evidence; /* per call: what main emitted (calls), or NULL */
    sb_pending_t *pending;           /* POSSIBLE answers left to the end step */
    int npending;
    int cap_pending;
    int proven;
    int possible;
    int absent;
} cbm_site_file_t;

/* Build the run's scope from every discovered file of a row language: its
 * fresh result (cache[i] for fresh[i], read through the spill-aware
 * accessor) or, for a file of `all` without one, its stored facts (the
 * surface row's "is" key; incremental closure repair). A row-language file
 * with neither takes part as unreadable (nothing proven through it). On one
 * thread, after IMPORTS and before any resolver. NULL: no row-language file
 * (or out of memory): every site gets NO_ANSWER. */
cbm_site_scope_t *cbm_pipeline_site_scope_build(const cbm_pipeline_ctx_t *ctx,
                                                const cbm_file_info_t *fresh, int fresh_count,
                                                CBMFileResult **cache, const cbm_file_info_t *all,
                                                int all_count, const cbm_lsp_surface_row_t *stored,
                                                int stored_count);
/* Log the run's counts and free. */
void cbm_pipeline_site_scope_free(cbm_site_scope_t *sc);

/* Start one file: before its first site is asked. `memo`: the calling
 * thread's slot; `evidence`: the file's per-call evidence (calls pass), or
 * NULL. */
void cbm_pipeline_site_bind_begin(cbm_site_file_t *sf, const cbm_pipeline_ctx_t *ctx,
                                  const cbm_gbuf_t *graph, const char *rel, CBMLanguage lang,
                                  const CBMFileResult *result, cbm_site_memo_t **memo,
                                  const CBMCallEvidence *evidence);

/* The answer for one call of sf->result (an element of result->calls),
 * made in the function or file node `source`. */
cbm_site_answer_t cbm_pipeline_site_bind_call(cbm_site_file_t *sf, const CBMCall *call,
                                              const cbm_gbuf_node_t *source);

/* The answer for one usage of sf->result (an element of result->usages),
 * made in `source`. */
cbm_site_answer_t cbm_pipeline_site_bind_usage(cbm_site_file_t *sf, const CBMUsage *usage,
                                               const cbm_gbuf_node_t *source);

/* Main's chain drew a USAGE edge to `target` at a usage the layer answered
 * POSSIBLE (both usage resolvers call it right after drawing). */
void cbm_pipeline_site_bind_usage_drawn(cbm_site_file_t *sf, const CBMUsage *usage,
                                        const cbm_gbuf_node_t *target);

/* The properties of a USAGE edge the layer draws: the usage's name, the
 * answer's confidence and strategy. */
void cbm_pipeline_site_usage_props(char *buf, size_t cap, const CBMUsage *usage,
                                   cbm_site_verdict_t verdict);

/* End one file: after its last site, with the buffer its edges went to
 * (the run's graph on the sequential route, the worker's edge buffer on the
 * parallel one). Draws what POSSIBLE answers leave to this step. */
void cbm_pipeline_site_bind_end(cbm_site_file_t *sf, cbm_gbuf_t *edges);

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Add a mark: every call and usage named `name` that the layer is asked
 * about, in a file of any language, answers `verdict` with the node
 * `target_qn` (PROVEN or POSSIBLE). Up to sixteen marks; a NULL name clears
 * them. Set only while no index runs. */
void cbm_pipeline_site_bind_test_mark(const char *name, const char *target_qn,
                                      cbm_site_verdict_t verdict);
#endif

#endif /* CBM_SITE_BIND_H */
