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
 *   PROVEN    replaces main's name chain at the site. A call keeps every
 *             later step of main (service and route classification, spawn,
 *             field policy, guards) on the answer; a usage takes the target.
 *   POSSIBLE  main's chain decides the site as before. The layer may draw
 *             its own edge for it when the file is done
 *             (cbm_pipeline_site_bind_end), and only where main drew no
 *             edge of that type between the same two nodes.
 *   NO_ANSWER main's chain, unchanged.
 *
 * Rules and their rows live in site_bind.c; a resolver is never edited for
 * a new rule. No language has a row yet: every file is inactive, every site
 * gets NO_ANSWER and the end step draws nothing, so the graph is main's on
 * both routes.
 *
 * Threads: one cbm_site_file_t per file being resolved, on the stack of the
 * worker resolving it. Whatever run-wide input a row reads is built and
 * sealed before any resolver starts.
 */

#include "cbm.h"
#include "graph_buffer/graph_buffer.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"

#include <stdbool.h>

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

/* One file's state, on the resolver's stack between begin and end. */
typedef struct {
    const cbm_pipeline_ctx_t *ctx;
    const cbm_gbuf_t *graph; /* the run's nodes, read-only while resolving */
    const CBMFileResult *result;
    const char *rel;
    CBMLanguage lang;
    bool active; /* false: no row covers this file, every site is NO_ANSWER */
} cbm_site_file_t;

/* Start one file: before its first site is asked. */
void cbm_pipeline_site_bind_begin(cbm_site_file_t *sf, const cbm_pipeline_ctx_t *ctx,
                                  const cbm_gbuf_t *graph, const char *rel, CBMLanguage lang,
                                  const CBMFileResult *result);

/* The answer for one call of sf->result (an element of result->calls),
 * made in the function or file node `source`. */
cbm_site_answer_t cbm_pipeline_site_bind_call(cbm_site_file_t *sf, const CBMCall *call,
                                              const cbm_gbuf_node_t *source);

/* The answer for one usage of sf->result (an element of result->usages),
 * made in `source`. */
cbm_site_answer_t cbm_pipeline_site_bind_usage(cbm_site_file_t *sf, const CBMUsage *usage,
                                               const cbm_gbuf_node_t *source);

/* End one file: after its last site, with the buffer its edges went to
 * (the run's graph on the sequential route, the worker's edge buffer on the
 * parallel one). Draws what POSSIBLE answers leave to this step. */
void cbm_pipeline_site_bind_end(cbm_site_file_t *sf, cbm_gbuf_t *edges);

#endif /* CBM_SITE_BIND_H */
