/*
 * pass_importance.c — Index-time persisted per-symbol importance score
 * (spec Part 1 / P3, AC1). Predump pass: computes a per-symbol importance for
 * every Function/Method/Class node and appends it as a numeric "importance"
 * key on the node's properties_json, so it persists through the store and is
 * read back by enrich_node_properties (mcp) for free.
 *
 * Model (Aider repo-map, validated by the GREEN spike
 * pai/aider-repomap-codebase-memory-mapping; the multipliers alone denoised
 * the real connectors graph):
 *
 *   importance = sqrt(num_refs) * priv * generic * distinct * test_penalty
 *     num_refs      = incoming CALLS + USAGE edges (both). 0 -> sqrt(0) = 0.
 *     priv     0.1  if the name is private (leading underscore)
 *     generic  0.1  if the name is DEFINED in >= 5 distinct files
 *     distinct 10   if the name is snake_case or camelCase AND len >= 8
 *     test_penalty 0.1 if the symbol is test scaffolding:
 *                  - the symbol is the TARGET of an incoming TESTS edge
 *                    (create_tests_edges: test-fn -> prod helper living in a
 *                    NON-test file, e.g. a fixture in testutil.go), OR
 *                  - the symbol lives in a test file per the graph's canonical
 *                    cbm_is_test_path() classifier (the SAME one pass_tests
 *                    uses).
 *
 * REAL-DATA DEVIATION FROM THE SPEC'S "edge-based TESTS/TESTS_FILE" mechanism
 * (surfaced to Peter, documented in builder-notes.md): on the real connectors
 * graph the TESTS/TESTS_FILE edges alone do NOT achieve AC1's exclusion goal.
 * TESTS_FILE is emitted only when a test file maps to a resolvable production
 * file (15 edges total on connectors); TESTS edges never point at a symbol that
 * lives in a test file (create_tests_edges excludes tgt_is_test); and the node
 * is_test property is stamped on <2% of nodes and on NONE of the test-resident
 * fixtures/classes. So edge-only left the entire top-40 as test scaffolding
 * (session_dir, FakeConnectorClient, ...). cbm_is_test_path() is the graph's
 * own precise classifier (test_ prefix, _test.<ext> suffix, /tests/ dir, ...) —
 * it is NOT the naive strstr("test") the spec warned against: it correctly
 * leaves e.g. src/testutil_helpers.go UNpenalised (test-plan row #6 inversion).
 * The TESTS-edge leg is retained so prod helpers in non-test files that are
 * exercised by tests are still demoted.
 *
 * Weighted-degree only. PageRank (transitive importance) is a measured
 * refinement deliberately NOT built here — the spike showed the weighted
 * multipliers alone remove the degree-noise, so PageRank stays deferred
 * unless it measurably beats weighted-degree on a fixed judgment set (AC1
 * "measured decision"; see builder-notes.md).
 *
 * ORDERING (load-bearing): registered LAST in run_predump_passes so it runs
 * after pass_tests (TESTS/TESTS_FILE edges) and after CALLS/USAGE extraction;
 * a mis-ordered pass would silently see num_refs = 0 and no test penalty.
 */
#include "foundation/constants.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "cbm.h"

#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>

enum {
    CBM_IMPORTANCE_GENERIC_MIN_FILES = 5, /* name defined in >= N files -> generic */
    CBM_IMPORTANCE_DISTINCT_MIN_LEN = 8,  /* distinctive-identifier length floor */
};
static const double CBM_IMPORTANCE_PRIV_MUL = 0.1;
static const double CBM_IMPORTANCE_GENERIC_MUL = 0.1;
static const double CBM_IMPORTANCE_DISTINCT_MUL = 10.0;
static const double CBM_IMPORTANCE_TEST_MUL = 0.1;

/* The symbol node labels that carry an importance score. File/Module and other
 * non-symbol nodes are deliberately excluded (test-plan row #1). */
static const char *const CBM_IMPORTANCE_LABELS[] = {"Function", "Method", "Class"};
enum { CBM_IMPORTANCE_LABEL_COUNT = 3 };

static bool name_is_private(const char *name) {
    return name != NULL && name[0] == '_';
}

/* Distinctive = (snake_case OR camelCase) AND length >= 8. snake_case is an
 * embedded '_'; camelCase is a lower->upper "hump". A plain len>=8 lowercase
 * word (no '_', no hump) is NOT distinctive. */
static bool name_is_distinctive(const char *name) {
    if (!name) {
        return false;
    }
    size_t len = strlen(name);
    if (len < (size_t)CBM_IMPORTANCE_DISTINCT_MIN_LEN) {
        return false;
    }
    if (strchr(name, '_') != NULL) {
        return true; /* snake_case */
    }
    for (size_t i = 1; i < len; i++) {
        if (islower((unsigned char)name[i - 1]) && isupper((unsigned char)name[i])) {
            return true; /* camelCase hump */
        }
    }
    return false;
}

/* Count the DISTINCT files a name is defined in (generic-name suppression).
 * Counts distinct file paths, not distinct nodes — two defs of one name in the
 * same file count once (test-plan row #4). */
static int name_distinct_file_count(const cbm_gbuf_t *gb, const char *name) {
    if (!name) {
        return 0;
    }
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_name(gb, name, &nodes, &count) != 0 || count == 0) {
        return 0;
    }
    int distinct = 0;
    for (int i = 0; i < count; i++) {
        const char *fp = nodes[i]->file_path;
        if (!fp) {
            continue;
        }
        bool seen = false;
        for (int j = 0; j < i; j++) {
            const char *pf = nodes[j]->file_path;
            if (pf && strcmp(pf, fp) == 0) {
                seen = true;
                break;
            }
        }
        if (!seen) {
            distinct++;
        }
    }
    return distinct;
}

static int incoming_edge_count(const cbm_gbuf_t *gb, int64_t id, const char *type) {
    const cbm_gbuf_edge_t **edges = NULL;
    int ne = 0;
    if (cbm_gbuf_find_edges_by_target_type(gb, id, type, &edges, &ne) != 0) {
        return 0;
    }
    return ne;
}

/* Append a numeric "importance" key to a node's properties JSON object. Mirrors
 * append_complexity_props: copy without the trailing '}', append the key, close.
 * A non-object properties_json (does not end in '}') is left untouched — the
 * store-open malformed-JSON guard (store.c) then never sees corruption. */
void cbm_pipeline_importance_append_prop(cbm_gbuf_node_t *node, double score) {
    const char *old = node->properties_json ? node->properties_json : "{}";
    size_t olen = strlen(old);
    if (olen < 2 || old[olen - 1] != '}') {
        return; /* not a JSON object — leave untouched */
    }
    bool empty = (olen == 2); /* "{}" */
    char *neu = malloc(olen + CBM_SZ_64);
    if (!neu) {
        return;
    }
    memcpy(neu, old, olen - 1); /* copy without the trailing '}' */
    int w =
        snprintf(neu + (olen - 1), CBM_SZ_64, "%s\"importance\":%.6f}", empty ? "" : ",", score);
    if (w < 0) {
        free(neu);
        return;
    }
    free(node->properties_json);
    node->properties_json = neu;
}

void cbm_pipeline_pass_importance(cbm_pipeline_ctx_t *ctx) {
    cbm_gbuf_t *gb = ctx->gbuf;
    if (!gb) {
        return;
    }

    int updated = 0;
    for (int li = 0; li < CBM_IMPORTANCE_LABEL_COUNT; li++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, CBM_IMPORTANCE_LABELS[li], &nodes, &count) != 0) {
            continue;
        }
        for (int i = 0; i < count; i++) {
            cbm_gbuf_node_t *n = (cbm_gbuf_node_t *)nodes[i];

            int num_refs =
                incoming_edge_count(gb, n->id, "CALLS") + incoming_edge_count(gb, n->id, "USAGE");
            double score = sqrt((double)num_refs);

            if (name_is_private(n->name)) {
                score *= CBM_IMPORTANCE_PRIV_MUL;
            }
            if (name_distinct_file_count(gb, n->name) >= CBM_IMPORTANCE_GENERIC_MIN_FILES) {
                score *= CBM_IMPORTANCE_GENERIC_MUL;
            }
            if (name_is_distinctive(n->name)) {
                score *= CBM_IMPORTANCE_DISTINCT_MUL;
            }
            bool is_test =
                cbm_is_test_path(n->file_path) || incoming_edge_count(gb, n->id, "TESTS") > 0;
            if (is_test) {
                score *= CBM_IMPORTANCE_TEST_MUL;
            }

            cbm_pipeline_importance_append_prop(n, score);
            updated++;
        }
    }

    char buf[CBM_SZ_32];
    snprintf(buf, sizeof(buf), "%d", updated);
    cbm_log_info("pass.importance", "symbols", buf);
}
