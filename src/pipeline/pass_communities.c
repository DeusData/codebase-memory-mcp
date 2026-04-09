/*
 * pass_communities.c — Pipeline pass that runs Louvain community detection
 * on all service-linking edges and creates Community nodes + MEMBER_OF edges.
 *
 * Runs after pass_servicelinks, before pass_configlink.
 */
#include "pipeline_internal.h"
#include "servicelink.h"
#include "store/store.h"
#include "foundation/log.h"
#include "foundation/compat.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

/* ── Format int to string for logging ───────────────────────── */

static const char *itoa_cm(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Edge types to feed into community detection ────────────── */

/* 3 base edge types + 14 SL_EDGE_* types = 17 total */
static const char *COMMUNITY_EDGE_TYPES[] = {
    "CALLS",
    "HTTP_CALLS",
    "ASYNC_CALLS",
    SL_EDGE_GRAPHQL, SL_EDGE_GRPC, SL_EDGE_KAFKA, SL_EDGE_SQS,
    SL_EDGE_SNS, SL_EDGE_PUBSUB, SL_EDGE_WS, SL_EDGE_SSE,
    SL_EDGE_AMQP, SL_EDGE_MQTT, SL_EDGE_NATS, SL_EDGE_REDIS_PS,
    SL_EDGE_TRPC, SL_EDGE_EVBRIDGE
};
#define COMMUNITY_EDGE_TYPE_COUNT 17

/* ── qsort comparator for int64_t dedup ─────────────────────── */

static int cmp_i64(const void *a, const void *b) {
    int64_t va = *(const int64_t *)a;
    int64_t vb = *(const int64_t *)b;
    return (va > vb) - (va < vb);
}

/* ── Main pass entry point ──────────────────────────────────── */

int cbm_pipeline_pass_communities(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("pass.communities.start");

    /* Step 1: Collect all edges from the 17 edge types */
    int total_edge_cap = 0;
    for (int i = 0; i < COMMUNITY_EDGE_TYPE_COUNT; i++) {
        total_edge_cap += cbm_gbuf_edge_count_by_type(ctx->gbuf,
                                                       COMMUNITY_EDGE_TYPES[i]);
    }

    if (total_edge_cap == 0) {
        cbm_log_info("pass.communities.skip", "reason", "no_edges");
        return 0;
    }

    /* Step 2: Build cbm_louvain_edge_t array */
    cbm_louvain_edge_t *lv_edges = calloc((size_t)total_edge_cap,
                                           sizeof(cbm_louvain_edge_t));
    if (!lv_edges) {
        cbm_log_warn("pass.communities.alloc_fail", "what", "edges");
        return 0;
    }

    /* Also collect raw node IDs for dedup (max 2 per edge) */
    int64_t *raw_ids = calloc((size_t)total_edge_cap * 2, sizeof(int64_t));
    if (!raw_ids) {
        free(lv_edges);
        cbm_log_warn("pass.communities.alloc_fail", "what", "raw_ids");
        return 0;
    }

    int lv_edge_count = 0;
    int raw_id_count = 0;

    for (int i = 0; i < COMMUNITY_EDGE_TYPE_COUNT; i++) {
        const cbm_gbuf_edge_t **edges = NULL;
        int count = 0;
        if (cbm_gbuf_find_edges_by_type(ctx->gbuf, COMMUNITY_EDGE_TYPES[i],
                                         &edges, &count) != 0)
            continue;
        for (int j = 0; j < count; j++) {
            lv_edges[lv_edge_count].src = edges[j]->source_id;
            lv_edges[lv_edge_count].dst = edges[j]->target_id;
            lv_edge_count++;

            raw_ids[raw_id_count++] = edges[j]->source_id;
            raw_ids[raw_id_count++] = edges[j]->target_id;
        }
    }

    if (lv_edge_count == 0) {
        free(lv_edges);
        free(raw_ids);
        cbm_log_info("pass.communities.skip", "reason", "no_edges_collected");
        return 0;
    }

    /* Step 3: Deduplicate node IDs */
    qsort(raw_ids, (size_t)raw_id_count, sizeof(int64_t), cmp_i64);

    int64_t *nodes = calloc((size_t)raw_id_count, sizeof(int64_t));
    if (!nodes) {
        free(lv_edges);
        free(raw_ids);
        cbm_log_warn("pass.communities.alloc_fail", "what", "nodes");
        return 0;
    }

    int node_count = 0;
    for (int i = 0; i < raw_id_count; i++) {
        if (node_count == 0 || raw_ids[i] != nodes[node_count - 1]) {
            nodes[node_count++] = raw_ids[i];
        }
    }
    free(raw_ids);

    cbm_log_info("pass.communities.collected",
                 "edges", itoa_cm(lv_edge_count),
                 "nodes", itoa_cm(node_count));

    /* Step 4: Run Louvain */
    cbm_louvain_result_t *results = NULL;
    int result_count = 0;
    int rc = cbm_louvain(nodes, node_count, lv_edges, lv_edge_count,
                          &results, &result_count);
    free(lv_edges);
    free(nodes);

    if (rc != 0) {
        cbm_log_warn("pass.communities.louvain_error", "rc", itoa_cm(rc));
        free(results);
        return 0; /* non-fatal */
    }

    if (result_count == 0) {
        free(results);
        cbm_log_info("pass.communities.done", "communities", "0", "members", "0");
        return 0;
    }

    /* Step 5: Group results by community ID — find max community */
    int max_community = 0;
    for (int i = 0; i < result_count; i++) {
        if (results[i].community > max_community)
            max_community = results[i].community;
    }

    /* Count members per community */
    int *member_counts = calloc((size_t)(max_community + 1), sizeof(int));
    if (!member_counts) {
        free(results);
        cbm_log_warn("pass.communities.alloc_fail", "what", "member_counts");
        return 0;
    }
    for (int i = 0; i < result_count; i++) {
        member_counts[results[i].community]++;
    }

    /* Step 5b: Create Community nodes */
    int communities_created = 0;
    int64_t *community_node_ids = calloc((size_t)(max_community + 1),
                                          sizeof(int64_t));
    if (!community_node_ids) {
        free(results);
        free(member_counts);
        cbm_log_warn("pass.communities.alloc_fail", "what", "community_node_ids");
        return 0;
    }

    for (int c = 0; c <= max_community; c++) {
        if (member_counts[c] == 0)
            continue;

        char qn[256];
        snprintf(qn, sizeof(qn), "%s.community.%d", ctx->project_name, c);

        char props[64];
        snprintf(props, sizeof(props), "{\"member_count\":%d}", member_counts[c]);

        char name[64];
        snprintf(name, sizeof(name), "community_%d", c);

        int64_t nid = cbm_gbuf_upsert_node(ctx->gbuf, "Community", name, qn,
                                             "", 0, 0, props);
        if (nid == 0) {
            cbm_log_warn("pass.communities.node_fail", "community", itoa_cm(c));
            continue;
        }
        community_node_ids[c] = nid;
        communities_created++;
    }

    /* Step 6: Create MEMBER_OF edges from each member to its community */
    int edges_created = 0;
    for (int i = 0; i < result_count; i++) {
        int c = results[i].community;
        if (community_node_ids[c] == 0)
            continue;

        int64_t eid = cbm_gbuf_insert_edge(ctx->gbuf, results[i].node_id,
                                             community_node_ids[c],
                                             "MEMBER_OF", "{}");
        if (eid != 0)
            edges_created++;
    }

    /* Step 7: Cleanup */
    free(results);
    free(member_counts);
    free(community_node_ids);

    cbm_log_info("pass.communities.done",
                 "communities", itoa_cm(communities_created),
                 "members", itoa_cm(edges_created));

    return 0;
}
