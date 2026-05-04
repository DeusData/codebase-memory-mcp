/*
 * pass_cross_repo.h — Cross-repo intelligence: match Routes, Channels, and
 * async topics across indexed projects to create CROSS_* edges.
 */
#ifndef CBM_PASS_CROSS_REPO_H
#define CBM_PASS_CROSS_REPO_H

#include "store/store.h"

/* ── CROSS_* edge type names ─────────────────────────────────────
 *
 * Upstream Route-QN matcher emits the first six. The messaging matcher
 * (pass_crossrepolinks.c) emits the remaining eleven.
 */
#define CBM_EDGE_CROSS_HTTP_CALLS    "CROSS_HTTP_CALLS"
#define CBM_EDGE_CROSS_ASYNC_CALLS   "CROSS_ASYNC_CALLS"
#define CBM_EDGE_CROSS_CHANNEL       "CROSS_CHANNEL"
#define CBM_EDGE_CROSS_GRPC_CALLS    "CROSS_GRPC_CALLS"
#define CBM_EDGE_CROSS_GRAPHQL_CALLS "CROSS_GRAPHQL_CALLS"
#define CBM_EDGE_CROSS_TRPC_CALLS    "CROSS_TRPC_CALLS"

#define CBM_EDGE_CROSS_KAFKA_CALLS        "CROSS_KAFKA_CALLS"
#define CBM_EDGE_CROSS_SQS_CALLS          "CROSS_SQS_CALLS"
#define CBM_EDGE_CROSS_SNS_CALLS          "CROSS_SNS_CALLS"
#define CBM_EDGE_CROSS_EVENTBRIDGE_CALLS  "CROSS_EVENTBRIDGE_CALLS"
#define CBM_EDGE_CROSS_PUBSUB_CALLS       "CROSS_PUBSUB_CALLS"
#define CBM_EDGE_CROSS_AMQP_CALLS         "CROSS_AMQP_CALLS"
#define CBM_EDGE_CROSS_MQTT_CALLS         "CROSS_MQTT_CALLS"
#define CBM_EDGE_CROSS_NATS_CALLS         "CROSS_NATS_CALLS"
#define CBM_EDGE_CROSS_REDIS_PUBSUB_CALLS "CROSS_REDIS_PUBSUB_CALLS"
#define CBM_EDGE_CROSS_WS_CALLS           "CROSS_WS_CALLS"
#define CBM_EDGE_CROSS_SSE_CALLS          "CROSS_SSE_CALLS"

/* All messaging CROSS_* edge types produced by pass_crossrepolinks.c.
 * Used for idempotent cleanup before re-emission and for MCP queries. */
extern const char *const CBM_MESSAGING_CROSS_EDGE_TYPES[];
#define CBM_MESSAGING_CROSS_EDGE_TYPE_COUNT 11

/* Map a messaging protocol name (e.g. "kafka") to its CROSS_* edge type
 * constant (e.g. "CROSS_KAFKA_CALLS"). Returns NULL for unknown/skipped
 * protocols ("http", "grpc", "graphql", "trpc" are owned by the upstream
 * Route-QN matcher and intentionally return NULL here). */
const char *cbm_messaging_protocol_to_cross_edge(const char *protocol);

/* Result of a cross-repo matching run. */
typedef struct {
    int http_edges;    /* CROSS_HTTP_CALLS edges created */
    int async_edges;   /* CROSS_ASYNC_CALLS edges created */
    int channel_edges; /* CROSS_CHANNEL edges created */
    int grpc_edges;    /* CROSS_GRPC_CALLS edges created */
    int graphql_edges; /* CROSS_GRAPHQL_CALLS edges created */
    int trpc_edges;    /* CROSS_TRPC_CALLS edges created */
    int projects_scanned;
    double elapsed_ms;
} cbm_cross_repo_result_t;

/* Run cross-repo matching for `project` against `target_projects`.
 * If target_count == 1 and target_projects[0] == "*", matches against all
 * indexed projects. Writes CROSS_* edges bidirectionally into both the
 * source and target project DBs.
 *
 * `project` must already be indexed (its .db must exist).
 * Returns result with edge counts. */
cbm_cross_repo_result_t cbm_cross_repo_match(const char *project, const char **target_projects,
                                             int target_count);

#endif /* CBM_PASS_CROSS_REPO_H */
