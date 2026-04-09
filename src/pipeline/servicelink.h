/*
 * servicelink.h — Shared types and declarations for cross-service protocol linking.
 *
 * Each protocol linker discovers producers/consumers in source code and creates
 * typed edges (GRAPHQL_CALLS, KAFKA_CALLS, etc.) in the graph buffer.
 */
#ifndef CBM_SERVICELINK_H
#define CBM_SERVICELINK_H

#include "pipeline_internal.h"
#include "pipeline.h"                 /* cbm_confidence_band */
#include "foundation/compat_regex.h"  /* portable regex: cbm_regex_t, cbm_regcomp, etc. */
#include "foundation/log.h"           /* cbm_log_info, cbm_log_warn, cbm_log_error */
#include "foundation/platform.h"      /* safe_realloc */

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Buffer limits ──────────────────────────────────────────── */
#define SL_MAX_PRODUCERS  8192
#define SL_MAX_CONSUMERS  8192
#define SL_MAX_PER_NODE   64     /* max discoveries per single function node */
#define SL_MIN_CONFIDENCE 0.25   /* minimum confidence to create an edge */

/* ── Edge type constants ────────────────────────────────────── */
#define SL_EDGE_GRAPHQL   "GRAPHQL_CALLS"
#define SL_EDGE_GRPC      "GRPC_CALLS"
#define SL_EDGE_KAFKA     "KAFKA_CALLS"
#define SL_EDGE_SQS       "SQS_CALLS"
#define SL_EDGE_SNS       "SNS_CALLS"
#define SL_EDGE_PUBSUB    "PUBSUB_CALLS"
#define SL_EDGE_WS        "WS_CALLS"
#define SL_EDGE_SSE       "SSE_CALLS"
#define SL_EDGE_AMQP      "AMQP_CALLS"
#define SL_EDGE_MQTT      "MQTT_CALLS"
#define SL_EDGE_NATS      "NATS_CALLS"
#define SL_EDGE_REDIS_PS  "REDIS_PUBSUB_CALLS"
#define SL_EDGE_TRPC      "TRPC_CALLS"
#define SL_EDGE_EVBRIDGE  "EVENTBRIDGE_CALLS"

/* ── All edge types for cleanup (defined in pass_servicelinks.c) ── */
extern const char *SL_ALL_EDGE_TYPES[];
#define SL_EDGE_TYPE_COUNT 14

/* ── Generic producer/consumer structs ──────────────────────── */

typedef struct {
    char identifier[256];        /* topic, subject, channel, operation, procedure */
    char source_qn[512];         /* qualified name of producing function */
    int64_t source_id;           /* gbuf node ID */
    char file_path[256];         /* file where discovered */
    char extra[256];             /* protocol-specific: method, exchange, qos, etc. */
} cbm_sl_producer_t;

typedef struct {
    char identifier[256];        /* topic, subject, channel, operation, procedure */
    char handler_qn[512];        /* qualified name of consuming function */
    int64_t handler_id;          /* gbuf node ID */
    char file_path[256];         /* file where discovered */
    char extra[256];             /* protocol-specific metadata */
} cbm_sl_consumer_t;

/* ── Linker result ──────────────────────────────────────────── */

typedef struct {
    const char *name;            /* protocol name for logging */
    int links_created;
    int producers_found;
    int consumers_found;
} cbm_sl_result_t;

/* ── Helper: read source lines from disk ───────────────────── */

static inline char *sl_read_source_lines(const char *root_dir, const char *rel_path,
                                         int start_line, int end_line) {
    char full_path[2048];
    snprintf(full_path, sizeof(full_path), "%s/%s", root_dir, rel_path);

    FILE *f = fopen(full_path, "r");
    if (!f) {
        return NULL;
    }

    char *result = NULL;
    int result_len = 0;
    int result_cap = 0;
    int line = 0;
    char line_buf[4096];

    while (fgets(line_buf, sizeof(line_buf), f)) {
        line++;
        if (line < start_line) {
            continue;
        }
        if (line > end_line) {
            break;
        }

        int llen = (int)strlen(line_buf);
        if (llen > 0 && line_buf[llen - 1] == '\n') {
            line_buf[--llen] = '\0';
        }

        if (result_len > 0) {
            if (result_len + 1 >= result_cap) {
                result_cap = (result_cap == 0) ? 1024 : result_cap * 2;
                result = safe_realloc(result, (size_t)result_cap);
            }
            result[result_len++] = '\n';
        }

        if (result_len + llen >= result_cap) {
            result_cap = result_len + llen + 256;
            result = safe_realloc(result, (size_t)result_cap);
        }
        memcpy(result + result_len, line_buf, (size_t)llen);
        result_len += llen;
    }

    (void)fclose(f);
    if (result) {
        result[result_len] = '\0';
    }
    return result;
}

static inline char *sl_read_node_source(const cbm_pipeline_ctx_t *ctx,
                                        const cbm_gbuf_node_t *node) {
    return sl_read_source_lines(ctx->repo_path, node->file_path,
                                node->start_line, node->end_line);
}

/* ── Helper: normalized Levenshtein similarity (0.0–1.0) ───── */

static inline double cbm_normalized_levenshtein(const char *a, const char *b) {
    if (strcmp(a, b) == 0) {
        return 1.0;
    }
    int la = (int)strlen(a);
    int lb = (int)strlen(b);
    int max_len = la > lb ? la : lb;
    if (max_len == 0) {
        return 1.0;
    }

    /* Compute Levenshtein distance with two-row DP */
    int *prev = (int *)calloc((size_t)(lb + 1), sizeof(int));
    int *curr = (int *)calloc((size_t)(lb + 1), sizeof(int));
    if (!prev || !curr) {
        free(prev);
        free(curr);
        return 0.0;
    }
    for (int j = 0; j <= lb; j++) {
        prev[j] = j;
    }
    for (int i = 1; i <= la; i++) {
        curr[0] = i;
        for (int j = 1; j <= lb; j++) {
            int cost = (a[i - 1] == b[j - 1]) ? 0 : 1;
            int del = prev[j] + 1;
            int ins = curr[j - 1] + 1;
            int sub = prev[j - 1] + cost;
            curr[j] = del < ins ? (del < sub ? del : sub) : (ins < sub ? ins : sub);
        }
        int *tmp = prev;
        prev = curr;
        curr = tmp;
    }
    int dist = prev[lb];
    free(prev);
    free(curr);
    return 1.0 - ((double)dist / (double)max_len);
}

/* ── Helper: path match score for WS/SSE endpoint matching ─── */

static inline double cbm_path_match_score(const char *call_path, const char *route_path) {
    if (!call_path || !route_path || !*call_path || !*route_path) {
        return 0.0;
    }

    /* Normalize: lowercase + strip trailing slash */
    char a[1024];
    char b[1024];
    int i;
    for (i = 0; call_path[i] && i < 1022; i++) {
        a[i] = (call_path[i] >= 'A' && call_path[i] <= 'Z')
                   ? (char)(call_path[i] + 32)
                   : call_path[i];
    }
    a[i] = '\0';
    if (i > 1 && a[i - 1] == '/') {
        a[i - 1] = '\0';
    }

    for (i = 0; route_path[i] && i < 1022; i++) {
        b[i] = (route_path[i] >= 'A' && route_path[i] <= 'Z')
                   ? (char)(route_path[i] + 32)
                   : route_path[i];
    }
    b[i] = '\0';
    if (i > 1 && b[i - 1] == '/') {
        b[i - 1] = '\0';
    }

    if (strcmp(a, b) == 0) {
        return 0.95;
    }

    /* Suffix match */
    int la = (int)strlen(a);
    int lb = (int)strlen(b);
    if (la > lb && strcmp(a + la - lb, b) == 0) {
        return 0.80;
    }
    if (lb > la && strcmp(b + lb - la, a) == 0) {
        return 0.80;
    }

    /* Fuzzy: normalized Levenshtein on path */
    double sim = cbm_normalized_levenshtein(a, b);
    if (sim >= 0.75) {
        return 0.65 * sim;
    }

    return 0.0;
}

/* ── Helper: get file extension ─────────────────────────────── */

static inline const char *sl_file_ext(const char *path) {
    const char *dot = strrchr(path, '.');
    return dot ? dot : "";
}

/* ── Helper: insert edge with standard props ────────────────── */

static inline int64_t sl_insert_edge(cbm_pipeline_ctx_t *ctx,
    int64_t src_id, int64_t tgt_id, const char *edge_type,
    const char *identifier, double confidence, const char *extra_json)
{
    char props[512];
    if (extra_json && extra_json[0]) {
        snprintf(props, sizeof(props),
            "{\"identifier\":\"%s\",\"confidence\":%.3f,\"confidence_band\":\"%s\",%s}",
            identifier, confidence, cbm_confidence_band(confidence), extra_json);
    } else {
        snprintf(props, sizeof(props),
            "{\"identifier\":\"%s\",\"confidence\":%.3f,\"confidence_band\":\"%s\"}",
            identifier, confidence, cbm_confidence_band(confidence));
    }
    return cbm_gbuf_insert_edge(ctx->gbuf, src_id, tgt_id, edge_type, props);
}

/* ── Per-protocol linker entry points ───────────────────────── */

int cbm_servicelink_graphql(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_grpc(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_kafka(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_sqs(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_sns(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_pubsub(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_ws(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_sse(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_rabbitmq(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_mqtt(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_nats(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_redis_pubsub(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_trpc(cbm_pipeline_ctx_t *ctx);
int cbm_servicelink_eventbridge(cbm_pipeline_ctx_t *ctx);

/* ── Service linker configuration ──────────────────────────────── */

/* Per-protocol config */
typedef struct {
    int enabled;          /* -1 = use default (true), 0 = disabled, 1 = enabled */
    double min_confidence; /* -1.0 = use default (SL_MIN_CONFIDENCE) */
} cbm_sl_protocol_config_t;

/* Full service linker config */
typedef struct {
    int enabled;          /* -1 = use default (true), 0 = disabled, 1 = enabled */
    cbm_sl_protocol_config_t protocols[SL_EDGE_TYPE_COUNT]; /* indexed same as LINKERS[] */
} cbm_sl_config_t;

/* Protocol name keys for YAML lookup (indexed same as LINKERS[]) */
extern const char *SL_PROTOCOL_KEYS[];

/* Return default config (all sentinel values = use defaults). */
cbm_sl_config_t cbm_sl_default_config(void);

/* Load config from .cgrconfig in the given directory. */
cbm_sl_config_t cbm_sl_load_config(const char *dir);

/* Check if a protocol is enabled. */
bool cbm_sl_protocol_enabled(const cbm_sl_config_t *cfg, int protocol_index);

/* Get effective min_confidence for a protocol. */
double cbm_sl_effective_min_confidence(const cbm_sl_config_t *cfg, int protocol_index);

/* ── Cross-repo endpoint registry ──────────────────────────────── */

typedef struct {
    char project[256];
    char protocol[32];       /* "graphql", "kafka", "pubsub", etc. */
    char role[16];           /* "producer" or "consumer" */
    char identifier[256];    /* topic name, operation name, etc. */
    char node_qn[512];       /* function qualified name */
    char file_path[256];     /* relative file path */
    char extra[256];         /* protocol-specific metadata (JSON) */
} cbm_sl_endpoint_t;

typedef struct cbm_sl_endpoint_list_t {
    cbm_sl_endpoint_t *items;
    int count;
    int capacity;
} cbm_sl_endpoint_list_t;

#define SL_ENDPOINT_INITIAL_CAP 256

static inline cbm_sl_endpoint_list_t *cbm_sl_endpoint_list_new(void) {
    cbm_sl_endpoint_list_t *list = calloc(1, sizeof(cbm_sl_endpoint_list_t));
    if (!list) return NULL;
    list->items = calloc(SL_ENDPOINT_INITIAL_CAP, sizeof(cbm_sl_endpoint_t));
    if (!list->items) { free(list); return NULL; }
    list->capacity = SL_ENDPOINT_INITIAL_CAP;
    list->count = 0;
    return list;
}

static inline void cbm_sl_endpoint_list_free(cbm_sl_endpoint_list_t *list) {
    if (!list) return;
    free(list->items);
    free(list);
}

static inline void sl_register_endpoint(cbm_sl_endpoint_list_t *list,
                                        const char *project, const char *protocol,
                                        const char *role, const char *identifier,
                                        const char *node_qn, const char *file_path,
                                        const char *extra) {
    if (!list) return;
    if (!identifier || !identifier[0]) return;
    if (list->count >= list->capacity) {
        int new_cap = list->capacity * 2;
        cbm_sl_endpoint_t *new_items = safe_realloc(list->items,
            (size_t)new_cap * sizeof(cbm_sl_endpoint_t));
        if (!new_items) return;
        list->items = new_items;
        list->capacity = new_cap;
    }
    cbm_sl_endpoint_t *ep = &list->items[list->count];
    memset(ep, 0, sizeof(*ep));
    if (project)    snprintf(ep->project,    sizeof(ep->project),    "%s", project);
    if (protocol)   snprintf(ep->protocol,   sizeof(ep->protocol),   "%s", protocol);
    if (role)       snprintf(ep->role,       sizeof(ep->role),       "%s", role);
    if (identifier) snprintf(ep->identifier, sizeof(ep->identifier), "%s", identifier);
    if (node_qn)    snprintf(ep->node_qn,    sizeof(ep->node_qn),    "%s", node_qn);
    if (file_path)  snprintf(ep->file_path,  sizeof(ep->file_path),  "%s", file_path);
    if (extra)      snprintf(ep->extra,      sizeof(ep->extra),      "%s", extra);
    list->count++;
}

/* Forward declarations — implemented in pass_crossrepolinks.c */
int cbm_persist_endpoints(const char *db_path, const char *project,
                          const cbm_sl_endpoint_list_t *endpoints);
int cbm_cross_project_link(const char *cache_dir);

#endif /* CBM_SERVICELINK_H */
