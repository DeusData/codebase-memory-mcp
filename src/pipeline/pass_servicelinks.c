/*
 * pass_servicelinks.c — Pipeline pass that orchestrates all cross-service protocol linkers.
 *
 * Called after pass_httplinks. Runs each protocol linker sequentially.
 * Individual linker failures are logged but don't stop execution.
 */
#include "servicelink.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "foundation/yaml.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Format int to string for logging ───────────────────────── */

static const char *itoa_sl(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Edge type array (declared extern in servicelink.h) ─────── */

const char *SL_ALL_EDGE_TYPES[] = {
    SL_EDGE_GRAPHQL, SL_EDGE_GRPC, SL_EDGE_KAFKA, SL_EDGE_SQS,
    SL_EDGE_SNS, SL_EDGE_PUBSUB, SL_EDGE_WS, SL_EDGE_SSE,
    SL_EDGE_AMQP, SL_EDGE_MQTT, SL_EDGE_NATS, SL_EDGE_REDIS_PS,
    SL_EDGE_TRPC, SL_EDGE_EVBRIDGE
};

/* Protocol keys for YAML config lookup — indexed same as LINKERS[] */
const char *SL_PROTOCOL_KEYS[] = {
    "graphql", "grpc", "kafka", "sqs", "sns", "pubsub",
    "ws", "sse", "rabbitmq", "mqtt", "nats", "redis_pubsub",
    "trpc", "eventbridge", "http"
};

/* ── Config functions ──────────────────────────────────────────── */

cbm_sl_config_t cbm_sl_default_config(void) {
    cbm_sl_config_t cfg;
    cfg.enabled = -1; /* use default = true */
    for (int i = 0; i < SL_EDGE_TYPE_COUNT; i++) {
        cfg.protocols[i].enabled = -1;
        cfg.protocols[i].min_confidence = -1.0;
    }
    return cfg;
}

cbm_sl_config_t cbm_sl_load_config(const char *dir) {
    cbm_sl_config_t cfg = cbm_sl_default_config();
    if (!dir) return cfg;

    /* Read .cgrconfig — follow exact pattern from httplink.c:1602 */
    char path[1024];
    int n = snprintf(path, sizeof(path), "%s/.cgrconfig", dir);
    if (n <= 0 || (size_t)n >= sizeof(path)) return cfg;

    FILE *f = fopen(path, "r");
    if (!f) return cfg;

    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > (long)1024 * 1024) { (void)fclose(f); return cfg; }

    char *buf = malloc((size_t)size + 1);
    if (!buf) { (void)fclose(f); return cfg; }
    size_t nread = fread(buf, 1, (size_t)size, f);
    (void)fclose(f);
    // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
    buf[nread] = '\0';

    cbm_yaml_node_t *root = cbm_yaml_parse(buf, (int)nread);
    free(buf);
    if (!root) return cfg;

    /* Top-level enabled */
    if (cbm_yaml_has(root, "service_linker.enabled")) {
        cfg.enabled = cbm_yaml_get_bool(root, "service_linker.enabled", true) ? 1 : 0;
    }

    /* Per-protocol settings */
    for (int i = 0; i < SL_EDGE_TYPE_COUNT; i++) {
        char key[128];
        snprintf(key, sizeof(key), "service_linker.%s.enabled", SL_PROTOCOL_KEYS[i]);
        if (cbm_yaml_has(root, key)) {
            cfg.protocols[i].enabled = cbm_yaml_get_bool(root, key, true) ? 1 : 0;
        }
        snprintf(key, sizeof(key), "service_linker.%s.min_confidence", SL_PROTOCOL_KEYS[i]);
        if (cbm_yaml_has(root, key)) {
            cfg.protocols[i].min_confidence = cbm_yaml_get_float(root, key, -1.0);
        }
    }

    cbm_yaml_free(root);
    return cfg;
}

bool cbm_sl_protocol_enabled(const cbm_sl_config_t *cfg, int protocol_index) {
    if (!cfg) return true;
    if (cfg->enabled == 0) return false; /* globally disabled */
    if (protocol_index < 0 || protocol_index >= SL_EDGE_TYPE_COUNT) return true;
    if (cfg->protocols[protocol_index].enabled == 0) return false;
    return true;
}

double cbm_sl_effective_min_confidence(const cbm_sl_config_t *cfg, int protocol_index) {
    if (!cfg) return SL_MIN_CONFIDENCE;
    if (protocol_index >= 0 && protocol_index < SL_EDGE_TYPE_COUNT) {
        if (cfg->protocols[protocol_index].min_confidence >= 0.0) {
            return cfg->protocols[protocol_index].min_confidence;
        }
    }
    return SL_MIN_CONFIDENCE;
}

/* ── Cleanup stale edges from previous runs ─────────────────── */

static void cleanup_stale_edges(cbm_pipeline_ctx_t *ctx) {
    /* NOTE: use the array's own size here, not SL_EDGE_TYPE_COUNT.
     * SL_ALL_EDGE_TYPES deliberately excludes HTTP_CALLS — those edges are
     * emitted by pass_calls.c before this pass runs, and servicelink_http
     * enriches them in place. Deleting them here would destroy that input. */
    const int n = (int)(sizeof(SL_ALL_EDGE_TYPES) / sizeof(*SL_ALL_EDGE_TYPES));
    for (int i = 0; i < n; i++) {
        cbm_gbuf_delete_edges_by_type(ctx->gbuf, SL_ALL_EDGE_TYPES[i]);
    }
}

/* ── Linker dispatch table ──────────────────────────────────── */

typedef int (*cbm_sl_linker_fn)(cbm_pipeline_ctx_t *ctx);

typedef struct {
    const char *name;
    cbm_sl_linker_fn fn;
} cbm_sl_linker_entry_t;

static const cbm_sl_linker_entry_t LINKERS[] = {
    { "GraphQL",      cbm_servicelink_graphql },
    { "gRPC",         cbm_servicelink_grpc },
    { "Kafka",        cbm_servicelink_kafka },
    { "SQS",          cbm_servicelink_sqs },
    { "SNS",          cbm_servicelink_sns },
    { "Pub/Sub",      cbm_servicelink_pubsub },
    { "WebSocket",    cbm_servicelink_ws },
    { "SSE",          cbm_servicelink_sse },
    { "RabbitMQ",     cbm_servicelink_rabbitmq },
    { "MQTT",         cbm_servicelink_mqtt },
    { "NATS",         cbm_servicelink_nats },
    { "Redis Pub/Sub", cbm_servicelink_redis_pubsub },
    { "tRPC",         cbm_servicelink_trpc },
    { "EventBridge",  cbm_servicelink_eventbridge },
    { "HTTP",         cbm_servicelink_http },
};
#define LINKER_COUNT (int)(sizeof(LINKERS) / sizeof(LINKERS[0]))

/* ── Main pass entry point ──────────────────────────────────── */

int cbm_pipeline_pass_servicelinks(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("pass.servicelinks.start", "linkers", itoa_sl(LINKER_COUNT));

    /* Step 0: Load config */
    cbm_sl_config_t cfg = cbm_sl_load_config(ctx->repo_path);

    if (cfg.enabled == 0) {
        cbm_log_info("pass.servicelinks.skip", "reason", "disabled");
        return 0;
    }

    /* Step 1: Clean stale edges */
    cleanup_stale_edges(ctx);

    /* Step 2: Run each linker */
    int total_links = 0;
    int errors = 0;

    for (int i = 0; i < LINKER_COUNT; i++) {
        if (!cbm_sl_protocol_enabled(&cfg, i)) {
            cbm_log_info("servicelink.skip", "name", LINKERS[i].name,
                         "reason", "disabled");
            continue;
        }
        cbm_log_info("servicelink.run", "name", LINKERS[i].name);
        int rc = LINKERS[i].fn(ctx);
        if (rc < 0) {
            cbm_log_warn("servicelink.error", "name", LINKERS[i].name,
                         "rc", itoa_sl(rc));
            errors++;
        } else {
            total_links += rc;
            cbm_log_info("servicelink.done", "name", LINKERS[i].name,
                         "links", itoa_sl(rc));
        }
    }

    cbm_log_info("pass.servicelinks.done", "total_links", itoa_sl(total_links),
                 "errors", itoa_sl(errors));

    /* Return 0 unless ALL linkers failed */
    return (errors == LINKER_COUNT) ? -1 : 0;
}
