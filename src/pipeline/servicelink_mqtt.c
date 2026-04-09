/*
 * servicelink_mqtt.c — MQTT protocol linker.
 *
 * Discovers MQTT publishers (client.publish, mqtt.publish, mosquitto_publish, etc.)
 * and subscribers (client.subscribe, mqtt.subscribe, mosquitto_subscribe, etc.) in
 * source code, then creates MQTT_CALLS edges in the graph buffer.
 *
 * MQTT topic wildcards:
 *   '+' matches exactly one topic level (separator is '/')
 *   '#' matches zero or more remaining levels (only valid at the end)
 *
 * Matching is ALL-match: a publisher can match multiple subscribers.
 *
 * Supported languages: Python (paho-mqtt), Go (eclipse/paho), Java (Eclipse Paho,
 * HiveMQ), Node.js/TypeScript (mqtt.js), Rust (rumqttc), C/C++ (mosquitto).
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define MQTT_CONF_EXACT    0.95  /* exact topic match */
#define MQTT_CONF_WILDCARD 0.90  /* wildcard (+/#) match */

/* ── itoa helper (thread-local rotating buffers) ────────────────── */

static const char *itoa_mqtt(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Forward declarations ──────────────────────────────────────── */

static void scan_producers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_producer_t *producers, int *prod_count);
static void scan_consumers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_consumer_t *consumers, int *cons_count);

/* ── Regex helpers ─────────────────────────────────────────────── */

/* Add a producer entry if there's room. */
static void add_producer(cbm_sl_producer_t *producers, int *count,
                         const char *identifier, const cbm_gbuf_node_t *node,
                         const char *extra) {
    if (*count >= SL_MAX_PRODUCERS) return;
    cbm_sl_producer_t *p = &producers[*count];
    snprintf(p->identifier, sizeof(p->identifier), "%s", identifier);
    snprintf(p->source_qn, sizeof(p->source_qn), "%s",
             node->qualified_name ? node->qualified_name : "");
    p->source_id = node->id;
    snprintf(p->file_path, sizeof(p->file_path), "%s",
             node->file_path ? node->file_path : "");
    snprintf(p->extra, sizeof(p->extra), "%s", extra ? extra : "");
    (*count)++;
}

/* Add a consumer entry if there's room. */
static void add_consumer(cbm_sl_consumer_t *consumers, int *count,
                         const char *identifier, const cbm_gbuf_node_t *node,
                         const char *extra) {
    if (*count >= SL_MAX_CONSUMERS) return;
    cbm_sl_consumer_t *c = &consumers[*count];
    snprintf(c->identifier, sizeof(c->identifier), "%s", identifier);
    snprintf(c->handler_qn, sizeof(c->handler_qn), "%s",
             node->qualified_name ? node->qualified_name : "");
    c->handler_id = node->id;
    snprintf(c->file_path, sizeof(c->file_path), "%s",
             node->file_path ? node->file_path : "");
    snprintf(c->extra, sizeof(c->extra), "%s", extra ? extra : "");
    (*count)++;
}

/* Extract a regex submatch into a buffer. Returns the buffer for convenience. */
static char *extract_match(const char *str, const cbm_regmatch_t *m,
                           char *buf, size_t bufsz) {
    if (m->rm_so < 0) {
        buf[0] = '\0';
        return buf;
    }
    int len = m->rm_eo - m->rm_so;
    if ((size_t)len >= bufsz) len = (int)bufsz - 1;
    memcpy(buf, str + m->rm_so, (size_t)len);
    buf[len] = '\0';
    return buf;
}

/* ── MQTT topic wildcard matching ─────────────────────────────── */

/*
 * Match an MQTT topic filter (pattern) against a concrete topic (subject).
 * MQTT topic wildcards (separator is '/'):
 *   '+' matches exactly one topic level
 *   '#' matches zero or more remaining levels (must be last segment)
 *
 * Returns 1 for match, 0 for no match.
 */
int mqtt_topic_match(const char *pattern, const char *subject) {
    if (!pattern || !subject) return 0;

    /* Exact match fast path */
    if (strcmp(pattern, subject) == 0) return 1;

    /* Split pattern and subject into segments on '/' */
    char pat_buf[256], sub_buf[256];
    snprintf(pat_buf, sizeof(pat_buf), "%s", pattern);
    snprintf(sub_buf, sizeof(sub_buf), "%s", subject);

    const char *pat_words[64];
    const char *sub_words[64];
    int pat_count = 0, sub_count = 0;

    /* Tokenize pattern on '/' */
    {
        char *tok = pat_buf;
        char *sep;
        while (tok && pat_count < 64) {
            sep = strchr(tok, '/');
            if (sep) *sep = '\0';
            pat_words[pat_count++] = tok;
            tok = sep ? sep + 1 : NULL;
        }
    }

    /* Tokenize subject on '/' */
    {
        char *tok = sub_buf;
        char *sep;
        while (tok && sub_count < 64) {
            sep = strchr(tok, '/');
            if (sep) *sep = '\0';
            sub_words[sub_count++] = tok;
            tok = sep ? sep + 1 : NULL;
        }
    }

    /* Dynamic programming match with # and + wildcards */
    /* dp[i][j] = can pat_words[0..i-1] match sub_words[0..j-1]? */
    int rows = pat_count + 1;
    int cols = sub_count + 1;
    char *dp = calloc((size_t)(rows * cols), 1);
    if (!dp) return 0;

    dp[0] = 1; /* empty pattern matches empty subject */

    /* '#' at the start can match zero words */
    for (int i = 1; i <= pat_count; i++) {
        if (strcmp(pat_words[i - 1], "#") == 0) {
            dp[i * cols + 0] = dp[(i - 1) * cols + 0];
        }
    }

    for (int i = 1; i <= pat_count; i++) {
        for (int j = 1; j <= sub_count; j++) {
            if (strcmp(pat_words[i - 1], "#") == 0) {
                /* '#' matches zero levels (skip pattern word) or one+ levels (skip subject word) */
                dp[i * cols + j] = dp[(i - 1) * cols + j]        /* # matches zero more */
                                 | dp[i * cols + (j - 1)]         /* # matches one more level */
                                 | dp[(i - 1) * cols + (j - 1)];  /* # matches exactly this level */
            } else if (strcmp(pat_words[i - 1], "+") == 0) {
                /* '+' matches exactly one level */
                dp[i * cols + j] = dp[(i - 1) * cols + (j - 1)];
            } else {
                /* Literal match */
                if (strcmp(pat_words[i - 1], sub_words[j - 1]) == 0) {
                    dp[i * cols + j] = dp[(i - 1) * cols + (j - 1)];
                }
            }
        }
    }

    int result = dp[pat_count * cols + sub_count];
    free(dp);
    return result;
}

/* ── Producer scanning ─────────────────────────────────────────── */

/*
 * Scan source code for MQTT publisher patterns.
 * Detected topic names become producer identifiers.
 */
static void scan_producers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Python: client.publish("topic", ...) */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "client\\.publish\\(['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Python: mqtt.publish(topic="X", ...) */
        if (cbm_regcomp(&re, "mqtt\\.publish\\([^)]*topic[ \t]*=[ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Go: Publish("topic", ...) or .Publish("topic", ...) */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "Publish\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java/Kotlin: .publish("topic", ...) or .publish(MqttMessage..."topic") */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "\\.publish\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: client.publish('topic', ...) or .publish('topic', ...) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "client\\.publish\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* .publish('topic', ...) — generic */
        if (cbm_regcomp(&re, "\\.publish\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Rust: client.publish("topic", ...) */
    if (strcmp(ext, ".rs") == 0) {
        if (cbm_regcomp(&re, "client\\.publish\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* C/C++: mosquitto_publish(..., "topic", ...) */
    if (strcmp(ext, ".c") == 0 || strcmp(ext, ".h") == 0) {
        if (cbm_regcomp(&re, "mosquitto_publish\\(.*,[ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── Consumer scanning ─────────────────────────────────────────── */

/*
 * Scan source code for MQTT subscriber patterns.
 * Detected topic filters become consumer identifiers.
 */
static void scan_consumers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_consumer_t *consumers, int *cons_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Python: client.subscribe("topic") */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "client\\.subscribe\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Python: mqtt.subscribe(topic="X") */
        if (cbm_regcomp(&re, "mqtt\\.subscribe\\([^)]*topic[ \t]*=[ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Go: Subscribe("topic", ...) */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "Subscribe\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java/Kotlin: .subscribe("topic", ...) or @MqttListener..."topic" */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "\\.subscribe\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Java: @MqttListener..."topic" */
        if (cbm_regcomp(&re, "@MqttListener.*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: client.subscribe('topic') */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "client\\.subscribe\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Rust: client.subscribe("topic", ...) */
    if (strcmp(ext, ".rs") == 0) {
        if (cbm_regcomp(&re, "client\\.subscribe\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* C/C++: mosquitto_subscribe(..., "topic", ...) */
    if (strcmp(ext, ".c") == 0 || strcmp(ext, ".h") == 0) {
        if (cbm_regcomp(&re, "mosquitto_subscribe\\(.*,[ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── Process a single node ─────────────────────────────────────── */

static void process_node(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *node,
                         cbm_sl_producer_t *producers, int *prod_count,
                         cbm_sl_consumer_t *consumers, int *cons_count) {
    if (!node->file_path) return;

    const char *ext = sl_file_ext(node->file_path);

    /* Source files: scan for publisher and subscriber patterns */
    if (strcmp(ext, ".go") == 0 || strcmp(ext, ".py") == 0 ||
        strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0 ||
        strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0 ||
        strcmp(ext, ".rs") == 0 || strcmp(ext, ".c") == 0 ||
        strcmp(ext, ".h") == 0) {
        char *source = sl_read_node_source(ctx, node);
        if (source) {
            scan_producers(source, ext, node, producers, prod_count);
            scan_consumers(source, ext, node, consumers, cons_count);
            free(source);
        }
    }
}

/* ── Main entry point ──────────────────────────────────────────── */

int cbm_servicelink_mqtt(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "mqtt");

    /* 1. Allocate producer/consumer arrays on heap */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.mqtt", "error", "alloc_failed");
        return -1;
    }
    int prod_count = 0;
    int cons_count = 0;

    /* 2. Get Function, Method, Module, Class, and Variable nodes from graph buffer */
    const cbm_gbuf_node_t **funcs = NULL, **methods = NULL, **modules = NULL;
    const cbm_gbuf_node_t **classes = NULL, **vars = NULL;
    int nfuncs = 0, nmethods = 0, nmodules = 0;
    int nclasses = 0, nvars = 0;
    cbm_gbuf_find_by_label(ctx->gbuf, "Function", &funcs, &nfuncs);
    cbm_gbuf_find_by_label(ctx->gbuf, "Method", &methods, &nmethods);
    cbm_gbuf_find_by_label(ctx->gbuf, "Module", &modules, &nmodules);
    cbm_gbuf_find_by_label(ctx->gbuf, "Class", &classes, &nclasses);
    cbm_gbuf_find_by_label(ctx->gbuf, "Variable", &vars, &nvars);

    /* 3. Process all nodes */
    for (int i = 0; i < nfuncs; i++) {
        process_node(ctx, funcs[i], producers, &prod_count, consumers, &cons_count);
    }
    for (int i = 0; i < nmethods; i++) {
        process_node(ctx, methods[i], producers, &prod_count, consumers, &cons_count);
    }
    for (int i = 0; i < nmodules; i++) {
        process_node(ctx, modules[i], producers, &prod_count, consumers, &cons_count);
    }
    for (int i = 0; i < nclasses; i++) {
        process_node(ctx, classes[i], producers, &prod_count, consumers, &cons_count);
    }
    for (int i = 0; i < nvars; i++) {
        process_node(ctx, vars[i], producers, &prod_count, consumers, &cons_count);
    }

    cbm_log_info("servicelink.mqtt.discovery",
                 "producers", itoa_mqtt(prod_count),
                 "consumers", itoa_mqtt(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "mqtt",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "mqtt",
                                 "consumer", consumers[i].identifier,
                                 consumers[i].handler_qn, consumers[i].file_path,
                                 consumers[i].extra);
        }
    }

    /* 4. ALL-match: for each consumer, check ALL producers and create edges for any match */
    int link_count = 0;

    for (int ci = 0; ci < cons_count; ci++) {
        const cbm_sl_consumer_t *c = &consumers[ci];

        for (int pi = 0; pi < prod_count; pi++) {
            const cbm_sl_producer_t *p = &producers[pi];

            /* Skip self-links (same node) */
            if (c->handler_id == p->source_id) continue;

            /* Try exact match first */
            if (strcmp(c->identifier, p->identifier) == 0) {
                sl_insert_edge(ctx, c->handler_id, p->source_id,
                              SL_EDGE_MQTT, c->identifier, MQTT_CONF_EXACT, NULL);
                link_count++;
                continue;
            }

            /* Try wildcard match: subscriber topic filter against publisher topic */
            if (mqtt_topic_match(c->identifier, p->identifier)) {
                sl_insert_edge(ctx, c->handler_id, p->source_id,
                              SL_EDGE_MQTT, c->identifier, MQTT_CONF_WILDCARD, NULL);
                link_count++;
            }
        }
    }

    cbm_log_info("servicelink.mqtt.done", "links", itoa_mqtt(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
