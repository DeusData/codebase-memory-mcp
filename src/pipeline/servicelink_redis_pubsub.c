/*
 * servicelink_redis_pubsub.c — Redis Pub/Sub protocol linker.
 *
 * Discovers Redis publishers (PUBLISH calls) and subscribers (SUBSCRIBE/PSUBSCRIBE
 * patterns) in source code, then creates REDIS_PUBSUB_CALLS edges in the graph buffer.
 *
 * PSUBSCRIBE uses Redis glob matching:
 *   '*' matches zero or more characters (any character, not path-level)
 *   '?' matches exactly one character
 *   '[abc]' matches character class
 *   '\x' escapes special characters
 *
 * Matching is ALL-match: a publisher can match multiple subscribers.
 *
 * Supported languages: Python (redis-py), Go (go-redis), Java (Jedis/Lettuce),
 *                      Node.js/TypeScript (ioredis/node-redis), Rust (redis-rs).
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define REDIS_CONF_EXACT   0.95  /* exact channel match */
#define REDIS_CONF_PATTERN 0.90  /* glob pattern match via PSUBSCRIBE */

/* ── itoa helper (thread-local rotating buffers) ────────────────── */

static const char *itoa_redis(int val) {
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

/* ── Redis glob matching for PSUBSCRIBE ───────────────────────── */

/*
 * Match a Redis glob pattern against a subject string.
 * Redis PSUBSCRIBE glob semantics:
 *   '*' matches zero or more of ANY characters
 *   '?' matches exactly one character
 *   '[abc]' matches one character in the set
 *   '\x' escapes the next character (literal match)
 *
 * Returns 1 for match, 0 for no match.
 * Non-static so tests can call it directly.
 */
int redis_glob_match(const char *pattern, const char *subject) {
    if (!pattern || !subject) return 0;

    const char *p = pattern;
    const char *s = subject;
    const char *star_p = NULL;
    const char *star_s = NULL;

    while (*s) {
        if (*p == '\\' && *(p + 1)) {
            /* Escaped character — literal match */
            p++;
            if (*p == *s) {
                p++;
                s++;
                continue;
            }
            /* Backtrack to star if possible */
            if (star_p) {
                p = star_p + 1;
                star_s++;
                s = star_s;
                continue;
            }
            return 0;
        }

        if (*p == '*') {
            /* Record star position for backtracking */
            star_p = p;
            star_s = s;
            p++;
            continue;
        }

        if (*p == '?') {
            /* Match exactly one character */
            p++;
            s++;
            continue;
        }

        if (*p == '[') {
            /* Character class */
            p++; /* skip '[' */
            int negated = 0;
            if (*p == '^' || *p == '!') {
                negated = 1;
                p++;
            }
            int found = 0;
            char prev = 0;
            while (*p && *p != ']') {
                if (*p == '-' && prev && *(p + 1) && *(p + 1) != ']') {
                    /* Range: prev-next */
                    p++;
                    if (*s >= prev && *s <= *p) found = 1;
                    prev = *p;
                    p++;
                } else {
                    if (*p == *s) found = 1;
                    prev = *p;
                    p++;
                }
            }
            if (*p == ']') p++;
            if (negated) found = !found;
            if (found) {
                s++;
                continue;
            }
            /* No match in class — backtrack to star if possible */
            if (star_p) {
                p = star_p + 1;
                star_s++;
                s = star_s;
                continue;
            }
            return 0;
        }

        if (*p == *s) {
            p++;
            s++;
            continue;
        }

        /* Mismatch — backtrack to star if possible */
        if (star_p) {
            p = star_p + 1;
            star_s++;
            s = star_s;
            continue;
        }

        return 0;
    }

    /* Consume trailing '*' in pattern */
    while (*p == '*') p++;

    return *p == '\0' ? 1 : 0;
}

/* ── Producer scanning ─────────────────────────────────────────── */

/*
 * Scan source code for Redis publish patterns.
 * Detected channel names become producer identifiers.
 */
static void scan_producers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Python: redis.publish("channel", message) / r.publish('channel', msg) */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "\\.publish\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_producer(producers, prod_count, channel, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Go: conn.Publish(ctx, "channel", msg) / conn.Publish("channel", msg) */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "\\.Publish\\([ \t]*ctx[ \t]*,[ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_producer(producers, prod_count, channel, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        if (cbm_regcomp(&re, "\\.Publish\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_producer(producers, prod_count, channel, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java: jedis.publish("channel", msg) / lettuce publish("channel", msg) */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "\\.publish\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_producer(producers, prod_count, channel, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: redis.publish('channel', msg) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "\\.publish\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_producer(producers, prod_count, channel, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Rust: conn.publish("channel", msg) */
    if (strcmp(ext, ".rs") == 0) {
        if (cbm_regcomp(&re, "\\.publish\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_producer(producers, prod_count, channel, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── Consumer scanning ─────────────────────────────────────────── */

/*
 * Scan source code for Redis subscribe/psubscribe patterns.
 * Detected channel names become consumer identifiers.
 * For psubscribe, extra field stores "type":"psubscribe" to trigger glob matching.
 */
static void scan_consumers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_consumer_t *consumers, int *cons_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Python: redis.subscribe("channel") / pubsub.subscribe('channel') */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "\\.subscribe\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_consumer(consumers, cons_count, channel, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Python: redis.psubscribe("channel.*") */
        if (cbm_regcomp(&re, "\\.psubscribe\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_consumer(consumers, cons_count, channel, node,
                             "\"role\":\"subscriber\",\"type\":\"psubscribe\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Go: conn.Subscribe(ctx, "channel") / conn.PSubscribe(ctx, "channel.*") */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "\\.Subscribe\\([ \t]*ctx[ \t]*,[ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_consumer(consumers, cons_count, channel, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        if (cbm_regcomp(&re, "\\.PSubscribe\\([ \t]*ctx[ \t]*,[ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_consumer(consumers, cons_count, channel, node,
                             "\"role\":\"subscriber\",\"type\":\"psubscribe\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java: jedis.subscribe(..., "channel") / jedis.psubscribe(..., "channel.*") */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "\\.subscribe\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_consumer(consumers, cons_count, channel, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        if (cbm_regcomp(&re, "\\.psubscribe\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_consumer(consumers, cons_count, channel, node,
                             "\"role\":\"subscriber\",\"type\":\"psubscribe\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: redis.subscribe('channel') / redis.psubscribe('channel.*') */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "\\.subscribe\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_consumer(consumers, cons_count, channel, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        if (cbm_regcomp(&re, "\\.psubscribe\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_consumer(consumers, cons_count, channel, node,
                             "\"role\":\"subscriber\",\"type\":\"psubscribe\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Rust: conn.subscribe("channel") / conn.psubscribe("channel.*") */
    if (strcmp(ext, ".rs") == 0) {
        if (cbm_regcomp(&re, "\\.subscribe\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_consumer(consumers, cons_count, channel, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        if (cbm_regcomp(&re, "\\.psubscribe\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char channel[256];
                extract_match(pos, &matches[1], channel, sizeof(channel));
                add_consumer(consumers, cons_count, channel, node,
                             "\"role\":\"subscriber\",\"type\":\"psubscribe\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── Channel matching logic ───────────────────────────────────── */

/*
 * Match a consumer channel against a producer channel.
 * If the consumer used psubscribe (extra contains "psubscribe"),
 * use redis_glob_match. Otherwise, exact string match.
 *
 * Returns confidence: 0.95 for exact, 0.90 for glob pattern, 0.0 for no match.
 */
static double match_channels(const char *consumer_id, const char *consumer_extra,
                             const char *producer_id) {
    /* Check if consumer used psubscribe */
    if (strstr(consumer_extra, "psubscribe") != NULL) {
        if (redis_glob_match(consumer_id, producer_id)) {
            return REDIS_CONF_PATTERN;
        }
        return 0.0;
    }

    /* Exact match */
    if (strcmp(consumer_id, producer_id) == 0) {
        return REDIS_CONF_EXACT;
    }
    return 0.0;
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
        strcmp(ext, ".rs") == 0) {
        char *source = sl_read_node_source(ctx, node);
        if (source) {
            scan_producers(source, ext, node, producers, prod_count);
            scan_consumers(source, ext, node, consumers, cons_count);
            free(source);
        }
    }
}

/* ── Main entry point ──────────────────────────────────────────── */

int cbm_servicelink_redis_pubsub(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "redis_pubsub");

    /* 1. Allocate producer/consumer arrays on heap */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.redis_pubsub", "error", "alloc_failed");
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

    cbm_log_info("servicelink.redis_pubsub.discovery",
                 "producers", itoa_redis(prod_count),
                 "consumers", itoa_redis(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "redis_pubsub",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "redis_pubsub",
                                 "consumer", consumers[i].identifier,
                                 consumers[i].handler_qn, consumers[i].file_path,
                                 consumers[i].extra);
        }
    }

    /* 4. ALL-match: for each consumer, check ALL producers, create edges for matches */
    int link_count = 0;

    for (int ci = 0; ci < cons_count; ci++) {
        const cbm_sl_consumer_t *c = &consumers[ci];

        for (int pi = 0; pi < prod_count; pi++) {
            const cbm_sl_producer_t *p = &producers[pi];

            /* Skip self-links (same node) */
            if (c->handler_id == p->source_id) continue;

            double conf = match_channels(c->identifier, c->extra, p->identifier);
            if (conf >= SL_MIN_CONFIDENCE) {
                sl_insert_edge(ctx, c->handler_id, p->source_id,
                              SL_EDGE_REDIS_PS, c->identifier, conf, NULL);
                link_count++;
            }
        }
    }

    cbm_log_info("servicelink.redis_pubsub.done", "links", itoa_redis(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
