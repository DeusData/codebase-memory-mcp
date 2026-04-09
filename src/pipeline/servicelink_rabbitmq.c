/*
 * servicelink_rabbitmq.c — RabbitMQ/AMQP protocol linker.
 *
 * Discovers AMQP producers (basic_publish, convertAndSend, ch.Publish, etc.)
 * and consumers (basic_consume, @RabbitListener, ch.Consume, etc.) in source
 * code, then creates AMQP_CALLS edges in the graph buffer.
 *
 * Supports exchange-type-aware matching:
 *   - Direct exchange: exact routing_key match (0.95)
 *   - Topic exchange: AMQP wildcard matching with * and # (0.90)
 *   - Fanout exchange: all bound queues receive all messages (0.85)
 *   - Default exchange (""): routing_key IS the queue name (0.95)
 *
 * Supported languages: Go, Python, Java/Kotlin, Node.js/TypeScript, Rust.
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define AMQP_CONF_EXACT   0.95  /* exact routing_key or default-exchange match */
#define AMQP_CONF_TOPIC   0.90  /* topic exchange wildcard match */
#define AMQP_CONF_FANOUT  0.85  /* fanout exchange — all consumers match */

/* ── itoa helper (thread-local rotating buffers) ────────────────── */

static const char *itoa_amqp(int val) {
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

/* ── AMQP topic wildcard matching ─────────────────────────────── */

/*
 * Match an AMQP topic routing pattern against a subject.
 * AMQP topic exchange wildcards:
 *   '*' matches exactly one dot-separated word
 *   '#' matches zero or more dot-separated words
 *
 * Both pattern and subject are split on '.'.
 * Returns 1 for match, 0 for no match.
 */
int amqp_topic_match(const char *pattern, const char *subject) {
    if (!pattern || !subject) return 0;

    /* Exact match fast path */
    if (strcmp(pattern, subject) == 0) return 1;

    /* Split pattern into words */
    char pat_buf[256], sub_buf[256];
    snprintf(pat_buf, sizeof(pat_buf), "%s", pattern);
    snprintf(sub_buf, sizeof(sub_buf), "%s", subject);

    /* Count maximum possible segments */
    const char *pat_words[64];
    const char *sub_words[64];
    int pat_count = 0, sub_count = 0;

    /* Tokenize pattern */
    {
        char *tok = pat_buf;
        char *dot;
        while (tok && pat_count < 64) {
            dot = strchr(tok, '.');
            if (dot) *dot = '\0';
            pat_words[pat_count++] = tok;
            tok = dot ? dot + 1 : NULL;
        }
    }

    /* Tokenize subject */
    {
        char *tok = sub_buf;
        char *dot;
        while (tok && sub_count < 64) {
            dot = strchr(tok, '.');
            if (dot) *dot = '\0';
            sub_words[sub_count++] = tok;
            tok = dot ? dot + 1 : NULL;
        }
    }

    /* Dynamic programming match with # and * wildcards */
    /* dp[i][j] = can pat_words[0..i-1] match sub_words[0..j-1]? */
    /* Use a flat array: dp[(pat_count+1) * (sub_count+1)] */
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
                /* '#' matches zero words (skip pattern word) or one+ words (skip subject word) */
                dp[i * cols + j] = dp[(i - 1) * cols + j]      /* # matches zero more */
                                 | dp[i * cols + (j - 1)]       /* # matches one more word */
                                 | dp[(i - 1) * cols + (j - 1)]; /* # matches exactly this word */
            } else if (strcmp(pat_words[i - 1], "*") == 0) {
                /* '*' matches exactly one word */
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
 * Scan source code for RabbitMQ/AMQP producer patterns.
 * The identifier is stored as "exchange|routing_key" to enable matching.
 * Extra JSON includes exchange and routing_key.
 */
static void scan_producers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[4];
    const char *pos;

    /* Python: channel.basic_publish(exchange='X', routing_key='Y') */
    if (strcmp(ext, ".py") == 0) {
        /* basic_publish with exchange and routing_key (both single/double quotes) */
        if (cbm_regcomp(&re,
                "basic_publish\\([^)]*exchange[ \t]*=[ \t]*['\"]([^'\"]*)['\"][^)]*routing_key[ \t]*=[ \t]*['\"]([^'\"]+)['\"]",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
                char exchange[256], rkey[256], ident[256], extra[256];
                extract_match(pos, &matches[1], exchange, sizeof(exchange));
                extract_match(pos, &matches[2], rkey, sizeof(rkey));
                snprintf(ident, sizeof(ident), "%s|%s", exchange, rkey);
                snprintf(extra, sizeof(extra),
                         "\"exchange\":\"%s\",\"routing_key\":\"%s\"",
                         exchange, rkey);
                add_producer(producers, prod_count, ident, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java/Kotlin: rabbitTemplate.convertAndSend("exchange", "routing_key", message) */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re,
                "rabbitTemplate\\.convertAndSend\\([ \t]*\"([^\"]*)\",[ \t]*\"([^\"]+)\"",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
                char exchange[256], rkey[256], ident[256], extra[256];
                extract_match(pos, &matches[1], exchange, sizeof(exchange));
                extract_match(pos, &matches[2], rkey, sizeof(rkey));
                snprintf(ident, sizeof(ident), "%s|%s", exchange, rkey);
                snprintf(extra, sizeof(extra),
                         "\"exchange\":\"%s\",\"routing_key\":\"%s\"",
                         exchange, rkey);
                add_producer(producers, prod_count, ident, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Go: ch.Publish("exchange", "routing_key", ...) */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re,
                "ch\\.Publish\\([ \t]*\"([^\"]*)\",[ \t]*\"([^\"]+)\"",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
                char exchange[256], rkey[256], ident[256], extra[256];
                extract_match(pos, &matches[1], exchange, sizeof(exchange));
                extract_match(pos, &matches[2], rkey, sizeof(rkey));
                snprintf(ident, sizeof(ident), "%s|%s", exchange, rkey);
                snprintf(extra, sizeof(extra),
                         "\"exchange\":\"%s\",\"routing_key\":\"%s\"",
                         exchange, rkey);
                add_producer(producers, prod_count, ident, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: channel.publish('exchange', 'routing_key', ...) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        /* channel.publish('exchange', 'routing_key', ...) */
        if (cbm_regcomp(&re,
                "channel\\.publish\\([ \t]*['\"]([^'\"]*)['\"],[ \t]*['\"]([^'\"]+)['\"]",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
                char exchange[256], rkey[256], ident[256], extra[256];
                extract_match(pos, &matches[1], exchange, sizeof(exchange));
                extract_match(pos, &matches[2], rkey, sizeof(rkey));
                snprintf(ident, sizeof(ident), "%s|%s", exchange, rkey);
                snprintf(extra, sizeof(extra),
                         "\"exchange\":\"%s\",\"routing_key\":\"%s\"",
                         exchange, rkey);
                add_producer(producers, prod_count, ident, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* channel.sendToQueue('queue', ...) — default exchange shorthand */
        if (cbm_regcomp(&re,
                "channel\\.sendToQueue\\([ \t]*['\"]([^'\"]+)['\"]",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char queue[256], ident[256], extra[256];
                extract_match(pos, &matches[1], queue, sizeof(queue));
                snprintf(ident, sizeof(ident), "|%s", queue);
                snprintf(extra, sizeof(extra),
                         "\"exchange\":\"\",\"routing_key\":\"%s\"", queue);
                add_producer(producers, prod_count, ident, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Rust: channel.basic_publish("exchange", "routing_key", ...) */
    if (strcmp(ext, ".rs") == 0) {
        if (cbm_regcomp(&re,
                "basic_publish\\([ \t]*\"([^\"]*)\",[ \t]*\"([^\"]+)\"",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
                char exchange[256], rkey[256], ident[256], extra[256];
                extract_match(pos, &matches[1], exchange, sizeof(exchange));
                extract_match(pos, &matches[2], rkey, sizeof(rkey));
                snprintf(ident, sizeof(ident), "%s|%s", exchange, rkey);
                snprintf(extra, sizeof(extra),
                         "\"exchange\":\"%s\",\"routing_key\":\"%s\"",
                         exchange, rkey);
                add_producer(producers, prod_count, ident, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── Consumer scanning ─────────────────────────────────────────── */

/*
 * Scan source code for RabbitMQ/AMQP consumer patterns.
 * The identifier is the queue name. Extra includes queue info.
 */
static void scan_consumers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_consumer_t *consumers, int *cons_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[4];
    const char *pos;

    /* Python: channel.basic_consume(queue='Q', ...) */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re,
                "basic_consume\\([^)]*queue[ \t]*=[ \t]*['\"]([^'\"]+)['\"]",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char queue[256], extra[256];
                extract_match(pos, &matches[1], queue, sizeof(queue));
                snprintf(extra, sizeof(extra), "\"queue\":\"%s\"", queue);
                add_consumer(consumers, cons_count, queue, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Python: @app.task (Celery — uses RabbitMQ as default broker) */
        if (cbm_regcomp(&re,
                "@app\\.task[^)]*name[ \t]*=[ \t]*['\"]([^'\"]+)['\"]",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char task[256], extra[256];
                extract_match(pos, &matches[1], task, sizeof(task));
                snprintf(extra, sizeof(extra), "\"queue\":\"%s\",\"celery\":true", task);
                add_consumer(consumers, cons_count, task, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java/Kotlin: @RabbitListener(queues = "Q") or @RabbitListener(queues = {"Q1", "Q2"}) */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re,
                "@RabbitListener\\([^)]*queues[ \t]*=[ \t]*\\{?[ \t]*\"([^\"]+)\"",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char queue[256], extra[256];
                extract_match(pos, &matches[1], queue, sizeof(queue));
                snprintf(extra, sizeof(extra), "\"queue\":\"%s\"", queue);
                add_consumer(consumers, cons_count, queue, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Java: @QueueBinding(value = @Queue("Q"), exchange = @Exchange("X"), key = "Y") */
        if (cbm_regcomp(&re,
                "@QueueBinding\\([^)]*@Queue\\([ \t]*\"([^\"]+)\"\\)",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char queue[256], extra[256];
                extract_match(pos, &matches[1], queue, sizeof(queue));
                snprintf(extra, sizeof(extra), "\"queue\":\"%s\"", queue);
                add_consumer(consumers, cons_count, queue, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Go: ch.Consume("queue", ...) */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re,
                "ch\\.Consume\\([ \t]*\"([^\"]+)\"",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char queue[256], extra[256];
                extract_match(pos, &matches[1], queue, sizeof(queue));
                snprintf(extra, sizeof(extra), "\"queue\":\"%s\"", queue);
                add_consumer(consumers, cons_count, queue, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: channel.consume('queue', callback) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re,
                "channel\\.consume\\([ \t]*['\"]([^'\"]+)['\"]",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char queue[256], extra[256];
                extract_match(pos, &matches[1], queue, sizeof(queue));
                snprintf(extra, sizeof(extra), "\"queue\":\"%s\"", queue);
                add_consumer(consumers, cons_count, queue, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* channel.assertQueue('queue') — declares intent to consume */
        if (cbm_regcomp(&re,
                "channel\\.assertQueue\\([ \t]*['\"]([^'\"]+)['\"]",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char queue[256], extra[256];
                extract_match(pos, &matches[1], queue, sizeof(queue));
                snprintf(extra, sizeof(extra), "\"queue\":\"%s\"", queue);
                add_consumer(consumers, cons_count, queue, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Rust: channel.basic_consume("queue", ...) */
    if (strcmp(ext, ".rs") == 0) {
        if (cbm_regcomp(&re,
                "basic_consume\\([ \t]*\"([^\"]+)\"",
                CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char queue[256], extra[256];
                extract_match(pos, &matches[1], queue, sizeof(queue));
                snprintf(extra, sizeof(extra), "\"queue\":\"%s\"", queue);
                add_consumer(consumers, cons_count, queue, node, extra);
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── AMQP matching logic ──────────────────────────────────────── */

/*
 * Match a consumer queue against a producer's exchange|routing_key.
 *
 * Producer identifier format: "exchange|routing_key"
 *   - Empty exchange (""|routing_key): default exchange, routing_key = queue name
 *   - "fanout_exchange|anything": fanout, all consumers match
 *   - Otherwise: direct or topic exchange matching
 *
 * For simplicity, we detect exchange type heuristically:
 *   - If routing_key contains '*' or '#', treat as topic exchange
 *   - If exchange contains "fanout", treat as fanout
 *   - Otherwise treat as direct exchange
 *
 * Consumer identifier is the queue name.
 */
static double match_amqp(const char *consumer_id, const char *producer_id) {
    /* Parse producer identifier: "exchange|routing_key" */
    char prod_copy[256];
    snprintf(prod_copy, sizeof(prod_copy), "%s", producer_id);

    char *sep = strchr(prod_copy, '|');
    if (!sep) return 0.0;

    *sep = '\0';
    const char *exchange = prod_copy;
    const char *routing_key = sep + 1;

    /* Default exchange: routing_key IS the queue name */
    if (exchange[0] == '\0') {
        if (strcmp(routing_key, consumer_id) == 0) {
            return AMQP_CONF_EXACT;
        }
        return 0.0;
    }

    /* Fanout exchange heuristic: exchange name contains "fanout" */
    if (strstr(exchange, "fanout") != NULL) {
        return AMQP_CONF_FANOUT;
    }

    /* Topic exchange heuristic: routing_key contains wildcards */
    if (strchr(routing_key, '*') || strchr(routing_key, '#')) {
        if (amqp_topic_match(routing_key, consumer_id)) {
            return AMQP_CONF_TOPIC;
        }
        return 0.0;
    }

    /* Direct exchange: exact match of routing_key to queue name */
    if (strcmp(routing_key, consumer_id) == 0) {
        return AMQP_CONF_EXACT;
    }

    return 0.0;
}

/* ── Process a single node ─────────────────────────────────────── */

static void process_node(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *node,
                         cbm_sl_producer_t *producers, int *prod_count,
                         cbm_sl_consumer_t *consumers, int *cons_count) {
    if (!node->file_path) return;

    const char *ext = sl_file_ext(node->file_path);

    /* Source files: scan for producer and consumer patterns */
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

int cbm_servicelink_rabbitmq(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "rabbitmq");

    /* 1. Allocate producer/consumer arrays on heap */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.rabbitmq", "error", "alloc_failed");
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

    cbm_log_info("servicelink.rabbitmq.discovery",
                 "producers", itoa_amqp(prod_count),
                 "consumers", itoa_amqp(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "rabbitmq",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "rabbitmq",
                                 "consumer", consumers[i].identifier,
                                 consumers[i].handler_qn, consumers[i].file_path,
                                 consumers[i].extra);
        }
    }

    /* 4. Match consumers to producers and create edges */
    int link_count = 0;

    for (int ci = 0; ci < cons_count; ci++) {
        const cbm_sl_consumer_t *c = &consumers[ci];

        for (int pi = 0; pi < prod_count; pi++) {
            const cbm_sl_producer_t *p = &producers[pi];

            /* Skip self-links (same node) */
            if (c->handler_id == p->source_id) continue;

            double conf = match_amqp(c->identifier, p->identifier);
            if (conf >= SL_MIN_CONFIDENCE) {
                /* Build extra JSON with exchange and routing_key */
                char extra_json[256];
                snprintf(extra_json, sizeof(extra_json), "%s", p->extra);
                sl_insert_edge(ctx, c->handler_id, p->source_id,
                              SL_EDGE_AMQP, c->identifier, conf, extra_json);
                link_count++;
            }
        }
    }

    cbm_log_info("servicelink.rabbitmq.done", "links", itoa_amqp(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
