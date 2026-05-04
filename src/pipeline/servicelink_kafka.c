/*
 * servicelink_kafka.c — Kafka protocol linker.
 *
 * Discovers Kafka producers (send/produce calls) and consumers (subscribe/listener
 * patterns) in source code, then creates KAFKA_CALLS edges in the graph buffer.
 *
 * Supported languages: Go, Python, Java/Kotlin, Node.js/TypeScript, Rust.
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define KAFKA_CONF_EXACT   0.95  /* exact topic match */

/* ── itoa helper (thread-local rotating buffers) ────────────────── */

static const char *itoa_kafka(int val) {
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

/* ── Producer scanning ─────────────────────────────────────────── */

/*
 * Scan source code for Kafka producer patterns.
 * Detected topic names become producer identifiers.
 */
static void scan_producers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Go: writer := &kafka.Writer{...Topic: "xxx"} */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "kafka\\.Writer\\{[^}]*Topic:[ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"producer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Go: .Produce(..."xxx") — generic */
        if (cbm_regcomp(&re, "\\.Produce\\(.*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"producer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java/Kotlin: kafkaTemplate.send("xxx") */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "kafkaTemplate\\.send\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"producer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Java: producer.send(new ProducerRecord<...>("xxx")) */
        if (cbm_regcomp(&re, "producer\\.send\\([ \t]*new[ \t]+ProducerRecord[^(]*\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"producer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Java: @SendTo("xxx") */
        if (cbm_regcomp(&re, "@SendTo\\([ \t]*\"([^\"]+)\"[ \t]*\\)",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"producer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Python: producer.send('xxx') or producer.produce('xxx') */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "producer\\.send\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"producer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        if (cbm_regcomp(&re, "producer\\.produce\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"producer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Python: send_message(topic='xxx') */
        if (cbm_regcomp(&re, "send_message\\([ \t]*topic[ \t]*=[ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"producer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: producer.send({...topic: 'xxx'}) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "producer\\.send\\([ \t]*\\{[^}]*topic:[ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"producer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* .produce({topic: 'xxx'}) */
        if (cbm_regcomp(&re, "\\.produce\\([ \t]*\\{[^}]*topic:[ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"producer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Rust: FutureRecord::to("xxx") */
    if (strcmp(ext, ".rs") == 0) {
        if (cbm_regcomp(&re, "FutureRecord::to\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"producer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── Consumer scanning ─────────────────────────────────────────── */

/*
 * Scan source code for Kafka consumer patterns.
 * Detected topic names become consumer identifiers.
 */
static void scan_consumers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_consumer_t *consumers, int *cons_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Go: kafka.NewReader(kafka.ReaderConfig{...Topic: "xxx"}) */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "kafka\\.NewReader\\([ \t]*kafka\\.ReaderConfig[ \t]*\\{[^}]*Topic:[ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"consumer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Go: consumer.SubscribeTopics([]string{"xxx"...}) */
        if (cbm_regcomp(&re, "consumer\\.SubscribeTopics\\([^{]*\\{[^}]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"consumer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java/Kotlin: @KafkaListener(topics = {"xxx"}) or @KafkaListener(topics = "xxx") */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "@KafkaListener\\([ \t]*topics[ \t]*=[ \t]*\\{?[ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"consumer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Java: consumer.subscribe(Arrays.asList("xxx") or List.of("xxx")) */
        if (cbm_regcomp(&re, "consumer\\.subscribe\\([ \t]*(Arrays\\.asList|List\\.of)[ \t]*\\([ \t]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[2], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"consumer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Python: KafkaConsumer('xxx') */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "KafkaConsumer\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"consumer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Python: consumer.subscribe(['xxx']) */
        if (cbm_regcomp(&re, "consumer\\.subscribe\\([ \t]*\\[['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"consumer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: consumer.subscribe({...topic(s): ['xxx']}) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "consumer\\.subscribe\\([ \t]*\\{[^}]*topics?[ \t]*:[ \t]*\\[?[ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"consumer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Rust: consumer.subscribe(&["xxx"]) */
    if (strcmp(ext, ".rs") == 0) {
        if (cbm_regcomp(&re, "consumer\\.subscribe\\([ \t]*&\\[?\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char topic[256];
                extract_match(pos, &matches[1], topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"consumer\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── Topic matching ────────────────────────────────────────────── */

/*
 * Match consumer topic against producer topic.
 * Returns confidence: 0.95 for exact match, 0.0 otherwise.
 * Kafka topics are matched by exact name only.
 */
static double match_topics(const char *consumer_id, const char *producer_id) {
    if (strcmp(consumer_id, producer_id) == 0) {
        return KAFKA_CONF_EXACT;
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

int cbm_servicelink_kafka(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "kafka");

    /* 1. Allocate producer/consumer arrays on heap */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.kafka", "error", "alloc_failed");
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

    cbm_log_info("servicelink.kafka.discovery",
                 "producers", itoa_kafka(prod_count),
                 "consumers", itoa_kafka(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "kafka",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "kafka",
                                 "consumer", consumers[i].identifier,
                                 consumers[i].handler_qn, consumers[i].file_path,
                                 consumers[i].extra);
        }
    }

    /* 4. Match consumers to producers and create edges */
    int link_count = 0;

    for (int ci = 0; ci < cons_count; ci++) {
        const cbm_sl_consumer_t *c = &consumers[ci];
        double best_conf = 0.0;
        int best_pi = -1;

        for (int pi = 0; pi < prod_count; pi++) {
            const cbm_sl_producer_t *p = &producers[pi];

            /* Skip self-links (same node) */
            if (c->handler_id == p->source_id) continue;

            double conf = match_topics(c->identifier, p->identifier);
            if (conf > best_conf) {
                best_conf = conf;
                best_pi = pi;
            }
        }

        if (best_pi >= 0 && best_conf >= SL_MIN_CONFIDENCE) {
            const cbm_sl_producer_t *p = &producers[best_pi];
            sl_insert_edge(ctx, c->handler_id, p->source_id,
                          SL_EDGE_KAFKA, c->identifier, best_conf, NULL);
            link_count++;
        }
    }

    cbm_log_info("servicelink.kafka.done", "links", itoa_kafka(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
