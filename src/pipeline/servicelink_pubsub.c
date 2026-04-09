/*
 * servicelink_pubsub.c — GCP Pub/Sub protocol linker.
 *
 * Discovers Pub/Sub publishers (topic.Publish, publisher.publish, etc.) and
 * subscribers (subscription.Receive, subscriber.subscribe, etc.) in source code,
 * then creates PUBSUB_CALLS edges in the graph buffer.
 *
 * Supported languages: Go, Python, Java/Kotlin, Node.js/TypeScript, Terraform.
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define PUBSUB_CONF_EXACT   0.95  /* exact topic match */

/* ── itoa helper (thread-local rotating buffers) ────────────────── */

static const char *itoa_pubsub(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Forward declarations ──────────────────────────────────────── */

static void scan_publishers(const char *source, const char *ext,
                            const cbm_gbuf_node_t *node,
                            cbm_sl_producer_t *producers, int *prod_count);
static void scan_subscribers(const char *source, const char *ext,
                             const cbm_gbuf_node_t *node,
                             cbm_sl_consumer_t *consumers, int *cons_count);

/* ── Helpers ───────────────────────────────────────────────────── */

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

/* Extract a regex submatch into a buffer. */
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

/* ── Topic name extraction ─────────────────────────────────────── */

/*
 * Extract a topic name from a GCP resource path or Terraform reference:
 *   "projects/my-project/topics/my-topic" → "my-topic"
 *   "google_pubsub_topic.order_events.name" → "order_events"
 *   "google_pubsub_topic.order_events.id"   → "order_events"
 *   "my-topic" → "my-topic" (pass-through)
 */
static void extract_topic_name(const char *raw, char *out, size_t out_size) {
    if (!raw || !out || out_size == 0) return;

    /* GCP resource path: projects/P/topics/T */
    const char *topics_seg = strstr(raw, "topics/");
    if (topics_seg) {
        const char *name_start = topics_seg + 7; /* strlen("topics/") */
        if (*name_start != '\0') {
            snprintf(out, out_size, "%s", name_start);
            return;
        }
    }

    /* Terraform reference: google_pubsub_topic.NAME.name or .id */
    if (strncmp(raw, "google_pubsub_topic.", 20) == 0) {
        const char *name_start = raw + 20;
        const char *dot = strchr(name_start, '.');
        if (dot) {
            size_t len = (size_t)(dot - name_start);
            if (len >= out_size) len = out_size - 1;
            memcpy(out, name_start, len);
            out[len] = '\0';
            return;
        }
    }

    /* Already a plain name */
    snprintf(out, out_size, "%s", raw);
}

/* ── Publisher scanning ────────────────────────────────────────── */

static void scan_publishers(const char *source, const char *ext,
                            const cbm_gbuf_node_t *node,
                            cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Go: client.Topic("topic-name") or pubsub.NewClient(...).Topic("xxx") */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "\\.Topic\\([[:space:]]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char raw[256], topic[256];
                extract_match(pos, &matches[1], raw, sizeof(raw));
                extract_topic_name(raw, topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Python: publisher.publish(topic_path, ...) where topic_path is a string */
    if (strcmp(ext, ".py") == 0) {
        /* publisher.publish("projects/P/topics/T", ...) or publish(topic_path) with string */
        if (cbm_regcomp(&re, "\\.publish\\([[:space:]]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char raw[256], topic[256];
                extract_match(pos, &matches[1], raw, sizeof(raw));
                extract_topic_name(raw, topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Python: topic_path(project, "my-topic") */
        if (cbm_regcomp(&re, "topic_path\\([^,]*,[[:space:]]*['\"]([^'\"]+)['\"]",
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

    /* Java/Kotlin: TopicName.of("project", "topic-name") or pubsub.topic("topic-name") */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        /* TopicName.of("project", "topic-name") */
        if (cbm_regcomp(&re, "TopicName\\.of\\([^,]*,[[:space:]]*\"([^\"]+)\"",
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

        /* pubsub.topic("topic-name") */
        if (cbm_regcomp(&re, "\\.topic\\([[:space:]]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char raw[256], topic[256];
                extract_match(pos, &matches[1], raw, sizeof(raw));
                extract_topic_name(raw, topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: pubsub.topic("topic-name").publish(...) or new PubSub().topic("xxx") */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "\\.topic\\([[:space:]]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char raw[256], topic[256];
                extract_match(pos, &matches[1], raw, sizeof(raw));
                extract_topic_name(raw, topic, sizeof(topic));
                add_producer(producers, prod_count, topic, node,
                             "\"role\":\"publisher\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Terraform: google_pubsub_topic resource with name = "xxx" */
    if (strcmp(ext, ".tf") == 0) {
        if (cbm_regcomp(&re, "google_pubsub_topic['\"]?[[:space:]]+['\"]?[a-zA-Z0-9_-]+['\"]?[[:space:]]*\\{[^}]*name[[:space:]]*=[[:space:]]*\"([^\"]+)\"",
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

/* ── Subscriber scanning ───────────────────────────────────────── */

static void scan_subscribers(const char *source, const char *ext,
                             const cbm_gbuf_node_t *node,
                             cbm_sl_consumer_t *consumers, int *cons_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Go: client.Subscription("sub-name") — often sub name equals topic name */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "\\.Subscription\\([[:space:]]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char raw[256], topic[256];
                extract_match(pos, &matches[1], raw, sizeof(raw));
                extract_topic_name(raw, topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Python: subscriber.subscribe(subscription_path, callback) */
    if (strcmp(ext, ".py") == 0) {
        /* subscriber.subscribe("projects/P/subscriptions/S", ...) */
        if (cbm_regcomp(&re, "\\.subscribe\\([[:space:]]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char raw[256], topic[256];
                extract_match(pos, &matches[1], raw, sizeof(raw));
                extract_topic_name(raw, topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Python: subscription_path(project, "sub-name") */
        if (cbm_regcomp(&re, "subscription_path\\([^,]*,[[:space:]]*['\"]([^'\"]+)['\"]",
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

    /* Java/Kotlin: SubscriptionName.of("project", "sub-name") */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "SubscriptionName\\.of\\([^,]*,[[:space:]]*\"([^\"]+)\"",
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

    /* Node.js/TypeScript: pubsub.subscription("sub-name").on("message", ...) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "\\.subscription\\([[:space:]]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char raw[256], topic[256];
                extract_match(pos, &matches[1], raw, sizeof(raw));
                extract_topic_name(raw, topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Terraform: google_pubsub_subscription with topic reference */
    if (strcmp(ext, ".tf") == 0) {
        /* topic = google_pubsub_topic.NAME.name or .id */
        if (cbm_regcomp(&re, "topic[[:space:]]*=[[:space:]]*(google_pubsub_topic\\.[a-zA-Z0-9_-]+\\.[a-z]+)",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char raw[256], topic[256];
                extract_match(pos, &matches[1], raw, sizeof(raw));
                /* Extract middle segment: google_pubsub_topic.NAME.xxx → NAME */
                extract_topic_name(raw, topic, sizeof(topic));
                add_consumer(consumers, cons_count, topic, node,
                             "\"role\":\"subscriber\"");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* topic = "projects/P/topics/T" */
        if (cbm_regcomp(&re, "topic[[:space:]]*=[[:space:]]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char raw[256], topic[256];
                extract_match(pos, &matches[1], raw, sizeof(raw));
                extract_topic_name(raw, topic, sizeof(topic));
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

    /* Source files: scan for publishers and subscribers */
    if (strcmp(ext, ".go") == 0 || strcmp(ext, ".py") == 0 ||
        strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0 ||
        strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0 ||
        strcmp(ext, ".tf") == 0) {
        char *source = sl_read_node_source(ctx, node);
        if (source) {
            scan_publishers(source, ext, node, producers, prod_count);
            scan_subscribers(source, ext, node, consumers, cons_count);
            free(source);
        }
    }
}

/* ── Main entry point ──────────────────────────────────────────── */

int cbm_servicelink_pubsub(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "pubsub");

    /* 1. Allocate producer/consumer arrays on heap */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.pubsub", "error", "alloc_failed");
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

    cbm_log_info("servicelink.pubsub.discovery",
                 "producers", itoa_pubsub(prod_count),
                 "consumers", itoa_pubsub(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "pubsub",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "pubsub",
                                 "consumer", consumers[i].identifier,
                                 consumers[i].handler_qn, consumers[i].file_path,
                                 consumers[i].extra);
        }
    }

    /* 4. Match consumers to producers by topic name and create edges */
    int link_count = 0;

    for (int ci = 0; ci < cons_count; ci++) {
        const cbm_sl_consumer_t *c = &consumers[ci];

        for (int pi = 0; pi < prod_count; pi++) {
            const cbm_sl_producer_t *p = &producers[pi];

            /* Skip self-links (same node) */
            if (c->handler_id == p->source_id) continue;

            /* Exact topic name match */
            if (strcmp(c->identifier, p->identifier) == 0) {
                sl_insert_edge(ctx, c->handler_id, p->source_id,
                              SL_EDGE_PUBSUB, c->identifier, PUBSUB_CONF_EXACT, NULL);
                link_count++;
                break;  /* one match per consumer is enough */
            }
        }
    }

    cbm_log_info("servicelink.pubsub.done", "links", itoa_pubsub(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
