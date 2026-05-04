/*
 * servicelink_sns.c — AWS SNS protocol linker.
 *
 * Discovers SNS publishers (sns.publish, PublishCommand, etc.) and subscribers
 * (sns.subscribe, SubscribeCommand, topic_subscription in Terraform), then
 * creates SNS_CALLS edges in the graph buffer.
 *
 * Supported languages: Go, Python, Java/Kotlin, Node.js/TypeScript, Terraform.
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define SNS_CONF_EXACT   0.95  /* exact topic name match */
#define SNS_CONF_PARTIAL 0.70  /* partial/fuzzy match (unused for now) */

/* ── itoa helper (thread-local rotating buffers) ────────────────── */

static const char *itoa_sns(int val) {
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
 * Extract a topic name from an ARN or reference:
 *   "arn:aws:sns:us-east-1:123456789:order-events" → "order-events"
 *   "aws_sns_topic.order_events.arn" → "order_events"
 *   "order-events" → "order-events" (pass-through)
 */
static void extract_topic_name(const char *arn_or_name, char *out, size_t out_size) {
    if (!arn_or_name || !out || out_size == 0) return;

    /* ARN format: arn:aws:sns:region:account:topic-name */
    if (strncmp(arn_or_name, "arn:", 4) == 0) {
        const char *last_colon = strrchr(arn_or_name, ':');
        if (last_colon && last_colon[1] != '\0') {
            snprintf(out, out_size, "%s", last_colon + 1);
            return;
        }
    }

    /* Terraform reference: aws_sns_topic.TOPIC_NAME.arn */
    const char *dot_arn = strstr(arn_or_name, ".arn");
    if (dot_arn) {
        /* Find the first dot to get the middle segment */
        const char *first_dot = strchr(arn_or_name, '.');
        if (first_dot && first_dot < dot_arn) {
            size_t len = (size_t)(dot_arn - first_dot - 1);
            if (len >= out_size) len = out_size - 1;
            memcpy(out, first_dot + 1, len);
            out[len] = '\0';
            return;
        }
    }

    /* Already a plain name */
    snprintf(out, out_size, "%s", arn_or_name);
}

/* ── Publisher scanning ────────────────────────────────────────── */

static void scan_publishers(const char *source, const char *ext,
                            const cbm_gbuf_node_t *node,
                            cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Python (boto3): sns.publish(TopicArn='arn:...:topic') */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "publish\\([^)]*TopicArn[[:space:]]*=[[:space:]]*['\"]([^'\"]+)['\"]",
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

    /* Go (AWS SDK): Publish(ctx, &sns.PublishInput{...TopicArn: aws.String("...")}) */
    if (strcmp(ext, ".go") == 0) {
        /* Pattern 1: TopicArn with aws.String */
        if (cbm_regcomp(&re, "TopicArn:[[:space:]]*aws\\.String\\([[:space:]]*['\"]([^'\"]+)['\"]",
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
        /* Pattern 2: TopicArn with string literal directly */
        if (cbm_regcomp(&re, "TopicArn:[[:space:]]*['\"]([^'\"]+)['\"]",
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

    /* Java/Kotlin: snsClient.publish(...topicArn("...")) or amazonSNS.publish("...") */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        /* PublishRequest.builder()...topicArn("...") */
        if (cbm_regcomp(&re, "topicArn\\([[:space:]]*\"([^\"]+)\"",
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
        /* amazonSNS.publish("arn:...", ...) */
        if (cbm_regcomp(&re, "\\.publish\\([[:space:]]*\"([^\"]+)\"",
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

    /* Node.js/TypeScript: .publish({TopicArn: '...'}) or sns.send(new PublishCommand({TopicArn: '...'})) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "TopicArn:[[:space:]]*['\"]([^'\"]+)['\"]",
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
}

/* ── Subscriber scanning ───────────────────────────────────────── */

static void scan_subscribers(const char *source, const char *ext,
                             const cbm_gbuf_node_t *node,
                             cbm_sl_consumer_t *consumers, int *cons_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Python (boto3): sns.subscribe(TopicArn='arn:...:topic') */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "subscribe\\([^)]*TopicArn[[:space:]]*=[[:space:]]*['\"]([^'\"]+)['\"]",
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

    /* Go (AWS SDK): Subscribe(ctx, &sns.SubscribeInput{...TopicArn: aws.String("...")}) */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "TopicArn:[[:space:]]*aws\\.String\\([[:space:]]*['\"]([^'\"]+)['\"]",
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
        /* Also match direct string TopicArn */
        if (cbm_regcomp(&re, "TopicArn:[[:space:]]*['\"]([^'\"]+)['\"]",
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

    /* Java/Kotlin: snsClient.subscribe(...topicArn("...")) or @SnsNotificationMapping("...") */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "topicArn\\([[:space:]]*\"([^\"]+)\"",
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
        /* @SnsNotificationMapping("topic-name") */
        if (cbm_regcomp(&re, "@SnsNotificationMapping\\([[:space:]]*\"([^\"]+)\"",
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

    /* Node.js/TypeScript: .subscribe({TopicArn: '...'}) or sns.send(new SubscribeCommand({TopicArn: '...'})) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "TopicArn:[[:space:]]*['\"]([^'\"]+)['\"]",
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

    /* Terraform: topic_arn = "arn:aws:sns:..." or topic_arn = aws_sns_topic.NAME.arn */
    if (strcmp(ext, ".tf") == 0) {
        /* topic_arn = "arn:..." */
        if (cbm_regcomp(&re, "topic_arn[[:space:]]*=[[:space:]]*\"([^\"]+)\"",
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
        /* topic_arn = aws_sns_topic.NAME.arn */
        if (cbm_regcomp(&re, "topic_arn[[:space:]]*=[[:space:]]*(aws_sns_topic\\.[a-zA-Z0-9_-]+\\.arn)",
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

/* ── Topic matching ────────────────────────────────────────────── */

/*
 * Match publishers to subscribers by extracted topic name.
 * Exact match on topic name → SNS_CONF_EXACT (0.95).
 * Skip self-links (same node ID).
 */

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

int cbm_servicelink_sns(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "sns");

    /* 1. Allocate producer/consumer arrays on heap */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.sns", "error", "alloc_failed");
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

    cbm_log_info("servicelink.sns.discovery",
                 "producers", itoa_sns(prod_count),
                 "consumers", itoa_sns(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "sns",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "sns",
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
                              SL_EDGE_SNS, c->identifier, SNS_CONF_EXACT, NULL);
                link_count++;
                break;  /* one match per consumer is enough */
            }
        }
    }

    cbm_log_info("servicelink.sns.done", "links", itoa_sns(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
