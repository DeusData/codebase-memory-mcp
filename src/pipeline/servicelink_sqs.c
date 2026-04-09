/*
 * servicelink_sqs.c — SQS protocol linker.
 *
 * Discovers SQS producers (send_message, SendMessage, sendMessage calls) and
 * consumers (receive_message, ReceiveMessage, @SqsListener, Lambda event sources),
 * then creates SQS_CALLS edges in the graph buffer.
 *
 * Supported languages: Python (boto3), Go (AWS SDK), Java/Kotlin, Node.js/TypeScript.
 * Also scans .tf files for Lambda SQS event source mappings.
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define SQS_CONF_EXACT   0.95  /* exact queue name match */
#define SQS_CONF_PARTIAL 0.70  /* partial / fuzzy match (unused — no fuzzy for SQS) */

/* ── itoa helper (thread-local rotating buffers) ────────────────── */

static const char *itoa_sqs(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

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

/* ── Queue name extraction ─────────────────────────────────────── */

/*
 * Extract the queue name from a URL, ARN, or plain name.
 *
 * "https://sqs.us-east-1.amazonaws.com/123456789/order-events" → "order-events"
 * "arn:aws:sqs:us-east-1:123456789:order-events"               → "order-events"
 * "order-events"                                                → "order-events"
 */
static void extract_queue_name(const char *url_or_name, char *out, size_t out_size) {
    if (!url_or_name || !url_or_name[0]) {
        out[0] = '\0';
        return;
    }

    /* ARN format: arn:aws:sqs:region:account:queue-name */
    if (strncmp(url_or_name, "arn:", 4) == 0) {
        const char *last_colon = strrchr(url_or_name, ':');
        if (last_colon && last_colon[1]) {
            snprintf(out, out_size, "%s", last_colon + 1);
            return;
        }
    }

    /* URL format: contains '/' — take last segment */
    const char *last_slash = strrchr(url_or_name, '/');
    if (last_slash && last_slash[1]) {
        snprintf(out, out_size, "%s", last_slash + 1);
        return;
    }

    /* Plain name */
    snprintf(out, out_size, "%s", url_or_name);
}

/* ── Producer scanning (SQS senders) ──────────────────────────── */

static void scan_producers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Python (boto3): sqs.send_message(QueueUrl='...') or send_message_batch */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "send_message(_batch)?\\([^)]*QueueUrl[[:space:]]*=[[:space:]]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
                char url[256];
                extract_match(pos, &matches[2], url, sizeof(url));
                char queue[256];
                extract_queue_name(url, queue, sizeof(queue));
                if (queue[0]) {
                    add_producer(producers, prod_count, queue, node,
                                 "\"role\":\"sender\"");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Go (AWS SDK): SendMessage(...&sqs.SendMessageInput{...QueueUrl: aws.String("...") */
    if (strcmp(ext, ".go") == 0) {
        /* Broad pattern: SendMessageInput with QueueUrl */
        if (cbm_regcomp(&re, "SendMessageInput[[:space:]]*\\{[^}]*QueueUrl:[^'\"]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char url[256];
                extract_match(pos, &matches[1], url, sizeof(url));
                char queue[256];
                extract_queue_name(url, queue, sizeof(queue));
                if (queue[0]) {
                    add_producer(producers, prod_count, queue, node,
                                 "\"role\":\"sender\"");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java/Kotlin: sqsClient.sendMessage(...queueUrl("...")...) or amazonSQS.sendMessage("...") */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        /* SendMessageRequest.builder().queueUrl("...") */
        if (cbm_regcomp(&re, "queueUrl\\([[:space:]]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char url[256];
                extract_match(pos, &matches[1], url, sizeof(url));
                char queue[256];
                extract_queue_name(url, queue, sizeof(queue));
                if (queue[0]) {
                    add_producer(producers, prod_count, queue, node,
                                 "\"role\":\"sender\"");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* amazonSQS.sendMessage("url", ...) */
        if (cbm_regcomp(&re, "sendMessage\\([[:space:]]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char url[256];
                extract_match(pos, &matches[1], url, sizeof(url));
                char queue[256];
                extract_queue_name(url, queue, sizeof(queue));
                if (queue[0]) {
                    add_producer(producers, prod_count, queue, node,
                                 "\"role\":\"sender\"");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: .sendMessage({QueueUrl: '...'}) or SendMessageCommand({QueueUrl: '...'}) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        /* sendMessage({...QueueUrl: '...'}) */
        if (cbm_regcomp(&re, "[Ss]end[Mm]essage[^{]*\\{[^}]*QueueUrl:[[:space:]]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char url[256];
                extract_match(pos, &matches[1], url, sizeof(url));
                char queue[256];
                extract_queue_name(url, queue, sizeof(queue));
                if (queue[0]) {
                    add_producer(producers, prod_count, queue, node,
                                 "\"role\":\"sender\"");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── Consumer scanning (SQS receivers) ────────────────────────── */

static void scan_consumers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_consumer_t *consumers, int *cons_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Python (boto3): sqs.receive_message(QueueUrl='...') */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "receive_message\\([^)]*QueueUrl[[:space:]]*=[[:space:]]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char url[256];
                extract_match(pos, &matches[1], url, sizeof(url));
                char queue[256];
                extract_queue_name(url, queue, sizeof(queue));
                if (queue[0]) {
                    add_consumer(consumers, cons_count, queue, node,
                                 "\"role\":\"receiver\"");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Go (AWS SDK): ReceiveMessageInput{...QueueUrl: aws.String("...")} */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "ReceiveMessageInput[[:space:]]*\\{[^}]*QueueUrl:[^'\"]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char url[256];
                extract_match(pos, &matches[1], url, sizeof(url));
                char queue[256];
                extract_queue_name(url, queue, sizeof(queue));
                if (queue[0]) {
                    add_consumer(consumers, cons_count, queue, node,
                                 "\"role\":\"receiver\"");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java/Kotlin: @SqsListener("queue-name") or @SqsListener(value = "queue-name") */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "@SqsListener\\([^)]*[\"']([^\"']+)[\"']",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char name[256];
                extract_match(pos, &matches[1], name, sizeof(name));
                char queue[256];
                extract_queue_name(name, queue, sizeof(queue));
                if (queue[0]) {
                    add_consumer(consumers, cons_count, queue, node,
                                 "\"role\":\"receiver\"");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* sqsClient.receiveMessage(...queueUrl("...")...) */
        if (cbm_regcomp(&re, "receiveMessage\\([^)]*queueUrl\\([[:space:]]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char url[256];
                extract_match(pos, &matches[1], url, sizeof(url));
                char queue[256];
                extract_queue_name(url, queue, sizeof(queue));
                if (queue[0]) {
                    add_consumer(consumers, cons_count, queue, node,
                                 "\"role\":\"receiver\"");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: .receiveMessage({QueueUrl: '...'}) or ReceiveMessageCommand({QueueUrl: '...'}) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "[Rr]eceive[Mm]essage[^{]*\\{[^}]*QueueUrl:[[:space:]]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char url[256];
                extract_match(pos, &matches[1], url, sizeof(url));
                char queue[256];
                extract_queue_name(url, queue, sizeof(queue));
                if (queue[0]) {
                    add_consumer(consumers, cons_count, queue, node,
                                 "\"role\":\"receiver\"");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Terraform: event_source_arn = "arn:aws:sqs:...:queue-name" (Lambda event source) */
    if (strcmp(ext, ".tf") == 0) {
        if (cbm_regcomp(&re, "event_source_arn[[:space:]]*=[[:space:]]*['\"]arn:aws:sqs:[^'\"]*:([^'\"/:]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char queue[256];
                extract_match(pos, &matches[1], queue, sizeof(queue));
                if (queue[0]) {
                    add_consumer(consumers, cons_count, queue, node,
                                 "\"role\":\"receiver\"");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── Queue name matching ───────────────────────────────────────── */

/*
 * Match queue names. Only exact match is supported (no fuzzy).
 * Returns SQS_CONF_EXACT (0.95) on match, 0.0 otherwise.
 */
static double match_queues(const char *consumer_queue, const char *producer_queue) {
    if (strcmp(consumer_queue, producer_queue) == 0) {
        return SQS_CONF_EXACT;
    }
    return 0.0;
}

/* ── Process a single node ─────────────────────────────────────── */

static void process_node(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *node,
                         cbm_sl_producer_t *producers, int *prod_count,
                         cbm_sl_consumer_t *consumers, int *cons_count) {
    if (!node->file_path) return;

    const char *ext = sl_file_ext(node->file_path);

    if (strcmp(ext, ".go") == 0 || strcmp(ext, ".py") == 0 ||
        strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0 ||
        strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0 ||
        strcmp(ext, ".tf") == 0) {
        char *source = sl_read_node_source(ctx, node);
        if (source) {
            scan_producers(source, ext, node, producers, prod_count);
            scan_consumers(source, ext, node, consumers, cons_count);
            free(source);
        }
    }
}

/* ── Main entry point ──────────────────────────────────────────── */

int cbm_servicelink_sqs(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "sqs");

    /* 1. Allocate producer/consumer arrays on heap */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.sqs", "error", "alloc_failed");
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

    cbm_log_info("servicelink.sqs.discovery",
                 "producers", itoa_sqs(prod_count),
                 "consumers", itoa_sqs(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "sqs",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "sqs",
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

            double conf = match_queues(c->identifier, p->identifier);
            if (conf > best_conf) {
                best_conf = conf;
                best_pi = pi;
            }
        }

        if (best_pi >= 0 && best_conf >= SL_MIN_CONFIDENCE) {
            const cbm_sl_producer_t *p = &producers[best_pi];
            sl_insert_edge(ctx, c->handler_id, p->source_id,
                          SL_EDGE_SQS, c->identifier, best_conf, NULL);
            link_count++;
        }
    }

    cbm_log_info("servicelink.sqs.done", "links", itoa_sqs(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
