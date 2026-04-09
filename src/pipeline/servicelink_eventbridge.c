/*
 * servicelink_eventbridge.c — AWS EventBridge protocol linker.
 *
 * Discovers EventBridge producers (put_events calls with Source+DetailType) and
 * consumers (Terraform event rules, CDK EventPattern) in source code, then
 * creates EVENTBRIDGE_CALLS edges in the graph buffer.
 *
 * Identifier format: "source:detail_type" compound key.
 *
 * Supported languages: Go, Python, Java/Kotlin, Node.js/TypeScript, Terraform.
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define EB_CONF_EXACT    0.95  /* exact source+detail_type match */
#define EB_CONF_SOURCE   0.80  /* source-only match */

/* ── itoa helper (thread-local rotating buffers) ────────────────── */

static const char *itoa_eb(int val) {
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

/* Build a compound identifier "source:detail_type". If detail_type is empty,
 * use just the source (will match at lower confidence). */
static void build_compound_id(const char *source_name, const char *detail_type,
                              char *out, size_t out_size) {
    if (detail_type[0] != '\0') {
        snprintf(out, out_size, "%s:%s", source_name, detail_type);
    } else {
        snprintf(out, out_size, "%s", source_name);
    }
}

/* Build extra JSON with source and detail_type fields. */
static void build_extra_json(const char *source_name, const char *detail_type,
                             char *out, size_t out_size) {
    if (detail_type[0] != '\0') {
        snprintf(out, out_size,
                 "\"source\":\"%s\",\"detail_type\":\"%s\",\"role\":\"producer\"",
                 source_name, detail_type);
    } else {
        snprintf(out, out_size,
                 "\"source\":\"%s\",\"role\":\"producer\"",
                 source_name);
    }
}

static void build_extra_json_consumer(const char *source_name, const char *detail_type,
                                      char *out, size_t out_size) {
    if (detail_type[0] != '\0') {
        snprintf(out, out_size,
                 "\"source\":\"%s\",\"detail_type\":\"%s\",\"role\":\"consumer\"",
                 source_name, detail_type);
    } else {
        snprintf(out, out_size,
                 "\"source\":\"%s\",\"role\":\"consumer\"",
                 source_name);
    }
}

/* ── Producer scanning ─────────────────────────────────────────── */

/*
 * Scan source code for EventBridge producer patterns (put_events).
 * Extracts Source and DetailType fields, builds compound identifier.
 */
static void scan_producers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re_src, re_dt;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Python (boto3): events.put_events(Entries=[{...Source: '...', DetailType: '...'}]) */
    if (strcmp(ext, ".py") == 0) {
        /* Look for put_events calls, then extract Source and DetailType */
        cbm_regex_t re_call;
        if (cbm_regcomp(&re_call, "put_events\\(", CBM_REG_EXTENDED) == CBM_REG_OK) {
            if (cbm_regexec(&re_call, source, 0, NULL, 0) == CBM_REG_OK) {
                /* Extract Source values */
                if (cbm_regcomp(&re_src, "['\"]Source['\"][[:space:]]*:[[:space:]]*['\"]([^'\"]+)['\"]",
                                CBM_REG_EXTENDED) == CBM_REG_OK) {
                    pos = source;
                    while (cbm_regexec(&re_src, pos, 2, matches, 0) == CBM_REG_OK) {
                        char src_name[256];
                        extract_match(pos, &matches[1], src_name, sizeof(src_name));

                        /* Try to find a DetailType near this Source */
                        char dt_name[256] = "";
                        if (cbm_regcomp(&re_dt, "['\"]DetailType['\"][[:space:]]*:[[:space:]]*['\"]([^'\"]+)['\"]",
                                        CBM_REG_EXTENDED) == CBM_REG_OK) {
                            if (cbm_regexec(&re_dt, source, 2, matches, 0) == CBM_REG_OK) {
                                extract_match(source, &matches[1], dt_name, sizeof(dt_name));
                            }
                            cbm_regfree(&re_dt);
                        }

                        char compound[256], extra[256];
                        build_compound_id(src_name, dt_name, compound, sizeof(compound));
                        build_extra_json(src_name, dt_name, extra, sizeof(extra));
                        add_producer(producers, prod_count, compound, node, extra);
                        pos += matches[0].rm_eo;
                    }
                    cbm_regfree(&re_src);
                }
            }
            cbm_regfree(&re_call);
        }

        /* Also: Source= keyword arg style */
        if (cbm_regcomp(&re_src, "Source[[:space:]]*=[[:space:]]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            cbm_regex_t re_pe;
            if (cbm_regcomp(&re_pe, "put_events\\(", CBM_REG_EXTENDED) == CBM_REG_OK) {
                if (cbm_regexec(&re_pe, source, 0, NULL, 0) == CBM_REG_OK) {
                    pos = source;
                    while (cbm_regexec(&re_src, pos, 2, matches, 0) == CBM_REG_OK) {
                        char src_name[256];
                        extract_match(pos, &matches[1], src_name, sizeof(src_name));

                        char dt_name[256] = "";
                        if (cbm_regcomp(&re_dt, "DetailType[[:space:]]*=[[:space:]]*['\"]([^'\"]+)['\"]",
                                        CBM_REG_EXTENDED) == CBM_REG_OK) {
                            if (cbm_regexec(&re_dt, source, 2, matches, 0) == CBM_REG_OK) {
                                extract_match(source, &matches[1], dt_name, sizeof(dt_name));
                            }
                            cbm_regfree(&re_dt);
                        }

                        char compound[256], extra[256];
                        build_compound_id(src_name, dt_name, compound, sizeof(compound));
                        build_extra_json(src_name, dt_name, extra, sizeof(extra));
                        add_producer(producers, prod_count, compound, node, extra);
                        pos += matches[0].rm_eo;
                    }
                }
                cbm_regfree(&re_pe);
            }
            cbm_regfree(&re_src);
        }
    }

    /* Go: PutEventsInput{...Source: aws.String("..."), DetailType: aws.String("...")} */
    if (strcmp(ext, ".go") == 0) {
        cbm_regex_t re_call;
        if (cbm_regcomp(&re_call, "PutEventsInput", CBM_REG_EXTENDED) == CBM_REG_OK) {
            if (cbm_regexec(&re_call, source, 0, NULL, 0) == CBM_REG_OK) {
                /* Extract Source */
                if (cbm_regcomp(&re_src, "Source:[[:space:]]*aws\\.String\\([[:space:]]*\"([^\"]+)\"",
                                CBM_REG_EXTENDED) == CBM_REG_OK) {
                    pos = source;
                    while (cbm_regexec(&re_src, pos, 2, matches, 0) == CBM_REG_OK) {
                        char src_name[256];
                        extract_match(pos, &matches[1], src_name, sizeof(src_name));

                        char dt_name[256] = "";
                        if (cbm_regcomp(&re_dt, "DetailType:[[:space:]]*aws\\.String\\([[:space:]]*\"([^\"]+)\"",
                                        CBM_REG_EXTENDED) == CBM_REG_OK) {
                            if (cbm_regexec(&re_dt, source, 2, matches, 0) == CBM_REG_OK) {
                                extract_match(source, &matches[1], dt_name, sizeof(dt_name));
                            }
                            cbm_regfree(&re_dt);
                        }

                        char compound[256], extra[256];
                        build_compound_id(src_name, dt_name, compound, sizeof(compound));
                        build_extra_json(src_name, dt_name, extra, sizeof(extra));
                        add_producer(producers, prod_count, compound, node, extra);
                        pos += matches[0].rm_eo;
                    }
                    cbm_regfree(&re_src);
                }
            }
            cbm_regfree(&re_call);
        }
    }

    /* Java/Kotlin: PutEventsRequestEntry.builder().source("...").detailType("...") */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re_src, "\\.source\\([[:space:]]*\"([^\"]+)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            cbm_regex_t re_pe;
            if (cbm_regcomp(&re_pe, "PutEventsRequestEntry", CBM_REG_EXTENDED) == CBM_REG_OK) {
                if (cbm_regexec(&re_pe, source, 0, NULL, 0) == CBM_REG_OK) {
                    pos = source;
                    while (cbm_regexec(&re_src, pos, 2, matches, 0) == CBM_REG_OK) {
                        char src_name[256];
                        extract_match(pos, &matches[1], src_name, sizeof(src_name));

                        char dt_name[256] = "";
                        if (cbm_regcomp(&re_dt, "\\.detailType\\([[:space:]]*\"([^\"]+)\"",
                                        CBM_REG_EXTENDED) == CBM_REG_OK) {
                            if (cbm_regexec(&re_dt, source, 2, matches, 0) == CBM_REG_OK) {
                                extract_match(source, &matches[1], dt_name, sizeof(dt_name));
                            }
                            cbm_regfree(&re_dt);
                        }

                        char compound[256], extra[256];
                        build_compound_id(src_name, dt_name, compound, sizeof(compound));
                        build_extra_json(src_name, dt_name, extra, sizeof(extra));
                        add_producer(producers, prod_count, compound, node, extra);
                        pos += matches[0].rm_eo;
                    }
                }
                cbm_regfree(&re_pe);
            }
            cbm_regfree(&re_src);
        }
    }

    /* Node.js/TypeScript: new PutEventsCommand({Entries: [{Source: '...', DetailType: '...'}]}) */
    /* Also: eventBridge.putEvents({Entries: [{Source: '...', DetailType: '...'}]}) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        cbm_regex_t re_call;
        int has_call = 0;
        if (cbm_regcomp(&re_call, "PutEventsCommand", CBM_REG_EXTENDED) == CBM_REG_OK) {
            if (cbm_regexec(&re_call, source, 0, NULL, 0) == CBM_REG_OK)
                has_call = 1;
            cbm_regfree(&re_call);
        }
        if (!has_call && cbm_regcomp(&re_call, "putEvents\\(", CBM_REG_EXTENDED) == CBM_REG_OK) {
            if (cbm_regexec(&re_call, source, 0, NULL, 0) == CBM_REG_OK)
                has_call = 1;
            cbm_regfree(&re_call);
        }

        if (has_call) {
            if (cbm_regcomp(&re_src, "Source:[[:space:]]*['\"]([^'\"]+)['\"]",
                            CBM_REG_EXTENDED) == CBM_REG_OK) {
                pos = source;
                while (cbm_regexec(&re_src, pos, 2, matches, 0) == CBM_REG_OK) {
                    char src_name[256];
                    extract_match(pos, &matches[1], src_name, sizeof(src_name));

                    char dt_name[256] = "";
                    if (cbm_regcomp(&re_dt, "DetailType:[[:space:]]*['\"]([^'\"]+)['\"]",
                                    CBM_REG_EXTENDED) == CBM_REG_OK) {
                        if (cbm_regexec(&re_dt, source, 2, matches, 0) == CBM_REG_OK) {
                            extract_match(source, &matches[1], dt_name, sizeof(dt_name));
                        }
                        cbm_regfree(&re_dt);
                    }

                    char compound[256], extra[256];
                    build_compound_id(src_name, dt_name, compound, sizeof(compound));
                    build_extra_json(src_name, dt_name, extra, sizeof(extra));
                    add_producer(producers, prod_count, compound, node, extra);
                    pos += matches[0].rm_eo;
                }
                cbm_regfree(&re_src);
            }
        }
    }
}

/* ── Consumer scanning ─────────────────────────────────────────── */

/*
 * Scan source code for EventBridge consumer patterns (event rules).
 * Extracts source and detail-type from Terraform event_pattern, CDK EventPattern.
 */
static void scan_consumers(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_consumer_t *consumers, int *cons_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Terraform: aws_cloudwatch_event_rule with event_pattern containing source + detail-type */
    if (strcmp(ext, ".tf") == 0) {
        cbm_regex_t re_rule;
        if (cbm_regcomp(&re_rule, "aws_cloudwatch_event_rule", CBM_REG_EXTENDED) == CBM_REG_OK) {
            if (cbm_regexec(&re_rule, source, 0, NULL, 0) == CBM_REG_OK) {
                /* Extract "source" from event_pattern */
                if (cbm_regcomp(&re, "\"source\"[[:space:]]*[:=][[:space:]]*\\[?[[:space:]]*\"([^\"]+)\"",
                                CBM_REG_EXTENDED) == CBM_REG_OK) {
                    pos = source;
                    while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                        char src_name[256];
                        extract_match(pos, &matches[1], src_name, sizeof(src_name));

                        /* Try to find detail-type */
                        char dt_name[256] = "";
                        cbm_regex_t re_dt;
                        if (cbm_regcomp(&re_dt, "\"detail-type\"[[:space:]]*[:=][[:space:]]*\\[?[[:space:]]*\"([^\"]+)\"",
                                        CBM_REG_EXTENDED) == CBM_REG_OK) {
                            if (cbm_regexec(&re_dt, source, 2, matches, 0) == CBM_REG_OK) {
                                extract_match(source, &matches[1], dt_name, sizeof(dt_name));
                            }
                            cbm_regfree(&re_dt);
                        }

                        char compound[256], extra[256];
                        build_compound_id(src_name, dt_name, compound, sizeof(compound));
                        build_extra_json_consumer(src_name, dt_name, extra, sizeof(extra));
                        add_consumer(consumers, cons_count, compound, node, extra);
                        pos += matches[0].rm_eo;
                    }
                    cbm_regfree(&re);
                }
            }
            cbm_regfree(&re_rule);
        }
    }

    /* Python CDK: Rule(event_pattern=EventPattern(source=["X"], detail_type=["Y"])) */
    if (strcmp(ext, ".py") == 0) {
        cbm_regex_t re_ep;
        if (cbm_regcomp(&re_ep, "EventPattern\\(", CBM_REG_EXTENDED) == CBM_REG_OK) {
            if (cbm_regexec(&re_ep, source, 0, NULL, 0) == CBM_REG_OK) {
                /* Extract source from EventPattern(source=["X"]) */
                if (cbm_regcomp(&re, "source[[:space:]]*=[[:space:]]*\\[[[:space:]]*['\"]([^'\"]+)['\"]",
                                CBM_REG_EXTENDED) == CBM_REG_OK) {
                    pos = source;
                    while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                        char src_name[256];
                        extract_match(pos, &matches[1], src_name, sizeof(src_name));

                        char dt_name[256] = "";
                        cbm_regex_t re_dt;
                        if (cbm_regcomp(&re_dt, "detail_type[[:space:]]*=[[:space:]]*\\[[[:space:]]*['\"]([^'\"]+)['\"]",
                                        CBM_REG_EXTENDED) == CBM_REG_OK) {
                            if (cbm_regexec(&re_dt, source, 2, matches, 0) == CBM_REG_OK) {
                                extract_match(source, &matches[1], dt_name, sizeof(dt_name));
                            }
                            cbm_regfree(&re_dt);
                        }

                        char compound[256], extra[256];
                        build_compound_id(src_name, dt_name, compound, sizeof(compound));
                        build_extra_json_consumer(src_name, dt_name, extra, sizeof(extra));
                        add_consumer(consumers, cons_count, compound, node, extra);
                        pos += matches[0].rm_eo;
                    }
                    cbm_regfree(&re);
                }
            }
            cbm_regfree(&re_ep);
        }

        /* Python handler: event['source'] access pattern — detect Lambda consumers */
        if (cbm_regcomp(&re, "event\\[['\"]source['\"]\\]", CBM_REG_EXTENDED) == CBM_REG_OK) {
            if (cbm_regexec(&re, source, 0, NULL, 0) == CBM_REG_OK) {
                /* This is a generic consumer — we can't extract the source name
                 * without more context, so skip unless we find it paired with
                 * a string comparison */
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript CDK: new Rule({eventPattern: {source: ['X'], detailType: ['Y']}}) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        cbm_regex_t re_ep;
        if (cbm_regcomp(&re_ep, "eventPattern", CBM_REG_EXTENDED) == CBM_REG_OK) {
            if (cbm_regexec(&re_ep, source, 0, NULL, 0) == CBM_REG_OK) {
                if (cbm_regcomp(&re, "source:[[:space:]]*\\[[[:space:]]*['\"]([^'\"]+)['\"]",
                                CBM_REG_EXTENDED) == CBM_REG_OK) {
                    pos = source;
                    while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                        char src_name[256];
                        extract_match(pos, &matches[1], src_name, sizeof(src_name));

                        char dt_name[256] = "";
                        cbm_regex_t re_dt;
                        if (cbm_regcomp(&re_dt, "detailType:[[:space:]]*\\[[[:space:]]*['\"]([^'\"]+)['\"]",
                                        CBM_REG_EXTENDED) == CBM_REG_OK) {
                            if (cbm_regexec(&re_dt, source, 2, matches, 0) == CBM_REG_OK) {
                                extract_match(source, &matches[1], dt_name, sizeof(dt_name));
                            }
                            cbm_regfree(&re_dt);
                        }

                        char compound[256], extra[256];
                        build_compound_id(src_name, dt_name, compound, sizeof(compound));
                        build_extra_json_consumer(src_name, dt_name, extra, sizeof(extra));
                        add_consumer(consumers, cons_count, compound, node, extra);
                        pos += matches[0].rm_eo;
                    }
                    cbm_regfree(&re);
                }
            }
            cbm_regfree(&re_ep);
        }
    }
}

/* ── Matching logic ────────────────────────────────────────────── */

/*
 * Match consumer identifier against producer identifier.
 * Compound identifiers: "source:detail_type".
 *
 * Exact match on compound → EB_CONF_EXACT (0.95).
 * Source-only match (consumer has no detail_type, just source name) → EB_CONF_SOURCE (0.80).
 */
static double match_identifiers(const char *consumer_id, const char *producer_id) {
    /* Exact match */
    if (strcmp(consumer_id, producer_id) == 0) {
        return EB_CONF_EXACT;
    }

    /* Source-only match: consumer has no colon (source-only), producer has same source prefix */
    const char *cons_colon = strchr(consumer_id, ':');
    const char *prod_colon = strchr(producer_id, ':');

    if (!cons_colon && prod_colon) {
        /* Consumer is source-only, producer has source:detail_type */
        size_t cons_len = strlen(consumer_id);
        size_t prod_src_len = (size_t)(prod_colon - producer_id);
        if (cons_len == prod_src_len && strncmp(consumer_id, producer_id, cons_len) == 0) {
            return EB_CONF_SOURCE;
        }
    }

    if (cons_colon && !prod_colon) {
        /* Producer is source-only, consumer has source:detail_type */
        size_t prod_len = strlen(producer_id);
        size_t cons_src_len = (size_t)(cons_colon - consumer_id);
        if (prod_len == cons_src_len && strncmp(consumer_id, producer_id, prod_len) == 0) {
            return EB_CONF_SOURCE;
        }
    }

    /* Both have colons — check source part only */
    if (cons_colon && prod_colon) {
        size_t cons_src_len = (size_t)(cons_colon - consumer_id);
        size_t prod_src_len = (size_t)(prod_colon - producer_id);
        if (cons_src_len == prod_src_len &&
            strncmp(consumer_id, producer_id, cons_src_len) == 0 &&
            strcmp(cons_colon + 1, prod_colon + 1) != 0) {
            /* Same source, different detail_type — no match */
            return 0.0;
        }
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
        strcmp(ext, ".tf") == 0) {
        char *src = sl_read_node_source(ctx, node);
        if (src) {
            scan_producers(src, ext, node, producers, prod_count);
            scan_consumers(src, ext, node, consumers, cons_count);
            free(src);
        }
    }
}

/* ── Main entry point ──────────────────────────────────────────── */

int cbm_servicelink_eventbridge(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "eventbridge");

    /* 1. Allocate producer/consumer arrays on heap */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.eventbridge", "error", "alloc_failed");
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

    cbm_log_info("servicelink.eventbridge.discovery",
                 "producers", itoa_eb(prod_count),
                 "consumers", itoa_eb(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "eventbridge",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "eventbridge",
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

            double conf = match_identifiers(c->identifier, p->identifier);
            if (conf > best_conf) {
                best_conf = conf;
                best_pi = pi;
            }
        }

        if (best_pi >= 0 && best_conf >= SL_MIN_CONFIDENCE) {
            const cbm_sl_producer_t *p = &producers[best_pi];
            /* Build extra JSON with source and detail_type */
            char extra_json[256] = "";
            const char *colon = strchr(p->identifier, ':');
            if (colon) {
                char src_part[128] = "", dt_part[128] = "";
                size_t src_len = (size_t)(colon - p->identifier);
                if (src_len >= sizeof(src_part)) src_len = sizeof(src_part) - 1;
                memcpy(src_part, p->identifier, src_len);
                src_part[src_len] = '\0';
                snprintf(dt_part, sizeof(dt_part), "%s", colon + 1);
                snprintf(extra_json, sizeof(extra_json),
                         "\"source\":\"%s\",\"detail_type\":\"%s\"",
                         src_part, dt_part);
            } else {
                snprintf(extra_json, sizeof(extra_json),
                         "\"source\":\"%s\"", p->identifier);
            }

            sl_insert_edge(ctx, c->handler_id, p->source_id,
                          SL_EDGE_EVBRIDGE, c->identifier, best_conf, extra_json);
            link_count++;
        }
    }

    cbm_log_info("servicelink.eventbridge.done", "links", itoa_eb(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
