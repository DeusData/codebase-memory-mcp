/*
 * servicelink_grpc.c — gRPC protocol linker.
 *
 * Discovers gRPC producers (service definitions in .proto files and server
 * implementations) and consumers (client stubs and RPC calls), then creates
 * GRPC_CALLS edges in the graph buffer.
 *
 * Supported languages: Go, Python, Java/Kotlin, Node.js/TypeScript, Rust, C#.
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define GRPC_CONF_EXACT   0.95  /* exact service.method match */
#define GRPC_CONF_METHOD  0.55  /* method-only match */

/* ── itoa helper (thread-local rotating buffers) ────────────────── */

static const char *itoa_grpc(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Forward declarations ──────────────────────────────────────── */

static void scan_proto_definitions(const char *source, const cbm_gbuf_node_t *node,
                                   cbm_sl_producer_t *producers, int *prod_count);
static void scan_server_impls(const char *source, const char *ext,
                              const cbm_gbuf_node_t *node,
                              cbm_sl_producer_t *producers, int *prod_count);
static void scan_client_calls(const char *source, const char *ext,
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

/* ── .proto file scanning ──────────────────────────────────────── */

/*
 * Parse .proto source for service + rpc definitions.
 * Produces identifiers like "ServiceName.MethodName".
 *
 * Grammar (simplified):
 *   service <Name> { ... rpc <Method>( ... }
 */
static void scan_proto_definitions(const char *source, const cbm_gbuf_node_t *node,
                                   cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re_service;
    if (cbm_regcomp(&re_service, "service[ \t]+([A-Za-z_][A-Za-z0-9_]*)",
                    CBM_REG_EXTENDED) != CBM_REG_OK) {
        return;
    }

    cbm_regex_t re_rpc;
    if (cbm_regcomp(&re_rpc, "rpc[ \t]+([A-Za-z_][A-Za-z0-9_]*)",
                    CBM_REG_EXTENDED) != CBM_REG_OK) {
        cbm_regfree(&re_service);
        return;
    }

    const char *pos = source;
    cbm_regmatch_t svc_matches[2];

    while (cbm_regexec(&re_service, pos, 2, svc_matches, 0) == CBM_REG_OK) {
        char service_name[128];
        extract_match(pos, &svc_matches[1], service_name, sizeof(service_name));

        /* Find the opening brace of the service block */
        const char *svc_start = pos + svc_matches[0].rm_eo;
        const char *brace = strchr(svc_start, '{');
        if (!brace) break;

        /* Find the matching closing brace (simple nesting) */
        int depth = 1;
        const char *scan = brace + 1;
        const char *block_end = NULL;
        while (*scan && depth > 0) {
            if (*scan == '{') depth++;
            else if (*scan == '}') {
                depth--;
                if (depth == 0) { block_end = scan; break; }
            }
            scan++;
        }
        if (!block_end) block_end = scan;

        /* Scan for rpc definitions within the service block */
        size_t block_len = (size_t)(block_end - (brace + 1));
        char *block = malloc(block_len + 1);
        if (block) {
            memcpy(block, brace + 1, block_len);
            block[block_len] = '\0';

            const char *rpc_pos = block;
            cbm_regmatch_t rpc_matches[2];
            while (cbm_regexec(&re_rpc, rpc_pos, 2, rpc_matches, 0) == CBM_REG_OK) {
                char method_name[128];
                extract_match(rpc_pos, &rpc_matches[1], method_name, sizeof(method_name));

                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.%s", service_name, method_name);
                add_producer(producers, prod_count, identifier, node, "proto_def");

                rpc_pos += rpc_matches[0].rm_eo;
            }
            free(block);
        }

        pos += svc_matches[0].rm_eo;
    }

    cbm_regfree(&re_service);
    cbm_regfree(&re_rpc);
}

/* ── Server implementation scanning ────────────────────────────── */

static void scan_server_impls(const char *source, const char *ext,
                              const cbm_gbuf_node_t *node,
                              cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[5];
    const char *pos;

    /* Go: pb.RegisterXxxServer() or RegisterXxxServer() */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "Register([A-Za-z_][A-Za-z0-9_]*)Server\\(",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char svc[128];
                extract_match(pos, &matches[1], svc, sizeof(svc));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_producer(producers, prod_count, identifier, node, "go_server");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Python: class XxxServicer */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "class[ \t]+([A-Za-z_][A-Za-z0-9_]*)Servicer",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char svc[128];
                extract_match(pos, &matches[1], svc, sizeof(svc));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_producer(producers, prod_count, identifier, node, "py_servicer");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java/Kotlin: extends XxxGrpc.XxxImplBase */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "extends[ \t]+([A-Za-z_][A-Za-z0-9_]*)Grpc\\.([A-Za-z_][A-Za-z0-9_]*)ImplBase",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
                char svc[128];
                extract_match(pos, &matches[1], svc, sizeof(svc));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_producer(producers, prod_count, identifier, node, "java_server");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* @GrpcService annotation on a class */
        if (cbm_regcomp(&re, "@GrpcService",
                        CBM_REG_EXTENDED | CBM_REG_NOSUB) == CBM_REG_OK) {
            if (cbm_regexec(&re, source, 0, NULL, 0) == CBM_REG_OK) {
                /* Try to extract the class name that follows */
                cbm_regex_t re_cls;
                if (cbm_regcomp(&re_cls, "class[ \t]+([A-Za-z_][A-Za-z0-9_]*)",
                                CBM_REG_EXTENDED) == CBM_REG_OK) {
                    cbm_regmatch_t cls_m[2];
                    if (cbm_regexec(&re_cls, source, 2, cls_m, 0) == CBM_REG_OK) {
                        char cls[128];
                        extract_match(source, &cls_m[1], cls, sizeof(cls));
                        char identifier[256];
                        snprintf(identifier, sizeof(identifier), "%s.*", cls);
                        add_producer(producers, prod_count, identifier, node, "java_grpc_service");
                    }
                    cbm_regfree(&re_cls);
                }
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: server.addService(XxxService, ...) */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "server\\.addService\\([ \t]*([A-Za-z_][A-Za-z0-9_.]*)",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char svc_raw[128];
                extract_match(pos, &matches[1], svc_raw, sizeof(svc_raw));
                /* Strip trailing .service or _service suffix */
                char *dot = strrchr(svc_raw, '.');
                char svc[128];
                if (dot) {
                    size_t prefix_len = (size_t)(dot - svc_raw);
                    if (prefix_len >= sizeof(svc)) prefix_len = sizeof(svc) - 1;
                    memcpy(svc, svc_raw, prefix_len);
                    svc[prefix_len] = '\0';
                } else {
                    snprintf(svc, sizeof(svc), "%s", svc_raw);
                }
                /* Strip trailing "Service" suffix to match client naming */
                size_t slen = strlen(svc);
                if (slen > 7 && strcmp(svc + slen - 7, "Service") == 0) {
                    svc[slen - 7] = '\0';
                }
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_producer(producers, prod_count, identifier, node, "node_server");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Rust: impl XxxService for ... (tonic pattern) */
    if (strcmp(ext, ".rs") == 0) {
        if (cbm_regcomp(&re, "impl[ \t]+([A-Za-z_][A-Za-z0-9_]*)[ \t]+for",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char svc[128];
                extract_match(pos, &matches[1], svc, sizeof(svc));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_producer(producers, prod_count, identifier, node, "rust_server");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* C#: class XxxService : XxxGrpc.XxxBase */
    if (strcmp(ext, ".cs") == 0) {
        if (cbm_regcomp(&re, "class[ \t]+([A-Za-z_][A-Za-z0-9_]*)[ \t]*:[ \t]*([A-Za-z_][A-Za-z0-9_]*)\\.([A-Za-z_][A-Za-z0-9_]*)Base",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 4, matches, 0) == CBM_REG_OK) {
                char svc[128];
                extract_match(pos, &matches[2], svc, sizeof(svc));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_producer(producers, prod_count, identifier, node, "cs_server");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── Client call scanning ──────────────────────────────────────── */

static void scan_client_calls(const char *source, const char *ext,
                              const cbm_gbuf_node_t *node,
                              cbm_sl_consumer_t *consumers, int *cons_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* Go: pb.NewXxxClient(conn) → creates a client for service Xxx */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "New([A-Za-z_][A-Za-z0-9_]*)Client\\(",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char svc[128];
                extract_match(pos, &matches[1], svc, sizeof(svc));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_consumer(consumers, cons_count, identifier, node, "go_client");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Go: client.MethodName() — look for direct method calls on a grpc client */
        if (cbm_regcomp(&re, "client\\.([A-Z][A-Za-z0-9_]*)\\(",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char method[128];
                extract_match(pos, &matches[1], method, sizeof(method));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "*.%s", method);
                add_consumer(consumers, cons_count, identifier, node, "go_method_call");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Python: XxxStub(channel) */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "([A-Za-z_][A-Za-z0-9_]*)Stub\\(",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char svc[128];
                extract_match(pos, &matches[1], svc, sizeof(svc));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_consumer(consumers, cons_count, identifier, node, "py_stub");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Python: stub.MethodName() */
        if (cbm_regcomp(&re, "stub\\.([A-Z][A-Za-z0-9_]*)\\(",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char method[128];
                extract_match(pos, &matches[1], method, sizeof(method));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "*.%s", method);
                add_consumer(consumers, cons_count, identifier, node, "py_method_call");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Java/Kotlin: XxxGrpc.newBlockingStub() or newStub() */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "([A-Za-z_][A-Za-z0-9_]*)Grpc\\.new[A-Za-z]*Stub\\(",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char svc[128];
                extract_match(pos, &matches[1], svc, sizeof(svc));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_consumer(consumers, cons_count, identifier, node, "java_stub");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Node.js/TypeScript: new XxxClient() or grpc client patterns */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "new[ \t]+([A-Za-z_][A-Za-z0-9_]*)Client\\(",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char svc[128];
                extract_match(pos, &matches[1], svc, sizeof(svc));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_consumer(consumers, cons_count, identifier, node, "node_client");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* Rust: XxxClient::new() or XxxClient::connect() */
    if (strcmp(ext, ".rs") == 0) {
        if (cbm_regcomp(&re, "([A-Za-z_][A-Za-z0-9_]*)Client::(new|connect)\\(",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char svc[128];
                extract_match(pos, &matches[1], svc, sizeof(svc));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_consumer(consumers, cons_count, identifier, node, "rust_client");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* C#: new XxxService.XxxServiceClient() or XxxClient() */
    if (strcmp(ext, ".cs") == 0) {
        if (cbm_regcomp(&re, "new[ \t]+([A-Za-z_][A-Za-z0-9_]*)\\.([A-Za-z_][A-Za-z0-9_]*)Client\\(",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
                char svc[128];
                extract_match(pos, &matches[1], svc, sizeof(svc));
                char identifier[256];
                snprintf(identifier, sizeof(identifier), "%s.*", svc);
                add_consumer(consumers, cons_count, identifier, node, "cs_client");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }
}

/* ── Matching ──────────────────────────────────────────────────── */

/*
 * Match a consumer identifier against a producer identifier.
 * Returns confidence (0.0 = no match, 0.95 = exact, 0.55 = method-only).
 *
 * Identifier formats:
 *   "ServiceName.MethodName"  — fully qualified rpc
 *   "ServiceName.*"           — service-level wildcard (client or server)
 *   "*.MethodName"            — method-only (from client.Method() calls)
 */
static double match_identifiers(const char *consumer_id, const char *producer_id) {
    /* Parse consumer */
    const char *c_dot = strchr(consumer_id, '.');
    if (!c_dot) return 0.0;

    char c_svc[128] = {0};
    char c_method[128] = {0};
    size_t c_svc_len = (size_t)(c_dot - consumer_id);
    if (c_svc_len >= sizeof(c_svc)) c_svc_len = sizeof(c_svc) - 1;
    memcpy(c_svc, consumer_id, c_svc_len);
    snprintf(c_method, sizeof(c_method), "%s", c_dot + 1);

    /* Parse producer */
    const char *p_dot = strchr(producer_id, '.');
    if (!p_dot) return 0.0;

    char p_svc[128] = {0};
    char p_method[128] = {0};
    size_t p_svc_len = (size_t)(p_dot - producer_id);
    if (p_svc_len >= sizeof(p_svc)) p_svc_len = sizeof(p_svc) - 1;
    memcpy(p_svc, producer_id, p_svc_len);
    snprintf(p_method, sizeof(p_method), "%s", p_dot + 1);

    bool c_svc_wild = (strcmp(c_svc, "*") == 0);
    bool p_svc_wild = (strcmp(p_svc, "*") == 0);
    bool c_method_wild = (strcmp(c_method, "*") == 0);
    bool p_method_wild = (strcmp(p_method, "*") == 0);

    /* Both have concrete service names */
    bool svc_match = (c_svc_wild || p_svc_wild || strcmp(c_svc, p_svc) == 0);
    bool method_match = (c_method_wild || p_method_wild || strcmp(c_method, p_method) == 0);

    if (!svc_match) return 0.0;

    /* Exact service + method match (neither is wildcard) */
    if (method_match && !c_svc_wild && !p_svc_wild &&
        !c_method_wild && !p_method_wild) {
        return GRPC_CONF_EXACT;
    }

    /* Service matches, method is wildcard on one or both sides */
    if (svc_match && !c_svc_wild && !p_svc_wild) {
        /* Both service names are concrete and match — good match even with wildcard method */
        return GRPC_CONF_EXACT;
    }

    /* Method-only match (service is wildcard on consumer side, e.g. "*.GetOrder") */
    if (c_svc_wild && !p_svc_wild && method_match && !p_method_wild) {
        return GRPC_CONF_METHOD;
    }

    /* Service-wildcard consumer matching service-wildcard producer — skip to avoid noise */
    if (c_svc_wild && p_svc_wild) return 0.0;

    /* Service matches (at least one wildcard), method matches */
    if (svc_match && method_match) {
        return GRPC_CONF_METHOD;
    }

    return 0.0;
}

/* ── Process a single node ─────────────────────────────────────── */

static void process_node(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *node,
                         cbm_sl_producer_t *producers, int *prod_count,
                         cbm_sl_consumer_t *consumers, int *cons_count) {
    if (!node->file_path) return;

    const char *ext = sl_file_ext(node->file_path);

    /* .proto files: scan for service/rpc definitions */
    if (strcmp(ext, ".proto") == 0) {
        char *source = sl_read_node_source(ctx, node);
        if (source) {
            scan_proto_definitions(source, node, producers, prod_count);
            free(source);
        }
        return;
    }

    /* Source files: scan for server impls and client calls */
    if (strcmp(ext, ".go") == 0 || strcmp(ext, ".py") == 0 ||
        strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0 ||
        strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0 ||
        strcmp(ext, ".rs") == 0 || strcmp(ext, ".cs") == 0) {
        char *source = sl_read_node_source(ctx, node);
        if (source) {
            scan_server_impls(source, ext, node, producers, prod_count);
            scan_client_calls(source, ext, node, consumers, cons_count);
            free(source);
        }
    }
}

/* ── Main entry point ──────────────────────────────────────────── */

int cbm_servicelink_grpc(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "grpc");

    /* 1. Allocate producer/consumer arrays on heap */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.grpc", "error", "alloc_failed");
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

    cbm_log_info("servicelink.grpc.discovery",
                 "producers", itoa_grpc(prod_count),
                 "consumers", itoa_grpc(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "grpc",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "grpc",
                                 "consumer", consumers[i].identifier,
                                 consumers[i].handler_qn, consumers[i].file_path,
                                 consumers[i].extra);
        }
    }

    /* 4. Match consumers to producers and create edges.
     *    Collect best matches, dedup by (src, tgt) keeping highest confidence
     *    to prevent lower-confidence overwrites via gbuf dedup. */
    int link_count = 0;

    typedef struct { int64_t src; int64_t tgt; int ci; int pi; double conf; } grpc_match_t;
    grpc_match_t *grpc_matches = calloc((size_t)(cons_count > 0 ? cons_count : 1),
                                        sizeof(grpc_match_t));
    int match_count = 0;

    if (!grpc_matches) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.grpc", "error", "match_alloc_failed");
        return -1;
    }

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
            /* Check if this (src, tgt) pair already has a match */
            int existing = -1;
            for (int m = 0; m < match_count; m++) {
                if (grpc_matches[m].src == c->handler_id &&
                    grpc_matches[m].tgt == producers[best_pi].source_id) {
                    existing = m;
                    break;
                }
            }
            if (existing >= 0) {
                /* Keep higher confidence */
                if (best_conf > grpc_matches[existing].conf) {
                    grpc_matches[existing].ci = ci;
                    grpc_matches[existing].pi = best_pi;
                    grpc_matches[existing].conf = best_conf;
                }
            } else {
                grpc_matches[match_count].src = c->handler_id;
                grpc_matches[match_count].tgt = producers[best_pi].source_id;
                grpc_matches[match_count].ci = ci;
                grpc_matches[match_count].pi = best_pi;
                grpc_matches[match_count].conf = best_conf;
                match_count++;
            }
        }
    }

    /* Insert deduped edges */
    for (int m = 0; m < match_count; m++) {
        const cbm_sl_consumer_t *c = &consumers[grpc_matches[m].ci];
        sl_insert_edge(ctx, grpc_matches[m].src, grpc_matches[m].tgt,
                      SL_EDGE_GRPC, c->identifier, grpc_matches[m].conf, NULL);
        link_count++;
    }

    free(grpc_matches);

    cbm_log_info("servicelink.grpc.done", "links", itoa_grpc(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
