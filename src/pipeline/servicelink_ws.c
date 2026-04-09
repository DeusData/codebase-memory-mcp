/*
 * servicelink_ws.c — WebSocket protocol linker.
 *
 * Discovers WebSocket endpoints (server-side upgrade handlers, decorators) and
 * clients (new WebSocket("ws://...") / dial calls), then creates WS_CALLS
 * edges in the graph buffer.
 *
 * Supported languages: Go, Python, Java/Kotlin, Node.js/TypeScript, Rust.
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define WS_CONF_EXACT  0.95  /* exact path match */

/* ── itoa helper (thread-local rotating buffers) ────────────────── */

static const char *itoa_ws(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Forward declarations ──────────────────────────────────────── */

static void scan_endpoints(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_producer_t *producers, int *prod_count);
static void scan_clients(const char *source, const char *ext,
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

/*
 * Extract the path component from a WebSocket URL.
 * Given "ws://host:port/path/to/endpoint" or "wss://host/path",
 * returns pointer to the first '/' after the host, or "/" if none found.
 * Writes into caller-supplied buffer.
 */
static void extract_ws_url_path(const char *url, char *out, size_t outsz) {
    /* Skip scheme: ws:// or wss:// */
    const char *p = strstr(url, "://");
    if (!p) {
        snprintf(out, outsz, "/");
        return;
    }
    p += 3; /* past "://" */

    /* Find first '/' after the host */
    const char *slash = strchr(p, '/');
    if (slash && slash[0]) {
        /* Strip any trailing quote or whitespace */
        size_t len = strlen(slash);
        while (len > 1 && (slash[len - 1] == '"' || slash[len - 1] == '\''
                           || slash[len - 1] == ')' || slash[len - 1] == ' ')) {
            len--;
        }
        if (len >= outsz) len = outsz - 1;
        memcpy(out, slash, len);
        out[len] = '\0';
    } else {
        snprintf(out, outsz, "/");
    }
}

/* ── Endpoint (producer) scanning ──────────────────────────────── */

/*
 * Scan source for WebSocket endpoint patterns.
 * The identifier is the path (e.g. "/ws", "/chat").
 */
static void scan_endpoints(const char *source, const char *ext,
                           const cbm_gbuf_node_t *node,
                           cbm_sl_producer_t *producers, int *prod_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[4];
    const char *pos;

    /* ── Go: r.HandleFunc("/path", ...) in files with websocket context ── */
    /* Note: websocket.Upgrader may be outside the node's line range (e.g. at
     * package level), so we also check the file path for WS-related names.
     * False-positive endpoints are harmless — edges only form if a WS client matches. */
    if (strcmp(ext, ".go") == 0) {
        bool has_ws = (strstr(source, "websocket") != NULL ||
                       strstr(source, "Upgrader") != NULL ||
                       strstr(source, "HandleFunc") != NULL);
        if (has_ws) {
            if (cbm_regcomp(&re, "HandleFunc\\([ \t]*\"(/[^\"]*)\"",
                            CBM_REG_EXTENDED) == CBM_REG_OK) {
                pos = source;
                while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                    char path[256];
                    extract_match(pos, &matches[1], path, sizeof(path));
                    add_producer(producers, prod_count, path, node, "go_ws_endpoint");
                    pos += matches[0].rm_eo;
                }
                cbm_regfree(&re);
            }
        }
    }

    /* ── Python: @app.websocket("/path") ── */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "@[a-zA-Z_]+\\.websocket\\([ \t]*['\"](/[^'\"]*)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char path[256];
                extract_match(pos, &matches[1], path, sizeof(path));
                add_producer(producers, prod_count, path, node, "py_ws_endpoint");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* @sockets.on("message") or @socketio.on("message") */
        if (cbm_regcomp(&re, "@(sockets|socketio)\\.on\\([ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
                char event[256];
                extract_match(pos, &matches[2], event, sizeof(event));
                /* Use /socketio/<event> as identifier */
                char ident[256];
                snprintf(ident, sizeof(ident), "/socketio/%s", event);
                add_producer(producers, prod_count, ident, node, "py_socketio_endpoint");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* ── Java/Kotlin: @ServerEndpoint("/path") ── */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        if (cbm_regcomp(&re, "@ServerEndpoint\\([ \t]*\"(/[^\"]*)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char path[256];
                extract_match(pos, &matches[1], path, sizeof(path));
                add_producer(producers, prod_count, path, node, "java_ws_endpoint");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* Spring @MessageMapping("/path") */
        if (cbm_regcomp(&re, "@MessageMapping\\([ \t]*\"(/[^\"]*)\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char path[256];
                extract_match(pos, &matches[1], path, sizeof(path));
                add_producer(producers, prod_count, path, node, "java_message_mapping");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* ── Node.js/TypeScript: app.ws("/path", ...) ── */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "\\.ws\\([ \t]*['\"](/[^'\"]*)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                char path[256];
                extract_match(pos, &matches[1], path, sizeof(path));
                add_producer(producers, prod_count, path, node, "node_ws_endpoint");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* io.on("connection") — Socket.IO server */
        if (strstr(source, "io.on(") != NULL &&
            (strstr(source, "\"connection\"") != NULL || strstr(source, "'connection'") != NULL)) {
            /* Look for a path in the Socket.IO config, or use /socket.io default */
            if (cbm_regcomp(&re, "new[ \t]+Server\\([ \t]*[^)]*path:[ \t]*['\"]([^'\"]+)['\"]",
                            CBM_REG_EXTENDED) == CBM_REG_OK) {
                pos = source;
                if (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                    char path[256];
                    extract_match(pos, &matches[1], path, sizeof(path));
                    add_producer(producers, prod_count, path, node, "node_socketio_endpoint");
                } else {
                    add_producer(producers, prod_count, "/socket.io", node, "node_socketio_endpoint");
                }
                cbm_regfree(&re);
            } else {
                add_producer(producers, prod_count, "/socket.io", node, "node_socketio_endpoint");
            }
        }

        /* new WebSocketServer({...path: "/path"}) or new WebSocket.Server({...path: "/path"}) */
        if (cbm_regcomp(&re, "new[ \t]+(WebSocketServer|WebSocket\\.Server)\\([ \t]*\\{[^}]*path:[ \t]*['\"]([^'\"]+)['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 3, matches, 0) == CBM_REG_OK) {
                char path[256];
                extract_match(pos, &matches[2], path, sizeof(path));
                add_producer(producers, prod_count, path, node, "node_wss_endpoint");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* wss.on("connection") without explicit path — use "/" as fallback */
        if (strstr(source, "wss.on(") != NULL &&
            (strstr(source, "\"connection\"") != NULL || strstr(source, "'connection'") != NULL)) {
            /* Only add if we haven't already found a WebSocketServer path */
            if (!strstr(source, "WebSocketServer") && !strstr(source, "WebSocket.Server")) {
                add_producer(producers, prod_count, "/", node, "node_wss_endpoint");
            }
        }
    }

    /* ── Rust: .route("/ws", get(ws_handler)) with axum/actix websocket ── */
    if (strcmp(ext, ".rs") == 0) {
        bool has_ws = (strstr(source, "WebSocket") != NULL ||
                       strstr(source, "ws::") != NULL);
        if (has_ws) {
            if (cbm_regcomp(&re, "\\.route\\([ \t]*\"(/[^\"]*)\"",
                            CBM_REG_EXTENDED) == CBM_REG_OK) {
                pos = source;
                while (cbm_regexec(&re, pos, 2, matches, 0) == CBM_REG_OK) {
                    char path[256];
                    extract_match(pos, &matches[1], path, sizeof(path));
                    add_producer(producers, prod_count, path, node, "rust_ws_endpoint");
                    pos += matches[0].rm_eo;
                }
                cbm_regfree(&re);
            }
        }
    }
}

/* ── Client (consumer) scanning ────────────────────────────────── */

/*
 * Scan source for WebSocket client patterns.
 * The identifier is the path extracted from the URL.
 */
static void scan_clients(const char *source, const char *ext,
                         const cbm_gbuf_node_t *node,
                         cbm_sl_consumer_t *consumers, int *cons_count) {
    cbm_regex_t re;
    cbm_regmatch_t matches[3];
    const char *pos;

    /* ── JavaScript/TypeScript: new WebSocket("ws://..." or "wss://...") ── */
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0) {
        if (cbm_regcomp(&re, "new[ \t]+WebSocket\\([ \t]*['\"]wss?://[^'\"]+['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 1, matches, 0) == CBM_REG_OK) {
                /* Extract the full URL from the match */
                char full_match[512];
                extract_match(pos, &matches[0], full_match, sizeof(full_match));

                /* Find the URL inside quotes */
                char *q1 = strchr(full_match, '\'');
                char *q2 = strchr(full_match, '"');
                char *url_start = NULL;
                char quote_char = 0;
                if (q1 && (!q2 || q1 < q2)) { url_start = q1 + 1; quote_char = '\''; }
                else if (q2) { url_start = q2 + 1; quote_char = '"'; }

                if (url_start) {
                    char *url_end = strchr(url_start, quote_char);
                    if (url_end) *url_end = '\0';

                    char path[256];
                    extract_ws_url_path(url_start, path, sizeof(path));
                    add_consumer(consumers, cons_count, path, node, "js_ws_client");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }

        /* io("ws://...") or io("wss://...") — Socket.IO client */
        if (cbm_regcomp(&re, "io\\([ \t]*['\"]wss?://[^'\"]*['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 1, matches, 0) == CBM_REG_OK) {
                add_consumer(consumers, cons_count, "/socket.io", node, "js_socketio_client");
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* ── Go: websocket.Dial("ws://...") or websocket.DefaultDialer.Dial("ws://...") ── */
    if (strcmp(ext, ".go") == 0) {
        if (cbm_regcomp(&re, "websocket\\.(DefaultDialer\\.)?Dial[a-zA-Z]*\\([ \t]*\"wss?://[^\"]*\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 1, matches, 0) == CBM_REG_OK) {
                char full_match[512];
                extract_match(pos, &matches[0], full_match, sizeof(full_match));

                char *q = strchr(full_match, '"');
                if (q) {
                    char *url_start = q + 1;
                    char *url_end = strchr(url_start, '"');
                    if (url_end) *url_end = '\0';

                    char path[256];
                    extract_ws_url_path(url_start, path, sizeof(path));
                    add_consumer(consumers, cons_count, path, node, "go_ws_client");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* ── Python: websockets.connect("ws://...") or WebSocketApp("ws://...") ── */
    if (strcmp(ext, ".py") == 0) {
        if (cbm_regcomp(&re, "(websockets\\.connect|WebSocketApp)\\([ \t]*['\"]wss?://[^'\"]*['\"]",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 1, matches, 0) == CBM_REG_OK) {
                char full_match[512];
                extract_match(pos, &matches[0], full_match, sizeof(full_match));

                /* Find URL in quotes */
                char *q1 = strchr(full_match, '\'');
                char *q2 = strchr(full_match, '"');
                char *url_start = NULL;
                char quote_char = 0;
                if (q1 && (!q2 || q1 < q2)) { url_start = q1 + 1; quote_char = '\''; }
                else if (q2) { url_start = q2 + 1; quote_char = '"'; }

                if (url_start) {
                    char *url_end = strchr(url_start, quote_char);
                    if (url_end) *url_end = '\0';

                    char path[256];
                    extract_ws_url_path(url_start, path, sizeof(path));
                    add_consumer(consumers, cons_count, path, node, "py_ws_client");
                }
                pos += matches[0].rm_eo;
            }
            cbm_regfree(&re);
        }
    }

    /* ── Java/Kotlin: new URI("ws://...") near WebSocket usage ── */
    if (strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0) {
        bool has_ws = (strstr(source, "WebSocket") != NULL ||
                       strstr(source, "websocket") != NULL ||
                       strstr(source, "stomp") != NULL);
        if (has_ws) {
            if (cbm_regcomp(&re, "new[ \t]+URI\\([ \t]*\"wss?://[^\"]*\"",
                            CBM_REG_EXTENDED) == CBM_REG_OK) {
                pos = source;
                while (cbm_regexec(&re, pos, 1, matches, 0) == CBM_REG_OK) {
                    char full_match[512];
                    extract_match(pos, &matches[0], full_match, sizeof(full_match));

                    char *q = strchr(full_match, '"');
                    if (q) {
                        char *url_start = q + 1;
                        char *url_end = strchr(url_start, '"');
                        if (url_end) *url_end = '\0';

                        char path[256];
                        extract_ws_url_path(url_start, path, sizeof(path));
                        add_consumer(consumers, cons_count, path, node, "java_ws_client");
                    }
                    pos += matches[0].rm_eo;
                }
                cbm_regfree(&re);
            }
        }
    }

    /* ── Rust: connect("ws://...") or connect_async("ws://...") ── */
    if (strcmp(ext, ".rs") == 0) {
        if (cbm_regcomp(&re, "connect(_async)?\\([ \t]*\"wss?://[^\"]*\"",
                        CBM_REG_EXTENDED) == CBM_REG_OK) {
            pos = source;
            while (cbm_regexec(&re, pos, 1, matches, 0) == CBM_REG_OK) {
                char full_match[512];
                extract_match(pos, &matches[0], full_match, sizeof(full_match));

                char *q = strchr(full_match, '"');
                if (q) {
                    char *url_start = q + 1;
                    char *url_end = strchr(url_start, '"');
                    if (url_end) *url_end = '\0';

                    char path[256];
                    extract_ws_url_path(url_start, path, sizeof(path));
                    add_consumer(consumers, cons_count, path, node, "rust_ws_client");
                }
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

    /* Source files: scan for endpoint and client patterns */
    if (strcmp(ext, ".go") == 0 || strcmp(ext, ".py") == 0 ||
        strcmp(ext, ".java") == 0 || strcmp(ext, ".kt") == 0 ||
        strcmp(ext, ".js") == 0 || strcmp(ext, ".ts") == 0 ||
        strcmp(ext, ".rs") == 0) {
        char *source = sl_read_node_source(ctx, node);
        if (source) {
            scan_endpoints(source, ext, node, producers, prod_count);
            scan_clients(source, ext, node, consumers, cons_count);
            free(source);
        }
    }
}

/* ── Main entry point ──────────────────────────────────────────── */

int cbm_servicelink_ws(cbm_pipeline_ctx_t *ctx) {
    cbm_log_info("servicelink.start", "protocol", "ws");

    /* 1. Allocate producer/consumer arrays on heap */
    cbm_sl_producer_t *producers = calloc(SL_MAX_PRODUCERS, sizeof(cbm_sl_producer_t));
    cbm_sl_consumer_t *consumers = calloc(SL_MAX_CONSUMERS, sizeof(cbm_sl_consumer_t));
    if (!producers || !consumers) {
        free(producers);
        free(consumers);
        cbm_log_error("servicelink.ws", "error", "alloc_failed");
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

    cbm_log_info("servicelink.ws.discovery",
                 "producers", itoa_ws(prod_count),
                 "consumers", itoa_ws(cons_count));

    /* Register endpoints for cross-repo matching */
    if (ctx->endpoints) {
        for (int i = 0; i < prod_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "ws",
                                 "producer", producers[i].identifier,
                                 producers[i].source_qn, producers[i].file_path,
                                 producers[i].extra);
        }
        for (int i = 0; i < cons_count; i++) {
            sl_register_endpoint(ctx->endpoints, ctx->project_name, "ws",
                                 "consumer", consumers[i].identifier,
                                 consumers[i].handler_qn, consumers[i].file_path,
                                 consumers[i].extra);
        }
    }

    /* 4. Match consumers to producers using path matching and create edges */
    int link_count = 0;

    for (int ci = 0; ci < cons_count; ci++) {
        const cbm_sl_consumer_t *c = &consumers[ci];
        double best_conf = 0.0;
        int best_pi = -1;

        for (int pi = 0; pi < prod_count; pi++) {
            const cbm_sl_producer_t *p = &producers[pi];

            /* Skip self-links (same node) */
            if (c->handler_id == p->source_id) continue;

            /* Exact identifier match → high confidence; fuzzy → path score */
            double conf;
            if (strcmp(c->identifier, p->identifier) == 0) {
                conf = WS_CONF_EXACT;
            } else {
                conf = cbm_path_match_score(c->identifier, p->identifier);
            }
            if (conf > best_conf) {
                best_conf = conf;
                best_pi = pi;
            }
        }

        if (best_pi >= 0 && best_conf >= SL_MIN_CONFIDENCE) {
            const cbm_sl_producer_t *p = &producers[best_pi];
            sl_insert_edge(ctx, c->handler_id, p->source_id,
                          SL_EDGE_WS, c->identifier, best_conf, NULL);
            link_count++;
        }
    }

    cbm_log_info("servicelink.ws.done", "links", itoa_ws(link_count));

    free(producers);
    free(consumers);
    return link_count;
}
