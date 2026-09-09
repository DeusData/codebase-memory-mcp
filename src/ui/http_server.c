/*
 * http_server.c — Authenticated, read-only diagnostic dashboard.
 *
 * Transport (sockets, parsing, limits) lives in httpd.c; this file owns
 * the routes and their handlers:
 *   GET /             → embedded index.html
 *   GET /assets/...   → embedded JS/CSS
 *   POST /rpc         → allow-listed read-only JSON-RPC dispatch
 *   OPTIONS /rpc      → CORS preflight (for vite dev on :5173)
 *   GET /api/...      → authenticated diagnostic endpoints
 *   *                 → 404
 *
 * Runs in a background pthread. Binds to 127.0.0.1 only (see httpd.c).
 * Has its own cbm_mcp_server_t with a separate SQLite connection (WAL reader).
 * Every /api and /rpc request requires a 256-bit per-process capability token.
 */
#ifdef _WIN32
#define _CRT_RAND_S
#endif

#include "ui/http_server.h"
#include "ui/httpd.h"
#include "ui/embedded_assets.h"
#include "mcp/mcp.h"
#include "store/store.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/str_util.h"
#include "foundation/compat_thread.h"

#include <yyjson/yyjson.h>

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Constants ────────────────────────────────────────────────── */

/* Max JSON-RPC request body size (1 MB) — transport enforces the same cap. */
#define MAX_BODY_SIZE CBM_HTTP_MAX_BODY
#define DASHBOARD_TOKEN_BYTES 32
#define DASHBOARD_TOKEN_HEX_LEN (DASHBOARD_TOKEN_BYTES * 2)

/* ── CORS: only allow localhost origins (blocks remote website attacks) ────── */

/* Per-request CORS header buffers. Updated at the start of each dispatch.
 * The server handles requests sequentially on one thread (see httpd.h),
 * which makes these statics safe. */
static char g_cors[256];      /* CORS headers only */
static char g_cors_json[768]; /* CORS + JSON security headers */

/* Inspect the Origin header and only reflect it if it's a localhost URL.
 * This prevents remote websites from making cross-origin requests to the
 * local graph-ui server (the key defense against CORS-based data exfil). */
static void update_cors(const cbm_http_req_t *req) {
    if (req->origin[0] != '\0' && (cbm_http_path_match(req->origin, "http://localhost:*") ||
                                   cbm_http_path_match(req->origin, "http://127.0.0.1:*"))) {
        snprintf(g_cors, sizeof(g_cors),
                 "Access-Control-Allow-Origin: %s\r\n"
                 "Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n"
                 "Access-Control-Allow-Headers: Content-Type\r\n",
                 req->origin);
    } else {
        /* No Access-Control-Allow-Origin → browser blocks cross-origin access */
        snprintf(g_cors, sizeof(g_cors),
                 "Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n"
                 "Access-Control-Allow-Headers: Content-Type\r\n");
    }
    snprintf(g_cors_json, sizeof(g_cors_json),
             "%sContent-Type: application/json\r\n"
             "Cache-Control: no-store\r\n"
             "Referrer-Policy: no-referrer\r\n"
             "X-Content-Type-Options: nosniff\r\n",
             g_cors);
}

/* ── Server state ─────────────────────────────────────────────── */

struct cbm_http_server {
    cbm_httpd_t *listener;
    cbm_mcp_server_t *mcp; /* own MCP server instance (read-only) */
    atomic_int stop_flag;
    int port;
    bool listener_ok;
    char token[DASHBOARD_TOKEN_HEX_LEN + 1];
};

/* ── Capability authentication ───────────────────────────────── */

static bool secure_random_bytes(unsigned char *out, size_t len) {
    if (!out || len == 0)
        return false;
#ifdef _WIN32
    size_t off = 0;
    while (off < len) {
        unsigned int value = 0;
        if (rand_s(&value) != 0)
            return false;
        size_t take = len - off;
        if (take > sizeof(value))
            take = sizeof(value);
        memcpy(out + off, &value, take);
        off += take;
    }
    return true;
#else
    FILE *f = fopen("/dev/urandom", "rb");
    if (!f)
        return false;
    size_t got = fread(out, 1, len, f);
    bool ok = got == len && ferror(f) == 0;
    fclose(f);
    return ok;
#endif
}

static bool generate_dashboard_token(char token[DASHBOARD_TOKEN_HEX_LEN + 1]) {
    static const char hex[] = "0123456789abcdef";
    unsigned char entropy[DASHBOARD_TOKEN_BYTES];
    if (!secure_random_bytes(entropy, sizeof(entropy)))
        return false;
    for (size_t i = 0; i < sizeof(entropy); i++) {
        token[i * 2] = hex[entropy[i] >> 4];
        token[i * 2 + 1] = hex[entropy[i] & 0x0f];
    }
    token[DASHBOARD_TOKEN_HEX_LEN] = '\0';
    memset(entropy, 0, sizeof(entropy));
    return true;
}

static bool token_equal(const char *expected, const char *provided) {
    if (!expected || !provided)
        return false;
    size_t provided_len = strlen(provided);
    unsigned char diff = (unsigned char)(provided_len ^ DASHBOARD_TOKEN_HEX_LEN);
    for (size_t i = 0; i < DASHBOARD_TOKEN_HEX_LEN; i++) {
        unsigned char value = i < provided_len ? (unsigned char)provided[i] : 0;
        diff |= (unsigned char)(value ^ (unsigned char)expected[i]);
    }
    return diff == 0;
}

static bool request_has_token(const cbm_http_server_t *srv, const cbm_http_req_t *req) {
    char provided[DASHBOARD_TOKEN_HEX_LEN + 1];
    if (!srv || !req ||
        !cbm_http_query_param(req->query, "token", provided, (int)sizeof(provided))) {
        return false;
    }
    return token_equal(srv->token, provided);
}

static bool is_protected_path(const char *path) {
    if (!path)
        return false;
    return strcmp(path, "/rpc") == 0 || strcmp(path, "/api") == 0 ||
           strncmp(path, "/api/", 5) == 0;
}

/* ── Serve embedded asset ─────────────────────────────────────── */

static bool serve_embedded(cbm_http_conn_t *c, const char *path) {
    const cbm_embedded_file_t *f = cbm_embedded_lookup(path);
    if (!f)
        return false;

    /* Build headers with correct Content-Type for this asset */
    char hdrs[768];
    snprintf(hdrs, sizeof(hdrs),
             "%sContent-Type: %s\r\n"
             "Referrer-Policy: no-referrer\r\n"
             "X-Content-Type-Options: nosniff\r\n"
             "Cache-Control: public, max-age=31536000, immutable\r\n",
             g_cors, f->content_type);

    cbm_http_reply_buf(c, 200, hdrs, f->data, (size_t)f->size);
    return true;
}

/* Build DB path for a project: <cache_dir>/<project>.db */
static void db_path_for_project(const char *project, char *buf, size_t bufsz) {
    if (!cbm_validate_project_name(project)) {
        buf[0] = '\0';
        return;
    }
    const char *dir = cbm_resolve_cache_dir();
    if (!dir) {
        dir = cbm_tmpdir();
    }
    snprintf(buf, bufsz, "%s/%s.db", dir, project);
}

/* ── Log ring buffer ──────────────────────────────────────────── */

#define LOG_RING_SIZE 500
#define LOG_LINE_MAX 512

static char g_log_ring[LOG_RING_SIZE][LOG_LINE_MAX];
static int g_log_head = 0;
static int g_log_count = 0;
static cbm_mutex_t g_log_mutex;

enum { CBM_LOG_MUTEX_UNINIT = 0, CBM_LOG_MUTEX_INITING = 1, CBM_LOG_MUTEX_INITED = 2 };
static atomic_int g_log_mutex_init = CBM_LOG_MUTEX_UNINIT;

/* Safe for concurrent callers: only publishes INITED after cbm_mutex_init()
 * has completed. Callers that lose the CAS race spin until init finishes. */
void cbm_ui_log_init(void) {
    int state = atomic_load(&g_log_mutex_init);
    if (state == CBM_LOG_MUTEX_INITED)
        return;

    state = CBM_LOG_MUTEX_UNINIT;
    if (atomic_compare_exchange_strong(&g_log_mutex_init, &state, CBM_LOG_MUTEX_INITING)) {
        cbm_mutex_init(&g_log_mutex);
        atomic_store(&g_log_mutex_init, CBM_LOG_MUTEX_INITED);
        return;
    }

    /* Another thread is initializing — spin until done */
    while (atomic_load(&g_log_mutex_init) != CBM_LOG_MUTEX_INITED) {
        cbm_usleep(1000); /* 1ms */
    }
}

/* Called from a log hook — appends a line to the ring buffer (thread-safe) */
void cbm_ui_log_append(const char *line) {
    if (!line)
        return;
    /* Ensure mutex is initialized (safe for early single-threaded logging
     * and concurrent calls via atomic_exchange once-init pattern). */
    cbm_ui_log_init();
    cbm_mutex_lock(&g_log_mutex);
    snprintf(g_log_ring[g_log_head], LOG_LINE_MAX, "%s", line);
    g_log_head = (g_log_head + 1) % LOG_RING_SIZE;
    if (g_log_count < LOG_RING_SIZE)
        g_log_count++;
    cbm_mutex_unlock(&g_log_mutex);
}

/* GET /api/logs?lines=N — returns last N log lines */
static void handle_logs(cbm_http_conn_t *c, const cbm_http_req_t *req) {
    char lines_str[16] = {0};
    int max_lines = 100;
    if (cbm_http_query_param(req->query, "lines", lines_str, (int)sizeof(lines_str))) {
        int v = atoi(lines_str);
        if (v > 0 && v <= LOG_RING_SIZE)
            max_lines = v;
    }

    cbm_mutex_lock(&g_log_mutex);
    int count = g_log_count < max_lines ? g_log_count : max_lines;
    int start = (g_log_head - count + LOG_RING_SIZE) % LOG_RING_SIZE;
    int total = g_log_count;

    /* Copy lines under lock */
    size_t buf_size = (size_t)count * (LOG_LINE_MAX + 10) + 64;
    char *buf = malloc(buf_size);
    if (!buf) {
        cbm_mutex_unlock(&g_log_mutex);
        cbm_http_replyf(c, 500, g_cors, "oom");
        return;
    }

    int pos = 0;
    pos += snprintf(buf + pos, buf_size - (size_t)pos, "{\"lines\":[");
    for (int i = 0; i < count; i++) {
        int idx = (start + i) % LOG_RING_SIZE;
        if (i > 0)
            buf[pos++] = ',';
        /* Escape quotes in log lines */
        buf[pos++] = '"';
        for (int j = 0; g_log_ring[idx][j] && (size_t)pos < buf_size - 10; j++) {
            char ch = g_log_ring[idx][j];
            if (ch == '"') {
                buf[pos++] = '\\';
                buf[pos++] = '"';
            } else if (ch == '\\') {
                buf[pos++] = '\\';
                buf[pos++] = '\\';
            } else if (ch == '\n') {
                buf[pos++] = '\\';
                buf[pos++] = 'n';
            } else {
                buf[pos++] = ch;
            }
        }
        buf[pos++] = '"';
    }
    cbm_mutex_unlock(&g_log_mutex);
    pos += snprintf(buf + pos, buf_size - (size_t)pos, "],\"total\":%d}", total);

    cbm_http_replyf(c, 200, g_cors_json, "%s", buf);
    free(buf);
}

/* GET /api/project-health?name=X — checks db integrity */
static void handle_project_health(cbm_http_conn_t *c, const cbm_http_req_t *req) {
    char name[256] = {0};
    if (!cbm_http_query_param(req->query, "name", name, (int)sizeof(name)) || name[0] == '\0') {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"missing name\"}");
        return;
    }

    char db_path[1024];
    db_path_for_project(name, db_path, sizeof(db_path));

    if (!cbm_file_exists(db_path)) {
        cbm_http_replyf(c, 200, g_cors_json, "{\"status\":\"missing\"}");
        return;
    }

    cbm_store_t *store = cbm_store_open_path_query(db_path);
    if (!store) {
        cbm_http_replyf(c, 200, g_cors_json, "{\"status\":\"corrupt\",\"reason\":\"cannot open\"}");
        return;
    }

    int node_count = cbm_store_count_nodes(store, name);
    int edge_count = cbm_store_count_edges(store, name);
    cbm_store_close(store);

    int64_t size = cbm_file_size(db_path);

    cbm_http_replyf(c, 200, g_cors_json,
                    "{\"status\":\"healthy\",\"nodes\":%d,\"edges\":%d,\"size_bytes\":%lld}",
                    node_count, edge_count, (long long)size);
}

/* The graph explorer endpoint was removed; this surface is diagnostics-only. */

/* ── Handle JSON-RPC request ──────────────────────────────────── */

typedef enum {
    RPC_ACCESS_INVALID = 0,
    RPC_ACCESS_ALLOWED,
    RPC_ACCESS_DENIED,
} rpc_access_t;

static bool dashboard_tool_allowed(const char *name) {
    static const char *const allowed[] = {
        "get_context",      "search_graph",     "search_code",
        "trace_path",       "get_code_snippet", "get_architecture",
        "detect_changes",   "index_status",
    };
    if (!name)
        return false;
    for (size_t i = 0; i < sizeof(allowed) / sizeof(allowed[0]); i++) {
        if (strcmp(name, allowed[i]) == 0)
            return true;
    }
    return false;
}

static rpc_access_t dashboard_rpc_access(const cbm_http_req_t *req) {
    if (!req || !req->body || req->body_len == 0)
        return RPC_ACCESS_INVALID;

    yyjson_doc *doc = yyjson_read(req->body, req->body_len, 0);
    if (!doc)
        return RPC_ACCESS_INVALID;
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *method_value =
        root && yyjson_is_obj(root) ? yyjson_obj_get(root, "method") : NULL;
    const char *method =
        method_value && yyjson_is_str(method_value) ? yyjson_get_str(method_value) : NULL;
    rpc_access_t access = RPC_ACCESS_DENIED;

    /* Protocol setup and liveness are harmless. Tool discovery is deliberately
     * not proxied because the process-wide server may expose toolsets that are
     * inappropriate for the read-only dashboard. */
    if (method && (strcmp(method, "initialize") == 0 || strcmp(method, "ping") == 0 ||
                   strcmp(method, "notifications/initialized") == 0)) {
        access = RPC_ACCESS_ALLOWED;
    } else if (method && strcmp(method, "tools/call") == 0) {
        yyjson_val *params = yyjson_obj_get(root, "params");
        yyjson_val *name_value =
            params && yyjson_is_obj(params) ? yyjson_obj_get(params, "name") : NULL;
        const char *name =
            name_value && yyjson_is_str(name_value) ? yyjson_get_str(name_value) : NULL;
        access = name ? (dashboard_tool_allowed(name) ? RPC_ACCESS_ALLOWED : RPC_ACCESS_DENIED)
                      : RPC_ACCESS_INVALID;
    } else if (!method) {
        access = RPC_ACCESS_INVALID;
    }

    yyjson_doc_free(doc);
    return access;
}

static void handle_rpc(cbm_http_conn_t *c, const cbm_http_req_t *req, cbm_mcp_server_t *mcp) {
    if (req->body_len == 0 || req->body_len > MAX_BODY_SIZE || !req->body) {
        cbm_http_replyf(c, 400, g_cors_json,
                        "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32600,"
                        "\"message\":\"invalid request size\"},\"id\":null}");
        return;
    }

    rpc_access_t access = dashboard_rpc_access(req);
    if (access == RPC_ACCESS_INVALID) {
        cbm_http_replyf(c, 400, g_cors_json,
                        "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32600,"
                        "\"message\":\"invalid request\"},\"id\":null}");
        return;
    }
    if (access == RPC_ACCESS_DENIED) {
        cbm_http_replyf(c, 403, g_cors_json,
                        "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32003,"
                        "\"message\":\"tool is unavailable in the read-only dashboard\"},"
                        "\"id\":null}");
        return;
    }

    /* req->body is NUL-terminated by the transport */
    char *response = cbm_mcp_server_handle(mcp, req->body);

    if (response) {
        cbm_http_replyf(c, 200, g_cors_json, "%s", response);
        free(response);
    } else {
        cbm_http_replyf(c, 204, g_cors, "%s", "");
    }
}

/* ── Request dispatch ─────────────────────────────────────────── */

static void dispatch_request(cbm_http_server_t *srv, cbm_http_conn_t *c,
                             const cbm_http_req_t *req) {
    /* Build per-request CORS headers (only reflects localhost origins) */
    update_cors(req);

    bool is_get = strcmp(req->method, "GET") == 0;
    bool is_post = strcmp(req->method, "POST") == 0;

    /* OPTIONS preflight for CORS */
    if (strcmp(req->method, "OPTIONS") == 0) {
        cbm_http_replyf(c, 204, g_cors, "%s", "");
        return;
    }

    if (is_protected_path(req->path) && !request_has_token(srv, req)) {
        cbm_http_replyf(c, 403, g_cors_json,
                        "{\"error\":\"dashboard authentication required\"}");
        return;
    }

    /* POST /rpc → JSON-RPC dispatch (reuses existing MCP tools) */
    if (is_post && cbm_http_path_match(req->path, "/rpc")) {
        handle_rpc(c, req, srv->mcp);
        return;
    }

    /* GET /api/project-health → check db integrity */
    if (is_get && cbm_http_path_match(req->path, "/api/project-health")) {
        handle_project_health(c, req);
        return;
    }

    /* GET /api/logs → recent log lines */
    if (is_get && cbm_http_path_match(req->path, "/api/logs")) {
        handle_logs(c, req);
        return;
    }

    /* GET / → index.html (no-cache so browser always gets latest) */
    if (cbm_http_path_match(req->path, "/")) {
        const cbm_embedded_file_t *f = cbm_embedded_lookup("/index.html");
        if (f) {
            char html_hdrs[1024];
            snprintf(html_hdrs, sizeof(html_hdrs),
                     "%sContent-Type: text/html\r\n"
                     "Cache-Control: no-store\r\n"
                     "Referrer-Policy: no-referrer\r\n"
                     "X-Content-Type-Options: nosniff\r\n"
                     "Content-Security-Policy: default-src 'self'; "
                     "script-src 'self'; style-src 'self' 'unsafe-inline'; "
                     "img-src 'self' data:; connect-src 'self'; "
                     "object-src 'none'; base-uri 'none'; frame-ancestors 'none'\r\n",
                     g_cors);
            cbm_http_reply_buf(c, 200, html_hdrs, f->data, (size_t)f->size);
            return;
        }
        cbm_http_replyf(c, 404, g_cors, "no frontend embedded");
        return;
    }

    /* GET /assets/... → embedded assets, then generic embedded fallback */
    if (serve_embedded(c, req->path))
        return;

    cbm_http_replyf(c, 404, g_cors, "not found");
}

/* ── Public API ───────────────────────────────────────────────── */

cbm_http_server_t *cbm_http_server_new(int port) {
    cbm_http_server_t *srv = calloc(1, sizeof(*srv));
    if (!srv)
        return NULL;

    cbm_ui_log_init();
    srv->port = port;
    atomic_store(&srv->stop_flag, 0);
    if (!generate_dashboard_token(srv->token)) {
        cbm_log_error("ui.http.token_fail", "reason", "secure random source unavailable");
        free(srv);
        return NULL;
    }

    /* Create a dedicated MCP server for HTTP (own SQLite connection) */
    srv->mcp = cbm_mcp_server_new(NULL);
    if (!srv->mcp) {
        cbm_log_error("ui.http.mcp_fail", "reason", "cannot create MCP instance");
        free(srv);
        return NULL;
    }

    /* Bind to localhost only (httpd refuses anything else by construction) */
    srv->listener = cbm_httpd_listen(port);
    if (!srv->listener) {
        char port_str[16];
        snprintf(port_str, sizeof(port_str), "%d", port);
        cbm_log_warn("ui.unavailable", "port", port_str, "reason", "in_use", "hint",
                     "use --port=N to override");
        cbm_mcp_server_free(srv->mcp);
        free(srv);
        return NULL;
    }

    srv->port = cbm_httpd_port(srv->listener);
    srv->listener_ok = true;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", srv->port);
    char url[96];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d", srv->port);
    cbm_log_info("ui.serving", "url", url, "port", port_str, "access", "read_only");

    return srv;
}

void cbm_http_server_free(cbm_http_server_t *srv) {
    if (!srv)
        return;
    cbm_httpd_close(srv->listener);
    cbm_mcp_server_free(srv->mcp);
    free(srv);
}

void cbm_http_server_stop(cbm_http_server_t *srv) {
    if (srv) {
        atomic_store(&srv->stop_flag, 1);
    }
}

void cbm_http_server_run(cbm_http_server_t *srv) {
    if (!srv || !srv->listener_ok)
        return;

    while (!atomic_load(&srv->stop_flag)) {
        cbm_http_conn_t *conn = cbm_httpd_accept(srv->listener, 200);
        if (!conn)
            continue; /* timeout — re-check stop flag */

        cbm_http_req_t req;
        int rc = cbm_httpd_read_request(conn, &req);
        if (rc == 0) {
            dispatch_request(srv, conn, &req);
            cbm_http_req_free(&req);
        } else if (rc > 0) {
            /* Parse/transport error with a known HTTP status (400/408/411/413/431).
             * No CORS reflection here — the request was never parsed. */
            cbm_http_replyf(conn, rc, "", "bad request");
        }
        cbm_httpd_conn_close(conn);
    }
}

bool cbm_http_server_is_running(const cbm_http_server_t *srv) {
    return srv && srv->listener_ok;
}

int cbm_http_server_port(const cbm_http_server_t *srv) {
    return (srv && srv->listener_ok) ? srv->port : -1;
}

const char *cbm_http_server_token(const cbm_http_server_t *srv) {
    return srv && srv->listener_ok ? srv->token : NULL;
}

bool cbm_http_server_launch_url(const cbm_http_server_t *srv, char *buf, size_t bufsz) {
    if (!srv || !srv->listener_ok || !buf || bufsz == 0)
        return false;
    int written =
        snprintf(buf, bufsz, "http://127.0.0.1:%d/#token=%s", srv->port, srv->token);
    return written >= 0 && (size_t)written < bufsz;
}

void cbm_http_server_set_recv_deadline_ms(cbm_http_server_t *srv, int ms) {
    if (srv && srv->listener_ok) {
        cbm_httpd_set_recv_deadline_ms(srv->listener, ms);
    }
}
