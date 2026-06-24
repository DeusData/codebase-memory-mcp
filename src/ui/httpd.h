/*
 * httpd.h — First-party HTTP/1.1 server transport for the graph UI.
 *
 * Original implementation written for this project from RFC 9112 and the
 * needs of the graph-UI endpoints. Localhost-only by construction.
 *
 * Design constraints (deliberate — do not "improve" without reading this):
 *   - SINGLE-THREADED, sequential request handling. The routing layer
 *     (http_server.c) keeps per-request state in static buffers; a thread
 *     pool would break it. One stalled client can hold the loop for at most
 *     the receive deadline (default 5 s) — acceptable for a localhost tool.
 *   - Binds 127.0.0.1 only (IPv4 loopback). Never any other interface.
 *   - Every response carries explicit Content-Length and "Connection: close";
 *     keep-alive is intentionally NOT implemented (smaller parsing surface;
 *     loopback reconnects are sub-millisecond). Known trade-off: on Windows,
 *     aggressive UI polling accumulates TIME_WAIT sockets against the ~16K
 *     dynamic-port ceiling — revisit only if real users report it.
 *   - Strict parsing: CRLF line endings only (bare LF rejected), request
 *     head capped at 16 KB (real requests here are < 1 KB; the cap exists
 *     to bound memory, not to accommodate growth), bodies read only via
 *     Content-Length (capped), Transfer-Encoding: chunked rejected with 411.
 *   - The request path is matched RAW — never percent-decoded before
 *     routing ("/api%2Fbrowse" must not match "/api/browse"). "%00" or a
 *     raw NUL anywhere in the request target is rejected with 400. Only
 *     query parameter VALUES are decoded (cbm_http_query_param), and
 *     decoded values containing NUL are rejected.
 */
#ifndef CBM_UI_HTTPD_H
#define CBM_UI_HTTPD_H

#include <stdbool.h>
#include <stddef.h>

/* Maximum request head (request line + headers + terminating CRLFCRLF). */
#define CBM_HTTP_MAX_HEAD (16 * 1024)
/* Maximum request body accepted via Content-Length. */
#define CBM_HTTP_MAX_BODY (1024 * 1024)
/* Default per-connection receive deadline. */
#define CBM_HTTP_RECV_DEADLINE_MS 5000

typedef struct cbm_httpd cbm_httpd_t;         /* listener */
typedef struct cbm_http_conn cbm_http_conn_t; /* accepted connection */

/* A parsed request. `path` and `query` are raw (NOT percent-decoded).
 * `origin` is the Origin header value ("" when absent) — the only header
 * the routing layer consumes. `body` is heap-allocated, NUL-terminated. */
typedef struct {
    char method[16];
    char path[2048];
    char query[2048];
    char origin[256];
    char *body;
    size_t body_len;
} cbm_http_req_t;

/* ── Listener lifecycle ───────────────────────────────────────── */

/* Listen on 127.0.0.1:<port>. port 0 binds an ephemeral port (tests).
 * Returns NULL if the port is unavailable. */
cbm_httpd_t *cbm_httpd_listen(int port);

/* The actually-bound port (differs from the requested one for port 0). */
int cbm_httpd_port(const cbm_httpd_t *d);

/* Override the per-connection receive deadline (tests use short values). */
void cbm_httpd_set_recv_deadline_ms(cbm_httpd_t *d, int ms);

void cbm_httpd_close(cbm_httpd_t *d);

/* ── Connection handling ──────────────────────────────────────── */

/* Wait up to timeout_ms for a client. NULL on timeout (caller re-checks
 * its stop flag and calls again). */
cbm_http_conn_t *cbm_httpd_accept(cbm_httpd_t *d, int timeout_ms);

/* Read and parse one request from the connection.
 * Returns 0 on success. On failure returns the HTTP status the caller
 * should send before closing (400, 408, 411, 413, 431), or -1 for a
 * connection-level error where no response is possible. */
int cbm_httpd_read_request(cbm_http_conn_t *c, cbm_http_req_t *req);

void cbm_http_req_free(cbm_http_req_t *req);

/* Send a response. extra_headers is a string of zero or more complete
 * "Name: value\r\n" lines (may be ""). Content-Length and
 * "Connection: close" are always added here — callers must not. */
void cbm_http_replyf(cbm_http_conn_t *c, int status, const char *extra_headers, const char *fmt,
                     ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((format(printf, 4, 5)))
#endif
    ;

/* Binary-safe variant for embedded assets. */
void cbm_http_reply_buf(cbm_http_conn_t *c, int status, const char *extra_headers, const void *data,
                        size_t len);

void cbm_httpd_conn_close(cbm_http_conn_t *c);

/* ── Pure helpers (unit-tested without sockets) ───────────────── */

/* Parse a request head from `data` (which may also contain body bytes).
 * On success returns 0 and sets *body_offset (start of body within data)
 * and *content_length (0 when no Content-Length header is present).
 * Returns CBM_HTTP_NEED_MORE when the terminating CRLFCRLF has not
 * arrived yet, otherwise the HTTP error status to send (400/411/413/431).
 * req->body / req->body_len are NOT touched here. */
#define CBM_HTTP_NEED_MORE (-1)
int cbm_http_parse_head(const char *data, size_t len, cbm_http_req_t *req, size_t *body_offset,
                        size_t *content_length);

/* Exact match, or prefix match when `pattern` ends with '*'.
 * Used for both route patterns ("/api/layout*", "/assets" + star) and the
 * CORS origin allow-list ("http://localhost:*", "http://127.0.0.1:*"). */
bool cbm_http_path_match(const char *str, const char *pattern);

/* Extract a query parameter value, percent-decoded (%XX and '+' → space).
 * Returns true only for a present, non-empty value that fits buf and
 * contains no NUL after decoding. */
bool cbm_http_query_param(const char *query, const char *name, char *buf, int bufsz);

#endif /* CBM_UI_HTTPD_H */
