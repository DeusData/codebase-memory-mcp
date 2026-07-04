/*
 * test_httpd.c — Tests for the first-party graph-UI HTTP server.
 *
 * Two layers:
 *   1. Parser/helper unit tests against httpd.h's pure functions
 *      (no sockets): request-line parsing, strict CRLF, Content-Length
 *      edge cases, chunked rejection, NUL/percent-decode rules,
 *      query-param decoding, route pattern matching.
 *   2. Live-socket integration tests against the full UI server
 *      (http_server.c) on an ephemeral port: routing, CORS policy,
 *      RPC dispatch, transport limits, receive deadline, clean shutdown.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/compat_thread.h"
#include "../src/foundation/log.h"
#include "../src/foundation/platform.h"
#include "../src/cli/cli.h"
#include "../src/ui/http_server.h"
#include "git/git_context.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "ui/httpd.h"
#include "ui/http_server.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <sys/stat.h>
#endif

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
typedef SOCKET th_sock_t;
#define th_sock_close closesocket
#define TH_SOCK_BAD INVALID_SOCKET
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
typedef int th_sock_t;
#define th_sock_close close
#define TH_SOCK_BAD (-1)
#endif

static char httpd_log_buf[8192];

static void httpd_capture_log(const char *line) {
    size_t used = strlen(httpd_log_buf);
    size_t avail = sizeof(httpd_log_buf) - used;
    if (avail <= 1)
        return;
    int n = snprintf(httpd_log_buf + used, avail, "%s\n", line ? line : "");
    if (n < 0 || (size_t)n >= avail)
        httpd_log_buf[sizeof(httpd_log_buf) - 1] = '\0';
}

/* ── Raw-socket test client ───────────────────────────────────── */

static th_sock_t th_connect(int port) {
#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa); /* refcounted; cleanup not needed in tests */
#endif
    th_sock_t s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == TH_SOCK_BAD)
        return TH_SOCK_BAD;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)port);
    addr.sin_addr.s_addr = htonl(0x7F000001); /* 127.0.0.1 */
    if (connect(s, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        th_sock_close(s);
        return TH_SOCK_BAD;
    }
    return s;
}

static int th_send_all(th_sock_t s, const char *data, size_t len) {
    size_t off = 0;
    while (off < len) {
#ifdef _WIN32
        int n = send(s, data + off, (int)(len - off), 0);
#else
        ssize_t n = send(s, data + off, len - off, 0);
#endif
        if (n <= 0)
            return -1;
        off += (size_t)n;
    }
    return 0;
}

/* Read until the server closes the connection (Connection: close model). */
static int th_recv_until_close(th_sock_t s, char *buf, size_t bufsz) {
    size_t off = 0;
    for (;;) {
#ifdef _WIN32
        int n = recv(s, buf + off, (int)(bufsz - 1 - off), 0);
#else
        ssize_t n = recv(s, buf + off, bufsz - 1 - off, 0);
#endif
        if (n <= 0)
            break;
        off += (size_t)n;
        if (off >= bufsz - 1)
            break;
    }
    buf[off] = '\0';
    return (int)off;
}

/* One-shot HTTP exchange. Returns response length, 0 on connect failure. */
static int th_http(int port, const char *request, char *resp, size_t respsz) {
    th_sock_t s = th_connect(port);
    if (s == TH_SOCK_BAD)
        return 0;
    if (th_send_all(s, request, strlen(request)) != 0) {
        th_sock_close(s);
        return 0;
    }
    int n = th_recv_until_close(s, resp, respsz);
    th_sock_close(s);
    return n;
}

/* HTTP status code from a raw response ("HTTP/1.1 404 ..."), or -1. */
static int th_status(const char *resp) {
    if (strncmp(resp, "HTTP/1.1 ", 9) != 0)
        return -1;
    return atoi(resp + 9);
}

/* ── Parser unit tests ────────────────────────────────────────── */

TEST(httpd_parse_simple_get) {
    const char *raw = "GET /api/logs?lines=5 HTTP/1.1\r\n"
                      "Host: 127.0.0.1\r\n"
                      "Origin: http://localhost:5173\r\n"
                      "\r\n";
    cbm_http_req_t req;
    size_t body_off = 0, clen = 99;
    int rc = cbm_http_parse_head(raw, strlen(raw), &req, &body_off, &clen);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(req.method, "GET");
    ASSERT_STR_EQ(req.path, "/api/logs");
    ASSERT_STR_EQ(req.query, "lines=5");
    ASSERT_STR_EQ(req.origin, "http://localhost:5173");
    ASSERT_EQ((int)clen, 0);
    ASSERT_EQ((int)body_off, (int)strlen(raw));
    PASS();
}

TEST(httpd_parse_post_with_body_offset) {
    const char *raw = "POST /rpc HTTP/1.1\r\n"
                      "Content-Length: 7\r\n"
                      "Content-Type: application/json\r\n"
                      "\r\n"
                      "{\"a\":1}";
    cbm_http_req_t req;
    size_t body_off = 0, clen = 0;
    int rc = cbm_http_parse_head(raw, strlen(raw), &req, &body_off, &clen);
    ASSERT_EQ(rc, 0);
    ASSERT_STR_EQ(req.method, "POST");
    ASSERT_STR_EQ(req.path, "/rpc");
    ASSERT_STR_EQ(req.query, "");
    ASSERT_EQ((int)clen, 7);
    ASSERT_STR_EQ(raw + body_off, "{\"a\":1}");
    PASS();
}

TEST(httpd_parse_origin_case_insensitive) {
    const char *raw = "GET / HTTP/1.1\r\n"
                      "origin: http://127.0.0.1:9749\r\n"
                      "\r\n";
    cbm_http_req_t req;
    size_t body_off = 0, clen = 0;
    ASSERT_EQ(cbm_http_parse_head(raw, strlen(raw), &req, &body_off, &clen), 0);
    ASSERT_STR_EQ(req.origin, "http://127.0.0.1:9749");
    PASS();
}

TEST(httpd_parse_rejects_bare_lf) {
    const char *raw = "GET / HTTP/1.1\nHost: x\n\n";
    cbm_http_req_t req;
    size_t body_off = 0, clen = 0;
    ASSERT_EQ(cbm_http_parse_head(raw, strlen(raw), &req, &body_off, &clen), 400);
    PASS();
}

TEST(httpd_parse_rejects_chunked) {
    const char *raw = "POST /rpc HTTP/1.1\r\n"
                      "Transfer-Encoding: chunked\r\n"
                      "\r\n";
    cbm_http_req_t req;
    size_t body_off = 0, clen = 0;
    ASSERT_EQ(cbm_http_parse_head(raw, strlen(raw), &req, &body_off, &clen), 411);
    PASS();
}

TEST(httpd_parse_rejects_oversized_content_length) {
    char raw[256];
    snprintf(raw, sizeof(raw), "POST /rpc HTTP/1.1\r\nContent-Length: %d\r\n\r\n",
             CBM_HTTP_MAX_BODY + 1);
    cbm_http_req_t req;
    size_t body_off = 0, clen = 0;
    ASSERT_EQ(cbm_http_parse_head(raw, strlen(raw), &req, &body_off, &clen), 413);
    PASS();
}

TEST(httpd_parse_rejects_garbage_content_length) {
    const char *raw = "POST /rpc HTTP/1.1\r\nContent-Length: abc\r\n\r\n";
    cbm_http_req_t req;
    size_t body_off = 0, clen = 0;
    ASSERT_EQ(cbm_http_parse_head(raw, strlen(raw), &req, &body_off, &clen), 400);

    const char *neg = "POST /rpc HTTP/1.1\r\nContent-Length: -5\r\n\r\n";
    ASSERT_EQ(cbm_http_parse_head(neg, strlen(neg), &req, &body_off, &clen), 400);
    PASS();
}

TEST(httpd_parse_rejects_percent00_in_target) {
    const char *raw = "GET /a%00b HTTP/1.1\r\n\r\n";
    cbm_http_req_t req;
    size_t body_off = 0, clen = 0;
    ASSERT_EQ(cbm_http_parse_head(raw, strlen(raw), &req, &body_off, &clen), 400);

    /* %00 hidden in the query string is rejected too */
    const char *q = "GET /ok?x=%00 HTTP/1.1\r\n\r\n";
    ASSERT_EQ(cbm_http_parse_head(q, strlen(q), &req, &body_off, &clen), 400);
    PASS();
}

TEST(httpd_parse_rejects_raw_nul_in_head) {
    char raw[64] = "GET /a";
    size_t len = 6;
    raw[len++] = '\0';
    memcpy(raw + len, " HTTP/1.1\r\n\r\n", 13);
    len += 13;
    cbm_http_req_t req;
    size_t body_off = 0, clen = 0;
    ASSERT_EQ(cbm_http_parse_head(raw, len, &req, &body_off, &clen), 400);
    PASS();
}

TEST(httpd_parse_incomplete_head_needs_more) {
    const char *raw = "GET /api/logs HTTP/1.1\r\nHost: x\r\n"; /* no CRLFCRLF yet */
    cbm_http_req_t req;
    size_t body_off = 0, clen = 0;
    ASSERT_EQ(cbm_http_parse_head(raw, strlen(raw), &req, &body_off, &clen), CBM_HTTP_NEED_MORE);
    PASS();
}

TEST(httpd_parse_rejects_missing_version) {
    const char *raw = "GET /\r\n\r\n";
    cbm_http_req_t req;
    size_t body_off = 0, clen = 0;
    ASSERT_EQ(cbm_http_parse_head(raw, strlen(raw), &req, &body_off, &clen), 400);

    const char *v2 = "GET / HTTP/2\r\n\r\n";
    ASSERT_EQ(cbm_http_parse_head(v2, strlen(v2), &req, &body_off, &clen), 400);
    PASS();
}

TEST(httpd_parse_rejects_oversized_head) {
    /* A head that exceeds CBM_HTTP_MAX_HEAD without terminating → 431 */
    size_t big = CBM_HTTP_MAX_HEAD + 1024;
    char *raw = malloc(big);
    ASSERT_NOT_NULL(raw);
    memcpy(raw, "GET / HTTP/1.1\r\nX-Junk: ", 24);
    memset(raw + 24, 'A', big - 24);
    cbm_http_req_t req;
    size_t body_off = 0, clen = 0;
    int rc = cbm_http_parse_head(raw, big, &req, &body_off, &clen);
    free(raw);
    ASSERT_EQ(rc, 431);
    PASS();
}

TEST(httpd_query_param_decode) {
    char buf[64];
    ASSERT_TRUE(cbm_http_query_param("a=hello+world&b=%2Ffoo%2F", "a", buf, (int)sizeof(buf)));
    ASSERT_STR_EQ(buf, "hello world");
    ASSERT_TRUE(cbm_http_query_param("a=hello+world&b=%2Ffoo%2F", "b", buf, (int)sizeof(buf)));
    ASSERT_STR_EQ(buf, "/foo/");
    /* uppercase + lowercase hex */
    ASSERT_TRUE(cbm_http_query_param("p=%2fTmp%2F", "p", buf, (int)sizeof(buf)));
    ASSERT_STR_EQ(buf, "/Tmp/");
    PASS();
}

TEST(httpd_query_param_edge_cases) {
    char buf[8];
    /* missing param */
    ASSERT_FALSE(cbm_http_query_param("a=1", "b", buf, (int)sizeof(buf)));
    /* empty value (current server treats it as absent) */
    ASSERT_FALSE(cbm_http_query_param("a=&b=2", "a", buf, (int)sizeof(buf)));
    /* value too large for buf */
    ASSERT_FALSE(cbm_http_query_param("a=123456789", "a", buf, (int)sizeof(buf)));
    /* decoded NUL rejected */
    char big[32];
    ASSERT_FALSE(cbm_http_query_param("a=x%00y", "a", big, (int)sizeof(big)));
    /* name is a prefix of another name — must not match */
    ASSERT_FALSE(cbm_http_query_param("abc=1", "ab", buf, (int)sizeof(buf)));
    /* truncated percent escape */
    ASSERT_FALSE(cbm_http_query_param("a=%2", "a", buf, (int)sizeof(buf)));
    PASS();
}

TEST(httpd_path_match_matrix) {
    /* exact */
    ASSERT_TRUE(cbm_http_path_match("/", "/"));
    ASSERT_FALSE(cbm_http_path_match("/x", "/"));
    ASSERT_TRUE(cbm_http_path_match("/rpc", "/rpc"));
    ASSERT_FALSE(cbm_http_path_match("/rpc2", "/rpc"));
    /* trailing-* prefix */
    ASSERT_TRUE(cbm_http_path_match("/api/layout", "/api/layout*"));
    ASSERT_TRUE(cbm_http_path_match("/assets/index-abc.js", "/assets/*"));
    ASSERT_FALSE(cbm_http_path_match("/api/browse", "/api/layout*"));
    /* raw path is matched — percent-encoded slash must NOT route */
    ASSERT_FALSE(cbm_http_path_match("/api%2Fbrowse", "/api/browse*"));
    ASSERT_FALSE(cbm_http_path_match("/api%2fbrowse", "/api/browse*"));
    /* CORS origin patterns */
    ASSERT_TRUE(cbm_http_path_match("http://localhost:5173", "http://localhost:*"));
    ASSERT_TRUE(cbm_http_path_match("http://127.0.0.1:9749", "http://127.0.0.1:*"));
    ASSERT_FALSE(cbm_http_path_match("http://evil.com", "http://localhost:*"));
    ASSERT_FALSE(cbm_http_path_match("https://localhost:5173", "http://localhost:*"));
    ASSERT_FALSE(cbm_http_path_match("http://localhost.evil.com:80", "http://localhost:*"));
    PASS();
}

TEST(httpd_resolves_bare_binary_path_from_path) {
#ifdef _WIN32
    PASS();
#else
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_httpd_bin_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char exe[512];
    snprintf(exe, sizeof(exe), "%s/codebase-memory-mcp", td);
    FILE *f = fopen(exe, "w");
    ASSERT_NOT_NULL(f);
    fputs("#!/bin/sh\nexit 0\n", f);
    fclose(f);
    ASSERT_EQ(chmod(exe, 0755), 0);

    char *old_path = getenv("PATH") ? strdup(getenv("PATH")) : NULL;
    cbm_setenv("PATH", td, 1);

    char resolved[1024];
    ASSERT_TRUE(
        cbm_http_server_resolve_binary_path("codebase-memory-mcp", resolved, sizeof(resolved)));
    ASSERT_STR_EQ(resolved, exe);

    if (old_path) {
        cbm_setenv("PATH", old_path, 1);
        free(old_path);
    } else {
        cbm_unsetenv("PATH");
    }
    PASS();
#endif
}

/* ── Transport integration (listener only) ────────────────────── */

TEST(httpd_listen_ephemeral_port) {
    cbm_httpd_t *d = cbm_httpd_listen(0);
    ASSERT_NOT_NULL(d);
    int port = cbm_httpd_port(d);
    ASSERT_GT(port, 0);
    /* accept with a short timeout and no client → NULL, promptly */
    cbm_http_conn_t *c = cbm_httpd_accept(d, 50);
    ASSERT_NULL(c);
    cbm_httpd_close(d);
    PASS();
}

TEST(httpd_listen_port_collision_returns_null) {
    cbm_httpd_t *d1 = cbm_httpd_listen(0);
    ASSERT_NOT_NULL(d1);
    cbm_httpd_t *d2 = cbm_httpd_listen(cbm_httpd_port(d1));
    ASSERT_NULL(d2);
    cbm_httpd_close(d1);
    PASS();
}

/* ── Full UI server integration ───────────────────────────────── */

typedef struct {
    cbm_http_server_t *srv;
    cbm_thread_t tid;
} th_server_t;

static void *th_server_thread(void *arg) {
    cbm_http_server_run((cbm_http_server_t *)arg);
    return NULL;
}

static int th_server_start(th_server_t *ts) {
    ts->srv = cbm_http_server_new(0);
    if (!ts->srv)
        return -1;
    if (cbm_thread_create(&ts->tid, 0, th_server_thread, ts->srv) != 0) {
        cbm_http_server_free(ts->srv);
        return -1;
    }
    return 0;
}

static void th_server_stop(th_server_t *ts) {
    cbm_http_server_stop(ts->srv);
    cbm_thread_join(&ts->tid);
    cbm_http_server_free(ts->srv);
}

/* ── Watchdog: turn a hang into a hard failure ─────────────────── */

/* A deadlocked git child cannot be recovered in-process, so a "must not hang"
 * guard needs a watchdog thread that force-fails the whole run if the operation
 * under test does not signal completion within a generous window. On a healthy
 * build the flag is set long before the window elapses and the watchdog is a
 * no-op. */
typedef struct {
    volatile int done;
    int timeout_ms;
    const char *what;
} th_watchdog_t;

static void *th_watchdog_fn(void *arg) {
    th_watchdog_t *wd = (th_watchdog_t *)arg;
    int waited = 0;
    while (waited < wd->timeout_ms) {
        cbm_usleep(100 * 1000);
        waited += 100;
        if (wd->done)
            return NULL;
    }
    if (!wd->done) {
        fprintf(stderr,
                "\n  FATAL: watchdog fired after %dms — %s hung "
                "(handle-inheritance deadlock, issue #798)\n",
                wd->timeout_ms, wd->what);
        fflush(stderr);
        _exit(99); /* _exit, not exit: don't run atexit handlers from a wedged process */
    }
    return NULL;
}

/* Probe for a usable git on PATH. Called BEFORE the UI server starts (no socket
 * handles exist yet), so this cbm_popen can never trip the #798 deadlock. */
static int th_git_available(void) {
    FILE *fp = cbm_popen("git --version", "r");
    if (!fp)
        return 0;
    char buf[128];
    char *got = fgets(buf, sizeof(buf), fp);
    cbm_pclose(fp);
    return got && strncmp(buf, "git version", 11) == 0;
}

TEST(ui_server_unknown_path_404) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    int port = cbm_http_server_port(ts.srv);

    char resp[4096];
    int n = th_http(port, "GET /definitely/not/here HTTP/1.1\r\n\r\n", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 404);
    /* every response is explicit-length + close */
    ASSERT_NOT_NULL(strstr(resp, "Connection: close"));
    ASSERT_NOT_NULL(strstr(resp, "Content-Length:"));

    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_root_serves_stub_404) {
    /* Test binary links embedded_stub.c → no frontend → 404 with marker */
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    char resp[4096];
    int n = th_http(cbm_http_server_port(ts.srv), "GET / HTTP/1.1\r\n\r\n", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 404);
    ASSERT_NOT_NULL(strstr(resp, "no frontend embedded"));
    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_cors_localhost_reflected) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    char resp[4096];
    int n = th_http(cbm_http_server_port(ts.srv),
                    "OPTIONS /rpc HTTP/1.1\r\n"
                    "Origin: http://localhost:5173\r\n\r\n",
                    resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 204);
    ASSERT_NOT_NULL(strstr(resp, "Access-Control-Allow-Origin: http://localhost:5173"));
    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_cors_evil_origin_not_reflected) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    char resp[4096];
    int n = th_http(cbm_http_server_port(ts.srv),
                    "OPTIONS /rpc HTTP/1.1\r\n"
                    "Origin: http://evil.example.com\r\n\r\n",
                    resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 204);
    ASSERT_NULL(strstr(resp, "Access-Control-Allow-Origin"));
    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_rpc_initialize) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    const char *body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
                       "\"params\":{\"protocolVersion\":\"2024-11-05\","
                       "\"capabilities\":{},"
                       "\"clientInfo\":{\"name\":\"t\",\"version\":\"0\"}}}";
    char req[1024];
    snprintf(req, sizeof(req),
             "POST /rpc HTTP/1.1\r\n"
             "Content-Type: application/json\r\n"
             "Content-Length: %d\r\n\r\n%s",
             (int)strlen(body), body);
    char resp[8192];
    int n = th_http(cbm_http_server_port(ts.srv), req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 200);
    ASSERT_NOT_NULL(strstr(resp, "\"jsonrpc\""));
    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_oversized_body_rejected) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    char req[256];
    snprintf(req, sizeof(req), "POST /rpc HTTP/1.1\r\nContent-Length: %d\r\n\r\n",
             CBM_HTTP_MAX_BODY + 1);
    char resp[4096];
    int n = th_http(cbm_http_server_port(ts.srv), req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 413);
    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_encoded_slash_not_routed) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    char resp[4096];
    int n = th_http(cbm_http_server_port(ts.srv), "GET /api%2Fbrowse?path=/tmp HTTP/1.1\r\n\r\n",
                    resp, sizeof(resp));
    ASSERT_GT(n, 0);
    /* must fall through to 404 — NOT the browse handler */
    ASSERT_EQ(th_status(resp), 404);
    ASSERT_NULL(strstr(resp, "\"dirs\""));
    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_nul_in_target_rejected) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    char resp[4096];
    int n =
        th_http(cbm_http_server_port(ts.srv), "GET /a%00b HTTP/1.1\r\n\r\n", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 400);
    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_browse_traversal_probe) {
    /* Percent-encoded traversal in the QUERY VALUE is decoded (that is the
     * documented contract) and then hits the same directory checks as any
     * other path. The server must answer with a well-formed JSON error or
     * listing — never crash, never echo raw unescaped input. */
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    char resp[65536];
    int n = th_http(cbm_http_server_port(ts.srv),
                    "GET /api/browse?path=%2Ftmp%2F..%2F..%2Fprivate HTTP/1.1\r\n\r\n", resp,
                    sizeof(resp));
    ASSERT_GT(n, 0);
    int st = th_status(resp);
    ASSERT_TRUE(st == 200 || st == 400 || st == 403);
    const char *json = strstr(resp, "\r\n\r\n");
    ASSERT_NOT_NULL(json);
    ASSERT_EQ(json[4], '{');
    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_ui_config_detects_zh_accept_language) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);

    char resp[4096];
    int n = th_http(cbm_http_server_port(ts.srv),
                    "GET /api/ui-config HTTP/1.1\r\n"
                    "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8\r\n"
                    "\r\n",
                    resp, sizeof(resp));
    ASSERT_TRUE(n > 0);
    ASSERT_EQ(th_status(resp), 200);
    ASSERT_NOT_NULL(strstr(resp, "\"lang\":\"zh\""));

    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_ui_config_prefers_config_lang) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_httpd_cfg_XXXXXX");
    char *td = cbm_mkdtemp(tmpdir);
    ASSERT_NOT_NULL(td);

    char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
    cbm_setenv("HOME", td, 1);

    char cache_dir[1024];
    snprintf(cache_dir, sizeof(cache_dir), "%s", cbm_resolve_cache_dir());
    cbm_config_t *cfg = cbm_config_open(cache_dir);
    ASSERT_NOT_NULL(cfg);
    ASSERT_EQ(cbm_config_set(cfg, CBM_CONFIG_UI_LANG, "zh"), 0);
    cbm_config_close(cfg);

    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);

    char resp[4096];
    int n = th_http(cbm_http_server_port(ts.srv),
                    "GET /api/ui-config HTTP/1.1\r\n"
                    "Accept-Language: en-US,en;q=0.9\r\n"
                    "\r\n",
                    resp, sizeof(resp));
    ASSERT_TRUE(n > 0);
    ASSERT_EQ(th_status(resp), 200);
    ASSERT_NOT_NULL(strstr(resp, "\"lang\":\"zh\""));

    th_server_stop(&ts);
    if (old_home) {
        cbm_setenv("HOME", old_home, 1);
        free(old_home);
    }
    PASS();
}

TEST(ui_server_slow_request_hits_deadline) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    /* Shorten the deadline so the test is fast */
    cbm_http_server_set_recv_deadline_ms(ts.srv, 300);
    int port = cbm_http_server_port(ts.srv);

    th_sock_t s = th_connect(port);
    ASSERT_TRUE(s != TH_SOCK_BAD);
    ASSERT_EQ(th_send_all(s, "GET /api", 8), 0); /* partial request, then stall */
    char resp[1024];
    int n = th_recv_until_close(s, resp, sizeof(resp)); /* server must give up */
    th_sock_close(s);
    /* Either a 408 or a bare close is acceptable — the loop must move on */
    if (n > 0) {
        ASSERT_EQ(th_status(resp), 408);
    }

    /* …and the server must still answer the next request */
    char resp2[4096];
    int n2 = th_http(port, "GET /definitely/not/here HTTP/1.1\r\n\r\n", resp2, sizeof(resp2));
    ASSERT_GT(n2, 0);
    ASSERT_EQ(th_status(resp2), 404);

    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_access_log_redacts_query) {
    httpd_log_buf[0] = '\0';
    CBMLogLevel prev_level = cbm_log_get_level();
    cbm_log_set_level(CBM_LOG_DEBUG);
    cbm_log_set_format(CBM_LOG_FORMAT_TEXT);
    cbm_log_set_sink_ex(httpd_capture_log, CBM_LOG_SINK_REPLACE);

    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    char resp[4096];
    int n = th_http(cbm_http_server_port(ts.srv),
                    "GET /definitely/not/here?token=secret HTTP/1.1\r\n\r\n", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 404);
    th_server_stop(&ts);

    cbm_log_set_sink(NULL);
    cbm_log_set_level(prev_level);

    ASSERT_NOT_NULL(strstr(httpd_log_buf, "msg=http.request"));
    ASSERT_NOT_NULL(strstr(httpd_log_buf, "component=graph_ui"));
    ASSERT_NOT_NULL(strstr(httpd_log_buf, "method=GET"));
    ASSERT_NOT_NULL(strstr(httpd_log_buf, "path=/definitely/not/here"));
    ASSERT_NOT_NULL(strstr(httpd_log_buf, "status=404"));
    ASSERT_NULL(strstr(httpd_log_buf, "token"));
    ASSERT_NULL(strstr(httpd_log_buf, "secret"));
    PASS();
}

TEST(ui_server_stop_joins_cleanly) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    /* no requests at all — stop must unblock the accept wait promptly */
    th_server_stop(&ts);
    PASS();
}

/* ── Regression: #798 — cbm_popen must not leak handles into the git child ──
 *
 * Root cause: on Windows the UI HTTP server calls WSAStartup and holds
 * listening/AFD socket handles. list_projects resolves git context per project
 * (add_git_context_json → cbm_git_context_resolve → git_capture → cbm_popen).
 * The CRT _popen spawns via CreateProcess(bInheritHandles=TRUE), leaking EVERY
 * inheritable handle — including those AFD handles — into git-for-Windows, whose
 * MSYS/Cygwin startup classifies each inherited handle with NtQueryObject and
 * DEADLOCKS on a socket/AFD handle. The single-threaded UI server then wedges
 * and the web UI hangs forever. The fix reimplements cbm_popen to inherit ONLY
 * the stdout pipe (STARTUPINFOEX + PROC_THREAD_ATTRIBUTE_HANDLE_LIST).
 *
 * Guard strategy — two layers, because the *deadlock* is environment-sensitive:
 * whether NtQueryObject actually hangs depends on the git-for-Windows/MSYS
 * runtime and the AFD provider, so a git-based test cannot be relied on to go
 * RED everywhere (verified: it does NOT reproduce on every dev machine, and the
 * Linux CI has no such sockets at all). So the load-bearing guard tests the
 * fix's *contract* directly and deterministically —
 * cbm_popen_isolates_inheritable_handle below asserts no foreign inheritable
 * handle crosses into the child, which goes RED on any Windows without the fix.
 *
 * The two tests here are end-to-end liveness invariants: they enforce "git
 * context resolves / list_projects responds while the UI server holds live
 * sockets, under a watchdog." They pass with or without the fix on git builds
 * that don't exhibit the NtQueryObject hang, but they catch #798 on the affected
 * builds and guard against any future hang regression in this path. (Same
 * pattern as the environment-sensitive guards in test_git_context.c.)
 *
 * A real repo is unnecessary — resolving any existing directory spawns
 * `git -C <dir> rev-parse …`, exercising the exact cbm_popen shell-out;
 * that also sidesteps the Windows CI shell's inability to `git init` via
 * system(). On POSIX popen() already sets O_CLOEXEC, so this is a sanity check
 * there. */
TEST(ui_server_git_context_under_live_sockets_no_hang) {
    /* Probe git before any socket exists — safe, cannot deadlock. */
    if (!th_git_available())
        SKIP_PLATFORM("git not on PATH — cannot exercise the git shell-out");

    char *tmp = th_mktempdir("cbm_ui798");
    if (!tmp)
        FAIL("th_mktempdir returned NULL");

    /* Bring up the real UI server — this allocates the AFD/socket handles that
     * the unfixed cbm_popen would leak into git. */
    th_server_t ts;
    if (th_server_start(&ts) != 0) {
        th_rmtree(tmp);
        FAIL("th_server_start failed");
    }

    th_watchdog_t wd = {0, 30000, "cbm_git_context_resolve under live UI sockets"};
    cbm_thread_t wdt;
    if (cbm_thread_create(&wdt, 0, th_watchdog_fn, &wd) != 0) {
        th_server_stop(&ts);
        th_rmtree(tmp);
        FAIL("watchdog thread create failed");
    }

    /* The exact per-project resolve list_projects runs. Without the fix this
     * spawns git with the AFD handles inherited → NtQueryObject deadlock → never
     * returns → watchdog force-fails the process. */
    cbm_git_context_t ctx = {0};
    int rc = cbm_git_context_resolve(tmp, &ctx);

    wd.done = 1; /* disarm before the watchdog window elapses */
    cbm_thread_join(&wdt);

    int returned = (rc == 0); /* resolve reached its return without hanging */
    cbm_git_context_free(&ctx);
    th_server_stop(&ts);
    th_rmtree(tmp);

    ASSERT_EQ(returned, 1);
    PASS();
}

/* End-to-end companion: the actual endpoint Martin named must answer under UI
 * mode. Drives POST /rpc list_projects against the live server under a watchdog
 * and asserts a 200 within the window. With projects present in the store this
 * also walks the per-project git shell-out end-to-end; with an empty store it
 * still proves the endpoint stays responsive under UI mode. */
TEST(ui_server_rpc_list_projects_responds) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    int port = cbm_http_server_port(ts.srv);

    th_watchdog_t wd = {0, 30000, "POST /rpc list_projects"};
    cbm_thread_t wdt;
    ASSERT_EQ(cbm_thread_create(&wdt, 0, th_watchdog_fn, &wd), 0);

    const char *body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
                       "\"params\":{\"name\":\"list_projects\",\"arguments\":{}}}";
    char req[512];
    snprintf(req, sizeof(req),
             "POST /rpc HTTP/1.1\r\n"
             "Content-Type: application/json\r\n"
             "Content-Length: %d\r\n\r\n%s",
             (int)strlen(body), body);
    char resp[8192];
    int n = th_http(port, req, resp, sizeof(resp));

    wd.done = 1;
    cbm_thread_join(&wdt);
    th_server_stop(&ts);

    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 200);
    ASSERT_NOT_NULL(strstr(resp, "\"jsonrpc\""));
    PASS();
}

/* Deterministic contract guard for the #798 fix: cbm_popen must NOT let a
 * foreign inheritable handle cross into the child it spawns. This is the exact
 * property whose absence caused the deadlock (git inheriting an AFD handle), but
 * it is tested directly — independent of whether the local git-for-Windows build
 * actually hangs on NtQueryObject — so it goes RED on any Windows build without
 * the fix and GREEN with it.
 *
 * Mechanism: create a marker file, opened inheritable, holding a known 16-byte
 * magic. Spawn — through cbm_popen, i.e. the real fixed spawn path — a re-exec
 * of this test binary in its hidden __handle_probe mode. Windows preserves
 * handle values across inheritance, so the child can test whether the parent's
 * handle value is a valid, readable copy of the marker. Without the fix _popen
 * leaks the marker (via cmd.exe) into the probe → it reads the magic → exit 1.
 * With the fix only the stdout pipe is inherited → the probe cannot read it →
 * exit 0. cbm_pclose surfaces that exit code. */
TEST(cbm_popen_isolates_inheritable_handle) {
#ifndef _WIN32
    SKIP_PLATFORM("cbm_popen handle isolation is Windows-specific (POSIX popen uses O_CLOEXEC)");
#else
    char self[1024];
    if (GetModuleFileNameA(NULL, self, sizeof(self)) == 0)
        FAIL("GetModuleFileNameA failed");

    char *tmp = th_mktempdir("cbm_leak798");
    if (!tmp)
        FAIL("th_mktempdir returned NULL");
    char marker[1024];
    snprintf(marker, sizeof(marker), "%s\\marker.bin", tmp);

    static const unsigned char MAGIC[16] = {0xC0, 0xFF, 0xEE, 0x42, 0xDE, 0xAD, 0xBE, 0xEF,
                                             0x13, 0x37, 0xA5, 0x5A, 0x0B, 0xAD, 0xF0, 0x0D};
    char magic_hex[33];
    for (int i = 0; i < 16; i++)
        snprintf(magic_hex + i * 2, 3, "%02x", MAGIC[i]);

    /* Inheritable so the UNFIXED _popen would leak it; the fix must exclude it. */
    SECURITY_ATTRIBUTES sa = {sizeof(sa), NULL, TRUE};
    HANDLE h = CreateFileA(marker, GENERIC_READ | GENERIC_WRITE,
                           FILE_SHARE_READ | FILE_SHARE_WRITE, &sa, CREATE_ALWAYS,
                           FILE_ATTRIBUTE_NORMAL, NULL);
    if (h == INVALID_HANDLE_VALUE) {
        th_rmtree(tmp);
        FAIL("CreateFileA marker failed");
    }
    DWORD wrote = 0;
    WriteFile(h, MAGIC, 16, &wrote, NULL);
    FlushFileBuffers(h);

    /* Double-outer-quote form so cmd.exe parses a possibly-spaced exe path. */
    char cmd[2048];
    snprintf(cmd, sizeof(cmd), "\"\"%s\" __handle_probe %lld %s\"", self,
             (long long)(intptr_t)h, magic_hex);

    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        CloseHandle(h);
        th_rmtree(tmp);
        FAIL("cbm_popen returned NULL");
    }
    char sink[256];
    while (fgets(sink, sizeof(sink), fp)) {
        /* probe writes nothing to stdout; drain defensively */
    }
    int leaked = cbm_pclose(fp); /* 1 iff the probe could read our marker */

    CloseHandle(h);
    th_rmtree(tmp);

    ASSERT_EQ(leaked, 0);
    PASS();
#endif
}

/* ── Suite ────────────────────────────────────────────────────── */

SUITE(httpd) {
    /* Parser / helpers */
    RUN_TEST(httpd_parse_simple_get);
    RUN_TEST(httpd_parse_post_with_body_offset);
    RUN_TEST(httpd_parse_origin_case_insensitive);
    RUN_TEST(httpd_parse_rejects_bare_lf);
    RUN_TEST(httpd_parse_rejects_chunked);
    RUN_TEST(httpd_parse_rejects_oversized_content_length);
    RUN_TEST(httpd_parse_rejects_garbage_content_length);
    RUN_TEST(httpd_parse_rejects_percent00_in_target);
    RUN_TEST(httpd_parse_rejects_raw_nul_in_head);
    RUN_TEST(httpd_parse_incomplete_head_needs_more);
    RUN_TEST(httpd_parse_rejects_missing_version);
    RUN_TEST(httpd_parse_rejects_oversized_head);
    RUN_TEST(httpd_query_param_decode);
    RUN_TEST(httpd_query_param_edge_cases);
    RUN_TEST(httpd_path_match_matrix);
    RUN_TEST(httpd_resolves_bare_binary_path_from_path);

    /* Transport */
    RUN_TEST(httpd_listen_ephemeral_port);
    RUN_TEST(httpd_listen_port_collision_returns_null);

    /* Full UI server */
    RUN_TEST(ui_server_unknown_path_404);
    RUN_TEST(ui_server_root_serves_stub_404);
    RUN_TEST(ui_server_cors_localhost_reflected);
    RUN_TEST(ui_server_cors_evil_origin_not_reflected);
    RUN_TEST(ui_server_rpc_initialize);
    RUN_TEST(ui_server_oversized_body_rejected);
    RUN_TEST(ui_server_encoded_slash_not_routed);
    RUN_TEST(ui_server_nul_in_target_rejected);
    RUN_TEST(ui_server_browse_traversal_probe);
    RUN_TEST(ui_server_ui_config_detects_zh_accept_language);
    RUN_TEST(ui_server_ui_config_prefers_config_lang);
    RUN_TEST(ui_server_slow_request_hits_deadline);
    RUN_TEST(ui_server_access_log_redacts_query);
    RUN_TEST(ui_server_stop_joins_cleanly);

    /* Regression #798: cbm_popen must not leak handles into the git child */
    RUN_TEST(cbm_popen_isolates_inheritable_handle);
    RUN_TEST(ui_server_git_context_under_live_sockets_no_hang);
    RUN_TEST(ui_server_rpc_list_projects_responds);
}
