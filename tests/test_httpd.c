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
#include "../src/daemon/host_internal.h"
#include "../src/git/git_context.h" /* #798 follow-up: live-socket git-resolve repro */
#include "../src/ui/http_server.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "ui/httpd.h"
#include "ui/http_server.h"
#include <store/store.h>
#include <watcher/watcher.h>

#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <sys/stat.h>
#endif

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h> /* #798 follow-up: CreateThread/WaitForSingleObject watchdog */
typedef SOCKET th_sock_t;
#define th_sock_close closesocket
#define th_sock_shutdown(s) shutdown((s), SD_BOTH)
#define TH_SOCK_BAD INVALID_SOCKET
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h> /* struct timeval for the SO_RCVTIMEO watchdog (#798 follow-up) */
#include <sys/wait.h> /* fork/waitpid crash-isolation for the browse overflow guard */
#include <unistd.h>
typedef int th_sock_t;
#define th_sock_close close
#define th_sock_shutdown(s) shutdown((s), SHUT_RDWR)
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

static th_sock_t th_connect_with_recv_buffer(int port, int recv_buffer) {
#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa); /* refcounted; cleanup not needed in tests */
#endif
    th_sock_t s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == TH_SOCK_BAD)
        return TH_SOCK_BAD;
    if (recv_buffer > 0 && setsockopt(s, SOL_SOCKET, SO_RCVBUF, (const char *)&recv_buffer,
                                      sizeof(recv_buffer)) != 0) {
        th_sock_close(s);
        return TH_SOCK_BAD;
    }
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

static th_sock_t th_connect(int port) {
    return th_connect_with_recv_buffer(port, 0);
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

/* One-shot raw HTTP exchange. Returns response length, 0 on connect failure. */
static int th_http_raw(int port, const char *request, char *resp, size_t respsz) {
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

/* Existing route tests focus on endpoint behavior. Add the loopback Host and
 * JSON mutation header the browser supplies. Security tests use th_http_raw()
 * to exercise missing/hostile headers without this convenience layer. */
static char *th_request_with_ui_headers(int port, const char *request) {
    const char *head_end = strstr(request, "\r\n\r\n");
    if (!head_end)
        return strdup(request);

    const char *target = strchr(request, ' ');
    target = target ? target + 1 : NULL;
    bool protected_route =
        target && (strncmp(target, "/api/", 5) == 0 || strncmp(target, "/rpc ", 5) == 0 ||
                   strncmp(target, "/rpc?", 5) == 0);
    bool mutation = strncmp(request, "POST ", 5) == 0;
    bool have_host = strstr(request, "\r\nHost:") != NULL;
    bool have_content_type = strstr(request, "\r\nContent-Type:") != NULL;

    size_t request_len = strlen(request);
    size_t capacity = request_len + 256;
    char *result = malloc(capacity);
    if (!result)
        return NULL;

    size_t head_len = (size_t)(head_end - request);
    memcpy(result, request, head_len);
    size_t pos = head_len;
    if (!have_host)
        pos += (size_t)snprintf(result + pos, capacity - pos, "\r\nHost: 127.0.0.1:%d", port);
    if (protected_route && mutation && !have_content_type) {
        pos += (size_t)snprintf(result + pos, capacity - pos, "\r\nContent-Type: application/json");
    }
    (void)snprintf(result + pos, capacity - pos, "\r\n\r\n%s", head_end + 4);
    return result;
}

static int th_http(int port, const char *request, char *resp, size_t respsz) {
    char *prepared = th_request_with_ui_headers(port, request);
    if (!prepared)
        return 0;
    int result = th_http_raw(port, prepared, resp, respsz);
    free(prepared);
    return result;
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
    ASSERT_EQ(req.http_minor, 1);
    ASSERT_STR_EQ(req.origin, "http://localhost:5173");
    ASSERT_EQ((int)clen, 0);
    ASSERT_EQ((int)body_off, (int)strlen(raw));
    PASS();
}

TEST(httpd_parse_security_headers_and_rejects_duplicates) {
    const char *raw = "POST /rpc HTTP/1.1\r\n"
                      "Host: 127.0.0.1:9749\r\n"
                      "Content-Type: application/json\r\n"
                      "Origin: http://127.0.0.1:9749\r\n"
                      "Content-Length: 0\r\n\r\n";
    cbm_http_req_t req;
    size_t body_off = 0, content_length = 0;
    ASSERT_EQ(cbm_http_parse_head(raw, strlen(raw), &req, &body_off, &content_length), 0);
    ASSERT_STR_EQ(req.host, "127.0.0.1:9749");
    ASSERT_STR_EQ(req.content_type, "application/json");

    static const char *duplicates[] = {
        "GET / HTTP/1.1\r\nHost: localhost\r\nHost: localhost\r\n\r\n",
        "GET / HTTP/1.1\r\nOrigin: http://localhost\r\nOrigin: http://localhost\r\n\r\n",
        ("POST / HTTP/1.1\r\nContent-Type: application/json\r\nContent-Type: "
         "application/json\r\n\r\n"),
    };
    for (size_t i = 0; i < sizeof(duplicates) / sizeof(duplicates[0]); i++) {
        ASSERT_EQ(cbm_http_parse_head(duplicates[i], strlen(duplicates[i]), &req, &body_off,
                                      &content_length),
                  400);
    }
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
    ASSERT_TRUE(cbm_httpd_close(d));
    PASS();
}

TEST(httpd_listen_port_collision_returns_null) {
    cbm_httpd_t *d1 = cbm_httpd_listen(0);
    ASSERT_NOT_NULL(d1);
    cbm_httpd_t *d2 = cbm_httpd_listen(cbm_httpd_port(d1));
    ASSERT_NULL(d2);
    ASSERT_TRUE(cbm_httpd_close(d1));
    PASS();
}

TEST(httpd_close_refuses_while_connection_owns_listener) {
    cbm_httpd_t *listener = cbm_httpd_listen(0);
    ASSERT_NOT_NULL(listener);
    th_sock_t client = th_connect(cbm_httpd_port(listener));
    ASSERT_TRUE(client != TH_SOCK_BAD);
    cbm_http_conn_t *connection = cbm_httpd_accept(listener, 1000);
    ASSERT_NOT_NULL(connection);

    ASSERT_FALSE(cbm_httpd_close(listener));
    cbm_httpd_conn_close(connection);
    th_sock_close(client);
    ASSERT_TRUE(cbm_httpd_close(listener));
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

static int th_server_thread_start(cbm_thread_t *thread, cbm_http_server_t *server) {
    if (!cbm_http_server_schedule_run(server))
        return -1;
    int rc = cbm_thread_create(thread, 0, th_server_thread, server);
    if (rc != 0 && !cbm_http_server_cancel_scheduled_run(server))
        return -1;
    return rc;
}

static int th_server_start(th_server_t *ts) {
    ts->srv = cbm_http_server_new(0);
    if (!ts->srv)
        return -1;
    if (th_server_thread_start(&ts->tid, ts->srv) != 0) {
        (void)cbm_http_server_free(ts->srv);
        return -1;
    }
    return 0;
}

static int th_server_start_with_watcher(th_server_t *ts, cbm_watcher_t *watcher) {
    ts->srv = cbm_http_server_new(0);
    if (!ts->srv)
        return -1;
    cbm_http_server_set_watcher(ts->srv, watcher);
    if (th_server_thread_start(&ts->tid, ts->srv) != 0) {
        (void)cbm_http_server_free(ts->srv);
        return -1;
    }
    return 0;
}

typedef struct {
    atomic_int calls;
    char root_path[512];
    char project_name[256];
} th_ui_index_executor_t;

typedef struct {
    atomic_int calls;
    atomic_int release;
} th_ui_blocking_index_executor_t;

static int th_ui_index_executor(void *opaque, const char *root_path, const char *project_name) {
    th_ui_index_executor_t *executor = opaque;
    snprintf(executor->root_path, sizeof(executor->root_path), "%s", root_path);
    snprintf(executor->project_name, sizeof(executor->project_name), "%s",
             project_name ? project_name : "");
    atomic_fetch_add(&executor->calls, 1);
    return 0;
}

static int th_ui_blocking_index_executor(void *opaque, const char *root_path,
                                         const char *project_name) {
    (void)root_path;
    (void)project_name;
    th_ui_blocking_index_executor_t *executor = opaque;
    atomic_fetch_add(&executor->calls, 1);
    while (!atomic_load(&executor->release))
        cbm_usleep(1000);
    return 0;
}

static bool th_wait_atomic_int(atomic_int *value, int expected, uint32_t timeout_ms) {
    uint64_t deadline = cbm_now_ms() + timeout_ms;
    while (cbm_now_ms() < deadline) {
        if (atomic_load(value) == expected) {
            return true;
        }
        cbm_usleep(1000);
    }
    return atomic_load(value) == expected;
}

static bool th_wait_http_server_activity(cbm_http_server_t *server, cbm_httpd_activity_t expected,
                                         uint32_t timeout_ms) {
    uint64_t deadline = cbm_now_ms() + timeout_ms;
    while (cbm_now_ms() < deadline) {
        if (cbm_http_server_activity_for_test(server) == expected)
            return true;
        cbm_usleep(1000);
    }
    return cbm_http_server_activity_for_test(server) == expected;
}

static bool th_wait_httpd_activity(cbm_httpd_t *listener, cbm_httpd_activity_t expected,
                                   uint32_t timeout_ms) {
    uint64_t deadline = cbm_now_ms() + timeout_ms;
    while (cbm_now_ms() < deadline) {
        if (cbm_httpd_activity_for_test(listener) == expected)
            return true;
        cbm_usleep(1000);
    }
    return cbm_httpd_activity_for_test(listener) == expected;
}

typedef struct {
    atomic_int begin_calls;
    atomic_int end_calls;
    bool allow;
    char begin_project[256];
    char end_project[256];
} th_ui_mutation_guard_t;

static void th_ui_mutation_guard_init(th_ui_mutation_guard_t *guard, bool allow) {
    memset(guard, 0, sizeof(*guard));
    atomic_init(&guard->begin_calls, 0);
    atomic_init(&guard->end_calls, 0);
    guard->allow = allow;
}

static bool th_ui_mutation_begin(void *opaque, const char *project) {
    th_ui_mutation_guard_t *guard = opaque;
    snprintf(guard->begin_project, sizeof(guard->begin_project), "%s", project ? project : "");
    atomic_fetch_add(&guard->begin_calls, 1);
    return guard->allow;
}

static void th_ui_mutation_end(void *opaque, const char *project) {
    th_ui_mutation_guard_t *guard = opaque;
    snprintf(guard->end_project, sizeof(guard->end_project), "%s", project ? project : "");
    atomic_fetch_add(&guard->end_calls, 1);
}

static int th_server_start_with_mutation_guard(th_server_t *ts, cbm_watcher_t *watcher,
                                               th_ui_mutation_guard_t *guard) {
    ts->srv = cbm_http_server_new(0);
    if (!ts->srv)
        return -1;
    if (watcher)
        cbm_http_server_set_watcher(ts->srv, watcher);
    cbm_http_server_set_project_mutation_guard(ts->srv, th_ui_mutation_begin, th_ui_mutation_end,
                                               guard);
    if (th_server_thread_start(&ts->tid, ts->srv) != 0) {
        (void)cbm_http_server_free(ts->srv);
        return -1;
    }
    return 0;
}

static void th_server_stop(th_server_t *ts) {
    cbm_http_server_stop(ts->srv);
    (void)cbm_thread_join(&ts->tid);
    (void)cbm_http_server_free(ts->srv);
}

typedef struct {
    char tmpdir[256];
    char cache_dir[512];
    char root_dir[512];
    char *saved_cache_dir;
    cbm_store_t *store;
    cbm_watcher_t *watcher;
} ui_delete_fixture_t;

static int ui_delete_fixture_init(ui_delete_fixture_t *fx) {
    memset(fx, 0, sizeof(*fx));
    char *tmp = th_mktempdir("cbm_httpd_delete");
    if (!tmp)
        return -1;
    snprintf(fx->tmpdir, sizeof(fx->tmpdir), "%s", tmp);
    snprintf(fx->cache_dir, sizeof(fx->cache_dir), "%s/cache", fx->tmpdir);
    snprintf(fx->root_dir, sizeof(fx->root_dir), "%s/root", fx->tmpdir);

    const char *saved = getenv("CBM_CACHE_DIR");
    fx->saved_cache_dir = saved ? strdup(saved) : NULL;
    if (th_mkdir_p(fx->cache_dir) != 0 || th_mkdir_p(fx->root_dir) != 0) {
        return -1;
    }
    cbm_setenv("CBM_CACHE_DIR", fx->cache_dir, 1);

    fx->store = cbm_store_open_memory();
    fx->watcher = cbm_watcher_new(fx->store, NULL, NULL);
    return fx->store && fx->watcher ? 0 : -1;
}

static void ui_delete_fixture_cleanup(ui_delete_fixture_t *fx) {
    if (fx->watcher)
        cbm_watcher_free(fx->watcher);
    if (fx->store)
        cbm_store_close(fx->store);
    if (fx->saved_cache_dir) {
        cbm_setenv("CBM_CACHE_DIR", fx->saved_cache_dir, 1);
        free(fx->saved_cache_dir);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    th_cleanup(fx->tmpdir);
}

static void ui_delete_db_path(const ui_delete_fixture_t *fx, const char *project, char *out,
                              size_t outsz) {
    snprintf(out, outsz, "%s/%s.db", fx->cache_dir, project);
}

static int ui_delete_make_db_file(const ui_delete_fixture_t *fx, const char *project) {
    char path[1024];
    ui_delete_db_path(fx, project, path, sizeof(path));
    return th_write_file(path, "test db");
}

static int ui_delete_make_sidecars(const ui_delete_fixture_t *fx, const char *project) {
    char path[1024];
    ui_delete_db_path(fx, project, path, sizeof(path));
    char wal[1040], shm[1040];
    snprintf(wal, sizeof(wal), "%s-wal", path);
    snprintf(shm, sizeof(shm), "%s-shm", path);
    return th_write_file(wal, "wal") == 0 && th_write_file(shm, "shm") == 0 ? 0 : -1;
}

static int ui_delete_request(th_server_t *ts, const char *target, char *resp, size_t respsz) {
    char req[512];
    snprintf(req, sizeof(req), "DELETE %s HTTP/1.1\r\n\r\n", target);
    return th_http(cbm_http_server_port(ts->srv), req, resp, respsz);
}

static int ui_adr_post_request(th_server_t *ts, const char *project, const char *content,
                               char *resp, size_t respsz) {
    char body[2048];
    int body_len =
        snprintf(body, sizeof(body), "{\"project\":\"%s\",\"content\":\"%s\"}", project, content);
    if (body_len < 0 || (size_t)body_len >= sizeof(body))
        return 0;

    char req[2304];
    int req_len = snprintf(req, sizeof(req),
                           "POST /api/adr HTTP/1.1\r\n"
                           "Content-Type: application/json\r\n"
                           "Content-Length: %d\r\n\r\n%s",
                           body_len, body);
    if (req_len < 0 || (size_t)req_len >= sizeof(req))
        return 0;
    return th_http(cbm_http_server_port(ts->srv), req, resp, respsz);
}

static int ui_adr_get_request(th_server_t *ts, const char *project, char *resp, size_t respsz) {
    char req[512];
    int req_len = snprintf(req, sizeof(req), "GET /api/adr?project=%s HTTP/1.1\r\n\r\n", project);
    if (req_len < 0 || (size_t)req_len >= sizeof(req))
        return 0;
    return th_http(cbm_http_server_port(ts->srv), req, resp, respsz);
}

static int ui_adr_seed(const ui_delete_fixture_t *fx, const char *project, const char *content) {
    char db_path[1024];
    ui_delete_db_path(fx, project, db_path, sizeof(db_path));
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store)
        return CBM_STORE_ERR;
    int rc = cbm_store_adr_store(store, project, content);
    cbm_store_close(store);
    return rc;
}

static bool ui_adr_equals(const ui_delete_fixture_t *fx, const char *project,
                          const char *expected) {
    char db_path[1024];
    ui_delete_db_path(fx, project, db_path, sizeof(db_path));
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    if (!store)
        return false;

    cbm_adr_t adr = {0};
    int rc = cbm_store_adr_get(store, project, &adr);
    bool equal = rc == CBM_STORE_OK && adr.content && strcmp(adr.content, expected) == 0;
    if (rc == CBM_STORE_OK)
        cbm_store_adr_free(&adr);
    cbm_store_close(store);
    return equal;
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

/* Security regression: process IDs are not cancellation capabilities. The UI
 * must never expose an endpoint that accepts an arbitrary PID and reaches an
 * OS process-termination API (the former Windows path accepted every PID but
 * self). Daemon-owned jobs are cancelled by opaque, owner-bound subscription
 * handles instead, so this legacy route must be completely absent. */
TEST(ui_server_process_kill_route_is_unavailable) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    int port = cbm_http_server_port(ts.srv);

    static const char body[] = "{\"pid\":2147483646}";
    char request[512];
    snprintf(request, sizeof(request),
             "POST /api/process-kill HTTP/1.1\r\n"
             "Content-Type: application/json\r\n"
             "Content-Length: %zu\r\n\r\n%s",
             strlen(body), body);
    char resp[4096];
    int n = th_http(port, request, resp, sizeof(resp));

    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 404);
    ASSERT_NULL(strstr(resp, "\"killed\""));
    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_routes_indexing_through_joinable_daemon_executor) {
    char *root = th_mktempdir("cbm_httpd_daemon_index");
    ASSERT_NOT_NULL(root);
    th_ui_index_executor_t executor = {0};
    atomic_init(&executor.calls, 0);
    th_server_t ts;
    ts.srv = cbm_http_server_new(0);
    ASSERT_NOT_NULL(ts.srv);
    cbm_http_server_set_index_executor(ts.srv, th_ui_index_executor, &executor);
    ASSERT_EQ(th_server_thread_start(&ts.tid, ts.srv), 0);

    char body[1024];
    snprintf(body, sizeof(body), "{\"root_path\":\"%s\",\"project_name\":\"ui-project\"}", root);
    char request[1400];
    snprintf(request, sizeof(request),
             "POST /api/index HTTP/1.1\r\nContent-Type: application/json\r\n"
             "Content-Length: %zu\r\n\r\n%s",
             strlen(body), body);
    char response[4096];
    int response_length =
        th_http(cbm_http_server_port(ts.srv), request, response, sizeof(response));
    bool called = th_wait_atomic_int(&executor.calls, 1, 2000);

    th_server_stop(&ts);
    ASSERT_GT(response_length, 0);
    ASSERT_EQ(th_status(response), 202);
    ASSERT_TRUE(called);
    ASSERT_STR_EQ(executor.root_path, root);
    ASSERT_STR_EQ(executor.project_name, "ui-project");
    th_cleanup(root);
    PASS();
}

TEST(ui_server_free_never_joins_active_index_worker) {
    char *root = th_mktempdir("cbm_httpd_active_index");
    ASSERT_NOT_NULL(root);
    th_ui_blocking_index_executor_t executor = {0};
    atomic_init(&executor.calls, 0);
    atomic_init(&executor.release, 0);

    th_server_t ts;
    ts.srv = cbm_http_server_new(0);
    ASSERT_NOT_NULL(ts.srv);
    cbm_http_server_set_index_executor(ts.srv, th_ui_blocking_index_executor, &executor);
    ASSERT_EQ(th_server_thread_start(&ts.tid, ts.srv), 0);

    char body[1024];
    snprintf(body, sizeof(body), "{\"root_path\":\"%s\",\"project_name\":\"blocked\"}", root);
    char request[1400];
    snprintf(request, sizeof(request),
             "POST /api/index HTTP/1.1\r\nContent-Type: application/json\r\n"
             "Content-Length: %zu\r\n\r\n%s",
             strlen(body), body);
    char response[4096];
    ASSERT_GT(th_http(cbm_http_server_port(ts.srv), request, response, sizeof(response)), 0);
    ASSERT_EQ(th_status(response), 202);
    ASSERT_TRUE(th_wait_atomic_int(&executor.calls, 1, 2000));

    cbm_http_server_stop(ts.srv);
    ASSERT_EQ(cbm_thread_join(&ts.tid), 0);
    uint64_t started = cbm_now_ms();
    ASSERT_FALSE(cbm_http_server_free(ts.srv));
    ASSERT_LT(cbm_now_ms() - started, 1000);

    atomic_store(&executor.release, 1);
    bool freed = false;
    uint64_t deadline = cbm_now_ms() + 2000;
    while (!freed && cbm_now_ms() < deadline) {
        freed = cbm_http_server_free(ts.srv);
        if (!freed)
            cbm_usleep(1000);
    }
    ASSERT_TRUE(freed);
    th_cleanup(root);
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

TEST(ui_server_same_origin_request_is_allowed) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    int port = cbm_http_server_port(ts.srv);
    char req[512];
    snprintf(req, sizeof(req),
             "OPTIONS /rpc HTTP/1.1\r\n"
             "Host: 127.0.0.1:%d\r\n"
             "Origin: http://127.0.0.1:%d\r\n\r\n",
             port, port);
    char resp[4096];
    int n = th_http_raw(port, req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 204);
    char expected_origin[128];
    snprintf(expected_origin, sizeof(expected_origin),
             "Access-Control-Allow-Origin: http://127.0.0.1:%d", port);
    ASSERT_NOT_NULL(strstr(resp, expected_origin));
    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_rejects_foreign_and_null_origins) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    int port = cbm_http_server_port(ts.srv);
    char resp[4096];
    char req[768];
    snprintf(req, sizeof(req),
             "OPTIONS /rpc HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n"
             "Origin: http://evil.example.com\r\n\r\n",
             port);
    int n = th_http_raw(port, req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 403);
    ASSERT_NULL(strstr(resp, "Access-Control-Allow-Origin"));

    snprintf(req, sizeof(req), "GET / HTTP/1.1\r\nHost: localhost:%d\r\nOrigin: null\r\n\r\n",
             port);
    n = th_http_raw(port, req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 403);

    snprintf(req, sizeof(req),
             "GET / HTTP/1.1\r\nHost: localhost:%d\r\n"
             "Origin: http://127.0.0.1:%d\r\n\r\n",
             port, port);
    n = th_http_raw(port, req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 403);

    static const char body[] = "{\"project\":\"victim\",\"content\":\"poison\"}";
    snprintf(req, sizeof(req),
             "POST /api/adr HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n"
             "Origin: http://evil.example.com\r\nContent-Type: text/plain\r\n"
             "Content-Length: %zu\r\n\r\n%s",
             port, strlen(body), body);
    n = th_http_raw(port, req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 403);
    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_mutations_require_json_content_type) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    int port = cbm_http_server_port(ts.srv);
    const char *body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
                       "\"params\":{\"name\":\"list_projects\",\"arguments\":{}}}";
    char req[1024];
    char resp[8192];

    snprintf(req, sizeof(req),
             "POST /rpc HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n"
             "Content-Type: text/plain\r\n"
             "Content-Length: %zu\r\n\r\n%s",
             port, strlen(body), body);
    int n = th_http_raw(port, req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 415);

    snprintf(req, sizeof(req),
             "POST /rpc HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n"
             "Content-Length: %zu\r\n\r\n%s",
             port, strlen(body), body);
    n = th_http_raw(port, req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 415);

    snprintf(req, sizeof(req),
             "POST /rpc HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n"
             "Content-Type: application/json; charset=utf-8\r\n"
             "Content-Length: %zu\r\n\r\n%s",
             port, strlen(body), body);
    n = th_http_raw(port, req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 200);

    th_server_stop(&ts);
    PASS();
}

TEST(ui_server_rpc_allows_only_ui_read_tools) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    const char *body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
                       "\"params\":{\"name\":\"list_projects\",\"arguments\":{}}}";
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

    static const char *blocked_tools[] = {"delete_project", "manage_adr", "ingest_traces",
                                          "index_repository"};
    for (size_t i = 0; i < sizeof(blocked_tools) / sizeof(blocked_tools[0]); i++) {
        char blocked_body[512];
        snprintf(blocked_body, sizeof(blocked_body),
                 "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
                 "\"params\":{\"name\":\"%s\",\"arguments\":{}}}",
                 blocked_tools[i]);
        snprintf(req, sizeof(req),
                 "POST /rpc HTTP/1.1\r\nContent-Type: application/json\r\n"
                 "Content-Length: %zu\r\n\r\n%s",
                 strlen(blocked_body), blocked_body);
        n = th_http(cbm_http_server_port(ts.srv), req, resp, sizeof(resp));
        ASSERT_GT(n, 0);
        ASSERT_EQ(th_status(resp), 403);
    }

    const char *initialize = "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"initialize\","
                             "\"params\":{}}";
    snprintf(req, sizeof(req),
             "POST /rpc HTTP/1.1\r\nContent-Type: application/json\r\n"
             "Content-Length: %zu\r\n\r\n%s",
             strlen(initialize), initialize);
    n = th_http(cbm_http_server_port(ts.srv), req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 403);

    const char *ambiguous = "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\","
                            "\"params\":{\"name\":\"list_projects\",\"name\":\"delete_project\","
                            "\"arguments\":{}}}";
    snprintf(req, sizeof(req),
             "POST /rpc HTTP/1.1\r\nContent-Type: application/json\r\n"
             "Content-Length: %zu\r\n\r\n%s",
             strlen(ambiguous), ambiguous);
    n = th_http(cbm_http_server_port(ts.srv), req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 403);

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

TEST(ui_server_adr_mutation_guard_busy_preserves_existing_adr) {
    static const char *project = "ui-guard-adr-busy";
    static const char *original = "original architecture";
    ui_delete_fixture_t fx;
    ASSERT_EQ(ui_delete_fixture_init(&fx), 0);
    ASSERT_EQ(ui_adr_seed(&fx, project, original), CBM_STORE_OK);

    th_ui_mutation_guard_t guard;
    th_ui_mutation_guard_init(&guard, false);
    th_server_t ts;
    ASSERT_EQ(th_server_start_with_mutation_guard(&ts, NULL, &guard), 0);

    char resp[4096];
    int n = ui_adr_post_request(&ts, project, "replacement architecture", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 423);
    ASSERT_NOT_NULL(strstr(resp, "project is busy; retry after indexing"));
    ASSERT_EQ(atomic_load(&guard.begin_calls), 1);
    ASSERT_EQ(atomic_load(&guard.end_calls), 0);
    ASSERT_STR_EQ(guard.begin_project, project);

    /* A rejected mutation must leave the published DB queryable and unchanged.
     * GET is a query operation and therefore must not enter the mutation guard. */
    n = ui_adr_get_request(&ts, project, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 200);
    ASSERT_NOT_NULL(strstr(resp, "\"has_adr\":true"));
    ASSERT_NOT_NULL(strstr(resp, original));
    ASSERT_EQ(atomic_load(&guard.begin_calls), 1);
    ASSERT_EQ(atomic_load(&guard.end_calls), 0);
    ASSERT_TRUE(ui_adr_equals(&fx, project, original));

    th_server_stop(&ts);
    ui_delete_fixture_cleanup(&fx);
    PASS();
}

TEST(ui_server_adr_mutation_guard_balances_success) {
    static const char *project = "ui-guard-adr-success";
    static const char *content = "coordinated architecture";
    ui_delete_fixture_t fx;
    ASSERT_EQ(ui_delete_fixture_init(&fx), 0);

    th_ui_mutation_guard_t guard;
    th_ui_mutation_guard_init(&guard, true);
    th_server_t ts;
    ASSERT_EQ(th_server_start_with_mutation_guard(&ts, NULL, &guard), 0);

    char resp[4096];
    int n = ui_adr_post_request(&ts, project, content, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 200);
    ASSERT_NOT_NULL(strstr(resp, "{\"saved\":true}"));
    ASSERT_EQ(atomic_load(&guard.begin_calls), 1);
    ASSERT_EQ(atomic_load(&guard.end_calls), 1);
    ASSERT_STR_EQ(guard.begin_project, project);
    ASSERT_STR_EQ(guard.end_project, project);
    ASSERT_TRUE(ui_adr_equals(&fx, project, content));

    th_server_stop(&ts);
    ui_delete_fixture_cleanup(&fx);
    PASS();
}

TEST(ui_server_delete_mutation_guard_busy_preserves_project) {
    static const char *project = "ui-guard-delete-busy";
    ui_delete_fixture_t fx;
    ASSERT_EQ(ui_delete_fixture_init(&fx), 0);
    ASSERT_EQ(ui_delete_make_db_file(&fx, project), 0);
    ASSERT_EQ(ui_delete_make_sidecars(&fx, project), 0);
    cbm_watcher_watch(fx.watcher, project, fx.root_dir);
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 1);

    th_ui_mutation_guard_t guard;
    th_ui_mutation_guard_init(&guard, false);
    th_server_t ts;
    ASSERT_EQ(th_server_start_with_mutation_guard(&ts, fx.watcher, &guard), 0);

    char resp[4096];
    int n = ui_delete_request(&ts, "/api/project?name=ui-guard-delete-busy", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 423);
    ASSERT_NOT_NULL(strstr(resp, "project is busy; retry after indexing"));
    ASSERT_EQ(atomic_load(&guard.begin_calls), 1);
    ASSERT_EQ(atomic_load(&guard.end_calls), 0);
    ASSERT_STR_EQ(guard.begin_project, project);

    char db_path[1024], wal_path[1040], shm_path[1040];
    ui_delete_db_path(&fx, project, db_path, sizeof(db_path));
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    snprintf(shm_path, sizeof(shm_path), "%s-shm", db_path);
    ASSERT_TRUE(cbm_file_exists(db_path));
    ASSERT_TRUE(cbm_file_exists(wal_path));
    ASSERT_TRUE(cbm_file_exists(shm_path));
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 1);

    th_server_stop(&ts);
    ui_delete_fixture_cleanup(&fx);
    PASS();
}

TEST(ui_server_delete_mutation_guard_balances_success) {
    static const char *project = "ui-guard-delete-success";
    ui_delete_fixture_t fx;
    ASSERT_EQ(ui_delete_fixture_init(&fx), 0);
    ASSERT_EQ(ui_delete_make_db_file(&fx, project), 0);
    cbm_watcher_watch(fx.watcher, project, fx.root_dir);
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 1);

    th_ui_mutation_guard_t guard;
    th_ui_mutation_guard_init(&guard, true);
    th_server_t ts;
    ASSERT_EQ(th_server_start_with_mutation_guard(&ts, fx.watcher, &guard), 0);

    char resp[4096];
    int n = ui_delete_request(&ts, "/api/project?name=ui-guard-delete-success", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 200);
    ASSERT_NOT_NULL(strstr(resp, "{\"deleted\":true}"));
    ASSERT_EQ(atomic_load(&guard.begin_calls), 1);
    ASSERT_EQ(atomic_load(&guard.end_calls), 1);
    ASSERT_STR_EQ(guard.begin_project, project);
    ASSERT_STR_EQ(guard.end_project, project);

    char db_path[1024];
    ui_delete_db_path(&fx, project, db_path, sizeof(db_path));
    ASSERT_FALSE(cbm_file_exists(db_path));
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 0);

    th_server_stop(&ts);
    ui_delete_fixture_cleanup(&fx);
    PASS();
}

TEST(ui_server_delete_project_unwatches_after_delete) {
    ui_delete_fixture_t fx;
    ASSERT_EQ(ui_delete_fixture_init(&fx), 0);
    ASSERT_EQ(ui_delete_make_db_file(&fx, "ui-delete-watch"), 0);
    ASSERT_EQ(ui_delete_make_sidecars(&fx, "ui-delete-watch"), 0);
    cbm_watcher_watch(fx.watcher, "ui-delete-watch", fx.root_dir);
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 1);

    th_server_t ts;
    ASSERT_EQ(th_server_start_with_watcher(&ts, fx.watcher), 0);
    char resp[4096];
    int n = ui_delete_request(&ts, "/api/project?name=ui-delete-watch", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 200);
    ASSERT_NOT_NULL(strstr(resp, "{\"deleted\":true}"));

    char db[1024], wal[1040], shm[1040];
    ui_delete_db_path(&fx, "ui-delete-watch", db, sizeof(db));
    snprintf(wal, sizeof(wal), "%s-wal", db);
    snprintf(shm, sizeof(shm), "%s-shm", db);
    ASSERT_FALSE(cbm_file_exists(db));
    ASSERT_FALSE(cbm_file_exists(wal));
    ASSERT_FALSE(cbm_file_exists(shm));
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 0);

    th_server_stop(&ts);
    ui_delete_fixture_cleanup(&fx);
    PASS();
}

TEST(ui_server_delete_project_unwatches_missing_db) {
    ui_delete_fixture_t fx;
    ASSERT_EQ(ui_delete_fixture_init(&fx), 0);
    cbm_watcher_watch(fx.watcher, "ui-delete-missing", fx.root_dir);
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 1);

    th_server_t ts;
    ASSERT_EQ(th_server_start_with_watcher(&ts, fx.watcher), 0);
    char resp[4096];
    int n = ui_delete_request(&ts, "/api/project?name=ui-delete-missing", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 404);
    ASSERT_NOT_NULL(strstr(resp, "{\"error\":\"project not found\"}"));
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 0);

    th_server_stop(&ts);
    ui_delete_fixture_cleanup(&fx);
    PASS();
}

TEST(ui_server_delete_project_no_watcher_still_deletes) {
    ui_delete_fixture_t fx;
    ASSERT_EQ(ui_delete_fixture_init(&fx), 0);
    ASSERT_EQ(ui_delete_make_db_file(&fx, "ui-delete-no-watcher"), 0);

    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    char resp[4096];
    int n = ui_delete_request(&ts, "/api/project?name=ui-delete-no-watcher", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 200);

    char db[1024];
    ui_delete_db_path(&fx, "ui-delete-no-watcher", db, sizeof(db));
    ASSERT_FALSE(cbm_file_exists(db));

    th_server_stop(&ts);
    ui_delete_fixture_cleanup(&fx);
    PASS();
}

TEST(ui_server_delete_project_missing_name_keeps_watch) {
    ui_delete_fixture_t fx;
    ASSERT_EQ(ui_delete_fixture_init(&fx), 0);
    cbm_watcher_watch(fx.watcher, "ui-delete-still-watched", fx.root_dir);
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 1);

    th_server_t ts;
    ASSERT_EQ(th_server_start_with_watcher(&ts, fx.watcher), 0);
    char resp[4096];
    int n = ui_delete_request(&ts, "/api/project", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 400);
    ASSERT_NOT_NULL(strstr(resp, "{\"error\":\"missing name\"}"));
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 1);

    th_server_stop(&ts);
    ui_delete_fixture_cleanup(&fx);
    PASS();
}

TEST(ui_server_delete_project_invalid_name_keeps_watch) {
    ui_delete_fixture_t fx;
    ASSERT_EQ(ui_delete_fixture_init(&fx), 0);
    cbm_watcher_watch(fx.watcher, "bad/name", fx.root_dir);
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 1);

    th_server_t ts;
    ASSERT_EQ(th_server_start_with_watcher(&ts, fx.watcher), 0);
    char resp[4096];
    int n = ui_delete_request(&ts, "/api/project?name=bad%2Fname", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 404);
    ASSERT_NOT_NULL(strstr(resp, "{\"error\":\"project not found\"}"));
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 1);

    th_server_stop(&ts);
    ui_delete_fixture_cleanup(&fx);
    PASS();
}

TEST(ui_server_delete_project_unlink_failure_keeps_watch) {
    ui_delete_fixture_t fx;
    ASSERT_EQ(ui_delete_fixture_init(&fx), 0);
    char db[1024];
    ui_delete_db_path(&fx, "ui-delete-unlink-fails", db, sizeof(db));
    ASSERT_EQ(th_mkdir_p(db), 0);
    cbm_watcher_watch(fx.watcher, "ui-delete-unlink-fails", fx.root_dir);
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 1);

    th_server_t ts;
    ASSERT_EQ(th_server_start_with_watcher(&ts, fx.watcher), 0);
    char resp[4096];
    int n = ui_delete_request(&ts, "/api/project?name=ui-delete-unlink-fails", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 500);
    ASSERT_NOT_NULL(strstr(resp, "{\"error\":\"failed to delete\"}"));
    ASSERT_TRUE(cbm_file_exists(db));
    ASSERT_EQ(cbm_watcher_watch_count(fx.watcher), 1);

    th_server_stop(&ts);
    ui_delete_fixture_cleanup(&fx);
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

TEST(ui_server_free_refuses_active_loop) {
    cbm_http_server_t *server = cbm_http_server_new(0);
    ASSERT_NOT_NULL(server);

    cbm_thread_t thread;
    ASSERT_EQ(th_server_thread_start(&thread, server), 0);
    char response[512];
    ASSERT_GT(
        th_http(cbm_http_server_port(server), "GET / HTTP/1.1\r\n\r\n", response, sizeof(response)),
        0);
    ASSERT_FALSE(cbm_http_server_free(server));
    cbm_http_server_stop(server);
    ASSERT_EQ(cbm_thread_join(&thread), 0);
    ASSERT_TRUE(cbm_http_server_free(server));
    PASS();
}

TEST(ui_server_free_refuses_scheduled_run_before_child_starts) {
    cbm_http_server_t *server = cbm_http_server_new(0);
    ASSERT_NOT_NULL(server);

    ASSERT_TRUE(cbm_http_server_schedule_run(server));
    ASSERT_FALSE(cbm_http_server_free(server));
    ASSERT_TRUE(cbm_http_server_cancel_scheduled_run(server));
    ASSERT_TRUE(cbm_http_server_free(server));
    PASS();
}

TEST(daemon_host_http_thread_create_failure_cancels_scheduled_run) {
    ASSERT_TRUE(cbm_daemon_host_http_thread_create_failure_lifecycle_for_test());
    PASS();
}

typedef struct {
    th_sock_t socket;
    atomic_int *operation_finished;
    atomic_int stop_finished;
    atomic_int watchdog_fired;
} th_http_stop_watchdog_t;

static void *th_http_stop_watchdog(void *opaque) {
    th_http_stop_watchdog_t *watchdog = opaque;
    /* Hang detector, not a latency assertion: sized for the slowest
     * sanitizer/loaded-runner tail (see AGENTS.md, CI determinism). */
    for (int elapsed_ms = 0; elapsed_ms < 10000; elapsed_ms += 10) {
        if (atomic_load(&watchdog->stop_finished) ||
            (watchdog->operation_finished && atomic_load(watchdog->operation_finished)))
            return NULL;
        cbm_usleep(10 * 1000);
    }
    atomic_store(&watchdog->watchdog_fired, 1);
    (void)th_sock_shutdown(watchdog->socket);
    return NULL;
}

typedef struct {
    cbm_httpd_t *listener;
    atomic_int accepted;
    atomic_int finished;
} th_httpd_large_reply_t;

static void *th_httpd_large_reply(void *opaque) {
    th_httpd_large_reply_t *reply = opaque;
    cbm_http_conn_t *connection = cbm_httpd_accept(reply->listener, 3000);
    if (!connection) {
        atomic_store(&reply->finished, 1);
        return NULL;
    }
    atomic_store(&reply->accepted, 1);
    size_t response_size = 8U * 1024U * 1024U;
    char *response = malloc(response_size);
    if (response) {
        memset(response, 'R', response_size);
        cbm_http_reply_buf(connection, 200, "Content-Type: application/octet-stream\r\n", response,
                           response_size);
        free(response);
    }
    cbm_httpd_conn_close(connection);
    atomic_store(&reply->finished, 1);
    return NULL;
}

TEST(httpd_interrupt_unblocks_nonreading_large_response_within_one_second) {
    cbm_httpd_t *listener = cbm_httpd_listen(0);
    ASSERT_NOT_NULL(listener);
    cbm_httpd_set_send_buffer_for_test(listener, 64 * 1024);
    /* Deadline pinned far out of reach: the only remaining way the blocked
     * 8 MB send can end is the interrupt, so the join itself proves
     * interrupt causality — no wall-clock discrimination needed (the old
     * elapsed<500ms check was a lottery under sanitizer slowdown). */
    cbm_httpd_set_send_deadline_for_test(listener, 60 * 1000);
    th_httpd_large_reply_t reply = {.listener = listener};
    atomic_init(&reply.accepted, 0);
    atomic_init(&reply.finished, 0);

    th_sock_t socket = th_connect_with_recv_buffer(cbm_httpd_port(listener), 1024);
    ASSERT_TRUE(socket != TH_SOCK_BAD);
    cbm_thread_t reply_thread;
    ASSERT_EQ(cbm_thread_create(&reply_thread, 0, th_httpd_large_reply, &reply), 0);

    ASSERT_TRUE(th_wait_httpd_activity(listener, CBM_HTTPD_ACTIVITY_RESPONDING, 1000));
    ASSERT_EQ(atomic_load(&reply.accepted), 1);
    ASSERT_EQ(atomic_load(&reply.finished), 0);

    th_http_stop_watchdog_t watchdog = {
        .socket = socket,
        .operation_finished = &reply.finished,
    };
    atomic_init(&watchdog.stop_finished, 0);
    atomic_init(&watchdog.watchdog_fired, 0);
    cbm_thread_t watchdog_thread;
    ASSERT_EQ(cbm_thread_create(&watchdog_thread, 0, th_http_stop_watchdog, &watchdog), 0);

    cbm_httpd_interrupt(listener);
    ASSERT_EQ(cbm_thread_join(&reply_thread), 0);
    atomic_store(&watchdog.stop_finished, 1);
    ASSERT_EQ(cbm_thread_join(&watchdog_thread), 0);
    (void)th_sock_shutdown(socket);
    th_sock_close(socket);
    ASSERT_TRUE(cbm_httpd_close(listener));

    /* The 60 s deadline cannot have fired, so the join above is the proof
     * of interrupt delivery; the watchdog is purely a hang detector. */
    ASSERT_EQ(atomic_load(&watchdog.watchdog_fired), 0);
    PASS();
}

TEST(httpd_nonreading_large_response_hits_send_deadline_without_interrupt) {
    cbm_httpd_t *listener = cbm_httpd_listen(0);
    ASSERT_NOT_NULL(listener);
    cbm_httpd_set_send_buffer_for_test(listener, 64 * 1024);
    th_httpd_large_reply_t reply = {.listener = listener};
    atomic_init(&reply.accepted, 0);
    atomic_init(&reply.finished, 0);

    th_sock_t socket = th_connect_with_recv_buffer(cbm_httpd_port(listener), 1024);
    ASSERT_TRUE(socket != TH_SOCK_BAD);
    cbm_thread_t reply_thread;
    ASSERT_EQ(cbm_thread_create(&reply_thread, 0, th_httpd_large_reply, &reply), 0);
    ASSERT_TRUE(th_wait_httpd_activity(listener, CBM_HTTPD_ACTIVITY_RESPONDING, 1000));
    ASSERT_EQ(atomic_load(&reply.accepted), 1);
    ASSERT_EQ(atomic_load(&reply.finished), 0);
    uint64_t started = cbm_now_ms();

    th_http_stop_watchdog_t watchdog = {
        .socket = socket,
        .operation_finished = &reply.finished,
    };
    atomic_init(&watchdog.stop_finished, 0);
    atomic_init(&watchdog.watchdog_fired, 0);
    cbm_thread_t watchdog_thread;
    ASSERT_EQ(cbm_thread_create(&watchdog_thread, 0, th_http_stop_watchdog, &watchdog), 0);

    ASSERT_EQ(cbm_thread_join(&reply_thread), 0);
    uint64_t elapsed = cbm_now_ms() - started;
    atomic_store(&watchdog.stop_finished, 1);
    ASSERT_EQ(cbm_thread_join(&watchdog_thread), 0);
    th_sock_close(socket);
    ASSERT_TRUE(cbm_httpd_close(listener));

    ASSERT_GTE(elapsed, 500);
    ASSERT_EQ(atomic_load(&watchdog.watchdog_fired), 0);
    PASS();
}

TEST(ui_server_stop_interrupts_partial_request_within_one_second) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    cbm_http_server_set_recv_deadline_ms(ts.srv, 3000);
    int port = cbm_http_server_port(ts.srv);
    th_sock_t socket = th_connect(port);
    ASSERT_TRUE(socket != TH_SOCK_BAD);

    char partial[256];
    snprintf(partial, sizeof(partial), "GET /api/logs HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n", port);
    ASSERT_EQ(th_send_all(socket, partial, strlen(partial)), 0);
    ASSERT_TRUE(th_wait_http_server_activity(ts.srv, CBM_HTTPD_ACTIVITY_READING_REQUEST, 1000));

    th_http_stop_watchdog_t watchdog = {.socket = socket};
    atomic_init(&watchdog.stop_finished, 0);
    atomic_init(&watchdog.watchdog_fired, 0);
    cbm_thread_t watchdog_thread;
    ASSERT_EQ(cbm_thread_create(&watchdog_thread, 0, th_http_stop_watchdog, &watchdog), 0);

    uint64_t started = cbm_now_ms();
    cbm_http_server_stop(ts.srv);
    ASSERT_EQ(cbm_thread_join(&ts.tid), 0);
    uint64_t elapsed = cbm_now_ms() - started;
    atomic_store(&watchdog.stop_finished, 1);
    ASSERT_EQ(cbm_thread_join(&watchdog_thread), 0);
    (void)th_sock_shutdown(socket);
    th_sock_close(socket);
    ASSERT_TRUE(cbm_http_server_free(ts.srv));

    ASSERT_LT(elapsed, 1000);
    ASSERT_EQ(atomic_load(&watchdog.watchdog_fired), 0);
    PASS();
}

/* ── /api/repo-info git-remote URL helpers (distilled from PR #789) ── */

/* The web base must always be https (deep-links can't be downgraded) and must
 * never carry embedded credentials, across scp / ssh / https remote shapes. */
TEST(repo_info_web_base_normalizes_to_https) {
    struct {
        const char *in;
        const char *want;
    } cases[] = {
        {"git@github.com:org/repo.git", "https://github.com/org/repo"},
        {"git@github.com:org/repo", "https://github.com/org/repo"},
        {"https://github.com/org/repo.git", "https://github.com/org/repo"},
        {"ssh://git@github.com/org/repo.git", "https://github.com/org/repo"},
        {"https://user:token@github.com/org/repo.git", "https://github.com/org/repo"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char *got = cbm_ui_git_web_base(cases[i].in);
        ASSERT_NOT_NULL(got);
        ASSERT_STR_EQ(got, cases[i].want);
        /* Never leak credentials into the web base. */
        ASSERT_NULL(strstr(got, "token"));
        ASSERT_NULL(strstr(got, "@"));
        free(got);
    }
    /* Unrecognized shapes yield NULL, not a bogus link. */
    ASSERT_NULL(cbm_ui_git_web_base(""));
    ASSERT_NULL(cbm_ui_git_web_base("not-a-url"));
    PASS();
}

/* The remote_url field echoed to the client must have any user:pass@ stripped. */
TEST(repo_info_strips_credentials_from_remote) {
    char *safe = cbm_ui_git_strip_credentials("https://alice:s3cr3t@github.com/org/repo.git");
    ASSERT_NOT_NULL(safe);
    ASSERT_STR_EQ(safe, "https://github.com/org/repo.git");
    ASSERT_NULL(strstr(safe, "s3cr3t"));
    ASSERT_NULL(strstr(safe, "alice"));
    free(safe);

    /* Credential-free URLs pass through unchanged. */
    char *plain = cbm_ui_git_strip_credentials("https://github.com/org/repo.git");
    ASSERT_NOT_NULL(plain);
    ASSERT_STR_EQ(plain, "https://github.com/org/repo.git");
    free(plain);

    /* An '@' in the path (not the authority) must not be treated as creds. */
    char *pathat = cbm_ui_git_strip_credentials("https://github.com/org/repo/@scope");
    ASSERT_NOT_NULL(pathat);
    ASSERT_STR_EQ(pathat, "https://github.com/org/repo/@scope");
    free(pathat);

    /* scp-style carries no secret and is left intact. */
    char *scp = cbm_ui_git_strip_credentials("git@github.com:org/repo.git");
    ASSERT_NOT_NULL(scp);
    ASSERT_STR_EQ(scp, "git@github.com:org/repo.git");
    free(scp);

    ASSERT_NULL(cbm_ui_git_strip_credentials(NULL));
    PASS();
}

/* ── #798 follow-up: full UI-mode hang repro (live sockets) ───── */

/* Like th_http but arms a client-side receive-timeout watchdog. If the
 * single-threaded server wedges, recv() returns instead of blocking forever, so
 * the test FAILs deterministically rather than hanging CI. 0 on connect/timeout. */
static int th_http_deadline(int port, const char *request, char *resp, size_t respsz,
                            int timeout_ms) {
    char *prepared = th_request_with_ui_headers(port, request);
    if (!prepared)
        return 0;
    th_sock_t s = th_connect(port);
    if (s == TH_SOCK_BAD) {
        free(prepared);
        return 0;
    }
#ifdef _WIN32
    DWORD tv = (DWORD)timeout_ms;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
#else
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif
    if (th_send_all(s, prepared, strlen(prepared)) != 0) {
        free(prepared);
        th_sock_close(s);
        return 0;
    }
    free(prepared);
    int n = th_recv_until_close(s, resp, respsz);
    th_sock_close(s);
    return n;
}

/* #798 was a single-threaded-server wedge: list_projects never returned and the
 * whole UI stopped answering. Assert the running server answers list_projects
 * within a hard deadline while it holds live listening sockets. The client
 * receive-timeout is the watchdog: a wedge → no 200 → FAIL, never a CI hang. */
TEST(ui_server_list_projects_responds_under_watchdog) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    const char *body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
                       "\"params\":{\"name\":\"list_projects\",\"arguments\":{}}}";
    char req[512];
    snprintf(req, sizeof(req),
             "POST /rpc HTTP/1.1\r\n"
             "Content-Type: application/json\r\n"
             "Content-Length: %d\r\n\r\n%s",
             (int)strlen(body), body);
    char resp[8192];
    int n = th_http_deadline(cbm_http_server_port(ts.srv), req, resp, sizeof(resp), 15000);
    th_server_stop(&ts);
    ASSERT_GT(n, 0); /* a response arrived before the watchdog fired */
    ASSERT_EQ(th_status(resp), 200);
    ASSERT_NOT_NULL(strstr(resp, "\"jsonrpc\""));
    PASS();
}

#ifdef _WIN32
typedef struct {
    char path[512];
    int resolved_ok;
} th_gitctx_probe_t;

static DWORD WINAPI th_gitctx_probe_thread(LPVOID arg) {
    th_gitctx_probe_t *p = (th_gitctx_probe_t *)arg;
    cbm_git_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    int rc = cbm_git_context_resolve(p->path, &ctx);
    p->resolved_ok = (rc == 0 && ctx.is_git) ? 1 : 0;
    cbm_git_context_free(&ctx);
    return 0;
}
#endif

/* The load-bearing end-to-end repro of #798: while the single-threaded UI server
 * holds LIVE listening/AFD socket handles in this process, cbm_git_context_resolve
 * — the exact path list_projects runs (add_git_context_json → resolve →
 * cbm_popen(git)) — must not hang. Under a raw-_popen regression git inherits
 * those sockets and its MSYS2 runtime deadlocks in NtQueryObject; the watchdog
 * turns that into a hard FAIL instead of an infinite hang. */
TEST(git_context_resolve_no_hang_under_live_ui_sockets) {
#ifndef _WIN32
    SKIP_PLATFORM("Windows-only: #798 UI listening-socket handle inheritance");
#else
    char *tmp = th_mktempdir("cbm_798repro");
    if (!tmp)
        FAIL("th_mktempdir returned NULL");

    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" init -q && git -C \"%s\" -c user.email=t@t -c user.name=t "
             "commit -q --allow-empty -m init",
             tmp, tmp);
    if (system(cmd) != 0) {
        th_rmtree(tmp);
        SKIP_PLATFORM("git not available to init a repo");
    }

    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);

    th_gitctx_probe_t *probe = (th_gitctx_probe_t *)calloc(1, sizeof(*probe));
    ASSERT_NOT_NULL(probe);
    snprintf(probe->path, sizeof(probe->path), "%s", tmp);

    HANDLE h = CreateThread(NULL, 0, th_gitctx_probe_thread, probe, 0, NULL);
    ASSERT_NOT_NULL(h);
    DWORD w = WaitForSingleObject(h, 30000);
    if (w != WAIT_OBJECT_0) {
        /* Wedged on the inherited-socket NtQueryObject walk. Deliberately leak
         * the heap probe + thread (a late wake must not touch freed memory);
         * process exit reaps them. Fail loudly rather than hang CI. */
        th_server_stop(&ts);
        FAIL("cbm_git_context_resolve hung under live UI sockets (#798 regression)");
    }
    CloseHandle(h);
    th_server_stop(&ts);
    int ok = probe->resolved_ok;
    free(probe);
    th_rmtree(tmp);
    ASSERT_EQ(ok, 1);
    PASS();
#endif
}

/* Host is part of the authority boundary: HTTP/1.1 requires exactly one, and
 * the optional port must be the actual bound port. This blocks DNS rebinding
 * and prevents a foreign localhost service from manufacturing same-origin
 * requests for this daemon. */
TEST(ui_server_rejects_non_loopback_host) {
    th_server_t ts;
    ASSERT_EQ(th_server_start(&ts), 0);
    int port = cbm_http_server_port(ts.srv);
    char resp[4096];

    int n = th_http(port, "GET / HTTP/1.1\r\nHost: evil.example.com\r\n\r\n", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 403);

    n = th_http_raw(port, "GET / HTTP/1.1\r\n\r\n", resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 400);

    static const char *bare_loopback_authorities[] = {
        "127.0.0.1",
        "localhost",
        "[::1]",
    };
    char req[256];
    for (size_t i = 0; i < sizeof(bare_loopback_authorities) / sizeof(bare_loopback_authorities[0]);
         i++) {
        snprintf(req, sizeof(req), "GET / HTTP/1.1\r\nHost: %s\r\n\r\n",
                 bare_loopback_authorities[i]);
        n = th_http_raw(port, req, resp, sizeof(resp));
        ASSERT_GT(n, 0);
        ASSERT_EQ(th_status(resp), 403);
    }

    snprintf(req, sizeof(req), "GET / HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n\r\n", port + 1);
    n = th_http_raw(port, req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_EQ(th_status(resp), 403);

    snprintf(req, sizeof(req), "GET / HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n\r\n", port);
    n = th_http_raw(port, req, resp, sizeof(resp));
    ASSERT_GT(n, 0);
    ASSERT_NEQ(th_status(resp), 400);
    ASSERT_NEQ(th_status(resp), 403);

    static const char *bare_origins[] = {
        "http://127.0.0.1",
        "http://localhost",
        "http://[::1]",
    };
    for (size_t i = 0; i < sizeof(bare_origins) / sizeof(bare_origins[0]); i++) {
        snprintf(req, sizeof(req), "GET / HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nOrigin: %s\r\n\r\n",
                 port, bare_origins[i]);
        n = th_http_raw(port, req, resp, sizeof(resp));
        ASSERT_GT(n, 0);
        ASSERT_EQ(th_status(resp), 403);
    }

    th_server_stop(&ts);
    PASS();
}

/* The directory browser formats readdir() entries into a fixed 32 KB response
 * buffer. The per-entry loop is clamped, but the trailing "parent"/"roots"
 * appends were not — once the entries filled the buffer, pos ran past the end
 * and the next size argument wrapped, writing out of bounds. Fill the buffer
 * with many long-named subdirectories and browse it in a forked child so an
 * overflow surfaces as a killing signal (ASan abort) rather than a clean run. */
TEST(ui_server_browse_wide_dir_no_overflow) {
#ifdef _WIN32
    SKIP_PLATFORM("fork crash-isolation is POSIX-only; the clamp is platform-agnostic");
#else
    char *dir = th_mktempdir("cbm_browse");
    if (!dir) {
        FAIL("mktempdir");
    }
    char longname[240];
    memset(longname, 'a', sizeof(longname) - 1);
    longname[sizeof(longname) - 1] = '\0';
    for (int i = 0; i < 250; i++) { /* 250 * ~220 chars overflows the 32 KB buffer */
        char sub[600];
        snprintf(sub, sizeof(sub), "%s/%s%03d", dir, longname, i);
        th_mkdir_p(sub);
    }
    fflush(NULL);
    pid_t pid = fork();
    if (pid == 0) {
        th_server_t ts;
        if (th_server_start(&ts) != 0) {
            _exit(2);
        }
        char req[512];
        int port = cbm_http_server_port(ts.srv);
        snprintf(req, sizeof(req), "GET /api/browse?path=%s HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n\r\n",
                 dir, port);
        char *resp = malloc(262144);
        int n = resp ? th_http(port, req, resp, 262144) : 0;
        int ok = (n > 0 && strstr(resp, "HTTP/1.1 200") != NULL);
        free(resp);
        th_server_stop(&ts);
        _exit(ok ? 0 : 3);
    }
    ASSERT_TRUE(pid > 0);
    int status = 0;
    (void)waitpid(pid, &status, 0);
    char rm[600];
    snprintf(rm, sizeof(rm), "rm -rf '%s'", dir);
    (void)system(rm);
    if (WIFSIGNALED(status)) {
        char m[96];
        snprintf(m, sizeof(m), "browse killed by signal %d — response buffer overflow",
                 WTERMSIG(status));
        FAIL(m);
    }
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    PASS();
#endif
}

/* ── Suite ────────────────────────────────────────────────────── */

SUITE(httpd) {
    RUN_TEST(ui_server_browse_wide_dir_no_overflow);
    /* Parser / helpers */
    RUN_TEST(httpd_parse_simple_get);
    RUN_TEST(httpd_parse_security_headers_and_rejects_duplicates);
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
    RUN_TEST(repo_info_web_base_normalizes_to_https);
    RUN_TEST(repo_info_strips_credentials_from_remote);

    /* Transport */
    RUN_TEST(httpd_listen_ephemeral_port);
    RUN_TEST(httpd_listen_port_collision_returns_null);
    RUN_TEST(httpd_close_refuses_while_connection_owns_listener);

    /* Full UI server */
    RUN_TEST(ui_server_rejects_non_loopback_host);
    RUN_TEST(ui_server_unknown_path_404);
    RUN_TEST(ui_server_process_kill_route_is_unavailable);
    RUN_TEST(ui_server_routes_indexing_through_joinable_daemon_executor);
    RUN_TEST(ui_server_free_never_joins_active_index_worker);
    RUN_TEST(ui_server_root_serves_stub_404);
    RUN_TEST(ui_server_same_origin_request_is_allowed);
    RUN_TEST(ui_server_rejects_foreign_and_null_origins);
    RUN_TEST(ui_server_mutations_require_json_content_type);
    RUN_TEST(ui_server_rpc_allows_only_ui_read_tools);
    RUN_TEST(ui_server_oversized_body_rejected);
    RUN_TEST(ui_server_encoded_slash_not_routed);
    RUN_TEST(ui_server_nul_in_target_rejected);
    RUN_TEST(ui_server_browse_traversal_probe);
    RUN_TEST(ui_server_adr_mutation_guard_busy_preserves_existing_adr);
    RUN_TEST(ui_server_adr_mutation_guard_balances_success);
    RUN_TEST(ui_server_delete_mutation_guard_busy_preserves_project);
    RUN_TEST(ui_server_delete_mutation_guard_balances_success);
    RUN_TEST(ui_server_delete_project_unwatches_after_delete);
    RUN_TEST(ui_server_delete_project_unwatches_missing_db);
    RUN_TEST(ui_server_delete_project_no_watcher_still_deletes);
    RUN_TEST(ui_server_delete_project_missing_name_keeps_watch);
    RUN_TEST(ui_server_delete_project_invalid_name_keeps_watch);
    RUN_TEST(ui_server_delete_project_unlink_failure_keeps_watch);
    RUN_TEST(ui_server_ui_config_detects_zh_accept_language);
    RUN_TEST(ui_server_ui_config_prefers_config_lang);
    RUN_TEST(ui_server_slow_request_hits_deadline);
    RUN_TEST(ui_server_access_log_redacts_query);
    RUN_TEST(ui_server_stop_joins_cleanly);
    RUN_TEST(ui_server_free_refuses_active_loop);
    RUN_TEST(ui_server_free_refuses_scheduled_run_before_child_starts);
    RUN_TEST(daemon_host_http_thread_create_failure_cancels_scheduled_run);
    RUN_TEST(httpd_interrupt_unblocks_nonreading_large_response_within_one_second);
    RUN_TEST(httpd_nonreading_large_response_hits_send_deadline_without_interrupt);
    RUN_TEST(ui_server_stop_interrupts_partial_request_within_one_second);
    /* #798 follow-up: full UI-mode hang repro under live sockets */
    RUN_TEST(ui_server_list_projects_responds_under_watchdog);
    RUN_TEST(git_context_resolve_no_hang_under_live_ui_sockets);
}
