/*
 * neo4j_http.c — Minimal HTTP/1.1 client for the Neo4j HTTP API.
 */
#include "neo4j/neo4j_http.h"

#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <winhttp.h>
typedef SOCKET cbm_n4_sock_t;
#define N4_SOCK_BAD INVALID_SOCKET
#define n4_sock_close closesocket
#else
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
typedef int cbm_n4_sock_t;
#define N4_SOCK_BAD (-1)
#define n4_sock_close close
#endif

#ifdef MSG_NOSIGNAL
#define N4_SEND_FLAGS MSG_NOSIGNAL
#else
#define N4_SEND_FLAGS 0
#endif

#define N4_DEFAULT_HTTP_PORT 7474
#define N4_DEFAULT_HTTPS_PORT 7473
#define N4_IO_TIMEOUT_MS 120000
#define N4_HDR_MAX (64U * 1024U)
#define N4_RESP_MAX ((size_t)1 << 30) /* 1 GiB: refuse anything larger */

struct cbm_neo4j_http {
    cbm_neo4j_url_t url;
    char *auth_header; /* "Basic xxx" or NULL */
    cbm_n4_sock_t sock;
    /* Bytes read past the end of the previous response (keep-alive). */
    char *carry;
    size_t carry_len;
#ifdef _WIN32
    HINTERNET session;
    HINTERNET connect;
#endif
};

static void n4_err(char *err, size_t err_sz, const char *fmt, const char *arg) {
    if (err && err_sz > 0) {
        (void)snprintf(err, err_sz, fmt, arg ? arg : "");
    }
}

/* ── URL / base64 ───────────────────────────────────────────────── */

static bool n4_has_prefix_ci(const char *s, const char *p) {
    for (; *p; s++, p++) {
        if (tolower((unsigned char)*s) != tolower((unsigned char)*p)) {
            return false;
        }
    }
    return true;
}

int cbm_neo4j_url_parse(const char *url, cbm_neo4j_url_t *out, char *err, size_t err_sz) {
    if (!url || !out || !*url) {
        n4_err(err, err_sz, "graph_db URL is empty%s", NULL);
        return -1;
    }
    memset(out, 0, sizeof(*out));
    const char *p = NULL;
    if (n4_has_prefix_ci(url, "http://")) {
        p = url + strlen("http://");
        out->port = N4_DEFAULT_HTTP_PORT;
    } else if (n4_has_prefix_ci(url, "https://")) {
        p = url + strlen("https://");
        out->tls = true;
        out->port = N4_DEFAULT_HTTPS_PORT;
    } else if (n4_has_prefix_ci(url, "bolt") || n4_has_prefix_ci(url, "neo4j")) {
        n4_err(err, err_sz,
               "unsupported graph_db scheme in '%s': codebase-memory-mcp talks to Neo4j over its "
               "HTTP API; use http://<host>:7474 (or https://<host>:7473) instead of bolt/neo4j",
               url);
        return -1;
    } else {
        n4_err(err, err_sz, "graph_db URL must start with http:// or https:// (got '%s')", url);
        return -1;
    }

    const char *host_end = p;
    if (*p == '[') { /* IPv6 literal */
        host_end = strchr(p, ']');
        if (!host_end) {
            n4_err(err, err_sz, "malformed IPv6 host in '%s'", url);
            return -1;
        }
        size_t hl = (size_t)(host_end - (p + 1));
        if (hl == 0 || hl >= sizeof(out->host)) {
            n4_err(err, err_sz, "bad host in '%s'", url);
            return -1;
        }
        memcpy(out->host, p + 1, hl);
        host_end++;
    } else {
        while (*host_end && *host_end != ':' && *host_end != '/') {
            host_end++;
        }
        size_t hl = (size_t)(host_end - p);
        if (hl == 0 || hl >= sizeof(out->host)) {
            n4_err(err, err_sz, "bad host in '%s'", url);
            return -1;
        }
        memcpy(out->host, p, hl);
    }
    p = host_end;
    if (*p == ':') {
        p++;
        char *end = NULL;
        long port = strtol(p, &end, 10);
        if (end == p || port <= 0 || port > 65535) {
            n4_err(err, err_sz, "bad port in '%s'", url);
            return -1;
        }
        out->port = (int)port;
        p = end;
    }
    if (*p && *p != '/') {
        n4_err(err, err_sz, "malformed graph_db URL '%s'", url);
        return -1;
    }
    size_t pl = strlen(p);
    while (pl > 0 && p[pl - 1] == '/') {
        pl--;
    }
    if (pl >= sizeof(out->base_path)) {
        n4_err(err, err_sz, "URL path too long in '%s'", url);
        return -1;
    }
    memcpy(out->base_path, p, pl);
    out->base_path[pl] = '\0';
    return 0;
}

char *cbm_neo4j_base64(const char *data, size_t len) {
    static const char tbl[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t out_len = ((len + 2) / 3) * 4;
    char *out = malloc(out_len + 1);
    if (!out) {
        return NULL;
    }
    const unsigned char *in = (const unsigned char *)data;
    size_t o = 0;
    size_t i = 0;
    for (; i + 2 < len; i += 3) {
        uint32_t v = ((uint32_t)in[i] << 16) | ((uint32_t)in[i + 1] << 8) | in[i + 2];
        out[o++] = tbl[(v >> 18) & 63];
        out[o++] = tbl[(v >> 12) & 63];
        out[o++] = tbl[(v >> 6) & 63];
        out[o++] = tbl[v & 63];
    }
    if (i < len) {
        uint32_t v = (uint32_t)in[i] << 16;
        if (i + 1 < len) {
            v |= (uint32_t)in[i + 1] << 8;
        }
        out[o++] = tbl[(v >> 18) & 63];
        out[o++] = tbl[(v >> 12) & 63];
        out[o++] = (i + 1 < len) ? tbl[(v >> 6) & 63] : '=';
        out[o++] = '=';
    }
    out[o] = '\0';
    return out;
}

/* ── Plain-socket transport ─────────────────────────────────────── */

static void n4_net_init(void) {
#ifdef _WIN32
    static volatile LONG done = 0;
    if (InterlockedCompareExchange(&done, 1, 0) == 0) {
        WSADATA wsa;
        (void)WSAStartup(MAKEWORD(2, 2), &wsa);
    }
#endif
}

static void n4_set_timeouts(cbm_n4_sock_t s) {
#ifdef _WIN32
    DWORD tv = N4_IO_TIMEOUT_MS;
    (void)setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof(tv));
    (void)setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (const char *)&tv, sizeof(tv));
#else
    struct timeval tv;
    tv.tv_sec = N4_IO_TIMEOUT_MS / 1000;
    tv.tv_usec = 0;
    (void)setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    (void)setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
#endif
    int one = 1;
    (void)setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (const char *)&one, sizeof(one));
}

static void n4_disconnect(cbm_neo4j_http_t *h) {
    if (h->sock != N4_SOCK_BAD) {
        n4_sock_close(h->sock);
        h->sock = N4_SOCK_BAD;
    }
    free(h->carry);
    h->carry = NULL;
    h->carry_len = 0;
}

static int n4_connect(cbm_neo4j_http_t *h, char *err, size_t err_sz) {
    n4_net_init();
    char port_s[16];
    (void)snprintf(port_s, sizeof(port_s), "%d", h->url.port);
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *res = NULL;
    if (getaddrinfo(h->url.host, port_s, &hints, &res) != 0 || !res) {
        n4_err(err, err_sz, "cannot resolve Neo4j host '%s'", h->url.host);
        return -1;
    }
    cbm_n4_sock_t s = N4_SOCK_BAD;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        s = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (s == N4_SOCK_BAD) {
            continue;
        }
        if (connect(s, ai->ai_addr, (int)ai->ai_addrlen) == 0) {
            break;
        }
        n4_sock_close(s);
        s = N4_SOCK_BAD;
    }
    freeaddrinfo(res);
    if (s == N4_SOCK_BAD) {
        char buf[CBM_NEO4J_HOST_MAX + 32];
        (void)snprintf(buf, sizeof(buf), "%s:%d", h->url.host, h->url.port);
        n4_err(err, err_sz, "cannot connect to Neo4j at %s (is the server running?)", buf);
        return -1;
    }
    n4_set_timeouts(s);
    h->sock = s;
    return 0;
}

static int n4_send_all(cbm_n4_sock_t s, const char *buf, size_t len) {
    while (len > 0) {
        int chunk = len > (size_t)(1 << 30) ? (1 << 30) : (int)len;
        int n = (int)send(s, buf, chunk, N4_SEND_FLAGS);
        if (n <= 0) {
            return -1;
        }
        buf += n;
        len -= (size_t)n;
    }
    return 0;
}

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} n4_buf_t;

static int n4_buf_reserve(n4_buf_t *b, size_t extra) {
    if (b->len + extra + 1 <= b->cap) {
        return 0;
    }
    size_t cap = b->cap ? b->cap : 8192;
    while (cap < b->len + extra + 1) {
        cap *= 2;
    }
    if (cap > N4_RESP_MAX + N4_HDR_MAX) {
        return -1;
    }
    char *d = realloc(b->data, cap);
    if (!d) {
        return -1;
    }
    b->data = d;
    b->cap = cap;
    return 0;
}

/* Read more bytes from the socket into b. Returns bytes read, 0 on EOF, -1 on error. */
static int n4_recv_more(cbm_neo4j_http_t *h, n4_buf_t *b) {
    if (n4_buf_reserve(b, 16384) != 0) {
        return -1;
    }
    int n = (int)recv(h->sock, b->data + b->len, 16384, 0);
    if (n > 0) {
        b->len += (size_t)n;
        b->data[b->len] = '\0';
    }
    return n;
}

static const char *n4_find_hdr_end(const char *d, size_t len) {
    for (size_t i = 0; i + 3 < len; i++) {
        if (d[i] == '\r' && d[i + 1] == '\n' && d[i + 2] == '\r' && d[i + 3] == '\n') {
            return d + i;
        }
    }
    return NULL;
}

/* Case-insensitive header lookup within [hdrs, end). Copies value into out. */
static bool n4_header(const char *hdrs, const char *end, const char *name, char *out,
                      size_t out_sz) {
    size_t nl = strlen(name);
    const char *line = hdrs;
    while (line < end) {
        const char *eol = line;
        while (eol < end && *eol != '\r') {
            eol++;
        }
        if ((size_t)(eol - line) > nl && line[nl] == ':' && n4_has_prefix_ci(line, name)) {
            const char *v = line + nl + 1;
            while (v < eol && (*v == ' ' || *v == '\t')) {
                v++;
            }
            size_t vl = (size_t)(eol - v);
            if (vl >= out_sz) {
                vl = out_sz - 1;
            }
            memcpy(out, v, vl);
            out[vl] = '\0';
            return true;
        }
        line = eol + 2;
    }
    return false;
}

/* Decode a chunked body in place. Returns decoded length, or -1 when the
 * buffer does not yet hold the complete body (*need_more set) or is malformed. */
static long long n4_dechunk(char *body, size_t len, size_t *consumed, bool *need_more) {
    size_t r = 0;
    size_t w = 0;
    *need_more = false;
    for (;;) {
        const char *crlf = NULL;
        for (size_t i = r; i + 1 < len; i++) {
            if (body[i] == '\r' && body[i + 1] == '\n') {
                crlf = body + i;
                break;
            }
        }
        if (!crlf) {
            *need_more = true;
            return -1;
        }
        char *endp = NULL;
        unsigned long long sz = strtoull(body + r, &endp, 16);
        if (endp == body + r) {
            return -1;
        }
        size_t data_start = (size_t)(crlf - body) + 2;
        if (sz == 0) {
            /* Trailer section ends with an empty line. */
            size_t t = data_start;
            for (;;) {
                if (t + 1 >= len) {
                    *need_more = true;
                    return -1;
                }
                if (body[t] == '\r' && body[t + 1] == '\n') {
                    *consumed = t + 2;
                    return (long long)w;
                }
                while (t + 1 < len && !(body[t] == '\r' && body[t + 1] == '\n')) {
                    t++;
                }
                if (t + 1 >= len) {
                    *need_more = true;
                    return -1;
                }
                t += 2;
            }
        }
        if (data_start + sz + 2 > len) {
            *need_more = true;
            return -1;
        }
        memmove(body + w, body + data_start, (size_t)sz);
        w += (size_t)sz;
        r = data_start + (size_t)sz + 2;
    }
}

/* One request/response exchange over the plain socket. Returns 0 on success,
 * -1 on transport failure; *retryable is set when the failure happened on a
 * reused keep-alive connection before any response byte arrived. */
static int n4_socket_exchange(cbm_neo4j_http_t *h, const char *req, size_t req_len,
                              const char *body, size_t body_len, int *status, char **resp,
                              size_t *resp_len, bool *retryable, char *err, size_t err_sz) {
    *retryable = false;
    bool reused = h->sock != N4_SOCK_BAD;
    if (!reused && n4_connect(h, err, err_sz) != 0) {
        return -1;
    }
    if (n4_send_all(h->sock, req, req_len) != 0 ||
        (body_len > 0 && n4_send_all(h->sock, body, body_len) != 0)) {
        n4_disconnect(h);
        *retryable = reused;
        n4_err(err, err_sz, "send to Neo4j failed%s", NULL);
        return -1;
    }

    n4_buf_t b = {0};
    if (h->carry_len > 0) {
        if (n4_buf_reserve(&b, h->carry_len) != 0) {
            n4_disconnect(h);
            return -1;
        }
        memcpy(b.data, h->carry, h->carry_len);
        b.len = h->carry_len;
        b.data[b.len] = '\0';
        free(h->carry);
        h->carry = NULL;
        h->carry_len = 0;
    }
    const char *hdr_end = NULL;
    while (!(hdr_end = n4_find_hdr_end(b.data ? b.data : "", b.len))) {
        if (b.len > N4_HDR_MAX) {
            free(b.data);
            n4_disconnect(h);
            n4_err(err, err_sz, "Neo4j response headers too large%s", NULL);
            return -1;
        }
        int n = n4_recv_more(h, &b);
        if (n <= 0) {
            *retryable = reused && b.len == 0;
            free(b.data);
            n4_disconnect(h);
            n4_err(err, err_sz, "no response from Neo4j (connection closed or timed out)%s", NULL);
            return -1;
        }
    }
    size_t hdr_len = (size_t)(hdr_end - b.data);
    int code = 0;
    if (sscanf(b.data, "HTTP/%*d.%*d %d", &code) != 1) {
        free(b.data);
        n4_disconnect(h);
        n4_err(err, err_sz, "malformed HTTP status line from Neo4j%s", NULL);
        return -1;
    }
    char val[128];
    bool chunked = n4_header(b.data, hdr_end + 2, "Transfer-Encoding", val, sizeof(val)) &&
                   strstr(val, "chunked") != NULL;
    bool conn_close = n4_header(b.data, hdr_end + 2, "Connection", val, sizeof(val)) &&
                      n4_has_prefix_ci(val, "close");
    long long content_len = -1;
    if (!chunked && n4_header(b.data, hdr_end + 2, "Content-Length", val, sizeof(val))) {
        content_len = strtoll(val, NULL, 10);
    }
    size_t body_off = hdr_len + 4;

    size_t out_len = 0;
    size_t consumed = 0;
    if (chunked) {
        for (;;) {
            /* Work on a scratch copy so a partial decode never corrupts b. */
            size_t avail = b.len - body_off;
            char *tmp = malloc(avail + 1);
            if (!tmp) {
                free(b.data);
                n4_disconnect(h);
                return -1;
            }
            memcpy(tmp, b.data + body_off, avail);
            bool need_more = false;
            long long dl = n4_dechunk(tmp, avail, &consumed, &need_more);
            if (dl >= 0) {
                memcpy(b.data + body_off, tmp, (size_t)dl);
                free(tmp);
                out_len = (size_t)dl;
                /* Decoded bytes sit before `consumed`, so any surplus after it
                 * is still intact for the carry below. */
                consumed += body_off;
                break;
            }
            free(tmp);
            if (!need_more) {
                free(b.data);
                n4_disconnect(h);
                n4_err(err, err_sz, "malformed chunked response from Neo4j%s", NULL);
                return -1;
            }
            if (n4_recv_more(h, &b) <= 0) {
                free(b.data);
                n4_disconnect(h);
                n4_err(err, err_sz, "truncated response from Neo4j%s", NULL);
                return -1;
            }
        }
    } else if (content_len >= 0) {
        if ((size_t)content_len > N4_RESP_MAX) {
            free(b.data);
            n4_disconnect(h);
            n4_err(err, err_sz, "Neo4j response too large%s", NULL);
            return -1;
        }
        while (b.len - body_off < (size_t)content_len) {
            if (n4_recv_more(h, &b) <= 0) {
                free(b.data);
                n4_disconnect(h);
                n4_err(err, err_sz, "truncated response from Neo4j%s", NULL);
                return -1;
            }
        }
        out_len = (size_t)content_len;
        consumed = body_off + out_len;
    } else {
        /* Body delimited by connection close. */
        while (n4_recv_more(h, &b) > 0) {}
        out_len = b.len - body_off;
        consumed = b.len;
        conn_close = true;
    }

    /* Keep any surplus bytes for the next response (rare; no pipelining). */
    if (consumed < b.len) {
        h->carry_len = b.len - consumed;
        h->carry = malloc(h->carry_len);
        if (h->carry) {
            memcpy(h->carry, b.data + consumed, h->carry_len);
        } else {
            h->carry_len = 0;
        }
    }

    char *out = malloc(out_len + 1);
    if (!out) {
        free(b.data);
        n4_disconnect(h);
        return -1;
    }
    memcpy(out, b.data + body_off, out_len);
    out[out_len] = '\0';
    free(b.data);
    if (conn_close) {
        n4_disconnect(h);
    }
    *status = code;
    *resp = out;
    if (resp_len) {
        *resp_len = out_len;
    }
    return 0;
}

/* ── WinHTTP transport (https on Windows) ───────────────────────── */

#ifdef _WIN32
static wchar_t *n4_widen(const char *s) {
    int n = MultiByteToWideChar(CP_UTF8, 0, s, -1, NULL, 0);
    if (n <= 0) {
        return NULL;
    }
    wchar_t *w = malloc((size_t)n * sizeof(wchar_t));
    if (w) {
        (void)MultiByteToWideChar(CP_UTF8, 0, s, -1, w, n);
    }
    return w;
}

static int n4_winhttp_post(cbm_neo4j_http_t *h, const char *full_path, const char *body,
                           size_t body_len, int *status, char **resp, size_t *resp_len, char *err,
                           size_t err_sz) {
    if (!h->session) {
        h->session = WinHttpOpen(L"codebase-memory-mcp", WINHTTP_ACCESS_TYPE_DEFAULT_PROXY,
                                 WINHTTP_NO_PROXY_NAME, WINHTTP_NO_PROXY_BYPASS, 0);
        if (!h->session) {
            n4_err(err, err_sz, "WinHttpOpen failed%s", NULL);
            return -1;
        }
        (void)WinHttpSetTimeouts(h->session, N4_IO_TIMEOUT_MS, N4_IO_TIMEOUT_MS, N4_IO_TIMEOUT_MS,
                                 N4_IO_TIMEOUT_MS);
        wchar_t *whost = n4_widen(h->url.host);
        if (!whost) {
            return -1;
        }
        h->connect = WinHttpConnect(h->session, whost, (INTERNET_PORT)h->url.port, 0);
        free(whost);
        if (!h->connect) {
            n4_err(err, err_sz, "cannot connect to Neo4j host '%s'", h->url.host);
            return -1;
        }
    }
    wchar_t *wpath = n4_widen(full_path);
    if (!wpath) {
        return -1;
    }
    HINTERNET req = WinHttpOpenRequest(h->connect, L"POST", wpath, NULL, WINHTTP_NO_REFERER,
                                       WINHTTP_DEFAULT_ACCEPT_TYPES, WINHTTP_FLAG_SECURE);
    free(wpath);
    if (!req) {
        n4_err(err, err_sz, "WinHttpOpenRequest failed%s", NULL);
        return -1;
    }
    char hdrs[1024];
    (void)snprintf(hdrs, sizeof(hdrs),
                   "Content-Type: application/json\r\nAccept: application/json\r\n%s%s%s",
                   h->auth_header ? "Authorization: " : "", h->auth_header ? h->auth_header : "",
                   h->auth_header ? "\r\n" : "");
    wchar_t *whdrs = n4_widen(hdrs);
    BOOL ok = whdrs && WinHttpSendRequest(req, whdrs, (DWORD)-1L, (LPVOID)body, (DWORD)body_len,
                                          (DWORD)body_len, 0);
    free(whdrs);
    if (ok) {
        ok = WinHttpReceiveResponse(req, NULL);
    }
    if (!ok) {
        char code[32];
        (void)snprintf(code, sizeof(code), "%lu", (unsigned long)GetLastError());
        n4_err(err, err_sz, "HTTPS request to Neo4j failed (WinHTTP error %s)", code);
        WinHttpCloseHandle(req);
        return -1;
    }
    DWORD sc = 0;
    DWORD sc_len = sizeof(sc);
    (void)WinHttpQueryHeaders(req, WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
                              WINHTTP_HEADER_NAME_BY_INDEX, &sc, &sc_len, WINHTTP_NO_HEADER_INDEX);
    n4_buf_t b = {0};
    for (;;) {
        DWORD avail = 0;
        if (!WinHttpQueryDataAvailable(req, &avail) || avail == 0) {
            break;
        }
        if (n4_buf_reserve(&b, avail) != 0) {
            free(b.data);
            WinHttpCloseHandle(req);
            n4_err(err, err_sz, "Neo4j response too large%s", NULL);
            return -1;
        }
        DWORD got = 0;
        if (!WinHttpReadData(req, b.data + b.len, avail, &got) || got == 0) {
            break;
        }
        b.len += got;
        b.data[b.len] = '\0';
    }
    WinHttpCloseHandle(req);
    if (!b.data) {
        b.data = calloc(1, 1);
    }
    *status = (int)sc;
    *resp = b.data;
    if (resp_len) {
        *resp_len = b.len;
    }
    return 0;
}
#endif

/* ── Public API ─────────────────────────────────────────────────── */

cbm_neo4j_http_t *cbm_neo4j_http_open(const cbm_neo4j_url_t *url, const char *user,
                                      const char *password, char *err, size_t err_sz) {
#ifndef _WIN32
    if (url->tls) {
        n4_err(err, err_sz,
               "https:// graph_db URLs are only supported on Windows builds; use http:// "
               "(e.g. through a local TLS-terminating proxy or SSH tunnel) on this platform%s",
               NULL);
        return NULL;
    }
#endif
    cbm_neo4j_http_t *h = calloc(1, sizeof(*h));
    if (!h) {
        return NULL;
    }
    h->url = *url;
    h->sock = N4_SOCK_BAD;
    if (user && *user) {
        size_t ul = strlen(user);
        size_t pl = password ? strlen(password) : 0;
        char *cred = malloc(ul + pl + 2);
        if (!cred) {
            free(h);
            return NULL;
        }
        memcpy(cred, user, ul);
        cred[ul] = ':';
        if (pl) {
            memcpy(cred + ul + 1, password, pl);
        }
        cred[ul + pl + 1] = '\0';
        char *b64 = cbm_neo4j_base64(cred, ul + pl + 1);
        /* Wipe the plaintext credential before freeing. */
        memset(cred, 0, ul + pl + 1);
        free(cred);
        if (!b64) {
            free(h);
            return NULL;
        }
        size_t hl = strlen(b64) + sizeof("Basic ");
        h->auth_header = malloc(hl);
        if (!h->auth_header) {
            free(b64);
            free(h);
            return NULL;
        }
        (void)snprintf(h->auth_header, hl, "Basic %s", b64);
        free(b64);
    }
    return h;
}

int cbm_neo4j_http_post(cbm_neo4j_http_t *h, const char *path, const char *body, size_t body_len,
                        int *status, char **resp, size_t *resp_len, char *err, size_t err_sz) {
    if (!h || !path || !status || !resp) {
        return -1;
    }
    *resp = NULL;
    char full_path[CBM_NEO4J_PATH_MAX * 2];
    (void)snprintf(full_path, sizeof(full_path), "%s%s", h->url.base_path, path);
#ifdef _WIN32
    if (h->url.tls) {
        return n4_winhttp_post(h, full_path, body, body_len, status, resp, resp_len, err, err_sz);
    }
#endif
    size_t req_cap = strlen(full_path) + strlen(h->url.host) +
                     (h->auth_header ? strlen(h->auth_header) : 0) + 256;
    char *req = malloc(req_cap);
    if (!req) {
        return -1;
    }
    int req_len = snprintf(req, req_cap,
                           "POST %s HTTP/1.1\r\n"
                           "Host: %s:%d\r\n"
                           "Content-Type: application/json\r\n"
                           "Accept: application/json;charset=UTF-8\r\n"
                           "Content-Length: %zu\r\n"
                           "Connection: keep-alive\r\n"
                           "%s%s%s"
                           "\r\n",
                           full_path, h->url.host, h->url.port, body_len,
                           h->auth_header ? "Authorization: " : "",
                           h->auth_header ? h->auth_header : "", h->auth_header ? "\r\n" : "");
    if (req_len < 0 || (size_t)req_len >= req_cap) {
        free(req);
        return -1;
    }
    bool retryable = false;
    int rc = n4_socket_exchange(h, req, (size_t)req_len, body, body_len, status, resp, resp_len,
                                &retryable, err, err_sz);
    if (rc != 0 && retryable) {
        /* The server closed an idle keep-alive connection: reconnect once. */
        rc = n4_socket_exchange(h, req, (size_t)req_len, body, body_len, status, resp, resp_len,
                                &retryable, err, err_sz);
    }
    memset(req, 0, (size_t)req_len); /* carries the auth header */
    free(req);
    return rc;
}

void cbm_neo4j_http_close(cbm_neo4j_http_t *h) {
    if (!h) {
        return;
    }
    n4_disconnect(h);
#ifdef _WIN32
    if (h->connect) {
        WinHttpCloseHandle(h->connect);
    }
    if (h->session) {
        WinHttpCloseHandle(h->session);
    }
#endif
    if (h->auth_header) {
        memset(h->auth_header, 0, strlen(h->auth_header));
        free(h->auth_header);
    }
    free(h);
}
