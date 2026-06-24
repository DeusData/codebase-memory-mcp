/*
 * httpd.c — First-party HTTP/1.1 server transport for the graph UI.
 *
 * Original implementation written for this project from RFC 9112 and the
 * needs of our endpoints (see httpd.h for the full design constraints:
 * single-threaded, localhost-only, strict CRLF, Connection: close).
 */
#include "ui/httpd.h"

#include <ctype.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
typedef SOCKET cbm_sock_t;
#define CBM_SOCK_BAD INVALID_SOCKET
#define cbm_sock_close closesocket
#else
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
typedef int cbm_sock_t;
#define CBM_SOCK_BAD (-1)
#define cbm_sock_close close
#endif

/* Suppress SIGPIPE on send: Linux has the per-call flag, macOS has the
 * per-socket option (set at accept), Windows has no signals. */
#ifdef MSG_NOSIGNAL
#define CBM_SEND_FLAGS MSG_NOSIGNAL
#else
#define CBM_SEND_FLAGS 0
#endif

struct cbm_httpd {
    cbm_sock_t fd;
    int port;
    int recv_deadline_ms;
};

struct cbm_http_conn {
    cbm_sock_t fd;
    int recv_deadline_ms;
};

/* ── Small platform helpers ───────────────────────────────────── */

static int64_t now_ms(void) {
#ifdef _WIN32
    return (int64_t)GetTickCount64();
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
#endif
}

/* Wait until the socket is readable. 1 = readable, 0 = timeout, -1 = error.
 * Windows uses select() deliberately (WSAPoll has a Won't-Fix bug where
 * certain socket errors are never reported, which can stall an event loop). */
static int wait_readable(cbm_sock_t fd, int timeout_ms) {
#ifdef _WIN32
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    int rc = select(0, &rfds, NULL, NULL, &tv);
    return rc < 0 ? -1 : (rc > 0 ? 1 : 0);
#else
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    int rc = poll(&pfd, 1, timeout_ms);
    if (rc < 0)
        return errno == EINTR ? 0 : -1;
    return rc > 0 ? 1 : 0;
#endif
}

static int send_all(cbm_sock_t fd, const void *data, size_t len) {
    const char *p = data;
    size_t off = 0;
    while (off < len) {
#ifdef _WIN32
        int n = send(fd, p + off, (int)(len - off), CBM_SEND_FLAGS);
#else
        ssize_t n = send(fd, p + off, len - off, CBM_SEND_FLAGS);
#endif
        if (n <= 0)
            return -1;
        off += (size_t)n;
    }
    return 0;
}

/* ── Listener ─────────────────────────────────────────────────── */

cbm_httpd_t *cbm_httpd_listen(int port) {
#ifdef _WIN32
    static atomic_int wsa_started = 0;
    int expected = 0;
    if (atomic_compare_exchange_strong(&wsa_started, &expected, 1)) {
        WSADATA wsa;
        WSAStartup(MAKEWORD(2, 2), &wsa);
    }
#endif

    cbm_sock_t fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == CBM_SOCK_BAD)
        return NULL;

    /* POSIX: SO_REUSEADDR only permits rebinding a TIME_WAIT port.
     * Windows: SO_REUSEADDR would let ANY local user hijack the port, so
     * use SO_EXCLUSIVEADDRUSE instead (Microsoft's own guidance). */
    int one = 1;
#ifdef _WIN32
    setsockopt(fd, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, (const char *)&one, sizeof(one));
#else
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#endif

    /* Loopback only — never any other interface. */
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)port);
    addr.sin_addr.s_addr = htonl(0x7F000001); /* 127.0.0.1 */

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(fd, 16) != 0) {
        cbm_sock_close(fd);
        return NULL;
    }

    /* Resolve the actually-bound port (needed for port 0). */
    struct sockaddr_in bound;
    socklen_t blen = sizeof(bound);
    if (getsockname(fd, (struct sockaddr *)&bound, &blen) != 0) {
        cbm_sock_close(fd);
        return NULL;
    }

    cbm_httpd_t *d = calloc(1, sizeof(*d));
    if (!d) {
        cbm_sock_close(fd);
        return NULL;
    }
    d->fd = fd;
    d->port = (int)ntohs(bound.sin_port);
    d->recv_deadline_ms = CBM_HTTP_RECV_DEADLINE_MS;
    return d;
}

int cbm_httpd_port(const cbm_httpd_t *d) {
    return d ? d->port : -1;
}

void cbm_httpd_set_recv_deadline_ms(cbm_httpd_t *d, int ms) {
    if (d && ms > 0)
        d->recv_deadline_ms = ms;
}

void cbm_httpd_close(cbm_httpd_t *d) {
    if (!d)
        return;
    cbm_sock_close(d->fd);
    free(d);
}

/* ── Accept ───────────────────────────────────────────────────── */

cbm_http_conn_t *cbm_httpd_accept(cbm_httpd_t *d, int timeout_ms) {
    if (!d)
        return NULL;
    if (wait_readable(d->fd, timeout_ms) != 1)
        return NULL;

    cbm_sock_t cfd = accept(d->fd, NULL, NULL);
    if (cfd == CBM_SOCK_BAD)
        return NULL;

    int one = 1;
    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, (const char *)&one, sizeof(one));
#ifdef SO_NOSIGPIPE
    setsockopt(cfd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

    cbm_http_conn_t *c = calloc(1, sizeof(*c));
    if (!c) {
        cbm_sock_close(cfd);
        return NULL;
    }
    c->fd = cfd;
    c->recv_deadline_ms = d->recv_deadline_ms;
    return c;
}

void cbm_httpd_conn_close(cbm_http_conn_t *c) {
    if (!c)
        return;
    cbm_sock_close(c->fd);
    free(c);
}

/* ── Head parsing ─────────────────────────────────────────────── */

static bool header_name_is(const char *line, size_t name_len, const char *name) {
    for (size_t i = 0; i < name_len; i++) {
        if (tolower((unsigned char)line[i]) != name[i])
            return false;
    }
    return name[name_len] == '\0' ? true : false;
}

/* Copy a trimmed header value into out. */
static void copy_header_value(const char *val, const char *val_end, char *out, size_t outsz) {
    while (val < val_end && (*val == ' ' || *val == '\t'))
        val++;
    while (val_end > val && (val_end[-1] == ' ' || val_end[-1] == '\t'))
        val_end--;
    size_t n = (size_t)(val_end - val);
    if (n >= outsz) {
        out[0] = '\0'; /* oversize values are ignored, never truncated */
        return;
    }
    memcpy(out, val, n);
    out[n] = '\0';
}

int cbm_http_parse_head(const char *data, size_t len, cbm_http_req_t *req, size_t *body_offset,
                        size_t *content_length) {
    memset(req, 0, sizeof(*req));
    *body_offset = 0;
    *content_length = 0;

    /* Scan for the CRLFCRLF terminator. While scanning, enforce the strict
     * byte rules: no raw NUL, no LF that is not preceded by CR. */
    size_t scan = len < CBM_HTTP_MAX_HEAD ? len : CBM_HTTP_MAX_HEAD;
    size_t head_end = 0; /* offset just past CRLFCRLF */
    for (size_t i = 0; i < scan; i++) {
        char ch = data[i];
        if (ch == '\0')
            return 400;
        if (ch == '\n' && (i == 0 || data[i - 1] != '\r'))
            return 400;
        if (ch == '\n' && i >= 3 && data[i - 3] == '\r' && data[i - 2] == '\n') {
            head_end = i + 1;
            break;
        }
    }
    if (head_end == 0)
        return len >= CBM_HTTP_MAX_HEAD ? 431 : CBM_HTTP_NEED_MORE;

    /* ── Request line: METHOD SP request-target SP HTTP/1.x ── */
    const char *line = data;
    const char *line_end = memchr(line, '\r', head_end);
    if (!line_end)
        return 400;

    const char *sp1 = memchr(line, ' ', (size_t)(line_end - line));
    if (!sp1)
        return 400;
    size_t mlen = (size_t)(sp1 - line);
    if (mlen == 0 || mlen >= sizeof(req->method))
        return 400;
    for (size_t i = 0; i < mlen; i++) {
        if (!isupper((unsigned char)line[i]))
            return 400;
    }
    memcpy(req->method, line, mlen);
    req->method[mlen] = '\0';

    const char *target = sp1 + 1;
    const char *sp2 = memchr(target, ' ', (size_t)(line_end - target));
    if (!sp2)
        return 400;
    const char *version = sp2 + 1;
    size_t vlen = (size_t)(line_end - version);
    /* Exactly HTTP/1.0 or HTTP/1.1 */
    if (vlen != 8 || memcmp(version, "HTTP/1.", 7) != 0 || (version[7] != '0' && version[7] != '1'))
        return 400;

    size_t tlen = (size_t)(sp2 - target);
    if (tlen == 0 || target[0] != '/')
        return 400;
    /* Reject percent-encoded NUL anywhere in the request target. */
    for (size_t i = 0; i + 2 < tlen; i++) {
        if (target[i] == '%' && target[i + 1] == '0' && target[i + 2] == '0')
            return 400;
    }

    /* Split raw target into path (matched raw — never decoded) and query. */
    const char *qmark = memchr(target, '?', tlen);
    size_t plen = qmark ? (size_t)(qmark - target) : tlen;
    if (plen >= sizeof(req->path))
        return 400;
    memcpy(req->path, target, plen);
    req->path[plen] = '\0';
    if (qmark) {
        size_t qlen = tlen - plen - 1;
        if (qlen >= sizeof(req->query))
            return 400;
        memcpy(req->query, qmark + 1, qlen);
        req->query[qlen] = '\0';
    }

    /* ── Header fields ── */
    bool have_content_length = false;
    const char *p = line_end + 2;
    const char *head_stop = data + head_end - 2; /* start of final CRLF */
    while (p < head_stop) {
        const char *eol = memchr(p, '\r', (size_t)(head_stop - p));
        if (!eol)
            eol = head_stop;
        const char *colon = memchr(p, ':', (size_t)(eol - p));
        if (!colon)
            return 400;
        size_t nlen = (size_t)(colon - p);

        if (header_name_is(p, nlen, "origin")) {
            copy_header_value(colon + 1, eol, req->origin, sizeof(req->origin));
        } else if (header_name_is(p, nlen, "transfer-encoding")) {
            /* Chunked (or any transfer coding) is not supported. */
            return 411;
        } else if (header_name_is(p, nlen, "content-length")) {
            char val[32];
            copy_header_value(colon + 1, eol, val, sizeof(val));
            if (val[0] == '\0')
                return 400;
            uint64_t cl = 0;
            for (const char *v = val; *v; v++) {
                if (!isdigit((unsigned char)*v))
                    return 400;
                cl = cl * 10 + (uint64_t)(*v - '0');
                if (cl > (uint64_t)CBM_HTTP_MAX_BODY)
                    return 413;
            }
            if (have_content_length && cl != (uint64_t)*content_length)
                return 400; /* conflicting duplicates */
            *content_length = (size_t)cl;
            have_content_length = true;
        }
        p = eol + 2;
    }

    *body_offset = head_end;
    return 0;
}

/* ── Request reading ──────────────────────────────────────────── */

int cbm_httpd_read_request(cbm_http_conn_t *c, cbm_http_req_t *req) {
    if (!c || !req)
        return -1;

    char *head = malloc(CBM_HTTP_MAX_HEAD);
    if (!head)
        return -1;

    int64_t deadline = now_ms() + c->recv_deadline_ms;
    size_t have = 0;
    size_t body_off = 0, clen = 0;
    int rc = CBM_HTTP_NEED_MORE;

    /* Read until the head parses (or fails to). */
    for (;;) {
        int64_t remaining = deadline - now_ms();
        if (remaining <= 0) {
            free(head);
            return 408;
        }
        int w = wait_readable(c->fd, (int)remaining);
        if (w == 0) {
            free(head);
            return 408;
        }
        if (w < 0) {
            free(head);
            return -1;
        }
#ifdef _WIN32
        int n = recv(c->fd, head + have, (int)(CBM_HTTP_MAX_HEAD - have), 0);
#else
        ssize_t n = recv(c->fd, head + have, CBM_HTTP_MAX_HEAD - have, 0);
#endif
        if (n <= 0) {
            free(head); /* peer vanished mid-request — nothing to answer */
            return -1;
        }
        have += (size_t)n;

        rc = cbm_http_parse_head(head, have, req, &body_off, &clen);
        if (rc == 0)
            break;
        if (rc != CBM_HTTP_NEED_MORE) {
            free(head);
            return rc;
        }
        if (have >= CBM_HTTP_MAX_HEAD) {
            free(head);
            return 431;
        }
    }

    /* Read the body per Content-Length (already capped by the parser). */
    if (clen > 0) {
        char *body = malloc(clen + 1);
        if (!body) {
            free(head);
            return -1;
        }
        size_t got = have - body_off;
        if (got > clen)
            got = clen;
        memcpy(body, head + body_off, got);

        while (got < clen) {
            int64_t remaining = deadline - now_ms();
            if (remaining <= 0) {
                free(body);
                free(head);
                return 408;
            }
            int w = wait_readable(c->fd, (int)remaining);
            if (w != 1) {
                free(body);
                free(head);
                return w == 0 ? 408 : -1;
            }
#ifdef _WIN32
            int n = recv(c->fd, body + got, (int)(clen - got), 0);
#else
            ssize_t n = recv(c->fd, body + got, clen - got, 0);
#endif
            if (n <= 0) {
                free(body);
                free(head);
                return -1;
            }
            got += (size_t)n;
        }
        body[clen] = '\0';
        req->body = body;
        req->body_len = clen;
    }

    free(head);
    return 0;
}

void cbm_http_req_free(cbm_http_req_t *req) {
    if (req && req->body) {
        free(req->body);
        req->body = NULL;
        req->body_len = 0;
    }
}

/* ── Responses ────────────────────────────────────────────────── */

static const char *status_reason(int status) {
    switch (status) {
    case 200:
        return "OK";
    case 202:
        return "Accepted";
    case 204:
        return "No Content";
    case 400:
        return "Bad Request";
    case 403:
        return "Forbidden";
    case 404:
        return "Not Found";
    case 408:
        return "Request Timeout";
    case 411:
        return "Length Required";
    case 413:
        return "Content Too Large";
    case 429:
        return "Too Many Requests";
    case 431:
        return "Request Header Fields Too Large";
    case 500:
        return "Internal Server Error";
    default:
        return "";
    }
}

void cbm_http_reply_buf(cbm_http_conn_t *c, int status, const char *extra_headers, const void *data,
                        size_t len) {
    if (!c)
        return;
    char head[1024];
    int hn = snprintf(head, sizeof(head),
                      "HTTP/1.1 %d %s\r\n"
                      "%s"
                      "Content-Length: %zu\r\n"
                      "Connection: close\r\n"
                      "\r\n",
                      status, status_reason(status), extra_headers ? extra_headers : "", len);
    if (hn < 0 || hn >= (int)sizeof(head))
        return; /* oversized extra_headers — drop the response, conn closes */
    if (send_all(c->fd, head, (size_t)hn) != 0)
        return;
    if (len > 0)
        (void)send_all(c->fd, data, len);
}

void cbm_http_replyf(cbm_http_conn_t *c, int status, const char *extra_headers, const char *fmt,
                     ...) {
    va_list ap;
    va_start(ap, fmt);
    va_list ap2;
    va_copy(ap2, ap);
    int need = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (need < 0) {
        va_end(ap2);
        return;
    }

    char stack_buf[4096];
    char *body = stack_buf;
    if ((size_t)need >= sizeof(stack_buf)) {
        body = malloc((size_t)need + 1);
        if (!body) {
            va_end(ap2);
            return;
        }
    }
    vsnprintf(body, (size_t)need + 1, fmt, ap2);
    va_end(ap2);

    cbm_http_reply_buf(c, status, extra_headers, body, (size_t)need);
    if (body != stack_buf)
        free(body);
}

/* ── Pure helpers ─────────────────────────────────────────────── */

bool cbm_http_path_match(const char *str, const char *pattern) {
    if (!str || !pattern)
        return false;
    size_t plen = strlen(pattern);
    if (plen > 0 && pattern[plen - 1] == '*')
        return strncmp(str, pattern, plen - 1) == 0;
    return strcmp(str, pattern) == 0;
}

static int hex_val(char ch) {
    if (ch >= '0' && ch <= '9')
        return ch - '0';
    if (ch >= 'a' && ch <= 'f')
        return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F')
        return ch - 'A' + 10;
    return -1;
}

bool cbm_http_query_param(const char *query, const char *name, char *buf, int bufsz) {
    if (!query || !name || !buf || bufsz <= 0)
        return false;
    buf[0] = '\0';
    size_t name_len = strlen(name);

    const char *p = query;
    while (*p) {
        const char *pair_end = strchr(p, '&');
        if (!pair_end)
            pair_end = p + strlen(p);
        const char *eq = memchr(p, '=', (size_t)(pair_end - p));
        size_t klen = eq ? (size_t)(eq - p) : (size_t)(pair_end - p);

        if (klen == name_len && memcmp(p, name, name_len) == 0) {
            if (!eq)
                return false; /* present but no value */
            /* Percent-decode the value into buf. */
            const char *v = eq + 1;
            int out = 0;
            while (v < pair_end) {
                char ch = *v;
                if (ch == '+') {
                    ch = ' ';
                    v++;
                } else if (ch == '%') {
                    if (pair_end - v < 3) /* needs two hex digits */
                        return false;
                    int hi = hex_val(v[1]);
                    int lo = hex_val(v[2]);
                    if (hi < 0 || lo < 0)
                        return false;
                    ch = (char)((hi << 4) | lo);
                    if (ch == '\0')
                        return false; /* decoded NUL never allowed */
                    v += 3;
                } else {
                    v++;
                }
                if (out >= bufsz - 1) {
                    buf[0] = '\0';
                    return false; /* oversize — reject, never truncate */
                }
                buf[out++] = ch;
            }
            buf[out] = '\0';
            return out > 0;
        }
        p = (*pair_end == '&') ? pair_end + 1 : pair_end;
    }
    return false;
}
