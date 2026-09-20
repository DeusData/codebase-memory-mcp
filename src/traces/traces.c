/*
 * traces.c — OTLP trace processing helpers.
 */
#include <stdint.h>
#include "traces/traces.h"
#include "foundation/constants.h"

enum { TRACE_PATH_SLASHES = 3, TRACE_NOT_FOUND = -1 };
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>

/* ── extractServiceName ──────────────────────────────────────────── */

const char *cbm_extract_service_name(const cbm_trace_resource_t *r) {
    if (!r) {
        return "";
    }
    for (int i = 0; i < r->attr_count; i++) {
        if (r->attributes[i].key && strcmp(r->attributes[i].key, "service.name") == 0) {
            return r->attributes[i].string_value ? r->attributes[i].string_value : "";
        }
    }
    return "";
}

/* ── extractPathFromURL ──────────────────────────────────────────── */

const char *cbm_extract_path_from_url(const char *url, char *buf, size_t buf_sz) {
    if (!url || !buf || buf_sz == 0) {
        if (buf) {
            buf[0] = '\0';
        }
        return buf ? buf : "";
    }

    /* Find the third '/' which starts the path: https://host/path */
    int slashes = 0;
    int idx = TRACE_NOT_FOUND;
    for (int i = 0; url[i]; i++) {
        if (url[i] == '/') {
            slashes++;
            if (slashes == TRACE_PATH_SLASHES) {
                idx = i;
                break;
            }
        }
    }
    if (idx < 0) {
        buf[0] = '\0';
        return buf;
    }

    /* Copy path, stopping at '?' */
    size_t j = 0;
    for (int i = idx; url[i] && url[i] != '?' && j < buf_sz - SKIP_ONE; i++) {
        buf[j++] = url[i];
    }
    buf[j] = '\0';
    return buf;
}

/* ── parseDuration ───────────────────────────────────────────────── */

/* Read one nanosecond timestamp. Answers false for text that does not read
 * cleanly from its first character to its last, the way src/main.c:1104 does
 * it: an end pointer says where the read stopped, errno catches a number too
 * large, and *end == '\0' catches anything left over. A leading blank is
 * refused too, because strtoll would otherwise step over it. */
static bool trace_read_nano(const char *text, int64_t *out) {
    if (!text || !text[0] || isspace((unsigned char)text[0])) {
        return false;
    }
    char *end = NULL;
    errno = 0;
    long long value = strtoll(text, &end, CBM_DECIMAL_BASE);
    if (errno != 0 || !end || end == text || *end != '\0') {
        return false;
    }
    *out = (int64_t)value;
    return true;
}

int64_t cbm_parse_duration_checked(const char *start_nano, const char *end_nano, bool *ok) {
    int64_t start = 0;
    int64_t end = 0;
    bool read_both = trace_read_nano(start_nano, &start) && trace_read_nano(end_nano, &end);
    if (ok) {
        *ok = read_both;
    }
    if (!read_both) {
        return 0;
    }
    return (end > start) ? (end - start) : 0;
}

int64_t cbm_parse_duration(const char *start_nano, const char *end_nano) {
    if (!start_nano || !end_nano) {
        return 0;
    }
    int64_t start = strtoll(start_nano, NULL, CBM_DECIMAL_BASE);
    int64_t end = strtoll(end_nano, NULL, CBM_DECIMAL_BASE);
    return (end > start) ? (end - start) : 0;
}

/* ── extractHTTPInfo ─────────────────────────────────────────────── */

bool cbm_extract_http_info(const cbm_trace_span_t *span, const char *service_name,
                           cbm_http_span_info_t *out) {
    if (!span || !out) {
        return false;
    }
    memset(out, 0, sizeof(*out));
    out->service_name = service_name ? service_name : "";
    out->span_kind = span->kind;

    bool has_http = false;
    static char url_buf[CBM_SZ_1K];

    for (int i = 0; i < span->attr_count; i++) {
        const char *key = span->attributes[i].key;
        const char *val = span->attributes[i].string_value;
        if (!key || !val) {
            continue;
        }

        if (strcmp(key, "http.method") == 0 || strcmp(key, "http.request.method") == 0) {
            out->method = val;
            has_http = true;
        } else if (strcmp(key, "http.route") == 0 || strcmp(key, "http.target") == 0 ||
                   strcmp(key, "url.path") == 0) {
            out->path = val;
            has_http = true;
        } else if (strcmp(key, "http.status_code") == 0) {
            out->status_code = val;
        } else if (strcmp(key, "url.full") == 0) {
            const char *path = cbm_extract_path_from_url(val, url_buf, sizeof(url_buf));
            if (path[0] != '\0') {
                out->path = path;
                has_http = true;
            }
        }
    }

    if (!has_http || !out->path || out->path[0] == '\0') {
        return false;
    }

    /* A timestamp nobody can read is not a measurement. It used to read as 0,
     * so an unreadable START time reported the whole end time as the duration
     * -- a made-up number that looks like a real one. The HTTP method and path
     * on this span are still good, so the span is still returned and only the
     * duration says it is missing. */
    bool timed = false;
    int64_t duration = cbm_parse_duration_checked(span->start_time, span->end_time, &timed);
    out->duration_ns = timed ? duration : CBM_DURATION_UNKNOWN;
    return true;
}

/* ── calculateP99 ────────────────────────────────────────────────── */

static int cmp_int64(const void *a, const void *b) {
    int64_t va = *(const int64_t *)a;
    int64_t vb = *(const int64_t *)b;
    return (va > vb) - (va < vb);
}

int64_t cbm_calculate_p99(int64_t *values, int count) {
    if (!values || count <= 0) {
        return 0;
    }
    qsort(values, count, sizeof(int64_t), cmp_int64);
#define P99_PERCENTILE 0.99

    int idx = (int)((double)count * P99_PERCENTILE);
    if (idx >= count) {
        idx = count - SKIP_ONE;
    }
    return values[idx];
}
