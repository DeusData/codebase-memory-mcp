/*
 * traces.c — OTLP trace processing helpers.
 */
#include <stdint.h>
#include "traces/traces.h"
#include "foundation/constants.h"

enum { TRACE_PATH_SLASHES = 3, TRACE_NOT_FOUND = -1 };
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

    out->duration_ns = cbm_parse_duration(span->start_time, span->end_time);
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
