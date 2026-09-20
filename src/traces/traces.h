/*
 * traces.h — OTLP trace processing helpers.
 *
 * Pure helper functions for extracting data from OpenTelemetry spans.
 * Used by the MCP ingest_traces handler.
 */
#ifndef CBM_TRACES_H
#define CBM_TRACES_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* ── Attribute key-value pair ─────────────────────────────────────── */

typedef struct {
    const char *key;
    const char *string_value;
} cbm_trace_attr_t;

/* ── Resource (service-level info) ────────────────────────────────── */

typedef struct {
    cbm_trace_attr_t *attributes;
    int attr_count;
} cbm_trace_resource_t;

/* ── Span ─────────────────────────────────────────────────────────── */

typedef struct {
    int kind; /* 1=internal, 2=server, 3=client */
    cbm_trace_attr_t *attributes;
    int attr_count;
    const char *start_time; /* nanosecond timestamp string */
    const char *end_time;   /* nanosecond timestamp string */
} cbm_trace_span_t;

/* ── HTTP span info (result of extractHTTPInfo) ───────────────────── */

typedef struct {
    const char *service_name;
    const char *method;
    const char *path;
    const char *status_code;
    int span_kind;
    /* Nanoseconds, or CBM_DURATION_UNKNOWN when the span's timestamps could
     * not be read. A duration of 0 means the span really took no time. */
    int64_t duration_ns;
} cbm_http_span_info_t;

/* ── Helper functions ─────────────────────────────────────────────── */

/* Extract "service.name" from resource attributes. Returns "" if absent. */
const char *cbm_extract_service_name(const cbm_trace_resource_t *r);

/* Extract HTTP info from a span. Returns true if HTTP span, false otherwise.
 * Fills out with method, path, status, duration. Caller owns string pointers
 * (they point to data within the span's attribute strings — NOT heap-allocated). */
bool cbm_extract_http_info(const cbm_trace_span_t *span, const char *service_name,
                           cbm_http_span_info_t *out);

/* Extract path component from a full URL.
 * E.g. "https://example.com/api/orders?q=1" → "/api/orders"
 * Writes to buf (up to buf_sz). Returns buf, or "" if not a valid URL. */
const char *cbm_extract_path_from_url(const char *url, char *buf, size_t buf_sz);

/* A duration nobody could measure. Kept apart from 0, which is a real span
 * that took no time. */
#define CBM_DURATION_UNKNOWN (-1)

/* Parse nanosecond timestamp strings and return duration. Text that cannot be
 * read counts as 0, so an unreadable timestamp and a span that took no time
 * give the same answer. Use cbm_parse_duration_checked when that matters. */
int64_t cbm_parse_duration(const char *start_nano, const char *end_nano);

/* Same, and also says whether both timestamps could be read. *ok is false for
 * a NULL argument and for any text that does not read cleanly end to end, so
 * a caller can tell "took no time" from "nobody could measure this". */
int64_t cbm_parse_duration_checked(const char *start_nano, const char *end_nano, bool *ok);

/* Calculate P99 from an array of int64_t values.
 * Sorts values in-place. Returns 0 for empty array. */
int64_t cbm_calculate_p99(int64_t *values, int count);

#endif /* CBM_TRACES_H */
