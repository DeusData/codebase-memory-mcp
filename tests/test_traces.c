/*
 * test_traces.c — Tests for OTLP trace processing helpers.
 *
 * Ported from internal/traces/traces_test.go (5 pure helper tests).
 * The TestIngestOTLPJSON integration test is deferred (needs full MCP pipeline).
 */
#include "test_framework.h"
#include <traces/traces.h>
#include <string.h>

/* ── TestExtractServiceName ─────────────────────────────────────── */

TEST(traces_extract_service_name) {
    cbm_trace_attr_t attrs[] = {
        {.key = "service.name", .string_value = "order-service"},
    };
    cbm_trace_resource_t r = {.attributes = attrs, .attr_count = 1};

    const char *name = cbm_extract_service_name(&r);
    ASSERT_STR_EQ(name, "order-service");
    PASS();
}

TEST(traces_extract_service_name_missing) {
    cbm_trace_attr_t attrs[] = {
        {.key = "other.attr", .string_value = "value"},
    };
    cbm_trace_resource_t r = {.attributes = attrs, .attr_count = 1};

    const char *name = cbm_extract_service_name(&r);
    ASSERT_STR_EQ(name, "");
    PASS();
}

TEST(traces_extract_service_name_null) {
    const char *name = cbm_extract_service_name(NULL);
    ASSERT_STR_EQ(name, "");
    PASS();
}

/* ── TestExtractHTTPInfo ────────────────────────────────────────── */

TEST(traces_extract_http_info) {
    cbm_trace_attr_t attrs[] = {
        {.key = "http.method", .string_value = "GET"},
        {.key = "http.route", .string_value = "/api/orders"},
        {.key = "http.status_code", .string_value = "200"},
    };
    cbm_trace_span_t span = {
        .kind = 2,
        .attributes = attrs,
        .attr_count = 3,
        .start_time = "1000000000",
        .end_time = "1050000000",
    };

    cbm_http_span_info_t info;
    bool ok = cbm_extract_http_info(&span, "svc", &info);
    ASSERT(ok);
    ASSERT_STR_EQ(info.method, "GET");
    ASSERT_STR_EQ(info.path, "/api/orders");
    ASSERT_EQ(info.duration_ns, 50000000);
    ASSERT_EQ(info.span_kind, 2);
    PASS();
}

/* ── TestExtractHTTPInfoNonHTTPSpan ─────────────────────────────── */

TEST(traces_extract_http_info_non_http) {
    cbm_trace_attr_t attrs[] = {
        {.key = "db.system", .string_value = "postgresql"},
    };
    cbm_trace_span_t span = {
        .kind = 1,
        .attributes = attrs,
        .attr_count = 1,
    };

    cbm_http_span_info_t info;
    bool ok = cbm_extract_http_info(&span, "svc", &info);
    ASSERT(!ok);
    PASS();
}

/* ── TestExtractHTTPInfo with url.full ──────────────────────────── */

TEST(traces_extract_http_info_url_full) {
    cbm_trace_attr_t attrs[] = {
        {.key = "http.method", .string_value = "GET"},
        {.key = "url.full", .string_value = "https://example.com/api/items?page=1"},
    };
    cbm_trace_span_t span = {
        .kind = 2,
        .attributes = attrs,
        .attr_count = 2,
        .start_time = "2000000000",
        .end_time = "2100000000",
    };

    cbm_http_span_info_t info;
    bool ok = cbm_extract_http_info(&span, "svc", &info);
    ASSERT(ok);
    ASSERT_STR_EQ(info.path, "/api/items");
    ASSERT_EQ(info.duration_ns, 100000000);
    PASS();
}

/* ── TestExtractServiceName — edge cases ───────────────────────── */

TEST(traces_extract_service_name_empty_attrs) {
    cbm_trace_resource_t r = {.attributes = NULL, .attr_count = 0};
    const char *name = cbm_extract_service_name(&r);
    ASSERT_STR_EQ(name, "");
    PASS();
}

TEST(traces_extract_service_name_multiple_attrs) {
    /* service.name is present but not the first attribute */
    cbm_trace_attr_t attrs[] = {
        {.key = "deployment.environment", .string_value = "production"},
        {.key = "telemetry.sdk.language", .string_value = "go"},
        {.key = "service.name", .string_value = "payment-gateway"},
    };
    cbm_trace_resource_t r = {.attributes = attrs, .attr_count = 3};
    const char *name = cbm_extract_service_name(&r);
    ASSERT_STR_EQ(name, "payment-gateway");
    PASS();
}

/* ── TestExtractPathFromURL ─────────────────────────────────────── */

TEST(traces_extract_path_from_url) {
    char buf[256];

    cbm_extract_path_from_url("https://example.com/api/orders", buf, sizeof(buf));
    ASSERT_STR_EQ(buf, "/api/orders");

    cbm_extract_path_from_url("http://localhost:8080/health?check=true", buf, sizeof(buf));
    ASSERT_STR_EQ(buf, "/health");

    cbm_extract_path_from_url("not-a-url", buf, sizeof(buf));
    ASSERT_STR_EQ(buf, "");

    cbm_extract_path_from_url("https://example.com/", buf, sizeof(buf));
    ASSERT_STR_EQ(buf, "/");

    PASS();
}

/* ── TestExtractPathFromURL — edge cases ──────────────────────── */

TEST(traces_extract_path_null_url) {
    char buf[256] = "untouched";
    const char *result = cbm_extract_path_from_url(NULL, buf, sizeof(buf));
    ASSERT_STR_EQ(result, "");
    PASS();
}

TEST(traces_extract_path_empty_url) {
    char buf[256];
    cbm_extract_path_from_url("", buf, sizeof(buf));
    ASSERT_STR_EQ(buf, "");
    PASS();
}

TEST(traces_extract_path_with_port) {
    char buf[256];
    cbm_extract_path_from_url("https://api.example.com:8443/v2/users", buf, sizeof(buf));
    ASSERT_STR_EQ(buf, "/v2/users");
    PASS();
}

TEST(traces_extract_path_with_query_and_fragment) {
    char buf[256];
    /* Query params should be stripped (stops at '?') */
    cbm_extract_path_from_url("https://example.com/search?q=test&page=2", buf, sizeof(buf));
    ASSERT_STR_EQ(buf, "/search");
    PASS();
}

TEST(traces_extract_path_buffer_too_small) {
    char buf[4]; /* only 4 bytes — path "/api/orders" gets truncated */
    cbm_extract_path_from_url("https://example.com/api/orders", buf, sizeof(buf));
    /* Should truncate safely to 3 chars + NUL */
    ASSERT_EQ(strlen(buf), 3);
    ASSERT_STR_EQ(buf, "/ap");
    PASS();
}

TEST(traces_extract_path_relative_path) {
    /* No scheme — "relative/path" has fewer than 3 slashes → empty */
    char buf[256];
    cbm_extract_path_from_url("/api/orders", buf, sizeof(buf));
    ASSERT_STR_EQ(buf, "");
    PASS();
}

/* ── TestCalculateP99 ───────────────────────────────────────────── */

TEST(traces_calculate_p99) {
    int64_t values[] = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    int64_t p99 = cbm_calculate_p99(values, 10);
    ASSERT_EQ(p99, 100);
    PASS();
}

TEST(traces_calculate_p99_single) {
    int64_t values[] = {42};
    int64_t p99 = cbm_calculate_p99(values, 1);
    ASSERT_EQ(p99, 42);
    PASS();
}

TEST(traces_calculate_p99_empty) {
    int64_t p99 = cbm_calculate_p99(NULL, 0);
    ASSERT_EQ(p99, 0);
    PASS();
}

/* ── TestParseDuration ──────────────────────────────────────────── */

TEST(traces_parse_duration) {
    int64_t d = cbm_parse_duration("1000000000", "1050000000");
    ASSERT_EQ(d, 50000000);
    PASS();
}

TEST(traces_parse_duration_zero) {
    int64_t d = cbm_parse_duration("1000", "500");
    ASSERT_EQ(d, 0); /* end < start → 0 */
    PASS();
}

/* ── TestParseDuration — edge cases ──────────────────────────────── */

TEST(traces_parse_duration_null_start) {
    int64_t d = cbm_parse_duration(NULL, "1000");
    ASSERT_EQ(d, 0);
    PASS();
}

TEST(traces_parse_duration_null_end) {
    int64_t d = cbm_parse_duration("1000", NULL);
    ASSERT_EQ(d, 0);
    PASS();
}

TEST(traces_parse_duration_both_null) {
    int64_t d = cbm_parse_duration(NULL, NULL);
    ASSERT_EQ(d, 0);
    PASS();
}

TEST(traces_parse_duration_equal) {
    /* Same start and end → 0 */
    int64_t d = cbm_parse_duration("5000000000", "5000000000");
    ASSERT_EQ(d, 0);
    PASS();
}

TEST(traces_parse_duration_large_values) {
    /* Realistic nanosecond timestamps (epoch ~2024) */
    int64_t d = cbm_parse_duration("1700000000000000000", "1700000000050000000");
    ASSERT_EQ(d, 50000000); /* 50ms */
    PASS();
}

/* A timestamp the code cannot read is not a measurement of no time.
 *
 * strtoll answers 0 for text it cannot read, and the subtraction guard turns
 * that into a duration of 0 ns. A span that genuinely took no time and a span
 * whose timestamps were garbage then look the same, and no later query can
 * tell them apart. cbm_parse_duration_checked reports which one it read. */
TEST(traces_parse_duration_checked_reports_unreadable_timestamps) {
    bool ok = false;

    /* Positive control: a good pair still reads, and still answers the same
     * number cbm_parse_duration answers. */
    int64_t d = cbm_parse_duration_checked("1000000000", "1050000000", &ok);
    ASSERT(ok);
    ASSERT_EQ(d, 50000000);

    /* A real measurement of no time reads fine. Zero is an answer here. */
    ok = false;
    d = cbm_parse_duration_checked("5000000000", "5000000000", &ok);
    ASSERT(ok);
    ASSERT_EQ(d, 0);

    /* End before start reads fine too. The clamp to 0 is the caller's rule,
     * not a reading failure. */
    ok = false;
    d = cbm_parse_duration_checked("1000", "500", &ok);
    ASSERT(ok);
    ASSERT_EQ(d, 0);

    /* The claim: text that does not read is reported, not folded into 0. */
    const char *unreadable[] = {"abc", "", " 100", "100ns", "1e9"};
    for (size_t i = 0; i < sizeof(unreadable) / sizeof(unreadable[0]); i++) {
        ok = true;
        (void)cbm_parse_duration_checked(unreadable[i], "1050000000", &ok);
        ASSERT(!ok);
        ok = true;
        (void)cbm_parse_duration_checked("1000000000", unreadable[i], &ok);
        ASSERT(!ok);
    }

    /* A NULL argument was never a measurement either. */
    ok = true;
    (void)cbm_parse_duration_checked(NULL, "1000", &ok);
    ASSERT(!ok);
    ok = true;
    (void)cbm_parse_duration_checked("1000", NULL, &ok);
    ASSERT(!ok);
    PASS();
}

/* A span whose timestamps cannot be read keeps its method and path, and says
 * the duration was not recorded rather than claiming it was zero. */
TEST(traces_extract_http_info_marks_unreadable_duration) {
    cbm_trace_attr_t attrs[] = {
        {.key = "http.method", .string_value = "GET"},
        {.key = "http.route", .string_value = "/api/orders"},
        {.key = "http.status_code", .string_value = "200"},
    };
    cbm_trace_span_t span = {
        .kind = 2,
        .attributes = attrs,
        .attr_count = 3,
        .start_time = "not-a-timestamp",
        .end_time = "1050000000",
    };

    cbm_http_span_info_t info;
    bool ok = cbm_extract_http_info(&span, "svc", &info);
    /* The HTTP part of the span is still good data, so it is still returned. */
    ASSERT(ok);
    ASSERT_STR_EQ(info.method, "GET");
    ASSERT_STR_EQ(info.path, "/api/orders");
    /* The claim: not recorded, rather than a duration of zero. */
    ASSERT_EQ(info.duration_ns, CBM_DURATION_UNKNOWN);
    PASS();
}

/* ── TestCalculateP99 — edge cases ────────────────────────────────── */

TEST(traces_calculate_p99_100_values) {
    int64_t values[100];
    for (int i = 0; i < 100; i++) {
        values[i] = (int64_t)(i + 1); /* 1..100 */
    }
    int64_t p99 = cbm_calculate_p99(values, 100);
    /* idx = (int)(100 * 0.99) = 99, values[99] = 100 */
    ASSERT_EQ(p99, 100);
    PASS();
}

TEST(traces_calculate_p99_reversed) {
    /* Reversed array — qsort should sort first */
    int64_t values[10] = {100, 90, 80, 70, 60, 50, 40, 30, 20, 10};
    int64_t p99 = cbm_calculate_p99(values, 10);
    /* After sort: 10..100, idx = (int)(10 * 0.99) = 9, values[9] = 100 */
    ASSERT_EQ(p99, 100);
    PASS();
}

TEST(traces_calculate_p99_two_values) {
    int64_t values[2] = {5, 500};
    int64_t p99 = cbm_calculate_p99(values, 2);
    /* idx = (int)(2 * 0.99) = 1, values[1] = 500 */
    ASSERT_EQ(p99, 500);
    PASS();
}

TEST(traces_calculate_p99_all_same) {
    int64_t values[5] = {42, 42, 42, 42, 42};
    int64_t p99 = cbm_calculate_p99(values, 5);
    ASSERT_EQ(p99, 42);
    PASS();
}

TEST(traces_calculate_p99_negative_count) {
    /* Negative count → treated like empty → 0 */
    int64_t values[3] = {10, 20, 30};
    int64_t p99 = cbm_calculate_p99(values, -1);
    ASSERT_EQ(p99, 0);
    PASS();
}

SUITE(traces) {
    RUN_TEST(traces_extract_service_name);
    RUN_TEST(traces_extract_service_name_missing);
    RUN_TEST(traces_extract_service_name_null);
    RUN_TEST(traces_extract_service_name_empty_attrs);
    RUN_TEST(traces_extract_service_name_multiple_attrs);
    RUN_TEST(traces_extract_http_info);
    RUN_TEST(traces_extract_http_info_non_http);
    RUN_TEST(traces_extract_http_info_url_full);
    RUN_TEST(traces_extract_path_from_url);
    RUN_TEST(traces_extract_path_null_url);
    RUN_TEST(traces_extract_path_empty_url);
    RUN_TEST(traces_extract_path_with_port);
    RUN_TEST(traces_extract_path_with_query_and_fragment);
    RUN_TEST(traces_extract_path_buffer_too_small);
    RUN_TEST(traces_extract_path_relative_path);
    RUN_TEST(traces_calculate_p99);
    RUN_TEST(traces_calculate_p99_single);
    RUN_TEST(traces_calculate_p99_empty);
    RUN_TEST(traces_calculate_p99_100_values);
    RUN_TEST(traces_calculate_p99_reversed);
    RUN_TEST(traces_calculate_p99_two_values);
    RUN_TEST(traces_calculate_p99_all_same);
    RUN_TEST(traces_calculate_p99_negative_count);
    RUN_TEST(traces_parse_duration);
    RUN_TEST(traces_parse_duration_zero);
    RUN_TEST(traces_parse_duration_null_start);
    RUN_TEST(traces_parse_duration_null_end);
    RUN_TEST(traces_parse_duration_both_null);
    RUN_TEST(traces_parse_duration_equal);
    RUN_TEST(traces_parse_duration_large_values);
    RUN_TEST(traces_parse_duration_checked_reports_unreadable_timestamps);
    RUN_TEST(traces_extract_http_info_marks_unreadable_duration);
}
