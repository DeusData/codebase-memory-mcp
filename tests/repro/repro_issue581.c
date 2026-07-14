// repro_issue581.c -- Reproduce-first case for OPEN bug #581.
//
// Issue: #581 -- "Memory leak: process grows to 50+ GB virtual memory over
//               hours/days, crashes Windows"
//   https://github.com/DeusData/codebase-memory-mcp/issues/581
//
// OBSERVED BEHAVIOUR:
//   codebase-memory-mcp in stdio MCP server mode grows from ~12 MB working
//   set to 50-107 GB virtual memory over 12-48 hours while the agent issues
//   repeated queries (search_graph, query_graph, get_architecture, etc.).
//   The reporter confirmed auto_index=false, so indexing is NOT the growth
//   path -- the leak occurs purely from query/read operations.
//
// ROOT-CAUSE HYPOTHESIS (two-part):
//
//   1. SQLite WAL file: every query-only store open uses WAL journal mode
//      (configure_pragmas, store.c:343) and mmap_size=64 MB
//      (store.c:355-358).  The WAL file accumulates un-checkpointed frames
//      on every write-side flush (which happens from other operations even
//      on a "read-only" query session because SQLite WAL readers also
//      participate in the WAL protocol).  The only checkpoint in the MCP
//      event loop is SQLITE_CHECKPOINT_PASSIVE, which never ftruncates
//      (mcp.c:869).  Over thousands of operations the WAL grows without
//      bound, with each page mapped via mmap into virtual address space.
//
//   2. mimalloc page retention: cbm_mem_collect() is called after
//      index_repository (mcp.c:2866, 4616) and after delete_project
//      (mcp.c:1860), but NEVER after query operations.  mimalloc retains
//      freed arena pages in its internal free-lists so they show up as
//      committed virtual memory (visible on Windows as "commit charge")
//      even after the query result is freed.
//
//   The combination -- SQLite WAL mapped pages + mimalloc retained pages
//   not returned to OS -- accumulates monotonically across thousands of
//   query iterations without any compaction trigger.
//
// BOUNDED REPRODUCTION STRATEGY:
//   Repeat a single MCP query tool call (search_graph) N=150 times against
//   a small indexed project.  Measure current RSS (not peak) at warmup
//   (iteration 10) and at the end (iteration 150).  Assert that end RSS is
//   not more than LEAK_FACTOR x warmup RSS.
//
//   The real-world leak is 50 GB over hours (~thousands of operations).
//   Per-query accumulation is therefore large but the signal over 150
//   iterations is proportionally small.  We choose a generous threshold
//   (3.0x) so a truly bounded implementation passes easily, while a
//   genuinely leaking implementation that retains ~10-100 kB per query
//   accumulates enough to exceed 3x warmup after 150 iterations (at
//   10 kB/call on a 30 MB baseline: 30 MB + 1.5 MB = 1.05x -- borderline).
//
// IMPORTANT CAVEATS / FLAKINESS NOTES:
//
//   (a) RSS MEASUREMENT: we use cbm_mem_rss() (src/foundation/mem.c) which
//       calls mi_process_info() for current RSS, or falls back to
//       /proc/self/statm (Linux), mach_task_basic_info.resident_size (macOS),
//       or GetProcessMemoryInfo.WorkingSetSize (Windows).  This is CURRENT
//       RSS, not peak -- suitable for detecting steady-state growth.
//
//   (b) ASan BUILD PITFALL: the repro runner uses ASAN_OPTIONS=detect_leaks=0,
//       so LSan won't catch this class of leak here (mimalloc/WAL accumulated
//       pages are not classically leaked -- they are reachable but never freed).
//       This test is an RSS-growth test, not a LSan test.  ASan instrumentation
//       inflates per-allocation overhead ~3x; iteration count (150) is chosen
//       conservatively to stay well within CI time budgets even with ASan.
//
//   (c) THRESHOLD 3.0x: the warmup RSS includes the full SQLite page cache
//       and mimalloc initial arenas.  On an 8-core machine warmup may be
//       50-100 MB; 3x would be 150-300 MB, achievable with a bad leak rate of
//       ~1 MB/query over 150 queries.  On a FIXED implementation the end RSS
//       should be close to 1.0-1.2x warmup (GC cycle, small jitter).
//       If this test produces a false FAIL on a correct implementation (warmup
//       RSS is very small, e.g. 5 MB, and allocator variance causes spike), the
//       threshold can be increased to 4x or the warmup moved later; this is
//       documented as a known-fragile point.
//
//   (d) LINUX-ONLY ALTERNATIVE: if cbm_mem_rss() returns 0 (e.g. MI_OVERRIDE=0
//       without the OS fallback compiled), the test falls back to reading
//       /proc/self/statm directly below.  On macOS and Windows cbm_mem_rss()
//       is expected to return non-zero.  If all RSS readings are zero the test
//       is declared inconclusive and PASSES to avoid false failures (the
//       growth assertion requires reliable RSS readings).
//
// FIX LOCATION (not implemented here -- this test must stay RED until fixed):
//   Two complementary fixes are needed:
//   1. src/mcp/mcp.c, cbm_mcp_server_run event loop (or after each tool call
//      in cbm_mcp_handle_tool): periodically call
//        sqlite3_wal_checkpoint_v2(..., SQLITE_CHECKPOINT_TRUNCATE, ...)
//      and cbm_mem_collect() after query bursts (e.g. every N=50 calls or
//      after exceeding a RSS threshold via cbm_mem_over_budget()).
//   2. src/mcp/mcp.c, cbm_mcp_server_evict_idle: on idle eviction, call
//      cbm_mem_collect() so mimalloc returns pages to the OS, matching the
//      same pattern used after index_repository.
//
//   Without both fixes the WAL and mimalloc page pools grow monotonically
//   across a long-running server session.

#include "test_framework.h"
#include "repro_harness.h"
#include <foundation/mem.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Number of search_graph calls per trial.
// 10 warmup + 140 measurement = 150 total.
// Deliberately modest to stay within CI time budgets even with ASan.
#define ITER_WARMUP   10
#define ITER_TOTAL   150

// Generous RSS growth multiplier: end RSS must not exceed LEAK_FACTOR x
// warmup RSS.  A correct implementation stays near 1.0-1.2x; a leaking
// implementation grows linearly.
// Set to 3.0 to tolerate allocator variance while still catching a real leak
// of >1 MB per query over 140 post-warmup iterations.
#define LEAK_FACTOR  3.0

// Fallback current-RSS reader for Linux, used if cbm_mem_rss() returns 0
// (MI_OVERRIDE=0 with no OS fallback compiled in).  Returns 0 if unavailable.
static size_t rss_bytes(void) {
    size_t v = cbm_mem_rss();
    if (v > 0) {
        return v;
    }
#if defined(__linux__)
    // /proc/self/statm: fields are "VmSize VmRSS ..." in pages
    FILE *f = fopen("/proc/self/statm", "r");
    if (!f) {
        return 0;
    }
    unsigned long vm_pages = 0;
    unsigned long rss_pages = 0;
    if (fscanf(f, "%lu %lu", &vm_pages, &rss_pages) != 2) {
        rss_pages = 0;
    }
    fclose(f);
    long ps = sysconf(_SC_PAGESIZE);
    return rss_pages * (size_t)(ps > 0 ? (unsigned long)ps : 4096UL);
#else
    return 0;
#endif
}

// Small fixture: a tiny Python module with a few functions.
// Chosen to produce a small but real graph (~5 nodes/edges) so that
// search_graph hits the actual SQLite code path including FTS5 lookup,
// node scan, and JSON serialisation -- replicating the real query workload.
static const char FIXTURE_PY[] =
    "def add(a, b):\n"
    "    return a + b\n"
    "\n"
    "def multiply(a, b):\n"
    "    result = a * b\n"
    "    return result\n"
    "\n"
    "def greet(name):\n"
    "    msg = 'hello ' + name\n"
    "    print(msg)\n"
    "    return msg\n";

// search_graph args JSON for repeated queries.
// Uses a broad name_pattern so results are always non-empty (exercises both
// the FTS5 and regex code paths and forces JSON result allocation + free).
static const char SEARCH_ARGS[] =
    "{\"project\":\"__PROJ__\","
    "\"name_pattern\":\".*\","
    "\"limit\":10}";

// Build the args string with the real project name substituted.
// Caller must free the returned string.
static char *build_search_args(const char *project) {
    const char *tmpl = SEARCH_ARGS;
    const char *marker = "__PROJ__";
    const char *pos = strstr(tmpl, marker);
    if (!pos || !project) {
        return NULL;
    }
    size_t prefix_len = (size_t)(pos - tmpl);
    size_t proj_len = strlen(project);
    size_t suffix_len = strlen(pos + strlen(marker));
    size_t total = prefix_len + proj_len + suffix_len + 1;
    char *out = malloc(total);
    if (!out) {
        return NULL;
    }
    memcpy(out, tmpl, prefix_len);
    memcpy(out + prefix_len, project, proj_len);
    memcpy(out + prefix_len + proj_len, pos + strlen(marker), suffix_len + 1);
    return out;
}

// repro_issue581_query_rss_stable
//
// Asserts that RSS does not grow monotonically when search_graph is called
// repeatedly against a single indexed project.
//
// RED on current code:
//   SQLite WAL frames + mimalloc retained pages accumulate across iterations.
//   After ITER_TOTAL iterations the RSS exceeds LEAK_FACTOR x warmup RSS.
//   The ASSERT below fires -> RED.
//
// GREEN after fix:
//   cbm_mem_collect() and/or TRUNCATE checkpoint called periodically by the
//   MCP event loop (or after tool calls) return pages to OS.  End RSS stays
//   near warmup RSS (jitter only) -> assertion passes -> GREEN.
//
// NOTE on ITER_WARMUP/ITER_TOTAL calibration:
//   The real leak is ~10 GB/day with an active agent (rough rate:
//   10 GB / 86400 s * avg call interval).  We cannot reproduce that scale
//   in CI, so we rely on the leak being MONOTONIC -- any growth per iteration
//   shows up as a slope over 150 iterations.  If the leak rate is so slow
//   that even 150x does not visibly move RSS beyond allocator jitter, this
//   test may not fire RED on every CI run (documented flakiness risk above).
TEST(repro_issue581_query_rss_stable) {
    RFile files[] = {{"module.py", FIXTURE_PY}};
    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, files, 1);
    ASSERT_NOT_NULL(store);

    // Project name from the harness.
    const char *project = lp.project;
    ASSERT_NOT_NULL(project);

    char *args = build_search_args(project);
    ASSERT_NOT_NULL(args);

    size_t rss_warmup = 0;
    size_t rss_end = 0;

    for (int i = 0; i < ITER_TOTAL; i++) {
        char *resp = cbm_mcp_handle_tool(lp.srv, "search_graph", args);
        // The response must be freed on every call -- verifying the MCP layer
        // does not itself accumulate the result (it doesn't; the leak is lower).
        if (resp) {
            free(resp);
        }

        if (i + 1 == ITER_WARMUP) {
            rss_warmup = rss_bytes();
        }
    }

    rss_end = rss_bytes();

    free(args);
    rh_cleanup(&lp, store);

    if (rss_warmup > 0 && rss_end > 0) {
        printf("  rss_warmup_kb=%zu rss_end_kb=%zu factor=%.2f threshold=%.1f\n", rss_warmup / 1024,
               rss_end / 1024, (double)rss_end / (double)rss_warmup, LEAK_FACTOR);
    } else {
        printf("  NOTE: RSS not measurable on this platform/build\n");
    }

    // HONEST RED — this guard is currently VACUOUS and #581 is OPEN.
    //
    // This fixture CANNOT reproduce the leak: a 3-node graph over 150
    // search_graph calls allocates far too little to move process RSS (observed
    // factor=1.00), so the old "rss_end <= 3.0 x rss_warmup" assertion passed
    // even on the leaking build. A green here would mean "leak fixed" while the
    // leak is unfixed — a false guard that violates the tests-are-guards rule
    // (green <=> fixed). So it stays RED.
    //
    // Turning this GREEN legitimately requires BOTH:
    //   (a) a real reproduction tier — a long-running MCP session issuing
    //       thousands of ops against a LARGE graph, measuring the SQLite WAL
    //       file size and mimalloc committed pages DIRECTLY (not process-RSS
    //       jitter) so the monotonic growth is actually observable; AND
    //   (b) the fix — periodic SQLITE_CHECKPOINT_TRUNCATE + cbm_mem_collect() in
    //       the MCP query loop / idle eviction (see the header + #581).
    //
    // Until both land this is an honest "not fixed / not provable here" RED, not
    // a false green.
    /* TODO(#581): whitelisted known-red on the non-gating bug-repro board. The
     * leak is a real OPEN bug; this fixture cannot yet reproduce it, so the test
     * stays RED (honest "not fixed") rather than vacuously green. Turning it
     * green requires a real WAL-size / mimalloc-committed-pages reproduction tier
     * plus the query-path compaction fix (see header). Tracked, not skipped. */
    FAIL("TODO(#581) whitelisted known-red: query-path memory leak is OPEN and "
         "cannot be reproduced in this fixture (RSS factor ~1.0 even when "
         "leaking) — needs a real WAL/committed-pages reproduction tier plus the "
         "query-path compaction fix");
}

// -- Suite ------------------------------------------------------------------

SUITE(repro_issue581) {
    RUN_TEST(repro_issue581_query_rss_stable);
}
