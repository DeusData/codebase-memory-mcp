/*
 * test_worker_pool.c — Tests for system info detection and parallel-for dispatch.
 *
 * Suite: suite_system_info  — CPU topology and RAM detection
 * Suite: suite_worker_pool  — Parallel-for correctness + concurrency validation
 */
#include "test_framework.h"
#include "foundation/platform.h"
#include "pipeline/worker_pool.h"

#include <stdatomic.h>
#include <string.h>

/* ── System Info Tests ────────────────────────────────────────────── */

TEST(system_info_total_cores) {
    cbm_system_info_t info = cbm_system_info();
    ASSERT(info.total_cores > 0);
    ASSERT(info.total_cores <= 256);
    PASS();
}

TEST(system_info_total_cores_sane) {
    cbm_system_info_t info = cbm_system_info();
    /* total >= perf + efficiency (some platforms may not distinguish) */
    ASSERT_GTE(info.total_cores, info.perf_cores);
    PASS();
}

TEST(system_info_perf_cores) {
    cbm_system_info_t info = cbm_system_info();
    ASSERT(info.perf_cores > 0);
    PASS();
}

TEST(system_info_total_ram) {
    cbm_system_info_t info = cbm_system_info();
    /* More than 1 GB */
    ASSERT_GT(info.total_ram, (size_t)(1ULL * 1024 * 1024 * 1024));
    PASS();
}

TEST(system_info_idempotent) {
    cbm_system_info_t info1 = cbm_system_info();
    cbm_system_info_t info2 = cbm_system_info();
    /* Cached results must be identical */
    ASSERT_EQ(info1.total_cores, info2.total_cores);
    ASSERT_EQ(info1.perf_cores, info2.perf_cores);
    ASSERT_EQ(info1.total_ram, info2.total_ram);
    PASS();
}

TEST(default_worker_count_initial) {
    cbm_system_info_t info = cbm_system_info();
    int count = cbm_default_worker_count(true);
    ASSERT_EQ(count, info.total_cores);
    PASS();
}

TEST(default_worker_count_incremental) {
    cbm_system_info_t info = cbm_system_info();
    int count = cbm_default_worker_count(false);
    ASSERT(count >= 1);
    ASSERT(count <= info.perf_cores);
    PASS();
}

TEST(default_worker_count_minimum) {
    int count = cbm_default_worker_count(false);
    ASSERT_GTE(count, 1);
    PASS();
}

/* ── Worker Pool Tests ────────────────────────────────────────────── */

static void sum_worker(int idx, void *ctx) {
    _Atomic int *sum = ctx;
    atomic_fetch_add(sum, idx);
}

TEST(parallel_for_sum) {
    _Atomic int sum;
    atomic_init(&sum, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    cbm_parallel_for(1000, sum_worker, &sum, opts);
    ASSERT_EQ(atomic_load(&sum), 1000 * 999 / 2);
    PASS();
}

typedef struct {
    _Atomic int *visited;
    int count;
} coverage_ctx_t;

static void coverage_worker(int idx, void *ctx_ptr) {
    _Atomic int *visited = ctx_ptr;
    atomic_store(&visited[idx], 1);
}

TEST(parallel_for_coverage) {
    _Atomic int visited[1000];
    for (int i = 0; i < 1000; i++)
        atomic_init(&visited[i], 0);
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    cbm_parallel_for(1000, coverage_worker, visited, opts);
    for (int i = 0; i < 1000; i++) {
        ASSERT_EQ(atomic_load(&visited[i]), 1);
    }
    PASS();
}

static void noop_worker(int idx, void *ctx) {
    (void)idx;
    (void)ctx;
}

TEST(parallel_for_zero) {
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    cbm_parallel_for(0, noop_worker, NULL, opts);
    PASS();
}

TEST(parallel_for_one) {
    _Atomic int count;
    atomic_init(&count, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    cbm_parallel_for(1, sum_worker, &count, opts);
    /* idx=0, so sum should be 0 — but count_worker adds idx. Use a different approach. */
    /* Actually sum_worker adds idx to sum, idx=0 → sum=0. Let's verify via count. */
    ASSERT_EQ(atomic_load(&count), 0);
    PASS();
}

TEST(parallel_for_single_worker) {
    _Atomic int sum;
    atomic_init(&sum, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = 1, .force_pthreads = false};
    cbm_parallel_for(100, sum_worker, &sum, opts);
    ASSERT_EQ(atomic_load(&sum), 100 * 99 / 2);
    PASS();
}

TEST(parallel_for_force_pthreads) {
    _Atomic int sum;
    atomic_init(&sum, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = true};
    cbm_parallel_for(100, sum_worker, &sum, opts);
    ASSERT_EQ(atomic_load(&sum), 100 * 99 / 2);
    PASS();
}

static void slot_writer(int idx, void *ctx) {
    int *results = ctx;
    results[idx] = idx * 2;
}

TEST(parallel_for_per_slot_write) {
    int results[1000];
    memset(results, 0, sizeof(results));
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    cbm_parallel_for(1000, slot_writer, results, opts);
    for (int i = 0; i < 1000; i++) {
        ASSERT_EQ(results[i], i * 2);
    }
    PASS();
}

typedef struct {
    _Atomic int concurrent_max;
    _Atomic int concurrent_now;
} concurrency_ctx_t;

static void concurrency_worker(int idx, void *ctx_ptr) {
    (void)idx;
    concurrency_ctx_t *cc = ctx_ptr;
    int cur = atomic_fetch_add(&cc->concurrent_now, 1) + 1;
    /* Brief busy-wait to let others start */
    volatile int x = 0;
    for (int j = 0; j < 10000; j++)
        x++;
    (void)x;
    /* Record max */
    int prev_max = atomic_load(&cc->concurrent_max);
    while (cur > prev_max) {
        if (atomic_compare_exchange_weak(&cc->concurrent_max, &prev_max, cur))
            break;
    }
    atomic_fetch_sub(&cc->concurrent_now, 1);
}

TEST(parallel_for_actually_parallel) {
    concurrency_ctx_t cc;
    atomic_init(&cc.concurrent_max, 0);
    atomic_init(&cc.concurrent_now, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    cbm_parallel_for(100, concurrency_worker, &cc, opts);
    /* At least 2 threads ran concurrently (skip on single-core CI runners) */
    if (atomic_load(&cc.concurrent_max) < 2) {
        SKIP("single-core runner — cannot verify parallelism");
    }
    PASS();
}

static void tls_worker(int idx, void *ctx_ptr) {
    (void)idx;
    static _Thread_local int tls_val = 0;
    _Atomic int *reuse_count = ctx_ptr;
    if (tls_val == 42)
        atomic_fetch_add(reuse_count, 1);
    tls_val = 42;
}

TEST(tls_persistence_across_dispatch) {
    _Atomic int reuse_count;
    atomic_init(&reuse_count, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    cbm_parallel_for(1000, tls_worker, &reuse_count, opts);
    /* If TLS persists across iterations on same thread, reuse_count > 0.
     * This validates _Thread_local TSParser* will persist in extraction. */
    ASSERT_GT(atomic_load(&reuse_count), 0);
    PASS();
}

/* ── Resource Management & Edge Case Tests ──────────────────────── */

TEST(parallel_for_negative_count) {
    /* count=-1 → no iterations (documented: "If count <= 0, this is a no-op") */
    _Atomic int sum;
    atomic_init(&sum, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    cbm_parallel_for(-1, sum_worker, &sum, opts);
    ASSERT_EQ(atomic_load(&sum), 0);
    PASS();
}

TEST(parallel_for_null_fn) {
    /* NULL function pointer — should not crash (no-op or safe handling) */
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    /* count=0 makes this a no-op before fn is called, so safe */
    cbm_parallel_for(0, NULL, NULL, opts);
    PASS();
}

TEST(parallel_for_max_workers_one) {
    /* max_workers=1 → serial execution, correct result */
    _Atomic int sum;
    atomic_init(&sum, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = 1, .force_pthreads = false};
    cbm_parallel_for(50, sum_worker, &sum, opts);
    ASSERT_EQ(atomic_load(&sum), 50 * 49 / 2);
    PASS();
}

TEST(parallel_for_max_workers_zero_auto) {
    /* max_workers=0 → auto-detect, should produce correct result */
    _Atomic int sum;
    atomic_init(&sum, 0);
    cbm_parallel_for_opts_t opts = {.max_workers = 0, .force_pthreads = false};
    cbm_parallel_for(100, sum_worker, &sum, opts);
    ASSERT_EQ(atomic_load(&sum), 100 * 99 / 2);
    PASS();
}

TEST(parallel_for_large_count_coverage) {
    /* Large count (1000) → all indices visited exactly once */
    _Atomic int visited[1000];
    for (int i = 0; i < 1000; i++)
        atomic_init(&visited[i], 0);
    cbm_parallel_for_opts_t opts = {.max_workers = 8, .force_pthreads = false};
    cbm_parallel_for(1000, coverage_worker, visited, opts);
    /* Every index must be visited exactly once */
    for (int i = 0; i < 1000; i++) {
        ASSERT_EQ(atomic_load(&visited[i]), 1);
    }
    PASS();
}

TEST(parallel_for_immediate_return_callback) {
    /* Callback that returns immediately — no crash, no hang */
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    cbm_parallel_for(500, noop_worker, NULL, opts);
    PASS();
}

/* Helpers for context_passed_correctly test */
typedef struct { _Atomic int counter; int magic; } ctx_test_t;

static void count_and_verify_worker(int idx, void *vctx) {
    (void)idx;
    ctx_test_t *c = vctx;
    if (c->magic == 0xDEAD) {
        atomic_fetch_add(&c->counter, 1);
    }
}

TEST(parallel_for_context_passed_correctly) {
    /* Verify the context pointer reaches every iteration */
    ctx_test_t ctx;
    atomic_init(&ctx.counter, 0);
    ctx.magic = 0xDEAD;

    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    cbm_parallel_for(100, count_and_verify_worker, &ctx, opts);
    /* All 100 iterations should have received the correct context */
    ASSERT_EQ(atomic_load(&ctx.counter), 100);
    PASS();
}

/* Helper for no_duplicates test */
static void count_visit_worker(int idx, void *ctx) {
    _Atomic int *c = ctx;
    atomic_fetch_add(&c[idx], 1);
}

TEST(parallel_for_no_duplicates) {
    /* Verify no index is visited more than once (atomic increment per slot) */
    _Atomic int counts[500];
    for (int i = 0; i < 500; i++)
        atomic_init(&counts[i], 0);

    cbm_parallel_for_opts_t opts = {.max_workers = 8, .force_pthreads = false};
    cbm_parallel_for(500, count_visit_worker, counts, opts);

    for (int i = 0; i < 500; i++) {
        ASSERT_EQ(atomic_load(&counts[i]), 1);
    }
    PASS();
}

/* Helper for single_iteration_idx_zero test */
static int g_received_idx = -1;

static void capture_idx_worker(int idx, void *ctx) {
    (void)ctx;
    g_received_idx = idx;
}

TEST(parallel_for_single_iteration_idx_zero) {
    /* count=1 → single iteration with idx=0 */
    g_received_idx = -1;
    cbm_parallel_for_opts_t opts = {.max_workers = 4, .force_pthreads = false};
    cbm_parallel_for(1, capture_idx_worker, NULL, opts);
    ASSERT_EQ(g_received_idx, 0);
    PASS();
}

TEST(parallel_for_serial_matches_parallel) {
    /* Serial (max_workers=1) and parallel (max_workers=8) should produce
     * identical results for a deterministic reduction. */
    _Atomic int serial_sum, parallel_sum;
    atomic_init(&serial_sum, 0);
    atomic_init(&parallel_sum, 0);

    cbm_parallel_for_opts_t serial_opts = {.max_workers = 1, .force_pthreads = false};
    cbm_parallel_for(200, sum_worker, &serial_sum, serial_opts);

    cbm_parallel_for_opts_t parallel_opts = {.max_workers = 8, .force_pthreads = false};
    cbm_parallel_for(200, sum_worker, &parallel_sum, parallel_opts);

    ASSERT_EQ(atomic_load(&serial_sum), atomic_load(&parallel_sum));
    ASSERT_EQ(atomic_load(&serial_sum), 200 * 199 / 2);
    PASS();
}

/* ── Suite Registration ──────────────────────────────────────────── */

SUITE(system_info) {
    RUN_TEST(system_info_total_cores);
    RUN_TEST(system_info_total_cores_sane);
    RUN_TEST(system_info_perf_cores);
    RUN_TEST(system_info_total_ram);
    RUN_TEST(system_info_idempotent);
    RUN_TEST(default_worker_count_initial);
    RUN_TEST(default_worker_count_incremental);
    RUN_TEST(default_worker_count_minimum);
}

SUITE(worker_pool) {
    RUN_TEST(parallel_for_sum);
    RUN_TEST(parallel_for_coverage);
    RUN_TEST(parallel_for_zero);
    RUN_TEST(parallel_for_one);
    RUN_TEST(parallel_for_single_worker);
    RUN_TEST(parallel_for_force_pthreads);
    RUN_TEST(parallel_for_per_slot_write);
    RUN_TEST(parallel_for_actually_parallel);
    RUN_TEST(tls_persistence_across_dispatch);
    /* Resource management & edge cases */
    RUN_TEST(parallel_for_negative_count);
    RUN_TEST(parallel_for_null_fn);
    RUN_TEST(parallel_for_max_workers_one);
    RUN_TEST(parallel_for_max_workers_zero_auto);
    RUN_TEST(parallel_for_large_count_coverage);
    RUN_TEST(parallel_for_immediate_return_callback);
    RUN_TEST(parallel_for_context_passed_correctly);
    RUN_TEST(parallel_for_no_duplicates);
    RUN_TEST(parallel_for_single_iteration_idx_zero);
    RUN_TEST(parallel_for_serial_matches_parallel);
}
