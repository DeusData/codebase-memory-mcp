/*
 * worker_pool.c — Parallel-for dispatch with pthreads.
 *
 * Uses pthreads with 8MB stacks and atomic work-stealing index.
 * GCD is avoided because its worker threads have 512KB stacks,
 * which overflows on deeply nested ASTs (tree-sitter + walk_defs).
 *
 * Each worker pulls indices from a shared atomic counter — zero
 * contention, natural load balancing across heterogeneous cores.
 */
#include "foundation/mem_events.h"
#include "pipeline/worker_pool.h"
#include "foundation/constants.h"

enum { WP_TRUE = 1, WP_MIN = 1, WP_STEP = 1 };
#include "foundation/platform.h"
#include "foundation/compat_thread.h"
#include "foundation/mem_core.h"

#include <stdatomic.h>
#include <stdlib.h>

/* 8 MB stack per worker — matches main thread default.
 * Required for deep AST recursion (tree-sitter + walk_defs). */
#define CBM_WORKER_STACK_SIZE ((size_t)8 * CBM_SZ_1K * CBM_SZ_1K)

/* ── Serial fallback ─────────────────────────────────────────────── */

static void run_serial(int count, cbm_parallel_fn fn, void *ctx) {
    for (int i = 0; i < count; i++) {
        fn(i, ctx);
    }
}

/* ── pthreads backend ────────────────────────────────────────────── */

typedef struct {
    cbm_parallel_fn fn;
    void *ctx;
    _Atomic int *next_idx;
    int count;
} pthread_worker_arg_t;

#if defined(CBM_MEMWASTE) && CBM_MEMWASTE
/* Per-thread item counts, so the waste sanitizer can see an unbalanced pool:
 * workers that sat idle while one thread did the work. */
typedef struct {
    pthread_worker_arg_t *wa;
    int done;
    uint64_t ops; /* events this worker produced: the work it actually did */
} wp_counted_arg_t;

static void *pthread_worker_counted(void *arg) {
    wp_counted_arg_t *ca = arg;
    pthread_worker_arg_t *wa = ca->wa;
    uint64_t ops_before = cbm_memev_thread_ops();
    while (WP_TRUE) {
        int idx = atomic_fetch_add_explicit(wa->next_idx, WP_STEP, memory_order_relaxed);
        if (idx >= wa->count) {
            break;
        }
        wa->fn(idx, wa->ctx);
        ca->done++;
    }
    ca->ops = cbm_memev_thread_ops() - ops_before;
    cbm_mem_class_flush_thread();
    return NULL;
}
#endif

static void *pthread_worker(void *arg) {
    pthread_worker_arg_t *wa = arg;
    while (WP_TRUE) {
        int idx = atomic_fetch_add_explicit(wa->next_idx, WP_STEP, memory_order_relaxed);
        if (idx >= wa->count) {
            break;
        }
        wa->fn(idx, wa->ctx);
    }
    /* This thread ends here: hand its pending memory-class deltas to the
     * shared counters, so the phase mark that follows the join is exact. */
    cbm_mem_class_flush_thread();
    return NULL;
}

#if defined(CBM_MEMWASTE) && CBM_MEMWASTE
static void run_pthreads_counted(int count, cbm_parallel_fn fn, void *ctx, int nworkers,
                                 void *site) {
    _Atomic int next_idx = 0;
    pthread_worker_arg_t wa = {.fn = fn, .ctx = ctx, .next_idx = &next_idx, .count = count};
    cbm_thread_t *threads = cbm_alloc(CBM_MEM_CLASS_OTHER, (size_t)nworkers * sizeof(cbm_thread_t));
    wp_counted_arg_t *args = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)nworkers * sizeof(*args));
    if (!threads || !args) {
        cbm_free(CBM_MEM_CLASS_OTHER, threads);
        cbm_free(CBM_MEM_CLASS_OTHER, args);
        run_serial(count, fn, ctx);
        return;
    }
    int started = 0;
    for (int i = 0; i < nworkers; i++) {
        args[i].wa = &wa;
        if (cbm_thread_create(&threads[i], CBM_WORKER_STACK_SIZE, pthread_worker_counted,
                              &args[i]) != 0) {
            break;
        }
        started++;
    }
    int main_done = 0;
    uint64_t main_ops_before = cbm_memev_thread_ops();
    while (WP_TRUE) {
        int idx = atomic_fetch_add_explicit(&next_idx, WP_STEP, memory_order_relaxed);
        if (idx >= count) {
            break;
        }
        fn(idx, ctx);
        main_done++;
    }
    uint64_t main_ops = cbm_memev_thread_ops() - main_ops_before;
    for (int i = 0; i < started; i++) {
        cbm_thread_join(&threads[i]);
    }
    int max_done = main_done;
    uint64_t max_ops = main_ops;
    uint64_t sum_ops = main_ops;
    for (int i = 0; i < started; i++) {
        if (args[i].done > max_done) {
            max_done = args[i].done;
        }
        if (args[i].ops > max_ops) {
            max_ops = args[i].ops;
        }
        sum_ops += args[i].ops;
    }
    int participants = started + 1;
    uint64_t balanced_max = (uint64_t)participants * (uint64_t)max_done;
    uint64_t imbalance = balanced_max > (uint64_t)count ? balanced_max - (uint64_t)count : 0;
    cbm_work_note(CBM_WORK_PARALLEL_FOR, site, (uint64_t)count, imbalance, (uint64_t)participants);
    cbm_work_note(CBM_WORK_POOL_OPS, site, sum_ops, (uint64_t)participants * max_ops - sum_ops,
                  (uint64_t)participants);
    cbm_free(CBM_MEM_CLASS_OTHER, threads);
    cbm_free(CBM_MEM_CLASS_OTHER, args);
}
#endif

static void run_pthreads(int count, cbm_parallel_fn fn, void *ctx, int nworkers) {
    _Atomic int next_idx = 0;

    pthread_worker_arg_t wa = {
        .fn = fn,
        .ctx = ctx,
        .next_idx = &next_idx,
        .count = count,
    };

    cbm_thread_t *threads = (cbm_thread_t *)malloc((size_t)nworkers * sizeof(cbm_thread_t));
    if (!threads) {
        run_serial(count, fn, ctx);
        return;
    }

    for (int i = 0; i < nworkers; i++) {
        if (cbm_thread_create(&threads[i], CBM_WORKER_STACK_SIZE, pthread_worker, &wa) != 0) {
            /* Failed to create thread — let remaining work run in main thread */
            nworkers = i;
            break;
        }
    }

    /* Main thread also participates */
    while (WP_TRUE) {
        int idx = atomic_fetch_add_explicit(&next_idx, WP_STEP, memory_order_relaxed);
        if (idx >= count) {
            break;
        }
        fn(idx, ctx);
    }

    for (int i = 0; i < nworkers; i++) {
        cbm_thread_join(&threads[i]);
    }

    free(threads);
}

/* ── Public API ──────────────────────────────────────────────────── */

void cbm_parallel_for(int count, cbm_parallel_fn fn, void *ctx, cbm_parallel_for_opts_t opts) {
    if (count <= 0 || !fn) {
        return;
    }

    /* Determine worker count */
    int nworkers = opts.max_workers;
    if (nworkers <= 0) {
        nworkers = cbm_default_worker_count(true);
    }
    if (nworkers < WP_MIN) {
        nworkers = SKIP_ONE;
    }

    /* Serial fallback: single worker or trivially small workload */
    if (nworkers <= WP_MIN || count <= WP_MIN) {
        run_serial(count, fn, ctx);
        return;
    }

#if defined(CBM_MEMWASTE) && CBM_MEMWASTE
    if (cbm_memev_enabled()) {
        run_pthreads_counted(count, fn, ctx, nworkers, __builtin_return_address(0));
        return;
    }
#endif
    run_pthreads(count, fn, ctx, nworkers);
}
