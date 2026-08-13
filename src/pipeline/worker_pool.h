/*
 * worker_pool.h — Generic parallel-for dispatch.
 *
 * Backend: pthreads with 8MB stacks and atomic work-stealing index.
 * Each worker pulls from a shared counter — zero contention, natural
 * load balancing across heterogeneous cores (P/E on Apple Silicon).
 *
 * Serial fallback when count <= 1 or max_workers <= 1.
 */
#ifndef CBM_WORKER_POOL_H
#define CBM_WORKER_POOL_H

#include <stdbool.h>

/* Worker callback: called once per iteration with index [0..count-1]. */
typedef void (*cbm_parallel_fn)(int idx, void *ctx);

/* Options for parallel dispatch. */
typedef struct {
    int max_workers;     /* 0 = auto-detect; total callbacks active at once */
    bool force_pthreads; /* unused, kept for API compat */
} cbm_parallel_for_opts_t;

/* Dispatch `count` iterations of `fn(idx, ctx)` across worker threads plus
 * the caller thread, with no more than opts.max_workers callbacks active.
 * Worker count is clamped to count; no idle helper threads are created.
 * Each index [0..count-1] is visited exactly once.
 * Blocks until all iterations complete.
 *
 * If count <= 0, this is a no-op.
 * If count <= 1 or workers <= 1, runs single-threaded. */
void cbm_parallel_for(int count, cbm_parallel_fn fn, void *ctx, cbm_parallel_for_opts_t opts);

#endif /* CBM_WORKER_POOL_H */
