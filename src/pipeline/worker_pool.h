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
    int max_workers;     /* 0 = auto-detect from cbm_default_worker_count */
    bool force_pthreads; /* unused, kept for API compat */
} cbm_parallel_for_opts_t;

/* Bound indexing parallelism by CPU, project size and physical/cgroup memory.
 * This avoids creating more parser workers than a small repository can feed
 * or a constrained machine can hold. */
int cbm_worker_count_for_files(int file_count, bool initial);

/* Dispatch `count` iterations of `fn(idx, ctx)` across worker threads.
 * Each index [0..count-1] is visited exactly once.
 * Blocks until all iterations complete.
 *
 * If count <= 0, this is a no-op.
 * If count <= 1 or workers <= 1, runs single-threaded. */
void cbm_parallel_for(int count, cbm_parallel_fn fn, void *ctx, cbm_parallel_for_opts_t opts);

#endif /* CBM_WORKER_POOL_H */
