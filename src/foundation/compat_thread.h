/*
 * compat_thread.h — Portable threading: pthreads on POSIX, Win32 threads on Windows.
 *
 * Provides: thread create/join, mutex, condition variable, aligned allocation.
 * All have zero overhead on POSIX (thin inlines or macros).
 */
#ifndef CBM_COMPAT_THREAD_H
#define CBM_COMPAT_THREAD_H

#include <stddef.h>
#include <stdint.h>

/* ── Thread ───────────────────────────────────────────────────── */

#ifdef _WIN32

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

typedef struct {
    HANDLE handle;
} cbm_thread_t;

#else /* POSIX */

#include <pthread.h>

typedef struct {
    pthread_t handle;
} cbm_thread_t;

#endif

/* Create a thread with the given stack size (0 = OS default).
 * fn receives arg. Returns 0 on success. */
int cbm_thread_create(cbm_thread_t *t, size_t stack_size, void *(*fn)(void *), void *arg);

/* Wait for thread to finish. Returns 0 on success. */
int cbm_thread_join(cbm_thread_t *t);

/* Detach thread so resources are freed on exit. Returns 0 on success. */
int cbm_thread_detach(cbm_thread_t *t);

/* ── Mutex ────────────────────────────────────────────────────── */

#ifdef _WIN32

typedef struct {
    CRITICAL_SECTION cs;
} cbm_mutex_t;

#else

typedef struct {
    pthread_mutex_t mtx;
} cbm_mutex_t;

#endif

void cbm_mutex_init(cbm_mutex_t *m);
void cbm_mutex_lock(cbm_mutex_t *m);
void cbm_mutex_unlock(cbm_mutex_t *m);
void cbm_mutex_destroy(cbm_mutex_t *m);

/* ── Condition variable ───────────────────────────────────────── */

#ifdef _WIN32

typedef struct {
    CONDITION_VARIABLE condition;
} cbm_thread_condition_t;

#else

typedef struct {
    pthread_cond_t condition;
} cbm_thread_condition_t;

#endif

typedef enum {
    CBM_THREAD_CONDITION_WAIT_ERROR = -1,
    CBM_THREAD_CONDITION_WAIT_SIGNALED = 0,
    CBM_THREAD_CONDITION_WAIT_TIMEOUT = 1,
} cbm_thread_condition_wait_status_t;

/* Initialize/destroy a condition variable associated with caller-owned
 * cbm_mutex_t instances. init returns 0 on success. */
int cbm_thread_condition_init(cbm_thread_condition_t *condition);
void cbm_thread_condition_destroy(cbm_thread_condition_t *condition);

/* Wake every waiter. The caller must hold the mutex used by those waiters so
 * predicate publication and wakeup form one indivisible state transition. */
void cbm_thread_condition_broadcast(cbm_thread_condition_t *condition);

/* Atomically release mutex and wait until broadcast or an absolute monotonic
 * deadline from cbm_now_ms(). Reacquires mutex before returning. Callers must
 * loop on their predicate because condition variables may wake spuriously. */
cbm_thread_condition_wait_status_t cbm_thread_condition_wait_until(
    cbm_thread_condition_t *condition, cbm_mutex_t *mutex, uint64_t deadline_ms);

/* ── Aligned allocation ───────────────────────────────────────── */

/* Allocate size bytes aligned to alignment boundary.
 * Returns 0 on success, non-zero on failure. *ptr receives the allocation. */
int cbm_aligned_alloc(void **ptr, size_t alignment, size_t size);

/* Free memory from cbm_aligned_alloc. */
void cbm_aligned_free(void *ptr);

#endif /* CBM_COMPAT_THREAD_H */
