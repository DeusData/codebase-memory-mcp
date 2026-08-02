/*
 * compat_thread.c — Portable thread, mutex, and aligned allocation.
 *
 * POSIX: thin wrappers around pthreads and posix_memalign.
 * Windows: CreateThread, CRITICAL_SECTION, _aligned_malloc.
 */
#include "foundation/constants.h"
#include "foundation/compat_thread.h"
#include "foundation/platform.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <mimalloc.h> /* mi_thread_done at thread exit */
#else
#include <pthread.h>
#endif

/* Default 8MB stack for all threads. macOS ARM64 default is only 512KB,
 * which is too small for deep pipeline passes (configlink, etc.). */
#define CBM_DEFAULT_STACK_SIZE ((size_t)8 * CBM_SZ_1K * CBM_SZ_1K)

/* ── Thread ───────────────────────────────────────────────────── */

#ifdef _WIN32

typedef struct {
    void *(*fn)(void *);
    void *arg;
} win_thread_arg_t;

/* Release each thread's allocator heap at DLL_THREAD_DETACH.
 *
 * Doing this from the thread wrapper instead crashes: the wrapper returns
 * before the platform has finished retiring the thread, and abandoning the
 * heap there raced with frees still in flight
 * (daemon_ipc_wait_forever_is_interruptible segfaulted, rc=139, reproducibly
 * and only with wrapper-side release enabled). A loader TLS callback is the
 * mechanism mimalloc itself uses under MSVC. It moves the release out of the
 * user-function wrapper and into Windows' DLL_THREAD_DETACH path, after the
 * thread entry point has returned.
 *
 * Without any of this, a static MinGW link -- no DllMain, no TLS callback --
 * never releases a thread heap at all: 607 heaps after 300 requests, 170 MiB
 * held against a ~300 KiB live set, growing without bound (#581). POSIX gets
 * this from a pthread TSD destructor, which is why only Windows leaked.
 *
 * CBM_MI_THREAD_DONE=0 disables it, so one binary can demonstrate both
 * behaviours rather than requiring a rebuild to establish causality. */
static bool thread_release_heap_enabled(void) {
    static int state = -1;
    if (state < 0) {
        char buf[CBM_SZ_16];
        state =
            (cbm_safe_getenv("CBM_MI_THREAD_DONE", buf, sizeof(buf), NULL) != NULL && buf[0] == '0')
                ? 0
                : 1;
    }
    return state == 1;
}

static void NTAPI cbm_thread_detach_callback(PVOID handle, DWORD reason, PVOID reserved) {
    (void)handle;
    (void)reserved;
    if (reason == DLL_THREAD_DETACH && thread_release_heap_enabled()) {
        mi_thread_done();
    }
}

/* Park the callback in .CRT$XLB, the table the loader walks. The linker only
 * emits a TLS directory when _tls_used is referenced, so anchor it. */
extern const IMAGE_TLS_DIRECTORY64 _tls_used;
static const void *const cbm_tls_anchor __attribute__((used)) = &_tls_used;
__attribute__((section(".CRT$XLB"), used)) PIMAGE_TLS_CALLBACK cbm_thread_detach_tls_cb =
    cbm_thread_detach_callback;

static DWORD WINAPI win_thread_wrapper(LPVOID lpParam) {
    win_thread_arg_t *a = (win_thread_arg_t *)lpParam;
    void *(*fn)(void *) = a->fn;
    void *arg = a->arg;
    free(a);
    fn(arg);
    /* Keep the allocator live while the thread returns through platform exit
     * machinery; DLL_THREAD_DETACH owns the eventual release above. */
    return 0;
}

int cbm_thread_create(cbm_thread_t *t, size_t stack_size, void *(*fn)(void *), void *arg) {
    if (stack_size == 0) {
        stack_size = CBM_DEFAULT_STACK_SIZE;
    }
    win_thread_arg_t *a = (win_thread_arg_t *)malloc(sizeof(win_thread_arg_t));
    if (!a) {
        return CBM_NOT_FOUND;
    }
    a->fn = fn;
    a->arg = arg;
    t->handle = CreateThread(NULL, stack_size, win_thread_wrapper, a, 0, NULL);
    if (!t->handle) {
        free(a);
        return CBM_NOT_FOUND;
    }
    return 0;
}

int cbm_thread_join(cbm_thread_t *t) {
    if (WaitForSingleObject(t->handle, INFINITE) != WAIT_OBJECT_0) {
        return CBM_NOT_FOUND;
    }
    CloseHandle(t->handle);
    t->handle = NULL;
    return 0;
}

int cbm_thread_detach(cbm_thread_t *t) {
    if (t->handle) {
        CloseHandle(t->handle);
        t->handle = NULL;
    }
    return 0;
}

#else /* POSIX */

int cbm_thread_create(cbm_thread_t *t, size_t stack_size, void *(*fn)(void *), void *arg) {
    if (stack_size == 0) {
        stack_size = CBM_DEFAULT_STACK_SIZE;
    }
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, stack_size);
    int rc = pthread_create(&t->handle, &attr, fn, arg);
    pthread_attr_destroy(&attr);
    return rc;
}

int cbm_thread_join(cbm_thread_t *t) {
    int rc = pthread_join(t->handle, NULL);
    if (rc == 0) {
        memset(&t->handle, 0, sizeof(t->handle));
    }
    return rc;
}

int cbm_thread_detach(cbm_thread_t *t) {
    int rc = pthread_detach(t->handle);
    if (rc == 0) {
        memset(&t->handle, 0, sizeof(t->handle));
    }
    return rc;
}

#endif

/* ── Mutex ────────────────────────────────────────────────────── */

#ifdef _WIN32

void cbm_mutex_init(cbm_mutex_t *m) {
    InitializeCriticalSection(&m->cs);
}

void cbm_mutex_lock(cbm_mutex_t *m) {
    EnterCriticalSection(&m->cs);
}

void cbm_mutex_unlock(cbm_mutex_t *m) {
    LeaveCriticalSection(&m->cs);
}

void cbm_mutex_destroy(cbm_mutex_t *m) {
    DeleteCriticalSection(&m->cs);
}

#else /* POSIX */

void cbm_mutex_init(cbm_mutex_t *m) {
    pthread_mutex_init(&m->mtx, NULL);
}

void cbm_mutex_lock(cbm_mutex_t *m) {
    pthread_mutex_lock(&m->mtx);
}

void cbm_mutex_unlock(cbm_mutex_t *m) {
    pthread_mutex_unlock(&m->mtx);
}

void cbm_mutex_destroy(cbm_mutex_t *m) {
    pthread_mutex_destroy(&m->mtx);
}

#endif

/* ── Condition variable ───────────────────────────────────────── */

#ifdef _WIN32

int cbm_thread_condition_init(cbm_thread_condition_t *condition) {
    InitializeConditionVariable(&condition->condition);
    return 0;
}

void cbm_thread_condition_destroy(cbm_thread_condition_t *condition) {
    (void)condition; /* Win32 condition variables require no destruction. */
}

void cbm_thread_condition_broadcast(cbm_thread_condition_t *condition) {
    WakeAllConditionVariable(&condition->condition);
}

cbm_thread_condition_wait_status_t cbm_thread_condition_wait(cbm_thread_condition_t *condition,
                                                             cbm_mutex_t *mutex) {
    return SleepConditionVariableCS(&condition->condition, &mutex->cs, INFINITE)
               ? CBM_THREAD_CONDITION_WAIT_SIGNALED
               : CBM_THREAD_CONDITION_WAIT_ERROR;
}

cbm_thread_condition_wait_status_t cbm_thread_condition_wait_until(
    cbm_thread_condition_t *condition, cbm_mutex_t *mutex, uint64_t deadline_ms) {
    uint64_t now_ms = cbm_now_ms();
    uint64_t remaining_ms = deadline_ms > now_ms ? deadline_ms - now_ms : 0;
    DWORD timeout_ms =
        remaining_ms < (uint64_t)INFINITE ? (DWORD)remaining_ms : (DWORD)(INFINITE - 1U);
    if (SleepConditionVariableCS(&condition->condition, &mutex->cs, timeout_ms)) {
        return CBM_THREAD_CONDITION_WAIT_SIGNALED;
    }
    return GetLastError() == ERROR_TIMEOUT ? CBM_THREAD_CONDITION_WAIT_TIMEOUT
                                           : CBM_THREAD_CONDITION_WAIT_ERROR;
}

#else /* POSIX */

int cbm_thread_condition_init(cbm_thread_condition_t *condition) {
#ifdef __APPLE__
    return pthread_cond_init(&condition->condition, NULL);
#else
    pthread_condattr_t attributes;
    int status = pthread_condattr_init(&attributes);
    if (status != 0) {
        return status;
    }
    status = pthread_condattr_setclock(&attributes, CLOCK_MONOTONIC);
    if (status == 0) {
        status = pthread_cond_init(&condition->condition, &attributes);
    }
    (void)pthread_condattr_destroy(&attributes);
    return status;
#endif
}

void cbm_thread_condition_destroy(cbm_thread_condition_t *condition) {
    (void)pthread_cond_destroy(&condition->condition);
}

void cbm_thread_condition_broadcast(cbm_thread_condition_t *condition) {
    (void)pthread_cond_broadcast(&condition->condition);
}

cbm_thread_condition_wait_status_t cbm_thread_condition_wait(cbm_thread_condition_t *condition,
                                                             cbm_mutex_t *mutex) {
    return pthread_cond_wait(&condition->condition, &mutex->mtx) == 0
               ? CBM_THREAD_CONDITION_WAIT_SIGNALED
               : CBM_THREAD_CONDITION_WAIT_ERROR;
}

cbm_thread_condition_wait_status_t cbm_thread_condition_wait_until(
    cbm_thread_condition_t *condition, cbm_mutex_t *mutex, uint64_t deadline_ms) {
    struct timespec timeout;
#ifdef __APPLE__
    uint64_t now_ms = cbm_now_ms();
    uint64_t remaining_ms = deadline_ms > now_ms ? deadline_ms - now_ms : 0;
    timeout.tv_sec = (time_t)(remaining_ms / CBM_MSEC_PER_SEC);
    timeout.tv_nsec = (long)((remaining_ms % CBM_MSEC_PER_SEC) * CBM_NSEC_PER_MSEC);
    int status = pthread_cond_timedwait_relative_np(&condition->condition, &mutex->mtx, &timeout);
#else
    timeout.tv_sec = (time_t)(deadline_ms / CBM_MSEC_PER_SEC);
    timeout.tv_nsec = (long)((deadline_ms % CBM_MSEC_PER_SEC) * CBM_NSEC_PER_MSEC);
    int status = pthread_cond_timedwait(&condition->condition, &mutex->mtx, &timeout);
#endif
    if (status == 0) {
        return CBM_THREAD_CONDITION_WAIT_SIGNALED;
    }
    return status == ETIMEDOUT ? CBM_THREAD_CONDITION_WAIT_TIMEOUT
                               : CBM_THREAD_CONDITION_WAIT_ERROR;
}

#endif

/* ── Aligned allocation ───────────────────────────────────────── */

#ifdef _WIN32

int cbm_aligned_alloc(void **ptr, size_t alignment, size_t size) {
    *ptr = _aligned_malloc(size, alignment);
    return *ptr ? 0 : -1;
}

void cbm_aligned_free(void *ptr) {
    _aligned_free(ptr);
}

#else /* POSIX */

int cbm_aligned_alloc(void **ptr, size_t alignment, size_t size) {
    return posix_memalign(ptr, alignment, size);
}

void cbm_aligned_free(void *ptr) {
    free(ptr);
}

#endif
