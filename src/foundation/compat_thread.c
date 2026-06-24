/*
 * compat_thread.c — Portable thread, mutex, and aligned allocation.
 *
 * POSIX: thin wrappers around pthreads and posix_memalign.
 * Windows: CreateThread, CRITICAL_SECTION, _aligned_malloc.
 */
#include "foundation/constants.h"
#include "foundation/compat_thread.h"

#include <pthread.h>
#include <stdlib.h>

/* Default 8MB stack for all threads. macOS ARM64 default is only 512KB,
 * which is too small for deep pipeline passes (configlink, etc.). */
#define CBM_DEFAULT_STACK_SIZE ((size_t)8 * CBM_SZ_1K * CBM_SZ_1K)
#include <string.h>

/* ── Thread ───────────────────────────────────────────────────── */

#ifdef _WIN32

typedef struct {
    void *(*fn)(void *);
    void *arg;
} win_thread_arg_t;

static DWORD WINAPI win_thread_wrapper(LPVOID lpParam) {
    win_thread_arg_t *a = (win_thread_arg_t *)lpParam;
    void *(*fn)(void *) = a->fn;
    void *arg = a->arg;
    free(a);
    fn(arg);
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
