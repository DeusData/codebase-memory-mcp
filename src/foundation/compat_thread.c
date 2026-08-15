/*
 * compat_thread.c — Portable thread, mutex, and aligned allocation.
 *
 * POSIX: thin wrappers around pthreads and posix_memalign.
 * Windows: CreateThread, CRITICAL_SECTION, _aligned_malloc.
 */
#include "foundation/constants.h"
#include "foundation/compat_thread.h"

#include "foundation/platform.h"
#include "foundation/sanitized.h" /* CBM_SANITIZED — diagnostic stack floor */

#include <mimalloc.h> /* mi_thread_done at thread exit */

#include <pthread.h>
#include <stdlib.h>

/* Default 8MB stack for all threads. macOS ARM64 default is only 512KB,
 * which is too small for deep pipeline passes (configlink, etc.). */
#define CBM_DEFAULT_STACK_SIZE ((size_t)8 * CBM_SZ_1K * CBM_SZ_1K)

#include <string.h>

/* Thread stacks are sized in code (explicitly by most callers, by the default
 * above otherwise), so RLIMIT_STACK does NOT reach them and a sanitizer lane
 * cannot size them with ulimit. MemorySanitizer's origin tracking inflates
 * every frame enough that deep parser recursion overflows the normal worker
 * stack, and without this hook that lane cannot run the parsing suites at all.
 *
 * The floor applies to EVERY thread — an earlier version only replaced the
 * DEFAULT, which silently did nothing for worker_pool/runtime/main, i.e. for
 * exactly the threads that overflow. Diagnostic builds only: the shipping
 * binary keeps its fixed, predictable stack sizes. */
static size_t cbm_thread_stack_floor(size_t requested) {
#if CBM_SANITIZED
    const char *env = getenv("CBM_THREAD_STACK_MB");
    if (env && env[0]) {
        char *end = NULL;
        unsigned long mb = strtoul(env, &end, 10);
        if (end && *end == '\0' && mb > 0 && mb <= 1024) {
            size_t floor_bytes = (size_t)mb * CBM_SZ_1K * CBM_SZ_1K;
            if (floor_bytes > requested) {
                return floor_bytes;
            }
        }
    }
#endif
    return requested;
}

static size_t cbm_thread_default_stack_size(void) {
    return cbm_thread_stack_floor(CBM_DEFAULT_STACK_SIZE);
}

/* ── Thread ───────────────────────────────────────────────────── */

#ifdef _WIN32

typedef struct {
    void *(*fn)(void *);
    void *arg;
} win_thread_arg_t;

/* Release each thread's allocator heap at DLL_THREAD_DETACH.
 *
 * Doing this from the thread wrapper instead crashes: the wrapper returns
 * before the CRT's own thread teardown, and abandoning the heap there raced
 * with frees still in flight (daemon_ipc_wait_forever_is_interruptible
 * segfaulted, rc=139, reproducibly and only with the release enabled). A TLS
 * callback is the mechanism mimalloc itself uses under MSVC and runs after all
 * other thread cleanup, which is the only point where the heap is genuinely
 * unreferenced.
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
    /* Release this thread's allocator heap before the thread dies.
     *
     * On POSIX a pthread TSD destructor calls this for us, which is why POSIX
     * stays flat. A static MinGW link has no DllMain and registers no TLS
     * callback, so nothing runs at thread exit and every thread leaves its
     * thread-heap behind for the life of the process: 607 heaps after 300
     * requests, 170 MiB committed against a live set of ~300 KiB, growing
     * without bound (#581). The heaps are invisible to mi_heap_visit -- which
     * only sees the main and calling heaps -- which is why the growth looked
     * like it belonged to no allocator at all. */
    return 0;
}

int cbm_thread_create(cbm_thread_t *t, size_t stack_size, void *(*fn)(void *), void *arg) {
    if (stack_size == 0) {
        stack_size = cbm_thread_default_stack_size();
    } else {
        stack_size = cbm_thread_stack_floor(stack_size);
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
        stack_size = cbm_thread_default_stack_size();
    } else {
        stack_size = cbm_thread_stack_floor(stack_size);
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
