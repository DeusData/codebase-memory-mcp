/*
 * platform.h — OS abstractions.
 *
 * Provides cross-platform wrappers for:
 *   - Memory-mapped files (mmap / VirtualAlloc)
 *   - High-resolution monotonic clock
 *   - CPU core count
 *   - File existence check
 */
#ifndef CBM_PLATFORM_H
#define CBM_PLATFORM_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>

/* ── Safe memory ──────────────────────────────────────────────── */

/* Safe realloc: frees old pointer on failure instead of leaking it.
 * Returns NULL on allocation failure (old memory is freed). */
static inline void *safe_realloc(void *ptr, size_t size) {
    enum { SAFE_REALLOC_MIN = 1 };
    if (size == 0) {
        size = SAFE_REALLOC_MIN;
    }
    void *tmp = realloc(ptr, size);
    if (!tmp) {
        free(ptr);
    }
    return tmp;
}

/* Safe free: frees and NULLs a pointer to prevent double-free / use-after-free.
 * Accepts void** so it works with any pointer type via the macro. */
static inline void safe_free_impl(void **pp) {
    if (pp && *pp) {
        free(*pp);
        *pp = NULL;
    }
}
#define safe_free(ptr) safe_free_impl((void **)(void *)&(ptr))

/* Safe string free: frees a const char* and NULLs it.
 * Casts away const so callers don't need the (void*) dance. */
static inline void safe_str_free(const char **sp) {
    if (sp && *sp) {
        free((void *)*sp);
        *sp = NULL;
    }
}

/* Safe buffer free: frees a heap array and zeros its element count.
 * Use for dynamic arrays paired with a size_t count. */
static inline void safe_buf_free_impl(void **buf, size_t *count) {
    if (buf && *buf) {
        free(*buf);
        *buf = NULL;
    }
    if (count) {
        *count = 0;
    }
}
#define safe_buf_free(buf, countp) safe_buf_free_impl((void **)(void *)&(buf), (countp))

/* Safe grow: doubles capacity and reallocs when count reaches cap.
 * Usage: safe_grow(arr, count, cap, growth_factor)
 * Evaluates to the new arr (NULL on OOM — old memory freed by safe_realloc). */
#define safe_grow(arr, n, cap, factor) do { \
    if ((size_t)(n) >= (size_t)(cap)) { \
        (cap) *= (factor); \
        (arr) = safe_realloc((arr), (size_t)(cap) * sizeof(*(arr))); \
    } \
} while (0)

/* ── Memory mapping ────────────────────────────────────────────── */

/* Map a file read-only into memory. Returns NULL on error.
 * *out_size is set to the file size. */
void *cbm_mmap_read(const char *path, size_t *out_size);

/* Unmap a previously mapped region. */
void cbm_munmap(void *addr, size_t size);

/* ── Timing ────────────────────────────────────────────────────── */

/* Monotonic nanosecond timestamp (for elapsed time measurement). */
uint64_t cbm_now_ns(void);

/* Monotonic millisecond timestamp. */
uint64_t cbm_now_ms(void);

/* ── System info ───────────────────────────────────────────────── */

/* Number of available CPU cores. */
int cbm_nprocs(void);

/* System topology: core types and RAM (only fields with production consumers). */
typedef struct {
    int total_cores;  /* hw.ncpu (all cores) */
    int perf_cores;   /* P-cores (Apple) or total_cores (others) */
    size_t total_ram; /* total physical RAM in bytes */
} cbm_system_info_t;

/* Query system information. Results are cached after first call. */
cbm_system_info_t cbm_system_info(void);

/* Recommended worker count for parallel indexing.
 * initial=true:  all cores (user is waiting for initial index)
 * initial=false: max(1, perf_cores-1) (leave headroom for user apps) */
int cbm_default_worker_count(bool initial);

/* ── Environment variables ──────────────────────────────────────── */

/* Thread-safe getenv: copies the value into a caller-provided buffer.
 * Returns buf on success, or fallback if the variable is unset.
 * Returns NULL when the variable is unset and fallback is NULL. */
const char *cbm_safe_getenv(const char *name, char *buf, size_t buf_sz, const char *fallback);

/* ── Home directory ─────────────────────────────────────────────── */

/* Cross-platform home directory: tries HOME first, then USERPROFILE (Windows).
 * Returns NULL when neither is set. */
const char *cbm_get_home_dir(void);

/* ── App config directories ────────────────────────────────────── */

/* Cross-platform app config directory (static buffer, not thread-safe).
 * Windows: %APPDATA% (e.g. C:/Users/.../AppData/Roaming)
 * macOS:   $HOME (callers append Library/Application Support/...)
 * Linux:   $XDG_CONFIG_HOME or ~/.config */
const char *cbm_app_config_dir(void);

/* Windows: %LOCALAPPDATA% (e.g. C:/Users/.../AppData/Local)
 * macOS/Linux: same as cbm_app_config_dir(). */
const char *cbm_app_local_dir(void);

/* ── Cache directory ────────────────────────────────────────────── */

/* Resolve the database cache directory. All project indexes are stored here.
 * Priority: CBM_CACHE_DIR env var > ~/.cache/codebase-memory-mcp (default).
 * Returns static buffer or NULL if home is unavailable. */
const char *cbm_resolve_cache_dir(void);

/* ── File system ───────────────────────────────────────────────── */

/* Check if a path exists. */
bool cbm_file_exists(const char *path);

/* Check if path is a directory. */
bool cbm_is_dir(const char *path);

/* Get file size. Returns -1 on error. */
int64_t cbm_file_size(const char *path);

/* Normalize path separators to forward slashes (in-place).
 * On Windows, converts backslashes to forward slashes.
 * On POSIX, this is a no-op. Returns the input pointer. */
char *cbm_normalize_path_sep(char *path);

#endif /* CBM_PLATFORM_H */
