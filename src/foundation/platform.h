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
    void *tmp = realloc(ptr, size); // NOLINT(clang-analyzer-optin.portability.UnixAPI)
    if (!tmp) {
        free(ptr);
    }
    return tmp;
}

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

/* ── Home directory ─────────────────────────────────────────────── */

/* Cross-platform home directory: tries HOME first, then USERPROFILE (Windows).
 * Returns NULL when neither is set. */
const char *cbm_get_home_dir(void);

/* ── File system ───────────────────────────────────────────────── */

/* Check if a path exists. */
bool cbm_file_exists(const char *path);

/* Check if path is a directory. */
bool cbm_is_dir(const char *path);

/* Get file size. Returns -1 on error. */
int64_t cbm_file_size(const char *path);

#endif /* CBM_PLATFORM_H */
