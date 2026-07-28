/*
 * service_internal.h — Internal daemon service helpers and test hooks.
 *
 * Production callers outside the daemon implementation must use
 * daemon/service.h.  Runtime identity verification hashes an already-open,
 * kernel-bound file handle through this internal boundary; the test hook lets
 * concurrency tests establish an exact interleaving without scheduler luck.
 */
#ifndef CBM_DAEMON_SERVICE_INTERNAL_H
#define CBM_DAEMON_SERVICE_INTERNAL_H

#include "daemon/service.h"

#include <stdbool.h>
#include <stdint.h>

/* Hash an already-open regular file without closing it. native_file is an int
 * file descriptor on POSIX and a HANDLE cast through uintptr_t on Windows.
 * Callers use this after binding the handle to kernel process-image metadata,
 * avoiding a second lookup through a replaceable pathname. */
bool cbm_daemon_build_fingerprint_native_file(uintptr_t native_file,
                                              char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]);

/* Reuse an exact digest only when cache_path contains a complete, checksummed
 * record for the same open file identity, size, and nanosecond change
 * metadata. The owner-private directory containing cache_path is a caller
 * precondition. Cache I/O or validation failures fall back to hashing the
 * native file and never weaken the exact digest. */
bool cbm_daemon_build_fingerprint_native_file_cached(
    uintptr_t native_file, const char *cache_path, bool allow_cache,
    char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE], bool *cache_hit_out);

#if defined(CBM_CLI_ENABLE_TEST_API)
/* Path-opening test seam for the native-file cache contract. Production
 * process identity remains bound to a kernel process-image handle. */
bool cbm_daemon_build_fingerprint_file_cached_for_testing(
    const char *path, const char *cache_path, bool allow_cache,
    char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE], bool *cache_hit_out);
#endif

typedef enum {
    CBM_DAEMON_CONFLICT_LOG_BEFORE_SERIALIZATION_LOCK = 1,
    CBM_DAEMON_CONFLICT_LOG_AFTER_SERIALIZATION_LOCK,
} cbm_daemon_conflict_log_test_stage_t;

typedef void (*cbm_daemon_conflict_log_test_hook_fn)(void *context,
                                                     cbm_daemon_conflict_log_test_stage_t stage);

void cbm_daemon_conflict_log_set_test_hook(cbm_daemon_conflict_log_test_hook_fn hook,
                                           void *context);

#endif /* CBM_DAEMON_SERVICE_INTERNAL_H */
