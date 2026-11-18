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

typedef enum {
    CBM_DAEMON_CONFLICT_LOG_BEFORE_SERIALIZATION_LOCK = 1,
    CBM_DAEMON_CONFLICT_LOG_AFTER_SERIALIZATION_LOCK,
} cbm_daemon_conflict_log_test_stage_t;

typedef void (*cbm_daemon_conflict_log_test_hook_fn)(void *context,
                                                     cbm_daemon_conflict_log_test_stage_t stage);

void cbm_daemon_conflict_log_set_test_hook(cbm_daemon_conflict_log_test_hook_fn hook,
                                           void *context);

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* TEMPORARY diagnostic (drop before merge): read CBM_SHA256_DIAG_FILE once,
 * before any thread starts; then every full-file fingerprint appends one
 * "CBM-SHA256-DIAG kind=hash" line to it. The compare call (at most once per
 * process) times the selected backend against the portable transform on the
 * same bytes. POSIX only; no-ops on Windows. */
void cbm_daemon_fingerprint_diag_init_for_testing(void);
void cbm_daemon_fingerprint_diag_compare_for_testing(const char *path);
#endif

#endif /* CBM_DAEMON_SERVICE_INTERNAL_H */
