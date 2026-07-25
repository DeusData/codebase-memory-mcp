#ifndef CBM_LOCK_REGISTRY_INTERNAL_H
#define CBM_LOCK_REGISTRY_INTERNAL_H

#include "foundation/lock_registry.h"
#include "foundation/private_file_lock_internal.h"

typedef enum {
    CBM_LOCK_REGISTRY_STAGE_TURN_BUSY = 1,
    CBM_LOCK_REGISTRY_STAGE_TURN_HELD = 2,
    CBM_LOCK_REGISTRY_STAGE_RW_BUSY = 3,
    CBM_LOCK_REGISTRY_STAGE_NATIVE_READY = 4,
} cbm_lock_registry_stage_t;

typedef void (*cbm_lock_registry_stage_hook_fn)(void *context, cbm_private_file_lock_mode_t mode,
                                                cbm_lock_registry_stage_t stage);

bool cbm_lock_registry_resource_names(const char *resource_key,
                                      char turn_out[CBM_LOCK_REGISTRY_NAME_CAP],
                                      char rw_out[CBM_LOCK_REGISTRY_NAME_CAP]);

size_t cbm_lock_registry_waiter_count(cbm_lock_registry_t *registry);
size_t cbm_lock_registry_active_lease_count_for_test(cbm_lock_registry_t *registry);
size_t cbm_lock_registry_pending_cleanup_count_for_test(cbm_lock_registry_t *registry);
bool cbm_lock_registry_is_retired_for_test(const cbm_lock_registry_t *registry);
size_t cbm_lock_registry_attempting_waiter_count_for_test(cbm_lock_registry_t *registry);
uint64_t cbm_lock_registry_condition_wait_call_count_for_test(const cbm_lock_registry_t *registry);
size_t cbm_lock_registry_condition_waiter_count_for_test(const cbm_lock_registry_t *registry);

typedef enum {
    CBM_LOCK_REGISTRY_RELEASE_RW = 1,
    CBM_LOCK_REGISTRY_RELEASE_TURN = 2,
} cbm_lock_registry_release_handle_t;

bool cbm_lock_lease_fail_next_release_step_for_test(cbm_lock_lease_t *lease,
                                                    cbm_lock_registry_release_handle_t handle,
                                                    cbm_private_file_lock_release_step_t step);
bool cbm_lock_lease_has_release_handle_for_test(const cbm_lock_lease_t *lease,
                                                cbm_lock_registry_release_handle_t handle);
bool cbm_lock_lease_used_abort_lock_failure_path_for_test(const cbm_lock_lease_t *lease);
#ifndef _WIN32
bool cbm_lock_lease_fail_close_after_consuming_for_test(cbm_lock_lease_t *lease,
                                                        cbm_lock_registry_release_handle_t handle);
#endif
bool cbm_lock_registry_fail_next_native_release_step_for_test(
    cbm_lock_registry_t *registry, cbm_lock_registry_release_handle_t handle,
    cbm_private_file_lock_release_step_t step);

typedef enum {
    CBM_LOCK_REGISTRY_ABORT_FAIL_LOCK = 1,
    CBM_LOCK_REGISTRY_ABORT_FAIL_REMOVE = 2,
} cbm_lock_registry_abort_failure_t;

/* One-shot abort-bookkeeping fault seam, armable only while idle. */
bool cbm_lock_registry_fail_next_abort_bookkeeping_for_test(
    cbm_lock_registry_t *registry, cbm_lock_registry_abort_failure_t failure);

/* Deterministic test seam. May only be changed while the registry is idle;
 * callbacks run without the fork guard or registry mutex held. */
bool cbm_lock_registry_set_stage_hook_for_test(cbm_lock_registry_t *registry,
                                               cbm_lock_registry_stage_hook_fn hook, void *context);

#endif /* CBM_LOCK_REGISTRY_INTERNAL_H */
