#ifndef CBM_DAEMON_PROJECT_LOCK_INTERNAL_H
#define CBM_DAEMON_PROJECT_LOCK_INTERNAL_H

#include "daemon/project_lock.h"
#include "foundation/lock_registry_internal.h"

/* Forward the existing idle-only registry seam without exposing its handle. */
bool cbm_project_lock_set_stage_hook_for_test(cbm_project_lock_manager_t *manager,
                                              cbm_lock_registry_stage_hook_fn hook, void *context);

#endif /* CBM_DAEMON_PROJECT_LOCK_INTERNAL_H */
