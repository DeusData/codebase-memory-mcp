/* RED contract for daemon/local-CLI cross-process project coordination. */
#include "test_framework.h"
#include "test_helpers.h"

#include "daemon/ipc.h"
#include "daemon/project_lock.h"
#include "daemon/project_lock_internal.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"

#include <stdint.h>
#include <stdio.h>

enum { PROJECT_LOCK_TEST_PATH_CAP = 1024 };

static void project_lock_test_release(cbm_project_lock_lease_t **lease) {
    while (lease && *lease && cbm_project_lock_lease_release(lease) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
}

TEST(project_lock_coordinates_instances_projects_wildcard_and_case_aliases) {
    char runtime_parent[PROJECT_LOCK_TEST_PATH_CAP];
    (void)snprintf(runtime_parent, sizeof(runtime_parent), "%s/cbm-project-lock-XXXXXX",
                   cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(runtime_parent));

    cbm_daemon_ipc_endpoint_t *endpoint =
        cbm_daemon_ipc_endpoint_new("0123456789abcdef", runtime_parent);
    cbm_project_lock_manager_t *first = cbm_project_lock_manager_new(endpoint);
    cbm_project_lock_manager_t *second = cbm_project_lock_manager_new(endpoint);
    ASSERT_NOT_NULL(endpoint);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(second);

    cbm_project_lock_lease_t *foo = NULL;
    cbm_project_lock_lease_t *alias = NULL;
    cbm_project_lock_lease_t *bar = NULL;
    ASSERT_EQ(cbm_project_lock_try_acquire(first, "Foo", &foo), CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NOT_NULL(foo);
    ASSERT_EQ(cbm_project_lock_try_acquire(second, "foo", &alias), CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_NULL(alias);
    ASSERT_EQ(cbm_project_lock_acquire(second, "bar", UINT64_MAX, NULL, &bar),
              CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NOT_NULL(bar);

    project_lock_test_release(&bar);
    project_lock_test_release(&foo);

    cbm_project_lock_lease_t *all = NULL;
    ASSERT_EQ(cbm_project_lock_acquire(first, "*", UINT64_MAX, NULL, &all),
              CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NOT_NULL(all);
    ASSERT_EQ(cbm_project_lock_try_acquire(second, "unrelated", &bar), CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_NULL(bar);

    project_lock_test_release(&all);
    ASSERT_EQ(cbm_project_lock_manager_free(&second), CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(cbm_project_lock_manager_free(&first), CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(second);
    ASSERT_NULL(first);
    cbm_daemon_ipc_endpoint_free(endpoint);
    (void)th_rmtree(runtime_parent);
    PASS();
}

#ifdef _WIN32
static void project_lock_test_slow_native_preparation(void *context,
                                                      cbm_private_file_lock_mode_t mode,
                                                      cbm_lock_registry_stage_t stage) {
    (void)mode;
    if (stage == CBM_LOCK_REGISTRY_STAGE_TURN_HELD) {
        (*(unsigned int *)context)++;
        cbm_usleep(150000);
    }
}
#endif

TEST(project_lock_cli_attempt_survives_slow_windows_preparation) {
#ifdef _WIN32
    char runtime_parent[PROJECT_LOCK_TEST_PATH_CAP];
    (void)snprintf(runtime_parent, sizeof(runtime_parent), "%s/cbm-cli-lock-budget-XXXXXX",
                   cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(runtime_parent));
    cbm_daemon_ipc_endpoint_t *endpoint =
        cbm_daemon_ipc_endpoint_new("9876543210abcdef", runtime_parent);
    cbm_project_lock_manager_t *manager = cbm_project_lock_manager_new(endpoint);
    cbm_project_lock_manager_t *competitor = cbm_project_lock_manager_new(endpoint);
    unsigned int delayed_turns = 0;
    bool ready = manager && competitor &&
                 cbm_project_lock_set_stage_hook_for_test(
                     manager, project_lock_test_slow_native_preparation, &delayed_turns);
    cbm_project_lock_lease_t *lease = NULL;
    cbm_private_file_lock_status_t old_status =
        ready ? cbm_project_lock_acquire(manager, "slow-preparation", cbm_now_ms() + 100U, NULL,
                                         &lease)
              : CBM_PRIVATE_FILE_LOCK_IO;
    bool old_rolled_back = lease == NULL;
    project_lock_test_release(&lease);
    cbm_private_file_lock_status_t status =
        ready ? cbm_project_lock_acquire(manager, "slow-preparation",
                                         cbm_project_lock_cli_attempt_deadline(cbm_now_ms()), NULL,
                                         &lease)
              : CBM_PRIVATE_FILE_LOCK_IO;
    bool acquired = status == CBM_PRIVATE_FILE_LOCK_OK && lease;
    cbm_project_lock_lease_t *other = NULL;
    cbm_private_file_lock_status_t blocked =
        acquired ? cbm_project_lock_try_acquire(competitor, "slow-preparation", &other)
                 : CBM_PRIVATE_FILE_LOCK_IO;
    bool competitor_rolled_back = other == NULL;
    project_lock_test_release(&other);
    project_lock_test_release(&lease);
    cbm_private_file_lock_status_t released =
        ready ? cbm_project_lock_try_acquire(competitor, "slow-preparation", &other)
              : CBM_PRIVATE_FILE_LOCK_IO;
    project_lock_test_release(&other);
    cbm_private_file_lock_status_t competitor_freed = cbm_project_lock_manager_free(&competitor);
    cbm_private_file_lock_status_t manager_freed = cbm_project_lock_manager_free(&manager);
    cbm_daemon_ipc_endpoint_free(endpoint);
    (void)th_rmtree(runtime_parent);

    ASSERT_TRUE(ready);
    ASSERT_EQ(old_status, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_TRUE(old_rolled_back);
    ASSERT_TRUE(delayed_turns >= 3U);
    ASSERT_TRUE(acquired);
    ASSERT_EQ(blocked, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_TRUE(competitor_rolled_back);
    ASSERT_EQ(released, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(competitor_freed, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(manager_freed, CBM_PRIVATE_FILE_LOCK_OK);
#else
    SKIP_PLATFORM("Windows CLI lock preparation budget");
#endif
    PASS();
}

SUITE(project_lock) {
    RUN_TEST(project_lock_coordinates_instances_projects_wildcard_and_case_aliases);
    RUN_TEST(project_lock_cli_attempt_survives_slow_windows_preparation);
}
