#ifndef TEST_TEST_IMPACT_INVENTORY_INTERNAL_H
#define TEST_TEST_IMPACT_INVENTORY_INTERNAL_H
#include "test_inventory_filter_native_internal.h"
#include "mcp/test_impact_inventory.h"

#define NI_CHECK(condition)                                                             \
    do {                                                                                \
        if (!(condition)) {                                                             \
            fprintf(stderr, "native adapter assertion %s:%d: %s\n", __FILE__, __LINE__, \
                    #condition);                                                        \
            goto done;                                                                  \
        }                                                                               \
    } while (0)

bool ni_write(if_native *n, const char *path, const void *bytes, size_t size);
bool ni_ambient_change(if_native *n);
bool ni_replace_snapshot(if_native *n, bool empty_controls);
bool ni_reset_input(if_native *n);
bool ni_commit_input(if_native *n);
bool ni_tree(if_native *n, cbm_git_revision_t revision);
bool ni_dependency(if_native *n, cbm_git_revision_t revision);
bool ni_start(if_native *n);
bool ni_prepare(if_native *n, cbm_test_impact_inventory_t **owner);
bool ni_error(if_native *n, cbm_inventory_status_t expected, size_t index, bool cleanup,
              bool allow_unknown_index);
bool ni_default_view(const if_native *n, const cbm_test_impact_inventory_t *owner);
bool ni_limits_equal(const cbm_inventory_limits_t *a, const cbm_inventory_limits_t *b);
int ni_finish(if_native *n, cbm_test_impact_inventory_t *owner, int result);
int ni_case_faults(void);
int ni_case_entry(void);
int ni_case_limits(void);
#endif
