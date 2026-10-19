#include "test_test_impact_inventory_internal.h"

static bool ni_cancelled(void *context) {
    return *(const bool *)context;
}

int ni_case_faults(void) {
    if_native n = {0};
    cbm_test_impact_inventory_t *owner = NULL;
    int result = 1;
    NI_CHECK(ni_start(&n));
    NI_CHECK(ni_prepare(&n, &owner) && ni_default_view(&n, owner));
    cbm_test_impact_inventory_free(owner);
    owner = NULL;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    const cbm_pinned_tree_read_fault_t faults[] = {CBM_PINNED_TREE_READ_FAULT_READ,
                                                   CBM_PINNED_TREE_READ_FAULT_EOF,
                                                   CBM_PINNED_TREE_READ_FAULT_CLOSE};
    for (size_t i = 0; i < sizeof(faults) / sizeof(faults[0]); i++) {
        if (i)
            NI_CHECK(ni_tree(&n, CBM_GIT_REV_HEAD) && ni_dependency(&n, CBM_GIT_REV_HEAD));
        NI_CHECK(cbm_pinned_tree_test_set_read_fault(n.input.tree, faults[i]));
        NI_CHECK(ni_error(&n, CBM_INVENTORY_IO, 0, faults[i] == CBM_PINNED_TREE_READ_FAULT_CLOSE,
                          false));
        NI_CHECK(n.input.tree && !cbm_pinned_tree_view(n.input.tree));
        NI_CHECK(ni_error(&n, CBM_INVENTORY_STATE, SIZE_MAX, false, false));
        /* The CLOSE seam retains a known open handle; this is its first actual
         * close, never a retry of a native close whose outcome is unknown. */
        NI_CHECK(if_native_close(&n) && !n.input.tree);
    }
    NI_CHECK(ni_tree(&n, CBM_GIT_REV_HEAD) && ni_dependency(&n, CBM_GIT_REV_HEAD));
    NI_CHECK(ni_prepare(&n, &owner) && ni_default_view(&n, owner));
    result = 0;
#else
    fprintf(stderr, "native inventory faults require canonical CBM_ENABLE_TEST_SEAMS\n");
#endif
done:
    return ni_finish(&n, owner, result);
}

static bool ni_invalid_arguments(if_native *n) {
    cbm_inventory_error_t error;
    for (unsigned missing = 0; missing < 4; missing++) {
        cbm_test_impact_inventory_t *sentinel = (cbm_test_impact_inventory_t *)(uintptr_t)1;
        cbm_test_impact_inventory_t *owner = sentinel;
        memset(&error, 0xa5, sizeof(error));
        cbm_inventory_status_t status = cbm_test_impact_inventory_prepare(
            missing == 0 ? NULL : n->input.tree, missing == 1 ? NULL : &n->input.limits,
            missing == 2 ? NULL : &n->input.control, missing == 3 ? NULL : &owner, &error);
        bool cleared = missing == 3 || owner == NULL;
        if (owner != sentinel)
            cbm_test_impact_inventory_free(owner);
        if (status != CBM_INVENTORY_INVALID || error.status != status || !cleared ||
            error.file_index != SIZE_MAX || error.cleanup_required ||
            !memchr(error.diagnostic, 0, sizeof(error.diagnostic)))
            return false;
    }
    return true;
}

static bool ni_zero_getters(void) {
    cbm_test_impact_inventory_usage_t usage;
    cbm_inventory_limits_t limits, zero_limits = {0};
    memset(&usage, 0xa5, sizeof(usage));
    memset(&limits, 0xa5, sizeof(limits));
    cbm_test_impact_inventory_free(NULL);
    return !cbm_test_impact_inventory_view(NULL) &&
           !cbm_test_impact_inventory_usage(NULL, &usage) &&
           !usage.attachment_arena_requested_bytes && !usage.arena_requested_bytes &&
           !usage.arena_budget_used_bytes && !usage.filter.verified_file_reads_reserved &&
           !usage.filter.verified_content_bytes_reserved && !usage.filter.control_bytes_reserved &&
           !usage.filter.ignore_bytes_reserved && !usage.filter.ignore_work_used &&
           !usage.filter.ignore_patterns_reserved && !usage.filter.ignore_arena_requested_bytes &&
           !usage.filter.non_ignore_arena_requested_bytes && !usage.filter.arena_requested_bytes &&
           !usage.filter.arena_budget_used_bytes &&
           !cbm_test_impact_inventory_limits(NULL, &limits) &&
           ni_limits_equal(&limits, &zero_limits) && !cbm_test_impact_inventory_usage(NULL, NULL) &&
           !cbm_test_impact_inventory_limits(NULL, NULL);
}

int ni_case_entry(void) {
    if_native n = {0};
    cbm_test_impact_inventory_t *owner = NULL;
    int result = 1;
    NI_CHECK(ni_start(&n));
    NI_CHECK(ni_invalid_arguments(&n) && ni_zero_getters());
    NI_CHECK(cbm_pinned_tree_view(n.input.tree) != NULL);
    bool cancel = true;
    n.input.control.cancelled = ni_cancelled;
    n.input.control.context = &cancel;
    n.input.control.deadline_ms = 1;
    /* Both pending: cancellation is the specified primary cause. */
    NI_CHECK(cbm_now_ms() > 1);
    NI_CHECK(ni_error(&n, CBM_INVENTORY_CANCELLED, SIZE_MAX, false, false));
    NI_CHECK(cbm_pinned_tree_view(n.input.tree) != NULL);
    cancel = false;
    NI_CHECK(ni_error(&n, CBM_INVENTORY_DEADLINE, SIZE_MAX, false, false));
    NI_CHECK(cbm_pinned_tree_view(n.input.tree) != NULL);
    n.input.control.deadline_ms = 0;
    NI_CHECK(ni_error(&n, CBM_INVENTORY_INVALID, SIZE_MAX, false, false));
    n.input.control.deadline_ms = UINT64_MAX;
    n.input.limits.max_ignore_arena_bytes = n.input.limits.max_arena_bytes + 1;
    NI_CHECK(ni_error(&n, CBM_INVENTORY_INVALID, SIZE_MAX, false, false));
    n.input.limits.max_ignore_arena_bytes = 512 * 1024;
    n.input.limits.max_files = 0;
    NI_CHECK(ni_error(&n, CBM_INVENTORY_INVALID, SIZE_MAX, false, false));
    n.input.limits.max_files = IF_ROWS;
    NI_CHECK(ni_prepare(&n, &owner) && ni_default_view(&n, owner));
    cbm_test_impact_inventory_free(owner);
    owner = NULL;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    unsigned char bytes[IF_BYTES];
    size_t copied = SIZE_MAX;
    cbm_pinned_tree_error_t native_error;
    cbm_pinned_tree_control_t native_control = {.deadline_ms = UINT64_MAX};
    NI_CHECK(cbm_pinned_tree_test_set_read_fault(n.input.tree, CBM_PINNED_TREE_READ_FAULT_READ));
    NI_CHECK(cbm_pinned_tree_read_prefix(n.input.tree, 0, 16384, bytes, sizeof(bytes), &copied,
                                         &native_control, &native_error) == CBM_PINNED_TREE_IO &&
             copied == 0 && !cbm_pinned_tree_view(n.input.tree));
    NI_CHECK(ni_error(&n, CBM_INVENTORY_STATE, SIZE_MAX, false, false));
    n.input.limits.max_arena_bytes = n.input.limits.max_ignore_arena_bytes;
    NI_CHECK(ni_error(&n, CBM_INVENTORY_STATE, SIZE_MAX, false, false));
    result = 0;
#else
    fprintf(stderr, "native inventory state fixture requires canonical test seams\n");
#endif
done:
    return ni_finish(&n, owner, result);
}

static bool ni_accounting(const cbm_test_impact_inventory_usage_t *u,
                          const cbm_inventory_limits_t *limits) {
    size_t a = u->attachment_arena_requested_bytes;
    size_t d = u->filter.non_ignore_arena_requested_bytes;
    size_t c = u->filter.ignore_arena_requested_bytes;
    size_t i = limits->max_ignore_arena_bytes;
    if (!a || a > limits->max_arena_bytes || i > limits->max_arena_bytes - a ||
        d > limits->max_arena_bytes - a - i || c > i)
        return false;
    return u->filter.arena_requested_bytes == d + c && u->filter.arena_budget_used_bytes == d + i &&
           u->arena_requested_bytes == a + d + c && u->arena_budget_used_bytes == a + d + i &&
           u->filter.verified_file_reads_reserved == 3 &&
           u->filter.verified_content_bytes_reserved == 25 &&
           u->filter.control_bytes_reserved == 25;
}

static bool ni_usage_same(const cbm_test_impact_inventory_usage_t *a,
                          const cbm_test_impact_inventory_usage_t *b) {
    return a->attachment_arena_requested_bytes == b->attachment_arena_requested_bytes &&
           a->arena_requested_bytes == b->arena_requested_bytes &&
           a->arena_budget_used_bytes == b->arena_budget_used_bytes &&
           a->filter.ignore_arena_requested_bytes == b->filter.ignore_arena_requested_bytes &&
           a->filter.non_ignore_arena_requested_bytes ==
               b->filter.non_ignore_arena_requested_bytes &&
           a->filter.ignore_bytes_reserved == b->filter.ignore_bytes_reserved &&
           a->filter.ignore_work_used == b->filter.ignore_work_used &&
           a->filter.ignore_patterns_reserved == b->filter.ignore_patterns_reserved;
}

int ni_case_limits(void) {
    if_native n = {0};
    cbm_test_impact_inventory_t *owner = NULL;
    int result = 1;
    NI_CHECK(ni_start(&n));
    n.input.limits.max_verified_file_reads = 3;
    n.input.limits.max_verified_content_bytes = 25;
    n.input.limits.max_control_total_bytes = 25;
    NI_CHECK(ni_prepare(&n, &owner) && ni_default_view(&n, owner));
    cbm_test_impact_inventory_usage_t high, exact;
    cbm_inventory_limits_t returned;
    NI_CHECK(cbm_test_impact_inventory_usage(owner, &high) &&
             ni_accounting(&high, &n.input.limits));
    NI_CHECK(cbm_test_impact_inventory_limits(owner, &returned) &&
             ni_limits_equal(&returned, &n.input.limits));
    cbm_test_impact_inventory_free(owner);
    owner = NULL;
    size_t total = n.input.limits.max_arena_bytes;
    NI_CHECK(high.arena_budget_used_bytes > n.input.limits.max_ignore_arena_bytes);
    n.input.limits.max_arena_bytes = high.arena_budget_used_bytes;
    NI_CHECK(ni_prepare(&n, &owner) && ni_default_view(&n, owner));
    NI_CHECK(cbm_test_impact_inventory_usage(owner, &exact) &&
             ni_accounting(&exact, &n.input.limits) && ni_usage_same(&high, &exact));
    NI_CHECK(cbm_test_impact_inventory_limits(owner, &returned) &&
             ni_limits_equal(&returned, &n.input.limits));
    cbm_test_impact_inventory_free(owner);
    owner = NULL;
    n.input.limits.max_arena_bytes--;
    NI_CHECK(ni_error(&n, CBM_INVENTORY_LIMIT, SIZE_MAX, false, true));
    NI_CHECK(cbm_pinned_tree_view(n.input.tree) != NULL);
    n.input.limits.max_arena_bytes = n.input.limits.max_ignore_arena_bytes;
    NI_CHECK(ni_error(&n, CBM_INVENTORY_LIMIT, SIZE_MAX, false, false));
    NI_CHECK(cbm_pinned_tree_view(n.input.tree) != NULL);
    n.input.limits.max_arena_bytes = total;
    n.input.limits.max_verified_file_reads = 2;
    NI_CHECK(ni_error(&n, CBM_INVENTORY_LIMIT, 7, false, false));
    n.input.limits.max_verified_file_reads = 3;
    n.input.limits.max_verified_content_bytes = 24;
    NI_CHECK(ni_error(&n, CBM_INVENTORY_LIMIT, 7, false, false));
    n.input.limits.max_verified_content_bytes = 25;
    NI_CHECK(ni_prepare(&n, &owner) && ni_default_view(&n, owner));
    NI_CHECK(cbm_test_impact_inventory_usage(owner, &exact) &&
             ni_accounting(&exact, &n.input.limits) && ni_usage_same(&high, &exact));
    result = 0;
done:
    return ni_finish(&n, owner, result);
}
