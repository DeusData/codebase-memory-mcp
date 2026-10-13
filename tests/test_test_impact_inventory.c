#include "test_test_impact_inventory_internal.h"

static bool ni_path(cbm_inventory_path_t path, const char *expected) {
    size_t length = strlen(expected);
    return path.length == length && (length ? path.data && !memcmp(path.data, expected, length + 1)
                                            : !path.data || !path.data[0]);
}

static bool ni_default_file(const if_native *n, const cbm_inventory_filter_view_t *view, size_t i,
                            const char *path) {
    const cbm_inventory_file_t *actual = &view->files[i], *expected = &n->input.files[i];
    const cbm_inventory_filter_row_t *row = &view->rows[i];
    bool ignored = i == 4 || i == 9;
    unsigned roles = i == 0             ? CBM_INVENTORY_FILTER_ROLE_CBMIGNORE
                     : i == 1           ? CBM_INVENTORY_FILTER_ROLE_PHYSICAL_PROJECT_CONFIG
                     : i == 2 || i == 7 ? CBM_INVENTORY_FILTER_ROLE_GITIGNORE
                                        : 0;
    return ni_path(actual->path, path) && actual->git_mode == 0100644 &&
           !strcmp(actual->oid, expected->oid) &&
           actual->content_length == expected->content_length &&
           !memcmp(actual->content_sha256, expected->content_sha256, 32) && row->file_index == i &&
           row->disposition == (ignored ? CBM_INVENTORY_FILTER_IGNORED_FILE
                                        : CBM_INVENTORY_FILTER_NEEDS_LANGUAGE) &&
           row->reason == (ignored ? CBM_INVENTORY_FILTER_REASON_GITIGNORE
                                   : CBM_INVENTORY_FILTER_REASON_NONE) &&
           row->roles == roles && !row->excluded_ancestor.data && !row->excluded_ancestor.length;
}

static bool ni_default_controls(const cbm_inventory_filter_view_t *view) {
    if (view->control_count != 3)
        return false;
    const cbm_inventory_control_row_t *c = view->controls;
    return c[0].kind == CBM_INVENTORY_CONTROL_GITIGNORE && ni_path(c[0].directory, "") &&
           c[0].file_index == 2 && c[0].outcome == CBM_INVENTORY_CONTROL_APPLIED &&
           c[0].effective_patterns == 1 && c[1].kind == CBM_INVENTORY_CONTROL_GITIGNORE &&
           ni_path(c[1].directory, "sub") && c[1].file_index == 7 &&
           c[1].outcome == CBM_INVENTORY_CONTROL_APPLIED && c[1].effective_patterns == 1 &&
           c[2].kind == CBM_INVENTORY_CONTROL_CBMIGNORE && ni_path(c[2].directory, "") &&
           c[2].file_index == 0 && c[2].outcome == CBM_INVENTORY_CONTROL_APPLIED_EMPTY &&
           c[2].effective_patterns == 0;
}

bool ni_default_view(const if_native *n, const cbm_test_impact_inventory_t *owner) {
    static const char *const paths[] = {
        ".cbmignore", ".codebase-memory.json", ".gitignore", "a.data",      "drop.c", "plain",
        "source.c",   "sub/.gitignore",        "sub/keep.c", "sub/nested.c"};
    const cbm_inventory_filter_view_t *view = cbm_test_impact_inventory_view(owner);
    if (!view || view->file_count != 10 || view->directory_count != 2 || view->excluded_count ||
        strcmp(view->native_root, n->input.root) ||
        memcmp(view->manifest_sha256, n->input.source.manifest_sha256, 32))
        return false;
    for (size_t i = 0; i < 10; i++) {
        if (!ni_default_file(n, view, i, paths[i]))
            return false;
    }
    return ni_default_controls(view);
}

TEST(native_inventory_head_metadata_and_ambient_independence) {
    if_native n = {0};
    cbm_test_impact_inventory_t *owner = NULL;
    int result = 1;
    NI_CHECK(ni_start(&n));
    /* Real HEAD, index and worktree changes precede any adapter assertion. */
    NI_CHECK(ni_ambient_change(&n) && ni_dependency(&n, CBM_GIT_REV_HEAD));
    NI_CHECK(ni_prepare(&n, &owner) && ni_default_view(&n, owner));
    cbm_test_impact_inventory_usage_t usage;
    NI_CHECK(cbm_test_impact_inventory_usage(owner, &usage));
    NI_CHECK(usage.filter.verified_file_reads_reserved == 3 &&
             usage.filter.verified_content_bytes_reserved == 25 &&
             usage.filter.control_bytes_reserved == 25);
    NI_CHECK(cbm_pinned_tree_view(n.input.tree) != NULL);
    result = 0;
done:
    return ni_finish(&n, owner, result);
}

TEST(native_inventory_merge_base_rejected_even_when_equal) {
    if_native n = {0};
    cbm_test_impact_inventory_t *owner = NULL;
    int result = 1;
    NI_CHECK(ni_start(&n));
    NI_CHECK(if_native_close(&n) && ni_tree(&n, CBM_GIT_REV_MERGE_BASE) &&
             ni_dependency(&n, CBM_GIT_REV_MERGE_BASE));
    const cbm_pinned_tree_view_t *native = cbm_pinned_tree_view(n.input.tree);
    NI_CHECK(native && native->revision == CBM_GIT_REV_MERGE_BASE &&
             !strcmp(native->commit, native->identity->head));
    NI_CHECK(ni_error(&n, CBM_INVENTORY_UNSUPPORTED, SIZE_MAX, false, false));
    size_t arena_total = n.input.limits.max_arena_bytes;
    n.input.limits.max_arena_bytes = n.input.limits.max_ignore_arena_bytes;
    NI_CHECK(ni_error(&n, CBM_INVENTORY_UNSUPPORTED, SIZE_MAX, false, false));
    n.input.limits.max_arena_bytes = arena_total;
    NI_CHECK(cbm_pinned_tree_view(n.input.tree) != NULL);
    NI_CHECK(if_native_close(&n) && ni_tree(&n, CBM_GIT_REV_HEAD) &&
             ni_dependency(&n, CBM_GIT_REV_HEAD));
    /* Ensures the missing-feature baseline does not pass on UNSUPPORTED alone. */
    NI_CHECK(ni_prepare(&n, &owner) && ni_default_view(&n, owner));
    result = 0;
done:
    return ni_finish(&n, owner, result);
}

static bool ni_empty_result(const cbm_test_impact_inventory_t *owner, bool controls) {
    const cbm_inventory_filter_view_t *view = cbm_test_impact_inventory_view(owner);
    cbm_test_impact_inventory_usage_t usage;
    if (!view || view->file_count != (controls ? 3u : 0u) || view->directory_count != 1 ||
        view->control_count != 2 || view->excluded_count ||
        !cbm_test_impact_inventory_usage(owner, &usage))
        return false;
    const cbm_inventory_control_row_t *c = view->controls;
    cbm_inventory_control_outcome_t outcome =
        controls ? CBM_INVENTORY_CONTROL_APPLIED_EMPTY : CBM_INVENTORY_CONTROL_ABSENT;
    if (c[0].kind != CBM_INVENTORY_CONTROL_GITIGNORE || !ni_path(c[0].directory, "") ||
        c[0].file_index != (controls ? 1u : SIZE_MAX) || c[0].outcome != outcome ||
        c[0].effective_patterns || c[1].kind != CBM_INVENTORY_CONTROL_CBMIGNORE ||
        !ni_path(c[1].directory, "") || c[1].file_index != (controls ? 0u : SIZE_MAX) ||
        c[1].outcome != outcome || c[1].effective_patterns ||
        usage.filter.verified_file_reads_reserved != (controls ? 2u : 0u) ||
        usage.filter.verified_content_bytes_reserved || usage.filter.control_bytes_reserved)
        return false;
    if (!controls)
        return true;
    const char *paths[] = {".cbmignore", ".gitignore", "source.c"};
    for (size_t i = 0; i < 3; i++) {
        unsigned role = i == 0   ? CBM_INVENTORY_FILTER_ROLE_CBMIGNORE
                        : i == 1 ? CBM_INVENTORY_FILTER_ROLE_GITIGNORE
                                 : 0;
        if (!ni_path(view->files[i].path, paths[i]) ||
            view->files[i].git_mode != (i == 2 ? 0100755u : 0100644u) ||
            view->rows[i].file_index != i || view->rows[i].roles != role ||
            view->rows[i].disposition != CBM_INVENTORY_FILTER_NEEDS_LANGUAGE ||
            view->rows[i].reason != CBM_INVENTORY_FILTER_REASON_NONE)
            return false;
    }
    return true;
}

TEST(native_inventory_empty_tree_and_empty_control_files) {
    if_native empty = {0}, controls = {0};
    cbm_test_impact_inventory_t *owner = NULL;
    int result = 1;
    /* Establish both real fixtures before the first adapter assertion, so an
     * absent bridge cannot hide a faulty empty-control fixture. */
    NI_CHECK(ni_start(&empty) && ni_replace_snapshot(&empty, false));
    NI_CHECK(ni_start(&controls) && ni_replace_snapshot(&controls, true));
    const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(empty.input.tree);
    NI_CHECK(view && view->file_count == 0 && view->directory_count == 1);
    view = cbm_pinned_tree_view(controls.input.tree);
    NI_CHECK(view && view->file_count == 3 && view->files[0].content_length == 0 &&
             view->files[1].content_length == 0 && view->files[2].git_mode == 0100755);
    NI_CHECK(ni_prepare(&empty, &owner) && ni_empty_result(owner, false));
    cbm_test_impact_inventory_free(owner);
    owner = NULL;
    NI_CHECK(ni_prepare(&controls, &owner) && ni_empty_result(owner, true));
    result = 0;
done:
    result = ni_finish(&controls, owner, result);
    return if_native_finish(&empty, result);
}

TEST(native_inventory_copied_owner_outlives_native_owners) {
    if_native n = {0};
    cbm_test_impact_inventory_t *first = NULL, *second = NULL;
    int result = 1;
    NI_CHECK(ni_start(&n));
    NI_CHECK(ni_prepare(&n, &first) && ni_prepare(&n, &second));
    NI_CHECK(ni_default_view(&n, first) && ni_default_view(&n, second));
    cbm_inventory_limits_t original = n.input.limits, actual;
    cbm_test_impact_inventory_usage_t before, after;
    NI_CHECK(cbm_test_impact_inventory_usage(second, &before));
    NI_CHECK(if_native_close(&n));
    cbm_git_facts_free(n.facts);
    n.facts = NULL;
    memset(&n.input.limits, 0xa5, sizeof(n.input.limits));
    memset(&n.input.control, 0, sizeof(n.input.control));
    cbm_test_impact_inventory_free(first);
    first = NULL;
    NI_CHECK(ni_default_view(&n, second));
    NI_CHECK(cbm_test_impact_inventory_limits(second, &actual) &&
             ni_limits_equal(&actual, &original));
    NI_CHECK(cbm_test_impact_inventory_usage(second, &after) &&
             after.attachment_arena_requested_bytes == before.attachment_arena_requested_bytes &&
             after.arena_requested_bytes == before.arena_requested_bytes &&
             after.arena_budget_used_bytes == before.arena_budget_used_bytes &&
             after.filter.verified_file_reads_reserved == 3 &&
             after.filter.verified_content_bytes_reserved == 25);
    result = 0;
done:
    cbm_test_impact_inventory_free(first);
    return ni_finish(&n, second, result);
}

TEST(native_inventory_read_faults_keep_caller_cleanup) {
    return ni_case_faults();
}
TEST(native_inventory_entry_cancel_deadline_and_state) {
    return ni_case_entry();
}
TEST(native_inventory_attachment_and_read_budget_boundaries) {
    return ni_case_limits();
}

SUITE(test_impact_inventory) {
    RUN_TEST(native_inventory_head_metadata_and_ambient_independence);
    RUN_TEST(native_inventory_merge_base_rejected_even_when_equal);
    RUN_TEST(native_inventory_empty_tree_and_empty_control_files);
    RUN_TEST(native_inventory_read_faults_keep_caller_cleanup);
    RUN_TEST(native_inventory_entry_cancel_deadline_and_state);
    RUN_TEST(native_inventory_attachment_and_read_budget_boundaries);
    RUN_TEST(native_inventory_copied_owner_outlives_native_owners);
}
