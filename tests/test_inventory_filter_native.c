#include "test_inventory_filter_native_internal.h"
#define N CBM_INVENTORY_FILTER_NEEDS_LANGUAGE
#define I CBM_INVENTORY_FILTER_IGNORED_FILE
#define NONE CBM_INVENTORY_FILTER_REASON_NONE
#define GIT CBM_INVENTORY_FILTER_REASON_GITIGNORE
int if_case_native(void) {
    if_native n = {0};
    int result = 1;
    IF_CHECK(if_dependency_control());
    IF_CHECK(if_native_start(&n));
    IF_CHECK(if_native_dependency(&n));
    /* Dependency witnesses finish before the first new-feature assertion. */
    IF_CHECK(if_prepare(&n.input));
    const if_row rows[] = {
        {".cbmignore", N, NONE, CBM_INVENTORY_FILTER_ROLE_CBMIGNORE, NULL},
        {".codebase-memory.json", N, NONE, CBM_INVENTORY_FILTER_ROLE_PHYSICAL_PROJECT_CONFIG, NULL},
        {".gitignore", N, NONE, CBM_INVENTORY_FILTER_ROLE_GITIGNORE, NULL},
        {"a.data", N, NONE, 0, NULL},
        {"drop.c", I, GIT, 0, NULL},
        {"plain", N, NONE, 0, NULL},
        {"source.c", N, NONE, 0, NULL},
        {"sub/.gitignore", N, NONE, CBM_INVENTORY_FILTER_ROLE_GITIGNORE, NULL},
        {"sub/keep.c", N, NONE, 0, NULL},
        {"sub/nested.c", I, GIT, 0, NULL}};
    const if_control_row controls[] = {
        {CBM_INVENTORY_CONTROL_GITIGNORE, "", 2, CBM_INVENTORY_CONTROL_APPLIED, 1},
        {CBM_INVENTORY_CONTROL_GITIGNORE, "sub", 7, CBM_INVENTORY_CONTROL_APPLIED, 1},
        {CBM_INVENTORY_CONTROL_CBMIGNORE, "", 0, CBM_INVENTORY_CONTROL_APPLIED_EMPTY, 0}};
    const size_t calls[] = {0, 2, 7};
    IF_CHECK(if_rows(&n.input, rows, 10) && if_controls(&n.input, controls, 3) &&
             if_reads(&n.input, calls, 3));
    const cbm_inventory_filter_view_t *v = cbm_inventory_filter_view(n.input.owner);
    IF_CHECK(v->directory_count == 2 && v->excluded_count == 0 &&
             v->native_root != n.input.source.native_root);
    cbm_pinned_tree_error_t error;
    cbm_pinned_tree_control_t control = {.deadline_ms = UINT64_MAX};
    IF_CHECK(cbm_pinned_tree_verify(n.input.tree, &control, &error) == CBM_PINNED_TREE_OK);
    IF_CHECK(if_native_close(&n));
    cbm_git_facts_free(n.facts);
    n.facts = NULL;
    IF_CHECK(if_rows(&n.input, rows, 10) && if_controls(&n.input, controls, 3) &&
             n.input.calls == 3);
    result = 0;
done:
    return if_native_finish(&n, result);
}
int if_case_native_faults(void) {
    if_native n = {0};
    int result = 1;
    IF_CHECK(if_native_start(&n) && if_native_dependency(&n) && if_prepare(&n.input));
    if_finish(&n.input, 0);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    const cbm_pinned_tree_read_fault_t faults[] = {CBM_PINNED_TREE_READ_FAULT_READ,
                                                   CBM_PINNED_TREE_READ_FAULT_EOF,
                                                   CBM_PINNED_TREE_READ_FAULT_CLOSE};
    for (size_t i = 0; i < 3; i++) {
        if (i)
            IF_CHECK(if_native_tree(&n) && if_native_dependency(&n));
        if_reset_reads(&n.input);
        IF_CHECK(cbm_pinned_tree_test_set_read_fault(n.input.tree, faults[i]));
        IF_CHECK(
            if_error(&n.input, CBM_INVENTORY_IO, 0, faults[i] == CBM_PINNED_TREE_READ_FAULT_CLOSE));
        IF_CHECK(n.input.calls == 1 && n.input.indices[0] == 0 && n.input.tree &&
                 !cbm_pinned_tree_view(n.input.tree));
        /* CLOSE is the existing known-handle simulation, not a real failed close retry. */
        IF_CHECK(if_native_close(&n) && !n.input.tree);
    }
    result = 0;
#else
    fprintf(stderr, "inventory native fault tests require canonical CBM_ENABLE_TEST_SEAMS\n");
#endif
done:
    return if_native_finish(&n, result);
}
