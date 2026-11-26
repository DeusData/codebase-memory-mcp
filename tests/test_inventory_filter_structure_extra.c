#include "test_inventory_filter_internal.h"
static bool if_root_bound(bool overflow) {
    if_fixture f;
    if_init(&f);
    bool ok = false;
    size_t length = overflow ? 4094 : 4093;
    memset(f.root, 'r', length);
    f.root[length] = 0;
#ifdef _WIN32
    f.root[0] = 'C';
    f.root[1] = ':';
    f.root[2] = '/';
#else
    f.root[0] = '/';
#endif
    if (if_text(&f, "a", "body"))
        ok = overflow ? if_error(&f, CBM_INVENTORY_LIMIT, 0, false) : if_prepare(&f);
    ok = ok && f.calls == 0;
    if_finish(&f, 0);
    return ok;
}
int if_case_namespace(void) {
    if_fixture f;
    if_init(&f);
    int result = 1;
    IF_CHECK(if_dependency_control() && if_root_bound(false) && if_root_bound(true));
    static const char *paths[] = {"CON", "a:b", "a\\b", "a.", "a\xff"};
    for (size_t i = 0; i < sizeof(paths) / sizeof(paths[0]); i++) {
        if_init(&f);
        IF_CHECK(if_text(&f, paths[i], "body"));
#ifdef _WIN32
        IF_CHECK(if_error(&f, CBM_INVENTORY_UNSUPPORTED, 0, false));
#else
        IF_CHECK(if_prepare(&f));
        const cbm_inventory_filter_view_t *v = cbm_inventory_filter_view(f.owner);
        IF_CHECK(v->rows[0].disposition == CBM_INVENTORY_FILTER_NEEDS_LANGUAGE &&
                 !memcmp(v->files[0].path.data, paths[i], strlen(paths[i]) + 1));
        if_finish(&f, 0);
#endif
        IF_CHECK(f.calls == 0);
    }
    if_init(&f);
    strcpy(f.root, "relative");
    IF_CHECK(if_error(&f, CBM_INVENTORY_INVALID, SIZE_MAX, false) && f.calls == 0);
    if_init(&f);
    IF_CHECK(if_text(&f, "a", "body"));
    memset(f.files[0].oid, 'b', 64);
    f.files[0].oid[64] = 0;
    IF_CHECK(if_prepare(&f));
    const cbm_inventory_filter_view_t *wide = cbm_inventory_filter_view(f.owner);
    IF_CHECK(wide && strlen(wide->files[0].oid) == 64 &&
             wide->rows[0].disposition == CBM_INVENTORY_FILTER_NEEDS_LANGUAGE);
    result = 0;
done:
    return if_finish(&f, result);
}
int if_case_required_arguments(void) {
    if_fixture f;
    if_init(&f);
    int result = 1;
    cbm_inventory_error_t error;
    IF_CHECK(if_dependency_control() && if_prepare(&f));
    if_finish(&f, 0);
    for (unsigned i = 0; i < 4; i++) {
        cbm_inventory_filter_t *sentinel = (cbm_inventory_filter_t *)(uintptr_t)1;
        f.owner = sentinel;
        memset(&error, 0xa5, sizeof(error));
        cbm_inventory_status_t status = cbm_inventory_filter_prepare(
            i == 0 ? NULL : &f.source, i == 1 ? NULL : &f.limits, i == 2 ? NULL : &f.control,
            i == 3 ? NULL : &f.owner, &error);
        bool cleared = i == 3 || f.owner == NULL;
        if (f.owner == sentinel)
            f.owner = NULL;
        IF_CHECK(status == CBM_INVENTORY_INVALID && error.status == CBM_INVENTORY_INVALID &&
                 cleared && error.file_index == SIZE_MAX && !error.cleanup_required &&
                 f.calls == 0);
    }
    IF_CHECK(cbm_inventory_filter_prepare(&f.source, &f.limits, &f.control, &f.owner, NULL) ==
                 CBM_INVENTORY_OK &&
             f.owner);
    result = 0;
done:
    return if_finish(&f, result);
}
