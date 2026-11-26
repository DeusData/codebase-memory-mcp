#include "test_inventory_filter_internal.h"
static bool if_limit_fixture(if_fixture *f) {
    if_init(f);
    return if_text(f, ".cbmignore", "one\n") && if_text(f, ".gitignore", "two\n") &&
           if_text(f, "a/z.c", "body");
}
static bool if_usage_valid(if_fixture *f, cbm_inventory_filter_usage_t *u) {
    if (!cbm_inventory_filter_usage(f->owner, u))
        return false;
    /* Two 4-byte controls; matches for root files, a, a/z.c against both programs. */
    return u->verified_file_reads_reserved == 2 && u->verified_content_bytes_reserved == 8 &&
           u->control_bytes_reserved == 8 && u->ignore_bytes_reserved == 68 &&
           u->ignore_patterns_reserved == 2 && u->ignore_work_used > 0 &&
           u->ignore_work_used <= f->limits.max_ignore_work &&
           u->ignore_arena_requested_bytes > 0 &&
           u->ignore_arena_requested_bytes < f->limits.max_ignore_arena_bytes &&
           u->non_ignore_arena_requested_bytes > 0 &&
           u->arena_requested_bytes ==
               u->non_ignore_arena_requested_bytes + u->ignore_arena_requested_bytes &&
           u->arena_budget_used_bytes ==
               u->non_ignore_arena_requested_bytes + f->limits.max_ignore_arena_bytes &&
           u->arena_budget_used_bytes <= f->limits.max_arena_bytes;
}
static void if_cap(if_fixture *f, unsigned which, uint64_t value) {
    switch (which) {
    case 0:
        f->limits.max_files = (size_t)value;
        break;
    case 1:
        f->limits.max_directories = (size_t)value;
        break;
    case 2:
        f->limits.max_control_file_bytes = (size_t)value;
        break;
    case 3:
        f->limits.max_control_total_bytes = value;
        break;
    case 4:
        f->limits.max_verified_file_reads = value;
        break;
    case 5:
        f->limits.max_verified_content_bytes = value;
        break;
    case 6:
        f->limits.max_ignore_patterns = (size_t)value;
        break;
    default:
        break;
    }
}
static bool if_fixed_cap_round(unsigned which, uint64_t exact) {
    if_fixture f;
    if_init(&f);
    bool ok = false;
    if (!if_limit_fixture(&f))
        return false;
    if_cap(&f, which, exact);
    if (!if_prepare(&f))
        goto done;
    if_finish(&f, 0);
    if (!if_limit_fixture(&f))
        goto done;
    if_cap(&f, which, exact - 1);
    /* Directory planning may be global or carry its originating row. Control
     * reservations/parsing always identify the next exact requested control. */
    if (which == 1)
        ok = if_error_during_work(&f, CBM_INVENTORY_LIMIT, false);
    else
        ok = if_error(&f, CBM_INVENTORY_LIMIT, which == 0 ? SIZE_MAX : which == 2 ? 0 : 1, false);
    if (which <= 2)
        ok = ok && f.calls == 0;
    if (which == 3 || which == 4 || which == 5)
        ok = ok && f.calls == 1 && f.indices[0] == 0;
    if (which == 6)
        ok = ok && f.calls == 2;
done:
    if_finish(&f, 0);
    return ok;
}
static bool if_derived_cap_round(unsigned which, const cbm_inventory_filter_usage_t *u) {
    if_fixture f;
    if_init(&f);
    bool ok = false;
    if (!if_limit_fixture(&f))
        return false;
    if (which == 0)
        f.limits.max_ignore_work = u->ignore_work_used;
    if (which == 1)
        f.limits.max_arena_bytes = u->arena_budget_used_bytes;
    if (which == 2)
        f.limits.max_ignore_arena_bytes = u->ignore_arena_requested_bytes;
    if (!if_prepare(&f))
        goto done;
    if_finish(&f, 0);
    if (!if_limit_fixture(&f))
        goto done;
    if (which == 0)
        f.limits.max_ignore_work = u->ignore_work_used - 1;
    if (which == 1)
        f.limits.max_arena_bytes = u->arena_budget_used_bytes - 1;
    if (which == 2)
        f.limits.max_ignore_arena_bytes = u->ignore_arena_requested_bytes - 1;
    /* Work or allocation can stop before a row is current or while handling one. */
    ok = if_error_during_work(&f, CBM_INVENTORY_LIMIT, false);
done:
    if_finish(&f, 0);
    return ok;
}
static bool if_reservation_round(const cbm_inventory_filter_usage_t *u) {
    if_fixture f;
    if_init(&f);
    bool ok = false;
    if (!if_limit_fixture(&f))
        return false;
    f.limits.max_arena_bytes = f.limits.max_ignore_arena_bytes;
    if (!if_error(&f, CBM_INVENTORY_LIMIT, SIZE_MAX, false) || f.calls != 0)
        goto done;
    if (!if_limit_fixture(&f))
        goto done;
    f.limits.max_arena_bytes = u->arena_requested_bytes + f.limits.max_ignore_arena_bytes;
    if (!if_prepare(&f))
        goto done;
    if_finish(&f, 0);
    if (!if_limit_fixture(&f))
        goto done;
    /* D+C fits this T but D+I exceeds it by exactly one; unused I cannot be reused. */
    f.limits.max_arena_bytes =
        f.limits.max_ignore_arena_bytes + u->non_ignore_arena_requested_bytes - 1;
    /* Work or allocation can stop before a row is current or while handling one. */
    ok = if_error_during_work(&f, CBM_INVENTORY_LIMIT, false);
done:
    if_finish(&f, 0);
    return ok;
}
int if_case_limits(void) {
    if_fixture f;
    if_init(&f);
    int result = 1;
    cbm_inventory_filter_usage_t u;
    IF_CHECK(if_dependency_control() && if_limit_fixture(&f) && if_prepare(&f) &&
             if_usage_valid(&f, &u));
    IF_CHECK(u.ignore_work_used > 1 && u.ignore_arena_requested_bytes > 1);
    if_finish(&f, 0);
    const uint64_t bounds[] = {3, 2, 4, 8, 2, 8, 2};
    for (unsigned i = 0; i < 7; i++)
        IF_CHECK(if_fixed_cap_round(i, bounds[i]));
    for (unsigned i = 0; i < 3; i++)
        IF_CHECK(if_derived_cap_round(i, &u));
    IF_CHECK(if_reservation_round(&u));
    IF_CHECK(if_limit_fixture(&f));
    f.limits.max_control_total_bytes = UINT64_MAX;
    IF_CHECK(if_error(&f, CBM_INVENTORY_LIMIT, SIZE_MAX, false) && f.calls == 0);
    IF_CHECK(if_limit_fixture(&f));
    f.limits.max_files = (size_t)INT_MAX + 1;
    IF_CHECK(if_error(&f, CBM_INVENTORY_INVALID, SIZE_MAX, false) && f.calls == 0);
    result = 0;
done:
    return if_finish(&f, result);
}
