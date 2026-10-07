#include "discover/gitignore_internal.h"
#include <limits.h>
#include <string.h>

static bool gi_limits_valid(const cbm_ignore_checked_limits_t *limits) {
    return limits && limits->max_bytes && limits->max_work && limits->max_patterns &&
           limits->max_patterns <= INT_MAX && limits->max_arena_bytes && limits->max_depth &&
           limits->max_depth <= CBM_IGNORE_CHECKED_DEPTH_MAX;
}

static cbm_ignore_checked_status_t gi_open(const cbm_ignore_checked_limits_t *limits,
                                           cbm_ignore_checked_t **out,
                                           cbm_ignore_checked_error_t *error, bool fault) {
    if (out) {
        *out = NULL;
    }
    gi_error(error, CBM_IGNORE_CHECKED_OK, CBM_IGNORE_CAP_NONE, SIZE_MAX);
    if (!out || !gi_limits_valid(limits)) {
        gi_error(error, CBM_IGNORE_CHECKED_INVALID, CBM_IGNORE_CAP_NONE, SIZE_MAX);
        return CBM_IGNORE_CHECKED_INVALID;
    }
    if (limits->max_arena_bytes < sizeof(cbm_ignore_checked_t)) {
        gi_error(error, CBM_IGNORE_CHECKED_LIMIT, CBM_IGNORE_CAP_ARENA, SIZE_MAX);
        return CBM_IGNORE_CHECKED_LIMIT;
    }
    CBMArena arena;
    cbm_arena_init_lazy(&arena, CBM_ARENA_DEFAULT_BLOCK_SIZE);
    cbm_ignore_checked_t *owner = fault ? NULL : cbm_arena_alloc(&arena, sizeof(*owner));
    if (!owner) {
        cbm_arena_destroy(&arena);
        gi_error(error, CBM_IGNORE_CHECKED_OOM, CBM_IGNORE_CAP_NONE, SIZE_MAX);
        return CBM_IGNORE_CHECKED_OOM;
    }
    memset(owner, 0, sizeof(*owner));
    owner->arena = arena;
    owner->limits = *limits;
    owner->usage.arena_requested_bytes = sizeof(*owner);
    gi_error(&owner->error, CBM_IGNORE_CHECKED_OK, CBM_IGNORE_CAP_NONE, SIZE_MAX);
    *out = owner;
    return CBM_IGNORE_CHECKED_OK;
}

cbm_ignore_checked_status_t cbm_ignore_checked_open(const cbm_ignore_checked_limits_t *limits,
                                                    cbm_ignore_checked_t **out,
                                                    cbm_ignore_checked_error_t *error) {
    return gi_open(limits, out, error, false);
}

static bool gi_path_valid(gi_guard_t *guard, const char *path, size_t length) {
    for (size_t i = 0; i <= length; i++) {
        guard->offset = i;
        if (!gi_work(guard, 1)) {
            return false;
        }
        if ((path[i] == '\0') != (i == length)) {
            return gi_fail(guard, CBM_IGNORE_CHECKED_UNSUPPORTED, CBM_IGNORE_CAP_NONE);
        }
    }
    guard->offset = SIZE_MAX;
    return true;
}

cbm_ignore_checked_status_t cbm_ignore_checked_match(
    cbm_ignore_checked_t *owner, const cbm_ignore_program_t *program, const char *path,
    size_t length, bool is_directory, const cbm_ignore_checked_control_t *control,
    cbm_ignore_decision_t *decision, cbm_ignore_checked_error_t *error) {
    if (decision) {
        *decision = CBM_IGNORE_UNAVAILABLE;
    }
    gi_error(error, CBM_IGNORE_CHECKED_OK, CBM_IGNORE_CAP_NONE, SIZE_MAX);
    if (!owner || !program || !path || length == SIZE_MAX || !control || !control->deadline_ms ||
        !decision || program->owner != owner) {
        gi_error(error, CBM_IGNORE_CHECKED_INVALID, CBM_IGNORE_CAP_NONE, SIZE_MAX);
        return CBM_IGNORE_CHECKED_INVALID;
    }
    gi_guard_t guard;
    if (!gi_begin(owner, control, &guard) || !gi_bytes(&guard, length + 1) ||
        !gi_path_valid(&guard, path, length)) {
        return gi_finish(owner, error);
    }
    int result = gi_core_match(program->rows, program->count, path, is_directory, &guard);
    if (gi_poll(&guard)) {
        *decision = (cbm_ignore_decision_t)result;
    }
    return gi_finish(owner, error);
}

bool cbm_ignore_checked_usage(const cbm_ignore_checked_t *owner, cbm_ignore_checked_usage_t *out) {
    if (!out) {
        return false;
    }
    memset(out, 0, sizeof(*out));
    if (!owner) {
        return false;
    }
    *out = owner->usage;
    return true;
}

void cbm_ignore_checked_free(cbm_ignore_checked_t *owner) {
    if (owner) {
        /* The owner itself lives in the arena being destroyed. */
        CBMArena arena = owner->arena;
        cbm_arena_destroy(&arena);
    }
}

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
cbm_ignore_checked_status_t cbm_ignore_checked_open_oom_for_tests(
    const cbm_ignore_checked_limits_t *limits, cbm_ignore_checked_t **out,
    cbm_ignore_checked_error_t *error) {
    return gi_open(limits, out, error, true);
}
#endif
