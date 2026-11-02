#include "discover/inventory_internal.h"
#include "discover/inventory_limits_internal.h"
#include <limits.h>
#include <string.h>

bool cif_limits_valid(const cbm_inventory_limits_t *l) {
    return l && l->max_files && l->max_files <= INT_MAX && l->max_directories &&
           l->max_directories <= INT_MAX && l->max_arena_bytes && l->max_ignore_arena_bytes &&
           l->max_ignore_arena_bytes <= l->max_arena_bytes && l->max_control_file_bytes &&
           l->max_control_total_bytes && l->max_ignore_patterns &&
           l->max_ignore_patterns <= INT_MAX && l->max_probe_prefix_bytes && l->max_ignore_work &&
           l->max_verified_file_reads && l->max_verified_content_bytes;
}

static bool cif_open_ignore(cif_context *c) {
    cbm_inventory_limits_t *l = &c->owner->limits;
    cbm_ignore_checked_limits_t limits = {.max_bytes =
                                              l->max_control_total_bytes + l->max_ignore_work,
                                          .max_work = l->max_ignore_work,
                                          .max_patterns = l->max_ignore_patterns,
                                          .max_arena_bytes = l->max_ignore_arena_bytes,
                                          .max_depth = CBM_IGNORE_CHECKED_DEPTH_MAX};
    c->file_index = SIZE_MAX;
    if (!cif_poll(c)) {
        return false;
    }
    cbm_ignore_checked_status_t status = cbm_ignore_checked_open(&limits, &c->owner->ignore, NULL);
    return cif_checked(c, status) && cif_poll(c);
}

static bool cif_publish(cif_context *c) {
    cbm_inventory_filter_t *o = c->owner;
    cbm_ignore_checked_usage_t checked;
    c->file_index = SIZE_MAX;
    if (!cbm_ignore_checked_usage(o->ignore, &checked) ||
        checked.terminal_status != CBM_IGNORE_CHECKED_OK) {
        return cif_fail(c, CBM_INVENTORY_STATE);
    }
    o->usage.ignore_bytes_reserved = checked.bytes_reserved;
    o->usage.ignore_work_used = checked.work_used;
    o->usage.ignore_patterns_reserved = checked.patterns_reserved;
    o->usage.ignore_arena_requested_bytes = checked.arena_requested_bytes;
    size_t d = o->usage.non_ignore_arena_requested_bytes;
    size_t i = o->limits.max_ignore_arena_bytes;
    if (checked.arena_requested_bytes > i || d > o->limits.max_arena_bytes - i) {
        return cif_fail(c, CBM_INVENTORY_STATE);
    }
    o->usage.arena_requested_bytes = d + checked.arena_requested_bytes;
    o->usage.arena_budget_used_bytes = d + i;
    o->view.files = o->files;
    o->view.rows = o->rows;
    o->view.controls = o->controls;
    o->view.excluded_directories = o->exclusions;
    return cif_poll(c);
}

cbm_inventory_status_t cbm_inventory_filter_prepare(const cbm_inventory_source_t *source,
                                                    const cbm_inventory_limits_t *limits,
                                                    const cbm_inventory_control_t *control,
                                                    cbm_inventory_filter_t **out,
                                                    cbm_inventory_error_t *error) {
    cbm_inventory_error_t initial = {.status = CBM_INVENTORY_OK, .file_index = SIZE_MAX};
    if (out) {
        *out = NULL;
    }
    if (error) {
        *error = initial;
    }
    cbm_inventory_filter_t bootstrap = {0};
    cif_context c = {.owner = &bootstrap,
                     .source = source,
                     .control = control,
                     .error = initial,
                     .file_index = SIZE_MAX};
    if (!out || !source || !cif_limits_valid(limits) || !control || !control->deadline_ms ||
        !source->native_root || !source->read || (source->file_count && !source->files)) {
        cif_fail(&c, CBM_INVENTORY_INVALID);
    } else if (source->file_count > limits->max_files || source->file_count > INT_MAX ||
               limits->max_control_total_bytes > UINT64_MAX - limits->max_ignore_work) {
        cif_fail(&c, CBM_INVENTORY_LIMIT);
    } else if (cif_poll(&c)) {
        bootstrap.limits = *limits;
        bootstrap.deadline_ms = control->deadline_ms;
        cbm_arena_init_lazy(&bootstrap.arena, CBM_ARENA_DEFAULT_BLOCK_SIZE);
        cbm_inventory_filter_t *owner = cif_alloc(&c, 1, sizeof(*owner));
        if (owner && cif_copy(&c, owner, &bootstrap, sizeof(*owner))) {
            c.owner = owner;
            if (cif_metadata(&c) && cif_directories(&c) && cif_open_ignore(&c) && cif_filter(&c) &&
                cif_publish(&c)) {
                *out = owner;
                return CBM_INVENTORY_OK;
            }
        }
    }
    cbm_inventory_filter_free(c.owner);
    if (error) {
        *error = c.error;
    }
    return c.error.status;
}

const cbm_inventory_filter_view_t *cbm_inventory_filter_view(const cbm_inventory_filter_t *owner) {
    return owner ? &owner->view : NULL;
}

bool cbm_inventory_filter_usage(const cbm_inventory_filter_t *owner,
                                cbm_inventory_filter_usage_t *out) {
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

bool cbm_inventory_filter_limits(const cbm_inventory_filter_t *owner, cbm_inventory_limits_t *out) {
    if (!out) {
        return false;
    }
    memset(out, 0, sizeof(*out));
    if (!owner) {
        return false;
    }
    *out = owner->limits;
    return true;
}

void cbm_inventory_filter_free(cbm_inventory_filter_t *owner) {
    if (owner) {
        cbm_ignore_checked_free(owner->ignore);
        CBMArena arena = owner->arena;
        cbm_arena_destroy(&arena);
    }
}
