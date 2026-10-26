#include "mcp/test_impact_inventory_internal.h"
#include "discover/inventory_limits_internal.h"
#include "foundation/platform.h"
#include <string.h>

bool cni_fail(cni_context *c, cbm_inventory_status_t status) {
    if (c->error.status == CBM_INVENTORY_OK) {
        c->error.status = status;
        c->error.file_index = c->file_index;
        strcpy(c->error.diagnostic, "native inventory preparation could not complete");
    }
    return false;
}

bool cni_poll(cni_context *c) {
    if (c->error.status != CBM_INVENTORY_OK) {
        return false;
    }
    if (c->control->cancelled && c->control->cancelled(c->control->context)) {
        return cni_fail(c, CBM_INVENTORY_CANCELLED);
    }
    if (cbm_now_ms() >= c->control->deadline_ms) {
        return cni_fail(c, CBM_INVENTORY_DEADLINE);
    }
    c->byte_gap = CNI_BYTE_GAP;
    c->event_gap = CNI_EVENT_GAP;
    return true;
}

bool cni_bytes(cni_context *c, size_t count) {
    if (c->error.status != CBM_INVENTORY_OK) {
        return false;
    }
    if (count > CNI_BYTE_GAP) {
        return cni_fail(c, CBM_INVENTORY_STATE);
    }
    if (count > c->byte_gap && !cni_poll(c)) {
        return false;
    }
    c->byte_gap -= count;
    return true;
}

bool cni_event(cni_context *c) {
    if (c->error.status != CBM_INVENTORY_OK || (!c->event_gap && !cni_poll(c))) {
        return false;
    }
    c->event_gap--;
    return true;
}

bool cni_copy(cni_context *c, void *out, const void *in, size_t length) {
    unsigned char *to = out;
    const unsigned char *from = in;
    while (length) {
        size_t count = length < CNI_COPY_CHUNK ? length : CNI_COPY_CHUNK;
        if (!cni_bytes(c, count * 2)) {
            return false;
        }
        if (from) {
            memcpy(to, from, count);
            from += count;
        } else {
            memset(to, 0, count);
        }
        to += count;
        length -= count;
    }
    return true;
}

static bool cni_arena_room(cni_context *c, const CBMArena *arena, size_t size) {
    if (size > SIZE_MAX - 7) {
        return cni_fail(c, CBM_INVENTORY_LIMIT);
    }
    size_t aligned = (size + 7) & ~(size_t)7;
    if (arena->used > SIZE_MAX - aligned || arena->total_alloc > SIZE_MAX - aligned) {
        return cni_fail(c, CBM_INVENTORY_LIMIT);
    }
    if (!arena->nblocks || arena->used + aligned > arena->block_size) {
        size_t next = arena->grow_size > aligned ? arena->grow_size : aligned;
        size_t tail = arena->block_size > arena->used ? arena->block_size - arena->used : 0;
        if (next > SIZE_MAX / 2 || arena->waste_tail > SIZE_MAX - tail ||
            arena->waste_grows == SIZE_MAX) {
            return cni_fail(c, CBM_INVENTORY_LIMIT);
        }
    }
    return true;
}

void *cni_alloc(cni_context *c, CBMArena *arena, size_t count, size_t width) {
    if (!count || !width) {
        return NULL;
    }
    if (count > SIZE_MAX / width) {
        cni_fail(c, CBM_INVENTORY_LIMIT);
        return NULL;
    }
    size_t size = count * width;
    size_t available = c->owner->limits.max_arena_bytes - c->owner->limits.max_ignore_arena_bytes;
    size_t *charged = &c->owner->usage.attachment_arena_requested_bytes;
    if (*charged > available || size > available - *charged) {
        cni_fail(c, CBM_INVENTORY_LIMIT);
        return NULL;
    }
    if (!cni_poll(c) || !cni_arena_room(c, arena, size)) {
        return NULL;
    }
    *charged += size;
    void *result = cbm_arena_alloc(arena, size);
    if (!result) {
        cni_fail(c, CBM_INVENTORY_OOM);
        return NULL;
    }
    return cni_poll(c) && cni_copy(c, result, NULL, size) ? result : NULL;
}

static bool cni_delegate(cni_context *c, cbm_pinned_tree_t *tree, cbm_inventory_source_t *source) {
    cbm_test_impact_inventory_t *owner = c->owner;
    cbm_inventory_limits_t limits = owner->limits;
    size_t charged = owner->usage.attachment_arena_requested_bytes;
    c->file_index = SIZE_MAX;
    if (charged > limits.max_arena_bytes - limits.max_ignore_arena_bytes) {
        return cni_fail(c, CBM_INVENTORY_LIMIT);
    }
    if (!cni_poll(c)) {
        return false;
    }
    limits.max_arena_bytes -= charged;
    cni_reader reader = {.context = c,
                         .tree = tree,
                         .files = source->files,
                         .file_count = source->file_count,
                         .content_bound = (uint64_t)limits.max_control_file_bytes};
    source->read = cni_read;
    source->read_context = &reader;
    cbm_inventory_error_t error;
    cbm_inventory_status_t status =
        cbm_inventory_filter_prepare(source, &limits, c->control, &owner->filter, &error);
    /* The reader lives in this frame; the caller's source must not keep it. */
    source->read = NULL;
    source->read_context = NULL;
    /* The filter owns all successful metadata; this release never refunds A. */
    cbm_arena_destroy(&c->temporary);
    if (status != CBM_INVENTORY_OK) {
        bool cleanup = c->error.cleanup_required || error.cleanup_required;
        if (c->error.status == CBM_INVENTORY_OK) {
            c->error = error;
        }
        c->error.cleanup_required = cleanup;
        return false;
    }
    if (!owner->filter) {
        return cni_fail(c, CBM_INVENTORY_STATE);
    }
    return true;
}

static bool cni_publish(cni_context *c) {
    cbm_test_impact_inventory_t *owner = c->owner;
    cbm_test_impact_inventory_usage_t *usage = &owner->usage;
    c->file_index = SIZE_MAX;
    if (!cbm_inventory_filter_usage(owner->filter, &usage->filter)) {
        return cni_fail(c, CBM_INVENTORY_STATE);
    }
    size_t a = usage->attachment_arena_requested_bytes;
    size_t d = usage->filter.non_ignore_arena_requested_bytes;
    size_t checked = usage->filter.ignore_arena_requested_bytes;
    size_t i = owner->limits.max_ignore_arena_bytes;
    size_t t = owner->limits.max_arena_bytes;
    if (a > t - i || d > t - i - a || checked > i ||
        usage->filter.arena_requested_bytes != d + checked ||
        usage->filter.arena_budget_used_bytes != d + i) {
        return cni_fail(c, CBM_INVENTORY_STATE);
    }
    usage->arena_requested_bytes = a + d + checked;
    usage->arena_budget_used_bytes = a + d + i;
    return cni_poll(c);
}

cbm_inventory_status_t cbm_test_impact_inventory_prepare(cbm_pinned_tree_t *tree,
                                                         const cbm_inventory_limits_t *limits,
                                                         const cbm_inventory_control_t *control,
                                                         cbm_test_impact_inventory_t **out,
                                                         cbm_inventory_error_t *error) {
    cbm_inventory_error_t initial = {.status = CBM_INVENTORY_OK, .file_index = SIZE_MAX};
    if (out) {
        *out = NULL;
    }
    if (error) {
        *error = initial;
    }
    cbm_test_impact_inventory_t bootstrap = {0};
    cni_context c = {
        .owner = &bootstrap, .control = control, .error = initial, .file_index = SIZE_MAX};
    if (!out || !tree || !cif_limits_valid(limits) || !control || !control->deadline_ms) {
        cni_fail(&c, CBM_INVENTORY_INVALID);
    } else if (limits->max_control_total_bytes > UINT64_MAX - limits->max_ignore_work ||
               (size_t)(uint64_t)limits->max_control_file_bytes != limits->max_control_file_bytes) {
        cni_fail(&c, CBM_INVENTORY_LIMIT);
    } else if (cni_poll(&c)) {
        bootstrap.limits = *limits;
        bootstrap.deadline_ms = control->deadline_ms;
        const cbm_pinned_tree_view_t *view = cbm_pinned_tree_view(tree);
        if (cni_binding(&c, view)) {
            cbm_arena_init_lazy(&bootstrap.arena, CBM_ARENA_DEFAULT_BLOCK_SIZE);
            cbm_arena_init_lazy(&c.temporary, CBM_ARENA_DEFAULT_BLOCK_SIZE);
            cbm_test_impact_inventory_t *owner = cni_alloc(&c, &bootstrap.arena, 1, sizeof(*owner));
            if (owner && cni_copy(&c, owner, &bootstrap, sizeof(*owner))) {
                c.owner = owner;
                cbm_inventory_source_t source = {0};
                if (cni_snapshot(&c, view, &source) && cni_delegate(&c, tree, &source) &&
                    cni_publish(&c)) {
                    *out = owner;
                    return CBM_INVENTORY_OK;
                }
            }
        }
    }
    cbm_arena_destroy(&c.temporary);
    cbm_test_impact_inventory_free(c.owner);
    if (error) {
        *error = c.error;
    }
    return c.error.status;
}

const cbm_inventory_filter_view_t *cbm_test_impact_inventory_view(
    const cbm_test_impact_inventory_t *owner) {
    return owner ? cbm_inventory_filter_view(owner->filter) : NULL;
}

bool cbm_test_impact_inventory_usage(const cbm_test_impact_inventory_t *owner,
                                     cbm_test_impact_inventory_usage_t *out) {
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

bool cbm_test_impact_inventory_limits(const cbm_test_impact_inventory_t *owner,
                                      cbm_inventory_limits_t *out) {
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

void cbm_test_impact_inventory_free(cbm_test_impact_inventory_t *owner) {
    if (owner) {
        cbm_inventory_filter_free(owner->filter);
        CBMArena arena = owner->arena;
        cbm_arena_destroy(&arena);
    }
}
