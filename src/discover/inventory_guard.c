#include "discover/inventory_internal.h"
#include "foundation/platform.h"
#include <string.h>

bool cif_fail(cif_context *c, cbm_inventory_status_t status) {
    if (c->error.status == CBM_INVENTORY_OK) {
        c->error.status = status;
        c->error.file_index = c->file_index;
        strcpy(c->error.diagnostic, "inventory filter could not complete");
    }
    return false;
}

bool cif_poll(cif_context *c) {
    if (c->error.status != CBM_INVENTORY_OK) {
        return false;
    }
    if (c->control->cancelled && c->control->cancelled(c->control->context)) {
        return cif_fail(c, CBM_INVENTORY_CANCELLED);
    }
    if (cbm_now_ms() >= c->control->deadline_ms) {
        return cif_fail(c, CBM_INVENTORY_DEADLINE);
    }
    c->byte_gap = CIF_BYTE_GAP;
    c->event_gap = CIF_EVENT_GAP;
    return true;
}

bool cif_bytes(cif_context *c, size_t count) {
    if (c->error.status != CBM_INVENTORY_OK) {
        return false;
    }
    if (count > CIF_BYTE_GAP) {
        return cif_fail(c, CBM_INVENTORY_STATE);
    }
    if (count > c->byte_gap && !cif_poll(c)) {
        return false;
    }
    c->byte_gap -= count;
    return true;
}

bool cif_event(cif_context *c) {
    if (c->error.status != CBM_INVENTORY_OK || (!c->event_gap && !cif_poll(c))) {
        return false;
    }
    c->event_gap--;
    return true;
}

bool cif_copy(cif_context *c, void *destination, const void *source, size_t length) {
    unsigned char *out = destination;
    const unsigned char *in = source;
    while (length) {
        size_t n = length < CIF_COPY_CHUNK ? length : CIF_COPY_CHUNK;
        if (!cif_bytes(c, n * 2)) {
            return false;
        }
        if (in) {
            memcpy(out, in, n);
            in += n;
        } else {
            memset(out, 0, n);
        }
        out += n;
        length -= n;
    }
    return true;
}

/* Guard arithmetic also used internally by the existing arena allocator. */
static bool cif_arena_room(cif_context *c, size_t n) {
    CBMArena *a = &c->owner->arena;
    if (n > SIZE_MAX - 7) {
        return cif_fail(c, CBM_INVENTORY_LIMIT);
    }
    size_t aligned = (n + 7) & ~(size_t)7;
    if (a->used > SIZE_MAX - aligned || a->total_alloc > SIZE_MAX - aligned) {
        return cif_fail(c, CBM_INVENTORY_LIMIT);
    }
    if (!a->nblocks || a->used + aligned > a->block_size) {
        size_t next = a->grow_size > aligned ? a->grow_size : aligned;
        size_t tail = a->block_size > a->used ? a->block_size - a->used : 0;
        if (next > SIZE_MAX / 2 || a->waste_tail > SIZE_MAX - tail) {
            return cif_fail(c, CBM_INVENTORY_LIMIT);
        }
    }
    return true;
}

void *cif_alloc(cif_context *c, size_t count, size_t width) {
    if (!count || !width) {
        return NULL;
    }
    if (count > SIZE_MAX / width) {
        cif_fail(c, CBM_INVENTORY_LIMIT);
        return NULL;
    }
    size_t n = count * width;
    cbm_inventory_filter_t *o = c->owner;
    size_t available = o->limits.max_arena_bytes - o->limits.max_ignore_arena_bytes;
    if (n > available - o->usage.non_ignore_arena_requested_bytes) {
        cif_fail(c, CBM_INVENTORY_LIMIT);
        return NULL;
    }
    if (!cif_poll(c) || !cif_arena_room(c, n)) {
        return NULL;
    }
    o->usage.non_ignore_arena_requested_bytes += n;
    void *result = cbm_arena_alloc(&o->arena, n);
    if (!result) {
        cif_fail(c, CBM_INVENTORY_OOM);
        return NULL;
    }
    if (!cif_poll(c) || !cif_copy(c, result, NULL, n)) {
        return NULL;
    }
    return result;
}

unsigned char *cif_string(cif_context *c, const unsigned char *source, size_t length) {
    if (length == SIZE_MAX) {
        cif_fail(c, CBM_INVENTORY_LIMIT);
        return NULL;
    }
    unsigned char *result = cif_alloc(c, length + 1, 1);
    return result && cif_copy(c, result, source, length) ? result : NULL;
}

bool cif_compare(cif_context *c, cbm_inventory_path_t a, cbm_inventory_path_t b, int *result) {
    if (!cif_event(c)) {
        return false;
    }
    size_t n = a.length < b.length ? a.length : b.length;
    for (size_t i = 0; i < n; i++) {
        if (!cif_bytes(c, 2)) {
            return false;
        }
        unsigned char left = a.data[i], right = b.data[i];
        if (left != right) {
            *result = left < right ? -1 : 1;
            return true;
        }
    }
    *result = a.length < b.length ? -1 : a.length > b.length;
    return true;
}

bool cif_checked(cif_context *c, cbm_ignore_checked_status_t status) {
    switch (status) {
    case CBM_IGNORE_CHECKED_OK:
        return true;
    case CBM_IGNORE_CHECKED_INVALID:
        return cif_fail(c, CBM_INVENTORY_INVALID);
    case CBM_IGNORE_CHECKED_UNSUPPORTED:
        return cif_fail(c, CBM_INVENTORY_UNSUPPORTED);
    case CBM_IGNORE_CHECKED_LIMIT:
        return cif_fail(c, CBM_INVENTORY_LIMIT);
    case CBM_IGNORE_CHECKED_OOM:
        return cif_fail(c, CBM_INVENTORY_OOM);
    case CBM_IGNORE_CHECKED_CANCELLED:
        return cif_fail(c, CBM_INVENTORY_CANCELLED);
    case CBM_IGNORE_CHECKED_DEADLINE:
        return cif_fail(c, CBM_INVENTORY_DEADLINE);
    }
    return cif_fail(c, CBM_INVENTORY_INVALID);
}

cbm_ignore_checked_control_t cif_ignore_control(const cif_context *c) {
    return (cbm_ignore_checked_control_t){c->control->deadline_ms, c->control->cancelled,
                                          c->control->context};
}
