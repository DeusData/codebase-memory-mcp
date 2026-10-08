#include "discover/gitignore_internal.h"
#include "foundation/platform.h"
#include <string.h>

void gi_error(cbm_ignore_checked_error_t *error, cbm_ignore_checked_status_t status,
              cbm_ignore_cap_t cap, size_t offset) {
    if (!error) {
        return;
    }
    memset(error, 0, sizeof(*error));
    error->status = status;
    error->cap = cap;
    error->byte_offset = offset;
    if (status != CBM_IGNORE_CHECKED_OK) {
        static const char message[] = "checked ignore operation failed";
        memcpy(error->diagnostic, message, sizeof(message));
    }
}

bool gi_active(const gi_guard_t *guard) {
    return !guard || guard->owner->usage.terminal_status == CBM_IGNORE_CHECKED_OK;
}

bool gi_fail(gi_guard_t *guard, cbm_ignore_checked_status_t status, cbm_ignore_cap_t cap) {
    if (guard && gi_active(guard)) {
        guard->owner->usage.terminal_status = status;
        gi_error(&guard->owner->error, status, cap, guard->offset);
    }
    return false;
}

bool gi_poll(gi_guard_t *guard) {
    if (!gi_active(guard)) {
        return false;
    }
    if (!guard) {
        return true;
    }
    const cbm_ignore_checked_control_t *control = guard->control;
    if (control->cancelled && control->cancelled(control->context)) {
        return gi_fail(guard, CBM_IGNORE_CHECKED_CANCELLED, CBM_IGNORE_CAP_NONE);
    }
    if (cbm_now_ms() >= control->deadline_ms) {
        return gi_fail(guard, CBM_IGNORE_CHECKED_DEADLINE, CBM_IGNORE_CAP_NONE);
    }
    guard->ticks = 0;
    return true;
}

bool gi_work(gi_guard_t *guard, size_t units) {
    if (!gi_active(guard)) {
        return false;
    }
    if (!guard) {
        return true;
    }
    cbm_ignore_checked_t *owner = guard->owner;
    if (units > owner->limits.max_work - owner->usage.work_used) {
        return gi_fail(guard, CBM_IGNORE_CHECKED_LIMIT, CBM_IGNORE_CAP_WORK);
    }
    /* All callers submit a constant-size event or a bounded memory chunk. */
    while (units) {
        size_t part = GI_POLL_UNITS - guard->ticks;
        if (part > units) {
            part = units;
        }
        owner->usage.work_used += part;
        guard->ticks += (unsigned)part;
        units -= part;
        if (guard->ticks == GI_POLL_UNITS && !gi_poll(guard)) {
            return false;
        }
    }
    return true;
}

bool gi_bytes(gi_guard_t *guard, size_t bytes) {
    cbm_ignore_checked_t *owner = guard->owner;
    if (bytes > owner->limits.max_bytes - owner->usage.bytes_reserved) {
        return gi_fail(guard, CBM_IGNORE_CHECKED_LIMIT, CBM_IGNORE_CAP_BYTES);
    }
    owner->usage.bytes_reserved += bytes;
    return true;
}

bool gi_pattern_reserve(gi_guard_t *guard) {
    cbm_ignore_checked_t *owner = guard->owner;
    if (!gi_work(guard, 1)) {
        return false;
    }
    if (owner->usage.patterns_reserved == owner->limits.max_patterns) {
        return gi_fail(guard, CBM_IGNORE_CHECKED_LIMIT, CBM_IGNORE_CAP_PATTERNS);
    }
    owner->usage.patterns_reserved++;
    return true;
}

bool gi_begin(cbm_ignore_checked_t *owner, const cbm_ignore_checked_control_t *control,
              gi_guard_t *guard) {
    *guard = (gi_guard_t){.owner = owner, .control = control, .offset = SIZE_MAX};
    return gi_poll(guard) && gi_work(guard, 1);
}

cbm_ignore_checked_status_t gi_finish(const cbm_ignore_checked_t *owner,
                                      cbm_ignore_checked_error_t *error) {
    if (error) {
        *error = owner->error;
    }
    return owner->usage.terminal_status;
}

void *gi_allocate(gi_guard_t *guard, size_t bytes, bool simulate_oom) {
    cbm_ignore_checked_t *owner = guard->owner;
    CBMArena *arena = &owner->arena;
    if (!gi_poll(guard)) {
        return NULL;
    }
    if (bytes > owner->limits.max_arena_bytes - owner->usage.arena_requested_bytes ||
        bytes > (SIZE_MAX / 2) - GI_ARENA_ALIGN) {
        gi_fail(guard, CBM_IGNORE_CHECKED_LIMIT, CBM_IGNORE_CAP_ARENA);
        return NULL;
    }
    size_t aligned = (bytes + GI_ARENA_ALIGN) & ~(size_t)GI_ARENA_ALIGN;
    if (aligned > SIZE_MAX - arena->used || aligned > SIZE_MAX - arena->total_alloc) {
        gi_fail(guard, CBM_IGNORE_CHECKED_LIMIT, CBM_IGNORE_CAP_ARENA);
        return NULL;
    }
    owner->usage.arena_requested_bytes += bytes;
    /* Keep the arena's doubling arithmetic representable; no allocator changes. */
    if (arena->grow_size > SIZE_MAX / 2) {
        arena->grow_size = CBM_ARENA_DEFAULT_BLOCK_SIZE;
    }
    void *memory = simulate_oom ? NULL : cbm_arena_alloc(arena, bytes);
    if (!memory) {
        gi_fail(guard, CBM_IGNORE_CHECKED_OOM, CBM_IGNORE_CAP_NONE);
    }
    return memory;
}

bool gi_zero(gi_guard_t *guard, void *storage, size_t bytes) {
    unsigned char *output = storage;
    enum { ZERO_CHUNK = 256 };
    while (bytes) {
        size_t chunk = bytes < ZERO_CHUNK ? bytes : ZERO_CHUNK;
        if (!gi_work(guard, chunk)) {
            return false;
        }
        memset(output, 0, chunk);
        output += chunk;
        bytes -= chunk;
    }
    return true;
}

static bool gi_trim(const char *line, size_t *length, gi_guard_t *guard) {
    while (*length) {
        if (!gi_work(guard, 1)) {
            return false;
        }
        char last = line[*length - 1];
        if (last != ' ' && last != '\t' && last != '\r') {
            break;
        }
        (*length)--;
    }
    return true;
}

bool gi_normalize(const char *line, size_t length, gi_view_t *view, gi_guard_t *guard) {
    *view = (gi_view_t){0};
    if (!length || !gi_work(guard, 1) || line[0] == '#') {
        return gi_active(guard);
    }
    if (!gi_trim(line, &length, guard) || !length) {
        return gi_active(guard);
    }
    if (!gi_work(guard, 1)) {
        return false;
    }
    if (*line == '!') {
        view->negated = true;
        line++;
        length--;
    }
    if (!length || !gi_work(guard, 1)) {
        return gi_active(guard);
    }
    if (line[length - 1] == '/') {
        view->dir_only = true;
        length--;
    }
    if (!length || !gi_work(guard, 1)) {
        return gi_active(guard);
    }
    if (*line == '/') {
        view->rooted = true;
        line++;
        length--;
    }
    for (size_t i = 0; !view->rooted && i < length; i++) {
        if (!gi_work(guard, 1)) {
            return false;
        }
        view->rooted = line[i] == '/';
    }
    view->text = line;
    view->length = length;
    return true;
}
