#include "discover/gitignore_internal.h"
#include <limits.h>

typedef struct {
    gi_guard_t *guard;
    const char *bytes;
    size_t length;
    cbm_ignore_program_t *program;
    size_t count;
    unsigned fault;
} gi_parse_t;

static bool gi_line(gi_parse_t *parse, size_t *cursor, gi_view_t *view) {
    size_t start = *cursor;
    while (*cursor < parse->length) {
        parse->guard->offset = *cursor;
        /* Both exact-byte validation and line splitting visit this byte. */
        if (!gi_work(parse->guard, 2)) {
            return false;
        }
        char byte = parse->bytes[*cursor];
        if (!byte) {
            return gi_fail(parse->guard, CBM_IGNORE_CHECKED_UNSUPPORTED, CBM_IGNORE_CAP_NONE);
        }
        if (byte == '\n') {
            break;
        }
        (*cursor)++;
        if (*cursor - start > INT_MAX) {
            return gi_fail(parse->guard, CBM_IGNORE_CHECKED_LIMIT, CBM_IGNORE_CAP_REPRESENTATION);
        }
    }
    size_t length = *cursor - start;
    if (*cursor < parse->length) {
        (*cursor)++;
    }
    parse->guard->offset = start;
    return gi_normalize(parse->bytes + start, length, view, parse->guard);
}

static bool gi_store_pattern(gi_parse_t *parse, const gi_view_t *view) {
    bool fault = parse->fault == GI_FAULT_PATTERN && parse->count == 0;
    char *text = gi_allocate(parse->guard, view->length + 1, fault);
    if (!text) {
        return false;
    }
    for (size_t i = 0; i <= view->length; i++) {
        if (!gi_work(parse->guard, 1)) {
            return false;
        }
        text[i] = i == view->length ? '\0' : view->text[i];
    }
    gi_pattern_t row = {.pattern = text,
                        .negated = view->negated,
                        .dir_only = view->dir_only,
                        .rooted = view->rooted};
    parse->program->rows[parse->count++] = row;
    return true;
}

static bool gi_parse_pass(gi_parse_t *parse) {
    size_t cursor = 0;
    while (cursor < parse->length) {
        gi_view_t view;
        if (!gi_line(parse, &cursor, &view)) {
            return false;
        }
        if (!view.length) {
            continue;
        }
        if (parse->program) {
            if (!gi_store_pattern(parse, &view)) {
                return false;
            }
        } else {
            if (!gi_pattern_reserve(parse->guard)) {
                return false;
            }
            parse->count++;
        }
    }
    return true;
}

static bool gi_parse_storage(gi_parse_t *parse) {
    size_t count = parse->count;
    if (count > (SIZE_MAX - sizeof(cbm_ignore_program_t)) / sizeof(gi_pattern_t)) {
        return gi_fail(parse->guard, CBM_IGNORE_CHECKED_LIMIT, CBM_IGNORE_CAP_ARENA);
    }
    size_t bytes = sizeof(cbm_ignore_program_t) + count * sizeof(gi_pattern_t);
    parse->program = gi_allocate(parse->guard, bytes, parse->fault == GI_FAULT_PROGRAM);
    if (!parse->program || !gi_zero(parse->guard, parse->program, bytes)) {
        return false;
    }
    parse->program->owner = parse->guard->owner;
    parse->program->count = count;
    parse->count = 0;
    return true;
}

static cbm_ignore_checked_status_t gi_parse(cbm_ignore_checked_t *owner, const void *bytes,
                                            size_t length,
                                            const cbm_ignore_checked_control_t *control,
                                            unsigned fault, cbm_ignore_checked_parse_result_t *out,
                                            cbm_ignore_checked_error_t *error) {
    if (out) {
        *out = (cbm_ignore_checked_parse_result_t){0};
    }
    gi_error(error, CBM_IGNORE_CHECKED_OK, CBM_IGNORE_CAP_NONE, SIZE_MAX);
    if (!owner || (!bytes && length) || !control || !control->deadline_ms || !out ||
        fault > GI_FAULT_PATTERN) {
        gi_error(error, CBM_IGNORE_CHECKED_INVALID, CBM_IGNORE_CAP_NONE, SIZE_MAX);
        return CBM_IGNORE_CHECKED_INVALID;
    }
    gi_guard_t guard;
    if (!gi_begin(owner, control, &guard) || !gi_bytes(&guard, length)) {
        return gi_finish(owner, error);
    }
    gi_parse_t parse = {.guard = &guard, .bytes = bytes, .length = length, .fault = fault};
    if (gi_parse_pass(&parse) && gi_parse_storage(&parse) && gi_parse_pass(&parse) &&
        gi_poll(&guard)) {
        out->program = parse.program;
        out->pattern_count = parse.count;
    }
    return gi_finish(owner, error);
}

cbm_ignore_checked_status_t cbm_ignore_checked_parse(cbm_ignore_checked_t *owner, const void *bytes,
                                                     size_t length,
                                                     const cbm_ignore_checked_control_t *control,
                                                     cbm_ignore_checked_parse_result_t *out,
                                                     cbm_ignore_checked_error_t *error) {
    return gi_parse(owner, bytes, length, control, GI_FAULT_NONE, out, error);
}

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
cbm_ignore_checked_status_t cbm_ignore_checked_parse_fault_for_tests(
    cbm_ignore_checked_t *owner, const void *bytes, size_t length,
    const cbm_ignore_checked_control_t *control, cbm_ignore_parse_fault_t fault,
    cbm_ignore_checked_parse_result_t *out, cbm_ignore_checked_error_t *error) {
    return gi_parse(owner, bytes, length, control, (unsigned)fault, out, error);
}
#endif
