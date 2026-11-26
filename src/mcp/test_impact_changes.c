#include "mcp/test_impact_changes.h"

#include "foundation/arena.h"

#include <limits.h>
#include <stdint.h>
#include <string.h>

struct cbm_changes {
    CBMArena arena;
    unsigned char *name_status;
    size_t name_status_length;
    unsigned char *patch;
    size_t patch_length;
    cbm_diff_t *diff;
    cbm_change_file_t *files;
    size_t count;
    cbm_changes_state_t state;
    unsigned issues;
    bool reconciled;
    bool can_narrow;
};

typedef struct {
    const cbm_diff_file_t *file;
    size_t count;
} changes_match_t;

typedef struct {
    const unsigned char *bytes;
    size_t length;
} changes_slice_t;

/* Git tree paths use '/' on every host; backslash/colon are literal bytes. */
static bool changes_path_valid(const unsigned char *path, size_t length) {
    if (!length || path[0] == '/' || path[length - 1] == '/') {
        return false;
    }
    size_t start = 0;
    for (size_t i = 0; i <= length; i++) {
        if (i < length && path[i] != '/') {
            if (path[i] == 0) {
                return false;
            }
            continue;
        }
        size_t component = i - start;
        if (!component || (component == 1 && path[start] == '.') ||
            (component == 2 && path[start] == '.' && path[start + 1] == '.')) {
            return false;
        }
        start = i + 1;
    }
    return true;
}

static bool changes_record(const unsigned char *bytes, size_t length, size_t *offset, char *status,
                           changes_slice_t *path) {
    size_t at = *offset;
    if (length - at < 4 || bytes[at + 1] != 0 ||
        (bytes[at] != 'A' && bytes[at] != 'M' && bytes[at] != 'D' && bytes[at] != 'T')) {
        return false;
    }
    const unsigned char *first = bytes + at + 2;
    const unsigned char *end = memchr(first, 0, length - at - 2);
    if (!end) {
        return false;
    }
    path->bytes = first;
    path->length = (size_t)(end - first);
    if (!changes_path_valid(path->bytes, path->length)) {
        return false;
    }
    *status = (char)bytes[at];
    *offset = (size_t)(end - bytes) + 1;
    return true;
}

static int changes_path_compare(const unsigned char *a, size_t a_length, const unsigned char *b,
                                size_t b_length) {
    size_t common = a_length < b_length ? a_length : b_length;
    int order = memcmp(a, b, common); /* specified unsigned-byte comparison */
    return order ? order : (a_length > b_length) - (a_length < b_length);
}

/* Stable merge order, including equal keys before duplicate rejection. */
static void changes_sort(cbm_change_file_t *files, cbm_change_file_t *scratch, size_t count) {
    cbm_change_file_t *source = files;
    cbm_change_file_t *target = scratch;
    for (size_t width = 1; width < count;) {
        for (size_t left = 0; left < count;) {
            size_t middle = left + (width < count - left ? width : count - left);
            size_t right = middle + (width < count - middle ? width : count - middle);
            size_t a = left;
            size_t b = middle;
            for (size_t i = left; i < right; i++) {
                if (b == right || (a < middle && changes_path_compare(
                                                     source[a].path, source[a].path_length,
                                                     source[b].path, source[b].path_length) <= 0)) {
                    target[i] = source[a++];
                } else {
                    target[i] = source[b++];
                }
            }
            left = right;
        }
        cbm_change_file_t *swap = source;
        source = target;
        target = swap;
        if (width > count / 2) {
            break;
        }
        width *= 2;
    }
    if (source != files) {
        memcpy(files, source, count * sizeof(*files));
    }
}

static size_t changes_find(const cbm_changes_t *changes, const unsigned char *path, size_t length) {
    size_t low = 0;
    size_t high = changes->count;
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        const cbm_change_file_t *file = &changes->files[middle];
        int order = changes_path_compare(file->path, file->path_length, path, length);
        if (!order) {
            return middle;
        }
        if (order < 0) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    return SIZE_MAX;
}

static bool changes_starts(changes_slice_t line, const char *prefix) {
    size_t length = strlen(prefix);
    return line.length >= length && memcmp(line.bytes, prefix, length) == 0;
}

static bool changes_line(const unsigned char *bytes, size_t length, size_t *offset,
                         changes_slice_t *line) {
    if (*offset == length) {
        return false;
    }
    const unsigned char *first = bytes + *offset;
    const unsigned char *end = memchr(first, '\n', length - *offset);
    line->bytes = first;
    line->length = end ? (size_t)(end - first) : length - *offset;
    *offset += line->length + (end ? 1 : 0);
    return true;
}

static bool changes_header_path(changes_slice_t line, changes_slice_t *path) {
    static const char prefix[] = "diff --git a/";
    const size_t prefix_length = sizeof(prefix) - 1;
    if (!changes_starts(line, prefix)) {
        return false;
    }
    size_t rest = line.length - prefix_length;
    if (rest < 5 || (rest - 3) % 2) {
        return false;
    }
    size_t length = (rest - 3) / 2;
    const unsigned char *first = line.bytes + prefix_length;
    if (memcmp(first + length, " b/", 3) || memcmp(first, first + length + 3, length) ||
        !changes_path_valid(first, length)) {
        return false;
    }
    for (size_t i = 0; i < length; i++) {
        /* Git quotes these bytes even with core.quotePath=false. The existing
         * reader does not decode that representation. Non-UTF8 remains raw. */
        if (first[i] < 32 || first[i] == 127 || first[i] == '"' || first[i] == '\\') {
            return false;
        }
    }
    *path = (changes_slice_t){first, length};
    return true;
}

static bool changes_mode(changes_slice_t line, const char *prefix) {
    size_t length = strlen(prefix);
    if (line.length != length + 6) {
        return false;
    }
    const unsigned char *mode = line.bytes + length;
    return memcmp(mode, "100644", 6) == 0 || memcmp(mode, "100755", 6) == 0 ||
           memcmp(mode, "120000", 6) == 0 || memcmp(mode, "160000", 6) == 0;
}

static bool changes_side_path(changes_slice_t line, const cbm_change_file_t *file, bool old) {
    const unsigned char *bytes = line.bytes + 4; /* checked ---/+++ prefix */
    size_t length = line.length - 4;
    bool absent = old ? file->status == 'A' : file->status == 'D';
    if (absent) {
        return length == 9 && memcmp(bytes, "/dev/null", 9) == 0;
    }
    if (length < 2 || bytes[0] != (old ? 'a' : 'b') || bytes[1] != '/') {
        return false;
    }
    bytes += 2;
    length -= 2;
    /* Accept an exact trailing TAB delimiter after the known path, never
     * timestamps, path truncation or another spelling. */
    if (length > file->path_length && length - file->path_length == 1 &&
        bytes[length - 1] == '\t') {
        length--;
    }
    return length == file->path_length && memcmp(bytes, file->path, length) == 0;
}

/* The legacy reader discards the old start. Keep its old-side range from
 * certifying an impossible nonempty range beginning at zero. */
static bool changes_old_range_usable(changes_slice_t line) {
    if (!changes_starts(line, "@@ -")) {
        return false;
    }
    size_t offset = 4;
    int values[2] = {0, 1};
    for (int part = 0; part < 2; part++) {
        size_t first = offset;
        int value = 0;
        while (offset < line.length && line.bytes[offset] >= '0' && line.bytes[offset] <= '9') {
            int digit = line.bytes[offset++] - '0';
            if (value > (INT_MAX - digit) / 10) {
                return false;
            }
            value = value * 10 + digit;
        }
        if (offset == first) {
            return false;
        }
        values[part] = value;
        if (part == 0 && offset < line.length && line.bytes[offset] == ',') {
            offset++;
            continue;
        }
        break;
    }
    return offset < line.length && line.bytes[offset] == ' ' && (values[1] == 0 || values[0] > 0);
}

/* The legacy file result does not retain old/new mode metadata or the old
 * hunk start. Audit headers, never payload (removed text can look like ---). */
static void changes_audit_headers(cbm_changes_t *changes) {
    size_t offset = 0;
    size_t current = SIZE_MAX;
    bool in_hunks = false;
    bool old_seen = false;
    bool new_seen = false;
    changes_slice_t line;
    while (changes_line(changes->patch, changes->patch_length, &offset, &line)) {
        if (changes_starts(line, "diff ")) {
            changes_slice_t path;
            current = SIZE_MAX;
            in_hunks = false;
            old_seen = false;
            new_seen = false;
            if (!changes_header_path(line, &path)) {
                changes->issues |= CBM_CHANGES_PATCH_INCOMPLETE;
            } else {
                current = changes_find(changes, path.bytes, path.length);
                if (current == SIZE_MAX) {
                    changes->issues |= CBM_CHANGES_PATCH_EXTRA_PATH;
                }
            }
            continue;
        }
        if (changes_starts(line, "@@ ")) {
            if (!changes_old_range_usable(line)) {
                changes->issues |= CBM_CHANGES_PATCH_INCOMPLETE;
            }
            if (current != SIZE_MAX && (!old_seen || !new_seen)) {
                changes->issues |= CBM_CHANGES_PATCH_INCOMPLETE;
            }
            in_hunks = true;
        }
        if (current == SIZE_MAX || in_hunks) {
            continue;
        }
        cbm_change_file_t *file = &changes->files[current];
        if (changes_starts(line, "old mode ") || changes_starts(line, "new mode ")) {
            file->reasons |= CBM_CHANGE_MODE;
            if (!changes_mode(line,
                              changes_starts(line, "old mode ") ? "old mode " : "new mode ")) {
                changes->issues |= CBM_CHANGES_PATCH_INCOMPLETE;
            }
        } else if (changes_starts(line, "new file mode ") ||
                   changes_starts(line, "deleted file mode ")) {
            if (!changes_mode(line, changes_starts(line, "new file mode ")
                                        ? "new file mode "
                                        : "deleted file mode ")) {
                changes->issues |= CBM_CHANGES_PATCH_INCOMPLETE;
            }
        } else if (changes_starts(line, "--- ")) {
            if (old_seen || !changes_side_path(line, file, true)) {
                changes->issues |= CBM_CHANGES_PATCH_INCOMPLETE;
            }
            old_seen = true;
        } else if (changes_starts(line, "+++ ")) {
            if (new_seen || !changes_side_path(line, file, false)) {
                changes->issues |= CBM_CHANGES_PATCH_INCOMPLETE;
            }
            new_seen = true;
        }
    }
}

static bool changes_hunks_usable(const cbm_diff_file_t *file) {
    if (file->hunk_count <= 0 || !file->hunks) {
        return false;
    }
    for (int i = 0; i < file->hunk_count; i++) {
        const cbm_diff_hunk_t *hunk = &file->hunks[i];
        if (hunk->start < 0 || hunk->count < 0 || hunk->count > INT_MAX - hunk->start ||
            hunk->added_count != hunk->count || hunk->removed_count < 0 ||
            (hunk->count > 0 && (!hunk->added || hunk->start == 0)) ||
            (hunk->removed_count > 0 && !hunk->removed) ||
            (hunk->added_count == 0 && hunk->removed_count == 0)) {
            return false;
        }
    }
    return true;
}

static void changes_reconcile(cbm_changes_t *changes, changes_match_t *matches) {
    int patch_count = 0;
    const cbm_diff_file_t *files = cbm_diff_files(changes->diff, &patch_count);
    if (!cbm_diff_complete(changes->diff)) {
        changes->issues |= CBM_CHANGES_PATCH_INCOMPLETE;
    }
    for (int i = 0; i < patch_count; i++) {
        const cbm_diff_file_t *file = &files[i];
        size_t length = strlen(file->path);
        size_t index = changes_path_valid((const unsigned char *)file->path, length)
                           ? changes_find(changes, (const unsigned char *)file->path, length)
                           : SIZE_MAX;
        if (index == SIZE_MAX) {
            changes->issues |= CBM_CHANGES_PATCH_EXTRA_PATH;
            continue;
        }
        changes_match_t *match = &matches[index];
        match->file = file;
        if (match->count++ != 0) {
            changes->issues |= CBM_CHANGES_PATCH_DUPLICATE_PATH;
        }
        char status = changes->files[index].status;
        if (file->created != (status == 'A') || file->deleted != (status == 'D')) {
            changes->issues |= CBM_CHANGES_PATCH_FLAG_MISMATCH;
        }
    }
    for (size_t i = 0; i < changes->count; i++) {
        if (!matches[i].count) {
            changes->issues |= CBM_CHANGES_PATCH_MISSING_PATH;
        }
    }
    if (changes->count == 0 && changes->patch_length != 0) {
        changes->issues |= CBM_CHANGES_PATCH_WITHOUT_NAMES;
    }
    changes_audit_headers(changes);
    changes->reconciled = changes->issues == 0;
    changes->can_narrow = changes->reconciled;
    for (size_t i = 0; i < changes->count; i++) {
        cbm_change_file_t *file = &changes->files[i];
        if (file->status == 'T') {
            file->reasons |= CBM_CHANGE_TYPE;
        }
        if (matches[i].count == 1) {
            if (matches[i].file->binary) {
                file->reasons |= CBM_CHANGE_BINARY;
            }
            if (!changes_hunks_usable(matches[i].file)) {
                file->reasons |= CBM_CHANGE_NO_HUNKS;
            }
        }
        if (!changes->reconciled) {
            file->reasons |= CBM_CHANGE_UNRECONCILED;
        }
        if (file->reasons) {
            file->evidence = CBM_CHANGE_WHOLE_FILE;
            changes->can_narrow = false;
        } else {
            file->evidence = CBM_CHANGE_HUNKS;
            file->patch_file = matches[i].file;
        }
    }
    changes->state =
        changes->count ? CBM_CHANGES_NONEMPTY
                       : (changes->patch_length == 0 && changes->reconciled ? CBM_CHANGES_EMPTY
                                                                            : CBM_CHANGES_UNKNOWN);
}

cbm_changes_status_t cbm_changes_parse(const unsigned char *name_status, size_t name_status_length,
                                       const unsigned char *patch, size_t patch_length,
                                       cbm_changes_t **out) {
    if (out) {
        *out = NULL;
    }
    if (!out || (!name_status && name_status_length) || (!patch && patch_length) ||
        name_status_length > PTRDIFF_MAX || patch_length > PTRDIFF_MAX) {
        return CBM_CHANGES_INVALID;
    }
    size_t count = 0;
    size_t offset = 0;
    while (offset < name_status_length) {
        char status;
        changes_slice_t path;
        if (!changes_record(name_status, name_status_length, &offset, &status, &path)) {
            return CBM_CHANGES_INVALID;
        }
        count++;
    }
    if (count > SIZE_MAX / sizeof(cbm_change_file_t) ||
        count > SIZE_MAX / sizeof(changes_match_t)) {
        return CBM_CHANGES_OOM;
    }
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_changes_t *changes = cbm_arena_calloc(&arena, sizeof(*changes));
    if (!changes) {
        cbm_arena_destroy(&arena);
        return CBM_CHANGES_OOM;
    }
    changes->arena = arena;
    changes->name_status = cbm_arena_alloc(&changes->arena, name_status_length + 1);
    changes->patch = cbm_arena_alloc(&changes->arena, patch_length + 1);
    if (!changes->name_status || !changes->patch) {
        goto oom;
    }
    if (name_status_length) {
        memcpy(changes->name_status, name_status, name_status_length);
    }
    if (patch_length) {
        memcpy(changes->patch, patch, patch_length);
    }
    changes->name_status[name_status_length] = 0;
    changes->patch[patch_length] = 0;
    changes->name_status_length = name_status_length;
    changes->patch_length = patch_length;
    changes->count = count;
    changes_match_t *matches = NULL;
    if (count) {
        changes->files = cbm_arena_calloc(&changes->arena, count * sizeof(*changes->files));
        cbm_change_file_t *scratch = cbm_arena_alloc(&changes->arena, count * sizeof(*scratch));
        matches = cbm_arena_calloc(&changes->arena, count * sizeof(*matches));
        if (!changes->files || !scratch || !matches) {
            goto oom;
        }
        offset = 0;
        for (size_t i = 0; i < count; i++) {
            changes_slice_t path;
            char status;
            if (!changes_record(changes->name_status, name_status_length, &offset, &status,
                                &path)) {
                cbm_changes_free(changes);
                return CBM_CHANGES_INVALID;
            }
            changes->files[i].path = path.bytes;
            changes->files[i].path_length = path.length;
            changes->files[i].status = status;
        }
        changes_sort(changes->files, scratch, count);
        for (size_t i = 1; i < count; i++) {
            if (changes_path_compare(changes->files[i - 1].path, changes->files[i - 1].path_length,
                                     changes->files[i].path, changes->files[i].path_length) == 0) {
                cbm_changes_free(changes);
                return CBM_CHANGES_INVALID;
            }
        }
    }
    /* Raw NUL is not a representable text patch. Avoid passing it to a legacy
     * C-string path consumer; retain the exact bytes and conservative inventory. */
    bool binary_text = patch_length && memchr(changes->patch, 0, patch_length);
    changes->diff = cbm_diff_parse(binary_text ? "" : (const char *)changes->patch,
                                   binary_text ? 0 : patch_length);
    if (!changes->diff) {
        goto oom;
    }
    if (binary_text) {
        changes->issues |= CBM_CHANGES_PATCH_INCOMPLETE;
    }
    changes_reconcile(changes, matches);
    *out = changes;
    return CBM_CHANGES_OK;

oom:
    cbm_changes_free(changes);
    return CBM_CHANGES_OOM;
}

void cbm_changes_free(cbm_changes_t *changes) {
    if (!changes) {
        return;
    }
    cbm_diff_free(changes->diff);
    CBMArena arena = changes->arena;
    cbm_arena_destroy(&arena);
}

const cbm_change_file_t *cbm_changes_files(const cbm_changes_t *changes, size_t *count) {
    if (count) {
        *count = changes ? changes->count : 0;
    }
    return changes ? changes->files : NULL;
}

cbm_changes_state_t cbm_changes_state(const cbm_changes_t *changes) {
    return changes ? changes->state : CBM_CHANGES_UNKNOWN;
}

bool cbm_changes_patch_reconciled(const cbm_changes_t *changes) {
    return changes && changes->reconciled;
}

bool cbm_changes_can_narrow(const cbm_changes_t *changes) {
    return changes && changes->can_narrow;
}

unsigned cbm_changes_issues(const cbm_changes_t *changes) {
    return changes ? changes->issues : 0;
}
