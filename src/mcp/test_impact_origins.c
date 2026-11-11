/* Pure authenticated-evidence checker. The adapter supplies trust; this module
 * validates exact bindings and joins whole-file origin claims, never proofs. */
#include "mcp/test_impact_origins.h"

#include "foundation/arena.h"
#include "foundation/sha256.h"
#include <limits.h>
#include <string.h>

enum { OR_BYTE_CHUNK = 65536, OR_ELEMENT_CHUNK = 4096 };

struct cbm_coverage_origin_join {
    CBMArena arena;
    cbm_coverage_origin_binding_t binding;
    cbm_coverage_origin_path_t *paths;
    size_t path_count;
    int *ids;
    size_t id_count;
    unsigned reasons;
    bool broad;
    cbm_changes_state_t request_state;
};

typedef struct {
    CBMArena arena;
    cbm_coverage_origin_limits_t limits;
    cbm_coverage_origin_cancel_fn cancelled;
    void *cancel_context;
    cbm_coverage_origin_status_t status;
    uint64_t items;
    size_t allocated;
    unsigned operations;
} or_work_t;

typedef struct {
    or_work_t *work;
    cbm_coverage_origin_bytes_t bytes;
    size_t position;
} or_reader_t;

typedef struct {
    cbm_coverage_origin_bytes_t path;
    unsigned state;
    cbm_coverage_origin_disposition_t disposition;
    unsigned edits;
    bool proof;
    int *ids;
    size_t id_count;
} or_claim_t;

typedef struct {
    cbm_coverage_origin_bytes_t path;
    char status;
} or_change_t;

static bool or_fail(or_work_t *work, cbm_coverage_origin_status_t status) {
    if (work->status == CBM_COVERAGE_ORIGIN_OK) {
        work->status = status;
    }
    return false;
}

static bool or_poll(or_work_t *work) {
    if (work->status != CBM_COVERAGE_ORIGIN_OK) {
        return false;
    }
    if (work->cancelled && work->cancelled(work->cancel_context)) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_CANCELLED);
    }
    return true;
}

static bool or_step(or_work_t *work) {
    if (work->status != CBM_COVERAGE_ORIGIN_OK) {
        return false;
    }
    if (++work->operations == OR_ELEMENT_CHUNK) {
        work->operations = 0;
        return or_poll(work);
    }
    return true;
}

static bool or_items(or_work_t *work, uint64_t count) {
    if (count > work->limits.max_items - work->items) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_LIMIT);
    }
    work->items += count;
    return or_step(work);
}

static void *or_array(or_work_t *work, size_t count, size_t element_size) {
    if (count == 0) {
        return NULL;
    }
    if (element_size == 0 || count > SIZE_MAX / element_size) {
        or_fail(work, CBM_COVERAGE_ORIGIN_LIMIT);
        return NULL;
    }
    size_t bytes = count * element_size;
    /* The arena rounds requests to its eight-byte alignment. */
    if (bytes > SIZE_MAX - 7 || bytes > work->limits.max_alloc_bytes - work->allocated) {
        or_fail(work, CBM_COVERAGE_ORIGIN_LIMIT);
        return NULL;
    }
    if (!or_poll(work)) {
        return NULL;
    }
    work->allocated += bytes;
    void *memory = cbm_arena_alloc(&work->arena, bytes);
    if (!memory) {
        or_fail(work, CBM_COVERAGE_ORIGIN_OOM);
        return NULL;
    }
    return or_poll(work) ? memory : NULL;
}

static bool or_copy(or_work_t *work, unsigned char *out, const unsigned char *input,
                    size_t length) {
    for (size_t offset = 0; offset < length;) {
        size_t count = length - offset;
        if (count > OR_BYTE_CHUNK) {
            count = OR_BYTE_CHUNK;
        }
        if (!or_poll(work)) {
            return false;
        }
        memcpy(out + offset, input + offset, count);
        offset += count;
        if (!or_poll(work)) {
            return false;
        }
    }
    return true;
}

static bool or_own(or_work_t *work, cbm_coverage_origin_bytes_t input,
                   cbm_coverage_origin_bytes_t *out) {
    *out = (cbm_coverage_origin_bytes_t){0};
    if (!input.length) {
        return true;
    }
    unsigned char *copy = or_array(work, input.length, 1);
    if (!copy || !or_copy(work, copy, input.data, input.length)) {
        return false;
    }
    *out = (cbm_coverage_origin_bytes_t){copy, input.length};
    return true;
}

static bool or_compare(or_work_t *work, cbm_coverage_origin_bytes_t a,
                       cbm_coverage_origin_bytes_t b, int *order) {
    if (!or_step(work)) {
        return false;
    }
    size_t length = a.length < b.length ? a.length : b.length;
    for (size_t offset = 0; offset < length;) {
        size_t count = length - offset;
        if (count > OR_BYTE_CHUNK) {
            count = OR_BYTE_CHUNK;
        }
        if (!or_poll(work)) {
            return false;
        }
        int compared = memcmp(a.data + offset, b.data + offset, count);
        if (!or_poll(work)) {
            return false;
        }
        if (compared) {
            *order = compared < 0 ? -1 : 1;
            return true;
        }
        offset += count;
    }
    *order = a.length < b.length ? -1 : a.length > b.length ? 1 : 0;
    return true;
}

static bool or_hash_matches(or_work_t *work, cbm_coverage_origin_bytes_t bytes,
                            const unsigned char expected[32]) {
    if (!or_poll(work)) {
        return false;
    }
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    for (size_t offset = 0; offset < bytes.length;) {
        size_t count = bytes.length - offset;
        if (count > OR_BYTE_CHUNK) {
            count = OR_BYTE_CHUNK;
        }
        if (!or_poll(work)) {
            return false;
        }
        cbm_sha256_update(&hash, bytes.data + offset, count);
        offset += count;
        if (!or_poll(work)) {
            return false;
        }
    }
    unsigned char digest[32];
    cbm_sha256_final(&hash, digest);
    if (!or_poll(work)) {
        return false;
    }
    return memcmp(digest, expected, 32) == 0 || or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
}

static bool or_take(or_reader_t *reader, size_t count, const unsigned char **out) {
    if (!or_step(reader->work)) {
        return false;
    }
    if (count > reader->bytes.length - reader->position) {
        return or_fail(reader->work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    *out = reader->bytes.data + reader->position;
    reader->position += count;
    return true;
}

static bool or_u8(or_reader_t *reader, unsigned *out) {
    const unsigned char *p;
    if (!or_take(reader, 1, &p)) {
        return false;
    }
    *out = p[0];
    return true;
}

static bool or_u32(or_reader_t *reader, uint32_t *out) {
    const unsigned char *p;
    if (!or_take(reader, 4, &p)) {
        return false;
    }
    *out = (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 | (uint32_t)p[2] << 8 | p[3];
    return true;
}

static bool or_bytes(or_reader_t *reader, cbm_coverage_origin_bytes_t *out) {
    uint32_t length;
    const unsigned char *p;
    if (!or_u32(reader, &length) || !or_take(reader, length, &p)) {
        return false;
    }
    *out = (cbm_coverage_origin_bytes_t){p, length};
    return true;
}

static bool or_proof(or_reader_t *reader, bool *out) {
    unsigned present;
    const unsigned char *ignored;
    if (!or_u8(reader, &present)) {
        return false;
    }
    if (present > 1) {
        return or_fail(reader->work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    *out = present != 0;
    return !present || or_take(reader, 32, &ignored);
}

static bool or_path(or_work_t *work, cbm_coverage_origin_bytes_t path) {
    if (!path.length || path.data[0] == '/' || path.data[path.length - 1] == '/') {
        return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    size_t component = 0;
    for (size_t offset = 0; offset < path.length;) {
        size_t count = path.length - offset;
        if (count > OR_BYTE_CHUNK) {
            count = OR_BYTE_CHUNK;
        }
        if (!or_poll(work)) {
            return false;
        }
        size_t end = offset + count;
        for (; offset < end; offset++) {
            if (!path.data[offset]) {
                return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
            }
            if (path.data[offset] == '/') {
                size_t length = offset - component;
                if (!length ||
                    (path.data[component] == '.' &&
                     (length == 1 || (length == 2 && path.data[component + 1] == '.')))) {
                    return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
                }
                component = offset + 1;
            }
        }
        if (!or_poll(work)) {
            return false;
        }
    }
    size_t length = path.length - component;
    if (path.data[component] == '.' &&
        (length == 1 || (length == 2 && path.data[component + 1] == '.'))) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    return true;
}

static unsigned or_edit(char status) {
    switch (status) {
    case 'A':
        return CBM_COVERAGE_ORIGIN_EDIT_A;
    case 'M':
        return CBM_COVERAGE_ORIGIN_EDIT_M;
    case 'D':
        return CBM_COVERAGE_ORIGIN_EDIT_D;
    case 'T':
        return CBM_COVERAGE_ORIGIN_EDIT_T;
    default:
        return 0;
    }
}

static bool or_manifest(or_work_t *work, cbm_coverage_origin_bytes_t bytes,
                        const cbm_coverage_origin_binding_t *binding, int function_count,
                        or_claim_t **claims, size_t *claim_count, unsigned *reasons) {
    or_reader_t r = {.work = work, .bytes = bytes};
    static const unsigned char magic[] = "cbm-coverage-origins";
    const unsigned char *p;
    uint32_t version, count;
    unsigned format, closed;
    bool proof;
    cbm_coverage_origin_bytes_t key;
    int order;
    if (!or_take(&r, sizeof(magic), &p)) {
        return false;
    }
    if (memcmp(p, magic, sizeof(magic)) != 0) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    if (!or_u32(&r, &version)) {
        return false;
    }
    if (version != CBM_COVERAGE_ORIGIN_FORMAT_VERSION) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    if (!or_bytes(&r, &key) || !or_u8(&r, &format)) {
        return false;
    }
    if (!key.length || (format != 1 && format != 2)) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    if (!or_compare(work, key, binding->repository_key, &order)) {
        return false;
    }
    if (order || format != (unsigned)binding->object_format) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    size_t oid_size = format == 1 ? 20 : 32;
    if (!or_take(&r, oid_size, &p)) {
        return false;
    }
    if (memcmp(p, binding->artifact.bytes, oid_size) != 0) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    if (!or_take(&r, 32, &p)) {
        return false;
    }
    if (memcmp(p, binding->functions_sha256, 32) != 0) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    if (!or_u32(&r, &count)) {
        return false;
    }
    if (count > INT_MAX) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    if (count != (uint32_t)function_count) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    if (!or_take(&r, 32, &p)) {
        return false;
    }
    if (memcmp(p, binding->compatibility_sha256, 32) != 0) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    if (!or_take(&r, 32, &p)) {
        return false;
    }
    if (memcmp(p, binding->producer_profile_sha256, 32) != 0) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    if (!or_u8(&r, &closed)) {
        return false;
    }
    if (closed > 1) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    if (!or_proof(&r, &proof) || !or_u32(&r, &count)) {
        return false;
    }
    *reasons = closed && proof ? 0 : CBM_COVERAGE_ORIGIN_PROFILE_UNIVERSE_UNKNOWN;
    /* Even an ALL row needs at least nine bytes, plus the final terminator. */
    size_t remaining = r.bytes.length - r.position;
    if (!remaining || (size_t)count > (remaining - 1) / 9) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    if (!or_items(work, count)) {
        return false;
    }
    or_claim_t *rows = or_array(work, count, sizeof(*rows));
    if (count && !rows) {
        return false;
    }
    for (size_t i = 0; i < count; i++) {
        or_claim_t row = {0};
        unsigned disposition;
        if (!or_bytes(&r, &row.path) || !or_path(work, row.path)) {
            return false;
        }
        if (i) {
            if (!or_compare(work, rows[i - 1].path, row.path, &order)) {
                return false;
            }
            if (order >= 0) {
                return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
            }
        }
        if (!or_u8(&r, &row.state)) {
            return false;
        }
        if (row.state > 3) {
            return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
        }
        if (row.state >= 2) {
            uint32_t mode;
            if (!or_u32(&r, &mode) || !or_take(&r, oid_size, &p)) {
                return false;
            }
            if ((row.state == 2 && mode != 0100644 && mode != 0100755 && mode != 0120000) ||
                (row.state == 3 && mode != 0160000)) {
                return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
            }
        }
        if (!or_u8(&r, &disposition) || !or_u8(&r, &row.edits)) {
            return false;
        }
        if (disposition > CBM_COVERAGE_ORIGIN_ALL || (row.edits & ~15u)) {
            return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
        }
        row.disposition = (cbm_coverage_origin_disposition_t)disposition;
        if (!or_proof(&r, &row.proof)) {
            return false;
        }
        if (row.disposition != CBM_COVERAGE_ORIGIN_ALL) {
            uint32_t members;
            if (!or_u32(&r, &members)) {
                return false;
            }
            if (members > (uint32_t)function_count ||
                (size_t)members > (r.bytes.length - r.position) / 4) {
                return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
            }
            if (!or_items(work, members)) {
                return false;
            }
            row.ids = or_array(work, members, sizeof(*row.ids));
            if (members && !row.ids) {
                return false;
            }
            int previous = -1;
            for (size_t j = 0; j < members; j++) {
                uint32_t id;
                if (!or_u32(&r, &id)) {
                    return false;
                }
                if (id >= (uint32_t)function_count || (j && id <= (uint32_t)previous)) {
                    return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
                }
                row.ids[j] = (int)id;
                previous = (int)id;
            }
            row.id_count = members;
        }
        rows[i] = row;
    }
    unsigned end;
    if (!or_u8(&r, &end)) {
        return false;
    }
    if (end != 0x7f || r.position != r.bytes.length) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    *claims = rows;
    *claim_count = count;
    return true;
}

static bool or_next_change(or_work_t *work, cbm_coverage_origin_bytes_t bytes, size_t *position,
                           or_change_t *out, bool validate_path) {
    size_t start = *position;
    if (!or_step(work)) {
        return false;
    }
    if (bytes.length - start < 4 || !or_edit((char)bytes.data[start]) || bytes.data[start + 1]) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    size_t path_start = start + 2;
    size_t end = path_start;
    bool found = false;
    while (end < bytes.length) {
        size_t count = bytes.length - end;
        if (count > OR_BYTE_CHUNK) {
            count = OR_BYTE_CHUNK;
        }
        if (!or_poll(work)) {
            return false;
        }
        const unsigned char *zero = memchr(bytes.data + end, 0, count);
        if (!or_poll(work)) {
            return false;
        }
        if (zero) {
            end = (size_t)(zero - bytes.data);
            found = true;
            break;
        }
        end += count;
    }
    if (!found) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
    }
    *out = (or_change_t){.path = {bytes.data + path_start, end - path_start},
                         .status = (char)bytes.data[start]};
    if (validate_path && !or_path(work, out->path)) {
        return false;
    }
    *position = end + 1;
    return true;
}

static bool or_change_sift(or_work_t *work, or_change_t *rows, size_t count, size_t root) {
    while (root < count / 2) {
        size_t child = root * 2 + 1;
        int order;
        if (child + 1 < count) {
            if (!or_compare(work, rows[child].path, rows[child + 1].path, &order)) {
                return false;
            }
            if (order < 0) {
                child++;
            }
        }
        if (!or_compare(work, rows[root].path, rows[child].path, &order)) {
            return false;
        }
        if (order >= 0) {
            break;
        }
        or_change_t saved = rows[root];
        rows[root] = rows[child];
        rows[child] = saved;
        root = child;
    }
    return true;
}

static bool or_changes(or_work_t *work, cbm_coverage_origin_bytes_t bytes, or_change_t **changes,
                       size_t *change_count) {
    size_t count = 0, position = 0;
    or_change_t ignored;
    while (position < bytes.length) {
        if (!or_items(work, 1) || !or_next_change(work, bytes, &position, &ignored, true)) {
            return false;
        }
        if (count == SIZE_MAX) {
            return or_fail(work, CBM_COVERAGE_ORIGIN_LIMIT);
        }
        count++;
    }
    or_change_t *rows = or_array(work, count, sizeof(*rows));
    if (count && !rows) {
        return false;
    }
    position = 0;
    for (size_t i = 0; i < count; i++) {
        if (!or_next_change(work, bytes, &position, &rows[i], false)) {
            return false;
        }
    }
    for (size_t i = count / 2; i > 0; i--) {
        if (!or_change_sift(work, rows, count, i - 1)) {
            return false;
        }
    }
    for (size_t i = count; i > 1; i--) {
        or_change_t saved = rows[0];
        rows[0] = rows[i - 1];
        rows[i - 1] = saved;
        if (!or_step(work) || !or_change_sift(work, rows, i - 1, 0)) {
            return false;
        }
    }
    for (size_t i = 1; i < count; i++) {
        int order;
        if (!or_compare(work, rows[i - 1].path, rows[i].path, &order)) {
            return false;
        }
        if (order == 0) {
            return or_fail(work, CBM_COVERAGE_ORIGIN_FORMAT);
        }
    }
    *changes = rows;
    *change_count = count;
    return true;
}

static bool or_reconcile(or_work_t *work, const cbm_changes_t *request,
                         const cbm_coverage_origin_binding_t *binding, const or_change_t *ah,
                         size_t ah_count, const or_change_t *mh, size_t mh_count,
                         cbm_changes_state_t *state) {
    size_t count;
    const cbm_change_file_t *files = cbm_changes_files(request, &count);
    if (count != mh_count || (count && !files)) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    *state = cbm_changes_state(request);
    if ((*state == CBM_CHANGES_EMPTY && count) || (*state == CBM_CHANGES_NONEMPTY && !count) ||
        (*state != CBM_CHANGES_EMPTY && *state != CBM_CHANGES_NONEMPTY &&
         *state != CBM_CHANGES_UNKNOWN)) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    for (size_t i = 0; i < count; i++) {
        int order;
        if (files[i].status != mh[i].status || files[i].path_length != mh[i].path.length ||
            !files[i].path) {
            return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
        }
        cbm_coverage_origin_bytes_t path = {files[i].path, files[i].path_length};
        if (!or_compare(work, path, mh[i].path, &order)) {
            return false;
        }
        if (order) {
            return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
        }
    }
    if (memcmp(binding->merge_base.bytes, binding->head.bytes, 32) == 0 && mh_count) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    if (memcmp(binding->artifact.bytes, binding->head.bytes, 32) == 0 && (ah_count || mh_count)) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    if (memcmp(binding->artifact.bytes, binding->merge_base.bytes, 32) == 0) {
        if (ah_count != mh_count) {
            return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
        }
        for (size_t i = 0; i < ah_count; i++) {
            int order;
            if (ah[i].status != mh[i].status) {
                return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
            }
            if (!or_compare(work, ah[i].path, mh[i].path, &order)) {
                return false;
            }
            if (order) {
                return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
            }
        }
    }
    return true;
}

static bool or_read_id(or_work_t *work, const int *ids, size_t index, int *id) {
    if (!or_items(work, 1)) {
        return false;
    }
    *id = ids[index];
    return true;
}

static bool or_id_sift(or_work_t *work, int *ids, size_t count, size_t root) {
    while (root < count / 2) {
        size_t child = root * 2 + 1;
        int child_id, root_id;
        if (!or_read_id(work, ids, child, &child_id)) {
            return false;
        }
        if (child + 1 < count) {
            int other;
            if (!or_read_id(work, ids, child + 1, &other)) {
                return false;
            }
            if (other > child_id) {
                child++;
                child_id = other;
            }
        }
        if (!or_read_id(work, ids, root, &root_id)) {
            return false;
        }
        if (root_id >= child_id) {
            break;
        }
        ids[root] = child_id;
        ids[child] = root_id;
        root = child;
    }
    return true;
}

static bool or_union_ids(or_work_t *work, cbm_coverage_origin_join_t *result) {
    if (result->broad) {
        return true; /* The shared ALL array already is the complete union. */
    }
    size_t total = 0;
    for (size_t i = 0; i < result->path_count; i++) {
        if (!or_step(work)) {
            return false;
        }
        if (result->paths[i].function_count > work->limits.max_result_ids) {
            return or_fail(work, CBM_COVERAGE_ORIGIN_LIMIT);
        }
        if (result->paths[i].function_count > SIZE_MAX - total) {
            return or_fail(work, CBM_COVERAGE_ORIGIN_LIMIT);
        }
        total += result->paths[i].function_count;
    }
    /* Reserve the known copy visits before allocating the temporary ID bag. */
    if (!or_items(work, (uint64_t)total)) {
        return false;
    }
    int *ids = or_array(work, total, sizeof(*ids));
    if (total && !ids) {
        return false;
    }
    size_t used = 0;
    for (size_t i = 0; i < result->path_count; i++) {
        if (!or_step(work)) {
            return false;
        }
        for (size_t j = 0; j < result->paths[i].function_count; j++) {
            if (!or_step(work)) {
                return false;
            }
            ids[used++] = result->paths[i].function_ids[j];
        }
    }
    for (size_t i = total / 2; i > 0; i--) {
        if (!or_id_sift(work, ids, total, i - 1)) {
            return false;
        }
    }
    for (size_t i = total; i > 1; i--) {
        int first, last;
        if (!or_read_id(work, ids, 0, &first) || !or_read_id(work, ids, i - 1, &last)) {
            return false;
        }
        ids[0] = last;
        ids[i - 1] = first;
        if (!or_id_sift(work, ids, i - 1, 0)) {
            return false;
        }
    }
    size_t unique = 0;
    int previous = -1;
    for (size_t i = 0; i < total; i++) {
        int id;
        if (!or_read_id(work, ids, i, &id)) {
            return false;
        }
        if (!unique || id != previous) {
            if (unique == work->limits.max_result_ids) {
                return or_fail(work, CBM_COVERAGE_ORIGIN_LIMIT);
            }
            ids[unique++] = id;
            previous = id;
        }
    }
    result->ids = ids;
    result->id_count = unique;
    return true;
}

static bool or_join_paths(or_work_t *work, cbm_coverage_origin_join_t *result,
                          const or_claim_t *claims, size_t claim_count, const or_change_t *ah,
                          size_t ah_count, const or_change_t *mh, size_t mh_count,
                          int function_count) {
    if (ah_count > SIZE_MAX - mh_count) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_LIMIT);
    }
    size_t capacity = ah_count + mh_count;
    result->paths = or_array(work, capacity, sizeof(*result->paths));
    if (capacity && !result->paths) {
        return false;
    }
    size_t ai = 0, mi = 0, ci = 0;
    while (ai < ah_count || mi < mh_count) {
        if (!or_step(work)) {
            return false;
        }
        int order = ai == ah_count ? 1 : mi == mh_count ? -1 : 0;
        if (ai < ah_count && mi < mh_count && !or_compare(work, ah[ai].path, mh[mi].path, &order)) {
            return false;
        }
        cbm_coverage_origin_path_t path = {0};
        if (order <= 0) {
            path.path = ah[ai].path;
            path.artifact_status = ah[ai++].status;
            path.comparisons |= CBM_COVERAGE_ORIGIN_FROM_ARTIFACT;
        }
        if (order >= 0) {
            path.path = mh[mi].path;
            path.merge_base_status = mh[mi++].status;
            path.comparisons |= CBM_COVERAGE_ORIGIN_FROM_MERGE_BASE;
        }
        if (path.artifact_status && path.merge_base_status &&
            ((path.artifact_status == 'D') != (path.merge_base_status == 'D'))) {
            return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
        }
        int claim_order = -1;
        while (ci < claim_count) {
            if (!or_compare(work, claims[ci].path, path.path, &claim_order)) {
                return false;
            }
            if (claim_order >= 0) {
                break;
            }
            ci++;
        }
        if (ci == claim_count || claim_order != 0) {
            path.reasons = CBM_COVERAGE_ORIGIN_PATH_MISSING;
        } else {
            const or_claim_t *claim = &claims[ci];
            path.disposition = claim->disposition;
            path.supported_edits = claim->edits;
            if (!claim->state) {
                path.reasons |= CBM_COVERAGE_ORIGIN_PRESENCE_UNKNOWN;
            } else {
                bool present_at_a = claim->state >= 2;
                bool expected_present = path.artifact_status ? path.artifact_status != 'A'
                                                             : path.merge_base_status != 'D';
                if (present_at_a != expected_present) {
                    return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
                }
            }
            if (claim->disposition == CBM_COVERAGE_ORIGIN_UNKNOWN) {
                path.reasons |= CBM_COVERAGE_ORIGIN_CLAIM_UNKNOWN;
            } else if (!claim->proof) {
                path.reasons |= CBM_COVERAGE_ORIGIN_CLAIM_UNPROVEN;
            }
            unsigned required = or_edit(path.artifact_status) | or_edit(path.merge_base_status);
            if (required & ~claim->edits) {
                path.reasons |= CBM_COVERAGE_ORIGIN_EDIT_UNSUPPORTED;
            }
            if (claim->disposition == CBM_COVERAGE_ORIGIN_ALL) {
                if (!result->broad) {
                    if ((size_t)function_count > work->limits.max_result_ids) {
                        return or_fail(work, CBM_COVERAGE_ORIGIN_LIMIT);
                    }
                    if (!or_items(work, (uint64_t)function_count)) {
                        return false;
                    }
                    result->ids = or_array(work, (size_t)function_count, sizeof(*result->ids));
                    if (function_count && !result->ids) {
                        return false;
                    }
                    for (int i = 0; i < function_count; i++) {
                        if (!or_step(work)) {
                            return false;
                        }
                        result->ids[i] = i;
                    }
                    result->id_count = (size_t)function_count;
                }
                result->broad = true;
                path.function_ids = result->ids;
                path.function_count = result->id_count;
            } else {
                path.function_ids = claim->ids;
                path.function_count = claim->id_count;
            }
        }
        result->reasons |= path.reasons;
        result->paths[result->path_count++] = path;
    }
    return or_union_ids(work, result);
}

static int or_hex_digit(unsigned char ch) {
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    return ch >= 'a' && ch <= 'f' ? ch - 'a' + 10 : -1;
}

static bool or_map(or_work_t *work, const cbm_coverage_map_t *map, const unsigned char expected[32],
                   int *count) {
    cbm_coverage_format_t format = cbm_coverage_map_format(map);
    if (format != CBM_COVERAGE_FORMAT_FUNCTIONS && format != CBM_COVERAGE_FORMAT_PROFILES) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    const char *hex = cbm_coverage_map_identity_sha256(map);
    if (!hex) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    for (size_t i = 0; i < 32; i++) {
        int high = or_hex_digit((unsigned char)hex[i * 2]);
        int low = or_hex_digit((unsigned char)hex[i * 2 + 1]);
        if (high < 0 || low < 0 || (unsigned char)((high << 4) | low) != expected[i]) {
            return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
        }
    }
    if (hex[64]) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    *count = cbm_coverage_map_id_count(map);
    if (*count < 0) {
        return or_fail(work, CBM_COVERAGE_ORIGIN_BINDING);
    }
    if (!or_items(work, (uint64_t)*count)) {
        return false;
    }
    /* Opaque map constructors guarantee dense IDs. Retain the existing
     * per-ID budget/cancellation visits without projecting identity rows. */
    for (int i = 0; i < *count; i++) {
        if (!or_step(work)) {
            return false;
        }
    }
    return true;
}

static bool or_span_valid(cbm_coverage_origin_bytes_t span) {
    return span.data || !span.length;
}

cbm_coverage_origin_status_t cbm_coverage_origin_join(const cbm_coverage_origin_input_t *input,
                                                      const cbm_coverage_origin_limits_t *limits,
                                                      cbm_coverage_origin_cancel_fn cancelled,
                                                      void *cancel_context,
                                                      cbm_coverage_origin_join_t **out) {
    if (out) {
        *out = NULL;
    }
    if (!out || !input || !limits || !input->coverage || !input->request_changes ||
        !input->context || !or_span_valid(input->manifest) || !input->manifest.length ||
        !or_span_valid(input->artifact_to_head) || !or_span_valid(input->merge_base_to_head) ||
        !limits->max_input_bytes || limits->max_input_bytes > UINT64_MAX / 8 ||
        !limits->max_items || !limits->max_alloc_bytes || !limits->max_result_ids ||
        limits->max_result_ids > INT_MAX) {
        return CBM_COVERAGE_ORIGIN_INVALID;
    }
    const cbm_coverage_origin_context_t *context = input->context;
    const cbm_coverage_origin_binding_t *binding = &context->binding;
    if (!or_span_valid(binding->repository_key) || !binding->repository_key.length ||
        binding->repository_key.length > UINT32_MAX ||
        (binding->object_format != CBM_COVERAGE_ORIGIN_GIT_SHA1 &&
         binding->object_format != CBM_COVERAGE_ORIGIN_GIT_SHA256) ||
        binding->manifest_version != CBM_COVERAGE_ORIGIN_FORMAT_VERSION) {
        return CBM_COVERAGE_ORIGIN_INVALID;
    }
    if (binding->object_format == CBM_COVERAGE_ORIGIN_GIT_SHA1) {
        static const unsigned char zero[12] = {0};
        if (memcmp(binding->artifact.bytes + 20, zero, 12) ||
            memcmp(binding->merge_base.bytes + 20, zero, 12) ||
            memcmp(binding->head.bytes + 20, zero, 12)) {
            return CBM_COVERAGE_ORIGIN_INVALID;
        }
    }
    if (!context->artifact_admitted || !context->origin_source_verified ||
        !context->origin_attestations_verified || !context->comparisons_verified) {
        return CBM_COVERAGE_ORIGIN_UNVERIFIED;
    }
    const size_t lengths[] = {input->manifest.length, input->artifact_to_head.length,
                              input->merge_base_to_head.length, binding->repository_key.length};
    uint64_t total = 0;
    for (size_t i = 0; i < sizeof(lengths) / sizeof(lengths[0]); i++) {
        if ((uint64_t)lengths[i] != lengths[i] ||
            (uint64_t)lengths[i] > limits->max_input_bytes - total) {
            return CBM_COVERAGE_ORIGIN_LIMIT;
        }
        total += (uint64_t)lengths[i];
    }
    or_work_t work = {.limits = *limits,
                      .cancelled = cancelled,
                      .cancel_context = cancel_context,
                      .status = CBM_COVERAGE_ORIGIN_OK};
    cbm_arena_init_lazy(&work.arena, 1024);
    cbm_coverage_origin_join_t *result;
    cbm_coverage_origin_bytes_t manifest, ah_bytes, mh_bytes;
    or_claim_t *claims;
    or_change_t *ah, *mh;
    size_t claim_count, ah_count, mh_count;
    unsigned reasons;
    int function_count;
    if (!or_poll(&work) || !or_hash_matches(&work, input->manifest, binding->manifest_sha256) ||
        !or_hash_matches(&work, input->artifact_to_head, context->artifact_to_head_sha256) ||
        !or_hash_matches(&work, input->merge_base_to_head, context->merge_base_to_head_sha256) ||
        !or_map(&work, input->coverage, binding->functions_sha256, &function_count) ||
        !or_own(&work, input->manifest, &manifest) ||
        !or_own(&work, input->artifact_to_head, &ah_bytes) ||
        !or_own(&work, input->merge_base_to_head, &mh_bytes) ||
        !or_manifest(&work, manifest, binding, function_count, &claims, &claim_count, &reasons) ||
        !or_changes(&work, ah_bytes, &ah, &ah_count) ||
        !or_changes(&work, mh_bytes, &mh, &mh_count)) {
        goto failure;
    }
    result = or_array(&work, 1, sizeof(*result));
    if (!result) {
        goto failure;
    }
    /* Avoid a bulk arena calloc: all large byte operations are chunk-polled. */
    result->binding = *binding;
    result->paths = NULL;
    result->path_count = 0;
    result->ids = NULL;
    result->id_count = 0;
    result->reasons = reasons;
    result->broad = false;
    result->request_state = CBM_CHANGES_UNKNOWN;
    if (!or_own(&work, binding->repository_key, &result->binding.repository_key) ||
        !or_reconcile(&work, input->request_changes, binding, ah, ah_count, mh, mh_count,
                      &result->request_state)) {
        goto failure;
    }
    if (result->request_state == CBM_CHANGES_UNKNOWN) {
        result->reasons |= CBM_COVERAGE_ORIGIN_REQUEST_UNKNOWN;
    }
    if (!or_join_paths(&work, result, claims, claim_count, ah, ah_count, mh, mh_count,
                       function_count) ||
        !or_poll(&work)) {
        goto failure;
    }
    result->arena = work.arena;
    *out = result;
    return CBM_COVERAGE_ORIGIN_OK;

failure:
    cbm_arena_destroy(&work.arena);
    return work.status;
}

void cbm_coverage_origin_join_free(cbm_coverage_origin_join_t *join) {
    if (join) {
        CBMArena arena = join->arena;
        cbm_arena_destroy(&arena);
    }
}

const cbm_coverage_origin_binding_t *cbm_coverage_origin_join_binding(
    const cbm_coverage_origin_join_t *join) {
    return join ? &join->binding : NULL;
}

const int *cbm_coverage_origin_join_ids(const cbm_coverage_origin_join_t *join, int *count) {
    if (count) {
        *count = join ? (int)join->id_count : 0;
    }
    return join ? join->ids : NULL;
}

const cbm_coverage_origin_path_t *cbm_coverage_origin_join_paths(
    const cbm_coverage_origin_join_t *join, size_t *count) {
    if (count) {
        *count = join ? join->path_count : 0;
    }
    return join ? join->paths : NULL;
}

bool cbm_coverage_origin_join_complete(const cbm_coverage_origin_join_t *join) {
    return join && !join->reasons;
}

bool cbm_coverage_origin_join_broad_fallback_required(const cbm_coverage_origin_join_t *join) {
    return join && join->broad;
}

bool cbm_coverage_origin_join_can_narrow(const cbm_coverage_origin_join_t *join) {
    return cbm_coverage_origin_join_complete(join) &&
           !cbm_coverage_origin_join_broad_fallback_required(join);
}

unsigned cbm_coverage_origin_join_reasons(const cbm_coverage_origin_join_t *join) {
    return join ? join->reasons : CBM_COVERAGE_ORIGIN_REQUEST_UNKNOWN;
}

cbm_changes_state_t cbm_coverage_origin_join_request_state(const cbm_coverage_origin_join_t *join) {
    return join ? join->request_state : CBM_CHANGES_UNKNOWN;
}
