#include "discover/inventory_internal.h"
#include "foundation/sha256.h"
#include <string.h>

static bool cif_control_index(cif_context *c, size_t directory, const char *name,
                              size_t name_length, size_t *index) {
    cbm_inventory_path_t path = c->owner->directories[directory].path;
    size_t separator = path.length ? 1 : 0;
    *index = SIZE_MAX;
    /* A longer candidate cannot occur in this fully validated inventory. */
    if (path.length > CIF_PATH_MAX - name_length - separator) {
        return true;
    }
    unsigned char buffer[CIF_PATH_MAX + 1];
    if (!cif_copy(c, buffer, path.data, path.length) || !cif_bytes(c, 1)) {
        return false;
    }
    if (separator) {
        buffer[path.length] = '/';
    }
    if (!cif_copy(c, buffer + path.length + separator, name, name_length + 1)) {
        return false;
    }
    cbm_inventory_path_t candidate = {buffer, path.length + separator + name_length};
    if (!cif_find_file(c, candidate, index)) {
        return false;
    }
    if (*index == SIZE_MAX) {
        size_t collision = SIZE_MAX;
        if (!cif_find_directory(c, candidate, &collision)) {
            return false;
        }
        if (collision != SIZE_MAX) {
            return cif_fail(c, CBM_INVENTORY_UNSUPPORTED);
        }
    }
    return true;
}

static bool cif_reserve_control(cif_context *c, uint64_t length) {
    cbm_inventory_filter_t *o = c->owner;
    cbm_inventory_limits_t *l = &o->limits;
    cbm_inventory_filter_usage_t *u = &o->usage;
    if ((uint64_t)(size_t)length != length || length > l->max_control_file_bytes ||
        u->verified_file_reads_reserved >= l->max_verified_file_reads ||
        length > l->max_verified_content_bytes - u->verified_content_bytes_reserved ||
        length > l->max_control_total_bytes - u->control_bytes_reserved) {
        return cif_fail(c, CBM_INVENTORY_LIMIT);
    }
    u->verified_file_reads_reserved++;
    u->verified_content_bytes_reserved += length;
    u->control_bytes_reserved += length;
    return true;
}

static bool cif_read_control(cif_context *c, size_t index, size_t length) {
    cbm_inventory_filter_t *o = c->owner;
    if (length > o->scratch_capacity) {
        unsigned char *buffer = cif_alloc(c, length, 1);
        if (!buffer) {
            return false;
        }
        o->scratch = buffer;
        o->scratch_capacity = length;
    }
    if (!cif_poll(c)) {
        return false;
    }
    size_t copied = 0;
    cbm_inventory_error_t error = {.status = CBM_INVENTORY_OK, .file_index = SIZE_MAX};
    cbm_inventory_status_t status =
        c->source->read(c->source->read_context, index, length ? o->scratch : NULL, length, &copied,
                        c->control, &error);
    c->error.cleanup_required |= error.cleanup_required;
    if ((unsigned)status > (unsigned)CBM_INVENTORY_DEADLINE || error.status != status) {
        return cif_fail(c, CBM_INVENTORY_INVALID);
    }
    if (status != CBM_INVENTORY_OK) {
        return cif_fail(c, copied ? CBM_INVENTORY_INVALID : status);
    }
    if (copied != length || error.cleanup_required) {
        return cif_fail(c, CBM_INVENTORY_INVALID);
    }
    return cif_poll(c);
}

static bool cif_hash_control(cif_context *c, size_t index, size_t length) {
    cbm_sha256_ctx hash;
    unsigned char digest[CBM_SHA256_DIGEST_LEN];
    if (!cif_poll(c)) {
        return false;
    }
    cbm_sha256_init(&hash);
    for (size_t at = 0; at < length;) {
        size_t n = length - at < 512 ? length - at : 512;
        if (!cif_poll(c) || !cif_bytes(c, n)) {
            return false;
        }
        cbm_sha256_update(&hash, c->owner->scratch + at, n);
        if (!cif_poll(c)) {
            return false;
        }
        at += n;
    }
    cbm_sha256_final(&hash, digest);
    if (!cif_poll(c)) {
        return false;
    }
    if (memcmp(digest, c->owner->files[index].content_sha256, sizeof(digest))) {
        return cif_fail(c, CBM_INVENTORY_CHANGED);
    }
    return true;
}

static bool cif_load_control(cif_context *c, size_t directory, cbm_inventory_control_kind_t kind,
                             cbm_inventory_control_row_t *row,
                             const cbm_ignore_program_t **program) {
    cbm_inventory_filter_t *o = c->owner;
    c->file_index = SIZE_MAX;
    if (!cif_event(c) || !cif_bytes(c, 256)) {
        return false;
    }
    *row = (cbm_inventory_control_row_t){
        .kind = kind, .directory = o->directories[directory].path, .file_index = SIZE_MAX};
    const char *name = kind == CBM_INVENTORY_CONTROL_CBMIGNORE ? ".cbmignore" : ".gitignore";
    size_t index = SIZE_MAX;
    if (!cif_control_index(c, directory, name, 10, &index)) {
        return false;
    }
    if (index == SIZE_MAX) {
        return true;
    }
    c->file_index = index;
    uint64_t size = o->files[index].content_length;
    if (!cif_reserve_control(c, size)) {
        return false;
    }
    size_t length = (size_t)size;
    if (!cif_read_control(c, index, length) || !cif_hash_control(c, index, length) ||
        !cif_poll(c)) {
        return false;
    }
    cbm_ignore_checked_control_t control = cif_ignore_control(c);
    cbm_ignore_checked_parse_result_t parsed = {0};
    cbm_ignore_checked_status_t status = cbm_ignore_checked_parse(
        o->ignore, length ? o->scratch : NULL, length, &control, &parsed, NULL);
    if (!cif_checked(c, status)) {
        return false;
    }
    if (!parsed.program) {
        return cif_fail(c, CBM_INVENTORY_STATE);
    }
    *program = parsed.program;
    row->file_index = index;
    row->effective_patterns = parsed.pattern_count;
    row->outcome =
        parsed.pattern_count ? CBM_INVENTORY_CONTROL_APPLIED : CBM_INVENTORY_CONTROL_APPLIED_EMPTY;
    o->rows[index].roles |= kind == CBM_INVENTORY_CONTROL_CBMIGNORE
                                ? CBM_INVENTORY_FILTER_ROLE_CBMIGNORE
                                : CBM_INVENTORY_FILTER_ROLE_GITIGNORE;
    return cif_poll(c);
}

bool cif_load_controls(cif_context *c, size_t directory) {
    cbm_inventory_filter_t *o = c->owner;
    if (!directory && !cif_load_control(c, 0, CBM_INVENTORY_CONTROL_CBMIGNORE, &o->cbm_control,
                                        &o->cbm_program)) {
        return false;
    }
    cbm_inventory_control_row_t *row = &o->controls[o->view.control_count];
    if (!cif_load_control(c, directory, CBM_INVENTORY_CONTROL_GITIGNORE, row,
                          &o->directories[directory].program)) {
        return false;
    }
    o->view.control_count++;
    return true;
}
