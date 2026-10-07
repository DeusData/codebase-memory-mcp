#include "mcp/test_impact_inventory_internal.h"
#include <string.h>

static bool cni_oid(cni_context *c, const char *oid, size_t width) {
    if (!cni_bytes(c, width + 1)) {
        return false;
    }
    for (size_t i = 0; i < width; i++) {
        unsigned char ch = (unsigned char)oid[i];
        if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'))) {
            return cni_fail(c, CBM_INVENTORY_INVALID);
        }
    }
    return !oid[width] || cni_fail(c, CBM_INVENTORY_INVALID);
}

bool cni_binding(cni_context *c, const cbm_pinned_tree_view_t *view) {
    if (!view) {
        return cni_fail(c, CBM_INVENTORY_STATE);
    }
    if (view->revision != CBM_GIT_REV_HEAD) {
        bool supported_revision =
            view->revision == CBM_GIT_REV_BASE || view->revision == CBM_GIT_REV_MERGE_BASE;
        return cni_fail(c, supported_revision ? CBM_INVENTORY_UNSUPPORTED : CBM_INVENTORY_INVALID);
    }
    if (!view->identity || !view->commit || !view->root || (view->file_count && !view->files) ||
        !view->directory_count) {
        return cni_fail(c, CBM_INVENTORY_INVALID);
    }
    size_t width = view->identity->oid_hex_length;
    if (width != 40 && width != 64) {
        return cni_fail(c, CBM_INVENTORY_INVALID);
    }
    if (!cni_oid(c, view->identity->head, width) || !cni_oid(c, view->commit, width) ||
        !cni_bytes(c, width * 2)) {
        return false;
    }
    if (memcmp(view->identity->head, view->commit, width)) {
        return cni_fail(c, CBM_INVENTORY_CHANGED);
    }
    if (view->file_count > c->owner->limits.max_files ||
        view->directory_count > c->owner->limits.max_directories) {
        return cni_fail(c, CBM_INVENTORY_LIMIT);
    }
    return true;
}

static char *cni_root(cni_context *c, const char *root) {
    size_t length = 0;
    for (;;) {
        if (!cni_bytes(c, 1)) {
            return NULL;
        }
        if (!root[length]) {
            break;
        }
        if (length == CNI_PATH_MAX) {
            cni_fail(c, CBM_INVENTORY_LIMIT);
            return NULL;
        }
        length++;
    }
    if (!length) {
        cni_fail(c, CBM_INVENTORY_INVALID);
        return NULL;
    }
    char *copy = cni_alloc(c, &c->temporary, length + 1, 1);
    return copy && cni_copy(c, copy, root, length + 1) ? copy : NULL;
}

static bool cni_file(cni_context *c, const cbm_pinned_tree_file_t *input,
                     cbm_inventory_file_t *output, size_t oid_width) {
    if (!cni_event(c) || !cni_bytes(c, 2 * (sizeof(*input) + sizeof(*output)))) {
        return false;
    }
    size_t length = input->path_length;
    if (!input->path || !length) {
        return cni_fail(c, CBM_INVENTORY_INVALID);
    }
    if (length > CNI_PATH_MAX) {
        return cni_fail(c, CBM_INVENTORY_LIMIT);
    }
    if (!cni_oid(c, input->oid, oid_width)) {
        return false;
    }
    unsigned char *path = cni_alloc(c, &c->temporary, length + 1, 1);
    if (!path || !cni_copy(c, path, input->path, length + 1) ||
        !cni_copy(c, output->oid, input->oid, oid_width + 1) ||
        !cni_copy(c, output->content_sha256, input->content_sha256, 32)) {
        return false;
    }
    output->path = (cbm_inventory_path_t){path, length};
    output->git_mode = input->git_mode;
    output->content_length = input->content_length;
    return true;
}

bool cni_snapshot(cni_context *c, const cbm_pinned_tree_view_t *view,
                  cbm_inventory_source_t *source) {
    source->native_root = cni_root(c, view->root);
    if (!source->native_root || !cni_copy(c, source->manifest_sha256, view->manifest_sha256, 32)) {
        return false;
    }
    cbm_inventory_file_t *files =
        view->file_count ? cni_alloc(c, &c->temporary, view->file_count, sizeof(*files)) : NULL;
    if (view->file_count && !files) {
        return false;
    }
    source->files = files;
    source->file_count = view->file_count;
    for (size_t i = 0; i < view->file_count; i++) {
        c->file_index = i;
        if (!cni_file(c, &view->files[i], &files[i], view->identity->oid_hex_length)) {
            return false;
        }
    }
    c->file_index = SIZE_MAX;
    return true;
}

static cbm_inventory_status_t cni_native_status(cbm_pinned_tree_status_t status) {
    switch (status) {
    case CBM_PINNED_TREE_OK:
        return CBM_INVENTORY_OK;
    case CBM_PINNED_TREE_INVALID:
        return CBM_INVENTORY_INVALID;
    case CBM_PINNED_TREE_UNSUPPORTED:
        return CBM_INVENTORY_UNSUPPORTED;
    case CBM_PINNED_TREE_LIMIT:
        return CBM_INVENTORY_LIMIT;
    case CBM_PINNED_TREE_OOM:
        return CBM_INVENTORY_OOM;
    case CBM_PINNED_TREE_IO:
        return CBM_INVENTORY_IO;
    case CBM_PINNED_TREE_CANCELLED:
        return CBM_INVENTORY_CANCELLED;
    case CBM_PINNED_TREE_DEADLINE:
        return CBM_INVENTORY_DEADLINE;
    case CBM_PINNED_TREE_CHANGED:
        return CBM_INVENTORY_CHANGED;
    case CBM_PINNED_TREE_COLLISION:
    case CBM_PINNED_TREE_GIT:
    case CBM_PINNED_TREE_CLEANUP_REQUIRED:
        return CBM_INVENTORY_INVALID;
    }
    return CBM_INVENTORY_INVALID;
}

static bool cni_read_result(cni_context *c, cbm_pinned_tree_status_t status,
                            const cbm_pinned_tree_error_t *error, size_t copied, size_t expected) {
    c->error.cleanup_required |= status == CBM_PINNED_TREE_CLEANUP_REQUIRED ||
                                 error->status == CBM_PINNED_TREE_CLEANUP_REQUIRED;
    if (status != error->status) {
        return cni_fail(c, CBM_INVENTORY_INVALID);
    }
    if (status == CBM_PINNED_TREE_CLEANUP_REQUIRED) {
        cbm_inventory_status_t cause = cni_native_status(error->cause);
        return cni_fail(c, copied || cause == CBM_INVENTORY_OK ? CBM_INVENTORY_INVALID : cause);
    }
    if (error->cause != status) {
        return cni_fail(c, CBM_INVENTORY_INVALID);
    }
    if (status != CBM_PINNED_TREE_OK) {
        return cni_fail(c, copied ? CBM_INVENTORY_INVALID : cni_native_status(status));
    }
    return copied == expected || cni_fail(c, CBM_INVENTORY_INVALID);
}

cbm_inventory_status_t cni_read(void *context, size_t index, unsigned char *prefix, size_t capacity,
                                size_t *copied, const cbm_inventory_control_t *control,
                                cbm_inventory_error_t *error) {
    cni_reader *reader = context;
    cni_context *c = reader->context;
    c->file_index = index < reader->file_count ? index : SIZE_MAX;
    if (copied) {
        *copied = 0;
    }
    if (!copied || !control || !control->deadline_ms || index >= reader->file_count ||
        (capacity && !prefix)) {
        cni_fail(c, CBM_INVENTORY_INVALID);
    } else if (cni_poll(c)) {
        /* This length comes from the attachment, before the native view expires. */
        uint64_t length = reader->files[index].content_length;
        size_t expected = length < capacity ? (size_t)length : capacity;
        size_t native_copied = 0;
        cbm_pinned_tree_error_t native_error = {0};
        cbm_pinned_tree_control_t native_control = {.deadline_ms = control->deadline_ms,
                                                    .cancelled = control->cancelled,
                                                    .cancel_context = control->context};
        cbm_pinned_tree_status_t status =
            cbm_pinned_tree_read_prefix(reader->tree, index, reader->content_bound, prefix,
                                        capacity, &native_copied, &native_control, &native_error);
        if (cni_read_result(c, status, &native_error, native_copied, expected) && cni_poll(c)) {
            *copied = native_copied;
        }
    }
    if (error) {
        *error = c->error;
    }
    return c->error.status;
}
