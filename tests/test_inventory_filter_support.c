#include "test_inventory_filter_internal.h"
#include "discover/gitignore_checked.h"

void if_init(if_fixture *f) {
    memset(f, 0, sizeof(*f));
#ifdef _WIN32
    strcpy(f->root, "C:/inventory-fixture");
#else
    strcpy(f->root, "/inventory-fixture");
#endif
    f->source = (cbm_inventory_source_t){
        .native_root = f->root, .files = f->files, .read = if_read, .read_context = f};
    memset(f->source.manifest_sha256, 0x4a, 32);
    f->limits = (cbm_inventory_limits_t){.max_files = IF_ROWS,
                                         .max_directories = 128,
                                         .max_arena_bytes = 2 * 1024 * 1024,
                                         .max_ignore_arena_bytes = 512 * 1024,
                                         .max_control_file_bytes = IF_BYTES,
                                         .max_control_total_bytes = 16384,
                                         .max_ignore_patterns = 256,
                                         .max_probe_prefix_bytes = 1,
                                         .max_ignore_work = 1000000,
                                         .max_verified_file_reads = IF_ROWS,
                                         .max_verified_content_bytes = 16384};
    f->control.deadline_ms = UINT64_MAX;
    f->fail_index = SIZE_MAX;
}
bool if_add(if_fixture *f, const char *path, const void *bytes, size_t size) {
    size_t i = f->source.file_count, n = strlen(path);
    if (i >= IF_ROWS || n >= IF_PATH || size > IF_BYTES)
        return false;
    strcpy(f->paths[i], path);
    if (size)
        memcpy(f->bytes[i], bytes, size);
    f->files[i] = (cbm_inventory_file_t){
        .path = {(unsigned char *)f->paths[i], n}, .git_mode = 0100644, .content_length = size};
    memset(f->files[i].oid, 'a', 40);
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    cbm_sha256_update(&hash, bytes, size);
    cbm_sha256_final(&hash, f->files[i].content_sha256);
    f->source.file_count++;
    return true;
}
bool if_text(if_fixture *f, const char *path, const char *bytes) {
    return if_add(f, path, bytes, strlen(bytes));
}
void if_reset_reads(if_fixture *f) {
    f->calls = 0;
    f->overflowed = false;
}
/* The clean native enum has no STATE member; native invalid-state use is INVALID.
 * Generic provider STATE forwarding is tested independently. */
static cbm_inventory_status_t if_native_status(cbm_pinned_tree_status_t status) {
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
    case CBM_PINNED_TREE_CHANGED:
        return CBM_INVENTORY_CHANGED;
    case CBM_PINNED_TREE_CANCELLED:
        return CBM_INVENTORY_CANCELLED;
    case CBM_PINNED_TREE_DEADLINE:
        return CBM_INVENTORY_DEADLINE;
    default:
        return CBM_INVENTORY_INVALID;
    }
}
cbm_inventory_status_t if_read(void *context, size_t i, unsigned char *prefix, size_t cap,
                               size_t *copied, const cbm_inventory_control_t *control,
                               cbm_inventory_error_t *error) {
    if_fixture *f = context;
    bool initialized =
        *copied == 0 && error->status == CBM_INVENTORY_OK && !error->cleanup_required;
    *copied = 0;
    *error = (cbm_inventory_error_t){.status = CBM_INVENTORY_OK, .file_index = SIZE_MAX};
    if (!initialized || f->calls >= IF_CALLS || i >= f->source.file_count) {
        f->overflowed = true;
        error->status = CBM_INVENTORY_LIMIT;
        return error->status;
    }
    f->indices[f->calls] = i;
    f->capacities[f->calls++] = cap;
    if (i == f->fail_index) {
        error->status = f->reported;
        error->cleanup_required = f->cleanup;
        strcpy(error->diagnostic, "provider-private-bytes");
        if (f->reply_bytes) {
            size_t amount = (size_t)f->files[i].content_length;
            if (amount > cap)
                amount = cap;
            if (amount)
                memcpy(prefix, f->bytes[i], amount);
        }
        *copied = f->reply_copied;
        return f->returned;
    }
    if (f->tree) {
        cbm_pinned_tree_control_t c = {.deadline_ms = control->deadline_ms,
                                       .cancelled = control->cancelled,
                                       .cancel_context = control->context};
        cbm_pinned_tree_error_t e;
        cbm_pinned_tree_status_t s =
            cbm_pinned_tree_read_prefix(f->tree, i, 16384, prefix, cap, copied, &c, &e);
        error->cleanup_required = s == CBM_PINNED_TREE_CLEANUP_REQUIRED;
        error->status = if_native_status(error->cleanup_required ? e.cause : s);
        return error->status;
    }
    size_t n = (size_t)f->files[i].content_length;
    if (cap < n)
        n = cap;
    if (n)
        memcpy(prefix, f->bytes[i], n);
    *copied = n;
    return CBM_INVENTORY_OK;
}
bool if_prepare(if_fixture *f) {
    cbm_inventory_error_t error;
    memset(&error, 0xa5, sizeof(error));
    cbm_inventory_status_t status =
        cbm_inventory_filter_prepare(&f->source, &f->limits, &f->control, &f->owner, &error);
    if (status != CBM_INVENTORY_OK || !f->owner) {
        fprintf(stderr, "inventory prepare status=%d error=%d index=%zu cleanup=%d\n", status,
                error.status, error.file_index, error.cleanup_required);
        return false;
    }
    return error.status == CBM_INVENTORY_OK && error.file_index == SIZE_MAX &&
           !error.cleanup_required && !error.diagnostic[0] && !f->overflowed;
}
typedef enum { IF_INDEX_EXACT, IF_INDEX_KNOWN_ROW, IF_INDEX_DURING_WORK } if_index_mode;
static bool if_error_impl(if_fixture *f, cbm_inventory_status_t expected, size_t index,
                          bool cleanup, if_index_mode mode) {
    cbm_inventory_error_t error;
    memset(&error, 0xa5, sizeof(error));
    cbm_inventory_filter_t *sentinel = (cbm_inventory_filter_t *)(uintptr_t)1;
    f->owner = sentinel;
    cbm_inventory_status_t actual =
        cbm_inventory_filter_prepare(&f->source, &f->limits, &f->control, &f->owner, &error);
    bool cleared = f->owner == NULL;
    if (f->owner == sentinel)
        f->owner = NULL;
    bool index_ok = mode == IF_INDEX_EXACT ? error.file_index == index
                    : mode == IF_INDEX_KNOWN_ROW
                        ? error.file_index < f->source.file_count
                        : error.file_index == SIZE_MAX || error.file_index < f->source.file_count;
    bool ok = actual == expected && error.status == expected && cleared && !f->overflowed &&
              error.cleanup_required == cleanup && index_ok &&
              memchr(error.diagnostic, 0, sizeof(error.diagnostic)) &&
              !strstr(error.diagnostic, "provider-private-bytes");
    if (!ok)
        fprintf(stderr,
                "inventory error actual=%d expected=%d error=%d index=%zu cleanup=%d cleared=%d\n",
                actual, expected, error.status, error.file_index, error.cleanup_required, cleared);
    return ok;
}
bool if_error(if_fixture *f, cbm_inventory_status_t expected, size_t index, bool cleanup) {
    return if_error_impl(f, expected, index, cleanup, IF_INDEX_EXACT);
}
bool if_error_known_row(if_fixture *f, cbm_inventory_status_t expected, bool cleanup) {
    return if_error_impl(f, expected, 0, cleanup, IF_INDEX_KNOWN_ROW);
}
bool if_error_during_work(if_fixture *f, cbm_inventory_status_t expected, bool cleanup) {
    return if_error_impl(f, expected, 0, cleanup, IF_INDEX_DURING_WORK);
}
int if_finish(if_fixture *f, int result) {
    cbm_inventory_filter_free(f->owner);
    f->owner = NULL;
    return result;
}
static bool if_path(cbm_inventory_path_t path, const char *expected) {
    if (expected && !expected[0])
        return path.length == 0 && (!path.data || !path.data[0]);
    return expected ? path.data && path.length == strlen(expected) &&
                          !memcmp(path.data, expected, path.length + 1)
                    : !path.data && !path.length;
}
bool if_rows(const if_fixture *f, const if_row *rows, size_t count) {
    const cbm_inventory_filter_view_t *v = cbm_inventory_filter_view(f->owner);
    if (!v || v->file_count != count || memcmp(v->manifest_sha256, f->source.manifest_sha256, 32))
        return false;
    for (size_t i = 0; i < count; i++) {
        const cbm_inventory_filter_row_t *r = &v->rows[i];
        if (!if_path(v->files[i].path, rows[i].path) ||
            v->files[i].git_mode != f->files[i].git_mode ||
            v->files[i].content_length != f->files[i].content_length ||
            strcmp(v->files[i].oid, f->files[i].oid) ||
            memcmp(v->files[i].content_sha256, f->files[i].content_sha256, 32) ||
            r->file_index != i || r->disposition != rows[i].disposition ||
            r->reason != rows[i].reason || r->roles != rows[i].roles ||
            !if_path(r->excluded_ancestor, rows[i].ancestor))
            return false;
    }
    return true;
}
bool if_controls(const if_fixture *f, const if_control_row *rows, size_t count) {
    const cbm_inventory_filter_view_t *v = cbm_inventory_filter_view(f->owner);
    if (!v || v->control_count != count)
        return false;
    for (size_t i = 0; i < count; i++) {
        const cbm_inventory_control_row_t *r = &v->controls[i];
        if (r->kind != rows[i].kind || !if_path(r->directory, rows[i].directory) ||
            r->file_index != rows[i].index || r->outcome != rows[i].outcome ||
            r->effective_patterns != rows[i].patterns)
            return false;
    }
    return true;
}
bool if_reads(const if_fixture *f, const size_t *indices, size_t count) {
    if (f->calls != count || f->overflowed)
        return false;
    for (size_t i = 0; i < count; i++)
        if (f->indices[i] != indices[i] || f->capacities[i] != f->files[indices[i]].content_length)
            return false;
    return true;
}
bool if_dependency_control(void) {
    cbm_gitignore_t *gi = cbm_gitignore_parse("*.tmp\n!keep.tmp\n");
    if (!gi)
        return false;
    bool ok = cbm_gitignore_matches(gi, "drop.tmp", false) &&
              !cbm_gitignore_matches(gi, "keep.tmp", false) &&
              cbm_should_skip_dir("node_modules", CBM_MODE_FULL) &&
              cbm_has_ignored_suffix("a.png", CBM_MODE_FULL) &&
              !cbm_should_skip_filename("LICENSE", CBM_MODE_FULL) &&
              !cbm_matches_fast_pattern("thing.generated.c", CBM_MODE_FULL);
    cbm_gitignore_free(gi);
    return ok;
}
