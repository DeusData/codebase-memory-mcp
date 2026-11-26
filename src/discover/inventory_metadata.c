#include "discover/inventory_internal.h"
#include "foundation/path_syntax_internal.h"
#include <limits.h>

static bool cif_separator(unsigned char ch) {
#ifdef _WIN32
    return ch == '/' || ch == '\\';
#else
    return ch == '/';
#endif
}

#ifdef _WIN32
static bool cif_windows_encoding(cif_context *c, const char *path, size_t length) {
    wchar_t wide[CIF_PATH_MAX + 1];
    char roundtrip[CIF_PATH_MAX + 1];
    if (!cif_poll(c)) {
        return false;
    }
    cbm_path_syntax_result_t s = cbm_path_windows_roundtrip(path, length, wide, CIF_PATH_MAX + 1,
                                                            roundtrip, sizeof(roundtrip));
    if (s != CBM_PATH_SYNTAX_OK) {
        return cif_fail(c, CBM_INVENTORY_UNSUPPORTED);
    }
    return cif_poll(c);
}

static bool cif_device_root(const unsigned char *root, size_t n) {
    if (n < 4) {
        return false;
    }
    return (cif_separator(root[0]) && cif_separator(root[1]) &&
            (root[2] == '?' || root[2] == '.') && cif_separator(root[3])) ||
           (root[0] == '\\' && root[1] == '?' && root[2] == '?' && root[3] == '\\');
}
#endif

static bool cif_root(cif_context *c, size_t *length) {
    const unsigned char *root = (const unsigned char *)c->source->native_root;
    size_t n = 0;
    for (; n <= CIF_PATH_MAX; n++) {
        if (!cif_bytes(c, 1)) {
            return false;
        }
        if (!root[n]) {
            break;
        }
    }
    if (!n) {
        return cif_fail(c, CBM_INVENTORY_INVALID);
    }
    if (n > CIF_PATH_MAX) {
        return cif_fail(c, CBM_INVENTORY_LIMIT);
    }
#ifdef _WIN32
    if (cif_device_root(root, n)) {
        return cif_fail(c, CBM_INVENTORY_UNSUPPORTED);
    }
    if (!cif_poll(c)) {
        return false;
    }
    if (cbm_path_windows_absolute((const char *)root, n) != CBM_PATH_SYNTAX_OK) {
        return cif_fail(c, CBM_INVENTORY_INVALID);
    }
    if (!cif_poll(c) || !cif_windows_encoding(c, (const char *)root, n)) {
        return false;
    }
#else
    if (root[0] != '/') {
        return cif_fail(c, CBM_INVENTORY_INVALID);
    }
#endif
    *length = n;
    c->owner->view.native_root = (const char *)cif_string(c, root, n);
    return c->owner->view.native_root != NULL;
}

static bool cif_component(cif_context *c, const unsigned char *p, size_t n) {
    if (!n || (n == 1 && p[0] == '.') || (n == 2 && p[0] == '.' && p[1] == '.')) {
        return cif_fail(c, CBM_INVENTORY_INVALID);
    }
#ifdef _WIN32
    if (!cif_poll(c)) {
        return false;
    }
    if (cbm_path_windows_component(p, n) != CBM_PATH_SYNTAX_OK) {
        return cif_fail(c, CBM_INVENTORY_UNSUPPORTED);
    }
    return cif_poll(c);
#else
    return true;
#endif
}

static bool cif_file_path(cif_context *c, cbm_inventory_path_t path, size_t root_length) {
    if (path.length > CIF_PATH_MAX) {
        return cif_fail(c, CBM_INVENTORY_LIMIT);
    }
    if (!path.data || !path.length) {
        return cif_fail(c, CBM_INVENTORY_INVALID);
    }
    size_t separator =
        cif_separator((unsigned char)c->owner->view.native_root[root_length - 1]) ? 0 : 1;
    if (path.length > CIF_PATH_MAX - root_length ||
        separator > CIF_PATH_MAX - root_length - path.length) {
        return cif_fail(c, CBM_INVENTORY_LIMIT);
    }
    size_t start = 0;
    for (size_t i = 0; i <= path.length; i++) {
        if (!cif_bytes(c, 1)) {
            return false;
        }
        unsigned char ch = path.data[i];
        if ((ch == 0) != (i == path.length)) {
            return cif_fail(c, CBM_INVENTORY_INVALID);
        }
        if (!ch || ch == '/') {
            if (!cif_component(c, path.data + start, i - start)) {
                return false;
            }
            start = i + 1;
        }
    }
#ifdef _WIN32
    return cif_windows_encoding(c, (const char *)path.data, path.length);
#else
    return true;
#endif
}

static bool cif_oid(cif_context *c, const char oid[65], size_t *width) {
    size_t n = 0;
    while (n < 65) {
        if (!cif_bytes(c, 1)) {
            return false;
        }
        unsigned char ch = (unsigned char)oid[n];
        if (!ch) {
            break;
        }
        if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'))) {
            return cif_fail(c, CBM_INVENTORY_INVALID);
        }
        n++;
    }
    if ((n != 40 && n != 64) || (*width && *width != n)) {
        return cif_fail(c, CBM_INVENTORY_INVALID);
    }
    *width = n;
    return true;
}

static bool cif_file(cif_context *c, size_t i, size_t root_length, size_t *width) {
    const cbm_inventory_file_t *in = &c->source->files[i];
    cbm_inventory_file_t *out = &c->owner->files[i];
    c->file_index = i;
    if (!cif_event(c) || !cif_bytes(c, 256) || !cif_file_path(c, in->path, root_length)) {
        return false;
    }
    if (i) {
        int order = 0;
        if (!cif_compare(c, c->owner->files[i - 1].path, in->path, &order)) {
            return false;
        }
        if (order >= 0) {
            return cif_fail(c, CBM_INVENTORY_INVALID);
        }
    }
    if (in->git_mode != 0100644 && in->git_mode != 0100755) {
        return cif_fail(c, CBM_INVENTORY_UNSUPPORTED);
    }
    if (in->content_length > INT64_MAX) {
        return cif_fail(c, CBM_INVENTORY_LIMIT);
    }
    if (!cif_oid(c, in->oid, width)) {
        return false;
    }
    out->path.data = cif_string(c, in->path.data, in->path.length);
    if (!out->path.data || !cif_bytes(c, 128)) {
        return false;
    }
    out->path.length = in->path.length;
    out->git_mode = in->git_mode;
    out->content_length = in->content_length;
    if (!out->path.data || !cif_copy(c, out->oid, in->oid, *width + 1) ||
        !cif_copy(c, out->content_sha256, in->content_sha256, 32)) {
        return false;
    }
    cbm_inventory_filter_row_t *row = &c->owner->rows[i];
    row->file_index = i;
    row->disposition = CBM_INVENTORY_FILTER_NEEDS_LANGUAGE;
    static const unsigned char config[] = ".codebase-memory.json";
    int order = 0;
    if (!cif_compare(c, out->path, (cbm_inventory_path_t){config, sizeof(config) - 1}, &order)) {
        return false;
    }
    if (!cif_bytes(c, 128)) {
        return false;
    }
    if (!order) {
        row->roles = CBM_INVENTORY_FILTER_ROLE_PHYSICAL_PROJECT_CONFIG;
    }
    c->owner->file_next[i] = SIZE_MAX;
    return true;
}

bool cif_metadata(cif_context *c) {
    cbm_inventory_filter_t *o = c->owner;
    size_t root_length = 0, width = 0;
    if (!cif_root(c, &root_length)) {
        return false;
    }
    size_t n = c->source->file_count;
    o->view.file_count = n;
    o->files = cif_alloc(c, n, sizeof(*o->files));
    o->rows = cif_alloc(c, n, sizeof(*o->rows));
    o->file_next = cif_alloc(c, n, sizeof(*o->file_next));
    if ((n && (!o->files || !o->rows || !o->file_next)) ||
        !cif_copy(c, o->view.manifest_sha256, c->source->manifest_sha256, 32)) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (!cif_file(c, i, root_length, &width)) {
            return false;
        }
    }
    c->file_index = SIZE_MAX;
    return true;
}
