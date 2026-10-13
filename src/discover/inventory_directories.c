#include "discover/inventory_internal.h"

static bool cif_common(cif_context *c, cbm_inventory_path_t a, cbm_inventory_path_t b,
                       size_t *length) {
    *length = 0;
    size_t n = a.length < b.length ? a.length : b.length;
    while (*length < n) {
        if (!cif_bytes(c, 2)) {
            return false;
        }
        if (a.data[*length] != b.data[*length]) {
            break;
        }
        (*length)++;
    }
    return true;
}

static bool cif_directory_init(cif_context *c, cif_directory *d, cbm_inventory_path_t path) {
    cif_directory value = {.parent = SIZE_MAX,
                           .next = SIZE_MAX,
                           .first_directory = SIZE_MAX,
                           .last_directory = SIZE_MAX,
                           .first_file = SIZE_MAX,
                           .last_file = SIZE_MAX,
                           .excluded = SIZE_MAX};
    value.path.data = cif_string(c, path.data, path.length);
    value.path.length = path.length;
    return value.path.data && cif_copy(c, d, &value, sizeof(value));
}

/* Equal slash-terminated prefixes occupy one contiguous run of sorted files.
 * Their first occurrence is unique; the generated directory order still needs
 * sorting (a-/x precedes a/x although directory a precedes directory a-). */
static bool cif_prefixes(cif_context *c, bool fill, size_t *total) {
    cbm_inventory_filter_t *o = c->owner;
    size_t count = 1;
    for (size_t i = 0; i < o->view.file_count; i++) {
        c->file_index = i;
        cbm_inventory_path_t path = o->files[i].path;
        size_t common = 0;
        if (!cif_event(c) || !cif_bytes(c, 128) ||
            (i && !cif_common(c, o->files[i - 1].path, path, &common))) {
            return false;
        }
        for (size_t j = 0; j < path.length; j++) {
            if (!cif_bytes(c, 1)) {
                return false;
            }
            if (path.data[j] != '/' || j < common) {
                continue;
            }
            if (count >= o->limits.max_directories) {
                return cif_fail(c, CBM_INVENTORY_LIMIT);
            }
            if (fill && !cif_directory_init(c, &o->directories[count],
                                            (cbm_inventory_path_t){path.data, j})) {
                return false;
            }
            count++;
        }
    }
    *total = count;
    return true;
}

static bool cif_merge(cif_context *c, const cif_directory *from, cif_directory *to, size_t start,
                      size_t middle, size_t end) {
    size_t left = start, right = middle;
    for (size_t at = start; at < end; at++) {
        int order = 0;
        if (!cif_event(c)) {
            return false;
        }
        if (left < middle && right < end &&
            !cif_compare(c, from[left].path, from[right].path, &order)) {
            return false;
        }
        size_t selected = right == end || (left < middle && order <= 0) ? left++ : right++;
        if (!cif_copy(c, &to[at], &from[selected], sizeof(*to))) {
            return false;
        }
    }
    return true;
}

static bool cif_sort_directories(cif_context *c) {
    cbm_inventory_filter_t *o = c->owner;
    size_t n = o->view.directory_count;
    if (n < 2) {
        return true;
    }
    cif_directory *temporary = cif_alloc(c, n, sizeof(*temporary));
    if (!temporary) {
        return false;
    }
    cif_directory *from = o->directories, *to = temporary;
    for (size_t width = 1; width < n;) {
        for (size_t start = 0; start < n;) {
            size_t middle = start + (width < n - start ? width : n - start);
            size_t end = middle + (width < n - middle ? width : n - middle);
            if (!cif_merge(c, from, to, start, middle, end)) {
                return false;
            }
            start = end;
        }
        cif_directory *swap = from;
        from = to;
        to = swap;
        if (width >= n - width) {
            break;
        }
        width *= 2;
    }
    return from == o->directories || cif_copy(c, o->directories, from, n * sizeof(*from));
}

static bool cif_find(cif_context *c, cbm_inventory_path_t path, bool directory, size_t *index) {
    cbm_inventory_filter_t *o = c->owner;
    size_t low = 0, high = directory ? o->view.directory_count : o->view.file_count;
    *index = SIZE_MAX;
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        cbm_inventory_path_t candidate = directory ? o->directories[mid].path : o->files[mid].path;
        int order = 0;
        if (!cif_compare(c, candidate, path, &order)) {
            return false;
        }
        if (!order) {
            *index = mid;
            return true;
        }
        if (order < 0) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return true;
}

bool cif_find_file(cif_context *c, cbm_inventory_path_t path, size_t *index) {
    return cif_find(c, path, false, index);
}

bool cif_find_directory(cif_context *c, cbm_inventory_path_t path, size_t *index) {
    return cif_find(c, path, true, index);
}

static bool cif_parent(cif_context *c, cbm_inventory_path_t path, size_t *parent) {
    size_t length = path.length;
    while (length) {
        if (!cif_bytes(c, 1)) {
            return false;
        }
        if (path.data[--length] == '/') {
            break;
        }
    }
    if (!cif_find_directory(c, (cbm_inventory_path_t){path.data, length}, parent)) {
        return false;
    }
    return *parent != SIZE_MAX || cif_fail(c, CBM_INVENTORY_STATE);
}

static bool cif_link_directories(cif_context *c) {
    cbm_inventory_filter_t *o = c->owner;
    for (size_t i = 1; i < o->view.directory_count; i++) {
        c->file_index = SIZE_MAX;
        size_t collision = SIZE_MAX;
        cif_directory *d = &o->directories[i];
        if (!cif_event(c) || !cif_bytes(c, 256) || !cif_find_file(c, d->path, &collision)) {
            return false;
        }
        if (collision != SIZE_MAX) {
            c->file_index = collision;
            return cif_fail(c, CBM_INVENTORY_INVALID);
        }
        if (!cif_parent(c, d->path, &d->parent)) {
            return false;
        }
        if (d->parent >= i) {
            return cif_fail(c, CBM_INVENTORY_STATE);
        }
        if (!cif_bytes(c, 128)) {
            return false;
        }
        cif_directory *parent = &o->directories[d->parent];
        if (parent->last_directory != SIZE_MAX) {
            o->directories[parent->last_directory].next = i;
        } else {
            parent->first_directory = i;
        }
        parent->last_directory = i;
    }
    return true;
}

static bool cif_link_files(cif_context *c) {
    cbm_inventory_filter_t *o = c->owner;
    for (size_t i = 0; i < o->view.file_count; i++) {
        c->file_index = i;
        size_t at = SIZE_MAX;
        if (!cif_event(c) || !cif_bytes(c, 256) || !cif_parent(c, o->files[i].path, &at)) {
            return false;
        }
        if (!cif_bytes(c, 128)) {
            return false;
        }
        cif_directory *parent = &o->directories[at];
        if (parent->last_file != SIZE_MAX) {
            o->file_next[parent->last_file] = i;
        } else {
            parent->first_file = i;
        }
        parent->last_file = i;
    }
    return true;
}

bool cif_directories(cif_context *c) {
    cbm_inventory_filter_t *o = c->owner;
    size_t count = 0;
    if (!cif_prefixes(c, false, &count)) {
        return false;
    }
    c->file_index = SIZE_MAX;
    o->view.directory_count = count;
    o->directories = cif_alloc(c, count, sizeof(*o->directories));
    o->controls = cif_alloc(c, count + 1, sizeof(*o->controls));
    o->exclusions = cif_alloc(c, count, sizeof(*o->exclusions));
    if (!o->directories || !o->controls || !o->exclusions ||
        !cif_directory_init(c, &o->directories[0], (cbm_inventory_path_t){NULL, 0}) ||
        !cif_prefixes(c, true, &count)) {
        return false;
    }
    c->file_index = SIZE_MAX;
    return cif_sort_directories(c) && cif_link_directories(c) && cif_link_files(c);
}
