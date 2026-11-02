#include "mcp/test_impact_tree_internal.h"

bool tpt_options_valid(const cbm_pinned_tree_options_t *o) {
    if (!o || !o->facts || !o->private_parent || !o->control.deadline_ms)
        return false;
    if (o->revision != CBM_GIT_REV_HEAD && o->revision != CBM_GIT_REV_MERGE_BASE)
        return false;
    const cbm_pinned_tree_limits_t *l = &o->limits;
    return l->max_files && l->max_directories && l->max_total_content_bytes &&
           l->max_relative_path_bytes && l->max_relative_path_bytes < TPT_PATH_CAP &&
           l->max_arena_bytes && l->blob_batch.max_entries && l->blob_batch.max_input_bytes &&
           l->blob_batch.max_arena_bytes;
}

static bool tpt_copy_identity_string(tpt_context *c, const char *source, const char **out) {
    size_t n = 0;
    if (!tpt_length(c, source, TPT_PATH_CAP - 1, &n))
        return false;
    *out = tpt_string(c, source, n);
    return *out != NULL;
}

bool tpt_identity_copy(tpt_context *c, const cbm_pinned_tree_options_t *o) {
    if (!tpt_poll(c))
        return false;
    const cbm_git_facts_identity_t *id = cbm_git_facts_identity(o->facts);
    if (!tpt_poll(c))
        return false;
    if (!id)
        return tpt_fail(c, CBM_PINNED_TREE_GIT, "missing pinned facts identity");
    cbm_pinned_tree_t *t = c->tree;
    unsigned width = id->oid_hex_length;
    if (!tpt_oid(c, id->head, width) || !tpt_oid(c, id->base, width) ||
        !tpt_oid(c, id->merge_base, width))
        return false;
    t->identity.oid_hex_length = width;
    memcpy(t->identity.head, id->head, width + 1);
    memcpy(t->identity.base, id->base, width + 1);
    memcpy(t->identity.merge_base, id->merge_base, width + 1);
    if (!tpt_copy_identity_string(c, id->root, &t->identity.root) ||
        !tpt_copy_identity_string(c, id->git_dir, &t->identity.git_dir) ||
        !tpt_copy_identity_string(c, id->common_dir, &t->identity.common_dir))
        return false;
    t->view.identity = &t->identity;
    t->view.revision = o->revision;
    t->view.commit = o->revision == CBM_GIT_REV_HEAD ? t->identity.head : t->identity.merge_base;
    return true;
}

static bool tpt_inventory_row(tpt_context *c, const cbm_git_tree_entry_t *entry,
                              const cbm_git_tree_entry_t *previous) {
    if (!entry->path)
        return tpt_fail(c, CBM_PINNED_TREE_GIT, "missing inventory path");
    if (entry->path_length > c->tree->limits.max_relative_path_bytes)
        return tpt_fail(c, CBM_PINNED_TREE_LIMIT, "inventory path bound");
    if (entry->object_type != CBM_GIT_TREE_BLOB ||
        (entry->mode != 0100644 && entry->mode != 0100755))
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "nonregular inventory entry");
    if (!tpt_oid(c, entry->oid, c->tree->identity.oid_hex_length) ||
        !tpt_path_valid(c, (const unsigned char *)entry->path, entry->path_length))
        return false;
    if (!previous)
        return true;
    if (tpt_compare(c, (const unsigned char *)previous->path, previous->path_length,
                    (const unsigned char *)entry->path, entry->path_length) >= 0)
        return tpt_fail(c, CBM_PINNED_TREE_GIT, "inventory ordering or duplicate");
    if (previous->path_length < entry->path_length && entry->path[previous->path_length] == '/' &&
        tpt_equal(c, previous->path, entry->path, previous->path_length))
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "file and directory conflict");
    return tpt_poll(c);
}

static bool tpt_prefix_new(tpt_context *c, const cbm_git_tree_entry_t *entry,
                           const cbm_git_tree_entry_t *previous, size_t length) {
    return !previous || previous->path_length <= length ||
           !tpt_equal(c, entry->path, previous->path, length + 1);
}

static bool tpt_file_prefix(tpt_context *c, const cbm_git_tree_inventory_t *inventory,
                            const char *path, size_t length) {
    size_t lo = 0, hi = inventory->count;
    while (lo < hi) {
        if (!tpt_poll(c))
            return false;
        size_t mid = lo + (hi - lo) / 2;
        const cbm_git_tree_entry_t *entry = &inventory->entries[mid];
        int order = tpt_compare(c, (const unsigned char *)entry->path, entry->path_length,
                                (const unsigned char *)path, length);
        if (!order)
            return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "file and directory conflict");
        if (order < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return tpt_poll(c);
}

static bool tpt_counts(tpt_context *c, const cbm_git_tree_inventory_t *inventory,
                       size_t *directories) {
    const cbm_git_tree_entry_t *previous = NULL;
    *directories = 1;
    for (size_t i = 0; i < inventory->count; i++) {
        if (!tpt_poll(c) || !tpt_inventory_row(c, &inventory->entries[i], previous))
            return false;
        previous = &inventory->entries[i];
    }
    previous = NULL;
    for (size_t i = 0; i < inventory->count; i++) {
        const cbm_git_tree_entry_t *entry = &inventory->entries[i];
        if (!tpt_poll(c))
            return false;
        for (size_t j = 0; j < entry->path_length; j++) {
            if (entry->path[j] != '/' || !tpt_prefix_new(c, entry, previous, j))
                continue;
            if (!tpt_file_prefix(c, inventory, entry->path, j))
                return false;
            if (*directories >= c->tree->limits.max_directories)
                return tpt_fail(c, CBM_PINNED_TREE_LIMIT, "directory count limit");
            (*directories)++;
        }
        previous = entry;
    }
    return tpt_poll(c);
}

static size_t tpt_find_directory(tpt_context *c, const char *path, size_t n, size_t used) {
    if (!n)
        return 0;
    for (size_t i = 1; i < used; i++) {
        if (!tpt_poll(c))
            return SIZE_MAX;
        tpt_object *d = &c->tree->objects[i];
        if (d->length == n && tpt_equal(c, d->path, path, n))
            return i;
    }
    return SIZE_MAX;
}

static bool tpt_plan_row(tpt_context *c, const cbm_git_tree_entry_t *entry, size_t index,
                         size_t *used) {
    cbm_pinned_tree_t *t = c->tree;
    char *path = tpt_string(c, entry->path, entry->path_length);
    if (!path)
        return false;
    size_t parent = 0;
    for (size_t j = 0; j < entry->path_length; j++) {
        if (path[j] != '/')
            continue;
        size_t found = tpt_find_directory(c, path, j, *used);
        if (found == SIZE_MAX) {
            if (!tpt_poll(c) || *used >= t->view.directory_count)
                return tpt_fail(c, CBM_PINNED_TREE_GIT, "directory plan inconsistency");
            found = (*used)++;
            tpt_object *d = &t->objects[found];
            d->path = tpt_string(c, path, j);
            d->length = j;
            d->parent = parent;
            d->directory = true;
            if (!d->path)
                return false;
        }
        parent = found;
    }
    cbm_pinned_tree_file_t *file = &t->files[index];
    file->path = (const unsigned char *)path;
    file->path_length = entry->path_length;
    file->git_mode = entry->mode;
    memcpy(file->oid, entry->oid, t->identity.oid_hex_length + 1);
    tpt_object *object = &t->objects[t->view.directory_count + index];
    object->path = path;
    object->length = entry->path_length;
    object->parent = parent;
    object->file = index;
    t->indices[index] = index;
    return true;
}

static bool tpt_scratch(tpt_context *c) {
    cbm_pinned_tree_t *t = c->tree;
    t->root_path = tpt_alloc(c, TPT_PATH_CAP, 1);
    t->path_scratch = tpt_alloc(c, TPT_PATH_CAP, 1);
    t->name_scratch = tpt_alloc(c, TPT_PATH_CAP, 1);
    t->parent_path = tpt_alloc(c, TPT_PATH_CAP, 1);
    t->io_scratch = tpt_alloc(c, TPT_CHUNK, 1);
    t->security_scratch = tpt_alloc(c, TPT_SECURITY_CAP, 1);
#ifdef _WIN32
    t->enumeration_scratch = tpt_alloc(c, TPT_CHUNK, 1);
    t->wide_path = tpt_alloc(c, TPT_PATH_CAP + 8, sizeof(wchar_t));
    t->wide_parent = tpt_alloc(c, TPT_PATH_CAP + 8, sizeof(wchar_t));
    t->wide_root = tpt_alloc(c, TPT_PATH_CAP + 8, sizeof(wchar_t));
#endif
    return tpt_poll(c);
}

bool tpt_plan(tpt_context *c, const cbm_pinned_tree_options_t *o,
              const cbm_git_tree_inventory_t *inventory) {
    cbm_pinned_tree_t *t = c->tree;
    if (inventory->count > t->limits.max_files ||
        inventory->count > SIZE_MAX / sizeof(*inventory->entries))
        return tpt_fail(c, CBM_PINNED_TREE_LIMIT, "inventory count limit");
    if ((inventory->count && !inventory->entries) || (!inventory->count && inventory->entries))
        return tpt_fail(c, CBM_PINNED_TREE_GIT, "missing complete inventory");
    if (!tpt_scratch(c) || !tpt_parent_syntax(c, o->private_parent))
        return false;
    size_t parent_length = 0, directories = 0;
    if (!tpt_length(c, o->private_parent, TPT_PATH_CAP - 1, &parent_length) ||
        !tpt_copy(c, t->parent_path, o->private_parent, parent_length + 1) ||
        !tpt_counts(c, inventory, &directories))
        return false;
    t->supplied_parent = tpt_string(c, o->private_parent, parent_length);
    if (!t->supplied_parent)
        return false;
    if (directories > SIZE_MAX - inventory->count)
        return tpt_fail(c, CBM_PINNED_TREE_LIMIT, "object count overflow");
    t->object_count = directories + inventory->count;
    t->objects = tpt_alloc(c, t->object_count, sizeof(*t->objects));
    t->files = tpt_alloc(c, inventory->count, sizeof(*t->files));
    t->indices = tpt_alloc(c, inventory->count, sizeof(*t->indices));
    t->ancestors = tpt_alloc(c, directories, sizeof(*t->ancestors));
    if (!tpt_poll(c))
        return false;
    t->view.file_count = inventory->count;
    t->view.directory_count = directories;
    t->view.files = t->files;
    t->view.root = t->root_path;
    for (size_t i = 0; i < t->object_count; i++) {
        if (!tpt_poll(c))
            return false;
        t->objects[i].handle = TPT_INVALID_HANDLE;
    }
    t->objects[0].path = t->basename;
    t->objects[0].length = 41;
    t->objects[0].parent = SIZE_MAX;
    t->objects[0].directory = true;
    size_t used = 1;
    for (size_t i = 0; i < inventory->count; i++) {
        if (!tpt_poll(c) || !tpt_plan_row(c, &inventory->entries[i], i, &used))
            return false;
    }
    return used == directories || tpt_fail(c, CBM_PINNED_TREE_GIT, "directory plan count");
}
