#include "mcp/test_impact_tree_internal.h"

static bool tpt_batch_row(tpt_context *c, const cbm_git_blob_batch_item_t *item, size_t index) {
    cbm_pinned_tree_t *t = c->tree;
    cbm_pinned_tree_file_t *file = &t->files[index];
    const cbm_git_tree_entry_t *e = item->entry;
    if (!e || !e->path || item->inventory_index != t->indices[index] ||
        e->object_type != CBM_GIT_TREE_BLOB || e->mode != file->git_mode ||
        e->path_length != file->path_length || !tpt_oid(c, e->oid, t->identity.oid_hex_length))
        return tpt_fail(c, CBM_PINNED_TREE_GIT, "batch row identity");
    if (!tpt_equal(c, e->path, file->path, file->path_length) ||
        !tpt_equal(c, e->oid, file->oid, t->identity.oid_hex_length + 1) ||
        (item->bytes.length && !item->bytes.data))
        return tpt_fail(c, CBM_PINNED_TREE_GIT, "batch row correspondence");
    if (item->bytes.length > t->limits.max_total_content_bytes - t->view.total_content_bytes)
        return tpt_fail(c, CBM_PINNED_TREE_LIMIT, "complete content size limit");
    file->content_length = item->bytes.length;
    t->view.total_content_bytes += item->bytes.length;
    return true;
}

bool tpt_batch(tpt_context *c, const cbm_pinned_tree_options_t *o, cbm_git_blob_batch_t *out) {
    cbm_pinned_tree_t *t = c->tree;
    cbm_git_blob_batch_request_t request = {o->revision, t->indices, t->view.file_count};
    if (!tpt_poll(c))
        return false;
    cbm_git_facts_error_t error = {0};
    bool ok = cbm_git_facts_read_blob_batch(o->facts, &request, &o->limits.blob_batch, out, &error);
    if (!tpt_after_facts(c, ok, &error))
        return false;
    if (out->revision != o->revision || out->count != t->view.file_count ||
        (out->count && !out->items) || (!out->count && out->items) ||
        out->count > SIZE_MAX / sizeof(*out->items) ||
        !tpt_oid(c, out->commit, t->identity.oid_hex_length) ||
        !tpt_equal(c, out->commit, t->view.commit, t->identity.oid_hex_length + 1))
        return tpt_fail(c, CBM_PINNED_TREE_GIT, "batch revision correspondence");
    for (size_t i = 0; i < out->count; i++) {
        if (!tpt_poll(c) || !tpt_batch_row(c, &out->items[i], i))
            return false;
    }
    return true;
}
