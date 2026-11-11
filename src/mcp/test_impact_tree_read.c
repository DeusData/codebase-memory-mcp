#include "mcp/test_impact_tree_internal.h"

static bool tpt_read_valid(tpt_context *c, size_t index, uint64_t limit,
                           const unsigned char *prefix, size_t capacity, const size_t *copied) {
    cbm_pinned_tree_t *t = c->tree;
    if (!t || !t->ready || !copied || !c->control || !c->control->deadline_ms || !limit ||
        index >= t->view.file_count || (capacity && !prefix))
        return tpt_fail(c, CBM_PINNED_TREE_INVALID, "invalid prefix read arguments");
    if (t->files[index].content_length > limit)
        return tpt_fail(c, CBM_PINNED_TREE_LIMIT, "prefix full-content bound");
    return true;
}

/* Unlike tpt_chain, this checks full policy on every ancestor, not just identity. */
static bool tpt_read_policy(tpt_context *c, tpt_object *o) {
    cbm_pinned_tree_t *t = c->tree;
    size_t count = 0, parent = o->parent;
    while (parent != SIZE_MAX) {
        if (!tpt_poll(c))
            return false;
        if (parent >= t->view.directory_count || count >= t->view.directory_count)
            return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "invalid prefix ancestor plan");
        t->ancestors[count++] = parent;
        parent = t->objects[parent].parent;
    }
    if (!tpt_poll(c) || !tpt_native_check(c, &t->parent, false))
        return false;
    while (count) {
        tpt_object *ancestor = &t->objects[t->ancestors[--count]];
        if (!tpt_poll(c) || !tpt_native_check(c, ancestor, false))
            return false;
    }
    return tpt_poll(c) && tpt_native_check(c, o, false);
}

static bool tpt_read_cleanup_error(tpt_context *c, const tpt_context *cleanup) {
    if (c->error.status == CBM_PINNED_TREE_OK)
        c->error = cleanup->error;
    if (c->error.status == CBM_PINNED_TREE_OK)
        tpt_fail(c, CBM_PINNED_TREE_IO, "prefix resource release incomplete");
    c->error.status = CBM_PINNED_TREE_CLEANUP_REQUIRED;
    memcpy(c->error.cleanup_path, c->tree->root_path, TPT_PATH_CAP);
    return false;
}

static bool tpt_read_release_already_failed(const tpt_context *c) {
    if (c->close_failed)
        return true;
#ifdef __APPLE__
    /* A retained ACL here means native checking already tried and failed to
     * free it. Leave it for a later explicit close, not an immediate retry. */
    if (c->tree->pending_acl)
        return true;
#endif
    return false;
}

/* Selected-file release happens once even when an auxiliary close failed.
 * Cleanup ignores cancellation and never unlinks this owner's namespace. */
static bool tpt_read_finish(tpt_context *c, tpt_object *o, bool close_fault) {
    tpt_context cleanup = {.tree = c->tree};
    tpt_error_init(&cleanup.error);
    bool closed;
    if (close_fault && o->handle != TPT_INVALID_HANDLE)
        closed = tpt_fail(&cleanup, CBM_PINNED_TREE_IO, "simulated prefix close failure");
    else
        closed = tpt_native_close(&cleanup, &o->handle);
    if (!closed || tpt_read_release_already_failed(c))
        return tpt_read_cleanup_error(c, &cleanup);
    if (c->error.status == CBM_PINNED_TREE_OK && tpt_poll(c))
        return true;
    if (!tpt_native_release(&cleanup))
        return tpt_read_cleanup_error(c, &cleanup);
    return false;
}

static bool tpt_read_take_fault(cbm_pinned_tree_t *t, tpt_stream_options *stream) {
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    cbm_pinned_tree_read_fault_t fault = t->read_fault;
    t->read_fault = CBM_PINNED_TREE_READ_FAULT_NONE;
    stream->read_fault = fault == CBM_PINNED_TREE_READ_FAULT_READ;
    stream->eof_fault = fault == CBM_PINNED_TREE_READ_FAULT_EOF;
    return fault == CBM_PINNED_TREE_READ_FAULT_CLOSE;
#else
    (void)t;
    (void)stream;
    return false;
#endif
}

cbm_pinned_tree_status_t cbm_pinned_tree_read_prefix(cbm_pinned_tree_t *t, size_t file_index,
                                                     uint64_t max_content_bytes,
                                                     unsigned char *prefix, size_t capacity,
                                                     size_t *copied,
                                                     const cbm_pinned_tree_control_t *control,
                                                     cbm_pinned_tree_error_t *error) {
    if (copied)
        *copied = 0;
    tpt_context c = {.tree = t, .control = control};
    tpt_error_init(&c.error);
    if (error)
        *error = c.error;
    if (tpt_read_valid(&c, file_index, max_content_bytes, prefix, capacity, copied)) {
        t->ready = false;
        tpt_stream_options stream = {.prefix = prefix, .capacity = capacity, .bounded = true};
        bool close_fault = tpt_read_take_fault(t, &stream);
        tpt_object *o = &t->objects[t->view.directory_count + file_index];
        if (tpt_poll(&c)) {
            if (o->handle != TPT_INVALID_HANDLE)
                tpt_fail(&c, CBM_PINNED_TREE_CHANGED, "prefix file handle was not closed");
            else if (tpt_read_policy(&c, o) && tpt_read_stream(&c, o, &stream))
                (void)tpt_read_policy(&c, o);
        }
        if (tpt_read_finish(&c, o, close_fault)) {
            uint64_t length = t->files[file_index].content_length;
            t->ready = true;
            *copied = length < capacity ? (size_t)length : capacity;
        }
    }
    if (error)
        *error = c.error;
    return c.error.status;
}

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
bool cbm_pinned_tree_test_set_read_fault(cbm_pinned_tree_t *t, cbm_pinned_tree_read_fault_t fault) {
    if (!t || !t->ready || (unsigned)fault > CBM_PINNED_TREE_READ_FAULT_CLOSE)
        return false;
    t->read_fault = fault;
    return true;
}
#endif
