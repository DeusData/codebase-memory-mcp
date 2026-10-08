#include "mcp/test_impact_tree_internal.h"
#include "foundation/secure_random.h"

bool tpt_join(tpt_context *c, const tpt_object *o) {
    cbm_pinned_tree_t *t = c->tree;
    const char *base = t->root_path;
    const char *suffix = o->path;
    size_t suffix_length = o->length, base_length = 0;
    if (o == &t->parent) {
        base = t->parent_path;
        suffix_length = 0;
    } else if (o == &t->objects[0]) {
        suffix_length = 0;
    }
    if (!tpt_length(c, base, TPT_PATH_CAP - 1, &base_length))
        return false;
    size_t separator = suffix_length ? 1 : 0;
    if (base_length + separator >= TPT_PATH_CAP ||
        suffix_length > TPT_PATH_CAP - 1 - base_length - separator)
        return tpt_fail(c, CBM_PINNED_TREE_LIMIT, "joined native path limit");
    memcpy(t->path_scratch, base, base_length);
    if (separator)
        t->path_scratch[base_length++] = '/';
    memcpy(t->path_scratch + base_length, suffix, suffix_length);
    t->path_scratch[base_length + suffix_length] = 0;
    return true;
}

bool tpt_chain(tpt_context *c, const tpt_object *o) {
    cbm_pinned_tree_t *t = c->tree;
    size_t count = 0, parent = o->parent;
    while (parent != SIZE_MAX) {
        if (!tpt_poll(c) || count >= t->view.directory_count)
            return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "invalid tracked ancestor plan");
        t->ancestors[count++] = parent;
        parent = t->objects[parent].parent;
    }
    bool absent = false;
    if (!tpt_native_identity(c, &t->parent, &absent))
        return false;
    if (absent)
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "tracked parent absent");
    while (count) {
        tpt_object *p = &t->objects[t->ancestors[--count]];
        if (!tpt_poll(c) || !tpt_native_identity(c, p, &absent))
            return false;
        if (absent)
            return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "tracked ancestor absent");
    }
    return true;
}

bool tpt_record_identity(tpt_context *c, tpt_object *o, tpt_identity identity) {
    cbm_pinned_tree_t *t = c->tree;
    o->identity = identity;
    o->identified = true;
    for (size_t i = 0; i < t->object_count; i++) {
        if (!tpt_poll(c))
            return false;
        const tpt_object *other = &t->objects[i];
        if (other != o && other->identified && tpt_native_id_equal(other->identity, identity))
            return tpt_fail(c, CBM_PINNED_TREE_COLLISION, "native object aliases tracked name");
    }
    o->identity = identity;
    o->identified = true;
    return true;
}

static bool tpt_stream_next(tpt_context *c, tpt_object *o, const tpt_stream_options *options,
                            uint64_t total, size_t *count) {
    if (!tpt_poll(c))
        return false;
    if (!options->bounded)
        return tpt_native_read(c, o, count);
    uint64_t remaining = c->tree->files[o->file].content_length - total;
    size_t request = remaining > TPT_CHUNK ? TPT_CHUNK : (size_t)remaining;
    return tpt_native_read_bounded(c, o, request ? request : 1, count);
}

static bool tpt_stream_chunk(tpt_context *c, cbm_sha256_ctx *hash,
                             const tpt_stream_options *options, uint64_t total, size_t count) {
    const unsigned char *bytes = c->tree->io_scratch;
    if (options->expected && !tpt_equal(c, bytes, options->expected->data + (size_t)total, count))
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "file readback differs from pinned blob");
    if (!tpt_hash(c, hash, bytes, count))
        return false;
    if (total < options->capacity) {
        size_t room = options->capacity - (size_t)total;
        size_t retained = count < room ? count : room;
        if (!tpt_copy(c, options->prefix + (size_t)total, bytes, retained))
            return false;
    }
    return true;
}

bool tpt_read_stream(tpt_context *c, tpt_object *o, const tpt_stream_options *options) {
    cbm_pinned_tree_file_t *file = &c->tree->files[o->file];
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    uint64_t total = 0;
    for (;;) {
        size_t n = 0;
        if (!tpt_stream_next(c, o, options, total, &n))
            return false;
        if (!n) {
            if (options->eof_fault)
                return tpt_fail(c, CBM_PINNED_TREE_IO, "simulated prefix EOF failure");
            break;
        }
        if (n > file->content_length - total)
            return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "file exceeds pinned length");
        if (!tpt_stream_chunk(c, &hash, options, total, n))
            return false;
        total += n;
        if (options->read_fault)
            return tpt_fail(c, CBM_PINNED_TREE_IO, "simulated prefix read failure");
    }
    if (total != file->content_length)
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "file shorter than pinned length");
    unsigned char digest[32];
    cbm_sha256_final(&hash, digest);
    if (options->expected)
        memcpy(file->content_sha256, digest, 32);
    else if (!tpt_equal(c, digest, file->content_sha256, 32))
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "file digest changed");
    return true;
}

bool tpt_readback(tpt_context *c, tpt_object *o, const cbm_git_bytes_t *expected) {
    tpt_stream_options options = {.expected = expected};
    return tpt_chain(c, o) && tpt_native_check(c, o, false) && tpt_read_stream(c, o, &options) &&
           tpt_native_check(c, o, false) && tpt_native_close(c, &o->handle);
}

bool tpt_entry(tpt_context *c, size_t directory, const char *name, size_t length) {
    cbm_pinned_tree_t *t = c->tree;
    for (size_t i = 1; i < t->object_count; i++) {
        if (!tpt_poll(c))
            return false;
        tpt_object *o = &t->objects[i];
        if (o->parent != directory)
            continue;
        const char *leaf = tpt_leaf(o);
        size_t n = o->length - (size_t)(leaf - o->path);
        if (n != length || !tpt_equal(c, leaf, name, n))
            continue;
        if (o->seen)
            return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "duplicate directory entry");
        o->seen = true;
        return o->directory ? tpt_native_check(c, o, false) : tpt_readback(c, o, NULL);
    }
    return tpt_fail(c, c->building ? CBM_PINNED_TREE_UNSUPPORTED : CBM_PINNED_TREE_CHANGED,
                    "unexpected or nonroundtrip directory entry");
}

bool tpt_audit(tpt_context *c) {
    cbm_pinned_tree_t *t = c->tree;
    if (!tpt_native_check(c, &t->parent, false) || !tpt_native_check(c, &t->objects[0], false))
        return false;
    for (size_t i = 0; i < t->object_count; i++) {
        if (!tpt_poll(c))
            return false;
        t->objects[i].seen = false;
    }
    t->objects[0].seen = true;
    for (size_t i = 0; i < t->view.directory_count; i++) {
        if (!tpt_poll(c) || !tpt_chain(c, &t->objects[i]) ||
            !tpt_native_check(c, &t->objects[i], false) || !tpt_native_enumerate(c, i))
            return false;
    }
    for (size_t i = 0; i < t->object_count; i++) {
        if (!tpt_poll(c))
            return false;
        if (!t->objects[i].seen)
            return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "expected entry missing");
    }
    return true;
}

static bool tpt_root_name(tpt_context *c) {
    cbm_pinned_tree_t *t = c->tree;
    unsigned char random[16];
    if (!tpt_poll(c) || !cbm_secure_random(random, sizeof(random)))
        return tpt_fail(c, CBM_PINNED_TREE_IO, "secure random source failed");
    static const char hex[] = "0123456789abcdef";
    memcpy(t->basename, "cbm-tree-", 9);
    for (size_t i = 0; i < sizeof(random); i++) {
        t->basename[9 + i * 2] = hex[random[i] >> 4];
        t->basename[10 + i * 2] = hex[random[i] & 15];
    }
    t->basename[41] = 0;
    size_t n = 0;
    if (!tpt_length(c, t->parent_path, TPT_PATH_CAP - 1, &n))
        return false;
    size_t separator = n && t->parent_path[n - 1] != '/' ? 1 : 0;
#ifdef _WIN32
    if (n && t->parent_path[n - 1] == '\\')
        separator = 0;
#endif
    if (n > TPT_PATH_CAP - 42 - separator)
        return tpt_fail(c, CBM_PINNED_TREE_LIMIT, "owned root path limit");
    memcpy(t->root_path, t->parent_path, n);
    if (separator)
        t->root_path[n++] = '/';
    memcpy(t->root_path + n, t->basename, 42);
    for (size_t i = 1; i < t->object_count; i++) {
        if (!tpt_poll(c) || !tpt_join(c, &t->objects[i]))
            return false;
    }
    return true;
}

bool tpt_build(tpt_context *c, const cbm_git_blob_batch_t *batch) {
    cbm_pinned_tree_t *t = c->tree;
    bool root = false;
    for (unsigned attempt = 0; attempt < 128; attempt++) {
        if (!tpt_root_name(c))
            return false;
        if (tpt_native_create(c, &t->objects[0])) {
            root = true;
            break;
        }
        if (c->error.status != CBM_PINNED_TREE_COLLISION || t->objects[0].created)
            return false;
        tpt_error_init(&c->error);
    }
    if (!root)
        return tpt_fail(c, CBM_PINNED_TREE_COLLISION, "random child collision budget");
    for (size_t i = 1; i < t->view.directory_count; i++) {
        if (!tpt_poll(c) || !tpt_native_create(c, &t->objects[i]))
            return false;
    }
    for (size_t i = 0; i < t->view.file_count; i++) {
        tpt_object *o = &t->objects[t->view.directory_count + i];
        if (!tpt_poll(c) || !tpt_native_create(c, o) ||
            !tpt_native_write(c, o, batch->items[i].bytes) || !tpt_native_finish(c, o) ||
            !tpt_readback(c, o, &batch->items[i].bytes))
            return false;
    }
    return true;
}

bool tpt_absent_disposable(tpt_context *c, const tpt_object *o) {
    if (!o->directory)
        return true;
    cbm_pinned_tree_t *t = c->tree;
    size_t index = (size_t)(o - t->objects);
    for (size_t i = 0; i < t->object_count; i++) {
        if (t->objects[i].created && t->objects[i].parent == index)
            return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "absent ancestor has undisposed children");
    }
    return true;
}

bool tpt_cleanup(tpt_context *c) {
    cbm_pinned_tree_t *t = c->tree;
    bool ok = tpt_native_release(c);
    bool owned = false;
    for (size_t i = t->objects ? t->object_count : 0; i > 0; i--) {
        tpt_object *o = &t->objects[i - 1];
        if (!o->created)
            continue;
        tpt_context step = {.tree = t};
        tpt_error_init(&step.error);
        if (!tpt_native_remove(&step, o)) {
            if (ok)
                c->error = step.error;
            ok = false;
        }
        if (o->created)
            owned = true;
    }
    if (!owned) {
        tpt_context parent = {.tree = t};
        tpt_error_init(&parent.error);
        if (!tpt_native_close(&parent, &t->parent.handle)) {
            if (ok)
                c->error = parent.error;
            ok = false;
        }
    }
    return ok && !owned;
}
