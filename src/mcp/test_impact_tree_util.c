#include "mcp/test_impact_tree_internal.h"
#include "foundation/platform.h"

void tpt_error_init(cbm_pinned_tree_error_t *e) {
    memset(e, 0, sizeof(*e));
    e->git.exit_code = -1;
}

bool tpt_fail(tpt_context *c, cbm_pinned_tree_status_t status, const char *message) {
    if (c->error.status == CBM_PINNED_TREE_OK) {
        c->error.status = status;
        c->error.cause = status;
        size_t i = 0;
        while (message[i] && i + 1 < sizeof(c->error.diagnostic)) {
            c->error.diagnostic[i] = message[i];
            i++;
        }
        c->error.diagnostic[i] = 0;
    }
    return false;
}

bool tpt_poll(tpt_context *c) {
    if (c->error.status != CBM_PINNED_TREE_OK)
        return false;
    if (!c->control)
        return true;
    if (c->control->cancelled && c->control->cancelled(c->control->cancel_context))
        return tpt_fail(c, CBM_PINNED_TREE_CANCELLED, "materializer cancelled");
    if (cbm_now_ms() >= c->control->deadline_ms)
        return tpt_fail(c, CBM_PINNED_TREE_DEADLINE, "materializer deadline");
    return true;
}

bool tpt_after_facts(tpt_context *c, bool ok, const cbm_git_facts_error_t *error) {
    if (!ok) {
        c->error.git = *error;
        tpt_fail(c, CBM_PINNED_TREE_GIT, "pinned facts operation failed");
    }
    tpt_context guard = {.tree = c->tree, .control = c->control};
    tpt_error_init(&guard.error);
    bool live = tpt_poll(&guard);
    if (ok && !live)
        c->error = guard.error;
    return ok && live;
}

void *tpt_alloc(tpt_context *c, size_t count, size_t size) {
    cbm_pinned_tree_t *t = c->tree;
    if (!tpt_poll(c))
        return NULL;
    if (!count || !size)
        return NULL;
    if (count > SIZE_MAX / size) {
        tpt_fail(c, CBM_PINNED_TREE_LIMIT, "allocation multiplication");
        return NULL;
    }
    size_t n = count * size;
    if (n > t->limits.max_arena_bytes - t->allocated || n > SIZE_MAX - 7 ||
        t->arena.used > SIZE_MAX - ((n + 7) & ~(size_t)7) ||
        t->arena.total_alloc > SIZE_MAX - ((n + 7) & ~(size_t)7)) {
        tpt_fail(c, CBM_PINNED_TREE_LIMIT, "materializer arena limit");
        return NULL;
    }
    size_t aligned = (n + 7) & ~(size_t)7;
    if (!t->arena.nblocks || aligned > t->arena.block_size - t->arena.used) {
        size_t block = aligned > t->arena.grow_size ? aligned : t->arena.grow_size;
        if (block > SIZE_MAX / 2) {
            tpt_fail(c, CBM_PINNED_TREE_LIMIT, "arena growth arithmetic bound");
            return NULL;
        }
    }
    t->allocated += n;
    void *memory = cbm_arena_alloc(&t->arena, n);
    if (!memory) {
        tpt_fail(c, CBM_PINNED_TREE_OOM, "materializer allocation");
        return NULL;
    }
    unsigned char *p = memory;
    while (n) {
        size_t chunk = n > TPT_CHUNK ? TPT_CHUNK : n;
        if (!tpt_poll(c))
            return NULL;
        memset(p, 0, chunk);
        p += chunk;
        n -= chunk;
    }
    return memory;
}

bool tpt_copy(tpt_context *c, void *target, const void *source, size_t n) {
    unsigned char *to = target;
    const unsigned char *from = source;
    while (n) {
        size_t chunk = n > TPT_CHUNK ? TPT_CHUNK : n;
        if (!tpt_poll(c))
            return false;
        memcpy(to, from, chunk);
        to += chunk;
        from += chunk;
        n -= chunk;
    }
    return true;
}

bool tpt_equal(tpt_context *c, const void *a, const void *b, size_t n) {
    const unsigned char *x = a, *y = b;
    while (n) {
        size_t chunk = n > TPT_CHUNK ? TPT_CHUNK : n;
        if (!tpt_poll(c) || memcmp(x, y, chunk))
            return false;
        x += chunk;
        y += chunk;
        n -= chunk;
    }
    return true;
}

bool tpt_length(tpt_context *c, const char *s, size_t max, size_t *out) {
    if (!s)
        return tpt_fail(c, CBM_PINNED_TREE_INVALID, "missing string");
    for (size_t i = 0; i <= max; i++) {
        if (i % 4096 == 0 && !tpt_poll(c))
            return false;
        if (!s[i]) {
            *out = i;
            return true;
        }
    }
    return tpt_fail(c, CBM_PINNED_TREE_LIMIT, "string length bound");
}

char *tpt_string(tpt_context *c, const char *s, size_t n) {
    if (n == SIZE_MAX) {
        tpt_fail(c, CBM_PINNED_TREE_LIMIT, "string size overflow");
        return NULL;
    }
    char *copy = tpt_alloc(c, n + 1, 1);
    return copy && tpt_copy(c, copy, s, n) ? copy : NULL;
}

bool tpt_oid(tpt_context *c, const char *oid, unsigned width) {
    if (!oid || (width != 40 && width != 64))
        return tpt_fail(c, CBM_PINNED_TREE_GIT, "invalid pinned object identity");
    for (unsigned i = 0; i < width; i++) {
        unsigned char ch = (unsigned char)oid[i];
        if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')))
            return tpt_fail(c, CBM_PINNED_TREE_GIT, "noncanonical pinned OID");
    }
    return !oid[width] || tpt_fail(c, CBM_PINNED_TREE_GIT, "pinned OID width");
}

int tpt_compare(tpt_context *c, const unsigned char *a, size_t an, const unsigned char *b,
                size_t bn) {
    size_t n = an < bn ? an : bn;
    for (size_t i = 0; i < n; i++) {
        if (i % 4096 == 0 && !tpt_poll(c))
            return 0;
        if (a[i] != b[i])
            return a[i] < b[i] ? -1 : 1;
    }
    return (an > bn) - (an < bn);
}

bool tpt_hash(tpt_context *c, cbm_sha256_ctx *hash, const void *bytes, size_t n) {
    const unsigned char *p = bytes;
    uint64_t used = hash->bitlen / 8 + hash->buflen;
    if ((uint64_t)n > UINT64_MAX / 8 - used)
        return tpt_fail(c, CBM_PINNED_TREE_LIMIT, "SHA256 length bound");
    while (n) {
        size_t chunk = n > TPT_CHUNK ? TPT_CHUNK : n;
        if (!tpt_poll(c))
            return false;
        cbm_sha256_update(hash, p, chunk);
        p += chunk;
        n -= chunk;
    }
    return true;
}

const char *tpt_leaf(const tpt_object *o) {
    size_t start = o->length;
    while (start && o->path[start - 1] != '/')
        start--;
    return o->path + start;
}

tpt_object *tpt_parent(cbm_pinned_tree_t *t, const tpt_object *o) {
    return o->parent == SIZE_MAX ? &t->parent : &t->objects[o->parent];
}

void tpt_dispose_memory(cbm_pinned_tree_t *t) {
    CBMArena arena = t->arena;
    cbm_arena_destroy(&arena);
}
