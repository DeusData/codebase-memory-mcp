#include "mcp/test_impact_tree_internal.h"

static bool tpt_number(tpt_context *c, cbm_sha256_ctx *h, uint64_t number, size_t width) {
    unsigned char bytes[8] = {0};
    for (size_t i = 0; i < width; i++)
        bytes[width - 1 - i] = (unsigned char)(number >> (i * 8));
    return tpt_hash(c, h, bytes, width);
}

static bool tpt_span(tpt_context *c, cbm_sha256_ctx *h, const void *p, size_t n) {
    return tpt_number(c, h, n, 8) && tpt_hash(c, h, p, n);
}

static bool tpt_text(tpt_context *c, cbm_sha256_ctx *h, const char *s) {
    size_t n = 0;
    return tpt_length(c, s, TPT_PATH_CAP - 1, &n) && tpt_span(c, h, s, n);
}

static bool tpt_manifest_identity(tpt_context *c, cbm_sha256_ctx *h) {
    const cbm_git_facts_identity_t *id = &c->tree->identity;
    const cbm_pinned_tree_view_t *v = &c->tree->view;
    static const char domain[] = "cbm.pinned-tree.v1";
    return tpt_hash(c, h, domain, sizeof(domain)) &&
           tpt_number(c, h, id->oid_hex_length == 40 ? 1 : 2, 1) && tpt_text(c, h, id->root) &&
           tpt_text(c, h, id->git_dir) && tpt_text(c, h, id->common_dir) &&
           tpt_text(c, h, id->head) && tpt_text(c, h, id->base) && tpt_text(c, h, id->merge_base) &&
           tpt_number(c, h, v->revision == CBM_GIT_REV_HEAD ? 1 : 2, 1) &&
           tpt_text(c, h, v->commit) && tpt_number(c, h, v->file_count, 8) &&
           tpt_number(c, h, v->directory_count, 8) && tpt_number(c, h, v->total_content_bytes, 8);
}

bool tpt_manifest(tpt_context *c) {
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    if (!tpt_manifest_identity(c, &hash))
        return false;
    for (size_t i = 0; i < c->tree->view.file_count; i++) {
        const cbm_pinned_tree_file_t *f = &c->tree->files[i];
        if (!tpt_poll(c) || !tpt_number(c, &hash, 'F', 1) ||
            !tpt_span(c, &hash, f->path, f->path_length) || !tpt_number(c, &hash, f->git_mode, 4) ||
            !tpt_text(c, &hash, f->oid) || !tpt_number(c, &hash, f->content_length, 8) ||
            !tpt_hash(c, &hash, f->content_sha256, 32))
            return false;
    }
    cbm_sha256_final(&hash, c->tree->view.manifest_sha256);
    return tpt_poll(c);
}
