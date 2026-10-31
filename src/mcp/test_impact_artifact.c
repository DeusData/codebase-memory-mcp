/*
 * test_impact_artifact.c — receipt.json and the bundle digests.
 */
#include "mcp/test_impact_artifact.h"

#include "foundation/compat_fs.h"
#include "foundation/mem_core.h"
#include "foundation/sha256.h"

#include <stdio.h>
#include <string.h>
#include <yyjson/yyjson.h>

enum { TA_READ_CHUNK = 65536, TA_RECEIPT_MAX = 65536, TA_PATH_CAP = 4096 };

/* yyjson allocates through the memory core, so documents and written text
 * are released with cbm_free like everything else here. */
static void *ta_yy_malloc(void *ctx, size_t size) {
    (void)ctx;
    return cbm_alloc(CBM_MEM_CLASS_OTHER, size);
}

static void *ta_yy_realloc(void *ctx, void *block, size_t old_size, size_t size) {
    (void)ctx;
    (void)old_size;
    return cbm_realloc(CBM_MEM_CLASS_OTHER, block, size);
}

static void ta_yy_free(void *ctx, void *block) {
    (void)ctx;
    cbm_free(CBM_MEM_CLASS_OTHER, block);
}

static const yyjson_alc ta_yy_alc = {ta_yy_malloc, ta_yy_realloc, ta_yy_free, NULL};

static bool ta_hex(const char *text, size_t want_a, size_t want_b) {
    size_t n = text ? strlen(text) : 0;
    if (n != want_a && n != want_b) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        if (!((text[i] >= '0' && text[i] <= '9') || (text[i] >= 'a' && text[i] <= 'f'))) {
            return false;
        }
    }
    return true;
}

static void ta_hex_out(const uint8_t digest[CBM_SHA256_DIGEST_LEN], char hex[65]) {
    static const char digits[] = "0123456789abcdef";
    for (int i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
        hex[2 * i] = digits[digest[i] >> 4];
        hex[2 * i + 1] = digits[digest[i] & 15];
    }
    hex[64] = '\0';
}

static bool ta_path(char *out, const char *dir, const char *name) {
    int n = snprintf(out, TA_PATH_CAP, "%s/%s", dir, name);
    return n > 0 && n < TA_PATH_CAP;
}

/* A length-prefixed frame, so no field can run into the next. */
static void ta_frame(cbm_sha256_ctx *ctx, const void *bytes, uint64_t length) {
    uint8_t prefix[8];
    for (int i = 0; i < 8; i++) {
        prefix[i] = (uint8_t)(length >> (56 - 8 * i));
    }
    cbm_sha256_update(ctx, prefix, sizeof(prefix));
    if (length) {
        cbm_sha256_update(ctx, bytes, (size_t)length);
    }
}

/* Feeds a file's exact bytes as one frame; false on a read error.
 * *absent: the file does not exist (a frame-free outcome the caller encodes). */
static bool ta_feed_file(cbm_sha256_ctx *ctx, const char *path, bool framed, bool *absent) {
    *absent = false;
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        cbm_path_info_t info;
        *absent = cbm_path_info_utf8(path, &info) != 0;
        return *absent;
    }
    unsigned char *buf = cbm_alloc(CBM_MEM_CLASS_OTHER, TA_READ_CHUNK);
    if (!buf) {
        (void)fclose(f);
        return false;
    }
    /* Size first, so a framed file's prefix is its real length. */
    uint64_t total = 0;
    size_t got;
    while ((got = fread(buf, 1, TA_READ_CHUNK, f)) > 0) {
        total += got;
    }
    bool ok = !ferror(f) && fseek(f, 0, SEEK_SET) == 0;
    if (ok && framed) {
        ta_frame(ctx, NULL, 0); /* domain: a present file */
        uint8_t prefix[8];
        for (int i = 0; i < 8; i++) {
            prefix[i] = (uint8_t)(total >> (56 - 8 * i));
        }
        cbm_sha256_update(ctx, prefix, sizeof(prefix));
    }
    uint64_t again = 0;
    while (ok && (got = fread(buf, 1, TA_READ_CHUNK, f)) > 0) {
        cbm_sha256_update(ctx, buf, got);
        again += got;
    }
    ok = ok && !ferror(f) && again == total;
    cbm_free(CBM_MEM_CLASS_OTHER, buf);
    ok = fclose(f) == 0 && ok;
    return ok;
}

bool cbm_ti_sha256_file(const char *path, char hex[65]) {
    hex[0] = '\0';
    cbm_sha256_ctx ctx;
    cbm_sha256_init(&ctx);
    bool absent = false;
    if (!path || !ta_feed_file(&ctx, path, false, &absent) || absent) {
        return false;
    }
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&ctx, digest);
    ta_hex_out(digest, hex);
    return true;
}

bool cbm_ti_compatibility_digest(const char *root, const char *const *paths, int path_count,
                                 const char *platform, char hex[65]) {
    hex[0] = '\0';
    if (!root || !platform || path_count < 0 || (path_count && !paths)) {
        return false;
    }
    static const char domain[] = "cbm.test_impact.compatibility.v1";
    cbm_sha256_ctx ctx;
    cbm_sha256_init(&ctx);
    ta_frame(&ctx, domain, sizeof(domain) - 1);
    ta_frame(&ctx, platform, strlen(platform));
    for (int i = 0; i < path_count; i++) {
        char path[TA_PATH_CAP];
        if (!paths[i] || !*paths[i] || !ta_path(path, root, paths[i])) {
            return false;
        }
        ta_frame(&ctx, paths[i], strlen(paths[i]));
        bool absent = false;
        if (!ta_feed_file(&ctx, path, true, &absent)) {
            return false;
        }
        if (absent) {
            static const char marker[] = "absent";
            ta_frame(&ctx, marker, sizeof(marker) - 1);
        }
    }
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&ctx, digest);
    ta_hex_out(digest, hex);
    return true;
}

static bool ta_copy_string(yyjson_val *root, const char *key, char *out, size_t cap) {
    yyjson_val *v = yyjson_obj_get(root, key);
    const char *s = yyjson_is_str(v) ? yyjson_get_str(v) : NULL;
    if (!s || strlen(s) >= cap) {
        return false;
    }
    memcpy(out, s, strlen(s) + 1);
    return true;
}

cbm_ti_receipt_status_t cbm_ti_receipt_read(const char *bundle_dir, cbm_ti_receipt_t *out) {
    if (!out) {
        return CBM_TI_RECEIPT_INVALID;
    }
    memset(out, 0, sizeof(*out));
    char path[TA_PATH_CAP];
    if (!bundle_dir || !ta_path(path, bundle_dir, CBM_TI_RECEIPT_FILE)) {
        return CBM_TI_RECEIPT_INVALID;
    }
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        cbm_path_info_t info;
        return cbm_path_info_utf8(path, &info) != 0 ? CBM_TI_RECEIPT_ABSENT : CBM_TI_RECEIPT_IO;
    }
    char *text = cbm_alloc(CBM_MEM_CLASS_OTHER, TA_RECEIPT_MAX + 1);
    size_t len = text ? fread(text, 1, TA_RECEIPT_MAX + 1, f) : 0;
    bool read_ok = text && !ferror(f) && len <= TA_RECEIPT_MAX;
    (void)fclose(f);
    if (!read_ok) {
        cbm_free(CBM_MEM_CLASS_OTHER, text);
        return text ? CBM_TI_RECEIPT_INVALID : CBM_TI_RECEIPT_IO;
    }
    yyjson_doc *doc = yyjson_read_opts(text, len, 0, &ta_yy_alc, NULL);
    cbm_free(CBM_MEM_CLASS_OTHER, text);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    cbm_ti_receipt_t r = {0};
    char schema[64] = "";
    bool ok = yyjson_is_obj(root) && ta_copy_string(root, "schema", schema, sizeof(schema)) &&
              strcmp(schema, CBM_TI_RECEIPT_SCHEMA) == 0 &&
              ta_copy_string(root, "commit", r.commit, sizeof(r.commit)) &&
              ta_hex(r.commit, 40, 64) &&
              ta_copy_string(root, "graph_sha256", r.graph_sha256, sizeof(r.graph_sha256)) &&
              ta_hex(r.graph_sha256, 64, 64) &&
              ta_copy_string(root, "graph_content_sha256", r.graph_content_sha256,
                             sizeof(r.graph_content_sha256)) &&
              ta_hex(r.graph_content_sha256, 64, 64) &&
              ta_copy_string(root, "graph_topology_sha256", r.graph_topology_sha256,
                             sizeof(r.graph_topology_sha256)) &&
              ta_hex(r.graph_topology_sha256, 64, 64) &&
              ta_copy_string(root, "platform", r.platform, sizeof(r.platform));
    yyjson_val *coverage = ok ? yyjson_obj_get(root, "coverage") : NULL;
    if (ok && coverage && !yyjson_is_null(coverage)) {
        yyjson_val *oldest = yyjson_obj_get(coverage, "oldest_observation_at");
        r.has_coverage = true;
        ok = yyjson_is_obj(coverage) &&
             ta_copy_string(coverage, "functions_sha256", r.functions_sha256, 65) &&
             ta_hex(r.functions_sha256, 64, 64) &&
             ta_copy_string(coverage, "tests_sha256", r.tests_sha256, 65) &&
             ta_hex(r.tests_sha256, 64, 64) &&
             ta_copy_string(coverage, "metadata_sha256", r.metadata_sha256, 65) &&
             ta_hex(r.metadata_sha256, 64, 64) &&
             ta_copy_string(coverage, "compatibility_sha256", r.compatibility_sha256, 65) &&
             ta_hex(r.compatibility_sha256, 64, 64) && yyjson_is_int(oldest) &&
             yyjson_get_sint(oldest) > 0;
        r.oldest_observation_at = ok ? yyjson_get_sint(oldest) : 0;
    }
    yyjson_doc_free(doc);
    if (!ok) {
        return CBM_TI_RECEIPT_INVALID;
    }
    *out = r;
    return CBM_TI_RECEIPT_OK;
}

bool cbm_ti_receipt_write(const char *bundle_dir, const cbm_ti_receipt_t *r) {
    char path[TA_PATH_CAP];
    char tmp[TA_PATH_CAP];
    if (!bundle_dir || !r || !ta_path(path, bundle_dir, CBM_TI_RECEIPT_FILE) ||
        !ta_path(tmp, bundle_dir, CBM_TI_RECEIPT_FILE ".tmp")) {
        return false;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(&ta_yy_alc);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!root) {
        yyjson_mut_doc_free(doc);
        return false;
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "schema", CBM_TI_RECEIPT_SCHEMA);
    yyjson_mut_obj_add_str(doc, root, "commit", r->commit);
    yyjson_mut_obj_add_str(doc, root, "graph_sha256", r->graph_sha256);
    yyjson_mut_obj_add_str(doc, root, "graph_content_sha256", r->graph_content_sha256);
    yyjson_mut_obj_add_str(doc, root, "graph_topology_sha256", r->graph_topology_sha256);
    yyjson_mut_obj_add_str(doc, root, "platform", r->platform);
    if (r->has_coverage) {
        yyjson_mut_val *coverage = yyjson_mut_obj_add_obj(doc, root, "coverage");
        yyjson_mut_obj_add_str(doc, coverage, "functions_sha256", r->functions_sha256);
        yyjson_mut_obj_add_str(doc, coverage, "tests_sha256", r->tests_sha256);
        yyjson_mut_obj_add_str(doc, coverage, "metadata_sha256", r->metadata_sha256);
        yyjson_mut_obj_add_str(doc, coverage, "compatibility_sha256", r->compatibility_sha256);
        yyjson_mut_obj_add_int(doc, coverage, "oldest_observation_at", r->oldest_observation_at);
    } else {
        yyjson_mut_obj_add_null(doc, root, "coverage");
    }
    size_t len = 0;
    char *json = yyjson_mut_write_opts(doc, YYJSON_WRITE_PRETTY, &ta_yy_alc, &len, NULL);
    yyjson_mut_doc_free(doc);
    if (!json) {
        return false;
    }
    FILE *f = cbm_fopen(tmp, "wb");
    bool ok = f && fwrite(json, 1, len, f) == len && fputc('\n', f) != EOF;
    cbm_free(CBM_MEM_CLASS_OTHER, json);
    if (f) {
        ok = fclose(f) == 0 && ok;
    }
    ok = ok && cbm_rename_replace(tmp, path) == 0;
    if (!ok) {
        (void)cbm_unlink(tmp);
    }
    return ok;
}
