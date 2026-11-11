/* Query-local test-path conventions from .codebase-memory.json. */
#include "mcp/test_impact.h"
#include "discover/discover.h"
#include "foundation/arena.h"
#include "foundation/compat_fs.h"
#include "foundation/sha256.h"
#include <yyjson/yyjson.h>
#include <stdio.h>
#include <string.h>

enum { TEST_CONFIG_MAX_BYTES = 65536 };

struct cbm_test_config {
    CBMArena arena;
    cbm_gitignore_t *test_globs;
    const char *source;
    size_t source_len;
    bool source_absent;
    char digest[CBM_SHA256_HEX_LEN + 1];
};

static yyjson_val *tc_field(yyjson_val *object, const char *name, bool *valid) {
    if (!yyjson_is_obj(object)) {
        *valid = false;
        return NULL;
    }
    yyjson_val *found = NULL, *key, *value;
    size_t index, maximum;
    size_t len = strlen(name);
    yyjson_obj_foreach(object, index, maximum, key, value) {
        if (yyjson_get_len(key) == len && memcmp(yyjson_get_str(key), name, len) == 0) {
            if (found) {
                *valid = false;
                return NULL;
            }
            found = value;
        }
    }
    return found;
}

static void tc_digest(cbm_test_config_t *config, const char *bytes, size_t len, bool absent) {
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    const char *domain = absent ? "cbm-test-config-absent-v1" : "cbm-test-config-bytes-v1";
    cbm_sha256_update(&hash, domain, strlen(domain) + 1);
    if (len)
        cbm_sha256_update(&hash, bytes, len);
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&hash, digest);
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
        config->digest[i * 2] = hex[digest[i] >> 4];
        config->digest[i * 2 + 1] = hex[digest[i] & 15];
    }
    config->digest[CBM_SHA256_HEX_LEN] = '\0';
}

static bool tc_parse(cbm_test_config_t *config, const char *bytes, size_t len) {
    if (memchr(bytes, '\0', len))
        return false;
    yyjson_doc *doc = yyjson_read(bytes, len, 0);
    if (!doc)
        return false;
    bool valid = true;
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *impact = tc_field(root, "test_impact", &valid);
    if (!valid || !impact)
        goto done;
    yyjson_val *version = tc_field(impact, "version", &valid);
    if (!yyjson_is_uint(version) || yyjson_get_uint(version) != 1) {
        valid = false;
        goto done;
    }
    yyjson_val *tests = tc_field(impact, "tests", &valid);
    if (!valid || !tests)
        goto done;
    yyjson_val *globs = tc_field(tests, "test_globs", &valid);
    if (!valid || !globs)
        goto done;
    if (!yyjson_is_arr(globs)) {
        valid = false;
        goto done;
    }
    /* JSON source size bounds decoded pattern bytes plus one newline per
     * element. Reject gitignore control records: this list is a union of
     * positive patterns, not an ordered include/exclude program. */
    char *patterns = cbm_arena_alloc(&config->arena, len + 1);
    if (!patterns) {
        valid = false;
        goto done;
    }
    size_t used = 0, index, maximum;
    yyjson_val *pattern;
    yyjson_arr_foreach(globs, index, maximum, pattern) {
        const char *text = yyjson_get_str(pattern);
        size_t n = yyjson_get_len(pattern);
        if (!text || !n || strlen(text) != n || text[0] == '!' || text[0] == '#' ||
            memchr(text, '\n', n) || memchr(text, '\r', n) || text[n - 1] == ' ' ||
            text[n - 1] == '\t' || used > len || n >= len - used) {
            valid = false;
            goto done;
        }
        memcpy(patterns + used, text, n);
        used += n;
        patterns[used++] = '\n';
    }
    patterns[used] = '\0';
    config->test_globs = cbm_gitignore_parse(patterns);
    if (!config->test_globs)
        valid = false;
done:
    yyjson_doc_free(doc);
    return valid;
}

cbm_test_config_t *cbm_test_config_load(const char *path, bool optional) {
    if (!path || !*path)
        return NULL;
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_test_config_t *config = cbm_arena_calloc(&arena, sizeof(*config));
    if (!config) {
        cbm_arena_destroy(&arena);
        return NULL;
    }
    config->arena = arena;
    cbm_path_info_t info = {0};
    int status = cbm_path_info_utf8(path, &info);
    if (status == CBM_PATH_INFO_ABSENT && optional) {
        config->source = "";
        config->source_absent = true;
        tc_digest(config, NULL, 0, true);
        return config;
    }
    if (status != CBM_PATH_INFO_OK || !info.is_regular || info.size > TEST_CONFIG_MAX_BYTES)
        goto invalid;
    FILE *file = cbm_fopen(path, "rb");
    if (!file)
        goto invalid;
    char *bytes = cbm_arena_alloc(&config->arena, TEST_CONFIG_MAX_BYTES + 1U);
    if (!bytes) {
        fclose(file);
        goto invalid;
    }
    size_t len = fread(bytes, 1, TEST_CONFIG_MAX_BYTES + 1U, file);
    bool read_ok = !ferror(file) && len <= TEST_CONFIG_MAX_BYTES;
    if (fclose(file) != 0)
        read_ok = false;
    if (!read_ok || !tc_parse(config, bytes, len))
        goto invalid;
    bytes[len] = '\0';
    config->source = bytes;
    config->source_len = len;
    tc_digest(config, bytes, len, false);
    return config;
invalid:
    cbm_test_config_free(config);
    return NULL;
}

void cbm_test_config_free(cbm_test_config_t *config) {
    if (!config)
        return;
    cbm_gitignore_free(config->test_globs);
    CBMArena arena = config->arena;
    cbm_arena_destroy(&arena);
}

bool cbm_test_config_is_test_file(const cbm_test_config_t *config, const char *file) {
    if (!config || !config->test_globs || !file)
        return false;
    if (cbm_gitignore_matches(config->test_globs, file, false))
        return true;
    if (!strchr(file, '/'))
        return false;
    /* The discovery walker normally tests each directory on its way down.
     * A graph file path needs the same ancestor checks, including patterns
     * ending in '/'. Positive patterns make these checks order-independent. */
    CBMArena scratch;
    cbm_arena_init_lazy(&scratch, 4096);
    char *copy = cbm_arena_strdup(&scratch, file);
    bool matched = false;
    if (copy) {
        for (char *slash = strchr(copy, '/'); slash; slash = strchr(slash + 1, '/')) {
            *slash = '\0';
            matched = cbm_gitignore_matches(config->test_globs, copy, true);
            *slash = '/';
            if (matched)
                break;
        }
    }
    cbm_arena_destroy(&scratch);
    /* Allocation failure retains the seed, the conservative side of this
     * product-only display/traversal filter. It cannot justify omitting tests. */
    return matched;
}

const char *cbm_test_config_digest(const cbm_test_config_t *config) {
    return config ? config->digest : "";
}

const char *cbm_test_config_source(const cbm_test_config_t *config, size_t *len, bool *absent) {
    if (len)
        *len = config ? config->source_len : 0;
    if (absent)
        *absent = config && config->source_absent;
    return config ? config->source : NULL;
}
