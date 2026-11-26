/* Immutable test-convention declarations from one project JSON snapshot. */
#include "discover/test_conventions.h"
#include "foundation/arena.h"
#include "foundation/sha256.h"
#include <limits.h>
#include <stdint.h>
#include <string.h>
#include <yyjson/yyjson.h>

enum { TD_MAX_BYTES = 65536, TD_MAX_NAME_ARGS = 64 };

struct cbm_test_declarations {
    CBMArena arena;
    cbm_test_declaration_t *items;
    int count;
    bool configured;
    bool presets[CBM_TEST_PRESET_COUNT];
    char digest[CBM_SHA256_HEX_LEN + 1];
};

static yyjson_val *td_field(yyjson_val *object, const char *name, bool *valid) {
    if (!yyjson_is_obj(object)) {
        *valid = false;
        return NULL;
    }
    yyjson_val *found = NULL, *key, *value;
    size_t index, maximum, len = strlen(name);
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

static const char *td_string(yyjson_val *value, size_t *len) {
    const char *text = yyjson_get_str(value);
    size_t size = yyjson_get_len(value);
    if (!text || !size || memchr(text, '\0', size) || memchr(text, '\n', size) ||
        memchr(text, '\r', size))
        return NULL;
    if (len)
        *len = size;
    return text;
}

static bool td_identifier_first(unsigned char ch) {
    return ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
}

static bool td_identifier(const char *text, size_t len) {
    if (!len || !td_identifier_first((unsigned char)text[0]))
        return false;
    for (size_t i = 1; i < len; i++) {
        unsigned char ch = (unsigned char)text[i];
        if (!td_identifier_first(ch) && !(ch >= '0' && ch <= '9'))
            return false;
    }
    return true;
}

static bool td_copy_string(cbm_test_declarations_t *declarations, yyjson_val *value,
                           bool identifier, const char **result) {
    size_t len = 0;
    const char *text = td_string(value, &len);
    if (!text || (identifier && !td_identifier(text, len)))
        return false;
    *result = cbm_arena_strndup(&declarations->arena, text, len);
    return *result != NULL;
}

static bool td_argument(yyjson_val *value, int *result) {
    if (!yyjson_is_uint(value) || yyjson_get_uint(value) > INT_MAX)
        return false;
    *result = (int)yyjson_get_uint(value);
    return true;
}

static bool td_name_args(cbm_test_declarations_t *declarations, yyjson_val *value,
                         cbm_test_declaration_t *item) {
    if (!yyjson_is_arr(value))
        return false;
    size_t count = yyjson_arr_size(value);
    if (!count || count > TD_MAX_NAME_ARGS)
        return false;
    int *args = cbm_arena_alloc(&declarations->arena, count * sizeof(*args));
    if (!args)
        return false;
    size_t index, maximum;
    yyjson_val *argument;
    yyjson_arr_foreach(value, index, maximum, argument) {
        if (!td_argument(argument, &args[index]))
            return false;
    }
    /* Ordered duplicates carry meaning for an adapter, so retain them. */
    item->name_args = args;
    item->name_arg_count = (int)count;
    return true;
}

static bool td_record(cbm_test_declarations_t *declarations, yyjson_val *record,
                      cbm_test_declaration_t *item) {
    bool valid = true;
    yyjson_val *language = td_field(record, "language", &valid);
    yyjson_val *role = td_field(record, "role", &valid);
    yyjson_val *define_macro = td_field(record, "define_macro", &valid);
    yyjson_val *name_args = td_field(record, "name_args", &valid);
    yyjson_val *runner_id = td_field(record, "runner_id", &valid);
    yyjson_val *macro = td_field(record, "macro", &valid);
    yyjson_val *test_arg = td_field(record, "test_arg", &valid);
    yyjson_val *suite_arg = td_field(record, "suite_arg", &valid);
    yyjson_val *perf_macro = td_field(record, "perf_macro", &valid);
    const char *role_text = td_string(role, NULL);
    if (!valid || !role_text || !td_copy_string(declarations, language, false, &item->language))
        return false;
    item->test_arg = -1;
    item->suite_arg = -1;
    if (strcmp(role_text, "case") == 0 || strcmp(role_text, "suite") == 0) {
        bool is_case = strcmp(role_text, "case") == 0;
        item->role = is_case ? CBM_TEST_DECL_CASE : CBM_TEST_DECL_SUITE;
        if (macro || test_arg || suite_arg || perf_macro || (!is_case && runner_id))
            return false;
        return td_copy_string(declarations, define_macro, true, &item->define_macro) &&
               td_name_args(declarations, name_args, item) &&
               (!runner_id || td_copy_string(declarations, runner_id, false, &item->runner_id));
    }
    if (strcmp(role_text, "registration") == 0) {
        item->role = CBM_TEST_DECL_REGISTRATION;
        if (define_macro || name_args || runner_id || suite_arg || perf_macro)
            return false;
        return td_copy_string(declarations, macro, true, &item->macro) &&
               td_argument(test_arg, &item->test_arg);
    }
    if (strcmp(role_text, "suite_registration") == 0) {
        item->role = CBM_TEST_DECL_SUITE_REGISTRATION;
        if (define_macro || name_args || runner_id || test_arg)
            return false;
        return td_copy_string(declarations, macro, true, &item->macro) &&
               td_argument(suite_arg, &item->suite_arg) &&
               (!perf_macro || td_copy_string(declarations, perf_macro, true, &item->perf_macro));
    }
    return false;
}

static bool td_conventions(cbm_test_declarations_t *declarations, yyjson_val *value) {
    if (!yyjson_is_arr(value))
        return false;
    size_t count = yyjson_arr_size(value);
    if (count > INT_MAX || count > SIZE_MAX / sizeof(*declarations->items))
        return false;
    if (!count)
        return true;
    declarations->items =
        cbm_arena_calloc(&declarations->arena, count * sizeof(*declarations->items));
    if (!declarations->items)
        return false;
    size_t index, maximum;
    yyjson_val *record;
    yyjson_arr_foreach(value, index, maximum, record) {
        if (!td_record(declarations, record, &declarations->items[index]))
            return false;
    }
    declarations->count = (int)count;
    return true;
}

static bool td_presets(cbm_test_declarations_t *declarations, yyjson_val *value) {
    static const char *const names[CBM_TEST_PRESET_COUNT] = {
        [CBM_TEST_PRESET_C_CBM] = "c-cbm", [CBM_TEST_PRESET_PYTEST] = "pytest",
        [CBM_TEST_PRESET_GO] = "go",       [CBM_TEST_PRESET_JUNIT] = "junit",
        [CBM_TEST_PRESET_GTEST] = "gtest", [CBM_TEST_PRESET_RUST] = "rust",
        [CBM_TEST_PRESET_JEST] = "jest"};
    if (!yyjson_is_obj(value))
        return false;
    bool seen[CBM_TEST_PRESET_COUNT] = {false};
    size_t index, maximum;
    yyjson_val *key, *enabled;
    yyjson_obj_foreach(value, index, maximum, key, enabled) {
        int preset = -1;
        size_t len = yyjson_get_len(key);
        const char *text = yyjson_get_str(key);
        for (int candidate = 0; candidate < CBM_TEST_PRESET_COUNT; candidate++) {
            if (len == strlen(names[candidate]) && memcmp(text, names[candidate], len) == 0) {
                preset = candidate;
                break;
            }
        }
        /* An unknown switch is an unsupported semantic request, not an
         * unrelated future field whose intent can safely remain unconsumed. */
        if (preset < 0 || seen[preset] || !yyjson_is_bool(enabled))
            return false;
        seen[preset] = true;
        declarations->presets[preset] = yyjson_get_bool(enabled);
    }
    return true;
}

static bool td_project(cbm_test_declarations_t *declarations, yyjson_val *root) {
    bool valid = true;
    yyjson_val *impact = td_field(root, "test_impact", &valid);
    if (!valid || !impact)
        return valid;
    yyjson_val *version = td_field(impact, "version", &valid);
    yyjson_val *tests = td_field(impact, "tests", &valid);
    if (!valid || !yyjson_is_uint(version) || yyjson_get_uint(version) != 1)
        return false;
    declarations->configured = true;
    if (!tests)
        return true;
    yyjson_val *conventions = td_field(tests, "conventions", &valid);
    yyjson_val *presets = td_field(tests, "presets", &valid);
    if (!valid)
        return false;
    return (!conventions || td_conventions(declarations, conventions)) &&
           (!presets || td_presets(declarations, presets));
}

static void td_digest(cbm_test_declarations_t *declarations, const char *bytes, size_t len,
                      bool absent) {
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
        declarations->digest[i * 2] = hex[digest[i] >> 4];
        declarations->digest[i * 2 + 1] = hex[digest[i] & 15];
    }
    declarations->digest[CBM_SHA256_HEX_LEN] = '\0';
}

cbm_test_declarations_t *cbm_test_declarations_parse(const char *bytes, size_t len, bool absent) {
    if (len > TD_MAX_BYTES || (absent && len) || (!absent && (!bytes || !len)) ||
        (len && memchr(bytes, '\0', len)))
        return NULL;
    CBMArena arena;
    cbm_arena_init_lazy(&arena, 4096);
    cbm_test_declarations_t *declarations = cbm_arena_calloc(&arena, sizeof(*declarations));
    if (!declarations) {
        cbm_arena_destroy(&arena);
        return NULL;
    }
    declarations->arena = arena;
    for (int preset = 0; preset < CBM_TEST_PRESET_COUNT; preset++)
        declarations->presets[preset] = preset != CBM_TEST_PRESET_C_CBM;
    bool valid = true;
    if (!absent) {
        yyjson_doc *doc = yyjson_read(bytes, len, 0);
        if (!doc) {
            valid = false;
        } else {
            valid = td_project(declarations, yyjson_doc_get_root(doc));
            yyjson_doc_free(doc);
        }
    }
    if (!valid) {
        cbm_test_declarations_free(declarations);
        return NULL;
    }
    td_digest(declarations, bytes, len, absent);
    return declarations;
}

void cbm_test_declarations_free(cbm_test_declarations_t *declarations) {
    if (!declarations)
        return;
    CBMArena arena = declarations->arena;
    cbm_arena_destroy(&arena);
}

bool cbm_test_declarations_configured(const cbm_test_declarations_t *declarations) {
    return declarations && declarations->configured;
}

const cbm_test_declaration_t *cbm_test_declarations_items(
    const cbm_test_declarations_t *declarations, int *count) {
    if (count)
        *count = declarations ? declarations->count : 0;
    return declarations ? declarations->items : NULL;
}

bool cbm_test_declarations_preset(const cbm_test_declarations_t *declarations,
                                  cbm_test_preset_t preset, bool *enabled) {
    if (enabled)
        *enabled = false;
    if (!declarations || !enabled || (unsigned)preset >= CBM_TEST_PRESET_COUNT)
        return false;
    *enabled = declarations->presets[preset];
    return true;
}

const char *cbm_test_declarations_digest(const cbm_test_declarations_t *declarations) {
    return declarations ? declarations->digest : "";
}
