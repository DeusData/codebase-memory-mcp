/* Coverage-map formats 1 and 2: strict syntax/reference validation. Admission of
 * an artifact (provenance, metadata, age, ancestry) belongs to the caller. */
#include "mcp/test_impact.h"

#include "foundation/arena.h"
#include "foundation/sha256.h"
#include <yyjson/yyjson.h>

#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct cbm_coverage_map {
    CBMArena arena;
    cbm_coverage_format_t format;
    cbm_coverage_profile_binding_t profile_binding;
    cbm_coverage_profile_t *profiles;
    cbm_coverage_function_t *functions;             /* ID order is never changed */
    const cbm_coverage_function_t **function_order; /* source/name lookup */
    int function_count;
    cbm_coverage_test_t *tests;
    int test_count;
    char functions_sha256[CBM_SHA256_HEX_LEN + 1];
    char tests_sha256[CBM_SHA256_HEX_LEN + 1];
};

static void cv_hash_bytes(const char *bytes, size_t len, char hex[CBM_SHA256_HEX_LEN + 1]) {
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    if (len)
        cbm_sha256_update(&hash, bytes, len);
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&hash, digest);
    static const char digits[] = "0123456789abcdef";
    for (int i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
        hex[i * 2] = digits[digest[i] >> 4];
        hex[i * 2 + 1] = digits[digest[i] & 15];
    }
    hex[CBM_SHA256_HEX_LEN] = '\0';
}

static bool cv_records(const char *text, size_t len, int *count) {
    *count = 0;
    if ((!text && len) || len == SIZE_MAX ||
        (len && (text[len - 1] != '\n' || memchr(text, '\0', len)))) {
        return false;
    }
    for (size_t i = 0; i < len; i++) {
        if (text[i] == '\n') {
            if (*count == INT_MAX) {
                return false;
            }
            (*count)++;
        }
    }
    return true;
}

static void *cv_array(cbm_coverage_map_t *map, int count, size_t size) {
    if (count <= 0 || (size_t)count > SIZE_MAX / size) {
        return NULL;
    }
    return cbm_arena_calloc(&map->arena, (size_t)count * size);
}

/* Records were counted before copying. Keep empty TSV fields and trim only a
 * record-ending CR, as emitted by the Python builder on Windows. */
static char *cv_line(char **cursor) {
    char *line = *cursor;
    char *end = strchr(line, '\n');
    if (!end) {
        return NULL;
    }
    *cursor = end + 1;
    *end = '\0';
    if (end > line && end[-1] == '\r') {
        end[-1] = '\0';
    }
    return line;
}

static bool cv_fields(char *line, char **fields, int count) {
    if (!line) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        fields[i] = line;
        char *tab = strchr(line, '\t');
        if (i + 1 == count) {
            return tab == NULL;
        }
        if (!tab) {
            return false;
        }
        *tab = '\0';
        line = tab + 1;
    }
    return false;
}

static bool cv_decimal(const char *text, int *value, const char **end) {
    if (*text < '0' || *text > '9') {
        return false;
    }
    int number = 0;
    while (*text >= '0' && *text <= '9') {
        int digit = *text++ - '0';
        if (number > (INT_MAX - digit) / 10) {
            return false;
        }
        number = number * 10 + digit;
    }
    *value = number;
    *end = text;
    return true;
}

static int cv_function_compare(const void *left, const void *right) {
    const cbm_coverage_function_t *a = *(const cbm_coverage_function_t *const *)left;
    const cbm_coverage_function_t *b = *(const cbm_coverage_function_t *const *)right;
    int cmp = strcmp(a->file, b->file);
    return cmp ? cmp : strcmp(a->name, b->name);
}

static int cv_test_compare(const void *left, const void *right) {
    const cbm_coverage_test_t *a = left;
    const cbm_coverage_test_t *b = right;
    int cmp = strcmp(a->suite, b->suite);
    return cmp ? cmp : strcmp(a->name, b->name);
}

static bool cv_functions(cbm_coverage_map_t *map, char *text) {
    for (int i = 0; i < map->function_count; i++) {
        char *fields[3];
        int id;
        const char *end;
        if (!cv_fields(cv_line(&text), fields, 3) || !cv_decimal(fields[0], &id, &end) || *end ||
            id != i || !fields[2][0]) {
            return false;
        }
        map->functions[i] =
            (cbm_coverage_function_t){.id = id, .file = fields[1], .name = fields[2]};
        map->function_order[i] = &map->functions[i];
    }
    if (map->function_count > 1) {
        qsort(map->function_order, (size_t)map->function_count, sizeof(*map->function_order),
              cv_function_compare);
        for (int i = 1; i < map->function_count; i++) {
            if (cv_function_compare(&map->function_order[i - 1], &map->function_order[i]) == 0) {
                return false;
            }
        }
    }
    return true;
}

static bool cv_ids(cbm_coverage_map_t *map, cbm_coverage_test_t *row, const char *text,
                   bool *seen) {
    if (!*text) {
        return true;
    }
    int count = 1;
    for (const char *p = text; *p; p++) {
        if (*p == ' ') {
            if (count == INT_MAX) {
                return false;
            }
            count++;
        }
    }
    /* A row cannot contain more unique IDs than there are function records.
     * Allocate from actual input tokens, never from a claimed numeric ID. */
    if (count > map->function_count) {
        return false;
    }
    int *ids = cv_array(map, count, sizeof(*ids));
    if (!ids) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        const char *end;
        if (!cv_decimal(text, &ids[i], &end) || ids[i] >= map->function_count ||
            (i > 0 && ids[i] <= ids[i - 1])) {
            return false;
        }
        seen[ids[i]] = true;
        if (i + 1 < count) {
            if (*end != ' ') {
                return false;
            }
            text = end + 1;
        } else if (*end) {
            return false;
        }
    }
    row->function_ids = ids;
    row->function_count = count;
    return true;
}

static bool cv_tests(cbm_coverage_map_t *map, char *text, bool *seen) {
    for (int i = 0; i < map->test_count; i++) {
        char *fields[4];
        if (!cv_fields(cv_line(&text), fields, 4)) {
            return false;
        }
        char *colon = strchr(fields[0], ':');
        if (!colon || colon == fields[0] || !colon[1]) {
            return false;
        }
        *colon = '\0';
        bool complete = strcmp(fields[1], "complete") == 0;
        if ((!complete && strcmp(fields[1], "incomplete") != 0) || (complete && fields[2][0]) ||
            (!complete && !fields[2][0])) {
            return false;
        }
        cbm_coverage_test_t *row = &map->tests[i];
        *row = (cbm_coverage_test_t){
            .suite = fields[0], .name = colon + 1, .complete = complete, .reason = fields[2]};
        if (!cv_ids(map, row, fields[3], seen)) {
            return false;
        }
    }
    if (map->test_count > 1) {
        qsort(map->tests, (size_t)map->test_count, sizeof(*map->tests), cv_test_compare);
    }
    bool setup = false;
    for (int i = 0; i < map->test_count; i++) {
        if (i == 0 || strcmp(map->tests[i - 1].suite, map->tests[i].suite) != 0) {
            if (i > 0 && !setup) {
                return false;
            }
            setup = false;
        } else if (cv_test_compare(&map->tests[i - 1], &map->tests[i]) == 0) {
            return false;
        }
        setup = setup || strcmp(map->tests[i].name, "*") == 0;
    }
    if (map->test_count && !setup) {
        return false;
    }
    /* The builder emits only functions executed by at least one test/setup.
     * An orphan could otherwise be mistaken for known, unneeded coverage. */
    for (int i = 0; i < map->function_count; i++) {
        if (!seen[i]) {
            return false;
        }
    }
    return true;
}

cbm_coverage_map_t *cbm_coverage_map_parse(const char *functions, size_t functions_len,
                                           const char *tests, size_t tests_len) {
    int function_count;
    int test_count;
    if (!cv_records(functions, functions_len, &function_count) ||
        !cv_records(tests, tests_len, &test_count)) {
        return NULL;
    }
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_coverage_map_t *map = cbm_arena_calloc(&arena, sizeof(*map));
    if (!map) {
        cbm_arena_destroy(&arena);
        return NULL;
    }
    map->arena = arena;
    map->format = CBM_COVERAGE_FORMAT_FUNCTIONS;
    map->function_count = function_count;
    map->test_count = test_count;
    map->functions = cv_array(map, function_count, sizeof(*map->functions));
    map->function_order = cv_array(map, function_count, sizeof(*map->function_order));
    map->tests = cv_array(map, test_count, sizeof(*map->tests));
    bool *seen = cv_array(map, function_count, sizeof(*seen));
    char *function_copy = cbm_arena_strndup(&map->arena, functions ? functions : "", functions_len);
    char *test_copy = cbm_arena_strndup(&map->arena, tests ? tests : "", tests_len);
    if ((function_count && (!map->functions || !map->function_order || !seen)) ||
        (test_count && !map->tests) || !function_copy || !test_copy ||
        !cv_functions(map, function_copy) || !cv_tests(map, test_copy, seen)) {
        cbm_coverage_map_free(map);
        return NULL;
    }
    cv_hash_bytes(functions, functions_len, map->functions_sha256);
    cv_hash_bytes(tests, tests_len, map->tests_sha256);
    return map;
}

void cbm_coverage_map_free(cbm_coverage_map_t *map) {
    if (map) {
        CBMArena arena = map->arena;
        cbm_arena_destroy(&arena);
    }
}

const char *cbm_coverage_map_functions_sha256(const cbm_coverage_map_t *map) {
    if (map && map->format != CBM_COVERAGE_FORMAT_FUNCTIONS)
        return NULL;
    return map ? map->functions_sha256 : NULL;
}

const cbm_coverage_function_t *cbm_coverage_map_functions(const cbm_coverage_map_t *map,
                                                          int *count) {
    if (map && map->format != CBM_COVERAGE_FORMAT_FUNCTIONS) {
        if (count)
            *count = 0;
        return NULL;
    }
    if (count) {
        *count = map ? map->function_count : 0;
    }
    return map ? map->functions : NULL;
}

const cbm_coverage_test_t *cbm_coverage_map_tests(const cbm_coverage_map_t *map, int *count) {
    if (count) {
        *count = map ? map->test_count : 0;
    }
    return map ? map->tests : NULL;
}

const cbm_coverage_function_t *cbm_coverage_map_find_function(const cbm_coverage_map_t *map,
                                                              const char *file, const char *name) {
    if (!map || map->format != CBM_COVERAGE_FORMAT_FUNCTIONS || !file || !name) {
        return NULL;
    }
    cbm_coverage_function_t key = {.file = file, .name = name};
    const cbm_coverage_function_t *key_ptr = &key;
    int lo = 0;
    int hi = map->function_count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        int cmp = cv_function_compare(&key_ptr, &map->function_order[mid]);
        if (cmp == 0) {
            return map->function_order[mid];
        }
        if (cmp < 0) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return NULL;
}

const cbm_coverage_test_t *cbm_coverage_map_find_test(const cbm_coverage_map_t *map,
                                                      const char *suite, const char *name) {
    if (!map || !suite || !name) {
        return NULL;
    }
    cbm_coverage_test_t key = {.suite = suite, .name = name};
    int lo = 0;
    int hi = map->test_count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        int cmp = cv_test_compare(&key, &map->tests[mid]);
        if (cmp == 0) {
            return &map->tests[mid];
        }
        if (cmp < 0) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return NULL;
}

bool cbm_coverage_test_intersects(const cbm_coverage_test_t *test, const int *changed_ids,
                                  int changed_count) {
    if (!test || !changed_ids) {
        return false;
    }
    for (int i = 0; i < changed_count; i++) {
        int lo = 0;
        int hi = test->function_count;
        while (lo < hi) {
            int mid = lo + (hi - lo) / 2;
            int id = test->function_ids[mid];
            if (id == changed_ids[i]) {
                return true;
            }
            if (id > changed_ids[i]) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
    }
    return false;
}

/* Coverage metadata consistency. */
typedef struct {
    const char *name;
    int tests;
    int incomplete;
    bool setup_complete;
    bool seen;
} cv_meta_suite_t;

/* Known fields must be unambiguous. Unknown fields are ignored for forward
 * compatibility; repeated known keys are invalid even when values agree. */
static yyjson_val *cv_meta_field(yyjson_val *object, const char *name, bool *valid) {
    if (!yyjson_is_obj(object)) {
        *valid = false;
        return NULL;
    }
    yyjson_val *result = NULL;
    size_t index, maximum;
    yyjson_val *key, *value;
    size_t len = strlen(name);
    yyjson_obj_foreach(object, index, maximum, key, value) {
        if (yyjson_get_len(key) == len && memcmp(yyjson_get_str(key), name, len) == 0) {
            if (result) {
                *valid = false;
                return NULL;
            }
            result = value;
        }
    }
    return result;
}

static bool cv_meta_count(yyjson_val *value, int *out) {
    if (yyjson_is_uint(value)) {
        uint64_t count = yyjson_get_uint(value);
        if (count > INT_MAX)
            return false;
        *out = (int)count;
        return true;
    }
    if (yyjson_is_sint(value)) {
        int64_t count = yyjson_get_sint(value);
        if (count < 0 || count > INT_MAX)
            return false;
        *out = (int)count;
        return true;
    }
    return false;
}

static bool cv_meta_count_equals(yyjson_val *value, int expected) {
    int count;
    return cv_meta_count(value, &count) && count == expected;
}

static const char *cv_meta_string(yyjson_val *value) {
    const char *text = yyjson_get_str(value);
    return text && *text && strlen(text) == yyjson_get_len(value) ? text : NULL;
}

static bool cv_meta_commit(const char *commit) {
    if (!commit)
        return false;
    size_t len = strlen(commit);
    if (len != 40 && len != 64)
        return false;
    for (size_t i = 0; i < len; i++) {
        char c = commit[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
            return false;
        }
    }
    return true;
}

static cv_meta_suite_t *cv_meta_find_suite(cv_meta_suite_t *suites, int count, const char *name) {
    int lo = 0, hi = count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        int cmp = strcmp(name, suites[mid].name);
        if (!cmp)
            return &suites[mid];
        if (cmp < 0)
            hi = mid;
        else
            lo = mid + 1;
    }
    return NULL;
}

bool cbm_coverage_map_metadata_matches(const cbm_coverage_map_t *map, const char *metadata,
                                       size_t len, const char *expected_commit) {
    if (!map || map->format != CBM_COVERAGE_FORMAT_FUNCTIONS || !metadata || len == SIZE_MAX ||
        !cv_meta_commit(expected_commit) || memchr(metadata, '\0', len)) {
        return false;
    }
    yyjson_doc *doc = yyjson_read(metadata, len, 0);
    if (!doc)
        return false;
    CBMArena arena;
    cbm_arena_init_lazy(&arena, 4096);
    bool result = false;
    bool valid = true;
    int suite_count = 0, test_count = 0, incomplete_count = 0;
    for (int i = 0; i < map->test_count; i++) {
        if (i == 0 || strcmp(map->tests[i - 1].suite, map->tests[i].suite) != 0)
            suite_count++;
        if (strcmp(map->tests[i].name, "*") != 0) {
            test_count++;
            incomplete_count += !map->tests[i].complete;
        }
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!cv_meta_count_equals(cv_meta_field(root, "format", &valid), 1) ||
        !cv_meta_count_equals(cv_meta_field(root, "functions", &valid), map->function_count) ||
        !cv_meta_count_equals(cv_meta_field(root, "tests", &valid), test_count) ||
        !cv_meta_count_equals(cv_meta_field(root, "incomplete", &valid), incomplete_count))
        goto done;
    const char *commit = cv_meta_string(cv_meta_field(root, "commit", &valid));
    if (!commit || strcmp(commit, expected_commit) != 0 ||
        !cv_meta_string(cv_meta_field(root, "platform", &valid)) ||
        !cv_meta_string(cv_meta_field(root, "llvm_profdata", &valid)))
        goto done;
    yyjson_val *compiled = cv_meta_field(root, "functions_compiled", &valid);
    int compiled_count;
    /* Diagnostic only: fallback profile functions can be absent from the
     * runner's source table, so this need not bound the executed functions. */
    if (compiled && !cv_meta_count(compiled, &compiled_count))
        goto done;
    yyjson_val *entries = cv_meta_field(root, "suites", &valid);
    if (!valid || !yyjson_is_arr(entries) || yyjson_arr_size(entries) != (size_t)suite_count ||
        (size_t)suite_count > SIZE_MAX / sizeof(cv_meta_suite_t))
        goto done;
    cv_meta_suite_t *suites =
        suite_count ? cbm_arena_calloc(&arena, (size_t)suite_count * sizeof(*suites)) : NULL;
    if (suite_count && !suites)
        goto done;
    int at = -1;
    for (int i = 0; i < map->test_count; i++) {
        const cbm_coverage_test_t *row = &map->tests[i];
        if (i == 0 || strcmp(map->tests[i - 1].suite, row->suite) != 0) {
            suites[++at].name = row->suite;
        }
        if (strcmp(row->name, "*") == 0) {
            suites[at].setup_complete = row->complete;
        } else {
            suites[at].tests++;
            suites[at].incomplete += !row->complete;
        }
    }
    size_t index, maximum;
    yyjson_val *entry;
    yyjson_arr_foreach(entries, index, maximum, entry) {
        const char *name = cv_meta_string(cv_meta_field(entry, "suite", &valid));
        if (!name)
            goto done;
        cv_meta_suite_t *suite = cv_meta_find_suite(suites, suite_count, name);
        if (!suite || suite->seen ||
            !cv_meta_count_equals(cv_meta_field(entry, "tests", &valid), suite->tests) ||
            !cv_meta_count_equals(cv_meta_field(entry, "incomplete", &valid), suite->incomplete))
            goto done;
        yyjson_val *exit = cv_meta_field(entry, "exit", &valid);
        if (!yyjson_is_int(exit))
            goto done;
        if (yyjson_get_uint(exit) != 0 &&
            (suite->incomplete != suite->tests || suite->setup_complete))
            goto done;
        yyjson_val *wall = cv_meta_field(entry, "wall_s", &valid);
        if (wall && (!yyjson_is_num(wall) || yyjson_get_num(wall) < 0))
            goto done;
        if (!valid)
            goto done;
        suite->seen = true;
    }
    result = true;
done:
    cbm_arena_destroy(&arena);
    yyjson_doc_free(doc);
    return result;
}

static bool cv_receipt_digest(const char *text) {
    if (!text || strlen(text) != CBM_SHA256_HEX_LEN)
        return false;
    for (int i = 0; i < CBM_SHA256_HEX_LEN; i++)
        if (!((text[i] >= '0' && text[i] <= '9') || (text[i] >= 'a' && text[i] <= 'f')))
            return false;
    return true;
}

unsigned cbm_coverage_map_check_receipt(const cbm_coverage_map_t *map, const char *metadata,
                                        size_t metadata_len, const cbm_coverage_receipt_t *receipt,
                                        const cbm_coverage_receipt_context_t *context) {
    if (map && map->format != CBM_COVERAGE_FORMAT_FUNCTIONS)
        return CBM_COVERAGE_RECEIPT_INVALID | CBM_COVERAGE_RECEIPT_METADATA;
    if (!map || !metadata || metadata_len == SIZE_MAX || !receipt || !context ||
        !cv_meta_commit(receipt->commit) || !cv_meta_commit(context->trusted_commit) ||
        !cv_meta_commit(context->graph_commit) || !cv_meta_commit(context->merge_base) ||
        !cv_meta_commit(context->ancestor_commit) ||
        !cv_meta_commit(context->ancestor_merge_base) ||
        !cv_receipt_digest(receipt->functions_sha256) ||
        !cv_receipt_digest(receipt->tests_sha256) || !cv_receipt_digest(receipt->metadata_sha256) ||
        !cv_receipt_digest(receipt->graph_sha256) ||
        !cv_receipt_digest(receipt->compatibility_sha256) ||
        !cv_receipt_digest(context->graph_sha256) ||
        !cv_receipt_digest(context->compatibility_sha256))
        return CBM_COVERAGE_RECEIPT_INVALID;
    unsigned reasons = 0;
    if (!context->source_verified)
        reasons |= CBM_COVERAGE_RECEIPT_SOURCE;
    if (!context->ancestor_verified || strcmp(receipt->commit, context->ancestor_commit) != 0 ||
        strcmp(context->merge_base, context->ancestor_merge_base) != 0)
        reasons |= CBM_COVERAGE_RECEIPT_ANCESTRY;
    if (strcmp(receipt->commit, context->trusted_commit) != 0 ||
        strcmp(receipt->commit, context->graph_commit) != 0)
        reasons |= CBM_COVERAGE_RECEIPT_COMMIT;
    char metadata_sha256[CBM_SHA256_HEX_LEN + 1];
    cv_hash_bytes(metadata, metadata_len, metadata_sha256);
    if (strcmp(map->functions_sha256, receipt->functions_sha256) != 0 ||
        strcmp(map->tests_sha256, receipt->tests_sha256) != 0 ||
        strcmp(metadata_sha256, receipt->metadata_sha256) != 0 ||
        strcmp(context->graph_sha256, receipt->graph_sha256) != 0)
        reasons |= CBM_COVERAGE_RECEIPT_CONTENT;
    if (context->now < 0 || receipt->oldest_observation_at <= 0 ||
        receipt->oldest_observation_at > context->now ||
        context->now - receipt->oldest_observation_at > 604800)
        reasons |= CBM_COVERAGE_RECEIPT_AGE;
    if (strcmp(receipt->compatibility_sha256, context->compatibility_sha256) != 0)
        reasons |= CBM_COVERAGE_RECEIPT_COMPATIBILITY;
    if (!cbm_coverage_map_metadata_matches(map, metadata, metadata_len, context->trusted_commit))
        reasons |= CBM_COVERAGE_RECEIPT_METADATA;
    return reasons;
}

/* Format 2 is deliberately separate from the legacy parser. The context owns
 * its arena until the final publish; no error can expose a partial map. */
typedef struct {
    const unsigned char *data;
    size_t length;
} cv2_span_t;

typedef struct {
    CBMArena arena;
    const cbm_coverage_parse_limits_t *limits;
    cbm_coverage_parse_cancel_fn cancelled;
    void *cancel_context;
    cbm_coverage_parse_status_t status;
    size_t allocated;
    uint64_t items;
    unsigned work;
    int profile_count;
    int test_count;
    size_t profile_start;
    cbm_coverage_profile_binding_t binding;
    cbm_coverage_map_t *map;
} cv2_context_t;

static bool cv2_fail(cv2_context_t *ctx, cbm_coverage_parse_status_t status) {
    if (ctx->status == CBM_COVERAGE_PARSE_OK)
        ctx->status = status;
    return false;
}

static bool cv2_poll(cv2_context_t *ctx) {
    if (ctx->status != CBM_COVERAGE_PARSE_OK)
        return false;
    ctx->work = 0;
    if (ctx->cancelled && ctx->cancelled(ctx->cancel_context))
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_CANCELLED);
    return true;
}

static bool cv2_work(cv2_context_t *ctx, unsigned units) {
    ctx->work += units;
    if (ctx->work >= 4096)
        return cv2_poll(ctx);
    return ctx->status == CBM_COVERAGE_PARSE_OK;
}

static bool cv2_step(cv2_context_t *ctx) {
    return cv2_work(ctx, 1);
}

static bool cv2_items(cv2_context_t *ctx, uint64_t count) {
    if (count > ctx->limits->max_items - ctx->items)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_LIMIT);
    ctx->items += count;
    return true;
}

/* Chunk all explicit bulk work; allocator internals are not a polling API. */
static bool cv2_copy(cv2_context_t *ctx, void *to, const void *from, size_t length) {
    unsigned char *dst = to;
    const unsigned char *src = from;
    size_t at = 0;
    while (at < length) {
        if (!cv2_poll(ctx))
            return false;
        size_t chunk = length - at;
        if (chunk > 4096)
            chunk = 4096;
        memcpy(dst + at, src + at, chunk);
        at += chunk;
    }
    return cv2_poll(ctx);
}

static bool cv2_zero(cv2_context_t *ctx, void *memory, size_t length) {
    unsigned char *bytes = memory;
    size_t at = 0;
    while (at < length) {
        if (!cv2_poll(ctx))
            return false;
        size_t chunk = length - at;
        if (chunk > 4096)
            chunk = 4096;
        memset(bytes + at, 0, chunk);
        at += chunk;
    }
    return cv2_poll(ctx);
}

static void *cv2_array(cv2_context_t *ctx, size_t count, size_t size) {
    if (!count)
        return NULL;
    if (size && count > SIZE_MAX / size) {
        cv2_fail(ctx, CBM_COVERAGE_PARSE_LIMIT);
        return NULL;
    }
    size_t bytes = count * size;
    if (bytes > ctx->limits->max_alloc_bytes - ctx->allocated) {
        cv2_fail(ctx, CBM_COVERAGE_PARSE_LIMIT);
        return NULL;
    }
    if (!cv2_poll(ctx))
        return NULL;
    ctx->allocated += bytes;
    void *memory = cbm_arena_alloc(&ctx->arena, bytes);
    if (!memory) {
        cv2_fail(ctx, CBM_COVERAGE_PARSE_OOM);
        return NULL;
    }
    return cv2_zero(ctx, memory, bytes) ? memory : NULL;
}

static char *cv2_string(cv2_context_t *ctx, cv2_span_t span) {
    if (span.length == SIZE_MAX) {
        cv2_fail(ctx, CBM_COVERAGE_PARSE_LIMIT);
        return NULL;
    }
    char *text = cv2_array(ctx, span.length + 1, 1);
    if (!text || !cv2_copy(ctx, text, span.data, span.length))
        return NULL;
    return text;
}

static bool cv2_line(cv2_context_t *ctx, cv2_span_t input, size_t *position, bool test_row,
                     cv2_span_t *line) {
    size_t start = *position;
    while (*position < input.length) {
        if (!cv2_step(ctx))
            return false;
        unsigned char byte = input.data[(*position)++];
        if (!byte || (!test_row && byte == '\r'))
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
        if (byte == '\n') {
            size_t length = *position - start - 1;
            if (test_row && length && input.data[start + length - 1] == '\r')
                length--;
            *line = (cv2_span_t){input.data + start, length};
            return true;
        }
    }
    return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
}

static bool cv2_fields(cv2_context_t *ctx, cv2_span_t line, cv2_span_t fields[4]) {
    size_t start = 0;
    int field = 0;
    for (size_t i = 0; i < line.length; i++) {
        if (!cv2_step(ctx))
            return false;
        if (line.data[i] != '\t')
            continue;
        if (field == 3)
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
        fields[field++] = (cv2_span_t){line.data + start, i - start};
        start = i + 1;
    }
    if (field != 3)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    fields[3] = (cv2_span_t){line.data + start, line.length - start};
    return true;
}

static bool cv2_row(cv2_context_t *ctx, cv2_span_t input, size_t *position, bool test_row,
                    cv2_span_t fields[4]) {
    cv2_span_t line;
    return cv2_line(ctx, input, position, test_row, &line) && cv2_fields(ctx, line, fields);
}

static bool cv2_literal(cv2_span_t span, const char *literal, size_t length) {
    return span.length == length && memcmp(span.data, literal, length) == 0;
}

static bool cv2_decimal(cv2_context_t *ctx, cv2_span_t span, bool canonical, uint64_t *out) {
    if (!span.length || (canonical && span.length > 1 && span.data[0] == '0'))
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    uint64_t value = 0;
    for (size_t i = 0; i < span.length; i++) {
        if (!cv2_step(ctx))
            return false;
        unsigned char byte = span.data[i];
        if (byte < '0' || byte > '9')
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
        unsigned digit = (unsigned)(byte - '0');
        if (value > (UINT64_MAX - digit) / 10)
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
        value = value * 10 + digit;
    }
    *out = value;
    return true;
}

static int cv2_hex_digit(unsigned char byte) {
    if (byte >= '0' && byte <= '9')
        return byte - '0';
    if (byte >= 'a' && byte <= 'f')
        return byte - 'a' + 10;
    return -1;
}

static bool cv2_hex64(cv2_context_t *ctx, cv2_span_t span, uint64_t *out) {
    if (span.length != 16)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    uint64_t value = 0;
    for (size_t i = 0; i < span.length; i++) {
        if (!cv2_step(ctx))
            return false;
        int digit = cv2_hex_digit(span.data[i]);
        if (digit < 0)
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
        value = (value << 4) | (unsigned)digit;
    }
    *out = value;
    return true;
}

static bool cv2_image(cv2_context_t *ctx, cv2_span_t span) {
    if (span.length != 64)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    for (size_t i = 0; i < span.length; i++) {
        if (!cv2_step(ctx))
            return false;
        if (cv2_hex_digit(span.data[i]) < 0)
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
        ctx->binding.image_sha256[i] = (char)span.data[i];
    }
    ctx->binding.image_sha256[64] = '\0';
    return true;
}

static bool cv2_header(cv2_context_t *ctx, cv2_span_t profiles) {
    cv2_span_t fields[4];
    uint64_t version, count;
    if (!cv2_row(ctx, profiles, &ctx->profile_start, false, fields))
        return false;
    if (!cv2_literal(fields[0], "CBM_PROFILE_MAP", 15))
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    if (!cv2_decimal(ctx, fields[1], true, &version) || !cv2_image(ctx, fields[2]) ||
        !cv2_decimal(ctx, fields[3], true, &count))
        return false;
    if (version != 2)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_UNSUPPORTED);
    if (count > INT_MAX || count > (uint64_t)ctx->limits->max_ids)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_LIMIT);
    ctx->profile_count = (int)count;
    return cv2_items(ctx, 1);
}

static bool cv2_profile_inventory(cv2_context_t *ctx, cv2_span_t profiles) {
    size_t position = ctx->profile_start;
    int count = 0;
    while (position < profiles.length) {
        cv2_span_t fields[4];
        if (!cv2_step(ctx) || !cv2_row(ctx, profiles, &position, false, fields))
            return false;
        if (count == ctx->profile_count)
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
        count++;
        if (!cv2_items(ctx, 1))
            return false;
    }
    if (count != ctx->profile_count)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    return true;
}

static bool cv2_id_count(cv2_context_t *ctx, cv2_span_t span, int *out) {
    int count = span.length ? 1 : 0;
    for (size_t i = 0; i < span.length; i++) {
        if (!cv2_step(ctx))
            return false;
        if (span.data[i] != ' ')
            continue;
        if (count == INT_MAX)
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_LIMIT);
        count++;
    }
    if (count > ctx->profile_count)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    *out = count;
    return true;
}

static bool cv2_test_inventory(cv2_context_t *ctx, cv2_span_t tests) {
    size_t position = 0;
    while (position < tests.length) {
        cv2_span_t fields[4];
        int ids;
        if (!cv2_step(ctx) || !cv2_row(ctx, tests, &position, true, fields))
            return false;
        if (ctx->test_count == INT_MAX)
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_LIMIT);
        ctx->test_count++;
        if (!cv2_id_count(ctx, fields[3], &ids) || !cv2_items(ctx, 1) ||
            !cv2_items(ctx, (uint64_t)ids))
            return false;
    }
    return true;
}

static bool cv2_prepare(cv2_context_t *ctx) {
    ctx->map = cv2_array(ctx, 1, sizeof(*ctx->map));
    if (!ctx->map)
        return false;
    ctx->map->format = CBM_COVERAGE_FORMAT_PROFILES;
    ctx->map->function_count = ctx->profile_count;
    ctx->map->test_count = ctx->test_count;
    ctx->map->profile_binding = ctx->binding;
    ctx->map->profiles = cv2_array(ctx, (size_t)ctx->profile_count, sizeof(*ctx->map->profiles));
    ctx->map->tests = cv2_array(ctx, (size_t)ctx->test_count, sizeof(*ctx->map->tests));
    return ctx->status == CBM_COVERAGE_PARSE_OK;
}

static bool cv2_name(cv2_context_t *ctx, cv2_span_t span, cbm_coverage_profile_t *row) {
    if (!span.length || (span.length & 1))
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    size_t length = span.length / 2;
    unsigned char *name = cv2_array(ctx, length, 1);
    if (!name)
        return false;
    for (size_t i = 0; i < length; i++) {
        if (!cv2_step(ctx))
            return false;
        int high = cv2_hex_digit(span.data[i * 2]);
        int low = cv2_hex_digit(span.data[i * 2 + 1]);
        if (high < 0 || low < 0)
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
        name[i] = (unsigned char)((high << 4) | low);
    }
    row->name = name;
    row->name_length = length;
    return true;
}

static bool cv2_profile_compare(cv2_context_t *ctx, const cbm_coverage_profile_t *left,
                                const cbm_coverage_profile_t *right, int *out) {
    size_t common = left->name_length < right->name_length ? left->name_length : right->name_length;
    for (size_t i = 0; i < common; i++) {
        if (!cv2_step(ctx))
            return false;
        if (left->name[i] != right->name[i]) {
            *out = left->name[i] < right->name[i] ? -1 : 1;
            return true;
        }
    }
    if (left->name_length != right->name_length) {
        *out = left->name_length < right->name_length ? -1 : 1;
        return true;
    }
    *out =
        (left->function_hash > right->function_hash) - (left->function_hash < right->function_hash);
    return true;
}

static bool cv2_profile(cv2_context_t *ctx, cv2_span_t fields[4], int index) {
    uint64_t id, counters;
    cbm_coverage_profile_t *row = &ctx->map->profiles[index];
    if (!cv2_decimal(ctx, fields[0], true, &id))
        return false;
    if (id != (uint64_t)index)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    row->id = index;
    if (!cv2_hex64(ctx, fields[2], &row->function_hash) ||
        !cv2_decimal(ctx, fields[3], true, &counters))
        return false;
    if (!counters)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    row->counter_count = counters;
    if (!cv2_name(ctx, fields[1], row))
        return false;
    if (index) {
        int order;
        if (!cv2_profile_compare(ctx, row - 1, row, &order))
            return false;
        if (order >= 0)
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    }
    return true;
}

static bool cv2_profiles(cv2_context_t *ctx, cv2_span_t profiles) {
    size_t position = ctx->profile_start;
    for (int i = 0; i < ctx->profile_count; i++) {
        cv2_span_t fields[4];
        if (!cv2_step(ctx) || !cv2_row(ctx, profiles, &position, false, fields) ||
            !cv2_profile(ctx, fields, i))
            return false;
    }
    if (position != profiles.length)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    return true;
}

static bool cv2_test_name(cv2_context_t *ctx, cv2_span_t field, cbm_coverage_test_t *row) {
    size_t colon = 0;
    while (colon < field.length && field.data[colon] != ':') {
        if (!cv2_step(ctx))
            return false;
        colon++;
    }
    if (!colon || colon == field.length || colon + 1 == field.length)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    row->suite = cv2_string(ctx, (cv2_span_t){field.data, colon});
    row->name = cv2_string(ctx, (cv2_span_t){field.data + colon + 1, field.length - colon - 1});
    return row->suite && row->name;
}

static bool cv2_test_state(cv2_context_t *ctx, cv2_span_t fields[4], cbm_coverage_test_t *row) {
    bool complete = cv2_literal(fields[1], "complete", 8);
    bool incomplete = cv2_literal(fields[1], "incomplete", 10);
    if ((!complete && !incomplete) || (complete && fields[2].length) ||
        (incomplete && !fields[2].length))
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    row->complete = complete;
    row->reason = cv2_string(ctx, fields[2]);
    return row->reason != NULL;
}

static bool cv2_test_ids(cv2_context_t *ctx, cv2_span_t span, cbm_coverage_test_t *row) {
    int count;
    if (!cv2_id_count(ctx, span, &count))
        return false;
    if (!count)
        return true;
    int *ids = cv2_array(ctx, (size_t)count, sizeof(*ids));
    if (!ids)
        return false;
    size_t start = 0;
    for (int i = 0; i < count; i++) {
        size_t end = start;
        while (end < span.length && span.data[end] != ' ') {
            if (!cv2_step(ctx))
                return false;
            end++;
        }
        uint64_t id;
        if (!cv2_decimal(ctx, (cv2_span_t){span.data + start, end - start}, false, &id))
            return false;
        if (id >= (uint64_t)ctx->profile_count || (i && id <= (uint64_t)ids[i - 1]))
            return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
        ids[i] = (int)id;
        start = end + 1;
    }
    row->function_ids = ids;
    row->function_count = count;
    return true;
}

static bool cv2_tests(cv2_context_t *ctx, cv2_span_t tests) {
    size_t position = 0;
    for (int i = 0; i < ctx->test_count; i++) {
        cv2_span_t fields[4];
        cbm_coverage_test_t *row = &ctx->map->tests[i];
        if (!cv2_step(ctx) || !cv2_row(ctx, tests, &position, true, fields) ||
            !cv2_test_name(ctx, fields[0], row) || !cv2_test_state(ctx, fields, row) ||
            !cv2_test_ids(ctx, fields[3], row))
            return false;
    }
    if (position != tests.length)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    return true;
}

static bool cv2_string_compare(cv2_context_t *ctx, const char *a, const char *b, int *out) {
    size_t i = 0;
    for (;;) {
        if (!cv2_step(ctx))
            return false;
        unsigned char left = (unsigned char)a[i];
        unsigned char right = (unsigned char)b[i];
        if (left != right || !left) {
            *out = (left > right) - (left < right);
            return true;
        }
        i++;
    }
}

static bool cv2_test_compare(cv2_context_t *ctx, const cbm_coverage_test_t *a,
                             const cbm_coverage_test_t *b, int *out) {
    if (!cv2_string_compare(ctx, a->suite, b->suite, out))
        return false;
    return *out || cv2_string_compare(ctx, a->name, b->name, out);
}

static bool cv2_merge(cv2_context_t *ctx, const cbm_coverage_test_t *source,
                      cbm_coverage_test_t *target, size_t start, size_t middle, size_t end) {
    size_t left = start, right = middle;
    for (size_t at = start; at < end; at++) {
        if (!cv2_work(ctx, (unsigned)sizeof(*target)))
            return false;
        bool take_left = right == end;
        if (!take_left && left < middle) {
            int order;
            if (!cv2_test_compare(ctx, &source[left], &source[right], &order))
                return false;
            take_left = order <= 0;
        }
        target[at] = take_left ? source[left++] : source[right++];
    }
    return true;
}

static bool cv2_sort_tests(cv2_context_t *ctx) {
    size_t count = (size_t)ctx->test_count;
    if (count < 2)
        return true;
    cbm_coverage_test_t *source = ctx->map->tests;
    cbm_coverage_test_t *target = cv2_array(ctx, count, sizeof(*target));
    if (!target)
        return false;
    for (size_t width = 1; width < count;) {
        size_t start = 0;
        while (start < count) {
            size_t half = count - start < width ? count - start : width;
            size_t middle = start + half;
            half = count - middle < width ? count - middle : width;
            size_t end = middle + half;
            if (!cv2_merge(ctx, source, target, start, middle, end))
                return false;
            start = end;
        }
        cbm_coverage_test_t *swap = source;
        source = target;
        target = swap;
        if (width > count / 2)
            break;
        width *= 2;
    }
    ctx->map->tests = source;
    return true;
}

static bool cv2_validate_tests(cv2_context_t *ctx) {
    bool setup = false;
    for (int i = 0; i < ctx->test_count; i++) {
        if (!cv2_step(ctx))
            return false;
        const cbm_coverage_test_t *row = &ctx->map->tests[i];
        int suite_order = 1, name_order = 1;
        if (i && !cv2_string_compare(ctx, row[-1].suite, row->suite, &suite_order))
            return false;
        if (suite_order) {
            if (i && !setup)
                return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
            setup = false;
        } else {
            if (!cv2_string_compare(ctx, row[-1].name, row->name, &name_order))
                return false;
            if (!name_order)
                return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
        }
        if (row->name[0] == '*' && row->name[1] == '\0')
            setup = true;
    }
    if (ctx->test_count && !setup)
        return cv2_fail(ctx, CBM_COVERAGE_PARSE_FORMAT);
    return true;
}

static bool cv2_hash(cv2_context_t *ctx, cv2_span_t span, char hex[CBM_SHA256_HEX_LEN + 1]) {
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    size_t at = 0;
    while (at < span.length) {
        if (!cv2_poll(ctx))
            return false;
        size_t chunk = span.length - at;
        if (chunk > 4096)
            chunk = 4096;
        cbm_sha256_update(&hash, span.data + at, chunk);
        at += chunk;
    }
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&hash, digest);
    static const char digits[] = "0123456789abcdef";
    for (int i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
        if (!cv2_step(ctx))
            return false;
        hex[i * 2] = digits[digest[i] >> 4];
        hex[i * 2 + 1] = digits[digest[i] & 15];
    }
    hex[CBM_SHA256_HEX_LEN] = '\0';
    return cv2_poll(ctx);
}

static cbm_coverage_parse_status_t cv2_arguments(const void *profiles, size_t profiles_len,
                                                 const char *tests, size_t tests_len,
                                                 const cbm_coverage_parse_limits_t *limits) {
    if (!profiles || (!tests && tests_len) || !limits)
        return CBM_COVERAGE_PARSE_INVALID;
    if (!limits->max_input_bytes || limits->max_input_bytes > UINT64_MAX / 8 ||
        !limits->max_items || !limits->max_alloc_bytes || limits->max_ids <= 0)
        return CBM_COVERAGE_PARSE_INVALID;
    if (profiles_len > (size_t)PTRDIFF_MAX || tests_len > (size_t)PTRDIFF_MAX)
        return CBM_COVERAGE_PARSE_LIMIT;
    uint64_t total = (uint64_t)profiles_len;
    if ((uint64_t)tests_len > UINT64_MAX - total)
        return CBM_COVERAGE_PARSE_LIMIT;
    total += (uint64_t)tests_len;
    if (total > limits->max_input_bytes)
        return CBM_COVERAGE_PARSE_LIMIT;
    return CBM_COVERAGE_PARSE_OK;
}

cbm_coverage_parse_status_t cbm_coverage_map_parse_v2(const void *profiles, size_t profiles_len,
                                                      const char *tests, size_t tests_len,
                                                      const cbm_coverage_parse_limits_t *limits,
                                                      cbm_coverage_parse_cancel_fn cancelled,
                                                      void *cancel_context,
                                                      cbm_coverage_map_t **out) {
    if (!out)
        return CBM_COVERAGE_PARSE_INVALID;
    *out = NULL;
    cbm_coverage_parse_status_t status =
        cv2_arguments(profiles, profiles_len, tests, tests_len, limits);
    if (status != CBM_COVERAGE_PARSE_OK)
        return status;
    cv2_context_t ctx = {.limits = limits,
                         .cancelled = cancelled,
                         .cancel_context = cancel_context,
                         .status = CBM_COVERAGE_PARSE_OK};
    cbm_arena_init_lazy(&ctx.arena, 4096);
    cv2_span_t profile_span = {profiles, profiles_len};
    cv2_span_t test_span = {(const unsigned char *)tests, tests_len};
    bool ok = cv2_poll(&ctx) && cv2_header(&ctx, profile_span) &&
              cv2_profile_inventory(&ctx, profile_span) && cv2_test_inventory(&ctx, test_span) &&
              cv2_prepare(&ctx) && cv2_profiles(&ctx, profile_span) && cv2_tests(&ctx, test_span) &&
              cv2_sort_tests(&ctx) && cv2_validate_tests(&ctx) &&
              cv2_hash(&ctx, profile_span, ctx.map->functions_sha256) &&
              cv2_hash(&ctx, test_span, ctx.map->tests_sha256) && cv2_poll(&ctx);
    if (!ok) {
        status = ctx.status == CBM_COVERAGE_PARSE_OK ? CBM_COVERAGE_PARSE_FORMAT : ctx.status;
        cbm_arena_destroy(&ctx.arena);
        return status;
    }
    ctx.map->arena = ctx.arena;
    *out = ctx.map;
    return CBM_COVERAGE_PARSE_OK;
}

cbm_coverage_format_t cbm_coverage_map_format(const cbm_coverage_map_t *map) {
    return map ? map->format : CBM_COVERAGE_FORMAT_NONE;
}

int cbm_coverage_map_id_count(const cbm_coverage_map_t *map) {
    return map ? map->function_count : 0;
}

const char *cbm_coverage_map_identity_sha256(const cbm_coverage_map_t *map) {
    return map ? map->functions_sha256 : NULL;
}

const cbm_coverage_profile_binding_t *cbm_coverage_map_profile_binding(
    const cbm_coverage_map_t *map) {
    return map && map->format == CBM_COVERAGE_FORMAT_PROFILES ? &map->profile_binding : NULL;
}

const cbm_coverage_profile_t *cbm_coverage_map_profiles(const cbm_coverage_map_t *map, int *count) {
    bool profiles = map && map->format == CBM_COVERAGE_FORMAT_PROFILES;
    if (count)
        *count = profiles ? map->function_count : 0;
    return profiles ? map->profiles : NULL;
}
