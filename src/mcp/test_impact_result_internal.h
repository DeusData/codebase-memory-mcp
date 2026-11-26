#ifndef CBM_TEST_IMPACT_RESULT_INTERNAL_H
#define CBM_TEST_IMPACT_RESULT_INTERNAL_H

#include "mcp/test_impact_result.h"
#include "foundation/arena.h"
#include "foundation/sha256.h"

typedef cbm_test_result_bytes_t tir_bytes;
typedef struct tir_json tir_json;
typedef struct tir_link {
    const char *key;
    tir_json *value;
    struct tir_link *next;
} tir_link;
typedef enum { TIR_NULL, TIR_BOOL, TIR_UINT, TIR_INT, TIR_STRING, TIR_ARRAY, TIR_OBJECT } tir_kind;
struct tir_json {
    tir_kind kind;
    union {
        bool boolean;
        uint64_t number;
        int64_t integer;
        tir_bytes string;
        struct {
            tir_link *first;
            tir_link *last;
        } children;
    } as;
};

typedef struct {
    tir_bytes name;
    const char *original;
    bool perf;
} tir_runner;
typedef struct {
    tir_bytes file, name;
    int line;
} tir_model_case;
typedef struct {
    tir_bytes name;
    bool whole;
    uint64_t reasons;
    size_t begin, count;
} tir_suite;
typedef struct {
    tir_bytes suite, test, file;
    uint64_t reasons, line;
} tir_case;
typedef struct {
    const cbm_test_rule_t *source;
    tir_bytes id;
    tir_bytes *lanes;
    bool matched, resolved;
} tir_rule;
typedef struct {
    const cbm_test_lane_t *source;
    tir_bytes name;
    bool active;
    tir_json *rules;
} tir_lane;
typedef struct {
    cbm_test_result_warning_code_t code;
    tir_bytes file;
    uint64_t line;
} tir_warning;
typedef struct tir_allocation {
    struct tir_allocation *next;
    size_t size;
    void *data;
} tir_allocation;

enum {
    TIR_SELECT_BITS = 16,
    TIR_REASON_POLICY_INACTIVE = TIR_SELECT_BITS + CBM_TEST_RESULT_FALLBACK_COUNT,
    TIR_REASON_NO_CHANGES,
    TIR_REASON_NO_SELECTED_TESTS,
    TIR_REASON_NO_MEMBER_SUITES,
    TIR_REASON_NARROW_DISABLED,
    TIR_REASON_COUNT
};
#define TIR_REASON(bit) (UINT64_C(1) << (bit))
#define TIR_FALLBACK(code) TIR_REASON(TIR_SELECT_BITS + (code))
#define TIR_SELECTION_MASK ((UINT64_C(1) << TIR_SELECT_BITS) - 1)

typedef struct {
    CBMArena arena;
    const cbm_test_result_input_t *input;
    const cbm_test_result_limits_t *limits;
    cbm_test_result_cancel_fn cancelled;
    void *cancel_context;
    cbm_test_result_status_t status;
    uint64_t input_bytes, items;
    size_t allocations, byte_steps, item_steps;
    tir_allocation *owned;
    tir_runner *runners;
    size_t runner_count;
    tir_model_case *definitions;
    size_t definition_count;
    tir_suite *suites;
    size_t suite_count, raw_suite_count;
    tir_case *cases;
    size_t case_count, raw_case_count;
    tir_rule *rules;
    size_t rule_count;
    tir_lane *lanes;
    size_t lane_count;
    tir_warning *warnings;
    size_t warning_count, warning_capacity;
    cbm_test_result_extension_t *extensions;
    uint64_t global;
    uint64_t runnable, suite_occurrences, test_occurrences;
    bool whole_occurrence;
    tir_json *lane_json, *global_json, *digest_json;
    size_t lane_offset, lane_length, global_offset, global_length, digest_offset;
} tir_context;

struct cbm_test_result {
    CBMArena arena;
    char *json;
    size_t length;
    char digest[65];
};

bool tir_fail(tir_context *c, cbm_test_result_status_t status);
bool tir_poll(tir_context *c);
bool tir_step(tir_context *c, size_t bytes, size_t items);
bool tir_charge(tir_context *c, uint64_t *used, uint64_t amount, uint64_t limit);
bool tir_items(tir_context *c, uint64_t count);
bool tir_input_bytes(tir_context *c, size_t count);
bool tir_reserve(tir_context *c, size_t count);
void *tir_alloc(tir_context *c, size_t count, size_t size);
bool tir_forget_borrowed(tir_context *c, const cbm_test_result_t *result);
bool tir_copy(tir_context *c, void *dest, const void *source, size_t count);
bool tir_cstring(tir_context *c, const char *s, tir_bytes *out);
bool tir_span(tir_context *c, tir_bytes b, bool text);
bool tir_utf8(tir_context *c, tir_bytes b);
bool tir_identifier(tir_context *c, tir_bytes b, bool suite);
int tir_compare(tir_context *c, tir_bytes a, tir_bytes b);
tir_bytes tir_literal(const char *s);
typedef int (*tir_compare_fn)(tir_context *, const void *, const void *);
bool tir_sort(tir_context *c, void *rows, size_t count, size_t size, tir_compare_fn compare);
bool tir_preflight(tir_context *c);
bool tir_receipt_validate(tir_context *c);
bool tir_prepare(tir_context *c);
bool tir_project(tir_context *c);
size_t tir_find_suite(tir_context *c, tir_bytes name);
tir_json *tir_case_json(tir_context *c, const tir_case *test);
tir_json *tir_suite_json(tir_context *c, tir_bytes name, const tir_suite *selection, bool whole,
                         uint64_t reasons);
tir_json *tir_filter_json(tir_context *c, tir_json *suites);
tir_json *tir_totals_json(tir_context *c);
tir_json *tir_receipt_json(tir_context *c, const char *digest);
tir_json *tir_warnings_json(tir_context *c);

tir_json *tir_node(tir_context *c, tir_kind kind);
tir_json *tir_null(tir_context *c);
tir_json *tir_bool(tir_context *c, bool value);
tir_json *tir_uint(tir_context *c, uint64_t value);
tir_json *tir_int(tir_context *c, int64_t value);
tir_json *tir_string(tir_context *c, tir_bytes value);
tir_json *tir_text(tir_context *c, const char *value);
bool tir_add(tir_context *c, tir_json *container, const char *key, tir_json *value);
bool tir_path(tir_context *c, tir_json *object, tir_bytes file);
tir_json *tir_hex(tir_context *c, const unsigned char *data, size_t count);
tir_json *tir_reasons(tir_context *c, uint64_t reasons);
tir_json *tir_evidence(tir_context *c, cbm_test_result_evidence_reasons_t reasons);
tir_json *tir_digest(tir_context *c, cbm_test_result_digest_t digest);
tir_json *tir_oid(tir_context *c, cbm_test_result_oid_t oid);
bool tir_render(tir_context *c, tir_json *node, char **bytes, size_t *length, bool output);
bool tir_decision_hash(tir_context *c, const char *data, size_t length, char out[65]);
const char *tir_reason_name(unsigned index);
const char *tir_warning_name(cbm_test_result_warning_code_t code);

#endif
