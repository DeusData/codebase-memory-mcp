#include "mcp/test_impact_result_internal.h"
#include "foundation/test_selection.h"

static const char *const tir_reason_names[TIR_REASON_COUNT] = {"CHANGED",
                                                               "STATIC",
                                                               "COVERAGE",
                                                               "UNMAPPED",
                                                               "COVERAGE_UNKNOWN",
                                                               "SETUP_HIT",
                                                               "SETUP_UNKNOWN",
                                                               "INVENTORY_UNKNOWN",
                                                               "CONDITIONAL",
                                                               "ARTIFACT_REJECTED",
                                                               "DIFF_INCOMPLETE",
                                                               "STATIC_INCOMPLETE",
                                                               "CHANGE_IDENTITY_UNKNOWN",
                                                               "INVALID_INPUT",
                                                               "CHANGED_UNREGISTERED",
                                                               "RULE",
                                                               "POLICY_UNAVAILABLE",
                                                               "ACTIVATION_UNKNOWN",
                                                               "RULE_TARGET_UNKNOWN",
                                                               "GRAPH_UNAVAILABLE",
                                                               "GRAPH_REJECTED",
                                                               "CAPABILITY_MISSING",
                                                               "GIT_UNAVAILABLE",
                                                               "CONFIG_INVALID",
                                                               "ENGINE_SATURATED",
                                                               "UNMAPPED_FILE",
                                                               "UNRESOLVED_CHANGED_TEST",
                                                               "SELECTION_UNAVAILABLE",
                                                               "RULE_RUN_ALL",
                                                               "POLICY_EVALUATION_FAILED",
                                                               "TRAVERSAL_POLICY_UNKNOWN",
                                                               "RUNNER_REACHED",
                                                               "POLICY_INACTIVE",
                                                               "NO_CHANGES",
                                                               "NO_SELECTED_TESTS",
                                                               "NO_MEMBER_SUITES",
                                                               "NARROW_DISABLED"};
static const char *const tir_evidence_names[CBM_TEST_RESULT_EVIDENCE_COUNT] = {
    "PROVIDER_UNAVAILABLE",
    "SOURCE_UNVERIFIED",
    "IDENTITY_MISMATCH",
    "CONTENT_MISMATCH",
    "COMMIT_MISMATCH",
    "ANCESTRY_UNPROVED",
    "TOO_OLD",
    "COMPATIBILITY_MISMATCH",
    "METADATA_INVALID",
    "FORMAT_UNSUPPORTED",
    "INVENTORY_INCOMPLETE",
    "DIAGNOSTICS_INCOMPLETE",
    "CAPABILITY_MISSING",
    "SEMANTIC_INPUTS_UNPROVED",
    "APPLICABILITY_UNPROVED",
    "OBSERVATIONS_INCOMPLETE",
    "IMAGE_UNPROVED"};
_Static_assert(TIR_REASON_COUNT < 64, "reason mask capacity");

const char *tir_reason_name(unsigned index) {
    return index < TIR_REASON_COUNT ? tir_reason_names[index] : "";
}
const char *tir_warning_name(cbm_test_result_warning_code_t code) {
    static const char *const names[] = {"TEST_NOT_REGISTERED", "SOURCE_MAPPING_UNKNOWN",
                                        "DIAGNOSTICS_INCOMPLETE", "UNSUPPORTED_MAPPING"};
    return code >= 0 && code < CBM_TEST_RESULT_WARNING_COUNT ? names[code] : "";
}

static tir_json *tir_named_set(tir_context *c, uint64_t mask, const char *const *names,
                               size_t count) {
    tir_json *array = tir_node(c, TIR_ARRAY);
    while (mask) {
        size_t best = SIZE_MAX;
        for (size_t i = 0; i < count; i++) {
            if (!tir_step(c, 0, 1))
                return NULL;
            if (!(mask & TIR_REASON(i)))
                continue;
            if (best == SIZE_MAX ||
                tir_compare(c, tir_literal(names[i]), tir_literal(names[best])) < 0)
                best = i;
        }
        if (best == SIZE_MAX) {
            tir_fail(c, CBM_TEST_RESULT_INVALID);
            return NULL;
        }
        if (!tir_add(c, array, NULL, tir_text(c, names[best])))
            return NULL;
        mask &= ~TIR_REASON(best);
    }
    return array;
}

tir_json *tir_reasons(tir_context *c, uint64_t reasons) {
    return tir_named_set(c, reasons, tir_reason_names, TIR_REASON_COUNT);
}

tir_json *tir_evidence(tir_context *c, cbm_test_result_evidence_reasons_t reasons) {
    uint64_t mask = 0;
    for (size_t i = 0; i < reasons.count; i++) {
        if (!tir_step(c, 0, 1))
            return NULL;
        mask |= TIR_REASON(reasons.values[i]);
    }
    return tir_named_set(c, mask, tir_evidence_names, CBM_TEST_RESULT_EVIDENCE_COUNT);
}

static tir_json *tir_case_id(tir_context *c, const tir_case *test) {
    if (test->test.length > SIZE_MAX - 2 || test->suite.length > SIZE_MAX - test->test.length - 2) {
        tir_fail(c, CBM_TEST_RESULT_LIMIT);
        return NULL;
    }
    size_t length = test->suite.length + 1 + test->test.length;
    unsigned char *bytes = tir_alloc(c, length + 1, 1);
    if (!bytes || !tir_copy(c, bytes, test->suite.data, test->suite.length) ||
        !tir_copy(c, bytes + test->suite.length + 1, test->test.data, test->test.length))
        return NULL;
    bytes[test->suite.length] = ':';
    return tir_string(c, (tir_bytes){bytes, length});
}

tir_json *tir_case_json(tir_context *c, const tir_case *test) {
    if (!tir_identifier(c, test->test, false))
        return NULL;
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_add(c, o, "test", tir_string(c, test->test));
    tir_add(c, o, "id", tir_case_id(c, test));
    tir_path(c, o, test->file);
    tir_add(c, o, "line", test->line ? tir_uint(c, test->line) : tir_null(c));
    tir_add(c, o, "reasons", tir_reasons(c, test->reasons));
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}

tir_json *tir_suite_json(tir_context *c, tir_bytes name, const tir_suite *selection, bool whole,
                         uint64_t reasons) {
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_add(c, o, "suite", tir_string(c, name));
    tir_add(c, o, "mode", tir_text(c, whole ? "whole" : "tests"));
    if (whole) {
        tir_add(c, o, "whole_reason", tir_reasons(c, reasons));
        return c->status == CBM_TEST_RESULT_OK ? o : NULL;
    }
    if (!selection || !selection->count || selection->begin > c->case_count ||
        selection->count > c->case_count - selection->begin) {
        tir_fail(c, CBM_TEST_RESULT_INVALID);
        return NULL;
    }
    tir_json *tests = tir_node(c, TIR_ARRAY);
    for (size_t i = 0; i < selection->count; i++) {
        if (!tir_step(c, 0, 1) ||
            !tir_add(c, tests, NULL, tir_case_json(c, &c->cases[selection->begin + i])))
            return NULL;
    }
    tir_add(c, o, "tests", tests);
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}

static tir_json *tir_member(tir_context *c, const tir_json *object, const char *key) {
    for (tir_link *entry = object->as.children.first; entry; entry = entry->next) {
        if (!tir_step(c, 0, 1))
            return NULL;
        if (!tir_compare(c, tir_literal(entry->key), tir_literal(key)))
            return entry->value;
    }
    return NULL;
}

typedef struct {
    tir_context *context;
    unsigned char *bytes;
    size_t length;
} tir_filter_writer;
static bool tir_filter_token(tir_filter_writer *w, tir_json *token) {
    if (!token || token->kind != TIR_STRING)
        return tir_fail(w->context, CBM_TEST_RESULT_INVALID);
    tir_bytes b = token->as.string;
    if (b.length > CBM_TEST_SELECTION_TOKEN_MAX)
        return tir_fail(w->context, CBM_TEST_RESULT_UNSUPPORTED);
    if (w->length > SIZE_MAX - 2 || b.length > SIZE_MAX - 2 - w->length)
        return tir_fail(w->context, CBM_TEST_RESULT_LIMIT);
    if (w->bytes) {
        if (!tir_copy(w->context, w->bytes + w->length, b.data, b.length))
            return false;
        w->bytes[w->length + b.length] = '\n';
    } else if (!tir_step(w->context, 0, 1))
        return false;
    w->length += b.length + 1;
    return true;
}

static bool tir_filter_suite(tir_filter_writer *w, const tir_json *suite) {
    tir_context *c = w->context;
    tir_json *tests = tir_member(c, suite, "tests");
    if (!tests)
        return tir_filter_token(w, tir_member(c, suite, "suite"));
    for (tir_link *test = tests->as.children.first; test; test = test->next) {
        if (!tir_step(c, 0, 1) || !tir_filter_token(w, tir_member(c, test->value, "id")))
            return false;
    }
    return true;
}

static bool tir_filter_pass(tir_filter_writer *w, const tir_json *suites) {
    for (tir_link *suite = suites->as.children.first; suite; suite = suite->next) {
        if (!tir_step(w->context, 0, 1) || !tir_filter_suite(w, suite->value))
            return false;
    }
    return true;
}

tir_json *tir_filter_json(tir_context *c, tir_json *suites) {
    tir_filter_writer measure = {c, NULL, 0};
    if (!tir_filter_pass(&measure, suites))
        return NULL;
    if (!measure.length) {
        tir_fail(c, CBM_TEST_RESULT_INVALID);
        return NULL;
    }
    unsigned char *bytes = tir_alloc(c, measure.length + 1, 1);
    if (!bytes)
        return NULL;
    tir_filter_writer write = {c, bytes, 0};
    if (!tir_filter_pass(&write, suites))
        return NULL;
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_add(c, o, "encoding", tir_text(c, "cbm-test-only-lines"));
    tir_add(c, o, "value", tir_string(c, (tir_bytes){bytes, write.length}));
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}

tir_json *tir_totals_json(tir_context *c) {
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_add(c, o, "lanes", c->input->policy ? tir_uint(c, c->lane_count) : tir_null(c));
    tir_add(c, o, "runnable_lanes", c->global ? tir_null(c) : tir_uint(c, c->runnable));
    tir_add(c, o, "suites", c->global ? tir_null(c) : tir_uint(c, c->suite_occurrences));
    tir_add(c, o, "tests",
            c->global || c->whole_occurrence ? tir_null(c) : tir_uint(c, c->test_occurrences));
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}

tir_json *tir_warnings_json(tir_context *c) {
    tir_json *array = tir_node(c, TIR_ARRAY);
    for (size_t i = 0; i < c->warning_count; i++) {
        if (!tir_step(c, 0, 1))
            return NULL;
        const tir_warning *w = &c->warnings[i];
        tir_json *o = tir_node(c, TIR_OBJECT);
        tir_add(c, o, "code", tir_text(c, tir_warning_name(w->code)));
        tir_path(c, o, w->file);
        tir_add(c, o, "line", w->line ? tir_uint(c, w->line) : tir_null(c));
        if (!tir_add(c, array, NULL, o))
            return NULL;
    }
    return array;
}
