#include "mcp/test_impact_result_internal.h"

static int tir_runner_compare(tir_context *c, const void *a, const void *b) {
    return tir_compare(c, ((const tir_runner *)a)->name, ((const tir_runner *)b)->name);
}
static int tir_suite_compare(tir_context *c, const void *a, const void *b) {
    return tir_compare(c, ((const tir_suite *)a)->name, ((const tir_suite *)b)->name);
}
static int tir_case_compare(tir_context *c, const void *left, const void *right) {
    const tir_case *a = left, *b = right;
    int order = tir_compare(c, a->suite, b->suite);
    return order ? order : tir_compare(c, a->test, b->test);
}
static int tir_definition_compare(tir_context *c, const void *left, const void *right) {
    const tir_model_case *a = left, *b = right;
    int order = tir_compare(c, a->file, b->file);
    return order ? order : tir_compare(c, a->name, b->name);
}
static int tir_warning_compare(tir_context *c, const void *left, const void *right) {
    const tir_warning *a = left, *b = right;
    int order = tir_compare(c, tir_literal(tir_warning_name(a->code)),
                            tir_literal(tir_warning_name(b->code)));
    if (!order)
        order = tir_compare(c, a->file, b->file);
    return order ? order : (a->line > b->line) - (a->line < b->line);
}
static int tir_extension_compare(tir_context *c, const void *left, const void *right) {
    const cbm_test_result_extension_t *a = left, *b = right;
    int order = tir_compare(c, a->language, b->language);
    return order ? order : tir_compare(c, a->edge, b->edge);
}

size_t tir_find_suite(tir_context *c, tir_bytes name) {
    size_t lo = 0, hi = c->suite_count;
    while (lo < hi) {
        if (!tir_step(c, 0, 1))
            return SIZE_MAX;
        size_t mid = lo + (hi - lo) / 2;
        int order = tir_compare(c, name, c->suites[mid].name);
        if (!order)
            return mid;
        if (order < 0)
            hi = mid;
        else
            lo = mid + 1;
    }
    return SIZE_MAX;
}

static bool tir_runner_contains(tir_context *c, tir_bytes name) {
    size_t lo = 0, hi = c->runner_count;
    while (lo < hi) {
        if (!tir_step(c, 0, 1))
            return false;
        size_t mid = lo + (hi - lo) / 2;
        int order = tir_compare(c, name, c->runners[mid].name);
        if (!order)
            return true;
        if (order < 0)
            hi = mid;
        else
            lo = mid + 1;
    }
    return false;
}

static bool tir_normalize_suites(tir_context *c) {
    if (!tir_sort(c, c->suites, c->suite_count, sizeof(*c->suites), tir_suite_compare))
        return false;
    size_t used = 0;
    /* Narrowable, not complete: a suite the model is unsure of arrives whole
     * in the selection, so only uncertainty it cannot scope is global. */
    bool known = c->input->inventory_complete && c->input->model &&
                 cbm_test_model_narrowable(c->input->model);
    for (size_t i = 0; i < c->suite_count; i++) {
        if (!tir_step(c, 0, 1))
            return false;
        tir_suite *row = &c->suites[i];
        if (known && !tir_runner_contains(c, row->name))
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
        if (used && !tir_compare(c, c->suites[used - 1].name, row->name)) {
            c->suites[used - 1].whole |= row->whole;
            c->suites[used - 1].reasons |= row->reasons;
        } else
            c->suites[used++] = *row;
    }
    c->suite_count = used;
    return c->status == CBM_TEST_RESULT_OK;
}

static bool tir_normalize_cases(tir_context *c) {
    if (!tir_sort(c, c->cases, c->case_count, sizeof(*c->cases), tir_case_compare))
        return false;
    size_t used = 0;
    for (size_t i = 0; i < c->case_count; i++) {
        if (!tir_step(c, 0, 1))
            return false;
        tir_case *row = &c->cases[i];
        size_t at = tir_find_suite(c, row->suite);
        if (at == SIZE_MAX)
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
        tir_suite *suite = &c->suites[at];
        suite->reasons |= row->reasons;
        if (used && !tir_case_compare(c, &c->cases[used - 1], row)) {
            tir_case *previous = &c->cases[used - 1];
            previous->reasons |= row->reasons;
            if (tir_compare(c, previous->file, row->file)) {
                suite->whole = true;
                suite->reasons |= CBM_TEST_SELECT_INVENTORY_UNKNOWN;
            }
        } else {
            if (!suite->count)
                suite->begin = used;
            suite->count++;
            c->cases[used++] = *row;
        }
    }
    c->case_count = used;
    for (size_t i = 0; i < c->suite_count; i++) {
        if (!tir_step(c, 0, 1))
            return false;
        if (!c->suites[i].whole && !c->suites[i].count)
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
    }
    return c->status == CBM_TEST_RESULT_OK;
}

static uint64_t tir_source_line(tir_context *c, const tir_case *test) {
    tir_model_case key = {test->file, test->test, 0};
    size_t lo = 0, hi = c->definition_count;
    while (lo < hi) {
        if (!tir_step(c, 0, 1))
            return 0;
        size_t mid = lo + (hi - lo) / 2;
        int order = tir_definition_compare(c, &key, &c->definitions[mid]);
        if (order > 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    if (lo == c->definition_count || tir_definition_compare(c, &key, &c->definitions[lo]))
        return 0;
    if (lo + 1 < c->definition_count && !tir_definition_compare(c, &key, &c->definitions[lo + 1]))
        return 0;
    return c->definitions[lo].line > 0 ? (uint64_t)c->definitions[lo].line : 0;
}

static bool tir_source_lines(tir_context *c) {
    if (!tir_sort(c, c->definitions, c->definition_count, sizeof(*c->definitions),
                  tir_definition_compare))
        return false;
    for (size_t i = 0; i < c->case_count; i++) {
        if (!tir_step(c, 0, 1))
            return false;
        tir_case *row = &c->cases[i];
        size_t suite = tir_find_suite(c, row->suite);
        if (suite == SIZE_MAX)
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
        if (c->suites[suite].whole)
            continue;
        row->line = tir_source_line(c, row);
        if (!row->line) {
            if (c->warning_count == c->warning_capacity)
                return tir_fail(c, CBM_TEST_RESULT_LIMIT);
            c->warnings[c->warning_count++] =
                (tir_warning){CBM_TEST_RESULT_WARNING_SOURCE_MAPPING_UNKNOWN, row->file, 0};
        }
    }
    return c->status == CBM_TEST_RESULT_OK;
}

static bool tir_diagnostic_order(tir_context *c) {
    if (!tir_sort(c, c->warnings, c->warning_count, sizeof(*c->warnings), tir_warning_compare))
        return false;
    size_t used = 0;
    for (size_t i = 0; i < c->warning_count; i++) {
        if (!tir_step(c, 0, 1))
            return false;
        if (!used || tir_warning_compare(c, &c->warnings[used - 1], &c->warnings[i]))
            c->warnings[used++] = c->warnings[i];
    }
    c->warning_count = used;
    size_t count = c->input->receipt->extension_count;
    if (!tir_sort(c, c->extensions, count, sizeof(*c->extensions), tir_extension_compare))
        return false;
    for (size_t i = 1; i < count; i++) {
        if (!tir_step(c, 0, 1))
            return false;
        if (!tir_extension_compare(c, &c->extensions[i - 1], &c->extensions[i]))
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
    }
    return c->status == CBM_TEST_RESULT_OK;
}

static bool tir_lane_references(tir_context *c) {
    for (size_t i = 0; i < c->rule_count; i++) {
        tir_rule *rule = &c->rules[i];
        for (int j = 0; j < rule->source->lane_count; j++) {
            bool found = false;
            for (size_t k = 0; k < c->lane_count; k++) {
                if (!tir_step(c, 0, 1))
                    return false;
                if (!tir_compare(c, rule->lanes[j], c->lanes[k].name))
                    found = true;
            }
            if (!found)
                return tir_fail(c, CBM_TEST_RESULT_INVALID);
        }
    }
    return true;
}

static bool tir_rule_names_lane(tir_context *c, const tir_rule *rule, tir_bytes name,
                                bool *matched) {
    *matched = false;
    for (int i = 0; i < rule->source->lane_count; i++) {
        if (!tir_step(c, 0, 1))
            return false;
        if (!tir_compare(c, name, rule->lanes[i]))
            *matched = true;
    }
    return c->status == CBM_TEST_RESULT_OK;
}

static bool tir_activate_lane(tir_context *c, tir_lane *lane) {
    lane->active = lane->source->default_run;
    lane->rules = tir_node(c, TIR_ARRAY);
    for (size_t i = 0; i < c->rule_count; i++) {
        if (!tir_step(c, 0, 1))
            return false;
        tir_rule *rule = &c->rules[i];
        if (!rule->matched || rule->source->action != CBM_TEST_RULE_LANES)
            continue;
        bool names_lane = false;
        if (!tir_rule_names_lane(c, rule, lane->name, &names_lane))
            return false;
        if (names_lane) {
            lane->active = true;
            if (!tir_add(c, lane->rules, NULL, tir_string(c, rule->id)))
                return false;
        }
    }
    return c->status == CBM_TEST_RESULT_OK;
}

static bool tir_activate(tir_context *c) {
    if (!tir_lane_references(c))
        return false;
    for (size_t i = 0; i < c->lane_count; i++) {
        if (!tir_step(c, 0, 1) || !tir_activate_lane(c, &c->lanes[i]))
            return false;
    }
    return true;
}

static void tir_global_floors(tir_context *c) {
    const cbm_test_result_input_t *in = c->input;
    if (in->comparison == CBM_TEST_RESULT_COMPARISON_UNAVAILABLE)
        c->global |= TIR_FALLBACK(CBM_TEST_RESULT_FALLBACK_GIT_UNAVAILABLE);
    bool empty_shortcut =
        in->comparison == CBM_TEST_RESULT_COMPARISON_EMPTY && !c->global && !in->matched_rule_count;
    if (!in->policy && !empty_shortcut)
        c->global |= TIR_FALLBACK(CBM_TEST_RESULT_FALLBACK_POLICY_UNAVAILABLE);
    if (in->policy && !in->activation_complete &&
        in->comparison != CBM_TEST_RESULT_COMPARISON_EMPTY)
        c->global |= TIR_FALLBACK(CBM_TEST_RESULT_FALLBACK_ACTIVATION_UNKNOWN);
    if (in->comparison != CBM_TEST_RESULT_COMPARISON_CHANGED)
        return;
    if (!in->inventory_complete || !in->model || !cbm_test_model_narrowable(in->model))
        c->global |= CBM_TEST_SELECT_INVENTORY_UNKNOWN;
    if (!in->selection)
        c->global |= TIR_FALLBACK(CBM_TEST_RESULT_FALLBACK_SELECTION_UNAVAILABLE);
}

bool tir_prepare(tir_context *c) {
    if (!tir_sort(c, c->runners, c->runner_count, sizeof(*c->runners), tir_runner_compare))
        return false;
    for (size_t i = 1; i < c->runner_count; i++) {
        if (!tir_step(c, 0, 1))
            return false;
        if (!tir_compare(c, c->runners[i - 1].name, c->runners[i].name))
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
    }
    if (!tir_normalize_suites(c) || !tir_normalize_cases(c) || !tir_source_lines(c) ||
        !tir_diagnostic_order(c) || !tir_activate(c))
        return false;
    tir_global_floors(c);
    return c->status == CBM_TEST_RESULT_OK;
}
