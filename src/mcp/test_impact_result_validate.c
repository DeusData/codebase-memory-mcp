#include "mcp/test_impact_result_internal.h"

static bool tir_count(tir_context *c, const void *rows, int count, size_t *out) {
    if (count < 0 || (count && !rows))
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    *out = (size_t)count;
    return tir_items(c, (uint64_t)count);
}

static bool tir_array_input(tir_context *c, const void *rows, size_t count, size_t size) {
    if (count && !rows)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (count > SIZE_MAX / size)
        return tir_fail(c, CBM_TEST_RESULT_LIMIT);
    return tir_items(c, count);
}

static bool tir_label(tir_context *c, const char *s, tir_bytes *out) {
    if (!tir_cstring(c, s, out))
        return false;
    if (!out->length)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (!tir_utf8(c, *out))
        return tir_fail(c, CBM_TEST_RESULT_UNSUPPORTED);
    return true;
}

static bool tir_string_array(tir_context *c, const char *const *strings, int count,
                             tir_bytes **saved) {
    size_t n = 0;
    if (!tir_count(c, strings, count, &n))
        return false;
    tir_bytes *copy = saved ? tir_alloc(c, n, sizeof(*copy)) : NULL;
    if (n && saved && !copy)
        return false;
    for (size_t i = 0; i < n; i++) {
        tir_bytes b;
        if (!tir_step(c, 0, 1) || !tir_cstring(c, strings[i], &b))
            return false;
        if (saved)
            copy[i] = b;
    }
    if (saved)
        *saved = copy;
    return true;
}

static bool tir_read_rules(tir_context *c) {
    int count = 0;
    const cbm_test_rule_t *rows = cbm_test_policy_rules(c->input->policy, &count);
    if (!tir_count(c, rows, count, &c->rule_count))
        return false;
    c->rules = tir_alloc(c, c->rule_count, sizeof(*c->rules));
    if (count && !c->rules)
        return false;
    for (size_t i = 0; i < c->rule_count; i++) {
        const cbm_test_rule_t *row = &rows[i];
        tir_rule *out = &c->rules[i];
        if (!tir_step(c, 0, 1))
            return false;
        if (row->action < CBM_TEST_RULE_RUN_ALL || row->action > CBM_TEST_RULE_IGNORE)
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
        out->source = row;
        if (!tir_label(c, row->id, &out->id) ||
            !tir_string_array(c, row->paths, row->path_count, NULL) ||
            !tir_string_array(c, row->suites, row->suite_count, NULL) ||
            !tir_string_array(c, row->lanes, row->lane_count, &out->lanes))
            return false;
        for (size_t j = 0; j < i; j++) {
            if (!tir_step(c, 0, 1))
                return false;
            if (!tir_compare(c, out->id, c->rules[j].id))
                return tir_fail(c, CBM_TEST_RESULT_INVALID);
        }
    }
    return true;
}

static bool tir_read_lanes(tir_context *c) {
    int count = 0;
    const cbm_test_lane_t *rows = cbm_test_policy_lanes(c->input->policy, &count);
    if (!tir_count(c, rows, count, &c->lane_count))
        return false;
    c->lanes = tir_alloc(c, c->lane_count, sizeof(*c->lanes));
    if (count && !c->lanes)
        return false;
    for (size_t i = 0; i < c->lane_count; i++) {
        const cbm_test_lane_t *row = &rows[i];
        tir_lane *out = &c->lanes[i];
        out->source = row;
        if (!tir_step(c, 0, 1) || !tir_label(c, row->name, &out->name) ||
            !tir_string_array(c, row->suites, row->suite_count, NULL) ||
            !tir_string_array(c, row->exclude_suites, row->exclude_count, NULL) ||
            !tir_string_array(c, row->signals, row->signal_count, NULL))
            return false;
        for (size_t j = 0; j < i; j++) {
            if (!tir_step(c, 0, 1))
                return false;
            if (!tir_compare(c, out->name, c->lanes[j].name))
                return tir_fail(c, CBM_TEST_RESULT_INVALID);
        }
    }
    return true;
}

static int tir_hex_digit(unsigned char ch) {
    if (ch >= '0' && ch <= '9')
        return ch - '0';
    if (ch >= 'a' && ch <= 'f')
        return ch - 'a' + 10;
    return -1;
}

static bool tir_policy_digest(tir_context *c) {
    tir_bytes digest;
    if (!tir_cstring(c, cbm_test_policy_digest(c->input->policy), &digest))
        return false;
    const cbm_test_result_digest_t *supplied = &c->input->receipt->policy_sha256;
    if (digest.length != 64 || !supplied->present)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    for (size_t i = 0; i < 32; i++) {
        int a = tir_hex_digit(digest.data[i * 2]);
        int b = tir_hex_digit(digest.data[i * 2 + 1]);
        if (a < 0 || b < 0 || (unsigned)(a * 16 + b) != supplied->bytes[i])
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
    }
    return true;
}

static bool tir_read_model(tir_context *c) {
    if (!c->input->model)
        return true;
    int nr = 0, nd = 0;
    const cbm_test_runner_suite_t *r = cbm_test_model_runner_suites(c->input->model, &nr);
    const cbm_test_case_t *d = cbm_test_model_cases(c->input->model, &nd);
    if (!tir_count(c, r, nr, &c->runner_count) || !tir_count(c, d, nd, &c->definition_count))
        return false;
    c->runners = tir_alloc(c, c->runner_count, sizeof(*c->runners));
    c->definitions = tir_alloc(c, c->definition_count, sizeof(*c->definitions));
    if ((nr && !c->runners) || (nd && !c->definitions))
        return false;
    for (size_t i = 0; i < c->runner_count; i++) {
        tir_runner *out = &c->runners[i];
        if (!tir_step(c, 0, 1) || !tir_cstring(c, r[i].name, &out->name) ||
            !tir_identifier(c, out->name, true))
            return false;
        out->original = r[i].name;
        out->perf = r[i].perf;
    }
    for (size_t i = 0; i < c->definition_count; i++) {
        tir_model_case *out = &c->definitions[i];
        if (!tir_step(c, 0, 1) || !tir_cstring(c, d[i].file, &out->file) ||
            !tir_cstring(c, d[i].name, &out->name))
            return false;
        out->line = d[i].start_line;
    }
    return true;
}

static bool tir_mask(tir_context *c, unsigned mask, bool require) {
    if (((uint64_t)mask & ~TIR_SELECTION_MASK) || (require && !mask))
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    return true;
}

static bool tir_selection_suites(tir_context *c, const cbm_test_selected_suite_t *s) {
    for (size_t i = 0; i < c->suite_count; i++) {
        if (!tir_step(c, 0, 1) || !tir_mask(c, s[i].reasons, true) ||
            !tir_cstring(c, s[i].name, &c->suites[i].name) ||
            !tir_identifier(c, c->suites[i].name, true))
            return false;
        c->suites[i].reasons = s[i].reasons;
        c->suites[i].whole = s[i].whole;
    }
    return true;
}

static bool tir_selection_cases(tir_context *c, const cbm_test_selected_case_t *t) {
    for (size_t i = 0; i < c->case_count; i++) {
        tir_case *out = &c->cases[i];
        if (!tir_step(c, 0, 1) || !tir_mask(c, t[i].reasons, true) ||
            !tir_cstring(c, t[i].suite, &out->suite) || !tir_cstring(c, t[i].test, &out->test) ||
            !tir_cstring(c, t[i].file, &out->file))
            return false;
        if (!out->file.length || !out->test.length || !out->suite.length)
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
        out->reasons = t[i].reasons;
    }
    return true;
}

static bool tir_read_selection(tir_context *c) {
    const cbm_test_selection_t *selection = c->input->selection;
    if (!selection)
        return true;
    unsigned global = cbm_test_selection_run_all(selection);
    if (!tir_mask(c, global, false))
        return false;
    c->global |= global;
    int ns = 0, nt = 0;
    const cbm_test_selected_suite_t *s = cbm_test_selection_suites(selection, &ns);
    const cbm_test_selected_case_t *t = cbm_test_selection_cases(selection, &nt);
    if (!tir_count(c, s, ns, &c->suite_count) || !tir_count(c, t, nt, &c->case_count))
        return false;
    if ((global || c->input->comparison == CBM_TEST_RESULT_COMPARISON_EMPTY) && (ns || nt))
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    c->raw_suite_count = c->suite_count;
    c->raw_case_count = c->case_count;
    c->suites = tir_alloc(c, c->suite_count, sizeof(*c->suites));
    c->cases = tir_alloc(c, c->case_count, sizeof(*c->cases));
    if ((ns && !c->suites) || (nt && !c->cases))
        return false;
    return tir_selection_suites(c, s) && tir_selection_cases(c, t);
}

static bool tir_match_record(tir_context *c, const cbm_test_result_rule_match_t *match) {
    if (match->rule_index < 0 || (size_t)match->rule_index >= c->rule_count)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    tir_rule *rule = &c->rules[match->rule_index];
    if (rule->matched)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    rule->matched = true;
    rule->resolved = match->target_resolved;
    cbm_test_rule_action_t action = rule->source->action;
    if (!rule->resolved && (action == CBM_TEST_RULE_IGNORE || action == CBM_TEST_RULE_RUN_ALL))
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (!rule->resolved)
        c->global |= TIR_FALLBACK(CBM_TEST_RESULT_FALLBACK_RULE_TARGET_UNKNOWN);
    if (action == CBM_TEST_RULE_RUN_ALL)
        c->global |= TIR_FALLBACK(CBM_TEST_RESULT_FALLBACK_RULE_RUN_ALL);
    return true;
}

static bool tir_ledger(tir_context *c) {
    const cbm_test_result_input_t *in = c->input;
    if (!tir_array_input(c, in->matched_rules, in->matched_rule_count,
                         sizeof(*in->matched_rules)) ||
        !tir_array_input(c, in->fallbacks, in->fallback_count, sizeof(*in->fallbacks)))
        return false;
    if (in->matched_rule_count &&
        (!in->policy || in->comparison == CBM_TEST_RESULT_COMPARISON_EMPTY))
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    for (size_t i = 0; i < in->matched_rule_count; i++) {
        if (!tir_step(c, 0, 1) || !tir_match_record(c, &in->matched_rules[i]))
            return false;
    }
    for (size_t i = 0; i < in->fallback_count; i++) {
        unsigned code = (unsigned)in->fallbacks[i];
        if (!tir_step(c, 0, 1))
            return false;
        if (code >= CBM_TEST_RESULT_FALLBACK_COUNT)
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
        c->global |= TIR_FALLBACK(code);
    }
    return true;
}

static bool tir_read_warnings(tir_context *c) {
    const cbm_test_result_input_t *in = c->input;
    if (!tir_array_input(c, in->warnings, in->warning_count, sizeof(*in->warnings)))
        return false;
    if (in->warning_count > SIZE_MAX - c->raw_case_count)
        return tir_fail(c, CBM_TEST_RESULT_LIMIT);
    c->warning_capacity = in->warning_count + c->raw_case_count;
    c->warnings = tir_alloc(c, c->warning_capacity, sizeof(*c->warnings));
    if (c->warning_capacity && !c->warnings)
        return false;
    for (size_t i = 0; i < in->warning_count; i++) {
        const cbm_test_result_warning_t *w = &in->warnings[i];
        if (!tir_step(c, 0, 1))
            return false;
        if (w->code < 0 || w->code >= CBM_TEST_RESULT_WARNING_COUNT || (w->line && !w->file.length))
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
        if (!tir_span(c, w->file, false))
            return false;
        c->warnings[c->warning_count++] = (tir_warning){w->code, w->file, w->line};
    }
    return true;
}

static bool tir_pairs(tir_context *c) {
    uint64_t rows = 0;
    if (!tir_charge(c, &rows, c->runner_count, UINT64_MAX) ||
        !tir_charge(c, &rows, c->raw_suite_count, UINT64_MAX) ||
        !tir_charge(c, &rows, c->raw_case_count, UINT64_MAX))
        return false;
    if (c->lane_count && rows > UINT64_MAX / c->lane_count)
        return tir_fail(c, CBM_TEST_RESULT_LIMIT);
    return tir_items(c, rows * c->lane_count);
}

bool tir_preflight(tir_context *c) {
    int comparison = c->input->comparison;
    if (comparison < CBM_TEST_RESULT_COMPARISON_EMPTY ||
        comparison > CBM_TEST_RESULT_COMPARISON_UNAVAILABLE)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (!tir_items(c, 5) || !tir_read_model(c) || !tir_read_selection(c))
        return false;
    if (c->input->policy && (!tir_read_rules(c) || !tir_read_lanes(c) || !tir_policy_digest(c)))
        return false;
    return tir_ledger(c) && tir_read_warnings(c) && tir_receipt_validate(c) && tir_pairs(c);
}
