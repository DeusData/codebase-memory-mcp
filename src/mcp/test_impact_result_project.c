#include "mcp/test_impact_result_internal.h"

typedef enum { TIR_SKIP, TIR_SELECTED, TIR_ALL } tir_lane_decision;
typedef struct {
    tir_json *suites;
    uint64_t reasons;
    size_t members;
    tir_lane_decision decision;
} tir_lane_plan;

static bool tir_membership(tir_context *c, size_t lane, const tir_runner *runner, bool *member) {
    if (runner->name.length > (SIZE_MAX / 2) - 1)
        return tir_fail(c, CBM_TEST_RESULT_LIMIT);
    if (!tir_reserve(c, 2 * (runner->name.length + 1)) || !tir_poll(c))
        return false;
    bool ok = cbm_test_policy_lane_selects_suite(c->input->policy, (int)lane, runner->original,
                                                 runner->perf, member);
    if (!tir_poll(c))
        return false;
    if (!ok)
        return tir_fail(c, CBM_TEST_RESULT_MATCH_FAILED);
    return true;
}

static bool tir_member_suite(tir_context *c, size_t lane_index, tir_lane_plan *plan,
                             const tir_runner *runner) {
    const tir_lane *lane = &c->lanes[lane_index];
    size_t found = tir_find_suite(c, runner->name);
    const tir_suite *selected = found == SIZE_MAX ? NULL : &c->suites[found];
    if (!c->global && lane->source->narrow && !selected)
        return c->status == CBM_TEST_RESULT_OK;
    bool whole = c->global || !lane->source->narrow || (selected && selected->whole);
    uint64_t reasons = c->global | (selected ? selected->reasons : 0);
    if (!lane->source->narrow)
        reasons |= TIR_REASON(TIR_REASON_NARROW_DISABLED);
    if (!tir_add(c, plan->suites, NULL, tir_suite_json(c, runner->name, selected, whole, reasons)))
        return false;
    if (c->global)
        return true;
    plan->reasons |= reasons;
    if (!tir_charge(c, &c->suite_occurrences, 1, UINT64_MAX))
        return false;
    if (whole)
        c->whole_occurrence = true;
    else if (!tir_charge(c, &c->test_occurrences, selected->count, UINT64_MAX))
        return false;
    return true;
}

static bool tir_members(tir_context *c, size_t lane_index, tir_lane_plan *plan) {
    for (size_t i = 0; i < c->runner_count; i++) {
        if (!tir_step(c, 0, 1))
            return false;
        bool member = false;
        if (!tir_membership(c, lane_index, &c->runners[i], &member))
            return false;
        if (!member)
            continue;
        plan->members++;
        if (!tir_member_suite(c, lane_index, plan, &c->runners[i]))
            return false;
    }
    return true;
}

static bool tir_lane_plan_build(tir_context *c, size_t index, tir_lane_plan *plan) {
    const tir_lane *lane = &c->lanes[index];
    plan->suites = tir_node(c, TIR_ARRAY);
    if (!plan->suites)
        return false;
    if (!c->global && c->input->comparison == CBM_TEST_RESULT_COMPARISON_EMPTY) {
        plan->reasons = TIR_REASON(TIR_REASON_NO_CHANGES);
        return true;
    }
    if (!lane->active) {
        plan->reasons = TIR_REASON(TIR_REASON_POLICY_INACTIVE);
        return true;
    }
    if (c->global) {
        plan->decision = TIR_ALL;
        plan->reasons = c->global;
        if (!lane->source->narrow)
            plan->reasons |= TIR_REASON(TIR_REASON_NARROW_DISABLED);
        if (!c->input->inventory_complete || !c->input->model ||
            !cbm_test_model_narrowable(c->input->model)) {
            plan->suites = tir_null(c);
            return plan->suites != NULL;
        }
        return tir_members(c, index, plan);
    }
    if (!tir_members(c, index, plan))
        return false;
    if (!plan->members)
        plan->reasons = TIR_REASON(TIR_REASON_NO_MEMBER_SUITES);
    else if (!plan->suites->as.children.first)
        plan->reasons = TIR_REASON(TIR_REASON_NO_SELECTED_TESTS);
    else {
        plan->decision = lane->source->narrow ? TIR_SELECTED : TIR_ALL;
        if (!tir_charge(c, &c->runnable, 1, UINT64_MAX))
            return false;
    }
    return true;
}

static tir_json *tir_because(tir_context *c, const tir_lane *lane, uint64_t reasons) {
    tir_json *activation = tir_node(c, TIR_ARRAY);
    if (lane->source->default_run)
        tir_add(c, activation, NULL, tir_text(c, "default"));
    if (lane->rules->as.children.first)
        tir_add(c, activation, NULL, tir_text(c, "rule"));
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_add(c, o, "activation", activation);
    tir_add(c, o, "rules", lane->rules);
    tir_add(c, o, "reasons", tir_reasons(c, reasons));
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}

static tir_json *tir_lane_json(tir_context *c, size_t index) {
    static const char *const decisions[] = {"skip", "selected", "run_all"};
    tir_lane_plan plan = {0};
    if (!tir_lane_plan_build(c, index, &plan))
        return NULL;
    tir_json *filter = tir_null(c);
    if (!c->global && plan.decision != TIR_SKIP)
        filter = tir_filter_json(c, plan.suites);
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_add(c, o, "lane", tir_string(c, c->lanes[index].name));
    tir_add(c, o, "decision", tir_text(c, decisions[plan.decision]));
    tir_add(c, o, "because", tir_because(c, &c->lanes[index], plan.reasons));
    tir_add(c, o, "suites", plan.suites);
    tir_add(c, o, "runner_filter", filter);
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}

bool tir_project(tir_context *c) {
    c->global_json = tir_reasons(c, c->global);
    if (!c->input->policy || (c->global && !c->input->activation_complete)) {
        c->lane_json = tir_null(c);
        return c->status == CBM_TEST_RESULT_OK;
    }
    c->lane_json = tir_node(c, TIR_ARRAY);
    for (size_t i = 0; i < c->lane_count; i++) {
        if (!tir_step(c, 0, 1) || !tir_add(c, c->lane_json, NULL, tir_lane_json(c, i)))
            return false;
    }
    return c->status == CBM_TEST_RESULT_OK;
}
