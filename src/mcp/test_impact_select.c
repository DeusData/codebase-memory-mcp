/* Pure selection: static no-lift reachability UNION per-test coverage.
 * Only complete, admitted evidence can justify an omission. */
#include "mcp/test_impact.h"
#include "foundation/arena.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct cbm_test_selection {
    CBMArena arena;
    unsigned run_all;
    cbm_test_selected_suite_t *suites;
    int suite_count;
    cbm_test_selected_case_t *cases;
    int case_count;
};

typedef struct {
    const cbm_test_reach_t *row;
    bool registered;
} sl_reach_t;

static void *sl_array(cbm_test_selection_t *result, int count, size_t size) {
    if (count <= 0 || (size_t)count > SIZE_MAX / size)
        return NULL;
    return cbm_arena_calloc(&result->arena, (size_t)count * size);
}

static int sl_key(const char *af, const char *an, const char *bf, const char *bn) {
    int cmp = strcmp(af, bf);
    return cmp ? cmp : strcmp(an, bn);
}

static int sl_reach_compare(const void *left, const void *right) {
    const cbm_test_reach_t *a = ((const sl_reach_t *)left)->row;
    const cbm_test_reach_t *b = ((const sl_reach_t *)right)->row;
    return sl_key(a->file, a->test, b->file, b->test);
}

static sl_reach_t *sl_reach_find(sl_reach_t *rows, int count, const char *file, const char *name) {
    int lo = 0, hi = count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        int cmp = sl_key(file, name, rows[mid].row->file, rows[mid].row->test);
        if (!cmp)
            return &rows[mid];
        if (cmp < 0)
            hi = mid;
        else
            lo = mid + 1;
    }
    return NULL;
}

static int sl_case_compare(const void *left, const void *right) {
    const cbm_test_case_t *a = *(const cbm_test_case_t *const *)left;
    const cbm_test_case_t *b = *(const cbm_test_case_t *const *)right;
    return sl_key(a->file, a->name, b->file, b->name);
}

/* Ambiguous definitions, including alternative preprocessor definitions, do
 * not establish a unique graph/source identity. Return no mapping. */
static const cbm_test_case_t *sl_case_find(const cbm_test_case_t **rows, int count,
                                           const char *file, const char *name) {
    int lo = 0, hi = count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        int cmp = sl_key(file, name, rows[mid]->file, rows[mid]->name);
        if (!cmp) {
            if ((mid && sl_case_compare(&rows[mid - 1], &rows[mid]) == 0) ||
                (mid + 1 < count && sl_case_compare(&rows[mid], &rows[mid + 1]) == 0))
                return NULL;
            return rows[mid];
        }
        if (cmp < 0)
            hi = mid;
        else
            lo = mid + 1;
    }
    return NULL;
}

static int sl_suite_find(const cbm_test_selected_suite_t *rows, int count, const char *name) {
    int lo = 0, hi = count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        int cmp = strcmp(name, rows[mid].name);
        if (!cmp)
            return mid;
        if (cmp < 0)
            hi = mid;
        else
            lo = mid + 1;
    }
    return -1;
}

static int sl_registration_compare(const void *left, const void *right) {
    const cbm_test_registration_t *a = *(const cbm_test_registration_t *const *)left;
    const cbm_test_registration_t *b = *(const cbm_test_registration_t *const *)right;
    int cmp = sl_key(a->suite, a->test, b->suite, b->test);
    return cmp ? cmp : strcmp(a->file, b->file);
}

static void sl_whole(cbm_test_selected_suite_t *suite, unsigned reason) {
    suite->whole = true;
    suite->reasons |= reason;
}

static cbm_test_selection_t *sl_full(cbm_test_selection_t *result, unsigned reasons) {
    result->run_all |= reasons;
    result->suite_count = 0;
    result->case_count = 0;
    return result;
}

cbm_test_selection_t *cbm_test_select(const cbm_test_selection_input_t *input) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_test_selection_t *result = cbm_arena_calloc(&arena, sizeof(*result));
    if (!result) {
        cbm_arena_destroy(&arena);
        return NULL;
    }
    result->arena = arena;
    if (!input || input->reach_count < 0 || input->changed_function_count < 0 ||
        input->suite_trigger_count < 0 || (input->suite_trigger_count && !input->suite_triggers) ||
        (input->reach_count && !input->reach) ||
        (input->changed_function_count && !input->changed_function_ids))
        return sl_full(result, CBM_TEST_SELECT_INVALID_INPUT);
    if (!input->diff_complete)
        return sl_full(result, CBM_TEST_SELECT_DIFF_INCOMPLETE);
    /* Empty is asserted from the complete diff, not inferred from the number
     * of graph seeds or coverage IDs that happened to resolve. */
    if (!input->has_changes)
        return result;
    unsigned incomplete = 0;
    if (!input->inventory_complete || !cbm_test_model_narrowable(input->model))
        incomplete |= CBM_TEST_SELECT_INVENTORY_UNKNOWN;
    if (!input->static_complete)
        incomplete |= CBM_TEST_SELECT_STATIC_INCOMPLETE;
    bool coverage_ok = input->coverage_admitted && input->coverage;
    if (coverage_ok && !input->coverage_changes_complete)
        incomplete |= CBM_TEST_SELECT_CHANGE_IDENTITY_UNKNOWN;
    if (incomplete)
        return sl_full(result, incomplete);
    if (coverage_ok) {
        int functions = cbm_coverage_map_id_count(input->coverage);
        for (int i = 0; i < input->changed_function_count; i++) {
            if (input->changed_function_ids[i] < 0 || input->changed_function_ids[i] >= functions)
                return sl_full(result, CBM_TEST_SELECT_INVALID_INPUT);
        }
    }

    int runner_count, definition_count, registration_count, case_count;
    const cbm_test_runner_suite_t *runners =
        cbm_test_model_runner_suites(input->model, &runner_count);
    const cbm_test_suite_t *definitions = cbm_test_model_suites(input->model, &definition_count);
    const cbm_test_registration_t *registrations =
        cbm_test_model_registrations(input->model, &registration_count);
    const cbm_test_case_t *cases = cbm_test_model_cases(input->model, &case_count);
    if (!runner_count)
        return sl_full(result, CBM_TEST_SELECT_INVENTORY_UNKNOWN);
    result->suites = sl_array(result, runner_count, sizeof(*result->suites));
    result->cases = sl_array(result, registration_count, sizeof(*result->cases));
    int *definition_counts = sl_array(result, runner_count, sizeof(*definition_counts));
    int *registration_counts = sl_array(result, runner_count, sizeof(*registration_counts));
    sl_reach_t *reach = sl_array(result, input->reach_count, sizeof(*reach));
    const cbm_test_case_t **case_order = sl_array(result, case_count, sizeof(*case_order));
    const cbm_test_registration_t **order = sl_array(result, registration_count, sizeof(*order));
    if (!result->suites || !definition_counts || !registration_counts ||
        (registration_count && (!result->cases || !order)) || (input->reach_count && !reach) ||
        (case_count && !case_order))
        goto exhausted;
    result->suite_count = runner_count;
    for (int i = 0; i < runner_count; i++) {
        cbm_test_selected_suite_t *suite = &result->suites[i];
        suite->name = cbm_arena_strdup(&result->arena, runners[i].name);
        if (!suite->name)
            goto exhausted;
        if (!coverage_ok) {
            sl_whole(suite, CBM_TEST_SELECT_ARTIFACT_REJECTED);
            continue;
        }
        const cbm_coverage_test_t *setup =
            cbm_coverage_map_find_test(input->coverage, suite->name, "*");
        if (!setup || !setup->complete)
            sl_whole(suite, CBM_TEST_SELECT_SETUP_UNKNOWN);
        if (cbm_coverage_test_intersects(setup, input->changed_function_ids,
                                         input->changed_function_count))
            sl_whole(suite, CBM_TEST_SELECT_SETUP_HIT);
    }
    for (int i = 0; i < input->suite_trigger_count; i++) {
        const cbm_test_suite_trigger_t *trigger = &input->suite_triggers[i];
        if (!trigger->suite || !*trigger->suite)
            return sl_full(result, CBM_TEST_SELECT_INVALID_INPUT);
        int at = sl_suite_find(result->suites, runner_count, trigger->suite);
        if (at < 0)
            return sl_full(result, CBM_TEST_SELECT_INVENTORY_UNKNOWN);
        unsigned reasons = (trigger->reached ? CBM_TEST_SELECT_STATIC : 0) |
                           (trigger->changed ? CBM_TEST_SELECT_CHANGED : 0) |
                           (trigger->rule ? CBM_TEST_SELECT_RULE : 0);
        if (reasons)
            sl_whole(&result->suites[at], reasons);
    }
    for (int i = 0; i < definition_count; i++) {
        int at = sl_suite_find(result->suites, runner_count, definitions[i].name);
        if (at >= 0) {
            definition_counts[at]++;
            if (definitions[i].macro_registrations || definitions[i].uncertain)
                sl_whole(&result->suites[at], CBM_TEST_SELECT_INVENTORY_UNKNOWN);
        }
    }
    for (int i = 0; i < input->reach_count; i++) {
        if (!input->reach[i].file || !*input->reach[i].file || !input->reach[i].test ||
            !*input->reach[i].test)
            return sl_full(result, CBM_TEST_SELECT_INVALID_INPUT);
        reach[i].row = &input->reach[i];
    }
    if (input->reach_count > 1) {
        qsort(reach, (size_t)input->reach_count, sizeof(*reach), sl_reach_compare);
        for (int i = 1; i < input->reach_count; i++)
            if (sl_reach_compare(&reach[i - 1], &reach[i]) == 0)
                return sl_full(result, CBM_TEST_SELECT_INVALID_INPUT);
    }
    for (int i = 0; i < case_count; i++)
        case_order[i] = &cases[i];
    if (case_count > 1)
        qsort(case_order, (size_t)case_count, sizeof(*case_order), sl_case_compare);
    for (int i = 0; i < registration_count; i++)
        order[i] = &registrations[i];
    if (registration_count > 1)
        qsort(order, (size_t)registration_count, sizeof(*order), sl_registration_compare);

    for (int i = 0; i < registration_count; i++) {
        const cbm_test_registration_t *reg = order[i];
        int at = sl_suite_find(result->suites, runner_count, reg->suite);
        sl_reach_t *evidence = sl_reach_find(reach, input->reach_count, reg->file, reg->test);
        if (evidence)
            evidence->registered = true;
        if (at < 0)
            /* A suite this runner never registers runs elsewhere (another
             * runner's list) or not at all: its tests are outside this
             * selection, and a change to one is no unregistered change. */
            continue;
        cbm_test_selected_suite_t *suite = &result->suites[at];
        registration_counts[at]++;
        const cbm_test_case_t *test = sl_case_find(case_order, case_count, reg->file, reg->test);
        if (!reg->resolved || !test)
            sl_whole(suite, CBM_TEST_SELECT_INVENTORY_UNKNOWN);
        bool repeated =
            i > 0 && sl_key(order[i - 1]->suite, order[i - 1]->test, reg->suite, reg->test) == 0;
        if (repeated && strcmp(order[i - 1]->file, reg->file) != 0)
            sl_whole(suite, CBM_TEST_SELECT_INVENTORY_UNKNOWN);
        unsigned reasons = 0;
        if (!evidence || !evidence->row->mapped)
            reasons |= CBM_TEST_SELECT_UNMAPPED;
        if (evidence && evidence->row->reached)
            reasons |= CBM_TEST_SELECT_STATIC;
        if (evidence && evidence->row->changed)
            reasons |= CBM_TEST_SELECT_CHANGED;
        if (coverage_ok) {
            const cbm_coverage_test_t *covered =
                cbm_coverage_map_find_test(input->coverage, reg->suite, reg->test);
            if (!covered || !covered->complete)
                reasons |= CBM_TEST_SELECT_COVERAGE_UNKNOWN;
            if (cbm_coverage_test_intersects(covered, input->changed_function_ids,
                                             input->changed_function_count))
                reasons |= CBM_TEST_SELECT_COVERAGE;
        }
        suite->reasons |= reasons;
        if (reasons && (reg->conditional || (test && test->conditional)))
            sl_whole(suite, CBM_TEST_SELECT_CONDITIONAL);
        if (suite->whole || !reasons)
            continue;
        if (repeated && result->case_count &&
            sl_key(result->cases[result->case_count - 1].suite,
                   result->cases[result->case_count - 1].test, reg->suite, reg->test) == 0) {
            result->cases[result->case_count - 1].reasons |= reasons;
            continue;
        }
        cbm_test_selected_case_t *out = &result->cases[result->case_count++];
        out->suite = suite->name;
        out->test = cbm_arena_strdup(&result->arena, reg->test);
        out->file = cbm_arena_strdup(&result->arena, reg->file);
        out->reasons = reasons;
        if (!out->test || !out->file)
            goto exhausted;
    }
    for (int i = 0; i < input->reach_count; i++)
        if (reach[i].row->changed && !reach[i].registered)
            return sl_full(result, CBM_TEST_SELECT_CHANGED_UNREGISTERED);
    for (int i = 0; i < runner_count; i++)
        if (definition_counts[i] != 1 || !registration_counts[i])
            sl_whole(&result->suites[i], CBM_TEST_SELECT_INVENTORY_UNKNOWN);
    /* A later registration can promote an earlier test's suite to whole.
     * Compact cases while the complete, sorted suite table is still present. */
    int used = 0;
    for (int i = 0; i < result->case_count; i++) {
        int at = sl_suite_find(result->suites, runner_count, result->cases[i].suite);
        if (!result->suites[at].whole)
            result->cases[used++] = result->cases[i];
    }
    result->case_count = used;
    used = 0;
    for (int i = 0; i < runner_count; i++)
        if (result->suites[i].whole || result->suites[i].reasons)
            result->suites[used++] = result->suites[i];
    result->suite_count = used;
    return result;
exhausted:
    cbm_test_selection_free(result);
    return NULL;
}

void cbm_test_selection_free(cbm_test_selection_t *selection) {
    if (selection) {
        CBMArena arena = selection->arena;
        cbm_arena_destroy(&arena);
    }
}

unsigned cbm_test_selection_run_all(const cbm_test_selection_t *selection) {
    return selection ? selection->run_all : CBM_TEST_SELECT_INVALID_INPUT;
}

const cbm_test_selected_suite_t *cbm_test_selection_suites(const cbm_test_selection_t *selection,
                                                           int *count) {
    if (count)
        *count = selection ? selection->suite_count : 0;
    return selection ? selection->suites : NULL;
}

const cbm_test_selected_case_t *cbm_test_selection_cases(const cbm_test_selection_t *selection,
                                                         int *count) {
    if (count)
        *count = selection ? selection->case_count : 0;
    return selection ? selection->cases : NULL;
}
