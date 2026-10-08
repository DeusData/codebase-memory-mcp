#include "mcp/test_impact_result_internal.h"

static tir_json *tir_result_object(tir_context *c, cbm_test_result_t *result) {
    const char *decision = c->global ? "run_all" : (c->runnable ? "selected" : "nothing");
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_add(c, o, "schema", tir_text(c, "cbm.test_impact.v0"));
    tir_add(c, o, "experimental", tir_bool(c, true));
    tir_add(c, o, "decision", tir_text(c, decision));
    tir_add(c, o, "run_all_reasons", c->global_json);
    tir_add(c, o, "lanes", c->lane_json);
    tir_add(c, o, "warnings", tir_warnings_json(c));
    tir_add(c, o, "totals", tir_totals_json(c));
    tir_add(c, o, "receipt", tir_receipt_json(c, result->digest));
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}

static bool tir_build_wire(tir_context *c, cbm_test_result_t *result) {
    for (size_t i = 0; i < 64; i++)
        result->digest[i] = '0';
    tir_json *object = tir_result_object(c, result);
    if (!object || !tir_render(c, object, &result->json, &result->length, true))
        return false;
    if (!tir_decision_hash(c, result->json, result->length, result->digest))
        return false;
    return tir_copy(c, result->json + c->digest_offset, result->digest, 64);
}

cbm_test_result_status_t cbm_test_result_build(const cbm_test_result_input_t *input,
                                               const cbm_test_result_limits_t *limits,
                                               cbm_test_result_cancel_fn cancelled,
                                               void *cancel_context, cbm_test_result_t **out) {
    if (!out)
        return CBM_TEST_RESULT_INVALID;
    *out = NULL;
    if (!input || !input->receipt || !limits || !limits->max_input_bytes || !limits->max_items ||
        !limits->max_alloc_bytes || !limits->max_output_bytes)
        return CBM_TEST_RESULT_INVALID;
    tir_context context = {0};
    context.input = input;
    context.limits = limits;
    context.cancelled = cancelled;
    context.cancel_context = cancel_context;
    cbm_arena_init_lazy(&context.arena, 4096);
    cbm_test_result_t *result = NULL;
    if (tir_poll(&context))
        result = tir_alloc(&context, 1, sizeof(*result));
    bool ok = result && tir_preflight(&context) && tir_prepare(&context) && tir_project(&context) &&
              tir_build_wire(&context, result) && tir_forget_borrowed(&context, result) &&
              tir_poll(&context);
    if (ok) {
        result->arena = context.arena;
        *out = result;
        return CBM_TEST_RESULT_OK;
    }
    cbm_arena_destroy(&context.arena);
    return context.status == CBM_TEST_RESULT_OK ? CBM_TEST_RESULT_INVALID : context.status;
}

const char *cbm_test_result_json(const cbm_test_result_t *result, size_t *length) {
    if (length)
        *length = result ? result->length : 0;
    return result ? result->json : NULL;
}

const char *cbm_test_result_decision_sha256(const cbm_test_result_t *result) {
    return result ? result->digest : NULL;
}

void cbm_test_result_free(cbm_test_result_t *result) {
    if (!result)
        return;
    CBMArena arena = result->arena;
    cbm_arena_destroy(&arena);
}
