#include "mcp/test_impact_result_internal.h"

static bool tir_zero(tir_context *c, const unsigned char *p, size_t count) {
    for (size_t i = 0; i < count; i++) {
        if (!tir_step(c, 1, 0))
            return false;
        if (p[i])
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
    }
    return true;
}

static bool tir_check_digest(tir_context *c, cbm_test_result_digest_t d) {
    return d.present ? tir_input_bytes(c, 32) : tir_zero(c, d.bytes, 32);
}

static bool tir_check_oid(tir_context *c, cbm_test_result_oid_t oid) {
    if (oid.format < CBM_TEST_RESULT_OBJECT_UNKNOWN || oid.format > CBM_TEST_RESULT_OBJECT_SHA256)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (oid.format == CBM_TEST_RESULT_OBJECT_UNKNOWN)
        return tir_zero(c, oid.bytes, 32);
    if (oid.format != c->input->receipt->object_format)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    size_t count = oid.format == CBM_TEST_RESULT_OBJECT_SHA1 ? 20 : 32;
    return tir_input_bytes(c, count) && tir_zero(c, oid.bytes + count, 32 - count);
}

static bool tir_same_oid(tir_context *c, cbm_test_result_oid_t a, cbm_test_result_oid_t b) {
    if (a.format == CBM_TEST_RESULT_OBJECT_UNKNOWN || a.format != b.format)
        return false;
    return tir_compare(c, (tir_bytes){a.bytes, 32}, (tir_bytes){b.bytes, 32}) == 0 &&
           c->status == CBM_TEST_RESULT_OK;
}

static bool tir_check_rejections(tir_context *c, cbm_test_result_evidence_reasons_t reasons,
                                 int state) {
    if ((reasons.count && !reasons.values) || (state == 1 && !reasons.count) ||
        (state == 2 && reasons.count))
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (reasons.count > SIZE_MAX / sizeof(*reasons.values))
        return tir_fail(c, CBM_TEST_RESULT_LIMIT);
    if (!tir_items(c, reasons.count))
        return false;
    for (size_t i = 0; i < reasons.count; i++) {
        if (!tir_step(c, 0, 1))
            return false;
        if (reasons.values[i] < 0 || reasons.values[i] >= CBM_TEST_RESULT_EVIDENCE_COUNT)
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
    }
    return true;
}

static bool tir_check_graph(tir_context *c) {
    const cbm_test_result_graph_receipt_t *g = &c->input->receipt->graph;
    if (g->state < CBM_TEST_RESULT_GRAPH_UNAVAILABLE || g->state > CBM_TEST_RESULT_GRAPH_CERTIFIED)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (!tir_check_oid(c, g->commit) || !tir_span(c, g->generation, true) ||
        !tir_check_digest(c, g->sha256) || !tir_check_rejections(c, g->rejection_reasons, g->state))
        return false;
    if (g->state == CBM_TEST_RESULT_GRAPH_CERTIFIED &&
        (!g->sha256.present || !tir_same_oid(c, g->commit, c->input->receipt->head)))
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    return true;
}

static bool tir_check_applicability(tir_context *c) {
    const cbm_test_result_receipt_t *r = c->input->receipt;
    const cbm_test_result_applicability_receipt_t *a = &r->coverage.applicability;
    if (a->state < CBM_TEST_RESULT_APPLICABILITY_UNAVAILABLE ||
        a->state > CBM_TEST_RESULT_APPLICABILITY_ESTABLISHED ||
        a->kind < CBM_TEST_RESULT_APPLICABILITY_KIND_UNKNOWN ||
        a->kind > CBM_TEST_RESULT_APPLICABILITY_EXACT_QUERY)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (!tir_check_oid(c, a->artifact_commit) || !tir_check_oid(c, a->merge_base) ||
        !tir_check_oid(c, a->head) || !tir_check_digest(c, a->certificate_sha256) ||
        !tir_span(c, a->profile, true) || !tir_check_rejections(c, a->rejection_reasons, a->state))
        return false;
    if (a->kind == CBM_TEST_RESULT_APPLICABILITY_KIND_UNKNOWN && (a->version || a->profile.length))
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (a->state != CBM_TEST_RESULT_APPLICABILITY_ESTABLISHED)
        return true;
    bool binding = tir_same_oid(c, a->artifact_commit, r->coverage.commit) &&
                   tir_same_oid(c, a->merge_base, r->merge_base) &&
                   tir_same_oid(c, a->head, r->head);
    bool profile = a->kind == CBM_TEST_RESULT_APPLICABILITY_ALL_MODIFICATIONS
                       ? a->version == 2
                       : a->kind == CBM_TEST_RESULT_APPLICABILITY_EXACT_QUERY && a->profile.length;
    if (!binding || !profile || !a->version || !a->certificate_sha256.present ||
        r->coverage.state != CBM_TEST_RESULT_COVERAGE_ADMITTED)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    return true;
}

static bool tir_coverage_required(const cbm_test_result_coverage_receipt_t *v) {
    return v->format != CBM_TEST_RESULT_COVERAGE_FORMAT_UNKNOWN && v->artifact.length &&
           v->commit.format != CBM_TEST_RESULT_OBJECT_UNKNOWN && v->identities_sha256.present &&
           v->tests_sha256.present && v->metadata_sha256.present && v->graph_sha256.present &&
           v->compatibility_sha256.present && v->oldest_observation_at.present &&
           (v->format != CBM_TEST_RESULT_COVERAGE_FORMAT_PROFILES_V2 || v->image_sha256.present);
}

static bool tir_check_coverage(tir_context *c) {
    const cbm_test_result_coverage_receipt_t *v = &c->input->receipt->coverage;
    if (v->state < CBM_TEST_RESULT_COVERAGE_UNAVAILABLE ||
        v->state > CBM_TEST_RESULT_COVERAGE_ADMITTED ||
        v->format < CBM_TEST_RESULT_COVERAGE_FORMAT_UNKNOWN ||
        v->format > CBM_TEST_RESULT_COVERAGE_FORMAT_PROFILES_V2)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (!tir_span(c, v->artifact, true) || !tir_check_oid(c, v->commit) ||
        !tir_check_digest(c, v->image_sha256) || !tir_check_digest(c, v->identities_sha256) ||
        !tir_check_digest(c, v->tests_sha256) || !tir_check_digest(c, v->metadata_sha256) ||
        !tir_check_digest(c, v->graph_sha256) || !tir_check_digest(c, v->compatibility_sha256) ||
        !tir_check_rejections(c, v->rejection_reasons, v->state))
        return false;
    if (v->oldest_observation_at.present) {
        if (!tir_input_bytes(c, 8))
            return false;
    } else if (v->oldest_observation_at.value)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (v->state == CBM_TEST_RESULT_COVERAGE_ADMITTED && !tir_coverage_required(v))
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    return tir_check_applicability(c);
}

static bool tir_check_extensions(tir_context *c) {
    const cbm_test_result_receipt_t *r = c->input->receipt;
    if (r->extension_count && !r->extensions)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (!tir_items(c, r->extension_count))
        return false;
    c->extensions = tir_alloc(c, r->extension_count, sizeof(*c->extensions));
    if (r->extension_count && !c->extensions)
        return false;
    for (size_t i = 0; i < r->extension_count; i++) {
        const cbm_test_result_extension_t *e = &r->extensions[i];
        if (!tir_step(c, 0, 1) || !tir_span(c, e->language, true) || !tir_span(c, e->edge, true))
            return false;
        if (!e->language.length || !e->edge.length)
            return tir_fail(c, CBM_TEST_RESULT_INVALID);
        c->extensions[i] = *e;
    }
    return true;
}

bool tir_receipt_validate(tir_context *c) {
    const cbm_test_result_receipt_t *r = c->input->receipt;
    if (r->object_format < CBM_TEST_RESULT_OBJECT_UNKNOWN ||
        r->object_format > CBM_TEST_RESULT_OBJECT_SHA256 ||
        r->routes < CBM_TEST_RESULT_ROUTES_NOT_USED || r->routes > CBM_TEST_RESULT_ROUTES_UNKNOWN)
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    if (!tir_check_oid(c, r->base) || !tir_check_oid(c, r->head) ||
        !tir_check_oid(c, r->merge_base) || !tir_check_digest(c, r->diff_sha256) ||
        !tir_check_digest(c, r->name_status_sha256) || !tir_check_digest(c, r->config_sha256) ||
        !tir_check_digest(c, r->policy_sha256))
        return false;
    if (c->input->comparison != CBM_TEST_RESULT_COMPARISON_UNAVAILABLE &&
        (r->object_format == CBM_TEST_RESULT_OBJECT_UNKNOWN ||
         r->base.format == CBM_TEST_RESULT_OBJECT_UNKNOWN ||
         r->head.format == CBM_TEST_RESULT_OBJECT_UNKNOWN ||
         r->merge_base.format == CBM_TEST_RESULT_OBJECT_UNKNOWN || !r->diff_sha256.present ||
         !r->name_status_sha256.present))
        return tir_fail(c, CBM_TEST_RESULT_INVALID);
    return tir_check_graph(c) && tir_check_coverage(c) && tir_check_extensions(c);
}

static tir_json *tir_optional(tir_context *c, tir_bytes b) {
    return b.length ? tir_string(c, b) : tir_null(c);
}

static tir_json *tir_graph_json(tir_context *c) {
    static const char *const states[] = {"unavailable", "rejected", "certified"};
    const cbm_test_result_graph_receipt_t *g = &c->input->receipt->graph;
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_add(c, o, "state", tir_text(c, states[g->state]));
    tir_add(c, o, "commit", tir_oid(c, g->commit));
    tir_add(c, o, "generation", tir_optional(c, g->generation));
    tir_add(c, o, "sha256", tir_digest(c, g->sha256));
    tir_add(c, o, "rejection_reasons", tir_evidence(c, g->rejection_reasons));
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}

static tir_json *tir_applicability_json(tir_context *c) {
    static const char *const states[] = {"unavailable", "rejected", "established"};
    const cbm_test_result_applicability_receipt_t *a = &c->input->receipt->coverage.applicability;
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_json *kind = tir_null(c);
    if (a->kind == CBM_TEST_RESULT_APPLICABILITY_ALL_MODIFICATIONS)
        kind = tir_text(c, "all_modifications");
    if (a->kind == CBM_TEST_RESULT_APPLICABILITY_EXACT_QUERY)
        kind = tir_text(c, "exact_query");
    tir_add(c, o, "state", tir_text(c, states[a->state]));
    tir_add(c, o, "kind", kind);
    tir_add(c, o, "version", a->version ? tir_uint(c, a->version) : tir_null(c));
    tir_add(c, o, "profile", tir_optional(c, a->profile));
    tir_add(c, o, "certificate_sha256", tir_digest(c, a->certificate_sha256));
    tir_add(c, o, "artifact_commit", tir_oid(c, a->artifact_commit));
    tir_add(c, o, "merge_base", tir_oid(c, a->merge_base));
    tir_add(c, o, "head", tir_oid(c, a->head));
    tir_add(c, o, "rejection_reasons", tir_evidence(c, a->rejection_reasons));
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}

static tir_json *tir_coverage_json(tir_context *c) {
    static const char *const states[] = {"unavailable", "rejected", "admitted"};
    const cbm_test_result_coverage_receipt_t *v = &c->input->receipt->coverage;
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_json *kind = tir_null(c);
    if (v->format == CBM_TEST_RESULT_COVERAGE_FORMAT_FUNCTIONS_V1)
        kind = tir_text(c, "functions");
    if (v->format == CBM_TEST_RESULT_COVERAGE_FORMAT_PROFILES_V2)
        kind = tir_text(c, "profiles");
    tir_add(c, o, "state", tir_text(c, states[v->state]));
    tir_add(c, o, "format", v->format ? tir_uint(c, (uint64_t)v->format) : tir_null(c));
    tir_add(c, o, "identity_kind", kind);
    tir_add(c, o, "artifact", tir_optional(c, v->artifact));
    tir_add(c, o, "commit", tir_oid(c, v->commit));
    tir_add(c, o, "image_sha256", tir_digest(c, v->image_sha256));
    tir_add(c, o, "identities_sha256", tir_digest(c, v->identities_sha256));
    tir_add(c, o, "tests_sha256", tir_digest(c, v->tests_sha256));
    tir_add(c, o, "metadata_sha256", tir_digest(c, v->metadata_sha256));
    tir_add(c, o, "graph_sha256", tir_digest(c, v->graph_sha256));
    tir_add(c, o, "compatibility_sha256", tir_digest(c, v->compatibility_sha256));
    tir_add(c, o, "oldest_observation_at",
            v->oldest_observation_at.present ? tir_int(c, v->oldest_observation_at.value)
                                             : tir_null(c));
    tir_add(c, o, "rejection_reasons", tir_evidence(c, v->rejection_reasons));
    tir_add(c, o, "applicability", tir_applicability_json(c));
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}

static tir_json *tir_traversal_json(tir_context *c) {
    static const char *const edges[] = {"ASYNC_CALLS", "CALLS", "CALL_REFERENCE", "READS", "USAGE"};
    const cbm_test_result_receipt_t *r = c->input->receipt;
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_json *defaults = tir_node(c, TIR_ARRAY), *extensions = tir_node(c, TIR_ARRAY);
    for (size_t i = 0; i < 5; i++)
        tir_add(c, defaults, NULL, tir_text(c, edges[i]));
    for (size_t i = 0; i < r->extension_count; i++) {
        if (!tir_step(c, 0, 1))
            return NULL;
        tir_json *e = tir_node(c, TIR_OBJECT);
        tir_add(c, e, "language", tir_string(c, c->extensions[i].language));
        tir_add(c, e, "edge", tir_string(c, c->extensions[i].edge));
        tir_add(c, extensions, NULL, e);
    }
    tir_add(c, o, "default_edges", defaults);
    tir_add(c, o, "extensions", extensions);
    tir_add(c, o, "direction", tir_text(c, "inbound"));
    tir_add(c, o, "fixpoint", tir_bool(c, true));
    tir_add(c, o, "lift", tir_bool(c, false));
    tir_add(c, o, "include_low_confidence", tir_bool(c, true));
    tir_add(c, o, "follow_routes",
            r->routes == CBM_TEST_RESULT_ROUTES_UNKNOWN
                ? tir_null(c)
                : tir_bool(c, r->routes == CBM_TEST_RESULT_ROUTES_FOLLOWED));
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}

tir_json *tir_receipt_json(tir_context *c, const char *digest) {
    const cbm_test_result_receipt_t *r = c->input->receipt;
    tir_json *o = tir_node(c, TIR_OBJECT);
    tir_json *format = tir_null(c);
    if (r->object_format == CBM_TEST_RESULT_OBJECT_SHA1)
        format = tir_text(c, "sha1");
    if (r->object_format == CBM_TEST_RESULT_OBJECT_SHA256)
        format = tir_text(c, "sha256");
    tir_add(c, o, "object_format", format);
    tir_add(c, o, "base", tir_oid(c, r->base));
    tir_add(c, o, "head", tir_oid(c, r->head));
    tir_add(c, o, "merge_base", tir_oid(c, r->merge_base));
    tir_add(c, o, "diff_sha256", tir_digest(c, r->diff_sha256));
    tir_add(c, o, "name_status_sha256", tir_digest(c, r->name_status_sha256));
    tir_add(c, o, "config_sha256", tir_digest(c, r->config_sha256));
    tir_add(c, o, "policy_sha256", tir_digest(c, r->policy_sha256));
    tir_add(c, o, "graph", tir_graph_json(c));
    tir_add(c, o, "coverage", tir_coverage_json(c));
    tir_add(c, o, "traversal", tir_traversal_json(c));
    c->digest_json = tir_text(c, digest);
    tir_add(c, o, "decision_sha256", c->digest_json);
    return c->status == CBM_TEST_RESULT_OK ? o : NULL;
}
