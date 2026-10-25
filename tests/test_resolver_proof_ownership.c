#include "test_framework.h"
#include "cbm.h"
#include "resolve/proof.h"
#include "resolve/policy.h"
#include "graph_buffer/graph_buffer.h"
#include "result_spill.h"
#include "pipeline/pass_lsp_cross.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include <stdio.h>
#include <string.h>

static cbm_resolve_result_t ownership_outcome(void) {
    return (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                  .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                  .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .rule = CBM_RESOLVE_RULE_DECLARED_TYPE,
                                  .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                           CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                                           CBM_RESOLVE_FACT_TYPE_BOUND,
                                  .target = "project.Widget",
                                  .candidate_count = 1,
                                  .reason = "exact_type"};
}
static const cbm_resolve_proof_t *ownership_dag(cbm_resolve_proof_pool_t *pool) {
    cbm_resolve_result_t outcome = ownership_outcome();
    cbm_resolve_proof_source_t source = {.project = "project",
                                         .file = "caller.py",
                                         .module = "project.caller",
                                         .scope = "project.caller.main",
                                         .content_identity = "bytes-v1",
                                         .configuration_identity = "grammar-v1",
                                         .source_origin = 1};
    cbm_resolve_proof_key_t key = {.lookup_domain = "project.Widget",
                                   .lookup_key = "name",
                                   .declaration_identity = "declaration",
                                   .binding_identity = "binding",
                                   .type_identity = "type",
                                   .detail = "audited",
                                   .start_byte = 10,
                                   .end_byte = 14};
    const cbm_resolve_proof_t *inputs[2];
    inputs[0] = cbm_resolve_proof_leaf(pool, CBM_PROOF_SOURCE_FACT, &source, &key, &outcome);
    key.binding_identity = "other-binding";
    inputs[1] = cbm_resolve_proof_leaf(pool, CBM_PROOF_SOURCE_FACT, &source, &key, &outcome);
    return cbm_resolve_proof_derive(pool, CBM_PROOF_TYPE_JOIN, &source, &key, &outcome, inputs, 2,
                                    true);
}
static CBMResolvedCall ownership_row(const cbm_resolve_proof_t *proof, uint32_t at) {
    return (CBMResolvedCall){.caller_qn = "project.caller.main",
                             .callee_qn = "project.Widget",
                             .strategy = "exact_test",
                             .reason = "proof_lifetime",
                             .confidence = 1.0f,
                             .site_start_byte = at,
                             .site_end_byte = at + 4,
                             .evidence = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                          .rule = CBM_RESOLVE_RULE_DECLARED_TYPE,
                                          .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                                   CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                                                   CBM_RESOLVE_FACT_TYPE_BOUND,
                                          .proof = proof}};
}
static bool ownership_range(const CBMArena *arena, const void *pointer, size_t bytes) {
    if (!pointer || arena->nblocks != 1)
        return false;
    uintptr_t base = (uintptr_t)arena->blocks[0], value = (uintptr_t)pointer;
    return value >= base && value - base <= arena->used && bytes <= arena->used - (value - base);
}
static bool ownership_node(const CBMArena *arena, const cbm_resolve_proof_t *node, unsigned depth) {
    if (depth > 256 || !ownership_range(arena, node, sizeof(*node)))
        return false;
    const cbm_resolve_proof_view_t *v = &node->value;
    const char *strings[] = {v->source.project,
                             v->source.file,
                             v->source.module,
                             v->source.scope,
                             v->source.content_identity,
                             v->source.configuration_identity,
                             v->key.lookup_domain,
                             v->key.lookup_key,
                             v->key.declaration_identity,
                             v->key.binding_identity,
                             v->key.type_identity,
                             v->key.detail,
                             v->outcome.target,
                             v->outcome.reason};
    for (size_t i = 0; i < sizeof(strings) / sizeof(strings[0]); i++)
        if (strings[i] && !ownership_range(arena, strings[i], 1))
            return false;
    if (v->premise_count &&
        !ownership_range(arena, v->premises, v->premise_count * sizeof(*v->premises)))
        return false;
    for (size_t i = 0; i < v->premise_count; i++)
        if (!ownership_node(arena, v->premises[i], depth + 1))
            return false;
    return v->outcome.proof == NULL;
}

TEST(resolver_proof_compaction_owns_and_shares_every_support) {
    CBMFileResult *result =
        cbm_extract_file("", 0, CBM_LANG_PYTHON, "project", "caller.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &result->arena, 0, 0, 0);
    const cbm_resolve_proof_t *proof = ownership_dag(&pool);
    ASSERT_NOT_NULL(proof);
    uint64_t fingerprint = proof->value.fingerprint;
    cbm_resolvedcall_push(&result->resolved_calls, &result->arena, ownership_row(proof, 10));
    cbm_resolvedcall_push(&result->resolved_calls, &result->arena, ownership_row(proof, 20));
    cbm_result_compact(result);
    const cbm_resolve_proof_t *copy = result->resolved_calls.items[0].evidence.proof;
    bool owned = copy != proof && ownership_node(&result->arena, copy, 1);
    if (!owned) {
        cbm_free_result(result);
        ASSERT(owned);
    }
    ASSERT(copy == result->resolved_calls.items[1].evidence.proof);
    ASSERT_EQ(copy->value.fingerprint, fingerprint);
    ASSERT_EQ(copy->value.premise_count, 2);
    ASSERT_STR_EQ(copy->value.source.configuration_identity, "grammar-v1");
    size_t bytes = result->arena.used;
    cbm_result_compact(result);
    ASSERT_EQ(result->arena.used, bytes);
    ASSERT(ownership_node(&result->arena, result->resolved_calls.items[0].evidence.proof, 1));
    ASSERT(result->resolved_calls.items[0].evidence.proof ==
           result->resolved_calls.items[1].evidence.proof);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_proof_relocation_moves_nodes_arrays_and_shared_roots) {
    CBMFileResult source = {0};
    cbm_arena_init_exact(&source.arena, 128 * 1024);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &source.arena, 0, 0, 0);
    const cbm_resolve_proof_t *proof = ownership_dag(&pool);
    ASSERT_NOT_NULL(proof);
    cbm_resolvedcall_push(&source.resolved_calls, &source.arena, ownership_row(proof, 10));
    cbm_resolvedcall_push(&source.resolved_calls, &source.arena, ownership_row(proof, 20));
    ASSERT_EQ(source.arena.nblocks, 1);
    CBMFileResult destination = source;
    cbm_arena_init_exact(&destination.arena, source.arena.used);
    memcpy(destination.arena.blocks[0], source.arena.blocks[0], source.arena.used);
    destination.arena.used = source.arena.used;
    cbm_result_relocate(&destination, source.arena.blocks[0], source.arena.used,
                        destination.arena.blocks[0]);
    bool owned =
        ownership_node(&destination.arena, destination.resolved_calls.items[0].evidence.proof, 1);
    bool shared = destination.resolved_calls.items[0].evidence.proof ==
                  destination.resolved_calls.items[1].evidence.proof;
    cbm_arena_destroy(&source.arena);
    if (owned) {
        ASSERT_STR_EQ(
            destination.resolved_calls.items[0].evidence.proof->value.source.content_identity,
            "bytes-v1");
    }
    cbm_arena_destroy(&destination.arena);
    ASSERT(owned);
    ASSERT(shared);
    PASS();
}

TEST(resolver_proof_cross_append_and_supersede_take_ownership) {
    for (int supersede = 0; supersede < 2; supersede++) {
        CBMArena source, destination;
        cbm_arena_init(&source);
        cbm_arena_init(&destination);
        cbm_resolve_proof_pool_t pool;
        cbm_resolve_proof_pool_init(&pool, &source, 0, 0, 0);
        const cbm_resolve_proof_t *proof = ownership_dag(&pool);
        ASSERT_NOT_NULL(proof);
        CBMResolvedCallArray input = {0}, output = {0};
        CBMResolvedCall row = ownership_row(proof, 10);
        if (supersede) {
            CBMResolvedCall old = ownership_row(NULL, 10);
            old.callee_qn = "project.decoy";
            cbm_resolvedcall_push(&output, &destination, old);
            row.strategy = "lsp_cross_crate";
        }
        cbm_resolvedcall_push(&input, &source, row);
        cbm_resolvedcall_push(&input, &source, ownership_row(proof, 20));
        cbm_pxc_append_resolutions_for_test(&destination, &output, &input);
        ASSERT_EQ(output.count, 2);
        const cbm_resolve_proof_t *copy = output.items[0].evidence.proof;
        bool owned = copy && copy != proof;
        bool shared = copy == output.items[1].evidence.proof;
        cbm_arena_destroy(&source);
        if (owned) {
            ASSERT_STR_EQ(copy->value.source.configuration_identity, "grammar-v1");
            ASSERT_EQ(copy->value.premise_count, 2);
        }
        cbm_arena_destroy(&destination);
        ASSERT(owned);
        ASSERT(shared);
    }
    PASS();
}

TEST(resolver_proof_spill_rejects_borrowed_dag_and_round_trips_owned_dag) {
    CBMFileResult *result =
        cbm_extract_file("", 0, CBM_LANG_PYTHON, "project", "caller.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena foreign;
    cbm_arena_init(&foreign);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &foreign, 0, 0, 0);
    const cbm_resolve_proof_t *proof = ownership_dag(&pool);
    ASSERT_NOT_NULL(proof);
    cbm_resolvedcall_push(&result->resolved_calls, &result->arena, ownership_row(proof, 10));
    char directory[512];
    snprintf(directory, sizeof(directory), "%s/cbm_proof_XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(directory));
    size_t saved = cbm_result_spill_pin_free_bytes_for_tests(SIZE_MAX);
    cbm_result_spill_t *spill = cbm_result_spill_open(directory, 1, 2);
    cbm_result_spill_pin_free_bytes_for_tests(saved);
    ASSERT_NOT_NULL(spill);
    bool parked_borrowed = cbm_result_spill_park(spill, 0, 0, result);
    if (parked_borrowed)
        result = NULL;
    bool ok = !parked_borrowed;
    if (result) {
        cbm_result_compact(result);
        ok =
            ownership_node(&result->arena, result->resolved_calls.items[0].evidence.proof, 1) && ok;
        if (ok && cbm_result_spill_park(spill, 0, 1, result)) {
            result = NULL;
            cbm_arena_destroy(&foreign);
            for (int i = 0; i < 2; i++) {
                CBMFileResult *loaded = cbm_result_spill_load(spill, 1);
                ok = loaded && ok;
                if (loaded) {
                    bool owned = ownership_node(&loaded->arena,
                                                loaded->resolved_calls.items[0].evidence.proof, 1);
                    ok = owned && ok;
                    if (owned)
                        ok = strcmp(loaded->resolved_calls.items[0]
                                        .evidence.proof->value.source.content_identity,
                                    "bytes-v1") == 0 &&
                             ok;
                    cbm_free_result(loaded);
                }
            }
        } else {
            ok = false;
            cbm_arena_destroy(&foreign);
        }
    } else {
        cbm_arena_destroy(&foreign);
    }
    if (result)
        cbm_free_result(result);
    cbm_result_spill_close(spill);
    char subdirectory[600];
    snprintf(subdirectory, sizeof(subdirectory), "%s/spill", directory);
    cbm_rmdir(subdirectory);
    cbm_rmdir(directory);
    ASSERT(!parked_borrowed);
    ASSERT(ok);
    PASS();
}

TEST(resolver_proof_cross_copy_failure_clears_borrowed_authority) {
    CBMArena source, destination;
    cbm_arena_init(&source);
    cbm_arena_init(&destination);
    cbm_resolve_proof_t *cycle = cbm_arena_calloc(&source, sizeof(*cycle));
    ASSERT_NOT_NULL(cycle);
    const cbm_resolve_proof_t **premise = cbm_arena_alloc(&source, sizeof(*premise));
    ASSERT_NOT_NULL(premise);
    *premise = cycle;
    cycle->value.kind = CBM_PROOF_TYPE_COPY;
    cycle->value.depth = 2;
    cycle->value.outcome = ownership_outcome();
    cycle->value.premises = premise;
    cycle->value.premise_count = 1;
    cycle->value.predecessors_closed = true;
    CBMResolvedCallArray input = {0}, output = {0};
    cbm_resolvedcall_push(&input, &source, ownership_row(cycle, 10));
    cbm_pxc_append_resolutions_for_test(&destination, &output, &input);
    ASSERT_EQ(output.count, 1);
    bool revoked = output.items[0].evidence.analysis == CBM_RESOLVE_ANALYSIS_FAILED &&
                   output.items[0].evidence.proof == NULL && output.items[0].evidence.facts == 0;
    cbm_arena_destroy(&source);
    cbm_arena_destroy(&destination);
    ASSERT(revoked);
    PASS();
}

/* Synthetic adapter facts isolate the publication boundary: site A has an
 * audited target; site B is deliberately unknown in the same source snapshot. */
static cbm_resolve_record_t ownership_site_record(void) {
    cbm_resolve_result_t result = ownership_outcome();
    result.facts |= CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    return (cbm_resolve_record_t){.file_path = "caller.py",
                                  .scope = "project.caller.main",
                                  .relation = "CALLS",
                                  .spelling = "Widget",
                                  .start_byte = 10,
                                  .end_byte = 16,
                                  .source_origin = CBM_SOURCE_ORIGIN_RAW,
                                  .content_identity = "bytes-v1",
                                  .configuration_identity = "grammar-v1",
                                  .result = result,
                                  .emitted = true};
}

static const cbm_resolve_proof_t *ownership_site_proof(cbm_resolve_proof_pool_t *pool,
                                                       cbm_resolve_record_t row,
                                                       const char *project, const char *role) {
    cbm_resolve_proof_source_t source = {.project = project,
                                         .file = row.file_path,
                                         .module = "project.caller",
                                         .scope = row.scope,
                                         .content_identity = row.content_identity,
                                         .configuration_identity = row.configuration_identity,
                                         .source_origin = row.source_origin};
    cbm_resolve_proof_key_t key = {.lookup_domain = row.scope,
                                   .lookup_key = row.spelling,
                                   .binding_identity = row.result.target,
                                   .start_byte = row.start_byte,
                                   .end_byte = row.end_byte,
                                   .origin = row.source_origin,
                                   .detail = role};
    cbm_resolve_result_t attested = row.result;
    attested.proof = NULL;
    return cbm_resolve_proof_leaf(pool, CBM_PROOF_ADAPTER_DOMAIN, &source, &key, &attested);
}

static int ownership_site_publish(cbm_resolve_record_t row) {
    cbm_gbuf_t *graph = cbm_gbuf_new("project", "/tmp");
    if (!graph)
        return -1;
    bool ok = cbm_gbuf_record_resolution(graph, &row) && cbm_gbuf_resolutions_own_shared(graph);
    size_t count = 0;
    const cbm_resolve_record_t *sealed = cbm_gbuf_resolutions(graph, &count);
    int admitted =
        ok && count == 1 ? sealed[0].emitted && cbm_resolve_record_admit(&sealed[0]) : -1;
    cbm_gbuf_free(graph);
    return admitted;
}

TEST(resolver_proof_sealing_rejects_reused_occurrence_authority) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    cbm_resolve_record_t known = ownership_site_record();
    known.result.proof = ownership_site_proof(&pool, known, "project", "CALLS");
    ASSERT_NOT_NULL(known.result.proof);
    ASSERT_EQ(ownership_site_publish(known), 1);
    cbm_resolve_record_t unknown = known;
    unknown.start_byte = 40;
    unknown.end_byte = 46;
    ASSERT_EQ(ownership_site_publish(unknown), 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_proof_sealing_rejects_source_role_and_fact_contradictions) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    cbm_resolve_record_t known = ownership_site_record();
    for (int mutation = 0; mutation < 12; mutation++) {
        cbm_resolve_record_t producer = known, carrier = known;
        const char *project = "project", *role = "CALLS";
        switch (mutation) {
        case 0:
            producer.file_path = "other.py";
            break;
        case 1:
            producer.content_identity = "other-bytes";
            break;
        case 2:
            producer.configuration_identity = "other-config";
            break;
        case 3:
            producer.scope = "project.caller.other";
            break;
        case 4:
            producer.source_origin = CBM_SOURCE_ORIGIN_PREPROCESSED;
            break;
        case 5:
            project = "other-project";
            break;
        case 6:
            role = "READS";
            break;
        case 7:
            producer.spelling = "Other";
            break;
        case 8:
            carrier.result.facts |= CBM_RESOLVE_FACT_CLASS_CALLABLE;
            break;
        case 9:
            producer.result.facts &= ~CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
            break;
        case 10:
            producer.result.rule = CBM_RESOLVE_RULE_TYPE_MEMBER;
            break;
        case 11:
            producer.start_byte = producer.end_byte = 0;
            break;
        }
        carrier.result.proof = ownership_site_proof(&pool, producer, project, role);
        ASSERT_NOT_NULL(carrier.result.proof);
        ASSERT_EQ(ownership_site_publish(carrier), 0);
    }
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_proof_sealing_shared_export_requires_exact_local_binding) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    cbm_resolve_record_t occurrence = ownership_site_record();
    occurrence.result.rule = CBM_RESOLVE_RULE_IMPORT_BINDING;
    occurrence.result.facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                              CBM_RESOLVE_FACT_IMPORT_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    cbm_resolve_result_t exported = occurrence.result;
    exported.facts &= ~CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    cbm_resolve_proof_source_t module = {.project = "project",
                                         .file = "widget.py",
                                         .module = "project.widget",
                                         .scope = "project.widget",
                                         .content_identity = "widget-bytes",
                                         .configuration_identity = "grammar-v1"};
    cbm_resolve_proof_key_t key = {.lookup_key = "Widget",
                                   .declaration_identity = "project.Widget",
                                   .detail = "exported declaration"};
    occurrence.result.proof =
        cbm_resolve_proof_leaf(&pool, CBM_PROOF_REGISTERED_DECLARATION, &module, &key, &exported);
    ASSERT_NOT_NULL(occurrence.result.proof);
    ASSERT_EQ(ownership_site_publish(occurrence), 0);
    occurrence.binding_proof = ownership_site_proof(&pool, occurrence, "project", "CALLS");
    ASSERT_NOT_NULL(occurrence.binding_proof);
    ASSERT_EQ(ownership_site_publish(occurrence), 1);
    cbm_resolve_record_t unrelated = occurrence;
    unrelated.result.target = "project.Other";
    occurrence.binding_proof = ownership_site_proof(&pool, unrelated, "project", "CALLS");
    ASSERT_EQ(ownership_site_publish(occurrence), 0);
    cbm_arena_destroy(&arena);
    PASS();
}

SUITE(resolver_proof_ownership) {
    RUN_TEST(resolver_proof_sealing_rejects_reused_occurrence_authority);
    RUN_TEST(resolver_proof_sealing_rejects_source_role_and_fact_contradictions);
    RUN_TEST(resolver_proof_sealing_shared_export_requires_exact_local_binding);
    RUN_TEST(resolver_proof_cross_copy_failure_clears_borrowed_authority);
    RUN_TEST(resolver_proof_compaction_owns_and_shares_every_support);
    RUN_TEST(resolver_proof_relocation_moves_nodes_arrays_and_shared_roots);
    RUN_TEST(resolver_proof_cross_append_and_supersede_take_ownership);
    RUN_TEST(resolver_proof_spill_rejects_borrowed_dag_and_round_trips_owned_dag);
}
