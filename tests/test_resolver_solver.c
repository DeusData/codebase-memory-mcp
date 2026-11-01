#include "test_framework.h"
#include "resolve/solver.h"

#include <stdio.h>
#include <string.h>

typedef struct {
    CBMArena arena;
    cbm_resolve_proof_pool_t proofs;
    cbm_resolve_solver_t *solver;
} solver_fixture_t;

static bool solver_fixture_init(solver_fixture_t *fixture, cbm_resolve_solver_limits_t limits) {
    memset(fixture, 0, sizeof(*fixture));
    cbm_arena_init(&fixture->arena);
    cbm_resolve_proof_pool_init(&fixture->proofs, &fixture->arena, 0, 0, 0);
    fixture->solver = cbm_resolve_solver_new(&fixture->arena, &fixture->proofs, limits);
    return fixture->solver != NULL;
}

static const cbm_resolve_proof_t *solver_audit(solver_fixture_t *fixture, const char *key) {
    cbm_resolve_proof_source_t source = {
        .project = "solver",
        .file = "input.py",
        .module = "solver.input",
        .scope = "solver.input",
        .content_identity = "f0c0a6775f31f5fb00a86b00c4d4581ef5600a551b9879fa7fd87014fbebaf26"};
    cbm_resolve_proof_key_t lookup = {.lookup_domain = "solver.input",
                                      .lookup_key = key,
                                      .binding_identity = key,
                                      .detail = "source_audit"};
    cbm_resolve_result_t outcome = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE};
    return cbm_resolve_proof_leaf(&fixture->proofs, CBM_PROOF_ADAPTER_DOMAIN, &source, &lookup,
                                  &outcome);
}

static bool solver_leaf(solver_fixture_t *fixture, const char *key, const char *target,
                        cbm_resolve_solver_status_t status) {
    return cbm_resolve_solver_leaf(fixture->solver, key, status,
                                   (cbm_resolve_solver_value_t){.target = target, .kind = 1},
                                   solver_audit(fixture, key));
}

static bool solver_copy(solver_fixture_t *fixture, const char *key, const char *dependency) {
    return cbm_resolve_solver_copy(fixture->solver, key, dependency, solver_audit(fixture, key));
}

TEST(resolver_solver_pending_is_not_unknown_and_copy_settles) {
    solver_fixture_t fixture;
    ASSERT(solver_fixture_init(&fixture, (cbm_resolve_solver_limits_t){0}));
    ASSERT(solver_copy(&fixture, "alias", "definition"));
    ASSERT(solver_leaf(&fixture, "definition", "project.actual.target", CBM_SOLVER_KNOWN));
    cbm_resolve_solver_result_t before, after, absent;
    cbm_resolve_solver_get(fixture.solver, "alias", &before);
    ASSERT_EQ(before.status, CBM_SOLVER_PENDING);
    ASSERT_NULL(before.value.target);
    ASSERT(cbm_resolve_solver_solve(fixture.solver));
    cbm_resolve_solver_get(fixture.solver, "alias", &after);
    cbm_resolve_solver_get(fixture.solver, "absent", &absent);
    ASSERT_EQ(after.status, CBM_SOLVER_KNOWN);
    ASSERT_STR_EQ(after.value.target, "project.actual.target");
    ASSERT(after.predecessors_closed);
    ASSERT_EQ(after.dependency_count, 1);
    ASSERT_STR_EQ(after.dependencies[0], "definition");
    ASSERT_EQ(absent.status, CBM_SOLVER_UNKNOWN);
    const cbm_resolve_proof_view_t *proof = cbm_resolve_proof_view(after.proof);
    ASSERT_NOT_NULL(proof);
    ASSERT_EQ(proof->kind, CBM_PROOF_NAMED_COPY);
    ASSERT_EQ(proof->premise_count, 2);
    cbm_arena_destroy(&fixture.arena);
    PASS();
}

TEST(resolver_solver_join_preserves_unknown_and_unsupported_competitors) {
    const cbm_resolve_solver_status_t states[] = {CBM_SOLVER_UNKNOWN, CBM_SOLVER_UNSUPPORTED,
                                                  CBM_SOLVER_LIMITED, CBM_SOLVER_FAILED};
    for (size_t i = 0; i < sizeof(states) / sizeof(states[0]); i++) {
        solver_fixture_t fixture;
        ASSERT(solver_fixture_init(&fixture, (cbm_resolve_solver_limits_t){0}));
        ASSERT(solver_leaf(&fixture, "known", "project.Type", CBM_SOLVER_KNOWN));
        ASSERT(solver_leaf(&fixture, "competitor", NULL, states[i]));
        const char *dependencies[] = {"known", "competitor"};
        ASSERT(cbm_resolve_solver_join(fixture.solver, "joined", dependencies, 2,
                                       solver_audit(&fixture, "joined")));
        ASSERT(cbm_resolve_solver_solve(fixture.solver));
        cbm_resolve_solver_result_t joined;
        cbm_resolve_solver_get(fixture.solver, "joined", &joined);
        ASSERT_EQ(joined.status, states[i]);
        ASSERT_NULL(joined.value.target);
        ASSERT(joined.predecessors_closed);
        ASSERT_EQ(joined.dependency_count, 2);
        ASSERT_EQ(cbm_resolve_proof_view(joined.proof)->premise_count, 3);
        cbm_arena_destroy(&fixture.arena);
    }
    PASS();
}

TEST(resolver_solver_join_compares_target_kind_and_module_identity) {
    for (int mismatch = 0; mismatch < 3; mismatch++) {
        solver_fixture_t fixture;
        ASSERT(solver_fixture_init(&fixture, (cbm_resolve_solver_limits_t){0}));
        ASSERT(solver_leaf(&fixture, "left", "project.Type", CBM_SOLVER_KNOWN));
        cbm_resolve_solver_value_t right = {.target =
                                                mismatch == 0 ? "project.Other" : "project.Type",
                                            .kind = mismatch == 1 ? 2 : 1,
                                            .module = mismatch == 2};
        ASSERT(cbm_resolve_solver_leaf(fixture.solver, "right", CBM_SOLVER_KNOWN, right,
                                       solver_audit(&fixture, "right")));
        const char *dependencies[] = {"left", "right"};
        ASSERT(cbm_resolve_solver_join(fixture.solver, "joined", dependencies, 2,
                                       solver_audit(&fixture, "joined")));
        ASSERT(cbm_resolve_solver_solve(fixture.solver));
        cbm_resolve_solver_result_t joined;
        cbm_resolve_solver_get(fixture.solver, "joined", &joined);
        ASSERT_EQ(joined.status, CBM_SOLVER_AMBIGUOUS);
        ASSERT_NULL(joined.value.target);
        cbm_arena_destroy(&fixture.arena);
    }
    PASS();
}

typedef struct {
    bool left, right, joined;
    size_t nodes;
} solver_supports_t;
static bool collect_solver_support(const cbm_resolve_proof_t *proof, void *context) {
    solver_supports_t *supports = context;
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(proof);
    supports->nodes++;
    if (view->kind == CBM_PROOF_ADAPTER_DOMAIN) {
        supports->left |= !strcmp(view->key.lookup_key, "left");
        supports->right |= !strcmp(view->key.lookup_key, "right");
        supports->joined |= !strcmp(view->key.lookup_key, "joined");
    }
    return true;
}

TEST(resolver_solver_equal_join_retains_every_support) {
    solver_fixture_t fixture;
    ASSERT(solver_fixture_init(&fixture, (cbm_resolve_solver_limits_t){0}));
    ASSERT(solver_leaf(&fixture, "left", "project.Type", CBM_SOLVER_KNOWN));
    ASSERT(solver_leaf(&fixture, "right", "project.Type", CBM_SOLVER_KNOWN));
    const char *dependencies[] = {"right", "left", "right"};
    ASSERT(cbm_resolve_solver_join(fixture.solver, "joined", dependencies, 3,
                                   solver_audit(&fixture, "joined")));
    ASSERT(cbm_resolve_solver_solve(fixture.solver));
    cbm_resolve_solver_result_t joined;
    cbm_resolve_solver_get(fixture.solver, "joined", &joined);
    ASSERT_EQ(joined.status, CBM_SOLVER_KNOWN);
    ASSERT_EQ(joined.dependency_count, 2);
    solver_supports_t supports = {0};
    ASSERT(cbm_resolve_proof_visit(joined.proof, 16, collect_solver_support, &supports));
    ASSERT(supports.left);
    ASSERT(supports.right);
    ASSERT(supports.joined);
    ASSERT_EQ(supports.nodes, 4);
    cbm_arena_destroy(&fixture.arena);
    PASS();
}

TEST(resolver_solver_missing_dependencies_and_cycles_never_borrow_known_arm) {
    solver_fixture_t fixture;
    ASSERT(solver_fixture_init(&fixture, (cbm_resolve_solver_limits_t){0}));
    ASSERT(solver_leaf(&fixture, "seed", "project.Type", CBM_SOLVER_KNOWN));
    ASSERT(solver_copy(&fixture, "a", "b"));
    const char *dependencies[] = {"a", "seed"};
    ASSERT(
        cbm_resolve_solver_join(fixture.solver, "b", dependencies, 2, solver_audit(&fixture, "b")));
    ASSERT(solver_copy(&fixture, "missing", "not_registered"));
    ASSERT(cbm_resolve_solver_solve(fixture.solver));
    cbm_resolve_solver_result_t a, b, missing;
    cbm_resolve_solver_get(fixture.solver, "a", &a);
    cbm_resolve_solver_get(fixture.solver, "b", &b);
    cbm_resolve_solver_get(fixture.solver, "missing", &missing);
    ASSERT_EQ(a.status, CBM_SOLVER_UNKNOWN);
    ASSERT_EQ(b.status, CBM_SOLVER_UNKNOWN);
    ASSERT_NULL(a.value.target);
    ASSERT_NULL(b.value.target);
    ASSERT(!a.predecessors_closed);
    ASSERT(!b.predecessors_closed);
    ASSERT_EQ(b.dependency_count, 2);
    ASSERT_EQ(missing.status, CBM_SOLVER_UNKNOWN);
    ASSERT(missing.predecessors_closed);
    ASSERT_EQ(cbm_resolve_proof_view(missing.proof)->premise_count, 2);
    cbm_arena_destroy(&fixture.arena);
    PASS();
}

static bool register_solver_chain(solver_fixture_t *fixture, int count, bool reverse) {
    for (int at = 0; at < count; at++) {
        int i = reverse ? count - at - 1 : at;
        char key[32], dependency[32];
        snprintf(key, sizeof(key), "name_%03d", i);
        snprintf(dependency, sizeof(dependency), "name_%03d", i + 1);
        if (!(i == count - 1 ? solver_leaf(fixture, key, "project.actual", CBM_SOLVER_KNOWN)
                             : solver_copy(fixture, key, dependency)))
            return false;
    }
    return true;
}

TEST(resolver_solver_registration_permutations_have_same_summary_and_linear_work) {
    solver_fixture_t forward, reverse;
    ASSERT(solver_fixture_init(&forward, (cbm_resolve_solver_limits_t){0}));
    ASSERT(solver_fixture_init(&reverse, (cbm_resolve_solver_limits_t){0}));
    ASSERT(register_solver_chain(&forward, 64, false));
    ASSERT(register_solver_chain(&reverse, 64, true));
    ASSERT(cbm_resolve_solver_solve(forward.solver));
    ASSERT(cbm_resolve_solver_solve(reverse.solver));
    ASSERT_EQ(cbm_resolve_solver_steps(forward.solver), 64 + 3 * 63);
    ASSERT_EQ(cbm_resolve_solver_steps(reverse.solver), 64 + 3 * 63);
    ASSERT_EQ(cbm_resolve_solver_count(forward.solver), 64);
    for (size_t i = 0; i < 64; i++) {
        const cbm_resolve_solver_result_t *left = cbm_resolve_solver_at(forward.solver, i);
        const cbm_resolve_solver_result_t *right = cbm_resolve_solver_at(reverse.solver, i);
        ASSERT_STR_EQ(left->key, right->key);
        ASSERT_EQ(left->status, CBM_SOLVER_KNOWN);
        ASSERT_EQ(right->status, CBM_SOLVER_KNOWN);
        ASSERT_STR_EQ(left->value.target, right->value.target);
        ASSERT_EQ(cbm_resolve_proof_compare(left->proof, right->proof), 0);
    }
    cbm_arena_destroy(&forward.arena);
    cbm_arena_destroy(&reverse.arena);
    PASS();
}

TEST(resolver_solver_conflicting_registrations_fail_closed_in_both_orders) {
    for (int reverse = 0; reverse < 2; reverse++) {
        solver_fixture_t fixture;
        ASSERT(solver_fixture_init(&fixture, (cbm_resolve_solver_limits_t){0}));
        ASSERT(solver_leaf(&fixture, "duplicate", reverse ? "B" : "A", CBM_SOLVER_KNOWN));
        ASSERT(solver_leaf(&fixture, "duplicate", reverse ? "A" : "B", CBM_SOLVER_KNOWN));
        ASSERT(solver_leaf(&fixture, "other", "C", CBM_SOLVER_KNOWN));
        ASSERT(!cbm_resolve_solver_solve(fixture.solver));
        cbm_resolve_solver_result_t duplicate, other;
        cbm_resolve_solver_get(fixture.solver, "duplicate", &duplicate);
        cbm_resolve_solver_get(fixture.solver, "other", &other);
        ASSERT_EQ(duplicate.status, CBM_SOLVER_FAILED);
        ASSERT_EQ(other.status, CBM_SOLVER_FAILED);
        ASSERT_NULL(duplicate.value.target);
        ASSERT_NULL(duplicate.proof);
        ASSERT(!duplicate.predecessors_closed);
        cbm_arena_destroy(&fixture.arena);
    }
    PASS();
}

TEST(resolver_solver_limits_abstain_globally_independent_of_registration_order) {
    const cbm_resolve_solver_limits_t limits[] = {{.nodes = 2}, {.edges = 1}, {.steps = 8}};
    for (size_t limit = 0; limit < sizeof(limits) / sizeof(limits[0]); limit++) {
        for (int reverse = 0; reverse < 2; reverse++) {
            solver_fixture_t fixture;
            ASSERT(solver_fixture_init(&fixture, limits[limit]));
            ASSERT(register_solver_chain(&fixture, 3, reverse != 0));
            ASSERT(cbm_resolve_solver_solve(fixture.solver));
            ASSERT(!cbm_resolve_solver_complete(fixture.solver));
            ASSERT_EQ(cbm_resolve_solver_count(fixture.solver), 0);
            ASSERT_NULL(cbm_resolve_solver_at(fixture.solver, 0));
            for (int i = 0; i < 3; i++) {
                char key[32];
                snprintf(key, sizeof(key), "name_%03d", i);
                cbm_resolve_solver_result_t result;
                cbm_resolve_solver_get(fixture.solver, key, &result);
                ASSERT_EQ(result.status, CBM_SOLVER_LIMITED);
                ASSERT_NULL(result.value.target);
                ASSERT_NULL(result.proof);
                ASSERT(!result.predecessors_closed);
            }
            cbm_arena_destroy(&fixture.arena);
        }
    }
    PASS();
}

/* One solver holds every fact of a project, and exhausting it voids every occurrence in
 * the generation. kubernetes' staging tree alone registers 1315391 facts and settles them
 * in 4.4M steps, so the former 1M node cap turned all 6.4M occurrences of the repository
 * into source_declaration_identity_conflict and published no CALLS at all. */
TEST(resolver_solver_zero_limits_do_not_cap_a_project_sized_population) {
    enum { FACTS = 1000001 };
    solver_fixture_t fixture;
    ASSERT(solver_fixture_init(&fixture, (cbm_resolve_solver_limits_t){0}));
    const cbm_resolve_proof_t *audit = solver_audit(&fixture, "project_audit");
    ASSERT_NOT_NULL(audit);
    for (int i = 0; i < FACTS; i++) {
        char key[32];
        snprintf(key, sizeof(key), "fact_%07d", i);
        ASSERT(cbm_resolve_solver_leaf(fixture.solver, key, CBM_SOLVER_KNOWN,
                                       (cbm_resolve_solver_value_t){.target = "project.target",
                                                                    .kind = 1},
                                       audit));
    }
    ASSERT(cbm_resolve_solver_solve(fixture.solver));
    ASSERT(cbm_resolve_solver_complete(fixture.solver));
    ASSERT_EQ(cbm_resolve_solver_count(fixture.solver), FACTS);
    cbm_resolve_solver_result_t last;
    cbm_resolve_solver_get(fixture.solver, "fact_1000000", &last);
    ASSERT_EQ(last.status, CBM_SOLVER_KNOWN);
    ASSERT_STR_EQ(last.value.target, "project.target");
    cbm_arena_destroy(&fixture.arena);
    PASS();
}

SUITE(resolver_solver) {
    RUN_TEST(resolver_solver_zero_limits_do_not_cap_a_project_sized_population);
    RUN_TEST(resolver_solver_pending_is_not_unknown_and_copy_settles);
    RUN_TEST(resolver_solver_join_preserves_unknown_and_unsupported_competitors);
    RUN_TEST(resolver_solver_join_compares_target_kind_and_module_identity);
    RUN_TEST(resolver_solver_equal_join_retains_every_support);
    RUN_TEST(resolver_solver_missing_dependencies_and_cycles_never_borrow_known_arm);
    RUN_TEST(resolver_solver_registration_permutations_have_same_summary_and_linear_work);
    RUN_TEST(resolver_solver_conflicting_registrations_fail_closed_in_both_orders);
    RUN_TEST(resolver_solver_limits_abstain_globally_independent_of_registration_order);
}
