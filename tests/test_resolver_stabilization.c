#include "test_framework.h"
#include "test_helpers.h"
#include "lsp/go_stabilize.h"
#include "pipeline/pipeline.h"
#include "pipeline/import_resolve.h"
#include "resolve/policy.h"
#include "store/store.h"
#include "sqlite3.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    CBMArena arena;
    cbm_resolve_proof_pool_t proofs;
    cbm_resolve_solver_t *solver;
} stabilization_t;
static void initialize(stabilization_t *f, cbm_resolve_solver_limits_t limits) {
    memset(f, 0, sizeof(*f));
    cbm_arena_init(&f->arena);
    cbm_resolve_proof_pool_init(&f->proofs, &f->arena, 0, 0, 0);
    f->solver = cbm_resolve_solver_new(&f->arena, &f->proofs, limits);
}
static const cbm_resolve_proof_t *witness(stabilization_t *f, const char *key) {
    cbm_resolve_proof_source_t source = {
        .project = "test", .file = "input.go", .scope = "test", .content_identity = "test-content"};
    cbm_resolve_proof_key_t fact = {.lookup_key = key, .detail = "source_test_fact"};
    cbm_resolve_result_t result = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE};
    return cbm_resolve_proof_leaf(&f->proofs, CBM_PROOF_SOURCE_FACT, &source, &fact, &result);
}
static bool leaf(stabilization_t *f, const char *key, const char *target,
                 cbm_resolve_solver_status_t status, const CBMType *type) {
    return cbm_resolve_solver_leaf(
        f->solver, key, status,
        (cbm_resolve_solver_value_t){.target = target,
                                     .type = type,
                                     .fact_kind =
                                         type ? CBM_SOLVER_FACT_TYPE : CBM_SOLVER_FACT_BINDING,
                                     .declaration_identity = key},
        witness(f, key));
}
TEST(resolver_stabilization_selected_pending_type_drives_binding_and_return) {
    for (int reverse = 0; reverse < 2; reverse++) {
        stabilization_t f;
        initialize(&f, (cbm_resolve_solver_limits_t){0});
        ASSERT_NOT_NULL(f.solver);
        if (!reverse)
            ASSERT(leaf(&f, "Factory", "pkg.Factory", CBM_SOLVER_KNOWN,
                        cbm_type_named(&f.arena, "pkg.Factory")));
        ASSERT(cbm_resolve_solver_select(f.solver, "new", "factory-result", "member:", ".New",
                                         witness(&f, "new")));
        ASSERT(cbm_resolve_solver_select(f.solver, "new-result", "new", "return:", "",
                                         witness(&f, "new-result")));
        ASSERT(cbm_resolve_solver_type(f.solver, "copy-v2", "new-result", CBM_TYPE_NAMED, 2,
                                       witness(&f, "copy-v2")));
        ASSERT(cbm_resolve_solver_select(f.solver, "send", "copy-v2", "member:", ".Send",
                                         witness(&f, "send")));
        ASSERT(cbm_resolve_solver_copy(f.solver, "return:pkg.Factory.New", "alias",
                                       witness(&f, "return")));
        ASSERT(cbm_resolve_solver_copy(f.solver, "alias", "Client", witness(&f, "alias")));
        ASSERT(cbm_resolve_solver_type(f.solver, "factory-result", "Factory", CBM_TYPE_POINTER, 1,
                                       witness(&f, "factory-result")));
        ASSERT(leaf(&f, "member:pkg.Factory.New", "pkg.Factory.New", CBM_SOLVER_KNOWN, NULL));
        ASSERT(leaf(&f, "member:pkg.Client.Send", "pkg.Client.Send", CBM_SOLVER_KNOWN, NULL));
        ASSERT(leaf(&f, "Client", "pkg.Client", CBM_SOLVER_KNOWN,
                    cbm_type_named(&f.arena, "pkg.Client")));
        if (reverse)
            ASSERT(leaf(&f, "Factory", "pkg.Factory", CBM_SOLVER_KNOWN,
                        cbm_type_named(&f.arena, "pkg.Factory")));
        ASSERT(cbm_resolve_solver_solve(f.solver));
        cbm_resolve_solver_result_t result;
        cbm_resolve_solver_get(f.solver, "send", &result);
        ASSERT_EQ(result.status, CBM_SOLVER_KNOWN);
        ASSERT_STR_EQ(result.value.target, "pkg.Client.Send");
        ASSERT_EQ(result.dependency_count, 2);
        ASSERT_STR_EQ(result.dependencies[1], "member:pkg.Client.Send");
        cbm_resolve_solver_get(f.solver, "copy-v2", &result);
        ASSERT_EQ(result.value.binding_version, 2);
        ASSERT_NOT_NULL(result.value.type_identity);
        ASSERT_EQ(result.value.type->kind, CBM_TYPE_NAMED);
        cbm_resolve_solver_get(f.solver, "factory-result", &result);
        ASSERT_EQ(result.value.type->kind, CBM_TYPE_POINTER);
        ASSERT_STR_EQ(result.value.type->data.pointer.elem->data.named.qualified_name,
                      "pkg.Factory");
        cbm_arena_destroy(&f.arena);
    }
    PASS();
}
TEST(resolver_stabilization_unknown_competitor_cycle_and_budget_abstain) {
    for (int mode = 0; mode < 3; mode++) {
        stabilization_t f;
        initialize(&f, mode == 2 ? (cbm_resolve_solver_limits_t){.edges = 2}
                                 : (cbm_resolve_solver_limits_t){0});
        ASSERT(leaf(&f, "known", "pkg.Client", CBM_SOLVER_KNOWN,
                    cbm_type_named(&f.arena, "pkg.Client")));
        if (mode == 1)
            ASSERT(cbm_resolve_solver_copy(f.solver, "other", "joined", witness(&f, "other")));
        else
            ASSERT(leaf(&f, "other", NULL, CBM_SOLVER_UNKNOWN, NULL));
        const char *arms[] = {"known", "other"};
        ASSERT(cbm_resolve_solver_join(f.solver, "joined", arms, 2, witness(&f, "joined")));
        ASSERT(cbm_resolve_solver_select(f.solver, "call", "joined", "member:", ".Send",
                                         witness(&f, "call")));
        ASSERT(leaf(&f, "member:pkg.Client.Send", "pkg.Client.Send", CBM_SOLVER_KNOWN, NULL));
        ASSERT(cbm_resolve_solver_solve(f.solver));
        cbm_resolve_solver_result_t result;
        cbm_resolve_solver_get(f.solver, "call", &result);
        ASSERT_EQ(result.status, mode == 2 ? CBM_SOLVER_LIMITED : CBM_SOLVER_UNKNOWN);
        ASSERT_NULL(result.value.target);
        cbm_arena_destroy(&f.arena);
    }
    PASS();
}

static const char API[] =
    "package api\n"
    "type Client struct{}\nfunc (c *Client) Send() {}\n"
    "type Other struct{}\nfunc (o *Other) Send() {}\n"
    "type Product = Client\n"
    "type Factory struct{}\nfunc (f *Factory) New() *Product { return nil }\n";
static const char BRIDGE[] =
    "package bridge\nimport \"example/api\"\nfunc Make() *api.Factory { return nil }\n";
static const char APP[] = "package app\nimport \"example/bridge\"\n"
                          "func Run() { f := bridge.Make(); p := f.New(); q := p; q.Send() }\n";

typedef struct {
    bool alias, version, return_rule, select;
} supports_t;
static bool collect_support(const cbm_resolve_proof_t *proof, void *context) {
    supports_t *s = context;
    const cbm_resolve_proof_view_t *v = cbm_resolve_proof_view(proof);
    const char *key = v->key.lookup_key ? v->key.lookup_key : "";
    const char *detail = v->key.detail ? v->key.detail : "";
    s->alias |= !strcmp(key, "go/type:test.api.Product");
    s->version |= !strcmp(detail, "immutable_lexical_binding_version");
    s->return_rule |= !strcmp(detail, "resolved_callable_return");
    s->select |= strstr(key, "go/addressable-member:") && strstr(key, "test.api.Client") &&
                 strstr(key, ".Send");
    return true;
}
static bool collect_fixture(cbm_go_stabilization_t *state, const char *file, const char *module,
                            const char *source, const char *import, const char *import_qn) {
    CBMFileResult *result =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test", file, 0, NULL, NULL);
    if (!result)
        return false;
    cbm_resolve_proof_source_t context = {.project = "test",
                                          .file = file,
                                          .module = module,
                                          .scope = module,
                                          .content_identity = result->source_content_id[0],
                                          .configuration_identity =
                                              result->source_configuration_id};
    CBMArena imported_arena;
    cbm_arena_init(&imported_arena);
    cbm_resolve_proof_pool_t imported_proofs;
    cbm_resolve_proof_pool_init(&imported_proofs, &imported_arena, 0, 0, 0);
    cbm_resolve_import_fact_t fact = {0};
    if (import && result->imports.count == 1) {
        const CBMImport *raw = &result->imports.items[0];
        fact = (cbm_resolve_import_fact_t){.local_name = import,
                                           .module_path = raw->module_path,
                                           .start_byte = raw->site_start_byte,
                                           .end_byte = raw->site_end_byte,
                                           .closed = import_qn && *import_qn};
        if (fact.closed) {
            fact.dependency = (cbm_resolve_result_t){
                .binding = CBM_RESOLVE_BINDING_UNIQUE,
                .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                .rule = CBM_RESOLVE_RULE_MODULE_PATH,
                .target = import_qn,
                .candidate_count = 1,
                .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                         CBM_RESOLVE_FACT_PATH_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT};
            cbm_resolve_proof_key_t path = {.lookup_key = raw->module_path,
                                            .binding_identity = import_qn,
                                            .start_byte = raw->site_start_byte,
                                            .end_byte = raw->site_end_byte,
                                            .lookup_closed = true,
                                            .detail = "IMPORTS"};
            fact.dependency.proof = cbm_resolve_proof_leaf(&imported_proofs, CBM_PROOF_SOURCE_FACT,
                                                           &context, &path, &fact.dependency);
        }
    }
    cbm_resolve_import_snapshot_t imports = {
        .source = context, .items = &fact, .count = import ? 1 : 0, .complete = true};
    bool ok = cbm_go_stabilization_collect(state, result, source, strlen(source), &context, module,
                                           &imports);
    cbm_arena_destroy(&imported_arena);
    cbm_free_result(result); /* Every normalization string/type/proof must now be owned. */
    return ok;
}
/* Locate the actual RAW carrier, including extractor normalization and the
 * full invocation span. Nested calls may share a start; the requested outer
 * occurrence has the largest end at that start in this scope. */
static const CBMCall *fixture_call(const CBMFileResult *parsed, const char *source,
                                   const char *scope, const char *anchor) {
    const char *at = strstr(source, anchor);
    if (!parsed || !at)
        return NULL;
    uint32_t start = (uint32_t)(at - source);
    const CBMCall *found = NULL;
    bool duplicate = false;
    for (int i = 0; i < parsed->calls.count; i++) {
        const CBMCall *call = &parsed->calls.items[i];
        if (call->source_origin != CBM_SOURCE_ORIGIN_RAW || !call->enclosing_func_qn ||
            strcmp(call->enclosing_func_qn, scope) || call->site_start_byte != start)
            continue;
        if (!found || call->site_end_byte > found->site_end_byte) {
            found = call;
            duplicate = false;
        } else if (call->site_end_byte == found->site_end_byte)
            duplicate = true;
    }
    return duplicate ? NULL : found;
}
static bool fixture_resolution(cbm_go_stabilization_t *state, const char *file, const char *source,
                               const char *scope, const char *anchor, cbm_resolve_result_t *out) {
    CBMFileResult *parsed =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test", file, 0, NULL, NULL);
    const CBMCall *call = fixture_call(parsed, source, scope, anchor);
    bool controlled =
        call && cbm_go_stabilization_call(state, file, call->enclosing_func_qn,
                                          call->site_start_byte, call->site_end_byte, out);
    if (controlled && out->proof) {
        const cbm_resolve_proof_view_t *proof = cbm_resolve_proof_view(out->proof);
        controlled = proof && proof->key.lookup_key && call->callee_name &&
                     !strcmp(proof->key.lookup_key, call->callee_name) &&
                     proof->key.start_byte == call->site_start_byte &&
                     proof->key.end_byte == call->site_end_byte;
    }
    cbm_free_result(parsed);
    return controlled;
}
TEST(resolver_stabilization_go_raw_call_identity_and_chained_spelling) {
    const char *source = "package api\ntype T struct{}\nfunc(T) M(){}\n"
                         "func Make() T { return T{} }\nfunc Run(){Make().M()}\n";
    const char *file = "api/types.go";
    CBMFileResult *parsed =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test", file, 0, NULL, NULL);
    ASSERT_NOT_NULL(parsed);
    const CBMCall *call = fixture_call(parsed, source, "test.api.Run", "Make().M()");
    ASSERT_NOT_NULL(call);
    ASSERT_STR_EQ(call->callee_name, "Make.M");
    uint32_t start = call->site_start_byte, end = call->site_end_byte;
    ASSERT_EQ(end - start, strlen("Make().M()"));
    stabilization_t f;
    initialize(&f, (cbm_resolve_solver_limits_t){0});
    cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
    ASSERT_NOT_NULL(state);
    cbm_resolve_proof_source_t context = {
        .project = "test",
        .file = file,
        .module = "test.api",
        .scope = "test.api",
        .content_identity = parsed->source_content_id[CBM_SOURCE_ORIGIN_RAW],
        .configuration_identity = parsed->source_configuration_id};
    ASSERT(cbm_go_stabilization_collect(state, parsed, source, strlen(source), &context, "test.api",
                                        NULL));
    cbm_free_result(parsed); /* The published spelling and proof must own their storage. */
    ASSERT(cbm_go_stabilization_register(state));
    ASSERT(cbm_resolve_solver_solve(f.solver));
    ASSERT(cbm_go_stabilization_seal(state));
    cbm_resolve_result_t result = {0};
    ASSERT(cbm_go_stabilization_call(state, file, "test.api.Run", start, end, &result));
    ASSERT(cbm_resolve_admit(&result));
    ASSERT_STR_EQ(result.target, "test.api.T.M");
    const cbm_resolve_proof_view_t *proof = cbm_resolve_proof_view(result.proof);
    ASSERT_NOT_NULL(proof);
    ASSERT_STR_EQ(proof->key.lookup_key, "Make.M");
    ASSERT_EQ(proof->key.start_byte, start);
    ASSERT_EQ(proof->key.end_byte, end);
    ASSERT(!cbm_go_stabilization_call(state, file, "test.api.Run", start, end - 2, &result));
    ASSERT(!cbm_go_stabilization_call(state, file, "test.api.Other", start, end, &result));
    cbm_go_stabilization_dispose(state);
    cbm_arena_destroy(&f.arena);
    PASS();
}
TEST(resolver_stabilization_go_missing_duplicate_and_transformed_carriers_abstain) {
    const char *source = "package api\nfunc Target(){}\nfunc Run(){Target()}\n";
    const char *file = "api/types.go";
    for (int mode = 0; mode < 3; mode++) {
        CBMFileResult *parsed =
            cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test", file, 0, NULL, NULL);
        ASSERT_NOT_NULL(parsed);
        const CBMCall *call = fixture_call(parsed, source, "test.api.Run", "Target()}");
        ASSERT_NOT_NULL(call);
        CBMCall copy = *call;
        if (mode == 0)
            parsed->calls.count = 0;
        else if (mode == 1) {
            copy.callee_name = "conflicting.carrier";
            cbm_calls_push(&parsed->calls, &parsed->arena, copy);
        } else
            parsed->calls.items[call - parsed->calls.items].source_origin =
                CBM_SOURCE_ORIGIN_PREPROCESSED;
        stabilization_t f;
        initialize(&f, (cbm_resolve_solver_limits_t){0});
        cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
        ASSERT_NOT_NULL(state);
        cbm_resolve_proof_source_t context = {
            .project = "test",
            .file = file,
            .module = "test.api",
            .scope = "test.api",
            .content_identity = parsed->source_content_id[CBM_SOURCE_ORIGIN_RAW],
            .configuration_identity = parsed->source_configuration_id};
        ASSERT(cbm_go_stabilization_collect(state, parsed, source, strlen(source), &context,
                                            "test.api", NULL));
        cbm_free_result(parsed);
        ASSERT(cbm_go_stabilization_register(state));
        ASSERT(cbm_resolve_solver_solve(f.solver));
        ASSERT(cbm_go_stabilization_seal(state));
        cbm_resolve_result_t result = {0};
        ASSERT(cbm_go_stabilization_call(state, file, "test.api.Run", copy.site_start_byte,
                                         copy.site_end_byte, &result));
        ASSERT(!cbm_resolve_admit(&result));
        ASSERT_NULL(result.target);
        ASSERT_STR_EQ(result.reason, "shared_call_carrier_missing_or_ambiguous");
        cbm_go_stabilization_dispose(state);
        cbm_arena_destroy(&f.arena);
    }
    PASS();
}
TEST(resolver_stabilization_go_call_carriers_require_matching_source_identity) {
    const char *source = "package api\nfunc Target(){}\nfunc Run(){Target()}\n";
    const char *changed = "package api\nfunc Target(){}\nfunc Run(){Target()} // changed\n";
    const char *file = "api/types.go";
    CBMFileResult *parsed =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test", file, 0, NULL, NULL);
    ASSERT_NOT_NULL(parsed);
    for (int mode = 0; mode < 5; mode++) {
        stabilization_t f;
        initialize(&f, (cbm_resolve_solver_limits_t){0});
        cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
        ASSERT_NOT_NULL(state);
        cbm_resolve_proof_source_t context = {
            .project = "test",
            .file = file,
            .module = "test.api",
            .scope = "test.api",
            .content_identity = parsed->source_content_id[CBM_SOURCE_ORIGIN_RAW],
            .configuration_identity = parsed->source_configuration_id};
        if (mode == 1)
            context.file = "other.go";
        else if (mode == 2)
            context.content_identity = "changed-content";
        else if (mode == 3)
            context.configuration_identity = "changed-configuration";
        else if (mode == 4)
            context.source_origin = CBM_SOURCE_ORIGIN_PREPROCESSED;
        const char *bytes = mode == 0 ? changed : source;
        ASSERT(!cbm_go_stabilization_collect(state, parsed, bytes, strlen(bytes), &context,
                                             "test.api", NULL));
        cbm_go_stabilization_dispose(state);
        cbm_arena_destroy(&f.arena);
    }
    cbm_free_result(parsed);
    PASS();
}
TEST(resolver_stabilization_go_three_package_alias_chain_and_registration_order) {
    for (int reverse = 0; reverse < 2; reverse++) {
        stabilization_t f;
        initialize(&f, (cbm_resolve_solver_limits_t){0});
        cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
        ASSERT_NOT_NULL(state);
        if (!reverse)
            ASSERT(collect_fixture(state, "api/types.go", "test.api", API, NULL, NULL));
        ASSERT(collect_fixture(state, "app/main.go", "test.app", APP, "bridge", "test.bridge"));
        ASSERT(collect_fixture(state, "bridge/make.go", "test.bridge", BRIDGE, "api", "test.api"));
        if (reverse)
            ASSERT(collect_fixture(state, "api/types.go", "test.api", API, NULL, NULL));
        ASSERT(cbm_go_stabilization_register(state));
        ASSERT(cbm_resolve_solver_solve(f.solver));
        ASSERT(cbm_go_stabilization_seal(state));
        cbm_resolve_result_t result;
        ASSERT(fixture_resolution(state, "app/main.go", APP, "test.app.Run", "q.Send()", &result));
        ASSERT(cbm_resolve_admit(&result));
        ASSERT_STR_EQ(result.target, "test.api.Client.Send");
        supports_t supports = {0};
        ASSERT(cbm_resolve_proof_visit(result.proof, 1024, collect_support, &supports));
        ASSERT(supports.alias && supports.version && supports.return_rule && supports.select);
        cbm_go_stabilization_dispose(state);
        cbm_arena_destroy(&f.arena);
    }
    PASS();
}

/* q's static type is fixed by q := p. `q = nil` compiles only because that type admits nil,
 * and no branch can change it, so these select Client.Send. This test used to pin abstention
 * for the first and third bodies, treating a Go local as flow-sensitive. Only a value the
 * declaration itself cannot type stays unknown. */
TEST(resolver_stabilization_go_static_type_survives_flow_and_unknown_stays_unknown) {
    const struct {
        const char *body;
        bool admitted;
    } cases[] = {
        {"func Run() { f:=bridge.Make(); p:=f.New(); q:=p; if flag { q=nil }; q.Send() }\n", true},
        {"func Run() { f:=bridge.Make(); p:=f.New(); q:=p; { q:=unknown(); _=q }; q.Send() }\n",
         true},
        {"func Run() { f:=bridge.Make(); p:=f.New(); q:=p; q=nil; q.Send() }\n", true},
        {"func Run() { q:=unknown(); q.Send() }\n", false},
        {"func Run() { f:=bridge.Make(); q:=f.New(); { q:=unknown(); q.Send() } }\n", false},
    };
    for (size_t variant = 0; variant < sizeof(cases) / sizeof(cases[0]); variant++) {
        stabilization_t f;
        initialize(&f, (cbm_resolve_solver_limits_t){0});
        cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
        ASSERT_NOT_NULL(state);
        char source[512];
        snprintf(source, sizeof(source), "package app\nimport \"example/bridge\"\n%s",
                 cases[variant].body);
        ASSERT(collect_fixture(state, "api/types.go", "test.api", API, NULL, NULL));
        ASSERT(collect_fixture(state, "bridge/make.go", "test.bridge", BRIDGE, "api", "test.api"));
        ASSERT(collect_fixture(state, "app/main.go", "test.app", source, "bridge", "test.bridge"));
        ASSERT(cbm_go_stabilization_register(state));
        ASSERT(cbm_resolve_solver_solve(f.solver));
        ASSERT(cbm_go_stabilization_seal(state));
        cbm_resolve_result_t result;
        ASSERT(
            fixture_resolution(state, "app/main.go", source, "test.app.Run", "q.Send()", &result));
        ASSERT_EQ(cbm_resolve_admit(&result), cases[variant].admitted);
        if (cases[variant].admitted)
            ASSERT_STR_EQ(result.target, "test.api.Client.Send");
        else
            ASSERT_NULL(result.target);
        cbm_go_stabilization_dispose(state);
        cbm_arena_destroy(&f.arena);
    }
    PASS();
}

static bool shadow_case_controlled_with_allowed_target(const char *source, const char *scope,
                                                       const char *spelling, const char *allowed) {
    stabilization_t f;
    initialize(&f, (cbm_resolve_solver_limits_t){0});
    cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
    bool ok = state && collect_fixture(state, "api/types.go", "test.api", source, NULL, NULL) &&
              cbm_go_stabilization_register(state) && cbm_resolve_solver_solve(f.solver) &&
              cbm_go_stabilization_seal(state);
    cbm_resolve_result_t result = {0};
    bool controlled =
        ok && fixture_resolution(state, "api/types.go", source, scope, spelling, &result);
    bool admitted = controlled && cbm_resolve_admit(&result);
    bool acceptable = (!admitted && !result.target) ||
                      (admitted && allowed && result.target && !strcmp(result.target, allowed));
    if (!ok || !controlled || !acceptable)
        fprintf(stderr,
                "stabilization negative: scope=%s spelling=%s setup=%d controlled=%d "
                "admitted=%d target=%s analysis=%d reason=%s\n",
                scope, spelling, ok, controlled, admitted, result.target ? result.target : "<none>",
                (int)result.analysis, result.reason ? result.reason : "<none>");
    ok = ok && controlled && acceptable;
    cbm_go_stabilization_dispose(state);
    cbm_arena_destroy(&f.arena);
    return ok;
}
static bool go_selects(const char *source, const char *scope, const char *anchor,
                       const char *expected);
static bool shadow_case_controlled_and_unknown(const char *source, const char *scope,
                                               const char *spelling) {
    return shadow_case_controlled_with_allowed_target(source, scope, spelling, NULL);
}
TEST(resolver_stabilization_generic_type_shadow_never_binds_package_type) {
    const char *source = "package api\n"
                         "type T struct{}\nfunc (v T) M() {}\n"
                         "type I interface{ M() }\n"
                         "func F[T I](x T) { x.M() }\n";
    ASSERT(shadow_case_controlled_and_unknown(source, "test.api.F", "x.M"));
    PASS();
}
TEST(resolver_stabilization_generic_receiver_parameters_shadow_package_types) {
    const char *value = "package api\n"
                        "type A struct{}\nfunc (A) Hit(){}\n"
                        "type Box[T interface{Hit()}] struct{}\n"
                        "func (b Box[A]) F(){ var x A; x.Hit() }\n";
    ASSERT(shadow_case_controlled_and_unknown(value, "test.api.Box.F", "x.Hit"));
    const char *pointer = "package api\n"
                          "type A struct{}\nfunc (A) Hit(){}\n"
                          "type Box[T interface{Hit()}] struct{}\n"
                          "func (b *Box[A]) G(){ var x A; x.Hit() }\n";
    ASSERT(shadow_case_controlled_and_unknown(pointer, "test.api.Box.G", "x.Hit"));
    PASS();
}
TEST(resolver_stabilization_captured_binding_does_not_regain_narrowed_authority) {
    const char *source = "package api\n"
                         "type A struct{}\nfunc(A) M(){}\n"
                         "type B struct{}\nfunc(B) M(){}\n"
                         "func F(){ var x interface{M()}; set:=func(){x=B{}}; "
                         "x=A{}; set(); x.M() }\n";
    ASSERT(shadow_case_controlled_and_unknown(source, "test.api.F", "x.M"));
    PASS();
}
TEST(resolver_stabilization_address_escape_survives_later_rebinding) {
    const char *immediate = "package api\n"
                            "type A struct{}\nfunc(A) M(){}\n"
                            "type B struct{}\nfunc(B) M(){}\n"
                            "func mutate(p *interface{M()}){*p=B{}}\n"
                            "func F(){ var x interface{M()}; x=A{}; mutate(&x); x.M() }\n";
    ASSERT(shadow_case_controlled_and_unknown(immediate, "test.api.F", "x.M"));
    const char *retained = "package api\n"
                           "type A struct{}\nfunc(A) M(){}\n"
                           "type B struct{}\nfunc(B) M(){}\n"
                           "var held *interface{M()}\n"
                           "func hold(p *interface{M()}){held=p}\n"
                           "func mutate(){*held=B{}}\n"
                           "func F(){ var x interface{M()}; hold(&x); x=A{}; mutate(); x.M() }\n";
    ASSERT(shadow_case_controlled_and_unknown(retained, "test.api.F", "x.M"));
    PASS();
}
TEST(resolver_stabilization_assignment_lhs_escape_survives_later_rebinding) {
    const char *source = "package api\n"
                         "type A struct{}\nfunc(A) M(){}\n"
                         "type B struct{}\nfunc(B) M(){}\n"
                         "var held *interface{M()}\n"
                         "func remember(p *interface{M()}) int {held=p; return 0}\n"
                         "func mutate(){*held=B{}}\n"
                         "func F(){var x interface{M()}; a:=[1]int{}; "
                         "a[remember(&x)]=0; x=A{}; mutate(); x.M()}\n";
    ASSERT(shadow_case_controlled_and_unknown(source, "test.api.F", "x.M"));
    PASS();
}
TEST(resolver_stabilization_async_capture_does_not_regain_narrowed_authority) {
    const char *source = "package api\n"
                         "type A struct{}\nfunc(A) M(){}\n"
                         "type B struct{}\nfunc(B) M(){}\n"
                         "func F(){ var x interface{M()}; go func(){x=B{}}(); "
                         "x=A{}; x.M() }\n";
    ASSERT(shadow_case_controlled_and_unknown(source, "test.api.F", "x.M"));
    PASS();
}
TEST(resolver_stabilization_multi_declaration_does_not_overwrite_outer_binding) {
    const char *source = "package api\n"
                         "type A struct{}\nfunc(A) M(){}\n"
                         "type B struct{}\nfunc(B) M(){}\n"
                         "func F(){ x:=A{}; {x,y:=B{},0; _=y; x=B{}}; x.M() }\n";
    ASSERT(shadow_case_controlled_with_allowed_target(source, "test.api.F", "x.M", "test.api.A.M"));
    PASS();
}
TEST(resolver_stabilization_grouped_var_callable_shadows_package_callable) {
    const char *source = "package api\n"
                         "type A struct{}\nfunc(A) M(){}\n"
                         "type B struct{}\nfunc(B) M(){}\n"
                         "func Make() A{return A{}}\n"
                         "func F(){\n"
                         "var (\nMake=func() B{return B{}}\n)\n"
                         "x:=Make(); x.M()\n}\n";
    /* The local closure returns B. The package's Make returning A must not be consulted. */
    ASSERT(go_selects(source, "test.api.F", "x.M", "test.api.B.M"));
    PASS();
}
TEST(resolver_stabilization_nonlocal_assignment_does_not_invent_local_authority) {
    const char *source = "package api\n"
                         "type A struct{}\nfunc(A) M(){}\n"
                         "type B struct{}\nfunc(B) M(){}\n"
                         "var x interface{M()}\n"
                         "func set(){x=B{}}\n"
                         "func F(){ x=A{}; set(); x.M() }\n";
    ASSERT(shadow_case_controlled_and_unknown(source, "test.api.F", "x.M"));
    PASS();
}
TEST(resolver_stabilization_local_type_shadows_function_and_package_type) {
    const char *conversion = "package api\nfunc Target() {}\n"
                             "func Run() { type Target int; _ = Target(1) }\n";
    ASSERT(shadow_case_controlled_and_unknown(conversion, "test.api.Run", "Target(1"));
    const char *annotation = "package api\n"
                             "type T struct{}\nfunc(v T) M(){}\n"
                             "type Other struct{}\nfunc(v Other) M(){}\n"
                             "func Run() { type T = Other; var x T; x.M() }\n";
    ASSERT(shadow_case_controlled_and_unknown(annotation, "test.api.Run", "x.M"));
    PASS();
}

TEST(resolver_stabilization_named_result_shadows_package_callable) {
    const char *source = "package api\nfunc Target() {}\n"
                         "func Run() (Target func()) { Target(); return }\n";
    ASSERT(shadow_case_controlled_and_unknown(source, "test.api.Run", "Target(); return"));
    PASS();
}
TEST(resolver_stabilization_inapplicable_pointer_methods_abstain) {
    const char *temporary = "package api\ntype T struct{}\nfunc(v *T) M(){}\n"
                            "func Make() T { return T{} }\nfunc Run() { Make().M() }\n";
    ASSERT(shadow_case_controlled_and_unknown(temporary, "test.api.Run", "Make().M"));
    const char *double_pointer = "package api\ntype T struct{}\nfunc(v *T) M(){}\n"
                                 "func Run(p **T) { p.M() }\n";
    ASSERT(shadow_case_controlled_and_unknown(double_pointer, "test.api.Run", "p.M"));
    PASS();
}
TEST(resolver_stabilization_external_import_is_unknown_not_global_failure) {
    const char *source = "package app\nimport \"fmt\"\nfunc Run(){fmt.Println(\"x\")}\n";
    stabilization_t f;
    initialize(&f, (cbm_resolve_solver_limits_t){0});
    cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
    ASSERT_NOT_NULL(state);
    ASSERT(collect_fixture(state, "app/main.go", "test.app", source, "fmt", ""));
    ASSERT(cbm_go_stabilization_register(state));
    ASSERT(cbm_resolve_solver_solve(f.solver));
    ASSERT(cbm_go_stabilization_seal(state));
    cbm_resolve_result_t result;
    ASSERT(
        fixture_resolution(state, "app/main.go", source, "test.app.Run", "fmt.Println", &result));
    ASSERT(!cbm_resolve_admit(&result));
    cbm_go_stabilization_dispose(state);
    cbm_arena_destroy(&f.arena);
    PASS();
}

TEST(resolver_stabilization_bodyless_declaration_does_not_poison_collection) {
    const char *source = "package api\nfunc Assembly() int\n"
                         "type T struct{}\nfunc (T) M(){}\nfunc Run(){x:=T{};x.M()}\n";
    stabilization_t f;
    initialize(&f, (cbm_resolve_solver_limits_t){0});
    cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
    ASSERT_NOT_NULL(state);
    ASSERT(collect_fixture(state, "api/types.go", "test.api", source, NULL, NULL));
    ASSERT(cbm_go_stabilization_register(state));
    ASSERT(cbm_resolve_solver_solve(f.solver));
    ASSERT(cbm_go_stabilization_seal(state));
    cbm_resolve_result_t result;
    ASSERT(fixture_resolution(state, "api/types.go", source, "test.api.Run", "x.M", &result));
    ASSERT(cbm_resolve_admit(&result));
    ASSERT_STR_EQ(result.target, "test.api.T.M");
    cbm_go_stabilization_dispose(state);
    cbm_arena_destroy(&f.arena);
    PASS();
}

TEST(resolver_stabilization_applicable_value_and_pointer_receivers_remain_exact) {
    const char *source = "package api\ntype T struct{}\n"
                         "func(v T) M(){}\nfunc(v *T) P(){}\n"
                         "func Make() T { return T{} }\n"
                         "func Run(p *T) { p.M(); p.P(); x:=T{}; x.P(); Make().M() }\n";
    const char *spellings[] = {"p.M", "p.P", "x.P", "Make().M"};
    const char *targets[] = {"test.api.T.M", "test.api.T.P", "test.api.T.P", "test.api.T.M"};
    stabilization_t f;
    initialize(&f, (cbm_resolve_solver_limits_t){0});
    cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
    ASSERT_NOT_NULL(state);
    ASSERT(collect_fixture(state, "api/types.go", "test.api", source, NULL, NULL));
    ASSERT(cbm_go_stabilization_register(state));
    ASSERT(cbm_resolve_solver_solve(f.solver));
    ASSERT(cbm_go_stabilization_seal(state));
    for (size_t i = 0; i < sizeof(spellings) / sizeof(spellings[0]); i++) {
        cbm_resolve_result_t result;
        ASSERT(fixture_resolution(state, "api/types.go", source, "test.api.Run", spellings[i],
                                  &result));
        ASSERT(cbm_resolve_admit(&result));
        ASSERT_STR_EQ(result.target, targets[i]);
    }
    cbm_go_stabilization_dispose(state);
    cbm_arena_destroy(&f.arena);
    PASS();
}
TEST(resolver_stabilization_declared_concrete_identity_survives_capture_and_escape) {
    const char *source = "package api\ntype A struct{}\nfunc(A) M(){}\n"
                         "func mutate(p *A){*p=A{}}\n"
                         "func F(){var x A; set:=func(){x=A{}};set();mutate(&x);x=A{};x.M()}\n";
    stabilization_t f;
    initialize(&f, (cbm_resolve_solver_limits_t){0});
    cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
    ASSERT_NOT_NULL(state);
    ASSERT(collect_fixture(state, "api/types.go", "test.api", source, NULL, NULL));
    ASSERT(cbm_go_stabilization_register(state));
    ASSERT(cbm_resolve_solver_solve(f.solver));
    ASSERT(cbm_go_stabilization_seal(state));
    cbm_resolve_result_t result;
    ASSERT(fixture_resolution(state, "api/types.go", source, "test.api.F", "x.M", &result));
    ASSERT(cbm_resolve_admit(&result));
    ASSERT_STR_EQ(result.target, "test.api.A.M");
    cbm_go_stabilization_dispose(state);
    cbm_arena_destroy(&f.arena);
    PASS();
}

TEST(resolver_stabilization_nested_module_is_not_import_authority) {
    const char *root = th_mktempdir("cbm_go_module_scope");
    ASSERT_NOT_NULL(root);
    ASSERT(cbm_mkdir_p(TH_PATH(root, "app"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(root, "unrelated"), 0755));
    ASSERT_EQ(th_write_file(TH_PATH(root, "go.mod"), "module example/root\ngo 1.22\n"), 0);
    ASSERT_EQ(
        th_write_file(TH_PATH(root, "unrelated/go.mod"), "module example/dependency\ngo 1.22\n"),
        0);
    const char *source =
        "package app\nimport \"example/dependency\"\nfunc Run(){dependency.Make()}\n";
    ASSERT_EQ(th_write_file(TH_PATH(root, "app/main.go"), source), 0);
    ASSERT_EQ(
        th_write_file(TH_PATH(root, "unrelated/lib.go"), "package dependency\nfunc Make(){}\n"), 0);
    CBMFileResult *parsed = cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test",
                                             "app/main.go", 0, NULL, NULL);
    ASSERT_NOT_NULL(parsed);
    cbm_gbuf_t *graph = cbm_gbuf_new("test", root);
    ASSERT_NOT_NULL(graph);
    ASSERT(cbm_gbuf_upsert_node(graph, "File", "main.go", "test.app.main.__file__", "app/main.go",
                                1, 3, "{}"));
    ASSERT(cbm_gbuf_upsert_node(graph, "Folder", "unrelated", "test.unrelated", "unrelated", 0, 0,
                                "{}"));
    /* This is the flat package map produced by discovering both manifests.
     * Neither a require/replace directive nor go.work binds the second module. */
    CBMHashTable *map = cbm_ht_create(16);
    ASSERT_NOT_NULL(map);
    cbm_ht_set(map, "example/root", (void *)"test");
    cbm_ht_set(map, "example/dependency", (void *)"test.unrelated");
    CBMHashTable *saved = cbm_pipeline_get_pkgmap();
    cbm_pipeline_set_pkgmap(map);
    cbm_pipeline_ctx_t pipeline = {.project_name = "test", .repo_path = root, .gbuf = graph};
    bool captured = cbm_pipeline_capture_go_imports(&pipeline, "app/main.go", parsed);
    cbm_pipeline_set_pkgmap(saved);
    ASSERT(captured);
    const cbm_resolve_import_snapshot_t *imports = cbm_gbuf_imports(graph, "app/main.go");
    ASSERT_NOT_NULL(imports);
    ASSERT_EQ(imports->count, 1);
    if (cbm_resolve_admit(&imports->items[0].dependency))
        fprintf(stderr, "unrelated nested module admitted target=%s\n",
                imports->items[0].dependency.target);
    ASSERT(!cbm_resolve_admit(&imports->items[0].dependency));
    cbm_ht_free(map);
    cbm_gbuf_free(graph);
    cbm_free_result(parsed);
    th_rmtree(root);
    PASS();
}

TEST(resolver_stabilization_normalized_imports_ignore_projection_and_reject_stale_source) {
    const char *root = th_mktempdir("cbm_go_import_snapshot");
    ASSERT_NOT_NULL(root);
    ASSERT(cbm_mkdir_p(TH_PATH(root, "app"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(root, "bridge"), 0755));
    ASSERT_EQ(th_write_file(TH_PATH(root, "go.mod"), "module example\ngo 1.22\n"), 0);
    const char *source = "package app\nimport \"example/bridge\"\nfunc Run(){ bridge.Make() }\n";
    const char *library = "package bridge\nfunc Make() {}\n";
    const char *rel = "app/main.go";
    for (int mode = 0; mode < 5; mode++) {
        CBMFileResult *parsed =
            cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test", rel, 0, NULL, NULL);
        ASSERT_NOT_NULL(parsed);
        cbm_gbuf_t *graph = cbm_gbuf_new("test", root);
        ASSERT_NOT_NULL(graph);
        char *file_qn = cbm_pipeline_fqn_compute("test", rel, "__file__");
        ASSERT_NOT_NULL(file_qn);
        int64_t file = cbm_gbuf_upsert_node(graph, "File", rel, file_qn, rel, 1, 3, "{}");
        int64_t target =
            cbm_gbuf_upsert_node(graph, "Folder", "bridge", "test.bridge", "bridge", 0, 0, "{}");
        int64_t decoy =
            cbm_gbuf_upsert_node(graph, "Folder", "decoy", "test.decoy", "decoy", 0, 0, "{}");
        ASSERT(file && target && decoy);
        ASSERT(cbm_gbuf_insert_edge(graph, file, target, "IMPORTS", "{}") > 0);
        CBMHashTable *map = cbm_ht_create(16);
        ASSERT_NOT_NULL(map);
        cbm_ht_set(map, "example/bridge", (void *)"test.bridge");
        CBMHashTable *saved = cbm_pipeline_get_pkgmap();
        cbm_pipeline_set_pkgmap(map);
        cbm_pipeline_ctx_t pipeline = {.project_name = "test", .repo_path = root, .gbuf = graph};
        bool captured = cbm_pipeline_capture_go_imports(&pipeline, rel, parsed);
        cbm_pipeline_set_pkgmap(saved);
        ASSERT(captured);
        if (mode == 1 || mode == 2) {
            ASSERT_EQ(cbm_gbuf_delete_edges_by_type(graph, "IMPORTS"), 0);
            ASSERT_EQ(cbm_gbuf_edge_count_by_type(graph, "IMPORTS"), 0);
            if (mode == 2)
                ASSERT(cbm_gbuf_insert_edge(graph, file, decoy, "IMPORTS",
                                            "{\"local_name\":\"bridge\"}") > 0);
            /* Re-capture after deletion/forgery, so even the production writer
             * must recover its answer from the exact path observation. */
            cbm_pipeline_set_pkgmap(map);
            captured = cbm_pipeline_capture_go_imports(&pipeline, rel, parsed);
            cbm_pipeline_set_pkgmap(saved);
            ASSERT(captured);
        }
        const cbm_resolve_import_snapshot_t *imports = cbm_gbuf_imports(graph, rel);
        ASSERT_NOT_NULL(imports);
        ASSERT(imports->complete && imports->count == 1);
        ASSERT(cbm_resolve_admit(&imports->items[0].dependency));
        ASSERT_STR_EQ(imports->items[0].dependency.target, "test.bridge");
        char changed[256];
        snprintf(changed, sizeof(changed), "%s// changed source identity\n", source);
        const char *current = mode == 3 ? changed : source;
        CBMFileResult *consumer = cbm_extract_file(current, (int)strlen(current), CBM_LANG_GO,
                                                   "test", rel, 0, NULL, NULL);
        ASSERT_NOT_NULL(consumer);
        if (mode == 4)
            snprintf(consumer->source_configuration_id, sizeof(consumer->source_configuration_id),
                     "changed-configuration");
        cbm_resolve_proof_source_t context = {
            .project = "test",
            .file = rel,
            .scope = "test.app",
            .module = "test.app",
            .content_identity = consumer->source_content_id[CBM_SOURCE_ORIGIN_RAW],
            .configuration_identity = consumer->source_configuration_id};
        stabilization_t f;
        initialize(&f, (cbm_resolve_solver_limits_t){0});
        cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
        ASSERT_NOT_NULL(state);
        ASSERT(collect_fixture(state, "bridge/make.go", "test.bridge", library, NULL, NULL));
        ASSERT(cbm_go_stabilization_collect(state, consumer, current, strlen(current), &context,
                                            "test.app", imports));
        cbm_gbuf_free(graph); /* Normalization owns every proof and string now. */
        cbm_free_result(consumer);
        cbm_free_result(parsed);
        cbm_ht_free(map);
        free(file_qn);
        ASSERT(cbm_go_stabilization_register(state));
        ASSERT(cbm_resolve_solver_solve(f.solver));
        ASSERT(cbm_go_stabilization_seal(state));
        cbm_resolve_result_t result;
        ASSERT(fixture_resolution(state, rel, current, "test.app.Run", "bridge.Make", &result));
        ASSERT_EQ(cbm_resolve_admit(&result), mode < 3);
        if (mode < 3)
            ASSERT_STR_EQ(result.target, "test.bridge.Make");
        else
            ASSERT_NULL(result.target);
        cbm_go_stabilization_dispose(state);
        cbm_arena_destroy(&f.arena);
    }
    th_rmtree(root);
    PASS();
}
TEST(resolver_stabilization_unsupported_go_forms_do_not_poison_other_bindings) {
    const char *source =
        "package api\n"
        "type T struct{}\ntype Alias = T\ntype Array [2]T\n"
        "type Interface interface { M() }\ntype Generic[T any] struct { Value T }\n"
        "func init() {}\nfunc init() {}\nfunc Target() {}\n"
        "func Pair() (T, error) { return T{}, nil }\n"
        "func GenericFunc[T any](x T) T { return x }\n"
        "func Run() { var a []T; var m map[string]T; var f func(); "
        "a, m, f = nil, nil, nil; _, _ = Pair(); Target() }\n";
    stabilization_t f;
    initialize(&f, (cbm_resolve_solver_limits_t){0});
    cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
    ASSERT_NOT_NULL(state);
    ASSERT(collect_fixture(state, "api/types.go", "test.api", source, NULL, NULL));
    ASSERT(cbm_go_stabilization_register(state));
    ASSERT(cbm_resolve_solver_solve(f.solver));
    ASSERT(cbm_go_stabilization_seal(state));
    cbm_resolve_result_t result;
    ASSERT(
        fixture_resolution(state, "api/types.go", source, "test.api.Run", "Target() }", &result));
    ASSERT(cbm_resolve_admit(&result));
    ASSERT_STR_EQ(result.target, "test.api.Target");
    cbm_go_stabilization_dispose(state);
    cbm_arena_destroy(&f.arena);
    PASS();
}

static int scalar(sqlite3 *db, const char *sql) {
    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK)
        return -1;
    int result = sqlite3_step(s) == SQLITE_ROW ? sqlite3_column_int(s, 0) : -1;
    sqlite3_finalize(s);
    return result;
}
/* Read-only store handles intentionally deny ATTACH. Compare complete rows
 * through independent connections, including every stored metadata column. */
static bool same_derived_bindings(sqlite3 *left, sqlite3 *right) {
    sqlite3_stmt *a = NULL, *b = NULL;
    const char *sql = "SELECT * FROM resolver_derived_bindings ORDER BY key";
    bool equal = false;
    if (sqlite3_prepare_v2(left, sql, -1, &a, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(right, sql, -1, &b, NULL) != SQLITE_OK)
        goto done;
    int columns = sqlite3_column_count(a);
    if (columns != sqlite3_column_count(b))
        goto done;
    for (;;) {
        int ra = sqlite3_step(a), rb = sqlite3_step(b);
        if (ra == SQLITE_DONE && rb == SQLITE_DONE) {
            equal = true;
            break;
        }
        if (ra != SQLITE_ROW || rb != SQLITE_ROW)
            break;
        for (int i = 0; i < columns; i++) {
            if (sqlite3_column_type(a, i) != sqlite3_column_type(b, i))
                goto done;
            const void *va = sqlite3_column_blob(a, i), *vb = sqlite3_column_blob(b, i);
            int na = sqlite3_column_bytes(a, i), nb = sqlite3_column_bytes(b, i);
            if (na != nb || (na && (!va || !vb || memcmp(va, vb, (size_t)na))))
                goto done;
        }
    }
done:
    sqlite3_finalize(a);
    sqlite3_finalize(b);
    return equal;
}
static int index_fixture(const char *repo, const char *database, bool parallel) {
    const char *workers = getenv("CBM_WORKERS"), *single = getenv("CBM_INDEX_SINGLE_THREAD");
    char *old_workers = workers ? strdup(workers) : NULL,
         *old_single = single ? strdup(single) : NULL;
    cbm_setenv("CBM_WORKERS", parallel ? "4" : "1", 1);
    if (parallel)
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    else
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", "1", 1);
    cbm_pipeline_t *pipeline = cbm_pipeline_new(repo, database, CBM_MODE_FULL);
    int rc = pipeline && cbm_pipeline_set_project_name(pipeline, "test")
                 ? cbm_pipeline_run(pipeline)
                 : -1;
    cbm_pipeline_free(pipeline);
    if (old_workers)
        cbm_setenv("CBM_WORKERS", old_workers, 1);
    else
        cbm_unsetenv("CBM_WORKERS");
    if (old_single)
        cbm_setenv("CBM_INDEX_SINGLE_THREAD", old_single, 1);
    else
        cbm_unsetenv("CBM_INDEX_SINGLE_THREAD");
    free(old_workers);
    free(old_single);
    return rc;
}
/* What one call site selects in a one-package fixture; expected NULL means abstention. */
static bool go_selects(const char *source, const char *scope, const char *anchor,
                       const char *expected) {
    stabilization_t f;
    initialize(&f, (cbm_resolve_solver_limits_t){0});
    cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
    bool ok = state && collect_fixture(state, "api/types.go", "test.api", source, NULL, NULL) &&
              cbm_go_stabilization_register(state) && cbm_resolve_solver_solve(f.solver) &&
              cbm_go_stabilization_seal(state);
    cbm_resolve_result_t result = {0};
    bool controlled =
        ok && fixture_resolution(state, "api/types.go", source, scope, anchor, &result);
    bool admitted = controlled && cbm_resolve_admit(&result);
    bool right = controlled && (expected ? admitted && result.target &&
                                               !strcmp(result.target, expected)
                                         : !admitted && !result.target);
    if (!right)
        fprintf(stderr, "go_selects: %s at %s: setup=%d controlled=%d target=%s expected=%s\n",
                anchor, scope, ok, controlled, result.target ? result.target : "<none>",
                expected ? expected : "<abstain>");
    cbm_go_stabilization_dispose(state);
    cbm_arena_destroy(&f.arena);
    return right;
}

#define GO_TWO_TYPES                     \
    "package api\n"                      \
    "type A struct{}\nfunc (A) M() {}\n" \
    "type B struct{}\nfunc (B) M() {}\nfunc (B) N() {}\n"

/* client-go: `if err != nil { return }` and friends surround nearly every call. */
TEST(resolver_stabilization_go_calls_in_every_statement_form_are_visited) {
    const char *source = GO_TWO_TYPES
        "func New() *A { return nil }\n"
        "func F(flag bool, xs []int, ch chan int) {\n"
        "  x := New()\n"
        "  if flag { x = nil; x.M(/*if*/) }\n"
        "  for range xs { x.M(/*for*/) }\n"
        "  switch { case flag: x.M(/*switch*/); default: x.M(/*default*/) }\n"
        "  select { case <-ch: x.M(/*select*/) }\n"
        "  go func() { x.M(/*closure*/) }()\n"
        "  defer x.M(/*defer*/)\n"
        "L:\n  for { x.M(/*label*/); break L }\n"
        "  x.M(/*after*/)\n"
        "}\n";
    const char *anchors[] = {"x.M(/*if*/",      "x.M(/*for*/",     "x.M(/*switch*/",
                             "x.M(/*default*/", "x.M(/*select*/",  "x.M(/*closure*/",
                             "x.M(/*defer*/",   "x.M(/*label*/",   "x.M(/*after*/"};
    for (size_t i = 0; i < sizeof(anchors) / sizeof(anchors[0]); i++)
        ASSERT(go_selects(source, "test.api.F", anchors[i], "test.api.A.M"));
    PASS();
}

/* Frames still decide which variable a name means. */
TEST(resolver_stabilization_go_block_frames_shadow_exactly) {
    const char *source = GO_TWO_TYPES
        "func New() *A { return nil }\n"
        "func F(flag bool) {\n"
        "  x := New()\n"
        "  if x := (B{}); flag { x.M(/*init*/) }\n"
        "  { x := B{}; x.M(/*block*/) }\n"
        "  x.M(/*outer*/)\n"
        "  for x := range []B{} { _ = x }\n"
        "  x.M(/*after*/)\n"
        "}\n";
    ASSERT(go_selects(source, "test.api.F", "x.M(/*init*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "x.M(/*block*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "x.M(/*outer*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "x.M(/*after*/", "test.api.A.M"));
    PASS();
}

/* `func New() *pkg.T` selects a member of pkg. It declares no local T for the body, while a
 * parameter or a named result really does. */
TEST(resolver_stabilization_go_bare_result_types_declare_no_local) {
    const char *source = GO_TWO_TYPES
        "func T() A { return A{} }\n"
        "func F() *other.T {\n"
        "  T().M(/*pointer*/)\n"
        "  return nil\n"
        "}\n"
        "func G() []other.T {\n"
        "  T().M(/*slice*/)\n"
        "  return nil\n"
        "}\n"
        "func H() {\n"
        "  _ = func() *other.T {\n"
        "    T().M(/*literal*/)\n"
        "    return nil\n"
        "  }\n"
        "}\n"
        "func P(T B) *other.T {\n"
        "  T.M(/*parameter*/)\n"
        "  return nil\n"
        "}\n"
        "func R() (T B) {\n"
        "  T.M(/*named-result*/)\n"
        "  return\n"
        "}\n";
    ASSERT(go_selects(source, "test.api.F", "T().M(/*pointer*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.G", "T().M(/*slice*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.H", "T().M(/*literal*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.P", "T.M(/*parameter*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.R", "T.M(/*named-result*/", "test.api.B.M"));
    PASS();
}

/* kubectl: o.Builder().WithScheme(). The static type of calling a function value is its declared
 * result, whether the value is a field, a parameter or a closure. Several results have no single
 * type, and what comes out of a container or a dereference stays unknown. */
TEST(resolver_stabilization_go_function_values_yield_their_declared_result) {
    const char *source = GO_TWO_TYPES
        "type Opts struct {\n"
        "  Make func() *A\n  Named func() (out B)\n  Many func() (A, error)\n  Plain func()\n"
        "}\n"
        "type Holder struct{ Opts }\n"
        "var pointer *func() *A\n"
        "func F(o Opts, h Holder, fn func() *A, many func() (A, error), fs []func() *A) {\n"
        "  o.Make().M(/*field*/)\n"
        "  o.Named().M(/*named-result*/)\n"
        "  o.Many().M(/*several*/)\n"
        "  h.Make().M(/*promoted*/)\n"
        "  fn().M(/*parameter*/)\n"
        "  x := fn()\n"
        "  x.M(/*stored*/)\n"
        "  many().M(/*several-parameter*/)\n"
        "  l := func() B { return B{} }\n"
        "  l().M(/*closure*/)\n"
        "  fs[0]().M(/*from-slice*/)\n"
        "  (*pointer)().M(/*dereference*/)\n"
        "}\n";
    ASSERT(go_selects(source, "test.api.F", "o.Make().M(/*field*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "o.Named().M(/*named-result*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "o.Many().M(/*several*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "h.Make().M(/*promoted*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "fn().M(/*parameter*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "x.M(/*stored*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "many().M(/*several-parameter*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "l().M(/*closure*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "fs[0]().M(/*from-slice*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "(*pointer)().M(/*dereference*/", NULL));
    PASS();
}

TEST(resolver_stabilization_go_tuple_results_bind_by_position) {
    const char *source = GO_TWO_TYPES
        "func Pair() (*A, *B) { return nil, nil }\n"
        "func Named() (x, y A, z B) { return }\n"
        "func Run() {\n"
        "  a, b := Pair()\n"
        "  a.M(/*first*/)\n"
        "  b.M(/*second*/)\n"
        "  _, q, r := Named()\n"
        "  q.M(/*grouped*/)\n"
        "  r.M(/*third*/)\n"
        "}\n";
    ASSERT(go_selects(source, "test.api.Run", "a.M(/*first*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.Run", "b.M(/*second*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.Run", "q.M(/*grouped*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.Run", "r.M(/*third*/", "test.api.B.M"));
    PASS();
}

/* client-go informers: f.factory.InformerFor(...), c.client.Put().Name(...). */
TEST(resolver_stabilization_go_field_chains_follow_declared_field_types) {
    const char *source = GO_TWO_TYPES
        "type H struct {\n"
        "  impl *A\n"
        "  left, right B\n"
        "  next *H\n"
        "  call func() int\n"
        "}\n"
        "func (h *H) Self() *H { return h }\n"
        "func F(h *H) {\n"
        "  h.impl.M(/*field*/)\n"
        "  h.right.N(/*grouped*/)\n"
        "  h.next.next.impl.M(/*deep*/)\n"
        "  h.Self().impl.M(/*through-call*/)\n"
        "  h.call(/*func-field*/)\n"
        "  h.absent.M(/*absent*/)\n"
        "}\n";
    ASSERT(go_selects(source, "test.api.F", "h.impl.M(/*field*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "h.right.N(/*grouped*/", "test.api.B.N"));
    ASSERT(go_selects(source, "test.api.F", "h.next.next.impl.M(/*deep*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "h.Self().impl.M(/*through-call*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "h.call(/*func-field*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "h.absent.M(/*absent*/", NULL));
    PASS();
}

/* CALLS names the interface method a static type selects, whatever implements it. */
TEST(resolver_stabilization_go_interface_methods_are_their_own_declarations) {
    const char *source = GO_TWO_TYPES
        "type I interface {\n  M()\n  Sub() J\n}\n"
        "type J interface {\n  N()\n}\n"
        "type Both interface {\n  I\n  J\n  Own()\n}\n"
        "func Make() I { return nil }\n"
        "func F(i I, b Both) {\n"
        "  i.M(/*direct*/)\n"
        "  i.Sub().N(/*chain*/)\n"
        "  Make().M(/*result*/)\n"
        "  b.M(/*embedded*/)\n"
        "  b.N(/*embedded-j*/)\n"
        "  b.Own(/*own*/)\n"
        "}\n";
    ASSERT(go_selects(source, "test.api.F", "i.M(/*direct*/", "test.api.I.M"));
    ASSERT(go_selects(source, "test.api.F", "i.Sub().N(/*chain*/", "test.api.J.N"));
    ASSERT(go_selects(source, "test.api.F", "Make().M(/*result*/", "test.api.I.M"));
    ASSERT(go_selects(source, "test.api.F", "b.M(/*embedded*/", "test.api.I.M"));
    ASSERT(go_selects(source, "test.api.F", "b.N(/*embedded-j*/", "test.api.J.N"));
    ASSERT(go_selects(source, "test.api.F", "b.Own(/*own*/", "test.api.Both.Own"));
    PASS();
}

/* The shallowest depth holding a name decides; two holders at that depth is illegal Go. */
TEST(resolver_stabilization_go_embedding_follows_the_selector_rule) {
    const char *source =
        "package api\n"
        "type In struct{}\nfunc (*In) Ptr() {}\nfunc (In) Val() {}\nfunc (In) Deep() {}\n"
        "type Mid struct{ In }\nfunc (Mid) Deep() {}\n"
        "type Out struct {\n  *Mid\n  Other\n}\n"
        "type Other struct{}\nfunc (Other) Twin() {}\nfunc (Other) Own() {}\n"
        "type Second struct{}\nfunc (Second) Twin() {}\n"
        "type Both struct {\n  Other\n  Second\n}\n"
        "type Shadow struct {\n  In\n  Val func()\n}\n"
        "func F(o Out, b Both, s Shadow, m *Mid) {\n"
        "  o.Ptr(/*depth2*/)\n"
        "  o.Val(/*depth2-val*/)\n"
        "  o.Deep(/*shallow-wins*/)\n"
        "  o.Own(/*depth1*/)\n"
        "  b.Twin(/*two-holders*/)\n"
        "  s.Val(/*field-shadows*/)\n"
        "  s.Deep(/*beside-field*/)\n"
        "  m.Ptr(/*through-pointer*/)\n"
        "  o.Mid.Deep(/*named-embed*/)\n"
        "}\n";
    ASSERT(go_selects(source, "test.api.F", "o.Ptr(/*depth2*/", "test.api.In.Ptr"));
    ASSERT(go_selects(source, "test.api.F", "o.Val(/*depth2-val*/", "test.api.In.Val"));
    ASSERT(go_selects(source, "test.api.F", "o.Deep(/*shallow-wins*/", "test.api.Mid.Deep"));
    ASSERT(go_selects(source, "test.api.F", "o.Own(/*depth1*/", "test.api.Other.Own"));
    ASSERT(go_selects(source, "test.api.F", "b.Twin(/*two-holders*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "s.Val(/*field-shadows*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "s.Deep(/*beside-field*/", "test.api.In.Deep"));
    ASSERT(go_selects(source, "test.api.F", "m.Ptr(/*through-pointer*/", "test.api.In.Ptr"));
    ASSERT(go_selects(source, "test.api.F", "o.Mid.Deep(/*named-embed*/", "test.api.Mid.Deep"));
    PASS();
}

/* An embedded type we cannot enumerate might hold the name at its own depth, so only what is
 * declared directly stays proved. Mirrors `struct { sync.Mutex; Inner }`. */
TEST(resolver_stabilization_go_open_embedded_type_blocks_promotion) {
    const char *source = "package api\n"
                         "type In struct{}\nfunc (In) Val() {}\n"
                         "type Open struct {\n  Undeclared\n  In\n}\nfunc (Open) Own() {}\n"
                         "func F(o Open) {\n"
                         "  o.Val(/*blocked*/)\n"
                         "  o.Own(/*direct*/)\n"
                         "}\n";
    ASSERT(go_selects(source, "test.api.F", "o.Val(/*blocked*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "o.Own(/*direct*/", "test.api.Open.Own"));
    PASS();
}

/* component-base/metrics: CounterVec embeds *prometheus.CounterVec beside lazyMetric, and calls
 * cv.lazyInit. Another package's type cannot hold this package's non-exported name, but it may
 * hold any exported one. A bare undeclared name may be this package's, so it hides both. */
TEST(resolver_stabilization_go_foreign_embed_hides_only_exported_names) {
    const char *source = "package api\n"
                         "type lazy struct{}\nfunc (lazy) start() {}\nfunc (lazy) Create() {}\n"
                         "type deep struct{ lazy }\n"
                         "type Vec struct {\n  *ext.Vec\n  lazy\n}\n"
                         "type Generic struct {\n  ext.Box[int]\n  lazy\n}\n"
                         "type Deep struct {\n  ext.Vec\n  deep\n}\n"
                         "type Bare struct {\n  Undeclared\n  lazy\n}\n"
                         "func F(v Vec, g Generic, d Deep, b Bare) {\n"
                         "  v.start(/*private*/)\n"
                         "  v.Create(/*exported*/)\n"
                         "  g.start(/*generic*/)\n"
                         "  g.Create(/*generic-exported*/)\n"
                         "  d.start(/*deep*/)\n"
                         "  d.Create(/*deep-exported*/)\n"
                         "  b.start(/*bare*/)\n"
                         "}\n";
    ASSERT(go_selects(source, "test.api.F", "v.start(/*private*/", "test.api.lazy.start"));
    ASSERT(go_selects(source, "test.api.F", "v.Create(/*exported*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "g.start(/*generic*/", "test.api.lazy.start"));
    ASSERT(go_selects(source, "test.api.F", "g.Create(/*generic-exported*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "d.start(/*deep*/", "test.api.lazy.start"));
    ASSERT(go_selects(source, "test.api.F", "d.Create(/*deep-exported*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "b.start(/*bare*/", NULL));
    PASS();
}

TEST(resolver_stabilization_go_range_index_and_channel_yield_their_element) {
    const char *source = GO_TWO_TYPES
        "type List []A\n"
        "type Table map[string]B\n"
        "type Struct struct{}\n"
        "func F(xs []*A, m map[string]B, ch chan A, p **A, l List, t Table, s Struct) {\n"
        "  for _, v := range xs { v.M(/*slice*/) }\n"
        "  for k, v := range m { k.M(/*map-key*/); v.M(/*map-value*/) }\n"
        "  for i := range xs { i.M(/*index*/) }\n"
        "  xs[0].M(/*indexed*/)\n"
        "  m[\"a\"].M(/*keyed*/)\n"
        "  (*p).M(/*deref*/)\n"
        "  e := <-ch\n"
        "  e.M(/*received*/)\n"
        "  for _, v := range l { v.M(/*named-slice*/) }\n"
        "  e2 := t[\"a\"]\n"
        "  e2.M(/*named-index*/)\n"
        "  s[0].M(/*struct-index*/)\n"
        "}\n";
    ASSERT(go_selects(source, "test.api.F", "v.M(/*slice*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "k.M(/*map-key*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "v.M(/*map-value*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "i.M(/*index*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "xs[0].M(/*indexed*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "m[\"a\"].M(/*keyed*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "(*p).M(/*deref*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "e.M(/*received*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "v.M(/*named-slice*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "e2.M(/*named-index*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "s[0].M(/*struct-index*/", NULL));
    PASS();
}

/* The written type decides. One spelled alias binds once per clause, so each clause must get
 * its own type: an earlier version of this rule gave every clause the first one. */
TEST(resolver_stabilization_go_assertions_and_type_switch_clauses_use_the_written_type) {
    const char *source = GO_TWO_TYPES
        "func F(v interface{}) {\n"
        "  i := v.(*A)\n"
        "  i.M(/*assert*/)\n"
        "  if j, ok := v.(B); ok { j.M(/*comma-ok*/); ok.M(/*ok*/) }\n"
        "  v.(*A).M(/*inline*/)\n"
        "  switch t := v.(type) {\n"
        "  case *A:\n    t.M(/*case-a*/)\n"
        "  case B:\n    t.M(/*case-b*/)\n"
        "  case int, string:\n    t.M(/*case-two*/)\n"
        "  default:\n    t.M(/*case-default*/)\n"
        "  }\n"
        "}\n";
    ASSERT(go_selects(source, "test.api.F", "i.M(/*assert*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "j.M(/*comma-ok*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "ok.M(/*ok*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "v.(*A).M(/*inline*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "t.M(/*case-a*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "t.M(/*case-b*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "t.M(/*case-two*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "t.M(/*case-default*/", NULL));
    PASS();
}

TEST(resolver_stabilization_go_literals_new_make_and_conversions_are_typed) {
    const char *source = GO_TWO_TYPES
        "type S string\nfunc (S) Up() {}\n"
        "func F(raw string) {\n"
        "  a := &A{}\n"
        "  a.M(/*address*/)\n"
        "  b := new(B)\n"
        "  b.M(/*new*/)\n"
        "  c := make([]A, 1)\n"
        "  c[0].M(/*make*/)\n"
        "  S(raw).Up(/*convert*/)\n"
        "  (*A)(nil).M(/*convert-pointer*/)\n"
        "}\n"
        /* new(n), since the grammar has no new(1) yet and would skip all of G as a syntax error. */
        "func G(n int) {\n"
        "  new := func(n int) *B { return nil }\n"
        "  z := new(n)\n"
        "  z.M(/*shadowed-new*/)\n"
        "}\n";
    ASSERT(go_selects(source, "test.api.F", "a.M(/*address*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "b.M(/*new*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "c[0].M(/*make*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "S(raw).Up(/*convert*/", "test.api.S.Up"));
    ASSERT(go_selects(source, "test.api.F", "(*A)(nil).M(/*convert-pointer*/", "test.api.A.M"));
    /* A local new is an ordinary closure returning *B, never the builtin. */
    ASSERT(go_selects(source, "test.api.G", "z.M(/*shadowed-new*/", "test.api.B.M"));
    PASS();
}

TEST(resolver_stabilization_go_package_variables_have_their_declared_type) {
    const char *source = GO_TWO_TYPES
        "func New() *A { return nil }\n"
        "var Default = New()\n"
        "var Typed *B\n"
        "var first, second = New(), B{}\n"
        "func F() {\n"
        "  Default.M(/*inferred*/)\n"
        "  Typed.M(/*annotated*/)\n"
        "  second.M(/*position*/)\n"
        "  Missing.M(/*undeclared*/)\n"
        "}\n"
        "func G() {\n"
        "  Default := B{}\n"
        "  Default.M(/*shadowed*/)\n"
        "}\n";
    ASSERT(go_selects(source, "test.api.F", "Default.M(/*inferred*/", "test.api.A.M"));
    ASSERT(go_selects(source, "test.api.F", "Typed.M(/*annotated*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "second.M(/*position*/", "test.api.B.M"));
    ASSERT(go_selects(source, "test.api.F", "Missing.M(/*undeclared*/", NULL));
    ASSERT(go_selects(source, "test.api.G", "Default.M(/*shadowed*/", "test.api.B.M"));
    PASS();
}

/* A defined type's method set is its own whatever the underlying type. */
TEST(resolver_stabilization_go_defined_non_struct_types_have_method_sets) {
    const char *source = "package api\n"
                         "type bits []uint64\nfunc (b bits) Set(i int) {}\n"
                         "type Fn func(int) int\nfunc (f Fn) Call(i int) int { return f(i) }\n"
                         "type other struct{}\nfunc (o other) Set(i int) {}\n"
                         "type Set = bits\n"
                         "func Make() Set { return nil }\n"
                         "func F(b bits, f Fn) {\n"
                         "  b.Set(1)\n"
                         "  f.Call(1)\n"
                         "  a := Make()\n"
                         "  a.Set(2)\n"
                         "}\n";
    ASSERT(go_selects(source, "test.api.F", "b.Set(1)", "test.api.bits.Set"));
    ASSERT(go_selects(source, "test.api.F", "f.Call(1)", "test.api.Fn.Call"));
    ASSERT(go_selects(source, "test.api.F", "a.Set(2)", "test.api.bits.Set"));
    PASS();
}

/* client-go: `type PodIndexInformer cache.TypedSharedIndexInformer[*v1.Pod]`. component-base:
 * `type GaugeOpts KubeOpts`. A new type keeps an interface's methods and a struct's fields and
 * embeds, never the methods bound to a concrete type. */
TEST(resolver_stabilization_go_defined_type_keeps_what_the_spec_says) {
    const char *source = "package api\n"
                         "type In struct{}\nfunc (In) Val() {}\n"
                         "type Getter interface {\n  Get() In\n}\n"
                         "type Generic[T any] interface {\n  Make() In\n}\n"
                         "type Named Getter\n"
                         "type Twice Named\n"
                         "type Inst Generic[int]\n"
                         "type Opts struct {\n  Inner In\n  In\n}\nfunc (Opts) Own() {}\n"
                         "type Copy Opts\nfunc (Copy) Mine() {}\n"
                         "type Over struct{ Copy }\n"
                         "type Remote other.Thing\n"
                         "type Bare Undeclared\n"
                         "type Holder struct{ Remote }\n"
                         "type Level int\nfunc (Level) Type() Level { return 0 }\n"
                         "type Node struct{ Level }\n"
                         "type Failure error\n"
                         "type Both struct {\n  Failure\n  In\n}\n"
                         "func F(n Named, t Twice, i Inst, c Copy, o Over, r Remote, b Bare, h Holder, k Node, e Both) {\n"
                         "  n.Get(/*interface*/)\n"
                         "  t.Get(/*chain*/)\n"
                         "  i.Make(/*generic*/)\n"
                         "  v := n.Get()\n"
                         "  v.Val(/*result*/)\n"
                         "  c.Inner.Val(/*field*/)\n"
                         "  c.Val(/*promoted*/)\n"
                         "  c.Mine(/*own*/)\n"
                         "  c.Own(/*not-inherited*/)\n"
                         "  o.Inner.Val(/*embedded*/)\n"
                         "  o.Own(/*embedded-not-inherited*/)\n"
                         "  r.Get(/*remote*/)\n"
                         "  b.Get(/*bare*/)\n"
                         "  h.Get(/*holder*/)\n"
                         "  k.Type(/*universe*/)\n"
                         "  e.Val(/*error*/)\n"
                         "}\n";
    ASSERT(go_selects(source, "test.api.F", "n.Get(/*interface*/", "test.api.Getter.Get"));
    ASSERT(go_selects(source, "test.api.F", "t.Get(/*chain*/", "test.api.Getter.Get"));
    ASSERT(go_selects(source, "test.api.F", "i.Make(/*generic*/", "test.api.Generic.Make"));
    ASSERT(go_selects(source, "test.api.F", "v.Val(/*result*/", "test.api.In.Val"));
    ASSERT(go_selects(source, "test.api.F", "c.Inner.Val(/*field*/", "test.api.In.Val"));
    ASSERT(go_selects(source, "test.api.F", "c.Val(/*promoted*/", "test.api.In.Val"));
    ASSERT(go_selects(source, "test.api.F", "c.Mine(/*own*/", "test.api.Copy.Mine"));
    ASSERT(go_selects(source, "test.api.F", "c.Own(/*not-inherited*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "o.Inner.Val(/*embedded*/", "test.api.In.Val"));
    ASSERT(go_selects(source, "test.api.F", "o.Own(/*embedded-not-inherited*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "r.Get(/*remote*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "b.Get(/*bare*/", NULL));
    ASSERT(go_selects(source, "test.api.F", "h.Get(/*holder*/", NULL));
    /* `type Level int` has nothing to inherit, so promotion through it holds. error has a method. */
    ASSERT(go_selects(source, "test.api.F", "k.Type(/*universe*/", "test.api.Level.Type"));
    ASSERT(go_selects(source, "test.api.F", "e.Val(/*error*/", NULL));
    PASS();
}

TEST(resolver_stabilization_go_generic_declarations_keep_their_method_sets) {
    const char *source = "package api\n"
                         "type T struct{}\nfunc (T) M() {}\n"
                         "type Box[E any] struct{ V E }\n"
                         "func (b *Box[E]) Get() E { return b.V }\n"
                         "func NewBox[E any]() *Box[E] { return nil }\n"
                         "func F() {\n"
                         "  var c Box[int]\n"
                         "  c.Get(/*annotated*/)\n"
                         "  d := NewBox[int]()\n"
                         "  d.Get(/*returned*/)\n"
                         "  c.V.M(/*parameter-field*/)\n"
                         "}\n";
    ASSERT(go_selects(source, "test.api.F", "c.Get(/*annotated*/", "test.api.Box.Get"));
    ASSERT(go_selects(source, "test.api.F", "d.Get(/*returned*/", "test.api.Box.Get"));
    ASSERT(go_selects(source, "test.api.F", "c.V.M(/*parameter-field*/", NULL));
    PASS();
}

/* Build-tag twins are one graph node, so one selection whichever file a build compiles. Their
 * results are a separate question: only agreeing twins type a value. */
TEST(resolver_stabilization_go_build_tag_twins_are_one_target_but_results_must_agree) {
    const char *linux_file = "package api\n"
                             "type A struct{}\nfunc (A) M() {}\n"
                             "type B struct{}\nfunc (B) M() {}\n"
                             "func work() A { return A{} }\n"
                             "func same() A { return A{} }\n";
    const char *windows_file = "package api\n"
                               "func work() B { return B{} }\n"
                               "func same() A { return A{} }\n";
    const char *use = "package api\n"
                      "func F() {\n"
                      "  work(/*twin*/)\n"
                      "  work(/*differ*/).M()\n"
                      "  same(/*agree*/).M()\n"
                      "}\n";
    stabilization_t f;
    initialize(&f, (cbm_resolve_solver_limits_t){0});
    cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
    ASSERT_NOT_NULL(state);
    ASSERT(collect_fixture(state, "api/a_linux.go", "test.api", linux_file, NULL, NULL));
    ASSERT(collect_fixture(state, "api/a_windows.go", "test.api", windows_file, NULL, NULL));
    ASSERT(collect_fixture(state, "api/use.go", "test.api", use, NULL, NULL));
    ASSERT(cbm_go_stabilization_register(state));
    ASSERT(cbm_resolve_solver_solve(f.solver));
    ASSERT(cbm_go_stabilization_seal(state));
    cbm_resolve_result_t result;
    ASSERT(fixture_resolution(state, "api/use.go", use, "test.api.F", "work(/*twin*/", &result));
    ASSERT(cbm_resolve_admit(&result));
    ASSERT_STR_EQ(result.target, "test.api.work");
    ASSERT(fixture_resolution(state, "api/use.go", use, "test.api.F", "work(/*differ*/).M",
                              &result));
    ASSERT(!cbm_resolve_admit(&result));
    ASSERT(fixture_resolution(state, "api/use.go", use, "test.api.F", "same(/*agree*/).M", &result));
    ASSERT(cbm_resolve_admit(&result));
    ASSERT_STR_EQ(result.target, "test.api.A.M");
    cbm_go_stabilization_dispose(state);
    cbm_arena_destroy(&f.arena);
    PASS();
}

/* Imported package variables and the qualified forms of the rules above. */
TEST(resolver_stabilization_go_imported_members_keep_their_package_identity) {
    const char *library = "package lib\n"
                          "type Impl struct{ Inner }\nfunc (i *Impl) Close() {}\n"
                          "type Inner struct{ hidden *Impl }\nfunc (i Inner) Touch() {}\n"
                          "var Default = &Impl{}\n"
                          "var private = &Impl{}\n";
    const char *use = "package app\nimport \"example/lib\"\n"
                      "type holder struct{ lib.Inner }\n"
                      "func F(h holder) {\n"
                      "  lib.Default.Close(/*var*/)\n"
                      "  lib.Default.Touch(/*promoted*/)\n"
                      "  h.Touch(/*embedded-import*/)\n"
                      "  lib.private.Close(/*private-var*/)\n"
                      "  h.hidden.Close(/*private-field*/)\n"
                      "}\n";
    stabilization_t f;
    initialize(&f, (cbm_resolve_solver_limits_t){0});
    cbm_go_stabilization_t *state = cbm_go_stabilization_new(&f.arena, &f.proofs, f.solver);
    ASSERT_NOT_NULL(state);
    ASSERT(collect_fixture(state, "lib/lib.go", "test.lib", library, NULL, NULL));
    ASSERT(collect_fixture(state, "app/use.go", "test.app", use, "lib", "test.lib"));
    ASSERT(cbm_go_stabilization_register(state));
    ASSERT(cbm_resolve_solver_solve(f.solver));
    ASSERT(cbm_go_stabilization_seal(state));
    cbm_resolve_result_t result;
    ASSERT(fixture_resolution(state, "app/use.go", use, "test.app.F", "lib.Default.Close(/*var*/",
                              &result));
    ASSERT(cbm_resolve_admit(&result));
    ASSERT_STR_EQ(result.target, "test.lib.Impl.Close");
    ASSERT(fixture_resolution(state, "app/use.go", use, "test.app.F",
                              "lib.Default.Touch(/*promoted*/", &result));
    ASSERT(cbm_resolve_admit(&result));
    ASSERT_STR_EQ(result.target, "test.lib.Inner.Touch");
    ASSERT(fixture_resolution(state, "app/use.go", use, "test.app.F", "h.Touch(/*embedded-import*/",
                              &result));
    ASSERT(cbm_resolve_admit(&result));
    ASSERT_STR_EQ(result.target, "test.lib.Inner.Touch");
    /* Lower-case names do not cross a package boundary, so neither may bind. */
    ASSERT(fixture_resolution(state, "app/use.go", use, "test.app.F",
                              "h.hidden.Close(/*private-field*/", &result));
    ASSERT(!cbm_resolve_admit(&result));
    cbm_go_stabilization_dispose(state);
    cbm_arena_destroy(&f.arena);
    PASS();
}

/* Go 1.26's new(40) is a parse error to this grammar: one literal in one test table used to
 * take every function in its file out of the fixed point. */
TEST(resolver_stabilization_go_syntax_error_costs_only_its_own_declaration) {
    const char *source = "package api\n"
                         "type T struct{}\nfunc (T) M() {}\n"
                         "func Broken() {\n  x := (\n  x.M()\n}\n"
                         "func Good(t T) { t.M() }\n";
    ASSERT(go_selects(source, "test.api.Good", "t.M()", "test.api.T.M"));
    PASS();
}

static int go_import_admitted(const char *root, const char *source, const char *rel,
                              const char *folder_qn, const char *folder_path, const char *import,
                              const char *target) {
    CBMFileResult *parsed =
        cbm_extract_file(source, (int)strlen(source), CBM_LANG_GO, "test", rel, 0, NULL, NULL);
    cbm_gbuf_t *graph = cbm_gbuf_new("test", root);
    if (!parsed || !graph)
        return -1;
    char *file_qn = cbm_pipeline_fqn_compute("test", rel, "__file__");
    cbm_gbuf_upsert_node(graph, "File", rel, file_qn, rel, 1, 3, "{}");
    cbm_gbuf_upsert_node(graph, "Folder", folder_path, folder_qn, folder_path, 0, 0, "{}");
    CBMHashTable *map = cbm_ht_create(16);
    cbm_ht_set(map, import, (void *)target);
    CBMHashTable *saved = cbm_pipeline_get_pkgmap();
    cbm_pipeline_set_pkgmap(map);
    cbm_pipeline_ctx_t pipeline = {.project_name = "test", .repo_path = root, .gbuf = graph};
    bool captured = cbm_pipeline_capture_go_imports(&pipeline, rel, parsed);
    cbm_pipeline_set_pkgmap(saved);
    const cbm_resolve_import_snapshot_t *imports = captured ? cbm_gbuf_imports(graph, rel) : NULL;
    int admitted = -1;
    if (imports && imports->count == 1) {
        admitted = cbm_resolve_admit(&imports->items[0].dependency) ? 1 : 0;
        if (admitted && strcmp(imports->items[0].dependency.target, folder_qn))
            admitted = -2;
    }
    cbm_ht_free(map);
    cbm_gbuf_free(graph);
    cbm_free_result(parsed);
    free(file_qn);
    return admitted;
}

/* kubernetes keeps its modules in one tree and binds them with replace directives. A module's
 * own go.mod names exactly one directory for that path, so the import is proved. Another
 * directory that merely declares the same module path, or an import with no directive, comes
 * from the module cache and stays unproven. */
TEST(resolver_stabilization_go_replace_directive_proves_a_sibling_module) {
    const char *root = th_mktempdir("cbm_go_replace");
    ASSERT_NOT_NULL(root);
    ASSERT(cbm_mkdir_p(TH_PATH(root, "staging/client/rest"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(root, "staging/api/core"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(root, "staging/other"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(root, "decoy/api/core"), 0755));
    ASSERT_EQ(th_write_file(TH_PATH(root, "staging/client/go.mod"),
                            "module k8s.io/client\n\ngo 1.22\n\n"
                            "require k8s.io/api v0.0.0\n\n"
                            "replace (\n"
                            "\tk8s.io/api => ../api\n"
                            "\tk8s.io/versioned v1.2.3 => ../versioned // pinned\n"
                            "\tk8s.io/remote => github.com/fork/remote v1.0.0\n"
                            ")\n"
                            "replace k8s.io/single => ../other\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(root, "staging/api/go.mod"), "module k8s.io/api\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(root, "staging/other/go.mod"), "module k8s.io/single\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(root, "decoy/api/go.mod"), "module k8s.io/api\n"), 0);
    const char *source = "package rest\nimport \"k8s.io/api/core\"\nfunc Run(){core.Make()}\n";
    const char *rel = "staging/client/rest/main.go";
    ASSERT_EQ(th_write_file(TH_PATH(root, rel), source), 0);
    ASSERT_EQ(go_import_admitted(root, source, rel, "test.staging.api.core", "staging/api/core",
                                 "k8s.io/api/core", "test.staging.api.core"),
              1);
    /* The directive names staging/api; a decoy module of the same name is not that directory. */
    ASSERT_EQ(go_import_admitted(root, source, rel, "test.decoy.api.core", "decoy/api/core",
                                 "k8s.io/api/core", "test.decoy.api.core"),
              0);
    const char *single = "package rest\nimport \"k8s.io/single\"\nfunc Run(){single.Make()}\n";
    ASSERT_EQ(go_import_admitted(root, single, rel, "test.staging.other", "staging/other",
                                 "k8s.io/single", "test.staging.other"),
              1);
    /* A replacement that names another module's source is that module, not a local path. */
    const char *remote = "package rest\nimport \"k8s.io/remote\"\nfunc Run(){remote.Make()}\n";
    ASSERT_EQ(go_import_admitted(root, remote, rel, "test.staging.other", "staging/other",
                                 "k8s.io/remote", "test.staging.other"),
              0);
    th_rmtree(root);
    PASS();
}

TEST(resolver_stabilization_go_workspace_use_proves_a_listed_module) {
    const char *root = th_mktempdir("cbm_go_workspace");
    ASSERT_NOT_NULL(root);
    ASSERT(cbm_mkdir_p(TH_PATH(root, "app/cmd"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(root, "lib/util"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(root, "unlisted/util"), 0755));
    ASSERT_EQ(th_write_file(TH_PATH(root, "go.work"),
                            "go 1.22\n\nuse (\n\t./app\n\t./lib // shared\n)\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(root, "app/go.mod"), "module example/app\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(root, "lib/go.mod"), "module example/lib\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(root, "unlisted/go.mod"), "module example/unlisted\n"), 0);
    const char *rel = "app/cmd/main.go";
    const char *listed = "package cmd\nimport \"example/lib/util\"\nfunc Run(){util.Make()}\n";
    ASSERT_EQ(th_write_file(TH_PATH(root, rel), listed), 0);
    ASSERT_EQ(go_import_admitted(root, listed, rel, "test.lib.util", "lib/util", "example/lib/util",
                                 "test.lib.util"),
              1);
    const char *unlisted =
        "package cmd\nimport \"example/unlisted/util\"\nfunc Run(){util.Make()}\n";
    ASSERT_EQ(go_import_admitted(root, unlisted, rel, "test.unlisted.util", "unlisted/util",
                                 "example/unlisted/util", "test.unlisted.util"),
              0);
    th_rmtree(root);
    PASS();
}

static int count_edge_between(sqlite3 *db, const char *relation, const char *source,
                              const char *target) {
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
             "JOIN nodes t ON t.id=e.target_id WHERE e.type='%s' AND s.qualified_name='%s' "
             "AND t.qualified_name='%s'",
             relation, source, target);
    return scalar(db, sql);
}

/* Build-tag twins through the real pipeline, both routes: an occurrence in either twin belongs
 * to the one node (graph_buffer.c: "edges are the union of every variant's body"), and a
 * package-level initializer belongs to its file, since the package's QN is a shared Folder. */
static int go_twins_case(bool parallel) {
    const char *root = th_mktempdir("cbm_go_twins");
    ASSERT_NOT_NULL(root);
    char repo[1024], database[1024];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "pkg"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "other"), 0755));
    ASSERT_EQ(th_write_file(TH_PATH(repo, "go.mod"), "module example\ngo 1.22\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "pkg/a_linux.go"),
                            "package pkg\n\nfunc forLinux() int { return 1 }\n\n"
                            "func work() int { return forLinux() }\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "pkg/a_windows.go"),
                            "package pkg\n\nfunc forWindows() int { return 2 }\n\n"
                            "func work() int { return forWindows() }\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "pkg/use.go"),
                            "package pkg\n\nvar Level = work()\n\n"
                            "func Use() int { return work() }\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "other/other.go"),
                            "package other\n\nfunc work() int { return 3 }\n"),
              0);
    for (int i = 0; parallel && i < 52; i++) {
        char name[64], source[96];
        snprintf(name, sizeof(name), "padding_%02d.py", i);
        snprintf(source, sizeof(source), "def padding_%02d():\n    return %d\n", i, i);
        ASSERT_EQ(th_write_file(TH_PATH(repo, name), source), 0);
    }
    ASSERT_EQ(index_fixture(repo, database, parallel), 0);
    cbm_store_t *store = cbm_store_open_path_existing(database);
    ASSERT_NOT_NULL(store);
    sqlite3 *db = cbm_store_get_db(store);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.Use", "test.pkg.work"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.work", "test.pkg.forLinux"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.work", "test.pkg.forWindows"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.use.go.__file__", "test.pkg.work"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.Use", "test.other.work"), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg", "test.pkg.work"), 0);
    cbm_store_close(store);
    th_rmtree(root);
    PASS();
}
static int go_references_case(bool parallel) {
    const char *root = th_mktempdir("cbm_go_references");
    ASSERT_NOT_NULL(root);
    char repo[1024], database[1024];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "lib"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "app"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "mount-utils"), 0755));
    ASSERT_EQ(th_write_file(TH_PATH(repo, "go.mod"), "module example\ngo 1.22\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "lib/lib.go"),
                            "package lib\n\n"
                            "type Impl struct {\n\tName string\n\tcount int\n}\n\n"
                            "type Getter interface {\n\tGet() string\n}\n\n"
                            "var Default = &Impl{}\n\n"
                            "const Limit = 3\n\n"
                            "func Helper() int { return Limit }\n"),
              0);
    /* The directory is mount-utils; the package it declares is mount. */
    ASSERT_EQ(th_write_file(TH_PATH(repo, "mount-utils/mount.go"),
                            "package mount\n\n"
                            "type Mounter struct{}\n\n"
                            "func New() *Mounter { return nil }\n\n"
                            "func (m *Mounter) Run() {}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "app/app.go"),
                            "package app\n\n"
                            "import (\n\t\"example/lib\"\n\t\"example/mount-utils\"\n)\n\n"
                            "var Limit = 9\n\nvar Name = \"package\"\n\n"
                            "func Reads(i *lib.Impl) int {\n"
                            "\t_ = i.Name\n"
                            "\treturn lib.Limit\n"
                            "}\n\n"
                            "func Writes(i *lib.Impl) {\n"
                            "\ti.Name = \"x\"\n"
                            "\tName = \"y\"\n"
                            "}\n\n"
                            "func Types(g []lib.Getter, m map[string]*lib.Impl) *lib.Impl {\n"
                            "\treturn nil\n"
                            "}\n\n"
                            "func Shadowed(Limit int) int {\n"
                            "\treturn Limit\n"
                            "}\n\n"
                            "func LocalWrite() {\n"
                            "\tName := 1\n"
                            "\tName = 2\n"
                            "\t_ = Name\n"
                            "}\n\n"
                            "func Literal() lib.Impl {\n"
                            "\treturn lib.Impl{Name: \"key\"}\n"
                            "}\n\n"
                            "func MapLiteral() map[string]int {\n"
                            "\treturn map[string]int{Name: 1}\n"
                            "}\n\n"
                            "var Getter = 1\n\n"
                            "func Result() *lib.Getter {\n"
                            "\t_ = Getter\n"
                            "\treturn nil\n"
                            "}\n\n"
                            "func ResultParam(Getter int) *lib.Getter {\n"
                            "\t_ = Getter\n"
                            "\treturn nil\n"
                            "}\n\n"
                            "type Opts struct {\n\tMake func() *lib.Impl\n}\n\n"
                            "func Fields(o Opts) string {\n"
                            "\treturn o.Make().Name\n"
                            "}\n\n"
                            "func Declared() {\n"
                            "\tmount.New().Run()\n"
                            "}\n"),
              0);
    for (int i = 0; parallel && i < 52; i++) {
        char name[64], source[96];
        snprintf(name, sizeof(name), "padding_%02d.py", i);
        snprintf(source, sizeof(source), "def padding_%02d():\n    return %d\n", i, i);
        ASSERT_EQ(th_write_file(TH_PATH(repo, name), source), 0);
    }
    ASSERT_EQ(index_fixture(repo, database, parallel), 0);
    cbm_store_t *store = cbm_store_open_path_existing(database);
    ASSERT_NOT_NULL(store);
    sqlite3 *db = cbm_store_get_db(store);
    /* A field through a declared type, a qualified constant, and not the namesakes. */
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.Reads", "test.lib.Impl.Name"), 1);
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.Reads", "test.lib.Limit"), 1);
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.Reads", "test.app.Limit"), 0);
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.Reads", "test.app.Name"), 0);
    ASSERT_EQ(count_edge_between(db, "WRITES", "test.app.Writes", "test.lib.Impl.Name"), 1);
    ASSERT_EQ(count_edge_between(db, "WRITES", "test.app.Writes", "test.app.Name"), 1);
    /* Every named type inside the wrappers, under the declaring function. */
    ASSERT_EQ(count_edge_between(db, "USES_TYPE", "test.app.Types", "test.lib.Getter"), 1);
    ASSERT_EQ(count_edge_between(db, "USES_TYPE", "test.app.Types", "test.lib.Impl"), 1);
    /* A parameter and a local hide the package-level names. */
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.Shadowed", "test.app.Limit"), 0);
    ASSERT_EQ(count_edge_between(db, "WRITES", "test.app.LocalWrite", "test.app.Name"), 0);
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.LocalWrite", "test.app.Name"), 0);
    /* A struct-literal key is the literal type's field, never the package variable it is
     * spelled like. A map literal's bare key is an ordinary value. */
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.Literal", "test.lib.Impl.Name"), 1);
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.Literal", "test.app.Name"), 0);
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.MapLiteral", "test.app.Name"), 1);
    /* A result `*lib.Getter` selects lib's member and declares no local Getter, so the body's
     * name is still the package variable. A real parameter does hide it. */
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.Result", "test.lib.Getter"), 1);
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.Result", "test.app.Getter"), 1);
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.ResultParam", "test.app.Getter"), 0);
    /* o.Make() selects the field, which gives the chain its type but is no callable. */
    ASSERT_EQ(count_edge_between(db, "USAGE", "test.app.Fields", "test.lib.Impl.Name"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Fields", "test.app.Opts.Make"), 0);
    /* The import binds mount, the package's declared name, though the path says mount-utils. */
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Declared", "test.mount-utils.New"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Declared", "test.mount-utils.Mounter.Run"),
              1);
    cbm_store_close(store);
    th_rmtree(root);
    PASS();
}
/* Overloads stay live, so one node is exact for any of them. A second Python def rebinds the
 * name, so the bodies are not equivalent and that node selects nothing. */
static int declarations_case(bool parallel) {
    const char *root = th_mktempdir("cbm_declarations");
    ASSERT_NOT_NULL(root);
    char repo[1024], database[1024];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    ASSERT(cbm_mkdir_p(repo, 0755));
    ASSERT_EQ(th_write_file(TH_PATH(repo, "A.java"),
                            "class A {\n"
                            "    static void helper() {}\n"
                            "    void f(int x) { helper(); }\n"
                            "    void f(String y) { helper(); }\n"
                            "}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "rebind.py"),
                            "def alpha():\n    pass\n\n"
                            "def beta():\n    pass\n\n"
                            "def pick():\n    alpha()\n\n"
                            "def pick():\n    beta()\n\n"
                            "def caller():\n    pick()\n"),
              0);
    for (int i = 0; parallel && i < 52; i++) {
        char name[64], source[96];
        snprintf(name, sizeof(name), "padding_%02d.py", i);
        snprintf(source, sizeof(source), "def padding_%02d():\n    return %d\n", i, i);
        ASSERT_EQ(th_write_file(TH_PATH(repo, name), source), 0);
    }
    ASSERT_EQ(index_fixture(repo, database, parallel), 0);
    cbm_store_t *store = cbm_store_open_path_existing(database);
    ASSERT_NOT_NULL(store);
    sqlite3 *db = cbm_store_get_db(store);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.A.f", "test.A.helper"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.rebind.pick", "test.rebind.alpha"), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.rebind.pick", "test.rebind.beta"), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.rebind.caller", "test.rebind.pick"), 0);
    cbm_store_close(store);
    th_rmtree(root);
    PASS();
}
TEST(resolver_stabilization_overloads_share_an_identity_and_rebinding_does_not_sequential) {
    return declarations_case(false);
}
TEST(resolver_stabilization_overloads_share_an_identity_and_rebinding_does_not_parallel) {
    return declarations_case(true);
}
/* cli/cli keeps a CodeQL fixture whose go.mod declares the repository's own module path. Two
 * directories then claim that path, but an import still means one of them: the importing
 * package's own module, or the directory its go.mod's replace directive names. A module with
 * neither takes that path from the module cache, so nothing in this tree is the target. */
static int go_duplicate_module_case(bool parallel) {
    const char *root = th_mktempdir("cbm_go_dup_module");
    ASSERT_NOT_NULL(root);
    char repo[1024], database[1024];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "pkg/mock"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "api"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "fixture/pkg/mock"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "fixture/app"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "foreign"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "sibling"), 0755));
    ASSERT_EQ(th_write_file(TH_PATH(repo, "go.mod"), "module example/cli/v2\ngo 1.22\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "fixture/go.mod"), "module example/cli/v2\ngo 1.22\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "foreign/go.mod"), "module example/foreign\ngo 1.22\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "sibling/go.mod"),
                            "module example/sibling\ngo 1.22\n\n"
                            "replace example/cli/v2 => ../\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "pkg/mock/mock.go"),
                            "package mock\n\ntype Reg struct{}\n\n"
                            "func New() *Reg { return nil }\n\n"
                            "func (r *Reg) Verify() {}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "fixture/pkg/mock/mock.go"),
                            "package mock\n\nfunc New() int { return 0 }\n"),
              0);
    const char *import = "import \"example/cli/v2/pkg/mock\"\n\n";
    char body[256];
    snprintf(body, sizeof(body), "package api\n\n%sfunc Run() {\n\tr := mock.New()\n\tr.Verify()\n}\n",
             import);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "api/client.go"), body), 0);
    snprintf(body, sizeof(body), "package app\n\n%sfunc Own() int { return mock.New() }\n", import);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "fixture/app/app.go"), body), 0);
    snprintf(body, sizeof(body), "package foreign\n\n%sfunc Cached() { mock.New() }\n", import);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "foreign/use.go"), body), 0);
    snprintf(body, sizeof(body), "package sibling\n\n%sfunc Replaced() { mock.New() }\n", import);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "sibling/use.go"), body), 0);
    for (int i = 0; parallel && i < 52; i++) {
        char name[64], source[96];
        snprintf(name, sizeof(name), "padding_%02d.py", i);
        snprintf(source, sizeof(source), "def padding_%02d():\n    return %d\n", i, i);
        ASSERT_EQ(th_write_file(TH_PATH(repo, name), source), 0);
    }
    ASSERT_EQ(index_fixture(repo, database, parallel), 0);
    cbm_store_t *store = cbm_store_open_path_existing(database);
    ASSERT_NOT_NULL(store);
    sqlite3 *db = cbm_store_get_db(store);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.api.Run", "test.pkg.mock.New"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.api.Run", "test.pkg.mock.Reg.Verify"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.api.Run", "test.fixture.pkg.mock.New"), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.fixture.app.Own", "test.fixture.pkg.mock.New"),
              1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.fixture.app.Own", "test.pkg.mock.New"), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.foreign.Cached", "test.pkg.mock.New"), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.foreign.Cached", "test.fixture.pkg.mock.New"),
              0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.sibling.Replaced", "test.pkg.mock.New"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.sibling.Replaced", "test.fixture.pkg.mock.New"),
              0);
    cbm_store_close(store);
    th_rmtree(root);
    PASS();
}
/* Go 1.26's new(expr) is an ERROR to the grammar, which used to cost the declaration around it.
 * A decoy method and a function that merely end in new stay what they are. */
static int go_new_expression_case(bool parallel) {
    const char *root = th_mktempdir("cbm_go_new");
    ASSERT_NOT_NULL(root);
    char repo[1024], database[1024];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "pkg"), 0755));
    ASSERT_EQ(th_write_file(TH_PATH(repo, "go.mod"), "module example\ngo 1.26\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "pkg/lib.go"),
                            "package pkg\n\n"
                            "type T struct{}\n\n"
                            "func (t *T) M() {}\n\n"
                            "func Make() T { return T{} }\n\n"
                            "func helper() {}\n\n"
                            "func renew(x int) {}\n\n"
                            "type F struct{}\n\n"
                            "func (f F) new(x int) {}\n\n"
                            "func Literal() { x := new(\"text\"); _ = x; helper() }\n\n"
                            "func Number() { _ = new(48 * 2); helper() }\n\n"
                            "func Conversion(n int) { _ = new(uint64(n)); helper() }\n\n"
                            "func CallResult() { new(Make()).M(); helper() }\n\n"
                            "func Composite() { new(T{}).M() }\n\n"
                            "func TypeArgument() { new(T).M() }\n\n"
                            "func Others(f F) { renew(1); f.new(2) }\n"),
              0);
    for (int i = 0; parallel && i < 52; i++) {
        char name[64], source[96];
        snprintf(name, sizeof(name), "padding_%02d.py", i);
        snprintf(source, sizeof(source), "def padding_%02d():\n    return %d\n", i, i);
        ASSERT_EQ(th_write_file(TH_PATH(repo, name), source), 0);
    }
    ASSERT_EQ(index_fixture(repo, database, parallel), 0);
    cbm_store_t *store = cbm_store_open_path_existing(database);
    ASSERT_NOT_NULL(store);
    sqlite3 *db = cbm_store_get_db(store);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.Literal", "test.pkg.helper"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.Number", "test.pkg.helper"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.Conversion", "test.pkg.helper"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.CallResult", "test.pkg.helper"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.CallResult", "test.pkg.Make"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.CallResult", "test.pkg.T.M"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.Composite", "test.pkg.T.M"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.TypeArgument", "test.pkg.T.M"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.Others", "test.pkg.renew"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.pkg.Others", "test.pkg.F.new"), 1);
    cbm_store_close(store);
    th_rmtree(root);
    PASS();
}
TEST(resolver_stabilization_go_new_takes_an_expression_sequential) {
    return go_new_expression_case(false);
}
TEST(resolver_stabilization_go_new_takes_an_expression_parallel) {
    return go_new_expression_case(true);
}
/* A table test's []struct{...} and a field's []struct{...} are types written where they stand. A
 * field holding a function with two results selects its declared results by position, and T(x)
 * converts: the site uses the type and nothing is called, though T's own method still is. */
static int go_anonymous_struct_case(bool parallel) {
    const char *root = th_mktempdir("cbm_go_anon");
    ASSERT_NOT_NULL(root);
    char repo[1024], database[1024];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "lib"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "app"), 0755));
    ASSERT_EQ(th_write_file(TH_PATH(repo, "go.mod"), "module example\ngo 1.22\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "lib/lib.go"),
                            "package lib\n\n"
                            "type Reg struct{}\n\n"
                            "func (r *Reg) Verify() {}\n\n"
                            "type Level string\n\n"
                            "func (l Level) Name() string { return string(l) }\n\n"
                            "type Options struct {\n"
                            "\tMake  func() (*Reg, error)\n"
                            "\tOne   func() *Reg\n"
                            "\tNodes []struct{ Inner *Reg }\n"
                            "}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "app/app.go"),
                            "package app\n\n"
                            "import (\n\t\"example/lib\"\n\t\"example/external\"\n)\n\n"
                            "type other struct{}\n\n"
                            "func (o *other) Verify() {}\n\n"
                            "func Table() {\n"
                            "\tcases := []struct {\n\t\tname  string\n\t\tin    *lib.Reg\n"
                            "\t\tlevel lib.Level\n\t}{{name: \"a\"}}\n"
                            "\tfor _, tt := range cases {\n\t\ttt.in.Verify()\n"
                            "\t\ttt.level.Name()\n\t}\n}\n\n"
                            "func Decoy() {\n"
                            "\tcases := []struct{ in *other }{}\n"
                            "\tfor _, tt := range cases {\n\t\ttt.in.Verify()\n\t}\n}\n\n"
                            "func Foreign() {\n"
                            "\tc := struct{ in *external.T }{}\n"
                            "\tc.in.Verify()\n}\n\n"
                            "func Nested(o lib.Options) {\n"
                            "\tfor _, n := range o.Nodes {\n\t\tn.Inner.Verify()\n\t}\n}\n\n"
                            "func Tuple(o lib.Options) {\n"
                            "\tr, err := o.Make()\n\t_ = err\n\tr.Verify()\n}\n\n"
                            "func Second(o lib.Options) {\n"
                            "\t_, r := o.Make()\n\tr.Verify()\n}\n\n"
                            "func Single(o lib.Options) {\n\to.One().Verify()\n}\n\n"
                            "func Convert(s string) lib.Level {\n\treturn lib.Level(s)\n}\n\n"
                            "func Chain(s string) string {\n\treturn lib.Level(s).Name()\n}\n\n"
                            "func pair() (*lib.Reg, error) {\n\treturn nil, nil\n}\n\n"
                            "var global, _ = pair()\n\n"
                            "func VarTuple() {\n\tvar (\n\t\tr, _ = pair()\n\t)\n\tr.Verify()\n}\n\n"
                            "func GlobalTuple() {\n\tglobal.Verify()\n}\n\n"
                            "func maker() func() *lib.Reg {\n\treturn nil\n}\n\n"
                            "func ViaLocal() {\n\tmk := maker()\n\tmk()\n}\n"),
              0);
    for (int i = 0; parallel && i < 52; i++) {
        char name[64], source[96];
        snprintf(name, sizeof(name), "padding_%02d.py", i);
        snprintf(source, sizeof(source), "def padding_%02d():\n    return %d\n", i, i);
        ASSERT_EQ(th_write_file(TH_PATH(repo, name), source), 0);
    }
    ASSERT_EQ(index_fixture(repo, database, parallel), 0);
    cbm_store_t *store = cbm_store_open_path_existing(database);
    ASSERT_NOT_NULL(store);
    sqlite3 *db = cbm_store_get_db(store);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Table", "test.lib.Reg.Verify"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Table", "test.lib.Level.Name"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Decoy", "test.app.other.Verify"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Decoy", "test.lib.Reg.Verify"), 0);
    ASSERT_EQ(scalar(db, "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                         "WHERE e.type='CALLS' AND s.qualified_name='test.app.Foreign'"),
              0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Nested", "test.lib.Reg.Verify"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Tuple", "test.lib.Reg.Verify"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Second", "test.lib.Reg.Verify"), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Single", "test.lib.Reg.Verify"), 1);
    ASSERT_EQ(count_edge_between(db, "USES_TYPE", "test.app.Convert", "test.lib.Level"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Convert", "test.lib.Level"), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Chain", "test.lib.Level.Name"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Chain", "test.lib.Level"), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.VarTuple", "test.lib.Reg.Verify"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.GlobalTuple", "test.lib.Reg.Verify"), 1);
    /* Calling a local function value names no type, whatever it returns. */
    ASSERT_EQ(count_edge_between(db, "USES_TYPE", "test.app.ViaLocal", "test.lib.Reg"), 0);
    cbm_store_close(store);
    th_rmtree(root);
    PASS();
}
/* What a function value yields, by position, whatever declared it; a const list's implicit
 * repetition; a local type; a variadic; two imports whose paths end alike; and the standard
 * library generics whose results the call spells, only when the file really imports them. */
static int go_function_values_case(bool parallel) {
    const char *root = th_mktempdir("cbm_go_fnvalues");
    ASSERT_NOT_NULL(root);
    char repo[1024], database[1024];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "lib"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "app"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "repo/v1"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "fake"), 0755));
    ASSERT_EQ(th_write_file(TH_PATH(repo, "go.mod"), "module example\ngo 1.22\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "repo/v1/repo.go"),
                            "package repo\n\nfunc Find() {}\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "fake/fake.go"),
                            "package fake\n\n"
                            "type Decoy struct{}\n\nfunc (d *Decoy) Verify() {}\n\n"
                            "func AsType[T any](v any) (*Decoy, bool) { return nil, false }\n\n"
                            "func Backward(v any) []*Decoy { return nil }\n"),
              0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "lib/lib.go"),
                            "package lib\n\n"
                            "type Reg struct{}\n\nfunc (r *Reg) Verify() {}\n\n"
                            "type Level int\n\n"
                            "const (\n\tLow Level = iota\n\tHigh\n\tTop\n)\n\n"
                            "func (l Level) Name() string { return \"\" }\n\n"
                            "type Fn func() (*Reg, error)\n\n"
                            "type One func() *Reg\n\n"
                            "type Cap struct{}\n\nfunc (c *Cap) Copy() *Cap { return c }\n\n"
                            "var Default = func() *Cap { return &Cap{} }()\n"),
              0);
    ASSERT_EQ(
        th_write_file(TH_PATH(repo, "app/app.go"),
                      "package app\n\n"
                      "import (\n\t\"errors\"\n\t\"slices\"\n\n"
                      "\t\"example/lib\"\n\t\"example/repo/v1\"\n\tv1 \"k8s.io/api/core/v1\"\n)\n\n"
                      "type other struct{}\n\nfunc (o *other) Verify() {}\n\n"
                      "func Enum() string { return lib.High.Name() }\n\n"
                      "func Literal() { lib.Default.Copy() }\n\n"
                      "func Closure() {\n\tmk := func() (*lib.Reg, error) { return nil, nil }\n"
                      "\tr, _ := mk()\n\tr.Verify()\n}\n\n"
                      "func First() {\n\tmk := func() (*other, *lib.Reg) { return nil, nil }\n"
                      "\tr, _ := mk()\n\tr.Verify()\n}\n\n"
                      "func Last() {\n\tmk := func() (*other, *lib.Reg) { return nil, nil }\n"
                      "\t_, s := mk()\n\ts.Verify()\n}\n\n"
                      "func Param(mk func() (*lib.Reg, error)) {\n\tr, _ := mk()\n\tr.Verify()\n}\n\n"
                      "func Named(mk lib.Fn) {\n\tr, _ := mk()\n\tr.Verify()\n}\n\n"
                      "func NamedOne(mk lib.One) {\n\tmk().Verify()\n}\n\n"
                      "func Reused(mk lib.Fn) {\n\tmk, err := mk()\n\t_ = err\n\tmk.Verify()\n}\n\n"
                      "func Table() {\n\tfor _, tt := range []struct {\n"
                      "\t\tmake func() (*lib.Reg, error)\n\t}{} {\n"
                      "\t\tr, _ := tt.make()\n\t\tr.Verify()\n\t}\n}\n\n"
                      "func TableOne() {\n\tfor _, tt := range []struct{ one func() *lib.Reg }{} {\n"
                      "\t\ttt.one().Verify()\n\t}\n}\n\n"
                      "func Local() {\n\ttype holder struct{ reg *lib.Reg }\n"
                      "\tcases := []holder{}\n\tfor _, c := range cases {\n\t\tc.reg.Verify()\n\t}\n}\n\n"
                      "func LocalShadow() {\n\ttype Reg struct{ inner *other }\n\tvar r Reg\n"
                      "\tr.inner.Verify()\n}\n\n"
                      "func Renamed(x *lib.Reg) {\n\ttype mine lib.Reg\n\tm := mine(*x)\n"
                      "\tm.Verify()\n}\n\n"
                      "func Many(rs ...lib.Reg) {\n\tfor _, r := range rs {\n\t\tr.Verify()\n\t}\n}\n\n"
                      "func Immediately() {\n\tr, _ := func() (*lib.Reg, error) { return nil, nil }()\n"
                      "\tr.Verify()\n}\n\n"
                      "func ImmediatelyOne() {\n\tfunc() *lib.Reg { return nil }().Verify()\n}\n\n"
                      "func Paths() {\n\trepo.Find()\n\tv1.Find()\n}\n\n"
                      "func Received() {\n\tch := make(chan *lib.Reg)\n"
                      "\tfor r := range ch {\n\t\tr.Verify()\n\t}\n}\n\n"
                      "func ReceivedParam(ch <-chan *lib.Reg) {\n"
                      "\tfor r := range ch {\n\t\tr.Verify()\n\t}\n}\n\n"
                      "func Indexes(xs []*lib.Reg) {\n\tfor r := range xs {\n\t\tr.Verify()\n\t}\n}\n\n"
                      "func Cast(err error) {\n\tif r, ok := errors.AsType[*lib.Reg](err); ok {\n"
                      "\t\tr.Verify()\n\t}\n}\n\n"
                      "func Backward(xs []*lib.Reg) {\n\tfor _, r := range slices.Backward(xs) {\n"
                      "\t\tr.Verify()\n\t}\n}\n\n"
                      "func Cloned(xs []*lib.Reg) {\n\tys := slices.Clone(xs)\n"
                      "\tfor _, r := range ys {\n\t\tr.Verify()\n\t}\n}\n"),
        0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "app/decoy.go"),
                            "package app\n\n"
                            "import (\n\terrors \"example/fake\"\n\tslices \"example/fake\"\n\n"
                            "\t\"example/lib\"\n)\n\n"
                            "func Imposter(err error) {\n"
                            "\tif r, ok := errors.AsType[*lib.Reg](err); ok {\n\t\tr.Verify()\n\t}\n}\n\n"
                            "func ImposterBackward(xs []*lib.Reg) {\n"
                            "\tfor _, r := range slices.Backward(xs) {\n\t\tr.Verify()\n\t}\n}\n"),
              0);
    for (int i = 0; parallel && i < 52; i++) {
        char name[64], source[96];
        snprintf(name, sizeof(name), "padding_%02d.py", i);
        snprintf(source, sizeof(source), "def padding_%02d():\n    return %d\n", i, i);
        ASSERT_EQ(th_write_file(TH_PATH(repo, name), source), 0);
    }
    ASSERT_EQ(index_fixture(repo, database, parallel), 0);
    cbm_store_t *store = cbm_store_open_path_existing(database);
    ASSERT_NOT_NULL(store);
    sqlite3 *db = cbm_store_get_db(store);
    static const char reg_verify[] = "test.lib.Reg.Verify", other_verify[] = "test.app.other.Verify";
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Enum", "test.lib.Level.Name"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Literal", "test.lib.Cap.Copy"), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Closure", reg_verify), 1);
    /* By position: the first name is not the second result. */
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.First", other_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.First", reg_verify), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Last", reg_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Last", other_verify), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Param", reg_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Named", reg_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.NamedOne", reg_verify), 1);
    /* The right side reads the parameter, which the left side then redeclares. */
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Reused", reg_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Table", reg_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.TableOne", reg_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Local", reg_verify), 1);
    /* A local type hides the package's, and has no methods of its own. */
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.LocalShadow", other_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.LocalShadow", reg_verify), 0);
    /* `type mine lib.Reg` keeps the fields, not the methods. */
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Renamed", reg_verify), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Many", reg_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Immediately", reg_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.ImmediatelyOne", reg_verify), 1);
    /* Package repo at path .../v1 beside an alias v1: two different names, and only one is ours. */
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Paths", "test.repo.v1.Find"), 1);
    ASSERT_EQ(scalar(db, "SELECT count(*) FROM edges e JOIN nodes s ON s.id=e.source_id "
                         "WHERE e.type='CALLS' AND s.qualified_name='test.app.Paths'"),
              1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Received", reg_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.ReceivedParam", reg_verify), 1);
    /* Over a slice one variable is the index. */
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Indexes", reg_verify), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Cast", reg_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Backward", reg_verify), 1);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Cloned", reg_verify), 1);
    /* The same spellings under an import that is not the standard library. */
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.Imposter", reg_verify), 0);
    ASSERT_EQ(count_edge_between(db, "CALLS", "test.app.ImposterBackward", reg_verify), 0);
    cbm_store_close(store);
    th_rmtree(root);
    PASS();
}
TEST(resolver_stabilization_go_function_values_by_position_sequential) {
    return go_function_values_case(false);
}
TEST(resolver_stabilization_go_function_values_by_position_parallel) {
    return go_function_values_case(true);
}
TEST(resolver_stabilization_go_anonymous_structs_and_conversions_sequential) {
    return go_anonymous_struct_case(false);
}
TEST(resolver_stabilization_go_anonymous_structs_and_conversions_parallel) {
    return go_anonymous_struct_case(true);
}
TEST(resolver_stabilization_go_duplicate_module_path_sequential) {
    return go_duplicate_module_case(false);
}
TEST(resolver_stabilization_go_duplicate_module_path_parallel) {
    return go_duplicate_module_case(true);
}
TEST(resolver_stabilization_go_pipeline_references_sequential) {
    return go_references_case(false);
}
TEST(resolver_stabilization_go_pipeline_references_parallel) {
    return go_references_case(true);
}
TEST(resolver_stabilization_go_pipeline_twins_and_package_level_sequential) {
    return go_twins_case(false);
}
TEST(resolver_stabilization_go_pipeline_twins_and_package_level_parallel) {
    return go_twins_case(true);
}

TEST(resolver_stabilization_go_pipeline_mutation_changes_target_and_metadata) {
    const char *root = th_mktempdir("cbm_stabilization");
    ASSERT_NOT_NULL(root);
    char repo[1024], db1[1024], db2[1024];
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(db1, sizeof(db1), "%s/index.db", root);
    snprintf(db2, sizeof(db2), "%s/fresh.db", root);
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "api"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "bridge"), 0755));
    ASSERT(cbm_mkdir_p(TH_PATH(repo, "app"), 0755));
    ASSERT_EQ(th_write_file(TH_PATH(repo, "go.mod"), "module example\ngo 1.22\n"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "api/types.go"), API), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "bridge/make.go"), BRIDGE), 0);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "app/main.go"), APP), 0);
    for (int i = 0; i < 52; i++) {
        char name[64], source[96];
        snprintf(name, sizeof(name), "padding_%02d.py", i);
        snprintf(source, sizeof(source), "def padding_%02d():\n    return %d\n", i, i);
        ASSERT_EQ(th_write_file(TH_PATH(repo, name), source), 0);
    }
    ASSERT_EQ(index_fixture(repo, db1, false), 0);
    cbm_store_t *store = cbm_store_open_path_existing(db1);
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(scalar(cbm_store_get_db(store),
                     "SELECT count(*) FROM resolver_occurrences WHERE file_path='app/main.go' AND "
                     "spelling='q.Send' AND relation='CALLS' AND emitted=1 AND "
                     "target='test.api.Client.Send'"),
              1);
    ASSERT_EQ(
        scalar(
            cbm_store_get_db(store),
            "SELECT count(*) FROM resolver_derived_bindings WHERE key='go/type:test.api.Product' "
            "AND target='test.api.Client' AND json_extract(payload_json,'$.type_identity')<>''"),
        1);
    cbm_store_close(store);
    char changed[sizeof(API) + 1];
    strcpy(changed, API);
    char *alias = strstr(changed, "Product = Client");
    ASSERT_NOT_NULL(alias);
    size_t prefix = (size_t)(alias - changed) + strlen("Product = ");
    snprintf(changed + prefix, sizeof(changed) - prefix, "Other%s",
             API + prefix + strlen("Client"));
    ASSERT_EQ(th_write_file(TH_PATH(repo, "api/types.go"), changed), 0);
    ASSERT_EQ(index_fixture(repo, db1, true), 0);
    ASSERT_EQ(index_fixture(repo, db2, false), 0);
    store = cbm_store_open_path_existing(db1);
    ASSERT_NOT_NULL(store);
    sqlite3 *db = cbm_store_get_db(store);
    ASSERT_EQ(scalar(db, "SELECT count(*) FROM resolver_occurrences WHERE file_path='app/main.go' "
                         "AND spelling='q.Send' AND relation='CALLS' AND emitted=1 AND "
                         "target='test.api.Other.Send'"),
              1);
    ASSERT_EQ(scalar(db, "SELECT count(*) FROM resolver_occurrences WHERE file_path='app/main.go' "
                         "AND spelling='q.Send' AND relation='CALLS' AND emitted=1 AND "
                         "target='test.api.Client.Send'"),
              0);
    cbm_store_t *fresh = cbm_store_open_path_existing(db2);
    ASSERT_NOT_NULL(fresh);
    bool metadata_equal = same_derived_bindings(db, cbm_store_get_db(fresh));
    cbm_store_close(fresh);
    ASSERT(metadata_equal);
    cbm_store_close(store);
    th_rmtree(root);
    PASS();
}
SUITE(resolver_stabilization) {
    RUN_TEST(resolver_stabilization_selected_pending_type_drives_binding_and_return);
    RUN_TEST(resolver_stabilization_unknown_competitor_cycle_and_budget_abstain);
    RUN_TEST(resolver_stabilization_go_raw_call_identity_and_chained_spelling);
    RUN_TEST(resolver_stabilization_go_missing_duplicate_and_transformed_carriers_abstain);
    RUN_TEST(resolver_stabilization_go_call_carriers_require_matching_source_identity);
    RUN_TEST(resolver_stabilization_go_three_package_alias_chain_and_registration_order);
    RUN_TEST(resolver_stabilization_go_static_type_survives_flow_and_unknown_stays_unknown);
    RUN_TEST(resolver_stabilization_go_calls_in_every_statement_form_are_visited);
    RUN_TEST(resolver_stabilization_go_block_frames_shadow_exactly);
    RUN_TEST(resolver_stabilization_go_bare_result_types_declare_no_local);
    RUN_TEST(resolver_stabilization_go_function_values_yield_their_declared_result);
    RUN_TEST(resolver_stabilization_go_tuple_results_bind_by_position);
    RUN_TEST(resolver_stabilization_go_field_chains_follow_declared_field_types);
    RUN_TEST(resolver_stabilization_go_interface_methods_are_their_own_declarations);
    RUN_TEST(resolver_stabilization_go_embedding_follows_the_selector_rule);
    RUN_TEST(resolver_stabilization_go_open_embedded_type_blocks_promotion);
    RUN_TEST(resolver_stabilization_go_foreign_embed_hides_only_exported_names);
    RUN_TEST(resolver_stabilization_go_range_index_and_channel_yield_their_element);
    RUN_TEST(resolver_stabilization_go_assertions_and_type_switch_clauses_use_the_written_type);
    RUN_TEST(resolver_stabilization_go_literals_new_make_and_conversions_are_typed);
    RUN_TEST(resolver_stabilization_go_package_variables_have_their_declared_type);
    RUN_TEST(resolver_stabilization_go_defined_non_struct_types_have_method_sets);
    RUN_TEST(resolver_stabilization_go_defined_type_keeps_what_the_spec_says);
    RUN_TEST(resolver_stabilization_go_generic_declarations_keep_their_method_sets);
    RUN_TEST(resolver_stabilization_go_build_tag_twins_are_one_target_but_results_must_agree);
    RUN_TEST(resolver_stabilization_go_imported_members_keep_their_package_identity);
    RUN_TEST(resolver_stabilization_go_syntax_error_costs_only_its_own_declaration);
    RUN_TEST(resolver_stabilization_go_replace_directive_proves_a_sibling_module);
    RUN_TEST(resolver_stabilization_go_workspace_use_proves_a_listed_module);
    RUN_TEST(resolver_stabilization_overloads_share_an_identity_and_rebinding_does_not_sequential);
    RUN_TEST(resolver_stabilization_overloads_share_an_identity_and_rebinding_does_not_parallel);
    RUN_TEST(resolver_stabilization_go_new_takes_an_expression_sequential);
    RUN_TEST(resolver_stabilization_go_new_takes_an_expression_parallel);
    RUN_TEST(resolver_stabilization_go_function_values_by_position_sequential);
    RUN_TEST(resolver_stabilization_go_function_values_by_position_parallel);
    RUN_TEST(resolver_stabilization_go_anonymous_structs_and_conversions_sequential);
    RUN_TEST(resolver_stabilization_go_anonymous_structs_and_conversions_parallel);
    RUN_TEST(resolver_stabilization_go_duplicate_module_path_sequential);
    RUN_TEST(resolver_stabilization_go_duplicate_module_path_parallel);
    RUN_TEST(resolver_stabilization_go_pipeline_references_sequential);
    RUN_TEST(resolver_stabilization_go_pipeline_references_parallel);
    RUN_TEST(resolver_stabilization_go_pipeline_twins_and_package_level_sequential);
    RUN_TEST(resolver_stabilization_go_pipeline_twins_and_package_level_parallel);
    RUN_TEST(resolver_stabilization_generic_type_shadow_never_binds_package_type);
    RUN_TEST(resolver_stabilization_generic_receiver_parameters_shadow_package_types);
    RUN_TEST(resolver_stabilization_captured_binding_does_not_regain_narrowed_authority);
    RUN_TEST(resolver_stabilization_address_escape_survives_later_rebinding);
    RUN_TEST(resolver_stabilization_assignment_lhs_escape_survives_later_rebinding);
    RUN_TEST(resolver_stabilization_async_capture_does_not_regain_narrowed_authority);
    RUN_TEST(resolver_stabilization_multi_declaration_does_not_overwrite_outer_binding);
    RUN_TEST(resolver_stabilization_grouped_var_callable_shadows_package_callable);
    RUN_TEST(resolver_stabilization_nonlocal_assignment_does_not_invent_local_authority);
    RUN_TEST(resolver_stabilization_local_type_shadows_function_and_package_type);
    RUN_TEST(resolver_stabilization_named_result_shadows_package_callable);
    RUN_TEST(resolver_stabilization_inapplicable_pointer_methods_abstain);
    RUN_TEST(resolver_stabilization_external_import_is_unknown_not_global_failure);
    RUN_TEST(resolver_stabilization_bodyless_declaration_does_not_poison_collection);
    RUN_TEST(resolver_stabilization_applicable_value_and_pointer_receivers_remain_exact);
    RUN_TEST(resolver_stabilization_declared_concrete_identity_survives_capture_and_escape);
    RUN_TEST(resolver_stabilization_nested_module_is_not_import_authority);
    RUN_TEST(resolver_stabilization_normalized_imports_ignore_projection_and_reject_stale_source);
    RUN_TEST(resolver_stabilization_unsupported_go_forms_do_not_poison_other_bindings);
    RUN_TEST(resolver_stabilization_go_pipeline_mutation_changes_target_and_metadata);
}
