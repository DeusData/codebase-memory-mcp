#include "test_framework.h"
#include "cbm.h"
#include "resolve/binding.h"
#include "resolve/policy.h"
#include "resolve/proof.h"

static cbm_resolve_result_t local_candidate(CBMArena *arena, const CBMFileResult *result,
                                            const char *name) {
    return (cbm_resolve_result_t){
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_SCOPE_BINDING,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_SCOPE_BOUND,
        .target = cbm_arena_sprintf(arena, "%s.%s", result->module_qn, name),
        .candidate_count = 1};
}

static const CBMCall *find_call(const CBMFileResult *result, const char *name) {
    for (int i = 0; i < result->calls.count; i++) {
        if (strcmp(result->calls.items[i].callee_name, name) == 0) {
            return &result->calls.items[i];
        }
    }
    return NULL;
}

static cbm_resolve_result_t validate_call(cbm_resolve_binding_index_t *index, const CBMCall *call,
                                          cbm_resolve_result_t candidate) {
    return cbm_resolve_binding_validate(index, call->callee_name, call->site_start_byte,
                                        call->site_end_byte, call->source_origin, candidate);
}

TEST(resolver_binding_python_direct_module_function) {
    const char *source = "def helper():\n    return 1\ndef caller():\n    return helper()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    const CBMCall *call = find_call(result, "helper");
    ASSERT_NOT_NULL(call);
    cbm_resolve_result_t resolved =
        validate_call(index, call, local_candidate(&arena, result, "helper"));
    ASSERT(cbm_resolve_admit(&resolved));
    ASSERT(resolved.facts & CBM_RESOLVE_FACT_OCCURRENCE_EXACT);
    ASSERT(cbm_resolve_binding_export(index, resolved.target));
    ASSERT(!cbm_resolve_binding_export(index, "test.decoy.helper"));
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_python_parameter_shadow_is_not_module_call) {
    const char *source = "def helper():\n    return 1\ndef caller(helper):\n    return helper()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    const CBMCall *call = find_call(result, "helper");
    ASSERT_NOT_NULL(call);
    cbm_resolve_result_t resolved =
        validate_call(index, call, local_candidate(&arena, result, "helper"));
    ASSERT(!cbm_resolve_admit(&resolved));
    ASSERT_STR_EQ(resolved.reason, "binding_shadowed_or_unavailable");
    ASSERT(cbm_resolve_binding_export(index, resolved.target));
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_python_assignment_and_duplicate_definitions_abstain) {
    const char *sources[] = {
        "def helper():\n    return 1\ndef caller():\n    helper = other\n    return helper()\n",
        "def helper():\n    return 1\ndef helper():\n    return 2\ndef caller():\n    return "
        "helper()\n",
        "def helper():\n    return 1\ndef caller():\n    def helper():\n        return 2\n    "
        "return helper()\n",
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                                 "test", "binding.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
        const CBMCall *call = find_call(result, "helper");
        ASSERT_NOT_NULL(call);
        cbm_resolve_result_t resolved =
            validate_call(index, call, local_candidate(&arena, result, "helper"));
        ASSERT(!cbm_resolve_admit(&resolved));
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_binding_dynamic_namespace_and_unknown_syntax_abstain) {
    const char *sources[] = {
        "def helper():\n    return 1\nexec(code)\ndef caller():\n    return helper()\n",
        "@decorator\ndef helper():\n    return 1\ndef caller():\n    return helper()\n",
        "def helper():\n    return 1\nfrom external import *\ndef caller():\n    return helper()\n",
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                                 "test", "binding.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
        const CBMCall *call = find_call(result, "helper");
        ASSERT_NOT_NULL(call);
        cbm_resolve_result_t resolved =
            validate_call(index, call, local_candidate(&arena, result, "helper"));
        ASSERT(!cbm_resolve_admit(&resolved));
        ASSERT_EQ(resolved.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_binding_python_import_needs_independent_export_proof) {
    const char *source = "from library import helper\ndef caller():\n    return helper()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    const CBMCall *call = find_call(result, "helper");
    ASSERT_NOT_NULL(call);
    cbm_resolve_result_t candidate = local_candidate(&arena, result, "helper");
    candidate.rule = CBM_RESOLVE_RULE_IMPORT_BINDING;
    candidate.target = "test.library.helper";
    candidate.facts |= CBM_RESOLVE_FACT_IMPORT_BOUND;
    cbm_resolve_result_t unresolved = validate_call(index, call, candidate);
    ASSERT(!cbm_resolve_admit(&unresolved));
    candidate.facts |= CBM_RESOLVE_FACT_LOOKUP_COMPLETE;
    cbm_resolve_result_t resolved = validate_call(index, call, candidate);
    ASSERT(cbm_resolve_admit(&resolved));
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_python_relative_import_preserves_local_authority) {
    const char *sources[] = {
        ("from .library import helper as execute\ndef caller():\n    return execute()\n"),
        ("from ..library import helper as execute\ndef caller():\n    return execute()\n"),
        ("from .library import helper as execute\ndef caller(execute):\n    return execute()\n"),
        ("from .library import helper as execute\ndef caller():\n    execute = other\n    return "
         "execute()\n"),
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                                 "test", "package/binding.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
        const CBMCall *call = find_call(result, "execute");
        ASSERT_NOT_NULL(call);
        cbm_resolve_result_t candidate = local_candidate(&arena, result, "helper");
        candidate.rule = CBM_RESOLVE_RULE_IMPORT_BINDING;
        candidate.target = "test.library.helper";
        candidate.facts |= CBM_RESOLVE_FACT_IMPORT_BOUND;
        /* Structural syntax support alone does not establish the imported target. */
        cbm_resolve_result_t unresolved = validate_call(index, call, candidate);
        ASSERT(!cbm_resolve_admit(&unresolved));
        candidate.facts |= CBM_RESOLVE_FACT_LOOKUP_COMPLETE;
        cbm_resolve_result_t resolved = validate_call(index, call, candidate);
        ASSERT_EQ(cbm_resolve_admit(&resolved), i < 2);
        if (i < 2) {
            ASSERT_STR_EQ(resolved.target, "test.library.helper");
            ASSERT(resolved.facts & CBM_RESOLVE_FACT_OCCURRENCE_EXACT);
        }
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_binding_exact_occurrence_and_origin_required) {
    const char *source = "def helper():\n    return 1\ndef caller():\n    return helper()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    const CBMCall *call = find_call(result, "helper");
    ASSERT_NOT_NULL(call);
    cbm_resolve_result_t candidate = local_candidate(&arena, result, "helper");
    cbm_resolve_result_t shifted =
        cbm_resolve_binding_validate(index, "helper", call->site_start_byte + 1,
                                     call->site_end_byte, CBM_SOURCE_ORIGIN_RAW, candidate);
    ASSERT(!cbm_resolve_admit(&shifted));
    cbm_resolve_result_t expanded =
        cbm_resolve_binding_validate(index, "helper", call->site_start_byte, call->site_end_byte,
                                     CBM_SOURCE_ORIGIN_PREPROCESSED, candidate);
    ASSERT(!cbm_resolve_admit(&expanded));
    candidate.target = "test.decoy.helper";
    cbm_resolve_result_t decoy = validate_call(index, call, candidate);
    ASSERT(!cbm_resolve_admit(&decoy));
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_lua_local_function_and_shadow) {
    const char *sources[] = {
        "local function helper() return 1 end\nlocal function caller() return helper() end\n",
        "local function helper() return 1 end\nlocal function caller(helper) return helper() end\n",
        "function helper() return 1 end\nfunction caller() return helper() end\n",
    };
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_LUA, "test",
                                                 "binding.lua", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_LUA, source, strlen(source));
        const CBMCall *call = find_call(result, "helper");
        ASSERT_NOT_NULL(call);
        cbm_resolve_result_t resolved =
            validate_call(index, call, local_candidate(&arena, result, "helper"));
        ASSERT_EQ(cbm_resolve_admit(&resolved), i == 0);
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_binding_unsupported_language_is_explicit) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMFileResult result = {.module_qn = "test.file"};
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, &result, CBM_LANG_JULIA, "helper()", 8);
    ASSERT_STR_EQ(cbm_resolve_binding_status(index), "unsupported_binding_language");
    cbm_resolve_result_t candidate = local_candidate(&arena, &result, "helper");
    cbm_resolve_result_t unresolved =
        cbm_resolve_binding_validate(index, "helper", 0, 8, CBM_SOURCE_ORIGIN_RAW, candidate);
    ASSERT(!cbm_resolve_admit(&unresolved));
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_binding_python_class_and_with_keep_independent_bindings) {
    const char *sources[] = {("def helper():\n    return 1\nclass Other:\n    pass\n"
                              "def caller():\n    return helper()\n"),
                             ("def helper():\n    return 1\ndef caller():\n"
                              "    with context():\n        return helper()\n"),
                             ("def helper():\n    return 1\ndef caller():\n"
                              "    with context() as helper:\n        return helper()\n"),
                             ("def helper():\n    return 1\nclass helper:\n    pass\n"
                              "def caller():\n    return helper()\n")};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                                 "test", "binding.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
        const CBMCall *call = find_call(result, "helper");
        ASSERT_NOT_NULL(call);
        cbm_resolve_result_t outcome =
            validate_call(index, call, local_candidate(&arena, result, "helper"));
        bool admitted = cbm_resolve_admit(&outcome);
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
        ASSERT_EQ(admitted, i < 2);
    }
    PASS();
}

TEST(resolver_binding_python_shadow_is_local_to_its_function) {
    const char *source = "def helper():\n    pass\n"
                         "def shadow(helper):\n    helper()\n"
                         "def clear():\n    helper()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    int admitted = 0, blocked = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (strcmp(call->callee_name, "helper"))
            continue;
        cbm_resolve_result_t proof =
            validate_call(index, call, local_candidate(&arena, result, "helper"));
        if (strstr(call->enclosing_func_qn, ".clear"))
            admitted += cbm_resolve_admit(&proof);
        else
            blocked += !cbm_resolve_admit(&proof);
    }
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    ASSERT_EQ(admitted, 1);
    ASSERT_EQ(blocked, 1);
    PASS();
}

TEST(resolver_binding_python_nested_function_uses_lexical_declaration) {
    const char *source = "def helper():\n    pass\n"
                         "def outer():\n    def helper():\n        pass\n    helper()\n"
                         "def clear():\n    helper()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    int exact = 0, wrong = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (strcmp(call->callee_name, "helper"))
            continue;
        bool nested = strstr(call->enclosing_func_qn, ".outer") != NULL;
        cbm_resolve_result_t candidate =
            local_candidate(&arena, result, nested ? "outer.helper" : "helper");
        cbm_resolve_result_t proof = validate_call(index, call, candidate);
        exact += cbm_resolve_admit(&proof);
        candidate = local_candidate(&arena, result, nested ? "helper" : "outer.helper");
        proof = validate_call(index, call, candidate);
        wrong += cbm_resolve_admit(&proof);
    }
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    ASSERT_EQ(exact, 2);
    ASSERT_EQ(wrong, 0);
    PASS();
}

TEST(resolver_binding_python_assignment_values_do_not_create_shadow_bindings) {
    const char *source = "def helper():\n    return 1\n"
                         "def assigned():\n    value = helper()\n"
                         "def default(value=helper()):\n    helper()\n"
                         "def keyword():\n    print(value=helper())\n"
                         "def loop():\n    for value in helper():\n        helper()\n"
                         "def attribute():\n    helper.field = helper()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    int calls = 0, admitted = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (strcmp(call->callee_name, "helper"))
            continue;
        calls++;
        cbm_resolve_result_t proof =
            validate_call(index, call, local_candidate(&arena, result, "helper"));
        admitted += cbm_resolve_admit(&proof);
    }
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    ASSERT_EQ(calls, 7);
    ASSERT_EQ(admitted, calls);
    PASS();
}

TEST(resolver_binding_python_decorators_block_the_wrapped_target_only) {
    const char *source = "def helper():\n    pass\n"
                         "@decorate\ndef wrapped():\n    helper()\n"
                         "def caller():\n    helper()\n    wrapped()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    int helpers = 0, wrapped = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        cbm_resolve_result_t proof =
            validate_call(index, call, local_candidate(&arena, result, call->callee_name));
        if (!strcmp(call->callee_name, "helper"))
            helpers += cbm_resolve_admit(&proof);
        if (!strcmp(call->callee_name, "wrapped"))
            wrapped += cbm_resolve_admit(&proof);
    }
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    ASSERT_EQ(helpers, 2);
    ASSERT_EQ(wrapped, 0);
    PASS();
}

TEST(resolver_binding_python_lambda_parameters_have_their_own_scope) {
    const char *source = "def helper():\n    pass\n"
                         "def caller():\n    callback = lambda helper: helper()\n    helper()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    int admitted = 0, blocked = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (strcmp(call->callee_name, "helper"))
            continue;
        cbm_resolve_result_t proof =
            validate_call(index, call, local_candidate(&arena, result, "helper"));
        if (cbm_resolve_admit(&proof))
            admitted++;
        else
            blocked++;
    }
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    ASSERT_EQ(admitted, 1);
    ASSERT_EQ(blocked, 1);
    PASS();
}

TEST(resolver_binding_python_explicit_binding_after_wildcard) {
    const char *sources[] = {
        "from external import *\ndef helper():\n    pass\ndef run():\n    helper()\n",
        "def helper():\n    pass\nfrom external import *\ndef run():\n    helper()\n",
        "from external import *\ndef run():\n    helper()\n",
        ("if condition:\n    from external import *\ndef helper():\n    pass\ndef run():\n    "
         "helper()\n")};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        const char *source = sources[i];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                                 "test", "binding.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
        const CBMCall *call = find_call(result, "helper");
        ASSERT_NOT_NULL(call);
        cbm_resolve_result_t proof =
            validate_call(index, call, local_candidate(&arena, result, "helper"));
        ASSERT_EQ(cbm_resolve_admit(&proof), i == 0);
        ASSERT_EQ(cbm_resolve_binding_export(index, proof.target), i == 0);
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
    }
    PASS();
}

TEST(resolver_binding_python_defaults_and_annotations_use_enclosing_namespace) {
    const char *source = "def target():\n    return 1\n"
                         "class C:\n    def target():\n        return 2\n"
                         "    def method(value: target() = target()) -> target():\n"
                         "        target()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    uint32_t body = (uint32_t)(strstr(source, "        target()") - source);
    int headers = 0, bodies = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (strcmp(call->callee_name, "target"))
            continue;
        bool header = call->site_start_byte < body;
        cbm_resolve_result_t correct = validate_call(
            index, call, local_candidate(&arena, result, header ? "C.target" : "target"));
        cbm_resolve_result_t wrong = validate_call(
            index, call, local_candidate(&arena, result, header ? "target" : "C.target"));
        ASSERT(cbm_resolve_admit(&correct));
        ASSERT_FALSE(cbm_resolve_admit(&wrong));
        if (header)
            headers++;
        else
            bodies++;
    }
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    ASSERT_EQ(headers, 3);
    ASSERT_EQ(bodies, 1);
    PASS();
}

TEST(resolver_binding_python_self_default_is_unbound_but_body_recurses) {
    const char *source = "def target(value=target()):\n    return target()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    uint32_t body = (uint32_t)(strstr(source, "    return") - source);
    int rejected = 0, recursive = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (strcmp(call->callee_name, "target"))
            continue;
        cbm_resolve_result_t proof =
            validate_call(index, call, local_candidate(&arena, result, "target"));
        if (call->site_start_byte < body) {
            ASSERT_FALSE(cbm_resolve_admit(&proof));
            ASSERT_STR_EQ(proof.reason, "binding_declaration_not_yet_bound");
            rejected++;
        } else {
            ASSERT(cbm_resolve_admit(&proof));
            recursive++;
        }
    }
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    ASSERT_EQ(rejected, 1);
    ASSERT_EQ(recursive, 1);
    PASS();
}

TEST(resolver_binding_python_default_reads_outer_name_before_parameter_shadow) {
    const char *sources[] = {
        "def target():\n    return 1\ndef caller(target=target()):\n    return target()\n",
        "def target():\n    return 1\ncallback = lambda target=target(): target()\n"};
    for (size_t example = 0; example < sizeof(sources) / sizeof(sources[0]); example++) {
        const char *source = sources[example];
        CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                                 "test", "binding.py", 0, NULL, NULL);
        ASSERT_NOT_NULL(result);
        CBMArena arena;
        cbm_arena_init(&arena);
        cbm_resolve_binding_index_t *index =
            cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
        uint32_t default_at =
            (uint32_t)(strstr(source, "target=target()") - source + strlen("target="));
        int defaults = 0, shadows = 0;
        for (int i = 0; i < result->calls.count; i++) {
            const CBMCall *call = &result->calls.items[i];
            if (strcmp(call->callee_name, "target"))
                continue;
            cbm_resolve_result_t proof =
                validate_call(index, call, local_candidate(&arena, result, "target"));
            if (call->site_start_byte == default_at) {
                ASSERT(cbm_resolve_admit(&proof));
                defaults++;
            } else {
                ASSERT_FALSE(cbm_resolve_admit(&proof));
                shadows++;
            }
        }
        cbm_arena_destroy(&arena);
        cbm_free_result(result);
        ASSERT_EQ(defaults, 1);
        ASSERT_EQ(shadows, 1);
    }
    PASS();
}

TEST(resolver_binding_python_package_symbol_scope_preserves_file_identity) {
    const char *source = "def target():\n    return 1\ndef caller():\n    return target()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "pkg/__init__.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    ASSERT_STR_EQ(result->module_qn, "test.pkg.__init__");
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    const CBMCall *call = find_call(result, "target");
    ASSERT_NOT_NULL(call);
    cbm_resolve_result_t proof = cbm_resolve_binding_local(
        index, call->callee_name, call->site_start_byte, call->site_end_byte, call->source_origin);
    ASSERT(cbm_resolve_admit(&proof));
    ASSERT_STR_EQ(proof.target, "test.pkg.target");
    ASSERT(cbm_resolve_binding_export(index, "test.pkg.target"));
    ASSERT_FALSE(cbm_resolve_binding_export(index, "test.pkg.__init__.target"));
    ASSERT_STR_EQ(result->module_qn, "test.pkg.__init__");
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_local_result_carries_captured_source_proof) {
    const char *source = "def helper():\n    return 1\ndef caller():\n    return helper()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    const CBMCall *call = find_call(result, "helper");
    ASSERT_NOT_NULL(call);
    cbm_resolve_result_t resolved =
        validate_call(index, call, local_candidate(&arena, result, "helper"));
    ASSERT(cbm_resolve_admit(&resolved));
    ASSERT_NOT_NULL(resolved.proof);
    const cbm_resolve_proof_view_t *proof = cbm_resolve_proof_view(resolved.proof);
    ASSERT_STR_EQ(proof->source.content_identity, result->source_content_id[0]);
    ASSERT_STR_EQ(proof->source.configuration_identity, result->source_configuration_id);
    ASSERT(proof->premise_count >= 3);
    ASSERT_EQ(proof->key.start_byte, call->site_start_byte);
    ASSERT_EQ(proof->key.end_byte, call->site_end_byte);
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_binding_missing_captured_source_remains_unknown) {
    const char *source = "def helper():\n    return 1\ndef caller():\n    return helper()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "binding.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    result->source_content_id[0][0] = '\0';
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    const CBMCall *call = find_call(result, "helper");
    ASSERT_NOT_NULL(call);
    cbm_resolve_result_t resolved =
        validate_call(index, call, local_candidate(&arena, result, "helper"));
    ASSERT(!cbm_resolve_admit(&resolved));
    ASSERT_EQ(resolved.analysis, CBM_RESOLVE_ANALYSIS_UNKNOWN);
    cbm_arena_destroy(&arena);
    cbm_free_result(result);
    PASS();
}

SUITE(resolver_binding) {
    RUN_TEST(resolver_binding_local_result_carries_captured_source_proof);
    RUN_TEST(resolver_binding_missing_captured_source_remains_unknown);
    RUN_TEST(resolver_binding_python_defaults_and_annotations_use_enclosing_namespace);
    RUN_TEST(resolver_binding_python_self_default_is_unbound_but_body_recurses);
    RUN_TEST(resolver_binding_python_default_reads_outer_name_before_parameter_shadow);
    RUN_TEST(resolver_binding_python_package_symbol_scope_preserves_file_identity);
    RUN_TEST(resolver_binding_python_explicit_binding_after_wildcard);
    RUN_TEST(resolver_binding_python_lambda_parameters_have_their_own_scope);
    RUN_TEST(resolver_binding_python_assignment_values_do_not_create_shadow_bindings);
    RUN_TEST(resolver_binding_python_decorators_block_the_wrapped_target_only);
    RUN_TEST(resolver_binding_python_shadow_is_local_to_its_function);
    RUN_TEST(resolver_binding_python_nested_function_uses_lexical_declaration);
    RUN_TEST(resolver_binding_python_class_and_with_keep_independent_bindings);
    RUN_TEST(resolver_binding_python_direct_module_function);
    RUN_TEST(resolver_binding_python_parameter_shadow_is_not_module_call);
    RUN_TEST(resolver_binding_python_assignment_and_duplicate_definitions_abstain);
    RUN_TEST(resolver_binding_dynamic_namespace_and_unknown_syntax_abstain);
    RUN_TEST(resolver_binding_python_import_needs_independent_export_proof);
    RUN_TEST(resolver_binding_python_relative_import_preserves_local_authority);
    RUN_TEST(resolver_binding_exact_occurrence_and_origin_required);
    RUN_TEST(resolver_binding_lua_local_function_and_shadow);
    RUN_TEST(resolver_binding_unsupported_language_is_explicit);
}
