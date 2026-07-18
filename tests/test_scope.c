/*
 * test_scope.c — Tests for the chunked linked-frame CBMScope.
 *
 * Phase 0 of Python LSP integration: replaces the legacy fixed 64-binding
 * array with growable per-scope chunks. Tests verify dynamic growth,
 * binding-shadowing semantics, and the parent-chain lookup contract that
 * Go and C/C++ LSP implementations rely on.
 */
#include "test_framework.h"
#include "cbm.h"
#include "lsp/scope.h"
#include "lsp/type_rep.h"

/* Build a NAMED type for a fixture string. Arena-allocated. */
static const CBMType *named_t(CBMArena *a, const char *qn) {
    return cbm_type_named(a, qn);
}

/* ── Basic API ─────────────────────────────────────────────────── */

TEST(scope_push_returns_distinct_scope) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *root = cbm_scope_push(&a, NULL);
    CBMScope *child = cbm_scope_push(&a, root);
    ASSERT_NOT_NULL(root);
    ASSERT_NOT_NULL(child);
    ASSERT(child != root);
    ASSERT(child->parent == root);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(scope_pop_returns_parent) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *root = cbm_scope_push(&a, NULL);
    CBMScope *child = cbm_scope_push(&a, root);
    ASSERT(cbm_scope_pop(child) == root);
    ASSERT(cbm_scope_pop(root) == NULL);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(scope_lookup_unbound_returns_unknown) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *s = cbm_scope_push(&a, NULL);
    const CBMType *t = cbm_scope_lookup(s, "missing");
    ASSERT_NOT_NULL(t);
    ASSERT(cbm_type_is_unknown(t));
    cbm_arena_destroy(&a);
    PASS();
}

/* ── Binding semantics ─────────────────────────────────────────── */

TEST(scope_bind_then_lookup) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *s = cbm_scope_push(&a, NULL);
    cbm_scope_bind(s, "x", named_t(&a, "int"));
    const CBMType *t = cbm_scope_lookup(s, "x");
    ASSERT_NOT_NULL(t);
    ASSERT(t->kind == CBM_TYPE_NAMED);
    ASSERT_STR_EQ(t->data.named.qualified_name, "int");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(scope_rebind_overwrites_in_place) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *s = cbm_scope_push(&a, NULL);
    cbm_scope_bind(s, "x", named_t(&a, "int"));
    cbm_scope_bind(s, "x", named_t(&a, "string"));
    const CBMType *t = cbm_scope_lookup(s, "x");
    ASSERT_STR_EQ(t->data.named.qualified_name, "string");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(scope_lookup_walks_parent_chain) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *root = cbm_scope_push(&a, NULL);
    CBMScope *child = cbm_scope_push(&a, root);
    cbm_scope_bind(root, "outer", named_t(&a, "Outer"));
    cbm_scope_bind(child, "inner", named_t(&a, "Inner"));
    const CBMType *outer_in_child = cbm_scope_lookup(child, "outer");
    const CBMType *inner_in_root = cbm_scope_lookup(root, "inner");
    ASSERT_STR_EQ(outer_in_child->data.named.qualified_name, "Outer");
    ASSERT(cbm_type_is_unknown(inner_in_root));
    cbm_arena_destroy(&a);
    PASS();
}

TEST(scope_child_shadows_parent) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *root = cbm_scope_push(&a, NULL);
    CBMScope *child = cbm_scope_push(&a, root);
    cbm_scope_bind(root, "x", named_t(&a, "ParentInt"));
    cbm_scope_bind(child, "x", named_t(&a, "ChildStr"));
    const CBMType *t = cbm_scope_lookup(child, "x");
    ASSERT_STR_EQ(t->data.named.qualified_name, "ChildStr");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(scope_callable_identity_follows_nearest_binding) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *root = cbm_scope_push(&a, NULL);
    CBMScope *child = cbm_scope_push(&a, root);
    const CBMType *callback_type = named_t(&a, "Callback");
    cbm_scope_bind_callable(root, "callback", callback_type, "pkg.actual");
    ASSERT_TRUE(cbm_scope_contains(child, "callback"));
    ASSERT_STR_EQ(cbm_scope_lookup_callable(child, "callback"), "pkg.actual");

    cbm_scope_bind(child, "callback", named_t(&a, "int"));
    ASSERT_NULL(cbm_scope_lookup_callable(child, "callback"));
    ASSERT_STR_EQ(cbm_scope_lookup_callable(root, "callback"), "pkg.actual");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(scope_assignment_updates_or_clears_nearest_callable_only) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *root = cbm_scope_push(&a, NULL);
    CBMScope *child = cbm_scope_push(&a, root);
    const CBMType *callback_type = named_t(&a, "Callback");
    cbm_scope_bind_callable(root, "callback", callback_type, "pkg.actual");

    ASSERT_TRUE(cbm_scope_update_callable(child, "callback", "pkg.alternate"));
    ASSERT_STR_EQ(cbm_scope_lookup_callable(root, "callback"), "pkg.alternate");
    ASSERT(cbm_scope_lookup(root, "callback") == callback_type);
    ASSERT_TRUE(cbm_scope_update_callable(child, "callback", NULL));
    ASSERT_NULL(cbm_scope_lookup_callable(root, "callback"));
    ASSERT_FALSE(cbm_scope_update_callable(child, "missing", "pkg.never"));

    cbm_scope_bind_callable(root, "callback", callback_type, "pkg.restored");
    cbm_scope_bind_callable(child, "callback", callback_type, "pkg.child");
    ASSERT_TRUE(cbm_scope_update_callable(child, "callback", "pkg.child_new"));
    ASSERT_STR_EQ(cbm_scope_lookup_callable(child, "callback"), "pkg.child_new");
    ASSERT_STR_EQ(cbm_scope_lookup_callable(root, "callback"), "pkg.restored");
    cbm_arena_destroy(&a);
    PASS();
}

TEST(scope_checked_bind_reports_child_oom_despite_parent_name) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *root = cbm_scope_push(&a, NULL);
    CBMScope *ordinary_child = cbm_scope_push(&a, root);
    CBMScope *callable_child = cbm_scope_push(&a, root);
    const CBMType *callback_type = named_t(&a, "Callback");
    const CBMType *shadow_type = named_t(&a, "Shadow");
    cbm_scope_bind_callable(root, "callback", callback_type, "pkg.parent");

    int saved_nblocks = a.nblocks;
    size_t saved_used = a.used;
    a.nblocks = CBM_ARENA_MAX_BLOCKS;
    a.used = a.block_size;
    bool ordinary_bound = cbm_scope_bind_checked(ordinary_child, "callback", shadow_type);
    bool callable_bound = cbm_scope_bind_callable_checked(
        callable_child, "callback", shadow_type, "pkg.child");
    a.nblocks = saved_nblocks;
    a.used = saved_used;

    ASSERT_FALSE(ordinary_bound);
    ASSERT_FALSE(callable_bound);
    ASSERT_STR_EQ(cbm_scope_lookup_callable(ordinary_child, "callback"), "pkg.parent");
    ASSERT_STR_EQ(cbm_scope_lookup_callable(callable_child, "callback"), "pkg.parent");
    cbm_arena_destroy(&a);
    PASS();
}

/* ── Dynamic growth — the Phase 0 win ───────────────────────────── */

TEST(scope_dynamic_growth_300_bindings) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *s = cbm_scope_push(&a, NULL);
    char names[300][16];
    for (int i = 0; i < 300; i++) {
        snprintf(names[i], sizeof(names[0]), "v%d", i);
        char qn[32];
        snprintf(qn, sizeof(qn), "T%d", i);
        cbm_scope_bind(s, names[i], named_t(&a, qn));
    }
    /* All 300 bindings retrievable — old 64-cap would have dropped 236 */
    for (int i = 0; i < 300; i++) {
        const CBMType *t = cbm_scope_lookup(s, names[i]);
        ASSERT_NOT_NULL(t);
        ASSERT(t->kind == CBM_TYPE_NAMED);
        char expected[32];
        snprintf(expected, sizeof(expected), "T%d", i);
        ASSERT_STR_EQ(t->data.named.qualified_name, expected);
    }
    cbm_arena_destroy(&a);
    PASS();
}

TEST(scope_growth_chunk_boundary) {
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *s = cbm_scope_push(&a, NULL);
    char names[CBM_SCOPE_CHUNK_BINDINGS + 1][16];
    /* Fill exactly one chunk + spillover: forces second chunk allocation */
    for (int i = 0; i <= CBM_SCOPE_CHUNK_BINDINGS; i++) {
        snprintf(names[i], sizeof(names[0]), "n%d", i);
        char qn[16];
        snprintf(qn, sizeof(qn), "T%d", i);
        cbm_scope_bind(s, names[i], named_t(&a, qn));
    }
    /* First-chunk binding still retrievable */
    const CBMType *first = cbm_scope_lookup(s, "n0");
    ASSERT_STR_EQ(first->data.named.qualified_name, "T0");
    /* Spillover binding (would have fallen off legacy 64-cap if cap < 17) */
    char last_name[16];
    snprintf(last_name, sizeof(last_name), "n%d", CBM_SCOPE_CHUNK_BINDINGS);
    char last_qn[16];
    snprintf(last_qn, sizeof(last_qn), "T%d", CBM_SCOPE_CHUNK_BINDINGS);
    const CBMType *last = cbm_scope_lookup(s, last_name);
    ASSERT_STR_EQ(last->data.named.qualified_name, last_qn);
    cbm_arena_destroy(&a);
    PASS();
}

TEST(scope_rebind_in_old_chunk) {
    /* After many bindings have spilled into newer chunks, rebinding a name
     * from the original chunk must overwrite that binding in place rather
     * than create a duplicate in the head chunk. */
    CBMArena a;
    cbm_arena_init(&a);
    CBMScope *s = cbm_scope_push(&a, NULL);
    cbm_scope_bind(s, "early", named_t(&a, "EarlyV1"));
    char names[100][16];
    for (int i = 0; i < 100; i++) {
        snprintf(names[i], sizeof(names[0]), "fill%d", i);
        cbm_scope_bind(s, names[i], named_t(&a, "Filler"));
    }
    cbm_scope_bind(s, "early", named_t(&a, "EarlyV2"));
    const CBMType *t = cbm_scope_lookup(s, "early");
    ASSERT_STR_EQ(t->data.named.qualified_name, "EarlyV2");
    cbm_arena_destroy(&a);
    PASS();
}

/* ── Suite registration ────────────────────────────────────────── */

SUITE(scope) {
    RUN_TEST(scope_push_returns_distinct_scope);
    RUN_TEST(scope_pop_returns_parent);
    RUN_TEST(scope_lookup_unbound_returns_unknown);
    RUN_TEST(scope_bind_then_lookup);
    RUN_TEST(scope_rebind_overwrites_in_place);
    RUN_TEST(scope_lookup_walks_parent_chain);
    RUN_TEST(scope_child_shadows_parent);
    RUN_TEST(scope_callable_identity_follows_nearest_binding);
    RUN_TEST(scope_assignment_updates_or_clears_nearest_callable_only);
    RUN_TEST(scope_checked_bind_reports_child_oom_despite_parent_name);
    RUN_TEST(scope_dynamic_growth_300_bindings);
    RUN_TEST(scope_growth_chunk_boundary);
    RUN_TEST(scope_rebind_in_old_chunk);
}
