#include "cbm.h"
#include "helpers.h"
#include "lang_specs.h"
#include "extract_unified.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include "foundation/constants.h"
#include "ts_node_stack.h"

enum { MAX_PARENT_DEPTH = 10, LAST_IDX = 1 };
#include <stdint.h> // uint32_t
#include <string.h>
#include <ctype.h>

// Forward declaration
static void walk_usages(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec);

// Check if a node is inside a call expression (to avoid double-counting as usage)
static bool is_inside_call(TSNode node, const CBMLangSpec *spec) {
    TSNode cur = ts_node_parent(node);
    int depth = 0;
    while (!ts_node_is_null(cur) && depth < MAX_PARENT_DEPTH) {
        if (cbm_kind_in_set(cur, spec->call_node_types)) {
            return true;
        }
        cur = ts_node_parent(cur);
        depth++;
    }
    return false;
}

// Check if a node is inside an import statement
static bool is_inside_import(TSNode node, const CBMLangSpec *spec) {
    if (!spec->import_node_types || !spec->import_node_types[0]) {
        return false;
    }
    TSNode cur = ts_node_parent(node);
    int depth = 0;
    while (!ts_node_is_null(cur) && depth < MAX_PARENT_DEPTH) {
        if (cbm_kind_in_set(cur, spec->import_node_types)) {
            return true;
        }
        cur = ts_node_parent(cur);
        depth++;
    }
    return false;
}

// Is this an identifier-like node that represents a reference?
static bool is_reference_node(TSNode node, CBMLanguage lang) {
    const char *kind = ts_node_type(node);

    // Common identifier types across languages
    if (strcmp(kind, "identifier") == 0 || strcmp(kind, "simple_identifier") == 0 ||
        strcmp(kind, "type_identifier") == 0) {
        return true;
    }

    // Language-specific reference types
    switch (lang) {
    case CBM_LANG_GO:
        return strcmp(kind, "field_identifier") == 0 || strcmp(kind, "package_identifier") == 0;
    case CBM_LANG_PYTHON:
        return strcmp(kind, "attribute") == 0;
    case CBM_LANG_RUST:
        return strcmp(kind, "field_identifier") == 0 || strcmp(kind, "scoped_identifier") == 0;
    case CBM_LANG_HASKELL:
        return strcmp(kind, "variable") == 0 || strcmp(kind, "constructor") == 0;
    case CBM_LANG_OCAML:
        return strcmp(kind, "value_path") == 0 || strcmp(kind, "constructor_path") == 0;
    case CBM_LANG_ERLANG:
        return strcmp(kind, "atom") == 0 || strcmp(kind, "var") == 0;
    default:
        return false;
    }
}

// Check if a reference node is a definition name (the "name" field of its parent).
static bool is_definition_name(TSNode node) {
    TSNode parent = ts_node_parent(node);
    if (ts_node_is_null(parent)) {
        return false;
    }
    TSNode name_field = ts_node_child_by_field_name(parent, TS_FIELD("name"));
    return !ts_node_is_null(name_field) &&
           ts_node_start_byte(name_field) == ts_node_start_byte(node) &&
           ts_node_end_byte(name_field) == ts_node_end_byte(node);
}

// Try to emit a usage for a reference node. Returns early if the node should be skipped.
static void try_emit_usage(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    if (!is_reference_node(node, ctx->language)) {
        return;
    }
    if (is_inside_call(node, spec) || is_inside_import(node, spec)) {
        return;
    }
    if (is_definition_name(node)) {
        return;
    }
    char *name = cbm_node_text(ctx->arena, node, ctx->source);
    if (name && name[0] && !cbm_is_keyword(name, ctx->language)) {
        CBMUsage usage;
        usage.ref_name = name;
        usage.enclosing_func_qn = cbm_enclosing_func_qn_cached(ctx, node);
        cbm_usages_push(&ctx->result->usages, ctx->arena, usage);
    }
}

// Iterative usage walker — explicit stack
static void walk_usages(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, 4096);
    ts_nstack_push(&stack, ctx->arena, root);

    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        try_emit_usage(ctx, node, spec);
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - LAST_IDX; i >= 0; i--) {
            ts_nstack_push(&stack, ctx->arena, ts_node_child(node, (uint32_t)i));
        }
    }
}

void cbm_extract_usages(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec) {
        return;
    }

    walk_usages(ctx, ctx->root, spec);
}

// --- Unified handler: called once per node by the cursor walk ---
// Uses WalkState flags instead of parent-chain walks for O(1) context checks.

void handle_usages(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state) {
    (void)spec;
    if (!is_reference_node(node, ctx->language)) {
        return;
    }

    // Skip if inside a call (already counted as CALLS edge) — O(1) via state
    if (state->inside_call) {
        return;
    }
    // Skip if inside an import
    if (state->inside_import) {
        return;
    }

    // Skip if it's a definition name (left side of assignment, function name)
    TSNode parent = ts_node_parent(node);
    if (!ts_node_is_null(parent)) {
        TSNode name_field = ts_node_child_by_field_name(parent, TS_FIELD("name"));
        if (!ts_node_is_null(name_field) &&
            ts_node_start_byte(name_field) == ts_node_start_byte(node) &&
            ts_node_end_byte(name_field) == ts_node_end_byte(node)) {
            return;
        }
    }

    char *name = cbm_node_text(ctx->arena, node, ctx->source);
    if (name && name[0] && !cbm_is_keyword(name, ctx->language)) {
        CBMUsage usage;
        usage.ref_name = name;
        usage.enclosing_func_qn = state->enclosing_func_qn;
        cbm_usages_push(&ctx->result->usages, ctx->arena, usage);
    }
}
