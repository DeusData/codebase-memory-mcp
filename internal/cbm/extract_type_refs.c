#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_sprintf/strndup
#include "helpers.h"
#include "lang_specs.h"
#include "extract_unified.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include "foundation/constants.h"
#include "ts_node_stack.h"

enum { MIN_TYPE_LEN = 1 };

#include <stdint.h> // uint32_t
#include <string.h>
#include <ctype.h>

// Builtin types that should not generate USES_TYPE edges.
static bool is_builtin_type(const char *name) {
    if (!name || !name[0]) {
        return true;
    }
    if (strlen(name) <= MIN_TYPE_LEN) {
        return true;
    }

    // Common primitives
    if (strcmp(name, "int") == 0 || strcmp(name, "string") == 0 || strcmp(name, "bool") == 0 ||
        strcmp(name, "float") == 0 || strcmp(name, "float32") == 0 ||
        strcmp(name, "float64") == 0 || strcmp(name, "int8") == 0 || strcmp(name, "int16") == 0 ||
        strcmp(name, "int32") == 0 || strcmp(name, "int64") == 0 || strcmp(name, "uint") == 0 ||
        strcmp(name, "uint8") == 0 || strcmp(name, "uint16") == 0 || strcmp(name, "uint32") == 0 ||
        strcmp(name, "uint64") == 0 || strcmp(name, "uintptr") == 0 || strcmp(name, "byte") == 0 ||
        strcmp(name, "rune") == 0 || strcmp(name, "void") == 0 || strcmp(name, "char") == 0 ||
        strcmp(name, "double") == 0 || strcmp(name, "long") == 0 || strcmp(name, "short") == 0 ||
        strcmp(name, "unsigned") == 0 || strcmp(name, "error") == 0 || strcmp(name, "any") == 0 ||
        strcmp(name, "interface") == 0 || strcmp(name, "object") == 0 ||
        strcmp(name, "Object") == 0 || strcmp(name, "None") == 0 || strcmp(name, "nil") == 0 ||
        strcmp(name, "null") == 0 || strcmp(name, "undefined") == 0 ||
        strcmp(name, "number") == 0 || strcmp(name, "boolean") == 0 || strcmp(name, "str") == 0 ||
        strcmp(name, "dict") == 0 || strcmp(name, "list") == 0 || strcmp(name, "tuple") == 0 ||
        strcmp(name, "set") == 0 || strcmp(name, "complex128") == 0 ||
        strcmp(name, "complex64") == 0) {
        return true;
    }
    return false;
}

// Strip pointer/reference/slice/optional markers from a type name.
static const char *clean_type_name(CBMArena *a, const char *name) {
    if (!name || !name[0]) {
        return name;
    }
    // Skip leading *, &, [], ?
    while (*name == '*' || *name == '&' || *name == '?' || *name == '[' || *name == ']') {
        name++;
    }
    if (!*name) {
        return NULL;
    }
    // Take only the base type before any < or [
    size_t len = strlen(name);
    for (size_t i = 0; i < len; i++) {
        if (name[i] == '<' || name[i] == '[') {
            return cbm_arena_strndup(a, name, i);
        }
    }
    // Strip trailing ? (optionals)
    if (len > 0 && name[len - SKIP_ONE] == '?') {
        return cbm_arena_strndup(a, name, len - SKIP_ONE);
    }
    return name;
}

// Extract type name from a type annotation node.
static const char *extract_type_text(CBMArena *a, TSNode node, const char *source) {
    enum { MAX_UNWRAP = 8 };
    for (int depth = 0; depth < MAX_UNWRAP; depth++) {
        const char *kind = ts_node_type(node);
        if (strcmp(kind, "type_identifier") == 0 || strcmp(kind, "identifier") == 0 ||
            strcmp(kind, "simple_identifier") == 0 || strcmp(kind, "name") == 0) {
            return cbm_node_text(a, node, source);
        }
        if (strcmp(kind, "generic_type") == 0 || strcmp(kind, "parameterized_type") == 0) {
            if (ts_node_child_count(node) > 0) {
                return cbm_node_text(a, ts_node_child(node, 0), source);
            }
        }
        if (strcmp(kind, "pointer_type") == 0 || strcmp(kind, "reference_type") == 0 ||
            strcmp(kind, "slice_type") == 0 || strcmp(kind, "array_type") == 0) {
            TSNode elem = ts_node_child_by_field_name(node, TS_FIELD("element"));
            if (!ts_node_is_null(elem)) {
                node = elem;
                continue;
            }
            TSNode type_node = ts_node_child_by_field_name(node, TS_FIELD("type"));
            if (!ts_node_is_null(type_node)) {
                node = type_node;
                continue;
            }
        }
        break;
    }
    return clean_type_name(a, cbm_node_text(a, node, source));
}

// Add a type reference for a function.
static void add_type_ref(CBMExtractCtx *ctx, const char *type_name, const char *func_qn) {
    if (!type_name || !type_name[0]) {
        return;
    }
    type_name = clean_type_name(ctx->arena, type_name);
    if (!type_name || !type_name[0]) {
        return;
    }
    if (is_builtin_type(type_name)) {
        return;
    }

    CBMTypeRef tr;
    tr.type_name = type_name;
    tr.enclosing_func_qn = func_qn;
    cbm_typerefs_push(&ctx->result->type_refs, ctx->arena, tr);
}

// Extract parameter types from a parameters/formal_parameters node.
static void extract_param_type_refs(CBMExtractCtx *ctx, TSNode params, const char *func_qn) {
    uint32_t count = ts_node_child_count(params);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(params, i);
        TSNode type_node = ts_node_child_by_field_name(child, TS_FIELD("type"));
        if (!ts_node_is_null(type_node)) {
            const char *tname = extract_type_text(ctx->arena, type_node, ctx->source);
            add_type_ref(ctx, tname, func_qn);
        }
    }
}

// Extract return type references.
static void extract_return_type_refs(CBMExtractCtx *ctx, TSNode func_node, const char *func_qn) {
    const char *fields[] = {"result", "return_type", "type", NULL};
    for (const char **f = fields; *f; f++) {
        TSNode rt = ts_node_child_by_field_name(func_node, *f, (uint32_t)strlen(*f));
        if (!ts_node_is_null(rt)) {
            const char *tname = extract_type_text(ctx->arena, rt, ctx->source);
            add_type_ref(ctx, tname, func_qn);
            break;
        }
    }
}

// Extract type ref from a node whose "type" field contains the type.
static void extract_type_field_ref(CBMExtractCtx *ctx, TSNode node, const char *func_qn) {
    TSNode type_node = ts_node_child_by_field_name(node, TS_FIELD("type"));
    if (!ts_node_is_null(type_node)) {
        add_type_ref(ctx, extract_type_text(ctx->arena, type_node, ctx->source), func_qn);
    }
}

// Extract type refs from TS/JS type_arguments or type_annotation children.
static void extract_ts_body_type_refs(CBMExtractCtx *ctx, TSNode node, const char *kind,
                                      const char *func_qn) {
    if (strcmp(kind, "as_expression") == 0 || strcmp(kind, "satisfies_expression") == 0) {
        extract_type_field_ref(ctx, node, func_qn);
    } else if (strcmp(kind, "type_arguments") == 0) {
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_child(node, i);
            if (strcmp(ts_node_type(child), "type_identifier") == 0 ||
                strcmp(ts_node_type(child), "identifier") == 0) {
                add_type_ref(ctx, cbm_node_text(ctx->arena, child, ctx->source), func_qn);
            }
        }
    } else if (strcmp(kind, "variable_declarator") == 0) {
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_child(node, i);
            if (strcmp(ts_node_type(child), "type_annotation") == 0) {
                if (ts_node_child_count(child) > 0) {
                    TSNode inner = ts_node_child(child, ts_node_child_count(child) - SKIP_ONE);
                    add_type_ref(ctx, extract_type_text(ctx->arena, inner, ctx->source), func_qn);
                }
            }
        }
    }
}

// Extract type refs from Java generic_type children.
static void extract_java_body_type_refs(CBMExtractCtx *ctx, TSNode node, const char *kind,
                                        const char *func_qn) {
    if (strcmp(kind, "local_variable_declaration") == 0 || strcmp(kind, "cast_expression") == 0) {
        extract_type_field_ref(ctx, node, func_qn);
    } else if (strcmp(kind, "generic_type") == 0) {
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_child(node, i);
            if (strcmp(ts_node_type(child), "type_arguments") == 0) {
                uint32_t nta = ts_node_child_count(child);
                for (uint32_t j = 0; j < nta; j++) {
                    TSNode ta = ts_node_child(child, j);
                    if (strcmp(ts_node_type(ta), "type_identifier") == 0) {
                        add_type_ref(ctx, cbm_node_text(ctx->arena, ta, ctx->source), func_qn);
                    }
                }
            }
        }
    }
}

// Process a single node for body-level type references.
static void process_body_type_ref(CBMExtractCtx *ctx, TSNode node, const char *func_qn) {
    const char *kind = ts_node_type(node);

    switch (ctx->language) {
    case CBM_LANG_GO:
        if (strcmp(kind, "var_spec") == 0 || strcmp(kind, "type_assertion") == 0 ||
            strcmp(kind, "type_conversion_expression") == 0 ||
            strcmp(kind, "composite_literal") == 0) {
            extract_type_field_ref(ctx, node, func_qn);
        }
        break;
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
        extract_ts_body_type_refs(ctx, node, kind, func_qn);
        break;
    case CBM_LANG_JAVA:
        extract_java_body_type_refs(ctx, node, kind, func_qn);
        break;
    case CBM_LANG_PYTHON:
        if (strcmp(kind, "assignment") == 0) {
            TSNode type_node = ts_node_child_by_field_name(node, TS_FIELD("type"));
            if (!ts_node_is_null(type_node)) {
                add_type_ref(ctx, cbm_node_text(ctx->arena, type_node, ctx->source), func_qn);
            }
        }
        break;
    case CBM_LANG_RUST:
        if (strcmp(kind, "let_declaration") == 0 || strcmp(kind, "type_cast_expression") == 0) {
            extract_type_field_ref(ctx, node, func_qn);
        }
        break;
    default:
        break;
    }
}

// Walk function body for type references (casts, type assertions, local var types, generics).
static void walk_body_type_refs(CBMExtractCtx *ctx, TSNode root, const char *func_qn) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, 4096);
    ts_nstack_push(&stack, ctx->arena, root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        process_body_type_ref(ctx, node, func_qn);
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0; i--) {
            ts_nstack_push(&stack, ctx->arena, ts_node_child(node, (uint32_t)i));
        }
    }
}

// Walk AST for function nodes, extract type references from signatures and bodies.
/* Process a single function node: extract type refs from signature + body. */
static void process_func_type_refs(CBMExtractCtx *ctx, TSNode node) {
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name_node)) {
        return;
    }
    char *func_name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!func_name || !func_name[0]) {
        return;
    }
    const char *func_qn = cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, func_name);
    TSNode params = ts_node_child_by_field_name(node, TS_FIELD("parameters"));
    if (!ts_node_is_null(params)) {
        extract_param_type_refs(ctx, params, func_qn);
    }
    extract_return_type_refs(ctx, node, func_qn);
    TSNode body = ts_node_child_by_field_name(node, TS_FIELD("body"));
    if (ts_node_is_null(body)) {
        body = ts_node_child_by_field_name(node, TS_FIELD("block"));
    }
    if (!ts_node_is_null(body)) {
        walk_body_type_refs(ctx, body, func_qn);
    }
}

static void walk_type_refs(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    if (!spec->function_node_types || !spec->function_node_types[0]) {
        return;
    }

    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, 4096);
    ts_nstack_push(&stack, ctx->arena, root);

    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);

        if (cbm_kind_in_set(node, spec->function_node_types)) {
            process_func_type_refs(ctx, node);
            continue;
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0; i--) {
            ts_nstack_push(&stack, ctx->arena, ts_node_child(node, (uint32_t)i));
        }
    }
}

void cbm_extract_type_refs(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec) {
        return;
    }

    walk_type_refs(ctx, ctx->root, spec);
}

// --- Unified handler ---
// For function nodes: extract signature type refs (params + return).
// For body-level type-bearing nodes: extract body type refs.
// The cursor visits both, so this single handler replaces the old
// walk_type_refs + walk_body_type_refs split.

// Extract signature type refs from a function node.
static void extract_signature_type_refs(CBMExtractCtx *ctx, TSNode node, WalkState *state) {
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name_node)) {
        return;
    }
    char *func_name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!func_name || !func_name[0]) {
        return;
    }

    const char *func_qn;
    if (state->enclosing_class_qn) {
        func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", state->enclosing_class_qn, func_name);
    } else {
        func_qn = cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, func_name);
    }

    TSNode params = ts_node_child_by_field_name(node, TS_FIELD("parameters"));
    if (!ts_node_is_null(params)) {
        extract_param_type_refs(ctx, params, func_qn);
    }
    extract_return_type_refs(ctx, node, func_qn);
}

void handle_type_refs(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state) {
    if (!spec->function_node_types || !spec->function_node_types[0]) {
        return;
    }

    if (cbm_kind_in_set(node, spec->function_node_types)) {
        extract_signature_type_refs(ctx, node, state);
        return;
    }

    process_body_type_ref(ctx, node, state->enclosing_func_qn);
}
