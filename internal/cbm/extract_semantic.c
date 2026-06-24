#include "cbm.h"
#include "arena.h"
#include "helpers.h"
#include "lang_specs.h"
#include "extract_unified.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include "foundation/constants.h"
#include "extract_node_stack.h"

enum { MAX_EXCEPTION_NAME_LEN = 100, LAST_IDX = 1 };
#include <stdint.h> // uint32_t
#include <string.h>
#include <ctype.h>

// Field name length for ts_node_child_by_field_name() calls.
#define FIELD_LEN_CONSTRUCTOR 11 // strlen("constructor")

// --- Throw/Raise extraction ---

// Detect whether a node is a throw node for the given spec.
//
// Kotlin special case: this grammar models `throw X(...)` as a `jump_expression`
// (the same node also covers return/break/continue), NOT a `throw_expression`,
// so the spec's throw_node_types ("throw_expression") never matches.  We treat a
// Kotlin `jump_expression` as a throw only when its first child is the `throw`
// keyword, which excludes return/break/continue without false positives.
static bool is_throw_node(TSNode node, const CBMLangSpec *spec) {
    if (cbm_kind_in_set(node, spec->throw_node_types)) {
        return true;
    }
    if (spec->language == CBM_LANG_KOTLIN && strcmp(ts_node_type(node), "jump_expression") == 0) {
        uint32_t nc = ts_node_child_count(node);
        if (nc > 0 && strcmp(ts_node_type(ts_node_child(node, 0)), "throw") == 0) {
            return true;
        }
    }
    return false;
}

// Resolve exception name from the first meaningful child of a throw/raise node.
static char *resolve_exception_name(CBMArena *a, TSNode throw_node, const char *source) {
    uint32_t nc = ts_node_child_count(throw_node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(throw_node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "raise") == 0 || strcmp(ck, "throw") == 0) {
            continue;
        }
        if (ck[0] == ';' || ck[0] == '(' || ck[0] == ')') {
            continue;
        }

        if (strcmp(ck, "call") == 0 || strcmp(ck, "call_expression") == 0 ||
            strcmp(ck, "new_expression") == 0 || strcmp(ck, "object_creation_expression") == 0 ||
            strcmp(ck, "instance_expression") == 0) {
            TSNode fn = ts_node_child_by_field_name(child, TS_FIELD("function"));
            if (ts_node_is_null(fn)) {
                fn = ts_node_child_by_field_name(child, "constructor", FIELD_LEN_CONSTRUCTOR);
            }
            if (ts_node_is_null(fn)) {
                fn = ts_node_child_by_field_name(child, TS_FIELD("type"));
            }
            if (ts_node_is_null(fn) && ts_node_named_child_count(child) > 0) {
                fn = ts_node_named_child(child, 0);
            }
            if (!ts_node_is_null(fn)) {
                return cbm_node_text(a, fn, source);
            }
        } else {
            return cbm_node_text(a, child, source);
        }
        break;
    }
    return NULL;
}

// Extract exception types from a Java-style throws clause.
static void extract_throws_clause(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                                  const char *func_qn) {
    if (!spec->throws_clause_field || !spec->throws_clause_field[0]) {
        return;
    }
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "method_declaration") != 0 && strcmp(kind, "constructor_declaration") != 0) {
        return;
    }
    TSNode throws_clause = ts_node_child_by_field_name(node, spec->throws_clause_field,
                                                       (uint32_t)strlen(spec->throws_clause_field));
    if (ts_node_is_null(throws_clause)) {
        return;
    }
    uint32_t nc = ts_node_child_count(throws_clause);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(throws_clause, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "type_identifier") == 0 || strcmp(ck, "identifier") == 0 ||
            strcmp(ck, "scoped_type_identifier") == 0) {
            char *exc = cbm_node_text(ctx->arena, child, ctx->source);
            if (exc && exc[0]) {
                CBMThrow thr = {.exception_name = exc, .enclosing_func_qn = func_qn};
                cbm_throws_push(&ctx->result->throws, ctx->arena, thr);
            }
        }
    }
}

// Process a single node for throw extraction (called from iterative walker).
static void process_throw_node(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    if (is_throw_node(node, spec)) {
        char *exc_name = resolve_exception_name(ctx->arena, node, ctx->source);
        if (exc_name && exc_name[0]) {
            if (strlen(exc_name) > MAX_EXCEPTION_NAME_LEN) {
                exc_name[MAX_EXCEPTION_NAME_LEN] = '\0';
            }
            CBMThrow thr;
            thr.exception_name = exc_name;
            thr.enclosing_func_qn = cbm_enclosing_func_qn_cached(ctx, node);
            cbm_throws_push(&ctx->result->throws, ctx->arena, thr);
        }
    }

    extract_throws_clause(ctx, node, spec, cbm_enclosing_func_qn_cached(ctx, node));
}

// Iterative throw walker
static void walk_throws(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        process_throw_node(ctx, node, spec);
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - LAST_IDX; i >= 0; i--) {
            ts_nstack_push(&stack, ctx->arena, ts_node_child(node, (uint32_t)i));
        }
    }
}

// --- Read/Write detection (iterative) ---

// Resolve an assignment LHS node to the bare name being written.  Handles
// common wrappers so WRITES resolve for more languages than a raw identifier:
//   - expression_list (Go `x = ...` desugars left to expression_list[x])
//   - index/subscript (`cache[k] = v` → write the base var `cache`)
//   - field/member/selector access (`self.total = ...`, `obj.Field = ...` →
//     write the trailing field name `total`/`Field`)
// Returns NULL if no simple write target can be determined.
static char *resolve_lhs_write_name(CBMExtractCtx *ctx, TSNode left) {
    // Unwrap a single-element expression_list (Go).
    if (strcmp(ts_node_type(left), "expression_list") == 0) {
        if (ts_node_named_child_count(left) != 1) {
            return NULL; // multi-assign: ambiguous, skip
        }
        left = ts_node_named_child(left, 0);
    }
    const char *lk = ts_node_type(left);
    if (strcmp(lk, "identifier") == 0 || strcmp(lk, "simple_identifier") == 0) {
        return cbm_node_text(ctx->arena, left, ctx->source);
    }
    // Indexed write: write the base operand's identifier (`cache[k]` → cache).
    if (strcmp(lk, "index_expression") == 0 || strcmp(lk, "subscript_expression") == 0) {
        TSNode base = ts_node_child_by_field_name(left, TS_FIELD("operand"));
        if (ts_node_is_null(base)) {
            base = ts_node_child_by_field_name(left, TS_FIELD("object"));
        }
        if (ts_node_is_null(base) && ts_node_named_child_count(left) > 0) {
            base = ts_node_named_child(left, 0);
        }
        if (!ts_node_is_null(base)) {
            const char *bk = ts_node_type(base);
            if (strcmp(bk, "identifier") == 0 || strcmp(bk, "simple_identifier") == 0) {
                return cbm_node_text(ctx->arena, base, ctx->source);
            }
        }
        return NULL;
    }
    // Field/member write: write the trailing field name (`self.total` → total,
    // `obj.Field` → Field).  Covers Rust field_expression, C#/Java member access.
    if (strcmp(lk, "field_expression") == 0 || strcmp(lk, "member_access_expression") == 0 ||
        strcmp(lk, "field_access") == 0 || strcmp(lk, "selector_expression") == 0) {
        TSNode fld = ts_node_child_by_field_name(left, TS_FIELD("field"));
        if (ts_node_is_null(fld)) {
            fld = ts_node_child_by_field_name(left, TS_FIELD("name"));
        }
        if (!ts_node_is_null(fld)) {
            return cbm_node_text(ctx->arena, fld, ctx->source);
        }
        return NULL;
    }
    return NULL;
}

// Resolve the write target of a node in an assignment_node_types set.  For a
// plain assignment the target is the "left" field (or first child).  For an
// increment/decrement unary expression (`x++`, `++x`, C#
// postfix_/prefix_unary_expression) there is no "left" field and the operand
// may sit on either side of the operator token, so scan named children for the
// first identifier / member-style operand.  Returns a null node when no simple
// target is found.
static TSNode resolve_write_lhs_node(TSNode node) {
    TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
    if (!ts_node_is_null(left)) {
        return left;
    }
    const char *nk = ts_node_type(node);
    if (strcmp(nk, "postfix_unary_expression") == 0 || strcmp(nk, "prefix_unary_expression") == 0 ||
        strcmp(nk, "update_expression") == 0) {
        // Only ++/-- mutate their operand. Other unary postfix/prefix forms
        // (C# null-forgiving `x!`, address-of `&x`, deref `*x`, logical `!x`)
        // READ the operand — never treat them as writes.
        bool is_incdec = false;
        uint32_t total = ts_node_child_count(node);
        for (uint32_t i = 0; i < total; i++) {
            TSNode c = ts_node_child(node, i);
            if (ts_node_is_named(c)) {
                continue; // operator is an anonymous token
            }
            const char *op = ts_node_type(c);
            if (strcmp(op, "++") == 0 || strcmp(op, "--") == 0) {
                is_incdec = true;
                break;
            }
        }
        if (!is_incdec) {
            return (TSNode){0};
        }
        uint32_t cnc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < cnc; i++) {
            TSNode c = ts_node_named_child(node, i);
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "identifier") == 0 || strcmp(ck, "simple_identifier") == 0 ||
                strcmp(ck, "member_access_expression") == 0 ||
                strcmp(ck, "field_expression") == 0 || strcmp(ck, "field_access") == 0 ||
                strcmp(ck, "selector_expression") == 0 || strcmp(ck, "subscript_expression") == 0 ||
                strcmp(ck, "index_expression") == 0) {
                return c;
            }
        }
        return (TSNode){0};
    }
    if (ts_node_child_count(node) > 0) {
        return ts_node_child(node, 0);
    }
    return (TSNode){0};
}

// Try to emit a write for an assignment node.
static void try_emit_assignment_write(CBMExtractCtx *ctx, TSNode node, const char *func_qn) {
    TSNode left = resolve_write_lhs_node(node);
    if (ts_node_is_null(left)) {
        return;
    }
    char *name = resolve_lhs_write_name(ctx, left);
    if (name && name[0] && !cbm_is_keyword(name, ctx->language)) {
        CBMReadWrite rw;
        rw.var_name = name;
        rw.is_write = true;
        rw.enclosing_func_qn = func_qn;
        cbm_rw_push(&ctx->result->rw, ctx->arena, rw);
    }
}

static void walk_readwrites(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (cbm_kind_in_set(node, spec->assignment_node_types)) {
            try_emit_assignment_write(ctx, node, cbm_enclosing_func_qn_cached(ctx, node));
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - LAST_IDX; i >= 0; i--) {
            ts_nstack_push(&stack, ctx->arena, ts_node_child(node, (uint32_t)i));
        }
    }
}

void cbm_extract_semantic(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec) {
        return;
    }

    // Throws
    if ((spec->throw_node_types && spec->throw_node_types[0]) ||
        (spec->throws_clause_field && spec->throws_clause_field[0])) {
        walk_throws(ctx, ctx->root, spec);
    }

    // Reads/Writes
    if (spec->assignment_node_types && spec->assignment_node_types[0]) {
        walk_readwrites(ctx, ctx->root, spec);
    }
}

// --- Unified handlers ---

void handle_throws(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state) {
    bool has_throws = spec->throw_node_types && spec->throw_node_types[0];
    bool has_clause = spec->throws_clause_field && spec->throws_clause_field[0];
    if (!has_throws && !has_clause) {
        return;
    }

    if (has_throws && is_throw_node(node, spec)) {
        char *exc_name = resolve_exception_name(ctx->arena, node, ctx->source);
        if (exc_name && exc_name[0]) {
            if (strlen(exc_name) > MAX_EXCEPTION_NAME_LEN) {
                exc_name[MAX_EXCEPTION_NAME_LEN] = '\0';
            }
            CBMThrow thr;
            thr.exception_name = exc_name;
            thr.enclosing_func_qn = state->enclosing_func_qn;
            cbm_throws_push(&ctx->result->throws, ctx->arena, thr);
        }
    }

    extract_throws_clause(ctx, node, spec, state->enclosing_func_qn);
}

void handle_readwrites(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state) {
    if (!spec->assignment_node_types || !spec->assignment_node_types[0]) {
        return;
    }

    if (cbm_kind_in_set(node, spec->assignment_node_types)) {
        TSNode left = resolve_write_lhs_node(node);

        if (!ts_node_is_null(left)) {
            char *name = resolve_lhs_write_name(ctx, left);
            if (name && name[0] && !cbm_is_keyword(name, ctx->language)) {
                CBMReadWrite rw;
                rw.var_name = name;
                rw.is_write = true;
                rw.enclosing_func_qn = state->enclosing_func_qn;
                cbm_rw_push(&ctx->result->rw, ctx->arena, rw);
            }
        }
    }
}
