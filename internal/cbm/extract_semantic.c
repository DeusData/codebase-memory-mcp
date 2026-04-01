#include "cbm.h"
#include "arena.h"
#include "helpers.h"
#include "lang_specs.h"
#include "extract_unified.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include "foundation/constants.h"

enum { MAX_EXCEPTION_NAME_LEN = 100, LAST_IDX = 1 };
#include <stdint.h> // uint32_t
#include <string.h>
#include <ctype.h>

// Field name length for ts_node_child_by_field_name() calls.
#define FIELD_LEN_CONSTRUCTOR 11 // strlen("constructor")

// --- Throw/Raise extraction ---

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
    if (cbm_kind_in_set(node, spec->throw_node_types)) {
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
#define THROWS_STACK_CAP CBM_SZ_512
static void walk_throws(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    TSNode stack[THROWS_STACK_CAP];
    int top = 0;
    stack[top++] = root;
    while (top > 0) {
        TSNode node = stack[--top];
        process_throw_node(ctx, node, spec);
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - LAST_IDX; i >= 0 && top < THROWS_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

// --- Read/Write detection (iterative) ---

// Try to emit a write for an assignment node.
static void try_emit_assignment_write(CBMExtractCtx *ctx, TSNode node, const char *func_qn) {
    TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
    if (ts_node_is_null(left)) {
        if (ts_node_child_count(node) > 0) {
            left = ts_node_child(node, 0);
        }
    }
    if (ts_node_is_null(left)) {
        return;
    }
    const char *lk = ts_node_type(left);
    if (strcmp(lk, "identifier") != 0 && strcmp(lk, "simple_identifier") != 0) {
        return;
    }
    char *name = cbm_node_text(ctx->arena, left, ctx->source);
    if (name && name[0] && !cbm_is_keyword(name, ctx->language)) {
        CBMReadWrite rw;
        rw.var_name = name;
        rw.is_write = true;
        rw.enclosing_func_qn = func_qn;
        cbm_rw_push(&ctx->result->rw, ctx->arena, rw);
    }
}

#define READWRITE_STACK_CAP CBM_SZ_512
static void walk_readwrites(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    TSNode stack[READWRITE_STACK_CAP];
    int top = 0;
    stack[top++] = root;
    while (top > 0) {
        TSNode node = stack[--top];
        if (cbm_kind_in_set(node, spec->assignment_node_types)) {
            try_emit_assignment_write(ctx, node, cbm_enclosing_func_qn_cached(ctx, node));
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - LAST_IDX; i >= 0 && top < READWRITE_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
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

    if (has_throws && cbm_kind_in_set(node, spec->throw_node_types)) {
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
        TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
        if (ts_node_is_null(left)) {
            if (ts_node_child_count(node) > 0) {
                left = ts_node_child(node, 0);
            }
        }

        if (!ts_node_is_null(left)) {
            const char *lk = ts_node_type(left);
            if (strcmp(lk, "identifier") == 0 || strcmp(lk, "simple_identifier") == 0) {
                char *name = cbm_node_text(ctx->arena, left, ctx->source);
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
}
