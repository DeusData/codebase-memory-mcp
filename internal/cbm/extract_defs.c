#include "cbm.h"
#include "arena.h"   // CBMArena, cbm_arena_alloc/strdup/sprintf
#include "doclink.h" // cbm_doclink_note_doc_line
#include "helpers.h"
#include "lang_specs.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h" // cbm_log_error
#include "discover/test_conventions.h"
#include "foundation/sha256.h"
#include "foundation/mem_core.h" // cbm_realloc/cbm_free -- walk_defs stack
#include "extract_node_stack.h"
#include "simhash/minhash.h"
#include "semantic/ast_profile.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include <limits.h>          // INT_MAX
#include <stdint.h>          // uint32_t, SIZE_MAX
#include <stdio.h>           // snprintf (ObjectScript storage/trigger sidecars)
#include <stdlib.h>          // malloc/free (child-collection scratch)
#include <string.h>
#include <ctype.h>

// Buffer sizes for local arrays (base classes, params, return types).
#define MAX_COMMENT_LEN 500
#define MAX_BASES 16
#define MAX_BASES_MINUS_1 15
#define MAX_PARAMS CBM_SZ_32
#define MAX_PARAMS_MINUS_1 31
#define MAX_RETURN_TYPES 16
#define MAX_RETURN_TYPES_MINUS_1 15
#define MAX_ATTR_WRAPPERS 16 // stacked C#/PHP attribute_list siblings per declaration (#1692)
#define MAX_ATTR_WRAPPERS_MINUS_1 15

// Tree traversal limits.
enum {
    TEMPLATE_DEPTH_LIMIT = 4,
    DECLARATOR_DEPTH_LIMIT = CBM_DECLARATOR_DEPTH_LIMIT, // shared define in helpers.h

    EXPORT_ANCESTOR_DEPTH = 4,
    FUNC_PARENT_CLIMB_LIMIT = 4, /* fun_expr -> term -> uni_term -> let_binding (Nickel) */
    /* Nix header lambdas to descend before the file's body: `{ pkgs, ... }:` is one,
     * the nixpkgs overlay `final: prev:` is two. Bounded so a pathological chain
     * cannot spin. */
    NIX_HEADER_HOP_MAX = 8,
    DECORATOR_SCAN_LIMIT = 3,
    C_RETURN_WALK_DEPTH = 5,
    /* Declarator nesting around a member name: `char *(*(*tbl[4])(int))(void)`
     * is pointer, function, parens, pointer, function, parens, pointer, array. */
    C_FIELD_DECL_WALK_DEPTH = 16,
    VAR_RECURSION_LIMIT = 8,
    NESTED_CLASS_STACK_CAP = 128,
    FP_HASH_MUL = 31,    /* FNV-like hash multiplier for identifier dedup */
    FP_HASH_NONZERO = 1, /* OR mask to ensure hash is never zero (sentinel) */
    FP_SPACE_SEP = 1,    /* one byte for space separator between tokens */
};

/* Hash a span of source text. */
static uint32_t hash_source_span(const char *source, uint32_t start, int len) {
    uint32_t h = 0;
    for (int x = 0; x < len; x++) {
        h = (h * FP_HASH_MUL) + (uint32_t)(unsigned char)source[start + (uint32_t)x];
    }
    return h;
}

/* Try to append a unique identifier to the buffer. Returns true if appended. */
static bool try_append_ident(const char *source, uint32_t s, int len, uint32_t *seen, int seen_size,
                             uint32_t seen_mask, char *buf, int buf_size, int *pos, int *count) {
    uint32_t h = hash_source_span(source, s, len);
    uint32_t key = h | FP_HASH_NONZERO;
    uint32_t slot = h & seen_mask;
    bool dup = false;
    for (int p = 0; p < seen_size; p++) {
        uint32_t idx = (slot + (uint32_t)p) & seen_mask;
        if (seen[idx] == 0) {
            seen[idx] = key;
            break;
        }
        if (seen[idx] == key) {
            dup = true;
            break;
        }
    }
    if (dup || *pos + len + FP_SPACE_SEP >= buf_size) {
        return false;
    }
    if (*pos > 0) {
        buf[(*pos)++] = ' ';
    }
    memcpy(buf + *pos, source + s, (size_t)len);
    *pos += len;
    (*count)++;
    return true;
}

/* Walk AST body, collect unique identifier text as space-separated string.
 * Contract: the first BT_MAX_IDENTS unique identifiers in pre-order (source
 * order), bounded by BT_BUF bytes. The ident/byte caps ARE the sampling
 * contract; the pending stack is not — BT_STACK is only the on-stack first
 * chunk and spills to the heap, because a full fixed stack dropped a wide
 * body's FIRST children and sampled a mid-body window instead.
 * Returns arena-allocated string, or NULL when there are no identifiers or the
 * stack could not be grown (logged; no truncated sample is ever returned). */
static char *extract_body_ident_tokens(CBMExtractCtx *ctx, TSNode body) {
    enum { BT_STACK = 512, BT_BUF = 2048, BT_MAX_IDENTS = 128, BT_SEEN = 256, BT_SEEN_MASK = 255 };
    TSNode bt_inline[BT_STACK];
    TSNode *bt_stack = bt_inline;
    int bt_cap = BT_STACK;
    int bt_top = 0;
    bt_stack[bt_top++] = body;
    char bt_buf[BT_BUF];
    int bt_pos = 0;
    uint32_t bt_seen[BT_SEEN];
    memset(bt_seen, 0, sizeof(bt_seen));
    int bt_count = 0;

    while (bt_top > 0 && bt_count < BT_MAX_IDENTS) {
        TSNode nd = bt_stack[--bt_top];
        uint32_t nc = ts_node_child_count(nd);
        if (nc == 0) {
            const char *k = ts_node_type(nd);
            if (strcmp(k, "identifier") == 0 || strcmp(k, "field_identifier") == 0 ||
                strcmp(k, "property_identifier") == 0 || strcmp(k, "type_identifier") == 0 ||
                strcmp(k, "objectscript_identifier") == 0 ||
                strcmp(k, "objectscript_identifier_special") == 0 ||
                strcmp(k, "identifier_segment_immediate") == 0 ||
                strcmp(k, "identifier_segment_immediate_special") == 0 ||
                strcmp(k, "class_name") == 0 || strcmp(k, "method_name") == 0 ||
                strcmp(k, "routine_name") == 0 || strcmp(k, "quote_permitting_identifier") == 0) {
                uint32_t s = ts_node_start_byte(nd);
                int len = (int)(ts_node_end_byte(nd) - s);
                if (len > 0 && len < CBM_SZ_64 && s < (uint32_t)ctx->source_len) {
                    try_append_ident(ctx->source, s, len, bt_seen, BT_SEEN, BT_SEEN_MASK, bt_buf,
                                     BT_BUF, &bt_pos, &bt_count);
                }
            }
        } else {
            if (!cbm_walk_stack_reserve((void **)&bt_stack, &bt_cap, bt_top + (int)nc,
                                        sizeof(TSNode), bt_inline, "body_ident_tokens")) {
                bt_pos = 0; /* sentinel: no sample rather than a truncated one */
                break;
            }
            for (int i = (int)nc - SKIP_ONE; i >= 0; i--) {
                bt_stack[bt_top++] = ts_node_child(nd, (uint32_t)i);
            }
        }
    }
    cbm_walk_stack_release(bt_stack, bt_inline);
    if (bt_pos > 0) {
        bt_buf[bt_pos] = '\0';
        return cbm_arena_strdup(ctx->arena, bt_buf);
    }
    return NULL;
}

/* Compute MinHash fingerprint for a function body node and store in def.
 * Sets def->fingerprint (arena-allocated) and def->fingerprint_k on success,
 * leaves them NULL/0 if the body is too short. */
static void compute_fingerprint(CBMExtractCtx *ctx, CBMDefinition *def, TSNode func_node) {
    /* Find the function body child */
    TSNode body = ts_node_child_by_field_name(func_node, TS_FIELD("body"));
    if (ts_node_is_null(body)) {
        body = func_node;
    }
    /* Extract raw identifier tokens from body for semantic search — before MinHash gate
     * so short functions still get body_tokens even without a fingerprint. */
    def->body_tokens = extract_body_ident_tokens(ctx, body);

    cbm_minhash_t result;
    if (!cbm_minhash_compute(body, ctx->source, (int)ctx->language, &result)) {
        return; /* Too short or empty — no fingerprint */
    }
    /* Arena-allocate the fingerprint array */
    uint32_t *fp = cbm_arena_alloc(ctx->arena, CBM_MINHASH_K * sizeof(uint32_t));
    if (!fp) {
        return;
    }
    memcpy(fp, result.values, CBM_MINHASH_K * sizeof(uint32_t));
    def->fingerprint = fp;
    def->fingerprint_k = CBM_MINHASH_K;

    /* AST structural profile (signals 8, 9, 11) — rides the same body node */
    cbm_ast_profile_t profile;
    int pc = 0;
    if (def->param_names) {
        while (def->param_names[pc]) {
            pc++;
        }
    }
    if (cbm_ast_profile_compute(body, ctx->source, def->param_names, pc, &profile)) {
        profile.body_lines = (uint16_t)def->lines;
        char sp_buf[CBM_AST_PROFILE_BUF];
        cbm_ast_profile_to_str(&profile, sp_buf, sizeof(sp_buf));
        def->structural_profile = cbm_arena_strdup(ctx->arena, sp_buf);
    }
}

// Tree-sitter row is 0-based; lines are 1-based.

// Null-terminated array allocation: need count + 1 for terminator.
enum { NULL_TERM = 1 };

// String operations.
enum {
    SKIP_CHAR = 1,        // skip one character (dot, quote, prefix)
    PAIR_CHARS = 2,       // pair of delimiters (quotes, parens)
    SECOND_CHILD_IDX = 1, // index of second child
    FIRST_LINE = 1,       // first line number
};

// Return type pair array size.
enum { RT_PAIR_SIZE = 2 };

// Forward declarations
static void extract_func_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);
static void extract_class_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);
static void emit_class_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                           const char *kind, char *name);
static void extract_c_typedef(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);
static void walk_defs(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec, int depth_unused);
static void extract_variables(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec);
static void extract_var_names(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);
static void extract_class_variables(CBMExtractCtx *ctx, TSNode class_node, const char *class_qn,
                                    const CBMLangSpec *spec, int fields_from);
static void fence_fields_named_like_methods(CBMExtractCtx *ctx, int methods_from, int fields_from);
static void extract_rust_impl(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);
static void extract_class_methods(CBMExtractCtx *ctx, TSNode class_node, const char *class_qn,
                                  const CBMLangSpec *spec);
static void extract_class_fields(CBMExtractCtx *ctx, TSNode class_node, const char *class_qn,
                                 const CBMLangSpec *spec);
static TSNode find_class_body(TSNode class_node, CBMLanguage lang);
static void extract_enum_members(CBMExtractCtx *ctx, TSNode node, const char *class_qn);
static void extract_elixir_call(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);

// --- Helpers ---

// Get "name" field from a node
static TSNode func_name_node(TSNode node) {
    TSNode name = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name)) {
        /* Protobuf rpc: name is in rpc_name child, not "name" field */
        name = cbm_find_child_by_kind(node, "rpc_name");
    }
    return name;
}

// Lua: resolve anonymous function assignment name from parent assignment_statement.
static TSNode resolve_lua_func_name(TSNode node) {
    TSNode parent = ts_node_parent(node);
    if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "expression_list") == 0) {
        parent = ts_node_parent(parent);
    }
    if (ts_node_is_null(parent) || strcmp(ts_node_type(parent), "assignment_statement") != 0) {
        TSNode null_node = {0};
        return null_node;
    }
    TSNode vars = ts_node_child_by_field_name(parent, TS_FIELD("variables"));
    if (ts_node_is_null(vars)) {
        uint32_t n = ts_node_child_count(parent);
        for (uint32_t i = 0; i < n; i++) {
            TSNode c = ts_node_child(parent, i);
            if (strcmp(ts_node_type(c), "variable_list") == 0) {
                vars = c;
                break;
            }
        }
    }
    if (!ts_node_is_null(vars) && ts_node_child_count(vars) > 0) {
        return ts_node_child(vars, 0);
    }
    TSNode null_node = {0};
    return null_node;
}

// Julia: walk named children to find first identifier.
static TSNode resolve_julia_func_name(TSNode node) {
    TSNode current = node;
    for (int depth = 0; depth < TEMPLATE_DEPTH_LIMIT; depth++) {
        if (ts_node_named_child_count(current) == 0) {
            break;
        }
        TSNode first = ts_node_named_child(current, 0);
        const char *fk = ts_node_type(first);
        if (strcmp(fk, "identifier") == 0 || strcmp(fk, "operator_identifier") == 0) {
            return first;
        }
        current = first;
    }
    TSNode null_node = {0};
    return null_node;
}

// OCaml: resolve value_definition name from let_binding→pattern.
static TSNode resolve_ocaml_func_name(TSNode node) {
    TSNode binding = cbm_find_child_by_kind(node, "let_binding");
    if (!ts_node_is_null(binding)) {
        TSNode pattern = ts_node_child_by_field_name(binding, TS_FIELD("pattern"));
        if (!ts_node_is_null(pattern)) {
            return pattern;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Last identifier (DFS pre-order) under `node`. For a schema-qualified
// object_reference (schema.table) this is the table name; the schema prefix is
// ignored. Leaves *found false and returns `best` unchanged if none is present.
static TSNode sql_last_identifier(TSNode node, TSNode best, bool *found) {
    if (strcmp(ts_node_type(node), "identifier") == 0) {
        best = node;
        *found = true;
    }
    uint32_t cc = ts_node_child_count(node);
    for (uint32_t i = 0; i < cc; i++) {
        best = sql_last_identifier(ts_node_child(node, i), best, found);
    }
    return best;
}

// SQL: resolve create_function / create_table / create_view name. The name sits
// on an object_reference; for a schema-qualified name (schema.table) take the
// last identifier (the table), not the first (the schema).
static TSNode resolve_sql_func_name(TSNode node) {
    TSNode obj_ref = cbm_find_child_by_kind(node, "object_reference");
    if (!ts_node_is_null(obj_ref)) {
        bool found = false;
        TSNode empty = {0};
        TSNode id = sql_last_identifier(obj_ref, empty, &found);
        if (found) {
            return id;
        }
    }
    return cbm_find_child_by_kind(node, "identifier");
}

// Zig: resolve test_declaration name from string→string_content.
static TSNode resolve_zig_test_name(TSNode node) {
    TSNode str_node = cbm_find_child_by_kind(node, "string");
    if (!ts_node_is_null(str_node)) {
        TSNode content = cbm_find_child_by_kind(str_node, "string_content");
        if (!ts_node_is_null(content)) {
            return content;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// VimScript: resolve function_definition name from function_declaration child.
static TSNode resolve_vimscript_func_name(TSNode node) {
    TSNode decl = cbm_find_child_by_kind(node, "function_declaration");
    if (!ts_node_is_null(decl) && ts_node_named_child_count(decl) > 0) {
        return ts_node_named_child(decl, 0);
    }
    if (ts_node_named_child_count(node) > 0) {
        return ts_node_named_child(node, 0);
    }
    TSNode null_node = {0};
    return null_node;
}

// Resolve function name for scripting/niche languages (Lua, OCaml, SQL, Zig, VimScript, Julia).
static TSNode resolve_func_name_scripting(TSNode node, CBMLanguage lang, const char *kind) {
    if (lang == CBM_LANG_LUA && strcmp(kind, "function_definition") == 0) {
        return resolve_lua_func_name(node);
    }
    if (lang == CBM_LANG_OCAML && strcmp(kind, "value_definition") == 0) {
        return resolve_ocaml_func_name(node);
    }
    if (lang == CBM_LANG_SQL && strcmp(kind, "create_function") == 0) {
        return resolve_sql_func_name(node);
    }
    if (lang == CBM_LANG_ZIG && strcmp(kind, "test_declaration") == 0) {
        return resolve_zig_test_name(node);
    }
    if (lang == CBM_LANG_VIMSCRIPT && strcmp(kind, "function_definition") == 0) {
        return resolve_vimscript_func_name(node);
    }
    if (lang == CBM_LANG_JULIA && strcmp(kind, "function_definition") == 0) {
        return resolve_julia_func_name(node);
    }
    /* Julia short-form `name(args) = body` parses as an `assignment` whose LHS is
     * a call_expression (`name(args)`); the function name is that call's head
     * identifier. A plain `x = 5` (non-call LHS) is not a function — resolve NULL
     * so it is neither extracted as a def nor scoped. */
    if (lang == CBM_LANG_JULIA && strcmp(kind, "assignment") == 0) {
        if (ts_node_named_child_count(node) > 0) {
            TSNode lhs = ts_node_named_child(node, 0);
            if (!ts_node_is_null(lhs) && strcmp(ts_node_type(lhs), "call_expression") == 0) {
                return resolve_julia_func_name(lhs);
            }
        }
    }

    TSNode null_node = {0};
    return null_node;
}

// Lean: resolve function name from declId field.
static TSNode resolve_lean_func_name(TSNode node, TSNode name) {
    TSNode decl_id = ts_node_child_by_field_name(node, TS_FIELD("declId"));
    if (!ts_node_is_null(decl_id)) {
        TSNode id = cbm_find_child_by_kind(decl_id, "ident");
        if (!ts_node_is_null(id)) {
            return id;
        }
        if (ts_node_named_child_count(decl_id) > 0) {
            return ts_node_named_child(decl_id, 0);
        }
        return decl_id;
    }
    if (!ts_node_is_null(name)) {
        return name;
    }
    return cbm_find_child_by_kind(node, "ident");
}

// Haskell: resolve function name from first named child (variable/name).
static TSNode resolve_haskell_func_name(TSNode node) {
    if (ts_node_named_child_count(node) > 0) {
        TSNode head = ts_node_named_child(node, 0);
        const char *hk = ts_node_type(head);
        if (strcmp(hk, "variable") == 0 || strcmp(hk, "name") == 0) {
            return head;
        }
        if (ts_node_named_child_count(head) > 0) {
            TSNode v = ts_node_named_child(head, 0);
            const char *vk = ts_node_type(v);
            if (strcmp(vk, "variable") == 0 || strcmp(vk, "name") == 0) {
                return v;
            }
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// CommonLisp: resolve defun name from function_name field or defun_header→sym_lit.
static TSNode resolve_commonlisp_func_name(TSNode node) {
    TSNode fn = ts_node_child_by_field_name(node, TS_FIELD("function_name"));
    if (!ts_node_is_null(fn)) {
        return fn;
    }
    TSNode header = cbm_find_child_by_kind(node, "defun_header");
    if (!ts_node_is_null(header)) {
        return cbm_find_child_by_kind(header, "sym_lit");
    }
    TSNode null_node = {0};
    return null_node;
}

// Makefile: resolve rule name from targets child or word fallback.
static TSNode resolve_makefile_func_name(TSNode node) {
    TSNode targets = cbm_find_child_by_kind(node, "targets");
    if (!ts_node_is_null(targets) && ts_node_named_child_count(targets) > 0) {
        return ts_node_named_child(targets, 0);
    }
    return cbm_find_child_by_kind(node, "word");
}

// Elm: resolve value_declaration name from functionDeclarationLeft field.
static TSNode resolve_elm_func_name(TSNode node) {
    TSNode fdl = ts_node_child_by_field_name(node, TS_FIELD("functionDeclarationLeft"));
    if (ts_node_is_null(fdl)) {
        fdl = cbm_find_child_by_kind(node, "function_declaration_left");
    }
    if (!ts_node_is_null(fdl) && ts_node_named_child_count(fdl) > 0) {
        return ts_node_named_child(fdl, 0);
    }
    TSNode null_node = {0};
    return null_node;
}

// Wolfram: resolve set/set_delayed name from LHS apply→symbol.
// The defined symbol's head can be a user_symbol (lowercase user names) OR a
// builtin_symbol (capitalized names like Square/Cube, which the grammar tags as
// builtin even when user-defined). For a bare `Name = value` (set_top with no
// apply), the LHS is the symbol itself. Accept all three forms so multiple
// defs in one file each resolve to a distinct name instead of collapsing.
static TSNode resolve_wolfram_func_name(TSNode node) {
    if (ts_node_named_child_count(node) > 0) {
        TSNode lhs = ts_node_named_child(node, 0);
        const char *lk = ts_node_type(lhs);
        if (strcmp(lk, "apply") == 0 && ts_node_named_child_count(lhs) > 0) {
            TSNode head = ts_node_named_child(lhs, 0);
            const char *hk = ts_node_type(head);
            if (strcmp(hk, "user_symbol") == 0 || strcmp(hk, "builtin_symbol") == 0) {
                return head;
            }
        } else if (strcmp(lk, "user_symbol") == 0 || strcmp(lk, "builtin_symbol") == 0) {
            return lhs;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Resolve function name for FP/scientific languages.
static TSNode resolve_func_name_fp(TSNode node, CBMLanguage lang, const char *kind, TSNode name) {
    if (lang == CBM_LANG_COMMONLISP && strcmp(kind, "defun") == 0) {
        return resolve_commonlisp_func_name(node);
    }

    if (lang == CBM_LANG_MAKEFILE && strcmp(kind, "rule") == 0) {
        return resolve_makefile_func_name(node);
    }

    if (lang == CBM_LANG_HASKELL && strcmp(kind, "function") == 0) {
        return resolve_haskell_func_name(node);
    }

    if (lang == CBM_LANG_ELM && strcmp(kind, "value_declaration") == 0) {
        return resolve_elm_func_name(node);
    }

    if (lang == CBM_LANG_MATLAB && strcmp(kind, "function_definition") == 0) {
        if (!ts_node_is_null(name)) {
            return name;
        }
        return cbm_find_child_by_kind(node, "identifier");
    }

    if (lang == CBM_LANG_LEAN) {
        return resolve_lean_func_name(node, name);
    }

    if (lang == CBM_LANG_WOLFRAM &&
        (strcmp(kind, "set_delayed_top") == 0 || strcmp(kind, "set_top") == 0 ||
         strcmp(kind, "set_delayed") == 0 || strcmp(kind, "set") == 0)) {
        return resolve_wolfram_func_name(node);
    }

    TSNode null_node = {0};
    return null_node;
}

// C++/CUDA: out-of-line method definitions name the function with a qualified
// declarator (`Foo::bar`, or `ns::Foo::bar`). Return the immediate enclosing
// class name (the scope segment directly left of the function name, e.g. "Foo"),
// or NULL when the declarator is unqualified (a plain free function). Without
// this, an out-of-line definition — whose class body lives declaration-only in a
// header — would be recorded as a free Function with no link to its class.
char *cbm_cpp_out_of_line_parent_class(CBMArena *a, TSNode node, const char *source) {
    // Descend the declarator chain to its qualified_identifier, if any.
    TSNode qid = {0};
    TSNode decl = ts_node_child_by_field_name(node, TS_FIELD("declarator"));
    for (int depth = 0; depth < DECLARATOR_DEPTH_LIMIT && !ts_node_is_null(decl); depth++) {
        const char *dk = ts_node_type(decl);
        if (strcmp(dk, "qualified_identifier") == 0 || strcmp(dk, "scoped_identifier") == 0) {
            if (cbm_c_qualifier_is_recovered(decl)) {
                /* `API RetT name(...)` misread as `RetT::name` (MISSING "::"):
                 * RetT is the return type, not a class. Keep looking for a real
                 * qualifier on the name side. */
                decl = ts_node_child_by_field_name(decl, TS_FIELD("name"));
                continue;
            }
            qid = decl;
            break;
        }
        TSNode inner = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
        if (ts_node_is_null(inner) && ts_node_named_child_count(decl) > 0) {
            inner = ts_node_named_child(decl, 0);
        }
        if (ts_node_is_null(inner)) {
            break;
        }
        decl = inner;
    }
    if (ts_node_is_null(qid)) {
        return NULL;
    }
    // The qualified_identifier's `scope` is the parent. For a nested scope
    // (`ns::Foo`) descend through its `name` field to the innermost segment so
    // the direct parent ("Foo") is returned, not the outer namespace.
    TSNode scope = ts_node_child_by_field_name(qid, TS_FIELD("scope"));
    if (ts_node_is_null(scope)) {
        return NULL;
    }
    for (int depth = 0; depth < DECLARATOR_DEPTH_LIMIT; depth++) {
        const char *sk = ts_node_type(scope);
        if (strcmp(sk, "qualified_identifier") != 0 && strcmp(sk, "scoped_identifier") != 0) {
            break;
        }
        TSNode name = ts_node_child_by_field_name(scope, TS_FIELD("name"));
        if (ts_node_is_null(name)) {
            break;
        }
        scope = name;
    }
    char *text = cbm_node_text(a, scope, source);
    return (text && text[0]) ? text : NULL;
}

// R: resolve function_definition name from parent binary_operator lhs.
static TSNode resolve_r_func_name(TSNode node) {
    TSNode parent = ts_node_parent(node);
    if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "binary_operator") == 0) {
        TSNode lhs = ts_node_child_by_field_name(parent, TS_FIELD("left"));
        if (ts_node_is_null(lhs)) {
            lhs = ts_node_child_by_field_name(parent, TS_FIELD("lhs"));
        }
        if (ts_node_is_null(lhs) && ts_node_named_child_count(parent) > 0) {
            lhs = ts_node_named_child(parent, 0);
        }
        if (!ts_node_is_null(lhs)) {
            return lhs;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Max descent for the (System)Verilog name-wrapper search (module/class headers
// nest the identifier a few levels deep; bounding avoids walking large subtrees
// like port lists).
enum { CBM_DESCENDANT_MAX_DEPTH = 6 };

// Verilog/SystemVerilog: find the first descendant node of the given kind in
// pre-order (depth-bounded). The (System)Verilog grammar has FIELD_COUNT 0, so
// def names live on nested *_identifier wrappers reachable only by node kind.
// Tree-sitter trees are acyclic, so the bounded recursion always terminates.
static TSNode find_first_descendant_by_kind(TSNode node,
                                            const char *kind, // NOLINT(misc-no-recursion)
                                            int max_depth) {
    if (max_depth < 0 || ts_node_is_null(node)) {
        TSNode null_node = {0};
        return null_node;
    }
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(child), kind) == 0) {
            return child;
        }
        TSNode found = find_first_descendant_by_kind(child, kind, max_depth - 1);
        if (!ts_node_is_null(found)) {
            return found;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Forward declaration for mutual recursion. Exported (see helpers.h) so the
// unified/calls extractor shares this one resolver — see cbm_resolve_func_name.
TSNode cbm_resolve_func_name(TSNode node, CBMLanguage lang);

static bool is_cpp_template_inner_kind(const char *kind) {
    return strcmp(kind, "function_definition") == 0 || strcmp(kind, "declaration") == 0 ||
           strcmp(kind, "field_declaration") == 0;
}

// C++/CUDA: find inner function/declaration inside template_declaration.
// Returns the inner node (not the resolved name) to break the recursive cycle.
static TSNode find_cpp_template_inner_node(TSNode node, CBMLanguage lang) {
    if ((lang != CBM_LANG_CPP && lang != CBM_LANG_CUDA) ||
        strcmp(ts_node_type(node), "template_declaration") != 0) {
        return node;
    }

    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode ch = ts_node_named_child(node, i);
        const char *ck = ts_node_type(ch);
        if (is_cpp_template_inner_kind(ck)) {
            return ch;
        }
        if (strcmp(ck, "template_declaration") == 0) {
            TSNode nested = find_cpp_template_inner_node(ch, lang);
            if (!ts_node_is_null(nested) && !ts_node_eq(nested, ch)) {
                return nested;
            }
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Try arrow_function name via parent variable_declarator (top-level resolution)
// or object-literal property (`pair` key) — the latter covers factory functions
// that return an object of arrow methods (the Zustand actions-slice pattern, #341).
static TSNode resolve_toplevel_arrow_name(TSNode node, const char *kind) {
    if (strcmp(kind, "arrow_function") != 0) {
        TSNode null_node = {0};
        return null_node;
    }
    TSNode parent = ts_node_parent(node);
    if (ts_node_is_null(parent)) {
        TSNode null_node = {0};
        return null_node;
    }
    const char *pk = ts_node_type(parent);
    if (strcmp(pk, "variable_declarator") == 0 || strcmp(pk, "public_field_definition") == 0) {
        /* `const f = () => {}` and the class-field form `f = () => {}` both name
         * the arrow via the parent's `name` child (#new_ts_class_field_arrow):
         * resolving it lets push_boundary_scopes push a SCOPE_FUNC so in-body
         * calls source to the method, not the enclosing class/module. */
        return ts_node_child_by_field_name(parent, TS_FIELD("name"));
    }
    if (strcmp(pk, "field_definition") == 0) {
        return ts_node_child_by_field_name(parent, TS_FIELD("property"));
    }
    if (strcmp(pk, "pair") == 0) {
        return ts_node_child_by_field_name(parent, TS_FIELD("key"));
    }
    TSNode null_node = {0};
    return null_node;
}

// Try C/C++/CUDA/GLSL function_definition declarator name or template unwrap.
static TSNode resolve_func_name_c_family(TSNode *node_ptr, CBMLanguage lang, const char *kind) {
    if ((lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA) &&
        strcmp(kind, "template_declaration") == 0) {
        TSNode inner = find_cpp_template_inner_node(*node_ptr, lang);
        if (!ts_node_is_null(inner)) {
            *node_ptr = inner; /* signal caller to retry */
        }
        TSNode null_node = {0};
        return null_node;
    }
    if ((lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA ||
         lang == CBM_LANG_GLSL || lang == CBM_LANG_HLSL || lang == CBM_LANG_ISPC ||
         lang == CBM_LANG_SLANG || lang == CBM_LANG_OBJC) &&
        strcmp(kind, "function_definition") == 0) {
        /* Objective-C top-level C functions (`static int helper(int x) {...}`)
         * have the same declarator structure as C — without this they get no
         * name node and are dropped, so a call to them never resolves an edge. */
        return cbm_resolve_c_declarator_name_node(*node_ptr);
    }
    TSNode null_node = {0};
    return null_node;
}

// Resolve the name node for a function, handling language-specific quirks.
// Uses a loop to handle template_declaration unwrapping (avoids recursion).
TSNode cbm_resolve_func_name(TSNode node, CBMLanguage lang) {
    enum { MAX_TEMPLATE_DEPTH = 2 };
    for (int tmpl_depth = 0; tmpl_depth < MAX_TEMPLATE_DEPTH; tmpl_depth++) {
        const char *kind = ts_node_type(node);

        if (lang == CBM_LANG_HASKELL && strcmp(kind, "signature") == 0) {
            TSNode null_node = {0};
            return null_node;
        }

        // A parameterized ObjectScript routine wraps its tag and body in a
        // procedure node. Use the direct tag as the callable name so the
        // definition spans the complete procedure instead of only its label.
        if (lang == CBM_LANG_OBJECTSCRIPT_ROUTINE &&
            (strcmp(kind, "tag") == 0 || strcmp(kind, "procedure") == 0)) {
            return strcmp(kind, "tag") == 0 ? node : cbm_find_child_by_kind(node, "tag");
        }
        // ObjectScript method/classmethod: name lives under method_definition ->
        // method_name -> first named child.
        if (lang == CBM_LANG_OBJECTSCRIPT_UDL &&
            (strcmp(kind, "method") == 0 || strcmp(kind, "classmethod") == 0)) {
            TSNode mdef = cbm_find_child_by_kind(node, "method_definition");
            if (!ts_node_is_null(mdef)) {
                TSNode mname = cbm_find_child_by_kind(mdef, "method_name");
                if (!ts_node_is_null(mname) && ts_node_named_child_count(mname) > 0) {
                    return ts_node_named_child(mname, 0);
                }
            }
            TSNode null_node = {0};
            return null_node;
        }
        if (lang == CBM_LANG_OBJECTSCRIPT_UDL && strcmp(kind, "query") == 0) {
            return cbm_find_child_by_kind(node, "query_name");
        }

        TSNode name = func_name_node(node);

        if (lang == CBM_LANG_R && strcmp(kind, "function_definition") == 0) {
            return resolve_r_func_name(node);
        }

        if (!ts_node_is_null(name)) {
            return name;
        }

        /* Swift and newer tree-sitter-kotlin: function_declaration has no `name`
         * field; the function name is a `simple_identifier` child. A Swift
         * protocol requirement (a bodyless `func` inside a protocol) is a
         * separate node type with the same shape; it is named here as well as in
         * resolve_method_name because swift_func_types now admits it, so it can
         * reach the free-function path too and would otherwise land unnamed. */
        if ((lang == CBM_LANG_SWIFT || lang == CBM_LANG_KOTLIN) &&
            (strcmp(kind, "function_declaration") == 0 ||
             strcmp(kind, "protocol_function_declaration") == 0)) {
            TSNode si = cbm_find_child_by_kind(node, "simple_identifier");
            if (!ts_node_is_null(si)) {
                return si;
            }
        }

        // PowerShell function_statement has no `name` field; the name is a
        // `function_name` child node (#35).
        if (lang == CBM_LANG_POWERSHELL && strcmp(kind, "function_statement") == 0) {
            TSNode fn = cbm_find_child_by_kind(node, "function_name");
            if (!ts_node_is_null(fn)) {
                return fn;
            }
        }

        /* Cairo / D / Odin / Squirrel: the def node has no `name` field; the name
         * is a plain `identifier` child (same shape as the Swift/Kotlin case). */
        if ((lang == CBM_LANG_CAIRO || lang == CBM_LANG_DLANG || lang == CBM_LANG_ODIN ||
             lang == CBM_LANG_SQUIRREL) &&
            (strcmp(kind, "function_definition") == 0 || strcmp(kind, "function_signature") == 0 ||
             strcmp(kind, "function_declaration") == 0 ||
             strcmp(kind, "procedure_declaration") == 0)) {
            TSNode id = cbm_find_child_by_kind(node, "identifier");
            if (!ts_node_is_null(id)) {
                return id;
            }
        }

        /* Ada: subprogram_body/_declaration carry the `name` field on a nested
         * procedure_specification/function_specification child, not on themselves. */
        if (lang == CBM_LANG_ADA &&
            (strcmp(kind, "subprogram_body") == 0 || strcmp(kind, "subprogram_declaration") == 0)) {
            TSNode spec = cbm_find_child_by_kind(node, "procedure_specification");
            if (ts_node_is_null(spec)) {
                spec = cbm_find_child_by_kind(node, "function_specification");
            }
            if (!ts_node_is_null(spec)) {
                TSNode nm = ts_node_child_by_field_name(spec, TS_FIELD("name"));
                if (!ts_node_is_null(nm)) {
                    return nm;
                }
            }
        }

        /* Pascal: defProc carries the `name` field on its `header` (declProc) child. */
        if (lang == CBM_LANG_PASCAL && strcmp(kind, "defProc") == 0) {
            TSNode hdr = ts_node_child_by_field_name(node, TS_FIELD("header"));
            if (!ts_node_is_null(hdr)) {
                TSNode nm = ts_node_child_by_field_name(hdr, TS_FIELD("name"));
                if (!ts_node_is_null(nm)) {
                    return nm;
                }
            }
        }

        /* Just: a `recipe` carries its name on the nested `recipe_header`'s
         * `name` field (an identifier), not on the recipe node itself. */
        if (lang == CBM_LANG_JUST && strcmp(kind, "recipe") == 0) {
            TSNode hdr = cbm_find_child_by_kind(node, "recipe_header");
            if (!ts_node_is_null(hdr)) {
                TSNode nm = ts_node_child_by_field_name(hdr, TS_FIELD("name"));
                if (!ts_node_is_null(nm)) {
                    return nm;
                }
            }
        }

        /* ReScript: the `function` (arrow) node — already in func_types — has no
         * name; the binding name is on the enclosing let_binding's `pattern` field.
         * Resolving via the parent keeps plain value let-bindings out of func_types. */
        if (lang == CBM_LANG_RESCRIPT && strcmp(kind, "function") == 0) {
            TSNode parent = ts_node_parent(node);
            if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "let_binding") == 0) {
                TSNode pat = ts_node_child_by_field_name(parent, TS_FIELD("pattern"));
                if (!ts_node_is_null(pat)) {
                    return pat;
                }
            }
        }

        /* Nickel: the lambda is a `fun_expr` with no name; the binding name is on
         * the enclosing let_binding's `pat` field (a `pattern` wrapping an `ident`).
         * Resolving via the parent keeps anonymous lambdas (e.g. `map (fun x => x)
         * xs`), whose parent is not a let_binding, out of func_types. */
        if (lang == CBM_LANG_NICKEL && strcmp(kind, "fun_expr") == 0) {
            TSNode parent = ts_node_parent(node);
            /* let_binding wraps the bound term in a `term`/`uni_term` chain, so the
             * fun_expr's immediate parent is not the let_binding directly. */
            for (int up = 0; up < FUNC_PARENT_CLIMB_LIMIT && !ts_node_is_null(parent); up++) {
                if (strcmp(ts_node_type(parent), "let_binding") == 0) {
                    TSNode pat = ts_node_child_by_field_name(parent, TS_FIELD("pat"));
                    if (!ts_node_is_null(pat)) {
                        TSNode inner = ts_node_child_by_field_name(pat, TS_FIELD("pat"));
                        return ts_node_is_null(inner) ? pat : inner;
                    }
                    break;
                }
                parent = ts_node_parent(parent);
            }
        }

        /* Nix: a named function is a `function_expression` (lambda `x: body`) with
         * no name of its own — the binding name lives on the enclosing `binding`'s
         * `attrpath` field (`name = x: ...`). Resolve through the parent binding to
         * the attrpath's LAST `attr` so `addOne = x: ...` mints a Function def. A
         * lambda whose parent is not a binding (e.g. an inline `map (x: x)`
         * argument) resolves null and stays out of func_types.
         *
         * The last segment, not the first: an attrpath is a PATH, and `a.b.fn = …`
         * is sugar for `a = { b = { fn = …; }; };`. Both spellings must mint the
         * same name (`fn`) and the same QN (`proj.mod.a.b.fn`) — the leading
         * segments are scope, supplied by cbm_nix_attrpath_scope. Taking the first
         * segment named `a.b.fn` "a", which collided with every other binding
         * whose path began `a`. */
        if (lang == CBM_LANG_NIX && strcmp(kind, "function_expression") == 0) {
            TSNode parent = ts_node_parent(node);
            if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "binding") == 0) {
                TSNode attrpath = ts_node_child_by_field_name(parent, TS_FIELD("attrpath"));
                if (!ts_node_is_null(attrpath)) {
                    TSNode attr = cbm_nix_attrpath_last_attr(attrpath);
                    return ts_node_is_null(attr) ? attrpath : attr;
                }
            }
        }

        /* Fortran: subroutine/function wrap an inner *_statement that carries the
         * `name` field; the outer node walk_defs matched has no name itself. */
        if (lang == CBM_LANG_FORTRAN &&
            (strcmp(kind, "subroutine") == 0 || strcmp(kind, "function") == 0)) {
            TSNode stmt = cbm_find_child_by_kind(node, "subroutine_statement");
            if (ts_node_is_null(stmt)) {
                stmt = cbm_find_child_by_kind(node, "function_statement");
            }
            if (!ts_node_is_null(stmt)) {
                TSNode nm = ts_node_child_by_field_name(stmt, TS_FIELD("name"));
                if (!ts_node_is_null(nm)) {
                    return nm;
                }
            }
        }

        /* F#: function_or_value_defn's name is on a function_declaration_left /
         * value_declaration_left child (a bare identifier, no `name` field). */
        if (lang == CBM_LANG_FSHARP && strcmp(kind, "function_or_value_defn") == 0) {
            TSNode lhs = cbm_find_child_by_kind(node, "function_declaration_left");
            if (ts_node_is_null(lhs)) {
                lhs = cbm_find_child_by_kind(node, "value_declaration_left");
            }
            if (!ts_node_is_null(lhs)) {
                TSNode nm = cbm_find_child_by_kind(lhs, "identifier");
                if (ts_node_is_null(nm)) {
                    nm = cbm_find_child_by_kind(lhs, "long_identifier");
                }
                if (!ts_node_is_null(nm)) {
                    return nm;
                }
            }
        }

        /* Groovy: top-level function_definition carries the name on the `function`
         * field (not `name`); fall back to the first `identifier` child. */
        if (lang == CBM_LANG_GROOVY && strcmp(kind, "function_definition") == 0) {
            TSNode fn = ts_node_child_by_field_name(node, TS_FIELD("function"));
            if (ts_node_is_null(fn)) {
                fn = cbm_find_child_by_kind(node, "identifier");
            }
            if (!ts_node_is_null(fn)) {
                return fn;
            }
        }

        /* Agda (FIELD_COUNT 0): the only `function` carrying the name is the type
         * signature line, whose lhs holds a `function_name` alias child. The body
         * line's lhs has no function_name child -> resolves null and is skipped. */
        if (lang == CBM_LANG_AGDA && strcmp(kind, "function") == 0) {
            TSNode lhs = cbm_find_child_by_kind(node, "lhs");
            if (!ts_node_is_null(lhs)) {
                TSNode fn = cbm_find_child_by_kind(lhs, "function_name");
                if (!ts_node_is_null(fn)) {
                    return fn;
                }
            }
        }

        /* Pony: def nodes have no `name` field; the name is the first plain
         * `identifier` child after the keyword/annotation/capability. */
        if (lang == CBM_LANG_PONY &&
            (strcmp(kind, "method") == 0 || strcmp(kind, "constructor") == 0 ||
             strcmp(kind, "ffi_method") == 0)) {
            TSNode id = cbm_find_child_by_kind(node, "identifier");
            if (!ts_node_is_null(id)) {
                return id;
            }
        }

        /* COBOL: program_definition has no `name` field; the program name is
         * identification_division > program_name (a leaf holding the clean name). */
        if (lang == CBM_LANG_COBOL && strcmp(kind, "program_definition") == 0) {
            TSNode iddiv = cbm_find_child_by_kind(node, "identification_division");
            if (!ts_node_is_null(iddiv)) {
                TSNode pname = cbm_find_child_by_kind(iddiv, "program_name");
                if (!ts_node_is_null(pname)) {
                    return pname;
                }
            }
        }

        /* Teal: the `local function foo()` form reduces to a function_statement
         * whose name is carried on a `function_name` child rather than the `name`
         * field (the field is only populated for the bare `function foo()` form).
         * func_name_node() already handled the field case above; here we cover the
         * function_name child so local functions also produce a Function def. */
        if (lang == CBM_LANG_TEAL &&
            (strcmp(kind, "function_statement") == 0 || strcmp(kind, "function_signature") == 0)) {
            TSNode fn = cbm_find_child_by_kind(node, "function_name");
            if (!ts_node_is_null(fn)) {
                return fn;
            }
        }

        /* SCSS: function_statement/mixin_statement have no `name` field; the def
         * name is a plain `name` child node. */
        if (lang == CBM_LANG_SCSS &&
            (strcmp(kind, "function_statement") == 0 || strcmp(kind, "mixin_statement") == 0)) {
            TSNode nm = cbm_find_child_by_kind(node, "name");
            if (!ts_node_is_null(nm)) {
                return nm;
            }
        }

        /* Jsonnet: a function binding is a `bind` node carrying the name on the
         * `function` field (an `id`), plus a `params` field. Plain value binds
         * (`local x = 1`) have no `params` field -> resolve null -> skipped, so
         * only function binds become Function defs. */
        if (lang == CBM_LANG_JSONNET && strcmp(kind, "bind") == 0) {
            TSNode params = ts_node_child_by_field_name(node, TS_FIELD("params"));
            if (!ts_node_is_null(params)) {
                TSNode nm = ts_node_child_by_field_name(node, TS_FIELD("function"));
                if (!ts_node_is_null(nm)) {
                    return nm;
                }
            }
        }

        /* Typst: `#let greet(name) = ...` parses to a `let` whose `pattern` field
         * is a `call` node (the function signature); the name is that call's
         * `item` field (an ident). A plain `#let x = 1` has a non-call pattern ->
         * resolve null -> skipped, keeping value bindings out of func_types. */
        if (lang == CBM_LANG_TYPST && strcmp(kind, "let") == 0) {
            TSNode pat = ts_node_child_by_field_name(node, TS_FIELD("pattern"));
            if (!ts_node_is_null(pat) && strcmp(ts_node_type(pat), "call") == 0) {
                TSNode item = ts_node_child_by_field_name(pat, TS_FIELD("item"));
                if (!ts_node_is_null(item)) {
                    return item;
                }
            }
        }

        /* SQL: create_function has no `name` field; the function name is nested as
         * object_reference > `name` field (an identifier). */
        if (lang == CBM_LANG_SQL && strcmp(kind, "create_function") == 0) {
            TSNode oref = cbm_find_child_by_kind(node, "object_reference");
            if (!ts_node_is_null(oref)) {
                TSNode nm = ts_node_child_by_field_name(oref, TS_FIELD("name"));
                if (!ts_node_is_null(nm)) {
                    return nm;
                }
            }
        }

        /* Elm: value_declaration carries its name on the
         * `functionDeclarationLeft` field's function_declaration_left child,
         * whose first lower_case_identifier is the function name. */
        if (lang == CBM_LANG_ELM && strcmp(kind, "value_declaration") == 0) {
            TSNode lhs = ts_node_child_by_field_name(node, TS_FIELD("functionDeclarationLeft"));
            if (ts_node_is_null(lhs)) {
                lhs = cbm_find_child_by_kind(node, "function_declaration_left");
            }
            if (!ts_node_is_null(lhs)) {
                TSNode nm = cbm_find_child_by_kind(lhs, "lower_case_identifier");
                if (!ts_node_is_null(nm)) {
                    return nm;
                }
            }
        }

        /* Pine Script: function_declaration_statement carries the name on the
         * `function` field (or `method` field for the method form), not `name`. */
        if (lang == CBM_LANG_PINE && strcmp(kind, "function_declaration_statement") == 0) {
            TSNode nm = ts_node_child_by_field_name(node, TS_FIELD("function"));
            if (ts_node_is_null(nm)) {
                nm = ts_node_child_by_field_name(node, TS_FIELD("method"));
            }
            if (!ts_node_is_null(nm)) {
                return nm;
            }
        }

        /* PL/SQL: create_function / create_procedure / function_* / procedure_*
         * carry fnc_name / prc_name, not the generic name field. */
        if (lang == CBM_LANG_PLSQL) {
            TSNode nm = ts_node_child_by_field_name(node, TS_FIELD("fnc_name"));
            if (ts_node_is_null(nm)) {
                nm = ts_node_child_by_field_name(node, TS_FIELD("prc_name"));
            }
            if (!ts_node_is_null(nm)) {
                return nm;
            }
        }

        /* Smali (no `name` field): method_definition > method_signature >
         * method_identifier holds the method name. */
        if (lang == CBM_LANG_SMALI && strcmp(kind, "method_definition") == 0) {
            TSNode sig = cbm_find_child_by_kind(node, "method_signature");
            if (!ts_node_is_null(sig)) {
                TSNode mid = cbm_find_child_by_kind(sig, "method_identifier");
                if (!ts_node_is_null(mid)) {
                    return mid;
                }
            }
        }

        /* Verilog/SystemVerilog (FIELD_COUNT 0): function/task names live on a
         * nested *_identifier wrapper; the function name is the first
         * simple_identifier descendant (params/returns come after the name). */
        if ((lang == CBM_LANG_VERILOG || lang == CBM_LANG_SYSTEMVERILOG) &&
            (strcmp(kind, "function_declaration") == 0 || strcmp(kind, "task_declaration") == 0)) {
            TSNode si =
                find_first_descendant_by_kind(node, "simple_identifier", CBM_DESCENDANT_MAX_DEPTH);
            if (!ts_node_is_null(si)) {
                return si;
            }
        }

        /* VHDL: subprogram_declaration/_definition carry the name on a nested
         * function_specification/procedure_specification child, via the
         * `function`/`procedure` field. */
        if (lang == CBM_LANG_VHDL && (strcmp(kind, "subprogram_declaration") == 0 ||
                                      strcmp(kind, "subprogram_definition") == 0)) {
            TSNode spec = cbm_find_child_by_kind(node, "function_specification");
            if (ts_node_is_null(spec)) {
                spec = cbm_find_child_by_kind(node, "procedure_specification");
            }
            if (!ts_node_is_null(spec)) {
                TSNode nm = ts_node_child_by_field_name(spec, TS_FIELD("function"));
                if (ts_node_is_null(nm)) {
                    nm = ts_node_child_by_field_name(spec, TS_FIELD("procedure"));
                }
                if (!ts_node_is_null(nm)) {
                    return nm;
                }
            }
        }

        /* Thrift / Cap'n Proto / Smithy (no `name` field): the def name is a
         * plain visible `identifier` child of the statement/definition node. */
        if (lang == CBM_LANG_THRIFT || lang == CBM_LANG_SMITHY) {
            TSNode id = cbm_find_child_by_kind(node, "identifier");
            if (!ts_node_is_null(id)) {
                return id;
            }
        }

        /* Cap'n Proto (FIELD_COUNT 0): name is an aliased *_identifier child. */
        if (lang == CBM_LANG_CAPNP) {
            const char *name_kind = NULL;
            if (strcmp(kind, "struct") == 0 || strcmp(kind, "interface") == 0) {
                name_kind = "type_identifier";
            } else if (strcmp(kind, "enum") == 0) {
                name_kind = "enum_identifier";
            } else if (strcmp(kind, "method") == 0) {
                name_kind = "method_identifier";
            }
            if (name_kind) {
                TSNode id = cbm_find_child_by_kind(node, name_kind);
                if (!ts_node_is_null(id)) {
                    return id;
                }
            }
        }

        /* CMake (FIELD_COUNT 0): function(foo)/macro(foo) — the name is nested as
         * *_command > argument_list > argument > unquoted_argument. */
        if (lang == CBM_LANG_CMAKE &&
            (strcmp(kind, "function_def") == 0 || strcmp(kind, "macro_def") == 0)) {
            const char *cmd_kind =
                strcmp(kind, "function_def") == 0 ? "function_command" : "macro_command";
            TSNode cmd = cbm_find_child_by_kind(node, cmd_kind);
            if (!ts_node_is_null(cmd)) {
                TSNode alist = cbm_find_child_by_kind(cmd, "argument_list");
                if (!ts_node_is_null(alist)) {
                    TSNode arg = cbm_find_child_by_kind(alist, "argument");
                    if (!ts_node_is_null(arg)) {
                        TSNode uq = cbm_find_child_by_kind(arg, "unquoted_argument");
                        return ts_node_is_null(uq) ? arg : uq;
                    }
                }
            }
        }

        /* Puppet: function_declaration name is a plain identifier/class_identifier
         * child (no `name` field). */
        if (lang == CBM_LANG_PUPPET &&
            (strcmp(kind, "function_declaration") == 0 || strcmp(kind, "lambda") == 0)) {
            TSNode id = cbm_find_child_by_kind(node, "identifier");
            if (ts_node_is_null(id)) {
                id = cbm_find_child_by_kind(node, "class_identifier");
            }
            if (!ts_node_is_null(id)) {
                return id;
            }
        }

        /* Assembly (GAS): a bare `label` (`foo:`) reduces with no `name` field;
         * its name child is aliased to `ident`. */
        if (lang == CBM_LANG_ASSEMBLY && strcmp(kind, "label") == 0) {
            TSNode id = cbm_find_child_by_kind(node, "ident");
            if (!ts_node_is_null(id)) {
                return id;
            }
        }

        /* BitBake: a shell task `do_foo() {...}` is a function_definition and a
         * python task `python do_foo() {...}` is an anonymous_python_function;
         * both carry the task name on a direct `identifier` child (no `name`
         * field). */
        if (lang == CBM_LANG_BITBAKE && (strcmp(kind, "function_definition") == 0 ||
                                         strcmp(kind, "anonymous_python_function") == 0)) {
            TSNode id = cbm_find_child_by_kind(node, "identifier");
            if (!ts_node_is_null(id)) {
                return id;
            }
        }

        /* PKL: a classMethod/objectMethod (`function foo(): T = ...`) has no
         * `name` field; the name is the `identifier` inside its methodHeader
         * child. */
        if (lang == CBM_LANG_PKL &&
            (strcmp(kind, "classMethod") == 0 || strcmp(kind, "objectMethod") == 0)) {
            TSNode hdr = cbm_find_child_by_kind(node, "methodHeader");
            if (!ts_node_is_null(hdr)) {
                TSNode id = cbm_find_child_by_kind(hdr, "identifier");
                if (!ts_node_is_null(id)) {
                    return id;
                }
            }
        }

        {
            TSNode r = resolve_toplevel_arrow_name(node, kind);
            if (!ts_node_is_null(r)) {
                return r;
            }
        }
        {
            TSNode r = resolve_func_name_scripting(node, lang, kind);
            if (!ts_node_is_null(r)) {
                return r;
            }
        }
        {
            TSNode r = resolve_func_name_fp(node, lang, kind, name);
            if (!ts_node_is_null(r)) {
                return r;
            }
        }

        {
            TSNode prev = node;
            TSNode r = resolve_func_name_c_family(&node, lang, kind);
            if (!ts_node_is_null(r)) {
                return r;
            }
            if (!ts_node_eq(prev, node)) {
                continue; /* template unwrapped — retry */
            }
        }

        break;
    } /* end template depth loop */
    TSNode null_node = {0};
    return null_node;
}

// Check for export_statement ancestor (JS/TS/TSX)
static bool is_js_exported(TSNode node) {
    return cbm_has_ancestor_kind(node, "export_statement", EXPORT_ANCESTOR_DEPTH);
}

// Check if a node is a comment node type.
static bool is_comment_node(const char *kind) {
    return (strcmp(kind, "comment") == 0 || strcmp(kind, "block_comment") == 0 ||
            strcmp(kind, "line_comment") == 0 || strcmp(kind, "multiline_comment") == 0);
}

// Python-specific: docstring as first expression_statement -> string in function body.
static const char *extract_python_docstring(CBMArena *a, TSNode node, const char *source) {
    TSNode body = ts_node_child_by_field_name(node, TS_FIELD("body"));
    if (ts_node_is_null(body) || ts_node_named_child_count(body) == 0) {
        return NULL;
    }
    TSNode first = ts_node_named_child(body, 0);
    if (ts_node_is_null(first) || strcmp(ts_node_type(first), "expression_statement") != 0) {
        return NULL;
    }
    if (ts_node_named_child_count(first) == 0) {
        return NULL;
    }
    TSNode str = ts_node_named_child(first, 0);
    if (ts_node_is_null(str)) {
        return NULL;
    }
    const char *sk = ts_node_type(str);
    if (strcmp(sk, "string") == 0 || strcmp(sk, "concatenated_string") == 0) {
        return cbm_node_text(a, str, source);
    }
    return NULL;
}

/* ── Doc comments ─────────────────────────────────────────────────────────
 * A definition's doc is the comment RUN directly above it, not only the one
 * comment node before it:
 *   - a run is contiguous (no blank line) and of one marker style; a Go comment
 *     group may mix styles and drops go/ast directives (//go:generate, //line);
 *   - it sits above the item's attributes (Rust), its declaration or export
 *     wrapper (JS/TS, C#, Go var/const) or its typedef (C);
 *   - a comment on the previous code's last line is that code's trailing
 *     comment, and a Rust inner doc documents the module, never the item;
 *   - a nearer plain comment does not hide a doc-style comment directly above;
 *   - a run a blank line away is detached (Go: always; elsewhere: unless it is
 *     doc-style), and a run with no letter or digit (a banner) is no doc;
 *   - the whole run is kept: the node's properties buffer grows with the doc.
 * Fields, variables, enum members and macros of code languages get the same
 * doc, a Perl sub the POD section that names it, and a Go or Rust file its
 * package comment or inner docs (on the File node).
 *
 * Leading trivia is read from the parent's child array, kept in a small
 * per-file memo: tree-sitter has no parent pointers, so ts_node_parent and
 * ts_node_prev_sibling are O(position), and one lookup per member of an
 * N-member class body cost O(N^2) (65,536 fields in one dotnet test class). */

enum {
    DOC_SPAN_COMMENT = 1,
    DOC_SPAN_ATTR = 2,
    DOC_SPAN_INIT_CAP = 8,
    DOC_SPAN_GROW = 2,
    DOC_MEMO_SLOTS = 8, /* parents whose child arrays stay cached */
    DOC_MEMO_WIDE = 32, /* a level wider than this is cached while descending */
    DOC_BISECT = 2,
};

/* Comment marker families (the slash forms are spelled out in words so this
 * comment does not nest). */
typedef enum {
    DOC_CS_OTHER = 0,  /* another family (--, ;, %) */
    DOC_CS_LINE,       /* slash-slash, or four and more slashes */
    DOC_CS_LINE_DOC,   /* slash-slash-slash */
    DOC_CS_LINE_BANG,  /* slash-slash-bang */
    DOC_CS_BLOCK,      /* plain block, empty block, star-banner block */
    DOC_CS_BLOCK_DOC,  /* slash-star-star */
    DOC_CS_BLOCK_BANG, /* slash-star-bang */
    DOC_CS_HASH,       /* hash */
} doc_style_t;

/* One trivia element as a byte span. erow is the EFFECTIVE end row: a node
 * that ends at column 0 (a Rust line_comment includes its newline) ends on the
 * row before. */
typedef struct {
    uint32_t sb;
    uint32_t eb;
    uint32_t srow;
    uint32_t erow;
    uint8_t kind;
    uint8_t style;
} doc_span_t;

typedef struct {
    doc_span_t *items; /* trivia directly before the anchor, in source order */
    int count;
    int cap;
    bool failed;           /* a missing span must not become shared documentation */
    bool code_before;      /* a non-trivia sibling precedes items[0] */
    uint32_t code_erow;    /* ... its effective end row */
    uint32_t code_eb;      /* ... its end byte (Kotlin gap scan) */
    uint32_t code_end_row; /* ... its raw end row (Kotlin gap scan) */
} doc_trivia_t;

static CBMArena *doc_scratch(CBMExtractCtx *ctx) {
    return ctx->scratch ? ctx->scratch : ctx->arena;
}

static uint32_t doc_erow(TSNode n) {
    TSPoint s = ts_node_start_point(n);
    TSPoint e = ts_node_end_point(n);
    return (e.column == 0 && e.row > s.row) ? e.row - SKIP_ONE : e.row;
}

static bool doc_has_prefix(const char *p, uint32_t n, const char *lit) {
    size_t len = strlen(lit);
    return n >= len && memcmp(p, lit, len) == 0;
}

static doc_style_t doc_style_of(const char *src, uint32_t sb, uint32_t eb) {
    const char *p = src + sb;
    uint32_t n = eb > sb ? eb - sb : 0;
    if (doc_has_prefix(p, n, "////")) {
        return DOC_CS_LINE;
    }
    if (doc_has_prefix(p, n, "///")) {
        return DOC_CS_LINE_DOC;
    }
    if (doc_has_prefix(p, n, "//!")) {
        return DOC_CS_LINE_BANG;
    }
    if (doc_has_prefix(p, n, "//")) {
        return DOC_CS_LINE;
    }
    if (doc_has_prefix(p, n, "/**/") || doc_has_prefix(p, n, "/***")) {
        return DOC_CS_BLOCK;
    }
    if (doc_has_prefix(p, n, "/**")) {
        return DOC_CS_BLOCK_DOC;
    }
    if (doc_has_prefix(p, n, "/*!")) {
        return DOC_CS_BLOCK_BANG;
    }
    if (doc_has_prefix(p, n, "/*")) {
        return DOC_CS_BLOCK;
    }
    return doc_has_prefix(p, n, "#") ? DOC_CS_HASH : DOC_CS_OTHER;
}

/* Languages whose toolchain has a doc-comment syntax distinct from a plain
 * comment (javadoc / KDoc / JSDoc / PHPDoc, rustdoc, C# XML docs, Doxygen). */
static bool doc_lang_has_doc_syntax(CBMLanguage lang) {
    switch (lang) {
    case CBM_LANG_RUST:
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
    case CBM_LANG_OBJC:
    case CBM_LANG_CSHARP:
    case CBM_LANG_SWIFT:
    case CBM_LANG_DART:
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_SCALA:
    case CBM_LANG_GROOVY:
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
    case CBM_LANG_ARKTS:
    case CBM_LANG_PHP:
        return true;
    default:
        return false;
    }
}

static bool doc_style_is_doc(CBMLanguage lang, int style) {
    switch (lang) {
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
    case CBM_LANG_OBJC:
        return style == DOC_CS_LINE_DOC || style == DOC_CS_LINE_BANG || style == DOC_CS_BLOCK_DOC ||
               style == DOC_CS_BLOCK_BANG;
    case CBM_LANG_RUST:
    case CBM_LANG_CSHARP:
    case CBM_LANG_SWIFT:
    case CBM_LANG_DART:
        return style == DOC_CS_LINE_DOC || style == DOC_CS_BLOCK_DOC;
    default:
        return doc_lang_has_doc_syntax(lang) && style == DOC_CS_BLOCK_DOC;
    }
}

/* Field/Variable docs are for code languages: config languages (YAML, TOML,
 * ...) mint a Variable per key, and a comment there is no API doc. */
static bool doc_lang_member_docs(CBMLanguage lang) {
    return doc_lang_has_doc_syntax(lang) || lang == CBM_LANG_GO || lang == CBM_LANG_PERL;
}

static uint8_t doc_trivia_kind(CBMLanguage lang, const char *kind) {
    if (is_comment_node(kind)) {
        return DOC_SPAN_COMMENT;
    }
    /* A Rust #[attr] is a sibling between the doc and the item. */
    if (lang == CBM_LANG_RUST && strcmp(kind, "attribute_item") == 0) {
        return DOC_SPAN_ATTR;
    }
    return 0;
}

static doc_span_t doc_span_of(TSNode n, const char *src, uint8_t kind) {
    doc_span_t sp;
    sp.sb = ts_node_start_byte(n);
    sp.eb = ts_node_end_byte(n);
    sp.srow = ts_node_start_point(n).row;
    sp.erow = doc_erow(n);
    sp.kind = kind;
    sp.style = (uint8_t)(kind == DOC_SPAN_COMMENT ? doc_style_of(src, sp.sb, sp.eb) : DOC_CS_OTHER);
    return sp;
}

static void doc_push_span(CBMArena *a, doc_trivia_t *t, const doc_span_t *sp) {
    if (t->count == t->cap) {
        int ncap = t->cap ? t->cap * DOC_SPAN_GROW : DOC_SPAN_INIT_CAP;
        doc_span_t *grown;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        if (cbm_doclink_test_fail_alloc(CBM_DOCLINK_ALLOC_SPAN)) {
            grown = NULL;
        } else
#endif
        {
            grown = (doc_span_t *)cbm_arena_alloc(a, (size_t)ncap * sizeof(doc_span_t));
        }
        if (!grown) {
            t->failed = true;
            return;
        }
        if (t->count > 0) {
            memcpy(grown, t->items, (size_t)t->count * sizeof(doc_span_t));
        }
        t->items = grown;
        t->cap = ncap;
    }
    t->items[t->count++] = *sp;
}

/* A non-trivia sibling before the anchor: trivia seen so far are not its. */
static void doc_note_code(doc_trivia_t *t, TSNode code) {
    t->count = 0;
    t->code_before = true;
    t->code_erow = doc_erow(code);
    t->code_eb = ts_node_end_byte(code);
    t->code_end_row = ts_node_end_point(code).row;
}

/* End of the Kotlin block comment opening at src[i] (block comments nest). */
static uint32_t doc_kotlin_block_end(const char *src, uint32_t i, uint32_t to, uint32_t *row) {
    int depth = 0;
    while (i < to) {
        bool pair = i + SKIP_ONE < to;
        if (pair && src[i] == '/' && src[i + SKIP_ONE] == '*') {
            depth++;
            i += PAIR_LEN;
            continue;
        }
        if (pair && src[i] == '*' && src[i + SKIP_ONE] == '/') {
            depth--;
            i += PAIR_LEN;
            if (depth == 0) {
                return i;
            }
            continue;
        }
        if (src[i] == '\n') {
            (*row)++;
        }
        i++;
    }
    return i;
}

/* tree-sitter-kotlin's automatic-semicolon scanner can swallow a comment that
 * sits between two declarations (seen before `enum class` and `abstract
 * class`): it has no node and survives only as bytes in the gap between the
 * siblings. Recover the comments from that gap. Any other byte is a token the
 * tree does not show, and the run restarts after it. */
static void doc_scan_kotlin_gap(CBMArena *a, const char *src, uint32_t from, uint32_t to,
                                uint32_t row, doc_trivia_t *t) {
    uint32_t i = from;
    while (i < to) {
        char c = src[i];
        if (c == '\n') {
            row++;
            i++;
            continue;
        }
        if (c == ' ' || c == '\t' || c == '\r' || c == '\f') {
            i++;
            continue;
        }
        if (c == '/' && i + SKIP_ONE < to &&
            (src[i + SKIP_ONE] == '/' || src[i + SKIP_ONE] == '*')) {
            doc_span_t sp = {i, i, row, row, DOC_SPAN_COMMENT, DOC_CS_OTHER};
            if (src[i + SKIP_ONE] == '/') {
                while (i < to && src[i] != '\n') {
                    i++;
                }
            } else {
                i = doc_kotlin_block_end(src, i, to, &row);
            }
            sp.eb = i;
            sp.erow = row;
            sp.style = (uint8_t)doc_style_of(src, sp.sb, sp.eb);
            doc_push_span(a, t, &sp);
            continue;
        }
        t->count = 0;
        t->code_before = true;
        t->code_erow = row;
        i++;
    }
}

static bool doc_space_byte(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f';
}

/* Start of the (nesting) block comment whose close ends at src[p), or
 * UINT32_MAX; *row moves to its start row. */
static uint32_t doc_back_block_start(const char *src, uint32_t p, uint32_t *row) {
    uint32_t q = p;
    int depth = 0;
    while (q >= PAIR_LEN) {
        if (src[q - PAIR_LEN] == '*' && src[q - SKIP_ONE] == '/') {
            depth++;
            q -= PAIR_LEN;
            continue;
        }
        if (src[q - PAIR_LEN] == '/' && src[q - SKIP_ONE] == '*') {
            depth--;
            q -= PAIR_LEN;
            if (depth == 0) {
                return q;
            }
            continue;
        }
        if (src[q - SKIP_ONE] == '\n' && *row > 0) {
            (*row)--;
        }
        q--;
    }
    return UINT32_MAX;
}

/* Start of a line comment that fills its line up to src[p), or UINT32_MAX. */
static uint32_t doc_back_line_start(const char *src, uint32_t p) {
    uint32_t ls = p;
    while (ls > 0 && src[ls - SKIP_ONE] != '\n') {
        ls--;
    }
    uint32_t k = ls;
    while (k < p && (src[k] == ' ' || src[k] == '\t')) {
        k++;
    }
    return (k + SKIP_ONE < p && src[k] == '/' && src[k + SKIP_ONE] == '/') ? k : UINT32_MAX;
}

/* tree-sitter-kotlin can also fold such a comment into the PREVIOUS node's
 * byte range (package_header ends after the comment that follows it), so
 * neither a sibling nor the gap holds it. Scan backwards from the anchor over
 * whitespace, block comments and whole-line line comments; stop at anything
 * else. The spans come out nearest-first and are appended in source order. */
static void doc_scan_kotlin_back(CBMArena *a, const char *src, uint32_t anchor_sb,
                                 uint32_t anchor_row, doc_trivia_t *t) {
    doc_trivia_t rev;
    memset(&rev, 0, sizeof(rev));
    uint32_t p = anchor_sb;
    uint32_t row = anchor_row;
    for (;;) {
        while (p > 0 && doc_space_byte(src[p - SKIP_ONE])) {
            if (src[p - SKIP_ONE] == '\n' && row > 0) {
                row--;
            }
            p--;
        }
        uint32_t srow = row;
        uint32_t start = (p >= PAIR_LEN && src[p - PAIR_LEN] == '*' && src[p - SKIP_ONE] == '/')
                             ? doc_back_block_start(src, p, &srow)
                             : doc_back_line_start(src, p);
        if (start == UINT32_MAX) {
            break;
        }
        doc_span_t sp = {
            start, p, srow, row, DOC_SPAN_COMMENT, (uint8_t)doc_style_of(src, start, p)};
        doc_push_span(a, &rev, &sp);
        p = start;
        row = srow;
    }
    for (int i = rev.count - SKIP_ONE; i >= 0; i--) {
        doc_push_span(a, t, &rev.items[i]);
    }
    t->code_before = p > 0;
    t->code_erow = row;
}

/* The memo: the child arrays of the last few parents looked up. A node's index
 * is a binary search on start bytes, and its parent is found by descending
 * from the deepest cached ancestor (the root is always reachable): the same
 * answers as the tree-sitter calls, O(log N) per lookup on wide levels. */
typedef struct {
    TSNode node;
    TSNode *kids;
    uint32_t *kid_sb;
    uint32_t n;
    uint32_t cap;
    uint32_t stamp;
    bool used;
} doc_memo_ent_t;

typedef struct {
    doc_memo_ent_t ent[DOC_MEMO_SLOTS];
    uint32_t clock;
} doc_memo_t;

/* The cached entry of `parent`, or the slot to reuse for it (unused first,
 * else least recently used). */
static doc_memo_ent_t *doc_memo_slot(doc_memo_t *m, TSNode parent, bool *hit) {
    doc_memo_ent_t *victim = &m->ent[0];
    for (int i = 0; i < DOC_MEMO_SLOTS; i++) {
        doc_memo_ent_t *e = &m->ent[i];
        if (e->used && ts_node_eq(e->node, parent)) {
            *hit = true;
            return e;
        }
        if (victim->used && (!e->used || e->stamp < victim->stamp)) {
            victim = e;
        }
    }
    *hit = false;
    return victim;
}

/* Fill `e` with the children of `parent`; false when the arrays cannot grow. */
static bool doc_memo_fill(CBMArena *a, doc_memo_ent_t *e, TSNode parent) {
    uint32_t n = ts_node_child_count(parent);
    if (n > e->cap) {
        TSNode *kids = (TSNode *)cbm_arena_alloc(a, (size_t)n * sizeof(TSNode));
        uint32_t *sb = (uint32_t *)cbm_arena_alloc(a, (size_t)n * sizeof(uint32_t));
        if (!kids || !sb) {
            e->used = false;
            return false;
        }
        e->kids = kids;
        e->kid_sb = sb;
        e->cap = n;
    }
    uint32_t k = 0;
    TSTreeCursor cur = ts_tree_cursor_new(parent);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            e->kids[k] = ts_tree_cursor_current_node(&cur);
            e->kid_sb[k] = ts_node_start_byte(e->kids[k]);
            k++;
        } while (k < n && ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    e->node = parent;
    e->n = k;
    e->used = true;
    return true;
}

static const doc_memo_ent_t *doc_memo_get(CBMExtractCtx *ctx, TSNode parent) {
    CBMArena *a = doc_scratch(ctx);
    if (!ctx->doc_memo) {
        doc_memo_t *fresh = (doc_memo_t *)cbm_arena_alloc(a, sizeof(doc_memo_t));
        if (!fresh) {
            return NULL;
        }
        memset(fresh, 0, sizeof(*fresh));
        ctx->doc_memo = fresh;
    }
    doc_memo_t *m = (doc_memo_t *)ctx->doc_memo;
    m->clock++;
    bool hit = false;
    doc_memo_ent_t *e = doc_memo_slot(m, parent, &hit);
    if (!hit && !doc_memo_fill(a, e, parent)) {
        return NULL;
    }
    e->stamp = m->clock;
    return e;
}

/* Index of `node` among the cached children, or -1. */
static int doc_memo_index(const doc_memo_ent_t *e, TSNode node) {
    uint32_t s = ts_node_start_byte(node);
    uint32_t lo = 0;
    uint32_t hi = e->n;
    while (lo < hi) {
        uint32_t mid = lo + ((hi - lo) / DOC_BISECT);
        if (e->kid_sb[mid] < s) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (uint32_t i = lo; i < e->n && e->kid_sb[i] == s; i++) {
        if (ts_node_eq(e->kids[i], node)) {
            return (int)i;
        }
    }
    return CBM_NOT_FOUND;
}

/* Index of the cached child whose byte range contains `node`, or -1. */
static int doc_memo_containing(const doc_memo_ent_t *e, TSNode node) {
    uint32_t s = ts_node_start_byte(node);
    uint32_t en = ts_node_end_byte(node);
    uint32_t lo = 0;
    uint32_t hi = e->n;
    while (lo < hi) {
        uint32_t mid = lo + ((hi - lo) / DOC_BISECT);
        if (e->kid_sb[mid] <= s) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid;
        }
    }
    for (int i = (int)lo - SKIP_ONE; i >= 0; i--) {
        TSNode kid = e->kids[i];
        uint32_t ke = ts_node_end_byte(kid);
        if (ke >= en && (ke > e->kid_sb[i] || ts_node_eq(kid, node))) {
            return i;
        }
        if (ke <= s && e->kid_sb[i] < s) {
            break;
        }
    }
    return CBM_NOT_FOUND;
}

/* The deepest cached node that contains `node` (the node itself excluded). */
static const doc_memo_ent_t *doc_memo_ancestor(const doc_memo_t *m, TSNode node) {
    const doc_memo_ent_t *best = NULL;
    uint32_t ns = ts_node_start_byte(node);
    uint32_t ne = ts_node_end_byte(node);
    for (int i = 0; m && i < DOC_MEMO_SLOTS; i++) {
        const doc_memo_ent_t *e = &m->ent[i];
        if (!e->used || ts_node_eq(e->node, node)) {
            continue;
        }
        uint32_t es = ts_node_start_byte(e->node);
        uint32_t ee = ts_node_end_byte(e->node);
        if (es <= ns && ne <= ee &&
            (!best || ee - es < ts_node_end_byte(best->node) - ts_node_start_byte(best->node))) {
            best = e;
        }
    }
    return best;
}

/* ts_node_parent() through the memo: descend from the deepest cached ancestor,
 * binary search on wide levels (cached on the way), linear on narrow ones. */
static TSNode doc_parent(CBMExtractCtx *ctx, TSNode node) {
    if (ts_node_eq(node, ctx->root)) {
        TSNode none = {0};
        return none;
    }
    const doc_memo_ent_t *e = doc_memo_ancestor((const doc_memo_t *)ctx->doc_memo, node);
    if (!e) {
        e = doc_memo_get(ctx, ctx->root);
    }
    while (e) {
        int k = doc_memo_containing(e, node);
        if (k < 0) {
            break;
        }
        TSNode cur = e->kids[k];
        if (ts_node_eq(cur, node)) {
            return e->node;
        }
        e = NULL;
        while (!ts_node_is_null(cur)) {
            if (ts_node_child_count(cur) > DOC_MEMO_WIDE) {
                e = doc_memo_get(ctx, cur);
                break;
            }
            TSNode next = ts_node_child_with_descendant(cur, node);
            if (ts_node_is_null(next)) {
                break;
            }
            if (ts_node_eq(next, node)) {
                return cur;
            }
            cur = next;
        }
    }
    return ts_node_parent(node); /* not reachable through the memo */
}

/* Trivia before the child at index mk of a cached child array. */
static void doc_collect_cached(CBMExtractCtx *ctx, const doc_memo_ent_t *me, int mk,
                               doc_trivia_t *t) {
    int j = mk - SKIP_ONE;
    while (j >= 0 && doc_trivia_kind(ctx->language, ts_node_type(me->kids[j]))) {
        j--;
    }
    if (j >= 0) {
        doc_note_code(t, me->kids[j]);
    }
    for (int i = j + SKIP_ONE; i < mk; i++) {
        uint8_t kind = doc_trivia_kind(ctx->language, ts_node_type(me->kids[i]));
        doc_span_t sp = doc_span_of(me->kids[i], ctx->source, kind);
        doc_push_span(doc_scratch(ctx), t, &sp);
    }
}

/* The same without the memo (its arrays could not be allocated): one cursor
 * pass over the parent. False when `anchor` is not among its children. */
static bool doc_collect_cursor(CBMExtractCtx *ctx, TSNode parent, TSNode anchor, doc_trivia_t *t) {
    bool found = false;
    TSTreeCursor cur = ts_tree_cursor_new(parent);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode ch = ts_tree_cursor_current_node(&cur);
            if (ts_node_eq(ch, anchor)) {
                found = true;
                break;
            }
            uint8_t kind = doc_trivia_kind(ctx->language, ts_node_type(ch));
            if (kind) {
                doc_span_t sp = doc_span_of(ch, ctx->source, kind);
                doc_push_span(doc_scratch(ctx), t, &sp);
            } else {
                doc_note_code(t, ch);
            }
        } while (ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    return found;
}

/* Kotlin comments the grammar hid: in the gap after the previous sibling, else
 * folded into the previous node (see the two scanners above). */
static void doc_collect_kotlin(CBMExtractCtx *ctx, TSNode parent, TSNode anchor, doc_trivia_t *t) {
    CBMArena *a = doc_scratch(ctx);
    uint32_t from = t->code_before ? t->code_eb : ts_node_start_byte(parent);
    uint32_t row = t->code_before ? t->code_end_row : ts_node_start_point(parent).row;
    uint32_t to = ts_node_start_byte(anchor);
    if (from < to) {
        doc_scan_kotlin_gap(a, ctx->source, from, to, row, t);
    }
    if (t->count == 0) {
        doc_scan_kotlin_back(a, ctx->source, to, ts_node_start_point(anchor).row, t);
    }
}

/* A doc comment was lost because memory ran out. For a language whose doc
 * comments are read for links, the doc-link layer must not then report a
 * complete graph (CBMDocLinkArray.failed). */
static void doc_lost(CBMExtractCtx *ctx) {
    if (ctx->result && cbm_doclink_lang_supported(ctx->language)) {
        ctx->result->doc_links.failed = true;
    }
}

/* Leading trivia of `anchor`, in source order. */
static void doc_collect_trivia(CBMExtractCtx *ctx, TSNode anchor, doc_trivia_t *t) {
    memset(t, 0, sizeof(*t));
    TSNode parent = doc_parent(ctx, anchor);
    if (ts_node_is_null(parent)) {
        return;
    }
    const doc_memo_ent_t *me = doc_memo_get(ctx, parent);
    int mk = me ? doc_memo_index(me, anchor) : CBM_NOT_FOUND;
    bool found = mk >= 0;
    if (found) {
        doc_collect_cached(ctx, me, mk, t);
    } else {
        found = doc_collect_cursor(ctx, parent, anchor, t);
    }
    if (!found) {
        bool failed = t->failed;
        memset(t, 0, sizeof(*t));
        t->failed = failed;
    } else if (t->count == 0 && ctx->language == CBM_LANG_KOTLIN) {
        doc_collect_kotlin(ctx, parent, anchor, t);
    }
    if (t->failed) {
        doc_lost(ctx);
    }
}

/* go/ast CommentGroup.Text: a line comment with no space after the slashes is
 * a directive, not documentation: //line, //extern, //export and the
 * //[a-z0-9]+:[a-z0-9] form (//go:generate, //nolint:errcheck, ...). */
static bool doc_go_is_directive(const char *src, const doc_span_t *sp) {
    const char *p = src + sp->sb;
    uint32_t n = sp->eb > sp->sb ? sp->eb - sp->sb : 0;
    if (n <= PAIR_LEN || p[0] != '/' || p[SKIP_ONE] != '/' || p[PAIR_LEN] == ' ') {
        return false;
    }
    p += PAIR_LEN;
    n -= PAIR_LEN;
    if (doc_has_prefix(p, n, "line ") || doc_has_prefix(p, n, "extern ") ||
        doc_has_prefix(p, n, "export ")) {
        return true;
    }
    uint32_t colon = 0;
    while (colon < n && p[colon] != ':' && p[colon] != '\n') {
        colon++;
    }
    if (colon == 0 || colon >= n || p[colon] != ':' || colon + SKIP_ONE >= n) {
        return false;
    }
    for (uint32_t i = 0; i <= colon + SKIP_ONE; i++) {
        char b = p[i];
        if (i != colon && !((b >= 'a' && b <= 'z') || (b >= '0' && b <= '9'))) {
            return false;
        }
    }
    return true;
}

/* UTF-8 bytes of the banner characters (see doc_has_words). */
enum {
    DOC_U8_ASCII_END = 0x80,
    DOC_U8_CONT_MASK = 0xC0, /* 10xxxxxx: continuation byte */
    DOC_U8_CONT = 0x80,
    DOC_U8_LATIN1 = 0xC2,      /* lead byte of U+0080..U+00BF */
    DOC_U8_MIDDLE_DOT = 0xB7,  /* U+00B7 */
    DOC_U8_U2000 = 0xE2,       /* lead byte of U+2000..U+2FFF */
    DOC_U8_PUNCT = 0x80,       /* U+2000..U+203F */
    DOC_U8_DASH_FIRST = 0x90,  /* U+2010 hyphen */
    DOC_U8_BULLET_LAST = 0xA7, /* U+2027 hyphenation point */
    DOC_U8_BOX_FIRST = 0x94,   /* U+2500 box drawing */
    DOC_U8_SHAPES_LAST = 0x97, /* U+25FF, end of the geometric shapes */
    DOC_U8_LATIN1_LEN = 2,
    DOC_U8_U2000_LEN = 3,
};

static bool doc_banner_char(const unsigned char *p, size_t left) {
    if (left >= DOC_U8_LATIN1_LEN && p[0] == DOC_U8_LATIN1 && p[SKIP_ONE] == DOC_U8_MIDDLE_DOT) {
        return true;
    }
    if (left < DOC_U8_U2000_LEN || p[0] != DOC_U8_U2000) {
        return false;
    }
    if (p[SKIP_ONE] >= DOC_U8_BOX_FIRST && p[SKIP_ONE] <= DOC_U8_SHAPES_LAST) {
        return true;
    }
    return p[SKIP_ONE] == DOC_U8_PUNCT && p[PAIR_LEN] >= DOC_U8_DASH_FIRST &&
           p[PAIR_LEN] <= DOC_U8_BULLET_LAST;
}

/* A comment with no letter or digit is a divider or banner, not documentation.
 * Any non-ASCII character counts as a letter except the ones banners are drawn
 * with: U+00B7, the dashes and bullets U+2010..U+2027, and the box-drawing,
 * block and geometric-shape characters U+2500..U+25FF. */
static bool doc_has_words(const char *s, size_t n) {
    const unsigned char *u = (const unsigned char *)s;
    for (size_t i = 0; i < n; i++) {
        if (u[i] < DOC_U8_ASCII_END) {
            if (isalnum(u[i])) {
                return true;
            }
        } else if ((u[i] & DOC_U8_CONT_MASK) != DOC_U8_CONT && !doc_banner_char(u + i, n - i)) {
            return true;
        }
    }
    return false;
}

static bool doc_span_kept(const char *src, const doc_span_t *sp, bool go_directives) {
    return sp->kind == DOC_SPAN_COMMENT && !(go_directives && doc_go_is_directive(src, sp));
}

/* Text of items[first..last]: one comment keeps its exact bytes; several are
 * joined by newlines, each without its own trailing newline. NULL when nothing
 * is left (Go directives only) or when no comment has a letter or digit. */
static const char *doc_run_text(CBMExtractCtx *ctx, const doc_trivia_t *t, int first, int last,
                                bool go_directives) {
    const char *src = ctx->source;
    int kept = 0;
    bool words = false;
    size_t total = 0;
    uint32_t first_row = 0;
    for (int k = first; k <= last; k++) {
        const doc_span_t *sp = &t->items[k];
        if (!doc_span_kept(src, sp, go_directives)) {
            continue;
        }
        if (kept == 0) {
            first_row = sp->srow;
        }
        total += (size_t)(sp->eb - sp->sb) + SKIP_ONE;
        words = words || doc_has_words(src + sp->sb, sp->eb - sp->sb);
        kept++;
    }
    if (kept == 0 || !words) {
        return NULL;
    }
    char *buf;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (cbm_doclink_test_fail_alloc(CBM_DOCLINK_ALLOC_TEXT)) {
        buf = NULL;
    } else
#endif
    {
        buf = (char *)cbm_arena_alloc(ctx->arena, total + SKIP_ONE);
    }
    if (!buf) {
        doc_lost(ctx);
        return NULL;
    }
    size_t w = 0;
    for (int k = first; k <= last; k++) {
        const doc_span_t *sp = &t->items[k];
        if (!doc_span_kept(src, sp, go_directives)) {
            continue;
        }
        uint32_t eb = sp->eb;
        while (kept > SKIP_ONE && eb > sp->sb &&
               (src[eb - SKIP_ONE] == '\n' || src[eb - SKIP_ONE] == '\r')) {
            eb--;
        }
        if (w > 0) {
            buf[w++] = '\n';
        }
        memcpy(buf + w, src + sp->sb, eb - sp->sb);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
        cbm_doclink_test_note_doc_work(eb - sp->sb, 0, 0);
#endif
        w += eb - sp->sb;
    }
    buf[w] = '\0';
    /* Doc-link references need their source lines: the text's line k is the
     * source line first_row + k (one comment per line, or a block keeping
     * its own newlines). */
    cbm_doclink_note_doc_line(ctx, buf, first_row + SKIP_ONE);
    return buf;
}

/* Index of the comment nearest the item, above its Rust attributes, or -1. */
static int doc_nearest_comment(const doc_trivia_t *t) {
    int k = t->count - SKIP_ONE;
    while (k >= 0 && t->items[k].kind == DOC_SPAN_ATTR) {
        k--;
    }
    return (k >= 0 && t->items[k].kind == DOC_SPAN_COMMENT) ? k : CBM_NOT_FOUND;
}

/* A nearer plain comment does not hide a doc-style comment directly above it
 * (a javadoc, then `// NOSONAR`): the index of that doc comment, else `near`. */
static int doc_prefer_doc_style(CBMLanguage lang, const doc_trivia_t *t, int near) {
    if (!doc_lang_has_doc_syntax(lang) || doc_style_is_doc(lang, t->items[near].style)) {
        return near;
    }
    for (int j = near - SKIP_ONE; j >= 0; j--) {
        const doc_span_t *sp = &t->items[j];
        if (t->items[j + SKIP_ONE].srow > sp->erow + SKIP_ONE) {
            break; /* a blank line */
        }
        if (sp->kind == DOC_SPAN_ATTR) {
            continue;
        }
        if (sp->kind != DOC_SPAN_COMMENT) {
            break;
        }
        if (doc_style_is_doc(lang, sp->style)) {
            return j;
        }
    }
    return near;
}

/* First index of the run that ends at `near`: contiguous, one style (a Go
 * comment group mixes styles), not the previous code's trailing comment. */
static int doc_run_first(CBMLanguage lang, const doc_trivia_t *t, int near) {
    int style = t->items[near].style;
    int first = near;
    for (int j = near - SKIP_ONE; j >= 0; j--) {
        const doc_span_t *sp = &t->items[j];
        if (sp->kind != DOC_SPAN_COMMENT || t->items[j + SKIP_ONE].srow > sp->erow + SKIP_ONE) {
            break; /* an attribute or a blank line ends the run */
        }
        if (lang != CBM_LANG_GO && sp->style != style) {
            break;
        }
        if (j == 0 && t->code_before && sp->srow == t->code_erow) {
            break;
        }
        first = j;
    }
    return first;
}

/* The doc of an anchor from its leading trivia. */
static const char *doc_from_trivia(CBMExtractCtx *ctx, const doc_trivia_t *t,
                                   uint32_t anchor_srow) {
    CBMLanguage lang = ctx->language;
    int near = t->items ? doc_nearest_comment(t) : CBM_NOT_FOUND;
    if (near < 0) {
        return NULL;
    }
    near = doc_prefer_doc_style(lang, t, near);
    int style = t->items[near].style;
    if (lang == CBM_LANG_RUST && (style == DOC_CS_LINE_BANG || style == DOC_CS_BLOCK_BANG)) {
        return NULL; /* an inner doc: it documents the enclosing module */
    }
    if (near == 0 && t->code_before && t->items[0].srow == t->code_erow) {
        return NULL; /* the previous code's trailing comment */
    }
    uint32_t next_row = near + SKIP_ONE < t->count ? t->items[near + SKIP_ONE].srow : anchor_srow;
    bool detached = next_row > t->items[near].erow + SKIP_ONE;
    if (detached && (lang == CBM_LANG_GO || !doc_style_is_doc(lang, style))) {
        return NULL;
    }
    return doc_run_text(ctx, t, doc_run_first(lang, t, near), near, lang == CBM_LANG_GO);
}

static const char *doc_for_anchor_status(CBMExtractCtx *ctx, TSNode anchor, bool *complete) {
    doc_trivia_t t;
    doc_collect_trivia(ctx, anchor, &t);
    if (complete) {
        *complete = !t.failed;
    }
    return doc_from_trivia(ctx, &t, ts_node_start_point(anchor).row);
}

static const char *doc_for_anchor(CBMExtractCtx *ctx, TSNode anchor) {
    return doc_for_anchor_status(ctx, anchor, NULL);
}

static bool doc_kind_is(TSNode n, const char *kind) {
    return !ts_node_is_null(n) && strcmp(ts_node_type(n), kind) == 0;
}

/* JS/TS: a function or class expression is documented above its declarator,
 * field, pair or assignment statement, a declarator above its declaration,
 * and any of them above `declare` and `export`. */
static TSNode doc_anchor_js(CBMExtractCtx *ctx, TSNode cur) {
    const char *k = ts_node_type(cur);
    if (strcmp(k, "arrow_function") == 0 || strcmp(k, "function_expression") == 0 ||
        strcmp(k, "generator_function") == 0 || strcmp(k, "class") == 0) {
        TSNode p = doc_parent(ctx, cur);
        if (doc_kind_is(p, "variable_declarator") || doc_kind_is(p, "public_field_definition") ||
            doc_kind_is(p, "field_definition") || doc_kind_is(p, "pair")) {
            cur = p;
        } else if (doc_kind_is(p, "assignment_expression")) {
            TSNode pp = doc_parent(ctx, p);
            if (doc_kind_is(pp, "expression_statement")) {
                cur = pp;
            }
        }
    }
    if (doc_kind_is(cur, "variable_declarator")) {
        TSNode p = doc_parent(ctx, cur);
        if (doc_kind_is(p, "lexical_declaration") || doc_kind_is(p, "variable_declaration")) {
            cur = p;
        }
    }
    TSNode p = doc_parent(ctx, cur);
    if (doc_kind_is(p, "ambient_declaration")) {
        cur = p;
        p = doc_parent(ctx, cur);
    }
    return doc_kind_is(p, "export_statement") ? p : cur;
}

/* C#: a field is documented above its field declaration, a top-level
 * function above its global statement. */
static TSNode doc_anchor_csharp(CBMExtractCtx *ctx, TSNode cur) {
    if (doc_kind_is(cur, "variable_declarator")) {
        TSNode p = doc_parent(ctx, cur);
        if (doc_kind_is(p, "variable_declaration")) {
            cur = p;
        }
    }
    if (doc_kind_is(cur, "variable_declaration")) {
        TSNode p = doc_parent(ctx, cur);
        if (doc_kind_is(p, "field_declaration") || doc_kind_is(p, "event_field_declaration")) {
            cur = p;
        }
    }
    TSNode p = doc_parent(ctx, cur);
    return doc_kind_is(p, "global_statement") ? p : cur;
}

/* C family: `typedef struct X {..} Y;` and `struct X {..} v;` are documented
 * above the declaration. Only a specifier WITH a body is a definition; a
 * bodiless one is a usage. */
static TSNode doc_anchor_c(CBMExtractCtx *ctx, TSNode cur) {
    if ((doc_kind_is(cur, "struct_specifier") || doc_kind_is(cur, "enum_specifier") ||
         doc_kind_is(cur, "union_specifier") || doc_kind_is(cur, "class_specifier")) &&
        !ts_node_is_null(ts_node_child_by_field_name(cur, TS_FIELD("body")))) {
        TSNode p = doc_parent(ctx, cur);
        if (doc_kind_is(p, "type_definition") || doc_kind_is(p, "declaration")) {
            return p;
        }
    }
    return cur;
}

/* Go: `var x = 1` (no parentheses) is documented above the declaration. */
static TSNode doc_anchor_go(CBMExtractCtx *ctx, TSNode cur) {
    if (doc_kind_is(cur, "var_spec") || doc_kind_is(cur, "const_spec")) {
        TSNode p = doc_parent(ctx, cur);
        if ((doc_kind_is(p, "var_declaration") || doc_kind_is(p, "const_declaration")) &&
            ts_node_eq(ts_node_child(p, SECOND_CHILD_IDX), cur)) {
            return p;
        }
    }
    return cur;
}

/* The node whose leading comments document `node`. */
static TSNode doc_anchor(CBMExtractCtx *ctx, TSNode node) {
    switch (ctx->language) {
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
    case CBM_LANG_ARKTS:
        return doc_anchor_js(ctx, node);
    case CBM_LANG_CSHARP:
        return doc_anchor_csharp(ctx, node);
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
    case CBM_LANG_OBJC:
        return doc_anchor_c(ctx, node);
    case CBM_LANG_GO:
        return doc_anchor_go(ctx, node);
    default:
        return node;
    }
}

/* ── Perl POD: the =head / =item section that names the sub ── */

enum { POD_HEAD = 1, POD_ITEM, POD_OVER, POD_BACK, POD_CUT, POD_OTHER };

typedef struct {
    uint32_t sb; /* command paragraph */
    uint32_t eb;
    uint32_t wb; /* end of the command word: its argument starts here */
    uint8_t type;
    uint8_t level; /* =headN: N; =item and =back: list depth */
} pod_cmd_t;

typedef struct {
    const char *name;
    size_t name_len;
    uint32_t sb;
    uint32_t eb;
} pod_sec_t;

typedef struct {
    pod_sec_t *items;
    int count;
    int cap;
} pod_index_t;

static bool pod_line_is_blank(const char *src, uint32_t b, uint32_t e) {
    for (uint32_t k = b; k < e; k++) {
        if (src[k] != ' ' && src[k] != '\t' && src[k] != '\r') {
            return false;
        }
    }
    return true;
}

/* Next POD paragraph (a maximal run of non-blank lines) in src[*pos, pe). */
static bool pod_next_para(const char *src, uint32_t *pos, uint32_t pe, uint32_t *psb,
                          uint32_t *peb) {
    uint32_t i = *pos;
    bool in = false;
    uint32_t s = 0;
    while (i < pe) {
        uint32_t e = i;
        while (e < pe && src[e] != '\n') {
            e++;
        }
        bool blank = pod_line_is_blank(src, i, e);
        if (!blank && !in) {
            in = true;
            s = i;
        }
        if (blank && in) {
            *psb = s;
            *peb = i;
            *pos = i;
            return true;
        }
        i = e < pe ? e + SKIP_ONE : pe;
    }
    *pos = pe;
    if (in) {
        *psb = s;
        *peb = pe;
    }
    return in;
}

static bool pod_word_is(const char *w, size_t wl, const char *lit) {
    return wl == strlen(lit) && strncmp(w, lit, wl) == 0;
}

/* Classify one command paragraph; `depth` is the =over nesting. */
static pod_cmd_t pod_classify(const char *src, uint32_t sb, uint32_t eb, int *depth) {
    uint32_t wb = sb + SKIP_ONE;
    while (wb < eb && (isalnum((unsigned char)src[wb]) || src[wb] == '_')) {
        wb++;
    }
    const char *w = src + sb + SKIP_ONE;
    size_t wl = wb - sb - SKIP_ONE;
    pod_cmd_t c = {sb, eb, wb, POD_OTHER, 0};
    size_t head_len = strlen("head");
    if (wl == head_len + SKIP_ONE && strncmp(w, "head", head_len) == 0 &&
        isdigit((unsigned char)w[head_len])) {
        c.type = POD_HEAD;
        c.level = (uint8_t)(w[head_len] - '0');
    } else if (pod_word_is(w, wl, "item")) {
        c.type = POD_ITEM;
        c.level = (uint8_t)*depth;
    } else if (pod_word_is(w, wl, "over")) {
        c.type = POD_OVER;
        (*depth)++;
    } else if (pod_word_is(w, wl, "back")) {
        c.type = POD_BACK;
        c.level = (uint8_t)*depth;
        if (*depth > 0) {
            (*depth)--;
        }
    } else if (pod_word_is(w, wl, "cut")) {
        c.type = POD_CUT;
    }
    return c;
}

/* Command paragraphs of one POD block, in order. */
static int pod_commands(CBMArena *a, const char *src, uint32_t pb, uint32_t pe, pod_cmd_t **out) {
    int cap = DOC_SPAN_INIT_CAP;
    int n = 0;
    pod_cmd_t *cmds = (pod_cmd_t *)cbm_arena_alloc(a, (size_t)cap * sizeof(pod_cmd_t));
    *out = cmds;
    if (!cmds) {
        return 0;
    }
    int depth = 0;
    uint32_t pos = pb;
    uint32_t sb = 0;
    uint32_t eb = 0;
    while (pod_next_para(src, &pos, pe, &sb, &eb)) {
        if (src[sb] != '=' || sb + SKIP_ONE >= eb || !isalpha((unsigned char)src[sb + SKIP_ONE])) {
            continue;
        }
        if (n == cap) {
            int ncap = cap * DOC_SPAN_GROW;
            pod_cmd_t *grown = (pod_cmd_t *)cbm_arena_alloc(a, (size_t)ncap * sizeof(pod_cmd_t));
            if (!grown) {
                break;
            }
            memcpy(grown, cmds, (size_t)n * sizeof(pod_cmd_t));
            cmds = grown;
            cap = ncap;
            *out = cmds;
        }
        cmds[n++] = pod_classify(src, sb, eb, &depth);
    }
    return n;
}

/* End byte of the section that cmds[ci] (a =head or =item) opens. */
static uint32_t pod_section_end(const pod_cmd_t *cmds, int n, int ci, uint32_t pe) {
    const pod_cmd_t *c = &cmds[ci];
    for (int j = ci + SKIP_ONE; j < n; j++) {
        const pod_cmd_t *d = &cmds[j];
        if (d->type == POD_CUT) {
            return d->sb;
        }
        if (d->type == POD_HEAD && (c->type == POD_ITEM || d->level <= c->level)) {
            return d->sb;
        }
        if (c->type == POD_ITEM && (d->type == POD_ITEM || d->type == POD_BACK) &&
            d->level == c->level) {
            return d->sb;
        }
    }
    return pe;
}

/* A heading's text with POD formatting codes (X<..>, X<< .. >>) removed and
 * newlines and tabs as spaces. */
static char *pod_plain_heading(CBMArena *a, const char *src, uint32_t b, uint32_t e) {
    char *buf = (char *)cbm_arena_alloc(a, (size_t)(e - b) + SKIP_ONE);
    if (!buf) {
        return NULL;
    }
    size_t w = 0;
    for (uint32_t i = b; i < e; i++) {
        char c = src[i];
        if (c >= 'A' && c <= 'Z' && i + SKIP_ONE < e && src[i + SKIP_ONE] == '<') {
            uint32_t j = i + SKIP_ONE;
            while (j < e && src[j] == '<') {
                j++;
            }
            if (j - i > PAIR_LEN && j < e && (src[j] == ' ' || src[j] == '\t')) {
                j++; /* X<< text >> */
            }
            i = j - SKIP_ONE;
            continue;
        }
        if (c == '>' && !(w > 0 && buf[w - SKIP_ONE] == '-')) {
            continue;
        }
        buf[w++] = (c == '\n' || c == '\r' || c == '\t') ? ' ' : c;
    }
    buf[w] = '\0';
    return buf;
}

static const char *pod_skip_spaces(const char *p) {
    while (*p == ' ') {
        p++;
    }
    return p;
}

/* The sub a heading names: after a list bullet ("*", "1."), a `$obj->`
 * receiver and a sigil, the identifier that follows. Returns its length
 * (0: none). */
static size_t pod_heading_name(CBMArena *a, const char *src, uint32_t b, uint32_t e,
                               const char **name) {
    const char *buf = pod_plain_heading(a, src, b, e);
    if (!buf) {
        return 0;
    }
    const char *p = pod_skip_spaces(buf);
    if (*p == '*') {
        p++;
    } else if (isdigit((unsigned char)*p)) {
        while (isdigit((unsigned char)*p)) {
            p++;
        }
        if (*p == '.') {
            p++;
        }
    }
    p = pod_skip_spaces(p);
    if (*p == '$') {
        const char *q = p + SKIP_ONE;
        while (isalnum((unsigned char)*q) || *q == '_') {
            q++;
        }
        q = pod_skip_spaces(q);
        if (q > p + SKIP_ONE && q[0] == '-' && q[SKIP_ONE] == '>') {
            p = pod_skip_spaces(q + PAIR_LEN);
        }
    }
    if (*p == '$' || *p == '@' || *p == '%' || *p == '&') {
        p++;
    }
    if (!(isalpha((unsigned char)*p) || *p == '_')) {
        return 0;
    }
    const char *s = p;
    while (isalnum((unsigned char)*p) || *p == '_') {
        p++;
    }
    *name = s;
    return (size_t)(p - s);
}

static uint32_t pod_trim_end(const char *src, uint32_t sb, uint32_t eb) {
    while (eb > sb && (src[eb - SKIP_ONE] == '\n' || src[eb - SKIP_ONE] == '\r' ||
                       src[eb - SKIP_ONE] == ' ' || src[eb - SKIP_ONE] == '\t')) {
        eb--;
    }
    return eb;
}

static void pod_index_push(CBMArena *a, pod_index_t *ix, const pod_sec_t *sec) {
    if (ix->count == ix->cap) {
        int ncap = ix->cap ? ix->cap * DOC_SPAN_GROW : DOC_SPAN_INIT_CAP;
        pod_sec_t *grown = (pod_sec_t *)cbm_arena_alloc(a, (size_t)ncap * sizeof(pod_sec_t));
        if (!grown) {
            return;
        }
        if (ix->count > 0) {
            memcpy(grown, ix->items, (size_t)ix->count * sizeof(pod_sec_t));
        }
        ix->items = grown;
        ix->cap = ncap;
    }
    ix->items[ix->count++] = *sec;
}

/* Index the named =head / =item sections of one POD block. */
static void pod_index_block(CBMExtractCtx *ctx, pod_index_t *ix, TSNode pod) {
    CBMArena *a = doc_scratch(ctx);
    uint32_t pb = ts_node_start_byte(pod);
    uint32_t pe = ts_node_end_byte(pod);
    pod_cmd_t *cmds = NULL;
    int n = pod_commands(a, ctx->source, pb, pe, &cmds);
    for (int ci = 0; ci < n; ci++) {
        if (cmds[ci].type != POD_HEAD && cmds[ci].type != POD_ITEM) {
            continue;
        }
        const char *nm = NULL;
        size_t nl = pod_heading_name(a, ctx->source, cmds[ci].wb, cmds[ci].eb, &nm);
        if (nl == 0) {
            continue;
        }
        uint32_t se = pod_section_end(cmds, n, ci, pe);
        pod_sec_t sec = {nm, nl, cmds[ci].sb, pod_trim_end(ctx->source, cmds[ci].sb, se)};
        pod_index_push(a, ix, &sec);
    }
}

/* The file's POD sections by name, built on the first Perl sub. */
static const pod_index_t *pod_index(CBMExtractCtx *ctx) {
    if (ctx->doc_pod_index) {
        return (const pod_index_t *)ctx->doc_pod_index;
    }
    pod_index_t *ix = (pod_index_t *)cbm_arena_alloc(doc_scratch(ctx), sizeof(pod_index_t));
    if (!ix) {
        return NULL;
    }
    memset(ix, 0, sizeof(*ix));
    TSTreeCursor cur = ts_tree_cursor_new(ctx->root);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode ch = ts_tree_cursor_current_node(&cur);
            if (strcmp(ts_node_type(ch), "pod") == 0) {
                pod_index_block(ctx, ix, ch);
            }
        } while (ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    ctx->doc_pod_index = ix;
    return ix;
}

static bool pod_sec_names(const pod_sec_t *s, const char *name, size_t nl) {
    return s->name_len == nl && memcmp(s->name, name, nl) == 0;
}

/* Every POD section whose =head / =item names the sub exactly, joined in
 * document order; NULL when none does. */
static const char *pod_doc_by_name(CBMExtractCtx *ctx, const char *name) {
    const pod_index_t *ix = pod_index(ctx);
    if (!ix || !name || !name[0]) {
        return NULL;
    }
    size_t nl = strlen(name);
    size_t total = 0;
    for (int i = 0; i < ix->count; i++) {
        if (pod_sec_names(&ix->items[i], name, nl)) {
            total += (size_t)(ix->items[i].eb - ix->items[i].sb) + PAIR_LEN;
        }
    }
    if (total == 0) {
        return NULL;
    }
    char *buf = (char *)cbm_arena_alloc(ctx->arena, total + SKIP_ONE);
    if (!buf) {
        return NULL;
    }
    size_t w = 0;
    for (int i = 0; i < ix->count; i++) {
        const pod_sec_t *s = &ix->items[i];
        if (!pod_sec_names(s, name, nl)) {
            continue;
        }
        if (w > 0) {
            buf[w++] = '\n';
            buf[w++] = '\n';
        }
        memcpy(buf + w, ctx->source + s->sb, s->eb - s->sb);
        w += s->eb - s->sb;
    }
    buf[w] = '\0';
    return buf;
}

/* ── Doc entry points ── */

/* go/doc: a type spec's own doc, else its declaration's (the doc of a
 * single-spec `type X ...` sits above the `type` keyword). */
static const char *doc_go_type(CBMExtractCtx *ctx, TSNode node) {
    const char *own = doc_for_anchor(ctx, node);
    TSNode parent = doc_parent(ctx, node);
    if (own || !doc_kind_is(parent, "type_declaration")) {
        return own;
    }
    return doc_for_anchor(ctx, parent);
}

/* Doc of a Function/Method/Class-like definition. */
static const char *extract_docstring(CBMExtractCtx *ctx, TSNode node, const char *name) {
    CBMLanguage lang = ctx->language;
    if (lang == CBM_LANG_PERL) {
        const char *pod = pod_doc_by_name(ctx, name);
        if (pod) {
            return pod;
        }
    }
    if (lang == CBM_LANG_GO &&
        (doc_kind_is(node, "type_spec") || doc_kind_is(node, "type_alias"))) {
        return doc_go_type(ctx, node);
    }
    const char *doc = doc_for_anchor(ctx, doc_anchor(ctx, node));
    if (!doc && lang == CBM_LANG_PYTHON) {
        return extract_python_docstring(ctx->arena, node, ctx->source);
    }
    return doc;
}

/* Doc of a Field, Variable, enum member or Macro (code languages only). */
static const char *extract_member_docstring_status(CBMExtractCtx *ctx, TSNode node,
                                                   bool *complete) {
    if (!doc_lang_member_docs(ctx->language)) {
        if (complete) {
            *complete = true;
        }
        return NULL;
    }
    return doc_for_anchor_status(ctx, doc_anchor(ctx, node), complete);
}

static const char *extract_member_docstring(CBMExtractCtx *ctx, TSNode node) {
    return extract_member_docstring_status(ctx, node, NULL);
}

/* Go package comment: the comment group touching `package`, directives
 * dropped (go/doc). */
static const char *doc_go_package(CBMExtractCtx *ctx) {
    doc_trivia_t t;
    memset(&t, 0, sizeof(t));
    TSNode pkg = {0};
    bool found = false;
    TSTreeCursor cur = ts_tree_cursor_new(ctx->root);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode ch = ts_tree_cursor_current_node(&cur);
            const char *k = ts_node_type(ch);
            if (strcmp(k, "package_clause") == 0) {
                pkg = ch;
                found = true;
                break;
            }
            if (is_comment_node(k)) {
                doc_span_t sp = doc_span_of(ch, ctx->source, DOC_SPAN_COMMENT);
                doc_push_span(doc_scratch(ctx), &t, &sp);
            } else {
                t.count = 0;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    int last = t.count - SKIP_ONE;
    if (!found || last < 0 || ts_node_start_point(pkg).row > t.items[last].erow + SKIP_ONE) {
        return NULL;
    }
    int first = last;
    while (first > 0 && t.items[first].srow <= t.items[first - SKIP_ONE].erow + SKIP_ONE) {
        first--;
    }
    return doc_run_text(ctx, &t, first, last, true);
}

/* Rust inner docs (//! and the bang block) before the first item. */
static const char *doc_rust_inner(CBMExtractCtx *ctx) {
    doc_trivia_t t;
    memset(&t, 0, sizeof(t));
    TSTreeCursor cur = ts_tree_cursor_new(ctx->root);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode ch = ts_tree_cursor_current_node(&cur);
            const char *k = ts_node_type(ch);
            if (is_comment_node(k)) {
                doc_span_t sp = doc_span_of(ch, ctx->source, DOC_SPAN_COMMENT);
                if (sp.style == DOC_CS_LINE_BANG || sp.style == DOC_CS_BLOCK_BANG) {
                    doc_push_span(doc_scratch(ctx), &t, &sp);
                }
                continue;
            }
            if (strcmp(k, "inner_attribute_item") != 0) {
                break; /* the first item: the inner docs are over */
            }
        } while (ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    return t.count > 0 ? doc_run_text(ctx, &t, 0, t.count - SKIP_ONE, false) : NULL;
}

/* A file's own doc, for its File node: the Go package comment or the Rust
 * inner docs. */
static const char *extract_module_doc(CBMExtractCtx *ctx) {
    if (ctx->language == CBM_LANG_GO) {
        return doc_go_package(ctx);
    }
    return ctx->language == CBM_LANG_RUST ? doc_rust_inner(ctx) : NULL;
}

static int find_jvm_modifiers(TSNode node, CBMLanguage lang, TSNode *out, int max);

/* HTTP method names recognized in decorator calls (e.g., @router.post → "POST") */
static const char *decorator_method_name(const char *attr_text) {
    if (!attr_text) {
        return NULL;
    }
    /* Match the last segment after the dot: "router.post" → "post" */
    const char *dot = strrchr(attr_text, '.');
    const char *method = dot ? dot + SKIP_CHAR : attr_text;
    if (strcmp(method, "get") == 0 || strcmp(method, "Get") == 0) {
        return "GET";
    }
    if (strcmp(method, "post") == 0 || strcmp(method, "Post") == 0) {
        return "POST";
    }
    if (strcmp(method, "put") == 0 || strcmp(method, "Put") == 0) {
        return "PUT";
    }
    if (strcmp(method, "delete") == 0 || strcmp(method, "Delete") == 0) {
        return "DELETE";
    }
    if (strcmp(method, "patch") == 0 || strcmp(method, "Patch") == 0) {
        return "PATCH";
    }
    if (strcmp(method, "route") == 0 || strcmp(method, "api_route") == 0) {
        return "ANY";
    }
    return NULL;
}

/* HTTP method for a Spring/JAX-RS style annotation name (e.g. "GetMapping" →
 * "GET", "RequestMapping" → "ANY", JAX-RS "GET" → "GET"). Returns NULL when the
 * annotation is not a route-mapping annotation. */
static const char *annotation_route_method(const char *name) {
    if (!name) {
        return NULL;
    }
    if (strcmp(name, "GetMapping") == 0) {
        return "GET";
    }
    if (strcmp(name, "PostMapping") == 0) {
        return "POST";
    }
    if (strcmp(name, "PutMapping") == 0) {
        return "PUT";
    }
    if (strcmp(name, "DeleteMapping") == 0) {
        return "DELETE";
    }
    if (strcmp(name, "PatchMapping") == 0) {
        return "PATCH";
    }
    if (strcmp(name, "RequestMapping") == 0) {
        return "ANY";
    }
    /* JAX-RS bare-verb annotations (@GET/@POST/...) — path comes from @Path. */
    if (strcmp(name, "GET") == 0 || strcmp(name, "POST") == 0 || strcmp(name, "PUT") == 0 ||
        strcmp(name, "DELETE") == 0 || strcmp(name, "PATCH") == 0 || strcmp(name, "HEAD") == 0 ||
        strcmp(name, "OPTIONS") == 0) {
        return name;
    }
    return NULL;
}

/* Extract route path + method from a decorator's AST nodes.
 * Works for: @app.route("/path"), @router.post("/path"), @GetMapping("/path"),
 * @app.get("/path", ...), etc.
 *
 * Pure AST approach: walks the decorator node's call children to find:
 * 1. The function/attribute name → infer HTTP method
 * 2. The first string argument → route path */
// Find the arguments node for a decorator call node.
static TSNode find_decorator_args(TSNode call_node) {
    TSNode args = ts_node_child_by_field_name(call_node, TS_FIELD("arguments"));
    if (ts_node_is_null(args)) {
        for (uint32_t ai = 0; ai < ts_node_named_child_count(call_node); ai++) {
            TSNode ac = ts_node_named_child(call_node, ai);
            if (strcmp(ts_node_type(ac), "argument_list") == 0) {
                return ac;
            }
        }
    }
    return args;
}

static bool is_route_string_kind(const char *kind) {
    return strcmp(kind, "string") == 0 || strcmp(kind, "string_literal") == 0 ||
           strcmp(kind, "interpreted_string_literal") == 0;
}

static const char *route_path_from_string_node(CBMArena *a, TSNode node, const char *source,
                                               bool allow_relative) {
    if (!is_route_string_kind(ts_node_type(node))) {
        return NULL;
    }
    char *path = cbm_node_text(a, node, source);
    if (!path) {
        return NULL;
    }
    int plen = (int)strlen(path);
    if (plen >= PAIR_CHARS && (path[0] == '"' || path[0] == '\'')) {
        path = cbm_arena_strndup(a, path + SKIP_CHAR, (size_t)(plen - PAIR_CHARS));
    }
    if (!path || path[0] == '/' || !allow_relative) {
        return (path && path[0] == '/') ? path : NULL;
    }
    /* JAX-RS @Path values are relative URI templates; a leading slash is
     * optional and ignored by the framework. Route nodes use absolute-looking
     * paths consistently, so normalize a non-empty relative value here. An
     * empty @Path("") means "the class path itself": leave it unset so the
     * caller falls back exactly as it does for a method without @Path. */
    return path[0] ? cbm_arena_sprintf(a, "/%s", path) : NULL;
}

static const char *find_route_path_literal(CBMArena *a, TSNode node, const char *source,
                                           int max_depth, bool allow_relative) {
    if (ts_node_is_null(node) || max_depth < 0) {
        return NULL;
    }
    const char *path = route_path_from_string_node(a, node, source, allow_relative);
    if (path || max_depth == 0) {
        return path;
    }
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc && i < DECORATOR_SCAN_LIMIT; i++) {
        path = find_route_path_literal(a, ts_node_named_child(node, i), source, max_depth - 1,
                                       allow_relative);
        if (path) {
            return path;
        }
    }
    return NULL;
}

// Extract route path from decorator arguments. Generic mappings keep only
// slash-prefixed strings; JAX-RS @Path additionally accepts relative templates.
static const char *extract_route_path_from_args(CBMArena *a, TSNode args, const char *source,
                                                bool allow_relative) {
    /* Every argument is checked. Java and Kotlin put no order on annotation
     * attributes, so `path` can sit anywhere in the list. Stopping early left
     * a real route unread and formed no Route node. Each argument's own
     * subtree walk stays bounded by find_route_path_literal below. */
    uint32_t nc = ts_node_named_child_count(args);
    for (uint32_t ai = 0; ai < nc; ai++) {
        TSNode arg = ts_node_named_child(args, ai);
        /* Spring/Kotlin frequently uses named or array-valued annotation args:
         *   @RequestMapping(value = ["/internal/v1"])
         *   @GetMapping(path = {"/orders"})
         * Walk a bounded subtree and keep the first string literal that is
         * path-shaped, while ignoring non-route literals such as media types. */
        const char *path =
            find_route_path_literal(a, arg, source, CBM_DESCENDANT_MAX_DEPTH, allow_relative);
        if (path) {
            return path;
        }
    }
    return NULL;
}

// Find a keyword argument by name in an argument_list node and return its value child.
static TSNode find_drf_kwarg_in_args(CBMArena *a, TSNode args, const char *kwarg_name,
                                     const char *source) {
    uint32_t nc = ts_node_named_child_count(args);
    for (uint32_t ai = 0; ai < nc; ai++) {
        TSNode child = ts_node_named_child(args, ai);
        if (strcmp(ts_node_type(child), "keyword_argument") != 0)
            continue;
        TSNode name_node = ts_node_child_by_field_name(child, TS_FIELD("name"));
        if (ts_node_is_null(name_node))
            continue;
        char *name = cbm_node_text(a, name_node, source);
        if (name && strcmp(name, kwarg_name) == 0) {
            return ts_node_child_by_field_name(child, TS_FIELD("value"));
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Try to extract a route from a Django REST Framework @action decorator on a
// ViewSet method:
//   @action(detail=True, methods=["post"], url_path="approve")
// Sets *out_path/*out_method so the downstream Route+HANDLES pipeline (Phase 2a
// ensure_one_decorator_route) emits the handler->Route edge in the normal
// direction. Falls back to the method name for url_path and "GET" for methods.
// Known limitation: multi-method actions (methods=["get","post"]) capture only
// the first method -> a single Route rather than one per method.
static bool try_drf_action_decorator(CBMArena *a, TSNode dchild, const char *source,
                                     TSNode func_node, const char **out_path,
                                     const char **out_method) {
    TSNode fn = ts_node_child_by_field_name(dchild, TS_FIELD("function"));
    if (ts_node_is_null(fn)) {
        fn = ts_node_named_child(dchild, 0);
    }
    if (ts_node_is_null(fn)) {
        return false;
    }
    const char *fn_type = ts_node_type(fn);
    if (strcmp(fn_type, "identifier") != 0) {
        return false;
    }
    char *fn_text = cbm_node_text(a, fn, source);
    if (!fn_text || strcmp(fn_text, "action") != 0) {
        return false;
    }
    TSNode args = find_decorator_args(dchild);
    if (ts_node_is_null(args)) {
        return false;
    }
    const char *method = NULL;
    TSNode methods_val = find_drf_kwarg_in_args(a, args, "methods", source);
    if (!ts_node_is_null(methods_val) && strcmp(ts_node_type(methods_val), "list") == 0) {
        uint32_t mc = ts_node_named_child_count(methods_val);
        for (uint32_t mi = 0; mi < mc && !method; mi++) {
            TSNode item = ts_node_named_child(methods_val, mi);
            if (strcmp(ts_node_type(item), "string") != 0)
                continue;
            char *text = cbm_node_text(a, item, source);
            if (!text)
                continue;
            int tlen = (int)strlen(text);
            if (tlen < PAIR_CHARS || (text[0] != '"' && text[0] != '\''))
                continue;
            char inner[CBM_SZ_16];
            int ilen = tlen - PAIR_CHARS;
            if (ilen <= 0 || ilen >= (int)sizeof(inner))
                continue;
            memcpy(inner, text + SKIP_CHAR, (size_t)ilen);
            inner[ilen] = '\0';
            for (int ci = 0; inner[ci]; ci++) {
                if (inner[ci] >= 'a' && inner[ci] <= 'z')
                    inner[ci] -= 32;
            }
            method = cbm_arena_strdup(a, inner);
        }
    }
    if (!method) {
        method = "GET";
    }
    const char *segment = NULL;
    TSNode url_path_val = find_drf_kwarg_in_args(a, args, "url_path", source);
    if (!ts_node_is_null(url_path_val) && strcmp(ts_node_type(url_path_val), "string") == 0) {
        char *text = cbm_node_text(a, url_path_val, source);
        if (text) {
            int tlen = (int)strlen(text);
            if (tlen >= PAIR_CHARS && (text[0] == '"' || text[0] == '\'')) {
                segment = cbm_arena_strndup(a, text + SKIP_CHAR, (size_t)(tlen - PAIR_CHARS));
            }
        }
    }
    if (!segment) {
        TSNode name_node = func_name_node(func_node);
        if (!ts_node_is_null(name_node)) {
            segment = cbm_node_text(a, name_node, source);
        }
    }
    if (!segment) {
        return false;
    }
    // Extract detail kwarg (default True in DRF)
    bool detail = true;
    TSNode detail_val = find_drf_kwarg_in_args(a, args, "detail", source);
    if (!ts_node_is_null(detail_val)) {
        const char *dv = ts_node_type(detail_val);
        if (strcmp(dv, "false") == 0) {
            detail = false;
        }
    }
    if (detail) {
        *out_path = cbm_arena_sprintf(a, "/{pk}/%s", segment);
    } else {
        *out_path = cbm_arena_sprintf(a, "/%s", segment);
    }
    *out_method = method;
    return true;
}

// Try to extract a route from a single decorator call node.
// Returns true if a route method was found. The fallback path "/" applies only
// to a zero-argument call (`@app.route()`): a call that HAS arguments but none
// path-shaped is not a route -- unittest.mock's `@patch("subprocess.run")`
// shares its name with the HTTP verb and used to mint a PATCH "/" handler for
// every mocked test function (distilled from PR #1245).
static bool try_route_from_decorator_call(CBMArena *a, TSNode dchild, const char *source,
                                          const char **out_path, const char **out_method) {
    TSNode fn = ts_node_child_by_field_name(dchild, TS_FIELD("function"));
    if (ts_node_is_null(fn)) {
        fn = ts_node_named_child(dchild, 0);
    }
    if (ts_node_is_null(fn)) {
        return false;
    }

    char *fn_text = cbm_node_text(a, fn, source);
    const char *method = decorator_method_name(fn_text);
    if (!method) {
        return false;
    }

    TSNode args = find_decorator_args(dchild);
    if (!ts_node_is_null(args)) {
        const char *path = extract_route_path_from_args(a, args, source, false);
        if (path) {
            *out_path = path;
            *out_method = method;
            return true;
        }
        if (ts_node_named_child_count(args) > 0) {
            return false;
        }
    }
    *out_path = "/";
    *out_method = method;
    return true;
}

/* NestJS (and routing-controllers) method decorators: the verb is the bare,
 * capitalized decorator name — @Get, @Post, ... @All. Returns NULL otherwise. */
static const char *nest_decorator_method(const char *name) {
    static const struct {
        const char *decorator;
        const char *method;
    } verbs[] = {{"Get", "GET"},     {"Post", "POST"}, {"Put", "PUT"},         {"Delete", "DELETE"},
                 {"Patch", "PATCH"}, {"Head", "HEAD"}, {"Options", "OPTIONS"}, {"All", "ANY"}};
    for (size_t i = 0; name && i < sizeof(verbs) / sizeof(verbs[0]); i++) {
        if (strcmp(name, verbs[i].decorator) == 0) {
            return verbs[i].method;
        }
    }
    return NULL;
}

/* Unquote a TS `string` literal node and root it at "/": Nest writes route
 * segments without a leading slash (@Get(':id'), @Controller('users')). */
static const char *nest_path_from_string(CBMArena *a, TSNode node, const char *source) {
    if (ts_node_is_null(node) || strcmp(ts_node_type(node), "string") != 0) {
        return NULL;
    }
    char *text = cbm_node_text(a, node, source);
    size_t len = text ? strlen(text) : 0;
    if (len < PAIR_CHARS) {
        return NULL;
    }
    const char *inner = cbm_arena_strndup(a, text + SKIP_CHAR, len - PAIR_CHARS);
    if (!inner) {
        return NULL;
    }
    return inner[0] == '/' ? inner : cbm_arena_sprintf(a, "/%s", inner);
}

/* The route path a Nest decorator's argument list carries. Accepted shapes:
 *   ()                         -> "/"
 *   ('users') / (['a', 'b'])   -> the (first) string literal
 *   ({ path: 'users', ... })   -> the `path` property (Nest's options form)
 * Any other first argument (a constant, a template) has no statically known
 * path: NULL, so no Route is invented for it. */
static const char *nest_path_from_args(CBMArena *a, TSNode args, const char *source) {
    if (ts_node_is_null(args) || ts_node_named_child_count(args) == 0) {
        return "/";
    }
    TSNode arg = ts_node_named_child(args, 0);
    const char *kind = ts_node_type(arg);
    if (strcmp(kind, "array") == 0 && ts_node_named_child_count(arg) > 0) {
        return nest_path_from_string(a, ts_node_named_child(arg, 0), source);
    }
    if (strcmp(kind, "object") != 0) {
        return nest_path_from_string(a, arg, source);
    }
    uint32_t nc = ts_node_named_child_count(arg);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode pair = ts_node_named_child(arg, i);
        if (strcmp(ts_node_type(pair), "pair") != 0) {
            continue;
        }
        TSNode key = ts_node_child_by_field_name(pair, TS_FIELD("key"));
        char *key_text = ts_node_is_null(key) ? NULL : cbm_node_text(a, key, source);
        if (key_text && strcmp(key_text, "path") == 0) {
            return nest_path_from_string(a, ts_node_child_by_field_name(pair, TS_FIELD("value")),
                                         source);
        }
    }
    return "/"; /* options object without `path` (e.g. only `host`) */
}

/* A TS decorator's call_expression as (bare name, argument list). Returns the
 * name, or NULL when the callee is not a plain identifier. */
static const char *ts_decorator_call_name(CBMArena *a, TSNode call, const char *source,
                                          TSNode *out_args) {
    if (ts_node_is_null(call) || strcmp(ts_node_type(call), "call_expression") != 0) {
        return NULL;
    }
    TSNode fn = ts_node_child_by_field_name(call, TS_FIELD("function"));
    if (ts_node_is_null(fn) || strcmp(ts_node_type(fn), "identifier") != 0) {
        return NULL;
    }
    *out_args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
    return cbm_node_text(a, fn, source);
}

/* NestJS method route: @Get(':id') on a TS class method. The class-level
 * @Controller prefix is composed by the caller (nest_class_route_prefix). */
static bool try_route_from_ts_decorator_call(CBMArena *a, TSNode dchild, const char *source,
                                             const char **out_path, const char **out_method) {
    TSNode args = {0};
    const char *method = nest_decorator_method(ts_decorator_call_name(a, dchild, source, &args));
    if (!method) {
        return false;
    }
    const char *path = nest_path_from_args(a, args, source);
    if (!path) {
        return false;
    }
    *out_path = path;
    *out_method = method;
    return true;
}

/* The @Controller('users') prefix of a NestJS controller, from `decorator`
 * nodes that are children of the class node (`@X class C`) or its preceding
 * siblings (`@X export class C`: the decorator belongs to export_statement).
 * NULL when the class is not a controller. */
static const char *nest_prefix_from_decorator(CBMArena *a, TSNode dec, const char *source) {
    if (strcmp(ts_node_type(dec), "decorator") != 0) {
        return NULL;
    }
    TSNode args = {0};
    const char *name = ts_decorator_call_name(a, ts_node_named_child(dec, 0), source, &args);
    if (!name || (strcmp(name, "Controller") != 0 && strcmp(name, "JsonController") != 0)) {
        return NULL;
    }
    return nest_path_from_args(a, args, source);
}

static const char *nest_class_route_prefix(CBMArena *a, TSNode class_node, const char *source) {
    uint32_t cc = ts_node_named_child_count(class_node);
    for (uint32_t i = 0; i < cc; i++) {
        const char *p = nest_prefix_from_decorator(a, ts_node_named_child(class_node, i), source);
        if (p) {
            return p;
        }
    }
    for (TSNode prev = ts_node_prev_named_sibling(class_node); !ts_node_is_null(prev);
         prev = ts_node_prev_named_sibling(prev)) {
        const char *p = nest_prefix_from_decorator(a, prev, source);
        if (p) {
            return p;
        }
    }
    return NULL;
}

/* Resolve an annotation's name node across grammars. Java exposes a `name`
 * field; tree-sitter-kotlin does not — its annotation name lives in a nested
 * type_identifier:
 *   @Foo        -> (annotation (user_type (type_identifier)))
 *   @Foo("/x")  -> (annotation (constructor_invocation (user_type (type_identifier))
 *                                                      (value_arguments ...)))
 * Returns a null node when no name can be resolved. */
static TSNode annotation_name_node(TSNode annotation) {
    TSNode name = ts_node_child_by_field_name(annotation, TS_FIELD("name"));
    if (!ts_node_is_null(name)) {
        return name;
    }
    TSNode ut = cbm_find_child_by_kind(annotation, "user_type");
    if (ts_node_is_null(ut)) {
        TSNode ci = cbm_find_child_by_kind(annotation, "constructor_invocation");
        if (!ts_node_is_null(ci)) {
            ut = cbm_find_child_by_kind(ci, "user_type");
        }
    }
    if (!ts_node_is_null(ut)) {
        TSNode ti = cbm_find_child_by_kind(ut, "type_identifier");
        if (ts_node_is_null(ti)) {
            ti = cbm_find_child_by_kind(ut, "simple_identifier");
        }
        return ti;
    }
    TSNode null_node = {0};
    return null_node;
}

/* Resolve an annotation's argument list across grammars. Kotlin keeps the args
 * under a `constructor_invocation` child as a `value_arguments` node rather than
 * the `arguments` field / `argument_list` child that Java exposes. */
static TSNode annotation_args_node(TSNode annotation) {
    TSNode args = ts_node_child_by_field_name(annotation, TS_FIELD("arguments"));
    if (!ts_node_is_null(args)) {
        return args;
    }
    args = find_decorator_args(annotation);
    if (!ts_node_is_null(args)) {
        return args;
    }
    TSNode ci = cbm_find_child_by_kind(annotation, "constructor_invocation");
    if (!ts_node_is_null(ci)) {
        return cbm_find_child_by_kind(ci, "value_arguments");
    }
    return args;
}

/* Try to extract a route from a Java/JVM/Kotlin annotation node (`annotation` or
 * `marker_annotation`). Spring mapping annotations carry the HTTP method in the
 * annotation name and the path in the (optional) argument list:
 *   @GetMapping("/orders")  @RequestMapping(value="/api")  @PostMapping
 * Returns true when the annotation is a route-mapping annotation. */
static bool try_route_from_annotation(CBMArena *a, TSNode annotation, const char *source,
                                      const char **out_path, const char **out_method) {
    TSNode name_node = annotation_name_node(annotation);
    if (ts_node_is_null(name_node)) {
        return false;
    }
    char *name = cbm_node_text(a, name_node, source);
    const char *method = annotation_route_method(name);
    if (!method) {
        return false;
    }
    TSNode args = annotation_args_node(annotation);
    const char *path = NULL;
    if (!ts_node_is_null(args)) {
        path = extract_route_path_from_args(a, args, source, false);
    }
    *out_path = path ? path : "/";
    *out_method = method;
    return true;
}

/* Scan ALL annotation nodes nested in a JVM/C# `modifiers`/`attribute_list`
 * wrapper (and direct children), collecting route information from the whole
 * set instead of stopping at the first mapping annotation. Java/Kotlin Spring
 * annotations (@GetMapping, @RequestMapping, ...) live here rather than as
 * prev-siblings, so the prev-sibling decorator walk never sees them.
 *
 * JAX-RS splits the route across two annotations: the verb comes from a bare
 * @GET/@POST/... and the path from a sibling @Path("..."). Returning on the
 * first mapping annotation therefore dropped every method-level @Path
 * (the @GET matched first, defaulted the path to "/", and @Path was never
 * read), and class-level @Path prefixes were never recognized at all. */
static void scan_route_annotations(CBMArena *a, TSNode owner, const char *source,
                                   const CBMLangSpec *spec, const char **out_map_path,
                                   const char **out_method, const char **out_jax_path) {
    *out_map_path = NULL;
    *out_method = NULL;
    *out_jax_path = NULL;

    /* MINUS_1: the owner node itself is appended below, after the wrappers. */
    TSNode wrappers[MAX_ATTR_WRAPPERS];
    int wn = find_jvm_modifiers(owner, spec->language, wrappers, MAX_ATTR_WRAPPERS_MINUS_1);
    /* Direct-child annotations (some grammars attach the annotation as a child
     * of the method node rather than under `modifiers`). */
    wrappers[wn++] = owner;

    for (int w = 0; w < wn; w++) {
        uint32_t cc = ts_node_child_count(wrappers[w]);
        for (uint32_t ci = 0; ci < cc; ci++) {
            TSNode child = ts_node_child(wrappers[w], ci);
            if (!cbm_kind_in_set(child, spec->decorator_node_types)) {
                continue;
            }
            TSNode name_node = annotation_name_node(child);
            if (ts_node_is_null(name_node)) {
                continue;
            }
            char *name = cbm_node_text(a, name_node, source);
            if (!name) {
                continue;
            }
            if (!*out_jax_path && strcmp(name, "Path") == 0) {
                TSNode args = annotation_args_node(child);
                if (!ts_node_is_null(args)) {
                    *out_jax_path = extract_route_path_from_args(a, args, source, true);
                }
                continue;
            }
            if (!*out_method) {
                const char *method = annotation_route_method(name);
                if (method) {
                    *out_method = method;
                    TSNode args = annotation_args_node(child);
                    if (!ts_node_is_null(args)) {
                        *out_map_path = extract_route_path_from_args(a, args, source, false);
                    }
                }
            }
        }
    }
}

static bool extract_route_from_annotations(CBMArena *a, TSNode func_node, const char *source,
                                           const CBMLangSpec *spec, const char **out_path,
                                           const char **out_method) {
    const char *map_path = NULL;
    const char *method = NULL;
    const char *jax_path = NULL;
    scan_route_annotations(a, func_node, source, spec, &map_path, &method, &jax_path);
    /* Method-level routes still require a verb/mapping annotation; a lone
     * @Path (JAX-RS sub-resource locator) is not an endpoint by itself. */
    if (!method) {
        return false;
    }
    *out_method = method;
    *out_path = map_path ? map_path : (jax_path ? jax_path : "/");
    return true;
}

static void extract_route_from_decorators(CBMArena *a, TSNode func_node, const char *source,
                                          const CBMLangSpec *spec, const char **out_path,
                                          const char **out_method) {
    *out_path = NULL;
    *out_method = NULL;

    if (!spec->decorator_node_types || !spec->decorator_node_types[0]) {
        return;
    }

    TSNode prev = ts_node_prev_sibling(func_node);
    while (!ts_node_is_null(prev)) {
        if (!cbm_kind_in_set(prev, spec->decorator_node_types)) {
            break;
        }

        uint32_t dc = ts_node_named_child_count(prev);
        for (uint32_t di = 0; di < dc; di++) {
            TSNode dchild = ts_node_named_child(prev, di);
            if (try_route_from_ts_decorator_call(a, dchild, source, out_path, out_method)) {
                return;
            }
            if (strcmp(ts_node_type(dchild), "call") != 0) {
                continue;
            }
            if (try_route_from_decorator_call(a, dchild, source, out_path, out_method)) {
                return;
            }
            if (try_drf_action_decorator(a, dchild, source, func_node, out_path, out_method)) {
                return;
            }
        }
        /* JVM/C# annotation-form route mapping (Spring @GetMapping etc.) — the
         * prev-sibling itself may be the annotation node. */
        if (try_route_from_annotation(a, prev, source, out_path, out_method)) {
            return;
        }
        prev = ts_node_prev_sibling(prev);
    }

    /* Spring/JAX-RS annotations live inside the method's `modifiers` child, not
     * as prev-siblings — scan there too. */
    extract_route_from_annotations(a, func_node, source, spec, out_path, out_method);
}

static const char *join_route_paths(CBMArena *a, const char *prefix, const char *path) {
    if (!path || !path[0]) {
        return prefix;
    }
    if (!prefix || !prefix[0] || strcmp(prefix, "/") == 0) {
        return path;
    }
    if (strcmp(path, "/") == 0) {
        return prefix;
    }
    size_t plen = strlen(prefix);
    bool prefix_slash = prefix[plen - 1] == '/';
    bool path_slash = path[0] == '/';
    if (prefix_slash && path_slash) {
        return cbm_arena_sprintf(a, "%s%s", prefix, path + SKIP_CHAR);
    }
    if (!prefix_slash && !path_slash) {
        return cbm_arena_sprintf(a, "%s/%s", prefix, path);
    }
    return cbm_arena_sprintf(a, "%s%s", prefix, path);
}

static const char *spring_class_route_prefix(CBMArena *a, TSNode class_node, const char *source,
                                             const CBMLangSpec *spec) {
    const char *map_path = NULL;
    const char *method = NULL;
    const char *jax_path = NULL;
    scan_route_annotations(a, class_node, source, spec, &map_path, &method, &jax_path);
    if (map_path) {
        return map_path; /* @RequestMapping("/api") and friends */
    }
    if (jax_path) {
        return jax_path; /* JAX-RS class-level @Path("/api") carries no verb */
    }
    if (method) {
        return "/"; /* mapping annotation without a path argument */
    }
    return NULL;
}

// Extract decorator names from preceding decorator/annotation nodes
// Count annotations inside a Java/Kotlin/C# "modifiers" node.
static int count_modifier_annotations(TSNode modifiers, const CBMLangSpec *spec) {
    int count = 0;
    uint32_t mc = ts_node_child_count(modifiers);
    for (uint32_t mi = 0; mi < mc; mi++) {
        TSNode mchild = ts_node_child(modifiers, mi);
        if (cbm_kind_in_set(mchild, spec->decorator_node_types)) {
            count++;
        }
    }
    return count;
}

// Find every wrapper child that holds annotations/attributes for languages
// where they are nested under an intermediate node rather than being a
// prev-sibling:
//   Java/Kotlin/Swift → `modifiers` (one node, contains every annotation)
//   C#/PHP 8          → `attribute_list` (contains attribute/attribute_group)
//
// C#/PHP attribute stacks are NOT a single wrapper: each bracketed group
// (`[Foo]`, `[Bar]`, ...) compiles to its own `attribute_list` node, so
// `[A] [B] [C]` above a declaration produces three separate `attribute_list`
// siblings among that declaration's children — not one `attribute_list`
// holding three entries. A field-name lookup (`ts_node_child_by_field_name`)
// only ever returns the first child registered under a given field, so using
// it here silently dropped every attribute after the first bracket group
// (#1692). Scanning all children by kind fixes that for C#/PHP and is a
// no-op change for Java/Kotlin/Swift, where `modifiers` never repeats.
// Writes up to `max` wrapper nodes into `out`; returns how many were found.
static int find_jvm_modifiers(TSNode node, CBMLanguage lang, TSNode *out, int max) {
    const char *wrapper = NULL;
    switch (lang) {
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_SWIFT:
        wrapper = "modifiers";
        break;
    case CBM_LANG_CSHARP:
    case CBM_LANG_PHP:
        wrapper = "attribute_list";
        break;
    default:
        return 0;
    }
    return cbm_find_children_by_kind(node, wrapper, out, max);
}

// Count direct children of `node` that are decorator/annotation nodes (used by
// languages like Scala where the annotation is a direct child of the def node).
static int count_child_decorators(TSNode node, const CBMLangSpec *spec) {
    int count = 0;
    uint32_t cc = ts_node_child_count(node);
    for (uint32_t ci = 0; ci < cc; ci++) {
        TSNode child = ts_node_child(node, ci);
        if (cbm_kind_in_set(child, spec->decorator_node_types)) {
            count++;
        }
    }
    return count;
}

// Collect direct-child decorator texts into result[] starting at idx.
static int collect_child_decorators(CBMArena *a, TSNode node, const char *source,
                                    const CBMLangSpec *spec, const char **result, int idx,
                                    int max) {
    uint32_t cc = ts_node_child_count(node);
    for (uint32_t ci = 0; ci < cc && idx < max; ci++) {
        TSNode child = ts_node_child(node, ci);
        if (cbm_kind_in_set(child, spec->decorator_node_types)) {
            result[idx++] = cbm_node_text(a, child, source);
        }
    }
    return idx;
}

// Collect decorator texts from a modifiers node into result array starting at idx.
static int collect_modifier_decorators(CBMArena *a, TSNode modifiers, const char *source,
                                       const CBMLangSpec *spec, const char **result, int idx,
                                       int max) {
    uint32_t mc = ts_node_child_count(modifiers);
    for (uint32_t mi = 0; mi < mc && idx < max; mi++) {
        TSNode mchild = ts_node_child(modifiers, mi);
        if (cbm_kind_in_set(mchild, spec->decorator_node_types)) {
            result[idx++] = cbm_node_text(a, mchild, source);
        }
    }
    return idx;
}

/* Comments are NAMED nodes in tree-sitter, so a comment interleaved in a
 * decorator run would end the walk and silently drop every decorator above it:
 *
 *   @Post('login')                    <-- lost
 *   @HttpCode(HttpStatus.OK)          <-- lost
 *   // why this route is throttled    <-- walk stopped here
 *   @Throttle({ ... })                <-- kept
 *   async login(...)
 *
 * Documenting a decorator must not make it disappear from the graph, so treat
 * comments as transparent — like the anonymous tokens already skipped below.
 * (is_comment_node() is defined above, near the docstring helpers.) */
static const char **extract_decorators(CBMArena *a, TSNode node, const char *source,
                                       CBMLanguage lang, const CBMLangSpec *spec) {
    if (!spec->decorator_node_types || !spec->decorator_node_types[0]) {
        return NULL;
    }

    int count = 0;
    TSNode prev = ts_node_prev_sibling(node);
    while (!ts_node_is_null(prev)) {
        if (cbm_kind_in_set(prev, spec->decorator_node_types)) {
            count++;
        } else if (ts_node_is_named(prev) && !is_comment_node(ts_node_type(prev))) {
            /* A real preceding construct ends the decorator run. Anonymous
             * tokens (e.g. TS `export` between `@Decorator` and the
             * `class_declaration`) and comments are skipped so the decorator
             * is still seen. */
            break;
        }
        prev = ts_node_prev_sibling(prev);
    }

    TSNode wrappers[MAX_ATTR_WRAPPERS];
    int wn = 0;
    int mod_count = 0;
    int child_count = 0;
    if (count == 0) {
        wn = find_jvm_modifiers(node, lang, wrappers, MAX_ATTR_WRAPPERS);
        for (int w = 0; w < wn; w++) {
            mod_count += count_modifier_annotations(wrappers[w], spec);
        }
        /* Languages like Scala attach the annotation directly as a child of the
         * definition node (no wrapper, no prev-sibling). */
        if (mod_count == 0) {
            child_count = count_child_decorators(node, spec);
        }
    }

    int total = count + mod_count + child_count;
    if (total == 0) {
        return NULL;
    }

    const char **result =
        (const char **)cbm_arena_alloc(a, sizeof(const char *) * (total + NULL_TERM));
    if (!result) {
        return NULL;
    }

    int idx = 0;
    prev = ts_node_prev_sibling(node);
    while (!ts_node_is_null(prev) && idx < count) {
        if (cbm_kind_in_set(prev, spec->decorator_node_types)) {
            result[idx++] = cbm_node_text(a, prev, source);
        } else if (ts_node_is_named(prev) && !is_comment_node(ts_node_type(prev))) {
            break;
        }
        prev = ts_node_prev_sibling(prev);
    }
    for (int w = 0; w < wn && mod_count > 0; w++) {
        idx = collect_modifier_decorators(a, wrappers[w], source, spec, result, idx, total);
    }
    if (child_count > 0) {
        idx = collect_child_decorators(a, node, source, spec, result, idx, total);
    }
    result[idx] = NULL;
    return result;
}

/* Rust: mark a function as a test when it carries a test attribute (#855).
 * cbm's test detection is otherwise file-path-based (cbm_is_test_file:
 * *_test.rs / test_*), so inline #[test]/#[tokio::test] functions inside a
 * regular .rs file are indexed as ordinary Functions (is_test=false) and leak
 * past the store.c `is_test != 1` filter into graph/agent context. The
 * attribute_item text extract_decorators stores is the bracketed form
 * ("#[test]", "#[tokio::test]", "#[tokio::test(...)]", ...). */
static bool rust_def_is_test(const char *const *decorators) {
    if (!decorators) {
        return false;
    }
    for (int i = 0; decorators[i]; i++) {
        const char *d = decorators[i];
        /* Path-qualified async/param test macros (substring match, robust to the
         * optional argument list and the surrounding #[ ]). */
        if (strstr(d, "tokio::test") || strstr(d, "async_std::test") ||
            strstr(d, "actix_rt::test") || strstr(d, "test_case::case")) {
            return true;
        }
        /* Bare #[test] / #[test(...)]: match the bracketed path exactly so we do
         * NOT match the unrelated #[test_case::case] (handled above) or a
         * hypothetical #[test_crate]. */
        if (strstr(d, "#[test]") || strstr(d, "#[test(")) {
            return true;
        }
    }
    return false;
}

// Extract base class name text from a single base_class child node.
static char *extract_cpp_base_text(CBMArena *a, TSNode bc, const char *source) {
    const char *bk = ts_node_type(bc);
    if (strcmp(bk, "access_specifier") == 0) {
        return NULL;
    }
    if (strcmp(bk, "type_identifier") == 0 || strcmp(bk, "qualified_identifier") == 0 ||
        strcmp(bk, "scoped_identifier") == 0) {
        /* A qualified base may embed a template_type in its `name` field
         * (e.g. `std::vector<T>`), so the raw text carries `<...>` args.
         * Strip the generic argument list to keep the bare qualified name. */
        char *t = cbm_node_text(a, bc, source);
        if (t) {
            char *angle = strchr(t, '<');
            if (angle) {
                *angle = '\0';
            }
        }
        return t;
    }
    if (strcmp(bk, "template_type") == 0) {
        TSNode tname = ts_node_child_by_field_name(bc, TS_FIELD("name"));
        if (!ts_node_is_null(tname)) {
            return cbm_node_text(a, tname, source);
        }
    }
    return NULL;
}

// Extract base classes from a C++ base_class_clause node.
static const char **extract_cpp_base_classes(CBMArena *a, TSNode clause, const char *source) {
    const char *bases[MAX_BASES];
    int base_count = 0;
    uint32_t bnc = ts_node_named_child_count(clause);
    for (uint32_t bi = 0; bi < bnc && base_count < MAX_BASES_MINUS_1; bi++) {
        char *text = extract_cpp_base_text(a, ts_node_named_child(clause, bi), source);
        if (text && text[0]) {
            bases[base_count++] = text;
        }
    }
    if (base_count > 0) {
        const char **result =
            (const char **)cbm_arena_alloc(a, (base_count + NULL_TERM) * sizeof(const char *));
        if (result) {
            for (int j = 0; j < base_count; j++) {
                result[j] = bases[j];
            }
            result[base_count] = NULL;
            return result;
        }
    }
    return NULL;
}

// Build a single-element NULL-terminated base class array.
static const char **make_single_base(CBMArena *a, const char *text) {
    if (!text || !text[0]) {
        return NULL;
    }
    const char **result = (const char **)cbm_arena_alloc(a, sizeof(const char *) * RT_PAIR_SIZE);
    if (result) {
        result[0] = text;
        result[SKIP_CHAR] = NULL;
    }
    return result;
}

// Search children for a child matching one of the base_types and return its text as single base.
static const char **find_base_from_children(CBMArena *a, TSNode node, const char *source,
                                            const char **base_types) {
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        for (const char **t = base_types; *t; t++) {
            if (strcmp(ck, *t) == 0) {
                const char **r = make_single_base(a, cbm_node_text(a, child, source));
                if (r) {
                    return r;
                }
            }
        }
    }
    return NULL;
}

/* Extract text from a single C# base_list named child, stripping generic args. */
static const char *extract_csharp_base_child_text(CBMArena *a, TSNode bc, const char *source) {
    const char *bk = ts_node_type(bc);
    char *text = NULL;
    if (strcmp(bk, "identifier") == 0 || strcmp(bk, "generic_name") == 0 ||
        strcmp(bk, "qualified_name") == 0) {
        text = cbm_node_text(a, bc, source);
    } else {
        TSNode inner = ts_node_named_child(bc, 0);
        if (!ts_node_is_null(inner)) {
            text = cbm_node_text(a, inner, source);
        }
    }
    if (text && text[0]) {
        char *angle = strchr(text, '<');
        if (angle) {
            *angle = '\0';
        }
        return text;
    }
    return NULL;
}

/* Collect bases from a single base_list node into an arena-allocated array. */
static const char **collect_csharp_bases(CBMArena *a, TSNode base_list, const char *source) {
    const char *bases[MAX_BASES];
    int base_count = 0;
    uint32_t bnc = ts_node_named_child_count(base_list);
    for (uint32_t bi = 0; bi < bnc && base_count < MAX_BASES_MINUS_1; bi++) {
        const char *text =
            extract_csharp_base_child_text(a, ts_node_named_child(base_list, bi), source);
        if (text) {
            bases[base_count++] = text;
        }
    }
    if (base_count == 0) {
        return NULL;
    }
    const char **result =
        (const char **)cbm_arena_alloc(a, (base_count + NULL_TERM) * sizeof(const char *));
    if (!result) {
        return NULL;
    }
    for (int j = 0; j < base_count; j++) {
        result[j] = bases[j];
    }
    result[base_count] = NULL;
    return result;
}

/* C# base_list: iterate children, find base_list node, extract bases. */
static const char **extract_csharp_base_list(CBMArena *a, TSNode node, const char *source,
                                             uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (strcmp(ts_node_type(child), "base_list") != 0) {
            continue;
        }
        const char **result = collect_csharp_bases(a, child, source);
        if (result) {
            return result;
        }
    }
    return NULL;
}

// Append a base name (generic args stripped) to out[] if non-empty.
static void push_base_text(CBMArena *a, TSNode n, const char *source, const char **out, int out_cap,
                           int *count) {
    if (*count >= out_cap) {
        return;
    }
    char *t = cbm_node_text(a, n, source);
    if (!t) {
        return;
    }
    char *angle = strchr(t, '<');
    if (angle) {
        *angle = '\0';
    }
    /* PHP qualified base may be backslash-prefixed (e.g. `\RuntimeException`);
     * keep the bare class name so it matches the unqualified declaration. */
    char *last_bs = strrchr(t, '\\');
    if (last_bs) {
        t = last_bs + 1;
    }
    if (t[0]) {
        out[(*count)++] = t;
    }
}

/* TypeScript/TSX: bases live in a `class_heritage` (class) or directly in an
 * `extends_type_clause` (interface).  The extractor previously captured the
 * literal "extends"/"implements" keyword text instead of the type names. */
static int collect_ts_bases(CBMArena *a, TSNode clause, const char *source, const char **out,
                            int out_cap, int *count) {
    const char *kk = ts_node_type(clause);
    if (strcmp(kk, "extends_clause") == 0) {
        /* `extends_clause` carries the superclass in its `value` field. */
        TSNode v = ts_node_child_by_field_name(clause, TS_FIELD("value"));
        if (!ts_node_is_null(v)) {
            push_base_text(a, v, source, out, out_cap, count);
        }
        return *count;
    }
    if (strcmp(kk, "implements_clause") == 0 || strcmp(kk, "extends_type_clause") == 0) {
        /* Named children are the implemented/extended types (possibly generic). */
        uint32_t nc = ts_node_named_child_count(clause);
        for (uint32_t i = 0; i < nc && *count < out_cap; i++) {
            TSNode c = ts_node_named_child(clause, i);
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "type_arguments") == 0) {
                continue;
            }
            if (strcmp(ck, "generic_type") == 0) {
                TSNode nm = ts_node_child_by_field_name(c, TS_FIELD("name"));
                if (!ts_node_is_null(nm)) {
                    push_base_text(a, nm, source, out, out_cap, count);
                    continue;
                }
            }
            push_base_text(a, c, source, out, out_cap, count);
        }
    }
    return *count;
}

/* TypeScript: walk the class_heritage container (which holds extends_clause +
 * implements_clause), or handle a bare interface extends_type_clause. */
static const char **extract_ts_bases(CBMArena *a, TSNode node, const char *source) {
    const char *bases[MAX_BASES];
    int count = 0;
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "class_heritage") == 0) {
            uint32_t hc = ts_node_child_count(child);
            for (uint32_t j = 0; j < hc; j++) {
                collect_ts_bases(a, ts_node_child(child, j), source, bases, MAX_BASES_MINUS_1,
                                 &count);
            }
        } else if (strcmp(ck, "extends_type_clause") == 0) {
            collect_ts_bases(a, child, source, bases, MAX_BASES_MINUS_1, &count);
        }
    }
    if (count == 0) {
        return NULL;
    }
    const char **result =
        (const char **)cbm_arena_alloc(a, (size_t)(count + NULL_TERM) * sizeof(const char *));
    if (!result) {
        return NULL;
    }
    for (int i = 0; i < count; i++) {
        result[i] = bases[i];
    }
    result[count] = NULL;
    return result;
}

/* PHP: bases live in `base_clause` (extends) and `class_interface_clause`
 * (implements) child nodes; named children are `name`/`qualified_name`. */
static const char **extract_php_bases(CBMArena *a, TSNode node, const char *source) {
    const char *bases[MAX_BASES];
    int count = 0;
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "base_clause") != 0 && strcmp(ck, "class_interface_clause") != 0) {
            continue;
        }
        uint32_t cc = ts_node_named_child_count(child);
        for (uint32_t j = 0; j < cc && count < MAX_BASES_MINUS_1; j++) {
            push_base_text(a, ts_node_named_child(child, j), source, bases, MAX_BASES_MINUS_1,
                           &count);
        }
    }
    if (count == 0) {
        return NULL;
    }
    const char **result =
        (const char **)cbm_arena_alloc(a, (size_t)(count + NULL_TERM) * sizeof(const char *));
    if (!result) {
        return NULL;
    }
    for (int i = 0; i < count; i++) {
        result[i] = bases[i];
    }
    result[count] = NULL;
    return result;
}

/* Kotlin: supertypes live in `delegation_specifier` children.  Each holds
 * either a bare `user_type` (interface) or a `constructor_invocation` whose
 * `user_type` is the superclass.  Descend to the `type_identifier`. */
static const char **extract_kotlin_bases(CBMArena *a, TSNode node, const char *source) {
    const char *bases[MAX_BASES];
    int count = 0;
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc && count < MAX_BASES_MINUS_1; i++) {
        TSNode child = ts_node_child(node, i);
        if (strcmp(ts_node_type(child), "delegation_specifier") != 0) {
            continue;
        }
        /* Find the user_type (directly or under a constructor_invocation). */
        TSNode ut = ts_node_named_child(child, 0);
        if (!ts_node_is_null(ut) && strcmp(ts_node_type(ut), "constructor_invocation") == 0) {
            ut = ts_node_named_child(ut, 0);
        }
        if (ts_node_is_null(ut)) {
            continue;
        }
        /* user_type → type_identifier (first child); strip generic args. */
        TSNode ti = ut;
        if (strcmp(ts_node_type(ut), "user_type") == 0 && ts_node_named_child_count(ut) > 0) {
            ti = ts_node_named_child(ut, 0);
        }
        push_base_text(a, ti, source, bases, MAX_BASES_MINUS_1, &count);
    }
    if (count == 0) {
        return NULL;
    }
    const char **result =
        (const char **)cbm_arena_alloc(a, (size_t)(count + NULL_TERM) * sizeof(const char *));
    if (!result) {
        return NULL;
    }
    for (int i = 0; i < count; i++) {
        result[i] = bases[i];
    }
    result[count] = NULL;
    return result;
}

// Walk a field node and collect type identifier names into out[].
// Handles: direct type_identifier/generic_type/qualified_name, type_list children
// (Java interfaces list), and raw text fallback (other languages).
static int collect_bases_from_field(CBMArena *a, TSNode field_node, const char *source,
                                    const char **out, int out_cap) {
    int count = 0;
    const char *fk = ts_node_type(field_node);

    // If the field node itself is a type node, extract directly.
    if (strcmp(fk, "type_identifier") == 0 || strcmp(fk, "generic_type") == 0 ||
        strcmp(fk, "qualified_name") == 0 || strcmp(fk, "scoped_type_identifier") == 0 ||
        strcmp(fk, "user_type") == 0) {
        char *t = cbm_node_text(a, field_node, source);
        if (t) {
            char *angle = strchr(t, '<');
            if (angle) {
                *angle = '\0';
            }
            if (t[0] && count < out_cap) {
                out[count++] = t;
            }
        }
        return count;
    }

    // Walk named children: look for type identifiers or type_list/interface_type_list.
    uint32_t nc = ts_node_named_child_count(field_node);
    for (uint32_t i = 0; i < nc && count < out_cap; i++) {
        TSNode child = ts_node_named_child(field_node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "type_identifier") == 0 || strcmp(ck, "generic_type") == 0 ||
            strcmp(ck, "qualified_name") == 0 || strcmp(ck, "scoped_type_identifier") == 0 ||
            strcmp(ck, "user_type") == 0 ||
            /* Python `class C(Base)` carries the base as a bare `identifier`
             * inside the `superclasses` argument_list (and `attribute` for a
             * dotted base like `mod.Base`). Without these the raw-text fallback
             * below captured the whole "(Base)" field text, which never
             * resolved -> zero INHERITS edges for Python subclasses. */
            strcmp(ck, "identifier") == 0 || strcmp(ck, "attribute") == 0 ||
            /* Ruby `class C < Base` wraps the base in a `superclass` node whose
             * child is a `constant` (or `scope_resolution` for `A::B`). Without
             * these the raw-text fallback captured "< Base" (operator included),
             * which never resolves — breaking INHERITS and the Ruby LSP's
             * superclass chain. */
            strcmp(ck, "constant") == 0 || strcmp(ck, "scope_resolution") == 0) {
            char *t = cbm_node_text(a, child, source);
            if (t) {
                char *angle = strchr(t, '<');
                if (angle) {
                    *angle = '\0';
                }
                if (t[0]) {
                    out[count++] = t;
                }
            }
        } else if (strcmp(ck, "subscript") == 0) {
            /* Python parameterized base, e.g. `class S(Generic[T])`: the base
             * type is the `value` field of the subscript; the bracketed type
             * args must not leak into the stored name. */
            TSNode val = ts_node_child_by_field_name(child, TS_FIELD("value"));
            if (ts_node_is_null(val) && ts_node_named_child_count(child) > 0) {
                val = ts_node_named_child(child, 0);
            }
            if (!ts_node_is_null(val)) {
                char *t = cbm_node_text(a, val, source);
                if (t && t[0]) {
                    out[count++] = t;
                }
            }
        } else if (strcmp(ck, "type_list") == 0 || strcmp(ck, "interface_type_list") == 0) {
            // Java: super_interfaces contains type_list with multiple type_identifiers.
            uint32_t tlnc = ts_node_named_child_count(child);
            for (uint32_t ti = 0; ti < tlnc && count < out_cap; ti++) {
                TSNode tl_child = ts_node_named_child(child, ti);
                const char *tlk = ts_node_type(tl_child);
                if (strcmp(tlk, "type_identifier") == 0 || strcmp(tlk, "generic_type") == 0 ||
                    strcmp(tlk, "qualified_name") == 0) {
                    char *t = cbm_node_text(a, tl_child, source);
                    if (t) {
                        char *angle = strchr(t, '<');
                        if (angle) {
                            *angle = '\0';
                        }
                        if (t[0]) {
                            out[count++] = t;
                        }
                    }
                }
            }
        }
    }

    // Fallback: raw node text (for languages where the field node is the type name directly).
    if (count == 0) {
        char *t = cbm_node_text(a, field_node, source);
        if (t && t[0] && count < out_cap) {
            out[count++] = t;
        }
    }

    return count;
}

// Extract base class names from a class node.
// Julia: a subtype declaration `Foo <: Bar` is a `binary_expression` (operator
// `<:`) inside the def's `type_head`. The base is the RHS identifier.
static const char **extract_julia_base_classes(CBMArena *a, TSNode node, const char *source) {
    TSNode th = cbm_find_child_by_kind(node, "type_head");
    if (ts_node_is_null(th)) {
        return NULL;
    }
    TSNode inner = ts_node_named_child_count(th) > 0 ? ts_node_named_child(th, 0) : th;
    if (ts_node_is_null(inner) || strcmp(ts_node_type(inner), "binary_expression") != 0 ||
        ts_node_named_child_count(inner) < 2) {
        return NULL;
    }
    TSNode base = ts_node_named_child(inner, ts_node_named_child_count(inner) - 1);
    char *bname = cbm_node_text(a, base, source);
    if (!bname || !bname[0]) {
        return NULL;
    }
    const char **result = (const char **)cbm_arena_alloc(a, 2 * sizeof(const char *));
    if (!result) {
        return NULL;
    }
    result[0] = bname;
    result[1] = NULL;
    return result;
}

static const char **extract_base_classes(CBMArena *a, TSNode node, const char *source,
                                         CBMLanguage lang) {
    // ObjectScript: `Class X Extends (A, B)` — bases are class_name children of
    // the class_extends node.
    if (lang == CBM_LANG_OBJECTSCRIPT_UDL) {
        TSNode ext = cbm_find_child_by_kind(node, "class_extends");
        if (!ts_node_is_null(ext)) {
            const char *bases[MAX_BASES];
            int base_count = 0;
            uint32_t nc = ts_node_named_child_count(ext);
            for (uint32_t i = 0; i < nc && base_count < MAX_BASES_MINUS_1; i++) {
                TSNode ch = ts_node_named_child(ext, i);
                if (strcmp(ts_node_type(ch), "class_name") == 0) {
                    char *base = cbm_node_text(a, ch, source);
                    if (base && base[0]) {
                        bases[base_count++] = base;
                    }
                }
            }
            if (base_count > 0) {
                const char **result =
                    (const char **)cbm_arena_alloc(a, (base_count + 1) * sizeof(const char *));
                if (result) {
                    for (int i = 0; i < base_count; i++) {
                        result[i] = bases[i];
                    }
                    result[base_count] = NULL;
                    return result;
                }
            }
        }
        return NULL;
    }
    // Languages whose heritage is not exposed via a tree-sitter field need
    // dedicated walkers; the generic field/keyword path mis-captures them.
    if (lang == CBM_LANG_TYPESCRIPT || lang == CBM_LANG_TSX || lang == CBM_LANG_ARKTS) {
        const char **ts_result = extract_ts_bases(a, node, source);
        if (ts_result) {
            return ts_result;
        }
    }
    if (lang == CBM_LANG_PHP) {
        const char **php_result = extract_php_bases(a, node, source);
        if (php_result) {
            return php_result;
        }
    }
    if (lang == CBM_LANG_KOTLIN) {
        const char **kt_result = extract_kotlin_bases(a, node, source);
        if (kt_result) {
            return kt_result;
        }
    }
    // Squirrel: `class Dog extends Animal` — heritage is an `identifier` child
    // directly following the `extends` keyword (no field). Grab it.
    if (lang == CBM_LANG_SQUIRREL) {
        uint32_t sc = ts_node_child_count(node);
        bool seen_extends = false;
        for (uint32_t i = 0; i < sc; i++) {
            TSNode child = ts_node_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "extends") == 0) {
                seen_extends = true;
                continue;
            }
            if (seen_extends && strcmp(ck, "identifier") == 0) {
                char *base = cbm_node_text(a, child, source);
                if (base && base[0]) {
                    const char **result =
                        (const char **)cbm_arena_alloc(a, 2 * sizeof(const char *));
                    if (result) {
                        result[0] = base;
                        result[1] = NULL;
                        return result;
                    }
                }
                break;
            }
        }
    }
    if (lang == CBM_LANG_JULIA) {
        const char **jb = extract_julia_base_classes(a, node, source);
        if (jb) {
            return jb;
        }
    }
    /* F#: `inherit Base(...)` appears as a `class_inherits_decl` descendant of
     * the type_definition; the base type is the `simple_type` it carries. */
    if (lang == CBM_LANG_FSHARP) {
        TSNode inh =
            find_first_descendant_by_kind(node, "class_inherits_decl", CBM_DESCENDANT_MAX_DEPTH);
        if (!ts_node_is_null(inh)) {
            TSNode st = cbm_find_child_by_kind(inh, "simple_type");
            char *bn = ts_node_is_null(st) ? NULL : cbm_node_text(a, st, source);
            if (bn && bn[0]) {
                const char **result = (const char **)cbm_arena_alloc(a, 2 * sizeof(const char *));
                if (result) {
                    result[0] = bn;
                    result[1] = NULL;
                    return result;
                }
            }
        }
    }
    /* D: `class Dog : Animal, IFoo` — class_declaration lists one `base_class`
     * child per base, each wrapping an identifier/qualified name. */
    if (lang == CBM_LANG_DLANG) {
        const char *pbases[MAX_BASES];
        int pc = 0;
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc && pc < MAX_BASES_MINUS_1; i++) {
            TSNode c = ts_node_child(node, i);
            if (strcmp(ts_node_type(c), "base_class") != 0) {
                continue;
            }
            char *bn = cbm_node_text(a, c, source);
            if (bn && bn[0]) {
                pbases[pc++] = bn;
            }
        }
        if (pc > 0) {
            const char **result =
                (const char **)cbm_arena_alloc(a, (pc + NULL_TERM) * sizeof(const char *));
            if (result) {
                for (int i = 0; i < pc; i++) {
                    result[i] = pbases[i];
                }
                result[pc] = NULL;
                return result;
            }
        }
    }
    /* PowerShell: `class Dog : Animal` — class_statement lists `simple_name`
     * children with a `:` token separating the class name from the base name(s).
     * Collect every simple_name that appears AFTER the first `:` token. */
    if (lang == CBM_LANG_POWERSHELL && strcmp(ts_node_type(node), "class_statement") == 0) {
        const char *pbases[MAX_BASES];
        int pc = 0;
        bool seen_colon = false;
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc && pc < MAX_BASES_MINUS_1; i++) {
            TSNode c = ts_node_child(node, i);
            const char *ck = ts_node_type(c);
            if (strcmp(ck, ":") == 0) {
                seen_colon = true;
                continue;
            }
            if (strcmp(ck, "{") == 0) {
                break; /* class body begins; base list is done */
            }
            if (seen_colon && strcmp(ck, "simple_name") == 0) {
                char *bn = cbm_node_text(a, c, source);
                if (bn && bn[0]) {
                    pbases[pc++] = bn;
                }
            }
        }
        if (pc > 0) {
            const char **result =
                (const char **)cbm_arena_alloc(a, (pc + NULL_TERM) * sizeof(const char *));
            if (result) {
                for (int i = 0; i < pc; i++) {
                    result[i] = pbases[i];
                }
                result[pc] = NULL;
                return result;
            }
        }
    }
    /* Pascal: declClass carries one or more `parent` fields, each a `typeref`
     * (`= class(TBase, IFoo)`). Collect all parent typeref identifiers. */
    if (lang == CBM_LANG_PASCAL && strcmp(ts_node_type(node), "declClass") == 0) {
        const char *pbases[MAX_BASES];
        int pc = 0;
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc && pc < MAX_BASES_MINUS_1; i++) {
            const char *fn = ts_node_field_name_for_child(node, i);
            if (!fn || strcmp(fn, "parent") != 0) {
                continue;
            }
            TSNode pn = ts_node_child(node, i);
            if (!ts_node_is_named(pn)) {
                continue; /* the '(' / ')' delimiters are also tagged `parent` */
            }
            char *bn = cbm_node_text(a, pn, source);
            if (bn && bn[0]) {
                pbases[pc++] = bn;
            }
        }
        if (pc > 0) {
            const char **result =
                (const char **)cbm_arena_alloc(a, (pc + NULL_TERM) * sizeof(const char *));
            if (result) {
                for (int i = 0; i < pc; i++) {
                    result[i] = pbases[i];
                }
                result[pc] = NULL;
                return result;
            }
        }
    }
    static const char *fields[] = {"superclass",
                                   "superclasses",
                                   "superinterfaces",
                                   "interfaces",
                                   "bases",
                                   "type_inheritance_clause",
                                   "delegation_specifiers",
                                   NULL};

    // Collect all bases from all matching fields (fixes early-return bug and keyword-text bug).
    const char *bases[MAX_BASES];
    int base_count = 0;

    for (const char **f = fields; *f; f++) {
        TSNode super = ts_node_child_by_field_name(node, *f, (uint32_t)strlen(*f));
        if (!ts_node_is_null(super)) {
            base_count += collect_bases_from_field(a, super, source, bases + base_count,
                                                   MAX_BASES_MINUS_1 - base_count);
        }
    }

    // Some grammars expose heritage as a named child rather than a field, e.g.
    // Java `interface X extends A, B` → `extends_interfaces` (holds a type_list).
    // Without this the interface's bases were never captured.
    static const char *heritage_children[] = {"extends_interfaces", "super_interfaces", NULL};
    uint32_t top_count = ts_node_child_count(node);
    for (uint32_t i = 0; i < top_count && base_count < MAX_BASES_MINUS_1; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        for (const char **h = heritage_children; *h; h++) {
            if (strcmp(ck, *h) == 0) {
                base_count += collect_bases_from_field(a, child, source, bases + base_count,
                                                       MAX_BASES_MINUS_1 - base_count);
            }
        }
    }

    if (base_count > 0) {
        const char **result =
            (const char **)cbm_arena_alloc(a, (base_count + NULL_TERM) * sizeof(const char *));
        if (result) {
            for (int i = 0; i < base_count; i++) {
                result[i] = bases[i];
            }
            result[base_count] = NULL;
            return result;
        }
    }

    // C/C++: handle base_class_clause
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (strcmp(ts_node_type(child), "base_class_clause") == 0) {
            const char **result = extract_cpp_base_classes(a, child, source);
            if (result) {
                return result;
            }
        }
    }

    // C#: explicit base_list handler
    {
        const char **csharp_result = extract_csharp_base_list(a, node, source, count);
        if (csharp_result) {
            return csharp_result;
        }
    }

    // Fallback: search for common base class node types as children
    static const char *base_types[] = {"superclass",
                                       "superinterfaces",
                                       "type_inheritance_clause",
                                       "class_heritage",
                                       "delegation_specifiers",
                                       "super_interfaces",
                                       "extends_clause",
                                       "implements_clause",
                                       "argument_list",
                                       "inheritance_specifier",
                                       NULL};
    return find_base_from_children(a, node, source, base_types);
}

// Classify class label from AST node kind
static const char *class_label_for_kind(const char *kind) {
    if (strcmp(kind, "interface_declaration") == 0 || strcmp(kind, "interface_type") == 0 ||
        strcmp(kind, "trait_item") == 0 || strcmp(kind, "trait_definition") == 0 ||
        strcmp(kind, "protocol_declaration") == 0) {
        return "Interface";
    }
    if (strcmp(kind, "enum_specifier") == 0 || strcmp(kind, "enum_declaration") == 0 ||
        strcmp(kind, "enum_item") == 0) {
        return "Enum";
    }
    if (strcmp(kind, "type_alias_declaration") == 0 || strcmp(kind, "type_item") == 0 ||
        strcmp(kind, "type_alias") == 0 || strcmp(kind, "type_definition") == 0) {
        return "Type";
    }
    return "Class";
}

// --- Parameter type extraction ---

// Builtin types we skip (not useful as USES_TYPE targets).
static bool is_builtin_type(const char *name) {
    static const char *builtins[] = {
        "int",       "int8",       "int16",     "int32",   "int64",   "uint",      "uint8",
        "uint16",    "uint32",     "uint64",    "float",   "float32", "float64",   "double",
        "string",    "str",        "bool",      "boolean", "byte",    "rune",      "void",
        "None",      "any",        "interface", "object",  "Object",  "error",     "uintptr",
        "complex64", "complex128", "number",    "bigint",  "symbol",  "undefined", "null",
        "char",      "short",      "long",      "i8",      "i16",     "i32",       "i64",
        "u8",        "u16",        "u32",       "u64",     "f32",     "f64",       "usize",
        "isize",     "self",       "Self",      "cls",     "type",    "Int",       "Int8",
        "Int16",     "Int32",      "Int64",     "UInt",    "UInt8",   "UInt16",    "UInt32",
        "UInt64",    "Float",      "Double",    "String",  "Bool",    "Boolean",   "Byte",
        "Short",     "Long",       "Char",      "Unit",    "Void",    "Any",       "Nothing",
        "Dynamic",   NULL};
    for (const char **b = builtins; *b; b++) {
        if (strcmp(name, *b) == 0) {
            return true;
        }
    }
    return false;
}

// Clean a type name: strip *, &, [], ..., generics
static char *clean_type_name(CBMArena *a, const char *raw) {
    if (!raw || !raw[0]) {
        return NULL;
    }
    const char *s = raw;
    // Skip leading whitespace, ":", "*", "&", "[]", "..."
    while (*s == ' ' || *s == '\t' || *s == ':' || *s == '*' || *s == '&' || *s == '[' ||
           *s == ']' || *s == '.') {
        s++;
    }
    if (!*s) {
        return NULL;
    }
    // Find end: stop at <, [, or whitespace
    size_t len = 0;
    while (s[len] && s[len] != '<' && s[len] != '[' && s[len] != ' ') {
        len++;
    }
    if (len == 0) {
        return NULL;
    }
    char *result = cbm_arena_alloc(a, len + NULL_TERM);
    memcpy(result, s, len);
    result[len] = '\0';
    return result;
}

// Extract param_names from a parameter list node.
// Returns NULL-terminated arena-allocated array.
// Extract the parameter name from a single parameter AST node.
static char *resolve_param_name(CBMArena *a, TSNode param, const char *source) {
    const char *pk = ts_node_type(param);

    if (strcmp(pk, "argument") == 0 || strcmp(pk, "tag_parameter") == 0) {
        TSNode name = find_first_descendant_by_kind(param, "objectscript_identifier", 6);
        if (ts_node_is_null(name)) {
            name = find_first_descendant_by_kind(param, "objectscript_identifier_special", 6);
        }
        return ts_node_is_null(name) ? NULL : cbm_node_text(a, name, source);
    }

    if (strcmp(pk, "parameter_declaration") == 0) {
        TSNode nm = ts_node_child_by_field_name(param, TS_FIELD("name"));
        if (!ts_node_is_null(nm)) {
            return cbm_node_text(a, nm, source);
        }
        return NULL;
    }
    if (strcmp(pk, "identifier") == 0) {
        return cbm_node_text(a, param, source);
    }
    if (strcmp(pk, "formal_parameter") == 0 || strcmp(pk, "parameter") == 0 ||
        strcmp(pk, "required_parameter") == 0 || strcmp(pk, "optional_parameter") == 0 ||
        strcmp(pk, "simple_parameter") == 0 || strcmp(pk, "typed_parameter") == 0 ||
        strcmp(pk, "default_parameter") == 0 || strcmp(pk, "typed_default_parameter") == 0) {
        TSNode nm = ts_node_child_by_field_name(param, TS_FIELD("name"));
        if (ts_node_is_null(nm)) {
            nm = ts_node_child_by_field_name(param, TS_FIELD("pattern"));
        }
        if (!ts_node_is_null(nm)) {
            if (strcmp(ts_node_type(nm), "identifier") == 0 ||
                strcmp(ts_node_type(nm), "simple_identifier") == 0) {
                return cbm_node_text(a, nm, source);
            }
        }
    }
    return NULL;
}

static const char **extract_param_names(CBMArena *a, TSNode params, const char *source,
                                        CBMLanguage lang) {
    (void)lang;
    if (ts_node_is_null(params)) {
        return NULL;
    }

    const char *names[MAX_PARAMS];
    int count = 0;

    uint32_t nc = ts_node_child_count(params);
    for (uint32_t i = 0; i < nc && count < MAX_PARAMS_MINUS_1; i++) {
        TSNode param = ts_node_child(params, i);
        if (ts_node_is_null(param) || !ts_node_is_named(param)) {
            continue;
        }

        char *name_text = resolve_param_name(a, param, source);

        if (name_text && name_text[0]) {
            names[count++] = name_text;
        }
    }

    if (count == 0) {
        return NULL;
    }

    const char **result =
        (const char **)cbm_arena_alloc(a, (count + NULL_TERM) * sizeof(const char *));
    for (int i = 0; i < count; i++) {
        result[i] = names[i];
    }
    result[count] = NULL;
    return result;
}

// Extract return_types from a return type node.
// Parses Go-style multi-return (T1, T2) and single return types.
// Returns NULL-terminated arena-allocated array.
// Clean a type text and add to types array if valid.
static void add_cleaned_type(CBMArena *a, const char **types, int *count, char *type_text) {
    if (!type_text || !type_text[0]) {
        return;
    }
    char *cleaned = clean_type_name(a, type_text);
    if (cleaned && cleaned[0]) {
        types[(*count)++] = cleaned;
    }
}

// Extract Go multi-return types from a parameter_list result node.
static void extract_go_multi_return(CBMArena *a, TSNode rt_node, const char *source,
                                    const char **types, int *count) {
    uint32_t nc = ts_node_child_count(rt_node);
    for (uint32_t i = 0; i < nc && *count < MAX_RETURN_TYPES_MINUS_1; i++) {
        TSNode child = ts_node_child(rt_node, i);
        if (ts_node_is_null(child) || !ts_node_is_named(child)) {
            continue;
        }
        if (strcmp(ts_node_type(child), "parameter_declaration") == 0) {
            TSNode tn = ts_node_child_by_field_name(child, TS_FIELD("type"));
            if (!ts_node_is_null(tn)) {
                add_cleaned_type(a, types, count, cbm_node_text(a, tn, source));
            }
        } else {
            add_cleaned_type(a, types, count, cbm_node_text(a, child, source));
        }
    }
}

// Build a NULL-terminated arena-allocated string array from a types buffer.
static const char **build_type_array(CBMArena *a, const char **types, int count) {
    if (count == 0) {
        return NULL;
    }
    const char **result =
        (const char **)cbm_arena_alloc(a, (count + NULL_TERM) * sizeof(const char *));
    for (int i = 0; i < count; i++) {
        result[i] = types[i];
    }
    result[count] = NULL;
    return result;
}

static const char **extract_return_types(CBMArena *a, TSNode rt_node, const char *source,
                                         CBMLanguage lang) {
    (void)lang;
    if (ts_node_is_null(rt_node)) {
        return NULL;
    }

    const char *types[MAX_RETURN_TYPES];
    int count = 0;

    if (strcmp(ts_node_type(rt_node), "parameter_list") == 0) {
        extract_go_multi_return(a, rt_node, source, types, &count);
    } else {
        add_cleaned_type(a, types, &count, cbm_node_text(a, rt_node, source));
    }

    return build_type_array(a, types, count);
}

// Extract param_types from a parameter list node.
// Returns NULL-terminated arena-allocated array.
// Extract type text from a TypeScript type_annotation child.
static char *extract_ts_param_type(CBMArena *a, TSNode param, const char *source) {
    TSNode ta = cbm_find_child_by_kind(param, "type_annotation");
    if (ts_node_is_null(ta)) {
        return NULL;
    }
    uint32_t tanc = ts_node_named_child_count(ta);
    for (uint32_t ti = 0; ti < tanc; ti++) {
        TSNode tch = ts_node_named_child(ta, ti);
        if (ts_node_is_null(tch)) {
            continue;
        }
        const char *tk = ts_node_type(tch);
        if (strcmp(tk, "type_identifier") == 0 || strcmp(tk, "generic_type") == 0 ||
            strcmp(tk, "predefined_type") == 0) {
            return cbm_node_text(a, tch, source);
        }
    }
    return NULL;
}

// Check if a param node type is a generic parameter-like node.
static bool is_generic_param_kind(const char *pk) {
    return strcmp(pk, "formal_parameter") == 0 || strcmp(pk, "parameter") == 0 ||
           strcmp(pk, "parameter_declaration") == 0 || strcmp(pk, "spread_parameter") == 0 ||
           strcmp(pk, "simple_parameter") == 0 || strcmp(pk, "variadic_parameter") == 0 ||
           strcmp(pk, "typed_parameter") == 0;
}

// Resolve param type for JVM/misc languages (Kotlin, Scala, Dart, Groovy, OCaml).
static char *resolve_jvm_param_type(CBMArena *a, TSNode param, const char *pk, const char *source,
                                    CBMLanguage lang) {
    if (strcmp(pk, "parameter") != 0 && strcmp(pk, "formal_parameter") != 0) {
        return NULL;
    }
    if (lang == CBM_LANG_KOTLIN) {
        TSNode tn = ts_node_child_by_field_name(param, TS_FIELD("type"));
        if (!ts_node_is_null(tn)) {
            return cbm_node_text(a, tn, source);
        }
        TSNode ut = cbm_find_child_by_kind(param, "user_type");
        return ts_node_is_null(ut) ? NULL : cbm_node_text(a, ut, source);
    }
    if (lang == CBM_LANG_SCALA || lang == CBM_LANG_DART) {
        TSNode tid = cbm_find_child_by_kind(param, "type_identifier");
        return ts_node_is_null(tid) ? NULL : cbm_node_text(a, tid, source);
    }
    if (lang == CBM_LANG_GROOVY) {
        TSNode tn = ts_node_child_by_field_name(param, TS_FIELD("type"));
        return ts_node_is_null(tn) ? NULL : cbm_node_text(a, tn, source);
    }
    if (lang == CBM_LANG_OCAML) {
        TSNode tp = cbm_find_child_by_kind(param, "typed_pattern");
        if (!ts_node_is_null(tp)) {
            TSNode tn = ts_node_child_by_field_name(tp, TS_FIELD("type"));
            if (!ts_node_is_null(tn)) {
                return cbm_node_text(a, tn, source);
            }
        }
        return NULL;
    }
    return NULL;
}

// Resolve parameter type text for a single param node.
static char *resolve_param_type_text(CBMArena *a, TSNode param, const char *source,
                                     CBMLanguage lang) {
    const char *pk = ts_node_type(param);

    if ((lang == CBM_LANG_OBJECTSCRIPT_UDL || lang == CBM_LANG_OBJECTSCRIPT_ROUTINE) &&
        (strcmp(pk, "argument") == 0 || strcmp(pk, "tag_parameter") == 0)) {
        TSNode return_type = cbm_find_child_by_kind(param, "return_type");
        TSNode typename = ts_node_is_null(return_type)
                              ? (TSNode){0}
                              : cbm_find_child_by_kind(return_type, "typename");
        return ts_node_is_null(typename) ? NULL : cbm_node_text(a, typename, source);
    }

    if (lang == CBM_LANG_TYPESCRIPT || lang == CBM_LANG_TSX || lang == CBM_LANG_ARKTS) {
        if (strcmp(pk, "required_parameter") == 0 || strcmp(pk, "optional_parameter") == 0) {
            return extract_ts_param_type(a, param, source);
        }
        return NULL;
    }

    if (lang == CBM_LANG_KOTLIN || lang == CBM_LANG_SCALA || lang == CBM_LANG_DART ||
        lang == CBM_LANG_GROOVY || lang == CBM_LANG_OCAML) {
        return resolve_jvm_param_type(a, param, pk, source, lang);
    }

    // Generic: parameter-like nodes with "type" field
    if (is_generic_param_kind(pk)) {
        TSNode tn = ts_node_child_by_field_name(param, TS_FIELD("type"));
        if (!ts_node_is_null(tn)) {
            return cbm_node_text(a, tn, source);
        }
    }
    return NULL;
}

// Add a cleaned, deduplicated type to the types array.
static void add_dedup_type(CBMArena *a, const char **types, int *count, char *type_text) {
    if (!type_text || !type_text[0]) {
        return;
    }
    char *cleaned = clean_type_name(a, type_text);
    if (!cleaned || !cleaned[0] || is_builtin_type(cleaned)) {
        return;
    }
    for (int j = 0; j < *count; j++) {
        if (strcmp(types[j], cleaned) == 0) {
            return;
        }
    }
    types[(*count)++] = cleaned;
}

static const char **extract_param_types(CBMArena *a, TSNode params, const char *source,
                                        CBMLanguage lang) {
    if (ts_node_is_null(params)) {
        return NULL;
    }

    const char *types[MAX_PARAMS];
    int count = 0;

    uint32_t nc = ts_node_child_count(params);
    for (uint32_t i = 0; i < nc && count < MAX_PARAMS_MINUS_1; i++) {
        TSNode param = ts_node_child(params, i);
        if (ts_node_is_null(param) || !ts_node_is_named(param)) {
            continue;
        }
        add_dedup_type(a, types, &count, resolve_param_type_text(a, param, source, lang));
    }

    if (count == 0) {
        return NULL;
    }

    const char **result =
        (const char **)cbm_arena_alloc(a, (count + NULL_TERM) * sizeof(const char *));
    for (int i = 0; i < count; i++) {
        result[i] = types[i];
    }
    result[count] = NULL;
    return result;
}

/* Internal, occurrence-preserving signature extraction.  `param_types` above is
 * the legacy USES_TYPE projection: it intentionally drops builtins and
 * duplicate types.  Semantic call resolution needs a different contract — one
 * type entry per source-level call argument, in declaration order. */
static const char signature_unknown_type[] = "?";

static char *signature_receiver_name(CBMArena *a, TSNode param, const char *source,
                                     bool unwrap_wrappers) {
    char *name = resolve_param_name(a, param, source);
    if (name && name[0]) {
        return name;
    }

    TSNode candidate = ts_node_child_by_field_name(param, TS_FIELD("pattern"));
    if (ts_node_is_null(candidate)) {
        candidate = ts_node_child_by_field_name(param, TS_FIELD("name"));
    }
    if (ts_node_is_null(candidate)) {
        uint32_t nc = ts_node_named_child_count(param);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_named_child(param, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "type_annotation") == 0 || strcmp(ck, "decorator") == 0 ||
                strcmp(ck, "attribute_item") == 0) {
                continue;
            }
            candidate = child;
            break;
        }
    }

    /* Python typed/splat wrappers and TS pattern wrappers place the actual
     * receiver identifier at the first named leaf. */
    for (int depth = 0; depth < 4 && !ts_node_is_null(candidate); depth++) {
        if (ts_node_named_child_count(candidate) == 0) {
            return cbm_node_text(a, candidate, source);
        }
        if (!unwrap_wrappers) {
            return NULL;
        }
        TSNode next = ts_node_child_by_field_name(candidate, TS_FIELD("pattern"));
        if (ts_node_is_null(next)) {
            next = ts_node_child_by_field_name(candidate, TS_FIELD("name"));
        }
        if (ts_node_is_null(next)) {
            next = ts_node_named_child(candidate, 0);
        }
        candidate = next;
    }
    return NULL;
}

static bool is_signature_implicit_receiver(CBMArena *a, TSNode param, const char *source,
                                           CBMLanguage lang, bool method, bool first_parameter) {
    const char *pk = ts_node_type(param);

    /* These grammar nodes describe a receiver supplied by member-call syntax,
     * not an argument at the call site. */
    if (strcmp(pk, "self_parameter") == 0 || strcmp(pk, "receiver_parameter") == 0 ||
        strcmp(pk, "explicit_object_parameter_declaration") == 0) {
        return true;
    }

    if (lang == CBM_LANG_TYPESCRIPT || lang == CBM_LANG_TSX || lang == CBM_LANG_ARKTS) {
        char *name = signature_receiver_name(a, param, source, false);
        return name && strcmp(name, "this") == 0;
    }

    /* Python instance/class receivers are conventional identifiers rather than
     * distinct AST nodes.  Only exclude the leading slot on a method path. */
    if (method && first_parameter && lang == CBM_LANG_PYTHON) {
        char *name = signature_receiver_name(a, param, source, true);
        return name && (strcmp(name, "self") == 0 || strcmp(name, "cls") == 0);
    }
    return false;
}

static bool is_signature_non_parameter(TSNode param, const char *source, CBMLanguage lang) {
    const char *pk = ts_node_type(param);
    if (strcmp(pk, "comment") == 0 || strcmp(pk, "line_comment") == 0 ||
        strcmp(pk, "block_comment") == 0 || strcmp(pk, "attribute_item") == 0 ||
        strcmp(pk, "inner_attribute_item") == 0 || strcmp(pk, "attribute_specifier") == 0 ||
        strcmp(pk, "attribute_declaration") == 0 || strcmp(pk, "annotation") == 0 ||
        strcmp(pk, "decorator") == 0 || strcmp(pk, "modifiers") == 0 ||
        strcmp(pk, "positional_separator") == 0 || strcmp(pk, "keyword_separator") == 0 ||
        strcmp(pk, "type_parameters") == 0 || strcmp(pk, "type_parameter_declaration") == 0) {
        return true;
    }

    /* In C-family declarations `(void)` means zero parameters.  The grammar
     * exposes `void` as a named primitive_type child rather than a parameter. */
    if ((lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA ||
         lang == CBM_LANG_GLSL) &&
        strcmp(pk, "primitive_type") == 0) {
        uint32_t start = ts_node_start_byte(param);
        uint32_t end = ts_node_end_byte(param);
        return end - start == 4 && strncmp(source + start, "void", 4) == 0;
    }
    return false;
}

/* Go permits one declaration node to introduce several positional parameters:
 * `func f(a, b T)`.  Count its direct name identifiers; unnamed parameter
 * declarations still represent one position. */
static int signature_param_multiplicity(TSNode param, CBMLanguage lang) {
    if (lang != CBM_LANG_GO) {
        return 1;
    }
    const char *pk = ts_node_type(param);
    if (strcmp(pk, "parameter_declaration") != 0 &&
        strcmp(pk, "variadic_parameter_declaration") != 0) {
        return 1;
    }

    int field_names = 0;
    int identifier_names = 0;
    uint32_t nc = ts_node_child_count(param);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(param, i);
        if (ts_node_is_null(child) || !ts_node_is_named(child)) {
            continue;
        }
        const char *field = ts_node_field_name_for_child(param, i);
        if (field && strcmp(field, "name") == 0) {
            field_names++;
        }
        if (strcmp(ts_node_type(child), "identifier") == 0) {
            identifier_names++;
        }
    }
    int names = field_names > 0 ? field_names : identifier_names;
    return names > 0 ? names : 1;
}

/* Resolve the full source spelling where the legacy resolver has no supported
 * case (defaulted C++ parameters, Python annotations, complex TS types, etc.).
 * This deliberately does not clean or deduplicate the text. */
static char *resolve_signature_param_type_text(CBMArena *a, TSNode param, const char *source,
                                               CBMLanguage lang) {
    TSNode type_node = ts_node_child_by_field_name(param, TS_FIELD("type"));
    if (!ts_node_is_null(type_node)) {
        /* TypeScript-family grammars may expose the `type_annotation` wrapper
         * (including its colon) through the field.  Store the actual type node. */
        if (strcmp(ts_node_type(type_node), "type_annotation") == 0 &&
            ts_node_named_child_count(type_node) > 0) {
            type_node = ts_node_named_child(type_node, 0);
        }
        char *type_text = cbm_node_text(a, type_node, source);
        if (type_text && type_text[0]) {
            return type_text;
        }
    }

    TSNode annotation = cbm_find_child_by_kind(param, "type_annotation");
    if (!ts_node_is_null(annotation)) {
        uint32_t nc = ts_node_named_child_count(annotation);
        if (nc > 0) {
            char *type_text = cbm_node_text(a, ts_node_named_child(annotation, 0), source);
            if (type_text && type_text[0]) {
                return type_text;
            }
        }
    }

    TSNode typed_pattern = cbm_find_child_by_kind(param, "typed_pattern");
    if (!ts_node_is_null(typed_pattern)) {
        type_node = ts_node_child_by_field_name(typed_pattern, TS_FIELD("type"));
        if (!ts_node_is_null(type_node)) {
            char *type_text = cbm_node_text(a, type_node, source);
            if (type_text && type_text[0]) {
                return type_text;
            }
        }
    }
    return resolve_param_type_text(a, param, source, lang);
}

static const char **extract_signature_param_types(CBMArena *a, TSNode params, const char *source,
                                                  CBMLanguage lang, bool method, int *out_count) {
    *out_count = 0;
    if (ts_node_is_null(params)) {
        return NULL;
    }

    const char *types[MAX_PARAMS];
    uint32_t nc = ts_node_child_count(params);
    for (uint32_t i = 0; i < nc && *out_count < MAX_PARAMS_MINUS_1; i++) {
        TSNode param = ts_node_child(params, i);
        if (ts_node_is_null(param) || !ts_node_is_named(param) ||
            is_signature_non_parameter(param, source, lang)) {
            continue;
        }
        if (is_signature_implicit_receiver(a, param, source, lang, method, *out_count == 0)) {
            continue;
        }

        char *type_text = resolve_signature_param_type_text(a, param, source, lang);
        const char *entry = type_text && type_text[0] ? type_text : signature_unknown_type;
        int multiplicity = signature_param_multiplicity(param, lang);
        for (int n = 0; n < multiplicity && *out_count < MAX_PARAMS_MINUS_1; n++) {
            types[(*out_count)++] = entry;
        }
    }

    if (*out_count == 0) {
        return NULL;
    }
    const char **result =
        (const char **)cbm_arena_alloc(a, (size_t)(*out_count + NULL_TERM) * sizeof(const char *));
    if (!result) {
        *out_count = 0;
        return NULL;
    }
    for (int i = 0; i < *out_count; i++) {
        result[i] = types[i];
    }
    result[*out_count] = NULL;
    return result;
}

// --- Function definition extraction ---

// For C++/CUDA template_declaration, find the inner function_definition or declaration.
static TSNode unwrap_template_inner(TSNode node, CBMLanguage lang) {
    TSNode inner = find_cpp_template_inner_node(node, lang);
    if (!ts_node_is_null(inner)) {
        return inner;
    }
    return node;
}

// C/C++/CUDA/GLSL: parameters live on function_declarator inside declarator chain.
static TSNode find_c_params(TSNode func_node) {
    TSNode decl = ts_node_child_by_field_name(func_node, TS_FIELD("declarator"));
    for (int d = 0; d < DECLARATOR_DEPTH_LIMIT && !ts_node_is_null(decl); d++) {
        TSNode params = ts_node_child_by_field_name(decl, TS_FIELD("parameters"));
        if (!ts_node_is_null(params)) {
            return params;
        }
        TSNode nested = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
        /* `API RetT *name(...)` misread as `RetT::*name(...)` (MISSING "::"):
         * the function declarator is on the qualifier's name side. */
        if (ts_node_is_null(nested) && cbm_c_qualifier_is_recovered(decl)) {
            nested = ts_node_child_by_field_name(decl, TS_FIELD("name"));
        }
        /* tree-sitter-cpp and tree-sitter-cuda do not assign the nested
         * function declarator a `declarator` field when a reference return
         * wraps it (`Item& operator[](int)`).  That wrapper has one named child;
         * descend only for this reproduced grammar shape. */
        if (ts_node_is_null(nested) && strcmp(ts_node_type(decl), "reference_declarator") == 0 &&
            ts_node_named_child_count(decl) > 0) {
            nested = ts_node_named_child(decl, 0);
        }
        decl = nested;
    }
    TSNode null_node = {0};
    return null_node;
}

/* Resolve the parameter-list node across the grammars used by the generic
 * definition extractor. Newer tree-sitter-kotlin exposes
 * `function_value_parameters` as a direct child rather than a `parameters`
 * field. Keep the legacy and ordered signature extractors on the same node so
 * adding positional metadata does not replace or reinterpret `param_types`. */
static TSNode find_function_params(TSNode func_node, CBMLanguage lang) {
    TSNode params = ts_node_child_by_field_name(func_node, TS_FIELD("parameters"));
    // ObjectScript exposes the parameter list under a `parameter_list` field.
    if (ts_node_is_null(params) &&
        (lang == CBM_LANG_OBJECTSCRIPT_UDL || lang == CBM_LANG_OBJECTSCRIPT_ROUTINE)) {
        params = ts_node_child_by_field_name(func_node, TS_FIELD("parameter_list"));
    }
    if (ts_node_is_null(params) && lang == CBM_LANG_OBJECTSCRIPT_ROUTINE) {
        params = cbm_find_child_by_kind(func_node, "parameter_list");
    }
    if (ts_node_is_null(params) && lang == CBM_LANG_OBJECTSCRIPT_UDL) {
        TSNode method_definition = cbm_find_child_by_kind(func_node, "method_definition");
        if (!ts_node_is_null(method_definition)) {
            params = cbm_find_child_by_kind(method_definition, "arguments");
        }
    }
    if (ts_node_is_null(params) && lang == CBM_LANG_KOTLIN) {
        params = cbm_find_child_by_kind(func_node, "function_value_parameters");
    }
    if (ts_node_is_null(params) && (lang == CBM_LANG_C || lang == CBM_LANG_CPP ||
                                    lang == CBM_LANG_CUDA || lang == CBM_LANG_GLSL)) {
        params = find_c_params(func_node);
    }
    return params;
}

/* ── C-family declared return type ──────────────────────────────────
 * The C-family grammars split a declared return type three ways: the `type`
 * field (`char`), sibling type_qualifier nodes (`const`), and the
 * pointer/reference declarators wrapping the function declarator (`*`). Taking
 * only the `type` field published `const char *get_name(void)` as "char".
 *
 * Canonical spelling: leading cv-qualifiers in source order, the base type text
 * verbatim, then one space and the declarator markers outermost-first with no
 * space between them — `const char *`, `char **`, `Text &`, `Text *&`. A
 * qualifier on a pointer level follows its `*` and is separated from the next
 * marker by a space: `char *const *`. A qualifier written after the base type
 * (`char const *`) is normalized to the leading position. */

/* Output sink: measures when buf is NULL, writes otherwise. Rendering twice
 * sizes the arena allocation exactly without a second copy of the logic. */
typedef struct {
    char *buf;
    size_t len;
} c_rt_out_t;

static void c_rt_put(c_rt_out_t *out, const char *text, size_t n) {
    if (out->buf) {
        memcpy(out->buf + out->len, text, n);
    }
    out->len += n;
}

static void c_rt_put_str(c_rt_out_t *out, const char *text) {
    c_rt_put(out, text, strlen(text));
}

static void c_rt_put_node(c_rt_out_t *out, TSNode node, const char *source) {
    uint32_t start = ts_node_start_byte(node);
    uint32_t end = ts_node_end_byte(node);
    if (end > start) {
        c_rt_put(out, source + start, (size_t)(end - start));
    }
}

/* type_qualifier also covers keywords that are not part of the type
 * (`constexpr`, `_Noreturn`, `mutable`, `__extension__`, …). Keep only the ones
 * that are, so `constexpr int f()` still returns "int". */
static bool is_c_return_cv_qualifier(TSNode node, const char *source) {
    static const char *const kept[] = {"const",        "volatile", "restrict", "__restrict",
                                       "__restrict__", "_Atomic",  NULL};
    if (strcmp(ts_node_type(node), "type_qualifier") != 0) {
        return false;
    }
    uint32_t start = ts_node_start_byte(node);
    uint32_t end = ts_node_end_byte(node);
    size_t len = end > start ? (size_t)(end - start) : 0;
    for (const char *const *k = kept; *k; k++) {
        if (strlen(*k) == len && memcmp(source + start, *k, len) == 0) {
            return true;
        }
    }
    return false;
}

static bool is_c_declarator_lang(CBMLanguage lang) {
    return lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA ||
           lang == CBM_LANG_GLSL || lang == CBM_LANG_HLSL || lang == CBM_LANG_ISPC ||
           lang == CBM_LANG_SLANG || lang == CBM_LANG_OBJC;
}

/* C-family `struct X` / `union X` / `enum X` / `class X` WITHOUT a body is a
 * reference to the type — a forward declaration, a variable, parameter or field
 * type, a sizeof operand, the aliased side of `typedef struct X Y;` — never its
 * definition. Minting a def for it put a one-line Class/Enum node on the type's
 * QN: when the definition shares that QN (same file, or a same-stem .h/.c pair,
 * whose module QNs coincide), the later or smaller-path reference displaced the
 * definition, and elsewhere it left a phantom type node in every file that
 * merely mentions the type (`struct timeval` in redis-cli.c). */
static bool is_c_tag_reference(CBMLanguage lang, TSNode node, const char *kind) {
    if (!is_c_declarator_lang(lang)) {
        return false;
    }
    if (strcmp(kind, "struct_specifier") != 0 && strcmp(kind, "union_specifier") != 0 &&
        strcmp(kind, "enum_specifier") != 0 && strcmp(kind, "class_specifier") != 0) {
        return false;
    }
    return ts_node_is_null(ts_node_child_by_field_name(node, TS_FIELD("body")));
}

/* Languages whose plain `enum` is UNSCOPED: its enumerators are names of the
 * scope that holds the enum, not of the enum (C, C++ and its CUDA dialect,
 * Objective-C). `XXH_OK` of `typedef enum { XXH_OK, XXH_ERROR } XXH_errorcode;`
 * is written and looked up as plain XXH_OK, so its QN is `<scope>.XXH_OK`. */
static bool c_enum_lang(CBMLanguage lang) {
    return lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA ||
           lang == CBM_LANG_OBJC;
}

/* Is a struct, union or class around an unscoped enum the scope of its
 * enumerators? In C++ (and CUDA; every .h is parsed as C++) it is: the
 * enumerator is `Brush::ROUND`. C and Objective-C have no struct scope for
 * ordinary identifiers: an enum declared inside a struct puts its enumerators
 * in the scope around the struct and the code names them unqualified, so the
 * struct is no segment of their QN. */
static bool c_enum_scope_is_class(CBMLanguage lang) {
    return lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA;
}

/* C++11 `enum class X` / `enum struct X`: the scoping keyword is an anonymous
 * token child between `enum` and the body. Its enumerators stay `X::A`. */
static bool c_enum_is_scoped(TSNode enum_node) {
    TSNode body = ts_node_child_by_field_name(enum_node, TS_FIELD("body"));
    uint32_t stop = ts_node_is_null(body) ? UINT32_MAX : ts_node_start_byte(body);
    uint32_t nc = ts_node_child_count(enum_node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(enum_node, i);
        if (ts_node_start_byte(c) >= stop) {
            break;
        }
        if (!ts_node_is_named(c) &&
            (strcmp(ts_node_type(c), "class") == 0 || strcmp(ts_node_type(c), "struct") == 0)) {
            return true;
        }
    }
    return false;
}

/* Render the canonical return type into `out`; returns how many qualifiers and
 * markers were added around the base type (0 = the base text alone is already
 * the whole type). The declarator walk is one strict child chain, so it is
 * O(depth) with no recursion and needs no depth cap. It stops at the first node
 * that is neither a pointer nor a reference declarator: for a function returning
 * a function pointer (`int (*f(void))(int)`) that is the outer
 * function_declarator, which leaves the base type as it was. That node is
 * handed back through `rest` when the caller asks for it. */
static size_t c_rt_render(c_rt_out_t *out, TSNode func_node, TSNode type_node, TSNode declarator,
                          const char *source, TSNode *rest) {
    size_t added = 0;
    uint32_t decl_start = ts_node_start_byte(declarator);
    uint32_t nc = ts_node_named_child_count(func_node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode ch = ts_node_named_child(func_node, i);
        if (ts_node_start_byte(ch) >= decl_start) {
            break;
        }
        if (is_c_return_cv_qualifier(ch, source)) {
            c_rt_put_node(out, ch, source);
            c_rt_put_str(out, " ");
            added++;
        }
    }
    c_rt_put_node(out, type_node, source);

    bool need_space = true;
    TSNode decl = declarator;
    while (!ts_node_is_null(decl)) {
        const char *dk = ts_node_type(decl);
        bool is_ref = strcmp(dk, "reference_declarator") == 0;
        if (!is_ref && strcmp(dk, "pointer_declarator") != 0) {
            break;
        }
        if (need_space) {
            c_rt_put_str(out, " ");
            need_space = false;
        }
        /* A reference_declarator opens with its `&` / `&&` token. */
        TSNode marker = ts_node_child(decl, 0);
        if (is_ref && !ts_node_is_null(marker) && !ts_node_is_named(marker)) {
            c_rt_put_node(out, marker, source);
        } else {
            c_rt_put_str(out, is_ref ? "&" : "*");
        }
        added++;
        uint32_t dn = ts_node_named_child_count(decl);
        for (uint32_t i = 0; i < dn; i++) {
            TSNode q = ts_node_named_child(decl, i);
            if (is_c_return_cv_qualifier(q, source)) {
                c_rt_put_node(out, q, source);
                need_space = true;
                added++;
            }
        }
        /* tree-sitter-cpp/-cuda give a reference_declarator's inner declarator no
         * `declarator` field (see find_c_params); it is the one named child. */
        TSNode inner = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
        if (ts_node_is_null(inner) && is_ref && dn > 0) {
            inner = ts_node_named_child(decl, 0);
        }
        decl = inner;
    }
    if (rest) {
        *rest = decl; /* the declarator the markers wrap; null when none is left */
    }
    return added;
}

/* Declared return type of a C-family function/method node whose `type` field is
 * `type_node`. Any other language, and any type with nothing around its base
 * type, gets the base type text exactly as before. */
static char *c_declared_return_type(CBMExtractCtx *ctx, TSNode func_node, TSNode type_node) {
    CBMArena *a = ctx->arena;
    TSNode declarator = ts_node_child_by_field_name(func_node, TS_FIELD("declarator"));
    if (!is_c_declarator_lang(ctx->language) || ts_node_is_null(declarator)) {
        return cbm_node_text(a, type_node, ctx->source);
    }
    c_rt_out_t out = {NULL, 0};
    if (c_rt_render(&out, func_node, type_node, declarator, ctx->source, NULL) == 0) {
        return cbm_node_text(a, type_node, ctx->source);
    }
    out.buf = (char *)cbm_arena_alloc(a, out.len + NULL_TERM);
    if (!out.buf) {
        return cbm_node_text(a, type_node, ctx->source);
    }
    out.len = 0;
    (void)c_rt_render(&out, func_node, type_node, declarator, ctx->source, NULL);
    out.buf[out.len] = '\0';
    return out.buf;
}

// C++: resolve trailing return type (auto f() -> Type) on a declarator node.
// Updates def->return_type and def->return_types if trailing type found.
static void resolve_cpp_trailing_return(CBMArena *a, TSNode func_node, const char *source,
                                        CBMDefinition *def) {
    TSNode declarator = ts_node_child_by_field_name(func_node, TS_FIELD("declarator"));
    if (ts_node_is_null(declarator)) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(declarator);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode ch = ts_node_named_child(declarator, i);
        if (strcmp(ts_node_type(ch), "trailing_return_type") == 0) {
            TSNode type_desc = ts_node_named_child_count(ch) > 0 ? ts_node_named_child(ch, 0) : ch;
            def->return_type = cbm_node_text(a, type_desc, source);
            if (def->return_type && def->return_type[0]) {
                const char **rt =
                    (const char **)cbm_arena_alloc(a, RT_PAIR_SIZE * sizeof(const char *));
                if (rt) {
                    rt[0] = def->return_type;
                    rt[SKIP_CHAR] = NULL;
                    def->return_types = rt;
                }
            }
            break;
        }
    }
}

/* Compute and store the structural complexity metrics for a definition. */
static void set_def_complexity(CBMDefinition *def, TSNode body, const CBMLangSpec *spec) {
    cbm_complexity_t cx;
    cbm_compute_complexity(body, spec->branching_node_types, &cx);
    def->complexity = cx.cyclomatic;
    def->cognitive = cx.cognitive;
    def->loop_count = cx.loop_count;
    def->loop_depth = cx.loop_depth;
    def->max_access_depth = cx.max_access_depth;
}

/* Extract the bare type name from a Go method receiver node.
 * The receiver is a parameter_list, e.g. "(s *OrderService)" or "(s Order)".
 * Walks to the parameter_declaration's `type` field, unwrapping pointer_type
 * and generic_type, and returns the type_identifier text (e.g. "OrderService").
 * Returns NULL if no type_identifier is found. */
static char *go_receiver_type_name(CBMArena *a, TSNode recv, const char *source) {
    uint32_t nc = ts_node_child_count(recv);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(recv, i);
        if (strcmp(ts_node_type(child), "parameter_declaration") != 0) {
            continue;
        }
        TSNode tn = ts_node_child_by_field_name(child, TS_FIELD("type"));
        if (ts_node_is_null(tn)) {
            continue;
        }
        /* Unwrap pointer_type / generic_type down to the type_identifier. */
        for (int guard = 0; guard < 4 && !ts_node_is_null(tn); guard++) {
            const char *tk = ts_node_type(tn);
            if (strcmp(tk, "type_identifier") == 0) {
                return cbm_node_text(a, tn, source);
            }
            if (strcmp(tk, "pointer_type") == 0 || strcmp(tk, "generic_type") == 0) {
                /* pointer_type: child is the pointee type; generic_type has a
                 * `type` field for the base type_identifier. */
                TSNode inner = ts_node_child_by_field_name(tn, TS_FIELD("type"));
                if (ts_node_is_null(inner)) {
                    inner = ts_node_named_child(tn, 0);
                }
                tn = inner;
                continue;
            }
            break;
        }
    }
    return NULL;
}

/* C++/CUDA: true when name is a GoogleTest test-definition macro whose
 * invocations parse as function definitions with bare-identifier parameters.
 * Multiple such macros in one file all share the extracted name (e.g. "TEST"),
 * causing qualified-name collisions and node loss (#1266). */
static bool is_cpp_test_macro(const char *name) {
    return strcmp(name, "TEST") == 0 || strcmp(name, "TEST_F") == 0 ||
           strcmp(name, "TEST_P") == 0 || strcmp(name, "TYPED_TEST") == 0 ||
           strcmp(name, "TYPED_TEST_P") == 0;
}

/* Compose a unique name from a GoogleTest-style macro invocation by appending
 * the macro arguments: TEST(Suite, Case) -> "TEST_Suite_Case".
 * Returns an arena-allocated string, or NULL when the parameters cannot be
 * resolved (caller keeps the original name in that case). */
static char *resolve_cpp_test_macro_name(CBMArena *a, const char *macro, TSNode node,
                                         const char *source) {
    TSNode params = ts_node_child_by_field_name(node, TS_FIELD("parameters"));
    if (ts_node_is_null(params)) {
        params = find_c_params(node);
    }
    if (ts_node_is_null(params)) {
        return NULL;
    }

    const char *args[2] = {NULL, NULL};
    int count = 0;
    uint32_t nc = ts_node_named_child_count(params);
    for (uint32_t i = 0; i < nc && count < 2; i++) {
        TSNode child = ts_node_named_child(params, i);
        if (ts_node_is_null(child)) {
            continue;
        }
        TSNode type_node = ts_node_child_by_field_name(child, TS_FIELD("type"));
        char *text = cbm_node_text(a, ts_node_is_null(type_node) ? child : type_node, source);
        if (text && text[0]) {
            args[count++] = text;
        }
    }

    if (count == 2) {
        return cbm_arena_sprintf(a, "%s_%s_%s", macro, args[0], args[1]);
    }
    if (count == 1) {
        return cbm_arena_sprintf(a, "%s_%s", macro, args[0]);
    }
    return NULL;
}

/* Configured macro definitions are interpreted only in their raw source.
 * The map lives in traversal scratch; its QNs live in the result arena. */
struct CBMTestDefinitionMatch {
    uint32_t name_byte;
    TSNode name_node;  /* same raw tree; never retained beyond extraction */
    TSNode scope_node; /* function, or split invocation's compound body */
    uint32_t line;
    int declaration;
    const char *qn;
};

typedef struct {
    CBMTestDefinitionRole role;
    int argument;
    int declaration;
    bool valid;
} TDXRule;

static int tdx_language(const char *name) {
    if (!name)
        return 0;
    if (strcmp(name, "c") == 0 || strcmp(name, "C") == 0)
        return 1;
    if (strcmp(name, "cpp") == 0 || strcmp(name, "c++") == 0 || strcmp(name, "C++") == 0)
        return 2;
    if (strcmp(name, "cuda") == 0 || strcmp(name, "CUDA") == 0)
        return 3;
    return 0;
}

static int tdx_source_language(CBMLanguage language) {
    return language == CBM_LANG_C      ? 1
           : language == CBM_LANG_CPP  ? 2
           : language == CBM_LANG_CUDA ? 3
                                       : 0;
}

const char *cbm_test_extract_status_message(CBMTestExtractStatus status) {
    static const char *const messages[] = {"",
                                           "unsupported configured definition language",
                                           "unsupported disabled native test preset",
                                           "unsupported configured definition name",
                                           "configured definition argument is missing",
                                           "configured definition argument is not an identifier",
                                           "ambiguous configured definition mapping",
                                           "unsupported configured definition form",
                                           "configured definition allocation failed"};
    size_t count = sizeof(messages) / sizeof(messages[0]);
    return (size_t)status < count ? messages[status] : "configured definition issue";
}

static bool tdx_issue(CBMFileResult *result, CBMTestExtractStatus status, int declaration,
                      uint32_t line) {
    if (result->test_declarations_status == CBM_TEST_EXTRACT_OOM)
        return false;
    bool earlier =
        result->test_declarations_status == CBM_TEST_EXTRACT_OK || status == CBM_TEST_EXTRACT_OOM ||
        line < result->test_declaration_line ||
        (line == result->test_declaration_line && (declaration < result->test_declaration_index ||
                                                   (declaration == result->test_declaration_index &&
                                                    status < result->test_declarations_status)));
    if (!earlier)
        return false;
    if (!result->has_error)
        result->test_error_only = true;

    result->has_error = true;
    result->test_declarations_status = status;
    result->test_declaration_index = declaration;
    result->test_declaration_line = line;
    result->error_msg = cbm_arena_strdup(&result->arena, cbm_test_extract_status_message(status));
    if (!result->error_msg)
        result->test_declarations_status = CBM_TEST_EXTRACT_OOM;
    return false;
}

void cbm_test_declarations_degrade(CBMFileResult *result) {
    if (!result || result->test_declarations_status == CBM_TEST_EXTRACT_OK ||
        result->test_declarations_status == CBM_TEST_EXTRACT_OOM)
        return;
    result->test_declarations_degraded = true;
    result->test_declarations_degraded_status = result->test_declarations_status;
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *def = &result->defs.items[i];
        def->test_role = CBM_TEST_ROLE_NONE;
        def->test_name_start_byte = def->test_name_end_byte = 0;
        def->test_body_start_byte = def->test_body_end_byte = 0;
    }
    result->has_test_definition_owners = false;
    result->test_declarations_status = CBM_TEST_EXTRACT_OK;
    if (result->test_error_only) {
        result->has_error = false;
        result->error_msg = NULL;
        result->test_error_only = false;
    }
}

bool cbm_test_declarations_validate(CBMFileResult *result,
                                    const cbm_test_declarations_t *declarations) {
    if (!declarations)
        return true;
    for (int i = 0; i < CBM_TEST_PRESET_COUNT; i++) {
        bool enabled = false;
        if (!cbm_test_declarations_preset(declarations, (cbm_test_preset_t)i, &enabled))
            return tdx_issue(result, CBM_TEST_EXTRACT_UNSUPPORTED_PRESET, -1, 0);
        if (!enabled && i != CBM_TEST_PRESET_C_CBM && i != CBM_TEST_PRESET_GTEST)
            tdx_issue(result, CBM_TEST_EXTRACT_UNSUPPORTED_PRESET, -1, 0);
    }
    int count = 0;
    const cbm_test_declaration_t *items = cbm_test_declarations_items(declarations, &count);
    for (int i = 0; i < count; i++) {
        const cbm_test_declaration_t *rule = &items[i];
        if (rule->role != CBM_TEST_DECL_CASE && rule->role != CBM_TEST_DECL_SUITE)
            continue;
        if (!tdx_language(rule->language))
            tdx_issue(result, CBM_TEST_EXTRACT_UNSUPPORTED_LANGUAGE, i, 0);
        if (rule->name_arg_count != 1 || !rule->name_args || rule->name_args[0] < 0)
            tdx_issue(result, CBM_TEST_EXTRACT_UNSUPPORTED_NAME, i, 0);
    }
    return result->test_declarations_status == CBM_TEST_EXTRACT_OK;
}

static bool tdx_preset(const CBMExtractCtx *ctx, cbm_test_preset_t preset) {
    if (!ctx->test_declarations)
        return preset != CBM_TEST_PRESET_C_CBM;
    bool enabled = false;
    return cbm_test_declarations_preset(ctx->test_declarations, preset, &enabled) && enabled;
}

static bool tdx_equal(const char *name, const char *text, size_t size) {
    return name && strlen(name) == size && memcmp(name, text, size) == 0;
}

static bool tdx_match(CBMExtractCtx *ctx, const char *text, size_t size, uint32_t line,
                      TDXRule *chosen) {
    *chosen = (TDXRule){.argument = -1, .declaration = -1};
    if (!ctx->test_declarations || !ctx->test_declarations_raw_source)
        return false;
    int language = tdx_source_language(ctx->language), count = 0;
    if (!language)
        return false;
    const cbm_test_declaration_t *items =
        cbm_test_declarations_items(ctx->test_declarations, &count);
    bool matched = false, valid = true;
    for (int i = 0; i < count; i++) {
        const cbm_test_declaration_t *rule = &items[i];
        if ((rule->role != CBM_TEST_DECL_CASE && rule->role != CBM_TEST_DECL_SUITE) ||
            tdx_language(rule->language) != language || !tdx_equal(rule->define_macro, text, size))
            continue;
        CBMTestDefinitionRole role =
            rule->role == CBM_TEST_DECL_CASE ? CBM_TEST_ROLE_CASE : CBM_TEST_ROLE_SUITE;
        if (rule->name_arg_count != 1 || !rule->name_args || rule->name_args[0] < 0) {
            tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_NAME, i, line);
            valid = false;
            matched = true;
            continue;
        }
        if (matched && (chosen->role != role || chosen->argument != rule->name_args[0])) {
            tdx_issue(ctx->result, CBM_TEST_EXTRACT_AMBIGUOUS, i, line);
            valid = false;
        }
        if (!matched)
            *chosen = (TDXRule){role, rule->name_args[0], i, true};
        matched = true;
    }
    if (language == 1 && tdx_preset(ctx, CBM_TEST_PRESET_C_CBM)) {
        CBMTestDefinitionRole role = tdx_equal("TEST", text, size)    ? CBM_TEST_ROLE_CASE
                                     : tdx_equal("SUITE", text, size) ? CBM_TEST_ROLE_SUITE
                                                                      : CBM_TEST_ROLE_NONE;
        if (role != CBM_TEST_ROLE_NONE) {
            if (matched && (chosen->role != role || chosen->argument != 0)) {
                tdx_issue(ctx->result, CBM_TEST_EXTRACT_AMBIGUOUS, -1, line);
                valid = false;
            }
            if (!matched)
                *chosen = (TDXRule){role, 0, -1, true};
            matched = true;
        }
    }
    /* The enabled native GTest detector consumes its own argument tuple.
     * A custom single-name mapping must explicitly disable that overlap. */
    if (matched && language != 1 && tdx_preset(ctx, CBM_TEST_PRESET_GTEST) &&
        (tdx_equal("TEST", text, size) || tdx_equal("TEST_F", text, size) ||
         tdx_equal("TEST_P", text, size) || tdx_equal("TYPED_TEST", text, size) ||
         tdx_equal("TYPED_TEST_P", text, size))) {
        tdx_issue(ctx->result, CBM_TEST_EXTRACT_AMBIGUOUS, -1, line);
        valid = false;
    }
    chosen->valid = valid;
    return matched;
}

static bool tdx_ident_first(unsigned char c) {
    return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

static bool tdx_ident(unsigned char c) {
    return tdx_ident_first(c) || (c >= '0' && c <= '9');
}

/* C/C++ preprocessing-number maximal munch; no numeric evaluation. This
 * protects digit separators and user-defined suffixes from quote/identifier
 * scanning, while a selected numeric argument still fails the identifier rule. */
static bool tdx_pp_number(const char *s, size_t n, size_t *at) {
    size_t i = *at;
    if (i == n || !((s[i] >= '0' && s[i] <= '9') ||
                    (s[i] == '.' && i + 1 < n && s[i + 1] >= '0' && s[i + 1] <= '9')))
        return false;
    i++;
    while (i < n) {
        unsigned char c = (unsigned char)s[i];
        if (tdx_ident(c) || c == '.') {
            i++;
            continue;
        }
        if (c == '\'' && i + 1 < n && tdx_ident((unsigned char)s[i + 1])) {
            i += 2;
            continue;
        }
        if ((c == '+' || c == '-') &&
            (s[i - 1] == 'e' || s[i - 1] == 'E' || s[i - 1] == 'p' || s[i - 1] == 'P')) {
            i++;
            continue;
        }
        break;
    }
    *at = i;
    return true;
}

static bool tdx_space(unsigned char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v';
}

static bool tdx_splice(const char *s, size_t n, size_t at) {
    return at < n && s[at] == '\\' && at + 1 < n &&
           (s[at + 1] == '\n' || (s[at + 1] == '\r' && at + 2 < n && s[at + 2] == '\n'));
}

/* Trivia and literals are read as bytes, never as C expressions. Unsupported
 * line splicing/raw strings are explicit uncertainty, not token guesses. */
static bool tdx_trivia(const char *s, size_t n, size_t *at) {
    for (;;) {
        while (*at < n && tdx_space((unsigned char)s[*at]))
            (*at)++;
        if (*at + 1 >= n || s[*at] != '/')
            return true;
        if (s[*at + 1] == '/') {
            *at += 2;
            while (*at < n && s[*at] != '\n') {
                if (tdx_splice(s, n, *at))
                    return false;
                (*at)++;
            }
        } else if (s[*at + 1] == '*') {
            *at += 2;
            while (*at + 1 < n && !(s[*at] == '*' && s[*at + 1] == '/')) {
                if (tdx_splice(s, n, *at))
                    return false;
                (*at)++;
            }
            if (*at + 1 >= n)
                return false;
            *at += 2;
        } else
            return true;
    }
}

static bool tdx_literal(const char *s, size_t n, size_t *at) {
    char quote = s[*at];
    if (quote == '"' && *at && s[*at - 1] == 'R')
        return false;
    (*at)++;
    while (*at < n) {
        char c = s[*at];
        if (c == quote) {
            (*at)++;
            return true;
        }
        if (c == '\0' || c == '\n' || c == '\r' || tdx_splice(s, n, *at))
            return false;
        if (c == '\\') {
            if (*at + 1 >= n)
                return false;
            *at += 2;
        } else
            (*at)++;
    }
    return false;
}

/* The audit skips complete raw strings outside consumed invocations. The
 * invocation adapter remains deliberately stricter (tdx_literal above). */
static bool tdx_audit_literal(const char *s, size_t n, size_t *at) {
    if (s[*at] != '"' || !*at || s[*at - 1] != 'R')
        return tdx_literal(s, n, at);
    size_t delimiter = *at + 1, open = delimiter;
    while (open < n && s[open] != '(') {
        unsigned char c = (unsigned char)s[open];
        if (open - delimiter == 16 || c <= ' ' || c == ')' || c == '\\' || c == 127)
            return false;
        open++;
    }
    if (open == n)
        return false;
    size_t length = open - delimiter;
    for (size_t end = open + 1; end < n; end++) {
        if (s[end] == ')' && length + 2 <= n - end &&
            memcmp(s + end + 1, s + delimiter, length) == 0 && s[end + length + 1] == '"') {
            *at = end + length + 2;
            return true;
        }
    }
    return false;
}

static bool tdx_argument(CBMExtractCtx *ctx, size_t at, int wanted, size_t *name_at,
                         size_t *name_length, size_t *after, const TDXRule *rule, uint32_t line) {
    const char *s = ctx->source;
    size_t n = (size_t)ctx->source_len;
    if (!tdx_trivia(s, n, &at) || at >= n || s[at] != '(')
        return tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, rule->declaration, line);
    char closing[64] = {')'};
    size_t depth = 1, begin = ++at, selected_begin = 0, selected_end = 0;
    int argument = 0;
    bool selected = false, closed = false;
    while (at < n && depth) {
        if (!tdx_trivia(s, n, &at) || at >= n || s[at] == '\0' || tdx_splice(s, n, at))
            break;
        if (tdx_pp_number(s, n, &at))
            continue;
        char c = s[at];
        if (c == '"' || c == '\'') {
            if (!tdx_literal(s, n, &at))
                break;
            continue;
        }
        if (c == '(' || c == '[' || c == '{') {
            if (depth == sizeof(closing))
                break;
            closing[depth++] = c == '(' ? ')' : c == '[' ? ']' : '}';
        } else if (c == ')' || c == ']' || c == '}') {
            if (c != closing[depth - 1])
                break;
            if (--depth == 0) {
                if (argument == wanted) {
                    selected_begin = begin;
                    selected_end = at;
                    selected = true;
                }
                *after = at + 1;
                closed = true;
                break;
            }
        } else if (c == ',' && depth == 1) {
            if (argument == wanted) {
                selected_begin = begin;
                selected_end = at;
                selected = true;
            }
            argument++;
            begin = at + 1;
        }
        at++;
    }
    if (!closed)
        return tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, rule->declaration, line);
    if (!selected)
        return tdx_issue(ctx->result, CBM_TEST_EXTRACT_MISSING_ARGUMENT, rule->declaration, line);
    at = selected_begin;
    if (!tdx_trivia(s, selected_end, &at) || at == selected_end)
        return tdx_issue(ctx->result, CBM_TEST_EXTRACT_MISSING_ARGUMENT, rule->declaration, line);
    if (!tdx_ident_first((unsigned char)s[at]))
        return tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_ARGUMENT, rule->declaration,
                         line);
    *name_at = at++;
    while (at < selected_end && tdx_ident((unsigned char)s[at]))
        at++;
    *name_length = at - *name_at;
    if (!tdx_trivia(s, selected_end, &at) || at != selected_end)
        return tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_ARGUMENT, rule->declaration,
                         line);
    return true;
}

/* The C grammar represents an unexpanded macro body in two additional
 * ways: TYPE(identifier) { ... }, or a call statement with a missing semicolon
 * followed by a sibling compound statement. Keep these candidates local to
 * configured extraction; they never change the language's function kinds. */
bool cbm_test_definition_candidate(TSNode scope, CBMLanguage language, TSNode *name, TSNode *body) {
    *name = (TSNode){0};
    *body = (TSNode){0};
    if (ts_node_is_null(scope))
        return false;
    const char *kind = ts_node_type(scope);
    if (language == CBM_LANG_C && strcmp(kind, "compound_statement") == 0) {
        TSNode previous = ts_node_prev_named_sibling(scope);
        while (!ts_node_is_null(previous) && strcmp(ts_node_type(previous), "comment") == 0)
            previous = ts_node_prev_named_sibling(previous);
        if (ts_node_is_null(previous) || ts_node_named_child_count(previous) != 1)
            return false;
        const char *previous_kind = ts_node_type(previous);
        bool error_wrapper = strcmp(previous_kind, "ERROR") == 0;
        if (!error_wrapper && strcmp(previous_kind, "expression_statement") != 0)
            return false;
        TSNode call = ts_node_named_child(previous, 0);
        if (strcmp(ts_node_type(call), "call_expression") != 0)
            return false;
        /* An empty invocation such as A(){} is an ERROR wrapping exactly the
         * call. Recover only that byte-exact shape; the argument validator then
         * reports MISSING_ARGUMENT for A, rather than the audit's generic form
         * error. Never consume an error region with extra unparsed content. */
        if (error_wrapper && (ts_node_start_byte(previous) != ts_node_start_byte(call) ||
                              ts_node_end_byte(previous) != ts_node_end_byte(call)))
            return false;
        TSNode callee = ts_node_child_by_field_name(call, TS_FIELD("function"));
        if (ts_node_is_null(callee) || strcmp(ts_node_type(callee), "identifier") != 0)
            return false;
        *name = callee;
        *body = scope;
        return true;
    }
    TSNode inner = unwrap_template_inner(scope, language);
    *body = ts_node_child_by_field_name(inner, TS_FIELD("body"));
    if (language == CBM_LANG_C && strcmp(kind, "function_definition") == 0) {
        TSNode type = ts_node_child_by_field_name(scope, TS_FIELD("type"));
        TSNode declarator = ts_node_child_by_field_name(scope, TS_FIELD("declarator"));
        if (!ts_node_is_null(type) && strcmp(ts_node_type(type), "type_identifier") == 0 &&
            !ts_node_is_null(declarator) &&
            strcmp(ts_node_type(declarator), "parenthesized_declarator") == 0) {
            *name = type;
            return true;
        }
    }
    *name = cbm_resolve_func_name(scope, language);
    return !ts_node_is_null(*name);
}

static bool tdx_observe(CBMExtractCtx *ctx, TSNode name_node, TSNode scope_node, const char *qn,
                        const TDXRule *rule);

static bool tdx_resolve(CBMExtractCtx *ctx, TSNode node, TSNode name_node, const char *macro,
                        const char **name, TDXRule *rule) {
    *name = NULL;
    uint32_t line = ts_node_start_point(name_node).row + 1;
    if (!tdx_match(ctx, macro, strlen(macro), line, rule))
        return false;
    if (ctx->result->test_declarations_status == CBM_TEST_EXTRACT_OOM)
        return true;
    if (!tdx_observe(ctx, name_node, node, NULL, rule) || !rule->valid)
        return true;
    uint32_t end = ts_node_end_byte(name_node);
    size_t id_at = 0, id_length = 0, after = 0;
    if (!ctx->source || ctx->source_len < 0 || end > (uint32_t)ctx->source_len) {
        tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, rule->declaration, line);
        return true;
    }
    if (!tdx_argument(ctx, end, rule->argument, &id_at, &id_length, &after, rule, line))
        return true;
    TSNode candidate_name, body;
    if (!cbm_test_definition_candidate(node, ctx->language, &candidate_name, &body) ||
        !ts_node_eq(candidate_name, name_node)) {
        tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, rule->declaration, line);
        return true;
    }
    if (!tdx_trivia(ctx->source, (size_t)ctx->source_len, &after) ||
        after >= (size_t)ctx->source_len || ctx->source[after] != '{' || ts_node_is_null(body) ||
        ts_node_has_error(body) || ts_node_is_missing(body) || ts_node_start_byte(body) != after ||
        ts_node_end_byte(body) <= after || ts_node_end_byte(body) > (uint32_t)ctx->source_len ||
        ctx->source[ts_node_end_byte(body) - 1] != '}') {
        tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, rule->declaration, line);
        return true;
    }
    *name = cbm_arena_sprintf(ctx->arena, "%s_%.*s", macro, (int)id_length, ctx->source + id_at);
    if (!*name)
        tdx_issue(ctx->result, CBM_TEST_EXTRACT_OOM, rule->declaration, line);
    return true;
}

/* NULL qn records a recognized attempt, not a graph definition. A later
 * successful extraction fills its QN; the audit must not invent a second,
 * synthetic error for an invocation whose local validation already failed. */
static bool tdx_observe(CBMExtractCtx *ctx, TSNode name_node, TSNode scope_node, const char *qn,
                        const TDXRule *rule) {
    uint32_t line = ts_node_start_point(name_node).row + 1;
    int count = ctx->test_definition_match_count;
    for (int i = 0; i < count; i++) {
        struct CBMTestDefinitionMatch *match = &ctx->test_definition_matches[i];
        if (ts_node_eq(match->name_node, name_node)) {
            if (qn)
                match->qn = qn;
            return true;
        }
    }
    if (count == ctx->test_definition_match_cap) {
        int cap = ctx->test_definition_match_cap;
        if (cap > INT_MAX / 2)
            return tdx_issue(ctx->result, CBM_TEST_EXTRACT_OOM, rule->declaration, line);
        int next = cap ? cap * 2 : 16;
        if ((size_t)next > SIZE_MAX / sizeof(struct CBMTestDefinitionMatch))
            return tdx_issue(ctx->result, CBM_TEST_EXTRACT_OOM, rule->declaration, line);
        CBMArena *scratch = ctx->scratch ? ctx->scratch : ctx->arena;
        struct CBMTestDefinitionMatch *items =
            cbm_arena_alloc(scratch, (size_t)next * sizeof(*items));
        if (!items)
            return tdx_issue(ctx->result, CBM_TEST_EXTRACT_OOM, rule->declaration, line);
        if (count)
            memcpy(items, ctx->test_definition_matches, (size_t)count * sizeof(*items));
        ctx->test_definition_matches = items;
        ctx->test_definition_match_cap = next;
    }
    ctx->test_definition_matches[count] = (struct CBMTestDefinitionMatch){
        ts_node_start_byte(name_node), name_node, scope_node, line, rule->declaration, qn};
    ctx->test_definition_match_count++;
    return true;
}

const char *cbm_test_definition_qn(CBMExtractCtx *ctx, TSNode function) {
    if (!ctx->test_declarations_raw_source || ctx->test_definition_match_count == 0 ||
        ts_node_is_null(function))
        return NULL;
    for (int i = 0; i < ctx->test_definition_match_count; i++)
        if (ts_node_eq(ctx->test_definition_matches[i].scope_node, function))
            return ctx->test_definition_matches[i].qn;
    return NULL;
}

/* Bind the immutable owner rows to these exact raw bytes once per LSP pass.
 * A local digest is identity evidence only, never artifact authentication. */
bool cbm_test_definition_owners_match(const CBMFileResult *owners, const char *source,
                                      int source_len, bool cpp_mode, const char *module_qn) {
    if (!owners)
        return true;
    if (owners->test_declarations_status != CBM_TEST_EXTRACT_OK)
        return false;
    if (!owners->has_test_definition_owners)
        return true;
    if (!source || source_len < 0 || source_len != owners->test_owner_source_len || !module_qn ||
        !owners->module_qn || strcmp(module_qn, owners->module_qn) != 0 ||
        !tdx_source_language(owners->test_owner_language) ||
        cpp_mode != (owners->test_owner_language != CBM_LANG_C))
        return false;
    char digest[65];
    cbm_sha256_hex(source, (size_t)source_len, digest);
    return memcmp(digest, owners->test_owner_source_sha256, sizeof(digest)) == 0;
}

/* The caller has already bound this result to source bytes and RAW origin.
 * Exact name AND body spans distinguish nodes, including nested definitions;
 * never infer an owner from a line window or a guessed macro name. */
const char *cbm_test_definition_owner_qn(const CBMFileResult *owners, TSNode function) {
    if (!owners || !owners->has_test_definition_owners || ts_node_is_null(function))
        return NULL;
    TSNode name, body;
    if (!cbm_test_definition_candidate(function, owners->test_owner_language, &name, &body) ||
        ts_node_is_null(body))
        return NULL;
    const char *found = NULL;
    for (int i = 0; i < owners->defs.count; i++) {
        const CBMDefinition *def = &owners->defs.items[i];
        if (def->test_role == CBM_TEST_ROLE_NONE ||
            def->test_name_start_byte != ts_node_start_byte(name) ||
            def->test_name_end_byte != ts_node_end_byte(name) ||
            def->test_body_start_byte != ts_node_start_byte(body) ||
            def->test_body_end_byte != ts_node_end_byte(body))
            continue;
        if (found)
            return NULL; /* duplicate rows are rejected by finish */
        found = def->qualified_name;
    }
    return found;
}

/* Bound a directive only after complete comments/literals, not at a physical
 * newline inside a block comment. The audit view already has line splices
 * removed. This locates its extent; tdx_directive validates consumed content. */
static bool tdx_directive_end(const char *s, size_t n, size_t *at, uint32_t *line) {
    while (*at < n) {
        if (s[*at] == '\0')
            return false;
        if (s[*at] == '\n') {
            (*at)++;
            (*line)++;
            return true;
        }
        if (*at + 1 < n && s[*at] == '/' && s[*at + 1] == '*') {
            *at += 2;
            while (*at + 1 < n && !(s[*at] == '*' && s[*at + 1] == '/')) {
                if (s[*at] == '\0')
                    return false;
                if (s[*at] == '\n')
                    (*line)++;
                (*at)++;
            }
            if (*at + 1 >= n)
                return false;
            *at += 2;
            continue;
        }
        if (*at + 1 < n && s[*at] == '/' && s[*at + 1] == '/') {
            *at += 2;
            while (*at < n && s[*at] != '\n') {
                if (s[*at] == '\0')
                    return false;
                (*at)++;
            }
            continue;
        }
        if (tdx_pp_number(s, n, at))
            continue;
        if (s[*at] == '"' || s[*at] == '\'') {
            size_t before = *at;
            if (!tdx_audit_literal(s, n, at)) {
                /* Let the consumed-directive parser report malformed literal
                 * content. Do not reinterpret its comment punctuation here. */
                *at = before + 1;
                while (*at < n && s[*at] != '\n') {
                    if (s[*at] == '\0')
                        return false;
                    (*at)++;
                }
            }
            for (size_t i = before; i < *at; i++)
                if (s[i] == '\n')
                    (*line)++;
            continue;
        }
        (*at)++;
    }
    return true;
}

/* A replacement list referring to a configured definition is not a raw test
 * declaration. Surface it as unsupported instead of certifying only the
 * handwritten subset. The macro being defined is deliberately skipped. */
static void tdx_directive(CBMExtractCtx *ctx, size_t begin, size_t end, uint32_t line) {
    const char *s = ctx->source + begin;
    end -= begin;
    /* Joining physical directive lines only detects opaque generated tests;
     * the joined bytes never become declaration or graph source evidence. */
    bool spliced = false;
    for (size_t i = 0; i < end; i++)
        if (tdx_splice(s, end, i)) {
            spliced = true;
            break;
        }
    if (spliced) {
        CBMArena *scratch = ctx->scratch ? ctx->scratch : ctx->arena;
        char *joined = cbm_arena_alloc(scratch, end + 1);
        if (!joined) {
            tdx_issue(ctx->result, CBM_TEST_EXTRACT_OOM, -1, line);
            return;
        }
        size_t used = 0;
        for (size_t i = 0; i < end;) {
            if (tdx_splice(s, end, i))
                i += s[i + 1] == '\r' ? 3 : 2;
            else
                joined[used++] = s[i++];
        }
        joined[used] = '\0';
        s = joined;
        end = used;
    }
    size_t at = 1;
    if (!tdx_trivia(s, end, &at)) {
        tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, -1, line);
        return;
    }
    size_t word = at;
    while (at < end && tdx_ident((unsigned char)s[at]))
        at++;
    if (!tdx_equal("define", s + word, at - word))
        return;
    if (!tdx_trivia(s, end, &at) || at == end || !tdx_ident_first((unsigned char)s[at])) {
        tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, -1, line);
        return;
    }
    while (at < end && tdx_ident((unsigned char)s[at]))
        at++;
    /* A function-like macro's formal parameters are not its replacement. */
    if (at < end && s[at] == '(') {
        while (at < end && s[at] != ')')
            at++;
        if (at == end) {
            tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, -1, line);
            return;
        }
        at++;
    }
    while (at < end) {
        if (tdx_splice(s, end, at)) {
            at += s[at + 1] == '\r' ? 3 : 2;
            continue;
        }
        if (!tdx_trivia(s, end, &at)) {
            tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, -1, line);
            return;
        }
        if (at == end)
            return;
        if (tdx_pp_number(s, end, &at))
            continue;
        if (s[at] == '"' || s[at] == '\'') {
            if (!tdx_audit_literal(s, end, &at)) {
                tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, -1, line);
                return;
            }
            continue;
        }
        if (!tdx_ident_first((unsigned char)s[at])) {
            at++;
            continue;
        }
        word = at++;
        while (at < end && tdx_ident((unsigned char)s[at]))
            at++;
        TDXRule rule;
        if (tdx_match(ctx, s + word, at - word, line, &rule))
            tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, rule.declaration, line);
    }
}

void cbm_test_declarations_finish(CBMExtractCtx *ctx) {
    if (!ctx->test_declarations || !ctx->test_declarations_raw_source ||
        ctx->result->test_declarations_status == CBM_TEST_EXTRACT_OOM)
        return;
    int language = tdx_source_language(ctx->language), count = 0;
    if (!language)
        return;
    const cbm_test_declaration_t *items =
        cbm_test_declarations_items(ctx->test_declarations, &count);
    bool active = language == 1 && tdx_preset(ctx, CBM_TEST_PRESET_C_CBM);
    for (int i = 0; i < count && !active; i++)
        active = (items[i].role == CBM_TEST_DECL_CASE || items[i].role == CBM_TEST_DECL_SUITE) &&
                 tdx_language(items[i].language) == language;
    if (!active)
        return;
    /* Detect configured invocations dropped by a grammar/error region. Never
     * invent definitions here; an unmatched raw invocation is uncertainty.
     * Normalize line splices for AUDIT ONLY, so a split macro cannot disappear.
     * If that view contains a consumed invocation, decline the file; do not
     * use normalized offsets as raw graph/owner evidence. A complete audit of
     * unrelated code succeeds; an incomplete applicable audit is uncertainty
     * even if no configured invocation was reached before it stopped. */
    const char *s = ctx->source;
    size_t n = ctx->source_len > 0 ? (size_t)ctx->source_len : 0, at = 0;
    uint32_t splice_line = 0, physical_line = 1;
    for (size_t i = 0; i < n; i++) {
        if (!splice_line && tdx_splice(s, n, i))
            splice_line = physical_line;
        if (s[i] == '\n')
            physical_line++;
    }
    if (splice_line) {
        CBMArena *scratch = ctx->scratch ? ctx->scratch : ctx->arena;
        char *joined = cbm_arena_alloc(scratch, n + 1);
        if (!joined) {
            tdx_issue(ctx->result, CBM_TEST_EXTRACT_OOM, -1, splice_line);
            return;
        }
        size_t used = 0;
        for (size_t i = 0; i < n;) {
            if (tdx_splice(s, n, i))
                i += s[i + 1] == '\r' ? 3 : 2;
            else
                joined[used++] = s[i++];
        }
        joined[used] = '\0';
        s = joined;
        n = used;
    }
    CBMExtractCtx audit = *ctx;
    audit.source = s;
    audit.source_len = (int)n;
    uint32_t line = 1, uncertain_line = 0;
    bool line_start = true;
    while (at < n) {
        size_t before = at;
        if (!tdx_trivia(s, n, &at)) {
            uncertain_line = line;
            break;
        }
        for (size_t i = before; i < at; i++)
            if (s[i] == '\n') {
                line++;
                line_start = true;
            }
        if (at == n)
            break;
        if (s[at] == '\0') {
            uncertain_line = line;
            break;
        }
        if (line_start && s[at] == '#') {
            size_t directive_begin = at;
            uint32_t directive_line = splice_line ? splice_line : line;
            if (!tdx_directive_end(s, n, &at, &line)) {
                uncertain_line = directive_line;
                break;
            }
            tdx_directive(&audit, directive_begin, at, directive_line);
            line_start = true;
            continue;
        }
        line_start = false;
        if (tdx_pp_number(s, n, &at))
            continue;
        if (s[at] == '"' || s[at] == '\'') {
            before = at;
            if (!tdx_audit_literal(s, n, &at)) {
                uncertain_line = line;
                break;
            }
            for (size_t i = before; i < at; i++)
                if (s[i] == '\n')
                    line++;
            continue;
        }
        if (!tdx_ident_first((unsigned char)s[at])) {
            at++;
            continue;
        }
        size_t begin = at++;
        while (at < n && tdx_ident((unsigned char)s[at]))
            at++;
        size_t after = at;
        if (!tdx_trivia(s, n, &after)) {
            uncertain_line = line;
            break;
        }
        if (after == n || s[after] != '(')
            continue;
        TDXRule rule;
        uint32_t issue_line = splice_line ? splice_line : line;
        if (!tdx_match(ctx, s + begin, at - begin, issue_line, &rule))
            continue;
        bool found = false;
        if (!splice_line) {
            for (int i = 0; i < ctx->test_definition_match_count; i++)
                if (ctx->test_definition_matches[i].name_byte == begin) {
                    found = true;
                    break;
                }
        }
        if (!found)
            tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, rule.declaration, issue_line);
    }
    if (uncertain_line)
        tdx_issue(ctx->result, CBM_TEST_EXTRACT_UNSUPPORTED_FORM, -1,
                  splice_line ? splice_line : uncertain_line);
    /* A configured QN collision is explicit rather than a later upsert loss. */
    for (int i = 0; i < ctx->test_definition_match_count; i++) {
        const struct CBMTestDefinitionMatch *match = &ctx->test_definition_matches[i];
        if (!match->qn)
            continue; /* failed invocation; diagnostic already retained */
        int found = 0;
        for (int j = 0; j < ctx->result->defs.count; j++) {
            const char *qn = ctx->result->defs.items[j].qualified_name;
            if (qn && strcmp(qn, match->qn) == 0)
                found++;
        }
        if (found != 1)
            tdx_issue(ctx->result, CBM_TEST_EXTRACT_AMBIGUOUS, match->declaration, match->line);
    }
    if (ctx->test_definition_match_count &&
        ctx->result->test_declarations_status == CBM_TEST_EXTRACT_OK) {
        ctx->result->has_test_definition_owners = true;
        ctx->result->test_owner_source_len = ctx->source_len;
        ctx->result->test_owner_language = ctx->language;
        cbm_sha256_hex(ctx->source, (size_t)ctx->source_len, ctx->result->test_owner_source_sha256);
    }
}

static void extract_func_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    CBMArena *a = ctx->arena;

    TSNode name_node = cbm_resolve_func_name(node, ctx->language);
    if (ctx->language == CBM_LANG_C && ctx->test_declarations &&
        ctx->test_declarations_raw_source) {
        TSNode candidate_name, candidate_body;
        if (cbm_test_definition_candidate(node, ctx->language, &candidate_name, &candidate_body)) {
            uint32_t begin = ts_node_start_byte(candidate_name);
            uint32_t end = ts_node_end_byte(candidate_name);
            TDXRule rule;
            if (end > begin && end <= (uint32_t)ctx->source_len &&
                tdx_match(ctx, ctx->source + begin, end - begin,
                          ts_node_start_point(candidate_name).row + 1, &rule))
                name_node = candidate_name;
        }
    }
    if (ts_node_is_null(name_node)) {
        return;
    }

    char *name = cbm_func_name_node_text(a, name_node, ctx->source, ctx->language);
    if (!name || !name[0] || strcmp(name, "function") == 0) {
        return;
    }

    // Makefile special targets (.PHONY, .DEFAULT, .SUFFIXES, …) are directives,
    // not build-rule defs. Their leading '.' would also make cbm_fqn_compute
    // emit a "..PHONY" segment (a "double dot") and thus a malformed QN. Skip
    // any dot-prefixed Make target.
    if (ctx->language == CBM_LANG_MAKEFILE && name[0] == '.') {
        return;
    }

    /* C++/CUDA: GoogleTest macros (TEST, TEST_F, TEST_P, ...) parse as
     * function definitions whose name resolves to the bare macro identifier.
     * Multiple test cases per file collide on qualified name; derive a unique
     * name from the macro arguments so each gets its own graph node (#1266). */
    bool is_gtest = false;
    const char *configured_name = NULL;
    TDXRule configured_rule;
    bool configured = tdx_resolve(ctx, node, name_node, name, &configured_name, &configured_rule);
    if (configured) {
        if (!configured_name)
            return;
        name = (char *)configured_name;
    }
    if (!configured && tdx_preset(ctx, CBM_TEST_PRESET_GTEST) &&
        (ctx->language == CBM_LANG_CPP || ctx->language == CBM_LANG_CUDA) &&
        is_cpp_test_macro(name)) {
        char *gtest_name = resolve_cpp_test_macro_name(a, name, node, ctx->source);
        if (gtest_name) {
            name = gtest_name;
            is_gtest = true;
        }
    }

    /* Nix `"${foo}" = x: …`: an interpolated attrpath segment has no statically
     * knowable name. Minting it would produce a def literally named `"${foo}"`
     * that nothing can ever look up or resolve a call against. An absent node is
     * the honest answer — same reasoning as the Makefile guard above. */
    if (ctx->language == CBM_LANG_NIX && cbm_nix_attr_is_interpolated(name_node)) {
        return;
    }

    TSNode func_node = unwrap_template_inner(node, ctx->language);

    CBMDefinition def;
    memset(&def, 0, sizeof(def));

    def.name = name;
    def.test_role = configured ? configured_rule.role : CBM_TEST_ROLE_NONE;
    if (configured) {
        TSNode candidate_name, body;
        (void)cbm_test_definition_candidate(node, ctx->language, &candidate_name, &body);
        def.test_name_start_byte = ts_node_start_byte(name_node);
        def.test_name_end_byte = ts_node_end_byte(name_node);
        def.test_body_start_byte = ts_node_start_byte(body);
        def.test_body_end_byte = ts_node_end_byte(body);
    }
    /* Nix: a binding's name is a path. The leaf is the name; the leading segments
     * are scope, so `a.b.fn = …` gets the same QN as `a = { b = { fn = …; }; }`.
     * Without this every binding whose path shares a leaf name collapsed onto one
     * node, silently discarding the later definition and its CALLS edges. */
    const char *qn_name = name;
    if (ctx->language == CBM_LANG_NIX) {
        qn_name = cbm_nix_qn_name(a, node, ctx->source, name);
    }
    /* Java/Go derive the module from the containing directory (package), so the
     * filename stem is NOT baked into the QN (Go func in myapp/db/conn.go ->
     * proj.myapp.db.Func, not proj.myapp.db.conn.Func). Other langs unchanged. */
    def.qualified_name =
        cbm_fqn_compute_source_lang(a, ctx->project, ctx->rel_path, qn_name, ctx->language);
    /* C-family platform variants (foo_win.c / foo_posix.c, src/unix/ vs
     * src/win/): an external-linkage file-scope function takes its
     * platform-neutral QN, so its variants are one definition. The call-scope
     * twin in extract_unified.c applies the same rule. */
    if (!ctx->enclosing_class_qn) {
        def.qualified_name = cbm_platform_variant_qn(a, ctx->language, ctx->project, ctx->rel_path,
                                                     qn_name, def.qualified_name, node);
    }
    /* A free function declared inside a namespace (C++/C#/PHP) is qualified by
     * the namespace scope the def walk carries (enclosing_class_qn was extended
     * by is_namespace_scope_kind), so `ns::serialize` is `proj.file.ns.serialize`
     * — without this it collapses to the file scope and namespace-aware
     * resolution (ADL, namespace-function lookup) can never see it. Class methods
     * never reach here (they go through extract_class_methods), so a set
     * enclosing scope here is always a namespace. The out-of-line method path
     * below overrides this for `Ns::Cls::method` definitions.
     *
     * Nix joins this list for the same reason: a binding inside an attrset is
     * scoped by it, so `setA = { fn = ...; }` is proj.file.setA.fn. Uses qn_name,
     * not name, so an attrpath's own leading segments compose with the enclosing
     * scope. Note the call-scope side (compute_func_qn in extract_unified.c) is
     * NOT language-gated — it qualifies whenever a scope is pushed — so this gate
     * and the scope-push rule must move together, or a call QN names a def QN that
     * was never minted and the edge is dropped at write. */
    if (ctx->enclosing_class_qn &&
        (ctx->language == CBM_LANG_CPP || ctx->language == CBM_LANG_CUDA ||
         ctx->language == CBM_LANG_TYPESCRIPT || ctx->language == CBM_LANG_TSX ||
         ctx->language == CBM_LANG_ARKTS || ctx->language == CBM_LANG_NIX)) {
        def.qualified_name = cbm_arena_sprintf(a, "%s.%s", ctx->enclosing_class_qn, qn_name);
    }
    def.label = "Function";
    def.file_path = ctx->rel_path;
    def.start_line =
        ts_node_start_point(configured && ctx->language == CBM_LANG_C ? name_node : node).row +
        TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
    def.is_exported = cbm_is_exported(name, ctx->language);
    if (ctx->language == CBM_LANG_RUST &&
        strcmp(ts_node_type(node), "function_signature_item") == 0) {
        def.is_abstract = true;
    }

    // Parameters — use func_node (inner function for templates)
    TSNode params = configured && ctx->language == CBM_LANG_C
                        ? (TSNode){0}
                        : find_function_params(func_node, ctx->language);
    if (!ts_node_is_null(params)) {
        def.signature = cbm_node_text(a, params, ctx->source);
        def.param_names = extract_param_names(a, params, ctx->source, ctx->language);
        def.param_types = extract_param_types(a, params, ctx->source, ctx->language);
        def.signature_param_types = extract_signature_param_types(
            a, params, ctx->source, ctx->language, false, &def.signature_param_count);
    }

    // Return type — use func_node (inner function for templates)
    static const char *rt_fields[] = {"result", "return_type", "type", NULL};
    for (const char **f = rt_fields; *f && !(configured && ctx->language == CBM_LANG_C); f++) {
        TSNode rt = ts_node_child_by_field_name(func_node, *f, (uint32_t)strlen(*f));
        if (!ts_node_is_null(rt)) {
            def.return_type = c_declared_return_type(ctx, func_node, rt);
            def.return_types = extract_return_types(a, rt, ctx->source, ctx->language);
            break;
        }
    }

    // C++: trailing return type (auto f() -> Type)
    if (def.return_type && strcmp(def.return_type, "auto") == 0 &&
        (ctx->language == CBM_LANG_CPP || ctx->language == CBM_LANG_CUDA)) {
        resolve_cpp_trailing_return(a, func_node, ctx->source, &def);
    }

    // Receiver (Go methods)
    TSNode recv = ts_node_child_by_field_name(node, TS_FIELD("receiver"));
    if (!ts_node_is_null(recv)) {
        def.receiver = cbm_node_text(a, recv, ctx->source);
        def.label = "Method";
        /* Derive parent_class from the receiver type so DEFINES_METHOD edges
         * (and downstream Go IMPLEMENTS/OVERRIDE) link the method to its owning
         * struct/type node.  The parent QN must match the type's node QN, which
         * is computed the same way (cbm_fqn_compute on the type name). */
        char *recv_type = go_receiver_type_name(a, recv, ctx->source);
        if (recv_type && recv_type[0]) {
            /* Must match the Go type node QN (directory-based module) so the
             * DEFINES_METHOD edge links the method to its owning type. */
            def.parent_class = cbm_fqn_compute_source_lang(a, ctx->project, ctx->rel_path,
                                                           recv_type, ctx->language);
        }
    }

    // C++/CUDA: out-of-line method definition (`Foo::bar` in a .cc/.cpp). The
    // class body in the header is declaration-only, so without this the
    // definition is recorded as a free Function. Promote it to a Method whose QN
    // is scoped to its class and whose parent_class links it back (matching the
    // class node QN computed the same way) so DEFINES_METHOD edges resolve.
    if ((ctx->language == CBM_LANG_CPP || ctx->language == CBM_LANG_CUDA) &&
        strcmp(ts_node_type(node), "function_definition") == 0) {
        char *scope_name = cbm_cpp_out_of_line_parent_class(a, node, ctx->source);
        if (scope_name && scope_name[0]) {
            const char *class_qn = cbm_fqn_compute(a, ctx->project, ctx->rel_path, scope_name);
            def.qualified_name = cbm_arena_sprintf(a, "%s.%s", class_qn, name);
            def.label = "Method";
            def.parent_class = class_qn;
        }
    }

    // Pony: fun/be/new (method/constructor/ffi_method) live in pony_func_types,
    // so the main def-walk extracts them here as "Function"; but one declared
    // inside a class/actor/struct/trait/interface/primitive IS a method. Detect
    // the enclosing class-like ancestor and promote it to "Method" with a
    // parent_class link (the class name is the first identifier child — no field).
    if (ctx->language == CBM_LANG_PONY && def.label && strcmp(def.label, "Function") == 0 &&
        spec->class_node_types) {
        for (TSNode cur = ts_node_parent(node); !ts_node_is_null(cur); cur = ts_node_parent(cur)) {
            if (cbm_kind_in_set(cur, spec->class_node_types)) {
                def.label = "Method";
                TSNode cn = cbm_find_child_by_kind(cur, "identifier");
                if (!ts_node_is_null(cn)) {
                    char *cname = cbm_node_text(a, cn, ctx->source);
                    if (cname && cname[0]) {
                        def.parent_class = cbm_fqn_compute(a, ctx->project, ctx->rel_path, cname);
                    }
                }
                break;
            }
        }
    }

    // Decorators + route extraction from decorator AST
    def.decorators = extract_decorators(a, node, ctx->source, ctx->language, spec);
    extract_route_from_decorators(a, node, ctx->source, spec, &def.route_path, &def.route_method);

    // Rust: cfg-gated twins (#[cfg(windows)] fn f / #[cfg(not(windows))] fn f)
    // keep the plain QN: they are variants of ONE definition, and the graph
    // keeps every twin's span and the union of their bodies' calls on one node
    // (graph_buffer.c, "Definition variants"). #495's lost branch is a variant
    // now; the old per-twin `f#cfg(...)` QNs left the bodies' calls without a
    // source node (calls are scoped to the plain QN).
    if (ctx->language == CBM_LANG_RUST) {
        def.is_test = rust_def_is_test(def.decorators);
    }

    // C++/CUDA: GoogleTest macros are test functions (#1266).
    if (is_gtest || def.test_role == CBM_TEST_ROLE_CASE) {
        def.is_test = true;
    }

    // A function/method defined in a file cbm treats as a test file is itself
    // a test, regardless of its own name (tests/helpers/fixtures.c has none
    // of the test_/_test naming conventions, but every function in it is
    // still test code) (#1294). OR'd so the Rust attribute check above, which
    // additionally catches #[test] functions embedded in an otherwise regular
    // .rs file, is never downgraded by this.
    def.is_test = def.is_test || ctx->result->is_test_file;

    // Docstring
    def.docstring = extract_docstring(ctx, node, name);

    // Complexity
    if (spec->branching_node_types && spec->branching_node_types[0]) {
        set_def_complexity(&def, node, spec);
    }

    // MinHash fingerprint
    compute_fingerprint(ctx, &def, func_node);

    // JS/TS export detection
    if (ctx->language == CBM_LANG_JAVASCRIPT || ctx->language == CBM_LANG_TYPESCRIPT ||
        ctx->language == CBM_LANG_TSX || ctx->language == CBM_LANG_ARKTS) {
        if (is_js_exported(node)) {
            def.is_entry_point = true;
        }
    }

    // main is always an entry point
    if (strcmp(name, "main") == 0) {
        def.is_entry_point = true;
    }

    if (configured) {
        if (!def.qualified_name) {
            tdx_issue(ctx->result, CBM_TEST_EXTRACT_OOM, configured_rule.declaration,
                      ts_node_start_point(name_node).row + 1);
            return;
        }
        if (!tdx_observe(ctx, name_node, node, def.qualified_name, &configured_rule))
            return;
    }
    cbm_defs_push(&ctx->result->defs, a, def);
}

// --- Class definition extraction ---

// Push a simple class definition (used by config language extractors).
// Replace each run of whitespace in `name` with a single '-' so the value is a
// well-formed QN segment. Markdown headings (e.g. "Codebase Memory") legitimately
// contain spaces; embedding them verbatim in a QN makes it malformed. Returns the
// original pointer when there is no whitespace to collapse. The human-readable
// def.name is kept intact; only the QN segment is slugified.
static const char *qn_safe_segment(CBMArena *a, const char *name) {
    if (!name) {
        return name;
    }
    bool has_ws = false;
    for (const char *p = name; *p; p++) {
        if (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') {
            has_ws = true;
            break;
        }
    }
    if (!has_ws) {
        return name;
    }
    char *out = cbm_arena_strdup(a, name);
    if (!out) {
        return name;
    }
    char *w = out;
    bool in_ws = false;
    for (char *r = out; *r; r++) {
        if (*r == ' ' || *r == '\t' || *r == '\n' || *r == '\r') {
            if (!in_ws && w != out) {
                *w++ = '-';
            }
            in_ws = true;
        } else {
            *w++ = *r;
            in_ws = false;
        }
    }
    *w = '\0';
    return out;
}

/* UTF-8 sequence classification: a lead byte's high bits name the sequence
 * length, and every continuation byte matches 10xxxxxx. */
enum {
    UTF8_CONT_MASK = 0xC0,  /* isolate the two high bits ... */
    UTF8_CONT_MARK = 0x80,  /* ... which are 10 on a continuation byte */
    UTF8_LEAD2_MASK = 0xE0, /* 110xxxxx → 2-byte sequence */
    UTF8_LEAD2_MARK = 0xC0,
    UTF8_LEAD3_MASK = 0xF0, /* 1110xxxx → 3-byte sequence */
    UTF8_LEAD3_MARK = 0xE0,
    UTF8_LEAD4_MASK = 0xF8, /* 11110xxx → 4-byte sequence */
    UTF8_LEAD4_MARK = 0xF0,
    UTF8_LEN_1 = 1,
    UTF8_LEN_2 = 2,
    UTF8_LEN_3 = 3,
    UTF8_LEN_4 = 4,
};

/* Bytes the sequence starting with `lead` occupies (1 for ASCII or a byte that
 * is not a valid lead). */
static size_t utf8_sequence_len(unsigned char lead) {
    if ((lead & UTF8_LEAD2_MASK) == UTF8_LEAD2_MARK) {
        return UTF8_LEN_2;
    }
    if ((lead & UTF8_LEAD3_MASK) == UTF8_LEAD3_MARK) {
        return UTF8_LEN_3;
    }
    if ((lead & UTF8_LEAD4_MASK) == UTF8_LEAD4_MARK) {
        return UTF8_LEN_4;
    }
    return UTF8_LEN_1;
}

/* Drop a trailing PARTIAL UTF-8 sequence left behind by a byte-length cut, so
 * a capped prose value never ends mid-codepoint (#1017's rule, applied to the
 * prose bodies below rather than to comments). */
static void utf8_trim_partial_tail(char *text) {
    size_t n = strlen(text);
    if (n == 0) {
        return;
    }
    size_t i = n;
    while (i > 0 && ((unsigned char)text[i - UTF8_LEN_1] & UTF8_CONT_MASK) == UTF8_CONT_MARK) {
        i--;
    }
    if (i == 0) {
        text[0] = '\0'; /* continuation bytes only — not decodable */
        return;
    }
    size_t need = utf8_sequence_len((unsigned char)text[i - UTF8_LEN_1]);
    if (i - UTF8_LEN_1 + need > n) {
        text[i - UTF8_LEN_1] = '\0';
    }
}

/* Collapse `len` bytes of raw prose into a single-spaced value capped at
 * MAX_COMMENT_LEN (500 bytes, which leaves room inside the 2 KB properties
 * buffer that carries it).
 *
 * The output buffer is FIXED at that cap, so a section body of any size costs a
 * bounded copy rather than a copy of the whole section. Returns NULL when
 * nothing but whitespace was there. */
static char *collapse_prose(CBMArena *a, const char *src, size_t len) {
    if (!src || len == 0) {
        return NULL;
    }
    char *out = (char *)cbm_arena_alloc(a, MAX_COMMENT_LEN + NULL_TERM);
    if (!out) {
        return NULL;
    }
    size_t w = 0;
    bool in_ws = true; /* start true so leading whitespace is swallowed */
    for (size_t i = 0; i < len && w < MAX_COMMENT_LEN; i++) {
        unsigned char c = (unsigned char)src[i];
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
            in_ws = true;
            continue;
        }
        if (in_ws && w > 0) {
            out[w++] = ' ';
            if (w >= MAX_COMMENT_LEN) {
                break;
            }
        }
        in_ws = false;
        out[w++] = (char)c;
    }
    out[w] = '\0';
    utf8_trim_partial_tail(out);
    return out[0] ? out : NULL;
}

static bool is_markdown_heading_kind(const char *kind) {
    return strcmp(kind, "atx_heading") == 0 || strcmp(kind, "setext_heading") == 0;
}

/* #518: a Markdown heading node is only the title line — the section's prose
 * lives in the blocks that FOLLOW it. Collect that prose as the Section's
 * docstring so the node carries the text a reader would search for; nodes_fts
 * indexes it from there (`body`), which is the whole point of the issue.
 *
 * tree-sitter-markdown wraps a heading and its content in a `section` node,
 * with nested subsections as further `section` children that own their own
 * text. Stopping at either a `section` or another heading therefore gives each
 * Section exactly its OWN body under both that shape and a flat one. The bytes
 * between are contiguous in the source, so one slice beats concatenation. */
static const char *extract_markdown_section_body(CBMArena *a, TSNode heading, const char *source) {
    uint32_t start = ts_node_end_byte(heading);
    uint32_t end = start;
    for (TSNode sib = ts_node_next_sibling(heading); !ts_node_is_null(sib);
         sib = ts_node_next_sibling(sib)) {
        const char *sk = ts_node_type(sib);
        if (strcmp(sk, "section") == 0 || is_markdown_heading_kind(sk)) {
            break;
        }
        end = ts_node_end_byte(sib);
    }
    if (end <= start) {
        return NULL;
    }
    return collapse_prose(a, source + start, (size_t)(end - start));
}

/* Push a config-language definition that carries prose. push_simple_class_def
 * delegates here with no docstring. */
static void push_simple_class_def_doc(CBMExtractCtx *ctx, TSNode node, char *name,
                                      const char *label, const char *docstring) {
    CBMArena *a = ctx->arena;
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, qn_safe_segment(a, name));
    def.label = label;
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.is_exported = true;
    def.docstring = docstring;
    cbm_defs_push(&ctx->result->defs, a, def);
}

static void push_simple_class_def(CBMExtractCtx *ctx, TSNode node, char *name, const char *label) {
    push_simple_class_def_doc(ctx, node, name, label, NULL);
}

// Find TOML table key name from children.
static char *find_toml_key_name(CBMArena *a, TSNode node, const char *source) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "bare_key") == 0 || strcmp(ck, "dotted_key") == 0 ||
            strcmp(ck, "quoted_key") == 0 || strcmp(ck, "key") == 0) {
            return cbm_node_text(a, child, source);
        }
    }
    return NULL;
}

// Extract XML element name from start_tag/self_closing_tag children.
static char *find_xml_element_name(CBMArena *a, TSNode node, const char *source) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "start_tag") == 0 || strcmp(ck, "self_closing_tag") == 0 ||
            strcmp(ck, "STag") == 0 || strcmp(ck, "EmptyElemTag") == 0) {
            uint32_t tnc = ts_node_child_count(child);
            for (uint32_t j = 0; j < tnc; j++) {
                TSNode tag = ts_node_child(child, j);
                const char *tk = ts_node_type(tag);
                if (strcmp(tk, "tag_name") == 0 || strcmp(tk, "Name") == 0) {
                    return cbm_node_text(a, tag, source);
                }
            }
        }
    }
    // Fallback: try "Name" field directly for some XML grammars
    TSNode name_child = cbm_find_child_by_kind(node, "Name");
    if (!ts_node_is_null(name_child)) {
        return cbm_node_text(a, name_child, source);
    }
    return NULL;
}

// Extract text from an atx_heading node (# Title).
static char *extract_atx_heading_text(CBMArena *a, TSNode node, const char *source) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "heading_content") == 0 || strcmp(ck, "inline") == 0) {
            return cbm_node_text(a, child, source);
        }
    }
    // Fallback: strip leading # and spaces from full text
    char *full = cbm_node_text(a, node, source);
    if (full) {
        char *p = full;
        while (*p == '#') {
            p++;
        }
        while (*p == ' ') {
            p++;
        }
        if (*p) {
            return cbm_arena_strdup(a, p);
        }
    }
    return NULL;
}

// Trim trailing whitespace/newlines from a heading name in-place.
static char *trim_heading_name(char *name) {
    if (!name || !name[0]) {
        return NULL;
    }
    size_t len = strlen(name);
    while (len > 0 && (name[len - SKIP_CHAR] == '\n' || name[len - SKIP_CHAR] == '\r' ||
                       name[len - SKIP_CHAR] == ' ')) {
        name[len - SKIP_CHAR] = '\0';
        len--;
    }
    return (name[0]) ? name : NULL;
}

// Extract Markdown heading name from atx_heading or setext_heading.
static char *extract_markdown_heading_name(CBMArena *a, TSNode node, const char *kind,
                                           const char *source) {
    char *name = NULL;
    if (strcmp(kind, "atx_heading") == 0) {
        name = extract_atx_heading_text(a, node, source);
    } else {
        if (ts_node_child_count(node) > 0) {
            name = cbm_node_text(a, ts_node_child(node, 0), source);
        }
    }
    return trim_heading_name(name);
}

// INI: extract section name from section node.
static char *find_ini_section_name(CBMArena *a, TSNode node, const char *source) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        if (strcmp(ts_node_type(child), "section_name") != 0) {
            continue;
        }
        // The section_name node spans the whole header line including the
        // surrounding brackets and the trailing newline (e.g. "[database]\n"),
        // which would put '[' / ']' and a '\n' into the QN (malformed). Its
        // inner `text` child holds the bare name ("database").
        TSNode text = cbm_find_child_by_kind(child, "text");
        if (!ts_node_is_null(text)) {
            return cbm_node_text(a, text, source);
        }
        return cbm_node_text(a, child, source);
    }
    return NULL;
}

// HCL: extract block name from identifier child.
static char *find_hcl_block_name(CBMArena *a, TSNode node, const char *source) {
    TSNode id = cbm_find_child_by_kind(node, "identifier");
    if (ts_node_is_null(id)) {
        return NULL;
    }
    char *name = cbm_node_text(a, id, source);
    if (!name || !name[0]) {
        return NULL;
    }
    // Append the block's quoted labels so each block gets a distinct,
    // query-friendly name: resource "aws_instance" "web" -> resource.aws_instance.web
    // rather than every resource collapsing to the bare keyword "resource" (#337).
    // HCL stores labels as string_lit -> template_literal children.
    uint32_t cc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < cc; i++) {
        TSNode ch = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(ch), "string_lit") != 0) {
            continue;
        }
        TSNode lit = cbm_find_child_by_kind(ch, "template_literal");
        if (ts_node_is_null(lit)) {
            continue;
        }
        char *label = cbm_node_text(a, lit, source);
        if (!label || !label[0]) {
            continue;
        }
        char *joined = cbm_arena_sprintf(a, "%s.%s", name, label);
        if (joined) {
            name = joined;
        }
    }
    return name;
}

// Handle config language class nodes (TOML, INI, XML, Markdown, HCL).
// Returns true if handled (caller should return early).
static bool extract_config_class_def(CBMExtractCtx *ctx, TSNode node, const char *kind) {
    CBMArena *a = ctx->arena;
    char *name = NULL;
    const char *label = "Class";
    const char *docstring = NULL;

    if (ctx->language == CBM_LANG_TOML &&
        (strcmp(kind, "table") == 0 || strcmp(kind, "table_array_element") == 0)) {
        name = find_toml_key_name(a, node, ctx->source);
    } else if (ctx->language == CBM_LANG_INI && strcmp(kind, "section") == 0) {
        name = find_ini_section_name(a, node, ctx->source);
    } else if (ctx->language == CBM_LANG_XML && strcmp(kind, "element") == 0) {
        name = find_xml_element_name(a, node, ctx->source);
    } else if (ctx->language == CBM_LANG_MARKDOWN &&
               (strcmp(kind, "atx_heading") == 0 || strcmp(kind, "setext_heading") == 0)) {
        name = extract_markdown_heading_name(a, node, kind, ctx->source);
        // A heading is a Section (a valid label), not a Class — keep the accurate
        // label rather than degrade it to match a test. The markdown repro asserts
        // "Class"; that assertion is the inaccurate side and is flagged for review.
        label = "Section";
        // #518: index what the section SAYS, not just its title.
        docstring = extract_markdown_section_body(a, node, ctx->source);
    } else if (ctx->language == CBM_LANG_HCL && strcmp(kind, "block") == 0) {
        name = find_hcl_block_name(a, node, ctx->source);
    } else {
        return false;
    }

    if (name && name[0]) {
        push_simple_class_def_doc(ctx, node, name, label, docstring);
    }
    return true;
}

// Collect FROM/JOIN table references (tree-sitter-sql `relation` nodes) anywhere
// under `node` and emit them as usages scoped to enclosing_qn. pass_usages then
// resolves each ref_name to the referenced Table/View def and creates a USAGE
// lineage edge (e.g. a view -> the tables it selects from). Emitting them here
// (rather than via the generic identifier walker) sets the correct enclosing
// scope and bypasses the is_definition_name suppression that drops them.
static void collect_sql_relation_usages(CBMExtractCtx *ctx, TSNode node, const char *enclosing_qn) {
    if (strcmp(ts_node_type(node), "relation") == 0) {
        TSNode nm = resolve_sql_func_name(node); // object_reference -> identifier
        if (!ts_node_is_null(nm)) {
            char *tname = cbm_node_text(ctx->arena, nm, ctx->source);
            if (tname && tname[0]) {
                CBMUsage usage = {0};
                usage.ref_name = tname;
                usage.enclosing_func_qn = enclosing_qn;
                cbm_usages_push(&ctx->result->usages, ctx->arena, usage);
            }
        }
    }
    uint32_t n = ts_node_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        collect_sql_relation_usages(ctx, ts_node_child(node, i), enclosing_qn);
    }
}

// Handle SQL DDL relation defs: CREATE TABLE / VIEW / MATERIALIZED VIEW become
// first-class Table/View nodes rather than generic Variable nodes. The relation
// name sits on an object_reference child (the same shape create_function uses),
// so resolve_sql_func_name locates it. Also emits FROM/JOIN dependencies as
// usages so lineage edges form. Returns true if handled.
static bool extract_sql_ddl_class_def(CBMExtractCtx *ctx, TSNode node, const char *kind) {
    if (ctx->language != CBM_LANG_SQL) {
        return false;
    }
    const char *label;
    if (strcmp(kind, "create_table") == 0) {
        label = "Table";
    } else if (strcmp(kind, "create_view") == 0 || strcmp(kind, "create_materialized_view") == 0) {
        label = "View";
    } else {
        return false;
    }
    TSNode name_node = resolve_sql_func_name(node);
    if (ts_node_is_null(name_node)) {
        return false;
    }
    char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!name || !name[0]) {
        return false;
    }
    push_simple_class_def(ctx, node, name, label);
    // Must match push_simple_class_def's QN exactly (qn_safe_segment included)
    // or pass_usages cannot find the enclosing def for the lineage source.
    const char *qn =
        cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, qn_safe_segment(ctx->arena, name));
    collect_sql_relation_usages(ctx, node, qn);
    return true;
}

/* ── Python annotated instance fields (#1277) ─────────────────────
 * Exported as result->field_types (never graph nodes) so the cross-file LSP
 * can type `obj.x` when obj's class lives in another file. Only DECLARED
 * types count: a class-body annotation, a `self.x: T = v` annotation in
 * __init__, or `self.x = p` where p is an annotated __init__ parameter. An
 * unannotated right-hand side is never guessed at. */

static void py_push_field_type(CBMExtractCtx *ctx, const char *class_qn, TSNode name_node,
                               const char *type_text) {
    char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!name || !name[0] || !type_text || !type_text[0]) {
        return;
    }
    CBMFieldType ft = {.class_qn = class_qn, .field_name = name, .type_text = type_text};
    cbm_fieldtype_push(&ctx->result->field_types, ctx->arena, ft);
}

/* The assignment inside an expression_statement, or a null node. */
static TSNode py_statement_assignment(TSNode stmt) {
    TSNode null_node = {0};
    if (strcmp(ts_node_type(stmt), "expression_statement") != 0 ||
        ts_node_named_child_count(stmt) == 0) {
        return null_node;
    }
    TSNode inner = ts_node_named_child(stmt, 0);
    return strcmp(ts_node_type(inner), "assignment") == 0 ? inner : null_node;
}

/* Annotation text of the __init__ parameter called `name`, or NULL. */
static const char *py_init_param_annotation(CBMExtractCtx *ctx, TSNode params, const char *name) {
    uint32_t n = ts_node_named_child_count(params);
    for (uint32_t i = 0; i < n; i++) {
        TSNode p = ts_node_named_child(params, i);
        const char *pk = ts_node_type(p);
        TSNode pname = {0};
        if (strcmp(pk, "typed_default_parameter") == 0) {
            pname = ts_node_child_by_field_name(p, TS_FIELD("name"));
        } else if (strcmp(pk, "typed_parameter") == 0 && ts_node_named_child_count(p) > 0) {
            pname = ts_node_named_child(p, 0);
        } else {
            continue;
        }
        TSNode ptype = ts_node_child_by_field_name(p, TS_FIELD("type"));
        if (ts_node_is_null(pname) || ts_node_is_null(ptype) ||
            strcmp(ts_node_type(pname), "identifier") != 0) {
            continue;
        }
        char *pn = cbm_node_text(ctx->arena, pname, ctx->source);
        if (pn && strcmp(pn, name) == 0) {
            return cbm_node_text(ctx->arena, ptype, ctx->source);
        }
    }
    return NULL;
}

/* `self.x: T = v` / `self.x = p` inside __init__ (self = its first parameter). */
static void py_init_field_assignment(CBMExtractCtx *ctx, const char *class_qn, TSNode assign,
                                     TSNode params, const char *self_name) {
    TSNode left = ts_node_child_by_field_name(assign, TS_FIELD("left"));
    if (ts_node_is_null(left) || strcmp(ts_node_type(left), "attribute") != 0) {
        return;
    }
    TSNode obj = ts_node_child_by_field_name(left, TS_FIELD("object"));
    TSNode attr = ts_node_child_by_field_name(left, TS_FIELD("attribute"));
    if (ts_node_is_null(obj) || ts_node_is_null(attr) ||
        strcmp(ts_node_type(obj), "identifier") != 0) {
        return;
    }
    char *obj_name = cbm_node_text(ctx->arena, obj, ctx->source);
    if (!obj_name || strcmp(obj_name, self_name) != 0) {
        return;
    }
    TSNode ann = ts_node_child_by_field_name(assign, TS_FIELD("type"));
    if (!ts_node_is_null(ann)) {
        py_push_field_type(ctx, class_qn, attr, cbm_node_text(ctx->arena, ann, ctx->source));
        return;
    }
    TSNode right = ts_node_child_by_field_name(assign, TS_FIELD("right"));
    if (ts_node_is_null(right) || strcmp(ts_node_type(right), "identifier") != 0) {
        return;
    }
    char *rhs = cbm_node_text(ctx->arena, right, ctx->source);
    if (rhs && strcmp(rhs, self_name) != 0) {
        py_push_field_type(ctx, class_qn, attr, py_init_param_annotation(ctx, params, rhs));
    }
}

/* Every `self.x` field assignment in one __init__ body, nested blocks
 * included; nested functions, lambdas and classes have their own `self`. */
static void py_extract_init_fields(CBMExtractCtx *ctx, const char *class_qn, TSNode fn) {
    TSNode params = ts_node_child_by_field_name(fn, TS_FIELD("parameters"));
    TSNode body = ts_node_child_by_field_name(fn, TS_FIELD("body"));
    if (ts_node_is_null(params) || ts_node_is_null(body) ||
        ts_node_named_child_count(params) == 0) {
        return;
    }
    TSNode self_node = ts_node_named_child(params, 0);
    if (strcmp(ts_node_type(self_node), "identifier") != 0) {
        return;
    }
    char *self_name = cbm_node_text(ctx->arena, self_node, ctx->source);
    if (!self_name || !self_name[0]) {
        return;
    }
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx, CBM_SZ_32);
    ts_nstack_push(&stack, body);
    while (stack.count > 0) {
        TSNode n = ts_nstack_pop(&stack);
        const char *k = ts_node_type(n);
        if (strcmp(k, "function_definition") == 0 || strcmp(k, "class_definition") == 0 ||
            strcmp(k, "lambda") == 0) {
            continue;
        }
        TSNode assign = py_statement_assignment(n);
        if (!ts_node_is_null(assign)) {
            py_init_field_assignment(ctx, class_qn, assign, params, self_name);
            continue;
        }
        /* Reverse push keeps source order on pop. */
        for (uint32_t i = ts_node_named_child_count(n); i > 0; i--) {
            ts_nstack_push(&stack, ts_node_named_child(n, i - 1));
        }
    }
}

static void extract_py_field_types(CBMExtractCtx *ctx, TSNode class_node, const char *class_qn) {
    TSNode body = ts_node_child_by_field_name(class_node, TS_FIELD("body"));
    if (ts_node_is_null(body)) {
        return;
    }
    uint32_t count = ts_node_named_child_count(body);
    /* Class-body annotations first: `x: T` / `x: T = v`. */
    for (uint32_t i = 0; i < count; i++) {
        TSNode assign = py_statement_assignment(ts_node_named_child(body, i));
        if (ts_node_is_null(assign)) {
            continue;
        }
        TSNode left = ts_node_child_by_field_name(assign, TS_FIELD("left"));
        TSNode ann = ts_node_child_by_field_name(assign, TS_FIELD("type"));
        if (!ts_node_is_null(left) && !ts_node_is_null(ann) &&
            strcmp(ts_node_type(left), "identifier") == 0) {
            py_push_field_type(ctx, class_qn, left, cbm_node_text(ctx->arena, ann, ctx->source));
        }
    }
    /* Then __init__. */
    for (uint32_t i = 0; i < count; i++) {
        TSNode fn = ts_node_named_child(body, i);
        if (strcmp(ts_node_type(fn), "decorated_definition") == 0) {
            fn = ts_node_child_by_field_name(fn, TS_FIELD("definition"));
        }
        if (ts_node_is_null(fn) || strcmp(ts_node_type(fn), "function_definition") != 0) {
            continue;
        }
        TSNode fname = ts_node_child_by_field_name(fn, TS_FIELD("name"));
        char *fn_name =
            ts_node_is_null(fname) ? NULL : cbm_node_text(ctx->arena, fname, ctx->source);
        if (fn_name && strcmp(fn_name, "__init__") == 0) {
            py_extract_init_fields(ctx, class_qn, fn);
        }
    }
}

/* Name of a C# namespace declaration node (block or file-scoped), or NULL. */
static const char *cs_namespace_decl_name(CBMArena *a, TSNode ns, const char *source) {
    TSNode nm = ts_node_child_by_field_name(ns, TS_FIELD("name"));
    if (ts_node_is_null(nm)) {
        return NULL;
    }
    const char *text = cbm_node_text(a, nm, source);
    return text && text[0] ? text : NULL;
}

/* Declared namespace of a C# type declaration: every enclosing namespace
 * block (outer to inner) under an optional file-scoped namespace, which the
 * grammar may attach either as an ancestor or as a preceding sibling of the
 * compilation unit's members. NULL = global namespace. The file-level
 * namespace_name keeps only the FIRST namespace of a file, which mislabels
 * every type of a multi-namespace file (#2120). */
static const char *cs_type_decl_namespace(CBMArena *a, TSNode node, const char *source) {
    const char *acc = NULL;
    bool file_scoped = false;
    TSNode top = node;
    for (TSNode cur = ts_node_parent(node); !ts_node_is_null(cur); cur = ts_node_parent(cur)) {
        const char *k = ts_node_type(cur);
        bool is_file_scoped = strcmp(k, "file_scoped_namespace_declaration") == 0;
        if (is_file_scoped || strcmp(k, "namespace_declaration") == 0) {
            const char *name = cs_namespace_decl_name(a, cur, source);
            if (name) {
                acc = acc ? cbm_arena_sprintf(a, "%s.%s", name, acc) : name;
            }
            file_scoped = file_scoped || is_file_scoped;
        }
        top = cur;
    }
    if (file_scoped) {
        return acc;
    }
    uint32_t start = ts_node_start_byte(node);
    uint32_t n = ts_node_named_child_count(top);
    for (uint32_t i = 0; i < n; i++) {
        TSNode c = ts_node_named_child(top, i);
        if (ts_node_start_byte(c) >= start) {
            break;
        }
        if (strcmp(ts_node_type(c), "file_scoped_namespace_declaration") == 0) {
            const char *name = cs_namespace_decl_name(a, c, source);
            if (name) {
                return acc ? cbm_arena_sprintf(a, "%s.%s", name, acc) : name;
            }
        }
    }
    return acc;
}

static void extract_class_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    CBMArena *a = ctx->arena;
    const char *kind = ts_node_type(node);

    if (extract_config_class_def(ctx, node, kind)) {
        return;
    }
    if (extract_sql_ddl_class_def(ctx, node, kind)) {
        return;
    }
    if (is_c_tag_reference(ctx->language, node, kind)) {
        return;
    }
    /* A C/C++ typedef carries no `name` field (the alias names live in its
     * declarators), so the generic path below always dropped it. */
    if (is_c_declarator_lang(ctx->language) && strcmp(kind, "type_definition") == 0) {
        extract_c_typedef(ctx, node, spec);
        return;
    }
    /* An anonymous C-family enum (`enum { A, B };`, also as a variable's or
     * member's type) has no Enum def to mint, but its enumerators are names of
     * the enclosing scope all the same. Inside a typedef, extract_c_typedef
     * names the enum and emits them. */
    if (c_enum_lang(ctx->language) && strcmp(kind, "enum_specifier") == 0 &&
        ts_node_is_null(ts_node_child_by_field_name(node, TS_FIELD("name")))) {
        if (!doc_kind_is(doc_parent(ctx, node), "type_definition")) {
            extract_enum_members(ctx, node, NULL);
        }
        return;
    }

    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    // ObjC: class name is first identifier child
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_OBJC) {
        name_node = cbm_find_child_by_kind(node, "identifier");
    }
    // ObjectScript UDL: class name is a `class_name` child (no "name" field).
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_OBJECTSCRIPT_UDL) {
        name_node = cbm_find_child_by_kind(node, "class_name");
    }
    // Swift and newer tree-sitter-kotlin: class/object name is a type_identifier
    // child (no "name" field).
    if (ts_node_is_null(name_node) &&
        (ctx->language == CBM_LANG_SWIFT || ctx->language == CBM_LANG_KOTLIN)) {
        name_node = cbm_find_child_by_kind(node, "type_identifier");
    }
    // Protobuf: service_name / message_name / enum_name children
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_PROTOBUF) {
        name_node = cbm_find_child_by_kind(node, "service_name");
        if (ts_node_is_null(name_node)) {
            name_node = cbm_find_child_by_kind(node, "message_name");
        }
        if (ts_node_is_null(name_node)) {
            name_node = cbm_find_child_by_kind(node, "enum_name");
        }
    }
    // Thrift / Smithy / Pony / PKL (no `name` field): class-type defs carry the
    // name on a plain `identifier` child (PKL `clazz` -> `(identifier) (classBody)`).
    if (ts_node_is_null(name_node) &&
        (ctx->language == CBM_LANG_THRIFT || ctx->language == CBM_LANG_SMITHY ||
         ctx->language == CBM_LANG_PONY || ctx->language == CBM_LANG_PKL)) {
        name_node = cbm_find_child_by_kind(node, "identifier");
    }
    // F#: type_definition wraps an `anon_type_defn` (or similar) whose
    // `type_name` child carries the type name on its own `type_name` field.
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_FSHARP) {
        TSNode tn = find_first_descendant_by_kind(node, "type_name", CBM_DESCENDANT_MAX_DEPTH);
        if (!ts_node_is_null(tn)) {
            TSNode id = ts_node_child_by_field_name(tn, "type_name", 9);
            if (ts_node_is_null(id)) {
                id = cbm_find_child_by_kind(tn, "identifier");
            }
            if (!ts_node_is_null(id)) {
                name_node = id;
            }
        }
    }
    // D: class/struct/interface/union/enum _declaration nodes carry the name on
    // a plain `identifier` child (no `name` field).
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_DLANG) {
        name_node = cbm_find_child_by_kind(node, "identifier");
    }
    // PowerShell: class_statement / enum_statement have no `name` field; the
    // name is the FIRST `simple_name` child (`class Dog : Animal { ... }`).
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_POWERSHELL) {
        name_node = cbm_find_child_by_kind(node, "simple_name");
    }
    // Pascal: a class/interface body (declClass/declIntf) has no name of its
    // own; the type name is on the enclosing `declType`'s `name` field
    // (`TFoo = class ... end`).
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_PASCAL) {
        TSNode parent = ts_node_parent(node);
        if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "declType") == 0) {
            name_node = ts_node_child_by_field_name(parent, TS_FIELD("name"));
        }
    }
    // Julia (no `name` field): struct_definition / abstract_definition carry a
    // `type_head` child whose name is either a plain identifier (`struct Foo`)
    // or the LHS of a `<:` binary_expression (`struct Foo <: Bar`).
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_JULIA) {
        TSNode th = cbm_find_child_by_kind(node, "type_head");
        if (!ts_node_is_null(th)) {
            TSNode inner = ts_node_named_child_count(th) > 0 ? ts_node_named_child(th, 0) : th;
            if (!ts_node_is_null(inner) && strcmp(ts_node_type(inner), "binary_expression") == 0 &&
                ts_node_named_child_count(inner) > 0) {
                name_node = ts_node_named_child(inner, 0); /* LHS of `<:` */
            } else if (!ts_node_is_null(inner) && strcmp(ts_node_type(inner), "identifier") == 0) {
                name_node = inner;
            } else {
                name_node = cbm_find_child_by_kind(th, "identifier");
            }
        }
    }
    // Cap'n Proto (FIELD_COUNT 0): struct/interface name is type_identifier, enum
    // name is enum_identifier (aliased identifier children).
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_CAPNP) {
        if (strcmp(kind, "enum") == 0) {
            name_node = cbm_find_child_by_kind(node, "enum_identifier");
        } else {
            name_node = cbm_find_child_by_kind(node, "type_identifier");
        }
    }
    // Agda (FIELD_COUNT 0): data > data_name, record > record_name (direct child).
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_AGDA) {
        if (strcmp(kind, "data") == 0) {
            name_node = cbm_find_child_by_kind(node, "data_name");
        } else if (strcmp(kind, "record") == 0) {
            name_node = cbm_find_child_by_kind(node, "record_name");
        }
    }
    // GraphQL / Prisma (FIELD_COUNT 0): the type name is a plain direct `name`
    // (GraphQL) or `identifier` (Prisma) child of the type-definition node.
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_GRAPHQL) {
        name_node = cbm_find_child_by_kind(node, "name");
    }
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_PRISMA) {
        name_node = cbm_find_child_by_kind(node, "identifier");
    }
    // Puppet: class_definition / type_declaration name is a plain identifier or
    // class_identifier child (no `name` field).
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_PUPPET) {
        name_node = cbm_find_child_by_kind(node, "identifier");
        if (ts_node_is_null(name_node)) {
            name_node = cbm_find_child_by_kind(node, "class_identifier");
        }
    }
    // Smali (no `name` field): class_definition > class_directive > class_identifier.
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_SMALI) {
        TSNode dir = cbm_find_child_by_kind(node, "class_directive");
        if (!ts_node_is_null(dir)) {
            name_node = cbm_find_child_by_kind(dir, "class_identifier");
        }
    }
    // VHDL: name lives on a declaration-keyword-named field whose value is an
    // `identifier`. Map node kind -> field name.
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_VHDL) {
        if (strcmp(kind, "entity_declaration") == 0) {
            name_node = ts_node_child_by_field_name(node, TS_FIELD("entity"));
        } else if (strcmp(kind, "architecture_definition") == 0) {
            name_node = ts_node_child_by_field_name(node, TS_FIELD("architecture"));
        } else if (strcmp(kind, "package_declaration") == 0) {
            name_node = ts_node_child_by_field_name(node, TS_FIELD("package"));
        } else if (strcmp(kind, "component_declaration") == 0) {
            name_node = ts_node_child_by_field_name(node, TS_FIELD("component"));
        } else if (strcmp(kind, "type_declaration") == 0) {
            name_node = ts_node_child_by_field_name(node, TS_FIELD("type"));
        }
    }
    // PL/SQL: package / type / trigger names use dedicated fields, not `name`.
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_PLSQL) {
        name_node = ts_node_child_by_field_name(node, TS_FIELD("package_name"));
        if (ts_node_is_null(name_node)) {
            name_node = ts_node_child_by_field_name(node, TS_FIELD("type_name"));
        }
        if (ts_node_is_null(name_node)) {
            name_node = ts_node_child_by_field_name(node, TS_FIELD("trigger_name"));
        }
    }
    // Verilog/SystemVerilog (FIELD_COUNT 0): module/class/interface/package use a
    // nested simple_identifier (first descendant); type_declaration must use the
    // DIRECT-child simple_identifier (member/enum idents precede the typedef name).
    if (ts_node_is_null(name_node) &&
        (ctx->language == CBM_LANG_VERILOG || ctx->language == CBM_LANG_SYSTEMVERILOG)) {
        if (strcmp(kind, "type_declaration") == 0) {
            name_node = cbm_find_child_by_kind(node, "simple_identifier");
        } else if (strcmp(kind, "module_declaration") == 0 ||
                   strcmp(kind, "class_declaration") == 0 ||
                   strcmp(kind, "interface_declaration") == 0 ||
                   strcmp(kind, "program_declaration") == 0 ||
                   strcmp(kind, "package_declaration") == 0) {
            name_node =
                find_first_descendant_by_kind(node, "simple_identifier", CBM_DESCENDANT_MAX_DEPTH);
        }
    }
    // Grammar-only languages whose class/struct/type node carries the name on a
    // plain identifier child (no `name` field) or nested one level under a
    // type-binding wrapper. Class 16 fix: extract_class_def is reached (dispatch
    // works) but name resolution fell through, so 0 nodes were emitted.
    if (ts_node_is_null(name_node)) {
        switch (ctx->language) {
        case CBM_LANG_SQUIRREL: // class_declaration > identifier
        case CBM_LANG_DLANG:    // class/struct/interface_declaration > identifier
        case CBM_LANG_HARE:     // type_declaration > identifier
        case CBM_LANG_ODIN:     // struct_declaration > identifier
        case CBM_LANG_BICEP:    // resource/module_declaration > identifier
            name_node = cbm_find_child_by_kind(node, "identifier");
            break;
        case CBM_LANG_POWERSHELL: // class_statement > simple_name
            name_node = cbm_find_child_by_kind(node, "simple_name");
            break;
        case CBM_LANG_SWAY: // impl_item: name is the implemented type (field `type`)
            if (strcmp(kind, "impl_item") == 0) {
                name_node = ts_node_child_by_field_name(node, TS_FIELD("type"));
            }
            break;
        case CBM_LANG_GLEAM: // type_definition > type_name > type_identifier
            name_node =
                find_first_descendant_by_kind(node, "type_identifier", CBM_DESCENDANT_MAX_DEPTH);
            break;
        case CBM_LANG_RESCRIPT: { // type_declaration > type_binding(name=type_identifier)
            TSNode binding = cbm_find_child_by_kind(node, "type_binding");
            if (!ts_node_is_null(binding)) {
                name_node = ts_node_child_by_field_name(binding, TS_FIELD("name"));
                if (ts_node_is_null(name_node)) {
                    name_node = cbm_find_child_by_kind(binding, "type_identifier");
                }
            }
            break;
        }
        case CBM_LANG_FSHARP: { // type_definition > *_type_defn > type_name > identifier
            TSNode tn = find_first_descendant_by_kind(node, "type_name", CBM_DESCENDANT_MAX_DEPTH);
            if (!ts_node_is_null(tn)) {
                name_node = ts_node_child_by_field_name(tn, TS_FIELD("type_name"));
                if (ts_node_is_null(name_node)) {
                    name_node = cbm_find_child_by_kind(tn, "identifier");
                }
            }
            break;
        }
        case CBM_LANG_JULIA: { // struct/abstract_definition > type_head > identifier
            TSNode head = cbm_find_child_by_kind(node, "type_head");
            if (!ts_node_is_null(head)) {
                name_node =
                    find_first_descendant_by_kind(head, "identifier", CBM_DESCENDANT_MAX_DEPTH);
            }
            break;
        }
        case CBM_LANG_TCL: { // namespace > word_list > simple_word ("eval" then name)
            TSNode wl = cbm_find_child_by_kind(node, "word_list");
            if (!ts_node_is_null(wl)) {
                int seen = 0;
                uint32_t wc = ts_node_child_count(wl);
                for (uint32_t i = 0; i < wc; i++) {
                    TSNode w = ts_node_child(wl, i);
                    if (strcmp(ts_node_type(w), "simple_word") == 0) {
                        if (seen == 1) { // skip "eval", take the namespace name
                            name_node = w;
                            break;
                        }
                        seen++;
                    }
                }
            }
            break;
        }
        case CBM_LANG_PASCAL: { // declClass is nested; name lives on the parent declType
            TSNode parent = ts_node_parent(node);
            if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "declType") == 0) {
                name_node = ts_node_child_by_field_name(parent, TS_FIELD("name"));
            }
            break;
        }
        case CBM_LANG_ZIG: { // `const Foo = struct {...}`: struct/enum/union_declaration
                             // is the value of a variable_declaration; the name is the
                             // parent variable_declaration's identifier child.
            TSNode parent = ts_node_parent(node);
            if (!ts_node_is_null(parent) &&
                strcmp(ts_node_type(parent), "variable_declaration") == 0) {
                name_node = cbm_find_child_by_kind(parent, "identifier");
            }
            break;
        }
        /* C/C++ `typedef struct { … } Name;` is named by extract_c_typedef from
         * the type_definition; naming the anonymous specifier here as well
         * emitted Name twice (members twice, a false variant). */
        default:
            break;
        }
    }
    if (ts_node_is_null(name_node)) {
        return;
    }

    char *name = cbm_node_text(a, name_node, ctx->source);
    if (!name || !name[0]) {
        return;
    }
    emit_class_def(ctx, node, spec, kind, name);
}

/* Emit the class-like def for `node` under `name`, plus its members (enum
 * members, methods, fields, class variables). Split from extract_class_def so a
 * C `typedef struct { ... } Name;` can name its anonymous struct after the
 * typedef (extract_c_typedef). */
static void emit_class_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                           const char *kind, char *name) {
    CBMArena *a = ctx->arena;

    // For nested classes, prefix with enclosing class QN (e.g., Outer.Inner).
    // Top-level classes use the language-aware module QN so Java/Go don't double
    // the filename stem (Java `Outer` in Outer.java -> proj.Outer, not
    // proj.Outer.Outer); the nested prefix then yields proj.Outer.Inner.
    const char *class_qn;
    if (ctx->enclosing_class_qn) {
        class_qn = cbm_arena_sprintf(a, "%s.%s", ctx->enclosing_class_qn, name);
    } else {
        class_qn = cbm_fqn_compute_source_lang(a, ctx->project, ctx->rel_path, name, ctx->language);
    }
    const char *label = class_label_for_kind(kind);

    // Sway/WGSL: label struct defs as "Struct" and Sway `abi` blocks as
    // "Interface". Scoped to these grammar-only languages so established
    // struct-as-"Class" labeling (C++/Cap'n Proto …) and the downstream
    // type/IMPLEMENTS resolvers that depend on it are unaffected.
    if (ctx->language == CBM_LANG_SWAY || ctx->language == CBM_LANG_WGSL) {
        if (strcmp(kind, "struct_item") == 0 || strcmp(kind, "struct_declaration") == 0) {
            label = "Struct";
        } else if (strcmp(kind, "abi_item") == 0) {
            label = "Interface";
        }
    }
    // Rust/Swift/D/ArkTS: a struct is a distinct kind from a class — emit the
    // precise "Struct" label rather than collapsing it to "Class". Scoped to
    // these grammar/LSP languages. Rust's struct node is `struct_item`; D's and
    // ArkTS's is `struct_declaration` (an ArkUI @Component custom component).
    // C/C++/Obj-C keep `struct_specifier` → "Class" (a C++ struct is
    // class-like). "Struct" is a type-like container: every type-resolution /
    // registry / IMPLEMENTS / LSP-registrar consumer routes through
    // cbm_label_is_type_like(), so a struct still resolves as a type for its
    // methods, fields, inheritance and impls.
    if (ctx->language == CBM_LANG_RUST || ctx->language == CBM_LANG_SWIFT ||
        ctx->language == CBM_LANG_DLANG || ctx->language == CBM_LANG_ARKTS) {
        if (strcmp(kind, "struct_item") == 0 || strcmp(kind, "struct_declaration") == 0) {
            label = "Struct";
        }
    }
    // Swift: tree-sitter-swift does NOT have a dedicated `struct_declaration`
    // node — `struct`, `class` and `actor` all parse to `class_declaration`,
    // distinguished only by the `declaration_kind` field (the leading keyword
    // token). Read that field and emit "Struct" when the keyword is `struct`
    // (and "Class" for `class`/`actor`, which class_label_for_kind already gives).
    if (ctx->language == CBM_LANG_SWIFT && strcmp(kind, "class_declaration") == 0) {
        TSNode dk = ts_node_child_by_field_name(node, TS_FIELD("declaration_kind"));
        if (!ts_node_is_null(dk)) {
            char *dk_text = cbm_node_text(a, dk, ctx->source);
            if (dk_text && strcmp(dk_text, "struct") == 0) {
                label = "Struct";
            }
        }
    }
    // F#: a `type_definition` that has a primary constructor (`type Foo(...) =`)
    // or an `inherit` clause is an OOP class, not a plain type alias. Label it
    // "Class" so it is registered as a resolvable inheritance target (the graph
    // registry only indexes Function/Method/Class/Interface labels), letting
    // `inherit Base` resolve into an INHERITS edge.
    if (ctx->language == CBM_LANG_FSHARP && strcmp(label, "Type") == 0) {
        if (!ts_node_is_null(find_first_descendant_by_kind(node, "primary_constr_args",
                                                           CBM_DESCENDANT_MAX_DEPTH)) ||
            !ts_node_is_null(find_first_descendant_by_kind(node, "class_inherits_decl",
                                                           CBM_DESCENDANT_MAX_DEPTH))) {
            label = "Class";
        }
    }

    // Go type_spec: check inner type for interface/struct. A Go `type T struct
    // {...}` is a struct → emit the precise "Struct" label (a type-like container;
    // its methods/fields/embedding resolve through cbm_label_is_type_like(), and
    // cbm_pipeline_implements_go() collects Struct nodes too).
    if (strcmp(kind, "type_spec") == 0) {
        TSNode type_inner = ts_node_child_by_field_name(node, TS_FIELD("type"));
        if (!ts_node_is_null(type_inner)) {
            const char *inner_kind = ts_node_type(type_inner);
            if (strcmp(inner_kind, "interface_type") == 0) {
                label = "Interface";
            } else if (strcmp(inner_kind, "struct_type") == 0) {
                label = "Struct";
            }
        }
    }

    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = class_qn;
    def.label = label;
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
    def.is_exported = cbm_is_exported(name, ctx->language);
    if (ctx->language == CBM_LANG_CSHARP && !ctx->enclosing_class_qn) {
        def.decl_namespace = cs_type_decl_namespace(a, node, ctx->source);
    }
    def.base_classes = extract_base_classes(a, node, ctx->source, ctx->language);
    def.decorators = extract_decorators(a, node, ctx->source, ctx->language, spec);
    def.docstring = extract_docstring(ctx, node, name);

    cbm_defs_push(&ctx->result->defs, a, def);

    if (strcmp(label, "Enum") == 0) {
        extract_enum_members(ctx, node, class_qn);
    }

    // Extract methods inside the class
    int methods_from = ctx->result->defs.count;
    extract_class_methods(ctx, node, class_qn, spec);

    // Extract typed struct/class fields (for cross-file LSP type resolution)
    int fields_from = ctx->result->defs.count;
    extract_class_fields(ctx, node, class_qn, spec);
    fence_fields_named_like_methods(ctx, methods_from, fields_from);

    // Extract class-level variables (field declarations not already Fields)
    extract_class_variables(ctx, node, class_qn, spec, fields_from);

    if (ctx->language == CBM_LANG_PYTHON) {
        extract_py_field_types(ctx, node, class_qn);
    }

    // C# 12 primary-constructor parameters: declared on the class line
    // (`class Foo(IBar bar, IBaz baz) : Base { ... }`) and bound to implicit
    // captured fields accessible from any instance member. Tree-sitter c-sharp
    // wraps them inside the hidden _class_declaration_initializer node, so the
    // `parameters` field on class_declaration may not always resolve directly;
    // iterate top-level children for parameter_list as a robust fallback.
    if (ctx->language == CBM_LANG_CSHARP) {
        TSNode primary_params = ts_node_child_by_field_name(node, TS_FIELD("parameters"));
        if (ts_node_is_null(primary_params)) {
            uint32_t total = ts_node_child_count(node);
            for (uint32_t i = 0; i < total; i++) {
                TSNode c = ts_node_child(node, i);
                if (!ts_node_is_null(c) && strcmp(ts_node_type(c), "parameter_list") == 0) {
                    primary_params = c;
                    break;
                }
            }
        }
        if (!ts_node_is_null(primary_params)) {
            uint32_t pcount = ts_node_child_count(primary_params);
            for (uint32_t k = 0; k < pcount; k++) {
                TSNode p = ts_node_child(primary_params, k);
                if (ts_node_is_null(p) || !ts_node_is_named(p)) {
                    continue;
                }
                char *pname = resolve_param_name(a, p, ctx->source);
                if (!pname || !pname[0]) {
                    continue;
                }
                char *ptype = resolve_param_type_text(a, p, ctx->source, ctx->language);
                if (!ptype || !ptype[0]) {
                    continue;
                }
                CBMDefinition pdef;
                memset(&pdef, 0, sizeof(pdef));
                pdef.name = pname;
                pdef.qualified_name = cbm_arena_sprintf(a, "%s.%s", class_qn, pname);
                pdef.label = "Field";
                pdef.file_path = ctx->rel_path;
                pdef.parent_class = class_qn;
                pdef.return_type = ptype;
                pdef.start_line = ts_node_start_point(p).row + TS_LINE_OFFSET;
                pdef.end_line = ts_node_end_point(p).row + TS_LINE_OFFSET;
                pdef.is_exported = false;
                cbm_defs_push(&ctx->result->defs, a, pdef);
            }
        }
    }
}

/* The alias a C typedef declarator introduces: the type_identifier at the end
 * of its pointer / array / function / parenthesized declarator chain
 * (`typedef int (*cmp_fn)(const void *, const void *);` names cmp_fn). The C
 * grammar lexes stdint-style names as primitive_type (`typedef unsigned
 * uint32_t;` in a compat header), so that leaf counts as well. */
static TSNode c_typedef_alias_node(TSNode decl) {
    for (int depth = 0; depth < DECLARATOR_DEPTH_LIMIT && !ts_node_is_null(decl); depth++) {
        const char *dk = ts_node_type(decl);
        if (strcmp(dk, "type_identifier") == 0 || strcmp(dk, "primitive_type") == 0) {
            return decl;
        }
        TSNode inner = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
        if (ts_node_is_null(inner) && ts_node_named_child_count(decl) > 0) {
            inner = ts_node_named_child(decl, 0);
        }
        decl = inner;
    }
    TSNode null_node = {0};
    return null_node;
}

/* C/C++ `typedef`: one def per alias name its declarators introduce.
 *   typedef struct { ... } Name;    -> the anonymous struct IS Name: Class/Enum
 *                                      Name with its fields / enum members
 *   typedef struct Tag { ... } Tag; -> nothing extra: the struct def Tag (the
 *                                      walk reaches the specifier) is the entity
 *   typedef struct Tag Name;        -> Type Name (alias; the struct is a
 *   typedef struct Tag Tag;            reference, see is_c_tag_reference). An
 *   typedef unsigned int Name;         identity alias of an incomplete struct is
 *   typedef int (*Name)(int);          often the only declaration of an opaque
 *                                      handle type in the repo.
 * The type_definition span and the doc comment above it belong to every alias.
 * QN scheme unchanged: <module>.<Name>, or <enclosing class>.<Name> inside a
 * C++ class body. drop_c_typedefs_shadowed_by_tags then removes an alias whose
 * QN a struct/union/enum definition in the same file holds. */
static void extract_c_typedef(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    CBMArena *a = ctx->arena;
    TSNode type = ts_node_child_by_field_name(node, TS_FIELD("type"));
    const char *tk = ts_node_is_null(type) ? "" : ts_node_type(type);
    bool tag_kind = strcmp(tk, "struct_specifier") == 0 || strcmp(tk, "union_specifier") == 0 ||
                    strcmp(tk, "enum_specifier") == 0 || strcmp(tk, "class_specifier") == 0;
    bool has_body =
        tag_kind && !ts_node_is_null(ts_node_child_by_field_name(type, TS_FIELD("body")));
    TSNode tag_node = {0};
    if (tag_kind) {
        tag_node = ts_node_child_by_field_name(type, TS_FIELD("name"));
    }
    /* Only a tag DEFINED here makes a same-named alias redundant. */
    const char *tag =
        (ts_node_is_null(tag_node) || !has_body) ? NULL : cbm_node_text(a, tag_node, ctx->source);
    bool anonymous_body = has_body && ts_node_is_null(tag_node);

    TSTreeCursor cursor = ts_tree_cursor_new(node);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    for (; more; more = ts_tree_cursor_goto_next_sibling(&cursor)) {
        const char *field = ts_tree_cursor_current_field_name(&cursor);
        if (!field || strcmp(field, "declarator") != 0) {
            continue;
        }
        TSNode decl = ts_tree_cursor_current_node(&cursor);
        TSNode alias_node = c_typedef_alias_node(decl);
        char *name = ts_node_is_null(alias_node) ? NULL : cbm_node_text(a, alias_node, ctx->source);
        if (!name || !name[0] || (tag && strcmp(tag, name) == 0)) {
            continue;
        }
        if (anonymous_body && ts_node_eq(decl, alias_node)) {
            /* its doc is the comment above the typedef (doc_anchor_c) */
            emit_class_def(ctx, type, spec, tk, name);
            anonymous_body = false; /* a second plain name is an alias of the first */
            continue;
        }
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name =
            ctx->enclosing_class_qn
                ? cbm_arena_sprintf(a, "%s.%s", ctx->enclosing_class_qn, name)
                : cbm_fqn_compute_source_lang(a, ctx->project, ctx->rel_path, name, ctx->language);
        def.label = "Type";
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
        def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
        def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
        def.is_exported = cbm_is_exported(name, ctx->language);
        def.docstring = extract_docstring(ctx, node, name);
        cbm_defs_push(&ctx->result->defs, a, def);
    }
    ts_tree_cursor_delete(&cursor);
    /* No plain declarator named the anonymous enum (`typedef enum {A, B} *p;`):
     * its enumerators are still names of the enclosing scope. */
    if (anonymous_body && c_enum_lang(ctx->language) && strcmp(tk, "enum_specifier") == 0) {
        extract_enum_members(ctx, type, NULL);
    }
}

/* `typedef struct X X;` plus `struct X { ... };` in one file give the alias and
 * the definition one QN, and the later line would win the graph upsert. The
 * definition (fields, enum members, span) is the entity: drop the alias. Only
 * defs from `first` on (this extraction pass) are considered, so the
 * preprocessed rescue pass never filters the raw pass's defs. */
static void drop_c_typedefs_shadowed_by_tags(CBMExtractCtx *ctx, int first) {
    CBMDefArray *defs = &ctx->result->defs;
    bool any_alias = false;
    for (int i = first; i < defs->count && !any_alias; i++) {
        any_alias = defs->items[i].label && strcmp(defs->items[i].label, "Type") == 0;
    }
    if (!any_alias) {
        return;
    }
    CBMHashTable *tags = cbm_ht_create(CBM_SZ_64);
    if (!tags) {
        return;
    }
    for (int i = first; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (d->label && d->qualified_name &&
            (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Enum") == 0)) {
            cbm_ht_set(tags, d->qualified_name, (void *)d);
        }
    }
    int w = first;
    for (int i = first; i < defs->count; i++) {
        const CBMDefinition *d = &defs->items[i];
        if (d->label && d->qualified_name && strcmp(d->label, "Type") == 0 &&
            cbm_ht_has(tags, d->qualified_name)) {
            continue;
        }
        defs->items[w++] = defs->items[i];
    }
    defs->count = w;
    cbm_ht_free(tags);
}

// Find the body/members node inside a class node
static TSNode find_class_body(TSNode class_node, CBMLanguage lang) {
    // Try field names first
    static const char *body_fields[] = {"body", "members", "class_body", "declaration_list", NULL};
    for (const char **f = body_fields; *f; f++) {
        TSNode body = ts_node_child_by_field_name(class_node, *f, (uint32_t)strlen(*f));
        if (!ts_node_is_null(body)) {
            return body;
        }
    }
    // Go: type_spec -> type field (interface_type or struct_type)
    if (lang == CBM_LANG_GO) {
        TSNode type_inner = ts_node_child_by_field_name(class_node, TS_FIELD("type"));
        if (!ts_node_is_null(type_inner)) {
            return type_inner;
        }
    }
    // ObjC: class_implementation/class_interface has no single body node
    // Methods are inside implementation_definition children directly
    if (lang == CBM_LANG_OBJC) {
        return class_node; // iterate children of the class node itself
    }
    // Squirrel: class_declaration has no body field — member_declaration nodes
    // (each wrapping a function_declaration) are direct children of the class.
    if (lang == CBM_LANG_SQUIRREL) {
        return class_node;
    }
    // Smali: field_definition nodes are direct children of class_definition (no
    // dedicated body node) — iterate the class node itself.
    if (lang == CBM_LANG_SMALI) {
        return class_node;
    }
    // GraphQL: object/interface fields live in a fields_definition child.
    if (lang == CBM_LANG_GRAPHQL) {
        TSNode b = cbm_find_child_by_kind(class_node, "fields_definition");
        if (!ts_node_is_null(b)) {
            return b;
        }
    }
    // Prisma: model columns live in a statement_block child. Gated to Prisma so
    // the common "statement_block" kind can never hijack another language's
    // class body via the generic fallback below.
    if (lang == CBM_LANG_PRISMA) {
        TSNode b = cbm_find_child_by_kind(class_node, "statement_block");
        if (!ts_node_is_null(b)) {
            return b;
        }
    }
    // Fallback: search children for known body node types
    static const char *body_types[] = {"class_body",
                                       "interface_body",
                                       "enum_body",
                                       "protocol_body",
                                       "template_body",
                                       "interface_type",
                                       "struct_type",
                                       "field_declaration_list",
                                       "compound_statement",
                                       "block",
                                       "closure",
                                       "implementation_definition",
                                       NULL};
    uint32_t count = ts_node_child_count(class_node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(class_node, i);
        const char *ck = ts_node_type(child);
        for (const char **t = body_types; *t; t++) {
            if (strcmp(ck, *t) == 0) {
                return child;
            }
        }
    }
    TSNode null_node = {0};
    return null_node;
}

/* Java keeps enum constants directly in enum_body, but declarations that
 * follow the semicolon (methods, fields, and nested types) inside a dedicated
 * enum_body_declarations child.  Keep find_class_body() returning the raw body
 * so enum-member extraction still sees the constants, and normalize only the
 * consumers that walk declaration members. */
static TSNode find_class_member_body(TSNode class_node, CBMLanguage lang) {
    TSNode body = find_class_body(class_node, lang);
    if (ts_node_is_null(body) || lang != CBM_LANG_JAVA ||
        strcmp(ts_node_type(body), "enum_body") != 0) {
        return body;
    }

    TSNode declarations = cbm_find_child_by_kind(body, "enum_body_declarations");
    return ts_node_is_null(declarations) ? body : declarations;
}

/* Go structs keep their field_declaration nodes one level below the body that
 * find_class_body() returns: type_spec's `type` child is a struct_type whose
 * only named child is a field_declaration_list. Interfaces need no such step --
 * interface_type holds its method specs directly, which is why interface members
 * extracted correctly while every struct field was silently skipped. Normalize
 * here, the same way the Java enum_body_declarations step above does. */
static TSNode go_normalize_struct_body(TSNode body) {
    if (ts_node_is_null(body) || strcmp(ts_node_type(body), "struct_type") != 0) {
        return body;
    }
    TSNode list = cbm_find_child_by_kind(body, "field_declaration_list");
    return ts_node_is_null(list) ? body : list;
}

// Dart: resolve method name from method_signature/function_signature.
static TSNode resolve_dart_method_name(TSNode child, const char *ck) {
    if (strcmp(ck, "method_signature") == 0) {
        TSNode func_sig = cbm_find_child_by_kind(child, "function_signature");
        if (!ts_node_is_null(func_sig)) {
            TSNode name_node = func_name_node(func_sig);
            if (!ts_node_is_null(name_node)) {
                return name_node;
            }
            return cbm_find_child_by_kind(func_sig, "identifier");
        }
    }
    if (strcmp(ck, "function_signature") == 0) {
        return cbm_find_child_by_kind(child, "identifier");
    }
    TSNode null_node = {0};
    return null_node;
}

// Arrow function: name on parent variable_declarator/field_definition, or the
// key of an object-literal property — the Zustand "actions returned from a
// factory" pattern, `{ addItem: (...) => {...} }` (#341).
static TSNode resolve_arrow_func_name(TSNode child) {
    TSNode parent = ts_node_parent(child);
    if (!ts_node_is_null(parent)) {
        const char *pk = ts_node_type(parent);
        if (strcmp(pk, "field_definition") == 0) {
            return ts_node_child_by_field_name(parent, TS_FIELD("property"));
        }
        if (strcmp(pk, "public_field_definition") == 0 || strcmp(pk, "variable_declarator") == 0) {
            return ts_node_child_by_field_name(parent, TS_FIELD("name"));
        }
        if (strcmp(pk, "pair") == 0) {
            // Object-literal property `key: () => {...}` → name is the key.
            return ts_node_child_by_field_name(parent, TS_FIELD("key"));
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Try to extract method name from a node, with language-specific fallbacks.
static TSNode resolve_method_name(TSNode child, CBMLanguage lang) {
    TSNode name_node = func_name_node(child);
    if (!ts_node_is_null(name_node)) {
        return name_node;
    }

    const char *ck = ts_node_type(child);

    if ((lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA ||
         lang == CBM_LANG_GLSL) &&
        strcmp(ck, "function_definition") == 0) {
        return cbm_resolve_func_name(child, lang);
    }

    if (lang == CBM_LANG_GROOVY && strcmp(ck, "function_definition") == 0) {
        TSNode fn = ts_node_child_by_field_name(child, TS_FIELD("function"));
        if (!ts_node_is_null(fn)) {
            return fn;
        }
        return cbm_find_child_by_kind(child, "identifier");
    }

    if (lang == CBM_LANG_DART) {
        return resolve_dart_method_name(child, ck);
    }

    if (lang == CBM_LANG_OBJC && strcmp(ck, "method_definition") == 0) {
        return cbm_find_child_by_kind(child, "identifier");
    }

    // Pony: `fun`/`be`/`new` members are `method`/`constructor`/`ffi_method`
    // nodes with no `name` field; the name is the first plain `identifier` child
    // (mirrors the free-function case in cbm_resolve_func_name).
    if (lang == CBM_LANG_PONY && (strcmp(ck, "method") == 0 || strcmp(ck, "constructor") == 0 ||
                                  strcmp(ck, "ffi_method") == 0)) {
        return cbm_find_child_by_kind(child, "identifier");
    }

    if ((lang == CBM_LANG_SWIFT || lang == CBM_LANG_KOTLIN) &&
        (strcmp(ck, "function_declaration") == 0 ||
         strcmp(ck, "protocol_function_declaration") == 0)) {
        return cbm_find_child_by_kind(child, "simple_identifier");
    }

    // Squirrel: function_declaration's name is a plain `identifier` child.
    if (lang == CBM_LANG_SQUIRREL && strcmp(ck, "function_declaration") == 0) {
        return cbm_find_child_by_kind(child, "identifier");
    }

    // ObjectScript method/classmethod: name under method_definition->method_name.
    if (lang == CBM_LANG_OBJECTSCRIPT_UDL &&
        (strcmp(ck, "method") == 0 || strcmp(ck, "classmethod") == 0)) {
        TSNode mdef = cbm_find_child_by_kind(child, "method_definition");
        if (!ts_node_is_null(mdef)) {
            TSNode mname = cbm_find_child_by_kind(mdef, "method_name");
            if (!ts_node_is_null(mname) && ts_node_named_child_count(mname) > 0) {
                return ts_node_named_child(mname, 0);
            }
        }
    }
    // ObjectScript query member.
    if (lang == CBM_LANG_OBJECTSCRIPT_UDL && strcmp(ck, "query") == 0) {
        return cbm_find_child_by_kind(child, "query_name");
    }

    if (strcmp(ck, "arrow_function") == 0) {
        return resolve_arrow_func_name(child);
    }

    TSNode null_node = {0};
    return null_node;
}

// Push a single method definition
static void push_method_def(CBMExtractCtx *ctx, TSNode child, TSNode class_node,
                            const char *class_qn, const CBMLangSpec *spec, TSNode name_node) {
    CBMArena *a = ctx->arena;

    char *name = cbm_func_name_node_text(a, name_node, ctx->source, ctx->language);
    if (!name || !name[0]) {
        return;
    }

    const char *method_qn = cbm_arena_sprintf(a, "%s.%s", class_qn, name);

    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = method_qn;
    def.label = "Method";
    def.file_path = ctx->rel_path;
    def.parent_class = class_qn;
    def.start_line = ts_node_start_point(child).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(child).row + TS_LINE_OFFSET;
    def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
    def.is_exported = cbm_is_exported(name, ctx->language);
    if (ctx->language == CBM_LANG_RUST &&
        strcmp(ts_node_type(child), "function_signature_item") == 0) {
        def.is_abstract = true;
    }

    TSNode params = find_function_params(child, ctx->language);
    if (!ts_node_is_null(params)) {
        def.signature = cbm_node_text(a, params, ctx->source);
        def.param_types = extract_param_types(a, params, ctx->source, ctx->language);
        def.signature_param_types = extract_signature_param_types(
            a, params, ctx->source, ctx->language, true, &def.signature_param_count);
    }

    // Return type (same fields as extract_func_def)
    {
        static const char *rt_fields[] = {"result", "return_type", "type", NULL};
        for (const char **f = rt_fields; *f; f++) {
            TSNode rt = ts_node_child_by_field_name(child, *f, (uint32_t)strlen(*f));
            if (!ts_node_is_null(rt)) {
                def.return_type = c_declared_return_type(ctx, child, rt);
                break;
            }
        }
    }

    // ObjectScript: return type is method_definition -> return_type -> typename.
    if (!def.return_type && (ctx->language == CBM_LANG_OBJECTSCRIPT_UDL ||
                             ctx->language == CBM_LANG_OBJECTSCRIPT_ROUTINE)) {
        TSNode mdef = cbm_find_child_by_kind(child, "method_definition");
        if (ts_node_is_null(mdef)) {
            mdef = child;
        }
        TSNode rt_node = cbm_find_child_by_kind(mdef, "return_type");
        if (!ts_node_is_null(rt_node)) {
            TSNode tname = cbm_find_child_by_kind(rt_node, "typename");
            if (!ts_node_is_null(tname)) {
                def.return_type = cbm_node_text(a, tname, ctx->source);
            }
        }
    }

    // C++: trailing return type (auto method() -> Type)
    if (def.return_type && strcmp(def.return_type, "auto") == 0 &&
        (ctx->language == CBM_LANG_CPP || ctx->language == CBM_LANG_CUDA)) {
        resolve_cpp_trailing_return(a, child, ctx->source, &def);
    }

    def.decorators = extract_decorators(a, child, ctx->source, ctx->language, spec);
    extract_route_from_decorators(a, child, ctx->source, spec, &def.route_path, &def.route_method);
    if (def.route_path && (ctx->language == CBM_LANG_JAVA || ctx->language == CBM_LANG_KOTLIN ||
                           ctx->language == CBM_LANG_SCALA)) {
        const char *prefix = spring_class_route_prefix(a, class_node, ctx->source, spec);
        def.route_path = join_route_paths(a, prefix, def.route_path);
    }
    if (def.route_path && (ctx->language == CBM_LANG_TYPESCRIPT || ctx->language == CBM_LANG_TSX)) {
        /* NestJS: a verb decorator only routes inside a @Controller class. */
        const char *prefix = nest_class_route_prefix(a, class_node, ctx->source);
        def.route_path = prefix ? join_route_paths(a, prefix, def.route_path) : NULL;
        def.route_method = prefix ? def.route_method : NULL;
    }
    def.docstring = extract_docstring(ctx, child, name);

    if (spec->branching_node_types && spec->branching_node_types[0]) {
        set_def_complexity(&def, child, spec);
    }

    // MinHash fingerprint
    compute_fingerprint(ctx, &def, child);

    // A method on a class defined in a test file (e.g. a JUnit/pytest
    // TestFoo.test_bar) is itself a test, same as free functions (#1294).
    def.is_test = def.is_test || ctx->result->is_test_file;

    cbm_defs_push(&ctx->result->defs, a, def);
}

// Extract methods from an ObjC implementation_definition node.
/* Members of a class body, flattened through conditional blocks: a method,
 * field or variable inside #if/#ifdef/#else/#elif (cbm_conditional_block_types:
 * C, C++, CUDA, Objective-C, C#, ...) is still a member of its class, and its
 * #else twin is a variant of the same member (graph_buffer.c, "Definition
 * variants"). One tree cursor: linear in the body's width, no recursion. */
enum { MEMBER_CONDITIONAL_DEPTH_MAX = 16 };

typedef struct {
    TSTreeCursor cursor;
    const char **conditional;
    int depth; /* conditional blocks entered below the body */
    bool started;
    bool named;
} member_iter_t;

static void member_iter_init(member_iter_t *it, TSNode body, CBMLanguage lang, bool named) {
    it->cursor = ts_tree_cursor_new(body);
    it->conditional = cbm_conditional_block_types(lang);
    it->depth = 0;
    it->started = false;
    it->named = named;
}

static void member_iter_done(member_iter_t *it) {
    ts_tree_cursor_delete(&it->cursor);
}

static bool member_iter_advance(member_iter_t *it) {
    if (!it->started) {
        it->started = true;
        return ts_tree_cursor_goto_first_child(&it->cursor);
    }
    while (!ts_tree_cursor_goto_next_sibling(&it->cursor)) {
        if (it->depth == 0) {
            return false;
        }
        ts_tree_cursor_goto_parent(&it->cursor);
        it->depth--;
    }
    return true;
}

static bool member_iter_next(member_iter_t *it, TSNode *out) {
    for (bool have = member_iter_advance(it); have; have = member_iter_advance(it)) {
        TSNode node = ts_tree_cursor_current_node(&it->cursor);
        while (it->conditional && it->depth < MEMBER_CONDITIONAL_DEPTH_MAX &&
               cbm_kind_in_set(node, it->conditional) &&
               ts_tree_cursor_goto_first_child(&it->cursor)) {
            it->depth++;
            node = ts_tree_cursor_current_node(&it->cursor);
        }
        if (it->conditional && cbm_kind_in_set(node, it->conditional)) {
            continue; /* an empty (or too deeply nested) block holds no member */
        }
        if (it->named && !ts_node_is_named(node)) {
            continue;
        }
        *out = node;
        return true;
    }
    return false;
}

/* Objective-C wraps every @implementation member in an implementation_definition,
 * and a #if/#else block inside @implementation wraps each branch's members in
 * another one: walk those nested wrappers too (worklist, no recursion). */
static void extract_objc_impl_methods(CBMExtractCtx *ctx, TSNode impl_node, const char *class_qn,
                                      const CBMLangSpec *spec) {
    TSNode pending[MEMBER_CONDITIONAL_DEPTH_MAX];
    int count = 0;
    pending[count++] = impl_node;
    while (count > 0) {
        TSNode cur = pending[--count];
        member_iter_t it;
        member_iter_init(&it, cur, ctx->language, false);
        TSNode inner;
        while (member_iter_next(&it, &inner)) {
            if (cbm_kind_in_set(inner, spec->function_node_types)) {
                TSNode nm = resolve_method_name(inner, ctx->language);
                if (!ts_node_is_null(nm)) {
                    push_method_def(ctx, inner, impl_node, class_qn, spec, nm);
                }
            } else if (count < MEMBER_CONDITIONAL_DEPTH_MAX &&
                       strcmp(ts_node_type(inner), "implementation_definition") == 0) {
                pending[count++] = inner;
            }
        }
        member_iter_done(&it);
    }
}

// Extract methods inside a class body
static void extract_class_methods(CBMExtractCtx *ctx, TSNode class_node, const char *class_qn,
                                  const CBMLangSpec *spec) {
    TSNode body = find_class_member_body(class_node, ctx->language);
    if (ts_node_is_null(body)) {
        return;
    }

    member_iter_t it;
    member_iter_init(&it, body, ctx->language, false);
    TSNode child;
    while (member_iter_next(&it, &child)) {

        if (ctx->language == CBM_LANG_OBJC &&
            strcmp(ts_node_type(child), "implementation_definition") == 0) {
            extract_objc_impl_methods(ctx, child, class_qn, spec);
            continue;
        }

        // Squirrel wraps each class member in a member_declaration node; the
        // method is the inner function_declaration. Peek through the wrapper.
        if (ctx->language == CBM_LANG_SQUIRREL &&
            strcmp(ts_node_type(child), "member_declaration") == 0) {
            TSNode inner = cbm_find_child_by_kind(child, "function_declaration");
            if (!ts_node_is_null(inner)) {
                child = inner;
            }
        }

        // Python wraps @classmethod / @staticmethod / @property methods in
        // a decorated_definition node. Peek through it to find the inner
        // function_definition so we still emit a Method entry.
        TSNode method_node = child;
        if (strcmp(ts_node_type(child), "decorated_definition") == 0) {
            TSNode def = ts_node_child_by_field_name(child, TS_FIELD("definition"));
            if (ts_node_is_null(def) || !cbm_kind_in_set(def, spec->function_node_types)) {
                continue;
            }
            method_node = def;
        }

        // TS/JS class-field arrow functions: `handleClick = () => {...}` is a
        // public_field_definition whose `value` is an arrow_function (a common
        // React event-handler pattern). It is not in function_node_types, so it
        // would otherwise be dropped. Peek through to the inner arrow and take
        // the method name from the field's `name` child (#new_ts_class_field_arrow).
        if (strcmp(ts_node_type(child), "public_field_definition") == 0) {
            TSNode value = ts_node_child_by_field_name(child, TS_FIELD("value"));
            if (ts_node_is_null(value) || !cbm_kind_in_set(value, spec->function_node_types)) {
                continue;
            }
            TSNode fname = ts_node_child_by_field_name(child, TS_FIELD("name"));
            if (ts_node_is_null(fname)) {
                continue;
            }
            push_method_def(ctx, value, class_node, class_qn, spec, fname);
            continue;
        }

        // ObjectScript UDL wraps each method/classmethod in a class_statement.
        if (ctx->language == CBM_LANG_OBJECTSCRIPT_UDL &&
            strcmp(ts_node_type(child), "class_statement") == 0) {
            if (ts_node_named_child_count(child) == 0) {
                continue;
            }
            TSNode inner = ts_node_named_child(child, 0);
            if (!cbm_kind_in_set(inner, spec->function_node_types)) {
                continue;
            }
            method_node = inner;
        }

        if (!cbm_kind_in_set(method_node, spec->function_node_types)) {
            continue;
        }

        /* Kotlin secondary constructors have no source-level name node. They
         * are nevertheless concrete callable declarations, so materialize one
         * under the owning class using the class name (`Explicit.Explicit`).
         * Classes with only an implicit constructor do not enter this branch
         * and therefore do not gain a fabricated callable target. */
        if (ctx->language == CBM_LANG_KOTLIN &&
            strcmp(ts_node_type(method_node), "secondary_constructor") == 0) {
            TSNode constructor_name = ts_node_child_by_field_name(class_node, TS_FIELD("name"));
            if (ts_node_is_null(constructor_name)) {
                constructor_name = cbm_find_child_by_kind(class_node, "type_identifier");
            }
            if (!ts_node_is_null(constructor_name)) {
                push_method_def(ctx, method_node, class_node, class_qn, spec, constructor_name);
            }
            continue;
        }

        TSNode name_node = resolve_method_name(method_node, ctx->language);
        if (ts_node_is_null(name_node)) {
            continue;
        }

        push_method_def(ctx, method_node, class_node, class_qn, spec, name_node);
    }
    member_iter_done(&it);
}

// --- Rust impl block extraction ---

static void extract_rust_impl(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    CBMArena *a = ctx->arena;

    TSNode type_node = ts_node_child_by_field_name(node, TS_FIELD("type"));
    if (ts_node_is_null(type_node)) {
        return;
    }

    char *type_name = cbm_node_text(a, type_node, ctx->source);
    if (!type_name || !type_name[0]) {
        return;
    }
    /* Strip generic args from the implementing type: `Buffer<T>` → `Buffer`,
     * `Wrapper<T>` → `Wrapper`. The struct identity is the base name. */
    {
        char *lt = strchr(type_name, '<');
        if (lt) {
            *lt = '\0';
        }
    }

    const char *type_qn = cbm_fqn_compute(a, ctx->project, ctx->rel_path, type_name);

    // Check for "impl Trait for Struct" pattern
    const char *impl_trait = NULL;
    TSNode trait_node = ts_node_child_by_field_name(node, TS_FIELD("trait"));
    if (!ts_node_is_null(trait_node)) {
        char *trait_name = cbm_node_text(a, trait_node, ctx->source);
        /* Strip generic args from the trait: `From<Feet>` → `From`,
         * `Index<usize>` → `Index`, `AsRef<str>` → `AsRef`. Qualified paths
         * like `io::Write` / `fmt::Display` have no `<` and are preserved. */
        if (trait_name) {
            char *lt = strchr(trait_name, '<');
            if (lt) {
                *lt = '\0';
            }
        }
        if (trait_name && trait_name[0]) {
            CBMImplTrait it = {0};
            it.trait_name = trait_name;
            it.struct_name = type_name;
            it.struct_qn = type_qn;
            cbm_impltrait_push(&ctx->result->impl_traits, a, it);
            impl_trait = trait_name;
        }
    }

    // Extract methods inside impl body
    TSNode body = ts_node_child_by_field_name(node, TS_FIELD("body"));
    if (ts_node_is_null(body)) {
        return;
    }

    uint32_t count = ts_node_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(body, i);
        if (ts_node_is_null(child)) {
            continue;
        }
        if (!cbm_kind_in_set(child, spec->function_node_types)) {
            continue;
        }

        TSNode name_node = func_name_node(child);
        if (ts_node_is_null(name_node)) {
            continue;
        }

        char *name = cbm_node_text(a, name_node, ctx->source);
        if (!name || !name[0]) {
            continue;
        }

        const char *method_qn = cbm_arena_sprintf(a, "%s.%s", type_qn, name);

        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = method_qn;
        def.label = "Method";
        def.file_path = ctx->rel_path;
        def.parent_class = type_qn;
        def.impl_trait = impl_trait;
        def.start_line = ts_node_start_point(child).row + TS_LINE_OFFSET;
        def.end_line = ts_node_end_point(child).row + TS_LINE_OFFSET;
        def.is_exported = cbm_is_exported(name, ctx->language);

        TSNode params = ts_node_child_by_field_name(child, TS_FIELD("parameters"));
        if (!ts_node_is_null(params)) {
            def.signature = cbm_node_text(a, params, ctx->source);
            def.param_types = extract_param_types(a, params, ctx->source, ctx->language);
            def.signature_param_types = extract_signature_param_types(
                a, params, ctx->source, ctx->language, true, &def.signature_param_count);
        }

        if (spec->branching_node_types && spec->branching_node_types[0]) {
            set_def_complexity(&def, child, spec);
        }

        // MinHash fingerprint
        compute_fingerprint(ctx, &def, child);
        def.docstring = extract_docstring(ctx, child, name);

        cbm_defs_push(&ctx->result->defs, a, def);
    }
}

// --- Elixir def/defp/defmodule ---

// Get the "arguments" node for an Elixir call, with fallback to second child.
static TSNode elixir_call_args(TSNode node) {
    TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
    if (ts_node_is_null(args) && ts_node_child_count(node) > SECOND_CHILD_IDX) {
        args = ts_node_child(node, SECOND_CHILD_IDX);
    }
    return args;
}

// Handle Elixir def/defp/defmacro — extract function definition.
static void extract_elixir_func_def(CBMExtractCtx *ctx, TSNode node, const char *macro) {
    CBMArena *a = ctx->arena;
    TSNode args = elixir_call_args(node);
    if (ts_node_is_null(args)) {
        return;
    }

    TSNode first_arg = ts_node_child(args, 0);
    if (ts_node_is_null(first_arg)) {
        return;
    }

    const char *fk = ts_node_type(first_arg);
    char *name = NULL;
    if (strcmp(fk, "call") == 0 && ts_node_child_count(first_arg) > 0) {
        name = cbm_node_text(a, ts_node_child(first_arg, 0), ctx->source);
    } else if (strcmp(fk, "identifier") == 0) {
        name = cbm_node_text(a, first_arg, ctx->source);
    }
    if (!name || !name[0]) {
        return;
    }

    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    def.label = "Function";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.is_exported = (strcmp(macro, "def") == 0 || strcmp(macro, "defmacro") == 0);
    cbm_defs_push(&ctx->result->defs, a, def);
}

// Emit Class definition for an Elixir defmodule node. Returns do_block or null.
static TSNode emit_elixir_module_class(CBMExtractCtx *ctx, TSNode cur) {
    CBMArena *a = ctx->arena;
    TSNode null_node = {0};
    TSNode args = elixir_call_args(cur);
    if (ts_node_is_null(args)) {
        return null_node;
    }
    TSNode name_node = ts_node_child(args, 0);
    if (ts_node_is_null(name_node)) {
        return null_node;
    }
    char *name = cbm_node_text(a, name_node, ctx->source);
    if (!name || !name[0]) {
        return null_node;
    }
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    def.label = "Class";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(cur).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(cur).row + TS_LINE_OFFSET;
    def.is_exported = true;
    cbm_defs_push(&ctx->result->defs, a, def);
    return cbm_find_child_by_kind(cur, "do_block");
}

// Process Elixir call nodes iteratively — handles defmodule/def/defp/defmacro
// without recursion between extract_elixir_call ↔ extract_elixir_module_def.
static void extract_elixir_call(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    (void)spec;
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx, CBM_SZ_64);
    ts_nstack_push(&stack, node);

    while (stack.count > 0) {
        TSNode cur = ts_nstack_pop(&stack);
        CBMArena *a = ctx->arena;

        if (ts_node_child_count(cur) == 0) {
            continue;
        }
        TSNode callee = ts_node_child(cur, 0);
        if (ts_node_is_null(callee)) {
            continue;
        }
        char *macro = cbm_node_text(a, callee, ctx->source);
        if (!macro) {
            continue;
        }

        if (strcmp(macro, "def") == 0 || strcmp(macro, "defp") == 0 ||
            strcmp(macro, "defmacro") == 0) {
            extract_elixir_func_def(ctx, cur, macro);
        } else if (strcmp(macro, "if") == 0 || strcmp(macro, "unless") == 0) {
            /* A compile-time if/unless in a module body defines functions
             * under a condition: its do and else branches hold variants of the
             * same definitions (graph_buffer.c, "Definition variants"). The
             * grammar nests the else_block inside the do_block. */
            TSNode branch = cbm_find_child_by_kind(cur, "do_block");
            for (int pass = 0; pass < 2 && !ts_node_is_null(branch); pass++) {
                uint32_t bc = ts_node_child_count(branch);
                for (int bi = (int)bc - SKIP_CHAR; bi >= 0; bi--) {
                    TSNode bchild = ts_node_child(branch, (uint32_t)bi);
                    if (!ts_node_is_null(bchild) && strcmp(ts_node_type(bchild), "call") == 0) {
                        ts_nstack_push(&stack, bchild);
                    }
                }
                branch = cbm_find_child_by_kind(branch, "else_block");
            }
        } else if (strcmp(macro, "defmodule") == 0) {
            TSNode do_block = emit_elixir_module_class(ctx, cur);
            if (!ts_node_is_null(do_block)) {
                uint32_t dbc = ts_node_child_count(do_block);
                for (int di = (int)dbc - SKIP_CHAR; di >= 0; di--) {
                    TSNode dchild = ts_node_child(do_block, (uint32_t)di);
                    if (!ts_node_is_null(dchild) && strcmp(ts_node_type(dchild), "call") == 0) {
                        ts_nstack_push(&stack, dchild);
                    }
                }
            }
        }
    }
}

// --- Variable extraction ---

// Helper to push a Variable definition
/* `qn_name` is the name as it should appear in the qualified name, which differs
 * from `name` only where a language scopes a variable below the module — Nix,
 * whose binding names are attrpaths (`a.b.c = …` is name `c`, QN suffix `a.b.c`).
 * Pass NULL to use `name` for both. */
static void push_var_def_qn_doc(CBMExtractCtx *ctx, const char *name, const char *qn_name,
                                TSNode node, const char *doc) {
    if (!name || !name[0] || strcmp(name, "_") == 0) {
        return;
    }
    CBMArena *a = ctx->arena;
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    /* Java/Go: directory-based module (package), so a Go package-level var in
     * myapp/db/conn.go is proj.myapp.db.Var, matching its siblings. */
    def.qualified_name = cbm_fqn_compute_source_lang(a, ctx->project, ctx->rel_path,
                                                     qn_name ? qn_name : name, ctx->language);
    def.label = "Variable";
    def.file_path = ctx->rel_path;
    /* Class-body variables record their declaring class (set by
     * extract_class_variables); the QN stays module-level as before, so this
     * is additive metadata that lets cross-file resolution attach the
     * property to its receiver type. */
    def.parent_class = ctx->var_parent_class;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.is_exported = cbm_is_exported(name, ctx->language);
    def.docstring = doc;
    cbm_defs_push(&ctx->result->defs, a, def);
}

static void push_var_def_qn(CBMExtractCtx *ctx, const char *name, const char *qn_name,
                            TSNode node) {
    if (!name || !name[0] || strcmp(name, "_") == 0) {
        return;
    }
    push_var_def_qn_doc(ctx, name, qn_name, node, extract_member_docstring(ctx, node));
}

static void push_var_def(CBMExtractCtx *ctx, const char *name, TSNode node) {
    push_var_def_qn(ctx, name, NULL, node);
}

// Helper: extract name from a declarator chain (C/C++/ObjC)
// declaration > init_declarator > declarator (may be pointer_declarator > identifier)
static const char *extract_c_declarator_name(CBMArena *a, TSNode decl, const char *source) {
    // Try "declarator" field on the declaration
    TSNode declarator = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
    if (ts_node_is_null(declarator)) {
        return NULL;
    }

    // Could be init_declarator wrapping the actual declarator
    const char *dk = ts_node_type(declarator);
    if (strcmp(dk, "init_declarator") == 0) {
        declarator = ts_node_child_by_field_name(declarator, TS_FIELD("declarator"));
        if (ts_node_is_null(declarator)) {
            return NULL;
        }
        dk = ts_node_type(declarator);
    }
    // Unwrap pointer_declarator
    while (strcmp(dk, "pointer_declarator") == 0 || strcmp(dk, "reference_declarator") == 0) {
        declarator = ts_node_child_by_field_name(declarator, TS_FIELD("declarator"));
        if (ts_node_is_null(declarator)) {
            return NULL;
        }
        dk = ts_node_type(declarator);
    }
    if (strcmp(dk, "identifier") == 0) {
        return cbm_node_text(a, declarator, source);
    }
    return NULL;
}

// Helper: extract name from Java/C# field_declaration (declarator > name)
static const char *extract_java_field_name(CBMArena *a, TSNode field, const char *source) {
    TSNode declarator = ts_node_child_by_field_name(field, TS_FIELD("declarator"));
    if (ts_node_is_null(declarator)) {
        // Try iterating children for variable_declarator
        uint32_t n = ts_node_named_child_count(field);
        for (uint32_t i = 0; i < n; i++) {
            TSNode child = ts_node_named_child(field, i);
            if (strcmp(ts_node_type(child), "variable_declarator") == 0) {
                declarator = child;
                break;
            }
        }
    }
    if (ts_node_is_null(declarator)) {
        return NULL;
    }
    TSNode name = ts_node_child_by_field_name(declarator, TS_FIELD("name"));
    if (!ts_node_is_null(name)) {
        return cbm_node_text(a, name, source);
    }
    return NULL;
}

/* ── Variable name extractors by language group ─────────────────── */

// C# variable extraction: handle field_declaration with nested variable_declaration.
static void extract_csharp_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    const char *fname = extract_java_field_name(a, node, ctx->source);
    if (fname) {
        push_var_def(ctx, fname, node);
        return;
    }
    /* All declarators have this field as their documentation anchor. Keep one
     * immutable arena string, while each variable retains its own identity:
     * the doc-link driver takes the references of that one text once, from
     * the first declarator. A text that could not be collected whole is no
     * doc of any of them (doc_lost has marked the file's doc links failed);
     * it is not looked up again per declarator. */
    bool complete = false;
    const char *doc = extract_member_docstring_status(ctx, node, &complete);
    if (!complete) {
        doc = NULL;
    }
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(child), "variable_declaration") != 0) {
            continue;
        }
        uint32_t nc = ts_node_named_child_count(child);
        for (uint32_t j = 0; j < nc; j++) {
            TSNode decl = ts_node_named_child(child, j);
            if (strcmp(ts_node_type(decl), "variable_declarator") == 0) {
                TSNode id = ts_node_child_by_field_name(decl, TS_FIELD("name"));
                if (ts_node_is_null(id)) {
                    id = cbm_find_child_by_kind(decl, "identifier");
                }
                if (!ts_node_is_null(id)) {
                    const char *name = cbm_node_text(a, id, ctx->source);
                    push_var_def_qn_doc(ctx, name, NULL, decl, doc);
                }
            }
        }
    }
}

/* Check if a tree-sitter node type is an enum member declaration. */
static bool is_enum_member_kind(const char *kind) {
    return strcmp(kind, "enum_member_declaration") == 0 || strcmp(kind, "enum_constant") == 0 ||
           strcmp(kind, "enum_member") == 0 || strcmp(kind, "enum_assignment") == 0 ||
           strcmp(kind, "enumerator") == 0;
}

/* How an enum's members are named.
 *
 * The member QN is `<enum QN>.<member>`, with one exception: a C-family
 * UNSCOPED enum (c_enum_lang, not `enum class` / `enum struct`). Its enumerators
 * live in the scope that holds the enum, so their QN is `<scope QN>.<member>`
 * and `class_qn` -- NULL for an anonymous enum -- is not a segment. That scope
 * is the module, or in C++ the enclosing namespace or class; a C or
 * Objective-C struct is none (c_enum_scope_is_class). Every C-family
 * enumerator of a named enum carries `parent_class` = the enum's QN, which is
 * what ties a flattened enumerator to its enum. */
typedef struct {
    const char *class_qn; /* the enum's QN; NULL for an anonymous enum */
    bool c_enum;          /* a C-family enum_specifier */
    bool flat;            /* the member QN leaves the enum's name out */
} enum_member_scope_t;

/* One enum member -> a Variable def. `doc_node` is the node whose leading
 * comments document it: the member itself, or the ERROR that opens its slot
 * in a macro-wrapped C list (see extract_c_enum_members). */
static void push_enum_member(CBMExtractCtx *ctx, TSNode member, TSNode doc_node,
                             const enum_member_scope_t *scope) {
    CBMArena *a = ctx->arena;
    TSNode mname = ts_node_child_by_field_name(member, TS_FIELD("name"));
    if (ts_node_is_null(mname)) {
        mname = cbm_find_child_by_kind(member, "identifier");
    }
    if (ts_node_is_null(mname)) {
        return;
    }
    char *member_name = cbm_node_text(a, mname, ctx->source);
    if (!member_name || !member_name[0]) {
        return;
    }
    CBMDefinition mdef;
    memset(&mdef, 0, sizeof(mdef));
    mdef.name = member_name;
    if (!scope->flat) {
        mdef.qualified_name = cbm_arena_sprintf(a, "%s.%s", scope->class_qn, member_name);
    } else if (ctx->enclosing_class_qn && c_enum_scope_is_class(ctx->language)) {
        mdef.qualified_name = cbm_arena_sprintf(a, "%s.%s", ctx->enclosing_class_qn, member_name);
    } else {
        mdef.qualified_name =
            cbm_fqn_compute_source_lang(a, ctx->project, ctx->rel_path, member_name, ctx->language);
    }
    if (scope->c_enum && scope->class_qn) {
        mdef.parent_class = scope->class_qn;
    }
    mdef.label = "Variable";
    mdef.file_path = ctx->rel_path;
    mdef.start_line = ts_node_start_point(member).row + TS_LINE_OFFSET;
    mdef.end_line = ts_node_end_point(member).row + TS_LINE_OFFSET;
    mdef.docstring = extract_member_docstring(ctx, doc_node);
    cbm_defs_push(&ctx->result->defs, a, mdef);
}

typedef struct {
    int depth;     /* parentheses an ERROR child opened and none closed yet */
    bool open;     /* the current slot has no constant yet */
    bool has_head; /* an ERROR opened the current slot: `head` is that node */
    TSNode head;
} c_enum_slot_t;

/* The parentheses and slot separators an ERROR child of the list carries. */
static void c_enum_slot_scan_error(TSNode err, c_enum_slot_t *slot) {
    if (slot->open && !slot->has_head) {
        slot->head = err;
        slot->has_head = true;
    }
    uint32_t n = ts_node_child_count(err);
    for (uint32_t i = 0; i < n; i++) {
        const char *k = ts_node_type(ts_node_child(err, i));
        if (strcmp(k, "(") == 0) {
            slot->depth++;
        } else if (strcmp(k, ")") == 0) {
            if (slot->depth > 0) {
                slot->depth--;
            }
        } else if (strcmp(k, ",") == 0 && slot->depth == 0) {
            slot->open = true;
            slot->has_head = false;
        }
    }
}

/* A C-family enumerator_list under error recovery. A macro in the list --
 *     CURLOPT(CURLOPT_URL, CURLOPTTYPE_STRINGPOINT, 2),
 *     CURLINFO_SPEED CURL_DEPRECATED(7.55.0, "...") = CURLINFO_DOUBLE + 9,
 * -- is not enumerator grammar: the parser keeps every bare identifier it can
 * as an `enumerator` and wraps the rest in ERROR nodes, so macro arguments and
 * value operands come back as constants that do not exist (and, flattened,
 * would take the plain QN of the macro they really are). The constant is the
 * enumerator that OPENS a slot: the first one after `{` or after a `,` outside
 * parentheses. A macro call that opens the slot hands that role to its first
 * argument (the X-macro form), and the comment above the call documents it.
 * A well-formed list has one enumerator per slot, so nothing changes there. */
static void extract_c_enum_members(CBMExtractCtx *ctx, TSNode body,
                                   const enum_member_scope_t *scope) {
    c_enum_slot_t slot = {.depth = 0, .open = true, .has_head = false, .head = body};
    TSTreeCursor cur = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cur);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, ",") == 0) {
                if (slot.depth == 0) {
                    slot.open = true;
                    slot.has_head = false;
                }
            } else if (strcmp(ck, "ERROR") == 0) {
                c_enum_slot_scan_error(child, &slot);
            } else if (strcmp(ck, "enumerator") == 0 && slot.open) {
                push_enum_member(ctx, child, slot.has_head ? slot.head : child, scope);
                slot.open = false;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
}

/* Extract enum members as Variable nodes (C#, Java, TypeScript, C++). */
static void extract_enum_members(CBMExtractCtx *ctx, TSNode node, const char *class_qn) {
    TSNode body = find_class_body(node, ctx->language);
    if (ts_node_is_null(body)) {
        return;
    }
    enum_member_scope_t scope = {.class_qn = class_qn, .c_enum = false, .flat = false};
    scope.c_enum = c_enum_lang(ctx->language) && strcmp(ts_node_type(node), "enum_specifier") == 0;
    scope.flat = scope.c_enum && (!class_qn || !c_enum_is_scoped(node));
    if (scope.c_enum) {
        extract_c_enum_members(ctx, body, &scope);
        return;
    }
    uint32_t mc = ts_node_named_child_count(body);
    for (uint32_t mi = 0; mi < mc; mi++) {
        TSNode member = ts_node_named_child(body, mi);
        if (is_enum_member_kind(ts_node_type(member))) {
            push_enum_member(ctx, member, member, &scope);
        }
    }
}

/* Resolve the identifier node from a destructure pattern child.
 * pair_pattern → value field; shorthand/identifier → itself; others → first named child. */
static TSNode destructure_ident(TSNode pat_child) {
    const char *pk = ts_node_type(pat_child);
    if (strcmp(pk, "shorthand_property_identifier_pattern") == 0 || strcmp(pk, "identifier") == 0) {
        return pat_child;
    }
    if (strcmp(pk, "pair_pattern") == 0) {
        return ts_node_child_by_field_name(pat_child, TS_FIELD("value"));
    }
    /* rest_pattern, assignment_pattern, etc. — first named child. */
    return ts_node_named_child(pat_child, 0);
}

/* Emit individual Variable nodes for each destructured binding. */
static void extract_destructured_vars(CBMExtractCtx *ctx, TSNode pattern, TSNode decl,
                                      CBMArena *a) {
    uint32_t pc = ts_node_named_child_count(pattern);
    for (uint32_t pi = 0; pi < pc; pi++) {
        TSNode pat_child = ts_node_named_child(pattern, pi);
        TSNode ident = destructure_ident(pat_child);
        if (ts_node_is_null(ident)) {
            continue;
        }
        char *id_text = cbm_node_text(a, ident, ctx->source);
        if (id_text && id_text[0]) {
            push_var_def(ctx, id_text, decl);
        }
    }
}

/* True for `require("...")` call_expressions with a string-literal path — the
 * CommonJS import form that extract_imports records with local_name = the
 * enclosing declarator's identifier. Same string-argument kinds as
 * process_commonjs_require so the two stay in lockstep. */
static bool is_require_import_call(TSNode value, const char *source, CBMArena *a) {
    if (strcmp(ts_node_type(value), "call_expression") != 0) {
        return false;
    }
    TSNode fn = ts_node_child_by_field_name(value, TS_FIELD("function"));
    if (ts_node_is_null(fn) || strcmp(ts_node_type(fn), "identifier") != 0) {
        return false;
    }
    char *fn_name = cbm_node_text(a, fn, source);
    if (!fn_name || strcmp(fn_name, "require") != 0) {
        return false;
    }
    TSNode args = ts_node_child_by_field_name(value, TS_FIELD("arguments"));
    if (ts_node_is_null(args)) {
        return false;
    }
    uint32_t argc = ts_node_named_child_count(args);
    for (uint32_t i = 0; i < argc; i++) {
        const char *ak = ts_node_type(ts_node_named_child(args, i));
        if (strcmp(ak, "string") == 0 || strcmp(ak, "string_literal") == 0 ||
            strcmp(ak, "template_string") == 0) {
            return true;
        }
    }
    return false;
}

/* True when text of `node` equals `want` exactly (no arena allocation). */
static bool js_node_text_is(TSNode node, const char *source, const char *want) {
    if (ts_node_is_null(node)) {
        return false;
    }
    uint32_t start = ts_node_start_byte(node);
    uint32_t end = ts_node_end_byte(node);
    size_t len = strlen(want);
    return end >= start && (size_t)(end - start) == len && memcmp(source + start, want, len) == 0;
}

/* #1916: `axios.create(...)` — the factory of a configured axios instance
 * (the vue-element-admin / RuoYi `request.js` wrapper). Only the literal
 * `axios` receiver is recognised: a look-alike `factory.create(...)` is not
 * an HTTP client and must not turn its binding into one. */
static bool js_is_axios_create_call(TSNode value, const char *source) {
    if (ts_node_is_null(value) || strcmp(ts_node_type(value), "call_expression") != 0) {
        return false;
    }
    TSNode fn = ts_node_child_by_field_name(value, TS_FIELD("function"));
    if (ts_node_is_null(fn) || strcmp(ts_node_type(fn), "member_expression") != 0) {
        return false;
    }
    TSNode obj = ts_node_child_by_field_name(fn, TS_FIELD("object"));
    TSNode prop = ts_node_child_by_field_name(fn, TS_FIELD("property"));
    return !ts_node_is_null(obj) && strcmp(ts_node_type(obj), "identifier") == 0 &&
           js_node_text_is(obj, source, "axios") && js_node_text_is(prop, source, "create");
}

/* The literal `baseURL` of `axios.create({ baseURL: '<lit>' })`, or NULL when
 * the config is absent, not an object literal, or the value is not a plain
 * string literal (process.env.X, a template with substitutions, an escape):
 * an unknown base is never guessed. */
static const char *js_axios_create_base_url(CBMArena *a, TSNode call, const char *source) {
    TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
    if (ts_node_is_null(args) || ts_node_named_child_count(args) == 0) {
        return NULL;
    }
    TSNode cfg = ts_node_named_child(args, 0);
    if (strcmp(ts_node_type(cfg), "object") != 0) {
        return NULL;
    }
    uint32_t n = ts_node_named_child_count(cfg);
    for (uint32_t i = 0; i < n; i++) {
        TSNode pair = ts_node_named_child(cfg, i);
        if (strcmp(ts_node_type(pair), "pair") != 0) {
            continue;
        }
        TSNode key = ts_node_child_by_field_name(pair, TS_FIELD("key"));
        if (!js_node_text_is(key, source, "baseURL") &&
            !js_node_text_is(key, source, "'baseURL'") &&
            !js_node_text_is(key, source, "\"baseURL\"")) {
            continue;
        }
        TSNode val = ts_node_child_by_field_name(pair, TS_FIELD("value"));
        if (ts_node_is_null(val) || strcmp(ts_node_type(val), "string") != 0) {
            return NULL;
        }
        char *text = cbm_node_text(a, val, source);
        size_t len = text ? strlen(text) : 0;
        if (len < PAIR_LEN || strchr(text, '\\') != NULL) {
            return NULL;
        }
        text[len - SKIP_ONE] = '\0';
        return text + SKIP_ONE;
    }
    return NULL;
}

/* Mark `def` as an axios client instance created by `call` (#1916). */
static void js_mark_axios_client(CBMArena *a, CBMDefinition *def, TSNode call, const char *source) {
    def->http_client = "axios";
    def->http_base_url = js_axios_create_base_url(a, call, source);
}

/* #1916: `export default api;` (api an axios instance declared above) or
 * `export default axios.create({...})` makes the MODULE's default export the
 * client — record it on the Module def so a default import can find it. */
static void js_mark_default_export_client(CBMExtractCtx *ctx, int mod_idx) {
    if (mod_idx < 0 || mod_idx >= ctx->result->defs.count) {
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode stmt = ts_tree_cursor_current_node(&cursor);
        if (strcmp(ts_node_type(stmt), "export_statement") != 0) {
            continue;
        }
        TSNode val = ts_node_child_by_field_name(stmt, TS_FIELD("value"));
        if (ts_node_is_null(val)) {
            continue;
        }
        CBMDefinition *mod = &ctx->result->defs.items[mod_idx];
        if (js_is_axios_create_call(val, ctx->source)) {
            js_mark_axios_client(ctx->arena, mod, val, ctx->source);
            continue;
        }
        if (strcmp(ts_node_type(val), "identifier") != 0) {
            continue;
        }
        for (int d = 0; d < ctx->result->defs.count; d++) {
            const CBMDefinition *v = &ctx->result->defs.items[d];
            if (v->http_client && v->label && strcmp(v->label, "Variable") == 0 && v->name &&
                !v->parent_class && js_node_text_is(val, ctx->source, v->name)) {
                mod->http_client = v->http_client;
                mod->http_base_url = v->http_base_url;
                break;
            }
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// JS/TS variable extraction: skip function-assigned declarators.
static void extract_js_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(child), "variable_declarator") != 0) {
            continue;
        }
        bool is_require = false;
        TSNode value = ts_node_child_by_field_name(child, TS_FIELD("value"));
        if (!ts_node_is_null(value)) {
            const char *vk = ts_node_type(value);
            if (strcmp(vk, "arrow_function") == 0 || strcmp(vk, "function_expression") == 0 ||
                strcmp(vk, "generator_function") == 0) {
                continue;
            }
            is_require = is_require_import_call(value, ctx->source, a);
        }
        TSNode vname = ts_node_child_by_field_name(child, TS_FIELD("name"));
        if (!ts_node_is_null(vname)) {
            const char *nk = ts_node_type(vname);
            /* Destructured patterns: emit individual identifiers instead of
             * the raw "{A, B, C}" text as a single Variable node. */
            if (strcmp(nk, "object_pattern") == 0 || strcmp(nk, "array_pattern") == 0) {
                extract_destructured_vars(ctx, vname, child, a);
            } else {
                /* `const foo = require('./foo')` is an import binding, not a
                 * definition — extract_imports records local_name="foo". A
                 * Variable node here shadows call resolution onto the alias
                 * and orphans the imported callee (#871); ESM `import`
                 * bindings emit no Variable either. Destructured requires
                 * keep theirs: the import row only records the module. */
                if (is_require) {
                    continue;
                }
                int before = ctx->result->defs.count;
                push_var_def(ctx, cbm_node_text(a, vname, ctx->source), child);
                if (ctx->result->defs.count > before &&
                    js_is_axios_create_call(value, ctx->source)) {
                    js_mark_axios_client(a, &ctx->result->defs.items[before], value, ctx->source);
                }
            }
        }
    }
}

static void extract_vars_mainstream(CBMExtractCtx *ctx, TSNode node, CBMArena *a,
                                    const char *kind) {
    (void)kind;
    switch (ctx->language) {
    case CBM_LANG_PYTHON: {
        TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
        if (ts_node_is_null(left)) {
            break;
        }
        const char *lt = ts_node_type(left);
        if (strcmp(lt, "identifier") == 0) {
            push_var_def(ctx, cbm_node_text(a, left, ctx->source), node);
        } else if (strcmp(lt, "pattern_list") == 0 || strcmp(lt, "tuple_pattern") == 0 ||
                   strcmp(lt, "list_pattern") == 0) {
            /* Tuple/list unpacking: `x, y = f()` — emit a Variable def for each
             * unpacked identifier on the LHS (#new_py_tuple_unpack). */
            uint32_t ln = ts_node_named_child_count(left);
            for (uint32_t li = 0; li < ln; li++) {
                TSNode part = ts_node_named_child(left, li);
                if (strcmp(ts_node_type(part), "identifier") == 0) {
                    push_var_def(ctx, cbm_node_text(a, part, ctx->source), node);
                }
            }
        }
        break;
    }
    case CBM_LANG_GO: {
        uint32_t n = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < n; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "var_spec") == 0 || strcmp(ck, "const_spec") == 0) {
                TSNode vname = ts_node_child_by_field_name(child, TS_FIELD("name"));
                if (!ts_node_is_null(vname)) {
                    push_var_def(ctx, cbm_node_text(a, vname, ctx->source), child);
                }
            }
        }
        break;
    }
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
    case CBM_LANG_ARKTS:
        extract_js_vars(ctx, node, a);
        break;
    case CBM_LANG_JAVA: {
        const char *fname = extract_java_field_name(a, node, ctx->source);
        if (fname) {
            push_var_def(ctx, fname, node);
        }
        break;
    }
    case CBM_LANG_CSHARP:
        extract_csharp_vars(ctx, node, a);
        break;
    case CBM_LANG_CPP:
    case CBM_LANG_C:
    case CBM_LANG_OBJC: {
        const char *vname = extract_c_declarator_name(a, node, ctx->source);
        if (vname) {
            push_var_def(ctx, vname, node);
        }
        break;
    }
    case CBM_LANG_RUST: {
        TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        }
        break;
    }
    default:
        break;
    }
}

// Lua variable extraction: handle assignment_statement with function-def filtering.
static void extract_lua_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(child), "assignment_statement") != 0) {
            continue;
        }
        TSNode expr_list = cbm_find_child_by_kind(child, "expression_list");
        if (!ts_node_is_null(expr_list) && ts_node_named_child_count(expr_list) > 0) {
            TSNode val = ts_node_named_child(expr_list, 0);
            if (!ts_node_is_null(val) && strcmp(ts_node_type(val), "function_definition") == 0) {
                continue;
            }
        }
        TSNode vars = ts_node_child_by_field_name(child, TS_FIELD("variables"));
        if (ts_node_is_null(vars)) {
            vars = cbm_find_child_by_kind(child, "variable_list");
        }
        if (!ts_node_is_null(vars) && ts_node_named_child_count(vars) > 0) {
            TSNode first = ts_node_named_child(vars, 0);
            if (!ts_node_is_null(first)) {
                push_var_def(ctx, cbm_node_text(a, first, ctx->source), node);
            }
        }
    }
}

// Strip Perl sigil ($, @, %) from name.
static char *strip_perl_sigil(char *name) {
    if (name && (name[0] == '$' || name[0] == '@' || name[0] == '%')) {
        return name + SKIP_CHAR;
    }
    return name;
}

// Check if a node type is a Perl variable type.
static bool is_perl_var_type(const char *ck) {
    return strcmp(ck, "scalar_variable") == 0 || strcmp(ck, "array_variable") == 0 ||
           strcmp(ck, "hash_variable") == 0 || strcmp(ck, "variable_declarator") == 0 ||
           strcmp(ck, "scalar") == 0 || strcmp(ck, "array") == 0 || strcmp(ck, "hash") == 0;
}

// Perl variable extraction: handle direct variable nodes and assignment_expression.
static void extract_perl_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *ck = ts_node_type(child);
        if (is_perl_var_type(ck)) {
            push_var_def(ctx, strip_perl_sigil(cbm_node_text(a, child, ctx->source)), node);
            return;
        }
        if (strcmp(ck, "assignment_expression") != 0) {
            continue;
        }
        TSNode left = ts_node_child_by_field_name(child, TS_FIELD("left"));
        if (ts_node_is_null(left) && ts_node_named_child_count(child) > 0) {
            left = ts_node_named_child(child, 0);
        }
        if (ts_node_is_null(left)) {
            continue;
        }
        if (strcmp(ts_node_type(left), "variable_declaration") == 0) {
            uint32_t lnc = ts_node_named_child_count(left);
            for (uint32_t li = 0; li < lnc; li++) {
                TSNode var_node = ts_node_named_child(left, li);
                if (is_perl_var_type(ts_node_type(var_node))) {
                    left = var_node;
                    break;
                }
            }
        }
        push_var_def(ctx, strip_perl_sigil(cbm_node_text(a, left, ctx->source)), node);
        return;
    }
}

// R variable extraction: skip function-definitions, then extract left/lhs.
static void extract_r_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    uint32_t rnc = ts_node_named_child_count(node);
    for (uint32_t ri = 0; ri < rnc; ri++) {
        TSNode rch = ts_node_named_child(node, ri);
        if (!ts_node_is_null(rch) && strcmp(ts_node_type(rch), "function_definition") == 0) {
            return;
        }
    }
    TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
    if (ts_node_is_null(left)) {
        left = ts_node_child_by_field_name(node, TS_FIELD("lhs"));
    }
    if (ts_node_is_null(left) && ts_node_named_child_count(node) > 0) {
        left = ts_node_named_child(node, 0);
    }
    if (!ts_node_is_null(left)) {
        const char *lk = ts_node_type(left);
        if (strcmp(lk, "identifier") == 0 || strcmp(lk, "constant") == 0 ||
            strcmp(lk, "string") == 0) {
            push_var_def(ctx, cbm_node_text(a, left, ctx->source), node);
        }
    }
}

// PHP variable extraction from expression_statement.
static void extract_php_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a, const char *kind) {
    if (strcmp(kind, "expression_statement") != 0) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t j = 0; j < nc; j++) {
        TSNode inner = ts_node_named_child(node, j);
        if (strcmp(ts_node_type(inner), "assignment_expression") == 0) {
            TSNode left = ts_node_child_by_field_name(inner, TS_FIELD("left"));
            if (!ts_node_is_null(left)) {
                char *name = cbm_node_text(a, left, ctx->source);
                if (name && name[0] == '$') {
                    name++;
                }
                push_var_def(ctx, name, node);
            }
        }
    }
}

static void extract_vars_dynamic(CBMExtractCtx *ctx, TSNode node, CBMArena *a, const char *kind) {
    switch (ctx->language) {
    case CBM_LANG_PHP:
        extract_php_vars(ctx, node, a, kind);
        break;
    case CBM_LANG_LUA:
        extract_lua_vars(ctx, node, a);
        break;
    case CBM_LANG_RUBY: {
        TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
        if (!ts_node_is_null(left)) {
            const char *lk = ts_node_type(left);
            if (strcmp(lk, "identifier") == 0 || strcmp(lk, "constant") == 0) {
                push_var_def(ctx, cbm_node_text(a, left, ctx->source), node);
            }
        }
        break;
    }
    case CBM_LANG_R:
        extract_r_vars(ctx, node, a);
        break;
    case CBM_LANG_PERL:
        extract_perl_vars(ctx, node, a);
        break;
    default:
        break;
    }
}

// Kotlin variable name resolution: name > simple_identifier > identifier > variable_declaration.
static TSNode resolve_kotlin_var_name(TSNode node) {
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (!ts_node_is_null(name_node)) {
        return name_node;
    }
    name_node = cbm_find_child_by_kind(node, "simple_identifier");
    if (!ts_node_is_null(name_node)) {
        return name_node;
    }
    name_node = cbm_find_child_by_kind(node, "identifier");
    if (!ts_node_is_null(name_node)) {
        return name_node;
    }
    TSNode var_decl = cbm_find_child_by_kind(node, "variable_declaration");
    if (!ts_node_is_null(var_decl)) {
        name_node = cbm_find_child_by_kind(var_decl, "simple_identifier");
        if (!ts_node_is_null(name_node)) {
            return name_node;
        }
        return cbm_find_child_by_kind(var_decl, "identifier");
    }
    TSNode null_node = {0};
    return null_node;
}

static void extract_vars_jvm(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    switch (ctx->language) {
    case CBM_LANG_SCALA: {
        TSNode pattern = ts_node_child_by_field_name(node, TS_FIELD("pattern"));
        if (!ts_node_is_null(pattern)) {
            push_var_def(ctx, cbm_node_text(a, pattern, ctx->source), node);
        } else {
            TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
            if (!ts_node_is_null(name_node)) {
                push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
            }
        }
        break;
    }
    case CBM_LANG_KOTLIN: {
        TSNode name_node = resolve_kotlin_var_name(node);
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        }
        break;
    }
    case CBM_LANG_GROOVY: {
        TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
        if (ts_node_is_null(name_node)) {
            const char *cname = extract_c_declarator_name(a, node, ctx->source);
            if (cname) {
                push_var_def(ctx, cname, node);
                break;
            }
            name_node = cbm_find_child_by_kind(node, "identifier");
        }
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        }
        break;
    }
    default:
        break;
    }
}

// Trim leading/trailing whitespace from a name in-place.
static char *trim_whitespace(char *name) {
    if (!name) {
        return name;
    }
    while (*name == ' ' || *name == '\t') {
        name++;
    }
    size_t nlen = strlen(name);
    while (nlen > 0 && (name[nlen - SKIP_CHAR] == ' ' || name[nlen - SKIP_CHAR] == '\t')) {
        name[nlen - SKIP_CHAR] = '\0';
        nlen--;
    }
    return name;
}

// INI variable extraction: find setting_name/name child, with fallback to first child.
static void extract_ini_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "setting_name") == 0 || strcmp(ck, "name") == 0) {
            push_var_def(ctx, trim_whitespace(cbm_node_text(a, child, ctx->source)), node);
            return;
        }
    }
    if (nc > 0) {
        bool found_name = false;
        for (uint32_t i = 0; i < nc; i++) {
            const char *ck = ts_node_type(ts_node_child(node, i));
            if (strcmp(ck, "setting_name") == 0 || strcmp(ck, "name") == 0) {
                found_name = true;
                break;
            }
        }
        if (!found_name) {
            push_var_def(
                ctx, trim_whitespace(cbm_node_text(a, ts_node_child(node, 0), ctx->source)), node);
        }
    }
}

// Find first named child matching one of the given types and push as var def.
static void push_first_matching_child(CBMExtractCtx *ctx, TSNode node, CBMArena *a,
                                      const char **match_types) {
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *ck = ts_node_type(child);
        for (const char **t = match_types; *t; t++) {
            if (strcmp(ck, *t) == 0) {
                push_var_def(ctx, cbm_node_text(a, child, ctx->source), node);
                return;
            }
        }
    }
}

// JSON variable extraction: strip quotes from key.
static void extract_json_var(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    TSNode key_node = ts_node_child_by_field_name(node, TS_FIELD("key"));
    if (ts_node_is_null(key_node)) {
        return;
    }
    char *raw = cbm_node_text(a, key_node, ctx->source);
    if (raw) {
        size_t rlen = strlen(raw);
        if (rlen >= PAIR_CHARS && raw[0] == '"' && raw[rlen - SKIP_CHAR] == '"') {
            raw[rlen - SKIP_CHAR] = '\0';
            raw++;
        }
        push_var_def(ctx, raw, node);
    }
}

// SCSS variable extraction: try property > name > property_name > variable_name.
static void extract_scss_var(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    TSNode prop = ts_node_child_by_field_name(node, TS_FIELD("property"));
    if (ts_node_is_null(prop)) {
        prop = ts_node_child_by_field_name(node, TS_FIELD("name"));
    }
    if (ts_node_is_null(prop)) {
        prop = cbm_find_child_by_kind(node, "property_name");
    }
    if (ts_node_is_null(prop)) {
        prop = cbm_find_child_by_kind(node, "variable_name");
    }
    if (!ts_node_is_null(prop)) {
        push_var_def(ctx, cbm_node_text(a, prop, ctx->source), node);
    }
}

static void extract_vars_config(CBMExtractCtx *ctx, TSNode node, CBMArena *a, const char *kind) {
    switch (ctx->language) {
    case CBM_LANG_YAML: {
        TSNode key = ts_node_child_by_field_name(node, TS_FIELD("key"));
        if (!ts_node_is_null(key)) {
            push_var_def(ctx, cbm_node_text(a, key, ctx->source), node);
        }
        break;
    }
    case CBM_LANG_TOML: {
        char *name = find_toml_key_name(a, node, ctx->source);
        if (name) {
            push_var_def(ctx, name, node);
        }
        break;
    }
    case CBM_LANG_JSON:
        extract_json_var(ctx, node, a);
        break;
    case CBM_LANG_INI:
        extract_ini_vars(ctx, node, a);
        break;
    case CBM_LANG_ERLANG: {
        if (strcmp(kind, "pp_define") == 0 || strcmp(kind, "record_decl") == 0) {
            static const char *erlang_var_types[] = {"atom", "var", "macro_lhs", NULL};
            push_first_matching_child(ctx, node, a, erlang_var_types);
        }
        break;
    }
    case CBM_LANG_SQL: {
        static const char *sql_var_types[] = {"identifier", "object_reference", NULL};
        push_first_matching_child(ctx, node, a, sql_var_types);
        break;
    }
    case CBM_LANG_BASH: {
        TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        } else {
            static const char *bash_var_types[] = {"variable_name", "word", NULL};
            push_first_matching_child(ctx, node, a, bash_var_types);
        }
        break;
    }
    case CBM_LANG_SCSS:
        extract_scss_var(ctx, node, a);
        break;
    default:
        break;
    }
}

/* ── Variable name extraction dispatcher ────────────────────────── */

/* Nix: a module-level `binding` whose value is neither a lambda nor an attribute
 * set. Both of those are already represented — a lambda-valued binding is minted
 * as a Function by the def walk, and an attrset-valued one is a scope
 * (is_namespace_scope_kind) — so minting either again here would double-count it.
 *
 * The name is the attrpath's leaf and the QN carries the whole path, matching how
 * the def walk names functions; `services.nginx.enable = true` is name `enable`,
 * QN proj.file.services.nginx.enable. */
static void extract_vars_nix(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    if (strcmp(ts_node_type(node), "binding") != 0) {
        return;
    }
    TSNode value = ts_node_child_by_field_name(node, TS_FIELD("expression"));
    if (ts_node_is_null(value)) {
        return;
    }
    if (strcmp(ts_node_type(value), "function_expression") == 0) {
        return; /* already a Function */
    }
    if (cbm_nix_binding_is_attrset_scope(node)) {
        return; /* a scope, not a value */
    }
    TSNode attrpath = ts_node_child_by_field_name(node, TS_FIELD("attrpath"));
    TSNode leaf = cbm_nix_attrpath_last_attr(attrpath);
    if (ts_node_is_null(leaf) || cbm_nix_attr_is_interpolated(leaf)) {
        return;
    }
    char *name = cbm_node_text(a, leaf, ctx->source);
    if (!name || !name[0]) {
        return;
    }
    cbm_nix_strip_attr_quotes(name);
    const char *scope = cbm_nix_attrpath_scope(a, attrpath, ctx->source);
    const char *qn_name = scope ? cbm_arena_sprintf(a, "%s.%s", scope, name) : name;
    push_var_def_qn(ctx, name, qn_name, node);
}

/* Mint every direct binding of one Nix binding container. */
static void extract_nix_binding_set(CBMExtractCtx *ctx, TSNode set, const CBMLangSpec *spec) {
    if (ts_node_is_null(set)) {
        return;
    }
    uint32_t n = ts_node_named_child_count(set);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(set, i);
        if (strcmp(ts_node_type(child), "binding") == 0) {
            extract_var_names(ctx, child, spec);
        }
    }
}

/* Walk past a Nix file's header lambda(s) to the container(s) holding its
 * file-scope bindings, minting each. Handles the curried header (`final: prev:`)
 * and both containers of a `let … in { … }` file: the let's own bindings are file
 * scope in the same sense a C++ file-static is, and the returned attrset's are the
 * exported surface. Anything deeper is nested and deliberately skipped. */
static void extract_nix_module_vars(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    TSNode cur = ts_node_named_child_count(root) > 0 ? ts_node_named_child(root, 0) : root;
    /* Descend header lambdas: `{ pkgs, ... }: <body>`, `final: prev: <body>`. */
    for (int hop = 0; hop < NIX_HEADER_HOP_MAX && !ts_node_is_null(cur) &&
                      strcmp(ts_node_type(cur), "function_expression") == 0;
         hop++) {
        cur = ts_node_child_by_field_name(cur, TS_FIELD("body"));
    }
    if (ts_node_is_null(cur)) {
        return;
    }
    if (strcmp(ts_node_type(cur), "let_expression") == 0) {
        extract_nix_binding_set(ctx, cbm_find_child_by_kind(cur, "binding_set"), spec);
        cur = ts_node_child_by_field_name(cur, TS_FIELD("body"));
        if (ts_node_is_null(cur)) {
            return;
        }
    }
    const char *k = ts_node_type(cur);
    if (strcmp(k, "attrset_expression") == 0 || strcmp(k, "rec_attrset_expression") == 0) {
        extract_nix_binding_set(ctx, cbm_find_child_by_kind(cur, "binding_set"), spec);
    }
}

static void extract_var_names(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    (void)spec;
    CBMArena *a = ctx->arena;
    const char *kind = ts_node_type(node);
    if (ctx->language == CBM_LANG_NIX) {
        extract_vars_nix(ctx, node, a);
        return;
    }

    switch (ctx->language) {
    /* Mainstream + C-family + Rust */
    case CBM_LANG_PYTHON:
    case CBM_LANG_GO:
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
    case CBM_LANG_ARKTS:
    case CBM_LANG_JAVA:
    case CBM_LANG_CSHARP:
    case CBM_LANG_CPP:
    case CBM_LANG_C:
    case CBM_LANG_OBJC:
    case CBM_LANG_RUST:
        extract_vars_mainstream(ctx, node, a, kind);
        return;
    /* Dynamic/scripting */
    case CBM_LANG_PHP:
    case CBM_LANG_LUA:
    case CBM_LANG_RUBY:
    case CBM_LANG_R:
    case CBM_LANG_PERL:
        extract_vars_dynamic(ctx, node, a, kind);
        return;
    /* JVM (non-Java) */
    case CBM_LANG_SCALA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_GROOVY:
        extract_vars_jvm(ctx, node, a);
        return;
    /* Config + other */
    case CBM_LANG_YAML:
    case CBM_LANG_TOML:
    case CBM_LANG_JSON:
    case CBM_LANG_INI:
    case CBM_LANG_ERLANG:
    case CBM_LANG_SQL:
    case CBM_LANG_BASH:
    case CBM_LANG_SCSS:
        extract_vars_config(ctx, node, a, kind);
        return;
    /* Dockerfile: `ENV K=V ...` is an env_instruction holding one or more
     * env_pair children, each with a `name` field; `ARG K=V` is an
     * arg_instruction whose name is the first unquoted_string child. The default
     * fallback misses both (no `name` field on the instruction, child is an
     * env_pair rather than a bare identifier). */
    case CBM_LANG_DOCKERFILE:
        if (strcmp(kind, "env_instruction") == 0) {
            uint32_t ec = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < ec; i++) {
                TSNode pair = ts_node_named_child(node, i);
                if (strcmp(ts_node_type(pair), "env_pair") != 0) {
                    continue;
                }
                TSNode nm = ts_node_child_by_field_name(pair, TS_FIELD("name"));
                if (!ts_node_is_null(nm)) {
                    push_var_def(ctx, cbm_node_text(a, nm, ctx->source), pair);
                }
            }
        } else if (strcmp(kind, "arg_instruction") == 0) {
            TSNode nm = ts_node_child_by_field_name(node, TS_FIELD("name"));
            if (ts_node_is_null(nm)) {
                nm = cbm_find_child_by_kind(node, "unquoted_string");
            }
            if (!ts_node_is_null(nm)) {
                push_var_def(ctx, cbm_node_text(a, nm, ctx->source), node);
            }
        }
        return;
    /* .properties: `key=value` is a `property` node whose name is the `key`
     * child (a bare `key` kind, not an identifier or a `name` field), so the
     * default fallback misses it. */
    case CBM_LANG_PROPERTIES:
        if (strcmp(kind, "property") == 0) {
            TSNode key = cbm_find_child_by_kind(node, "key");
            if (!ts_node_is_null(key)) {
                push_var_def(ctx, cbm_node_text(a, key, ctx->source), node);
            }
        }
        return;
    /* go.mod: a `require_directive` wraps one or more `require_spec` children,
     * each `(module_path version)`. Mint one Variable per required module,
     * named by its module_path. The default fallback misses both (no `name`
     * field; child is a require_spec, not a bare identifier). */
    case CBM_LANG_GOMOD:
        if (strcmp(kind, "require_directive") == 0 || strcmp(kind, "replace_directive") == 0) {
            uint32_t rc = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < rc; i++) {
                TSNode req_spec = ts_node_named_child(node, i);
                const char *sk = ts_node_type(req_spec);
                if (strcmp(sk, "require_spec") != 0 && strcmp(sk, "replace_spec") != 0) {
                    continue;
                }
                TSNode mp = cbm_find_child_by_kind(req_spec, "module_path");
                if (!ts_node_is_null(mp)) {
                    push_var_def(ctx, cbm_node_text(a, mp, ctx->source), req_spec);
                }
            }
        }
        return;
    default:
        break;
    }

    /* Default fallback: name field → C-declarator → first identifier */
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (!ts_node_is_null(name_node)) {
        push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        return;
    }
    const char *cname = extract_c_declarator_name(a, node, ctx->source);
    if (cname) {
        push_var_def(ctx, cname, node);
        return;
    }
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(child), "identifier") == 0) {
            push_var_def(ctx, cbm_node_text(a, child, ctx->source), node);
            return;
        }
    }
}

// Iterative variable walker for config languages with nested structure.
// Used by YAML, TOML, INI, JSON.
static void walk_variables_iter(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx, CBM_SZ_256);
    ts_nstack_push(&stack, root);

    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_CHAR; i >= 0; i--) {
            TSNode child = ts_node_child(node, (uint32_t)i);
            if (ts_node_is_null(child)) {
                continue;
            }
            if (cbm_kind_in_set(child, spec->variable_node_types)) {
                /* parent of `child` is `node` (we're iterating its children) —
                 * pass it directly to avoid the O(n) ts_node_parent rescan. */
                if (cbm_is_module_level_p(node, ctx->language)) {
                    extract_var_names(ctx, child, spec);
                }
            }
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "document") == 0 || strcmp(ck, "block_node") == 0 ||
                strcmp(ck, "block_mapping") == 0 || strcmp(ck, "stream") == 0 ||
                strcmp(ck, "table") == 0 || strcmp(ck, "table_array_element") == 0 ||
                strcmp(ck, "section") == 0 || strcmp(ck, "object") == 0 ||
                strcmp(ck, "array") == 0 || strcmp(ck, "pair") == 0 || strcmp(ck, "element") == 0 ||
                strcmp(ck, "content") == 0) {
                ts_nstack_push(&stack, child);
            }
        }
    }
}

// True if the file's basename is values.yaml / values.yml (Helm values, #338).
static bool is_helm_values_file(const char *rel) {
    if (!rel) {
        return false;
    }
    const char *b = strrchr(rel, '/');
    b = b ? b + 1 : rel;
    return strcmp(b, "values.yaml") == 0 || strcmp(b, "values.yml") == 0;
}

// Descend stream -> document -> block_node to a YAML document's top-level
// block_mapping. Returns a null node when the document has none.
static TSNode find_yaml_toplevel_mapping(TSNode root) {
    TSNode bm = {0};
    TSNode cur = root;
    for (int depth = 0; depth < 6 && ts_node_is_null(bm); depth++) {
        uint32_t n = ts_node_child_count(cur);
        TSNode next = {0};
        bool have_next = false;
        for (uint32_t i = 0; i < n; i++) {
            TSNode ch = ts_node_child(cur, i);
            const char *ck = ts_node_type(ch);
            if (strcmp(ck, "block_mapping") == 0) {
                bm = ch;
                break;
            }
            if (!have_next && (strcmp(ck, "document") == 0 || strcmp(ck, "block_node") == 0)) {
                next = ch;
                have_next = true;
            }
        }
        if (!ts_node_is_null(bm) || !have_next) {
            break;
        }
        cur = next;
    }
    return bm;
}

// Descend to a JSON document's top-level object. Returns a null node if absent.
static TSNode find_json_toplevel_object(TSNode root) {
    if (strcmp(ts_node_type(root), "object") == 0) {
        return root;
    }
    TSNode obj = cbm_find_child_by_kind(root, "object");
    if (!ts_node_is_null(obj)) {
        return obj;
    }
    uint32_t n = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < n; i++) {
        TSNode inner = cbm_find_child_by_kind(ts_node_named_child(root, i), "object");
        if (!ts_node_is_null(inner)) {
            return inner;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

/* #519: a config file's own prose sits in a top-level `description` (or one of
 * its usual synonyms) — META.yaml, action.yml, an OpenAPI document,
 * package.json. Nothing indexed it: the VALUE is not a definition, so no node
 * carried it and BM25 could not see it at all. Checked in priority order; the
 * first key present wins. */
static const char *const config_desc_keys[] = {"description", "summary", "purpose", NULL};

// Strip one layer of matching surrounding quotes. Operates on arena text.
static char *strip_surrounding_quotes(char *t) {
    if (!t) {
        return t;
    }
    size_t n = strlen(t);
    if (n >= PAIR_CHARS && (t[0] == '"' || t[0] == '\'') && t[n - SKIP_CHAR] == t[0]) {
        t[n - SKIP_CHAR] = '\0';
        return t + SKIP_CHAR;
    }
    return t;
}

// Normalise a config scalar into an indexable value: drop a YAML block-scalar
// header (`|`/`>` plus its chomping and indent modifiers), collapse whitespace
// to the shared MAX_COMMENT_LEN cap, then unquote.
static const char *config_scalar_value(CBMArena *a, const char *raw) {
    if (!raw) {
        return NULL;
    }
    while (*raw == ' ' || *raw == '\t' || *raw == '\n' || *raw == '\r') {
        raw++;
    }
    if (*raw == '|' || *raw == '>') {
        while (*raw && *raw != '\n') {
            raw++;
        }
    }
    char *v = collapse_prose(a, raw, strlen(raw));
    return v ? strip_surrounding_quotes(v) : NULL;
}

// Value of `pair` when its key is `want`, else NULL. A YAML block_mapping_pair
// and a JSON pair both expose key/value fields, so one reader serves both.
static const char *config_pair_value_if_key(CBMExtractCtx *ctx, TSNode pair, const char *want) {
    TSNode key = ts_node_child_by_field_name(pair, TS_FIELD("key"));
    TSNode val = ts_node_child_by_field_name(pair, TS_FIELD("value"));
    if (ts_node_is_null(key) || ts_node_is_null(val)) {
        return NULL;
    }
    char *kt = cbm_node_text(ctx->arena, key, ctx->source);
    if (!kt || strcmp(strip_surrounding_quotes(kt), want) != 0) {
        return NULL;
    }
    return config_scalar_value(ctx->arena, cbm_node_text(ctx->arena, val, ctx->source));
}

// First non-empty description value among `container`'s direct pairs, scanned
// in config_desc_keys priority order.
static const char *config_container_description(CBMExtractCtx *ctx, TSNode container,
                                                const char *pair_kind) {
    uint32_t n = ts_node_named_child_count(container);
    for (const char *const *k = config_desc_keys; *k; k++) {
        for (uint32_t i = 0; i < n; i++) {
            TSNode pair = ts_node_named_child(container, i);
            if (strcmp(ts_node_type(pair), pair_kind) != 0) {
                continue;
            }
            const char *v = config_pair_value_if_key(ctx, pair, *k);
            if (v && v[0]) {
                return v;
            }
        }
    }
    return NULL;
}

/* #519 entry point: the file-level description a config document declares about
 * itself, promoted onto the Module node by cbm_extract_definitions so
 * nodes_fts.body indexes it and the file is findable by what it SAYS it does,
 * not only by its path. NULL for every other language. */
static const char *extract_config_module_description(CBMExtractCtx *ctx) {
    if (ctx->language == CBM_LANG_YAML) {
        TSNode bm = find_yaml_toplevel_mapping(ctx->root);
        return ts_node_is_null(bm) ? NULL
                                   : config_container_description(ctx, bm, "block_mapping_pair");
    }
    if (ctx->language == CBM_LANG_JSON) {
        TSNode obj = find_json_toplevel_object(ctx->root);
        return ts_node_is_null(obj) ? NULL : config_container_description(ctx, obj, "pair");
    }
    return NULL;
}

// Extract ONLY top-level keys of a YAML document (no leaf explosion). Used for
// Helm values.yaml so each chart's tunables surface as a handful of structured
// Variables instead of one node per nested leaf (#338).
static void extract_yaml_toplevel_keys(CBMExtractCtx *ctx, TSNode root) {
    CBMArena *a = ctx->arena;
    TSNode bm = find_yaml_toplevel_mapping(root);
    if (ts_node_is_null(bm)) {
        return;
    }
    uint32_t n = ts_node_named_child_count(bm);
    for (uint32_t i = 0; i < n; i++) {
        TSNode pair = ts_node_named_child(bm, i);
        if (strcmp(ts_node_type(pair), "block_mapping_pair") != 0) {
            continue;
        }
        TSNode key = ts_node_child_by_field_name(pair, TS_FIELD("key"));
        if (!ts_node_is_null(key)) {
            push_var_def(ctx, cbm_node_text(a, key, ctx->source), pair);
        }
    }
}

static void extract_variables(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    if (!spec->variable_node_types || !spec->variable_node_types[0]) {
        return;
    }

    // Helm values.yaml: only top-level keys, not the per-leaf flood.
    if (ctx->language == CBM_LANG_YAML && is_helm_values_file(ctx->rel_path)) {
        extract_yaml_toplevel_keys(ctx, root);
        return;
    }

    // Config languages with nested structure: use recursive walk
    if (ctx->language == CBM_LANG_YAML || ctx->language == CBM_LANG_TOML ||
        ctx->language == CBM_LANG_INI || ctx->language == CBM_LANG_JSON) {
        walk_variables_iter(ctx, root, spec);
        return;
    }

    /* Nix: the file's top level sits behind its header lambda(s), so the root's
     * only child is a function_expression and the generic loop below would see
     * nothing. Resolve past the header to the binding container(s) that actually
     * constitute file scope, and mint only THEIR direct bindings.
     *
     * That bound is the point. Every Nix binding's parent is a binding_set at any
     * depth, so admitting them all would mint a node per `enable = true` in a
     * NixOS module's settings tree — the per-leaf flood the Helm values.yaml case
     * above exists to avoid. C++ mints file-scope declarations and never locals;
     * this is the same rule applied to a language whose file scope is behind a
     * lambda. */
    if (ctx->language == CBM_LANG_NIX) {
        extract_nix_module_vars(ctx, root, spec);
        return;
    }

    // `root` is the file's root node (the sole caller passes ctx->root), so it
    // is the parent of every top-level child and the module-level check is
    // invariant across the loop — hoist it out (and it's now O(1) via _p).
    // NB: this short-circuit relies on `root` always being the file root; if a
    // future caller passes a non-root node, restore the per-child check.
    if (!cbm_is_module_level_p(root, ctx->language)) {
        return;
    }

    // Iterate top-level children via a cursor (O(n) total) instead of
    // ts_node_child(root, i) per index, which is O(i) each → O(n²) on files
    // with tens of thousands of top-level statements (e.g. generated/fixture
    // files like TS's reallyLargeFile.ts, 583K lines).
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);

            if (cbm_kind_in_set(child, spec->variable_node_types)) {
                extract_var_names(ctx, child, spec);
                continue;
            }

            // Unwrap wrapper nodes: expression_statement, export_statement, statement
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "expression_statement") == 0 || strcmp(ck, "export_statement") == 0 ||
                strcmp(ck, "statement") == 0) {
                // Check inner named children for variable types
                uint32_t nc = ts_node_named_child_count(child);
                for (uint32_t j = 0; j < nc; j++) {
                    TSNode inner = ts_node_named_child(child, j);
                    if (cbm_kind_in_set(inner, spec->variable_node_types)) {
                        extract_var_names(ctx, inner, spec);
                    }
                }
                // Also check if the wrapper itself is a variable type (e.g., PHP
                // expression_statement)
                if (cbm_kind_in_set(child, spec->variable_node_types)) {
                    extract_var_names(ctx, child, spec);
                }
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

// Extract typed struct/class fields for cross-file LSP resolution (C/C++/CUDA/Go/Java/Rust etc.)
// Creates "Field" label definitions with return_type set to the field's type text.
// These are later collected by DefsToLSPDefs to build FieldDefs pipe-separated strings.
// Check if a field_declaration has a function-pointer declarator chain.
static bool is_func_ptr_field(TSNode field) {
    TSNode decl = ts_node_child_by_field_name(field, TS_FIELD("declarator"));
    for (int depth = 0; depth < C_RETURN_WALK_DEPTH && !ts_node_is_null(decl); depth++) {
        if (strcmp(ts_node_type(decl), "function_declarator") == 0) {
            return true;
        }
        TSNode inner = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
        if (ts_node_is_null(inner)) {
            uint32_t nc = ts_node_named_child_count(decl);
            for (uint32_t k = 0; k < nc; k++) {
                inner = ts_node_named_child(decl, k);
                if (!ts_node_is_null(inner)) {
                    break;
                }
            }
        }
        decl = inner;
    }
    return false;
}

/* ── C-family member declarations ───────────────────────────────────
 * `type declarator, declarator;` in a struct/union/class body. Each declarator
 * is one member: its name is the identifier at the bottom of the declarator,
 * its type everything around that identifier. */

/* The name of one member declarator, or a null node when the declarator is not
 * a data member. A member FUNCTION declaration has the same field_declaration
 * + function_declarator shape as a pointer-to-function member; the two differ
 * in what the function_declarator wraps: `(*open)` is a parenthesized pointer
 * declarator, `resize` (or `(resize)`) a name. */
static TSNode c_member_declarator_name(TSNode decl) {
    TSNode null_node = {0};
    bool need_pointer = false;
    for (int depth = 0; depth < C_FIELD_DECL_WALK_DEPTH && !ts_node_is_null(decl); depth++) {
        const char *kind = ts_node_type(decl);
        if (strcmp(kind, "field_identifier") == 0 || strcmp(kind, "identifier") == 0) {
            return need_pointer ? null_node : decl;
        }
        TSNode inner = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
        bool first_child = false;
        if (strcmp(kind, "function_declarator") == 0) {
            if (ts_node_is_null(inner) ||
                strcmp(ts_node_type(inner), "parenthesized_declarator") != 0) {
                return null_node; /* a member function declaration */
            }
            need_pointer = true;
        } else if (strstr(kind, "pointer_declarator") != NULL ||
                   strcmp(kind, "reference_declarator") == 0) {
            need_pointer = false;
        } else if (strcmp(kind, "attributed_declarator") == 0) {
            first_child = true; /* the declarator, then its attributes */
        } else if (strcmp(kind, "array_declarator") != 0 &&
                   strcmp(kind, "parenthesized_declarator") != 0) {
            return null_node; /* operator, destructor, qualified name, ... */
        }
        if (ts_node_is_null(inner)) {
            /* No `declarator` field: a parenthesized declarator holds it last
             * (after an optional calling-convention modifier), a reference
             * declarator as its only named child. */
            uint32_t named = ts_node_named_child_count(decl);
            for (uint32_t k = 0; k < named; k++) {
                TSNode cand = ts_node_named_child(decl, first_child ? k : named - SKIP_ONE - k);
                if (strcmp(ts_node_type(cand), "comment") != 0) {
                    inner = cand;
                    break;
                }
            }
        }
        decl = inner;
    }
    return null_node;
}

/* Type of one member declarator: the canonical prefix a return type gets from
 * c_rt_render (cv-qualifiers, base type, pointer/reference markers), then the
 * declarator that is left with the member name cut out, which is how C spells
 * an abstract declarator: `int [8]`, `char *[4]`, `void (*)(int fd)`.
 * Whitespace runs in that remainder collapse to one space; a `|` in it (the
 * separator of the field list the cross-file registry is fed) drops it. */
static char *c_member_type_text(CBMExtractCtx *ctx, TSNode field, TSNode type_node, TSNode decl,
                                TSNode name_node) {
    CBMArena *a = ctx->arena;
    const char *src = ctx->source;
    TSNode rest = decl;
    c_rt_out_t out = {NULL, 0};
    (void)c_rt_render(&out, field, type_node, decl, src, &rest);
    uint32_t rest_start = ts_node_is_null(rest) ? 0 : ts_node_start_byte(rest);
    uint32_t rest_end = ts_node_is_null(rest) ? 0 : ts_node_end_byte(rest);
    uint32_t cut_start = ts_node_start_byte(name_node);
    uint32_t cut_end = ts_node_end_byte(name_node);
    /* prefix + one separating space + remainder + NUL */
    char *buf = (char *)cbm_arena_alloc(a, out.len + (size_t)(rest_end - rest_start) + PAIR_LEN);
    if (!buf) {
        return cbm_node_text(a, type_node, src);
    }
    out.buf = buf;
    out.len = 0;
    (void)c_rt_render(&out, field, type_node, decl, src, NULL);
    size_t prefix_len = out.len;
    size_t n = prefix_len;
    bool pending_space = n > 0 && buf[n - SKIP_ONE] != '*' && buf[n - SKIP_ONE] != '&';
    bool wrote_rest = false;
    for (uint32_t i = rest_start; i < rest_end; i++) {
        if (i >= cut_start && i < cut_end) {
            continue;
        }
        char c = src[i];
        if (c == '|') {
            n = prefix_len;
            break;
        }
        if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
            pending_space = pending_space || wrote_rest;
            continue;
        }
        if (pending_space) {
            buf[n++] = ' ';
            pending_space = false;
        }
        buf[n++] = c;
        wrote_rest = true;
    }
    buf[n] = '\0';
    return buf;
}

/* True when `field` is a C-family member declaration: a type and at least one
 * declarator. An anonymous struct/union member has no declarator and is left
 * to the caller. */
static bool is_c_member_declaration(CBMExtractCtx *ctx, TSNode field) {
    if (!is_c_declarator_lang(ctx->language) ||
        strcmp(ts_node_type(field), "field_declaration") != 0) {
        return false;
    }
    return !ts_node_is_null(ts_node_child_by_field_name(field, TS_FIELD("type"))) &&
           !ts_node_is_null(ts_node_child_by_field_name(field, TS_FIELD("declarator")));
}

/* A member whose type is a macro invocation (`ql_head(tcache_slow_t)
 * tcache_ql;`). The C grammar reads the type as a macro_type_specifier; the
 * C++ grammar, which every header gets, reads `(tcache_slow_t)` as a
 * parenthesized declarator and leaves the member's real name in an ERROR node
 * behind it. Returns that name, or a null node for any other shape. */
static TSNode c_macro_typed_member_name(TSNode decl) {
    TSNode null_node = {0};
    if (strcmp(ts_node_type(decl), "parenthesized_declarator") != 0) {
        return null_node;
    }
    TSNode err = ts_node_next_sibling(decl);
    if (ts_node_is_null(err) || strcmp(ts_node_type(err), "ERROR") != 0 ||
        ts_node_named_child_count(err) != SKIP_ONE) {
        return null_node;
    }
    TSNode name = ts_node_named_child(err, 0);
    const char *kind = ts_node_type(name);
    if (strcmp(kind, "identifier") != 0 && strcmp(kind, "field_identifier") != 0) {
        return null_node;
    }
    return name;
}

/* One Field per data-member declarator of a C-family member declaration. */
static void extract_c_member_fields(CBMExtractCtx *ctx, TSNode field, const char *class_qn,
                                    const CBMLangSpec *spec) {
    CBMArena *a = ctx->arena;
    TSNode type_node = ts_node_child_by_field_name(field, TS_FIELD("type"));
    uint32_t nc = ts_node_child_count(field);
    for (uint32_t i = 0; i < nc; i++) {
        const char *field_name = ts_node_field_name_for_child(field, i);
        if (!field_name || strcmp(field_name, "declarator") != 0) {
            continue;
        }
        TSNode decl = ts_node_child(field, i);
        char *name = NULL;
        char *type_text = NULL;
        TSNode macro_name = c_macro_typed_member_name(decl);
        if (!ts_node_is_null(macro_name)) {
            /* The type is the macro invocation: the `type` node through the
             * misread declarator. */
            uint32_t start = ts_node_start_byte(type_node);
            uint32_t end = ts_node_end_byte(decl);
            name = cbm_node_text(a, macro_name, ctx->source);
            type_text = end > start ? cbm_arena_strndup(a, ctx->source + start, end - start) : NULL;
        } else {
            TSNode name_node = c_member_declarator_name(decl);
            if (ts_node_is_null(name_node)) {
                continue;
            }
            name = cbm_node_text(a, name_node, ctx->source);
            type_text = c_member_type_text(ctx, field, type_node, decl, name_node);
        }
        if (!name || !name[0] || !type_text || !type_text[0]) {
            continue;
        }
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = cbm_arena_sprintf(a, "%s.%s", class_qn, name);
        def.label = "Field";
        def.file_path = ctx->rel_path;
        def.parent_class = class_qn;
        def.return_type = type_text;
        def.start_line = ts_node_start_point(field).row + TS_LINE_OFFSET;
        def.end_line = ts_node_end_point(field).row + TS_LINE_OFFSET;
        def.is_exported = cbm_is_exported(name, ctx->language);
        def.decorators = extract_decorators(a, field, ctx->source, ctx->language, spec);
        cbm_defs_push(&ctx->result->defs, a, def);
    }
}

// Resolve the name node for a field declaration, unwrapping C pointer/array declarators.
static TSNode resolve_field_name_node(TSNode child) {
    TSNode name_node = ts_node_child_by_field_name(child, TS_FIELD("declarator"));
    if (ts_node_is_null(name_node)) {
        name_node = ts_node_child_by_field_name(child, TS_FIELD("name"));
    }
    if (ts_node_is_null(name_node)) {
        TSNode null_node = {0};
        return null_node;
    }
    const char *nk = ts_node_type(name_node);
    /* Java `private static final String PROP = "v";`: the declarator holds the
     * initializer too, so its text named the Field `PROP = "v"`; its name is
     * the declarator's own name field */
    if (strcmp(nk, "variable_declarator") == 0) {
        TSNode inner = ts_node_child_by_field_name(name_node, TS_FIELD("name"));
        if (!ts_node_is_null(inner)) {
            return inner;
        }
    }
    if (strcmp(nk, "pointer_declarator") == 0 || strcmp(nk, "array_declarator") == 0) {
        TSNode inner = ts_node_child_by_field_name(name_node, TS_FIELD("declarator"));
        if (!ts_node_is_null(inner)) {
            return inner;
        }
        TSNode null_node = {0};
        return null_node;
    }
    return name_node;
}

/* Schema/grammar languages whose field node carries the field name on a plain
 * child (no C-style `declarator`/`type` field), so the generic field path below
 * skips them. Emit a "Field" def (with optional return_type) and return true if
 * handled. GraphQL: field_definition (name)(type:named_type); Prisma:
 * column_declaration (identifier)(column_type); Smali: field_definition
 * (field_identifier)(field_type). */
static bool extract_schema_field(CBMExtractCtx *ctx, TSNode child, const char *class_qn) {
    CBMArena *a = ctx->arena;
    TSNode name_node = {0};
    TSNode type_node = {0};

    if (ctx->language == CBM_LANG_GRAPHQL) {
        name_node = ts_node_child_by_field_name(child, TS_FIELD("name"));
        if (ts_node_is_null(name_node)) {
            name_node = cbm_find_child_by_kind(child, "name");
        }
        type_node = ts_node_child_by_field_name(child, TS_FIELD("type"));
    } else if (ctx->language == CBM_LANG_PRISMA) {
        name_node = cbm_find_child_by_kind(child, "identifier");
        type_node = cbm_find_child_by_kind(child, "column_type");
    } else if (ctx->language == CBM_LANG_SMALI) {
        name_node = cbm_find_child_by_kind(child, "field_identifier");
        type_node = cbm_find_child_by_kind(child, "field_type");
    } else {
        return false;
    }

    if (ts_node_is_null(name_node)) {
        return true; // language matched but no name → nothing to emit
    }
    char *name = cbm_node_text(a, name_node, ctx->source);
    if (!name || !name[0]) {
        return true;
    }

    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = cbm_arena_sprintf(a, "%s.%s", class_qn, name);
    def.label = "Field";
    def.file_path = ctx->rel_path;
    def.parent_class = class_qn;
    if (!ts_node_is_null(type_node)) {
        def.return_type = cbm_node_text(a, type_node, ctx->source);
    }
    def.start_line = ts_node_start_point(child).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(child).row + TS_LINE_OFFSET;
    def.is_exported = cbm_is_exported(name, ctx->language);
    cbm_defs_push(&ctx->result->defs, a, def);
    return true;
}

static void extract_class_fields(CBMExtractCtx *ctx, TSNode class_node, const char *class_qn,
                                 const CBMLangSpec *spec) {
    if (!spec->field_node_types || !spec->field_node_types[0]) {
        return;
    }

    TSNode body = find_class_member_body(class_node, ctx->language);
    if (ctx->language == CBM_LANG_GO) {
        body = go_normalize_struct_body(body);
    }
    if (ts_node_is_null(body)) {
        return;
    }

    CBMArena *a = ctx->arena;
    member_iter_t it;
    member_iter_init(&it, body, ctx->language, true);
    TSNode child;
    while (member_iter_next(&it, &child)) {
        // ObjectScript UDL wraps each member in a class_statement node.
        if (ctx->language == CBM_LANG_OBJECTSCRIPT_UDL &&
            strcmp(ts_node_type(child), "class_statement") == 0 &&
            ts_node_named_child_count(child) > 0) {
            child = ts_node_named_child(child, 0);
        }

        if (!cbm_kind_in_set(child, spec->field_node_types)) {
            continue;
        }

        if (is_c_member_declaration(ctx, child)) {
            extract_c_member_fields(ctx, child, class_qn, spec);
            continue;
        }

        if (is_func_ptr_field(child)) {
            continue;
        }

        /* Schema/grammar languages (GraphQL/Prisma/Smali) carry the field name on
         * a plain child rather than a C-style declarator/type field; handle them
         * up front so the generic "type"-field path below doesn't skip them. */
        if (extract_schema_field(ctx, child, class_qn)) {
            continue;
        }

        // ObjectScript UDL member extraction. property/parameter -> Variable;
        // index/trigger/xdata/storage/foreignkey -> labelled members with
        // storage-XML and trigger-body sidecars.
        if (ctx->language == CBM_LANG_OBJECTSCRIPT_UDL) {
            if (strcmp(ts_node_type(child), "property") == 0 ||
                strcmp(ts_node_type(child), "parameter") == 0) {
                TSNode pname = cbm_find_child_by_kind(child, "property_name");
                if (ts_node_is_null(pname)) {
                    pname = cbm_find_child_by_kind(child, "parameter_name");
                }
                if (!ts_node_is_null(pname) && ts_node_named_child_count(pname) > 0) {
                    TSNode ident = ts_node_named_child(pname, 0);
                    char *pn = cbm_node_text(a, ident, ctx->source);
                    if (pn && pn[0]) {
                        CBMDefinition pdef;
                        memset(&pdef, 0, sizeof(pdef));
                        pdef.name = pn;
                        pdef.qualified_name = cbm_arena_sprintf(a, "%s.%s", class_qn, pn);
                        pdef.label = "Variable";
                        pdef.file_path = ctx->rel_path;
                        pdef.parent_class = class_qn;
                        pdef.start_line = ts_node_start_point(child).row + TS_LINE_OFFSET;
                        pdef.end_line = ts_node_end_point(child).row + TS_LINE_OFFSET;
                        cbm_defs_push(&ctx->result->defs, a, pdef);
                    }
                }
                continue;
            }

            const char *ntype = ts_node_type(child);
            const char *name_child_kind = NULL;
            const char *member_label = NULL;
            if (strcmp(ntype, "index") == 0) {
                name_child_kind = "index_name";
                member_label = "Index";
            } else if (strcmp(ntype, "trigger") == 0) {
                name_child_kind = "trigger_name";
                member_label = "Trigger";
            } else if (strcmp(ntype, "xdata") == 0) {
                name_child_kind = "xdata_name";
                member_label = "XData";
            } else if (strcmp(ntype, "storage") == 0) {
                name_child_kind = "storage_name";
                member_label = "Storage";
            } else if (strcmp(ntype, "foreignkey") == 0) {
                name_child_kind = "foreignkey_name";
                member_label = "Variable";
            }

            if (name_child_kind) {
                TSNode nname = cbm_find_child_by_kind(child, name_child_kind);
                if (!ts_node_is_null(nname)) {
                    char *mn = cbm_node_text(a, nname, ctx->source);
                    if (mn && mn[0]) {
                        CBMDefinition mdef;
                        memset(&mdef, 0, sizeof(mdef));
                        mdef.name = mn;
                        mdef.qualified_name = cbm_arena_sprintf(a, "%s.%s", class_qn, mn);
                        mdef.label = member_label;
                        mdef.file_path = ctx->rel_path;
                        mdef.parent_class = class_qn;
                        mdef.start_line = ts_node_start_point(child).row + TS_LINE_OFFSET;
                        mdef.end_line = ts_node_end_point(child).row + TS_LINE_OFFSET;

                        if (strcmp(member_label, "Storage") == 0) {
                            TSNode sbody = cbm_find_child_by_kind(child, "storage_body");
                            if (!ts_node_is_null(sbody)) {
                                char *xml = cbm_node_text(a, sbody, ctx->source);
                                if (xml) {
                                    char props[CBM_SZ_2K];
                                    int pos = snprintf(props, sizeof(props), "{");
                                    static const struct {
                                        const char *tag;
                                        const char *key;
                                    } kv[] = {{"ExtentSize", "extent_size"},
                                              {"DataLocation", "data_global"},
                                              {"IdLocation", "id_global"},
                                              {"IndexLocation", "index_global"},
                                              {"StreamLocation", "stream_global"},
                                              {"Type", "storage_type"},
                                              {NULL, NULL}};
                                    bool first = true;
                                    for (int ki = 0; kv[ki].tag; ki++) {
                                        char open[64], close[64], buf[256];
                                        snprintf(open, sizeof(open), "<%s>", kv[ki].tag);
                                        snprintf(close, sizeof(close), "</%s>", kv[ki].tag);
                                        const char *s = strstr(xml, open);
                                        if (!s) {
                                            continue;
                                        }
                                        s += strlen(open);
                                        const char *e = strstr(s, close);
                                        if (!e) {
                                            continue;
                                        }
                                        size_t vlen = (size_t)(e - s);
                                        if (vlen >= sizeof(buf)) {
                                            vlen = sizeof(buf) - 1;
                                        }
                                        memcpy(buf, s, vlen);
                                        buf[vlen] = '\0';
                                        char esc[300];
                                        int ei = 0;
                                        for (size_t ci = 0; ci < vlen && ei < (int)sizeof(esc) - 2;
                                             ci++) {
                                            if (buf[ci] == '"' || buf[ci] == '\\') {
                                                esc[ei++] = '\\';
                                            }
                                            esc[ei++] = buf[ci];
                                        }
                                        esc[ei] = '\0';
                                        if (pos < 0 || pos >= (int)sizeof(props) - 1) {
                                            break; // buffer full — stop appending
                                        }
                                        pos += snprintf(props + pos, sizeof(props) - (size_t)pos,
                                                        "%s\"%s\":\"%s\"", first ? "" : ",",
                                                        kv[ki].key, esc);
                                        if (pos >= (int)sizeof(props)) {
                                            pos = (int)sizeof(props) - 1; // truncated
                                        }
                                        first = false;
                                    }
                                    const char *sql_tag = "<Global>";
                                    const char *sql_end = "</Global>";
                                    char sql_map_buf[512];
                                    int smi = 0;
                                    const char *sp = xml;
                                    bool sql_first = true;
                                    while ((sp = strstr(sp, sql_tag)) != NULL) {
                                        sp += strlen(sql_tag);
                                        const char *ep = strstr(sp, sql_end);
                                        if (!ep) {
                                            break;
                                        }
                                        size_t glen = (size_t)(ep - sp);
                                        if (smi + (int)glen + 2 < (int)sizeof(sql_map_buf) - 1) {
                                            if (!sql_first) {
                                                sql_map_buf[smi++] = ' ';
                                            }
                                            memcpy(sql_map_buf + smi, sp, glen);
                                            smi += (int)glen;
                                            sql_first = false;
                                        }
                                        sp = ep + strlen(sql_end);
                                    }
                                    sql_map_buf[smi] = '\0';
                                    if (smi > 0 && pos >= 0 && pos < (int)sizeof(props) - 1) {
                                        pos += snprintf(props + pos, sizeof(props) - (size_t)pos,
                                                        "%s\"sql_map_globals\":\"%s\"",
                                                        first ? "" : ",", sql_map_buf);
                                        if (pos >= (int)sizeof(props)) {
                                            pos = (int)sizeof(props) - 1; // truncated
                                        }
                                        first = false;
                                    }
                                    if (pos < (int)sizeof(props) - 1) {
                                        props[pos++] = '}';
                                        props[pos] = '\0';
                                    }
                                    if (!first) {
                                        mdef.docstring = cbm_arena_strdup(a, props);
                                    }
                                }
                            }
                        }

                        if (strcmp(member_label, "Trigger") == 0) {
                            TSNode tbody = cbm_find_child_by_kind(child, "core_trigger");
                            if (ts_node_is_null(tbody)) {
                                tbody = cbm_find_child_by_kind(child, "external_trigger");
                            }
                            if (!ts_node_is_null(tbody)) {
                                mdef.body_tokens = extract_body_ident_tokens(ctx, tbody);
                                char *raw = cbm_node_text(a, tbody, ctx->source);
                                if (raw && raw[0]) {
                                    char esc[CBM_SZ_512];
                                    int ei = 0;
                                    for (int ci = 0; raw[ci] && ei < (int)sizeof(esc) - 3; ci++) {
                                        if (raw[ci] == '"' || raw[ci] == '\\') {
                                            esc[ei++] = '\\';
                                        } else if (raw[ci] == '\n') {
                                            esc[ei++] = '\\';
                                            esc[ei++] = 'n';
                                            continue;
                                        } else if (raw[ci] == '\r') {
                                            continue;
                                        }
                                        esc[ei++] = raw[ci];
                                    }
                                    esc[ei] = '\0';
                                    char props[CBM_SZ_512];
                                    snprintf(props, sizeof(props), "{\"trigger_body\":\"%s\"}",
                                             esc);
                                    mdef.docstring = cbm_arena_strdup(a, props);
                                }
                            }
                        }

                        cbm_defs_push(&ctx->result->defs, a, mdef);
                    }
                }
                continue;
            }
        }

        /* Locate the field's "type" + name node. Two shapes:
         *   - direct (Java/Go/Rust/C/C++):
         *       field_declaration .type=identifier .declarator=variable_declarator(.name)
         *   - nested (C#):
         *       field_declaration > variable_declaration(.type=identifier,
         *                                               variable_declarator(.name))
         * For the nested case, the child has no "type" field directly. Detect by
         * walking named children for a variable_declaration. */
        TSNode type_node = ts_node_child_by_field_name(child, TS_FIELD("type"));
        TSNode empty_node = {0};
        TSNode name_node = ts_node_is_null(type_node) ? empty_node : resolve_field_name_node(child);

        if (ts_node_is_null(type_node)) {
            uint32_t cnc = ts_node_named_child_count(child);
            for (uint32_t k = 0; k < cnc; k++) {
                TSNode inner = ts_node_named_child(child, k);
                if (strcmp(ts_node_type(inner), "variable_declaration") != 0) {
                    continue;
                }
                type_node = ts_node_child_by_field_name(inner, TS_FIELD("type"));
                /* Find first variable_declarator child for the name. */
                uint32_t nc = ts_node_named_child_count(inner);
                for (uint32_t j = 0; j < nc; j++) {
                    TSNode vd = ts_node_named_child(inner, j);
                    if (strcmp(ts_node_type(vd), "variable_declarator") == 0) {
                        TSNode nm = ts_node_child_by_field_name(vd, TS_FIELD("name"));
                        if (!ts_node_is_null(nm)) {
                            name_node = nm;
                            break;
                        }
                    }
                }
                break;
            }
        }

        if (ts_node_is_null(type_node)) {
            continue;
        }
        char *type_text = cbm_node_text(a, type_node, ctx->source);
        if (!type_text || !type_text[0]) {
            continue;
        }

        if (ts_node_is_null(name_node)) {
            continue;
        }

        char *name = cbm_node_text(a, name_node, ctx->source);
        if (!name || !name[0]) {
            continue;
        }

        /* Go: `_` is the blank identifier, used for explicit struct padding in
         * generated code. It is not a referenceable field, and emitting it gives
         * every `_` in the repository a same-named node to collide with. */
        if (ctx->language == CBM_LANG_GO && strcmp(name, "_") == 0) {
            continue;
        }

        const char *field_qn = cbm_arena_sprintf(a, "%s.%s", class_qn, name);

        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = field_qn;
        def.label = "Field";
        def.file_path = ctx->rel_path;
        def.parent_class = class_qn;
        def.return_type = type_text;
        def.start_line = ts_node_start_point(child).row + TS_LINE_OFFSET;
        def.end_line = ts_node_end_point(child).row + TS_LINE_OFFSET;
        def.is_exported = cbm_is_exported(name, ctx->language);
        /* Field decorators/annotations (ArkTS @State/@Prop/@Link state members,
         * TS @Input()-style fields, JVM field annotations via the modifiers
         * child) — same extraction the class and method paths already run. */
        def.decorators = extract_decorators(a, child, ctx->source, ctx->language, spec);
        def.docstring = extract_member_docstring(ctx, child);

        cbm_defs_push(&ctx->result->defs, a, def);
    }
    member_iter_done(&it);
}

static int cmp_cstr_ptr(const void *pa, const void *pb) {
    return strcmp(*(const char *const *)pa, *(const char *const *)pb);
}

// A field and a method of one class body may share a name (Java `count` and
// `count()`), and the graph keeps one node per qualified name, so one of them
// was lost. The field then takes the qualified name `<Owner>.<name>#field`
// (the `base#suffix` fence of `#macro` and the Rust cfg twins; the registry
// still files it under its plain name). Only a collision in the same body
// renames, so every other field keeps its name. The body's methods are the
// defs [methods_from, fields_from), its fields [fields_from, count); a sorted
// copy of the method names keeps a large class linear-logarithmic.
static void fence_fields_named_like_methods(CBMExtractCtx *ctx, int methods_from, int fields_from) {
    CBMDefinition *defs = ctx->result->defs.items;
    int fields_to = ctx->result->defs.count;
    if (fields_from >= fields_to || methods_from >= fields_from) {
        return;
    }
    const char **qns = (const char **)cbm_arena_alloc(
        ctx->arena, (size_t)(fields_from - methods_from) * sizeof(const char *));
    if (!qns) {
        return;
    }
    size_t n = 0;
    for (int i = methods_from; i < fields_from; i++) {
        if (defs[i].qualified_name && strcmp(defs[i].label, "Method") == 0) {
            qns[n++] = defs[i].qualified_name;
        }
    }
    if (n == 0) {
        return;
    }
    qsort(qns, n, sizeof(qns[0]), cmp_cstr_ptr);
    for (int i = fields_from; i < fields_to; i++) {
        const char *qn = defs[i].qualified_name;
        if (qn && strcmp(defs[i].label, "Field") == 0 &&
            bsearch(&qn, qns, n, sizeof(qns[0]), cmp_cstr_ptr)) {
            char *fenced = cbm_arena_sprintf(ctx->arena, "%s#field", qn);
            if (fenced) {
                defs[i].qualified_name = fenced;
            }
        }
    }
}

// Extract class-level variables (field declarations inside class bodies).
// A declaration that extract_class_fields already made a Field (the defs from
// `fields_from` on) is not minted again: Java, C# and Solidity list the same
// kind as field and variable, and the second copy was a Variable with the
// module's qualified name (`demo.query.filters` for field Query.filters),
// which collided with package paths.
static void extract_class_variables(CBMExtractCtx *ctx, TSNode class_node, const char *class_qn,
                                    const CBMLangSpec *spec, int fields_from) {
    if (!spec->variable_node_types || !spec->variable_node_types[0]) {
        return;
    }

    TSNode body = find_class_member_body(class_node, ctx->language);
    if (ts_node_is_null(body)) {
        return;
    }

    /* the Fields of this body, in body order: walked alongside the children */
    int fields_to = ctx->result->defs.count;
    int fi = fields_from;

    /* Record the declaring class on every variable minted from this body (see
     * push_var_def_qn); saved/restored so module-level minting stays bare. */
    const char *saved_parent = ctx->var_parent_class;
    ctx->var_parent_class = class_qn;
    member_iter_t it;
    member_iter_init(&it, body, ctx->language, true);
    TSNode child;
    while (member_iter_next(&it, &child)) {
        if (!cbm_kind_in_set(child, spec->variable_node_types)) {
            continue;
        }
        int line = (int)ts_node_start_point(child).row + TS_LINE_OFFSET;
        while (fi < fields_to && (strcmp(ctx->result->defs.items[fi].label, "Field") != 0 ||
                                  (int)ctx->result->defs.items[fi].start_line < line)) {
            fi++;
        }
        if (fi < fields_to && (int)ctx->result->defs.items[fi].start_line == line &&
            spec->field_node_types && cbm_kind_in_set(child, spec->field_node_types)) {
            continue; /* already a Field of this class */
        }
        extract_var_names(ctx, child, spec);
    }
    member_iter_done(&it);
    ctx->var_parent_class = saved_parent;
}

// --- Module node + main walk ---

// Iterative walk_defs — explicit stack with enclosing class context per frame.
typedef struct {
    TSNode node;
    const char *enclosing_class_qn; // saved context for class nesting
} walk_defs_frame_t;

/* #668: walk_defs previously used a fixed `walk_defs_frame_t stack[4096]` — a
 * single ~160 KB C-stack frame. That overflowed small thread stacks (the
 * pre-2026-03 Windows 1 MB main thread) on the definitions pass, and its
 * `top < 4096` push guards SILENTLY DROPPED every top-level definition past
 * 4096. It became a growable stack bounded by an 8M-frame ceiling
 * (env CBM_WALK_DEFS_MAX) that still stopped pushing once reached — and since
 * children are pushed last-to-first, a file wider than the ceiling lost its
 * FIRST definitions. No work cap may decide graph content, so there is no
 * ceiling: the stack doubles on demand, and only an allocation failure stops
 * the walk. That failure is loud — logged at error level and reported as a
 * per-file extract error (result->has_error) — never a silent partial file. */
typedef struct {
    walk_defs_frame_t *data;
    int top;
    int cap;
    const char *path; // for the error log on allocation failure (may be NULL)
    bool failed;      // growth failed: pending frames dropped, walk drains
    /* The per-file traversal scratch (ctx->scratch) when there is one: frames
     * then come from memory the thread reuses file after file, where a malloc
     * of 256 frames per file was 28 k allocations and 255 MB never written on
     * the Go corpus (waste sanitizer, 2026-09-17). Growth copies into a
     * doubled buffer and abandons the old one to the arena, like TSNodeStack.
     * NULL: the memory core (class EXTRACT), released by the walk. */
    CBMArena *arena;
} wd_stack_t;

enum { WD_STACK_INITIAL = 256 };

/* Double `s` (or give it its first WD_STACK_INITIAL frames). Returns false,
 * after logging, when the doubled size is not representable or cannot be
 * allocated; the old buffer is left intact so the walk can still release it. */
static bool wd_grow(wd_stack_t *s) {
    walk_defs_frame_t *nd = NULL;
    int ncap = WD_STACK_INITIAL;
    bool fits = true;
    if (s->cap > 0) {
        fits = s->cap <= INT_MAX / 2;
        ncap = fits ? s->cap * 2 : s->cap;
    }
    fits = fits && (size_t)ncap <= SIZE_MAX / sizeof(walk_defs_frame_t);
    if (fits) {
        size_t bytes = (size_t)ncap * sizeof(walk_defs_frame_t);
        if (s->arena) {
            nd = (walk_defs_frame_t *)cbm_arena_alloc(s->arena, bytes);
            if (nd && s->top > 0) {
                memcpy(nd, s->data, (size_t)s->top * sizeof(walk_defs_frame_t));
            }
        } else {
            nd = (walk_defs_frame_t *)cbm_realloc(CBM_MEM_CLASS_EXTRACT, s->data, bytes);
        }
    }
    if (!nd) {
        char pending[24];
        snprintf(pending, sizeof(pending), "%d", s->top);
        cbm_log_error("extract.walk_stack_alloc_failed", "walker", "walk_defs", "pending", pending,
                      "path", s->path ? s->path : "");
        return false;
    }
    s->data = nd;
    s->cap = ncap;
    return true;
}

static void wd_push(wd_stack_t *s, TSNode node, const char *enclosing_qn) {
    if (s->failed) {
        return;
    }
    if (s->top >= s->cap && !wd_grow(s)) {
        /* Drop every pending frame so the walk_defs loop drains and exits;
         * walk_defs then marks the file's result as an extract error. */
        s->failed = true;
        s->top = 0;
        return;
    }
    s->data[s->top++] = (walk_defs_frame_t){node, enclosing_qn};
}

/* Push all children of `node` in REVERSE order (so they pop in source order)
 * using a LINEAR traversal. Index-based ts_node_child(i) is O(i) per call in
 * tree-sitter, so a reverse index loop is O(n^2) per node — a file whose
 * root has ~580k flat siblings (ms-typescript's reallyLargeFile.ts fourslash
 * fixture) needed ~1.7e11 child-iterator steps and hung extraction for
 * hours; the supervisor then killed the silent worker as a hang and
 * quarantined innocent files off the stale marker. Collect the children
 * FORWARD with a TSTreeCursor (O(1) amortized per step) into a scratch
 * buffer, then push in reverse. Small nodes keep the direct index loop —
 * no cursor/buffer overhead on the overwhelmingly common case. */
enum { WD_CURSOR_MIN_CHILDREN = 64 };

/* Collect all `cc` children of `node` linearly via a TSTreeCursor into a
 * malloc'd array (caller frees). Returns NULL for small nodes and on OOM —
 * the caller then uses indexed ts_node_child access, which is fine (and
 * cheaper) at small child counts and merely quadratic-but-correct on OOM. */
static TSNode *wd_collect_children(TSNode node, uint32_t cc) {
    if (cc < WD_CURSOR_MIN_CHILDREN) {
        return NULL;
    }
    TSNode *buf = (TSNode *)malloc((size_t)cc * sizeof(TSNode));
    if (!buf) {
        return NULL;
    }
    TSTreeCursor cur = ts_tree_cursor_new(node);
    uint32_t got = 0;
    if (ts_tree_cursor_goto_first_child(&cur)) {
        do {
            buf[got++] = ts_tree_cursor_current_node(&cur);
        } while (got < cc && ts_tree_cursor_goto_next_sibling(&cur));
    }
    ts_tree_cursor_delete(&cur);
    if (got != cc) {
        /* Defensive: cursor and child_count disagree — fall back to indexed. */
        free(buf);
        return NULL;
    }
    return buf;
}

static void wd_push_children_reverse(wd_stack_t *s, TSNode node, const char *enclosing_qn) {
    uint32_t cc = ts_node_child_count(node);
    if (cc == 0) {
        return;
    }
    TSNode *kids = wd_collect_children(node, cc);
    for (int i = (int)cc - SKIP_CHAR; i >= 0; i--) {
        wd_push(s, kids ? kids[i] : ts_node_child(node, (uint32_t)i), enclosing_qn);
    }
    free(kids);
}

/* 1-based line of the `}` that closes the `{` at `open`, or 0 when it does not
 * close before `limit`. Counts only the first branch of every #if group (the
 * first-branch projection rule of the C rescue in cbm.c): `#ifdef X struct {
 * #else union { #endif` opens one brace, not two. Comments, string and char
 * literals are skipped; dropped branches are ignored wholesale. */
enum { C_BRACE_PP_DEPTH = 64 };

static bool c_brace_directive_is(const char *p, const char *e, const char *w) {
    size_t n = strlen(w);
    return (size_t)(e - p) >= n && strncmp(p, w, n) == 0 &&
           ((size_t)(e - p) == n || !isalpha((unsigned char)p[n]));
}

static uint32_t c_matching_brace_line(const char *src, uint32_t open, uint32_t limit,
                                      uint32_t open_row) {
    uint8_t branch[C_BRACE_PP_DEPTH];
    uint8_t keep[C_BRACE_PP_DEPTH];
    int pp = 0;
    int depth = 0;
    uint32_t row = open_row;
    bool line_start = false;
    for (uint32_t i = open; i < limit; i++) {
        char c = src[i];
        if (c == '\n') {
            row++;
            line_start = true;
            continue;
        }
        if (line_start && (c == ' ' || c == '\t')) {
            continue;
        }
        if (line_start && c == '#') {
            const char *p = src + i + 1;
            const char *e = src + limit;
            while (p < e && (*p == ' ' || *p == '\t')) {
                p++;
            }
            if (c_brace_directive_is(p, e, "if") || c_brace_directive_is(p, e, "ifdef") ||
                c_brace_directive_is(p, e, "ifndef")) {
                if (pp >= C_BRACE_PP_DEPTH) {
                    return 0;
                }
                const char *q = p + 2;
                while (q < e && isalpha((unsigned char)*q)) {
                    q++;
                }
                while (q < e && (*q == ' ' || *q == '\t')) {
                    q++;
                }
                bool zero = c_brace_directive_is(p, e, "if") && q < e && *q == '0' &&
                            (q + 1 >= e || !isalnum((unsigned char)q[1]));
                branch[pp] = 0;
                keep[pp] = zero ? 1 : 0;
                pp++;
            } else if (c_brace_directive_is(p, e, "elif") || c_brace_directive_is(p, e, "else") ||
                       c_brace_directive_is(p, e, "elifdef") ||
                       c_brace_directive_is(p, e, "elifndef")) {
                if (pp > 0) {
                    branch[pp - 1] = branch[pp - 1] < UINT8_MAX ? branch[pp - 1] + 1 : UINT8_MAX;
                } else {
                    /* the next branch of a group opened before the brace: drop
                     * everything up to that group's #endif */
                    branch[0] = 1;
                    keep[0] = 0;
                    pp = 1;
                }
            } else if (c_brace_directive_is(p, e, "endif") && pp > 0) {
                pp--;
            }
            /* skip the directive's logical line, backslash continuations included */
            for (;;) {
                while (i + 1 < limit && src[i + 1] != '\n') {
                    i++;
                }
                if (i + 1 < limit && src[i] == '\\') {
                    i++; /* onto the continued line's newline */
                    row++;
                    continue;
                }
                break;
            }
            continue;
        }
        line_start = false;
        bool active = true;
        for (int k = 0; k < pp && active; k++) {
            active = branch[k] == keep[k];
        }
        if (!active) {
            continue;
        }
        if (c == '/' && i + 1 < limit && src[i + 1] == '*') {
            for (i += 2; i + 1 < limit && !(src[i] == '*' && src[i + 1] == '/'); i++) {
                row += src[i] == '\n';
            }
            i++;
            continue;
        }
        if (c == '/' && i + 1 < limit && src[i + 1] == '/') {
            while (i + 1 < limit && src[i + 1] != '\n') {
                i++;
            }
            continue;
        }
        if (c == '"' || c == '\'') {
            for (i++; i < limit && src[i] != c && src[i] != '\n'; i++) {
                if (src[i] == '\\' && i + 1 < limit) {
                    i++;
                    row += src[i] == '\n';
                }
            }
            if (i < limit && src[i] == '\n') {
                i--; /* unterminated literal: let the loop count the newline */
            }
            continue;
        }
        if (c == '{') {
            depth++;
        } else if (c == '}' && --depth == 0) {
            return row + TS_LINE_OFFSET;
        }
    }
    return 0;
}

/* tree-sitter can give up on a whole region and leave a definition head as loose
 * ERROR tokens: `struct task_struct {` in the kernel's include-guarded sched.h is
 * never recovered as a struct_specifier, so the type had no definition node (its
 * only nodes were references, now dropped by is_c_tag_reference). The tokens
 * `struct|union|class|enum NAME {` can only start a definition of NAME; recover it
 * with its span up to the matching brace (members stay with the ERROR region). */
static void recover_c_error_tag_heads(CBMExtractCtx *ctx, TSNode error_node) {
    CBMArena *a = ctx->arena;
    /* One linear cursor pass with a three-token window: a file-level ERROR can
     * hold every top-level token of the file as a flat sibling, where indexed
     * child access is quadratic (see wd_collect_children). */
    TSTreeCursor cursor = ts_tree_cursor_new(error_node);
    TSNode win[3];
    memset(win, 0, sizeof(win));
    uint32_t seen = 0;
    /* Once one head never closes, later heads in this region will not either:
     * do not rescan to the region end for each of them. */
    bool unclosed = false;
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    for (; more; more = ts_tree_cursor_goto_next_sibling(&cursor)) {
        win[0] = win[1];
        win[1] = win[2];
        win[2] = ts_tree_cursor_current_node(&cursor);
        if (++seen < 3) {
            continue;
        }
        TSNode kw = win[0];
        TSNode name_node = win[1];
        TSNode brace = win[2];
        if (ts_node_is_named(kw)) {
            continue;
        }
        const char *k = ts_node_type(kw);
        bool is_enum = strcmp(k, "enum") == 0;
        if (!is_enum && strcmp(k, "struct") != 0 && strcmp(k, "union") != 0 &&
            strcmp(k, "class") != 0) {
            continue;
        }
        if (strcmp(ts_node_type(name_node), "type_identifier") != 0 || ts_node_is_named(brace) ||
            strcmp(ts_node_type(brace), "{") != 0) {
            continue;
        }
        char *name = cbm_node_text(a, name_node, ctx->source);
        if (!name || !name[0]) {
            continue;
        }
        uint32_t end_line = unclosed ? 0
                                     : c_matching_brace_line(ctx->source, ts_node_start_byte(brace),
                                                             ts_node_end_byte(error_node),
                                                             ts_node_start_point(brace).row);
        unclosed = end_line == 0;
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name =
            ctx->enclosing_class_qn
                ? cbm_arena_sprintf(a, "%s.%s", ctx->enclosing_class_qn, name)
                : cbm_fqn_compute_source_lang(a, ctx->project, ctx->rel_path, name, ctx->language);
        def.label = is_enum ? "Enum" : "Class";
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(kw).row + TS_LINE_OFFSET;
        def.end_line = end_line ? end_line : ts_node_end_point(error_node).row + TS_LINE_OFFSET;
        def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
        def.is_exported = cbm_is_exported(name, ctx->language);
        /* The doc comment sits before the ERROR node when the head opens it. */
        def.docstring = extract_docstring(ctx, seen == 3 ? error_node : kw, name);
        cbm_defs_push(&ctx->result->defs, a, def);
    }
    ts_tree_cursor_delete(&cursor);
}

// Push nested class nodes from a class body container onto the defs stack.
// Iteratively walks into wrapper nodes (field_declaration, template_declaration).
static void push_nested_class_nodes(TSNode body, const CBMLangSpec *spec, wd_stack_t *s,
                                    const char *enclosing_qn, const CBMExtractCtx *ctx) {
    TSNodeStack nc_stack;
    ts_nstack_init(&nc_stack, ctx, NESTED_CLASS_STACK_CAP);
    ts_nstack_push(&nc_stack, body);

    while (nc_stack.count > 0) {
        TSNode cur = ts_nstack_pop(&nc_stack);
        uint32_t nc = ts_node_child_count(cur);
        /* Linear child access for wide class bodies (see wd_collect_children). */
        TSNode *kids = wd_collect_children(cur, nc);
        for (int i = (int)nc - SKIP_CHAR; i >= 0; i--) {
            TSNode child = kids ? kids[i] : ts_node_child(cur, (uint32_t)i);
            if (cbm_kind_in_set(child, spec->class_node_types)) {
                wd_push(s, child, enclosing_qn);
            } else {
                const char *ck = ts_node_type(child);
                /* A nested class inside #if/#else is still nested here. */
                if (strcmp(ck, "field_declaration") == 0 ||
                    strcmp(ck, "template_declaration") == 0 || strcmp(ck, "declaration") == 0 ||
                    cbm_kind_in_set(child, cbm_conditional_block_types(spec->language))) {
                    ts_nstack_push(&nc_stack, child);
                }
            }
        }
        free(kids);
    }
}

// Check if a C++/CUDA template_declaration wraps a class/struct/union (not a function).
static bool is_template_class_node(TSNode node, CBMLanguage lang) {
    if ((lang != CBM_LANG_CPP && lang != CBM_LANG_CUDA) ||
        strcmp(ts_node_type(node), "template_declaration") != 0) {
        return false;
    }
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        const char *ck = ts_node_type(ts_node_named_child(node, i));
        if (strcmp(ck, "class_specifier") == 0 || strcmp(ck, "struct_specifier") == 0 ||
            strcmp(ck, "union_specifier") == 0) {
            return true;
        }
    }
    return false;
}

// Compute the enclosing class/namespace QN for a scoped node.
/* A namespace contributes a QN segment so a symbol declared in `namespace ns`
 * is `proj.file.ns.sym`, not a top-level `proj.file.sym`. Without the namespace
 * in the QN, namespace-aware resolution (C++ ADL) is starved: a bare call
 * collapses to the file scope and resolves directly instead. Unlike a class, a
 * namespace emits no def of its own — it only extends the enclosing scope for
 * its members. C#/PHP need the same treatment paired with their LSP resolvers
 * (a def-only change breaks their existing namespace handling), done separately. */
static bool is_namespace_scope_kind(CBMLanguage lang, const char *kind, TSNode node) {
    /* Delegate the kind-only languages to the shared predicate rather than
     * restating them. This function once carried its own copy of the C++/CUDA
     * case, which silently dropped the TypeScript `internal_module` case the
     * shared predicate also has -- so TS namespace members lost their namespace
     * QN segment on the def side while extract_unified and the enclosing-scope
     * walk in helpers.c still qualified them, and `MyNS.inner()` stopped
     * resolving. One source of truth for kind -> namespace-scope; this wrapper
     * adds only the case the shared predicate cannot express. */
    if (cbm_is_namespace_scope_kind(lang, kind)) {
        return true;
    }
    /* Nix: a binding whose value is an attribute set is a named scope that is not
     * itself a definition — the same shape as a C++ namespace. `setA = { fn = …; }`
     * makes `fn` proj.file.setA.fn, so two attrsets can each hold a `fn` without
     * the second silently overwriting the first.
     *
     * Deliberately NOT `let` bindings: those are lexical, and C++ does not qualify
     * by block scope either. Takes the node because the decision depends on the
     * binding's VALUE, which the kind string alone cannot express. */
    if (lang == CBM_LANG_NIX && strcmp(kind, "binding") == 0) {
        return cbm_nix_binding_is_attrset_scope(node);
    }
    return false;
}

static const char *compute_class_qn(CBMExtractCtx *ctx, TSNode node, const char *saved_enclosing) {
    /* Nix scopes are `binding` nodes, which carry an `attrpath` rather than a
     * `name` field. Shared with the unified extractor's own compute_class_qn so
     * the def QN and the call-scope QN cannot drift. */
    if (ctx->language == CBM_LANG_NIX) {
        return cbm_nix_binding_scope_qn(ctx, node, saved_enclosing);
    }
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_OBJC) {
        name_node = cbm_find_child_by_kind(node, "identifier");
    }
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_SWIFT) {
        name_node = cbm_find_child_by_kind(node, "type_identifier");
    }
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_OBJECTSCRIPT_UDL) {
        name_node = cbm_find_child_by_kind(node, "class_name");
    }
    if (!ts_node_is_null(name_node)) {
        char *cname = cbm_node_text(ctx->arena, name_node, ctx->source);
        if (cname && cname[0]) {
            if (saved_enclosing) {
                return cbm_arena_sprintf(ctx->arena, "%s.%s", saved_enclosing, cname);
            }
            /* Top-level: language-aware module so Java/Go don't double the
             * filename stem (matches extract_class_def above). */
            return cbm_fqn_compute_source_lang(ctx->arena, ctx->project, ctx->rel_path, cname,
                                               ctx->language);
        }
    }
    return saved_enclosing;
}

static void extract_typescript_namespace_def(CBMExtractCtx *ctx, TSNode node,
                                             const char *saved_enclosing) {
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name_node)) {
        return;
    }
    char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    const char *namespace_qn = compute_class_qn(ctx, node, saved_enclosing);
    if (!name || !name[0] || !namespace_qn || !namespace_qn[0]) {
        return;
    }
    CBMDefinition def = {0};
    def.name = name;
    def.qualified_name = namespace_qn;
    def.label = "Module";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
    def.is_exported = true;
    cbm_defs_push(&ctx->result->defs, ctx->arena, def);
}

/* Dart `extension on T { ... }` has no name: there is no container def to hang
 * its members on, and extract_class_def would bail before extract_class_methods,
 * dropping every member (#1457). */
static bool is_dart_unnamed_extension(const CBMExtractCtx *ctx, TSNode node) {
    return ctx->language == CBM_LANG_DART &&
           strcmp(ts_node_type(node), "extension_declaration") == 0 &&
           ts_node_is_null(ts_node_child_by_field_name(node, TS_FIELD("name")));
}

/* Walk an unnamed Dart extension's members as file-level functions. A member is
 * `method_signature > function_signature`; the wrapper carries no name of its
 * own, so push the inner function_signature (which does) for the generic walk. */
static void push_dart_unnamed_extension_members(TSNode node, wd_stack_t *s,
                                                const char *enclosing_qn) {
    TSNode body = ts_node_child_by_field_name(node, TS_FIELD("body"));
    if (ts_node_is_null(body)) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(body);
    for (int i = (int)nc - 1; i >= 0; i--) {
        TSNode child = ts_node_named_child(body, (uint32_t)i);
        if (strcmp(ts_node_type(child), "method_signature") == 0) {
            TSNode sig = cbm_find_child_by_kind(child, "function_signature");
            if (!ts_node_is_null(sig)) {
                wd_push(s, sig, enclosing_qn);
            }
            continue;
        }
        wd_push(s, child, enclosing_qn);
    }
}

// Push nested class children from a class body container onto the walk stack.
static void push_class_body_children(TSNode node, const CBMLangSpec *spec, wd_stack_t *s,
                                     const char *new_enclosing, const CBMExtractCtx *ctx) {
    /* Use the same language-aware body selection as method extraction.  The old
     * independent spelling list omitted valid containers such as Scala's
     * `template_body` and Solidity's contract body.  Methods were extracted
     * once by extract_class_methods(), then the generic walk descended through
     * the unrecognized body and extracted them again as free functions. */
    /* Smali's class node is itself the member container.  Method extraction
     * currently preserves Smali's established file-level Function contract,
     * so the generic walk must still visit those direct method_definition
     * children.  Treating the class itself as a completed body here would only
     * scan it for nested classes and silently drop every method. */
    TSNode body = {0};
    if (spec->language != CBM_LANG_SMALI) {
        body = find_class_member_body(node, spec->language);
    }
    if (!ts_node_is_null(body)) {
        push_nested_class_nodes(body, spec, s, new_enclosing, ctx);
        return;
    }

    // No body found — push all children directly
    uint32_t nc = ts_node_child_count(node);
    for (int ci = (int)nc - SKIP_CHAR; ci >= 0; ci--) {
        wd_push(s, ts_node_child(node, (uint32_t)ci), new_enclosing);
    }
}

// ASCII case-insensitive equality vs a lowercase literal (CFML attribute names
// are case-insensitive, e.g. NAME / Name / name).
static bool ascii_ci_equals(const char *s, const char *lower_lit) {
    if (!s) {
        return false;
    }
    for (; *s && *lower_lit; s++, lower_lit++) {
        if (tolower((unsigned char)*s) != (unsigned char)*lower_lit) {
            return false;
        }
    }
    return *s == '\0' && *lower_lit == '\0';
}

// CFML tag function: <cffunction name="foo" ...> ... </cffunction> (#38). The
// cf_function_tag node has no `name` field — the name lives in a cf_attribute
// child (name="foo"). Emit a Function node named after that attribute value.
static void extract_cfml_function_tag(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    char *name = NULL;
    uint32_t cc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < cc && !name; i++) {
        TSNode ch = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(ch), "cf_attribute") != 0) {
            continue;
        }
        TSNode an = cbm_find_child_by_kind(ch, "cf_attribute_name");
        if (ts_node_is_null(an)) {
            continue;
        }
        char *aname = cbm_node_text(a, an, ctx->source);
        if (!ascii_ci_equals(aname, "name")) {
            continue;
        }
        TSNode val = cbm_find_child_by_kind(ch, "quoted_cf_attribute_value");
        if (ts_node_is_null(val)) {
            val = cbm_find_child_by_kind(ch, "cf_attribute_value");
        }
        if (ts_node_is_null(val)) {
            continue;
        }
        TSNode inner = cbm_find_child_by_kind(val, "attribute_value");
        name = cbm_node_text(a, ts_node_is_null(inner) ? val : inner, ctx->source);
    }
    if (!name || !name[0]) {
        return;
    }

    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    def.label = "Function";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
    def.is_exported = true;
    cbm_defs_push(&ctx->result->defs, a, def);
}

// Helm / Go template named-template definition: {{ define "chart.fullname" }} ...
// {{ end }} (#338). The name is a string-literal child of define_action. Emit a
// Function node so `include`/`template` references resolve to it via CALLS.
static void extract_gotemplate_define(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    TSNode s = cbm_find_child_by_kind(node, "interpreted_string_literal");
    if (ts_node_is_null(s)) {
        return;
    }
    char *raw = cbm_node_text(a, s, ctx->source);
    if (!raw) {
        return;
    }
    size_t len = strlen(raw);
    if (len >= 2 && (raw[0] == '"' || raw[0] == '`')) {
        raw = cbm_arena_strndup(a, raw + 1, len - 2); // strip surrounding quotes
    }
    if (!raw || !raw[0]) {
        return;
    }

    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = raw;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, raw);
    def.label = "Function";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
    def.is_exported = true;
    cbm_defs_push(&ctx->result->defs, a, def);
}

// Janet (janet-simple S-expression grammar): definitions are generic `par_tup_lit`
// forms whose head `sym_lit` is a def keyword — `(defn foo [] 1)` -> "foo". There
// is no dedicated def node type, so gate on the head keyword and pull the name
// from the second named child (also a `sym_lit`).
static bool janet_is_def_head(const char *t) {
    if (!t) {
        return false;
    }
    static const char *heads[] = {"defn",      "defn-",   "defmacro",    "defmacro-", "varfn",
                                  "fn",        "def",     "def-",        "var",       "var-",
                                  "defstruct", "deftype", "defprotocol", NULL};
    for (int i = 0; heads[i]; i++) {
        if (strcmp(t, heads[i]) == 0) {
            return true;
        }
    }
    return false;
}

static void extract_janet_def(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    if (ts_node_named_child_count(node) < 2) {
        return;
    }
    TSNode head = ts_node_named_child(node, 0);
    if (strcmp(ts_node_type(head), "sym_lit") != 0) {
        return;
    }
    char *head_text = cbm_node_text(a, head, ctx->source);
    if (!janet_is_def_head(head_text)) {
        return;
    }
    TSNode name_node = ts_node_named_child(node, 1);
    if (strcmp(ts_node_type(name_node), "sym_lit") != 0) {
        return;
    }
    char *name = cbm_node_text(a, name_node, ctx->source);
    if (!name || !name[0]) {
        return;
    }
    bool is_class = strcmp(head_text, "defstruct") == 0 || strcmp(head_text, "deftype") == 0 ||
                    strcmp(head_text, "defprotocol") == 0;
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    def.label = is_class ? "Class" : "Function";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
    def.is_exported = true;
    cbm_defs_push(&ctx->result->defs, a, def);
}

// C/C++ preprocessor macros become Macro nodes (#375), in the languages of
// cbm_is_c_preprocessor_lang:
//   #define SIMPLE 1          -> preproc_def
//   #define FN(x) (2 * (x))   -> preproc_function_def
// The name is the `name` field; a function-like macro's parameter list is kept
// as the signature. The macro body (a preproc_arg) is not descended into.
// The QN is `<module>.<NAME>#macro` (CBM_MACRO_QN_SUFFIX): macros have a
// namespace of their own, so a rename macro (`#define XXH32 XXH_NAME2(...)`) or
// an #else stand-in (`#define match(c, m) TRUE`) never competes with the
// function, type or enumerator of the same name for one node.
static void extract_c_macro_def(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name_node)) {
        return;
    }
    char *name = cbm_node_text(a, name_node, ctx->source);
    if (!name || !name[0]) {
        return;
    }

    const char *plain_qn = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    if (!plain_qn) {
        return;
    }

    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = cbm_arena_sprintf(a, "%s" CBM_MACRO_QN_SUFFIX, plain_qn);
    def.label = "Macro";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
    def.is_exported = true; // macros have no translation-unit scoping — globally visible

    TSNode params = ts_node_child_by_field_name(node, TS_FIELD("parameters"));
    if (!ts_node_is_null(params)) {
        def.signature = cbm_node_text(a, params, ctx->source);
    }
    def.docstring = extract_member_docstring(ctx, node);

    cbm_defs_push(&ctx->result->defs, a, def);
}

// Clojure/Racket/Scheme: definitions are macro forms inside a generic `list`
// node — these grammars have no dedicated def node. Detect a definition head
// symbol and pull the name from the following form:
//   (defn foo [] ...) / (define (foo) ...) / (define foo ...)  ->  "foo".
static bool lisp_is_def_head(const char *t) {
    if (!t) {
        return false;
    }
    static const char *heads[] = {"defn",
                                  "defn-",
                                  "def",
                                  "defmacro",
                                  "defmulti",
                                  "defmethod",
                                  "defprotocol",
                                  "defrecord",
                                  "deftype",
                                  "definterface",
                                  "defonce", // Clojure
                                  "define",
                                  "define-syntax",
                                  "define-values",
                                  "define-syntax-rule",
                                  "define-struct",
                                  "define-record-type",
                                  "define/contract", // Scheme/Racket
                                  "struct",          // Racket struct
                                  NULL};
    for (int i = 0; heads[i]; i++) {
        if (strcmp(t, heads[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Basename stem (no directory, no extension) of a path, into an arena string.
 * A Chialisp `(mod ...)` has no name of its own — the file IS the puzzle — so
 * the module entry point is named after the file that holds it. */
static char *lisp_path_stem(CBMArena *a, const char *path) {
    if (!path) {
        return NULL;
    }
    const char *slash = strrchr(path, '/');
    const char *base = slash ? slash + SKIP_CHAR : path;
    const char *dot = strrchr(base, '.');
    size_t len = (dot && dot != base) ? (size_t)(dot - base) : strlen(base);
    return cbm_arena_strndup(a, base, len);
}

static void extract_lisp_def(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    bool chialisp = (ctx->language == CBM_LANG_CHIALISP);
    if (ts_node_named_child_count(node) < 2) {
        return;
    }
    TSNode head_node =
        chialisp ? cbm_lisp_named_child_skip_comments(node, 0) : ts_node_named_child(node, 0);
    if (ts_node_is_null(head_node)) {
        return;
    }
    char *head = cbm_node_text(a, head_node, ctx->source);
    if (!(chialisp ? cbm_chialisp_is_def_head(head) : lisp_is_def_head(head))) {
        return;
    }
    /* A def head inside `(q ...)`/`(qq ...)` is quoted DATA — Chialisp macros
     * embed puzzle-shaped literals there — so it must not mint a node. */
    if (chialisp && cbm_lisp_node_in_quote(a, node, ctx->source)) {
        return;
    }
    char *name = NULL;
    if (chialisp && strcmp(head, "mod") == 0) {
        /* A top-level `(mod (ARGS) ...)` is the puzzle entry point; its second
         * form is the curried-argument LIST, so the generic nested-name path
         * below would name the module after its first curried argument. */
        name = lisp_path_stem(a, ctx->rel_path);
    } else {
        TSNode target =
            chialisp ? cbm_lisp_named_child_skip_comments(node, 1) : ts_node_named_child(node, 1);
        if (ts_node_is_null(target)) {
            return;
        }
        const char *tk = ts_node_type(target);
        TSNode name_node = target;
        // (define (foo args) ...) — the name is the head symbol of the nested list.
        if ((strcmp(tk, "list") == 0 || strcmp(tk, "list_lit") == 0) &&
            ts_node_named_child_count(target) > 0) {
            name_node = ts_node_named_child(target, 0);
        }
        if (ts_node_is_null(name_node)) {
            return;
        }
        name = cbm_node_text(a, name_node, ctx->source);
    }
    if (!name || !name[0]) {
        return;
    }
    /* struct/record/type defining forms produce a type node, not a callable
     * (Racket `(struct point ...)`, Clojure `(defrecord ...)`, etc.). */
    const char *lisp_label = "Function";
    if (chialisp) {
        /* Chialisp label map. `Constant` is admitted to the cross-file registry
         * by cbm_label_is_registry_symbol, so a constant defined in an included
         * .clib resolves from every puzzle that includes it. */
        if (strcmp(head, "mod") == 0) {
            lisp_label = "Module";
        } else if (strcmp(head, "defconstant") == 0 || strcmp(head, "defconst") == 0 ||
                   strcmp(head, "embed-file") == 0 || strcmp(head, "compile-file") == 0) {
            lisp_label = "Constant";
        } else if (strcmp(head, "defmacro") == 0 || strcmp(head, "defmac") == 0) {
            lisp_label = "Macro";
        }
    } else if (strcmp(head, "struct") == 0 || strcmp(head, "define-struct") == 0 ||
               strcmp(head, "define-record-type") == 0 || strcmp(head, "defrecord") == 0 ||
               strcmp(head, "deftype") == 0) {
        lisp_label = "Struct";
    } else if (strcmp(head, "definterface") == 0 || strcmp(head, "defprotocol") == 0) {
        lisp_label = "Interface";
    }
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    def.label = lisp_label;
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
    def.is_exported = true;
    cbm_defs_push(&ctx->result->defs, a, def);
}

/* Kotlin ERROR-node class recovery.
 *
 * The vendored fwcd tree-sitter-kotlin (commit 93bfeee) fails to parse two
 * member-modifier constructs and wraps the WHOLE enclosing class in a single
 * `ERROR` node, so the outer class is never recognized as a `class_declaration`
 * and disappears from the graph:
 *
 *   class MyClass { companion object : Factory<MyClass>() { ... } }   // anon companion w/
 * delegation class Tree    { inner class Node : BaseNode() { ... } }            // inner +
 * delegation
 *
 * Inside the ERROR node the tokens are still present as a flat child list:
 *   `class`/`object` keyword token → simple_identifier/type_identifier (name)
 *   → optional `:` then one or more `delegation_specifier` siblings (bases).
 *
 * Recover each named class/object declaration from that flat sequence and emit a
 * Class definition (with bases) so it is discoverable. Strictly additive and
 * gated to Kotlin ERROR nodes: an ERROR region is already a broken parse, so
 * recovering names from it cannot regress a correct parse. Anonymous declarations
 * (e.g. a `companion object` with no name) are skipped — there is nothing to emit.
 */
static void recover_kotlin_error_classes(CBMExtractCtx *ctx, TSNode err_node) {
    CBMArena *a = ctx->arena;
    uint32_t cc = ts_node_child_count(err_node);
    for (uint32_t i = 0; i < cc; i++) {
        TSNode kw = ts_node_child(err_node, i);
        const char *kwt = ts_node_type(kw);
        /* Anonymous `class` / `object` keyword token starts a declaration. */
        if (strcmp(kwt, "class") != 0 && strcmp(kwt, "object") != 0) {
            continue;
        }
        /* The name is the next child, when it is an identifier token. */
        if (i + 1 >= cc) {
            continue;
        }
        TSNode name_node = ts_node_child(err_node, i + 1);
        const char *nt = ts_node_type(name_node);
        if (strcmp(nt, "simple_identifier") != 0 && strcmp(nt, "type_identifier") != 0) {
            /* Anonymous declaration (e.g. `companion object :`) — nothing to emit. */
            continue;
        }
        char *name = cbm_node_text(a, name_node, ctx->source);
        if (!name || !name[0]) {
            continue;
        }

        const char *class_qn;
        if (ctx->enclosing_class_qn) {
            class_qn = cbm_arena_sprintf(a, "%s.%s", ctx->enclosing_class_qn, name);
        } else {
            class_qn = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
        }

        /* Collect bases from any `delegation_specifier` siblings that follow the
         * name (until the class body `{` or the next class/object keyword). */
        const char *bases[MAX_BASES];
        int bcount = 0;
        for (uint32_t j = i + 2; j < cc && bcount < MAX_BASES_MINUS_1; j++) {
            TSNode sib = ts_node_child(err_node, j);
            const char *st = ts_node_type(sib);
            if (strcmp(st, "{") == 0 || strcmp(st, "class") == 0 || strcmp(st, "object") == 0) {
                break;
            }
            if (strcmp(st, "delegation_specifier") != 0) {
                continue;
            }
            /* delegation_specifier → user_type (directly or under
             * constructor_invocation) → type_identifier; strip generic args. */
            TSNode ut = ts_node_named_child(sib, 0);
            if (!ts_node_is_null(ut) && strcmp(ts_node_type(ut), "constructor_invocation") == 0) {
                ut = ts_node_named_child(ut, 0);
            }
            if (ts_node_is_null(ut)) {
                continue;
            }
            TSNode ti = ut;
            if (strcmp(ts_node_type(ut), "user_type") == 0 && ts_node_named_child_count(ut) > 0) {
                ti = ts_node_named_child(ut, 0);
            }
            push_base_text(a, ti, ctx->source, bases, MAX_BASES_MINUS_1, &bcount);
        }

        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = class_qn;
        def.label = "Class";
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(name_node).row + TS_LINE_OFFSET;
        def.end_line = ts_node_end_point(err_node).row + TS_LINE_OFFSET;
        def.is_exported = cbm_is_exported(name, ctx->language);
        if (bcount > 0) {
            const char **result = (const char **)cbm_arena_alloc(a, (size_t)(bcount + NULL_TERM) *
                                                                        sizeof(const char *));
            if (result) {
                for (int k = 0; k < bcount; k++) {
                    result[k] = bases[k];
                }
                result[bcount] = NULL;
                def.base_classes = result;
            }
        }
        cbm_defs_push(&ctx->result->defs, a, def);
    }
}

static void walk_defs(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec, int depth_unused) {
    (void)depth_unused;
    wd_stack_t s = {0};
    s.path = ctx->rel_path;
    s.arena = ctx->scratch;
    wd_push(&s, root, ctx->enclosing_class_qn);

    while (s.top > 0) {
        walk_defs_frame_t frame = s.data[--s.top];
        TSNode node = frame.node;
        ctx->enclosing_class_qn = frame.enclosing_class_qn;
        const char *kind = ts_node_type(node);

        /* Kotlin: recover class/object declarations the grammar lost inside an
         * ERROR node (companion-object-with-delegation, inner-class-with-base).
         * Additive — fall through so child descent still visits any well-formed
         * subtrees nested in the error region. */
        if (ctx->language == CBM_LANG_KOTLIN && strcmp(kind, "ERROR") == 0) {
            recover_kotlin_error_classes(ctx, node);
        }
        /* C/C++: a definition head left as loose ERROR tokens (additive, as
         * above — descent below still visits the region's parsed subtrees). */
        if (is_c_declarator_lang(ctx->language) && strcmp(kind, "ERROR") == 0) {
            recover_c_error_tag_heads(ctx, node);
        }

        if (ctx->language == CBM_LANG_ELIXIR && strcmp(kind, "call") == 0) {
            extract_elixir_call(ctx, node, spec);
            continue;
        }

        if (cbm_is_c_preprocessor_lang(ctx->language) &&
            (strcmp(kind, "preproc_def") == 0 || strcmp(kind, "preproc_function_def") == 0)) {
            // Gated to full/advanced index modes — macros dominate extraction on
            // macro-dense codebases (e.g. the Linux kernel). See #375.
            if (cbm_macro_extraction_enabled()) {
                extract_c_macro_def(ctx, node);
            }
            continue; // the macro body is a preproc_arg — nothing more to extract
        }

        if (ctx->language == CBM_LANG_CFML && strcmp(kind, "cf_function_tag") == 0) {
            extract_cfml_function_tag(ctx, node);
            // cf_function_tag is in cfml_func_types (for call-scope attribution),
            // but its name lives in a cf_attribute, not a `name` field — so the
            // generic extract_func_def below must NOT also run on it (it would
            // resolve a null name and, for grammars where the kind has a `name`
            // field, double-mint). Push children so nested tags/defs are still
            // traversed, then skip the generic func path.
            wd_push_children_reverse(&s, node, frame.enclosing_class_qn);
            continue;
        }

        if (ctx->language == CBM_LANG_GOTEMPLATE && strcmp(kind, "define_action") == 0) {
            extract_gotemplate_define(ctx, node);
            // define_action is in gotemplate_func_types (for call-scope
            // attribution), but its `name` field is a quoted string literal — the
            // generic extract_func_def below would double-mint a def whose name
            // still carries the quotes. Push children so nested defines are still
            // traversed, then skip the generic func path.
            wd_push_children_reverse(&s, node, frame.enclosing_class_qn);
            continue;
        }

        if ((ctx->language == CBM_LANG_CLOJURE || ctx->language == CBM_LANG_RACKET ||
             ctx->language == CBM_LANG_SCHEME || ctx->language == CBM_LANG_CHIALISP) &&
            (strcmp(kind, "list") == 0 || strcmp(kind, "list_lit") == 0)) {
            extract_lisp_def(ctx, node);
            if (ctx->language == CBM_LANG_CHIALISP) {
                /* Chialisp nests every helper `(defun ...)` inside the top-level
                 * `(mod ...)`, and a .clib wraps its defuns in one enclosing
                 * list. `list` is also chialisp_func_types, so the generic
                 * function_node_types match below would fire on this same node
                 * and `continue` WITHOUT descending — losing every nested def.
                 * Push the children here and skip that match. Gated to Chialisp;
                 * the other lisps keep their existing fall-through. */
                wd_push_children_reverse(&s, node, frame.enclosing_class_qn);
                continue;
            }
            // fall through: descend into children so nested defs are captured too
        }

        if (ctx->language == CBM_LANG_JANET && strcmp(kind, "par_tup_lit") == 0) {
            extract_janet_def(ctx, node);
            // fall through: descend so nested defs inside the form are captured too
        }

        /* WIT: world-scoped `export name: func(...)` / `import name: func(...)`
         * are export_item/import_item nodes (added to wit_func_types). But an
         * export/import can also reference a non-function type (e.g. an
         * interface) — only treat it as a Function when it actually contains a
         * func_type; otherwise descend so its inner body is still traversed. */
        if (ctx->language == CBM_LANG_WIT &&
            (strcmp(kind, "export_item") == 0 || strcmp(kind, "import_item") == 0) &&
            ts_node_is_null(
                find_first_descendant_by_kind(node, "func_type", CBM_DESCENDANT_MAX_DEPTH))) {
            wd_push_children_reverse(&s, node, frame.enclosing_class_qn);
            continue;
        }

        /* A split C invocation's body is its scope node. Candidate matching
         * and raw argument/body validation remain inside extract_func_def. */
        bool split_c_candidate = ctx->language == CBM_LANG_C && ctx->test_declarations &&
                                 ctx->test_declarations_raw_source &&
                                 strcmp(kind, "compound_statement") == 0;
        if (split_c_candidate) {
            extract_func_def(ctx, node, spec);
        }
        if (cbm_kind_in_set(node, spec->function_node_types)) {
            if (!is_template_class_node(node, ctx->language)) {
                extract_func_def(ctx, node, spec);
                // Most languages stop here. JS/TS (and Wolfram) descend into the
                // function body so NESTED named definitions are also captured —
                // e.g. arrow methods of an object literal returned from a factory
                // (the Zustand actions-slice pattern, #341). Anonymous nested
                // arrows have no resolvable name and are skipped.
                // Ada subprograms nest (a procedure body's declarative part can
                // contain inner subprogram bodies); descend so the nested defs
                // are captured and same-file calls to them resolve to a CALLS edge.
                // Nix: a library/module file's ROOT expression is normally itself a
                // function_expression (`{ pkgs, lib, ... }: <body>`), so stopping here
                // abandons the entire file — every binding in it is lost. Descend so
                // the body's `name = args: ...` bindings are reached. Inner curried
                // lambdas (`f = a: b: ...`) resolve no name and mint nothing, so the
                // extra descent adds defs without adding noise.
                bool descend_into_func =
                    (ctx->language == CBM_LANG_C && cbm_test_definition_qn(ctx, node) != NULL) ||
                    (ctx->language == CBM_LANG_WOLFRAM || ctx->language == CBM_LANG_TYPESCRIPT ||
                     ctx->language == CBM_LANG_JAVASCRIPT || ctx->language == CBM_LANG_TSX ||
                     ctx->language == CBM_LANG_ARKTS || ctx->language == CBM_LANG_ADA ||
                     ctx->language == CBM_LANG_NIX);
                if (!descend_into_func) {
                    continue;
                }
            }
        }

        if (ctx->language == CBM_LANG_RUST && strcmp(kind, "impl_item") == 0) {
            extract_rust_impl(ctx, node, spec);
            continue;
        }

        /* A namespace extends the enclosing scope (so members are QN-qualified by
         * it) without being a def itself. Push its children (its declaration_list
         * body and any nested namespaces) under the extended scope so each member
         * is walked normally — functions AND classes, unlike a class body which
         * routes methods through extract_class_methods. Do NOT emit a def or run
         * the class/func paths on the namespace node itself. */
        if (is_namespace_scope_kind(ctx->language, kind, node)) {
            const char *new_enclosing = compute_class_qn(ctx, node, frame.enclosing_class_qn);
            if (ctx->language == CBM_LANG_TYPESCRIPT || ctx->language == CBM_LANG_TSX ||
                ctx->language == CBM_LANG_ARKTS) {
                extract_typescript_namespace_def(ctx, node, frame.enclosing_class_qn);
            }
            wd_push_children_reverse(&s, node, new_enclosing);
            continue;
        }

        if (is_dart_unnamed_extension(ctx, node)) {
            push_dart_unnamed_extension_members(node, &s, frame.enclosing_class_qn);
            continue;
        }

        if (cbm_kind_in_set(node, spec->class_node_types)) {
            extract_class_def(ctx, node, spec);
            const char *new_enclosing = compute_class_qn(ctx, node, frame.enclosing_class_qn);
            push_class_body_children(node, spec, &s, new_enclosing, ctx);
            continue;
        }

        /* Default descent — THE hot loop: a file whose root has hundreds of
         * thousands of flat siblings (580k comment lines in ms-typescript's
         * reallyLargeFile.ts) lands here every visit, so linear child
         * collection is mandatory (see wd_push_children_reverse). */
        wd_push_children_reverse(&s, node, frame.enclosing_class_qn);
    }
    if (!s.arena) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, s.data);
    }
    if (s.failed) {
        ctx->result->has_error = true;
        ctx->result->error_msg = "definitions walk: stack allocation failed";
    }
}

void cbm_extract_definitions_without_module(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec) {
        return;
    }

    // Walk AST for function/class definitions
    if (is_c_declarator_lang(ctx->language)) {
        int first = ctx->result->defs.count;
        walk_defs(ctx, ctx->root, spec, 0);
        drop_c_typedefs_shadowed_by_tags(ctx, first);
    } else {
        walk_defs(ctx, ctx->root, spec, 0);
    }

    // Extract module-level variables
    extract_variables(ctx, ctx->root, spec);
}

/* True when rel_path names a Blazor component file. */
static bool cbm_path_is_razor(const char *rel_path) {
    /* Both Razor file types, not just components. `@page` is what DEFINES a
     * Razor Page, so a .cshtml route is at least as worth extracting as a
     * .razor one. Deliberately not .aspx/.ascx: Web Forms is a different
     * templating syntax (`<%@ %>`, `runat="server"`) with no `@page`
     * directive, so neither the C# recovery nor the scan below applies. */
    if (!rel_path) {
        return false;
    }
    static const char *const suffixes[] = {".razor", ".cshtml"};
    size_t len = strlen(rel_path);
    for (size_t i = 0; i < sizeof(suffixes) / sizeof(suffixes[0]); i++) {
        size_t slen = strlen(suffixes[i]);
        if (len > slen && strcmp(rel_path + (len - slen), suffixes[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Match `@page "/route"` on ONE line; returns the route text or NULL.
 *
 * Deliberately strict: the directive must be the first token on the line and be
 * followed by whitespace and a double-quoted path beginning with '/', so
 * neither `@pageSize` nor a `@page` mentioned in markup prose can match.
 *
 * A blank line is rejected up front rather than falling through the length
 * check, which keeps every later comparison reachable on some path — the
 * all-whitespace case would otherwise leave `line_end - p` provably zero. */
static const char *razor_page_route_on_line(CBMArena *a, const char *line, const char *line_end) {
    static const char directive[] = "@page";
    const size_t dlen = sizeof(directive) - 1U;

    const char *p = line;
    while (p < line_end && (*p == ' ' || *p == '\t')) {
        p++;
    }
    if (p == line_end) {
        return NULL; /* blank line — nothing can follow */
    }
    if ((size_t)(line_end - p) <= dlen || strncmp(p, directive, dlen) != 0) {
        return NULL;
    }
    p += dlen;
    if (*p != ' ' && *p != '\t') {
        return NULL; /* `@pageSize` and friends */
    }
    while (p < line_end && (*p == ' ' || *p == '\t')) {
        p++;
    }
    if (p == line_end || *p != '"') {
        return NULL;
    }
    p++;
    const char *route = p;
    while (p < line_end && *p != '"') {
        p++;
    }
    if (p == line_end || p == route || *route != '/') {
        return NULL; /* unterminated, empty, or not a rooted path */
    }
    return cbm_arena_strndup(a, route, (size_t)(p - route));
}

/* Blazor route directive: `@page "/counter"` lives in MARKUP above the `@code`
 * block. Tree-sitter's C# grammar recovers `@code` but never parses the
 * directive, so there is no AST node to read it from — this scans the raw
 * source instead. That is why routes need no Razor grammar.
 *
 * A component may declare several routes; the first is taken, because
 * CBMDefinition carries a single route_path. */
static const char *cbm_razor_page_route(CBMArena *a, const char *source, int source_len) {
    if (!source || source_len <= 0) {
        return NULL;
    }
    const char *end = source + source_len;
    const char *line = source;

    while (line < end) {
        const char *nl = memchr(line, '\n', (size_t)(end - line));
        const char *route = razor_page_route_on_line(a, line, nl ? nl : end);
        if (route) {
            return route;
        }
        if (!nl) {
            break;
        }
        line = nl + 1;
    }
    return NULL;
}

void cbm_extract_definitions(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec) {
        return;
    }

    CBMArena *a = ctx->arena;

    // Create module node (always first definition)
    CBMDefinition mod;
    memset(&mod, 0, sizeof(mod));
    mod.name = ctx->rel_path; // will be refined by Go layer
    mod.qualified_name = ctx->module_qn;
    mod.label = "Module";
    mod.file_path = ctx->rel_path;
    mod.start_line = FIRST_LINE;
    mod.end_line = ts_node_end_point(ctx->root).row + TS_LINE_OFFSET;
    mod.is_exported = true;
    mod.is_test = ctx->result->is_test_file;
    // #519: index what a config file declares itself to be, not only its path.
    mod.docstring = extract_config_module_description(ctx);
    /* A routable Blazor component carries its route on the module def: the
     * component's class is implicit in a .razor file, so there is no class node
     * to hang it on, and the module QN already is the component's identity.
     * insert_def_into_gbuf creates Route+HANDLES for any def with route_path. */
    if (ctx->language == CBM_LANG_CSHARP && cbm_path_is_razor(ctx->rel_path)) {
        const char *route = cbm_razor_page_route(a, ctx->source, ctx->source_len);
        if (route) {
            mod.route_path = route;
            mod.route_method = "GET"; /* a routable page is reached by navigation */
        }
    }
    int mod_idx = ctx->result->defs.count;
    cbm_defs_push(&ctx->result->defs, a, mod);
    ctx->result->module_doc = extract_module_doc(ctx);

    cbm_extract_definitions_without_module(ctx);

    if (ctx->language == CBM_LANG_JAVASCRIPT || ctx->language == CBM_LANG_TYPESCRIPT ||
        ctx->language == CBM_LANG_TSX || ctx->language == CBM_LANG_ARKTS) {
        /* Same language set as extract_js_vars, which marks the bindings. */
        js_mark_default_export_client(ctx, mod_idx);
    }
}
