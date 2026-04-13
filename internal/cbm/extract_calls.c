#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_sprintf
#include "helpers.h"
#include "lang_specs.h"
#include "extract_unified.h"
#include "foundation/constants.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include <stdint.h>          // uint32_t
#include <string.h>
#include <ctype.h>

/* Max ancestor depth for Lean type-position check. */
enum { LEAN_MAX_PARENT_DEPTH = 20 };
/* Max positional args to scan for URL/string. */
enum { MAX_POSITIONAL_SCAN = 3 };
/* Max positional args to scan for handler ref. */
enum { MAX_HANDLER_SCAN = 4 };
/* Max string arg length before rejection. */
enum { MAX_STRING_ARG_LEN = CBM_SZ_512 };
/* Min printable ASCII (space). */
enum { MIN_PRINTABLE = 0x20 };
/* Handler arg scan start index (skip first positional). */
enum { HANDLER_START_IDX = 1 };

/* Look up a module-level string constant by name. */
static const char *lookup_string_constant(const CBMExtractCtx *ctx, const char *name) {
    if (!name || !name[0]) {
        return NULL;
    }
    const CBMStringConstantMap *map = &ctx->string_constants;
    for (int i = 0; i < map->count; i++) {
        if (strcmp(map->names[i], name) == 0) {
            return map->values[i];
        }
    }
    return NULL;
}

/* Check if a node type is a string literal */
static int is_string_like(const char *kind) {
    return (strcmp(kind, "string") == 0 || strcmp(kind, "string_literal") == 0 ||
            strcmp(kind, "interpreted_string_literal") == 0 ||
            strcmp(kind, "raw_string_literal") == 0 || strcmp(kind, "string_content") == 0);
}

/* Strip surrounding quotes from a string, return arena-allocated copy */
static const char *strip_quotes(CBMArena *a, const char *text) {
    if (!text || !text[0]) {
        return NULL;
    }
    int len = (int)strlen(text);
    if (len >= CBM_QUOTE_PAIR && (text[0] == '"' || text[0] == '\'')) {
        return cbm_arena_strndup(a, text + CBM_QUOTE_OFFSET, (size_t)(len - CBM_QUOTE_PAIR));
    }
    return text;
}

// Forward declarations
static void walk_calls(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec);
static char *extract_callee_name(CBMArena *a, TSNode node, const char *source, CBMLanguage lang);
static void extract_jsx_refs(CBMExtractCtx *ctx, TSNode node);

// Lean 4: check if an apply node is inside a type annotation.
// Strategy: walk up to the nearest declaration boundary; if the apply falls
// inside that declaration's explicit_binder/implicit_binder, or before the
// body field, it's a type annotation. We check byte ranges: a call is valid
// only if it overlaps the body range of the enclosing declaration.
static bool lean_is_in_type_position(TSNode node) {
    TSNode cur = ts_node_parent(node);
    for (int depth = 0; depth < LEAN_MAX_PARENT_DEPTH; depth++) {
        if (ts_node_is_null(cur)) {
            return false;
        }
        const char *pk = ts_node_type(cur);
        // Inside a binder — definitely type position
        if (strcmp(pk, "explicit_binder") == 0 || strcmp(pk, "implicit_binder") == 0 ||
            strcmp(pk, "instance_binder") == 0) {
            return true;
        }
        // At a declaration boundary: check if apply is inside the body field
        if (strcmp(pk, "def") == 0 || strcmp(pk, "theorem") == 0 || strcmp(pk, "instance") == 0 ||
            strcmp(pk, "abbrev") == 0 || strcmp(pk, "structure") == 0 ||
            strcmp(pk, "inductive") == 0) {
            // Check if apply comes after the type annotation.
            // Strategy: if the node starts after the end of the "type" field, it's in value
            // position. If there's no "type" field, allow the call (no annotation to filter).
            TSNode type_field = ts_node_child_by_field_name(cur, TS_FIELD("type"));
            if (ts_node_is_null(type_field)) {
                return false; // no type annotation → allow call
            }
            uint32_t type_end = ts_node_end_byte(type_field);
            uint32_t node_start = ts_node_start_byte(node);
            // If apply starts after the type annotation ends, it's a value (call)
            if (node_start > type_end) {
                return false;
            }
            return true; // apply is within or before type annotation → type position
        }
        cur = ts_node_parent(cur);
    }
    return false;
}

// Try common field-based callee resolution (function, name, method fields).
static char *extract_callee_from_fields(CBMArena *a, TSNode node, const char *source) {
    // Try "function" field
    TSNode func_node = ts_node_child_by_field_name(node, TS_FIELD("function"));
    if (!ts_node_is_null(func_node)) {
        const char *fk = ts_node_type(func_node);
        if (strcmp(fk, "identifier") == 0 || strcmp(fk, "simple_identifier") == 0 ||
            strcmp(fk, "selector_expression") == 0 || strcmp(fk, "attribute") == 0 ||
            strcmp(fk, "member_expression") == 0 || strcmp(fk, "field_expression") == 0 ||
            strcmp(fk, "dot") == 0 || strcmp(fk, "function") == 0 ||
            strcmp(fk, "dotted_identifier") == 0 || strcmp(fk, "member_access_expression") == 0 ||
            strcmp(fk, "scoped_identifier") == 0 || strcmp(fk, "qualified_identifier") == 0) {
            return cbm_node_text(a, func_node, source);
        }
    }

    // Try "name" field (Java method_invocation)
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (!ts_node_is_null(name_node)) {
        char *name = cbm_node_text(a, name_node, source);
        TSNode obj = ts_node_child_by_field_name(node, TS_FIELD("object"));
        if (!ts_node_is_null(obj) && name) {
            char *obj_text = cbm_node_text(a, obj, source);
            if (obj_text && obj_text[0]) {
                return cbm_arena_sprintf(a, "%s.%s", obj_text, name);
            }
        }
        return name;
    }

    // Ruby: "method" + "receiver" fields
    TSNode method_node = ts_node_child_by_field_name(node, TS_FIELD("method"));
    if (!ts_node_is_null(method_node)) {
        char *method = cbm_node_text(a, method_node, source);
        TSNode recv = ts_node_child_by_field_name(node, TS_FIELD("receiver"));
        if (!ts_node_is_null(recv) && method) {
            char *recv_text = cbm_node_text(a, recv, source);
            if (recv_text && recv_text[0]) {
                return cbm_arena_sprintf(a, "%s.%s", recv_text, method);
            }
        }
        return method;
    }

    return NULL;
}

// Haskell/OCaml: extract callee from apply/infix nodes.
static char *extract_fp_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "apply") == 0 || strcmp(nk, "application_expression") == 0) {
        if (ts_node_child_count(node) > 0) {
            TSNode callee = ts_node_child(node, 0);
            const char *ck = ts_node_type(callee);
            if (strcmp(ck, "identifier") == 0 || strcmp(ck, "variable") == 0 ||
                strcmp(ck, "constructor") == 0 || strcmp(ck, "value_path") == 0) {
                return cbm_node_text(a, callee, source);
            }
        }
    }
    if (strcmp(nk, "infix") == 0 || strcmp(nk, "infix_expression") == 0) {
        TSNode op = ts_node_child_by_field_name(node, TS_FIELD("operator"));
        if (!ts_node_is_null(op)) {
            return cbm_node_text(a, op, source);
        }
        enum { INFIX_MIN_CHILDREN = 3, INFIX_OP_IDX = 1 };
        if (ts_node_child_count(node) >= INFIX_MIN_CHILDREN) {
            return cbm_node_text(a, ts_node_child(node, INFIX_OP_IDX), source);
        }
    }
    return NULL;
}

// Wolfram: extract callee from apply, skipping LHS of set definitions.
static char *extract_wolfram_callee(CBMArena *a, TSNode node, const char *source) {
    TSNode parent = ts_node_parent(node);
    if (!ts_node_is_null(parent)) {
        const char *pk = ts_node_type(parent);
        if ((strcmp(pk, "set_delayed_top") == 0 || strcmp(pk, "set_top") == 0 ||
             strcmp(pk, "set_delayed") == 0 || strcmp(pk, "set") == 0) &&
            ts_node_named_child_count(parent) > 0 &&
            ts_node_eq(ts_node_named_child(parent, 0), node)) {
            return NULL;
        }
    }
    if (ts_node_named_child_count(node) > 0) {
        TSNode head = ts_node_named_child(node, 0);
        const char *hk = ts_node_type(head);
        if (strcmp(hk, "user_symbol") == 0 || strcmp(hk, "builtin_symbol") == 0) {
            return cbm_node_text(a, head, source);
        }
    }
    return NULL;
}

// Language-specific callee extraction for FP and niche languages.
// Swift callee extraction from call/constructor expressions.
static char *extract_swift_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "call_expression") != 0 && strcmp(nk, "constructor_expression") != 0) {
        return NULL;
    }
    if (ts_node_named_child_count(node) > 0) {
        TSNode callee = ts_node_named_child(node, 0);
        const char *ck = ts_node_type(callee);
        if (strcmp(ck, "simple_identifier") == 0 || strcmp(ck, "navigation_expression") == 0) {
            return cbm_node_text(a, callee, source);
        }
    }
    return NULL;
}

// Callee extraction for scripting languages (Elixir, Perl, PHP, Kotlin, MATLAB).
static char *extract_scripting_callee(CBMArena *a, TSNode node, const char *source,
                                      CBMLanguage lang, const char *nk) {
    if (lang == CBM_LANG_ELIXIR && strcmp(nk, "call") == 0 && ts_node_child_count(node) > 0) {
        TSNode first = ts_node_child(node, 0);
        const char *fk = ts_node_type(first);
        if (strcmp(fk, "identifier") == 0 || strcmp(fk, "dot") == 0) {
            return cbm_node_text(a, first, source);
        }
        return NULL;
    }
    if (lang == CBM_LANG_PERL && ts_node_child_count(node) > 0) {
        return cbm_node_text(a, ts_node_child(node, 0), source);
    }
    if (lang == CBM_LANG_PHP) {
        TSNode func_node = ts_node_child_by_field_name(node, TS_FIELD("function"));
        if (ts_node_is_null(func_node)) {
            func_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
        }
        return ts_node_is_null(func_node) ? NULL : cbm_node_text(a, func_node, source);
    }
    if (lang == CBM_LANG_KOTLIN && ts_node_child_count(node) > 0) {
        return cbm_node_text(a, ts_node_child(node, 0), source);
    }
    if (lang == CBM_LANG_MATLAB && strcmp(nk, "command") == 0 && ts_node_child_count(node) > 0) {
        return cbm_node_text(a, ts_node_child(node, 0), source);
    }
    return NULL;
}

// ObjC: extract callee from message_expression selector.
static char *extract_objc_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "message_expression") != 0) {
        return NULL;
    }
    TSNode selector = ts_node_child_by_field_name(node, TS_FIELD("selector"));
    return ts_node_is_null(selector) ? NULL : cbm_node_text(a, selector, source);
}

// Erlang: extract callee from call node's first child.
static char *extract_erlang_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "call") != 0 || ts_node_child_count(node) == 0) {
        return NULL;
    }
    return cbm_node_text(a, ts_node_child(node, 0), source);
}

static char *extract_callee_lang_specific(CBMArena *a, TSNode node, const char *source,
                                          CBMLanguage lang) {
    const char *nk = ts_node_type(node);

    if (lang == CBM_LANG_OBJC) {
        return extract_objc_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_ERLANG) {
        return extract_erlang_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_HASKELL || lang == CBM_LANG_OCAML) {
        return extract_fp_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_WOLFRAM && strcmp(nk, "apply") == 0) {
        return extract_wolfram_callee(a, node, source);
    }
    if (lang == CBM_LANG_SWIFT) {
        return extract_swift_callee(a, node, source, nk);
    }

    return extract_scripting_callee(a, node, source, lang, nk);
}

// Extract callee name from a call node
static char *extract_callee_name(CBMArena *a, TSNode node, const char *source, CBMLanguage lang) {
    // Lean 4: skip type-position applies
    if (lang == CBM_LANG_LEAN && strcmp(ts_node_type(node), "apply") == 0) {
        if (lean_is_in_type_position(node)) {
            return NULL;
        }
    }

    // Try common field-based resolution first
    char *name = extract_callee_from_fields(a, node, source);
    if (name) {
        return name;
    }

    // Language-specific patterns
    name = extract_callee_lang_specific(a, node, source, lang);
    if (name) {
        return name;
    }

    // Generic fallback: first identifier child
    if (ts_node_child_count(node) > 0) {
        TSNode first = ts_node_child(node, 0);
        if (strcmp(ts_node_type(first), "identifier") == 0) {
            return cbm_node_text(a, first, source);
        }
    }

    return NULL;
}

// Strip quotes and validate a string arg. Returns validated text or NULL.
static const char *strip_and_validate_string_arg(CBMArena *a, char *text) {
    if (!text || !text[0]) {
        return NULL;
    }
    int len = (int)strlen(text);
    if (len >= CBM_QUOTE_PAIR && (text[0] == '"' || text[0] == '\'')) {
        text = cbm_arena_strndup(a, text + CBM_QUOTE_OFFSET, (size_t)(len - CBM_QUOTE_PAIR));
        len -= CBM_QUOTE_PAIR;
    }
    if (!text || len <= 0 || len >= MAX_STRING_ARG_LEN) {
        return NULL;
    }
    for (int vi = 0; vi < len; vi++) {
        if ((unsigned char)text[vi] < MIN_PRINTABLE && text[vi] != '\t') {
            return NULL;
        }
    }
    return text;
}

// Extract first string argument from a call's arguments node.
static const char *extract_first_string_arg(CBMExtractCtx *ctx, TSNode args) {
    uint32_t nc = ts_node_named_child_count(args);
    for (uint32_t ai = 0; ai < nc && ai < MAX_POSITIONAL_SCAN; ai++) {
        TSNode arg = ts_node_named_child(args, ai);
        const char *ak = ts_node_type(arg);
        if (is_string_like(ak)) {
            char *text = cbm_node_text(ctx->arena, arg, ctx->source);
            return strip_and_validate_string_arg(ctx->arena, text);
        }
    }
    return NULL;
}

// Walk AST for call nodes (iterative)
#define CALLS_STACK_CAP CBM_SZ_512
static void walk_calls(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    TSNode stack[CALLS_STACK_CAP];
    int top = 0;
    stack[top++] = root;

    while (top > 0) {
        TSNode node = stack[--top];
        const char *kind = ts_node_type(node);

        if (cbm_kind_in_set(node, spec->call_node_types)) {
            char *callee = extract_callee_name(ctx->arena, node, ctx->source, ctx->language);
            if (callee && callee[0] && !cbm_is_keyword(callee, ctx->language)) {
                CBMCall call = {0};
                call.callee_name = callee;
                call.enclosing_func_qn = cbm_enclosing_func_qn_cached(ctx, node);

                TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
                if (!ts_node_is_null(args)) {
                    call.first_string_arg = extract_first_string_arg(ctx, args);
                }
                cbm_calls_push(&ctx->result->calls, ctx->arena, call);
            }
        }

        if (ctx->language == CBM_LANG_TSX || ctx->language == CBM_LANG_JAVASCRIPT) {
            if (strcmp(kind, "jsx_self_closing_element") == 0 ||
                strcmp(kind, "jsx_opening_element") == 0) {
                extract_jsx_refs(ctx, node);
            }
        }

        uint32_t count = ts_node_child_count(node);
        enum { LAST_IDX_OFFSET = 1 };
        for (int i = (int)(count - LAST_IDX_OFFSET); i >= 0 && top < CALLS_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

// Extract JSX component references (uppercase = component, lowercase = HTML)
static void extract_jsx_refs(CBMExtractCtx *ctx, TSNode node) {
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name_node)) {
        return;
    }

    char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (!name || !name[0]) {
        return;
    }

    // Only uppercase names are components
    if (name[0] < 'A' || name[0] > 'Z') {
        return;
    }

    CBMCall call = {0};
    call.callee_name = name;
    call.enclosing_func_qn = cbm_enclosing_func_qn_cached(ctx, node);
    cbm_calls_push(&ctx->result->calls, ctx->arena, call);
}

void cbm_extract_calls(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec || !spec->call_node_types || !spec->call_node_types[0]) {
        return;
    }

    walk_calls(ctx, ctx->root, spec);
}

// --- Unified handler: called once per node by the cursor walk ---

// Process a keyword argument (keyword_argument or pair node).
static void process_keyword_arg(CBMExtractCtx *ctx, TSNode arg_node, CBMCallArg *ca) {
    TSNode key_n = ts_node_child_by_field_name(arg_node, TS_FIELD("name"));
    TSNode val_n = ts_node_child_by_field_name(arg_node, TS_FIELD("value"));
    if (ts_node_is_null(key_n)) {
        key_n = ts_node_child_by_field_name(arg_node, TS_FIELD("key"));
    }
    if (!ts_node_is_null(key_n)) {
        ca->keyword = cbm_node_text(ctx->arena, key_n, ctx->source);
    }
    if (!ts_node_is_null(val_n)) {
        ca->expr = cbm_node_text(ctx->arena, val_n, ctx->source);
        if (strcmp(ts_node_type(val_n), "identifier") == 0 && ca->expr) {
            ca->value = lookup_string_constant(ctx, ca->expr);
        } else if (is_string_like(ts_node_type(val_n)) && ca->expr) {
            ca->value = strip_quotes(ctx->arena, ca->expr);
        }
    }
}

/* Extract all arguments from a call expression into call->args[]. */
static void extract_call_args(CBMExtractCtx *ctx, TSNode args, CBMCall *call) {
    uint32_t argc = ts_node_named_child_count(args);
    int positional_idx = 0;
    for (uint32_t ai = 0; ai < argc && call->arg_count < CBM_MAX_CALL_ARGS; ai++) {
        TSNode arg_node = ts_node_named_child(args, ai);
        const char *ak = ts_node_type(arg_node);
        CBMCallArg *ca = &call->args[call->arg_count];
        memset(ca, 0, sizeof(*ca));

        if (strcmp(ak, "keyword_argument") == 0 || strcmp(ak, "pair") == 0) {
            process_keyword_arg(ctx, arg_node, ca);
            ca->index = positional_idx++;
            call->arg_count++;
        } else if (strcmp(ak, "list_splat") == 0 || strcmp(ak, "dictionary_splat") == 0 ||
                   strcmp(ak, "spread_element") == 0) {
            positional_idx++;
        } else {
            ca->expr = cbm_node_text(ctx->arena, arg_node, ctx->source);
            ca->index = positional_idx++;
            if (is_string_like(ak) && ca->expr) {
                ca->value = strip_quotes(ctx->arena, ca->expr);
            } else if (strcmp(ak, "identifier") == 0 && ca->expr) {
                ca->value = lookup_string_constant(ctx, ca->expr);
            }
            call->arg_count++;
        }
    }
}

// Check if a keyword name matches URL or topic patterns.
static bool is_url_or_topic_keyword(const char *key) {
    static const char *url_keywords[] = {"url",        "endpoint", "path", "uri",
                                         "target_url", "base_url", NULL};
    static const char *topic_keywords[] = {"topic",   "topic_id",   "topic_name",
                                           "queue",   "queue_name", "queue_id",
                                           "subject", "channel",    NULL};
    for (int i = 0; url_keywords[i]; i++) {
        if (strcmp(key, url_keywords[i]) == 0) {
            return true;
        }
    }
    for (int i = 0; topic_keywords[i]; i++) {
        if (strcmp(key, topic_keywords[i]) == 0) {
            return true;
        }
    }
    return false;
}

// Extract string value from a node (literal or constant reference).
static const char *extract_string_value(CBMExtractCtx *ctx, TSNode val_node) {
    const char *vk = ts_node_type(val_node);
    if (is_string_like(vk)) {
        char *text = cbm_node_text(ctx->arena, val_node, ctx->source);
        if (text && text[0]) {
            return strip_quotes(ctx->arena, text);
        }
    } else if (strcmp(vk, "identifier") == 0) {
        char *const_name = cbm_node_text(ctx->arena, val_node, ctx->source);
        if (const_name) {
            return lookup_string_constant(ctx, const_name);
        }
    }
    return NULL;
}

// Try to extract URL/topic from a keyword_argument or pair node.
static const char *extract_keyword_url(CBMExtractCtx *ctx, TSNode arg) {
    TSNode key_node = ts_node_child_by_field_name(arg, TS_FIELD("name"));
    TSNode val_node = ts_node_child_by_field_name(arg, TS_FIELD("value"));
    if (ts_node_is_null(key_node)) {
        key_node = ts_node_child_by_field_name(arg, TS_FIELD("key"));
    }
    if (ts_node_is_null(key_node) || ts_node_is_null(val_node)) {
        return NULL;
    }
    char *key = cbm_node_text(ctx->arena, key_node, ctx->source);
    if (!key || !is_url_or_topic_keyword(key)) {
        return NULL;
    }
    return extract_string_value(ctx, val_node);
}

// Try to extract URL/topic from a positional argument (string or constant).
static const char *extract_positional_url(CBMExtractCtx *ctx, TSNode arg, const char *ak) {
    if (is_string_like(ak)) {
        char *text = cbm_node_text(ctx->arena, arg, ctx->source);
        const char *validated = strip_and_validate_string_arg(ctx->arena, text);
        if (validated) {
            return validated;
        }
    }
    if (strcmp(ak, "identifier") == 0) {
        char *const_name = cbm_node_text(ctx->arena, arg, ctx->source);
        if (const_name) {
            return lookup_string_constant(ctx, const_name);
        }
    }
    return NULL;
}

// Extract URL/topic from keyword or positional args.
static const char *extract_url_or_topic_arg(CBMExtractCtx *ctx, TSNode args) {
    uint32_t nc = ts_node_named_child_count(args);
    for (uint32_t ai = 0; ai < nc; ai++) {
        TSNode arg = ts_node_named_child(args, ai);
        const char *ak = ts_node_type(arg);

        if (strcmp(ak, "keyword_argument") == 0 || strcmp(ak, "pair") == 0) {
            const char *val = extract_keyword_url(ctx, arg);
            if (val) {
                return val;
            }
            continue;
        }

        if (ai < MAX_POSITIONAL_SCAN) {
            const char *val = extract_positional_url(ctx, arg, ak);
            if (val) {
                return val;
            }
        }
    }
    return NULL;
}

// Extract second argument name (handler ref for route registrations).
static const char *extract_handler_arg(CBMExtractCtx *ctx, TSNode args) {
    uint32_t nc = ts_node_named_child_count(args);
    for (uint32_t ai = HANDLER_START_IDX; ai < nc && ai < MAX_HANDLER_SCAN; ai++) {
        TSNode arg2 = ts_node_named_child(args, ai);
        const char *ak2 = ts_node_type(arg2);
        if (strcmp(ak2, "identifier") == 0 || strcmp(ak2, "member_expression") == 0 ||
            strcmp(ak2, "selector_expression") == 0 || strcmp(ak2, "attribute") == 0 ||
            strcmp(ak2, "field_expression") == 0) {
            return cbm_node_text(ctx->arena, arg2, ctx->source);
        }
    }
    return NULL;
}

// Extract JSX component refs (uppercase tags) as CALLS edges.
static void extract_jsx_component_ref(CBMExtractCtx *ctx, TSNode node, const char *kind,
                                      const char *enclosing_func_qn) {
    if (strcmp(kind, "jsx_self_closing_element") != 0 && strcmp(kind, "jsx_opening_element") != 0) {
        return;
    }
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name_node)) {
        return;
    }
    char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
    if (name && name[0] >= 'A' && name[0] <= 'Z') {
        CBMCall call = {0};
        call.callee_name = name;
        call.enclosing_func_qn = enclosing_func_qn;
        cbm_calls_push(&ctx->result->calls, ctx->arena, call);
    }
}

void handle_calls(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state) {
    if (!spec->call_node_types || !spec->call_node_types[0]) {
        return;
    }

    if (cbm_kind_in_set(node, spec->call_node_types)) {
        char *callee = extract_callee_name(ctx->arena, node, ctx->source, ctx->language);
        if (callee && callee[0] && !cbm_is_keyword(callee, ctx->language)) {
            CBMCall call = {0};
            call.callee_name = callee;
            call.enclosing_func_qn = state->enclosing_func_qn;

            TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
            if (!ts_node_is_null(args)) {
                call.first_string_arg = extract_url_or_topic_arg(ctx, args);
                if (call.first_string_arg && call.first_string_arg[0] == '/') {
                    call.second_arg_name = extract_handler_arg(ctx, args);
                }
                extract_call_args(ctx, args, &call);
            }

            cbm_calls_push(&ctx->result->calls, ctx->arena, call);
        }
    }

    if (ctx->language == CBM_LANG_TSX || ctx->language == CBM_LANG_JAVASCRIPT) {
        extract_jsx_component_ref(ctx, node, ts_node_type(node), state->enclosing_func_qn);
    }
}
