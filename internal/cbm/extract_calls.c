#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_sprintf
#include "helpers.h"
#include "lang_specs.h"
#include "extract_unified.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include <stdint.h>          // uint32_t
#include <string.h>
#include <ctype.h>

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
    if (len >= 2 && (text[0] == '"' || text[0] == '\'')) {
        return cbm_arena_strndup(a, text + 1, (size_t)(len - 2));
    }
    return text;
}

// Forward declarations
static void walk_calls(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);
static char *extract_callee_name(CBMArena *a, TSNode node, const char *source, CBMLanguage lang);
static void extract_jsx_refs(CBMExtractCtx *ctx, TSNode node);

static bool gdscript_signal_call_name(const char *callee) {
    if (!callee || !callee[0]) {
        return false;
    }
    if (strcmp(callee, "emit_signal") == 0 || strcmp(callee, ".emit") == 0 ||
        strcmp(callee, ".connect") == 0) {
        return true;
    }
    size_t len = strlen(callee);
    return (len >= 5 && strcmp(callee + (len - 5), ".emit") == 0) ||
           (len >= 8 && strcmp(callee + (len - 8), ".connect") == 0);
}

static bool gdscript_is_identifier_segment(const char *text, size_t len) {
    if (!text || len == 0) {
        return false;
    }
    if (!(text[0] == '_' || isalpha((unsigned char)text[0]))) {
        return false;
    }
    for (size_t i = 1; i < len; i++) {
        unsigned char ch = (unsigned char)text[i];
        if (!(ch == '_' || isalnum(ch))) {
            return false;
        }
    }
    return true;
}

static const char *gdscript_signal_name_from_callee(CBMArena *a, const char *callee) {
    if (!callee || !callee[0]) {
        return NULL;
    }
    size_t len = strlen(callee);
    size_t end = 0;
    if (len >= 5 && strcmp(callee + (len - 5), ".emit") == 0) {
        end = len - 5;
    } else if (len >= 8 && strcmp(callee + (len - 8), ".connect") == 0) {
        end = len - 8;
    } else {
        return NULL;
    }

    const char *segment_start = callee;
    for (size_t i = end; i > 0; i--) {
        if (callee[i - 1] == '.') {
            segment_start = callee + i;
            break;
        }
    }
    size_t seg_start = (size_t)(segment_start - callee);
    if (seg_start >= end) {
        return NULL;
    }
    size_t seg_len = end - seg_start;
    if (!gdscript_is_identifier_segment(segment_start, seg_len)) {
        return NULL;
    }
    return cbm_arena_strndup(a, segment_start, seg_len);
}

static bool gdscript_callee_receiver_has_dot(const char *callee, size_t suffix_len) {
    if (!callee) {
        return false;
    }
    size_t len = strlen(callee);
    if (len <= suffix_len) {
        return false;
    }
    size_t receiver_len = len - suffix_len;
    for (size_t i = 0; i < receiver_len; i++) {
        if (callee[i] == '.') {
            return true;
        }
    }
    return false;
}

static bool gdscript_identifier_is_flag_constant(const char *text) {
    if (!text || !text[0]) {
        return false;
    }
    bool saw_alpha = false;
    for (size_t i = 0; text[i]; i++) {
        unsigned char ch = (unsigned char)text[i];
        if (isalpha(ch)) {
            saw_alpha = true;
            if (!isupper(ch)) {
                return false;
            }
        } else if (!(isdigit(ch) || ch == '_')) {
            return false;
        }
    }
    return saw_alpha;
}

static char *gdscript_callee_from_text(CBMArena *a, const char *raw) {
    if (!raw || !raw[0]) {
        return NULL;
    }

    size_t len = strlen(raw);
    while (len > 0 && isspace((unsigned char)raw[len - 1])) {
        len--;
    }
    if (len == 0) {
        return NULL;
    }

    // Normal case for a call expression: trim only the outer call arguments,
    // preserving any parentheses in the receiver chain (e.g. get_tree().root.foo()).
    if (raw[len - 1] == ')') {
        int depth = 0;
        bool in_single = false;
        bool in_double = false;
        bool escape = false;
        for (size_t i = len; i > 0; i--) {
            char ch = raw[i - 1];
            if (escape) {
                escape = false;
                continue;
            }
            if ((in_single || in_double) && ch == '\\') {
                escape = true;
                continue;
            }
            if (!in_double && ch == '\'' && !escape) {
                in_single = !in_single;
                continue;
            }
            if (!in_single && ch == '"' && !escape) {
                in_double = !in_double;
                continue;
            }
            if (in_single || in_double) {
                continue;
            }
            if (ch == ')') {
                depth++;
            } else if (ch == '(') {
                depth--;
                if (depth == 0) {
                    size_t callee_len = i - 1;
                    while (callee_len > 0 && isspace((unsigned char)raw[callee_len - 1])) {
                        callee_len--;
                    }
                    if (callee_len > 0) {
                        return cbm_arena_strndup(a, raw, callee_len);
                    }
                    break;
                }
            }
        }
    }

    const char *lparen = strchr(raw, '(');
    size_t fallback_len = lparen ? (size_t)(lparen - raw) : len;
    while (fallback_len > 0 && isspace((unsigned char)raw[fallback_len - 1])) {
        fallback_len--;
    }
    return fallback_len > 0 ? cbm_arena_strndup(a, raw, fallback_len) : NULL;
}

// Lean 4: check if an apply node is inside a type annotation.
// Strategy: walk up to the nearest declaration boundary; if the apply falls
// inside that declaration's explicit_binder/implicit_binder, or before the
// body field, it's a type annotation. We check byte ranges: a call is valid
// only if it overlaps the body range of the enclosing declaration.
static bool lean_is_in_type_position(TSNode node) {
    TSNode cur = ts_node_parent(node);
    for (int depth = 0; depth < 20; depth++) {
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
            TSNode type_field = ts_node_child_by_field_name(cur, "type", 4);
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

// Extract callee name from a call node
static char *extract_callee_name(CBMArena *a, TSNode node, const char *source, CBMLanguage lang) {
    // Lean 4: apply — name field is callee. Skip if in a type annotation position.
    // Must be checked before the generic "name" field handler below.
    if (lang == CBM_LANG_LEAN && strcmp(ts_node_type(node), "apply") == 0) {
        if (lean_is_in_type_position(node)) {
            return NULL;
        }
        // Fall through to generic handler
    }

    if (lang == CBM_LANG_GDSCRIPT) {
        const char *nk = ts_node_type(node);
        if (strcmp(nk, "call") == 0 || strcmp(nk, "attribute_call") == 0) {
            TSNode text_node = node;
            if (strcmp(nk, "attribute_call") == 0) {
                TSNode parent = ts_node_parent(node);
                if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "attribute") == 0) {
                    text_node = parent;
                }
            }

            char *raw = cbm_node_text(a, text_node, source);
            if (raw && raw[0]) {
                char *callee = gdscript_callee_from_text(a, raw);
                if (callee && callee[0]) {
                    return callee;
                }
            }
        }

        if (ts_node_named_child_count(node) > 0) {
            TSNode first = ts_node_named_child(node, 0);
            const char *fk = ts_node_type(first);
            if (strcmp(fk, "arguments") != 0) {
                char *raw = cbm_node_text(a, first, source);
                if (raw && raw[0]) {
                    return raw;
                }
            }
        }

        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        TSNode obj = ts_node_child_by_field_name(node, "object", 6);
        if (!ts_node_is_null(name_node)) {
            char *name = cbm_node_text(a, name_node, source);
            if (!ts_node_is_null(obj) && name && name[0]) {
                char *obj_text = cbm_node_text(a, obj, source);
                if (obj_text && obj_text[0]) {
                    return cbm_arena_sprintf(a, "%s.%s", obj_text, name);
                }
            }
            if (name && name[0]) {
                return name;
            }
        }

        TSNode function_node = ts_node_child_by_field_name(node, "function", 8);
        if (!ts_node_is_null(function_node)) {
            char *raw = cbm_node_text(a, function_node, source);
            if (raw && raw[0]) {
                return raw;
            }
        }
    }

    // Try "function" field (most languages: call_expression, etc.)
    TSNode func_node = ts_node_child_by_field_name(node, "function", 8);
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
    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
    if (!ts_node_is_null(name_node)) {
        char *name = cbm_node_text(a, name_node, source);
        // For Java: prepend object if present
        TSNode obj = ts_node_child_by_field_name(node, "object", 6);
        if (!ts_node_is_null(obj) && name) {
            char *obj_text = cbm_node_text(a, obj, source);
            if (obj_text && obj_text[0]) {
                return cbm_arena_sprintf(a, "%s.%s", obj_text, name);
            }
        }
        return name;
    }

    // Ruby: "method" + "receiver" fields
    TSNode method_node = ts_node_child_by_field_name(node, "method", 6);
    if (!ts_node_is_null(method_node)) {
        char *method = cbm_node_text(a, method_node, source);
        TSNode recv = ts_node_child_by_field_name(node, "receiver", 8);
        if (!ts_node_is_null(recv) && method) {
            char *recv_text = cbm_node_text(a, recv, source);
            if (recv_text && recv_text[0]) {
                return cbm_arena_sprintf(a, "%s.%s", recv_text, method);
            }
        }
        return method;
    }

    // ObjC message_expression: [receiver message]
    if (lang == CBM_LANG_OBJC && strcmp(ts_node_type(node), "message_expression") == 0) {
        TSNode selector = ts_node_child_by_field_name(node, "selector", 8);
        if (!ts_node_is_null(selector)) {
            return cbm_node_text(a, selector, source);
        }
    }

    // Erlang: call -> first child is module:function or just function
    if (lang == CBM_LANG_ERLANG && strcmp(ts_node_type(node), "call") == 0) {
        if (ts_node_child_count(node) > 0) {
            return cbm_node_text(a, ts_node_child(node, 0), source);
        }
    }

    // Haskell/OCaml: application_expression, infix, apply
    if (lang == CBM_LANG_HASKELL || lang == CBM_LANG_OCAML) {
        const char *nk = ts_node_type(node);
        if (strcmp(nk, "apply") == 0 || strcmp(nk, "application_expression") == 0) {
            if (ts_node_child_count(node) > 0) {
                TSNode callee = ts_node_child(node, 0);
                if (strcmp(ts_node_type(callee), "identifier") == 0 ||
                    strcmp(ts_node_type(callee), "variable") == 0 ||
                    strcmp(ts_node_type(callee), "constructor") == 0 ||
                    strcmp(ts_node_type(callee), "value_path") == 0) {
                    return cbm_node_text(a, callee, source);
                }
            }
        }
        if (strcmp(nk, "infix") == 0 || strcmp(nk, "infix_expression") == 0) {
            TSNode op = ts_node_child_by_field_name(node, "operator", 8);
            if (!ts_node_is_null(op)) {
                return cbm_node_text(a, op, source);
            }
            // Fallback: second child is usually the operator
            if (ts_node_child_count(node) >= 3) {
                return cbm_node_text(a, ts_node_child(node, 1), source);
            }
        }
    }

    // Elixir: first child of call is the function name
    if (lang == CBM_LANG_ELIXIR && strcmp(ts_node_type(node), "call") == 0) {
        if (ts_node_child_count(node) > 0) {
            TSNode first = ts_node_child(node, 0);
            const char *fk = ts_node_type(first);
            if (strcmp(fk, "identifier") == 0 || strcmp(fk, "dot") == 0) {
                return cbm_node_text(a, first, source);
            }
        }
    }

    // Perl: various call expression types
    if (lang == CBM_LANG_PERL) {
        if (ts_node_child_count(node) > 0) {
            return cbm_node_text(a, ts_node_child(node, 0), source);
        }
    }

    // PHP: function_call_expression, member_call_expression, etc.
    if (lang == CBM_LANG_PHP) {
        func_node = ts_node_child_by_field_name(node, "function", 8);
        if (ts_node_is_null(func_node)) {
            func_node = ts_node_child_by_field_name(node, "name", 4);
        }
        if (!ts_node_is_null(func_node)) {
            return cbm_node_text(a, func_node, source);
        }
    }

    // Kotlin: navigation_expression, call_expression
    if (lang == CBM_LANG_KOTLIN) {
        if (ts_node_child_count(node) > 0) {
            return cbm_node_text(a, ts_node_child(node, 0), source);
        }
    }

    // MATLAB: command node — first child is command_name (not identifier)
    if (lang == CBM_LANG_MATLAB && strcmp(ts_node_type(node), "command") == 0) {
        if (ts_node_child_count(node) > 0) {
            return cbm_node_text(a, ts_node_child(node, 0), source);
        }
    }

    // Wolfram: apply — first named child is callee (user_symbol or builtin_symbol)
    // Skip if this apply is the LHS of a set/set_delayed definition (top or nested)
    if (lang == CBM_LANG_WOLFRAM && strcmp(ts_node_type(node), "apply") == 0) {
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

    // Swift: call_expression has no "function" field — callee is first named child.
    // Also handle constructor_expression for MyClass() syntax.
    if (lang == CBM_LANG_SWIFT) {
        const char *nk = ts_node_type(node);
        if (strcmp(nk, "call_expression") == 0 || strcmp(nk, "constructor_expression") == 0) {
            if (ts_node_named_child_count(node) > 0) {
                TSNode callee = ts_node_named_child(node, 0);
                const char *ck = ts_node_type(callee);
                if (strcmp(ck, "simple_identifier") == 0 ||
                    strcmp(ck, "navigation_expression") == 0) {
                    return cbm_node_text(a, callee, source);
                }
            }
        }
        return NULL;
    }

    // Generic fallback: first child
    if (ts_node_child_count(node) > 0) {
        TSNode first = ts_node_child(node, 0);
        if (strcmp(ts_node_type(first), "identifier") == 0) {
            return cbm_node_text(a, first, source);
        }
    }

    return NULL;
}

// Walk AST for call nodes
// NOLINTNEXTLINE(misc-no-recursion) — intentional AST tree walk
static void walk_calls(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    const char *kind = ts_node_type(node);

    if (cbm_kind_in_set(node, spec->call_node_types)) {
        char *callee = extract_callee_name(ctx->arena, node, ctx->source, ctx->language);
        if (callee && callee[0]) {
            // Skip keywords
            if (!cbm_is_keyword(callee, ctx->language)) {
                CBMCall call = {0};
                call.callee_name = callee;
                call.enclosing_func_qn = cbm_enclosing_func_qn_cached(ctx, node);
                call.first_string_arg = NULL;

                /* Extract first string literal argument (URL, topic, key) */
                TSNode args = ts_node_child_by_field_name(node, "arguments", 9);
                if (!ts_node_is_null(args)) {
                    uint32_t nc = ts_node_named_child_count(args);
                    for (uint32_t ai = 0; ai < nc && ai < 3; ai++) {
                        TSNode arg = ts_node_named_child(args, ai);
                        const char *ak = ts_node_type(arg);
                        if (strcmp(ak, "string") == 0 || strcmp(ak, "string_literal") == 0 ||
                            strcmp(ak, "interpreted_string_literal") == 0 ||
                            strcmp(ak, "raw_string_literal") == 0 ||
                            strcmp(ak, "string_content") == 0) {
                            char *text = cbm_node_text(ctx->arena, arg, ctx->source);
                            if (text && text[0]) {
                                /* Strip quotes */
                                int len = (int)strlen(text);
                                if (len >= 2 && (text[0] == '"' || text[0] == '\'')) {
                                    text =
                                        cbm_arena_strndup(ctx->arena, text + 1, (size_t)(len - 2));
                                    len -= 2;
                                }
                                /* Validate: must be printable ASCII, no control chars */
                                // NOLINTNEXTLINE(readability-implicit-bool-conversion)
                                bool valid = (text != NULL && len > 0 && len < 512);
                                for (int vi = 0; vi < len && valid; vi++) {
                                    unsigned char ch = (unsigned char)text[vi];
                                    if (ch < 0x20 && ch != '\t') {
                                        valid = false;
                                    }
                                }
                                if (valid) {
                                    call.first_string_arg = text;
                                }
                            }
                            break;
                        }
                    }
                }

                cbm_calls_push(&ctx->result->calls, ctx->arena, call);
            }
        }
        // Don't recurse into call arguments for nested calls — the walk handles that
    }

    // JSX component refs (TSX/JSX)
    if (ctx->language == CBM_LANG_TSX || ctx->language == CBM_LANG_JAVASCRIPT) {
        if (strcmp(kind, "jsx_self_closing_element") == 0 ||
            strcmp(kind, "jsx_opening_element") == 0) {
            extract_jsx_refs(ctx, node);
        }
    }

    // Recurse
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        walk_calls(ctx, ts_node_child(node, i), spec);
    }
}

// Extract JSX component references (uppercase = component, lowercase = HTML)
static void extract_jsx_refs(CBMExtractCtx *ctx, TSNode node) {
    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
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

/* Extract all arguments from a call expression into call->args[].
 * Captures expression text, resolved constants, keyword names, and
 * dotted field chains (member_expression → "payload.info.url"). */
static void extract_call_args(CBMExtractCtx *ctx, TSNode args, CBMCall *call) {
    uint32_t argc = ts_node_named_child_count(args);
    int positional_idx = 0;
    for (uint32_t ai = 0; ai < argc && call->arg_count < CBM_MAX_CALL_ARGS; ai++) {
        TSNode arg_node = ts_node_named_child(args, ai);
        const char *ak = ts_node_type(arg_node);
        CBMCallArg *ca = &call->args[call->arg_count];
        memset(ca, 0, sizeof(*ca));

        if (strcmp(ak, "keyword_argument") == 0 || strcmp(ak, "pair") == 0) {
            TSNode key_n = ts_node_child_by_field_name(arg_node, "name", 4);
            TSNode val_n = ts_node_child_by_field_name(arg_node, "value", 5);
            if (ts_node_is_null(key_n)) {
                key_n = ts_node_child_by_field_name(arg_node, "key", 3);
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

void handle_calls(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state) {
    if (!spec->call_node_types || !spec->call_node_types[0]) {
        return;
    }

    const char *kind = ts_node_type(node);

    if (cbm_kind_in_set(node, spec->call_node_types)) {
        char *callee = extract_callee_name(ctx->arena, node, ctx->source, ctx->language);
        if (callee && callee[0] && !cbm_is_keyword(callee, ctx->language)) {
            CBMCall call = {0};
            call.callee_name = callee;
            call.enclosing_func_qn = state->enclosing_func_qn;

            /* Extract URL/topic/key from call arguments.
             * Strategy: check keyword args first (url=, topic_id=, queue=),
             * then first positional string, then resolve constant references. */
            TSNode args = ts_node_child_by_field_name(node, "arguments", 9);
            if (!ts_node_is_null(args)) {
                /* Keyword patterns that indicate URL/topic/key */
                static const char *url_keywords[] = {"url",        "endpoint", "path", "uri",
                                                     "target_url", "base_url", NULL};
                static const char *topic_keywords[] = {"topic",   "topic_id",   "topic_name",
                                                       "queue",   "queue_name", "queue_id",
                                                       "subject", "channel",    NULL};

                uint32_t nc = ts_node_named_child_count(args);
                for (uint32_t ai = 0; ai < nc && !call.first_string_arg; ai++) {
                    TSNode arg = ts_node_named_child(args, ai);
                    const char *ak = ts_node_type(arg);

                    /* Check keyword_argument nodes: url="...", topic_id="..." */
                    if (strcmp(ak, "keyword_argument") == 0 || strcmp(ak, "pair") == 0) {
                        TSNode key_node = ts_node_child_by_field_name(arg, "name", 4);
                        TSNode val_node = ts_node_child_by_field_name(arg, "value", 5);
                        if (ts_node_is_null(key_node)) {
                            key_node = ts_node_child_by_field_name(arg, "key", 3);
                        }
                        if (ts_node_is_null(key_node) || ts_node_is_null(val_node)) {
                            continue;
                        }

                        char *key = cbm_node_text(ctx->arena, key_node, ctx->source);
                        if (!key) {
                            continue;
                        }

                        /* Check if key matches URL or topic patterns */
                        bool is_url_kw = false;
                        bool is_topic_kw = false;
                        for (int ki = 0; url_keywords[ki]; ki++) {
                            if (strcmp(key, url_keywords[ki]) == 0) {
                                is_url_kw = true;
                                break;
                            }
                        }
                        if (!is_url_kw) {
                            for (int ki = 0; topic_keywords[ki]; ki++) {
                                if (strcmp(key, topic_keywords[ki]) == 0) {
                                    is_topic_kw = true;
                                    break;
                                }
                            }
                        }
                        if (!is_url_kw && !is_topic_kw) {
                            continue;
                        }

                        /* Extract value — string literal or constant reference */
                        const char *vk = ts_node_type(val_node);
                        if (strcmp(vk, "string") == 0 || strcmp(vk, "string_literal") == 0 ||
                            strcmp(vk, "interpreted_string_literal") == 0 ||
                            strcmp(vk, "raw_string_literal") == 0) {
                            char *text = cbm_node_text(ctx->arena, val_node, ctx->source);
                            if (text && text[0]) {
                                int len = (int)strlen(text);
                                if (len >= 2 && (text[0] == '"' || text[0] == '\'')) {
                                    text =
                                        cbm_arena_strndup(ctx->arena, text + 1, (size_t)(len - 2));
                                }
                                if (text && text[0]) {
                                    call.first_string_arg = text;
                                }
                            }
                        } else if (strcmp(vk, "identifier") == 0) {
                            /* Constant reference: url=MY_URL_CONSTANT */
                            char *const_name = cbm_node_text(ctx->arena, val_node, ctx->source);
                            if (const_name) {
                                const char *resolved = lookup_string_constant(ctx, const_name);
                                if (resolved) {
                                    call.first_string_arg = resolved;
                                }
                            }
                        }
                        continue;
                    }

                    /* First positional string argument (fallback) */
                    if (ai < 3) {
                        if (strcmp(ak, "string") == 0 || strcmp(ak, "string_literal") == 0 ||
                            strcmp(ak, "interpreted_string_literal") == 0 ||
                            strcmp(ak, "raw_string_literal") == 0) {
                            char *text = cbm_node_text(ctx->arena, arg, ctx->source);
                            if (text && text[0]) {
                                int len = (int)strlen(text);
                                if (len >= 2 && (text[0] == '"' || text[0] == '\'')) {
                                    text =
                                        cbm_arena_strndup(ctx->arena, text + 1, (size_t)(len - 2));
                                    len -= 2;
                                }
                                /* Validate printable */
                                // NOLINTNEXTLINE(readability-implicit-bool-conversion)
                                bool valid = (text != NULL && len > 0 && len < 512);
                                for (int vi = 0; vi < len && valid; vi++) {
                                    if ((unsigned char)text[vi] < 0x20 && text[vi] != '\t') {
                                        valid = false;
                                    }
                                }
                                if (valid) {
                                    call.first_string_arg = text;
                                }
                            }
                            break;
                        }
                        /* Positional constant reference: create_task(MY_URL) */
                        if (strcmp(ak, "identifier") == 0) {
                            char *const_name = cbm_node_text(ctx->arena, arg, ctx->source);
                            if (const_name) {
                                const char *resolved = lookup_string_constant(ctx, const_name);
                                if (resolved) {
                                    call.first_string_arg = resolved;
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            /* Extract second argument name (handler ref for route registrations). */
            if (call.first_string_arg != NULL && call.first_string_arg[0] == '/' &&
                !ts_node_is_null(args)) {
                uint32_t nc2 = ts_node_named_child_count(args);
                for (uint32_t ai = 1; ai < nc2 && ai < 4 && !call.second_arg_name; ai++) {
                    TSNode arg2 = ts_node_named_child(args, ai);
                    const char *ak2 = ts_node_type(arg2);
                    if (strcmp(ak2, "identifier") == 0 || strcmp(ak2, "member_expression") == 0 ||
                        strcmp(ak2, "selector_expression") == 0 || strcmp(ak2, "attribute") == 0 ||
                        strcmp(ak2, "field_expression") == 0) {
                        call.second_arg_name = cbm_node_text(ctx->arena, arg2, ctx->source);
                    }
                }
            }

            if (ctx->language == CBM_LANG_GDSCRIPT && gdscript_signal_call_name(call.callee_name)) {
                const char *signal_name =
                    gdscript_signal_name_from_callee(ctx->arena, call.callee_name);
                size_t callee_len = strlen(call.callee_name);
                bool callee_is_emit =
                    (strcmp(call.callee_name, ".emit") == 0) ||
                    (callee_len >= 5 && strcmp(call.callee_name + (callee_len - 5), ".emit") == 0);
                bool callee_is_connect =
                    (strcmp(call.callee_name, ".connect") == 0) ||
                    (callee_len >= 8 &&
                     strcmp(call.callee_name + (callee_len - 8), ".connect") == 0);
                if (strcmp(call.callee_name, "emit_signal") == 0) {
                    if (!call.first_string_arg && !ts_node_is_null(args) &&
                        ts_node_named_child_count(args) > 0) {
                        TSNode first = ts_node_named_child(args, 0);
                        const char *fk = ts_node_type(first);
                        if (is_string_like(fk)) {
                            char *text = cbm_node_text(ctx->arena, first, ctx->source);
                            if (text && text[0]) {
                                call.first_string_arg = strip_quotes(ctx->arena, text);
                            }
                        }
                    }
                } else if (callee_is_emit && signal_name) {
                    /* Receiver-form signal emit metadata should prefer receiver signal. */
                    call.first_string_arg = signal_name;
                } else if (!call.first_string_arg && signal_name) {
                    bool allow_receiver_signal_infer = true;
                    if (callee_is_connect && !ts_node_is_null(args) &&
                        ts_node_named_child_count(args) > 0) {
                        uint32_t argc = ts_node_named_child_count(args);
                        TSNode first = ts_node_named_child(args, 0);
                        const char *fk = ts_node_type(first);
                        if (is_string_like(fk)) {
                            allow_receiver_signal_infer = false;
                        } else if (strcmp(fk, "identifier") == 0 || strcmp(fk, "name") == 0 ||
                                   strcmp(fk, "type_identifier") == 0) {
                            /*
                             * Suppress only when arg0 is likely explicit signal naming in
                             * object.connect(signal_name, ...). Keep receiver-derived metadata
                             * for explicit signal receivers like enemy.died.connect(...).
                             */
                            if (argc >= 2 &&
                                !gdscript_callee_receiver_has_dot(call.callee_name, 8)) {
                                bool second_arg_is_flag = false;
                                TSNode second = ts_node_named_child(args, 1);
                                const char *sk = ts_node_type(second);
                                if (strcmp(sk, "identifier") == 0 || strcmp(sk, "name") == 0 ||
                                    strcmp(sk, "type_identifier") == 0) {
                                    char *second_text =
                                        cbm_node_text(ctx->arena, second, ctx->source);
                                    second_arg_is_flag =
                                        gdscript_identifier_is_flag_constant(second_text);
                                }
                                if (!second_arg_is_flag) {
                                    allow_receiver_signal_infer = false;
                                }
                            }
                        }
                    }
                    if (allow_receiver_signal_infer) {
                        call.first_string_arg = signal_name;
                    }
                }
            }

            /* B2+B3: Capture all arguments with expressions + field chains. */
            if (!ts_node_is_null(args)) {
                extract_call_args(ctx, args, &call);
            }

            cbm_calls_push(&ctx->result->calls, ctx->arena, call);
        }
    }

    // JSX component refs (TSX/JSX)
    if (ctx->language == CBM_LANG_TSX || ctx->language == CBM_LANG_JAVASCRIPT) {
        if (strcmp(kind, "jsx_self_closing_element") == 0 ||
            strcmp(kind, "jsx_opening_element") == 0) {
            TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
            if (!ts_node_is_null(name_node)) {
                char *name = cbm_node_text(ctx->arena, name_node, ctx->source);
                if (name && name[0] >= 'A' && name[0] <= 'Z') {
                    CBMCall call = {0};
                    call.callee_name = name;
                    call.enclosing_func_qn = state->enclosing_func_qn;
                    cbm_calls_push(&ctx->result->calls, ctx->arena, call);
                }
            }
        }
    }
}
