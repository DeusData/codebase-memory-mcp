#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_sprintf
#include "helpers.h"
#include "lang_specs.h"
#include "extract_unified.h"
#include "foundation/constants.h"
#include "extract_node_stack.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include <stdint.h>          // uint32_t
#include <stdio.h>
#include <stdlib.h>
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

// Callee suffixes for IRIS Python interop string-dispatch. Kept at file scope
// (not inside the function) to satisfy cppcheck variableScope.
static const char *s_py_dispatch_suffixes[] = {".classMethodValue", ".classMethodVoid",
                                               ".classMethodBoolean", ".classMethodObject", NULL};

// Per-language callee-suffix dispatch table — returns a NULL-terminated list of
// method-name suffixes whose calls should be resolved by extracting class+method
// from the first two string arguments (e.g. IRIS Python interop). Kept here
// rather than in CBMLangSpec to avoid -Wmissing-field-initializers across 155
// language rows.
const char **cbm_string_dispatch_suffixes(CBMLanguage lang) {
    if (lang == CBM_LANG_PYTHON) {
        return s_py_dispatch_suffixes;
    }
    return NULL;
}

// Forward declarations
static char *extract_callee_name(CBMArena *a, TSNode node, const char *source, CBMLanguage lang);
static char *gotemplate_callee(CBMArena *a, TSNode node, const char *source);

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

/* Resolve a selector_expression that may chain through call_expressions.
 * Go pattern: pb.NewFooClient(conn).GetBar → "pb.NewFooClient.GetBar"
 * Without this, cbm_node_text returns full text including args/parens.
 * Iteratively walks the chain: selector → operand(call) → function(selector) → ... */
static char *resolve_chained_selector(CBMArena *a, TSNode sel, const char *source) {
    TSNode operand = ts_node_child_by_field_name(sel, TS_FIELD("operand"));
    TSNode field = ts_node_child_by_field_name(sel, TS_FIELD("field"));
    if (ts_node_is_null(operand) || ts_node_is_null(field) ||
        strcmp(ts_node_type(operand), "call_expression") != 0) {
        return cbm_node_text(a, sel, source);
    }

    /* Operand is a call_expression — extract its callee iteratively.
     * Walk: call_expression → function field → if selector_expression, repeat. */
    char *method = cbm_node_text(a, field, source);
    TSNode inner = operand;
    enum { MAX_CHAIN_DEPTH = 4 };
    for (int depth = 0; depth < MAX_CHAIN_DEPTH; depth++) {
        TSNode fn = ts_node_child_by_field_name(inner, TS_FIELD("function"));
        if (ts_node_is_null(fn)) {
            break;
        }
        const char *fnk = ts_node_type(fn);
        if (strcmp(fnk, "selector_expression") == 0) {
            /* Check if this selector also chains through a call */
            TSNode inner_op = ts_node_child_by_field_name(fn, TS_FIELD("operand"));
            if (!ts_node_is_null(inner_op) &&
                strcmp(ts_node_type(inner_op), "call_expression") == 0) {
                inner = inner_op;
                continue;
            }
        }
        /* Reached a non-chained callee — extract its text */
        char *base = cbm_node_text(a, fn, source);
        if (base && method) {
            return cbm_arena_sprintf(a, "%s.%s", base, method);
        }
        return method;
    }

    /* Fallback: just return the method name */
    return method;
}

// Strip a trailing generic argument list ("<...>" / "[...]") from a type name,
// returning the bare type identifier. Mutates an arena-owned copy in place.
static char *strip_generic_args(char *t) {
    if (!t) {
        return NULL;
    }
    char *angle = strchr(t, '<');
    if (angle) {
        *angle = '\0';
    }
    char *brack = strchr(t, '[');
    if (brack) {
        *brack = '\0';
    }
    return t;
}

// Pull the constructed type name out of a constructor/instantiation node:
//   new_expression               (TS/JS)  -> `constructor`/`type` field or first type child
//   object_creation_expression   (Java/C#/PHP) -> `type` field or first type child
//   instance_expression          (Scala)  -> nested type in the wrapped type/call
// Returns the bare type name (generic args stripped) or NULL if not a
// constructor node / no type found. Constructor calls resolve to the class's
// constructor (or the class node) downstream, producing a CALLS edge.
static char *extract_constructor_callee(CBMArena *a, TSNode node, const char *source,
                                        const char *nk) {
    if (strcmp(nk, "new_expression") != 0 && strcmp(nk, "object_creation_expression") != 0 &&
        strcmp(nk, "instance_expression") != 0) {
        return NULL;
    }

    // Preferred: explicit fields used by the various grammars.
    static const char *type_fields[] = {"constructor", "type", "name", NULL};
    for (const char **f = type_fields; *f; f++) {
        TSNode tn = ts_node_child_by_field_name(node, *f, (uint32_t)strlen(*f));
        if (!ts_node_is_null(tn)) {
            const char *tk = ts_node_type(tn);
            // For a generic_type wrapper, descend to the bare name child.
            if (strcmp(tk, "generic_type") == 0 && ts_node_named_child_count(tn) > 0) {
                tn = ts_node_named_child(tn, 0);
            }
            char *t = strip_generic_args(cbm_node_text(a, tn, source));
            if (t && t[0]) {
                return t;
            }
        }
    }

    // Fallback: first type-like named child (covers grammars that don't expose
    // a field, e.g. Scala's instance_expression wraps the type directly).
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "type_identifier") == 0 || strcmp(ck, "identifier") == 0 ||
            strcmp(ck, "qualified_name") == 0 || strcmp(ck, "scoped_type_identifier") == 0 ||
            strcmp(ck, "qualified_identifier") == 0 || strcmp(ck, "name") == 0 ||
            strcmp(ck, "type") == 0 || strcmp(ck, "generic_type") == 0 ||
            strcmp(ck, "simple_type") == 0 || strcmp(ck, "stable_type_identifier") == 0 ||
            strcmp(ck, "user_type") == 0) {
            // Descend through a generic_type wrapper to the bare name.
            if (strcmp(ck, "generic_type") == 0 && ts_node_named_child_count(child) > 0) {
                child = ts_node_named_child(child, 0);
            }
            char *t = strip_generic_args(cbm_node_text(a, child, source));
            if (t && t[0]) {
                return t;
            }
        }
    }
    return NULL;
}

// Try common field-based callee resolution (function, name, method fields).
static char *extract_callee_from_fields(CBMArena *a, TSNode node, const char *source) {
    // Try "function" field
    TSNode func_node = ts_node_child_by_field_name(node, TS_FIELD("function"));
    if (!ts_node_is_null(func_node)) {
        const char *fk = ts_node_type(func_node);
        if (strcmp(fk, "selector_expression") == 0) {
            return resolve_chained_selector(a, func_node, source);
        }
        if (strcmp(fk, "identifier") == 0 || strcmp(fk, "simple_identifier") == 0 ||
            strcmp(fk, "attribute") == 0 || strcmp(fk, "member_expression") == 0 ||
            strcmp(fk, "field_expression") == 0 || strcmp(fk, "dot") == 0 ||
            strcmp(fk, "function") == 0 || strcmp(fk, "dotted_identifier") == 0 ||
            strcmp(fk, "member_access_expression") == 0 || strcmp(fk, "scoped_identifier") == 0 ||
            strcmp(fk, "qualified_identifier") == 0 ||
            /* ReScript: call_expression `function` field is a value_identifier
             * (or value_identifier_path for module-qualified calls). */
            strcmp(fk, "value_identifier") == 0 || strcmp(fk, "value_identifier_path") == 0) {
            return cbm_node_text(a, func_node, source);
        }
        // R member call: module$fn() — function node is an extract_operator
        // with lhs (object) and rhs (method). Emit "module.fn" so it resolves
        // like other member calls (#219). Previously dropped → no CALLS edge.
        if (strcmp(fk, "extract_operator") == 0) {
            TSNode lhs = ts_node_child_by_field_name(func_node, TS_FIELD("lhs"));
            TSNode rhs = ts_node_child_by_field_name(func_node, TS_FIELD("rhs"));
            if (!ts_node_is_null(rhs)) {
                char *rt = cbm_node_text(a, rhs, source);
                if (!ts_node_is_null(lhs)) {
                    char *lt = cbm_node_text(a, lhs, source);
                    if (lt && lt[0] && rt && rt[0]) {
                        return cbm_arena_sprintf(a, "%s.%s", lt, rt);
                    }
                }
                return rt;
            }
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

// A Perl sub/method name is an identifier: it starts with a letter or '_',
// contains only [A-Za-z0-9_] plus the '::' package separator, and is never a
// string/config literal. tree-sitter-perl mis-parses config lines in .cgi /
// heredoc-heavy files into call-shaped nodes whose "callee" is a dotted config
// token (e.g. "log4perl.appender.File.utf8"); rejecting non-identifier text
// here stops those from becoming bogus CALLS edges. Any '.', whitespace, quote,
// or '/' disqualifies the token.
static bool perl_is_identifier_callee(const char *name) {
    if (!name || !name[0]) {
        return false;
    }
    unsigned char c0 = (unsigned char)name[0];
    if (!(isalpha(c0) || c0 == '_')) {
        return false;
    }
    for (const char *p = name; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (isalnum(c) || c == '_') {
            continue;
        }
        if (c == ':') {
            // Only the '::' package separator is allowed: require an adjacent
            // pair, and reject a lone ':', ':::', or a trailing '::'.
            if (p[1] != ':' || p[2] == ':' || p[2] == '\0') {
                return false;
            }
            p++; // consume the second ':'; the loop's p++ moves past the pair
            continue;
        }
        return false; // '.', space, quote, '/', etc. → not a sub/method name
    }
    return true;
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
        // Pull the actual sub/method name token rather than blindly taking
        // child(0). Grammar (verified against the vendored parser):
        //   method_call_expression   : field `method`   ($obj->m / Class->m)
        //   function_call_expression : field `function` (foo(); name with '.'
        //                              from a config-string misparse lands here)
        //   ambiguous_function_call_expression : field `function`
        //   func1op_call_expression  : builtin keyword as child(0) (no field)
        TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("method"));
        if (ts_node_is_null(name_node)) {
            name_node = ts_node_child_by_field_name(node, TS_FIELD("function"));
        }
        if (ts_node_is_null(name_node)) {
            name_node = ts_node_child(node, 0);
        }
        char *pn = cbm_node_text(a, name_node, source);
        // Reject anything that is not a bare Perl sub/method identifier (config
        // strings, quoted literals, paths) so no spurious CALLS edge is emitted.
        return perl_is_identifier_callee(pn) ? pn : NULL;
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

// Lisp dialects: a call is a list (`list` / `list_lit`) whose head (first named
// child) is the function symbol (`symbol` / `sym_lit`). Generic field/first-child
// extraction misses it because the head is not an `identifier` node.
static char *extract_lisp_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "list") != 0 && strcmp(nk, "list_lit") != 0) {
        return NULL;
    }
    if (ts_node_named_child_count(node) > 0) {
        TSNode head = ts_node_named_child(node, 0);
        const char *hk = ts_node_type(head);
        if (strcmp(hk, "symbol") == 0 || strcmp(hk, "sym_lit") == 0 ||
            strcmp(hk, "identifier") == 0) {
            return cbm_node_text(a, head, source);
        }
    }
    return NULL;
}

// F#: application_expression head is a long_identifier_or_op wrapper, not a bare
// identifier, so extract_fp_callee's accepted-type list would miss it.
static char *extract_fsharp_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "application_expression") != 0 || ts_node_named_child_count(node) == 0) {
        return NULL;
    }
    TSNode head = ts_node_named_child(node, 0);
    const char *hk = ts_node_type(head);
    if (strcmp(hk, "long_identifier_or_op") == 0 || strcmp(hk, "long_identifier") == 0 ||
        strcmp(hk, "identifier") == 0) {
        return cbm_node_text(a, head, source);
    }
    return NULL;
}

// PowerShell: a `command` node's callee is its `command_name` child.
static char *extract_powershell_callee(CBMArena *a, TSNode node, const char *source,
                                       const char *nk) {
    if (strcmp(nk, "command") != 0) {
        return NULL;
    }
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode c = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(c), "command_name") == 0) {
            return cbm_node_text(a, c, source);
        }
    }
    return NULL;
}

// Ada: procedure_call_statement / function_call carry the callee in a `name` field.
static char *extract_ada_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "procedure_call_statement") != 0 && strcmp(nk, "function_call") != 0) {
        return NULL;
    }
    TSNode name = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (!ts_node_is_null(name)) {
        return cbm_node_text(a, name, source);
    }
    if (ts_node_named_child_count(node) > 0) {
        TSNode head = ts_node_named_child(node, 0);
        const char *hk = ts_node_type(head);
        if (strcmp(hk, "name") == 0 || strcmp(hk, "identifier") == 0) {
            return cbm_node_text(a, head, source);
        }
    }
    return NULL;
}

// Solidity: a call_expression's callee is on the `function` field, wrapped in an
// `expression` node (call_expression -> function:expression -> identifier). Descend
// left-most through expression wrappers until we reach the identifier/member.
static char *extract_solidity_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "call_expression") != 0 && strcmp(nk, "call") != 0) {
        return NULL;
    }
    TSNode head = ts_node_child_by_field_name(node, TS_FIELD("function"));
    if (ts_node_is_null(head) && ts_node_named_child_count(node) > 0) {
        head = ts_node_named_child(node, 0);
    }
    // Unwrap nested `expression` wrappers down to the callee identifier/member.
    for (int depth = 0; depth < 4 && !ts_node_is_null(head); depth++) {
        const char *hk = ts_node_type(head);
        if (strcmp(hk, "identifier") == 0 || strcmp(hk, "member_expression") == 0 ||
            strcmp(hk, "member_access") == 0) {
            return cbm_node_text(a, head, source);
        }
        if (strcmp(hk, "expression") == 0 && ts_node_named_child_count(head) > 0) {
            head = ts_node_named_child(head, 0);
            continue;
        }
        break;
    }
    return NULL;
}

// Groovy: function_call's first named child is the callee identifier (the generic
// first-child fallback misses it because child 0 is anonymous).
static char *extract_groovy_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "function_call") != 0 && strcmp(nk, "juxt_function_call") != 0) {
        return NULL;
    }
    if (ts_node_named_child_count(node) > 0) {
        TSNode head = ts_node_named_child(node, 0);
        if (!ts_node_is_null(head) && strcmp(ts_node_type(head), "identifier") == 0) {
            return cbm_node_text(a, head, source);
        }
    }
    return NULL;
}

// WGSL: callee is nested type_constructor_or_function_call_expression ->
// type_declaration -> identifier. Descend left-most until an identifier.
static char *extract_wgsl_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "type_constructor_or_function_call_expression") != 0) {
        return NULL;
    }
    TSNode head = node;
    while (ts_node_named_child_count(head) > 0 && strcmp(ts_node_type(head), "identifier") != 0) {
        head = ts_node_named_child(head, 0);
    }
    if (strcmp(ts_node_type(head), "identifier") == 0) {
        return cbm_node_text(a, head, source);
    }
    return NULL;
}

// Dart: the invocation `selector` (the `(...)` part) follows the callee
// identifier as a sibling; `new_expression`'s first named child is the type.
static char *extract_dart_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "selector") == 0) {
        TSNode prev = ts_node_prev_named_sibling(node);
        if (!ts_node_is_null(prev) && strcmp(ts_node_type(prev), "identifier") == 0) {
            return cbm_node_text(a, prev, source);
        }
        return NULL;
    }
    if (strcmp(nk, "new_expression") == 0 && ts_node_named_child_count(node) > 0) {
        TSNode head = ts_node_named_child(node, 0);
        const char *hk = ts_node_type(head);
        if (strcmp(hk, "identifier") == 0 || strcmp(hk, "type_identifier") == 0) {
            return cbm_node_text(a, head, source);
        }
    }
    return NULL;
}

static char *extract_callee_lang_specific(CBMArena *a, TSNode node, const char *source,
                                          CBMLanguage lang) {
    const char *nk = ts_node_type(node);

    if (lang == CBM_LANG_CLOJURE || lang == CBM_LANG_COMMONLISP || lang == CBM_LANG_SCHEME ||
        lang == CBM_LANG_FENNEL || lang == CBM_LANG_RACKET || lang == CBM_LANG_EMACSLISP) {
        return extract_lisp_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_FSHARP) {
        return extract_fsharp_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_POWERSHELL) {
        return extract_powershell_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_ADA) {
        return extract_ada_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_SOLIDITY) {
        return extract_solidity_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_GROOVY) {
        return extract_groovy_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_WGSL) {
        return extract_wgsl_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_DART) {
        return extract_dart_callee(a, node, source, nk);
    }
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

    // Helm / Go templates: resolve `include "x"` / `template "x"` to the
    // referenced named template so it links to the define'd Function (#338).
    if (lang == CBM_LANG_GOTEMPLATE) {
        char *g = gotemplate_callee(a, node, source);
        if (g) {
            return g;
        }
    }

    // Constructor / instantiation nodes (new T(), object_creation, instance_expression):
    // resolve to the constructed type so a CALLS edge links to the class/constructor.
    char *ctor = extract_constructor_callee(a, node, source, ts_node_type(node));
    if (ctor) {
        return ctor;
    }

    // Ruby: `Widget.new(...)` is a method call on a constant receiver whose
    // method is `new`.  The constructor body lives in `initialize`, so a callee
    // of "new" never resolves.  Redirect to the receiver type name so the call
    // links to the class/constructor like every other language's `new T()`.
    if (lang == CBM_LANG_RUBY) {
        TSNode m = ts_node_child_by_field_name(node, TS_FIELD("method"));
        TSNode recv = ts_node_child_by_field_name(node, TS_FIELD("receiver"));
        if (!ts_node_is_null(m) && !ts_node_is_null(recv) &&
            strcmp(ts_node_type(recv), "constant") == 0) {
            char *mt = cbm_node_text(a, m, source);
            if (mt && strcmp(mt, "new") == 0) {
                char *rt = cbm_node_text(a, recv, source);
                if (rt && rt[0]) {
                    return rt;
                }
            }
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

// Return the (dequoted) first string-literal child of a node, or NULL.
static char *gotemplate_string_child(CBMArena *a, TSNode parent, const char *source) {
    TSNode s = cbm_find_child_by_kind(parent, "interpreted_string_literal");
    if (ts_node_is_null(s)) {
        return NULL;
    }
    char *text = cbm_node_text(a, s, source);
    const char *v = strip_and_validate_string_arg(a, text);
    return (char *)v;
}

// Resolve a Go-template / Helm call to the referenced named template:
//   {{ template "x" . }}            -> template_action, name is a string child
//   {{ include "x" . }}             -> function_call(include), name is first string arg
// Returns NULL for any other node so generic resolution names the function.
static char *gotemplate_callee(CBMArena *a, TSNode node, const char *source) {
    const char *k = ts_node_type(node);
    if (strcmp(k, "template_action") == 0) {
        return gotemplate_string_child(a, node, source);
    }
    if (strcmp(k, "function_call") == 0) {
        TSNode fn = cbm_find_child_by_kind(node, "identifier");
        if (ts_node_is_null(fn)) {
            return NULL;
        }
        char *fname = cbm_node_text(a, fn, source);
        if (!fname || (strcmp(fname, "include") != 0 && strcmp(fname, "template") != 0 &&
                       strcmp(fname, "tpl") != 0)) {
            return NULL;
        }
        TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
        if (ts_node_is_null(args)) {
            args = cbm_find_child_by_kind(node, "argument_list");
        }
        if (ts_node_is_null(args)) {
            return NULL;
        }
        return gotemplate_string_child(a, args, source);
    }
    return NULL;
}

static const char *extract_nth_string_arg(CBMExtractCtx *ctx, TSNode args, uint32_t n) {
    uint32_t nc = ts_node_named_child_count(args);
    uint32_t found = 0;
    for (uint32_t ai = 0; ai < nc && ai < MAX_POSITIONAL_SCAN + n; ai++) {
        TSNode arg = ts_node_named_child(args, ai);
        const char *ak = ts_node_type(arg);
        if (is_string_like(ak)) {
            if (found == n) {
                char *text = cbm_node_text(ctx->arena, arg, ctx->source);
                return strip_and_validate_string_arg(ctx->arena, text);
            }
            found++;
        }
    }
    return NULL;
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

// Check if a struct-field name identifies a queue/topic target.  Cloud SDKs pass
// the destination via a composite-literal input struct rather than a bare string
// arg (e.g. Go `SendMessageInput{QueueUrl: ...}`, `PublishInput{TopicArn: ...}`).
// Case-insensitive so QueueUrl/QueueURL/queue_url all match.
static bool is_queue_topic_field(const char *key) {
    static const char *fields[] = {"QueueUrl",  "QueueURL", "TopicArn", "TopicARN",    "QueueName",
                                   "TopicName", "QueueArn", "QueueARN", "Destination", NULL};
    if (!key || !key[0]) {
        return false;
    }
    for (int i = 0; fields[i]; i++) {
        if (strcasecmp(key, fields[i]) == 0) {
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

// Recover a queue/topic identity from a Go composite-literal input struct, e.g.
//   &sqs.SendMessageInput{QueueUrl: queueUrl, MessageBody: body}
//   sns.PublishInput{TopicArn: "arn:aws:sns:..."}
// The dispatch target is carried by a struct field (QueueUrl/TopicArn/...), not a
// bare string arg, so the async edge would otherwise degrade to a plain CALLS.
// Returns the field's value: the string-literal content when present, else the
// referenced identifier text (which still names the queue/topic for edge formation).
static const char *extract_composite_queue_field(CBMExtractCtx *ctx, TSNode node) {
    // Unwrap a pointer-of-composite: `&Type{...}` is a unary_expression whose
    // operand is the composite_literal.
    if (strcmp(ts_node_type(node), "unary_expression") == 0) {
        TSNode operand = ts_node_child_by_field_name(node, TS_FIELD("operand"));
        if (ts_node_is_null(operand)) {
            return NULL;
        }
        node = operand;
    }
    if (strcmp(ts_node_type(node), "composite_literal") != 0) {
        return NULL;
    }
    TSNode body = ts_node_child_by_field_name(node, TS_FIELD("body"));
    if (ts_node_is_null(body)) {
        body = cbm_find_child_by_kind(node, "literal_value");
    }
    if (ts_node_is_null(body)) {
        return NULL;
    }
    uint32_t nc = ts_node_named_child_count(body);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode el = ts_node_named_child(body, i);
        if (strcmp(ts_node_type(el), "keyed_element") != 0) {
            continue;
        }
        // keyed_element children: key then value. Each side may be wrapped in a
        // literal_element; unwrap to the underlying identifier/literal.
        uint32_t ec = ts_node_named_child_count(el);
        if (ec < PAIR_LEN) {
            continue;
        }
        TSNode key_n = ts_node_named_child(el, 0);
        TSNode val_n = ts_node_named_child(el, 1);
        if (strcmp(ts_node_type(key_n), "literal_element") == 0 &&
            ts_node_named_child_count(key_n) > 0) {
            key_n = ts_node_named_child(key_n, 0);
        }
        if (strcmp(ts_node_type(val_n), "literal_element") == 0 &&
            ts_node_named_child_count(val_n) > 0) {
            val_n = ts_node_named_child(val_n, 0);
        }
        char *key = cbm_node_text(ctx->arena, key_n, ctx->source);
        if (!is_queue_topic_field(key)) {
            continue;
        }
        const char *resolved = extract_string_value(ctx, val_n);
        if (resolved && resolved[0]) {
            return resolved;
        }
        // Value is a variable/expression (no constant value); use its source text
        // as the queue/topic identity so the async edge still forms.
        char *raw = cbm_node_text(ctx->arena, val_n, ctx->source);
        if (raw && raw[0]) {
            return raw;
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
        /* PHP and C# wrap each positional argument in an `argument` node;
         * unwrap to the underlying value so the URL string is reachable. */
        if (strcmp(ts_node_type(arg), "argument") == 0 && ts_node_named_child_count(arg) > 0) {
            arg = ts_node_named_child(arg, 0);
        }
        const char *ak = ts_node_type(arg);

        if (strcmp(ak, "keyword_argument") == 0 || strcmp(ak, "pair") == 0) {
            const char *val = extract_keyword_url(ctx, arg);
            if (val) {
                return val;
            }
            continue;
        }

        /* Cloud SDK dispatch via input struct: the queue/topic target is a field
         * of a composite literal (Go `&sqs.SendMessageInput{QueueUrl: ...}`), not
         * a bare string arg. Recover it so the async edge forms. */
        if (strcmp(ak, "composite_literal") == 0 || strcmp(ak, "unary_expression") == 0) {
            const char *val = extract_composite_queue_field(ctx, arg);
            if (val) {
                return val;
            }
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
/* Normalize a string-form route handler to a resolvable handler name.
 *   'showUsers'              → showUsers
 *   'UserController@show'    → show   (Laravel "Controller@method")
 * The method segment after '@' is the resolvable function/method name. */
static const char *normalize_string_handler(CBMArena *a, const char *raw) {
    const char *unq = strip_quotes(a, raw);
    if (!unq || !unq[0]) {
        return NULL;
    }
    const char *at = strchr(unq, '@');
    if (at && at[1]) {
        return cbm_arena_strdup(a, at + 1);
    }
    return unq;
}

static const char *extract_handler_arg(CBMExtractCtx *ctx, TSNode args) {
    uint32_t nc = ts_node_named_child_count(args);
    for (uint32_t ai = HANDLER_START_IDX; ai < nc && ai < MAX_HANDLER_SCAN; ai++) {
        TSNode arg2 = ts_node_named_child(args, ai);
        /* PHP wraps each argument in an `argument` node — unwrap to the value. */
        if (strcmp(ts_node_type(arg2), "argument") == 0 && ts_node_named_child_count(arg2) > 0) {
            arg2 = ts_node_named_child(arg2, 0);
        }
        const char *ak2 = ts_node_type(arg2);
        /* `name` = PHP bare identifier handler; string = Laravel string handler
         * ('showUsers' or 'Controller@method'). */
        if (strcmp(ak2, "identifier") == 0 || strcmp(ak2, "member_expression") == 0 ||
            strcmp(ak2, "selector_expression") == 0 || strcmp(ak2, "attribute") == 0 ||
            strcmp(ak2, "field_expression") == 0 || strcmp(ak2, "name") == 0) {
            return cbm_node_text(ctx->arena, arg2, ctx->source);
        }
        if (is_string_like(ak2)) {
            const char *h =
                normalize_string_handler(ctx->arena, cbm_node_text(ctx->arena, arg2, ctx->source));
            if (h && h[0]) {
                return h;
            }
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
            call.loop_depth = state->loop_depth;     // enclosing loop nesting at this call
            call.branch_depth = state->branch_depth; // enclosing branch nesting at this call
            call.start_line = (int)ts_node_start_point(node).row + TS_LINE_OFFSET;
            // Perl-only: flag arrow/method calls ($obj->m / Class->m). The
            // generic short-name resolver cannot place a method without a known
            // receiver type, so the call-resolution pass suppresses those edges.
            // Default false for every other language (struct is zero-init).
            if (ctx->language == CBM_LANG_PERL &&
                strcmp(ts_node_type(node), "method_call_expression") == 0) {
                call.is_method = true;
            }

            TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
            if (!ts_node_is_null(args)) {
                call.first_string_arg = extract_url_or_topic_arg(ctx, args);
                if (call.first_string_arg && call.first_string_arg[0] == '/') {
                    call.second_arg_name = extract_handler_arg(ctx, args);
                }
                extract_call_args(ctx, args, &call);
            }

            cbm_calls_push(&ctx->result->calls, ctx->arena, call);

            const char **dispatch_suffixes = cbm_string_dispatch_suffixes(ctx->language);
            if (dispatch_suffixes && !ts_node_is_null(args)) {
                const char *cn = call.callee_name;
                size_t len = cn ? strlen(cn) : 0;
                for (const char **nm = dispatch_suffixes; *nm; nm++) {
                    size_t nlen = strlen(*nm);
                    if (len >= nlen && strcmp(cn + len - nlen, *nm) == 0) {
                        const char *cls = extract_nth_string_arg(ctx, args, 0);
                        const char *mth = extract_nth_string_arg(ctx, args, 1);
                        if (cls && mth) {
                            CBMCall xcall = {0};
                            xcall.callee_name = cbm_arena_sprintf(ctx->arena, "%s.%s", cls, mth);
                            xcall.enclosing_func_qn = call.enclosing_func_qn;
                            cbm_calls_push(&ctx->result->calls, ctx->arena, xcall);
                        }
                        break;
                    }
                }
            }
        }
    }

    if (ctx->language == CBM_LANG_TSX || ctx->language == CBM_LANG_JAVASCRIPT) {
        extract_jsx_component_ref(ctx, node, ts_node_type(node), state->enclosing_func_qn);
    }
}
