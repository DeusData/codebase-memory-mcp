#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_sprintf
#include "helpers.h"
#include "lang_specs.h"
#include "macro_table.h"
#include "extract_unified.h"
#include "foundation/constants.h"
#include "extract_node_stack.h"
#include "service_patterns.h" // cbm_service_pattern_route_method (#952)
#include "tree_sitter/api.h"  // TSNode, ts_node_*
#include <stdint.h>           // uint32_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#if defined(CBM_KOTLIN_DEDUP_TEST_API) && CBM_KOTLIN_DEDUP_TEST_API
#include <stdatomic.h>
#endif

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
static const char *strip_and_validate_string_arg(CBMArena *a, char *text);

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
        // C++ explicit template call f<T>(args): the `function` field is a
        // template_function whose `name` child is the bare callee (identifier
        // "identity" or qualified_identifier "ns::f"). Without this the whole
        // "identity<int>" text would never be produced as a textual callee, so
        // no CALLS edge — and the LSP's lsp_template resolution has nothing to
        // attach to. Return the name child so the join recovers the bare method.
        if (strcmp(fk, "template_function") == 0) {
            TSNode tname = ts_node_child_by_field_name(func_node, TS_FIELD("name"));
            if (!ts_node_is_null(tname)) {
                return cbm_node_text(a, tname, source);
            }
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
    if (strcmp(nk, "apply") == 0 || strcmp(nk, "application_expression") == 0 ||
        strcmp(nk, "exp_apply") == 0) {
        if (ts_node_child_count(node) > 0) {
            TSNode callee = ts_node_child(node, 0);
            const char *ck = ts_node_type(callee);
            if (strcmp(ck, "identifier") == 0 || strcmp(ck, "variable") == 0 ||
                strcmp(ck, "constructor") == 0 || strcmp(ck, "value_path") == 0 ||
                /* PureScript: exp_apply's function head is an `exp_name` whose
                 * text is the (possibly qualified) function name. */
                strcmp(ck, "exp_name") == 0) {
                return cbm_node_text(a, callee, source);
            }
            /* Curried application `f a b` nests exp_apply/apply — descend the
             * function head to recover the leftmost callee. */
            if (strcmp(ck, "exp_apply") == 0 || strcmp(ck, "apply") == 0 ||
                strcmp(ck, "application_expression") == 0) {
                return extract_fp_callee(a, callee, source, ck);
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
    if (lang == CBM_LANG_ELIXIR && strcmp(nk, "binary_operator") == 0) {
        /* The grammar exposes the operator as an exact field. Reading the bytes
         * between operands also captured binding punctuation in definition
         * heads; those containers are not invocations. */
        TSNode op = ts_node_child_by_field_name(node, TS_FIELD("operator"));
        if (ts_node_is_null(op)) {
            return NULL;
        }
        char *operator_name = cbm_node_text(a, op, source);
        if (!operator_name || strcmp(operator_name, "=") == 0 || strcmp(operator_name, "<-") == 0 ||
            strcmp(operator_name, "->") == 0 || strcmp(operator_name, "\\\\") == 0 ||
            strcmp(operator_name, "::") == 0 || strcmp(operator_name, "when") == 0) {
            return NULL;
        }
        return operator_name;
    }
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
        char *pn = ts_node_is_null(func_node) ? NULL : cbm_node_text(a, func_node, source);
        return pn;
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

static bool call_node_contains(TSNode outer, TSNode inner) {
    return !ts_node_is_null(outer) && !ts_node_is_null(inner) &&
           ts_node_start_byte(outer) <= ts_node_start_byte(inner) &&
           ts_node_end_byte(outer) >= ts_node_end_byte(inner);
}

static bool call_node_text_equals(const char *source, TSNode node, const char *expected) {
    if (!source || ts_node_is_null(node) || !expected) {
        return false;
    }
    uint32_t start = ts_node_start_byte(node);
    uint32_t end = ts_node_end_byte(node);
    size_t expected_len = strlen(expected);
    return end >= start && (size_t)(end - start) == expected_len &&
           memcmp(source + start, expected, expected_len) == 0;
}

static bool call_node_text_in(TSNode node, const char *source, const char *const *values) {
    for (const char *const *value = values; *value; value++) {
        if (call_node_text_equals(source, node, *value)) {
            return true;
        }
    }
    return false;
}

static bool lisp_definition_head(CBMLanguage lang, TSNode head, const char *source) {
    static const char *const clojure_scheme_racket_heads[] = {"defn",
                                                              "defn-",
                                                              "def",
                                                              "defmacro",
                                                              "defmulti",
                                                              "defmethod",
                                                              "defprotocol",
                                                              "defrecord",
                                                              "deftype",
                                                              "definterface",
                                                              "defonce",
                                                              "define",
                                                              "define-syntax",
                                                              "define-values",
                                                              "define-syntax-rule",
                                                              "define-struct",
                                                              "define-record-type",
                                                              "define/contract",
                                                              "struct",
                                                              NULL};
    static const char *const common_lisp_heads[] = {
        "defun",       "defmacro", "defgeneric", "defmethod", "defvar", "defparameter",
        "defconstant", "deftype",  "defstruct",  "defclass",  NULL};

    return call_node_text_in(head, source,
                             lang == CBM_LANG_COMMONLISP ? common_lisp_heads
                                                         : clojure_scheme_racket_heads);
}

static bool lisp_list_is_definition_role(CBMLanguage lang, TSNode node, const char *source) {
    uint32_t count = ts_node_named_child_count(node);
    if (count > 0 && lisp_definition_head(lang, ts_node_named_child(node, 0), source)) {
        return true;
    }

    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        const char *parent_kind = ts_node_type(parent);
        if (lang == CBM_LANG_COMMONLISP &&
            (strcmp(parent_kind, "defun_header") == 0 || strcmp(parent_kind, "lambda_list") == 0)) {
            return true;
        }
        if (lang == CBM_LANG_EMACSLISP && (strcmp(parent_kind, "function_definition") == 0 ||
                                           strcmp(parent_kind, "macro_definition") == 0)) {
            TSNode parameters = ts_node_child_by_field_name(parent, TS_FIELD("parameters"));
            return call_node_contains(parameters, node);
        }
        if ((strcmp(parent_kind, "list") == 0 || strcmp(parent_kind, "list_lit") == 0) &&
            ts_node_named_child_count(parent) >= 2 &&
            lisp_definition_head(lang, ts_node_named_child(parent, 0), source)) {
            /* `(define (name args) body)` nests the signature list in the
             * definition's second form. Body lists remain genuine calls. */
            return call_node_contains(ts_node_named_child(parent, 1), node);
        }
    }
    return false;
}

static bool call_node_has_direct_token(TSNode node, const char *token) {
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(ts_node_type(ts_node_child(node, i)), token) == 0) {
            return true;
        }
    }
    return false;
}

static bool julia_call_is_definition_head(TSNode node) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        const char *kind = ts_node_type(parent);
        if (strcmp(kind, "assignment") == 0 || strcmp(kind, "function_definition") == 0 ||
            strcmp(kind, "short_function_definition") == 0) {
            return ts_node_named_child_count(parent) > 0 &&
                   call_node_contains(ts_node_named_child(parent, 0), node);
        }
    }
    return false;
}

static bool typst_call_is_let_pattern(TSNode node) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), "let") == 0) {
            TSNode pattern = ts_node_child_by_field_name(parent, TS_FIELD("pattern"));
            return call_node_contains(pattern, node);
        }
    }
    return false;
}

static bool agda_expr_is_definition_role(TSNode node) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        const char *kind = ts_node_type(parent);
        if (strcmp(kind, "lhs") == 0 || strcmp(kind, "typed_binding") == 0 ||
            strcmp(kind, "signature") == 0 || strcmp(kind, "type_signature") == 0 ||
            strcmp(kind, "data_signature") == 0 || strcmp(kind, "record_signature") == 0) {
            return true;
        }
        if (strcmp(kind, "function") == 0) {
            /* A ':' function line is a type signature. In an '=' definition,
             * lhs expressions were rejected above and rhs applications remain
             * executable calls. */
            return call_node_has_direct_token(parent, ":");
        }
    }
    return false;
}

static TSNode elixir_call_arguments(TSNode call) {
    TSNode arguments = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
    if (ts_node_is_null(arguments) && ts_node_child_count(call) > 1) {
        arguments = ts_node_child(call, 1);
    }
    return arguments;
}

static bool elixir_call_head_in(TSNode call, const char *source, const char *const *heads) {
    return ts_node_child_count(call) > 0 &&
           call_node_text_in(ts_node_child(call, 0), source, heads);
}

static bool elixir_call_is_definition_role(TSNode node, const char *source) {
    static const char *const structural_heads[] = {"def", "defp", "defmacro", "defmodule", NULL};
    static const char *const function_heads[] = {"def", "defp", "defmacro", NULL};
    if (elixir_call_head_in(node, source, structural_heads)) {
        return true;
    }

    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), "call") != 0 ||
            !elixir_call_head_in(parent, source, function_heads)) {
            continue;
        }
        TSNode arguments = elixir_call_arguments(parent);
        TSNode signature = ts_node_named_child_count(arguments) > 0
                               ? ts_node_named_child(arguments, 0)
                               : arguments;
        return ts_node_eq(signature, node);
    }
    return false;
}

static bool call_node_is_definition_container(CBMLanguage lang, TSNode node, const char *source) {
    const char *kind = ts_node_type(node);
    if ((lang == CBM_LANG_CLOJURE || lang == CBM_LANG_SCHEME || lang == CBM_LANG_RACKET ||
         lang == CBM_LANG_COMMONLISP || lang == CBM_LANG_EMACSLISP) &&
        (strcmp(kind, "list") == 0 || strcmp(kind, "list_lit") == 0)) {
        return lisp_list_is_definition_role(lang, node, source);
    }
    if (lang == CBM_LANG_JULIA &&
        (strcmp(kind, "call_expression") == 0 || strcmp(kind, "broadcast_call_expression") == 0)) {
        return julia_call_is_definition_head(node);
    }
    if (lang == CBM_LANG_TYPST && strcmp(kind, "call") == 0) {
        return typst_call_is_let_pattern(node);
    }
    if (lang == CBM_LANG_AGDA && strcmp(kind, "expr") == 0) {
        return agda_expr_is_definition_role(node);
    }
    return lang == CBM_LANG_ELIXIR && strcmp(kind, "call") == 0 &&
           elixir_call_is_definition_role(node, source);
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

// CSS: a `call_expression` (e.g. `url(...)`, `calc(...)`) carries its callee on a
// plain `function_name` child rather than a `function`/`name` field, so generic
// field/first-child resolution misses it.
static char *extract_css_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "call_expression") != 0) {
        return NULL;
    }
    TSNode fn = cbm_find_child_by_kind(node, "function_name");
    return ts_node_is_null(fn) ? NULL : cbm_node_text(a, fn, source);
}

// Linker scripts expose the builtin in the exact `function:` field as a
// `symbol`. Keep the `arguments:` subtree separate so symbols passed to the
// builtin remain value usages.
static char *extract_linkerscript_callee(CBMArena *a, TSNode node, const char *source,
                                         const char *nk) {
    if (strcmp(nk, "call_expression") != 0) {
        return NULL;
    }
    TSNode function = ts_node_child_by_field_name(node, TS_FIELD("function"));
    if (ts_node_is_null(function) || strcmp(ts_node_type(function), "symbol") != 0) {
        return NULL;
    }
    return cbm_node_text(a, function, source);
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

// SCSS: an `@include foo;` is an include_statement whose callee is its
// `identifier` child (the mixin name).
static char *extract_scss_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "include_statement") == 0) {
        TSNode id = cbm_find_child_by_kind(node, "identifier");
        return ts_node_is_null(id) ? NULL : cbm_node_text(a, id, source);
    }
    /* SCSS @function call `double($x)` is a call_expression whose callee is a
     * `function_name` child (there is no `function` field), so the generic
     * field-based resolver returns NULL and the call is dropped — no CALLS edge
     * to the in-file @function. */
    if (strcmp(nk, "call_expression") == 0) {
        TSNode fn = cbm_find_child_by_kind(node, "function_name");
        if (!ts_node_is_null(fn)) {
            return cbm_node_text(a, fn, source);
        }
    }
    return NULL;
}

// SQL: an `invocation` node's callee is nested object_reference > `name` field
// (the same shape as a create_function's name).
static char *extract_sql_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "invocation") != 0) {
        return NULL;
    }
    TSNode oref = cbm_find_child_by_kind(node, "object_reference");
    if (ts_node_is_null(oref)) {
        return NULL;
    }
    TSNode nm = ts_node_child_by_field_name(oref, TS_FIELD("name"));
    return ts_node_is_null(nm) ? NULL : cbm_node_text(a, nm, source);
}

// COBOL: a `CALL 'HELPER'` is a call_statement whose `x` field is a string
// literal naming the called program; the callee is that string sans quotes.
static char *extract_cobol_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "call_statement") != 0) {
        return NULL;
    }
    TSNode x = ts_node_child_by_field_name(node, TS_FIELD("x"));
    if (ts_node_is_null(x)) {
        x = cbm_find_child_by_kind(node, "string");
    }
    if (ts_node_is_null(x)) {
        return NULL;
    }
    char *text = cbm_node_text(a, x, source);
    return (char *)strip_and_validate_string_arg(a, text);
}

// Elm: a `function_call_expr` has a `target` field; the callee identifier is
// target > value_expr > `name` field (value_qid) > lower_case_identifier.
static char *extract_elm_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "function_call_expr") != 0) {
        return NULL;
    }
    TSNode target = ts_node_child_by_field_name(node, TS_FIELD("target"));
    if (ts_node_is_null(target)) {
        return NULL;
    }
    TSNode ve = strcmp(ts_node_type(target), "value_expr") == 0
                    ? target
                    : cbm_find_child_by_kind(target, "value_expr");
    if (ts_node_is_null(ve)) {
        return NULL;
    }
    TSNode qid = ts_node_child_by_field_name(ve, TS_FIELD("name"));
    if (ts_node_is_null(qid)) {
        qid = cbm_find_child_by_kind(ve, "value_qid");
    }
    if (ts_node_is_null(qid)) {
        return NULL;
    }
    TSNode id = cbm_find_child_by_kind(qid, "lower_case_identifier");
    if (ts_node_is_null(id)) {
        // module-qualified call: emit the whole qualified id text
        return cbm_node_text(a, qid, source);
    }
    return cbm_node_text(a, id, source);
}

// Jsonnet: a `functioncall` node's callee is its first `id` child (the called
// binding name); the generic field path misses it (no `function`/`name` field).
static char *extract_jsonnet_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "functioncall") != 0) {
        return NULL;
    }
    TSNode id = cbm_find_child_by_kind(node, "id");
    return ts_node_is_null(id) ? NULL : cbm_node_text(a, id, source);
}

// Nickel: function application is `applicative` and curries left-associatively:
// `f x y` parses as `(applicative t1:(applicative t1:f t2:x) t2:y)`. A real call
// node carries a `t2` (argument) field; a bare value (`applicative
// (record_operand (atom (ident))))` wraps every expression and has no `t2`, so it
// is NOT a call. We also skip applicatives whose parent is itself an applicative
// (the inner partial-application nodes) so a curried call emits exactly one edge,
// keyed on the leftmost ident reached by descending the `t1` chain.
// (`infix_expr` is binary operator application, not a call, and is excluded from
// nickel_call_types.)
static char *extract_nickel_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "applicative") != 0) {
        return NULL;
    }
    // Not an application unless it has an argument (`t2`).
    if (ts_node_is_null(ts_node_child_by_field_name(node, TS_FIELD("t2")))) {
        return NULL;
    }
    // Emit only at the outermost applicative of a curried chain.
    TSNode parent = ts_node_parent(node);
    if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "applicative") == 0) {
        return NULL;
    }
    enum { NICKEL_APPLY_DEPTH = 8 };
    TSNode cur = node;
    for (int depth = 0; depth < NICKEL_APPLY_DEPTH && !ts_node_is_null(cur); depth++) {
        const char *ck = ts_node_type(cur);
        if (strcmp(ck, "ident") == 0) {
            return cbm_node_text(a, cur, source);
        }
        // Descend the function side: the `t1` field for curried applicatives, or
        // the wrapper's first named child (record_operand -> atom -> ident).
        TSNode next = ts_node_child_by_field_name(cur, TS_FIELD("t1"));
        if (ts_node_is_null(next) && ts_node_named_child_count(cur) > 0) {
            next = ts_node_named_child(cur, 0);
        }
        if (ts_node_is_null(next) || ts_node_eq(next, cur)) {
            break;
        }
        cur = next;
    }
    return NULL;
}

// Typst: a `call` node's callee is its `item` field (an ident), matching the
// def-side resolution of `#let greet(name) = ...`.
static char *extract_typst_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "call") != 0) {
        return NULL;
    }
    TSNode item = ts_node_child_by_field_name(node, TS_FIELD("item"));
    return ts_node_is_null(item) ? NULL : cbm_node_text(a, item, source);
}

// Meson: a builtin invocation (`executable(...)`, `dependency(...)`) is a
// `normal_command` whose `command` field is the called identifier.
static char *extract_meson_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "normal_command") != 0) {
        return NULL;
    }
    TSNode cmd = ts_node_child_by_field_name(node, TS_FIELD("command"));
    return ts_node_is_null(cmd) ? NULL : cbm_node_text(a, cmd, source);
}

// Descend left-most through wrapper nodes to the first identifier-bearing leaf.
// Used by HDL call nodes whose callee identifier is nested under one or more
// grammar wrappers (Verilog tf_call -> simple_identifier; SystemVerilog
// tf_call -> hierarchical_identifier -> simple_identifier).
static char *first_leaf_identifier(CBMArena *a, TSNode node, const char *source) {
    TSNode cur = node;
    for (int depth = 0; depth < 8 && !ts_node_is_null(cur); depth++) {
        const char *k = ts_node_type(cur);
        if (strcmp(k, "simple_identifier") == 0 || strcmp(k, "identifier") == 0 ||
            strcmp(k, "word") == 0 || strcmp(k, "name") == 0 || strcmp(k, "qid") == 0) {
            char *t = cbm_node_text(a, cur, source);
            return (t && t[0]) ? t : NULL;
        }
        if (ts_node_named_child_count(cur) == 0) {
            return NULL;
        }
        cur = ts_node_named_child(cur, 0);
    }
    return NULL;
}

// Verilog / SystemVerilog: a function_subroutine_call wraps
// subroutine_call -> tf_call -> [hierarchical_identifier ->] simple_identifier.
// Descend to the first identifier leaf to name the callee.
static char *extract_hdl_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "function_subroutine_call") != 0 && strcmp(nk, "subroutine_call") != 0 &&
        strcmp(nk, "tf_call") != 0 && strcmp(nk, "system_tf_call") != 0) {
        return NULL;
    }
    return first_leaf_identifier(a, node, source);
}

// VHDL: `add(x, 1)` parses as `(name (library_function) (parenthesis_group ...))`
// inside a `simple_expression` (the function-call / indexed-name ambiguity). The
// call_node_types set targets `parenthesis_group`; the callee is its immediately
// preceding named sibling (a `library_function`/`identifier`/`name` token).
static char *extract_vhdl_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "parenthesis_group") != 0) {
        return NULL;
    }
    TSNode prev = ts_node_prev_named_sibling(node);
    if (ts_node_is_null(prev)) {
        return NULL;
    }
    const char *pk = ts_node_type(prev);
    if (strcmp(pk, "library_function") == 0 || strcmp(pk, "identifier") == 0 ||
        strcmp(pk, "name") == 0 || strcmp(pk, "simple_name") == 0) {
        char *t = cbm_node_text(a, prev, source);
        return (t && t[0]) ? t : NULL;
    }
    return NULL;
}

// NASM: a `call`/`jmp`-style instruction is an `actual_instruction` whose
// `instruction:` field is the mnemonic word and whose first operand word is the
// target label. Only treat call/jump mnemonics as calls; everything else (add,
// mov, ret, ...) is plain data-flow, not a call.
static char *extract_nasm_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "call_syntax_expression") == 0) {
        TSNode base = ts_node_child_by_field_name(node, TS_FIELD("base"));
        return ts_node_is_null(base) ? NULL : first_leaf_identifier(a, base, source);
    }
    if (strcmp(nk, "actual_instruction") != 0) {
        return NULL;
    }
    TSNode mnem = ts_node_child_by_field_name(node, TS_FIELD("instruction"));
    if (ts_node_is_null(mnem)) {
        return NULL;
    }
    char *m = cbm_node_text(a, mnem, source);
    if (!m || (strcmp(m, "call") != 0 && strcmp(m, "jmp") != 0 && strcmp(m, "je") != 0 &&
               strcmp(m, "jne") != 0 && strcmp(m, "jz") != 0 && strcmp(m, "jnz") != 0)) {
        return NULL;
    }
    TSNode ops = ts_node_child_by_field_name(node, TS_FIELD("operands"));
    if (ts_node_is_null(ops) || ts_node_named_child_count(ops) == 0) {
        return NULL;
    }
    return first_leaf_identifier(a, ts_node_named_child(ops, 0), source);
}

// LLVM-IR: a `call`/`invoke` is an `instruction_call` whose `callee:` field is a
// `value -> var -> global_var` chain (e.g. `@inner`). Strip the leading sigil.
static char *extract_llvm_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "instruction_call") != 0) {
        return NULL;
    }
    TSNode callee = ts_node_child_by_field_name(node, TS_FIELD("callee"));
    if (ts_node_is_null(callee)) {
        return NULL;
    }
    char *t = first_leaf_identifier(a, callee, source);
    if (!t) {
        t = cbm_node_text(a, callee, source);
    }
    if (t && (t[0] == '@' || t[0] == '%')) {
        return t + 1;
    }
    return t;
}

// FunC: a `function_application` carries the callee on its `function:` field.
static char *extract_func_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "function_application") != 0) {
        return NULL;
    }
    TSNode fn = ts_node_child_by_field_name(node, TS_FIELD("function"));
    return ts_node_is_null(fn) ? NULL : cbm_node_text(a, fn, source);
}

// Nix: an `apply_expression` (`f x`) carries the applied function on its
// `function:` field. The head is a `variable_expression` whose `name` is the
// callee identifier; curried application (`f x y`) nests apply_expressions, so
// descend the `function` chain to the head variable_expression. The generic
// field resolver does not recognise `variable_expression`, so without this the
// call to `addOne` would never be captured.
static char *extract_nix_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "apply_expression") != 0) {
        return NULL;
    }
    TSNode fn = ts_node_child_by_field_name(node, TS_FIELD("function"));
    for (int depth = 0; depth < 8 && !ts_node_is_null(fn); depth++) {
        const char *fk = ts_node_type(fn);
        if (strcmp(fk, "apply_expression") == 0) {
            fn = ts_node_child_by_field_name(fn, TS_FIELD("function"));
            continue;
        }
        if (strcmp(fk, "variable_expression") == 0) {
            TSNode nm = ts_node_child_by_field_name(fn, TS_FIELD("name"));
            return ts_node_is_null(nm) ? NULL : cbm_node_text(a, nm, source);
        }
        if (strcmp(fk, "identifier") == 0) {
            return cbm_node_text(a, fn, source);
        }
        return NULL;
    }
    return NULL;
}

// Agda has no fields. A plain application is the exact adjacency shape
// `expr(atom, atom, ...)`; operator/type expressions also have multiple named
// children, but include an anonymous operator token and must not become calls.
static TSNode agda_application_head_atom(TSNode node) {
    if (strcmp(ts_node_type(node), "expr") != 0 || ts_node_named_child_count(node) < 2 ||
        ts_node_child_count(node) != ts_node_named_child_count(node)) {
        return (TSNode){0};
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(ts_node_type(ts_node_named_child(node, i)), "atom") != 0) {
            return (TSNode){0};
        }
    }
    return ts_node_named_child(node, 0);
}

static TSNode agda_head_qid(TSNode head) {
    TSNode current = head;
    for (int depth = 0; depth < 8 && !ts_node_is_null(current); depth++) {
        if (strcmp(ts_node_type(current), "qid") == 0) {
            return current;
        }
        if (ts_node_named_child_count(current) == 0) {
            break;
        }
        current = ts_node_named_child(current, 0);
    }
    return (TSNode){0};
}

static char *extract_agda_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "expr") != 0) {
        return NULL;
    }
    TSNode qid = agda_head_qid(agda_application_head_atom(node));
    return ts_node_is_null(qid) ? NULL : cbm_node_text(a, qid, source);
}

// Make: `$(shell ...)` is a `shell_function` node; the callee is the literal
// `shell` keyword. tree-sitter-make also exposes `function_call` for other
// builtins ($(wildcard ...), $(patsubst ...)).
static char *extract_make_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "shell_function") == 0) {
        return cbm_arena_strndup(a, "shell", 5);
    }
    if (strcmp(nk, "function_call") == 0) {
        TSNode fn = ts_node_child_by_field_name(node, TS_FIELD("function"));
        if (ts_node_is_null(fn) && ts_node_named_child_count(node) > 0) {
            fn = ts_node_named_child(node, 0);
        }
        return ts_node_is_null(fn) ? NULL : cbm_node_text(a, fn, source);
    }
    return NULL;
}

// Just: a recipe dependency `recipe: dep` is a `dependency` node whose `name:`
// field is the referenced recipe.
static char *extract_just_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "dependency") != 0) {
        return NULL;
    }
    TSNode name = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name) && ts_node_named_child_count(node) > 0) {
        name = ts_node_named_child(node, 0);
    }
    return ts_node_is_null(name) ? NULL : cbm_node_text(a, name, source);
}

// Puppet: `include foo` is an `include_statement`; the callee is the literal
// `include` keyword (the class/identifier args are resolved as separate refs).
static char *extract_puppet_callee(CBMArena *a, TSNode node, const char *source, const char *nk) {
    if (strcmp(nk, "include_statement") == 0) {
        return cbm_arena_strndup(a, "include", 7);
    }
    if (strcmp(nk, "function_call") == 0) {
        if (ts_node_named_child_count(node) > 0) {
            TSNode head = ts_node_named_child(node, 0);
            if (strcmp(ts_node_type(head), "identifier") == 0) {
                return cbm_node_text(a, head, source);
            }
        }
    }
    return NULL;
}

static char *extract_callee_lang_specific(CBMArena *a, TSNode node, const char *source,
                                          CBMLanguage lang) {
    const char *nk = ts_node_type(node);

    if (lang == CBM_LANG_FORTRAN && strcmp(nk, "subroutine_call") == 0) {
        TSNode subroutine = ts_node_child_by_field_name(node, TS_FIELD("subroutine"));
        if (!ts_node_is_null(subroutine)) {
            return cbm_node_text(a, subroutine, source);
        }
    }

    /* Python dict-dispatch call `funcs["a"](v)`: the call's `function` field is a
     * subscript whose base is the identifier holding the dispatch table. Emit the
     * base identifier ("funcs") as the textual callee so a CALLS edge exists; the
     * py-LSP resolves it to the real target and joins via `reason` (lsp_resolve.h,
     * lsp_dict_dispatch). Gated to the literal-string-key shape the LSP handles so
     * other subscript calls (arr[i]()) are unaffected. */
    if (lang == CBM_LANG_PYTHON && strcmp(nk, "call") == 0) {
        TSNode fnf = ts_node_child_by_field_name(node, TS_FIELD("function"));
        if (!ts_node_is_null(fnf) && strcmp(ts_node_type(fnf), "subscript") == 0) {
            TSNode val = ts_node_child_by_field_name(fnf, TS_FIELD("value"));
            TSNode idx = ts_node_child_by_field_name(fnf, TS_FIELD("subscript"));
            if (!ts_node_is_null(val) && !ts_node_is_null(idx) &&
                strcmp(ts_node_type(val), "identifier") == 0 &&
                strcmp(ts_node_type(idx), "string") == 0) {
                return cbm_node_text(a, val, source);
            }
        }
    }

    if (lang == CBM_LANG_JSONNET) {
        char *c = extract_jsonnet_callee(a, node, source, nk);
        return c ? c : extract_scripting_callee(a, node, source, lang, nk);
    }
    if (lang == CBM_LANG_NICKEL) {
        char *c = extract_nickel_callee(a, node, source, nk);
        return c ? c : extract_scripting_callee(a, node, source, lang, nk);
    }
    if (lang == CBM_LANG_TYPST) {
        char *c = extract_typst_callee(a, node, source, nk);
        return c ? c : extract_scripting_callee(a, node, source, lang, nk);
    }
    if (lang == CBM_LANG_MESON) {
        char *c = extract_meson_callee(a, node, source, nk);
        return c ? c : extract_scripting_callee(a, node, source, lang, nk);
    }

    if (lang == CBM_LANG_SCSS) {
        char *c = extract_scss_callee(a, node, source, nk);
        return c ? c : extract_scripting_callee(a, node, source, lang, nk);
    }
    if (lang == CBM_LANG_CSS) {
        char *c = extract_css_callee(a, node, source, nk);
        return c ? c : extract_scripting_callee(a, node, source, lang, nk);
    }
    if (lang == CBM_LANG_LINKERSCRIPT) {
        char *c = extract_linkerscript_callee(a, node, source, nk);
        return c ? c : extract_scripting_callee(a, node, source, lang, nk);
    }
    if (lang == CBM_LANG_SQL) {
        char *c = extract_sql_callee(a, node, source, nk);
        return c ? c : extract_scripting_callee(a, node, source, lang, nk);
    }
    if (lang == CBM_LANG_COBOL) {
        char *c = extract_cobol_callee(a, node, source, nk);
        return c ? c : extract_scripting_callee(a, node, source, lang, nk);
    }
    if (lang == CBM_LANG_ELM) {
        char *c = extract_elm_callee(a, node, source, nk);
        return c ? c : extract_scripting_callee(a, node, source, lang, nk);
    }

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
    if (lang == CBM_LANG_HASKELL || lang == CBM_LANG_OCAML || lang == CBM_LANG_PURESCRIPT ||
        lang == CBM_LANG_SCALA) {
        return extract_fp_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_WOLFRAM && strcmp(nk, "apply") == 0) {
        return extract_wolfram_callee(a, node, source);
    }
    if (lang == CBM_LANG_SWIFT) {
        return extract_swift_callee(a, node, source, nk);
    }
    if (lang == CBM_LANG_VERILOG || lang == CBM_LANG_SYSTEMVERILOG) {
        char *c = extract_hdl_callee(a, node, source, nk);
        if (c) {
            return c;
        }
    }
    if (lang == CBM_LANG_VHDL) {
        char *c = extract_vhdl_callee(a, node, source, nk);
        if (c) {
            return c;
        }
    }
    if (lang == CBM_LANG_NASM) {
        char *c = extract_nasm_callee(a, node, source, nk);
        if (c) {
            return c;
        }
    }
    if (lang == CBM_LANG_LLVM_IR) {
        char *c = extract_llvm_callee(a, node, source, nk);
        if (c) {
            return c;
        }
    }
    if (lang == CBM_LANG_FUNC) {
        char *c = extract_func_callee(a, node, source, nk);
        if (c) {
            return c;
        }
    }
    if (lang == CBM_LANG_AGDA) {
        char *c = extract_agda_callee(a, node, source, nk);
        if (c) {
            return c;
        }
    }
    if (lang == CBM_LANG_NIX) {
        char *c = extract_nix_callee(a, node, source, nk);
        if (c) {
            return c;
        }
    }
    if (lang == CBM_LANG_MAKEFILE) {
        char *c = extract_make_callee(a, node, source, nk);
        if (c) {
            return c;
        }
    }
    if (lang == CBM_LANG_JUST) {
        char *c = extract_just_callee(a, node, source, nk);
        if (c) {
            return c;
        }
    }
    if (lang == CBM_LANG_PUPPET) {
        char *c = extract_puppet_callee(a, node, source, nk);
        if (c) {
            return c;
        }
    }
    if (lang == CBM_LANG_OBJECTSCRIPT_UDL || lang == CBM_LANG_OBJECTSCRIPT_ROUTINE) {
        // ##class(Pkg.Class).Method() -> "Pkg.Class.Method"
        if (strcmp(nk, "class_method_call") == 0) {
            TSNode class_ref = cbm_find_child_by_kind(node, "class_ref");
            TSNode method_name = cbm_find_child_by_kind(node, "method_name");
            if (!ts_node_is_null(class_ref) && !ts_node_is_null(method_name)) {
                TSNode cname = cbm_find_child_by_kind(class_ref, "class_name");
                if (ts_node_is_null(cname)) {
                    return NULL;
                }
                char *cls = cbm_node_text(a, cname, source);
                if (!cls || !cls[0]) {
                    return NULL;
                }
                TSNode mname_ident = ts_node_named_child_count(method_name) > 0
                                         ? ts_node_named_child(method_name, 0)
                                         : (TSNode){0};
                if (ts_node_is_null(mname_ident)) {
                    return cls;
                }
                char *meth = cbm_node_text(a, mname_ident, source);
                if (!meth || !meth[0]) {
                    return cls;
                }
                return cbm_arena_sprintf(a, "%s.%s", cls, meth);
            }
            return NULL;
        }
        // $$label^routine extrinsic / routine tag call -> the line_ref text.
        // The routine grammar keeps the leading `$$` as an unnamed token, so
        // both call forms have the same exact named callee container.
        if (strcmp(nk, "extrinsic_function") == 0 || strcmp(nk, "routine_tag_call") == 0) {
            TSNode line_ref = cbm_find_child_by_kind(node, "line_ref");
            if (!ts_node_is_null(line_ref)) {
                return cbm_node_text(a, line_ref, source);
            }
            return NULL;
        }
        // $$$Macro(...) -> raw "$$$Name" callee (expanded later in handle_calls)
        if (strcmp(nk, "macro") == 0) {
            char *raw = cbm_node_text(a, node, source);
            if (!raw || raw[0] != '$' || raw[1] != '$' || raw[2] != '$') {
                return NULL;
            }
            char *name_start = raw + 3;
            char *paren = strchr(name_start, '(');
            if (paren) {
                *paren = '\0';
            }
            if (!name_start[0]) {
                return NULL;
            }
            return cbm_arena_sprintf(a, "$$$%s", name_start);
        }
        return NULL;
    }

    return extract_scripting_callee(a, node, source, lang, nk);
}

// Extract callee name from a call node
/* #952: compose Laravel group prefixes into a route path. Walks UP from a
 * route-registration call: every enclosing anonymous function that is the
 * argument of a `->group(...)` member call contributes the `prefix('...')`
 * (or `Route::prefix('...')`) found on that group call's receiver chain.
 * Chain methods that don't shape the path (middleware, name, as, domain)
 * are skipped. Outer groups accumulate before inner ones; nested groups
 * compose left-to-right. Returns the composed path (arena) or NULL when no
 * enclosing group carries a prefix. */
enum { PHP_GROUP_WALK_MAX = 64, PHP_PREFIX_PARTS_MAX = 8 };

static const char *php_chain_prefix_arg(CBMArena *a, TSNode call_node, const char *source) {
    /* call_node is a member_call_expression / scoped_call_expression whose
     * name is `prefix`; return its first string argument (unquoted). */
    TSNode args = ts_node_child_by_field_name(call_node, TS_FIELD("arguments"));
    if (ts_node_is_null(args)) {
        return NULL;
    }
    uint32_t nc = ts_node_named_child_count(args);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode arg = ts_node_named_child(args, i);
        TSNode inner = arg;
        if (strcmp(ts_node_type(arg), "argument") == 0 && ts_node_named_child_count(arg) > 0) {
            inner = ts_node_named_child(arg, 0);
        }
        if (strcmp(ts_node_type(inner), "string") == 0 ||
            strcmp(ts_node_type(inner), "encapsed_string") == 0) {
            char *txt = cbm_node_text(a, inner, source);
            if (txt && txt[0]) {
                size_t len = strlen(txt);
                if (len >= 2 && (txt[0] == '\'' || txt[0] == '"')) {
                    txt[len - 1] = '\0';
                    txt++;
                }
                return txt[0] ? txt : NULL;
            }
        }
    }
    return NULL;
}

static const char *php_group_prefix_for_call(CBMArena *a, TSNode node, const char *source) {
    const char *parts[PHP_PREFIX_PARTS_MAX];
    int part_count = 0;
    TSNode cur = ts_node_parent(node);
    for (int depth = 0; depth < PHP_GROUP_WALK_MAX && !ts_node_is_null(cur); depth++) {
        if (strcmp(ts_node_type(cur), "anonymous_function") == 0 ||
            strcmp(ts_node_type(cur), "arrow_function") == 0) {
            /* Is this closure the argument of a ->group(...) call? Walk to
             * the enclosing call and check its method name. */
            TSNode p = ts_node_parent(cur);
            while (!ts_node_is_null(p) && strcmp(ts_node_type(p), "member_call_expression") != 0 &&
                   strcmp(ts_node_type(p), "scoped_call_expression") != 0) {
                if (strcmp(ts_node_type(p), "statement") == 0 ||
                    strcmp(ts_node_type(p), "expression_statement") == 0) {
                    break; /* left the argument position */
                }
                p = ts_node_parent(p);
            }
            if (!ts_node_is_null(p) && (strcmp(ts_node_type(p), "member_call_expression") == 0 ||
                                        strcmp(ts_node_type(p), "scoped_call_expression") == 0)) {
                TSNode gname = ts_node_child_by_field_name(p, TS_FIELD("name"));
                char *gtxt = ts_node_is_null(gname) ? NULL : cbm_node_text(a, gname, source);
                if (gtxt && strcmp(gtxt, "group") == 0) {
                    /* Scan the receiver chain for prefix('...'). */
                    TSNode recv = ts_node_child_by_field_name(p, TS_FIELD("object"));
                    for (int hops = 0; hops < PHP_GROUP_WALK_MAX && !ts_node_is_null(recv);
                         hops++) {
                        const char *rk = ts_node_type(recv);
                        if (strcmp(rk, "member_call_expression") == 0 ||
                            strcmp(rk, "scoped_call_expression") == 0) {
                            TSNode rname = ts_node_child_by_field_name(recv, TS_FIELD("name"));
                            char *rtxt =
                                ts_node_is_null(rname) ? NULL : cbm_node_text(a, rname, source);
                            if (rtxt && strcmp(rtxt, "prefix") == 0) {
                                const char *pf = php_chain_prefix_arg(a, recv, source);
                                if (pf && part_count < PHP_PREFIX_PARTS_MAX) {
                                    parts[part_count++] = pf; /* inner-first */
                                }
                                break;
                            }
                            recv = ts_node_child_by_field_name(recv, TS_FIELD("object"));
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        cur = ts_node_parent(cur);
    }
    if (part_count == 0) {
        return NULL;
    }
    /* parts[] is inner-first; compose outer-first. Ensure exactly one '/'
     * between segments and a leading '/'. */
    char buf[CBM_SZ_256];
    size_t pos = 0;
    for (int i = part_count - 1; i >= 0; i--) {
        const char *seg = parts[i];
        while (*seg == '/') {
            seg++;
        }
        size_t sl = strlen(seg);
        if (sl == 0) {
            continue;
        }
        if (pos + sl + 2 >= sizeof(buf)) {
            return NULL; /* oversized — leave path un-prefixed */
        }
        buf[pos++] = '/';
        memcpy(buf + pos, seg, sl);
        pos += sl;
        while (pos > 1 && buf[pos - 1] == '/') {
            pos--; /* strip trailing slash per segment */
        }
    }
    buf[pos] = '\0';
    return pos ? cbm_arena_strndup(a, buf, pos) : NULL;
}

static bool is_nested_verilog_call_wrapper(CBMLanguage lang, TSNode node) {
    if (lang != CBM_LANG_VERILOG || strcmp(ts_node_type(node), "subroutine_call") != 0) {
        return false;
    }

    TSNode parent = ts_node_parent(node);
    return !ts_node_is_null(parent) &&
           strcmp(ts_node_type(parent), "function_subroutine_call") == 0;
}

static char *extract_callee_name(CBMArena *a, TSNode node, const char *source, CBMLanguage lang) {
    if (call_node_is_definition_container(lang, node, source)) {
        return NULL;
    }
    if (is_nested_verilog_call_wrapper(lang, node)) {
        return NULL;
    }

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

    /* #952: PHP facade route registrations (`Route::get(...)`, a
     * scoped_call_expression) must carry the scope in the callee text — the
     * empty-resolution route fallback keys on the "::get" suffix table, and
     * the bare "get" that generic field resolution would return deliberately
     * never matches (every $obj->get() would become a route). Runs BEFORE
     * field-based resolution, which short-circuits on the `name` field.
     * Gated to the literal `Route` scope AND a route-method match: any other
     * scope (Cache::get) would suffix-match "::get" too and mint junk routes
     * from slash-prefixed keys. Known limitation: aliased facade imports are
     * not recognized. */
    if (lang == CBM_LANG_PHP && strcmp(ts_node_type(node), "scoped_call_expression") == 0) {
        TSNode scope = ts_node_child_by_field_name(node, TS_FIELD("scope"));
        TSNode mname = ts_node_child_by_field_name(node, TS_FIELD("name"));
        if (!ts_node_is_null(scope) && !ts_node_is_null(mname)) {
            char *sc = cbm_node_text(a, scope, source);
            char *mn = cbm_node_text(a, mname, source);
            if (sc && mn && strcmp(sc, "Route") == 0) {
                char *qual = cbm_arena_sprintf(a, "%s::%s", sc, mn);
                if (qual && cbm_service_pattern_route_method(qual) != NULL) {
                    return qual;
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
        call.site_start_byte = ts_node_start_byte(node);
        call.site_end_byte = ts_node_end_byte(node);
        /* An uppercase tag is only a component invocation when the TS resolver
         * proves which lexical value owns it.  In particular, a parameter or
         * local named like a module component must not fall through to the
         * textual call resolver. */
        call.requires_lsp_resolution = true;
        cbm_calls_push(&ctx->result->calls, ctx->arena, call);
    }
}

static bool kotlin_direct_unnamed_token(TSNode node, const char *token) {
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (!ts_node_is_named(child) && strcmp(ts_node_type(child), token) == 0) {
            return true;
        }
    }
    return false;
}

static const char *kotlin_binary_operator_method(TSNode node) {
    if (kotlin_direct_unnamed_token(node, "===") || kotlin_direct_unnamed_token(node, "!==")) {
        return NULL; /* identity comparison has no operator method */
    }
    if (kotlin_direct_unnamed_token(node, "==") || kotlin_direct_unnamed_token(node, "!=")) {
        return "equals";
    }
    if (kotlin_direct_unnamed_token(node, "..<"))
        return "rangeUntil";
    if (kotlin_direct_unnamed_token(node, ".."))
        return "rangeTo";
    if (kotlin_direct_unnamed_token(node, "<=") || kotlin_direct_unnamed_token(node, ">=") ||
        kotlin_direct_unnamed_token(node, "<") || kotlin_direct_unnamed_token(node, ">")) {
        return "compareTo";
    }
    if (kotlin_direct_unnamed_token(node, "+"))
        return "plus";
    if (kotlin_direct_unnamed_token(node, "-"))
        return "minus";
    if (kotlin_direct_unnamed_token(node, "*"))
        return "times";
    if (kotlin_direct_unnamed_token(node, "/"))
        return "div";
    if (kotlin_direct_unnamed_token(node, "%"))
        return "rem";
    return NULL;
}

static const char *kotlin_unary_operator_method(TSNode node, const char *kind) {
    if (strcmp(kind, "postfix_expression") == 0) {
        if (kotlin_direct_unnamed_token(node, "++"))
            return "inc";
        if (kotlin_direct_unnamed_token(node, "--"))
            return "dec";
        return NULL; /* `!!` and call/navigation suffixes are not operator calls */
    }
    if (kotlin_direct_unnamed_token(node, "++"))
        return "inc";
    if (kotlin_direct_unnamed_token(node, "--"))
        return "dec";
    if (kotlin_direct_unnamed_token(node, "!"))
        return "not";
    if (kotlin_direct_unnamed_token(node, "-"))
        return "unaryMinus";
    if (kotlin_direct_unnamed_token(node, "+"))
        return "unaryPlus";
    return NULL;
}

static bool kotlin_node_is_update(TSNode node) {
    const char *kind = ts_node_type(node);
    return (strcmp(kind, "prefix_expression") == 0 || strcmp(kind, "postfix_expression") == 0 ||
            strcmp(kind, "unary_expression") == 0) &&
           (kotlin_direct_unnamed_token(node, "++") || kotlin_direct_unnamed_token(node, "--"));
}

static bool kotlin_index_is_write_lhs(TSNode index) {
    for (TSNode parent = ts_node_parent(index); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        const char *kind = ts_node_type(parent);
        if (strcmp(kind, "assignment") == 0) {
            TSNode lhs = ts_node_child_by_field_name(parent, TS_FIELD("left"));
            if (ts_node_is_null(lhs) && ts_node_named_child_count(parent) > 0) {
                lhs = ts_node_named_child(parent, 0);
            }
            return call_node_contains(lhs, index);
        }
        if (kotlin_node_is_update(parent)) {
            return call_node_contains(parent, index);
        }
        if (strcmp(kind, "parenthesized_expression") != 0 &&
            strcmp(kind, "directly_assignable_expression") != 0 &&
            strcmp(kind, "assignable_expression") != 0 &&
            strcmp(kind, "parenthesized_directly_assignable_expression") != 0 &&
            strcmp(kind, "expression") != 0) {
            break;
        }
    }
    return false;
}

static bool kotlin_same_call_owner(const char *left, const char *right) {
    return left == right || (left && right && strcmp(left, right) == 0);
}

#if defined(CBM_KOTLIN_DEDUP_TEST_API) && CBM_KOTLIN_DEDUP_TEST_API
static _Atomic uint64_t g_kotlin_operator_dedup_comparisons = 0;

void cbm_kotlin_operator_dedup_test_reset(void) {
    atomic_store_explicit(&g_kotlin_operator_dedup_comparisons, 0, memory_order_relaxed);
}

uint64_t cbm_kotlin_operator_dedup_test_comparisons(void) {
    return atomic_load_explicit(&g_kotlin_operator_dedup_comparisons, memory_order_relaxed);
}

static void count_kotlin_operator_dedup_comparison(void) {
    atomic_fetch_add_explicit(&g_kotlin_operator_dedup_comparisons, 1, memory_order_relaxed);
}
#else
static void count_kotlin_operator_dedup_comparison(void) {}
#endif

static bool kotlin_operator_call_matches(const CBMCall *existing, const char *callee_name,
                                         const char *enclosing_func_qn, uint32_t start,
                                         uint32_t end) {
    count_kotlin_operator_dedup_comparison();
    return existing->callee_name && strcmp(existing->callee_name, callee_name) == 0 &&
           kotlin_same_call_owner(existing->enclosing_func_qn, enclosing_func_qn) &&
           existing->site_start_byte == start && existing->site_end_byte == end;
}

static void push_kotlin_operator_call(CBMExtractCtx *ctx, TSNode node, const char *callee_name,
                                      const char *enclosing_func_qn) {
    if (!callee_name || !callee_name[0])
        return;
    uint32_t start = ts_node_start_byte(node);
    uint32_t end = ts_node_end_byte(node);
    /* Equal-span Kotlin grammar wrappers are visited consecutively by the
     * unified preorder walk. Only the last emitted carrier can therefore be
     * the wrapper duplicate for this node; scanning every prior call made a
     * file with N operator expressions perform O(N^2) comparisons. */
    if (ctx->result->calls.count > 0) {
        const CBMCall *existing = &ctx->result->calls.items[ctx->result->calls.count - SKIP_ONE];
        if (kotlin_operator_call_matches(existing, callee_name, enclosing_func_qn, start, end)) {
            return;
        }
    }
    CBMCall call = {0};
    call.callee_name = callee_name;
    call.enclosing_func_qn = enclosing_func_qn;
    call.start_line = (int)ts_node_start_point(node).row + TS_LINE_OFFSET;
    call.site_start_byte = start;
    call.site_end_byte = end;
    call.requires_lsp_resolution = true;
    cbm_calls_push(&ctx->result->calls, ctx->arena, call);
}

// Kotlin operator syntax desugars to method calls, but these expressions are
// not call_expression nodes. Emit an exact-span, LSP-guarded carrier whose bare
// name can join only the semantic row for the same expression. Operator tokens
// come from direct unnamed AST children, so comments/operand text cannot alter
// the mapping. Writes through an index stay fail-closed until set/compound-set
// semantics are implemented.
static void extract_kotlin_operator_call(CBMExtractCtx *ctx, TSNode node, const char *kind,
                                         const char *enclosing_func_qn) {
    const char *op_method = NULL;
    if (strcmp(kind, "binary_expression") == 0 || strcmp(kind, "additive_expression") == 0 ||
        strcmp(kind, "multiplicative_expression") == 0 ||
        strcmp(kind, "comparison_expression") == 0 || strcmp(kind, "equality_expression") == 0 ||
        strcmp(kind, "range_expression") == 0) {
        if (ts_node_named_child_count(node) < 2)
            return;
        op_method = kotlin_binary_operator_method(node);
    } else if (strcmp(kind, "unary_expression") == 0 || strcmp(kind, "prefix_expression") == 0 ||
               strcmp(kind, "postfix_expression") == 0) {
        if (ts_node_named_child_count(node) < 1)
            return;
        op_method = kotlin_unary_operator_method(node, kind);
    } else if (strcmp(kind, "in_expression") == 0 || strcmp(kind, "check_expression") == 0) {
        if (ts_node_named_child_count(node) < 2 ||
            (!kotlin_direct_unnamed_token(node, "in") &&
             !kotlin_direct_unnamed_token(node, "!in")) ||
            kotlin_direct_unnamed_token(node, "is")) {
            return;
        }
        op_method = "contains";
    } else if (strcmp(kind, "index_expression") == 0 || strcmp(kind, "indexing_expression") == 0) {
        if (ts_node_named_child_count(node) < 1 || kotlin_index_is_write_lhs(node))
            return;
        op_method = "get";
    } else {
        return;
    }
    push_kotlin_operator_call(ctx, node, op_method, enclosing_func_qn);
}

// Kotlin convention-desugared calls that the call walk never sees as
// call_expressions: `val (a,b) = e` -> e.component1()/e.component2(); and
// `for (x in e)` -> e.iterator()/hasNext()/next(). Record textual calls to those
// operator-convention method names so the LSP's lsp_kt_destructure /
// lsp_kt_iterator resolutions have a call site to join (names match the LSP's).
static void kt_push_implicit_call(CBMExtractCtx *ctx, TSNode node, const char *callee,
                                  const char *enclosing_func_qn) {
    CBMCall call = {0};
    call.callee_name = callee;
    call.enclosing_func_qn = enclosing_func_qn;
    call.start_line = (int)ts_node_start_point(node).row + TS_LINE_OFFSET;
    call.site_start_byte = ts_node_start_byte(node);
    call.site_end_byte = ts_node_end_byte(node);
    call.requires_lsp_resolution = true;
    cbm_calls_push(&ctx->result->calls, ctx->arena, call);
}

static void push_cpp_semantic_call_candidate(CBMExtractCtx *ctx, TSNode node,
                                             const char *callee_name,
                                             const char *enclosing_func_qn) {
    if (!callee_name || !callee_name[0] || !enclosing_func_qn) {
        return;
    }
    CBMCall call = {0};
    call.callee_name = callee_name;
    call.enclosing_func_qn = enclosing_func_qn;
    call.start_line = (int)ts_node_start_point(node).row + TS_LINE_OFFSET;
    call.site_start_byte = ts_node_start_byte(node);
    call.site_end_byte = ts_node_end_byte(node);
    call.requires_lsp_resolution = true;
    cbm_calls_push(&ctx->result->calls, ctx->arena, call);
}

static const char *cpp_operator_token(CBMExtractCtx *ctx, TSNode node) {
    for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
        TSNode child = ts_node_child(node, i);
        if (!ts_node_is_named(child)) {
            const char *token = cbm_node_text(ctx->arena, child, ctx->source);
            if (token && token[0]) {
                return token;
            }
        }
    }
    return NULL;
}

static bool cpp_is_compound_assignment_token(const char *token) {
    return token &&
           (strcmp(token, "+=") == 0 || strcmp(token, "-=") == 0 || strcmp(token, "*=") == 0 ||
            strcmp(token, "/=") == 0 || strcmp(token, "%=") == 0 || strcmp(token, "<<=") == 0 ||
            strcmp(token, ">>=") == 0 || strcmp(token, "&=") == 0 || strcmp(token, "|=") == 0 ||
            strcmp(token, "^=") == 0);
}

// C++ operators represented by expression nodes are real invocations only when
// overload resolution finds a concrete function. Emit exact-occurrence raw
// candidates here; requires_lsp_resolution keeps primitive/pointer syntax from
// falling through to textual registry lookup and fabricating CALLS edges.
static void extract_cpp_operator_call(CBMExtractCtx *ctx, TSNode node, const char *kind,
                                      const char *enclosing_func_qn) {
    if (strcmp(kind, "subscript_expression") == 0) {
        TSNode receiver = ts_node_child_by_field_name(node, TS_FIELD("argument"));
        TSNode index = ts_node_child_by_field_name(node, TS_FIELD("index"));
        if (ts_node_is_null(receiver) && ts_node_named_child_count(node) > 0) {
            receiver = ts_node_named_child(node, 0);
        }
        if (ts_node_is_null(index) && ts_node_named_child_count(node) > 1) {
            index = ts_node_named_child(node, 1);
        }
        if (!ts_node_is_null(receiver) && !ts_node_is_null(index)) {
            push_cpp_semantic_call_candidate(ctx, node, "operator[]", enclosing_func_qn);
        }
        return;
    }

    if (strcmp(kind, "unary_expression") == 0 || strcmp(kind, "pointer_expression") == 0 ||
        strcmp(kind, "update_expression") == 0) {
        TSNode operand = ts_node_child_by_field_name(node, TS_FIELD("argument"));
        if (ts_node_is_null(operand)) {
            operand = ts_node_child_by_field_name(node, TS_FIELD("operand"));
        }
        if (ts_node_is_null(operand) && ts_node_named_child_count(node) > 0) {
            operand = ts_node_named_child(node, 0);
        }
        const char *token = cpp_operator_token(ctx, node);
        bool unary_token = token && (strcmp(token, "-") == 0 || strcmp(token, "*") == 0 ||
                                     strcmp(token, "!") == 0);
        bool update_token = token && (strcmp(token, "++") == 0 || strcmp(token, "--") == 0);
        /* `this` has the built-in pointer type in C++. Consequently `*this` is
         * always pointer dereference and can never dispatch an overloaded
         * operator*. Do not manufacture a semantic candidate for it. */
        if (!ts_node_is_null(operand) && token && strcmp(token, "*") == 0 &&
            strcmp(ts_node_type(operand), "this") == 0) {
            return;
        }
        if (!ts_node_is_null(operand) &&
            ((strcmp(kind, "update_expression") == 0 && update_token) ||
             (strcmp(kind, "update_expression") != 0 && unary_token))) {
            push_cpp_semantic_call_candidate(
                ctx, node, cbm_arena_sprintf(ctx->arena, "operator%s", token), enclosing_func_qn);
        }
        return;
    }

    if (strcmp(kind, "field_expression") == 0) {
        TSNode receiver = ts_node_child_by_field_name(node, TS_FIELD("argument"));
        TSNode member = ts_node_child_by_field_name(node, TS_FIELD("field"));
        const char *token = cpp_operator_token(ctx, node);
        if (!ts_node_is_null(receiver) && !ts_node_is_null(member) && token &&
            strcmp(token, "->") == 0) {
            push_cpp_semantic_call_candidate(ctx, node, "operator->", enclosing_func_qn);
        }
        return;
    }

    if (strcmp(kind, "assignment_expression") == 0) {
        TSNode lhs = ts_node_child_by_field_name(node, TS_FIELD("left"));
        TSNode rhs = ts_node_child_by_field_name(node, TS_FIELD("right"));
        const char *token = cpp_operator_token(ctx, node);
        if (!ts_node_is_null(lhs) && !ts_node_is_null(rhs) &&
            cpp_is_compound_assignment_token(token)) {
            push_cpp_semantic_call_candidate(
                ctx, node, cbm_arena_sprintf(ctx->arena, "operator%s", token), enclosing_func_qn);
        }
        return;
    }

    if (strcmp(kind, "binary_expression") != 0) {
        return;
    }
    TSNode lhs = ts_node_child_by_field_name(node, TS_FIELD("left"));
    TSNode rhs = ts_node_child_by_field_name(node, TS_FIELD("right"));
    const char *token = cpp_operator_token(ctx, node);
    if (!ts_node_is_null(lhs) && !ts_node_is_null(rhs) && token) {
        push_cpp_semantic_call_candidate(
            ctx, node, cbm_arena_sprintf(ctx->arena, "operator%s", token), enclosing_func_qn);
    }
}

// C++ implicit calls that produce no textual call node: the destructor
// (`delete p`), the copy/move constructor (`T a = b;` copy-init), and the
// conversion operator (`if (obj)` where obj has `operator bool`). The c-LSP
// resolves each to the corresponding member but there is no call site to join
// to (callable=0). Synthesize a textual call sourced to the enclosing function
// so the lsp_{destructor,copy_constructor,conversion} resolution binds.
//
//   - destructor: the callee QN embeds the type (`T.~T`), which is not textually
//     available from `delete p`. Use a non-textual destructor marker; the exact
//     occurrence is joined only to an LSP-proved, materialized destructor and is
//     rewritten to `~T` when the same-file pass can prove it.
//   - copy constructor: the callee short-name is the constructed type (`T`),
//     which IS textually present as the declaration's type — join by short-name.
//   - conversion: the callee short-name is the type-independent `operator bool`.
//
// Spurious synthesis (a condition/operand that has no such member) resolves to
// nothing and is dropped, so no extra edge is produced.
static void extract_cpp_implicit_calls(CBMExtractCtx *ctx, TSNode node, const char *kind,
                                       const char *enclosing_func_qn) {
    const char *callee = NULL;
    if (strcmp(kind, "delete_expression") == 0) {
        TSNode operand = ts_node_child_by_field_name(node, TS_FIELD("argument"));
        if (ts_node_is_null(operand) && ts_node_named_child_count(node) > 0) {
            operand = ts_node_named_child(node, 0);
        }
        if (!ts_node_is_null(operand)) {
            callee = "~";
        }
    } else if (strcmp(kind, "if_statement") == 0 || strcmp(kind, "while_statement") == 0 ||
               strcmp(kind, "do_statement") == 0) {
        // `if (obj)` invokes obj's `operator bool`. Only a lone-identifier
        // condition triggers it; comparisons/logical exprs evaluate to bool.
        TSNode cond = ts_node_child_by_field_name(node, TS_FIELD("condition"));
        if (!ts_node_is_null(cond)) {
            TSNode inner = cond;
            if (strcmp(ts_node_type(cond), "condition_clause") == 0 &&
                ts_node_named_child_count(cond) == 1) {
                inner = ts_node_named_child(cond, 0);
            }
            if (strcmp(ts_node_type(inner), "identifier") == 0) {
                callee = "operator bool";
            }
        }
    } else if (strcmp(kind, "declaration") == 0) {
        // `T a = b;` — copy-init from an identifier invokes T's copy constructor.
        TSNode type = ts_node_child_by_field_name(node, TS_FIELD("type"));
        TSNode decl = ts_node_child_by_field_name(node, TS_FIELD("declarator"));
        if (!ts_node_is_null(type) && !ts_node_is_null(decl) &&
            strcmp(ts_node_type(decl), "init_declarator") == 0) {
            TSNode value = ts_node_child_by_field_name(decl, TS_FIELD("value"));
            if (!ts_node_is_null(value) && strcmp(ts_node_type(value), "identifier") == 0) {
                char *tn = cbm_node_text(ctx->arena, type, ctx->source);
                if (tn) {
                    const char *colon = strrchr(tn, ':');
                    callee = colon ? colon + 1 : tn;
                }
            }
        }
    }
    if (callee && callee[0]) {
        push_cpp_semantic_call_candidate(ctx, node, callee, enclosing_func_qn);
    }
}

static void extract_kotlin_desugared_calls(CBMExtractCtx *ctx, TSNode node, const char *kind,
                                           const char *enclosing_func_qn) {
    if (strcmp(kind, "property_declaration") == 0) {
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_named_child(node, i);
            if (strcmp(ts_node_type(c), "multi_variable_declaration") != 0) {
                continue;
            }
            // One componentN() call per destructured variable.
            uint32_t vc = ts_node_named_child_count(c);
            uint32_t comp = 0;
            for (uint32_t j = 0; j < vc; j++) {
                TSNode v = ts_node_named_child(c, j);
                if (strcmp(ts_node_type(v), "variable_declaration") != 0) {
                    continue;
                }
                comp++;
                kt_push_implicit_call(ctx, node, cbm_arena_sprintf(ctx->arena, "component%u", comp),
                                      enclosing_func_qn);
            }
            break;
        }
    } else if (strcmp(kind, "for_statement") == 0) {
        kt_push_implicit_call(ctx, node, "iterator", enclosing_func_qn);
        kt_push_implicit_call(ctx, node, "hasNext", enclosing_func_qn);
        kt_push_implicit_call(ctx, node, "next", enclosing_func_qn);
    }
}

static bool php_is_first_class_callable(TSNode node) {
    TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
    if (ts_node_is_null(args)) {
        return false;
    }
    if (strcmp(ts_node_type(args), "variadic_placeholder") == 0) {
        return true;
    }
    uint32_t count = ts_node_named_child_count(args);
    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(ts_node_type(ts_node_named_child(args, i)), "variadic_placeholder") == 0) {
            return true;
        }
    }
    return false;
}

// ObjectScript: resolve `var.Method(...)` / `..Property.Method(...)` instance
// calls against the per-method variable type map. Returns arena "Class.Method"
// or NULL if the receiver's type is unknown.
static char *resolve_objectscript_instance_call(CBMArena *a, TSNode node, const char *source,
                                                os_type_map_t *type_map) {
    TSNode receiver = {0};
    TSNode oref = {0};
    const char *nk_first = NULL;
    for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "lvn") == 0 || strcmp(ck, "variable") == 0) {
            receiver = child;
        } else if (strcmp(ck, "relative_dot_property") == 0) {
            receiver = child;
            nk_first = "relative_dot_property";
        } else if (strcmp(ck, "oref_method") == 0) {
            oref = child;
        }
    }
    if (ts_node_is_null(oref)) {
        return NULL;
    }
    TSNode method_name_node = cbm_find_child_by_kind(oref, "method_name");
    if (ts_node_is_null(method_name_node)) {
        return NULL;
    }
    TSNode mn_ident = ts_node_named_child_count(method_name_node) > 0
                          ? ts_node_named_child(method_name_node, 0)
                          : (TSNode){0};
    if (ts_node_is_null(mn_ident)) {
        return NULL;
    }
    char *method = cbm_node_text(a, mn_ident, source);
    if (!method || !method[0]) {
        return NULL;
    }
    if (ts_node_is_null(receiver)) {
        return NULL;
    }
    char *var_text = NULL;
    if (nk_first && strcmp(nk_first, "relative_dot_property") == 0) {
        TSNode prop_name = cbm_find_child_by_kind(receiver, "member_name");
        if (!ts_node_is_null(prop_name)) {
            char *pname = cbm_node_text(a, prop_name, source);
            if (pname && pname[0]) {
                var_text = cbm_arena_sprintf(a, "..%s", pname);
            }
        }
        if (!var_text) {
            var_text = cbm_node_text(a, receiver, source);
        }
    } else {
        var_text = cbm_node_text(a, receiver, source);
    }
    if (!var_text || !var_text[0]) {
        return NULL;
    }
    for (int i = 0; i < type_map->count; i++) {
        if (strcasecmp(type_map->entries[i].var_name, var_text) == 0) {
            return cbm_arena_sprintf(a, "%s.%s", type_map->entries[i].class_name, method);
        }
    }
    return NULL;
}

static bool is_objectscript_language(CBMLanguage language) {
    return language == CBM_LANG_OBJECTSCRIPT_UDL || language == CBM_LANG_OBJECTSCRIPT_ROUTINE;
}

// Preserve ObjectScript's grammar/type-map resolution while feeding the
// invocation descriptor used by the exact callee-usage suppression pass.
static const char *resolve_objectscript_callee(CBMExtractCtx *ctx, TSNode node, WalkState *state,
                                               const char *callee) {
    if (!is_objectscript_language(ctx->language)) {
        return callee;
    }

    const char *kind = ts_node_type(node);
    if ((!callee || !callee[0]) && strcmp(kind, "method_call") == 0) {
        callee =
            resolve_objectscript_instance_call(ctx->arena, node, ctx->source, &state->os_type_map);
    }

    if ((!callee || !callee[0]) && strcmp(kind, "relative_dot_method") == 0 &&
        state->enclosing_class_qn && state->enclosing_class_qn[0]) {
        TSNode oref = cbm_find_child_by_kind(node, "oref_method");
        TSNode method_name =
            ts_node_is_null(oref) ? (TSNode){0} : cbm_find_child_by_kind(oref, "method_name");
        TSNode identifier =
            !ts_node_is_null(method_name) && ts_node_named_child_count(method_name) > 0
                ? ts_node_named_child(method_name, 0)
                : (TSNode){0};
        if (!ts_node_is_null(identifier)) {
            char *method = cbm_node_text(ctx->arena, identifier, ctx->source);
            if (method && method[0]) {
                callee = cbm_arena_sprintf(ctx->arena, "%s.%s", state->enclosing_class_qn, method);
            }
        }
    }

    if (callee && strncmp(callee, "$$$", 3) == 0 && ctx->macro_table) {
        const CBMMacroEntry *entry = cbm_macro_table_find(ctx->macro_table, callee + 3);
        if (entry) {
            if (entry->resolved_callee) {
                callee = cbm_arena_strdup(ctx->arena, entry->resolved_callee);
            } else if (entry->expansion) {
                callee = cbm_macro_extract_callee(ctx->arena, entry->expansion);
            } else {
                callee = NULL;
            }
        }
    }
    return callee;
}

static TSNode objectscript_call_args(TSNode node) {
    TSNode oref = cbm_find_child_by_kind(node, "oref_method");
    if (!ts_node_is_null(oref)) {
        TSNode args = cbm_find_child_by_kind(oref, "method_args");
        if (!ts_node_is_null(args)) {
            return args;
        }
    }

    TSNode args = cbm_find_child_by_kind(node, "method_args");
    if (!ts_node_is_null(args)) {
        return args;
    }

    /* A macro's optional arguments live below macro_function rather than as a
     * direct child of macro. Keep them as call arguments/data-flow inputs; the
     * macro name is an unnamed token and is handled separately. */
    TSNode macro_function = cbm_find_child_by_kind(node, "macro_function");
    return ts_node_is_null(macro_function) ? (TSNode){0}
                                           : cbm_find_child_by_kind(macro_function, "method_args");
}

static bool node_has_token(TSNode node, const char *token) {
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(ts_node_type(ts_node_child(node, i)), token) == 0) {
            return true;
        }
    }
    return false;
}

static bool is_callable_reference_value(CBMLanguage language, TSNode node) {
    const char *kind = ts_node_type(node);
    if (language == CBM_LANG_JAVA && strcmp(kind, "method_reference") == 0) {
        return true;
    }
    if (language == CBM_LANG_KOTLIN && strcmp(kind, "callable_reference") == 0) {
        // `Type::class` shares the callable-reference grammar node but denotes
        // a class literal, not a reference to executable code.
        return !node_has_token(node, "class");
    }
    return language == CBM_LANG_PHP && php_is_first_class_callable(node);
}

static TSNode first_present_field(TSNode node, const char *const *fields) {
    for (const char *const *field = fields; *field; field++) {
        TSNode child = ts_node_child_by_field_name(node, *field, (uint32_t)strlen(*field));
        if (!ts_node_is_null(child)) {
            return child;
        }
    }
    return (TSNode){0};
}

static bool is_constructor_kind(const char *kind) {
    return strcmp(kind, "new_expression") == 0 || strcmp(kind, "object_creation_expression") == 0 ||
           strcmp(kind, "instance_expression") == 0 || strcmp(kind, "constructor_expression") == 0;
}

static bool is_dynamic_callee_expr(const CBMExtractCtx *ctx, TSNode expr) {
    if (ts_node_is_null(expr)) {
        return false;
    }
    const char *kind = ts_node_type(expr);
    if (strstr(kind, "subscript") || strstr(kind, "element_access") ||
        strstr(kind, "index_expression") || strstr(kind, "computed")) {
        return true;
    }

    // JS/TS use member_expression for both `obj.method` and `obj[key]`.
    // A bracketed property is an evaluated value, not a static terminal name.
    static const char *const terminal_fields[] = {"property", "field",  "attribute",
                                                  "name",     "method", NULL};
    TSNode terminal = first_present_field(expr, terminal_fields);
    if (!ts_node_is_null(terminal)) {
        uint32_t start = ts_node_start_byte(terminal);
        uint32_t expr_start = ts_node_start_byte(expr);
        while (start > expr_start && isspace((unsigned char)ctx->source[start - 1])) {
            start--;
        }
        if (start > expr_start && ctx->source[start - 1] == '[') {
            return true;
        }
    }
    return false;
}

static TSNode primary_callee_expr(TSNode node) {
    const char *kind = ts_node_type(node);
    if (is_constructor_kind(kind)) {
        static const char *const constructor_fields[] = {"constructor", "type", "name", NULL};
        TSNode constructor = first_present_field(node, constructor_fields);
        if (!ts_node_is_null(constructor)) {
            return constructor;
        }
    }

    static const char *const callee_fields[] = {"function", "name",       "method", "target",
                                                "macro",    "subroutine", NULL};
    TSNode callee = first_present_field(node, callee_fields);
    if (!ts_node_is_null(callee)) {
        return callee;
    }
    if (ts_node_named_child_count(node) > 0) {
        return ts_node_named_child(node, 0);
    }
    return (TSNode){0};
}

static TSNode terminal_callee_leaf(const CBMExtractCtx *ctx, TSNode expr) {
    if (ts_node_is_null(expr) || is_dynamic_callee_expr(ctx, expr)) {
        return (TSNode){0};
    }

    static const char *const terminal_fields[] = {"property", "field", "attribute",
                                                  "method",   "name",  NULL};
    TSNode terminal = first_present_field(expr, terminal_fields);
    if (!ts_node_is_null(terminal)) {
        if (is_dynamic_callee_expr(ctx, expr)) {
            return (TSNode){0};
        }
        return terminal_callee_leaf(ctx, terminal);
    }

    const char *kind = ts_node_type(expr);
    if (strcmp(kind, "identifier") == 0 || strcmp(kind, "simple_identifier") == 0 ||
        strcmp(kind, "type_identifier") == 0 || strcmp(kind, "field_identifier") == 0 ||
        strcmp(kind, "property_identifier") == 0 || strcmp(kind, "name") == 0 ||
        strcmp(kind, "variable_name") == 0 || strcmp(kind, "attribute") == 0 ||
        strcmp(kind, "symbol") == 0 || strcmp(kind, "sym_lit") == 0 ||
        strcmp(kind, "user_symbol") == 0) {
        return expr;
    }

    // Known wrappers keep their static terminal in one of their named children.
    // Prefer the last child for qualified/member/navigation forms and the first
    // for transparent parentheses and generic wrappers whose name field was not
    // exposed by the grammar.
    uint32_t count = ts_node_named_child_count(expr);
    if (count == 0) {
        return (TSNode){0};
    }
    bool terminal_is_last = strstr(kind, "qualified") || strstr(kind, "scoped") ||
                            strstr(kind, "member") || strstr(kind, "selector") ||
                            strstr(kind, "navigation") || strstr(kind, "field_expression");
    TSNode child = ts_node_named_child(expr, terminal_is_last ? count - 1 : 0);
    return terminal_callee_leaf(ctx, child);
}

typedef struct {
    const char *name;
    TSNode expr;
    TSNode leaf;
} CBMPrimaryCalleeSelection;

static bool node_kind_is_one_of(TSNode node, const char *const *kinds) {
    const char *kind = ts_node_type(node);
    for (const char *const *candidate = kinds; *candidate; candidate++) {
        if (strcmp(kind, *candidate) == 0) {
            return true;
        }
    }
    return false;
}

static TSNode first_named_descendant_of_kind(TSNode node, const char *const *kinds,
                                             int remaining_depth) {
    if (ts_node_is_null(node) || remaining_depth < 0) {
        return (TSNode){0};
    }
    if (node_kind_is_one_of(node, kinds)) {
        return node;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode found = first_named_descendant_of_kind(ts_node_named_child(node, i), kinds,
                                                      remaining_depth - 1);
        if (!ts_node_is_null(found)) {
            return found;
        }
    }
    return (TSNode){0};
}

static TSNode last_named_descendant_of_kind(TSNode node, const char *const *kinds,
                                            int remaining_depth) {
    if (ts_node_is_null(node) || remaining_depth < 0) {
        return (TSNode){0};
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = count; i > 0; i--) {
        TSNode found = last_named_descendant_of_kind(ts_node_named_child(node, i - 1), kinds,
                                                     remaining_depth - 1);
        if (!ts_node_is_null(found)) {
            return found;
        }
    }
    return node_kind_is_one_of(node, kinds) ? node : (TSNode){0};
}

/* ObjectScript call nodes do not expose generic `function`/`name` fields. Keep
 * the descriptor on the grammar-proven callee occurrence instead of falling
 * back to the first named child (which is a receiver for method_call and an
 * argument container for macro). The macro spelling itself is unnamed, so it
 * intentionally has no named callee occurrence to consume. */
static TSNode objectscript_callee_expr(TSNode node) {
    const char *kind = ts_node_type(node);

    if (strcmp(kind, "class_method_call") == 0) {
        return cbm_find_child_by_kind(node, "method_name");
    }
    if (strcmp(kind, "method_call") == 0 || strcmp(kind, "relative_dot_method") == 0) {
        TSNode oref = cbm_find_child_by_kind(node, "oref_method");
        return ts_node_is_null(oref) ? (TSNode){0} : cbm_find_child_by_kind(oref, "method_name");
    }
    if (strcmp(kind, "extrinsic_function") == 0 || strcmp(kind, "routine_tag_call") == 0) {
        return cbm_find_child_by_kind(node, "line_ref");
    }
    return (TSNode){0};
}

static TSNode objectscript_callee_leaf(TSNode expr) {
    if (ts_node_is_null(expr)) {
        return (TSNode){0};
    }
    const char *kind = ts_node_type(expr);
    if (strcmp(kind, "method_name") == 0 && ts_node_named_child_count(expr) > 0) {
        return ts_node_named_child(expr, 0);
    }
    if (strcmp(kind, "line_ref") == 0) {
        static const char *const identifier_kinds[] = {"objectscript_identifier",
                                                       "objectscript_identifier_special", NULL};
        return first_named_descendant_of_kind(expr, identifier_kinds, 4);
    }
    return (TSNode){0};
}

static TSNode language_specific_callee_expr(CBMLanguage language, TSNode node) {
    const char *kind = ts_node_type(node);

    if (is_objectscript_language(language)) {
        return objectscript_callee_expr(node);
    }

    if (language == CBM_LANG_SCALA && strcmp(kind, "infix_expression") == 0) {
        /* Scala exposes `lhs merge rhs` as left/operator/right fields. Consume
         * only the operator occurrence; both operands remain ordinary usages. */
        return ts_node_child_by_field_name(node, TS_FIELD("operator"));
    }

    if (language == CBM_LANG_DART) {
        if (strcmp(kind, "selector") == 0) {
            TSNode previous = ts_node_prev_named_sibling(node);
            return !ts_node_is_null(previous) && strcmp(ts_node_type(previous), "identifier") == 0
                       ? previous
                       : (TSNode){0};
        }
        if (strcmp(kind, "new_expression") == 0) {
            return primary_callee_expr(node);
        }
    }

    if (language == CBM_LANG_VHDL && strcmp(kind, "parenthesis_group") == 0) {
        TSNode previous = ts_node_prev_named_sibling(node);
        if (!ts_node_is_null(previous)) {
            const char *previous_kind = ts_node_type(previous);
            if (strcmp(previous_kind, "library_function") == 0 ||
                strcmp(previous_kind, "identifier") == 0 || strcmp(previous_kind, "name") == 0 ||
                strcmp(previous_kind, "simple_name") == 0) {
                return previous;
            }
        }
        return (TSNode){0};
    }

    if (language == CBM_LANG_CSS && strcmp(kind, "call_expression") == 0) {
        return cbm_find_child_by_kind(node, "function_name");
    }

    if (language == CBM_LANG_NASM) {
        if (strcmp(kind, "call_syntax_expression") == 0) {
            return ts_node_child_by_field_name(node, TS_FIELD("base"));
        }
        if (strcmp(kind, "actual_instruction") == 0) {
            TSNode operands = ts_node_child_by_field_name(node, TS_FIELD("operands"));
            return !ts_node_is_null(operands) && ts_node_named_child_count(operands) > 0
                       ? ts_node_named_child(operands, 0)
                       : (TSNode){0};
        }
    }

    if (language == CBM_LANG_LLVM_IR) {
        return ts_node_child_by_field_name(node, TS_FIELD("callee"));
    }

    if (language == CBM_LANG_AGDA && strcmp(kind, "expr") == 0) {
        return agda_application_head_atom(node);
    }

    if (language == CBM_LANG_ADA &&
        (strcmp(kind, "procedure_call_statement") == 0 || strcmp(kind, "function_call") == 0)) {
        TSNode name = ts_node_child_by_field_name(node, TS_FIELD("name"));
        if (!ts_node_is_null(name)) {
            return name;
        }
        if (ts_node_named_child_count(node) > 0) {
            TSNode head = ts_node_named_child(node, 0);
            const char *head_kind = ts_node_type(head);
            if (strcmp(head_kind, "name") == 0 || strcmp(head_kind, "identifier") == 0) {
                return head;
            }
        }
        return (TSNode){0};
    }

    if (language == CBM_LANG_MAKEFILE) {
        // `shell` is a literal keyword minted by the extractor, not an AST
        // identifier. It must not consume the command/argument child.
        if (strcmp(kind, "shell_function") == 0) {
            return (TSNode){0};
        }
        if (strcmp(kind, "function_call") == 0) {
            TSNode function = ts_node_child_by_field_name(node, TS_FIELD("function"));
            if (ts_node_is_null(function) && ts_node_named_child_count(node) > 0) {
                function = ts_node_named_child(node, 0);
            }
            return function;
        }
    }

    // A Just dependency is build-graph metadata. It deliberately emits the
    // legacy raw call while retaining the recipe name as an ordinary USAGE.
    if (language == CBM_LANG_JUST && strcmp(kind, "dependency") == 0) {
        return (TSNode){0};
    }

    // Puppet `include` is another synthetic literal callee; its named child is
    // the included class and must remain a value/reference occurrence.
    if (language == CBM_LANG_PUPPET && strcmp(kind, "include_statement") == 0) {
        return (TSNode){0};
    }

    return primary_callee_expr(node);
}

static TSNode language_specific_callee_leaf(CBMLanguage language, TSNode expr,
                                            const CBMExtractCtx *ctx) {
    if (ts_node_is_null(expr)) {
        return (TSNode){0};
    }

    if (language == CBM_LANG_LLVM_IR) {
        static const char *const llvm_leaf_kinds[] = {"global_var", "local_var", NULL};
        return first_named_descendant_of_kind(expr, llvm_leaf_kinds, 8);
    }
    if (is_objectscript_language(language)) {
        return objectscript_callee_leaf(expr);
    }
    if (language == CBM_LANG_CSS && strcmp(ts_node_type(expr), "function_name") == 0) {
        return expr;
    }
    if (language == CBM_LANG_NASM) {
        static const char *const nasm_leaf_kinds[] = {"word", NULL};
        return first_named_descendant_of_kind(expr, nasm_leaf_kinds, 8);
    }
    if (language == CBM_LANG_AGDA) {
        return agda_head_qid(expr);
    }
    if (language == CBM_LANG_ELM) {
        static const char *const elm_leaf_kinds[] = {"lower_case_identifier", NULL};
        return first_named_descendant_of_kind(expr, elm_leaf_kinds, 8);
    }
    if (language == CBM_LANG_PURESCRIPT) {
        static const char *const purescript_leaf_kinds[] = {"variable", NULL};
        return first_named_descendant_of_kind(expr, purescript_leaf_kinds, 8);
    }
    if (language == CBM_LANG_NICKEL) {
        static const char *const nickel_leaf_kinds[] = {"ident", NULL};
        return first_named_descendant_of_kind(expr, nickel_leaf_kinds, 8);
    }
    if (language == CBM_LANG_ADA) {
        static const char *const ada_leaf_kinds[] = {"identifier", "simple_identifier", NULL};
        TSNode leaf = last_named_descendant_of_kind(expr, ada_leaf_kinds, 8);
        if (!ts_node_is_null(leaf)) {
            return leaf;
        }
    }
    if (language == CBM_LANG_VHDL) {
        static const char *const vhdl_leaf_kinds[] = {"identifier", "simple_identifier", NULL};
        TSNode leaf = last_named_descendant_of_kind(expr, vhdl_leaf_kinds, 8);
        if (!ts_node_is_null(leaf)) {
            return leaf;
        }
    }
    return terminal_callee_leaf(ctx, expr);
}

// Produce the textual callee and the AST occurrence from one operation. The
// generic fallback retains the former field/terminal behavior, while grammars
// whose extractor uses a sibling, a special field, or a synthetic literal are
// selected explicitly above. This prevents the descriptor from independently
// guessing that the first named child is the callee (often an argument).
static CBMPrimaryCalleeSelection select_primary_callee(CBMExtractCtx *ctx, TSNode node,
                                                       WalkState *state) {
    CBMPrimaryCalleeSelection selection = {0};
    selection.name = extract_callee_name(ctx->arena, node, ctx->source, ctx->language);
    selection.name = resolve_objectscript_callee(ctx, node, state, selection.name);
    if (!selection.name || !selection.name[0]) {
        return selection;
    }

    const char *site_kind = ts_node_type(node);
    bool scala_named_infix =
        ctx->language == CBM_LANG_SCALA && strcmp(site_kind, "infix_expression") == 0;
    if (!scala_named_infix && (strstr(site_kind, "binary") || strstr(site_kind, "infix"))) {
        return selection;
    }

    selection.expr = language_specific_callee_expr(ctx->language, node);
    if (is_dynamic_callee_expr(ctx, selection.expr)) {
        selection.expr = (TSNode){0};
        return selection;
    }
    selection.leaf = language_specific_callee_leaf(ctx->language, selection.expr, ctx);
    return selection;
}

static bool primary_callee_name_is_allowed(const CBMExtractCtx *ctx,
                                           const CBMPrimaryCalleeSelection *callee) {
    if (!callee->name || !callee->name[0]) {
        return false;
    }
    if (!cbm_is_keyword(callee->name, ctx->language) ||
        cbm_is_resolvable_builtin(callee->name, ctx->language)) {
        return true;
    }
    /* CSS function names occupy a grammar-proven callable role. Generic keyword
     * vocabulary includes `var`, but that must not erase `var(...)`. */
    return ctx->language == CBM_LANG_CSS && !ts_node_is_null(callee->expr) &&
           strcmp(ts_node_type(callee->expr), "function_name") == 0;
}

static bool php_callable_reference_has_dynamic_member_name(TSNode call, TSNode callee_expr) {
    if (ts_node_is_null(callee_expr)) {
        return false;
    }

    const char *call_kind = ts_node_type(call);
    if (strcmp(call_kind, "member_call_expression") != 0 &&
        strcmp(call_kind, "nullsafe_member_call_expression") != 0 &&
        strcmp(call_kind, "scoped_call_expression") != 0) {
        return false;
    }

    const char *callee_kind = ts_node_type(callee_expr);
    return strcmp(callee_kind, "variable_name") == 0 ||
           strcmp(callee_kind, "dynamic_variable_name") == 0;
}

/* A callable-reference token inside a branch expression names a possible value,
 * not one statically selected value at that occurrence. Do not consume its leaf
 * as explicit CALL_REFERENCE syntax; the usage walker will retain each named
 * alternative as ordinary USAGE. */
static bool callable_reference_is_branch_alternative(TSNode reference) {
    for (TSNode parent = ts_node_parent(reference); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        const char *kind = ts_node_type(parent);
        if (strcmp(kind, "ternary_expression") == 0 ||
            strcmp(kind, "conditional_expression") == 0 || strcmp(kind, "if_expression") == 0) {
            return true;
        }
        if (strcmp(kind, "arguments") == 0 || strcmp(kind, "argument_list") == 0 ||
            strcmp(kind, "assignment_expression") == 0 || strcmp(kind, "assignment") == 0 ||
            strcmp(kind, "variable_declarator") == 0) {
            return false;
        }
    }
    return false;
}

static CBMInvocationDescriptor describe_callable_reference(CBMExtractCtx *ctx, TSNode node) {
    CBMInvocationDescriptor descriptor = {0};
    descriptor.kind = CBM_INVOCATION_CALLABLE_REFERENCE;
    descriptor.site = node;

    if (callable_reference_is_branch_alternative(node)) {
        return descriptor;
    }

    uint32_t count = ts_node_named_child_count(node);
    if (ctx->language == CBM_LANG_JAVA && count > 0) {
        // In `Type::new`, `new` is an unnamed token, so the referenced
        // constructor is represented by the left-hand type. For ordinary
        // method references, the final named child is the referenced method.
        descriptor.callee_expr =
            ts_node_named_child(node, node_has_token(node, "new") ? 0 : count - 1);
    } else if (ctx->language == CBM_LANG_KOTLIN && count > 0) {
        descriptor.callee_expr = ts_node_named_child(node, count - 1);
    } else if (ctx->language == CBM_LANG_PHP) {
        descriptor.callee_expr = primary_callee_expr(node);
        /* `$obj->$method(...)` and its nullsafe/static equivalents evaluate the
         * member-name variable; the syntax alone does not prove a callable
         * target. Leave the terminal unconsumed so the usage pass records it as
         * an ordinary value instead of a CALL_REFERENCE. A bare typed
         * `$handler(...)` remains eligible for PHP-LSP `__invoke` resolution. */
        if (php_callable_reference_has_dynamic_member_name(node, descriptor.callee_expr)) {
            descriptor.callee_expr = (TSNode){0};
            return descriptor;
        }
    }

    if (is_dynamic_callee_expr(ctx, descriptor.callee_expr)) {
        descriptor.callee_expr = (TSNode){0};
        return descriptor;
    }
    descriptor.callee_leaf = terminal_callee_leaf(ctx, descriptor.callee_expr);
    TSNode name_node =
        !ts_node_is_null(descriptor.callee_leaf) ? descriptor.callee_leaf : descriptor.callee_expr;
    if (!ts_node_is_null(name_node)) {
        descriptor.callee_name = cbm_node_text(ctx->arena, name_node, ctx->source);
    }
    return descriptor;
}

static CBMInvocationDescriptor describe_emitted_primary_call(
    TSNode node, const CBMPrimaryCalleeSelection *callee) {
    CBMInvocationDescriptor descriptor = {0};
    descriptor.kind = CBM_INVOCATION_PRIMARY;
    descriptor.site = node;
    descriptor.callee_name = callee->name;
    descriptor.raw_call_emitted = true;
    descriptor.callee_expr = callee->expr;
    descriptor.callee_leaf = callee->leaf;
    return descriptor;
}

CBMInvocationDescriptor handle_calls(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                                     WalkState *state) {
    CBMInvocationDescriptor invocation = {0};
    bool callable_reference = is_callable_reference_value(ctx->language, node);
    if (callable_reference) {
        invocation = describe_callable_reference(ctx, node);
    }

    if (!callable_reference && spec->call_node_types && spec->call_node_types[0] &&
        cbm_kind_in_set(node, spec->call_node_types)) {
        CBMPrimaryCalleeSelection callee = select_primary_callee(ctx, node, state);
        // Keyword-filter callees, but keep builtins we mint a node for (len, str,
        // ...) so the LSP-resolved builtin call still forms a CALLS edge.
        if (primary_callee_name_is_allowed(ctx, &callee)) {
            CBMCall call = {0};
            call.callee_name = callee.name;
            call.enclosing_func_qn = state->enclosing_func_qn;
            call.loop_depth = state->loop_depth;     // enclosing loop nesting at this call
            call.branch_depth = state->branch_depth; // enclosing branch nesting at this call
            call.start_line = (int)ts_node_start_point(node).row + TS_LINE_OFFSET;
            call.site_start_byte = ts_node_start_byte(node);
            call.site_end_byte = ts_node_end_byte(node);
            // Perl-only: flag arrow/method calls ($obj->m / Class->m). The
            // generic short-name resolver cannot place a method without a known
            // receiver type, so the call-resolution pass suppresses those edges.
            // Default false for every other language (struct is zero-init).
            if (ctx->language == CBM_LANG_PERL &&
                strcmp(ts_node_type(node), "method_call_expression") == 0) {
                call.is_method = true;
            }
            // TS/JS/TSX receiver-aware guard (#592/#606 direction; same intent
            // as the Perl flag above). Flag a member call x.foo() whose receiver
            // is NOT `this`/`super`. When the TS-LSP cannot resolve the receiver
            // type, the call-resolution pass suppresses weak short-name matches
            // for these (so `re.test()` cannot fabricate an edge to a project
            // `test`). this/super receivers stay unflagged — their target is the
            // enclosing class, where a namespace-proximity weak match is usually
            // right. Bare calls (helper()) and new_expression have no member
            // receiver, so they keep is_method=false (struct is zero-init).
            if ((ctx->language == CBM_LANG_JAVASCRIPT || ctx->language == CBM_LANG_TYPESCRIPT ||
                 ctx->language == CBM_LANG_TSX) &&
                strcmp(ts_node_type(node), "call_expression") == 0) {
                TSNode fn = ts_node_child_by_field_name(node, TS_FIELD("function"));
                if (!ts_node_is_null(fn) && strcmp(ts_node_type(fn), "member_expression") == 0) {
                    TSNode obj = ts_node_child_by_field_name(fn, TS_FIELD("object"));
                    if (!ts_node_is_null(obj)) {
                        const char *ok = ts_node_type(obj);
                        if (strcmp(ok, "this") != 0 && strcmp(ok, "super") != 0) {
                            call.is_method = true;
                        }
                    }
                }
            }

            TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
            // ObjectScript stores args under oref_method/method_args, not the
            // generic "arguments" field; macro arguments add one wrapper.
            if (ts_node_is_null(args) && is_objectscript_language(ctx->language)) {
                args = objectscript_call_args(node);
            }
            if (!ts_node_is_null(args)) {
                call.first_string_arg = extract_url_or_topic_arg(ctx, args);
                /* #952: routes registered inside Laravel `prefix()->group()`
                 * closures must carry the composed path — the resolve passes
                 * only see the flat CBMCall, so the enclosing chain can only
                 * be read here where the AST still exists. */
                if (ctx->language == CBM_LANG_PHP && call.first_string_arg &&
                    call.first_string_arg[0] == '/' && call.callee_name &&
                    cbm_service_pattern_route_method(call.callee_name) != NULL) {
                    const char *gp = php_group_prefix_for_call(ctx->arena, node, ctx->source);
                    if (gp && gp[0]) {
                        const char *rel = call.first_string_arg;
                        while (*rel == '/') {
                            rel++;
                        }
                        call.first_string_arg =
                            rel[0] ? cbm_arena_sprintf(ctx->arena, "%s/%s", gp, rel)
                                   : cbm_arena_strndup(ctx->arena, gp, strlen(gp));
                    }
                }
                if (call.first_string_arg && call.first_string_arg[0] == '/') {
                    call.second_arg_name = extract_handler_arg(ctx, args);
                }
                if (ctx->language == CBM_LANG_OBJECTSCRIPT_UDL ||
                    ctx->language == CBM_LANG_OBJECTSCRIPT_ROUTINE) {
                    for (uint32_t ai = 0;
                         ai < ts_node_named_child_count(args) && call.arg_count < CBM_MAX_CALL_ARGS;
                         ai++) {
                        TSNode achild = ts_node_named_child(args, ai);
                        const char *ack = ts_node_type(achild);
                        if (strcmp(ack, "bracket") == 0) {
                            continue;
                        }
                        if (strcmp(ack, "method_arg") != 0) {
                            continue;
                        }
                        CBMCallArg *ca = &call.args[call.arg_count];
                        memset(ca, 0, sizeof(*ca));
                        ca->index = call.arg_count;
                        ca->expr = cbm_node_text(ctx->arena, achild, ctx->source);
                        if (ca->expr && ca->expr[0]) {
                            call.arg_count++;
                        }
                    }
                } else {
                    extract_call_args(ctx, args, &call);
                }
            }

            cbm_calls_push(&ctx->result->calls, ctx->arena, call);
            invocation = describe_emitted_primary_call(node, &callee);

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
                            xcall.site_start_byte = call.site_start_byte;
                            xcall.site_end_byte = call.site_end_byte;
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

    if (ctx->language == CBM_LANG_KOTLIN) {
        extract_kotlin_operator_call(ctx, node, ts_node_type(node), state->enclosing_func_qn);
        extract_kotlin_desugared_calls(ctx, node, ts_node_type(node), state->enclosing_func_qn);
    }

    if (ctx->language == CBM_LANG_CPP || ctx->language == CBM_LANG_CUDA) {
        extract_cpp_operator_call(ctx, node, ts_node_type(node), state->enclosing_func_qn);
        extract_cpp_implicit_calls(ctx, node, ts_node_type(node), state->enclosing_func_qn);
    }
    return invocation;
}
