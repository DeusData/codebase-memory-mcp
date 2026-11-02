#include "proof_bridge.h"
#include "ts_proof.h"
#include "../helpers.h"
#include "../resolve/binding.h"
#include "../resolve/policy.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { TS_PROOF_DEPTH = 512, TS_PROOF_LOOKUP = 16384 };

typedef struct {
    const char *name;
    TSNode declaration;
    uint32_t start, end;
    bool write;   /* A write no scope answers for: the whole file abstains on this name. */
    bool pending; /* A rebinding at `start`, until settle_rebindings pins its declaration. */
    bool written; /* A declaration some rebinding resolves to. */
    /* What its initializer holds: settled at most once, and a cycle answers nothing. */
    bool value_settled, value_visiting;
    const CBMRegisteredType *value_type;
} ts_proof_binding_t;

/* Something that can narrow a value below its declared class inside one function, where the
 * checker then selects a member of the narrower class: `x instanceof Y`, or a call passing `x` to
 * a function that may turn out to declare `x is Y`. */
typedef struct {
    const char *path;
    uint32_t start, end;
    TSNode guard; /* The callee; null for instanceof. */
} ts_proof_narrowing_t;

struct CBMTSProofAudit {
    ts_proof_binding_t *bindings;
    size_t count, capacity;
    bool complete;
    bool inert_gaps;
    ts_proof_narrowing_t *narrowings;
    size_t narrowing_count, narrowing_capacity;
    bool guards_settled;
    uint32_t function_start, function_end; /* The function the walk is in. */
};

static bool kind(TSNode node, const char *name) {
    return !ts_node_is_null(node) && strcmp(ts_node_type(node), name) == 0;
}

static TSNode field(TSNode node, const char *name) {
    if (ts_node_is_null(node))
        return (TSNode){0};
    return ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}

static const char *text(TSLSPContext *ctx, TSNode node) {
    return ts_node_is_null(node) ? NULL : cbm_node_text(ctx->arena, node, ctx->source);
}

static bool function(TSNode node) {
    return kind(node, "function_declaration") || kind(node, "function_expression") ||
           kind(node, "arrow_function") || kind(node, "method_definition") ||
           kind(node, "generator_function") || kind(node, "generator_function_declaration");
}

static TSNode binding_scope(TSNode node, bool function_scope) {
    TSNode parent = ts_node_parent(node);
    while (!ts_node_is_null(parent)) {
        if (function(parent) || kind(parent, "program") ||
            (!function_scope && (kind(parent, "statement_block") || kind(parent, "for_statement") ||
                                 kind(parent, "for_in_statement"))))
            return parent;
        parent = ts_node_parent(parent);
    }
    return node;
}

static void add_binding(TSLSPContext *ctx, TSNode declaration, TSNode name, TSNode scope,
                        bool write) {
    struct CBMTSProofAudit *audit = ctx->proof_audit;
    if (!kind(name, "identifier") && !kind(name, "type_identifier") &&
        !kind(name, "shorthand_property_identifier_pattern") &&
        !kind(name, "property_identifier") && !kind(name, "string_fragment")) {
        audit->complete = false;
        return;
    }
    if (audit->count == audit->capacity) {
        size_t capacity = audit->capacity ? audit->capacity * 2 : 32;
        ts_proof_binding_t *items = cbm_arena_alloc(ctx->arena, capacity * sizeof(*items));
        if (!items) {
            audit->complete = false;
            return;
        }
        if (audit->count)
            memcpy(items, audit->bindings, audit->count * sizeof(*items));
        audit->bindings = items;
        audit->capacity = capacity;
    }
    const char *name_text = text(ctx, name);
    if (!name_text) {
        audit->complete = false;
        return;
    }
    audit->bindings[audit->count++] = (ts_proof_binding_t){
        .name = name_text,
        .declaration = declaration,
        .start = write ? 0 : ts_node_start_byte(scope),
        .end = write ? (uint32_t)ctx->source_len : ts_node_end_byte(scope),
        .write = write,
    };
}

/* The scope a destructured name binds in, from the construct that declares it. */
static TSNode pattern_scope(TSNode node) {
    TSNode at = ts_node_parent(node);
    while (!ts_node_is_null(at)) {
        if (kind(at, "variable_declarator"))
            return binding_scope(at, kind(ts_node_parent(at), "variable_declaration"));
        if (kind(at, "catch_clause"))
            return at;
        if (kind(at, "required_parameter") || kind(at, "optional_parameter") ||
            kind(at, "formal_parameters"))
            return binding_scope(at, true);
        if (kind(at, "for_in_statement"))
            return at;
        if (!kind(at, "object_pattern") && !kind(at, "array_pattern") &&
            !kind(at, "rest_pattern") && !kind(at, "pair_pattern") &&
            !kind(at, "assignment_pattern") && !kind(at, "object_assignment_pattern"))
            break;
        at = ts_node_parent(at);
    }
    return binding_scope(node, false);
}

/* A name a destructuring pattern binds, with no type of its own. Default values
 * and computed keys read; only these positions bind. */
static bool pattern_binder(TSNode node) {
    if (kind(node, "shorthand_property_identifier_pattern"))
        return true;
    if (!kind(node, "identifier"))
        return false;
    TSNode parent = ts_node_parent(node);
    if (kind(parent, "array_pattern") || kind(parent, "rest_pattern"))
        return true;
    if (kind(parent, "pair_pattern"))
        return ts_node_eq(node, field(parent, "value"));
    if (kind(parent, "assignment_pattern") || kind(parent, "object_assignment_pattern"))
        return ts_node_eq(node, field(parent, "left"));
    return false;
}

static size_t audit_binding_count(const TSLSPContext *ctx) {
    return ctx->proof_audit->count;
}

static void add_rebinding(TSLSPContext *ctx, TSNode target) {
    size_t before = audit_binding_count(ctx);
    add_binding(ctx, target, target, target, false);
    if (audit_binding_count(ctx) > before)
        ctx->proof_audit->bindings[before].pending = true;
}

/* A write's target. Rebinding a name changes the one declaration lexical scope picks for
 * it, wherever a closure or branch runs it, and no same-named declaration elsewhere: see
 * settle_rebindings. Writing a property rebinds no name; in JavaScript it can still
 * replace a method, so that one method abstains. */
static void audit_write_target(TSLSPContext *ctx, TSNode write, TSNode target, unsigned depth) {
    struct CBMTSProofAudit *audit = ctx->proof_audit;
    if (ts_node_is_null(target) || depth > TS_PROOF_DEPTH) {
        audit->complete = false;
    } else if (kind(target, "identifier") ||
               kind(target, "shorthand_property_identifier_pattern")) {
        add_rebinding(ctx, target);
    } else if (kind(target, "member_expression")) {
        TSNode property = field(target, "property");
        if (ctx->js_mode && kind(property, "property_identifier"))
            add_binding(ctx, write, property, write, true);
    } else if (kind(target, "subscript_expression")) {
        /* `this['m'] = v` writes that method as plainly as `this.m = v`. A key computed at
         * runtime declares nothing: which method it hits is dispatch, not static selection. */
        TSNode key = field(target, "index");
        const char *literal = NULL;
        if (ctx->js_mode && kind(key, "string") && ts_node_named_child_count(key) == 1 &&
            kind(ts_node_named_child(key, 0), "string_fragment"))
            literal = text(ctx, ts_node_named_child(key, 0));
        if (literal)
            add_binding(ctx, write, ts_node_named_child(key, 0), write, true);
        else if (ctx->js_mode && kind(key, "string"))
            audit->complete = false;
    } else if (kind(target, "array_pattern") || kind(target, "object_pattern") ||
               kind(target, "parenthesized_expression") || kind(target, "non_null_expression") ||
               kind(target, "rest_pattern")) {
        uint32_t count = ts_node_named_child_count(target);
        for (uint32_t i = 0; i < count; i++)
            audit_write_target(ctx, write, ts_node_named_child(target, i), depth + 1);
    } else if (kind(target, "as_expression") || kind(target, "satisfies_expression")) {
        /* `;(resolved.build as Options) = v` writes the expression; the cast changes no target. */
        audit_write_target(ctx, write, ts_node_named_child(target, 0), depth + 1);
    } else if (kind(target, "pair_pattern")) {
        audit_write_target(ctx, write, field(target, "value"), depth + 1);
    } else if (kind(target, "assignment_pattern") || kind(target, "object_assignment_pattern")) {
        audit_write_target(ctx, write, field(target, "left"), depth + 1);
    } else if (!kind(target, "comment")) {
        audit->complete = false;
    }
}

/* `for (x of xs)` writes x; `for (const x of xs)` declares it. */
static bool for_declares(TSNode node) {
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (kind(child, "var") || kind(child, "let") || kind(child, "const"))
            return true;
    }
    return false;
}

/* `a.b.c` as spelled, past `!` and parentheses. NULL for anything a narrowing cannot name. */
static const char *narrowing_path(TSLSPContext *ctx, TSNode node, unsigned depth) {
    enum { TS_NARROWING_PATH_DEPTH = 64 };
    while (kind(node, "non_null_expression") || kind(node, "parenthesized_expression"))
        node = ts_node_named_child(node, 0);
    if (kind(node, "identifier") || kind(node, "this"))
        return text(ctx, node);
    if (!kind(node, "member_expression") || depth > TS_NARROWING_PATH_DEPTH)
        return NULL;
    TSNode property = field(node, "property");
    const char *base = kind(property, "property_identifier")
                           ? narrowing_path(ctx, field(node, "object"), depth + 1)
                           : NULL;
    return base ? cbm_arena_sprintf(ctx->arena, "%s.%s", base, text(ctx, property)) : NULL;
}

static void note_narrowing(TSLSPContext *ctx, TSNode value, TSNode guard) {
    struct CBMTSProofAudit *audit = ctx->proof_audit;
    const char *path = narrowing_path(ctx, value, 0);
    if (!path)
        return;
    if (audit->narrowing_count == audit->narrowing_capacity) {
        size_t capacity = audit->narrowing_capacity ? audit->narrowing_capacity * 2 : 16;
        ts_proof_narrowing_t *grown = cbm_arena_alloc(ctx->arena, capacity * sizeof(*grown));
        if (!grown) {
            audit->complete = false;
            return;
        }
        if (audit->narrowing_count)
            memcpy(grown, audit->narrowings, audit->narrowing_count * sizeof(*grown));
        audit->narrowings = grown;
        audit->narrowing_capacity = capacity;
    }
    audit->narrowings[audit->narrowing_count++] = (ts_proof_narrowing_t){
        .path = path, .start = audit->function_start, .end = audit->function_end, .guard = guard};
}

static void audit_node(TSLSPContext *ctx, TSNode node) {
    struct CBMTSProofAudit *audit = ctx->proof_audit;
    if (kind(node, "ERROR") && audit->inert_gaps) {
        return;
    } else if (ts_node_is_missing(node) || kind(node, "ERROR") || kind(node, "with_statement")) {
        audit->complete = false;
    } else if (pattern_binder(node)) {
        add_binding(ctx, node, node, pattern_scope(node), false);
    } else if (kind(node, "catch_clause")) {
        TSNode parameter = field(node, "parameter");
        if (kind(parameter, "identifier"))
            add_binding(ctx, parameter, parameter, node, false);
    } else if (kind(node, "type_parameter")) {
        add_binding(ctx, node, field(node, "name"), binding_scope(node, false), false);
    } else if (kind(node, "variable_declarator")) {
        TSNode name = field(node, "name");
        bool hoisted = kind(ts_node_parent(node), "variable_declaration");
        if (kind(name, "identifier"))
            add_binding(ctx, node, name, binding_scope(node, hoisted), false);
    } else if (kind(node, "required_parameter") || kind(node, "optional_parameter")) {
        TSNode name = field(node, "pattern");
        if (ts_node_is_null(name))
            name = field(node, "name");
        if (kind(name, "identifier"))
            add_binding(ctx, node, name, binding_scope(node, true), false);
    } else if (kind(node, "identifier") && kind(ts_node_parent(node), "formal_parameters")) {
        add_binding(ctx, node, node, binding_scope(node, true), false);
    } else if (kind(node, "class_declaration") || kind(node, "abstract_class_declaration") ||
               kind(node, "interface_declaration") || kind(node, "function_declaration") ||
               kind(node, "type_alias_declaration") || kind(node, "enum_declaration")) {
        add_binding(ctx, node, field(node, "name"), binding_scope(node, false), false);
    } else if (kind(node, "function_expression") || kind(node, "generator_function")) {
        TSNode name = field(node, "name");
        if (!ts_node_is_null(name))
            add_binding(ctx, node, name, node, false);
    } else if (kind(node, "arrow_function")) {
        TSNode parameter = field(node, "parameter");
        if (!ts_node_is_null(parameter))
            add_binding(ctx, parameter, parameter, node, false);
    } else if (kind(node, "assignment_expression") ||
               kind(node, "augmented_assignment_expression")) {
        audit_write_target(ctx, node, field(node, "left"), 0);
    } else if (kind(node, "update_expression")) {
        audit_write_target(ctx, node, field(node, "argument"), 0);
    } else if (kind(node, "for_in_statement")) {
        TSNode left = field(node, "left");
        if (!for_declares(node))
            audit_write_target(ctx, node, left, 0);
        else if (kind(left, "identifier"))
            add_binding(ctx, node, left, node, false);
    } else if (kind(node, "binary_expression")) {
        const char *spelled = text(ctx, field(node, "operator"));
        if (spelled && !strcmp(spelled, "instanceof"))
            note_narrowing(ctx, field(node, "left"), (TSNode){0});
    } else if (kind(node, "call_expression")) {
        TSNode callee = field(node, "function");
        if (kind(callee, "identifier")) {
            const char *name = text(ctx, callee);
            /* `Function(code)` compiles in the global scope, which sees no binding here. */
            if (!name || strcmp(name, "eval") == 0)
                audit->complete = false;
        }
        TSNode arguments = field(node, "arguments");
        if (ts_node_named_child_count(arguments) &&
            (kind(callee, "identifier") ||
             (kind(callee, "member_expression") && kind(field(callee, "object"), "this"))))
            note_narrowing(ctx, ts_node_named_child(arguments, 0), callee);
    }
}

static int narrowing_compare(const void *left, const void *right) {
    const ts_proof_narrowing_t *a = left, *b = right;
    int order = strcmp(a->path, b->path);
    if (order)
        return order;
    return a->start == b->start ? 0 : (a->start < b->start ? -1 : 1);
}

static int binding_compare(const void *left, const void *right) {
    const ts_proof_binding_t *a = left, *b = right;
    int order = strcmp(a->name, b->name);
    if (order)
        return order;
    if (a->start != b->start)
        return a->start < b->start ? -1 : 1;
    /* The wider scope first, so a sweep opens an outer declaration before an inner one. */
    return a->end == b->end ? 0 : (a->end > b->end ? -1 : 1);
}

/* A type-only name cannot be what a value assignment rebinds. */
static bool value_declaration(const ts_proof_binding_t *entry) {
    return !kind(entry->declaration, "type_parameter") &&
           !kind(entry->declaration, "interface_declaration") &&
           !kind(entry->declaration, "type_alias_declaration");
}

/* Pin each rebinding on the declaration it resolves to, then drop it from the table. Scopes
 * are AST nodes, so they nest: one sweep over each name's sorted entries with a stack of the
 * scopes still open. Every entry is pushed and popped at most once, so this is linear after
 * the sort. A name nothing here declares is a global, and stays a whole-file write. */
static void settle_rebindings(TSLSPContext *ctx) {
    struct CBMTSProofAudit *audit = ctx->proof_audit;
    if (!audit->count)
        return;
    ts_proof_binding_t **open = cbm_arena_alloc(ctx->arena, audit->count * sizeof(*open));
    if (!open) {
        audit->complete = false;
        return;
    }
    size_t depth = 0;
    const char *name = NULL;
    for (size_t i = 0; i < audit->count; i++) {
        ts_proof_binding_t *entry = &audit->bindings[i];
        if (!name || strcmp(name, entry->name)) {
            name = entry->name;
            depth = 0;
        }
        while (depth && open[depth - 1]->end <= entry->start)
            depth--;
        if (!entry->pending) {
            if (!entry->write && value_declaration(entry))
                open[depth++] = entry;
        } else if (depth) {
            open[depth - 1]->written = true;
        } else {
            entry->pending = false;
            entry->write = true;
        }
    }
    size_t kept = 0;
    for (size_t i = 0; i < audit->count; i++) {
        if (!audit->bindings[i].pending)
            audit->bindings[kept++] = audit->bindings[i];
    }
    audit->count = kept;
}

void cbm_ts_proof_prepare(TSLSPContext *ctx, TSNode root) {
    if (!ctx || !ctx->arena || ts_node_is_null(root))
        return;
    cbm_resolve_proof_source_t source_context = ctx->proven_types.proof_source;
    /* One fact per receiver occurrence of this file. A budget here dropped the
     * tail of a large class: `this.postMessage` proved until byte 212,000 of a
     * 247,000-byte file and not after, for being late rather than unproved. */
    cbm_resolve_types_init(&ctx->proven_types, ctx->arena, UINT64_MAX);
    cbm_resolve_proof_pool_init_project(&ctx->proven_types.proofs, ctx->arena);
    (void)cbm_lsp_proof_context(&ctx->proven_types, &source_context, ctx->module_qn);
    ctx->proof_audit = cbm_arena_calloc(ctx->arena, sizeof(*ctx->proof_audit));
    if (!ctx->proof_audit)
        return;
    ctx->proof_audit->inert_gaps = cbm_resolve_binding_js_parse_gaps_inert(root, ctx->source);
    ctx->proof_audit->complete = !ts_node_has_error(root) || ctx->proof_audit->inert_gaps;
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    uint32_t depth = 0;
    bool walking = true;
    /* The open functions, as byte spans: they nest, so the top is the one a node is in. */
    struct {
        uint32_t start, end;
    } open_functions[TS_PROOF_DEPTH + 2];
    int open_count = 0;
    while (walking && ctx->proof_audit->complete) {
        /* Linear in the file; size alone must not withdraw its proofs. */
        if (depth > TS_PROOF_DEPTH) {
            ctx->proof_audit->complete = false;
            break;
        }
        TSNode current = ts_tree_cursor_current_node(&cursor);
        uint32_t at = ts_node_start_byte(current);
        while (open_count && open_functions[open_count - 1].end <= at)
            open_count--;
        if (function(current) && open_count <= TS_PROOF_DEPTH) {
            open_functions[open_count].start = at;
            open_functions[open_count].end = ts_node_end_byte(current);
            open_count++;
        }
        ctx->proof_audit->function_start = open_count ? open_functions[open_count - 1].start : 0;
        ctx->proof_audit->function_end =
            open_count ? open_functions[open_count - 1].end : UINT32_MAX;
        audit_node(ctx, current);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            depth++;
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (!ts_tree_cursor_goto_parent(&cursor)) {
                walking = false;
                break;
            }
            depth--;
        }
    }
    ts_tree_cursor_delete(&cursor);
    if (ctx->proof_audit->count > 1)
        qsort(ctx->proof_audit->bindings, ctx->proof_audit->count,
              sizeof(*ctx->proof_audit->bindings), binding_compare);
    settle_rebindings(ctx);
}

static const ts_proof_binding_t *scope_binding(TSLSPContext *ctx, const char *name, uint32_t at,
                                               bool *occupied) {
    if (occupied)
        *occupied = false;
    struct CBMTSProofAudit *audit = ctx->proof_audit;
    size_t low = 0, high = audit->count;
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        if (strcmp(audit->bindings[mid].name, name) < 0)
            low = mid + 1;
        else
            high = mid;
    }
    const ts_proof_binding_t *selected = NULL;
    bool duplicate = false;
    uint32_t work = 0;
    for (; low < audit->count && strcmp(audit->bindings[low].name, name) == 0; low++) {
        if (++work > TS_PROOF_LOOKUP) {
            if (occupied)
                *occupied = true;
            return NULL;
        }
        const ts_proof_binding_t *entry = &audit->bindings[low];
        if (entry->write) {
            if (occupied)
                *occupied = true;
            return NULL;
        }
        if (at < entry->start || at >= entry->end)
            continue;
        if (occupied)
            *occupied = true;
        if (!selected || entry->end - entry->start < selected->end - selected->start) {
            selected = entry;
            duplicate = false;
        } else if (entry->end - entry->start == selected->end - selected->start) {
            duplicate = true;
        }
    }
    return duplicate ? NULL : selected;
}

static const ts_proof_binding_t *find_binding(TSLSPContext *ctx, const char *name, uint32_t at,
                                              bool *occupied) {
    const ts_proof_binding_t *selected = scope_binding(ctx, name, at, occupied);
    return selected && selected->written ? NULL : selected;
}

static uint64_t qn_hash(const char *name) {
    uint64_t hash = UINT64_C(14695981039346656037);
    for (const unsigned char *p = (const unsigned char *)name; *p; p++) {
        hash ^= *p;
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static const CBMRegisteredType *exact_type(TSLSPContext *ctx, const char *qn) {
    const CBMRegisteredType *selected = NULL;
    uint32_t work = 0, links = 0;
    for (const CBMTypeRegistry *reg = ctx->registry; reg; reg = reg->fallback) {
        if (++links > 64 || reg->type_count != reg->type_qn_entry_count)
            return NULL;
        if (reg->type_count == 0)
            continue;
        if (!reg->type_qn_buckets || reg->type_qn_bucket_count <= 0 || !reg->type_qn_entries)
            return NULL;
        int slot = (int)(qn_hash(qn) & (uint64_t)(reg->type_qn_bucket_count - 1));
        for (int at = reg->type_qn_buckets[slot]; at >= 0;) {
            if (++work > TS_PROOF_LOOKUP || at >= reg->type_qn_entry_count)
                return NULL;
            const CBMRegistryHashEntry *entry = &reg->type_qn_entries[at];
            if (entry->payload_index < 0 || entry->payload_index >= reg->type_count)
                return NULL;
            const CBMRegisteredType *candidate = &reg->types[entry->payload_index];
            if (!candidate->alias_view && candidate->qualified_name &&
                strcmp(candidate->qualified_name, qn) == 0) {
                if (!candidate->project_source)
                    return NULL;
                if (selected && selected != candidate &&
                    (!selected->declaration_identity || !candidate->declaration_identity ||
                     strcmp(selected->declaration_identity, candidate->declaration_identity) != 0))
                    return NULL;
                /* A per-file overlay copies the type; only the shared one has its members. */
                if (!selected || !selected->member_names)
                    selected = candidate;
            }
            at = entry->next_index;
        }
    }
    return selected;
}

static const CBMRegisteredType *named_type(TSLSPContext *ctx, const char *name, uint32_t at) {
    bool occupied = false;
    const ts_proof_binding_t *local = find_binding(ctx, name, at, &occupied);
    if (occupied && !local)
        return NULL;
    if (local) {
        if (!kind(local->declaration, "class_declaration") &&
            !kind(local->declaration, "abstract_class_declaration") &&
            !kind(local->declaration, "interface_declaration") &&
            !kind(local->declaration, "type_alias_declaration"))
            return NULL;
        /* Nested classes need their canonical lexical-owner identity first. */
        TSNode parent = ts_node_parent(local->declaration);
        if (kind(parent, "export_statement"))
            parent = ts_node_parent(parent);
        if (!kind(parent, "program"))
            return NULL;
        const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, name);
        return qn ? exact_type(ctx, qn) : NULL;
    }
    const char *target = NULL;
    for (int i = 0; i < ctx->import_count; i++) {
        const char *key = ctx->import_local_names[i];
        const char *value = ctx->import_module_qns[i];
        if (!key || strcmp(key, name) != 0)
            continue;
        if (!value || !*value || (target && strcmp(target, value) != 0))
            return NULL;
        target = value;
    }
    if (!target)
        return NULL;
    const CBMRegisteredType *direct = exact_type(ctx, target);
    const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", target, name);
    const CBMRegisteredType *member = qn ? exact_type(ctx, qn) : NULL;
    if (direct && member)
        return NULL;
    const CBMRegisteredType *result = direct ? direct : member;
    return result && result->export_binding_proven ? result : NULL;
}

/* `Object.defineProperty(this, key, { get() { this.m() } })`: an accessor runs on the object it
 * is defined on, so the descriptor's methods see the `this` around the call. */
static bool descriptor_of_this(TSLSPContext *ctx, TSNode object) {
    TSNode arguments = ts_node_parent(object);
    TSNode call = ts_node_parent(arguments);
    if (!kind(arguments, "arguments") || !kind(call, "call_expression") ||
        ts_node_named_child_count(arguments) != 3 ||
        !kind(ts_node_named_child(arguments, 0), "this") ||
        !ts_node_eq(ts_node_named_child(arguments, 2), object))
        return false;
    const char *callee = text(ctx, field(call, "function"));
    if (!callee || strcmp(callee, "Object.defineProperty"))
        return false;
    bool occupied = false;
    (void)find_binding(ctx, "Object", ts_node_start_byte(call), &occupied);
    return !occupied;
}

/* The class `this` means here: through methods and arrows, not plain functions. */
static TSNode this_class(TSLSPContext *ctx, TSNode receiver) {
    TSNode owner = ts_node_parent(receiver);
    while (!ts_node_is_null(owner) && !kind(owner, "class_declaration") &&
           !kind(owner, "abstract_class_declaration")) {
        if (function(owner) && !kind(owner, "method_definition") && !kind(owner, "arrow_function"))
            return (TSNode){0};
        /* A method of an object literal or of a class expression has its own `this`. */
        if (kind(owner, "class") ||
            (kind(owner, "method_definition") && kind(ts_node_parent(owner), "object") &&
             !descriptor_of_this(ctx, ts_node_parent(owner))))
            return (TSNode){0};
        owner = ts_node_parent(owner);
    }
    return owner;
}

static const char *type_class_name(TSLSPContext *ctx, TSNode type) {
    TSNode name = cbm_ts_single_type_name(type);
    return ts_node_is_null(name) ? NULL : text(ctx, name);
}

/* A plain annotation's named type, or NULL. */
static const char *annotated_type_name(TSLSPContext *ctx, TSNode declaration) {
    return type_class_name(ctx, field(declaration, "type"));
}

/* The base class `super` starts from: the one class this class extends. An interface has no
 * body to select. A heritage the index cannot see could be the class, so it has no answer. */
static const CBMRegisteredType *super_type(TSLSPContext *ctx, TSNode receiver, uint32_t at) {
    const char *name = text(ctx, field(this_class(ctx, receiver), "name"));
    const CBMRegisteredType *type = name ? named_type(ctx, name, at) : NULL;
    if (!type || !type->embedded_types)
        return NULL;
    const CBMRegisteredType *found = NULL;
    for (int i = 0; type->embedded_types[i]; i++) {
        const CBMRegisteredType *base = exact_type(ctx, type->embedded_types[i]);
        if (!base || (found && !base->is_interface))
            return NULL;
        if (!base->is_interface)
            found = base;
    }
    return found;
}

/* `this.store`: its one declaration in the class body, a field or a constructor
 * parameter property. TypeScript keeps every write to it within the declared type.
 * A field the class only inherits, or declares twice, has no answer here. */
static const char *declared_field_type(TSLSPContext *ctx, TSNode owner, const char *name) {
    TSNode body = field(owner, "body");
    const char *found = NULL;
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        if (kind(member, "public_field_definition")) {
            const char *field_name = text(ctx, field(member, "name"));
            if (!field_name)
                field_name = text(ctx, field(member, "property"));
            if (!field_name || strcmp(field_name, name))
                continue;
            if (found)
                return NULL;
            found = annotated_type_name(ctx, member);
            /* An annotation wins even when it names no class: `x: unknown = new A()`. */
            if (!found && ts_node_is_null(field(member, "type"))) {
                TSNode value = field(member, "value");
                TSNode constructor = field(value, "constructor");
                if (kind(value, "new_expression") && kind(constructor, "identifier"))
                    found = text(ctx, constructor);
            }
            if (!found)
                return NULL;
        } else if (kind(member, "method_definition")) {
            const char *method = text(ctx, field(member, "name"));
            if (!method || strcmp(method, "constructor"))
                continue;
            TSNode parameters = field(member, "parameters");
            uint32_t params =
                ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
            for (uint32_t p = 0; p < params; p++) {
                TSNode parameter = ts_node_named_child(parameters, p);
                if (!kind(parameter, "required_parameter") &&
                    !kind(parameter, "optional_parameter"))
                    continue;
                bool property = false;
                uint32_t parts = ts_node_named_child_count(parameter);
                for (uint32_t c = 0; c < parts; c++) {
                    TSNode part = ts_node_named_child(parameter, c);
                    property |=
                        kind(part, "accessibility_modifier") || kind(part, "override_modifier");
                }
                for (uint32_t c = 0; c < ts_node_child_count(parameter); c++)
                    property |= kind(ts_node_child(parameter, c), "readonly");
                const char *parameter_name = text(ctx, field(parameter, "pattern"));
                if (!property || !parameter_name || strcmp(parameter_name, name))
                    continue;
                if (found)
                    return NULL;
                found = annotated_type_name(ctx, parameter);
                if (!found)
                    return NULL;
            }
        }
    }
    return found;
}

/* method_names exists only for this file's own classes, swept from its AST. A class
 * of another file answers by the methods registered with that exact receiver. */
static bool declares_method(TSLSPContext *ctx, const CBMRegisteredType *type, const char *name) {
    for (int i = 0; type->method_names && type->method_names[i]; i++)
        if (!strcmp(type->method_names[i], name))
            return true;
    return cbm_registry_lookup_method(ctx->registry, type->qualified_name, name) != NULL;
}

/* A class's `<extends>` row (ts_push_class_base), which is apart from what it implements: an
 * interface it implements declares nothing for it, known to the index or not. True when the row
 * settles the class's base. *base is then the class it extends, or NULL for none or unknown. */
static bool class_base(TSLSPContext *ctx, const CBMRegisteredType *type,
                       const CBMRegisteredType **base) {
    *base = NULL;
    const char *row = type->member_names ? type->member_names[0] : NULL;
    if (!row || strcmp(row, "<extends>"))
        return false;
    const char *qn = type->member_class_qns[0];
    if (strcmp(qn, type->qualified_name))
        *base = exact_type(ctx, qn);
    return true;
}

/* The nearest class up the extends chain that declares this method. A base the
 * index cannot see may declare it first, so an unknown base ends the search. */
static const CBMRegisteredType *inherited_owner(TSLSPContext *ctx, const CBMRegisteredType *type,
                                                const char *name, unsigned depth) {
    enum { TS_PROOF_INHERITANCE_DEPTH = 32 };
    if (depth > TS_PROOF_INHERITANCE_DEPTH)
        return NULL;
    const CBMRegisteredType *parent = NULL;
    if (class_base(ctx, type, &parent)) {
        if (!parent)
            return NULL;
        return declares_method(ctx, parent, name) ? parent
                                                  : inherited_owner(ctx, parent, name, depth + 1);
    }
    if (!type->embedded_types)
        return NULL;
    const CBMRegisteredType *found = NULL;
    for (int i = 0; type->embedded_types[i]; i++) {
        const CBMRegisteredType *base = exact_type(ctx, type->embedded_types[i]);
        if (!base)
            return NULL;
        if (base->is_interface)
            continue;
        const CBMRegisteredType *owner =
            declares_method(ctx, base, name) ? base : inherited_owner(ctx, base, name, depth + 1);
        if (!owner || (found && found != owner))
            return NULL;
        found = owner;
    }
    return found;
}

/* A type alias, read from the rows extraction wrote for it (extract_ts_alias_shape). A member
 * call on `A | B` type-checks only if both have the member. When both reach one declaration, that
 * is what the checker selected; when they reach two, no one declaration was, so it abstains. An
 * intersection has it from whichever constituent declares it. Rows are in source order and each
 * is read once, so one lookup costs the alias's own length. */
typedef enum { TS_MEMBER_ABSENT, TS_MEMBER_UNKNOWN, TS_MEMBER_FOUND } ts_member_state_t;

enum {
    TS_ALIAS_PATH = 128,
    /* Distinct aliases one lookup may pass through. It stops a hostile file, and no real one. */
    TS_ALIAS_SEEN = 64,
};

typedef struct {
    TSLSPContext *ctx;
    const char *name;
    size_t name_length; /* without a `[]` or `{}` ending */
    bool method;        /* the class declaring a method, or the class a member holds */
    struct {
        const CBMRegisteredType *alias;
        ts_member_state_t state;
        const CBMRegisteredType *found;
    } seen[TS_ALIAS_SEEN];
    int seen_count;
} ts_alias_query_t;

static const CBMRegisteredType *member_type(TSLSPContext *ctx, const CBMRegisteredType *type,
                                            const char *name, unsigned depth);

static bool type_is_alias(const CBMRegisteredType *type) {
    const char *first = type->member_names ? type->member_names[0] : NULL;
    return first && (first[0] == '~' || first[0] == '^');
}

static ts_member_state_t type_member_state(ts_alias_query_t *query, const CBMRegisteredType *type,
                                           const CBMRegisteredType **found);

static ts_member_state_t alias_level(ts_alias_query_t *query, const CBMRegisteredType *alias,
                                     int *at, char *path, size_t length, char op,
                                     const CBMRegisteredType **found);

static bool alias_key_is(const ts_alias_query_t *query, const char *key) {
    return strlen(key) == query->name_length && !strncmp(key, query->name, query->name_length);
}

/* One constituent, whose rows start at *at. Leaves *at past them on anything but an unknown,
 * which ends the whole lookup. */
static ts_member_state_t alias_constituent(ts_alias_query_t *query, const CBMRegisteredType *alias,
                                           int *at, char *path, size_t length,
                                           const CBMRegisteredType **found) {
    const char **rows = alias->member_names;
    bool restricted = false, picked = false, omitted = false;
    ts_member_state_t state = TS_MEMBER_UNKNOWN; /* No row: the class it names is unknown. */
    *found = NULL;
    for (; rows[*at]; (*at)++) {
        const char *row = rows[*at];
        if (strncmp(row, path, length) || (row[length] >= '0' && row[length] <= '9'))
            break;
        const char *rest = row + length;
        if (*rest == '+') {
            restricted = true;
            picked = picked || alias_key_is(query, rest + 1);
            continue;
        }
        if (*rest == '-') {
            omitted = omitted || alias_key_is(query, rest + 1);
            continue;
        }
        if (omitted || (restricted && !picked))
            continue; /* Nothing below can bring the member back. */
        if (*rest == '?') {
            return TS_MEMBER_UNKNOWN;
        } else if (*rest == '%') {
            state = TS_MEMBER_ABSENT;
        } else if (*rest == '=') {
            /* An object type declares it, and no graph node stands for that declaration. */
            if (alias_key_is(query, rest + 1))
                return TS_MEMBER_UNKNOWN;
        } else if (*rest == '~' || *rest == '^') {
            state = alias_level(query, alias, at, path, length, *rest, found);
            if (state == TS_MEMBER_UNKNOWN)
                return state;
            (*at)--; /* alias_level left *at on the next row; the loop steps. */
        } else if (*rest == '\0') {
            const CBMRegisteredType *type = exact_type(query->ctx, alias->member_class_qns[*at]);
            if (!type)
                return TS_MEMBER_UNKNOWN;
            state = type_member_state(query, type, found);
            if (state == TS_MEMBER_UNKNOWN)
                return state;
        }
    }
    if (omitted || (restricted && !picked)) {
        *found = NULL;
        return TS_MEMBER_ABSENT;
    }
    return state;
}

static ts_member_state_t alias_level(ts_alias_query_t *query, const CBMRegisteredType *alias,
                                     int *at, char *path, size_t length, char op,
                                     const CBMRegisteredType **found) {
    const char *row = alias->member_names[*at];
    if (row[length] != op || row[length + 1] != '#')
        return TS_MEMBER_UNKNOWN;
    int count = atoi(row + length + 2);
    (*at)++;
    const CBMRegisteredType *agreed = NULL;
    for (int i = 0; i < count; i++) {
        int wrote = snprintf(path + length, TS_ALIAS_PATH - length, "%c%d", op, i);
        if (wrote <= 0 || (size_t)wrote >= TS_ALIAS_PATH - length)
            return TS_MEMBER_UNKNOWN;
        const CBMRegisteredType *answer = NULL;
        ts_member_state_t state =
            alias_constituent(query, alias, at, path, length + (size_t)wrote, &answer);
        path[length] = '\0';
        if (state == TS_MEMBER_UNKNOWN || (op == '~' && state == TS_MEMBER_ABSENT))
            return TS_MEMBER_UNKNOWN;
        if (state == TS_MEMBER_FOUND) {
            if (agreed && agreed != answer)
                return TS_MEMBER_UNKNOWN;
            agreed = answer;
        }
    }
    *found = agreed;
    return agreed ? TS_MEMBER_FOUND : op == '^' ? TS_MEMBER_ABSENT : TS_MEMBER_UNKNOWN;
}

static ts_member_state_t type_member_state(ts_alias_query_t *query, const CBMRegisteredType *type,
                                           const CBMRegisteredType **found) {
    *found = NULL;
    if (type_is_alias(type)) {
        for (int i = 0; i < query->seen_count; i++) {
            if (query->seen[i].alias == type) {
                *found = query->seen[i].found;
                return query->seen[i].state;
            }
        }
        if (query->seen_count >= TS_ALIAS_SEEN)
            return TS_MEMBER_UNKNOWN;
        int slot = query->seen_count++;
        /* Unknown while walking: an alias of itself has no answer. */
        query->seen[slot].alias = type;
        query->seen[slot].state = TS_MEMBER_UNKNOWN;
        query->seen[slot].found = NULL;
        char path[TS_ALIAS_PATH];
        path[0] = '\0';
        int at = 0;
        ts_member_state_t state =
            alias_level(query, type, &at, path, 0, type->member_names[0][0], found);
        query->seen[slot].state = state;
        query->seen[slot].found = *found;
        return state;
    }
    TSLSPContext *ctx = query->ctx;
    if (query->method) {
        *found = declares_method(ctx, type, query->name)
                     ? type
                     : inherited_owner(ctx, type, query->name, 0);
    } else {
        *found = member_type(ctx, type, query->name, 0);
    }
    if (*found)
        return TS_MEMBER_FOUND;
    /* A base the index cannot see may hold it, and a field is not a method. */
    const CBMRegisteredType *parent = NULL;
    if (class_base(ctx, type, &parent))
        return parent || strcmp(type->member_class_qns[0], type->qualified_name) ? TS_MEMBER_UNKNOWN
                                                                                 : TS_MEMBER_ABSENT;
    return type->embedded_types ? TS_MEMBER_UNKNOWN : TS_MEMBER_ABSENT;
}

static const CBMRegisteredType *alias_member(TSLSPContext *ctx, const CBMRegisteredType *alias,
                                             const char *name, bool method) {
    size_t length = strlen(name);
    if (length > 2 && (!strcmp(name + length - 2, "[]") || !strcmp(name + length - 2, "{}")))
        length -= 2;
    ts_alias_query_t query = {.ctx = ctx, .name = name, .name_length = length, .method = method};
    const CBMRegisteredType *found = NULL;
    return type_member_state(&query, alias, &found) == TS_MEMBER_FOUND ? found : NULL;
}

/* The class a member of this type holds: its own declaration, else the nearest up the extends
 * chain. A base the index cannot see may declare it first, so that ends the search. */
static const CBMRegisteredType *member_type(TSLSPContext *ctx, const CBMRegisteredType *type,
                                            const char *name, unsigned depth) {
    enum { TS_PROOF_INHERITANCE_DEPTH = 32 };
    if (depth > TS_PROOF_INHERITANCE_DEPTH)
        return NULL;
    if (type_is_alias(type))
        return alias_member(ctx, type, name, false);
    const char *declared = NULL;
    for (int i = 0; type->member_names && type->member_names[i]; i++) {
        if (strcmp(type->member_names[i], name))
            continue;
        if (declared)
            return NULL; /* Declared twice: no one answer. */
        declared = type->member_class_qns[i];
    }
    if (declared)
        return exact_type(ctx, declared);
    if (declares_method(ctx, type, name))
        return NULL;
    const CBMRegisteredType *parent = NULL;
    if (class_base(ctx, type, &parent)) {
        return parent && !type_is_alias(parent) ? member_type(ctx, parent, name, depth + 1) : NULL;
    }
    if (!type->embedded_types)
        return NULL;
    const CBMRegisteredType *found = NULL;
    for (int i = 0; type->embedded_types[i]; i++) {
        const CBMRegisteredType *base = exact_type(ctx, type->embedded_types[i]);
        /* `class A extends X` over `type X = A` is rejected by the checker and has no answer. */
        if (!base || type_is_alias(base))
            return NULL;
        const CBMRegisteredType *answer = member_type(ctx, base, name, depth + 1);
        if (found && answer && found != answer)
            return NULL;
        if (answer)
            found = answer;
    }
    return found;
}

static const CBMRegisteredType *receiver_type(TSLSPContext *ctx, TSNode receiver, uint32_t at);
static const CBMRegisteredFunc *callee_function(TSLSPContext *ctx, TSNode callee, uint32_t at);

/* Keep only the calls whose callee really is a guard. Once per file, at the first receiver, since
 * the registry is what knows. A callee is a name or `this.m`, so the only receiver typed on the
 * way is `this`, and the table reads as empty until it is built. */
static void settle_guards(TSLSPContext *ctx) {
    struct CBMTSProofAudit *audit = ctx->proof_audit;
    audit->guards_settled = true;
    size_t total = audit->narrowing_count, kept = 0;
    audit->narrowing_count = 0;
    for (size_t i = 0; i < total; i++) {
        ts_proof_narrowing_t entry = audit->narrowings[i];
        if (!ts_node_is_null(entry.guard)) {
            const CBMRegisteredFunc *callee =
                callee_function(ctx, entry.guard, ts_node_start_byte(entry.guard));
            if (!callee || !callee->type_guard)
                continue;
        }
        audit->narrowings[kept++] = entry;
    }
    audit->narrowing_count = kept;
    if (kept > 1)
        qsort(audit->narrowings, kept, sizeof(*audit->narrowings), narrowing_compare);
}

/* The checker selects on what the value was narrowed to, and this proof reads declarations, so
 * it has no answer in the function where that can happen. A value only reaches a narrower class
 * through a named path, and the table holds few entries once the guards are settled. */
static bool receiver_narrowed(TSLSPContext *ctx, TSNode receiver, uint32_t at) {
    struct CBMTSProofAudit *audit = ctx->proof_audit;
    if (!audit->narrowing_count)
        return false;
    if (!audit->guards_settled)
        settle_guards(ctx);
    if (!audit->narrowing_count)
        return false;
    const char *path = narrowing_path(ctx, receiver, 0);
    if (!path)
        return false;
    size_t low = 0, high = audit->narrowing_count;
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        if (strcmp(audit->narrowings[mid].path, path) < 0)
            low = mid + 1;
        else
            high = mid;
    }
    for (; low < audit->narrowing_count && !strcmp(audit->narrowings[low].path, path); low++)
        if (at >= audit->narrowings[low].start && at < audit->narrowings[low].end)
            return true;
    return false;
}

/* The registered function a call's callee names: a module function, an import, or a method of a
 * receiver this proof can type. NULL for anything else. */
static const CBMRegisteredFunc *callee_function(TSLSPContext *ctx, TSNode callee, uint32_t at) {
    if (kind(callee, "identifier")) {
        const char *name = text(ctx, callee);
        bool occupied = false;
        const ts_proof_binding_t *local = name ? find_binding(ctx, name, at, &occupied) : NULL;
        if (!name || (occupied && !local))
            return NULL;
        const char *qn = NULL;
        if (local) {
            /* `function f() {}` or `const f = () => {}`, at the top of the module. */
            TSNode holder = local->declaration;
            bool callable = kind(holder, "function_declaration");
            if (kind(holder, "variable_declarator") &&
                (kind(field(holder, "value"), "arrow_function") ||
                 kind(field(holder, "value"), "function_expression"))) {
                callable = true;
                holder = ts_node_parent(holder);
            }
            TSNode parent = ts_node_parent(holder);
            if (kind(parent, "export_statement"))
                parent = ts_node_parent(parent);
            if (!callable || !kind(parent, "program"))
                return NULL;
            qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, name);
        } else {
            for (int i = 0; i < ctx->import_count; i++) {
                const char *key = ctx->import_local_names[i];
                const char *value = ctx->import_module_qns[i];
                if (!key || strcmp(key, name))
                    continue;
                if (!value || !*value || (qn && strcmp(qn, value)))
                    return NULL;
                qn = value;
            }
        }
        return qn ? cbm_registry_lookup_func(ctx->registry, qn) : NULL;
    }
    if (!kind(callee, "member_expression"))
        return NULL;
    TSNode property = field(callee, "property");
    const char *name = kind(property, "property_identifier") ? text(ctx, property) : NULL;
    const CBMRegisteredType *holder = name ? receiver_type(ctx, field(callee, "object"), at) : NULL;
    if (!holder)
        return NULL;
    if (type_is_alias(holder))
        holder = alias_member(ctx, holder, name, true);
    else if (!declares_method(ctx, holder, name))
        holder = inherited_owner(ctx, holder, name, 0);
    return holder ? cbm_registry_lookup_method(ctx->registry, holder->qualified_name, name) : NULL;
}

/* `{ a, b: c }` destructured from a value this proof can type: what that member holds. */
static const CBMRegisteredType *pattern_member_type(TSLSPContext *ctx, TSNode name) {
    TSNode pattern = ts_node_parent(name);
    const char *member = NULL;
    if (kind(name, "shorthand_property_identifier_pattern")) {
        member = text(ctx, name);
    } else if (kind(pattern, "pair_pattern") &&
               kind(field(pattern, "key"), "property_identifier")) {
        member = text(ctx, field(pattern, "key"));
        pattern = ts_node_parent(pattern);
    }
    TSNode declarator = kind(pattern, "object_pattern") ? ts_node_parent(pattern) : (TSNode){0};
    TSNode value =
        kind(declarator, "variable_declarator") ? field(declarator, "value") : (TSNode){0};
    if (!member || ts_node_is_null(value))
        return NULL;
    const CBMRegisteredType *holder = receiver_type(ctx, value, ts_node_start_byte(value));
    return holder ? member_type(ctx, holder, member, 0) : NULL;
}

/* The container this annotation spells is the file's own, not the global one. */
static bool annotation_names_local_type(TSLSPContext *ctx, TSNode annotation, uint32_t at) {
    TSNode spelled = cbm_ts_single_type_name(annotation);
    const char *name = ts_node_is_null(spelled) ? NULL : text(ctx, spelled);
    if (!name)
        return false;
    bool occupied = false;
    (void)find_binding(ctx, name, at, &occupied);
    if (occupied)
        return true;
    for (int i = 0; i < ctx->import_count; i++)
        if (ctx->import_local_names[i] && !strcmp(ctx->import_local_names[i], name))
            return true;
    return false;
}

/* The class the elements of this value are: `items[i]` and `for (const x of items)`. */
static const CBMRegisteredType *element_class(TSLSPContext *ctx, TSNode value, uint32_t at) {
    while (kind(value, "non_null_expression") || kind(value, "parenthesized_expression"))
        value = ts_node_named_child(value, 0);
    if (kind(value, "member_expression")) {
        TSNode property = field(value, "property");
        const char *member = kind(property, "property_identifier") ? text(ctx, property) : NULL;
        const CBMRegisteredType *holder =
            member ? receiver_type(ctx, field(value, "object"), at) : NULL;
        const char *key = holder ? cbm_arena_sprintf(ctx->arena, "%s[]", member) : NULL;
        return key ? member_type(ctx, holder, key, 0) : NULL;
    }
    if (!kind(value, "identifier"))
        return NULL;
    const char *name = text(ctx, value);
    const ts_proof_binding_t *binding = name ? scope_binding(ctx, name, at, NULL) : NULL;
    if (!binding || ts_node_start_byte(binding->declaration) > at)
        return NULL;
    TSNode annotation = field(binding->declaration, "type");
    if (annotation_names_local_type(ctx, annotation, at))
        return NULL;
    TSNode element = cbm_ts_element_type(annotation, ctx->source);
    const char *type_name = ts_node_is_null(element) ? NULL : type_class_name(ctx, element);
    return type_name ? named_type(ctx, type_name, at) : NULL;
}

/* What a name annotated `Record<K, V>` holds under any key. */
static const CBMRegisteredType *record_value(TSLSPContext *ctx, TSNode value, uint32_t at) {
    const char *name = kind(value, "identifier") ? text(ctx, value) : NULL;
    const ts_proof_binding_t *binding = name ? scope_binding(ctx, name, at, NULL) : NULL;
    if (!binding || ts_node_start_byte(binding->declaration) > at)
        return NULL;
    TSNode annotation = field(binding->declaration, "type");
    if (annotation_names_local_type(ctx, annotation, at))
        return NULL;
    TSNode held = cbm_ts_record_value_type(annotation, ctx->source);
    const char *type_name = ts_node_is_null(held) ? NULL : type_class_name(ctx, held);
    return type_name ? named_type(ctx, type_name, at) : NULL;
}

/* `a.b.c`: what the holder declares that member to be. A Record holds its value under any key,
 * so `.c` on one is that value. The walk hands up what `a.b` holds that way, which visits each
 * link of a chain once. */
static const CBMRegisteredType *member_receiver(TSLSPContext *ctx, TSNode receiver, uint32_t at,
                                                const CBMRegisteredType **keyed) {
    if (keyed)
        *keyed = NULL;
    TSNode object = field(receiver, "object");
    TSNode property = field(receiver, "property");
    const char *member = kind(property, "property_identifier") ? text(ctx, property) : NULL;
    if (!member || receiver_narrowed(ctx, receiver, at))
        return NULL;
    if (kind(object, "this")) {
        TSNode owner = this_class(ctx, object);
        const char *type_name =
            ts_node_is_null(owner) ? NULL : declared_field_type(ctx, owner, member);
        const CBMRegisteredType *declared = type_name ? named_type(ctx, type_name, at) : NULL;
        if (declared)
            return declared;
    }
    while (kind(object, "non_null_expression") || kind(object, "parenthesized_expression"))
        object = ts_node_named_child(object, 0);
    const CBMRegisteredType *under_key = NULL;
    const CBMRegisteredType *holder = kind(object, "member_expression")
                                          ? member_receiver(ctx, object, at, &under_key)
                                          : receiver_type(ctx, object, at);
    if (!holder)
        return under_key ? under_key : record_value(ctx, object, at);
    if (keyed) {
        const char *key = cbm_arena_sprintf(ctx->arena, "%s{}", member);
        *keyed = key ? member_type(ctx, holder, key, 0) : NULL;
    }
    return member_type(ctx, holder, member, 0);
}

/* What an unannotated declaration holds. The checker infers it from the initializer, and each
 * declaration is settled once, so a chain of aliases costs its length in total. */
static const CBMRegisteredType *initializer_type(TSLSPContext *ctx, ts_proof_binding_t *binding) {
    if (binding->value_settled)
        return binding->value_type;
    if (binding->value_visiting)
        return NULL;
    binding->value_visiting = true;
    TSNode value = field(binding->declaration, "value");
    const CBMRegisteredType *type = NULL;
    if (kind(binding->declaration, "variable_declarator")) {
        if (!ts_node_is_null(value))
            type = receiver_type(ctx, value, ts_node_start_byte(value));
    } else if (kind(binding->declaration, "for_in_statement")) {
        /* `for (const k in o)` holds keys; only `of` walks the elements. */
        TSNode iterated = field(binding->declaration, "right");
        const char *loop = text(ctx, field(binding->declaration, "operator"));
        if (loop && !strcmp(loop, "of") && !ts_node_is_null(iterated))
            type = element_class(ctx, iterated, ts_node_start_byte(iterated));
    } else {
        type = pattern_member_type(ctx, binding->declaration);
    }
    binding->value_visiting = false;
    binding->value_settled = true;
    binding->value_type = type;
    return type;
}

static const CBMRegisteredType *receiver_type(TSLSPContext *ctx, TSNode receiver, uint32_t at) {
    /* `a!.m()` and `(a).m()` are `a.m()` to the checker. */
    while (!ctx->js_mode &&
           (kind(receiver, "non_null_expression") || kind(receiver, "parenthesized_expression")))
        receiver = ts_node_named_child(receiver, 0);
    if (!ctx->js_mode && (kind(receiver, "this") || kind(receiver, "identifier")) &&
        receiver_narrowed(ctx, receiver, at))
        return NULL;
    if (kind(receiver, "this")) {
        const char *name = text(ctx, field(this_class(ctx, receiver), "name"));
        return name ? named_type(ctx, name, at) : NULL;
    }
    if (kind(receiver, "super"))
        return ctx->js_mode ? NULL : super_type(ctx, receiver, at);
    if (kind(receiver, "subscript_expression") && !ctx->js_mode)
        return element_class(ctx, field(receiver, "object"), at);
    /* `make().go()`: what the callee declares it returns. */
    if (kind(receiver, "call_expression") && !ctx->js_mode) {
        const CBMRegisteredFunc *callee = callee_function(ctx, field(receiver, "function"), at);
        return callee && callee->return_class_qn ? exact_type(ctx, callee->return_class_qn) : NULL;
    }
    if (kind(receiver, "await_expression") && !ctx->js_mode) {
        TSNode call = ts_node_named_child(receiver, 0);
        if (!kind(call, "call_expression"))
            return NULL;
        const CBMRegisteredFunc *callee = callee_function(ctx, field(call, "function"), at);
        const char *qn = !callee                  ? NULL
                         : callee->await_class_qn ? callee->await_class_qn
                                                  : callee->return_class_qn;
        return qn ? exact_type(ctx, qn) : NULL;
    }
    /* `(a as Foo).m()`: the cast is what the checker selects the member on. */
    if (kind(receiver, "as_expression") && !ctx->js_mode) {
        uint32_t parts = ts_node_named_child_count(receiver);
        const char *name =
            parts ? type_class_name(ctx, ts_node_named_child(receiver, parts - 1)) : NULL;
        return name ? named_type(ctx, name, at) : NULL;
    }
    /* A construction's value has exactly the class it names, in a typed file. */
    if (kind(receiver, "new_expression") && !ctx->js_mode) {
        TSNode constructor = field(receiver, "constructor");
        const char *name = kind(constructor, "identifier") ? text(ctx, constructor) : NULL;
        return name ? named_type(ctx, name, at) : NULL;
    }
    if (kind(receiver, "member_expression") && !ctx->js_mode)
        return member_receiver(ctx, receiver, at, NULL);
    if (!kind(receiver, "identifier"))
        return NULL;
    const char *name = text(ctx, receiver);
    const ts_proof_binding_t *binding = name ? scope_binding(ctx, name, at, NULL) : NULL;
    if (!binding || ts_node_start_byte(binding->declaration) > at)
        return NULL;
    TSNode annotation = field(binding->declaration, "type");
    /* A write must fit the annotation, so `let e: Foo; e = make()` stays a Foo in TypeScript.
     * JavaScript checks no such thing, and an inferred type follows the value. */
    if (binding->written && (ctx->js_mode || ts_node_is_null(annotation)))
        return NULL;
    const char *annotated = ctx->js_mode ? NULL : type_class_name(ctx, annotation);
    if (annotated)
        return named_type(ctx, annotated, at);
    if (kind(annotation, "type_annotation"))
        annotation = ts_node_named_child(annotation, 0);
    if (kind(annotation, "type_identifier")) {
        const char *type_name = text(ctx, annotation);
        return type_name ? named_type(ctx, type_name, at) : NULL;
    }
    if (!ctx->js_mode && ts_node_is_null(annotation))
        return initializer_type(ctx, (ts_proof_binding_t *)binding);
    return NULL;
}

static cbm_resolve_method_candidate_t classify_method(const cbm_resolve_method_query_t *query,
                                                      const CBMTypeRegistry *owner,
                                                      const CBMRegisteredFunc *candidate,
                                                      void *context) {
    (void)query;
    (void)owner;
    TSLSPContext *ctx = context;
    const char *target = candidate->qualified_name;
    if (target && candidate->sig && candidate->sig[0])
        target = cbm_arena_sprintf(ctx->arena, "%s%s", target, candidate->sig);
    return (cbm_resolve_method_candidate_t){
        .exposed = CBM_RESOLVE_METHOD_YES,
        .applicable = CBM_RESOLVE_METHOD_YES,
        .domain = (candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE) ? CBM_RESOLVE_DOMAIN_PROJECT
                                                                    : CBM_RESOLVE_DOMAIN_EXTERNAL,
        .graph_target = target,
        .graph_target_exact = target != NULL,
        .declaration_identity = candidate->declaration_identity,
        .declaration_identity_exact = candidate->declaration_identity != NULL};
}

cbm_resolve_method_result_t cbm_ts_proof_method(TSLSPContext *ctx, TSNode receiver,
                                                const char *name, TSNode occurrence) {
    cbm_resolve_method_result_t missing = {.result = {.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                                      .reason = "ts_receiver_binding_unproven"}};
    if (!ctx || !ctx->proof_audit || !ctx->proof_audit->complete || !name ||
        ts_node_is_null(occurrence))
        return missing;
    uint32_t at = ts_node_start_byte(occurrence);
    /* A method the class itself declares wins over anything it inherits. The
     * query below looks in that direct domain only; an inherited name abstains. */
    const CBMRegisteredType *owner = receiver_type(ctx, receiver, at);
    if (!owner)
        return missing;
    const CBMRegisteredType *selected =
        type_is_alias(owner) ? alias_member(ctx, owner, name, true) : owner;
    if (!selected)
        return missing;
    /* JavaScript can replace a method by assignment; TypeScript checks it. */
    if (ctx->js_mode) {
        bool occupied = false;
        (void)find_binding(ctx, name, at, &occupied);
        if (occupied)
            return missing;
    }
    /* Flow narrowing holds a more exact class than the declaration: `instanceof` picks another
     * method, so the proof and the walk must not disagree. A template or union is the
     * annotation as written, and a name with no entry here is a module const or a default
     * value's `this`, walked before its method's scope binds it. */
    if (kind(receiver, "identifier") || kind(receiver, "this")) {
        const char *spelled = text(ctx, receiver);
        const CBMType *held = spelled ? cbm_scope_lookup(ctx->current_scope, spelled) : NULL;
        /* A name the registry does not know is the walk's own guess at an imported class. */
        if (held && held->kind == CBM_TYPE_NAMED && held->data.named.qualified_name &&
            strcmp(held->data.named.qualified_name, owner->qualified_name) &&
            strcmp(held->data.named.qualified_name, selected->qualified_name) &&
            exact_type(ctx, held->data.named.qualified_name))
            return missing;
    }
    owner = selected;
    if (!declares_method(ctx, owner, name) && !ctx->js_mode) {
        const CBMRegisteredType *base = inherited_owner(ctx, owner, name, 0);
        if (base)
            owner = base;
    }
    /* One entry per occurrence, spelled so it can answer no name lookup. The walk visits a value
     * reference before the arrow beside it, so a shared entry would see one site after a
     * later one, and that stale read used to end every proof for the rest of the file. */
    const char *binding = cbm_arena_sprintf(ctx->arena, "@%u", at);
    if (!binding)
        return missing;
    cbm_scope_bind(ctx->current_scope, binding, cbm_type_named(ctx->arena, owner->qualified_name));
    const CBMType *observed = cbm_scope_lookup(ctx->current_scope, binding);
    if (!observed || observed->kind != CBM_TYPE_NAMED || !observed->data.named.qualified_name ||
        strcmp(observed->data.named.qualified_name, owner->qualified_name) != 0)
        return missing;
    cbm_resolve_result_t evidence = cbm_lsp_proof_type_lookup(
        &ctx->proven_types, binding, receiver, "typescript-source-receiver-type",
        owner->qualified_name, owner->declaration_identity,
        owner->graph_qualified_name ? owner->graph_qualified_name : owner->qualified_name,
        owner->source_flags,
        "complete-binding-audit;exact-annotation-or-constructor;observed-type-agrees");
    if (!cbm_resolve_types_observe(&ctx->proven_types, ctx->current_scope, binding, at, observed,
                                   CBM_RESOLVE_TYPE_OBSERVED_BINDING, &evidence))
        return missing;
    cbm_resolve_type_fact_t fact =
        cbm_resolve_types_lookup(&ctx->proven_types, ctx->current_scope, binding, at);
    cbm_resolve_method_query_t query = {.proof_pool = &ctx->proven_types.proofs,
                                        .proof_source = &ctx->proven_types.proof_source,
                                        .lookup_domain = owner->qualified_name,
                                        .registry = ctx->registry,
                                        .receiver = &fact,
                                        .receiver_qn = owner->qualified_name,
                                        .method_name = name,
                                        .argument_count = -1,
                                        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                                                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT |
                                                         CBM_RESOLVE_METHOD_NAME_IDENTITY,
                                        .classify = classify_method,
                                        .context = ctx};
    return cbm_resolve_method(&query);
}
