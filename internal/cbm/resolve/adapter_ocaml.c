/* OCaml binding rules, from the manual's module language chapter.
 *
 * Scope is sequential. A `let` is visible to what follows it, and to itself only
 * with `rec`. A local let, parameter, fun or match pattern shadows anything outer.
 * `open M` brings M's names in from that point, shadowing earlier ones, so an open
 * of a module whose contents are not known leaves every earlier name uncertain.
 * Two top-level lets of one name are two declarations with one graph identity,
 * which the resolver's target check refuses.
 *
 * A file foo.ml is the compilation unit Foo. Within one directory a build's modules
 * see each other by name, unless something in scope defines a module of that name.
 * `M.f` therefore looks through local module aliases, then at the unit beside this
 * file, and only when no open in scope is open-ended. A functor argument, a first
 * class module and anything behind `include` of an unknown module are not followed. */
#include "source_adapter_internal.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#define OCAML_OPEN_ENDED "&open_ended"
#define OCAML_MODULE_PREFIX "&module:"

typedef struct ocaml_name {
    const char *name;
    struct ocaml_name *next;
} ocaml_name_t;

typedef struct ocaml_open {
    const char *unit; /* a project unit's key, NULL when the module is not known */
    uint32_t at, line;
    struct ocaml_open *next;
} ocaml_open_t;

typedef struct {
    cbm_source_walk_t *walk;
    bool reference;     /* the name being read is a value, not an application */
    uint32_t site_line; /* the occurrence being decided */
    const char *unit;
    ocaml_name_t *locals;
    ocaml_open_t *opens;
} ocaml_ctx_t;

/* src/foo/bar.ml is unit key src/foo/bar; its module name is Bar. */
static const char *ocaml_unit_key(cbm_source_walk_t *walk) {
    const char *file = walk->file ? walk->file : "";
    const char *dot = strrchr(file, '.');
    return dot ? cbm_arena_strndup(walk->arena, file, (size_t)(dot - file)) : file;
}

/* The unit a module name denotes beside this file. */
static const char *ocaml_sibling_unit(const ocaml_ctx_t *ctx, const char *module) {
    char lowered[256];
    size_t length = strlen(module);
    if (!length || length >= sizeof(lowered))
        return NULL;
    memcpy(lowered, module, length + 1);
    lowered[0] = (char)tolower((unsigned char)lowered[0]);
    const char *slash = strrchr(ctx->unit, '/');
    const char *key = slash ? cbm_arena_sprintf(ctx->walk->arena, "%.*s/%s",
                                                (int)(slash - ctx->unit), ctx->unit, lowered)
                            : cbm_arena_strdup(ctx->walk->arena, lowered);
    return key && cbm_source_lookup(ctx->walk, key, "") ? key : NULL;
}

static TSNode ocaml_child(TSNode node, const char *kind) {
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        if (cbm_source_is(ts_node_named_child(node, i), kind))
            return ts_node_named_child(node, i);
    return (TSNode){0};
}

/* ── Phase one ─────────────────────────────────────────────────────── */

static void ocaml_collect_binding(cbm_source_walk_t *walk, const char *owner, TSNode binding) {
    TSNode pattern = cbm_source_field(binding, "pattern");
    if (!cbm_source_is(pattern, "value_name")) {
        /* A destructuring let binds values, and those shadow functions by name. */
        return;
    }
    const char *name = cbm_source_text(walk, pattern);
    if (name) {
        uint32_t line = ts_node_start_point(binding).row + 1;
        (void)cbm_source_declare(walk, owner, name, cbm_source_def_qn(walk, name, line), 0, line);
    }
}

static void ocaml_collect_structure(cbm_source_walk_t *walk, const char *owner, TSNode structure,
                                    unsigned depth) {
    if (!cbm_source_step(walk, depth))
        return;
    uint32_t count = ts_node_named_child_count(structure);
    for (uint32_t i = 0; i < count; i++) {
        TSNode item = ts_node_named_child(structure, i);
        if (cbm_source_is(item, "value_definition")) {
            uint32_t parts = ts_node_named_child_count(item);
            for (uint32_t p = 0; p < parts; p++)
                if (cbm_source_is(ts_node_named_child(item, p), "let_binding"))
                    ocaml_collect_binding(walk, owner, ts_node_named_child(item, p));
        } else if (cbm_source_is(item, "module_definition")) {
            TSNode binding = ocaml_child(item, "module_binding");
            const char *name = cbm_source_text(walk, ocaml_child(binding, "module_name"));
            TSNode body = cbm_source_field(binding, "body");
            if (!name)
                continue;
            const char *key = cbm_arena_sprintf(walk->arena, "%s%s", OCAML_MODULE_PREFIX, name);
            cbm_source_decl_t *decl = cbm_source_declare(
                walk, owner, key, NULL, CBM_SOURCE_DECL_F_TYPE, ts_node_start_point(item).row + 1);
            if (cbm_source_is(body, "structure")) {
                const char *nested = cbm_arena_sprintf(walk->arena, "%s#%s", owner, name);
                if (decl)
                    decl->type = cbm_source_table_intern(walk, nested);
                (void)cbm_source_declare(walk, nested, "", NULL, CBM_SOURCE_DECL_F_TYPE, 0);
                ocaml_collect_structure(walk, nested, body, depth + 1);
            } else if (cbm_source_is(body, "module_path") && decl) {
                /* `module C = Config`: stored as the spelling, read beside the file. */
                decl->flags |= CBM_SOURCE_DECL_F_VALUE;
                decl->type = cbm_source_table_intern(walk, cbm_source_text(walk, body));
            }
        } else if (cbm_source_is(item, "include_module") || cbm_source_is(item, "open_module")) {
            /* What this unit's `include` adds is only known if the module is. */
            if (cbm_source_is(item, "include_module"))
                (void)cbm_source_declare(walk, owner, OCAML_OPEN_ENDED, NULL,
                                         CBM_SOURCE_DECL_F_OPEN, 0);
        }
    }
}

static void ocaml_collect(cbm_source_walk_t *walk) {
    const char *unit = ocaml_unit_key(walk);
    (void)cbm_source_declare(walk, unit, "", NULL, CBM_SOURCE_DECL_F_TYPE, 1);
    ocaml_collect_structure(walk, unit, walk->root, 0);
}

/* ── Phase two ─────────────────────────────────────────────────────── */

static bool ocaml_is_local(const ocaml_ctx_t *ctx, const char *name) {
    for (const ocaml_name_t *local = ctx->locals; local; local = local->next)
        if (!strcmp(local->name, name))
            return true;
    return false;
}

static void ocaml_bind(ocaml_ctx_t *ctx, const char *name) {
    ocaml_name_t *local = name ? cbm_arena_alloc(ctx->walk->arena, sizeof(*local)) : NULL;
    if (!local)
        return;
    *local = (ocaml_name_t){name, ctx->locals};
    ctx->locals = local;
}

/* Every name a pattern introduces. */
static void ocaml_bind_pattern(ocaml_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "value_pattern") || cbm_source_is(node, "value_name"))
        ocaml_bind(ctx, cbm_source_text(ctx->walk, node));
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        ocaml_bind_pattern(ctx, ts_node_named_child(node, i), depth + 1);
}

/* A module alias in this unit, or the unit beside this file. */
static const char *ocaml_module_unit(ocaml_ctx_t *ctx, const char *owner, const char *module,
                                     unsigned depth) {
    if (depth > 8 || !module)
        return NULL;
    char key[320];
    int length = snprintf(key, sizeof(key), "%s%s", OCAML_MODULE_PREFIX, module);
    if (length < 0 || (size_t)length >= sizeof(key))
        return NULL;
    const char *scope = owner;
    while (scope) {
        const cbm_source_decl_t *alias = cbm_source_lookup(ctx->walk, scope, key);
        /* A module binding is sequential and not recursive: it is in scope only after
         * its definition ends, so the same name inside its body or before it is another
         * module. A structure's own key is a prefix of everything inside it. */
        if (alias && alias->type && !alias->next) {
            size_t own = strlen(alias->type);
            bool inside = !(alias->flags & CBM_SOURCE_DECL_F_VALUE) &&
                          !strncmp(ctx->unit, alias->type, own) &&
                          (ctx->unit[own] == '\0' || ctx->unit[own] == '#');
            if (inside || alias->line > ctx->site_line)
                alias = NULL;
        }
        if (alias) {
            if (alias->next || !alias->type)
                return NULL;
            return alias->flags & CBM_SOURCE_DECL_F_VALUE
                       ? ocaml_module_unit(ctx, scope, alias->type, depth + 1)
                       : alias->type;
        }
        const char *hash = strrchr(scope, '#');
        scope = hash ? cbm_arena_strndup(ctx->walk->arena, scope, (size_t)(hash - scope)) : NULL;
    }
    return ocaml_sibling_unit(ctx, module);
}

static void ocaml_note_open(ocaml_ctx_t *ctx, TSNode item) {
    TSNode path = cbm_source_field(item, "module");
    const char *text = cbm_source_text(ctx->walk, path);
    ocaml_open_t *open = cbm_arena_alloc(ctx->walk->arena, sizeof(*open));
    if (!open)
        return;
    *open = (ocaml_open_t){
        .unit = text && !strchr(text, '.') ? ocaml_module_unit(ctx, ctx->unit, text, 0) : NULL,
        .at = ts_node_start_byte(item),
        .line = ts_node_start_point(item).row + 1,
        .next = ctx->opens};
    ctx->opens = open;
}

static bool ocaml_unit_closed(const ocaml_ctx_t *ctx, const char *unit) {
    return unit && !cbm_source_lookup(ctx->walk, unit, OCAML_OPEN_ENDED);
}

static void ocaml_emit(ocaml_ctx_t *ctx, TSNode site, const char *target, cbm_resolve_rule_t rule,
                       const char *domain, const char *reason) {
    cbm_source_emit_node(ctx->walk, site,
                         ctx->reference ? CBM_SOURCE_SITE_REFERENCE : CBM_SOURCE_SITE_CALL, target,
                         rule, domain, reason);
}

static void ocaml_visit_application(ocaml_ctx_t *ctx, TSNode site, TSNode function, uint32_t at) {
    ctx->site_line = ts_node_start_point(site).row + 1;
    if (!cbm_source_is(function, "value_path")) {
        ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_computed_function");
        return;
    }
    TSNode path = ocaml_child(function, "module_path");
    TSNode leaf = ocaml_child(function, "value_name");
    const char *name = cbm_source_text(ctx->walk, leaf);
    if (!name) {
        ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_operator_value");
        return;
    }
    const char *reason = NULL;
    if (ts_node_is_null(path)) {
        if (ocaml_is_local(ctx, name)) {
            ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_locally_bound_name");
            return;
        }
        /* A nested structure sees the names of those around it. */
        const cbm_source_decl_t *decl = NULL;
        const char *scope = ctx->unit;
        while (scope && !(decl = cbm_source_lookup(ctx->walk, scope, name))) {
            const char *hash = strrchr(scope, '#');
            scope =
                hash ? cbm_arena_strndup(ctx->walk->arena, scope, (size_t)(hash - scope)) : NULL;
        }
        if (decl) {
            uint32_t line = ts_node_start_point(site).row + 1;
            const char *qn = decl->line <= line ? cbm_source_unique_qn(decl, &reason) : NULL;
            if (!qn) {
                ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                           reason ? reason : "ocaml_let_after_its_use");
                return;
            }
            /* Any open since the declaration could have replaced the name. */
            for (const ocaml_open_t *open = ctx->opens; open; open = open->next) {
                if (open->at < at && open->line > decl->line &&
                    (!ocaml_unit_closed(ctx, open->unit) ||
                     cbm_source_lookup(ctx->walk, open->unit, name))) {
                    ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                               "ocaml_open_may_replace_name");
                    return;
                }
            }
            ocaml_emit(ctx, site, qn, CBM_RESOLVE_RULE_SCOPE_BINDING, scope,
                       "ocaml_sequential_let");
            return;
        }
        /* The latest open that declares the name, with nothing unknown after it. */
        for (const ocaml_open_t *open = ctx->opens; open; open = open->next) {
            if (open->at >= at)
                continue;
            if (!ocaml_unit_closed(ctx, open->unit)) {
                ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                           "ocaml_open_of_unknown_module");
                return;
            }
            const cbm_source_decl_t *opened = cbm_source_lookup(ctx->walk, open->unit, name);
            if (opened) {
                const char *qn = cbm_source_unique_qn(opened, &reason);
                ocaml_emit(ctx, site, qn, CBM_RESOLVE_RULE_IMPORT_BINDING, open->unit,
                           qn ? "ocaml_name_from_open" : reason);
                return;
            }
        }
        ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_name_not_in_scope");
        return;
    }
    const char *module = cbm_source_text(ctx->walk, path);
    if (!module || strchr(module, '.')) {
        ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_nested_module_path");
        return;
    }
    const char *unit = ocaml_module_unit(ctx, ctx->unit, module, 0);
    if (!unit) {
        ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_module_not_in_project");
        return;
    }
    /* An open-ended open could define a module of this name. */
    for (const ocaml_open_t *open = ctx->opens; open; open = open->next) {
        if (open->at >= at)
            continue;
        char key[320];
        int length = snprintf(key, sizeof(key), "%s%s", OCAML_MODULE_PREFIX, module);
        bool defines = length > 0 && (size_t)length < sizeof(key) && open->unit &&
                       cbm_source_lookup(ctx->walk, open->unit, key);
        if (!ocaml_unit_closed(ctx, open->unit) || defines) {
            ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                       "ocaml_open_may_define_module");
            return;
        }
    }
    const cbm_source_decl_t *decl = cbm_source_lookup(ctx->walk, unit, name);
    const char *qn = cbm_source_unique_qn(decl, &reason);
    ocaml_emit(ctx, site, qn, CBM_RESOLVE_RULE_IMPORT_BINDING, unit,
               qn ? "ocaml_compilation_unit_member" : reason);
}

static void ocaml_visit(ocaml_ctx_t *ctx, TSNode node, unsigned depth);

static void ocaml_visit_children(ocaml_ctx_t *ctx, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                ocaml_visit(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

/* Parameters and the body of one binding, in a scope of their own. */
static void ocaml_visit_binding(ocaml_ctx_t *ctx, TSNode binding, unsigned depth) {
    ocaml_name_t *saved = ctx->locals;
    uint32_t count = ts_node_named_child_count(binding);
    TSNode pattern = cbm_source_field(binding, "pattern");
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(binding, i);
        if (cbm_source_is(child, "parameter"))
            ocaml_bind_pattern(ctx, child, depth + 1);
    }
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(binding, i);
        if (!ts_node_eq(child, pattern) && !cbm_source_is(child, "parameter"))
            ocaml_visit(ctx, child, depth + 1);
    }
    ctx->locals = saved;
}

static void ocaml_visit_value_definition(ocaml_ctx_t *ctx, TSNode node, bool local,
                                         unsigned depth) {
    bool recursive = cbm_source_token(node, "rec");
    uint32_t count = ts_node_named_child_count(node);
    if (local && recursive) {
        for (uint32_t i = 0; i < count; i++) {
            TSNode binding = ts_node_named_child(node, i);
            if (cbm_source_is(binding, "let_binding"))
                ocaml_bind_pattern(ctx, cbm_source_field(binding, "pattern"), depth + 1);
        }
    }
    for (uint32_t i = 0; i < count; i++) {
        TSNode binding = ts_node_named_child(node, i);
        if (cbm_source_is(binding, "let_binding"))
            ocaml_visit_binding(ctx, binding, depth + 1);
    }
    if (local && !recursive) {
        for (uint32_t i = 0; i < count; i++) {
            TSNode binding = ts_node_named_child(node, i);
            if (cbm_source_is(binding, "let_binding"))
                ocaml_bind_pattern(ctx, cbm_source_field(binding, "pattern"), depth + 1);
        }
    }
}

static void ocaml_visit(ocaml_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "value_path")) {
        /* A name read as a value follows the same lookup as an application's head. */
        ctx->reference = true;
        ocaml_visit_application(ctx, node, node, ts_node_start_byte(node));
        ctx->reference = false;
        return;
    }
    if (!strcmp(kind, "open_module")) {
        ocaml_note_open(ctx, node);
        return;
    }
    if (!strcmp(kind, "value_definition")) {
        ocaml_visit_value_definition(ctx, node, false, depth);
        return;
    }
    if (!strcmp(kind, "let_expression")) {
        ocaml_name_t *saved = ctx->locals;
        ocaml_open_t *saved_opens = ctx->opens;
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (cbm_source_is(child, "value_definition"))
                ocaml_visit_value_definition(ctx, child, true, depth);
            else
                ocaml_visit(ctx, child, depth + 1);
        }
        ctx->locals = saved;
        ctx->opens = saved_opens;
        return;
    }
    if (!strcmp(kind, "fun_expression") || !strcmp(kind, "match_case") ||
        !strcmp(kind, "for_expression") || !strcmp(kind, "function_expression")) {
        ocaml_name_t *saved = ctx->locals;
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (cbm_source_is(child, "parameter"))
                ocaml_bind_pattern(ctx, child, depth + 1);
        }
        TSNode pattern = cbm_source_field(node, "pattern");
        if (!ts_node_is_null(pattern))
            ocaml_bind_pattern(ctx, pattern, depth + 1);
        ocaml_visit_children(ctx, node, depth);
        ctx->locals = saved;
        return;
    }
    if (!strcmp(kind, "module_definition")) {
        /* A nested structure has names and opens of its own, which this walk does
         * not scope; what runs in it decides nothing. */
        TSNode binding = ocaml_child(node, "module_binding");
        TSNode body = cbm_source_field(binding, "body");
        if (cbm_source_is(body, "structure")) {
            const char *name = cbm_source_text(ctx->walk, ocaml_child(binding, "module_name"));
            const char *saved = ctx->unit;
            ocaml_open_t *saved_opens = ctx->opens;
            if (name)
                ctx->unit = cbm_arena_sprintf(ctx->walk->arena, "%s#%s", saved, name);
            ocaml_visit_children(ctx, body, depth);
            ctx->unit = saved;
            ctx->opens = saved_opens;
        }
        return;
    }
    if (!strcmp(kind, "application_expression"))
        ocaml_visit_application(ctx, node, cbm_source_field(node, "function"),
                                ts_node_start_byte(node));
    ocaml_visit_children(ctx, node, depth);
}

static void ocaml_resolve(cbm_source_walk_t *walk) {
    ocaml_ctx_t ctx = {.walk = walk, .unit = ocaml_unit_key(walk)};
    ocaml_visit_children(&ctx, walk->root, 0);
}

const cbm_source_adapter_t cbm_source_adapter_ocaml = {.language = CBM_LANG_OCAML,
                                                       .collect = ocaml_collect,
                                                       .resolve = ocaml_resolve,
                                                       .decides_references = true};
