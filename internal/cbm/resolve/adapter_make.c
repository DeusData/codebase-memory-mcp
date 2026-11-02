/* Make variable rules, from the GNU make manual's "How to Use Variables".
 *
 * A make process has one variable namespace. `X = v`, `X := v`, `X ?= v` and `X += v` all write
 * the same variable, which is why the graph has one node for them, and `$(X)` or `${X}` reads it
 * wherever the assignment sits, since a recursive variable expands when it is used. A name the
 * file does not assign comes from an included file, the environment or the command line, which
 * this does not decide. A computed name (`$($(X)_FLAGS)`) is unknown. */
#include "source_adapter_internal.h"

#define MAKE_VARIABLES "V:"

typedef struct {
    cbm_source_walk_t *walk;
    const char *owner;
} make_ctx_t;

static void make_collect_walk(make_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "variable_assignment")) {
        TSNode name = cbm_source_field(node, "name");
        const char *text = cbm_source_is(name, "word") ? cbm_source_text(ctx->walk, name) : NULL;
        if (text && !cbm_source_lookup(ctx->walk, ctx->owner, text))
            (void)cbm_source_declare(
                ctx->walk, ctx->owner, text,
                cbm_source_def_qn(ctx->walk, text, ts_node_start_point(node).row + 1), 0, 0);
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                make_collect_walk(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void make_visit_reference(make_ctx_t *ctx, TSNode node) {
    cbm_source_walk_t *walk = ctx->walk;
    const char *name = NULL;
    if (ts_node_named_child_count(node) == 1 && cbm_source_is(ts_node_named_child(node, 0), "word"))
        name = cbm_source_text(walk, ts_node_named_child(node, 0));
    const cbm_source_decl_t *decl = name ? cbm_source_lookup(walk, ctx->owner, name) : NULL;
    cbm_source_emit_node(walk, node, CBM_SOURCE_SITE_REFERENCE, decl ? decl->qn : NULL,
                         CBM_RESOLVE_RULE_SCOPE_BINDING, ctx->owner,
                         !name      ? "make_variable_name_computed"
                         : !decl    ? "make_variable_not_assigned_in_this_file"
                         : decl->qn ? "make_variable_of_this_file"
                                    : "make_variable_has_no_graph_node");
}

static void make_resolve_walk(make_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "variable_reference"))
        make_visit_reference(ctx, node);
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                make_resolve_walk(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static const char *make_owner(cbm_source_walk_t *walk) {
    return cbm_arena_sprintf(walk->arena, MAKE_VARIABLES "%s", walk->file);
}

static void make_collect(cbm_source_walk_t *walk) {
    walk->table = walk->writable;
    make_ctx_t ctx = {.walk = walk, .owner = make_owner(walk)};
    if (ctx.owner)
        make_collect_walk(&ctx, walk->root, 0);
}

static void make_resolve(cbm_source_walk_t *walk) {
    make_ctx_t ctx = {.walk = walk, .owner = make_owner(walk)};
    if (ctx.owner)
        make_resolve_walk(&ctx, walk->root, 0);
}

const cbm_source_adapter_t cbm_source_adapter_make = {.language = CBM_LANG_MAKEFILE,
                                                      .collect = make_collect,
                                                      .resolve = make_resolve,
                                                      .decides_references = true,
                                                      .parse_errors_are_local = true,
                                                      .one_name_one_node = true};
