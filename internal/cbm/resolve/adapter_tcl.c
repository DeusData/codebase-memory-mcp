/* Tcl binding rules, from the Tcl(n) dodecalogue and the `namespace` manual page.
 *
 * Every word in command position is looked up in the command table of the current namespace,
 * then of the global one, when the command runs; nothing lexical can shadow it, and a variable
 * lives in another table. `proc` writes that table, so which procs are visible is a matter of
 * which files ran in the interpreter. `a::b` and `::a::b` name their namespace directly.
 *
 * A script's own procs are always visible to it. Another file's proc reaches it only through the
 * global table of a shared interpreter, and the evidence for one is `source` in the project: a
 * file sourced by a literal path is library code, and a file that sources by a computed path is
 * a harness whose procs the scripts it loads can call. A proc declared by two such files leaves
 * the name open, because the last `source` decides, and `rename` of a name makes it unknown
 * everywhere. A standalone script's procs are visible to nobody else.
 *
 * The Tcl grammar leaves expression syntax as parse errors in most real files. No binding here
 * depends on the structure around such an error, so the rest of the file still decides. */
#include "source_adapter_internal.h"
#include <stdio.h>

#define TCL_PROCS "P:"
#define TCL_OWN "H:"
#define TCL_SOURCED "L:"

enum { TCL_PATH_BUF = 1024 };

typedef struct {
    cbm_source_walk_t *walk;
    const char *directory;
    const char *namespace_name; /* current namespace, "" for the global one */
} tcl_ctx_t;

static const char *tcl_directory(cbm_source_walk_t *walk) {
    const char *slash = strrchr(walk->file, '/');
    return slash ? cbm_arena_strndup(walk->arena, walk->file, (size_t)(slash - walk->file)) : "";
}

static const char *tcl_owner(tcl_ctx_t *ctx, const char *table, const char *space) {
    return cbm_arena_sprintf(ctx->walk->arena, "%s%s", table, space);
}

static const char *tcl_own_owner(tcl_ctx_t *ctx, const char *space) {
    return cbm_arena_sprintf(ctx->walk->arena, TCL_OWN "%s\x1d%s", ctx->walk->file, space);
}

/* A name's namespace and leaf, from where it is written. `::a::b` is absolute, and so is a name
 * inside a namespace once it has been joined with that namespace. */
static void tcl_split(tcl_ctx_t *ctx, const char *name, const char *space, const char **out_space,
                      const char **out_leaf) {
    bool absolute = !strncmp(name, "::", 2);
    const char *body = absolute ? name + 2 : name;
    const char *last = NULL;
    for (const char *p = body; (p = strstr(p, "::")); p += 2)
        last = p;
    *out_leaf = last ? last + 2 : body;
    if (!last) {
        *out_space = absolute ? "" : space;
        return;
    }
    const char *prefix = cbm_arena_strndup(ctx->walk->arena, body, (size_t)(last - body));
    *out_space = absolute || !*space
                     ? prefix
                     : cbm_arena_sprintf(ctx->walk->arena, "%s::%s", space, prefix ? prefix : "");
}

static void tcl_declare_once(tcl_ctx_t *ctx, const char *owner, const char *name, const char *qn) {
    cbm_source_decl_t *head = (cbm_source_decl_t *)cbm_source_lookup(ctx->walk, owner, name);
    if (!head) {
        (void)cbm_source_declare(ctx->walk, owner, name, qn, 0, 0);
        return;
    }
    if (!qn || !head->qn || strcmp(qn, head->qn))
        head->flags |= CBM_SOURCE_DECL_F_OPEN;
}

static const char *tcl_word(tcl_ctx_t *ctx, TSNode word) {
    if (cbm_source_is(word, "simple_word"))
        return cbm_source_text(ctx->walk, word);
    if (cbm_source_is(word, "quoted_word") && ts_node_named_child_count(word) == 0) {
        const char *text = cbm_source_text(ctx->walk, word);
        size_t length = text ? strlen(text) : 0;
        return length > 2 ? cbm_arena_strndup(ctx->walk->arena, text + 1, length - 2) : NULL;
    }
    return NULL;
}

static void tcl_collect_source(tcl_ctx_t *ctx, TSNode arguments) {
    /* The grammar splits `tests/$name.tcl` into words: the whole list says if it is computed. */
    const char *text = cbm_source_text(ctx->walk, arguments);
    size_t length = text ? strlen(text) : 0;
    const char *path = text && length >= 2 && text[0] == '"' && text[length - 1] == '"'
                           ? cbm_arena_strndup(ctx->walk->arena, text + 1, length - 2)
                           : text;
    if (!path || strpbrk(path, "$[")) {
        (void)cbm_source_declare(ctx->walk, TCL_SOURCED, ctx->walk->file, NULL, 0, 0);
        return;
    }
    if (!*path || *path == '/')
        return;
    char joined[TCL_PATH_BUF];
    /* Relative to the working directory: the script's own, or the repository's. */
    if (ctx->directory && cbm_source_join_path(ctx->directory, path, joined, sizeof(joined)))
        (void)cbm_source_declare(ctx->walk, TCL_SOURCED, joined, NULL, 0, 0);
    if (cbm_source_join_path("", path, joined, sizeof(joined)))
        (void)cbm_source_declare(ctx->walk, TCL_SOURCED, joined, NULL, 0, 0);
}

static void tcl_collect_command(tcl_ctx_t *ctx, TSNode command) {
    const char *name = cbm_source_text(ctx->walk, cbm_source_field(command, "name"));
    TSNode arguments = cbm_source_field(command, "arguments");
    if (!name || ts_node_is_null(arguments) || !ts_node_named_child_count(arguments))
        return;
    TSNode first = ts_node_named_child(arguments, 0);
    if (!strcmp(name, "source")) {
        tcl_collect_source(ctx, arguments);
    } else if (!strcmp(name, "rename")) {
        const char *renamed = tcl_word(ctx, first);
        if (renamed) {
            const char *space = "", *leaf = renamed;
            tcl_split(ctx, renamed, ctx->namespace_name, &space, &leaf);
            const char *owner = tcl_owner(ctx, TCL_PROCS, space ? space : "");
            if (owner) {
                tcl_declare_once(ctx, owner, leaf, NULL);
                tcl_declare_once(ctx, owner, leaf, NULL);
            }
        }
    }
}

static void tcl_collect_walk(tcl_ctx_t *ctx, TSNode node, unsigned depth);

static void tcl_collect_children(tcl_ctx_t *ctx, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                tcl_collect_walk(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

/* `namespace eval a { ... }`: the namespace a word list names, NULL for any other subcommand. */
static const char *tcl_namespace_of(tcl_ctx_t *ctx, TSNode node, TSNode *body) {
    TSNode words = (TSNode){0};
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count && ts_node_is_null(words); i++)
        if (cbm_source_is(ts_node_named_child(node, i), "word_list"))
            words = ts_node_named_child(node, i);
    if (ts_node_is_null(words) || ts_node_named_child_count(words) < 3)
        return NULL;
    const char *verb = cbm_source_text(ctx->walk, ts_node_named_child(words, 0));
    const char *name = tcl_word(ctx, ts_node_named_child(words, 1));
    if (!verb || strcmp(verb, "eval") || !name)
        return NULL;
    *body = ts_node_named_child(words, 2);
    const char *leaf = NULL, *space = NULL;
    const char *full = cbm_arena_sprintf(ctx->walk->arena, "%s::x", name);
    if (!full)
        return NULL;
    tcl_split(ctx, full, ctx->namespace_name, &space, &leaf);
    return space;
}

static void tcl_collect_walk(tcl_ctx_t *ctx, TSNode node, unsigned depth) {
    cbm_source_walk_t *walk = ctx->walk;
    if (!cbm_source_step(walk, depth))
        return;
    if (cbm_source_is(node, "procedure")) {
        const char *name = cbm_source_text(walk, cbm_source_field(node, "name"));
        if (name) {
            const char *space = "", *leaf = name;
            tcl_split(ctx, name, ctx->namespace_name, &space, &leaf);
            const char *qn = cbm_source_def_qn_at(walk, name, ts_node_start_point(node).row + 1);
            const char *project = tcl_owner(ctx, TCL_PROCS, space ? space : "");
            const char *own = tcl_own_owner(ctx, space ? space : "");
            if (project && own) {
                tcl_declare_once(ctx, project, leaf, qn);
                tcl_declare_once(ctx, own, leaf, qn);
            }
        }
    } else if (cbm_source_is(node, "command")) {
        tcl_collect_command(ctx, node);
    } else if (cbm_source_is(node, "namespace")) {
        TSNode body = (TSNode){0};
        const char *inner = tcl_namespace_of(ctx, node, &body);
        if (inner) {
            const char *outer = ctx->namespace_name;
            ctx->namespace_name = inner;
            tcl_collect_children(ctx, body, depth);
            ctx->namespace_name = outer;
            return;
        }
    }
    tcl_collect_children(ctx, node, depth);
}

static void tcl_collect(cbm_source_walk_t *walk) {
    walk->table = walk->writable;
    tcl_ctx_t ctx = {.walk = walk, .namespace_name = ""};
    ctx.directory = tcl_directory(walk);
    if (!ctx.directory) {
        cbm_source_fail(walk, "tcl_scope_allocation");
        return;
    }
    tcl_collect_walk(&ctx, walk->root, 0);
}

static void tcl_emit_abstain(tcl_ctx_t *ctx, TSNode command, const char *reason) {
    cbm_source_emit_node(ctx->walk, command, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                         NULL, reason);
}

/* A command's own namespace first, then the global one, for a name with no `::` in it. */
static const cbm_source_decl_t *tcl_lookup_table(tcl_ctx_t *ctx, const char *space,
                                                 const char *leaf, bool own) {
    const char *owner = own ? tcl_own_owner(ctx, space) : tcl_owner(ctx, TCL_PROCS, space);
    return owner ? cbm_source_lookup(ctx->walk, owner, leaf) : NULL;
}

static void tcl_visit_command(tcl_ctx_t *ctx, TSNode command) {
    cbm_source_walk_t *walk = ctx->walk;
    TSNode name_node = cbm_source_field(command, "name");
    const char *name =
        cbm_source_is(name_node, "simple_word") ? cbm_source_text(walk, name_node) : NULL;
    if (!name || strpbrk(name, "$[")) {
        tcl_emit_abstain(ctx, command, "tcl_command_computed");
        return;
    }
    const char *space = "", *leaf = name;
    bool qualified = strstr(name, "::") != NULL;
    tcl_split(ctx, name, ctx->namespace_name, &space, &leaf);
    if (!space)
        space = "";
    /* Inside a namespace an unqualified name tries it, then the global one. */
    const char *tries[2] = {space, qualified || !*space ? NULL : ""};
    for (size_t i = 0; i < 2 && tries[i]; i++) {
        const cbm_source_decl_t *own = tcl_lookup_table(ctx, tries[i], leaf, true);
        const cbm_source_decl_t *project = tcl_lookup_table(ctx, tries[i], leaf, false);
        if (!project)
            continue;
        /* `rename` marked the shared entry: the name is not what its proc declared. */
        if (own && !(own->flags & CBM_SOURCE_DECL_F_OPEN)) {
            cbm_source_emit_node(
                walk, command, CBM_SOURCE_SITE_CALL, own->qn, CBM_RESOLVE_RULE_SCOPE_BINDING,
                walk->file, own->qn ? "tcl_proc_of_this_script" : "tcl_proc_has_no_graph_node");
            return;
        }
        if (project->flags & CBM_SOURCE_DECL_F_OPEN) {
            tcl_emit_abstain(ctx, command, "tcl_proc_declared_by_more_than_one_file");
            return;
        }
        if (!cbm_source_lookup(walk, TCL_SOURCED, project->file)) {
            tcl_emit_abstain(ctx, command, "tcl_proc_of_a_file_nothing_sources");
            return;
        }
        cbm_source_emit_node(walk, command, CBM_SOURCE_SITE_CALL, project->qn,
                             CBM_RESOLVE_RULE_IMPORT_BINDING, project->file,
                             project->qn ? "tcl_proc_of_a_sourced_file"
                                         : "tcl_proc_has_no_graph_node");
        return;
    }
    tcl_emit_abstain(ctx, command, "tcl_command_not_a_project_proc");
}

static void tcl_resolve_walk(tcl_ctx_t *ctx, TSNode node, unsigned depth);

static void tcl_resolve_children(tcl_ctx_t *ctx, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                tcl_resolve_walk(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void tcl_resolve_walk(tcl_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "command")) {
        tcl_visit_command(ctx, node);
    } else if (cbm_source_is(node, "namespace")) {
        TSNode body = (TSNode){0};
        const char *inner = tcl_namespace_of(ctx, node, &body);
        if (inner) {
            const char *outer = ctx->namespace_name;
            ctx->namespace_name = inner;
            tcl_resolve_children(ctx, body, depth);
            ctx->namespace_name = outer;
            return;
        }
    }
    tcl_resolve_children(ctx, node, depth);
}

static void tcl_resolve(cbm_source_walk_t *walk) {
    tcl_ctx_t ctx = {.walk = walk, .namespace_name = ""};
    tcl_resolve_walk(&ctx, walk->root, 0);
}

const cbm_source_adapter_t cbm_source_adapter_tcl = {.language = CBM_LANG_TCL,
                                                     .collect = tcl_collect,
                                                     .resolve = tcl_resolve,
                                                     .parse_errors_are_local = true,
                                                     .one_name_one_node = true};
