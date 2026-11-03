/* Shell binding rules, from the Bash manual's "Command Search and Execution".
 *
 * A command name without a slash is looked up as a function first, then as a builtin, then on
 * PATH, so a function a script defines wins over everything but an alias. Functions live in one
 * table per shell process, and a process consists of the script and what it `source`s: another
 * script in the project is another process, however uniquely it names a function.
 *
 * A definition takes effect when it runs. A function body runs later, so it can call what is
 * defined further down; a command at the top level runs now, and only sees definitions above it.
 * `command foo` and `builtin foo` skip the table by design, and need nothing here: the command
 * there is `command`. `unset -f foo` makes the name unknown again, and a command computed from a
 * variable is unknown.
 *
 * `source` reaches another file when its path is spelled literally, or relative to the script's
 * own directory through `$(dirname "$0")`, its BASH_SOURCE spellings, or a variable assigned one
 * of those once (`ROOT=$(dirname "$0")/..`, `source "$ROOT/lib.sh"`). A literal path is relative
 * to the working directory: the script's own directory or the repository root. */
#include "source_adapter_internal.h"
#include <stdio.h>

#define SHELL_FUNCTIONS "F:"
#define SHELL_SOURCES "S:"
#define SHELL_UNSET "U:"
#define SHELL_VARIABLES "V:"
#define SHELL_LOCALS "W:"
#define SHELL_FILES "P:"

enum { SHELL_PATH_BUF = 1024, SHELL_CLOSURE_MAX = 256 };

typedef struct {
    cbm_source_walk_t *walk;
    const char *functions, *sources, *unset, *variables, *locals;
    const char **closure; /* files this script's process consists of, itself first */
    size_t closure_count;
    unsigned function_depth;
} shell_ctx_t;

static const char *shell_directory(cbm_source_walk_t *walk) {
    const char *slash = strrchr(walk->file, '/');
    return slash ? cbm_arena_strndup(walk->arena, walk->file, (size_t)(slash - walk->file)) : "";
}

/* Past `$(dirname "$0")` or its BASH_SOURCE spellings: the script's own directory. */
static const char *shell_after_dirname(const char *text) {
    static const char *const forms[] = {"\"$0\"",
                                        "$0",
                                        "\"${0}\"",
                                        "${0}",
                                        "\"${BASH_SOURCE[0]}\"",
                                        "${BASH_SOURCE[0]}",
                                        "\"${BASH_SOURCE}\"",
                                        "${BASH_SOURCE}",
                                        "\"$BASH_SOURCE\"",
                                        "$BASH_SOURCE"};
    const char *at = strstr(text, "$(dirname ");
    if (!at)
        return NULL;
    at += strlen("$(dirname ");
    for (size_t i = 0; i < sizeof(forms) / sizeof(forms[0]); i++) {
        size_t length = strlen(forms[i]);
        if (!strncmp(at, forms[i], length) && at[length] == ')')
            return at + length + 1;
    }
    return NULL;
}

static size_t shell_path_span(const char *text) {
    return strspn(text, "/._-+abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
}

/* What a path expression names, relative to the script's directory (*own) or to the working
 * directory. It reads the forms scripts use to find themselves: a literal, `$(dirname "$0")/x`,
 * `"$(cd "$(dirname "$0")/.." && pwd)"/x`, and a variable assigned one of those. */
static const char *shell_path_expression(shell_ctx_t *ctx, const char *text, bool *own) {
    cbm_source_walk_t *walk = ctx->walk;
    *own = false;
    const char *after = shell_after_dirname(text);
    if (after) {
        size_t first = shell_path_span(after);
        const char *rest = after + first;
        const char *close = strrchr(rest, ')');
        rest = close ? close + 1 : rest;
        while (*rest == '"')
            rest++;
        size_t second = shell_path_span(rest);
        const char *tail = rest + second;
        while (*tail == '"')
            tail++;
        if (*tail)
            return NULL;
        *own = true;
        const char *joined =
            cbm_arena_sprintf(walk->arena, "%.*s%.*s", (int)first, after, (int)second, rest);
        while (joined && *joined == '/')
            joined++;
        return joined;
    }
    const char *at = text;
    while (*at == '"')
        at++;
    if (*at != '$') {
        if (strpbrk(at, "$`*?"))
            return NULL;
        size_t length = strlen(at);
        while (length && (at[length - 1] == '"' || at[length - 1] == '\''))
            length--;
        if (*at == '\'') {
            at++;
            length--;
        }
        return cbm_arena_strndup(walk->arena, at, length);
    }
    at++;
    bool braced = *at == '{';
    if (braced)
        at++;
    size_t name_size =
        strspn(at, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_");
    if (!name_size || (braced && at[name_size] != '}'))
        return NULL;
    const char *name = cbm_arena_strndup(walk->arena, at, name_size);
    const cbm_source_decl_t *variable = name ? cbm_source_lookup(walk, ctx->variables, name) : NULL;
    if (!variable || (variable->flags & CBM_SOURCE_DECL_F_OPEN) || !variable->type)
        return NULL;
    at += name_size + (braced ? 1 : 0);
    while (*at == '"')
        at++;
    size_t tail_size = shell_path_span(at);
    const char *tail = at + tail_size;
    while (*tail == '"')
        tail++;
    if (*tail)
        return NULL;
    *own = true;
    const char *joined =
        cbm_arena_sprintf(walk->arena, "%s%.*s", variable->type, (int)tail_size, at);
    while (joined && *joined == '/')
        joined++;
    return joined;
}

static const char *shell_command_word(cbm_source_walk_t *walk, TSNode command) {
    TSNode name = cbm_source_field(command, "name");
    if (!cbm_source_is(name, "command_name") || ts_node_named_child_count(name) != 1 ||
        !cbm_source_is(ts_node_named_child(name, 0), "word"))
        return NULL;
    return cbm_source_text(walk, name);
}

/* One chained list per file, under the empty name: a closure walks a file's sources. */
static void shell_note_source(shell_ctx_t *ctx, const char *path) {
    cbm_source_decl_t *decl = cbm_source_declare(ctx->walk, ctx->sources, "", NULL, 0, 0);
    if (decl)
        decl->type = cbm_source_table_intern(ctx->walk, path);
}

static void shell_collect_source(shell_ctx_t *ctx, TSNode command) {
    cbm_source_walk_t *walk = ctx->walk;
    TSNode argument = cbm_source_field(command, "argument");
    const char *text = cbm_source_text(walk, argument);
    bool own = false;
    const char *path = text ? shell_path_expression(ctx, text, &own) : NULL;
    if (!path || !*path || *path == '/')
        return;
    char joined[SHELL_PATH_BUF];
    const char *directory = shell_directory(walk);
    if (directory && cbm_source_join_path(directory, path, joined, sizeof(joined)))
        shell_note_source(ctx, joined);
    if (!own && cbm_source_join_path("", path, joined, sizeof(joined)))
        shell_note_source(ctx, joined);
}

/* `ROOT=$(dirname "$0")/..`: a name for a directory relative to the script. */
static void shell_collect_variable(shell_ctx_t *ctx, TSNode assignment) {
    cbm_source_walk_t *walk = ctx->walk;
    const char *name = cbm_source_text(walk, cbm_source_field(assignment, "name"));
    const char *text = cbm_source_text(walk, cbm_source_field(assignment, "value"));
    if (!name)
        return;
    cbm_source_decl_t *known = (cbm_source_decl_t *)cbm_source_lookup(walk, ctx->variables, name);
    const char *qn = cbm_source_def_qn(walk, name, ts_node_start_point(assignment).row + 1);
    if (known) {
        known->flags |= CBM_SOURCE_DECL_F_OPEN;
        if (!known->qn && qn)
            known->qn = cbm_source_table_intern(walk, qn);
        return;
    }
    bool own = false;
    const char *path = text ? shell_path_expression(ctx, text, &own) : NULL;
    cbm_source_decl_t *decl = cbm_source_declare(walk, ctx->variables, name, qn, 0, 0);
    if (decl && path && own)
        decl->type = cbm_source_table_intern(walk, path);
}

/* `local x`, `declare x`: this name is a function's own somewhere in the file, and a read of it
 * is not decided here. */
static void shell_collect_local(shell_ctx_t *ctx, TSNode node) {
    cbm_source_walk_t *walk = ctx->walk;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        TSNode bound =
            cbm_source_is(child, "variable_assignment") ? cbm_source_field(child, "name") : child;
        if (!cbm_source_is(bound, "variable_name"))
            continue;
        const char *name = cbm_source_text(walk, bound);
        if (name && !cbm_source_lookup(walk, ctx->locals, name))
            (void)cbm_source_declare(walk, ctx->locals, name, NULL, 0, 0);
    }
}

static void shell_collect_walk(shell_ctx_t *ctx, TSNode node, unsigned depth) {
    cbm_source_walk_t *walk = ctx->walk;
    if (!cbm_source_step(walk, depth))
        return;
    if (cbm_source_is(node, "function_definition")) {
        const char *name = cbm_source_text(walk, cbm_source_field(node, "name"));
        const cbm_source_decl_t *first =
            name ? cbm_source_lookup(walk, ctx->functions, name) : NULL;
        if (name && !first) {
            uint32_t line = ts_node_start_point(node).row + 1;
            (void)cbm_source_declare(walk, ctx->functions, name,
                                     cbm_source_def_qn_at(walk, name, line), 0, line);
        }
    } else if (cbm_source_is(node, "command")) {
        const char *word = shell_command_word(walk, node);
        if (word && (!strcmp(word, "source") || !strcmp(word, ".")))
            shell_collect_source(ctx, node);
    } else if (cbm_source_is(node, "declaration_command")) {
        shell_collect_local(ctx, node);
    } else if (cbm_source_is(node, "variable_assignment")) {
        shell_collect_variable(ctx, node);
    } else if (cbm_source_is(node, "unset_command")) {
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *name = cbm_source_text(walk, child);
            if (name && name[0] != '-')
                (void)cbm_source_declare(walk, ctx->unset, name, NULL, 0, 0);
        }
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                shell_collect_walk(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static bool shell_ctx_init(shell_ctx_t *ctx, cbm_source_walk_t *walk) {
    *ctx = (shell_ctx_t){.walk = walk};
    ctx->functions = cbm_arena_sprintf(walk->arena, SHELL_FUNCTIONS "%s", walk->file);
    ctx->sources = cbm_arena_sprintf(walk->arena, SHELL_SOURCES "%s", walk->file);
    ctx->unset = cbm_arena_sprintf(walk->arena, SHELL_UNSET "%s", walk->file);
    ctx->variables = cbm_arena_sprintf(walk->arena, SHELL_VARIABLES "%s", walk->file);
    ctx->locals = cbm_arena_sprintf(walk->arena, SHELL_LOCALS "%s", walk->file);
    if (!ctx->functions || !ctx->sources || !ctx->unset || !ctx->variables || !ctx->locals) {
        cbm_source_fail(walk, "shell_scope_allocation");
        return false;
    }
    return true;
}

static void shell_collect(cbm_source_walk_t *walk) {
    walk->table = walk->writable;
    shell_ctx_t ctx;
    if (!shell_ctx_init(&ctx, walk))
        return;
    (void)cbm_source_declare(walk, SHELL_FILES, walk->file, NULL, 0, 0);
    shell_collect_walk(&ctx, walk->root, 0);
}

static bool shell_in_closure(const shell_ctx_t *ctx, const char *file) {
    for (size_t i = 0; i < ctx->closure_count; i++)
        if (!strcmp(ctx->closure[i], file))
            return true;
    return false;
}

/* Breadth first through literal `source` statements; each file enters once. Past the bound the
 * process is not fully known, and a name no file so far declares stays unknown. */
static bool shell_build_closure(shell_ctx_t *ctx) {
    cbm_source_walk_t *walk = ctx->walk;
    ctx->closure = cbm_arena_calloc(walk->arena, SHELL_CLOSURE_MAX * sizeof(*ctx->closure));
    if (!ctx->closure)
        return false;
    ctx->closure[ctx->closure_count++] = walk->file;
    for (size_t at = 0; at < ctx->closure_count; at++) {
        const char *owner = cbm_arena_sprintf(walk->arena, SHELL_SOURCES "%s", ctx->closure[at]);
        if (!owner)
            return false;
        for (const cbm_source_decl_t *entry = cbm_source_lookup(walk, owner, ""); entry;
             entry = entry->next) {
            if (!entry->type || !cbm_source_lookup(walk, SHELL_FILES, entry->type) ||
                shell_in_closure(ctx, entry->type))
                continue;
            if (ctx->closure_count == SHELL_CLOSURE_MAX)
                return true;
            ctx->closure[ctx->closure_count++] = entry->type;
        }
    }
    return true;
}

static void shell_visit_command(shell_ctx_t *ctx, TSNode command) {
    cbm_source_walk_t *walk = ctx->walk;
    const char *word = shell_command_word(walk, command);
    if (!word) {
        cbm_source_emit_node(walk, command, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                             "shell_command_computed");
        return;
    }
    if (cbm_source_lookup(walk, ctx->unset, word)) {
        cbm_source_emit_node(walk, command, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                             "shell_function_unset_in_this_file");
        return;
    }
    const cbm_source_decl_t *own = cbm_source_lookup(walk, ctx->functions, word);
    if (own) {
        uint32_t line = ts_node_start_point(command).row + 1;
        if (!ctx->function_depth && own->line > line) {
            cbm_source_emit_node(walk, command, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                                 NULL, "shell_function_not_yet_defined");
            return;
        }
        cbm_source_emit_node(walk, command, CBM_SOURCE_SITE_CALL, own->qn,
                             CBM_RESOLVE_RULE_SCOPE_BINDING, ctx->functions,
                             own->qn ? "shell_function_table" : "shell_function_has_no_graph_node");
        return;
    }
    const cbm_source_decl_t *found = NULL;
    for (size_t i = 1; i < ctx->closure_count; i++) {
        const char *owner = cbm_arena_sprintf(walk->arena, SHELL_FUNCTIONS "%s", ctx->closure[i]);
        const cbm_source_decl_t *decl = owner ? cbm_source_lookup(walk, owner, word) : NULL;
        if (!decl)
            continue;
        /* The later `source` wins at runtime, and order across files is not modelled. */
        if (found && (!decl->qn || !found->qn || strcmp(decl->qn, found->qn))) {
            cbm_source_emit_node(walk, command, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                                 NULL, "shell_function_in_two_sourced_files");
            return;
        }
        found = decl;
    }
    if (found) {
        cbm_source_emit_node(walk, command, CBM_SOURCE_SITE_CALL, found->qn,
                             CBM_RESOLVE_RULE_IMPORT_BINDING, found->file,
                             found->qn ? "shell_function_from_sourced_file"
                                       : "shell_function_has_no_graph_node");
        return;
    }
    cbm_source_emit_node(walk, command, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                         "shell_command_not_a_function_of_this_process");
}

/* `$x` and `${x}`: the variable this file assigns, unless a function here makes the name local. */
static void shell_visit_expansion(shell_ctx_t *ctx, TSNode node) {
    cbm_source_walk_t *walk = ctx->walk;
    TSNode name = ts_node_named_child_count(node) ? ts_node_named_child(node, 0) : (TSNode){0};
    if (!cbm_source_is(name, "variable_name"))
        return;
    const char *text = cbm_source_text(walk, name);
    if (!text)
        return;
    const cbm_source_decl_t *variable = cbm_source_lookup(walk, ctx->variables, text);
    bool local = cbm_source_lookup(walk, ctx->locals, text) != NULL;
    const char *target = variable && !local ? variable->qn : NULL;
    cbm_source_emit_node(walk, name, CBM_SOURCE_SITE_REFERENCE, target,
                         CBM_RESOLVE_RULE_SCOPE_BINDING, ctx->variables,
                         target  ? "shell_variable_of_this_file"
                         : local ? "shell_name_is_local_in_this_file"
                                 : "shell_variable_not_assigned_at_the_top_of_this_file");
}

static void shell_resolve_walk(shell_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    bool function = cbm_source_is(node, "function_definition");
    if (cbm_source_is(node, "command"))
        shell_visit_command(ctx, node);
    else if (cbm_source_is(node, "simple_expansion") || cbm_source_is(node, "expansion"))
        shell_visit_expansion(ctx, node);
    if (function)
        ctx->function_depth++;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                shell_resolve_walk(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    if (function)
        ctx->function_depth--;
}

static void shell_resolve(cbm_source_walk_t *walk) {
    shell_ctx_t ctx;
    if (!shell_ctx_init(&ctx, walk) || !shell_build_closure(&ctx))
        return;
    shell_resolve_walk(&ctx, walk->root, 0);
}

const cbm_source_adapter_t cbm_source_adapter_shell = {.language = CBM_LANG_BASH,
                                                       .collect = shell_collect,
                                                       .resolve = shell_resolve,
                                                       .decides_references = true,
                                                       .parse_errors_are_local = true,
                                                       .one_name_one_node = true};
