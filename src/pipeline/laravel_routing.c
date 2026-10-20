/*
 * laravel_routing.c — Laravel 11+ framework route mounts (#1146).
 *
 * See laravel_routing.h for the contract. bootstrap/app.php is parsed with
 * the same PHP grammar the extractor uses; only named `withRouting(...)`
 * arguments whose values are plain literals (or `__DIR__`/`dirname(__DIR__)`
 * concatenations and `base_path()` calls over literals) are trusted.
 */
#include "pipeline/laravel_routing.h"

#include "cbm.h"
#include "lang_specs.h"
#include "service_patterns.h"
#include "tree_sitter/api.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/mem_core.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

enum {
    LR_PATH_MAX = CBM_SZ_512,               /* repo-relative path buffer */
    LR_KEY_MAX = CBM_SZ_32,                 /* named-argument key */
    LR_PREFIX_MAX = CBM_SZ_128,             /* apiPrefix literal */
    LR_SEG_MAX = CBM_SZ_64,                 /* path segments tracked while normalising */
    LR_SOURCE_MAX = CBM_SZ_256 * CBM_SZ_1K, /* bootstrap/app.php read cap */
};

static const char LR_BOOTSTRAP_REL[] = "bootstrap/app.php";

/* Copy a node's source text into buf. False when it does not fit. */
static bool lr_node_text(const char *src, TSNode node, char *buf, size_t buf_sz) {
    if (ts_node_is_null(node)) {
        return false;
    }
    uint32_t start = ts_node_start_byte(node);
    uint32_t end = ts_node_end_byte(node);
    size_t len = end > start ? (size_t)(end - start) : 0;
    if (len >= buf_sz) {
        return false;
    }
    memcpy(buf, src + start, len);
    buf[len] = '\0';
    return true;
}

static bool lr_node_is(TSNode node, const char *type) {
    return !ts_node_is_null(node) && strcmp(ts_node_type(node), type) == 0;
}

static bool lr_node_text_is(const char *src, TSNode node, const char *text) {
    char buf[LR_KEY_MAX];
    return lr_node_text(src, node, buf, sizeof(buf)) && strcmp(buf, text) == 0;
}

/* A plain string literal: '...' or "..." whose only children are
 * string_content (no interpolation, no escapes). Writes the unquoted value. */
static bool lr_string_literal(const char *src, TSNode node, char *buf, size_t buf_sz) {
    if (!lr_node_is(node, "string") && !lr_node_is(node, "encapsed_string")) {
        return false;
    }
    size_t pos = 0;
    buf[0] = '\0';
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode part = ts_node_named_child(node, i);
        if (!lr_node_is(part, "string_content") ||
            !lr_node_text(src, part, buf + pos, buf_sz - pos)) {
            return false;
        }
        pos += strlen(buf + pos);
    }
    return true;
}

/* The value expression of a call argument: its first named child that is
 * neither the `name:` label nor a comment. */
static TSNode lr_arg_value(TSNode arg, TSNode label) {
    uint32_t nc = ts_node_named_child_count(arg);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_named_child(arg, i);
        if ((ts_node_is_null(label) || !ts_node_eq(child, label)) &&
            !lr_node_is(child, "comment")) {
            return child;
        }
    }
    TSNode none = {0};
    return none;
}

/* The single literal argument of `fn('...')` when the callee is `fn`. */
static bool lr_call_literal_arg(const char *src, TSNode call, const char *fn, char *buf,
                                size_t buf_sz) {
    if (!lr_node_is(call, "function_call_expression") ||
        !lr_node_text_is(src, ts_node_child_by_field_name(call, TS_FIELD("function")), fn)) {
        return false;
    }
    TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
    if (ts_node_is_null(args) || ts_node_named_child_count(args) != SKIP_ONE) {
        return false;
    }
    TSNode arg = ts_node_named_child(args, 0);
    TSNode val = lr_arg_value(arg, ts_node_child_by_field_name(arg, TS_FIELD("name")));
    return lr_string_literal(src, val, buf, buf_sz);
}

/* `dirname(__DIR__)` — the application base directory. */
static bool lr_is_dirname_dir(const char *src, TSNode node) {
    if (!lr_node_is(node, "function_call_expression") ||
        !lr_node_text_is(src, ts_node_child_by_field_name(node, TS_FIELD("function")), "dirname")) {
        return false;
    }
    TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
    if (ts_node_is_null(args) || ts_node_named_child_count(args) != SKIP_ONE) {
        return false;
    }
    TSNode arg = ts_node_named_child(args, 0);
    TSNode val = lr_arg_value(arg, ts_node_child_by_field_name(arg, TS_FIELD("name")));
    return lr_node_is(val, "name") && lr_node_text_is(src, val, "__DIR__");
}

/* Resolve "." and ".." segments of a '/'-joined path in place. False when the
 * path climbs above the repository root. */
static bool lr_normalize(char *path) {
    size_t seg_start[LR_SEG_MAX];
    int depth = 0;
    size_t out = 0;
    const char *p = path;
    while (*p) {
        const char *slash = strchr(p, '/');
        size_t len = slash ? (size_t)(slash - p) : strlen(p);
        if (len == PAIR_LEN && p[0] == '.' && p[SKIP_ONE] == '.') {
            if (depth == 0) {
                return false;
            }
            out = seg_start[--depth];
        } else if (len > 0 && !(len == SKIP_ONE && p[0] == '.')) {
            if (depth == LR_SEG_MAX) {
                return false;
            }
            size_t at = out > 0 ? out + SKIP_ONE : 0;
            seg_start[depth++] = out;
            if (out > 0) {
                path[out] = '/';
            }
            memmove(path + at, p, len);
            out = at + len;
        }
        p += len + (slash ? SKIP_ONE : 0);
    }
    path[out] = '\0';
    return true;
}

/* Resolve one `api:` path expression of the app rooted at app_dir to a
 * normalised repo-relative path:
 *   __DIR__ . '/../routes/api.php'           (relative to bootstrap/)
 *   dirname(__DIR__) . '/routes/api.php'     (relative to the app base)
 *   base_path('routes/api.php')              (relative to the app base) */
static bool lr_route_file_path(const char *src, TSNode expr, const char *app_dir, char *out,
                               size_t out_sz) {
    char lit[LR_PATH_MAX];
    const char *base_suffix = NULL;
    if (lr_node_is(expr, "binary_expression")) {
        TSNode op = ts_node_child_by_field_name(expr, TS_FIELD("operator"));
        TSNode left = ts_node_child_by_field_name(expr, TS_FIELD("left"));
        if (!lr_node_text_is(src, op, ".") ||
            !lr_string_literal(src, ts_node_child_by_field_name(expr, TS_FIELD("right")), lit,
                               sizeof(lit))) {
            return false;
        }
        if (lr_node_is(left, "name") && lr_node_text_is(src, left, "__DIR__")) {
            base_suffix = "bootstrap";
        } else if (lr_is_dirname_dir(src, left)) {
            base_suffix = "";
        } else {
            return false;
        }
    } else if (lr_call_literal_arg(src, expr, "base_path", lit, sizeof(lit))) {
        base_suffix = "";
    } else {
        return false;
    }
    int n = snprintf(out, out_sz, "%s/%s/%s", app_dir, base_suffix, lit);
    return n > 0 && (size_t)n < out_sz && lr_normalize(out);
}

/* Does the `api:` value (one path or an array of paths) name rel_path? */
static bool lr_api_names_file(const char *src, TSNode value, const char *app_dir,
                              const char *rel_path) {
    char resolved[LR_PATH_MAX];
    if (!lr_node_is(value, "array_creation_expression")) {
        return lr_route_file_path(src, value, app_dir, resolved, sizeof(resolved)) &&
               strcmp(resolved, rel_path) == 0;
    }
    uint32_t nc = ts_node_named_child_count(value);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode elem = ts_node_named_child(value, i);
        uint32_t ec = ts_node_named_child_count(elem);
        if (!lr_node_is(elem, "array_element_initializer") || ec == 0) {
            continue;
        }
        TSNode path_expr = ts_node_named_child(elem, ec - SKIP_ONE); /* value of `k => v` too */
        if (lr_route_file_path(src, path_expr, app_dir, resolved, sizeof(resolved)) &&
            strcmp(resolved, rel_path) == 0) {
            return true;
        }
    }
    return false;
}

/* Evaluate the named arguments of a withRouting(...) call. */
static bool lr_eval_with_routing(const char *src, TSNode call, const char *app_dir,
                                 const char *rel_path, char *out, size_t out_sz) {
    TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
    char prefix[LR_PREFIX_MAX] = "api"; /* ApplicationBuilder::withRouting default */
    bool prefix_known = true;
    bool mounted = false;
    uint32_t nc = ts_node_is_null(args) ? 0 : ts_node_named_child_count(args);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode arg = ts_node_named_child(args, i);
        if (lr_node_is(arg, "comment")) {
            continue;
        }
        TSNode label = ts_node_child_by_field_name(arg, TS_FIELD("name"));
        char key[LR_KEY_MAX];
        /* Positional arguments would need the parameter order modelled, and a
         * `using:` callback replaces the web/api registration entirely. */
        if (!lr_node_is(arg, "argument") || ts_node_is_null(label) ||
            !lr_node_text(src, label, key, sizeof(key)) || strcmp(key, "using") == 0) {
            return false;
        }
        TSNode value = lr_arg_value(arg, label);
        if (strcmp(key, "api") == 0) {
            mounted = lr_api_names_file(src, value, app_dir, rel_path);
        } else if (strcmp(key, "apiPrefix") == 0) {
            prefix_known = lr_string_literal(src, value, prefix, sizeof(prefix));
        }
    }
    if (!mounted || !prefix_known) {
        return false;
    }
    /* Laravel trims the group prefix of '/' on both ends. */
    const char *start = prefix;
    size_t len = strlen(prefix);
    while (*start == '/') {
        start++;
        len--;
    }
    while (len > 0 && start[len - SKIP_ONE] == '/') {
        len--;
    }
    int n = snprintf(out, out_sz, "/%.*s", (int)len, start);
    return len > 0 && n > 0 && (size_t)n < out_sz;
}

/* First `->withRouting(...)` call in the tree (iterative pre-order walk). */
static TSNode lr_find_with_routing(const char *src, TSNode root) {
    TSNode found = {0};
    TSTreeCursor cur = ts_tree_cursor_new(root);
    bool more = true;
    while (more) {
        TSNode node = ts_tree_cursor_current_node(&cur);
        if ((lr_node_is(node, "member_call_expression") ||
             lr_node_is(node, "nullsafe_member_call_expression")) &&
            lr_node_text_is(src, ts_node_child_by_field_name(node, TS_FIELD("name")),
                            "withRouting")) {
            found = node;
            break;
        }
        if (ts_tree_cursor_goto_first_child(&cur)) {
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cur)) {
            if (!ts_tree_cursor_goto_parent(&cur)) {
                more = false;
                break;
            }
        }
    }
    ts_tree_cursor_delete(&cur);
    return found;
}

static bool lr_mount_from_bootstrap(const char *src, uint32_t len, const char *app_dir,
                                    const char *rel_path, char *out, size_t out_sz) {
    const TSLanguage *php = cbm_ts_language(CBM_LANG_PHP);
    TSParser *parser = php ? ts_parser_new() : NULL;
    if (!parser) {
        return false;
    }
    bool mounted = false;
    TSTree *tree =
        ts_parser_set_language(parser, php) ? ts_parser_parse_string(parser, NULL, src, len) : NULL;
    if (tree) {
        TSNode call = lr_find_with_routing(src, ts_tree_root_node(tree));
        mounted = !ts_node_is_null(call) &&
                  lr_eval_with_routing(src, call, app_dir, rel_path, out, out_sz);
        ts_tree_delete(tree);
    }
    ts_parser_delete(parser);
    return mounted;
}

/* Read <repo>/<app_dir>/bootstrap/app.php. NULL when absent or unreadable. */
static char *lr_read_bootstrap(const char *repo_path, const char *app_dir, uint32_t *out_len) {
    char path[CBM_SZ_4K];
    int n = snprintf(path, sizeof(path), "%s/%s%s%s", repo_path, app_dir, app_dir[0] ? "/" : "",
                     LR_BOOTSTRAP_REL);
    if (n <= 0 || (size_t)n >= sizeof(path)) {
        return NULL;
    }
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    char *buf = (size > 0 && size <= (long)LR_SOURCE_MAX)
                    ? cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)size + SKIP_ONE)
                    : NULL;
    if (buf) {
        size_t nread = fread(buf, SKIP_ONE, (size_t)size, f);
        buf[nread] = '\0';
        *out_len = (uint32_t)nread;
    }
    (void)fclose(f);
    return buf;
}

bool cbm_laravel_api_mount(const char *repo_path, const char *rel_path, char *out, size_t out_sz) {
    if (!out || out_sz == 0) {
        return false;
    }
    out[0] = '\0';
    size_t rel_len = rel_path ? strlen(rel_path) : 0;
    if (!repo_path || rel_len < sizeof(".php") || rel_len >= LR_PATH_MAX ||
        strcmp(rel_path + rel_len - (sizeof(".php") - SKIP_ONE), ".php") != 0) {
        return false;
    }
    /* Walk the file's ancestor directories nearest-first; the nearest one
     * holding bootstrap/app.php is the Laravel app that owns the file. */
    char app_dir[LR_PATH_MAX];
    memcpy(app_dir, rel_path, rel_len + SKIP_ONE);
    char *slash = strrchr(app_dir, '/');
    for (;;) {
        if (slash) {
            *slash = '\0';
        } else {
            app_dir[0] = '\0';
        }
        uint32_t len = 0;
        char *src = lr_read_bootstrap(repo_path, app_dir, &len);
        if (src) {
            bool mounted = lr_mount_from_bootstrap(src, len, app_dir, rel_path, out, out_sz);
            cbm_free(CBM_MEM_CLASS_EXTRACT, src);
            if (!mounted) {
                out[0] = '\0';
            }
            return mounted;
        }
        if (!app_dir[0]) {
            return false;
        }
        slash = strrchr(app_dir, '/');
    }
}

void cbm_laravel_file_route_mount(const char *repo_path, const char *rel_path, CBMLanguage lang,
                                  const CBMFileResult *result, char *out, size_t out_sz) {
    if (!out || out_sz == 0) {
        return;
    }
    out[0] = '\0';
    if (lang != CBM_LANG_PHP || !result) {
        return;
    }
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (call->callee_name && call->first_string_arg && call->first_string_arg[0] == '/' &&
            cbm_service_pattern_route_method(call->callee_name) != NULL) {
            (void)cbm_laravel_api_mount(repo_path, rel_path, out, out_sz);
            return;
        }
    }
}

const char *cbm_laravel_mount_route(const char *mount, const char *path, char *buf, size_t buf_sz) {
    if (!mount || !mount[0] || !path || !buf || buf_sz == 0) {
        return path;
    }
    const char *rel = path;
    while (*rel == '/') {
        rel++;
    }
    int n =
        rel[0] ? snprintf(buf, buf_sz, "%s/%s", mount, rel) : snprintf(buf, buf_sz, "%s", mount);
    return (n > 0 && (size_t)n < buf_sz) ? buf : path;
}
