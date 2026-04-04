/*
 * pass_compile_commands.c — compile_commands.json parsing helpers.
 *
 * Parses compile_commands.json to extract per-file include paths, defines,
 * and C/C++ standard flags.
 */
#include "foundation/constants.h"

enum { CC_FLAG_IDX = 1, CC_FLAG_SKIP = 2 };

#define SLEN(s) (sizeof(s) - 1)
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "yyjson/yyjson.h"

/* Emit the current token if non-empty. Returns updated count. */
static int emit_token(char *current, int *clen, char **out, int count, int max_out) {
    if (*clen > 0 && count < max_out) {
        current[*clen] = '\0';
        out[count++] = strdup(current);
        *clen = 0;
    }
    return count;
}

int cbm_split_command(const char *cmd, char **out, int max_out) {
    if (!cmd || !out || max_out <= 0) {
        return 0;
    }

    int count = 0;
    char current[CBM_SZ_4K];
    int clen = 0;
    char in_quote = 0;

    for (int i = 0; cmd[i]; i++) {
        char c = cmd[i];
        if (in_quote) {
            if (c == in_quote) {
                in_quote = 0;
            } else if (clen < (int)sizeof(current) - SKIP_ONE) {
                current[clen++] = c;
            }
        } else if (c == '"' || c == '\'') {
            in_quote = c;
        } else if (c == ' ' || c == '\t') {
            count = emit_token(current, &clen, out, count, max_out);
        } else if (clen < (int)sizeof(current) - SKIP_ONE) {
            current[clen++] = c;
        }
    }
    return emit_token(current, &clen, out, count, max_out);
}

/* Resolve a path: if relative, join with directory. */
static char *resolve_path(const char *path, const char *directory) {
    if (!path) {
        return NULL;
    }

    /* Absolute path */
    if (path[0] == '/') {
        return strdup(path);
    }

    /* Relative — join with directory */
    if (directory && directory[0]) {
        char buf[CBM_SZ_4K];
        snprintf(buf, sizeof(buf), "%s/%s", directory, path);
        return strdup(buf);
    }

    return strdup(path);
}

/* Try to consume a -I or -isystem include path flag. Returns true if consumed. */
static bool try_include_flag(cbm_compile_flags_t *f, const char **args, int argc, int *i,
                             const char *directory) {
    const char *arg = args[*i];
    if (arg[0] == '-' && arg[CC_FLAG_IDX] == 'I') {
        const char *path = arg + CC_FLAG_SKIP;
        if (*path == '\0' && *i + SKIP_ONE < argc) {
            (*i)++;
            path = args[*i];
        }
        if (path && *path) {
            f->include_paths[f->include_count++] = resolve_path(path, directory);
        }
        return true;
    }
    if (strcmp(arg, "-isystem") == 0 && *i + SKIP_ONE < argc) {
        (*i)++;
        f->include_paths[f->include_count++] = resolve_path(args[*i], directory);
        return true;
    }
    return false;
}

/* Try to consume a -D define flag. Returns true if consumed. */
static bool try_define_flag(cbm_compile_flags_t *f, const char **args, int argc, int *i) {
    const char *arg = args[*i];
    if (arg[0] != '-' || arg[CC_FLAG_IDX] != 'D') {
        return false;
    }
    const char *define = arg + CC_FLAG_SKIP;
    if (*define == '\0' && *i + SKIP_ONE < argc) {
        (*i)++;
        define = args[*i];
    }
    if (define && *define) {
        f->defines[f->define_count++] = strdup(define);
    }
    return true;
}

cbm_compile_flags_t *cbm_extract_flags(const char **args, int argc, const char *directory) {
    cbm_compile_flags_t *f = calloc(CBM_ALLOC_ONE, sizeof(*f));
    if (!f) {
        return NULL;
    }
    f->include_paths = calloc(argc, sizeof(char *));
    f->defines = calloc(argc, sizeof(char *));

    for (int i = 0; i < argc; i++) {
        if (try_include_flag(f, args, argc, &i, directory)) {
            continue;
        }
        if (try_define_flag(f, args, argc, &i)) {
            continue;
        }
        if (strncmp(args[i], "-std=", SLEN("-std=")) == 0) {
            snprintf(f->standard, sizeof(f->standard), "%s", args[i] + 5);
        }
    }
    return f;
}

void cbm_compile_flags_free(cbm_compile_flags_t *f) {
    if (!f) {
        return;
    }
    for (int i = 0; i < f->include_count; i++) {
        free(f->include_paths[i]);
    }
    free(f->include_paths);
    for (int i = 0; i < f->define_count; i++) {
        free(f->defines[i]);
    }
    free(f->defines);
    free(f);
}

/* Extract compiler flag args from either "arguments" array or "command" string. */
static int extract_flag_args(yyjson_val *args_val, yyjson_val *cmd_val, const char **flag_args,
                             char **split_args, int max_args) {
    if (args_val && yyjson_is_arr(args_val)) {
        int n = 0;
        yyjson_val *a;
        yyjson_arr_iter aiter;
        yyjson_arr_iter_init(args_val, &aiter);
        while ((a = yyjson_arr_iter_next(&aiter)) && n < max_args) {
            const char *s = yyjson_get_str(a);
            if (s) {
                flag_args[n++] = s;
            }
        }
        return n;
    }
    if (cmd_val && yyjson_is_str(cmd_val)) {
        int n = cbm_split_command(yyjson_get_str(cmd_val), split_args, max_args);
        for (int j = 0; j < n; j++) {
            flag_args[j] = split_args[j];
        }
        return n;
    }
    return 0;
}

/* Process a single compile_commands.json entry. Returns 1 if added, 0 otherwise. */
static int process_compile_entry(yyjson_val *entry, const char *repo_path, char **out_path,
                                 cbm_compile_flags_t **out_flag) {
    yyjson_val *dir_val = yyjson_obj_get(entry, "directory");
    yyjson_val *file_val = yyjson_obj_get(entry, "file");
    yyjson_val *cmd_val = yyjson_obj_get(entry, "command");
    yyjson_val *args_val = yyjson_obj_get(entry, "arguments");

    if (!file_val) {
        return 0;
    }
    const char *directory = dir_val ? yyjson_get_str(dir_val) : "";
    const char *file_path = yyjson_get_str(file_val);
    if (!file_path) {
        return 0;
    }

    char *split_args[CBM_SZ_256] = {NULL};
    const char *flag_args[CBM_SZ_256];
    int flag_argc = extract_flag_args(args_val, cmd_val, flag_args, split_args, CBM_SZ_256);

    if (flag_argc == 0) {
        return 0;
    }

    cbm_compile_flags_t *f = cbm_extract_flags(flag_args, flag_argc, directory);

    if (cmd_val && yyjson_is_str(cmd_val)) {
        for (int j = 0; j < flag_argc; j++) {
            free(split_args[j]);
        }
    }

    if (!f) {
        return 0;
    }

    char abs_path[CBM_SZ_4K];
    if (file_path[0] != '/' && directory && directory[0]) {
        snprintf(abs_path, sizeof(abs_path), "%s/%s", directory, file_path);
    } else {
        snprintf(abs_path, sizeof(abs_path), "%s", file_path);
    }

    size_t repo_len = strlen(repo_path);
    if (strncmp(abs_path, repo_path, repo_len) != 0 || abs_path[repo_len] != '/') {
        cbm_compile_flags_free(f);
        return 0;
    }

    *out_path = strdup(abs_path + repo_len + SKIP_ONE);
    *out_flag = f;
    return SKIP_ONE;
}

int cbm_parse_compile_commands(const char *json_data, const char *repo_path, char ***out_paths,
                               cbm_compile_flags_t ***out_flags) {
    if (!json_data || !repo_path || !out_paths || !out_flags) {
        return CBM_NOT_FOUND;
    }
    *out_paths = NULL;
    *out_flags = NULL;

    yyjson_doc *doc = yyjson_read(json_data, strlen(json_data), 0);
    if (!doc) {
        return CBM_NOT_FOUND;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_arr(root)) {
        yyjson_doc_free(doc);
        return CBM_NOT_FOUND;
    }

    int arr_len = (int)yyjson_arr_size(root);
    if (arr_len == 0) {
        yyjson_doc_free(doc);
        return 0;
    }

    char **paths = calloc(arr_len, sizeof(char *));
    cbm_compile_flags_t **flags = calloc(arr_len, sizeof(cbm_compile_flags_t *));
    int count = 0;

    yyjson_val *entry;
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(root, &iter);

    while ((entry = yyjson_arr_iter_next(&iter))) {
        char *p = NULL;
        cbm_compile_flags_t *f = NULL;
        if (process_compile_entry(entry, repo_path, &p, &f)) {
            paths[count] = p;
            flags[count] = f;
            count++;
        }
    }

    yyjson_doc_free(doc);
    *out_paths = paths;
    *out_flags = flags;
    return count;
}
