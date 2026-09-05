#include "git/git_context.h"

#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/str_util.h"

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

enum {
    GIT_CMD_MAX = 1024,
    GIT_OUTPUT_MAX = 4096,
};

/* Bounded worktree-status stream constants.
 * STATUS_FIELD_MAX is the largest single NUL-delimited record field we
 * buffer; a field at/over the bound is treated as a malformed stream and the
 * whole snapshot fails closed. PATH components of real repos are far below
 * this even with long paths. */
enum {
    STATUS_HEADER_LEN = 3, /* porcelain-v1 record prefix "XY " */
    STATUS_CHUNK_MAX = CBM_SZ_4K,
    STATUS_FIELD_MAX = CBM_SZ_16K,
};

static char *git_strdup(const char *s) {
    if (!s) {
        s = "";
    }
    size_t n = strlen(s) + 1;
    char *out = (char *)malloc(n);
    if (!out) {
        return NULL;
    }
    memcpy(out, s, n);
    return out;
}

static void trim_newlines(char *s) {
    if (!s) {
        return;
    }
    size_t n = strlen(s);
    while (n > 0 && (s[n - 1] == '\n' || s[n - 1] == '\r')) {
        s[--n] = '\0';
    }
}

static bool git_validate_repo_path(const char *repo_path) {
    return cbm_validate_shell_path_arg(repo_path);
}

static int git_capture(const char *repo_path, const char *git_args, char **out) {
    if (!out) {
        return CBM_NOT_FOUND;
    }
    *out = NULL;
    if (!repo_path || !git_args || !git_validate_repo_path(repo_path)) {
        return CBM_NOT_FOUND;
    }

    char cmd[GIT_CMD_MAX];
#ifdef _WIN32
    const char *null_dev = "NUL";
#else
    const char *null_dev = "/dev/null";
#endif
    /* Double quotes work for POSIX shells and cmd.exe. cbm_validate_shell_arg()
     * rejects quote/backslash/substitution metacharacters before interpolation. */
    int n = snprintf(cmd, sizeof(cmd), "git -C \"%s\" %s 2>%s", repo_path, git_args, null_dev);
    if (n < 0 || n >= (int)sizeof(cmd)) {
        return CBM_NOT_FOUND;
    }

    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return CBM_NOT_FOUND;
    }

    char buf[GIT_OUTPUT_MAX];
    if (!fgets(buf, sizeof(buf), fp)) {
        cbm_pclose(fp);
        return CBM_NOT_FOUND;
    }
    trim_newlines(buf);

    int rc = cbm_pclose(fp);
    if (rc != 0 || buf[0] == '\0') {
        return CBM_NOT_FOUND;
    }

    *out = git_strdup(buf);
    return *out ? 0 : CBM_NOT_FOUND;
}

static bool path_is_absolute(const char *path) {
    if (!path || !path[0]) {
        return false;
    }
    if (path[0] == '/') {
        return true;
    }
#ifdef _WIN32
    return isalpha((unsigned char)path[0]) && path[1] == ':';
#else
    return false;
#endif
}

static char *join_root_relative(const char *root, const char *rel) {
    if (!root || !root[0]) {
        return git_strdup(rel);
    }
    int n = snprintf(NULL, 0, "%s/%s", root, rel);
    if (n < 0) {
        return NULL;
    }
    char *out = (char *)malloc((size_t)n + 1);
    if (!out) {
        return NULL;
    }
    snprintf(out, (size_t)n + 1, "%s/%s", root, rel);
    return out;
}

/* Derive the canonical repo root.
 *
 * Preferred (git 2.31+): abs_common_dir is `git rev-parse --path-format=absolute
 * --git-common-dir` — git's OWN absolute, canonical common-dir. Because a main
 * repo and its linked worktree both ask the same git binary, they resolve to the
 * IDENTICAL path, so canonical_root is consistent across worktrees AND platforms
 * with no manual join or realpath/_fullpath — which diverged under msys vs native
 * path representations (#659 root cause, and the worktree==main-root breakage in
 * test_pipeline.c git_context_linked_worktree). git also resolves the relative
 * ".." internally, so the subdirectory case (#659) is fixed here too.
 *
 * Fallback (git < 2.31, no --path-format → abs_common_dir empty): the relative
 * --git-common-dir is relative to input_path (the -C dir), so join against it and
 * realpath-normalize the "..". Unix only — on Windows git emits an absolute
 * common-dir so this branch isn't reached in practice, and _fullpath there
 * reintroduces the msys divergence. */
static char *derive_canonical_root(const char *input_path, const char *worktree_root,
                                   const char *git_common_dir, const char *abs_common_dir) {
    char *root = NULL;
    if (abs_common_dir && abs_common_dir[0] && path_is_absolute(abs_common_dir)) {
        root = git_strdup(abs_common_dir);
        if (!root) {
            return NULL;
        }
    } else {
        const char *src = git_common_dir && git_common_dir[0] ? git_common_dir : worktree_root;
        if (!src) {
            return git_strdup("");
        }
#ifndef _WIN32
        root = path_is_absolute(src) ? git_strdup(src) : join_root_relative(input_path, src);
        if (!root) {
            return NULL;
        }
        {
            char resolved[4096];
            if (realpath(root, resolved) != NULL) {
                free(root);
                root = git_strdup(resolved);
                if (!root) {
                    return NULL;
                }
            }
        }
#else
        (void)input_path;
        root = path_is_absolute(src) ? git_strdup(src) : join_root_relative(worktree_root, src);
        if (!root) {
            return NULL;
        }
#endif
    }

    size_t len = strlen(root);
    while (len > 1 && (root[len - 1] == '/' || root[len - 1] == '\\')) {
        root[--len] = '\0';
    }

    if (len >= 5 && strcmp(root + len - 5, "/.git") == 0) {
        root[len - 5] = '\0';
    }
#ifdef _WIN32
    else if (len >= 5 && strcmp(root + len - 5, "\\.git") == 0) {
        root[len - 5] = '\0';
    }
#endif

    return root;
}

static char *slug_from_branch(const char *branch, bool detached) {
    const char *fallback = detached ? "detached" : "working-tree";
    const char *src = detached ? fallback : (branch && branch[0] ? branch : fallback);
    size_t len = strlen(src);
    char *slug = (char *)malloc(len + 1);
    if (!slug) {
        return NULL;
    }

    size_t j = 0;
    bool in_dash = false;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)src[i];
        if (isalnum(c) || c == '-' || c == '_' || c == '.') {
            if (j == 0 && c == '-') {
                in_dash = true;
                continue;
            }
            slug[j++] = (char)c;
            in_dash = false;
        } else if (j > 0 && !in_dash) {
            slug[j++] = '-';
            in_dash = true;
        }
    }
    while (j > 0 && slug[j - 1] == '-') {
        j--;
    }
    slug[j] = '\0';

    if (slug[0] == '\0') {
        free(slug);
        return git_strdup(fallback);
    }
    return slug;
}

void cbm_git_context_free(cbm_git_context_t *ctx) {
    if (!ctx) {
        return;
    }
    free(ctx->input_path);
    free(ctx->worktree_root);
    free(ctx->git_dir);
    free(ctx->git_common_dir);
    free(ctx->canonical_root);
    free(ctx->branch);
    free(ctx->branch_slug);
    free(ctx->head_sha);
    free(ctx->base_sha);
    memset(ctx, 0, sizeof(*ctx));
}

int cbm_git_context_resolve(const char *path, cbm_git_context_t *out) {
    if (!out) {
        return CBM_NOT_FOUND;
    }

    memset(out, 0, sizeof(*out));
    if (!path || !path[0]) {
        return CBM_NOT_FOUND;
    }

    out->input_path = git_strdup(path);
    if (!out->input_path) {
        return CBM_NOT_FOUND;
    }

    struct stat st;
    out->root_exists = (stat(path, &st) == 0);
    if (!out->root_exists) {
        return 0;
    }

    if (git_capture(path, "rev-parse --show-toplevel", &out->worktree_root) != 0) {
        out->is_git = false;
        return 0;
    }
    out->is_git = true;

    if (git_capture(path, "rev-parse --git-dir", &out->git_dir) != 0) {
        out->git_dir = git_strdup("");
    }
    if (git_capture(path, "rev-parse --git-common-dir", &out->git_common_dir) != 0) {
        out->git_common_dir = git_strdup("");
    }
    if (git_capture(path, "rev-parse --verify HEAD", &out->head_sha) != 0) {
        out->head_sha = git_strdup("");
    }

    if (git_capture(path, "symbolic-ref --quiet --short HEAD", &out->branch) != 0) {
        out->branch = git_strdup("DETACHED");
        out->is_detached = true;
    }

    out->is_worktree =
        out->git_dir && out->git_common_dir && strcmp(out->git_dir, out->git_common_dir) != 0;
    /* git 2.31+ canonical absolute common-dir (best-effort; NULL on older git,
     * where derive_canonical_root falls back to the relative common-dir). */
    char *abs_common_dir = NULL;
    (void)git_capture(path, "rev-parse --path-format=absolute --git-common-dir", &abs_common_dir);
    out->canonical_root =
        derive_canonical_root(path, out->worktree_root, out->git_common_dir, abs_common_dir);
    free(abs_common_dir);
    out->branch_slug = slug_from_branch(out->branch, out->is_detached);
    if (git_capture(path, "merge-base HEAD @{upstream}", &out->base_sha) != 0) {
        out->base_sha = git_strdup("");
    }

    if (!out->git_dir || !out->git_common_dir || !out->head_sha || !out->branch ||
        !out->canonical_root || !out->branch_slug || !out->base_sha) {
        cbm_git_context_free(out);
        return CBM_NOT_FOUND;
    }

    return 0;
}

char *cbm_git_context_branch_qn(const char *project_name, const cbm_git_context_t *ctx) {
    const char *project = project_name && project_name[0] ? project_name : "project";
    const char *slug = "working-tree";
    if (ctx) {
        if (ctx->is_detached) {
            slug = "detached";
        } else if (ctx->is_git && ctx->branch_slug && ctx->branch_slug[0]) {
            slug = ctx->branch_slug;
        }
    }

    int n = snprintf(NULL, 0, "%s.__branch__.%s", project, slug);
    if (n < 0) {
        return NULL;
    }
    char *out = (char *)malloc((size_t)n + 1);
    if (!out) {
        return NULL;
    }
    snprintf(out, (size_t)n + 1, "%s.__branch__.%s", project, slug);
    return out;
}

static bool append_fmt_checked(char *buf, int buf_size, int *off, const char *fmt, ...) {
    if (!buf || !off || buf_size <= 0 || *off < 0 || *off >= buf_size) {
        return false;
    }

    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf + *off, (size_t)(buf_size - *off), fmt, ap);
    va_end(ap);
    if (n < 0 || n >= buf_size - *off) {
        buf[buf_size - 1] = '\0';
        return false;
    }
    *off += n;
    return true;
}

static int json_escaped_len(const char *src) {
    if (!src) {
        return 0;
    }
    int len = 0;
    for (int i = 0; src[i]; i++) {
        unsigned char c = (unsigned char)src[i];
        if (c == '"' || c == '\\' || c == '\n' || c == '\r' || c == '\t') {
            len += 2;
        } else if (c < 0x20) {
            len += 6; /* \u00XX */
        } else {
            len++;
        }
    }
    return len;
}

static bool json_append_bool(char *buf, int buf_size, int *off, const char *name, bool value,
                             bool comma) {
    return append_fmt_checked(buf, buf_size, off, "\"%s\":%s%s", name, value ? "true" : "false",
                              comma ? "," : "");
}

static bool json_append_string(char *buf, int buf_size, int *off, const char *name,
                               const char *value, bool comma) {
    int needed = json_escaped_len(value ? value : "");
    char *escaped = malloc((size_t)needed + 1);
    if (!escaped) {
        return false;
    }
    int actual = cbm_json_escape(escaped, needed + 1, value ? value : "");
    bool ok = actual == needed && append_fmt_checked(buf, buf_size, off, "\"%s\":\"%s\"%s", name,
                                                     escaped, comma ? "," : "");
    free(escaped);
    return ok;
}

int cbm_git_context_props_json(const cbm_git_context_t *ctx, char *buf, int buf_size) {
    if (!ctx || !buf || buf_size <= 0) {
        return 0;
    }

    int off = 0;
    bool ok =
        append_fmt_checked(buf, buf_size, &off, "{") &&
        json_append_bool(buf, buf_size, &off, "is_git", ctx->is_git, true) &&
        json_append_bool(buf, buf_size, &off, "is_worktree", ctx->is_worktree, true) &&
        json_append_bool(buf, buf_size, &off, "is_detached", ctx->is_detached, true) &&
        json_append_bool(buf, buf_size, &off, "root_exists", ctx->root_exists, true) &&
        json_append_string(buf, buf_size, &off, "canonical_root", ctx->canonical_root, true) &&
        json_append_string(buf, buf_size, &off, "worktree_root", ctx->worktree_root, true) &&
        json_append_string(buf, buf_size, &off, "git_common_dir", ctx->git_common_dir, true) &&
        json_append_string(buf, buf_size, &off, "branch", ctx->branch, true) &&
        json_append_string(buf, buf_size, &off, "head_sha", ctx->head_sha, true) &&
        json_append_string(buf, buf_size, &off, "base_sha", ctx->base_sha, false) &&
        append_fmt_checked(buf, buf_size, &off, "}");
    if (!ok) {
        if (buf_size > 0) {
            buf[0] = '\0';
        }
        return 0;
    }
    return off;
}

/* ── Bounded worktree status ──────────────────────────────────────────────
 * git status --porcelain=v1 -z --untracked-files=all is parsed record by
 * record, NUL-delimited, over a bounded chunk buffer. The full stream is
 * counted but only max_samples paths per class (tracked / untracked) are
 * ever retained, so a huge dirty tree cannot grow memory. Rename/copy
 * records carry a second NUL-terminated path (the source); it is consumed
 * without being counted so one rename counts once. */

typedef struct {
    cbm_worktree_status_t *out;
    int max_samples;
    bool expecting_source; /* next field is the source path of an R/C record */
    bool parse_ok;
    char partial[STATUS_FIELD_MAX];
    size_t partial_len;
} status_parser_t;

/* porcelain-v1 status columns (X/Y): documented codes plus the lowercase
 * submodule variants and type-change 'T'. Anything else is not a record
 * header, so a malformed stream fails closed instead of being misread. */
static bool status_code_ok(char c) {
    switch (c) {
        case ' ':
        case 'M':
        case 'A':
        case 'D':
        case 'R':
        case 'C':
        case 'U':
        case '?':
        case '!':
        case 'T':
        case 'm':
        case 'a':
        case 'd':
        case 'r':
        case 'c':
        case 'u':
        case 't':
            return true;
        default:
            return false;
    }
}

/* Retain one bounded sample; the running total always advances so the caller
 * sees the true stream count even when samples are capped or OOM. */
static void status_add_sample(int *total, char ***samples, int *sample_count,
                              bool *truncated, int max_samples, const char *path,
                              size_t path_len) {
    (*total)++;
    if (*sample_count >= max_samples) {
        *truncated = true;
        return;
    }
    char *copy = (char *)malloc(path_len + 1);
    if (!copy) {
        *truncated = true;
        return;
    }
    memcpy(copy, path, path_len);
    copy[path_len] = '\0';
    char **grown = (char **)realloc(*samples, (size_t)(*sample_count + 1) * sizeof(char *));
    if (!grown) {
        free(copy);
        *truncated = true;
        return;
    }
    *samples = grown;
    (*samples)[*sample_count] = copy;
    (*sample_count)++;
}

/* Parse one complete NUL-delimited field. Returns false on malformed input,
 * which fails the whole snapshot closed. */
static bool status_parse_record(cbm_worktree_status_t *out, status_parser_t *parser,
                                const char *field, size_t field_len) {
    if (parser->expecting_source) {
        parser->expecting_source = false;
        return true; /* second path of a rename/copy record — not counted */
    }
    if (field_len < STATUS_HEADER_LEN || field[2] != ' ' ||
        !status_code_ok(field[0]) || !status_code_ok(field[1])) {
        return false;
    }
    const char *path = field + STATUS_HEADER_LEN;
    size_t path_len = field_len - STATUS_HEADER_LEN;
    if (field[0] == '?' && field[1] == '?') {
        status_add_sample(&out->untracked_count, &out->untracked_paths,
                          &out->untracked_sample_count, &out->untracked_truncated,
                          parser->max_samples, path, path_len);
        return true;
    }
    if (field[0] == 'R' || field[0] == 'C' || field[1] == 'R' || field[1] == 'C') {
        parser->expecting_source = true;
    }
    status_add_sample(&out->tracked_count, &out->tracked_paths,
                      &out->tracked_sample_count, &out->tracked_truncated,
                      parser->max_samples, path, path_len);
    return true;
}

/* Feed a chunk of the NUL-delimited stream through the bounded record parser. */
static void status_feed(status_parser_t *parser, const char *data, size_t data_len) {
    size_t i = 0;
    while (i < data_len) {
        if (parser->partial_len >= STATUS_FIELD_MAX) {
            parser->parse_ok = false; /* field exceeded the bound: malformed */
            return;
        }
        size_t j = i;
        while (j < data_len && data[j] != '\0' &&
               parser->partial_len + (j - i) < STATUS_FIELD_MAX) {
            j++;
        }
        size_t take = j - i;
        memcpy(parser->partial + parser->partial_len, data + i, take);
        parser->partial_len += take;
        i += take;
        if (i < data_len && data[i] == '\0') {
            if (!status_parse_record(parser->out, parser, parser->partial,
                                     parser->partial_len)) {
                parser->parse_ok = false;
                return;
            }
            parser->partial_len = 0;
            i++; /* consume the record terminator */
        } else if (i >= data_len) {
            return; /* chunk exhausted mid-record */
        }
    }
}

int cbm_git_worktree_status(const char *validated_root, int max_samples,
                            cbm_worktree_status_t *out) {
    if (!out) {
        return CBM_NOT_FOUND;
    }
    memset(out, 0, sizeof(*out));
    if (!validated_root || !validated_root[0] || max_samples < 0) {
        return CBM_NOT_FOUND;
    }
    if (!git_validate_repo_path(validated_root)) {
        return CBM_NOT_FOUND;
    }

    char cmd[GIT_CMD_MAX];
#ifdef _WIN32
    const char *null_dev = "NUL";
#else
    const char *null_dev = "/dev/null";
#endif
    int n = snprintf(cmd, sizeof(cmd),
                     "git -C \"%s\" status --porcelain=v1 -z --untracked-files=all 2>%s",
                     validated_root, null_dev);
    if (n < 0 || n >= (int)sizeof(cmd)) {
        return CBM_NOT_FOUND;
    }

    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return 0; /* out is zeroed: available=false */
    }

    status_parser_t parser;
    memset(&parser, 0, sizeof(parser));
    parser.out = out;
    parser.max_samples = max_samples;
    parser.parse_ok = true;

    char chunk[STATUS_CHUNK_MAX];
    size_t got;
    bool io_error = false;
    while (parser.parse_ok && (got = fread(chunk, 1, sizeof(chunk), fp)) > 0) {
        status_feed(&parser, chunk, got);
    }
    if (ferror(fp)) {
        io_error = true;
    }
    int rc = cbm_pclose(fp);

    if (rc != 0 || !parser.parse_ok || io_error || parser.partial_len > 0) {
        /* Fail closed: any deviation from a clean, complete status stream is a
         * machine-readable "unavailable", never a clean worktree. */
        cbm_git_worktree_status_free(out);
        return 0;
    }

    out->available = true;
    return 0;
}

void cbm_git_worktree_status_free(cbm_worktree_status_t *st) {
    if (!st) {
        return;
    }
    for (int i = 0; i < st->tracked_sample_count; i++) {
        free(st->tracked_paths[i]);
    }
    free(st->tracked_paths);
    for (int i = 0; i < st->untracked_sample_count; i++) {
        free(st->untracked_paths[i]);
    }
    free(st->untracked_paths);
    memset(st, 0, sizeof(*st));
}
