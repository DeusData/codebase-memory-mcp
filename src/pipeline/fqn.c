/*
 * fqn.c — Fully Qualified Name computation for graph nodes.
 *
 * Implements the FQN scheme: project.dir.parts.name
 * Handles Python __init__.py, JS/TS index.{js,ts}, path separators.
 */
#include "pipeline/pipeline.h"
#include "foundation/constants.h"
#include "foundation/platform.h"

#include <stdbool.h>
#include <stddef.h> // NULL
#include <stdio.h>
#include <stdlib.h>
#include <string.h> // strdup

/* Maximum path segments in a FQN (CBM_SZ_256 slots total, -2 for project + name) */
#define FQN_MAX_PATH_SEGS 254
#define FQN_MAX_DIR_SEGS 255

/* ── Internal helpers ─────────────────────────────────────────────── */

/* Build a dot-joined string from segments. Returns heap-allocated string. */
static char *join_segments(const char **segments, int count) {
    if (count == 0) {
        return strdup("");
    }
    size_t total = 0;
    for (int i = 0; i < count; i++) {
        total += strlen(segments[i]);
        if (i > 0) {
            total++; /* dot separator */
        }
    }
    char *result = malloc(total + SKIP_ONE);
    if (!result) {
        return NULL;
    }
    char *p = result;
    for (int i = 0; i < count; i++) {
        if (i > 0) {
            *p++ = '.';
        }
        size_t len = strlen(segments[i]);
        memcpy(p, segments[i], len);
        p += len;
    }
    *p = '\0';
    return result;
}

/* Strip file extension from the last path component. */
static void strip_file_extension(char *path) {
    char *last_slash = strrchr(path, '/');
    char *start = last_slash ? last_slash + SKIP_ONE : path;
    char *ext = strrchr(start, '.');
    if (ext) {
        *ext = '\0';
    }
}

/* Tokenize path by '/' into segments array. Returns number of segments added. */
static int tokenize_path(char *path, const char **segments, int max_segs) {
    int count = 0;
    if (path[0] == '\0') {
        return 0;
    }
    char *tok = path;
    while (tok && *tok && count < max_segs) {
        char *slash = strchr(tok, '/');
        if (slash) {
            *slash = '\0';
        }
        if (tok[0] != '\0') {
            segments[count++] = tok;
        }
        tok = slash ? slash + SKIP_ONE : NULL;
    }
    return count;
}

/* Strip __init__ (Python) / index (JS/TS) from the last segment when a
 * symbol name is provided. Keeps it when no name is given to avoid QN
 * collision with Folder nodes for the same directory. */
static void strip_init_or_index(const char **segments, int *seg_count, const char *name) {
    if (*seg_count <= SKIP_ONE) {
        return;
    }
    const char *last = segments[*seg_count - SKIP_ONE];
    if (strcmp(last, "__init__") != 0 && strcmp(last, "index") != 0) {
        return;
    }
    if (name && name[0] != '\0') {
        (*seg_count)--;
    }
}

/* ── Public API ──────────────────────────────────────────────────── */

char *cbm_pipeline_fqn_compute(const char *project, const char *rel_path, const char *name) {
    if (!project) {
        return strdup("");
    }

    char *path = strdup(rel_path ? rel_path : "");
    cbm_normalize_path_sep(path);
    strip_file_extension(path);

    const char *segments[CBM_SZ_256];
    int seg_count = 0;
    segments[seg_count++] = project;
    seg_count += tokenize_path(path, segments + seg_count, FQN_MAX_PATH_SEGS);

    strip_init_or_index(segments, &seg_count, name);

    if (name && name[0] != '\0') {
        segments[seg_count++] = name;
    }

    char *result = join_segments(segments, seg_count);
    free(path);
    return result;
}

char *cbm_pipeline_fqn_module(const char *project, const char *rel_path) {
    return cbm_pipeline_fqn_compute(project, rel_path, NULL);
}

enum {
    FQN_PATH_BUF = 1024,
    FQN_SEP_LEN = 1, /* one byte for the '/' separator */
    FQN_NUL_LEN = 1, /* one byte for the terminating NUL */
    FQN_DOTDOT_LEN = 2,
    FQN_MIN_PY_DOTS = 1, /* first leading dot is "current package", not a pop */
    FQN_REL_KIND_NONE = 0,
    FQN_REL_KIND_PYTHON = 1,
    FQN_REL_KIND_JS = 2,
};

/* Append a single path segment to a mutable buffer that already holds a
 * normalized slash-separated path.  Adds a '/' separator when needed,
 * returns false if the buffer would overflow. */
static bool path_append_segment(char *buf, size_t buf_size, const char *seg, size_t seg_len) {
    size_t cur = strlen(buf);
    size_t need = cur + (cur > 0 ? FQN_SEP_LEN : 0) + seg_len + FQN_NUL_LEN;
    if (need > buf_size) {
        return false;
    }
    if (cur > 0) {
        buf[cur++] = '/';
    }
    memcpy(buf + cur, seg, seg_len);
    buf[cur + seg_len] = '\0';
    return true;
}

/* Pop the last segment from a mutable slash-separated path. */
static void path_pop_segment(char *buf) {
    char *last = strrchr(buf, '/');
    if (last) {
        *last = '\0';
    } else {
        buf[0] = '\0';
    }
}

/* Seed `buf` with the source file's directory (strip the basename) and
 * normalize backslashes. */
static void seed_source_dir(char *buf, size_t buf_size, const char *source_rel) {
    snprintf(buf, buf_size, "%s", source_rel ? source_rel : "");
    for (char *p = buf; *p; p++) {
        if (*p == '\\') {
            *p = '/';
        }
    }
    char *last = strrchr(buf, '/');
    if (last) {
        *last = '\0';
    } else {
        buf[0] = '\0';
    }
}

/* Detect the flavor of relative import based on the leading characters.
 * Returns 1 for Python dotted form (e.g. ".foo" or "..bar.baz"),
 *         2 for JS/TS slash form (e.g. "./foo" or "../bar/baz"),
 *         0 for anything not relative (caller should skip). */
static int classify_relative_import(const char *module_path) {
    if (!module_path || module_path[0] != '.') {
        return FQN_REL_KIND_NONE;
    }
    bool has_slash = strchr(module_path, '/') != NULL;
    bool js_like = module_path[FQN_SEP_LEN] == '/' ||
                   (module_path[FQN_SEP_LEN] == '.' && module_path[FQN_DOTDOT_LEN] == '/');
    if (has_slash || js_like) {
        return FQN_REL_KIND_JS;
    }
    return FQN_REL_KIND_PYTHON;
}

/* Python relative import: ".foo", "..bar.baz" → resolve against source dir. */
static char *resolve_python_relative(char *buf, size_t buf_size, const char *module_path) {
    const char *p = module_path;
    int dot_count = 0;
    while (*p == '.') {
        dot_count++;
        p++;
    }
    for (int i = FQN_MIN_PY_DOTS; i < dot_count; i++) {
        path_pop_segment(buf);
    }
    while (*p) {
        const char *seg_start = p;
        while (*p && *p != '.') {
            p++;
        }
        size_t seg_len = (size_t)(p - seg_start);
        if (seg_len > 0 && !path_append_segment(buf, buf_size, seg_start, seg_len)) {
            return NULL;
        }
        if (*p == '.') {
            p++;
        }
    }
    return strdup(buf);
}

/* Strip a trailing file extension from a segment (e.g. "helpers.ts" → "helpers").
 * Returns the new segment length. */
static size_t strip_ext(const char *seg_start, size_t seg_len) {
    const char *seg_end = seg_start + seg_len;
    const char *dot = NULL;
    for (const char *d = seg_end - FQN_SEP_LEN; d >= seg_start; d--) {
        if (*d == '.') {
            dot = d;
            break;
        }
    }
    if (dot && dot > seg_start) {
        return (size_t)(dot - seg_start);
    }
    return seg_len;
}

/* JS/TS relative import: "./foo", "../bar/baz" → resolve against source dir. */
static char *resolve_js_relative(char *buf, size_t buf_size, const char *module_path) {
    const char *p = module_path;
    while (*p) {
        while (*p == '/') {
            p++;
        }
        if (!*p) {
            break;
        }
        const char *seg_start = p;
        while (*p && *p != '/') {
            p++;
        }
        size_t seg_len = (size_t)(p - seg_start);
        if (seg_len == FQN_SEP_LEN && seg_start[0] == '.') {
            continue;
        }
        if (seg_len == FQN_DOTDOT_LEN && seg_start[0] == '.' && seg_start[FQN_SEP_LEN] == '.') {
            path_pop_segment(buf);
            continue;
        }
        if (*p == '\0') {
            seg_len = strip_ext(seg_start, seg_len);
        }
        if (seg_len > 0 && !path_append_segment(buf, buf_size, seg_start, seg_len)) {
            return NULL;
        }
    }
    return strdup(buf);
}

char *cbm_pipeline_resolve_relative_import(const char *source_rel, const char *module_path) {
    int kind = classify_relative_import(module_path);
    if (kind == FQN_REL_KIND_NONE) {
        return NULL;
    }
    char buf[FQN_PATH_BUF];
    seed_source_dir(buf, sizeof(buf), source_rel);
    if (kind == FQN_REL_KIND_PYTHON) {
        return resolve_python_relative(buf, sizeof(buf), module_path);
    }
    return resolve_js_relative(buf, sizeof(buf), module_path);
}

char *cbm_pipeline_fqn_folder(const char *project, const char *rel_dir) {
    if (!project) {
        return strdup("");
    }

    /* Work on mutable copy */
    char *dir = strdup(rel_dir ? rel_dir : "");
    cbm_normalize_path_sep(dir);

    const char *segments[CBM_SZ_256];
    int seg_count = 0;
    segments[seg_count++] = project;

    if (dir[0] != '\0') {
        char *tok = dir;
        while (tok && *tok && seg_count < FQN_MAX_DIR_SEGS) {
            char *slash = strchr(tok, '/');
            if (slash) {
                *slash = '\0';
            }
            if (tok[0] != '\0') {
                segments[seg_count++] = tok;
            }
            tok = slash ? slash + SKIP_ONE : NULL;
        }
    }

    char *result = join_segments(segments, seg_count);
    free(dir);
    return result;
}

char *cbm_project_name_from_path(const char *abs_path) {
    if (!abs_path || !abs_path[0]) {
        return strdup("root");
    }

    /* Work on mutable copy */
    char *path = strdup(abs_path);
    size_t len = strlen(path);

    /* Normalize path separators */
    cbm_normalize_path_sep(path);

    /* Replace / and : with - */
    for (size_t i = 0; i < len; i++) {
        if (path[i] == '/' || path[i] == ':') {
            path[i] = '-';
        }
    }

    /* Collapse consecutive dashes */
    char *dst = path;
    char prev = 0;
    for (size_t i = 0; i < len; i++) {
        if (path[i] == '-' && prev == '-') {
            continue;
        }
        *dst++ = path[i];
        prev = path[i];
    }
    *dst = '\0';

    /* Trim leading dashes */
    char *start = path;
    while (*start == '-') {
        start++;
    }

    /* Trim trailing dashes */
    size_t slen = strlen(start);
    while (slen > 0 && start[slen - SKIP_ONE] == '-') {
        start[--slen] = '\0';
    }

    if (*start == '\0') {
        free(path);
        return strdup("root");
    }

    char *result = strdup(start);
    free(path);
    return result;
}
