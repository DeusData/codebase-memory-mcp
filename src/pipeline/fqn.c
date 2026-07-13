/*
 * fqn.c — Fully Qualified Name computation for graph nodes.
 *
 * Implements the FQN scheme: project.dir.parts.name
 * Handles Python __init__.py, JS/TS index.{js,ts}, path separators.
 */
#include "pipeline/pipeline.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/platform.h"

#include <stdbool.h>
#include <stddef.h> // NULL
#include <stdint.h> // uint32_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h> // strdup
#ifdef _WIN32
#include <direct.h>
#include <io.h>
#endif

/* Maximum path segments in a FQN (CBM_SZ_256 slots total, -2 for project + name) */
#define FQN_MAX_PATH_SEGS 254
#define FQN_MAX_DIR_SEGS 255

/* Max bytes for a derived project name. The name becomes a filename component
 * ("<cache>/<name>.db" and sidecars ".db-wal"/".db.corrupt"), so it must stay
 * under the filesystem's 255-byte component limit. 200 leaves headroom for the
 * longest sidecar suffix. #571 hex-encodes each non-ASCII byte to 2 chars, so a
 * deep CJK path can triple past 255 and make the DB file un-openable (#624). */
#define FQN_MAX_NAME_LEN 200

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

char *cbm_pipeline_fqn_module_dir(const char *project, const char *rel_path, bool module_is_dir) {
    if (!module_is_dir) {
        /* Filename-stem module (default for all but Java/Go). */
        return cbm_pipeline_fqn_module(project, rel_path);
    }
    /* Directory-module languages (Java package, Go package): the module is the
     * CONTAINING DIRECTORY — strip the basename so a sibling file in the same
     * dir shares the module QN. This MUST agree with the extraction-side
     * cbm_fqn_module_source_lang() (internal/cbm/helpers.c) so the cross-file
     * LSP caller_qn matches the def-node QN. */
    const char *src = rel_path ? rel_path : "";
    /* Strip the last path segment using either separator (the extraction side
     * normalizes too); look for the rightmost '/' or '\\'. */
    const char *last_fwd = strrchr(src, '/');
    const char *last_bwd = strrchr(src, '\\');
    const char *last_sep = last_fwd > last_bwd ? last_fwd : last_bwd;
    if (!last_sep) {
        /* Root file: empty directory → module is just the project. */
        return cbm_pipeline_fqn_folder(project, "");
    }
    size_t dir_len = (size_t)(last_sep - src);
    char *dir = (char *)malloc(dir_len + 1); /* +1 for NUL */
    if (!dir) {
        return NULL;
    }
    memcpy(dir, src, dir_len);
    dir[dir_len] = '\0';
    char *res = cbm_pipeline_fqn_folder(project, dir);
    free(dir);
    return res;
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

/* Bound a derived project name to FQN_MAX_NAME_LEN bytes so "<cache>/<name>.db"
 * stays within the filesystem's 255-byte filename-component limit (#624). Names
 * within the cap are returned UNCHANGED (no drift). Longer names keep their first
 * (CAP-9) bytes and get a "-XXXXXXXX" FNV-1a hash of the FULL name appended, so
 * two long paths that share a prefix but differ later still map to distinct
 * names. The suffix ends in a hex digit, so the result stays validator-safe. */
static char *fqn_bound_name_len(char *name) {
    if (!name) {
        return name;
    }
    size_t n = strlen(name);
    if (n <= FQN_MAX_NAME_LEN) {
        return name; /* within cap → unchanged, no drift */
    }
    uint32_t h = 2166136261u; /* FNV-1a offset basis over the FULL name */
    for (size_t i = 0; i < n; i++) {
        h ^= (unsigned char)name[i];
        h *= 16777619u;
    }
    /* Keep first (CAP-9) bytes + "-" + 8 hex = CAP total. The buffer holds n+1
     * bytes and n > CAP, so writing 9 chars + NUL at offset (CAP-9) fits. */
    snprintf(name + (FQN_MAX_NAME_LEN - 9), 10, "-%08x", h);
    return name;
}

static bool path_is_root_syntax(const char *path) {
    if (!path || !path[0]) {
        return false;
    }
    for (const char *p = path; *p; p++) {
        if (*p != '/' && *p != '\\' && *p != ':') {
            return false;
        }
    }
    return true;
}

char *cbm_project_name_from_path(const char *abs_path) {
    if (!abs_path || !abs_path[0]) {
        return strdup("root");
    }
    if (path_is_root_syntax(abs_path)) {
        return strdup("root");
    }

    char real[CBM_SZ_4K];
    const char *name_path = abs_path;
    /* Wide-path canonicalization — the ANSI _access/_fullpath pair corrupted
     * CJK paths on CJK-locale Windows (#973). */
    if (cbm_canonical_path(abs_path, real, sizeof(real))) {
        cbm_normalize_path_sep(real);
        name_path = real;
    }

    /* Work on mutable copy */
    char *path = strdup(name_path);
    if (!path) {
        return NULL;
    }
    size_t len = strlen(path);

    /* Normalize path separators */
    cbm_normalize_path_sep(path);

    /* Map every character that is unsafe for portable project DB names. We
     * keep derived names in [A-Za-z0-9._-], so anything else — path
     * separators, ':', spaces, '@', '+', … — must be normalized here.
     * Otherwise a repo like
     * "/home/u/my project" yields the name "home-u-my project": indexing
     * creates the DB and it shows in list_projects, but resolve_store rejects
     * the space and reports project-not-found (#349).
     *
     * Non-ASCII bytes (UTF-8 of CJK and other scripts, all >= 0x80) are NOT
     * dropped to '-' — that silently erased whole path segments and produced
     * unrecognizable / colliding names (#571). Instead each non-ASCII byte is
     * transliterated to its two lowercase hex digits, which use only [0-9a-f]
     * and therefore stay validator-safe while preserving the segment. */
    static const char hex_digits[] = "0123456789abcdef";
    char *mapped = malloc(len * 2 + 1); /* worst case: every byte → 2 hex chars */
    if (!mapped) {
        free(path);
        return strdup("root");
    }
    size_t mlen = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)path[i];
        bool safe = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
                    c == '.' || c == '_' || c == '-';
        if (safe) {
            mapped[mlen++] = (char)c;
        } else if (c >= 0x80) {
            mapped[mlen++] = hex_digits[(c >> 4) & 0xF];
            mapped[mlen++] = hex_digits[c & 0xF];
        } else {
            mapped[mlen++] = '-';
        }
    }
    mapped[mlen] = '\0';
    free(path);
    path = mapped;
    len = mlen;

    /* Collapse consecutive dashes, and consecutive dots (the validator also
     * rejects any ".." sequence). */
    char *dst = path;
    char prev = 0;
    for (size_t i = 0; i < len; i++) {
        if ((path[i] == '-' && prev == '-') || (path[i] == '.' && prev == '.')) {
            continue;
        }
        *dst++ = path[i];
        prev = path[i];
    }
    *dst = '\0';

    /* Trim leading dashes and dots (the validator rejects a leading dot). */
    char *start = path;
    while (*start == '-' || *start == '.') {
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
    if (result) {
        result = fqn_bound_name_len(result); /* #624: cap filename-component length */
    }
    return result;
}
