/*
 * fqn.c — Fully Qualified Name computation for graph nodes.
 *
 * Implements the FQN scheme: project.dir.parts.name
 * Handles Python __init__.py, JS/TS index.{js,ts}, path separators.
 */
#include "pipeline/pipeline.h"
#include "foundation/platform.h"

#include <stddef.h> // NULL
#include <stdio.h>
#include <stdlib.h>
#include <string.h> // strdup

/* Maximum path segments in a FQN (256 slots total, -2 for project + name) */
#define FQN_MAX_PATH_SEGS 254
#define FQN_MAX_DIR_SEGS 255

/* ── Internal helpers ─────────────────────────────────────────────── */

/* Build a dot-joined string from segments. Returns heap-allocated string. */
static char *join_segments(const char **segments, int count) {
    if (count == 0) {
        // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
        return strdup("");
    }
    size_t total = 0;
    for (int i = 0; i < count; i++) {
        total += strlen(segments[i]);
        if (i > 0) {
            total++; /* dot separator */
        }
    }
    char *result = malloc(total + 1);
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

/* ── Public API ──────────────────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
char *cbm_pipeline_fqn_compute(const char *project, const char *rel_path, const char *name) {
    if (!project) {
        return strdup("");
    }

    /* Work on a mutable copy for path manipulation */
    char *path = strdup(rel_path ? rel_path : "");

    /* Normalize path separators */
    cbm_normalize_path_sep(path);

    /* Strip file extension */
    {
        char *last_slash = strrchr(path, '/');
        char *start = last_slash ? last_slash + 1 : path;
        char *ext = strrchr(start, '.');
        if (ext) {
            *ext = '\0';
        }
    }

    /* Split by '/' into segments */
    const char *segments[256];
    int seg_count = 0;

    /* First segment is always the project */
    segments[seg_count++] = project;

    /* Add path segments */
    if (path[0] != '\0') {
        char *tok = path;
        while (tok && *tok && seg_count < FQN_MAX_PATH_SEGS) {
            char *slash = strchr(tok, '/');
            if (slash) {
                *slash = '\0';
            }
            if (tok[0] != '\0') {
                segments[seg_count++] = tok;
            }
            tok = slash ? slash + 1 : NULL;
        }
    }

    /* Handle __init__ (Python) and index (JS/TS):
     * Strip from module QN to get the package name, BUT only when a name
     * suffix is provided (e.g., fqn_compute("proj", "pkg/__init__.py", "MyClass")
     * → "proj.pkg.MyClass"). When no name is given (fqn_module for the file
     * itself), keep "__init__" to avoid QN collision with the Folder node
     * for the same directory. */
    if (seg_count > 1) {
        const char *last = segments[seg_count - 1];
        if (strcmp(last, "__init__") == 0 || strcmp(last, "index") == 0) {
            if (name && name[0] != '\0') {
                /* Has a symbol name — strip __init__ so symbols get clean package QN */
                seg_count--;
            }
            /* else: no name → keep __init__/index as disambiguator */
        }
    }

    /* Add name if provided */
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

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
char *cbm_pipeline_fqn_folder(const char *project, const char *rel_dir) {
    if (!project) {
        return strdup("");
    }

    /* Work on mutable copy */
    char *dir = strdup(rel_dir ? rel_dir : "");
    cbm_normalize_path_sep(dir);

    const char *segments[256];
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
            tok = slash ? slash + 1 : NULL;
        }
    }

    char *result = join_segments(segments, seg_count);
    free(dir);
    return result;
}

/**
 * Resolve an import module_path relative to the importing file's directory.
 *
 * For relative paths (starting with ./ or ../), resolves against the importer's
 * directory. For bare module specifiers (no ./ prefix), returns a copy unchanged.
 *
 * Examples (importer_rel_path="src/routes/api.js"):
 *   "./controllers/auth"     → "src/routes/controllers/auth"
 *   "../utils/helpers"       → "src/utils/helpers"
 *   "lodash"                 → "lodash" (bare module, unchanged)
 *   "@hapi/hapi"             → "@hapi/hapi" (scoped package, unchanged)
 *
 * Returns: heap-allocated resolved path. Caller must free().
 */
char *cbm_pipeline_resolve_import_path(const char *importer_rel_path, const char *module_path) {
    if (!module_path || !module_path[0]) {
        return strdup("");
    }

    /* Bare module specifier — no relative path resolution needed */
    if (module_path[0] != '.') {
        return strdup(module_path);
    }

    /* Get the importing file's directory */
    char *importer_dir = strdup(importer_rel_path ? importer_rel_path : "");
    cbm_normalize_path_sep(importer_dir);
    char *last_slash = strrchr(importer_dir, '/');
    if (last_slash) {
        *(last_slash + 1) = '\0'; /* keep trailing slash */
    } else {
        importer_dir[0] = '\0'; /* file is at root */
    }

    /* Concatenate: importer_dir + module_path */
    size_t dir_len = strlen(importer_dir);
    size_t mod_len = strlen(module_path);
    char *combined = malloc(dir_len + mod_len + 2);
    snprintf(combined, dir_len + mod_len + 2, "%s%s", importer_dir, module_path);
    free(importer_dir);

    /* Normalize: resolve . and .. segments */
    cbm_normalize_path_sep(combined);
    const char *segments[256];
    int seg_count = 0;

    char *tok = combined;
    while (tok && *tok) {
        char *slash = strchr(tok, '/');
        if (slash) *slash = '\0';

        if (strcmp(tok, ".") == 0) {
            /* skip */
        } else if (strcmp(tok, "..") == 0) {
            if (seg_count > 0) seg_count--; /* pop parent */
        } else if (tok[0] != '\0') {
            if (seg_count < 255) {
                segments[seg_count++] = tok;
            }
        }

        tok = slash ? slash + 1 : NULL;
    }

    /* Rebuild path */
    if (seg_count == 0) {
        free(combined);
        return strdup("");
    }

    size_t total = 0;
    for (int i = 0; i < seg_count; i++) {
        total += strlen(segments[i]) + 1;
    }
    char *result = malloc(total + 1);
    result[0] = '\0';
    for (int i = 0; i < seg_count; i++) {
        if (i > 0) strcat(result, "/");
        strcat(result, segments[i]);
    }

    free(combined);
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
    while (slen > 0 && start[slen - 1] == '-') {
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
