
/*
 * gitignore.c — Gitignore-style pattern matching.
 *
 * Implements the core gitignore pattern matching algorithm:
 *   - * matches anything except /
 *   - ** matches any number of path components
 *   - ? matches any single character except /
 *   - [abc] and [a-z] character classes
 *   - ! prefix for negation
 *   - trailing / for directory-only matching
 *   - patterns with / are rooted (anchored to base)
 */
#include "foundation/constants.h"
#include "foundation/compat_fs.h"
#include "discover/gitignore_internal.h"

enum { GI_INIT_CAP = 16 };
#include "discover/discover.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Pattern representation ──────────────────────────────────────── */

struct cbm_gitignore {
    gi_pattern_t *patterns;
    int count;
    int capacity;
};

/* ── Pattern parsing ─────────────────────────────────────────────── */

static void gi_add_pattern(cbm_gitignore_t *gi, const char *line, int len) {
    gi_view_t view;
    if (!gi_normalize(line, (size_t)len, &view, NULL) || !view.length) {
        return;
    }
    gi_pattern_t p = {.negated = view.negated, .dir_only = view.dir_only, .rooted = view.rooted};
    const char *start = view.text;
    len = (int)view.length;

    /* Copy pattern */
    p.pattern = malloc(len + SKIP_ONE);
    if (!p.pattern) {
        return;
    }
    memcpy(p.pattern, start, len);
    p.pattern[len] = '\0';

    /* Grow array if needed */
    if (gi->count >= gi->capacity) {
        int new_cap = gi->capacity ? gi->capacity * PAIR_LEN : GI_INIT_CAP;
        gi_pattern_t *new_patterns = realloc(gi->patterns, new_cap * sizeof(gi_pattern_t));
        if (!new_patterns) {
            free(p.pattern);
            return;
        }
        gi->patterns = new_patterns;
        gi->capacity = new_cap;
    }

    gi->patterns[gi->count++] = p;
}

/* ── Public API ──────────────────────────────────────────────────── */

cbm_gitignore_t *cbm_gitignore_parse(const char *content) {
    if (!content) {
        return NULL;
    }

    cbm_gitignore_t *gi = calloc(CBM_ALLOC_ONE, sizeof(cbm_gitignore_t));
    if (!gi) {
        return NULL;
    }

    const char *line = content;
    while (*line) {
        /* Find end of line */
        const char *eol = strchr(line, '\n');
        int len = eol ? (int)(eol - line) : (int)strlen(line);

        /* Skip comments and blank lines */
        if (len > 0 && line[0] != '#') {
            gi_add_pattern(gi, line, len);
        }

        if (!eol) {
            break;
        }
        line = eol + SKIP_ONE;
    }

    return gi;
}

cbm_gitignore_t *cbm_gitignore_load(const char *path) {
    if (!path) {
        return NULL;
    }

    FILE *f = cbm_fopen(path, "r");
    if (!f) {
        return NULL;
    }

    /* Read entire file */
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);

    if (size <= 0) {
        (void)fclose(f);
        return cbm_gitignore_parse("");
    }

    char *buf = malloc(size + SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }

    size_t n = fread(buf, SKIP_ONE, size, f);
    buf[n] = '\0';
    (void)fclose(f);

    cbm_gitignore_t *gi = cbm_gitignore_parse(buf);
    free(buf);
    return gi;
}

int cbm_gitignore_match_result(const cbm_gitignore_t *gi, const char *rel_path, bool is_dir) {
    if (!gi || !rel_path) {
        return 0;
    }
    return gi_core_match(gi->patterns, (size_t)gi->count, rel_path, is_dir, NULL);
}

bool cbm_gitignore_matches(const cbm_gitignore_t *gi, const char *rel_path, bool is_dir) {
    return cbm_gitignore_match_result(gi, rel_path, is_dir) > 0;
}

void cbm_gitignore_free(cbm_gitignore_t *gi) {
    if (!gi) {
        return;
    }
    for (int i = 0; i < gi->count; i++) {
        free(gi->patterns[i].pattern);
    }
    free(gi->patterns);
    free(gi);
}

/* Test seam: lets a unit test simulate strdup() failure mid-merge so the
 * atomic-rollback path can be exercised without real OOM. NULL = use strdup. */
char *(*cbm_gitignore_merge_dup_hook_for_test)(const char *) = NULL;

bool cbm_gitignore_merge(cbm_gitignore_t *dst, const cbm_gitignore_t *src) {
    if (!dst) {
        return false;
    }
    if (!src || src->count == 0) {
        return true; /* nothing to merge */
    }
    int needed = dst->count + src->count;
    if (needed > dst->capacity) {
        gi_pattern_t *grown = realloc(dst->patterns, (size_t)needed * sizeof(gi_pattern_t));
        if (!grown) {
            return false; /* dst left unchanged */
        }
        dst->patterns = grown;
        dst->capacity = needed;
    }
    int start_count = dst->count;
    for (int i = 0; i < src->count; i++) {
        char *pat = cbm_gitignore_merge_dup_hook_for_test
                        ? cbm_gitignore_merge_dup_hook_for_test(src->patterns[i].pattern)
                        : strdup(src->patterns[i].pattern);
        if (!pat) {
            /* Roll back partial copies so dst is unchanged on failure (atomic
             * merge). A silent partial merge could drop the very exclude
             * pattern the caller relied on while keeping others. */
            for (int j = start_count; j < dst->count; j++) {
                free(dst->patterns[j].pattern);
            }
            dst->count = start_count;
            return false;
        }
        dst->patterns[dst->count].pattern = pat;
        dst->patterns[dst->count].negated = src->patterns[i].negated;
        dst->patterns[dst->count].dir_only = src->patterns[i].dir_only;
        dst->patterns[dst->count].rooted = src->patterns[i].rooted;
        dst->count++;
    }
    return true;
}
