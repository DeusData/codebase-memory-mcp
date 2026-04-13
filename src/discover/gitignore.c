
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

enum { GI_INIT_CAP = 16, GI_CHAR_IDX1 = 1, GI_CHAR_IDX2 = 2, GI_SKIP3 = 3 };
#include "discover/discover.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Pattern representation ──────────────────────────────────────── */

typedef struct {
    char *pattern; /* the glob pattern (normalized) */
    bool negated;  /* starts with ! */
    bool dir_only; /* ends with / */
    bool rooted;   /* contains / (anchored to root) */
} gi_pattern_t;

struct cbm_gitignore {
    gi_pattern_t *patterns;
    int count;
    int capacity;
};

/* ── Pattern matching engine ─────────────────────────────────────── */

/* Forward declaration for recursive calls. */
static bool glob_match(const char *pat, const char *str); // NOLINT(misc-no-recursion)

/* Match a ** (doublestar-slash) pattern: try rest at every / boundary. */
static bool glob_match_doublestar_slash(const char *pat, // NOLINT(misc-no-recursion)
                                        const char *str) {
    if (glob_match(pat, str)) {
        return true;
    }
    for (const char *s = str; *s; s++) {
        if (*s == '/' && glob_match(pat, s + SKIP_ONE)) {
            return true;
        }
    }
    return false;
}

/* Match a ** (doublestar) followed by non-slash: try at every position. */
static bool glob_match_doublestar_any(const char *pat, // NOLINT(misc-no-recursion)
                                      const char *str) {
    for (const char *s = str;; s++) {
        if (glob_match(pat, s)) {
            return true;
        }
        if (!*s) {
            return false;
        }
    }
}

/* Match a * (single star): match any sequence not containing /. */
static bool glob_match_star(const char *pat, const char *str) { // NOLINT(misc-no-recursion)
    for (const char *s = str;; s++) {
        if (glob_match(pat, s)) {
            return true;
        }
        if (!*s || *s == '/') {
            return false;
        }
    }
}

/* Match a [...] character class at current position.
 * Returns true if matched. Advances *pat_out past the closing ']'. */
static bool glob_match_charclass(const char *pat, char ch, const char **pat_out) {
    bool negate_class = false;
    if (*pat == '!' || *pat == '^') {
        negate_class = true;
        pat++;
    }
    bool matched = false;
    char prev = 0;
    while (*pat && *pat != ']') {
        if (*pat == '-' && prev && pat[GI_CHAR_IDX1] && pat[GI_CHAR_IDX1] != ']') {
            pat++;
            if (ch >= prev && ch <= *pat) {
                matched = true;
            }
            prev = *pat;
            pat++;
        } else {
            if (ch == *pat) {
                matched = true;
            }
            prev = *pat;
            pat++;
        }
    }
    if (*pat == ']') {
        pat++;
    }
    *pat_out = pat;
    return negate_class ? !matched : matched;
}

/*
 * Match a glob pattern against a string.
 * Handles: * (non-slash), ** (any path), ? (single non-slash), [class]
 */
/* Handle ** at current position. Returns match result. */
static bool glob_match_doublestar(const char *pat, const char *str) { // NOLINT(misc-no-recursion)
    if (pat[GI_CHAR_IDX2] == '/') {
        return glob_match_doublestar_slash(pat + GI_SKIP3, str);
    }
    if (pat[GI_CHAR_IDX2] == '\0') {
        return true;
    }
    return glob_match_doublestar_any(pat + GI_CHAR_IDX2, str);
}

static bool glob_match(const char *pat, const char *str) { // NOLINT(misc-no-recursion)
    while (*pat && *str) {
        if (pat[0] == '*' && pat[GI_CHAR_IDX1] == '*') {
            return glob_match_doublestar(pat, str);
        }

        if (*pat == '*') {
            return glob_match_star(pat + SKIP_ONE, str);
        }

        if (*pat == '?') {
            if (*str == '/') {
                return false;
            }
            pat++;
            str++;
            continue;
        }

        if (*pat == '[') {
            const char *new_pat = NULL;
            if (!glob_match_charclass(pat + SKIP_ONE, *str, &new_pat)) {
                return false;
            }
            pat = new_pat;
            str++;
            continue;
        }

        if (*pat != *str) {
            return false;
        }
        pat++;
        str++;
    }

    while (*pat == '*') {
        pat++;
    }
    return *pat == '\0' && *str == '\0';
}

/* ── Pattern parsing ─────────────────────────────────────────────── */

static void gi_add_pattern(cbm_gitignore_t *gi, const char *line, int len) {
    /* Trim trailing whitespace */
    while (len > 0 && (line[len - SKIP_ONE] == ' ' || line[len - SKIP_ONE] == '\t' ||
                       line[len - SKIP_ONE] == '\r')) {
        len--;
    }
    if (len == 0) {
        return;
    }

    gi_pattern_t p = {0};

    /* Check for negation */
    const char *start = line;
    if (*start == '!') {
        p.negated = true;
        start++;
        len--;
    }

    if (len == 0) {
        return;
    }

    /* Check for trailing / (directory-only) */
    if (start[len - SKIP_ONE] == '/') {
        p.dir_only = true;
        len--;
    }

    if (len == 0) {
        return;
    }

    /* Check for leading / (rooted) */
    if (*start == '/') {
        p.rooted = true;
        start++;
        len--;
    }

    if (len == 0) {
        return;
    }

    /* Check if pattern contains / anywhere (makes it rooted) */
    if (!p.rooted) {
        for (int i = 0; i < len; i++) {
            if (start[i] == '/') {
                p.rooted = true;
                break;
            }
        }
    }

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

    FILE *f = fopen(path, "r");
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

/* Match a non-rooted pattern against basename and path suffixes. */
static bool match_unrooted(const char *pattern, const char *rel_path, const char *basename) {
    if (glob_match(pattern, basename)) {
        return true;
    }
    if (!strchr(rel_path, '/')) {
        return false;
    }
    /* Try matching at every / boundary */
    const char *s = rel_path;
    while (*s) {
        if (glob_match(pattern, s)) {
            return true;
        }
        const char *next = strchr(s, '/');
        if (!next) {
            break;
        }
        s = next + SKIP_ONE;
    }
    return false;
}

bool cbm_gitignore_matches(const cbm_gitignore_t *gi, const char *rel_path, bool is_dir) {
    if (!gi || !rel_path) {
        return false;
    }

    /* Extract the basename for non-rooted pattern matching */
    const char *basename = strrchr(rel_path, '/');
    basename = basename ? basename + SKIP_ONE : rel_path;

    bool matched = false;

    for (int i = 0; i < gi->count; i++) {
        const gi_pattern_t *p = &gi->patterns[i];

        if (p->dir_only && !is_dir) {
            continue;
        }

        bool this_match = p->rooted ? glob_match(p->pattern, rel_path)
                                    : match_unrooted(p->pattern, rel_path, basename);

        if (this_match) {
            matched = !p->negated;
        }
    }

    return matched;
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
