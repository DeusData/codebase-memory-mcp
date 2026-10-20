#include "discover/gitignore_internal.h"

/* These recursive functions and annotations move unchanged in identity from
 * gitignore.c. A NULL guard preserves the legacy budget/fallback behavior. */
static bool glob_match(const char *pat, const char *str, int *budget,
                       gi_guard_t *guard); // NOLINT(misc-no-recursion)

static bool glob_match_doublestar_slash(const char *pat, const char *str, int *budget,
                                        gi_guard_t *guard) { // NOLINT(misc-no-recursion)
    if (glob_match(pat, str, budget, guard)) {
        return true;
    }
    for (const char *s = str;; s++) {
        if (!gi_work(guard, 1) || !*s) {
            return false;
        }
        if (*s == '/' && glob_match(pat, s + 1, budget, guard)) {
            return true;
        }
    }
}

static bool glob_match_doublestar_any(const char *pat, const char *str, int *budget,
                                      gi_guard_t *guard) { // NOLINT(misc-no-recursion)
    for (const char *s = str;; s++) {
        if (glob_match(pat, s, budget, guard)) {
            return true;
        }
        if (!gi_work(guard, 1) || !*s) {
            return false;
        }
    }
}

static bool glob_match_star(const char *pat, const char *str, int *budget,
                            gi_guard_t *guard) { // NOLINT(misc-no-recursion)
    for (const char *s = str;; s++) {
        if (glob_match(pat, s, budget, guard)) {
            return true;
        }
        if (!gi_work(guard, 1) || !*s || *s == '/') {
            return false;
        }
    }
}

static bool glob_match_charclass(const char *pat, char ch, const char **pat_out,
                                 gi_guard_t *guard) {
    bool negate_class = false;
    if (*pat == '!' || *pat == '^') {
        negate_class = true;
        pat++;
    }
    bool matched = false;
    char prev = 0;
    while (*pat && *pat != ']') {
        if (!gi_work(guard, 1)) {
            return false;
        }
        if (*pat == '-' && prev && pat[1] && pat[1] != ']') {
            pat++;
            if (!gi_work(guard, 1)) {
                return false;
            }
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

static bool glob_match_doublestar(const char *pat, const char *str, int *budget,
                                  gi_guard_t *guard) { // NOLINT(misc-no-recursion)
    if (pat[2] == '/') {
        return glob_match_doublestar_slash(pat + 3, str, budget, guard);
    }
    if (pat[2] == '\0') {
        return true;
    }
    return glob_match_doublestar_any(pat + 2, str, budget, guard);
}

static bool glob_match(const char *pat, const char *str, int *budget,
                       gi_guard_t *guard) { // NOLINT(misc-no-recursion)
    if (!gi_work(guard, 1)) {
        return false;
    }
    if (--*budget <= 0) {
        return gi_fail(guard, CBM_IGNORE_CHECKED_LIMIT, CBM_IGNORE_CAP_ATTEMPT);
    }
    if (guard) {
        if (guard->depth == guard->owner->limits.max_depth) {
            return gi_fail(guard, CBM_IGNORE_CHECKED_LIMIT, CBM_IGNORE_CAP_DEPTH);
        }
        guard->depth++;
    }
    bool matched = false;
    while (*pat && *str) {
        if (!gi_work(guard, 1)) {
            goto done;
        }
        if (pat[0] == '*' && pat[1] == '*') {
            matched = glob_match_doublestar(pat, str, budget, guard);
            goto done;
        }
        if (*pat == '*') {
            matched = glob_match_star(pat + 1, str, budget, guard);
            goto done;
        }
        if (*pat == '?') {
            if (*str == '/') {
                goto done;
            }
            pat++;
            str++;
            continue;
        }
        if (*pat == '[') {
            const char *next = NULL;
            if (*str == '/' || !glob_match_charclass(pat + 1, *str, &next, guard)) {
                goto done;
            }
            pat = next;
            str++;
            continue;
        }
        if (*pat != *str) {
            goto done;
        }
        pat++;
        str++;
    }
    while (*pat == '*') {
        if (!gi_work(guard, 1)) {
            goto done;
        }
        pat++;
    }
    matched = *pat == '\0' && *str == '\0';
done:
    if (guard) {
        guard->depth--;
    }
    return matched;
}

static bool glob_match_bounded(const char *pat, const char *str, gi_guard_t *guard) {
    int budget = GI_MATCH_MAX_STEPS;
    return gi_work(guard, 1) && glob_match(pat, str, &budget, guard);
}

static const char *gi_slash(const char *path, gi_guard_t *guard) {
    for (;;) {
        if (!gi_work(guard, 1)) {
            return NULL;
        }
        if (*path == '/') {
            return path;
        }
        if (!*path) {
            return NULL;
        }
        path++;
    }
}

static const char *gi_basename(const char *path, gi_guard_t *guard) {
    const char *basename = path;
    for (;;) {
        if (!gi_work(guard, 1)) {
            return NULL;
        }
        if (!*path) {
            return basename;
        }
        if (*path == '/') {
            basename = path + 1;
        }
        path++;
    }
}

static bool match_unrooted(const char *pattern, const char *path, const char *basename,
                           gi_guard_t *guard) {
    if (glob_match_bounded(pattern, basename, guard)) {
        return true;
    }
    if (!gi_slash(path, guard)) {
        return false;
    }
    const char *suffix = path;
    while (*suffix) {
        if (glob_match_bounded(pattern, suffix, guard)) {
            return true;
        }
        const char *next = gi_slash(suffix, guard);
        if (!next) {
            break;
        }
        suffix = next + 1;
    }
    return false;
}

int gi_core_match(const gi_pattern_t *rows, size_t count, const char *path, bool is_directory,
                  gi_guard_t *guard) {
    const char *basename = gi_basename(path, guard);
    if (!basename) {
        return 0;
    }
    int matched = 0;
    for (size_t i = 0; i < count; i++) {
        if (!gi_work(guard, 1)) {
            return 0;
        }
        const gi_pattern_t *pattern = &rows[i];
        if (pattern->dir_only && !is_directory) {
            continue;
        }
        bool this_match = pattern->rooted ? glob_match_bounded(pattern->pattern, path, guard)
                                          : match_unrooted(pattern->pattern, path, basename, guard);
        if (!gi_active(guard)) {
            return 0;
        }
        if (this_match) {
            matched = pattern->negated ? -1 : 1;
        }
    }
    return matched;
}
