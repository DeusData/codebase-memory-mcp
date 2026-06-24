/*
 * test_gitignore.c — Tests for gitignore-style pattern matching.
 *
 * RED phase: These tests define the expected pattern matching behavior.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "discover/discover.h"
#include <string.h> /* strdup (test seam) */

/* ── Basic pattern matching ────────────────────────────────────── */

TEST(gi_empty_pattern) {
    cbm_gitignore_t *gi = cbm_gitignore_parse("");
    ASSERT_NOT_NULL(gi);
    ASSERT_FALSE(cbm_gitignore_matches(gi, "foo.txt", false));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_exact_file) {
    cbm_gitignore_t *gi = cbm_gitignore_parse("secret.key\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "secret.key", false));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "other.key", false));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_wildcard_star) {
    cbm_gitignore_t *gi = cbm_gitignore_parse("*.log\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "error.log", false));
    ASSERT_TRUE(cbm_gitignore_matches(gi, "access.log", false));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "error.txt", false));
    /* Non-rooted pattern matches basename at any depth */
    ASSERT_TRUE(cbm_gitignore_matches(gi, "logs/error.log", false));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_double_star_prefix) {
    /* ** matches any number of directories */
    cbm_gitignore_t *gi = cbm_gitignore_parse("**/build\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "build", true));
    ASSERT_TRUE(cbm_gitignore_matches(gi, "src/build", true));
    ASSERT_TRUE(cbm_gitignore_matches(gi, "a/b/c/build", true));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_double_star_suffix) {
    cbm_gitignore_t *gi = cbm_gitignore_parse("logs/**\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "logs/debug.log", false));
    ASSERT_TRUE(cbm_gitignore_matches(gi, "logs/sub/trace.log", false));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "src/logs", false));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_double_star_middle) {
    cbm_gitignore_t *gi = cbm_gitignore_parse("a/**/b\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "a/b", false));
    ASSERT_TRUE(cbm_gitignore_matches(gi, "a/x/b", false));
    ASSERT_TRUE(cbm_gitignore_matches(gi, "a/x/y/z/b", false));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "c/a/b", false));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_directory_only) {
    /* Trailing slash means match directories only */
    cbm_gitignore_t *gi = cbm_gitignore_parse("build/\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "build", true));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "build", false)); /* not a directory */
    /* Should match anywhere in tree */
    ASSERT_TRUE(cbm_gitignore_matches(gi, "src/build", true));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_negation) {
    /* ! prefix negates a pattern */
    cbm_gitignore_t *gi = cbm_gitignore_parse("*.log\n!important.log\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "error.log", false));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "important.log", false));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_comment_and_blank) {
    cbm_gitignore_t *gi = cbm_gitignore_parse("# This is a comment\n\n*.tmp\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "data.tmp", false));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "data.txt", false));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_rooted_pattern) {
    /* Pattern with slash is anchored to the root */
    cbm_gitignore_t *gi = cbm_gitignore_parse("/build\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "build", true));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "src/build", true));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_path_with_slash) {
    /* Pattern containing / (not just leading) is rooted */
    cbm_gitignore_t *gi = cbm_gitignore_parse("doc/frotz\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "doc/frotz", false));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "src/doc/frotz", false));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_question_mark) {
    cbm_gitignore_t *gi = cbm_gitignore_parse("file?.txt\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "file1.txt", false));
    ASSERT_TRUE(cbm_gitignore_matches(gi, "fileA.txt", false));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "file12.txt", false));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_bracket_range) {
    cbm_gitignore_t *gi = cbm_gitignore_parse("file[0-9].txt\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "file3.txt", false));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "fileA.txt", false));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_multiple_patterns) {
    cbm_gitignore_t *gi = cbm_gitignore_parse("*.pyc\n"
                                              "__pycache__/\n"
                                              ".env\n"
                                              "*.log\n");
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "module.pyc", false));
    ASSERT_TRUE(cbm_gitignore_matches(gi, "__pycache__", true));
    ASSERT_TRUE(cbm_gitignore_matches(gi, ".env", false));
    ASSERT_TRUE(cbm_gitignore_matches(gi, "app.log", false));
    ASSERT_FALSE(cbm_gitignore_matches(gi, "main.py", false));
    cbm_gitignore_free(gi);
    PASS();
}

TEST(gi_null_safe_free) {
    cbm_gitignore_free(NULL); /* should not crash */
    PASS();
}

/* ── Load from file ────────────────────────────────────────────── */

TEST(gi_load_file) {
    char path[256]; snprintf(path, sizeof(path), "%s/test_gitignore_file", cbm_tmpdir());
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "*.o\nbuild/\n");
    fclose(f);

    cbm_gitignore_t *gi = cbm_gitignore_load(path);
    ASSERT_NOT_NULL(gi);
    ASSERT_TRUE(cbm_gitignore_matches(gi, "main.o", false));
    ASSERT_TRUE(cbm_gitignore_matches(gi, "build", true));
    cbm_gitignore_free(gi);
    remove(path);
    PASS();
}

TEST(gi_load_nonexistent) {
    char np[256];
    snprintf(np, sizeof(np), "%s/nonexistent_gitignore_12345", cbm_tmpdir());
    cbm_gitignore_t *gi = cbm_gitignore_load(np);
    ASSERT_NULL(gi);
    PASS();
}

/* ── Merge ─────────────────────────────────────────────────────── */

TEST(gi_merge_patterns) {
    cbm_gitignore_t *base_gi = cbm_gitignore_parse("*.log\n");
    cbm_gitignore_t *extra   = cbm_gitignore_parse("tmp/\nbuild/\n");
    ASSERT_NOT_NULL(base_gi);
    ASSERT_NOT_NULL(extra);

    cbm_gitignore_merge(base_gi, extra);
    cbm_gitignore_free(extra);

    ASSERT_TRUE(cbm_gitignore_matches(base_gi, "error.log", false));
    ASSERT_TRUE(cbm_gitignore_matches(base_gi, "tmp", true));
    ASSERT_TRUE(cbm_gitignore_matches(base_gi, "build", true));
    ASSERT_FALSE(cbm_gitignore_matches(base_gi, "main.go", false));

    cbm_gitignore_free(base_gi);
    PASS();
}

TEST(gi_merge_into_empty) {
    cbm_gitignore_t *dst = cbm_gitignore_parse("");
    cbm_gitignore_t *src = cbm_gitignore_parse("*.o\n");
    ASSERT_NOT_NULL(dst);
    ASSERT_NOT_NULL(src);

    cbm_gitignore_merge(dst, src);
    cbm_gitignore_free(src);

    ASSERT_TRUE(cbm_gitignore_matches(dst, "main.o", false));
    ASSERT_FALSE(cbm_gitignore_matches(dst, "main.c", false));

    cbm_gitignore_free(dst);
    PASS();
}

TEST(gi_merge_null_safe) {
    cbm_gitignore_t *gi = cbm_gitignore_parse("*.log\n");
    cbm_gitignore_merge(gi, NULL);  /* should not crash */
    cbm_gitignore_merge(NULL, gi);  /* should not crash */
    cbm_gitignore_free(gi);
    PASS();
}

/* Reproduce-first guard for #493: when an allocation fails mid-merge,
 * cbm_gitignore_merge must leave dst exactly as it was (atomic) and signal
 * failure — never a partial merge that keeps some src patterns and drops
 * others. The seam below injects a strdup failure on the 2nd src pattern.
 * Without the atomic rollback this test is RED: the first src pattern ("a/")
 * leaks into dst, so `matches(dst, "a", true)` is true. */
extern char *(*cbm_gitignore_merge_dup_hook_for_test)(const char *);

static int gi_dup_calls;
static int gi_dup_fail_at;
static char *gi_failing_dup(const char *s) {
    if (++gi_dup_calls > gi_dup_fail_at) {
        return NULL;
    }
    return strdup(s);
}

TEST(gi_merge_atomic_on_alloc_failure) {
    cbm_gitignore_t *dst = cbm_gitignore_parse("*.log\n");      /* 1 pattern */
    cbm_gitignore_t *src = cbm_gitignore_parse("a/\nb/\nc/\n"); /* 3 patterns */
    ASSERT_NOT_NULL(dst);
    ASSERT_NOT_NULL(src);

    gi_dup_calls = 0;
    gi_dup_fail_at = 1; /* first src pattern copies; second fails */
    cbm_gitignore_merge_dup_hook_for_test = gi_failing_dup;

    bool ok = cbm_gitignore_merge(dst, src);

    cbm_gitignore_merge_dup_hook_for_test = NULL; /* restore for other tests */

    ASSERT_FALSE(ok); /* failure is signalled, not silent */
    /* dst unchanged: its own pattern still matches, and the partially copied
     * src patterns were rolled back. */
    ASSERT_TRUE(cbm_gitignore_matches(dst, "x.log", false));
    ASSERT_FALSE(cbm_gitignore_matches(dst, "a", true));
    ASSERT_FALSE(cbm_gitignore_matches(dst, "b", true));

    cbm_gitignore_free(src);
    cbm_gitignore_free(dst);
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

SUITE(gitignore) {
    RUN_TEST(gi_empty_pattern);
    RUN_TEST(gi_exact_file);
    RUN_TEST(gi_wildcard_star);
    RUN_TEST(gi_double_star_prefix);
    RUN_TEST(gi_double_star_suffix);
    RUN_TEST(gi_double_star_middle);
    RUN_TEST(gi_directory_only);
    RUN_TEST(gi_negation);
    RUN_TEST(gi_comment_and_blank);
    RUN_TEST(gi_rooted_pattern);
    RUN_TEST(gi_path_with_slash);
    RUN_TEST(gi_question_mark);
    RUN_TEST(gi_bracket_range);
    RUN_TEST(gi_multiple_patterns);
    RUN_TEST(gi_null_safe_free);
    RUN_TEST(gi_load_file);
    RUN_TEST(gi_load_nonexistent);
    RUN_TEST(gi_merge_patterns);
    RUN_TEST(gi_merge_into_empty);
    RUN_TEST(gi_merge_null_safe);
    RUN_TEST(gi_merge_atomic_on_alloc_failure);
}
