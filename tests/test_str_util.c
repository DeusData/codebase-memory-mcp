/*
 * test_str_util.c — RED phase tests for foundation/str_util.
 */
#include "test_framework.h"
#include "../src/foundation/str_util.h"

static CBMArena a;

static void setup(void) {
    cbm_arena_init(&a);
}
static void teardown(void) {
    cbm_arena_destroy(&a);
}

TEST(path_join_basic) {
    setup();
    ASSERT_STR_EQ(cbm_path_join(&a, "src", "main.c"), "src/main.c");
    teardown();
    PASS();
}

TEST(path_join_trailing_slash) {
    setup();
    ASSERT_STR_EQ(cbm_path_join(&a, "src/", "main.c"), "src/main.c");
    teardown();
    PASS();
}

TEST(path_join_leading_slash) {
    setup();
    ASSERT_STR_EQ(cbm_path_join(&a, "src", "/main.c"), "src/main.c");
    teardown();
    PASS();
}

TEST(path_join_empty_base) {
    setup();
    ASSERT_STR_EQ(cbm_path_join(&a, "", "main.c"), "main.c");
    teardown();
    PASS();
}

TEST(path_join_empty_name) {
    setup();
    ASSERT_STR_EQ(cbm_path_join(&a, "src", ""), "src");
    teardown();
    PASS();
}

TEST(path_join_n) {
    setup();
    const char *parts[] = {"a", "b", "c", "d.txt"};
    ASSERT_STR_EQ(cbm_path_join_n(&a, parts, 4), "a/b/c/d.txt");
    teardown();
    PASS();
}

TEST(path_ext) {
    ASSERT_STR_EQ(cbm_path_ext("foo.go"), "go");
    ASSERT_STR_EQ(cbm_path_ext("foo.tar.gz"), "gz");
    ASSERT_STR_EQ(cbm_path_ext("Makefile"), "");
    ASSERT_STR_EQ(cbm_path_ext(".gitignore"), "gitignore");
    PASS();
}

TEST(path_base) {
    ASSERT_STR_EQ(cbm_path_base("src/main.c"), "main.c");
    ASSERT_STR_EQ(cbm_path_base("main.c"), "main.c");
    ASSERT_STR_EQ(cbm_path_base("/a/b/c"), "c");
    PASS();
}

TEST(path_dir) {
    setup();
    ASSERT_STR_EQ(cbm_path_dir(&a, "src/main.c"), "src");
    ASSERT_STR_EQ(cbm_path_dir(&a, "main.c"), ".");
    ASSERT_STR_EQ(cbm_path_dir(&a, "/a/b/c"), "/a/b");
    teardown();
    PASS();
}

TEST(str_starts_with) {
    ASSERT_TRUE(cbm_str_starts_with("hello world", "hello"));
    ASSERT_FALSE(cbm_str_starts_with("hello", "hello world"));
    ASSERT_TRUE(cbm_str_starts_with("hello", ""));
    ASSERT_TRUE(cbm_str_starts_with("hello", "hello"));
    PASS();
}

TEST(str_ends_with) {
    ASSERT_TRUE(cbm_str_ends_with("hello world", "world"));
    ASSERT_FALSE(cbm_str_ends_with("hello", "hello world"));
    ASSERT_TRUE(cbm_str_ends_with("hello", ""));
    ASSERT_TRUE(cbm_str_ends_with("hello", "hello"));
    PASS();
}

TEST(str_contains) {
    ASSERT_TRUE(cbm_str_contains("hello world", "lo wo"));
    ASSERT_FALSE(cbm_str_contains("hello", "xyz"));
    ASSERT_TRUE(cbm_str_contains("hello", ""));
    PASS();
}

TEST(str_tolower) {
    setup();
    ASSERT_STR_EQ(cbm_str_tolower(&a, "Hello World"), "hello world");
    ASSERT_STR_EQ(cbm_str_tolower(&a, "already"), "already");
    ASSERT_STR_EQ(cbm_str_tolower(&a, ""), "");
    teardown();
    PASS();
}

TEST(str_replace_char) {
    setup();
    ASSERT_STR_EQ(cbm_str_replace_char(&a, "a/b/c", '/', '.'), "a.b.c");
    ASSERT_STR_EQ(cbm_str_replace_char(&a, "no-change", '/', '.'), "no-change");
    teardown();
    PASS();
}

TEST(str_strip_ext) {
    setup();
    ASSERT_STR_EQ(cbm_str_strip_ext(&a, "foo.go"), "foo");
    ASSERT_STR_EQ(cbm_str_strip_ext(&a, "foo.tar.gz"), "foo.tar");
    ASSERT_STR_EQ(cbm_str_strip_ext(&a, "Makefile"), "Makefile");
    teardown();
    PASS();
}

TEST(str_split) {
    setup();
    int count = 0;
    char **parts = cbm_str_split(&a, "a/b/c/d", '/', &count);
    ASSERT_EQ(count, 4);
    ASSERT_STR_EQ(parts[0], "a");
    ASSERT_STR_EQ(parts[1], "b");
    ASSERT_STR_EQ(parts[2], "c");
    ASSERT_STR_EQ(parts[3], "d");
    teardown();
    PASS();
}

TEST(str_split_empty) {
    setup();
    int count = 0;
    char **parts = cbm_str_split(&a, "", '/', &count);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(parts[0], "");
    teardown();
    PASS();
}

TEST(str_split_no_delim) {
    setup();
    int count = 0;
    char **parts = cbm_str_split(&a, "hello", '/', &count);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(parts[0], "hello");
    teardown();
    PASS();
}

/* ── NULL safety tests ─────────────────────────────────────────── */

TEST(path_join_null_base) {
    setup();
    ASSERT_NULL(cbm_path_join(&a, NULL, "main.c"));
    teardown();
    PASS();
}

TEST(path_join_null_name) {
    setup();
    ASSERT_NULL(cbm_path_join(&a, "src", NULL));
    teardown();
    PASS();
}

TEST(path_join_both_null) {
    setup();
    ASSERT_NULL(cbm_path_join(&a, NULL, NULL));
    teardown();
    PASS();
}

TEST(path_dir_null) {
    setup();
    ASSERT_STR_EQ(cbm_path_dir(&a, NULL), ".");
    teardown();
    PASS();
}

TEST(str_starts_with_null_s) {
    ASSERT_FALSE(cbm_str_starts_with(NULL, "hello"));
    PASS();
}

TEST(str_starts_with_null_prefix) {
    ASSERT_FALSE(cbm_str_starts_with("hello", NULL));
    PASS();
}

TEST(str_ends_with_null) {
    ASSERT_FALSE(cbm_str_ends_with(NULL, "world"));
    ASSERT_FALSE(cbm_str_ends_with("world", NULL));
    PASS();
}

TEST(str_contains_null) {
    ASSERT_FALSE(cbm_str_contains(NULL, "sub"));
    ASSERT_FALSE(cbm_str_contains("hello", NULL));
    PASS();
}

TEST(str_tolower_null) {
    setup();
    ASSERT_NULL(cbm_str_tolower(&a, NULL));
    teardown();
    PASS();
}

TEST(str_replace_char_null) {
    setup();
    ASSERT_NULL(cbm_str_replace_char(&a, NULL, 'a', 'b'));
    teardown();
    PASS();
}

TEST(str_strip_ext_null) {
    setup();
    ASSERT_NULL(cbm_str_strip_ext(&a, NULL));
    teardown();
    PASS();
}

TEST(str_split_null_string) {
    setup();
    int count = 0;
    ASSERT_NULL(cbm_str_split(&a, NULL, '/', &count));
    teardown();
    PASS();
}

TEST(str_split_null_out_count) {
    setup();
    ASSERT_NULL(cbm_str_split(&a, "a/b", '/', NULL));
    teardown();
    PASS();
}

/* ── path_join edge cases ─────────────────────────────────────── */

TEST(path_join_multi_slashes) {
    setup();
    ASSERT_STR_EQ(cbm_path_join(&a, "src///", "///main.c"), "src/main.c");
    teardown();
    PASS();
}

TEST(path_join_only_slash_base) {
    setup();
    ASSERT_STR_EQ(cbm_path_join(&a, "/", "main.c"), "main.c");
    teardown();
    PASS();
}

TEST(path_join_long_paths) {
    setup();
    char long_base[512];
    char long_name[512];
    memset(long_base, 'a', 511);
    long_base[511] = '\0';
    memset(long_name, 'b', 511);
    long_name[511] = '\0';
    char *result = cbm_path_join(&a, long_base, long_name);
    ASSERT_NOT_NULL(result);
    ASSERT_TRUE(cbm_str_starts_with(result, "aaa"));
    ASSERT_TRUE(cbm_str_ends_with(result, "bbb"));
    ASSERT_TRUE(cbm_str_contains(result, "/"));
    ASSERT_EQ(strlen(result), 511 + 1 + 511);
    teardown();
    PASS();
}

TEST(path_join_n_zero) {
    setup();
    ASSERT_STR_EQ(cbm_path_join_n(&a, NULL, 0), "");
    teardown();
    PASS();
}

TEST(path_join_n_null_parts) {
    setup();
    ASSERT_STR_EQ(cbm_path_join_n(&a, NULL, 3), "");
    teardown();
    PASS();
}

/* ── path_ext edge cases ──────────────────────────────────────── */

TEST(path_ext_null) {
    ASSERT_STR_EQ(cbm_path_ext(NULL), "");
    PASS();
}

TEST(path_ext_dot_directory) {
    ASSERT_STR_EQ(cbm_path_ext("dir.name/file"), "");
    PASS();
}

TEST(path_ext_hidden_file) {
    ASSERT_STR_EQ(cbm_path_ext(".gitignore"), "gitignore");
    PASS();
}

TEST(path_ext_no_extension) {
    ASSERT_STR_EQ(cbm_path_ext("Makefile"), "");
    PASS();
}

TEST(path_ext_multiple_dots) {
    ASSERT_STR_EQ(cbm_path_ext("file.test.spec.ts"), "ts");
    PASS();
}

TEST(path_ext_trailing_dot) {
    ASSERT_STR_EQ(cbm_path_ext("file."), "");
    PASS();
}

/* ── path_base edge cases ─────────────────────────────────────── */

TEST(path_base_null) {
    ASSERT_STR_EQ(cbm_path_base(NULL), "");
    PASS();
}

TEST(path_base_empty) {
    ASSERT_STR_EQ(cbm_path_base(""), "");
    PASS();
}

TEST(path_base_just_filename) {
    ASSERT_STR_EQ(cbm_path_base("hello.txt"), "hello.txt");
    PASS();
}

TEST(path_base_trailing_slash) {
    ASSERT_STR_EQ(cbm_path_base("dir/"), "");
    PASS();
}

/* ── validate_shell_arg tests ─────────────────────────────────── */

TEST(validate_shell_arg_null) {
    ASSERT_FALSE(cbm_validate_shell_arg(NULL));
    PASS();
}

TEST(validate_shell_arg_safe) {
    ASSERT_TRUE(cbm_validate_shell_arg("hello-world_123"));
    PASS();
}

TEST(validate_shell_arg_single_quote) {
    ASSERT_FALSE(cbm_validate_shell_arg("it's bad"));
    PASS();
}

TEST(validate_shell_arg_semicolon) {
    ASSERT_FALSE(cbm_validate_shell_arg("cmd; rm -rf"));
    PASS();
}

TEST(validate_shell_arg_pipe) {
    ASSERT_FALSE(cbm_validate_shell_arg("cmd | evil"));
    PASS();
}

TEST(validate_shell_arg_ampersand) {
    ASSERT_FALSE(cbm_validate_shell_arg("cmd & bg"));
    PASS();
}

TEST(validate_shell_arg_dollar) {
    ASSERT_FALSE(cbm_validate_shell_arg("$HOME"));
    PASS();
}

TEST(validate_shell_arg_backtick) {
    ASSERT_FALSE(cbm_validate_shell_arg("`whoami`"));
    PASS();
}

TEST(validate_shell_arg_newline) {
    ASSERT_FALSE(cbm_validate_shell_arg("line1\nline2"));
    PASS();
}

TEST(validate_shell_arg_carriage_return) {
    ASSERT_FALSE(cbm_validate_shell_arg("line1\rline2"));
    PASS();
}

TEST(validate_shell_arg_backslash) {
#ifdef _WIN32
    /* Backslash is allowed on Windows (path separator) */
    ASSERT_TRUE(cbm_validate_shell_arg("path\\to\\file"));
#else
    ASSERT_FALSE(cbm_validate_shell_arg("path\\to\\file"));
#endif
    PASS();
}

TEST(validate_shell_arg_empty) {
    ASSERT_TRUE(cbm_validate_shell_arg(""));
    PASS();
}

TEST(validate_shell_arg_spaces) {
    ASSERT_TRUE(cbm_validate_shell_arg("hello world with spaces"));
    PASS();
}

/* ── SNPRINTF_APPEND tests ────────────────────────────────────── */

TEST(snprintf_append_basic) {
    char buf[64];
    int off = 0;
    CBM_SNPRINTF_APPEND(buf, sizeof(buf), off, "hello");
    CBM_SNPRINTF_APPEND(buf, sizeof(buf), off, " world");
    ASSERT_STR_EQ(buf, "hello world");
    ASSERT_EQ(off, 11);
    PASS();
}

TEST(snprintf_append_fills_exactly) {
    char buf[6];
    int off = 0;
    CBM_SNPRINTF_APPEND(buf, sizeof(buf), off, "hello");
    ASSERT_STR_EQ(buf, "hello");
    ASSERT_EQ(off, 5);
    PASS();
}

TEST(snprintf_append_overflow) {
    char buf[8];
    int off = 0;
    CBM_SNPRINTF_APPEND(buf, sizeof(buf), off, "hello world this is way too long");
    /* offset clamped to sizeof(buf) - 1 */
    ASSERT_EQ(off, 7);
    /* buffer is null-terminated and truncated */
    ASSERT_EQ(strlen(buf), 7);
    PASS();
}

TEST(snprintf_append_multiple_sequential) {
    char buf[32];
    int off = 0;
    CBM_SNPRINTF_APPEND(buf, sizeof(buf), off, "a=%d", 1);
    CBM_SNPRINTF_APPEND(buf, sizeof(buf), off, " b=%d", 2);
    CBM_SNPRINTF_APPEND(buf, sizeof(buf), off, " c=%d", 3);
    CBM_SNPRINTF_APPEND(buf, sizeof(buf), off, " d=%d", 4);
    ASSERT_STR_EQ(buf, "a=1 b=2 c=3 d=4");
    ASSERT_EQ(off, 15);
    PASS();
}

SUITE(str_util) {
    RUN_TEST(path_join_basic);
    RUN_TEST(path_join_trailing_slash);
    RUN_TEST(path_join_leading_slash);
    RUN_TEST(path_join_empty_base);
    RUN_TEST(path_join_empty_name);
    RUN_TEST(path_join_n);
    RUN_TEST(path_ext);
    RUN_TEST(path_base);
    RUN_TEST(path_dir);
    RUN_TEST(str_starts_with);
    RUN_TEST(str_ends_with);
    RUN_TEST(str_contains);
    RUN_TEST(str_tolower);
    RUN_TEST(str_replace_char);
    RUN_TEST(str_strip_ext);
    RUN_TEST(str_split);
    RUN_TEST(str_split_empty);
    RUN_TEST(str_split_no_delim);
    /* NULL safety */
    RUN_TEST(path_join_null_base);
    RUN_TEST(path_join_null_name);
    RUN_TEST(path_join_both_null);
    RUN_TEST(path_dir_null);
    RUN_TEST(str_starts_with_null_s);
    RUN_TEST(str_starts_with_null_prefix);
    RUN_TEST(str_ends_with_null);
    RUN_TEST(str_contains_null);
    RUN_TEST(str_tolower_null);
    RUN_TEST(str_replace_char_null);
    RUN_TEST(str_strip_ext_null);
    RUN_TEST(str_split_null_string);
    RUN_TEST(str_split_null_out_count);
    /* path_join edge cases */
    RUN_TEST(path_join_multi_slashes);
    RUN_TEST(path_join_only_slash_base);
    RUN_TEST(path_join_long_paths);
    RUN_TEST(path_join_n_zero);
    RUN_TEST(path_join_n_null_parts);
    /* path_ext edge cases */
    RUN_TEST(path_ext_null);
    RUN_TEST(path_ext_dot_directory);
    RUN_TEST(path_ext_hidden_file);
    RUN_TEST(path_ext_no_extension);
    RUN_TEST(path_ext_multiple_dots);
    RUN_TEST(path_ext_trailing_dot);
    /* path_base edge cases */
    RUN_TEST(path_base_null);
    RUN_TEST(path_base_empty);
    RUN_TEST(path_base_just_filename);
    RUN_TEST(path_base_trailing_slash);
    /* validate_shell_arg */
    RUN_TEST(validate_shell_arg_null);
    RUN_TEST(validate_shell_arg_safe);
    RUN_TEST(validate_shell_arg_single_quote);
    RUN_TEST(validate_shell_arg_semicolon);
    RUN_TEST(validate_shell_arg_pipe);
    RUN_TEST(validate_shell_arg_ampersand);
    RUN_TEST(validate_shell_arg_dollar);
    RUN_TEST(validate_shell_arg_backtick);
    RUN_TEST(validate_shell_arg_newline);
    RUN_TEST(validate_shell_arg_carriage_return);
    RUN_TEST(validate_shell_arg_backslash);
    RUN_TEST(validate_shell_arg_empty);
    RUN_TEST(validate_shell_arg_spaces);
    /* SNPRINTF_APPEND */
    RUN_TEST(snprintf_append_basic);
    RUN_TEST(snprintf_append_fills_exactly);
    RUN_TEST(snprintf_append_overflow);
    RUN_TEST(snprintf_append_multiple_sequential);
}
