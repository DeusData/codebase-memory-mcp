/*
 * test_yaml.c — Tests for foundation/yaml YAML parser.
 */
#include "test_framework.h"
#include "../src/foundation/yaml.h"

/* ── Parsing: NULL and empty input ─────────────────────────────── */

TEST(yaml_parse_null_input) {
    cbm_yaml_node_t *root = cbm_yaml_parse(NULL, 0);
    ASSERT_NOT_NULL(root); /* returns empty map on NULL */
    ASSERT_FALSE(cbm_yaml_has(root, "anything"));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_parse_empty_string) {
    cbm_yaml_node_t *root = cbm_yaml_parse("", 0);
    ASSERT_NOT_NULL(root);
    ASSERT_FALSE(cbm_yaml_has(root, "key"));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_parse_negative_len) {
    cbm_yaml_node_t *root = cbm_yaml_parse("key: val", -1);
    ASSERT_NOT_NULL(root);
    /* Negative len treated as empty */
    ASSERT_FALSE(cbm_yaml_has(root, "key"));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_free_null) {
    /* Must not crash */
    cbm_yaml_free(NULL);
    PASS();
}

/* ── Parsing: single key-value pair ────────────────────────────── */

TEST(yaml_single_kv) {
    const char *yaml = "name: hello";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "name"), "hello");
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_single_kv_trailing_newline) {
    const char *yaml = "name: hello\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "name"), "hello");
    cbm_yaml_free(root);
    PASS();
}

/* ── Parsing: multiple key-value pairs ─────────────────────────── */

TEST(yaml_multiple_kv) {
    const char *yaml =
        "name: myproject\n"
        "version: 1.2.3\n"
        "author: someone\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "name"), "myproject");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "version"), "1.2.3");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "author"), "someone");
    cbm_yaml_free(root);
    PASS();
}

/* ── Parsing: nested maps ──────────────────────────────────────── */

TEST(yaml_nested_map_2_levels) {
    const char *yaml =
        "database:\n"
        "  host: localhost\n"
        "  port: 5432\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "database.host"), "localhost");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "database.port"), "5432");
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_nested_map_3_levels) {
    const char *yaml =
        "level1:\n"
        "  level2:\n"
        "    level3: deep_value\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "level1.level2.level3"), "deep_value");
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_nested_siblings) {
    const char *yaml =
        "server:\n"
        "  host: 0.0.0.0\n"
        "  port: 8080\n"
        "database:\n"
        "  host: db.local\n"
        "  port: 3306\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "server.host"), "0.0.0.0");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "server.port"), "8080");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "database.host"), "db.local");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "database.port"), "3306");
    cbm_yaml_free(root);
    PASS();
}

/* ── Parsing: string lists ─────────────────────────────────────── */

TEST(yaml_string_list) {
    const char *yaml =
        "fruits:\n"
        "  - apple\n"
        "  - banana\n"
        "  - cherry\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    const char *items[8];
    int count = cbm_yaml_get_str_list(root, "fruits", items, 8);
    ASSERT_EQ(count, 3);
    ASSERT_STR_EQ(items[0], "apple");
    ASSERT_STR_EQ(items[1], "banana");
    ASSERT_STR_EQ(items[2], "cherry");
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_list_max_out_limit) {
    const char *yaml =
        "items:\n"
        "  - a\n"
        "  - b\n"
        "  - c\n"
        "  - d\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    const char *items[2];
    int count = cbm_yaml_get_str_list(root, "items", items, 2);
    ASSERT_EQ(count, 2);
    ASSERT_STR_EQ(items[0], "a");
    ASSERT_STR_EQ(items[1], "b");
    cbm_yaml_free(root);
    PASS();
}

/* ── Parsing: comments ─────────────────────────────────────────── */

TEST(yaml_comment_only) {
    const char *yaml =
        "# This is a comment\n"
        "# Another comment\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_FALSE(cbm_yaml_has(root, "#"));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_inline_comment) {
    const char *yaml = "name: hello # this is a comment\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "name"), "hello");
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_comment_between_keys) {
    const char *yaml =
        "a: 1\n"
        "# skip me\n"
        "b: 2\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "a"), "1");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "b"), "2");
    cbm_yaml_free(root);
    PASS();
}

/* ── Parsing: mixed maps and lists ─────────────────────────────── */

TEST(yaml_mixed_maps_and_lists) {
    const char *yaml =
        "project:\n"
        "  name: myapp\n"
        "  tags:\n"
        "    - web\n"
        "    - api\n"
        "  version: 2.0\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "project.name"), "myapp");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "project.version"), "2.0");
    const char *tags[4];
    int count = cbm_yaml_get_str_list(root, "project.tags", tags, 4);
    ASSERT_EQ(count, 2);
    ASSERT_STR_EQ(tags[0], "web");
    ASSERT_STR_EQ(tags[1], "api");
    cbm_yaml_free(root);
    PASS();
}

/* ── Parsing: colons in values ─────────────────────────────────── */

TEST(yaml_url_value) {
    /* Only the first colon is the separator */
    const char *yaml = "url: https://example.com:8080/path\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "url"), "https://example.com:8080/path");
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_multiple_colons) {
    const char *yaml = "time: 12:30:45\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "time"), "12:30:45");
    cbm_yaml_free(root);
    PASS();
}

/* ── Parsing: empty values ─────────────────────────────────────── */

TEST(yaml_empty_value_becomes_map) {
    /* "key:" with nothing after becomes a map node (if next line isn't "- ...") */
    const char *yaml =
        "parent:\n"
        "  child: val\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_has(root, "parent"));
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "parent.child"), "val");
    /* parent itself is a map, not a scalar */
    ASSERT_NULL(cbm_yaml_get_str(root, "parent"));
    cbm_yaml_free(root);
    PASS();
}

/* ── Query: get_str ────────────────────────────────────────────── */

TEST(yaml_get_str_scalar) {
    const char *yaml = "key: value\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "key"), "value");
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_str_missing) {
    const char *yaml = "key: value\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_NULL(cbm_yaml_get_str(root, "nonexistent"));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_str_nested) {
    const char *yaml =
        "a:\n"
        "  b:\n"
        "    c: found\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "a.b.c"), "found");
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_str_on_map_node) {
    /* Querying a map node (not scalar) returns NULL */
    const char *yaml =
        "group:\n"
        "  key: val\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_NULL(cbm_yaml_get_str(root, "group"));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_str_null_root) {
    ASSERT_NULL(cbm_yaml_get_str(NULL, "key"));
    PASS();
}

TEST(yaml_get_str_null_path) {
    const char *yaml = "key: val\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_NULL(cbm_yaml_get_str(root, NULL));
    cbm_yaml_free(root);
    PASS();
}

/* ── Query: get_float ──────────────────────────────────────────── */

TEST(yaml_get_float_valid) {
    const char *yaml = "confidence: 0.85\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_FLOAT_EQ(cbm_yaml_get_float(root, "confidence", -1.0), 0.85, 0.001);
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_float_integer) {
    const char *yaml = "count: 42\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_FLOAT_EQ(cbm_yaml_get_float(root, "count", -1.0), 42.0, 0.001);
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_float_negative) {
    const char *yaml = "offset: -3.14\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_FLOAT_EQ(cbm_yaml_get_float(root, "offset", 0.0), -3.14, 0.001);
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_float_invalid_string) {
    const char *yaml = "val: not_a_number\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_FLOAT_EQ(cbm_yaml_get_float(root, "val", 99.0), 99.0, 0.001);
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_float_missing_key) {
    const char *yaml = "a: 1\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_FLOAT_EQ(cbm_yaml_get_float(root, "missing", 77.7), 77.7, 0.001);
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_float_zero) {
    const char *yaml = "val: 0.0\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_FLOAT_EQ(cbm_yaml_get_float(root, "val", -1.0), 0.0, 0.001);
    cbm_yaml_free(root);
    PASS();
}

/* ── Query: get_bool ───────────────────────────────────────────── */

TEST(yaml_get_bool_true_false) {
    const char *yaml =
        "enabled: true\n"
        "disabled: false\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_get_bool(root, "enabled", false));
    ASSERT_FALSE(cbm_yaml_get_bool(root, "disabled", true));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_bool_yes_no) {
    const char *yaml =
        "feature_a: yes\n"
        "feature_b: no\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_get_bool(root, "feature_a", false));
    ASSERT_FALSE(cbm_yaml_get_bool(root, "feature_b", true));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_bool_on_off) {
    const char *yaml =
        "logging: on\n"
        "debug: off\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_get_bool(root, "logging", false));
    ASSERT_FALSE(cbm_yaml_get_bool(root, "debug", true));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_bool_1_0) {
    const char *yaml =
        "flag_on: 1\n"
        "flag_off: 0\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_get_bool(root, "flag_on", false));
    ASSERT_FALSE(cbm_yaml_get_bool(root, "flag_off", true));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_bool_case_insensitive) {
    const char *yaml =
        "a: TRUE\n"
        "b: False\n"
        "c: YES\n"
        "d: No\n"
        "e: ON\n"
        "f: Off\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_get_bool(root, "a", false));
    ASSERT_FALSE(cbm_yaml_get_bool(root, "b", true));
    ASSERT_TRUE(cbm_yaml_get_bool(root, "c", false));
    ASSERT_FALSE(cbm_yaml_get_bool(root, "d", true));
    ASSERT_TRUE(cbm_yaml_get_bool(root, "e", false));
    ASSERT_FALSE(cbm_yaml_get_bool(root, "f", true));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_bool_missing_returns_default) {
    const char *yaml = "a: 1\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_get_bool(root, "missing", true));
    ASSERT_FALSE(cbm_yaml_get_bool(root, "missing", false));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_bool_unrecognized_returns_default) {
    const char *yaml = "val: maybe\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_get_bool(root, "val", true));
    ASSERT_FALSE(cbm_yaml_get_bool(root, "val", false));
    cbm_yaml_free(root);
    PASS();
}

/* ── Query: get_str_list ───────────────────────────────────────── */

TEST(yaml_get_str_list_non_list_path) {
    const char *yaml = "scalar: hello\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    const char *items[4];
    int count = cbm_yaml_get_str_list(root, "scalar", items, 4);
    ASSERT_EQ(count, 0);
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_str_list_missing_path) {
    const char *yaml = "key: val\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    const char *items[4];
    int count = cbm_yaml_get_str_list(root, "nonexistent", items, 4);
    ASSERT_EQ(count, 0);
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_get_str_list_null_root) {
    const char *items[4];
    int count = cbm_yaml_get_str_list(NULL, "key", items, 4);
    ASSERT_EQ(count, 0);
    PASS();
}

/* ── Query: has() ──────────────────────────────────────────────── */

TEST(yaml_has_existing) {
    const char *yaml = "key: value\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_has(root, "key"));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_has_missing) {
    const char *yaml = "key: value\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_FALSE(cbm_yaml_has(root, "nope"));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_has_nested) {
    const char *yaml =
        "a:\n"
        "  b: val\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_TRUE(cbm_yaml_has(root, "a"));
    ASSERT_TRUE(cbm_yaml_has(root, "a.b"));
    ASSERT_FALSE(cbm_yaml_has(root, "a.c"));
    ASSERT_FALSE(cbm_yaml_has(root, "a.b.c"));
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_has_null_root) {
    ASSERT_FALSE(cbm_yaml_has(NULL, "key"));
    PASS();
}

/* ── Edge cases: long values ───────────────────────────────────── */

TEST(yaml_long_value) {
    char yaml[1200];
    char expected[1025];
    memset(expected, 'x', 1024);
    expected[1024] = '\0';
    int n = snprintf(yaml, sizeof(yaml), "longkey: %s\n", expected);
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, n);
    ASSERT_NOT_NULL(root);
    const char *val = cbm_yaml_get_str(root, "longkey");
    ASSERT_NOT_NULL(val);
    ASSERT_EQ((int)strlen(val), 1024);
    ASSERT_STR_EQ(val, expected);
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: special characters ────────────────────────────── */

TEST(yaml_special_chars_in_value) {
    const char *yaml = "pattern: [a-z]+(foo|bar)*\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "pattern"), "[a-z]+(foo|bar)*");
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_equals_in_value) {
    const char *yaml = "query: SELECT * FROM t WHERE x=1\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "query"), "SELECT * FROM t WHERE x=1");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: deeply nested (4+ levels) ─────────────────────── */

TEST(yaml_deeply_nested) {
    const char *yaml =
        "l1:\n"
        "  l2:\n"
        "    l3:\n"
        "      l4:\n"
        "        l5: bottom\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "l1.l2.l3.l4.l5"), "bottom");
    /* Intermediate nodes exist but are not scalars */
    ASSERT_TRUE(cbm_yaml_has(root, "l1.l2.l3.l4"));
    ASSERT_NULL(cbm_yaml_get_str(root, "l1.l2.l3.l4"));
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: non-existent intermediate path ────────────────── */

TEST(yaml_missing_intermediate) {
    const char *yaml = "a: 1\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_NULL(cbm_yaml_get_str(root, "x.y.z"));
    ASSERT_FALSE(cbm_yaml_has(root, "x.y.z"));
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: tab characters ────────────────────────────────── */

TEST(yaml_tab_not_indentation) {
    /* Tabs are not treated as indentation by leading_spaces() */
    const char *yaml =
        "key1: val1\n"
        "\tkey2: val2\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "key1"), "val1");
    /* tab-indented line has 0 leading spaces, parsed as top-level */
    /* The key will be "key2" after trim_dup strips the tab from the key */
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: carriage return ───────────────────────────────── */

TEST(yaml_crlf_line_endings) {
    const char *yaml = "name: hello\r\nversion: 1.0\r\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "name"), "hello");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "version"), "1.0");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: quoted strings with # ─────────────────────────── */

TEST(yaml_quoted_string_with_hash) {
    /* Quoted strings: inline comment stripping is skipped for quoted values */
    const char *yaml = "color: \"#ff0000\"\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    const char *val = cbm_yaml_get_str(root, "color");
    ASSERT_NOT_NULL(val);
    /* Parser preserves quotes in value since it doesn't strip them */
    ASSERT_STR_EQ(val, "\"#ff0000\"");
    cbm_yaml_free(root);
    PASS();
}

TEST(yaml_single_quoted_with_hash) {
    const char *yaml = "regex: '# not a comment'\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    const char *val = cbm_yaml_get_str(root, "regex");
    ASSERT_NOT_NULL(val);
    /* Single-quoted values are also preserved with quotes */
    ASSERT_STR_EQ(val, "'# not a comment'");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: empty lines between content ───────────────────── */

TEST(yaml_empty_lines_between_keys) {
    const char *yaml =
        "a: 1\n"
        "\n"
        "\n"
        "b: 2\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "a"), "1");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "b"), "2");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: whitespace-only value ─────────────────────────── */

TEST(yaml_whitespace_after_colon) {
    /* "key:   " with only spaces after colon -> empty value -> map node */
    const char *yaml = "bare:   \n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    /* after_len is 0 after trimming spaces, so this becomes a map/list node */
    ASSERT_TRUE(cbm_yaml_has(root, "bare"));
    /* Not a scalar, so get_str returns NULL */
    ASSERT_NULL(cbm_yaml_get_str(root, "bare"));
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: value is just a number ────────────────────────── */

TEST(yaml_numeric_string_value) {
    const char *yaml = "port: 8080\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    /* Everything is stored as a string */
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "port"), "8080");
    /* But can be retrieved as float */
    ASSERT_FLOAT_EQ(cbm_yaml_get_float(root, "port", -1.0), 8080.0, 0.001);
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: no trailing newline ───────────────────────────── */

TEST(yaml_no_trailing_newline) {
    const char *yaml = "key: value";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "key"), "value");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: line with no colon ────────────────────────────── */

TEST(yaml_line_without_colon_skipped) {
    const char *yaml =
        "valid: yes\n"
        "this line has no colon\n"
        "also_valid: sure\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "valid"), "yes");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "also_valid"), "sure");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: hash at start of value (no space before it) ───── */

TEST(yaml_hash_no_preceding_space) {
    /* "#" only stripped as inline comment when preceded by space */
    const char *yaml = "channel: #general\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "channel"), "#general");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: list after comment lines ──────────────────────── */

TEST(yaml_list_after_comments) {
    const char *yaml =
        "paths:\n"
        "  # some paths to exclude\n"
        "  - /tmp\n"
        "  - /var/log\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    const char *items[4];
    int count = cbm_yaml_get_str_list(root, "paths", items, 4);
    ASSERT_EQ(count, 2);
    ASSERT_STR_EQ(items[0], "/tmp");
    ASSERT_STR_EQ(items[1], "/var/log");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: navigate() path segment > 255 chars ───────────── */

TEST(yaml_path_segment_overflow) {
    /* navigate() uses buf[256]; segment >= 256 chars returns NULL */
    char long_path[300];
    memset(long_path, 'a', 260);
    long_path[260] = '\0';

    const char *yaml = "key: val\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_NULL(cbm_yaml_get_str(root, long_path));
    ASSERT_FALSE(cbm_yaml_has(root, long_path));
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: empty path string ─────────────────────────────── */

TEST(yaml_empty_path) {
    const char *yaml = "key: val\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    /* Empty path -> navigate returns root (seg_len 0 -> NULL) */
    ASSERT_NULL(cbm_yaml_get_str(root, ""));
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: path with trailing dot ────────────────────────── */

TEST(yaml_path_trailing_dot) {
    const char *yaml =
        "a:\n"
        "  b: val\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    /* "a." -> segment "a", then empty segment -> seg_len 0 -> NULL */
    ASSERT_NULL(cbm_yaml_get_str(root, "a."));
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: duplicate keys ────────────────────────────────── */

TEST(yaml_duplicate_keys) {
    /* Parser stores both; find_child returns the first match */
    const char *yaml =
        "key: first\n"
        "key: second\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "key"), "first");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: indentation jump ──────────────────────────────── */

TEST(yaml_indentation_dedent) {
    /* After deep nesting, dedent back to top level */
    const char *yaml =
        "outer:\n"
        "  inner: deep\n"
        "top: level\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "outer.inner"), "deep");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "top"), "level");
    cbm_yaml_free(root);
    PASS();
}

/* ── Smoke: real-world .cgrconfig format ───────────────────────── */

TEST(yaml_smoke_cgrconfig) {
    const char *yaml =
        "# codebase-memory config\n"
        "mode: fast\n"
        "\n"
        "http_linker:\n"
        "  enabled: true\n"
        "  min_confidence: 0.7\n"
        "  exclude_paths:\n"
        "    - /health\n"
        "    - /metrics\n"
        "    - /internal/debug\n"
        "\n"
        "pipeline:\n"
        "  workers: 4\n"
        "  verbose: false\n"
        "\n"
        "# End of config\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);

    /* Top-level scalar */
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "mode"), "fast");

    /* Nested bool */
    ASSERT_TRUE(cbm_yaml_get_bool(root, "http_linker.enabled", false));

    /* Nested float */
    ASSERT_FLOAT_EQ(cbm_yaml_get_float(root, "http_linker.min_confidence", 0.0), 0.7, 0.001);

    /* Nested list */
    const char *paths[8];
    int count = cbm_yaml_get_str_list(root, "http_linker.exclude_paths", paths, 8);
    ASSERT_EQ(count, 3);
    ASSERT_STR_EQ(paths[0], "/health");
    ASSERT_STR_EQ(paths[1], "/metrics");
    ASSERT_STR_EQ(paths[2], "/internal/debug");

    /* Another nested section */
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "pipeline.workers"), "4");
    ASSERT_FALSE(cbm_yaml_get_bool(root, "pipeline.verbose", true));

    /* Non-existent */
    ASSERT_FALSE(cbm_yaml_has(root, "pipeline.timeout"));
    ASSERT_FLOAT_EQ(cbm_yaml_get_float(root, "pipeline.timeout", 30.0), 30.0, 0.001);

    cbm_yaml_free(root);
    PASS();
}

/* ── Smoke: parse, query many paths, free ──────────────────────── */

TEST(yaml_smoke_multi_query) {
    const char *yaml =
        "app:\n"
        "  name: testapp\n"
        "  debug: yes\n"
        "  port: 9090\n"
        "  features:\n"
        "    - auth\n"
        "    - logging\n"
        "  db:\n"
        "    host: pghost\n"
        "    ssl: on\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);

    /* Scalars at different depths */
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "app.name"), "testapp");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "app.db.host"), "pghost");

    /* Bools */
    ASSERT_TRUE(cbm_yaml_get_bool(root, "app.debug", false));
    ASSERT_TRUE(cbm_yaml_get_bool(root, "app.db.ssl", false));

    /* Float */
    ASSERT_FLOAT_EQ(cbm_yaml_get_float(root, "app.port", 0.0), 9090.0, 0.001);

    /* List */
    const char *feats[4];
    int count = cbm_yaml_get_str_list(root, "app.features", feats, 4);
    ASSERT_EQ(count, 2);
    ASSERT_STR_EQ(feats[0], "auth");
    ASSERT_STR_EQ(feats[1], "logging");

    /* has() checks */
    ASSERT_TRUE(cbm_yaml_has(root, "app"));
    ASSERT_TRUE(cbm_yaml_has(root, "app.features"));
    ASSERT_TRUE(cbm_yaml_has(root, "app.db"));
    ASSERT_FALSE(cbm_yaml_has(root, "app.cache"));

    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: len shorter than string ───────────────────────── */

TEST(yaml_partial_len) {
    /* Only parse first 7 bytes: "key: va" */
    const char *yaml = "key: value\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, 7);
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "key"), "va");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: list at top level via map last-child promotion ── */

TEST(yaml_top_level_list_items) {
    /* List items after a "key:" become children of that key's list node */
    const char *yaml =
        "colors:\n"
        "- red\n"
        "- green\n"
        "- blue\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    const char *items[4];
    int count = cbm_yaml_get_str_list(root, "colors", items, 4);
    ASSERT_EQ(count, 3);
    ASSERT_STR_EQ(items[0], "red");
    ASSERT_STR_EQ(items[1], "green");
    ASSERT_STR_EQ(items[2], "blue");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: mixed indentation levels (2 vs 4 spaces) ──────── */

TEST(yaml_inconsistent_indentation) {
    /* Parser tracks indentation levels via stack, not fixed width */
    const char *yaml =
        "a:\n"
        "    b:\n"
        "        c: deep\n"
        "    d: shallow\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "a.b.c"), "deep");
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "a.d"), "shallow");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: value with leading/trailing whitespace ─────────── */

TEST(yaml_value_whitespace_trimmed) {
    const char *yaml = "key:   spaced   \n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    /* trim_dup trims leading and trailing whitespace */
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "key"), "spaced");
    cbm_yaml_free(root);
    PASS();
}

/* ── Edge cases: key with leading whitespace at top level ──────── */

TEST(yaml_indented_top_level) {
    /* Leading spaces make the parser think it's nested */
    const char *yaml = "  indented_key: val\n";
    cbm_yaml_node_t *root = cbm_yaml_parse(yaml, (int)strlen(yaml));
    ASSERT_NOT_NULL(root);
    /* With indent=2, stack pops until parent.indent < 2; root.indent=-1 qualifies */
    ASSERT_STR_EQ(cbm_yaml_get_str(root, "indented_key"), "val");
    cbm_yaml_free(root);
    PASS();
}

/* ── Suite registration ────────────────────────────────────────── */

SUITE(yaml) {
    /* Parsing: NULL / empty */
    RUN_TEST(yaml_parse_null_input);
    RUN_TEST(yaml_parse_empty_string);
    RUN_TEST(yaml_parse_negative_len);
    RUN_TEST(yaml_free_null);

    /* Parsing: key-value */
    RUN_TEST(yaml_single_kv);
    RUN_TEST(yaml_single_kv_trailing_newline);
    RUN_TEST(yaml_multiple_kv);

    /* Parsing: nested maps */
    RUN_TEST(yaml_nested_map_2_levels);
    RUN_TEST(yaml_nested_map_3_levels);
    RUN_TEST(yaml_nested_siblings);

    /* Parsing: lists */
    RUN_TEST(yaml_string_list);
    RUN_TEST(yaml_list_max_out_limit);

    /* Parsing: comments */
    RUN_TEST(yaml_comment_only);
    RUN_TEST(yaml_inline_comment);
    RUN_TEST(yaml_comment_between_keys);

    /* Parsing: mixed */
    RUN_TEST(yaml_mixed_maps_and_lists);

    /* Parsing: colons */
    RUN_TEST(yaml_url_value);
    RUN_TEST(yaml_multiple_colons);

    /* Parsing: empty values */
    RUN_TEST(yaml_empty_value_becomes_map);

    /* Query: get_str */
    RUN_TEST(yaml_get_str_scalar);
    RUN_TEST(yaml_get_str_missing);
    RUN_TEST(yaml_get_str_nested);
    RUN_TEST(yaml_get_str_on_map_node);
    RUN_TEST(yaml_get_str_null_root);
    RUN_TEST(yaml_get_str_null_path);

    /* Query: get_float */
    RUN_TEST(yaml_get_float_valid);
    RUN_TEST(yaml_get_float_integer);
    RUN_TEST(yaml_get_float_negative);
    RUN_TEST(yaml_get_float_invalid_string);
    RUN_TEST(yaml_get_float_missing_key);
    RUN_TEST(yaml_get_float_zero);

    /* Query: get_bool */
    RUN_TEST(yaml_get_bool_true_false);
    RUN_TEST(yaml_get_bool_yes_no);
    RUN_TEST(yaml_get_bool_on_off);
    RUN_TEST(yaml_get_bool_1_0);
    RUN_TEST(yaml_get_bool_case_insensitive);
    RUN_TEST(yaml_get_bool_missing_returns_default);
    RUN_TEST(yaml_get_bool_unrecognized_returns_default);

    /* Query: get_str_list */
    RUN_TEST(yaml_get_str_list_non_list_path);
    RUN_TEST(yaml_get_str_list_missing_path);
    RUN_TEST(yaml_get_str_list_null_root);

    /* Query: has() */
    RUN_TEST(yaml_has_existing);
    RUN_TEST(yaml_has_missing);
    RUN_TEST(yaml_has_nested);
    RUN_TEST(yaml_has_null_root);

    /* Edge cases */
    RUN_TEST(yaml_long_value);
    RUN_TEST(yaml_special_chars_in_value);
    RUN_TEST(yaml_equals_in_value);
    RUN_TEST(yaml_deeply_nested);
    RUN_TEST(yaml_missing_intermediate);
    RUN_TEST(yaml_tab_not_indentation);
    RUN_TEST(yaml_crlf_line_endings);
    RUN_TEST(yaml_quoted_string_with_hash);
    RUN_TEST(yaml_single_quoted_with_hash);
    RUN_TEST(yaml_empty_lines_between_keys);
    RUN_TEST(yaml_whitespace_after_colon);
    RUN_TEST(yaml_numeric_string_value);
    RUN_TEST(yaml_no_trailing_newline);
    RUN_TEST(yaml_line_without_colon_skipped);
    RUN_TEST(yaml_hash_no_preceding_space);
    RUN_TEST(yaml_list_after_comments);
    RUN_TEST(yaml_path_segment_overflow);
    RUN_TEST(yaml_empty_path);
    RUN_TEST(yaml_path_trailing_dot);
    RUN_TEST(yaml_duplicate_keys);
    RUN_TEST(yaml_indentation_dedent);
    RUN_TEST(yaml_partial_len);
    RUN_TEST(yaml_top_level_list_items);
    RUN_TEST(yaml_inconsistent_indentation);
    RUN_TEST(yaml_value_whitespace_trimmed);
    RUN_TEST(yaml_indented_top_level);

    /* Smoke tests */
    RUN_TEST(yaml_smoke_cgrconfig);
    RUN_TEST(yaml_smoke_multi_query);
}
