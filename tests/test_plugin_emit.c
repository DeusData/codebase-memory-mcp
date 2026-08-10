/* test_plugin_emit.c — emit-plugin generator contract (Claude Code plugin). */
#include "test_framework.h"

#include <cli/agent_profiles.h>
#include <cli/cli.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <foundation/compat_fs.h>
#include <yyjson/yyjson.h>

#ifndef _WIN32
#include <unistd.h>
#endif

/* A unique temp dir under the build tree; deterministic name (no mkstemp
 * randomness needed — the suite runs single-threaded and cleans up). */
static const char *emit_tmp_dir(void) {
    return "build/test-plugin-emit";
}

static char *read_all(const char *path) {
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)n + 1);
    if (buf && n > 0) {
        size_t got = fread(buf, 1, (size_t)n, f);
        buf[got] = '\0';
    } else if (buf) {
        buf[0] = '\0';
    }
    fclose(f);
    return buf;
}

static int write_all(const char *path, const char *content) {
    FILE *f = cbm_fopen(path, "wb");
    if (!f) {
        return -1;
    }
    size_t len = strlen(content);
    int rc = fwrite(content, 1, len, f) == len ? 0 : -1;
    if (fclose(f) != 0) {
        rc = -1;
    }
    return rc;
}

#ifdef _WIN32
static void test_windows_path(char *dst, size_t dst_sz, const char *src) {
    size_t i = 0;
    for (; src[i] && i + 1 < dst_sz; i++) {
        dst[i] = src[i] == '/' ? '\\' : src[i];
    }
    dst[i] = '\0';
}

static int create_directory_link(const char *target, const char *link_path) {
    char native_target[512];
    char native_link[512];
    test_windows_path(native_target, sizeof(native_target), target);
    test_windows_path(native_link, sizeof(native_link), link_path);
    const char *argv[] = {"cmd.exe", "/d", "/c", "mklink", "/J", native_link, native_target, NULL};
    return cbm_exec_no_shell(argv) == 0;
}
#else
static int create_directory_link(const char *target, const char *link_path) {
    return symlink(target, link_path) == 0;
}
#endif

TEST(plugin_emit_writes_plugin_json_with_version) {
    ASSERT_EQ(cbm_emit_plugin(emit_tmp_dir(), "9.9.9"), 0);

    char *json = read_all("build/test-plugin-emit/.claude-plugin/plugin.json");
    ASSERT_NOT_NULL(json);
    int has_name = strstr(json, "\"name\": \"codebase-memory\"") != NULL;
    int has_version = strstr(json, "\"version\": \"9.9.9\"") != NULL;
    int has_description = strstr(json, "\"description\"") != NULL;
    free(json);
    ASSERT_TRUE(has_name);
    ASSERT_TRUE(has_version);
    ASSERT_TRUE(has_description);
    PASS();
}

TEST(plugin_emit_skill_matches_source_bytes) {
    ASSERT_EQ(cbm_emit_plugin(emit_tmp_dir(), "9.9.9"), 0);

    const cbm_skill_t *skills = cbm_get_skills();
    ASSERT_NOT_NULL(skills);

    char *written = read_all("build/test-plugin-emit/skills/codebase-memory/SKILL.md");
    ASSERT_NOT_NULL(written);
    int eq = strcmp(written, skills[0].content) == 0;
    free(written);
    ASSERT_TRUE(eq);
    PASS();
}

TEST(plugin_emit_agents_match_rendered_profiles) {
    ASSERT_EQ(cbm_emit_plugin(emit_tmp_dir(), "9.9.9"), 0);

    const cbm_graph_tier_t tiers[] = {CBM_GRAPH_TIER_SCOUT, CBM_GRAPH_TIER_VERIFY,
                                      CBM_GRAPH_TIER_AUDIT};

    for (int i = 0; i < 3; i++) {
        char *expected = cbm_render_graph_profile(CBM_GRAPH_DIALECT_CLAUDE, tiers[i],
                                                  CBM_GRAPH_ACCESS_DIRECT, NULL);
        ASSERT_NOT_NULL(expected);

        char path[512];
        snprintf(path, sizeof(path), "build/test-plugin-emit/agents/%s.md",
                 cbm_graph_tier_slug(tiers[i]));
        char *written = read_all(path);
        if (!written) {
            free(expected);
        }
        ASSERT_NOT_NULL(written);

        int eq = strcmp(written, expected) == 0;
        free(written);
        free(expected);
        ASSERT_TRUE(eq);
    }

    PASS();
}

TEST(plugin_emit_mcp_json_has_single_npx_server) {
    ASSERT_EQ(cbm_emit_plugin(emit_tmp_dir(), "9.9.9"), 0);

    char *json = read_all("build/test-plugin-emit/.mcp.json");
    ASSERT_NOT_NULL(json);

    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    int has_doc = doc != NULL;

    int server_count = -1;
    int has_srv = 0;
    int command_is_npx = 0;
    int args_len = -1;
    int arg0_is_y = 0;
    int arg1_is_pkg = 0;

    if (has_doc) {
        yyjson_val *servers = yyjson_obj_get(yyjson_doc_get_root(doc), "mcpServers");
        if (servers) {
            server_count = (int)yyjson_obj_size(servers);
            yyjson_val *srv = yyjson_obj_get(servers, "codebase-memory-mcp");
            has_srv = srv != NULL;
            if (srv) {
                const char *cmd = yyjson_get_str(yyjson_obj_get(srv, "command"));
                command_is_npx = cmd && strcmp(cmd, "npx") == 0;
                yyjson_val *args = yyjson_obj_get(srv, "args");
                if (args) {
                    args_len = (int)yyjson_arr_size(args);
                    const char *a0 = yyjson_get_str(yyjson_arr_get(args, 0));
                    const char *a1 = yyjson_get_str(yyjson_arr_get(args, 1));
                    arg0_is_y = a0 && strcmp(a0, "-y") == 0;
                    arg1_is_pkg = a1 && strcmp(a1, "codebase-memory-mcp@9.9.9") == 0;
                }
            }
        }
    }

    int no_tool_profile = strstr(json, "--tool-profile") == NULL;

    if (has_doc) {
        yyjson_doc_free(doc);
    }
    free(json);

    ASSERT_TRUE(has_doc);
    ASSERT_EQ(server_count, 1);
    ASSERT_TRUE(has_srv);
    ASSERT_TRUE(command_is_npx);
    ASSERT_EQ(args_len, 2);
    ASSERT_TRUE(arg0_is_y);
    ASSERT_TRUE(arg1_is_pkg);
    ASSERT_TRUE(no_tool_profile);

    PASS();
}

static int hook_uses_pinned_package(yyjson_doc *doc, const char *event) {
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *rules = yyjson_obj_get(root, event);
    yyjson_val *rule = rules ? yyjson_arr_get(rules, 0) : NULL;
    yyjson_val *hooks = rule ? yyjson_obj_get(rule, "hooks") : NULL;
    yyjson_val *hook = hooks ? yyjson_arr_get(hooks, 0) : NULL;
    if (!hook) {
        return 0;
    }

    const char *type = yyjson_get_str(yyjson_obj_get(hook, "type"));
    const char *command = yyjson_get_str(yyjson_obj_get(hook, "command"));
    yyjson_val *args = yyjson_obj_get(hook, "args");
    const char *arg0 = args ? yyjson_get_str(yyjson_arr_get(args, 0)) : NULL;
    const char *arg1 = args ? yyjson_get_str(yyjson_arr_get(args, 1)) : NULL;
    const char *arg2 = args ? yyjson_get_str(yyjson_arr_get(args, 2)) : NULL;

    return type && strcmp(type, "command") == 0 && command && strcmp(command, "npx") == 0 && arg0 &&
           strcmp(arg0, "-y") == 0 && arg1 && strcmp(arg1, "codebase-memory-mcp@9.9.9") == 0 &&
           arg2 && strcmp(arg2, "hook-augment") == 0;
}

TEST(plugin_emit_hooks_json_has_four_events) {
    ASSERT_EQ(cbm_emit_plugin(emit_tmp_dir(), "9.9.9"), 0);

    char *json = read_all("build/test-plugin-emit/hooks/hooks.json");
    ASSERT_NOT_NULL(json);

    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    int has_doc = doc != NULL;

    int has_session_start = 0;
    int has_subagent_start = 0;
    int has_pre_tool_use = 0;
    int has_post_tool_use = 0;
    int session_is_pinned = 0;
    int subagent_is_pinned = 0;
    int pre_tool_is_pinned = 0;
    int post_tool_is_pinned = 0;
    if (has_doc) {
        yyjson_val *root = yyjson_doc_get_root(doc);
        has_session_start = yyjson_obj_get(root, "SessionStart") != NULL;
        has_subagent_start = yyjson_obj_get(root, "SubagentStart") != NULL;
        has_pre_tool_use = yyjson_obj_get(root, "PreToolUse") != NULL;
        has_post_tool_use = yyjson_obj_get(root, "PostToolUse") != NULL;
        session_is_pinned = hook_uses_pinned_package(doc, "SessionStart");
        subagent_is_pinned = hook_uses_pinned_package(doc, "SubagentStart");
        pre_tool_is_pinned = hook_uses_pinned_package(doc, "PreToolUse");
        post_tool_is_pinned = hook_uses_pinned_package(doc, "PostToolUse");
    }

    /* matchers */
    int has_grep_glob_matcher = strstr(json, "\"Grep|Glob\"") != NULL;
    int has_read_matcher = strstr(json, "\"Read\"") != NULL;

    if (has_doc) {
        yyjson_doc_free(doc);
    }
    free(json);

    ASSERT_TRUE(has_doc);
    ASSERT_TRUE(has_session_start);
    ASSERT_TRUE(has_subagent_start);
    ASSERT_TRUE(has_pre_tool_use);
    ASSERT_TRUE(has_post_tool_use);
    ASSERT_TRUE(session_is_pinned);
    ASSERT_TRUE(subagent_is_pinned);
    ASSERT_TRUE(pre_tool_is_pinned);
    ASSERT_TRUE(post_tool_is_pinned);
    ASSERT_TRUE(has_grep_glob_matcher);
    ASSERT_TRUE(has_read_matcher);

    PASS();
}

/* Concatenate every emitted file's bytes into one buffer, in a fixed order,
 * so two runs can be compared for byte-identity. */
static char *emit_snapshot(const char *dir) {
    static const char *const rel[] = {
        ".claude-plugin/plugin.json",
        ".mcp.json",
        "hooks/hooks.json",
        "skills/codebase-memory/SKILL.md",
        "agents/codebase-memory-scout.md",
        "agents/codebase-memory.md",
        "agents/codebase-memory-auditor.md",
    };
    size_t cap = 1 << 20, len = 0;
    char *out = malloc(cap);
    out[0] = '\0';
    for (size_t i = 0; i < sizeof(rel) / sizeof(rel[0]); i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", dir, rel[i]);
        char *c = read_all(path);
        if (c) {
            size_t n = strlen(c);
            if (len + n + 1 < cap) {
                memcpy(out + len, c, n);
                len += n;
                out[len] = '\0';
            }
            free(c);
        }
    }
    return out;
}

TEST(plugin_emit_is_idempotent) {
    ASSERT_EQ(cbm_emit_plugin("build/test-plugin-emit-a", "9.9.9"), 0);
    ASSERT_EQ(cbm_emit_plugin("build/test-plugin-emit-a", "9.9.9"), 0); /* twice, same dir */
    ASSERT_EQ(cbm_emit_plugin("build/test-plugin-emit-b", "9.9.9"),
              0); /* all emits before any alloc */

    char *first = emit_snapshot("build/test-plugin-emit-a");
    char *second = emit_snapshot("build/test-plugin-emit-b");

    int eq = strcmp(first, second) == 0;
    free(first);
    free(second);
    ASSERT_TRUE(eq);
    PASS();
}

TEST(plugin_emit_refuses_non_plugin_dir) {
    /* A dir with a stray file and NO .claude-plugin/plugin.json must NOT be
     * wiped — emit-plugin recursively clears out_dir, so the guard protects
     * against `emit-plugin .` / a typo destroying real files. */
    cbm_mkdir_p("build/test-plugin-guard", 0755);
    ASSERT_EQ(write_all("build/test-plugin-guard/keepme.txt", "x"), 0);

    int refused = cbm_emit_plugin("build/test-plugin-guard", "9.9.9") != 0;
    char *kept = read_all("build/test-plugin-guard/keepme.txt");
    int survived = kept != NULL;
    free(kept);

    /* Normal path still works: build/test-plugin-emit either already carries
     * the marker from earlier tests, or is absent (a first-ever emit) — both
     * are allowed by the guard. */
    int normal_ok = cbm_emit_plugin("build/test-plugin-emit", "9.9.9") == 0;

    ASSERT_TRUE(refused);
    ASSERT_TRUE(survived);
    ASSERT_TRUE(normal_ok);
    PASS();
}

TEST(plugin_emit_refuses_foreign_plugin_marker) {
    const char *dir = "build/test-plugin-foreign";
    ASSERT_TRUE(cbm_mkdir_p("build/test-plugin-foreign/.claude-plugin", 0755));
    ASSERT_EQ(write_all("build/test-plugin-foreign/.claude-plugin/plugin.json",
                        "{\n  \"name\": \"some-other-plugin\"\n}\n"),
              0);
    ASSERT_EQ(write_all("build/test-plugin-foreign/keepme.txt", "foreign"), 0);

    int status = cbm_emit_plugin(dir, "9.9.9");
    char *marker = read_all("build/test-plugin-foreign/.claude-plugin/plugin.json");
    char *kept = read_all("build/test-plugin-foreign/keepme.txt");
    int marker_survived = marker && strstr(marker, "some-other-plugin") != NULL;
    int file_survived = kept && strcmp(kept, "foreign") == 0;
    free(marker);
    free(kept);

    ASSERT_NEQ(status, 0);
    ASSERT_TRUE(marker_survived);
    ASSERT_TRUE(file_survived);
    PASS();
}

TEST(plugin_emit_refuses_malformed_plugin_marker) {
    const char *dir = "build/test-plugin-malformed";
    ASSERT_TRUE(cbm_mkdir_p("build/test-plugin-malformed/.claude-plugin", 0755));
    ASSERT_EQ(write_all("build/test-plugin-malformed/.claude-plugin/plugin.json",
                        "{ definitely-not-json"),
              0);
    ASSERT_EQ(write_all("build/test-plugin-malformed/keepme.txt", "malformed"), 0);

    int status = cbm_emit_plugin(dir, "9.9.9");
    char *marker = read_all("build/test-plugin-malformed/.claude-plugin/plugin.json");
    char *kept = read_all("build/test-plugin-malformed/keepme.txt");
    int marker_survived = marker && strcmp(marker, "{ definitely-not-json") == 0;
    int file_survived = kept && strcmp(kept, "malformed") == 0;
    free(marker);
    free(kept);

    ASSERT_NEQ(status, 0);
    ASSERT_TRUE(marker_survived);
    ASSERT_TRUE(file_survived);
    PASS();
}

TEST(plugin_emit_supports_utf8_output_directory) {
    const char *dir = "build/test-plugin-emit-\xE6\xB5\x8B\xE8\xAF\x95";
    ASSERT_EQ(cbm_emit_plugin(dir, "9.9.9"), 0);
    ASSERT_EQ(cbm_emit_plugin(dir, "9.9.9"), 0);

    char marker_path[512];
    snprintf(marker_path, sizeof(marker_path), "%s/.claude-plugin/plugin.json", dir);
    char *marker = read_all(marker_path);
    int marker_is_current = marker && strstr(marker, "\"version\": \"9.9.9\"") != NULL;
    free(marker);
    ASSERT_TRUE(marker_is_current);
    PASS();
}

TEST(plugin_emit_does_not_traverse_directory_links) {
    const char *root = "build/test-plugin-link-root";
    const char *target = "build/test-plugin-link-target";
    const char *link_path = "build/test-plugin-link-root/escape";
    ASSERT_EQ(cbm_emit_plugin(root, "9.9.9"), 0);
    ASSERT_TRUE(cbm_mkdir_p(target, 0755));
    ASSERT_EQ(write_all("build/test-plugin-link-target/sentinel.txt", "safe"), 0);

#ifdef _WIN32
    ASSERT_TRUE(create_directory_link(target, link_path));
#else
    ASSERT_TRUE(create_directory_link("../test-plugin-link-target", link_path));
#endif
    ASSERT_EQ(cbm_emit_plugin(root, "9.9.9"), 0);

    char *sentinel = read_all("build/test-plugin-link-target/sentinel.txt");
    int sentinel_survived = sentinel && strcmp(sentinel, "safe") == 0;
    free(sentinel);
    ASSERT_TRUE(sentinel_survived);
    PASS();
}

TEST(plugin_emit_path_info_distinguishes_missing_from_errors) {
    cbm_path_info_t info;
    ASSERT_EQ(cbm_path_info_utf8("build/test-plugin-path-info-definitely-missing", &info),
              CBM_PATH_INFO_MISSING);
    ASSERT_EQ(cbm_path_info_utf8(NULL, &info), CBM_PATH_INFO_ERROR);
    PASS();
}

TEST(plugin_emit_command_returns_positive_failure_status) {
    const char *dir = "build/test-plugin-command-failure";
    ASSERT_TRUE(cbm_mkdir_p(dir, 0755));
    ASSERT_EQ(write_all("build/test-plugin-command-failure/keepme.txt", "safe"), 0);

    char *argv[] = {(char *)dir, (char *)"--version", (char *)"9.9.9"};
    int status = cbm_cmd_emit_plugin(3, argv);
    char *kept = read_all("build/test-plugin-command-failure/keepme.txt");
    int file_survived = kept && strcmp(kept, "safe") == 0;
    free(kept);

    ASSERT_GT(status, 0);
    ASSERT_TRUE(file_survived);
    PASS();
}

SUITE(plugin_emit) {
    RUN_TEST(plugin_emit_writes_plugin_json_with_version);
    RUN_TEST(plugin_emit_skill_matches_source_bytes);
    RUN_TEST(plugin_emit_agents_match_rendered_profiles);
    RUN_TEST(plugin_emit_mcp_json_has_single_npx_server);
    RUN_TEST(plugin_emit_hooks_json_has_four_events);
    RUN_TEST(plugin_emit_is_idempotent);
    RUN_TEST(plugin_emit_refuses_non_plugin_dir);
    RUN_TEST(plugin_emit_refuses_foreign_plugin_marker);
    RUN_TEST(plugin_emit_refuses_malformed_plugin_marker);
    RUN_TEST(plugin_emit_supports_utf8_output_directory);
    RUN_TEST(plugin_emit_does_not_traverse_directory_links);
    RUN_TEST(plugin_emit_path_info_distinguishes_missing_from_errors);
    RUN_TEST(plugin_emit_command_returns_positive_failure_status);
}
