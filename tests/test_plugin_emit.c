/* test_plugin_emit.c — emit-plugin generator contract (Claude Code plugin). */
#include "test_framework.h"

#include <cli/agent_profiles.h>
#include <cli/cli.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* A unique temp dir under the build tree; deterministic name (no mkstemp
 * randomness needed — the suite runs single-threaded and cleans up). */
static const char *emit_tmp_dir(void) {
    return "build/test-plugin-emit";
}

static char *read_all(const char *path) {
    FILE *f = fopen(path, "rb");
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

TEST(plugin_emit_writes_plugin_json_with_version) {
    ASSERT_EQ(cbm_emit_plugin(emit_tmp_dir(), "9.9.9"), 0);

    char *json = read_all("build/test-plugin-emit/.claude-plugin/plugin.json");
    ASSERT_NOT_NULL(json);
    ASSERT_TRUE(strstr(json, "\"name\": \"codebase-memory\"") != NULL);
    ASSERT_TRUE(strstr(json, "\"version\": \"9.9.9\"") != NULL);
    ASSERT_TRUE(strstr(json, "\"description\"") != NULL);
    free(json);
    PASS();
}

TEST(plugin_emit_skill_matches_source_bytes) {
    ASSERT_EQ(cbm_emit_plugin(emit_tmp_dir(), "9.9.9"), 0);

    const cbm_skill_t *skills = cbm_get_skills();
    ASSERT_NOT_NULL(skills);

    char *written = read_all("build/test-plugin-emit/skills/codebase-memory/SKILL.md");
    ASSERT_NOT_NULL(written);
    ASSERT_STR_EQ(written, skills[0].content);
    free(written);
    PASS();
}

TEST(plugin_emit_agents_match_rendered_profiles) {
    ASSERT_EQ(cbm_emit_plugin(emit_tmp_dir(), "9.9.9"), 0);

    const cbm_graph_tier_t tiers[] = {
        CBM_GRAPH_TIER_SCOUT, CBM_GRAPH_TIER_VERIFY, CBM_GRAPH_TIER_AUDIT};

    for (int i = 0; i < 3; i++) {
        char *expected = cbm_render_graph_profile(
            CBM_GRAPH_DIALECT_CLAUDE, tiers[i], CBM_GRAPH_ACCESS_DIRECT, NULL);
        ASSERT_NOT_NULL(expected);

        char path[512];
        snprintf(path, sizeof(path), "build/test-plugin-emit/agents/%s.md",
                 cbm_graph_tier_slug(tiers[i]));
        char *written = read_all(path);
        ASSERT_NOT_NULL(written);
        ASSERT_STR_EQ(written, expected);

        free(written);
        free(expected);
    }

    PASS();
}

SUITE(plugin_emit) {
    RUN_TEST(plugin_emit_writes_plugin_json_with_version);
    RUN_TEST(plugin_emit_skill_matches_source_bytes);
    RUN_TEST(plugin_emit_agents_match_rendered_profiles);
}
