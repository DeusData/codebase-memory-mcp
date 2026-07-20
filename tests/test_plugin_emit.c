/* test_plugin_emit.c — emit-plugin generator contract (Claude Code plugin). */
#include "test_framework.h"

#include <cli/cli.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

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
    free(json);
    PASS();
}

SUITE(plugin_emit) {
    RUN_TEST(plugin_emit_writes_plugin_json_with_version);
}
