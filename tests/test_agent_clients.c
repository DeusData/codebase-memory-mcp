/* test_agent_clients.c — Agent-client registry, resolution, and owned MCP edits. */
#include "test_framework.h"
#include "test_helpers.h"

#include <cli/agent_clients.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *paths[16];
    size_t path_count;
    const char *commands[16];
    size_t command_count;
} agent_probe_t;

static bool agent_probe_contains(const char *value, const void *context, bool commands) {
    const agent_probe_t *probe = (const agent_probe_t *)context;
    const char *const *values = commands ? probe->commands : probe->paths;
    size_t count = commands ? probe->command_count : probe->path_count;
    for (size_t i = 0U; i < count; i++) {
        if (strcmp(values[i], value) == 0) {
            return true;
        }
    }
    return false;
}

static bool agent_path_exists(const char *value, const void *context) {
    return agent_probe_contains(value, context, false);
}

static bool agent_command_exists(const char *value, const void *context) {
    return agent_probe_contains(value, context, true);
}

static cbm_agent_client_resolve_options_t agent_options(agent_probe_t *probe) {
    cbm_agent_client_resolve_options_t options = {
        .home_dir = "/home/tester",
        .xdg_config_home = NULL,
        .appdata_dir = NULL,
        .glab_config_dir = NULL,
        .kimi_code_home = NULL,
        .continue_config_path = NULL,
        .trae_config_path = NULL,
        .roo_config_path = NULL,
        .cody_config_path = NULL,
        .is_windows = false,
        .path_exists = agent_path_exists,
        .command_exists = agent_command_exists,
        .probe_context = probe,
    };
    return options;
}

static char *agent_fixture(const char *initial, char **dir_out) {
    char *dir = th_mktempdir("cbm_agent_clients");
    if (!dir) {
        return NULL;
    }
    size_t length = strlen(dir) + strlen("/config.json") + 1U;
    char *path = (char *)malloc(length);
    if (!path) {
        th_cleanup(dir);
        return NULL;
    }
    snprintf(path, length, "%s/config.json", dir);
    if (initial && th_write_file(path, initial) != 0) {
        free(path);
        th_cleanup(dir);
        return NULL;
    }
    *dir_out = dir;
    return path;
}

static char *agent_read(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp || fseek(fp, 0L, SEEK_END) != 0) {
        if (fp) {
            fclose(fp);
        }
        return NULL;
    }
    long length = ftell(fp);
    if (length < 0L || fseek(fp, 0L, SEEK_SET) != 0) {
        fclose(fp);
        return NULL;
    }
    char *text = (char *)malloc((size_t)length + 1U);
    if (!text) {
        fclose(fp);
        return NULL;
    }
    size_t read_count = fread(text, 1U, (size_t)length, fp);
    int close_rc = fclose(fp);
    if (read_count != (size_t)length || close_rc != 0) {
        free(text);
        return NULL;
    }
    text[read_count] = '\0';
    return text;
}

static size_t agent_occurrences(const char *text, const char *needle) {
    size_t count = 0U;
    size_t length = strlen(needle);
    while (length > 0U && (text = strstr(text, needle)) != NULL) {
        count++;
        text += length;
    }
    return count;
}

static int agent_write_rovo_override(const char *config_path, const char *mcp_path) {
    size_t length = strlen("mcp:\n  mcpConfigPath: \"\"\n") + strlen(mcp_path) + 1U;
    char *yaml = (char *)malloc(length);
    if (!yaml) {
        return -1;
    }
    int written = snprintf(yaml, length, "mcp:\n  mcpConfigPath: \"%s\"\n", mcp_path);
    int result = written >= 0 && (size_t)written < length ? th_write_file(config_path, yaml) : -1;
    free(yaml);
    return result;
}

static char *agent_deep_same_name_json(size_t depth) {
    static const char prefix[] = "{\"mcpServers\":";
    static const char leaf[] = "{\"codebase-memory-mcp\":{\"command\":\"foreign\",\"args\":[]}}";
    size_t prefix_length = strlen(prefix);
    size_t leaf_length = strlen(leaf);
    if (depth > (SIZE_MAX - leaf_length - 2U) / (prefix_length + 1U)) {
        return NULL;
    }
    size_t capacity = depth * (prefix_length + 1U) + leaf_length + 2U;
    char *json = (char *)malloc(capacity);
    if (!json) {
        return NULL;
    }
    char *cursor = json;
    for (size_t i = 0U; i < depth; i++) {
        memcpy(cursor, prefix, prefix_length);
        cursor += prefix_length;
    }
    memcpy(cursor, leaf, leaf_length);
    cursor += leaf_length;
    for (size_t i = 0U; i < depth; i++) {
        *cursor++ = '}';
    }
    *cursor++ = '\n';
    *cursor = '\0';
    return json;
}

TEST(agent_clients_registry_is_stable_and_callback_driven) {
    static const char *expected[] = {
        "qoder",     "kimi",        "gitlab-duo",    "rovo-dev", "amp",      "devin",
        "tabnine",   "continue",    "visual-studio", "trae",     "roo-code", "amazon-q",
        "codebuddy", "ibm-bob-ide", "ibm-bob-shell", "pochi",    "pi",       "sourcegraph-cody",
    };
    static const uint32_t expected_capabilities[] = {
        CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_AGENT | CBM_AGENT_CAP_HOOK,
        CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_HOOK,
        CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_HOOK,
        CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_AGENT,
        CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL,
        CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_HOOK,
        CBM_AGENT_CAP_MCP,
        CBM_AGENT_CAP_MCP,
        CBM_AGENT_CAP_MCP,
        CBM_AGENT_CAP_MCP,
        CBM_AGENT_CAP_MCP,
        CBM_AGENT_CAP_MCP,
        CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_AGENT,
        CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL,
        CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS,
        CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_AGENT,
        CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL,
        CBM_AGENT_CAP_MCP,
    };
    ASSERT_EQ(cbm_agent_client_count(), CBM_AGENT_CLIENT_COUNT);
    ASSERT_EQ(CBM_AGENT_CLIENT_COUNT, sizeof(expected) / sizeof(expected[0]));
    ASSERT_EQ(CBM_AGENT_CLIENT_COUNT,
              sizeof(expected_capabilities) / sizeof(expected_capabilities[0]));
    for (size_t i = 0U; i < CBM_AGENT_CLIENT_COUNT; i++) {
        const cbm_agent_client_profile_t *profile = cbm_agent_client_at(i);
        ASSERT_NOT_NULL(profile);
        ASSERT_EQ(profile->id, (cbm_agent_client_id_t)i);
        ASSERT_STR_EQ(profile->stable_id, expected[i]);
        ASSERT_EQ(profile->capabilities, expected_capabilities[i]);
        ASSERT_NOT_NULL(profile->display_name);
        if (profile->id == CBM_AGENT_CLIENT_PI) {
            ASSERT(!(profile->capabilities & CBM_AGENT_CAP_MCP));
            ASSERT_NULL(profile->install_mcp);
            ASSERT_NULL(profile->remove_mcp);
        } else {
            ASSERT(profile->capabilities & CBM_AGENT_CAP_MCP);
            ASSERT_NOT_NULL(profile->install_mcp);
            ASSERT_NOT_NULL(profile->remove_mcp);
        }
        ASSERT(cbm_agent_client_by_id(profile->id) == profile);
        ASSERT(cbm_agent_client_by_stable_id(profile->stable_id) == profile);
    }
    ASSERT_NULL(cbm_agent_client_by_id(CBM_AGENT_CLIENT_COUNT));
    ASSERT_NULL(cbm_agent_client_by_stable_id("glab"));
    ASSERT_EQ(cbm_agent_client_by_id(CBM_AGENT_CLIENT_TRAE)->stability, CBM_AGENT_CONDITIONAL);
    PASS();
}

TEST(agent_clients_visual_studio_cleanup_survives_missing_command) {
    agent_probe_t probe = {.paths = {"/home/tester/.mcp.json"}, .path_count = 1U};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    options.is_windows = true;
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_VISUAL_STUDIO, &options));
    ASSERT(cbm_agent_client_cleanup_candidate(CBM_AGENT_CLIENT_VISUAL_STUDIO, &options));

    const char *foreign = "{\"servers\":{\"codebase-memory-mcp\":{\"type\":\"stdio\","
                          "\"command\":\"C:/User/tool.exe\",\"args\":[]}}}\n";
    char *dir = NULL;
    char *path = agent_fixture(foreign, &dir);
    ASSERT_NOT_NULL(path);
    ASSERT_EQ(cbm_agent_client_remove_mcp(CBM_AGENT_CLIENT_VISUAL_STUDIO, path,
                                          "C:/Tools/codebase-memory-mcp.exe"),
              CBM_AGENT_EDIT_FOREIGN);
    char *after = agent_read(path);
    ASSERT_NOT_NULL(after);
    ASSERT_STR_EQ(after, foreign);
    free(after);
    free(path);
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_next_wave_metadata_matches_supported_surfaces) {
    const cbm_agent_client_profile_t *continue_profile =
        cbm_agent_client_by_id(CBM_AGENT_CLIENT_CONTINUE);
    const cbm_agent_client_profile_t *visual_studio =
        cbm_agent_client_by_id(CBM_AGENT_CLIENT_VISUAL_STUDIO);
    const cbm_agent_client_profile_t *rovo = cbm_agent_client_by_id(CBM_AGENT_CLIENT_ROVO_DEV);
    const cbm_agent_client_profile_t *gitlab = cbm_agent_client_by_id(CBM_AGENT_CLIENT_GITLAB_DUO);
    const cbm_agent_client_profile_t *devin = cbm_agent_client_by_id(CBM_AGENT_CLIENT_DEVIN);
    const cbm_agent_client_profile_t *roo = cbm_agent_client_by_id(CBM_AGENT_CLIENT_ROO_CODE);
    const cbm_agent_client_profile_t *amazon_q = cbm_agent_client_by_id(CBM_AGENT_CLIENT_AMAZON_Q);
    const cbm_agent_client_profile_t *codebuddy =
        cbm_agent_client_by_id(CBM_AGENT_CLIENT_CODEBUDDY);
    const cbm_agent_client_profile_t *ibm_bob_ide =
        cbm_agent_client_by_id(CBM_AGENT_CLIENT_IBM_BOB_IDE);
    const cbm_agent_client_profile_t *ibm_bob_shell =
        cbm_agent_client_by_id(CBM_AGENT_CLIENT_IBM_BOB_SHELL);
    const cbm_agent_client_profile_t *pochi = cbm_agent_client_by_id(CBM_AGENT_CLIENT_POCHI);
    const cbm_agent_client_profile_t *pi = cbm_agent_client_by_id(CBM_AGENT_CLIENT_PI);
    const cbm_agent_client_profile_t *cody =
        cbm_agent_client_by_id(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY);
    ASSERT_NOT_NULL(continue_profile);
    ASSERT_NOT_NULL(visual_studio);
    ASSERT_NOT_NULL(rovo);
    ASSERT_NOT_NULL(gitlab);
    ASSERT_NOT_NULL(devin);
    ASSERT_NOT_NULL(roo);
    ASSERT_NOT_NULL(amazon_q);
    ASSERT_NOT_NULL(codebuddy);
    ASSERT_NOT_NULL(ibm_bob_ide);
    ASSERT_NOT_NULL(ibm_bob_shell);
    ASSERT_NOT_NULL(pochi);
    ASSERT_NOT_NULL(pi);
    ASSERT_NOT_NULL(cody);
    ASSERT_EQ(continue_profile->stability, CBM_AGENT_CONDITIONAL);
    ASSERT_EQ(visual_studio->stability, CBM_AGENT_CONDITIONAL);
    ASSERT(rovo->capabilities & CBM_AGENT_CAP_SKILL);
    ASSERT(rovo->capabilities & CBM_AGENT_CAP_AGENT);
    ASSERT(gitlab->capabilities & CBM_AGENT_CAP_HOOK);
    ASSERT(!(devin->capabilities & CBM_AGENT_CAP_AGENT));
    ASSERT_EQ(roo->stability, CBM_AGENT_CONDITIONAL);
    ASSERT(roo->capabilities & CBM_AGENT_CAP_MCP);
    ASSERT(amazon_q->capabilities & CBM_AGENT_CAP_MCP);
    ASSERT_STR_EQ(amazon_q->display_name, "Amazon Q Developer IDE");
    ASSERT_EQ(codebuddy->stability, CBM_AGENT_STABLE);
    ASSERT(codebuddy->capabilities & CBM_AGENT_CAP_AGENT);
    ASSERT(codebuddy->capabilities & CBM_AGENT_CAP_SKILL);
    ASSERT_STR_EQ(codebuddy->display_name, "CodeBuddy Code CLI");
    ASSERT_STR_EQ(codebuddy->detection_command, "codebuddy");
    ASSERT_EQ(ibm_bob_ide->stability, CBM_AGENT_CONDITIONAL);
    ASSERT_STR_EQ(ibm_bob_ide->display_name, "IBM Bob IDE");
    ASSERT_NULL(ibm_bob_ide->detection_command);
    ASSERT_EQ(ibm_bob_shell->stability, CBM_AGENT_STABLE);
    ASSERT_STR_EQ(ibm_bob_shell->display_name, "IBM Bob Shell");
    ASSERT_STR_EQ(ibm_bob_shell->detection_command, "bob");
    ASSERT_EQ(pochi->stability, CBM_AGENT_STABLE);
    ASSERT(pochi->capabilities & CBM_AGENT_CAP_AGENT);
    ASSERT_STR_EQ(pochi->detection_command, "pochi");
    ASSERT(!(pi->capabilities & CBM_AGENT_CAP_MCP));
    ASSERT(pi->capabilities & CBM_AGENT_CAP_INSTRUCTIONS);
    ASSERT(pi->capabilities & CBM_AGENT_CAP_SKILL);
    ASSERT_NULL(pi->install_mcp);
    ASSERT_NULL(pi->remove_mcp);
    ASSERT_EQ(cody->stability, CBM_AGENT_OPT_IN);
    ASSERT_EQ(cody->capabilities, CBM_AGENT_CAP_MCP);
    ASSERT_NOT_NULL(cody->install_mcp);
    ASSERT_NOT_NULL(cody->remove_mcp);
    PASS();
}

TEST(agent_clients_resolve_documented_paths_and_precedence) {
    agent_probe_t probe = {0};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    char path[512];

    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_QODER, &options, path, sizeof(path)),
              0);
    ASSERT_STR_EQ(path, "/home/tester/.qoder/settings.json");
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_KIMI, &options, path, sizeof(path)),
              0);
    ASSERT_STR_EQ(path, "/home/tester/.kimi-code/mcp.json");
    ASSERT_EQ(
        cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_GITLAB_DUO, &options, path, sizeof(path)),
        0);
    ASSERT_STR_EQ(path, "/home/tester/.gitlab/duo/mcp.json");

    options.kimi_code_home = "/opt/kimi home";
    options.glab_config_dir = "/opt/gitlab";
    options.xdg_config_home = "/xdg";
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_KIMI, &options, path, sizeof(path)),
              0);
    ASSERT_STR_EQ(path, "/opt/kimi home/mcp.json");
    ASSERT_EQ(
        cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_GITLAB_DUO, &options, path, sizeof(path)),
        0);
    ASSERT_STR_EQ(path, "/opt/gitlab/duo/mcp.json");
    options.glab_config_dir = NULL;
    ASSERT_EQ(
        cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_GITLAB_DUO, &options, path, sizeof(path)),
        0);
    ASSERT_STR_EQ(path, "/xdg/gitlab/duo/mcp.json");
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_AMP, &options, path, sizeof(path)), 0);
    ASSERT_STR_EQ(path, "/home/tester/.config/agents/skills/codebase-memory/mcp.json");
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_DEVIN, &options, path, sizeof(path)),
              0);
    ASSERT_STR_EQ(path, "/home/tester/.config/devin/config.json");

    options.is_windows = true;
    options.appdata_dir = "/roaming";
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_DEVIN, &options, path, sizeof(path)),
              0);
    ASSERT_STR_EQ(path, "/roaming/devin/config.json");
    PASS();
}

TEST(agent_clients_resolve_rovo_override_and_conditional_paths) {
    char *dir = th_mktempdir("cbm_agent_clients_rovo");
    ASSERT_NOT_NULL(dir);
    char config[1024];
    char safe_override[1024];
    ASSERT(snprintf(config, sizeof(config), "%s/.rovodev/config.yml", dir) > 0);
    ASSERT(snprintf(safe_override, sizeof(safe_override), "%s/.rovodev/custom/rovo mcp.json", dir) >
           0);
    ASSERT_EQ(agent_write_rovo_override(config, safe_override), 0);
    agent_probe_t probe = {.paths = {config}, .path_count = 1U};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    options.home_dir = dir;
#ifdef _WIN32
    options.is_windows = true;
#endif
    char path[1024];
    ASSERT_EQ(
        cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_ROVO_DEV, &options, path, sizeof(path)), 0);
    ASSERT_STR_EQ(path, safe_override);

    ASSERT_EQ(
        cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_CONTINUE, &options, path, sizeof(path)), 1);
    options.continue_config_path = config;
    ASSERT_EQ(
        cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_CONTINUE, &options, path, sizeof(path)), 0);
    ASSERT_STR_EQ(path, config);
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_TRAE, &options, path, sizeof(path)),
              1);
    options.trae_config_path = "/missing/trae.json";
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_TRAE, &options, path, sizeof(path)),
              1);
    probe.paths[probe.path_count++] = options.trae_config_path;
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_TRAE, &options, path, sizeof(path)),
              0);
    ASSERT_STR_EQ(path, options.trae_config_path);
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_rovo_override_rejects_relative_and_traversal_paths) {
    static const char *invalid_paths[] = {
        "mcp.json",
        "../outside.json",
        "~/.rovodev/../outside.json",
    };
    char *dir = th_mktempdir("cbm_agent_clients_rovo_relative");
    ASSERT_NOT_NULL(dir);
    char config[1024];
    ASSERT(snprintf(config, sizeof(config), "%s/.rovodev/config.yml", dir) > 0);
    agent_probe_t probe = {.paths = {config}, .path_count = 1U};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    options.home_dir = dir;
#ifdef _WIN32
    options.is_windows = true;
#endif

    for (size_t i = 0U; i < sizeof(invalid_paths) / sizeof(invalid_paths[0]); i++) {
        ASSERT_EQ(agent_write_rovo_override(config, invalid_paths[i]), 0);
        char resolved[1024] = "untouched-on-rejection";
        ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_ROVO_DEV, &options, resolved,
                                                sizeof(resolved)),
                  -1);
        ASSERT_STR_EQ(resolved, "untouched-on-rejection");
    }
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_rovo_override_rejects_absolute_paths_outside_user_root) {
    char *dir = th_mktempdir("cbm_agent_clients_rovo_absolute");
    ASSERT_NOT_NULL(dir);
    char config[1024];
    char outside[1024];
    char prefix_collision[1024];
    char absolute_traversal[1024];
    ASSERT(snprintf(config, sizeof(config), "%s/.rovodev/config.yml", dir) > 0);
    ASSERT(snprintf(outside, sizeof(outside), "%s/outside.json", dir) > 0);
    ASSERT(snprintf(prefix_collision, sizeof(prefix_collision), "%s/.rovodev-shadow/mcp.json",
                    dir) > 0);
    ASSERT(snprintf(absolute_traversal, sizeof(absolute_traversal),
                    "%s/.rovodev/nested/../../outside.json", dir) > 0);
    const char *invalid_paths[] = {outside, prefix_collision, absolute_traversal};
    agent_probe_t probe = {.paths = {config}, .path_count = 1U};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    options.home_dir = dir;
#ifdef _WIN32
    options.is_windows = true;
#endif

    for (size_t i = 0U; i < sizeof(invalid_paths) / sizeof(invalid_paths[0]); i++) {
        ASSERT_EQ(agent_write_rovo_override(config, invalid_paths[i]), 0);
        char resolved[1024] = "untouched-on-rejection";
        ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_ROVO_DEV, &options, resolved,
                                                sizeof(resolved)),
                  -1);
        ASSERT_STR_EQ(resolved, "untouched-on-rejection");
    }
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_rovo_compatibility_prefers_existing_documented_filename) {
    agent_probe_t probe = {0};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    char path[512];
    probe.paths[probe.path_count++] = "/home/tester/.rovodev/mcp_config.json";
    ASSERT_EQ(
        cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_ROVO_DEV, &options, path, sizeof(path)), 0);
    ASSERT_STR_EQ(path, "/home/tester/.rovodev/mcp_config.json");
    probe.paths[probe.path_count++] = "/home/tester/.rovodev/mcp.json";
    ASSERT_EQ(
        cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_ROVO_DEV, &options, path, sizeof(path)), 0);
    ASSERT_STR_EQ(path, "/home/tester/.rovodev/mcp.json");
    probe.path_count = 0U;
    ASSERT_EQ(
        cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_ROVO_DEV, &options, path, sizeof(path)), 0);
    ASSERT_STR_EQ(path, "/home/tester/.rovodev/mcp.json");
    PASS();
}

TEST(agent_clients_detection_avoids_generic_binary_false_positives) {
    agent_probe_t probe = {.commands = {"glab", "acli"}, .command_count = 2U};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_GITLAB_DUO, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_ROVO_DEV, &options));
    probe.commands[probe.command_count++] = "duo";
    probe.commands[probe.command_count++] = "rovodev";
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_GITLAB_DUO, &options));
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_ROVO_DEV, &options));

    probe.commands[probe.command_count++] = "cn";
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_CONTINUE, &options));
    options.is_windows = false;
    probe.commands[probe.command_count++] = "devenv";
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_VISUAL_STUDIO, &options));
    options.is_windows = true;
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_VISUAL_STUDIO, &options));
    PASS();
}

TEST(agent_clients_detect_installed_client_directories_before_mcp_exists) {
    agent_probe_t probe = {0};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);

    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_QODER, &options));
    probe.paths[0] = "/home/tester/.qoder";
    probe.path_count = 1U;
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_QODER, &options));

    probe.path_count = 0U;
    options.kimi_code_home = "/opt/kimi-code-home";
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_KIMI, &options));
    probe.paths[0] = options.kimi_code_home;
    probe.path_count = 1U;
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_KIMI, &options));

    probe.path_count = 0U;
    options.glab_config_dir = "/opt/gitlab";
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_GITLAB_DUO, &options));
    probe.paths[0] = "/opt/gitlab/duo";
    probe.path_count = 1U;
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_GITLAB_DUO, &options));

    probe.path_count = 0U;
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_ROVO_DEV, &options));
    probe.paths[0] = "/home/tester/.rovodev";
    probe.path_count = 1U;
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_ROVO_DEV, &options));

    probe.path_count = 0U;
    options.glab_config_dir = NULL;
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_AMP, &options));
    probe.paths[0] = "/home/tester/.config/amp";
    probe.path_count = 1U;
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_AMP, &options));

    probe.path_count = 0U;
    options.xdg_config_home = "/xdg";
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_DEVIN, &options));
    probe.paths[0] = "/home/tester/.config/devin";
    probe.path_count = 1U;
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_DEVIN, &options));

    probe.path_count = 0U;
    options.xdg_config_home = NULL;
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_TABNINE, &options));
    probe.paths[0] = "/home/tester/.tabnine";
    probe.path_count = 1U;
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_TABNINE, &options));

    probe.path_count = 0U;
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_AMAZON_Q, &options));
    probe.paths[0] = "/home/tester/.aws/amazonq";
    probe.path_count = 1U;
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_AMAZON_Q, &options));

    probe.path_count = 0U;
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_PI, &options));
    probe.paths[0] = "/home/tester/.pi/agent";
    probe.path_count = 1U;
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_PI, &options));
    PASS();
}

TEST(agent_clients_marker_detection_remains_fail_closed_for_conditional_clients) {
    agent_probe_t probe = {
        .paths = {"/home/tester/.config/agents", "/home/tester/.continue", "/home/tester/.roo",
                  "/home/tester/.config/Code/User"},
        .path_count = 4U,
        .commands = {"glab", "acli", "code", "cody", "cn", "roo", "trae"},
        .command_count = 7U,
    };
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_GITLAB_DUO, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_ROVO_DEV, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_AMP, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_CONTINUE, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_ROO_CODE, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_TRAE, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, &options));

    options.continue_config_path = "/active/continue.yaml";
    options.roo_config_path = "/active/roo.json";
    options.trae_config_path = "/active/trae.json";
    options.cody_config_path = "/active/cody-settings.json";
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_CONTINUE, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_ROO_CODE, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_TRAE, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, &options));
    probe.paths[probe.path_count++] = options.continue_config_path;
    probe.paths[probe.path_count++] = options.roo_config_path;
    probe.paths[probe.path_count++] = options.trae_config_path;
    probe.paths[probe.path_count++] = options.cody_config_path;
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_CONTINUE, &options));
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_ROO_CODE, &options));
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_TRAE, &options));
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, &options));

    options.is_windows = true;
    probe.paths[probe.path_count++] = "/home/tester/.mcp.json";
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_VISUAL_STUDIO, &options));
    probe.commands[probe.command_count++] = "devenv";
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_VISUAL_STUDIO, &options));
    PASS();
}

TEST(agent_clients_roo_code_requires_an_explicit_existing_config) {
    agent_probe_t probe = {0};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    char resolved[512];
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_ROO_CODE, &options, resolved,
                                            sizeof(resolved)),
              1);
    options.roo_config_path = "/work/project/.roo/mcp.json";
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_ROO_CODE, &options, resolved,
                                            sizeof(resolved)),
              1);
    probe.paths[probe.path_count++] = options.roo_config_path;
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_ROO_CODE, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, options.roo_config_path);
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_ROO_CODE, &options));

    char *dir = NULL;
    char *path = agent_fixture("{}\n", &dir);
    ASSERT_NOT_NULL(path);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_ROO_CODE, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_OK);
    char *installed = agent_read(path);
    ASSERT_NOT_NULL(installed);
    ASSERT_NOT_NULL(strstr(installed, "\"mcpServers\""));
    ASSERT_NULL(strstr(installed, "alwaysAllow"));
    free(installed);
    free(path);
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_amazon_q_prefers_current_then_existing_legacy_config) {
    agent_probe_t probe = {0};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    char resolved[512];
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_AMAZON_Q, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, "/home/tester/.aws/amazonq/default.json");

    probe.paths[probe.path_count++] = "/home/tester/.aws/amazonq/mcp.json";
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_AMAZON_Q, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, "/home/tester/.aws/amazonq/mcp.json");
    probe.paths[probe.path_count++] = "/home/tester/.aws/amazonq/agents/default.json";
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_AMAZON_Q, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, "/home/tester/.aws/amazonq/agents/default.json");
    probe.paths[probe.path_count++] = "/home/tester/.aws/amazonq/default.json";
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_AMAZON_Q, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, "/home/tester/.aws/amazonq/default.json");

    char *dir = NULL;
    char *path = agent_fixture("{\"permissions\":{\"keep\":true}}\n", &dir);
    ASSERT_NOT_NULL(path);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_AMAZON_Q, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_OK);
    char *installed = agent_read(path);
    ASSERT_NOT_NULL(installed);
    ASSERT_NOT_NULL(strstr(installed, "\"mcpServers\""));
    ASSERT_NOT_NULL(strstr(installed, "\"permissions\":{\"keep\":true}"));
    ASSERT_EQ(agent_occurrences(installed, "\"permissions\""), 1U);
    ASSERT_NULL(strstr(installed, "alwaysAllow"));
    ASSERT_NULL(strstr(installed, "autoApprove"));
    free(installed);
    free(path);
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_codebuddy_bob_and_pochi_use_documented_global_paths) {
    agent_probe_t probe = {0};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    char resolved[512];

    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_CODEBUDDY, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, "/home/tester/.codebuddy/.mcp.json");
    probe.paths[probe.path_count++] = "/home/tester/.codebuddy.json";
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_CODEBUDDY, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, "/home/tester/.codebuddy.json");
    probe.paths[probe.path_count++] = "/home/tester/.codebuddy/mcp.json";
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_CODEBUDDY, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, "/home/tester/.codebuddy/mcp.json");
    probe.paths[probe.path_count++] = "/home/tester/.codebuddy/.mcp.json";
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_CODEBUDDY, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, "/home/tester/.codebuddy/.mcp.json");
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_IBM_BOB_IDE, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, "/home/tester/.bob/mcp.json");
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_IBM_BOB_SHELL, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, "/home/tester/.bob/mcp_settings.json");
    ASSERT_EQ(
        cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_POCHI, &options, resolved, sizeof(resolved)),
        0);
    ASSERT_STR_EQ(resolved, "/home/tester/.pochi/config.jsonc");

    probe.path_count = 0U;
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_CODEBUDDY, &options));
    probe.commands[probe.command_count++] = "codebuddy";
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_CODEBUDDY, &options));
    probe.command_count = 0U;
    probe.paths[probe.path_count++] = "/home/tester/.codebuddy";
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_CODEBUDDY, &options));

    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_IBM_BOB_IDE, &options));
    probe.paths[probe.path_count++] = "/home/tester/.bob";
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_IBM_BOB_IDE, &options));
    probe.paths[probe.path_count++] = "/home/tester/.bob/mcp.json";
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_IBM_BOB_IDE, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_IBM_BOB_SHELL, &options));
    probe.commands[probe.command_count++] = "bob";
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_IBM_BOB_SHELL, &options));
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_POCHI, &options));
    probe.commands[probe.command_count++] = "pochi";
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_POCHI, &options));
    PASS();
}

TEST(agent_clients_pi_has_no_mcp_path_or_mutation) {
    agent_probe_t probe = {.commands = {"pi"}, .command_count = 1U};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    char resolved[512];
    ASSERT_EQ(
        cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_PI, &options, resolved, sizeof(resolved)),
        1);
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_PI, &options));

    const char *original = "{\"keep\":true}\n";
    char *dir = NULL;
    char *path = agent_fixture(original, &dir);
    ASSERT_NOT_NULL(path);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_PI, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_NOT_APPLICABLE);
    char *after = agent_read(path);
    ASSERT_STR_EQ(after, original);
    free(after);
    free(path);
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_cody_is_opt_in_and_requires_explicit_existing_settings) {
    agent_probe_t probe = {.commands = {"code", "cody"}, .command_count = 2U};
    cbm_agent_client_resolve_options_t options = agent_options(&probe);
    char resolved[512];
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, &options, resolved,
                                            sizeof(resolved)),
              1);
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, &options));
    options.cody_config_path = "/home/tester/.config/Code/User/settings.json";
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, &options, resolved,
                                            sizeof(resolved)),
              1);
    ASSERT(!cbm_agent_client_detect(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, &options));
    probe.paths[probe.path_count++] = options.cody_config_path;
    ASSERT_EQ(cbm_agent_client_resolve_path(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, &options, resolved,
                                            sizeof(resolved)),
              0);
    ASSERT_STR_EQ(resolved, options.cody_config_path);
    ASSERT(cbm_agent_client_detect(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, &options));
    PASS();
}

TEST(agent_clients_cody_uses_literal_dotted_key_without_feature_or_permission_edits) {
    const char *initial = "{\n  // user policy\n  \"cody.enabled\": false,\n"
                          "  \"cody.permissions\": {\"trusted\": false},\n"
                          "  \"cody.experimental.agent\": false\n}\n";
    char *dir = NULL;
    char *path = agent_fixture(initial, &dir);
    ASSERT_NOT_NULL(path);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_OK);
    char *installed = agent_read(path);
    ASSERT_NOT_NULL(installed);
    ASSERT_NOT_NULL(strstr(installed, "\"cody.mcpServers\""));
    ASSERT_EQ(agent_occurrences(installed, "\"mcpServers\""), 0U);
    ASSERT_NOT_NULL(strstr(installed, "\"cody.enabled\": false"));
    ASSERT_NOT_NULL(strstr(installed, "\"cody.permissions\": {\"trusted\": false}"));
    ASSERT_NOT_NULL(strstr(installed, "\"cody.experimental.agent\": false"));
    ASSERT_EQ(agent_occurrences(installed, "\"cody.enabled\""), 1U);
    ASSERT_EQ(agent_occurrences(installed, "\"cody.permissions\""), 1U);
    ASSERT_EQ(agent_occurrences(installed, "\"cody.experimental.agent\""), 1U);
    ASSERT_NULL(strstr(installed, "alwaysAllow"));
    ASSERT_NULL(strstr(installed, "autoApprove"));
    free(installed);

    const char *foreign = "{\"cody.mcpServers\":{\"codebase-memory-mcp\":{\"command\":\"foreign\","
                          "\"args\":[]}},\"cody.enabled\":false}\n";
    ASSERT_EQ(th_write_file(path, foreign), 0);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_FOREIGN);
    char *after = agent_read(path);
    ASSERT_STR_EQ(after, foreign);
    free(after);
    free(path);
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_json_schemas_are_exact_and_policy_neutral) {
    static const struct {
        cbm_agent_client_id_t id;
        const char *required;
    } cases[] = {
        {CBM_AGENT_CLIENT_QODER, "\"mcpServers\""},
        {CBM_AGENT_CLIENT_KIMI, "\"mcpServers\""},
        {CBM_AGENT_CLIENT_GITLAB_DUO, "\"type\": \"stdio\""},
        {CBM_AGENT_CLIENT_ROVO_DEV, "\"transport\": \"stdio\""},
        {CBM_AGENT_CLIENT_AMP, "\"codebase-memory-mcp\""},
        {CBM_AGENT_CLIENT_DEVIN, "\"mcpServers\""},
        {CBM_AGENT_CLIENT_TABNINE, "\"mcpServers\""},
        {CBM_AGENT_CLIENT_VISUAL_STUDIO, "\"servers\""},
        {CBM_AGENT_CLIENT_TRAE, "\"mcpServers\""},
        {CBM_AGENT_CLIENT_CODEBUDDY, "\"mcpServers\""},
        {CBM_AGENT_CLIENT_IBM_BOB_IDE, "\"mcpServers\""},
        {CBM_AGENT_CLIENT_IBM_BOB_SHELL, "\"mcpServers\""},
        {CBM_AGENT_CLIENT_POCHI, "\"mcp\""},
    };
    const char *binary = "/opt/Codebase Memory/bin/cbm\\\"special";
    for (size_t i = 0U; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char *dir = NULL;
        char *path = agent_fixture("{\n  // user-owned\n  \"keep\": true,\n}\n", &dir);
        ASSERT_NOT_NULL(path);
        ASSERT_EQ(cbm_agent_client_install_mcp(cases[i].id, path, binary), CBM_AGENT_EDIT_OK);
        char *first = agent_read(path);
        ASSERT_NOT_NULL(first);
        ASSERT_NOT_NULL(strstr(first, cases[i].required));
        ASSERT_NOT_NULL(strstr(first, "\"command\""));
        ASSERT_NOT_NULL(strstr(first, "\"args\": []"));
        ASSERT_NULL(strstr(first, "approvedTools"));
        ASSERT_NULL(strstr(first, "allowedTools"));
        ASSERT_NULL(strstr(first, "enable_instructions"));
        ASSERT_NULL(strstr(first, "autoApprove"));
        ASSERT_NULL(strstr(first, "alwaysAllow"));
        ASSERT_NULL(strstr(first, "allowlist"));
        ASSERT_NULL(strstr(first, "permissions"));
        ASSERT_EQ(cbm_agent_client_install_mcp(cases[i].id, path, binary), CBM_AGENT_EDIT_OK);
        char *second = agent_read(path);
        ASSERT_NOT_NULL(second);
        ASSERT_STR_EQ(first, second);
        free(first);
        free(second);
        free(path);
        th_cleanup(dir);
    }
    PASS();
}

TEST(agent_clients_new_standard_json_profiles_preserve_foreign_entries) {
    static const cbm_agent_client_id_t clients[] = {
        CBM_AGENT_CLIENT_CODEBUDDY,
        CBM_AGENT_CLIENT_IBM_BOB_IDE,
        CBM_AGENT_CLIENT_IBM_BOB_SHELL,
        CBM_AGENT_CLIENT_POCHI,
    };
    for (size_t i = 0U; i < sizeof(clients) / sizeof(clients[0]); i++) {
        const char *foreign =
            clients[i] == CBM_AGENT_CLIENT_POCHI
                ? "{\"mcp\":{\"codebase-memory-mcp\":{\"command\":\"foreign\","
                  "\"args\":[]}}}\n"
                : "{\"mcpServers\":{\"codebase-memory-mcp\":{\"command\":\"foreign\","
                  "\"args\":[]}}}\n";
        char *dir = NULL;
        char *path = agent_fixture(foreign, &dir);
        ASSERT_NOT_NULL(path);
        ASSERT_EQ(cbm_agent_client_install_mcp(clients[i], path, "/usr/bin/cbm"),
                  CBM_AGENT_EDIT_FOREIGN);
        char *after = agent_read(path);
        ASSERT_NOT_NULL(after);
        ASSERT_STR_EQ(after, foreign);
        free(after);
        free(path);
        th_cleanup(dir);
    }
    PASS();
}

TEST(agent_clients_refuse_foreign_and_preserve_modified_entries) {
    const char *foreign =
        "{\"mcpServers\":{\"codebase-memory-mcp\":{\"command\":\"foreign\",\"args\":[]}}}\n";
    char *dir = NULL;
    char *path = agent_fixture(foreign, &dir);
    ASSERT_NOT_NULL(path);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_QODER, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_FOREIGN);
    char *after = agent_read(path);
    ASSERT_STR_EQ(after, foreign);
    free(after);

    const char *escaped_foreign =
        "{mcpServers:{\"codebase\\u002dmemory-mcp\":{command:'foreign',args:[]}}}\n";
    ASSERT_EQ(th_write_file(path, escaped_foreign), 0);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_QODER, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_FOREIGN);
    after = agent_read(path);
    ASSERT_STR_EQ(after, escaped_foreign);
    free(after);

    ASSERT_EQ(th_write_file(path, "{}\n"), 0);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_QODER, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_OK);
    char *canonical = agent_read(path);
    ASSERT_NOT_NULL(canonical);
    char *command = strstr(canonical, "/usr/bin/cbm");
    ASSERT_NOT_NULL(command);
    command[0] = 'X';
    ASSERT_EQ(th_write_file(path, canonical), 0);
    ASSERT_EQ(cbm_agent_client_remove_mcp(CBM_AGENT_CLIENT_QODER, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_FOREIGN);
    after = agent_read(path);
    ASSERT_STR_EQ(after, canonical);
    free(after);
    free(canonical);
    free(path);
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_remove_only_canonical_and_missing_is_noop) {
    char *dir = NULL;
    char *path = agent_fixture("{\"keep\":true}\n", &dir);
    ASSERT_NOT_NULL(path);
    ASSERT_EQ(cbm_agent_client_remove_mcp(CBM_AGENT_CLIENT_GITLAB_DUO, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_OK);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_GITLAB_DUO, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_OK);
    ASSERT_EQ(cbm_agent_client_remove_mcp(CBM_AGENT_CLIENT_GITLAB_DUO, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_OK);
    char *after = agent_read(path);
    ASSERT_NOT_NULL(after);
    ASSERT_NULL(strstr(after, "codebase-memory-mcp"));
    ASSERT_NOT_NULL(strstr(after, "\"keep\":true"));
    free(after);
    free(path);
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_registry_callbacks_apply_the_selected_schema) {
    char *dir = NULL;
    char *path = agent_fixture("{}\n", &dir);
    ASSERT_NOT_NULL(path);
    const cbm_agent_client_profile_t *profile =
        cbm_agent_client_by_id(CBM_AGENT_CLIENT_VISUAL_STUDIO);
    ASSERT_NOT_NULL(profile);
    ASSERT_EQ(profile->install_mcp(profile->id, path, "C:/Tools/cbm.exe"), CBM_AGENT_EDIT_OK);
    char *installed = agent_read(path);
    ASSERT_NOT_NULL(installed);
    ASSERT_NOT_NULL(strstr(installed, "\"servers\""));
    ASSERT_NOT_NULL(strstr(installed, "\"type\": \"stdio\""));
    free(installed);
    ASSERT_EQ(profile->remove_mcp(profile->id, path, "C:/Tools/cbm.exe"), CBM_AGENT_EDIT_OK);
    free(path);
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_malformed_json_fails_byte_identically) {
    const char *malformed = "{\"mcpServers\": [}\n";
    char *dir = NULL;
    char *path = agent_fixture(malformed, &dir);
    ASSERT_NOT_NULL(path);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_TABNINE, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_ERROR);
    char *after = agent_read(path);
    ASSERT_STR_EQ(after, malformed);
    free(after);
    free(path);
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_deep_same_name_json_fails_closed_byte_identically) {
    char *deep = agent_deep_same_name_json(256U);
    ASSERT_NOT_NULL(deep);
    char *dir = NULL;
    char *path = agent_fixture(deep, &dir);
    ASSERT_NOT_NULL(path);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_QODER, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_ERROR);
    char *after_install = agent_read(path);
    ASSERT_NOT_NULL(after_install);
    ASSERT_STR_EQ(after_install, deep);
    free(after_install);

    ASSERT_EQ(cbm_agent_client_remove_mcp(CBM_AGENT_CLIENT_QODER, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_ERROR);
    char *after_remove = agent_read(path);
    ASSERT_NOT_NULL(after_remove);
    ASSERT_STR_EQ(after_remove, deep);
    free(after_remove);
    free(path);
    th_cleanup(dir);
    free(deep);
    PASS();
}

TEST(agent_clients_continue_uses_owned_yaml_sequence_item) {
    const char *initial = "name: Local\nmcpServers:\n  - name: other\n    command: other\n";
    char *dir = NULL;
    char *path = agent_fixture(initial, &dir);
    ASSERT_NOT_NULL(path);
    ASSERT_EQ(
        cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_CONTINUE, path, "/opt/cbm path/#quoted"),
        CBM_AGENT_EDIT_OK);
    char *first = agent_read(path);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(strstr(first, "  - name: codebase-memory-mcp\n"));
    ASSERT_NOT_NULL(strstr(first, "    command: \"/opt/cbm path/#quoted\"\n"));
    ASSERT_NOT_NULL(strstr(first, "  - name: other\n"));
    ASSERT_EQ(agent_occurrences(first, "name: codebase-memory-mcp"), 1U);
    ASSERT_EQ(
        cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_CONTINUE, path, "/opt/cbm path/#quoted"),
        CBM_AGENT_EDIT_OK);
    char *second = agent_read(path);
    ASSERT_STR_EQ(first, second);
    ASSERT_EQ(cbm_agent_client_remove_mcp(CBM_AGENT_CLIENT_CONTINUE, path, "/opt/cbm path/#quoted"),
              CBM_AGENT_EDIT_OK);
    char *removed = agent_read(path);
    ASSERT_NULL(strstr(removed, "name: codebase-memory-mcp"));
    ASSERT_NOT_NULL(strstr(removed, "name: other"));
    free(first);
    free(second);
    free(removed);
    free(path);
    th_cleanup(dir);
    PASS();
}

TEST(agent_clients_continue_refuses_foreign_same_name_and_nonsequence_section) {
    const char *foreign = "mcpServers:\n  - name: \"codebase-memory-mcp\"\n"
                          "    command: foreign\n    args: []\n";
    char *dir = NULL;
    char *path = agent_fixture(foreign, &dir);
    ASSERT_NOT_NULL(path);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_CONTINUE, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_FOREIGN);
    char *after = agent_read(path);
    ASSERT_STR_EQ(after, foreign);
    free(after);

    const char *wrong_shape = "mcpServers: []\nkeep: true\n";
    ASSERT_EQ(th_write_file(path, wrong_shape), 0);
    ASSERT_EQ(cbm_agent_client_install_mcp(CBM_AGENT_CLIENT_CONTINUE, path, "/usr/bin/cbm"),
              CBM_AGENT_EDIT_ERROR);
    after = agent_read(path);
    ASSERT_STR_EQ(after, wrong_shape);
    free(after);
    free(path);
    th_cleanup(dir);
    PASS();
}

SUITE(agent_clients) {
    RUN_TEST(agent_clients_registry_is_stable_and_callback_driven);
    RUN_TEST(agent_clients_next_wave_metadata_matches_supported_surfaces);
    RUN_TEST(agent_clients_resolve_documented_paths_and_precedence);
    RUN_TEST(agent_clients_resolve_rovo_override_and_conditional_paths);
    RUN_TEST(agent_clients_rovo_override_rejects_relative_and_traversal_paths);
    RUN_TEST(agent_clients_rovo_override_rejects_absolute_paths_outside_user_root);
    RUN_TEST(agent_clients_rovo_compatibility_prefers_existing_documented_filename);
    RUN_TEST(agent_clients_detection_avoids_generic_binary_false_positives);
    RUN_TEST(agent_clients_visual_studio_cleanup_survives_missing_command);
    RUN_TEST(agent_clients_detect_installed_client_directories_before_mcp_exists);
    RUN_TEST(agent_clients_marker_detection_remains_fail_closed_for_conditional_clients);
    RUN_TEST(agent_clients_roo_code_requires_an_explicit_existing_config);
    RUN_TEST(agent_clients_amazon_q_prefers_current_then_existing_legacy_config);
    RUN_TEST(agent_clients_codebuddy_bob_and_pochi_use_documented_global_paths);
    RUN_TEST(agent_clients_pi_has_no_mcp_path_or_mutation);
    RUN_TEST(agent_clients_cody_is_opt_in_and_requires_explicit_existing_settings);
    RUN_TEST(agent_clients_cody_uses_literal_dotted_key_without_feature_or_permission_edits);
    RUN_TEST(agent_clients_json_schemas_are_exact_and_policy_neutral);
    RUN_TEST(agent_clients_new_standard_json_profiles_preserve_foreign_entries);
    RUN_TEST(agent_clients_refuse_foreign_and_preserve_modified_entries);
    RUN_TEST(agent_clients_remove_only_canonical_and_missing_is_noop);
    RUN_TEST(agent_clients_registry_callbacks_apply_the_selected_schema);
    RUN_TEST(agent_clients_malformed_json_fails_byte_identically);
    RUN_TEST(agent_clients_deep_same_name_json_fails_closed_byte_identically);
    RUN_TEST(agent_clients_continue_uses_owned_yaml_sequence_item);
    RUN_TEST(agent_clients_continue_refuses_foreign_same_name_and_nonsequence_section);
}
