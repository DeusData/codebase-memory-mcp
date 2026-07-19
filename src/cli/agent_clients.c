/*
 * agent_clients.c — Table-driven MCP adapters for agent clients.
 *
 * Resolution is deliberately separate from mutation. Callers decide which
 * detected clients are in scope, then pass the exact resolved config path to
 * the ownership-preserving editor.
 */
#include "agent_clients.h"

#include "config_json_like.h"
#include "config_text_edit.h"
#include "config_yaml_edit.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define AGENT_ENTRY_KEY "codebase-memory-mcp"
#define AGENT_MAX_CONFIG_BYTES (8U * 1024U * 1024U)
#define AGENT_JSON_MAX_DEPTH 64U

static int agent_install_callback(cbm_agent_client_id_t id, const char *config_path,
                                  const char *binary_path);
static int agent_remove_callback(cbm_agent_client_id_t id, const char *config_path,
                                 const char *binary_path);

static const cbm_agent_client_profile_t agent_profiles[CBM_AGENT_CLIENT_COUNT] = {
    {CBM_AGENT_CLIENT_QODER, "qoder", "Qoder CLI", CBM_AGENT_STABLE,
     CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_AGENT | CBM_AGENT_CAP_HOOK, "qodercli",
     agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_KIMI, "kimi", "Kimi Code CLI", CBM_AGENT_STABLE,
     CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_HOOK,
     "kimi", agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_GITLAB_DUO, "gitlab-duo", "GitLab Duo CLI", CBM_AGENT_STABLE,
     CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_HOOK, "duo", agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_ROVO_DEV, "rovo-dev", "Rovo Dev CLI", CBM_AGENT_STABLE,
     CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_AGENT,
     "rovodev", agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_AMP, "amp", "Amp", CBM_AGENT_STABLE,
     CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL, "amp",
     agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_DEVIN, "devin", "Devin CLI / Local", CBM_AGENT_STABLE,
     CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_HOOK,
     "devin", agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_TABNINE, "tabnine", "Tabnine", CBM_AGENT_STABLE, CBM_AGENT_CAP_MCP, "tabnine",
     agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_CONTINUE, "continue", "Continue / cn", CBM_AGENT_CONDITIONAL,
     CBM_AGENT_CAP_MCP, "cn", agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_VISUAL_STUDIO, "visual-studio", "Visual Studio", CBM_AGENT_CONDITIONAL,
     CBM_AGENT_CAP_MCP, "devenv", agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_TRAE, "trae", "TRAE", CBM_AGENT_CONDITIONAL, CBM_AGENT_CAP_MCP, NULL,
     agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_ROO_CODE, "roo-code", "Roo Code", CBM_AGENT_CONDITIONAL, CBM_AGENT_CAP_MCP,
     NULL, agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_AMAZON_Q, "amazon-q", "Amazon Q Developer IDE", CBM_AGENT_STABLE,
     CBM_AGENT_CAP_MCP, NULL, agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_CODEBUDDY, "codebuddy", "CodeBuddy Code CLI", CBM_AGENT_STABLE,
     CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_AGENT,
     "codebuddy", agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_IBM_BOB_IDE, "ibm-bob-ide", "IBM Bob IDE", CBM_AGENT_CONDITIONAL,
     CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL, NULL,
     agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_IBM_BOB_SHELL, "ibm-bob-shell", "IBM Bob Shell", CBM_AGENT_STABLE,
     CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS, "bob", agent_install_callback,
     agent_remove_callback},
    {CBM_AGENT_CLIENT_POCHI, "pochi", "Pochi", CBM_AGENT_STABLE,
     CBM_AGENT_CAP_MCP | CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL | CBM_AGENT_CAP_AGENT,
     "pochi", agent_install_callback, agent_remove_callback},
    {CBM_AGENT_CLIENT_PI, "pi", "Pi", CBM_AGENT_STABLE,
     CBM_AGENT_CAP_INSTRUCTIONS | CBM_AGENT_CAP_SKILL, "pi", NULL, NULL},
    {CBM_AGENT_CLIENT_SOURCEGRAPH_CODY, "sourcegraph-cody", "Sourcegraph Cody", CBM_AGENT_OPT_IN,
     CBM_AGENT_CAP_MCP, NULL, agent_install_callback, agent_remove_callback},
};

size_t cbm_agent_client_count(void) {
    return CBM_AGENT_CLIENT_COUNT;
}

const cbm_agent_client_profile_t *cbm_agent_client_at(size_t index) {
    return index < CBM_AGENT_CLIENT_COUNT ? &agent_profiles[index] : NULL;
}

const cbm_agent_client_profile_t *cbm_agent_client_by_id(cbm_agent_client_id_t id) {
    return (unsigned)id < CBM_AGENT_CLIENT_COUNT ? &agent_profiles[id] : NULL;
}

const cbm_agent_client_profile_t *cbm_agent_client_by_stable_id(const char *stable_id) {
    if (!stable_id || stable_id[0] == '\0') {
        return NULL;
    }
    for (size_t i = 0U; i < CBM_AGENT_CLIENT_COUNT; i++) {
        if (strcmp(agent_profiles[i].stable_id, stable_id) == 0) {
            return &agent_profiles[i];
        }
    }
    return NULL;
}

static bool agent_path_exists(const cbm_agent_client_resolve_options_t *options, const char *path) {
    return options->path_exists ? options->path_exists(path, options->probe_context)
                                : cbm_file_exists(path);
}

static bool agent_command_exists(const cbm_agent_client_resolve_options_t *options,
                                 const char *command) {
    return command && options->command_exists &&
           options->command_exists(command, options->probe_context);
}

static int agent_join_path(char *output, size_t output_size, const char *base, const char *suffix) {
    if (!output || output_size == 0U || !base || base[0] == '\0' || !suffix || suffix[0] == '\0') {
        return -1;
    }
    size_t base_len = strlen(base);
    const char *separator = base[base_len - 1U] == '/' || base[base_len - 1U] == '\\' ? "" : "/";
    int written = snprintf(output, output_size, "%s%s%s", base, separator, suffix);
    return written >= 0 && (size_t)written < output_size ? 0 : -1;
}

static bool agent_path_separator(char value, bool windows) {
    return value == '/' || (windows && value == '\\');
}

/* Lexically normalizes an absolute path without consulting the filesystem.
 * Rovo may point at a config file that does not exist yet, so realpath-style
 * canonicalization is not available. Traversal is rejected rather than
 * resolved, and Windows aliases that trim trailing dots/spaces are rejected. */
static int agent_normalize_absolute_path(const char *input, bool windows, char *output,
                                         size_t output_size) {
    if (!input || !input[0] || !output || output_size == 0U) {
        return -1;
    }
    size_t read = 0U;
    size_t written = 0U;
    bool unc = false;
    if (windows) {
        if (isalpha((unsigned char)input[0]) && input[1] == ':' &&
            agent_path_separator(input[2], true)) {
            if (output_size < 4U) {
                return -1;
            }
            output[written++] = (char)toupper((unsigned char)input[0]);
            output[written++] = ':';
            output[written++] = '/';
            read = 3U;
        } else if (agent_path_separator(input[0], true) && agent_path_separator(input[1], true)) {
            if (output_size < 3U) {
                return -1;
            }
            output[written++] = '/';
            output[written++] = '/';
            read = 2U;
            unc = true;
        } else {
            return -1;
        }
    } else {
        if (input[0] != '/' || output_size < 2U) {
            return -1;
        }
        output[written++] = '/';
        read = 1U;
    }

    size_t components = 0U;
    while (input[read]) {
        while (input[read] && agent_path_separator(input[read], windows)) {
            read++;
        }
        if (!input[read]) {
            break;
        }
        size_t start = read;
        while (input[read] && !agent_path_separator(input[read], windows)) {
            read++;
        }
        size_t length = read - start;
        if (length == 1U && input[start] == '.') {
            continue;
        }
        if (length == 2U && input[start] == '.' && input[start + 1U] == '.') {
            return -1;
        }
        if (windows) {
            if (input[start + length - 1U] == '.' || input[start + length - 1U] == ' ') {
                return -1;
            }
            for (size_t i = 0U; i < length; i++) {
                if (input[start + i] == ':') {
                    return -1;
                }
            }
        }
        bool needs_separator = written > 0U && output[written - 1U] != '/';
        if (length > output_size - written - 1U ||
            (needs_separator && written + 1U >= output_size)) {
            return -1;
        }
        if (needs_separator) {
            output[written++] = '/';
        }
        memcpy(output + written, input + start, length);
        written += length;
        components++;
    }
    if (unc && components < 2U) {
        return -1;
    }
    output[written] = '\0';
    return 0;
}

static bool agent_path_prefix_equal(const char *candidate, const char *root, size_t length,
                                    bool windows) {
    for (size_t i = 0U; i < length; i++) {
        unsigned char left = (unsigned char)candidate[i];
        unsigned char right = (unsigned char)root[i];
        if (windows) {
            left = (unsigned char)tolower(left);
            right = (unsigned char)tolower(right);
        }
        if (left != right) {
            return false;
        }
    }
    return true;
}

static int agent_rovo_safe_override(const char *configured_path, const char *home_dir, bool windows,
                                    char *path_out, size_t path_out_size) {
    size_t home_length = strlen(home_dir);
    size_t configured_length = strlen(configured_path);
    static const char root_suffix[] = ".rovodev";
    if (home_length > SIZE_MAX - sizeof(root_suffix) - 1U) {
        return -1;
    }
    size_t root_capacity = home_length + sizeof(root_suffix) + 1U;
    char *root = (char *)malloc(root_capacity);
    if (!root || agent_join_path(root, root_capacity, home_dir, root_suffix) != 0) {
        free(root);
        return -1;
    }

    bool tilde_path = configured_path[0] == '~';
    if (tilde_path && !agent_path_separator(configured_path[1], windows)) {
        free(root);
        return -1;
    }
    size_t candidate_capacity = configured_length + 1U;
    if (tilde_path) {
        size_t suffix_length = configured_length - 2U;
        if (home_length > SIZE_MAX - suffix_length - 2U) {
            free(root);
            return -1;
        }
        candidate_capacity = home_length + suffix_length + 2U;
    }
    char *candidate = (char *)malloc(candidate_capacity);
    if (!candidate) {
        free(root);
        return -1;
    }
    int candidate_written =
        tilde_path
            ? snprintf(candidate, candidate_capacity, "%s/%s", home_dir, configured_path + 2U)
            : snprintf(candidate, candidate_capacity, "%s", configured_path);
    if (candidate_written < 0 || (size_t)candidate_written >= candidate_capacity) {
        free(candidate);
        free(root);
        return -1;
    }

    char *normalized_root = (char *)calloc(root_capacity, 1U);
    char *normalized_candidate = (char *)calloc(candidate_capacity, 1U);
    if (!normalized_root || !normalized_candidate ||
        agent_normalize_absolute_path(root, windows, normalized_root, root_capacity) != 0 ||
        agent_normalize_absolute_path(candidate, windows, normalized_candidate,
                                      candidate_capacity) != 0) {
        free(normalized_candidate);
        free(normalized_root);
        free(candidate);
        free(root);
        return -1;
    }
    size_t root_length = strlen(normalized_root);
    size_t candidate_length = strlen(normalized_candidate);
    bool contained =
        candidate_length > root_length && normalized_candidate[root_length] == '/' &&
        agent_path_prefix_equal(normalized_candidate, normalized_root, root_length, windows);
    int result = -1;
    if (contained && candidate_length < path_out_size) {
        memcpy(path_out, normalized_candidate, candidate_length + 1U);
        result = 0;
    }
    free(normalized_candidate);
    free(normalized_root);
    free(candidate);
    free(root);
    return result;
}

static char *agent_trim_copy(const char *start, size_t length) {
    while (length > 0U && isspace((unsigned char)*start)) {
        start++;
        length--;
    }
    while (length > 0U && isspace((unsigned char)start[length - 1U])) {
        length--;
    }
    if (length >= 2U && ((start[0] == '"' && start[length - 1U] == '"') ||
                         (start[0] == '\'' && start[length - 1U] == '\''))) {
        start++;
        length -= 2U;
    }
    char *copy = (char *)malloc(length + 1U);
    if (!copy) {
        return NULL;
    }
    memcpy(copy, start, length);
    copy[length] = '\0';
    return copy;
}

/* Reads only the documented `mcp.mcpConfigPath` scalar. Unsupported aliases,
 * block scalars, duplicates, or malformed indentation fail closed. */
static int agent_rovo_override(const char *config_path, const char *home_dir, bool windows,
                               char *path_out, size_t path_out_size) {
    FILE *file = cbm_fopen(config_path, "rb");
    if (!file) {
        return 1;
    }
    if (fseek(file, 0L, SEEK_END) != 0) {
        fclose(file);
        return -1;
    }
    long size = ftell(file);
    if (size < 0L || (size_t)size > AGENT_MAX_CONFIG_BYTES || fseek(file, 0L, SEEK_SET) != 0) {
        fclose(file);
        return -1;
    }
    char *text = (char *)malloc((size_t)size + 1U);
    if (!text) {
        fclose(file);
        return -1;
    }
    size_t read_count = fread(text, 1U, (size_t)size, file);
    int close_rc = fclose(file);
    if (read_count != (size_t)size || close_rc != 0) {
        free(text);
        return -1;
    }
    text[read_count] = '\0';

    bool in_mcp = false;
    char *found = NULL;
    const char *cursor = text;
    while (*cursor) {
        const char *line = cursor;
        const char *end = strchr(cursor, '\n');
        if (!end) {
            end = cursor + strlen(cursor);
        }
        size_t length = (size_t)(end - line);
        if (length > 0U && line[length - 1U] == '\r') {
            length--;
        }
        size_t indent = 0U;
        while (indent < length && line[indent] == ' ') {
            indent++;
        }
        if (indent < length && line[indent] == '\t') {
            free(found);
            free(text);
            return -1;
        }
        if (indent == 0U && length == strlen("mcp:") && memcmp(line, "mcp:", length) == 0) {
            in_mcp = true;
        } else if (indent == 0U && length > 0U && line[0] != '#') {
            in_mcp = false;
        } else if (in_mcp && indent > 0U) {
            static const char key[] = "mcpConfigPath:";
            if (length - indent >= sizeof(key) - 1U &&
                memcmp(line + indent, key, sizeof(key) - 1U) == 0) {
                const char *value = line + indent + sizeof(key) - 1U;
                size_t value_len = length - indent - (sizeof(key) - 1U);
                if (found || value_len == 0U || value[0] == '|' || value[0] == '>') {
                    free(found);
                    free(text);
                    return -1;
                }
                found = agent_trim_copy(value, value_len);
                if (!found || found[0] == '\0') {
                    free(found);
                    free(text);
                    return -1;
                }
            }
        }
        cursor = *end ? end + 1U : end;
    }
    free(text);
    if (!found) {
        return 1;
    }
    int result = agent_rovo_safe_override(found, home_dir, windows, path_out, path_out_size);
    free(found);
    return result;
}

int cbm_agent_client_resolve_path(cbm_agent_client_id_t id,
                                  const cbm_agent_client_resolve_options_t *options, char *path_out,
                                  size_t path_out_size) {
    if (!cbm_agent_client_by_id(id) || !options || !path_out || path_out_size == 0U ||
        !options->home_dir || options->home_dir[0] == '\0') {
        return -1;
    }
    const char *config_home =
        options->xdg_config_home && options->xdg_config_home[0] ? options->xdg_config_home : NULL;
    switch (id) {
    case CBM_AGENT_CLIENT_QODER:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".qoder/settings.json");
    case CBM_AGENT_CLIENT_KIMI:
        return agent_join_path(
            path_out, path_out_size,
            options->kimi_code_home && options->kimi_code_home[0] ? options->kimi_code_home
                                                                  : options->home_dir,
            options->kimi_code_home && options->kimi_code_home[0] ? "mcp.json"
                                                                  : ".kimi-code/mcp.json");
    case CBM_AGENT_CLIENT_GITLAB_DUO:
        if (options->glab_config_dir && options->glab_config_dir[0]) {
            return agent_join_path(path_out, path_out_size, options->glab_config_dir,
                                   "duo/mcp.json");
        }
        if (options->is_windows && options->appdata_dir && options->appdata_dir[0]) {
            return agent_join_path(path_out, path_out_size, options->appdata_dir,
                                   "GitLab/duo/mcp.json");
        }
        if (config_home) {
            return agent_join_path(path_out, path_out_size, config_home, "gitlab/duo/mcp.json");
        }
        return agent_join_path(path_out, path_out_size, options->home_dir, ".gitlab/duo/mcp.json");
    case CBM_AGENT_CLIENT_ROVO_DEV: {
        char config_path[1024];
        if (agent_join_path(config_path, sizeof(config_path), options->home_dir,
                            ".rovodev/config.yml") != 0) {
            return -1;
        }
        if (agent_path_exists(options, config_path)) {
            int override_result = agent_rovo_override(config_path, options->home_dir,
                                                      options->is_windows, path_out, path_out_size);
            if (override_result <= 0) {
                return override_result;
            }
        }
        /* The dedicated MCP page documents mcp.json while the settings
         * reference uses mcp_config.json. Preserve either active file,
         * prefer the dedicated filename, and create only mcp.json. */
        char dedicated_path[1024];
        char reference_path[1024];
        if (agent_join_path(dedicated_path, sizeof(dedicated_path), options->home_dir,
                            ".rovodev/mcp.json") != 0 ||
            agent_join_path(reference_path, sizeof(reference_path), options->home_dir,
                            ".rovodev/mcp_config.json") != 0) {
            return -1;
        }
        const char *selected =
            agent_path_exists(options, dedicated_path)
                ? dedicated_path
                : (agent_path_exists(options, reference_path) ? reference_path : dedicated_path);
        int written = snprintf(path_out, path_out_size, "%s", selected);
        return written >= 0 && (size_t)written < path_out_size ? 0 : -1;
    }
    case CBM_AGENT_CLIENT_AMP:
        /* Amp documents literal $HOME/.config locations and does not
         * advertise XDG_CONFIG_HOME support. Keep its bundled skill MCP
         * next to SKILL.md under the documented global skill root. */
        return agent_join_path(path_out, path_out_size, options->home_dir,
                               ".config/agents/skills/codebase-memory/mcp.json");
    case CBM_AGENT_CLIENT_DEVIN:
        if (options->is_windows && options->appdata_dir && options->appdata_dir[0]) {
            return agent_join_path(path_out, path_out_size, options->appdata_dir,
                                   "devin/config.json");
        }
        return agent_join_path(path_out, path_out_size, options->home_dir,
                               ".config/devin/config.json");
    case CBM_AGENT_CLIENT_TABNINE:
        return agent_join_path(path_out, path_out_size, options->home_dir,
                               ".tabnine/mcp_servers.json");
    case CBM_AGENT_CLIENT_CONTINUE: {
        const char *explicit_path = options->continue_config_path;
        if (explicit_path && explicit_path[0]) {
            if (!agent_path_exists(options, explicit_path)) {
                return 1;
            }
            int written = snprintf(path_out, path_out_size, "%s", explicit_path);
            return written >= 0 && (size_t)written < path_out_size ? 0 : -1;
        }
        char default_path[1024];
        if (agent_join_path(default_path, sizeof(default_path), options->home_dir,
                            ".continue/config.yaml") != 0) {
            return -1;
        }
        if (!agent_path_exists(options, default_path)) {
            return 1;
        }
        int written = snprintf(path_out, path_out_size, "%s", default_path);
        return written >= 0 && (size_t)written < path_out_size ? 0 : -1;
    }
    case CBM_AGENT_CLIENT_VISUAL_STUDIO:
        if (!options->is_windows) {
            return 1;
        }
        return agent_join_path(path_out, path_out_size, options->home_dir, ".mcp.json");
    case CBM_AGENT_CLIENT_TRAE:
        if (!options->trae_config_path || options->trae_config_path[0] == '\0' ||
            !agent_path_exists(options, options->trae_config_path)) {
            return 1;
        }
        {
            int written = snprintf(path_out, path_out_size, "%s", options->trae_config_path);
            return written >= 0 && (size_t)written < path_out_size ? 0 : -1;
        }
    case CBM_AGENT_CLIENT_ROO_CODE:
        if (!options->roo_config_path || options->roo_config_path[0] == '\0' ||
            !agent_path_exists(options, options->roo_config_path)) {
            return 1;
        }
        {
            int written = snprintf(path_out, path_out_size, "%s", options->roo_config_path);
            return written >= 0 && (size_t)written < path_out_size ? 0 : -1;
        }
    case CBM_AGENT_CLIENT_AMAZON_Q: {
        char recommended_path[1024];
        char overview_path[1024];
        char legacy_mcp_path[1024];
        if (agent_join_path(recommended_path, sizeof(recommended_path), options->home_dir,
                            ".aws/amazonq/default.json") != 0 ||
            agent_join_path(overview_path, sizeof(overview_path), options->home_dir,
                            ".aws/amazonq/agents/default.json") != 0 ||
            agent_join_path(legacy_mcp_path, sizeof(legacy_mcp_path), options->home_dir,
                            ".aws/amazonq/mcp.json") != 0) {
            return -1;
        }
        const char *selected =
            agent_path_exists(options, recommended_path)
                ? recommended_path
                : (agent_path_exists(options, overview_path)
                       ? overview_path
                       : (agent_path_exists(options, legacy_mcp_path) ? legacy_mcp_path
                                                                      : recommended_path));
        int written = snprintf(path_out, path_out_size, "%s", selected);
        return written >= 0 && (size_t)written < path_out_size ? 0 : -1;
    }
    case CBM_AGENT_CLIENT_CODEBUDDY: {
        char recommended[1024];
        char deprecated[1024];
        char legacy[1024];
        if (agent_join_path(recommended, sizeof(recommended), options->home_dir,
                            ".codebuddy/.mcp.json") != 0 ||
            agent_join_path(deprecated, sizeof(deprecated), options->home_dir,
                            ".codebuddy/mcp.json") != 0 ||
            agent_join_path(legacy, sizeof(legacy), options->home_dir, ".codebuddy.json") != 0) {
            return -1;
        }
        const char *selected =
            agent_path_exists(options, recommended)
                ? recommended
                : (agent_path_exists(options, deprecated)
                       ? deprecated
                       : (agent_path_exists(options, legacy) ? legacy : recommended));
        int written = snprintf(path_out, path_out_size, "%s", selected);
        return written >= 0 && (size_t)written < path_out_size ? 0 : -1;
    }
    case CBM_AGENT_CLIENT_IBM_BOB_IDE:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".bob/mcp.json");
    case CBM_AGENT_CLIENT_IBM_BOB_SHELL:
        return agent_join_path(path_out, path_out_size, options->home_dir,
                               ".bob/mcp_settings.json");
    case CBM_AGENT_CLIENT_POCHI:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".pochi/config.jsonc");
    case CBM_AGENT_CLIENT_PI:
        return 1;
    case CBM_AGENT_CLIENT_SOURCEGRAPH_CODY:
        if (!options->cody_config_path || options->cody_config_path[0] == '\0' ||
            !agent_path_exists(options, options->cody_config_path)) {
            return 1;
        }
        {
            int written = snprintf(path_out, path_out_size, "%s", options->cody_config_path);
            return written >= 0 && (size_t)written < path_out_size ? 0 : -1;
        }
    default:
        return -1;
    }
}

/* Resolve only client-specific installation directories. These markers prove
 * the client is present before its MCP file exists; shared editor/config roots
 * are deliberately excluded. Returns 1 for clients without a safe marker. */
static int agent_client_marker_path(cbm_agent_client_id_t id,
                                    const cbm_agent_client_resolve_options_t *options,
                                    char *path_out, size_t path_out_size) {
    const char *config_home =
        options->xdg_config_home && options->xdg_config_home[0] ? options->xdg_config_home : NULL;
    switch (id) {
    case CBM_AGENT_CLIENT_QODER:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".qoder");
    case CBM_AGENT_CLIENT_KIMI:
        if (options->kimi_code_home && options->kimi_code_home[0]) {
            int written = snprintf(path_out, path_out_size, "%s", options->kimi_code_home);
            return written >= 0 && (size_t)written < path_out_size ? 0 : -1;
        }
        return agent_join_path(path_out, path_out_size, options->home_dir, ".kimi-code");
    case CBM_AGENT_CLIENT_GITLAB_DUO:
        if (options->glab_config_dir && options->glab_config_dir[0]) {
            return agent_join_path(path_out, path_out_size, options->glab_config_dir, "duo");
        }
        if (options->is_windows && options->appdata_dir && options->appdata_dir[0]) {
            return agent_join_path(path_out, path_out_size, options->appdata_dir, "GitLab/duo");
        }
        if (config_home) {
            return agent_join_path(path_out, path_out_size, config_home, "gitlab/duo");
        }
        return agent_join_path(path_out, path_out_size, options->home_dir, ".gitlab/duo");
    case CBM_AGENT_CLIENT_ROVO_DEV:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".rovodev");
    case CBM_AGENT_CLIENT_AMP:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".config/amp");
    case CBM_AGENT_CLIENT_DEVIN:
        if (options->is_windows && options->appdata_dir && options->appdata_dir[0]) {
            return agent_join_path(path_out, path_out_size, options->appdata_dir, "devin");
        }
        return agent_join_path(path_out, path_out_size, options->home_dir, ".config/devin");
    case CBM_AGENT_CLIENT_TABNINE:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".tabnine");
    case CBM_AGENT_CLIENT_AMAZON_Q:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".aws/amazonq");
    case CBM_AGENT_CLIENT_CODEBUDDY:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".codebuddy");
    case CBM_AGENT_CLIENT_IBM_BOB_IDE:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".bob/mcp.json");
    case CBM_AGENT_CLIENT_IBM_BOB_SHELL:
        return 1;
    case CBM_AGENT_CLIENT_POCHI:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".pochi");
    case CBM_AGENT_CLIENT_PI:
        return agent_join_path(path_out, path_out_size, options->home_dir, ".pi/agent");
    default:
        return 1;
    }
}

bool cbm_agent_client_detect(cbm_agent_client_id_t id,
                             const cbm_agent_client_resolve_options_t *options) {
    const cbm_agent_client_profile_t *profile = cbm_agent_client_by_id(id);
    if (!profile || !options) {
        return false;
    }
    char path[1024];
    int resolved = cbm_agent_client_resolve_path(id, options, path, sizeof(path));
    if (id == CBM_AGENT_CLIENT_CONTINUE || id == CBM_AGENT_CLIENT_TRAE ||
        id == CBM_AGENT_CLIENT_ROO_CODE || id == CBM_AGENT_CLIENT_SOURCEGRAPH_CODY) {
        return resolved == 0 && agent_path_exists(options, path);
    }
    if (id == CBM_AGENT_CLIENT_VISUAL_STUDIO) {
        return options->is_windows && agent_command_exists(options, profile->detection_command);
    }
    char marker_path[1024];
    int marker_resolved = agent_client_marker_path(id, options, marker_path, sizeof(marker_path));
    return (marker_resolved == 0 && agent_path_exists(options, marker_path)) ||
           (resolved == 0 && agent_path_exists(options, path)) ||
           agent_command_exists(options, profile->detection_command);
}

bool cbm_agent_client_cleanup_candidate(cbm_agent_client_id_t id,
                                        const cbm_agent_client_resolve_options_t *options) {
    if (cbm_agent_client_detect(id, options)) {
        return true;
    }
    if (id != CBM_AGENT_CLIENT_VISUAL_STUDIO || !options || !options->is_windows) {
        return false;
    }
    char path[1024];
    return cbm_agent_client_resolve_path(id, options, path, sizeof(path)) == 0 &&
           agent_path_exists(options, path);
}

typedef struct {
    const char *data;
    size_t length;
    size_t position;
} agent_json_scanner_t;

static int agent_json_skip_space(agent_json_scanner_t *scanner) {
    for (;;) {
        while (scanner->position < scanner->length &&
               isspace((unsigned char)scanner->data[scanner->position])) {
            scanner->position++;
        }
        if (scanner->position + 1U >= scanner->length || scanner->data[scanner->position] != '/') {
            return 0;
        }
        if (scanner->data[scanner->position + 1U] == '/') {
            scanner->position += 2U;
            while (scanner->position < scanner->length &&
                   scanner->data[scanner->position] != '\n') {
                scanner->position++;
            }
        } else if (scanner->data[scanner->position + 1U] == '*') {
            scanner->position += 2U;
            while (scanner->position + 1U < scanner->length &&
                   !(scanner->data[scanner->position] == '*' &&
                     scanner->data[scanner->position + 1U] == '/')) {
                scanner->position++;
            }
            if (scanner->position + 1U >= scanner->length) {
                return -1;
            }
            scanner->position += 2U;
        } else {
            return 0;
        }
    }
}

static int agent_json_scan_string(agent_json_scanner_t *scanner, size_t *start_out,
                                  size_t *end_out) {
    if (scanner->position >= scanner->length ||
        (scanner->data[scanner->position] != '"' && scanner->data[scanner->position] != '\'')) {
        return -1;
    }
    char quote = scanner->data[scanner->position++];
    size_t start = scanner->position;
    bool escaped = false;
    while (scanner->position < scanner->length) {
        char current = scanner->data[scanner->position++];
        if (escaped) {
            escaped = false;
        } else if (current == '\\') {
            escaped = true;
        } else if (current == quote) {
            if (start_out) {
                *start_out = start;
            }
            if (end_out) {
                *end_out = scanner->position - 1U;
            }
            return 0;
        } else if ((unsigned char)current < 0x20U) {
            return -1;
        }
    }
    return -1;
}

static int agent_json_scan_value(agent_json_scanner_t *scanner, unsigned depth);

static int agent_json_scan_object(agent_json_scanner_t *scanner, unsigned depth) {
    if (depth >= AGENT_JSON_MAX_DEPTH || scanner->position >= scanner->length ||
        scanner->data[scanner->position++] != '{') {
        return -1;
    }
    if (agent_json_skip_space(scanner) != 0) {
        return -1;
    }
    if (scanner->position < scanner->length && scanner->data[scanner->position] == '}') {
        scanner->position++;
        return 0;
    }
    for (;;) {
        if (scanner->position >= scanner->length) {
            return -1;
        }
        if (scanner->data[scanner->position] == '"' || scanner->data[scanner->position] == '\'') {
            if (agent_json_scan_string(scanner, NULL, NULL) != 0) {
                return -1;
            }
        } else {
            size_t start = scanner->position;
            while (scanner->position < scanner->length &&
                   (isalnum((unsigned char)scanner->data[scanner->position]) ||
                    strchr("_$-", scanner->data[scanner->position]))) {
                scanner->position++;
            }
            if (scanner->position == start) {
                return -1;
            }
        }
        if (agent_json_skip_space(scanner) != 0 || scanner->position >= scanner->length ||
            scanner->data[scanner->position++] != ':' ||
            agent_json_scan_value(scanner, depth + 1U) != 0 ||
            agent_json_skip_space(scanner) != 0 || scanner->position >= scanner->length) {
            return -1;
        }
        char separator = scanner->data[scanner->position++];
        if (separator == '}') {
            return 0;
        }
        if (separator != ',') {
            return -1;
        }
        if (agent_json_skip_space(scanner) != 0) {
            return -1;
        }
        if (scanner->position < scanner->length && scanner->data[scanner->position] == '}') {
            scanner->position++;
            return 0;
        }
    }
}

static int agent_json_scan_array(agent_json_scanner_t *scanner, unsigned depth) {
    if (depth >= AGENT_JSON_MAX_DEPTH) {
        return -1;
    }
    scanner->position++;
    if (agent_json_skip_space(scanner) != 0) {
        return -1;
    }
    if (scanner->position < scanner->length && scanner->data[scanner->position] == ']') {
        scanner->position++;
        return 0;
    }
    for (;;) {
        if (agent_json_scan_value(scanner, depth + 1U) != 0 ||
            agent_json_skip_space(scanner) != 0 || scanner->position >= scanner->length) {
            return -1;
        }
        char separator = scanner->data[scanner->position++];
        if (separator == ']') {
            return 0;
        }
        if (separator != ',' || agent_json_skip_space(scanner) != 0) {
            return -1;
        }
        if (scanner->position < scanner->length && scanner->data[scanner->position] == ']') {
            scanner->position++;
            return 0;
        }
    }
}

static int agent_json_scan_value(agent_json_scanner_t *scanner, unsigned depth) {
    if (depth > AGENT_JSON_MAX_DEPTH || agent_json_skip_space(scanner) != 0 ||
        scanner->position >= scanner->length) {
        return -1;
    }
    char first = scanner->data[scanner->position];
    if (first == '{') {
        return agent_json_scan_object(scanner, depth);
    }
    if (first == '[') {
        return agent_json_scan_array(scanner, depth);
    }
    if (first == '"' || first == '\'') {
        return agent_json_scan_string(scanner, NULL, NULL);
    }
    size_t start = scanner->position;
    while (scanner->position < scanner->length &&
           !isspace((unsigned char)scanner->data[scanner->position]) &&
           !strchr(",]}", scanner->data[scanner->position])) {
        scanner->position++;
    }
    return scanner->position > start ? 0 : -1;
}

static bool agent_json_key_equals(const char *data, size_t start, size_t end, const char *key) {
    if (end < start) {
        return false;
    }
    size_t key_offset = 0U;
    size_t key_len = strlen(key);
    for (size_t cursor = start; cursor < end;) {
        unsigned value = (unsigned char)data[cursor++];
        if (value == '\\') {
            if (cursor >= end) {
                return false;
            }
            char escape = data[cursor++];
            if (escape == 'u' || escape == 'x') {
                size_t digits = escape == 'u' ? 4U : 2U;
                if (end - cursor < digits) {
                    return false;
                }
                value = 0U;
                for (size_t i = 0U; i < digits; i++) {
                    unsigned char digit = (unsigned char)data[cursor++];
                    if (digit >= '0' && digit <= '9') {
                        value = value * 16U + (unsigned)(digit - '0');
                    } else if (digit >= 'a' && digit <= 'f') {
                        value = value * 16U + (unsigned)(digit - 'a' + 10U);
                    } else if (digit >= 'A' && digit <= 'F') {
                        value = value * 16U + (unsigned)(digit - 'A' + 10U);
                    } else {
                        return false;
                    }
                }
            } else if (strchr("\\/\"'", escape)) {
                value = (unsigned char)escape;
            } else {
                return false;
            }
        }
        if (value > 0x7fU || key_offset >= key_len || (unsigned char)key[key_offset++] != value) {
            return false;
        }
    }
    return key_offset == key_len;
}

/* Returns 0 found, 1 absent, -1 malformed/ambiguous/not-an-object. */
static int agent_json_find_member(const char *data, size_t object_start, size_t object_end,
                                  const char *key, size_t *value_start, size_t *value_end) {
    agent_json_scanner_t scanner = {.data = data, .length = object_end, .position = object_start};
    if (agent_json_skip_space(&scanner) != 0 || scanner.position >= object_end ||
        scanner.data[scanner.position++] != '{' || agent_json_skip_space(&scanner) != 0) {
        return -1;
    }
    bool found = false;
    if (scanner.position < object_end && scanner.data[scanner.position] == '}') {
        return 1;
    }
    for (;;) {
        size_t key_start = 0U;
        size_t key_end = 0U;
        if (scanner.position >= object_end) {
            return -1;
        }
        if (scanner.data[scanner.position] == '"' || scanner.data[scanner.position] == '\'') {
            if (agent_json_scan_string(&scanner, &key_start, &key_end) != 0) {
                return -1;
            }
        } else {
            key_start = scanner.position;
            while (scanner.position < object_end &&
                   (isalnum((unsigned char)scanner.data[scanner.position]) ||
                    strchr("_$-", scanner.data[scanner.position]))) {
                scanner.position++;
            }
            key_end = scanner.position;
            if (key_end == key_start) {
                return -1;
            }
        }
        if (agent_json_skip_space(&scanner) != 0 || scanner.position >= object_end ||
            scanner.data[scanner.position++] != ':' || agent_json_skip_space(&scanner) != 0) {
            return -1;
        }
        size_t start = scanner.position;
        if (agent_json_scan_value(&scanner, 1U) != 0) {
            return -1;
        }
        size_t end = scanner.position;
        if (agent_json_key_equals(data, key_start, key_end, key)) {
            if (found) {
                return -1;
            }
            found = true;
            *value_start = start;
            *value_end = end;
        }
        if (agent_json_skip_space(&scanner) != 0 || scanner.position >= object_end) {
            return -1;
        }
        char separator = scanner.data[scanner.position++];
        if (separator == '}') {
            return found ? 0 : 1;
        }
        if (separator != ',' || agent_json_skip_space(&scanner) != 0) {
            return -1;
        }
        if (scanner.position < object_end && scanner.data[scanner.position] == '}') {
            return found ? 0 : 1;
        }
    }
}

static int agent_json_find_entry(const char *document, size_t length, const char *section,
                                 size_t *entry_start, size_t *entry_end) {
    size_t bom = length >= 3U && (unsigned char)document[0] == 0xefU &&
                         (unsigned char)document[1] == 0xbbU && (unsigned char)document[2] == 0xbfU
                     ? 3U
                     : 0U;
    agent_json_scanner_t scanner = {.data = document, .length = length, .position = bom};
    if (agent_json_skip_space(&scanner) != 0) {
        return -1;
    }
    size_t root_start = scanner.position;
    if (agent_json_scan_value(&scanner, 0U) != 0 || agent_json_skip_space(&scanner) != 0 ||
        scanner.position != length || document[root_start] != '{') {
        return -1;
    }
    size_t object_start = root_start;
    size_t object_end = scanner.position;
    if (section) {
        size_t section_start = 0U;
        size_t section_end = 0U;
        int section_result = agent_json_find_member(document, root_start, object_end, section,
                                                    &section_start, &section_end);
        if (section_result != 0) {
            return section_result;
        }
        if (document[section_start] != '{') {
            return -1;
        }
        object_start = section_start;
        object_end = section_end;
    }
    return agent_json_find_member(document, object_start, object_end, AGENT_ENTRY_KEY, entry_start,
                                  entry_end);
}

static char *agent_json_escape(const char *value) {
    size_t length = strlen(value);
    if (length > (SIZE_MAX - 1U) / 6U) {
        return NULL;
    }
    char *escaped = (char *)malloc(length * 6U + 1U);
    if (!escaped) {
        return NULL;
    }
    size_t output = 0U;
    for (size_t i = 0U; i < length; i++) {
        unsigned char byte = (unsigned char)value[i];
        if (byte == '"' || byte == '\\') {
            escaped[output++] = '\\';
            escaped[output++] = (char)byte;
        } else if (byte == '\b') {
            escaped[output++] = '\\';
            escaped[output++] = 'b';
        } else if (byte == '\f') {
            escaped[output++] = '\\';
            escaped[output++] = 'f';
        } else if (byte == '\n') {
            escaped[output++] = '\\';
            escaped[output++] = 'n';
        } else if (byte == '\r') {
            escaped[output++] = '\\';
            escaped[output++] = 'r';
        } else if (byte == '\t') {
            escaped[output++] = '\\';
            escaped[output++] = 't';
        } else if (byte < 0x20U) {
            int written = snprintf(escaped + output, 7U, "\\u%04x", (unsigned)byte);
            if (written != 6) {
                free(escaped);
                return NULL;
            }
            output += 6U;
        } else {
            escaped[output++] = (char)byte;
        }
    }
    escaped[output] = '\0';
    return escaped;
}

static char *agent_json_canonical(cbm_agent_client_id_t id, const char *binary_path) {
    char *escaped = agent_json_escape(binary_path);
    if (!escaped) {
        return NULL;
    }
    const char *extra = "";
    if (id == CBM_AGENT_CLIENT_GITLAB_DUO || id == CBM_AGENT_CLIENT_VISUAL_STUDIO) {
        extra = ", \"type\": \"stdio\"";
    } else if (id == CBM_AGENT_CLIENT_ROVO_DEV) {
        extra = ", \"transport\": \"stdio\"";
    }
    size_t needed = strlen(escaped) + strlen(extra) + 64U;
    char *canonical = (char *)malloc(needed);
    if (canonical) {
        int written =
            snprintf(canonical, needed, "{ \"command\": \"%s\", \"args\": []%s }", escaped, extra);
        if (written < 0 || (size_t)written >= needed) {
            free(canonical);
            canonical = NULL;
        }
    }
    free(escaped);
    return canonical;
}

static char *agent_json_compact(const char *data, size_t length) {
    char *output = (char *)malloc(length + 1U);
    if (!output) {
        return NULL;
    }
    size_t written = 0U;
    char quote = '\0';
    bool escaped = false;
    for (size_t i = 0U; i < length; i++) {
        char current = data[i];
        if (quote) {
            output[written++] = current;
            if (escaped) {
                escaped = false;
            } else if (current == '\\') {
                escaped = true;
            } else if (current == quote) {
                quote = '\0';
            }
        } else if (current == '"' || current == '\'') {
            quote = current;
            output[written++] = current;
        } else if (isspace((unsigned char)current)) {
            continue;
        } else if (current == '/' && i + 1U < length && data[i + 1U] == '/') {
            i += 2U;
            while (i < length && data[i] != '\n') {
                i++;
            }
        } else if (current == '/' && i + 1U < length && data[i + 1U] == '*') {
            i += 2U;
            while (i + 1U < length && !(data[i] == '*' && data[i + 1U] == '/')) {
                i++;
            }
            i++;
        } else {
            output[written++] = current;
        }
    }
    output[written] = '\0';
    return output;
}

static bool agent_json_owned(const char *document, size_t start, size_t end,
                             const char *canonical) {
    char *actual_compact = agent_json_compact(document + start, end - start);
    char *canonical_compact = agent_json_compact(canonical, strlen(canonical));
    bool equal =
        actual_compact && canonical_compact && strcmp(actual_compact, canonical_compact) == 0;
    free(actual_compact);
    free(canonical_compact);
    return equal;
}

static const char *agent_json_section(cbm_agent_client_id_t id) {
    if (id == CBM_AGENT_CLIENT_AMP) {
        return NULL;
    }
    if (id == CBM_AGENT_CLIENT_VISUAL_STUDIO) {
        return "servers";
    }
    if (id == CBM_AGENT_CLIENT_SOURCEGRAPH_CODY) {
        return "cody.mcpServers";
    }
    if (id == CBM_AGENT_CLIENT_POCHI) {
        return "mcp";
    }
    return "mcpServers";
}

static int agent_json_edit(cbm_agent_client_id_t id, const char *config_path,
                           const char *binary_path, bool remove) {
    char *canonical = agent_json_canonical(id, binary_path);
    if (!canonical) {
        return CBM_AGENT_EDIT_ERROR;
    }
    char *document = NULL;
    size_t length = 0U;
    int read_result = cbm_json_like_read_document(config_path, &document, &length);
    if (read_result < 0) {
        free(canonical);
        return CBM_AGENT_EDIT_ERROR;
    }
    size_t entry_start = 0U;
    size_t entry_end = 0U;
    int find_result = read_result == 1
                          ? 1
                          : agent_json_find_entry(document, length, agent_json_section(id),
                                                  &entry_start, &entry_end);
    if (find_result < 0) {
        free(document);
        free(canonical);
        return CBM_AGENT_EDIT_ERROR;
    }
    if (find_result == 0 && !agent_json_owned(document, entry_start, entry_end, canonical)) {
        free(document);
        free(canonical);
        return CBM_AGENT_EDIT_FOREIGN;
    }
    if ((remove && find_result == 1) || (!remove && find_result == 0)) {
        free(document);
        free(canonical);
        return CBM_AGENT_EDIT_OK;
    }
    const char *section = agent_json_section(id);
    const char *path[1] = {section};
    int edit_result =
        remove
            ? cbm_json_like_remove_entry_if_unchanged(config_path, path, section ? 1U : 0U,
                                                      AGENT_ENTRY_KEY, document, length)
            : cbm_json_like_upsert_entry_if_unchanged(config_path, path, section ? 1U : 0U,
                                                      AGENT_ENTRY_KEY, canonical,
                                                      read_result == 1 ? NULL : document, length);
    free(document);
    free(canonical);
    return edit_result == 0 ? CBM_AGENT_EDIT_OK : CBM_AGENT_EDIT_ERROR;
}

static size_t agent_next_line(const char *data, size_t length, size_t start, size_t *content_end) {
    size_t end = start;
    while (end < length && data[end] != '\n') {
        end++;
    }
    *content_end = end;
    return end < length ? end + 1U : end;
}

static size_t agent_line_indent(const char *data, size_t start, size_t end) {
    size_t indent = 0U;
    while (start + indent < end && data[start + indent] == ' ') {
        indent++;
    }
    return indent;
}

static bool agent_yaml_top_level_key(const char *data, size_t start, size_t end, const char *key,
                                     bool *empty_value) {
    if (end > start && data[end - 1U] == '\r') {
        end--;
    }
    size_t key_len = strlen(key);
    if (end - start < key_len + 1U || memcmp(data + start, key, key_len) != 0 ||
        data[start + key_len] != ':') {
        return false;
    }
    size_t cursor = start + key_len + 1U;
    while (cursor < end && data[cursor] == ' ') {
        cursor++;
    }
    *empty_value = cursor == end || data[cursor] == '#';
    return true;
}

static bool agent_yaml_named_item(const char *data, size_t start, size_t end,
                                  const char *expected_name) {
    if (end > start && data[end - 1U] == '\r') {
        end--;
    }
    static const char prefix[] = "  - name:";
    if (end - start < sizeof(prefix) - 1U ||
        memcmp(data + start, prefix, sizeof(prefix) - 1U) != 0) {
        return false;
    }
    const char *value = data + start + sizeof(prefix) - 1U;
    size_t value_len = end - start - (sizeof(prefix) - 1U);
    char *decoded = agent_trim_copy(value, value_len);
    if (!decoded) {
        return false;
    }
    char *comment = strchr(decoded, '#');
    if (comment) {
        char *before = comment;
        while (before > decoded && isspace((unsigned char)before[-1])) {
            before--;
        }
        *before = '\0';
    }
    bool matches = strcmp(decoded, expected_name) == 0;
    free(decoded);
    return matches;
}

static char *agent_splice(const char *document, size_t length, size_t start, size_t end,
                          const char *replacement) {
    size_t replacement_len = strlen(replacement);
    if (start > end || end > length || replacement_len > SIZE_MAX - (length - (end - start)) - 1U) {
        return NULL;
    }
    size_t new_length = length - (end - start) + replacement_len;
    char *updated = (char *)malloc(new_length + 1U);
    if (!updated) {
        return NULL;
    }
    memcpy(updated, document, start);
    memcpy(updated + start, replacement, replacement_len);
    memcpy(updated + start + replacement_len, document + end, length - end);
    updated[new_length] = '\0';
    return updated;
}

static int agent_continue_edit(const char *config_path, const char *binary_path, bool remove) {
    char *document = NULL;
    size_t length = 0U;
    if (cbm_json_like_read_document(config_path, &document, &length) != 0 || !document) {
        free(document);
        return CBM_AGENT_EDIT_NOT_APPLICABLE;
    }
    char *quoted = NULL;
    if (cbm_yaml_encode_double_quoted_scalar(binary_path, &quoted) != 0) {
        free(document);
        return CBM_AGENT_EDIT_ERROR;
    }
    const char *eol = strstr(document, "\r\n") ? "\r\n" : "\n";
    size_t canonical_size = strlen(quoted) + strlen(eol) * 3U + 96U;
    char *canonical = (char *)malloc(canonical_size);
    if (!canonical) {
        free(quoted);
        free(document);
        return CBM_AGENT_EDIT_ERROR;
    }
    int canonical_written = snprintf(canonical, canonical_size,
                                     "  - name: codebase-memory-mcp%s    command: %s%s"
                                     "    args: []%s",
                                     eol, quoted, eol, eol);
    free(quoted);
    if (canonical_written < 0 || (size_t)canonical_written >= canonical_size) {
        free(canonical);
        free(document);
        return CBM_AGENT_EDIT_ERROR;
    }

    size_t section_start = SIZE_MAX;
    size_t section_body = SIZE_MAX;
    size_t section_end = length;
    size_t sections = 0U;
    bool invalid_section = false;
    size_t cursor = 0U;
    while (cursor < length) {
        size_t line_end = 0U;
        size_t next = agent_next_line(document, length, cursor, &line_end);
        size_t indent = agent_line_indent(document, cursor, line_end);
        bool blank_or_comment = cursor + indent >= line_end || document[cursor + indent] == '#';
        bool empty_section_value = false;
        if (indent == 0U && agent_yaml_top_level_key(document, cursor, line_end, "mcpServers",
                                                     &empty_section_value)) {
            sections++;
            section_start = cursor;
            section_body = next;
            section_end = length;
            invalid_section = invalid_section || !empty_section_value;
        } else if (sections == 1U && section_body != SIZE_MAX && cursor >= section_body &&
                   indent == 0U && !blank_or_comment) {
            section_end = cursor;
            section_body = SIZE_MAX;
        }
        cursor = next;
    }
    if (sections > 1U || invalid_section) {
        free(canonical);
        free(document);
        return CBM_AGENT_EDIT_ERROR;
    }

    size_t item_start = SIZE_MAX;
    size_t item_end = SIZE_MAX;
    size_t items = 0U;
    if (sections == 1U) {
        size_t scan = agent_next_line(document, length, section_start, &cursor);
        while (scan < section_end) {
            size_t line_end = 0U;
            size_t next = agent_next_line(document, length, scan, &line_end);
            if (agent_yaml_named_item(document, scan, line_end, AGENT_ENTRY_KEY)) {
                items++;
                item_start = scan;
                size_t end_scan = next;
                while (end_scan < section_end) {
                    size_t candidate_end = 0U;
                    size_t candidate_next =
                        agent_next_line(document, length, end_scan, &candidate_end);
                    size_t indent = agent_line_indent(document, end_scan, candidate_end);
                    if (indent <= 2U && end_scan + indent < candidate_end &&
                        document[end_scan + indent] == '-') {
                        break;
                    }
                    end_scan = candidate_next;
                }
                item_end = end_scan;
            }
            scan = next;
        }
    }
    if (items > 1U) {
        free(canonical);
        free(document);
        return CBM_AGENT_EDIT_ERROR;
    }
    if (items == 1U) {
        size_t canonical_len = strlen(canonical);
        if (item_end - item_start != canonical_len ||
            memcmp(document + item_start, canonical, canonical_len) != 0) {
            free(canonical);
            free(document);
            return CBM_AGENT_EDIT_FOREIGN;
        }
        if (!remove) {
            free(canonical);
            free(document);
            return CBM_AGENT_EDIT_OK;
        }
    } else if (remove) {
        free(canonical);
        free(document);
        return CBM_AGENT_EDIT_OK;
    }

    char *updated = NULL;
    if (items == 1U) {
        updated = agent_splice(document, length, item_start, item_end, "");
    } else if (sections == 1U) {
        const char *prefix = section_end > 0U && document[section_end - 1U] != '\n' ? eol : "";
        size_t insertion_size = strlen(prefix) + strlen(canonical) + 1U;
        char *insertion = (char *)malloc(insertion_size);
        if (insertion) {
            snprintf(insertion, insertion_size, "%s%s", prefix, canonical);
            updated = agent_splice(document, length, section_end, section_end, insertion);
            free(insertion);
        }
    } else {
        const char *prefix = length > 0U && document[length - 1U] != '\n' ? eol : "";
        size_t append_size =
            strlen(prefix) + strlen("mcpServers:") + strlen(eol) + strlen(canonical) + 1U;
        char *append = (char *)malloc(append_size);
        if (append) {
            snprintf(append, append_size, "%smcpServers:%s%s", prefix, eol, canonical);
            updated = agent_splice(document, length, length, length, append);
            free(append);
        }
    }
    int result = updated && cbm_text_write_owned_document_if_unchanged(config_path, updated,
                                                                       document, length) == 0
                     ? CBM_AGENT_EDIT_OK
                     : CBM_AGENT_EDIT_ERROR;
    free(updated);
    free(canonical);
    free(document);
    return result;
}

static bool agent_json_client(cbm_agent_client_id_t id) {
    switch (id) {
    case CBM_AGENT_CLIENT_QODER:
    case CBM_AGENT_CLIENT_KIMI:
    case CBM_AGENT_CLIENT_GITLAB_DUO:
    case CBM_AGENT_CLIENT_ROVO_DEV:
    case CBM_AGENT_CLIENT_AMP:
    case CBM_AGENT_CLIENT_DEVIN:
    case CBM_AGENT_CLIENT_TABNINE:
    case CBM_AGENT_CLIENT_VISUAL_STUDIO:
    case CBM_AGENT_CLIENT_TRAE:
    case CBM_AGENT_CLIENT_ROO_CODE:
    case CBM_AGENT_CLIENT_AMAZON_Q:
    case CBM_AGENT_CLIENT_CODEBUDDY:
    case CBM_AGENT_CLIENT_IBM_BOB_IDE:
    case CBM_AGENT_CLIENT_IBM_BOB_SHELL:
    case CBM_AGENT_CLIENT_POCHI:
    case CBM_AGENT_CLIENT_SOURCEGRAPH_CODY:
        return true;
    case CBM_AGENT_CLIENT_CONTINUE:
    case CBM_AGENT_CLIENT_PI:
    case CBM_AGENT_CLIENT_COUNT:
        return false;
    default:
        return false;
    }
}

int cbm_agent_client_install_mcp(cbm_agent_client_id_t id, const char *config_path,
                                 const char *binary_path) {
    if (!config_path || config_path[0] == '\0' || !binary_path || binary_path[0] == '\0' ||
        !cbm_agent_client_by_id(id)) {
        return CBM_AGENT_EDIT_ERROR;
    }
    if (id == CBM_AGENT_CLIENT_CONTINUE) {
        return agent_continue_edit(config_path, binary_path, false);
    }
    return agent_json_client(id) ? agent_json_edit(id, config_path, binary_path, false)
                                 : CBM_AGENT_EDIT_NOT_APPLICABLE;
}

int cbm_agent_client_remove_mcp(cbm_agent_client_id_t id, const char *config_path,
                                const char *binary_path) {
    if (!config_path || config_path[0] == '\0' || !binary_path || binary_path[0] == '\0' ||
        !cbm_agent_client_by_id(id)) {
        return CBM_AGENT_EDIT_ERROR;
    }
    if (id == CBM_AGENT_CLIENT_CONTINUE) {
        return agent_continue_edit(config_path, binary_path, true);
    }
    return agent_json_client(id) ? agent_json_edit(id, config_path, binary_path, true)
                                 : CBM_AGENT_EDIT_NOT_APPLICABLE;
}

static int agent_install_callback(cbm_agent_client_id_t id, const char *config_path,
                                  const char *binary_path) {
    return cbm_agent_client_install_mcp(id, config_path, binary_path);
}

static int agent_remove_callback(cbm_agent_client_id_t id, const char *config_path,
                                 const char *binary_path) {
    return cbm_agent_client_remove_mcp(id, config_path, binary_path);
}
