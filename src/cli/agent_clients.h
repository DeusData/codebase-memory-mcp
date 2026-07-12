/*
 * agent_clients.h — Table-driven agent client MCP installation profiles.
 */
#ifndef CBM_CLI_AGENT_CLIENTS_H
#define CBM_CLI_AGENT_CLIENTS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    CBM_AGENT_CLIENT_QODER = 0,
    CBM_AGENT_CLIENT_KIMI,
    CBM_AGENT_CLIENT_GITLAB_DUO,
    CBM_AGENT_CLIENT_ROVO_DEV,
    CBM_AGENT_CLIENT_AMP,
    CBM_AGENT_CLIENT_DEVIN,
    CBM_AGENT_CLIENT_TABNINE,
    CBM_AGENT_CLIENT_CONTINUE,
    CBM_AGENT_CLIENT_VISUAL_STUDIO,
    CBM_AGENT_CLIENT_TRAE,
    CBM_AGENT_CLIENT_ROO_CODE,
    CBM_AGENT_CLIENT_AMAZON_Q,
    CBM_AGENT_CLIENT_CODEBUDDY,
    CBM_AGENT_CLIENT_IBM_BOB_IDE,
    CBM_AGENT_CLIENT_IBM_BOB_SHELL,
    CBM_AGENT_CLIENT_POCHI,
    CBM_AGENT_CLIENT_PI,
    CBM_AGENT_CLIENT_SOURCEGRAPH_CODY,
    CBM_AGENT_CLIENT_COUNT
} cbm_agent_client_id_t;

typedef enum {
    CBM_AGENT_STABLE = 0,
    CBM_AGENT_CONDITIONAL,
    CBM_AGENT_OPT_IN
} cbm_agent_client_stability_t;

enum {
    CBM_AGENT_CAP_MCP = UINT32_C(1) << 0,
    CBM_AGENT_CAP_INSTRUCTIONS = UINT32_C(1) << 1,
    CBM_AGENT_CAP_SKILL = UINT32_C(1) << 2,
    CBM_AGENT_CAP_AGENT = UINT32_C(1) << 3,
    CBM_AGENT_CAP_HOOK = UINT32_C(1) << 4,
    CBM_AGENT_CAP_PLUGIN = UINT32_C(1) << 5
};

typedef int (*cbm_agent_mcp_edit_fn)(cbm_agent_client_id_t id, const char *config_path,
                                     const char *binary_path);

typedef struct {
    cbm_agent_client_id_t id;
    const char *stable_id;
    const char *display_name;
    cbm_agent_client_stability_t stability;
    uint32_t capabilities;
    const char *detection_command;
    cbm_agent_mcp_edit_fn install_mcp;
    cbm_agent_mcp_edit_fn remove_mcp;
} cbm_agent_client_profile_t;

typedef bool (*cbm_agent_probe_fn)(const char *value, const void *context);

typedef struct {
    const char *home_dir;
    const char *xdg_config_home;
    const char *appdata_dir;
    const char *glab_config_dir;
    const char *kimi_code_home;
    const char *continue_config_path;
    const char *trae_config_path;
    const char *roo_config_path;
    const char *cody_config_path;
    bool is_windows;
    cbm_agent_probe_fn path_exists;
    cbm_agent_probe_fn command_exists;
    const void *probe_context;
} cbm_agent_client_resolve_options_t;

enum {
    CBM_AGENT_EDIT_ERROR = -1,
    CBM_AGENT_EDIT_OK = 0,
    CBM_AGENT_EDIT_FOREIGN = 1,
    CBM_AGENT_EDIT_NOT_APPLICABLE = 2
};

size_t cbm_agent_client_count(void);
const cbm_agent_client_profile_t *cbm_agent_client_at(size_t index);
const cbm_agent_client_profile_t *cbm_agent_client_by_id(cbm_agent_client_id_t id);
const cbm_agent_client_profile_t *cbm_agent_client_by_stable_id(const char *stable_id);

/* Resolves the documented user config path. Returns 0 on success, 1 when a
 * conditional target has no safe active path, and -1 for invalid input or an
 * ambiguous/unsupported configuration. */
int cbm_agent_client_resolve_path(cbm_agent_client_id_t id,
                                  const cbm_agent_client_resolve_options_t *options, char *path_out,
                                  size_t path_out_size);
bool cbm_agent_client_detect(cbm_agent_client_id_t id,
                             const cbm_agent_client_resolve_options_t *options);
bool cbm_agent_client_cleanup_candidate(cbm_agent_client_id_t id,
                                        const cbm_agent_client_resolve_options_t *options);

/* config_path must already have been resolved. The adapter never guesses a
 * target here. Existing same-name foreign entries fail closed with
 * CBM_AGENT_EDIT_FOREIGN. Removal requires the original installed binary path
 * and only removes the still-canonical entry. */
int cbm_agent_client_install_mcp(cbm_agent_client_id_t id, const char *config_path,
                                 const char *binary_path);
int cbm_agent_client_remove_mcp(cbm_agent_client_id_t id, const char *config_path,
                                const char *binary_path);

#ifdef __cplusplus
}
#endif

#endif /* CBM_CLI_AGENT_CLIENTS_H */
