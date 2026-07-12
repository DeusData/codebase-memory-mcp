/*
 * config.h — Persistent UI configuration.
 *
 * Stores ui_enabled, ui_port, and ui_host in ~/.cache/codebase-memory-mcp/config.json.
 * Thread-safe: load/save are independent operations on the filesystem.
 */
#ifndef CBM_UI_CONFIG_H
#define CBM_UI_CONFIG_H

#include <stdbool.h>

/* Default values */
#define CBM_UI_DEFAULT_PORT 9749
#define CBM_UI_DEFAULT_ENABLED false
#define CBM_UI_DEFAULT_HOST "127.0.0.1"
#define CBM_UI_HOST_MAX 16 /* INET_ADDRSTRLEN: max IPv4 text length + NUL */

typedef struct {
    bool ui_enabled;
    int ui_port;
    char ui_host[CBM_UI_HOST_MAX];
} cbm_ui_config_t;

/* Validate numeric IPv4 text and identify IPv4 loopback addresses. */
bool cbm_ui_host_is_valid(const char *host);
bool cbm_ui_host_is_loopback(const char *host);

/* Load config from disk. Missing/corrupt file → defaults. */
void cbm_ui_config_load(cbm_ui_config_t *cfg);

/* Save config to disk. Creates directory if needed. */
void cbm_ui_config_save(const cbm_ui_config_t *cfg);

/* Get the config file path. Writes to buf (up to bufsz bytes).
 * Exposed for testing. */
void cbm_ui_config_path(char *buf, int bufsz);

#endif /* CBM_UI_CONFIG_H */
