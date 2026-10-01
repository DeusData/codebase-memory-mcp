/*
 * config.h — Persistent UI configuration.
 *
 * Stores ui_enabled and ui_port in ~/.cache/codebase-memory-mcp/config.json.
 * Thread-safe: load/save are independent operations on the filesystem.
 */
#ifndef CBM_UI_CONFIG_H
#define CBM_UI_CONFIG_H

#include <stdbool.h>

/* Default values */
#define CBM_UI_DEFAULT_PORT 9749
#define CBM_UI_DEFAULT_ENABLED false

typedef struct {
    bool ui_enabled;
    int ui_port;
} cbm_ui_config_t;

/* Load legacy JSON/defaults, then the shared transactional settings override. */
void cbm_ui_config_load(cbm_ui_config_t *cfg);
/* Legacy JSON/default baseline, before transactional settings overrides. */
void cbm_ui_config_load_legacy(cbm_ui_config_t *cfg);

/* Atomically save one complete config generation. Initialized applications use
 * the shared SQLite settings store; legacy embedders retain JSON persistence. */
bool cbm_ui_config_save(const cbm_ui_config_t *cfg);

/* Get the config file path. Writes to buf (up to bufsz bytes).
 * Exposed for testing. */
void cbm_ui_config_path(char *buf, int bufsz);

#endif /* CBM_UI_CONFIG_H */
