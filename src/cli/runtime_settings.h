#ifndef CBM_RUNTIME_SETTINGS_H
#define CBM_RUNTIME_SETTINGS_H

#include <stdbool.h>
#include <stddef.h>

/* Call once before starting threads or initializing environment-dependent
 * subsystems. Saved environment values are immutable for this process; they
 * never enter the inherited process environment. */
bool cbm_runtime_settings_init(const char *cache_dir);
bool cbm_runtime_settings_initialized(void);

/* JSON API. Returned strings are heap-owned by the caller. HTTP status is
 * 200, 400 (invalid batch), 409 (stale revision), or 500 (storage failure).
 * A batch either commits completely or changes nothing. */
char *cbm_runtime_settings_get_json(const char *cache_dir);
char *cbm_runtime_settings_apply_json(const char *cache_dir, const char *body, size_t length,
                                      int *status);

/* UI JSON is the legacy fallback. Once initialized, CLI/daemon UI writes use
 * the same transactional store as the Config dialog. Last explicit write wins. */
void cbm_runtime_settings_load_ui(bool *enabled, int *port);
bool cbm_runtime_settings_save_ui(bool enabled, int port);
/* The HTTP handler supplies its actual bound port for truthful listener state. */
void cbm_runtime_settings_note_http_port(int port);

#ifdef CBM_ENABLE_TEST_SEAMS
/* Single-threaded test teardown only; never exposed in production builds. */
void cbm_runtime_settings_reset_for_tests(void);
#endif

#endif
