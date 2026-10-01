/* Typed, local configuration. SQLite batches are atomic; environment overrides
 * are loaded once per process and never exported to child environments. */
#include "cli/runtime_settings.h"
#include "cli/cli.h"
#include "foundation/platform.h"
#include "foundation/log.h"
#include "foundation/compat_fs.h"
#include "daemon/ipc.h"
#include "ui/config.h"
#include <sqlite3.h>
#include <yyjson/yyjson.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#define SETTINGS_VALUE_CAP 256
#define SETTINGS_PATH_CAP 4096
#define SETTINGS_BODY_CAP 32768
#define SETTINGS_MAX_LONG \
    ((double)LONG_MAX < 9007199254740991.0 ? (double)LONG_MAX : 9007199254740991.0)

typedef enum { SET_ENV, SET_CONFIG, SET_UI, SET_REFERENCE } setting_storage_t;
typedef struct {
    const char *key, *label, *description, *category, *type, *default_value;
    double minimum, maximum;
    const char *options, *source_file, *apply_mode;
    setting_storage_t storage;
} setting_t;
#define ENV(k, l, d, c, t, v, lo, hi, o, p) {k, l, d, c, t, v, lo, hi, o, p, "restart", SET_ENV}
#define CFG(k, l, d, t, v, lo, hi, o, p, m) {k, l, d, "Indexing", t, v, lo, hi, o, p, m, SET_CONFIG}
#define REF(k, l, d, p) \
    {k, l, d, "Reference", "string", NULL, 0, 0, NULL, p, "launch", SET_REFERENCE}
static const setting_t settings[] = {
    CFG("auto_index", "Automatic indexing", "Index a project when an MCP session connects.",
        "boolean", "false", 0, 0, NULL, "src/mcp/mcp.c", "next-session"),
    CFG("auto_index_limit", "Automatic indexing file limit",
        "Maximum discovered files for automatic indexing of a new project.", "integer", "50000", 1,
        INT_MAX, NULL, "src/mcp/mcp.c", "next-session"),
    CFG("auto_watch", "Automatic watching",
        "Register a background Git watcher when an MCP session connects.", "boolean", "true", 0, 0,
        NULL, "src/mcp/mcp.c", "next-session"),
    CFG("watcher_enabled", "Background watcher", "Enable the daemon's background watcher thread.",
        "boolean", "true", 0, 0, NULL, "src/daemon/application.c", "restart"),
    {"ui-lang", "Interface language", "Language preference used by subsequent HTTP requests.",
     "Server", "enum", "auto", 0, 0, "auto|en|zh", "src/ui/http_server.c", "next-request",
     SET_CONFIG},
    {"ui_enabled", "HTTP interface",
     "Enable the loopback HTTP interface. Without a legacy config file, a build with embedded "
     "assets enables it automatically.",
     "Server", "boolean", "false", 0, 0, NULL, "src/ui/config.c", "restart", SET_UI},
    {"ui_port", "HTTP port",
     "Loopback listener port. The running listener remains on its current port until restart.",
     "Server", "integer", "9749", 1, 65535, NULL, "src/ui/config.c", "restart", SET_UI},
    ENV("CBM_MAX_FILE_BYTES", "Maximum file bytes", "Skip files larger than this limit.",
        "Indexing", "integer", "536870912", 1, SETTINGS_MAX_LONG, NULL, "src/foundation/limits.c"),
    ENV("CBM_CYPHER_MAX_DEPTH", "Query traversal depth",
        "Maximum variable-length Cypher traversal depth.", "Indexing", "integer", "10", 1, INT_MAX,
        NULL, "src/foundation/limits.c"),
    ENV("CBM_MCP_MAX_DEPTH", "MCP traversal depth", "Maximum MCP graph traversal depth.",
        "Indexing", "integer", "15", 1, INT_MAX, NULL, "src/foundation/limits.c"),
    ENV("CBM_WORKERS", "Indexing workers",
        "Automatic uses available CPU cores and foreground/background policy. Supervisor recovery "
        "can still force a single worker.",
        "Resources", "integer", NULL, 1, 256, NULL, "src/foundation/system_info.c"),
    ENV("CBM_MEM_BUDGET_MB", "Memory budget (MiB)",
        "Automatic uses a RAM-dependent fraction; the result remains capped by physical RAM and "
        "the daemon hard limit.",
        "Resources", "integer", NULL, 1, 2147483647, NULL, "src/foundation/mem.c"),
    ENV("CBM_SQLITE_MMAP_SIZE", "SQLite mapping bytes",
        "Memory mapping limit for new database connections; zero disables mapping.", "Resources",
        "integer", "67108864", 0, SETTINGS_MAX_LONG, NULL, "src/store/store.c"),
    ENV("CBM_RETAIN_TOTAL_MB", "Retained extraction budget (MiB)",
        "Automatic is the smaller of one eighth of the memory budget and 1024 MiB. Explicit "
        "indexing options retain precedence.",
        "Resources", "integer", NULL, 1, 2147483647, NULL, "src/pipeline/pass_parallel.c"),
    ENV("CBM_RETAIN_PER_FILE_MB", "Retained per-file budget (MiB)",
        "Automatic is the smaller of 32 MiB and the total retention budget. Explicit indexing "
        "options retain precedence.",
        "Resources", "integer", NULL, 1, 2147483647, NULL, "src/pipeline/pass_parallel.c"),
    ENV("CBM_SEMANTIC_ENABLED", "Semantic indexing",
        "Enable optional semantic embedding generation.", "Indexing", "boolean", "false", 0, 0,
        NULL, "src/semantic/semantic.c"),
    ENV("CBM_SEMANTIC_THRESHOLD", "Semantic similarity threshold",
        "Minimum similarity score, greater than zero and at most one.", "Indexing", "number",
        "0.75", 0, 1, NULL, "src/semantic/semantic.c"),
    ENV("CBM_DUMP_VERIFY_MIN_RATIO", "Dump verification ratio",
        "Minimum ratio accepted when verifying an index dump.", "Indexing", "number", "0.5", 0, 1,
        NULL, "src/foundation/dump_verify.c"),
    ENV("CBM_WATCHER_PRUNE_GRACE_S", "Watcher prune grace (seconds)",
        "Grace period before removing stale watcher registrations.", "Indexing", "integer", "600",
        0, INT_MAX, NULL, "src/watcher/watcher.c"),
    ENV("CBM_DISABLE_LSP_CROSS", "Disable cross-file resolution",
        "Disable cross-file LSP resolution. False masks an inherited presence flag, even if its "
        "inherited value is zero.",
        "Indexing", "boolean", "false", 0, 0, NULL, "src/pipeline/pipeline.c"),
    ENV("CBM_TS_TYPE_BUDGET", "TypeScript resolver budget",
        "Automatic is 1,000,000 plus 64 times source length; zero means unlimited.", "Indexing",
        "integer", NULL, 0, INT_MAX, NULL, "internal/cbm/lsp/ts_lsp.c"),
    ENV("CBM_LSP_MAX_WALK_DEPTH", "Resolver walk depth", "Maximum recursive resolver walk depth.",
        "Indexing", "integer", "512", 1, INT_MAX, NULL, "internal/cbm/lsp/scope.h"),
    ENV("CBM_WALK_DEFS_MAX", "Definition walk frame limit",
        "Maximum extraction definition-walk frames.", "Indexing", "integer", "8388608", 1, INT_MAX,
        NULL, "internal/cbm/extract_defs.c"),
    ENV("CBM_INDEX_MAX_RESTARTS", "Index worker restart limit",
        "Maximum worker restarts. Values start at one to preserve the daemon and MCP consumers' "
        "shared behavior.",
        "Indexing", "integer", "100", 1, INT_MAX, NULL, "src/mcp/mcp.c; src/daemon/application.c"),
    ENV("CBM_INDEX_WORKER_TIMEOUT_S", "Index worker quiet timeout (seconds)",
        "Terminate an index worker that has not reported progress within this period.", "Indexing",
        "integer", "900", 1, INT_MAX / 1000, NULL, "src/mcp/index_supervisor.c"),
    ENV("CBM_STARTUP_TIMEOUT_MS", "Startup timeout (milliseconds)",
        "Time allowed for daemon startup.", "Server", "integer", "30000", 1000, 600000, NULL,
        "src/main.c"),
    ENV("CBM_HOOK_DEADLINE_MS", "Hook deadline (milliseconds)",
        "POSIX hook processing deadline; unavailable on Windows.", "Server", "integer", "2000", 50,
        10000, NULL, "src/cli/hook_augment.c"),
    ENV("CBM_UI_MAX_RENDER_NODES", "Graph layout node ceiling",
        "Maximum nodes the layout backend may return; frontend render budgets remain separate.",
        "Server", "integer", "10000000", 1, 10000000, NULL, "src/ui/layout3d.c"),
    ENV("CBM_UI_LOG_ROTATE_BYTES", "Frontend log rotation bytes",
        "Rotate the frontend compatibility log after this size.", "Server", "integer", "5242880", 1,
        SETTINGS_MAX_LONG, NULL, "src/ui/http_server.c"),
    ENV("CBM_LOG_LEVEL", "Log level",
        "Daemon log level. Explicit command flags and required worker progress records retain "
        "their process-specific policies.",
        "Diagnostics", "enum", "info", 0, 0, "debug|info|warn|error|none", "src/foundation/log.c"),
    ENV("CBM_LOG_FORMAT", "Log format", "Choose plain text or structured JSON log records.",
        "Diagnostics", "enum", "text", 0, 0, "text|json", "src/foundation/log.c"),
    ENV("CBM_PROFILE", "Profiling",
        "Enable performance profiling; the explicit --profile launch flag also enables it.",
        "Diagnostics", "boolean", "false", 0, 0, NULL, "src/foundation/profile.c"),
    ENV("CBM_DIAGNOSTICS", "Diagnostics", "Enable runtime diagnostics.", "Diagnostics", "boolean",
        "false", 0, 0, NULL, "src/foundation/diagnostics.c"),
    ENV("CBM_MEM_STATS", "Memory statistics", "Emit memory statistics in diagnostics.",
        "Diagnostics", "boolean", "false", 0, 0, NULL, "src/foundation/diagnostics.c"),
    REF("CBM_MEM_PROFILE", "Memory profiling",
        "This profiler is not compiled into the shipped application; setting it has no effect in "
        "this build.",
        "src/foundation/mem_profile.c"),
    REF("CBM_MEM_PROFILE_MIN", "Memory profile minimum bytes",
        "This profiler is not compiled into the shipped application; setting it has no effect in "
        "this build.",
        "src/foundation/mem_profile.c"),
    ENV("CBM_MEM_PHASES", "Memory phase diagnostics", "Emit indexing memory phase measurements.",
        "Diagnostics", "boolean", "false", 0, 0, NULL, "src/foundation/mem.c"),
    ENV("CBM_MEM_CENSUS", "Memory census",
        "Enable allocation census diagnostics; primarily useful on Windows.", "Diagnostics",
        "boolean", "false", 0, 0, NULL, "src/foundation/mem.c"),
    ENV("CBM_MI_THREAD_DONE", "Allocator thread cleanup",
        "Windows allocator thread cleanup; other platforms do not use this switch.", "Resources",
        "boolean", "true", 0, 0, NULL, "src/foundation/compat_thread.c"),
    REF("CBM_CACHE_DIR", "Cache directory",
        "Bootstrap location of indexes and this configuration database. Change the launch "
        "environment, then restart.",
        "src/foundation/platform.c"),
    REF("CBM_RUNTIME_DIR", "Runtime directory",
        "Bootstrap location of local daemon coordination. Configure in the launch environment.",
        "src/daemon/bootstrap.c"),
    REF("CBM_ALLOWED_ROOT", "Allowed root",
        "Launch-time filesystem boundary. The Config dialog cannot widen permissions.",
        "src/foundation/workspace.c"),
    REF("CBM_INDEX_SUPERVISOR", "Index supervisor",
        "Process isolation is a safety boundary; disabling it is refused on physical hosts.",
        "src/mcp/index_supervisor.c"),
    REF("CBM_HOOK_TIMEOUT_LOG", "Hook timeout log path",
        "Launch-time diagnostic output path. Arbitrary write destinations are not editable here.",
        "src/cli/hook_augment.c"),
    REF("CBM_INDEX_LOG", "Index worker log path",
        "Launch-time diagnostic output path; automatic uses the project and current time.",
        "src/mcp/mcp.c"),
    REF("CBM_MEM_STATS_OUT", "Memory statistics output path",
        "Launch-time diagnostic output path. Arbitrary write destinations are not editable here.",
        "src/foundation/mem.c"),
    REF("CBM_LSP_DISABLED", "Resolver diagnostic disable",
        "Diagnostic resolver bypass, configured explicitly at launch.",
        "internal/cbm/lsp/ts_lsp.c"),
    REF("CBM_LSP_DEBUG", "Resolver debug output",
        "Diagnostic output may expose source information; configured explicitly at launch.",
        "internal/cbm/lsp"),
    REF("CBM_LSP_KOTLIN_AST", "Kotlin syntax tree dump",
        "Source-dumping diagnostic, configured explicitly at launch.",
        "internal/cbm/lsp/kotlin_lsp.c")};
#define SETTINGS_COUNT (sizeof(settings) / sizeof(settings[0]))
static bool initialized;
static char initial_cache[SETTINGS_PATH_CAP];
static char initial_values[SETTINGS_COUNT][SETTINGS_VALUE_CAP];
static bool initial_present[SETTINGS_COUNT];
static char environment_values[SETTINGS_COUNT][SETTINGS_VALUE_CAP];
static bool environment_overridden[SETTINGS_COUNT];
static atomic_int active_http_port;

void cbm_runtime_settings_note_http_port(int port) {
    atomic_store_explicit(&active_http_port, port, memory_order_relaxed);
}

static bool canonical_cache_path(const char *cache, char out[SETTINGS_PATH_CAP], bool create) {
    if (!cache || !cache[0])
        return false;
    bool ready = cbm_canonical_path(cache, out, SETTINGS_PATH_CAP);
    if (!ready && create && cbm_mkdir_p(cache, 0700))
        ready = cbm_canonical_path(cache, out, SETTINGS_PATH_CAP);
    if (!ready || !cbm_is_dir(out))
        return false;
    cbm_normalize_path_sep(out);
    return cbm_daemon_ipc_private_directory_secure(out);
}

static sqlite3 *settings_open(const char *cache) {
    if (!cache || !cache[0] || strlen(cache) + 12 >= SETTINGS_PATH_CAP)
        return NULL;
    char canonical[SETTINGS_PATH_CAP];
    if (!canonical_cache_path(cache, canonical, true))
        return NULL;
    cbm_config_t *base = cbm_config_open(canonical);
    if (!base)
        return NULL;
    cbm_config_close(base);
    char path[SETTINGS_PATH_CAP];
    if (snprintf(path, sizeof(path), "%s/_config.db", canonical) >= (int)sizeof(path))
        return NULL;
    sqlite3 *db = NULL;
    if (sqlite3_open(path, &db) != SQLITE_OK) {
        if (db)
            sqlite3_close(db);
        return NULL;
    }
    sqlite3_busy_timeout(db, 3000);
    const char *schema =
        "BEGIN IMMEDIATE;"
        "CREATE TABLE IF NOT EXISTS runtime_settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS runtime_settings_revision(id INTEGER PRIMARY KEY "
        "CHECK(id=1),revision INTEGER NOT NULL);"
        "INSERT OR IGNORE INTO runtime_settings_revision VALUES(1,0);"
        "CREATE TRIGGER IF NOT EXISTS settings_insert AFTER INSERT ON runtime_settings BEGIN "
        "UPDATE runtime_settings_revision SET revision=revision+1 WHERE id=1; END;"
        "CREATE TRIGGER IF NOT EXISTS settings_update AFTER UPDATE ON runtime_settings BEGIN "
        "UPDATE runtime_settings_revision SET revision=revision+1 WHERE id=1; END;"
        "CREATE TRIGGER IF NOT EXISTS settings_delete AFTER DELETE ON runtime_settings BEGIN "
        "UPDATE runtime_settings_revision SET revision=revision+1 WHERE id=1; END;"
        "CREATE TRIGGER IF NOT EXISTS config_settings_insert AFTER INSERT ON config BEGIN UPDATE "
        "runtime_settings_revision SET revision=revision+1 WHERE id=1; END;"
        "CREATE TRIGGER IF NOT EXISTS config_settings_update AFTER UPDATE ON config BEGIN UPDATE "
        "runtime_settings_revision SET revision=revision+1 WHERE id=1; END;"
        "CREATE TRIGGER IF NOT EXISTS config_settings_delete AFTER DELETE ON config BEGIN UPDATE "
        "runtime_settings_revision SET revision=revision+1 WHERE id=1; END;COMMIT;";
    if (sqlite3_exec(db, schema, NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    return db;
}

static bool read_value(sqlite3 *db, const setting_t *setting, char out[SETTINGS_VALUE_CAP]) {
    if (!db)
        return false;
    sqlite3_stmt *statement = NULL;
    const char *sql = setting->storage == SET_CONFIG
                          ? "SELECT value FROM config WHERE key=?"
                          : "SELECT value FROM runtime_settings WHERE key=?";
    bool found = false;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, setting->key, -1, SQLITE_STATIC);
        if (sqlite3_step(statement) == SQLITE_ROW) {
            const char *value = (const char *)sqlite3_column_text(statement, 0);
            if (value && strlen(value) < SETTINGS_VALUE_CAP) {
                (void)snprintf(out, SETTINGS_VALUE_CAP, "%s", value);
                found = true;
            }
        }
    }
    sqlite3_finalize(statement);
    return found;
}

static bool valid_value(const setting_t *setting, const char *value) {
    if (!value || strlen(value) >= SETTINGS_VALUE_CAP)
        return false;
    if (strcmp(setting->type, "boolean") == 0)
        return strcmp(value, "true") == 0 || strcmp(value, "false") == 0;
    if (strcmp(setting->type, "enum") == 0) {
        for (const char *option = setting->options; option && *option;) {
            size_t length = strcspn(option, "|");
            if (strlen(value) == length && strncmp(option, value, length) == 0)
                return true;
            option += length;
            if (*option)
                option++;
        }
        return false;
    }
    if (!value[0] || strspn(value, "+-0123456789.eE") != strlen(value))
        return false;
    char *end = NULL;
    errno = 0;
    double number;
    if (strcmp(setting->type, "integer") == 0) {
        long long integer = strtoll(value, &end, 10);
        number = (double)integer;
    } else
        number = strtod(value, &end);
    if (strcmp(setting->key, "CBM_SEMANTIC_THRESHOLD") == 0 && number <= 0)
        return false;
    return errno == 0 && end && !*end && isfinite(number) && number >= setting->minimum &&
           number <= setting->maximum;
}

static const char *inherited_value(const setting_t *setting, char out[SETTINGS_VALUE_CAP]) {
    if (!cbm_native_getenv(setting->key, out, SETTINGS_VALUE_CAP, NULL))
        return NULL;
    if (setting->storage == SET_REFERENCE)
        return out;
    if (strcmp(setting->key, "CBM_STARTUP_TIMEOUT_MS") == 0 ||
        strcmp(setting->key, "CBM_HOOK_DEADLINE_MS") == 0 ||
        strcmp(setting->key, "CBM_SQLITE_MMAP_SIZE") == 0) {
        char *end = NULL;
        errno = 0;
        long long number = strtoll(out, &end, 10);
        bool startup = strcmp(setting->key, "CBM_STARTUP_TIMEOUT_MS") == 0;
        bool mmap_size = strcmp(setting->key, "CBM_SQLITE_MMAP_SIZE") == 0;
        if (end == out || *end ||
            (!startup && !mmap_size && (errno || strspn(out, "+-0123456789") != strlen(out))))
            return out;
        if (startup && number <= 0)
            return out;
        if (number < (long long)setting->minimum)
            number = (long long)setting->minimum;
        if (!mmap_size && number > (long long)setting->maximum)
            number = (long long)setting->maximum;
        (void)snprintf(out, SETTINGS_VALUE_CAP, "%lld", number);
        return out;
    }
    if (strcmp(setting->type, "boolean") == 0) {
        bool enabled;
        if (strcmp(setting->key, "CBM_DISABLE_LSP_CROSS") == 0)
            enabled = true;
        else if (strcmp(setting->key, "CBM_PROFILE") == 0)
            enabled = out[0] && out[0] != '0';
        else if (strcmp(setting->key, "CBM_DIAGNOSTICS") == 0)
            enabled = strcmp(out, "1") == 0 || strcmp(out, "true") == 0;
        else if (strcmp(setting->key, "CBM_MI_THREAD_DONE") == 0)
            enabled = out[0] != '0';
        else
            enabled = out[0] == '1';
        (void)snprintf(out, SETTINGS_VALUE_CAP, "%s", enabled ? "true" : "false");
        return out;
    }
    char original[SETTINGS_VALUE_CAP];
    (void)snprintf(original, sizeof(original), "%s", out);
    if (strcmp(setting->key, "CBM_LOG_LEVEL") == 0 || strcmp(setting->key, "CBM_LOG_FORMAT") == 0) {
        for (char *p = out; *p; p++)
            *p = (char)tolower((unsigned char)*p);
    }
    if (strcmp(setting->key, "CBM_LOG_LEVEL") == 0 && strlen(out) == 1 && out[0] >= '0' &&
        out[0] <= '4') {
        const char *levels[] = {"debug", "info", "warn", "error", "none"};
        (void)snprintf(out, SETTINGS_VALUE_CAP, "%s", levels[out[0] - '0']);
    }
    /* Legacy readers have different permissive parsing rules. Preserve an
     * unrecognized inherited input instead of claiming the default is active;
     * the snapshot explicitly marks an unverified effective value below. */
    if (!valid_value(setting, out))
        (void)snprintf(out, SETTINGS_VALUE_CAP, "%s", original);
    return out;
}

static const char *desired_value(sqlite3 *db, const setting_t *setting,
                                 char out[SETTINGS_VALUE_CAP], bool *saved, const char **source) {
    *saved = setting->storage != SET_REFERENCE && read_value(db, setting, out);
    if (*saved && setting->storage == SET_CONFIG && strcmp(setting->type, "boolean") == 0) {
        if (strcmp(out, "1") == 0 || strcmp(out, "on") == 0)
            (void)snprintf(out, SETTINGS_VALUE_CAP, "true");
        else if (strcmp(out, "0") == 0 || strcmp(out, "off") == 0)
            (void)snprintf(out, SETTINGS_VALUE_CAP, "false");
    }
    *saved = *saved && valid_value(setting, out);
    if (*saved) {
        *source = setting->storage == SET_CONFIG ? "config" : "saved override";
        return out;
    }
    if (setting->storage == SET_ENV || setting->storage == SET_REFERENCE) {
        if (inherited_value(setting, out)) {
            *source = "environment";
            return out;
        }
    } else if (setting->storage == SET_UI) {
        cbm_ui_config_t legacy;
        cbm_ui_config_load_legacy(&legacy);
        if (strcmp(setting->key, "ui_enabled") == 0)
            (void)snprintf(out, SETTINGS_VALUE_CAP, "%s", legacy.ui_enabled ? "true" : "false");
        else
            (void)snprintf(out, SETTINGS_VALUE_CAP, "%d", legacy.ui_port);
        *source = "legacy config / default";
        return out;
    }
    *source = "default";
    return setting->default_value;
}

static bool resolve_environment(const char *key, const char **value) {
    if (!key)
        return false;
    for (size_t i = 0; i < SETTINGS_COUNT; i++) {
        if (!environment_overridden[i] || strcmp(settings[i].key, key) != 0)
            continue;
        if (strcmp(settings[i].type, "boolean") == 0) {
            bool enabled = strcmp(environment_values[i], "true") == 0;
            *value = !enabled && strcmp(key, "CBM_DISABLE_LSP_CROSS") == 0 ? NULL
                     : enabled                                             ? "1"
                                                                           : "0";
        } else
            *value = environment_values[i];
        return true;
    }
    return false;
}

bool cbm_runtime_settings_initialized(void) {
    return initialized;
}

#ifdef CBM_ENABLE_TEST_SEAMS
void cbm_runtime_settings_reset_for_tests(void) {
    cbm_set_environment_resolver(NULL);
    initialized = false;
    initial_cache[0] = '\0';
    memset(initial_present, 0, sizeof(initial_present));
    memset(environment_overridden, 0, sizeof(environment_overridden));
    cbm_runtime_settings_note_http_port(0);
}
#endif

bool cbm_runtime_settings_init(const char *cache) {
    char canonical[SETTINGS_PATH_CAP];
    if (!canonical_cache_path(cache, canonical, true))
        return false;
    if (initialized)
        return strcmp(canonical, initial_cache) == 0;
    char path[SETTINGS_PATH_CAP];
    if (snprintf(path, sizeof(path), "%s/_config.db", canonical) >= (int)sizeof(path))
        return false;
    sqlite3 *db = NULL;
    if (cbm_file_exists(path)) {
        if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
            if (db)
                sqlite3_close(db);
            return false;
        }
        /* Startup, especially hook clients, must never wait on a schema writer.
         * This read forces the shared snapshot before individual optional tables
         * are queried; a busy database is an error, not an absent override. */
        sqlite3_busy_timeout(db, 50);
        if (sqlite3_exec(db, "BEGIN; SELECT count(*) FROM sqlite_master", NULL, NULL, NULL) !=
            SQLITE_OK) {
            sqlite3_close(db);
            return false;
        }
    }
    for (size_t i = 0; i < SETTINGS_COUNT; i++) {
        bool saved = false;
        const char *source = NULL;
        char temporary[SETTINGS_VALUE_CAP];
        const char *value = desired_value(db, &settings[i], temporary, &saved, &source);
        initial_present[i] = value != NULL;
        if (value)
            (void)snprintf(initial_values[i], SETTINGS_VALUE_CAP, "%s", value);
        environment_overridden[i] = saved && settings[i].storage == SET_ENV;
        if (environment_overridden[i])
            (void)snprintf(environment_values[i], SETTINGS_VALUE_CAP, "%s", value);
    }
    bool ok = !db || sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) == SQLITE_OK;
    if (db)
        sqlite3_close(db);
    if (!ok)
        return false;
    (void)snprintf(initial_cache, sizeof(initial_cache), "%s", canonical);
    cbm_set_environment_resolver(resolve_environment);
    initialized = true;
    return true;
}

static void json_string(yyjson_mut_doc *doc, yyjson_mut_val *object, const char *key,
                        const char *value) {
    if (value)
        yyjson_mut_obj_add_strcpy(doc, object, key, value);
    else
        yyjson_mut_obj_add_null(doc, object, key);
}

static void revision(sqlite3 *db, char out[32]) {
    sqlite3_stmt *statement = NULL;
    out[0] = '\0';
    if (sqlite3_prepare_v2(db, "SELECT revision FROM runtime_settings_revision WHERE id=1", -1,
                           &statement, NULL) == SQLITE_OK &&
        sqlite3_step(statement) == SQLITE_ROW)
        (void)snprintf(out, 32, "%lld", (long long)sqlite3_column_int64(statement, 0));
    sqlite3_finalize(statement);
}

static char *snapshot(sqlite3 *db, const char *cache) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc)
        return NULL;
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    char rev[32];
    revision(db, rev);
    json_string(doc, root, "revision", rev);
    json_string(doc, root, "precedence",
                "Explicit CLI/UI configuration writes share one store; saved environment overrides "
                "take precedence over inherited environment after restart. Per-command flags and "
                "supervisor safety limits retain precedence.");
    yyjson_mut_val *items = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, root, "settings", items);
    char canonical[SETTINGS_PATH_CAP];
    bool current_process = initialized && cbm_canonical_path(cache, canonical, sizeof(canonical)) &&
                           strcmp(canonical, initial_cache) == 0;
    for (size_t i = 0; i < SETTINGS_COUNT; i++) {
        const setting_t *setting = &settings[i];
        char buffer[SETTINGS_VALUE_CAP];
        bool saved = false;
        const char *source = NULL;
        const char *value = desired_value(db, setting, buffer, &saved, &source);
        const char *effective = value;
        if (current_process && strcmp(setting->apply_mode, "restart") == 0)
            effective = initial_present[i] ? initial_values[i] : NULL;
        const char *baseline = effective;
        char bound_port[32];
        int actual_port = atomic_load_explicit(&active_http_port, memory_order_relaxed);
        if (current_process && actual_port > 0 && setting->storage == SET_UI) {
            (void)snprintf(bound_port, sizeof(bound_port), "%d", actual_port);
            effective = strcmp(setting->key, "ui_port") == 0 ? bound_port : "true";
            baseline = effective;
        }
        if (current_process && strcmp(setting->key, "CBM_LOG_LEVEL") == 0) {
            static const char *levels[] = {"debug", "info", "warn", "error", "none"};
            unsigned level = (unsigned)cbm_log_get_level();
            if (level < sizeof(levels) / sizeof(levels[0]))
                effective = levels[level];
        }
        if (current_process && strcmp(setting->key, "CBM_LOG_FORMAT") == 0)
            effective = cbm_log_get_format() == CBM_LOG_FORMAT_JSON ? "json" : "text";
        bool pending =
            strcmp(setting->apply_mode, "restart") == 0 &&
            ((!value != !baseline) || (value && baseline && strcmp(value, baseline) != 0));
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_arr_append(items, item);
        json_string(doc, item, "key", setting->key);
        json_string(doc, item, "label", setting->label);
        json_string(doc, item, "description", setting->description);
        json_string(doc, item, "category", setting->category);
        json_string(doc, item, "type", setting->type);
        json_string(doc, item, "defaultValue", setting->default_value);
        json_string(doc, item, "value", value);
        json_string(doc, item, "override", saved ? value : NULL);
        json_string(doc, item, "effective", effective);
        yyjson_mut_obj_add_bool(doc, item, "effectiveKnown",
                                setting->storage != SET_ENV || !effective ||
                                    valid_value(setting, effective));
        json_string(doc, item, "source", source);
        json_string(doc, item, "applyMode", setting->apply_mode);
        json_string(doc, item, "sourceFile", setting->source_file);
        yyjson_mut_obj_add_bool(doc, item, "pendingRestart", pending);
        bool editable = setting->storage != SET_REFERENCE;
#ifdef _WIN32
        if (strcmp(setting->key, "CBM_HOOK_DEADLINE_MS") == 0)
            editable = false;
#else
        if (strcmp(setting->key, "CBM_MI_THREAD_DONE") == 0)
            editable = false;
#endif
        yyjson_mut_obj_add_bool(doc, item, "editable", editable);
        if (!editable)
            json_string(doc, item, "readOnlyReason", setting->description);
        if (setting->storage == SET_ENV || setting->storage == SET_REFERENCE)
            json_string(doc, item, "environment", setting->key);
        if (setting->storage == SET_CONFIG || setting->storage == SET_UI) {
            char command[SETTINGS_VALUE_CAP];
            (void)snprintf(command, sizeof(command), "config set %s <value>", setting->key);
            json_string(doc, item, "cli", command);
        }
        if (strcmp(setting->type, "integer") == 0 || strcmp(setting->type, "number") == 0) {
            yyjson_mut_obj_add_real(doc, item, "minimum", setting->minimum);
            yyjson_mut_obj_add_real(doc, item, "maximum", setting->maximum);
        }
        if (setting->options) {
            yyjson_mut_val *options = yyjson_mut_arr(doc);
            yyjson_mut_obj_add_val(doc, item, "options", options);
            for (const char *option = setting->options; *option;) {
                size_t size = strcspn(option, "|");
                yyjson_mut_arr_append(options, yyjson_mut_strncpy(doc, option, size));
                option += size;
                if (*option)
                    option++;
            }
        }
    }
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

char *cbm_runtime_settings_get_json(const char *cache) {
    sqlite3 *db = settings_open(cache);
    if (!db)
        return NULL;
    if (sqlite3_exec(db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    char *json = snapshot(db, cache);
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_close(db);
    return json;
}

static char *error_json(const char *message, int code, int *status) {
    if (status)
        *status = code;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc)
        return NULL;
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    json_string(doc, root, "error", message);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

static bool write_value(sqlite3 *db, const setting_t *setting, const char *value) {
    const char *sql;
    if (setting->storage == SET_CONFIG)
        sql = value ? "INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET "
                      "value=excluded.value"
                    : "DELETE FROM config WHERE key=?";
    else
        sql = value ? "INSERT INTO runtime_settings(key,value) VALUES(?,?) ON CONFLICT(key) DO "
                      "UPDATE SET value=excluded.value"
                    : "DELETE FROM runtime_settings WHERE key=?";
    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK)
        return false;
    sqlite3_bind_text(statement, 1, setting->key, -1, SQLITE_STATIC);
    if (value)
        sqlite3_bind_text(statement, 2, value, -1, SQLITE_TRANSIENT);
    bool ok = sqlite3_step(statement) == SQLITE_DONE;
    sqlite3_finalize(statement);
    return ok;
}

char *cbm_runtime_settings_apply_json(const char *cache, const char *body, size_t length,
                                      int *status) {
    if (!body || !length || length > SETTINGS_BODY_CAP)
        return error_json("Invalid configuration request.", 400, status);
    yyjson_doc *doc = yyjson_read(body, length, 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *rev = yyjson_obj_get(root, "revision"), *changes = yyjson_obj_get(root, "changes");
    if (!yyjson_is_obj(root) || yyjson_obj_size(root) != 2 || !yyjson_is_str(rev) ||
        !yyjson_is_obj(changes) || yyjson_obj_size(changes) == 0 ||
        yyjson_obj_size(changes) > SETTINGS_COUNT) {
        yyjson_doc_free(doc);
        return error_json("Expected revision and a non-empty changes object.", 400, status);
    }
    const char *expected = yyjson_get_str(rev);
    if (!expected || !expected[0] || yyjson_get_len(rev) != strlen(expected) ||
        strlen(expected) >= 32 || strspn(expected, "0123456789") != strlen(expected)) {
        yyjson_doc_free(doc);
        return error_json("Invalid configuration revision.", 400, status);
    }
    bool seen[SETTINGS_COUNT] = {false};
    const char *values[SETTINGS_COUNT] = {0};
    size_t index, maximum;
    yyjson_val *key, *value;
    yyjson_obj_foreach(changes, index, maximum, key, value) {
        const char *name = yyjson_get_str(key);
        size_t found = SETTINGS_COUNT;
        for (size_t i = 0; i < SETTINGS_COUNT; i++)
            if (strcmp(name, settings[i].key) == 0) {
                found = i;
                break;
            }
        bool editable = found < SETTINGS_COUNT && settings[found].storage != SET_REFERENCE;
#ifdef _WIN32
        if (editable && strcmp(name, "CBM_HOOK_DEADLINE_MS") == 0)
            editable = false;
#else
        if (editable && strcmp(name, "CBM_MI_THREAD_DONE") == 0)
            editable = false;
#endif
        if (!editable || yyjson_get_len(key) != strlen(name) || seen[found] ||
            (!yyjson_is_null(value) &&
             (!yyjson_is_str(value) || yyjson_get_len(value) != strlen(yyjson_get_str(value)) ||
              !valid_value(&settings[found], yyjson_get_str(value))))) {
            yyjson_doc_free(doc);
            return error_json(
                "Unknown, read-only, duplicate or invalid setting; no changes were saved.", 400,
                status);
        }
        seen[found] = true;
        values[found] = yyjson_is_null(value) ? NULL : yyjson_get_str(value);
    }
    sqlite3 *db = settings_open(cache);
    if (!db || sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) {
        if (db)
            sqlite3_close(db);
        yyjson_doc_free(doc);
        return error_json("Configuration storage unavailable.", 500, status);
    }
    char actual[32];
    revision(db, actual);
    if (strcmp(expected, actual) != 0) {
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        sqlite3_close(db);
        yyjson_doc_free(doc);
        return error_json(
            "Configuration changed in another window or command. Reload before saving.", 409,
            status);
    }
    bool ok = true;
    for (size_t i = 0; i < SETTINGS_COUNT && ok; i++)
        if (seen[i])
            ok = write_value(db, &settings[i], values[i]);
    char *result = ok ? snapshot(db, cache) : NULL;
    ok = ok && result && sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) == SQLITE_OK;
    if (!ok) {
        free(result);
        result = NULL;
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    }
    sqlite3_close(db);
    yyjson_doc_free(doc);
    if (!ok)
        return error_json("Could not save configuration; no changes were saved.", 500, status);
    if (status)
        *status = 200;
    return result;
}

void cbm_runtime_settings_load_ui(bool *enabled, int *port) {
    const char *cache = cbm_resolve_cache_dir();
    if (!cache)
        return;
    char canonical[SETTINGS_PATH_CAP];
    if (!canonical_cache_path(cache, canonical, false))
        return;
    char path[SETTINGS_PATH_CAP];
    if (snprintf(path, sizeof(path), "%s/_config.db", canonical) >= (int)sizeof(path))
        return;
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (db)
            sqlite3_close(db);
        return;
    }
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    for (size_t i = 0; i < SETTINGS_COUNT; i++)
        if (settings[i].storage == SET_UI) {
            char value[SETTINGS_VALUE_CAP];
            if (read_value(db, &settings[i], value) && valid_value(&settings[i], value)) {
                if (strcmp(settings[i].key, "ui_enabled") == 0)
                    *enabled = strcmp(value, "true") == 0;
                else
                    *port = (int)strtol(value, NULL, 10);
            }
        }
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_close(db);
}

bool cbm_runtime_settings_save_ui(bool enabled, int port) {
    if (port < 1 || port > 65535)
        return false;
    sqlite3 *db = settings_open(cbm_resolve_cache_dir());
    if (!db)
        return false;
    bool ok = sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) == SQLITE_OK;
    for (size_t i = 0; i < SETTINGS_COUNT && ok; i++)
        if (settings[i].storage == SET_UI) {
            char value[32];
            if (strcmp(settings[i].key, "ui_enabled") == 0)
                (void)snprintf(value, sizeof(value), "%s", enabled ? "true" : "false");
            else
                (void)snprintf(value, sizeof(value), "%d", port);
            ok = write_value(db, &settings[i], value);
        }
    ok = ok && sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) == SQLITE_OK;
    if (!ok)
        sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    sqlite3_close(db);
    return ok;
}
