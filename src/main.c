/*
 * main.c — Entry point for codebase-memory-mcp.
 *
 * Modes:
 *   (default)       Run as MCP server on stdin/stdout (JSON-RPC 2.0)
 *   cli <tool> <json>  Run a single tool call and print result
 *   --version       Print version and exit
 *   --help          Print usage and exit
 *   --ui=true/false Enable/disable HTTP UI server (persisted)
 *   --port=N        Set HTTP UI port (persisted, default 9749)
 *
 * Signal handling: SIGTERM/SIGINT trigger graceful shutdown.
 * Watcher runs in a background thread, polling for git changes.
 * HTTP UI server (optional) runs in a background thread on localhost.
 */
#include "mcp/mcp.h"
#include "watcher/watcher.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "cli/cli.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat_thread.h"
#include "foundation/mem.h"
#include "ui/config.h"
#include "ui/http_server.h"
#include "ui/embedded_assets.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <stdatomic.h>

#ifndef CBM_VERSION
#define CBM_VERSION "dev"
#endif

/* ── Globals for signal handling ────────────────────────────────── */

static cbm_watcher_t *g_watcher = NULL;
static cbm_mcp_server_t *g_server = NULL;
static cbm_http_server_t *g_http_server = NULL;
static atomic_int g_shutdown = 0;

static void signal_handler(int sig) {
    (void)sig;
    atomic_store(&g_shutdown, 1);
    if (g_watcher) {
        cbm_watcher_stop(g_watcher);
    }
    if (g_http_server) {
        cbm_http_server_stop(g_http_server);
    }
    /* Close stdin to unblock getline in the MCP server loop */
    (void)fclose(stdin);
}

/* ── Watcher background thread ──────────────────────────────────── */

static void *watcher_thread(void *arg) {
    cbm_watcher_t *w = arg;
#define WATCHER_BASE_INTERVAL_MS 5000
    cbm_watcher_run(w, WATCHER_BASE_INTERVAL_MS);
    return NULL;
}

/* ── HTTP UI background thread ──────────────────────────────────── */

static void *http_thread(void *arg) {
    cbm_http_server_t *srv = arg;
    cbm_http_server_run(srv);
    return NULL;
}

/* ── Index callback for watcher ─────────────────────────────────── */

static int watcher_index_fn(const char *project_name, const char *root_path, void *user_data) {
    (void)user_data;
    cbm_log_info("watcher.reindex", "project", project_name, "path", root_path);

    cbm_pipeline_t *p = cbm_pipeline_new(root_path, NULL, CBM_MODE_FULL);
    if (!p) {
        return -1;
    }

    int rc = cbm_pipeline_run(p);
    cbm_pipeline_free(p);
    return rc;
}

/* ── CLI mode ───────────────────────────────────────────────────── */

static int run_cli(int argc, char **argv) {
    if (argc < 1) {
        // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
        (void)fprintf(stderr, "Usage: codebase-memory-mcp cli <tool_name> [json_args]\n");
        return 1;
    }

    const char *tool_name = argv[0];
    const char *args_json = argc >= 2 ? argv[1] : "{}";

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
        (void)fprintf(stderr, "Failed to create server\n");
        return 1;
    }

    char *result = cbm_mcp_handle_tool(srv, tool_name, args_json);
    if (result) {
        printf("%s\n", result);
        free(result);
    }

    cbm_mcp_server_free(srv);
    return 0;
}

/* ── Help ───────────────────────────────────────────────────────── */

static void print_help(void) {
    printf("codebase-memory-mcp %s\n\n", CBM_VERSION);
    printf("Usage:\n");
    printf("  codebase-memory-mcp              Run MCP server on stdio\n");
    printf("  codebase-memory-mcp cli <tool> [json]  Run a single tool\n");
    printf("  codebase-memory-mcp install [-y|-n] [--force] [--dry-run]\n");
    printf("  codebase-memory-mcp uninstall [-y|-n] [--dry-run]\n");
    printf("  codebase-memory-mcp update [-y|-n]\n");
    printf("  codebase-memory-mcp config <list|get|set|reset>\n");
    printf("  codebase-memory-mcp --version    Print version\n");
    printf("  codebase-memory-mcp --help       Print this help\n");
    printf("\nUI options:\n");
    printf("  --ui=true    Enable HTTP graph visualization (persisted)\n");
    printf("  --ui=false   Disable HTTP graph visualization (persisted)\n");
    printf("  --port=N     Set UI port (default 9749, persisted)\n");
    printf("\nSupported agents (auto-detected):\n");
    printf("  Claude Code, Codex CLI, Gemini CLI, Zed, OpenCode, Antigravity, Aider, KiloCode\n");
    printf("\nTools: index_repository, search_graph, query_graph, trace_call_path,\n");
    printf("  get_code_snippet, get_graph_schema, get_architecture, search_code,\n");
    printf("  list_projects, delete_project, index_status, detect_changes,\n");
    printf("  manage_adr, ingest_traces\n");
}

/* ── Main ───────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    /* Parse arguments */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--version") == 0) {
            printf("codebase-memory-mcp %s\n", CBM_VERSION);
            return 0;
        }
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_help();
            return 0;
        }
        if (strcmp(argv[i], "cli") == 0) {
            cbm_mem_init(0.5);
            return run_cli(argc - i - 1, argv + i + 1);
        }
        if (strcmp(argv[i], "install") == 0) {
            return cbm_cmd_install(argc - i - 1, argv + i + 1);
        }
        if (strcmp(argv[i], "uninstall") == 0) {
            return cbm_cmd_uninstall(argc - i - 1, argv + i + 1);
        }
        if (strcmp(argv[i], "update") == 0) {
            return cbm_cmd_update(argc - i - 1, argv + i + 1);
        }
        if (strcmp(argv[i], "config") == 0) {
            return cbm_cmd_config(argc - i - 1, argv + i + 1);
        }
    }

    /* Default: MCP server on stdio */
    cbm_mem_init(0.5); /* 50% of RAM — safe now because mimalloc tracks ALL
                        * memory (C + C++ allocations) via global override.
                        * No more untracked heap blind spots. */
    /* Store binary path for subprocess spawning + hook log sink */
    cbm_http_server_set_binary_path(argv[0]);
    cbm_log_set_sink(cbm_ui_log_append);
    cbm_log_info("server.start", "version", CBM_VERSION);

    /* Parse --ui and --port flags (persisted config) */
    cbm_ui_config_t ui_cfg;
    cbm_ui_config_load(&ui_cfg);

    bool config_changed = false;
    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--ui=", 5) == 0) {
            ui_cfg.ui_enabled = (strcmp(argv[i] + 5, "true") == 0);
            config_changed = true;
        }
        if (strncmp(argv[i], "--port=", 7) == 0) {
            int p = (int)strtol(argv[i] + 7, NULL, 10);
            if (p > 0 && p < 65536) {
                ui_cfg.ui_port = p;
                config_changed = true;
            }
        }
    }
    if (config_changed) {
        cbm_ui_config_save(&ui_cfg);
    }

    /* Install signal handlers */
#ifdef _WIN32
    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);
#else
    // NOLINTNEXTLINE(misc-include-cleaner) — sigaction provided by standard header
    struct sigaction sa = {0};
    // NOLINTNEXTLINE(misc-include-cleaner) — sa_handler provided by standard header
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
#endif

    /* Open config store for runtime settings */
    char config_dir[1024];
    const char *cfg_home = cbm_get_home_dir();
    cbm_config_t *runtime_config = NULL;
    if (cfg_home) {
        snprintf(config_dir, sizeof(config_dir), "%s/.cache/codebase-memory-mcp", cfg_home);
        runtime_config = cbm_config_open(config_dir);
    }

    /* Create MCP server */
    g_server = cbm_mcp_server_new(NULL);
    if (!g_server) {
        cbm_log_error("server.err", "msg", "failed to create server");
        cbm_config_close(runtime_config);
        return 1;
    }

    /* Create and start watcher in background thread */
    cbm_store_t *watch_store = cbm_store_open_memory();
    g_watcher = cbm_watcher_new(watch_store, watcher_index_fn, NULL);

    /* Wire watcher + config into MCP server for session auto-index */
    cbm_mcp_server_set_watcher(g_server, g_watcher);
    cbm_mcp_server_set_config(g_server, runtime_config);
    cbm_thread_t watcher_tid;
    bool watcher_started = false;

    if (g_watcher) {
        if (cbm_thread_create(&watcher_tid, 0, watcher_thread, g_watcher) == 0) {
            watcher_started = true;
        }
    }

    /* Optionally start HTTP UI server in background thread */
    cbm_thread_t http_tid;
    bool http_started = false;

    if (ui_cfg.ui_enabled && CBM_EMBEDDED_FILE_COUNT > 0) {
        g_http_server = cbm_http_server_new(ui_cfg.ui_port);
        if (g_http_server) {
            if (cbm_thread_create(&http_tid, 0, http_thread, g_http_server) == 0) {
                http_started = true;
            }
        }
    } else if (ui_cfg.ui_enabled && CBM_EMBEDDED_FILE_COUNT == 0) {
        cbm_log_warn("ui.no_assets", "hint", "rebuild with: make -f Makefile.cbm cbm-with-ui");
    }

    /* Run MCP event loop (blocks until EOF or signal) */
    int rc = cbm_mcp_server_run(g_server, stdin, stdout);

    /* Shutdown */
    cbm_log_info("server.shutdown");

    if (http_started) {
        cbm_http_server_stop(g_http_server);
        cbm_thread_join(&http_tid);
        cbm_http_server_free(g_http_server);
        g_http_server = NULL;
    }

    if (watcher_started) {
        cbm_watcher_stop(g_watcher);
        cbm_thread_join(&watcher_tid);
    }
    cbm_watcher_free(g_watcher);
    cbm_store_close(watch_store);
    cbm_mcp_server_free(g_server);
    cbm_config_close(runtime_config);

    g_watcher = NULL;
    g_server = NULL;

    return rc;
}
