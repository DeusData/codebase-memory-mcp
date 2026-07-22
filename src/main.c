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
 *   --tool-profile=analysis|scout  Expose a restricted agent tool surface
 *   --idle-timeout=N Exit after N seconds without MCP requests (default: disabled)
 *
 * Signal handling: SIGTERM/SIGINT trigger graceful shutdown.
 * Watcher runs in a background thread, polling for git changes.
 * HTTP UI server (optional) runs in a background thread on localhost.
 */
#include "cbm.h" // cbm_alloc_init — bind 3rd-party allocators to mimalloc before any sqlite/git init
#include "mcp/mcp.h"
#include "mcp/index_supervisor.h"
#include "watcher/watcher.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "cli/cli.h"
#include "cli/progress_sink.h"
#include "foundation/constants.h"

enum {
    MAIN_MIN_ARGC = 1,
    MAIN_CLI_ARGC = 2,
    MAIN_FLAG_OFF = 5, /* strlen("--ui=") */
    MAIN_PORT_OFF = 7, /* strlen("--port=") */
    MAIN_IDLE_TIMEOUT_OFF = 15, /* strlen("--idle-timeout=") */
    MAIN_MAX_PORT = 65536,
    PARENT_WATCHDOG_STACK_SIZE = 64 * CBM_SZ_1K, /* watchdog only polls — tiny stack suffices */
};
#define SLEN(s) (sizeof(s) - 1)
#include "foundation/log.h"
#include "foundation/diagnostics.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/mem.h"
#include "foundation/profile.h"
#include "foundation/win_utf8.h" /* cbm_wide_to_utf8 — Windows UTF-8 argv (#423/#20); no-op on POSIX */
#ifdef _WIN32
#include <shellapi.h> /* CommandLineToArgvW — not pulled in by windows.h under WIN32_LEAN_AND_MEAN */
#endif
#include "ui/config.h"
#include "ui/http_server.h"
#include "ui/embedded_assets.h"
#include <yyjson/yyjson.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <stdatomic.h>
#ifndef _WIN32
#include <unistd.h>
#endif

#ifndef CBM_VERSION
#define CBM_VERSION "dev"
#endif

/* ── Globals for signal handling ────────────────────────────────── */

static cbm_watcher_t *g_watcher = NULL;
static cbm_mcp_server_t *g_server = NULL;
static cbm_http_server_t *g_http_server = NULL;
static atomic_int g_shutdown = 0;

/* Idempotent shutdown: cancels the active pipeline, stops background servers,
 * and closes stdin to unblock the MCP read loop. Invoked from the signal
 * handler and from the parent-death watchdog, hence the atomic_exchange guard
 * so the body runs at most once. Body is async-signal-safe (only atomic stores
 * and stop calls that themselves only set atomics). */
static void request_shutdown(void) {
    if (atomic_exchange(&g_shutdown, 1)) {
        return; /* already shutting down */
    }

    /* Cancel any in-progress pipeline (async-signal-safe: only does atomic_store) */
    if (g_server) {
        cbm_pipeline_t *p = cbm_mcp_server_active_pipeline(g_server);
        if (p) {
            cbm_pipeline_cancel(p);
        }
    }
    /* Release pipeline lock to prevent stale lock on restart */
    cbm_pipeline_unlock();

    if (g_watcher) {
        cbm_watcher_stop(g_watcher);
    }
    if (g_http_server) {
        cbm_http_server_stop(g_http_server);
    }
    /* Close stdin to unblock getline in the MCP server loop */
    (void)fclose(stdin);
}

static void signal_handler(int sig) {
    (void)sig;
    request_shutdown();
}

/* ── Parent-process watchdog ────────────────────────────────────── */
/* parent-death watchdog — distilled from #407 (fixes #406, thanks @nvt-pankajsharma).
 *
 * When this stdio MCP server is launched by an agent that later dies without a
 * clean SIGTERM (e.g. the editor is force-killed), the orphaned server would
 * otherwise linger forever blocked on stdin. POSIX has no portable "notify on
 * parent death" primitive (PR_SET_PDEATHSIG is Linux-only), so we poll getppid:
 * once the parent dies the process is reparented (ppid changes, typically to 1)
 * and we shut down. Windows is unaffected (job objects handle this) — #ifndef. */

#ifndef _WIN32
static void *parent_watchdog_thread(void *arg) {
    pid_t initial_ppid = *(pid_t *)arg;
    const unsigned int poll_interval_us = 500000; /* 500ms */

    while (!atomic_load(&g_shutdown)) {
        cbm_usleep(poll_interval_us);
        if (atomic_load(&g_shutdown)) {
            break;
        }
        /* initial_ppid > 1 guards against an already-orphaned start (ppid==1),
         * where a changing ppid carries no signal. */
        if (initial_ppid > 1 && getppid() != initial_ppid) {
            static const char msg[] = "level=warn msg=parent.exited reason=ppid_changed\n";
            (void)write(STDERR_FILENO, msg, sizeof(msg) - 1);
            _exit(0);
        }
    }
    return NULL;
}
#endif

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

    /* Skip indexing if shutdown is in progress (skipped, not indexed) */
    if (atomic_load(&g_shutdown)) {
        return 1;
    }

    /* Non-blocking: skip if another pipeline is already running. The
     * positive return keeps the watcher's baselines uncommitted so the
     * change is retried on the next poll cycle (5-60s) instead of being
     * recorded as seen and lost (#937). */
    if (!cbm_pipeline_try_lock()) {
        cbm_log_info("watcher.skip", "project", project_name, "reason", "pipeline_busy");
        return 1;
    }

    cbm_log_info("watcher.reindex", "project", project_name, "path", root_path);

    /* #832: route the re-index through the supervised worker subprocess so this
     * long-lived server process hands its RSS back to the OS on every cycle
     * instead of ratcheting (mimalloc v3 does not reclaim pages that worker
     * threads abandon at exit). The child writes the DB; the parent only needs the
     * return code. The pipeline lock (already held) still serialises re-indexes.
     * Degrade to the in-process pipeline when the supervisor is off (kill switch)
     * or the spawn fails. */
    if (cbm_index_supervisor_should_wrap()) {
        char *resp = cbm_mcp_index_run_supervised_path(root_path);
        if (resp) {
            free(resp);
            cbm_pipeline_unlock();
            return 0;
        }
        /* resp == NULL → spawn-failure degrade → fall through to in-process. */
    }

    cbm_pipeline_t *p = cbm_pipeline_new(root_path, NULL, CBM_MODE_FULL);
    if (!p) {
        cbm_pipeline_unlock();
        return CBM_NOT_FOUND;
    }

    int rc = cbm_pipeline_run(p);
    cbm_pipeline_free(p);
    cbm_pipeline_unlock();
    return rc;
}

/* ── CLI mode ───────────────────────────────────────────────────── */

#define CLI_USAGE "Usage: codebase-memory-mcp cli [--progress] [--json] <tool_name> [json_args]\n"

/* Extract text content from MCP tool result envelope and print it.
 * MCP results: {"content":[{"type":"text","text":"..."}],"isError":...}
 * Returns 1 if the result was an error, 0 otherwise. */
static int cli_print_mcp_result(const char *result) {
    yyjson_doc *doc = yyjson_read(result, strlen(result), 0);
    if (!doc) {
        printf("%s\n", result);
        return 0;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *err_val = yyjson_obj_get(root, "isError");
    bool is_error = err_val && yyjson_get_bool(err_val);

    const char *text = NULL;
    yyjson_val *content = yyjson_obj_get(root, "content");
    if (yyjson_is_arr(content) && yyjson_arr_size(content) > 0) {
        yyjson_val *tv = yyjson_obj_get(yyjson_arr_get_first(content), "text");
        text = tv ? yyjson_get_str(tv) : NULL;
    }

    if (text) {
        (void)fprintf(is_error ? stderr : stdout, "%s\n", text);
    } else {
        printf("%s\n", result);
    }

    yyjson_doc_free(doc);
    return is_error ? SKIP_ONE : 0;
}

/* Strip a flag from argv, returning true if found. */
static bool cli_strip_flag(int *argc, char **argv, const char *flag) {
    for (int i = 0; i < *argc; i++) {
        if (strcmp(argv[i], flag) != 0) {
            continue;
        }
        for (int j = i; j < *argc - SKIP_ONE; j++) {
            argv[j] = argv[j + SKIP_ONE];
        }
        (*argc)--;
        return true;
    }
    return false;
}

/* Strip a flag AND its following value from argv, returning the value (a pointer
 * into the original argv strings, valid for the process lifetime) or NULL if the
 * flag is absent. */
static const char *cli_strip_flag_value(int *argc, char **argv, const char *flag) {
    for (int i = 0; i < *argc; i++) {
        if (strcmp(argv[i], flag) != 0) {
            continue;
        }
        const char *value = (i + SKIP_ONE < *argc) ? argv[i + SKIP_ONE] : NULL;
        int remove_count = value ? 2 : 1;
        for (int j = i; j < *argc - remove_count; j++) {
            argv[j] = argv[j + remove_count];
        }
        *argc -= remove_count;
        return value;
    }
    return NULL;
}

/* Portable "is fd a terminal?" — _isatty on Windows, isatty on POSIX. */
#ifdef _WIN32
#define cli_isatty(fd) _isatty(fd)
#else
#define cli_isatty(fd) isatty(fd)
#endif

enum { CLI_SLURP_CHUNK = 4096 };

/* Read an open stream fully into a heap, NUL-terminated string. Caller frees.
 * Returns NULL on allocation failure. Reads binary-clean (UTF-8 JSON, no shell
 * quoting needed). */
static char *cli_slurp_stream(FILE *f) {
    size_t cap = CLI_SLURP_CHUNK;
    size_t len = 0;
    char *buf = malloc(cap);
    if (!buf) {
        return NULL;
    }
    char tmp[CLI_SLURP_CHUNK];
    size_t n;
    while ((n = fread(tmp, 1, sizeof(tmp), f)) > 0) {
        if (len + n + 1 > cap) {
            while (len + n + 1 > cap) {
                cap *= 2;
            }
            char *nb = realloc(buf, cap);
            if (!nb) {
                free(buf);
                return NULL;
            }
            buf = nb;
        }
        memcpy(buf + len, tmp, n);
        len += n;
    }
    buf[len] = '\0';
    return buf;
}

/* Slurp a file path into a heap, NUL-terminated string. Caller frees. */
static char *cli_slurp_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    char *s = cli_slurp_stream(f);
    (void)fclose(f);
    return s;
}

/* True if the first non-whitespace byte of s is '{' (raw-JSON detection). */
static bool cli_first_nonspace_is_brace(const char *s) {
    while (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r') {
        s++;
    }
    return *s == '{';
}

static int run_cli(int argc, char **argv) {
    if (argc < MAIN_MIN_ARGC) {
        (void)fprintf(stderr, CLI_USAGE);
        return SKIP_ONE;
    }

    bool progress = cli_strip_flag(&argc, argv, "--progress");
    bool raw_json = cli_strip_flag(&argc, argv, "--json");

    /* Supervisor worker role: when this process was spawned as a supervised index
     * worker, run indexing in-process (never re-supervise) and write the result to
     * the given file for the parent to read back. Stripped here so the tool
     * dispatch below sees only the tool name + its args. */
    bool index_worker = cli_strip_flag(&argc, argv, "--index-worker");
    const char *response_out = cli_strip_flag_value(&argc, argv, "--response-out");
    cbm_index_set_worker_role(index_worker, response_out);

#ifndef _WIN32
    /* #845: a supervised worker must not outlive its supervisor. If the parent
     * dies without reaping us (agent killed, supervisor crashed), an orphaned
     * worker would index on unsupervised — observed contributing to memory
     * pressure during the 2026-07-04 host panics. Reuse the parent-death
     * watchdog (safe outside server mode: on ppid change it only writes to
     * stderr and _exit(0)s — no cleanup dependencies). Detached: the worker
     * exits by returning from run_cli; exit() tears the thread down. Failure
     * to start is non-fatal, same policy as the MCP-server watchdog. */
    if (index_worker) {
        static pid_t worker_initial_ppid; /* static: outlives run_cli for the thread */
        worker_initial_ppid = getppid();
        cbm_thread_t worker_watchdog_tid;
        if (cbm_thread_create(&worker_watchdog_tid, PARENT_WATCHDOG_STACK_SIZE,
                              parent_watchdog_thread, &worker_initial_ppid) == 0) {
            (void)cbm_thread_detach(&worker_watchdog_tid);
            cbm_log_info("worker.watchdog.start");
        } else {
            cbm_log_warn("worker.watchdog.unavailable", "reason", "thread_create_failed");
        }
    }
#endif

    if (argc < MAIN_MIN_ARGC) {
        (void)fprintf(stderr, CLI_USAGE);
        return SKIP_ONE;
    }

    const char *tool_name = argv[0];
    int rem_argc = argc - SKIP_ONE; /* args following the tool name */
    char **rem_argv = argv + SKIP_ONE;

    /* --help / -h : print per-tool help (from the tool's input_schema) and exit
     * before any server work. */
    for (int i = 0; i < rem_argc; i++) {
        if (strcmp(rem_argv[i], "--help") == 0 || strcmp(rem_argv[i], "-h") == 0) {
            if (cbm_cli_print_tool_help(tool_name) != 0) {
                (void)fprintf(stderr, "error: unknown tool '%s'\n", tool_name);
                return SKIP_ONE;
            }
            return 0;
        }
    }

    /* Resolve the JSON arguments. Precedence: --args-file, then raw JSON
     * (back-compat), then --flags, then piped stdin, then empty {}. */
    char *heap_args = NULL; /* freed before return when set */
    const char *args_json = "{}";

    int args_file_idx = -1;
    for (int i = 0; i < rem_argc; i++) {
        if (strcmp(rem_argv[i], "--args-file") == 0) {
            args_file_idx = i;
            break;
        }
    }

    if (args_file_idx >= 0) {
        if (args_file_idx + SKIP_ONE >= rem_argc) {
            (void)fprintf(stderr, "error: --args-file requires a path argument\n");
            return SKIP_ONE;
        }
        const char *path = rem_argv[args_file_idx + SKIP_ONE];
        heap_args = cli_slurp_file(path);
        if (!heap_args) {
            (void)fprintf(stderr, "error: cannot read args file '%s'\n", path);
            return SKIP_ONE;
        }
        args_json = heap_args;
    } else if (rem_argc >= SKIP_ONE && cli_first_nonspace_is_brace(rem_argv[0])) {
        /* raw-JSON back-compat: cli <tool> '{"k":"v"}' (deprecated path). Warn on
         * STDERR only — stdout must stay clean JSON for piping. */
        (void)fprintf(stderr,
                      "warning: passing raw JSON to 'cli %s' is deprecated and "
                      "will be removed in a future release; use flags (run 'cli "
                      "%s --help'), --args-file <path>, or piped stdin.\n",
                      tool_name, tool_name);
        args_json = rem_argv[0];
    } else if (rem_argc >= SKIP_ONE && strncmp(rem_argv[0], "--", 2) == 0) {
        /* flag form: cli <tool> --flag value --bare-bool ... */
        char *err = NULL;
        heap_args = cbm_cli_build_args_json(tool_name, rem_argc, rem_argv, &err);
        if (!heap_args) {
            (void)fprintf(stderr, "error: %s\n", err ? err : "invalid arguments");
            free(err);
            return SKIP_ONE;
        }
        args_json = heap_args;
    } else if (!cli_isatty(0)) {
        /* piped stdin (UTF-8 clean, no shell quoting): cli <tool> < args.json */
        heap_args = cli_slurp_stream(stdin);
        if (heap_args && heap_args[0]) {
            args_json = heap_args;
        } else {
            free(heap_args);
            heap_args = NULL;
            args_json = "{}";
        }
    }

    if (progress) {
        cbm_progress_sink_init(stderr);
    }

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        (void)fprintf(stderr, "error: failed to create server\n");
        if (progress) {
            cbm_progress_sink_fini();
        }
        return SKIP_ONE;
    }

    char *result = cbm_mcp_handle_tool(srv, tool_name, args_json);
    int exit_code = 0;

    if (result) {
        /* Supervised worker: hand the full result string to the parent via the
         * response file before printing (parent reads it back on a clean exit). */
        const char *ro = cbm_index_worker_response_out();
        if (ro) {
            FILE *rf = cbm_fopen(ro, "wb");
            if (rf) {
                (void)fputs(result, rf);
                (void)fclose(rf);
            }
        }
        if (raw_json) {
            printf("%s\n", result);
        } else {
            exit_code = cli_print_mcp_result(result);
        }
        if (cbm_index_worker_active()) {
            /* Supervised worker: the response is delivered (file + stdout).
             * Skip the multi-GB teardown (server/store frees) — the process
             * dies now and the OS reclaims everything wholesale; piecemeal
             * free() of a kernel-scale graph costs minutes. _Exit skips
             * atexit/LSan by design for this prod worker path. */
            cbm_log_info("index.worker.fast_exit", "action", "_Exit");
            fflush(NULL);
            _Exit(exit_code);
        }
        free(result);
    }

    cbm_mcp_server_free(srv);
    if (progress) {
        cbm_progress_sink_fini();
    }
    free(heap_args);
    return exit_code;
}

/* ── Help ───────────────────────────────────────────────────────── */

static void print_help(void) {
    printf("codebase-memory-mcp %s\n\n", CBM_VERSION);
    printf("Usage:\n");
    printf("  codebase-memory-mcp              Run MCP server on stdio\n");
    printf("  codebase-memory-mcp cli <tool> [json]  Run a single tool\n");
    printf("  codebase-memory-mcp install [-y|-n] [--force] [--dry-run] [--skill-mode=cli|mcp]\n");
    printf("  codebase-memory-mcp uninstall [-y|-n] [--dry-run]\n");
    printf("  codebase-memory-mcp update [-y|-n]\n");
    printf("  codebase-memory-mcp config <list|get|set|reset>\n");
    printf("  codebase-memory-mcp --version    Print version\n");
    printf("  codebase-memory-mcp --help       Print this help\n");
    printf("\nUI options:\n");
    printf("  --ui=true    Enable HTTP graph visualization (persisted)\n");
    printf("  --ui=false   Disable HTTP graph visualization (persisted)\n");
    printf("  --port=N     Set UI port (default 9749, persisted)\n");
    printf("  --tool-profile=analysis|scout  Expose a restricted inspection surface\n");
    printf("  --idle-timeout=N  Exit after N seconds without MCP requests\n");
    printf("\nInstall modes:\n");
    printf("  --skill-mode=mcp  Install MCP config, Claude Code skill, and supported hooks (default)\n");
    printf("  --skill-mode=cli  Install only the Claude Code CLI skill and skip MCP configs/hooks\n");
    printf("  Claude skill path: ~/.claude/skills/codebase-memory/SKILL.md\n");
    printf("  Shared skill path for supported clients: ~/.agents/skills/codebase-memory/SKILL.md\n");
    printf("\nSupported automatic/conditional client surfaces (43):\n");
    printf("  Claude Code, Codex CLI, Gemini CLI, Zed, OpenCode,\n");
    printf("  Antigravity, Aider, KiloCode, VS Code, Cursor, Windsurf,\n");
    printf("  Augment / Auggie, OpenClaw, Kiro, Junie, Hermes, OpenHands,\n");
    printf("  Cline, Warp, Qwen Code, GitHub Copilot CLI, Factory Droid, Crush,\n");
    printf("  Goose, Mistral Vibe, Qoder CLI, Kimi Code CLI, GitLab Duo CLI,\n");
    printf("  Rovo Dev CLI, Amp, Devin CLI / Local, Tabnine, Continue / cn,\n");
    printf("  Visual Studio, TRAE, Roo Code, Amazon Q Developer IDE,\n");
    printf("  CodeBuddy Code CLI, IBM Bob IDE, IBM Bob Shell, Pochi, Pi,\n");
    printf("  Sourcegraph Cody\n");
    printf("  Conditional/explicit targets are changed only when their documented\n");
    printf("  platform, marker, or explicit existing config path is present.\n");
    printf("  Manual/UI MCP boundaries: Qodo, Warp, JetBrains AI/ACP, Replit,\n");
    printf("  Plandex, SWE-agent, BLACKBOX, GitHub cloud agents, Jules,\n");
    printf("  CodeRabbit.\n");
    printf("\nTools: index_repository, search_graph, query_graph, trace_path,\n");
    printf("  get_code_snippet, get_graph_schema, get_architecture, search_code,\n");
    printf("  list_projects, delete_project, index_status, detect_changes,\n");
    printf("  check_index_coverage, manage_adr, ingest_traces\n");
}

/* ── Main ───────────────────────────────────────────────────────── */

/* Try to handle a subcommand (cli/install/uninstall/update/config/--version/--help).
 * Returns -1 if no subcommand matched, otherwise the exit code. */
static int handle_subcommand(int argc, char **argv) {
    /* First scan: global flags */
    for (int i = SKIP_ONE; i < argc; i++) {
        if (strcmp(argv[i], "--profile") == 0) {
            cbm_profile_enable();
        }
    }
    for (int i = SKIP_ONE; i < argc; i++) {
        if (strcmp(argv[i], "--version") == 0) {
            printf("codebase-memory-mcp %s\n", CBM_VERSION);
            return 0;
        }
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_help();
            return 0;
        }
        if (strcmp(argv[i], "cli") == 0) {
            cbm_mem_init(cbm_mem_ram_fraction_for_total(cbm_system_info().total_ram));
            return run_cli(argc - i - SKIP_ONE, argv + i + SKIP_ONE);
        }
        if (strcmp(argv[i], "hook-augment") == 0) {
            cbm_mem_init(cbm_mem_ram_fraction_for_total(cbm_system_info().total_ram));
            return cbm_cmd_hook_augment(argc - i - SKIP_ONE, argv + i + SKIP_ONE);
        }
        if (strcmp(argv[i], "install") == 0) {
            return cbm_cmd_install(argc - i - SKIP_ONE, argv + i + SKIP_ONE);
        }
        if (strcmp(argv[i], "uninstall") == 0) {
            return cbm_cmd_uninstall(argc - i - SKIP_ONE, argv + i + SKIP_ONE);
        }
        if (strcmp(argv[i], "update") == 0) {
            return cbm_cmd_update(argc - i - SKIP_ONE, argv + i + SKIP_ONE);
        }
        if (strcmp(argv[i], "config") == 0) {
            return cbm_cmd_config(argc - i - SKIP_ONE, argv + i + SKIP_ONE);
        }
    }
    return CBM_NOT_FOUND;
}

/* Parse --ui= and --port= flags. Returns true if config was modified. */
static bool parse_ui_flags(int argc, char **argv, cbm_ui_config_t *cfg, bool *explicit_enable) {
    bool changed = false;
    for (int i = SKIP_ONE; i < argc; i++) {
        if (strncmp(argv[i], "--ui=", SLEN("--ui=")) == 0) {
            cfg->ui_enabled = (strcmp(argv[i] + MAIN_FLAG_OFF, "true") == 0);
            if (explicit_enable && cfg->ui_enabled) {
                *explicit_enable = true;
            }
            changed = true;
        }
        if (strncmp(argv[i], "--port=", SLEN("--port=")) == 0) {
            int p = (int)strtol(argv[i] + MAIN_PORT_OFF, NULL, CBM_DECIMAL_BASE);
            if (p > 0 && p < MAIN_MAX_PORT) {
                cfg->ui_port = p;
                changed = true;
            }
        }
    }
    return changed;
}

/* Optional whole-server idle timeout. Default 0 keeps permanent MCP sessions.
 * CBM_IDLE_TIMEOUT_S supports wrappers that launch a stdio server from skills
 * and want it to self-exit after intermittent requests stop. */
static int parse_idle_timeout(int argc, char **argv) {
    int timeout_s = 0;
    char env_buf[128];
    const char *env = cbm_safe_getenv("CBM_IDLE_TIMEOUT_S", env_buf, sizeof(env_buf), NULL);
    if (env && env[0]) {
        long v = strtol(env, NULL, CBM_DECIMAL_BASE);
        if (v > 0 && v < MAIN_MAX_PORT) {
            timeout_s = (int)v;
        }
    }
    for (int i = SKIP_ONE; i < argc; i++) {
        if (strncmp(argv[i], "--idle-timeout=", SLEN("--idle-timeout=")) == 0) {
            long v = strtol(argv[i] + MAIN_IDLE_TIMEOUT_OFF, NULL, CBM_DECIMAL_BASE);
            if (v > 0 && v < MAIN_MAX_PORT) {
                timeout_s = (int)v;
            }
        }
    }
    return timeout_s;
}

/* Install platform-specific signal handlers. */
static void setup_signal_handlers(void) {
#ifdef _WIN32
    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);
#else
    struct sigaction sa = {0};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
#endif
}

#ifdef _WIN32
/* On Windows the CRT hands main() an argv encoded in the active ANSI code page, so a
 * non-ASCII CLI argument (e.g. a repo path like café_日本語_repo) is mangled before the
 * program ever sees it — the documented `cli index_repository "<json>"` then fails with
 * "repo_path is required" (#423/#20). Rebuild argv from the wide command line
 * (GetCommandLineW → CommandLineToArgvW) and convert each element to UTF-8 so the rest
 * of the program receives the same UTF-8 bytes it gets on POSIX. Returns a
 * NULL-terminated argv and sets *out_argc, or NULL on any failure (caller then keeps
 * the original narrow argv). The returned block lives for the whole process (argv must
 * stay valid until exit), so it is intentionally never freed. */
static char **cbm_win_utf8_argv(int *out_argc) {
    int wargc = 0;
    LPWSTR *wargv = CommandLineToArgvW(GetCommandLineW(), &wargc);
    if (!wargv) {
        return NULL;
    }
    if (wargc <= 0) {
        LocalFree(wargv);
        return NULL;
    }
    char **u8argv = (char **)calloc((size_t)wargc + 1, sizeof(char *));
    if (!u8argv) {
        LocalFree(wargv);
        return NULL;
    }
    for (int i = 0; i < wargc; i++) {
        u8argv[i] = cbm_wide_to_utf8(wargv[i]);
        if (!u8argv[i]) {
            for (int j = 0; j < i; j++) {
                free(u8argv[j]);
            }
            free(u8argv);
            LocalFree(wargv);
            return NULL;
        }
    }
    LocalFree(wargv);
    *out_argc = wargc;
    return u8argv; /* NULL-terminated (calloc'd wargc+1) */
}
#endif /* _WIN32 */

int main(int argc, char **argv) {
    /* Defense-in-depth: bind tree-sitter and sqlite3 to mimalloc so a
     * correct binary does not rely on the fragile MI_OVERRIDE symbol override
     * (#424). MUST be the VERY FIRST statement: SQLITE_CONFIG_MALLOC has to run
     * before the first sqlite3_open* (cbm_mcp_server_new → cbm_store_open_memory
     * below opens sqlite early), else sqlite3_config returns SQLITE_MISUSE and
     * the bind is silently ignored. No-op in the test build. */
    cbm_alloc_init();
#ifdef _WIN32
    /* Replace the ANSI-code-page argv the CRT handed us with a UTF-8 argv rebuilt from
     * the wide command line, so non-ASCII CLI arguments survive (#423/#20). Falls back
     * to the original argv if the wide rebuild fails. Done after cbm_alloc_init (which
     * must stay the very first statement) but before argv is first read below. */
    {
        int win_argc = 0;
        char **win_argv = cbm_win_utf8_argv(&win_argc);
        if (win_argv) {
            argc = win_argc;
            argv = win_argv;
        }
    }
#endif
    /* #845: mark this process as the REAL binary so the index supervisor may
     * wrap index_repository in a worker subprocess. Must run before any
     * subcommand dispatch so MCP-server, CLI, and HTTP paths are all covered.
     * Embedders of cbm_mcp_handle_tool (test binaries) never mark themselves,
     * so they index in-process instead of re-invoking themselves as
     * `<self> cli --index-worker …` (recursive suite re-runs / spawn chains). */
    cbm_index_supervisor_mark_host();
    cbm_cli_set_version(CBM_VERSION);
    cbm_profile_init(); /* reads CBM_PROFILE env var, gates all prof macros */
    /* CBM_LOG_LEVEL support — distilled from #414 (closes #413). Apply before
     * the first log statement so the configured level governs all output. */
    cbm_log_init_from_env();
    int subcmd = handle_subcommand(argc, argv);
    if (subcmd >= 0) {
        return subcmd;
    }
    cbm_mcp_tool_profile_t tool_profile = CBM_MCP_TOOL_PROFILE_ALL;
    if (cbm_mcp_parse_tool_profile_args(argc, (const char *const *)argv, &tool_profile) != 0) {
        (void)fprintf(stderr, "codebase-memory-mcp: --tool-profile requires the supported value "
                              "'analysis' or 'scout'\n");
        return 2;
    }
    bool restricted_tool_profile = tool_profile != CBM_MCP_TOOL_PROFILE_ALL;

    /* parent-death watchdog — distilled from #407 (fixes #406). Start it early so
     * an orphaned server exits even if it dies before reaching the MCP loop. A
     * thread-create failure (or ppid<=1) is non-fatal: the server still runs, it
     * just won't auto-exit on parent death — same policy as the watcher/HTTP
     * threads below. We deliberately do NOT exit at startup when ppid<=1 (the PR's
     * original behaviour): a legitimately-launched server can transiently show
     * ppid==1 (early reparent races, double-fork/container launchers), and the
     * watchdog already no-ops safely in that case via its initial_ppid>1 guard. */
#ifndef _WIN32
    /* main() outlives the watchdog (it joins before returning), so a stack
     * local is a valid lifetime for the thread's argument. */
    pid_t initial_ppid = getppid();
    cbm_thread_t parent_watchdog_tid;
    bool parent_watchdog_started = false;
    if (cbm_thread_create(&parent_watchdog_tid, PARENT_WATCHDOG_STACK_SIZE, parent_watchdog_thread,
                          &initial_ppid) == 0) {
        parent_watchdog_started = true;
    } else {
        cbm_log_warn("parent.watchdog.unavailable", "reason", "thread_create_failed");
    }
#endif

    /* Default: MCP server on stdio */
    cbm_mem_init(cbm_mem_ram_fraction_for_total(cbm_system_info().total_ram));
    /* Store binary path for subprocess spawning + hook log sink */
    cbm_http_server_set_binary_path(argv[0]);
    cbm_log_set_sink_ex(cbm_ui_log_append, CBM_LOG_SINK_TEE);
    cbm_log_info("server.start", "version", CBM_VERSION);
    cbm_diag_start(); /* starts if CBM_DIAGNOSTICS=1 */

    /* Parse --ui and --port flags (persisted config) */
    cbm_ui_config_t ui_cfg;
    cbm_ui_config_load(&ui_cfg);
    bool explicit_ui_enable = false;
    if (!restricted_tool_profile && parse_ui_flags(argc, argv, &ui_cfg, &explicit_ui_enable)) {
        cbm_ui_config_save(&ui_cfg);
    }
    /* If the user explicitly asked for the UI but this binary has no embedded
     * frontend, the HTTP server can never start (see below). The warning that
     * covers this goes to the log sink, which a user running `--ui=true` on a
     * terminal won't see — so tell them plainly on stderr why nothing happens
     * and which build to use (#350). */
    if (explicit_ui_enable && CBM_EMBEDDED_FILE_COUNT == 0) {
        (void)fprintf(stderr,
                      "codebase-memory-mcp: --ui requested, but this binary was built without the "
                      "embedded UI, so the HTTP server will not start.\n"
                      "Use the UI release asset (codebase-memory-mcp-ui) or rebuild with: "
                      "make -f Makefile.cbm cbm-with-ui\n");
    }

    setup_signal_handlers();

    /* Open config store for runtime settings */
    char config_dir[CBM_SZ_1K];
    const char *cfg_home = cbm_get_home_dir();
    cbm_config_t *runtime_config = NULL;
    if (cfg_home && !restricted_tool_profile) {
        snprintf(config_dir, sizeof(config_dir), "%s", cbm_resolve_cache_dir());
        runtime_config = cbm_config_open(config_dir);
    }

    /* Create MCP server */
    g_server = cbm_mcp_server_new(NULL);
    if (!g_server) {
        cbm_log_error("server.err", "msg", "failed to create server");
        cbm_config_close(runtime_config);
#ifndef _WIN32
        if (parent_watchdog_started) {
            atomic_store(&g_shutdown, 1);
            cbm_thread_join(&parent_watchdog_tid);
        }
#endif
        return SKIP_ONE;
    }
    cbm_mcp_server_set_tool_profile(g_server, tool_profile);

    /* Create and start watcher in background thread */
    /* Initialize log mutex before any threads are created */
    cbm_ui_log_init();

    cbm_store_t *watch_store = NULL;
    if (!restricted_tool_profile) {
        watch_store = cbm_store_open_memory();
        g_watcher = cbm_watcher_new(watch_store, watcher_index_fn, NULL);
        /* Wire watcher + config into MCP server for session auto-index. */
        cbm_mcp_server_set_watcher(g_server, g_watcher);
        cbm_mcp_server_set_config(g_server, runtime_config);
    }
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

    if (cbm_mcp_tool_profile_allows_http(tool_profile) && ui_cfg.ui_enabled &&
        CBM_EMBEDDED_FILE_COUNT > 0) {
        g_http_server = cbm_http_server_new(ui_cfg.ui_port);
        if (g_http_server) {
            cbm_http_server_set_watcher(g_http_server, g_watcher);
            if (cbm_thread_create(&http_tid, 0, http_thread, g_http_server) == 0) {
                http_started = true;
            }
        }
    } else if (cbm_mcp_tool_profile_allows_http(tool_profile) && ui_cfg.ui_enabled &&
               CBM_EMBEDDED_FILE_COUNT == 0) {
        cbm_log_warn("ui.no_assets", "hint", "rebuild with: make -f Makefile.cbm cbm-with-ui");
    }

    /* Run MCP event loop (blocks until EOF or signal) */
    int idle_timeout_s = parse_idle_timeout(argc, argv);
    if (idle_timeout_s > 0) {
        cbm_log_int(CBM_LOG_INFO, "server.idle_timeout.enabled", "seconds", idle_timeout_s);
    }
    int rc = cbm_mcp_server_run_with_idle_timeout(g_server, stdin, stdout, idle_timeout_s);
    atomic_store(&g_shutdown, 1); /* unblock the watchdog poll loop */

    /* Shutdown */
    cbm_log_info("server.shutdown");

#ifndef _WIN32
    if (parent_watchdog_started) {
        cbm_thread_join(&parent_watchdog_tid);
    }
#endif

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
    if (watch_store) {
        cbm_store_close(watch_store);
    }
    cbm_mcp_server_free(g_server);
    cbm_config_close(runtime_config);

    g_watcher = NULL;
    g_server = NULL;
    cbm_diag_stop();

    return rc;
}
