/*
 * cli.h — CLI subcommand handlers for install, uninstall, update, version.
 *
 * Port of Go cmd/codebase-memory-mcp/ install/update/config logic.
 *
 * Functions accept explicit paths (home_dir, binary_path) rather than
 * reading HOME internally, making them testable with temp directories.
 */
#ifndef CBM_CLI_H
#define CBM_CLI_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct cbm_mcp_server cbm_mcp_server_t;

/* ── Version ──────────────────────────────────────────────────── */

/* Set the version string (called from main). */
void cbm_cli_set_version(const char *ver);

/* Get the version string. */
const char *cbm_cli_get_version(void);

/* ── CLI tool arguments (flags / --args-file / --help) ────────── */

/* Convert `--flag value` / `--flag=value` / bare-boolean `--flag` arguments for
 * a tool into a JSON arguments object string, using the tool's input_schema to
 * type values (string/integer/boolean) and to collect repeated flags into
 * array-typed properties. kebab-case flags map to snake_case keys
 * (--repo-path -> repo_path). A bare `--` ends flag parsing. On error returns
 * NULL and, if err_out is non-NULL, sets *err_out to a heap message the caller
 * must free. Caller frees the returned JSON string. */
char *cbm_cli_build_args_json(const char *tool_name, int argc, char **argv, char **err_out);

/* Print per-tool help (usage + the tool's flags with type/description/required)
 * derived from its input_schema, to stdout. Returns 0 if the tool is known,
 * non-zero (and prints nothing) if it is not. */
int cbm_cli_print_tool_help(const char *tool_name);

/* Inspect a raw MCP tool-result envelope. Returns true only when the root
 * object carries the exact boolean field `isError: true`; malformed JSON,
 * strings, and nested lookalikes are not tool errors. */
bool cbm_cli_mcp_result_is_error(const char *result);

/* Maintenance cancellation is authoritative even if a tool races to produce
 * a nominal success result. Preserve an existing failure code; otherwise turn
 * accepted cancellation into EXIT_FAILURE. */
int cbm_cli_exit_status_after_maintenance(int exit_status, bool maintenance_cancelled);

/* ── Self-update: version comparison ──────────────────────────── */

/* Compare two semver strings (e.g. "0.2.1" vs "0.2.0").
 * Returns >0 if a > b, <0 if a < b, 0 if equal.
 * Handles v-prefix and -dev suffix. */
int cbm_compare_versions(const char *a, const char *b);

/* ── Shell RC detection ───────────────────────────────────────── */

/* Detect the appropriate shell RC file path for the current user.
 * Uses SHELL env var. home_dir is the user's home directory.
 * Returns static buffer — do NOT free. Returns "" if unknown. */
const char *cbm_detect_shell_rc(const char *home_dir);

/* ── CLI binary detection ─────────────────────────────────────── */

/* Find a CLI binary by name.
 * Checks PATH first, then common install locations.
 * Returns static buffer — do NOT free. Returns "" if not found. */
const char *cbm_find_cli(const char *name, const char *home_dir);

/* ── File utilities ───────────────────────────────────────────── */

/* Copy a file from src to dst. Returns 0 on success, -1 on error. */
int cbm_copy_file(const char *src, const char *dst);

/* Copy the running binary into the canonical install target, preserving the
 * executable bit, skipping the copy when src and dst are the same file (which
 * would otherwise truncate the running binary). Returns 0 on success or skip,
 * -1 on error. Regression surface for the install --force binary-swap bug. */
int cbm_copy_binary_to_target(const char *src, const char *dst);

/* Replace a binary file: unlinks the existing file first (handles read-only),
 * then creates a new file with the given data and permissions.
 * Returns 0 on success, -1 on error. */
int cbm_replace_binary(const char *path, const unsigned char *data, int len, int mode);

/* ── Skill file management ────────────────────────────────────── */

/* Number of skill files. */
#define CBM_SKILL_COUNT 1

/* Skill name/content pair. */
typedef struct {
    const char *name;    /* e.g. "codebase-memory" */
    const char *content; /* full SKILL.md content */
} cbm_skill_t;

/* Get the array of skill definitions. */
const cbm_skill_t *cbm_get_skills(void);

/* Install skills to skills_dir (e.g. ~/.claude/skills/).
 * If force is true, overwrite existing skills.
 * Returns count of skills written. */
int cbm_install_skills(const char *skills_dir, bool force, bool dry_run);

/* Remove skills from skills_dir.
 * Returns count of skills removed. */
int cbm_remove_skills(const char *skills_dir, bool dry_run);

/* Remove old monolithic skill dir if it exists.
 * Returns true if it was removed. */
bool cbm_remove_old_monolithic_skill(const char *skills_dir, bool dry_run);

/* ── Editor MCP config management ─────────────────────────────── */

/* Install MCP server entry in Cursor/Windsurf/Gemini JSON config.
 * Format: { "mcpServers": { "codebase-memory-mcp": { "command": binary_path } } }
 * Preserves existing entries. Returns 0 on success. */
int cbm_install_editor_mcp(const char *binary_path, const char *config_path);

/* Remove MCP server entry from Cursor/Windsurf/Gemini JSON config.
 * Returns 0 on success. */
int cbm_remove_editor_mcp(const char *config_path);
int cbm_remove_editor_mcp_owned(const char *binary_path, const char *config_path);

/* Install MCP server entry in OpenClaw JSON config.
 * Format: { "mcp": { "servers": { "codebase-memory-mcp":
 * { "enabled": true, "command": binary_path, "args": [] } } } }
 * Preserves existing entries. Returns 0 on success. */
int cbm_install_openclaw_mcp(const char *binary_path, const char *config_path);

/* Remove MCP server entry from OpenClaw JSON config.
 * Returns 0 on success. */
int cbm_remove_openclaw_mcp(const char *config_path);
int cbm_remove_openclaw_mcp_owned(const char *binary_path, const char *config_path);

/* Install MCP server entry in VS Code JSON config.
 * Format: { "servers": { "codebase-memory-mcp": { "type": "stdio", "command": binary_path } } }
 * Returns 0 on success. */
int cbm_install_vscode_mcp(const char *binary_path, const char *config_path);

/* Remove MCP server entry from VS Code JSON config.
 * Returns 0 on success. */
int cbm_remove_vscode_mcp(const char *config_path);
int cbm_remove_vscode_mcp_owned(const char *binary_path, const char *config_path);

/* Install MCP server entry in Zed settings.json.
 * Format: { "context_servers": { "codebase-memory-mcp": { "command": path, "args": [] } } }
 * Returns 0 on success. */
int cbm_install_zed_mcp(const char *binary_path, const char *config_path);

/* Remove MCP server entry from Zed settings.json.
 * Returns 0 on success. */
int cbm_remove_zed_mcp(const char *config_path);
int cbm_remove_zed_mcp_owned(const char *binary_path, const char *config_path);

/* ── Agent detection ──────────────────────────────────────────── */

/* Detected coding agents on the system. */
typedef struct {
    bool claude_code;   /* ~/.claude/ exists */
    bool codex;         /* $CODEX_HOME or ~/.codex exists */
    bool gemini;        /* Gemini settings or executable exists */
    bool zed;           /* platform-specific Zed config dir exists */
    bool opencode;      /* opencode on PATH or config exists */
    bool antigravity;   /* Antigravity CLI config or executable exists */
    bool aider;         /* aider on PATH */
    bool kilocode;      /* KiloCode globalStorage dir exists */
    bool vscode;        /* VS Code User config dir exists */
    bool cursor;        /* ~/.cursor/ exists */
    bool windsurf;      /* ~/.codeium/windsurf/ exists */
    bool augment;       /* ~/.augment/ or Auggie CLI exists */
    bool openclaw;      /* ~/.openclaw/ exists */
    bool kiro;          /* ~/.kiro/ exists */
    bool junie;         /* ~/.junie/ exists */
    bool hermes;        /* ~/.hermes/ or hermes CLI exists */
    bool openhands;     /* ~/.openhands/ or openhands CLI exists */
    bool cline;         /* ~/.cline/ or cline CLI exists */
    bool warp;          /* Warp footprint or oz/oz-preview/warp-cli exists */
    bool qwen;          /* ~/.qwen/ or qwen CLI exists */
    bool copilot_cli;   /* $COPILOT_HOME, ~/.copilot/, or copilot CLI exists */
    bool factory_droid; /* ~/.factory/ or droid CLI exists */
    bool crush;         /* Crush config or CLI exists */
    bool goose;         /* Goose config or CLI exists */
    bool mistral_vibe;  /* $VIBE_HOME, ~/.vibe/, or vibe CLI exists */
} cbm_detected_agents_t;

/* Detect which coding agents are installed.
 * Checks config dirs and PATH. home_dir is used for config dir checks. */
cbm_detected_agents_t cbm_detect_agents(const char *home_dir);

/* Install or refresh every detected agent integration below home. */
int cbm_install_agent_configs(const char *home, const char *binary_path, bool force, bool dry_run);

#ifdef CBM_CLI_ENABLE_TEST_API
int cbm_build_qwen_hook_command_for_testing(const char *binary_path, bool windows, char *command,
                                            size_t command_size, char *shell, size_t shell_size);
int cbm_build_qoder_hook_command_for_testing(const char *binary_path, bool windows, char *command,
                                             size_t command_size, char *shell, size_t shell_size);
int cbm_resolve_claude_hook_command_for_testing(const char *script_name, bool windows,
                                                char *command, size_t command_size);
bool cbm_optional_hook_supported_for_testing(const char *agent_name, bool windows);
void cbm_hook_sanitize_metadata_for_testing(const char *input, char *output, size_t output_size);
char *cbm_hook_augment_format_context_for_testing(const char *envelope, const char *token,
                                                  bool *is_error);
int cbm_upsert_qwen_lifecycle_hooks_for_testing(const char *settings_path, const char *binary_path,
                                                bool windows);
int cbm_upsert_qoder_context_hooks_for_testing(const char *settings_path, const char *binary_path);
int cbm_remove_qoder_context_hooks_for_testing(const char *settings_path, const char *binary_path);
/* Explicit lifecycle adapter seam for hook protocols whose output envelope is
 * not Claude/Gemini-compatible. Returns allocated JSON or NULL to fail open. */
char *cbm_hook_augment_lifecycle_json_for_dialect(const char *input, const char *forced_event,
                                                  const char *dialect);
char *cbm_hook_augment_tool_json_for_testing(const char *input, const char *dialect,
                                             const char *context, char *path, size_t path_size);
bool cbm_hook_augment_invocation_supported_for_testing(const char *dialect,
                                                       const char *forced_event);
bool cbm_hook_path_contains_for_testing(const char *root, const char *candidate,
                                        bool case_insensitive);
const char *cbm_hook_no_project_index_guidance_for_testing(const char *event);
#endif

/* ── Agent MCP config upsert (per agent) ──────────────────────── */

/* Codex CLI: upsert MCP entry in $CODEX_HOME/config.toml. Returns 0 on success. */
int cbm_upsert_codex_mcp(const char *binary_path, const char *config_path);

/* Remove CMM MCP entry from Codex config.toml. Returns 0 on success. */
int cbm_remove_codex_mcp(const char *config_path);

/* OpenCode: upsert MCP entry in opencode.json. Returns 0 on success. */
int cbm_upsert_opencode_mcp(const char *binary_path, const char *config_path);

/* Remove CMM MCP entry from opencode.json. Returns 0 on success. */
int cbm_remove_opencode_mcp(const char *config_path);
int cbm_remove_opencode_mcp_owned(const char *binary_path, const char *config_path);

/* Antigravity: upsert MCP entry in ~/.gemini/config/mcp_config.json.
 * Returns 0 on success. */
int cbm_upsert_antigravity_mcp(const char *binary_path, const char *config_path);

/* Remove CMM MCP entry from antigravity mcp_config.json. Returns 0 on success. */
int cbm_remove_antigravity_mcp(const char *config_path);
int cbm_remove_antigravity_mcp_owned(const char *binary_path, const char *config_path);

/* Junie (JetBrains): upsert MCP entry in ~/.junie/mcp/mcp.json (mcpServers format).
 * Returns 0 on success. */
int cbm_upsert_junie_mcp(const char *binary_path, const char *config_path);

/* Remove CMM MCP entry from Junie mcp.json. Returns 0 on success. */
int cbm_remove_junie_mcp(const char *config_path);
int cbm_remove_junie_mcp_owned(const char *binary_path, const char *config_path);

/* ── Instructions file upsert ─────────────────────────────────── */

/* Upsert a codebase-memory-mcp instruction section in a markdown file.
 * Uses <!-- codebase-memory-mcp:start --> / <!-- codebase-memory-mcp:end --> markers.
 * If markers exist, replaces content between them. Otherwise appends.
 * If file doesn't exist, creates it. Returns 0 on success. */
int cbm_upsert_instructions(const char *path, const char *content);

/* Remove the codebase-memory-mcp instruction section from a markdown file.
 * Returns 0 on success, 1 if not found. */
int cbm_remove_instructions(const char *path);

/* Get the shared agent instructions content (markdown). */
const char *cbm_get_agent_instructions(void);

/* #1032: Aider variant — CLI-form discovery commands (Aider has no MCP). */
const char *cbm_get_aider_instructions(void);

/* ── Pre-tool hook management ─────────────────────────────────── */

/* Upsert a PreToolUse hook in ~/.claude/settings.json for Claude Code.
 * Adds a Grep|Glob matcher that reminds to use MCP tools.
 * Returns 0 on success. */
int cbm_upsert_claude_hooks(const char *settings_path);

/* Remove our PreToolUse hook from Claude Code settings.json.
 * Returns 0 on success. */
int cbm_remove_claude_hooks(const char *settings_path);

/* Write the PreToolUse gate shim to <home>/.claude/hooks/. The shim is a thin
 * wrapper that invokes the compiled `hook-augment` and writes to stdout only —
 * it must never create a predictable temp/state file (issue #384). Exposed for
 * testing that security property. */
bool cbm_install_hook_gate_script(const char *home, const char *binary_path);

/* Upsert a BeforeTool hook in ~/.gemini/settings.json for Gemini CLI.
 * Returns 0 on success. */
int cbm_upsert_gemini_hooks(const char *settings_path);

/* Remove our BeforeTool hook from Gemini settings.json.
 * Returns 0 on success. */
int cbm_remove_gemini_hooks(const char *settings_path);

/* Install/remove a SessionStart reminder hook in Codex config.toml (#330) and
 * Gemini settings.json — same methodology as the Claude Code
 * SessionStart hook (non-blocking; stdout injected as session context). */
int cbm_upsert_codex_hooks(const char *config_path);
int cbm_remove_codex_hooks(const char *config_path);
int cbm_upsert_gemini_session_hooks(const char *settings_path);
int cbm_remove_gemini_session_hooks(const char *settings_path);

#ifdef CBM_JSON_LIKE_ENABLE_TEST_API
typedef void (*cbm_hook_json_prewrite_test_hook_t)(const char *settings_path, void *context);
void cbm_set_hook_json_prewrite_hook_for_testing(cbm_hook_json_prewrite_test_hook_t hook,
                                                 void *context);
#endif

/* Install/remove a Claude Code SubagentStart reminder hook in settings.json.
 * Subagents spawned via the Agent tool do not fire SessionStart, so this is the
 * channel that gives them the same code-discovery guidance. Non-blocking; the
 * hook injects context via JSON additionalContext. Returns 0 on success. */
int cbm_upsert_claude_subagent_hooks(const char *settings_path);
int cbm_remove_claude_subagent_hooks(const char *settings_path);

/* ── PATH management ──────────────────────────────────────────── */

/* Append an export PATH line to the given rc file.
 * Checks if already present. Returns 0 on success, 1 if already present. */
int cbm_ensure_path(const char *bin_dir, const char *rc_file, bool dry_run);

/* ── Codex instructions (legacy, wraps cbm_get_agent_instructions) ── */

/* Get the Codex CLI instructions content. */
const char *cbm_get_codex_instructions(void);

/* ── Tar.gz extraction ────────────────────────────────────────── */

/* Extract a binary named "codebase-memory-mcp*" from a tar.gz buffer.
 * Returns malloc'd binary content and sets *out_len.
 * Returns NULL on error. Caller must free. */
unsigned char *cbm_extract_binary_from_targz(const unsigned char *data, int data_len, int *out_len);

/* Extract the codebase-memory-mcp binary from a zip archive in memory.
 * Returns malloc'd binary content and sets *out_len.
 * Returns NULL on error. Caller must free. */
unsigned char *cbm_extract_binary_from_zip(const unsigned char *data, int data_len, int *out_len);

/* Strict two-file Windows release bundle extraction. The archive must contain
 * exactly one root launcher and one root payload; ambiguous aliases,
 * traversal, duplicates, and malformed central/local metadata fail closed. */
typedef struct {
    unsigned char *launcher;
    int launcher_len;
    unsigned char *payload;
    int payload_len;
} cbm_windows_release_pair_t;
bool cbm_extract_windows_release_pair_from_zip(const unsigned char *data, int data_len,
                                               cbm_windows_release_pair_t *pair_out);
void cbm_windows_release_pair_free(cbm_windows_release_pair_t *pair);

/* ── Index management ─────────────────────────────────────────── */

/* List .db files in the cache directory (~/.cache/codebase-memory-mcp/).
 * Prints each file path to stdout. Returns count of .db files found. */
int cbm_list_indexes(const char *home_dir);

/* Remove all .db files in the cache directory. Returns count removed. */
int cbm_remove_indexes(const char *home_dir);

/* ── Config store (persistent key-value, backed by _config.db) ── */

typedef struct cbm_config cbm_config_t;

/* Open the config store in the given cache directory.
 * Creates _config.db if it doesn't exist. Returns NULL on error. */
cbm_config_t *cbm_config_open(const char *cache_dir);

/* Close the config store. */
void cbm_config_close(cbm_config_t *cfg);

/* Get a config value. Returns default_val if key not found. Database-backed
 * results remain valid until the next call on the same thread. */
const char *cbm_config_get(cbm_config_t *cfg, const char *key, const char *default_val);

/* Get a config value as bool. "true"/"1"/"on" → true. */
bool cbm_config_get_bool(cbm_config_t *cfg, const char *key, bool default_val);

/* Get a config value as int. Returns default_val if not found or invalid. */
int cbm_config_get_int(cbm_config_t *cfg, const char *key, int default_val);

/* Set a config value. Returns 0 on success. */
int cbm_config_set(cbm_config_t *cfg, const char *key, const char *value);

/* Delete a config key. Returns 0 on success. */
int cbm_config_delete(cbm_config_t *cfg, const char *key);

/* Well-known config keys */
#define CBM_CONFIG_AUTO_INDEX "auto_index"
#define CBM_CONFIG_AUTO_INDEX_LIMIT "auto_index_limit"
#define CBM_CONFIG_AUTO_WATCH "auto_watch"
#define CBM_CONFIG_UI_LANG "ui-lang"

/* ── Binary activation safety ─────────────────────────────────── */

typedef void *cbm_cli_activation_lock_t;
typedef int (*cbm_cli_activation_mutation_fn)(void *context);

/* High-level mutation reservation seam. Production maps this to the
 * crash-safe version-cohort maintenance barrier, which requests bounded
 * quiescence of active modern CBM processes before returning an exclusive
 * lease. Tests use it to prove that mutation never runs before the cohort has
 * drained. Return 1 with a non-NULL lease on success, 0 when participants did
 * not drain before the deadline, or -1 on an unsafe/IO failure. */
typedef int (*cbm_cli_activation_reserve_fn)(void *context, cbm_cli_activation_lock_t *lease_out);
typedef void (*cbm_cli_activation_release_fn)(void *context, cbm_cli_activation_lock_t lease);

/* Injectable high-level boundary for deterministic drain/mutation ordering
 * tests. Production never acquires startup while asking participants to
 * quiesce: a bootstrap participant may already retain it. Only after lifetime
 * EX proves the cohort drained does production acquire startup, re-probe the
 * daemon generation, and retain startup through mutation. Startup is released
 * before the maintenance lease. */
typedef struct {
    void *context;
    cbm_cli_activation_reserve_fn reserve_for_mutation;
    cbm_cli_activation_release_fn mutation_lease_release;
    void (*visible_diagnostic)(void *context, const char *message);
} cbm_cli_activation_ops_t;

/* Reserve and drain the coordinated cohort, run the mutation only while its
 * lease is retained, then release it on every callback result. A NULL mutation
 * is a guarded no-op. Returns 0 on success, 1 when activation is refused, or
 * the mutation callback's non-zero error. */
int cbm_cli_activation_guard_with_ops(const cbm_cli_activation_ops_t *ops,
                                      cbm_cli_activation_mutation_fn mutation,
                                      void *mutation_context);

/* Internal test seam used only by the C suite to route install/update/uninstall
 * through deterministic callbacks. NULL restores the production daemon IPC path. */
void cbm_cli_set_activation_ops_for_test(const cbm_cli_activation_ops_t *ops);

/* Internal integration-test seam: isolate the stable endpoint beneath a
 * private runtime parent. NULL restores the platform default. This is not a
 * command-line or environment override. */
void cbm_cli_set_activation_runtime_parent_for_test(const char *runtime_parent);

/* Consume and authenticate any inherited permanent-launcher context before
 * process-role classification. An absent context is the normal portable
 * payload case; an advertised but invalid context fails closed. */
int cbm_cli_windows_launcher_startup_authenticate(int argc, char *const argv[]);
/* Internal release-pair probe. Returns -1 when argv does not select the role,
 * otherwise a process exit code. It runs before cache/daemon initialization. */
int cbm_cli_windows_payload_descriptor_role(int argc, char *const argv[]);

/* ── Subcommands (wired from main.c) ─────────────────────────── */

/* install: copy binary, install skills, install editor MCP configs, ensure PATH.
 * Prompts to delete old indexes if any exist — rejects on "no". */
int cbm_cmd_install(int argc, char **argv);

/* uninstall: remove skills, remove editor MCP configs, remove binary. */
int cbm_cmd_uninstall(int argc, char **argv);

/* update: check latest release, prompt for index deletion, prompt for ui/standard,
 * download and replace binary. */
int cbm_cmd_update(int argc, char **argv);

/* config: get/set/list/reset runtime config values. */
int cbm_cmd_config(int argc, char **argv);

/* hook-augment: stdin-driven Claude Code PreToolUse augmenter.
 * Reads the hook JSON from stdin and emits hookSpecificOutput.additionalContext
 * with search_graph hits for Grep/Glob calls. NEVER blocks: every failure
 * path returns 0 with no stdout output. */
int cbm_cmd_hook_augment(int argc, char **argv);

/* Build the documented hookSpecificOutput payload for a SessionStart or
 * SubagentStart input. Returns allocated JSON, or NULL for another/invalid
 * event. Exposed so lifecycle adapters have contract-level regression tests. */
char *cbm_hook_augment_lifecycle_json(const char *input);

/* Variant used by lifecycle adapters whose stdin omits the event. When
 * copilot_dialect is true, emits Copilot CLI's top-level additionalContext. */
char *cbm_hook_augment_lifecycle_json_for(const char *input, const char *forced_event,
                                          bool copilot_dialect);

/* Thin daemon frontend support: preserve the hook's bounded stdin read and
 * hard fail-open deadline without constructing a local MCP/store instance. */
void cbm_hook_augment_arm_deadline(void);
char *cbm_hook_augment_read_stdin(void);

/* Process one already-read hook payload using a caller-owned MCP session.
 * Returns a malloc-owned hook output JSON string, or NULL for fail-open/no
 * augmentation. This is the daemon entry; it never arms a process-global
 * timer and never constructs a second store/session. */
char *cbm_hook_augment_process(cbm_mcp_server_t *srv, const char *input_json);

/* Dialect-aware daemon entry. forced_event and dialect_name are borrowed and
 * may be NULL for the ordinary event dialect. Unsupported combinations fail
 * open with NULL, matching the direct hook command. */
bool cbm_hook_augment_invocation_supported(const char *forced_event, const char *dialect_name);
char *cbm_hook_augment_process_for(cbm_mcp_server_t *srv, const char *input_json,
                                   const char *forced_event, const char *dialect_name);

/* True for an absolute path the augmenter can walk up: POSIX "/..." or a
 * Windows drive root — "X:/..." or a bare "X:" (callers normalize '\\' to '/'
 * first). Exposed for tests — regression coverage for the Windows drive-letter
 * no-op (#618). */
bool cbm_hook_path_is_abs(const char *path);

/* Build the agent.install.plan.v1 install receipt for <home> (issue #388):
 * a machine-readable JSON list of config/instruction/skill/agent/hook files `install`
 * would write, produced WITHOUT mutating anything. Returns a heap JSON string
 * (caller frees) or NULL on error. Exposed for `install --plan` and testing. */
char *cbm_build_install_plan_json(const char *home, const char *binary_path);

#endif /* CBM_CLI_H */
