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

/* Install MCP server entry in OpenClaw JSON config.
 * Format: { "mcp": { "servers": { "codebase-memory-mcp":
 * { "enabled": true, "command": binary_path, "args": [] } } } }
 * Preserves existing entries. Returns 0 on success. */
int cbm_install_openclaw_mcp(const char *binary_path, const char *config_path);

/* Remove MCP server entry from OpenClaw JSON config.
 * Returns 0 on success. */
int cbm_remove_openclaw_mcp(const char *config_path);

/* Install MCP server entry in VS Code JSON config.
 * Format: { "servers": { "codebase-memory-mcp": { "type": "stdio", "command": binary_path } } }
 * Returns 0 on success. */
int cbm_install_vscode_mcp(const char *binary_path, const char *config_path);

/* Remove MCP server entry from VS Code JSON config.
 * Returns 0 on success. */
int cbm_remove_vscode_mcp(const char *config_path);

/* Install MCP server entry in Zed settings.json.
 * Format: { "context_servers": { "codebase-memory-mcp": { "command": path, "args": [""] } } }
 * Returns 0 on success. */
int cbm_install_zed_mcp(const char *binary_path, const char *config_path);

/* Remove MCP server entry from Zed settings.json.
 * Returns 0 on success. */
int cbm_remove_zed_mcp(const char *config_path);

/* ── Agent detection ──────────────────────────────────────────── */

/* Detected coding agents on the system. */
typedef struct {
    bool claude_code; /* ~/.claude/ exists */
    bool codex;       /* ~/.codex/ exists */
    bool gemini;      /* ~/.gemini/ exists */
    bool zed;         /* platform-specific Zed config dir exists */
    bool opencode;    /* opencode on PATH or config exists */
    bool antigravity; /* ~/.gemini/antigravity/ exists */
    bool aider;       /* aider on PATH */
    bool kilocode;    /* KiloCode globalStorage dir exists */
    bool vscode;      /* VS Code User config dir exists */
    bool cursor;      /* ~/.cursor/ exists */
    bool openclaw;    /* ~/.openclaw/ exists */
    bool kiro;        /* ~/.kiro/ exists */
} cbm_detected_agents_t;

/* Detect which coding agents are installed.
 * Checks config dirs and PATH. home_dir is used for config dir checks. */
cbm_detected_agents_t cbm_detect_agents(const char *home_dir);

/* ── Agent MCP config upsert (per agent) ──────────────────────── */

/* Codex CLI: upsert MCP entry in ~/.codex/config.toml. Returns 0 on success. */
int cbm_upsert_codex_mcp(const char *binary_path, const char *config_path);

/* Remove CMM MCP entry from Codex config.toml. Returns 0 on success. */
int cbm_remove_codex_mcp(const char *config_path);

/* OpenCode: upsert MCP entry in opencode.json. Returns 0 on success. */
int cbm_upsert_opencode_mcp(const char *binary_path, const char *config_path);

/* Remove CMM MCP entry from opencode.json. Returns 0 on success. */
int cbm_remove_opencode_mcp(const char *config_path);

/* Antigravity: upsert MCP entry in ~/.gemini/antigravity/mcp_config.json.
 * Returns 0 on success. */
int cbm_upsert_antigravity_mcp(const char *binary_path, const char *config_path);

/* Remove CMM MCP entry from antigravity mcp_config.json. Returns 0 on success. */
int cbm_remove_antigravity_mcp(const char *config_path);

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
void cbm_install_hook_gate_script(const char *home, const char *binary_path);

/* Upsert a BeforeTool hook in ~/.gemini/settings.json for Gemini CLI / Antigravity.
 * Returns 0 on success. */
int cbm_upsert_gemini_hooks(const char *settings_path);

/* Remove our BeforeTool hook from Gemini settings.json.
 * Returns 0 on success. */
int cbm_remove_gemini_hooks(const char *settings_path);

/* Install/remove a SessionStart reminder hook in Codex config.toml (#330) and
 * Gemini/Antigravity settings.json — same methodology as the Claude Code
 * SessionStart hook (non-blocking; stdout injected as session context). */
int cbm_upsert_codex_hooks(const char *config_path);
int cbm_remove_codex_hooks(const char *config_path);
int cbm_upsert_gemini_session_hooks(const char *settings_path);
int cbm_remove_gemini_session_hooks(const char *settings_path);

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

/* Get a config value. Returns default_val if key not found. */
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
#define CBM_CONFIG_UI_LANG "ui-lang"

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
int cbm_cmd_hook_augment(void);

/* True for an absolute path the augmenter can walk up: POSIX "/..." or a
 * Windows drive root — "X:/..." or a bare "X:" (callers normalize '\\' to '/'
 * first). Exposed for tests — regression coverage for the Windows drive-letter
 * no-op (#618). */
bool cbm_hook_path_is_abs(const char *path);

/* Build the agent.install.plan.v1 install receipt for <home> (issue #388):
 * a machine-readable JSON list of the config/instruction/hook files `install`
 * would write, produced WITHOUT mutating anything. Returns a heap JSON string
 * (caller frees) or NULL on error. Exposed for `install --plan` and testing. */
char *cbm_build_install_plan_json(const char *home, const char *binary_path);

#endif /* CBM_CLI_H */
