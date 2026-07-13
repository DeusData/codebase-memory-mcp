/*
 * repro_issue570.c -- Reproduce-first case for OPEN bug #570.
 *
 * BUG #570: "Installer adds hooks to both hooks.json and config.toml"
 *   https://github.com/DeusData/codebase-memory-mcp/issues/570
 *
 * TWO FILES WRONGLY WRITTEN (Codex SessionStart hook):
 *   ~/.codex/config.toml   -- always written by cbm_upsert_codex_hooks()
 *   ~/.codex/hooks.json    -- pre-existing JSON hook representation
 *
 * ROOT CAUSE (src/cli/cli.c, install_cli_agent_configs, ~line 3116-3130):
 *   The Codex install path unconditionally passes config.toml as the hook
 *   target to cbm_upsert_codex_hooks():
 *
 *     snprintf(cp, sizeof(cp), "%s/.codex/config.toml", home);
 *     ...
 *     cbm_upsert_codex_hooks(cp);
 *
 *   It never checks whether ~/.codex/hooks.json already exists.  When a user
 *   has configured Codex via hooks.json (the JSON representation), the
 *   installer still writes the SessionStart hook into config.toml, causing
 *   Codex to warn about loading hooks from both representations simultaneously.
 *
 *   The same blind write is reflected in the install plan path (~line 3123):
 *
 *     if (g_install_plan)
 *         plan_record("Codex CLI", "hook", cp);  -- cp is always config.toml
 *
 *   So cbm_build_install_plan_json() always lists config.toml as the Codex
 *   hook target, even when hooks.json is already in use.
 *
 * EXPECTED vs ACTUAL (oracle: cbm_build_install_plan_json plan JSON):
 *   Scenario: ~/.codex/ exists AND ~/.codex/hooks.json exists.
 *
 *   Expected: hooks_planned for Codex CLI lists ~/.codex/hooks.json as the
 *             hook target (the representation already in use).  config.toml
 *             may still appear as an mcp_config target, but NOT as a hook.
 *   Actual:   hooks_planned lists ~/.codex/config.toml -- the wrong file --
 *             even though hooks.json is present.  The test asserts the correct
 *             single-target behavior, so it is RED on unpatched code.
 *
 * WHY RED:
 *   The PRIMARY assertion below checks that the plan does NOT list
 *   config.toml as a hook target for Codex.  On current code the plan
 *   always records "hook" -> config.toml regardless of hooks.json, so the
 *   assertion ASSERT_NULL(strstr(json, "\"hook\"")) combined with the check
 *   that config.toml appears ONLY as a config path (not a hook) fails.
 *
 *   Concretely: the JSON will contain a hooks_planned entry with
 *   "config.toml" in the path field, which the test asserts must NOT be
 *   there.  ASSERT_NULL(config_toml_as_hook) fires -> RED.
 *
 * WHAT MAKES CODEX "DETECTED":
 *   cbm_detect_agents() sets agents.codex = dir_exists("~/.codex").
 *   Creating the directory ~/.codex is sufficient for detection.
 *   Creating ~/.codex/hooks.json in addition signals the JSON representation
 *   is already in use and is the trigger for the correct single-target behavior.
 *
 * FIX LOCATION (after this test is written):
 *   install_cli_agent_configs() in src/cli/cli.c:
 *     - Before choosing the hook target path for Codex, check whether
 *       ~/.codex/hooks.json exists.
 *     - If it does, pass that path to cbm_upsert_codex_session_hooks_json()
 *       (or equivalent JSON-format writer) and update plan_record accordingly.
 *     - Only fall back to config.toml when hooks.json does not exist.
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "test_helpers.h"
#include <cli/cli.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* ── Test ───────────────────────────────────────────────────────────────── */

/*
 * repro_issue570_no_dual_hook_write
 *
 * Setup:
 *   - Temp HOME with ~/.codex/ (makes Codex "detected")
 *   - ~/.codex/hooks.json with a minimal hooks payload (signals JSON in use)
 *
 * Oracle: cbm_build_install_plan_json(home, binary) -- dry-run plan, no writes.
 *
 * Assertion (correct behavior that the bug violates):
 *   The hooks_planned array for Codex CLI must reference hooks.json, NOT
 *   config.toml.  Specifically: the plan JSON must NOT contain a hooks_planned
 *   entry whose "path" contains "config.toml".
 *
 * RED condition on unpatched code:
 *   install_cli_agent_configs() always calls
 *     plan_record("Codex CLI", "hook", "<home>/.codex/config.toml")
 *   so the hooks_planned entry always names config.toml.  The assertion
 *     ASSERT_NULL(config_toml_hook_marker)
 *   fires because we find "config.toml" in the hooks section -> FAIL -> RED.
 *
 * GREEN condition after fix:
 *   The installer detects hooks.json is present, writes the hook there
 *   instead, and the plan lists hooks.json as the hook target.
 *   "config.toml" still appears in config_files_planned (MCP config) but
 *   no longer in hooks_planned -> both assertions pass -> GREEN.
 */
TEST(repro_issue570_no_dual_hook_write) {
    char home[256];
    snprintf(home, sizeof(home), "/tmp/cbm-repro570-XXXXXX");
    if (!cbm_mkdtemp(home))
        FAIL("cbm_mkdtemp failed");

    /* Create ~/.codex/ -- sufficient to make Codex "detected". */
    char codex_dir[512];
    snprintf(codex_dir, sizeof(codex_dir), "%s/.codex", home);
    if (th_mkdir_p(codex_dir) != 0)
        FAIL("failed to create .codex dir");

    /*
     * Create ~/.codex/hooks.json -- signals the JSON hook representation
     * is already in use.  Minimal valid content; the installer should
     * detect this file and choose it as the sole hook target.
     */
    char hooks_json_path[512];
    snprintf(hooks_json_path, sizeof(hooks_json_path), "%s/.codex/hooks.json", home);
    if (th_write_file(hooks_json_path,
                      "{\"hooks\":{\"SessionStart\":[]}}\n") != 0)
        FAIL("failed to create hooks.json");

    /* Build the dry-run install plan -- no files are mutated. */
    char *json = cbm_build_install_plan_json(home, "/usr/local/bin/codebase-memory-mcp");
    ASSERT_NOT_NULL(json);

    /* Sanity: plan must be valid and detect Codex. */
    ASSERT(strstr(json, "agent.install.plan.v1") != NULL);
    ASSERT(strstr(json, "\"codex\"") != NULL);

    /*
     * PRIMARY assertion (RED on unpatched code):
     *
     * The plan must NOT list config.toml as a hook target.  We verify this
     * by searching for the string "config.toml" inside the hooks_planned
     * section of the JSON.
     *
     * To isolate the hooks_planned section we search for the hooks_planned
     * key and then check whether "config.toml" appears after it (before the
     * next top-level array key).  A simpler but robust proxy: the raw text
     * "hooks.json" must appear in the JSON (proving the correct target is
     * listed) while "config.toml" must NOT appear paired with a "hook" kind.
     *
     * We use the plan's text structure: in the serialized plan, each hooks
     * entry is a JSON object {"agent":"Codex CLI","path":"<p>"}.  The path
     * for a hook must end in hooks.json, not config.toml.
     *
     * On buggy code: hooks_planned contains {"agent":"Codex CLI",
     * "path":".../.codex/config.toml"}.  The assertion below that
     * "config.toml" must not appear in the hooks section therefore FAILS.
     *
     * Implementation: locate the hooks_planned array in the output and scan
     * for "config.toml" inside it.
     */
    const char *hooks_section = strstr(json, "\"hooks_planned\"");
    ASSERT_NOT_NULL(hooks_section); /* plan must include this key */

    /*
     * config.toml must NOT appear as a hook-planned path.
     * On buggy code the hooks_planned entry is:
     *   {"agent": "Codex CLI", "path": ".../.codex/config.toml"}
     * which will make strstr(hooks_section, "config.toml") non-NULL -> FAIL.
     *
     * After the fix the hooks_planned entry names hooks.json instead, so
     * "config.toml" does not appear in this section -> PASS.
     */
    const char *config_toml_in_hooks = strstr(hooks_section, "config.toml");
    if (config_toml_in_hooks != NULL) {
        printf("  BUG #570 reproduced: plan lists config.toml as a Codex hook target\n");
        printf("  even though hooks.json already exists.\n");
        printf("  hooks_planned section:\n  %.400s\n", hooks_section);
    }
    ASSERT_NULL(config_toml_in_hooks);

    /*
     * SECONDARY assertion: hooks.json must appear as the hook target.
     * After the fix the plan should list ~/.codex/hooks.json in hooks_planned.
     * This assertion will also be RED on buggy code because the plan never
     * mentions hooks.json at all (it uses config.toml instead).
     */
    const char *hooks_json_in_plan = strstr(hooks_section, "hooks.json");
    if (hooks_json_in_plan == NULL) {
        printf("  BUG #570: plan does not list hooks.json as Codex hook target.\n");
    }
    ASSERT_NOT_NULL(hooks_json_in_plan);

    /*
     * INVARIANT: config.toml must still appear in config_files_planned
     * (that is the correct MCP config target), just not in hooks_planned.
     * This confirms the plan is otherwise intact.
     */
    ASSERT(strstr(json, "config.toml") != NULL);

    free(json);

    /* Building the plan must not have created any actual config files. */
    struct stat st;
    char cfg[512];
    snprintf(cfg, sizeof(cfg), "%s/.codex/config.toml", home);
    ASSERT(stat(cfg, &st) != 0); /* config.toml must NOT have been created */

    th_rmtree(home);
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────────── */
SUITE(repro_issue570) {
    RUN_TEST(repro_issue570_no_dual_hook_write);
}
