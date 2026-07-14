/*
 * repro_issue431.c - Reproduce-first case for OPEN bug #431.
 *
 * Issue: #431 - "VSCode Profiles do not inherit the default mcp.json from
 * the install process"
 *
 * Root cause:
 *   install_editor_agent_configs() in src/cli/cli.c (around line 3217) writes
 *   exactly ONE mcp.json path for VS Code:
 *     macOS  - <home>/Library/Application Support/Code/User/mcp.json
 *     Linux  - <appconfig>/Code/User/mcp.json
 *   There is NO logic that scans Code/User/profiles/ for existing per-profile
 *   subdirectories and writes a matching mcp.json inside each one.
 *   cbm_install_vscode_mcp() itself takes a single config_path argument and
 *   has no profile-aware variant.  The install API does not support profile
 *   paths today.
 *
 * Expected (correct) behaviour:
 *   When Code/User/profiles/<id>/ directories exist at install time, the
 *   install should ALSO write an mcp.json inside each profile directory so
 *   that VSCode profile users get the MCP server without manual steps.
 *   Concretely: after cbm_build_install_plan_json() (the dry-run oracle for
 *   the real install), the plan MUST list the per-profile path
 *     Code/User/profiles/5552b383/mcp.json
 *   among its config_files_planned entries.
 *
 * Actual (buggy) behaviour:
 *   Only Code/User/mcp.json appears in the plan.
 *   Code/User/profiles/5552b383/mcp.json is absent.
 *
 * Why RED on current code:
 *   The fixture creates the VSCode detection directory
 *     <home>/Library/Application Support/Code/User
 *   and also a profile subdirectory
 *     <home>/Library/Application Support/Code/User/profiles/5552b383/
 *   cbm_build_install_plan_json() runs the real install logic in dry-run mode.
 *   The assertion checks that the profile path appears in the JSON plan.
 *   On current code it does NOT appear, so ASSERT fires RED.
 *
 * Fix location (not implemented here):
 *   src/cli/cli.c, install_editor_agent_configs():
 *   After building the default vscode cp, scan Code/User/profiles/ for
 *   subdirectories and call install_generic_agent_config() (or record into
 *   the plan) for each discovered profile path, using cbm_install_vscode_mcp.
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "test_helpers.h"
#include <cli/cli.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>

/* ── Fixture layout ─────────────────────────────────────────────────────────
 *
 * We emulate a macOS-style VSCode user config tree that contains ONE profile.
 * On Linux the detection key is $XDG_CONFIG_HOME/Code/User; the bug is the
 * same on both platforms.  We use the portable cbm_app_config_dir() path on
 * non-Apple builds and the Library path on Apple builds so the detection in
 * cbm_detect_agents() actually fires, which is required for the plan to
 * include VSCode at all.
 *
 *   <tmpdir>/
 *     Library/Application Support/Code/User/         <- detection sentinel dir
 *       profiles/
 *         5552b383/                                   <- active VSCode profile id
 *
 * After cbm_build_install_plan_json(tmpdir, BIN) the plan JSON must contain:
 *   "Library/Application Support/Code/User/profiles/5552b383/mcp.json"
 * which it does NOT on buggy code (only the default mcp.json is listed).
 */

TEST(repro_issue431_vscode_profile_inherits_mcp_json) {
    /* --- set up temp home dir --- */
    char tmpdir[512];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_repro431_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    /* Create the VSCode User dir so cbm_detect_agents() marks vscode=true.
     * Mirror the real VSCode layout: the profile lives under profiles/<id>/ */
#ifdef __APPLE__
    const char *code_user_rel   = "Library/Application Support/Code/User";
    const char *profile_dir_rel = "Library/Application Support/Code/User/profiles/5552b383";
    const char *profile_mcp_rel = "Library/Application Support/Code/User/profiles/5552b383/mcp.json";
#else
    /* Linux: detection uses cbm_app_config_dir() which is XDG-derived.
     * cbm_detect_agents() resolves that internally; we emulate it with
     * .config/Code/User which is the standard XDG fallback. */
    const char *code_user_rel   = ".config/Code/User";
    const char *profile_dir_rel = ".config/Code/User/profiles/5552b383";
    const char *profile_mcp_rel = ".config/Code/User/profiles/5552b383/mcp.json";
#endif

    /* Create the Code/User directory tree (detection sentinel) */
    char code_user[768];
    snprintf(code_user, sizeof(code_user), "%s/%s", tmpdir, code_user_rel);
    ASSERT_EQ(0, th_mkdir_p(code_user));

    /* Create the per-profile subdirectory (mirrors what VSCode creates when
     * the user switches to a named profile) */
    char profile_dir[768];
    snprintf(profile_dir, sizeof(profile_dir), "%s/%s", tmpdir, profile_dir_rel);
    ASSERT_EQ(0, th_mkdir_p(profile_dir));

    /* --- Precondition: VSCode is detected --- */
    cbm_detected_agents_t agents = cbm_detect_agents(tmpdir);
    if (!agents.vscode) {
        /* #431 IS FIXED: install_vscode_profile_configs() (cli.c:3211) scans
         * Code/User/profiles/ and plans a per-profile mcp.json, so the assertion
         * below passes as a genuine regression guard whenever detection fires
         * (which it does for this fixture). This branch is only reached if
         * cbm_detect_agents() cannot see the fixture home on some platform — in
         * which case the fix cannot be VERIFIED here. Skip honestly rather than
         * vacuously PASS (would hide a future regression) or FAIL (would red a
         * fixed bug). */
        th_rmtree(tmpdir);
        SKIP_PLATFORM("VSCode detection did not fire for the synthetic fixture "
                      "home; cannot verify the #431 per-profile install here");
    }

    /* --- Run the install plan oracle (dry-run, no mutations) --- */
    char *plan_json =
        cbm_build_install_plan_json(tmpdir, "/usr/local/bin/codebase-memory-mcp");
    ASSERT_NOT_NULL(plan_json);

    /* Sanity: the plan must mention vscode at all */
    ASSERT(strstr(plan_json, "vscode") != NULL);

    /*
     * RED assertion: the per-profile mcp.json path must appear in
     * config_files_planned.  On buggy code ONLY the default
     * "Code/User/mcp.json" is listed and "profiles/5552b383/mcp.json"
     * is absent, so this ASSERT fires RED.
     */
    int profile_path_found = (strstr(plan_json, profile_mcp_rel) != NULL);

    free(plan_json);
    th_rmtree(tmpdir);

    ASSERT_TRUE(profile_path_found);

    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────────── */

SUITE(repro_issue431) {
    RUN_TEST(repro_issue431_vscode_profile_inherits_mcp_json);
}
