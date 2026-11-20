/*
 * repro_issue409.c — Reproduce-first case for OPEN bug #409.
 *
 * Issue #409: "v0.7.0 install/update wires the legacy blocking PreToolUse
 * gate, not hook_augment (regresses #214)"
 *
 * Root cause (as filed):
 *   cbm_install_hook_gate_script wrote the legacy blocking shell gate
 *   (keyed on $PPID, emitting `exit 2` to block tool calls) instead of the
 *   non-blocking augmenter shim that delegates to `<binary> hook-augment`.
 *   On an upgrade from a pre-v0.7.0 install the old gate script remained on
 *   disk (or was rewritten with blocking content), so every Grep/Glob call
 *   was blocked rather than being non-blocking augmented — the exact symptom
 *   of #214 which was supposed to be fixed.
 *
 * Expected (correct) behaviour after cbm_upsert_claude_hooks +
 * cbm_install_hook_gate_script:
 *   1. The gate script written to
 *      <home>/.claude/hooks/cbm-code-discovery-gate
 *      MUST contain "hook-augment" (delegating to the compiled augmenter).
 *   2. The gate script MUST NOT contain "PPID" (the $PPID-keyed blocking
 *      logic) or "exit 2" (the blocking exit code).
 *   3. The settings.json PreToolUse command must reference
 *      "cbm-code-discovery-gate" (the shim), not an inline blocking script.
 *
 * Actual (buggy) behaviour (if bug is present):
 *   The gate script still contains $PPID and exit 2; the assertions below
 *   that check for absence of "PPID" and "exit 2" FAIL -> RED.
 *
 * Upgrade scenario tested here (NOT covered by existing tests):
 *   This test simulates an upgrade from a pre-v0.7.0 install by:
 *     a) Pre-seeding the gate-script path with the OLD blocking content
 *        (containing $PPID and exit 2) — as would be present on disk after
 *        a pre-v0.7.0 install.
 *     b) Pre-seeding settings.json with a stale CMM hook entry using the
 *        old "Grep|Glob|Read" matcher and an old command string.
 *   Then running both cbm_upsert_claude_hooks + cbm_install_hook_gate_script
 *   (the actual install/update code path) and asserting the CORRECT result.
 *
 *   This is the critical gap: existing tests call cbm_install_hook_gate_script
 *   into an EMPTY directory (no pre-existing script).  The upgrade path
 *   (old script on disk) was not verified to be overwritten correctly.
 *
 * Relationship to existing tests:
 *   cli_hook_gate_script_no_predictable_tmp_issue384 (test_cli.c:2196):
 *     Tests cbm_install_hook_gate_script in isolation on a fresh dir.
 *     Does NOT test the upgrade/overwrite scenario.
 *   cli_upsert_claude_hook_fresh (test_cli.c:2167):
 *     Tests cbm_upsert_claude_hooks in isolation on fresh settings.json.
 *     Does NOT test the integrated (both calls) upgrade path.
 *
 * NOTE (2026-06-26): Code review of the current codebase shows that
 * cbm_install_hook_gate_script already uses fopen(path, "w") (truncate)
 * and writes the non-blocking shim. If this test is GREEN it means the bug
 * is fixed on main and the issue can be closed (the test then acts as a
 * permanent regression guard for this upgrade scenario).
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "test_helpers.h"
#include <cli/cli.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

/* ── Local helpers (mirror the helpers in test_cli.c) ──────────────── */

static int rp409_write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "%s", content);
    fclose(f);
    return 0;
}

static const char *rp409_read_file(const char *path) {
    static char buf[16384];
    FILE *f = fopen(path, "r");
    if (!f)
        return NULL;
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return buf;
}

/* Recursively create directory (simple two-level: parent + child). */
static int rp409_mkdirp(const char *path) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            cbm_mkdir(tmp);
            *p = '/';
        }
    }
    return cbm_mkdir(tmp) == 0 || errno == EEXIST ? 0 : -1;
}

/* ── Test ──────────────────────────────────────────────────────────── */

/*
 * repro_issue409_install_wires_hook_augment_not_blocking_gate
 *
 * Simulates an upgrade from a pre-v0.7.0 install:
 *   - The hooks dir already contains the OLD blocking gate script
 *     (containing $PPID and exit 2).
 *   - settings.json already contains a stale CMM hook with the old matcher
 *     "Grep|Glob|Read" and an old inline command.
 *
 * After calling cbm_upsert_claude_hooks + cbm_install_hook_gate_script
 * (the actual install/update flow), asserts that:
 *   1. The gate script is OVERWRITTEN with the non-blocking shim
 *      (contains "hook-augment", does NOT contain "PPID" or "exit 2").
 *   2. settings.json PreToolUse command references "cbm-code-discovery-gate"
 *      (the shim path), not inline blocking code.
 *   3. settings.json uses the current non-blocking matcher "Grep|Glob"
 *      (not the old "Grep|Glob|Read" that was silently upgrading Read-gating
 *      behaviour).
 *
 * RED if:
 *   - The gate script still contains "PPID"  (old blocking logic not cleared)
 *   - The gate script still contains "exit 2" (old blocking exit not cleared)
 *   - The gate script does NOT contain "hook-augment" (shim not written)
 *   - settings.json does NOT contain "cbm-code-discovery-gate" (wrong command)
 *
 * Oracle used: cbm_upsert_claude_hooks(settings_path) +
 *              cbm_install_hook_gate_script(home, binary_path)
 * (the same two calls made by install_claude_code_config in cli.c).
 */
TEST(repro_issue409_install_wires_hook_augment_not_blocking_gate) {
    /* Create a temp HOME directory tree that simulates a pre-v0.7.0 install. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/rp409-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    /* Create <home>/.claude/hooks/ (mirrors real Claude Code layout). */
    char hooks_dir[512];
    snprintf(hooks_dir, sizeof(hooks_dir), "%s/.claude/hooks", tmpdir);
    if (rp409_mkdirp(hooks_dir) != 0)
        FAIL("mkdirp hooks_dir failed");

    /* Pre-seed the gate script with the OLD blocking content that the issue
     * reporter observed on v0.7.0.  This is the content that must be
     * overwritten (truncated) by cbm_install_hook_gate_script. */
    char script_path[512];
    snprintf(script_path, sizeof(script_path),
             "%s/cbm-code-discovery-gate", hooks_dir);
    rp409_write_file(script_path,
        "#!/bin/bash\n"
        "# Gate hook: nudges Claude toward codebase-memory-mcp for code discovery.\n"
        "# First Grep/Glob/Read per session -> block. Subsequent -> allow.\n"
        "# PPID = Claude Code process PID, unique per session.\n"
        "GATE=/tmp/cbm-code-discovery-gate-$PPID\n"
        "if [ -f \"$GATE\" ]; then exit 0; fi\n"
        "touch \"$GATE\"\n"
        "echo 'BLOCKED: use codebase-memory-mcp' >&2\n"
        "exit 2\n");

    /* Pre-seed settings.json with a stale CMM hook entry (old matcher). */
    char settings_path[512];
    snprintf(settings_path, sizeof(settings_path),
             "%s/.claude/settings.json", tmpdir);
    rp409_write_file(settings_path,
        "{\"hooks\":{\"PreToolUse\":["
        "{\"matcher\":\"Grep|Glob|Read\","
        "\"hooks\":[{\"type\":\"command\","
        "\"command\":\"~/.claude/hooks/cbm-code-discovery-gate\"}]}]}}");

    /* Run the actual install/update hook wiring (same two calls as
     * install_claude_code_config in src/cli/cli.c lines 3045-3046). */
    int rc = cbm_upsert_claude_hooks(settings_path);
    ASSERT_EQ(rc, 0);
    cbm_install_hook_gate_script(tmpdir, "/usr/local/bin/codebase-memory-mcp");

    /* ── Assert the gate script was OVERWRITTEN with the non-blocking shim ── */
    const char *script_data = rp409_read_file(script_path);
    ASSERT_NOT_NULL(script_data);

    /* MUST NOT contain $PPID: the old blocking gate used
     * /tmp/cbm-code-discovery-gate-$PPID as a per-invocation state file.
     * If present, the blocking gate was not overwritten -> RED for #409. */
    ASSERT(strstr(script_data, "PPID") == NULL);

    /* MUST NOT contain "exit 2": the old gate blocked tool calls with exit 2.
     * If present, the installer still emits the blocking exit code -> RED. */
    ASSERT(strstr(script_data, "exit 2") == NULL);

    /* MUST contain "hook-augment": the non-blocking shim delegates to the
     * compiled augmenter via `"$BIN" hook-augment 2>/dev/null`.
     * If absent, install did not write the correct shim -> RED for #409. */
    ASSERT(strstr(script_data, "hook-augment") != NULL);

    /* ── Assert settings.json was updated to the correct non-blocking config ── */
    const char *settings_data = rp409_read_file(settings_path);
    ASSERT_NOT_NULL(settings_data);

    /* The PreToolUse command must reference the shim (by its well-known name),
     * not an inline blocking script. */
    ASSERT(strstr(settings_data, "cbm-code-discovery-gate") != NULL);

    /* The old "Grep|Glob|Read" matcher (which gated Read calls, breaking
     * the read-before-edit invariant per issue #362) must have been replaced
     * with the current "Grep|Glob" matcher. */
    ASSERT(strstr(settings_data, "\"Grep|Glob\"") != NULL);
    ASSERT(strstr(settings_data, "Glob|Read") == NULL);

    th_rmtree(tmpdir);
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────── */
SUITE(repro_issue409) {
    RUN_TEST(repro_issue409_install_wires_hook_augment_not_blocking_gate);
}
