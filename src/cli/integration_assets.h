/*
 * integration_assets.h — Verified access to the shipped integration templates.
 *
 * The binary used to embed every integration artifact it installs — nine
 * complete shebang'd shell scripts, their PowerShell/.cmd twins, and the
 * generated JS/TS client modules — as C string literals, written to client
 * config dirs at 0755. That block of script-shaped bytes inside an unsigned
 * executable is exactly the surface AV classifiers score (Wacatac.B!ml), so
 * the bodies now live in ONE shipped data file, assets/cbm-integrations.json,
 * and the binary embeds exactly ONE constant about them: the file's SHA-256
 * (generated at build time into cbm_integrations_hash.h).
 *
 * Integrity model: every use verifies the file against that constant first.
 * This preserves the tamper-evidence the templates had while compiled in;
 * a verified copy is bit-identical to the shipped one by construction, so
 * WHICH copy satisfied the check never matters — only availability does.
 *
 * Resolution order: $CBM_ASSETS_DIR (authoritative when set — a bad explicit
 * override fails, it never falls back), else next to the running binary (the
 * flat release-archive layout install.sh/install.ps1 execute from), else the
 * source-tree layout <bindir>/../../assets (build/c/ builds and test-runner),
 * else the stored copy under <home>/.cbm/assets/<version>/.
 *
 * The stored copy is the OWNERSHIP REFERENCE: `install` persists the verified
 * bytes to <home>/.cbm/assets/<version>/ — a SIBLING of the disposable cache
 * (~/.cache/codebase-memory-mcp), never inside it — so uninstall can decide
 * "is this deployed file ours?" by materializing templates from it long after
 * the release archive is gone. Deciding ownership by materialize-and-compare
 * against these templates REPLACES the old exact-match against embedded
 * literals; released[] carries the historical bodies older versions wrote, so
 * upgrades and uninstalls keep recognising them. Foreign or user-modified
 * files are preserved exactly as before.
 *
 * Single-threaded by design: only the CLI install/uninstall paths touch this,
 * never the MCP server or hook-augment hot paths.
 */
#ifndef CBM_CLI_INTEGRATION_ASSETS_H
#define CBM_CLI_INTEGRATION_ASSETS_H

#include <stdbool.h>
#include <stddef.h>

#define CBM_INTEGRATIONS_ASSET_NAME "cbm-integrations.json"

/* One template body. `text` may contain {{BIN}} / {{EVENT}} / {{TOOLS}} /
 * {{TOOL}} placeholders; `bin_mode` names how {{BIN}} must be encoded when
 * substituted ("sh", "ps", "cmd", "raw", "js"), or NULL for a body that
 * takes no binary path at all. */
typedef struct {
    const char *text;
    const char *bin_mode;
} cbm_integration_body_t;

/* A template resolved for THIS platform (posix/windows/all variant). The
 * released list is the exact set of historical bodies previous versions
 * materialized; it exists so ownership checks still recognise files written
 * before this version — the ONE place old bodies may live. */
typedef struct {
    cbm_integration_body_t current;
    const cbm_integration_body_t *released;
    size_t released_count;
    const char *tool_line; /* adapters only: per-tool registration line */
} cbm_integration_template_t;

/* Locate, read and hash-verify the asset file (cached after first success;
 * failures are re-resolved on every call so a transient state never sticks).
 * `home` may be NULL when no stored-copy candidate should be considered.
 * Returns true on success; on failure fills `err` (when given) with the
 * actionable message. */
bool cbm_integration_assets_require(const char *home, char *err, size_t err_sz);

/* Install-time step: verify (as above) and persist the verified bytes to
 * <home>/.cbm/assets/<version>/cbm-integrations.json. Verification failure or
 * an unwritable store is an error — the caller must not partially install.
 * dry_run verifies only and writes nothing. */
bool cbm_integration_assets_install(const char *home, bool dry_run, char *err, size_t err_sz);

/* Template lookup for this platform. Returns NULL when the assets are
 * unavailable or the id/variant is unknown. The returned pointers stay valid
 * for the process lifetime (or until the test-only reset). */
const cbm_integration_template_t *cbm_integration_template(const char *id);

#ifdef CBM_CLI_ENABLE_TEST_API
/* Drop the cached document so a test can steer resolution via CBM_ASSETS_DIR
 * (set = authoritative) and observe verification failures deterministically. */
void cbm_integration_assets_reset_for_testing(void);
#endif

#endif /* CBM_CLI_INTEGRATION_ASSETS_H */
