#ifndef CBM_FOUNDATION_WORKSPACE_H
#define CBM_FOUNDATION_WORKSPACE_H

#include <stdbool.h>
#include <stddef.h>

/*
 * Workspace boundary policy — classify a directory as an indexing root.
 *
 * This is the breadth half of the boundary: it rejects roots that are too broad
 * or too sensitive to be indexed as a unit. It is NOT the authorization half.
 * Whether a caller may index a given tree at all is answered by the user-level
 * grant store, because that is the only input a caller cannot write. A verdict
 * of CBM_WS_ALLOW here means "this path is not obviously too broad", never
 * "this path is authorized".
 *
 * Every function takes an ALREADY-CANONICALIZED absolute path. Classifying
 * before symlink resolution lets `/a/b/c/d/e/link -> /` present as a deep path
 * and launder straight through, so callers must resolve first.
 *
 * home_dir and cache_dir are passed in rather than read from the environment so
 * the policy is a pure function and can be tested without touching the host.
 * Either may be NULL, which disables the checks that depend on it.
 */

typedef enum {
    CBM_WS_ALLOW = 0,
    /* Fewer path components than the platform minimum — e.g. "/etc", "/home". */
    CBM_WS_DENY_TOO_SHALLOW,
    /* A filesystem root, a drive root, a UNC share root, or a path that is or
     * contains the cbm cache directory. Never overridable. */
    CBM_WS_DENY_ABSOLUTE,
    /* The home directory itself, or a credential/system directory. Overridable
     * by an explicit human action, never by a manifest or a tool call. */
    CBM_WS_DENY_SENSITIVE,
} cbm_ws_verdict_t;

/* Classify canonical_path as a candidate indexing root. */
cbm_ws_verdict_t cbm_workspace_classify_root(const char *canonical_path, const char *home_dir,
                                             const char *cache_dir);

/* Stable, human-facing reason for a verdict. Never NULL. */
const char *cbm_workspace_verdict_reason(cbm_ws_verdict_t verdict);

/* True when an explicit human action may override this verdict. Absolute denials
 * and shallow paths are never overridable; sensitive ones are. */
bool cbm_workspace_verdict_is_overridable(cbm_ws_verdict_t verdict);

/* Number of path components below the volume root, ignoring repeated and
 * trailing separators. "/" is 0, "/etc" is 1, "C:/repos" is 1, "//srv/share" is
 * 0 (the share root itself). Exposed for tests. */
int cbm_workspace_path_depth(const char *canonical_path);

/* Canonical containment check: is abs_path at or below root_path, after symlink
 * and junction resolution? The definition currently lives in src/mcp/mcp.c; it is
 * declared here so the MCP handler, the HTTP UI route and the CLI all reach the
 * same implementation. Two entry points evaluating the same policy separately is
 * what produced the UI/MCP divergence in the first place. */
bool cbm_path_within_root(const char *root_path, const char *abs_path);

/*
 * ── Grant store ──────────────────────────────────────────────────────────────
 *
 * The authorization half. Grants live in a user-level file under the cache
 * directory and are only ever written by an explicit human action (the CLI), so
 * neither an indexed repository nor a tool caller can widen its own boundary.
 *
 * Enforcement is deliberately two-tier, because default-deny with an empty store
 * would refuse every first run:
 *
 *   - The breadth policy above is ALWAYS enforced. Out of the box that already
 *     refuses "/", "/etc", "$HOME", "~/.ssh" and friends — the paths the reports
 *     actually demonstrate — with no configuration at all.
 *   - Containment in a granted root is enforced ONLY once the store is non-empty
 *     or a root is configured. Declaring your first root is therefore what turns
 *     on true confinement, and it never silently loosens: adding a root can only
 *     narrow what was previously unrestricted.
 */

/* Absolute path of the grant file for a cache directory. */
bool cbm_workspace_grant_path(const char *cache_dir, char *out, size_t out_sz);

/* Record canonical_path as an allowed root. approve_sensitive records an explicit
 * human override for a CBM_WS_DENY_SENSITIVE path; absolute denials and shallow
 * paths are never grantable. Returns false and fills err on refusal. */
bool cbm_workspace_grant_add(const char *cache_dir, const char *home_dir,
                             const char *canonical_path, bool approve_sensitive, char *err,
                             size_t err_sz);

/* Newline-separated granted roots into out. True when at least one exists. */
bool cbm_workspace_grant_list(const char *cache_dir, char *out, size_t out_sz);

/* The whole decision, used by every entry point that accepts a repo path.
 * canonical_path must already be canonicalized. configured_root is the legacy
 * CBM_ALLOWED_ROOT / session policy, treated as an additional grant, or NULL.
 * On refusal, err receives a message that names the command which would fix it. */
bool cbm_workspace_root_allowed(const char *canonical_path, const char *home_dir,
                                const char *cache_dir, const char *configured_root, char *err,
                                size_t err_sz);

/* The home and cache directories the policy should be evaluated against. Shared
 * so two callers cannot derive them differently and reach different verdicts for
 * the same path. Either may return NULL. */
const char *cbm_workspace_home_dir(void);
const char *cbm_workspace_cache_dir(void);

#endif /* CBM_FOUNDATION_WORKSPACE_H */

/*
 * ── Per-project request manifest (.cbmpathwhitelist) ─────────────────────────
 *
 * A repository may ship a manifest listing outside roots it would like indexed
 * alongside it. The file REQUESTS; it never GRANTS. Both attackers in this
 * project's threat model can write a file inside a repository — a malicious
 * indexed repo, and an agent with project write access — so a repo-local file
 * cannot be authoritative without handing them the boundary.
 *
 * Approval is recorded user-level and keyed to a SHA-256 of the manifest bytes,
 * so editing the file (a `git pull` that widens the requests, say) lapses the
 * approval and asks again. Without the hash, a repo approved once could widen
 * itself forever. This is direnv's model, and it is the only shape that keeps the
 * ergonomics of a checked-in file without making the file a permission slip.
 */

#define CBM_WS_MANIFEST_NAME ".cbmpathwhitelist"

enum { CBM_WS_MANIFEST_MAX_ENTRIES = 64 };

typedef struct {
    /* Requested roots, verbatim from the file (already control-char screened). */
    char entries[CBM_WS_MANIFEST_MAX_ENTRIES][1024];
    int count;
    /* SHA-256 hex of the raw file bytes; empty when there is no manifest. */
    char digest[65];
    bool present;
} cbm_ws_manifest_t;

/* Read <project_root>/.cbmpathwhitelist. Returns false only on a malformed file
 * (an entry containing control characters, or more entries than the cap); a
 * missing file is success with present=false. */
bool cbm_workspace_manifest_read(const char *project_root, cbm_ws_manifest_t *out);

/* True when this exact manifest content has been approved for this project. A
 * changed manifest is a different digest and so is not approved. */
bool cbm_workspace_manifest_is_approved(const char *cache_dir, const char *project_root,
                                        const cbm_ws_manifest_t *manifest);

/* Record approval for the manifest currently on disk. Refuses when a requested
 * entry would not be allowable as a root on its own — approving a manifest must
 * not become a way around the breadth policy. */
bool cbm_workspace_manifest_approve(const char *cache_dir, const char *home_dir,
                                    const char *project_root, char *err, size_t err_sz);

/* True when candidate is at or below an APPROVED manifest entry of project_root. */
bool cbm_workspace_manifest_allows(const char *cache_dir, const char *home_dir,
                                   const char *project_root, const char *candidate);
