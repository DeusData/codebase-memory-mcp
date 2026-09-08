/*
 * artifact.h — Persistent artifact export/import for team sharing.
 *
 * Exports the SQLite knowledge graph as a zstd-compressed artifact
 * to .codebase-memory/graph.db.zst in the repository. Teammates
 * can import the artifact to bootstrap their local index instead
 * of running a full pipeline from scratch.
 */
#ifndef CBM_ARTIFACT_H
#define CBM_ARTIFACT_H

#include <stdbool.h>

/* Schema version — increment when DB schema changes (new tables/indexes).
 * Import refuses artifacts with schema_version > current.
 * v2: edges uniqueness widened to (source_id, target_id, type,
 *     local_name_gen) so sibling named imports coexist (#768) — old
 *     binaries cannot upsert against the widened constraint. */
#define CBM_ARTIFACT_SCHEMA_VERSION 2

#define CBM_ARTIFACT_FILENAME "graph.db.zst"
#define CBM_ARTIFACT_META "artifact.json"
#define CBM_ARTIFACT_DIR ".codebase-memory"

/* Export quality levels */
enum {
    CBM_ARTIFACT_FAST = 0, /* zstd -3, no index stripping (watcher path) */
    CBM_ARTIFACT_BEST = 1, /* zstd -9 + drop indexes + VACUUM INTO (explicit index) */
};

/* Export DB to .codebase-memory/graph.db.zst artifact.
 * quality: CBM_ARTIFACT_FAST or CBM_ARTIFACT_BEST.
 * Creates .codebase-memory/ dir, .gitattributes, and artifact.json.
 * Returns 0 on success, -1 on error. */
int cbm_artifact_export(const char *db_path, const char *repo_path, const char *project_name,
                        int quality);

/* Get details for the most recent export failure on this thread.
 * Returns NULL if no export error is recorded. */
const char *cbm_artifact_export_last_error(void);

/* Why the most recent cbm_artifact_export on THIS thread did NOT write the
 * optional "reconcile_basis" marker, or NULL when it did write one. The string
 * names the failing precondition (head_unresolved / head_not_hex_oid /
 * tree_not_clean / db_hashes_differ_from_disk) and, for the last of those, the
 * offending row with both stamps.
 *
 * Must be read from the thread that called export and BEFORE anything else
 * exports on it. It cannot be reconstructed by re-running export: export's own
 * ensure_gitattributes leaves an untracked .gitattributes behind, so a second
 * evaluation always answers tree_not_clean regardless of the real cause. */
const char *cbm_artifact_reconcile_basis_last_blocker(void);

/* Import artifact from .codebase-memory/graph.db.zst to cache_db_path.
 * Decompresses, runs integrity check, recreates indexes.
 * Returns 0 on success, -1 on error. */
int cbm_artifact_import(const char *repo_path, const char *cache_db_path);

/* Check if a compatible artifact exists in repo_path/.codebase-memory/.
 * Returns true only if both graph.db.zst and artifact.json exist
 * and schema_version is compatible. */
bool cbm_artifact_exists(const char *repo_path);

/* Get the git commit hash from artifact metadata. Caller must free().
 * Returns NULL if artifact doesn't exist or has no commit field. */
char *cbm_artifact_commit(const char *repo_path);

/* Whether repo_path is safe to interpolate into a double-quoted `git -C "…"` shell
 * command (as artifact.c does via cbm_popen). Rejects quote / backslash / shell
 * substitution metacharacters (cbm_validate_shell_arg); on Windows also rejects the
 * cmd.exe expansion metacharacters % ! ^. Spaces ARE allowed — double quotes handle
 * them on both POSIX sh and cmd.exe (single quotes, which cmd.exe does not honor,
 * were the pre-existing bug). Exposed so the shell-safety contract is unit-tested. */
bool cbm_artifact_repo_path_is_shell_safe(const char *repo_path);

/* After importing a teammate's artifact, re-stamp the file_hashes rows whose
 * content git proves unchanged between the artifact's commit and the local
 * working tree, using local stat() values. Without this every imported row
 * carries the EXPORTER's mtime, so the first incremental run re-parses
 * ~every file and the artifact saves almost nothing (see #885).
 *
 * TRUST TRADE-OFF — read before widening this.
 *   Today an imported artifact is already trusted for graph CONTENT; nothing
 *   verifies that the nodes/edges inside it describe the code they claim to.
 *   What limits the damage is an accident of mechanics, not a check: the
 *   foreign mtimes force a full re-parse, so a poisoned artifact is
 *   auto-scrubbed the first time anyone indexes — the exposure is TRANSIENT
 *   and SELF-HEALING, and it ends at a clone time the attacker cannot predict.
 *
 *   Reconciliation deliberately removes that scrub for rows it restamps. The
 *   exposure becomes DURABLE: poisoned nodes survive until the file's content
 *   changes. And the gate that decides it is a string the artifact PRODUCER
 *   wrote ("reconcile_basis"), so it is only as trustworthy as whoever could
 *   write the artifact — the same party who could poison the graph.
 *
 *   What keeps that acceptable is that the marker alone is never sufficient.
 *   Every restamped row must ALSO be independently confirmed by the local git:
 *   tracked at a commit that exists in THIS clone, and reported unchanged
 *   against the local working tree. A row git cannot vouch for stays foreign
 *   and is re-parsed. So the marker can only ever suppress re-parsing of files
 *   whose bytes the local repository itself certifies.
 *
 *   Anyone extending this must preserve the polarity: on ANY doubt, skip the
 *   row (leave it foreign -> re-parse). A dropped entry must never be able to
 *   read as "unchanged".
 *
 * Returns the number of rows re-stamped, or -1 when reconciliation was skipped
 * (NULL args / no git / untrusted metadata / unknown or non-hex commit /
 * shallow clone / allocation failure / any parse uncertainty).
 * Best-effort: never fails the import. */
int cbm_artifact_reconcile_hashes(const char *repo_path, const char *cache_db_path,
                                  const char *project_name);

#endif /* CBM_ARTIFACT_H */
