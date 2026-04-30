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
 * Import refuses artifacts with schema_version > current. */
#define CBM_ARTIFACT_SCHEMA_VERSION 1

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

#endif /* CBM_ARTIFACT_H */
