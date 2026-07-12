/*
 * memory_share.h — Portable Global Memory bundles and Git transport.
 *
 * Sharing deliberately operates on logical rows and immutable raw objects;
 * it never swaps the live SQLite database.  Git is only a transport for the
 * deterministic JSON bundle produced here.
 */
#ifndef CBM_MEMORY_SHARE_H
#define CBM_MEMORY_SHARE_H

#include "memory/memory.h"

#define CBM_MEMORY_BUNDLE_SCHEMA "cbm-global-memory-bundle"
#define CBM_MEMORY_BUNDLE_VERSION 1
#define CBM_MEMORY_EXPORT_FILENAME "memory-export.json"

/* All functions return a malloc'd JSON object.  The caller frees it with
 * free().  Domain and validation failures are represented as
 * {"ok":false,"error":"..."}; NULL means allocation/internal failure.
 *
 * export args:
 *   {"path"?: string}
 *   Default path: <memory-home>/export/memory-export.json.
 *
 * import args:
 *   {"path"?: string, "policy": "reject"|"keep_local"|
 *                                     "keep_remote"|"newest"}
 *   `newest` is only applied where a deterministic timestamp/revision order
 *   is safe.  Wiki/claim/decision/experience semantic conflicts are retained
 *   as conflict proposals and are never resolved with last-write-wins.
 *
 * sync args:
 *   {"action":"init"|"status"|"pull"|"push"|"configure_remote",
 *    "remote"?: string, "remote_name"?: string, "branch"?: string,
 *    "policy"?: string, "allow_local_remote"?: boolean}
 */
char *cbm_memory_export_json(cbm_memory_t *memory, const char *args_json);
char *cbm_memory_import_json(cbm_memory_t *memory, const char *args_json);
char *cbm_memory_sync_json(cbm_memory_t *memory, const char *args_json);

#endif /* CBM_MEMORY_SHARE_H */
