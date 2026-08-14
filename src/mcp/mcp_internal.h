#ifndef CBM_MCP_INTERNAL_H
#define CBM_MCP_INTERNAL_H

#include "mcp/mcp.h"
#include "pipeline/pipeline.h" /* cbm_changed_hunk_t */
#include "store/store.h"       /* cbm_node_t */

/* White-box fault injection for deterministic cross-platform quarantine
 * safety tests. This header is internal and is not part of the MCP API. */
typedef bool (*cbm_mcp_quarantine_test_hook_fn)(void *context, const char *step);
typedef bool (*cbm_mcp_command_test_hook_fn)(void *context, const char *command);

void cbm_mcp_server_set_quarantine_test_hook(cbm_mcp_server_t *srv,
                                             cbm_mcp_quarantine_test_hook_fn hook, void *context);
void cbm_mcp_server_set_command_test_hook(cbm_mcp_server_t *srv, cbm_mcp_command_test_hook_fn hook,
                                          void *context);

/* Release only the constructor-created pristine in-memory store. Public
 * cbm_mcp_server_new(NULL) semantics remain unchanged; daemon sessions use
 * this immediately before publication so idle sessions retain no SQLite DB. */
bool cbm_mcp_server_release_pristine_memory_store(cbm_mcp_server_t *srv);

/* Prepend one daemon-owned notice to a successful JSON-RPC tool response.
 * On success replaces and frees *response_io; on failure it is unchanged. */
bool cbm_mcp_jsonrpc_response_prepend_notice(char **response_io, const char *notice);

enum { CBM_MCP_DEFAULT_AUTO_INDEX_LIMIT = 50000 };

/* Count indexable files with the pipeline's native full-mode discovery policy,
 * without retaining per-file results. A false result means the count exceeded
 * file_limit or could not be established before the bounded deadline; every
 * such failure is fail-closed because this is the memory-admission guard. */
/* Map an internal resolver strategy (as recorded on a CALLS edge by
 * pass_calls.c) to the CLOSED public class published by trace_path's
 * include_evidence output: "lsp" | "language_rule" | "heuristic" |
 * "unresolved". NULL only for a NULL/empty strategy.
 *
 * Exposed so tests/test_mcp.c can pin every strategy production can emit to a
 * known class — a new resolver KIND must fail there rather than leaking an
 * unmapped internal name into a user-visible field. */
const char *cbm_mcp_edge_strategy_class(const char *strategy);

bool cbm_mcp_auto_index_within_file_limit(const char *root_path, int file_limit,
                                          int *file_count_out);

/* detect_changes seed scoping (#1363): does `node`'s line range overlap any
 * recorded hunk for `file`? Exposed for direct unit testing of the overlap
 * logic, independent of the git/subprocess/index plumbing around it. */
bool cbm_detect_node_in_hunks(const cbm_node_t *node, const cbm_changed_hunk_t *hunks,
                              int hunk_count, const char *file);

#ifdef CBM_ENABLE_TEST_SEAMS
/* Reject opening one changed file while detect_changes fingerprints its live
 * snapshot. This makes otherwise platform-specific permission/read races
 * deterministic without changing production filesystem behavior. */
typedef bool (*cbm_mcp_snapshot_read_test_hook_fn)(void *context, const char *absolute_path);
void cbm_mcp_server_set_snapshot_read_test_hook(cbm_mcp_server_t *srv,
                                                cbm_mcp_snapshot_read_test_hook_fn hook,
                                                void *context);

/* Filesystem PATH_MAX prevents a portable end-to-end fixture for multi-KiB
 * stored paths. This seam drives the normal search result ownership,
 * directory aggregation, and renderer pipeline with synthetic identities. */
char *cbm_mcp_render_search_rows_for_testing(const char *const *qualified_names,
                                             const char *const *file_paths, int row_count,
                                             bool json_format);

/* Drive the production raw-source slicer and renderer without relying on an
 * external grep/PowerShell text decoder. This keeps malformed-byte paging
 * deterministic on every test platform. */
char *cbm_mcp_render_raw_preview_for_testing(const char *content, bool content_offset_set,
                                             size_t content_offset, bool json_format);

/* Exercise search_graph's semantic continuation metadata without allocating
 * a 100k-row vector fixture. The production emitters remain the code under
 * test; this seam only supplies their already-ranked page descriptor. */
char *cbm_mcp_render_semantic_paging_for_testing(int total, int offset, int returned, int limit,
                                                 bool total_exact, bool json_format);
#endif

#endif
