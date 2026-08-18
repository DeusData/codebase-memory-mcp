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
void cbm_mcp_server_set_search_output_limit_for_test(cbm_mcp_server_t *srv, size_t limit);

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

/* search_code Windows pre-scan optimization: only simple suffix globs can be
 * moved ahead of
 * Select-String without changing the existing full-path
 * PowerShell -like contract. Exposed for
 * direct boundary tests only. */
bool cbm_search_code_file_pattern_can_prefilter(const char *file_pattern);

/* Internal command builder exposed so tests can pin the PowerShell pipeline
 * ordering without
 * starting an external shell. */
void cbm_search_code_build_grep_cmd(char *cmd, size_t cmd_sz, bool use_regex, bool scoped,
                                    const char *file_pattern, const char *tmpfile,
                                    const char *filelist, const char *root_path);

#endif
