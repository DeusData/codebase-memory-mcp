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

/* Inspect, then atomically consume, one coalesced tools/list_changed
 * notification after a response is ready. Daemon dispatch peeks before its
 * cancellation linearization point and consumes only after the request wins;
 * direct stdio consumes after writing its response. Both are O(1) time/memory,
 * and no background thread writes a protocol stream. */
bool cbm_mcp_server_tools_list_changed_pending(cbm_mcp_server_t *srv);
bool cbm_mcp_server_take_tools_list_changed(cbm_mcp_server_t *srv);

/* White-box counter for query-store open attempts made on behalf of this
 * server. Tests use it to pin one-open validation/dispatch without depending
 * on wall-clock timing. */
#ifdef CBM_ENABLE_TEST_SEAMS
uint64_t cbm_mcp_server_query_store_open_count_for_testing(const cbm_mcp_server_t *srv);
uint64_t cbm_mcp_server_request_mem_collect_count_for_testing(const cbm_mcp_server_t *srv);
void cbm_mcp_test_fail_next_semantic_keyword_allocation(void);
#endif

/* Prepend one daemon-owned notice to a successful JSON-RPC tool response.
 * On success replaces and frees *response_io; on failure it is unchanged. */
bool cbm_mcp_jsonrpc_response_prepend_notice(char **response_io, const char *notice);

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

/* Apply the CONFIGURED auto_index_limit, which is not the same thing as the raw
 * file limit above. Every limit key in the configuration registry documents 0 as
 * "unlimited" rather than "allow nothing": auto_index_limit says "0=no limit,
 * index everything" (src/cli/cli.c), and auto_dep_limit, query_max_output_bytes,
 * and snippet_max_lines all say "0 = unlimited". Callers holding a value read
 * from configuration must use this; cbm_mcp_auto_index_within_file_limit is the
 * mechanism and honors 0 literally as a limit of zero. Keeping the convention in
 * one place is deliberate: it previously lived only inside the MCP resolve path,
 * so the daemon admission path read the same key and reached the opposite
 * conclusion. file_count_out receives -1 when no count was needed. */
bool cbm_mcp_auto_index_within_configured_limit(const char *root_path, int configured_limit,
                                                int *file_count_out);

/* detect_changes seed scoping (#1363): does `node`'s line range overlap any
 * recorded hunk for `file`? Exposed for direct unit testing of the overlap
 * logic, independent of the git/subprocess/index plumbing around it. */
bool cbm_detect_node_in_hunks(const cbm_node_t *node, const cbm_changed_hunk_t *hunks,
                              int hunk_count, const char *file);

#endif
