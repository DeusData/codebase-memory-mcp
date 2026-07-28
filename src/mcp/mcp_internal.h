#ifndef CBM_MCP_INTERNAL_H
#define CBM_MCP_INTERNAL_H

#include "mcp/mcp.h"

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

/* Prepend one daemon-owned notice to a successful JSON-RPC tool response.
 * On success replaces and frees *response_io; on failure it is unchanged. */
bool cbm_mcp_jsonrpc_response_prepend_notice(char **response_io, const char *notice);

enum { CBM_MCP_DEFAULT_AUTO_INDEX_LIMIT = 50000 };

/* Count indexable files with the pipeline's native full-mode discovery policy,
 * without retaining per-file results. A false result means the count exceeded
 * file_limit or could not be established before the bounded deadline; every
 * such failure is fail-closed because this is the memory-admission guard. */
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

#endif
