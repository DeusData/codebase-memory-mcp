/*
 * client_adapter.h — Generate client extension modules from the MCP tool registry.
 *
 * Some clients have no MCP support (pi), and some have no declarative hook
 * configuration (OpenCode); their only extension point is a JavaScript/TypeScript
 * module. We do not ship such a module as a repository asset — it would be a
 * second, hand-maintained copy of the tool surface, and hand-maintained copies of
 * the tool list have already produced defects here.
 *
 * Instead the module is GENERATED at install time from the live registry
 * (cbm_mcp_tool_count / cbm_mcp_tool_name), so it cannot drift: adding a tool to
 * TOOLS[] adds it to every generated adapter with no second edit.
 *
 * Generated text is returned heap-allocated; the caller frees it. Nothing here
 * touches the filesystem — writing is the caller's job, so dry-run and
 * owned-block semantics stay in one place.
 */
#ifndef CBM_CLI_CLIENT_ADAPTER_H
#define CBM_CLI_CLIENT_ADAPTER_H

#include <stdbool.h>
#include <stddef.h>

/* Marker lines delimiting the generated region inside a client's extension
 * module. A file carrying these is ours to rewrite; one without them is the
 * user's and is left alone. Kept in the comment syntax shared by JS and TS. */
#define CBM_ADAPTER_MARKER_START "// codebase-memory-mcp:start"
#define CBM_ADAPTER_MARKER_END "// codebase-memory-mcp:end"

/* Emit a pi extension registering every tool in the MCP registry.
 *
 * pi has no MCP client, so this bridge is the only route to the graph for it.
 * Each registered tool shells out to `<binary> cli --json <tool>` and writes
 * its JSON arguments to stdin, using an existing public entry point without
 * the deprecated positional-JSON transport.
 *
 * `binary_path` is embedded as a JS string literal and is escaped; a Windows
 * path such as C:\Users\x\bin\cbm.exe must not corrupt the module or, worse,
 * produce a file that fails to parse inside an auto-loaded extension directory.
 *
 * Returns NULL on allocation failure or when binary_path is NULL/empty. */
char *cbm_client_adapter_pi(const char *binary_path);

/* Emit an OpenCode plugin that augments grep/glob results from the graph.
 *
 * OpenCode HAS MCP (see cbm_upsert_opencode_mcp), so this adds no tools — it
 * only supplies the automatic "consult the graph before grepping" reflex that
 * other clients get through their own native hook configuration. OpenCode has
 * no such configuration; a plugin module is its only extension point.
 *
 * NOTE for maintainers: this hooks `tool.execute.after`, whose ability to
 * modify a tool's output is NOT part of OpenCode's documented plugin contract
 * (only `tool.execute.before`'s argument mutation is). If OpenCode changes it,
 * augmentation stops silently — no error, no signal. That risk is accepted
 * deliberately and recorded here so it is not rediscovered as a mystery.
 *
 * Returns NULL on allocation failure or when binary_path is NULL/empty. */
char *cbm_client_adapter_opencode(const char *binary_path);

/* Escape a path for embedding in a single-quoted JS string literal: backslash,
 * single quote, and the newline/CR that would terminate the literal outright.
 * Writes at most out_sz-1 characters plus NUL. Returns false when the escaped
 * form would not fit, so a caller never emits a truncated literal.
 *
 * Exposed for tests: the escaping is the part most likely to break a user's
 * install on Windows, and it deserves direct coverage rather than only being
 * reachable through a full generate call. */
bool cbm_client_adapter_escape_js(const char *in, char *out, size_t out_sz);

#endif /* CBM_CLI_CLIENT_ADAPTER_H */
