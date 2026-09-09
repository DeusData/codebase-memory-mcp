/*
 * http_server.h — Embedded HTTP server for the diagnostic dashboard.
 *
 * Binds to 127.0.0.1:<port> only (localhost).
 * Serves embedded frontend assets and proxies /rpc to a dedicated
 * read-only cbm_mcp_server_t instance.
 *
 * Runs in a background pthread, same pattern as the watcher thread.
 */
#ifndef CBM_UI_HTTP_SERVER_H
#define CBM_UI_HTTP_SERVER_H

#include <stdbool.h>
#include <stddef.h>

typedef struct cbm_http_server cbm_http_server_t;

/* Create an HTTP server on the given port.
 * Creates its own cbm_mcp_server_t with a separate read-only SQLite connection.
 * Returns NULL on failure (e.g. port in use). */
cbm_http_server_t *cbm_http_server_new(int port);

/* Free the HTTP server (call after thread has been joined). */
void cbm_http_server_free(cbm_http_server_t *srv);

/* Signal the HTTP server to stop (safe to call from any thread). */
void cbm_http_server_stop(cbm_http_server_t *srv);

/* Run the HTTP server event loop (call from background thread).
 * Blocks until cbm_http_server_stop() is called. */
void cbm_http_server_run(cbm_http_server_t *srv);

/* Check if the server started successfully (listener bound). */
bool cbm_http_server_is_running(const cbm_http_server_t *srv);

/* The actually-bound port (useful when constructed with port 0 in tests). */
int cbm_http_server_port(const cbm_http_server_t *srv);

/* Per-process dashboard capability token.
 *
 * The token is 32 bytes of operating-system entropy encoded as 64 lowercase
 * hexadecimal characters. It is generated when the server is created and is
 * never accepted for any non-read-only operation. The returned pointer remains
 * valid until cbm_http_server_free(). */
const char *cbm_http_server_token(const cbm_http_server_t *srv);

/* Build the URL users should open. The capability is carried in the URL
 * fragment so it is not sent in the initial HTTP request or Referer header.
 * The embedded client moves it into authenticated API/RPC requests.
 * Returns false when the server is unavailable or the buffer is too small. */
bool cbm_http_server_launch_url(const cbm_http_server_t *srv, char *buf, size_t bufsz);

/* Override the per-connection receive deadline (tests use short values). */
void cbm_http_server_set_recv_deadline_ms(cbm_http_server_t *srv, int ms);

/* Initialize the log ring buffer mutex. Must be called once before any threads. */
void cbm_ui_log_init(void);

/* Append a log line to the UI ring buffer (called from log hook). */
void cbm_ui_log_append(const char *line);

#endif /* CBM_UI_HTTP_SERVER_H */
