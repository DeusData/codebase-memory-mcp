/*
 * neo4j_http.h — Minimal HTTP/1.1 client for the Neo4j HTTP API.
 *
 * One persistent (keep-alive) connection per handle, POST-only, Basic auth.
 * Plain http:// works on every platform over raw sockets. https:// goes
 * through WinHTTP on Windows; on POSIX builds it is refused with a clear
 * error (no TLS library is vendored).
 */
#ifndef CBM_NEO4J_HTTP_H
#define CBM_NEO4J_HTTP_H

#include <stdbool.h>
#include <stddef.h>

#define CBM_NEO4J_HOST_MAX 256
#define CBM_NEO4J_PATH_MAX 512

typedef struct {
    bool tls;
    char host[CBM_NEO4J_HOST_MAX];
    int port;
    char base_path[CBM_NEO4J_PATH_MAX]; /* "" or "/prefix" without trailing slash */
} cbm_neo4j_url_t;

/* Parse "http[s]://host[:port][/prefix]". bolt://, neo4j:// and their +s
 * variants are rejected with an error that names the HTTP port to use.
 * Returns 0 on success, -1 with err filled otherwise. */
int cbm_neo4j_url_parse(const char *url, cbm_neo4j_url_t *out, char *err, size_t err_sz);

/* Standard base64 (with padding). Returns a malloc'd string. */
char *cbm_neo4j_base64(const char *data, size_t len);

typedef struct cbm_neo4j_http cbm_neo4j_http_t;

/* Open a client handle; the connection itself is made lazily on the first
 * request and re-made once if a kept-alive socket turns out to be dead. */
cbm_neo4j_http_t *cbm_neo4j_http_open(const cbm_neo4j_url_t *url, const char *user,
                                      const char *password, char *err, size_t err_sz);

/* POST a JSON body to base_path + path. On success returns 0, sets *status to
 * the HTTP status and *resp to a malloc'd NUL-terminated body (caller frees).
 * Transport failures return -1 with err filled. */
int cbm_neo4j_http_post(cbm_neo4j_http_t *h, const char *path, const char *body, size_t body_len,
                        int *status, char **resp, size_t *resp_len, char *err, size_t err_sz);

void cbm_neo4j_http_close(cbm_neo4j_http_t *h);

#endif /* CBM_NEO4J_HTTP_H */
