/*
 * httplink.h — HTTP route discovery and cross-service linking.
 *
 * Port of Go httplink package: discovers HTTP routes from Python decorators,
 * Go gin/chi, Express.js, Spring, Ktor, Laravel, Actix, ASP.NET, and matches
 * HTTP call sites to create HTTP_CALLS/ASYNC_CALLS edges.
 *
 * Depends on: foundation, store (for linker integration)
 */
#ifndef CBM_HTTPLINK_H
#define CBM_HTTPLINK_H

#include <stdbool.h>
#include <stdint.h>

/* ── Types ─────────────────────────────────────────────────────── */

typedef struct {
    char path[256];
    char method[16];
    char function_name[256];
    char qualified_name[512];
    char handler_ref[256];
    char resolved_handler_qn[512];
    char protocol[8]; /* "ws", "sse", or "" */
} cbm_route_handler_t;

typedef struct {
    char path[256];
    char method[16];
    char source_name[256];
    char source_qn[512];
    char source_label[32];
    bool is_async;
} cbm_http_call_site_t;

typedef struct {
    char caller_qn[512];
    char caller_label[32];
    char handler_qn[512];
    char url_path[256];
    char edge_type[16]; /* "HTTP_CALLS" or "ASYNC_CALLS" */
} cbm_http_link_t;

/* ── Similarity functions ──────────────────────────────────────── */

int cbm_levenshtein_distance(const char *a, const char *b);
double cbm_normalized_levenshtein(const char *a, const char *b);
double cbm_ngram_overlap(const char *a, const char *b, int n);
const char *cbm_confidence_band(double score);

/* ── Path matching ─────────────────────────────────────────────── */

/* Normalize a URL path: lowercase, strip trailing /, replace :param and {param} with *.
 * Returns static thread-local buffer — do NOT free. */
const char *cbm_normalize_path(const char *input);

/* Check if two paths match after normalization (exact or suffix). */
bool cbm_paths_match(const char *call_path, const char *route_path);

/* Score path similarity (0.0–1.0). Returns 0 if no match. */
double cbm_path_match_score(const char *call_path, const char *route_path);

/* Check if two QNs belong to the same service (same dir prefix). */
bool cbm_same_service(const char *qn1, const char *qn2);

/* ── Detection ─────────────────────────────────────────────────── */

/* Detect WebSocket or SSE protocol from source code. Returns "ws", "sse", or "". */
const char *cbm_detect_protocol(const char *source);

/* Check if a node is from a test file (by file_path + is_test property). */
bool cbm_is_test_node_fp(const char *file_path, bool is_test_prop);

/* Check if a path is in the exclude list (case-insensitive, strips trailing /). */
bool cbm_is_path_excluded(const char *path, const char **exclude_paths, int count);

/* ── URL extraction ────────────────────────────────────────────── */

/* Extract URL paths from text (https://host/path or "/path" patterns).
 * Returns count of paths written to out[]. Caller must free each string. */
int cbm_extract_url_paths(const char *text, char **out, int max_out);

/* Extract URL paths from JSON strings in text.
 * Returns count. Caller must free each string. */
int cbm_extract_json_string_paths(const char *text, char **out, int max_out);

/* ── Route extraction ──────────────────────────────────────────── */

/* Extract Python routes from decorator strings.
 * decorators: array of decorator strings like `@app.post("/api/orders")`
 * Returns count of routes written to out[]. */
int cbm_extract_python_routes(const char *name, const char *qn, const char **decorators, int ndec,
                              cbm_route_handler_t *out, int max_out);

/* Extract Go gin/chi routes from function source.
 * Returns count of routes written to out[]. */
int cbm_extract_go_routes(const char *name, const char *qn, const char *source,
                          cbm_route_handler_t *out, int max_out);

/* Extract Java Spring routes from decorator strings.
 * Returns count. */
int cbm_extract_java_routes(const char *name, const char *qn, const char **decorators, int ndec,
                            cbm_route_handler_t *out, int max_out);

/* Extract Kotlin Ktor routes from source.
 * Returns count. */
int cbm_extract_ktor_routes(const char *name, const char *qn, const char *source,
                            cbm_route_handler_t *out, int max_out);

/* Extract Express.js routes from source (with receiver allowlist filtering).
 * Returns count. */
int cbm_extract_express_routes(const char *name, const char *qn, const char *source,
                               cbm_route_handler_t *out, int max_out);

/* Hapi.js object-literal routes: { method: 'GET', path: '/api/...', handler: ... } */
int cbm_extract_hapi_routes(const char *name, const char *qn, const char *source,
                            cbm_route_handler_t *out, int max_out);

/* Extract PHP Laravel routes from source.
 * Returns count. */
int cbm_extract_laravel_routes(const char *name, const char *qn, const char *source,
                               cbm_route_handler_t *out, int max_out);

/* ── Read source lines ─────────────────────────────────────────── */

/* Read specific lines from a file on disk.
 * root_dir + "/" + rel_path is the full path.
 * Returns malloc'd string (caller must free), or NULL on error. */
char *cbm_read_source_lines_disk(const char *root_dir, const char *rel_path, int start_line,
                                 int end_line);

/* Read specific lines from a cached source buffer (no disk I/O).
 * source is the full file content, source_len its byte length.
 * Returns malloc'd string (caller must free), or NULL if out of range. */
char *cbm_read_source_lines_cached(const char *source, int source_len, int start_line,
                                   int end_line);

/* ── Config ────────────────────────────────────────────────────── */

/* Default exclude paths for HTTP link filtering. */
extern const char *cbm_default_exclude_paths[];
extern const int cbm_default_exclude_paths_count;

/* HTTP client keywords for AC pre-screening. */
extern const char *cbm_http_client_keywords[];
extern const int cbm_http_client_keywords_count;

/* Async dispatch keywords. */
extern const char *cbm_async_dispatch_keywords[];
extern const int cbm_async_dispatch_keywords_count;

/* ── Linker config (loaded from .cgrconfig YAML) ──────────────── */

typedef struct {
    double min_confidence; /* -1 means "use default" */
    int fuzzy_matching;    /* -1 = use default, 0 = false, 1 = true */
    char **exclude_paths;  /* user-configured exclude paths */
    int exclude_path_count;
} cbm_httplink_config_t;

/* Return a default config (all fields at "use default" sentinel values). */
cbm_httplink_config_t cbm_httplink_default_config(void);

/* Load config from .cgrconfig in the given directory.
 * Returns default config if file doesn't exist or YAML is invalid. */
cbm_httplink_config_t cbm_httplink_load_config(const char *dir);

/* Free resources owned by a config. */
void cbm_httplink_config_free(cbm_httplink_config_t *cfg);

/* Effective values (applies defaults when config field is sentinel). */
double cbm_httplink_effective_min_confidence(const cbm_httplink_config_t *cfg);
bool cbm_httplink_effective_fuzzy_matching(const cbm_httplink_config_t *cfg);

/* Build combined exclude paths: defaults + user-configured.
 * Writes up to max_out pointers into out[]. Pointers are valid until
 * config is freed. Returns total count. */
int cbm_httplink_all_exclude_paths(const cbm_httplink_config_t *cfg, const char **out, int max_out);

#endif /* CBM_HTTPLINK_H */
