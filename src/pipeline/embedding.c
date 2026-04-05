/*
 * embedding.c — Semantic embedding generation via HTTP API + RRF hybrid search.
 *
 * Uses Mongoose for synchronous HTTP POST to OpenAI-compatible /v1/embeddings.
 * Uses yyjson for JSON serialization/deserialization.
 */

#include "pipeline/embedding.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat.h"

#include <mongoose/mongoose.h>
#include <yyjson/yyjson.h>
#include <sqlite3/sqlite3.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Thread-local int-to-string buffer for log key-value pairs. */
static _Thread_local char _itoa_buf[32];
static const char *itoa_buf(int val) {
    snprintf(_itoa_buf, sizeof(_itoa_buf), "%d", val);
    return _itoa_buf;
}

/* ── Configuration ──────────────────────────────────────────────── */

cbm_embedding_config_t cbm_embedding_get_config(void) {
    cbm_embedding_config_t cfg = {0};
    cfg.url = getenv("CBM_EMBEDDING_URL");
    cfg.model = getenv("CBM_EMBEDDING_MODEL");
    if (!cfg.model) cfg.model = "nomic-embed-text";

    const char *dims_str = getenv("CBM_EMBEDDING_DIMS");
    cfg.dims = dims_str ? atoi(dims_str) : 768;
    if (cfg.dims <= 0) cfg.dims = 768;

    const char *batch_str = getenv("CBM_EMBEDDING_BATCH_SIZE");
    cfg.batch_size = batch_str ? atoi(batch_str) : 32;
    if (cfg.batch_size <= 0) cfg.batch_size = 32;

    cfg.timeout_ms = 30000;
    return cfg;
}

bool cbm_embedding_is_configured(void) {
    const char *url = getenv("CBM_EMBEDDING_URL");
    return url && url[0];
}

/* ── HTTP embedding client (Mongoose synchronous) ──────────────── */

/* State for the synchronous HTTP request. */
typedef struct {
    bool done;
    bool error;
    char *response_body;
    int response_len;
    const char *url;         /* original URL for building the request */
    const char *content_type;
    const char *body;
    bool request_sent;
} http_state_t;

static void http_handler(struct mg_connection *c, int ev, void *ev_data) {
    http_state_t *state = (http_state_t *)c->fn_data;

    if (ev == MG_EV_CONNECT) {
        /* Connection established — send the HTTP request */
        struct mg_str host = mg_url_host(state->url);
        mg_printf(c,
                  "POST %s HTTP/1.1\r\n"
                  "Host: %.*s\r\n"
                  "Content-Type: %s\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  mg_url_uri(state->url),
                  (int)host.len, host.buf,
                  state->content_type,
                  (int)strlen(state->body),
                  state->body);
        state->request_sent = true;
    } else if (ev == MG_EV_HTTP_MSG) {
        struct mg_http_message *hm = (struct mg_http_message *)ev_data;
        state->response_body = malloc((size_t)hm->body.len + 1);
        if (state->response_body) {
            memcpy(state->response_body, hm->body.buf, hm->body.len);
            state->response_body[hm->body.len] = '\0';
            state->response_len = (int)hm->body.len;
        }
        state->done = true;
        c->is_draining = 1;
    } else if (ev == MG_EV_ERROR) {
        state->error = true;
        state->done = true;
        c->is_draining = 1;
    }
}

/* Synchronous HTTP POST. Returns allocated response body or NULL on error.
 * Caller must free the returned string. */
static char *http_post_sync(const char *url, const char *content_type,
                            const char *body, int timeout_ms) {
    struct mg_mgr mgr;
    mg_mgr_init(&mgr);

    http_state_t state = {0};
    state.url = url;
    state.content_type = content_type;
    state.body = body;

    struct mg_connection *c = mg_http_connect(&mgr, url, http_handler, &state);
    if (!c) {
        mg_mgr_free(&mgr);
        return NULL;
    }

    /* Poll until done or timeout */
    int elapsed = 0;
    while (!state.done && elapsed < timeout_ms) {
        mg_mgr_poll(&mgr, 50);
        elapsed += 50;
    }

    mg_mgr_free(&mgr);

    if (state.error || !state.done) {
        free(state.response_body);
        return NULL;
    }
    return state.response_body;
}

/* ── Embedding API calls ───────────────────────────────────────── */

/* Build the JSON request body for /v1/embeddings.
 * {"model": "...", "input": ["text1", "text2", ...]} */
static char *build_embedding_request(const char *model, const char **texts, int count) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "model", model);

    yyjson_mut_val *input = yyjson_mut_arr(doc);
    for (int i = 0; i < count; i++) {
        yyjson_mut_arr_add_str(doc, input, texts[i]);
    }
    yyjson_mut_obj_add_val(doc, root, "input", input);

    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

/* Parse the JSON response from /v1/embeddings.
 * Returns allocated float[count * dims] or NULL on error. */
static float *parse_embedding_response(const char *json, int expected_count,
                                       int expected_dims) {
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) return NULL;

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *data = yyjson_obj_get(root, "data");
    if (!data || !yyjson_is_arr(data)) {
        yyjson_doc_free(doc);
        return NULL;
    }

    int arr_len = (int)yyjson_arr_size(data);
    if (arr_len != expected_count) {
        cbm_log_error("embedding.parse", "msg", "count_mismatch",
                      "expected", itoa_buf(expected_count),
                      "got", itoa_buf(arr_len));
        yyjson_doc_free(doc);
        return NULL;
    }

    float *result = calloc((size_t)(expected_count * expected_dims), sizeof(float));
    if (!result) {
        yyjson_doc_free(doc);
        return NULL;
    }

    size_t idx, max;
    yyjson_val *item;
    yyjson_arr_foreach(data, idx, max, item) {
        /* Each item: {"embedding": [...], "index": N} */
        yyjson_val *emb = yyjson_obj_get(item, "embedding");
        yyjson_val *index_val = yyjson_obj_get(item, "index");
        if (!emb || !yyjson_is_arr(emb)) continue;

        int emb_idx = index_val ? (int)yyjson_get_int(index_val) : (int)idx;
        if (emb_idx < 0 || emb_idx >= expected_count) continue;

        int emb_dims = (int)yyjson_arr_size(emb);
        if (emb_dims != expected_dims) {
            /* Dimension mismatch — first occurrence, log and bail */
            if (idx == 0) {
                cbm_log_error("embedding.parse", "msg", "dims_mismatch",
                              "expected", itoa_buf(expected_dims),
                              "got", itoa_buf(emb_dims));
            }
            /* Still try to copy what we can */
            emb_dims = emb_dims < expected_dims ? emb_dims : expected_dims;
        }

        float *dest = &result[emb_idx * expected_dims];
        size_t ei, emax;
        yyjson_val *val;
        int d = 0;
        yyjson_arr_foreach(emb, ei, emax, val) {
            if (d >= expected_dims) break;
            dest[d++] = (float)yyjson_get_real(val);
        }
    }

    yyjson_doc_free(doc);
    return result;
}

float *cbm_embedding_embed_text(const cbm_embedding_config_t *cfg, const char *text) {
    if (!cfg || !cfg->url || !text) return NULL;
    const char *texts[] = {text};
    return cbm_embedding_embed_batch(cfg, texts, 1);
}

float *cbm_embedding_embed_batch(const cbm_embedding_config_t *cfg,
                                 const char **texts, int count) {
    if (!cfg || !cfg->url || !texts || count <= 0) return NULL;

    /* Build URL: base_url + "/embeddings" */
    char url[1024];
    snprintf(url, sizeof(url), "%s/embeddings", cfg->url);

    /* Build JSON request */
    char *request_json = build_embedding_request(cfg->model, texts, count);
    if (!request_json) return NULL;

    /* HTTP POST */
    char *response = http_post_sync(url, "application/json",
                                    request_json, cfg->timeout_ms);
    free(request_json);

    if (!response) {
        cbm_log_error("embedding.http", "msg", "request_failed", "url", url);
        return NULL;
    }

    /* Parse response */
    float *embeddings = parse_embedding_response(response, count, cfg->dims);
    free(response);

    return embeddings;
}

/* ── Text generation ───────────────────────────────────────────── */

char *cbm_embedding_node_text(const cbm_node_t *node) {
    if (!node || !node->name) return NULL;

    /* Extract directory from file_path */
    char dir[256] = "";
    if (node->file_path) {
        const char *last_slash = strrchr(node->file_path, '/');
        if (last_slash && last_slash > node->file_path) {
            int dlen = (int)(last_slash - node->file_path);
            if (dlen >= (int)sizeof(dir)) dlen = (int)sizeof(dir) - 1;
            memcpy(dir, node->file_path, (size_t)dlen);
            dir[dlen] = '\0';
        }
    }

    /* Extract filename from file_path */
    const char *filename = node->file_path;
    if (filename) {
        const char *last_slash = strrchr(filename, '/');
        if (last_slash) filename = last_slash + 1;
    }

    /* Extract code snippet from properties JSON (first 500 chars) */
    char snippet[512] = "";
    if (node->properties_json && node->properties_json[0] != '{') {
        /* properties_json IS the code sometimes */
    } else if (node->properties_json) {
        yyjson_doc *pdoc = yyjson_read(node->properties_json,
                                       strlen(node->properties_json), 0);
        if (pdoc) {
            yyjson_val *proot = yyjson_doc_get_root(pdoc);
            yyjson_val *code = yyjson_obj_get(proot, "code");
            if (!code) code = yyjson_obj_get(proot, "content");
            if (!code) code = yyjson_obj_get(proot, "signature");
            if (code && yyjson_is_str(code)) {
                const char *s = yyjson_get_str(code);
                if (s) {
                    int slen = (int)strlen(s);
                    if (slen > 500) slen = 500;
                    memcpy(snippet, s, (size_t)slen);
                    snippet[slen] = '\0';
                }
            }
            yyjson_doc_free(pdoc);
        }
    }

    /* Build: "Label: name\nFile: filename\nDirectory: dir\n\nsnippet" */
    int buf_size = 2048;
    char *buf = malloc((size_t)buf_size);
    if (!buf) return NULL;

    int len = snprintf(buf, (size_t)buf_size,
                       "%s: %s\nFile: %s\nDirectory: %s",
                       node->label ? node->label : "Symbol",
                       node->name,
                       filename ? filename : "",
                       dir[0] ? dir : "");

    if (snippet[0]) {
        len += snprintf(buf + len, (size_t)(buf_size - len), "\n\n%s", snippet);
    }

    return buf;
}

/* ── RRF merge ─────────────────────────────────────────────────── */

int cbm_embedding_rrf_merge(const int64_t *bm25_ids, int bm25_count,
                            const cbm_vector_result_t *vec_results, int vec_count,
                            cbm_rrf_result_t **out, int *out_count) {
    if (!out || !out_count) return CBM_STORE_ERR;
    *out = NULL;
    *out_count = 0;

    /* Estimate max unique results */
    int max_results = bm25_count + vec_count;
    if (max_results == 0) return CBM_STORE_OK;

    cbm_rrf_result_t *results = calloc((size_t)max_results, sizeof(cbm_rrf_result_t));
    if (!results) return CBM_STORE_ERR;

    int count = 0;

    /* Add BM25 results with RRF scores */
    for (int i = 0; i < bm25_count; i++) {
        double rrf_score = 1.0 / (CBM_RRF_K + i);
        /* Check if already in results (shouldn't happen for BM25) */
        results[count].node_id = bm25_ids[i];
        results[count].rrf_score = rrf_score;
        results[count].bm25_rank = i;
        results[count].vec_rank = -1;
        results[count].similarity = 0;
        count++;
    }

    /* Add vector results, merging with existing BM25 results */
    for (int i = 0; i < vec_count; i++) {
        double rrf_score = 1.0 / (CBM_RRF_K + i);
        int64_t nid = vec_results[i].node_id;

        /* Check if this node_id already exists from BM25 */
        bool found = false;
        for (int j = 0; j < count; j++) {
            if (results[j].node_id == nid) {
                results[j].rrf_score += rrf_score;
                results[j].vec_rank = i;
                results[j].similarity = vec_results[i].similarity;
                found = true;
                break;
            }
        }

        if (!found) {
            results[count].node_id = nid;
            results[count].rrf_score = rrf_score;
            results[count].bm25_rank = -1;
            results[count].vec_rank = i;
            results[count].similarity = vec_results[i].similarity;
            count++;
        }
    }

    /* Sort by RRF score descending */
    for (int i = 0; i < count - 1; i++) {
        for (int j = i + 1; j < count; j++) {
            if (results[j].rrf_score > results[i].rrf_score) {
                cbm_rrf_result_t tmp = results[i];
                results[i] = results[j];
                results[j] = tmp;
            }
        }
    }

    *out = results;
    *out_count = count;
    return CBM_STORE_OK;
}

/* ── Pipeline integration ──────────────────────────────────────── */

int cbm_embedding_generate_for_project(cbm_store_t *s, const char *project, bool force) {
    if (!s || !project) return -1;

    cbm_embedding_config_t cfg = cbm_embedding_get_config();
    if (!cfg.url) {
        cbm_log_info("embedding.skip", "reason", "not_configured");
        return 0;
    }

    /* Query embeddable nodes */
    const char *sql = force
        ? "SELECT id, project, label, name, qualified_name, file_path, "
          "start_line, end_line, properties FROM nodes "
          "WHERE project = ?1 "
          "AND label IN ('Function','Method','Class','Interface','Route')"
        : "SELECT id, project, label, name, qualified_name, file_path, "
          "start_line, end_line, properties FROM nodes "
          "WHERE project = ?1 "
          "AND label IN ('Function','Method','Class','Interface','Route') "
          "AND id NOT IN (SELECT node_id FROM embeddings WHERE project = ?1)";

    sqlite3_stmt *stmt = NULL;
    struct sqlite3 *db = cbm_store_get_db(s);
    if (!db) return -1;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, project, -1, SQLITE_STATIC);

    /* Collect nodes into batches */
    int total_embedded = 0;
    int batch_cap = cfg.batch_size;
    int64_t *batch_ids = malloc((size_t)batch_cap * sizeof(int64_t));
    const char **batch_texts = malloc((size_t)batch_cap * sizeof(char *));
    int batch_count = 0;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        cbm_node_t node = {0};
        node.id = sqlite3_column_int64(stmt, 0);
        node.project = (const char *)sqlite3_column_text(stmt, 1);
        node.label = (const char *)sqlite3_column_text(stmt, 2);
        node.name = (const char *)sqlite3_column_text(stmt, 3);
        node.qualified_name = (const char *)sqlite3_column_text(stmt, 4);
        node.file_path = (const char *)sqlite3_column_text(stmt, 5);
        node.start_line = sqlite3_column_int(stmt, 6);
        node.end_line = sqlite3_column_int(stmt, 7);
        node.properties_json = (const char *)sqlite3_column_text(stmt, 8);

        char *text = cbm_embedding_node_text(&node);
        if (!text) continue;

        batch_ids[batch_count] = node.id;
        batch_texts[batch_count] = text;
        batch_count++;

        /* Flush batch when full */
        if (batch_count >= batch_cap) {
            float *embeddings = cbm_embedding_embed_batch(&cfg, batch_texts, batch_count);
            if (embeddings) {
                cbm_store_upsert_embedding_batch(s, batch_ids, project,
                                                 embeddings, cfg.dims, batch_count);
                total_embedded += batch_count;
                free(embeddings);
            } else {
                cbm_log_error("embedding.batch", "msg", "failed",
                              "batch_size", itoa_buf(batch_count));
            }

            /* Free batch texts */
            for (int i = 0; i < batch_count; i++) {
                free((void *)batch_texts[i]);
            }
            batch_count = 0;
        }
    }

    /* Flush remaining batch */
    if (batch_count > 0) {
        float *embeddings = cbm_embedding_embed_batch(&cfg, batch_texts, batch_count);
        if (embeddings) {
            cbm_store_upsert_embedding_batch(s, batch_ids, project,
                                             embeddings, cfg.dims, batch_count);
            total_embedded += batch_count;
            free(embeddings);
        }
        for (int i = 0; i < batch_count; i++) {
            free((void *)batch_texts[i]);
        }
    }

    sqlite3_finalize(stmt);
    free(batch_ids);
    free(batch_texts);

    cbm_log_info("embedding.done", "project", project,
                 "embedded", itoa_buf(total_embedded));
    return total_embedded;
}
