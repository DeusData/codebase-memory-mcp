/*
 * httplink.c — HTTP route discovery and cross-service linking.
 *
 * Port of Go internal/httplink package.
 */

#include "pipeline/httplink.h"
#include "foundation/hash_table.h"
#include "foundation/yaml.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_regex.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Constants ─────────────────────────────────────────────────── */

/* Confidence band thresholds */
#define CONF_BAND_HIGH 0.7
#define CONF_BAND_MEDIUM 0.45
#define CONF_BAND_SPECULATIVE 0.25

/* Path match confidence */
#define CONF_PATH_EXACT 0.95
#define CONF_PATH_SUFFIX 0.75

/* Depth normalization factor */
#define DEPTH_DIVISOR 3.0

/* Default minimum confidence for filtering */
#define DEFAULT_MIN_CONFIDENCE 0.25

/* Buffer size guard: sizeof(buf) - 2 for trailing NUL + safety */
#define NORM_BUF_GUARD 1022
#define PARAM_BUF_GUARD 1020
#define HALF_BUF_GUARD 510

/* Receiver buffer max index */
#define RECEIVER_MAX 63

/* ── Similarity ────────────────────────────────────────────────── */

static int imin(int a, int b) {
    return a < b ? a : b;
}
static int imax(int a, int b) {
    return a > b ? a : b;
}

int cbm_levenshtein_distance(const char *a, const char *b) {
    int la = (int)strlen(a);
    int lb = (int)strlen(b);
    if (la == 0) {
        return lb;
    }
    if (lb == 0) {
        return la;
    }

    int *prev = calloc((size_t)lb + 1, sizeof(int));
    int *curr = calloc((size_t)lb + 1, sizeof(int));
    if (!prev || !curr) {
        free(prev);
        free(curr);
        return imax(la, lb);
    }

    for (int j = 0; j <= lb; j++) {
        prev[j] = j;
    }

    for (int i = 1; i <= la; i++) {
        curr[0] = i;
        for (int j = 1; j <= lb; j++) {
            int cost = (a[i - 1] == b[j - 1]) ? 0 : 1;
            curr[j] = imin(curr[j - 1] + 1, imin(prev[j] + 1, prev[j - 1] + cost));
        }
        int *tmp = prev;
        prev = curr;
        curr = tmp;
    }
    int result = prev[lb];
    free(prev);
    free(curr);
    return result;
}

double cbm_normalized_levenshtein(const char *a, const char *b) {
    if (strcmp(a, b) == 0) {
        return 1.0;
    }
    int la = (int)strlen(a);
    int lb = (int)strlen(b);
    int max_len = imax(la, lb);
    if (max_len == 0) {
        return 1.0;
    }
    int dist = cbm_levenshtein_distance(a, b);
    return 1.0 - ((double)dist / (double)max_len);
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void free_ht_key(const char *key, void *val, void *ud) {
    (void)val;
    (void)ud;
    free((void *)key);
}

double cbm_ngram_overlap(const char *a, const char *b, int n) {
    int la = (int)strlen(a);
    int lb = (int)strlen(b);
    if (la < n || lb < n) {
        return 0.0;
    }

    int na = la - n + 1;
    int nb = lb - n + 1;

    /* Build set A using hash table for O(1) dedup */
    CBMHashTable *set_a = cbm_ht_create(na);
    for (int i = 0; i < na; i++) {
        char key[64];
        int klen = imin(n, (int)sizeof(key) - 1);
        memcpy(key, a + i, (size_t)klen);
        key[klen] = '\0';
        if (!cbm_ht_get(set_a, key)) {
            // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by string.h
            cbm_ht_set(set_a, strdup(key), (void *)1);
        }
    }

    /* Build set B using hash table, count intersection with A */
    CBMHashTable *set_b = cbm_ht_create(nb);
    int intersection = 0;
    for (int i = 0; i < nb; i++) {
        char key[64];
        int klen = imin(n, (int)sizeof(key) - 1);
        memcpy(key, b + i, (size_t)klen);
        key[klen] = '\0';
        if (!cbm_ht_get(set_b, key)) {
            // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by string.h
            cbm_ht_set(set_b, strdup(key), (void *)1);
            if (cbm_ht_get(set_a, key)) {
                intersection++;
            }
        }
    }

    int unique_a = (int)cbm_ht_count(set_a);
    int unique_b = (int)cbm_ht_count(set_b);

    /* Free hash table keys */
    cbm_ht_foreach(set_a, free_ht_key, NULL);
    cbm_ht_free(set_a);
    cbm_ht_foreach(set_b, free_ht_key, NULL);
    cbm_ht_free(set_b);

    int min_size = imin(unique_a, unique_b);
    if (min_size == 0) {
        return 0.0;
    }
    return (double)intersection / (double)min_size;
}

const char *cbm_confidence_band(double score) {
    if (score >= CONF_BAND_HIGH) {
        return "high";
    }
    if (score >= CONF_BAND_MEDIUM) {
        return "medium";
    }
    if (score >= CONF_BAND_SPECULATIVE) {
        return "speculative";
    }
    return "";
}

/* ── Path matching ─────────────────────────────────────────────── */

const char *cbm_normalize_path(const char *input) {
    static CBM_TLS char buf[1024];
    if (!input || !*input) {
        buf[0] = '\0';
        return buf;
    }

    /* Copy and lowercase */
    char work[1024] = {0};
    int len = 0;
    for (int i = 0; input[i] && len < NORM_BUF_GUARD; i++) {
        work[len++] = (char)tolower((unsigned char)input[i]);
    }
    work[len] = '\0';

    /* Strip URL scheme + host: https://host/path → /path */
    char *path_start = work;
    char *scheme = strstr(work, "://");
    if (scheme) {
        path_start = scheme + 3;
        /* Skip host */
        while (*path_start && *path_start != '/') {
            path_start++;
        }
        if (!*path_start) {
            buf[0] = '\0';
            return buf;
        }
    }

    /* Strip trailing slash */
    int plen = (int)strlen(path_start);
    if (plen > 1 && path_start[plen - 1] == '/') {
        path_start[--plen] = '\0';
    }

    /* Replace :param and {param} with * */
    int out_len = 0;
    for (int i = 0; i < plen && out_len < PARAM_BUF_GUARD;) {
        if (path_start[i] == ':' && (i == 0 || path_start[i - 1] == '/')) {
            buf[out_len++] = '*';
            i++;
            while (i < plen && path_start[i] != '/') {
                i++;
            }
        } else if (path_start[i] == '{') {
            buf[out_len++] = '*';
            i++;
            while (i < plen && path_start[i] != '}') {
                i++;
            }
            if (i < plen) {
                i++;
            }
        } else {
            buf[out_len++] = path_start[i++];
        }
    }
    buf[out_len] = '\0';

    if (strcmp(buf, "/") == 0) {
        buf[0] = '\0';
    }

    return buf;
}

/* Normalize a path into a caller-owned buffer for comparison */
static void normalize_to_buf(const char *input, char *out, int out_size) {
    const char *norm = cbm_normalize_path(input);
    int len = (int)strlen(norm);
    if (len >= out_size) {
        len = out_size - 1;
    }
    memcpy(out, norm, (size_t)len);
    out[len] = '\0';
}

bool cbm_paths_match(const char *call_path, const char *route_path) {
    char norm_call[1024];
    char norm_route[1024];
    normalize_to_buf(call_path, norm_call, sizeof(norm_call));
    normalize_to_buf(route_path, norm_route, sizeof(norm_route));

    if (!*norm_call || !*norm_route) {
        return false;
    }

    /* Exact match */
    if (strcmp(norm_call, norm_route) == 0) {
        return true;
    }

    /* Suffix match: normalized call ends with normalized route */
    int lc = (int)strlen(norm_call);
    int lr = (int)strlen(norm_route);
    if (lc > lr && strcmp(norm_call + lc - lr, norm_route) == 0) {
        return true;
    }

    /* Segment-by-segment comparison with wildcard matching */
    /* Split both into segments */
    char call_copy[1024];
    char route_copy[1024];
    strncpy(call_copy, norm_call, sizeof(call_copy) - 1);
    call_copy[sizeof(call_copy) - 1] = '\0';
    strncpy(route_copy, norm_route, sizeof(route_copy) - 1);
    route_copy[sizeof(route_copy) - 1] = '\0';

    /* For suffix matching with segments: try matching from the end.
     * But first try direct segment comparison. */
    char *call_segs[64];
    char *route_segs[64];
    int nc = 0;
    int nr = 0;

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    char *tok = strtok(call_copy, "/");
    while (tok && nc < 64) {
        call_segs[nc++] = tok;
        // NOLINTNEXTLINE(concurrency-mt-unsafe)
        tok = strtok(NULL, "/");
    }

    /* Need to re-copy route since strtok consumed call_copy's context */
    normalize_to_buf(route_path, route_copy, sizeof(route_copy));
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    tok = strtok(route_copy, "/");
    while (tok && nr < 64) {
        route_segs[nr++] = tok;
        // NOLINTNEXTLINE(concurrency-mt-unsafe)
        tok = strtok(NULL, "/");
    }

    if (nc == nr) {
        bool match = true;
        for (int i = 0; i < nc; i++) {
            if (strcmp(call_segs[i], "*") == 0 || strcmp(route_segs[i], "*") == 0) {
                continue; /* wildcard matches anything */
            }
            if (strcmp(call_segs[i], route_segs[i]) != 0) {
                match = false;
                break;
            }
        }
        if (match) {
            return true;
        }
    }

    /* Suffix match at segment level */
    if (nc > nr) {
        int offset = nc - nr;
        bool match = true;
        for (int i = 0; i < nr; i++) {
            if (strcmp(call_segs[offset + i], "*") == 0 || strcmp(route_segs[i], "*") == 0) {
                continue;
            }
            if (strcmp(call_segs[offset + i], route_segs[i]) != 0) {
                match = false;
                break;
            }
        }
        if (match) {
            return true;
        }
    }

    return false;
}

/* Count segments in a normalized path */
static int count_segments(const char *path) {
    int count = 0;
    for (int i = 0; path[i]; i++) {
        if (path[i] == '/') {
            count++;
        }
    }
    return count;
}

static double segment_jaccard(const char *norm_call, const char *norm_route) {
    /* Split into segments */
    char a[1024];
    char b[1024];
    strncpy(a, norm_call, sizeof(a) - 1);
    a[sizeof(a) - 1] = '\0';
    strncpy(b, norm_route, sizeof(b) - 1);
    b[sizeof(b) - 1] = '\0';

    char *a_segs[64];
    char *b_segs[64];
    int na = 0;
    int nb = 0;

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    char *t = strtok(a, "/");
    while (t && na < 64) {
        a_segs[na++] = t;
        // NOLINTNEXTLINE(concurrency-mt-unsafe)
        t = strtok(NULL, "/");
    }

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    t = strtok(b, "/");
    while (t && nb < 64) {
        b_segs[nb++] = t;
        // NOLINTNEXTLINE(concurrency-mt-unsafe)
        t = strtok(NULL, "/");
    }

    if (na == 0 && nb == 0) {
        return 1.0;
    }
    if (na == 0 || nb == 0) {
        return 0.0;
    }

    /* Count intersection (match segments in order, accounting for wildcards) */
    int intersect = 0;
    int max_len = imax(na, nb);
    int min_len = imin(na, nb);
    for (int i = 0; i < min_len; i++) {
        if (strcmp(a_segs[i], b_segs[i]) == 0 || strcmp(a_segs[i], "*") == 0 ||
            strcmp(b_segs[i], "*") == 0) {
            intersect++;
        }
    }

    return (double)intersect / (double)max_len;
}

double cbm_path_match_score(const char *call_path, const char *route_path) {
    char norm_call[1024];
    char norm_route[1024];
    normalize_to_buf(call_path, norm_call, sizeof(norm_call));
    normalize_to_buf(route_path, norm_route, sizeof(norm_route));

    if (!*norm_call || !*norm_route) {
        return 0.0;
    }

    /* Determine match type and base score */
    double match_base = 0.0;
    bool is_suffix = false;

    if (strcmp(norm_call, norm_route) == 0) {
        match_base = CONF_PATH_EXACT;
    } else {
        int lc = (int)strlen(norm_call);
        int lr = (int)strlen(norm_route);

        /* Check suffix match */
        if (lc > lr && strcmp(norm_call + lc - lr, norm_route) == 0) {
            match_base = CONF_PATH_SUFFIX;
            is_suffix = true;
        } else {
            /* Segment-wise match with wildcards */
            char c2[1024];
            char r2[1024];
            strncpy(c2, norm_call, sizeof(c2) - 1);
            c2[sizeof(c2) - 1] = '\0';
            strncpy(r2, norm_route, sizeof(r2) - 1);
            r2[sizeof(r2) - 1] = '\0';

            char *cs[64];
            char *rs[64];
            int nc2 = 0;
            int nr2 = 0;
            // NOLINTNEXTLINE(concurrency-mt-unsafe)
            char *tk = strtok(c2, "/");
            while (tk && nc2 < 64) {
                cs[nc2++] = tk;
                // NOLINTNEXTLINE(concurrency-mt-unsafe)
                tk = strtok(NULL, "/");
            }
            // NOLINTNEXTLINE(concurrency-mt-unsafe)
            tk = strtok(r2, "/");
            while (tk && nr2 < 64) {
                rs[nr2++] = tk;
                // NOLINTNEXTLINE(concurrency-mt-unsafe)
                tk = strtok(NULL, "/");
            }

            if (nc2 == nr2) {
                bool all_match = true;
                for (int i = 0; i < nc2; i++) {
                    if (strcmp(cs[i], "*") != 0 && strcmp(rs[i], "*") != 0 &&
                        strcmp(cs[i], rs[i]) != 0) {
                        all_match = false;
                        break;
                    }
                }
                if (all_match) {
                    match_base = CONF_PATH_EXACT;
                }
            }
        }
    }

    if (match_base == 0.0) {
        return 0.0;
    }

    /* Compute confidence: 0.5 × jaccard + 0.5 × depthFactor */
    double jaccard = segment_jaccard(norm_call, norm_route);
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    int depth = count_segments(is_suffix ? norm_route : norm_call);
    double depth_factor = (double)depth / DEPTH_DIVISOR;
    if (depth_factor > 1.0) {
        depth_factor = 1.0;
    }

    double confidence = (0.5 * jaccard) + (0.5 * depth_factor);
    return match_base * confidence;
}

bool cbm_same_service(const char *qn1, const char *qn2) {
    /* Split QN by '.', strip last 2 segments (module+name), compare rest */
    char a[1024];
    char b[1024];
    strncpy(a, qn1, sizeof(a) - 1);
    a[sizeof(a) - 1] = '\0';
    strncpy(b, qn2, sizeof(b) - 1);
    b[sizeof(b) - 1] = '\0';

    /* Count segments */
    char *a_segs[64];
    char *b_segs[64];
    int na = 0;
    int nb = 0;

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    char *tok = strtok(a, ".");
    while (tok && na < 64) {
        a_segs[na++] = tok;
        // NOLINTNEXTLINE(concurrency-mt-unsafe)
        tok = strtok(NULL, ".");
    }

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    tok = strtok(b, ".");
    while (tok && nb < 64) {
        b_segs[nb++] = tok;
        // NOLINTNEXTLINE(concurrency-mt-unsafe)
        tok = strtok(NULL, ".");
    }

    /* Need at least 3 segments (dir + module + name) */
    if (na < 3 || nb < 3) {
        return false;
    }

    /* Strip last 2 segments */
    int da = na - 2;
    int db = nb - 2;

    if (da != db) {
        return false;
    }

    for (int i = 0; i < da; i++) {
        if (strcmp(a_segs[i], b_segs[i]) != 0) {
            return false;
        }
    }
    return true;
}

/* ── Detection ─────────────────────────────────────────────────── */

const char *cbm_detect_protocol(const char *source) {
    if (!source || !*source) {
        return "";
    }

    /* WebSocket patterns */
    if (strstr(source, "websocket.Upgrade") || strstr(source, "websocket.Accept") ||
        strstr(source, "upgrader.Upgrade") || strstr(source, "ws.on(") ||
        strstr(source, "io.on(")) {
        return "ws";
    }

    /* SSE patterns */
    if (strstr(source, "text/event-stream") || strstr(source, "EventSourceResponse") ||
        strstr(source, "SseEmitter") || strstr(source, "ServerSentEvent")) {
        return "sse";
    }

    return "";
}

/* containsTestSegment: check if path has a segment that equals testWord */
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static bool contains_test_segment(const char *fp, const char *test_word) {
    char work[1024];
    int len = (int)strlen(fp);
    if (len >= (int)sizeof(work)) {
        len = (int)sizeof(work) - 1;
    }
    memcpy(work, fp, (size_t)len);
    work[len] = '\0';

    /* Replace / with \0 and check each segment */
    char *p = work;
    while (*p) {
        char *seg = p;
        while (*p && *p != '/') {
            p++;
        }
        char saved = *p;
        *p = '\0';
        if (strcmp(seg, test_word) == 0) {
            return true;
        }
        if (saved) {
            *p = saved;
            p++;
        } else {
            { break; }
        }
    }
    return false;
}

bool cbm_is_test_node_fp(const char *file_path, bool is_test_prop) {
    if (is_test_prop) {
        return true;
    }
    if (!file_path || !*file_path) {
        return false;
    }

    /* Convert to forward slashes for consistent matching */
    char fp[1024];
    int len = 0;
    for (int i = 0; file_path[i] && len < NORM_BUF_GUARD; i++) {
        fp[len++] = (file_path[i] == '\\') ? '/' : file_path[i];
    }
    fp[len] = '\0';

    if (contains_test_segment(fp, "test")) {
        return true;
    }
    if (contains_test_segment(fp, "tests")) {
        return true;
    }
    if (contains_test_segment(fp, "__tests__")) {
        return true;
    }
    if (strstr(fp, "_test.")) {
        return true;
    }
    if (strstr(fp, ".test.")) {
        return true;
    }
    if (strstr(fp, "spec/")) {
        return true;
    }
    if (strstr(fp, "fixtures/")) {
        return true;
    }
    return false;
}

bool cbm_is_path_excluded(const char *path, const char **exclude_paths, int count) {
    if (!path || !*path) {
        return false;
    }

    /* Normalize: lowercase, strip trailing / */
    char norm[512];
    int len = 0;
    for (int i = 0; path[i] && len < HALF_BUF_GUARD; i++) {
        norm[len++] = (char)tolower((unsigned char)path[i]);
    }
    norm[len] = '\0';
    if (len > 1 && norm[len - 1] == '/') {
        norm[--len] = '\0';
    }

    for (int i = 0; i < count; i++) {
        /* Normalize exclude path too */
        char excl[512];
        int elen = 0;
        for (int j = 0; exclude_paths[i][j] && elen < HALF_BUF_GUARD; j++) {
            excl[elen++] = (char)tolower((unsigned char)exclude_paths[i][j]);
        }
        excl[elen] = '\0';
        if (elen > 1 && excl[elen - 1] == '/') {
            excl[--elen] = '\0';
        }

        if (strcmp(norm, excl) == 0) {
            return true;
        }
    }
    return false;
}

/* ── URL extraction ────────────────────────────────────────────── */

int cbm_extract_url_paths(const char *text, char **out, int max_out) {
    if (!text || !*text) {
        return 0;
    }

    int count = 0;
    cbm_regex_t url_re;
    cbm_regex_t path_re;

    /* URL pattern: https?://host/path */
    if (cbm_regcomp(&url_re, "https?://[a-zA-Z0-9.\\-]+(/[a-zA-Z0-9_/:.\\.\\-]+)",
                    CBM_REG_EXTENDED) != 0) {
        return 0;
    }

    /* Path pattern: "/path" (quoted paths starting with /) */
    if (cbm_regcomp(&path_re, "[\"'](/[a-zA-Z0-9_/:.\\.\\-]{2,})[\"']", CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&url_re);
        return 0;
    }

    /* Search for URL patterns */
    const char *p = text;
    cbm_regmatch_t match[2];

    while (count < max_out && cbm_regexec(&url_re, p, 2, match, 0) == 0) {
        int path_start = match[1].rm_so;
        int path_end = match[1].rm_eo;
        int path_len = path_end - path_start;
        if (path_len > 0 && path_len < 256) {
            out[count] = malloc((size_t)path_len + 1);
            memcpy(out[count], p + path_start, (size_t)path_len);
            out[count][path_len] = '\0';
            count++;
        }
        p += match[0].rm_eo;
    }

    /* Search for path-only patterns */
    p = text;
    while (count < max_out && cbm_regexec(&path_re, p, 2, match, 0) == 0) {
        int path_start = match[1].rm_so;
        int path_end = match[1].rm_eo;
        int path_len = path_end - path_start;
        if (path_len > 0 && path_len < 256) {
            /* Check not already captured as part of a URL */
            char *path_str = malloc((size_t)path_len + 1);
            memcpy(path_str, p + path_start, (size_t)path_len);
            path_str[path_len] = '\0';

            bool dup = false;
            for (int i = 0; i < count; i++) {
                if (strcmp(out[i], path_str) == 0) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                out[count++] = path_str;
            } else {
                free(path_str);
            }
        }
        p += match[0].rm_eo;
    }

    cbm_regfree(&url_re);
    cbm_regfree(&path_re);
    return count;
}

int cbm_extract_json_string_paths(const char *text, char **out, int max_out) {
    if (!text || !*text) {
        return 0;
    }

    /* Look for JSON-like strings containing URLs or paths */
    /* Extract all quoted strings and check for URL/path patterns */
    int count = 0;

    /* First try extracting URLs from the raw text (covers JSON string values) */
    count = cbm_extract_url_paths(text, out, max_out);

    return count;
}

/* ── Route extraction: Python ──────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
int cbm_extract_python_routes(const char *name, const char *qn, const char **decorators, int ndec,
                              cbm_route_handler_t *out, int max_out) {
    if (!decorators || ndec <= 0) {
        return 0;
    }

    cbm_regex_t py_route_re;
    cbm_regex_t py_ws_re;
    if (cbm_regcomp(&py_route_re,
                    "@[a-zA-Z_]+\\.(get|post|put|delete|patch)\\([[:space:]]*[\"']([^\"']*)[\"']",
                    CBM_REG_EXTENDED) != 0) {
        return 0;
    }
    if (cbm_regcomp(&py_ws_re, "@[a-zA-Z_]+\\.websocket\\([[:space:]]*[\"']([^\"']*)[\"']",
                    CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&py_route_re);
        return 0;
    }

    int count = 0;
    for (int i = 0; i < ndec && count < max_out; i++) {
        cbm_regmatch_t match[3];

        /* Try websocket first */
        if (cbm_regexec(&py_ws_re, decorators[i], 2, match, 0) == 0) {
            cbm_route_handler_t *r = &out[count];
            memset(r, 0, sizeof(*r));

            int plen = (match[1].rm_eo - match[1].rm_so);
            if (plen >= (int)sizeof(r->path)) {
                plen = (int)sizeof(r->path) - 1;
            }
            memcpy(r->path, decorators[i] + match[1].rm_so, (size_t)plen);
            r->path[plen] = '\0';

            strncpy(r->method, "WS", sizeof(r->method) - 1);
            strncpy(r->protocol, "ws", sizeof(r->protocol) - 1);
            strncpy(r->function_name, name ? name : "", sizeof(r->function_name) - 1);
            strncpy(r->qualified_name, qn ? qn : "", sizeof(r->qualified_name) - 1);
            count++;
            continue;
        }

        /* Try regular HTTP route */
        if (cbm_regexec(&py_route_re, decorators[i], 3, match, 0) == 0) {
            cbm_route_handler_t *r = &out[count];
            memset(r, 0, sizeof(*r));

            /* Method */
            int mlen = (match[1].rm_eo - match[1].rm_so);
            if (mlen >= (int)sizeof(r->method)) {
                mlen = (int)sizeof(r->method) - 1;
            }
            memcpy(r->method, decorators[i] + match[1].rm_so, (size_t)mlen);
            r->method[mlen] = '\0';
            /* Uppercase */
            for (int j = 0; r->method[j]; j++) {
                r->method[j] = (char)toupper((unsigned char)r->method[j]);
            }

            /* Path */
            int plen = (match[2].rm_eo - match[2].rm_so);
            if (plen >= (int)sizeof(r->path)) {
                plen = (int)sizeof(r->path) - 1;
            }
            memcpy(r->path, decorators[i] + match[2].rm_so, (size_t)plen);
            r->path[plen] = '\0';

            strncpy(r->function_name, name ? name : "", sizeof(r->function_name) - 1);
            strncpy(r->qualified_name, qn ? qn : "", sizeof(r->qualified_name) - 1);
            count++;
        }
    }

    cbm_regfree(&py_route_re);
    cbm_regfree(&py_ws_re);
    return count;
}

/* ── Route extraction: Go gin/chi ──────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
int cbm_extract_go_routes(const char *name, const char *qn, const char *source,
                          cbm_route_handler_t *out, int max_out) {
    if (!source || !*source) {
        return 0;
    }

    cbm_regex_t go_route_re;
    cbm_regex_t go_group_re;
    cbm_regex_t go_chi_re;

    if (cbm_regcomp(
            &go_route_re,
            "\\.(GET|POST|PUT|DELETE|PATCH|Get|Post|Put|Delete|Patch)\\([[:space:]]*[\"']([^\"'"
            "]*)[\"'][[:space:]]*,[[:space:]]*([^,)]+)",
            CBM_REG_EXTENDED) != 0) {
        return 0;
    }

    if (cbm_regcomp(
            &go_group_re,
            "([a-zA-Z_][a-zA-Z0-9_]*)[[:space:]]*(:=|=)[[:space:]]*[a-zA-Z_][a-zA-Z0-9_]*\\."
            "Group\\([[:space:]]*[\"']([^\"']+)[\"']",
            CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&go_route_re);
        return 0;
    }

    if (cbm_regcomp(&go_chi_re, "\\.Route\\([[:space:]]*\"([^\"]+)\"[[:space:]]*,[[:space:]]*func",
                    CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&go_route_re);
        cbm_regfree(&go_group_re);
        return 0;
    }

    /* ── Step 1: Collect gin Group() bindings (var → prefix) ──── */
    typedef struct {
        char var[64];
        char prefix[256];
    } gin_group_t;
    gin_group_t gin_groups[32];
    int ngin = 0;

    const char *sp = source;
    cbm_regmatch_t gm[4];
    while (cbm_regexec(&go_group_re, sp, 4, gm, 0) == 0 && ngin < 32) {
        int vlen = (gm[1].rm_eo - gm[1].rm_so);
        int plen = (gm[3].rm_eo - gm[3].rm_so);
        if (vlen < 64 && plen < 256) {
            memcpy(gin_groups[ngin].var, sp + gm[1].rm_so, (size_t)vlen);
            gin_groups[ngin].var[vlen] = '\0';
            memcpy(gin_groups[ngin].prefix, sp + gm[3].rm_so, (size_t)plen);
            gin_groups[ngin].prefix[plen] = '\0';
            ngin++;
        }
        sp += gm[0].rm_eo;
    }

    /* ── Step 2: Collect chi Route() matches with byte positions ── */
    typedef struct {
        int end_pos;
        char prefix[256];
    } chi_match_t;
    chi_match_t chi_matches[32];
    int nchi = 0;

    sp = source;
    cbm_regmatch_t cm[2];
    while (cbm_regexec(&go_chi_re, sp, 2, cm, 0) == 0 && nchi < 32) {
        int abs_end = (int)((sp - source) + cm[0].rm_eo);
        int plen = (cm[1].rm_eo - cm[1].rm_so);
        if (plen < 256) {
            chi_matches[nchi].end_pos = abs_end;
            memcpy(chi_matches[nchi].prefix, sp + cm[1].rm_so, (size_t)plen);
            chi_matches[nchi].prefix[plen] = '\0';
            nchi++;
        }
        sp += cm[0].rm_eo;
    }

    /* ── Step 3: Collect route matches with byte positions ─────── */
    typedef struct {
        int start_pos, end_pos;
        int method_so, method_eo;
        int path_so, path_eo;
        int handler_so, handler_eo; /* capture group 3: handler ref */
    } route_match_t;
    route_match_t route_matches[64];
    int nmatches = 0;

    sp = source;
    cbm_regmatch_t rm[4];
    while (cbm_regexec(&go_route_re, sp, 4, rm, 0) == 0 && nmatches < 64) {
        int base = (int)(sp - source);
        route_matches[nmatches].start_pos = (base + rm[0].rm_so);
        route_matches[nmatches].end_pos = (base + rm[0].rm_eo);
        route_matches[nmatches].method_so = (base + rm[1].rm_so);
        route_matches[nmatches].method_eo = (base + rm[1].rm_eo);
        route_matches[nmatches].path_so = (base + rm[2].rm_so);
        route_matches[nmatches].path_eo = (base + rm[2].rm_eo);
        route_matches[nmatches].handler_so = (rm[3].rm_so >= 0) ? (base + rm[3].rm_so) : -1;
        route_matches[nmatches].handler_eo = (rm[3].rm_eo >= 0) ? (base + rm[3].rm_eo) : -1;
        nmatches++;
        sp += rm[0].rm_eo;
    }

    /* ── Step 4: Compute chi prefix for each route via brace tracking ── */
    char route_chi_prefix[64][512];
    memset(route_chi_prefix, 0, sizeof(route_chi_prefix));

    if (nchi > 0 && nmatches > 0) {
        /* Single pass through source tracking brace depth + chi prefix stack.
         * When we pass a chi Route() match end, mark it pending.
         * When we see '{', if pending, push onto stack at current depth.
         * When we see '}', pop stack entries at current depth, then decrement.
         * At each route position, concat stack entries for the prefix. */

        struct {
            char prefix[256];
            int depth;
        } chi_stack[32];
        int chi_top = 0;
        int brace_depth = 0;
        int next_chi = 0;
        int next_route = 0;
        bool pending = false;
        char pending_prefix[256] = "";

        int src_len = (int)strlen(source);
        for (int i = 0; i <= src_len && next_route < nmatches; i++) {
            /* Check if we've passed a chi Route() match end */
            while (next_chi < nchi && i >= chi_matches[next_chi].end_pos) {
                pending = true;
                strncpy(pending_prefix, chi_matches[next_chi].prefix, sizeof(pending_prefix) - 1);
                pending_prefix[sizeof(pending_prefix) - 1] = '\0';
                next_chi++;
            }

            if (i < src_len && source[i] == '{') {
                brace_depth++;
                if (pending && chi_top < 32) {
                    strncpy(chi_stack[chi_top].prefix, pending_prefix,
                            sizeof(chi_stack[chi_top].prefix) - 1);
                    chi_stack[chi_top].prefix[sizeof(chi_stack[chi_top].prefix) - 1] = '\0';
                    chi_stack[chi_top].depth = brace_depth;
                    chi_top++;
                    pending = false;
                }
            } else if (i < src_len && source[i] == '}') {
                while (chi_top > 0 && chi_stack[chi_top - 1].depth == brace_depth) {
                    chi_top--;
                }
                brace_depth--;
            }

            /* Check if this position is a route match start */
            while (next_route < nmatches && i == route_matches[next_route].start_pos) {
                /* Build prefix from chi stack */
                route_chi_prefix[next_route][0] = '\0';
                for (int s = 0; s < chi_top; s++) {
                    int cur = (int)strlen(route_chi_prefix[next_route]);
                    int pf = (int)strlen(chi_stack[s].prefix);
                    if (cur + pf < HALF_BUF_GUARD) {
                        strcat(route_chi_prefix[next_route], chi_stack[s].prefix);
                    }
                }
                next_route++;
            }
        }
    }

    /* ── Step 5: Build output routes ───────────────────────────── */
    int count = 0;
    for (int ri = 0; ri < nmatches && count < max_out; ri++) {
        cbm_route_handler_t *r = &out[count];
        memset(r, 0, sizeof(*r));

        /* Method */
        int mlen = route_matches[ri].method_eo - route_matches[ri].method_so;
        if (mlen >= (int)sizeof(r->method)) {
            mlen = (int)sizeof(r->method) - 1;
        }
        memcpy(r->method, source + route_matches[ri].method_so, (size_t)mlen);
        r->method[mlen] = '\0';
        for (int j = 0; r->method[j]; j++) {
            r->method[j] = (char)toupper((unsigned char)r->method[j]);
        }

        /* Path */
        int plen = route_matches[ri].path_eo - route_matches[ri].path_so;
        if (plen >= (int)sizeof(r->path)) {
            plen = (int)sizeof(r->path) - 1;
        }
        memcpy(r->path, source + route_matches[ri].path_so, (size_t)plen);
        r->path[plen] = '\0';

        /* Find the receiver: look back for "varName." before .METHOD */
        const char *dot_pos = source + route_matches[ri].start_pos;
        char receiver[64] = "";
        if (dot_pos > source) {
            const char *rp = dot_pos - 1;
            while (rp >= source && (isalnum((unsigned char)*rp) || *rp == '_')) {
                rp--;
            }
            rp++;
            int rlen = (int)(dot_pos - rp);
            if (rlen > 0 && rlen < 64) {
                memcpy(receiver, rp, (size_t)rlen);
                receiver[rlen] = '\0';
            }
        }

        /* Apply gin group prefix if receiver matches a binding */
        bool gin_applied = false;
        for (int g = 0; g < ngin; g++) {
            if (strcmp(receiver, gin_groups[g].var) == 0) {
                char full_path[512];
                snprintf(full_path, sizeof(full_path), "%s%s", gin_groups[g].prefix, r->path);
                strncpy(r->path, full_path, sizeof(r->path) - 1);
                r->path[sizeof(r->path) - 1] = '\0';
                gin_applied = true;
                break;
            }
        }

        /* Apply chi prefix (only if gin group wasn't applied) */
        if (!gin_applied && route_chi_prefix[ri][0]) {
            char full_path[512];
            snprintf(full_path, sizeof(full_path), "%s%s", route_chi_prefix[ri], r->path);
            strncpy(r->path, full_path, sizeof(r->path) - 1);
            r->path[sizeof(r->path) - 1] = '\0';
        }

        strncpy(r->function_name, name ? name : "", sizeof(r->function_name) - 1);
        strncpy(r->qualified_name, qn ? qn : "", sizeof(r->qualified_name) - 1);

        /* Handler ref from capture group 3 (e.g., "h.CreateOrder") */
        if (route_matches[ri].handler_so >= 0) {
            int hlen = route_matches[ri].handler_eo - route_matches[ri].handler_so;
            /* Trim whitespace */
            const char *hs = source + route_matches[ri].handler_so;
            while (hlen > 0 && (hs[0] == ' ' || hs[0] == '\t')) {
                hs++;
                hlen--;
            }
            while (hlen > 0 && (hs[hlen - 1] == ' ' || hs[hlen - 1] == '\t' ||
                                hs[hlen - 1] == ')' || hs[hlen - 1] == '\n')) {
                hlen--;
            }
            if (hlen > 0 && hlen < (int)sizeof(r->handler_ref)) {
                memcpy(r->handler_ref, hs, (size_t)hlen);
                r->handler_ref[hlen] = '\0';
            }
        }

        count++;
    }

    cbm_regfree(&go_route_re);
    cbm_regfree(&go_group_re);
    cbm_regfree(&go_chi_re);
    return count;
}

/* ── Route extraction: Java Spring ─────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
int cbm_extract_java_routes(const char *name, const char *qn, const char **decorators, int ndec,
                            cbm_route_handler_t *out, int max_out) {
    if (!decorators || ndec <= 0) {
        return 0;
    }

    cbm_regex_t spring_re;
    cbm_regex_t spring_ws_re;
    if (cbm_regcomp(
            &spring_re,
            "@(Get|Post|Put|Delete|Patch|Request)Mapping\\([[:space:]]*(value[[:space:]]*=[[:"
            "space:]]*)?[\"']([^\"']+)[\"']",
            CBM_REG_EXTENDED) != 0) {
        return 0;
    }
    if (cbm_regcomp(&spring_ws_re, "@MessageMapping\\([[:space:]]*[\"']([^\"']+)[\"']",
                    CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&spring_re);
        return 0;
    }

    int count = 0;
    for (int i = 0; i < ndec && count < max_out; i++) {
        cbm_regmatch_t match[4];

        /* Try WebSocket first */
        if (cbm_regexec(&spring_ws_re, decorators[i], 2, match, 0) == 0) {
            cbm_route_handler_t *r = &out[count];
            memset(r, 0, sizeof(*r));

            int plen = (match[1].rm_eo - match[1].rm_so);
            if (plen >= (int)sizeof(r->path)) {
                plen = (int)sizeof(r->path) - 1;
            }
            memcpy(r->path, decorators[i] + match[1].rm_so, (size_t)plen);
            r->path[plen] = '\0';

            strncpy(r->method, "WS", sizeof(r->method) - 1);
            strncpy(r->protocol, "ws", sizeof(r->protocol) - 1);
            strncpy(r->function_name, name ? name : "", sizeof(r->function_name) - 1);
            strncpy(r->qualified_name, qn ? qn : "", sizeof(r->qualified_name) - 1);
            count++;
            continue;
        }

        /* Try Spring mapping */
        if (cbm_regexec(&spring_re, decorators[i], 4, match, 0) == 0) {
            cbm_route_handler_t *r = &out[count];
            memset(r, 0, sizeof(*r));

            /* Method from annotation name */
            int mlen = (match[1].rm_eo - match[1].rm_so);
            char method_name[32];
            if (mlen >= (int)sizeof(method_name)) {
                mlen = (int)sizeof(method_name) - 1;
            }
            memcpy(method_name, decorators[i] + match[1].rm_so, (size_t)mlen);
            method_name[mlen] = '\0';

            if (strcmp(method_name, "Get") == 0) {
                strncpy(r->method, "GET", sizeof(r->method) - 1);
            } else if (strcmp(method_name, "Post") == 0) {
                strncpy(r->method, "POST", sizeof(r->method) - 1);
            } else if (strcmp(method_name, "Put") == 0) {
                strncpy(r->method, "PUT", sizeof(r->method) - 1);
            } else if (strcmp(method_name, "Delete") == 0) {
                strncpy(r->method, "DELETE", sizeof(r->method) - 1);
            } else if (strcmp(method_name, "Patch") == 0) {
                strncpy(r->method, "PATCH", sizeof(r->method) - 1);
            } else {
                strncpy(r->method, "ANY", sizeof(r->method) - 1);
            }

            /* Path */
            int plen = (match[3].rm_eo - match[3].rm_so);
            if (plen >= (int)sizeof(r->path)) {
                plen = (int)sizeof(r->path) - 1;
            }
            memcpy(r->path, decorators[i] + match[3].rm_so, (size_t)plen);
            r->path[plen] = '\0';

            strncpy(r->function_name, name ? name : "", sizeof(r->function_name) - 1);
            strncpy(r->qualified_name, qn ? qn : "", sizeof(r->qualified_name) - 1);
            count++;
        }
    }

    cbm_regfree(&spring_re);
    cbm_regfree(&spring_ws_re);
    return count;
}

/* ── Route extraction: Kotlin Ktor ─────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
int cbm_extract_ktor_routes(const char *name, const char *qn, const char *source,
                            cbm_route_handler_t *out, int max_out) {
    if (!source || !*source) {
        return 0;
    }

    cbm_regex_t ktor_re;
    cbm_regex_t ktor_ws_re;
    if (cbm_regcomp(&ktor_re,
                    "(get|post|put|delete|patch)\\([[:space:]]*\"([^\"]+)\"[[:space:]]*\\)",
                    CBM_REG_EXTENDED) != 0) {
        return 0;
    }
    if (cbm_regcomp(&ktor_ws_re, "webSocket\\([[:space:]]*\"([^\"]+)\"[[:space:]]*\\)",
                    CBM_REG_EXTENDED) != 0) {
        cbm_regfree(&ktor_re);
        return 0;
    }

    int count = 0;
    const char *p = source;
    cbm_regmatch_t match[3];

    /* WebSocket routes */
    while (count < max_out && cbm_regexec(&ktor_ws_re, p, 2, match, 0) == 0) {
        cbm_route_handler_t *r = &out[count];
        memset(r, 0, sizeof(*r));

        int plen = (match[1].rm_eo - match[1].rm_so);
        if (plen >= (int)sizeof(r->path)) {
            plen = (int)sizeof(r->path) - 1;
        }
        memcpy(r->path, p + match[1].rm_so, (size_t)plen);
        r->path[plen] = '\0';

        strncpy(r->method, "WS", sizeof(r->method) - 1);
        strncpy(r->protocol, "ws", sizeof(r->protocol) - 1);
        strncpy(r->function_name, name ? name : "", sizeof(r->function_name) - 1);
        strncpy(r->qualified_name, qn ? qn : "", sizeof(r->qualified_name) - 1);
        count++;
        p += match[0].rm_eo;
    }

    /* HTTP routes */
    p = source;
    while (count < max_out && cbm_regexec(&ktor_re, p, 3, match, 0) == 0) {
        /* Make sure this isn't the webSocket match (check if preceded by "web") */
        const char *match_start = p + match[0].rm_so;
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        bool is_ws = (match_start >= source + 3 && strncmp(match_start - 3, "web", 3) == 0);

        if (!is_ws) {
            cbm_route_handler_t *r = &out[count];
            memset(r, 0, sizeof(*r));

            /* Method */
            int mlen = (match[1].rm_eo - match[1].rm_so);
            if (mlen >= (int)sizeof(r->method)) {
                mlen = (int)sizeof(r->method) - 1;
            }
            memcpy(r->method, p + match[1].rm_so, (size_t)mlen);
            r->method[mlen] = '\0';
            for (int j = 0; r->method[j]; j++) {
                r->method[j] = (char)toupper((unsigned char)r->method[j]);
            }

            /* Path */
            int plen = (match[2].rm_eo - match[2].rm_so);
            if (plen >= (int)sizeof(r->path)) {
                plen = (int)sizeof(r->path) - 1;
            }
            memcpy(r->path, p + match[2].rm_so, (size_t)plen);
            r->path[plen] = '\0';

            strncpy(r->function_name, name ? name : "", sizeof(r->function_name) - 1);
            strncpy(r->qualified_name, qn ? qn : "", sizeof(r->qualified_name) - 1);
            count++;
        }
        p += match[0].rm_eo;
    }

    cbm_regfree(&ktor_re);
    cbm_regfree(&ktor_ws_re);
    return count;
}

/* ── Route extraction: Express.js ──────────────────────────────── */

static bool is_express_receiver(const char *receiver) {
    static const char *allowlist[] = {"app",    "router",  "server", "api",
                                      "routes", "express", "route"};
    for (int i = 0; i < 7; i++) {
        if (strcmp(receiver, allowlist[i]) == 0) {
            return true;
        }
    }
    return false;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
int cbm_extract_express_routes(const char *name, const char *qn, const char *source,
                               cbm_route_handler_t *out, int max_out) {
    if (!source || !*source) {
        return 0;
    }

    cbm_regex_t express_re;
    if (cbm_regcomp(
            &express_re,
            "([a-zA-Z_][a-zA-Z0-9_]*)\\.(get|post|put|delete|patch)\\([[:space:]]*[\"'`]([^\"'`"
            "]+)[\"'`]",
            CBM_REG_EXTENDED) != 0) {
        return 0;
    }

    int count = 0;
    const char *p = source;
    cbm_regmatch_t match[4];

    while (count < max_out && cbm_regexec(&express_re, p, 4, match, 0) == 0) {
        /* Extract receiver */
        int rlen = (match[1].rm_eo - match[1].rm_so);
        char receiver[64];
        if (rlen >= 64) {
            rlen = RECEIVER_MAX;
        }
        memcpy(receiver, p + match[1].rm_so, (size_t)rlen);
        receiver[rlen] = '\0';

        if (is_express_receiver(receiver)) {
            cbm_route_handler_t *r = &out[count];
            memset(r, 0, sizeof(*r));

            /* Method */
            int mlen = (match[2].rm_eo - match[2].rm_so);
            if (mlen >= (int)sizeof(r->method)) {
                mlen = (int)sizeof(r->method) - 1;
            }
            memcpy(r->method, p + match[2].rm_so, (size_t)mlen);
            r->method[mlen] = '\0';
            for (int j = 0; r->method[j]; j++) {
                r->method[j] = (char)toupper((unsigned char)r->method[j]);
            }

            /* Path */
            int plen = (match[3].rm_eo - match[3].rm_so);
            if (plen >= (int)sizeof(r->path)) {
                plen = (int)sizeof(r->path) - 1;
            }
            memcpy(r->path, p + match[3].rm_so, (size_t)plen);
            r->path[plen] = '\0';

            strncpy(r->function_name, name ? name : "", sizeof(r->function_name) - 1);
            strncpy(r->qualified_name, qn ? qn : "", sizeof(r->qualified_name) - 1);
            count++;
        }
        p += match[0].rm_eo;
    }

    cbm_regfree(&express_re);
    return count;
}

/* ── Route extraction: Hapi.js ─────────────────────────────────── */

/* Extract a quoted string value after a colon, e.g. method: 'GET' → "GET".
 * Returns the number of chars consumed from `src` (0 on failure). */
static int hapi_extract_string_value(const char *src, char *out, int outsz) {
    const char *p = src;
    /* Skip whitespace after colon */
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    char quote = *p;
    if (quote != '\'' && quote != '"' && quote != '`') return 0;
    p++;
    const char *start = p;
    while (*p && *p != quote) p++;
    if (*p != quote) return 0;
    int len = (int)(p - start);
    if (len >= outsz) len = outsz - 1;
    memcpy(out, start, (size_t)len);
    out[len] = '\0';
    return (int)(p + 1 - src);
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
int cbm_extract_hapi_routes(const char *name, const char *qn, const char *source,
                            cbm_route_handler_t *out, int max_out) {
    if (!source || !*source) {
        return 0;
    }

    int count = 0;
    const char *p = source;

    /* Scan for object literals containing method: and path: properties.
     * Hapi pattern:
     *   { method: 'GET', path: '/api/users', handler: ... }
     * or:
     *   { method: 'POST', path: '/api/users', handler: UsersController.create }
     *
     * We look for "method:" followed by a string value, then scan nearby for
     * "path:" followed by a string value (or vice versa). */
    while (*p && count < max_out) {
        /* Find next "method:" or "method :" */
        const char *mkey = strstr(p, "method");
        if (!mkey) break;

        /* Verify it looks like a property key (preceded by space/newline/comma/brace) */
        if (mkey > source) {
            char before = *(mkey - 1);
            if (before != ' ' && before != '\t' && before != '\n' && before != '\r' &&
                before != ',' && before != '{') {
                p = mkey + 6;
                continue;
            }
        }

        const char *after_method = mkey + 6;
        /* Skip optional whitespace and colon */
        while (*after_method == ' ' || *after_method == '\t') after_method++;
        if (*after_method != ':') {
            p = after_method;
            continue;
        }
        after_method++; /* skip ':' */

        char method_val[16] = {0};
        int consumed = hapi_extract_string_value(after_method, method_val, sizeof(method_val));
        if (consumed == 0) {
            p = after_method;
            continue;
        }

        /* Uppercase the method */
        for (int j = 0; method_val[j]; j++) {
            method_val[j] = (char)toupper((unsigned char)method_val[j]);
        }

        /* Validate it's a real HTTP method */
        if (strcmp(method_val, "GET") != 0 && strcmp(method_val, "POST") != 0 &&
            strcmp(method_val, "PUT") != 0 && strcmp(method_val, "DELETE") != 0 &&
            strcmp(method_val, "PATCH") != 0 && strcmp(method_val, "OPTIONS") != 0 &&
            strcmp(method_val, "HEAD") != 0 && strcmp(method_val, "*") != 0) {
            p = after_method + consumed;
            continue;
        }

        /* Search for "path:" within the same object literal — look forward from the
         * method: position. Both method: and path: are in the same {...} block,
         * typically within 300 chars of each other. Also search a small window
         * backward in case path: comes before method: in the object. */
        const char *search_start = (mkey - 300 > source) ? mkey - 300 : source;
        const char *search_end_limit = mkey + 500;
        char path_val[256] = {0};
        bool found_path = false;

        /* Find the enclosing '{' to scope the search to this object literal */
        const char *obj_start = mkey;
        int brace_depth = 0;
        while (obj_start > source) {
            obj_start--;
            if (*obj_start == '{') {
                if (brace_depth == 0) break;
                brace_depth--;
            } else if (*obj_start == '}') {
                brace_depth++;
            }
        }
        if (*obj_start == '{') {
            search_start = obj_start;
        }

        const char *pkey = search_start;
        while ((pkey = strstr(pkey, "path")) != NULL && pkey < search_end_limit) {
            /* Verify it looks like a property key */
            if (pkey > source) {
                char pb = *(pkey - 1);
                if (pb != ' ' && pb != '\t' && pb != '\n' && pb != '\r' &&
                    pb != ',' && pb != '{') {
                    pkey += 4;
                    continue;
                }
            }
            const char *after_path = pkey + 4;
            while (*after_path == ' ' || *after_path == '\t') after_path++;
            if (*after_path != ':') {
                pkey += 4;
                continue;
            }
            after_path++;
            int pc = hapi_extract_string_value(after_path, path_val, sizeof(path_val));
            if (pc > 0 && path_val[0] == '/') {
                found_path = true;
                break;
            }
            pkey += 4;
        }

        if (found_path) {
            /* Optionally extract handler reference — scope to same object */
            char handler_val[256] = {0};
            const char *hkey = strstr(obj_start, "handler");
            while (hkey && hkey < search_end_limit) {
                /* Verify property key */
                if (hkey > source) {
                    char hb = *(hkey - 1);
                    if (hb != ' ' && hb != '\t' && hb != '\n' && hb != '\r' &&
                        hb != ',' && hb != '{') {
                        hkey = strstr(hkey + 7, "handler");
                        continue;
                    }
                }
                const char *after_handler = hkey + 7;
                while (*after_handler == ' ' || *after_handler == '\t') after_handler++;
                if (*after_handler == ':') {
                    after_handler++;
                    while (*after_handler == ' ' || *after_handler == '\t') after_handler++;
                    /* Handler can be identifier.identifier or just identifier */
                    const char *hs = after_handler;
                    while (*after_handler && *after_handler != ',' && *after_handler != '\n' &&
                           *after_handler != '}' && *after_handler != ' ') {
                        after_handler++;
                    }
                    int hlen = (int)(after_handler - hs);
                    if (hlen > 0 && hlen < (int)sizeof(handler_val)) {
                        memcpy(handler_val, hs, (size_t)hlen);
                        handler_val[hlen] = '\0';
                    }
                }
                break;
            }

            cbm_route_handler_t *r = &out[count];
            memset(r, 0, sizeof(*r));
            strncpy(r->method, method_val, sizeof(r->method) - 1);
            strncpy(r->path, path_val, sizeof(r->path) - 1);
            strncpy(r->function_name, name ? name : "", sizeof(r->function_name) - 1);
            strncpy(r->qualified_name, qn ? qn : "", sizeof(r->qualified_name) - 1);
            if (handler_val[0]) {
                strncpy(r->handler_ref, handler_val, sizeof(r->handler_ref) - 1);
            }
            count++;
        }

        p = after_method + consumed;
    }

    return count;
}

/* ── Route extraction: Laravel ─────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
int cbm_extract_laravel_routes(const char *name, const char *qn, const char *source,
                               cbm_route_handler_t *out, int max_out) {
    if (!source || !*source) {
        return 0;
    }

    cbm_regex_t laravel_re;
    if (cbm_regcomp(&laravel_re,
                    "Route::(get|post|put|delete|patch)\\([[:space:]]*[\"']([^\"']+)[\"']",
                    CBM_REG_EXTENDED) != 0) {
        return 0;
    }

    int count = 0;
    const char *p = source;
    cbm_regmatch_t match[3];

    while (count < max_out && cbm_regexec(&laravel_re, p, 3, match, 0) == 0) {
        cbm_route_handler_t *r = &out[count];
        memset(r, 0, sizeof(*r));

        /* Method */
        int mlen = (match[1].rm_eo - match[1].rm_so);
        if (mlen >= (int)sizeof(r->method)) {
            mlen = (int)sizeof(r->method) - 1;
        }
        memcpy(r->method, p + match[1].rm_so, (size_t)mlen);
        r->method[mlen] = '\0';
        for (int j = 0; r->method[j]; j++) {
            r->method[j] = (char)toupper((unsigned char)r->method[j]);
        }

        /* Path */
        int plen = (match[2].rm_eo - match[2].rm_so);
        if (plen >= (int)sizeof(r->path)) {
            plen = (int)sizeof(r->path) - 1;
        }
        memcpy(r->path, p + match[2].rm_so, (size_t)plen);
        r->path[plen] = '\0';

        /* Skip non-route strings that match the regex but contain characters
         * invalid in URL paths (e.g., cache keys, interpolated expressions).
         * Laravel route parameters use {param} syntax, so valid routes pass. */
        if (strchr(r->path, '$') || strchr(r->path, ':')) {
            p += match[0].rm_eo;
            continue;
        }

        strncpy(r->function_name, name ? name : "", sizeof(r->function_name) - 1);
        strncpy(r->qualified_name, qn ? qn : "", sizeof(r->qualified_name) - 1);
        count++;
        p += match[0].rm_eo;
    }

    cbm_regfree(&laravel_re);
    return count;
}

/* ── Read source lines ─────────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
char *cbm_read_source_lines_disk(const char *root_dir, const char *rel_path, int start_line,
                                 int end_line) {
    char full_path[2048];
    snprintf(full_path, sizeof(full_path), "%s/%s", root_dir, rel_path);

    FILE *f = fopen(full_path, "r");
    if (!f) {
        return NULL;
    }

    char *result = NULL;
    int result_len = 0;
    int result_cap = 0;
    int line = 0;
    char line_buf[4096];

    while (fgets(line_buf, sizeof(line_buf), f)) {
        line++;
        if (line < start_line) {
            continue;
        }
        if (line > end_line) {
            break;
        }

        int llen = (int)strlen(line_buf);
        /* Strip trailing newline */
        if (llen > 0 && line_buf[llen - 1] == '\n') {
            line_buf[--llen] = '\0';
        }

        /* Add separator between lines */
        if (result_len > 0) {
            if (result_len + 1 >= result_cap) {
                result_cap = (result_cap == 0) ? 1024 : result_cap * 2;
                result = safe_realloc(result, (size_t)result_cap);
            }
            result[result_len++] = '\n';
        }

        /* Add line content */
        if (result_len + llen >= result_cap) {
            result_cap = result_len + llen + 256;
            result = safe_realloc(result, (size_t)result_cap);
        }
        memcpy(result + result_len, line_buf, (size_t)llen);
        result_len += llen;
    }

    (void)fclose(f);
    if (result) {
        // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
        result[result_len] = '\0';
    }
    return result;
}

/* ── Read source lines from cached buffer ──────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
char *cbm_read_source_lines_cached(const char *source, int source_len, int start_line,
                                   int end_line) {
    if (!source || source_len <= 0 || start_line <= 0 || end_line < start_line) {
        return NULL;
    }

    char *result = NULL;
    int result_len = 0;
    int result_cap = 0;
    int line = 0;
    const char *p = source;
    const char *end = source + source_len;

    while (p < end) {
        line++;
        /* Find end of this line */
        const char *eol = memchr(p, '\n', (size_t)(end - p));
        int llen = eol ? (int)(eol - p) : (int)(end - p);

        if (line >= start_line && line <= end_line) {
            /* Strip trailing \r for CRLF files */
            int copy_len = llen;
            // cppcheck-suppress knownConditionTrueFalse
            if (copy_len > 0 && p[copy_len - 1] == '\r') {
                copy_len--;
            }

            /* Add separator between lines */
            if (result_len > 0) {
                if (result_len + 1 >= result_cap) {
                    result_cap = (result_cap == 0) ? 1024 : result_cap * 2;
                    result = safe_realloc(result, (size_t)result_cap);
                }
                result[result_len++] = '\n';
            }

            /* Add line content */
            if (result_len + copy_len >= result_cap) {
                result_cap = result_len + copy_len + 256;
                result = safe_realloc(result, (size_t)result_cap);
            }
            if (copy_len > 0) {
                memcpy(result + result_len, p, (size_t)copy_len);
                result_len += copy_len;
            }
        }

        if (line > end_line) {
            break;
        }

        p = eol ? eol + 1 : end;
    }

    if (result) {
        // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
        result[result_len] = '\0';
    }
    return result;
}

/* ── Config data ───────────────────────────────────────────────── */

const char *cbm_default_exclude_paths[] = {
    "/health",  "/healthz", "/ready",       "/readyz",      "/livez",
    "/metrics", "/debug",   "/favicon.ico", "/.well-known",
};
const int cbm_default_exclude_paths_count = 9;

const char *cbm_http_client_keywords[] = {
    "requests.get", "requests.post", "requests.put", "requests.delete", "httpx.",
    "aiohttp.",     "http.Get",      "http.Post",    "http.NewRequest", "fetch(",
    "axios.",       "HttpClient",    "RestTemplate", "reqwest::",       "hyper::",
    "curl_exec",    "Guzzle",        "sttp.",        "http4s",          "curl_easy",
    "cpr::Get",     "socket.http",   "http.request", "WebClient",       "RestClient",
    "OkHttpClient",
};
const int cbm_http_client_keywords_count = 26;

const char *cbm_async_dispatch_keywords[] = {
    "CreateTask", "create_task", "send_task", "send_message",
    "publish(",   "Publish(",    "SQS",       "EventBridge",
};
const int cbm_async_dispatch_keywords_count = 8;

/* ── Linker config ────────────────────────────────────────────── */

cbm_httplink_config_t cbm_httplink_default_config(void) {
    cbm_httplink_config_t cfg = {0};
    cfg.min_confidence = -1.0; /* sentinel: use default 0.25 */
    cfg.fuzzy_matching = -1;   /* sentinel: use default true */
    cfg.exclude_paths = NULL;
    cfg.exclude_path_count = 0;
    return cfg;
}

cbm_httplink_config_t cbm_httplink_load_config(const char *dir) {
    cbm_httplink_config_t cfg = cbm_httplink_default_config();
    if (!dir) {
        return cfg;
    }

    /* Build path: dir/.cgrconfig */
    char path[1024];
    int n = snprintf(path, sizeof(path), "%s/.cgrconfig", dir);
    if (n <= 0 || (size_t)n >= sizeof(path)) {
        return cfg;
    }

    /* Read file */
    FILE *f = fopen(path, "r");
    if (!f) {
        return cfg;
    }

    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);

    if (size <= 0 || size > (long)1024 * 1024) {
        (void)fclose(f);
        return cfg;
    }

    char *buf = malloc((size_t)size + 1);
    if (!buf) {
        (void)fclose(f);
        return cfg;
    }

    size_t nread = fread(buf, 1, (size_t)size, f);
    (void)fclose(f);
    // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
    buf[nread] = '\0';

    /* Parse YAML */
    cbm_yaml_node_t *root = cbm_yaml_parse(buf, (int)nread);
    free(buf);
    if (!root) {
        return cfg;
    }

    /* Extract min_confidence */
    if (cbm_yaml_has(root, "http_linker.min_confidence")) {
        cfg.min_confidence = cbm_yaml_get_float(root, "http_linker.min_confidence", -1.0);
    }

    /* Extract fuzzy_matching */
    if (cbm_yaml_has(root, "http_linker.fuzzy_matching")) {
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        cfg.fuzzy_matching = cbm_yaml_get_bool(root, "http_linker.fuzzy_matching", true) ? 1 : 0;
    }

    /* Extract exclude_paths */
    const char *items[128];
    int count = cbm_yaml_get_str_list(root, "http_linker.exclude_paths", items, 128);
    if (count > 0) {
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        cfg.exclude_paths = calloc((size_t)count, sizeof(char *));
        if (cfg.exclude_paths) {
            for (int i = 0; i < count; i++) {
                // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
                cfg.exclude_paths[i] = strdup(items[i]);
            }
            cfg.exclude_path_count = count;
        }
    }

    cbm_yaml_free(root);
    return cfg;
}

void cbm_httplink_config_free(cbm_httplink_config_t *cfg) {
    if (!cfg) {
        return;
    }
    for (int i = 0; i < cfg->exclude_path_count; i++) {
        free(cfg->exclude_paths[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(cfg->exclude_paths);
    cfg->exclude_paths = NULL;
    cfg->exclude_path_count = 0;
}

double cbm_httplink_effective_min_confidence(const cbm_httplink_config_t *cfg) {
    if (!cfg || cfg->min_confidence < 0) {
        return DEFAULT_MIN_CONFIDENCE;
    }
    return cfg->min_confidence;
}

bool cbm_httplink_effective_fuzzy_matching(const cbm_httplink_config_t *cfg) {
    if (!cfg || cfg->fuzzy_matching < 0) {
        return true;
    }
    return cfg->fuzzy_matching == 1;
}

int cbm_httplink_all_exclude_paths(const cbm_httplink_config_t *cfg, const char **out,
                                   int max_out) {
    int count = 0;

    /* Add defaults first */
    for (int i = 0; i < cbm_default_exclude_paths_count && count < max_out; i++) {
        out[count++] = cbm_default_exclude_paths[i];
    }

    /* Add user-configured paths */
    if (cfg) {
        for (int i = 0; i < cfg->exclude_path_count && count < max_out; i++) {
            out[count++] = cfg->exclude_paths[i];
        }
    }

    return count;
}

/* ── Channel extraction: Socket.IO / EventEmitter ────────────────── */

typedef struct cbm_channel_match {
    char channel[256];
    char direction[8]; /* "emit" or "listen" */
    char transport[32]; /* "socketio", "eventemitter" */
} cbm_channel_match_t;

int cbm_extract_channels(const char *source, cbm_channel_match_t *out, int max_out) {
    if (!source || !*source) return 0;

    cbm_regex_t re;
    if (cbm_regcomp(&re,
            "([a-zA-Z_][a-zA-Z0-9_]*)\\.("
            "emit|on|once|addListener|removeListener"
            ")\\([[:space:]]*['\"`]([^'\"`]{1,128})['\"`]",
            CBM_REG_EXTENDED) != 0) {
        return 0;
    }

    static const char *channel_receivers[] = {
        "socket", "io", "client", "server", "connection",
        "emitter", "eventEmitter", "eventBus", "bus", "pubsub",
        "producer", "consumer", "channel", "broker",
        "nsp", "namespace", "this", NULL
    };

    int count = 0;
    const char *p = source;
    cbm_regmatch_t match[4];

    while (count < max_out && cbm_regexec(&re, p, 4, match, 0) == 0) {
        int rlen = match[1].rm_eo - match[1].rm_so;
        char receiver[64];
        if (rlen >= (int)sizeof(receiver)) rlen = (int)sizeof(receiver) - 1;
        memcpy(receiver, p + match[1].rm_so, (size_t)rlen);
        receiver[rlen] = '\0';

        bool is_channel = false;
        for (int i = 0; channel_receivers[i]; i++) {
            if (strcasecmp(receiver, channel_receivers[i]) == 0) {
                is_channel = true;
                break;
            }
        }

        if (is_channel) {
            int mlen = match[2].rm_eo - match[2].rm_so;
            char method[32];
            if (mlen >= (int)sizeof(method)) mlen = (int)sizeof(method) - 1;
            memcpy(method, p + match[2].rm_so, (size_t)mlen);
            method[mlen] = '\0';

            int clen = match[3].rm_eo - match[3].rm_so;
            if (clen >= (int)sizeof(out[count].channel))
                clen = (int)sizeof(out[count].channel) - 1;
            memcpy(out[count].channel, p + match[3].rm_so, (size_t)clen);
            out[count].channel[clen] = '\0';

            const char *ch = out[count].channel;
            if (strcmp(ch, "error") != 0 && strcmp(ch, "close") != 0 &&
                strcmp(ch, "end") != 0 && strcmp(ch, "data") != 0 &&
                strcmp(ch, "connect") != 0 && strcmp(ch, "disconnect") != 0 &&
                strcmp(ch, "connection") != 0 && strcmp(ch, "message") != 0 &&
                strcmp(ch, "open") != 0 && strcmp(ch, "drain") != 0 &&
                strcmp(ch, "finish") != 0 && strcmp(ch, "pipe") != 0 &&
                strcmp(ch, "unpipe") != 0 && strcmp(ch, "readable") != 0 &&
                strcmp(ch, "resume") != 0 && strcmp(ch, "pause") != 0) {
                if (strcmp(method, "emit") == 0) {
                    strncpy(out[count].direction, "emit", sizeof(out[count].direction) - 1);
                } else {
                    strncpy(out[count].direction, "listen", sizeof(out[count].direction) - 1);
                }
                if (strcasecmp(receiver, "socket") == 0 || strcasecmp(receiver, "io") == 0 ||
                    strcasecmp(receiver, "nsp") == 0 || strcasecmp(receiver, "namespace") == 0) {
                    strncpy(out[count].transport, "socketio", sizeof(out[count].transport) - 1);
                } else {
                    strncpy(out[count].transport, "eventemitter", sizeof(out[count].transport) - 1);
                }
                count++;
            }
        }
        p += match[0].rm_eo;
    }

    cbm_regfree(&re);
    return count;
}

/* ── C# channel extraction: Socket.IO with constant resolution ─── */

/* Extract channels from C# source that uses constant names for event strings.
 * Pattern: _socket.Emit(CONSTANT_NAME, data) / _socket.OnRequest<T>(CONSTANT_NAME, ...)
 * Resolves constants via: const string CONSTANT_NAME = "ActualChannelName"; */
int cbm_extract_csharp_channels(const char *source, cbm_channel_match_t *out, int max_out) {
    if (!source || !*source) return 0;

    /* Pass 1: Collect const string mappings: name → value */
    typedef struct { char name[128]; char value[256]; } const_map_t;
    const_map_t cmap[128];
    int cmap_count = 0;

    cbm_regex_t re_const;
    if (cbm_regcomp(&re_const,
            "const[[:space:]]+string[[:space:]]+([A-Z_][A-Z_0-9]*)[[:space:]]*=[[:space:]]*\"([^\"]{1,128})\"",
            CBM_REG_EXTENDED) == 0) {
        const char *p = source;
        cbm_regmatch_t cm[3];
        while (cmap_count < 128 && cbm_regexec(&re_const, p, 3, cm, 0) == 0) {
            int nlen = cm[1].rm_eo - cm[1].rm_so;
            int vlen = cm[2].rm_eo - cm[2].rm_so;
            if (nlen > 0 && nlen < 128 && vlen > 0 && vlen < 256) {
                memcpy(cmap[cmap_count].name, p + cm[1].rm_so, (size_t)nlen);
                cmap[cmap_count].name[nlen] = '\0';
                memcpy(cmap[cmap_count].value, p + cm[2].rm_so, (size_t)vlen);
                cmap[cmap_count].value[vlen] = '\0';
                cmap_count++;
            }
            p += cm[0].rm_eo;
        }
        cbm_regfree(&re_const);
    }

    /* Pass 2: Find .Emit( and .OnRequest patterns */
    int count = 0;

    /* Pattern: .Emit(IDENTIFIER  or  .OnRequest<...>(IDENTIFIER */
    cbm_regex_t re_emit;
    if (cbm_regcomp(&re_emit,
            "\\.(Emit|OnRequest)[^(]*\\([[:space:]]*([A-Z_][A-Z_0-9]*)",
            CBM_REG_EXTENDED) == 0) {
        const char *p = source;
        cbm_regmatch_t em[3];
        while (count < max_out && cbm_regexec(&re_emit, p, 3, em, 0) == 0) {
            int mlen = em[1].rm_eo - em[1].rm_so;
            char method[16];
            if (mlen >= (int)sizeof(method)) mlen = (int)sizeof(method) - 1;
            memcpy(method, p + em[1].rm_so, (size_t)mlen);
            method[mlen] = '\0';

            int ilen = em[2].rm_eo - em[2].rm_so;
            char ident[128];
            if (ilen >= (int)sizeof(ident)) ilen = (int)sizeof(ident) - 1;
            memcpy(ident, p + em[2].rm_so, (size_t)ilen);
            ident[ilen] = '\0';

            /* Resolve constant to string value */
            const char *resolved = NULL;
            for (int i = 0; i < cmap_count; i++) {
                if (strcmp(cmap[i].name, ident) == 0) {
                    resolved = cmap[i].value;
                    break;
                }
            }

            if (resolved) {
                strncpy(out[count].channel, resolved, sizeof(out[count].channel) - 1);
                out[count].channel[sizeof(out[count].channel) - 1] = '\0';

                if (strcmp(method, "Emit") == 0) {
                    strncpy(out[count].direction, "emit", sizeof(out[count].direction) - 1);
                } else {
                    strncpy(out[count].direction, "listen", sizeof(out[count].direction) - 1);
                }
                strncpy(out[count].transport, "socketio", sizeof(out[count].transport) - 1);
                count++;
            }
            p += em[0].rm_eo;
        }
        cbm_regfree(&re_emit);
    }

    /* Also match direct string literal patterns: .Emit("ChannelName" */
    cbm_regex_t re_literal;
    if (cbm_regcomp(&re_literal,
            "\\.(Emit|On|OnRequest)[^(]*\\([[:space:]]*\"([^\"]{1,128})\"",
            CBM_REG_EXTENDED) == 0) {
        const char *p = source;
        cbm_regmatch_t lm[3];
        while (count < max_out && cbm_regexec(&re_literal, p, 3, lm, 0) == 0) {
            int mlen = lm[1].rm_eo - lm[1].rm_so;
            char method[16];
            if (mlen >= (int)sizeof(method)) mlen = (int)sizeof(method) - 1;
            memcpy(method, p + lm[1].rm_so, (size_t)mlen);
            method[mlen] = '\0';

            int clen = lm[2].rm_eo - lm[2].rm_so;
            strncpy(out[count].channel, p + lm[2].rm_so, (size_t)(clen < 255 ? clen : 255));
            out[count].channel[clen < 255 ? clen : 255] = '\0';

            if (strcmp(method, "Emit") == 0) {
                strncpy(out[count].direction, "emit", sizeof(out[count].direction) - 1);
            } else {
                strncpy(out[count].direction, "listen", sizeof(out[count].direction) - 1);
            }
            strncpy(out[count].transport, "socketio", sizeof(out[count].transport) - 1);
            count++;
            p += lm[0].rm_eo;
        }
        cbm_regfree(&re_literal);
    }

    return count;
}
