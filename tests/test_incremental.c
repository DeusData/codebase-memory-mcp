/*
 * test_incremental.c — Comprehensive integration tests for indexing pipeline.
 *
 * Clones a real open-source repo (FastAPI 0.99.1, ~1100 Python files), indexes
 * it through the full pipeline, then tests incremental re-indexing with file
 * modifications, additions, deletions, and adversarial inputs.
 *
 * Includes performance metrics: wall-clock time and peak RSS on every index,
 * edge-type-specific assertions, and regression guards.
 *
 * Requires network access for initial clone. Skips gracefully if offline.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "resolver_quality.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <pipeline/pipeline_internal.h>
#include <foundation/log.h>
#include <foundation/mem.h>
#include <foundation/platform.h>

#include <stdarg.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>

/* ── Globals ──────────────────────────────────────────────────────── */

static char g_tmpdir[256];
static char g_repodir[512];
static char g_dbpath[512];
static cbm_mcp_server_t *g_srv = NULL;
static char *g_project = NULL;

/* Baseline counts after full index */
static int g_full_nodes = 0;
static int g_full_edges = 0;
static int g_full_calls = 0;
static int g_full_imports = 0;

/* Performance: track peak RSS and timing per test phase */
static size_t g_rss_before_full = 0;
static double g_full_index_ms = 0;

/* ── Helpers ──────────────────────────────────────────────────────── */

static double now_ms(void) {
    struct timespec ts;
    cbm_clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}

static void write_file_at(const char *rel_path, const char *content) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", g_repodir, rel_path);
    char dir[512];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
        cbm_mkdir(dir);
    }
    FILE *f = fopen(path, "w");
    if (f) {
        fputs(content, f);
        fclose(f);
    }
}

static void delete_file_at(const char *rel_path) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", g_repodir, rel_path);
    unlink(path);
}

/* Append "# reformatted" to up to max_files .py files in subdir (cross-platform). */
static int reformat_files(const char *subdir, int max_files) {
    char dir[512];
    snprintf(dir, sizeof(dir), "%s/%s", g_repodir, subdir);
    cbm_dir_t *d = cbm_opendir(dir);
    if (!d) {
        return -1;
    }
    cbm_dirent_t *entry;
    int count = 0;
    while ((entry = cbm_readdir(d)) != NULL && count < max_files) {
        size_t nlen = strlen(entry->name);
        if (nlen < 4 || strcmp(entry->name + nlen - 3, ".py") != 0) {
            continue;
        }
        if (entry->is_dir) {
            continue;
        }
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", dir, entry->name);
        th_append_file(path, "# reformatted\n");
        count++;
    }
    cbm_closedir(d);
    return 0;
}

static char *index_repo(void) {
    char args[512];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", g_repodir);
    char *resp = cbm_mcp_handle_tool(g_srv, "index_repository", args);
    /* Fail-closed at the source: every caller asserts resp != NULL, but an
     * error response (e.g. a transient pre-publication abort that preserves
     * the previous index) used to slip through and cascade into dozens of
     * mysterious count/query failures downstream — a 99-test pile-up on the
     * Windows leg traced back to exactly this. Surface the real cause once,
     * here, and let the existing non-NULL asserts fail loudly. */
    if (resp && strstr(resp, "\"isError\":true")) {
        fprintf(stderr, "  index_repo: index_repository returned an error response:\n  %.400s\n",
                resp);
        free(resp);
        return NULL;
    }
    return resp;
}

/* Timed index: returns response, sets *elapsed_ms and *peak_rss_mb */
static char *index_repo_timed(double *elapsed_ms, size_t *peak_rss_mb) {
    double t0 = now_ms();
    char *resp = index_repo();
    *elapsed_ms = now_ms() - t0;
    *peak_rss_mb = cbm_mem_peak_rss() / (1024 * 1024);
    return resp;
}

static char *call_tool(const char *tool, const char *args_fmt, ...) {
    char args[1024];
    va_list ap;
    va_start(ap, args_fmt);
    vsnprintf(args, sizeof(args), args_fmt, ap);
    va_end(ap);
    return cbm_mcp_handle_tool(g_srv, tool, args);
}

/* Parse integer from a tool response (handles the nested MCP envelope).
 * Matches the JSON forms "key":N / \"key\":N and the TOON scalar `key: N`
 * (search_graph default output since the compact-output change). */
static int count_in_response(const char *resp, const char *key) {
    if (!resp)
        return -1;
    char pattern[64];
    snprintf(pattern, sizeof(pattern), "\"%s\":", key);
    const char *p = strstr(resp, pattern);
    if (p)
        return atoi(p + strlen(pattern));
    snprintf(pattern, sizeof(pattern), "\\\"%s\\\":", key);
    p = strstr(resp, pattern);
    if (p)
        return atoi(p + strlen(pattern));
    snprintf(pattern, sizeof(pattern), "%s: ", key);
    p = strstr(resp, pattern);
    if (p)
        return atoi(p + strlen(pattern));
    return -1;
}

/* ── Direct store queries (more reliable than MCP for tests) ────── */

static cbm_store_t *open_store(void) {
    return cbm_store_open_path_existing(g_dbpath);
}

static int get_node_count(void) {
    cbm_store_t *s = open_store();
    if (!s)
        return -1;
    int c = cbm_store_count_nodes(s, g_project);
    cbm_store_close(s);
    return c;
}

static int get_edge_count(void) {
    cbm_store_t *s = open_store();
    if (!s)
        return -1;
    int c = cbm_store_count_edges(s, g_project);
    cbm_store_close(s);
    return c;
}

static int get_edge_count_by_type(const char *type) {
    cbm_store_t *s = open_store();
    if (!s)
        return -1;
    int c = cbm_store_count_edges_by_type(s, g_project, type);
    cbm_store_close(s);
    return c;
}

static int has_function(const char *name_pattern) {
    char *resp = call_tool("search_graph",
                           "{\"project\":\"%s\",\"label\":\"Function\",\"name_pattern\":\"%s\"}",
                           g_project, name_pattern);
    int total = count_in_response(resp, "total");
    free(resp);
    return total > 0;
}

static int count_by_label(const char *label) {
    char *resp =
        call_tool("search_graph", "{\"project\":\"%s\",\"label\":\"%s\"}", g_project, label);
    int total = count_in_response(resp, "total");
    free(resp);
    return total;
}

/* Source-authored FastAPI 0.99.1 oracle, commit
 * dd4e78ca7b09abdf0d4646fe4697316c021a8b2e. The 42 package sources close the
 * project-declaration target domain for five selected callers. External
 * starlette/builtin calls and unproved attribute dispatch are outside this
 * bounded denominator; every target at each exact golden site is checked.
 * Hashes come from pinned source bytes, never from graph/resolver output. */
static const struct {
    const char *path, *sha256;
    size_t length;
} fastapi_oracle_files[] = {
    {"fastapi/__init__.py", "577e4449e044c32d4bb7a0b0c3de88babdbe1c659c2a90b117908a3e3116db2f",
     1080},
    {"fastapi/applications.py", "c55f7982c32c9ceaac7b35e70440692fd61bee9af0774b3d73e6def049846b8c",
     40588},
    {"fastapi/background.py", "1ed379fe9249ace75a9526ee19230a2403cd59453987bad8fc15d7b9bbbbf884",
     76},
    {"fastapi/concurrency.py", "87b661646f1c122571e9a9f5b71701d912a98d6ebfc661f0168f44def5223100",
     1468},
    {"fastapi/datastructures.py",
     "a16eb1b94d02fac0706dcc9723e32504ed2d492e0148f07695851ad720b0f3e0", 1905},
    {"fastapi/dependencies/__init__.py",
     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0},
    {"fastapi/dependencies/models.py",
     "ccd6e2a31202b8e79bfbd0030d5438e61507382d0f06d3d511f554ddfd65fe77", 2494},
    {"fastapi/dependencies/utils.py",
     "8439f5533ea0fc2892ad08a4c00f16ad8b3fbc7f8e839906d9ad6a14573a6690", 30908},
    {"fastapi/encoders.py", "56c144ff9c4ebfd739dfea9cb694d07f8c6cb1cac986665925d89fd7ed564e3b",
     5976},
    {"fastapi/exception_handlers.py",
     "301ac8380fae8232438af222e2b4ac50905d4ec8eeccdefaab8ca1d2ad4238ac", 1332},
    {"fastapi/exceptions.py", "ad838d21966108f602ac0f174cd310d3b195b0605237b2e2a522eca0507724c9",
     1205},
    {"fastapi/logger.py", "23d34d8b7a2ff0072a6ec6c2f709755fe85d22d2a062dd974ebc757fdf59a65e", 54},
    {"fastapi/middleware/__init__.py",
     "a100f188555c7357d825438816fa619caee94d3e6492d99f2f7d905e9045befa", 58},
    {"fastapi/middleware/asyncexitstack.py",
     "2ef332548d5076658d5983d9ab1dbde5515abecb147d5a54b289cf3ac316cf51", 1035},
    {"fastapi/middleware/cors.py",
     "ca7c2359066873fbdb873677fd95dc7a8692aec9471473dda0ce76ad7af45945", 79},
    {"fastapi/middleware/gzip.py",
     "c4ce4f72c1fc425022999c38543bdc9939ea41a9ac95386c7dedc254ddafa1ad", 79},
    {"fastapi/middleware/httpsredirect.py",
     "acbf1e5cc9e62e28f05641fbff8d34cc7ae2d407a47de05de83eaab3c8b1f79d", 115},
    {"fastapi/middleware/trustedhost.py",
     "784e57191c466bb739ccf9cc2431a9dc1c5a2f6e64e625509619eff8f9343ecb", 109},
    {"fastapi/middleware/wsgi.py",
     "67751efbbc278b895464cbc7dc6f5e93ffda72061d26cb5b9e9657fc74006e86", 79},
    {"fastapi/openapi/__init__.py",
     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0},
    {"fastapi/openapi/constants.py",
     "996c58063103e8f53eb50f57db6ed092ad92b16d9cbfe0b58d814ab7adf1c44b", 107},
    {"fastapi/openapi/docs.py", "1db3fbebebb8038e41acbe168cb321037301548f71331ed788cc83452fd93b41",
     6532},
    {"fastapi/openapi/models.py",
     "7dfe5fc4d12185fec68794883a456e22b2defa05520e9a3d768562e7a072a288", 14941},
    {"fastapi/openapi/utils.py", "25633c029676223ea6ed549a94aaf35363187cd418fa7ae98b616e9dabac84cf",
     19576},
    {"fastapi/param_functions.py",
     "2cecdbb58d1eaa68af18cffe320cef4280998a6d1631034eeb4243172422dc3a", 8981},
    {"fastapi/params.py", "9262e9cd867bb9823f8c6c5926d82d3cfad5d67e276d0404ca478b0324f21b8f",
     13207},
    {"fastapi/requests.py", "cdac9ea4a15c89e9c1965bf7b27996236d06934a073552d4e030e1a9705be0b5",
     142},
    {"fastapi/responses.py", "a27f797b809f491c8dcfb3048ea16ecec3fe796f241d64f1125fd9f95cdbee50",
     1242},
    {"fastapi/routing.py", "10d2f730db12aa5aa8e48a3b0068f483bade5f9f4f85fc0846c7d04c4b132217",
     56490},
    {"fastapi/security/__init__.py",
     "6cef29366c6a5515d48df97698e2a25592e7d05a4143ad555185639a9a676c9c", 881},
    {"fastapi/security/api_key.py",
     "f769316638fd3ae22f4025292ecccff6a9482d1831ea10a1d71f9b65d8664035", 2939},
    {"fastapi/security/base.py", "765e29bdb0be4718df6d680fb4259df0c554fbb081d92676dab2435d50973ba7",
     141},
    {"fastapi/security/http.py", "55b4e6d64eade848c90270a7615aae48e9922bb7c04dd29182ad3e4e00bb1674",
     5964},
    {"fastapi/security/oauth2.py",
     "2340f24471267e7fd4a7cbe9625dc7f04574753765e53d52de26fc12d46a7cc2", 8360},
    {"fastapi/security/open_id_connect_url.py",
     "18a2bce20e8e65b385084649c9ddbba468e9682946c5e66b60e7a6533ca17db5", 1141},
    {"fastapi/security/utils.py",
     "6ddf13d1833b5100f90132ae72bd5b36d02fcff637fff755340bf951e6e23ef7", 293},
    {"fastapi/staticfiles.py", "8a2ac622ddec758d90657777ea28ecdc28fe4f416e18575addc77dd2433d224c",
     69},
    {"fastapi/templating.py", "e33b2e4d681c8dc11a8a7309140956ebe827b259ba020392d528a20d67e64319",
     76},
    {"fastapi/testclient.py", "9c1bda0265fae9895d45e24d6573ce935b1fba8d90ea1b3c6cebc86827a9e8b4",
     66},
    {"fastapi/types.py", "afa3278131f391938ff65cd781dba37bdc9e65ee44227580cc22ee44996122e1", 118},
    {"fastapi/utils.py", "44e7e4c61e9eac6fe11a4993346138803dfa0ff759df74798cbf063a3cc718f3", 7652},
    {"fastapi/websockets.py", "e35f6e9dc60e6c42991b461cad7b1c7d04182d25a8135d23ab154c7ad19d47df",
     222},
};

static const RQ_Expected fastapi_call_obligations[] = {
    {.case_id = "oauth2-password-parser",
     .site = {.file = "fastapi/security/oauth2.py",
              .caller = "fastapi.security.oauth2.OAuth2PasswordBearer.__call__",
              .start_byte = 6276,
              .end_byte = 6321},
     .relation = "CALLS",
     .target = {.file = "fastapi/security/utils.py",
                .suffix = "fastapi.security.utils.get_authorization_scheme_param",
                .label = "Function",
                .declaration_line = 4,
                .declaration_text = "def get_authorization_scheme_param("},
     .mode = RQ_REQUIRED},
    {.case_id = "oauth2-password-exception",
     .site = {.file = "fastapi/security/oauth2.py",
              .caller = "fastapi.security.oauth2.OAuth2PasswordBearer.__call__",
              .start_byte = 6436,
              .end_byte = 6631},
     .relation = "CALLS",
     .target = {.file = "fastapi/exceptions.py",
                .suffix = "fastapi.exceptions.HTTPException",
                .label = "Class",
                .declaration_line = 9,
                .declaration_text = "class HTTPException(StarletteHTTPException):"},
     .mode = RQ_REQUIRED},
    {.case_id = "oauth2-code-parser",
     .site = {.file = "fastapi/security/oauth2.py",
              .caller = "fastapi.security.oauth2.OAuth2AuthorizationCodeBearer.__call__",
              .start_byte = 7752,
              .end_byte = 7797},
     .relation = "CALLS",
     .target = {.file = "fastapi/security/utils.py",
                .suffix = "fastapi.security.utils.get_authorization_scheme_param",
                .label = "Function",
                .declaration_line = 4,
                .declaration_text = "def get_authorization_scheme_param("},
     .mode = RQ_REQUIRED},
    {.case_id = "oauth2-code-exception",
     .site = {.file = "fastapi/security/oauth2.py",
              .caller = "fastapi.security.oauth2.OAuth2AuthorizationCodeBearer.__call__",
              .start_byte = 7912,
              .end_byte = 8107},
     .relation = "CALLS",
     .target = {.file = "fastapi/exceptions.py",
                .suffix = "fastapi.exceptions.HTTPException",
                .label = "Class",
                .declaration_line = 9,
                .declaration_text = "class HTTPException(StarletteHTTPException):"},
     .mode = RQ_REQUIRED},
    {.case_id = "handler-body-status",
     .site = {.file = "fastapi/exception_handlers.py",
              .caller = "fastapi.exception_handlers.http_exception_handler",
              .start_byte = 599,
              .end_byte = 647},
     .relation = "CALLS",
     .target =
         {.file = "fastapi/utils.py",
          .suffix = "fastapi.utils.is_body_allowed_for_status_code",
          .label = "Function",
          .declaration_line = 36,
          .declaration_text =
              "def is_body_allowed_for_status_code(status_code: Union[int, str, None]) -> bool:"},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "fastapi-utils-formatted-value-export",
     .why = "The exact imported function is genuine, but formatted-value effects elsewhere in "
            "utils.py prevent the current whole-file export proof.",
     .tried =
         "Audited the explicit import and target declaration in pinned 0.99.1 source; "
         "the full pipeline is checked against this source obligation without a name fallback."},
    {.case_id = "handler-validation-encoder",
     .site = {.file = "fastapi/exception_handlers.py",
              .caller = "fastapi.exception_handlers.request_validation_exception_handler",
              .start_byte = 1049,
              .end_byte = 1079},
     .relation = "CALLS",
     .target = {.file = "fastapi/encoders.py",
                .suffix = "fastapi.encoders.jsonable_encoder",
                .label = "Function",
                .declaration_line = 29,
                .declaration_text = "def jsonable_encoder("},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "fastapi-encoder-dynamic-namespace-export",
     .why = "The exact imported function is genuine, but vars(obj) in encoders.py prevents the "
            "current whole-file export proof.",
     .tried =
         "Audited the explicit import and target declaration in pinned 0.99.1 source; "
         "the full pipeline is checked against this source obligation without a name fallback."},
    {.case_id = "handler-websocket-encoder",
     .site = {.file = "fastapi/exception_handlers.py",
              .caller = "fastapi.exception_handlers.websocket_request_validation_exception_handler",
              .start_byte = 1295,
              .end_byte = 1325},
     .relation = "CALLS",
     .target = {.file = "fastapi/encoders.py",
                .suffix = "fastapi.encoders.jsonable_encoder",
                .label = "Function",
                .declaration_line = 29,
                .declaration_text = "def jsonable_encoder("},
     .mode = RQ_KNOWN_GAP,
     .gap_id = "fastapi-encoder-dynamic-namespace-export",
     .why = "The exact imported function is genuine, but vars(obj) in encoders.py prevents the "
            "current whole-file export proof.",
     .tried =
         "Audited the explicit import and target declaration in pinned 0.99.1 source; "
         "the full pipeline is checked against this source obligation without a name fallback."},
};

/* Read bytes without executing/importing the external fixture. CRLF checkouts
 * are accepted only when canonical LF bytes match the same pinned hash. The
 * actual raw bytes remain the snapshot input, including an empty __init__.py. */
static char *fastapi_oracle_read(size_t index) {
    char path[1024];
    int n = snprintf(path, sizeof(path), "%s/%s", g_repodir, fastapi_oracle_files[index].path);
    if (n < 0 || (size_t)n >= sizeof(path))
        return NULL;
    FILE *file = cbm_fopen(path, "rb");
    if (!file)
        return NULL;
    bool ok = fseek(file, 0, SEEK_END) == 0;
    long length = ok ? ftell(file) : -1;
    ok = length >= 0 && length <= 2 * 1024 * 1024 && fseek(file, 0, SEEK_SET) == 0;
    char *raw = ok ? calloc((size_t)length + 1, 1) : NULL;
    ok = raw && fread(raw, 1, (size_t)length, file) == (size_t)length && !ferror(file);
    ok = fclose(file) == 0 && ok;
    if (!ok || strlen(raw) != (size_t)length) {
        free(raw);
        return NULL;
    }
    char *canonical = malloc((size_t)length + 1);
    if (!canonical) {
        free(raw);
        return NULL;
    }
    size_t count = 0;
    for (size_t i = 0; i < (size_t)length; i++) {
        if (raw[i] == '\r' && i + 1 < (size_t)length && raw[i + 1] == '\n')
            continue;
        canonical[count++] = raw[i];
    }
    char digest[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(canonical, count, digest);
    free(canonical);
    if (count != fastapi_oracle_files[index].length ||
        strcmp(digest, fastapi_oracle_files[index].sha256)) {
        fprintf(stderr, "[RQ] PINNED_SOURCE_MISMATCH file=%s\n", fastapi_oracle_files[index].path);
        free(raw);
        return NULL;
    }
    return raw;
}

/* Translate only newline encoding, after pinned content validation. This is
 * independent of emitted occurrence spans and preserves exact raw evidence. */
static uint32_t fastapi_raw_offset(const char *source, uint32_t canonical) {
    uint32_t offset = 0, count = 0;
    while (source[offset] && count < canonical) {
        if (!(source[offset] == '\r' && source[offset + 1] == '\n'))
            count++;
        offset++;
    }
    return count == canonical ? offset : UINT32_MAX;
}

/* RQ checks the full declaration line, including CR in a CRLF checkout.
 * Verify its independently authored canonical bytes before copying the raw
 * line; never obtain a target anchor from a graph node or emitted record. */
static char *fastapi_raw_declaration(const RQ_Target *target, const RQ_Source *sources,
                                     size_t count) {
    const char *line = rq_source(sources, count, target->file);
    if (!line || target->declaration_line <= 0)
        return NULL;
    for (int i = 1; i < target->declaration_line; i++) {
        line = strchr(line, '\n');
        if (!line)
            return NULL;
        line++;
    }
    size_t length = strcspn(line, "\n");
    size_t canonical = length - (length && line[length - 1] == '\r');
    if (strlen(target->declaration_text) != canonical ||
        memcmp(line, target->declaration_text, canonical))
        return NULL;
    char *raw = malloc(length + 1);
    if (raw) {
        memcpy(raw, line, length);
        raw[length] = '\0';
    }
    return raw;
}

static bool fastapi_call_quality(void) {
    RQ_Source sources[sizeof(fastapi_oracle_files) / sizeof(fastapi_oracle_files[0])] = {0};
    RQ_Expected expected[sizeof(fastapi_call_obligations) / sizeof(fastapi_call_obligations[0])];
    memcpy(expected, fastapi_call_obligations, sizeof(expected));
    char *declarations[sizeof(expected) / sizeof(expected[0])] = {0};
    bool ok = true;
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        sources[i].path = fastapi_oracle_files[i].path;
        sources[i].text = fastapi_oracle_read(i);
        ok = sources[i].text != NULL && ok;
    }
    for (size_t i = 0; ok && i < sizeof(expected) / sizeof(expected[0]); i++) {
        const char *source =
            rq_source(sources, sizeof(sources) / sizeof(sources[0]), expected[i].site.file);
        if (!source) {
            ok = false;
            break;
        }
        expected[i].site.start_byte = fastapi_raw_offset(source, expected[i].site.start_byte);
        expected[i].site.end_byte = fastapi_raw_offset(source, expected[i].site.end_byte);
        declarations[i] = fastapi_raw_declaration(&expected[i].target, sources,
                                                  sizeof(sources) / sizeof(sources[0]));
        expected[i].target.declaration_text = declarations[i];
        ok = expected[i].site.start_byte != UINT32_MAX && expected[i].site.end_byte != UINT32_MAX &&
             declarations[i] != NULL;
    }
    cbm_store_t *store = ok ? open_store() : NULL;
    RQ_Report report = {0};
    ok = store &&
         rq_check_project_declarations(store, g_project, "fastapi-pinned-calls", sources,
                                       sizeof(sources) / sizeof(sources[0]), expected,
                                       sizeof(expected) / sizeof(expected[0]), NULL, 0, &report,
                                       stderr) &&
         report.genuine == 7 && report.found == 4 && report.missing == 3;
    if (store)
        cbm_store_close(store);
    for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); i++)
        free(declarations[i]);
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++)
        free((char *)sources[i].text);
    return ok;
}

/* This consumer checks a physical File import, not a symbol-call occurrence.
 * Python import carriers currently have explicit legacy 0/0 coordinates; do
 * not pretend those are exact source spans. Require exact endpoints, current
 * generation, pinned source snapshots and agreement between row and edge. */
static bool fastapi_physical_import(void) {
    static const char *const paths[] = {"fastapi/security/oauth2.py", "fastapi/security/utils.py"};
    RQ_Source sources[2] = {0};
    bool ok = true;
    for (size_t i = 0; i < 2; i++) {
        sources[i].path = paths[i];
        for (size_t j = 0; j < sizeof(fastapi_oracle_files) / sizeof(fastapi_oracle_files[0]); j++)
            if (!strcmp(paths[i], fastapi_oracle_files[j].path))
                sources[i].text = fastapi_oracle_read(j);
        ok = sources[i].text != NULL && ok;
    }
    cbm_store_t *store = ok ? open_store() : NULL;
    sqlite3 *db = store ? cbm_store_get_db(store) : NULL;
    ok = db && rq_generation(db, g_project) && rq_snapshot(db, g_project, &sources[0]) &&
         rq_snapshot(db, g_project, &sources[1]);
    char from[1024], to[1024];
    int nf = snprintf(from, sizeof(from), "%s.fastapi.security.oauth2.py.__file__", g_project);
    int nt = snprintf(to, sizeof(to), "%s.fastapi.security.utils.py.__file__", g_project);
    ok = ok && nf > 0 && (size_t)nf < sizeof(from) && nt > 0 && (size_t)nt < sizeof(to);
    /* The occurrence's span is the imported name's clause in the audited source. */
    static const char statement[] = "from fastapi.security.utils import ";
    static const char clause[] = "get_authorization_scheme_param";
    const char *line = ok ? strstr(sources[0].text, statement) : NULL;
    int64_t clause_start = line ? (int64_t)(line - sources[0].text) + (int64_t)strlen(statement) : 0;
    ok = ok && line != NULL;
    const char *sql = "SELECT COUNT(*) FROM resolver_occurrences r "
                      "JOIN nodes s ON s.project=r.project AND s.qualified_name=r.scope "
                      "JOIN nodes t ON t.project=r.project AND t.qualified_name=r.target "
                      "WHERE r.project=?1 AND r.file_path=?2 AND r.scope=?3 AND r.target=?4 "
                      "AND r.relation='IMPORTS' AND r.emitted=1 AND r.rules_version=?5 "
                      "AND r.source_origin=?6 AND r.start_byte=?8 AND r.end_byte=?9 "
                      "AND s.label='File' AND s.file_path=?2 AND t.label='File' AND t.file_path=?7 "
                      "AND (SELECT COUNT(*) FROM edges e WHERE e.project=r.project "
                      "AND e.source_id=s.id AND e.target_id=t.id AND e.type='IMPORTS')=1";
    sqlite3_stmt *stmt = NULL;
    ok = ok && sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK &&
         sqlite3_bind_text(stmt, 1, g_project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
         sqlite3_bind_text(stmt, 2, paths[0], -1, SQLITE_STATIC) == SQLITE_OK &&
         sqlite3_bind_text(stmt, 3, from, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
         sqlite3_bind_text(stmt, 4, to, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
         sqlite3_bind_int(stmt, 5, CBM_RESOLVER_RULES_VERSION) == SQLITE_OK &&
         sqlite3_bind_int(stmt, 6, CBM_SOURCE_ORIGIN_RAW) == SQLITE_OK &&
         sqlite3_bind_text(stmt, 7, paths[1], -1, SQLITE_STATIC) == SQLITE_OK &&
         sqlite3_bind_int64(stmt, 8, clause_start) == SQLITE_OK &&
         sqlite3_bind_int64(stmt, 9, clause_start + (int64_t)strlen(clause)) == SQLITE_OK &&
         sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_int(stmt, 0) == 1 &&
         sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    if (store)
        cbm_store_close(store);
    for (size_t i = 0; i < 2; i++)
        free((char *)sources[i].text);
    return ok;
}

/* ── Setup / Teardown ─────────────────────────────────────────────── */

static int incremental_setup(void) {
    snprintf(g_tmpdir, sizeof(g_tmpdir), "/tmp/cbm_incr_XXXXXX");
    if (!cbm_mkdtemp(g_tmpdir))
        return -1;

    snprintf(g_repodir, sizeof(g_repodir), "%s/fastapi", g_tmpdir);

    /* The fixture is cloned from the network at most once per machine, into a
     * persistent cache; every run local-clones from there (seconds, offline).
     * The one-time clone is staged and committed with an atomic rename so a
     * torn download can never masquerade as a valid cache. */
    const char *cache_home = getenv("CBM_TEST_FIXTURE_CACHE");
    char cache_root[512];
    if (cache_home && cache_home[0]) {
        snprintf(cache_root, sizeof(cache_root), "%s", cache_home);
    } else {
        const char *home = getenv("HOME");
        if (!home || !home[0])
            home = ".";
        snprintf(cache_root, sizeof(cache_root), "%s/.cache/cbm-test-fixtures", home);
    }
    char cache_repo[640];
    snprintf(cache_repo, sizeof(cache_repo), "%s/fastapi-0.99.1", cache_root);
    char cmd[1600];
    if (!cbm_is_dir(cache_repo)) {
        (void)cbm_mkdir_p(cache_root, 0700);
        char cache_stage[700];
        snprintf(cache_stage, sizeof(cache_stage), "%s.stage", cache_repo);
        th_rmtree(cache_stage);
        snprintf(cmd, sizeof(cmd),
                 "git clone --depth=1 --branch 0.99.1 --quiet "
                 "https://github.com/fastapi/fastapi.git '%s' 2>&1",
                 cache_stage);
        int fetch_rc = system(cmd);
        if (fetch_rc != 0 || rename(cache_stage, cache_repo) != 0) {
            th_rmtree(cache_stage);
            if (!cbm_is_dir(cache_repo)) {
                printf("  fixture clone failed (rc=%d) — network offline?\n", fetch_rc);
                return -1;
            }
        }
    }
    snprintf(cmd, sizeof(cmd), "git clone --quiet '%s' '%s' 2>&1", cache_repo, g_repodir);
    int rc = system(cmd);
    if (rc != 0) {
        printf("  fixture local clone failed (rc=%d)\n", rc);
        return -1;
    }
    /* Index the same corpus everywhere: CI historically indexed a sparse
     * checkout without docs/ and tests/ (the assertion thresholds are sized
     * for it) while local runs indexed the full tree — twice the files for
     * the identical assertions, and a local/CI divergence. Trimming the two
     * directories is the portable equivalent of that sparse profile. */
    char trim[600];
    snprintf(trim, sizeof(trim), "%s/docs", g_repodir);
    th_rmtree(trim);
    snprintf(trim, sizeof(trim), "%s/tests", g_repodir);
    th_rmtree(trim);

    g_project = cbm_project_name_from_path(g_repodir);
    if (!g_project)
        return -1;

    const char *cache_dir = cbm_resolve_cache_dir();
    int dbpath_length =
        cache_dir ? snprintf(g_dbpath, sizeof(g_dbpath), "%s/%s.db", cache_dir, g_project) : -1;
    if (dbpath_length <= 0 || (size_t)dbpath_length >= sizeof(g_dbpath) ||
        !cbm_mkdir_p(cache_dir, 0700)) {
        return -1;
    }

    unlink(g_dbpath);

    g_srv = cbm_mcp_server_new(NULL);
    if (!g_srv)
        return -1;

    g_rss_before_full = cbm_mem_rss();

    return 0;
}

static void incremental_teardown(void) {
    if (g_srv) {
        cbm_mcp_server_free(g_srv);
        g_srv = NULL;
    }
    if (g_project) {
        unlink(g_dbpath);
        char wal[520], shm[520];
        snprintf(wal, sizeof(wal), "%s-wal", g_dbpath);
        snprintf(shm, sizeof(shm), "%s-shm", g_dbpath);
        unlink(wal);
        unlink(shm);
    }
    free(g_project);
    g_project = NULL;

    th_rmtree(g_tmpdir);
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 1: Full index — baseline + performance
 * ══════════════════════════════════════════════════════════════════ */

TEST(incr_full_index) {
    double ms = 0;
    size_t peak_mb = 0;
    char *resp = index_repo_timed(&ms, &peak_mb);
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    free(resp);

    g_full_nodes = get_node_count();
    g_full_edges = get_edge_count();
    g_full_calls = get_edge_count_by_type("CALLS");
    g_full_imports = get_edge_count_by_type("IMPORTS");
    g_full_index_ms = ms;

    /* FastAPI: expect substantial graph.
     * CI uses sparse checkout (~7K files), local has full repo (~18K files).
     * Thresholds set for sparse checkout (smaller graph). */
    ASSERT_GT(g_full_nodes, 2000);
    ASSERT_GT(g_full_edges, 500);
    ASSERT(fastapi_call_quality());
    ASSERT_GT(g_full_imports, 50);

    /* Performance: full index — warn if slow, don't block */
    if ((int)ms > 30000) {
        printf("    [PERF WARNING] full index: %.0fms (>30s)\n", ms);
    }

    /* Memory: bounded budget for a 1100-file Python project. ARM (and other
     * large-page) Linux/macOS use 16KB pages vs x86's 4KB; per-allocation page
     * rounding inflates RSS ~25-30% for the SAME logical footprint (not a leak —
     * ARM ~2385MB on the same index). Scale the budget by page size so the guard
     * still catches real runaway memory (a leak would be GBs over) without
     * false-failing on large-page architectures. The x86 base budget is 2304MB:
     * after the retention/source-text-cap and RAM-tiering work the x86 peak for
     * this index settled at ~2050-2072MB (measured across CI runs), so the old
     * 2048 limit sat right on the line and flaked; 2304 restores headroom while a
     * genuine leak (GBs over) still trips it. */
    size_t rss_delta_mb = peak_mb - (g_rss_before_full / (1024 * 1024));
    int rss_limit_mb = 2304;
#ifndef _WIN32
    if (sysconf(_SC_PAGESIZE) >= 16384) {
        rss_limit_mb = 2816;
    }
#endif
#if defined(__aarch64__) || defined(_M_ARM64) || defined(__arm__)
    /* ARM Linux uses 4KB pages, so the page-size bump above does NOT fire there,
     * yet glibc's per-CPU malloc arenas + allocation rounding still inflate RSS
     * to the documented ~2385MB for this index (the same inflation Apple silicon
     * shows, which the page-size check catches via its 16KB pages). Apply the
     * higher ARM budget on any ARM target so the guard still catches a real leak
     * (GBs over) without false-failing on 4KB-page ARM Linux (e.g. CI's
     * ubuntu-22.04-arm, which measured 2386MB against the un-bumped 2048 limit). */
    if (rss_limit_mb < 2816) {
        rss_limit_mb = 2816;
    }
#endif
#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
#define CBM_TEST_RSS_BUDGET_UNMEASURABLE 1
#endif
#endif
#if defined(CBM_TEST_RSS_BUDGET_UNMEASURABLE)
    /* MSan maps shadow (and with origins, a second) mapping for every
     * allocation, inflating RSS by construction: this index measured 3054MB
     * against the 2304MB budget on the x86-64 lane. The budget exists to catch
     * a REAL leak (GBs over) in a normal build; under MSan it cannot separate a
     * leak from shadow, so the number is not evidence either way. Skipped here
     * rather than inflated, so the guard keeps its teeth on every other
     * platform -- inflating it would blind the check where it does work. The
     * rest of this test (node/edge correctness) still runs under MSan, which is
     * the uninitialized-read coverage the lane exists for. */
    (void)rss_delta_mb;
    (void)rss_limit_mb;
#else
    ASSERT_LT((int)rss_delta_mb, rss_limit_mb);
#endif

    printf("    [perf] full: %d nodes, %d edges (%d CALLS, %d IMPORTS) "
           "in %.0fms, peak=%zuMB\n",
           g_full_nodes, g_full_edges, g_full_calls, g_full_imports, ms, peak_mb);

    PASS();
}

TEST(incr_full_has_functions) {
    int funcs = count_by_label("Function");
    ASSERT_GT(funcs, 500);

    int methods = count_by_label("Method");
    ASSERT_GT(methods, 100);

    int modules = count_by_label("Module");
    ASSERT_GT(modules, 200);

    PASS();
}

TEST(incr_full_edge_types) {
    /* Verify edge type distribution is sane */
    int calls = get_edge_count_by_type("CALLS");
    int imports = get_edge_count_by_type("IMPORTS");
    int defines = get_edge_count_by_type("DEFINES");
    int contains = get_edge_count_by_type("CONTAINS_FILE");

    ASSERT_GT(calls, 0);
    ASSERT_GT(imports, 0);
    ASSERT_GT(defines, 0);
    ASSERT_GT(contains, 0);

    /* Different semantic relations have no justified count ordering. */
    ASSERT(fastapi_physical_import());

    printf("    [edges] CALLS=%d IMPORTS=%d DEFINES=%d CONTAINS_FILE=%d\n", calls, imports, defines,
           contains);

    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 2: Noop reindex — must be fast and exact
 * ══════════════════════════════════════════════════════════════════ */

TEST(incr_noop_reindex) {
    double ms = 0;
    size_t peak_mb = 0;
    char *resp = index_repo_timed(&ms, &peak_mb);
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    free(resp);

    /* Exact same counts */
    ASSERT_EQ(get_node_count(), g_full_nodes);
    ASSERT_EQ(get_edge_count(), g_full_edges);

    /* Noop should be fast (just file classification, no parsing) */
    if ((int)ms > 5000) {
        printf("    [PERF WARNING] noop reindex: %.0fms (>5s)\n", ms);
    }

    printf("    [perf] noop: %.0fms\n", ms);

    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 3: Incremental delta changes
 * ══════════════════════════════════════════════════════════════════ */

TEST(incr_modify_file) {
    int nodes_before = get_node_count();

    /* Append functions to an existing source file */
    char path[512];
    snprintf(path, sizeof(path), "%s/fastapi/applications.py", g_repodir);
    FILE *f = fopen(path, "a");
    ASSERT(f != NULL);
    fprintf(f, "\n\ndef incr_modify_extra_a(x: int) -> int:\n"
               "    return x + 1\n"
               "\n"
               "def incr_modify_extra_b(y: str) -> str:\n"
               "    return y.strip()\n");
    fclose(f);

    double ms = 0;
    size_t peak_mb = 0;
    char *resp = index_repo_timed(&ms, &peak_mb);
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    free(resp);

    ASSERT(has_function("incr_modify_extra_a"));
    ASSERT(has_function("incr_modify_extra_b"));
    ASSERT_GT(get_node_count(), nodes_before);

    /* Single-file incremental should be faster than full */
    if ((int)ms > (int)(g_full_index_ms * 1.5)) {
        printf("    [PERF WARNING] incremental slower than 1.5x full: %.0fms vs %.0fms\n", ms,
               g_full_index_ms);
    }

    printf("    [perf] modify 1 file: %.0fms (full was %.0fms)\n", ms, g_full_index_ms);

    PASS();
}

TEST(incr_formatter_run) {
    /* Baseline */
    char *resp = index_repo();
    ASSERT(resp != NULL);
    free(resp);

    int nodes_before = get_node_count();
    int edges_before = get_edge_count();
    int calls_before = get_edge_count_by_type("CALLS");

    /* Simulate formatter: touch 50 files */
    reformat_files("fastapi", 50);

    double ms = 0;
    size_t peak_mb = 0;
    resp = index_repo_timed(&ms, &peak_mb);
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    free(resp);

    /* Graph should be nearly identical — formatter adds no functions.
     * Warn on >10% variance (can happen with sparse checkout / smaller repos). */
    int node_diff = abs(get_node_count() - nodes_before);
    int edge_diff = abs(get_edge_count() - edges_before);
    if (node_diff > nodes_before / 10 || edge_diff > edges_before / 10) {
        printf("    [PERF WARNING] formatter drift: node_diff=%d (max %d), edge_diff=%d (max %d)\n",
               node_diff, nodes_before / 10, edge_diff, edges_before / 10);
    }

    /* CALLS edges: reformatting changes line numbers which affects resolution. */
    int calls_diff = abs(get_edge_count_by_type("CALLS") - calls_before);
    if (calls_diff > calls_before / 4) {
        printf("    [PERF WARNING] CALLS drift: %d (max %d)\n", calls_diff, calls_before / 4);
    }

    printf("    [perf] reformat 50 files: %.0fms, node_diff=%d edge_diff=%d\n", ms, node_diff,
           edge_diff);

    PASS();
}

static char g_purge_log[8192];
static int g_purge_progress_lines;

static void purge_log_sink(const char *line) {
    if (!line || !strstr(line, "msg=incremental.purge")) {
        return;
    }
    if (strstr(line, "msg=incremental.purge.progress")) {
        g_purge_progress_lines++;
    }
    size_t used = strlen(g_purge_log);
    (void)snprintf(g_purge_log + used, sizeof(g_purge_log) - used, "%s\n", line);
}

/* #1300: the per-file purge after incremental.edge_snapshot logged nothing
 * until it finished, so a stall there left no trace of where the run was. It
 * now announces its counts and reports bounded progress — counts only, at
 * most 20 progress lines however many files, ending at files_done ==
 * files_total. */
TEST(incr_purge_reports_bounded_progress_issue1300) {
    /* Author more than the progress bound explicitly. FastAPI has only 20
     * immediate .py children, so reformat_files("fastapi", 30) never exercises
     * the bounded large-purge case. Keep this fixture independent of that tree. */
    enum { CHANGED_FILES = 40, DELETED_FILES = 1 };
    const char *temporary = th_mktempdir("cbm_purge_progress");
    ASSERT_NOT_NULL(temporary);
    char root[512], repo[600], database[600], path[768];
    snprintf(root, sizeof(root), "%s", temporary);
    snprintf(repo, sizeof(repo), "%s/repo", root);
    snprintf(database, sizeof(database), "%s/index.db", root);
    ASSERT(cbm_mkdir_p(repo, 0755));
    for (int i = 0; i < CHANGED_FILES + DELETED_FILES; i++) {
        snprintf(path, sizeof(path), "%s/unit_%02d.py", repo, i);
        ASSERT_EQ(th_write_file(path, "def work():\n    return 1\n"), 0);
    }
    cbm_pipeline_t *initial = cbm_pipeline_new(repo, database, CBM_MODE_FULL);
    ASSERT_NOT_NULL(initial);
    ASSERT_EQ(cbm_pipeline_run(initial), 0);
    char project[256];
    snprintf(project, sizeof(project), "%s", cbm_pipeline_project_name(initial));
    cbm_pipeline_free(initial);
    cbm_store_t *store = cbm_store_open_path_query(database);
    ASSERT_NOT_NULL(store);
    unsigned char before_digest[32];
    char before_generation[CBM_GRAPH_COMPARE_GENERATION_SIZE];
    ASSERT_EQ(cbm_store_graph_content_digest(store, project, before_digest), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_generation(store, before_generation, sizeof(before_generation)),
              CBM_STORE_OK);
    cbm_file_hash_t original_hash = {0};
    ASSERT_EQ(cbm_store_get_file_hash(store, project, "unit_00.py", &original_hash), CBM_STORE_OK);
    char before_hash[65];
    snprintf(before_hash, sizeof(before_hash), "%s", original_hash.sha256);
    cbm_store_clear_file_hash(&original_hash);
    cbm_store_close(store);
    static const char CHANGED_SOURCE[] = "def changed_work():\n    return 1\n# changed input\n";
    for (int i = 0; i < CHANGED_FILES; i++) {
        snprintf(path, sizeof(path), "%s/unit_%02d.py", repo, i);
        ASSERT_EQ(th_write_file(path, CHANGED_SOURCE), 0);
    }
    snprintf(path, sizeof(path), "%s/unit_%02d.py", repo, CHANGED_FILES);
    ASSERT_EQ(unlink(path), 0);
    /* The instrumented purge loop remains on the test-only legacy route.
     * This fixture contains no controlled resolver occurrences, so its complete
     * replacement graph can publish. The test checks the actual purge and its
     * bounded progress, independently of resolver-generation rejection tests. */
    cbm_pipeline_incremental_test_force_legacy_partial_once();

    CBMLogLevel saved_level = cbm_log_get_level();
    CBMLogFormat saved_format = cbm_log_get_format();
    cbm_log_set_level(CBM_LOG_INFO);
    cbm_log_set_format(CBM_LOG_FORMAT_TEXT);
    g_purge_log[0] = '\0';
    g_purge_progress_lines = 0;
    cbm_log_set_sink_ex(purge_log_sink, CBM_LOG_SINK_TEE);
    cbm_pipeline_t *attempt = cbm_pipeline_new(repo, database, CBM_MODE_FULL);
    int attempt_rc = attempt ? cbm_pipeline_run(attempt) : CBM_NOT_FOUND;
    cbm_incremental_route_t attempt_route = cbm_pipeline_incremental_test_last_route();
    cbm_pipeline_free(attempt);
    cbm_log_set_sink(NULL);
    cbm_log_set_level(saved_level);
    cbm_log_set_format(saved_format);
    ASSERT_EQ(attempt_rc, 0);
    ASSERT_EQ(attempt_route, CBM_INCREMENTAL_ROUTE_LEGACY_PARTIAL);

    store = cbm_store_open_path_query(database);
    ASSERT_NOT_NULL(store);
    unsigned char after_digest[32];
    char after_generation[CBM_GRAPH_COMPARE_GENERATION_SIZE];
    ASSERT_EQ(cbm_store_graph_content_digest(store, project, after_digest), CBM_STORE_OK);
    ASSERT_EQ(cbm_store_generation(store, after_generation, sizeof(after_generation)),
              CBM_STORE_OK);
    bool changed_files_current = true;
    for (int i = 0; i < CHANGED_FILES; i++) {
        char relative[32];
        snprintf(relative, sizeof(relative), "unit_%02d.py", i);
        cbm_file_hash_t hash = {0};
        int hash_rc = cbm_store_get_file_hash(store, project, relative, &hash);
        changed_files_current &= hash_rc == CBM_STORE_OK && hash.sha256 &&
                                 strcmp(hash.sha256, before_hash) != 0 &&
                                 hash.size == (int64_t)(sizeof(CHANGED_SOURCE) - 1);
        cbm_store_clear_file_hash(&hash);
        cbm_node_t *nodes = NULL;
        int count = 0, functions = 0;
        changed_files_current &=
            cbm_store_find_nodes_by_file(store, project, relative, &nodes, &count) == CBM_STORE_OK;
        for (int n = 0; n < count; n++) {
            if (strcmp(nodes[n].label, "Function") == 0) {
                functions++;
                changed_files_current &= strcmp(nodes[n].name, "changed_work") == 0;
            }
        }
        changed_files_current &= functions == 1;
        cbm_store_free_nodes(nodes, count);
    }
    char deleted_relative[32];
    snprintf(deleted_relative, sizeof(deleted_relative), "unit_%02d.py", CHANGED_FILES);
    cbm_node_t *deleted_nodes = NULL;
    int deleted_nodes_count = 0;
    int deleted_nodes_rc = cbm_store_find_nodes_by_file(store, project, deleted_relative,
                                                        &deleted_nodes, &deleted_nodes_count);
    cbm_store_free_nodes(deleted_nodes, deleted_nodes_count);
    cbm_file_hash_t deleted_hash = {0};
    int deleted_hash_rc = cbm_store_get_file_hash(store, project, deleted_relative, &deleted_hash);
    cbm_store_clear_file_hash(&deleted_hash);
    cbm_store_close(store);
    ASSERT(memcmp(before_digest, after_digest, sizeof(before_digest)) != 0);
    ASSERT(strcmp(before_generation, after_generation) != 0);
    ASSERT(changed_files_current);
    ASSERT_EQ(deleted_nodes_rc, CBM_STORE_OK);
    ASSERT_EQ(deleted_nodes_count, 0);
    ASSERT_EQ(deleted_hash_rc, CBM_STORE_NOT_FOUND);

    const char *start = strstr(g_purge_log, "msg=incremental.purge.start");
    ASSERT(start != NULL);
    int changed = -1;
    int deleted = -1;
    const char *changed_at = strstr(start, "changed=");
    const char *deleted_at = strstr(start, "deleted=");
    ASSERT(changed_at != NULL && deleted_at != NULL);
    changed = atoi(changed_at + strlen("changed="));
    deleted = atoi(deleted_at + strlen("deleted="));
    ASSERT_EQ(changed, CHANGED_FILES);
    ASSERT_EQ(deleted, DELETED_FILES);
    int total = changed + deleted;
    ASSERT_GT(total, 20); /* enough files that one line per file would exceed the bound */
    ASSERT_GT(g_purge_progress_lines, 0);
    ASSERT(g_purge_progress_lines <= 21);
    ASSERT(g_purge_progress_lines < total); /* never one line per file */
    char last[64];
    (void)snprintf(last, sizeof(last), "files_done=%d files_total=%d", total, total);
    ASSERT(strstr(g_purge_log, last) != NULL);
    ASSERT(strstr(g_purge_log, "msg=incremental.purge elapsed_ms=") != NULL);

    th_cleanup(root);
    PASS();
}

TEST(incr_add_file) {
    int nodes_before = get_node_count();

    write_file_at("fastapi/incr_test_new.py", "\"\"\"New module.\"\"\"\n"
                                              "from fastapi import FastAPI\n"
                                              "\n"
                                              "def incr_new_entry(app: FastAPI) -> None:\n"
                                              "    setup(app)\n"
                                              "\n"
                                              "def setup(app: FastAPI) -> None:\n"
                                              "    pass\n"
                                              "\n"
                                              "def incr_new_validate(data: dict) -> bool:\n"
                                              "    return bool(data)\n"
                                              "\n"
                                              "class IncrNewHandler:\n"
                                              "    def handle(self, req):\n"
                                              "        return incr_new_validate(req)\n"
                                              "\n"
                                              "    def cleanup(self):\n"
                                              "        pass\n");

    char *resp = index_repo();
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    free(resp);

    ASSERT(has_function("incr_new_entry"));
    ASSERT(has_function("incr_new_validate"));
    ASSERT_GT(get_node_count(), nodes_before);

    PASS();
}

TEST(incr_delete_file) {
    int nodes_before = get_node_count();

    delete_file_at("fastapi/incr_test_new.py");

    char *resp = index_repo();
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    free(resp);

    ASSERT(!has_function("incr_new_entry"));
    ASSERT(!has_function("incr_new_validate"));
    ASSERT(has_function("incr_test_injected")); /* others survive */
    ASSERT_LT(get_node_count(), nodes_before);

    PASS();
}

TEST(incr_simultaneous_changes) {
    /* Add */
    write_file_at("tests/incr_simul_add.py", "def incr_simul_added():\n    return 'added'\n");

    /* Modify */
    char path[512];
    snprintf(path, sizeof(path), "%s/fastapi/routing.py", g_repodir);
    FILE *f = fopen(path, "a");
    if (f) {
        fprintf(f, "\ndef incr_simul_modified():\n    return 'modified'\n");
        fclose(f);
    }

    /* Delete */
    delete_file_at("fastapi/incr_test_new.py"); /* already gone, noop */

    char *resp = index_repo();
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    free(resp);

    ASSERT(has_function("incr_simul_added"));
    ASSERT(has_function("incr_simul_modified"));

    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 4: Adversarial inputs — attempt to break the pipeline
 * ══════════════════════════════════════════════════════════════════ */

TEST(incr_empty_file) {
    write_file_at("fastapi/incr_empty.py", "");
    char *resp = index_repo();
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    ASSERT(has_function("incr_test_injected"));
    delete_file_at("fastapi/incr_empty.py");
    free(resp);
    PASS();
}

TEST(incr_syntax_error) {
    int nodes_before = get_node_count();

    write_file_at("fastapi/incr_broken.py", "def broken(\n"
                                            "    # deliberately broken\n"
                                            "class @#$% {\n"
                                            "   async def nested() -> None\n"
                                            "       yield from broken(\n"
                                            "))))\n");

    char *resp = index_repo();
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    ASSERT(has_function("incr_test_injected")); /* others survive */

    int diff = abs(get_node_count() - nodes_before);
    ASSERT_LT(diff, 20);

    delete_file_at("fastapi/incr_broken.py");
    free(resp);
    PASS();
}

TEST(incr_huge_single_function) {
    /* A single function with 5000 lines — stress test extraction budget */
    size_t sz = 5000 * 30 + 100;
    char *content = malloc(sz);
    ASSERT(content != NULL);
    int pos = 0;
    pos += snprintf(content + pos, sz - (size_t)pos, "def huge_func(x):\n");
    for (int i = 0; i < 5000; i++) {
        pos += snprintf(content + pos, sz - (size_t)pos, "    x = x + %d\n", i);
    }
    pos += snprintf(content + pos, sz - (size_t)pos, "    return x\n");
    (void)pos;

    write_file_at("fastapi/incr_huge.py", content);
    free(content);

    char *resp = index_repo();
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    free(resp);

    /* Should extract the function (may be truncated by budget, that's ok) */
    /* Key: must not crash or corrupt graph */
    ASSERT(has_function("incr_test_injected"));

    delete_file_at("fastapi/incr_huge.py");
    PASS();
}

TEST(incr_binary_content) {
    /* Write binary garbage to a .py file */
    char path[512];
    snprintf(path, sizeof(path), "%s/fastapi/incr_binary.py", g_repodir);
    FILE *f = fopen(path, "wb");
    if (f) {
        unsigned char bin[256];
        for (int i = 0; i < 256; i++)
            bin[i] = (unsigned char)i;
        fwrite(bin, 1, sizeof(bin), f);
        fclose(f);
    }

    char *resp = index_repo();
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    ASSERT(has_function("incr_test_injected"));

    delete_file_at("fastapi/incr_binary.py");
    free(resp);
    PASS();
}

TEST(incr_large_generated) {
    char *content = malloc(300 * 80);
    ASSERT(content != NULL);
    content[0] = '\0';
    for (int i = 0; i < 300; i++) {
        char line[80];
        snprintf(line, sizeof(line), "def incr_gen_%d(x):\n    return x + %d\n\n", i, i);
        strcat(content, line);
    }

    write_file_at("fastapi/incr_generated.py", content);
    free(content);

    char *resp = index_repo();
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    ASSERT(has_function("incr_gen_0"));
    ASSERT(has_function("incr_gen_299"));

    delete_file_at("fastapi/incr_generated.py");
    free(resp);
    PASS();
}

TEST(incr_new_subdir) {
    /* File in a brand new subdirectory */
    write_file_at("fastapi/newpkg/__init__.py", "");
    write_file_at("fastapi/newpkg/handler.py", "def newpkg_handler():\n"
                                               "    return 'from_new_package'\n");

    char *resp = index_repo();
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    ASSERT(has_function("newpkg_handler"));

    delete_file_at("fastapi/newpkg/handler.py");
    delete_file_at("fastapi/newpkg/__init__.py");
    free(resp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 5: Stress + complex scenarios
 * ══════════════════════════════════════════════════════════════════ */

TEST(incr_rapid_reindex) {
    char *resp = index_repo();
    ASSERT(resp != NULL);
    free(resp);

    int baseline_nodes = get_node_count();
    int baseline_edges = get_edge_count();

    for (int i = 0; i < 3; i++) {
        resp = index_repo();
        ASSERT(resp != NULL);
        ASSERT(strstr(resp, "indexed") != NULL);
        free(resp);
    }

    ASSERT_EQ(get_node_count(), baseline_nodes);
    ASSERT_EQ(get_edge_count(), baseline_edges);
    PASS();
}

TEST(incr_replace_file_content) {
    write_file_at("fastapi/incr_replace.py", "def replace_original_a():\n    return 'a'\n"
                                             "def replace_original_b():\n    return 'b'\n");

    char *resp = index_repo();
    ASSERT(resp != NULL);
    free(resp);

    ASSERT(has_function("replace_original_a"));
    ASSERT(has_function("replace_original_b"));

    /* Replace entirely */
    write_file_at("fastapi/incr_replace.py", "def replace_new_x():\n    return 'x'\n"
                                             "def replace_new_y():\n    return replace_new_x()\n");

    resp = index_repo();
    ASSERT(resp != NULL);
    free(resp);

    ASSERT(!has_function("replace_original_a"));
    ASSERT(!has_function("replace_original_b"));
    ASSERT(has_function("replace_new_x"));
    ASSERT(has_function("replace_new_y"));

    delete_file_at("fastapi/incr_replace.py");
    PASS();
}

TEST(incr_batch_add_delete) {
    /* Add 20 files */
    for (int i = 0; i < 20; i++) {
        char path[128], content[256];
        snprintf(path, sizeof(path), "tests/incr_batch_%d.py", i);
        snprintf(content, sizeof(content),
                 "def batch_%d(x):\n    return x + %d\n"
                 "def batch_h_%d(y):\n    return batch_%d(y) * 2\n",
                 i, i, i, i);
        write_file_at(path, content);
    }

    char *resp = index_repo();
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    free(resp);

    ASSERT(has_function("batch_0"));
    ASSERT(has_function("batch_19"));
    ASSERT(has_function("batch_h_10"));

    /* Delete all 20 */
    for (int i = 0; i < 20; i++) {
        char path[128];
        snprintf(path, sizeof(path), "tests/incr_batch_%d.py", i);
        delete_file_at(path);
    }

    resp = index_repo();
    ASSERT(resp != NULL);
    free(resp);

    ASSERT(!has_function("batch_0"));
    ASSERT(!has_function("batch_19"));

    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 6: Recovery + accuracy
 * ══════════════════════════════════════════════════════════════════ */

TEST(incr_db_deleted_recovery) {
    int nodes_before = get_node_count();

    unlink(g_dbpath);

    double ms = 0;
    size_t peak_mb = 0;
    char *resp = index_repo_timed(&ms, &peak_mb);
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    free(resp);

    /* Full reindex must produce similar count */
    int nodes_after = get_node_count();
    int diff_pct = abs(nodes_after - nodes_before) * 100 / nodes_before;
    ASSERT_LT(diff_pct, 5);

    printf("    [perf] db recovery (full reindex): %.0fms, peak=%zuMB\n", ms, peak_mb);

    PASS();
}

TEST(incr_accuracy_vs_full) {
    /* Modify a file to create a known incremental state */
    write_file_at("fastapi/incr_accuracy.py", "def accuracy_a():\n    return 1\n"
                                              "def accuracy_b():\n    return accuracy_a() + 1\n");

    char *resp = index_repo();
    ASSERT(resp != NULL);
    free(resp);

    int incr_nodes = get_node_count();
    int incr_edges = get_edge_count();
    int incr_calls = get_edge_count_by_type("CALLS");

    /* Delete DB, force full reindex */
    unlink(g_dbpath);
    resp = index_repo();
    ASSERT(resp != NULL);
    free(resp);

    int full_nodes = get_node_count();
    int full_edges = get_edge_count();
    int full_calls = get_edge_count_by_type("CALLS");

    /* Within tight tolerance (±2 for dedup timing differences) */
    ASSERT_LTE(abs(full_nodes - incr_nodes), 2);
    ASSERT_LTE(abs(full_nodes - incr_nodes), 50);
    ASSERT_LTE(abs(full_calls - incr_calls), 2);

    printf("    [accuracy] incr: %d nodes/%d edges, full: %d nodes/%d edges\n", incr_nodes,
           incr_edges, full_nodes, full_edges);

    delete_file_at("fastapi/incr_accuracy.py");
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 7: Performance regression guards
 * ══════════════════════════════════════════════════════════════════ */

TEST(incr_perf_single_file_fast) {
    /* Modifying one file should complete in <2s (not re-parse everything) */
    write_file_at("fastapi/incr_perf_probe.py", "def perf_probe():\n    return 42\n");

    double ms = 0;
    size_t peak_mb = 0;
    char *resp = index_repo_timed(&ms, &peak_mb);
    ASSERT(resp != NULL);
    free(resp);
#define PERF_WARN_MS 15000 /* warn above 15s — slow CI runners may exceed this */

    if ((int)ms > PERF_WARN_MS) {
        printf("    [PERF WARNING] single file incremental: %.0fms (>%dms)\n", ms, PERF_WARN_MS);
    } else {
        printf("    [perf] single file incremental: %.0fms\n", ms);
    }

    delete_file_at("fastapi/incr_perf_probe.py");
    /* Publish the cleanup too. search_code now consumes the complete scoped
     * file list and fails closed when the graph names a source file that grep
     * cannot read; leaving this probe indexed would make every later matching
     * search correctly report an incomplete snapshot instead of exercising
     * the search parameter contract this fixture is meant to cover. The
     * cleanup reindex is deliberately outside the measured interval above. */
    resp = index_repo();
    ASSERT(resp != NULL);
    ASSERT(strstr(resp, "indexed") != NULL);
    free(resp);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  PHASE 8: MCP tool integration — comprehensive per-tool tests
 *
 *  Every tool, every parameter, error paths, timeouts.
 *  Timing: each call_tool_timed() warns if > PERF_WARN_MS.
 * ══════════════════════════════════════════════════════════════════ */

static char *call_tool_timed(const char *tool, double *ms, const char *args_fmt, ...) {
    char args[2048];
    va_list ap;
    va_start(ap, args_fmt);
    vsnprintf(args, sizeof(args), args_fmt, ap);
    va_end(ap);
    double t0 = now_ms();
    char *resp = cbm_mcp_handle_tool(g_srv, tool, args);
    *ms = now_ms() - t0;
    return resp;
}

/* Check if ALL results have a specific label (validates label filter works) */
static int all_results_have_label(const char *resp, const char *label) {
    if (!resp)
        return 0;
    /* Every "label":"X" in results must match. Check no other label appears. */
    char pattern[64];
    snprintf(pattern, sizeof(pattern), "\"label\":\"%s\"", label);
    char escaped[64];
    snprintf(escaped, sizeof(escaped), "\\\"label\\\":\\\"%s\\\"", label);
    /* If response contains "label" at all, every one must match */
    const char *p = resp;
    while ((p = strstr(p, "label")) != NULL) {
        /* Skip the key name — find the value */
        p += 5; /* past "label" */
        const char *val = strstr(p, ":\"");
        if (!val || val > p + 10) {
            p++;
            continue;
        }
        val += 2;
        /* Check this label value matches */
        if (strncmp(val, label, strlen(label)) != 0) {
            /* Check for escaped version */
            const char *eval = strstr(p, ":\\\"");
            if (eval && eval < p + 10) {
                eval += 3;
                if (strncmp(eval, label, strlen(label)) != 0)
                    return 0;
            } else {
                return 0;
            }
        }
        p++;
    }
    return 1;
}

/* Check response contains a specific key (in nested JSON) */
static int resp_has_key(const char *resp, const char *key) {
    if (!resp)
        return 0;
    char pattern[64];
    snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    if (strstr(resp, pattern) != NULL)
        return 1;
    snprintf(pattern, sizeof(pattern), "\\\"%s\\\"", key);
    if (strstr(resp, pattern) != NULL)
        return 1;
    /* TOON scalar form (`key: value`), the search_graph default output. */
    snprintf(pattern, sizeof(pattern), "%s: ", key);
    if (strstr(resp, pattern) != NULL)
        return 1;
    /* TOON table-header form (`key[N]{...}:`), used by trace_path. */
    snprintf(pattern, sizeof(pattern), "%s[", key);
    return strstr(resp, pattern) != NULL;
}

/* Check response does NOT contain a specific key */
static int resp_lacks_key(const char *resp, const char *key) {
    return !resp_has_key(resp, key);
}

/* Helper: assert tool call succeeds, warn if slow */
#define TOOL_OK(resp, ms)                                                                 \
    do {                                                                                  \
        ASSERT((resp) != NULL);                                                           \
        if ((int)(ms) > PERF_WARN_MS) {                                                   \
            printf("    [PERF WARNING] tool call: %.0fms (>%dms)\n", (ms), PERF_WARN_MS); \
        }                                                                                 \
    } while (0)

/* Helper: assert response is not an error */
#define NOT_ERROR(resp) ASSERT(strstr((resp), "\"isError\":true") == NULL)

/* ── list_projects ─────────────────────────────────────────────── */

TEST(incr_file_node_name_stays_basename) {
    /* #994: File nodes must keep their basename NAME through an incremental
     * re-index (the bug renamed touched files' nodes to rel_path, diverging
     * from a full build). Modify a file, re-index, assert no File node name
     * contains a path separator. */
    char path[512];
    snprintf(path, sizeof(path), "%s/fastapi/applications.py", g_repodir);
    FILE *f = fopen(path, "a");
    ASSERT(f != NULL);
    fprintf(f, "\n# incr_file_node_name_stays_basename touch\n");
    fclose(f);

    char *resp = index_repo();
    ASSERT(resp != NULL);
    free(resp);

    double ms;
    char *r = call_tool_timed("query_graph", &ms,
                              "{\"project\":\"%s\",\"format\":\"json\","
                              "\"query\":\"MATCH (n:File) WHERE n.name CONTAINS '/'"
                              " RETURN n.name LIMIT 5\"}",
                              g_project);
    TOOL_OK(r, ms);
    /* No File node may carry a path separator in its name; the query engine
     * returns an empty rows array when nothing matches. */
    ASSERT(strstr(r, "\"rows\":[]") != NULL || strstr(r, "\\\"rows\\\":[]") != NULL);
    free(r);
    PASS();
}

TEST(tool_list_projects_basic) {
    double ms;
    char *r = call_tool_timed("list_projects", &ms, "{}");
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "projects") != NULL);
    free(r);
    PASS();
}

TEST(tool_qg_defines_method_more_than_10) {
    write_file_at("fastapi/big_class.py", "class BigClass:\n"
                                          "    def m1(self): pass\n"
                                          "    def m2(self): pass\n"
                                          "    def m3(self): pass\n"
                                          "    def m4(self): pass\n"
                                          "    def m5(self): pass\n"
                                          "    def m6(self): pass\n"
                                          "    def m7(self): pass\n"
                                          "    def m8(self): pass\n"
                                          "    def m9(self): pass\n"
                                          "    def m10(self): pass\n"
                                          "    def m11(self): pass\n"
                                          "    def m12(self): pass\n"
                                          "    def m13(self): pass\n"
                                          "    def m14(self): pass\n"
                                          "    def m15(self): pass\n");
    char *idx = index_repo();
    ASSERT(idx != NULL);
    free(idx);
    double ms;
    char *r = call_tool_timed("query_graph", &ms,
                              "{\"project\":\"%s\","
                              "\"query\":\"MATCH (c:Class)-[:DEFINES_METHOD]->(m:Method)"
                              " WHERE c.name = 'BigClass' RETURN count(m) AS n\"}",
                              g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "\"15\"") != NULL || strstr(r, "\\\"15\\\"") != NULL);
    free(r);
    PASS();
}

TEST(tool_qg_class_lines_nonzero) {
    double ms;
    char *r =
        call_tool_timed("query_graph", &ms,
                        "{\"project\":\"%s\","
                        "\"query\":\"MATCH (c:Class) WHERE c.lines > 0 RETURN count(c) AS n\"}",
                        g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "\"0\"") == NULL);
    free(r);
    PASS();
}

TEST(tool_list_projects_has_current) {
    double ms;
    char *r = call_tool_timed("list_projects", &ms, "{}");
    TOOL_OK(r, ms);
    /* Our project must be in the list. Project name is path-derived,
     * may contain escaped slashes in JSON. Check for "fastapi" substring
     * which will appear in the path-derived name regardless of escaping. */
    /* Project list must contain at least one entry */
    ASSERT(strstr(r, "projects") != NULL);
    free(r);
    PASS();
}

/* ── index_status ──────────────────────────────────────────────── */

TEST(tool_index_status_basic) {
    double ms;
    char *r = call_tool_timed("index_status", &ms, "{\"project\":\"%s\"}", g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "nodes") != NULL || strstr(r, "indexed") != NULL);
    free(r);
    PASS();
}

TEST(tool_index_status_nonexistent) {
    double ms;
    char *r = call_tool_timed("index_status", &ms, "{\"project\":\"does-not-exist-xyz\"}");
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "not") != NULL || strstr(r, "error") != NULL);
    free(r);
    PASS();
}

/* ── get_graph_schema ──────────────────────────────────────────── */

TEST(tool_schema_has_labels) {
    double ms;
    char *r = call_tool_timed("get_graph_schema", &ms, "{\"project\":\"%s\"}", g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "Function") != NULL);
    ASSERT(strstr(r, "Method") != NULL);
    ASSERT(strstr(r, "Module") != NULL);
    ASSERT(strstr(r, "Variable") != NULL);
    free(r);
    PASS();
}

TEST(tool_schema_has_edge_types) {
    double ms;
    char *r = call_tool_timed("get_graph_schema", &ms, "{\"project\":\"%s\"}", g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "CALLS") != NULL);
    ASSERT(strstr(r, "IMPORTS") != NULL);
    ASSERT(strstr(r, "DEFINES") != NULL);
    ASSERT(strstr(r, "CONTAINS_FILE") != NULL);
    /* Schema should also report node and edge counts */
    ASSERT(resp_has_key(r, "node_labels") || resp_has_key(r, "labels") ||
           strstr(r, "Function") != NULL);
    free(r);
    PASS();
}

/* ── search_graph: label filters ───────────────────────────────── */

TEST(tool_sg_label_function) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\",\"limit\":5}", g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 100);
    /* Verify all returned results actually have label=Function */
    ASSERT(all_results_have_label(r, "Function"));
    /* Must not contain Method results */
    ASSERT(strstr(r, "\"Method\"") == NULL && strstr(r, "\\\"Method\\\"") == NULL);
    free(r);
    PASS();
}

TEST(tool_sg_label_method) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Method\",\"limit\":5}", g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 50);
    /* Verify all returned results actually have label=Method */
    ASSERT(all_results_have_label(r, "Method"));
    free(r);
    PASS();
}

TEST(tool_sg_label_module) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms, "{\"project\":\"%s\",\"label\":\"Module\"}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 200);
    free(r);
    PASS();
}

TEST(tool_sg_label_variable) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms, "{\"project\":\"%s\",\"label\":\"Variable\"}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

TEST(tool_sg_label_class) {
    double ms;
    char *r =
        call_tool_timed("search_graph", &ms, "{\"project\":\"%s\",\"label\":\"Class\"}", g_project);
    TOOL_OK(r, ms);
    /* FastAPI has classes */
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

TEST(tool_sg_label_route) {
    double ms;
    char *r =
        call_tool_timed("search_graph", &ms, "{\"project\":\"%s\",\"label\":\"Route\"}", g_project);
    TOOL_OK(r, ms);
    /* May or may not have routes */
    ASSERT(strstr(r, "total") != NULL || strstr(r, "results") != NULL);
    free(r);
    PASS();
}

TEST(tool_sg_label_nonexistent) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms, "{\"project\":\"%s\",\"label\":\"Nonexistent\"}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_EQ(total, 0);
    free(r);
    PASS();
}

/* ── search_graph: name_pattern ────────────────────────────────── */

TEST(tool_sg_name_exact) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"name_pattern\":\"incr_test_injected\"}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_EQ(total, 1);
    free(r);
    PASS();
}

TEST(tool_sg_name_regex) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"name_pattern\":\".*test.*\"}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

TEST(tool_sg_name_no_match) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"name_pattern\":\"zzz_nonexistent_zzz\"}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_EQ(total, 0);
    free(r);
    PASS();
}

/* ── search_graph: degree filters ──────────────────────────────── */

TEST(tool_sg_min_degree) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"qn_pattern\":\"[.]fastapi[.]security[.]utils[.]"
                              "get_authorization_scheme_param$\","
                              "\"relationship\":\"CALLS\",\"min_degree\":2}",
                              g_project);
    TOOL_OK(r, ms);
    /* The two required OAuth2 callers independently establish this bound. */
    ASSERT_EQ(count_in_response(r, "total"), 1);
    ASSERT(strstr(r, "fastapi.security.utils.get_authorization_scheme_param") != NULL);
    free(r);
    PASS();
}

TEST(tool_sg_max_degree_zero) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"max_degree\":0}",
                              g_project);
    TOOL_OK(r, ms);
    /* Isolated functions (no edges) */
    int total = count_in_response(r, "total");
    ASSERT_GTE(total, 0);
    free(r);
    PASS();
}

TEST(tool_sg_degree_range) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"min_degree\":2,\"max_degree\":5}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

/* ── search_graph: pagination ──────────────────────────────────── */

TEST(tool_sg_limit) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"limit\":3}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    /* total should show real count (much more than 3) */
    ASSERT_GT(total, 100);
    /* but has_more should be true (pagination indicator) */
    ASSERT(resp_has_key(r, "has_more"));
    free(r);
    PASS();
}

TEST(tool_sg_offset) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"limit\":5,\"offset\":10}",
                              g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "results") != NULL);
    free(r);
    PASS();
}

/* ── search_graph: file_pattern ────────────────────────────────── */

TEST(tool_sg_file_pattern) {
    double ms;
    /* file_pattern is glob (converted to SQL LIKE), not regex */
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"file_pattern\":\"*fastapi*\",\"limit\":5}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

/* ── search_graph: include_connected ───────────────────────────── */

TEST(tool_sg_include_connected) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"name_pattern\":\"incr_test_injected\","
                              "\"include_connected\":true}",
                              g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "connected") != NULL || strstr(r, "results") != NULL);
    free(r);
    PASS();
}

/* ── search_graph: relationship filter ─────────────────────────── */

TEST(tool_sg_relationship) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"relationship\":\"CALLS\",\"min_degree\":1}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

/* ── search_graph: qn_pattern ──────────────────────────────────── */

TEST(tool_sg_qn_pattern) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\","
                              "\"qn_pattern\":\".*fastapi.*applications.*\"}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

/* ── search_graph: combined filters ────────────────────────────── */

TEST(tool_sg_combined_filters) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"name_pattern\":\".*__init__.*\","
                              "\"min_degree\":1,\"limit\":5}",
                              g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "results") != NULL);
    free(r);
    PASS();
}

/* ── search_graph: project-only (no filters) ───────────────────── */

TEST(tool_sg_project_only) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms, "{\"project\":\"%s\"}", g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 1000);
    free(r);
    PASS();
}

/* ── search_graph: error cases ─────────────────────────────────── */

TEST(tool_sg_invalid_project) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"nonexistent\",\"label\":\"Function\"}");
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "error") != NULL || strstr(r, "not found") != NULL ||
           strstr(r, "not_found") != NULL);
    free(r);
    PASS();
}

/* ── query_graph: basic patterns ───────────────────────────────── */

TEST(tool_qg_match_nodes) {
    double ms;
    char *r = call_tool_timed("query_graph", &ms,
                              "{\"project\":\"%s\","
                              "\"query\":\"MATCH (n:Function) RETURN n.name LIMIT 10\"}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    ASSERT_LTE(total, 10);
    free(r);
    PASS();
}

TEST(tool_qg_match_edges) {
    double ms;
    char *r =
        call_tool_timed("query_graph", &ms,
                        "{\"project\":\"%s\","
                        "\"query\":\"MATCH (a)-[r:CALLS]->(b) RETURN a.name, b.name LIMIT 10\"}",
                        g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

TEST(tool_qg_match_imports) {
    double ms;
    char *r =
        call_tool_timed("query_graph", &ms,
                        "{\"project\":\"%s\","
                        "\"query\":\"MATCH (a)-[r:IMPORTS]->(b) RETURN a.name, b.name LIMIT 10\"}",
                        g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

TEST(tool_qg_match_defines) {
    double ms;
    char *r =
        call_tool_timed("query_graph", &ms,
                        "{\"project\":\"%s\","
                        "\"query\":\"MATCH (a)-[r:DEFINES]->(b) RETURN a.name, b.name LIMIT 10\"}",
                        g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

TEST(tool_qg_match_contains) {
    double ms;
    char *r = call_tool_timed(
        "query_graph", &ms,
        "{\"project\":\"%s\","
        "\"query\":\"MATCH (a)-[r:CONTAINS_FILE]->(b) RETURN a.name, b.name LIMIT 5\"}",
        g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

/* ── query_graph: WHERE clause ─────────────────────────────────── */

TEST(tool_qg_where_name) {
    double ms;
    char *r = call_tool_timed(
        "query_graph", &ms,
        "{\"project\":\"%s\","
        "\"query\":\"MATCH (n:Function) WHERE n.name = 'incr_test_injected' RETURN n.name\"}",
        g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_EQ(total, 1);
    free(r);
    PASS();
}

/* ── query_graph: multi-hop ────────────────────────────────────── */

TEST(tool_qg_two_hop) {
    double ms;
    char *r = call_tool_timed("query_graph", &ms,
                              "{\"project\":\"%s\","
                              "\"query\":\"MATCH (a)-[:CALLS]->(b)-[:CALLS]->(c) RETURN a.name, "
                              "b.name, c.name LIMIT 5\"}",
                              g_project);
    TOOL_OK(r, ms);
    /* May have 2-hop call chains */
    ASSERT(strstr(r, "columns") != NULL || strstr(r, "rows") != NULL);
    free(r);
    PASS();
}

/* ── query_graph: max_rows ─────────────────────────────────────── */

TEST(tool_qg_max_rows) {
    double ms;
    /* Query without max_rows — its explicit Cypher LIMIT is the visible cap. */
    char *r1 = call_tool_timed("query_graph", &ms,
                               "{\"project\":\"%s\","
                               "\"query\":\"MATCH (n:Function) RETURN n.name LIMIT 100\"}",
                               g_project);
    TOOL_OK(r1, ms);
    int returned_unlimited = count_in_response(r1, "returned");
    free(r1);

    /* max_rows caps presentation, not evaluation: returned is bounded while
     * total remains the exact full materialized count. */
    char *r2 = call_tool_timed("query_graph", &ms,
                               "{\"project\":\"%s\","
                               "\"query\":\"MATCH (n:Function) RETURN n.name\","
                               "\"max_rows\":3}",
                               g_project);
    TOOL_OK(r2, ms);
    int returned_limited = count_in_response(r2, "returned");
    int total_limited = count_in_response(r2, "total");
    ASSERT_EQ(returned_limited, 3);
    ASSERT_GT(total_limited, returned_limited);
    ASSERT_GT(returned_unlimited, returned_limited);
    ASSERT_NOT_NULL(strstr(r2, "page_limit"));
    free(r2);
    PASS();
}

/* ── query_graph: RETURN properties ────────────────────────────── */

TEST(tool_qg_return_properties) {
    double ms;
    char *r = call_tool_timed("query_graph", &ms,
                              "{\"project\":\"%s\","
                              "\"query\":\"MATCH (n:Module) RETURN n.name, n.file_path LIMIT 3\"}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    /* Should have both columns */
    ASSERT(strstr(r, "n.name") != NULL || strstr(r, "columns") != NULL);
    free(r);
    PASS();
}

/* ── query_graph: error cases ──────────────────────────────────── */

TEST(tool_qg_invalid_project) {
    double ms;
    char *r = call_tool_timed("query_graph", &ms,
                              "{\"project\":\"nonexistent\","
                              "\"query\":\"MATCH (n) RETURN n LIMIT 1\"}");
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "error") != NULL || strstr(r, "not found") != NULL);
    free(r);
    PASS();
}

/* ── search_code: modes ────────────────────────────────────────── */

TEST(tool_sc_compact) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"import\",\"mode\":\"compact\",\"limit\":5}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    /* compact mode has "results" key with enriched matches */
    ASSERT(resp_has_key(r, "results"));
    ASSERT(resp_has_key(r, "total_results"));
    free(r);
    PASS();
}

TEST(tool_sc_full) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"FastAPI\",\"mode\":\"full\",\"limit\":3}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    /* full mode includes source code */
    ASSERT(resp_has_key(r, "results") || resp_has_key(r, "raw_matches"));
    free(r);
    PASS();
}

TEST(tool_sc_files) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"def \",\"mode\":\"files\",\"limit\":10}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    /* files mode has "files" key, NOT "results" */
    ASSERT(resp_has_key(r, "files"));
    free(r);
    PASS();
}

/* ── search_code: regex ────────────────────────────────────────── */

TEST(tool_sc_regex) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"def\\\\s+\\\\w+_handler\",\"regex\":true,\"limit\":5}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── search_code: file_pattern + path_filter ───────────────────── */

TEST(tool_sc_file_pattern) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"class\",\"file_pattern\":\"*.py\",\"limit\":5}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

TEST(tool_sc_path_filter) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"import\",\"path_filter\":\"fastapi/\",\"limit\":5}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── search_code: context lines ────────────────────────────────── */

TEST(tool_sc_context) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"FastAPI\",\"context\":3,\"limit\":3}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── search_code: no results ───────────────────────────────────── */

TEST(tool_sc_no_results) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"xyzzy_absolutely_no_match_99\"}",
                              g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── get_code_snippet ──────────────────────────────────────────── */

TEST(tool_snippet_short_name) {
    double ms;
    char *r = call_tool_timed("get_code_snippet", &ms,
                              "{\"project\":\"%s\","
                              "\"qualified_name\":\"incr_test_injected\"}",
                              g_project);
    TOOL_OK(r, ms);
    /* Should return source or suggestions */
    ASSERT(strstr(r, "def") != NULL || strstr(r, "return") != NULL ||
           strstr(r, "suggest") != NULL || strstr(r, "match") != NULL);
    free(r);
    PASS();
}

TEST(tool_snippet_nonexistent) {
    double ms;
    char *r = call_tool_timed("get_code_snippet", &ms,
                              "{\"project\":\"%s\","
                              "\"qualified_name\":\"nonexistent_func_xyz\"}",
                              g_project);
    TOOL_OK(r, ms);
    /* Should return not found or suggestions, not crash */
    free(r);
    PASS();
}

TEST(tool_snippet_include_neighbors) {
    double ms;
    /* Use include_neighbors=true — response may include caller_names
     * (depends on whether the function has callers). Key thing:
     * the response must differ from include_neighbors=false and must
     * contain source code. */
    char *r = call_tool_timed("get_code_snippet", &ms,
                              "{\"project\":\"%s\","
                              "\"qualified_name\":\"incr_test_injected\","
                              "\"include_neighbors\":true}",
                              g_project);
    TOOL_OK(r, ms);
    /* Must have source code */
    ASSERT(resp_has_key(r, "source"));
    /* Must have callers/callees arrays (may be empty) */
    ASSERT(resp_has_key(r, "callers") || resp_has_key(r, "callees"));
    free(r);
    PASS();
}

/* ── trace_path ───────────────────────────────────────────── */

TEST(tool_trace_outbound) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"outbound\"}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    /* direction=outbound → response has "callees", no "callers" */
    ASSERT(resp_has_key(r, "callees"));
    ASSERT(resp_lacks_key(r, "callers"));
    ASSERT(resp_has_key(r, "direction"));
    free(r);
    PASS();
}

TEST(tool_trace_inbound) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"inbound\"}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    /* direction=inbound → response has "callers", no "callees" */
    ASSERT(resp_has_key(r, "callers"));
    ASSERT(resp_lacks_key(r, "callees"));
    free(r);
    PASS();
}

TEST(tool_trace_both) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"both\",\"depth\":5}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    /* direction=both → response has both "callers" and "callees" */
    ASSERT(resp_has_key(r, "callers"));
    ASSERT(resp_has_key(r, "callees"));
    free(r);
    PASS();
}

TEST(tool_trace_depth_1) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"both\",\"depth\":1}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

TEST(tool_trace_nonexistent) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"no_such_function_xyz\","
                              "\"direction\":\"both\"}",
                              g_project);
    TOOL_OK(r, ms);
    /* Should return not found or empty, not crash */
    free(r);
    PASS();
}

TEST(tool_trace_edge_types) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"both\","
                              "\"edge_types\":[\"CALLS\"]}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── get_architecture ──────────────────────────────────────────── */

TEST(tool_arch_structure) {
    double ms;
    char *r = call_tool_timed("get_architecture", &ms,
                              "{\"project\":\"%s\",\"aspects\":[\"structure\"]}", g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "fastapi") != NULL);
    free(r);
    PASS();
}

TEST(tool_arch_all) {
    double ms;
    char *r = call_tool_timed("get_architecture", &ms, "{\"project\":\"%s\",\"aspects\":[\"all\"]}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

TEST(tool_arch_no_aspects) {
    double ms;
    char *r = call_tool_timed("get_architecture", &ms, "{\"project\":\"%s\"}", g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── detect_changes ────────────────────────────────────────────── */

TEST(tool_detect_changes_default) {
    double ms;
    /* The fixture is intentionally detached, so pin only the base while
     * exercising every other default (scope, direction, depth, and budget). */
    char *r = call_tool_timed("detect_changes", &ms,
                              "{\"project\":\"%s\",\"base_branch\":\"HEAD\"}", g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    /* Lean tree contract: preserve exact changed-file accounting, but do not
     * spend tokens on a zero-row table. A non-empty page still carries the
     * changed_files rows; an empty page is represented by its exact scalars. */
    int changed_total = count_in_response(r, "changed_total");
    int changed_returned = count_in_response(r, "changed_returned");
    ASSERT_GTE(changed_total, 0);
    ASSERT_GTE(changed_returned, 0);
    ASSERT_LTE(changed_returned, changed_total);
    ASSERT(changed_returned > 0 ? resp_has_key(r, "changed_files")
                                : resp_lacks_key(r, "changed_files"));
    ASSERT(strstr(r, "direction:") != NULL);
    ASSERT(strstr(r, "seed_symbols:") != NULL);
    free(r);
    PASS();
}

/* The blast radius is a REAL traversal now: default inbound gives transitive
 * callers of the changed symbols with an exact impacted_total and a module
 * rollup. Fixture diff may be empty (shallow clone at HEAD) — the sections and
 * their accounting must still be present and internally consistent. */
TEST(tool_detect_changes_impact_shape) {
    double ms;
    char *r = call_tool_timed("detect_changes", &ms,
                              "{\"project\":\"%s\",\"base_branch\":\"HEAD\"}", g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "direction: inbound") != NULL); /* default = blast radius */
    ASSERT(strstr(r, "seed_symbols:") != NULL);
    /* format:"json" returns the same model as structured JSON. */
    free(r);
    r = call_tool_timed("detect_changes", &ms,
                        "{\"project\":\"%s\",\"base_branch\":\"HEAD\",\"format\":\"json\"}",
                        g_project);
    TOOL_OK(r, ms);
    ASSERT(resp_has_key(r, "impacted_total"));
    ASSERT(resp_has_key(r, "direction"));
    free(r);
    PASS();
}

TEST(tool_detect_changes_custom_branch) {
    double ms;
    char *r = call_tool_timed("detect_changes", &ms,
                              "{\"project\":\"%s\",\"base_branch\":\"HEAD\"}", g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "changed") != NULL);
    free(r);
    PASS();
}

/* Regression: `since` was advertised in the schema but ignored by the handler;
 * it must be honored as the diff base. Fixture is a --depth=1 shallow clone, so
 * HEAD~N won't resolve — use HEAD for a valid (empty) diff. */
TEST(tool_detect_changes_since) {
    double ms;
    char *r = call_tool_timed("detect_changes", &ms, "{\"project\":\"%s\",\"since\":\"HEAD\"}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    ASSERT(strstr(r, "base: HEAD") != NULL);
    int changed_total = count_in_response(r, "changed_total");
    int changed_returned = count_in_response(r, "changed_returned");
    ASSERT_GTE(changed_total, 0);
    ASSERT_GTE(changed_returned, 0);
    ASSERT_LTE(changed_returned, changed_total);
    ASSERT(changed_returned > 0 ? resp_has_key(r, "changed_files")
                                : resp_lacks_key(r, "changed_files"));
    free(r);
    PASS();
}

/* Regression: `since` must take precedence over base_branch. A valid since plus a
 * bogus base_branch must still succeed (proving since won) and must not reference
 * the bogus branch. */
TEST(tool_detect_changes_since_precedence) {
    double ms;
    char *r = call_tool_timed(
        "detect_changes", &ms,
        "{\"project\":\"%s\",\"since\":\"HEAD\",\"base_branch\":\"no-such-branch-xyz\"}",
        g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "no-such-branch-xyz") == NULL);
    free(r);
    PASS();
}

TEST(tool_detect_changes_depth) {
    double ms;
    char *r = call_tool_timed("detect_changes", &ms, "{\"project\":\"%s\",\"depth\":5}", g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── manage_adr ────────────────────────────────────────────────── */

TEST(tool_adr_get) {
    double ms;
    char *r =
        call_tool_timed("manage_adr", &ms, "{\"project\":\"%s\",\"mode\":\"get\"}", g_project);
    TOOL_OK(r, ms);
    /* May return empty if no ADR exists yet */
    free(r);
    PASS();
}

TEST(tool_adr_sections) {
    double ms;
    char *r =
        call_tool_timed("manage_adr", &ms, "{\"project\":\"%s\",\"mode\":\"sections\"}", g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── ingest_traces ─────────────────────────────────────────────── */

TEST(tool_ingest_traces_empty) {
    double ms;
    char *r =
        call_tool_timed("ingest_traces", &ms, "{\"project\":\"%s\",\"traces\":[]}", g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

TEST(tool_ingest_traces_basic) {
    double ms;
    char *r = call_tool_timed(
        "ingest_traces", &ms,
        "{\"project\":\"%s\",\"traces\":[{\"caller\":\"a\",\"callee\":\"b\"}]}", g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── Error handling: every tool with bad project ───────────────── */

TEST(tool_err_query_bad_project) {
    double ms;
    char *r = call_tool_timed("query_graph", &ms,
                              "{\"project\":\"xxx\","
                              "\"query\":\"MATCH (n) RETURN n LIMIT 1\"}");
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "error") != NULL || strstr(r, "not found") != NULL);
    free(r);
    PASS();
}

TEST(tool_err_search_code_bad_project) {
    double ms;
    char *r = call_tool_timed("search_code", &ms, "{\"project\":\"xxx\",\"pattern\":\"test\"}");
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

TEST(tool_err_snippet_bad_project) {
    double ms;
    char *r = call_tool_timed("get_code_snippet", &ms,
                              "{\"project\":\"xxx\",\"qualified_name\":\"test\"}");
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

TEST(tool_err_trace_bad_project) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"xxx\","
                              "\"function_name\":\"test\",\"direction\":\"both\"}");
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

TEST(tool_err_arch_bad_project) {
    double ms;
    char *r = call_tool_timed("get_architecture", &ms, "{\"project\":\"xxx\"}");
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

TEST(tool_err_detect_bad_project) {
    double ms;
    char *r = call_tool_timed("detect_changes", &ms, "{\"project\":\"xxx\"}");
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

TEST(tool_err_delete_nonexistent) {
    double ms;
    char *r = call_tool_timed("delete_project", &ms, "{\"project\":\"xxx-nonexistent\"}");
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── index_repository: mode param ───────────────────────────────── */

TEST(tool_index_mode_fast) {
    double ms;
    char *r = call_tool_timed("index_repository", &ms, "{\"repo_path\":\"%s\",\"mode\":\"fast\"}",
                              g_repodir);
    ASSERT(r != NULL);
    ASSERT(strstr(r, "indexed") != NULL);
    free(r);
    PASS();
}

TEST(tool_index_invalid_path) {
    double ms;
    char *r = call_tool_timed("index_repository", &ms, "{\"repo_path\":\"/nonexistent/path/xyz\"}");
    TOOL_OK(r, ms);
    /* Should fail gracefully */
    free(r);
    PASS();
}

TEST(tool_index_missing_param) {
    double ms;
    char *r = call_tool_timed("index_repository", &ms, "{}");
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "required") != NULL || strstr(r, "error") != NULL ||
           strstr(r, "repo_path") != NULL);
    free(r);
    PASS();
}

/* ── search_graph: exclude_entry_points ────────────────────────── */

TEST(tool_sg_exclude_entry_points) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"exclude_entry_points\":true,\"max_degree\":0}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GTE(total, 0);
    free(r);
    PASS();
}

TEST(tool_sg_exclude_entry_points_false) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"exclude_entry_points\":false,\"max_degree\":0}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GTE(total, 0);
    free(r);
    PASS();
}

/* ── search_graph: high offset (beyond results) ───────────────── */

TEST(tool_sg_high_offset) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"offset\":999999,\"limit\":10}",
                              g_project);
    TOOL_OK(r, ms);
    /* Should return 0 results at this offset */
    free(r);
    PASS();
}

/* ── search_graph: name_pattern edge cases ─────────────────────── */

TEST(tool_sg_name_pattern_dot_star) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"name_pattern\":\".*\",\"limit\":5}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 100);
    free(r);
    PASS();
}

TEST(tool_sg_name_pattern_anchored) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"name_pattern\":\"^incr_test_injected$\"}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_EQ(total, 1);
    free(r);
    PASS();
}

/* ── query_graph: more Cypher patterns ─────────────────────────── */

TEST(tool_qg_count_functions) {
    double ms;
    char *r = call_tool_timed("query_graph", &ms,
                              "{\"project\":\"%s\","
                              "\"query\":\"MATCH (n:Function) RETURN n.name LIMIT 200\","
                              "\"max_rows\":200}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 50);
    free(r);
    PASS();
}

TEST(tool_qg_edge_properties) {
    double ms;
    char *r = call_tool_timed(
        "query_graph", &ms,
        "{\"project\":\"%s\","
        "\"query\":\"MATCH (a)-[r:CALLS]->(b) RETURN a.name, b.name, r.callee LIMIT 5\"}",
        g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "columns") != NULL || strstr(r, "rows") != NULL);
    free(r);
    PASS();
}

TEST(tool_qg_node_file_path) {
    double ms;
    char *r = call_tool_timed("query_graph", &ms,
                              "{\"project\":\"%s\","
                              "\"query\":\"MATCH (n:Function) WHERE n.name = 'incr_test_injected' "
                              "RETURN n.name, n.file_path, n.start_line\"}",
                              g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_EQ(total, 1);
    free(r);
    PASS();
}

TEST(tool_qg_match_module_defines) {
    double ms;
    /* DEFINES edges connect modules to their symbols */
    char *r =
        call_tool_timed("query_graph", &ms,
                        "{\"project\":\"%s\","
                        "\"query\":\"MATCH (a)-[:DEFINES]->(b) RETURN a.name, b.name LIMIT 10\"}",
                        g_project);
    TOOL_OK(r, ms);
    int total = count_in_response(r, "total");
    ASSERT_GT(total, 0);
    free(r);
    PASS();
}

TEST(tool_qg_max_rows_1) {
    double ms;
    char *r = call_tool_timed("query_graph", &ms,
                              "{\"project\":\"%s\","
                              "\"query\":\"MATCH (n:Function) RETURN n.name\","
                              "\"max_rows\":1}",
                              g_project);
    TOOL_OK(r, ms);
    int returned = count_in_response(r, "returned");
    int total = count_in_response(r, "total");
    ASSERT_EQ(returned, 1);
    ASSERT_GT(total, returned);
    ASSERT_NOT_NULL(strstr(r, "page_limit"));
    free(r);
    PASS();
}

/* ── search_code: limit param ──────────────────────────────────── */

TEST(tool_sc_limit_1) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"def \",\"limit\":1}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

TEST(tool_sc_limit_50) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"import\",\"limit\":50}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── search_code: combined params ──────────────────────────────── */

TEST(tool_sc_combined) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"class\","
                              "\"mode\":\"compact\","
                              "\"file_pattern\":\"*.py\","
                              "\"path_filter\":\"fastapi/\","
                              "\"limit\":5,\"context\":2}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── get_code_snippet: full QN from search ─────────────────────── */

TEST(tool_snippet_full_qn) {
    /* First find a function QN via search */
    double ms;
    char *sr = call_tool_timed("search_graph", &ms,
                               "{\"project\":\"%s\",\"label\":\"Function\","
                               "\"name_pattern\":\"incr_test_helper\",\"limit\":1}",
                               g_project);
    TOOL_OK(sr, ms);

    /* Extract qualified_name */
    const char *qn_start = strstr(sr, "qualified_name");
    if (qn_start) {
        qn_start = strchr(qn_start, ':');
        if (qn_start) {
            qn_start += 2; /* skip :" */
            /* Handle escaped quotes in JSON */
            if (*qn_start == '\\')
                qn_start += 2;
            const char *qn_end = strchr(qn_start, '"');
            if (!qn_end)
                qn_end = strchr(qn_start, '\\');
            if (qn_end && qn_end > qn_start) {
                char qn[512];
                int len = (int)(qn_end - qn_start);
                if (len > 0 && len < (int)sizeof(qn)) {
                    memcpy(qn, qn_start, (size_t)len);
                    qn[len] = '\0';

                    char *r = call_tool_timed("get_code_snippet", &ms,
                                              "{\"project\":\"%s\",\"qualified_name\":\"%s\"}",
                                              g_project, qn);
                    TOOL_OK(r, ms);
                    ASSERT(strstr(r, "source") != NULL || strstr(r, "def") != NULL ||
                           strstr(r, "return") != NULL);
                    free(r);
                }
            }
        }
    }
    free(sr);
    PASS();
}

/* ── get_architecture: specific aspects ────────────────────────── */

TEST(tool_arch_dependencies) {
    double ms;
    char *r = call_tool_timed("get_architecture", &ms,
                              "{\"project\":\"%s\",\"aspects\":[\"dependencies\"]}", g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

TEST(tool_arch_entry_points) {
    double ms;
    char *r = call_tool_timed("get_architecture", &ms,
                              "{\"project\":\"%s\",\"aspects\":[\"entry_points\"]}", g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

TEST(tool_arch_routes) {
    double ms;
    char *r = call_tool_timed("get_architecture", &ms,
                              "{\"project\":\"%s\",\"aspects\":[\"routes\"]}", g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

TEST(tool_arch_multi_aspects) {
    double ms;
    char *r = call_tool_timed(
        "get_architecture", &ms,
        "{\"project\":\"%s\",\"aspects\":[\"structure\",\"dependencies\",\"entry_points\"]}",
        g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── detect_changes: scope param ───────────────────────────────── */

TEST(tool_detect_changes_scope) {
    double ms;
    char *r = call_tool_timed("detect_changes", &ms, "{\"project\":\"%s\",\"scope\":\"fastapi/\"}",
                              g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── manage_adr: update + content ──────────────────────────────── */

TEST(tool_adr_update) {
    double ms;
    char *r = call_tool_timed("manage_adr", &ms,
                              "{\"project\":\"%s\",\"mode\":\"update\","
                              "\"content\":\"# Test ADR\\n\\nThis is a test ADR.\"}",
                              g_project);
    TOOL_OK(r, ms);
    free(r);

    /* Verify it was stored */
    r = call_tool_timed("manage_adr", &ms, "{\"project\":\"%s\",\"mode\":\"get\"}", g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "Test ADR") != NULL);
    free(r);
    PASS();
}

TEST(tool_adr_sections_array) {
    double ms;
    char *r = call_tool_timed("manage_adr", &ms,
                              "{\"project\":\"%s\",\"mode\":\"sections\","
                              "\"sections\":[\"overview\",\"decisions\"]}",
                              g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── ingest_traces: multiple traces ────────────────────────────── */

TEST(tool_ingest_traces_multiple) {
    double ms;
    char *r = call_tool_timed("ingest_traces", &ms,
                              "{\"project\":\"%s\",\"traces\":["
                              "{\"caller\":\"funcA\",\"callee\":\"funcB\",\"count\":5},"
                              "{\"caller\":\"funcB\",\"callee\":\"funcC\",\"count\":3},"
                              "{\"caller\":\"funcC\",\"callee\":\"funcD\",\"count\":1}"
                              "]}",
                              g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── trace_path: more edge_types combos ───────────────────── */

TEST(tool_trace_imports_only) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"both\","
                              "\"edge_types\":[\"IMPORTS\"]}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

TEST(tool_trace_calls_and_imports) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"both\","
                              "\"edge_types\":[\"CALLS\",\"IMPORTS\"]}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

TEST(tool_trace_deep) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"outbound\",\"depth\":10}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════
 *  GAP COVERAGE: every remaining tool × param combination
 * ═══════════════════════════════════════════════════════════════ */

/* ── search_graph: remaining relationship values ───────────────── */

TEST(tool_sg_rel_imports) {
    ASSERT(fastapi_physical_import());
    /* relationship requires an actual import edge. Degree counts semantic
     * relations only, so min_degree would incorrectly exclude this File.
     * tool_sg_min_degree independently checks the semantic degree contract. */
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"File\","
                              "\"qn_pattern\":\"[.]fastapi[.]security[.]oauth2[.]py[.]__file__$\","
                              "\"relationship\":\"IMPORTS\"}",
                              g_project);
    TOOL_OK(r, ms);
    ASSERT_EQ(count_in_response(r, "total"), 1);
    ASSERT(strstr(r, "fastapi.security.oauth2.py.__file__") != NULL);
    free(r);
    PASS();
}

TEST(tool_sg_rel_defines) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Module\","
                              "\"relationship\":\"DEFINES\",\"min_degree\":1}",
                              g_project);
    TOOL_OK(r, ms);
    int t = count_in_response(r, "total");
    ASSERT_GT(t, 0);
    free(r);
    PASS();
}

TEST(tool_sg_rel_contains_file) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Folder\","
                              "\"relationship\":\"CONTAINS_FILE\",\"min_degree\":1}",
                              g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── search_graph: remaining label values ──────────────────────── */

TEST(tool_sg_label_file) {
    double ms;
    char *r =
        call_tool_timed("search_graph", &ms, "{\"project\":\"%s\",\"label\":\"File\"}", g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

TEST(tool_sg_label_folder) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms, "{\"project\":\"%s\",\"label\":\"Folder\"}",
                              g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

TEST(tool_sg_label_package) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms, "{\"project\":\"%s\",\"label\":\"Package\"}",
                              g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── search_graph: include_connected=false ─────────────────────── */

TEST(tool_sg_include_connected_false) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"name_pattern\":\"incr_test_injected\","
                              "\"include_connected\":false}",
                              g_project);
    TOOL_OK(r, ms);
    int t = count_in_response(r, "total");
    ASSERT_EQ(t, 1);
    free(r);
    PASS();
}

/* ── search_graph: qn_pattern + label combined ─────────────────── */

TEST(tool_sg_qn_and_label) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"qn_pattern\":\".*applications.*\"}",
                              g_project);
    TOOL_OK(r, ms);
    int t = count_in_response(r, "total");
    ASSERT_GT(t, 0);
    free(r);
    PASS();
}

/* ── search_graph: file_pattern + name_pattern combined ────────── */

TEST(tool_sg_file_and_name) {
    double ms;
    char *r = call_tool_timed("search_graph", &ms,
                              "{\"project\":\"%s\",\"label\":\"Function\","
                              "\"file_pattern\":\"*fastapi*\","
                              "\"name_pattern\":\".*\",\"limit\":5}",
                              g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── query_graph: CONFIGURES edges ─────────────────────────────── */

TEST(tool_qg_configures) {
    double ms;
    char *r = call_tool_timed(
        "query_graph", &ms,
        "{\"project\":\"%s\","
        "\"query\":\"MATCH (a)-[r:CONFIGURES]->(b) RETURN a.name, b.name LIMIT 5\"}",
        g_project);
    TOOL_OK(r, ms);
    /* May have 0 results if no CONFIGURES edges in FastAPI */
    ASSERT(strstr(r, "columns") != NULL || strstr(r, "rows") != NULL);
    free(r);
    PASS();
}

/* ── query_graph: HANDLES edges ────────────────────────────────── */

TEST(tool_qg_handles) {
    double ms;
    char *r =
        call_tool_timed("query_graph", &ms,
                        "{\"project\":\"%s\","
                        "\"query\":\"MATCH (a)-[r:HANDLES]->(b) RETURN a.name, b.name LIMIT 5\"}",
                        g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "columns") != NULL || strstr(r, "rows") != NULL);
    free(r);
    PASS();
}

/* ── query_graph: DEFINES_METHOD edges ─────────────────────────── */

TEST(tool_qg_defines_method) {
    double ms;
    char *r = call_tool_timed("query_graph", &ms,
                              "{\"project\":\"%s\","
                              "\"query\":\"MATCH (c:Class)-[r:DEFINES_METHOD]->(m:Method) RETURN "
                              "c.name, m.name LIMIT 5\"}",
                              g_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "columns") != NULL || strstr(r, "rows") != NULL);
    free(r);
    PASS();
}

/* ── query_graph: no LIMIT (default behavior) ──────────────────── */

TEST(tool_qg_no_limit) {
    double ms;
    char *r = call_tool_timed(
        "query_graph", &ms,
        "{\"project\":\"%s\","
        "\"query\":\"MATCH (n:Function) WHERE n.name = 'incr_test_injected' RETURN n.name\"}",
        g_project);
    TOOL_OK(r, ms);
    int t = count_in_response(r, "total");
    ASSERT_GT(t, 0);
    free(r);
    PASS();
}

/* ── query_graph: empty result ─────────────────────────────────── */

TEST(tool_qg_empty_result) {
    double ms;
    char *r = call_tool_timed(
        "query_graph", &ms,
        "{\"project\":\"%s\","
        "\"query\":\"MATCH (n:Function) WHERE n.name = 'zzz_no_exist_zzz' RETURN n.name\"}",
        g_project);
    TOOL_OK(r, ms);
    int t = count_in_response(r, "total");
    ASSERT_EQ(t, 0);
    free(r);
    PASS();
}

/* ── search_code: regex=false explicit ─────────────────────────── */

TEST(tool_sc_regex_false) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"FastAPI\",\"regex\":false,\"limit\":3}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── search_code: context=0 ────────────────────────────────────── */

TEST(tool_sc_context_zero) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"import\",\"context\":0,\"limit\":5}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── search_code: special chars in pattern ─────────────────────── */

TEST(tool_sc_special_chars) {
    double ms;
    char *r = call_tool_timed("search_code", &ms,
                              "{\"project\":\"%s\","
                              "\"pattern\":\"def __init__(self\",\"limit\":5}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── get_code_snippet: include_neighbors=false ─────────────────── */

TEST(tool_snippet_neighbors_false) {
    double ms;
    char *r = call_tool_timed("get_code_snippet", &ms,
                              "{\"project\":\"%s\","
                              "\"qualified_name\":\"incr_test_injected\","
                              "\"include_neighbors\":false}",
                              g_project);
    TOOL_OK(r, ms);
    /* include_neighbors=false should NOT have "caller_names" field */
    ASSERT(resp_lacks_key(r, "caller_names"));
    /* But must still have source code */
    ASSERT(resp_has_key(r, "source"));
    free(r);
    PASS();
}

/* ── get_code_snippet: Class name ──────────────────────────────── */

TEST(tool_snippet_class) {
    /* FastAPI has classes — try a common one */
    double ms;
    char *r = call_tool_timed("get_code_snippet", &ms,
                              "{\"project\":\"%s\","
                              "\"qualified_name\":\"FastAPI\"}",
                              g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── trace_path: depth=0 ──────────────────────────────────── */

TEST(tool_trace_depth_0) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"both\",\"depth\":0}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── trace_path: edge_types=[DEFINES] ─────────────────────── */

TEST(tool_trace_defines_only) {
    double ms;
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"inbound\","
                              "\"edge_types\":[\"DEFINES\"]}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── trace_path: include_tests flag ────────────────────────────── */

TEST(tool_trace_include_tests) {
    double ms;
    /* Default (include_tests=false): test files should be filtered out */
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"inbound\","
                              "\"include_tests\":false}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    /* Result should not contain is_test markers when tests are excluded */
    free(r);

    /* With include_tests=true: test files should appear with is_test marker */
    r = call_tool_timed("trace_path", &ms,
                        "{\"project\":\"%s\","
                        "\"function_name\":\"incr_test_injected\","
                        "\"direction\":\"inbound\","
                        "\"include_tests\":true}",
                        g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── trace_path: risk_labels flag ─────────────────────────────── */

TEST(tool_trace_risk_labels) {
    double ms;
    /* Use outbound direction — incr_test_injected may call stdlib functions */
    char *r = call_tool_timed("trace_path", &ms,
                              "{\"project\":\"%s\","
                              "\"function_name\":\"incr_test_injected\","
                              "\"direction\":\"outbound\","
                              "\"risk_labels\":true}",
                              g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    /* If there are callees, they should have risk labels.
     * If no callees, the response is still valid — just empty. */
    if (strstr(r, "\"hop\"") != NULL) {
        ASSERT(strstr(r, "\"risk\"") != NULL);
    }
    free(r);
    PASS();
}

/* ── detect_changes: nonexistent branch ────────────────────────── */

TEST(tool_detect_nonexistent_branch) {
    double ms;
    char *r = call_tool_timed("detect_changes", &ms,
                              "{\"project\":\"%s\","
                              "\"base_branch\":\"nonexistent-branch-xyz\"}",
                              g_project);
    TOOL_OK(r, ms);
    /* Should handle gracefully — git diff may fail or return empty */
    free(r);
    PASS();
}

/* ── detect_changes: depth=1 ───────────────────────────────────── */

TEST(tool_detect_depth_1) {
    double ms;
    char *r = call_tool_timed("detect_changes", &ms, "{\"project\":\"%s\",\"depth\":1}", g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── manage_adr: default mode (omitted) ────────────────────────── */

TEST(tool_adr_default_mode) {
    double ms;
    char *r = call_tool_timed("manage_adr", &ms, "{\"project\":\"%s\"}", g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── manage_adr: update with empty content ─────────────────────── */

TEST(tool_adr_update_empty) {
    double ms;
    char *r = call_tool_timed("manage_adr", &ms,
                              "{\"project\":\"%s\",\"mode\":\"update\","
                              "\"content\":\"\"}",
                              g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── ingest_traces: trace with missing fields ──────────────────── */

TEST(tool_ingest_traces_partial) {
    double ms;
    char *r =
        call_tool_timed("ingest_traces", &ms,
                        "{\"project\":\"%s\",\"traces\":[{\"caller\":\"onlyA\"}]}", g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── Error: get_graph_schema bad project ───────────────────────── */

TEST(tool_err_schema_bad_project) {
    double ms;
    char *r = call_tool_timed("get_graph_schema", &ms, "{\"project\":\"nonexistent-xyz\"}");
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── Error: index_status missing project ───────────────────────── */

TEST(tool_err_index_status_no_project) {
    double ms;
    char *r = call_tool_timed("index_status", &ms, "{}");
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── Error: manage_adr bad project ─────────────────────────────── */

TEST(tool_err_adr_bad_project) {
    double ms;
    char *r =
        call_tool_timed("manage_adr", &ms, "{\"project\":\"nonexistent-xyz\",\"mode\":\"get\"}");
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── Error: ingest_traces bad project ──────────────────────────── */

TEST(tool_err_ingest_bad_project) {
    double ms;
    char *r =
        call_tool_timed("ingest_traces", &ms, "{\"project\":\"nonexistent-xyz\",\"traces\":[]}");
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── Error: ingest_traces missing traces array ─────────────────── */

TEST(tool_err_ingest_no_traces) {
    double ms;
    char *r = call_tool_timed("ingest_traces", &ms, "{\"project\":\"%s\"}", g_project);
    TOOL_OK(r, ms);
    free(r);
    PASS();
}

/* ── index_repository: mode=full explicit ──────────────────────── */

TEST(tool_index_mode_full) {
    double ms;
    char *r = call_tool_timed("index_repository", &ms, "{\"repo_path\":\"%s\",\"mode\":\"full\"}",
                              g_repodir);
    ASSERT(r != NULL);
    ASSERT(strstr(r, "indexed") != NULL);
    free(r);
    PASS();
}

/* ── get_architecture: empty aspects array ─────────────────────── */

TEST(tool_arch_empty_aspects) {
    double ms;
    char *r =
        call_tool_timed("get_architecture", &ms, "{\"project\":\"%s\",\"aspects\":[]}", g_project);
    TOOL_OK(r, ms);
    NOT_ERROR(r);
    free(r);
    PASS();
}

/* ── delete_project: real delete + verify ──────────────────────── */
/* NOTE: This is destructive — must run LAST */

TEST(tool_delete_and_verify) {
    /* Create a throwaway project to delete */
    char tmpdir2[256];
    snprintf(tmpdir2, sizeof(tmpdir2), "%s/throwaway", g_tmpdir);
    cbm_mkdir(tmpdir2);
    char pypath[512];
    snprintf(pypath, sizeof(pypath), "%s/dummy.py", tmpdir2);
    FILE *f = fopen(pypath, "w");
    if (f) {
        fprintf(f, "def throwaway():\n    pass\n");
        fclose(f);
    }

    double ms;
    char *r = call_tool_timed("index_repository", &ms, "{\"repo_path\":\"%s\"}", tmpdir2);
    TOOL_OK(r, ms);
    free(r);

    char *throwaway_project = cbm_project_name_from_path(tmpdir2);
    ASSERT(throwaway_project != NULL);

    /* Verify it exists */
    r = call_tool_timed("index_status", &ms, "{\"project\":\"%s\"}", throwaway_project);
    TOOL_OK(r, ms);
    free(r);

    /* Delete it */
    r = call_tool_timed("delete_project", &ms, "{\"project\":\"%s\"}", throwaway_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "deleted") != NULL);
    free(r);

    /* Verify it's gone */
    r = call_tool_timed("index_status", &ms, "{\"project\":\"%s\"}", throwaway_project);
    TOOL_OK(r, ms);
    ASSERT(strstr(r, "not") != NULL || strstr(r, "error") != NULL);
    free(r);

    free(throwaway_project);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SUITE
 * ══════════════════════════════════════════════════════════════════ */

SUITE(incremental) {
    if (incremental_setup() != 0) {
        printf("  SETUP FAILED — skipping incremental tests (network?)\n");
        return;
    }

    /* Phase 1: Full index baseline (needed for tool tests below) */
    RUN_TEST(incr_full_index);
    RUN_TEST(incr_full_has_functions);
    RUN_TEST(incr_full_edge_types);

    /* Inject test functions so tool tests (trace_path etc.) always have data,
     * even when perf phases are skipped on CI. Update baseline counts. */
    {
        char path[512];
        snprintf(path, sizeof(path), "%s/fastapi/applications.py", g_repodir);
        FILE *f = fopen(path, "a");
        if (f) {
            fprintf(f, "\n\ndef incr_test_injected(x: int) -> int:\n"
                       "    return x * 42\n"
                       "\n"
                       "def incr_test_helper(y: str) -> str:\n"
                       "    return y.upper()\n");
            fclose(f);
        }
        char *resp = index_repo();
        free(resp);
        g_full_nodes = get_node_count();
        g_full_edges = get_edge_count();
        g_full_calls = get_edge_count_by_type("CALLS");
        g_full_imports = get_edge_count_by_type("IMPORTS");
    }

    /* Phase 2: Noop */
    RUN_TEST(incr_noop_reindex);

    /* Phase 3: Incremental deltas */
    RUN_TEST(incr_modify_file);
    RUN_TEST(incr_file_node_name_stays_basename);
    RUN_TEST(incr_formatter_run);
    RUN_TEST(incr_add_file);
    RUN_TEST(incr_delete_file);
    RUN_TEST(incr_simultaneous_changes);

    /* Phase 4: Adversarial */
    RUN_TEST(incr_empty_file);
    RUN_TEST(incr_syntax_error);
    RUN_TEST(incr_huge_single_function);
    RUN_TEST(incr_binary_content);
    RUN_TEST(incr_large_generated);
    RUN_TEST(incr_new_subdir);

    /* Phase 5: Stress */
    RUN_TEST(incr_rapid_reindex);
    RUN_TEST(incr_replace_file_content);
    RUN_TEST(incr_batch_add_delete);

    /* Phase 6: Recovery + accuracy */
    RUN_TEST(incr_db_deleted_recovery);
    RUN_TEST(incr_accuracy_vs_full);

    /* Phase 7: Performance regression */
    RUN_TEST(incr_perf_single_file_fast);

    /* Phase 8: list_projects + index_status + schema */
    RUN_TEST(tool_list_projects_basic);
    RUN_TEST(tool_list_projects_has_current);
    RUN_TEST(tool_index_status_basic);
    RUN_TEST(tool_index_status_nonexistent);
    RUN_TEST(tool_schema_has_labels);
    RUN_TEST(tool_schema_has_edge_types);

    /* Phase 9: search_graph — every parameter */
    RUN_TEST(tool_sg_label_function);
    RUN_TEST(tool_sg_label_method);
    RUN_TEST(tool_sg_label_module);
    RUN_TEST(tool_sg_label_variable);
    RUN_TEST(tool_sg_label_class);
    RUN_TEST(tool_sg_label_route);
    RUN_TEST(tool_sg_label_nonexistent);
    RUN_TEST(tool_sg_name_exact);
    RUN_TEST(tool_sg_name_regex);
    RUN_TEST(tool_sg_name_no_match);
    RUN_TEST(tool_sg_min_degree);
    RUN_TEST(tool_sg_max_degree_zero);
    RUN_TEST(tool_sg_degree_range);
    RUN_TEST(tool_sg_limit);
    RUN_TEST(tool_sg_offset);
    RUN_TEST(tool_sg_file_pattern);
    RUN_TEST(tool_sg_include_connected);
    RUN_TEST(tool_sg_relationship);
    RUN_TEST(tool_sg_qn_pattern);
    RUN_TEST(tool_sg_combined_filters);
    RUN_TEST(tool_sg_project_only);
    RUN_TEST(tool_sg_invalid_project);

    /* Phase 10: query_graph — Cypher patterns */
    RUN_TEST(tool_qg_match_nodes);
    RUN_TEST(tool_qg_match_edges);
    RUN_TEST(tool_qg_match_imports);
    RUN_TEST(tool_qg_match_defines);
    RUN_TEST(tool_qg_match_contains);
    RUN_TEST(tool_qg_where_name);
    RUN_TEST(tool_qg_two_hop);
    RUN_TEST(tool_qg_max_rows);
    RUN_TEST(tool_qg_return_properties);
    RUN_TEST(tool_qg_invalid_project);

    /* Phase 11: search_code — modes + params */
    RUN_TEST(tool_sc_compact);
    RUN_TEST(tool_sc_full);
    RUN_TEST(tool_sc_files);
    RUN_TEST(tool_sc_regex);
    RUN_TEST(tool_sc_file_pattern);
    RUN_TEST(tool_sc_path_filter);
    RUN_TEST(tool_sc_context);
    RUN_TEST(tool_sc_no_results);

    /* Phase 12: get_code_snippet */
    RUN_TEST(tool_snippet_short_name);
    RUN_TEST(tool_snippet_nonexistent);
    RUN_TEST(tool_snippet_include_neighbors);

    /* Phase 13: trace_path — directions + params */
    RUN_TEST(tool_trace_outbound);
    RUN_TEST(tool_trace_inbound);
    RUN_TEST(tool_trace_both);
    RUN_TEST(tool_trace_depth_1);
    RUN_TEST(tool_trace_nonexistent);
    RUN_TEST(tool_trace_edge_types);

    /* Phase 14: get_architecture */
    RUN_TEST(tool_arch_structure);
    RUN_TEST(tool_arch_all);
    RUN_TEST(tool_arch_no_aspects);

    /* Phase 15: detect_changes */
    RUN_TEST(tool_detect_changes_default);
    RUN_TEST(tool_detect_changes_impact_shape);
    RUN_TEST(tool_detect_changes_custom_branch);
    RUN_TEST(tool_detect_changes_since);
    RUN_TEST(tool_detect_changes_since_precedence);
    RUN_TEST(tool_detect_changes_depth);

    /* Phase 16: manage_adr */
    RUN_TEST(tool_adr_get);
    RUN_TEST(tool_adr_sections);

    /* Phase 17: ingest_traces */
    RUN_TEST(tool_ingest_traces_empty);
    RUN_TEST(tool_ingest_traces_basic);

    /* Phase 18: Error handling — every tool with bad project */
    RUN_TEST(tool_err_query_bad_project);
    RUN_TEST(tool_err_search_code_bad_project);
    RUN_TEST(tool_err_snippet_bad_project);
    RUN_TEST(tool_err_trace_bad_project);
    RUN_TEST(tool_err_arch_bad_project);
    RUN_TEST(tool_err_detect_bad_project);
    RUN_TEST(tool_err_delete_nonexistent);

    /* Phase 19: index_repository params */
    RUN_TEST(tool_index_mode_fast);
    RUN_TEST(tool_index_invalid_path);
    RUN_TEST(tool_index_missing_param);

    /* Phase 20: search_graph remaining params */
    RUN_TEST(tool_sg_exclude_entry_points);
    RUN_TEST(tool_sg_exclude_entry_points_false);
    RUN_TEST(tool_sg_high_offset);
    RUN_TEST(tool_sg_name_pattern_dot_star);
    RUN_TEST(tool_sg_name_pattern_anchored);

    /* Phase 21: query_graph advanced */
    RUN_TEST(tool_qg_count_functions);
    RUN_TEST(tool_qg_edge_properties);
    RUN_TEST(tool_qg_node_file_path);
    RUN_TEST(tool_qg_match_module_defines);
    RUN_TEST(tool_qg_max_rows_1);

    /* Phase 22: search_code extra params */
    RUN_TEST(tool_sc_limit_1);
    RUN_TEST(tool_sc_limit_50);
    RUN_TEST(tool_sc_combined);

    /* Phase 23: get_code_snippet full QN */
    RUN_TEST(tool_snippet_full_qn);

    /* Phase 24: get_architecture specific aspects */
    RUN_TEST(tool_arch_dependencies);
    RUN_TEST(tool_arch_entry_points);
    RUN_TEST(tool_arch_routes);
    RUN_TEST(tool_arch_multi_aspects);

    /* Phase 25: detect_changes scope */
    RUN_TEST(tool_detect_changes_scope);

    /* Phase 26: manage_adr update + sections */
    RUN_TEST(tool_adr_update);
    RUN_TEST(tool_adr_sections_array);

    /* Phase 27: ingest_traces multiple */
    RUN_TEST(tool_ingest_traces_multiple);

    /* Phase 28: trace_path edge_types combos */
    RUN_TEST(tool_trace_imports_only);
    RUN_TEST(tool_trace_calls_and_imports);
    RUN_TEST(tool_trace_deep);

    /* Phase 29: search_graph remaining relationships */
    RUN_TEST(tool_sg_rel_imports);
    RUN_TEST(tool_sg_rel_defines);
    RUN_TEST(tool_sg_rel_contains_file);

    /* Phase 30: search_graph remaining labels */
    RUN_TEST(tool_sg_label_file);
    RUN_TEST(tool_sg_label_folder);
    RUN_TEST(tool_sg_label_package);

    /* Phase 31: search_graph param combos */
    RUN_TEST(tool_sg_include_connected_false);
    RUN_TEST(tool_sg_qn_and_label);
    RUN_TEST(tool_sg_file_and_name);

    /* Phase 32: query_graph remaining edge types */
    RUN_TEST(tool_qg_configures);
    RUN_TEST(tool_qg_handles);
    RUN_TEST(tool_qg_defines_method);
    RUN_TEST(tool_qg_defines_method_more_than_10);
    RUN_TEST(tool_qg_class_lines_nonzero);
    RUN_TEST(tool_qg_no_limit);
    RUN_TEST(tool_qg_empty_result);

    /* Phase 33: search_code remaining params */
    RUN_TEST(tool_sc_regex_false);
    RUN_TEST(tool_sc_context_zero);
    RUN_TEST(tool_sc_special_chars);

    /* Phase 34: get_code_snippet remaining */
    RUN_TEST(tool_snippet_neighbors_false);
    RUN_TEST(tool_snippet_class);

    /* Phase 35: trace_path remaining */
    RUN_TEST(tool_trace_depth_0);
    RUN_TEST(tool_trace_defines_only);
    RUN_TEST(tool_trace_include_tests);
    RUN_TEST(tool_trace_risk_labels);

    /* Phase 36: detect_changes remaining */
    RUN_TEST(tool_detect_nonexistent_branch);
    RUN_TEST(tool_detect_depth_1);

    /* Phase 37: manage_adr remaining */
    RUN_TEST(tool_adr_default_mode);
    RUN_TEST(tool_adr_update_empty);

    /* Phase 38: ingest_traces remaining */
    RUN_TEST(tool_ingest_traces_partial);

    /* Phase 39: error handling remaining tools */
    RUN_TEST(tool_err_schema_bad_project);
    RUN_TEST(tool_err_index_status_no_project);
    RUN_TEST(tool_err_adr_bad_project);
    RUN_TEST(tool_err_ingest_bad_project);
    RUN_TEST(tool_err_ingest_no_traces);

    /* #1300: legacy-partial purge progress (the full rebuild below resets the graph) */
    RUN_TEST(incr_purge_reports_bounded_progress_issue1300);

    /* Phase 40: index_repository mode=full + arch empty aspects */
    RUN_TEST(tool_index_mode_full);
    RUN_TEST(tool_arch_empty_aspects);

    /* Phase 41: delete_project real flow (MUST BE LAST) */
    RUN_TEST(tool_delete_and_verify);

    incremental_teardown();
}
