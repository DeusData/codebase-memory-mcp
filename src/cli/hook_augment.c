/*
 * hook_augment.c — `codebase-memory-mcp hook-augment`
 *
 * A non-blocking Claude Code PreToolUse augmenter. Reads the hook JSON from
 * stdin, and for Grep/Glob calls injects matching graph symbols as
 * `additionalContext` so the agent gets structured context alongside its
 * normal search results.
 *
 * Cardinal rule: this NEVER blocks a tool call. Every error, timeout, missing
 * project, or short/odd pattern path results in `exit 0` with NO stdout
 * output (a clean pass-through). This is what makes issue #362 structurally
 * impossible to recur — the hook cannot deny a tool.
 *
 * The underlying query is `search_graph` (pure SQLite, shell-free) — chosen
 * over `search_code` (which shells out to grep|xargs) so the hook stays cheap
 * enough to run before every Grep/Glob.
 */

#include "cli/cli.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/mem.h"
#include "mcp/mcp.h"
#include "pipeline/pipeline.h"
#include "yyjson/yyjson.h"

#include <ctype.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <fcntl.h>
#include <signal.h>
#include <sys/time.h>
#include <unistd.h>
#endif

#define HA_STDIN_CAP (256 * 1024) /* hook payloads are tiny; cap defensively */
#define HA_MIN_TOKEN 4            /* skip short/noisy patterns before any work */
#define HA_MAX_TOKEN 96
#define HA_RESULT_LIMIT 5
#define HA_MAX_WALKUP 8             /* cwd may be a subdir of the indexed root  */
#define HA_DEADLINE_DEFAULT_MS 2000 /* in-process budget; see ha_deadline_ms()  */
#define HA_DEADLINE_MIN_MS 50
#define HA_DEADLINE_MAX_MS 10000

/* #858: the original 300ms budget silently self-terminated on real cold
 * starts (SQLite/mmap open under load), so augmentation never appeared in
 * real sessions (0/24 observed) while manual warm invocations worked. The
 * budget is now generous by default and env-configurable; the settings.json
 * hook "timeout" remains the outer backstop. */
static int ha_deadline_ms(void) {
    const char *env = getenv("CBM_HOOK_DEADLINE_MS");
    if (!env || !env[0]) {
        return HA_DEADLINE_DEFAULT_MS;
    }
    int v = atoi(env);
    if (v < HA_DEADLINE_MIN_MS) {
        return HA_DEADLINE_MIN_MS;
    }
    if (v > HA_DEADLINE_MAX_MS) {
        return HA_DEADLINE_MAX_MS;
    }
    return v;
}

/* ── Hard deadline ────────────────────────────────────────────────
 * A slow SQLite open or query must never stall the agent. When the timer
 * fires we _exit(0) immediately. Output is written exactly once at the very
 * end, so firing mid-work simply yields a clean no-op (no partial JSON).
 *
 * Observability (#858): a fired deadline is otherwise indistinguishable from
 * "no matches", so the handler first write()s a pre-formatted breadcrumb to
 * ~/.cache/codebase-memory-mcp/logs/hook-augment-timeouts.log (fd and message
 * prepared at arm time — only async-signal-safe write/_exit in the handler). */
#ifndef _WIN32
static int g_ha_crumb_fd = -1;
static char g_ha_crumb_msg[160];
static size_t g_ha_crumb_len = 0;

static void ha_deadline_exit(int sig) {
    (void)sig;
    if (g_ha_crumb_fd >= 0 && g_ha_crumb_len > 0) {
        ssize_t w = write(g_ha_crumb_fd, g_ha_crumb_msg, g_ha_crumb_len);
        (void)w;
    }
    _exit(0);
}

static void ha_open_crumb_log(int deadline_ms) {
    const char *override = getenv("CBM_HOOK_TIMEOUT_LOG"); /* tests + power users */
    char path[CBM_SZ_1K];
    if (override && override[0]) {
        snprintf(path, sizeof(path), "%s", override);
    } else {
        const char *home = getenv("HOME");
        if (!home || !home[0]) {
            return;
        }
        char dir[CBM_SZ_1K];
        snprintf(dir, sizeof(dir), "%s/.cache/codebase-memory-mcp/logs", home);
        cbm_mkdir_p(dir, 0755);
        snprintf(path, sizeof(path), "%s/hook-augment-timeouts.log", dir);
    }
    g_ha_crumb_fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_ha_crumb_fd < 0) {
        return;
    }
    int n = snprintf(g_ha_crumb_msg, sizeof(g_ha_crumb_msg),
                     "hook-augment: deadline_exceeded ms=%d pid=%ld (raise via "
                     "CBM_HOOK_DEADLINE_MS)\n",
                     deadline_ms, (long)getpid());
    g_ha_crumb_len = (n > 0 && n < (int)sizeof(g_ha_crumb_msg)) ? (size_t)n : 0;
}

static void ha_arm_deadline(void) {
    int ms = ha_deadline_ms();
    ha_open_crumb_log(ms);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = ha_deadline_exit;
    sigaction(SIGALRM, &sa, NULL);

    struct itimerval it;
    memset(&it, 0, sizeof(it));
    it.it_value.tv_sec = ms / 1000;
    it.it_value.tv_usec = (ms % 1000) * 1000;
    setitimer(ITIMER_REAL, &it, NULL);
}
#else
static void ha_arm_deadline(void) { /* Windows: rely on settings.json timeout */ }
#endif

/* ── stdin ────────────────────────────────────────────────────────── */

static char *ha_read_stdin(void) {
    char *buf = malloc(HA_STDIN_CAP + 1);
    if (!buf) {
        return NULL;
    }
    size_t total = 0;
    size_t n;
    while (total < HA_STDIN_CAP && (n = fread(buf + total, 1, HA_STDIN_CAP - total, stdin)) > 0) {
        total += n;
    }
    buf[total] = '\0';
    return buf;
}

/* ── pattern → token ──────────────────────────────────────────────
 * Extract the longest identifier-like run ([A-Za-z_][A-Za-z0-9_]*) of at
 * least HA_MIN_TOKEN chars. Pure-identifier output means it is always safe
 * to embed in a regex (name_pattern) with no escaping. Returns false when
 * the pattern has no usable token (path globs, short/regex-only patterns) —
 * the caller then no-ops, which keeps the common cheap case cheap. */
static bool ha_extract_token(const char *pattern, char *out, size_t out_sz) {
    if (!pattern) {
        return false;
    }
    size_t best_start = 0;
    size_t best_len = 0;
    size_t i = 0;
    while (pattern[i]) {
        if (isalpha((unsigned char)pattern[i]) || pattern[i] == '_') {
            size_t start = i;
            while (pattern[i] && (isalnum((unsigned char)pattern[i]) || pattern[i] == '_')) {
                i++;
            }
            size_t len = i - start;
            if (len > best_len) {
                best_len = len;
                best_start = start;
            }
        } else {
            i++;
        }
    }
    if (best_len < HA_MIN_TOKEN) {
        return false;
    }
    if (best_len > HA_MAX_TOKEN) {
        best_len = HA_MAX_TOKEN;
    }
    if (best_len + 1 > out_sz) {
        best_len = out_sz - 1;
    }
    memcpy(out, pattern + best_start, best_len);
    out[best_len] = '\0';
    return true;
}

/* ── JSON helpers ─────────────────────────────────────────────────── */

static const char *ha_obj_str(yyjson_val *obj, const char *key) {
    yyjson_val *v = obj ? yyjson_obj_get(obj, key) : NULL;
    return (v && yyjson_is_str(v)) ? yyjson_get_str(v) : NULL;
}

/* Build the search_graph args JSON: {"project":..,"name_pattern":".*tok.*",
 * "limit":N}. `token` is a pure identifier so regex embedding is safe. */
static char *ha_build_args(const char *project, const char *token) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    char name_pattern[HA_MAX_TOKEN + 8];
    snprintf(name_pattern, sizeof(name_pattern), ".*%s.*", token);

    yyjson_mut_obj_add_str(doc, root, "project", project);
    yyjson_mut_obj_add_str(doc, root, "name_pattern", name_pattern);
    yyjson_mut_obj_add_int(doc, root, "limit", HA_RESULT_LIMIT);
    /* Programmatic consumer: search_graph defaults to TOON text, but
     * ha_format_context parses the inner payload as JSON ("results"). */
    yyjson_mut_obj_add_str(doc, root, "format", "json");

    char *out = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return out; /* caller frees */
}

/* Parse the MCP envelope returned by cbm_mcp_handle_tool and, if it is a
 * successful search_graph result with >=1 hit, format a compact
 * additionalContext string. Returns malloc'd text or NULL.
 *
 * *is_error is set when the envelope is an MCP error (e.g. project not
 * indexed) so the caller can try a parent directory. */
static char *ha_format_context(const char *envelope, const char *token, bool *is_error) {
    *is_error = false;
    yyjson_doc *edoc = yyjson_read(envelope, strlen(envelope), 0);
    if (!edoc) {
        return NULL;
    }
    yyjson_val *eroot = yyjson_doc_get_root(edoc);
    yyjson_val *err = yyjson_obj_get(eroot, "isError");
    if (err && yyjson_is_true(err)) {
        *is_error = true;
        yyjson_doc_free(edoc);
        return NULL;
    }
    yyjson_val *content = yyjson_obj_get(eroot, "content");
    yyjson_val *item0 = (content && yyjson_is_arr(content)) ? yyjson_arr_get(content, 0) : NULL;
    const char *inner = ha_obj_str(item0, "text");
    if (!inner) {
        yyjson_doc_free(edoc);
        return NULL;
    }

    yyjson_doc *idoc = yyjson_read(inner, strlen(inner), 0);
    if (!idoc) {
        yyjson_doc_free(edoc);
        return NULL;
    }
    yyjson_val *iroot = yyjson_doc_get_root(idoc);
    yyjson_val *results = yyjson_obj_get(iroot, "results");
    size_t nres = (results && yyjson_is_arr(results)) ? yyjson_arr_size(results) : 0;
    if (nres == 0) {
        yyjson_doc_free(idoc);
        yyjson_doc_free(edoc);
        return NULL; /* valid project, just no matching symbols */
    }

    char *text = malloc(4096);
    if (!text) {
        yyjson_doc_free(idoc);
        yyjson_doc_free(edoc);
        return NULL;
    }
    int off = snprintf(text, 4096,
                       "[codebase-memory] %zu graph symbol(s) match \"%s\" "
                       "(structured context; your search results below are "
                       "unaffected):",
                       nres, token);
    size_t idx;
    size_t maxn;
    yyjson_val *r;
    yyjson_arr_foreach(results, idx, maxn, r) {
        if (off < 0 || off >= 3900) {
            break;
        }
        const char *qn = ha_obj_str(r, "qualified_name");
        const char *nm = ha_obj_str(r, "name");
        const char *fp = ha_obj_str(r, "file_path");
        const char *lb = ha_obj_str(r, "label");
        const char *disp = (qn && qn[0]) ? qn : (nm ? nm : "");
        off += snprintf(text + off, (size_t)(4096 - off), "\n- %s  %s%s%s", disp, fp ? fp : "",
                        (lb && lb[0]) ? "  " : "", (lb && lb[0]) ? lb : "");
    }

    yyjson_doc_free(idoc);
    yyjson_doc_free(edoc);
    return text;
}

/* ── Read coverage note (#963) ────────────────────────────────────
 * For Read calls: if the file being read is listed in the project's
 * index_coverage table (parse_partial or a skip), inject a note so the agent
 * knows the knowledge graph may under-report this file. Best-effort and
 * non-blocking like everything else here — no entry, no output. */

/* Parse an index_status envelope (which carries the coverage report) and
 * return a note when `rel` is listed.
 * *is_error is set for MCP errors (project not indexed) → caller climbs. */
static char *ha_coverage_context(const char *envelope, const char *rel, bool *is_error) {
    *is_error = false;
    yyjson_doc *edoc = yyjson_read(envelope, strlen(envelope), 0);
    if (!edoc) {
        return NULL;
    }
    yyjson_val *eroot = yyjson_doc_get_root(edoc);
    yyjson_val *err = yyjson_obj_get(eroot, "isError");
    if (err && yyjson_is_true(err)) {
        *is_error = true;
        yyjson_doc_free(edoc);
        return NULL;
    }
    yyjson_val *content = yyjson_obj_get(eroot, "content");
    yyjson_val *item0 = (content && yyjson_is_arr(content)) ? yyjson_arr_get(content, 0) : NULL;
    const char *inner = ha_obj_str(item0, "text");
    if (!inner) {
        yyjson_doc_free(edoc);
        return NULL;
    }
    yyjson_doc *idoc = yyjson_read(inner, strlen(inner), 0);
    if (!idoc) {
        yyjson_doc_free(edoc);
        return NULL;
    }
    yyjson_val *iroot = yyjson_doc_get_root(idoc);
    char *text = NULL;

    yyjson_val *pp = yyjson_obj_get(iroot, "parse_partial");
    yyjson_val *files = pp ? yyjson_obj_get(pp, "files") : NULL;
    size_t idx;
    size_t maxn;
    yyjson_val *fe;
    if (files && yyjson_is_arr(files)) {
        yyjson_arr_foreach(files, idx, maxn, fe) {
            const char *fp = ha_obj_str(fe, "path");
            if (fp && strcmp(fp, rel) == 0) {
                const char *ranges = ha_obj_str(fe, "error_ranges");
                text = malloc(1024);
                if (text) {
                    snprintf(text, 1024,
                             "[codebase-memory] Coverage note: this file was only PARTIALLY "
                             "indexed — line range(s) %s could not be parsed, so constructs "
                             "there may be missing from the knowledge graph. The file content "
                             "you are reading is ground truth; graph queries may under-report "
                             "this file. (best-effort signal)",
                             ranges && ranges[0] ? ranges : "?");
                }
                break;
            }
        }
    }
    if (!text) {
        yyjson_val *sk = yyjson_obj_get(iroot, "skipped");
        files = sk ? yyjson_obj_get(sk, "files") : NULL;
        if (files && yyjson_is_arr(files)) {
            yyjson_arr_foreach(files, idx, maxn, fe) {
                const char *fp = ha_obj_str(fe, "path");
                if (fp && strcmp(fp, rel) == 0) {
                    const char *phase = ha_obj_str(fe, "phase");
                    const char *reason = ha_obj_str(fe, "reason");
                    text = malloc(1024);
                    if (text) {
                        snprintf(text, 1024,
                                 "[codebase-memory] Coverage note: this file was NOT indexed "
                                 "(%s%s%s) — the knowledge graph has no data for it. "
                                 "(best-effort signal)",
                                 phase ? phase : "skipped", reason && reason[0] ? ": " : "",
                                 reason ? reason : "");
                    }
                    break;
                }
            }
        }
    }
    yyjson_doc_free(idoc);
    yyjson_doc_free(edoc);
    return text;
}

/* Strip the last path component in place. Returns false at a filesystem or
 * drive root (nothing left to strip). */
static bool ha_strip_last_component(char *dir) {
    char *slash = strrchr(dir, '/');
    if (!slash || slash == dir) {
        return false; /* POSIX root "/" */
    }
    if (slash == dir + 2 && dir[1] == ':') {
        return false; /* Windows drive root "X:/" — don't strip to "X:" */
    }
    *slash = '\0';
    return true;
}

/* Walk up from the file's parent directory to find the indexed project, then
 * check whether the file (repo-relative) is listed in its coverage report.
 * Mirrors ha_resolve_and_query: an MCP error means "not indexed here" →
 * climb; a valid project with no entry for this file → stop, no output. */
static char *ha_resolve_coverage(cbm_mcp_server_t *srv, const char *file_path) {
    char dir[4096];
    snprintf(dir, sizeof(dir), "%s", file_path);
    if (!ha_strip_last_component(dir)) {
        return NULL; /* file directly at a root — nothing to resolve against */
    }

    for (int level = 0; level < HA_MAX_WALKUP && cbm_hook_path_is_abs(dir); level++) {
        char *project = cbm_project_name_from_path(dir);
        if (project) {
            yyjson_mut_doc *adoc = yyjson_mut_doc_new(NULL);
            yyjson_mut_val *aroot = yyjson_mut_obj(adoc);
            yyjson_mut_doc_set_root(adoc, aroot);
            yyjson_mut_obj_add_str(adoc, aroot, "project", project);
            char *args = yyjson_mut_write(adoc, 0, NULL);
            yyjson_mut_doc_free(adoc);
            free(project);
            if (args) {
                char *res = cbm_mcp_handle_tool(srv, "index_status", args);
                free(args);
                if (res) {
                    bool is_error = false;
                    const char *rel = file_path + strlen(dir) + 1;
                    char *ctx = ha_coverage_context(res, rel, &is_error);
                    free(res);
                    if (ctx) {
                        return ctx; /* listed → note */
                    }
                    if (!is_error) {
                        return NULL; /* indexed project, file not listed → stop */
                    }
                }
            }
        }
        if (!ha_strip_last_component(dir)) {
            break;
        }
    }
    return NULL;
}

/* Emit the PreToolUse additionalContext payload to stdout (exactly once). */
static void ha_emit(const char *text) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *hso = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, hso, "hookEventName", "PreToolUse");
    yyjson_mut_obj_add_str(doc, hso, "additionalContext", text);
    yyjson_mut_obj_add_val(doc, root, "hookSpecificOutput", hso);

    char *json = yyjson_mut_write(doc, 0, NULL);
    if (json) {
        fputs(json, stdout);
        free(json);
    }
    yyjson_mut_doc_free(doc);
}

/* True for an absolute path we can walk up: POSIX "/..." or a Windows drive
 * root — "X:/..." or a bare "X:" (callers normalize '\\' to '/' first).
 * Declared in cli.h so the Windows drive-letter handling (#618) has direct
 * regression coverage. */
bool cbm_hook_path_is_abs(const char *d) {
    if (!d || !d[0]) {
        return false;
    }
    if (d[0] == '/') {
        return true;
    }
    return isalpha((unsigned char)d[0]) && d[1] == ':' && (d[2] == '/' || d[2] == '\0');
}

/* Walk up from `start`, deriving a project name at each level and querying
 * search_graph until an indexed project is found (or the walk is exhausted).
 * Stops at the first non-error result: a valid project with zero hits is a
 * legitimate "no match" and must NOT cause a parent-directory probe. */
static char *ha_resolve_and_query(cbm_mcp_server_t *srv, const char *start, const char *token) {
    char dir[4096];
    snprintf(dir, sizeof(dir), "%s", start);

    for (int level = 0; level < HA_MAX_WALKUP && cbm_hook_path_is_abs(dir); level++) {
        char *project = cbm_project_name_from_path(dir);
        if (project) {
            char *args = ha_build_args(project, token);
            free(project);
            if (args) {
                char *res = cbm_mcp_handle_tool(srv, "search_graph", args);
                free(args);
                if (res) {
                    bool is_error = false;
                    char *ctx = ha_format_context(res, token, &is_error);
                    free(res);
                    if (ctx) {
                        return ctx; /* hits → done */
                    }
                    if (!is_error) {
                        return NULL; /* valid project, no hits → stop */
                    }
                }
            }
        }
        /* Not indexed at this level — climb to the parent. */
        char *slash = strrchr(dir, '/');
        if (!slash || slash == dir) {
            break; /* POSIX root "/" */
        }
        if (slash == dir + 2 && dir[1] == ':') {
            break; /* Windows drive root "X:/" — don't strip to "X:" */
        }
        *slash = '\0';
    }
    return NULL;
}

int cbm_cmd_hook_augment(void) {
    ha_arm_deadline();

    char *input = ha_read_stdin();
    if (!input) {
        return 0;
    }
    yyjson_doc *doc = yyjson_read(input, strlen(input), 0);
    if (!doc) {
        free(input);
        return 0;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);

    const char *tool = ha_obj_str(root, "tool_name");
    if (!tool ||
        (strcmp(tool, "Grep") != 0 && strcmp(tool, "Glob") != 0 && strcmp(tool, "Read") != 0)) {
        yyjson_doc_free(doc);
        free(input);
        return 0;
    }

    yyjson_val *tin = yyjson_obj_get(root, "tool_input");

    /* Read → coverage note (#963): warn when the file being read is listed as
     * not fully indexed. Independent of the Grep/Glob symbol augment below. */
    if (strcmp(tool, "Read") == 0) {
        const char *fp = ha_obj_str(tin, "file_path");
        char fpbuf[4096];
        if (fp) {
            snprintf(fpbuf, sizeof(fpbuf), "%s", fp);
            for (char *p = fpbuf; *p; p++) {
                if (*p == '\\') {
                    *p = '/';
                }
            }
        }
        if (fp && cbm_hook_path_is_abs(fpbuf)) {
            cbm_mcp_server_t *rsrv = cbm_mcp_server_new(NULL);
            if (rsrv) {
                char *note = ha_resolve_coverage(rsrv, fpbuf);
                if (note) {
                    ha_emit(note);
                    free(note);
                }
                cbm_mcp_server_free(rsrv);
            }
        }
        yyjson_doc_free(doc);
        free(input);
        return 0;
    }

    const char *pattern = ha_obj_str(tin, "pattern");
    char token[HA_MAX_TOKEN + 1];
    if (!ha_extract_token(pattern, token, sizeof(token))) {
        yyjson_doc_free(doc);
        free(input);
        return 0;
    }

    const char *cwd = ha_obj_str(root, "cwd");
    char cwdbuf[4096];
#ifndef _WIN32
    if (!cwd || !cbm_hook_path_is_abs(cwd)) {
        if (!getcwd(cwdbuf, sizeof(cwdbuf))) {
            yyjson_doc_free(doc);
            free(input);
            return 0;
        }
        cwd = cwdbuf;
    }
#else
    /* Windows: Claude Code passes an absolute drive-letter cwd in the hook
     * payload (e.g. C:\repo). Normalize '\\' -> '/' and require an absolute
     * path; the walk-up loop handles POSIX and "X:/..." roots alike. Without
     * a usable cwd there is nothing to augment — fail open cleanly. */
    if (cwd) {
        snprintf(cwdbuf, sizeof(cwdbuf), "%s", cwd);
        for (char *p = cwdbuf; *p; p++) {
            if (*p == '\\') {
                *p = '/';
            }
        }
        cwd = cwdbuf;
    }
    if (!cwd || !cbm_hook_path_is_abs(cwd)) {
        yyjson_doc_free(doc);
        free(input);
        return 0;
    }
#endif

    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        yyjson_doc_free(doc);
        free(input);
        return 0;
    }

    char *ctx = ha_resolve_and_query(srv, cwd, token);
    if (ctx) {
        ha_emit(ctx);
        free(ctx);
    }

    cbm_mcp_server_free(srv);
    yyjson_doc_free(doc);
    free(input);
    return 0;
}
