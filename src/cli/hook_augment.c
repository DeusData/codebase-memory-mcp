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
#include <signal.h>
#include <sys/time.h>
#include <unistd.h>
#endif

#define HA_STDIN_CAP (256 * 1024) /* hook payloads are tiny; cap defensively */
#define HA_MIN_TOKEN 4            /* skip short/noisy patterns before any work */
#define HA_MAX_TOKEN 96
#define HA_RESULT_LIMIT 5
#define HA_MAX_WALKUP 8    /* cwd may be a subdir of the indexed root  */
#define HA_DEADLINE_MS 300 /* hard in-process budget (see also: the    */
                           /* settings.json "timeout" backstop)        */

/* ── Hard deadline ────────────────────────────────────────────────
 * A slow SQLite open or query must never stall the agent. When the timer
 * fires we _exit(0) immediately. Output is written exactly once at the very
 * end, so firing mid-work simply yields a clean no-op (no partial JSON). */
#ifndef _WIN32
static void ha_deadline_exit(int sig) {
    (void)sig;
    _exit(0);
}

static void ha_arm_deadline(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = ha_deadline_exit;
    sigaction(SIGALRM, &sa, NULL);

    struct itimerval it;
    memset(&it, 0, sizeof(it));
    it.it_value.tv_sec = HA_DEADLINE_MS / 1000;
    it.it_value.tv_usec = (HA_DEADLINE_MS % 1000) * 1000;
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

/* Walk up from `start`, deriving a project name at each level and querying
 * search_graph until an indexed project is found (or the walk is exhausted).
 * Stops at the first non-error result: a valid project with zero hits is a
 * legitimate "no match" and must NOT cause a parent-directory probe. */
static char *ha_resolve_and_query(cbm_mcp_server_t *srv, const char *start, const char *token) {
    char dir[4096];
    snprintf(dir, sizeof(dir), "%s", start);

    for (int level = 0; level < HA_MAX_WALKUP && dir[0] == '/'; level++) {
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
            break;
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
    if (!tool || (strcmp(tool, "Grep") != 0 && strcmp(tool, "Glob") != 0)) {
        yyjson_doc_free(doc);
        free(input);
        return 0;
    }

    yyjson_val *tin = yyjson_obj_get(root, "tool_input");
    const char *pattern = ha_obj_str(tin, "pattern");
    char token[HA_MAX_TOKEN + 1];
    if (!ha_extract_token(pattern, token, sizeof(token))) {
        yyjson_doc_free(doc);
        free(input);
        return 0;
    }

    const char *cwd = ha_obj_str(root, "cwd");
#ifndef _WIN32
    char cwdbuf[4096];
    if (!cwd || cwd[0] != '/') {
        if (!getcwd(cwdbuf, sizeof(cwdbuf))) {
            yyjson_doc_free(doc);
            free(input);
            return 0;
        }
        cwd = cwdbuf;
    }
#else
    /* Windows: Claude Code passes cwd in the hook payload. The walk-up loop
     * below requires POSIX-style absolute paths ('/'-prefixed), so without a
     * usable cwd there is nothing to augment — fail open cleanly. */
    if (!cwd || cwd[0] != '/') {
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
