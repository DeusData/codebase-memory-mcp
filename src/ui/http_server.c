/*
 * http_server.c — Routing + endpoint handlers for the graph UI.
 *
 * Transport (sockets, parsing, limits) lives in httpd.c; this file owns
 * the routes and their handlers:
 *   GET /             → embedded index.html
 *   GET /assets/...   → embedded JS/CSS
 *   POST /rpc         → JSON-RPC dispatch via own cbm_mcp_server_t
 *   OPTIONS /rpc      → CORS preflight (for vite dev on :5173)
 *   GET/POST /api/... → UI support endpoints (layout, index, browse, …)
 *   *                 → 404
 *
 * Runs in a background pthread. Binds to 127.0.0.1 only (see httpd.c).
 * Has its own cbm_mcp_server_t with a separate SQLite connection (WAL reader).
 */
#include "ui/http_server.h"
#include "ui/httpd.h"
#include "ui/embedded_assets.h"
#include "ui/layout3d.h"
#include "mcp/mcp.h"
#include "store/store.h"
#include "watcher/watcher.h"
#include "cli/cli.h"
/* pipeline.h no longer needed — indexing runs as subprocess */
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/str_util.h"
#include "foundation/compat_thread.h"

#include <sqlite3/sqlite3.h>
#include <yyjson/yyjson.h>

#include <errno.h>
#include <math.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <windows.h>
#include <process.h>
#include <psapi.h> /* GetProcessMemoryInfo */
#else
#include <sys/stat.h>
#include <unistd.h>
#include <sys/wait.h>
#endif
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

/* ── Constants ────────────────────────────────────────────────── */

/* Max JSON-RPC request body size (1 MB) — transport enforces the same cap. */
#define MAX_BODY_SIZE CBM_HTTP_MAX_BODY

/* ── CORS: only allow localhost origins (blocks remote website attacks) ────── */

/* Per-request CORS header buffers. Updated at the start of each dispatch.
 * The server handles requests sequentially on one thread (see httpd.h),
 * which makes these statics safe. */
static char g_cors[256];      /* CORS headers only */
static char g_cors_json[512]; /* CORS + Content-Type: application/json */

/* Inspect the Origin header and only reflect it if it's a localhost URL.
 * This prevents remote websites from making cross-origin requests to the
 * local graph-ui server (the key defense against CORS-based data exfil). */
static void update_cors(const cbm_http_req_t *req) {
    if (req->origin[0] != '\0' && (cbm_http_path_match(req->origin, "http://localhost:*") ||
                                   cbm_http_path_match(req->origin, "http://127.0.0.1:*"))) {
        snprintf(g_cors, sizeof(g_cors),
                 "Access-Control-Allow-Origin: %s\r\n"
                 "Access-Control-Allow-Methods: POST, GET, DELETE, OPTIONS\r\n"
                 "Access-Control-Allow-Headers: Content-Type\r\n",
                 req->origin);
    } else {
        /* No Access-Control-Allow-Origin → browser blocks cross-origin access */
        snprintf(g_cors, sizeof(g_cors),
                 "Access-Control-Allow-Methods: POST, GET, DELETE, OPTIONS\r\n"
                 "Access-Control-Allow-Headers: Content-Type\r\n");
    }
    snprintf(g_cors_json, sizeof(g_cors_json), "%sContent-Type: application/json\r\n", g_cors);
}

static const char *detect_ui_lang(const char *accept_language) {
    if (accept_language && (strstr(accept_language, "zh-CN") || strstr(accept_language, "zh"))) {
        return "zh";
    }
    return "en";
}

static void handle_ui_config(cbm_http_conn_t *c, const cbm_http_req_t *req) {
    const char *lang = NULL;
    char cache_dir[1024];
    snprintf(cache_dir, sizeof(cache_dir), "%s", cbm_resolve_cache_dir());
    cbm_config_t *cfg = cbm_config_open(cache_dir);
    if (cfg) {
        const char *pinned = cbm_config_get(cfg, CBM_CONFIG_UI_LANG, "auto");
        if (strcmp(pinned, "zh") == 0 || strcmp(pinned, "en") == 0) {
            lang = pinned;
        }
    }

    char lang_buf[8];
    snprintf(lang_buf, sizeof(lang_buf), "%s", lang ? lang : detect_ui_lang(req->accept_language));
    if (cfg) {
        cbm_config_close(cfg);
    }
    cbm_http_replyf(c, 200, g_cors_json, "{\"lang\":\"%s\"}", lang_buf);
}

/* ── Server state ─────────────────────────────────────────────── */

struct cbm_http_server {
    cbm_httpd_t *listener;
    cbm_mcp_server_t *mcp; /* own MCP server instance (read-only) */
    struct cbm_watcher *watcher; /* external watcher ref (not owned) */
    atomic_int stop_flag;
    int port;
    bool listener_ok;
};

/* ── Forward declarations for process-kill PID validation ──────── */

#define MAX_INDEX_JOBS 4

typedef struct {
    char root_path[1024];
    char project_name[256];
    atomic_int status; /* 0=idle, 1=running, 2=done, 3=error */
    char error_msg[256];
#ifndef _WIN32
    pid_t child_pid; /* tracked for process-kill validation */
#endif
} index_job_t;

static index_job_t g_index_jobs[MAX_INDEX_JOBS];

/* ── Serve embedded asset ─────────────────────────────────────── */

static bool serve_embedded(cbm_http_conn_t *c, const char *path) {
    const cbm_embedded_file_t *f = cbm_embedded_lookup(path);
    if (!f)
        return false;

    /* Build headers with correct Content-Type for this asset */
    char hdrs[512];
    snprintf(hdrs, sizeof(hdrs),
             "%sContent-Type: %s\r\n"
             "Cache-Control: public, max-age=31536000, immutable\r\n",
             g_cors, f->content_type);

    cbm_http_reply_buf(c, 200, hdrs, f->data, (size_t)f->size);
    return true;
}

/* Build DB path for a project: <cache_dir>/<project>.db */
static void db_path_for_project(const char *project, char *buf, size_t bufsz) {
    if (!cbm_validate_project_name(project)) {
        buf[0] = '\0';
        return;
    }
    const char *dir = cbm_resolve_cache_dir();
    if (!dir) {
        dir = cbm_tmpdir();
    }
    snprintf(buf, bufsz, "%s/%s.db", dir, project);
}

/* ── Log ring buffer ──────────────────────────────────────────── */

#define LOG_RING_SIZE 500
#define LOG_LINE_MAX 512

static char g_log_ring[LOG_RING_SIZE][LOG_LINE_MAX];
static int g_log_head = 0;
static int g_log_count = 0;
static cbm_mutex_t g_log_mutex;

enum { CBM_LOG_MUTEX_UNINIT = 0, CBM_LOG_MUTEX_INITING = 1, CBM_LOG_MUTEX_INITED = 2 };
static atomic_int g_log_mutex_init = CBM_LOG_MUTEX_UNINIT;

/* Safe for concurrent callers: only publishes INITED after cbm_mutex_init()
 * has completed. Callers that lose the CAS race spin until init finishes. */
void cbm_ui_log_init(void) {
    int state = atomic_load(&g_log_mutex_init);
    if (state == CBM_LOG_MUTEX_INITED)
        return;

    state = CBM_LOG_MUTEX_UNINIT;
    if (atomic_compare_exchange_strong(&g_log_mutex_init, &state, CBM_LOG_MUTEX_INITING)) {
        cbm_mutex_init(&g_log_mutex);
        atomic_store(&g_log_mutex_init, CBM_LOG_MUTEX_INITED);
        return;
    }

    /* Another thread is initializing — spin until done */
    while (atomic_load(&g_log_mutex_init) != CBM_LOG_MUTEX_INITED) {
        cbm_usleep(1000); /* 1ms */
    }
}

/* Called from a log hook — appends a line to the ring buffer (thread-safe) */
void cbm_ui_log_append(const char *line) {
    if (!line)
        return;
    /* Ensure mutex is initialized (safe for early single-threaded logging
     * and concurrent calls via atomic_exchange once-init pattern). */
    cbm_ui_log_init();
    cbm_mutex_lock(&g_log_mutex);
    snprintf(g_log_ring[g_log_head], LOG_LINE_MAX, "%s", line);
    g_log_head = (g_log_head + 1) % LOG_RING_SIZE;
    if (g_log_count < LOG_RING_SIZE)
        g_log_count++;
    cbm_mutex_unlock(&g_log_mutex);
}

/* GET /api/logs?lines=N — returns last N log lines */
static void handle_logs(cbm_http_conn_t *c, const cbm_http_req_t *req) {
    char lines_str[16] = {0};
    int max_lines = 100;
    if (cbm_http_query_param(req->query, "lines", lines_str, (int)sizeof(lines_str))) {
        int v = atoi(lines_str);
        if (v > 0 && v <= LOG_RING_SIZE)
            max_lines = v;
    }

    cbm_mutex_lock(&g_log_mutex);
    int count = g_log_count < max_lines ? g_log_count : max_lines;
    int start = (g_log_head - count + LOG_RING_SIZE) % LOG_RING_SIZE;
    int total = g_log_count;

    /* Copy lines under lock */
    size_t buf_size = (size_t)count * (LOG_LINE_MAX + 10) + 64;
    char *buf = malloc(buf_size);
    if (!buf) {
        cbm_mutex_unlock(&g_log_mutex);
        cbm_http_replyf(c, 500, g_cors, "oom");
        return;
    }

    int pos = 0;
    pos += snprintf(buf + pos, buf_size - (size_t)pos, "{\"lines\":[");
    for (int i = 0; i < count; i++) {
        int idx = (start + i) % LOG_RING_SIZE;
        if (i > 0)
            buf[pos++] = ',';
        /* Escape quotes in log lines */
        buf[pos++] = '"';
        for (int j = 0; g_log_ring[idx][j] && (size_t)pos < buf_size - 10; j++) {
            char ch = g_log_ring[idx][j];
            if (ch == '"') {
                buf[pos++] = '\\';
                buf[pos++] = '"';
            } else if (ch == '\\') {
                buf[pos++] = '\\';
                buf[pos++] = '\\';
            } else if (ch == '\n') {
                buf[pos++] = '\\';
                buf[pos++] = 'n';
            } else {
                buf[pos++] = ch;
            }
        }
        buf[pos++] = '"';
    }
    cbm_mutex_unlock(&g_log_mutex);
    pos += snprintf(buf + pos, buf_size - (size_t)pos, "],\"total\":%d}", total);

    cbm_http_replyf(c, 200, g_cors_json, "%s", buf);
    free(buf);
}

/* ── Process monitoring ───────────────────────────────────────── */

#ifndef _WIN32
#include <sys/resource.h>
#endif
#include <signal.h>

/* GET /api/processes — list codebase-memory-mcp processes via ps */
static void handle_processes(cbm_http_conn_t *c) {
    char buf[8192];
    int pos = 0;

#ifdef _WIN32
    /* Windows: GetProcessMemoryInfo + GetProcessTimes */
    PROCESS_MEMORY_COUNTERS pmc;
    FILETIME ft_create, ft_exit, ft_kernel, ft_user;
    double user_s = 0, sys_s = 0;
    size_t rss_bytes = 0;
    if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc)))
        rss_bytes = pmc.WorkingSetSize;
    if (GetProcessTimes(GetCurrentProcess(), &ft_create, &ft_exit, &ft_kernel, &ft_user)) {
        ULARGE_INTEGER u, k;
        u.LowPart = ft_user.dwLowDateTime;
        u.HighPart = ft_user.dwHighDateTime;
        k.LowPart = ft_kernel.dwLowDateTime;
        k.HighPart = ft_kernel.dwHighDateTime;
        user_s = (double)u.QuadPart / 1e7;
        sys_s = (double)k.QuadPart / 1e7;
    }
    pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos,
                    "{\"self_pid\":%d,\"self_rss_mb\":%.1f,"
                    "\"self_user_cpu_s\":%.1f,\"self_sys_cpu_s\":%.1f,\"processes\":[]}",
                    (int)_getpid(), (double)rss_bytes / (1024.0 * 1024.0), user_s, sys_s);
#else
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    long rss_kb = ru.ru_maxrss;
#ifdef __APPLE__
    rss_kb /= 1024;
#endif
    pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos,
                    "{\"self_pid\":%d,\"self_rss_mb\":%.1f,"
                    "\"self_user_cpu_s\":%.1f,\"self_sys_cpu_s\":%.1f,\"processes\":[",
                    (int)getpid(), (double)rss_kb / 1024.0,
                    (double)ru.ru_utime.tv_sec + (double)ru.ru_utime.tv_usec / 1e6,
                    (double)ru.ru_stime.tv_sec + (double)ru.ru_stime.tv_usec / 1e6);

    FILE *fp = popen("LC_ALL=C ps -eo pid,pcpu,rss,etime,comm 2>/dev/null"
                     " | grep '[c]odebase-memory-mcp'",
                     "r");
    int proc_count = 0;
    if (fp) {
        char line[1024];
        while (fgets(line, sizeof(line), fp)) {
            int pid = 0;
            float cpu = 0;
            long rss = 0;
            char elapsed[64] = {0};
            char comm[256] = {0};

            if (sscanf(line, "%d %f %ld %63s %255s", &pid, &cpu, &rss, elapsed, comm) >= 4) {
                if (proc_count > 0)
                    buf[pos++] = ',';
                pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos,
                                "{\"pid\":%d,\"cpu\":%.1f,\"rss_mb\":%.1f,"
                                "\"elapsed\":\"%s\",\"command\":\"%s\",\"is_self\":%s}",
                                pid, (double)cpu, (double)rss / 1024.0, elapsed, comm,
                                pid == (int)getpid() ? "true" : "false");
                if (pos >= (int)sizeof(buf)) {
                    pos = (int)sizeof(buf) - 1;
                }
                proc_count++;
            }
        }
        pclose(fp);
    }
    pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos, "]}");
#endif

    cbm_http_replyf(c, 200, g_cors_json, "%s", buf);
}

/* POST /api/process-kill — kill a process by PID */
static void handle_process_kill(cbm_http_conn_t *c, const cbm_http_req_t *req) {
    if (req->body_len == 0 || req->body_len > 256) {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"invalid body\"}");
        return;
    }

    yyjson_doc *doc = yyjson_read(req->body, req->body_len, 0);
    if (!doc) {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"invalid json\"}");
        return;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *v_pid = yyjson_obj_get(root, "pid");
    if (!v_pid || !yyjson_is_int(v_pid)) {
        yyjson_doc_free(doc);
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"missing pid\"}");
        return;
    }
    int target_pid = (int)yyjson_get_int(v_pid);
    yyjson_doc_free(doc);

#ifdef _WIN32
    if (target_pid == (int)_getpid()) {
#else
    if (target_pid == (int)getpid()) {
#endif
        cbm_http_replyf(c, 400, g_cors_json,
                        "{\"error\":\"cannot kill self (use the UI server's own shutdown)\"}");
        return;
    }

#ifndef _WIN32
    /* Only allow killing PIDs that were spawned by this server (indexing jobs) */
    {
        bool pid_is_ours = false;
        for (int i = 0; i < MAX_INDEX_JOBS; i++) {
            if (atomic_load(&g_index_jobs[i].status) == 1 &&
                g_index_jobs[i].child_pid == target_pid) {
                pid_is_ours = true;
                break;
            }
        }
        if (!pid_is_ours) {
            cbm_http_replyf(c, 403, g_cors_json,
                            "{\"error\":\"can only kill server-spawned processes\"}");
            return;
        }
    }
#endif

#ifdef _WIN32
    HANDLE hproc = OpenProcess(PROCESS_TERMINATE, FALSE, (DWORD)target_pid);
    if (!hproc || !TerminateProcess(hproc, 1)) {
        if (hproc)
            CloseHandle(hproc);
        cbm_http_replyf(c, 500, g_cors_json, "{\"error\":\"kill failed\"}");
        return;
    }
    CloseHandle(hproc);
#else
    if (kill(target_pid, SIGTERM) != 0) {
        cbm_http_replyf(c, 500, g_cors_json, "{\"error\":\"kill failed\"}");
        return;
    }
#endif

    cbm_http_replyf(c, 200, g_cors_json, "{\"killed\":%d}", target_pid);
}

/* ── Directory browser ────────────────────────────────────────── */

#include <dirent.h>

static void append_roots_json(char *buf, size_t bufsz, int *pos) {
    *pos += snprintf(buf + *pos, bufsz - (size_t)*pos, ",\"roots\":[");
#ifdef _WIN32
    DWORD drives = GetLogicalDrives();
    int count = 0;
    for (int i = 0; i < 26; i++) {
        if (!(drives & (1u << i))) {
            continue;
        }
        if (count++ > 0) {
            buf[(*pos)++] = ',';
        }
        *pos += snprintf(buf + *pos, bufsz - (size_t)*pos, "\"%c:/\"", 'A' + i);
    }
#else
    *pos += snprintf(buf + *pos, bufsz - (size_t)*pos, "\"/\"");
#endif
    *pos += snprintf(buf + *pos, bufsz - (size_t)*pos, "]");
}

/* GET /api/browse?path=/some/dir — list subdirectories for file picker */
static void handle_browse(cbm_http_conn_t *c, const cbm_http_req_t *req) {
    char path[1024] = {0};
    const char *home = cbm_get_home_dir();
    if (!cbm_http_query_param(req->query, "path", path, (int)sizeof(path)) || path[0] == '\0') {
        /* Default to home directory */
        if (home)
            snprintf(path, sizeof(path), "%s", home);
        else
            snprintf(path, sizeof(path), "/");
    }

    /* The browser UI may send Windows backslash separators (e.g.
     * "D:\projects\demo"). Normalize to forward slashes before the cbm_is_dir
     * gate, exactly as the MCP repo_path handler and cbm_project_name_from_path
     * already do — otherwise a real D:/ directory is rejected (#548). */
    cbm_normalize_path_sep(path);

    if (!cbm_is_dir(path)) {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"not a directory\"}");
        return;
    }

    DIR *dir = opendir(path);
    if (!dir) {
        cbm_http_replyf(c, 403, g_cors_json, "{\"error\":\"cannot open directory\"}");
        return;
    }

    /* Build JSON response */
    char buf[32768];
    int pos = 0;
    pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos, "{\"path\":\"%s\",\"dirs\":[", path);

    struct dirent *ent;
    int count = 0;
    while ((ent = readdir(dir)) != NULL) {
        /* Skip hidden dirs and . / .. */
        if (ent->d_name[0] == '.')
            continue;

        /* Check if it's actually a directory */
        char full[2048];
        snprintf(full, sizeof(full), "%s/%s", path, ent->d_name);
        if (!cbm_is_dir(full))
            continue;

        if (count > 0)
            buf[pos++] = ',';
        /* Escape directory name to prevent XSS (e.g., names with quotes/angle brackets) */
        {
            char esc[512];
            cbm_json_escape(esc, (int)sizeof(esc), ent->d_name);
            pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos, "\"%s\"", esc);
        }
        if (pos >= (int)sizeof(buf)) {
            pos = (int)sizeof(buf) - 1;
        }
        count++;

        if (count >= 200)
            break; /* safety limit */
    }
    closedir(dir);

    /* Parent path — escape to prevent injection */
    char parent[1024];
    snprintf(parent, sizeof(parent), "%s", path);
    char *last_slash = strrchr(parent, '/');
    /* A Windows drive root "X:/" is its own parent (like POSIX "/"): truncating
     * at the slash would yield the bare drive spec "X:", which the next browse
     * resolves to the wrong directory and strands the user at the root (#548). */
    size_t parent_len = strlen(parent);
    bool is_drive_root = parent_len == 3 && parent[1] == ':' && parent[2] == '/';
    if (is_drive_root) {
        /* leave "X:/" unchanged */
    } else if (last_slash && last_slash != parent) {
        *last_slash = '\0';
    } else {
        snprintf(parent, sizeof(parent), "/");
    }

    {
        char esc_parent[2048];
        cbm_json_escape(esc_parent, (int)sizeof(esc_parent), parent);
        pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos, "],\"parent\":\"%s\"", esc_parent);
        append_roots_json(buf, sizeof(buf), &pos);
        pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos, "}");
    }
    cbm_http_replyf(c, 200, g_cors_json, "%s", buf);
}

/* ── ADR endpoints ────────────────────────────────────────────── */

/* GET /api/adr?project=X — get ADR content for a project */
static void handle_adr_get(cbm_http_conn_t *c, const cbm_http_req_t *req) {
    char name[256] = {0};
    if (!cbm_http_query_param(req->query, "project", name, (int)sizeof(name)) || name[0] == '\0') {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"missing project\"}");
        return;
    }

    char db_path[1024];
    db_path_for_project(name, db_path, sizeof(db_path));

    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        cbm_http_replyf(c, 200, g_cors_json, "{\"has_adr\":false}");
        return;
    }

    cbm_adr_t adr;
    memset(&adr, 0, sizeof(adr));
    if (cbm_store_adr_get(store, name, &adr) == CBM_STORE_OK && adr.content) {
        /* Escape content for JSON — simple: replace quotes and newlines */
        size_t clen = strlen(adr.content);
        size_t buf_size = clen * 2 + 256;
        char *buf = malloc(buf_size);
        if (buf) {
            int pos = snprintf(buf, buf_size, "{\"has_adr\":true,\"content\":\"");
            for (size_t i = 0; i < clen && (size_t)pos < buf_size - 10; i++) {
                char ch = adr.content[i];
                if (ch == '"') {
                    buf[pos++] = '\\';
                    buf[pos++] = '"';
                } else if (ch == '\\') {
                    buf[pos++] = '\\';
                    buf[pos++] = '\\';
                } else if (ch == '\n') {
                    buf[pos++] = '\\';
                    buf[pos++] = 'n';
                } else if (ch == '\r') { /* skip */
                } else if (ch == '\t') {
                    buf[pos++] = '\\';
                    buf[pos++] = 't';
                } else {
                    buf[pos++] = ch;
                }
            }
            pos += snprintf(buf + pos, buf_size - (size_t)pos, "\",\"updated_at\":\"%s\"}",
                            adr.updated_at ? adr.updated_at : "");
            cbm_http_replyf(c, 200, g_cors_json, "%s", buf);
            free(buf);
        } else {
            cbm_http_replyf(c, 500, g_cors, "oom");
        }
        cbm_store_adr_free(&adr);
    } else {
        cbm_http_replyf(c, 200, g_cors_json, "{\"has_adr\":false}");
    }
    cbm_store_close(store);
}

/* POST /api/adr — save ADR content. Body: {"project":"...","content":"..."} */
static void handle_adr_save(cbm_http_conn_t *c, const cbm_http_req_t *req) {
    if (req->body_len == 0 || req->body_len > 16384) {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"invalid body\"}");
        return;
    }

    yyjson_doc *doc = yyjson_read(req->body, req->body_len, 0);
    if (!doc) {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"invalid json\"}");
        return;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *v_proj = yyjson_obj_get(root, "project");
    yyjson_val *v_content = yyjson_obj_get(root, "content");
    if (!v_proj || !yyjson_is_str(v_proj) || !v_content || !yyjson_is_str(v_content)) {
        yyjson_doc_free(doc);
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"missing project or content\"}");
        return;
    }

    const char *proj = yyjson_get_str(v_proj);
    const char *content = yyjson_get_str(v_content);

    char db_path[1024];
    db_path_for_project(proj, db_path, sizeof(db_path));

    cbm_store_t *store = cbm_store_open_path(db_path);
    yyjson_doc_free(doc);
    if (!store) {
        cbm_http_replyf(c, 500, g_cors_json, "{\"error\":\"cannot open store\"}");
        return;
    }

    int rc = cbm_store_adr_store(store, proj, content);
    cbm_store_close(store);

    if (rc == CBM_STORE_OK) {
        cbm_http_replyf(c, 200, g_cors_json, "{\"saved\":true}");
    } else {
        cbm_http_replyf(c, 500, g_cors_json, "{\"error\":\"save failed\"}");
    }
}

/* ── Background indexing ──────────────────────────────────────── */

static char g_binary_path[1024] = {0};

static bool copy_path(char *out, size_t outsz, const char *path) {
    if (!out || outsz == 0 || !path || !path[0]) {
        return false;
    }
    int n = snprintf(out, outsz, "%s", path);
    return n > 0 && (size_t)n < outsz;
}

#ifndef _WIN32
static bool is_executable_file(const char *path) {
    struct stat st;
    return path && stat(path, &st) == 0 && S_ISREG(st.st_mode) && access(path, X_OK) == 0;
}

static bool resolve_from_path(const char *name, char *out, size_t outsz) {
    const char *path = getenv("PATH");
    if (!name || !name[0] || strchr(name, '/') || !path || !path[0]) {
        return false;
    }

    const char *cur = path;
    while (*cur) {
        const char *colon = strchr(cur, ':');
        size_t dir_len = colon ? (size_t)(colon - cur) : strlen(cur);
        if (dir_len > 0 && dir_len < 900) {
            char candidate[1024];
            int n = snprintf(candidate, sizeof(candidate), "%.*s/%s", (int)dir_len, cur, name);
            if (n > 0 && (size_t)n < sizeof(candidate) && is_executable_file(candidate)) {
                return copy_path(out, outsz, candidate);
            }
        }
        if (!colon) {
            break;
        }
        cur = colon + 1;
    }
    return false;
}

static bool resolve_self_executable(char *out, size_t outsz) {
#if defined(__APPLE__)
    char buf[1024];
    uint32_t sz = sizeof(buf);
    if (_NSGetExecutablePath(buf, &sz) == 0 && buf[0]) {
        return copy_path(out, outsz, buf);
    }
    return false;
#else
    char buf[1024];
    ssize_t len = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (len > 0) {
        buf[len] = '\0';
        return copy_path(out, outsz, buf);
    }
    return false;
#endif
}
#else
static bool resolve_self_executable(char *out, size_t outsz) {
    char buf[1024];
    DWORD n = GetModuleFileNameA(NULL, buf, (DWORD)sizeof(buf));
    if (n > 0 && n < sizeof(buf)) {
        return copy_path(out, outsz, buf);
    }
    return false;
}
#endif

bool cbm_http_server_resolve_binary_path(const char *argv0, char *out, size_t outsz) {
    if (!out || outsz == 0) {
        return false;
    }
    out[0] = '\0';

#ifndef _WIN32
    if (argv0 && strchr(argv0, '/') && is_executable_file(argv0)) {
        return copy_path(out, outsz, argv0);
    }
    if (resolve_from_path(argv0, out, outsz)) {
        return true;
    }
#else
    if (argv0 && argv0[0]) {
        DWORD attrs = GetFileAttributesA(argv0);
        if (attrs != INVALID_FILE_ATTRIBUTES && !(attrs & FILE_ATTRIBUTE_DIRECTORY)) {
            return copy_path(out, outsz, argv0);
        }
    }
#endif

    if (resolve_self_executable(out, outsz)) {
        return true;
    }
    return copy_path(out, outsz, argv0);
}

void cbm_http_server_set_binary_path(const char *path) {
    if (path) {
        if (!cbm_http_server_resolve_binary_path(path, g_binary_path, sizeof(g_binary_path))) {
            g_binary_path[0] = '\0';
        }
    }
}

/* Index via subprocess — isolates crashes from the main process. */
static void *index_thread_fn(void *arg) {
    index_job_t *job = arg;
    cbm_log_info("ui.index.start", "path", job->root_path);

    /* Use stored binary path, or try to find it */
    const char *bin = g_binary_path;
    char self_path[1024] = {0};
    if (!bin[0]) {
        cbm_http_server_resolve_binary_path(NULL, self_path, sizeof(self_path));
        bin = self_path[0] ? self_path : "codebase-memory-mcp";
    }

    char log_file[256];

    /* JSON-escape root_path and optional project name. */
    char escaped_path[2048];
    cbm_json_escape(escaped_path, (int)sizeof(escaped_path), job->root_path);
    char escaped_name[512];
    cbm_json_escape(escaped_name, (int)sizeof(escaped_name), job->project_name);
    char json_arg[4096];
    if (job->project_name[0]) {
        snprintf(json_arg, sizeof(json_arg), "{\"repo_path\":\"%s\",\"name\":\"%s\"}", escaped_path,
                 escaped_name);
    } else {
        snprintf(json_arg, sizeof(json_arg), "{\"repo_path\":\"%s\"}", escaped_path);
    }

#ifdef _WIN32
    snprintf(log_file, sizeof(log_file), "%s\\cbm_index_%d.log",
             getenv("TEMP") ? getenv("TEMP") : ".", (int)_getpid());

    /* Build command line for CreateProcess */
    char cmdline[2048];
    /* --index-worker: this http_server spawn is already the crash-isolation layer,
     * so the child runs indexing in-process rather than spawning its own supervisor
     * (avoids redundant process nesting). */
    snprintf(cmdline, sizeof(cmdline), "\"%s\" cli --index-worker index_repository \"%s\"", bin,
             json_arg);

    cbm_log_info("ui.index.spawn", "bin", bin, "log", log_file);

    HANDLE hlog = CreateFileA(log_file, GENERIC_WRITE, FILE_SHARE_READ, NULL, CREATE_ALWAYS,
                              FILE_ATTRIBUTE_NORMAL, NULL);
    STARTUPINFOA si_proc = {.cb = sizeof(si_proc)};
    if (hlog != INVALID_HANDLE_VALUE) {
        si_proc.dwFlags = STARTF_USESTDHANDLES;
        si_proc.hStdError = hlog;
        si_proc.hStdOutput = hlog;
    }
    PROCESS_INFORMATION pi = {0};
    if (!CreateProcessA(NULL, cmdline, NULL, NULL, TRUE, 0, NULL, NULL, &si_proc, &pi)) {
        snprintf(job->error_msg, sizeof(job->error_msg), "CreateProcess failed");
        atomic_store(&job->status, 3);
        if (hlog != INVALID_HANDLE_VALUE)
            CloseHandle(hlog);
        return NULL;
    }
    if (hlog != INVALID_HANDLE_VALUE)
        CloseHandle(hlog);

    /* Poll log file while child runs */
    long tail_pos = 0;
    for (;;) {
        DWORD wait = WaitForSingleObject(pi.hProcess, 500);
        FILE *lf = fopen(log_file, "r");
        if (lf) {
            fseek(lf, tail_pos, SEEK_SET);
            char line[512];
            while (fgets(line, sizeof(line), lf)) {
                size_t l = strlen(line);
                if (l > 0 && line[l - 1] == '\n')
                    line[l - 1] = '\0';
                if (line[0])
                    cbm_ui_log_append(line);
            }
            tail_pos = ftell(lf);
            fclose(lf);
        }
        if (wait == WAIT_OBJECT_0)
            break;
    }

    DWORD win_exit = 1;
    GetExitCodeProcess(pi.hProcess, &win_exit);
    int exit_code = (int)win_exit;
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
    (void)DeleteFileA(log_file);
#else
    snprintf(log_file, sizeof(log_file), "/tmp/cbm_index_%d.log", (int)getpid());

    cbm_log_info("ui.index.fork", "bin", bin, "log", log_file);

    pid_t child_pid = fork();
    if (child_pid < 0) {
        snprintf(job->error_msg, sizeof(job->error_msg), "fork failed");
        atomic_store(&job->status, 3);
        return NULL;
    }
    job->child_pid = child_pid;

    if (child_pid == 0) {
        FILE *lf = freopen(log_file, "w", stderr);
        (void)lf;
        freopen("/dev/null", "w", stdout);
        execl(bin, bin, "cli", "--index-worker", "index_repository", json_arg, (char *)NULL);
        _exit(127);
    }

    long tail_pos = 0;
    for (;;) {
        int wstatus = 0;
        pid_t wr = waitpid(child_pid, &wstatus, WNOHANG);
        bool child_done = (wr == child_pid);

        FILE *lf = fopen(log_file, "r");
        if (lf) {
            fseek(lf, tail_pos, SEEK_SET);
            char line[512];
            while (fgets(line, sizeof(line), lf)) {
                size_t l = strlen(line);
                if (l > 0 && line[l - 1] == '\n')
                    line[l - 1] = '\0';
                if (line[0])
                    cbm_ui_log_append(line);
            }
            tail_pos = ftell(lf);
            fclose(lf);
        }

        if (child_done)
            break;

        struct timespec ts = {0, 500000000};
        cbm_nanosleep(&ts, NULL);
    }

    int wstatus = 0;
    waitpid(child_pid, &wstatus, 0);
    int exit_code = WIFEXITED(wstatus) ? WEXITSTATUS(wstatus) : -1;

    (void)unlink(log_file);
#endif

    if (exit_code != 0) {
        snprintf(job->error_msg, sizeof(job->error_msg), "indexing failed (exit code %d)",
                 exit_code);
        atomic_store(&job->status, 3);
    } else {
        atomic_store(&job->status, 2);
    }
    cbm_log_info("ui.index.done", "path", job->root_path, "rc", exit_code == 0 ? "ok" : "err");
    return NULL;
}

/* POST /api/index — body: {"root_path": "/abs/path", "project_name": "..."} */
static void handle_index_start(cbm_http_conn_t *c, const cbm_http_req_t *req) {
    if (req->body_len == 0 || req->body_len > 4096) {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"invalid body\"}");
        return;
    }

    yyjson_doc *doc = yyjson_read(req->body, req->body_len, 0);
    if (!doc) {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"invalid json\"}");
        return;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *v_path = yyjson_obj_get(root, "root_path");
    if (!v_path || !yyjson_is_str(v_path)) {
        yyjson_doc_free(doc);
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"missing root_path\"}");
        return;
    }
    const char *rpath = yyjson_get_str(v_path);
    yyjson_val *v_project_name = yyjson_obj_get(root, "project_name");
    const char *project_name = yyjson_is_str(v_project_name) ? yyjson_get_str(v_project_name) : "";

    /* Check path exists */
    if (!cbm_is_dir(rpath)) {
        yyjson_doc_free(doc);
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"directory not found\"}");
        return;
    }

    /* Find free job slot */
    int slot = -1;
    for (int i = 0; i < MAX_INDEX_JOBS; i++) {
        int st = atomic_load(&g_index_jobs[i].status);
        if (st == 0 || st == 2 || st == 3) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        yyjson_doc_free(doc);
        cbm_http_replyf(c, 429, g_cors_json, "{\"error\":\"all index slots busy\"}");
        return;
    }

    index_job_t *job = &g_index_jobs[slot];
    snprintf(job->root_path, sizeof(job->root_path), "%s", rpath);
    snprintf(job->project_name, sizeof(job->project_name), "%s", project_name);
    job->error_msg[0] = '\0';
    atomic_store(&job->status, 1);
    yyjson_doc_free(doc);

    /* Spawn background thread */
    cbm_thread_t tid;
    if (cbm_thread_create(&tid, 0, index_thread_fn, job) != 0) {
        atomic_store(&job->status, 3);
        snprintf(job->error_msg, sizeof(job->error_msg), "thread creation failed");
        cbm_http_replyf(c, 500, g_cors_json, "{\"error\":\"thread creation failed\"}");
        return;
    }
    cbm_thread_detach(&tid); /* Don't leak thread handle */

    cbm_http_replyf(c, 202, g_cors_json, "{\"status\":\"indexing\",\"slot\":%d,\"path\":\"%s\"}",
                    slot, job->root_path);
}

/* GET /api/index-status — returns status of all index jobs */
static void handle_index_status(cbm_http_conn_t *c) {
    char buf[2048] = "[";
    int pos = 1;
    for (int i = 0; i < MAX_INDEX_JOBS; i++) {
        int st = atomic_load(&g_index_jobs[i].status);
        if (st == 0)
            continue;
        if (pos > 1)
            buf[pos++] = ',';
        const char *ss = st == 1 ? "indexing" : st == 2 ? "done" : "error";
        pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos,
                        "{\"slot\":%d,\"status\":\"%s\",\"path\":\"%s\",\"error\":\"%s\"}", i, ss,
                        g_index_jobs[i].root_path, st == 3 ? g_index_jobs[i].error_msg : "");
    }
    buf[pos++] = ']';
    buf[pos] = '\0';
    cbm_http_replyf(c, 200, g_cors_json, "%s", buf);
}

static void unwatch_project(cbm_http_server_t *srv, const char *name) {
    if (srv && srv->watcher) {
        cbm_watcher_unwatch(srv->watcher, name);
    }
}

/* DELETE /api/project?name=X — deletes the .db file */
static void handle_delete_project(cbm_http_server_t *srv, cbm_http_conn_t *c,
                                  const cbm_http_req_t *req) {
    char name[256] = {0};
    if (!cbm_http_query_param(req->query, "name", name, (int)sizeof(name)) || name[0] == '\0') {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"missing name\"}");
        return;
    }

    char db_path[1024];
    db_path_for_project(name, db_path, sizeof(db_path));
    if (db_path[0] == '\0') {
        cbm_http_replyf(c, 404, g_cors_json, "{\"error\":\"project not found\"}");
        return;
    }

    if (unlink(db_path) != 0) {
        if (errno == ENOENT) {
            unwatch_project(srv, name);
            cbm_http_replyf(c, 404, g_cors_json, "{\"error\":\"project not found\"}");
            return;
        }
        cbm_http_replyf(c, 500, g_cors_json, "{\"error\":\"failed to delete\"}");
        return;
    }

    /* Also remove WAL and SHM files if they exist */
    char wal_path[1040], shm_path[1040];
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    snprintf(shm_path, sizeof(shm_path), "%s-shm", db_path);
    (void)unlink(wal_path);
    (void)unlink(shm_path);

    unwatch_project(srv, name);
    cbm_log_info("ui.project.deleted", "name", name);
    cbm_http_replyf(c, 200, g_cors_json, "{\"deleted\":true}");
}

/* GET /api/project-health?name=X — checks db integrity */
static void handle_project_health(cbm_http_conn_t *c, const cbm_http_req_t *req) {
    char name[256] = {0};
    if (!cbm_http_query_param(req->query, "name", name, (int)sizeof(name)) || name[0] == '\0') {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"missing name\"}");
        return;
    }

    char db_path[1024];
    db_path_for_project(name, db_path, sizeof(db_path));

    if (!cbm_file_exists(db_path)) {
        cbm_http_replyf(c, 200, g_cors_json, "{\"status\":\"missing\"}");
        return;
    }

    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        cbm_http_replyf(c, 200, g_cors_json, "{\"status\":\"corrupt\",\"reason\":\"cannot open\"}");
        return;
    }

    int node_count = cbm_store_count_nodes(store, name);
    int edge_count = cbm_store_count_edges(store, name);
    cbm_store_close(store);

    int64_t size = cbm_file_size(db_path);

    cbm_http_replyf(c, 200, g_cors_json,
                    "{\"status\":\"healthy\",\"nodes\":%d,\"edges\":%d,\"size_bytes\":%lld}",
                    node_count, edge_count, (long long)size);
}

/* ── Handle GET /api/layout ───────────────────────────────────── */

/* Find distinct target_project values from CROSS_* edges in a store.
 * Writes up to max_out project names (heap-allocated). Returns count. */
static int find_cross_repo_targets(cbm_store_t *store, const char *project, char **out,
                                   int max_out) {
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        return 0;
    }
    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(
            db,
            "SELECT DISTINCT json_extract(properties, '$.target_project') FROM edges "
            "WHERE project = ?1 AND type LIKE 'CROSS_%' "
            "AND json_extract(properties, '$.target_project') IS NOT NULL",
            -1, &s, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(s, 1, project, -1, SQLITE_STATIC);
    int count = 0;
    while (sqlite3_step(s) == SQLITE_ROW && count < max_out) {
        const char *tp = (const char *)sqlite3_column_text(s, 0);
        if (tp && tp[0]) {
            size_t len = strlen(tp);
            out[count] = malloc(len + 1);
            memcpy(out[count], tp, len + 1);
            count++;
        }
    }
    sqlite3_finalize(s);
    return count;
}

enum { LAYOUT_MAX_LINKED = 16 };
#define LAYOUT_GALAXY_SPACING 600.0
#define LAYOUT_GALAXY_PAD 400.0

/* Bounding-radius of a layout result: max distance from origin across all
 * nodes. Used to size galaxy spacing so satellites don't overlap the primary
 * cluster. Layouts with a 1000-node cluster have radius ~1500; the previous
 * fixed 600 spacing buried satellites inside the primary mass. */
static double layout_radius(const cbm_layout_result_t *r) {
    if (!r || r->node_count == 0)
        return 0.0;
    double max_r2 = 0.0;
    for (int i = 0; i < r->node_count; i++) {
        double x = (double)r->nodes[i].x;
        double y = (double)r->nodes[i].y;
        double z = (double)r->nodes[i].z;
        if (!isfinite(x) || !isfinite(y) || !isfinite(z))
            continue;
        double r2 = x * x + y * y + z * z;
        if (r2 > max_r2)
            max_r2 = r2;
    }
    return sqrt(max_r2);
}

static void handle_layout(cbm_http_conn_t *c, const cbm_http_req_t *req) {
    char project[256] = {0};
    char max_str[32] = {0};

    if (!cbm_http_query_param(req->query, "project", project, (int)sizeof(project)) ||
        project[0] == '\0') {
        cbm_http_replyf(c, 400, g_cors_json, "{\"error\":\"missing project parameter\"}");
        return;
    }

    int max_nodes = 50000;
    if (cbm_http_query_param(req->query, "max_nodes", max_str, (int)sizeof(max_str))) {
        int v = atoi(max_str);
        if (v > 0)
            max_nodes = v;
    }

    char db_path[1024];
    db_path_for_project(project, db_path, sizeof(db_path));

    if (!cbm_file_exists(db_path)) {
        cbm_http_replyf(c, 404, g_cors_json, "{\"error\":\"project not found\"}");
        return;
    }

    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        cbm_http_replyf(c, 500, g_cors_json, "{\"error\":\"cannot open store\"}");
        return;
    }

    cbm_layout_result_t *layout =
        cbm_layout_compute(store, project, CBM_LAYOUT_OVERVIEW, NULL, 0, max_nodes);

    /* Find linked projects from CROSS_* edges. Keep `store` open through the
     * linked-projects loop below so we can resolve target Route QNs against
     * the linked stores when populating cross_edges. */
    char *linked[LAYOUT_MAX_LINKED];
    int linked_count = find_cross_repo_targets(store, project, linked, LAYOUT_MAX_LINKED);

    if (!layout) {
        cbm_store_close(store);
        cbm_http_replyf(c, 500, g_cors_json, "{\"error\":\"layout computation failed\"}");
        return;
    }

    /* Capture primary cluster radius before freeing the layout. */
    double primary_radius = layout_radius(layout);

    /* Build JSON: primary layout + linked_projects */
    char *primary_json = cbm_layout_to_json(layout);
    cbm_layout_free(layout);
    if (!primary_json) {
        cbm_store_close(store);
        cbm_http_replyf(c, 500, g_cors_json, "{\"error\":\"JSON serialization failed\"}");
        return;
    }

    if (linked_count == 0) {
        cbm_store_close(store);
        cbm_http_replyf(c, 200, g_cors_json, "%s", primary_json);
        free(primary_json);
        return;
    }

    /* Parse primary JSON and append linked_projects array */
    yyjson_doc *pdoc = yyjson_read(primary_json, strlen(primary_json), 0);
    free(primary_json);
    if (!pdoc) {
        cbm_store_close(store);
        cbm_http_replyf(c, 500, g_cors_json, "{\"error\":\"JSON parse failed\"}");
        return;
    }

    yyjson_mut_doc *mdoc = yyjson_doc_mut_copy(pdoc, NULL);
    yyjson_doc_free(pdoc);
    yyjson_mut_val *mroot = yyjson_mut_doc_get_root(mdoc);

    yyjson_mut_val *lp_arr = yyjson_mut_arr(mdoc);

    for (int li = 0; li < linked_count; li++) {
        char lp_path[1024];
        db_path_for_project(linked[li], lp_path, sizeof(lp_path));
        if (!cbm_file_exists(lp_path)) {
            free(linked[li]);
            continue;
        }

        cbm_store_t *lp_store = cbm_store_open_path(lp_path);
        if (!lp_store) {
            free(linked[li]);
            continue;
        }

        /* Keep lp_store open through cross_edges resolution below. */
        cbm_layout_result_t *lp_layout =
            cbm_layout_compute(lp_store, linked[li], CBM_LAYOUT_OVERVIEW, NULL, 0, max_nodes);

        if (!lp_layout) {
            cbm_store_close(lp_store);
            free(linked[li]);
            continue;
        }

        double sat_radius = layout_radius(lp_layout);
        char *lp_json = cbm_layout_to_json(lp_layout);
        cbm_layout_free(lp_layout);
        if (!lp_json) {
            cbm_store_close(lp_store);
            free(linked[li]);
            continue;
        }

        /* Parse linked project layout */
        yyjson_doc *lpdoc = yyjson_read(lp_json, strlen(lp_json), 0);
        free(lp_json);
        if (!lpdoc) {
            cbm_store_close(lp_store);
            free(linked[li]);
            continue;
        }

        yyjson_mut_doc *lm = yyjson_doc_mut_copy(lpdoc, NULL);
        yyjson_doc_free(lpdoc);
        yyjson_mut_val *lmroot = yyjson_mut_doc_get_root(lm);

        /* Build linked project entry */
        yyjson_mut_val *entry = yyjson_mut_obj(mdoc);
        yyjson_mut_obj_add_strcpy(mdoc, entry, "project", linked[li]);

        /* Copy nodes and edges from linked layout */
        yyjson_mut_val *ln = yyjson_mut_obj_get(lmroot, "nodes");
        yyjson_mut_val *le = yyjson_mut_obj_get(lmroot, "edges");
        if (ln) {
            yyjson_mut_obj_add_val(mdoc, entry, "nodes", yyjson_mut_val_mut_copy(mdoc, ln));
        }
        if (le) {
            yyjson_mut_obj_add_val(mdoc, entry, "edges", yyjson_mut_val_mut_copy(mdoc, le));
        }

        /* Compute galaxy offset: evenly spaced around primary, far enough out
         * that the primary cluster (radius primary_radius) and the satellite
         * cluster (radius sat_radius) don't overlap. Bounded below by
         * LAYOUT_GALAXY_SPACING for trivially small projects. */
        double angle = (2.0 * 3.14159265358979) * (double)li / (double)linked_count;
        double dist = primary_radius + sat_radius + LAYOUT_GALAXY_PAD;
        if (dist < LAYOUT_GALAXY_SPACING) {
            dist = LAYOUT_GALAXY_SPACING;
        }
        yyjson_mut_val *offset = yyjson_mut_obj(mdoc);
        yyjson_mut_obj_add_real(mdoc, offset, "x", cos(angle) * dist);
        yyjson_mut_obj_add_real(mdoc, offset, "y", sin(angle) * dist);
        yyjson_mut_obj_add_real(mdoc, offset, "z", 0.0);
        yyjson_mut_obj_add_val(mdoc, entry, "offset", offset);

        /* Populate cross_edges connecting primary→this linked galaxy. Each
         * entry: {source: <primary node id>, target: <linked node id>, type}.
         *
         * A CROSS_* edge in the source store points caller_id → local_route_id
         * (a Route node in the source store). The Route's qualified_name is
         * canonical and the same Route exists in the linked store too — that's
         * the cross-repo matching contract. Join edges → nodes in source to
         * pull the QN, then look it up in the linked store. */
        yyjson_mut_val *cross_arr = yyjson_mut_arr(mdoc);
        struct sqlite3 *src_db = cbm_store_get_db(store);
        struct sqlite3 *lp_db = cbm_store_get_db(lp_store);
        if (src_db && lp_db) {
            sqlite3_stmt *eq = NULL;
            if (sqlite3_prepare_v2(src_db,
                                   "SELECT e.source_id, e.type, n.qualified_name "
                                   "FROM edges e JOIN nodes n "
                                   "  ON n.id = e.target_id AND n.project = e.project "
                                   "WHERE e.project = ?1 AND e.type LIKE 'CROSS_%' "
                                   "  AND json_extract(e.properties, '$.target_project') = ?2 "
                                   "  AND n.qualified_name IS NOT NULL",
                                   -1, &eq, NULL) == SQLITE_OK) {
                sqlite3_bind_text(eq, 1, project, -1, SQLITE_STATIC);
                sqlite3_bind_text(eq, 2, linked[li], -1, SQLITE_STATIC);

                sqlite3_stmt *lookup = NULL;
                sqlite3_prepare_v2(lp_db, "SELECT id FROM nodes WHERE qualified_name = ?1 LIMIT 1",
                                   -1, &lookup, NULL);

                while (sqlite3_step(eq) == SQLITE_ROW) {
                    int64_t src_id = sqlite3_column_int64(eq, 0);
                    const char *etype = (const char *)sqlite3_column_text(eq, 1);
                    const char *qn = (const char *)sqlite3_column_text(eq, 2);
                    if (!qn || !etype || !lookup) {
                        continue;
                    }
                    sqlite3_reset(lookup);
                    sqlite3_clear_bindings(lookup);
                    sqlite3_bind_text(lookup, 1, qn, -1, SQLITE_STATIC);
                    if (sqlite3_step(lookup) != SQLITE_ROW) {
                        continue;
                    }
                    int64_t tgt_id = sqlite3_column_int64(lookup, 0);
                    yyjson_mut_val *ce = yyjson_mut_obj(mdoc);
                    yyjson_mut_obj_add_int(mdoc, ce, "source", src_id);
                    yyjson_mut_obj_add_int(mdoc, ce, "target", tgt_id);
                    yyjson_mut_obj_add_strcpy(mdoc, ce, "type", etype);
                    yyjson_mut_arr_append(cross_arr, ce);
                }
                if (lookup)
                    sqlite3_finalize(lookup);
                sqlite3_finalize(eq);
            }
        }
        yyjson_mut_obj_add_val(mdoc, entry, "cross_edges", cross_arr);

        cbm_store_close(lp_store);
        yyjson_mut_arr_append(lp_arr, entry);
        yyjson_mut_doc_free(lm);
        free(linked[li]);
    }

    cbm_store_close(store);
    yyjson_mut_obj_add_val(mdoc, mroot, "linked_projects", lp_arr);

    size_t len = 0;
    char *final_json = yyjson_mut_write(mdoc, 0, &len);
    yyjson_mut_doc_free(mdoc);

    if (final_json) {
        cbm_http_replyf(c, 200, g_cors_json, "%s", final_json);
        free(final_json);
    } else {
        cbm_http_replyf(c, 500, g_cors_json, "{\"error\":\"JSON write failed\"}");
    }
}

/* ── Handle JSON-RPC request ──────────────────────────────────── */

static void handle_rpc(cbm_http_conn_t *c, const cbm_http_req_t *req, cbm_mcp_server_t *mcp) {
    if (req->body_len == 0 || req->body_len > MAX_BODY_SIZE || !req->body) {
        cbm_http_replyf(c, 400, g_cors_json,
                        "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32600,"
                        "\"message\":\"invalid request size\"},\"id\":null}");
        return;
    }

    /* req->body is NUL-terminated by the transport */
    char *response = cbm_mcp_server_handle(mcp, req->body);

    if (response) {
        cbm_http_replyf(c, 200, g_cors_json, "%s", response);
        free(response);
    } else {
        cbm_http_replyf(c, 204, g_cors, "%s", "");
    }
}

/* ── Request dispatch ─────────────────────────────────────────── */

static void dispatch_request(cbm_http_server_t *srv, cbm_http_conn_t *c,
                             const cbm_http_req_t *req) {
    /* Build per-request CORS headers (only reflects localhost origins) */
    update_cors(req);

    bool is_get = strcmp(req->method, "GET") == 0;
    bool is_post = strcmp(req->method, "POST") == 0;
    bool is_delete = strcmp(req->method, "DELETE") == 0;

    /* OPTIONS preflight for CORS */
    if (strcmp(req->method, "OPTIONS") == 0) {
        cbm_http_replyf(c, 204, g_cors, "%s", "");
        return;
    }

    /* POST /rpc → JSON-RPC dispatch (reuses existing MCP tools) */
    if (is_post && cbm_http_path_match(req->path, "/rpc")) {
        handle_rpc(c, req, srv->mcp);
        return;
    }

    /* GET /api/layout → 3D graph layout */
    if (is_get && cbm_http_path_match(req->path, "/api/layout*")) {
        handle_layout(c, req);
        return;
    }

    /* POST /api/index → start background indexing */
    if (is_post && cbm_http_path_match(req->path, "/api/index")) {
        handle_index_start(c, req);
        return;
    }

    /* GET /api/index-status → check indexing progress */
    if (is_get && cbm_http_path_match(req->path, "/api/index-status")) {
        handle_index_status(c);
        return;
    }

    /* GET /api/ui-config → language and local UI preferences */
    if (is_get && cbm_http_path_match(req->path, "/api/ui-config")) {
        handle_ui_config(c, req);
        return;
    }

    /* DELETE /api/project → delete a project's .db file */
    if (is_delete && cbm_http_path_match(req->path, "/api/project*")) {
        handle_delete_project(srv, c, req);
        return;
    }

    /* GET /api/browse → directory browser for file picker */
    if (is_get && cbm_http_path_match(req->path, "/api/browse*")) {
        handle_browse(c, req);
        return;
    }

    /* GET /api/adr → get ADR for project */
    if (is_get && cbm_http_path_match(req->path, "/api/adr*")) {
        handle_adr_get(c, req);
        return;
    }

    /* POST /api/adr → save ADR for project */
    if (is_post && cbm_http_path_match(req->path, "/api/adr")) {
        handle_adr_save(c, req);
        return;
    }

    /* GET /api/project-health → check db integrity */
    if (is_get && cbm_http_path_match(req->path, "/api/project-health*")) {
        handle_project_health(c, req);
        return;
    }

    /* GET /api/processes → list running codebase-memory-mcp processes */
    if (is_get && cbm_http_path_match(req->path, "/api/processes")) {
        handle_processes(c);
        return;
    }

    /* GET /api/logs → recent log lines */
    if (is_get && cbm_http_path_match(req->path, "/api/logs*")) {
        handle_logs(c, req);
        return;
    }

    /* POST /api/process-kill → kill a process */
    if (is_post && cbm_http_path_match(req->path, "/api/process-kill")) {
        handle_process_kill(c, req);
        return;
    }

    /* GET / → index.html (no-cache so browser always gets latest) */
    if (cbm_http_path_match(req->path, "/")) {
        const cbm_embedded_file_t *f = cbm_embedded_lookup("/index.html");
        if (f) {
            char html_hdrs[512];
            snprintf(html_hdrs, sizeof(html_hdrs),
                     "%sContent-Type: text/html\r\nCache-Control: no-cache\r\n", g_cors);
            cbm_http_reply_buf(c, 200, html_hdrs, f->data, (size_t)f->size);
            return;
        }
        cbm_http_replyf(c, 404, g_cors, "no frontend embedded");
        return;
    }

    /* GET /assets/... → embedded assets, then generic embedded fallback */
    if (serve_embedded(c, req->path))
        return;

    cbm_http_replyf(c, 404, g_cors, "not found");
}

/* ── Public API ───────────────────────────────────────────────── */

cbm_http_server_t *cbm_http_server_new(int port) {
    cbm_http_server_t *srv = calloc(1, sizeof(*srv));
    if (!srv)
        return NULL;

    srv->port = port;
    atomic_store(&srv->stop_flag, 0);

    /* Create a dedicated MCP server for HTTP (own SQLite connection) */
    srv->mcp = cbm_mcp_server_new(NULL);
    if (!srv->mcp) {
        cbm_log_error("ui.http.mcp_fail", "reason", "cannot create MCP instance");
        free(srv);
        return NULL;
    }

    /* Bind to localhost only (httpd refuses anything else by construction) */
    srv->listener = cbm_httpd_listen(port);
    if (!srv->listener) {
        char port_str[16];
        snprintf(port_str, sizeof(port_str), "%d", port);
        cbm_log_warn("ui.unavailable", "port", port_str, "reason", "in_use", "hint",
                     "use --port=N to override");
        cbm_mcp_server_free(srv->mcp);
        free(srv);
        return NULL;
    }

    srv->port = cbm_httpd_port(srv->listener);
    srv->listener_ok = true;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", srv->port);
    char url[64];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d", srv->port);
    cbm_log_info("ui.serving", "url", url, "port", port_str);

    return srv;
}

void cbm_http_server_free(cbm_http_server_t *srv) {
    if (!srv)
        return;
    cbm_httpd_close(srv->listener);
    cbm_mcp_server_free(srv->mcp);
    free(srv);
}

void cbm_http_server_stop(cbm_http_server_t *srv) {
    if (srv) {
        atomic_store(&srv->stop_flag, 1);
    }
}

void cbm_http_server_run(cbm_http_server_t *srv) {
    if (!srv || !srv->listener_ok)
        return;

    while (!atomic_load(&srv->stop_flag)) {
        cbm_http_conn_t *conn = cbm_httpd_accept(srv->listener, 200);
        if (!conn)
            continue; /* timeout — re-check stop flag */

        uint64_t request_start_ms = cbm_now_ms();
        cbm_http_req_t req;
        int rc = cbm_httpd_read_request(conn, &req);
        if (rc == 0) {
            dispatch_request(srv, conn, &req);
            cbm_log_http_request("graph_ui", req.method, req.path, cbm_http_conn_status(conn),
                                 (int64_t)(cbm_now_ms() - request_start_ms), req.body_len,
                                 cbm_http_conn_response_bytes(conn));
            cbm_http_req_free(&req);
        } else if (rc > 0) {
            /* Parse/transport error with a known HTTP status (400/408/411/413/431).
             * No CORS reflection here — the request was never parsed. */
            cbm_http_replyf(conn, rc, "", "bad request");
            cbm_log_http_request("graph_ui", "", "", cbm_http_conn_status(conn),
                                 (int64_t)(cbm_now_ms() - request_start_ms), 0,
                                 cbm_http_conn_response_bytes(conn));
        }
        cbm_httpd_conn_close(conn);
    }
}

bool cbm_http_server_is_running(const cbm_http_server_t *srv) {
    return srv && srv->listener_ok;
}

int cbm_http_server_port(const cbm_http_server_t *srv) {
    return (srv && srv->listener_ok) ? srv->port : -1;
}

void cbm_http_server_set_recv_deadline_ms(cbm_http_server_t *srv, int ms) {
    if (srv && srv->listener_ok) {
        cbm_httpd_set_recv_deadline_ms(srv->listener, ms);
    }
}

void cbm_http_server_set_watcher(cbm_http_server_t *srv, struct cbm_watcher *watcher) {
    if (srv) {
        srv->watcher = watcher;
    }
}
