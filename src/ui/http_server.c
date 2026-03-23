/*
 * http_server.c — Mongoose-based HTTP server for graph UI.
 *
 * Routes:
 *   GET /             → embedded index.html
 *   GET /assets/...   → embedded JS/CSS
 *   POST /rpc         → JSON-RPC dispatch via own cbm_mcp_server_t
 *   OPTIONS /rpc      → CORS preflight (for vite dev on :5173)
 *   *                 → 404
 *
 * Runs in a background pthread. Binds to 127.0.0.1 only.
 * Has its own cbm_mcp_server_t with a separate SQLite connection (WAL reader).
 */
#include "ui/http_server.h"
#include "ui/embedded_assets.h"
#include "ui/layout3d.h"
#include "mcp/mcp.h"
#include "store/store.h"
/* pipeline.h no longer needed — indexing runs as subprocess */
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"

#include <mongoose/mongoose.h>
#include <yyjson/yyjson.h>

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <windows.h>
#include <process.h>
#include <psapi.h> /* GetProcessMemoryInfo */
#else
#include <unistd.h>
#include <sys/wait.h>
#endif
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

/* ── Constants ────────────────────────────────────────────────── */

/* Max JSON-RPC request body size (1 MB) */
#define MAX_BODY_SIZE (1024 * 1024)

/* ── CORS: only allow localhost origins (blocks remote website attacks) ────── */

/* Per-request CORS header buffers. Updated at the start of each HTTP handler
 * call by update_cors(). Single-threaded mongoose event loop makes statics safe. */
static char g_cors[256];      /* CORS headers only */
static char g_cors_json[512]; /* CORS + Content-Type: application/json */

/* Inspect the Origin header and only reflect it if it's a localhost URL.
 * This prevents remote websites from making cross-origin requests to the
 * local graph-ui server (the key defense against CORS-based data exfil). */
static void update_cors(struct mg_http_message *hm) {
    struct mg_str *origin = mg_http_get_header(hm, "Origin");
    if (origin && origin->len > 0 &&
        (mg_match(*origin, mg_str("http://localhost:*"), NULL) ||
         mg_match(*origin, mg_str("http://127.0.0.1:*"), NULL))) {
        snprintf(g_cors, sizeof(g_cors),
                 "Access-Control-Allow-Origin: %.*s\r\n"
                 "Access-Control-Allow-Methods: POST, GET, DELETE, OPTIONS\r\n"
                 "Access-Control-Allow-Headers: Content-Type\r\n",
                 (int)origin->len, origin->buf);
    } else {
        /* No Access-Control-Allow-Origin → browser blocks cross-origin access */
        snprintf(g_cors, sizeof(g_cors),
                 "Access-Control-Allow-Methods: POST, GET, DELETE, OPTIONS\r\n"
                 "Access-Control-Allow-Headers: Content-Type\r\n");
    }
    snprintf(g_cors_json, sizeof(g_cors_json),
             "%sContent-Type: application/json\r\n", g_cors);
}

/* ── Server state ─────────────────────────────────────────────── */

struct cbm_http_server {
    struct mg_mgr mgr;
    cbm_mcp_server_t *mcp; /* own MCP server instance (read-only) */
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

static bool serve_embedded(struct mg_connection *c, const char *path) {
    const cbm_embedded_file_t *f = cbm_embedded_lookup(path);
    if (!f)
        return false;

    /* Build headers with correct Content-Type for this asset */
    char hdrs[512];
    snprintf(hdrs, sizeof(hdrs),
             "%sContent-Type: %s\r\n"
             "Cache-Control: public, max-age=31536000, immutable\r\n",
             g_cors, f->content_type);

    mg_http_reply(c, 200, hdrs, "%.*s", (int)f->size, (const char *)f->data);
    return true;
}

/* Forward declaration */
static bool get_query_param(struct mg_str query, const char *name, char *buf, int bufsz);

/* ── Log ring buffer ──────────────────────────────────────────── */

#define LOG_RING_SIZE 500
#define LOG_LINE_MAX 512

static char g_log_ring[LOG_RING_SIZE][LOG_LINE_MAX];
static int g_log_head = 0;
static int g_log_count = 0;
static cbm_mutex_t g_log_mutex;
static atomic_int g_log_mutex_init = 0;

/* Called from a log hook — appends a line to the ring buffer (thread-safe) */
void cbm_ui_log_append(const char *line) {
    if (!line)
        return;
    if (!atomic_load(&g_log_mutex_init)) {
        cbm_mutex_init(&g_log_mutex);
        atomic_store(&g_log_mutex_init, 1);
    }
    cbm_mutex_lock(&g_log_mutex);
    snprintf(g_log_ring[g_log_head], LOG_LINE_MAX, "%s", line);
    g_log_head = (g_log_head + 1) % LOG_RING_SIZE;
    if (g_log_count < LOG_RING_SIZE)
        g_log_count++;
    cbm_mutex_unlock(&g_log_mutex);
}

/* GET /api/logs?lines=N — returns last N log lines */
static void handle_logs(struct mg_connection *c, struct mg_http_message *hm) {
    char lines_str[16] = {0};
    int max_lines = 100;
    if (get_query_param(hm->query, "lines", lines_str, (int)sizeof(lines_str))) {
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
        mg_http_reply(c, 500, g_cors, "oom");
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

    mg_http_reply(c, 200, g_cors_json, "%s", buf);
    free(buf);
}

/* ── Process monitoring ───────────────────────────────────────── */

#ifndef _WIN32
#include <sys/resource.h>
#endif
#include <signal.h>

/* GET /api/processes — list codebase-memory-mcp processes via ps */
static void handle_processes(struct mg_connection *c) {
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

    mg_http_reply(c, 200, g_cors_json, "%s", buf);
}

/* POST /api/process-kill — kill a process by PID */
static void handle_process_kill(struct mg_connection *c, struct mg_http_message *hm) {
    if (hm->body.len == 0 || hm->body.len > 256) {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"invalid body\"}");
        return;
    }

    char body[257];
    memcpy(body, hm->body.buf, hm->body.len);
    body[hm->body.len] = '\0';

    yyjson_doc *doc = yyjson_read(body, hm->body.len, 0);
    if (!doc) {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"invalid json\"}");
        return;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *v_pid = yyjson_obj_get(root, "pid");
    if (!v_pid || !yyjson_is_int(v_pid)) {
        yyjson_doc_free(doc);
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"missing pid\"}");
        return;
    }
    int target_pid = (int)yyjson_get_int(v_pid);
    yyjson_doc_free(doc);

#ifdef _WIN32
    if (target_pid == (int)_getpid()) {
#else
    if (target_pid == (int)getpid()) {
#endif
        mg_http_reply(c, 400, g_cors_json,
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
            mg_http_reply(c, 403, g_cors_json,
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
        mg_http_reply(c, 500, g_cors_json,
                      "{\"error\":\"kill failed\"}");
        return;
    }
    CloseHandle(hproc);
#else
    if (kill(target_pid, SIGTERM) != 0) {
        mg_http_reply(c, 500, g_cors_json,
                      "{\"error\":\"kill failed\"}");
        return;
    }
#endif

    mg_http_reply(c, 200, g_cors_json, "{\"killed\":%d}",
                  target_pid);
}

/* ── Directory browser ────────────────────────────────────────── */

#include <dirent.h>

/* GET /api/browse?path=/some/dir — list subdirectories for file picker */
static void handle_browse(struct mg_connection *c, struct mg_http_message *hm) {
    char path[1024] = {0};
    if (!get_query_param(hm->query, "path", path, (int)sizeof(path)) || path[0] == '\0') {
        /* Default to home directory */
        const char *home = cbm_get_home_dir();
        if (home)
            snprintf(path, sizeof(path), "%s", home);
        else
            snprintf(path, sizeof(path), "/");
    }

    if (!cbm_is_dir(path)) {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"not a directory\"}");
        return;
    }

    DIR *dir = opendir(path);
    if (!dir) {
        mg_http_reply(c, 403, g_cors_json,
                      "{\"error\":\"cannot open directory\"}");
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
        pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos, "\"%s\"", ent->d_name);
        if (pos >= (int)sizeof(buf)) {
            pos = (int)sizeof(buf) - 1;
        }
        count++;

        if (count >= 200)
            break; /* safety limit */
    }
    closedir(dir);

    /* Parent path */
    char parent[1024];
    snprintf(parent, sizeof(parent), "%s", path);
    char *last_slash = strrchr(parent, '/');
    if (last_slash && last_slash != parent)
        *last_slash = '\0';
    else
        snprintf(parent, sizeof(parent), "/");

    pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos, "],\"parent\":\"%s\"}", parent);
    mg_http_reply(c, 200, g_cors_json, "%s", buf);
}

/* ── ADR endpoints ────────────────────────────────────────────── */

/* GET /api/adr?project=X — get ADR content for a project */
static void handle_adr_get(struct mg_connection *c, struct mg_http_message *hm) {
    char name[256] = {0};
    if (!get_query_param(hm->query, "project", name, (int)sizeof(name)) || name[0] == '\0') {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"missing project\"}");
        return;
    }

    const char *home = cbm_get_home_dir();
    if (!home)
        home = cbm_tmpdir();
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/.cache/codebase-memory-mcp/%s.db", home, name);

    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        mg_http_reply(c, 200, g_cors_json,
                      "{\"has_adr\":false}");
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
            mg_http_reply(c, 200, g_cors_json, "%s", buf);
            free(buf);
        } else {
            mg_http_reply(c, 500, g_cors, "oom");
        }
        cbm_store_adr_free(&adr);
    } else {
        mg_http_reply(c, 200, g_cors_json,
                      "{\"has_adr\":false}");
    }
    cbm_store_close(store);
}

/* POST /api/adr — save ADR content. Body: {"project":"...","content":"..."} */
static void handle_adr_save(struct mg_connection *c, struct mg_http_message *hm) {
    if (hm->body.len == 0 || hm->body.len > 16384) {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"invalid body\"}");
        return;
    }

    char *body = malloc(hm->body.len + 1);
    if (!body) {
        mg_http_reply(c, 500, g_cors, "oom");
        return;
    }
    memcpy(body, hm->body.buf, hm->body.len);
    body[hm->body.len] = '\0';

    yyjson_doc *doc = yyjson_read(body, hm->body.len, 0);
    free(body);
    if (!doc) {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"invalid json\"}");
        return;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *v_proj = yyjson_obj_get(root, "project");
    yyjson_val *v_content = yyjson_obj_get(root, "content");
    if (!v_proj || !yyjson_is_str(v_proj) || !v_content || !yyjson_is_str(v_content)) {
        yyjson_doc_free(doc);
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"missing project or content\"}");
        return;
    }

    const char *proj = yyjson_get_str(v_proj);
    const char *content = yyjson_get_str(v_content);

    const char *home = cbm_get_home_dir();
    if (!home)
        home = cbm_tmpdir();
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/.cache/codebase-memory-mcp/%s.db", home, proj);

    cbm_store_t *store = cbm_store_open_path(db_path);
    yyjson_doc_free(doc);
    if (!store) {
        mg_http_reply(c, 500, g_cors_json,
                      "{\"error\":\"cannot open store\"}");
        return;
    }

    int rc = cbm_store_adr_store(store, proj, content);
    cbm_store_close(store);

    if (rc == CBM_STORE_OK) {
        mg_http_reply(c, 200, g_cors_json,
                      "{\"saved\":true}");
    } else {
        mg_http_reply(c, 500, g_cors_json,
                      "{\"error\":\"save failed\"}");
    }
}

/* ── Background indexing ──────────────────────────────────────── */

static char g_binary_path[1024] = {0};

void cbm_http_server_set_binary_path(const char *path) {
    if (path) {
        snprintf(g_binary_path, sizeof(g_binary_path), "%s", path);
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
#ifdef _WIN32
        GetModuleFileNameA(NULL, self_path, sizeof(self_path));
#elif defined(__APPLE__)
        uint32_t sz = sizeof(self_path);
        _NSGetExecutablePath(self_path, &sz);
#else
        ssize_t len = readlink("/proc/self/exe", self_path, sizeof(self_path) - 1);
        if (len > 0)
            self_path[len] = '\0';
#endif
        bin = self_path[0] ? self_path : "codebase-memory-mcp";
    }

    char log_file[256];
    char json_arg[1200];
    snprintf(json_arg, sizeof(json_arg), "{\"repo_path\":\"%s\"}", job->root_path);

#ifdef _WIN32
    snprintf(log_file, sizeof(log_file), "%s\\cbm_index_%d.log",
             getenv("TEMP") ? getenv("TEMP") : ".", (int)_getpid());

    /* Build command line for CreateProcess */
    char cmdline[2048];
    snprintf(cmdline, sizeof(cmdline), "\"%s\" cli index_repository \"%s\"", bin, json_arg);

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
        execl(bin, bin, "cli", "index_repository", json_arg, (char *)NULL);
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

/* POST /api/index — body: {"root_path": "/abs/path"} → starts background indexing */
static void handle_index_start(struct mg_connection *c, struct mg_http_message *hm) {
    if (hm->body.len == 0 || hm->body.len > 4096) {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"invalid body\"}");
        return;
    }

    char body_buf[4097];
    memcpy(body_buf, hm->body.buf, hm->body.len);
    body_buf[hm->body.len] = '\0';

    yyjson_doc *doc = yyjson_read(body_buf, hm->body.len, 0);
    if (!doc) {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"invalid json\"}");
        return;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *v_path = yyjson_obj_get(root, "root_path");
    if (!v_path || !yyjson_is_str(v_path)) {
        yyjson_doc_free(doc);
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"missing root_path\"}");
        return;
    }
    const char *rpath = yyjson_get_str(v_path);

    /* Check path exists */
    if (!cbm_is_dir(rpath)) {
        yyjson_doc_free(doc);
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"directory not found\"}");
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
        mg_http_reply(c, 429, g_cors_json,
                      "{\"error\":\"all index slots busy\"}");
        return;
    }

    index_job_t *job = &g_index_jobs[slot];
    snprintf(job->root_path, sizeof(job->root_path), "%s", rpath);
    job->error_msg[0] = '\0';
    atomic_store(&job->status, 1);
    yyjson_doc_free(doc);

    /* Spawn background thread */
    cbm_thread_t tid;
    if (cbm_thread_create(&tid, 0, index_thread_fn, job) != 0) {
        atomic_store(&job->status, 3);
        snprintf(job->error_msg, sizeof(job->error_msg), "thread creation failed");
        mg_http_reply(c, 500, g_cors_json,
                      "{\"error\":\"thread creation failed\"}");
        return;
    }

    mg_http_reply(c, 202, g_cors_json,
                  "{\"status\":\"indexing\",\"slot\":%d,\"path\":\"%s\"}", slot, job->root_path);
}

/* GET /api/index-status — returns status of all index jobs */
static void handle_index_status(struct mg_connection *c) {
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
    mg_http_reply(c, 200, g_cors_json, "%s", buf);
}

/* DELETE /api/project?name=X — deletes the .db file */
static void handle_delete_project(struct mg_connection *c, struct mg_http_message *hm) {
    char name[256] = {0};
    if (!get_query_param(hm->query, "name", name, (int)sizeof(name)) || name[0] == '\0') {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"missing name\"}");
        return;
    }

    const char *home = cbm_get_home_dir();
    if (!home)
        home = cbm_tmpdir();
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/.cache/codebase-memory-mcp/%s.db", home, name);

    if (!cbm_file_exists(db_path)) {
        mg_http_reply(c, 404, g_cors_json,
                      "{\"error\":\"project not found\"}");
        return;
    }

    if (unlink(db_path) != 0) {
        mg_http_reply(c, 500, g_cors_json,
                      "{\"error\":\"failed to delete\"}");
        return;
    }

    /* Also remove WAL and SHM files if they exist */
    char wal_path[1040], shm_path[1040];
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    snprintf(shm_path, sizeof(shm_path), "%s-shm", db_path);
    (void)unlink(wal_path);
    (void)unlink(shm_path);

    cbm_log_info("ui.project.deleted", "name", name);
    mg_http_reply(c, 200, g_cors_json, "{\"deleted\":true}");
}

/* GET /api/project-health?name=X — checks db integrity */
static void handle_project_health(struct mg_connection *c, struct mg_http_message *hm) {
    char name[256] = {0};
    if (!get_query_param(hm->query, "name", name, (int)sizeof(name)) || name[0] == '\0') {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"missing name\"}");
        return;
    }

    const char *home = cbm_get_home_dir();
    if (!home)
        home = cbm_tmpdir();
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/.cache/codebase-memory-mcp/%s.db", home, name);

    if (!cbm_file_exists(db_path)) {
        mg_http_reply(c, 200, g_cors_json,
                      "{\"status\":\"missing\"}");
        return;
    }

    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        mg_http_reply(c, 200, g_cors_json,
                      "{\"status\":\"corrupt\",\"reason\":\"cannot open\"}");
        return;
    }

    int node_count = cbm_store_count_nodes(store, name);
    int edge_count = cbm_store_count_edges(store, name);
    cbm_store_close(store);

    int64_t size = cbm_file_size(db_path);

    mg_http_reply(c, 200, g_cors_json,
                  "{\"status\":\"healthy\",\"nodes\":%d,\"edges\":%d,\"size_bytes\":%lld}",
                  node_count, edge_count, (long long)size);
}

/* ── Extract query parameter from URI ─────────────────────────── */

static bool get_query_param(struct mg_str query, const char *name, char *buf, int bufsz) {
    int n = mg_http_get_var(&query, name, buf, (size_t)bufsz);
    return n > 0;
}

/* ── Handle GET /api/layout ───────────────────────────────────── */

static void handle_layout(struct mg_connection *c, struct mg_http_message *hm) {
    char project[256] = {0};
    char max_str[32] = {0};

    if (!get_query_param(hm->query, "project", project, (int)sizeof(project)) ||
        project[0] == '\0') {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"error\":\"missing project parameter\"}");
        return;
    }

    int max_nodes = 50000;
    if (get_query_param(hm->query, "max_nodes", max_str, (int)sizeof(max_str))) {
        int v = atoi(max_str);
        if (v > 0)
            max_nodes = v;
    }

    /* Open a read-only store for this project */
    const char *home = cbm_get_home_dir();
    if (!home)
        home = cbm_tmpdir();
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/.cache/codebase-memory-mcp/%s.db", home, project);

    if (!cbm_file_exists(db_path)) {
        mg_http_reply(c, 404, g_cors_json,
                      "{\"error\":\"project not found\"}");
        return;
    }

    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        mg_http_reply(c, 500, g_cors_json,
                      "{\"error\":\"cannot open store\"}");
        return;
    }

    cbm_layout_result_t *layout =
        cbm_layout_compute(store, project, CBM_LAYOUT_OVERVIEW, NULL, 0, max_nodes);
    cbm_store_close(store);

    if (!layout) {
        mg_http_reply(c, 500, g_cors_json,
                      "{\"error\":\"layout computation failed\"}");
        return;
    }

    char *json = cbm_layout_to_json(layout);
    cbm_layout_free(layout);

    if (!json) {
        mg_http_reply(c, 500, g_cors_json,
                      "{\"error\":\"JSON serialization failed\"}");
        return;
    }

    mg_http_reply(c, 200, g_cors_json, "%s", json);
    free(json);
}

/* ── Handle JSON-RPC request ──────────────────────────────────── */

static void handle_rpc(struct mg_connection *c, struct mg_http_message *hm, cbm_mcp_server_t *mcp) {
    if (hm->body.len == 0 || hm->body.len > MAX_BODY_SIZE) {
        mg_http_reply(c, 400, g_cors_json,
                      "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32600,"
                      "\"message\":\"invalid request size\"},\"id\":null}");
        return;
    }

    /* NUL-terminate the body for cbm_mcp_server_handle */
    char *body = malloc(hm->body.len + 1);
    if (!body) {
        mg_http_reply(c, 500, g_cors, "out of memory");
        return;
    }
    memcpy(body, hm->body.buf, hm->body.len);
    body[hm->body.len] = '\0';

    char *response = cbm_mcp_server_handle(mcp, body);
    free(body);

    if (response) {
        mg_http_reply(c, 200, g_cors_json, "%s", response);
        free(response);
    } else {
        mg_http_reply(c, 204, g_cors, "");
    }
}

/* ── HTTP event handler ───────────────────────────────────────── */

static void http_handler(struct mg_connection *c, int ev, void *ev_data) {
    if (ev != MG_EV_HTTP_MSG)
        return;

    struct mg_http_message *hm = ev_data;
    cbm_http_server_t *srv = c->fn_data;

    /* Build per-request CORS headers (only reflects localhost origins) */
    update_cors(hm);

    /* OPTIONS preflight for CORS */
    if (mg_strcmp(hm->method, mg_str("OPTIONS")) == 0) {
                char opt_hdrs[512];
        snprintf(opt_hdrs, sizeof(opt_hdrs), "%sContent-Length: 0\r\n", g_cors);
        mg_http_reply(c, 204, opt_hdrs, "");
        return;
    }

    /* POST /rpc → JSON-RPC dispatch (reuses existing MCP tools) */
    if (mg_strcmp(hm->method, mg_str("POST")) == 0 && mg_match(hm->uri, mg_str("/rpc"), NULL)) {
        handle_rpc(c, hm, srv->mcp);
        return;
    }

    /* GET /api/layout → 3D graph layout */
    if (mg_strcmp(hm->method, mg_str("GET")) == 0 &&
        mg_match(hm->uri, mg_str("/api/layout*"), NULL)) {
        handle_layout(c, hm);
        return;
    }

    /* POST /api/index → start background indexing */
    if (mg_strcmp(hm->method, mg_str("POST")) == 0 &&
        mg_match(hm->uri, mg_str("/api/index"), NULL)) {
        handle_index_start(c, hm);
        return;
    }

    /* GET /api/index-status → check indexing progress */
    if (mg_strcmp(hm->method, mg_str("GET")) == 0 &&
        mg_match(hm->uri, mg_str("/api/index-status"), NULL)) {
        handle_index_status(c);
        return;
    }

    /* DELETE /api/project → delete a project's .db file */
    if (mg_strcmp(hm->method, mg_str("DELETE")) == 0 &&
        mg_match(hm->uri, mg_str("/api/project*"), NULL)) {
        handle_delete_project(c, hm);
        return;
    }

    /* GET /api/browse → directory browser for file picker */
    if (mg_strcmp(hm->method, mg_str("GET")) == 0 &&
        mg_match(hm->uri, mg_str("/api/browse*"), NULL)) {
        handle_browse(c, hm);
        return;
    }

    /* GET /api/adr → get ADR for project */
    if (mg_strcmp(hm->method, mg_str("GET")) == 0 && mg_match(hm->uri, mg_str("/api/adr*"), NULL)) {
        handle_adr_get(c, hm);
        return;
    }

    /* POST /api/adr → save ADR for project */
    if (mg_strcmp(hm->method, mg_str("POST")) == 0 && mg_match(hm->uri, mg_str("/api/adr"), NULL)) {
        handle_adr_save(c, hm);
        return;
    }

    /* GET /api/project-health → check db integrity */
    if (mg_strcmp(hm->method, mg_str("GET")) == 0 &&
        mg_match(hm->uri, mg_str("/api/project-health*"), NULL)) {
        handle_project_health(c, hm);
        return;
    }

    /* GET /api/processes → list running codebase-memory-mcp processes */
    if (mg_strcmp(hm->method, mg_str("GET")) == 0 &&
        mg_match(hm->uri, mg_str("/api/processes"), NULL)) {
        handle_processes(c);
        return;
    }

    /* GET /api/logs → recent log lines */
    if (mg_strcmp(hm->method, mg_str("GET")) == 0 &&
        mg_match(hm->uri, mg_str("/api/logs*"), NULL)) {
        handle_logs(c, hm);
        return;
    }

    /* POST /api/process-kill → kill a process */
    if (mg_strcmp(hm->method, mg_str("POST")) == 0 &&
        mg_match(hm->uri, mg_str("/api/process-kill"), NULL)) {
        handle_process_kill(c, hm);
        return;
    }

    /* GET / → index.html (no-cache so browser always gets latest) */
    if (mg_match(hm->uri, mg_str("/"), NULL)) {
        const cbm_embedded_file_t *f = cbm_embedded_lookup("/index.html");
        if (f) {
            char html_hdrs[512];
            snprintf(html_hdrs, sizeof(html_hdrs),
                     "%sContent-Type: text/html\r\nCache-Control: no-cache\r\n", g_cors);
            mg_http_reply(c, 200, html_hdrs, "%.*s", (int)f->size, (const char *)f->data);
            return;
        }
        mg_http_reply(c, 404, g_cors, "no frontend embedded");
        return;
    }

    /* GET /assets/... → embedded assets */
    if (mg_match(hm->uri, mg_str("/assets/*"), NULL)) {
        /* Build path string from mg_str */
        char path[256];
        int len = (int)hm->uri.len;
        if (len >= (int)sizeof(path))
            len = (int)sizeof(path) - 1;
        memcpy(path, hm->uri.buf, (size_t)len);
        path[len] = '\0';

        if (serve_embedded(c, path))
            return;
        mg_http_reply(c, 404, g_cors, "not found");
        return;
    }

    /* Fallback: try as embedded path, then 404 */
    {
        char path[256];
        int len = (int)hm->uri.len;
        if (len >= (int)sizeof(path))
            len = (int)sizeof(path) - 1;
        memcpy(path, hm->uri.buf, (size_t)len);
        path[len] = '\0';

        if (serve_embedded(c, path))
            return;
    }

    mg_http_reply(c, 404, g_cors, "not found");
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

    /* Initialize Mongoose */
    mg_mgr_init(&srv->mgr);
    srv->mgr.userdata = srv;

    /* Bind to localhost only */
    char url[64];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d", port);

    struct mg_connection *listener = mg_http_listen(&srv->mgr, url, http_handler, srv);
    if (!listener) {
        char port_str[16];
        snprintf(port_str, sizeof(port_str), "%d", port);
        cbm_log_warn("ui.unavailable", "port", port_str, "reason", "in_use", "hint",
                     "use --port=N to override");
        cbm_mcp_server_free(srv->mcp);
        mg_mgr_free(&srv->mgr);
        free(srv);
        return NULL;
    }

    srv->listener_ok = true;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);
    cbm_log_info("ui.serving", "url", url, "port", port_str);

    return srv;
}

void cbm_http_server_free(cbm_http_server_t *srv) {
    if (!srv)
        return;
    mg_mgr_free(&srv->mgr);
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
        mg_mgr_poll(&srv->mgr, 200); /* 200ms poll interval */
    }
}

bool cbm_http_server_is_running(const cbm_http_server_t *srv) {
    return srv && srv->listener_ok;
}
