/*
 * progress_sink.c — Human-readable progress for one-shot CLI commands.
 *
 * Maps structured log events (msg=pass.timing, msg=pipeline.done, etc.)
 * to phase labels on stderr. Interactive terminals enable it automatically;
 * --progress forces it for redirected stderr. When installed, it replaces
 * default log output.
 *
 * Thread-safe: one sink mutex serializes shared summary state and each complete
 * output update, not merely the individual stdio calls.
 */
#include "progress_sink.h"
#include "foundation/compat_thread.h"
#include "foundation/constants.h"
#include "foundation/log.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    PERCENT = 100,
    NOT_SET = -1,
    LOCK_UNINITIALIZED = 0,
    LOCK_INITIALIZING = 1,
    LOCK_READY = 2,
};

static FILE *s_out;
static atomic_int s_needs_newline;
static int s_gbuf_nodes = NOT_SET;
static int s_gbuf_edges = NOT_SET;
static cbm_mutex_t s_sink_mutex;
static atomic_int s_sink_mutex_state = ATOMIC_VAR_INIT(LOCK_UNINITIALIZED);

/* The CLI may install the sink more than once in one process (notably tests),
 * so keep one process-lifetime mutex rather than destroying it at fini. */
static void progress_sink_mutex_ensure(void) {
    int expected = LOCK_UNINITIALIZED;
    if (atomic_compare_exchange_strong_explicit(&s_sink_mutex_state, &expected, LOCK_INITIALIZING,
                                                memory_order_acq_rel, memory_order_acquire)) {
        cbm_mutex_init(&s_sink_mutex);
        atomic_store_explicit(&s_sink_mutex_state, LOCK_READY, memory_order_release);
        return;
    }
    while (atomic_load_explicit(&s_sink_mutex_state, memory_order_acquire) != LOCK_READY) {}
}

bool cbm_cli_progress_enabled(bool explicitly_requested, bool stderr_is_tty) {
    return explicitly_requested || stderr_is_tty;
}

static void progress_tool_name(const char *tool_name, char out[CBM_SZ_64]) {
    size_t offset = 0;
    if (tool_name) {
        for (; tool_name[offset] && offset + 1 < CBM_SZ_64; offset++) {
            unsigned char ch = (unsigned char)tool_name[offset];
            bool safe = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                        (ch >= '0' && ch <= '9') || ch == '_' || ch == '-';
            if (!safe) {
                offset = 0;
                break;
            }
            out[offset] = (char)ch;
        }
    }
    if (offset == 0) {
        (void)snprintf(out, CBM_SZ_64, "%s", "tool");
    } else {
        out[offset] = '\0';
    }
}

void cbm_cli_progress_start(FILE *out, const char *tool_name) {
    FILE *stream = out ? out : stderr;
    char safe_name[CBM_SZ_64] = {0};
    progress_tool_name(tool_name, safe_name);
    (void)fprintf(stream, "Running %s locally...\n", safe_name);
    (void)fflush(stream);
}

void cbm_cli_progress_finish(FILE *out, const char *tool_name, bool success, uint64_t elapsed_ms) {
    FILE *stream = out ? out : stderr;
    char safe_name[CBM_SZ_64] = {0};
    progress_tool_name(tool_name, safe_name);
    (void)fprintf(stream, "%s %s (%llu ms)\n", success ? "Completed" : "Failed", safe_name,
                  (unsigned long long)elapsed_ms);
    (void)fflush(stream);
}

/* Extract one string field from the logger's compact JSON format. Keys are
 * accepted only at object boundaries so worker-controlled values cannot spoof
 * a progress event by merely containing a key-shaped substring. */
static const char *extract_json_field(const char *line, const char *key, char *buf, int buf_len) {
    char needle[CBM_SZ_64];
    int needle_len = snprintf(needle, sizeof(needle), "\"%s\":", key);
    if (needle_len <= 0 || needle_len >= (int)sizeof(needle)) {
        return NULL;
    }
    const char *candidate = line;
    while ((candidate = strstr(candidate, needle)) != NULL) {
        const char *before = candidate;
        while (before > line && (before[-1] == ' ' || before[-1] == '\t')) {
            before--;
        }
        if (before == line || (before[-1] != '{' && before[-1] != ',')) {
            candidate += needle_len;
            continue;
        }
        const char *value = candidate + needle_len;
        while (*value == ' ' || *value == '\t') {
            value++;
        }
        if (*value++ != '\"') {
            return NULL;
        }
        int offset = 0;
        bool escaped = false;
        while (*value && offset < buf_len - 1) {
            if (!escaped && *value == '\"') {
                buf[offset] = '\0';
                return buf;
            }
            if (!escaped && *value == '\\') {
                escaped = true;
                value++;
                continue;
            }
            if (escaped) {
                switch (*value) {
                case 'n':
                    buf[offset++] = '\n';
                    break;
                case 'r':
                    buf[offset++] = '\r';
                    break;
                case 't':
                    buf[offset++] = '\t';
                    break;
                default:
                    buf[offset++] = *value;
                    break;
                }
                escaped = false;
            } else {
                buf[offset++] = *value;
            }
            value++;
        }
        return NULL;
    }
    return NULL;
}

/* Extract a field from either structured text or JSON worker logs. */
static const char *extract_kv(const char *line, const char *key, char *buf, int buf_len) {
    if (!line || !key || !buf || buf_len <= 0) {
        return NULL;
    }
    size_t klen = strlen(key);
    const char *p = line;
    while (*p) {
        if ((p == line || p[-SKIP_ONE] == ' ') && strncmp(p, key, klen) == 0 && p[klen] == '=') {
            const char *val = p + klen + SKIP_ONE;
            int i = 0;
            while (val[i] && val[i] != ' ' && i < buf_len - SKIP_ONE) {
                buf[i] = val[i];
                i++;
            }
            buf[i] = '\0';
            return buf;
        }
        p++;
    }
    const char *json_key = strcmp(key, "msg") == 0 ? "event" : key;
    return extract_json_field(line, json_key, buf, buf_len);
}

void cbm_progress_sink_init(FILE *out) {
    progress_sink_mutex_ensure();
    cbm_mutex_lock(&s_sink_mutex);
    s_out = out ? out : stderr;
    atomic_store(&s_needs_newline, 0);
    s_gbuf_nodes = NOT_SET;
    s_gbuf_edges = NOT_SET;
    cbm_log_set_sink(cbm_progress_sink_fn);
    cbm_mutex_unlock(&s_sink_mutex);
}

void cbm_progress_sink_fini(void) {
    progress_sink_mutex_ensure();
    cbm_mutex_lock(&s_sink_mutex);
    cbm_log_set_sink(NULL);
    if (atomic_load(&s_needs_newline) && s_out) {
        (void)fprintf(s_out, "\n");
        (void)fflush(s_out);
    }
    s_out = NULL;
    cbm_mutex_unlock(&s_sink_mutex);
}

/* Phase label table: maps pass names to display labels. */
typedef struct {
    const char *pass;
    const char *label;
} phase_t;

static const phase_t phases[] = {
    {"parallel_extract", "[2/9] Extracting definitions"},
    {"registry_build", "[3/9] Building registry"},
    {"parallel_resolve", "[4/9] Resolving calls & edges"},
    {"tests", "[5/9] Detecting tests"},
    {"httplinks", "[6/9] Scanning HTTP links"},
    {"githistory_compute", "[7/9] Analyzing git history"},
    {"configlink", "[8/9] Linking config files"},
    {"dump", "[9/9] Writing database"},
};

enum { PHASE_COUNT = sizeof(phases) / sizeof(phases[0]) };

/* Flush pending \r line if needed. */
static void flush_carriage(void) {
    if (atomic_load(&s_needs_newline)) {
        (void)fprintf(s_out, "\n");
        atomic_store(&s_needs_newline, 0);
    }
}

/* Handle pipeline.discover event. */
static void on_discover(const char *line) {
    char files[CBM_SZ_32] = {0};
    if (extract_kv(line, "files", files, (int)sizeof(files))) {
        (void)fprintf(s_out, "  Discovering files (%s found)\n", files);
    } else {
        (void)fprintf(s_out, "  Discovering files...\n");
    }
    (void)fflush(s_out);
}

/* Handle pipeline.route event. */
static void on_route(const char *line) {
    char val[CBM_SZ_32] = {0};
    const char *path = extract_kv(line, "path", val, (int)sizeof(val));
    if (path && strcmp(path, "incremental") == 0) {
        (void)fprintf(s_out, "  Starting incremental index\n");
    } else {
        (void)fprintf(s_out, "  Starting full index\n");
    }
    (void)fflush(s_out);
}

/* Handle pass.start event. */
static void on_pass_start(const char *line) {
    char val[CBM_SZ_64] = {0};
    const char *pass = extract_kv(line, "pass", val, (int)sizeof(val));
    if (pass && strcmp(pass, "structure") == 0) {
        (void)fprintf(s_out, "[1/9] Building file structure\n");
        (void)fflush(s_out);
    }
}

/* Handle pass.timing event. */
static void on_pass_timing(const char *line) {
    char val[CBM_SZ_64] = {0};
    const char *pass = extract_kv(line, "pass", val, (int)sizeof(val));
    if (!pass) {
        return;
    }
    flush_carriage();
    for (int i = 0; i < PHASE_COUNT; i++) {
        if (strcmp(pass, phases[i].pass) == 0) {
            (void)fprintf(s_out, "%s\n", phases[i].label);
            (void)fflush(s_out);
            return;
        }
    }
}

/* Handle gbuf.dump event — capture node/edge counts. */
static void on_gbuf_dump(const char *line) {
    char n[CBM_SZ_32] = {0};
    char e[CBM_SZ_32] = {0};
    if (extract_kv(line, "nodes", n, (int)sizeof(n))) {
        s_gbuf_nodes = (int)strtol(n, NULL, CBM_DECIMAL_BASE);
    }
    if (extract_kv(line, "edges", e, (int)sizeof(e))) {
        s_gbuf_edges = (int)strtol(e, NULL, CBM_DECIMAL_BASE);
    }
}

/* Handle pipeline.done event. */
static void on_done(const char *line) {
    flush_carriage();
    char ms[CBM_SZ_32] = {0};
    const char *elapsed = extract_kv(line, "elapsed_ms", ms, (int)sizeof(ms));
    if (s_gbuf_nodes >= 0 && s_gbuf_edges >= 0 && elapsed) {
        (void)fprintf(s_out, "Done: %d nodes, %d edges (%s ms)\n", s_gbuf_nodes, s_gbuf_edges,
                      elapsed);
    } else if (s_gbuf_nodes >= 0 && s_gbuf_edges >= 0) {
        (void)fprintf(s_out, "Done: %d nodes, %d edges\n", s_gbuf_nodes, s_gbuf_edges);
    } else {
        (void)fprintf(s_out, "Done.\n");
    }
    (void)fflush(s_out);
}

/* Handle parallel.extract.progress event — in-place counter. */
static void on_extract_progress(const char *line) {
    char done[CBM_SZ_32] = {0};
    char total[CBM_SZ_32] = {0};
    if (extract_kv(line, "done", done, (int)sizeof(done)) &&
        extract_kv(line, "total", total, (int)sizeof(total))) {
        long d = strtol(done, NULL, CBM_DECIMAL_BASE);
        long t = strtol(total, NULL, CBM_DECIMAL_BASE);
        int pct = (t > 0) ? (int)((d * PERCENT) / t) : 0;
        (void)fprintf(s_out, "\r  Extracting: %ld/%ld files (%d%%)", d, t, pct);
        (void)fflush(s_out);
        atomic_store(&s_needs_newline, SKIP_ONE);
    }
}

/* Event dispatch table. */
typedef struct {
    const char *msg;
    void (*handler)(const char *line);
} event_handler_t;

static const event_handler_t handlers[] = {
    {"pipeline.discover", on_discover},
    {"pipeline.route", on_route},
    {"pass.start", on_pass_start},
    {"pass.timing", on_pass_timing},
    {"gbuf.dump", on_gbuf_dump},
    {"pipeline.done", on_done},
    {"parallel.extract.progress", on_extract_progress},
};

enum { HANDLER_COUNT = sizeof(handlers) / sizeof(handlers[0]) };

void cbm_progress_sink_fn(const char *line) {
    progress_sink_mutex_ensure();
    cbm_mutex_lock(&s_sink_mutex);
    if (!line || !s_out) {
        cbm_mutex_unlock(&s_sink_mutex);
        return;
    }
    char msg[CBM_SZ_64] = {0};
    if (!extract_kv(line, "msg", msg, (int)sizeof(msg))) {
        cbm_mutex_unlock(&s_sink_mutex);
        return;
    }
    for (int i = 0; i < HANDLER_COUNT; i++) {
        if (strcmp(msg, handlers[i].msg) == 0) {
            handlers[i].handler(line);
            cbm_mutex_unlock(&s_sink_mutex);
            return;
        }
    }
    cbm_mutex_unlock(&s_sink_mutex);
}
