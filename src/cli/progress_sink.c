/*
 * progress_sink.c — Human-readable progress for --progress CLI flag.
 *
 * Maps structured log events (msg=pass.timing, msg=pipeline.done, etc.)
 * to phase labels on stderr. When installed, replaces default log output.
 *
 * Thread-safe: fprintf has per-FILE* locking on POSIX.
 */
#include "progress_sink.h"
#include "foundation/constants.h"
#include "foundation/log.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { PERCENT = 100, NOT_SET = -1 };

static FILE *s_out;
static atomic_int s_needs_newline;
static int s_gbuf_nodes = NOT_SET;
static int s_gbuf_edges = NOT_SET;

/* Extract value of "key=VALUE" from a structured log line. */
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
    return NULL;
}

void cbm_progress_sink_init(FILE *out) {
    s_out = out ? out : stderr;
    atomic_store(&s_needs_newline, 0);
    s_gbuf_nodes = NOT_SET;
    s_gbuf_edges = NOT_SET;
    cbm_log_set_sink(cbm_progress_sink_fn);
}

void cbm_progress_sink_fini(void) {
    if (atomic_load(&s_needs_newline) && s_out) {
        (void)fprintf(s_out, "\n");
        (void)fflush(s_out);
    }
    cbm_log_set_sink(NULL);
    s_out = NULL;
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
    if (!line || !s_out) {
        return;
    }
    char msg[CBM_SZ_64] = {0};
    if (!extract_kv(line, "msg", msg, (int)sizeof(msg))) {
        return;
    }
    for (int i = 0; i < HANDLER_COUNT; i++) {
        if (strcmp(msg, handlers[i].msg) == 0) {
            handlers[i].handler(line);
            return;
        }
    }
}
