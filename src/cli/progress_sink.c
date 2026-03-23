/*
 * progress_sink.c — Human-readable progress sink for the --progress CLI flag.
 *
 * Parses structured log lines (format: "level=info msg=TAG key=val ...") and
 * maps known pipeline event tags to human-readable phase labels on stderr.
 *
 * Thread safety: fprintf is thread-safe on POSIX via per-FILE* internal
 * locking (flockfile/funlockfile). Individual fprintf calls will not
 * interleave even when called from parallel worker threads.
 * The \r progress lines for parallel.extract.progress do not use a newline
 * (in-place update), so they rely on the terminal rendering.
 */
#include "progress_sink.h"
#include "../foundation/log.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* ── Module state ─────────────────────────────────────────────── */

static FILE *s_out = NULL;                 /* target stream (stderr) */
static cbm_log_sink_fn s_prev_sink = NULL; /* restored by _fini */
/* Set to 1 after a \r line is emitted so _fini can flush a trailing \n.
 * Written by parallel worker threads, read by the orchestration thread —
 * declare volatile to prevent the compiler from caching the value. */
static volatile int s_needs_newline = 0;
/* Node/edge counts captured from gbuf.dump (before node_by_qn is freed).
 * pipeline.done arrives after the QN table is freed so its nodes= is 0. */
static int s_gbuf_nodes = -1;
static int s_gbuf_edges = -1;

/* ── Internal helpers ─────────────────────────────────────────── */

/*
 * Extract the value of the first occurrence of "key=VALUE" in `line`.
 * VALUE ends at the next space or end-of-string.
 * Writes at most (buf_len-1) chars into buf and NUL-terminates.
 * Returns buf, or NULL if the key was not found.
 */
static const char *extract_kv(const char *line, const char *key, char *buf, int buf_len) {
    if (!line || !key || !buf || buf_len <= 0) {
        return NULL;
    }

    size_t klen = strlen(key);
    const char *p = line;
    while (*p) {
        /* Look for " key=" or start-of-string "key=" */
        if ((p == line || p[-1] == ' ') && strncmp(p, key, klen) == 0 && p[klen] == '=') {
            const char *val = p + klen + 1;
            int i = 0;
            while (val[i] && val[i] != ' ' && i < buf_len - 1) {
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

/* ── Public API ───────────────────────────────────────────────── */

void cbm_progress_sink_init(FILE *out) {
    s_out = out ? out : stderr;
    s_needs_newline = 0;
    s_gbuf_nodes = -1;
    s_gbuf_edges = -1;
    /* Save and replace the current sink. */
    s_prev_sink = NULL; /* cbm_log_set_sink does not expose get; we shadow it */
    cbm_log_set_sink(cbm_progress_sink_fn);
}

void cbm_progress_sink_fini(void) {
    if (s_needs_newline && s_out) {
        /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling) */
        (void)fprintf(s_out, "\n");
        (void)fflush(s_out);
        s_needs_newline = 0;
    }
    /* Restore previous sink (NULL → disable, which is fine for CLI). */
    cbm_log_set_sink(s_prev_sink);
    s_out = NULL;
}

/*
 * cbm_progress_sink_fn — the log-sink callback.
 *
 * Called with each formatted log line, e.g.:
 *   "level=info msg=pass.timing pass=parallel_extract elapsed_ms=1234"
 *
 * We extract msg= to identify the event, then extract additional keys to
 * build the human-readable label.  Unknown tags are passed to s_prev_sink
 * (pass-through) so existing MCP UI routing is not broken.
 */
void cbm_progress_sink_fn(const char *line) {
    if (!line || !s_out) {
        return;
    }

    char msg[64] = {0};
    char val[128] = {0};

    if (!extract_kv(line, "msg", msg, (int)sizeof(msg))) {
        /* No msg= tag — pass through. */
        if (s_prev_sink) {
            s_prev_sink(line);
        }
        return;
    }

    /* ── pipeline.discover ─────────────────────────────────────── */
    if (strcmp(msg, "pipeline.discover") == 0) {
        char files_buf[32] = {0};
        const char *files = extract_kv(line, "files", files_buf, (int)sizeof(files_buf));
        if (files) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "  Discovering files (%s found)\n", files);
        } else {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "  Discovering files...\n");
        }
        (void)fflush(s_out);
        return;
    }

    /* ── pipeline.route ────────────────────────────────────────── */
    if (strcmp(msg, "pipeline.route") == 0) {
        const char *path = extract_kv(line, "path", val, (int)sizeof(val));
        if (path && strcmp(path, "incremental") == 0) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "  Starting incremental index\n");
        } else {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "  Starting full index\n");
        }
        (void)fflush(s_out);
        return;
    }

    /* ── pass.start ────────────────────────────────────────────── */
    if (strcmp(msg, "pass.start") == 0) {
        const char *pass = extract_kv(line, "pass", val, (int)sizeof(val));
        if (pass && strcmp(pass, "structure") == 0) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "[1/9] Building file structure\n");
            (void)fflush(s_out);
        }
        /* Other pass.start events are silently skipped (pass.timing carries timing). */
        return;
    }

    /* ── pass.timing ───────────────────────────────────────────── */
    if (strcmp(msg, "pass.timing") == 0) {
        const char *pass = extract_kv(line, "pass", val, (int)sizeof(val));
        if (!pass) {
            return;
        }

        if (strcmp(pass, "parallel_extract") == 0) {
            /* Finish the \r in-place line with a proper newline first. */
            if (s_needs_newline) {
                /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
                 */
                (void)fprintf(s_out, "\n");
                s_needs_newline = 0;
            }
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "[2/9] Extracting definitions\n");
        } else if (strcmp(pass, "registry_build") == 0) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "[3/9] Building registry\n");
        } else if (strcmp(pass, "parallel_resolve") == 0) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "[4/9] Resolving calls & edges\n");
        } else if (strcmp(pass, "tests") == 0) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "[5/9] Detecting tests\n");
        } else if (strcmp(pass, "httplinks") == 0) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "[6/9] Scanning HTTP links\n");
        } else if (strcmp(pass, "githistory_compute") == 0) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "[7/9] Analyzing git history\n");
        } else if (strcmp(pass, "configlink") == 0) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "[8/9] Linking config files\n");
        } else if (strcmp(pass, "dump") == 0) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "[9/9] Writing database\n");
        }
        /* k8s, decorator_tags, persist_hashes, and other passes: silently skip. */
        (void)fflush(s_out);
        return;
    }

    /* ── gbuf.dump — capture accurate node/edge counts ────────── */
    if (strcmp(msg, "gbuf.dump") == 0) {
        char n_buf[32] = {0};
        char e_buf[32] = {0};
        if (extract_kv(line, "nodes", n_buf, (int)sizeof(n_buf))) {
            s_gbuf_nodes = (int)strtol(n_buf, NULL, 10);
        }
        if (extract_kv(line, "edges", e_buf, (int)sizeof(e_buf))) {
            s_gbuf_edges = (int)strtol(e_buf, NULL, 10);
        }
        return;
    }

    /* ── pipeline.done ─────────────────────────────────────────── */
    if (strcmp(msg, "pipeline.done") == 0) {
        if (s_needs_newline) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "\n");
            s_needs_newline = 0;
        }
        char ms_buf[32] = {0};
        const char *elapsed = extract_kv(line, "elapsed_ms", ms_buf, (int)sizeof(ms_buf));
        /* Use counts from gbuf.dump (fired before node_by_qn is freed).
         * pipeline.done's own nodes= field is always 0 after the QN table free. */
        if (s_gbuf_nodes >= 0 && s_gbuf_edges >= 0 && elapsed) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "Done: %d nodes, %d edges (%s ms)\n", s_gbuf_nodes, s_gbuf_edges,
                          elapsed);
        } else if (s_gbuf_nodes >= 0 && s_gbuf_edges >= 0) {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "Done: %d nodes, %d edges\n", s_gbuf_nodes, s_gbuf_edges);
        } else {
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "Done.\n");
        }
        (void)fflush(s_out);
        return;
    }

    /* ── parallel.extract.progress ─────────────────────────────── */
    if (strcmp(msg, "parallel.extract.progress") == 0) {
        char done_buf[32] = {0};
        char total_buf[32] = {0};
        const char *done = extract_kv(line, "done", done_buf, (int)sizeof(done_buf));
        const char *total = extract_kv(line, "total", total_buf, (int)sizeof(total_buf));
        if (done && total) {
            long d = strtol(done, NULL, 10);
            long t = strtol(total, NULL, 10);
            int pct = (t > 0) ? (int)((d * 100L) / t) : 0;
            /* \r writes in-place on the current terminal line (no newline). */
            /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
             */
            (void)fprintf(s_out, "\r  Extracting: %ld/%ld files (%d%%)", d, t, pct);
            (void)fflush(s_out);
            s_needs_newline = 1;
        }
        return;
    }

    /* ── Unknown tag — pass through to previous sink (if any) ─── */
    if (s_prev_sink) {
        s_prev_sink(line);
    }
    /* Otherwise silently discard (don't print raw log lines to stderr). */
}
