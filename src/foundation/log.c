/*
 * log.c — Structured key-value logging to stderr.
 */
#include "log.h"
#include "foundation/constants.h"
#include <inttypes.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static CBMLogLevel g_log_level = CBM_LOG_INFO;
static cbm_log_sink_fn g_log_sink = NULL;

void cbm_log_init_from_env(void) {
    /* Read CBM_LOG_LEVEL — use getenv() directly: this runs before any threads
     * are created, so there is no concurrent setenv() race to worry about. */
    const char *val = getenv("CBM_LOG_LEVEL");
    if (!val || val[0] == '\0') {
        return;
    }

    /* Case-insensitive comparison via tolower on first char + strcmp */
    char lo[16];
    size_t i = 0;
    while (i < sizeof(lo) - 1 && val[i] != '\0') {
        char c = val[i];
        if (c >= 'A' && c <= 'Z') {
            c = (char)(c + ('a' - 'A'));
        }
        lo[i] = c;
        i++;
    }
    lo[i] = '\0';

    if      (strcmp(lo, "debug") == 0) { cbm_log_set_level(CBM_LOG_DEBUG); }
    else if (strcmp(lo, "info")  == 0) { cbm_log_set_level(CBM_LOG_INFO);  }
    else if (strcmp(lo, "warn")  == 0) { cbm_log_set_level(CBM_LOG_WARN);  }
    else if (strcmp(lo, "error") == 0) { cbm_log_set_level(CBM_LOG_ERROR); }
    else if (strcmp(lo, "none")  == 0) { cbm_log_set_level(CBM_LOG_NONE);  }
    else {
        /* Try numeric: 0=debug 1=info 2=warn 3=error 4=none */
        char *end;
        long n = strtol(val, &end, 10);
        if (end != val && *end == '\0' && n >= CBM_LOG_DEBUG && n <= CBM_LOG_NONE) {
            cbm_log_set_level((CBMLogLevel)n);
        }
        /* Unknown value: leave level unchanged (fail-open). */
    }
}

void cbm_log_set_sink(cbm_log_sink_fn fn) {
    g_log_sink = fn;
}

void cbm_log_set_level(CBMLogLevel level) {
    g_log_level = level;
}

CBMLogLevel cbm_log_get_level(void) {
    return g_log_level;
}

static const char *level_str(CBMLogLevel level) {
    switch (level) {
    case CBM_LOG_DEBUG:
        return "debug";
    case CBM_LOG_INFO:
        return "info";
    case CBM_LOG_WARN:
        return "warn";
    case CBM_LOG_ERROR:
        return "error";
    default:
        return "unknown";
    }
}

void cbm_log(CBMLogLevel level, const char *msg, ...) {
    if (level < g_log_level) {
        return;
    }

    /* Build the log line into a buffer ONCE — no double va_list iteration */
    char line_buf[CBM_SZ_512];
    int pos =
        snprintf(line_buf, sizeof(line_buf), "level=%s msg=%s", level_str(level), msg ? msg : "");

    va_list args;
    va_start(args, msg);
    for (;;) {
        const char *key = va_arg(args, const char *);
        if (!key) {
            break;
        }
        const char *val = va_arg(args, const char *);
        if (!val) {
            val = "";
        }
        if ((size_t)pos < sizeof(line_buf) - SKIP_ONE) {
            pos += snprintf(line_buf + pos, sizeof(line_buf) - (size_t)pos, " %s=%s", key, val);
        }
    }
    va_end(args);

    /* When a sink is registered it takes over all output (exclusive).
     * Otherwise write structured log to stderr. */
    if (g_log_sink) {
        g_log_sink(line_buf);
    } else {
        (void)fprintf(stderr, "%s\n", line_buf);
    }
}

void cbm_log_int(CBMLogLevel level, const char *msg, const char *key, int64_t value) {
    if (level < g_log_level) {
        return;
    }

    char line_buf[CBM_SZ_256];
    snprintf(line_buf, sizeof(line_buf), "level=%s msg=%s %s=%" PRId64, level_str(level),
             msg ? msg : "", key ? key : "?", value);

    if (g_log_sink) {
        g_log_sink(line_buf);
    } else {
        (void)fprintf(stderr, "%s\n", line_buf);
    }
}
