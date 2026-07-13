/*
 * log.c — Structured key-value logging to stderr.
 */
#include "log.h"
#include "foundation/constants.h"
#include <ctype.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static CBMLogLevel g_log_level = CBM_LOG_INFO;
static CBMLogFormat g_log_format = CBM_LOG_FORMAT_TEXT;
static cbm_log_sink_fn g_log_sink = NULL;
static CBMLogSinkMode g_log_sink_mode = CBM_LOG_SINK_REPLACE;

/* CBM_LOG_LEVEL support — distilled from #414 (closes #413, thanks @santanusinha). */
void cbm_log_init_from_env(void) {
    /* getenv() is safe here: this runs at startup before any thread is created,
     * so there is no concurrent setenv() to race against. */
    const char *raw = getenv("CBM_LOG_LEVEL");
    if (raw && raw[0] != '\0') {
        /* Textual form, case-insensitive. Index of each name == its enum value. */
        static const char *const names[] = {"debug", "info", "warn", "error", "none"};
        char lower[8];
        size_t i = 0;
        for (; i < sizeof(lower) - 1 && raw[i] != '\0'; i++) {
            lower[i] = (char)tolower((unsigned char)raw[i]);
        }
        lower[i] = '\0';
        if (raw[i] == '\0') { /* fully consumed — candidate textual match */
            for (size_t lvl = 0; lvl < sizeof(names) / sizeof(names[0]); lvl++) {
                if (strcmp(lower, names[lvl]) == 0) {
                    cbm_log_set_level((CBMLogLevel)lvl);
                    goto parse_format;
                }
            }
        }

        /* Numeric form: 0=debug .. 4=none, matching CBMLogLevel. */
        char *end = NULL;
        long n = strtol(raw, &end, CBM_DECIMAL_BASE);
        if (end != raw && *end == '\0' && n >= CBM_LOG_DEBUG && n <= CBM_LOG_NONE) {
            cbm_log_set_level((CBMLogLevel)n);
        }
    }

    /* Unrecognised value: leave the level unchanged (fail-open). */

parse_format:;
    const char *fmt = getenv("CBM_LOG_FORMAT");
    if (fmt && fmt[0] != '\0') {
        char lower_fmt[8];
        size_t i = 0;
        for (; i < sizeof(lower_fmt) - 1 && fmt[i] != '\0'; i++) {
            lower_fmt[i] = (char)tolower((unsigned char)fmt[i]);
        }
        lower_fmt[i] = '\0';
        if (fmt[i] == '\0' && strcmp(lower_fmt, "json") == 0) {
            cbm_log_set_format(CBM_LOG_FORMAT_JSON);
        } else if (fmt[i] == '\0' && strcmp(lower_fmt, "text") == 0) {
            cbm_log_set_format(CBM_LOG_FORMAT_TEXT);
        }
        return;
    }

    /* Format is intentionally explicit-only. Logs stay local to stderr and the
     * optional in-process sink; deployment environment variables must not
     * silently change the operator-selected output shape. */
}

void cbm_log_set_sink(cbm_log_sink_fn fn) {
    cbm_log_set_sink_ex(fn, CBM_LOG_SINK_REPLACE);
}

void cbm_log_set_sink_ex(cbm_log_sink_fn fn, CBMLogSinkMode mode) {
    g_log_sink = fn;
    g_log_sink_mode = mode;
}

void cbm_log_set_level(CBMLogLevel level) {
    g_log_level = level;
}

CBMLogLevel cbm_log_get_level(void) {
    return g_log_level;
}

void cbm_log_set_format(CBMLogFormat format) {
    g_log_format = format;
}

CBMLogFormat cbm_log_get_format(void) {
    return g_log_format;
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

static void append_char(char *buf, size_t bufsz, size_t *pos, char ch) {
    if (*pos < bufsz - 1) {
        buf[*pos] = ch;
    }
    (*pos)++;
}

static void append_raw(char *buf, size_t bufsz, size_t *pos, const char *s) {
    if (!s) {
        return;
    }
    while (*s) {
        append_char(buf, bufsz, pos, *s++);
    }
}

static void append_text_atom(char *buf, size_t bufsz, size_t *pos, const char *s) {
    if (!s) {
        return;
    }
    while (*s) {
        unsigned char ch = (unsigned char)*s++;
        if (ch <= ' ' || ch == 0x7f) {
            append_char(buf, bufsz, pos, '_');
        } else {
            append_char(buf, bufsz, pos, (char)ch);
        }
    }
}

static void append_json_string(char *buf, size_t bufsz, size_t *pos, const char *s) {
    append_char(buf, bufsz, pos, '"');
    if (s) {
        while (*s) {
            unsigned char ch = (unsigned char)*s++;
            switch (ch) {
            case '"':
                append_raw(buf, bufsz, pos, "\\\"");
                break;
            case '\\':
                append_raw(buf, bufsz, pos, "\\\\");
                break;
            case '\b':
                append_raw(buf, bufsz, pos, "\\b");
                break;
            case '\f':
                append_raw(buf, bufsz, pos, "\\f");
                break;
            case '\n':
                append_raw(buf, bufsz, pos, "\\n");
                break;
            case '\r':
                append_raw(buf, bufsz, pos, "\\r");
                break;
            case '\t':
                append_raw(buf, bufsz, pos, "\\t");
                break;
            default:
                if (ch < 0x20) {
                    static const char hex[] = "0123456789abcdef";
                    append_raw(buf, bufsz, pos, "\\u00");
                    append_char(buf, bufsz, pos, hex[ch >> 4]);
                    append_char(buf, bufsz, pos, hex[ch & 0xf]);
                } else {
                    append_char(buf, bufsz, pos, (char)ch);
                }
                break;
            }
        }
    }
    append_char(buf, bufsz, pos, '"');
}

static void finish_line(char *buf, size_t bufsz, size_t pos) {
    if (bufsz == 0) {
        return;
    }
    if (pos >= bufsz) {
        buf[bufsz - 1] = '\0';
    } else {
        buf[pos] = '\0';
    }
}

static void emit_line(const char *line) {
    if (g_log_sink) {
        g_log_sink(line);
        if (g_log_sink_mode == CBM_LOG_SINK_REPLACE) {
            return;
        }
    }
    (void)fprintf(stderr, "%s\n", line);
}

void cbm_log(CBMLogLevel level, const char *msg, ...) {
    if (level < g_log_level) {
        return;
    }

    char line_buf[CBM_SZ_4K];
    size_t pos = 0;
    va_list args;
    va_start(args, msg);

    if (g_log_format == CBM_LOG_FORMAT_JSON) {
        append_raw(line_buf, sizeof(line_buf), &pos, "{\"level\":");
        append_json_string(line_buf, sizeof(line_buf), &pos, level_str(level));
        append_raw(line_buf, sizeof(line_buf), &pos, ",\"event\":");
        append_json_string(line_buf, sizeof(line_buf), &pos, msg ? msg : "");
        for (;;) {
            const char *key = va_arg(args, const char *);
            if (!key) {
                break;
            }
            const char *val = va_arg(args, const char *);
            append_char(line_buf, sizeof(line_buf), &pos, ',');
            append_json_string(line_buf, sizeof(line_buf), &pos, key);
            append_char(line_buf, sizeof(line_buf), &pos, ':');
            append_json_string(line_buf, sizeof(line_buf), &pos, val ? val : "");
        }
        append_char(line_buf, sizeof(line_buf), &pos, '}');
    } else {
        append_raw(line_buf, sizeof(line_buf), &pos, "level=");
        append_text_atom(line_buf, sizeof(line_buf), &pos, level_str(level));
        append_raw(line_buf, sizeof(line_buf), &pos, " msg=");
        append_text_atom(line_buf, sizeof(line_buf), &pos, msg ? msg : "");
        for (;;) {
            const char *key = va_arg(args, const char *);
            if (!key) {
                break;
            }
            const char *val = va_arg(args, const char *);
            append_char(line_buf, sizeof(line_buf), &pos, ' ');
            append_text_atom(line_buf, sizeof(line_buf), &pos, key);
            append_char(line_buf, sizeof(line_buf), &pos, '=');
            append_text_atom(line_buf, sizeof(line_buf), &pos, val ? val : "");
        }
    }
    va_end(args);

    finish_line(line_buf, sizeof(line_buf), pos);
    emit_line(line_buf);
}

void cbm_log_int(CBMLogLevel level, const char *msg, const char *key, int64_t value) {
    char value_buf[CBM_SZ_32];
    snprintf(value_buf, sizeof(value_buf), "%" PRId64, value);
    cbm_log(level, msg, key ? key : "?", value_buf, NULL);
}

static void copy_path_without_query(const char *path, char *out, size_t outsz) {
    if (!out || outsz == 0) {
        return;
    }
    out[0] = '\0';
    if (!path) {
        return;
    }
    size_t n = 0;
    while (path[n] && path[n] != '?' && path[n] != '#' && n < outsz - 1) {
        out[n] = path[n];
        n++;
    }
    out[n] = '\0';
}

void cbm_log_mcp_request(const char *method, const char *tool_name, bool is_error,
                         int64_t duration_us) {
    char duration_ms[CBM_SZ_32];
    snprintf(duration_ms, sizeof(duration_ms), "%" PRId64, duration_us / 1000);
    if (tool_name && tool_name[0] != '\0') {
        cbm_log(is_error ? CBM_LOG_WARN : CBM_LOG_INFO, "mcp.request", "protocol", "jsonrpc",
                "method", method ? method : "", "tool", tool_name, "status",
                is_error ? "error" : "ok", "duration_ms", duration_ms, NULL);
    } else {
        cbm_log(is_error ? CBM_LOG_WARN : CBM_LOG_INFO, "mcp.request", "protocol", "jsonrpc",
                "method", method ? method : "", "status", is_error ? "error" : "ok", "duration_ms",
                duration_ms, NULL);
    }
}

void cbm_log_http_request(const char *component, const char *method, const char *path, int status,
                          int64_t duration_ms, size_t request_bytes, size_t response_bytes) {
    char safe_path[CBM_SZ_1K];
    char status_buf[CBM_SZ_16];
    char duration_buf[CBM_SZ_32];
    char request_buf[CBM_SZ_32];
    char response_buf[CBM_SZ_32];
    copy_path_without_query(path, safe_path, sizeof(safe_path));
    snprintf(status_buf, sizeof(status_buf), "%d", status);
    snprintf(duration_buf, sizeof(duration_buf), "%" PRId64, duration_ms);
    snprintf(request_buf, sizeof(request_buf), "%zu", request_bytes);
    snprintf(response_buf, sizeof(response_buf), "%zu", response_bytes);

    CBMLogLevel level = CBM_LOG_INFO;
    if (status >= 500) {
        level = CBM_LOG_ERROR;
    } else if (status >= 400) {
        level = CBM_LOG_WARN;
    }

    cbm_log(level, "http.request", "component", component ? component : "", "method",
            method ? method : "", "path", safe_path, "status", status_buf, "duration_ms",
            duration_buf, "request_bytes", request_buf, "response_bytes", response_buf, NULL);
}
