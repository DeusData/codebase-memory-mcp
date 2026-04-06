/*
 * pass_gitdiff.c — Git diff output parsing helpers.
 *
 * Pure string parsers for git diff --name-status and --unified=0 output.
 * No git execution — just parsing pre-captured output strings.
 */
#include "foundation/constants.h"

enum { GD_STATUS_IDX = 1, GD_PLUS_PREFIX = 6 };

#define SLEN(s) (sizeof(s) - 1)
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void cbm_parse_range(const char *s, int *out_start, int *out_count) {
    *out_start = 0;
    *out_count = SKIP_ONE;

    const char *comma = strchr(s, ',');
    if (!comma) {
        *out_start = (int)strtol(s, NULL, CBM_DECIMAL_BASE);
        return;
    }

    /* Parse start */
    char buf[CBM_SZ_32];
    size_t len = (size_t)(comma - s);
    if (len >= sizeof(buf)) {
        len = sizeof(buf) - SKIP_ONE;
    }
    memcpy(buf, s, len);
    buf[len] = '\0';
    *out_start = (int)strtol(buf, NULL, CBM_DECIMAL_BASE);

    /* Parse count */
    *out_count = (int)strtol(comma + SKIP_ONE, NULL, CBM_DECIMAL_BASE);
}

#define HUNK_LINE_BUF 1536

/* Parse a single name-status line into a cbm_changed_file_t.
 * Returns true if a valid entry was produced. */
static bool parse_one_name_status(const char *line, size_t line_len, cbm_changed_file_t *out_f) {
    char tmp[HUNK_LINE_BUF];
    if (line_len >= sizeof(tmp)) {
        line_len = sizeof(tmp) - SKIP_ONE;
    }
    memcpy(tmp, line, line_len);
    tmp[line_len] = '\0';

    char *status_str = tmp;
    char *tab1 = strchr(tmp, '\t');
    if (!tab1) {
        return false;
    }
    *tab1 = '\0';
    char *path1 = tab1 + SKIP_ONE;

    char *tab2 = strchr(path1, '\t');
    char *path2 = NULL;
    if (tab2) {
        *tab2 = '\0';
        path2 = tab2 + SKIP_ONE;
    }

    memset(out_f, 0, sizeof(*out_f));

    if (status_str[0] == 'R') {
        out_f->status[0] = 'R';
        out_f->status[GD_STATUS_IDX] = '\0';
        snprintf(out_f->old_path, sizeof(out_f->old_path), "%s", path1);
        snprintf(out_f->path, sizeof(out_f->path), "%s", path2 ? path2 : path1);
    } else {
        out_f->status[0] = status_str[0];
        out_f->status[GD_STATUS_IDX] = '\0';
        snprintf(out_f->path, sizeof(out_f->path), "%s", path1);
        out_f->old_path[0] = '\0';
    }

    return cbm_is_trackable_file(out_f->path);
}

int cbm_parse_name_status(const char *output, cbm_changed_file_t *out, int max_out) {
    if (!output || !out || max_out <= 0) {
        return 0;
    }

    int count = 0;
    const char *line = output;

    while (*line && count < max_out) {
        const char *eol = strchr(line, '\n');
        size_t line_len = eol ? (size_t)(eol - line) : strlen(line);

        if (line_len > 0) {
            cbm_changed_file_t f;
            if (parse_one_name_status(line, line_len, &f)) {
                out[count++] = f;
            }
        }

        line = eol ? eol + SKIP_ONE : line + line_len;
    }
    return count;
}

/* Parse a single @@ hunk header line and emit a hunk entry if valid.
 * Returns true if a hunk was added. */
static bool parse_hunk_line(const char *line, size_t line_len, const char *current_file,
                            cbm_changed_hunk_t *out_h) {
    const char *plus = memchr(line, '+', line_len);
    if (!plus || plus <= line) {
        return false;
    }

    const char *end_at = strstr(plus, " @@");
    size_t range_len;
    if (end_at) {
        range_len = (size_t)(end_at - plus - SKIP_ONE);
    } else {
        range_len = (size_t)(line + line_len - plus - SKIP_ONE);
    }

    char range_str[CBM_SZ_64];
    if (range_len >= sizeof(range_str)) {
        range_len = sizeof(range_str) - SKIP_ONE;
    }
    memcpy(range_str, plus + SKIP_ONE, range_len);
    range_str[range_len] = '\0';

    int start;
    int cnt;
    cbm_parse_range(range_str, &start, &cnt);

    if (start <= 0 || !cbm_is_trackable_file(current_file)) {
        return false;
    }

    int end = start + cnt - SKIP_ONE;
    if (end < start) {
        end = start;
    }

    snprintf(out_h->path, sizeof(out_h->path), "%s", current_file);
    out_h->start_line = start;
    out_h->end_line = end;
    return true;
}

int cbm_parse_hunks(const char *output, cbm_changed_hunk_t *out, int max_out) {
    if (!output || !out || max_out <= 0) {
        return 0;
    }

    int count = 0;
    char current_file[CBM_SZ_512] = {0};
    const char *line = output;

    while (*line && count < max_out) {
        const char *eol = strchr(line, '\n');
        size_t line_len = eol ? (size_t)(eol - line) : strlen(line);

        if (line_len == 0) {
            line = eol ? eol + SKIP_ONE : line + line_len;
            continue;
        }

        if (line_len > GD_PLUS_PREFIX && strncmp(line, "+++ b/", SLEN("+++ b/")) == 0) {
            size_t flen = line_len - GD_PLUS_PREFIX;
            if (flen >= sizeof(current_file)) {
                flen = sizeof(current_file) - SKIP_ONE;
            }
            memcpy(current_file, line + 6, flen);
            current_file[flen] = '\0';
        } else if (line_len >= PAIR_LEN && line[0] == '@' && line[GD_STATUS_IDX] == '@' &&
                   current_file[0]) {
            cbm_changed_hunk_t h;
            if (parse_hunk_line(line, line_len, current_file, &h)) {
                out[count++] = h;
            }
        }

        line = eol ? eol + SKIP_ONE : line + line_len;
    }
    return count;
}
