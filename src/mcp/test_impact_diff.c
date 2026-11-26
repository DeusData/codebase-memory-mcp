/*
 * test_impact_diff.c — the change the test-impact engine selects for: a
 * zero-context unified diff, read into files and hunks (test_impact.h, part 2).
 *
 * The reader trusts the line counts of each hunk header, not the look of a
 * line: a removed line whose text is "-- a/x" arrives as "--- a/x", exactly
 * like a file header. Whatever it cannot account for makes the diff
 * incomplete, and an incomplete diff narrows nothing.
 */
#include "mcp/test_impact.h"

#include "foundation/arena.h"

#include <stdlib.h>
#include <string.h>

enum {
    DF_ARENA_BLOCK = 64 * 1024,
    DF_FIRST_CAP = 16,
    DF_GROWTH = 2,
    DF_DECIMAL = 10,
    DF_LINE_MAX = 1000000000, /* a line number beyond this is not one */
};

struct cbm_diff {
    CBMArena arena; /* owns the files, hunks and every line */
    cbm_diff_file_t *files;
    int file_count;
    int file_cap;
    bool complete;
};

typedef struct {
    const char *p; /* start of the next unread line */
    const char *end;
} df_reader_t;

typedef struct {
    const char *text;
    size_t len; /* without the line terminator */
} df_line_t;

static bool df_next_line(df_reader_t *rd, df_line_t *line) {
    if (rd->p >= rd->end) {
        return false;
    }
    const char *nl = memchr(rd->p, '\n', (size_t)(rd->end - rd->p));
    line->text = rd->p;
    line->len = nl ? (size_t)(nl - rd->p) : (size_t)(rd->end - rd->p);
    rd->p = nl ? nl + 1 : rd->end;
    return true;
}

static bool df_starts(const df_line_t *line, const char *prefix) {
    size_t n = strlen(prefix);
    return line->len >= n && memcmp(line->text, prefix, n) == 0;
}

static bool df_is(const df_line_t *line, const char *text) {
    size_t n = strlen(text);
    return line->len == n && memcmp(line->text, text, n) == 0;
}

/* A decimal number at *p, bounded by end. false when there is none or it is
 * out of range. */
static bool df_number(const char **p, const char *end, int *out) {
    const char *q = *p;
    long value = 0;
    while (q < end && *q >= '0' && *q <= '9') {
        value = value * DF_DECIMAL + (*q - '0');
        if (value > DF_LINE_MAX) {
            return false;
        }
        q++;
    }
    if (q == *p) {
        return false;
    }
    *p = q;
    *out = (int)value;
    return true;
}

/* "start[,count]" after a '-' or '+' in a hunk header. No count means 1. */
static bool df_range(const char **p, const char *end, char sign, int *start, int *count) {
    if (*p >= end || **p != sign) {
        return false;
    }
    (*p)++;
    if (!df_number(p, end, start)) {
        return false;
    }
    *count = 1;
    if (*p < end && **p == ',') {
        (*p)++;
        return df_number(p, end, count);
    }
    return true;
}

/* "@@ -a[,b] +c[,d] @@": the old and new line ranges. */
static bool df_hunk_header(const df_line_t *line, int *old_count, int *new_start, int *new_count) {
    static const char open[] = "@@ ";
    const char *p = line->text + (sizeof(open) - 1);
    const char *end = line->text + line->len;
    int old_start = 0;
    if (!df_starts(line, open) || !df_range(&p, end, '-', &old_start, old_count)) {
        return false;
    }
    if (p >= end || *p != ' ') {
        return false;
    }
    p++;
    if (!df_range(&p, end, '+', new_start, new_count)) {
        return false;
    }
    return (size_t)(end - p) >= 3 && memcmp(p, " @@", 3) == 0;
}

/* The path of "diff --git a/P b/P". Without renames both sides name the same
 * file, so the line is the path twice; that also reads a path with " b/" in
 * it. NULL for anything else: git quotes a path with a quote, a backslash or
 * a control character in it, and that form is not read. */
static const char *df_header_path(cbm_diff_t *d, const df_line_t *line) {
    static const char open[] = "diff --git a/";
    static const char mid[] = " b/";
    size_t open_len = sizeof(open) - 1;
    size_t mid_len = sizeof(mid) - 1;
    if (!df_starts(line, open)) {
        return NULL;
    }
    size_t rest = line->len - open_len;
    if (rest < mid_len + 2 || (rest - mid_len) % 2 != 0) {
        return NULL;
    }
    size_t path_len = (rest - mid_len) / 2;
    const char *left = line->text + open_len;
    const char *right = left + path_len + mid_len;
    if (memcmp(left + path_len, mid, mid_len) != 0 || memcmp(left, right, path_len) != 0) {
        return NULL;
    }
    return cbm_arena_strndup(&d->arena, left, path_len);
}

static cbm_diff_file_t *df_add_file(cbm_diff_t *d, const char *path) {
    if (d->file_count == d->file_cap) {
        int cap = d->file_cap > 0 ? d->file_cap * DF_GROWTH : DF_FIRST_CAP;
        cbm_diff_file_t *grown = cbm_arena_alloc(&d->arena, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return NULL;
        }
        if (d->file_count > 0) {
            memcpy(grown, d->files, (size_t)d->file_count * sizeof(*grown));
        }
        d->files = grown;
        d->file_cap = cap;
    }
    cbm_diff_file_t *file = &d->files[d->file_count++];
    *file = (cbm_diff_file_t){.path = path};
    return file;
}

/* The hunks of the file being read; handed to the file when it ends. */
typedef struct {
    cbm_diff_hunk_t *items;
    int count;
    int cap;
} df_hunks_t;

static cbm_diff_hunk_t *df_add_hunk(cbm_diff_t *d, df_hunks_t *hunks) {
    if (hunks->count == hunks->cap) {
        int cap = hunks->cap > 0 ? hunks->cap * DF_GROWTH : DF_FIRST_CAP;
        cbm_diff_hunk_t *grown = cbm_arena_alloc(&d->arena, (size_t)cap * sizeof(*grown));
        if (!grown) {
            return NULL;
        }
        if (hunks->count > 0) {
            memcpy(grown, hunks->items, (size_t)hunks->count * sizeof(*grown));
        }
        hunks->items = grown;
        hunks->cap = cap;
    }
    cbm_diff_hunk_t *hunk = &hunks->items[hunks->count++];
    memset(hunk, 0, sizeof(*hunk));
    return hunk;
}

typedef enum { DF_OK = 0, DF_SHORT, DF_NO_MEMORY } df_status_t;

/* The body of one hunk: `old_count` removed lines and `new_count` added ones,
 * in the counts the header gave. A "\ No newline at end of file" note may
 * follow any of them. DF_SHORT when the text holds fewer; the reader then
 * stands on the line that did not fit. */
static df_status_t df_hunk_body(cbm_diff_t *d, df_reader_t *rd, cbm_diff_hunk_t *hunk,
                                int old_count, int new_count) {
    /* Every line takes at least its marker and a terminator, so a count the
     * rest of the text cannot hold is not worth an allocation. */
    size_t room = (size_t)(rd->end - rd->p);
    if ((size_t)old_count + (size_t)new_count > room) {
        return DF_SHORT;
    }
    const char **removed = cbm_arena_alloc(&d->arena, ((size_t)old_count + 1) * sizeof(*removed));
    const char **added = cbm_arena_alloc(&d->arena, ((size_t)new_count + 1) * sizeof(*added));
    if (!removed || !added) {
        return DF_NO_MEMORY;
    }
    hunk->removed = removed;
    hunk->added = added;
    while (hunk->removed_count < old_count || hunk->added_count < new_count) {
        df_reader_t at = *rd;
        df_line_t line;
        if (!df_next_line(rd, &line)) {
            return DF_SHORT;
        }
        if (line.len > 0 && line.text[0] == '\\') {
            continue;
        }
        bool is_removed = hunk->removed_count < old_count && line.len > 0 && line.text[0] == '-';
        bool is_added =
            !is_removed && hunk->removed_count == old_count && line.len > 0 && line.text[0] == '+';
        if (!is_removed && !is_added) {
            *rd = at;
            return DF_SHORT;
        }
        const char *copy = cbm_arena_strndup(&d->arena, line.text + 1, line.len - 1);
        if (!copy) {
            return DF_NO_MEMORY;
        }
        if (is_removed) {
            removed[hunk->removed_count++] = copy;
        } else {
            added[hunk->added_count++] = copy;
        }
    }
    /* The note about a missing final newline can also come last. */
    df_reader_t at = *rd;
    df_line_t line;
    if (df_next_line(rd, &line) && !(line.len > 0 && line.text[0] == '\\')) {
        *rd = at;
    }
    return DF_OK;
}

/* A line of a file's header, before its first hunk. false = not one the reader
 * knows. */
static bool df_file_header_line(const df_line_t *line, cbm_diff_file_t *file) {
    if (df_starts(line, "new file mode ")) {
        file->created = true;
    } else if (df_starts(line, "deleted file mode ")) {
        file->deleted = true;
    } else if (df_starts(line, "Binary files ") || df_is(line, "GIT binary patch")) {
        file->binary = true;
    } else if (!df_starts(line, "index ") && !df_starts(line, "old mode ") &&
               !df_starts(line, "new mode ") && !df_starts(line, "--- ") &&
               !df_starts(line, "+++ ")) {
        return false;
    }
    return true;
}

static int df_cmp_file(const void *left, const void *right) {
    const cbm_diff_file_t *a = left;
    const cbm_diff_file_t *b = right;
    return strcmp(a->path, b->path);
}

/* false = out of memory. */
static bool df_read(cbm_diff_t *d, df_reader_t *rd) {
    cbm_diff_file_t *file = NULL;
    df_hunks_t hunks = {0};
    bool in_hunks = false; /* past the file's header lines */
    df_line_t line;
    while (df_next_line(rd, &line)) {
        if (df_starts(&line, "diff ")) {
            if (file) {
                file->hunks = hunks.items;
                file->hunk_count = hunks.count;
            }
            hunks = (df_hunks_t){0};
            in_hunks = false;
            file = NULL;
            const char *path = df_header_path(d, &line);
            if (!path) {
                d->complete = false; /* a header this reader does not read */
                continue;
            }
            file = df_add_file(d, path);
            if (!file) {
                return false;
            }
            continue;
        }
        int old_count = 0;
        int new_start = 0;
        int new_count = 0;
        if (file && df_hunk_header(&line, &old_count, &new_start, &new_count)) {
            in_hunks = true;
            cbm_diff_hunk_t *hunk = df_add_hunk(d, &hunks);
            if (!hunk) {
                return false;
            }
            hunk->start = new_start;
            hunk->count = new_count;
            df_status_t status = df_hunk_body(d, rd, hunk, old_count, new_count);
            if (status == DF_NO_MEMORY) {
                return false;
            }
            d->complete = d->complete && status == DF_OK;
            continue;
        }
        if (line.len == 0 && !file) {
            continue; /* blank lines around the diff */
        }
        if (!file || in_hunks || !df_file_header_line(&line, file)) {
            d->complete = false;
        }
    }
    if (file) {
        file->hunks = hunks.items;
        file->hunk_count = hunks.count;
    }
    return true;
}

cbm_diff_t *cbm_diff_parse(const char *text, size_t len) {
    if (!text && len > 0) {
        return NULL;
    }
    CBMArena arena;
    cbm_arena_init_sized(&arena, DF_ARENA_BLOCK);
    cbm_diff_t *d = cbm_arena_calloc(&arena, sizeof(*d));
    if (!d) {
        cbm_arena_destroy(&arena);
        return NULL;
    }
    d->arena = arena; /* the diff lives in its own arena */
    d->complete = true;
    df_reader_t rd = {.p = text, .end = text ? text + len : text};
    if (!df_read(d, &rd)) {
        cbm_diff_free(d);
        return NULL;
    }
    if (d->file_count > 1) {
        qsort(d->files, (size_t)d->file_count, sizeof(*d->files), df_cmp_file);
    }
    return d;
}

void cbm_diff_free(cbm_diff_t *d) {
    if (!d) {
        return;
    }
    CBMArena arena = d->arena; /* d itself is inside it */
    cbm_arena_destroy(&arena);
}

const cbm_diff_file_t *cbm_diff_files(const cbm_diff_t *d, int *count) {
    if (count) {
        *count = d ? d->file_count : 0;
    }
    return d ? d->files : NULL;
}

bool cbm_diff_complete(const cbm_diff_t *d) {
    return d && d->complete;
}
