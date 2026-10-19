/*
 * svn_state.c -- Subversion working-copy observation core (#1127, part A).
 *
 * See svn_state.h for the contract. Three rules shape the file walk:
 *
 *   - Open first, then decide. Every file and directory is opened one path
 *     component at a time without following it, and its type and size come
 *     from the open handle -- there is no stat-then-open window, and no
 *     symlink or junction anywhere on a path is ever crossed.
 *   - Working-copy content is never fatal. Symlinks, FIFOs, sockets, devices,
 *     unreadable and oversized files are skipped and counted. UNCERTAIN is
 *     reserved for output that cannot be trusted and for I/O errors.
 *   - Bounded resources. Directories are walked from an explicit heap work
 *     stack, so tree depth never reaches the native stack, and the size cap is
 *     enforced on the bytes actually read, not on a size measured earlier.
 */
#include "watcher/svn_state.h"

#include "discover/discover.h"
#include "foundation/compat_fs.h"
#include "foundation/hash_table.h"
#include "foundation/limits.h"
#include "foundation/mem_core.h"
#include "foundation/platform.h"
#ifdef _WIN32
#include "foundation/win_utf8.h"
#include <windows.h>
#else
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#include <ctype.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef _WIN32
#define SVN_PATH_DELIM ';'
#define SVN_EXE_NAME "svn.exe"
#else
#define SVN_PATH_DELIM ':'
#define SVN_EXE_NAME "svn"
#endif

#define SVN_MEM CBM_MEM_CLASS_OTHER
#define SVN_FNV_OFFSET UINT64_C(1469598103934665603)
#define SVN_FNV_PRIME UINT64_C(1099511628211)
#define SVN_MAX_XML_DEPTH 64
#define SVN_MAX_TAG_NAME 96
#define SVN_TAG_BUFFER CBM_SZ_16K
#define SVN_READ_CHUNK CBM_SZ_16K
#define SVN_STACK_INITIAL 16

typedef struct {
    char **paths;
    size_t count;
    size_t capacity;
    CBMHashTable *seen;
} candidate_list_t;

typedef struct {
    char item[32];
    char props[32];
    char revision[64];
    char tree_conflicted[16];
    char switched[16];
    char copied[16];
    char wc_locked[16];
    char depth[32];
    char moved_from[CBM_SZ_4K];
    char moved_to[CBM_SZ_4K];
} wc_fields_t;

typedef struct {
    char root[CBM_SZ_4K];
    char input_root[CBM_SZ_4K];
    char stack[SVN_MAX_XML_DEPTH][SVN_MAX_TAG_NAME];
    int depth;
    bool xml_decl_seen;
    bool status_seen;
    bool status_closed;
    bool target_seen;
    bool entry_open;
    bool entry_has_status;
    char entry_path[CBM_SZ_4K];
    uint64_t semantic_xor;
    uint64_t semantic_sum;
    bool has_local_changes;
    int entry_count;
    candidate_list_t candidates;
} parser_t;

#ifdef CBM_SVN_STATE_ENABLE_TEST_API
static void (*g_after_measure_hook)(const char *relative_path);
#endif

static uint64_t hash_fold(uint64_t hash, const void *data, size_t length) {
    const unsigned char *bytes = (const unsigned char *)data;
    for (size_t i = 0; i < length; i++) {
        hash ^= bytes[i];
        hash *= SVN_FNV_PRIME;
    }
    return hash;
}

static uint64_t hash_string(uint64_t hash, const char *value) {
    hash = hash_fold(hash, value, strlen(value));
    const unsigned char separator = 0;
    return hash_fold(hash, &separator, sizeof(separator));
}

static uint64_t hash_mix(uint64_t value) {
    value ^= value >> 30;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

/* ── Paths ─────────────────────────────────────────────────────── */

static bool path_char_equal(char left, char right) {
#ifdef _WIN32
    return tolower((unsigned char)left) == tolower((unsigned char)right);
#else
    return left == right;
#endif
}

/* '\' separates components only on Windows. On POSIX it is an ordinary byte
 * of a legal filename and must survive, or the file can never be opened. */
static void normalize_separators(char *path) {
#ifdef _WIN32
    for (char *cursor = path; *cursor; cursor++) {
        if (*cursor == '\\') {
            *cursor = '/';
        }
    }
#endif
    size_t length = strlen(path);
    while (length > 1 && path[length - 1] == '/') {
        path[--length] = '\0';
    }
}

static bool path_has_prefix(const char *path, const char *prefix) {
    size_t prefix_length = strlen(prefix);
    for (size_t i = 0; i < prefix_length; i++) {
        if (!path[i] || !path_char_equal(path[i], prefix[i])) {
            return false;
        }
    }
    return path[prefix_length] == '\0' || path[prefix_length] == '/';
}

static bool path_is_absolute(const char *path) {
    if (!path || !path[0]) {
        return false;
    }
#ifdef _WIN32
    return path[0] == '/' || path[0] == '\\' || (isalpha((unsigned char)path[0]) && path[1] == ':');
#else
    return path[0] == '/';
#endif
}

static bool relative_path_is_safe(const char *path) {
    if (!path || !path[0] || path_is_absolute(path)) {
        return false;
    }
    const char *segment = path;
    for (const char *cursor = path;; cursor++) {
        if (*cursor == '\0' || *cursor == '/') {
            size_t length = (size_t)(cursor - segment);
            if (length == 0 || (length == 1 && segment[0] == '.') ||
                (length == 2 && segment[0] == '.' && segment[1] == '.')) {
                return false;
            }
            if (*cursor == '\0') {
                break;
            }
            segment = cursor + 1;
        }
#ifdef _WIN32
        /* A drive-relative path or an alternate data stream; on POSIX ':' is
         * an ordinary filename byte. */
        if (*cursor == ':') {
            return false;
        }
#endif
    }
    return true;
}

static bool entry_relative_path(const char *root, const char *input_root, const char *entry,
                                char *out, size_t out_size) {
    char normalized[CBM_SZ_4K];
    int written = snprintf(normalized, sizeof(normalized), "%s", entry ? entry : "");
    if (written < 0 || (size_t)written >= sizeof(normalized)) {
        return false;
    }
    normalize_separators(normalized);

    const char *relative = normalized;
    if (path_is_absolute(normalized)) {
        const char *matched_root = root;
        if (!path_has_prefix(normalized, matched_root)) {
            matched_root = input_root;
        }
        if (!path_has_prefix(normalized, matched_root)) {
            return false;
        }
        relative = normalized + strlen(matched_root);
        if (*relative == '/') {
            relative++;
        }
    }
    if (!relative[0]) {
        relative = ".";
    }
    if (strcmp(relative, ".") != 0 && !relative_path_is_safe(relative)) {
        return false;
    }
    written = snprintf(out, out_size, "%s", relative);
    return written >= 0 && (size_t)written < out_size;
}

static const char *path_basename(const char *relative) {
    const char *slash = strrchr(relative, '/');
    return slash ? slash + 1 : relative;
}

/* The same name rules discovery applies, so a change the indexer would never
 * see does not count as a change here either. */
static bool name_is_indexable_file(const char *name) {
    return cbm_language_for_filename(name) != CBM_LANG_COUNT &&
           !cbm_has_ignored_suffix(name, CBM_MODE_FULL);
}

static bool name_is_excluded_dir(const char *name) {
    return strcmp(name, ".svn") == 0 || cbm_should_skip_dir(name, CBM_MODE_FULL);
}

static bool child_path(const char *parent, const char *name, char *out, size_t out_size) {
    int written = snprintf(out, out_size, "%s/%s", parent, name);
    return written >= 0 && (size_t)written < out_size;
}

/* ── Candidate list (memory core) ──────────────────────────────── */

static void candidates_free(candidate_list_t *list) {
    if (!list) {
        return;
    }
    for (size_t i = 0; i < list->count; i++) {
        cbm_free(SVN_MEM, list->paths[i]);
    }
    cbm_free(SVN_MEM, list->paths);
    cbm_ht_free(list->seen);
    memset(list, 0, sizeof(*list));
}

static bool grow_pointer_array(char ***items, size_t *capacity) {
    size_t next_capacity = *capacity ? *capacity * 2 : SVN_STACK_INITIAL;
    if (next_capacity < *capacity || next_capacity > SIZE_MAX / sizeof(**items)) {
        return false;
    }
    char **next = (char **)cbm_realloc(SVN_MEM, *items, next_capacity * sizeof(**items));
    if (!next) {
        return false;
    }
    *items = next;
    *capacity = next_capacity;
    return true;
}

static bool candidates_add(candidate_list_t *list, const char *path) {
    if (strcmp(path, ".") == 0 || cbm_ht_has(list->seen, path)) {
        return true;
    }
    if (list->count == list->capacity && !grow_pointer_array(&list->paths, &list->capacity)) {
        return false;
    }
    char *copy = cbm_mem_strdup(SVN_MEM, path);
    if (!copy) {
        return false;
    }
    list->paths[list->count++] = copy;
    cbm_ht_set(list->seen, copy, copy);
    return true;
}

/* ── XML ───────────────────────────────────────────────────────── */

static int utf8_append(uint32_t codepoint, char *out, size_t out_size, size_t *used) {
    unsigned char encoded[4];
    size_t count = 0;
    if (codepoint <= 0x7f) {
        encoded[count++] = (unsigned char)codepoint;
    } else if (codepoint <= 0x7ff) {
        encoded[count++] = (unsigned char)(0xc0 | (codepoint >> 6));
        encoded[count++] = (unsigned char)(0x80 | (codepoint & 0x3f));
    } else if (codepoint >= 0xd800 && codepoint <= 0xdfff) {
        return -1;
    } else if (codepoint <= 0xffff) {
        encoded[count++] = (unsigned char)(0xe0 | (codepoint >> 12));
        encoded[count++] = (unsigned char)(0x80 | ((codepoint >> 6) & 0x3f));
        encoded[count++] = (unsigned char)(0x80 | (codepoint & 0x3f));
    } else if (codepoint <= 0x10ffff) {
        encoded[count++] = (unsigned char)(0xf0 | (codepoint >> 18));
        encoded[count++] = (unsigned char)(0x80 | ((codepoint >> 12) & 0x3f));
        encoded[count++] = (unsigned char)(0x80 | ((codepoint >> 6) & 0x3f));
        encoded[count++] = (unsigned char)(0x80 | (codepoint & 0x3f));
    } else {
        return -1;
    }
    if (*used + count >= out_size) {
        return -1;
    }
    memcpy(out + *used, encoded, count);
    *used += count;
    return 0;
}

static int decode_xml(const char *input, size_t length, char *out, size_t out_size) {
    size_t used = 0;
    for (size_t i = 0; i < length;) {
        if (input[i] != '&') {
            if (used + 1 >= out_size || input[i] == '<') {
                return -1;
            }
            out[used++] = input[i++];
            continue;
        }
        size_t end = i + 1;
        while (end < length && input[end] != ';' && end - i <= 16) {
            end++;
        }
        if (end >= length || input[end] != ';') {
            return -1;
        }
        const char *entity = input + i + 1;
        size_t entity_length = end - i - 1;
        uint32_t codepoint = 0;
        if (entity_length == 3 && memcmp(entity, "amp", 3) == 0) {
            codepoint = '&';
        } else if (entity_length == 2 && memcmp(entity, "lt", 2) == 0) {
            codepoint = '<';
        } else if (entity_length == 2 && memcmp(entity, "gt", 2) == 0) {
            codepoint = '>';
        } else if (entity_length == 4 && memcmp(entity, "quot", 4) == 0) {
            codepoint = '"';
        } else if (entity_length == 4 && memcmp(entity, "apos", 4) == 0) {
            codepoint = '\'';
        } else if (entity_length >= 2 && entity[0] == '#') {
            size_t digit = 1;
            int base = 10;
            if (entity[digit] == 'x' || entity[digit] == 'X') {
                base = 16;
                digit++;
            }
            if (digit == entity_length) {
                return -1;
            }
            for (; digit < entity_length; digit++) {
                int value;
                if (entity[digit] >= '0' && entity[digit] <= '9') {
                    value = entity[digit] - '0';
                } else if (base == 16 && entity[digit] >= 'a' && entity[digit] <= 'f') {
                    value = entity[digit] - 'a' + 10;
                } else if (base == 16 && entity[digit] >= 'A' && entity[digit] <= 'F') {
                    value = entity[digit] - 'A' + 10;
                } else {
                    return -1;
                }
                if (codepoint > (UINT32_MAX - (uint32_t)value) / (uint32_t)base) {
                    return -1;
                }
                codepoint = codepoint * (uint32_t)base + (uint32_t)value;
            }
        } else {
            return -1;
        }
        if ((codepoint < 0x20 && codepoint != '\t' && codepoint != '\n' && codepoint != '\r') ||
            utf8_append(codepoint, out, out_size, &used) != 0) {
            return -1;
        }
        i = end + 1;
    }
    out[used] = '\0';
    return 0;
}

static int copy_attribute(const char *name, const char *value, size_t value_length,
                          const char *wanted, char *out, size_t out_size) {
    if (strcmp(name, wanted) != 0) {
        char scratch[CBM_SZ_4K];
        return decode_xml(value, value_length, scratch, sizeof(scratch));
    }
    return decode_xml(value, value_length, out, out_size);
}

static int parse_attributes(const char *cursor, char *entry_path, wc_fields_t *fields) {
    while (*cursor) {
        while (isspace((unsigned char)*cursor)) {
            cursor++;
        }
        if (!*cursor) {
            return 0;
        }
        const char *name_start = cursor;
        while (*cursor && !isspace((unsigned char)*cursor) && *cursor != '=') {
            cursor++;
        }
        size_t name_length = (size_t)(cursor - name_start);
        if (name_length == 0 || name_length >= SVN_MAX_TAG_NAME) {
            return -1;
        }
        char name[SVN_MAX_TAG_NAME];
        memcpy(name, name_start, name_length);
        name[name_length] = '\0';
        while (isspace((unsigned char)*cursor)) {
            cursor++;
        }
        if (*cursor++ != '=') {
            return -1;
        }
        while (isspace((unsigned char)*cursor)) {
            cursor++;
        }
        char quote = *cursor++;
        if (quote != '\'' && quote != '"') {
            return -1;
        }
        const char *value = cursor;
        while (*cursor && *cursor != quote) {
            cursor++;
        }
        if (!*cursor) {
            return -1;
        }
        size_t value_length = (size_t)(cursor - value);
        cursor++;

        int result = 0;
        if (entry_path) {
            result = copy_attribute(name, value, value_length, "path", entry_path, CBM_SZ_4K);
        } else if (fields) {
            bool matched = false;
#define SVN_COPY_FIELD(attr, member)                                                          \
    do {                                                                                      \
        if (strcmp(name, attr) == 0) {                                                        \
            matched = true;                                                                   \
            result = decode_xml(value, value_length, fields->member, sizeof(fields->member)); \
        }                                                                                     \
    } while (0)
            SVN_COPY_FIELD("item", item);
            SVN_COPY_FIELD("props", props);
            SVN_COPY_FIELD("revision", revision);
            SVN_COPY_FIELD("tree-conflicted", tree_conflicted);
            SVN_COPY_FIELD("switched", switched);
            SVN_COPY_FIELD("copied", copied);
            SVN_COPY_FIELD("wc-locked", wc_locked);
            SVN_COPY_FIELD("depth", depth);
            SVN_COPY_FIELD("moved-from", moved_from);
            SVN_COPY_FIELD("moved-to", moved_to);
#undef SVN_COPY_FIELD
            if (result == 0 && !matched) {
                char scratch[CBM_SZ_4K];
                result = decode_xml(value, value_length, scratch, sizeof(scratch));
            }
        } else {
            char scratch[CBM_SZ_4K];
            result = decode_xml(value, value_length, scratch, sizeof(scratch));
        }
        if (result != 0) {
            return -1;
        }
    }
    return 0;
}

static bool status_needs_content(const char *item) {
    return strcmp(item, "modified") == 0 || strcmp(item, "added") == 0 ||
           strcmp(item, "replaced") == 0 || strcmp(item, "conflicted") == 0 ||
           strcmp(item, "obstructed") == 0 || strcmp(item, "unversioned") == 0 ||
           strcmp(item, "ignored") == 0;
}

static bool status_has_local_changes(const wc_fields_t *fields) {
    bool item_changed =
        strcmp(fields->item, "normal") != 0 && strcmp(fields->item, "external") != 0;
    bool props_changed = fields->props[0] && strcmp(fields->props, "none") != 0 &&
                         strcmp(fields->props, "normal") != 0;
    return item_changed || props_changed;
}

static int finalize_status(parser_t *parser, const wc_fields_t *fields) {
    if (!fields->item[0] || parser->entry_has_status) {
        return -1;
    }
    char relative[CBM_SZ_4K];
    if (!entry_relative_path(parser->root, parser->input_root, parser->entry_path, relative,
                             sizeof(relative))) {
        return -1;
    }

    uint64_t entry_hash = SVN_FNV_OFFSET;
    entry_hash = hash_string(entry_hash, relative);
    entry_hash = hash_string(entry_hash, fields->item);
    entry_hash = hash_string(entry_hash, fields->props);
    entry_hash = hash_string(entry_hash, fields->revision);
    entry_hash = hash_string(entry_hash, fields->tree_conflicted);
    entry_hash = hash_string(entry_hash, fields->switched);
    entry_hash = hash_string(entry_hash, fields->copied);
    entry_hash = hash_string(entry_hash, fields->wc_locked);
    entry_hash = hash_string(entry_hash, fields->depth);
    entry_hash = hash_string(entry_hash, fields->moved_from);
    entry_hash = hash_string(entry_hash, fields->moved_to);
    parser->semantic_xor ^= hash_mix(entry_hash);
    parser->semantic_sum += entry_hash * SVN_FNV_PRIME;
    if (status_has_local_changes(fields)) {
        parser->has_local_changes = true;
    }
    parser->entry_count++;
    parser->entry_has_status = true;

    return !status_needs_content(fields->item) || candidates_add(&parser->candidates, relative)
               ? 0
               : -1;
}

static int process_open_tag(parser_t *parser, char *tag) {
    char *cursor = tag;
    while (isspace((unsigned char)*cursor)) {
        cursor++;
    }
    if (*cursor == '?') {
        size_t length = strlen(cursor);
        if (parser->depth != 0 || parser->status_seen || parser->xml_decl_seen || length < 2 ||
            cursor[length - 1] != '?' || strncmp(cursor, "?xml", 4) != 0) {
            return -1;
        }
        parser->xml_decl_seen = true;
        return 0;
    }
    if (*cursor == '!') {
        return -1;
    }

    bool closing = *cursor == '/';
    if (closing) {
        cursor++;
    }
    while (isspace((unsigned char)*cursor)) {
        cursor++;
    }
    char *name = cursor;
    while (*cursor && !isspace((unsigned char)*cursor) && *cursor != '/') {
        cursor++;
    }
    size_t name_length = (size_t)(cursor - name);
    if (name_length == 0 || name_length >= SVN_MAX_TAG_NAME) {
        return -1;
    }
    char name_copy[SVN_MAX_TAG_NAME];
    memcpy(name_copy, name, name_length);
    name_copy[name_length] = '\0';

    if (closing) {
        while (isspace((unsigned char)*cursor)) {
            cursor++;
        }
        if (*cursor || parser->depth <= 0 ||
            strcmp(parser->stack[parser->depth - 1], name_copy) != 0) {
            return -1;
        }
        if (strcmp(name_copy, "entry") == 0) {
            if (!parser->entry_open || !parser->entry_has_status) {
                return -1;
            }
            parser->entry_open = false;
            parser->entry_has_status = false;
            parser->entry_path[0] = '\0';
        } else if (strcmp(name_copy, "status") == 0) {
            parser->status_closed = true;
        }
        parser->depth--;
        return 0;
    }

    char *end = cursor + strlen(cursor);
    while (end > cursor && isspace((unsigned char)end[-1])) {
        *--end = '\0';
    }
    bool self_closing = end > cursor && end[-1] == '/';
    if (self_closing) {
        *--end = '\0';
        while (end > cursor && isspace((unsigned char)end[-1])) {
            *--end = '\0';
        }
    }
    if (parser->status_closed) {
        return -1;
    }

    if (strcmp(name_copy, "status") == 0) {
        if (parser->depth != 0 || parser->status_seen || self_closing) {
            return -1;
        }
        if (parse_attributes(cursor, NULL, NULL) != 0) {
            return -1;
        }
        parser->status_seen = true;
    } else if (strcmp(name_copy, "target") == 0) {
        if (!parser->status_seen || parser->depth != 1 ||
            strcmp(parser->stack[parser->depth - 1], "status") != 0 ||
            parse_attributes(cursor, NULL, NULL) != 0) {
            return -1;
        }
        parser->target_seen = true;
    } else if (strcmp(name_copy, "entry") == 0) {
        if (!parser->target_seen || parser->entry_open || self_closing || parser->depth < 2 ||
            strcmp(parser->stack[parser->depth - 1], "target") != 0) {
            return -1;
        }
        parser->entry_path[0] = '\0';
        if (parse_attributes(cursor, parser->entry_path, NULL) != 0 || !parser->entry_path[0]) {
            return -1;
        }
        parser->entry_open = true;
        parser->entry_has_status = false;
    } else if (strcmp(name_copy, "wc-status") == 0) {
        if (!parser->entry_open || parser->depth < 3 ||
            strcmp(parser->stack[parser->depth - 1], "entry") != 0) {
            return -1;
        }
        wc_fields_t fields = {0};
        if (parse_attributes(cursor, NULL, &fields) != 0 || finalize_status(parser, &fields) != 0) {
            return -1;
        }
    } else if (parse_attributes(cursor, NULL, NULL) != 0) {
        return -1;
    }

    if (!self_closing) {
        if (parser->depth >= SVN_MAX_XML_DEPTH) {
            return -1;
        }
        snprintf(parser->stack[parser->depth], SVN_MAX_TAG_NAME, "%s", name_copy);
        parser->depth++;
    }
    return 0;
}

/* ── Contained opens ───────────────────────────────────────────── */

typedef enum {
    SVN_NODE_FILE,
    SVN_NODE_DIR,
    SVN_NODE_SYMLINK,
    SVN_NODE_SPECIAL,
    SVN_NODE_UNREADABLE,
    SVN_NODE_EXCLUDED, /* under a directory discovery skips: neither read nor counted */
    SVN_NODE_ERROR,
} svn_node_kind_t;

typedef struct {
#ifdef _WIN32
    HANDLE handle;
#else
    int fd;
#endif
    int64_t size;
} svn_node_t;

typedef struct {
    const char *root; /* canonical, '/'-separated */
#ifndef _WIN32
    int root_fd;
#endif
    unsigned char *buffer; /* SVN_READ_CHUNK bytes */
    uint64_t xor_value;
    uint64_t sum_value;
    uint64_t bytes_hashed;
    int file_count;
    int skipped_symlinks;
    int skipped_special;
    int skipped_unreadable;
    int skipped_oversize;
} svn_walk_t;

typedef struct {
    char **items;
    size_t count;
    size_t capacity;
} svn_work_stack_t;

/* Excluded directories anywhere above the final component end the open
 * before anything is touched. */
static bool path_has_excluded_ancestor(const char *relative) {
    char component[CBM_SZ_4K];
    const char *cursor = relative;
    const char *slash;
    while ((slash = strchr(cursor, '/')) != NULL) {
        size_t length = (size_t)(slash - cursor);
        if (length >= sizeof(component)) {
            return false;
        }
        memcpy(component, cursor, length);
        component[length] = '\0';
        if (name_is_excluded_dir(component)) {
            return true;
        }
        cursor = slash + 1;
    }
    return false;
}

#ifndef _WIN32

static svn_node_kind_t node_kind_for_errno(int error) {
    if (error == ELOOP) {
        return SVN_NODE_SYMLINK; /* O_NOFOLLOW met a symlink */
    }
    if (error == EACCES || error == EPERM) {
        return SVN_NODE_UNREADABLE;
    }
    if (error == ENXIO) {
        return SVN_NODE_SPECIAL; /* a socket or an absent device (Linux) */
    }
#ifdef EOPNOTSUPP
    if (error == EOPNOTSUPP) {
        return SVN_NODE_SPECIAL; /* a socket (macOS) */
    }
#endif
    return SVN_NODE_ERROR;
}

static void node_close(svn_node_t *node) {
    if (node->fd >= 0) {
        (void)close(node->fd);
        node->fd = -1;
    }
}

/* Open one name below parent_fd without following it, then classify the
 * open descriptor. O_NONBLOCK keeps a FIFO from waiting for a writer. */
static svn_node_kind_t open_below(int parent_fd, const char *name, svn_node_t *node) {
    node->fd = -1;
    node->size = 0;
    int fd = openat(parent_fd, name, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    if (fd < 0) {
        return node_kind_for_errno(errno);
    }
    struct stat state;
    if (fstat(fd, &state) != 0) {
        (void)close(fd);
        return SVN_NODE_ERROR;
    }
    if (!S_ISREG(state.st_mode) && !S_ISDIR(state.st_mode)) {
        (void)close(fd);
        return SVN_NODE_SPECIAL;
    }
    node->fd = fd;
    node->size = (int64_t)state.st_size;
    return S_ISREG(state.st_mode) ? SVN_NODE_FILE : SVN_NODE_DIR;
}

/* root/relative, one component at a time from the root descriptor. */
static svn_node_kind_t open_contained(const svn_walk_t *walk, const char *relative,
                                      svn_node_t *node) {
    node->fd = -1;
    node->size = 0;
    if (path_has_excluded_ancestor(relative)) {
        return SVN_NODE_EXCLUDED;
    }
    char component[CBM_SZ_4K];
    svn_node_t parent = {.fd = -1, .size = 0};
    int parent_fd = walk->root_fd;
    const char *cursor = relative;
    for (;;) {
        const char *slash = strchr(cursor, '/');
        size_t length = slash ? (size_t)(slash - cursor) : strlen(cursor);
        if (length == 0 || length >= sizeof(component)) {
            node_close(&parent);
            return SVN_NODE_ERROR;
        }
        memcpy(component, cursor, length);
        component[length] = '\0';
        svn_node_t next;
        svn_node_kind_t kind = open_below(parent_fd, component, &next);
        node_close(&parent);
        if (!slash) {
            *node = next;
            return kind;
        }
        if (kind != SVN_NODE_DIR) {
            node_close(&next);
            /* A regular file where the status reported a directory is a race
             * with the working copy, not a property of its content. */
            return kind == SVN_NODE_FILE ? SVN_NODE_ERROR : kind;
        }
        parent = next;
        parent_fd = parent.fd;
        cursor = slash + 1;
    }
}

static long long node_read(svn_node_t *node, unsigned char *buffer, size_t want,
                           svn_node_kind_t *failure) {
    for (;;) {
        ssize_t got = read(node->fd, buffer, want);
        if (got >= 0) {
            return (long long)got;
        }
        if (errno != EINTR) {
            *failure = errno == EACCES || errno == EPERM ? SVN_NODE_UNREADABLE : SVN_NODE_ERROR;
            return -1;
        }
    }
}

#else /* _WIN32 */

static svn_node_kind_t node_kind_for_win_error(DWORD error) {
    if (error == ERROR_ACCESS_DENIED || error == ERROR_SHARING_VIOLATION ||
        error == ERROR_LOCK_VIOLATION) {
        return SVN_NODE_UNREADABLE;
    }
    return SVN_NODE_ERROR;
}

static void node_close(svn_node_t *node) {
    if (node->handle != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(node->handle);
        node->handle = INVALID_HANDLE_VALUE;
    }
}

static bool paths_equal(const char *left, const char *right) {
    for (; *left && *right; left++, right++) {
        if (!path_char_equal(*left, *right)) {
            return false;
        }
    }
    return *left == *right;
}

/* The open handle's own final path, '/'-separated, without the \\?\ prefix:
 * the same spelling cbm_canonical_path gives the root. */
static bool handle_final_path(HANDLE handle, char *out, size_t out_size) {
    const DWORD flags = FILE_NAME_NORMALIZED | VOLUME_NAME_DOS;
    DWORD needed = GetFinalPathNameByHandleW(handle, NULL, 0, flags);
    if (needed == 0 || needed >= CBM_SZ_64K) {
        return false;
    }
    wchar_t *wide = (wchar_t *)cbm_alloc(SVN_MEM, ((size_t)needed + 1) * sizeof(wchar_t));
    if (!wide) {
        return false;
    }
    DWORD written = GetFinalPathNameByHandleW(handle, wide, needed + 1, flags);
    bool ok = written > 0 && written <= needed;
    if (ok) {
        cbm_module_path_unprefix(wide);
        ok = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, wide, -1, out, (int)out_size, NULL,
                                 NULL) > 0;
    }
    cbm_free(SVN_MEM, wide);
    if (ok) {
        normalize_separators(out);
    }
    return ok;
}

/* Windows has no openat: open the full path without following its final
 * component, then prove from the open handle that nothing on the way was a
 * junction or link -- its final path must be exactly root/relative. */
static svn_node_kind_t open_contained(const svn_walk_t *walk, const char *relative,
                                      svn_node_t *node) {
    node->handle = INVALID_HANDLE_VALUE;
    node->size = 0;
    if (path_has_excluded_ancestor(relative)) {
        return SVN_NODE_EXCLUDED;
    }
    char expected[CBM_SZ_4K];
    if (!child_path(walk->root, relative, expected, sizeof(expected))) {
        return SVN_NODE_ERROR;
    }
    wchar_t *wide = cbm_path_to_wide_in(SVN_MEM, expected);
    if (!wide) {
        return SVN_NODE_ERROR;
    }
    HANDLE handle = CreateFileW(
        wide, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
        OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, NULL);
    DWORD error = handle == INVALID_HANDLE_VALUE ? GetLastError() : ERROR_SUCCESS;
    cbm_free(SVN_MEM, wide);
    if (handle == INVALID_HANDLE_VALUE) {
        return node_kind_for_win_error(error);
    }
    svn_node_t opened = {.handle = handle, .size = 0};
    BY_HANDLE_FILE_INFORMATION information;
    if (!GetFileInformationByHandle(handle, &information)) {
        node_close(&opened);
        return SVN_NODE_ERROR;
    }
    if ((information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0) {
        node_close(&opened);
        return SVN_NODE_SYMLINK;
    }
    bool directory = (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
    if (!directory && GetFileType(handle) != FILE_TYPE_DISK) {
        node_close(&opened);
        return SVN_NODE_SPECIAL;
    }
    char final_path[CBM_SZ_4K];
    if (!handle_final_path(handle, final_path, sizeof(final_path)) ||
        !paths_equal(final_path, expected)) {
        node_close(&opened);
        return SVN_NODE_SYMLINK; /* reached through a junction or link on an ancestor */
    }
    opened.size = (int64_t)(((uint64_t)information.nFileSizeHigh << 32) | information.nFileSizeLow);
    *node = opened;
    return directory ? SVN_NODE_DIR : SVN_NODE_FILE;
}

static long long node_read(svn_node_t *node, unsigned char *buffer, size_t want,
                           svn_node_kind_t *failure) {
    DWORD got = 0;
    if (!ReadFile(node->handle, buffer, (DWORD)want, &got, NULL)) {
        *failure = node_kind_for_win_error(GetLastError());
        return -1;
    }
    return (long long)got;
}

#endif /* _WIN32 */

/* ── Tree fingerprint ──────────────────────────────────────────── */

static bool work_stack_push(svn_work_stack_t *stack, const char *relative) {
    if (stack->count == stack->capacity && !grow_pointer_array(&stack->items, &stack->capacity)) {
        return false;
    }
    char *copy = cbm_mem_strdup(SVN_MEM, relative);
    if (!copy) {
        return false;
    }
    stack->items[stack->count++] = copy;
    return true;
}

static void work_stack_free(svn_work_stack_t *stack) {
    for (size_t i = 0; i < stack->count; i++) {
        cbm_free(SVN_MEM, stack->items[i]);
    }
    cbm_free(SVN_MEM, stack->items);
    memset(stack, 0, sizeof(*stack));
}

static int hash_open_file(svn_walk_t *walk, svn_node_t *node, const char *relative) {
    const uint64_t cap = (uint64_t)cbm_max_file_bytes();
    if (node->size < 0 || (uint64_t)node->size > cap) {
        walk->skipped_oversize++;
        return 0;
    }
#ifdef CBM_SVN_STATE_ENABLE_TEST_API
    if (g_after_measure_hook) {
        g_after_measure_hook(relative);
    }
#endif
    uint64_t file_hash = hash_string(SVN_FNV_OFFSET, relative);
    uint64_t total = 0;
    for (;;) {
        /* Never read more than one byte past the cap: enough to learn that the
         * file grew beyond it after it was measured. */
        uint64_t budget = cap + 1 - total;
        size_t want = budget < SVN_READ_CHUNK ? (size_t)budget : SVN_READ_CHUNK;
        svn_node_kind_t failure = SVN_NODE_ERROR;
        long long got = node_read(node, walk->buffer, want, &failure);
        if (got < 0) {
            if (failure == SVN_NODE_UNREADABLE) {
                walk->skipped_unreadable++;
                return 0;
            }
            return -1;
        }
        if (got == 0) {
            break;
        }
        total += (uint64_t)got;
        if (total > cap) {
            walk->skipped_oversize++;
            return 0;
        }
        file_hash = hash_fold(file_hash, walk->buffer, (size_t)got);
    }
    walk->xor_value ^= hash_mix(file_hash);
    walk->sum_value += file_hash * SVN_FNV_PRIME;
    walk->bytes_hashed += total;
    walk->file_count++;
    return 0;
}

/* Fold one opened node into the walk and close it. A directory is not read
 * here; it goes onto the work stack. */
static int visit_node(svn_walk_t *walk, svn_work_stack_t *stack, const char *relative,
                      svn_node_kind_t kind, svn_node_t *node) {
    int result = 0;
    switch (kind) {
    case SVN_NODE_FILE:
        if (name_is_indexable_file(path_basename(relative))) {
            result = hash_open_file(walk, node, relative);
        }
        break;
    case SVN_NODE_DIR:
        if (!name_is_excluded_dir(path_basename(relative)) && !work_stack_push(stack, relative)) {
            result = -1;
        }
        break;
    case SVN_NODE_SYMLINK:
        walk->skipped_symlinks++;
        break;
    case SVN_NODE_SPECIAL:
        walk->skipped_special++;
        break;
    case SVN_NODE_UNREADABLE:
        walk->skipped_unreadable++;
        break;
    case SVN_NODE_EXCLUDED:
        break;
    case SVN_NODE_ERROR:
    default:
        result = -1;
        break;
    }
    node_close(node);
    return result;
}

/* List one directory taken off the work stack. Each child is opened through
 * the same contained route, so a directory swapped for a link between being
 * pushed and being listed is caught on the child's own handle. */
static int walk_directory(svn_walk_t *walk, svn_work_stack_t *stack, const char *relative) {
    svn_node_t directory;
    svn_node_kind_t kind = open_contained(walk, relative, &directory);
    if (kind != SVN_NODE_DIR) {
        return visit_node(walk, stack, relative, kind, &directory);
    }
    char child[CBM_SZ_4K];
    int result = 0;
#ifndef _WIN32
    DIR *listing = fdopendir(directory.fd);
    if (!listing) {
        node_close(&directory);
        return -1;
    }
    directory.fd = -1; /* owned by the listing now */
    struct dirent *entry;
    while (result == 0 && (entry = readdir(listing)) != NULL) {
        const char *name = entry->d_name;
        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
            continue;
        }
#ifdef DT_REG
        if (entry->d_type == DT_REG && !name_is_indexable_file(name)) {
            continue;
        }
#endif
        if (!child_path(relative, name, child, sizeof(child))) {
            result = -1;
            break;
        }
        svn_node_t node;
        svn_node_kind_t child_kind = open_below(dirfd(listing), name, &node);
        result = visit_node(walk, stack, child, child_kind, &node);
    }
    (void)closedir(listing);
#else
    char absolute[CBM_SZ_4K];
    cbm_dir_t *listing =
        child_path(walk->root, relative, absolute, sizeof(absolute)) ? cbm_opendir(absolute) : NULL;
    if (!listing) {
        node_close(&directory);
        return -1;
    }
    cbm_dirent_t *entry;
    while (result == 0 && (entry = cbm_readdir(listing)) != NULL) {
        const char *name = entry->name;
        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
            continue;
        }
        if (!entry->is_dir && !name_is_indexable_file(name)) {
            continue;
        }
        if (!child_path(relative, name, child, sizeof(child))) {
            result = -1;
            break;
        }
        svn_node_t node;
        svn_node_kind_t child_kind = open_contained(walk, child, &node);
        result = visit_node(walk, stack, child, child_kind, &node);
    }
    cbm_closedir(listing);
    node_close(&directory); /* held open while listing */
#endif
    return result;
}

static int candidate_compare(const void *left, const void *right) {
    const char *left_path = *(const char *const *)left;
    const char *right_path = *(const char *const *)right;
    size_t left_length = strlen(left_path);
    size_t right_length = strlen(right_path);
    if (left_length != right_length) {
        return left_length < right_length ? -1 : 1;
    }
    return strcmp(left_path, right_path);
}

static bool candidate_has_ancestor(const CBMHashTable *scanned_directories, const char *candidate) {
    char prefix[CBM_SZ_4K];
    int written = snprintf(prefix, sizeof(prefix), "%s", candidate);
    if (written < 0 || (size_t)written >= sizeof(prefix)) {
        return false;
    }
    char *cursor = prefix;
    while ((cursor = strchr(cursor, '/')) != NULL) {
        *cursor = '\0';
        if (cbm_ht_has(scanned_directories, prefix)) {
            return true;
        }
        *cursor++ = '/';
    }
    return false;
}

static int walk_candidates(svn_walk_t *walk, candidate_list_t *candidates) {
    CBMHashTable *scanned_directories =
        cbm_ht_create((uint32_t)(candidates->count ? candidates->count : 1));
    if (!scanned_directories) {
        return -1;
    }
    svn_work_stack_t stack = {0};
    int result = 0;
    for (size_t i = 0; result == 0 && i < candidates->count; i++) {
        const char *relative = candidates->paths[i];
        if (candidate_has_ancestor(scanned_directories, relative)) {
            continue;
        }
        svn_node_t node;
        svn_node_kind_t kind = open_contained(walk, relative, &node);
        if (kind == SVN_NODE_DIR) {
            cbm_ht_set(scanned_directories, relative, candidates->paths[i]);
        }
        result = visit_node(walk, &stack, relative, kind, &node);
        while (result == 0 && stack.count > 0) {
            char *directory = stack.items[--stack.count];
            result = walk_directory(walk, &stack, directory);
            cbm_free(SVN_MEM, directory);
        }
    }
    work_stack_free(&stack);
    cbm_ht_free(scanned_directories);
    return result;
}

static int fingerprint_candidates(parser_t *parser, cbm_svn_observation_t *observation) {
    qsort(parser->candidates.paths, parser->candidates.count, sizeof(*parser->candidates.paths),
          candidate_compare);
    svn_walk_t walk = {0};
    walk.root = parser->root;
    int result = 0;
    if (parser->candidates.count > 0) {
        walk.buffer = (unsigned char *)cbm_alloc(SVN_MEM, SVN_READ_CHUNK);
#ifndef _WIN32
        walk.root_fd = open(parser->root, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        result =
            walk.buffer && walk.root_fd >= 0 ? walk_candidates(&walk, &parser->candidates) : -1;
        if (walk.root_fd >= 0) {
            (void)close(walk.root_fd);
        }
#else
        result = walk.buffer ? walk_candidates(&walk, &parser->candidates) : -1;
#endif
        cbm_free(SVN_MEM, walk.buffer);
    }
    if (result != 0) {
        return -1;
    }
    observation->content_signature =
        walk.file_count ? hash_mix(walk.xor_value ^ walk.sum_value ^ (uint64_t)walk.file_count) : 0;
    observation->bytes_hashed = walk.bytes_hashed;
    observation->candidate_count = walk.file_count;
    observation->skipped_symlinks = walk.skipped_symlinks;
    observation->skipped_special = walk.skipped_special;
    observation->skipped_unreadable = walk.skipped_unreadable;
    observation->skipped_oversize = walk.skipped_oversize;
    return 0;
}

/* ── Status stream ─────────────────────────────────────────────── */

static bool parse_document(FILE *stream, parser_t *parser, char *tag) {
    size_t tag_length = 0;
    bool in_tag = false;
    char attribute_quote = '\0';
    int character;
    while ((character = fgetc(stream)) != EOF) {
        if (!in_tag) {
            if (character == '<') {
                in_tag = true;
                tag_length = 0;
                attribute_quote = '\0';
            } else if (!isspace((unsigned char)character) && !parser->status_seen) {
                return false;
            }
            continue;
        }
        if (character == '\'' || character == '"') {
            if (attribute_quote == '\0') {
                attribute_quote = (char)character;
            } else if (attribute_quote == character) {
                attribute_quote = '\0';
            }
        }
        if (character == '>' && attribute_quote == '\0') {
            tag[tag_length] = '\0';
            if (process_open_tag(parser, tag) != 0) {
                return false;
            }
            in_tag = false;
            continue;
        }
        if (tag_length + 1 >= SVN_TAG_BUFFER) {
            return false;
        }
        tag[tag_length++] = (char)character;
    }
    return !ferror(stream) && !in_tag && parser->depth == 0 && parser->status_seen &&
           parser->status_closed && parser->target_seen && !parser->entry_open;
}

static bool parser_init(parser_t *parser, const char *root_path, const char *input_root) {
    parser->candidates.seen = cbm_ht_create(32);
    if (!parser->candidates.seen ||
        !cbm_canonical_path(root_path, parser->root, sizeof(parser->root))) {
        return false;
    }
    normalize_separators(parser->root);
    int written = snprintf(parser->input_root, sizeof(parser->input_root), "%s", input_root);
    if (written < 0 || (size_t)written >= sizeof(parser->input_root)) {
        return false;
    }
    normalize_separators(parser->input_root);
    return true;
}

cbm_svn_probe_result_t cbm_svn_parse_status_stream(FILE *stream, const char *root_path,
                                                   const char *status_root,
                                                   cbm_svn_observation_t *observation) {
    if (!stream || !root_path || !observation) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    /* Both are large (the tag buffer alone is 16 KB): keep them off the
     * caller's stack, which on the watcher thread is the default size. */
    parser_t *parser = (parser_t *)cbm_calloc(SVN_MEM, sizeof(*parser));
    char *tag = (char *)cbm_alloc(SVN_MEM, SVN_TAG_BUFFER);
    cbm_svn_probe_result_t result = CBM_SVN_PROBE_UNCERTAIN;
    if (parser && tag && parser_init(parser, root_path, status_root ? status_root : root_path) &&
        parse_document(stream, parser, tag)) {
        if (parser->entry_count == 0) {
            result = CBM_SVN_PROBE_NOT_WORKING_COPY;
        } else {
            cbm_svn_observation_t parsed = {0};
            parsed.semantic_signature = hash_mix(parser->semantic_xor ^ parser->semantic_sum ^
                                                 (uint64_t)parser->entry_count);
            parsed.has_local_changes = parser->has_local_changes;
            parsed.entry_count = parser->entry_count;
            if (fingerprint_candidates(parser, &parsed) == 0) {
                *observation = parsed;
                result = CBM_SVN_PROBE_OK;
            }
        }
    }
    if (parser) {
        candidates_free(&parser->candidates);
    }
    cbm_free(SVN_MEM, tag);
    cbm_free(SVN_MEM, parser);
    return result;
}

/* ── Client resolution ─────────────────────────────────────────── */

static bool executable_is_regular(const char *path) {
#ifdef _WIN32
    wchar_t *wide = cbm_path_to_wide_in(SVN_MEM, path);
    if (!wide) {
        return false;
    }
    DWORD attributes = GetFileAttributesW(wide);
    cbm_free(SVN_MEM, wide);
    return attributes != INVALID_FILE_ATTRIBUTES &&
           (attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0;
#else
    struct stat state;
    return stat(path, &state) == 0 && S_ISREG(state.st_mode) && access(path, X_OK) == 0;
#endif
}

#if defined(_WIN32) || defined(CBM_SVN_STATE_ENABLE_TEST_API)
static bool executable_has_sibling(const char *executable, const char *name) {
    char sibling[CBM_SZ_4K];
    int written = snprintf(sibling, sizeof(sibling), "%s", executable);
    if (written < 0 || (size_t)written >= sizeof(sibling)) {
        return false;
    }
    normalize_separators(sibling);
    char *slash = strrchr(sibling, '/');
    if (!slash) {
        return false;
    }
    size_t used = (size_t)(slash + 1 - sibling);
    written = snprintf(slash + 1, sizeof(sibling) - used, "%s", name);
    if (written < 0 || (size_t)written >= sizeof(sibling) - used) {
        return false;
    }
    cbm_path_info_t information;
    return cbm_path_info_utf8(sibling, &information) == CBM_PATH_INFO_OK && information.is_regular;
}

/* An MSYS2 or Cygwin program loads its POSIX runtime from its own directory;
 * the DLL, not the install prefix, says which path spelling it expects. */
static cbm_svn_path_style_t path_style_for_executable(const char *executable) {
    if (!executable || !executable[0]) {
        return CBM_SVN_PATHS_NATIVE;
    }
    if (executable_has_sibling(executable, "msys-2.0.dll")) {
        return CBM_SVN_PATHS_MSYS;
    }
    if (executable_has_sibling(executable, "cygwin1.dll")) {
        return CBM_SVN_PATHS_CYGWIN;
    }
    return CBM_SVN_PATHS_NATIVE;
}
#endif

#ifdef CBM_SVN_STATE_ENABLE_TEST_API
cbm_svn_path_style_t cbm_svn_test_path_style_for_executable(const char *executable) {
    return path_style_for_executable(executable);
}

void cbm_svn_test_set_after_measure_hook(void (*hook)(const char *relative_path)) {
    g_after_measure_hook = hook;
}
#endif

bool cbm_svn_format_path_arg(const cbm_svn_client_t *client, const char *path, char *out,
                             size_t out_size) {
    if (!client || !path || !out || out_size == 0) {
        return false;
    }
    char normalized[CBM_SZ_4K];
    int written = snprintf(normalized, sizeof(normalized), "%s", path);
    if (written < 0 || (size_t)written >= sizeof(normalized)) {
        return false;
    }
#ifdef _WIN32
    normalize_separators(normalized);
#endif
    bool drive =
        isalpha((unsigned char)normalized[0]) && normalized[1] == ':' && normalized[2] == '/';
    char letter = (char)tolower((unsigned char)normalized[0]);
    if (drive && client->path_style == CBM_SVN_PATHS_MSYS) {
        written = snprintf(out, out_size, "/%c%s", letter, normalized + 2);
    } else if (drive && client->path_style == CBM_SVN_PATHS_CYGWIN) {
        written = snprintf(out, out_size, "/cygdrive/%c%s", letter, normalized + 2);
    } else {
        written = snprintf(out, out_size, "%s", normalized);
    }
    return written >= 0 && (size_t)written < out_size;
}

cbm_svn_probe_result_t cbm_svn_client_init(const char *root_path, cbm_svn_client_t *client) {
    if (!root_path || !client) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    memset(client, 0, sizeof(*client));
    char root[CBM_SZ_4K];
    if (!cbm_canonical_path(root_path, root, sizeof(root))) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    normalize_separators(root);
    char cwd[CBM_SZ_4K];
    if (!cbm_canonical_path(".", cwd, sizeof(cwd))) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    normalize_separators(cwd);

    char path_buffer[CBM_SZ_16K];
    if (!cbm_safe_getenv("PATH", path_buffer, sizeof(path_buffer), NULL)) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    char *cursor = path_buffer;
    while (cursor) {
        char *delimiter = strchr(cursor, SVN_PATH_DELIM);
        if (delimiter) {
            *delimiter = '\0';
        }
        /* Absolute entries only: a relative one ("", ".", "bin") resolves
         * against whatever directory the process happens to be in. */
        if (cursor[0] && path_is_absolute(cursor)) {
            char candidate[CBM_SZ_4K];
            int written = snprintf(candidate, sizeof(candidate), "%s/%s", cursor, SVN_EXE_NAME);
            char resolved[CBM_SZ_4K];
            if (written >= 0 && (size_t)written < sizeof(candidate) &&
                cbm_canonical_path(candidate, resolved, sizeof(resolved))) {
                normalize_separators(resolved);
                if (!path_has_prefix(resolved, root) && !path_has_prefix(resolved, cwd) &&
                    executable_is_regular(resolved)) {
                    snprintf(client->executable, sizeof(client->executable), "%s", resolved);
#ifdef _WIN32
                    client->path_style = path_style_for_executable(resolved);
#endif
                    return CBM_SVN_PROBE_OK;
                }
            }
        }
        cursor = delimiter ? delimiter + 1 : NULL;
    }
    return CBM_SVN_PROBE_UNCERTAIN;
}
