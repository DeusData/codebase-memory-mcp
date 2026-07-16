#include "watcher/svn_state.h"

#include "discover/discover.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/hash_table.h"
#include "foundation/limits.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"
#ifdef _WIN32
#include "foundation/win_utf8.h"
#include <windows.h>
#endif

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#ifndef _WIN32
#include <unistd.h>
#endif

#ifdef _WIN32
#define SVN_PATH_DELIM ';'
#define SVN_EXE_NAME "svn.exe"
#define SVN_NULDEV "NUL"
#else
#define SVN_PATH_DELIM ':'
#define SVN_EXE_NAME "svn"
#define SVN_NULDEV "/dev/null"
#endif

#define SVN_FNV_OFFSET UINT64_C(1469598103934665603)
#define SVN_FNV_PRIME UINT64_C(1099511628211)
#define SVN_MAX_XML_DEPTH 64
#define SVN_MAX_TAG_NAME 96

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

static bool path_char_equal(char left, char right) {
#ifdef _WIN32
    return tolower((unsigned char)left) == tolower((unsigned char)right);
#else
    return left == right;
#endif
}

static void normalize_separators(char *path) {
    cbm_normalize_path_sep(path);
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
    if (!path || path_is_absolute(path)) {
        return false;
    }
    const char *segment = path;
    for (const char *cursor = path;; cursor++) {
        if (*cursor == ':' || *cursor == '\0' || *cursor == '/') {
            size_t length = (size_t)(cursor - segment);
            if (*cursor == ':' || (length == 2 && segment[0] == '.' && segment[1] == '.') ||
                (length == 1 && segment[0] == '.')) {
                return false;
            }
            if (*cursor == '\0') {
                break;
            }
            if (length == 0) {
                return false;
            }
            segment = cursor + 1;
        }
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

static void candidates_free(candidate_list_t *list) {
    if (!list) {
        return;
    }
    for (size_t i = 0; i < list->count; i++) {
        free(list->paths[i]);
    }
    free(list->paths);
    cbm_ht_free(list->seen);
    memset(list, 0, sizeof(*list));
}

static bool candidates_add(candidate_list_t *list, const char *path) {
    if (strcmp(path, ".") == 0 || cbm_ht_has(list->seen, path)) {
        return true;
    }
    char *copy = cbm_strdup(path);
    if (!copy) {
        return false;
    }
    if (list->count == list->capacity) {
        size_t next_capacity = list->capacity ? list->capacity * 2 : 16;
        char **next = (char **)realloc(list->paths, next_capacity * sizeof(*next));
        if (!next) {
            free(copy);
            return false;
        }
        list->paths = next;
        list->capacity = next_capacity;
    }
    list->paths[list->count++] = copy;
    cbm_ht_set(list->seen, copy, copy);
    return true;
}

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

static int safe_stat_path(const char *root, const char *relative, char *absolute,
                          size_t absolute_size, struct stat *state) {
    int written = snprintf(absolute, absolute_size, "%s", root);
    if (written < 0 || (size_t)written >= absolute_size) {
        return -1;
    }
    const char *cursor = relative;
    while (*cursor) {
        const char *slash = strchr(cursor, '/');
        size_t length = slash ? (size_t)(slash - cursor) : strlen(cursor);
        size_t used = strlen(absolute);
        if (used + 1 + length >= absolute_size) {
            return -1;
        }
        absolute[used++] = '/';
        memcpy(absolute + used, cursor, length);
        absolute[used + length] = '\0';
#ifdef _WIN32
        wchar_t *wide = cbm_utf8_to_wide(absolute);
        if (!wide) {
            return -1;
        }
        DWORD attributes = GetFileAttributesW(wide);
        free(wide);
        if (attributes == INVALID_FILE_ATTRIBUTES ||
            (attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0) {
            return -1;
        }
#else
        struct stat component;
        if (lstat(absolute, &component) != 0 || S_ISLNK(component.st_mode)) {
            return -1;
        }
#endif
        cursor = slash ? slash + 1 : cursor + length;
    }
#ifdef _WIN32
    wchar_t *wide = cbm_utf8_to_wide(absolute);
    if (!wide) {
        return -1;
    }
    struct _stat64 wide_state;
    int result = _wstat64(wide, &wide_state);
    free(wide);
    if (result != 0) {
        return -1;
    }
    memset(state, 0, sizeof(*state));
    state->st_mode = wide_state.st_mode;
    state->st_size = wide_state.st_size;
#else
    if (lstat(absolute, state) != 0) {
        return -1;
    }
#endif
    return 0;
}

typedef struct {
    uint64_t xor_value;
    uint64_t sum_value;
    uint64_t bytes_hashed;
    int file_count;
} content_accumulator_t;

static int fingerprint_file(const char *absolute, const char *relative, const struct stat *state,
                            content_accumulator_t *content) {
    if (!S_ISREG(state->st_mode) || state->st_size < 0 ||
        (uint64_t)state->st_size > (uint64_t)cbm_max_file_bytes()) {
        return S_ISREG(state->st_mode) ? 0 : -1;
    }
    const char *name = strrchr(relative, '/');
    name = name ? name + 1 : relative;
    if (cbm_language_for_filename(name) == CBM_LANG_COUNT ||
        cbm_has_ignored_suffix(name, CBM_MODE_FULL)) {
        return 0;
    }

    FILE *file = cbm_fopen(absolute, "rb");
    if (!file) {
        return -1;
    }
    uint64_t file_hash = hash_string(SVN_FNV_OFFSET, relative);
    unsigned char buffer[CBM_SZ_16K];
    size_t total = 0;
    for (;;) {
        size_t count = fread(buffer, 1, sizeof(buffer), file);
        if (count > 0) {
            file_hash = hash_fold(file_hash, buffer, count);
            total += count;
        }
        if (count < sizeof(buffer)) {
            if (ferror(file)) {
                fclose(file);
                return -1;
            }
            break;
        }
    }
    fclose(file);
    content->xor_value ^= hash_mix(file_hash);
    content->sum_value += file_hash * SVN_FNV_PRIME;
    content->bytes_hashed += total;
    content->file_count++;
    return 0;
}

static int fingerprint_tree(const char *root, const char *relative,
                            content_accumulator_t *content) {
    char absolute[CBM_SZ_4K];
    struct stat state;
    if (safe_stat_path(root, relative, absolute, sizeof(absolute), &state) != 0) {
        return -1;
    }
    if (S_ISREG(state.st_mode)) {
        return fingerprint_file(absolute, relative, &state, content);
    }
    if (!S_ISDIR(state.st_mode)) {
        return -1;
    }
    const char *name = strrchr(relative, '/');
    name = name ? name + 1 : relative;
    if (cbm_should_skip_dir(name, CBM_MODE_FULL)) {
        return 0;
    }

    cbm_dir_t *directory = cbm_opendir(absolute);
    if (!directory) {
        return -1;
    }
    int result = 0;
    cbm_dirent_t *entry;
    while (result == 0 && (entry = cbm_readdir(directory)) != NULL) {
        if (strcmp(entry->name, ".") == 0 || strcmp(entry->name, "..") == 0 ||
            strcmp(entry->name, ".svn") == 0) {
            continue;
        }
        if (entry->is_dir && cbm_should_skip_dir(entry->name, CBM_MODE_FULL)) {
            continue;
        }
        char child[CBM_SZ_4K];
        int written = snprintf(child, sizeof(child), "%s/%s", relative, entry->name);
        if (written < 0 || (size_t)written >= sizeof(child)) {
            result = -1;
            break;
        }
        result = fingerprint_tree(root, child, content);
    }
    cbm_closedir(directory);
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

static int fingerprint_candidates(parser_t *parser, cbm_svn_observation_t *observation) {
    qsort(parser->candidates.paths, parser->candidates.count, sizeof(*parser->candidates.paths),
          candidate_compare);
    CBMHashTable *scanned_directories =
        cbm_ht_create((uint32_t)(parser->candidates.count ? parser->candidates.count : 1));
    if (!scanned_directories) {
        return -1;
    }
    content_accumulator_t content = {0};

    for (size_t i = 0; i < parser->candidates.count; i++) {
        const char *relative = parser->candidates.paths[i];
        if (candidate_has_ancestor(scanned_directories, relative)) {
            continue;
        }
        char absolute[CBM_SZ_4K];
        struct stat state;
        if (safe_stat_path(parser->root, relative, absolute, sizeof(absolute), &state) != 0) {
            cbm_ht_free(scanned_directories);
            return -1;
        }
        if (S_ISDIR(state.st_mode)) {
            cbm_ht_set(scanned_directories, relative, parser->candidates.paths[i]);
        }
        if (fingerprint_tree(parser->root, relative, &content) != 0) {
            cbm_ht_free(scanned_directories);
            return -1;
        }
    }
    cbm_ht_free(scanned_directories);

    observation->content_signature =
        content.file_count
            ? hash_mix(content.xor_value ^ content.sum_value ^ (uint64_t)content.file_count)
            : 0;
    observation->bytes_hashed = content.bytes_hashed;
    observation->candidate_count = content.file_count;
    return 0;
}

#ifndef CBM_SVN_STATE_ENABLE_TEST_API
static
#endif
    cbm_svn_probe_result_t
    cbm_svn_parse_status_stream(FILE *stream, const char *root_path,
                                cbm_svn_observation_t *observation) {
    if (!stream || !root_path || !observation) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    parser_t parser = {0};
    parser.candidates.seen = cbm_ht_create(32);
    if (!parser.candidates.seen ||
        !cbm_canonical_path(root_path, parser.root, sizeof(parser.root))) {
        candidates_free(&parser.candidates);
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    normalize_separators(parser.root);
    int input_root_length = snprintf(parser.input_root, sizeof(parser.input_root), "%s", root_path);
    if (input_root_length < 0 || (size_t)input_root_length >= sizeof(parser.input_root)) {
        candidates_free(&parser.candidates);
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    normalize_separators(parser.input_root);

    char tag[CBM_SZ_16K];
    size_t tag_length = 0;
    bool in_tag = false;
    char attribute_quote = '\0';
    bool failed = false;
    int character;
    while ((character = fgetc(stream)) != EOF) {
        if (!in_tag) {
            if (character == '<') {
                in_tag = true;
                tag_length = 0;
                attribute_quote = '\0';
            } else if (!isspace((unsigned char)character) && !parser.status_seen) {
                failed = true;
                break;
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
            if (process_open_tag(&parser, tag) != 0) {
                failed = true;
                break;
            }
            in_tag = false;
            continue;
        }
        if (tag_length + 1 >= sizeof(tag)) {
            failed = true;
            break;
        }
        tag[tag_length++] = (char)character;
    }
    if (ferror(stream) || in_tag || parser.depth != 0 || !parser.status_seen ||
        !parser.status_closed || !parser.target_seen || parser.entry_open) {
        failed = true;
    }

    cbm_svn_probe_result_t result = CBM_SVN_PROBE_UNCERTAIN;
    cbm_svn_observation_t parsed = {0};
    if (!failed && parser.entry_count == 0) {
        result = CBM_SVN_PROBE_NOT_WORKING_COPY;
    } else if (!failed) {
        parsed.semantic_signature =
            hash_mix(parser.semantic_xor ^ parser.semantic_sum ^ (uint64_t)parser.entry_count);
        parsed.has_local_changes = parser.has_local_changes;
        parsed.entry_count = parser.entry_count;
        if (fingerprint_candidates(&parser, &parsed) == 0) {
            *observation = parsed;
            result = CBM_SVN_PROBE_OK;
        }
    }
    candidates_free(&parser.candidates);
    return result;
}

static bool executable_is_regular(const char *path) {
#ifdef _WIN32
    wchar_t *wide = cbm_utf8_to_wide(path);
    if (!wide) {
        return false;
    }
    DWORD attributes = GetFileAttributesW(wide);
    free(wide);
    return attributes != INVALID_FILE_ATTRIBUTES &&
           (attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0;
#else
    struct stat state;
    return stat(path, &state) == 0 && S_ISREG(state.st_mode) && access(path, X_OK) == 0;
#endif
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
        if (cursor[0] && path_is_absolute(cursor)) {
            char candidate[CBM_SZ_4K];
            int written = snprintf(candidate, sizeof(candidate), "%s/%s", cursor, SVN_EXE_NAME);
            if (written >= 0 && (size_t)written < sizeof(candidate)) {
                char resolved[CBM_SZ_4K];
                if (cbm_canonical_path(candidate, resolved, sizeof(resolved))) {
                    normalize_separators(resolved);
                    if (!path_has_prefix(resolved, root) && !path_has_prefix(resolved, cwd) &&
                        executable_is_regular(resolved)) {
                        snprintf(client->executable, sizeof(client->executable), "%s", resolved);
                        return CBM_SVN_PROBE_OK;
                    }
                }
            }
        }
        cursor = delimiter ? delimiter + 1 : NULL;
    }
    return CBM_SVN_PROBE_UNCERTAIN;
}

cbm_svn_probe_result_t cbm_svn_probe(const cbm_svn_client_t *client, const char *root_path,
                                     cbm_svn_observation_t *observation) {
    if (!client || !client->executable[0] || !root_path || !observation ||
        !cbm_validate_shell_arg(client->executable) || !cbm_validate_shell_arg(root_path)) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
#ifdef _WIN32
    /* cbm_popen routes through cmd.exe, where percent expansion occurs even
     * inside double quotes. Fail closed until this leaf can use argv capture. */
    if (strchr(client->executable, '%') || strchr(root_path, '%')) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
#endif
    char command[CBM_SZ_16K];
    int written = snprintf(command, sizeof(command),
                           "\"%s\" status --xml --verbose --no-ignore --depth infinity "
                           "--non-interactive -- \"%s@\" 2>%s",
                           client->executable, root_path, SVN_NULDEV);
    if (written < 0 || (size_t)written >= sizeof(command)) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }

    FILE *process = cbm_popen(command, "r");
    if (!process) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    cbm_svn_observation_t parsed = {0};
    cbm_svn_probe_result_t result = cbm_svn_parse_status_stream(process, root_path, &parsed);
    int exit_code = cbm_pclose(process);
    if (exit_code != 0 || result == CBM_SVN_PROBE_UNCERTAIN) {
        return CBM_SVN_PROBE_UNCERTAIN;
    }
    if (result == CBM_SVN_PROBE_OK) {
        *observation = parsed;
    }
    return result;
}
