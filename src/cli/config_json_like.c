/*
 * config_json_like.c — Structure-preserving edits for JSON, JSONC, and JSON5.
 *
 * This deliberately does not round-trip through a DOM. Agent configuration
 * files are user-owned and often contain comments or hand formatting, so the
 * parser validates the complete document and the editor changes only the
 * selected member bytes (plus an adjacent comma when required).
 */
#include "cli/config_json_like.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include "foundation/win_utf8.h"

#include <fcntl.h>
#include <io.h>
#include <process.h>
#include <sys/stat.h>
#define JL_SYNC _commit
#define JL_PROCESS_ID _getpid
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#define JL_SYNC fsync
#define JL_PROCESS_ID getpid
#endif

#define JL_MAX_DEPTH 64U
#define JL_MAX_KEY_BYTES 4096U
#define JL_MAX_FILE_BYTES (16U * 1024U * 1024U)

static atomic_uint jl_temp_sequence = ATOMIC_VAR_INIT(0);
#ifdef CBM_JSON_LIKE_ENABLE_TEST_API
static CBM_TLS cbm_json_like_precommit_test_hook_t jl_precommit_test_hook = NULL;
static CBM_TLS void *jl_precommit_test_context = NULL;
static CBM_TLS cbm_json_like_precommit_test_hook_t jl_prepublish_test_hook = NULL;
static CBM_TLS void *jl_prepublish_test_context = NULL;
#endif

typedef struct {
    bool exists;
#ifdef _WIN32
    DWORD volume_serial;
    DWORD file_index_high;
    DWORD file_index_low;
    DWORD attributes;
    DWORD link_count;
    FILETIME creation_time;
    FILETIME write_time;
    uint64_t size;
#else
    dev_t device;
    ino_t inode;
    mode_t mode;
    nlink_t link_count;
    uid_t owner;
    gid_t group;
    off_t size;
    int64_t modified_sec;
    long modified_nsec;
    int64_t changed_sec;
    long changed_nsec;
#endif
} jl_file_snapshot_t;

typedef struct {
    const char *text;
    size_t length;
    size_t pos;
    bool strict;
} jl_parser_t;

typedef struct {
    size_t key_start;
    size_t key_end;
    size_t value_start;
    size_t value_end;
    size_t comma_pos;
    size_t previous_comma_pos;
} jl_member_t;

typedef struct {
    size_t close_pos;
    size_t first_key_start;
    size_t last_value_end;
    size_t last_comma_pos;
    size_t member_count;
    size_t match_count;
    bool trailing_comma;
    jl_member_t match;
} jl_object_t;

typedef struct {
    size_t close_pos;
    size_t first_value_start;
    size_t last_value_end;
    size_t last_comma_pos;
    size_t element_count;
    size_t match_count;
    bool trailing_comma;
    jl_member_t match;
} jl_array_t;

typedef struct {
    char *data;
    size_t length;
    size_t capacity;
} jl_buffer_t;

typedef struct {
    size_t start;
    size_t end;
    const char *replacement;
    size_t replacement_length;
} jl_edit_t;

typedef struct {
    const char *data;
    size_t length;
} jl_slice_t;

static uint32_t jl_decode_hex4(const char *text);
static int jl_validate_utf8(const char *text, size_t length);

static bool jl_is_space(unsigned char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v';
}

static size_t jl_line_terminator_width(const char *text, size_t length, size_t pos) {
    if (pos >= length) {
        return 0U;
    }
    unsigned char c = (unsigned char)text[pos];
    if (c == '\n' || c == '\r') {
        return 1U;
    }
    if (length - pos >= 3U && c == 0xE2U && (unsigned char)text[pos + 1U] == 0x80U &&
        ((unsigned char)text[pos + 2U] == 0xA8U || (unsigned char)text[pos + 2U] == 0xA9U)) {
        return 3U;
    }
    return 0U;
}

static size_t jl_whitespace_width(const jl_parser_t *parser) {
    if (parser->pos >= parser->length) {
        return 0U;
    }
    unsigned char c = (unsigned char)parser->text[parser->pos];
    if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
        return 1U;
    }
    if (parser->strict) {
        return 0U;
    }
    if (c == '\f' || c == '\v') {
        return 1U;
    }
    if (parser->length - parser->pos >= 2U && c == 0xC2U &&
        (unsigned char)parser->text[parser->pos + 1U] == 0xA0U) {
        return 2U;
    }
    if (parser->length - parser->pos >= 3U && c == 0xE2U &&
        (unsigned char)parser->text[parser->pos + 1U] == 0x80U &&
        ((unsigned char)parser->text[parser->pos + 2U] == 0xA8U ||
         (unsigned char)parser->text[parser->pos + 2U] == 0xA9U)) {
        return 3U;
    }
    if (parser->length - parser->pos >= 3U && c == 0xEFU &&
        (unsigned char)parser->text[parser->pos + 1U] == 0xBBU &&
        (unsigned char)parser->text[parser->pos + 2U] == 0xBFU) {
        return 3U;
    }
    return 0U;
}

static bool jl_is_hex(unsigned char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

static unsigned jl_hex_value(unsigned char c) {
    if (c >= '0' && c <= '9') {
        return (unsigned)(c - '0');
    }
    if (c >= 'a' && c <= 'f') {
        return (unsigned)(c - 'a') + 10U;
    }
    return (unsigned)(c - 'A') + 10U;
}

static bool jl_is_identifier_start(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '$' || c >= 0x80U;
}

static bool jl_is_identifier_continue(unsigned char c) {
    return jl_is_identifier_start(c) || (c >= '0' && c <= '9');
}

static int jl_skip_trivia(jl_parser_t *parser) {
    while (parser->pos < parser->length) {
        unsigned char c = (unsigned char)parser->text[parser->pos];
        size_t whitespace_width = jl_whitespace_width(parser);
        if (whitespace_width > 0U) {
            parser->pos += whitespace_width;
            continue;
        }
        if (parser->strict || c != '/' || parser->pos + 1U >= parser->length) {
            return 0;
        }

        unsigned char next = (unsigned char)parser->text[parser->pos + 1U];
        if (next == '/') {
            parser->pos += 2U;
            while (parser->pos < parser->length &&
                   jl_line_terminator_width(parser->text, parser->length, parser->pos) == 0U) {
                parser->pos++;
            }
            continue;
        }
        if (next == '*') {
            parser->pos += 2U;
            bool closed = false;
            while (parser->pos + 1U < parser->length) {
                if (parser->text[parser->pos] == '*' && parser->text[parser->pos + 1U] == '/') {
                    parser->pos += 2U;
                    closed = true;
                    break;
                }
                parser->pos++;
            }
            if (!closed) {
                return -1;
            }
            continue;
        }
        return 0;
    }
    return 0;
}

static int jl_parse_string(jl_parser_t *parser, size_t *start_out, size_t *end_out) {
    if (parser->pos >= parser->length) {
        return -1;
    }
    unsigned char quote = (unsigned char)parser->text[parser->pos];
    if (quote != '"' && (parser->strict || quote != '\'')) {
        return -1;
    }

    size_t start = parser->pos++;
    while (parser->pos < parser->length) {
        unsigned char c = (unsigned char)parser->text[parser->pos++];
        if (c == quote) {
            if (start_out) {
                *start_out = start;
            }
            if (end_out) {
                *end_out = parser->pos;
            }
            return 0;
        }
        if (c < 0x20U) {
            return -1;
        }
        if (c != '\\') {
            if (!parser->strict && c == 0xE2U && parser->length - parser->pos >= 2U &&
                (unsigned char)parser->text[parser->pos] == 0x80U &&
                ((unsigned char)parser->text[parser->pos + 1U] == 0xA8U ||
                 (unsigned char)parser->text[parser->pos + 1U] == 0xA9U)) {
                return -1;
            }
            continue;
        }
        if (parser->pos >= parser->length) {
            return -1;
        }

        unsigned char escaped = (unsigned char)parser->text[parser->pos++];
        if (escaped == 'u') {
            if (parser->length - parser->pos < 4U) {
                return -1;
            }
            for (size_t i = 0; i < 4U; i++) {
                if (!jl_is_hex((unsigned char)parser->text[parser->pos + i])) {
                    return -1;
                }
            }
            parser->pos += 4U;
            continue;
        }
        if (!parser->strict && escaped == 'x') {
            if (parser->length - parser->pos < 2U ||
                !jl_is_hex((unsigned char)parser->text[parser->pos]) ||
                !jl_is_hex((unsigned char)parser->text[parser->pos + 1U])) {
                return -1;
            }
            parser->pos += 2U;
            continue;
        }
        if (!parser->strict && (escaped == '\n' || escaped == '\r')) {
            if (escaped == '\r' && parser->pos < parser->length &&
                parser->text[parser->pos] == '\n') {
                parser->pos++;
            }
            continue;
        }
        if (!parser->strict && escaped == 0xE2U && parser->length - parser->pos >= 2U &&
            (unsigned char)parser->text[parser->pos] == 0x80U &&
            ((unsigned char)parser->text[parser->pos + 1U] == 0xA8U ||
             (unsigned char)parser->text[parser->pos + 1U] == 0xA9U)) {
            parser->pos += 2U;
            continue;
        }
        if (!parser->strict &&
            ((escaped >= '1' && escaped <= '9') ||
             (escaped == '0' && parser->pos < parser->length && parser->text[parser->pos] >= '0' &&
              parser->text[parser->pos] <= '9'))) {
            return -1;
        }
        if (parser->strict && !strchr("\"\\/bfnrt", (int)escaped)) {
            return -1;
        }
        if (escaped < 0x20U) {
            return -1;
        }
    }
    return -1;
}

static int jl_parse_identifier(jl_parser_t *parser, size_t *start_out, size_t *end_out) {
    size_t start = parser->pos;
    bool first = true;
    while (parser->pos < parser->length) {
        unsigned char c = (unsigned char)parser->text[parser->pos];
        bool accepted = first ? jl_is_identifier_start(c) : jl_is_identifier_continue(c);
        if (accepted) {
            parser->pos++;
            first = false;
            continue;
        }
        if (c == '\\' && parser->length - parser->pos >= 6U &&
            parser->text[parser->pos + 1U] == 'u') {
            bool valid_escape = true;
            for (size_t i = 0; i < 4U; i++) {
                if (!jl_is_hex((unsigned char)parser->text[parser->pos + 2U + i])) {
                    valid_escape = false;
                    break;
                }
            }
            if (valid_escape) {
                uint32_t codepoint = jl_decode_hex4(parser->text + parser->pos + 2U);
                valid_escape = codepoint >= 0x80U ||
                               (first ? jl_is_identifier_start((unsigned char)codepoint)
                                      : jl_is_identifier_continue((unsigned char)codepoint));
                if (codepoint >= 0xD800U && codepoint <= 0xDFFFU) {
                    valid_escape = false;
                }
            }
            if (valid_escape) {
                parser->pos += 6U;
                first = false;
                continue;
            }
        }
        break;
    }
    if (first) {
        return -1;
    }
    if (start_out) {
        *start_out = start;
    }
    if (end_out) {
        *end_out = parser->pos;
    }
    return 0;
}

static int jl_parse_key(jl_parser_t *parser, size_t *start_out, size_t *end_out) {
    if (parser->pos >= parser->length) {
        return -1;
    }
    unsigned char c = (unsigned char)parser->text[parser->pos];
    if (c == '"' || (!parser->strict && c == '\'')) {
        return jl_parse_string(parser, start_out, end_out);
    }
    if (!parser->strict) {
        return jl_parse_identifier(parser, start_out, end_out);
    }
    return -1;
}

static bool jl_has_prefix(const jl_parser_t *parser, const char *word) {
    size_t word_length = strlen(word);
    return parser->length - parser->pos >= word_length &&
           memcmp(parser->text + parser->pos, word, word_length) == 0;
}

static int jl_parse_number(jl_parser_t *parser) {
    size_t pos = parser->pos;
    if (pos < parser->length &&
        (parser->text[pos] == '-' || (!parser->strict && parser->text[pos] == '+'))) {
        pos++;
    }
    if (!parser->strict && parser->length - pos >= 8U &&
        memcmp(parser->text + pos, "Infinity", 8U) == 0) {
        parser->pos = pos + 8U;
        return 0;
    }
    if (!parser->strict && parser->length - pos >= 3U &&
        memcmp(parser->text + pos, "NaN", 3U) == 0) {
        parser->pos = pos + 3U;
        return 0;
    }
    if (!parser->strict && parser->length - pos >= 3U && parser->text[pos] == '0' &&
        (parser->text[pos + 1U] == 'x' || parser->text[pos + 1U] == 'X')) {
        pos += 2U;
        size_t digits = pos;
        while (pos < parser->length && jl_is_hex((unsigned char)parser->text[pos])) {
            pos++;
        }
        if (pos == digits) {
            return -1;
        }
        parser->pos = pos;
        return 0;
    }

    bool integer_digits = false;
    if (pos < parser->length && parser->text[pos] == '0') {
        integer_digits = true;
        pos++;
        if (parser->strict && pos < parser->length && parser->text[pos] >= '0' &&
            parser->text[pos] <= '9') {
            return -1;
        }
    } else {
        while (pos < parser->length && parser->text[pos] >= '0' && parser->text[pos] <= '9') {
            pos++;
            integer_digits = true;
        }
    }
    if (parser->strict && !integer_digits) {
        return -1;
    }

    bool fraction_digits = false;
    if (pos < parser->length && parser->text[pos] == '.') {
        pos++;
        while (pos < parser->length && parser->text[pos] >= '0' && parser->text[pos] <= '9') {
            pos++;
            fraction_digits = true;
        }
        if (parser->strict && !fraction_digits) {
            return -1;
        }
    }
    if (!integer_digits && !fraction_digits) {
        return -1;
    }

    if (pos < parser->length && (parser->text[pos] == 'e' || parser->text[pos] == 'E')) {
        pos++;
        if (pos < parser->length && (parser->text[pos] == '+' || parser->text[pos] == '-')) {
            pos++;
        }
        size_t exponent_start = pos;
        while (pos < parser->length && parser->text[pos] >= '0' && parser->text[pos] <= '9') {
            pos++;
        }
        if (pos == exponent_start) {
            return -1;
        }
    }
    parser->pos = pos;
    return 0;
}

static int jl_parse_value(jl_parser_t *parser, unsigned depth, size_t *start_out, size_t *end_out);

static int jl_parse_object(jl_parser_t *parser, unsigned depth) {
    if (depth >= JL_MAX_DEPTH || parser->pos >= parser->length ||
        parser->text[parser->pos] != '{') {
        return -1;
    }
    parser->pos++;
    if (jl_skip_trivia(parser) != 0) {
        return -1;
    }
    if (parser->pos < parser->length && parser->text[parser->pos] == '}') {
        parser->pos++;
        return 0;
    }

    while (parser->pos < parser->length) {
        if (jl_parse_key(parser, NULL, NULL) != 0 || jl_skip_trivia(parser) != 0 ||
            parser->pos >= parser->length || parser->text[parser->pos] != ':') {
            return -1;
        }
        parser->pos++;
        if (jl_skip_trivia(parser) != 0 || jl_parse_value(parser, depth + 1U, NULL, NULL) != 0 ||
            jl_skip_trivia(parser) != 0 || parser->pos >= parser->length) {
            return -1;
        }
        if (parser->text[parser->pos] == '}') {
            parser->pos++;
            return 0;
        }
        if (parser->text[parser->pos] != ',') {
            return -1;
        }
        parser->pos++;
        if (jl_skip_trivia(parser) != 0 || parser->pos >= parser->length) {
            return -1;
        }
        if (parser->text[parser->pos] == '}') {
            if (parser->strict) {
                return -1;
            }
            parser->pos++;
            return 0;
        }
    }
    return -1;
}

static int jl_parse_array(jl_parser_t *parser, unsigned depth) {
    if (depth >= JL_MAX_DEPTH || parser->pos >= parser->length ||
        parser->text[parser->pos] != '[') {
        return -1;
    }
    parser->pos++;
    if (jl_skip_trivia(parser) != 0) {
        return -1;
    }
    if (parser->pos < parser->length && parser->text[parser->pos] == ']') {
        parser->pos++;
        return 0;
    }
    while (parser->pos < parser->length) {
        if (jl_parse_value(parser, depth + 1U, NULL, NULL) != 0 || jl_skip_trivia(parser) != 0 ||
            parser->pos >= parser->length) {
            return -1;
        }
        if (parser->text[parser->pos] == ']') {
            parser->pos++;
            return 0;
        }
        if (parser->text[parser->pos] != ',') {
            return -1;
        }
        parser->pos++;
        if (jl_skip_trivia(parser) != 0 || parser->pos >= parser->length) {
            return -1;
        }
        if (parser->text[parser->pos] == ']') {
            if (parser->strict) {
                return -1;
            }
            parser->pos++;
            return 0;
        }
    }
    return -1;
}

static int jl_parse_value(jl_parser_t *parser, unsigned depth, size_t *start_out, size_t *end_out) {
    if (depth > JL_MAX_DEPTH || parser->pos >= parser->length) {
        return -1;
    }
    size_t start = parser->pos;
    unsigned char c = (unsigned char)parser->text[parser->pos];
    int result = -1;
    if (c == '{') {
        result = jl_parse_object(parser, depth);
    } else if (c == '[') {
        result = jl_parse_array(parser, depth);
    } else if (c == '"' || (!parser->strict && c == '\'')) {
        result = jl_parse_string(parser, NULL, NULL);
    } else if (jl_has_prefix(parser, "true")) {
        parser->pos += 4U;
        result = 0;
    } else if (jl_has_prefix(parser, "false")) {
        parser->pos += 5U;
        result = 0;
    } else if (jl_has_prefix(parser, "null")) {
        parser->pos += 4U;
        result = 0;
    } else {
        result = jl_parse_number(parser);
    }
    if (result != 0) {
        return -1;
    }
    if (start_out) {
        *start_out = start;
    }
    if (end_out) {
        *end_out = parser->pos;
    }
    return 0;
}

static int jl_validate_document(const char *text, size_t length, size_t *root_out) {
    if (jl_validate_utf8(text, length) != 0) {
        return -1;
    }
    jl_parser_t parser = {.text = text, .length = length, .pos = 0, .strict = false};
    if (jl_skip_trivia(&parser) != 0 || parser.pos >= parser.length ||
        parser.text[parser.pos] != '{') {
        return -1;
    }
    size_t root = parser.pos;
    if (jl_parse_value(&parser, 0, NULL, NULL) != 0 || jl_skip_trivia(&parser) != 0 ||
        parser.pos != parser.length) {
        return -1;
    }
    *root_out = root;
    return 0;
}

static int jl_validate_entry(const char *entry_json, size_t *start_out, size_t *length_out) {
    size_t length = strlen(entry_json);
    if (length == 0 || length > JL_MAX_FILE_BYTES || jl_validate_utf8(entry_json, length) != 0) {
        return -1;
    }
    jl_parser_t parser = {
        .text = entry_json,
        .length = length,
        .pos = 0,
        .strict = true,
    };
    if (jl_skip_trivia(&parser) != 0 || parser.pos >= parser.length) {
        return -1;
    }
    size_t value_start = 0;
    size_t value_end = 0;
    if (jl_parse_value(&parser, 0, &value_start, &value_end) != 0 || jl_skip_trivia(&parser) != 0 ||
        parser.pos != parser.length) {
        return -1;
    }
    *start_out = value_start;
    *length_out = value_end - value_start;
    return 0;
}

static size_t jl_encode_utf8(uint32_t codepoint, unsigned char output[4]) {
    if (codepoint <= 0x7FU) {
        output[0] = (unsigned char)codepoint;
        return 1U;
    }
    if (codepoint <= 0x7FFU) {
        output[0] = (unsigned char)(0xC0U | (codepoint >> 6U));
        output[1] = (unsigned char)(0x80U | (codepoint & 0x3FU));
        return 2U;
    }
    if (codepoint <= 0xFFFFU) {
        output[0] = (unsigned char)(0xE0U | (codepoint >> 12U));
        output[1] = (unsigned char)(0x80U | ((codepoint >> 6U) & 0x3FU));
        output[2] = (unsigned char)(0x80U | (codepoint & 0x3FU));
        return 3U;
    }
    output[0] = (unsigned char)(0xF0U | (codepoint >> 18U));
    output[1] = (unsigned char)(0x80U | ((codepoint >> 12U) & 0x3FU));
    output[2] = (unsigned char)(0x80U | ((codepoint >> 6U) & 0x3FU));
    output[3] = (unsigned char)(0x80U | (codepoint & 0x3FU));
    return 4U;
}

static uint32_t jl_decode_hex4(const char *text) {
    uint32_t value = 0;
    for (size_t i = 0; i < 4U; i++) {
        value = (value << 4U) | jl_hex_value((unsigned char)text[i]);
    }
    return value;
}

static bool jl_match_output(const unsigned char *bytes, size_t byte_count, const char *target,
                            size_t target_length, size_t *target_pos) {
    if (target_length - *target_pos < byte_count ||
        memcmp(target + *target_pos, bytes, byte_count) != 0) {
        return false;
    }
    *target_pos += byte_count;
    return true;
}

static bool jl_key_equals(const char *text, size_t start, size_t end, const char *target) {
    size_t target_length = strlen(target);
    size_t target_pos = 0;
    bool quoted = end > start + 1U && (text[start] == '"' || text[start] == '\'');
    size_t pos = quoted ? start + 1U : start;
    size_t limit = quoted ? end - 1U : end;

    while (pos < limit) {
        unsigned char c = (unsigned char)text[pos++];
        if (c != '\\') {
            if (!jl_match_output(&c, 1U, target, target_length, &target_pos)) {
                return false;
            }
            continue;
        }
        if (pos >= limit) {
            return false;
        }
        unsigned char escaped = (unsigned char)text[pos++];
        if (escaped == '\n' || escaped == '\r') {
            if (escaped == '\r' && pos < limit && text[pos] == '\n') {
                pos++;
            }
            continue;
        }

        uint32_t codepoint = escaped;
        switch (escaped) {
        case 'b':
            codepoint = '\b';
            break;
        case 'f':
            codepoint = '\f';
            break;
        case 'n':
            codepoint = '\n';
            break;
        case 'r':
            codepoint = '\r';
            break;
        case 't':
            codepoint = '\t';
            break;
        case 'v':
            codepoint = '\v';
            break;
        case '0':
            codepoint = 0;
            break;
        case 'x':
            if (limit - pos < 2U) {
                return false;
            }
            codepoint = (jl_hex_value((unsigned char)text[pos]) << 4U) |
                        jl_hex_value((unsigned char)text[pos + 1U]);
            pos += 2U;
            break;
        case 'u':
            if (limit - pos < 4U) {
                return false;
            }
            codepoint = jl_decode_hex4(text + pos);
            pos += 4U;
            if (codepoint >= 0xD800U && codepoint <= 0xDBFFU && limit - pos >= 6U &&
                text[pos] == '\\' && text[pos + 1U] == 'u') {
                uint32_t low = jl_decode_hex4(text + pos + 2U);
                if (low >= 0xDC00U && low <= 0xDFFFU) {
                    codepoint = 0x10000U + ((codepoint - 0xD800U) << 10U) + (low - 0xDC00U);
                    pos += 6U;
                }
            }
            break;
        default:
            break;
        }
        unsigned char encoded[4];
        size_t encoded_length = jl_encode_utf8(codepoint, encoded);
        if (!jl_match_output(encoded, encoded_length, target, target_length, &target_pos)) {
            return false;
        }
    }
    return target_pos == target_length;
}

static int jl_scan_object(const char *text, size_t length, size_t object_start,
                          const char *lookup_key, jl_object_t *object) {
    memset(object, 0, sizeof(*object));
    object->first_key_start = SIZE_MAX;
    object->last_value_end = SIZE_MAX;
    object->last_comma_pos = SIZE_MAX;
    object->match.comma_pos = SIZE_MAX;
    object->match.previous_comma_pos = SIZE_MAX;

    if (object_start >= length || text[object_start] != '{') {
        return -1;
    }
    jl_parser_t parser = {
        .text = text, .length = length, .pos = object_start + 1U, .strict = false};
    if (jl_skip_trivia(&parser) != 0 || parser.pos >= parser.length) {
        return -1;
    }
    if (text[parser.pos] == '}') {
        object->close_pos = parser.pos;
        return 0;
    }

    size_t previous_comma = SIZE_MAX;
    while (parser.pos < parser.length) {
        size_t key_start = 0;
        size_t key_end = 0;
        if (jl_parse_key(&parser, &key_start, &key_end) != 0) {
            return -1;
        }
        if (object->first_key_start == SIZE_MAX) {
            object->first_key_start = key_start;
        }
        if (jl_skip_trivia(&parser) != 0 || parser.pos >= parser.length ||
            text[parser.pos] != ':') {
            return -1;
        }
        parser.pos++;
        if (jl_skip_trivia(&parser) != 0 || parser.pos >= parser.length) {
            return -1;
        }
        size_t value_start = 0;
        size_t value_end = 0;
        if (jl_parse_value(&parser, 1U, &value_start, &value_end) != 0 ||
            jl_skip_trivia(&parser) != 0 || parser.pos >= parser.length) {
            return -1;
        }

        bool matches = jl_key_equals(text, key_start, key_end, lookup_key);
        if (matches) {
            object->match_count++;
            if (object->match_count == 1U) {
                object->match.key_start = key_start;
                object->match.key_end = key_end;
                object->match.value_start = value_start;
                object->match.value_end = value_end;
                object->match.previous_comma_pos = previous_comma;
            }
        }

        object->member_count++;
        object->last_value_end = value_end;
        if (text[parser.pos] == '}') {
            object->close_pos = parser.pos;
            return 0;
        }
        if (text[parser.pos] != ',') {
            return -1;
        }
        size_t comma_pos = parser.pos++;
        object->last_comma_pos = comma_pos;
        if (matches && object->match_count == 1U) {
            object->match.comma_pos = comma_pos;
        }
        previous_comma = comma_pos;
        if (jl_skip_trivia(&parser) != 0 || parser.pos >= parser.length) {
            return -1;
        }
        if (text[parser.pos] == '}') {
            object->close_pos = parser.pos;
            object->trailing_comma = true;
            return 0;
        }
    }
    return -1;
}

static int jl_scan_array(const char *text, size_t length, size_t array_start,
                         const char *lookup_string, jl_array_t *array) {
    memset(array, 0, sizeof(*array));
    array->first_value_start = SIZE_MAX;
    array->last_value_end = SIZE_MAX;
    array->last_comma_pos = SIZE_MAX;
    array->match.comma_pos = SIZE_MAX;
    array->match.previous_comma_pos = SIZE_MAX;

    if (array_start >= length || text[array_start] != '[') {
        return -1;
    }
    jl_parser_t parser = {
        .text = text,
        .length = length,
        .pos = array_start + 1U,
        .strict = false,
    };
    if (jl_skip_trivia(&parser) != 0 || parser.pos >= parser.length) {
        return -1;
    }
    if (text[parser.pos] == ']') {
        array->close_pos = parser.pos;
        return 0;
    }

    size_t previous_comma = SIZE_MAX;
    while (parser.pos < parser.length) {
        size_t value_start = 0;
        size_t value_end = 0;
        if (jl_parse_value(&parser, 1U, &value_start, &value_end) != 0) {
            return -1;
        }
        if (array->first_value_start == SIZE_MAX) {
            array->first_value_start = value_start;
        }
        bool is_string = text[value_start] == '"' || text[value_start] == '\'';
        bool matches = is_string && jl_key_equals(text, value_start, value_end, lookup_string);
        if (matches) {
            array->match_count++;
            if (array->match_count == 1U) {
                array->match.value_start = value_start;
                array->match.value_end = value_end;
                array->match.previous_comma_pos = previous_comma;
            }
        }

        array->element_count++;
        array->last_value_end = value_end;
        if (jl_skip_trivia(&parser) != 0 || parser.pos >= parser.length) {
            return -1;
        }
        if (text[parser.pos] == ']') {
            array->close_pos = parser.pos;
            return 0;
        }
        if (text[parser.pos] != ',') {
            return -1;
        }
        size_t comma_pos = parser.pos++;
        array->last_comma_pos = comma_pos;
        if (matches && array->match_count == 1U) {
            array->match.comma_pos = comma_pos;
        }
        previous_comma = comma_pos;
        if (jl_skip_trivia(&parser) != 0 || parser.pos >= parser.length) {
            return -1;
        }
        if (text[parser.pos] == ']') {
            array->close_pos = parser.pos;
            array->trailing_comma = true;
            return 0;
        }
    }
    return -1;
}

static int jl_buffer_reserve(jl_buffer_t *buffer, size_t additional) {
    if (additional > SIZE_MAX - buffer->length - 1U) {
        return -1;
    }
    size_t needed = buffer->length + additional + 1U;
    if (needed <= buffer->capacity) {
        return 0;
    }
    size_t capacity = buffer->capacity ? buffer->capacity : 128U;
    while (capacity < needed) {
        if (capacity > SIZE_MAX / 2U) {
            capacity = needed;
            break;
        }
        capacity *= 2U;
    }
    char *grown = realloc(buffer->data, capacity);
    if (!grown) {
        return -1;
    }
    buffer->data = grown;
    buffer->capacity = capacity;
    return 0;
}

static int jl_buffer_append(jl_buffer_t *buffer, const char *data, size_t length) {
    if (jl_buffer_reserve(buffer, length) != 0) {
        return -1;
    }
    if (length > 0) {
        memcpy(buffer->data + buffer->length, data, length);
    }
    buffer->length += length;
    buffer->data[buffer->length] = '\0';
    return 0;
}

static int jl_buffer_char(jl_buffer_t *buffer, char c) {
    return jl_buffer_append(buffer, &c, 1U);
}

static int jl_buffer_string(jl_buffer_t *buffer, const char *text) {
    return jl_buffer_append(buffer, text, strlen(text));
}

static int jl_append_quoted_string(jl_buffer_t *buffer, const char *value) {
    static const char hex[] = "0123456789abcdef";
    if (jl_buffer_char(buffer, '"') != 0) {
        return -1;
    }
    for (const unsigned char *pos = (const unsigned char *)value; *pos; pos++) {
        if (*pos == '"' || *pos == '\\') {
            if (jl_buffer_char(buffer, '\\') != 0) {
                return -1;
            }
        } else if (*pos == '\b' || *pos == '\f' || *pos == '\n' || *pos == '\r' || *pos == '\t') {
            char escaped = 'b';
            if (*pos == '\f') {
                escaped = 'f';
            } else if (*pos == '\n') {
                escaped = 'n';
            } else if (*pos == '\r') {
                escaped = 'r';
            } else if (*pos == '\t') {
                escaped = 't';
            }
            if (jl_buffer_char(buffer, '\\') != 0 || jl_buffer_char(buffer, escaped) != 0) {
                return -1;
            }
            continue;
        } else if (*pos < 0x20U) {
            char escaped[6] = {'\\', 'u', '0', '0', hex[*pos >> 4U], hex[*pos & 0x0FU]};
            if (jl_buffer_append(buffer, escaped, sizeof(escaped)) != 0) {
                return -1;
            }
            continue;
        }
        if (jl_buffer_char(buffer, (char)*pos) != 0) {
            return -1;
        }
    }
    return jl_buffer_char(buffer, '"');
}

static int jl_append_quoted_key(jl_buffer_t *buffer, const char *key) {
    return jl_append_quoted_string(buffer, key);
}

static int jl_append_repeated(jl_buffer_t *buffer, const char *data, size_t length, size_t count) {
    for (size_t i = 0; i < count; i++) {
        if (jl_buffer_append(buffer, data, length) != 0) {
            return -1;
        }
    }
    return 0;
}

static int jl_append_styled_indent(jl_buffer_t *buffer, jl_slice_t base, jl_slice_t unit,
                                   size_t unit_count) {
    return jl_buffer_append(buffer, base.data, base.length) == 0 &&
                   jl_append_repeated(buffer, unit.data, unit.length, unit_count) == 0
               ? 0
               : -1;
}

static int jl_build_member(jl_buffer_t *buffer, const char *const *object_path, size_t path_len,
                           size_t missing_path_index, const char *entry_key, const char *entry_json,
                           size_t entry_length, const char *newline, jl_slice_t base_indent,
                           jl_slice_t indent_unit) {
    if (missing_path_index == SIZE_MAX) {
        if (jl_append_quoted_key(buffer, entry_key) != 0 || jl_buffer_string(buffer, ": ") != 0 ||
            jl_buffer_append(buffer, entry_json, entry_length) != 0) {
            return -1;
        }
        return 0;
    }

    if (jl_append_quoted_key(buffer, object_path[missing_path_index]) != 0 ||
        jl_buffer_string(buffer, ": {") != 0 || jl_buffer_string(buffer, newline) != 0) {
        return -1;
    }
    size_t level = 1U;
    for (size_t i = missing_path_index + 1U; i < path_len; i++) {
        if (jl_append_styled_indent(buffer, base_indent, indent_unit, level + 1U) != 0 ||
            jl_append_quoted_key(buffer, object_path[i]) != 0 ||
            jl_buffer_string(buffer, ": {") != 0 || jl_buffer_string(buffer, newline) != 0) {
            return -1;
        }
        level++;
    }
    if (jl_append_styled_indent(buffer, base_indent, indent_unit, level + 1U) != 0 ||
        jl_append_quoted_key(buffer, entry_key) != 0 || jl_buffer_string(buffer, ": ") != 0 ||
        jl_buffer_append(buffer, entry_json, entry_length) != 0) {
        return -1;
    }
    while (level > 0U) {
        if (jl_buffer_string(buffer, newline) != 0 ||
            jl_append_styled_indent(buffer, base_indent, indent_unit, level) != 0 ||
            jl_buffer_char(buffer, '}') != 0) {
            return -1;
        }
        level--;
    }
    return 0;
}

static bool jl_line_indent(const char *text, size_t pos, jl_slice_t *indent) {
    size_t line_start = pos;
    while (line_start > 0U && text[line_start - 1U] != '\n' && text[line_start - 1U] != '\r') {
        line_start--;
    }
    for (size_t i = line_start; i < pos; i++) {
        if (text[i] != ' ' && text[i] != '\t') {
            indent->data = text + pos;
            indent->length = 0;
            return false;
        }
    }
    indent->data = text + line_start;
    indent->length = pos - line_start;
    return true;
}

static const char *jl_newline_style(const char *text, size_t length) {
    for (size_t i = 0; i < length; i++) {
        if (text[i] == '\n') {
            return i > 0U && text[i - 1U] == '\r' ? "\r\n" : "\n";
        }
        if (text[i] == '\r') {
            return i + 1U < length && text[i + 1U] == '\n' ? "\r\n" : "\n";
        }
    }
    return "\n";
}

static bool jl_range_has_newline(const char *text, size_t start, size_t end) {
    for (size_t i = start; i < end; i++) {
        if (text[i] == '\n' || text[i] == '\r') {
            return true;
        }
    }
    return false;
}

static size_t jl_leading_whitespace_start(const char *text, size_t token_start) {
    while (token_start > 0U && (text[token_start - 1U] == ' ' || text[token_start - 1U] == '\t')) {
        token_start--;
    }
    size_t newline_start = token_start;
    if (newline_start > 0U && text[newline_start - 1U] == '\n') {
        newline_start--;
        if (newline_start > 0U && text[newline_start - 1U] == '\r') {
            newline_start--;
        }
    } else if (newline_start > 0U && text[newline_start - 1U] == '\r') {
        newline_start--;
    }
    if (newline_start < token_start && newline_start > 0U) {
        char preceding = text[newline_start - 1U];
        if (preceding == '{' || preceding == '[' || preceding == ',') {
            return newline_start;
        }
    }
    return token_start;
}

static void jl_detect_indent(const char *text, const jl_object_t *object, jl_slice_t *base,
                             jl_slice_t *unit, bool *close_indent_only) {
    static const char spaces[] = "  ";
    static const char tab[] = "\t";
    *close_indent_only = jl_line_indent(text, object->close_pos, base);
    unit->data = spaces;
    unit->length = 2U;

    if (object->first_key_start == SIZE_MAX) {
        if (base->length > 0U && base->data[base->length - 1U] == '\t') {
            unit->data = tab;
            unit->length = 1U;
        }
        return;
    }
    jl_slice_t member_indent;
    if (!jl_line_indent(text, object->first_key_start, &member_indent)) {
        return;
    }
    if (member_indent.length > base->length &&
        (base->length == 0U || memcmp(member_indent.data, base->data, base->length) == 0)) {
        unit->data = member_indent.data + base->length;
        unit->length = member_indent.length - base->length;
    } else if (member_indent.length > 0U && member_indent.data[member_indent.length - 1U] == '\t') {
        unit->data = tab;
        unit->length = 1U;
    }
}

static int jl_make_insertion(const char *text, size_t length, size_t object_start,
                             const jl_object_t *object, const jl_buffer_t *member,
                             jl_buffer_t *insertion) {
    jl_slice_t base;
    jl_slice_t unit;
    bool close_indent_only = false;
    jl_detect_indent(text, object, &base, &unit, &close_indent_only);
    const char *newline = jl_newline_style(text, length);

    size_t gap_start = object_start + 1U;
    if (object->member_count > 0U) {
        gap_start = object->trailing_comma ? object->last_comma_pos + 1U : object->last_value_end;
    }
    bool multiline = jl_range_has_newline(text, gap_start, object->close_pos);
    bool empty_tight = object->member_count == 0U && gap_start == object->close_pos;

    if (empty_tight) {
        if (jl_buffer_string(insertion, newline) != 0 ||
            jl_append_styled_indent(insertion, base, unit, 1U) != 0 ||
            jl_buffer_append(insertion, member->data, member->length) != 0 ||
            jl_buffer_string(insertion, newline) != 0 ||
            jl_buffer_append(insertion, base.data, base.length) != 0) {
            return -1;
        }
        return 0;
    }

    if (multiline) {
        if (!close_indent_only && jl_buffer_string(insertion, newline) != 0) {
            return -1;
        }
        if (jl_buffer_append(insertion, unit.data, unit.length) != 0 ||
            jl_buffer_append(insertion, member->data, member->length) != 0) {
            return -1;
        }
        if (object->trailing_comma && jl_buffer_char(insertion, ',') != 0) {
            return -1;
        }
        if (jl_buffer_string(insertion, newline) != 0 ||
            jl_buffer_append(insertion, base.data, base.length) != 0) {
            return -1;
        }
        return 0;
    }

    if (object->close_pos == gap_start ||
        !jl_is_space((unsigned char)text[object->close_pos - 1U])) {
        if (jl_buffer_char(insertion, ' ') != 0) {
            return -1;
        }
    }
    if (jl_buffer_append(insertion, member->data, member->length) != 0) {
        return -1;
    }
    if (object->trailing_comma && jl_buffer_char(insertion, ',') != 0) {
        return -1;
    }
    return jl_buffer_char(insertion, ' ');
}

static int jl_apply_edits(const char *source, size_t source_length, jl_edit_t *edits,
                          size_t edit_count, char **result_out, size_t *result_length_out) {
    if (edit_count == 2U && edits[1].start < edits[0].start) {
        jl_edit_t swap = edits[0];
        edits[0] = edits[1];
        edits[1] = swap;
    }
    size_t result_length = source_length;
    size_t previous_end = 0;
    for (size_t i = 0; i < edit_count; i++) {
        if (edits[i].start < previous_end || edits[i].end < edits[i].start ||
            edits[i].end > source_length) {
            return -1;
        }
        size_t removed = edits[i].end - edits[i].start;
        if (removed > result_length) {
            return -1;
        }
        result_length -= removed;
        if (edits[i].replacement_length > SIZE_MAX - result_length) {
            return -1;
        }
        result_length += edits[i].replacement_length;
        previous_end = edits[i].end;
    }
    if (result_length > JL_MAX_FILE_BYTES || result_length == SIZE_MAX) {
        return -1;
    }

    char *result = malloc(result_length + 1U);
    if (!result) {
        return -1;
    }
    size_t source_pos = 0;
    size_t result_pos = 0;
    for (size_t i = 0; i < edit_count; i++) {
        size_t prefix_length = edits[i].start - source_pos;
        memcpy(result + result_pos, source + source_pos, prefix_length);
        result_pos += prefix_length;
        if (edits[i].replacement_length > 0U) {
            memcpy(result + result_pos, edits[i].replacement, edits[i].replacement_length);
            result_pos += edits[i].replacement_length;
        }
        source_pos = edits[i].end;
    }
    memcpy(result + result_pos, source + source_pos, source_length - source_pos);
    result_pos += source_length - source_pos;
    result[result_pos] = '\0';
    *result_out = result;
    *result_length_out = result_pos;
    return 0;
}

static bool jl_snapshot_state_equal(const jl_file_snapshot_t *left,
                                    const jl_file_snapshot_t *right) {
    if (left->exists != right->exists) {
        return false;
    }
    if (!left->exists) {
        return true;
    }
#ifdef _WIN32
    return left->volume_serial == right->volume_serial &&
           left->file_index_high == right->file_index_high &&
           left->file_index_low == right->file_index_low && left->attributes == right->attributes &&
           left->link_count == right->link_count &&
           left->creation_time.dwLowDateTime == right->creation_time.dwLowDateTime &&
           left->creation_time.dwHighDateTime == right->creation_time.dwHighDateTime &&
           left->write_time.dwLowDateTime == right->write_time.dwLowDateTime &&
           left->write_time.dwHighDateTime == right->write_time.dwHighDateTime &&
           left->size == right->size;
#else
    return left->device == right->device && left->inode == right->inode &&
           left->mode == right->mode && left->link_count == right->link_count &&
           left->owner == right->owner && left->group == right->group &&
           left->size == right->size && left->modified_sec == right->modified_sec &&
           left->modified_nsec == right->modified_nsec && left->changed_sec == right->changed_sec &&
           left->changed_nsec == right->changed_nsec;
#endif
}

#ifdef _WIN32
static int jl_snapshot_from_handle(HANDLE handle, jl_file_snapshot_t *snapshot) {
    BY_HANDLE_FILE_INFORMATION info;
    if (GetFileType(handle) != FILE_TYPE_DISK || !GetFileInformationByHandle(handle, &info) ||
        (info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) != 0 ||
        info.nNumberOfLinks != 1U || (info.nFileIndexHigh == 0U && info.nFileIndexLow == 0U)) {
        return -1;
    }
    uint64_t size = ((uint64_t)info.nFileSizeHigh << 32U) | (uint64_t)info.nFileSizeLow;
    if (size > JL_MAX_FILE_BYTES) {
        return -1;
    }
    *snapshot = (jl_file_snapshot_t){
        .exists = true,
        .volume_serial = info.dwVolumeSerialNumber,
        .file_index_high = info.nFileIndexHigh,
        .file_index_low = info.nFileIndexLow,
        .attributes = info.dwFileAttributes,
        .link_count = info.nNumberOfLinks,
        .creation_time = info.ftCreationTime,
        .write_time = info.ftLastWriteTime,
        .size = size,
    };
    return 0;
}
#else
static int jl_snapshot_from_stat(const struct stat *state, jl_file_snapshot_t *snapshot) {
    if (!S_ISREG(state->st_mode) || state->st_ino == 0 || state->st_nlink != 1U ||
        state->st_size < 0 || (uint64_t)state->st_size > JL_MAX_FILE_BYTES ||
        (state->st_mode & (S_ISUID | S_ISGID | S_ISVTX)) != 0) {
        return -1;
    }
    *snapshot = (jl_file_snapshot_t){
        .exists = true,
        .device = state->st_dev,
        .inode = state->st_ino,
        .mode = state->st_mode,
        .link_count = state->st_nlink,
        .owner = state->st_uid,
        .group = state->st_gid,
        .size = state->st_size,
#ifdef __APPLE__
        .modified_sec = state->st_mtimespec.tv_sec,
        .modified_nsec = state->st_mtimespec.tv_nsec,
        .changed_sec = state->st_ctimespec.tv_sec,
        .changed_nsec = state->st_ctimespec.tv_nsec,
#else
        .modified_sec = state->st_mtim.tv_sec,
        .modified_nsec = state->st_mtim.tv_nsec,
        .changed_sec = state->st_ctim.tv_sec,
        .changed_nsec = state->st_ctim.tv_nsec,
#endif
    };
    return 0;
}
#endif

static int jl_read_file(const char *path, char **content_out, size_t *length_out, bool *missing_out,
                        jl_file_snapshot_t *snapshot_out) {
    *content_out = NULL;
    *length_out = 0;
    *missing_out = false;
    memset(snapshot_out, 0, sizeof(*snapshot_out));
#ifdef _WIN32
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    if (!wide_path) {
        return -1;
    }
    HANDLE handle = CreateFileW(
        wide_path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
        OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    free(wide_path);
    if (handle == INVALID_HANDLE_VALUE) {
        DWORD error = GetLastError();
        if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND) {
            *missing_out = true;
            return 0;
        }
        return -1;
    }
    jl_file_snapshot_t before;
    if (jl_snapshot_from_handle(handle, &before) != 0) {
        CloseHandle(handle);
        return -1;
    }
    size_t length = (size_t)before.size;
    char *content = malloc(length + 1U);
    if (!content) {
        CloseHandle(handle);
        return -1;
    }
    DWORD read_count = 0U;
    BOOL read_ok = ReadFile(handle, content, (DWORD)length, &read_count, NULL);
    jl_file_snapshot_t after;
    int after_result = jl_snapshot_from_handle(handle, &after);
    BOOL close_ok = CloseHandle(handle);
    if (!read_ok || read_count != (DWORD)length || after_result != 0 || !close_ok ||
        !jl_snapshot_state_equal(&before, &after)) {
        free(content);
        return -1;
    }
#else
#ifndef O_NOFOLLOW
    (void)path;
    return -1;
#else
    int flags = O_RDONLY | O_NOFOLLOW | O_NONBLOCK;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    int descriptor = open(path, flags);
    if (descriptor < 0) {
        if (errno == ENOENT) {
            struct stat path_state;
            if (lstat(path, &path_state) == 0 || errno != ENOENT) {
                return -1;
            }
            *missing_out = true;
            return 0;
        }
        return -1;
    }
    struct stat before_state;
    jl_file_snapshot_t before;
    if (fstat(descriptor, &before_state) != 0 ||
        jl_snapshot_from_stat(&before_state, &before) != 0) {
        close(descriptor);
        return -1;
    }
    FILE *file = fdopen(descriptor, "rb");
    if (!file) {
        close(descriptor);
        return -1;
    }
    size_t length = (size_t)before.size;
    char *content = malloc(length + 1U);
    if (!content) {
        fclose(file);
        return -1;
    }
    size_t read_count = fread(content, 1U, length, file);
    int read_failed = ferror(file);
    struct stat after_state;
    jl_file_snapshot_t after;
    int after_result = fstat(cbm_fileno(file), &after_state) == 0
                           ? jl_snapshot_from_stat(&after_state, &after)
                           : -1;
    int close_failed = fclose(file);
    if (read_count != length || read_failed || after_result != 0 || close_failed != 0 ||
        !jl_snapshot_state_equal(&before, &after)) {
        free(content);
        return -1;
    }
#endif
#endif
    content[length] = '\0';
    *content_out = content;
    *length_out = length;
    *snapshot_out = before;
    return 0;
}

static char *jl_parent_directory(const char *path) {
    const char *separator = strrchr(path, '/');
#ifdef _WIN32
    const char *backslash = strrchr(path, '\\');
    if (!separator || (backslash && backslash > separator)) {
        separator = backslash;
    }
#endif
    if (!separator) {
        return cbm_strdup(".");
    }
    if (separator == path) {
        return cbm_strdup("/");
    }
    return cbm_strndup(path, (size_t)(separator - path));
}

static int jl_ensure_parent(const char *path) {
    char *parent = jl_parent_directory(path);
    if (!parent) {
        return -1;
    }
    int result = strcmp(parent, ".") == 0 || cbm_mkdir_p(parent, 0755) ? 0 : -1;
    free(parent);
    return result;
}

static int jl_snapshot_matches_path(const char *path, const char *expected_content,
                                    size_t expected_length,
                                    const jl_file_snapshot_t *expected_snapshot) {
    char *current_content = NULL;
    size_t current_length = 0U;
    bool missing = false;
    jl_file_snapshot_t current_snapshot;
    if (jl_read_file(path, &current_content, &current_length, &missing, &current_snapshot) != 0) {
        return -1;
    }
    bool matches = false;
    if (!expected_snapshot->exists) {
        matches = missing;
    } else {
        matches = !missing && current_length == expected_length &&
                  jl_snapshot_state_equal(expected_snapshot, &current_snapshot) &&
                  (expected_length == 0U ||
                   memcmp(current_content, expected_content, expected_length) == 0);
    }
    free(current_content);
    return matches ? 0 : -1;
}

#ifndef _WIN32
static int jl_sync_parent_directory(const char *path) {
    char *parent = jl_parent_directory(path);
    if (!parent) {
        return -1;
    }
    int flags = O_RDONLY;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    int descriptor = open(parent, flags);
    free(parent);
    if (descriptor < 0) {
        return -1;
    }
    struct stat state;
    int result =
        fstat(descriptor, &state) == 0 && S_ISDIR(state.st_mode) && fsync(descriptor) == 0 ? 0 : -1;
    if (close(descriptor) != 0) {
        result = -1;
    }
    return result;
}
#endif

static int jl_replace_atomic(const char *temp_path, const char *path, bool destination_exists) {
#ifdef _WIN32
    wchar_t *wide_temp = cbm_utf8_to_wide(temp_path);
    wchar_t *wide_path = cbm_utf8_to_wide(path);
    if (!wide_temp || !wide_path) {
        free(wide_temp);
        free(wide_path);
        return -1;
    }
    /* ReplaceFileW retains the destination's ACL and other mergeable metadata;
     * refusing merge errors is safer than silently dropping that metadata. */
    BOOL replaced = destination_exists ? ReplaceFileW(wide_path, wide_temp, NULL,
                                                      REPLACEFILE_WRITE_THROUGH, NULL, NULL)
                                       : MoveFileExW(wide_temp, wide_path, MOVEFILE_WRITE_THROUGH);
    free(wide_temp);
    free(wide_path);
    return replaced ? 0 : -1;
#else
    if (!destination_exists) {
        if (link(temp_path, path) != 0) {
            return -1;
        }
        if (cbm_unlink(temp_path) != 0) {
            return -1;
        }
        return jl_sync_parent_directory(path);
    }
    if (rename(temp_path, path) != 0) {
        return -1;
    }
    return jl_sync_parent_directory(path);
#endif
}

static int jl_write_atomic(const char *path, const char *content, size_t length,
                           const char *expected_content, size_t expected_length,
                           const jl_file_snapshot_t *expected_snapshot) {
    if (jl_ensure_parent(path) != 0) {
        return -1;
    }
    size_t path_length = strlen(path);
    enum { TEMP_SUFFIX_SPACE = 80 };
    if (path_length > SIZE_MAX - TEMP_SUFFIX_SPACE) {
        return -1;
    }
    size_t temp_capacity = path_length + TEMP_SUFFIX_SPACE;
    char *temp_path = malloc(temp_capacity);
    if (!temp_path) {
        return -1;
    }

    FILE *file = NULL;
    for (unsigned attempt = 0; attempt < 64U; attempt++) {
        unsigned sequence = atomic_fetch_add_explicit(&jl_temp_sequence, 1U, memory_order_relaxed);
        int written = snprintf(temp_path, temp_capacity, "%s.cbm.tmp.%ld.%u", path,
                               (long)JL_PROCESS_ID(), sequence);
        if (written < 0 || (size_t)written >= temp_capacity) {
            free(temp_path);
            return -1;
        }
        errno = 0;
#ifdef _WIN32
        file = cbm_fopen(temp_path, "wbx");
#else
#ifndef O_NOFOLLOW
        free(temp_path);
        return -1;
#else
        int flags = O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW;
#ifdef O_CLOEXEC
        flags |= O_CLOEXEC;
#endif
        int descriptor = open(temp_path, flags, 0600);
        if (descriptor >= 0) {
            file = fdopen(descriptor, "wb");
            if (!file) {
                int saved_error = errno;
                close(descriptor);
                (void)cbm_unlink(temp_path);
                errno = saved_error;
            }
        }
#endif
#endif
        if (file) {
            break;
        }
        if (errno != EEXIST) {
            free(temp_path);
            return -1;
        }
    }
    if (!file) {
        free(temp_path);
        return -1;
    }

    bool failed = false;
    if (fwrite(content, 1U, length, file) != length) {
        failed = true;
    }
    if (!failed && fflush(file) != 0) {
        failed = true;
    }
#ifndef _WIN32
    if (!failed && expected_snapshot->exists &&
        fchown(cbm_fileno(file), expected_snapshot->owner, expected_snapshot->group) != 0) {
        failed = true;
    }
    mode_t mode = expected_snapshot->exists ? expected_snapshot->mode & 0777U : 0600U;
    if (!failed && fchmod(cbm_fileno(file), mode) != 0) {
        failed = true;
    }
#endif
    if (!failed && JL_SYNC(cbm_fileno(file)) != 0) {
        failed = true;
    }
    if (fclose(file) != 0) {
        failed = true;
    }
    if (failed) {
        cbm_unlink(temp_path);
        free(temp_path);
        return -1;
    }
    char *temp_content = NULL;
    size_t temp_length = 0U;
    bool temp_missing = false;
    jl_file_snapshot_t temp_snapshot;
    if (jl_read_file(temp_path, &temp_content, &temp_length, &temp_missing, &temp_snapshot) != 0 ||
        temp_missing || temp_length != length ||
        (length != 0U && memcmp(temp_content, content, length) != 0)) {
        free(temp_content);
        cbm_unlink(temp_path);
        free(temp_path);
        return -1;
    }
    free(temp_content);
#ifdef CBM_JSON_LIKE_ENABLE_TEST_API
    if (jl_precommit_test_hook) {
        jl_precommit_test_hook(path, jl_precommit_test_context);
    }
#endif
    if (jl_snapshot_matches_path(path, expected_content, expected_length, expected_snapshot) != 0) {
        cbm_unlink(temp_path);
        free(temp_path);
        return -1;
    }
#ifdef CBM_JSON_LIKE_ENABLE_TEST_API
    if (jl_prepublish_test_hook) {
        jl_prepublish_test_hook(path, jl_prepublish_test_context);
    }
#endif
    if (jl_snapshot_matches_path(path, expected_content, expected_length, expected_snapshot) != 0 ||
        jl_snapshot_matches_path(temp_path, content, length, &temp_snapshot) != 0 ||
        jl_replace_atomic(temp_path, path, expected_snapshot->exists) != 0) {
        cbm_unlink(temp_path);
        free(temp_path);
        return -1;
    }
    free(temp_path);
    return 0;
}

static int jl_decode_utf8(const unsigned char *text, size_t remaining, uint32_t *codepoint,
                          size_t *byte_count) {
    if (remaining == 0U) {
        return -1;
    }
    unsigned char first = text[0];
    if (first < 0x80U) {
        *codepoint = first;
        *byte_count = 1U;
        return 0;
    }
    size_t count = 0;
    uint32_t value = 0;
    uint32_t minimum = 0;
    if (first >= 0xC2U && first <= 0xDFU) {
        count = 2U;
        value = first & 0x1FU;
        minimum = 0x80U;
    } else if (first >= 0xE0U && first <= 0xEFU) {
        count = 3U;
        value = first & 0x0FU;
        minimum = 0x800U;
    } else if (first >= 0xF0U && first <= 0xF4U) {
        count = 4U;
        value = first & 0x07U;
        minimum = 0x10000U;
    } else {
        return -1;
    }
    if (remaining < count) {
        return -1;
    }
    for (size_t i = 1U; i < count; i++) {
        if ((text[i] & 0xC0U) != 0x80U) {
            return -1;
        }
        value = (value << 6U) | (text[i] & 0x3FU);
    }
    if (value < minimum || value > 0x10FFFFU || (value >= 0xD800U && value <= 0xDFFFU)) {
        return -1;
    }
    *codepoint = value;
    *byte_count = count;
    return 0;
}

static int jl_validate_utf8(const char *text, size_t length) {
    size_t pos = 0;
    while (pos < length) {
        uint32_t codepoint = 0;
        size_t byte_count = 0;
        if (jl_decode_utf8((const unsigned char *)text + pos, length - pos, &codepoint,
                           &byte_count) != 0) {
            return -1;
        }
        pos += byte_count;
    }
    return 0;
}

static int jl_validate_key(const char *key) {
    if (!key) {
        return -1;
    }
    size_t length = strlen(key);
    if (length > JL_MAX_KEY_BYTES) {
        return -1;
    }
    size_t pos = 0;
    while (pos < length) {
        uint32_t codepoint = 0;
        size_t byte_count = 0;
        if (jl_decode_utf8((const unsigned char *)key + pos, length - pos, &codepoint,
                           &byte_count) != 0 ||
            codepoint < 0x20U || (codepoint >= 0x7FU && codepoint <= 0x9FU)) {
            return -1;
        }
        pos += byte_count;
    }
    return 0;
}

static int jl_validate_string_value(const char *value) {
    if (!value) {
        return -1;
    }
    size_t length = strlen(value);
    return length <= JL_MAX_FILE_BYTES && jl_validate_utf8(value, length) == 0 ? 0 : -1;
}

static int jl_validate_arguments(const char *file_path, const char *const *object_path,
                                 size_t path_len, const char *entry_key) {
    if (!file_path || !*file_path || !object_path || !entry_key || path_len >= JL_MAX_DEPTH) {
        return -1;
    }
    if (jl_validate_key(entry_key) != 0) {
        return -1;
    }
    for (size_t i = 0; i < path_len; i++) {
        if (jl_validate_key(object_path[i]) != 0) {
            return -1;
        }
    }
    return 0;
}

static int jl_build_fresh(const char *const *object_path, size_t path_len, const char *entry_key,
                          const char *entry_json, size_t entry_length, char **result_out,
                          size_t *result_length_out) {
    jl_buffer_t result = {0};
    static const char indent[] = "  ";
    if (jl_buffer_string(&result, "{\n") != 0) {
        free(result.data);
        return -1;
    }
    for (size_t i = 0; i < path_len; i++) {
        if (jl_append_repeated(&result, indent, 2U, i + 1U) != 0 ||
            jl_append_quoted_key(&result, object_path[i]) != 0 ||
            jl_buffer_string(&result, ": {\n") != 0) {
            free(result.data);
            return -1;
        }
    }
    if (jl_append_repeated(&result, indent, 2U, path_len + 1U) != 0 ||
        jl_append_quoted_key(&result, entry_key) != 0 || jl_buffer_string(&result, ": ") != 0 ||
        jl_buffer_append(&result, entry_json, entry_length) != 0 ||
        jl_buffer_char(&result, '\n') != 0) {
        free(result.data);
        return -1;
    }
    for (size_t depth = path_len; depth > 0U; depth--) {
        if (jl_append_repeated(&result, indent, 2U, depth) != 0 ||
            jl_buffer_string(&result, "}\n") != 0) {
            free(result.data);
            return -1;
        }
    }
    if (jl_buffer_string(&result, "}\n") != 0) {
        free(result.data);
        return -1;
    }
    *result_out = result.data;
    *result_length_out = result.length;
    return 0;
}

static int jl_insert_member(const char *source, size_t source_length, size_t object_start,
                            const jl_object_t *object, const char *const *object_path,
                            size_t path_len, size_t missing_path_index, const char *entry_key,
                            const char *entry_json, size_t entry_length, char **result_out,
                            size_t *result_length_out) {
    jl_slice_t base;
    jl_slice_t unit;
    bool close_indent_only = false;
    jl_detect_indent(source, object, &base, &unit, &close_indent_only);
    (void)close_indent_only;

    jl_buffer_t member = {0};
    jl_buffer_t insertion = {0};
    const char *newline = jl_newline_style(source, source_length);
    int result = jl_build_member(&member, object_path, path_len, missing_path_index, entry_key,
                                 entry_json, entry_length, newline, base, unit);
    if (result == 0) {
        result =
            jl_make_insertion(source, source_length, object_start, object, &member, &insertion);
    }
    if (result != 0) {
        free(member.data);
        free(insertion.data);
        return -1;
    }

    jl_edit_t edits[2];
    size_t edit_count = 0;
    if (object->member_count > 0U && !object->trailing_comma) {
        edits[edit_count++] = (jl_edit_t){
            .start = object->last_value_end,
            .end = object->last_value_end,
            .replacement = ",",
            .replacement_length = 1U,
        };
    }
    edits[edit_count++] = (jl_edit_t){
        .start = object->close_pos,
        .end = object->close_pos,
        .replacement = insertion.data,
        .replacement_length = insertion.length,
    };
    result =
        jl_apply_edits(source, source_length, edits, edit_count, result_out, result_length_out);
    free(member.data);
    free(insertion.data);
    return result;
}

static int jl_insert_array_string(const char *source, size_t source_length, size_t array_start,
                                  const jl_array_t *array, const char *string_value,
                                  char **result_out, size_t *result_length_out) {
    jl_buffer_t element = {0};
    jl_buffer_t insertion = {0};
    if (jl_append_quoted_string(&element, string_value) != 0) {
        free(element.data);
        return -1;
    }

    jl_object_t style = {
        .close_pos = array->close_pos,
        .first_key_start = array->first_value_start,
        .last_value_end = array->last_value_end,
        .last_comma_pos = array->last_comma_pos,
        .member_count = array->element_count,
        .trailing_comma = array->trailing_comma,
    };
    int result =
        jl_make_insertion(source, source_length, array_start, &style, &element, &insertion);
    if (result != 0) {
        free(element.data);
        free(insertion.data);
        return -1;
    }

    jl_edit_t edits[2];
    size_t edit_count = 0;
    if (array->element_count > 0U && !array->trailing_comma) {
        edits[edit_count++] = (jl_edit_t){
            .start = array->last_value_end,
            .end = array->last_value_end,
            .replacement = ",",
            .replacement_length = 1U,
        };
    }
    edits[edit_count++] = (jl_edit_t){
        .start = array->close_pos,
        .end = array->close_pos,
        .replacement = insertion.data,
        .replacement_length = insertion.length,
    };
    result =
        jl_apply_edits(source, source_length, edits, edit_count, result_out, result_length_out);
    free(element.data);
    free(insertion.data);
    return result;
}

static int jl_write_document(const char *path, const char *content, size_t length,
                             const char *expected_content, size_t expected_length,
                             const jl_file_snapshot_t *expected_snapshot) {
    size_t root = 0;
    if (jl_validate_document(content, length, &root) != 0) {
        return -1;
    }
    return jl_write_atomic(path, content, length, expected_content, expected_length,
                           expected_snapshot);
}

#ifdef CBM_JSON_LIKE_ENABLE_TEST_API
void cbm_json_like_set_precommit_hook_for_testing(cbm_json_like_precommit_test_hook_t hook,
                                                  void *context) {
    jl_precommit_test_hook = hook;
    jl_precommit_test_context = context;
}

void cbm_json_like_set_prepublish_hook_for_testing(cbm_json_like_precommit_test_hook_t hook,
                                                   void *context) {
    jl_prepublish_test_hook = hook;
    jl_prepublish_test_context = context;
}
#endif

int cbm_json_like_read_document(const char *file_path, char **content_out, size_t *length_out) {
    if (!file_path || !content_out || !length_out) {
        return -1;
    }
    bool missing = false;
    jl_file_snapshot_t snapshot;
    if (jl_read_file(file_path, content_out, length_out, &missing, &snapshot) != 0) {
        return -1;
    }
    return missing ? 1 : 0;
}

int cbm_json_like_get_raw_entry(const char *file_path, const char *const *object_path,
                                size_t path_len, const char *entry_key, char **value_json_out,
                                size_t *value_length_out) {
    if (!value_json_out || !value_length_out) {
        return -1;
    }
    *value_json_out = NULL;
    *value_length_out = 0U;
    if (jl_validate_arguments(file_path, object_path, path_len, entry_key) != 0) {
        return -1;
    }

    char *source = NULL;
    size_t source_length = 0U;
    bool missing = false;
    jl_file_snapshot_t snapshot;
    if (jl_read_file(file_path, &source, &source_length, &missing, &snapshot) != 0) {
        return -1;
    }
    if (missing || source_length == 0U) {
        free(source);
        return 1;
    }

    size_t object_start = 0U;
    if (jl_validate_document(source, source_length, &object_start) != 0) {
        free(source);
        return -1;
    }
    for (size_t i = 0U; i < path_len; ++i) {
        jl_object_t parent;
        if (jl_scan_object(source, source_length, object_start, object_path[i], &parent) != 0 ||
            parent.match_count > 1U) {
            free(source);
            return -1;
        }
        if (parent.match_count == 0U) {
            free(source);
            return 1;
        }
        if (source[parent.match.value_start] != '{') {
            free(source);
            return -1;
        }
        object_start = parent.match.value_start;
    }

    jl_object_t object;
    if (jl_scan_object(source, source_length, object_start, entry_key, &object) != 0 ||
        object.match_count > 1U) {
        free(source);
        return -1;
    }
    if (object.match_count == 0U) {
        free(source);
        return 1;
    }
    size_t value_length = object.match.value_end - object.match.value_start;
    char *value = (char *)malloc(value_length + 1U);
    if (!value) {
        free(source);
        return -1;
    }
    memcpy(value, source + object.match.value_start, value_length);
    value[value_length] = '\0';
    free(source);
    *value_json_out = value;
    *value_length_out = value_length;
    return 0;
}

static int jl_upsert_entry(const char *file_path, const char *const *object_path, size_t path_len,
                           const char *entry_key, const char *entry_json, bool enforce_expected,
                           const char *expected_content, size_t expected_length) {
    if (jl_validate_arguments(file_path, object_path, path_len, entry_key) != 0 || !entry_json) {
        return -1;
    }
    size_t entry_start = 0;
    size_t entry_length = 0;
    if (jl_validate_entry(entry_json, &entry_start, &entry_length) != 0) {
        return -1;
    }
    const char *entry_value = entry_json + entry_start;

    char *source = NULL;
    size_t source_length = 0;
    bool missing = false;
    jl_file_snapshot_t snapshot;
    if (jl_read_file(file_path, &source, &source_length, &missing, &snapshot) != 0) {
        return -1;
    }
    if (enforce_expected) {
        bool expected_missing = expected_content == NULL;
        bool content_matches = expected_missing
                                   ? missing
                                   : !missing && source_length == expected_length &&
                                         (expected_length == 0U ||
                                          memcmp(source, expected_content, expected_length) == 0);
        if (!content_matches) {
            free(source);
            return -1;
        }
    }
    if (missing || source_length == 0U) {
        char *fresh = NULL;
        size_t fresh_length = 0;
        int result = jl_build_fresh(object_path, path_len, entry_key, entry_value, entry_length,
                                    &fresh, &fresh_length);
        if (result == 0) {
            result =
                jl_write_document(file_path, fresh, fresh_length, source, source_length, &snapshot);
        }
        free(fresh);
        free(source);
        return result;
    }

    size_t object_start = 0;
    if (jl_validate_document(source, source_length, &object_start) != 0) {
        free(source);
        return -1;
    }
    for (size_t i = 0; i < path_len; i++) {
        jl_object_t object;
        if (jl_scan_object(source, source_length, object_start, object_path[i], &object) != 0 ||
            object.match_count > 1U) {
            free(source);
            return -1;
        }
        if (object.match_count == 0U) {
            char *updated = NULL;
            size_t updated_length = 0;
            int result = jl_insert_member(source, source_length, object_start, &object, object_path,
                                          path_len, i, entry_key, entry_value, entry_length,
                                          &updated, &updated_length);
            if (result == 0) {
                result = jl_write_document(file_path, updated, updated_length, source,
                                           source_length, &snapshot);
            }
            free(updated);
            free(source);
            return result;
        }
        if (source[object.match.value_start] != '{') {
            free(source);
            return -1;
        }
        object_start = object.match.value_start;
    }

    jl_object_t object;
    if (jl_scan_object(source, source_length, object_start, entry_key, &object) != 0 ||
        object.match_count > 1U) {
        free(source);
        return -1;
    }
    if (object.match_count == 0U) {
        char *updated = NULL;
        size_t updated_length = 0;
        int result = jl_insert_member(source, source_length, object_start, &object, object_path,
                                      path_len, SIZE_MAX, entry_key, entry_value, entry_length,
                                      &updated, &updated_length);
        if (result == 0) {
            result = jl_write_document(file_path, updated, updated_length, source, source_length,
                                       &snapshot);
        }
        free(updated);
        free(source);
        return result;
    }

    size_t old_length = object.match.value_end - object.match.value_start;
    if (old_length == entry_length &&
        memcmp(source + object.match.value_start, entry_value, entry_length) == 0) {
        free(source);
        return 0;
    }
    jl_edit_t edit = {
        .start = object.match.value_start,
        .end = object.match.value_end,
        .replacement = entry_value,
        .replacement_length = entry_length,
    };
    char *updated = NULL;
    size_t updated_length = 0;
    int result = jl_apply_edits(source, source_length, &edit, 1U, &updated, &updated_length);
    if (result == 0) {
        result =
            jl_write_document(file_path, updated, updated_length, source, source_length, &snapshot);
    }
    free(updated);
    free(source);
    return result;
}

int cbm_json_like_upsert_entry(const char *file_path, const char *const *object_path,
                               size_t path_len, const char *entry_key, const char *entry_json) {
    return jl_upsert_entry(file_path, object_path, path_len, entry_key, entry_json, false, NULL,
                           0U);
}

int cbm_json_like_upsert_entry_if_unchanged(const char *file_path, const char *const *object_path,
                                            size_t path_len, const char *entry_key,
                                            const char *entry_json, const char *expected_content,
                                            size_t expected_length) {
    return jl_upsert_entry(file_path, object_path, path_len, entry_key, entry_json, true,
                           expected_content, expected_length);
}

static int jl_remove_entry(const char *file_path, const char *const *object_path, size_t path_len,
                           const char *entry_key, bool enforce_expected,
                           const char *expected_content, size_t expected_length) {
    if (jl_validate_arguments(file_path, object_path, path_len, entry_key) != 0) {
        return -1;
    }
    char *source = NULL;
    size_t source_length = 0;
    bool missing = false;
    jl_file_snapshot_t snapshot;
    if (jl_read_file(file_path, &source, &source_length, &missing, &snapshot) != 0) {
        return -1;
    }
    if (enforce_expected) {
        bool expected_missing = expected_content == NULL;
        bool content_matches = expected_missing
                                   ? missing
                                   : !missing && source_length == expected_length &&
                                         (expected_length == 0U ||
                                          memcmp(source, expected_content, expected_length) == 0);
        if (!content_matches) {
            free(source);
            return -1;
        }
    }
    if (missing || source_length == 0U) {
        free(source);
        return 0;
    }

    size_t object_start = 0;
    if (jl_validate_document(source, source_length, &object_start) != 0) {
        free(source);
        return -1;
    }
    for (size_t i = 0; i < path_len; i++) {
        jl_object_t object;
        if (jl_scan_object(source, source_length, object_start, object_path[i], &object) != 0 ||
            object.match_count > 1U) {
            free(source);
            return -1;
        }
        if (object.match_count == 0U) {
            free(source);
            return 0;
        }
        if (source[object.match.value_start] != '{') {
            free(source);
            return -1;
        }
        object_start = object.match.value_start;
    }

    jl_object_t object;
    if (jl_scan_object(source, source_length, object_start, entry_key, &object) != 0 ||
        object.match_count > 1U) {
        free(source);
        return -1;
    }
    if (object.match_count == 0U) {
        free(source);
        return 0;
    }

    jl_edit_t edits[2];
    size_t edit_count = 0;
    edits[edit_count++] = (jl_edit_t){
        .start = jl_leading_whitespace_start(source, object.match.key_start),
        .end = object.match.value_end,
        .replacement = NULL,
        .replacement_length = 0,
    };
    if (object.match.comma_pos != SIZE_MAX) {
        edits[edit_count++] = (jl_edit_t){
            .start = object.match.comma_pos,
            .end = object.match.comma_pos + 1U,
            .replacement = NULL,
            .replacement_length = 0,
        };
    } else if (object.match.previous_comma_pos != SIZE_MAX) {
        edits[edit_count++] = (jl_edit_t){
            .start = object.match.previous_comma_pos,
            .end = object.match.previous_comma_pos + 1U,
            .replacement = NULL,
            .replacement_length = 0,
        };
    }

    char *updated = NULL;
    size_t updated_length = 0;
    int result =
        jl_apply_edits(source, source_length, edits, edit_count, &updated, &updated_length);
    if (result == 0) {
        result =
            jl_write_document(file_path, updated, updated_length, source, source_length, &snapshot);
    }
    free(updated);
    free(source);
    return result;
}

int cbm_json_like_remove_entry(const char *file_path, const char *const *object_path,
                               size_t path_len, const char *entry_key) {
    return jl_remove_entry(file_path, object_path, path_len, entry_key, false, NULL, 0U);
}

int cbm_json_like_remove_entry_if_unchanged(const char *file_path, const char *const *object_path,
                                            size_t path_len, const char *entry_key,
                                            const char *expected_content, size_t expected_length) {
    return jl_remove_entry(file_path, object_path, path_len, entry_key, true, expected_content,
                           expected_length);
}

int cbm_json_like_add_unique_string_at_path(const char *file_path, const char *const *object_path,
                                            size_t path_len, const char *array_key,
                                            const char *string_value) {
    if (jl_validate_arguments(file_path, object_path, path_len, array_key) != 0 ||
        jl_validate_string_value(string_value) != 0) {
        return -1;
    }

    jl_buffer_t array_json = {0};
    if (jl_buffer_char(&array_json, '[') != 0 ||
        jl_append_quoted_string(&array_json, string_value) != 0 ||
        jl_buffer_char(&array_json, ']') != 0) {
        free(array_json.data);
        return -1;
    }

    char *source = NULL;
    size_t source_length = 0;
    bool missing = false;
    jl_file_snapshot_t snapshot;
    if (jl_read_file(file_path, &source, &source_length, &missing, &snapshot) != 0) {
        free(array_json.data);
        return -1;
    }
    if (missing || source_length == 0U) {
        char *fresh = NULL;
        size_t fresh_length = 0;
        int result = jl_build_fresh(object_path, path_len, array_key, array_json.data,
                                    array_json.length, &fresh, &fresh_length);
        if (result == 0) {
            result =
                jl_write_document(file_path, fresh, fresh_length, source, source_length, &snapshot);
        }
        free(fresh);
        free(source);
        free(array_json.data);
        return result;
    }

    size_t object_start = 0;
    if (jl_validate_document(source, source_length, &object_start) != 0) {
        free(source);
        free(array_json.data);
        return -1;
    }
    for (size_t i = 0; i < path_len; i++) {
        jl_object_t parent;
        if (jl_scan_object(source, source_length, object_start, object_path[i], &parent) != 0 ||
            parent.match_count > 1U) {
            free(source);
            free(array_json.data);
            return -1;
        }
        if (parent.match_count == 0U) {
            char *updated = NULL;
            size_t updated_length = 0U;
            int result = jl_insert_member(source, source_length, object_start, &parent, object_path,
                                          path_len, i, array_key, array_json.data,
                                          array_json.length, &updated, &updated_length);
            if (result == 0) {
                result = jl_write_document(file_path, updated, updated_length, source,
                                           source_length, &snapshot);
            }
            free(updated);
            free(source);
            free(array_json.data);
            return result;
        }
        if (source[parent.match.value_start] != '{') {
            free(source);
            free(array_json.data);
            return -1;
        }
        object_start = parent.match.value_start;
    }

    jl_object_t object;
    if (jl_scan_object(source, source_length, object_start, array_key, &object) != 0 ||
        object.match_count > 1U) {
        free(source);
        free(array_json.data);
        return -1;
    }

    char *updated = NULL;
    size_t updated_length = 0;
    int result = 0;
    if (object.match_count == 0U) {
        result = jl_insert_member(source, source_length, object_start, &object, object_path,
                                  path_len, SIZE_MAX, array_key, array_json.data, array_json.length,
                                  &updated, &updated_length);
    } else if (source[object.match.value_start] != '[') {
        result = -1;
    } else {
        jl_array_t array;
        if (jl_scan_array(source, source_length, object.match.value_start, string_value, &array) !=
                0 ||
            array.match_count > 1U) {
            result = -1;
        } else if (array.match_count == 1U) {
            free(source);
            free(array_json.data);
            return 0;
        } else {
            result = jl_insert_array_string(source, source_length, object.match.value_start, &array,
                                            string_value, &updated, &updated_length);
        }
    }
    if (result == 0) {
        result =
            jl_write_document(file_path, updated, updated_length, source, source_length, &snapshot);
    }
    free(updated);
    free(source);
    free(array_json.data);
    return result;
}

int cbm_json_like_remove_string_at_path(const char *file_path, const char *const *object_path,
                                        size_t path_len, const char *array_key,
                                        const char *string_value) {
    if (jl_validate_arguments(file_path, object_path, path_len, array_key) != 0 ||
        jl_validate_string_value(string_value) != 0) {
        return -1;
    }
    char *source = NULL;
    size_t source_length = 0;
    bool missing = false;
    jl_file_snapshot_t snapshot;
    if (jl_read_file(file_path, &source, &source_length, &missing, &snapshot) != 0) {
        return -1;
    }
    if (missing || source_length == 0U) {
        free(source);
        return 0;
    }

    size_t object_start = 0;
    if (jl_validate_document(source, source_length, &object_start) != 0) {
        free(source);
        return -1;
    }
    for (size_t i = 0; i < path_len; i++) {
        jl_object_t parent;
        if (jl_scan_object(source, source_length, object_start, object_path[i], &parent) != 0 ||
            parent.match_count > 1U) {
            free(source);
            return -1;
        }
        if (parent.match_count == 0U) {
            free(source);
            return 0;
        }
        if (source[parent.match.value_start] != '{') {
            free(source);
            return -1;
        }
        object_start = parent.match.value_start;
    }

    jl_object_t object;
    if (jl_scan_object(source, source_length, object_start, array_key, &object) != 0 ||
        object.match_count > 1U) {
        free(source);
        return -1;
    }
    if (object.match_count == 0U) {
        free(source);
        return 0;
    }
    if (source[object.match.value_start] != '[') {
        free(source);
        return -1;
    }

    jl_array_t array;
    if (jl_scan_array(source, source_length, object.match.value_start, string_value, &array) != 0 ||
        array.match_count > 1U) {
        free(source);
        return -1;
    }
    if (array.match_count == 0U) {
        free(source);
        return 0;
    }

    jl_edit_t edits[2];
    size_t edit_count = 0;
    edits[edit_count++] = (jl_edit_t){
        .start = jl_leading_whitespace_start(source, array.match.value_start),
        .end = array.match.value_end,
        .replacement = NULL,
        .replacement_length = 0,
    };
    if (array.match.comma_pos != SIZE_MAX) {
        edits[edit_count++] = (jl_edit_t){
            .start = array.match.comma_pos,
            .end = array.match.comma_pos + 1U,
            .replacement = NULL,
            .replacement_length = 0,
        };
    } else if (array.match.previous_comma_pos != SIZE_MAX) {
        edits[edit_count++] = (jl_edit_t){
            .start = array.match.previous_comma_pos,
            .end = array.match.previous_comma_pos + 1U,
            .replacement = NULL,
            .replacement_length = 0,
        };
    }

    char *updated = NULL;
    size_t updated_length = 0;
    int result =
        jl_apply_edits(source, source_length, edits, edit_count, &updated, &updated_length);
    if (result == 0) {
        result =
            jl_write_document(file_path, updated, updated_length, source, source_length, &snapshot);
    }
    free(updated);
    free(source);
    return result;
}

static int jl_append_decoded_codepoint(jl_buffer_t *decoded, uint32_t codepoint) {
    if (codepoint == 0U || codepoint > 0x10FFFFU ||
        (codepoint >= 0xD800U && codepoint <= 0xDFFFU)) {
        return -1;
    }
    unsigned char encoded[4];
    size_t encoded_length = jl_encode_utf8(codepoint, encoded);
    return jl_buffer_append(decoded, (const char *)encoded, encoded_length);
}

static int jl_decode_string_value(const char *text, size_t start, size_t end, char **value_out) {
    *value_out = NULL;
    if (end <= start + 1U || (text[start] != '"' && text[start] != '\'') ||
        text[end - 1U] != text[start]) {
        return -1;
    }

    jl_buffer_t decoded = {0};
    size_t pos = start + 1U;
    size_t limit = end - 1U;
    while (pos < limit) {
        unsigned char current = (unsigned char)text[pos++];
        if (current != '\\') {
            if (jl_buffer_char(&decoded, (char)current) != 0) {
                free(decoded.data);
                return -1;
            }
            continue;
        }
        if (pos >= limit) {
            free(decoded.data);
            return -1;
        }

        unsigned char escaped = (unsigned char)text[pos++];
        if (escaped == '\n' || escaped == '\r') {
            if (escaped == '\r' && pos < limit && text[pos] == '\n') {
                pos++;
            }
            continue;
        }
        if (escaped == 0xE2U && limit - pos >= 2U && (unsigned char)text[pos] == 0x80U &&
            ((unsigned char)text[pos + 1U] == 0xA8U || (unsigned char)text[pos + 1U] == 0xA9U)) {
            pos += 2U;
            continue;
        }

        uint32_t codepoint = escaped;
        switch (escaped) {
        case 'b':
            codepoint = '\b';
            break;
        case 'f':
            codepoint = '\f';
            break;
        case 'n':
            codepoint = '\n';
            break;
        case 'r':
            codepoint = '\r';
            break;
        case 't':
            codepoint = '\t';
            break;
        case 'v':
            codepoint = '\v';
            break;
        case '0':
            codepoint = 0U;
            break;
        case 'x':
            if (limit - pos < 2U) {
                free(decoded.data);
                return -1;
            }
            codepoint = (jl_hex_value((unsigned char)text[pos]) << 4U) |
                        jl_hex_value((unsigned char)text[pos + 1U]);
            pos += 2U;
            break;
        case 'u': {
            if (limit - pos < 4U) {
                free(decoded.data);
                return -1;
            }
            codepoint = jl_decode_hex4(text + pos);
            pos += 4U;
            if (codepoint >= 0xD800U && codepoint <= 0xDBFFU) {
                if (limit - pos < 6U || text[pos] != '\\' || text[pos + 1U] != 'u') {
                    free(decoded.data);
                    return -1;
                }
                uint32_t low = jl_decode_hex4(text + pos + 2U);
                if (low < 0xDC00U || low > 0xDFFFU) {
                    free(decoded.data);
                    return -1;
                }
                codepoint = 0x10000U + ((codepoint - 0xD800U) << 10U) + (low - 0xDC00U);
                pos += 6U;
            }
            break;
        }
        default:
            break;
        }
        if (jl_append_decoded_codepoint(&decoded, codepoint) != 0) {
            free(decoded.data);
            return -1;
        }
    }

    if (!decoded.data) {
        decoded.data = malloc(1U);
        if (!decoded.data) {
            return -1;
        }
        decoded.data[0] = '\0';
    }
    *value_out = decoded.data;
    return 0;
}

static int jl_decode_field_string(const char *text, size_t start, size_t end,
                                  cbm_json_like_value_shape_t shape, char **value_out) {
    *value_out = NULL;
    if (shape == CBM_JSON_LIKE_VALUE_STRING) {
        if (start >= end || (text[start] != '"' && text[start] != '\'')) {
            return 1;
        }
        return jl_decode_string_value(text, start, end, value_out) == 0 ? 0 : 1;
    }
    if (shape != CBM_JSON_LIKE_VALUE_SINGLE_STRING_ARRAY || start >= end || text[start] != '[') {
        return 1;
    }

    jl_parser_t parser = {.text = text, .length = end, .pos = start + 1U, .strict = false};
    if (jl_skip_trivia(&parser) != 0 || parser.pos >= end || text[parser.pos] == ']') {
        return 1;
    }
    size_t value_start = 0U;
    size_t value_end = 0U;
    if (jl_parse_value(&parser, 1U, &value_start, &value_end) != 0 ||
        (text[value_start] != '"' && text[value_start] != '\'') || jl_skip_trivia(&parser) != 0 ||
        parser.pos >= end) {
        return 1;
    }
    if (text[parser.pos] == ',') {
        parser.pos++;
        if (jl_skip_trivia(&parser) != 0 || parser.pos >= end) {
            return 1;
        }
    }
    if (text[parser.pos] != ']' || parser.pos + 1U != end) {
        return 1;
    }
    return jl_decode_string_value(text, value_start, value_end, value_out) == 0 ? 0 : 1;
}

static bool jl_field_shape_matches(const char *text, const jl_member_t *member,
                                   cbm_json_like_value_shape_t shape, char **decoded_out) {
    *decoded_out = NULL;
    if (shape == CBM_JSON_LIKE_VALUE_EMPTY_ARRAY) {
        if (member->value_start >= member->value_end || text[member->value_start] != '[') {
            return false;
        }
        jl_parser_t parser = {
            .text = text,
            .length = member->value_end,
            .pos = member->value_start + 1U,
            .strict = false,
        };
        return jl_skip_trivia(&parser) == 0 && parser.pos < member->value_end &&
               text[parser.pos] == ']' && parser.pos + 1U == member->value_end;
    }
    return jl_decode_field_string(text, member->value_start, member->value_end, shape,
                                  decoded_out) == 0;
}

int cbm_json_like_match_object_entry(const char *document, size_t document_length,
                                     const char *const *object_path, size_t path_len,
                                     const char *entry_key,
                                     const cbm_json_like_object_field_t *fields, size_t field_count,
                                     char **captured_string_out) {
    if (!captured_string_out) {
        return -1;
    }
    *captured_string_out = NULL;
    if (!document || document_length > JL_MAX_FILE_BYTES || !entry_key || entry_key[0] == '\0' ||
        !fields || field_count == 0U || (path_len > 0U && !object_path)) {
        return -1;
    }
    for (size_t i = 0U; i < path_len; ++i) {
        if (!object_path[i] || object_path[i][0] == '\0') {
            return -1;
        }
    }
    size_t capture_count = 0U;
    for (size_t i = 0U; i < field_count; ++i) {
        if (!fields[i].key || fields[i].key[0] == '\0' ||
            fields[i].shape > CBM_JSON_LIKE_VALUE_SINGLE_STRING_ARRAY ||
            (fields[i].flags &
             ~(CBM_JSON_LIKE_FIELD_REQUIRED | CBM_JSON_LIKE_FIELD_CAPTURE_STRING)) != 0U ||
            ((fields[i].flags & CBM_JSON_LIKE_FIELD_CAPTURE_STRING) != 0U &&
             fields[i].shape == CBM_JSON_LIKE_VALUE_EMPTY_ARRAY)) {
            return -1;
        }
        capture_count += (fields[i].flags & CBM_JSON_LIKE_FIELD_CAPTURE_STRING) != 0U ? 1U : 0U;
        for (size_t j = 0U; j < i; ++j) {
            if (strcmp(fields[i].key, fields[j].key) == 0) {
                return -1;
            }
        }
    }
    if (capture_count != 1U) {
        return -1;
    }
    if (document_length == 0U) {
        return CBM_JSON_LIKE_OBJECT_MISSING;
    }

    size_t object_start = 0U;
    if (jl_validate_document(document, document_length, &object_start) != 0) {
        return -1;
    }
    for (size_t i = 0U; i < path_len; ++i) {
        jl_object_t parent;
        if (jl_scan_object(document, document_length, object_start, object_path[i], &parent) != 0) {
            return -1;
        }
        if (parent.match_count == 0U) {
            return CBM_JSON_LIKE_OBJECT_MISSING;
        }
        if (parent.match_count != 1U || document[parent.match.value_start] != '{') {
            return CBM_JSON_LIKE_OBJECT_MISMATCH;
        }
        object_start = parent.match.value_start;
    }

    jl_object_t entry_parent;
    if (jl_scan_object(document, document_length, object_start, entry_key, &entry_parent) != 0) {
        return -1;
    }
    if (entry_parent.match_count == 0U) {
        return CBM_JSON_LIKE_OBJECT_MISSING;
    }
    if (entry_parent.match_count != 1U || document[entry_parent.match.value_start] != '{') {
        return CBM_JSON_LIKE_OBJECT_MISMATCH;
    }

    size_t entry_start = entry_parent.match.value_start;
    size_t found_count = 0U;
    char *captured = NULL;
    size_t member_count = 0U;
    for (size_t i = 0U; i < field_count; ++i) {
        jl_object_t entry;
        if (jl_scan_object(document, document_length, entry_start, fields[i].key, &entry) != 0) {
            free(captured);
            return -1;
        }
        member_count = entry.member_count;
        if (entry.match_count > 1U ||
            (entry.match_count == 0U && (fields[i].flags & CBM_JSON_LIKE_FIELD_REQUIRED) != 0U)) {
            free(captured);
            return CBM_JSON_LIKE_OBJECT_MISMATCH;
        }
        if (entry.match_count == 0U) {
            continue;
        }
        found_count++;
        char *decoded = NULL;
        if (!jl_field_shape_matches(document, &entry.match, fields[i].shape, &decoded)) {
            free(captured);
            return CBM_JSON_LIKE_OBJECT_MISMATCH;
        }
        if (fields[i].expected_string &&
            (!decoded || strcmp(decoded, fields[i].expected_string) != 0)) {
            free(decoded);
            free(captured);
            return CBM_JSON_LIKE_OBJECT_MISMATCH;
        }
        if ((fields[i].flags & CBM_JSON_LIKE_FIELD_CAPTURE_STRING) != 0U) {
            captured = decoded;
        } else {
            free(decoded);
        }
    }
    if (member_count != found_count || !captured) {
        free(captured);
        return CBM_JSON_LIKE_OBJECT_MISMATCH;
    }
    *captured_string_out = captured;
    return CBM_JSON_LIKE_OBJECT_MATCH;
}

int cbm_json_like_get_string_at_path(const char *file_path, const char *const *object_path,
                                     size_t path_len, const char *string_key, char **value_out) {
    if (!value_out) {
        return -1;
    }
    *value_out = NULL;
    if (jl_validate_arguments(file_path, object_path, path_len, string_key) != 0) {
        return -1;
    }

    char *source = NULL;
    size_t source_length = 0U;
    bool missing = false;
    jl_file_snapshot_t snapshot;
    if (jl_read_file(file_path, &source, &source_length, &missing, &snapshot) != 0) {
        return -1;
    }
    if (missing || source_length == 0U) {
        free(source);
        return 1;
    }

    size_t object_start = 0U;
    if (jl_validate_document(source, source_length, &object_start) != 0) {
        free(source);
        return -1;
    }
    for (size_t i = 0; i < path_len; i++) {
        jl_object_t parent;
        if (jl_scan_object(source, source_length, object_start, object_path[i], &parent) != 0 ||
            parent.match_count > 1U) {
            free(source);
            return -1;
        }
        if (parent.match_count == 0U) {
            free(source);
            return 1;
        }
        if (source[parent.match.value_start] != '{') {
            free(source);
            return -1;
        }
        object_start = parent.match.value_start;
    }

    jl_object_t object;
    if (jl_scan_object(source, source_length, object_start, string_key, &object) != 0 ||
        object.match_count > 1U) {
        free(source);
        return -1;
    }
    if (object.match_count == 0U) {
        free(source);
        return 1;
    }
    if (source[object.match.value_start] != '"' && source[object.match.value_start] != '\'') {
        free(source);
        return -1;
    }

    int result =
        jl_decode_string_value(source, object.match.value_start, object.match.value_end, value_out);
    free(source);
    return result;
}

int cbm_json_like_add_unique_string(const char *file_path, const char *array_key,
                                    const char *string_value) {
    const char *root_path[] = {NULL};
    return cbm_json_like_add_unique_string_at_path(file_path, root_path, 0U, array_key,
                                                   string_value);
}

int cbm_json_like_remove_string(const char *file_path, const char *array_key,
                                const char *string_value) {
    const char *root_path[] = {NULL};
    return cbm_json_like_remove_string_at_path(file_path, root_path, 0U, array_key, string_value);
}
