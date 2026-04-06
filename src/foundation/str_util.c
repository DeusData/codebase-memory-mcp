/*
 * str_util.c — Safe string operations (arena-allocated).
 */
#include "str_util.h"
#include "arena.h" // CBMArena, cbm_arena_alloc/strdup/strndup
#include "foundation/constants.h"
#include <string.h>
#include <ctype.h>

enum {
    JSON_ESC_LEN = 2,       /* escaped char takes 2 bytes (backslash + char) */
    JSON_NUL_RESERVE = 1,   /* reserve 1 byte for NUL terminator */
    JSON_CTRL_LIMIT = 0x20, /* ASCII control character upper bound */
};

char *cbm_path_join(CBMArena *a, const char *base, const char *name) {
    if (!base || !name) {
        return NULL;
    }
    size_t blen = strlen(base);
    size_t nlen = strlen(name);

    /* Handle empty components */
    if (blen == 0) {
        return cbm_arena_strdup(a, name);
    }
    if (nlen == 0) {
        return cbm_arena_strdup(a, base);
    }

    /* Strip trailing slash from base */
    while (blen > 0 && base[blen - SKIP_ONE] == '/') {
        blen--;
    }
    /* Strip leading slash from name */
    while (nlen > 0 && *name == '/') {
        name++;
        nlen--;
    }

    if (blen == 0) {
        return cbm_arena_strndup(a, name, nlen);
    }
    if (nlen == 0) {
        return cbm_arena_strndup(a, base, blen);
    }

    char *result = (char *)cbm_arena_alloc(a, blen + SKIP_ONE + nlen + SKIP_ONE);
    if (!result) {
        return NULL;
    }
    memcpy(result, base, blen);
    result[blen] = '/';
    memcpy(result + blen + SKIP_ONE, name, nlen);
    result[blen + SKIP_ONE + nlen] = '\0';
    return result;
}

char *cbm_path_join_n(CBMArena *a, const char **parts, int n) {
    if (n <= 0 || !parts) {
        return cbm_arena_strdup(a, "");
    }
    if (n == SKIP_ONE) {
        return cbm_arena_strdup(a, parts[0]);
    }

    char *result = cbm_arena_strdup(a, parts[0]);
    for (int i = SKIP_ONE; i < n; i++) {
        result = cbm_path_join(a, result, parts[i]);
    }
    return result;
}

const char *cbm_path_ext(const char *path) {
    if (!path) {
        return "";
    }
    const char *dot = NULL;
    const char *slash = NULL;
    for (const char *p = path; *p; p++) {
        if (*p == '.') {
            dot = p;
        }
        if (*p == '/') {
            slash = p;
        }
    }
    /* dot must be after last slash and not at start of basename */
    if (!dot) {
        return "";
    }
    if (slash && dot < slash) {
        return "";
    }
    return dot + SKIP_ONE;
}

const char *cbm_path_base(const char *path) {
    if (!path) {
        return "";
    }
    const char *last_slash = NULL;
    for (const char *p = path; *p; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }
    return last_slash ? last_slash + SKIP_ONE : path;
}

char *cbm_path_dir(CBMArena *a, const char *path) {
    if (!path) {
        return cbm_arena_strdup(a, ".");
    }
    const char *last_slash = NULL;
    for (const char *p = path; *p; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }
    if (!last_slash) {
        return cbm_arena_strdup(a, ".");
    }
    return cbm_arena_strndup(a, path, (size_t)(last_slash - path));
}

bool cbm_str_starts_with(const char *s, const char *prefix) {
    if (!s || !prefix) {
        return false;
    }
    size_t plen = strlen(prefix);
    return strncmp(s, prefix, plen) == 0;
}

bool cbm_str_ends_with(const char *s, const char *suffix) {
    if (!s || !suffix) {
        return false;
    }
    size_t slen = strlen(s);
    size_t xlen = strlen(suffix);
    if (xlen > slen) {
        return false;
    }
    return strcmp(s + slen - xlen, suffix) == 0;
}

bool cbm_str_contains(const char *s, const char *sub) {
    if (!s || !sub) {
        return false;
    }
    if (sub[0] == '\0') {
        return true;
    }
    return strstr(s, sub) != NULL;
}

char *cbm_str_tolower(CBMArena *a, const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    char *result = (char *)cbm_arena_alloc(a, len + SKIP_ONE);
    if (!result) {
        return NULL;
    }
    for (size_t i = 0; i < len; i++) {
        result[i] = (char)tolower((unsigned char)s[i]);
    }
    result[len] = '\0';
    return result;
}

char *cbm_str_replace_char(CBMArena *a, const char *s, char from, char to) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    char *result = (char *)cbm_arena_alloc(a, len + SKIP_ONE);
    if (!result) {
        return NULL;
    }
    for (size_t i = 0; i < len; i++) {
        result[i] = (s[i] == from) ? to : s[i];
    }
    result[len] = '\0';
    return result;
}

char *cbm_str_strip_ext(CBMArena *a, const char *path) {
    if (!path) {
        return NULL;
    }
    const char *dot = NULL;
    const char *slash = NULL;
    for (const char *p = path; *p; p++) {
        if (*p == '.') {
            dot = p;
        }
        if (*p == '/') {
            slash = p;
        }
    }
    if (!dot || (slash && dot < slash)) {
        return cbm_arena_strdup(a, path);
    }
    return cbm_arena_strndup(a, path, (size_t)(dot - path));
}

char **cbm_str_split(CBMArena *a, const char *s, char delim, int *out_count) {
    if (!s || !out_count) {
        return NULL;
    }

    /* Count parts */
    int count = SKIP_ONE;
    for (const char *p = s; *p; p++) {
        if (*p == delim) {
            count++;
        }
    }

    char **result = (char **)cbm_arena_alloc(a, (size_t)(count + SKIP_ONE) * sizeof(char *));
    if (!result) {
        return NULL;
    }

    int idx = 0;
    const char *start = s;
    for (const char *p = s;; p++) {
        if (*p == delim || *p == '\0') {
            size_t part_len = (size_t)(p - start);
            result[idx++] = cbm_arena_strndup(a, start, part_len);
            if (*p == '\0') {
                break;
            }
            start = p + SKIP_ONE;
        }
    }

    result[idx] = NULL;
    *out_count = count;
    return result;
}

bool cbm_validate_shell_arg(const char *s) {
    if (!s) {
        return false;
    }
    for (const char *p = s; *p; p++) {
        switch (*p) {
        case '\'':
        case ';':
        case '|':
        case '&':
        case '$':
        case '`':
        case '\n':
        case '\r':
#ifndef _WIN32
        case '\\':
#endif
            return false;
        default:
            break;
        }
    }
    return true;
}

int cbm_json_escape(char *buf, int bufsize, const char *src) {
    if (!buf || bufsize <= 0) {
        return 0;
    }
    if (!src) {
        buf[0] = '\0';
        return 0;
    }
    int pos = 0;
    for (int i = 0; src[i] && pos < bufsize - JSON_NUL_RESERVE; i++) {
        unsigned char c = (unsigned char)src[i];
        if (c == '"' || c == '\\') {
            if (pos + JSON_ESC_LEN > bufsize - JSON_NUL_RESERVE) {
                break;
            }
            buf[pos++] = '\\';
            buf[pos++] = (char)c;
        } else if (c == '\n') {
            if (pos + JSON_ESC_LEN > bufsize - JSON_NUL_RESERVE) {
                break;
            }
            buf[pos++] = '\\';
            buf[pos++] = 'n';
        } else if (c == '\r') {
            if (pos + JSON_ESC_LEN > bufsize - JSON_NUL_RESERVE) {
                break;
            }
            buf[pos++] = '\\';
            buf[pos++] = 'r';
        } else if (c == '\t') {
            if (pos + JSON_ESC_LEN > bufsize - JSON_NUL_RESERVE) {
                break;
            }
            buf[pos++] = '\\';
            buf[pos++] = 't';
        } else if (c < JSON_CTRL_LIMIT) {
            /* Other control chars: skip */
            continue;
        } else {
            buf[pos++] = (char)c;
        }
    }
    buf[pos] = '\0';
    return pos;
}
