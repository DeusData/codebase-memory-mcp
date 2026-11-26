#include "foundation/path_syntax_internal.h"
#include <stdbool.h>
#include <string.h>
#ifdef _WIN32
#include <limits.h>
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

static unsigned char path_upper(unsigned char c) {
    return c >= 'a' && c <= 'z' ? (unsigned char)(c - 'a' + 'A') : c;
}

static bool path_device(const unsigned char *p, size_t n) {
    size_t base = 0;
    while (base < n && p[base] != '.')
        base++;
    if (base < 3)
        return false;
    unsigned char a = path_upper(p[0]), b = path_upper(p[1]), d = path_upper(p[2]);
    if (base == 3)
        return (a == 'C' && b == 'O' && d == 'N') || (a == 'P' && b == 'R' && d == 'N') ||
               (a == 'A' && b == 'U' && d == 'X') || (a == 'N' && b == 'U' && d == 'L');
    if (!((a == 'C' && b == 'O' && d == 'M') || (a == 'L' && b == 'P' && d == 'T')))
        return false;
    if (base == 4)
        return p[3] >= '1' && p[3] <= '9';
    return base == 5 && p[3] == 0xc2 && (p[4] == 0xb9 || p[4] == 0xb2 || p[4] == 0xb3);
}

cbm_path_syntax_result_t cbm_path_windows_component(const unsigned char *component, size_t length) {
    if (!component || !length || length > CBM_PATH_SYNTAX_MAX_BYTES)
        return CBM_PATH_SYNTAX_INVALID;
    if (component[length - 1] == '.' || component[length - 1] == ' ' ||
        path_device(component, length))
        return CBM_PATH_SYNTAX_DEVICE_OR_TRIMMED;
    for (size_t i = 0; i < length; i++) {
        unsigned char ch = component[i];
        if (ch < 32 || ch == '\\' || ch == ':' || ch == '<' || ch == '>' || ch == '"' ||
            ch == '|' || ch == '?' || ch == '*')
            return CBM_PATH_SYNTAX_COMPONENT_BYTE;
    }
    return CBM_PATH_SYNTAX_OK;
}

cbm_path_syntax_result_t cbm_path_windows_absolute(const char *path, size_t length) {
    if (!path || !length || length > CBM_PATH_SYNTAX_MAX_BYTES)
        return CBM_PATH_SYNTAX_INVALID;
    bool drive = length >= 3 &&
                 ((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) &&
                 path[1] == ':' && (path[2] == '/' || path[2] == '\\');
    bool unc = length >= 5 && path[0] == '\\' && path[1] == '\\' && path[2] != '?' &&
               path[2] != '.' && path[2] != '\\';
    if (!drive && !unc)
        return CBM_PATH_SYNTAX_NOT_ABSOLUTE;
    if (unc) {
        size_t i = 2;
        while (i < length && path[i] != '\\')
            i++;
        if (i + 1 >= length || path[i + 1] == '\\')
            return CBM_PATH_SYNTAX_INCOMPLETE_UNC;
    }
    return CBM_PATH_SYNTAX_OK;
}

#ifdef _WIN32
static bool path_roundtrip_args(const char *source, size_t length, const wchar_t *wide,
                                size_t wide_capacity, const char *roundtrip,
                                size_t roundtrip_capacity) {
    if (!source || !wide || !roundtrip || !length || length > CBM_PATH_SYNTAX_MAX_BYTES ||
        !wide_capacity || wide_capacity > INT_MAX || !roundtrip_capacity ||
        roundtrip_capacity > INT_MAX)
        return false;
    return source[length] == '\0' && !memchr(source, '\0', length);
}

cbm_path_syntax_result_t cbm_path_windows_roundtrip(const char *source, size_t length,
                                                    wchar_t *wide, size_t wide_capacity,
                                                    char *roundtrip, size_t roundtrip_capacity) {
    if (!path_roundtrip_args(source, length, wide, wide_capacity, roundtrip, roundtrip_capacity))
        return CBM_PATH_SYNTAX_INVALID;
    /* length <=4095 establishes the checked int conversion including terminal NUL. */
    int source_count = (int)(length + 1);
    int count = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, source, source_count, wide,
                                    (int)wide_capacity);
    if (!count)
        return CBM_PATH_SYNTAX_UTF8;
    int bytes = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, wide, count, roundtrip,
                                    (int)roundtrip_capacity, NULL, NULL);
    if (bytes != source_count || memcmp(source, roundtrip, length + 1))
        return CBM_PATH_SYNTAX_ROUNDTRIP;
    return CBM_PATH_SYNTAX_OK;
}
#endif
