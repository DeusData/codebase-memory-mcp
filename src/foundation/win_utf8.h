#ifndef CBM_WIN_UTF8_H
#define CBM_WIN_UTF8_H

#ifdef _WIN32

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <stdlib.h>
#include <wchar.h>

static inline wchar_t *cbm_utf8_to_wide(const char *utf8) {
    if (!utf8) {
        return NULL;
    }
    int len = MultiByteToWideChar(CP_UTF8, 0, utf8, -1, NULL, 0);
    if (len <= 0) {
        return NULL;
    }
    wchar_t *w = (wchar_t *)malloc((size_t)len * sizeof(wchar_t));
    if (w) {
        MultiByteToWideChar(CP_UTF8, 0, utf8, -1, w, len);
    }
    return w;
}

/* Path-aware conversion for FILE-SYSTEM paths: absolute drive paths longer
 * than the legacy limit are canonicalized (GetFullPathNameW) and given the
 * extended-length \\?\ prefix so every Win32 file API accepts them
 * regardless of the machine's LongPathsEnabled policy. Never use this for
 * non-file strings (command lines, pipe names, registry paths). */
static inline wchar_t *cbm_path_to_wide(const char *utf8_path) {
    wchar_t *wide = cbm_utf8_to_wide(utf8_path);
    if (!wide) {
        return NULL;
    }
    size_t length = wcslen(wide);
    int drive_absolute =
        length >= 3 &&
        ((wide[0] >= L'A' && wide[0] <= L'Z') || (wide[0] >= L'a' && wide[0] <= L'z')) &&
        wide[1] == L':' && (wide[2] == L'\\' || wide[2] == L'/');
    if (!drive_absolute || length < 240U) {
        return wide;
    }
    DWORD needed = GetFullPathNameW(wide, 0, NULL, NULL);
    wchar_t *prefixed =
        needed > 0 ? (wchar_t *)malloc(((size_t)needed + 5U) * sizeof(wchar_t)) : NULL;
    if (!prefixed) {
        free(wide);
        return NULL;
    }
    wmemcpy(prefixed, L"\\\\?\\", 4);
    DWORD copied = GetFullPathNameW(wide, needed, prefixed + 4, NULL);
    free(wide);
    if (copied == 0 || copied >= needed) {
        free(prefixed);
        return NULL;
    }
    return prefixed;
}

static inline char *cbm_wide_to_utf8(const wchar_t *wide) {
    if (!wide) {
        return NULL;
    }
    int len = WideCharToMultiByte(CP_UTF8, 0, wide, -1, NULL, 0, NULL, NULL);
    if (len <= 0) {
        return NULL;
    }
    char *u8 = (char *)malloc((size_t)len);
    if (u8) {
        WideCharToMultiByte(CP_UTF8, 0, wide, -1, u8, len, NULL, NULL);
    }
    return u8;
}

#endif /* _WIN32 */
#endif /* CBM_WIN_UTF8_H */
