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
