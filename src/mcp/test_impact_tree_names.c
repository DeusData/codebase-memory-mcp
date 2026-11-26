#include "mcp/test_impact_tree_internal.h"
#include "foundation/path_syntax_internal.h"

#ifdef _WIN32
static bool tpt_component(tpt_context *c, const unsigned char *p, size_t n) {
    cbm_path_syntax_result_t result = cbm_path_windows_component(p, n);
    if (result == CBM_PATH_SYNTAX_DEVICE_OR_TRIMMED)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "Windows device or trimmed component");
    if (result != CBM_PATH_SYNTAX_OK)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "Windows unsupported path component");
    return true;
}

bool tpt_wide(tpt_context *c, const char *source, wchar_t *target) {
    if (!source || !target)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "invalid native UTF-8 path");
    size_t length = 0;
    while (length < TPT_PATH_CAP && source[length])
        length++;
    /* The shared helper accepts positive lengths. Preserve this private
     * conversion's historical empty-string result without broadening it. */
    if (!length) {
        target[0] = 0;
        if (!c->tree->name_scratch)
            return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "path encoding did not roundtrip");
        c->tree->name_scratch[0] = 0;
        return true;
    }
    cbm_path_syntax_result_t result = cbm_path_windows_roundtrip(
        source, length, target, TPT_PATH_CAP, c->tree->name_scratch, TPT_PATH_CAP);
    if (result == CBM_PATH_SYNTAX_ROUNDTRIP)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "path encoding did not roundtrip");
    if (result != CBM_PATH_SYNTAX_OK)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "invalid native UTF-8 path");
    return true;
}

bool tpt_utf8(tpt_context *c, const wchar_t *source, char *target) {
    int n = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, source, -1, target, TPT_PATH_CAP,
                                NULL, NULL);
    if (!n)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "native name cannot roundtrip");
    int wide = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, target, n, c->tree->wide_path,
                                   TPT_PATH_CAP);
    if (!wide || memcmp(source, c->tree->wide_path, (size_t)wide * sizeof(wchar_t)))
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "native name changed encoding");
    return true;
}
#endif

bool tpt_path_valid(tpt_context *c, const unsigned char *path, size_t length) {
    if (!length || !path || path[0] == '/' || path[length - 1] == '/')
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "nonrelative inventory path");
    size_t start = 0;
    for (size_t i = 0; i <= length; i++) {
        if (i < length && !path[i])
            return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "NUL in inventory path");
        if (i < length && path[i] != '/')
            continue;
        size_t n = i - start;
        if (!n || (n == 1 && path[start] == '.') ||
            (n == 2 && path[start] == '.' && path[start + 1] == '.'))
            return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "unsafe inventory component");
#ifdef _WIN32
        if (!tpt_component(c, path + start, n))
            return false;
#endif
        start = i + 1;
    }
#ifdef _WIN32
    memcpy(c->tree->path_scratch, path, length);
    c->tree->path_scratch[length] = 0;
    return tpt_wide(c, c->tree->path_scratch, c->tree->wide_path);
#else
    return tpt_poll(c);
#endif
}

bool tpt_parent_syntax(tpt_context *c, const char *path) {
    size_t n = 0;
    if (!tpt_length(c, path, TPT_PATH_CAP - 1, &n))
        return false;
#ifdef _WIN32
    cbm_path_syntax_result_t result = cbm_path_windows_absolute(path, n);
    if (result == CBM_PATH_SYNTAX_INCOMPLETE_UNC)
        return tpt_fail(c, CBM_PINNED_TREE_INVALID, "incomplete UNC parent");
    if (result != CBM_PATH_SYNTAX_OK)
        return tpt_fail(c, CBM_PINNED_TREE_INVALID, "parent must be absolute drive or UNC path");
    return tpt_wide(c, path, c->tree->wide_parent);
#else
    return n && path[0] == '/' ? true
                               : tpt_fail(c, CBM_PINNED_TREE_INVALID, "parent must be absolute");
#endif
}
