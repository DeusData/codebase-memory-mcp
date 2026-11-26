#include "mcp/test_impact_tree_internal.h"
#ifdef _WIN32

bool tpt_native_write(tpt_context *c, tpt_object *o, cbm_git_bytes_t bytes) {
    size_t offset = 0;
    while (offset < bytes.length) {
        if (!tpt_poll(c))
            return false;
        size_t count = bytes.length - offset;
        DWORD n = count > TPT_CHUNK ? TPT_CHUNK : (DWORD)count;
        DWORD written = 0;
        if (!WriteFile(o->handle, bytes.data + offset, n, &written, NULL) || !written)
            return tpt_win_error(c, GetLastError(), false);
        offset += written;
    }
    return true;
}

bool tpt_native_read_bounded(tpt_context *c, tpt_object *o, size_t request, size_t *count) {
    *count = 0;
    if (!request || request > TPT_CHUNK)
        return tpt_fail(c, CBM_PINNED_TREE_INVALID, "invalid native read bound");
    DWORD n = 0;
    if (!ReadFile(o->handle, c->tree->io_scratch, (DWORD)request, &n, NULL))
        return tpt_win_error(c, GetLastError(), false);
    *count = n;
    return true;
}

bool tpt_native_read(tpt_context *c, tpt_object *o, size_t *count) {
    return tpt_native_read_bounded(c, o, TPT_CHUNK, count);
}

bool tpt_native_finish(tpt_context *c, tpt_object *o) {
    if (!tpt_native_close(c, &o->handle) || !tpt_chain(c, o))
        return false;
    bool absent = false;
    if (!tpt_native_identity(c, o, &absent))
        return false;
    if (absent)
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "created file disappeared");
    if (!tpt_win_path(c, o))
        return false;
    DWORD attributes = GetFileAttributesW(c->tree->wide_path);
    if (attributes == INVALID_FILE_ATTRIBUTES ||
        !SetFileAttributesW(c->tree->wide_path, attributes | FILE_ATTRIBUTE_READONLY))
        return tpt_win_error(c, GetLastError(), false);
    return tpt_native_check(c, o, false) && tpt_native_close(c, &o->handle);
}

static bool tpt_windows_entries(tpt_context *c, size_t directory) {
    cbm_pinned_tree_t *t = c->tree;
    size_t offset = 0;
    for (;;) {
        if (!tpt_poll(c))
            return false;
        size_t prefix = offsetof(FILE_ID_BOTH_DIR_INFO, FileName);
        if (offset > TPT_CHUNK - prefix)
            return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "native directory record overflow");
        FILE_ID_BOTH_DIR_INFO *entry = (void *)(t->enumeration_scratch + offset);
        size_t bytes = entry->FileNameLength;
        if (bytes % sizeof(wchar_t) || bytes > TPT_CHUNK - offset - prefix ||
            bytes / sizeof(wchar_t) >= TPT_PATH_CAP)
            return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "native directory name overflow");
        size_t chars = bytes / sizeof(wchar_t);
        memcpy(t->wide_root, entry->FileName, bytes);
        t->wide_root[chars] = 0;
        for (size_t i = 0; i < chars; i++) {
            if (!t->wide_root[i])
                return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "NUL in native directory name");
        }
        bool dot = chars == 1 && t->wide_root[0] == L'.';
        bool dots = chars == 2 && t->wide_root[0] == L'.' && t->wide_root[1] == L'.';
        if (!dot && !dots) {
            if (!tpt_utf8(c, t->wide_root, t->name_scratch))
                return false;
            size_t length = 0;
            if (!tpt_length(c, t->name_scratch, TPT_PATH_CAP - 1, &length) ||
                !tpt_entry(c, directory, t->name_scratch, length))
                return false;
        }
        DWORD next = entry->NextEntryOffset;
        if (!next)
            return true;
        if (next < prefix + bytes || next > TPT_CHUNK - offset || next % 8)
            return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "native directory record offset");
        offset += next;
    }
}

bool tpt_native_enumerate(tpt_context *c, size_t directory) {
    cbm_pinned_tree_t *t = c->tree;
    if (!tpt_win_path(c, &t->objects[directory]))
        return false;
    t->enumeration = CreateFileW(t->wide_path, FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES,
                                 FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
                                 FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (t->enumeration == INVALID_HANDLE_VALUE)
        return tpt_win_error(c, GetLastError(), false);
    bool first = true;
    for (;;) {
        if (!tpt_poll(c))
            return false;
        FILE_INFO_BY_HANDLE_CLASS kind =
            first ? FileIdBothDirectoryRestartInfo : FileIdBothDirectoryInfo;
        if (!GetFileInformationByHandleEx(t->enumeration, kind, t->enumeration_scratch,
                                          TPT_CHUNK)) {
            DWORD e = GetLastError();
            if (e == ERROR_NO_MORE_FILES)
                return tpt_native_close(c, &t->enumeration);
            return tpt_win_error(c, e, false);
        }
        first = false;
        if (!tpt_windows_entries(c, directory))
            return false;
    }
}

bool tpt_native_release(tpt_context *c) {
    cbm_pinned_tree_t *t = c->tree;
    bool ok = tpt_native_close(c, &t->enumeration);
    if (!tpt_native_close(c, &t->probe))
        ok = false;
    if (!tpt_native_close(c, &t->token))
        ok = false;
    if (!tpt_native_close(c, &t->impersonation))
        ok = false;
    bool owned = false;
    for (size_t i = 0; t->objects && i < t->object_count; i++) {
        tpt_object *o = &t->objects[i];
        if (!o->created)
            continue;
        owned = true;
        if (!o->directory && !tpt_native_close(c, &o->handle))
            ok = false;
    }
    if (!owned && !tpt_native_close(c, &t->parent.handle))
        ok = false;
    return ok;
}
#endif
