#include "mcp/test_impact_tree_internal.h"
#ifdef _WIN32

bool tpt_win_error(tpt_context *c, DWORD e, bool creating) {
    /* Creating a file over a directory of the same (case-folded) name answers
     * ERROR_ACCESS_DENIED rather than "exists"; with the attempted name
     * present it is the same collision. Both create sites leave that name in
     * wide_path. */
    bool present = creating && e == ERROR_ACCESS_DENIED &&
                   GetFileAttributesW(c->tree->wide_path) != INVALID_FILE_ATTRIBUTES;
    if (creating && (e == ERROR_ALREADY_EXISTS || e == ERROR_FILE_EXISTS || present))
        return tpt_fail(c, CBM_PINNED_TREE_COLLISION, "exclusive native name already exists");
    if (e == ERROR_NOT_ENOUGH_MEMORY || e == ERROR_OUTOFMEMORY)
        return tpt_fail(c, CBM_PINNED_TREE_OOM, "native allocation failed");
    if (e == ERROR_INVALID_NAME || e == ERROR_FILENAME_EXCED_RANGE || e == ERROR_NOT_SUPPORTED ||
        e == ERROR_INVALID_FUNCTION)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "unsupported native representation");
    return tpt_fail(c, CBM_PINNED_TREE_IO, "native filesystem operation failed");
}

bool tpt_native_id_equal(tpt_identity a, tpt_identity b) {
    return a.volume == b.volume && memcmp(a.file, b.file, sizeof(a.file)) == 0;
}

static bool tpt_win_id(tpt_context *c, HANDLE handle, tpt_identity *out) {
    FILE_ID_INFO id;
    if (!GetFileInformationByHandleEx(handle, FileIdInfo, &id, sizeof(id)))
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "stable native file identity unavailable");
    out->volume = id.VolumeSerialNumber;
    memcpy(out->file, id.FileId.Identifier, sizeof(out->file));
    return true;
}

bool tpt_win_path(tpt_context *c, const tpt_object *o) {
    if (!tpt_join(c, o) || !tpt_wide(c, c->tree->path_scratch, c->tree->wide_path))
        return false;
    wchar_t *p = c->tree->wide_path;
    size_t n = 0;
    while (p[n]) {
        if (p[n] == L'/')
            p[n] = L'\\';
        n++;
    }
    if (n >= 2 && p[0] == L'\\' && p[1] == L'\\') {
        memmove(p + 8, p + 2, (n - 1) * sizeof(wchar_t));
        memcpy(p, L"\\\\?\\UNC\\", 8 * sizeof(wchar_t));
    } else {
        memmove(p + 4, p, (n + 1) * sizeof(wchar_t));
        memcpy(p, L"\\\\?\\", 4 * sizeof(wchar_t));
    }
    return true;
}

static bool tpt_win_info(tpt_context *c, tpt_object *o, BY_HANDLE_FILE_INFORMATION *info) {
    if (!GetFileInformationByHandle(o->handle, info))
        return tpt_win_error(c, GetLastError(), false);
    bool directory = (info->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
    if ((info->dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) || directory != o->directory ||
        (!directory && info->nNumberOfLinks != 1) || GetFileType(o->handle) != FILE_TYPE_DISK)
        return tpt_fail(c, o->identified ? CBM_PINNED_TREE_CHANGED : CBM_PINNED_TREE_UNSUPPORTED,
                        "native type, reparse or link mismatch");
    return true;
}

static bool tpt_win_open(tpt_context *c, tpt_object *o) {
    if (o->handle != INVALID_HANDLE_VALUE)
        return true;
    if (!tpt_win_path(c, o))
        return false;
    o->handle = CreateFileW(c->tree->wide_path, GENERIC_READ | READ_CONTROL,
                            FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
                            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, NULL);
    return o->handle != INVALID_HANDLE_VALUE || tpt_win_error(c, GetLastError(), false);
}

static bool tpt_win_probe_identity(tpt_context *c, const tpt_object *o) {
    BY_HANDLE_FILE_INFORMATION path_info;
    if (!GetFileInformationByHandle(c->tree->probe, &path_info))
        return tpt_win_error(c, GetLastError(), false);
    tpt_identity path_id;
    if (!tpt_win_id(c, c->tree->probe, &path_id))
        return false;
    return (o->identified && tpt_native_id_equal(o->identity, path_id)) ||
           tpt_fail(c, CBM_PINNED_TREE_CHANGED, "tracked path identity replaced");
}

static bool tpt_win_probe(tpt_context *c, const tpt_object *o) {
    if (!tpt_native_close(c, &c->tree->probe))
        return false;
    c->tree->probe = CreateFileW(c->tree->wide_path, FILE_READ_ATTRIBUTES,
                                 FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
                                 FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, NULL);
    if (c->tree->probe == INVALID_HANDLE_VALUE)
        return tpt_win_error(c, GetLastError(), false);
    bool matched = tpt_win_probe_identity(c, o);
    /* Always close a completed probe, including mismatch/inspection failures.
     * A close failure retains its handle; tpt_fail preserves any primary error. */
    bool closed = tpt_native_close(c, &c->tree->probe);
    return matched && closed;
}

bool tpt_native_identity(tpt_context *c, tpt_object *o, bool *absent) {
    *absent = false;
    if (!tpt_win_path(c, o))
        return false;
    DWORD attributes = GetFileAttributesW(c->tree->wide_path);
    if (attributes == INVALID_FILE_ATTRIBUTES) {
        DWORD e = GetLastError();
        if (e == ERROR_FILE_NOT_FOUND || e == ERROR_PATH_NOT_FOUND) {
            *absent = true;
            return true;
        }
        return tpt_win_error(c, e, false);
    }
    if ((attributes & FILE_ATTRIBUTE_REPARSE_POINT) ||
        ((attributes & FILE_ATTRIBUTE_DIRECTORY) != 0) != o->directory)
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "tracked path type replaced");
    if (!tpt_win_probe(c, o) || !tpt_win_open(c, o))
        return false;
    BY_HANDLE_FILE_INFORMATION info;
    if (!tpt_win_info(c, o, &info))
        return false;
    tpt_identity handle_id;
    if (!tpt_win_id(c, o->handle, &handle_id))
        return false;
    if (!o->identified || !tpt_native_id_equal(o->identity, handle_id))
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "tracked native identity replaced");
    return true;
}

bool tpt_native_check(tpt_context *c, tpt_object *o, bool writing) {
    if (!tpt_win_effective(c))
        return false;
    bool absent = false;
    if (!tpt_native_identity(c, o, &absent))
        return false;
    if (absent)
        return tpt_fail(c, CBM_PINNED_TREE_CHANGED, "expected native object missing");
    BY_HANDLE_FILE_INFORMATION info;
    if (!tpt_win_info(c, o, &info))
        return false;
    if (!o->directory && !writing) {
        uint64_t size = ((uint64_t)info.nFileSizeHigh << 32) | info.nFileSizeLow;
        if (!(info.dwFileAttributes & FILE_ATTRIBUTE_READONLY) ||
            size != c->tree->files[o->file].content_length)
            return tpt_fail(c, CBM_PINNED_TREE_CHANGED,
                            "native file size or read-only policy changed");
    }
    return tpt_native_acl(c, o, 0, !writing);
}

bool tpt_native_prepare(tpt_context *c) {
    cbm_pinned_tree_t *t = c->tree;
    if (!tpt_win_security(c))
        return false;
    size_t parent_length = 0;
    while (t->wide_parent[parent_length])
        parent_length++;
    while (parent_length > 3 && (t->wide_parent[parent_length - 1] == L'/' ||
                                 t->wide_parent[parent_length - 1] == L'\\'))
        t->wide_parent[--parent_length] = 0;
    DWORD n = GetFullPathNameW(t->wide_parent, TPT_PATH_CAP, t->wide_root, NULL);
    if (!n || n >= TPT_PATH_CAP)
        return tpt_fail(c, CBM_PINNED_TREE_UNSUPPORTED, "parent canonical path unavailable");
    if (!tpt_utf8(c, t->wide_root, t->parent_path))
        return false;
    t->parent.path = t->parent_path;
    if (!tpt_length(c, t->parent_path, TPT_PATH_CAP - 1, &t->parent.length) ||
        !tpt_win_open(c, &t->parent))
        return false;
    BY_HANDLE_FILE_INFORMATION info;
    if (!tpt_win_info(c, &t->parent, &info))
        return false;
    if (!tpt_win_id(c, t->parent.handle, &t->parent.identity))
        return false;
    t->parent.identified = true;
    return tpt_native_check(c, &t->parent, true);
}

bool tpt_native_create(tpt_context *c, tpt_object *o) {
    if (!tpt_poll(c) || !tpt_chain(c, o) || !tpt_win_path(c, o))
        return false;
    void *ace = NULL;
    if (!GetAce(c->tree->owned_acl, 0, &ace))
        return tpt_win_error(c, GetLastError(), false);
    ((ACE_HEADER *)ace)->AceFlags = o->directory ? OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE : 0;
    if (o->directory) {
        if (!CreateDirectoryW(c->tree->wide_path, &c->tree->attributes))
            return tpt_win_error(c, GetLastError(), true);
        o->created = true;
        if (!tpt_win_open(c, o))
            return false;
    } else {
        o->handle =
            CreateFileW(c->tree->wide_path, GENERIC_READ | GENERIC_WRITE | READ_CONTROL,
                        FILE_SHARE_READ | FILE_SHARE_WRITE, &c->tree->attributes, CREATE_NEW,
                        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
        if (o->handle == INVALID_HANDLE_VALUE)
            return tpt_win_error(c, GetLastError(), true);
        o->created = true;
    }
    BY_HANDLE_FILE_INFORMATION info;
    tpt_identity identity;
    if (!tpt_win_info(c, o, &info) || !tpt_win_id(c, o->handle, &identity) ||
        !tpt_record_identity(c, o, identity))
        return false;
    return tpt_native_check(c, o, true);
}

bool tpt_native_close(tpt_context *c, tpt_handle *handle) {
    if (*handle == INVALID_HANDLE_VALUE)
        return true;
    if (!CloseHandle(*handle)) {
        c->close_failed = true;
        return tpt_win_error(c, GetLastError(), false);
    }
    *handle = INVALID_HANDLE_VALUE;
    return true;
}

bool tpt_native_remove(tpt_context *c, tpt_object *o) {
    if (!tpt_chain(c, o))
        return false;
    bool absent = false;
    if (!tpt_native_identity(c, o, &absent))
        return false;
    if (absent && !tpt_absent_disposable(c, o))
        return false;
    if (!tpt_native_close(c, &o->handle))
        return false;
    if (absent) {
        o->created = false;
        return true;
    }
    if (!tpt_win_path(c, o))
        return false;
    if (!o->directory) {
        DWORD attributes = GetFileAttributesW(c->tree->wide_path);
        if (attributes == INVALID_FILE_ATTRIBUTES ||
            !SetFileAttributesW(c->tree->wide_path, attributes & ~FILE_ATTRIBUTE_READONLY))
            return tpt_win_error(c, GetLastError(), false);
    }
    BOOL ok = o->directory ? RemoveDirectoryW(c->tree->wide_path) : DeleteFileW(c->tree->wide_path);
    if (!ok)
        return tpt_win_error(c, GetLastError(), false);
    o->created = false;
    return true;
}
#endif
