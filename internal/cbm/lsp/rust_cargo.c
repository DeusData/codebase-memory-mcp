/* rust_cargo.c — Hand-rolled TOML subset parser for Cargo.toml.
 *
 * Recognises only the shapes we need (per RUST_LSP_FOLLOWUP §A3):
 *   - `[section]`, `[a.b.c]`, `[workspace]`, `[dependencies]`
 *   - `key = "string"`, `key = 'string'`, `key = [...]`,
 *     `key = { ... }`
 *   - `members = ["a", "b/c"]`
 *
 * Everything else (numbers, dates, deep tables, comments past EOL) is
 * skipped without error.
 */

#include "rust_cargo.h"
#include <stdbool.h>
#include <string.h>
#include <ctype.h>

/* ── Tiny tokenizer ──────────────────────────────────────────── */

static int skip_ws_and_comment(const char* s, int len, int from) {
    while (from < len) {
        char c = s[from];
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') { from++; continue; }
        if (c == '#') {
            while (from < len && s[from] != '\n') from++;
            continue;
        }
        break;
    }
    return from;
}

static bool is_ident_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') || c == '_' || c == '-';
}

/* Parse a bare key (ident-like). Stores arena-allocated copy in *out. */
static int parse_key(CBMArena* a, const char* s, int len, int from,
    const char** out) {
    int start = from;
    if (from < len && s[from] == '"') {
        from++;
        start = from;
        while (from < len && s[from] != '"') {
            if (s[from] == '\\' && from + 1 < len) from += 2;
            else from++;
        }
        *out = cbm_arena_strndup(a, s + start, (size_t)(from - start));
        if (from < len && s[from] == '"') from++;
        return from;
    }
    while (from < len && is_ident_char(s[from])) from++;
    if (from > start) {
        *out = cbm_arena_strndup(a, s + start, (size_t)(from - start));
    }
    return from;
}

/* Parse a string literal (single or double quoted). */
static int parse_string(CBMArena* a, const char* s, int len, int from,
    const char** out) {
    if (from >= len) return from;
    char q = s[from];
    if (q != '"' && q != '\'') return from;
    from++;
    int start = from;
    while (from < len && s[from] != q) {
        if (s[from] == '\\' && from + 1 < len) from += 2;
        else from++;
    }
    *out = cbm_arena_strndup(a, s + start, (size_t)(from - start));
    if (from < len) from++;
    return from;
}

/* Skip a value (used for keys we don't care about). Handles strings,
 * arrays, inline tables, bare values. */
static int skip_value(const char* s, int len, int from) {
    from = skip_ws_and_comment(s, len, from);
    if (from >= len) return from;
    char c = s[from];
    if (c == '"' || c == '\'') {
        from++;
        while (from < len && s[from] != c) {
            if (s[from] == '\\' && from + 1 < len) from += 2;
            else from++;
        }
        if (from < len) from++;
        return from;
    }
    if (c == '[' || c == '{') {
        char open = c, close = (c == '[' ? ']' : '}');
        int depth = 1;
        from++;
        while (from < len && depth > 0) {
            char d = s[from];
            if (d == '"' || d == '\'') {
                from++;
                while (from < len && s[from] != d) {
                    if (s[from] == '\\' && from + 1 < len) from += 2;
                    else from++;
                }
                if (from < len) from++;
                continue;
            }
            if (d == open) depth++;
            else if (d == close) depth--;
            from++;
        }
        return from;
    }
    /* Bare value: skip to end of line. */
    while (from < len && s[from] != '\n' && s[from] != '#') from++;
    return from;
}

/* Parse `[section.path]` header — returns the section name as a flat
 * dotted string, e.g. "dependencies" or "workspace.dependencies". */
static int parse_section(CBMArena* a, const char* s, int len, int from,
    const char** out) {
    if (from >= len || s[from] != '[') return from;
    /* Skip leading `[` or `[[`. */
    bool array_of_tables = false;
    from++;
    if (from < len && s[from] == '[') { array_of_tables = true; from++; }
    int start = from;
    while (from < len && s[from] != ']') from++;
    *out = cbm_arena_strndup(a, s + start, (size_t)(from - start));
    /* Consume closing `]` (or `]]`). */
    if (from < len) from++;
    if (array_of_tables && from < len && s[from] == ']') from++;
    return from;
}

/* For the `[dependencies]` / `[dev-dependencies]` / `[workspace.dependencies]`
 * sections, parse `key = value` lines until the next section. The value
 * may be a string (the version) or an inline table. We capture both
 * shapes — only the key (crate name) and optional `path = "..."` field
 * matter for us. */
static int parse_dep_entry(CBMArena* a, const char* s, int len, int from,
    CBMCargoManifest* out) {
    from = skip_ws_and_comment(s, len, from);
    if (from >= len || s[from] == '[') return from;
    const char* key = NULL;
    from = parse_key(a, s, len, from, &key);
    from = skip_ws_and_comment(s, len, from);
    if (from < len && s[from] == '=') {
        from++;
        from = skip_ws_and_comment(s, len, from);
    }
    const char* path_val = NULL;
    if (from < len && s[from] == '{') {
        /* Inline table — scan for `path = "..."`. */
        int depth = 1;
        from++;
        while (from < len && depth > 0) {
            from = skip_ws_and_comment(s, len, from);
            if (from >= len) break;
            char c = s[from];
            if (c == '}') { depth--; from++; continue; }
            if (c == ',') { from++; continue; }
            /* sub-key */
            const char* sub_key = NULL;
            from = parse_key(a, s, len, from, &sub_key);
            from = skip_ws_and_comment(s, len, from);
            if (from < len && s[from] == '=') {
                from++;
                from = skip_ws_and_comment(s, len, from);
            }
            if (sub_key && strcmp(sub_key, "path") == 0) {
                from = parse_string(a, s, len, from, &path_val);
            } else {
                from = skip_value(s, len, from);
            }
        }
    } else {
        from = skip_value(s, len, from);
    }
    if (key && out->dep_count < CBM_CARGO_MAX_DEPS) {
        out->deps[out->dep_count].name = key;
        out->deps[out->dep_count].path = path_val;
        out->dep_count++;
    }
    return from;
}

/* `[target.'cfg(unix)'.dependencies]` / `[target.x86_64-….dev-dependencies]`
 * — platform-conditional dep tables. Any section starting `target.` and
 * ending in a `dependencies` table name carries deps we should know. */
static bool section_is_target_deps(const char* section) {
    if (!section || strncmp(section, "target.", 7) != 0) return false;
    size_t len = strlen(section);
    static const char suffix[] = ".dependencies";
    size_t sfx = sizeof(suffix) - 1;
    return len > sfx && strcmp(section + len - sfx, suffix) == 0;
}

/* ── Section dispatcher ──────────────────────────────────────── */

static int parse_package_kv(CBMArena* a, const char* s, int len, int from,
    CBMCargoManifest* out) {
    from = skip_ws_and_comment(s, len, from);
    if (from >= len || s[from] == '[') return from;
    const char* key = NULL;
    from = parse_key(a, s, len, from, &key);
    from = skip_ws_and_comment(s, len, from);
    if (from < len && s[from] == '=') {
        from++;
        from = skip_ws_and_comment(s, len, from);
    }
    if (key && strcmp(key, "name") == 0) {
        from = parse_string(a, s, len, from, &out->package_name);
    } else if (key && strcmp(key, "version") == 0) {
        from = parse_string(a, s, len, from, &out->package_version);
    } else {
        from = skip_value(s, len, from);
    }
    return from;
}

static int parse_workspace_kv(CBMArena* a, const char* s, int len, int from,
    CBMCargoManifest* out) {
    from = skip_ws_and_comment(s, len, from);
    if (from >= len || s[from] == '[') return from;
    out->is_workspace_root = true;
    const char* key = NULL;
    from = parse_key(a, s, len, from, &key);
    from = skip_ws_and_comment(s, len, from);
    if (from < len && s[from] == '=') {
        from++;
        from = skip_ws_and_comment(s, len, from);
    }
    if (key && strcmp(key, "members") == 0 && from < len && s[from] == '[') {
        from++;
        while (from < len && s[from] != ']') {
            from = skip_ws_and_comment(s, len, from);
            if (from < len && (s[from] == '"' || s[from] == '\'')) {
                const char* mem = NULL;
                from = parse_string(a, s, len, from, &mem);
                if (mem && out->member_count < CBM_CARGO_MAX_MEMBERS) {
                    /* Derive a member NAME from the path's last segment. */
                    const char* last = mem;
                    for (const char* p = mem; *p; p++) {
                        if (*p == '/') last = p + 1;
                    }
                    out->members[out->member_count].member_name = last;
                    out->members[out->member_count].member_path = mem;
                    out->member_count++;
                }
            }
            from = skip_ws_and_comment(s, len, from);
            if (from < len && s[from] == ',') from++;
            from = skip_ws_and_comment(s, len, from);
        }
        if (from < len) from++;  /* consume `]` */
    } else {
        from = skip_value(s, len, from);
    }
    return from;
}

void cbm_cargo_parse(CBMArena* arena, const char* src, int src_len,
    CBMCargoManifest* out) {
    if (!arena || !src || !out) return;
    memset(out, 0, sizeof(*out));
    if (src_len <= 0) src_len = (int)strlen(src);

    int from = 0;
    /* Default: pre-header content treated as [package]. */
    const char* section = "package";

    while (from < src_len) {
        from = skip_ws_and_comment(src, src_len, from);
        if (from >= src_len) break;
        if (src[from] == '[') {
            const char* hdr = NULL;
            from = parse_section(arena, src, src_len, from, &hdr);
            section = hdr ? hdr : "";
            continue;
        }
        if (!section) {
            from = skip_value(src, src_len, from);
            continue;
        }
        if (strcmp(section, "package") == 0) {
            from = parse_package_kv(arena, src, src_len, from, out);
        } else if (strcmp(section, "workspace") == 0) {
            from = parse_workspace_kv(arena, src, src_len, from, out);
        } else if (strcmp(section, "dependencies") == 0 ||
                   strcmp(section, "dev-dependencies") == 0 ||
                   strcmp(section, "build-dependencies") == 0 ||
                   strcmp(section, "workspace.dependencies") == 0 ||
                   section_is_target_deps(section)) {
            from = parse_dep_entry(arena, src, src_len, from, out);
        } else {
            /* Section we don't care about — skip the line. */
            while (from < src_len && src[from] != '\n' && src[from] != '[') {
                from++;
            }
        }
    }
}

bool cbm_cargo_name_eq(const char* a, const char* b) {
    if (!a || !b) return false;
    while (*a && *b) {
        char ca = (*a == '-') ? '_' : *a;
        char cb = (*b == '-') ? '_' : *b;
        if (ca != cb) return false;
        a++;
        b++;
    }
    return *a == '\0' && *b == '\0';
}

bool cbm_cargo_is_known_dep(const CBMCargoManifest* m, const char* head) {
    if (!m || !head) return false;
    for (int i = 0; i < m->dep_count; i++) {
        if (cbm_cargo_name_eq(m->deps[i].name, head)) {
            return true;
        }
    }
    for (int i = 0; i < m->member_count; i++) {
        if (cbm_cargo_name_eq(m->members[i].member_name, head) ||
            cbm_cargo_name_eq(m->members[i].package_name, head)) {
            return true;
        }
    }
    return false;
}

const CBMCargoMember* cbm_cargo_find_member(const CBMCargoManifest* m,
    const char* name) {
    if (!m || !name) return NULL;
    for (int i = 0; i < m->member_count; i++) {
        if (cbm_cargo_name_eq(m->members[i].member_name, name) ||
            cbm_cargo_name_eq(m->members[i].package_name, name)) {
            return &m->members[i];
        }
    }
    return NULL;
}

const char* cbm_cargo_merge_member_deps(CBMArena* arena, CBMCargoManifest* dst,
    const char* toml, int toml_len) {
    if (!arena || !dst || !toml) return NULL;
    CBMCargoManifest tmp;
    cbm_cargo_parse(arena, toml, toml_len, &tmp);
    for (int i = 0; i < tmp.dep_count && dst->dep_count < CBM_CARGO_MAX_DEPS; i++) {
        const char* name = tmp.deps[i].name;
        if (!name) continue;
        bool dup = false;
        for (int j = 0; j < dst->dep_count; j++) {
            if (cbm_cargo_name_eq(dst->deps[j].name, name)) {
                dup = true;
                break;
            }
        }
        if (dup) continue;
        dst->deps[dst->dep_count].name = name;
        dst->deps[dst->dep_count].path = tmp.deps[i].path;
        dst->dep_count++;
    }
    return tmp.package_name;
}
