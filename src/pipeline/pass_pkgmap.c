/*
 * pass_pkgmap.c — Generic package/module manifest resolution.
 *
 * Scans discovered files for manifest files (package.json, go.mod, Cargo.toml,
 * pyproject.toml, composer.json, pubspec.yaml, pom.xml, build.gradle, mix.exs,
 * *.gemspec, Package.swift) and builds a hash table mapping bare package specifiers to resolved
 * module QNs. This enables IMPORTS edges for non-relative imports like
 * "@myorg/pkg", "github.com/foo/bar", "use my_crate::foo".
 *
 * Integration: called from parallel extract workers (per-worker local arrays)
 * and merged sequentially before registry build.
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "pipeline/walk_path.h"
#include "discover/discover.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/limits.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"
#include "foundation/win_utf8.h"
#include "foundation/yaml.h"

#include <yyjson/yyjson.h>

#include <inttypes.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static void pkgmap_log_read_failure(const char *path, const char *reason, const char *constraint,
                                    long file_bytes, long read_bytes, long limit_bytes) {
    char file_buf[CBM_SZ_32];
    char read_buf[CBM_SZ_32];
    char limit_buf[CBM_SZ_32];
    snprintf(file_buf, sizeof(file_buf), "%ld", file_bytes);
    snprintf(read_buf, sizeof(read_buf), "%ld", read_bytes);
    snprintf(limit_buf, sizeof(limit_buf), "%ld", limit_bytes);
    cbm_log_warn("pkgmap.manifest_skipped", "path", path, "reason", reason, "constraint",
                 constraint, "file_bytes", file_buf, "read_bytes", read_buf, "limit_bytes",
                 limit_buf);
}

/* Read an entire manifest into one exact malloc'd buffer. A successful read is
 * O(B) time, O(B) returned memory, and one allocation for B file bytes. Every
 * failure closes the stream; an unstable/partial read is rejected rather than
 * parsed as a plausible complete manifest. The shared file policy is a
 * resource guard, while INT_MAX is the parser API's representational bound. */
static char *pkgmap_read_file(const char *path, int *out_len) {
    if (!path || !out_len) {
        return NULL;
    }
    *out_len = 0;
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        pkgmap_log_read_failure(path, "open_failed", "filesystem", -1, -1, -1);
        return NULL;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        (void)fclose(f);
        pkgmap_log_read_failure(path, "seek_end_failed", "filesystem", -1, -1, -1);
        return NULL;
    }
    long size = ftell(f);
    if (size < 0) {
        (void)fclose(f);
        pkgmap_log_read_failure(path, "size_failed", "filesystem", -1, -1, -1);
        return NULL;
    }
    if (fseek(f, 0, SEEK_SET) != 0) {
        (void)fclose(f);
        pkgmap_log_read_failure(path, "seek_start_failed", "filesystem", size, -1, -1);
        return NULL;
    }
    long cap = cbm_max_file_bytes();
    if (size > cap) {
        (void)fclose(f);
        pkgmap_log_read_failure(path, "oversized", "CBM_MAX_FILE_BYTES", size, 0, cap);
        return NULL;
    }
    if (size > INT_MAX) {
        (void)fclose(f);
        pkgmap_log_read_failure(path, "parser_length_overflow", "signed_int_parser_length", size, 0,
                                INT_MAX);
        return NULL;
    }
    char *buf = (char *)malloc((size_t)size + SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        pkgmap_log_read_failure(path, "out_of_memory", "allocation", size, 0, cap);
        return NULL;
    }
    size_t nread = fread(buf, SKIP_ONE, (size_t)size, f);
    (void)fclose(f);
    if (nread != (size_t)size) {
        free(buf);
        pkgmap_log_read_failure(path, "short_read", "filesystem_consistency", size, (long)nread,
                                cap);
        return NULL;
    }
    buf[size] = '\0';
    *out_len = (int)size;
    return buf;
}

/* ── Constants ─────────────────────────────────────────────────── */

enum {
    PKGMAP_INIT_CAP = 16,
    PKGMAP_HT_INIT = 64,
    PKGMAP_ITOA_BUF = 16,
    /* Initial allocation only: the iterative walker grows geometrically, so
     * directory depth never changes which manifests are reachable. */
    PKGMAP_WALK_STACK_INIT = 16,
    PKGMAP_IDENTITY_HEX_LEN = (int)(sizeof(uint64_t) * PAIR_LEN),
    PKGMAP_IDENTITY_KEY_LEN = PKGMAP_IDENTITY_HEX_LEN * PAIR_LEN + SKIP_ONE,
    PKGMAP_IDENTITY_KEY_BUFSZ = PKGMAP_IDENTITY_KEY_LEN + SKIP_ONE,
    /* String lengths for manifest parsing (avoid magic numbers in memcmp) */
    TOML_NAME_LEN = 4,      /* strlen("name") */
    TOML_NAME_SP = 5,       /* strlen("name ") */
    TOML_NAME_EQ = 5,       /* strlen("name=") */
    XML_PARENT_OPEN = 8,    /* strlen("<parent>") */
    XML_PARENT_CLOSE = 9,   /* strlen("</parent>") */
    XML_GROUP_OPEN = 9,     /* strlen("<groupId>") */
    XML_ARTIFACT_OPEN = 12, /* strlen("<artifactId>") */
};

/* Thread-local int→string for log key-value pairs */
static const char *pkgmap_itoa(int val) {
    static _Thread_local char buf[PKGMAP_ITOA_BUF];
    snprintf(buf, sizeof(buf), "%d", val);
    return buf;
}

/* Check if src at position p starts with literal str of known length. */
static bool at_prefix(const char *p, const char *end, const char *prefix, int prefix_len) {
    return p + prefix_len <= end && memcmp(p, prefix, (size_t)prefix_len) == 0;
}

/* ── Per-worker collection ─────────────────────────────────────── */

void cbm_pkg_entries_init(cbm_pkg_entries_t *e) {
    e->items = NULL;
    e->count = 0;
    e->cap = 0;
}

static void pkg_entries_push(cbm_pkg_entries_t *e, char *pkg_name, char *entry_rel) {
    if (!e || !pkg_name || !entry_rel) {
        free(pkg_name);
        free(entry_rel);
        return;
    }
    if (e->count < 0 || e->cap < 0 || e->count > e->cap) {
        free(pkg_name);
        free(entry_rel);
        return;
    }
    if (e->count >= e->cap) {
        if (e->cap > INT_MAX / PAIR_LEN) {
            free(pkg_name);
            free(entry_rel);
            return;
        }
        int new_cap = e->cap == 0 ? PKGMAP_INIT_CAP : e->cap * PAIR_LEN;
        if ((size_t)new_cap > SIZE_MAX / sizeof(cbm_pkg_entry_t)) {
            free(pkg_name);
            free(entry_rel);
            return;
        }
        cbm_pkg_entry_t *tmp = realloc(e->items, (size_t)new_cap * sizeof(cbm_pkg_entry_t));
        if (!tmp) {
            free(pkg_name);
            free(entry_rel);
            return;
        }
        e->items = tmp;
        e->cap = new_cap;
    }
    e->items[e->count].pkg_name = pkg_name;
    e->items[e->count].entry_rel = entry_rel;
    e->count++;
}

void cbm_pkg_entries_free(cbm_pkg_entries_t *e) {
    for (int i = 0; i < e->count; i++) {
        free(e->items[i].pkg_name);
        free(e->items[i].entry_rel);
    }
    free(e->items);
    e->items = NULL;
    e->count = 0;
    e->cap = 0;
}

/* ── Helpers ───────────────────────────────────────────────────── */

/* Get the basename from a relative path. Returns pointer into rel_path. */
static const char *path_basename(const char *rel_path) {
    const char *last = strrchr(rel_path, '/');
    return last ? last + SKIP_ONE : rel_path;
}

/* Get the directory part of a relative path (without trailing slash).
 * Returns heap-allocated string. For "foo.json" returns "". */
static char *path_dirname(const char *rel_path) {
    const char *last = strrchr(rel_path, '/');
    if (!last) {
        return cbm_strdup("");
    }
    return cbm_strndup(rel_path, (size_t)(last - rel_path));
}

static bool pkgmap_size_add(size_t *total, size_t amount) {
    if (!total || amount > SIZE_MAX - *total) {
        return false;
    }
    *total += amount;
    return true;
}

typedef cbm_walk_path_t pkgmap_path_t;
#define pkgmap_path_init cbm_walk_path_init
#define pkgmap_path_append cbm_walk_path_append
#define pkgmap_path_restore cbm_walk_path_restore
#define pkgmap_path_free cbm_walk_path_free

/* Concatenate three borrowed strings with one checked exact allocation.
 * Runtime and returned memory are O(total bytes); NULL is returned on size
 * overflow or allocation failure. */
static char *pkgmap_join_text(const char *left, const char *separator, const char *right) {
    if (!left || !separator || !right) {
        return NULL;
    }
    size_t left_len = strlen(left);
    size_t separator_len = strlen(separator);
    size_t right_len = strlen(right);
    size_t allocation_size = 0;
    if (!pkgmap_size_add(&allocation_size, left_len) ||
        !pkgmap_size_add(&allocation_size, separator_len) ||
        !pkgmap_size_add(&allocation_size, right_len) ||
        !pkgmap_size_add(&allocation_size, SKIP_ONE)) {
        return NULL;
    }
    char *result = malloc(allocation_size);
    if (!result) {
        return NULL;
    }
    size_t offset = 0;
    memcpy(result + offset, left, left_len);
    offset += left_len;
    memcpy(result + offset, separator, separator_len);
    offset += separator_len;
    memcpy(result + offset, right, right_len);
    offset += right_len;
    result[offset] = '\0';
    return result;
}

char *cbm_pkgmap_join_path(const char *dir, const char *name) {
    pkgmap_path_t path = {0};
    if (!pkgmap_path_init(&path, dir) || !pkgmap_path_append(&path, name)) {
        pkgmap_path_free(&path);
        return NULL;
    }
    return path.data;
}

static char *pkgmap_join_path_parts(const char *dir, const char *const *parts, size_t part_count) {
    /* For P output bytes and S path components, this performs O(P + S) work,
     * returns O(P) owned memory, and uses O(1) auxiliary state. */
    pkgmap_path_t path = {0};
    if (!pkgmap_path_init(&path, dir)) {
        return NULL;
    }
    for (size_t i = 0; i < part_count; i++) {
        if (!pkgmap_path_append(&path, parts[i])) {
            pkgmap_path_free(&path);
            return NULL;
        }
    }
    return path.data;
}

/* Strip a file extension from an owned mutable path in place.
 * "src/index.ts" -> "src/index", "lib/main" -> "lib/main". */
static void strip_extension_in_place(char *path) {
    size_t len = strlen(path);
    for (size_t i = len; i > 0; i--) {
        if (path[i - SKIP_ONE] == '.') {
            path[i - SKIP_ONE] = '\0';
            return;
        }
        if (path[i - SKIP_ONE] == '/') {
            break;
        }
    }
}

/* Join directory + relative entry path, normalize.
 * "packages/foo" + "src/index.ts" → "packages/foo/src/index" (stripped ext) */
static char *join_and_strip(const char *dir, const char *entry) {
    if (!entry || entry[0] == '\0') {
        return NULL;
    }
    /* Skip leading ./ from entry */
    if (entry[0] == '.' && entry[SKIP_ONE] == '/') {
        entry += PAIR_LEN;
    }
    char *result = pkgmap_join_text(dir, dir[0] ? "/" : "", entry);
    if (result) {
        strip_extension_in_place(result);
    }
    return result;
}

/* Check if a string ends with a suffix. */
static bool ends_with(const char *s, const char *suffix) {
    size_t slen = strlen(s);
    size_t suflen = strlen(suffix);
    if (suflen > slen) {
        return false;
    }
    return strcmp(s + slen - suflen, suffix) == 0;
}

/* Find a line starting with a prefix in source. Returns pointer to first char
 * after prefix, or NULL. Handles leading whitespace. */
static const char *find_line_value(const char *src, int src_len, const char *prefix) {
    size_t plen = strlen(prefix);
    const char *p = src;
    const char *end = src + src_len;
    while (p < end) {
        /* Skip leading whitespace */
        while (p < end && (*p == ' ' || *p == '\t')) {
            p++;
        }
        if (p + plen <= end && memcmp(p, prefix, plen) == 0) {
            return p + plen;
        }
        /* Skip to next line */
        while (p < end && *p != '\n') {
            p++;
        }
        if (p < end) {
            p++; /* skip \n */
        }
    }
    return NULL;
}

/* Extract a quoted string value from position. Handles both "..." and '...'
 * Returns heap-allocated string, or NULL. */
static char *extract_quoted(const char *p, const char *end) {
    /* Skip whitespace and = sign */
    while (p < end && (*p == ' ' || *p == '\t' || *p == '=')) {
        p++;
    }
    if (p >= end) {
        return NULL;
    }
    char quote = *p;
    if (quote != '"' && quote != '\'') {
        return NULL;
    }
    p++;
    const char *start = p;
    while (p < end && *p != quote && *p != '\n') {
        p++;
    }
    if (p >= end || *p != quote) {
        return NULL;
    }
    return cbm_strndup(start, (size_t)(p - start));
}

/* ── Language-specific manifest parsers ────────────────────────── */

/* Resolve JS/TS entry point from exports["."] object. */
static const char *resolve_exports_dot(yyjson_val *dot) {
    if (yyjson_is_str(dot)) {
        return yyjson_get_str(dot);
    }
    if (!yyjson_is_obj(dot)) {
        return NULL;
    }
    static const char *keys[] = {"import", "default", "require"};
    for (int i = 0; i < (int)(sizeof(keys) / sizeof(keys[0])); i++) {
        yyjson_val *v = yyjson_obj_get(dot, keys[i]);
        if (yyjson_is_str(v)) {
            return yyjson_get_str(v);
        }
    }
    return NULL;
}

/* Resolve JS/TS entry point from package.json root. */
static const char *resolve_pkg_entry(yyjson_val *root) {
    yyjson_val *exports = yyjson_obj_get(root, "exports");
    if (yyjson_is_obj(exports)) {
        const char *e = resolve_exports_dot(yyjson_obj_get(exports, "."));
        if (e) {
            return e;
        }
    }
    static const char *fallback_keys[] = {"main", "module"};
    for (int i = 0; i < (int)(sizeof(fallback_keys) / sizeof(fallback_keys[0])); i++) {
        yyjson_val *v = yyjson_obj_get(root, fallback_keys[i]);
        if (yyjson_is_str(v)) {
            return yyjson_get_str(v);
        }
    }
    return "src/index.ts"; /* last resort default */
}

/* JS/TS: package.json — name + entry point resolution */
static void parse_package_json(const char *source, int source_len, const char *rel_path,
                               cbm_pkg_entries_t *entries) {
    yyjson_doc *doc = yyjson_read(source, (size_t)source_len, 0);
    if (!doc) {
        return;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return;
    }

    yyjson_val *name_val = yyjson_obj_get(root, "name");
    if (!yyjson_is_str(name_val)) {
        yyjson_doc_free(doc);
        return;
    }
    const char *name = yyjson_get_str(name_val);
    if (!name || name[0] == '\0') {
        yyjson_doc_free(doc);
        return;
    }

    const char *entry = resolve_pkg_entry(root);
    if (entry) {
        char *dir = path_dirname(rel_path);
        if (dir) {
            char *resolved = join_and_strip(dir, entry);
            if (resolved) {
                pkg_entries_push(entries, cbm_strdup(name), resolved);
            }
        }
        free(dir);
    }

    yyjson_doc_free(doc);
}

/* Go: go.mod — module directive */
static void parse_go_mod(const char *source, int source_len, const char *rel_path,
                         cbm_pkg_entries_t *entries) {
    const char *end = source + source_len;
    const char *val = find_line_value(source, source_len, "module ");
    if (!val) {
        return;
    }
    /* Extract module path (rest of line, trimmed) */
    while (val < end && (*val == ' ' || *val == '\t')) {
        val++;
    }
    const char *start = val;
    while (val < end && *val != '\n' && *val != '\r' && *val != ' ') {
        val++;
    }
    if (val <= start) {
        return;
    }
    char *module_path = cbm_strndup(start, (size_t)(val - start));
    char *dir = path_dirname(rel_path);

    /* The module path maps to the directory containing go.mod.
     * For a repository-root manifest, use an empty string. Both allocations
     * transfer to the collection so failure cleanup stays centralized. */
    pkg_entries_push(entries, module_path, dir);
}

/* Extract "name" value from a TOML section. Scans from section_start until
 * the next [...] header or EOF. Returns heap string or NULL. */
static char *toml_extract_name(const char *section_start, const char *end) {
    const char *p = section_start;
    while (p < end) {
        while (p < end && (*p == ' ' || *p == '\t')) {
            p++;
        }
        if (p < end && *p == '[') {
            break;
        }
        if (at_prefix(p, end, "name ", TOML_NAME_SP)) {
            return extract_quoted(p + TOML_NAME_SP, end);
        }
        if (at_prefix(p, end, "name=", TOML_NAME_EQ)) {
            return extract_quoted(p + TOML_NAME_EQ, end);
        }
        while (p < end && *p != '\n') {
            p++;
        }
        if (p < end) {
            p++;
        }
    }
    return NULL;
}

/* Build a manifest-directory-relative path from borrowed segments with one
 * checked exact allocation. For P result bytes and S segments, size+copy cost
 * is O(P + S) time, returned memory is O(P), and auxiliary memory is O(1).
 * No fixed buffer may silently turn a valid manifest path into another path. */
static char *build_entry_path_segments(const char *rel_path, const char *const *segments,
                                       size_t segment_count) {
    if (!rel_path || (!segments && segment_count > 0)) {
        return NULL;
    }
    const char *slash = strrchr(rel_path, '/');
    size_t dir_len = slash ? (size_t)(slash - rel_path) : 0;
    size_t separator_len = dir_len > 0 ? SKIP_ONE : 0;
    size_t length = 0;
    if (!pkgmap_size_add(&length, dir_len) || !pkgmap_size_add(&length, separator_len)) {
        return NULL;
    }
    for (size_t i = 0; i < segment_count; i++) {
        if (!segments[i] || !pkgmap_size_add(&length, strlen(segments[i]))) {
            return NULL;
        }
    }
    size_t allocation_size = length;
    if (!pkgmap_size_add(&allocation_size, SKIP_ONE)) {
        return NULL;
    }
    char *result = malloc(allocation_size);
    if (!result) {
        return NULL;
    }
    size_t offset = 0;
    if (dir_len > 0) {
        memcpy(result, rel_path, dir_len);
        offset = dir_len;
        result[offset++] = '/';
    }
    for (size_t i = 0; i < segment_count; i++) {
        size_t segment_len = strlen(segments[i]);
        memcpy(result + offset, segments[i], segment_len);
        offset += segment_len;
    }
    result[offset] = '\0';
    return result;
}

/* Build an exact manifest-directory-relative entry with one checked
 * allocation. For P total path bytes and S segments, runtime is O(P + S),
 * returned memory is O(P), and auxiliary memory is O(1). */
static char *build_entry_path_parts(const char *rel_path, const char *prefix, const char *suffix) {
    const char *segments[] = {prefix, suffix ? suffix : ""};
    return build_entry_path_segments(rel_path, segments, sizeof(segments) / sizeof(segments[0]));
}

/* Build entry path: dir/suffix or just suffix if dir is empty. */
static char *build_entry_path(const char *rel_path, const char *suffix) {
    return build_entry_path_parts(rel_path, suffix, NULL);
}

/* Rust: Cargo.toml — [package] name */
static void parse_cargo_toml(const char *source, int source_len, const char *rel_path,
                             cbm_pkg_entries_t *entries) {
    const char *section = find_line_value(source, source_len, "[package]");
    if (!section) {
        return;
    }
    char *name = toml_extract_name(section, source + source_len);
    if (!name) {
        return;
    }
    char *entry = build_entry_path(rel_path, "src/lib");
    pkg_entries_push(entries, name, entry);
}

/* Python: pyproject.toml — [project] name */
/* Normalize Python package name: hyphens → underscores (PEP 503). */
static void py_normalize_name(char *name) {
    for (char *c = name; *c; c++) {
        if (*c == '-') {
            *c = '_';
        }
    }
}

static void parse_pyproject_toml(const char *source, int source_len, const char *rel_path,
                                 cbm_pkg_entries_t *entries) {
    const char *section = find_line_value(source, source_len, "[project]");
    if (!section) {
        return;
    }
    char *name = toml_extract_name(section, source + source_len);
    if (!name) {
        return;
    }
    py_normalize_name(name);

    /* Register src/<name>/__init__ as primary entry. */
    const char *primary_segments[] = {"src/", name, "/__init__"};
    char *entry = build_entry_path_segments(rel_path, primary_segments,
                                            sizeof(primary_segments) / sizeof(primary_segments[0]));
    char *name_copy = cbm_strdup(name);
    pkg_entries_push(entries, name, entry);

    /* Also register <name>/__init__ as alternative (no src/ prefix) */
    char *alt_entry = NULL;
    if (name_copy) {
        const char *alt_segments[] = {name_copy, "/__init__"};
        alt_entry = build_entry_path_segments(rel_path, alt_segments,
                                              sizeof(alt_segments) / sizeof(alt_segments[0]));
    }

    if (name_copy && alt_entry) {
        pkg_entries_push(entries, name_copy, alt_entry);
    } else {
        free(name_copy);
        free(alt_entry);
    }
}

/* Extract PSR-4 autoload entries from composer.json root. */
static void extract_psr4(yyjson_val *root, const char *dir, cbm_pkg_entries_t *entries) {
    yyjson_val *autoload = yyjson_obj_get(root, "autoload");
    if (!yyjson_is_obj(autoload)) {
        return;
    }
    yyjson_val *psr4 = yyjson_obj_get(autoload, "psr-4");
    if (!yyjson_is_obj(psr4)) {
        return;
    }
    yyjson_val *key = NULL;
    yyjson_obj_iter iter = yyjson_obj_iter_with(psr4);
    while ((key = yyjson_obj_iter_next(&iter)) != NULL) {
        yyjson_val *val = yyjson_obj_iter_get_val(key);
        if (!yyjson_is_str(key) || !yyjson_is_str(val)) {
            continue;
        }
        const char *ns_prefix = yyjson_get_str(key);
        const char *ns_dir = yyjson_get_str(val);
        char *ns_entry = pkgmap_join_text(dir, dir[0] ? "/" : "", ns_dir);
        size_t nelen = ns_entry ? strlen(ns_entry) : 0;
        if (nelen > 0 && ns_entry[nelen - SKIP_ONE] == '/') {
            ns_entry[nelen - SKIP_ONE] = '\0';
        }
        pkg_entries_push(entries, cbm_strdup(ns_prefix), ns_entry);
    }
}

/* PHP: composer.json — name + PSR-4 autoload */
static void parse_composer_json(const char *source, int source_len, const char *rel_path,
                                cbm_pkg_entries_t *entries) {
    yyjson_doc *doc = yyjson_read(source, (size_t)source_len, 0);
    if (!doc) {
        return;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return;
    }

    char *dir = path_dirname(rel_path);
    if (!dir) {
        yyjson_doc_free(doc);
        return;
    }

    /* Register package name → directory */
    yyjson_val *name_val = yyjson_obj_get(root, "name");
    if (yyjson_is_str(name_val)) {
        const char *name = yyjson_get_str(name_val);
        if (name && name[0] != '\0') {
            pkg_entries_push(entries, cbm_strdup(name), cbm_strdup(dir));
        }
    }

    extract_psr4(root, dir, entries);

    free(dir);
    yyjson_doc_free(doc);
}

/* Dart: pubspec.yaml — name */
static void parse_pubspec_yaml(const char *source, int source_len, const char *rel_path,
                               cbm_pkg_entries_t *entries) {
    cbm_yaml_node_t *root = cbm_yaml_parse(source, source_len);
    if (!root) {
        return;
    }
    const char *name = cbm_yaml_get_str(root, "name");
    if (name && name[0] != '\0') {
        char *entry = build_entry_path(rel_path, "lib");
        pkg_entries_push(entries, cbm_strdup(name), entry);
    }
    cbm_yaml_free(root);
}

/* Extract text content of an XML tag at position p. Returns heap string or NULL.
 * p must point to the char after the opening tag's '>'. */
static char *xml_tag_content(const char *p, const char *end) {
    const char *s = p;
    while (p < end && *p != '<') {
        p++;
    }
    if (p <= s) {
        return NULL;
    }
    return cbm_strndup(s, (size_t)(p - s));
}

/* Java: pom.xml — <groupId> + <artifactId> */
/* Scan pom.xml for a top-level XML tag (outside <parent>). Returns heap string or NULL. */
static char *pom_find_tag(const char *source, const char *end, const char *tag, int tag_len) {
    const char *p = source;
    bool in_parent = false;
    while (p < end) {
        if (at_prefix(p, end, "<parent>", XML_PARENT_OPEN)) {
            in_parent = true;
        }
        if (at_prefix(p, end, "</parent>", XML_PARENT_CLOSE)) {
            in_parent = false;
        }
        if (!in_parent && at_prefix(p, end, tag, tag_len)) {
            return xml_tag_content(p + tag_len, end);
        }
        p++;
    }
    return NULL;
}

static void parse_pom_xml(const char *source, int source_len, const char *rel_path,
                          cbm_pkg_entries_t *entries) {
    const char *end = source + source_len;
    char *group_id = pom_find_tag(source, end, "<groupId>", XML_GROUP_OPEN);
    char *artifact_id = pom_find_tag(source, end, "<artifactId>", XML_ARTIFACT_OPEN);

    if (group_id && artifact_id) {
        /* Map: "com.myorg.myapp" → src/main/java directory */
        char *pkg_name = pkgmap_join_text(group_id, ".", artifact_id);
        char *entry = build_entry_path(rel_path, "src/main/java");
        char *grp_entry = entry ? cbm_strdup(entry) : NULL;
        pkg_entries_push(entries, pkg_name, entry);

        /* Also register just the groupId for package-level imports */
        pkg_entries_push(entries, cbm_strdup(group_id), grp_entry);
    }

    free(group_id);
    free(artifact_id);
}

/* Gradle: build.gradle / build.gradle.kts — group = '...' */
static void parse_build_gradle(const char *source, int source_len, const char *rel_path,
                               cbm_pkg_entries_t *entries) {
    const char *end = source + source_len;
    /* Look for group = '...' or group '...' or group = "..." */
    const char *val = find_line_value(source, source_len, "group");
    if (!val) {
        return;
    }
    char *group = extract_quoted(val, end);
    if (!group) {
        return;
    }
    /* Check for src/main/java or src/main/kotlin */
    char *entry = build_entry_path(rel_path, "src/main/java");
    pkg_entries_push(entries, group, entry);
}

/* Elixir: mix.exs — app: :name */
static void parse_mix_exs(const char *source, int source_len, const char *rel_path,
                          cbm_pkg_entries_t *entries) {
    const char *end = source + source_len;
    /* Look for app: :app_name */
    const char *val = find_line_value(source, source_len, "app:");
    if (!val) {
        return;
    }
    while (val < end && (*val == ' ' || *val == '\t')) {
        val++;
    }
    if (val >= end || *val != ':') {
        return;
    }
    val++; /* skip : */
    const char *start = val;
    while (val < end && *val != ',' && *val != '\n' && *val != ' ' && *val != ')') {
        val++;
    }
    if (val <= start) {
        return;
    }
    char *app_name = cbm_strndup(start, (size_t)(val - start));
    const char *segments[] = {"lib/", app_name};
    char *entry =
        build_entry_path_segments(rel_path, segments, sizeof(segments) / sizeof(segments[0]));
    pkg_entries_push(entries, app_name, entry);
}

/* Ruby: *.gemspec — spec.name = '...' */
static void parse_gemspec(const char *source, int source_len, const char *rel_path,
                          cbm_pkg_entries_t *entries) {
    const char *end = source + source_len;
    /* Try spec.name, s.name, gem.name patterns */
    static const char *patterns[] = {".name", NULL};
    for (int i = 0; patterns[i]; i++) {
        const char *p = source;
        while (p < end) {
            const char *found = strstr(p, patterns[i]);
            if (!found || found >= end) {
                break;
            }
            char *name = extract_quoted(found + strlen(patterns[i]), end);
            if (name) {
                const char *segments[] = {"lib/", name};
                char *entry = build_entry_path_segments(rel_path, segments,
                                                        sizeof(segments) / sizeof(segments[0]));
                pkg_entries_push(entries, name, entry);
                return;
            }
            p = found + SKIP_ONE;
        }
    }
}

/* Swift: Package.swift — SwiftPM manifest.
 *
 * Package.swift is executable Swift, not declarative, so this is a
 * hand-rolled literal pattern-extractor (same spirit as parse_cargo_toml),
 * not a Swift evaluator: it locates regular `.target(...)` and
 * `.executableTarget(...)` source declarations via a comment/string-aware
 * token scan and pulls only their quoted-string-literal `name:` / `path:`
 * arguments. Anything computed or concatenated is skipped, not guessed —
 * fail-closed, like every other parser here.
 *
 * Only a manifest's OWN source-target declarations self-register — never
 * `.library(...)` products (a product name is not generally an importable
 * module: SwiftPM lets it alias multiple targets, or none sharing its
 * name), and never `.package(url:)` / `.package(path:)` dependencies or
 * target-to-target dependency references. A dependency resolves because
 * the DEPENDENCY's own Package.swift registers itself when the repo-wide
 * manifest walk (cbm_pkgmap_scan_repo) reaches it, exactly like a JS
 * workspace sibling's package.json (see repro_issue408.c).
 *
 * Deliberately out of scope (matches the item-1 authorization): evaluating
 * `#if`/`#endif` conditional-compilation blocks. A source target inside one
 * is scanned like any other — teaching the extractor which platform
 * conditions are "active" would need a Swift semantic tier, which this PR
 * was explicitly asked not to add. */

/* One step of comment/string-aware scanning, shared by swift_find_code_token
 * and swift_match_paren below. If `*pp` sits inside, or is entering, a `//`
 * line comment, a nesting-aware slash-star block comment (continued across
 * calls via `*block_depth`), or a "..." string literal (escape aware,
 * tracked via `*in_str`), advances `*pp` past that one step and returns
 * true. Otherwise leaves `*pp` untouched and returns false, so the caller
 * handles the "real code" character itself (paren tracking, needle
 * matching, ...). */
static bool swift_skip_comment_or_string(const char **pp, const char *end, int *block_depth,
                                         bool *in_str) {
    const char *p = *pp;
    if (*block_depth > 0) {
        if (p + SKIP_ONE < end && p[0] == '/' && p[SKIP_ONE] == '*') {
            (*block_depth)++;
            *pp = p + PAIR_LEN;
        } else if (p + SKIP_ONE < end && p[0] == '*' && p[SKIP_ONE] == '/') {
            (*block_depth)--;
            *pp = p + PAIR_LEN;
        } else {
            *pp = p + SKIP_ONE;
        }
        return true;
    }
    if (*in_str) {
        if (*p == '\\' && p + SKIP_ONE < end) {
            *pp = p + PAIR_LEN;
        } else {
            if (*p == '"') {
                *in_str = false;
            }
            *pp = p + SKIP_ONE;
        }
        return true;
    }
    if (p + SKIP_ONE < end && p[0] == '/' && p[SKIP_ONE] == '/') {
        while (p < end && *p != '\n') {
            p++;
        }
        *pp = p;
        return true;
    }
    if (p + SKIP_ONE < end && p[0] == '/' && p[SKIP_ONE] == '*') {
        *block_depth = SKIP_ONE;
        *pp = p + PAIR_LEN;
        return true;
    }
    if (*p == '"') {
        *in_str = true;
        *pp = p + SKIP_ONE;
        return true;
    }
    return false;
}

/* Scans [start, end) for the next occurrence of `needle` that is not inside
 * a `//` line comment, a nesting-aware slash-star block comment, or a "..."
 * string literal (backslash-escapes honored). This is the single source of
 * truth for "real code" scanning below — a `.target(`, `name:`, or `path:`
 * spelled inside a comment or a string constant must never be mistaken for
 * a live declaration. Returns a pointer to the match, or NULL. */
static const char *swift_find_code_token(const char *start, const char *end, const char *needle) {
    size_t nlen = strlen(needle);
    int block_depth = 0;
    bool in_str = false;
    const char *p = start;
    while (p < end) {
        if (swift_skip_comment_or_string(&p, end, &block_depth, &in_str)) {
            continue;
        }
        if ((size_t)(end - p) >= nlen && memcmp(p, needle, nlen) == 0) {
            return p;
        }
        p++;
    }
    return NULL;
}

/* Find the matching ')' for the '(' at `open`, scanning forward to `end`.
 * Delegates comment/string spans to swift_skip_comment_or_string — matching
 * swift_find_code_token — so a ')', '(', or comment delimiter inside a
 * quoted argument value or a comment never perturbs the depth count.
 * Returns NULL for an unbalanced/unterminated call — the caller treats
 * that span as unparseable and skips it (fail-closed). */
static const char *swift_match_paren(const char *open, const char *end) {
    int depth = 0;
    int block_depth = 0;
    bool in_str = false;
    const char *p = open;
    while (p < end) {
        if (swift_skip_comment_or_string(&p, end, &block_depth, &in_str)) {
            continue;
        }
        if (*p == '(') {
            depth++;
        } else if (*p == ')') {
            depth--;
            if (depth == 0) {
                return p;
            }
        }
        p++;
    }
    return NULL;
}

/* Extract a bare double-quoted string-literal value at `p` (after skipping
 * leading whitespace and honoring backslash-escapes inside the literal),
 * requiring the closing quote be immediately followed by a comma, or by
 * `end` — rejects `name: "Foo" + suffix`-style concatenation, which a plain
 * quote-scan alone cannot tell apart from a true literal. Every caller here
 * passes the enclosing call's own closing ')' position as `end`, so landing
 * exactly on it after the closing quote correctly means "immediately
 * followed by the close-paren", not just "ran off the end of the buffer".
 * Returns heap string, or NULL (fail-closed). */
static char *swift_quoted_literal(const char *p, const char *end) {
    while (p < end && (*p == ' ' || *p == '\t')) {
        p++;
    }
    if (p >= end || *p != '"') {
        return NULL;
    }
    p++;
    const char *start = p;
    while (p < end && *p != '"' && *p != '\n') {
        if (*p == '\\' && p + SKIP_ONE < end) {
            p += PAIR_LEN;
            continue;
        }
        p++;
    }
    if (p >= end || *p != '"') {
        return NULL;
    }
    char *value = cbm_strndup(start, (size_t)(p - start));
    p++; /* past closing quote */
    while (p < end && (*p == ' ' || *p == '\t')) {
        p++;
    }
    if (p >= end || *p == ',') {
        return value;
    }
    free(value);
    return NULL;
}

/* Search [start, end) for `needle` via swift_find_code_token (skipping
 * comments/strings), then extract the bare quoted-literal argument
 * immediately following it via swift_quoted_literal. Bounded to `end` so a
 * match can never leak past the enclosing call's own argument list into a
 * later, unrelated declaration. When `out_found` is non-NULL, it is set to
 * whether `needle` was located at all — independent of whether its value
 * parsed as a bare literal — so callers can tell "argument absent" (fine,
 * apply a default) apart from "argument present but not a literal"
 * (unknowable, fail closed). Returns heap string or NULL. */
static char *swift_extract_after(const char *start, const char *end, const char *needle,
                                 bool *out_found) {
    if (out_found) {
        *out_found = false;
    }
    const char *hit = swift_find_code_token(start, end, needle);
    if (!hit) {
        return NULL;
    }
    if (out_found) {
        *out_found = true;
    }
    return swift_quoted_literal(hit + strlen(needle), end);
}

/* Register `target_name` → its resolved source directory: the literal
 * `path:` argument the target declared, if any, else the conventional
 * `Sources/<target_name>` (fixed-convention, same approach parse_cargo_toml
 * takes for `src/lib`). `literal_path` is borrowed; caller retains
 * ownership. Runtime and returned memory are O(path bytes). */
static void swift_register_target(const char *rel_path, const char *target_name,
                                  const char *literal_path, cbm_pkg_entries_t *entries) {
    if (!target_name || !target_name[0]) {
        return;
    }
    char *entry = literal_path && literal_path[0]
                      ? build_entry_path(rel_path, literal_path)
                      : build_entry_path_parts(rel_path, "Sources/", target_name);
    if (entry) {
        pkg_entries_push(entries, cbm_strdup(target_name), entry);
    }
}

typedef struct {
    const char *name;
    size_t name_len;
} swift_source_target_factory_t;

/* Recognize PackageDescription factories whose default source directory is
 * Sources/<name>. Test, plugin, binary, and system-library targets have
 * different layout/vending rules and must not be guessed through this path. */
static const swift_source_target_factory_t *swift_source_target_factory_at(const char *dot,
                                                                           const char *end,
                                                                           const char **out_open) {
    static const swift_source_target_factory_t factories[] = {
        {"target", sizeof("target") - SKIP_ONE},
        {"executableTarget", sizeof("executableTarget") - SKIP_ONE},
    };
    const char *name = dot + SKIP_ONE;
    for (size_t i = 0; i < sizeof(factories) / sizeof(factories[0]); i++) {
        const swift_source_target_factory_t *factory = &factories[i];
        if ((size_t)(end - name) < factory->name_len ||
            memcmp(name, factory->name, factory->name_len) != 0) {
            continue;
        }
        const char *p = name + factory->name_len;
        while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) {
            p++;
        }
        if (p < end && *p == '(') {
            *out_open = p;
            return factory;
        }
    }
    return NULL;
}

/* Scan once for regular and executable source-target declarations, skipping
 * every candidate inside a comment or string literal.
 * Non-overlapping: the scan resumes AFTER each matched close paren, so a
 * `dependencies: [.target(name: "Bar")]` reference nested inside Foo's own
 * argument list is never re-visited as a second top-level target.
 *
 * A target with a `path:` argument that isn't a bare literal (computed,
 * concatenated, ...) is skipped entirely, even though its `name:` may be a
 * valid literal: SwiftPM would use the computed path, not the
 * `Sources/<name>` convention, and guessing that convention anyway would
 * mint a location that is not just unconfirmed but actively likely wrong. */
static void swift_scan_targets(const char *source, const char *end, const char *rel_path,
                               cbm_pkg_entries_t *entries) {
    const char *cursor = source;
    while (cursor < end) {
        const char *hit = swift_find_code_token(cursor, end, ".");
        if (!hit) {
            return;
        }
        const char *open = NULL;
        if (!swift_source_target_factory_at(hit, end, &open)) {
            cursor = hit + SKIP_ONE;
            continue;
        }
        const char *close = swift_match_paren(open, end);
        if (!close) {
            return; /* unbalanced — nothing further here is trustworthy */
        }
        char *name = swift_extract_after(open, close, "name:", NULL);
        if (name) {
            bool path_present = false;
            char *literal_path = swift_extract_after(open, close, "path:", &path_present);
            if (literal_path || !path_present) {
                swift_register_target(rel_path, name, literal_path, entries);
            }
            free(literal_path);
            free(name);
        }
        cursor = close + SKIP_ONE;
    }
}

/* SwiftPM: Package.swift — regular/executable source targets → Sources/<name>
 * (or their literal `path:`). Products deliberately do not self-register: see
 * the file comment above swift_find_code_token. */
static void parse_package_swift(const char *source, int source_len, const char *rel_path,
                                cbm_pkg_entries_t *entries) {
    const char *end = source + source_len;
    swift_scan_targets(source, end, rel_path, entries);
}

/* ── Public: manifest detection + parsing ──────────────────────── */

bool cbm_pkgmap_try_parse(const char *basename, const char *rel_path, const char *source,
                          int source_len, cbm_pkg_entries_t *entries) {
    if (!basename || !source || source_len <= 0) {
        return false;
    }

    if (strcmp(basename, "package.json") == 0) {
        parse_package_json(source, source_len, rel_path, entries);
        return true;
    }
    if (strcmp(basename, "go.mod") == 0) {
        parse_go_mod(source, source_len, rel_path, entries);
        return true;
    }
    if (strcmp(basename, "Cargo.toml") == 0) {
        parse_cargo_toml(source, source_len, rel_path, entries);
        return true;
    }
    if (strcmp(basename, "pyproject.toml") == 0) {
        parse_pyproject_toml(source, source_len, rel_path, entries);
        return true;
    }
    if (strcmp(basename, "composer.json") == 0) {
        parse_composer_json(source, source_len, rel_path, entries);
        return true;
    }
    if (strcmp(basename, "pubspec.yaml") == 0) {
        parse_pubspec_yaml(source, source_len, rel_path, entries);
        return true;
    }
    if (strcmp(basename, "pom.xml") == 0) {
        parse_pom_xml(source, source_len, rel_path, entries);
        return true;
    }
    if (strcmp(basename, "build.gradle") == 0 || strcmp(basename, "build.gradle.kts") == 0) {
        parse_build_gradle(source, source_len, rel_path, entries);
        return true;
    }
    if (strcmp(basename, "mix.exs") == 0) {
        parse_mix_exs(source, source_len, rel_path, entries);
        return true;
    }
    if (ends_with(basename, ".gemspec")) {
        parse_gemspec(source, source_len, rel_path, entries);
        return true;
    }
    if (strcmp(basename, "Package.swift") == 0) {
        parse_package_swift(source, source_len, rel_path, entries);
        return true;
    }
    return false;
}

/* ── Merge: per-worker entries → hash table ────────────────────── */

CBMHashTable *cbm_pkgmap_build(cbm_pkg_entries_t *worker_entries, int worker_count,
                               const char *project_name) {
    /* Count total entries */
    int total = 0;
    for (int w = 0; w < worker_count; w++) {
        total += worker_entries[w].count;
    }
    if (total == 0) {
        return NULL;
    }

    CBMHashTable *map = cbm_ht_create(PKGMAP_HT_INIT);
    int merged = 0;

    for (int w = 0; w < worker_count; w++) {
        cbm_pkg_entries_t *we = &worker_entries[w];
        for (int i = 0; i < we->count; i++) {
            /* Convert entry_rel to QN: project.dir.parts */
            char *qn = cbm_pipeline_fqn_module(project_name, we->items[i].entry_rel);
            if (!qn) {
                continue;
            }

            /* Check for duplicate — first wins */
            if (cbm_ht_has(map, we->items[i].pkg_name)) {
                free(qn);
                continue;
            }

            /* Transfer ownership: key = strdup'd pkg_name, value = qn */
            char *key = strdup(we->items[i].pkg_name);
            cbm_ht_set(map, key, qn);
            merged++;
        }
    }

    if (merged == 0) {
        cbm_ht_free(map);
        return NULL;
    }
    cbm_log_info("pkgmap.build", "entries", pkgmap_itoa(merged));
    return map;
}

/* Returns true if basename is a package manifest we know how to parse.
 * Used by the filesystem walker; cbm_pkgmap_try_parse is the source of
 * truth for which basenames produce entries. */
static bool is_pkgmap_manifest_basename(const char *basename) {
    if (!basename) {
        return false;
    }
    if (strcmp(basename, "package.json") == 0 || strcmp(basename, "go.mod") == 0 ||
        strcmp(basename, "Cargo.toml") == 0 || strcmp(basename, "pyproject.toml") == 0 ||
        strcmp(basename, "composer.json") == 0 || strcmp(basename, "pubspec.yaml") == 0 ||
        strcmp(basename, "pom.xml") == 0 || strcmp(basename, "build.gradle") == 0 ||
        strcmp(basename, "build.gradle.kts") == 0 || strcmp(basename, "mix.exs") == 0 ||
        strcmp(basename, "Package.swift") == 0) {
        return true;
    }
    return ends_with(basename, ".gemspec");
}

/* Stat a path, skipping symlinks. Returns 0 on success, -1 to skip.
 * On POSIX, lstat + S_ISLNK avoids following symlink cycles. On Windows
 * we use the UTF-8-safe wide stat (mirroring discover.c's wide_stat);
 * reparse points (junctions/symlinks) are detected separately by
 * pkgmap_is_reparse_point below before we descend. Mirrors discover.c's
 * safe_stat. */
static int pkgmap_safe_stat(const char *abs_path, struct stat *st) {
#ifdef _WIN32
    wchar_t *wpath = cbm_path_to_wide(abs_path);
    if (!wpath) {
        return CBM_NOT_FOUND;
    }
    struct _stat64 wst;
    int ret = _wstat64(wpath, &wst);
    free(wpath);
    if (ret != 0) {
        return CBM_NOT_FOUND;
    }
    st->st_mode = wst.st_mode;
    st->st_size = wst.st_size;
    st->st_mtime = wst.st_mtime;
    return 0;
#else
    if (lstat(abs_path, st) != 0) {
        return CBM_NOT_FOUND;
    }
    if (S_ISLNK(st->st_mode)) {
        return CBM_NOT_FOUND;
    }
    return 0;
#endif
}

/* Windows directory junctions, mount points, and symlinks are reparse
 * points. The walker must distinguish "not a reparse point" from "could not
 * inspect attributes": after removing the lossy depth cap, treating an
 * inspection failure as safe-to-follow would invalidate the cycle-prevention
 * proof. POSIX symlinks are already rejected by pkgmap_safe_stat. */
#ifdef _WIN32
typedef enum {
    PKGMAP_REPARSE_INSPECTION_FAILED = -1,
    PKGMAP_REPARSE_CLEAR = 0,
    PKGMAP_REPARSE_PRESENT = 1,
} pkgmap_reparse_status_t;

static pkgmap_reparse_status_t pkgmap_reparse_status(const char *abs_path) {
    wchar_t *wpath = cbm_path_to_wide(abs_path);
    if (!wpath) {
        return PKGMAP_REPARSE_INSPECTION_FAILED;
    }
    DWORD attrs = GetFileAttributesW(wpath);
    free(wpath);
    if (attrs == INVALID_FILE_ATTRIBUTES) {
        return PKGMAP_REPARSE_INSPECTION_FAILED;
    }
    return (attrs & FILE_ATTRIBUTE_REPARSE_POINT) != 0 ? PKGMAP_REPARSE_PRESENT
                                                       : PKGMAP_REPARSE_CLEAR;
}
#endif

typedef struct {
    cbm_dir_t *dir;
    size_t abs_parent_len;
    size_t rel_parent_len;
    /* POSIX only: owned key also borrowed by active_identities. Windows
     * prevents cyclic edges by rejecting every reparse point instead. */
    char *identity_key;
} pkgmap_walk_frame_t;

typedef struct {
    pkgmap_walk_frame_t *frames;
    size_t count;
    size_t capacity;
#ifndef _WIN32
    CBMHashTable *active_identities;
#endif
} pkgmap_walk_stack_t;

typedef enum {
    PKGMAP_WALK_PUSH_FAILED = -1,
    PKGMAP_WALK_PUSH_CYCLE = 0,
    PKGMAP_WALK_PUSHED = 1,
} pkgmap_walk_push_result_t;

#ifndef _WIN32
static char pkgmap_active_identity_present;
#endif

#ifndef _WIN32
static bool pkgmap_identity_key(const cbm_file_identity_t *identity,
                                char key[PKGMAP_IDENTITY_KEY_BUFSZ]) {
    if (!identity || !identity->valid) {
        return false;
    }
    int written = snprintf(key, PKGMAP_IDENTITY_KEY_BUFSZ, "%0*" PRIx64 ":%0*" PRIx64,
                           PKGMAP_IDENTITY_HEX_LEN, identity->volume, PKGMAP_IDENTITY_HEX_LEN,
                           identity->file);
    return written == PKGMAP_IDENTITY_KEY_LEN;
}
#endif

/* Add one already-open directory frame. Capacity growth is geometric and
 * therefore amortized O(1) per descent. On POSIX, the active-ancestor set is
 * expected O(1) lookup/insert and O(depth) memory; removing its key at pop
 * avoids retaining one identity per directory in wide repositories. */
static pkgmap_walk_push_result_t pkgmap_walk_stack_push(pkgmap_walk_stack_t *stack, cbm_dir_t *dir,
                                                        size_t abs_parent_len,
                                                        size_t rel_parent_len,
                                                        const cbm_file_identity_t *identity) {
    if (!stack || !dir) {
        return PKGMAP_WALK_PUSH_FAILED;
    }
    char *owned_identity_key = NULL;
#ifndef _WIN32
    char identity_key[PKGMAP_IDENTITY_KEY_BUFSZ];
    if (!stack->active_identities || !pkgmap_identity_key(identity, identity_key)) {
        return PKGMAP_WALK_PUSH_FAILED;
    }
    if (cbm_ht_has(stack->active_identities, identity_key)) {
        return PKGMAP_WALK_PUSH_CYCLE;
    }
    owned_identity_key = cbm_strdup(identity_key);
    if (!owned_identity_key) {
        return PKGMAP_WALK_PUSH_FAILED;
    }
#else
    (void)identity;
#endif

    if (stack->count == stack->capacity) {
        size_t new_capacity =
            stack->capacity == 0 ? PKGMAP_WALK_STACK_INIT : stack->capacity * PAIR_LEN;
        if (new_capacity < stack->capacity ||
            new_capacity > SIZE_MAX / sizeof(pkgmap_walk_frame_t)) {
            free(owned_identity_key);
            return PKGMAP_WALK_PUSH_FAILED;
        }
        pkgmap_walk_frame_t *grown =
            realloc(stack->frames, new_capacity * sizeof(pkgmap_walk_frame_t));
        if (!grown) {
            free(owned_identity_key);
            return PKGMAP_WALK_PUSH_FAILED;
        }
        stack->frames = grown;
        stack->capacity = new_capacity;
    }
#ifndef _WIN32
    cbm_ht_set(stack->active_identities, owned_identity_key, &pkgmap_active_identity_present);
    if (!cbm_ht_has(stack->active_identities, owned_identity_key)) {
        free(owned_identity_key);
        return PKGMAP_WALK_PUSH_FAILED;
    }
#endif
    stack->frames[stack->count++] = (pkgmap_walk_frame_t){
        .dir = dir,
        .abs_parent_len = abs_parent_len,
        .rel_parent_len = rel_parent_len,
        .identity_key = owned_identity_key,
    };
    return PKGMAP_WALK_PUSHED;
}

static void pkgmap_walk_stack_pop(pkgmap_walk_stack_t *stack, pkgmap_path_t *abs_path,
                                  pkgmap_path_t *rel_path) {
    if (!stack || stack->count == 0) {
        return;
    }
    pkgmap_walk_frame_t *frame = &stack->frames[stack->count - SKIP_ONE];
    cbm_closedir(frame->dir);
#ifndef _WIN32
    if (frame->identity_key) {
        (void)cbm_ht_delete(stack->active_identities, frame->identity_key);
    }
#endif
    free(frame->identity_key);
    pkgmap_path_restore(abs_path, frame->abs_parent_len);
    pkgmap_path_restore(rel_path, frame->rel_parent_len);
    stack->count--;
}

static void pkgmap_walk_stack_free(pkgmap_walk_stack_t *stack, pkgmap_path_t *abs_path,
                                   pkgmap_path_t *rel_path) {
    if (!stack) {
        return;
    }
    while (stack->count > 0) {
        pkgmap_walk_stack_pop(stack, abs_path, rel_path);
    }
#ifndef _WIN32
    cbm_ht_free(stack->active_identities);
#endif
    free(stack->frames);
    memset(stack, 0, sizeof(*stack));
}

/* Iterative filesystem walker that finds package manifests independently of
 * the main discovery filter. It keeps one portable directory handle per
 * active depth, the same resource order as the former recursive DFS, but no
 * C call-stack growth and no capability-changing depth ceiling.
 *
 * Termination: POSIX lstat rejects symlinks and an expected-O(1)
 * active-ancestor identity set rejects bind-mount/alias cycles. Windows
 * rejects all reparse points and fails closed when their attributes cannot be
 * inspected. Thus every accepted descent is an acyclic tree edge.
 *
 * Complexity: walker-owned work is expected O(E + N + B), where E is visited
 * entries, N is their total name bytes, and B is parsed manifest bytes, plus
 * delegated filesystem, exclusion-policy, and parser costs. Stack growth and
 * identity-set operations are amortized/expected O(1). Live auxiliary memory
 * and open handles are O(D), plus O(P) for the longest exact path, where D is
 * current depth and P is path bytes; emitted package entries are output, not
 * auxiliary storage. */
static int pkgmap_walk_dir(pkgmap_path_t *abs_path, pkgmap_path_t *rel_path,
                           cbm_pkg_entries_t *entries, char **excluded_dirs, int excluded_count) {
    pkgmap_walk_stack_t stack = {0};
#ifndef _WIN32
    stack.active_identities = cbm_ht_create(PKGMAP_WALK_STACK_INIT);
    if (!stack.active_identities) {
        cbm_log_warn("pkgmap.walk_skipped", "path", abs_path->data, "reason",
                     "identity_set_allocation_failed");
        return 0;
    }
#endif

    cbm_file_identity_t root_identity = {0};
#ifndef _WIN32
    if (!cbm_file_identity_read(abs_path->data, &root_identity)) {
        cbm_log_warn("pkgmap.walk_skipped", "path", abs_path->data, "reason",
                     "root_identity_unavailable");
        pkgmap_walk_stack_free(&stack, abs_path, rel_path);
        return 0;
    }
#endif
    cbm_dir_t *root = cbm_opendir(abs_path->data);
    if (!root) {
        cbm_log_warn("pkgmap.walk_skipped", "path", abs_path->data, "reason",
                     "directory_open_failed");
        pkgmap_walk_stack_free(&stack, abs_path, rel_path);
        return 0;
    }
    pkgmap_walk_push_result_t root_push =
        pkgmap_walk_stack_push(&stack, root, abs_path->length, rel_path->length, &root_identity);
    if (root_push != PKGMAP_WALK_PUSHED) {
        cbm_closedir(root);
        cbm_log_warn("pkgmap.walk_skipped", "path", abs_path->data, "reason",
                     "walk_stack_allocation_failed");
        pkgmap_walk_stack_free(&stack, abs_path, rel_path);
        return 0;
    }

    int parsed = 0;
    while (stack.count > 0) {
        pkgmap_walk_frame_t *frame = &stack.frames[stack.count - SKIP_ONE];
        cbm_dirent_t *entry = cbm_readdir(frame->dir);
        if (!entry) {
            pkgmap_walk_stack_pop(&stack, abs_path, rel_path);
            continue;
        }
        const char *name = entry->name;
        if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'))) {
            continue;
        }
        size_t abs_parent_len = abs_path->length;
        size_t rel_parent_len = rel_path->length;
        if (!pkgmap_path_append(abs_path, name) || !pkgmap_path_append(rel_path, name)) {
            pkgmap_path_restore(abs_path, abs_parent_len);
            pkgmap_path_restore(rel_path, rel_parent_len);
            cbm_log_warn("pkgmap.walk_entry_skipped", "dir", abs_path->data, "entry", name,
                         "reason", "path_allocation_failed");
            continue;
        }

        struct stat st;
        int stat_result = pkgmap_safe_stat(abs_path->data, &st);
        if (stat_result == 0 && S_ISDIR(st.st_mode)) {
            bool descend =
                !cbm_should_skip_dir(name, CBM_MODE_FULL) &&
                !cbm_pipeline_relpath_is_excluded(rel_path->data, excluded_dirs, excluded_count);
#ifdef _WIN32
            if (descend) {
                pkgmap_reparse_status_t reparse = pkgmap_reparse_status(abs_path->data);
                if (reparse != PKGMAP_REPARSE_CLEAR) {
                    descend = false;
                    if (reparse == PKGMAP_REPARSE_INSPECTION_FAILED) {
                        cbm_log_warn("pkgmap.walk_entry_skipped", "path", abs_path->data, "reason",
                                     "reparse_inspection_failed");
                    }
                }
            }
#endif
            if (descend) {
                cbm_file_identity_t identity = {0};
#ifndef _WIN32
                identity.volume = (uint64_t)st.st_dev;
                identity.file = (uint64_t)st.st_ino;
                identity.valid = true;
#endif
                cbm_dir_t *child = cbm_opendir(abs_path->data);
                if (child) {
                    pkgmap_walk_push_result_t pushed = pkgmap_walk_stack_push(
                        &stack, child, abs_parent_len, rel_parent_len, &identity);
                    if (pushed == PKGMAP_WALK_PUSHED) {
                        continue;
                    }
                    cbm_closedir(child);
                    cbm_log_warn("pkgmap.walk_entry_skipped", "path", abs_path->data, "reason",
                                 pushed == PKGMAP_WALK_PUSH_CYCLE ? "directory_cycle"
                                                                  : "walk_stack_allocation_failed");
                } else {
                    cbm_log_warn("pkgmap.walk_entry_skipped", "path", abs_path->data, "reason",
                                 "directory_open_failed");
                }
            }
        } else if (stat_result == 0 && S_ISREG(st.st_mode) && is_pkgmap_manifest_basename(name)) {
            int source_len = 0;
            char *source = pkgmap_read_file(abs_path->data, &source_len);
            if (source) {
                if (cbm_pkgmap_try_parse(name, rel_path->data, source, source_len, entries)) {
                    parsed++;
                }
                free(source);
            }
        }
        pkgmap_path_restore(abs_path, abs_parent_len);
        pkgmap_path_restore(rel_path, rel_parent_len);
    }
    pkgmap_walk_stack_free(&stack, abs_path, rel_path);
    return parsed;
}

/* Scan a repository for package manifest files via the filesystem
 * walker above. Always-available companion to the parallel path's
 * per-worker manifest parsing, which is bound to whatever `files[]`
 * the discoverer produces and therefore misses ignored manifests like
 * package.json. NULL-safe; returns 0 entries when repo_path is unset.
 *
 * Cross-platform: pkgmap_walk_dir uses an iterative exact-path DFS with
 * platform-appropriate cycle prevention, so directory depth cannot silently
 * remove package capability or overflow the C call stack. */
int cbm_pkgmap_scan_repo(const char *repo_path, cbm_pkg_entries_t *entries, char **excluded_dirs,
                         int excluded_count) {
    if (!repo_path || !entries) {
        return 0;
    }
    pkgmap_path_t abs_path = {0};
    pkgmap_path_t rel_path = {0};
    if (!pkgmap_path_init(&abs_path, repo_path) || !pkgmap_path_init(&rel_path, "")) {
        pkgmap_path_free(&abs_path);
        pkgmap_path_free(&rel_path);
        cbm_log_warn("pkgmap.walk_skipped", "path", repo_path, "reason", "path_allocation_failed");
        return 0;
    }
    int parsed = pkgmap_walk_dir(&abs_path, &rel_path, entries, excluded_dirs, excluded_count);
    pkgmap_path_free(&rel_path);
    pkgmap_path_free(&abs_path);
    cbm_log_info("pkgmap.scan_repo", "manifests", pkgmap_itoa(parsed));
    return parsed;
}

/* Build pkgmap for sequential path (reads manifest files directly) */
CBMHashTable *cbm_pkgmap_build_from_files(const cbm_file_info_t *files, int file_count,
                                          const char *project_name) {
    cbm_pkg_entries_t entries;
    cbm_pkg_entries_init(&entries);

    for (int i = 0; i < file_count; i++) {
        const char *basename = path_basename(files[i].rel_path);
        if (!is_pkgmap_manifest_basename(basename)) {
            continue;
        }

        /* Read file */
        int source_len = 0;
        char *source = pkgmap_read_file(files[i].path, &source_len);
        if (!source) {
            continue;
        }
        cbm_pkgmap_try_parse(basename, files[i].rel_path, source, source_len, &entries);
        free(source);
    }

    CBMHashTable *map = cbm_pkgmap_build(&entries, SKIP_ONE, project_name);
    cbm_pkg_entries_free(&entries);
    return map;
}

/* Variant of cbm_pkgmap_build_from_files that ALSO walks the repo
 * filesystem to pick up manifests filtered out by the main discoverer
 * (the canonical case: package.json, which is in IGNORED_JSON_FILES).
 * Falls back to the files[]-only behaviour if repo_path is NULL. */
CBMHashTable *cbm_pkgmap_build_from_repo(const char *repo_path, const cbm_file_info_t *files,
                                         int file_count, const char *project_name,
                                         char **excluded_dirs, int excluded_count) {
    cbm_pkg_entries_t entries;
    cbm_pkg_entries_init(&entries);

    /* Manifests already visible through discovery (Cargo.toml, go.mod,
     * pyproject.toml, ...). package.json typically isn't, but we still
     * harvest whatever the discovery filter exposed in case downstream
     * filters change. */
    int from_files = 0;
    for (int i = 0; i < file_count; i++) {
        const char *basename = path_basename(files[i].rel_path);
        if (!is_pkgmap_manifest_basename(basename)) {
            continue;
        }
        from_files++;
        int source_len = 0;
        char *source = pkgmap_read_file(files[i].path, &source_len);
        if (!source) {
            continue;
        }
        cbm_pkgmap_try_parse(basename, files[i].rel_path, source, source_len, &entries);
        free(source);
    }

    int from_walk = cbm_pkgmap_scan_repo(repo_path, &entries, excluded_dirs, excluded_count);
    cbm_log_info("pkgmap.scan", "manifests_from_files", pkgmap_itoa(from_files),
                 "manifests_from_walk", pkgmap_itoa(from_walk), "entries",
                 pkgmap_itoa(entries.count));
    CBMHashTable *map = cbm_pkgmap_build(&entries, SKIP_ONE, project_name);
    cbm_pkg_entries_free(&entries);
    return map;
}

static void pkgmap_free_entry(const char *key, void *value, void *userdata) {
    (void)userdata;
    free((void *)key);
    free(value);
}

void cbm_pkgmap_free(CBMHashTable *pkgmap) {
    if (!pkgmap) {
        return;
    }
    cbm_ht_foreach(pkgmap, pkgmap_free_entry, NULL);
    cbm_ht_free(pkgmap);
}

/* ── Resolver ──────────────────────────────────────────────────── */

/* Try slash-based prefix matching (Go: github.com/foo/bar/pkg/utils).
 * Returns heap QN or NULL. */
static char *resolve_slash_prefix(CBMHashTable *map, const char *module_path) {
    char *buf = strdup(module_path);
    if (!buf) {
        return NULL;
    }
    for (char *slash = buf + strlen(buf) - SKIP_ONE; slash > buf; slash--) {
        if (*slash != '/') {
            continue;
        }
        *slash = '\0';
        const char *base_qn = (const char *)cbm_ht_get(map, buf);
        if (!base_qn) {
            continue;
        }
        const char *subpath = module_path + (size_t)(slash - buf) + SKIP_ONE;
        char *result = pkgmap_join_text(base_qn, ".", subpath);
        if (!result) {
            free(buf);
            return NULL;
        }
        /* Replace / with . in the appended part */
        for (char *c = result + strlen(base_qn) + SKIP_ONE; *c; c++) {
            if (*c == '/') {
                *c = '.';
            }
        }
        free(buf);
        return result;
    }
    free(buf);
    return NULL;
}

/* Try dot-based prefix matching (Java: com.myorg.pkg.Foo).
 * Returns heap QN or NULL. */
static char *resolve_dot_prefix(CBMHashTable *map, const char *module_path,
                                const char *project_name) {
    char *buf = strdup(module_path);
    if (!buf) {
        return NULL;
    }
    for (char *dot = buf + strlen(buf) - SKIP_ONE; dot > buf; dot--) {
        if (*dot != '.') {
            continue;
        }
        *dot = '\0';
        const char *base_qn = (const char *)cbm_ht_get(map, buf);
        if (!base_qn) {
            continue;
        }
        const char *subpath = module_path + (size_t)(dot - buf) + SKIP_ONE;
        char *subpath_slashed = cbm_strdup(subpath);
        if (!subpath_slashed) {
            free(buf);
            return NULL;
        }
        for (char *c = subpath_slashed; *c; c++) {
            if (*c == '.') {
                *c = '/';
            }
        }
        char *result = pkgmap_join_text(base_qn, "/", subpath_slashed);
        free(subpath_slashed);
        free(buf);
        if (!result) {
            return NULL;
        }
        char *qn = cbm_pipeline_fqn_module(project_name, result);
        free(result);
        return qn;
    }
    free(buf);
    return NULL;
}

/* Try backslash-based prefix matching (PHP PSR-4: App\\Controllers\\Foo).
 * Returns heap QN or NULL. */
static char *resolve_backslash_prefix(CBMHashTable *map, const char *module_path,
                                      const char *project_name) {
    char *buf = strdup(module_path);
    if (!buf) {
        return NULL;
    }
    for (char *bs = buf + strlen(buf) - SKIP_ONE; bs > buf; bs--) {
        if (*bs != '\\') {
            continue;
        }
        *bs = '\0';
        char *prefix = pkgmap_join_text(buf, "\\", "");
        if (!prefix) {
            free(buf);
            return NULL;
        }
        const char *base_dir = (const char *)cbm_ht_get(map, prefix);
        free(prefix);
        if (!base_dir) {
            continue;
        }
        const char *subpath = module_path + (size_t)(bs - buf) + SKIP_ONE;
        char *path_result = pkgmap_join_text(base_dir, "/", subpath);
        if (!path_result) {
            free(buf);
            return NULL;
        }
        for (char *c = path_result; *c; c++) {
            if (*c == '\\') {
                *c = '/';
            }
        }
        free(buf);
        char *qn = cbm_pipeline_fqn_module(project_name, path_result);
        free(path_result);
        return qn;
    }
    free(buf);
    return NULL;
}

char *cbm_pipeline_resolve_module(const cbm_pipeline_ctx_t *ctx, const char *source_rel,
                                  const char *module_path) {
    if (!ctx || !module_path) {
        return cbm_pipeline_fqn_module(ctx ? ctx->project_name : NULL, module_path);
    }

    /* 1. Try relative import resolution (existing logic) */
    char *resolved = cbm_pipeline_resolve_relative_import(source_rel, module_path);
    if (resolved) {
        char *qn = cbm_pipeline_fqn_module(ctx->project_name, resolved);
        free(resolved);
        return qn;
    }

    /* 1b. Try build-tool path aliases (tsconfig/jsconfig paths today;
     *     other loaders can register here later). Independent of pkgmap. */
    if (ctx->path_aliases && source_rel) {
        const cbm_path_alias_map_t *amap =
            cbm_path_alias_find_for_file(ctx->path_aliases, source_rel);
        if (amap) {
            char *aliased = cbm_path_alias_resolve(amap, module_path);
            if (aliased) {
                char *qn = cbm_pipeline_fqn_module(ctx->project_name, aliased);
                free(aliased);
                return qn;
            }
        }
    }

    /* 2. No pkgmap → fall through immediately */
    CBMHashTable *pkgmap = cbm_pipeline_get_pkgmap();
    if (!pkgmap) {
        return cbm_pipeline_fqn_module(ctx->project_name, module_path);
    }

    /* 3. Exact lookup */
    const char *mapped_qn = (const char *)cbm_ht_get(pkgmap, module_path);
    if (mapped_qn) {
        return strdup(mapped_qn);
    }

    /* 4. Prefix matching by separator type */
    char *result = resolve_slash_prefix(pkgmap, module_path);
    if (!result) {
        result = resolve_dot_prefix(pkgmap, module_path, ctx->project_name);
    }
    if (!result) {
        result = resolve_backslash_prefix(pkgmap, module_path, ctx->project_name);
    }
    if (result) {
        return result;
    }

    /* 5. Fallthrough to default resolution */
    return cbm_pipeline_fqn_module(ctx->project_name, module_path);
}

/* ── Import-target node resolver ─────────────────────────────────── */

/* Return the last path segment of an import string, recognizing the separators
 * used across languages: '.', '/', '\\', and "::".  Returns a pointer into
 * `path` (no allocation). */
static const char *import_last_segment(const char *path) {
    const char *seg = path;
    for (const char *p = path; *p; p++) {
        if (*p == '.' || *p == '/' || *p == '\\' || *p == ':') {
            seg = p + 1;
        }
    }
    return seg;
}

/* Canonicalize selected graph-language import separators in place and collapse
 * adjacent output separators. Decorations remain for each strategy to validate
 * or strip according to its language conventions. With the fixed separator
 * sets used here, this is O(M) runtime and O(1) auxiliary memory for M bytes. */
static void normalize_import_separators_in_place(char *text, const char *input_separators,
                                                 char output_separator) {
    if (!text || !input_separators) {
        return;
    }
    size_t output = 0;
    for (const char *input = text; *input; input++) {
        bool is_separator = strchr(input_separators, *input) != NULL;
        if (is_separator) {
            if (output > 0 && text[output - SKIP_ONE] == output_separator) {
                continue;
            }
            text[output++] = output_separator;
        } else {
            text[output++] = *input;
        }
    }
    text[output] = '\0';
}

/* Derive a representative imported symbol name from a raw module/use path,
 * stripping language decorations so it can be matched against an in-graph node:
 *   - trailing alias " as X"      (Rust/Kotlin)
 *   - trailing glob "::*" / ".*"  (Rust/Kotlin/Java wildcard)
 *   - brace groups "{a, b, ...}"  (Rust grouped use) → first member
 * Returns an exact owned result that the caller must free, or NULL if none.
 * For grouped/braced forms the first listed symbol is used as the representative
 * (the tests only require at least one resolved IMPORTS edge per statement).
 * Runtime and returned memory are O(M) for M module-path bytes. */
static char *import_candidate_symbol_dup(const char *module_path) {
    if (!module_path || !module_path[0]) {
        return NULL;
    }
    char *buf = cbm_strdup(module_path);
    if (!buf) {
        return NULL;
    }

    /* Brace group: `prefix::{a, b}` → take first member `a`. */
    char *brace = strchr(buf, '{');
    if (brace) {
        char *first = brace + 1;
        char *end = first;
        while (*end && *end != ',' && *end != '}') {
            end++;
        }
        *end = '\0';
        /* Trim whitespace. */
        while (*first == ' ' || *first == '\t') {
            first++;
        }
        char *t = first + strlen(first);
        while (t > first && (t[-1] == ' ' || t[-1] == '\t')) {
            *--t = '\0';
        }
        if (first[0] && strcmp(first, "self") != 0) {
            memmove(buf, first, strlen(first) + SKIP_ONE);
            return buf;
        }
        /* `{self, ...}` → fall back to the path before the brace group. */
        *brace = '\0';
    }

    /* Strip trailing " as <alias>". */
    char *as = strstr(buf, " as ");
    if (as) {
        *as = '\0';
    }

    /* Strip trailing glob and separator noise. */
    size_t len = strlen(buf);
    while (len > 0 && (buf[len - 1] == '*' || buf[len - 1] == ':' || buf[len - 1] == '.' ||
                       buf[len - 1] == '/' || buf[len - 1] == '\\' || buf[len - 1] == ' ')) {
        buf[--len] = '\0';
    }
    if (!buf[0]) {
        free(buf);
        return NULL;
    }
    const char *seg = import_last_segment(buf);
    if (!seg || !seg[0] || strcmp(seg, "*") == 0) {
        free(buf);
        return NULL;
    }
    memmove(buf, seg, strlen(seg) + SKIP_ONE);
    return buf;
}

/* Restrict fallback resolution to importable definitions. Strategy 1 may
 * intentionally resolve a directory-module import to a Folder, but shortened
 * or sibling fallback paths must not create phantom Folder/Variable edges
 * for a path the source import never named (#767). */
static bool import_targetable_label(const char *label) {
    if (!label) {
        return false;
    }
    static const char *const targetable_labels[] = {
        "Class", "Interface", "Function", "Method", "Module", "Struct",
        "Enum",  "Trait",     "Type",     "File",   NULL,
    };
    for (const char *const *candidate = targetable_labels; *candidate; candidate++) {
        if (strcmp(*candidate, label) == 0) {
            return true;
        }
    }
    return false;
}

static int import_target_score(const cbm_gbuf_node_t *target, const char *context_qn) {
    if (!target || !target->qualified_name) {
        return CBM_NOT_FOUND;
    }
    int score = cbm_str_common_dot_prefix_len(target->qualified_name, context_qn ? context_qn : "");
    if (!cbm_is_test_path(target->file_path)) {
        score += CBM_RESOLUTION_NON_TEST_BONUS;
    }
    return score;
}

static bool import_target_better(const cbm_gbuf_node_t *candidate, const cbm_gbuf_node_t *current,
                                 const char *context_qn) {
    if (!candidate) {
        return false;
    }
    if (!current) {
        return true;
    }
    int candidate_score = import_target_score(candidate, context_qn);
    int current_score = import_target_score(current, context_qn);
    if (candidate_score != current_score) {
        return candidate_score > current_score;
    }
    const char *candidate_qn = candidate->qualified_name ? candidate->qualified_name : "";
    const char *current_qn = current->qualified_name ? current->qualified_name : "";
    return strcmp(candidate_qn, current_qn) < 0;
}

/* Resolve one exact simple-name candidate through the graph and registry using
 * the same deterministic ranking. Runtime is O(Hg + Hr) for graph/registry
 * hits; no candidate-list storage or semantic count ceiling is introduced. */
static const cbm_gbuf_node_t *resolve_named_import_candidate(cbm_pipeline_ctx_t *ctx,
                                                             const char *candidate_name,
                                                             const char *source_file_qn,
                                                             const char *context_qn) {
    if (!ctx || !candidate_name || !candidate_name[0]) {
        return NULL;
    }
    const cbm_gbuf_node_t *best = NULL;
    const cbm_gbuf_node_t **hits = NULL;
    int hit_count = 0;
    if (cbm_gbuf_find_by_name(ctx->gbuf, candidate_name, &hits, &hit_count) == 0 && hits) {
        for (int i = 0; i < hit_count; i++) {
            const cbm_gbuf_node_t *candidate = hits[i];
            if (!candidate || !cbm_pipeline_label_is_import_target(candidate->label) ||
                (source_file_qn && candidate->qualified_name &&
                 strcmp(candidate->qualified_name, source_file_qn) == 0)) {
                continue;
            }
            if (import_target_better(candidate, best, context_qn)) {
                best = candidate;
            }
        }
    }
    if (best) {
        return best;
    }
    if (!ctx->registry) {
        return NULL;
    }

    const char **registry_qns = NULL;
    int registry_count = 0;
    if (cbm_registry_find_by_name(ctx->registry, candidate_name, &registry_qns, &registry_count) !=
            0 ||
        !registry_qns) {
        return NULL;
    }
    for (int i = 0; i < registry_count; i++) {
        const cbm_gbuf_node_t *candidate = cbm_pipeline_find_node_by_qn(ctx, registry_qns[i]);
        if (!candidate || !cbm_pipeline_label_is_import_target(candidate->label) ||
            (source_file_qn && candidate->qualified_name &&
             strcmp(candidate->qualified_name, source_file_qn) == 0)) {
            continue;
        }
        if (import_target_better(candidate, best, context_qn)) {
            best = candidate;
        }
    }
    return best;
}

static const char *path_leaf(const char *path) {
    const char *leaf = path;
    for (const char *p = path; p && *p; p++) {
        if (*p == '/' || *p == '\\') {
            leaf = p + 1;
        }
    }
    return leaf;
}

static bool is_header_include(const char *path) {
    if (!path || !path[0]) {
        return false;
    }
    static const char *const header_exts[] = {
        ".h", ".hh", ".hpp", ".hxx", ".inc", ".inl", ".ipp", NULL,
    };
    size_t path_len = strlen(path);
    for (const char *const *ext = header_exts; *ext; ext++) {
        size_t ext_len = strlen(*ext);
        if (path_len > ext_len && strcmp(path + path_len - ext_len, *ext) == 0) {
            return true;
        }
    }
    return false;
}

static bool is_c_family_source(const char *source_rel) {
    if (!source_rel || !source_rel[0]) {
        return false;
    }
    static const char *const exts[] = {
        ".c", ".cc", ".cpp", ".cxx", ".c++", ".h", ".hh", ".hpp", ".hxx", NULL,
    };
    size_t path_len = strlen(source_rel);
    for (const char *const *ext = exts; *ext; ext++) {
        size_t ext_len = strlen(*ext);
        if (path_len > ext_len && strcmp(source_rel + path_len - ext_len, *ext) == 0) {
            return true;
        }
    }
    return false;
}

static const cbm_gbuf_node_t *resolve_exact_file_node(const cbm_pipeline_ctx_t *ctx,
                                                      const char *file_path,
                                                      const char *source_file_qn) {
    if (!ctx || !file_path || !file_path[0]) {
        return NULL;
    }
    const char *leaf = path_leaf(file_path);
    if (!leaf || !leaf[0]) {
        return NULL;
    }

    char *owned_stem = NULL;
    const char *stem = leaf;
    const char *last_dot = strrchr(leaf, '.');
    if (last_dot && last_dot != leaf) {
        owned_stem = cbm_strndup(leaf, (size_t)(last_dot - leaf));
        stem = owned_stem;
    }

    const char *names[] = {stem && stem[0] ? stem : NULL, leaf};
    const cbm_gbuf_node_t *best = NULL;
    for (size_t ni = 0; ni < sizeof(names) / sizeof(names[0]); ni++) {
        if (!names[ni] || !names[ni][0]) {
            continue;
        }
        const cbm_gbuf_node_t **hits = NULL;
        int hit_count = 0;
        if (cbm_gbuf_find_by_name(ctx->gbuf, names[ni], &hits, &hit_count) != 0 || !hits) {
            continue;
        }
        for (int i = 0; i < hit_count; i++) {
            const cbm_gbuf_node_t *candidate = hits[i];
            if (!candidate || !candidate->file_path ||
                !ends_with(candidate->file_path, file_path) ||
                !import_targetable_label(candidate->label)) {
                continue;
            }
            if (source_file_qn && candidate->qualified_name &&
                strcmp(candidate->qualified_name, source_file_qn) == 0) {
                continue;
            }
            if (strcmp(candidate->label, "File") == 0) {
                free(owned_stem);
                return candidate;
            }
            if (!best) {
                best = candidate;
            }
        }
        if (best) {
            free(owned_stem);
            return best;
        }
    }
    free(owned_stem);
    return NULL;
}

static const cbm_gbuf_node_t *resolve_header_include(const cbm_pipeline_ctx_t *ctx,
                                                     const char *source_rel,
                                                     const char *source_file_qn,
                                                     const char *module_path) {
    if (!is_c_family_source(source_rel) || !is_header_include(module_path)) {
        return NULL;
    }
    const char *base = module_path;
    if (base[0] == '.' && base[1] == '/') {
        base += 2;
    }
    if (source_rel && source_rel[0]) {
        char *dir = path_dirname(source_rel);
        char *candidate = dir ? cbm_pkgmap_join_path(dir, base) : NULL;
        free(dir);
        if (candidate) {
            const cbm_gbuf_node_t *relative =
                resolve_exact_file_node(ctx, candidate, source_file_qn);
            free(candidate);
            if (relative) {
                return relative;
            }
        }
    }
    return resolve_exact_file_node(ctx, base, source_file_qn);
}

static const cbm_gbuf_node_t *resolve_sibling_candidate(cbm_pipeline_ctx_t *ctx,
                                                        const char *source_file_qn,
                                                        const char *candidate) {
    if (!candidate) {
        return NULL;
    }
    char *qn = cbm_pipeline_fqn_module(ctx->project_name, candidate);
    const cbm_gbuf_node_t *node = qn ? cbm_pipeline_find_node_by_qn(ctx, qn) : NULL;
    free(qn);
    if (node && import_targetable_label(node->label) &&
        (!source_file_qn || !node->qualified_name ||
         strcmp(node->qualified_name, source_file_qn) != 0)) {
        return node;
    }
    return NULL;
}

/* Resolve a sibling-file import: a bare path/name (no leading "./") that names
 * a file relative to the importer's directory.  This covers build/markup
 * grammars whose import string is a sibling filename or directory rather than a
 * dotted module path:
 *   - SCSS  `@use 'vars'`              → sibling `_vars.scss` (partial underscore)
 *   - Just  `import 'common.just'`     → sibling `common.just`
 *   - BitBake `require mypackage.inc`  → sibling `mypackage.inc`
 *   - Meson `subdir('lib')`            → `lib/meson.build`
 *   - func  `#include "utils.fc"`      → sibling `utils.fc`
 *   - Pony  `use "util"`               → sibling `util.pony`
 * Builds a path relative to source_rel's directory, then looks up the resulting
 * File/Module-node QN (extension is stripped by fqn_module).  Returns a borrowed
 * node or NULL.  Several filename conventions are tried in turn. */
static const cbm_gbuf_node_t *resolve_sibling_file(cbm_pipeline_ctx_t *ctx, const char *source_rel,
                                                   const char *source_file_qn,
                                                   const char *module_path) {
    if (!module_path || !module_path[0]) {
        return NULL;
    }
    /* Directory of the importing file (empty for repo-root files). */
    char *dir = path_dirname(source_rel ? source_rel : "");
    if (!dir) {
        return NULL;
    }

    const char *base = module_path;
    /* Skip a leading "./". */
    if (base[0] == '.' && base[1] == '/') {
        base += 2;
    }
    /* 1. Direct sibling: dir/<module_path>. */
    char *candidate = cbm_pkgmap_join_path(dir, base);
    const cbm_gbuf_node_t *found = resolve_sibling_candidate(ctx, source_file_qn, candidate);
    free(candidate);
    if (found) {
        free(dir);
        return found;
    }
    /* 2. SCSS partial: dir/[subdir/]_<basename>.scss (underscore-prefixed).
     * Candidates are built and released sequentially, keeping peak temporary
     * memory O(P) for the longest candidate rather than storing a candidate
     * array or imposing a semantic count ceiling. */
    {
        const char *slash = strrchr(base, '/');
        const char *bn = slash ? slash + 1 : base;
        char *dpart = slash ? cbm_strndup(base, (size_t)(slash - base)) : cbm_strdup("");
        if (dpart && bn[0] != '_') {
            char *partial_name = pkgmap_join_text("_", bn, ".scss");
            const char *nested_parts[] = {dpart, partial_name};
            const char *root_parts[] = {partial_name};
            candidate = partial_name
                            ? pkgmap_join_path_parts(dir, dpart[0] ? nested_parts : root_parts,
                                                     dpart[0] ? PAIR_LEN : SKIP_ONE)
                            : NULL;
            found = resolve_sibling_candidate(ctx, source_file_qn, candidate);
            free(candidate);
            free(partial_name);
        }
        free(dpart);
        if (found) {
            free(dir);
            return found;
        }
    }
    /* 3. Meson subdir: dir/<module_path>/meson.build. */
    {
        const char *parts[] = {base, "meson.build"};
        candidate = pkgmap_join_path_parts(dir, parts, sizeof(parts) / sizeof(parts[0]));
        found = resolve_sibling_candidate(ctx, source_file_qn, candidate);
        free(candidate);
        if (found) {
            free(dir);
            return found;
        }
    }
    /* 4. Basename sibling: dir/<basename(module_path)>.  Covers include paths
     *    that carry a non-relative prefix (Hyprlang `source = ~/.config/.../x.conf`,
     *    absolute include paths) but reference a file sitting beside the importer. */
    {
        const char *slash = strrchr(base, '/');
        if (slash && slash[1]) {
            candidate = cbm_pkgmap_join_path(dir, slash + SKIP_ONE);
            found = resolve_sibling_candidate(ctx, source_file_qn, candidate);
            free(candidate);
        }
    }
    free(dir);
    return found;
}

static bool import_edge_local_name_span(const cbm_gbuf_edge_t *edge, const char **out_start,
                                        size_t *out_len, bool *out_has_escape) {
    if (out_start) {
        *out_start = NULL;
    }
    if (out_len) {
        *out_len = 0;
    }
    if (out_has_escape) {
        *out_has_escape = false;
    }
    if (!edge || !edge->properties_json || !out_start || !out_len) {
        return false;
    }
    static const char key[] = "\"local_name\":\"";
    const char *start = strstr(edge->properties_json, key);
    if (!start) {
        return false;
    }
    start += sizeof(key) - 1;
    bool escaped = false;
    for (const char *end = start; *end; end++) {
        if (escaped) {
            if (out_has_escape) {
                *out_has_escape = true;
            }
            escaped = false;
            continue;
        }
        if (*end == '\\') {
            escaped = true;
            continue;
        }
        if (*end == '"') {
            if (end <= start) {
                return false;
            }
            *out_start = start;
            *out_len = (size_t)(end - start);
            return true;
        }
    }
    return false;
}

static char *import_edge_local_name_dup_json(const cbm_gbuf_edge_t *edge) {
    yyjson_doc *doc = edge && edge->properties_json
                          ? yyjson_read(edge->properties_json, strlen(edge->properties_json), 0)
                          : NULL;
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *local = yyjson_obj_get(root, "local_name");
    const char *value = yyjson_is_str(local) ? yyjson_get_str(local) : NULL;
    char *dup = value && value[0] ? cbm_strdup(value) : NULL;
    yyjson_doc_free(doc);
    return dup;
}

static bool import_edge_local_name_equals(const cbm_gbuf_edge_t *edge, const char *local_name) {
    if (!local_name || !local_name[0]) {
        return false;
    }
    const char *start = NULL;
    size_t len = 0;
    bool has_escape = false;
    if (!import_edge_local_name_span(edge, &start, &len, &has_escape)) {
        return false;
    }
    if (has_escape) {
        char *decoded = import_edge_local_name_dup_json(edge);
        bool match = decoded && strcmp(decoded, local_name) == 0;
        free(decoded);
        return match;
    }
    return len == strlen(local_name) && strncmp(start, local_name, len) == 0;
}

char *cbm_pipeline_import_edge_local_name_dup(const cbm_gbuf_edge_t *edge) {
    const char *start = NULL;
    size_t len = 0;
    bool has_escape = false;
    if (!import_edge_local_name_span(edge, &start, &len, &has_escape)) {
        return NULL;
    }
    if (has_escape) {
        return import_edge_local_name_dup_json(edge);
    }
    return cbm_strndup(start, len);
}

static const cbm_gbuf_node_t *find_file_node_for_module_qn(const cbm_gbuf_t *gbuf,
                                                           const char *module_qn);

static bool import_map_target_can_own_reexports(const cbm_gbuf_node_t *target) {
    return target && target->label &&
           (strcmp(target->label, "Folder") == 0 || strcmp(target->label, "Module") == 0);
}

static const cbm_gbuf_node_t *resolve_import_map_reexport_target(const cbm_gbuf_t *gbuf,
                                                                 const cbm_gbuf_node_t *source_file,
                                                                 const cbm_gbuf_node_t *target,
                                                                 const char *local_name) {
    if (!gbuf || !source_file || !target || !target->qualified_name || !local_name ||
        !local_name[0] || strcmp(local_name, "*") == 0) {
        return NULL;
    }

    const cbm_gbuf_node_t *owner_file = NULL;
    if (target->label && strcmp(target->label, "File") == 0) {
        owner_file = target;
    } else if (import_map_target_can_own_reexports(target)) {
        owner_file = find_file_node_for_module_qn(gbuf, target->qualified_name);
    }
    if (!owner_file || owner_file->id == source_file->id) {
        return NULL;
    }

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    int rc =
        cbm_gbuf_find_edges_by_source_type(gbuf, owner_file->id, "IMPORTS", &edges, &edge_count);
    if (rc != 0 || edge_count <= 0 || !edges) {
        return NULL;
    }

    const cbm_gbuf_node_t *best = NULL;
    for (int i = 0; i < edge_count; i++) {
        if (!import_edge_local_name_equals(edges[i], local_name)) {
            continue;
        }
        const cbm_gbuf_node_t *candidate = cbm_gbuf_find_by_id(gbuf, edges[i]->target_id);
        if (candidate && cbm_pipeline_label_is_import_target(candidate->label) &&
            import_target_better(candidate, best, target->qualified_name)) {
            best = candidate;
        }
    }
    return best;
}

static const cbm_gbuf_node_t *import_map_source_file(const cbm_gbuf_t *gbuf,
                                                     const char *project_name,
                                                     const char *rel_path) {
    if (!gbuf || !project_name || !rel_path) {
        return NULL;
    }
    char *file_qn = cbm_pipeline_fqn_compute(project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *file_node = cbm_gbuf_find_by_qn(gbuf, file_qn);
    free(file_qn);
    return file_node;
}

bool cbm_pipeline_import_map_entry_is_reexport(const cbm_gbuf_t *gbuf, const char *project_name,
                                               const char *rel_path, const char *local_name,
                                               const char *resolved_qn) {
    if (!local_name || !resolved_qn) {
        return false;
    }
    const cbm_gbuf_node_t *source_file = import_map_source_file(gbuf, project_name, rel_path);
    if (!source_file) {
        return false;
    }
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, source_file->id, "IMPORTS", &edges, &edge_count) !=
        0) {
        return false;
    }
    for (int i = 0; i < edge_count; i++) {
        if (!import_edge_local_name_equals(edges[i], local_name)) {
            continue;
        }
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(gbuf, edges[i]->target_id);
        const cbm_gbuf_node_t *reexported =
            resolve_import_map_reexport_target(gbuf, source_file, target, local_name);
        if (reexported && reexported->qualified_name &&
            strcmp(reexported->qualified_name, resolved_qn) == 0) {
            return true;
        }
    }
    return false;
}

int cbm_pipeline_build_import_map_from_edges(const cbm_gbuf_t *gbuf, const char *project_name,
                                             const char *rel_path, const char ***out_keys,
                                             const char ***out_vals, int *out_count) {
    if (out_keys) {
        *out_keys = NULL;
    }
    if (out_vals) {
        *out_vals = NULL;
    }
    if (out_count) {
        *out_count = 0;
    }
    if (!gbuf || !project_name || !rel_path || !out_keys || !out_vals || !out_count) {
        return 0;
    }

    const cbm_gbuf_node_t *file_node = import_map_source_file(gbuf, project_name, rel_path);
    if (!file_node) {
        return 0;
    }

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    int rc =
        cbm_gbuf_find_edges_by_source_type(gbuf, file_node->id, "IMPORTS", &edges, &edge_count);
    if (rc != 0 || edge_count <= 0 || !edges) {
        return 0;
    }

    const char **keys = calloc((size_t)edge_count, sizeof(const char *));
    const char **vals = calloc((size_t)edge_count, sizeof(const char *));
    if (!keys || !vals) {
        free(keys);
        free(vals);
        return CBM_NOT_FOUND;
    }

    int count = 0;
    for (int i = 0; i < edge_count; i++) {
        const cbm_gbuf_edge_t *edge = edges[i];
        const cbm_gbuf_node_t *target = edge ? cbm_gbuf_find_by_id(gbuf, edge->target_id) : NULL;
        char *key = cbm_pipeline_import_edge_local_name_dup(edge);
        if (!target || !key) {
            free(key);
            continue;
        }
        const cbm_gbuf_node_t *resolved =
            resolve_import_map_reexport_target(gbuf, file_node, target, key);
        keys[count] = key;
        vals[count] = resolved ? resolved->qualified_name : target->qualified_name;
        count++;
    }

    *out_keys = keys;
    *out_vals = vals;
    *out_count = count;
    return 0;
}

void cbm_pipeline_free_import_map(const char **keys, const char **vals, int count) {
    if (keys) {
        for (int i = 0; i < count; i++) {
            free((void *)keys[i]);
        }
        free((void *)keys);
    }
    free((void *)vals);
}

static const cbm_gbuf_node_t *find_file_node_for_module_qn(const cbm_gbuf_t *gbuf,
                                                           const char *module_qn) {
    if (!gbuf || !module_qn || !module_qn[0]) {
        return NULL;
    }
    char *file_qn = pkgmap_join_text(module_qn, ".", "__file__");
    if (!file_qn) {
        return NULL;
    }
    const cbm_gbuf_node_t *exact = cbm_gbuf_find_by_qn(gbuf, file_qn);
    if (exact) {
        free(file_qn);
        return exact;
    }

    size_t module_len = strlen(module_qn);
    file_qn[module_len + SKIP_ONE] = '\0';
    const char *qn_prefix = file_qn;

    const cbm_gbuf_node_t **files = NULL;
    int file_count = 0;
    if (cbm_gbuf_find_by_label(gbuf, "File", &files, &file_count) != 0 || !files) {
        free(file_qn);
        return NULL;
    }

    static const char file_qn_suffix[] = ".__file__";
    const cbm_gbuf_node_t *best = NULL;
    size_t best_len = 0;
    bool best_ambiguous = false;
    for (int i = 0; i < file_count; i++) {
        const cbm_gbuf_node_t *node = files[i];
        const char *qn = node ? node->qualified_name : NULL;
        if (!qn || !cbm_str_starts_with(qn, qn_prefix) || !cbm_str_ends_with(qn, file_qn_suffix)) {
            continue;
        }
        size_t qn_len = strlen(qn);
        if (!best || qn_len < best_len) {
            best = node;
            best_len = qn_len;
            best_ambiguous = false;
        } else if (qn_len == best_len) {
            best_ambiguous = true;
        }
    }
    const cbm_gbuf_node_t *result = best_ambiguous ? NULL : best;
    free(file_qn);
    return result;
}

static const cbm_gbuf_node_t *resolve_reexported_symbol(cbm_pipeline_ctx_t *ctx,
                                                        const char *source_rel,
                                                        const char *source_file_qn,
                                                        const char *owner, const char *local_name) {
    if (!ctx || !owner || !owner[0] || !local_name || !local_name[0] ||
        strcmp(local_name, "*") == 0) {
        return NULL;
    }

    char *owner_module_qn = cbm_pipeline_resolve_module(ctx, source_rel, owner);
    const cbm_gbuf_node_t *owner_file = find_file_node_for_module_qn(ctx->gbuf, owner_module_qn);
    if (!owner_file) {
        free(owner_module_qn);
        owner_module_qn = cbm_pipeline_fqn_module(ctx->project_name, owner);
        owner_file = find_file_node_for_module_qn(ctx->gbuf, owner_module_qn);
    }
    if (!owner_file || (source_file_qn && owner_file->qualified_name &&
                        strcmp(owner_file->qualified_name, source_file_qn) == 0)) {
        free(owner_module_qn);
        return NULL;
    }

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    int rc = cbm_gbuf_find_edges_by_source_type(ctx->gbuf, owner_file->id, "IMPORTS", &edges,
                                                &edge_count);
    if (rc != 0 || edge_count == 0 || !edges) {
        free(owner_module_qn);
        return NULL;
    }
    const cbm_gbuf_node_t *best = NULL;
    for (int i = 0; i < edge_count; i++) {
        if (!import_edge_local_name_equals(edges[i], local_name)) {
            continue;
        }
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(ctx->gbuf, edges[i]->target_id);
        if (target && cbm_pipeline_label_is_import_target(target->label) &&
            import_target_better(target, best, owner_module_qn)) {
            best = target;
        }
    }
    free(owner_module_qn);
    return best;
}

static const cbm_gbuf_node_t *resolve_reexported_import(cbm_pipeline_ctx_t *ctx,
                                                        const char *source_rel,
                                                        const char *source_file_qn,
                                                        const char *module_path) {
    if (!ctx || !module_path || !strchr(module_path, '.')) {
        return NULL;
    }

    char *owner = cbm_strdup(module_path);
    if (!owner) {
        return NULL;
    }
    char *dot = strrchr(owner, '.');
    if (!dot || dot == owner || dot[1] == '\0') {
        free(owner);
        return NULL;
    }
    const char *local_name = dot + 1;
    *dot = '\0';

    const cbm_gbuf_node_t *resolved =
        resolve_reexported_symbol(ctx, source_rel, source_file_qn, owner, local_name);
    free(owner);
    return resolved;
}

const cbm_gbuf_node_t *cbm_pipeline_resolve_import_node(cbm_pipeline_ctx_t *ctx,
                                                        const char *source_rel,
                                                        const char *source_file_qn,
                                                        const CBMImport *imp,
                                                        CBMHashTable *namespace_map) {
    if (!ctx || !imp || !imp->module_path) {
        return NULL;
    }

    /* Prefer exact header-file nodes for C/C++ includes so same-stem source or
     * module nodes do not steal the edge target. */
    const cbm_gbuf_node_t *header_target =
        resolve_header_include(ctx, source_rel, source_file_qn, imp->module_path);
    if (header_target) {
        return header_target;
    }

    /* Strategy 1: module-path resolution → existing node (Python/TS/Go).
     * No label filter here: directory-module languages (Go/Java packages)
     * legitimately resolve straight to a Folder node -- that's the intended,
     * correct import target, not a collision. The Folder-collision problem
     * (#767) only shows up downstream, in Strategy 4's retry-with-truncated-
     * path loop, which re-enters resolve_module with a DIFFERENT, shortened
     * string that the original import never named. */
    char *target_qn = cbm_pipeline_resolve_module(ctx, source_rel, imp->module_path);
    const cbm_gbuf_node_t *target = target_qn ? cbm_pipeline_find_node_by_qn(ctx, target_qn) : NULL;
    free(target_qn);
    if (target) {
        return target;
    }

    /* Strategy 1a: package/module re-export. For `from fastapi import Body`,
     * the direct QN `fastapi.Body` may not exist; follow the package file's
     * own IMPORTS edge for local_name=Body before falling back to duplicate
     * short-name matching. */
    target = resolve_reexported_import(ctx, source_rel, source_file_qn, imp->module_path);
    if (target) {
        return target;
    }
    target = resolve_reexported_symbol(ctx, source_rel, source_file_qn, imp->module_path,
                                       imp->local_name);
    if (target) {
        return target;
    }

    /* Strategy 1b: sibling-file resolution for build/markup grammars whose
     * import string is a sibling filename or directory (SCSS partials, Just/
     * BitBake/func includes, Meson subdir, Pony use). */
    {
        const cbm_gbuf_node_t *sib =
            resolve_sibling_file(ctx, source_rel, source_file_qn, imp->module_path);
        if (sib) {
            return sib;
        }
    }

    /* Strategy 2: namespace map.  `using App.Utils`, `import com.example.Foo`,
     * `use App\Utils\Helper` name a NAMESPACE (or a member of it) that the
     * path-based QN cannot express.  Try the full module path and progressively
     * shorter prefixes (dropping the trailing member segment) so both a bare
     * namespace import and a member import resolve to the declaring file. */
    if (namespace_map) {
        /* Normalize separators to '.' for namespace keys (PHP uses '\\').
         * Strip decorations first so `com.example.Util as U`, `crate::ops::*`
         * and `App\Utils\{A, B}` reduce to a clean dotted path. */
        char *norm = cbm_strdup(imp->module_path);
        if (norm) {
            char *brace = strchr(norm, '{');
            if (brace) {
                *brace = '\0'; /* drop the group; the prefix is the namespace */
            }
            char *as = strstr(norm, " as ");
            if (as) {
                *as = '\0';
            }
            normalize_import_separators_in_place(norm, ".:/\\", '.');
            size_t norm_len = strlen(norm);
            while (norm_len > 0 &&
                   (norm[norm_len - SKIP_ONE] == '*' || norm[norm_len - SKIP_ONE] == '.' ||
                    norm[norm_len - SKIP_ONE] == ' ')) {
                norm[--norm_len] = '\0';
            }
            char *prefix_end = norm + strlen(norm);
            for (;;) {
                /* The map value is a '\n'-delimited list of __file__ QNs declaring
                 * this namespace. Return the FIRST that is not the importing file,
                 * so a same-package wildcard import (`import com.example.*` from a
                 * file that is itself in com.example) resolves to a sibling — and
                 * deterministically, independent of file-iteration order across
                 * platforms (amd64 vs arm64). */
                const char *list = (const char *)cbm_ht_get(namespace_map, norm);
                for (const char *seg = list; seg && *seg;) {
                    const char *eol = strchr(seg, '\n');
                    size_t len = eol ? (size_t)(eol - seg) : strlen(seg);
                    char *candidate_qn = len > 0 ? cbm_strndup(seg, len) : NULL;
                    if (candidate_qn) {
                        const cbm_gbuf_node_t *n = cbm_pipeline_find_node_by_qn(ctx, candidate_qn);
                        free(candidate_qn);
                        if (n && n->qualified_name &&
                            (!source_file_qn || strcmp(n->qualified_name, source_file_qn) != 0)) {
                            free(norm);
                            return n;
                        }
                    }
                    seg = eol ? eol + 1 : NULL;
                }
                char *dot = NULL;
                for (char *cursor = prefix_end; cursor > norm;) {
                    cursor--;
                    if (*cursor == '.') {
                        dot = cursor;
                        break;
                    }
                }
                if (!dot) {
                    break;
                }
                *dot = '\0';
                prefix_end = dot;
            }
            free(norm);
        }
    }

    /* Strategy 3: symbol-name fallback.  Derive a representative imported
     * symbol (handling alias / glob / grouped forms) and match it against an
     * in-graph definition of the same simple name in another file
     * (Rust `helper`, Java `Util`, Kotlin grouped, ...). */
    /* Prefer the clean candidate from the module path; the local_name may be an
     * alias (Rust `as h`, Kotlin `as U`) that names no real symbol. */
    char *derived_symbol = import_candidate_symbol_dup(imp->module_path);
    const char *first_candidate = derived_symbol;
    if (!first_candidate && imp->local_name && imp->local_name[0] &&
        strcmp(imp->local_name, "*") != 0) {
        first_candidate = imp->local_name;
    }
    char *context_qn = cbm_pipeline_resolve_module(ctx, source_rel, imp->module_path);
    target = resolve_named_import_candidate(ctx, first_candidate, source_file_qn, context_qn);
    if (target) {
        free(context_qn);
        free(derived_symbol);
        return target;
    }

    /* Walk every enclosing segment from leaf to root. The old eight-element
     * candidate array silently discarded valid outer types/namespaces. This
     * mutable duplicate requires O(M) memory and O(M) segment scanning for M
     * module-path bytes, independent of segment count. */
    char *module_segments = cbm_strdup(imp->module_path);
    if (module_segments) {
        char *brace = strchr(module_segments, '{');
        if (brace) {
            *brace = '\0';
        }
        char *alias = strstr(module_segments, " as ");
        if (alias) {
            *alias = '\0';
        }
        normalize_import_separators_in_place(module_segments, ".:/\\", '.');
        char *end = module_segments + strlen(module_segments);
        while (end > module_segments) {
            char *dot = NULL;
            for (char *p = end; p > module_segments;) {
                p--;
                if (*p == '.') {
                    dot = p;
                    break;
                }
            }
            const char *candidate = dot ? dot + SKIP_ONE : module_segments;
            if (candidate[0] && strcmp(candidate, "*") != 0 &&
                (!first_candidate || strcmp(candidate, first_candidate) != 0)) {
                target = resolve_named_import_candidate(ctx, candidate, source_file_qn, context_qn);
                if (target) {
                    free(module_segments);
                    free(context_qn);
                    free(derived_symbol);
                    return target;
                }
            }
            if (!dot) {
                break;
            }
            *dot = '\0';
            end = dot;
        }
        free(module_segments);
    }
    free(context_qn);
    free(derived_symbol);

    /* Strategy 4: crate-relative module path → File/Module node.  Rust glob
     * `use crate::ops::*` names a module, not a symbol; strip the glob and the
     * `crate::`/`self::`/`super::` prefix, convert `::`→`/`, then resolve the
     * remaining path (and successive prefixes) to a Module/File node. */
    {
        char *work = cbm_strdup(imp->module_path);
        if (work) {
            char *brace = strchr(work, '{');
            if (brace) {
                *brace = '\0';
            }
            char *alias = strstr(work, " as ");
            if (alias) {
                *alias = '\0';
            }
            normalize_import_separators_in_place(work, ":", '/');
            size_t output = 0;
            for (const char *input = work; *input; input++) {
                if (*input != '*' && *input != ' ') {
                    work[output++] = *input;
                }
            }
            while (output > 0 && work[output - SKIP_ONE] == '/') {
                output--;
            }
            work[output] = '\0';
            const char *body = work;
            static const char *prefixes[] = {"crate/", "self/", "super/", NULL};
            for (const char **prefix = prefixes; *prefix; prefix++) {
                size_t prefix_len = strlen(*prefix);
                if (strncmp(body, *prefix, prefix_len) == 0) {
                    body += prefix_len;
                    break;
                }
            }
            if (body != work) {
                memmove(work, body, strlen(body) + SKIP_ONE);
            }
            char *prefix_end = work + strlen(work);
            while (work[0]) {
                char *resolved_qn = cbm_pipeline_resolve_module(ctx, source_rel, work);
                const cbm_gbuf_node_t *node =
                    resolved_qn ? cbm_pipeline_find_node_by_qn(ctx, resolved_qn) : NULL;
                free(resolved_qn);
                if (node && import_targetable_label(node->label) &&
                    (!source_file_qn || !node->qualified_name ||
                     strcmp(node->qualified_name, source_file_qn) != 0)) {
                    free(work);
                    return node;
                }
                char *slash = NULL;
                for (char *cursor = prefix_end; cursor > work;) {
                    cursor--;
                    if (*cursor == '/') {
                        slash = cursor;
                        break;
                    }
                }
                if (!slash) {
                    break;
                }
                *slash = '\0';
                prefix_end = slash;
            }
            free(work);
        }
    }

    return NULL;
}

int cbm_pipeline_insert_import_edge(cbm_pipeline_ctx_t *ctx, int64_t source_id,
                                    const cbm_gbuf_node_t *target, const char *local_name) {
    if (!ctx || !ctx->gbuf || source_id <= 0 || !target || target->id <= 0 ||
        target->id == source_id) {
        return 0;
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return 0;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    if (!root ||
        !yyjson_mut_obj_add_strcpy(doc, root, "local_name", local_name ? local_name : "")) {
        yyjson_mut_doc_free(doc);
        return 0;
    }
    yyjson_mut_doc_set_root(doc, root);

    char *props = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INVALID_UNICODE, NULL);
    yyjson_mut_doc_free(doc);
    if (!props) {
        return 0;
    }

    int emitted =
        cbm_gbuf_insert_edge(ctx->gbuf, source_id, target->id, "IMPORTS", props) > 0 ? 1 : 0;
    free(props);
    return emitted;
}

int cbm_pipeline_create_import_edges_for_file(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                              const char *rel_path, CBMHashTable *namespace_map) {
    if (!ctx || !ctx->gbuf || !result || !rel_path) {
        return 0;
    }

    int count = 0;
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *source_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
    if (!source_node) {
        free(file_qn);
        return 0;
    }

    for (int i = 0; i < result->imports.count; i++) {
        const CBMImport *imp = &result->imports.items[i];
        if (!imp->module_path) {
            continue;
        }
        const cbm_gbuf_node_t *target =
            cbm_pipeline_resolve_import_node(ctx, rel_path, file_qn, imp, namespace_map);
        count += cbm_pipeline_insert_import_edge(ctx, source_node->id, target, imp->local_name);
    }

    free(file_qn);
    return count;
}

/* ── Namespace map ───────────────────────────────────────────────── */

CBMHashTable *cbm_pipeline_namespace_map_build(const char *project_name,
                                               CBMFileResult *const *results,
                                               const char *const *rels, int count) {
    CBMHashTable *map = NULL;
    for (int i = 0; i < count; i++) {
        const CBMFileResult *r = results[i];
        if (!r || !r->namespace_name || !r->namespace_name[0] || !rels[i]) {
            continue;
        }
        if (!map) {
            map = cbm_ht_create(CBM_SZ_64);
            if (!map) {
                return NULL;
            }
        }
        char *file_qn = cbm_pipeline_fqn_compute(project_name, rels[i], "__file__");
        if (!file_qn) {
            continue;
        }
        /* Normalize the namespace key to dot-separated form so it matches the
         * dot-normalized lookups in cbm_pipeline_resolve_import_node (PHP uses
         * '\\', some grammars '::' or '/'). */
        char *key = strdup(r->namespace_name);
        if (!key) {
            free(file_qn);
            continue;
        }
        normalize_import_separators_in_place(key, ".:/\\", '.');
        /* Store ALL files declaring a namespace as a '\n'-delimited list so the
         * resolver can pick a non-importer sibling (see resolve loop). The hash
         * table does not copy keys, so the strdup'd key is owned by the map and
         * freed in ns_map_free_entry. */
        if (!cbm_ht_has(map, key)) {
            cbm_ht_set(map, key, file_qn); /* map owns key + file_qn */
        } else {
            /* Append to the existing list. Re-key with the STORED key pointer
             * (not our fresh strdup) so the map's key pointer never changes —
             * otherwise Verstable would adopt the new key and our free(key)
             * below would free the live key (use-after-free). */
            const char *stored_key = cbm_ht_get_key(map, key);
            const char *cur = (const char *)cbm_ht_get(map, key);
            char *combined = NULL;
            if (stored_key && cur) {
                size_t need = strlen(cur) + 1 + strlen(file_qn) + 1;
                combined = malloc(need);
                if (combined) {
                    snprintf(combined, need, "%s\n%s", cur, file_qn);
                    void *prev = cbm_ht_set(map, stored_key, combined);
                    free(prev); /* old value string */
                }
            }
            free(key);     /* our fresh strdup — never stored */
            free(file_qn); /* content copied into combined */
        }
    }
    return map;
}

static void ns_map_free_entry(const char *key, void *value, void *ud) {
    (void)ud;
    free((void *)key); /* strdup'd in cbm_pipeline_namespace_map_build */
    free(value);
}

void cbm_pipeline_namespace_map_free(CBMHashTable *map) {
    if (!map) {
        return;
    }
    cbm_ht_foreach(map, ns_map_free_entry, NULL);
    cbm_ht_free(map);
}
