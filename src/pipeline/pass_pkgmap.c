/*
 * pass_pkgmap.c — Generic package/module manifest resolution.
 *
 * Scans discovered files for manifest files (package.json, go.mod, Cargo.toml,
 * pyproject.toml, composer.json, pubspec.yaml, pom.xml, build.gradle, mix.exs,
 * *.gemspec) and builds a hash table mapping bare package specifiers to resolved
 * module QNs. This enables IMPORTS edges for non-relative imports like
 * "@myorg/pkg", "github.com/foo/bar", "use my_crate::foo".
 *
 * Integration: called from parallel extract workers (per-worker local arrays)
 * and merged sequentially before registry build.
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "discover/discover.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"
#include "foundation/win_utf8.h"
#include "foundation/yaml.h"

#include <yyjson/yyjson.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* Read an entire file into a malloc'd buffer. Returns NULL on failure. */
static char *pkgmap_read_file(const char *path, int *out_len) {
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > (long)CBM_SZ_1K * CBM_SZ_1K) { /* 1MB cap for manifests */
        (void)fclose(f);
        return NULL;
    }
    char *buf = (char *)malloc((size_t)size + SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, SKIP_ONE, (size_t)size, f);
    (void)fclose(f);
    buf[nread] = '\0';
    *out_len = (int)nread;
    return buf;
}

/* ── Constants ─────────────────────────────────────────────────── */

enum {
    PKGMAP_INIT_CAP = 16,
    PKGMAP_PATH_BUF = 1024,
    PKGMAP_LINE_BUF = 512,
    PKGMAP_HT_INIT = 64,
    PKGMAP_ITOA_BUF = 16,
    /* Hard ceiling on recursive directory descent. This is the
     * non-negotiable termination guarantee for the manifest walk: even
     * if the filesystem contains directory junctions / symlink cycles
     * (the documented reason the walk was once disabled on Windows),
     * descent stops at this depth so the walk can never hang. 64 is far
     * deeper than any real source tree. */
    PKGMAP_WALK_MAX_DEPTH = 64,
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
    if (e->count >= e->cap) {
        int new_cap = e->cap == 0 ? PKGMAP_INIT_CAP : e->cap * SKIP_ONE * PAIR_LEN;
        cbm_pkg_entry_t *tmp = realloc(e->items, new_cap * sizeof(cbm_pkg_entry_t));
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
        return strdup("");
    }
    return cbm_strndup(rel_path, (size_t)(last - rel_path));
}

/* Strip file extension from a path. Returns heap-allocated string.
 * "src/index.ts" → "src/index", "lib/main" → "lib/main" */
static char *strip_extension(const char *path) {
    size_t len = strlen(path);
    for (size_t i = len; i > 0; i--) {
        if (path[i - SKIP_ONE] == '.') {
            return cbm_strndup(path, i - SKIP_ONE);
        }
        if (path[i - SKIP_ONE] == '/') {
            break;
        }
    }
    return strdup(path);
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
    char buf[PKGMAP_PATH_BUF];
    if (dir[0] == '\0') {
        snprintf(buf, sizeof(buf), "%s", entry);
    } else {
        snprintf(buf, sizeof(buf), "%s/%s", dir, entry);
    }
    return strip_extension(buf);
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
        char *resolved = join_and_strip(dir, entry);
        if (resolved) {
            pkg_entries_push(entries, strdup(name), resolved);
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
     * For "." dir, use empty string. */
    pkg_entries_push(entries, module_path, strdup(dir));
    free(dir);
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

/* Build entry path: dir/suffix or just suffix if dir is empty. */
static char *build_entry_path(const char *rel_path, const char *suffix) {
    char *dir = path_dirname(rel_path);
    char buf[PKGMAP_PATH_BUF];
    snprintf(buf, sizeof(buf), "%s%s%s", dir[0] ? dir : "", dir[0] ? "/" : "", suffix);
    free(dir);
    return strdup(buf);
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

    /* Register src/<name>/__init__ as primary entry */
    char suffix[PKGMAP_PATH_BUF];
    snprintf(suffix, sizeof(suffix), "src/%s/__init__", name);
    char *entry = build_entry_path(rel_path, suffix);
    char *name_copy = strdup(name);
    pkg_entries_push(entries, name, entry);

    /* Also register <name>/__init__ as alternative (no src/ prefix) */
    snprintf(suffix, sizeof(suffix), "%s/__init__", name_copy);
    char *alt_entry = build_entry_path(rel_path, suffix);
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
        char ns_entry[PKGMAP_PATH_BUF];
        if (dir[0]) {
            snprintf(ns_entry, sizeof(ns_entry), "%s/%s", dir, ns_dir);
        } else {
            snprintf(ns_entry, sizeof(ns_entry), "%s", ns_dir);
        }
        size_t nelen = strlen(ns_entry);
        if (nelen > 0 && ns_entry[nelen - SKIP_ONE] == '/') {
            ns_entry[nelen - SKIP_ONE] = '\0';
        }
        pkg_entries_push(entries, strdup(ns_prefix), strdup(ns_entry));
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

    /* Register package name → directory */
    yyjson_val *name_val = yyjson_obj_get(root, "name");
    if (yyjson_is_str(name_val)) {
        const char *name = yyjson_get_str(name_val);
        if (name && name[0] != '\0') {
            pkg_entries_push(entries, strdup(name), strdup(dir));
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
        char *dir = path_dirname(rel_path);
        char entry[PKGMAP_PATH_BUF];
        snprintf(entry, sizeof(entry), "%s%slib", dir[0] ? dir : "", dir[0] ? "/" : "");
        pkg_entries_push(entries, strdup(name), strdup(entry));
        free(dir);
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
        char pkg_name[PKGMAP_PATH_BUF];
        snprintf(pkg_name, sizeof(pkg_name), "%s.%s", group_id, artifact_id);
        char *dir = path_dirname(rel_path);
        char entry[PKGMAP_PATH_BUF];
        snprintf(entry, sizeof(entry), "%s%ssrc/main/java", dir[0] ? dir : "", dir[0] ? "/" : "");
        pkg_entries_push(entries, strdup(pkg_name), strdup(entry));

        /* Also register just the groupId for package-level imports */
        char grp_entry[PKGMAP_PATH_BUF];
        snprintf(grp_entry, sizeof(grp_entry), "%s%ssrc/main/java", dir[0] ? dir : "",
                 dir[0] ? "/" : "");
        pkg_entries_push(entries, strdup(group_id), strdup(grp_entry));
        free(dir);
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
    char *dir = path_dirname(rel_path);
    /* Check for src/main/java or src/main/kotlin */
    char entry[PKGMAP_PATH_BUF];
    snprintf(entry, sizeof(entry), "%s%ssrc/main/java", dir[0] ? dir : "", dir[0] ? "/" : "");
    pkg_entries_push(entries, group, strdup(entry));
    free(dir);
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
    char *dir = path_dirname(rel_path);
    char entry[PKGMAP_PATH_BUF];
    snprintf(entry, sizeof(entry), "%s%slib/%s", dir[0] ? dir : "", dir[0] ? "/" : "", app_name);
    /* Register with colon prefix as Elixir uses :atom syntax */
    char atom_name[PKGMAP_PATH_BUF];
    snprintf(atom_name, sizeof(atom_name), "%s", app_name);
    pkg_entries_push(entries, strdup(atom_name), strdup(entry));
    free(app_name);
    free(dir);
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
                char *dir = path_dirname(rel_path);
                char entry[PKGMAP_PATH_BUF];
                snprintf(entry, sizeof(entry), "%s%slib/%s", dir[0] ? dir : "", dir[0] ? "/" : "",
                         name);
                pkg_entries_push(entries, name, strdup(entry));
                free(dir);
                return;
            }
            p = found + SKIP_ONE;
        }
    }
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
        strcmp(basename, "build.gradle.kts") == 0 || strcmp(basename, "mix.exs") == 0) {
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
    wchar_t *wpath = cbm_utf8_to_wide(abs_path);
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

/* True if abs_path is a Windows reparse point (directory junction or
 * symlink). Following these is the documented cause of the historic CI
 * hang, so we skip them before recursing. On POSIX this is a no-op
 * (symlinks are already filtered by pkgmap_safe_stat's S_ISLNK check).
 * Note: the PKGMAP_WALK_MAX_DEPTH bound below is the hard termination
 * guarantee; this check is a best-effort early skip on top of it. */
#ifdef _WIN32
static bool pkgmap_is_reparse_point(const char *abs_path) {
    wchar_t *wpath = cbm_utf8_to_wide(abs_path);
    if (!wpath) {
        return false;
    }
    DWORD attrs = GetFileAttributesW(wpath);
    free(wpath);
    if (attrs == INVALID_FILE_ATTRIBUTES) {
        return false;
    }
    return (attrs & FILE_ATTRIBUTE_REPARSE_POINT) != 0;
}
#endif

/* Recursive filesystem walker that finds and parses package manifest
 * files independently of the main discovery filter. The main discovery
 * filter intentionally hides package.json / composer.json etc. from
 * code indexing (they're config, not source), but pass_pkgmap still
 * needs to read them to resolve workspace imports. Skips directories
 * matched by the shared cbm_should_skip_dir helper so we don't walk
 * node_modules, .git, build, etc. Returns the number of manifests
 * parsed, accumulated across the whole walk.
 *
 * Cross-platform: uses the portable cbm_opendir/cbm_readdir/cbm_closedir
 * API (same as src/discover/discover.c) and the symlink-skipping
 * pkgmap_safe_stat. Termination is guaranteed by the PKGMAP_WALK_MAX_DEPTH
 * recursion bound — even directory junctions / symlink cycles cannot make
 * it hang. On Windows we additionally skip reparse points before
 * descending as a best-effort early-out. */
static int pkgmap_walk_dir(const char *abs_dir, const char *rel_dir, cbm_pkg_entries_t *entries,
                           int depth, char **excluded_dirs, int excluded_count) {
    if (depth >= PKGMAP_WALK_MAX_DEPTH) {
        cbm_log_info("pkgmap.walk", "depth_cap", rel_dir && rel_dir[0] ? rel_dir : ".");
        return 0;
    }
    cbm_dir_t *dir = cbm_opendir(abs_dir);
    if (!dir) {
        return 0;
    }
    int parsed = 0;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(dir)) != NULL) {
        const char *name = entry->name;
        if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'))) {
            continue;
        }
        char abs_path[PKGMAP_PATH_BUF];
        char rel_path[PKGMAP_PATH_BUF];
        snprintf(abs_path, sizeof(abs_path), "%s/%s", abs_dir, name);
        if (rel_dir && rel_dir[0]) {
            snprintf(rel_path, sizeof(rel_path), "%s/%s", rel_dir, name);
        } else {
            snprintf(rel_path, sizeof(rel_path), "%s", name);
        }
        struct stat st;
        if (pkgmap_safe_stat(abs_path, &st) != 0) {
            continue;
        }
        if (S_ISDIR(st.st_mode)) {
            if (cbm_should_skip_dir(name, CBM_MODE_FULL) ||
                cbm_pipeline_relpath_is_excluded(rel_path, excluded_dirs, excluded_count)) {
                continue;
            }
#ifdef _WIN32
            /* Don't follow Windows directory junctions — they can form
             * cycles. The depth bound is the hard guarantee; this just
             * avoids wasted descent. (POSIX symlinks are already skipped by
             * pkgmap_safe_stat's S_ISLNK check.) */
            if (pkgmap_is_reparse_point(abs_path)) {
                continue;
            }
#endif
            parsed += pkgmap_walk_dir(abs_path, rel_path, entries, depth + 1, excluded_dirs,
                                      excluded_count);
            continue;
        }
        if (!S_ISREG(st.st_mode)) {
            continue;
        }
        if (!is_pkgmap_manifest_basename(name)) {
            continue;
        }
        int source_len = 0;
        char *source = pkgmap_read_file(abs_path, &source_len);
        if (!source) {
            continue;
        }
        if (cbm_pkgmap_try_parse(name, rel_path, source, source_len, entries)) {
            parsed++;
        }
        free(source);
    }
    cbm_closedir(dir);
    return parsed;
}

/* Scan a repository for package manifest files via the filesystem
 * walker above. Always-available companion to the parallel path's
 * per-worker manifest parsing, which is bound to whatever `files[]`
 * the discoverer produces and therefore misses ignored manifests like
 * package.json. NULL-safe; returns 0 entries when repo_path is unset.
 *
 * Cross-platform: the walk runs on every platform via pkgmap_walk_dir,
 * which is depth-bounded (PKGMAP_WALK_MAX_DEPTH) and skips symlinks /
 * Windows reparse points, so it cannot hang on directory junctions.
 * This is what lets bare workspace imports (e.g. "@org/pkg" declared in
 * an ignored package.json) resolve on Windows as well as POSIX. */
int cbm_pkgmap_scan_repo(const char *repo_path, cbm_pkg_entries_t *entries, char **excluded_dirs,
                         int excluded_count) {
    if (!repo_path || !entries) {
        return 0;
    }
    int parsed = pkgmap_walk_dir(repo_path, "", entries, 0, excluded_dirs, excluded_count);
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
        char result[PKGMAP_PATH_BUF];
        snprintf(result, sizeof(result), "%s.%s", base_qn, subpath);
        /* Replace / with . in the appended part */
        for (char *c = result + strlen(base_qn) + SKIP_ONE; *c; c++) {
            if (*c == '/') {
                *c = '.';
            }
        }
        free(buf);
        return strdup(result);
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
        char subpath_slashed[PKGMAP_PATH_BUF];
        snprintf(subpath_slashed, sizeof(subpath_slashed), "%s", subpath);
        for (char *c = subpath_slashed; *c; c++) {
            if (*c == '.') {
                *c = '/';
            }
        }
        char result[PKGMAP_PATH_BUF];
        snprintf(result, sizeof(result), "%s/%s", base_qn, subpath_slashed);
        free(buf);
        return cbm_pipeline_fqn_module(project_name, result);
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
        char prefix[PKGMAP_PATH_BUF];
        snprintf(prefix, sizeof(prefix), "%s\\", buf);
        const char *base_dir = (const char *)cbm_ht_get(map, prefix);
        if (!base_dir) {
            continue;
        }
        const char *subpath = module_path + (size_t)(bs - buf) + SKIP_ONE;
        char path_result[PKGMAP_PATH_BUF];
        snprintf(path_result, sizeof(path_result), "%s/%s", base_dir, subpath);
        for (char *c = path_result; *c; c++) {
            if (*c == '\\') {
                *c = '/';
            }
        }
        free(buf);
        return cbm_pipeline_fqn_module(project_name, path_result);
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

/* Derive a representative imported symbol name from a raw module/use path,
 * stripping language decorations so it can be matched against an in-graph node:
 *   - trailing alias " as X"      (Rust/Kotlin)
 *   - trailing glob "::*" / ".*"  (Rust/Kotlin/Java wildcard)
 *   - brace groups "{a, b, ...}"  (Rust grouped use) → first member
 * Writes the result into `out` (size `outsz`) and returns it, or NULL if none.
 * For grouped/braced forms the first listed symbol is used as the representative
 * (the tests only require at least one resolved IMPORTS edge per statement). */
static const char *import_candidate_symbol(const char *module_path, char *out, size_t outsz) {
    if (!module_path || !module_path[0]) {
        return NULL;
    }
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s", module_path);

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
            snprintf(out, outsz, "%s", first);
            return out;
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
        return NULL;
    }
    const char *seg = import_last_segment(buf);
    if (!seg || !seg[0] || strcmp(seg, "*") == 0) {
        return NULL;
    }
    snprintf(out, outsz, "%s", seg);
    return out;
}

/* True for node labels that represent an importable definition (so a symbol-name
 * fallback does not link to, e.g., a Variable or Field). */
static bool import_targetable_label(const char *label) {
    if (!label) {
        return false;
    }
    static const char *ok[] = {"Class", "Interface", "Function", "Method", "Module", "Struct",
                               "Enum",  "Trait",     "Type",     "File",   NULL};
    for (const char **l = ok; *l; l++) {
        if (strcmp(*l, label) == 0) {
            return true;
        }
    }
    return false;
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
static const cbm_gbuf_node_t *resolve_sibling_file(const cbm_pipeline_ctx_t *ctx,
                                                   const char *source_rel,
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

    /* Candidate relative paths, in priority order. */
    char cands[5][PKGMAP_PATH_BUF];
    int ncand = 0;
    const char *base = module_path;
    /* Skip a leading "./". */
    if (base[0] == '.' && base[1] == '/') {
        base += 2;
    }
    /* 1. Direct sibling: dir/<module_path>. */
    snprintf(cands[ncand++], PKGMAP_PATH_BUF, "%s%s%s", dir, dir[0] ? "/" : "", base);
    /* 2. SCSS partial: dir/[subdir/]_<basename>.scss (underscore-prefixed). */
    {
        const char *slash = strrchr(base, '/');
        const char *bn = slash ? slash + 1 : base;
        char *dpart = slash ? cbm_strndup(base, (size_t)(slash - base)) : strdup("");
        if (dpart && bn[0] != '_') {
            snprintf(cands[ncand++], PKGMAP_PATH_BUF, "%s%s%s%s_%s.scss", dir, dir[0] ? "/" : "",
                     dpart[0] ? dpart : "", dpart[0] ? "/" : "", bn);
        }
        free(dpart);
    }
    /* 3. Meson subdir: dir/<module_path>/meson.build. */
    snprintf(cands[ncand++], PKGMAP_PATH_BUF, "%s%s%s/meson.build", dir, dir[0] ? "/" : "", base);
    /* 4. Basename sibling: dir/<basename(module_path)>.  Covers include paths
     *    that carry a non-relative prefix (Hyprlang `source = ~/.config/.../x.conf`,
     *    absolute include paths) but reference a file sitting beside the importer. */
    {
        const char *slash = strrchr(base, '/');
        if (slash && slash[1]) {
            snprintf(cands[ncand++], PKGMAP_PATH_BUF, "%s%s%s", dir, dir[0] ? "/" : "", slash + 1);
        }
    }

    const cbm_gbuf_node_t *found = NULL;
    for (int i = 0; i < ncand; i++) {
        char *qn = cbm_pipeline_fqn_module(ctx->project_name, cands[i]);
        if (!qn) {
            continue;
        }
        const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(ctx->gbuf, qn);
        free(qn);
        if (n && import_targetable_label(n->label) &&
            (!source_file_qn || !n->qualified_name ||
             strcmp(n->qualified_name, source_file_qn) != 0)) {
            found = n;
            break;
        }
    }
    free(dir);
    return found;
}

const cbm_gbuf_node_t *cbm_pipeline_resolve_import_node(const cbm_pipeline_ctx_t *ctx,
                                                        const char *source_rel,
                                                        const char *source_file_qn,
                                                        const CBMImport *imp,
                                                        CBMHashTable *namespace_map) {
    if (!ctx || !imp || !imp->module_path) {
        return NULL;
    }

    /* Strategy 1: module-path resolution → existing node (Python/TS/Go).
     * No label filter here: directory-module languages (Go/Java packages)
     * legitimately resolve straight to a Folder node -- that's the intended,
     * correct import target, not a collision. The Folder-collision problem
     * (#767) only shows up downstream, in Strategy 4's retry-with-truncated-
     * path loop, which re-enters resolve_module with a DIFFERENT, shortened
     * string that the original import never named. */
    char *target_qn = cbm_pipeline_resolve_module(ctx, source_rel, imp->module_path);
    const cbm_gbuf_node_t *target = target_qn ? cbm_gbuf_find_by_qn(ctx->gbuf, target_qn) : NULL;
    free(target_qn);
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
        char norm[1024];
        snprintf(norm, sizeof(norm), "%s", imp->module_path);
        char *brace = strchr(norm, '{');
        if (brace) {
            *brace = '\0'; /* drop the group; the prefix is the namespace */
        }
        char *as = strstr(norm, " as ");
        if (as) {
            *as = '\0';
        }
        for (char *p = norm; *p; p++) {
            if (*p == '\\' || *p == ':' || *p == '/') {
                *p = '.';
            }
        }
        /* Strip trailing glob/separator noise. */
        size_t nl = strlen(norm);
        while (nl > 0 && (norm[nl - 1] == '*' || norm[nl - 1] == '.' || norm[nl - 1] == ' ')) {
            norm[--nl] = '\0';
        }
        /* Collapse any "::"-induced empty segments ("a..b" → "a.b"). */
        for (;;) {
            char *dd = strstr(norm, "..");
            if (!dd) {
                break;
            }
            memmove(dd, dd + 1, strlen(dd + 1) + 1);
        }
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
                char qbuf[1024];
                if (len > 0 && len < sizeof(qbuf)) {
                    memcpy(qbuf, seg, len);
                    qbuf[len] = '\0';
                    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(ctx->gbuf, qbuf);
                    if (n && (!source_file_qn || strcmp(n->qualified_name, source_file_qn) != 0)) {
                        return n;
                    }
                }
                seg = eol ? eol + 1 : NULL;
            }
            char *dot = strrchr(norm, '.');
            if (!dot) {
                break;
            }
            *dot = '\0';
        }
    }

    /* Strategy 3: symbol-name fallback.  Derive a representative imported
     * symbol (handling alias / glob / grouped forms) and match it against an
     * in-graph definition of the same simple name in another file
     * (Rust `helper`, Java `Util`, Kotlin grouped, ...). */
    char symbuf[256];
    /* Prefer the clean candidate from the module path; the local_name may be an
     * alias (Rust `as h`, Kotlin `as U`) that names no real symbol. */
    const char *seg = import_candidate_symbol(imp->module_path, symbuf, sizeof(symbuf));
    if (!seg && imp->local_name && imp->local_name[0] && strcmp(imp->local_name, "*") != 0) {
        seg = imp->local_name;
    }
    /* Build the candidate symbol list: the derived symbol plus, when the import
     * is a dotted member path like `com.example.Config.DEFAULT`, the enclosing
     * type segments (`Config`).  This resolves object/class-member imports to
     * the declaring type when the leaf member isn't an importable node. */
    const char *cands[8];
    int ncands = 0;
    if (seg && seg[0] && strcmp(seg, "*") != 0) {
        cands[ncands++] = seg;
    }
    {
        /* Strip decorations, normalize separators to '.', and collect the
         * trailing path segments (last first) as fallback candidates. */
        char mp[1024];
        snprintf(mp, sizeof(mp), "%s", imp->module_path);
        char *br = strchr(mp, '{');
        if (br) {
            *br = '\0';
        }
        char *as3 = strstr(mp, " as ");
        if (as3) {
            *as3 = '\0';
        }
        for (char *p = mp; *p; p++) {
            if (*p == '\\' || *p == ':' || *p == '/') {
                *p = '.';
            }
        }
        /* Walk segments from the end. */
        char *end = mp + strlen(mp);
        while (end > mp && ncands < 8) {
            char *dot = NULL;
            for (char *p = end - 1; p >= mp; p--) {
                if (*p == '.') {
                    dot = p;
                    break;
                }
            }
            const char *s = dot ? dot + 1 : mp;
            if (s[0] && strcmp(s, "*") != 0) {
                bool dup = false;
                for (int k = 0; k < ncands; k++) {
                    if (strcmp(cands[k], s) == 0) {
                        dup = true;
                        break;
                    }
                }
                if (!dup) {
                    cands[ncands++] = s;
                }
            }
            if (!dot) {
                break;
            }
            *dot = '\0';
            end = dot;
        }
        for (int ci = 0; ci < ncands; ci++) {
            const cbm_gbuf_node_t **hits = NULL;
            int n = 0;
            if (cbm_gbuf_find_by_name(ctx->gbuf, cands[ci], &hits, &n) == 0 && hits) {
                /* Deterministic winner: hits[] is in node-registration order,
                 * which under parallel extraction varies run to run — taking
                 * the FIRST targetable hit made the same import resolve to
                 * different same-named nodes across runs (xfs: 360 flickering
                 * IMPORTS diff lines between two MT runs). Pick the candidate
                 * with the lexicographically smallest qualified name instead:
                 * stable, content-derived, identical for ST and MT. */
                const cbm_gbuf_node_t *best = NULL;
                for (int i = 0; i < n; i++) {
                    const cbm_gbuf_node_t *cand = hits[i];
                    if (!cand || !import_targetable_label(cand->label)) {
                        continue;
                    }
                    if (source_file_qn && cand->qualified_name &&
                        strcmp(cand->qualified_name, source_file_qn) == 0) {
                        continue; /* self */
                    }
                    if (!best || (cand->qualified_name && best->qualified_name &&
                                  strcmp(cand->qualified_name, best->qualified_name) < 0)) {
                        best = cand;
                    }
                }
                if (best) {
                    return best;
                }
            }
        }
    }

    /* Strategy 4: crate-relative module path → File/Module node.  Rust glob
     * `use crate::ops::*` names a module, not a symbol; strip the glob and the
     * `crate::`/`self::`/`super::` prefix, convert `::`→`/`, then resolve the
     * remaining path (and successive prefixes) to a Module/File node. */
    {
        char mp[1024];
        snprintf(mp, sizeof(mp), "%s", imp->module_path);
        char *brace = strchr(mp, '{');
        if (brace) {
            *brace = '\0';
        }
        char *as2 = strstr(mp, " as ");
        if (as2) {
            *as2 = '\0';
        }
        /* Convert "::" → "/" (drop the doubled colon cleanly). */
        char clean[1024];
        size_t ci = 0;
        for (const char *p = mp; *p && ci + 1 < sizeof(clean); p++) {
            if (*p == ':') {
                if (ci > 0 && clean[ci - 1] == '/') {
                    continue; /* collapse "::" → single "/" */
                }
                clean[ci++] = '/';
            } else if (*p == '*' || *p == ' ') {
                continue;
            } else {
                clean[ci++] = *p;
            }
        }
        clean[ci] = '\0';
        /* Drop trailing slashes. */
        while (ci > 0 && clean[ci - 1] == '/') {
            clean[--ci] = '\0';
        }
        /* Strip a leading crate-relative prefix. */
        const char *body = clean;
        static const char *prefixes[] = {"crate/", "self/", "super/", NULL};
        for (const char **pf = prefixes; *pf; pf++) {
            size_t pl = strlen(*pf);
            if (strncmp(body, *pf, pl) == 0) {
                body += pl;
                break;
            }
        }
        if (body[0]) {
            char work[1024];
            snprintf(work, sizeof(work), "%s", body);
            for (;;) {
                char *rqn = cbm_pipeline_resolve_module(ctx, source_rel, work);
                const cbm_gbuf_node_t *n = rqn ? cbm_gbuf_find_by_qn(ctx->gbuf, rqn) : NULL;
                free(rqn);
                if (n && import_targetable_label(n->label) &&
                    (!source_file_qn || !n->qualified_name ||
                     strcmp(n->qualified_name, source_file_qn) != 0)) {
                    return n;
                }
                char *sl = strrchr(work, '/');
                if (!sl) {
                    break;
                }
                *sl = '\0';
            }
        }
    }

    return NULL;
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
        for (char *p = key; *p; p++) {
            if (*p == '\\' || *p == ':' || *p == '/') {
                *p = '.';
            }
        }
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
