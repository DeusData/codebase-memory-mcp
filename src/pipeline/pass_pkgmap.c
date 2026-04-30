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
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"
#include "foundation/yaml.h"

#include <yyjson/yyjson.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Read an entire file into a malloc'd buffer. Returns NULL on failure. */
static char *pkgmap_read_file(const char *path, int *out_len) {
    FILE *f = fopen(path, "rb");
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
    return strndup(rel_path, (size_t)(last - rel_path));
}

/* Strip file extension from a path. Returns heap-allocated string.
 * "src/index.ts" → "src/index", "lib/main" → "lib/main" */
static char *strip_extension(const char *path) {
    size_t len = strlen(path);
    for (size_t i = len; i > 0; i--) {
        if (path[i - SKIP_ONE] == '.') {
            return strndup(path, i - SKIP_ONE);
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
    return strndup(start, (size_t)(p - start));
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
    char *module_path = strndup(start, (size_t)(val - start));
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
    return strndup(s, (size_t)(p - s));
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
    char *app_name = strndup(start, (size_t)(val - start));
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

/* Build pkgmap for sequential path (reads manifest files directly) */
CBMHashTable *cbm_pkgmap_build_from_files(const cbm_file_info_t *files, int file_count,
                                          const char *project_name) {
    cbm_pkg_entries_t entries;
    cbm_pkg_entries_init(&entries);

    for (int i = 0; i < file_count; i++) {
        const char *basename = path_basename(files[i].rel_path);
        /* Quick check: is this a manifest file? */
        bool is_manifest =
            (strcmp(basename, "package.json") == 0 || strcmp(basename, "go.mod") == 0 ||
             strcmp(basename, "Cargo.toml") == 0 || strcmp(basename, "pyproject.toml") == 0 ||
             strcmp(basename, "composer.json") == 0 || strcmp(basename, "pubspec.yaml") == 0 ||
             strcmp(basename, "pom.xml") == 0 || strcmp(basename, "build.gradle") == 0 ||
             strcmp(basename, "build.gradle.kts") == 0 || strcmp(basename, "mix.exs") == 0 ||
             ends_with(basename, ".gemspec"));
        if (!is_manifest) {
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
