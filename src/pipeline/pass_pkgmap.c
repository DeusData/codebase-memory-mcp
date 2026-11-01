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
#include "pipeline/pass_lsp_cross.h"
#include "pipeline/import_resolve.h"
#include "resolve/capabilities.h"
#include "resolve/policy.h"
#include "lang_specs.h"
#include "helpers.h"
#include "discover/discover.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/hash_table.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/platform.h"
#include "foundation/json_writer.h"
#include "foundation/str_util.h"
#include "foundation/win_utf8.h"
#include "foundation/yaml.h"

#include <yyjson/yyjson.h>

#include <ctype.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

/* Read an entire file into a core-owned buffer (class extract; release with
 * cbm_free(CBM_MEM_CLASS_EXTRACT, ...)). Returns NULL on failure. */
static char *pkgmap_read_file(const char *path, int *out_len) {
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        (void)fclose(f);
        return NULL;
    }
    long size = ftell(f);
    if (fseek(f, 0, SEEK_SET) != 0) {
        (void)fclose(f);
        return NULL;
    }
    if (size <= 0 || size > (long)CBM_SZ_1K * CBM_SZ_1K) { /* 1MB cap for manifests */
        (void)fclose(f);
        return NULL;
    }
    char *buf = (char *)cbm_alloc(CBM_MEM_CLASS_EXTRACT, (size_t)size + SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, SKIP_ONE, (size_t)size, f);
    bool complete = nread == (size_t)size && !ferror(f);
    int closed = fclose(f);
    if (!complete || closed != 0) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, buf);
        return NULL;
    }
    buf[nread] = '\0';
    *out_len = (int)nread;
    return buf;
}

/* ── Constants ─────────────────────────────────────────────────── */

static const char ambiguous_pkgmap_value[] = "@cbm:ambiguous-package-root";
static const char import_context_failed[] = "@cbm:import-context:failed";

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

/* Package collections and their private path/parser strings share SEMANTIC
 * ownership. Public raw-string APIs retain their caller-owned free() contract. */
static char *pkgmap_strndup(const char *source, size_t limit) {
    if (!source)
        return NULL;
    size_t length = 0;
    while (length < limit && source[length])
        length++;
    if (length == SIZE_MAX)
        return NULL;
    char *copy = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, length + 1);
    if (copy) {
        memcpy(copy, source, length);
        copy[length] = '\0';
    }
    return copy;
}

/* Consume a libc-owned FQN/binding result at the private semantic boundary.
 * The public producer still serves callers that release its result with free(). */
static char *pkgmap_take_raw(char *raw) {
    char *owned = raw ? cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, raw) : NULL;
    free(raw);
    return owned;
}

/* ── Per-worker collection ─────────────────────────────────────── */

void cbm_pkg_entries_init(cbm_pkg_entries_t *e) {
    e->items = NULL;
    e->count = 0;
    e->cap = 0;
    e->incomplete = false;
}

static void pkg_entries_push(cbm_pkg_entries_t *e, char *pkg_name, char *entry_rel) {
    if (e->incomplete || !pkg_name || !entry_rel || e->count < 0 || e->cap < 0) {
        e->incomplete = true;
        cbm_free(CBM_MEM_CLASS_SEMANTIC, pkg_name);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, entry_rel);
        return;
    }
    if (e->count >= e->cap) {
        if (e->cap > INT32_MAX / 2) {
            e->incomplete = true;
            cbm_free(CBM_MEM_CLASS_SEMANTIC, pkg_name);
            cbm_free(CBM_MEM_CLASS_SEMANTIC, entry_rel);
            return;
        }
        int new_cap = e->cap == 0 ? PKGMAP_INIT_CAP : e->cap * 2;
        cbm_pkg_entry_t *tmp =
            (size_t)new_cap <= SIZE_MAX / sizeof(*tmp)
                ? cbm_realloc(CBM_MEM_CLASS_SEMANTIC, e->items, (size_t)new_cap * sizeof(*tmp))
                : NULL;
        if (!tmp) {
            e->incomplete = true;
            cbm_free(CBM_MEM_CLASS_SEMANTIC, pkg_name);
            cbm_free(CBM_MEM_CLASS_SEMANTIC, entry_rel);
            return;
        }
        e->items = tmp;
        e->cap = new_cap;
    }
    e->items[e->count].pkg_name = pkg_name;
    e->items[e->count].entry_rel = entry_rel;
    e->items[e->count].node_fact = false;
    e->count++;
}

void cbm_pkg_entries_free(cbm_pkg_entries_t *e) {
    for (int i = 0; i < e->count; i++) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, e->items[i].pkg_name);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, e->items[i].entry_rel);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, e->items);
    e->items = NULL;
    e->count = 0;
    e->cap = 0;
    e->incomplete = false;
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
        return cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, "");
    }
    return pkgmap_strndup(rel_path, (size_t)(last - rel_path));
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
    return pkgmap_strndup(start, (size_t)(p - start));
}

/* ── Language-specific manifest parsers ────────────────────────── */

/* A package map has no per-import condition environment. Only a target
 * invariant over the possible earlier conditions is authoritative. Preserve
 * object order: an unconditional default ends the search. */
static const char *resolve_exports_dot(yyjson_val *value, unsigned depth) {
    if (depth > 16)
        return NULL;
    if (yyjson_is_str(value))
        return yyjson_get_str(value);
    if (!yyjson_is_obj(value))
        return NULL;
    size_t at, count;
    yyjson_val *key, *branch;
    yyjson_obj_iter keys = yyjson_obj_iter_with(value);
    while ((key = yyjson_obj_iter_next(&keys))) {
        const char *condition = yyjson_get_str(key);
        if (!condition || !*condition || condition[0] == '.' ||
            isdigit((unsigned char)condition[0]))
            return NULL;
    }
    const char *common = NULL;
    yyjson_obj_foreach(value, at, count, key, branch) {
        const char *target = resolve_exports_dot(branch, depth + 1);
        if (!target || (common && strcmp(common, target)))
            return NULL;
        if (!strcmp(yyjson_get_str(key), "default"))
            return target;
        common = target;
    }
    return NULL; /* No unconditional branch proves the domain covered. */
}

static bool package_export_path(const char *path) {
    if (!path || strncmp(path, "./", 2) || !path[2])
        return false;
    for (const char *segment = path + 2; *segment;) {
        const char *end = strchr(segment, '/');
        size_t length = end ? (size_t)(end - segment) : strlen(segment);
        if (!length || (length == 1 && segment[0] == '.') ||
            (length == 2 && !memcmp(segment, "..", 2)) ||
            (length == 12 && !strncasecmp(segment, "node_modules", 12)))
            return false;
        for (size_t i = 0; i < length; i++)
            if ((unsigned char)segment[i] < 32 || strchr("%\\?#*", segment[i]))
                return false;
        if (!end)
            return true;
        segment = end + 1;
    }
    return false;
}

/* Node package exports take precedence over main, including blocked or
 * unsupported exports. Neither absent configuration nor the bundler-specific
 * module hint authorizes an invented source entry. */
static const char *resolve_pkg_entry(yyjson_val *root) {
    yyjson_val *exports = yyjson_obj_get(root, "exports");
    if (exports) {
        yyjson_val *entry = exports;
        if (yyjson_is_obj(exports)) {
            bool subpaths = false, conditions = false;
            yyjson_val *key;
            yyjson_obj_iter keys = yyjson_obj_iter_with(exports);
            while ((key = yyjson_obj_iter_next(&keys))) {
                const char *name = yyjson_get_str(key);
                subpaths |= name && name[0] == '.';
                conditions |= !name || name[0] != '.';
            }
            if (subpaths && conditions)
                return NULL;
            if (subpaths)
                entry = yyjson_obj_get(exports, ".");
        }
        const char *target = resolve_exports_dot(entry, 0);
        return package_export_path(target) ? target : NULL;
    }
    yyjson_val *main = yyjson_obj_get(root, "main");
    return yyjson_is_str(main) && yyjson_get_str(main)[0] ? yyjson_get_str(main) : NULL;
}

/* Typed Node facts retain filesystem identities. A package name alone is
 * only a blocker; workspace ownership or package self-reference must supply
 * the source lookup domain before any entry is used. */
static bool node_fact_key(char *out, size_t capacity, const char *kind, const char *directory,
                          const char *name) {
    int n = snprintf(out, capacity, "@cbm:node:%s:%s\n%s", kind, directory, name ? name : "");
    return n > 0 && (size_t)n < capacity;
}

static void node_collect(cbm_pkg_entries_t *entries, const char *kind, const char *directory,
                         const char *name, const char *value) {
    char key[PKGMAP_PATH_BUF];
    if (entries->incomplete)
        return;
    if (!directory || strchr(directory, '\n') || !value ||
        !node_fact_key(key, sizeof(key), kind, directory, name)) {
        entries->incomplete = true;
        return;
    }
    char *owned_key = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, key),
         *owned_value = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, value);
    if (owned_key && owned_value) {
        int previous_count = entries->count;
        pkg_entries_push(entries, owned_key, owned_value);
        if (entries->count > previous_count)
            entries->items[previous_count].node_fact = true;
    } else {
        entries->incomplete = true;
        cbm_free(CBM_MEM_CLASS_SEMANTIC, owned_key);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, owned_value);
    }
}

static bool node_package_name(const char *name) {
    if (!name || !*name || name[0] == '.' || name[0] == '_')
        return false;
    int slash = 0;
    for (const char *p = name; *p; p++) {
        if (*p == '/') {
            if (name[0] != '@' || p == name + 1 || !p[1] || p[1] == '.' || p[1] == '_' ||
                ++slash != 1)
                return false;
        } else if (!(isalnum((unsigned char)*p) || *p == '-' || *p == '_' || *p == '.' ||
                     (p == name && *p == '@')))
            return false;
    }
    return name[0] != '@' || slash == 1;
}

static bool node_workspace_path(const char *value, const char *directory, char *path,
                                size_t capacity, bool *glob) {
    if (!value || !*value || strpbrk(value, "!{}[]"))
        return false;
    if (!strncmp(value, "./", 2))
        value += 2;
    size_t length = strlen(value);
    *glob = !strcmp(value, "*") || (length >= 2 && !strcmp(value + length - 2, "/*"));
    if (!strcmp(value, "*")) {
        int n = snprintf(path, capacity, "%s", directory);
        return n >= 0 && (size_t)n < capacity;
    }
    char relative[PKGMAP_PATH_BUF];
    int n = snprintf(relative, sizeof(relative), "./%.*s", (int)(length - (*glob ? 2 : 0)), value);
    return n > 0 && (size_t)n < sizeof(relative) && package_export_path(relative) &&
           cbm_repo_rel_join(directory, relative, path, capacity);
}

static void node_workspace_paths(yyjson_val *root, const char *directory,
                                 cbm_pkg_entries_t *entries) {
    yyjson_val *workspaces = yyjson_obj_get(root, "workspaces");
    if (!yyjson_is_arr(workspaces))
        return;
    node_collect(entries, "root", directory, NULL, "1");
    size_t i, count;
    yyjson_val *item;
    char path[PKGMAP_PATH_BUF];
    bool glob;
    /* Unknown glob/exclusion semantics cannot preserve only the convenient
     * positive half of a workspace declaration. */
    yyjson_arr_foreach(workspaces, i, count, item) {
        if (!node_workspace_path(yyjson_get_str(item), directory, path, sizeof(path), &glob))
            return;
    }
    yyjson_arr_foreach(workspaces, i, count, item) {
        if (node_workspace_path(yyjson_get_str(item), directory, path, sizeof(path), &glob))
            node_collect(entries, glob ? "glob" : "workspace", path, NULL, directory);
    }
}

/* JSON duplicate keys and embedded NULs have no authoritative first-value
 * interpretation. Reject the manifest rather than differ from its runtime. */
static bool node_manifest_unique(yyjson_val *value, unsigned depth) {
    if (!value || depth > 32)
        return false;
    if (yyjson_is_str(value))
        return strlen(yyjson_get_str(value)) == yyjson_get_len(value);
    size_t i, count;
    yyjson_val *item, *key;
    if (yyjson_is_arr(value)) {
        yyjson_arr_foreach(value, i, count, item) {
            if (!node_manifest_unique(item, depth + 1))
                return false;
        }
    } else if (yyjson_is_obj(value)) {
        CBMHashTable *seen = cbm_ht_create(PKGMAP_HT_INIT);
        if (!seen)
            return false;
        bool valid = true;
        yyjson_obj_foreach(value, i, count, key, item) {
            const char *name = yyjson_get_str(key);
            if (!name || strlen(name) != yyjson_get_len(key) || cbm_ht_has(seen, name) ||
                !node_manifest_unique(item, depth + 1)) {
                valid = false;
                break;
            }
            cbm_ht_set(seen, name, (void *)(uintptr_t)1);
            if (!cbm_ht_has(seen, name)) {
                valid = false;
                break;
            }
        }
        cbm_ht_free(seen);
        return valid;
    }
    return true;
}

/* A nearer dependency declaration may install a different version or npm
 * alias ahead of the root workspace link. Without dependency solving it is
 * a blocking fact, even when a same-named workspace package exists. */
static void node_dependencies(yyjson_val *root, const char *directory, cbm_pkg_entries_t *entries) {
    static const char *const kinds[] = {"dependencies", "devDependencies", "optionalDependencies",
                                        "peerDependencies"};
    for (size_t k = 0; k < sizeof(kinds) / sizeof(kinds[0]); k++) {
        yyjson_val *dependencies = yyjson_obj_get(root, kinds[k]);
        if (!dependencies)
            continue;
        if (!yyjson_is_obj(dependencies)) {
            node_collect(entries, "dependency", directory, NULL, "1");
            continue;
        }
        yyjson_obj_iter keys = yyjson_obj_iter_with(dependencies);
        yyjson_val *key;
        while ((key = yyjson_obj_iter_next(&keys))) {
            const char *name = yyjson_get_str(key);
            if (node_package_name(name))
                node_collect(entries, "dependency", directory, name, "1");
        }
    }
}

static void node_entry(cbm_pkg_entries_t *entries, const char *directory, const char *subpath,
                       const char *target) {
    char path[PKGMAP_PATH_BUF];
    const char *value = target && cbm_repo_rel_join(directory, target, path, sizeof(path))
                            ? path
                            : ambiguous_pkgmap_value;
    node_collect(entries, "entry", directory, subpath, value);
}

/* JS/TS package manifests declare exact entries, never a guessed src/index. */
static void parse_package_json(const char *source, int source_len, const char *rel_path,
                               cbm_pkg_entries_t *entries) {
    char *directory = path_dirname(rel_path);
    if (!directory) {
        entries->incomplete = true;
        return;
    }
    yyjson_doc *doc = yyjson_read(source, (size_t)source_len, 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    if (!yyjson_is_obj(root) || !node_manifest_unique(root, 0)) {
        entries->incomplete = true;
        goto done;
    }
    const char *name = yyjson_get_str(yyjson_obj_get(root, "name"));
    node_collect(entries, "scope", directory, NULL, node_package_name(name) ? name : "");
    node_workspace_paths(root, directory, entries);
    node_dependencies(root, directory, entries);
    if (!node_package_name(name))
        goto done;
    /* Legacy consumers may recognize ownership, but cannot convert this
     * bare name into a global module or directory prefix. */
    char *owned_name = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, name),
         *blocked = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, ambiguous_pkgmap_value);
    if (owned_name && blocked)
        pkg_entries_push(entries, owned_name, blocked);
    else {
        entries->incomplete = true;
        cbm_free(CBM_MEM_CLASS_SEMANTIC, owned_name);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, blocked);
    }
    yyjson_val *exports = yyjson_obj_get(root, "exports");
    if (exports)
        node_collect(entries, "self", directory, NULL, "1");
    node_entry(entries, directory, ".", resolve_pkg_entry(root));
    if (yyjson_is_obj(exports)) {
        bool subpaths = true;
        yyjson_val *key;
        yyjson_obj_iter keys = yyjson_obj_iter_with(exports);
        while ((key = yyjson_obj_iter_next(&keys))) {
            const char *value = yyjson_get_str(key);
            subpaths = subpaths && value && value[0] == '.';
        }
        if (subpaths) {
            size_t i, count;
            yyjson_val *branch;
            yyjson_obj_foreach(exports, i, count, key, branch) {
                const char *path = yyjson_get_str(key);
                if (!package_export_path(path))
                    continue;
                const char *target = resolve_exports_dot(branch, 0);
                node_entry(entries, directory, path, package_export_path(target) ? target : NULL);
            }
        }
    }
done:
    yyjson_doc_free(doc);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, directory);
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
    char *module_path = pkgmap_strndup(start, (size_t)(val - start));
    char *dir = path_dirname(rel_path);

    /* The module path maps to the directory containing go.mod.
     * For "." dir, use empty string. */
    pkg_entries_push(entries, module_path, cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, dir));
    cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
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
    cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
    return cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, buf);
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

/* Extract PSR-4 entries from one composer.json autoload section ("autoload"
 * or "autoload-dev": composer registers both, so test namespaces such as
 * Tests\ map to their directory as well). */
static void extract_psr4(yyjson_val *root, const char *section, const char *dir,
                         cbm_pkg_entries_t *entries) {
    yyjson_val *autoload = yyjson_obj_get(root, section);
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
        while (ns_dir[0] == '.' && ns_dir[SKIP_ONE] == '/') {
            ns_dir += PAIR_LEN; /* "./src/" names src/ */
        }
        if (strcmp(ns_dir, ".") == 0) {
            ns_dir = ""; /* "." names the manifest's own directory */
        }
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
        pkg_entries_push(entries, cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, ns_prefix),
                         cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, ns_entry));
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
            pkg_entries_push(entries, cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, name),
                             cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, dir));
        }
    }

    extract_psr4(root, "autoload", dir, entries);
    extract_psr4(root, "autoload-dev", dir, entries);

    cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
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
        pkg_entries_push(entries, cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, name),
                         cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, entry));
        cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
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
    return pkgmap_strndup(s, (size_t)(p - s));
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
        pkg_entries_push(entries, cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, pkg_name),
                         cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, entry));

        /* Also register just the groupId for package-level imports */
        char grp_entry[PKGMAP_PATH_BUF];
        snprintf(grp_entry, sizeof(grp_entry), "%s%ssrc/main/java", dir[0] ? dir : "",
                 dir[0] ? "/" : "");
        pkg_entries_push(entries, cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, group_id),
                         cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, grp_entry));
        cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
    }

    cbm_free(CBM_MEM_CLASS_SEMANTIC, group_id);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, artifact_id);
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
    pkg_entries_push(entries, group, cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, entry));
    cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
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
    char *app_name = pkgmap_strndup(start, (size_t)(val - start));
    char *dir = path_dirname(rel_path);
    char entry[PKGMAP_PATH_BUF];
    snprintf(entry, sizeof(entry), "%s%slib/%s", dir[0] ? dir : "", dir[0] ? "/" : "", app_name);
    /* Register with colon prefix as Elixir uses :atom syntax */
    char atom_name[PKGMAP_PATH_BUF];
    snprintf(atom_name, sizeof(atom_name), "%s", app_name);
    pkg_entries_push(entries, cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, atom_name),
                     cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, entry));
    cbm_free(CBM_MEM_CLASS_SEMANTIC, app_name);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
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
                pkg_entries_push(entries, name, cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, entry));
                cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
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
 * not a Swift evaluator: it locates `.target(...)` call expressions via a
 * comment/string-aware token scan and pulls only their quoted-string-literal
 * `name:` / `path:` arguments. Anything computed or concatenated is
 * skipped, not guessed — fail-closed, like every other parser here.
 *
 * Only a manifest's OWN `.target(...)` declarations self-register — never
 * `.library(...)` products (a product name is not generally an importable
 * module: SwiftPM lets it alias multiple targets, or none sharing its
 * name), and never `.package(url:)` / `.package(path:)` dependencies or
 * target-to-target dependency references. A dependency resolves because
 * the DEPENDENCY's own Package.swift registers itself when the repo-wide
 * manifest walk (cbm_pkgmap_scan_repo) reaches it, exactly like a JS
 * workspace sibling's package.json (see repro_issue408.c).
 *
 * Deliberately out of scope (matches the item-1 authorization): evaluating
 * `#if`/`#endif` conditional-compilation blocks. A `.target(...)` inside
 * one is scanned like any other — teaching the extractor which platform
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
    char *value = pkgmap_strndup(start, (size_t)(p - start));
    p++; /* past closing quote */
    while (p < end && (*p == ' ' || *p == '\t')) {
        p++;
    }
    if (p >= end || *p == ',') {
        return value;
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, value);
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
 * ownership. */
static void swift_register_target(const char *rel_path, const char *target_name,
                                  const char *literal_path, cbm_pkg_entries_t *entries) {
    if (!target_name || !target_name[0]) {
        return;
    }
    char suffix_buf[PKGMAP_PATH_BUF];
    const char *suffix;
    if (literal_path && literal_path[0]) {
        suffix = literal_path;
    } else {
        snprintf(suffix_buf, sizeof(suffix_buf), "Sources/%s", target_name);
        suffix = suffix_buf;
    }
    char *entry = build_entry_path(rel_path, suffix);
    if (entry) {
        pkg_entries_push(entries, cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, target_name), entry);
    }
}

/* Scan for `.target(name: "Foo", ...)` declarations, skipping any
 * `.target(` spelling that falls inside a comment or string literal.
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
    static const char prefix[] = ".target(";
    const char *cursor = source;
    while (cursor < end) {
        const char *hit = swift_find_code_token(cursor, end, prefix);
        if (!hit) {
            return;
        }
        const char *open = hit + strlen(prefix) - SKIP_ONE;
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
            cbm_free(CBM_MEM_CLASS_SEMANTIC, literal_path);
            cbm_free(CBM_MEM_CLASS_SEMANTIC, name);
        }
        cursor = close + SKIP_ONE;
    }
}

/* SwiftPM: Package.swift — targets → Sources/<name> (or their literal
 * `path:`). Products deliberately do not self-register: see the file
 * comment above swift_find_code_token. */
static void parse_package_swift(const char *source, int source_len, const char *rel_path,
                                cbm_pkg_entries_t *entries) {
    const char *end = source + source_len;
    swift_scan_targets(source, end, rel_path, entries);
}

/* ── Public: manifest detection + parsing ──────────────────────── */

bool cbm_pkgmap_try_parse(const char *basename, const char *rel_path, const char *source,
                          int source_len, cbm_pkg_entries_t *entries) {
    if (!basename || !source || source_len <= 0) {
        if (entries && basename && !strcmp(basename, "package.json"))
            entries->incomplete = true;
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
        /* A distribution name is not an import path or a package directory.
         * Do not invent competing root/src mappings from [project].name.
         * Exact physical modules and explicitly supplied mappings retain
         * their existing independent resolution paths. */
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

/* A composer.json `autoload.psr-4` key: a namespace prefix ending in '\\'. */
static bool pkgmap_is_psr4_prefix(const char *pkg_name) {
    size_t n = pkg_name ? strlen(pkg_name) : 0;
    return n > 0 && pkg_name[n - SKIP_ONE] == '\\';
}

static const char *node_fact(CBMHashTable *map, const char *kind, const char *directory,
                             const char *name) {
    char key[PKGMAP_PATH_BUF];
    if (!map)
        return NULL;
    return node_fact_key(key, sizeof(key), kind, directory, name) ? cbm_ht_get(map, key)
                                                                  : ambiguous_pkgmap_value;
}

static bool node_bind_workspace(CBMHashTable *map, const char *directory, const char *name) {
    if (!directory[0] || !node_package_name(name))
        return true;
    const char *owner = node_fact(map, "workspace", directory, NULL);
    char parent[PKGMAP_PATH_BUF];
    int length = snprintf(parent, sizeof(parent), "%s", directory);
    if (length < 0 || (size_t)length >= sizeof(parent))
        return false;
    char *slash = strrchr(parent, '/');
    if (slash)
        *slash = '\0';
    else
        parent[0] = '\0';
    const char *glob =
        path_basename(directory)[0] == '.' ? NULL : node_fact(map, "glob", parent, NULL);
    if (owner && glob && strcmp(owner, glob))
        owner = ambiguous_pkgmap_value;
    else if (!owner)
        owner = glob;
    if (!owner || !strcmp(owner, ambiguous_pkgmap_value) ||
        node_fact(map, "workspace-incomplete", owner, NULL))
        return true;
    char key[PKGMAP_PATH_BUF];
    if (!node_fact_key(key, sizeof(key), "bound", owner, name))
        return false;
    const char *previous = cbm_ht_get(map, key);
    if (previous && !strcmp(previous, directory))
        return true;
    char *value =
        cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, previous ? ambiguous_pkgmap_value : directory);
    const char *stored =
        previous ? cbm_ht_get_key(map, key) : cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, key);
    if (!value || !stored) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, value);
        if (!previous)
            cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)stored);
        return false;
    }
    cbm_ht_set(map, stored, value);
    if (cbm_ht_get(map, stored) != value) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, value);
        if (!previous)
            cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)stored);
        return false;
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)previous);
    return true;
}

/* Exclusions are observed without reading their contents. A terminal
 * wildcard requires the complete immediate directory set, and every explicit
 * member requires a collected manifest scope. */
static bool node_block_workspace(CBMHashTable *map, const char *owner) {
    if (!owner || !strcmp(owner, ambiguous_pkgmap_value) ||
        node_fact(map, "workspace-incomplete", owner, NULL))
        return true;
    char key[PKGMAP_PATH_BUF];
    if (!node_fact_key(key, sizeof(key), "workspace-incomplete", owner, NULL))
        return false;
    char *owned_key = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, key),
         *value = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, "1");
    if (owned_key && value)
        cbm_ht_set(map, owned_key, value);
    if (!owned_key || !value || cbm_ht_get(map, owned_key) != value) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, owned_key);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, value);
        return false;
    }
    return true;
}

static bool node_fact_directory(const char *key, const char *kind, char *out, size_t capacity) {
    char prefix[64];
    int n = snprintf(prefix, sizeof(prefix), "@cbm:node:%s:", kind);
    if (n < 0 || (size_t)n >= sizeof(prefix) || strncmp(key, prefix, (size_t)n))
        return false;
    const char *start = key + n, *end = strchr(start, '\n');
    if (!end || (size_t)(end - start) >= capacity)
        return false;
    memcpy(out, start, (size_t)(end - start));
    out[end - start] = '\0';
    return true;
}

static void node_parent_path(char *path) {
    char *slash = strrchr(path, '/');
    if (slash)
        *slash = '\0';
    else
        path[0] = '\0';
}

static bool node_glob_covered(CBMHashTable *map, const char *directory) {
    if (!node_fact(map, "directory", directory, NULL) ||
        node_fact(map, "excluded-child", directory, NULL))
        return false;
    char parent[PKGMAP_PATH_BUF];
    int n = snprintf(parent, sizeof(parent), "%s", directory);
    if (n < 0 || (size_t)n >= sizeof(parent))
        return false;
    for (;;) {
        if (node_fact(map, "excluded", parent, NULL))
            return false;
        if (!parent[0])
            return true;
        node_parent_path(parent);
    }
}

static bool node_workspace_coverage(CBMHashTable *map, cbm_pkg_entries_t *workers, int count) {
    for (int w = 0; w < count; w++)
        for (int i = 0; i < workers[w].count; i++) {
            const cbm_pkg_entry_t *entry = &workers[w].items[i];
            if (!entry->node_fact)
                continue;
            char directory[PKGMAP_PATH_BUF];
            const char *owner = NULL;
            bool incomplete = false;
            if (node_fact_directory(entry->pkg_name, "workspace", directory, sizeof(directory))) {
                owner = node_fact(map, "workspace", directory, NULL);
                const char *scope = node_fact(map, "scope", directory, NULL);
                incomplete = !scope || !scope[0] || !strcmp(scope, ambiguous_pkgmap_value);
            } else if (node_fact_directory(entry->pkg_name, "glob", directory, sizeof(directory))) {
                owner = node_fact(map, "glob", directory, NULL);
                incomplete = !node_glob_covered(map, directory);
            } else if (node_fact_directory(entry->pkg_name, "directory", directory,
                                           sizeof(directory)) &&
                       directory[0]) {
                const char *leaf = path_basename(directory);
                if (leaf[0] == '.')
                    continue; /* A bare terminal star does not match dot folders. */
                const char *scope = node_fact(map, "scope", directory, NULL);
                node_parent_path(directory);
                owner = node_fact(map, "glob", directory, NULL);
                incomplete = !scope || !scope[0] || !strcmp(scope, ambiguous_pkgmap_value);
            }
            if (incomplete && !node_block_workspace(map, owner))
                return false;
        }
    return true;
}

/* One bounded lookup per manifest; no file/reference × package scan. */
static bool node_workspaces_finalize(CBMHashTable *map, cbm_pkg_entries_t *workers, int count) {
    static const char prefix[] = "@cbm:node:scope:";
    if (!node_workspace_coverage(map, workers, count))
        return false;
    for (int w = 0; w < count; w++)
        for (int i = 0; i < workers[w].count; i++) {
            const char *key = workers[w].items[i].pkg_name;
            if (!workers[w].items[i].node_fact || strncmp(key, prefix, sizeof(prefix) - 1))
                continue;
            const char *start = key + sizeof(prefix) - 1;
            const char *end = strchr(start, '\n');
            char directory[PKGMAP_PATH_BUF];
            if (!end || (size_t)(end - start) >= sizeof(directory))
                return false;
            memcpy(directory, start, (size_t)(end - start));
            directory[end - start] = '\0';
            const char *name = node_fact(map, "scope", directory, NULL);
            if (!node_bind_workspace(map, directory, name))
                return false;
        }
    return true;
}

CBMHashTable *cbm_pkgmap_build(cbm_pkg_entries_t *worker_entries, int worker_count,
                               const char *project_name) {
    /* Count total entries */
    int total = 0;
    for (int w = 0; w < worker_count; w++) {
        if (worker_entries[w].incomplete || worker_entries[w].count < 0 ||
            worker_entries[w].count > INT32_MAX - total)
            return NULL;
        total += worker_entries[w].count;
    }
    if (total == 0) {
        return NULL;
    }

    CBMHashTable *map = cbm_ht_create(PKGMAP_HT_INIT);
    if (!map)
        return NULL;
    int merged = 0;

    for (int w = 0; w < worker_count; w++) {
        cbm_pkg_entries_t *we = &worker_entries[w];
        for (int i = 0; i < we->count; i++) {
            /* Convert entry_rel to QN: project.dir.parts. A PSR-4 namespace
             * prefix (key ends in '\\') keeps its repo-relative DIRECTORY
             * instead: its readers map `Prefix\Sub\Class` onto a file path
             * (<dir>/Sub/Class.php), which a dotted QN cannot express (#1186). */
            const char *entry_rel = we->items[i].entry_rel;
            bool private_key = !strncmp(we->items[i].pkg_name, "@cbm:node:", 10);
            if (private_key != we->items[i].node_fact) {
                cbm_pkgmap_free(map);
                return NULL;
            }
            char *qn = we->items[i].node_fact || !strcmp(entry_rel, ambiguous_pkgmap_value) ||
                               pkgmap_is_psr4_prefix(we->items[i].pkg_name)
                           ? pkgmap_strndup(entry_rel, strlen(entry_rel))
                           : pkgmap_take_raw(cbm_pipeline_fqn_module(project_name, entry_rel));
            if (!qn) {
                cbm_pkgmap_free(map);
                return NULL;
            }

            /* Repeated extraction of the same manifest is harmless. Distinct
             * roots for one package are ambiguous without modeled build scope;
             * merge order cannot select a binding. Keep a blocking entry. */
            if (cbm_ht_has(map, we->items[i].pkg_name)) {
                const char *key = cbm_ht_get_key(map, we->items[i].pkg_name);
                char *previous = cbm_ht_get(map, key);
                if (previous && strcmp(previous, qn) != 0 &&
                    strcmp(previous, ambiguous_pkgmap_value) != 0) {
                    char *blocked = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, ambiguous_pkgmap_value);
                    if (!blocked) {
                        cbm_free(CBM_MEM_CLASS_SEMANTIC, qn);
                        cbm_pkgmap_free(map);
                        return NULL;
                    }
                    cbm_ht_set(map, key, blocked);
                    if (cbm_ht_get(map, key) != blocked) {
                        cbm_free(CBM_MEM_CLASS_SEMANTIC, blocked);
                        cbm_free(CBM_MEM_CLASS_SEMANTIC, qn);
                        cbm_pkgmap_free(map);
                        return NULL;
                    }
                    cbm_free(CBM_MEM_CLASS_SEMANTIC, previous);
                }
                cbm_free(CBM_MEM_CLASS_SEMANTIC, qn);
                continue;
            }

            /* Transfer ownership: key = strdup'd pkg_name, value = qn */
            char *key = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, we->items[i].pkg_name);
            if (key)
                cbm_ht_set(map, key, qn);
            if (!key || cbm_ht_get(map, key) != qn) {
                cbm_free(CBM_MEM_CLASS_SEMANTIC, key);
                cbm_free(CBM_MEM_CLASS_SEMANTIC, qn);
                cbm_pkgmap_free(map);
                return NULL;
            }
            merged++;
        }
    }

    if (!node_workspaces_finalize(map, worker_entries, worker_count)) {
        cbm_pkgmap_free(map);
        return NULL;
    }
    if (merged == 0) {
        cbm_ht_free(map);
        return NULL;
    }
    if (cbm_ht_has(map, "@cbm:node:complete")) {
        cbm_pkgmap_free(map);
        return NULL;
    }
    char *seal = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, "@cbm:node:complete"),
         *complete = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, "1");
    if (seal && complete)
        cbm_ht_set(map, seal, complete);
    if (!seal || !complete || cbm_ht_get(map, seal) != complete) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, seal);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, complete);
        cbm_pkgmap_free(map);
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

static void node_excluded_path(cbm_pkg_entries_t *entries, const char *path) {
    node_collect(entries, "excluded", path, NULL, "1");
    if (path_basename(path)[0] == '.')
        return;
    char parent[PKGMAP_PATH_BUF];
    int n = snprintf(parent, sizeof(parent), "%s", path);
    if (n < 0 || (size_t)n >= sizeof(parent)) {
        entries->incomplete = true;
        return;
    }
    node_parent_path(parent);
    node_collect(entries, "excluded-child", parent, NULL, "1");
}

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
 * API (same as src/discover/discover.c), with explicit metadata and
 * enumeration status. Symlinks/reparse points are not traversed; their
 * boundaries withhold affected workspace declarations. The depth
 * bound prevents cycles and excessive traversal from becoming unbounded. */
static int pkgmap_walk_dir(const char *abs_dir, const char *rel_dir, cbm_pkg_entries_t *entries,
                           int depth, char **excluded_dirs, int excluded_count) {
    if (depth >= PKGMAP_WALK_MAX_DEPTH) {
        entries->incomplete = true;
        cbm_log_info("pkgmap.walk", "depth_cap", rel_dir && rel_dir[0] ? rel_dir : ".");
        return 0;
    }
    cbm_dir_t *dir = cbm_opendir(abs_dir);
    if (!dir) {
        entries->incomplete = true;
        return 0;
    }
    node_collect(entries, "directory", rel_dir ? rel_dir : "", NULL, "1");
    int parsed = 0;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(dir)) != NULL) {
        const char *name = entry->name;
        if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'))) {
            continue;
        }
        char abs_path[PKGMAP_PATH_BUF];
        char rel_path[PKGMAP_PATH_BUF];
        int absolute_length = snprintf(abs_path, sizeof(abs_path), "%s/%s", abs_dir, name);
        int relative_length = snprintf(rel_path, sizeof(rel_path), "%s%s%s", rel_dir ? rel_dir : "",
                                       rel_dir && rel_dir[0] ? "/" : "", name);
        if (absolute_length < 0 || (size_t)absolute_length >= sizeof(abs_path) ||
            relative_length < 0 || (size_t)relative_length >= sizeof(rel_path)) {
            entries->incomplete = true;
            continue;
        }
        /* These exclusions define the indexed project domain. Uninspectable
         * entries inside that domain must not disappear as absent manifests. */
        if (cbm_should_skip_dir(name, CBM_MODE_FULL) ||
            cbm_pipeline_relpath_is_excluded(rel_path, excluded_dirs, excluded_count)) {
            node_excluded_path(entries, rel_path);
            if (is_pkgmap_manifest_basename(name))
                entries->incomplete = true;
            continue;
        }
        cbm_path_info_t info = {0};
        if (cbm_path_info_utf8(abs_path, &info) != CBM_PATH_INFO_OK) {
            entries->incomplete = true;
            continue;
        }
        if (info.is_symlink) {
            node_excluded_path(entries, rel_path);
            if (is_pkgmap_manifest_basename(name))
                entries->incomplete = true;
            continue;
        }
        if (info.is_directory) {
            if (is_pkgmap_manifest_basename(name)) {
                entries->incomplete = true;
                continue;
            }
            parsed += pkgmap_walk_dir(abs_path, rel_path, entries, depth + 1, excluded_dirs,
                                      excluded_count);
            continue;
        }
        if (!is_pkgmap_manifest_basename(name))
            continue;
        if (!info.is_regular) {
            entries->incomplete = true;
            continue;
        }
        int source_len = 0;
        char *source = pkgmap_read_file(abs_path, &source_len);
        if (!source) {
            entries->incomplete = true;
            continue;
        }
        if (cbm_pkgmap_try_parse(name, rel_path, source, source_len, entries)) {
            parsed++;
        }
        cbm_free(CBM_MEM_CLASS_EXTRACT, source);
    }
    if (cbm_dir_failed(dir))
        entries->incomplete = true;
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
 * which is depth-bounded (PKGMAP_WALK_MAX_DEPTH) and rejects incomplete
 * coverage, including symlinks / Windows reparse points inside the domain.
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
            entries.incomplete = true;
            continue;
        }
        cbm_pkgmap_try_parse(basename, files[i].rel_path, source, source_len, &entries);
        cbm_free(CBM_MEM_CLASS_EXTRACT, source);
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
            entries.incomplete = true;
            continue;
        }
        cbm_pkgmap_try_parse(basename, files[i].rel_path, source, source_len, &entries);
        cbm_free(CBM_MEM_CLASS_EXTRACT, source);
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
    cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)key);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, value);
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
    char *buf = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, module_path);
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
        if (strcmp(base_qn, ambiguous_pkgmap_value) == 0) {
            cbm_free(CBM_MEM_CLASS_SEMANTIC, buf);
            return strdup("");
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
        cbm_free(CBM_MEM_CLASS_SEMANTIC, buf);
        return strdup(result);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, buf);
    return NULL;
}

/* Try dot-based prefix matching (Java: com.myorg.pkg.Foo).
 * Returns heap QN or NULL. */
static char *resolve_dot_prefix(CBMHashTable *map, const char *module_path,
                                const char *project_name) {
    char *buf = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, module_path);
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
        if (strcmp(base_qn, ambiguous_pkgmap_value) == 0) {
            cbm_free(CBM_MEM_CLASS_SEMANTIC, buf);
            return strdup("");
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
        cbm_free(CBM_MEM_CLASS_SEMANTIC, buf);
        return cbm_pipeline_fqn_module(project_name, result);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, buf);
    return NULL;
}

/* Try backslash-based prefix matching (PHP PSR-4: App\\Controllers\\Foo).
 * The map value of a PSR-4 prefix is its repo-relative directory (see
 * cbm_pkgmap_build). Returns heap QN or NULL. */
static char *resolve_backslash_prefix(CBMHashTable *map, const char *module_path,
                                      const char *project_name) {
    char *buf = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, module_path);
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
        if (strcmp(base_dir, ambiguous_pkgmap_value) == 0) {
            cbm_free(CBM_MEM_CLASS_SEMANTIC, buf);
            return strdup("");
        }
        const char *subpath = module_path + (size_t)(bs - buf) + SKIP_ONE;
        char path_result[PKGMAP_PATH_BUF];
        snprintf(path_result, sizeof(path_result), "%s%s%s", base_dir, base_dir[0] ? "/" : "",
                 subpath);
        for (char *c = path_result; *c; c++) {
            if (*c == '\\') {
                *c = '/';
            }
        }
        cbm_free(CBM_MEM_CLASS_SEMANTIC, buf);
        return cbm_pipeline_fqn_module(project_name, path_result);
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, buf);
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
        /* The relative resolver has already removed an explicit JS/TS file
         * extension.  Treat the remaining path as a module path verbatim so a
         * dotted extensionless basename such as `featureX.engine` is not
         * stripped a second time by cbm_pipeline_fqn_module. */
        char *qn = cbm_pipeline_fqn_folder(ctx->project_name, resolved);
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
        return strdup(strcmp(mapped_qn, ambiguous_pkgmap_value) == 0 ? "" : mapped_qn);
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

/* #2127: a Python import path spells the module chain that owns the imported
 * name (`from unittest.mock import patch` -> unittest, mock). Strategy 3 matches
 * the leaf name alone, so an EXTERNAL import (stdlib / third party: no project
 * module, Strategy 1 misses) bound to any same-named project definition -- a
 * REST view's `patch` handler became the target of every mock.patch call at
 * import_map confidence. For such an external import, accept a symbol hit only
 * when the module segments preceding `name` in the import path occur, in
 * order, among the hit's enclosing QN segments -- a src/ layout or sys.path
 * root the module resolver missed (`from template_tests.utils import setup` ->
 * proj.tests.template_tests.utils.setup) still qualifies. Relative prefixes
 * (leading dots) carry no segment and are skipped. Imports of project modules
 * are never judged by it (they may re-export from anywhere). Python only. */
enum { PY_IMPORT_MAX_SEGS = 64 };
bool cbm_python_import_path_matches_qn(const char *module_path, const char *name,
                                       const char *hit_qn) {
    if (!module_path || !name || !hit_qn) {
        return true;
    }
    char path[1024];
    snprintf(path, sizeof(path), "%s", module_path);
    char *as = strstr(path, " as ");
    if (as) {
        *as = '\0';
    }
    const char *p = path;
    while (*p == '.') {
        p++;
    }
    /* Segments of the import path; name_at = index of the LAST `name`. */
    const char *segs[PY_IMPORT_MAX_SEGS];
    size_t lens[PY_IMPORT_MAX_SEGS];
    int nseg = 0;
    int name_at = -1;
    size_t name_len = strlen(name);
    while (*p && nseg < PY_IMPORT_MAX_SEGS) {
        const char *dot = strchr(p, '.');
        size_t len = dot ? (size_t)(dot - p) : strlen(p);
        if (len == name_len && strncmp(p, name, len) == 0) {
            name_at = nseg;
        }
        segs[nseg] = p;
        lens[nseg] = len;
        nseg++;
        if (!dot) {
            break;
        }
        p = dot + SKIP_ONE;
    }
    if (name_at <= 0) {
        return true; /* no module chain before the name: nothing to check */
    }
    /* Ordered-subsequence match against the hit's enclosing segments (every
     * segment before the hit's own leaf). */
    const char *leaf = strrchr(hit_qn, '.');
    const char *q = hit_qn;
    int want = 0;
    while (want < name_at && leaf && q < leaf) {
        const char *dot = strchr(q, '.');
        size_t len = (size_t)(dot - q);
        if (len == lens[want] && strncmp(q, segs[want], len) == 0) {
            want++;
        }
        q = dot + SKIP_ONE;
    }
    return want == name_at;
}

/* #2127, call side: an imported name binds the file's identifier to that
 * import for the whole module, exactly like a parameter binds it for a body.
 * When the import could not be materialized (external module) the resolver
 * falls through to project-wide short-name strategies and binds `patch(...)`
 * (or `mock.patch(...)`) to whatever project definition shares the leaf. True
 * when the callee's root identifier is bound by this file's Python imports and
 * EVERY such binding's module chain (`unittest.mock` for `from unittest.mock
 * import patch`; the callee's own dotted path for a root `import a.b`) is
 * absent from `resolved_qn`'s enclosing segments AND the binding is external
 * (no IMPORTS edge materialized it: the module is not in the project). An
 * internal binding is never judged here -- a project module can re-export a
 * name from anywhere -- and a chain consistent with the target (src/ layout)
 * keeps the edge. `gbuf` NULL means "no IMPORTS edges" (all external). */
static bool python_import_local_materialized(const cbm_gbuf_t *gbuf, const char *project_name,
                                             const char *rel_path, const char *local_name) {
    if (!gbuf || !rel_path) {
        return false;
    }
    char *file_qn = cbm_pipeline_fqn_compute(project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *file_node = file_qn ? cbm_gbuf_find_by_qn(gbuf, file_qn) : NULL;
    safe_free(file_qn);
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (!file_node || cbm_gbuf_find_edges_by_source_type(gbuf, file_node->id, "IMPORTS", &edges,
                                                         &edge_count) != 0) {
        return false;
    }
    char needle[CBM_SZ_256];
    snprintf(needle, sizeof(needle), "\"local_name\":\"%s\"", local_name);
    for (int i = 0; i < edge_count; i++) {
        if (edges[i]->properties_json && strstr(edges[i]->properties_json, needle)) {
            return true;
        }
    }
    return false;
}

bool cbm_python_import_binding_contradicts(const CBMImportArray *imports, const char *callee_name,
                                           const char *resolved_qn, const cbm_gbuf_t *gbuf,
                                           const char *project_name, const char *rel_path) {
    if (!imports || imports->count <= 0 || !callee_name || !callee_name[0] || !resolved_qn ||
        !resolved_qn[0]) {
        return false;
    }
    const char *root_end = strchr(callee_name, '.');
    size_t root_len = root_end ? (size_t)(root_end - callee_name) : strlen(callee_name);
    bool bound = false;
    for (int i = 0; i < imports->count; i++) {
        const CBMImport *imp = &imports->items[i];
        if (!imp->local_name || !imp->module_path || strlen(imp->local_name) != root_len ||
            strncmp(imp->local_name, callee_name, root_len) != 0) {
            continue;
        }
        char full[1024];
        snprintf(full, sizeof(full), "%s", imp->module_path);
        char *as = strstr(full, " as ");
        if (as) {
            *as = '\0';
        }
        const char *path_leaf_seg = strrchr(full, '.');
        bool from_import = path_leaf_seg && strcmp(path_leaf_seg + SKIP_ONE, imp->local_name) == 0;
        if (!as && !from_import && strncmp(full, callee_name, root_len) == 0 &&
            (full[root_len] == '.' || full[root_len] == '\0')) {
            /* `import a.b` binds the ROOT package `a`: the callee already
             * spells its own full dotted path (`a.b.f()`). */
            snprintf(full, sizeof(full), "%s", callee_name);
        } else if (!as && !path_leaf_seg && full[0] != '.' && strcmp(full, imp->local_name) != 0) {
            /* `import m as y` (the extractor stores module_path `m`, local
             * `y`; a from-import always spells a dotted path): `y` IS module
             * `m`, so `y.T` spells `m.T`. */
            snprintf(full, sizeof(full), "%s%s", imp->module_path, callee_name + root_len);
        }
        /* Otherwise (`from m import x [as y]`) only the import's own module
         * chain is evidence: members reached THROUGH x (`x.objects.create`)
         * may live on any type, so they are not compared. */
        const char *leaf = strrchr(full, '.');
        leaf = leaf ? leaf + SKIP_ONE : full;
        if (cbm_python_import_path_matches_qn(full, leaf, resolved_qn) ||
            python_import_local_materialized(gbuf, project_name, rel_path, imp->local_name)) {
            return false; /* consistent with the target, or a project import */
        }
        bound = true;
    }
    return bound;
}

/* Base-class twin of the #2127 call gate. `class SimpleTestCase(
 * unittest.TestCase)` under `import unittest` names the STDLIB base; the
 * registry's same-module suffix fallback (`unittest.TestCase` -> module's
 * `TestCase`) or a short-name strategy bound it to a same-named PROJECT class
 * (django: an inheritance cycle SimpleTestCase -> TestCase ->
 * TransactionTestCase -> SimpleTestCase, plus fabricated OVERRIDEs). An
 * external import binding (never materialized as an IMPORTS edge) whose
 * module chain the target does not spell can never be that target, whatever
 * the registry strategy. Exact metadata import maps remain valid when the
 * target spells the imported module chain. Every base resolver (sequential
 * + parallel semantic, cross-LSP pxc) calls this ONE
 * gate so the venues cannot diverge. Python only. */
bool cbm_python_external_base_contradicts(CBMLanguage lang, const CBMImportArray *imports,
                                          const char *base_spelling, const char *base_qn,
                                          const cbm_gbuf_t *gbuf, const char *project_name,
                                          const char *rel_path) {
    return lang == CBM_LANG_PYTHON &&
           cbm_python_import_binding_contradicts(imports, base_spelling, base_qn, gbuf,
                                                 project_name, rel_path);
}

/* Symbol spelling alone does not establish an import binding in any language. */
bool cbm_import_symbol_fallback_allowed(CBMLanguage lang) {
    (void)lang;
    return false;
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

static bool node_in_php_file(const cbm_gbuf_node_t *n) {
    return n && n->file_path && cbm_language_for_filename(path_leaf(n->file_path)) == CBM_LANG_PHP;
}

static bool import_language_matches(CBMLanguage lang, const cbm_gbuf_node_t *node) {
    if (!node || !node->file_path)
        return false;
    CBMLanguage target = cbm_language_for_filename(path_leaf(node->file_path));
    if (lang == target)
        return true;
    bool source_es = lang == CBM_LANG_JAVASCRIPT || lang == CBM_LANG_TYPESCRIPT ||
                     lang == CBM_LANG_TSX || lang == CBM_LANG_ARKTS || lang == CBM_LANG_ASTRO ||
                     lang == CBM_LANG_VUE || lang == CBM_LANG_SVELTE || lang == CBM_LANG_HTML;
    bool target_es = target == CBM_LANG_JAVASCRIPT || target == CBM_LANG_TYPESCRIPT ||
                     target == CBM_LANG_TSX || target == CBM_LANG_ARKTS;
    bool source_jvm = lang == CBM_LANG_JAVA || lang == CBM_LANG_KOTLIN || lang == CBM_LANG_SCALA;
    bool target_jvm =
        target == CBM_LANG_JAVA || target == CBM_LANG_KOTLIN || target == CBM_LANG_SCALA;
    return (source_es && target_es) || (source_jvm && target_jvm);
}

/* A quoted C include searches the importing file's directory first. The
 * extractor preserves its delimiter; system includes and macro expressions
 * have no modeled search-root order and must remain unresolved. */
static bool import_fact_put(CBMHashTable *map, const char *key, const char *value) {
    const char *old = cbm_ht_get(map, key);
    if (old && strcmp(old, value) == 0)
        return true;
    char *copy = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, old ? ambiguous_pkgmap_value : value);
    const char *stored_key =
        old ? cbm_ht_get_key(map, key) : cbm_mem_strdup(CBM_MEM_CLASS_OTHER, key);
    if (!copy || !stored_key) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, copy);
        if (!old)
            cbm_free(CBM_MEM_CLASS_OTHER, (void *)stored_key);
        return false;
    }
    cbm_ht_set(map, stored_key, copy);
    if (cbm_ht_get(map, key) != copy) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, copy);
        if (!old)
            cbm_free(CBM_MEM_CLASS_OTHER, (void *)stored_key);
        return false;
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)old);
    return true;
}

static bool import_absolute_path(const char *directory, const char *path, char *out,
                                 size_t capacity) {
    bool absolute = path && (path[0] == '/' || (isalpha((unsigned char)path[0]) && path[1] == ':'));
    int length = snprintf(out, capacity, "%s%s%s", absolute ? "" : directory, absolute ? "" : "/",
                          path ? path : "");
    return path && length > 0 && (size_t)length < capacity;
}

/* A found but unindexed/out-of-project file also stops the compiler's search.
 * This prevents a later project namesake from stealing an external include. */
static const cbm_gbuf_node_t *include_in_directory(const cbm_pipeline_ctx_t *ctx,
                                                   const char *directory, const char *name,
                                                   bool *stop) {
    char candidate[PKGMAP_PATH_BUF], canonical[CBM_SZ_4K], root[CBM_SZ_4K];
    if (!import_absolute_path(directory, name, candidate, sizeof(candidate))) {
        *stop = true;
        return NULL;
    }
    struct stat st;
    if (stat(candidate, &st) != 0) {
        *stop = errno != ENOENT && errno != ENOTDIR;
        return NULL;
    }
    *stop = true;
    if (!S_ISREG(st.st_mode) || !cbm_canonical_path(candidate, canonical, sizeof(canonical)) ||
        !cbm_canonical_path(ctx->repo_path, root, sizeof(root)))
        return NULL;
    cbm_normalize_path_sep(canonical);
    cbm_normalize_path_sep(root);
    size_t size = strlen(root);
    if (strncmp(canonical, root, size) != 0 || canonical[size] != '/')
        return NULL;
    return cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, canonical + size + 1);
}

static const cbm_gbuf_node_t *resolve_configured_include(const cbm_pipeline_ctx_t *ctx,
                                                         const char *source_rel,
                                                         const CBMImport *imp, CBMHashTable *map) {
    if (!source_rel || (imp->kind != CBM_IMPORT_KIND_INCLUDE_QUOTED &&
                        imp->kind != CBM_IMPORT_KIND_INCLUDE_SYSTEM))
        return NULL;
    bool stop = false;
    if (imp->kind == CBM_IMPORT_KIND_INCLUDE_QUOTED) {
        char *dir = path_dirname(source_rel);
        char relative[PKGMAP_PATH_BUF];
        const cbm_gbuf_node_t *exact =
            dir && cbm_repo_rel_join(dir, imp->module_path, relative, sizeof(relative))
                ? cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, relative)
                : NULL;
        if (exact) {
            cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
            return exact;
        }
        char absolute[PKGMAP_PATH_BUF];
        bool valid = dir && ctx->repo_path &&
                     import_absolute_path(ctx->repo_path, dir, absolute, sizeof(absolute));
        cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
        if (!valid)
            return NULL;
        const cbm_gbuf_node_t *local = include_in_directory(ctx, absolute, imp->module_path, &stop);
        if (stop)
            return local;
    }
    char key[PKGMAP_PATH_BUF];
    int length = snprintf(key, sizeof(key), "@cbm:cc:%s", source_rel);
    const char *roots =
        map && length > 0 && (size_t)length < sizeof(key) ? cbm_ht_get(map, key) : NULL;
    if (!roots || strcmp(roots, ambiguous_pkgmap_value) == 0)
        return NULL;
    for (const char *p = roots; p && p[0];) {
        const char *end = strchr(p, '\n');
        size_t size = end ? (size_t)(end - p) : strlen(p);
        if (size < 2 || size >= sizeof(key))
            return NULL;
        if (p[0] != 'Q' || imp->kind == CBM_IMPORT_KIND_INCLUDE_QUOTED) {
            memcpy(key, p + 1, size - 1);
            key[size - 1] = '\0';
            const cbm_gbuf_node_t *target = include_in_directory(ctx, key, imp->module_path, &stop);
            if (stop)
                return target;
        }
        p = end ? end + 1 : NULL;
    }
    return NULL;
}

typedef struct {
    char *start;
    size_t length;
    char *canonical;
    bool drop;
} compile_include_root_t;

static int compile_root_compare(const void *left, const void *right) {
    const compile_include_root_t *const *a = left;
    const compile_include_root_t *const *b = right;
    return strcmp((*a)->canonical, (*b)->canonical);
}

static bool explicit_relative_path(const char *p) {
    return strncmp(p, "./", PAIR_LEN) == 0 || strncmp(p, "../", PAIR_LEN + SKIP_ONE) == 0;
}

/* A directory supplied as both -I and -isystem remains in the system group.
 * Compare canonical directory identities, including spellings containing ..,
 * without sorting the search order itself. The auxiliary sort keeps a command
 * containing many include roots linear-logarithmic rather than pairwise. */
static bool compile_drop_user_system_duplicates(char *roots) {
    size_t count = 0;
    for (const char *p = roots; *p; p++)
        count += *p == '\n';
    if (count < 2)
        return true;
    /* A root contains pointers, so this also bounds the smaller pointer index. */
    if (count > SIZE_MAX / sizeof(compile_include_root_t))
        return false;
    compile_include_root_t *entries = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, count * sizeof(*entries));
    compile_include_root_t **sorted = cbm_calloc(CBM_MEM_CLASS_SEMANTIC, count * sizeof(*sorted));
    if (!entries || !sorted) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, entries);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, sorted);
        return false;
    }
    bool valid = true;
    char *cursor = roots;
    for (size_t i = 0; i < count; i++) {
        char *end = strchr(cursor, '\n');
        size_t length = end ? (size_t)(end - cursor) : 0;
        char path[CBM_SZ_4K], canonical[CBM_SZ_4K];
        if (length < 2 || length > sizeof(path)) {
            valid = false;
            break;
        }
        memcpy(path, cursor + 1, length - 1);
        path[length - 1] = '\0';
        bool resolved = cbm_canonical_path(path, canonical, sizeof(canonical)) != 0;
        if (!resolved) {
            struct stat st;
            /* An existing but uncanonicalizable root cannot support identity
             * comparison. Missing directories cannot contain an include. */
            if (stat(path, &st) == 0 || (errno != ENOENT && errno != ENOTDIR)) {
                valid = false;
                break;
            }
        }
        entries[i] = (compile_include_root_t){
            .start = cursor,
            .length = length + 1,
            .canonical = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, resolved ? canonical : path)};
        if (!entries[i].canonical) {
            valid = false;
            break;
        }
        cbm_normalize_path_sep(entries[i].canonical);
        sorted[i] = &entries[i];
        cursor = end + 1;
    }
    if (valid) {
        qsort(sorted, count, sizeof(*sorted), compile_root_compare);
        for (size_t first = 0; first < count;) {
            size_t end = first + 1;
            while (end < count && strcmp(sorted[first]->canonical, sorted[end]->canonical) == 0)
                end++;
            bool system = false;
            for (size_t i = first; i < end; i++)
                system = system || sorted[i]->start[0] == 'S';
            if (system)
                for (size_t i = first; i < end; i++)
                    sorted[i]->drop = sorted[i]->start[0] == 'I';
            first = end;
        }
        char *write = roots;
        for (size_t i = 0; i < count; i++) {
            if (entries[i].drop)
                continue;
            memmove(write, entries[i].start, entries[i].length);
            write += entries[i].length;
        }
        *write = '\0';
    }
    for (size_t i = 0; i < count; i++)
        cbm_free(CBM_MEM_CLASS_SEMANTIC, entries[i].canonical);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, entries);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, sorted);
    return valid;
}

/* Preserve compiler groups (-iquote, -I, -isystem, -idirafter) and order
 * within each group. Unknown response/configuration flags prevent a proof. */
static char *compile_include_roots(yyjson_val *entry, const char *directory) {
    char *owned[CBM_SZ_1K] = {0};
    const char *args[CBM_SZ_1K];
    int count = 0;
    yyjson_val *arguments = yyjson_obj_get(entry, "arguments");
    const char *command = yyjson_get_str(yyjson_obj_get(entry, "command"));
    if (yyjson_is_arr(arguments)) {
        if (yyjson_arr_size(arguments) >= CBM_SZ_1K)
            return NULL;
        size_t i, max;
        yyjson_val *value;
        yyjson_arr_foreach(arguments, i, max, value) {
            const char *arg = yyjson_get_str(value);
            if (!arg)
                return NULL;
            args[count++] = arg;
        }
    } else if (command) {
        count = cbm_split_command(command, owned, CBM_SZ_1K);
        for (int i = 0; i < count; i++)
            args[i] = owned[i];
    }
    size_t capacity = count >= 0 && (size_t)count < SIZE_MAX / PKGMAP_PATH_BUF
                          ? ((size_t)count + 1) * PKGMAP_PATH_BUF
                          : 0;
    char *roots = capacity ? cbm_calloc(CBM_MEM_CLASS_SEMANTIC, capacity) : NULL;
    bool valid = roots && count > 0 && count < CBM_SZ_1K;
    size_t used = 0;
    static const char *const flags[] = {"-iquote", "-I", "-isystem", "-idirafter"};
    static const char groups[] = {'Q', 'I', 'S', 'A'};
    for (int group = 0; valid && group < 4; group++) {
        for (int i = 1; i < count; i++) {
            const char *arg = args[i];
            if (arg[0] == '@' || strcmp(arg, "-I-") == 0 || strncmp(arg, "--sysroot", 9) == 0 ||
                strncmp(arg, "-isysroot", 9) == 0 || strncmp(arg, "-iprefix", 8) == 0 ||
                strncmp(arg, "-iwithprefix", 12) == 0 || strncmp(arg, "/I", 2) == 0) {
                valid = false;
                break;
            }
            size_t prefix = strlen(flags[group]);
            if (strncmp(arg, flags[group], prefix) != 0)
                continue;
            const char *path = arg + prefix;
            if (!path[0])
                path = ++i < count ? args[i] : NULL;
            char absolute[PKGMAP_PATH_BUF];
            if (!path || path[0] == '=' || strchr(path, '\n') ||
                !import_absolute_path(directory, path, absolute, sizeof(absolute))) {
                valid = false;
                break;
            }
            int length = snprintf(roots + used, capacity - used, "%c%s\n", groups[group], absolute);
            if (length < 0 || (size_t)length >= capacity - used) {
                valid = false;
                break;
            }
            used += (size_t)length;
        }
    }
    for (int i = 0; i < count; i++)
        free(owned[i]);
    if (valid)
        valid = compile_drop_user_system_duplicates(roots);
    if (!valid) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, roots);
        return NULL;
    }
    return roots;
}

static bool build_compile_import_context(const cbm_pipeline_ctx_t *ctx, CBMHashTable *map) {
    char config[PKGMAP_PATH_BUF];
    if (!import_absolute_path(ctx->repo_path, "compile_commands.json", config, sizeof(config)))
        return false;
    int size = 0;
    char *text = pkgmap_read_file(config, &size);
    if (!text)
        return true;
    yyjson_doc *doc = yyjson_read(text, (size_t)size, 0);
    cbm_free(CBM_MEM_CLASS_EXTRACT, text);
    if (!doc)
        return false;
    yyjson_val *entries = yyjson_doc_get_root(doc);
    bool complete = yyjson_is_arr(entries);
    size_t i, max;
    yyjson_val *entry;
    yyjson_arr_foreach(entries, i, max, entry) {
        const char *directory = yyjson_get_str(yyjson_obj_get(entry, "directory"));
        const char *file = yyjson_get_str(yyjson_obj_get(entry, "file"));
        char absolute[PKGMAP_PATH_BUF], canonical[CBM_SZ_4K], root[CBM_SZ_4K];
        if (!directory || !file ||
            !import_absolute_path(directory, file, absolute, sizeof(absolute)) ||
            !cbm_canonical_path(absolute, canonical, sizeof(canonical)) ||
            !cbm_canonical_path(ctx->repo_path, root, sizeof(root)))
            continue;
        cbm_normalize_path_sep(canonical);
        cbm_normalize_path_sep(root);
        size_t root_size = strlen(root);
        if (strncmp(canonical, root, root_size) != 0 || canonical[root_size] != '/')
            continue;
        char key[PKGMAP_PATH_BUF];
        int length = snprintf(key, sizeof(key), "@cbm:cc:%s", canonical + root_size + 1);
        char *roots = compile_include_roots(entry, directory);
        if (length <= 0 || (size_t)length >= sizeof(key) ||
            !import_fact_put(map, key, roots ? roots : ambiguous_pkgmap_value))
            complete = false;
        cbm_free(CBM_MEM_CLASS_SEMANTIC, roots);
    }
    yyjson_doc_free(doc);
    return complete;
}

typedef struct {
    const char *rel;
    int cache_index;
    CBMFileResult *result;
    bool loaded;
    const char *source;
    char *owned_source;
    TSTree *tree;
    bool owned_tree;
} rust_import_file_t;

typedef struct {
    const cbm_pipeline_ctx_t *ctx;
    CBMHashTable *map;
    rust_import_file_t *files;
    int count;
    TSParser *parser;
} rust_import_context_t;

static int rust_file_compare(const void *left, const void *right) {
    return strcmp(((const rust_import_file_t *)left)->rel,
                  ((const rust_import_file_t *)right)->rel);
}

static rust_import_file_t *rust_import_file(rust_import_context_t *context, const char *rel) {
    rust_import_file_t key = {.rel = rel};
    return bsearch(&key, context->files, (size_t)context->count, sizeof(context->files[0]),
                   rust_file_compare);
}

static bool rust_node_text(TSNode node, const char *source, char *out, size_t capacity) {
    if (ts_node_is_null(node))
        return false;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    if (end < start || end - start >= capacity)
        return false;
    memcpy(out, source + start, end - start);
    out[end - start] = '\0';
    return true;
}

static bool rust_context_fact(rust_import_context_t *context, const char *kind, const char *root,
                              const char *path, const char *value) {
    char key[PKGMAP_PATH_BUF];
    int size = snprintf(key, sizeof(key), "@cbm:rust:%s:%s:%s", kind, root, path);
    return size > 0 && (size_t)size < sizeof(key) && import_fact_put(context->map, key, value);
}

static const char *rust_import_fact(CBMHashTable *map, const char *kind, const char *root,
                                    const char *path) {
    char key[PKGMAP_PATH_BUF];
    int size = snprintf(key, sizeof(key), "@cbm:rust:%s:%s:%s", kind, root, path);
    return map && size > 0 && (size_t)size < sizeof(key) ? cbm_ht_get(map, key) : NULL;
}

static bool rust_source_tree(rust_import_context_t *context, rust_import_file_t *file,
                             CBMFileResult **cache) {
    if (file->tree)
        return true;
    file->result =
        cbm_pipeline_result_acquire(context->ctx, cache, file->cache_index, NULL, &file->loaded);
    if (!file->result || file->result->parse_unusable || file->result->walk_truncated)
        return false;
    file->source = file->result->source;
    int length = file->result->source_len;
    if (!file->source) {
        char absolute[PKGMAP_PATH_BUF];
        if (!import_absolute_path(context->ctx->repo_path, file->rel, absolute, sizeof(absolute)))
            return false;
        file->owned_source = pkgmap_read_file(absolute, &length);
        file->source = file->owned_source;
    }
    if (!file->source || length < 0)
        return false;
    file->tree = file->result->cached_tree;
    if (!file->tree) {
        file->tree =
            cbm_parse_source(context->parser, file->source, (uint32_t)length, (TSParseOptions){0});
        file->owned_tree = file->tree != NULL;
    }
    return file->tree && !ts_node_has_error(ts_tree_root_node(file->tree));
}

/* Only the active module ancestry may retain a spilled extraction result.
 * Module facts own their strings, so completed siblings need no parse/source
 * lifetime after their declaration walk. Unspilled cache entries stay owned
 * by the pipeline; this helper releases only the copies acquired here. */
static void rust_release_source(rust_import_file_t *file) {
    if (file->owned_tree)
        ts_tree_delete(file->tree);
    cbm_free(CBM_MEM_CLASS_EXTRACT, file->owned_source);
    cbm_pipeline_result_release(file->result, file->loaded);
    file->result = NULL;
    file->loaded = false;
    file->source = NULL;
    file->owned_source = NULL;
    file->tree = NULL;
    file->owned_tree = false;
}

/* Private module/items are visible to their parent module and its descendants.
 * pub(crate) is crate-wide; restricted pub(in ...) needs a richer scope fact.
 */
static char rust_item_visibility(TSNode node, const char *source) {
    char visibility = 'L';
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (strcmp(ts_node_type(child), "visibility_modifier") != 0)
                continue;
            char text[CBM_SZ_256];
            visibility = rust_node_text(child, source, text, sizeof(text)) &&
                                 (strcmp(text, "pub") == 0 || strcmp(text, "pub(crate)") == 0)
                             ? 'P'
                             : '?';
            break;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    return visibility;
}

static bool rust_walk_module(rust_import_context_t *context, CBMFileResult **cache,
                             rust_import_file_t *file, TSNode body, const char *root,
                             const char *logical, const char *directory, unsigned depth) {
    if (depth > PKGMAP_WALK_MAX_DEPTH)
        return false;
    if (!rust_context_fact(context, "module", root, logical, file->rel))
        return false;
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return true;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (!ts_node_is_named(node))
            continue;
        const char *kind = ts_node_type(node);
        /* Macro expansion and attributes can replace or introduce names.
         * Until their configured expansion is available this module tree is
         * not a complete import domain, even if one spelling looks unique. */
        if (strcmp(kind, "attribute_item") == 0 || strcmp(kind, "inner_attribute_item") == 0 ||
            strcmp(kind, "macro_invocation") == 0 || strcmp(kind, "macro_definition") == 0 ||
            strcmp(kind, "expression_statement") == 0)
            goto failed;
        if (strcmp(kind, "line_comment") == 0 || strcmp(kind, "block_comment") == 0)
            continue;
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        char name[CBM_SZ_256], path[PKGMAP_PATH_BUF];
        bool eligible = rust_node_text(name_node, file->source, name, sizeof(name));
        bool is_module = strcmp(kind, "mod_item") == 0;
        if (!eligible)
            continue;
        int size = snprintf(path, sizeof(path), "%s%s%s", logical, logical[0] ? "::" : "", name);
        if (size <= 0 || (size_t)size >= sizeof(path))
            goto failed;
        char visibility[2] = {rust_item_visibility(node, file->source), '\0'};
        if (!rust_context_fact(context, is_module ? "visibility" : "item", root, path, visibility))
            goto failed;
        if (!is_module)
            continue;
        TSNode inline_body = ts_node_child_by_field_name(node, "body", 4);
        char child_dir[PKGMAP_PATH_BUF];
        if (!cbm_repo_rel_join(directory, name, child_dir, sizeof(child_dir)))
            goto failed;
        if (!ts_node_is_null(inline_body)) {
            /* Extracted imports currently omit their lexical module scope.
             * A nested self/super import could otherwise bind in this file's
             * outer module. Withhold the domain until scoped occurrences are
             * available, even when its target spelling looks exact. */
            goto failed;
        }
        char flat[PKGMAP_PATH_BUF], nested[PKGMAP_PATH_BUF];
        int flat_size = snprintf(flat, sizeof(flat), "%s.rs", child_dir);
        int nested_size = snprintf(nested, sizeof(nested), "%s/mod.rs", child_dir);
        if (flat_size <= 0 || (size_t)flat_size >= sizeof(flat) || nested_size <= 0 ||
            (size_t)nested_size >= sizeof(nested))
            goto failed;
        rust_import_file_t *first = rust_import_file(context, flat);
        rust_import_file_t *second = rust_import_file(context, nested);
        if (first && second) {
            if (!rust_context_fact(context, "module", root, path, ambiguous_pkgmap_value))
                goto failed;
            continue;
        }
        rust_import_file_t *child = first ? first : second;
        if (!child)
            continue;
        if (!rust_source_tree(context, child, cache)) {
            rust_release_source(child);
            continue;
        }
        char membership[PKGMAP_PATH_BUF], key[PKGMAP_PATH_BUF];
        size = snprintf(membership, sizeof(membership), "%s\n%s", root, path);
        int key_size = snprintf(key, sizeof(key), "@cbm:rust:file:%s", child->rel);
        bool complete = size > 0 && (size_t)size < sizeof(membership) && key_size > 0 &&
                        (size_t)key_size < sizeof(key) &&
                        import_fact_put(context->map, key, membership) &&
                        rust_walk_module(context, cache, child, ts_tree_root_node(child->tree),
                                         root, path, child_dir, depth + 1);
        rust_release_source(child);
        if (!complete)
            goto failed;
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return true;

failed:
    ts_tree_cursor_delete(&cursor);
    return false;
}

static bool rust_add_root(rust_import_context_t *context, CBMFileResult **cache, const char *rel) {
    rust_import_file_t *file = rust_import_file(context, rel);
    if (!file)
        return true;
    if (!rust_source_tree(context, file, cache)) {
        rust_release_source(file);
        return true; /* No ready marker: a parse failure never supplies complete
                        evidence. */
    }
    char key[PKGMAP_PATH_BUF], membership[PKGMAP_PATH_BUF];
    int size = snprintf(key, sizeof(key), "@cbm:rust:file:%s", rel);
    int value_size = snprintf(membership, sizeof(membership), "%s\n", rel);
    char *directory = path_dirname(rel);
    bool complete = size > 0 && (size_t)size < sizeof(key) && value_size > 0 &&
                    (size_t)value_size < sizeof(membership) && directory &&
                    import_fact_put(context->map, key, membership) &&
                    rust_walk_module(context, cache, file, ts_tree_root_node(file->tree), rel, "",
                                     directory, 0);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, directory);
    rust_release_source(file);
    if (!complete)
        return false;
    return rust_context_fact(context, "ready", rel, "", "1");
}

/* Cargo target paths are facts separate from module filenames. Custom lib/bin
 * roots use the same declaration walk as the default targets. */
static bool rust_cargo_roots(rust_import_context_t *context, CBMFileResult **cache,
                             const char *manifest) {
    char absolute[PKGMAP_PATH_BUF];
    if (!import_absolute_path(context->ctx->repo_path, manifest, absolute, sizeof(absolute)))
        return false;
    int length = 0;
    char *source = pkgmap_read_file(absolute, &length);
    if (!source)
        return true;
    char *directory = path_dirname(manifest);
    if (!directory) {
        cbm_free(CBM_MEM_CLASS_EXTRACT, source);
        return false;
    }
    bool package = false, custom_lib = false, success = true;
    enum { CARGO_OTHER, CARGO_PACKAGE, CARGO_LIB, CARGO_BIN } section = CARGO_OTHER;
    for (char *line = source; line && line[0];) {
        char *next = strchr(line, '\n');
        if (next)
            *next++ = '\0';
        while (isspace((unsigned char)*line))
            line++;
        if (*line == '[') {
            section = strncmp(line, "[package]", 9) == 0 ? CARGO_PACKAGE
                      : strncmp(line, "[lib]", 5) == 0   ? CARGO_LIB
                      : strncmp(line, "[[bin]]", 7) == 0 ? CARGO_BIN
                                                         : CARGO_OTHER;
            package = package || section == CARGO_PACKAGE;
        } else if ((section == CARGO_LIB || section == CARGO_BIN) &&
                   strncmp(line, "path", 4) == 0 &&
                   (isspace((unsigned char)line[4]) || line[4] == '=')) {
            char *value = extract_quoted(line + 4, line + strlen(line));
            char path[PKGMAP_PATH_BUF];
            if (!value || !cbm_repo_rel_join(directory, value, path, sizeof(path)))
                success = false;
            else
                success = rust_add_root(context, cache, path) && success;
            custom_lib = custom_lib || section == CARGO_LIB;
            cbm_free(CBM_MEM_CLASS_SEMANTIC, value);
        }
        line = next;
    }
    if (package) {
        const char *defaults[] = {"src/lib.rs", "src/main.rs"};
        for (int i = custom_lib ? 1 : 0; i < 2; i++) {
            char path[PKGMAP_PATH_BUF];
            if (!cbm_repo_rel_join(directory, defaults[i], path, sizeof(path)) ||
                !rust_add_root(context, cache, path))
                success = false;
        }
    }
    cbm_free(CBM_MEM_CLASS_SEMANTIC, directory);
    cbm_free(CBM_MEM_CLASS_EXTRACT, source);
    return success;
}

static bool import_context_build(const cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                 int count, CBMFileResult **cache, CBMHashTable *map) {
    if (!ctx || !map || !build_compile_import_context(ctx, map))
        return false;
    rust_import_context_t context = {.ctx = ctx, .map = map};
    for (int i = 0; i < count; i++)
        if (files[i].language == CBM_LANG_RUST)
            context.count++;
    if (!context.count)
        return true;
    if ((size_t)context.count > SIZE_MAX / sizeof(context.files[0]))
        return false;
    context.files =
        cbm_calloc(CBM_MEM_CLASS_SEMANTIC, (size_t)context.count * sizeof(context.files[0]));
    context.parser = ts_parser_new();
    if (!context.files || !context.parser ||
        !ts_parser_set_language(context.parser, cbm_ts_language(CBM_LANG_RUST))) {
        cbm_free(CBM_MEM_CLASS_SEMANTIC, context.files);
        ts_parser_delete(context.parser);
        return false;
    }
    int index = 0;
    for (int i = 0; i < count; i++)
        if (files[i].language == CBM_LANG_RUST)
            context.files[index++] =
                (rust_import_file_t){.rel = files[i].rel_path, .cache_index = i};
    qsort(context.files, (size_t)context.count, sizeof(context.files[0]), rust_file_compare);
    bool success = true, cargo_found = false;
    for (int i = 0; i < count; i++) {
        if (strcmp(path_leaf(files[i].rel_path), "Cargo.toml") == 0) {
            cargo_found = true;
            success = rust_cargo_roots(&context, cache, files[i].rel_path) && success;
        }
    }
    /* A standalone Rust entry file is an explicit rustc root. Its siblings
     * become members only through actual mod declarations below that root. */
    if (!cargo_found)
        for (int i = 0; i < context.count; i++) {
            const char *leaf = path_leaf(context.files[i].rel);
            if (strcmp(leaf, "main.rs") == 0 || strcmp(leaf, "lib.rs") == 0)
                success = rust_add_root(&context, cache, context.files[i].rel) && success;
        }
    for (int i = 0; i < context.count; i++) {
        rust_release_source(&context.files[i]);
    }
    ts_parser_delete(context.parser);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, context.files);
    return success;
}

/* The context holds Rust and compile-database facts only. Its failure withholds
 * them from those languages (create_import_edges), and says nothing about any
 * other language's namespaces: one crate without a src/main.rs must not void
 * every Java import of the project. */
bool cbm_pipeline_import_context_build(const cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                       int count, CBMFileResult **cache, CBMHashTable *map) {
    if (import_context_build(ctx, files, count, cache, map))
        return true;
    if (map)
        (void)import_fact_put(map, import_context_failed, "1");
    return false;
}

static bool rust_visible_from(const char *visibility, const char *path, const char *source_module) {
    if (!visibility || (visibility[0] != 'P' && visibility[0] != 'L'))
        return false;
    if (visibility[0] == 'P')
        return true;
    const char *last = NULL;
    for (const char *p = path; (p = strstr(p, "::")) != NULL; p += 2)
        last = p;
    size_t parent = last ? (size_t)(last - path) : 0;
    return parent == 0 || (strncmp(source_module, path, parent) == 0 &&
                           (source_module[parent] == '\0' || source_module[parent] == ':'));
}

/* Only a physical file's own module maps its direct declarations to the
 * file symbol scope. Inline modules need their nested declaration identity;
 * a same-file top-level namesake is never a substitute. */
static bool rust_physical_module_matches(CBMHashTable *map, const char *root, const char *file,
                                         const char *logical) {
    char key[PKGMAP_PATH_BUF];
    int length = snprintf(key, sizeof(key), "@cbm:rust:file:%s", file);
    const char *membership =
        map && length > 0 && (size_t)length < sizeof(key) ? cbm_ht_get(map, key) : NULL;
    size_t root_length = strlen(root);
    return membership && !strncmp(membership, root, root_length) &&
           membership[root_length] == '\n' && !strcmp(membership + root_length + 1, logical);
}

static const cbm_gbuf_node_t *rust_use_target(const cbm_pipeline_ctx_t *ctx, CBMHashTable *map,
                                              const char *root, const char *source_module,
                                              char *path, bool *ambiguous,
                                              const cbm_gbuf_node_t **binding) {
    if (binding)
        *binding = NULL;
    char prefix[PKGMAP_PATH_BUF] = "";
    char *cursor = path;
    const char *file = rust_import_fact(map, "module", root, "");
    while (cursor && cursor[0]) {
        char *next = strstr(cursor, "::");
        if (next)
            *next = '\0';
        if (strcmp(cursor, "*") == 0 && !next)
            break;
        char full[PKGMAP_PATH_BUF];
        int size = snprintf(full, sizeof(full), "%s%s%s", prefix, prefix[0] ? "::" : "", cursor);
        if (size <= 0 || (size_t)size >= sizeof(full))
            return NULL;
        const char *module = rust_import_fact(map, "module", root, full);
        if (module) {
            if (strcmp(module, ambiguous_pkgmap_value) == 0) {
                *ambiguous = true;
                return NULL;
            }
            if (!rust_visible_from(rust_import_fact(map, "visibility", root, full), full,
                                   source_module))
                return NULL;
            file = module;
            snprintf(prefix, sizeof(prefix), "%s", full);
        } else {
            if (next ||
                !rust_visible_from(rust_import_fact(map, "item", root, full), full, source_module))
                return NULL;
            if (binding && file && rust_physical_module_matches(map, root, file, prefix)) {
                char *qn = cbm_pipeline_fqn_compute(ctx->project_name, file, cursor);
                const cbm_gbuf_node_t *member = qn ? cbm_gbuf_find_by_qn(ctx->gbuf, qn) : NULL;
                if (member && member->file_path && !strcmp(member->file_path, file) &&
                    member->label && strcmp(member->label, "File") &&
                    strcmp(member->label, "Module"))
                    *binding = member;
                free(qn);
            }
        }
        cursor = next ? next + 2 : NULL;
    }
    return file ? cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, file) : NULL;
}

static const cbm_gbuf_node_t *resolve_rust_import(const cbm_pipeline_ctx_t *ctx,
                                                  const char *source_rel, const CBMImport *imp,
                                                  CBMHashTable *map, bool *ambiguous,
                                                  const cbm_gbuf_node_t **binding) {
    if (binding)
        *binding = NULL;
    char key[PKGMAP_PATH_BUF], membership[PKGMAP_PATH_BUF];
    int size = snprintf(key, sizeof(key), "@cbm:rust:file:%s", source_rel);
    const char *context =
        map && size > 0 && (size_t)size < sizeof(key) ? cbm_ht_get(map, key) : NULL;
    if (!context || strcmp(context, ambiguous_pkgmap_value) == 0 ||
        strlen(context) >= sizeof(membership))
        return NULL;
    snprintf(membership, sizeof(membership), "%s", context);
    char *source_module = strchr(membership, '\n');
    if (!source_module)
        return NULL;
    *source_module++ = '\0';
    const char *root = membership;
    if (!rust_import_fact(map, "ready", root, ""))
        return NULL;
    const char *raw = imp->module_path;
    char base[PKGMAP_PATH_BUF] = "";
    if (imp->kind == CBM_IMPORT_KIND_MODULE_DECLARATION) {
        char module[PKGMAP_PATH_BUF];
        size = snprintf(module, sizeof(module), "%s%s%s", source_module,
                        source_module[0] ? "::" : "", raw);
        if (size <= 0 || (size_t)size >= sizeof(module))
            return NULL;
        const char *file = rust_import_fact(map, "module", root, module);
        if (file && strcmp(file, ambiguous_pkgmap_value) == 0) {
            *ambiguous = true;
            return NULL;
        }
        return file ? cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, file) : NULL;
    }
    if (strncmp(raw, "crate::", 7) == 0)
        raw += 7;
    else if (strncmp(raw, "self::", 6) == 0) {
        raw += 6;
        snprintf(base, sizeof(base), "%s", source_module);
    } else if (strncmp(raw, "super::", 7) == 0) {
        snprintf(base, sizeof(base), "%s", source_module);
        while (strncmp(raw, "super::", 7) == 0) {
            if (!base[0])
                return NULL;
            char *last = NULL;
            for (char *p = base; (p = strstr(p, "::")) != NULL; p += 2)
                last = p;
            if (last)
                *last = '\0';
            else
                base[0] = '\0';
            raw += 7;
        }
    } else {
        /* Unqualified paths can name extern crates or lexical use aliases. */
        return NULL;
    }
    char path[PKGMAP_PATH_BUF];
    size = snprintf(path, sizeof(path), "%s%s%s", base, base[0] ? "::" : "", raw);
    if (size <= 0 || (size_t)size >= sizeof(path))
        return NULL;
    char *group = strchr(path, '{');
    if (!group) {
        char *alias = strstr(path, " as ");
        if (alias)
            *alias = '\0';
        return rust_use_target(ctx, map, root, source_module, path, ambiguous, binding);
    }
    char *end = strchr(group, '}');
    if (!end || end[1] || strchr(group + 1, '{'))
        return NULL;
    *group++ = '\0';
    *end = '\0';
    const cbm_gbuf_node_t *target = NULL;
    for (char *part = group; part;) {
        char *next = strchr(part, ',');
        if (next)
            *next++ = '\0';
        while (isspace((unsigned char)*part))
            part++;
        size_t length = strlen(part);
        while (length && isspace((unsigned char)part[length - 1]))
            part[--length] = '\0';
        char *alias = strstr(part, " as ");
        if (alias)
            *alias = '\0';
        char member[PKGMAP_PATH_BUF];
        size = snprintf(member, sizeof(member), "%s%s", path, part);
        if (size <= 0 || (size_t)size >= sizeof(member))
            return NULL;
        const cbm_gbuf_node_t *hit =
            rust_use_target(ctx, map, root, source_module, member, ambiguous, NULL);
        if (!hit || (target && target->id != hit->id))
            return NULL;
        target = hit;
        part = next;
    }
    return target;
}

/* These adapters accept only audited literal carriers. Their conventions
 * never authorize generic ./ or ../ paths from another syntax or language. */
static bool meson_child_path(const char *path) {
    for (const char *part = path; *part;) {
        const char *end = strchr(part, '/');
        size_t length = end ? (size_t)(end - part) : strlen(part);
        if (length == 2 && part[0] == '.' && part[1] == '.')
            return false;
        if (!end)
            break;
        part = end + 1;
    }
    return true;
}

static int scss_literal_candidates(const char *base, char paths[6][PKGMAP_PATH_BUF]) {
    const char *leaf = path_leaf(base), *dot = strrchr(leaf, '.');
    const char *extensions[] = {".scss", ".sass", ".css"};
    if (dot && strcmp(dot, ".scss") && strcmp(dot, ".sass") && strcmp(dot, ".css"))
        return 0;
    int count = 0;
    for (size_t i = 0; i < (dot ? 1 : 3); i++) {
        const char *extension = dot ? "" : extensions[i];
        int length = snprintf(paths[count], PKGMAP_PATH_BUF, "%s%s", base, extension);
        if (length <= 0 || length >= PKGMAP_PATH_BUF)
            return 0;
        count++;
        /* Sass tests the added underscore even for an already-prefixed URL. */
        length = snprintf(paths[count], PKGMAP_PATH_BUF, "%.*s_%s%s", (int)(leaf - base), base,
                          leaf, extension);
        if (length <= 0 || length >= PKGMAP_PATH_BUF)
            return 0;
        count++;
    }
    return count;
}

static const cbm_gbuf_node_t *resolve_sibling_file(const cbm_pipeline_ctx_t *ctx,
                                                   const char *source_rel, const char *module_path,
                                                   CBMLanguage lang, bool *ambiguous) {
    if (!source_rel || !module_path || !module_path[0] || module_path[0] == '~')
        return NULL;
    if (lang == CBM_LANG_MESON && !meson_child_path(module_path))
        return NULL;
    char *dir = path_dirname(source_rel);
    char base[PKGMAP_PATH_BUF];
    bool valid = dir && cbm_repo_rel_join(dir, module_path, base, sizeof(base));
    cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
    if (!valid)
        return NULL;
    char paths[6][PKGMAP_PATH_BUF];
    int count = 1;
    if (lang == CBM_LANG_SCSS) {
        count = scss_literal_candidates(base, paths);
    } else {
        int length = snprintf(paths[0], sizeof(paths[0]), "%s%s", base,
                              lang == CBM_LANG_MESON ? "/meson.build" : "");
        if (length <= 0 || (size_t)length >= sizeof(paths[0]))
            return NULL;
    }
    const cbm_gbuf_node_t *found = NULL;
    for (int i = 0; i < count; i++) {
        if (lang == CBM_LANG_MESON && !strcmp(paths[i], source_rel))
            return NULL;
        const cbm_gbuf_node_t *node =
            cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, paths[i]);
        if (node && found && node->id != found->id) {
            *ambiguous = true;
            return NULL;
        }
        if (node)
            found = node;
    }
    return found;
}

/* The File an explicit relative import (`./x`, `../x`) names exactly, for
 * files whose File QN differs from the module QN Strategy 1 tries
 * (`../styles/global.css`, `../layouts/Layout.astro`, #2558). Resolved in
 * stack buffers by cbm_repo_rel_join, which refuses a path above the
 * repository root. */
static const cbm_gbuf_node_t *resolve_relative_file(const cbm_pipeline_ctx_t *ctx,
                                                    const char *source_rel, const char *module_path,
                                                    bool php_only) {
    if (!source_rel || !module_path || !explicit_relative_path(module_path)) {
        return NULL;
    }
    char dir[PKGMAP_PATH_BUF];
    int dir_len = snprintf(dir, sizeof(dir), "%s", source_rel);
    if (dir_len < 0 || (size_t)dir_len >= sizeof(dir)) {
        return NULL;
    }
    char *slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
    } else {
        dir[0] = '\0';
    }
    char path[PKGMAP_PATH_BUF];
    if (!cbm_repo_rel_join(dir, module_path, path, sizeof(path))) {
        return NULL;
    }
    const cbm_gbuf_node_t *file = cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, path);
    return file && (!php_only || node_in_php_file(file)) ? file : NULL;
}

/* ── PHP PSR-4 class imports (#1186) ──────────────────────────────── */

typedef enum {
    PSR4_NOT_APPLICABLE = 0, /* no composer psr-4 prefix covers the import */
    PSR4_RESOLVED,           /* the class file exists: *out is its File node */
    PSR4_UNRESOLVED,         /* a prefix covers it but no class file exists */
} psr4_outcome_t;

/* The File node at exactly `rel_path`, or NULL. Looked up through the name
 * index (File nodes are named by basename) and matched on the full path, so
 * a same-named file in another directory never qualifies. */
static const cbm_gbuf_node_t *psr4_file_node(const cbm_gbuf_t *gbuf, const char *rel_path) {
    const cbm_gbuf_node_t **hits = NULL;
    int hit_count = 0;
    if (cbm_gbuf_find_by_name(gbuf, path_leaf(rel_path), &hits, &hit_count) != 0 || !hits) {
        return NULL;
    }
    for (int i = 0; i < hit_count; i++) {
        const cbm_gbuf_node_t *n = hits[i];
        if (n && n->label && strcmp(n->label, "File") == 0 && n->file_path &&
            strcmp(n->file_path, rel_path) == 0) {
            return n;
        }
    }
    return NULL;
}

/* Resolve a PHP class import `use A\B\C;` the way composer's PSR-4 autoloader
 * does: for every autoload.psr-4 prefix covering the name, longest first, the
 * class lives in exactly <prefix-dir>/<rest>.php with sub-namespaces as
 * subdirectories; the first existing file wins. When a prefix covers the name
 * but no such file exists the import is UNRESOLVED: it must never fall through
 * to the namespace bucket, which binds it to whichever file of the namespace
 * came first. `use function` / `use const` name namespace members, not class
 * files, so they are not applicable. */
static psr4_outcome_t resolve_php_psr4_class(const cbm_gbuf_t *gbuf, const char *source_file_qn,
                                             const CBMImport *imp, const cbm_gbuf_node_t **out) {
    *out = NULL;
    CBMHashTable *pkgmap = cbm_pipeline_get_pkgmap();
    if (!pkgmap || imp->kind != CBM_IMPORT_KIND_DEFAULT) {
        return PSR4_NOT_APPLICABLE;
    }
    const char *name = imp->module_path;
    if (name[0] == '\\') {
        name++; /* fully qualified `use \App\X;` */
    }
    char key[PKGMAP_PATH_BUF];
    size_t name_len = strlen(name);
    if (name_len == 0 || name_len >= sizeof(key) || !strchr(name, '\\')) {
        return PSR4_NOT_APPLICABLE;
    }
    bool covered = false;
    /* Right to left: each '\' ends a candidate prefix, longest first. */
    for (size_t cut = name_len - SKIP_ONE; cut > 0; cut--) {
        if (name[cut - SKIP_ONE] != '\\') {
            continue;
        }
        memcpy(key, name, cut);
        key[cut] = '\0';
        const char *dir = (const char *)cbm_ht_get(pkgmap, key);
        if (!dir) {
            continue;
        }
        covered = true;
        if (strcmp(dir, ambiguous_pkgmap_value) == 0)
            return PSR4_UNRESOLVED;
        char rel[PKGMAP_PATH_BUF];
        int w = snprintf(rel, sizeof(rel), "%s%s%s.php", dir, dir[0] ? "/" : "", name + cut);
        if (w <= 0 || (size_t)w >= sizeof(rel)) {
            continue;
        }
        for (char *c = rel; *c; c++) {
            if (*c == '\\') {
                *c = '/';
            }
        }
        const cbm_gbuf_node_t *file = psr4_file_node(gbuf, rel);
        if (file && (!source_file_qn || !file->qualified_name ||
                     strcmp(file->qualified_name, source_file_qn) != 0)) {
            *out = file;
            return PSR4_RESOLVED;
        }
    }
    return covered ? PSR4_UNRESOLVED : PSR4_NOT_APPLICABLE;
}

static bool psr4_class_label(const char *label) {
    return label && (strcmp(label, "Class") == 0 || strcmp(label, "Interface") == 0 ||
                     strcmp(label, "Trait") == 0 || strcmp(label, "Enum") == 0);
}

/* The QN of `member` on the class `cls` defined in `file_path`, or NULL when
 * that file defines no such class or the class has no such member. */
static const char *psr4_member_in_file(const cbm_gbuf_t *gbuf, const char *file_path,
                                       const char *cls, const char *member) {
    const cbm_gbuf_node_t **hits = NULL;
    int hit_count = 0;
    if (cbm_gbuf_find_by_name(gbuf, cls, &hits, &hit_count) != 0 || !hits) {
        return NULL;
    }
    for (int i = 0; i < hit_count; i++) {
        const cbm_gbuf_node_t *c = hits[i];
        if (!c || !psr4_class_label(c->label) || !c->qualified_name || !c->file_path ||
            strcmp(c->file_path, file_path) != 0) {
            continue;
        }
        char qn[CBM_SZ_1K];
        int w = snprintf(qn, sizeof(qn), "%s.%s", c->qualified_name, member);
        if (w <= 0 || (size_t)w >= sizeof(qn)) {
            return NULL;
        }
        const cbm_gbuf_node_t *m = cbm_gbuf_find_by_qn(gbuf, qn);
        return m ? m->qualified_name : NULL;
    }
    return NULL;
}

cbm_psr4_member_t cbm_pipeline_psr4_member_qn(const cbm_gbuf_t *gbuf, const char *class_fqn,
                                              const char *member, const char **out_qn) {
    *out_qn = NULL;
    if (!gbuf || !class_fqn || !member || !member[0]) {
        return CBM_PSR4_MEMBER_NOT_COVERED;
    }
    const CBMImport imp = {.module_path = class_fqn, .kind = CBM_IMPORT_KIND_DEFAULT};
    const cbm_gbuf_node_t *file = NULL;
    psr4_outcome_t placed = resolve_php_psr4_class(gbuf, NULL, &imp, &file);
    if (placed == PSR4_NOT_APPLICABLE) {
        return CBM_PSR4_MEMBER_NOT_COVERED;
    }
    /* Covered from here on: composer would load this file or none, so a
     * missing file, class or member leaves the member unresolved. */
    if (placed != PSR4_RESOLVED || !file->file_path) {
        return CBM_PSR4_MEMBER_UNRESOLVED;
    }
    const char *cls = strrchr(class_fqn, '\\');
    cls = cls ? cls + SKIP_ONE : class_fqn;
    *out_qn = psr4_member_in_file(gbuf, file->file_path, cls, member);
    return *out_qn ? CBM_PSR4_MEMBER_RESOLVED : CBM_PSR4_MEMBER_UNRESOLVED;
}

/* A kustomization entry naming a remote base -- a URL, a go-getter host path,
 * an ssh remote or a `?ref=` pin -- lies outside the repository. */
static bool kustomize_remote_entry(const char *entry) {
    static const char *const hosts[] = {"github.com/", "gitlab.com/", "bitbucket.org/", "git@",
                                        NULL};
    if (strstr(entry, "://") || strstr(entry, "?ref=") || strstr(entry, ".git/")) {
        return true;
    }
    for (int i = 0; hosts[i]; i++) {
        if (strncmp(entry, hosts[i], strlen(hosts[i])) == 0) {
            return true;
        }
    }
    return false;
}

static bool kustomize_file_label(const char *label) {
    return label && (strcmp(label, "Module") == 0 || strcmp(label, "File") == 0);
}

/* The Folder of a directory entry: plain, or `proj.pkg.base/` when a file
 * beside it shares the stem (cbm_stem_marks_t). */
static const cbm_gbuf_node_t *kustomize_folder(const cbm_pipeline_ctx_t *ctx, const char *path) {
    char *fqn = cbm_pipeline_fqn_folder(ctx->project_name, path);
    if (!fqn) {
        return NULL;
    }
    const cbm_gbuf_node_t *n = cbm_gbuf_find_by_qn(ctx->gbuf, fqn);
    if (!n || !n->label || strcmp(n->label, "Folder") != 0) {
        char marked[PKGMAP_PATH_BUF];
        int len = snprintf(marked, sizeof(marked), "%s/", fqn);
        n = len > 0 && (size_t)len < sizeof(marked) ? cbm_gbuf_find_by_qn(ctx->gbuf, marked) : NULL;
    }
    free(fqn);
    return n && n->label && strcmp(n->label, "Folder") == 0 ? n : NULL;
}

/* A kustomization entry is a path relative to the kustomization's own
 * directory -- a manifest file or a directory -- or a remote base; it never
 * names a symbol. Resolve it exactly or not at all: the name-guess strategies
 * bound a remote base's URL segment `deploy` to a shell function, component
 * paths to Go structs, and an entry's basename to a same-named file beside
 * the importer. */
static const cbm_gbuf_node_t *resolve_kustomize_entry(const cbm_pipeline_ctx_t *ctx,
                                                      const char *source_rel, const char *entry) {
    if (!entry[0] || kustomize_remote_entry(entry)) {
        return NULL;
    }
    /* "x.yaml" and "./x.yaml" name the same file; make every entry relative. */
    bool dotted = entry[0] == '.' && (entry[1] == '/' || (entry[1] == '.' && entry[2] == '/'));
    char rel[PKGMAP_PATH_BUF];
    snprintf(rel, sizeof(rel), "%s%s", dotted ? "" : "./", entry);
    char *path = cbm_pipeline_resolve_relative_import(source_rel, rel);
    if (!path) {
        return NULL;
    }
    const cbm_gbuf_node_t *found = NULL;
    char *qn = cbm_pipeline_fqn_module(ctx->project_name, path);
    const cbm_gbuf_node_t *n = qn ? cbm_gbuf_find_by_qn(ctx->gbuf, qn) : NULL;
    free(qn);
    /* The Module must be the entry's own file: a directory entry `base`
     * beside `base.yaml` shares its module QN but names the directory. */
    if (n && kustomize_file_label(n->label) && n->file_path && strcmp(n->file_path, path) == 0) {
        found = n;
    } else {
        found = kustomize_folder(ctx, path);
    }
    free(path);
    return found;
}

/* A configured package/path alias can justify bare-specifier resolution.
 * The no-map default conversion is only a Python module-root convention; it
 * cannot turn an npm/vendor dependency into a same-named repository file. */
static bool import_has_configured_root(const cbm_pipeline_ctx_t *ctx, const char *source_rel,
                                       const char *path) {
    CBMHashTable *map = cbm_pipeline_get_pkgmap();
    if (map) {
        char prefix[PKGMAP_PATH_BUF];
        size_t length = strlen(path);
        if (length < sizeof(prefix)) {
            memcpy(prefix, path, length + 1);
            if (cbm_ht_get(map, prefix))
                return true;
            for (size_t i = length; i > 0; i--) {
                if (path[i - 1] != '/' && path[i - 1] != '.' && path[i - 1] != '\\')
                    continue;
                prefix[i - 1] = '\0';
                if (cbm_ht_get(map, prefix))
                    return true;
            }
        }
    }
    if (ctx->path_aliases && source_rel) {
        const cbm_path_alias_map_t *aliases =
            cbm_path_alias_find_for_file(ctx->path_aliases, source_rel);
        char *resolved = aliases ? cbm_path_alias_resolve(aliases, path) : NULL;
        bool found = resolved != NULL;
        free(resolved);
        return found;
    }
    return false;
}

/* Materialize a complete member path within one declared namespace's file.
 * Physical file names and namespace names are separate: try only the exact
 * representations emitted by the language's definition extractor. */
static const cbm_gbuf_node_t *namespace_member(const cbm_pipeline_ctx_t *ctx,
                                               const cbm_gbuf_node_t *file, const char *space,
                                               const char *member) {
    if (!file->file_path || !member || !member[0])
        return NULL;
    CBMLanguage lang = cbm_language_for_filename(path_leaf(file->file_path));
    char *dir = lang == CBM_LANG_JAVA ? path_dirname(file->file_path) : NULL;
    char *module = lang == CBM_LANG_JAVA
                       ? (dir ? cbm_pipeline_fqn_folder(ctx->project_name, dir) : NULL)
                       : cbm_pipeline_fqn_module(ctx->project_name, file->file_path);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, dir);
    if (!module)
        return NULL;
    char qn[PKGMAP_PATH_BUF];
    int length = snprintf(qn, sizeof(qn), "%s.%s", module, member);
    const cbm_gbuf_node_t *hit =
        length > 0 && (size_t)length < sizeof(qn) ? cbm_gbuf_find_by_qn(ctx->gbuf, qn) : NULL;
    if (!hit && space && space[0]) {
        length = snprintf(qn, sizeof(qn), "%s.%s.%s", module, space, member);
        hit = length > 0 && (size_t)length < sizeof(qn) ? cbm_gbuf_find_by_qn(ctx->gbuf, qn) : NULL;
    }
    free(module);
    return hit && hit->file_path && strcmp(hit->file_path, file->file_path) == 0 && hit->label &&
                   strcmp(hit->label, "File") != 0 && strcmp(hit->label, "Module") != 0
               ? hit
               : NULL;
}

/* Reverse keys do not collide with language namespaces. They keep member
 * lookup bounded by that member's name bucket instead of files × namespace. */
static const char *namespace_for_file(CBMHashTable *map, const char *rel) {
    char key[PKGMAP_PATH_BUF];
    int length = snprintf(key, sizeof(key), "@cbm:file:%s", rel);
    return length > 0 && (size_t)length < sizeof(key) ? cbm_ht_get(map, key) : NULL;
}

static const cbm_gbuf_node_t *namespace_member_target(const cbm_pipeline_ctx_t *ctx,
                                                      CBMHashTable *map, const char *space,
                                                      const char *member, CBMLanguage lang,
                                                      bool *ambiguous) {
    const char *leaf = strrchr(member, '.');
    leaf = leaf ? leaf + 1 : member;
    const cbm_gbuf_node_t **hits = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_name(ctx->gbuf, leaf, &hits, &count) != 0)
        return NULL;
    const cbm_gbuf_node_t *found = NULL;
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *hit = hits[i];
        if (!hit || !hit->file_path || !import_language_matches(lang, hit))
            continue;
        const char *declared = namespace_for_file(map, hit->file_path);
        if (!declared || strcmp(declared, space) != 0)
            continue;
        const cbm_gbuf_node_t *file =
            cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, hit->file_path);
        if (!file || namespace_member(ctx, file, space, member) != hit)
            continue;
        if (found && found->id != file->id) {
            *ambiguous = true;
            return NULL;
        }
        found = file;
    }
    return found;
}

static bool namespace_target_qn(const cbm_pipeline_ctx_t *ctx, CBMLanguage lang, const char *space,
                                char *qn, size_t capacity) {
    int length =
        snprintf(qn, capacity, "%s.__namespace.%d.%s", ctx->project_name, (int)lang, space);
    return length > 0 && (size_t)length < capacity;
}

static const cbm_gbuf_node_t *namespace_bucket_target(const cbm_pipeline_ctx_t *ctx,
                                                      CBMHashTable *map, const char *bucket,
                                                      const char *space, const char *member,
                                                      CBMLanguage lang, bool *ambiguous) {
    if (member && member[0])
        return namespace_member_target(ctx, map, space, member, lang, ambiguous);
    const cbm_gbuf_node_t *found = NULL;
    for (const char *entry = bucket; entry && entry[0];) {
        const char *end = strchr(entry, '\n');
        size_t length = end ? (size_t)(end - entry) : strlen(entry);
        char qn[PKGMAP_PATH_BUF];
        if (length == 0 || length >= sizeof(qn))
            return NULL;
        memcpy(qn, entry, length);
        qn[length] = '\0';
        const cbm_gbuf_node_t *file = cbm_gbuf_find_by_qn(ctx->gbuf, qn);
        if (!file || !file->label || strcmp(file->label, "File") != 0)
            return NULL;
        if (!import_language_matches(lang, file)) {
            entry = end ? end + 1 : NULL;
            continue;
        }
        if (found && found->id != file->id) {
            char namespace_qn[PKGMAP_PATH_BUF];
            const cbm_gbuf_node_t *namespace_node =
                namespace_target_qn(ctx, lang, space, namespace_qn, sizeof(namespace_qn))
                    ? cbm_gbuf_find_by_qn(ctx->gbuf, namespace_qn)
                    : NULL;
            if (namespace_node && namespace_node->label &&
                strcmp(namespace_node->label, "Module") == 0 && namespace_node->properties_json &&
                strstr(namespace_node->properties_json, "\"resolver_namespace\":true"))
                return namespace_node;
            *ambiguous = true;
            return NULL;
        }
        found = file;
        entry = end ? end + 1 : NULL;
    }
    return found;
}

static bool namespace_import_path(const CBMImport *imp, CBMLanguage lang, char *path,
                                  size_t capacity) {
    if (!imp || !imp->module_path || strchr(imp->module_path, '{'))
        return false;
    const char *spelling = imp->module_path;
    if (lang == CBM_LANG_PHP && spelling[0] == '\\')
        spelling++;
    int length = snprintf(path, capacity, "%s", spelling);
    if (length <= 0 || (size_t)length >= capacity)
        return false;
    char *alias = strstr(path, " as ");
    if (alias)
        *alias = '\0';
    for (char *p = path; *p; p++)
        if (*p == '\\')
            *p = '.';
    size_t size = strlen(path);
    if (size >= 2 && strcmp(path + size - 2, ".*") == 0)
        path[size - 2] = '\0';
    return path[0] != '\0';
}

/* A namespace can span files. Materialize its exact internal identity during
 * coordinator linking, before read-only resolution, instead of selecting one
 * arbitrary file. It does not claim any exported member binding. */
static void materialize_namespace_import(cbm_pipeline_ctx_t *ctx, const CBMImport *imp,
                                         CBMLanguage lang, CBMHashTable *map) {
    if (!map || (lang != CBM_LANG_JAVA && lang != CBM_LANG_KOTLIN && lang != CBM_LANG_CSHARP &&
                 lang != CBM_LANG_SCALA && lang != CBM_LANG_PHP))
        return;
    char path[PKGMAP_PATH_BUF];
    if (!namespace_import_path(imp, lang, path, sizeof(path)))
        return;
    const char *bucket = cbm_ht_get(map, path);
    if (!bucket)
        return;
    bool ambiguous = false;
    (void)namespace_bucket_target(ctx, map, bucket, path, "", lang, &ambiguous);
    if (!ambiguous)
        return;
    char qn[PKGMAP_PATH_BUF];
    if (namespace_target_qn(ctx, lang, path, qn, sizeof(qn)))
        (void)cbm_gbuf_upsert_node(ctx->gbuf, "Module", path, qn, "", 0, 0,
                                   "{\"resolver_namespace\":true}");
}

static const cbm_gbuf_node_t *resolve_declared_namespace(const cbm_pipeline_ctx_t *ctx,
                                                         const CBMImport *imp,
                                                         CBMHashTable *namespace_map,
                                                         CBMLanguage lang, bool *ambiguous) {
    char path[PKGMAP_PATH_BUF];
    if (!namespace_map || !namespace_import_path(imp, lang, path, sizeof(path)))
        return NULL;
    char space[PKGMAP_PATH_BUF];
    snprintf(space, sizeof(space), "%s", path);
    for (;;) {
        const char *bucket = cbm_ht_get(namespace_map, space);
        if (bucket) {
            const char *member = path + strlen(space);
            if (*member == '.')
                member++;
            /* A matching namespace is a binding boundary, including a missing
             * member; do not retry a shorter namespace or global leaf name. */
            return namespace_bucket_target(ctx, namespace_map, bucket, space, member, lang,
                                           ambiguous);
        }
        char *dot = strrchr(space, '.');
        if (!dot)
            return NULL;
        *dot = '\0';
    }
}

/* Import syntax names modules, independently of the symbol-QN extension
 * stripping used elsewhere. A physical module/package identity must exist;
 * declarations with matching spellings never stand in for a module. */
static const cbm_gbuf_node_t *python_module_file(const cbm_pipeline_ctx_t *ctx, const char *path) {
    const cbm_gbuf_node_t *file = cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, path);
    if (file)
        return file;
    char *qn = cbm_pipeline_fqn_module(ctx->project_name, path);
    const cbm_gbuf_node_t *module = qn ? cbm_gbuf_find_by_qn(ctx->gbuf, qn) : NULL;
    free(qn);
    return module && module->label && strcmp(module->label, "Module") == 0 && module->file_path &&
                   strcmp(module->file_path, path) == 0
               ? module
               : NULL;
}

static const cbm_gbuf_node_t *python_module_at_path(const cbm_pipeline_ctx_t *ctx,
                                                    const char *relative) {
    char file[PKGMAP_PATH_BUF], init[PKGMAP_PATH_BUF];
    int file_length = snprintf(file, sizeof(file), "%s.py", relative);
    int init_length =
        snprintf(init, sizeof(init), "%s%s__init__.py", relative, relative[0] ? "/" : "");
    if (file_length <= 0 || (size_t)file_length >= sizeof(file) || init_length <= 0 ||
        (size_t)init_length >= sizeof(init))
        return NULL;
    const cbm_gbuf_node_t *module = relative[0] ? python_module_file(ctx, file) : NULL;
    const cbm_gbuf_node_t *package = python_module_file(ctx, init);
    /* Competing physical module/package forms need loader precedence facts. */
    return module && package ? NULL : module ? module : package;
}

static bool python_import_path(const char *path) {
    const unsigned char *p = (const unsigned char *)path;
    if (!p || !*p)
        return false;
    while (*p == '.')
        p++;
    if (!*p)
        return true; /* `from . import ...` names the current package. */
    for (;;) {
        if (*p != '_' && !isalpha(*p) && *p < 0x80)
            return false;
        do {
            p++;
        } while (*p == '_' || isalnum(*p) || *p >= 0x80);
        if (!*p)
            return true;
        if (*p++ != '.' || !*p)
            return false;
    }
}

static const cbm_gbuf_node_t *python_configured_module(const cbm_pipeline_ctx_t *ctx,
                                                       const char *path, bool *covered) {
    *covered = false;
    CBMHashTable *map = cbm_pipeline_get_pkgmap();
    if (!map)
        return NULL;
    char prefix[PKGMAP_PATH_BUF];
    int length = snprintf(prefix, sizeof(prefix), "%s", path);
    if (length <= 0 || (size_t)length >= sizeof(prefix))
        return NULL;
    for (;;) {
        const char *mapped = cbm_ht_get(map, prefix);
        if (mapped) {
            *covered = true;
            if (strcmp(mapped, ambiguous_pkgmap_value) == 0)
                return NULL;
            const cbm_gbuf_node_t *base = cbm_gbuf_find_by_qn(ctx->gbuf, mapped);
            if (!base || !base->file_path || !base->label ||
                (strcmp(base->label, "Module") != 0 && strcmp(base->label, "File") != 0) ||
                !import_language_matches(CBM_LANG_PYTHON, base))
                return NULL;
            const char *tail = path + strlen(prefix);
            if (!*tail)
                return base;
            if (*tail != '.' || strcmp(path_leaf(base->file_path), "__init__.py") != 0)
                return NULL;
            char *directory = path_dirname(base->file_path);
            char child[PKGMAP_PATH_BUF], relative[PKGMAP_PATH_BUF];
            int size = snprintf(child, sizeof(child), "%s", tail + 1);
            bool valid = size > 0 && (size_t)size < sizeof(child);
            for (char *p = child; valid && *p; p++)
                if (*p == '.')
                    *p = '/';
            valid = valid && directory &&
                    cbm_repo_rel_join(directory, child, relative, sizeof(relative));
            cbm_free(CBM_MEM_CLASS_SEMANTIC, directory);
            return valid ? python_module_at_path(ctx, relative) : NULL;
        }
        char *dot = strrchr(prefix, '.');
        if (!dot)
            return NULL;
        *dot = '\0';
    }
}

static const cbm_gbuf_node_t *python_exact_module(const cbm_pipeline_ctx_t *ctx,
                                                  const char *source_rel, const char *path) {
    if (!python_import_path(path))
        return NULL;
    if (path[0] != '.') {
        bool covered = false;
        const cbm_gbuf_node_t *configured = python_configured_module(ctx, path, &covered);
        if (covered)
            return configured;
    } else {
        size_t dots = strspn(path, "."), depth = 1;
        for (const char *p = source_rel; p && *p; p++)
            depth += *p == '/';
        if (dots > depth)
            return NULL;
    }
    char *relative = path[0] == '.' ? cbm_pipeline_resolve_relative_import(source_rel, path) : NULL;
    if (path[0] == '.' && !relative)
        return NULL;
    const char *value = relative ? relative : path;
    char normalized[PKGMAP_PATH_BUF];
    int length = snprintf(normalized, sizeof(normalized), "%s", value);
    free(relative);
    if (length < 0 || (size_t)length >= sizeof(normalized))
        return NULL;
    if (path[0] != '.')
        for (char *p = normalized; *p; p++)
            if (*p == '.')
                *p = '/';
    return python_module_at_path(ctx, normalized);
}

static bool python_source_import_kind(CBMImportKind kind) {
    return kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT ||
           kind == CBM_IMPORT_KIND_PYTHON_MODULE_ALIAS || kind == CBM_IMPORT_KIND_PYTHON_FROM;
}

static const cbm_gbuf_node_t *python_source_import_target(const cbm_pipeline_ctx_t *ctx,
                                                          const char *source_rel,
                                                          const CBMImport *imp, bool dependency) {
    if (imp->kind != CBM_IMPORT_KIND_PYTHON_FROM)
        return python_exact_module(ctx, source_rel, imp->module_path);
    const char *last = strrchr(imp->module_path, '.');
    if (!last || !last[1])
        return NULL;
    char parent[PKGMAP_PATH_BUF];
    size_t length = (size_t)(last - imp->module_path);
    /* Preserve every leading dot when the parent is just `.` or `..`. */
    if (length < strspn(imp->module_path, "."))
        length++;
    if (length >= sizeof(parent))
        return NULL;
    memcpy(parent, imp->module_path, length);
    parent[length] = '\0';
    const cbm_gbuf_node_t *module = python_exact_module(ctx, source_rel, parent);
    if (module && module->file_path) {
        if (dependency || strcmp(last + 1, "*") == 0)
            return module; /* IMPORTS describes the physical module dependency.
                              Local member bindings are resolved independently. */
        char *qn = cbm_pipeline_fqn_compute(ctx->project_name, module->file_path, last + 1);
        const cbm_gbuf_node_t *member = qn ? cbm_gbuf_find_by_qn(ctx->gbuf, qn) : NULL;
        free(qn);
        if (member && member->file_path && strcmp(member->file_path, module->file_path) == 0 &&
            member->label && strcmp(member->label, "File") != 0 &&
            strcmp(member->label, "Module") != 0 && strcmp(member->label, "Folder") != 0)
            return member;
    }
    /* Dependency on the parent does not prove a local member binding or
     * load an indexed child. The shared namespace audit proves that separately. */
    return dependency ? module : NULL;
}

char *cbm_pipeline_python_import_binding(const cbm_gbuf_t *gbuf, const char *project,
                                         const char *source_rel, const CBMImport *imp) {
    if (!gbuf || !project || !source_rel || !imp || !imp->local_name || !imp->module_path ||
        !python_source_import_kind(imp->kind) || strcmp(imp->local_name, "*") == 0)
        return NULL;
    cbm_pipeline_ctx_t ctx = {.project_name = project, .gbuf = (cbm_gbuf_t *)gbuf};
    const cbm_gbuf_node_t *target = python_source_import_target(&ctx, source_rel, imp, false);
    if (!target || !target->label || !target->qualified_name)
        return NULL;
    if (strcmp(target->label, "File") != 0 && strcmp(target->label, "Module") != 0)
        return imp->kind == CBM_IMPORT_KIND_PYTHON_FROM ? strdup(target->qualified_name) : NULL;
    if (!target->file_path)
        return NULL;
    char *binding = cbm_pipeline_fqn_module(project, target->file_path);
    size_t prefix = cbm_pipeline_fqn_symbol_prefix_len(project, target->file_path);
    if (!binding || prefix == 0 || prefix > strlen(binding)) {
        free(binding);
        return NULL;
    }
    binding[prefix] = '\0'; /* Package members omit the physical __init__ stem. */
    if (imp->kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT) {
        size_t root_length = strcspn(imp->module_path, ".");
        if (strlen(imp->local_name) != root_length ||
            strncmp(imp->local_name, imp->module_path, root_length) != 0) {
            free(binding);
            return NULL;
        }
    }
    return binding;
}

/* Slot results cross into the shared resolver as SEMANTIC-owned strings. */
char *cbm_pipeline_python_import_slot(const cbm_gbuf_t *graph, const char *project,
                                      const char *source_rel, const CBMImport *import,
                                      bool *module) {
    *module = false;
    if (!graph || !project || !source_rel || !import || !import->module_path ||
        !import->local_name || !python_source_import_kind(import->kind) ||
        !strcmp(import->local_name, "*"))
        return NULL;
    if (import->kind != CBM_IMPORT_KIND_PYTHON_FROM) {
        *module = true;
        return pkgmap_take_raw(
            cbm_pipeline_python_import_binding(graph, project, source_rel, import));
    }
    const char *last = strrchr(import->module_path, '.');
    if (!last || !last[1] || !strcmp(last + 1, "*"))
        return NULL;
    size_t length = (size_t)(last - import->module_path);
    if (length < strspn(import->module_path, "."))
        length++;
    char parent[PKGMAP_PATH_BUF];
    if (length >= sizeof(parent))
        return NULL;
    memcpy(parent, import->module_path, length);
    parent[length] = '\0';
    cbm_pipeline_ctx_t context = {.project_name = project, .gbuf = (cbm_gbuf_t *)graph};
    const cbm_gbuf_node_t *file = python_exact_module(&context, source_rel, parent);
    if (!file || !file->file_path)
        return NULL;
    /* This names an exported symbol, including one literally called __file__.
     * File-node identity has a separate extension-preserving contract. */
    CBMArena arena;
    cbm_arena_init(&arena);
    const char *symbol = cbm_fqn_compute(&arena, project, file->file_path, last + 1);
    char *slot = symbol ? cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, symbol) : NULL;
    cbm_arena_destroy(&arena);
    return slot;
}

bool cbm_pipeline_python_submodule_candidate(const cbm_gbuf_t *graph, const char *project,
                                             const char *source_rel, const CBMImport *import,
                                             const cbm_gbuf_node_t **parent,
                                             const cbm_gbuf_node_t **child) {
    *parent = NULL;
    *child = NULL;
    if (!graph || !project || !source_rel || !import ||
        import->kind != CBM_IMPORT_KIND_PYTHON_FROM || !import->module_path)
        return false;
    const char *last = strrchr(import->module_path, '.');
    if (!last || !last[1] || !strcmp(last + 1, "*"))
        return false;
    size_t length = (size_t)(last - import->module_path);
    if (length < strspn(import->module_path, "."))
        length++;
    char parent_path[PKGMAP_PATH_BUF];
    if (length >= sizeof(parent_path))
        return false;
    memcpy(parent_path, import->module_path, length);
    parent_path[length] = '\0';
    cbm_pipeline_ctx_t ctx = {.project_name = project, .gbuf = (cbm_gbuf_t *)graph};
    const cbm_gbuf_node_t *package = python_exact_module(&ctx, source_rel, parent_path);
    const cbm_gbuf_node_t *submodule = python_exact_module(&ctx, source_rel, import->module_path);
    if (!package || !package->file_path || strcmp(path_leaf(package->file_path), "__init__.py") ||
        !submodule || !submodule->file_path || !strcmp(package->file_path, submodule->file_path))
        return false;
    *parent = package;
    *child = submodule;
    return true;
}

static bool python_map_put(CBMHashTable *by_local, const char **keys, const char **vals, int *count,
                           const char *local, char *value) {
    if (!value)
        return false;
    uintptr_t previous = (uintptr_t)cbm_ht_get(by_local, local);
    if (previous) {
        char *prior = (char *)vals[previous - 1];
        if (strcmp(prior, value) != 0)
            prior[0] = '\0';
        free(value);
        return true;
    }
    char *key = strdup(local);
    if (!key) {
        free(value);
        return false;
    }
    keys[*count] = key;
    vals[*count] = value;
    (*count)++;
    cbm_ht_set(by_local, key, (void *)(uintptr_t)*count);
    return (uintptr_t)cbm_ht_get(by_local, key) == (uintptr_t)*count;
}

int cbm_pipeline_ecma_import_map(const cbm_pipeline_ctx_t *ctx, const char *source_rel,
                                 const CBMFileResult *result, bool include_type_only,
                                 const char ***out_keys, const char ***out_values, int *out_count) {
    *out_keys = NULL;
    *out_values = NULL;
    *out_count = 0;
    int imports = result ? result->imports.count : 0;
    if (imports <= 0)
        return 0;
    const char **keys = calloc((size_t)imports, sizeof(*keys));
    const char **values = calloc((size_t)imports, sizeof(*values));
    CBMHashTable *locals = cbm_ht_create((uint32_t)imports);
    int count = 0;
    if (!keys || !values || !locals)
        goto failed;
    for (int i = 0; i < imports; i++) {
        const CBMImport *import = &result->imports.items[i];
        if (!import->local_name || !import->local_name[0] ||
            (!include_type_only && !cbm_import_binds_runtime_name(import)))
            continue;
        const cbm_gbuf_node_t *dependency = NULL;
        char *value = NULL;
        if (!result->parse_unusable && !result->walk_truncated && !import->is_default) {
            cbm_resolve_result_t proof = cbm_pipeline_resolve_import_result(
                ctx, source_rel, NULL, import, NULL, &dependency);
            if (cbm_resolve_admit(&proof) && dependency && dependency->file_path &&
                dependency->label &&
                (!strcmp(dependency->label, "File") || !strcmp(dependency->label, "Module"))) {
                value = cbm_pipeline_fqn_compute(ctx->project_name, dependency->file_path,
                                                 import->member);
            }
        }
        /* An unresolved/default binding still shadows project names. Export
         * and lexical checks remain mandatory at the reference occurrence. */
        if (!value)
            value = strdup("");
        if (!python_map_put(locals, keys, values, &count, import->local_name, value))
            goto failed;
    }
    cbm_ht_free(locals);
    *out_keys = keys;
    *out_values = values;
    *out_count = count;
    return 0;
failed:
    cbm_ht_free(locals);
    for (int i = 0; i < count; i++) {
        free((void *)keys[i]);
        free((void *)values[i]);
    }
    free(keys);
    free(values);
    return -1;
}

int cbm_pipeline_python_import_map(const cbm_gbuf_t *gbuf, const char *project,
                                   const char *source_rel, const CBMFileResult *result,
                                   bool include_type_only, const char ***out_keys,
                                   const char ***out_vals, int *out_count) {
    *out_keys = NULL;
    *out_vals = NULL;
    *out_count = 0;
    int imports = result ? result->imports.count : 0;
    if (imports <= 0)
        return 0;
    if (imports > INT32_MAX / 2 || (size_t)imports > SIZE_MAX / 2 / sizeof(const char *))
        return -1;
    size_t capacity = (size_t)imports * 2;
    const char **keys = calloc(capacity, sizeof(*keys));
    const char **vals = calloc(capacity, sizeof(*vals));
    CBMHashTable *by_local = cbm_ht_create((uint32_t)capacity);
    int count = 0;
    if (!keys || !vals || !by_local)
        goto failed;
    for (int i = 0; i < imports; i++) {
        const CBMImport *imp = &result->imports.items[i];
        if (!imp->local_name || !imp->local_name[0] ||
            (!include_type_only && !cbm_import_binds_runtime_name(imp)))
            continue;
        bool dotted_root = imp->kind == CBM_IMPORT_KIND_PYTHON_MODULE_ROOT && imp->module_path &&
                           strchr(imp->module_path, '.') != NULL;
        if (dotted_root &&
            !python_map_put(by_local, keys, vals, &count, imp->local_name, strdup("")))
            goto failed;
        char *value = !result->parse_unusable && !result->walk_truncated
                          ? cbm_pipeline_python_import_binding(gbuf, project, source_rel, imp)
                          : NULL;
        if (!value)
            value = strdup("");
        const char *local = dotted_root ? imp->module_path : imp->local_name;
        if (!python_map_put(by_local, keys, vals, &count, local, value))
            goto failed;
    }
    cbm_ht_free(by_local);
    *out_keys = keys;
    *out_vals = vals;
    *out_count = count;
    return 0;

failed:
    cbm_ht_free(by_local);
    for (int i = 0; i < count; i++) {
        free((void *)keys[i]);
        free((void *)vals[i]);
    }
    free(keys);
    free(vals);
    return -1;
}

/* Node bare package lookup uses the nearest manifest for self-reference,
 * then an enclosing declared workspace. Entry paths stay exact through File
 * materialization; dotted filenames cannot lose a second extension. */
static const cbm_gbuf_node_t *node_package_target(const cbm_pipeline_ctx_t *ctx, const char *source,
                                                  const char *specifier, CBMLanguage language,
                                                  bool *covered) {
    *covered = false;
    CBMHashTable *map = cbm_pipeline_get_pkgmap();
    if (!map || !source || specifier[0] == '.' || specifier[0] == '/')
        return NULL;
    const char *seal = cbm_ht_get(map, "@cbm:node:complete");
    if (!seal || strcmp(seal, "1")) {
        *covered = true;
        return NULL;
    }
    const char *slash = strchr(specifier, '/');
    if (specifier[0] == '@')
        slash = slash ? strchr(slash + 1, '/') : NULL;
    size_t length = slash ? (size_t)(slash - specifier) : strlen(specifier);
    char package[PKGMAP_PATH_BUF], subpath[PKGMAP_PATH_BUF];
    if (!length || length >= sizeof(package))
        return NULL;
    memcpy(package, specifier, length);
    package[length] = '\0';
    if (!node_package_name(package))
        return NULL;
    int n = snprintf(subpath, sizeof(subpath), ".%s", slash ? slash : "");
    if (n <= 0 || (size_t)n >= sizeof(subpath) || (slash && !package_export_path(subpath))) {
        *covered = true;
        return NULL;
    }
    *covered = cbm_ht_get(map, package) != NULL;
    char directory[PKGMAP_PATH_BUF];
    int size = snprintf(directory, sizeof(directory), "%s", source);
    if (size < 0 || (size_t)size >= sizeof(directory)) {
        *covered = true;
        return NULL;
    }
    char *parent = strrchr(directory, '/');
    if (parent)
        *parent = '\0';
    else
        directory[0] = '\0';
    const char *owner = NULL;
    bool scope_seen = false;
    for (;;) {
        const char *scope = node_fact(map, "scope", directory, NULL);
        if (!scope_seen && scope) {
            scope_seen = true;
            if (!strcmp(scope, ambiguous_pkgmap_value)) {
                *covered = true;
                break;
            }
            if (!strcmp(scope, package) && node_fact(map, "self", directory, NULL)) {
                owner = directory;
                *covered = true;
                break;
            }
        }
        if (scope && (node_fact(map, "dependency", directory, package) ||
                      node_fact(map, "dependency", directory, NULL))) {
            *covered = true;
            break;
        }
        if (node_fact(map, "root", directory, NULL)) {
            owner = node_fact(map, "bound", directory, package);
            *covered = *covered || owner != NULL;
            break;
        }
        if (!*directory)
            break;
        parent = strrchr(directory, '/');
        if (parent)
            *parent = '\0';
        else
            directory[0] = '\0';
    }
    const char *entry = owner && strcmp(owner, ambiguous_pkgmap_value)
                            ? node_fact(map, "entry", owner, subpath)
                            : NULL;
    const cbm_gbuf_node_t *target =
        entry && strcmp(entry, ambiguous_pkgmap_value)
            ? cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, entry)
            : NULL;
    return target && target->file_path && !strcmp(target->file_path, entry) &&
                   import_language_matches(language, target)
               ? target
               : NULL;
}

static const cbm_gbuf_node_t *resolve_import_strategies(
    const cbm_pipeline_ctx_t *ctx, const char *source_rel, const CBMImport *imp,
    CBMHashTable *namespace_map, CBMLanguage src_lang, bool php_only, bool *ambiguous) {
    /* Explicit paths preserve the full filename before any module-QN
     * normalization. This also keeps an above-root path from being retried. */
    bool slash_relative =
        imp->module_path[0] == '.' &&
        (imp->module_path[1] == '/' || (imp->module_path[1] == '.' && imp->module_path[2] == '/'));
    const cbm_gbuf_node_t *file =
        resolve_relative_file(ctx, source_rel, imp->module_path, php_only);
    if (file)
        return file;

    if (src_lang == CBM_LANG_JAVASCRIPT || src_lang == CBM_LANG_TYPESCRIPT ||
        src_lang == CBM_LANG_TSX || src_lang == CBM_LANG_ARKTS) {
        bool covered = false;
        const cbm_gbuf_node_t *package =
            node_package_target(ctx, source_rel, imp->module_path, src_lang, &covered);
        if (covered)
            return package;
    }

    if (src_lang == CBM_LANG_PYTHON && python_source_import_kind(imp->kind))
        return python_source_import_target(ctx, source_rel, imp, true);
    if (src_lang == CBM_LANG_PYTHON) {
        const cbm_gbuf_node_t *module = python_exact_module(ctx, source_rel, imp->module_path);
        if (module)
            return module;
    }
    bool module_rule = slash_relative || src_lang == CBM_LANG_PYTHON ||
                       import_has_configured_root(ctx, source_rel, imp->module_path);
    if (module_rule) {
        char *qn = cbm_pipeline_resolve_module(ctx, source_rel, imp->module_path);
        const cbm_gbuf_node_t *target = qn ? cbm_gbuf_find_by_qn(ctx->gbuf, qn) : NULL;
        free(qn);
        if (target && (!php_only || node_in_php_file(target)) &&
            (import_language_matches(src_lang, target) ||
             (src_lang == CBM_LANG_GO && target->label && strcmp(target->label, "Folder") == 0))) {
            if (target->label &&
                (strcmp(target->label, "Module") == 0 || strcmp(target->label, "File") == 0)) {
                char sym[CBM_SZ_256];
                const char *name = src_lang == CBM_LANG_PYTHON
                                       ? import_candidate_symbol(imp->module_path, sym, sizeof(sym))
                                       : NULL;
                if (name && strcmp(import_last_segment(target->qualified_name), name) != 0) {
                    char member[PKGMAP_PATH_BUF];
                    int len =
                        snprintf(member, sizeof(member), "%s.%s", target->qualified_name, name);
                    const cbm_gbuf_node_t *hit = len > 0 && (size_t)len < sizeof(member)
                                                     ? cbm_gbuf_find_by_qn(ctx->gbuf, member)
                                                     : NULL;
                    if (hit && import_targetable_label(hit->label))
                        return hit;
                }
                return target;
            }
            if (src_lang == CBM_LANG_GO && target->label && strcmp(target->label, "Folder") == 0)
                return target;
            /* Python's dotted import path may name a declaration exactly; a
             * plain module import may never bind an arbitrary function/class. */
            if (src_lang == CBM_LANG_PYTHON && strchr(imp->module_path, '.') &&
                import_targetable_label(target->label))
                return target;
        }
        if (slash_relative)
            return NULL;
    }
    if (src_lang == CBM_LANG_JAVA || src_lang == CBM_LANG_KOTLIN || src_lang == CBM_LANG_CSHARP ||
        src_lang == CBM_LANG_SCALA)
        return resolve_declared_namespace(ctx, imp, namespace_map, src_lang, ambiguous);
    return NULL;
}

/* ── PHP `use` of a namespaced name (#1186 follow-up) ─────────────── */

/* The shape of a PHP `use` import. The importing file is PHP and the path is
 * a name: identifier segments, joined by '\' when qualified. require/include
 * paths ("lib/x.php", 'a\b.php') contain '/', '.' or quotes: PHP_USE_NONE. */
typedef enum {
    PHP_USE_NONE = 0,   /* not a PHP `use` (other language, require/include) */
    PHP_USE_GLOBAL,     /* `use Closure;` names the global namespace */
    PHP_USE_NAMESPACED, /* `use A\B\C;` */
} php_use_shape_t;

enum { PHP_NON_ASCII_MIN = 0x80 }; /* PHP identifiers admit any byte >= 0x80 */

static bool php_ident_byte(unsigned char c) {
    return c == '_' || isalnum(c) || c >= PHP_NON_ASCII_MIN;
}

static php_use_shape_t php_use_shape(const char *source_rel, const CBMImport *imp) {
    if (!source_rel || cbm_language_for_filename(path_leaf(source_rel)) != CBM_LANG_PHP) {
        return PHP_USE_NONE;
    }
    const char *p = imp->module_path;
    if (p[0] == '\\') {
        p++;
    }
    if (!p[0]) {
        return PHP_USE_NONE;
    }
    bool qualified = false;
    for (; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '\\') {
            qualified = true;
        } else if (!php_ident_byte(c)) {
            return PHP_USE_NONE;
        }
    }
    return qualified ? PHP_USE_NAMESPACED : PHP_USE_GLOBAL;
}

static const cbm_gbuf_node_t *resolve_import_target(const cbm_pipeline_ctx_t *ctx,
                                                    const char *source_rel,
                                                    const char *source_file_qn,
                                                    const CBMImport *imp,
                                                    CBMHashTable *namespace_map, CBMLanguage lang,
                                                    bool *ambiguous, const char **reason) {
    if (!ctx || !ctx->gbuf || !imp || !imp->module_path || !imp->module_path[0]) {
        *reason = "invalid_import";
        return NULL;
    }
    /* Availability is only a negative boundary. A supported language still
     * needs its adapter's source/path proof below; a captured path alone must
     * never enable the generic strategy for an unaudited language. */
    cbm_resolve_edge_capability_t capability;
    if (!cbm_resolve_edge_capability(lang, "IMPORTS", &capability) ||
        capability.availability != CBM_RESOLVE_EDGE_SUPPORTED_SUBSET) {
        *reason = "unsupported_import_language";
        return NULL;
    }
    /* ECMAScript carriers retain raw StringLiteral spelling. Until escape
     * decoding is proved, a backslash must not become a filesystem separator
     * during File-QN lookup. This shared negative gate also protects callable
     * import maps while preserving the unresolved local binding as a blocker. */
    if ((lang == CBM_LANG_JAVASCRIPT || lang == CBM_LANG_TYPESCRIPT || lang == CBM_LANG_TSX ||
         lang == CBM_LANG_ARKTS) &&
        strchr(imp->module_path, '\\')) {
        *reason = "ecma_module_escape_unproven";
        return NULL;
    }
    if (lang == CBM_LANG_PYTHON && imp->module_path[0] == '.') {
        int dots = 0;
        while (imp->module_path[dots] == '.')
            dots++;
        int depth = 1;
        for (const char *p = source_rel; p && *p; p++)
            if (*p == '/')
                depth++;
        if (dots > depth) {
            *reason = "relative_import_above_root";
            return NULL;
        }
    }
    if (lang == CBM_LANG_DART) {
        *reason = "dart_import_uri_unproven";
        if (imp->kind != CBM_IMPORT_KIND_LITERAL_RELATIVE_PATH)
            return NULL;
        char *directory = path_dirname(source_rel);
        char path[PKGMAP_PATH_BUF];
        bool valid =
            directory && cbm_repo_rel_join(directory, imp->module_path, path, sizeof(path));
        cbm_free(CBM_MEM_CLASS_SEMANTIC, directory);
        *reason = valid ? "dart_direct_literal_uri" : "relative_import_above_root";
        const cbm_gbuf_node_t *target =
            valid ? cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, path) : NULL;
        return target && import_language_matches(CBM_LANG_DART, target) ? target : NULL;
    }
    if (lang == CBM_LANG_JUST || lang == CBM_LANG_FUNC || lang == CBM_LANG_SCSS ||
        lang == CBM_LANG_MESON) {
        *reason = "literal_relative_path_unproven";
        if (imp->kind != CBM_IMPORT_KIND_LITERAL_RELATIVE_PATH)
            return NULL;
        *reason = "audited_language_relative_path";
        return resolve_sibling_file(ctx, source_rel, imp->module_path, lang, ambiguous);
    }
    if (lang == CBM_LANG_CSS || lang == CBM_LANG_BICEP || lang == CBM_LANG_CAPNP) {
        *reason = "literal_relative_path_unproven";
        if (imp->kind != CBM_IMPORT_KIND_LITERAL_RELATIVE_PATH)
            return NULL;
        char *directory = path_dirname(source_rel);
        char path[PKGMAP_PATH_BUF];
        bool valid =
            directory && cbm_repo_rel_join(directory, imp->module_path, path, sizeof(path));
        cbm_free(CBM_MEM_CLASS_SEMANTIC, directory);
        *reason = valid ? "audited_literal_relative_path" : "relative_import_above_root";
        return valid ? cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, path) : NULL;
    }
    if (lang == CBM_LANG_KUSTOMIZE) {
        *reason = "relative_resource_path";
        return resolve_kustomize_entry(ctx, source_rel, imp->module_path);
    }
    if (lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA ||
        lang == CBM_LANG_OBJC) {
        *reason = imp->kind == CBM_IMPORT_KIND_INCLUDE_QUOTED ? "quoted_include_path"
                                                              : "include_search_roots_unknown";
        return resolve_configured_include(ctx, source_rel, imp, namespace_map);
    }
    if (lang == CBM_LANG_RUST) {
        *reason = "declared_rust_module_path";
        return resolve_rust_import(ctx, source_rel, imp, namespace_map, ambiguous, NULL);
    }
    if (lang == CBM_LANG_PHP) {
        if (imp->kind == CBM_IMPORT_KIND_PHP_INCLUDE_UNPROVEN) {
            *reason = "php_runtime_include_context_unknown";
            return NULL;
        }
        if (imp->kind == CBM_IMPORT_KIND_LITERAL_RELATIVE_PATH) {
            char *directory = path_dirname(source_rel ? source_rel : "");
            char path[PKGMAP_PATH_BUF];
            bool valid =
                directory && cbm_repo_rel_join(directory, imp->module_path, path, sizeof(path));
            cbm_free(CBM_MEM_CLASS_SEMANTIC, directory);
            *reason = valid ? "php_dir_anchored_include" : "relative_import_above_root";
            return valid ? cbm_pipeline_file_node(ctx->gbuf, ctx->project_name, path) : NULL;
        }
        const cbm_gbuf_node_t *psr4_target = NULL;
        switch (resolve_php_psr4_class(ctx->gbuf, source_file_qn, imp, &psr4_target)) {
        case PSR4_RESOLVED:
            *reason = "composer_psr4_path";
            return psr4_target;
        case PSR4_UNRESOLVED:
            *reason = "composer_psr4_target_missing";
            return NULL;
        case PSR4_NOT_APPLICABLE:
            break;
        }
        php_use_shape_t shape = php_use_shape(source_rel, imp);
        if (shape != PHP_USE_NONE) {
            *reason = namespace_map ? "declared_namespace_member" : "namespace_context_missing";
            if (!namespace_map)
                return NULL;
            if (shape == PHP_USE_NAMESPACED)
                return resolve_declared_namespace(ctx, imp, namespace_map, lang, ambiguous);
            const char *name =
                imp->module_path[0] == '\\' ? imp->module_path + 1 : imp->module_path;
            return namespace_member_target(ctx, namespace_map, "", name, lang, ambiguous);
        }
        *reason = "php_include_path_authority_missing";
        return NULL;
    }
    *reason = "supported_import_path";
    const cbm_gbuf_node_t *target =
        resolve_import_strategies(ctx, source_rel, imp, namespace_map, lang, false, ambiguous);
    if (lang == CBM_LANG_GO && target &&
        !cbm_pipeline_go_import_owned(ctx, source_rel, imp, target)) {
        *reason = "go_module_dependency_authority_unproven";
        return NULL;
    }
    return target;
}

static bool import_occurrence_valid(const CBMFileResult *result, const CBMImport *import) {
    return result && import && import->source_origin == CBM_SOURCE_ORIGIN_RAW &&
           import->site_end_byte > import->site_start_byte &&
           import->site_end_byte <= result->source_content_length[CBM_SOURCE_ORIGIN_RAW];
}

static cbm_resolve_result_t resolve_import_observed(
    const cbm_pipeline_ctx_t *ctx, const char *source_rel, const char *source_file_qn,
    const CBMImport *imp, CBMHashTable *namespace_map, const cbm_gbuf_node_t **out,
    cbm_resolve_lookup_context_t *context, CBMLanguage language) {
    cbm_resolve_lookup_t query;
    bool site = imp && imp->source_origin == CBM_SOURCE_ORIGIN_RAW &&
                imp->site_end_byte > imp->site_start_byte;
    cbm_resolve_lookup_begin(&query, context, "import.path_resolution",
                             imp ? imp->module_path : NULL, site ? imp->site_start_byte : 0,
                             site ? imp->site_end_byte : 0, CBM_SOURCE_ORIGIN_RAW);
    const char *reason = "unsupported_import_rule";
    bool ambiguous = false;
    const cbm_gbuf_node_t *target = resolve_import_target(
        ctx, source_rel, source_file_qn, imp, namespace_map, language, &ambiguous, &reason);
    /* Capture the path adapter's actual observation before making an edge
     * admission result. Failure remains an open/unknown search unless the
     * adapter supplied a target or explicit competing targets. */
    (void)cbm_resolve_lookup_observe(
        &query, source_rel, imp ? imp->module_path : NULL, target ? target->qualified_name : NULL,
        ambiguous ? CBM_RESOLVE_BINDING_AMBIGUOUS
        : target  ? CBM_RESOLVE_BINDING_UNIQUE
                  : CBM_RESOLVE_BINDING_UNRESOLVED,
        target || ambiguous ? CBM_RESOLVE_ANALYSIS_COMPLETE : CBM_RESOLVE_ANALYSIS_UNKNOWN,
        imp ? (uint32_t)imp->kind : 0, target && !ambiguous, reason);
    cbm_resolve_result_t result = {
        .binding = ambiguous ? CBM_RESOLVE_BINDING_AMBIGUOUS : CBM_RESOLVE_BINDING_UNRESOLVED,
        .domain = CBM_RESOLVE_DOMAIN_UNKNOWN,
        .analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
        .rule = CBM_RESOLVE_RULE_MODULE_PATH,
        .reason = ambiguous ? "ambiguous_import_target" : reason,
        .candidate_count = ambiguous ? 2 : 0,
    };
    if (target && target->qualified_name) {
        result.binding = CBM_RESOLVE_BINDING_UNIQUE;
        result.domain = CBM_RESOLVE_DOMAIN_PROJECT;
        result.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        result.target = target->qualified_name;
        result.candidate_count = 1;
        result.facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                       CBM_RESOLVE_FACT_PATH_BOUND;
        if (site)
            result.facts |= CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    }
    if (context) {
        cbm_resolve_result_t symbolic = result;
        if (ambiguous)
            symbolic.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        /* This query was driven by the captured AST import carrier. Its path
         * observation proves this dependency role, independently of edges. */
        query.key.binding_identity = symbolic.target;
        query.key.detail = "IMPORTS";
        result.proof = cbm_resolve_lookup_finish(&query, CBM_PROOF_IMPORT_BINDING, symbolic);
        const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(result.proof);
        if (!view || view->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE) {
            result.analysis = view ? view->outcome.analysis
                              : context->pool->failure == CBM_RESOLVE_ANALYSIS_LIMITED
                                  ? CBM_RESOLVE_ANALYSIS_LIMITED
                                  : CBM_RESOLVE_ANALYSIS_FAILED;
            result.facts = 0;
        }
    }
    if (out)
        *out = cbm_resolve_admit(&result) ? target : NULL;
    return result;
}

cbm_resolve_result_t cbm_pipeline_resolve_import_result(
    const cbm_pipeline_ctx_t *ctx, const char *source_rel, const char *source_file_qn,
    const CBMImport *imp, CBMHashTable *namespace_map, const cbm_gbuf_node_t **out) {
    return resolve_import_observed(
        ctx, source_rel, source_file_qn, imp, namespace_map, out, NULL,
        cbm_language_for_filename(path_leaf(source_rel ? source_rel : "")));
}

static void import_lookup_context(const cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                  const char *file, const char *scope, CBMArena *arena,
                                  cbm_resolve_proof_pool_t *pool,
                                  cbm_resolve_lookup_context_t *context) {
    cbm_arena_init(arena);
    cbm_resolve_proof_pool_init(pool, arena, 0, 0, 0);
    cbm_resolve_proof_source_t source = {.project = ctx->project_name,
                                         .file = file,
                                         .scope = scope,
                                         .module = result->module_qn,
                                         .content_identity =
                                             result->source_content_id[CBM_SOURCE_ORIGIN_RAW],
                                         .configuration_identity = result->source_configuration_id,
                                         .source_origin = CBM_SOURCE_ORIGIN_RAW};
    (void)cbm_resolve_lookup_context_init(context, pool, source);
}

static cbm_resolve_record_t import_record(const CBMFileResult *result, const char *file,
                                          const char *scope, const CBMImport *import,
                                          cbm_resolve_result_t proof, bool emitted) {
    bool site = import_occurrence_valid(result, import);
    return (cbm_resolve_record_t){.file_path = file,
                                  .scope = scope,
                                  .relation = "IMPORTS",
                                  .start_byte = site ? import->site_start_byte : 0,
                                  .end_byte = site ? import->site_end_byte : 0,
                                  .spelling = import->module_path,
                                  .source_origin = CBM_SOURCE_ORIGIN_RAW,
                                  .content_identity =
                                      result->source_content_id[CBM_SOURCE_ORIGIN_RAW],
                                  .configuration_identity = result->source_configuration_id,
                                  .result = proof,
                                  .emitted = emitted};
}

/* Module discovery is inventory, not dependency authority. This bounded Go
 * rule proves only an import within the source's nearest module, with no
 * nested go.mod boundary at the destination. Workspace/replacement mappings
 * remain unknown until their source directives have an exact model. */
static bool go_module_declaration(char *source, char *module, size_t capacity) {
    if (strstr(source, "/*"))
        return false; /* Block-comment stripping is outside this finite reader. */
    bool found = false;
    for (char *line = source; line;) {
        char *next = strchr(line, '\n');
        if (next)
            *next++ = '\0';
        while (*line == ' ' || *line == '\t' || *line == '\r')
            line++;
        if (!strncmp(line, "module", 6) && isspace((unsigned char)line[6])) {
            if (found)
                return false;
            line += 6;
            while (isspace((unsigned char)*line))
                line++;
            char *end = line;
            while (*end && !isspace((unsigned char)*end))
                end++;
            size_t size = (size_t)(end - line);
            if (!size || size >= capacity || memchr(line, '"', size) || memchr(line, '`', size) ||
                memchr(line, '\\', size))
                return false;
            memcpy(module, line, size);
            module[size] = '\0';
            while (isspace((unsigned char)*end))
                end++;
            if (*end && strncmp(end, "//", 2))
                return false;
            found = true;
        }
        line = next;
    }
    return found;
}
static bool go_module_owner(const cbm_pipeline_ctx_t *ctx, const char *path, bool directory,
                            char *owner, char *module, size_t capacity) {
    if (!ctx->repo_path || !path || !cbm_repo_rel_join("", path, owner, capacity))
        return false;
    if (!directory) {
        char *slash = strrchr(owner, '/');
        if (slash)
            *slash = '\0';
        else
            owner[0] = '\0';
    }
    for (;;) {
        char relative[PKGMAP_PATH_BUF], absolute[PKGMAP_PATH_BUF];
        if (!cbm_repo_rel_join(owner, "go.mod", relative, sizeof(relative)) ||
            !import_absolute_path(ctx->repo_path, relative, absolute, sizeof(absolute)))
            return false;
        struct stat info;
        if (stat(absolute, &info) == 0) {
            if (!S_ISREG(info.st_mode))
                return false;
            int length = 0;
            char *source = pkgmap_read_file(absolute, &length);
            bool valid = source && !memchr(source, '\0', (size_t)length) &&
                         go_module_declaration(source, module, capacity);
            cbm_free(CBM_MEM_CLASS_EXTRACT, source);
            return valid;
        }
        if ((errno != ENOENT && errno != ENOTDIR) || !*owner)
            return false;
        char *slash = strrchr(owner, '/');
        if (slash)
            *slash = '\0';
        else
            owner[0] = '\0';
    }
}
bool cbm_pipeline_go_import_owned(const cbm_pipeline_ctx_t *ctx, const char *source_rel,
                                  const CBMImport *import, const cbm_gbuf_node_t *target) {
    if (!ctx || !source_rel || !import || !import->module_path || !target || !target->file_path ||
        !target->label || strcmp(target->label, "Folder"))
        return false;
    char source_owner[PKGMAP_PATH_BUF], source_module[PKGMAP_PATH_BUF],
        target_owner[PKGMAP_PATH_BUF], target_module[PKGMAP_PATH_BUF], expected[PKGMAP_PATH_BUF];
    if (!go_module_owner(ctx, source_rel, false, source_owner, source_module,
                         sizeof(source_owner)) ||
        !go_module_owner(ctx, target->file_path, true, target_owner, target_module,
                         sizeof(target_owner)) ||
        strcmp(source_owner, target_owner) || strcmp(source_module, target_module))
        return false;
    size_t size = strlen(source_module);
    if (strncmp(import->module_path, source_module, size) ||
        (import->module_path[size] && import->module_path[size] != '/'))
        return false;
    const char *suffix = import->module_path + size;
    if (*suffix)
        suffix++;
    if (!cbm_repo_rel_join(source_owner, suffix, expected, sizeof(expected)))
        return false;
    return !strcmp(expected, target->file_path);
}

/* Coordinator normalization before shared semantic resolution. The ordinary
 * dependency projection may already exist, but neither it nor its properties
 * is read: repeat the source/path lookup against the frozen project registry. */
bool cbm_pipeline_capture_go_imports(const cbm_pipeline_ctx_t *ctx, const char *rel,
                                     const CBMFileResult *result) {
    if (!ctx || !rel || !result || result->imports.count < 0)
        return false;
    cbm_gbuf_forget_imports(ctx->gbuf, rel);
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
    if (!file_qn)
        return false;
    CBMArena arena;
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_lookup_context_t context;
    import_lookup_context(ctx, result, rel, file_qn, &arena, &pool, &context);
    size_t count = (size_t)result->imports.count;
    cbm_resolve_import_fact_t *facts =
        count ? cbm_arena_calloc(&arena, count * sizeof(*facts)) : NULL;
    bool ok = !count || facts;
    for (size_t i = 0; ok && i < count; i++) {
        const CBMImport *import = &result->imports.items[i];
        const cbm_gbuf_node_t *target = NULL;
        cbm_resolve_result_t dependency = resolve_import_observed(ctx, rel, file_qn, import, NULL,
                                                                  &target, &context, CBM_LANG_GO);
        facts[i] = (cbm_resolve_import_fact_t){.local_name = import->local_name,
                                               .module_path = import->module_path,
                                               .member = import->member,
                                               .kind = import->kind,
                                               .is_default = import->is_default,
                                               .type_checking_only = import->type_checking_only,
                                               .start_byte = import->site_start_byte,
                                               .end_byte = import->site_end_byte,
                                               .source_origin = import->source_origin,
                                               .dependency = dependency,
                                               .closed = dependency.analysis ==
                                                         CBM_RESOLVE_ANALYSIS_COMPLETE};
        ok = dependency.proof != NULL;
    }
    cbm_resolve_import_snapshot_t snapshot = {
        .source = context.source, .items = facts, .count = count, .complete = ok};
    if (ok)
        ok = cbm_gbuf_capture_imports(ctx->gbuf, &snapshot);
    cbm_arena_destroy(&arena);
    free(file_qn);
    return ok;
}

const cbm_gbuf_node_t *cbm_pipeline_resolve_import_node(const cbm_pipeline_ctx_t *ctx,
                                                        const char *source_rel,
                                                        const char *source_file_qn,
                                                        const CBMImport *imp,
                                                        CBMHashTable *namespace_map) {
    const cbm_gbuf_node_t *target = NULL;
    (void)cbm_pipeline_resolve_import_result(ctx, source_rel, source_file_qn, imp, namespace_map,
                                             &target);
    return target;
}

/* PHP binding evidence is per File. The first field is canonical so its
 * completion bit can be invalidated without allocating, before a rebuild.
 * Node properties are exclusively owned heap strings; both writers run on
 * the coordinator before resolve workers borrow the graph read-only. */
static const char php_import_complete[] = "{\"php_imports_complete\":1,";

/* A resolver NULL can also mean unsupported input or a failed lookup. Only
 * the bounded namespace-only path and an unaliased global class `use` with no
 * same-named PHP type anywhere admit an absence proof. A covered-but-missing
 * PSR-4 file, any other global use, or an unavailable namespace map leaves the
 * whole file unknown; already resolved IMPORTS are still written. */
/* A global-namespace class `use Exception;` names a project type only when a
 * PHP file declares a class-like symbol of that name. With no such node at all
 * (namespaced ones included, so the proof stays conservative), the import is
 * absent from the project: the PHP builtin or a vendor class. Without this a
 * single `use Exception;` left the whole file's evidence unknown, and its
 * project imports reached the PHP LSP as File QNs again. */
static bool php_global_use_known_absent(const cbm_gbuf_t *gbuf, const CBMImport *imp) {
    if (!gbuf || imp->kind != CBM_IMPORT_KIND_DEFAULT) {
        return false;
    }
    const char *name = imp->module_path[0] == '\\' ? imp->module_path + 1 : imp->module_path;
    if (!name[0] || strcmp(name, imp->local_name) != 0) {
        return false; /* `use X as Y;` keeps the old, unknown verdict */
    }
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_name(gbuf, name, &nodes, &count) != 0) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *n = nodes[i];
        if (!n || !n->label || !node_in_php_file(n)) {
            continue;
        }
        if (strcmp(n->label, "Class") == 0 || strcmp(n->label, "Interface") == 0 ||
            strcmp(n->label, "Trait") == 0 || strcmp(n->label, "Enum") == 0 ||
            strcmp(n->label, "File") == 0 || strcmp(n->label, "Module") == 0) {
            return false;
        }
    }
    return true;
}

static bool php_import_known_absent(const cbm_gbuf_t *gbuf, const char *rel, const CBMImport *imp,
                                    CBMHashTable *namespace_map) {
    if (!namespace_map || !imp->module_path || !imp->local_name || !imp->local_name[0] ||
        (imp->kind != CBM_IMPORT_KIND_DEFAULT && imp->kind != CBM_IMPORT_KIND_FUNCTION &&
         imp->kind != CBM_IMPORT_KIND_CONST))
        return false;
    php_use_shape_t shape = php_use_shape(rel, imp);
    if (shape == PHP_USE_GLOBAL)
        return php_global_use_known_absent(gbuf, imp);
    if (shape != PHP_USE_NAMESPACED)
        return false;
    const char *name = imp->module_path[0] == '\\' ? imp->module_path + 1 : imp->module_path;
    char key[CBM_SZ_1K];
    size_t length = strlen(name);
    if (length >= sizeof(key) || name[0] == '\\' || name[length - 1] == '\\' ||
        strstr(name, "\\\\"))
        return false;
    CBMHashTable *pkgmap = cbm_pipeline_get_pkgmap();
    if (pkgmap && imp->kind == CBM_IMPORT_KIND_DEFAULT) {
        /* The resolver does not handle Composer's fallback empty prefix.
         * Its presence therefore cannot support a negative proof either. */
        if (cbm_ht_get(pkgmap, ""))
            return false;
        for (size_t cut = length - 1; cut > 0; cut--) {
            if (name[cut - 1] != '\\')
                continue;
            memcpy(key, name, cut);
            key[cut] = '\0';
            if (cbm_ht_get(pkgmap, key))
                return false;
        }
    }
    memcpy(key, name, length + 1);
    for (char *p = key; *p; p++)
        if (*p == '\\')
            *p = '.';
    if (cbm_ht_get(namespace_map, key))
        return false;
    *strrchr(key, '.') = '\0';
    return cbm_ht_get(namespace_map, key) == NULL;
}

static bool php_json_add_string(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                                const char *text) {
    return text ? yyjson_mut_obj_add_strcpy(doc, obj, key, text)
                : yyjson_mut_obj_add_null(doc, obj, key);
}

static yyjson_mut_val *php_binding_json(yyjson_mut_doc *doc, const CBMImport *imp,
                                        const char *target) {
    yyjson_mut_val *obj = yyjson_mut_obj(doc);
    if (!obj || !php_json_add_string(doc, obj, "local_name", imp->local_name) ||
        !php_json_add_string(doc, obj, "module_path", imp->module_path) ||
        !yyjson_mut_obj_add_int(doc, obj, "kind", imp->kind) ||
        !yyjson_mut_obj_add_bool(doc, obj, "is_default", imp->is_default) ||
        !php_json_add_string(doc, obj, "target", target)) {
        return NULL;
    }
    return obj;
}

static bool php_import_text_equal(const char *left, const char *right) {
    return left && right ? strcmp(left, right) == 0 : left == right;
}

/* Semantic binding consumes normalized source facts. Graph edges and their
 * properties can be absent, coalesced or rebuilt without changing this input. */
static bool php_read_binding_snapshot(const cbm_gbuf_t *gbuf, const char *project, const char *rel,
                                      const CBMFileResult *result, cbm_php_vendor_names_t *out,
                                      bool require_complete) {
    const cbm_resolve_import_snapshot_t *snapshot = cbm_gbuf_imports(gbuf, rel);
    if (!snapshot || !result || result->imports.count < 0 ||
        (result->imports.count && !result->imports.items) ||
        snapshot->count != (size_t)result->imports.count ||
        (require_complete && !snapshot->complete) ||
        !php_import_text_equal(snapshot->source.project, project) ||
        !php_import_text_equal(snapshot->source.file, rel) ||
        !php_import_text_equal(snapshot->source.module, result->module_qn) ||
        !php_import_text_equal(snapshot->source.content_identity,
                               result->source_content_id[CBM_SOURCE_ORIGIN_RAW]) ||
        !php_import_text_equal(snapshot->source.configuration_identity,
                               result->source_configuration_id))
        return false;
    size_t slots = snapshot->count ? snapshot->count : 1;
    if (slots > SIZE_MAX / sizeof(const char *))
        return false;
    const char **targets = cbm_calloc(CBM_MEM_CLASS_OTHER, slots * sizeof(*targets));
    if (!targets)
        return false;
    for (size_t i = 0; i < snapshot->count; i++) {
        const cbm_resolve_import_fact_t *fact = &snapshot->items[i];
        const CBMImport *import = &result->imports.items[i];
        if (!php_import_text_equal(fact->local_name, import->local_name) ||
            !php_import_text_equal(fact->module_path, import->module_path) ||
            !php_import_text_equal(fact->member, import->member) ||
            fact->kind != (uint32_t)import->kind || fact->is_default != import->is_default ||
            fact->type_checking_only != import->type_checking_only ||
            fact->start_byte != import->site_start_byte ||
            fact->end_byte != import->site_end_byte ||
            fact->source_origin != import->source_origin) {
            cbm_free(CBM_MEM_CLASS_OTHER, targets);
            return false;
        }
        if (fact->dependency.proof && cbm_resolve_admit(&fact->dependency))
            targets[i] = fact->dependency.target;
    }
    out->imports = &result->imports;
    out->facts = snapshot;
    if (require_complete)
        out->targets = targets;
    else
        out->proven_targets = targets;
    return true;
}

static bool php_import_sites_unique(const CBMImportArray *imports) {
    CBMHashTable *sites = cbm_ht_create((uint32_t)(imports->count ? imports->count : 1));
    if (!sites)
        return false;
    CBMArena arena;
    cbm_arena_init(&arena);
    bool unique = true;
    for (int i = 0; unique && i < imports->count; i++) {
        const CBMImport *import = &imports->items[i];
        char *key = cbm_arena_sprintf(&arena, "%u:%u:%u", import->source_origin,
                                      import->site_start_byte, import->site_end_byte);
        unique = key && !cbm_ht_get(sites, key);
        if (unique) {
            cbm_ht_set(sites, key, (void *)import);
            unique = cbm_ht_get(sites, key) == import;
        }
    }
    cbm_ht_free(sites);
    cbm_arena_destroy(&arena);
    return unique;
}

#ifdef CBM_ENABLE_TEST_SEAMS
static _Thread_local bool php_test_fail_projection;

void cbm_pipeline_php_test_fail_projection_once(void) {
    php_test_fail_projection = true;
}
#endif

static yyjson_mut_val *php_import_edge_object(yyjson_mut_doc *doc) {
#ifdef CBM_ENABLE_TEST_SEAMS
    if (php_test_fail_projection) {
        php_test_fail_projection = false;
        return NULL;
    }
#endif
    return yyjson_mut_obj(doc);
}

/* Shared by the sequential and parallel PHP IMPORTS writers. Other languages
 * retain their original writer and property layout. */
int cbm_pipeline_php_create_import_edges(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                         const char *rel, const char *file_qn,
                                         const cbm_gbuf_node_t *file, CBMHashTable *namespace_map) {
    cbm_gbuf_forget_imports(ctx->gbuf, rel);
    char *old_json = file->properties_json;
    if (old_json && strncmp(old_json, php_import_complete, sizeof(php_import_complete) - 1) == 0)
        old_json[sizeof(php_import_complete) - 3] = '0';
    /* A failed rebuild invalidates the positive snapshot too. A completion
     * bit cleared on its own must not revive an earlier generation's targets. */
    const char positive_marker[] = "\"php_imports_proven\":1";
    char *old_positive = old_json ? strstr(old_json, positive_marker) : NULL;
    if (old_positive)
        old_positive[sizeof(positive_marker) - 2] = '0';

    int count = result->imports.count;
    if (count < 0 || (count > 0 && !result->imports.items) ||
        !php_import_sites_unique(&result->imports))
        return 0;
    size_t slots = count > 0 ? (size_t)count : 1;
    if (slots > SIZE_MAX / sizeof(const cbm_gbuf_node_t *))
        return 0;
    CBMArena proof_arena;
    cbm_resolve_proof_pool_t proof_pool;
    cbm_resolve_lookup_context_t lookup_context;
    import_lookup_context(ctx, result, rel, file_qn, &proof_arena, &proof_pool, &lookup_context);
    const cbm_gbuf_node_t **targets = cbm_calloc(CBM_MEM_CLASS_OTHER, slots * sizeof(*targets));
    cbm_resolve_result_t *proofs = cbm_calloc(CBM_MEM_CLASS_OTHER, slots * sizeof(*proofs));
    cbm_resolve_import_fact_t *facts = cbm_calloc(CBM_MEM_CLASS_OTHER, slots * sizeof(*facts));
    bool *emitted = cbm_calloc(CBM_MEM_CLASS_OTHER, slots * sizeof(*emitted));
    const char **edge_properties =
        cbm_calloc(CBM_MEM_CLASS_OTHER, slots * sizeof(*edge_properties));
    yyjson_doc *old = old_json ? yyjson_read(old_json, strlen(old_json), 0) : NULL;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    yyjson_mut_val *manifest = doc ? yyjson_mut_arr(doc) : NULL;
    bool ok = targets && proofs && facts && emitted && edge_properties && root && manifest &&
              (!old_json || (old && yyjson_is_obj(yyjson_doc_get_root(old)))) &&
              yyjson_mut_obj_add_int(doc, root, "php_imports_complete", 1) &&
              yyjson_mut_obj_add_int(doc, root, "php_imports_proven", 1) &&
              yyjson_mut_obj_add_val(doc, root, "php_import_bindings", manifest);
    int written = 0;
    bool complete = true;
    if (!ok)
        goto done;
    yyjson_mut_doc_set_root(doc, root);
    if (old) {
        size_t idx, max;
        yyjson_val *key, *value;
        yyjson_obj_foreach(yyjson_doc_get_root(old), idx, max, key, value) {
            const char *name = yyjson_get_str(key);
            if (strcmp(name, "php_imports_complete") == 0 ||
                strcmp(name, "php_imports_proven") == 0 || strcmp(name, "php_import_bindings") == 0)
                continue;
            yyjson_mut_val *copy = yyjson_val_mut_copy(doc, value);
            if (!copy || !yyjson_mut_obj_add_val(doc, root, name, copy)) {
                ok = false;
                goto done;
            }
        }
    }
    for (int i = 0; i < count; i++) {
        const CBMImport *imp = &result->imports.items[i];
        if (!import_occurrence_valid(result, imp)) {
            proofs[i] = (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                               .rule = CBM_RESOLVE_RULE_MODULE_PATH,
                                               .reason = "import_occurrence_invalid"};
        } else {
            proofs[i] = resolve_import_observed(ctx, rel, file_qn, imp, namespace_map, &targets[i],
                                                &lookup_context, result->cached_lang);
        }
        if (targets[i] && targets[i]->id == file->id)
            targets[i] = NULL;
        bool closed = targets[i] || php_import_known_absent(ctx->gbuf, rel, imp, namespace_map);
        if (!closed)
            complete = false;
        facts[i] = (cbm_resolve_import_fact_t){.local_name = imp->local_name,
                                               .module_path = imp->module_path,
                                               .member = imp->member,
                                               .kind = (uint32_t)imp->kind,
                                               .is_default = imp->is_default,
                                               .type_checking_only = imp->type_checking_only,
                                               .start_byte = imp->site_start_byte,
                                               .end_byte = imp->site_end_byte,
                                               .source_origin = imp->source_origin,
                                               .dependency = proofs[i],
                                               .closed = closed};
        if (!targets[i])
            facts[i].dependency.target = NULL;
        yyjson_mut_val *entry =
            php_binding_json(doc, imp, targets[i] ? targets[i]->qualified_name : NULL);
        if (!entry || !yyjson_mut_arr_add_val(manifest, entry)) {
            ok = false;
            goto done;
        }
    }
    cbm_resolve_import_snapshot_t snapshot = {.source = lookup_context.source,
                                              .items = facts,
                                              .count = (size_t)count,
                                              .complete = complete};
    if (!cbm_gbuf_capture_imports(ctx->gbuf, &snapshot)) {
        ok = false;
        goto done;
    }
    for (int i = 0; ok && i < count; i++) {
        if (!targets[i])
            continue;
        cbm_resolve_record_t candidate =
            import_record(result, rel, file_qn, &result->imports.items[i], proofs[i], false);
        if (!cbm_resolve_record_candidate(&candidate))
            continue;
        const char *local = result->imports.items[i].local_name;
        if (!local || !local[0]) {
            ok = false;
            break;
        }
        bool prior = false;
        for (int j = 0; j < i; j++)
            if (targets[j] && targets[j]->id == targets[i]->id &&
                result->imports.items[j].local_name &&
                strcmp(local, result->imports.items[j].local_name) == 0)
                prior = true;
        if (prior)
            continue;
        yyjson_mut_val *edge = php_import_edge_object(doc);
        yyjson_mut_val *bindings = yyjson_mut_arr(doc);
        ok = edge && bindings && yyjson_mut_obj_add_strcpy(doc, edge, "local_name", local) &&
             yyjson_mut_obj_add_val(doc, edge, "php_import_bindings", bindings);
        for (int j = i; ok && j < count; j++) {
            if (!targets[j] || targets[j]->id != targets[i]->id ||
                !result->imports.items[j].local_name ||
                strcmp(local, result->imports.items[j].local_name) != 0)
                continue;
            yyjson_mut_val *entry =
                php_binding_json(doc, &result->imports.items[j], targets[j]->qualified_name);
            ok = entry && yyjson_mut_arr_add_val(bindings, entry);
        }
        char *props = ok ? cbm_json_val_write_text(doc, edge, NULL, NULL) : NULL;
        const char *retained = props ? cbm_arena_strdup(&proof_arena, props) : NULL;
        ok = retained &&
             cbm_gbuf_insert_edge(ctx->gbuf, file->id, targets[i]->id, "IMPORTS", retained) > 0;
        free(props);
        if (ok) {
            written++;
            for (int j = i; j < count; j++)
                if (targets[j] && targets[j]->id == targets[i]->id &&
                    result->imports.items[j].local_name &&
                    strcmp(local, result->imports.items[j].local_name) == 0) {
                    emitted[j] = true;
                    edge_properties[j] = retained;
                }
        }
    }
    if (ok) {
        if (!complete)
            yyjson_mut_set_int(yyjson_mut_obj_get(root, "php_imports_complete"), 0);
        char *props = cbm_json_write_text(doc, NULL, NULL);
        if (props)
            (void)cbm_gbuf_node_set_properties_json((cbm_gbuf_node_t *)file, props);
        free(props);
    }
done:
    if (!ok)
        cbm_gbuf_fail_resolutions(ctx->gbuf);
    for (int i = 0; i < count; i++) {
        cbm_resolve_result_t proof = proofs ? proofs[i] : (cbm_resolve_result_t){0};
        if (!proof.reason) {
            proof.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
            proof.reason = "import_evidence_allocation_failed";
        }
        cbm_resolve_record_t row = import_record(result, rel, file_qn, &result->imports.items[i],
                                                 proof, emitted && emitted[i]);
        row.properties_json = edge_properties ? edge_properties[i] : NULL;
        (void)cbm_gbuf_record_resolution(ctx->gbuf, &row);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, facts);
    cbm_free(CBM_MEM_CLASS_OTHER, proofs);
    cbm_free(CBM_MEM_CLASS_OTHER, emitted);
    cbm_free(CBM_MEM_CLASS_OTHER, edge_properties);
    yyjson_doc_free(old);
    yyjson_mut_doc_free(doc);
    cbm_free(CBM_MEM_CLASS_OTHER, targets);
    cbm_arena_destroy(&proof_arena);
    return written;
}

/* Imported symbol evidence is distinct from the IMPORTS dependency endpoint.
 * Keep a source-ordered manifest so a module named like a type cannot donate
 * that type, and retain unresolved imports as binding blockers. */
static const char rust_binding_marker[] = "{\"rust_imports_proven\":1,";
static const char c_binding_marker[] = "{\"c_includes_proven\":1,";
/* Source binding snapshots retain the exact dependency identity independently
 * of the aggregate IMPORTS edges. C includes name a file's declaration scope;
 * Rust use declarations name an exported item within a proved module tree. */
static cbm_resolve_result_t source_import_binding(const cbm_pipeline_ctx_t *ctx, const char *rel,
                                                  const CBMImport *import,
                                                  CBMHashTable *namespace_map, bool c_family,
                                                  const cbm_gbuf_node_t *dependency,
                                                  cbm_resolve_result_t dependency_proof,
                                                  cbm_resolve_lookup_context_t *context) {
    cbm_resolve_result_t result = {.analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN,
                                   .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
                                   .reason = "source_import_binding_unresolved"};
    const cbm_gbuf_node_t *member = NULL;
    bool ambiguous = false;
    char *namespace = NULL;
    const char *target = NULL;
    if (cbm_resolve_admit(&dependency_proof) && dependency) {
        if (c_family && dependency->label && !strcmp(dependency->label, "File") &&
            dependency->file_path) {
            namespace = cbm_pipeline_fqn_module(ctx->project_name, dependency->file_path);
            target = namespace;
        } else if (!c_family && import->module_path) {
            const cbm_gbuf_node_t *observed =
                resolve_rust_import(ctx, rel, import, namespace_map, &ambiguous, &member);
            if (observed == dependency && member && !ambiguous)
                target = member->qualified_name;
        }
    }
    if (target) {
        result = (cbm_resolve_result_t){
            .binding = CBM_RESOLVE_BINDING_UNIQUE,
            .domain = CBM_RESOLVE_DOMAIN_PROJECT,
            .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
            .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
            .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                     CBM_RESOLVE_FACT_IMPORT_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
            .target = target,
            .candidate_count = 1,
            .reason = c_family ? "source_include_namespace" : "source_rust_exported_member"};
    }
    cbm_resolve_lookup_t lookup;
    const char *domain = c_family ? "c-source-include-namespace" : "rust-source-exported-member";
    cbm_resolve_lookup_begin(&lookup, context, domain, import->module_path, import->site_start_byte,
                             import->site_end_byte, import->source_origin);
    (void)cbm_resolve_lookup_support(&lookup, dependency_proof.proof);
    (void)cbm_resolve_lookup_observe(&lookup, domain, import->module_path, target, result.binding,
                                     result.analysis, result.facts, target != NULL,
                                     c_family ? "exact-header-file-declaration-scope"
                                              : "audited-crate-module-export");
    lookup.key.binding_identity = target;
    lookup.key.declaration_identity = member ? member->qualified_name : NULL;
    const cbm_resolve_proof_t *proof =
        cbm_resolve_lookup_finish(&lookup, CBM_PROOF_EXPORT_BINDING, result);
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(proof);
    if (view) {
        result = view->outcome;
        result.proof = proof;
    } else {
        result.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
        result.facts = 0;
        result.target = NULL;
    }
    free(namespace);
    return result;
}

static void source_write_binding_snapshot(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                          const char *rel, const char *file_qn,
                                          CBMHashTable *namespace_map, bool c_family) {
    cbm_gbuf_forget_imports(ctx->gbuf, rel);
    CBMArena arena;
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_lookup_context_t context;
    import_lookup_context(ctx, result, rel, file_qn, &arena, &pool, &context);
    size_t count = (size_t)result->imports.count;
    cbm_resolve_import_fact_t *facts =
        count ? cbm_arena_calloc(&arena, count * sizeof(*facts)) : NULL;
    bool captured = !count || facts;
    bool every_occurrence_valid = true;
    for (size_t i = 0; captured && i < count; i++) {
        const CBMImport *import = &result->imports.items[i];
        /* An import with no occurrence to prove is one unsupported import, as the
         * ordinary projection above records it, not a failed index: this file's
         * imports stay incomplete, its bindings abstain, the generation publishes. */
        if (!import_occurrence_valid(result, import)) {
            every_occurrence_valid = false;
            cbm_resolve_result_t unsupported = {.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                                .rule = CBM_RESOLVE_RULE_MODULE_PATH,
                                                .reason = "import_occurrence_invalid"};
            facts[i] = (cbm_resolve_import_fact_t){.local_name = import->local_name,
                                                   .module_path = import->module_path,
                                                   .member = import->member,
                                                   .kind = import->kind,
                                                   .is_default = import->is_default,
                                                   .type_checking_only = import->type_checking_only,
                                                   .start_byte = import->site_start_byte,
                                                   .end_byte = import->site_end_byte,
                                                   .source_origin = import->source_origin,
                                                   .dependency = unsupported,
                                                   .binding = unsupported};
            continue;
        }
        const cbm_gbuf_node_t *dependency = NULL;
        cbm_resolve_result_t proof = resolve_import_observed(
            ctx, rel, file_qn, import, namespace_map, &dependency, &context, result->cached_lang);
        facts[i] = (cbm_resolve_import_fact_t){
            .local_name = import->local_name,
            .module_path = import->module_path,
            .member = import->member,
            .kind = import->kind,
            .is_default = import->is_default,
            .type_checking_only = import->type_checking_only,
            .start_byte = import->site_start_byte,
            .end_byte = import->site_end_byte,
            .source_origin = import->source_origin,
            .dependency = proof,
            .binding = source_import_binding(ctx, rel, import, namespace_map, c_family, dependency,
                                             proof, &context),
            .closed = proof.analysis == CBM_RESOLVE_ANALYSIS_COMPLETE};
        if (facts[i].dependency.proof && facts[i].binding.proof)
            continue;
        /* A file can exceed what one proof pool analyses: clang's own lexer tests
         * include one header tens of thousands of times. That limits this file's
         * imports, visibly, and no other file's. Only a failed allocation stops
         * the generation. */
        captured = pool.failure == CBM_RESOLVE_ANALYSIS_LIMITED;
        if (captured) {
            every_occurrence_valid = false;
            cbm_resolve_result_t limited = {.analysis = CBM_RESOLVE_ANALYSIS_LIMITED,
                                            .rule = CBM_RESOLVE_RULE_MODULE_PATH,
                                            .reason = "import_analysis_limited"};
            facts[i].dependency = limited;
            facts[i].binding = limited;
            facts[i].closed = false;
        }
    }
    cbm_resolve_import_snapshot_t snapshot = {.source = context.source,
                                              .items = facts,
                                              .count = count,
                                              .complete = captured && every_occurrence_valid};
    captured = captured && cbm_gbuf_capture_imports(ctx->gbuf, &snapshot);

    /* Compatibility projection only. No semantic consumer reads this JSON. */
    const char *marker = c_family ? c_binding_marker : rust_binding_marker;
    const char *proof_key = c_family ? "c_includes_proven" : "rust_imports_proven";
    const char *bindings_key = c_family ? "c_include_bindings" : "rust_import_bindings";
    const cbm_gbuf_node_t *file = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
    char *old_json = file ? file->properties_json : NULL;
    if (old_json && !strncmp(old_json, marker, strlen(marker)))
        old_json[strlen(marker) - 2] = '0';
    yyjson_doc *old = old_json ? yyjson_read(old_json, strlen(old_json), 0) : NULL;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    yyjson_mut_val *array = doc ? yyjson_mut_arr(doc) : NULL;
    bool ok = captured && file && root && array &&
              (!old_json || (old && yyjson_is_obj(yyjson_doc_get_root(old)))) &&
              yyjson_mut_obj_add_int(doc, root, proof_key, every_occurrence_valid ? 1 : 0) &&
              yyjson_mut_obj_add_val(doc, root, bindings_key, array);
    if (ok)
        yyjson_mut_doc_set_root(doc, root);
    if (ok && old) {
        size_t index, maximum;
        yyjson_val *key, *value;
        yyjson_obj_foreach(yyjson_doc_get_root(old), index, maximum, key, value) {
            const char *name = yyjson_get_str(key);
            if (!strcmp(name, proof_key) || !strcmp(name, bindings_key))
                continue;
            yyjson_mut_val *copy = yyjson_val_mut_copy(doc, value);
            if (!copy || !yyjson_mut_obj_add_val(doc, root, name, copy)) {
                ok = false;
                break;
            }
        }
    }
    for (size_t i = 0; ok && i < count; i++) {
        const cbm_resolve_result_t *proof = c_family ? &facts[i].dependency : &facts[i].binding;
        const char *target = cbm_resolve_admit(proof) ? proof->target : NULL;
        yyjson_mut_val *entry = php_binding_json(doc, &result->imports.items[i], target);
        ok = entry && yyjson_mut_arr_add_val(array, entry);
    }
    char *json = ok ? cbm_json_write_text(doc, NULL, NULL) : NULL;
    if (json)
        ok = cbm_gbuf_node_set_properties_json((cbm_gbuf_node_t *)file, json) == 0;
    else
        ok = false;
    if (!ok) {
        cbm_log_error("resolver.imports.error", "file", rel, "reason", "snapshot_not_written");
        cbm_gbuf_fail_resolutions(ctx->gbuf);
    }
    free(json);
    yyjson_mut_doc_free(doc);
    yyjson_doc_free(old);
    cbm_arena_destroy(&arena);
}

int cbm_pipeline_create_import_edges(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                     const char *rel, CBMHashTable *namespace_map) {
    if (!ctx || !ctx->gbuf || !result || result->imports.count < 0 ||
        (result->imports.count > 0 && !result->imports.items))
        return 0;
    int count = 0;
    /* Extraction disambiguates shared extensions such as Objective-C .m.
     * Re-deriving the language from the filename loses that source fact. */
    CBMLanguage lang = result->cached_lang;
    if (namespace_map && cbm_ht_get(namespace_map, import_context_failed) &&
        (lang == CBM_LANG_RUST || lang == CBM_LANG_C || lang == CBM_LANG_CPP ||
         lang == CBM_LANG_CUDA || lang == CBM_LANG_OBJC))
        namespace_map = NULL;
    for (int i = 0; i < result->imports.count; i++)
        materialize_namespace_import(ctx, &result->imports.items[i], lang, namespace_map);
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
    const cbm_gbuf_node_t *source = file_qn ? cbm_gbuf_find_by_qn(ctx->gbuf, file_qn) : NULL;
    if (source && lang == CBM_LANG_PHP) {
        count =
            cbm_pipeline_php_create_import_edges(ctx, result, rel, file_qn, source, namespace_map);
    } else {
        CBMArena proof_arena;
        cbm_resolve_proof_pool_t proof_pool;
        cbm_resolve_lookup_context_t lookup_context;
        import_lookup_context(ctx, result, rel, file_qn, &proof_arena, &proof_pool,
                              &lookup_context);
        for (int i = 0; i < result->imports.count; i++) {
            const CBMImport *imp = &result->imports.items[i];
            const cbm_gbuf_node_t *target = NULL;
            cbm_resolve_result_t proof =
                import_occurrence_valid(result, imp)
                    ? resolve_import_observed(ctx, rel, file_qn, imp, namespace_map, &target,
                                              &lookup_context, lang)
                    : (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                             .rule = CBM_RESOLVE_RULE_MODULE_PATH,
                                             .reason = "import_occurrence_invalid"};
            cbm_resolve_record_t row = import_record(result, rel, file_qn, imp, proof, false);
            yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
            yyjson_mut_val *properties = doc ? yyjson_mut_obj(doc) : NULL;
            bool valid =
                properties && yyjson_mut_obj_add_strcpy(doc, properties, "local_name",
                                                        imp->local_name ? imp->local_name : "");
            if (valid)
                yyjson_mut_doc_set_root(doc, properties);
            char *props = valid ? cbm_json_write_text(doc, NULL, NULL) : NULL;
            if (!props) {
                row.result.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
                row.result.reason = "import_properties_allocation_failed";
            }
            row.properties_json = props;
            if (props && source && target && source->id != target->id &&
                cbm_resolve_record_candidate(&row)) {
                row.emitted =
                    cbm_gbuf_insert_edge(ctx->gbuf, source->id, target->id, "IMPORTS", props) > 0;
                if (row.emitted)
                    count++;
            }
            (void)cbm_gbuf_record_resolution(ctx->gbuf, &row);
            free(props);
            yyjson_mut_doc_free(doc);
        }
        cbm_arena_destroy(&proof_arena);
    }
    bool c_family = lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA;
    if (source && (lang == CBM_LANG_RUST || c_family))
        source_write_binding_snapshot(ctx, result, rel, file_qn, namespace_map, c_family);
    free(file_qn);
    return count;
}

void cbm_pipeline_php_vendor_names_build(const cbm_gbuf_t *gbuf, const char *project_name,
                                         const char *rel, const CBMFileResult *result,
                                         cbm_php_vendor_names_t *out) {
    memset(out, 0, sizeof(*out));
    if (!gbuf || !result || !rel || cbm_language_for_filename(path_leaf(rel)) != CBM_LANG_PHP)
        return;
    if (!php_read_binding_snapshot(gbuf, project_name, rel, result, out, true))
        (void)php_read_binding_snapshot(gbuf, project_name, rel, result, out, false);
}

void cbm_pipeline_php_vendor_names_free(cbm_php_vendor_names_t *value) {
    if (value) {
        cbm_free(CBM_MEM_CLASS_OTHER, value->targets);
        cbm_free(CBM_MEM_CLASS_OTHER, value->proven_targets);
        memset(value, 0, sizeof(*value));
    }
}

static char *php_bound_class(const cbm_gbuf_t *graph, const char *project, const CBMImport *import,
                             const char *target) {
    if (!target || import->kind != CBM_IMPORT_KIND_DEFAULT || !import->module_path)
        return NULL;
    const cbm_gbuf_node_t *file = cbm_gbuf_find_by_qn(graph, target);
    if (!file || !file->file_path || !file->label)
        return NULL;
    const char *leaf = strrchr(import->module_path, '\\');
    leaf = leaf ? leaf + 1 : import->module_path;
    char *qn = cbm_pipeline_fqn_compute(project, file->file_path, leaf);
    const cbm_gbuf_node_t *member = qn ? cbm_gbuf_find_by_qn(graph, qn) : NULL;
    if (!member || !member->file_path || strcmp(member->file_path, file->file_path) ||
        !member->label ||
        (strcmp(member->label, "Class") && strcmp(member->label, "Interface") &&
         strcmp(member->label, "Enum") && strcmp(member->label, "Trait"))) {
        free(qn);
        return NULL;
    }
    return qn;
}

int cbm_pipeline_php_import_map(const cbm_gbuf_t *graph, const char *project,
                                const char *source_rel, const CBMFileResult *result,
                                const char ***out_keys, const char ***out_values, int *out_count) {
    *out_keys = NULL;
    *out_values = NULL;
    *out_count = 0;
    int capacity = result ? result->imports.count : 0;
    if (capacity <= 0)
        return 0;
    const char **keys = calloc((size_t)capacity, sizeof(*keys));
    const char **values = calloc((size_t)capacity, sizeof(*values));
    CBMHashTable *by_local = cbm_ht_create((uint32_t)capacity);
    cbm_php_vendor_names_t bindings = {0};
    cbm_pipeline_php_vendor_names_build(graph, project, source_rel, result, &bindings);
    const char *const *targets = bindings.targets ? bindings.targets : bindings.proven_targets;
    int count = 0;
    bool ok = keys && values && by_local;
    for (int i = 0; ok && i < capacity; i++) {
        const CBMImport *import = &result->imports.items[i];
        if (!import->local_name || !import->local_name[0])
            continue;
        char *value = targets ? php_bound_class(graph, project, import, targets[i]) : NULL;
        if (!value)
            value = strdup("");
        ok = python_map_put(by_local, keys, values, &count, import->local_name, value);
    }
    cbm_pipeline_php_vendor_names_free(&bindings);
    cbm_ht_free(by_local);
    if (!ok) {
        for (int i = 0; i < count; i++) {
            free((void *)keys[i]);
            free((void *)values[i]);
        }
        free(keys);
        free(values);
        return -1;
    }
    *out_keys = keys;
    *out_values = values;
    *out_count = count;
    return 0;
}

static bool php_vendor_kind(const cbm_php_vendor_names_t *value, const char *name, int kind) {
    if (!value || !value->targets || !value->imports || !name || name[0] == '\\')
        return false;
    size_t length = 0;
    while (php_ident_byte((unsigned char)name[length]))
        length++;
    if (length == 0)
        return false;
    for (int i = 0; i < value->imports->count; i++) {
        const CBMImport *imp = &value->imports->items[i];
        if (!imp->local_name || !imp->module_path || value->targets[i] ||
            (kind >= 0 && (int)imp->kind != kind))
            continue;
        if (strlen(imp->local_name) == length && strncmp(imp->local_name, name, length) == 0)
            return true;
    }
    return false;
}

bool cbm_pipeline_php_vendor_bound(const cbm_php_vendor_names_t *value, const char *name) {
    return php_vendor_kind(value, name, CBM_IMPORT_KIND_DEFAULT);
}

/* Existing occurrence metadata can identify a constructor or a type without
 * adding serialized fields. If its tree/source was not retained, -1 preserves
 * uncertainty: only an unresolved matching import can veto a guessed edge. */
static int php_import_kind_at_site(const CBMFileResult *result, uint32_t start, uint32_t end,
                                   CBMSourceOrigin origin, bool call) {
    if (!result || origin != CBM_SOURCE_ORIGIN_RAW || end <= start)
        return -1;
    if (result->cached_tree) {
        TSNode node = ts_node_descendant_for_byte_range(ts_tree_root_node(result->cached_tree),
                                                        start, end - 1);
        for (int depth = 0; !ts_node_is_null(node) && depth < 16; depth++) {
            const char *type = ts_node_type(node);
            if (strcmp(type, "named_type") == 0 || strcmp(type, "base_clause") == 0 ||
                strcmp(type, "class_interface_clause") == 0)
                return CBM_IMPORT_KIND_DEFAULT;
            if (strcmp(type, "object_creation_expression") == 0 ||
                strcmp(type, "scoped_call_expression") == 0 ||
                strcmp(type, "class_constant_access_expression") == 0) {
                TSNode scope = ts_node_named_child(node, 0);
                if (call || (!ts_node_is_null(scope) && ts_node_start_byte(scope) <= start &&
                             ts_node_end_byte(scope) >= end))
                    return CBM_IMPORT_KIND_DEFAULT;
                break;
            }
            if (strcmp(type, "function_call_expression") == 0) {
                TSNode function = ts_node_child_by_field_name(node, "function", 8);
                if (call || (!ts_node_is_null(function) && ts_node_start_byte(function) <= start &&
                             ts_node_end_byte(function) >= end))
                    return CBM_IMPORT_KIND_FUNCTION;
                break;
            }
            if (strcmp(type, "namespace_use_declaration") == 0 ||
                strcmp(type, "method_declaration") == 0 || strcmp(type, "function_definition") == 0)
                break;
            node = ts_node_parent(node);
        }
    }
    if (call && result->source && end <= (uint32_t)result->source_len && end - start > 3 &&
        memcmp(result->source + start, "new", 3) == 0 &&
        isspace((unsigned char)result->source[start + 3]))
        return CBM_IMPORT_KIND_DEFAULT;
    return -1;
}

bool cbm_pipeline_php_vendor_call(const cbm_php_vendor_names_t *value, const CBMFileResult *result,
                                  const CBMCall *call) {
    if (!call || !call->callee_name)
        return false;
    int kind = strstr(call->callee_name, "::")
                   ? CBM_IMPORT_KIND_DEFAULT
                   : php_import_kind_at_site(result, call->site_start_byte, call->site_end_byte,
                                             call->source_origin, true);
    return php_vendor_kind(value, call->callee_name, kind);
}

bool cbm_pipeline_php_vendor_usage(const cbm_php_vendor_names_t *value, const CBMFileResult *result,
                                   const CBMUsage *usage) {
    if (!usage)
        return false;
    int kind = php_import_kind_at_site(result, usage->site_start_byte, usage->site_end_byte,
                                       usage->source_origin, false);
    return php_vendor_kind(value, usage->ref_name, kind);
}

/* ── Namespace map ───────────────────────────────────────────────── */

/* The namespace names themselves, so a caller that has parked some results on
 * disk can still contribute their namespaces (see
 * cbm_result_spill_namespace). A file missing from this map does not fail to
 * resolve -- it resolves DIFFERENTLY, through the looser fallback, which is why
 * an incomplete map changed edge counts in both directions rather than only
 * losing edges. */
CBMHashTable *cbm_pipeline_namespace_map_build_names(const char *project_name,
                                                     const char *const *namespaces,
                                                     const char *const *rels, int count) {
    /* NULL means no namespace information (e.g. the CALLS import map), while
     * an empty map means no project file declares a namespace. A partial map
     * cannot prove a PHP import absent, so every construction failure returns
     * NULL after releasing the accumulated entries. */
    if (count < 0 || (count > 0 && (!namespaces || !rels)))
        return NULL;
    CBMHashTable *map = cbm_ht_create(CBM_SZ_64);
    if (!map) {
        return NULL;
    }
    for (int i = 0; i < count; i++) {
        const char *namespace_name = namespaces[i] ? namespaces[i] : "";
        char reverse[PKGMAP_PATH_BUF];
        int reverse_len =
            rels[i] ? snprintf(reverse, sizeof(reverse), "@cbm:file:%s", rels[i]) : -1;
        if (reverse_len <= 0 || (size_t)reverse_len >= sizeof(reverse)) {
            cbm_pipeline_namespace_map_free(map);
            return NULL;
        }
        if (cbm_ht_has(map, reverse)) {
            cbm_pipeline_namespace_map_free(map);
            return NULL;
        }
        char *reverse_key = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, reverse);
        char *reverse_value = cbm_mem_strdup(CBM_MEM_CLASS_SEMANTIC, namespace_name);
        if (reverse_value)
            for (char *p = reverse_value; *p; p++)
                if (*p == '\\' || *p == ':' || *p == '/')
                    *p = '.';
        if (!reverse_key || !reverse_value) {
            cbm_free(CBM_MEM_CLASS_OTHER, reverse_key);
            cbm_free(CBM_MEM_CLASS_SEMANTIC, reverse_value);
            cbm_pipeline_namespace_map_free(map);
            return NULL;
        }
        cbm_ht_set(map, reverse_key, reverse_value);
        if (cbm_ht_get_key(map, reverse_key) != reverse_key ||
            cbm_ht_get(map, reverse_key) != reverse_value) {
            if (cbm_ht_get_key(map, reverse_key) != reverse_key)
                cbm_free(CBM_MEM_CLASS_OTHER, reverse_key);
            if (cbm_ht_get(map, reverse) != reverse_value)
                cbm_free(CBM_MEM_CLASS_SEMANTIC, reverse_value);
            cbm_pipeline_namespace_map_free(map);
            return NULL;
        }
        if (!namespace_name[0])
            continue;
        char *file_qn = NULL;
        char *key = NULL;
        bool ok = false;
        if (!rels[i])
            goto item_done;
        file_qn = pkgmap_take_raw(cbm_pipeline_fqn_compute(project_name, rels[i], "__file__"));
        if (!file_qn) {
            goto item_done;
        }
        /* Normalize the namespace key to dot-separated form so it matches the
         * dot-normalized lookups in cbm_pipeline_resolve_import_node (PHP uses
         * '\\', some grammars '::' or '/'). */
        key = cbm_mem_strdup(CBM_MEM_CLASS_OTHER, namespace_name);
        if (!key) {
            goto item_done;
        }
        for (char *p = key; *p; p++) {
            if (*p == '\\' || *p == ':' || *p == '/') {
                *p = '.';
            }
        }
        /* Store ALL files declaring a namespace as a '\n'-delimited list so the
         * resolver can pick a non-importer sibling (see resolve loop). The hash
         * table does not copy keys. The map owns OTHER keys and SEMANTIC
         * values; raw FQN API results are copied into that value class. */
        if (!cbm_ht_has(map, key)) {
            cbm_ht_set(map, key, file_qn); /* map owns key + file_qn */
            /* set returns NULL for both insertion and allocation failure.
             * These lookups allocate nothing and verify ownership transfer. */
            const char *stored_key = cbm_ht_get_key(map, key);
            void *stored_value = cbm_ht_get(map, key);
            ok = stored_key == key && stored_value == file_qn;
            if (stored_key == key)
                key = NULL;
            if (stored_value == file_qn)
                file_qn = NULL;
        } else {
            /* Append to the existing list. Re-key with the STORED key pointer
             * (not our fresh copy) so the map's key pointer never changes —
             * otherwise Verstable would adopt the new key and our cleanup
             * below would free the live key (use-after-free). */
            const char *stored_key = cbm_ht_get_key(map, key);
            const char *cur = (const char *)cbm_ht_get(map, key);
            char *combined = NULL;
            if (stored_key && cur) {
                size_t cur_len = strlen(cur), file_len = strlen(file_qn);
                if (file_len > SIZE_MAX - 2 || cur_len > SIZE_MAX - file_len - 2) {
                    goto item_done;
                }
                size_t need = cur_len + 1 + file_len + 1;
                combined = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, need);
                if (combined) {
                    snprintf(combined, need, "%s\n%s", cur, file_qn);
                    (void)cbm_ht_set(map, stored_key, combined);
                    if (cbm_ht_get(map, stored_key) != combined) {
                        cbm_free(CBM_MEM_CLASS_SEMANTIC, combined);
                        combined = NULL;
                    } else {
                        cbm_free(CBM_MEM_CLASS_SEMANTIC, (void *)cur); /* replaced value string */
                    }
                }
            }
            ok = combined != NULL;
        }
    item_done:
        cbm_free(CBM_MEM_CLASS_OTHER, key);
        cbm_free(CBM_MEM_CLASS_SEMANTIC, file_qn);
        if (!ok) {
            cbm_pipeline_namespace_map_free(map);
            return NULL;
        }
    }
    return map;
}

/* Convenience for callers whose results are all in memory (the sequential
 * definitions pass). A caller that can SPILL must use the _names variant and
 * fill the parked slots from cbm_result_spill_namespace, or its map silently
 * loses those files. */
CBMHashTable *cbm_pipeline_namespace_map_build(const char *project_name,
                                               CBMFileResult *const *results,
                                               const char *const *rels, int count) {
    if (count < 0 || (count > 0 && (!results || !rels)) ||
        (size_t)count > SIZE_MAX / sizeof(char *))
        return NULL;
    if (count == 0)
        return cbm_pipeline_namespace_map_build_names(project_name, NULL, rels, 0);
    const char **names = cbm_calloc(CBM_MEM_CLASS_OTHER, (size_t)count * sizeof(char *));
    if (!names) {
        return NULL;
    }
    for (int i = 0; i < count; i++) {
        names[i] = results[i] ? results[i]->namespace_name : NULL;
    }
    CBMHashTable *map = cbm_pipeline_namespace_map_build_names(project_name, names, rels, count);
    cbm_free(CBM_MEM_CLASS_OTHER, names);
    return map;
}

static void ns_map_free_entry(const char *key, void *value, void *ud) {
    (void)ud;
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, value);
}

void cbm_pipeline_namespace_map_free(CBMHashTable *map) {
    if (!map) {
        return;
    }
    cbm_ht_foreach(map, ns_map_free_entry, NULL);
    cbm_ht_free(map);
}

const cbm_resolve_import_snapshot_t *cbm_pipeline_import_snapshot(const cbm_gbuf_t *graph,
                                                                  const char *project,
                                                                  const char *rel,
                                                                  const CBMFileResult *result) {
    const cbm_resolve_import_snapshot_t *snapshot = cbm_gbuf_imports(graph, rel);
    if (!snapshot || !result || result->imports.count < 0 || !snapshot->complete ||
        snapshot->count != (size_t)result->imports.count ||
        !php_import_text_equal(snapshot->source.project, project) ||
        !php_import_text_equal(snapshot->source.file, rel) ||
        !php_import_text_equal(snapshot->source.module, result->module_qn) ||
        !php_import_text_equal(snapshot->source.content_identity,
                               result->source_content_id[CBM_SOURCE_ORIGIN_RAW]) ||
        !php_import_text_equal(snapshot->source.configuration_identity,
                               result->source_configuration_id))
        return NULL;
    for (size_t i = 0; i < snapshot->count; i++) {
        const cbm_resolve_import_fact_t *fact = &snapshot->items[i];
        const CBMImport *import = &result->imports.items[i];
        if (!import_occurrence_valid(result, import) ||
            !php_import_text_equal(fact->local_name, import->local_name) ||
            !php_import_text_equal(fact->module_path, import->module_path) ||
            !php_import_text_equal(fact->member, import->member) || fact->kind != import->kind ||
            fact->is_default != import->is_default ||
            fact->type_checking_only != import->type_checking_only ||
            fact->start_byte != import->site_start_byte ||
            fact->end_byte != import->site_end_byte || fact->source_origin != import->source_origin)
            return NULL;
    }
    return snapshot;
}

static int source_import_map(const cbm_gbuf_t *graph, const char *project, const char *source_rel,
                             const CBMFileResult *result, bool c_family, const char ***out_keys,
                             const char ***out_values, int *out_count) {
    *out_keys = NULL;
    *out_values = NULL;
    *out_count = 0;
    int capacity = result ? result->imports.count : 0;
    if (capacity <= 0)
        return 0;
    const char **keys = calloc((size_t)capacity, sizeof(*keys));
    const char **values = calloc((size_t)capacity, sizeof(*values));
    CBMHashTable *locals = cbm_ht_create((uint32_t)capacity);
    const cbm_resolve_import_snapshot_t *snapshot =
        cbm_pipeline_import_snapshot(graph, project, source_rel, result);
    bool ok = keys && values && locals;
    int count = 0;
    for (int i = 0; ok && i < capacity; i++) {
        const CBMImport *import = &result->imports.items[i];
        const char *key = c_family ? import->module_path : import->local_name;
        if (!key || !key[0])
            continue;
        const cbm_resolve_import_fact_t *fact = snapshot ? &snapshot->items[i] : NULL;
        bool proven = fact && cbm_resolve_admit(&fact->dependency) &&
                      cbm_resolve_admit(&fact->binding) && fact->dependency.proof &&
                      fact->binding.proof;
        char *binding = strdup(proven ? fact->binding.target : "");
        ok = python_map_put(locals, keys, values, &count, key, binding);
    }
    cbm_ht_free(locals);
    if (!ok) {
        for (int i = 0; i < count; i++) {
            free((void *)keys[i]);
            free((void *)values[i]);
        }
        free(keys);
        free(values);
        return -1;
    }
    *out_keys = keys;
    *out_values = values;
    *out_count = count;
    return 0;
}

int cbm_pipeline_rust_import_map(const cbm_gbuf_t *graph, const char *project,
                                 const char *source_rel, const CBMFileResult *result,
                                 const char ***out_keys, const char ***out_values, int *out_count) {
    return source_import_map(graph, project, source_rel, result, false, out_keys, out_values,
                             out_count);
}

int cbm_pipeline_c_include_map(const cbm_gbuf_t *graph, const char *project, const char *source_rel,
                               const CBMFileResult *result, const char ***out_keys,
                               const char ***out_values, int *out_count) {
    return source_import_map(graph, project, source_rel, result, true, out_keys, out_values,
                             out_count);
}
